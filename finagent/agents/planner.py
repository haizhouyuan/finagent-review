"""Planner agent — analyzes graph state and generates micro-queries.

The Planner is the "brain" of the research loop. It:
  1. Reads the current knowledge graph summary and blind spots
  2. Compares against the research goal
  3. Generates 3-5 targeted micro-queries to fill the most impactful gaps
  4. Outputs ``missing`` and ``superfluous`` signals (semantic gradient)
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from .state import ResearchState

logger = logging.getLogger(__name__)

PLANNER_SYSTEM_PROMPT = """\
你是一个投研图谱探索规划师。你的任务是分析当前知识图谱的状态，与研究目标对比，找出最关键的信息缺口，然后生成精准的搜索策略。

## 输出格式（严格JSON）：
```json
{
  "analysis": "对当前图谱覆盖度的简短分析（1-2句话）",
  "missing": ["缺失的关键信息1", "缺失的关键信息2"],
  "superfluous": ["可能多余/噪声的信息"],
  "queries": [
    {"query": "搜索查询文本", "priority": 1, "target_entity": "目标实体", "expected_info": "期望获得的信息"},
    {"query": "搜索查询文本2", "priority": 2, "target_entity": "目标实体2", "expected_info": "期望获得的信息2"}
  ],
  "confidence": 0.3
}
```

## 规则：
1. queries 最多 5 条，按优先级排序
2. 优先填充介数中心性高但信息密度低的节点（产业链关键卡点）
3. 优先探索 missing_entity > missing_relation > missing_attribute
4. confidence 表示当前研究完成度（0.0-1.0），达到 0.85+ 说明研究充分
5. query 文本应该是可直接用于搜索引擎的中文查询
"""


def planner_node(
    state: ResearchState,
    *,
    llm_fn: Callable[[str, str], str] | None = None,
    graph_store: Any | None = None,
    graph_retriever: Any | None = None,
    memory_manager: Any | None = None,
) -> dict[str, Any]:
    """Planner agent node for LangGraph.

    Analyzes the current graph state and research goal to generate
    targeted micro-queries.

    Args:
        graph_retriever: Optional GraphRetriever instance. When provided,
            injects structured graph relationship context into the prompt
            via hybrid retrieval (local + global).
    """
    step = state["iteration_step"]
    goal = state["research_goal"]
    context = state["context"]
    kg_summary = state.get("knowledge_graph_summary", "")
    completed = state.get("completed_queries", [])
    addressed = state.get("blind_spots_addressed", [])

    memory_summary = ""
    if memory_manager is not None:
        try:
            from finagent.memory import MemoryTier

            semantics = memory_manager.recall(
                "", tier=MemoryTier.SEMANTIC, limit=10,
            )
            if semantics:
                memory_summary = "\n## 已固化认知\n" + "\n".join(
                    f"- [{record.category}] {record.content}"
                    for record in semantics
                )
        except Exception as exc:
            logger.warning("semantic memory recall failed: %s", exc)

    # Generate blind spots analysis from graph if available
    blind_spots_text = ""
    if graph_store:
        try:
            from finagent.graph_v2.blind_spots import BlindSpotClassifier
            classifier = BlindSpotClassifier(graph_store)
            spots = classifier.find_all(max_results=10)
            if spots:
                blind_spots_text = "\n当前盲区:\n" + "\n".join(
                    f"  - [{s.spot_type.value}] {s.description} (优先级={s.priority:.2f})"
                    for s in spots
                    if s.node_id not in addressed
                )
        except Exception as exc:
            logger.warning("blind spot analysis failed: %s", exc)

    # GraphRetriever context (P1b) ─────────────────────────────────
    graph_context_text = ""
    if graph_retriever is not None:
        try:
            # Pick a focus node from the most recent completed query entity
            focus_node = None
            if addressed:
                focus_node = addressed[-1]

            retrieval_ctx = graph_retriever.retrieve(
                goal, focus_node=focus_node, mode="hybrid",
            )
            if retrieval_ctx and retrieval_ctx != "(no relevant graph context found)":
                graph_context_text = f"\n## 图谱关系上下文\n{retrieval_ctx}"
        except Exception as exc:
            logger.warning("graph retriever failed: %s", exc)

    user_prompt = f"""\
## 研究目标
{goal}

## 产业领域
{context}

## 当前图谱状态
{kg_summary or '(空图谱，需要从头开始)'}
{graph_context_text}
{memory_summary}
{blind_spots_text}

## 已执行的查询（避免重复）
{', '.join(completed[-10:]) if completed else '(无)'}

## 已探索的节点
{', '.join(addressed[-10:]) if addressed else '(无)'}

请基于以上信息，生成下一轮的搜索策略。
"""

    if llm_fn:
        try:
            raw = llm_fn(PLANNER_SYSTEM_PROMPT, user_prompt)
            parsed = _parse_planner_output(raw)
            queries = [q["query"] for q in parsed.get("queries", [])]
            confidence = parsed.get("confidence", 0.0)

            logger.info(
                "planner [step %d]: %d queries generated, confidence=%.2f",
                step, len(queries), confidence,
            )

            return {
                "pending_queries": queries[:5],
                "confidence_score": confidence,
                "iteration_step": step + 1,
            }
        except Exception as exc:
            logger.error("planner LLM call failed: %s", exc)
            return {
                "errors": state.get("errors", []) + [f"planner error: {exc}"],
                "iteration_step": step + 1,
            }
    else:
        # Fallback: generate queries from blind spots
        queries = _generate_fallback_queries(goal, context, graph_store, addressed)
        return {
            "pending_queries": queries[:5],
            "iteration_step": step + 1,
        }


def _generate_fallback_queries(
    goal: str,
    context: str,
    graph_store: Any | None,
    addressed: list[str],
) -> list[str]:
    """Generate queries without LLM (uses blind spots heuristic)."""
    queries = []

    if graph_store:
        try:
            from finagent.graph_v2.blind_spots import BlindSpotClassifier
            classifier = BlindSpotClassifier(graph_store)
            spots = classifier.find_all(max_results=5)
            for spot in spots:
                if spot.node_id not in addressed:
                    queries.append(f"{spot.node_id} {context} 产业链 供应商 客户 研报")
        except Exception:
            pass

    if not queries:
        # Ultimate fallback: search the goal itself
        queries = [f"{goal} 产业链 核心公司 供应链"]

    return queries


def _parse_planner_output(raw: str) -> dict[str, Any]:
    """Parse the planner's JSON output, handling markdown fences."""
    import json
    import re

    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        logger.warning("failed to parse planner output: %s...", text[:200])
        return {"queries": [], "confidence": 0.0}
