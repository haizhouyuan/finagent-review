"""Synthesizer — generates evidence-grounded research reports.

Converts the accumulated research state (graph + evidence + triples)
into a structured investment research memo with provenance tracking.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


def synthesize_report(
    state: dict[str, Any],
    *,
    graph_store: Any | None = None,
    llm_fn: Callable[[str, str], str] | None = None,
) -> str:
    """Generate a structured research report from the final state.

    Report structure:
      1. Research Q & executive summary
      2. Confirmed relationships (with evidence)
      3. Unconfirmed hypotheses
      4. Remaining blind spots
      5. Topology insights (chokepoints, hubs)
    """
    goal = state.get("research_goal", "")
    total_triples = state.get("total_triples_added", 0)
    confidence = state.get("confidence_score", 0.0)
    steps = state.get("iteration_step", 0)
    term_reason = state.get("termination_reason", "")

    sections: list[str] = []

    # 1. Header
    sections.append(f"# 投研图谱分析报告\n")
    sections.append(f"## 研究目标\n{goal}\n")
    sections.append(f"**完成度**: {confidence:.0%} | "
                    f"**迭代**: {steps} 轮 | "
                    f"**新增关系**: {total_triples} 条 | "
                    f"**终止原因**: {term_reason}\n")

    # 2. Graph statistics
    if graph_store:
        try:
            stats = graph_store.stats()
            sections.append("## 图谱概况\n")
            sections.append(f"- 总节点: {stats['total_nodes']}")
            sections.append(f"- 总边数: {stats['total_edges']}")
            for nt, count in sorted(stats["node_types"].items(), key=lambda x: -x[1]):
                sections.append(f"  - {nt}: {count}")
            sections.append("")
        except Exception as exc:
            logger.warning("stats generation failed: %s", exc)

    # 3. Key entities (by degree)
    if graph_store:
        try:
            top_nodes = sorted(
                graph_store.g.degree(), key=lambda x: -x[1],
            )[:15]
            if top_nodes:
                sections.append("## 核心实体\n")
                sections.append("| 实体 | 类型 | 度 |")
                sections.append("|------|------|---|")
                for nid, degree in top_nodes:
                    node = graph_store.get_node(nid) or {}
                    label = node.get("label", nid)
                    ntype = node.get("node_type", "?")
                    sections.append(f"| {label} | {ntype} | {degree} |")
                sections.append("")
        except Exception:
            pass

    # 4. Topology insights
    if graph_store:
        try:
            from finagent.graph_v2.topology import TopologyAnalyzer
            topo = TopologyAnalyzer(graph_store)

            bc = topo.betweenness_centrality(top_k=5)
            if bc:
                sections.append("## 卡脖子节点（介数中心性 Top 5）\n")
                for item in bc:
                    sections.append(
                        f"- **{item['label']}** [{item['node_type']}]: "
                        f"betweenness={item['betweenness']}, degree={item['degree']}"
                    )
                sections.append("")

            hubs = topo.hub_nodes(min_degree=4)
            if hubs:
                sections.append("## 枢纽节点\n")
                for h in hubs[:5]:
                    sections.append(
                        f"- **{h['label']}**: degree={h['degree']} "
                        f"(in={h['in_degree']}, out={h['out_degree']})"
                    )
                sections.append("")
        except Exception:
            pass

    # 5. Blind spots
    if graph_store:
        try:
            from finagent.graph_v2.blind_spots import BlindSpotClassifier
            classifier = BlindSpotClassifier(graph_store)
            spots = classifier.find_all(max_results=10)
            if spots:
                sections.append("## 待探索盲区\n")
                for s in spots:
                    sections.append(f"- [{s.spot_type.value}] {s.description} (优先级={s.priority:.2f})")
                sections.append("")
        except Exception:
            pass

    # 6. Errors
    errors = state.get("errors", [])
    if errors:
        sections.append("## 处理过程中的错误\n")
        for e in errors:
            sections.append(f"- ⚠️ {e}")
        sections.append("")

    return "\n".join(sections)
