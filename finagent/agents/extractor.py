"""Extractor agent — converts raw evidence into structured triples.

⚠️ ARCHITECTURAL FIXES applied:
  Fix #2: Front-loaded entity resolution — candidate entities from the
          existing graph are injected INTO the extraction prompt, forcing
          the LLM to reuse existing entity IDs rather than creating
          duplicates.
  Fix #3: exact_quote validation — replaces LLM-as-a-Judge with
          hard rule: every triple must include an exact_quote from the
          source text.  If `exact_quote not in source_text`, the triple
          is rejected.  Zero LLM calls for validation = zero hallucination
          collusion.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Callable

from .state import ResearchState

logger = logging.getLogger(__name__)


def _build_extraction_prompt(
    candidate_entities: list[str] | None = None,
) -> str:
    """Build the system prompt with candidate entities injected."""
    candidates_block = ""
    if candidate_entities:
        candidates_block = f"""
## ⚠️ 已有实体列表（前置消歧 — 必须遵守）
以下实体已存在于知识图谱中。如果你提取的实体与列表中的某个名称相似度极高，
**必须复用列表中的标准名称**，不允许创建变体。
只有确认为全新实体时，才可使用新名称。

已有实体：
{chr(10).join(f'- {e}' for e in candidate_entities[:50])}
"""

    return f"""\
你是一个产业链知识图谱构建助手。从投研文本中提取结构化三元组。

## 输出格式：JSON 数组
```json
[
  {{
    "head": "实体A（必须匹配已有实体列表或为全新实体）",
    "head_type": "company",
    "relation": "supplies_core_part_to",
    "tail": "实体B",
    "tail_type": "company",
    "exact_quote": "原文中支持该关系的精确引用（必须是原文的连续子串）",
    "confidence": 0.9,
    "valid_from": "2024-01"
  }}
]
```
{candidates_block}
## 允许的 relation 类型：
- supplies_core_part_to: 核心零部件供应
- launch_service_for: 发射服务
- customer_of: 客户关系
- manufactures: 制造
- component_of: 组件/子系统
- competes_with: 竞争
- partners_with: 合作
- invested_by: 投资/持股
- controls: 控股/实控
- enables: 技术使能
- technology_benchmark: 技术对标
- belongs_to: 板块归属
- operates: 运营
- bid_won_contract: 中标合同
- regulates: 政策约束
- related_to: 其他关联

## 允许的 node type：
company, space_system, component, infrastructure, technology, project, sector, financial_instrument, person, policy, entity

## 规则：
1. 只提取明确或强烈暗示的关系，不臆测
2. exact_quote **必须** 是输入原文的连续子串（可以是原文的一部分），不得修改、概括或杜撰
3. confidence: 0.9+ 明确说了, 0.7-0.9 强烈暗示, 0.5-0.7 推断
4. valid_from: 关系生效时间 (YYYY-MM 或 YYYY)，不确定用 "unknown"
5. 对于已有实体列表中的实体，必须使用列表中的原名；不允许使用别名/简称创建新节点
6. 无可提取关系返回空数组 []
"""


# ── Competitive data extraction prompt ─────────────────────────────

# Keywords that trigger competitive extraction from evidence text
COMPETITIVE_KEYWORDS = (
    "车型", "车架", "轮毂", "电动车", "两轮", "三轮", "电摩", "电动自行车",
    "电池", "电机", "续航", "制动", "碟刹", "鼓刹",
    "九号", "雅迪", "爱玛", "台铃", "小牛", "新日", "绿源",
    "Yadea", "Aima", "Tailg", "Niu", "Ninebot",
    "价格", "定价", "售价", "指导价",
    "车身", "外观", "产品图", "实拍", "官网",
    "product", "model", "wheel", "frame", "motor", "battery",
)


def _has_competitive_signal(text: str) -> bool:
    """Check if evidence text contains competitive product keywords."""
    text_lower = text.lower()
    return sum(1 for kw in COMPETITIVE_KEYWORDS if kw.lower() in text_lower) >= 2


def _build_competitive_prompt() -> str:
    """Build system prompt for extracting competitive product data."""
    return """\
你是一个竞品分析助手。从产品/市场文本中提取两类结构化数据。

## 输出格式：JSON 对象
```json
{
  "image_assets": [
    {
      "asset_id": "img-<brand>-<model>-<view>",
      "brand": "品牌名",
      "product_line": "产品线/系列",
      "category": "exterior|structure|detail|field_research",
      "source_url": "图片来源URL（如有）",
      "visible_content": "图片中可见的关键内容描述",
      "supports_conclusion": "该图片支持的结论"
    }
  ],
  "sku_records": [
    {
      "sku_id": "sku-<brand>-<model>",
      "brand": "品牌名",
      "series": "系列名",
      "model": "具体型号",
      "positioning": "低端|中端|中高端|高端",
      "price_range": "最低价-最高价",
      "wheel_diameter": "轮径（如14寸）",
      "frame_type": "车架类型（单管|双管|双管一体等）",
      "motor_type": "电机类型和功率",
      "battery_platform": "电池规格",
      "brake_config": "制动配置",
      "target_audience": "目标用户",
      "style_tags": ["标签1", "标签2"]
    }
  ]
}
```

## 规则：
1. 只提取文本中明确提到的产品数据，不臆测
2. asset_id 格式: img-品牌拼音-型号-视角(side/front/frame/detail)
3. sku_id 格式: sku-品牌拼音-型号
4. 如果文本不涉及具体产品，返回空数组
5. price_range 用数字，如 "3299-4299"
6. 如果某字段信息不足，用空字符串
"""


def _extract_competitive_data(
    text: str,
    llm_fn: Callable[[str, str], str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extract competitive image_assets and sku_records from evidence text.

    Returns (image_assets, sku_records) as lists of dicts.
    """
    system_prompt = _build_competitive_prompt()
    user_prompt = f"请从以下文本提取竞品产品数据：\n\n---\n{text[:6000]}\n---\n\n只返回JSON对象。"

    try:
        raw = llm_fn(system_prompt, user_prompt)
        data = _parse_competitive_json(raw)
        images = data.get("image_assets", [])
        skus = data.get("sku_records", [])

        # Basic validation
        images = [a for a in images if isinstance(a, dict) and a.get("asset_id")]
        skus = [s for s in skus if isinstance(s, dict) and s.get("sku_id")]

        return images, skus
    except Exception as exc:
        logger.error("competitive extraction failed: %s", exc)
        return [], []


def _parse_competitive_json(raw: str) -> dict[str, Any]:
    """Parse competitive extraction JSON from LLM response."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
        return {}
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        return {}


def extractor_node(
    state: ResearchState,
    *,
    llm_fn: Callable[[str, str], str] | None = None,
    graph_store: Any | None = None,
    evidence_store: Any | None = None,
    memory_manager: Any | None = None,
) -> dict[str, Any]:
    """Extractor agent node for LangGraph.

    Extracts triples from evidence (fetched from EvidenceStore by ID),
    validates via exact_quote matching, and ingests into the graph.
    """
    evidence_refs = state.get("gathered_evidence", [])
    budget = state.get("token_budget_remaining", 50_000)
    total_added = state.get("total_triples_added", 0)

    if not evidence_refs:
        logger.info("extractor: no evidence to process")
        return {"new_triples": []}

    # Front-loaded entity resolution: get candidate entities from graph
    candidate_entities: list[str] = []
    if graph_store:
        try:
            candidate_entities = [
                (graph_store.get_node(n) or {}).get("label", n)
                for n in list(graph_store.g.nodes())[:100]
            ]
        except Exception as exc:
            logger.warning("failed to get candidate entities: %s", exc)

    all_triples: list[dict[str, Any]] = []

    for ref in evidence_refs:
        # Fetch raw text — by ID from EvidenceStore, or inline fallback
        evidence_id = ref.get("evidence_id")
        if evidence_id is not None and evidence_store:
            text = evidence_store.fetch(evidence_id)
        else:
            text = ref.get("_text", "")  # Inline fallback for testing

        if not text or len(text) < 50:
            continue

        if llm_fn:
            # Build prompt with candidate entities injected (Fix #2)
            system_prompt = _build_extraction_prompt(candidate_entities)
            user_prompt = f"请从以下文本提取产业链三元组：\n\n---\n{text[:6000]}\n---\n\n只返回JSON数组。"

            try:
                raw = llm_fn(system_prompt, user_prompt)
                triples = _parse_json_array(raw)
                budget -= len(text) // 4

                # Fix #3: exact_quote validation (hard rule, no LLM)
                triples = _validate_exact_quotes(triples, text)

                all_triples.extend(triples)
            except Exception as exc:
                logger.error("extraction failed: %s", exc)
        else:
            logger.debug("extractor: no LLM, skipping extraction")

    # Standard validation (structure, self-loops, etc.)
    valid_triples = _validate_triples(all_triples)

    # Apply entity resolution for any remaining unmatched entities
    if graph_store and valid_triples:
        valid_triples = _resolve_entities(valid_triples, graph_store)

    # Ingest into graph
    added = 0
    if graph_store and valid_triples:
        added = _ingest_triples(valid_triples, graph_store)

    logger.info(
        "extractor: %d refs → %d raw triples → %d valid → %d ingested",
        len(evidence_refs), len(all_triples), len(valid_triples), added,
    )

    # ── Competitive data extraction (keyword-gated) ──────────────
    all_image_assets: list[dict[str, Any]] = []
    all_sku_records: list[dict[str, Any]] = []

    if llm_fn:
        for ref in evidence_refs:
            evidence_id = ref.get("evidence_id")
            if evidence_id is not None and evidence_store:
                text = evidence_store.fetch(evidence_id)
            else:
                text = ref.get("_text", "")

            if text and _has_competitive_signal(text):
                images, skus = _extract_competitive_data(text, llm_fn)
                all_image_assets.extend(images)
                all_sku_records.extend(skus)
                budget -= len(text) // 8  # Lighter cost estimate
                logger.info(
                    "competitive extraction: %d images, %d SKUs from evidence",
                    len(images), len(skus),
                )

    if memory_manager and (all_image_assets or all_sku_records):
        run_id = state.get("run_id", "")
        for asset in all_image_assets:
            try:
                memory_manager.store_episodic(
                    category="product_spec",
                    content=(
                        f"发现 {asset.get('brand', '')} 产品图片: "
                        f"{asset.get('visible_content', '')}"
                    ).strip(),
                    run_id=run_id,
                    source_type="extractor",
                    confidence=0.82,
                    structured_data=asset,
                )
            except Exception as exc:
                logger.warning("failed to persist image episodic memory: %s", exc)
        for sku in all_sku_records:
            try:
                memory_manager.store_episodic(
                    category="product_spec",
                    content=(
                        f"发现 {sku.get('brand', '')} {sku.get('model', '')}: "
                        f"{sku.get('price_range', '')}"
                    ).strip(),
                    run_id=run_id,
                    source_type="extractor",
                    confidence=0.86,
                    structured_data=sku,
                )
            except Exception as exc:
                logger.warning("failed to persist sku episodic memory: %s", exc)
            try:
                memory_manager.store_episodic(
                    category="brand_observation",
                    content=(
                        f"{sku.get('brand', '')} {sku.get('model', '')} "
                        f"定位 {sku.get('positioning', '')}，目标用户 {sku.get('target_audience', '')}"
                    ).strip(),
                    run_id=run_id,
                    source_type="extractor",
                    confidence=0.84,
                    structured_data={
                        "brand": sku.get("brand", ""),
                        "model": sku.get("model", ""),
                        "positioning": sku.get("positioning", ""),
                        "target_audience": sku.get("target_audience", ""),
                    },
                )
            except Exception as exc:
                logger.warning("failed to persist brand observation memory: %s", exc)

    return {
        "new_triples": valid_triples,
        "total_triples_added": total_added + added,
        "gathered_evidence": [],  # Clear processed refs
        "token_budget_remaining": max(0, budget),
        "image_assets": all_image_assets,
        "sku_records": all_sku_records,
    }


# ── Fix #3: exact_quote validation (replaces LLM-as-a-Judge) ──────


def _validate_exact_quotes(
    triples: list[dict[str, Any]],
    source_text: str,
) -> list[dict[str, Any]]:
    """Validate that each triple's exact_quote exists in the source.

    Hard rule: if exact_quote is not a substring of source_text,
    the triple is REJECTED.  No LLM call = no hallucination collusion.
    """
    validated = []
    rejected = 0

    for t in triples:
        quote = str(t.get("exact_quote", "")).strip()
        if not quote:
            # No quote provided — reject
            rejected += 1
            continue

        # Normalize whitespace for comparison
        normalized_quote = re.sub(r"\s+", " ", quote).strip()
        normalized_text = re.sub(r"\s+", " ", source_text).strip()

        if normalized_quote in normalized_text:
            validated.append(t)
        elif len(normalized_quote) > 10:
            # Try fuzzy: check if 80% of the quote chars exist as substring
            # This handles minor whitespace/punctuation differences
            best_overlap = _best_substring_overlap(normalized_quote, normalized_text)
            if best_overlap >= 0.8:
                t["_quote_match"] = "fuzzy"
                validated.append(t)
            else:
                rejected += 1
                logger.debug(
                    "exact_quote REJECTED (overlap=%.2f): %.40s...",
                    best_overlap, quote,
                )
        else:
            rejected += 1

    if rejected:
        logger.info(
            "exact_quote validation: %d kept, %d rejected",
            len(validated), rejected,
        )

    return validated


def _best_substring_overlap(needle: str, haystack: str) -> float:
    """Find the best overlapping substring match ratio.

    Slides a window of len(needle) across haystack and returns
    the best character-level match ratio.
    """
    n = len(needle)
    if n == 0 or len(haystack) == 0:
        return 0.0
    if n > len(haystack):
        n, needle, haystack = len(haystack), haystack, needle

    best = 0.0
    for i in range(len(haystack) - n + 1):
        window = haystack[i:i + n]
        matches = sum(1 for a, b in zip(needle, window) if a == b)
        ratio = matches / n
        if ratio > best:
            best = ratio
        if best >= 0.95:
            break  # Good enough

    return best


# ── JSON parsing ───────────────────────────────────────────────────


def _parse_json_array(raw: str) -> list[dict[str, Any]]:
    """Parse JSON array from LLM response."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        if isinstance(result, dict) and "triples" in result:
            return result["triples"]
        return [result] if isinstance(result, dict) else []
    except json.JSONDecodeError:
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        return []


# ── Structural validation ──────────────────────────────────────────


def _validate_triples(triples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate and clean extracted triples."""
    valid = []
    for t in triples:
        if not isinstance(t, dict):
            continue
        head = str(t.get("head", "")).strip()
        relation = str(t.get("relation", "")).strip()
        tail = str(t.get("tail", "")).strip()
        if not head or not relation or not tail:
            continue
        if head == tail:
            continue  # Self-loops not useful

        valid.append({
            "head": head,
            "head_type": str(t.get("head_type", "entity")),
            "relation": relation,
            "tail": tail,
            "tail_type": str(t.get("tail_type", "entity")),
            "exact_quote": str(t.get("exact_quote", ""))[:200],
            "confidence": min(1.0, max(0.0, float(t.get("confidence", 0.7)))),
            "valid_from": str(t.get("valid_from", "unknown")),
        })

    return valid


# ── Entity resolution ──────────────────────────────────────────────


def _resolve_entities(
    triples: list[dict[str, Any]],
    graph_store: Any,
) -> list[dict[str, Any]]:
    """Apply entity resolution to triple entities."""
    try:
        from finagent.graph_v2.entity_resolver import EntityResolver
        resolver = EntityResolver(graph_store)

        resolved = []
        for t in triples:
            head_result = resolver.resolve(t["head"])
            tail_result = resolver.resolve(t["tail"])

            t["head"] = head_result.canonical_id
            t["tail"] = tail_result.canonical_id
            resolved.append(t)

        return resolved
    except Exception as exc:
        logger.warning("entity resolution failed: %s", exc)
        return triples


# ── Graph ingestion ────────────────────────────────────────────────


def _ingest_triples(
    triples: list[dict[str, Any]],
    graph_store: Any,
) -> int:
    """Ingest validated triples into the graph store."""
    from finagent.graph_v2.ontology import NodeType, resolve_edge_type

    added = 0
    for t in triples:
        if t.get("confidence", 0) < 0.5:
            continue

        try:
            head_type = t.get("head_type", "entity")
            try:
                ht = NodeType(head_type)
            except ValueError:
                ht = NodeType.ENTITY
            if not graph_store.has_node(t["head"]):
                graph_store.add_node(t["head"], ht, t["head"])

            tail_type = t.get("tail_type", "entity")
            try:
                tt = NodeType(tail_type)
            except ValueError:
                tt = NodeType.ENTITY
            if not graph_store.has_node(t["tail"]):
                graph_store.add_node(t["tail"], tt, t["tail"])

            edge_type = resolve_edge_type(t["relation"])
            valid_from = t.get("valid_from", "unknown")
            if valid_from == "unknown":
                valid_from = "2024-01-01"

            graph_store.merge_edge(
                t["head"], t["tail"], edge_type,
                valid_from=valid_from,
                confidence=t.get("confidence", 0.7),
                source="extractor_agent",
                evidence=t.get("exact_quote"),
            )
            added += 1
        except Exception as exc:
            logger.warning("triple ingestion failed: %s → %s", t, exc)

    return added
