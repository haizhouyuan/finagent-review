"""Semantic promotion engine with strict schema contract."""

from __future__ import annotations

import re
from dataclasses import dataclass
from statistics import mean

from finagent.memory import (
    MemoryManager,
    MemoryTier,
    SEMANTIC_CATEGORIES,
)


@dataclass(frozen=True)
class SemanticCandidate:
    category: str
    conclusion: str
    evidence_ids: list[str]
    brands_involved: list[str]
    confidence: float
    valid_from: str
    supersedes: str | None = None


PROMOTION_RULES = {
    "brand_positioning": 3,
    "market_structure": 2,
    "price_band": 2,
    "technology_trend": 3,
    "supply_chain_map": 2,
    "regulatory": 1,
}

_BRAND_HINTS = ("雅迪", "爱玛", "九号", "台铃", "小牛", "新日", "绿源", "金谷", "春风")
_TECH_HINTS = ("石墨烯", "钠离子", "轮毂电机", "中置电机", "电池", "电机")
_REGULATORY_HINTS = ("国标", "政策", "监管", "标准", "GB17761")


def find_promotion_candidates(
    memory: MemoryManager,
    *,
    llm_fn=None,
) -> list[SemanticCandidate]:
    del llm_fn
    episodic = memory.recall("", tier=MemoryTier.EPISODIC, limit=500)
    if not episodic:
        return []

    candidates: list[SemanticCandidate] = []
    candidates.extend(_brand_positioning_candidates(memory, episodic))
    candidates.extend(_price_band_candidates(memory, episodic))
    candidates.extend(_supply_chain_candidates(memory, episodic))
    candidates.extend(_technology_candidates(memory, episodic))
    candidates.extend(_market_structure_candidates(memory, episodic))
    candidates.extend(_regulatory_candidates(memory, episodic))

    deduped: dict[tuple[str, str], SemanticCandidate] = {}
    for candidate in candidates:
        key = (candidate.category, candidate.conclusion)
        if key not in deduped or deduped[key].confidence < candidate.confidence:
            deduped[key] = candidate
    return list(deduped.values())


def execute_promotion(
    memory: MemoryManager,
    candidates: list[SemanticCandidate],
    *,
    dry_run: bool = True,
) -> list[str]:
    promoted_ids: list[str] = []
    episodic_ids = {
        record.record_id
        for record in memory.recall("", tier=MemoryTier.EPISODIC, limit=1000)
    }

    for candidate in candidates:
        if candidate.category not in SEMANTIC_CATEGORIES:
            continue
        if candidate.confidence < 0.8:
            continue
        if len(candidate.evidence_ids) < PROMOTION_RULES[candidate.category]:
            continue
        if not set(candidate.evidence_ids).issubset(episodic_ids):
            continue
        existing = [
            record for record in memory.get_by_category(
                candidate.category, tier=MemoryTier.SEMANTIC,
            )
            if record.content == candidate.conclusion
        ]
        if existing:
            promoted_ids.append(existing[0].record_id)
            continue

        if dry_run:
            promoted_ids.append(_preview_id(candidate))
            continue

        record_id = memory.promote_to_semantic(
            candidate.evidence_ids,
            candidate.conclusion,
            candidate.category,
            candidate.confidence,
            structured_data={
                "evidence_ids": candidate.evidence_ids,
                "brands_involved": candidate.brands_involved,
                "valid_from": candidate.valid_from,
                "supersedes": candidate.supersedes,
            },
            supersedes=candidate.supersedes,
        )
        promoted_ids.append(record_id)

    return promoted_ids


def _brand_positioning_candidates(memory: MemoryManager, episodic) -> list[SemanticCandidate]:
    grouped: dict[str, list] = {}
    for record in episodic:
        brand = _extract_brand(record)
        if not brand:
            continue
        grouped.setdefault(brand, []).append(record)

    candidates = []
    for brand, records in grouped.items():
        if len(records) < PROMOTION_RULES["brand_positioning"]:
            continue
        confidence = mean(record.confidence for record in records)
        if confidence < 0.8:
            continue
        conclusion = f"{brand} 在两轮车样本中持续出现，已形成稳定品牌定位。"
        candidates.append(_candidate_for(
            memory,
            category="brand_positioning",
            conclusion=conclusion,
            records=records,
            brands=[brand],
        ))
    return candidates


def _price_band_candidates(memory: MemoryManager, episodic) -> list[SemanticCandidate]:
    grouped: dict[str, list] = {}
    for record in episodic:
        data = record.structured_data
        brand = _extract_brand(record)
        if not brand or not data.get("price_range"):
            continue
        grouped.setdefault(brand, []).append(record)

    candidates = []
    for brand, records in grouped.items():
        if len(records) < PROMOTION_RULES["price_band"]:
            continue
        confidence = mean(record.confidence for record in records)
        if confidence < 0.8:
            continue
        low, high = _merge_price_ranges(
            str(record.structured_data.get("price_range", ""))
            for record in records
        )
        conclusion = f"{brand} 产品价格带稳定落在 {low}-{high} 元区间。"
        candidates.append(_candidate_for(
            memory,
            category="price_band",
            conclusion=conclusion,
            records=records,
            brands=[brand],
        ))
    return candidates


def _supply_chain_candidates(memory: MemoryManager, episodic) -> list[SemanticCandidate]:
    grouped: dict[tuple[str, str], list] = {}
    for record in episodic:
        data = record.structured_data
        supplier = str(data.get("supplier") or data.get("source_brand") or "").strip()
        customer = str(data.get("customer") or data.get("target_brand") or "").strip()
        if not supplier or not customer:
            if record.category != "supply_chain":
                continue
            brands = _extract_brands(record)
            if len(brands) < 2:
                continue
            supplier, customer = brands[0], brands[1]
        grouped.setdefault((supplier, customer), []).append(record)

    candidates = []
    for (supplier, customer), records in grouped.items():
        if len(records) < PROMOTION_RULES["supply_chain_map"]:
            continue
        confidence = mean(record.confidence for record in records)
        if confidence < 0.8:
            continue
        conclusion = f"{supplier} 与 {customer} 的供应链关联在试点样本中重复出现。"
        candidates.append(_candidate_for(
            memory,
            category="supply_chain_map",
            conclusion=conclusion,
            records=records,
            brands=[supplier, customer],
        ))
    return candidates


def _technology_candidates(memory: MemoryManager, episodic) -> list[SemanticCandidate]:
    grouped: dict[str, list] = {}
    for record in episodic:
        technology = str(record.structured_data.get("technology") or "").strip()
        if not technology:
            technology = _extract_tech(record.content)
        if not technology:
            continue
        grouped.setdefault(technology, []).append(record)

    candidates = []
    for technology, records in grouped.items():
        if len(records) < PROMOTION_RULES["technology_trend"]:
            continue
        confidence = mean(record.confidence for record in records)
        if confidence < 0.8:
            continue
        conclusion = f"{technology} 在两轮车样本中呈现持续技术趋势。"
        candidates.append(_candidate_for(
            memory,
            category="technology_trend",
            conclusion=conclusion,
            records=records,
            brands=sorted({brand for record in records for brand in _extract_brands(record)}),
        ))
    return candidates


def _market_structure_candidates(memory: MemoryManager, episodic) -> list[SemanticCandidate]:
    records = [
        record for record in episodic
        if record.category == "competitor_move" and len(_extract_brands(record)) >= 2
    ]
    if len(records) < PROMOTION_RULES["market_structure"]:
        return []
    confidence = mean(record.confidence for record in records)
    if confidence < 0.8:
        return []
    brands = sorted({brand for record in records for brand in _extract_brands(record)})
    conclusion = f"{'、'.join(brands[:5])} 之间形成稳定竞争格局。"
    return [_candidate_for(
        memory,
        category="market_structure",
        conclusion=conclusion,
        records=records,
        brands=brands,
    )]


def _regulatory_candidates(memory: MemoryManager, episodic) -> list[SemanticCandidate]:
    records = [
        record for record in episodic
        if any(hint in record.content for hint in _REGULATORY_HINTS)
    ]
    if len(records) < PROMOTION_RULES["regulatory"]:
        return []
    confidence = max(record.confidence for record in records)
    if confidence < 0.8:
        return []
    seed = records[0].content.split("。", 1)[0].strip()
    conclusion = seed or "监管约束在两轮车试点样本中持续有效。"
    return [_candidate_for(
        memory,
        category="regulatory",
        conclusion=conclusion,
        records=records[:1],
        brands=sorted({brand for record in records for brand in _extract_brands(record)}),
    )]


def _candidate_for(
    memory: MemoryManager,
    *,
    category: str,
    conclusion: str,
    records: list,
    brands: list[str],
) -> SemanticCandidate:
    supersedes = None
    for semantic in memory.get_by_category(category, tier=MemoryTier.SEMANTIC):
        if _overlaps(semantic.structured_data.get("brands_involved", []), brands):
            supersedes = semantic.record_id
            break
    return SemanticCandidate(
        category=category,
        conclusion=conclusion,
        evidence_ids=[record.record_id for record in records],
        brands_involved=brands,
        confidence=round(mean(record.confidence for record in records), 3),
        valid_from=min(record.created_at[:10] for record in records),
        supersedes=supersedes,
    )


def _extract_brand(record) -> str:
    data = record.structured_data
    brand = str(data.get("brand") or "").strip()
    if brand:
        return brand
    for hint in _BRAND_HINTS:
        if hint in record.content:
            return hint
    return ""


def _extract_brands(record) -> list[str]:
    brands_involved = record.structured_data.get("brands_involved")
    if isinstance(brands_involved, list):
        cleaned = [str(item).strip() for item in brands_involved if str(item).strip()]
        if cleaned:
            return cleaned
    brand = _extract_brand(record)
    if brand:
        return [brand]
    return [hint for hint in _BRAND_HINTS if hint in record.content]


def _extract_tech(content: str) -> str:
    for hint in _TECH_HINTS:
        if hint in content:
            return hint
    return ""


def _merge_price_ranges(price_ranges) -> tuple[int, int]:
    lows: list[int] = []
    highs: list[int] = []
    for value in price_ranges:
        numbers = [int(part) for part in re.findall(r"\d+", value)]
        if not numbers:
            continue
        lows.append(numbers[0])
        highs.append(numbers[-1])
    if not lows:
        return 0, 0
    return min(lows), max(highs)


def _preview_id(candidate: SemanticCandidate) -> str:
    base = f"{candidate.category}:{candidate.conclusion}"
    total = sum(ord(ch) for ch in base)
    return f"preview-{total:08x}"


def _overlaps(existing: list[str] | tuple[str, ...], current: list[str]) -> bool:
    return bool(set(existing or []) & set(current))
