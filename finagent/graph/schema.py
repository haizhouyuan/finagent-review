from __future__ import annotations

import json
import re
from enum import Enum
from typing import Any

import networkx as nx


class NodeType(str, Enum):
    # --- Claim verification layer (existing) ---
    CLAIM = "claim"
    ENTITY = "entity"
    EVIDENCE = "evidence"
    REVIEW = "review"
    THESIS = "thesis"
    # --- Industry chain layer (new) ---
    COMPANY = "company"           # 公司 (蓝箭航天, 长光卫星)
    PRODUCT_LINE = "product_line" # 产品线 (液氧甲烷发动机, SAR卫星)
    MATERIAL = "material"         # 核心原材料 (T800碳纤维, 特种合金)
    PROJECT = "project"           # 重大工程 (G60星链, 千帆星座)
    TECHNOLOGY = "technology"     # 核心技术 (可回收火箭, 相控阵天线)
    SUBSYSTEM = "subsystem"       # 子系统 (姿控系统, 星载通信载荷)
    STANDARD = "standard"         # 标准/政策 (GJB, 商业航天牌照)
    SECTOR = "sector"             # 赛道/板块 (卫星互联网, 发射服务)


class EdgeType(str, Enum):
    # --- Claim verification layer (existing) ---
    ABOUT = "about"
    SUPPORTED_BY = "supported_by"
    REVIEWED_BY = "reviewed_by"
    DEPENDS_ON = "depends_on"
    CONFLICTS_WITH = "conflicts_with"
    # --- Industry chain layer (new) ---
    SUPPLIES_TO = "supplies_to"       # A 供应给 B
    CUSTOMER_OF = "customer_of"       # A 是 B 的客户
    COMPETES_WITH = "competes_with"   # A 与 B 竞争
    MANUFACTURES = "manufactures"     # A 制造 B(产品)
    COMPONENT_OF = "component_of"     # A 是 B 的组件/子系统
    ENABLES = "enables"               # 技术使能关系
    REGULATES = "regulates"           # 标准/政策约束
    INVESTED_BY = "invested_by"       # 资本关系
    PARTNERS_WITH = "partners_with"   # 合作/联合体
    BELONGS_TO = "belongs_to"         # 公司隶属板块/赛道


ENTITY_PATTERNS = {
    "cows": [r"\bcowos\b", "先进封装"],
    "hbm": [r"\bhbm\d*\b", "高带宽内存"],
    "dram": [r"\bddr[45]\b", r"\bdram\b", "内存"],
    "nand": [r"\bnand\b", "闪存"],
    "cxmt": [r"\bcxmt\b", "长鑫"],
    "mu": [r"\bmu\b", "micron", "美光"],
    "sk_hynix": [r"\bhynix\b", "海力士"],
    "samsung": [r"\bsamsung\b", "三星"],
    "naura": [r"\bnaura\b", "北方华创"],
    "sic": [r"\bsic\b", "碳化硅"],
    "gan": [r"\bgan\b", "氮化镓"],
}

POSITIVE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"扩产|增长|上升|短缺|稀缺|涨价|改善|accelerat|surge|expand|tight|bullish|看好",
    )
]
STRONG_NEGATIVE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"缓解.*短缺|短缺.*缓解|稀缺溢价.*难以持续|premium.*fade|tightness.*ease",
    )
]
NEGATIVE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"下跌|缓解|过剩|放缓|恶化|库存|oversupply|declin|fall|bearish|风险|看错",
    )
]


def _json_load(value: str | None) -> Any:
    if not value:
        return []
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return []


def infer_entities(text: str) -> list[str]:
    lowered = text.lower()
    matches: list[str] = []
    for entity_id, patterns in ENTITY_PATTERNS.items():
        for pattern in patterns:
            if pattern.startswith(r"\b"):
                if re.search(pattern, lowered, re.IGNORECASE):
                    matches.append(entity_id)
                    break
            elif pattern.lower() in lowered:
                matches.append(entity_id)
                break
    return sorted(set(matches))


def infer_direction(text: str) -> str:
    lowered = text.lower()
    if any(pattern.search(lowered) for pattern in STRONG_NEGATIVE_PATTERNS):
        return "negative"
    positive = sum(1 for pattern in POSITIVE_PATTERNS if pattern.search(lowered))
    negative = sum(1 for pattern in NEGATIVE_PATTERNS if pattern.search(lowered))
    if positive and negative:
        return "mixed"
    if positive:
        return "positive"
    if negative:
        return "negative"
    return "neutral"


def build_graph_from_rows(
    claims: list[dict[str, Any]],
    theses: list[dict[str, Any]],
    reviews: list[dict[str, Any]],
    *,
    thesis_claim_map: dict[str, list[str]] | None = None,
) -> nx.DiGraph:
    graph = nx.DiGraph()
    review_by_claim: dict[str, list[dict[str, Any]]] = {}
    for review in reviews:
        for claim_id in _json_load(review.get("claim_ids_json")):
            review_by_claim.setdefault(str(claim_id), []).append(review)

    for thesis in theses:
        thesis_id = str(thesis["thesis_id"])
        graph.add_node(thesis_id, node_type=NodeType.THESIS.value, title=thesis.get("title", ""))

    for claim in claims:
        claim_id = str(claim["claim_id"])
        graph.add_node(
            claim_id,
            node_type=NodeType.CLAIM.value,
            text=claim.get("claim_text", ""),
            claim_type=claim.get("claim_type", ""),
            review_status=claim.get("review_status", "unreviewed"),
            direction=infer_direction(claim.get("claim_text", "")),
        )
        artifact_id = str(claim.get("artifact_id", ""))
        if artifact_id:
            evidence_id = f"evidence::{artifact_id}"
            graph.add_node(evidence_id, node_type=NodeType.EVIDENCE.value, artifact_id=artifact_id)
            graph.add_edge(claim_id, evidence_id, edge_type=EdgeType.SUPPORTED_BY.value)
        for entity_id in infer_entities(claim.get("claim_text", "")):
            graph.add_node(entity_id, node_type=NodeType.ENTITY.value, entity_id=entity_id)
            graph.add_edge(claim_id, entity_id, edge_type=EdgeType.ABOUT.value)
        for review in review_by_claim.get(claim_id, []):
            review_id = str(review["review_id"])
            graph.add_node(review_id, node_type=NodeType.REVIEW.value, result=review.get("result", ""))
            graph.add_edge(claim_id, review_id, edge_type=EdgeType.REVIEWED_BY.value)

    if thesis_claim_map:
        for thesis_id, claim_ids in thesis_claim_map.items():
            for claim_id in claim_ids:
                if graph.has_node(thesis_id) and graph.has_node(claim_id):
                    graph.add_edge(thesis_id, claim_id, edge_type=EdgeType.DEPENDS_ON.value)
    return graph
