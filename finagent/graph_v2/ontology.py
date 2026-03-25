"""Ontology definitions for the v2 knowledge graph.

Defines typed node and edge schemas with mandatory temporal fields,
confidence scoring, and evidence provenance.

Design principles:
  - Every edge MUST carry (valid_from, confidence, source) — no undated facts
  - Nodes have required vs optional attribute sets per type
  - Schema validation is strict: unknown fields are warnings, missing required
    fields are errors
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, ClassVar


# ── Node types ──────────────────────────────────────────────────────

class NodeType(str, Enum):
    """Core entity categories in the knowledge graph."""

    COMPANY = "company"                          # 公司 / 机构
    SPACE_SYSTEM = "space_system"                # 航天系统（火箭 / 卫星 / 星座）
    COMPONENT = "component"                      # 核心零部件 / 材料
    INFRASTRUCTURE = "infrastructure"            # 基础设施（发射场 / 测控站）
    TECHNOLOGY = "technology"                    # 核心技术 / 专利簇
    PROJECT = "project"                          # 重大工程 / 星座计划
    SECTOR = "sector"                            # 产业板块 / 赛道
    FINANCIAL_INSTRUMENT = "financial_instrument"  # 股票 / 基金 / 债券
    PERSON = "person"                            # 关键人物
    POLICY = "policy"                            # 政策 / 标准 / 法规
    # Catch-all for entities that don't fit above categories
    ENTITY = "entity"


# ── Edge types ──────────────────────────────────────────────────────

class EdgeType(str, Enum):
    """Relationship categories between nodes."""

    # Supply chain
    SUPPLIES_CORE_PART_TO = "supplies_core_part_to"    # 核心零部件供应
    LAUNCH_SERVICE_FOR = "launch_service_for"          # 发射服务
    CUSTOMER_OF = "customer_of"                        # 客户关系

    # Manufacturing & composition
    MANUFACTURES = "manufactures"                      # 制造
    COMPONENT_OF = "component_of"                      # 组件 / 子系统关系
    OPERATES = "operates"                              # 运营

    # Business
    BID_WON_CONTRACT = "bid_won_contract"              # 中标合同
    COMPETES_WITH = "competes_with"                    # 竞争
    PARTNERS_WITH = "partners_with"                    # 合作 / 联合体
    INVESTED_BY = "invested_by"                        # 投资 / 持股
    CONTROLS = "controls"                              # 控股 / 实控

    # Technology & enablement
    ENABLES = "enables"                                # 技术使能
    TECHNOLOGY_BENCHMARK = "technology_benchmark"      # 技术对标

    # Classification & membership
    BELONGS_TO = "belongs_to"                          # 板块归属
    REGULATES = "regulates"                            # 政策约束

    # Generic fallback (for LLM-extracted relations not yet categorized)
    RELATED_TO = "related_to"


# ── Edge type aliases for LLM extraction normalization ──────────────

_EDGE_ALIASES: dict[str, EdgeType] = {
    "supplies_to": EdgeType.SUPPLIES_CORE_PART_TO,
    "supply": EdgeType.SUPPLIES_CORE_PART_TO,
    "供应": EdgeType.SUPPLIES_CORE_PART_TO,
    "launch_service": EdgeType.LAUNCH_SERVICE_FOR,
    "customer": EdgeType.CUSTOMER_OF,
    "客户": EdgeType.CUSTOMER_OF,
    "manufacture": EdgeType.MANUFACTURES,
    "制造": EdgeType.MANUFACTURES,
    "competes": EdgeType.COMPETES_WITH,
    "competition": EdgeType.COMPETES_WITH,
    "竞争": EdgeType.COMPETES_WITH,
    "partners": EdgeType.PARTNERS_WITH,
    "partner": EdgeType.PARTNERS_WITH,
    "合作": EdgeType.PARTNERS_WITH,
    "invest": EdgeType.INVESTED_BY,
    "投资": EdgeType.INVESTED_BY,
    "控股": EdgeType.CONTROLS,
    "enable": EdgeType.ENABLES,
    "使能": EdgeType.ENABLES,
    "component": EdgeType.COMPONENT_OF,
    "组件": EdgeType.COMPONENT_OF,
    "belongs": EdgeType.BELONGS_TO,
    "属于": EdgeType.BELONGS_TO,
    "regulates": EdgeType.REGULATES,
    "约束": EdgeType.REGULATES,
    "related": EdgeType.RELATED_TO,
    "about": EdgeType.RELATED_TO,
}


def resolve_edge_type(raw: str) -> EdgeType:
    """Resolve a raw relation string to a canonical EdgeType.

    Tries exact match, then alias lookup, then name/value match.
    Falls back to RELATED_TO for unrecognized relations.
    """
    cleaned = raw.strip().lower()

    # Exact enum value match
    try:
        return EdgeType(cleaned)
    except ValueError:
        pass

    # Alias lookup
    if cleaned in _EDGE_ALIASES:
        return _EDGE_ALIASES[cleaned]

    # Name match (e.g. "SUPPLIES_CORE_PART_TO")
    for et in EdgeType:
        if et.name.lower() == cleaned:
            return et

    return EdgeType.RELATED_TO


# ── Attribute schemas ───────────────────────────────────────────────

@dataclass(frozen=True)
class NodeSchema:
    """Schema for node attributes by type."""

    node_type: NodeType
    required_attrs: frozenset[str] = field(default_factory=frozenset)
    optional_attrs: frozenset[str] = field(default_factory=frozenset)

    def validate(self, attrs: dict[str, Any]) -> list[str]:
        """Return list of validation errors (empty = valid)."""
        errors = []
        for req in self.required_attrs:
            if req not in attrs or attrs[req] in (None, ""):
                errors.append(f"missing required attr '{req}' for {self.node_type.value}")
        return errors


# Pre-defined schemas per node type
NODE_SCHEMAS: dict[NodeType, NodeSchema] = {
    NodeType.COMPANY: NodeSchema(
        node_type=NodeType.COMPANY,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({
            "ticker", "market", "sector", "founded", "parent",
            "listed", "military_qualification", "description",
            "aliases",
        }),
    ),
    NodeType.SPACE_SYSTEM: NodeSchema(
        node_type=NodeType.SPACE_SYSTEM,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({
            "system_subtype",  # launch_vehicle | satellite | constellation
            "operator", "scale", "status", "orbit_type",
            "payload_capacity_kg", "description",
        }),
    ),
    NodeType.COMPONENT: NodeSchema(
        node_type=NodeType.COMPONENT,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({
            "component_type",  # material | subsystem | module
            "trl", "supplier", "criticality", "description",
        }),
    ),
    NodeType.INFRASTRUCTURE: NodeSchema(
        node_type=NodeType.INFRASTRUCTURE,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({
            "location", "capacity", "operator", "description",
        }),
    ),
    NodeType.TECHNOLOGY: NodeSchema(
        node_type=NodeType.TECHNOLOGY,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({
            "trl", "patent_count", "key_holder", "description",
        }),
    ),
    NodeType.PROJECT: NodeSchema(
        node_type=NodeType.PROJECT,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({
            "operator", "scale", "status", "budget",
            "timeline", "description",
        }),
    ),
    NodeType.SECTOR: NodeSchema(
        node_type=NodeType.SECTOR,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({"parent_sector", "description"}),
    ),
    NodeType.FINANCIAL_INSTRUMENT: NodeSchema(
        node_type=NodeType.FINANCIAL_INSTRUMENT,
        required_attrs=frozenset({"label", "ticker"}),
        optional_attrs=frozenset({
            "instrument_type", "exchange", "currency", "description",
        }),
    ),
    NodeType.PERSON: NodeSchema(
        node_type=NodeType.PERSON,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({
            "title", "organization", "description",
        }),
    ),
    NodeType.POLICY: NodeSchema(
        node_type=NodeType.POLICY,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({
            "issuer", "effective_date", "scope", "description",
        }),
    ),
    NodeType.ENTITY: NodeSchema(
        node_type=NodeType.ENTITY,
        required_attrs=frozenset({"label"}),
        optional_attrs=frozenset({"description"}),
    ),
}


@dataclass(frozen=True)
class EdgeSchema:
    """Schema for edge attributes — all edges share this structure.

    Temporal fields (valid_from) are MANDATORY on every edge.
    This is the core design principle from FinDKG research.
    """

    # Mandatory fields
    REQUIRED: ClassVar[frozenset[str]] = frozenset({
        "edge_type",
        "valid_from",   # When this relationship became true
        "confidence",   # 0.0 - 1.0
        "source",       # provenance identifier
    })

    # Optional fields
    OPTIONAL: ClassVar[frozenset[str]] = frozenset({
        "valid_until",    # None = still active
        "evidence",       # supporting text snippet
        "evidence_url",   # URL to source document
        "contract_value", # monetary value if applicable
        "supply_share",   # percentage if supply relationship
        "notes",          # free-form annotation
    })

    @classmethod
    def validate(cls, attrs: dict[str, Any]) -> list[str]:
        """Validate edge attributes. Returns list of errors."""
        errors = []
        for req in cls.REQUIRED:
            if req not in attrs or attrs[req] in (None, ""):
                errors.append(f"missing required edge attr '{req}'")

        # Validate confidence range
        conf = attrs.get("confidence")
        if conf is not None:
            try:
                c = float(conf)
                if not 0.0 <= c <= 1.0:
                    errors.append(f"confidence must be 0.0-1.0, got {c}")
            except (ValueError, TypeError):
                errors.append(f"confidence must be numeric, got {conf!r}")

        # Validate edge_type is a known EdgeType
        et = attrs.get("edge_type")
        if et is not None:
            try:
                EdgeType(et)
            except ValueError:
                errors.append(f"unknown edge_type: {et!r}")

        return errors


# ── LLM extraction prompt components ───────────────────────────────

def ontology_prompt_block() -> str:
    """Generate prompt text describing the ontology for LLM extraction."""
    lines = [
        "## 知识图谱本体定义",
        "",
        "### 允许的节点类型：",
    ]
    for nt in NodeType:
        lines.append(f"- `{nt.value}`: {nt.name}")

    lines.extend([
        "",
        "### 允许的关系类型（edge_type 必须严格使用以下值）：",
    ])
    for et in EdgeType:
        lines.append(f"- `{et.value}`: {et.name}")

    lines.extend([
        "",
        "### 三元组输出格式：",
        "每个三元组必须包含：",
        "- head: 实体名称",
        "- head_type: 节点类型",
        "- relation: 关系类型（必须使用上述 edge_type 值）",
        "- tail: 实体名称",
        "- tail_type: 节点类型",
        "- evidence: 原文证据（不超过 80 字）",
        "- confidence: 0.5-1.0 的浮点数",
        "- valid_from: 关系生效日期 (YYYY-MM-DD 或 YYYY-MM)，如无法确定用 'unknown'",
    ])

    return "\n".join(lines)
