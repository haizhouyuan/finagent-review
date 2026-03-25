"""Canonical research contracts for finagent v2.

Defines the three core data objects that flow through the v2 research pipeline:

  ResearchRun    — lifecycle of a single research session
  ResearchPackage — immutable output of a completed run
  WritebackAction — mapping from package → old thesis OS objects

These are the "schema border" between v2 research engine and the rest
of the system. Everything downstream (writeback, finbot bridge, dashboards)
consumes these objects, not raw LangGraph state.

Design principles:
  - Serializable to JSON (for SQLite storage and IPC)
  - Immutable after creation (frozen dataclass or dict)
  - Evidence provenance mandatory (source_tier + source_uri + published_at)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ── Enums ────────────────────────────────────────────────────────────

class RunStatus(str, Enum):
    """Lifecycle states of a ResearchRun."""
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"           # checkpoint or manual pause
    AWAITING_HUMAN = "awaiting_human"  # HITL gate: waiting for review
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


class WritebackTarget(str, Enum):
    """Object types that a WritebackAction can target.

    v1 thesis OS types: THESIS, TARGET_CASE, WATCH_ITEM, DECISION, SOURCE
    v2 competitive types: ASSET_LEDGER, SKU_CATALOG
    """
    THESIS = "thesis"
    TARGET_CASE = "target_case"
    WATCH_ITEM = "watch_item"       # monitor
    DECISION = "decision"
    SOURCE = "source"
    # v2-native competitive research targets
    ASSET_LEDGER = "asset_ledger"
    SKU_CATALOG = "sku_catalog"


class WritebackOp(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    ARCHIVE = "archive"


class SourceTier(str, Enum):
    """Evidence source quality tiers — used by source policy gating."""
    PRIMARY = "primary"            # 一手来源：财报、公告、招股书、专利
    SECONDARY = "secondary"        # 二手来源：券商研报、行业分析
    AGGREGATED = "aggregated"      # 聚合来源：新闻、搜索结果、KOL 摘要
    UNVERIFIED = "unverified"      # 未验证来源


# ── ResearchRun ──────────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_run_id() -> str:
    return f"run-{uuid.uuid4().hex[:12]}"


@dataclass
class ResearchRun:
    """Lifecycle record of a single research session.

    Created at the start of `finagent-research research`, updated
    after each LangGraph node, finalized when the run completes.
    """
    run_id: str = field(default_factory=_new_run_id)
    goal: str = ""
    context: str = ""
    status: str = field(default=RunStatus.QUEUED.value)

    # Configuration snapshot (frozen at run start)
    llm_backend: str = "mock"
    max_iterations: int = 10
    token_budget: int = 50_000
    confidence_threshold: float = 0.85

    # Progress tracking
    current_iteration: int = 0
    total_triples: int = 0
    confidence_score: float = 0.0
    token_budget_remaining: int = 50_000
    termination_reason: str = ""

    # Timestamps
    created_at: str = field(default_factory=_utcnow)
    updated_at: str = field(default_factory=_utcnow)
    completed_at: str = ""

    # Error info
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ResearchRun":
        # Filter to only known fields
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ── Evidence Reference ───────────────────────────────────────────────

@dataclass(frozen=True)
class EvidenceRef:
    """A reference to a piece of stored evidence with full provenance.

    This is what flows through ResearchPackage — NOT raw text.
    The actual text lives in EvidenceStore, retrieved by evidence_id.
    """
    evidence_id: int | None = None
    query: str = ""
    char_count: int = 0
    source_type: str = "web_search"
    source_tier: str = SourceTier.UNVERIFIED.value
    source_uri: str = ""           # URL or file path
    published_at: str = ""         # ISO date of source publication

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "EvidenceRef":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ── ResearchPackage ──────────────────────────────────────────────────

@dataclass
class ResearchPackage:
    """Immutable output bundle of a completed research run.

    Contains everything needed for writeback, review, or export:
    - Structured triples with confidence + provenance
    - Evidence references (not raw text)
    - Image assets and SKU records (competitive research)
    - Graph topology snapshot
    - Generated report (markdown)
    - Blind spots remaining
    """
    run_id: str = ""
    goal: str = ""
    context: str = ""

    # Core outputs
    triples: list[dict[str, Any]] = field(default_factory=list)
    evidence_refs: list[EvidenceRef] = field(default_factory=list)
    report_md: str = ""

    # Competitive research outputs
    image_assets: list[Any] = field(default_factory=list)   # list[ImageAssetRef]
    sku_records: list[Any] = field(default_factory=list)     # list[SkuRecord]

    # Graph state snapshot
    graph_snapshot_path: str = ""    # path to exported graph JSON
    node_count: int = 0
    edge_count: int = 0

    # Quality metrics
    confidence: float = 0.0
    blind_spots: list[dict[str, Any]] = field(default_factory=list)
    iterations_used: int = 0
    token_cost_est: int = 0

    # Metadata
    created_at: str = field(default_factory=_utcnow)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # EvidenceRef / ImageAssetRef / SkuRecord are already dicts after asdict()
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ResearchPackage":
        d = dict(d)  # shallow copy
        # Reconstruct EvidenceRef objects from dicts
        if "evidence_refs" in d and d["evidence_refs"]:
            d["evidence_refs"] = [
                EvidenceRef.from_dict(r) if isinstance(r, dict) else r
                for r in d["evidence_refs"]
            ]
        # Reconstruct ImageAssetRef objects from dicts
        if "image_assets" in d and d["image_assets"]:
            d["image_assets"] = [
                ImageAssetRef.from_dict(r) if isinstance(r, dict) else r
                for r in d["image_assets"]
            ]
        # Reconstruct SkuRecord objects from dicts
        if "sku_records" in d and d["sku_records"]:
            d["sku_records"] = [
                SkuRecord.from_dict(r) if isinstance(r, dict) else r
                for r in d["sku_records"]
            ]
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ── WritebackAction ──────────────────────────────────────────────────

@dataclass
class WritebackAction:
    """A single write operation from ResearchPackage → old thesis OS.

    Generated by the apply bridge, reviewed (optionally) by human,
    then executed against the v1 database.
    """
    package_id: str = ""            # run_id of source package
    target_type: str = ""           # WritebackTarget enum value
    target_id: str = ""             # existing object ID (or "" for create)
    op: str = WritebackOp.CREATE.value
    payload: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0         # from source evidence
    source_evidence_ids: list[int] = field(default_factory=list)

    # Audit trail
    applied: bool = False
    applied_at: str = ""
    dry_run_result: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "WritebackAction":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ── Competitive Research Objects ─────────────────────────────────────

@dataclass(frozen=True)
class ImageAssetRef:
    """Reference to a product/structure image with provenance.

    Used by competitive research to track visual evidence:
    product photos, structure diagrams, wheel details, etc.
    """
    asset_id: str = ""
    brand: str = ""
    product_line: str = ""      # e.g. "Fz3 110", "朱雀二号"
    category: str = ""          # "exterior" | "structure" | "wheel" | "detail"
    source_url: str = ""
    local_path: str = ""        # relative to project root
    acquisition_date: str = ""
    is_official: bool = False   # from OEM website
    quality_grade: str = ""     # "high" | "medium" | "low"
    visible_content: str = ""   # what's visible in the image
    supports_conclusion: str = ""     # what analysis this supports
    prohibits_conclusion: str = ""    # what this CANNOT support

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ImageAssetRef":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


@dataclass(frozen=True)
class SkuRecord:
    """Structured product-level data for competitive analysis.

    Each record represents one SKU (brand + series + model)
    with evidence provenance for every field.
    """
    sku_id: str = ""
    brand: str = ""
    series: str = ""
    model: str = ""
    positioning: str = ""       # "通勤" | "中高端" | "运动" | "外卖"
    price_range: str = ""       # e.g. "4000-6000"
    wheel_diameter: str = ""    # e.g. "14寸"
    frame_type: str = ""        # "双管" | "一体钣金" | "铝合金"
    motor_type: str = ""
    battery_platform: str = ""
    brake_config: str = ""
    target_audience: str = ""
    style_tags: tuple[str, ...] = ()       # frozen-compatible
    evidence_sources: tuple[str, ...] = ()  # frozen-compatible

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["style_tags"] = list(d["style_tags"])
        d["evidence_sources"] = list(d["evidence_sources"])
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SkuRecord":
        d = dict(d)
        if "style_tags" in d and isinstance(d["style_tags"], list):
            d["style_tags"] = tuple(d["style_tags"])
        if "evidence_sources" in d and isinstance(d["evidence_sources"], list):
            d["evidence_sources"] = tuple(d["evidence_sources"])
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})
