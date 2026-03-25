"""Event mining engine for finagent.

Production slice responsibilities:

- validate and normalize extracted event drafts
- validate and sync sentinel specs
- persist append-only event ledger entries
- maintain state projections and opportunity candidates
- emit missing-milestone stalled events
- provide a prompt builder for external LLM extraction
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import yaml  # type: ignore

from .db import insert_event, list_rows, select_one
from .sector_grammars import get_sector_grammar, grammar_prompt_lines
from .source_policy import (
    SOURCE_ROLES as SOURCE_POLICY_ROLES,
    SOURCE_ROLE_TO_TIER as SOURCE_POLICY_ROLE_TO_TIER,
    SOURCE_TIERS as SOURCE_POLICY_TIERS,
    source_policy_prompt_lines,
)
from .utils import json_dumps, stable_id, utc_now_iso

SCHEMA_VERSION = "3.0"
DEFAULT_SPEC_PATHS = ("specs/sentinel_v2.yaml", "specs/sentinel_v1.yaml", "specs/sentinel_v0.yaml")

STAGE_SEQUENCE = [
    "concept",
    "early_prototype",
    "prototype",
    "sample",
    "customer_validation",
    "qualification",
    "pilot",
    "first_commercial_shipment",
    "repeat_order",
    "capacity_expansion",
    "mass_adoption",
]
STAGES = frozenset(STAGE_SEQUENCE)
FINAL_STAGES = frozenset({"mass_adoption"})

EVENT_TYPES = frozenset([
    "product_milestone",
    "competition",
    "customer",
    "macro",
    "financial",
    "stalled",
    "candidate",
])

SOURCE_ROLES = frozenset(SOURCE_POLICY_ROLES)
SOURCE_TIERS = frozenset(SOURCE_POLICY_TIERS)
SOURCE_ROLE_TO_TIER = dict(SOURCE_POLICY_ROLE_TO_TIER)

NOVELTY_LEVELS = frozenset(["high", "medium", "low"])
RELEVANCE_LEVELS = frozenset(["direct", "adjacent", "peripheral"])
CONFIDENCE_LEVELS = frozenset(["high", "medium", "low"])

BUCKET_ROLES = frozenset(["core", "option", "constraint", "alternative"])
ENTITY_ROLES = frozenset(["tracked", "competitor", "customer", "upstream", "benchmark"])
RESIDUAL_CLASSES = frozenset(["watch", "adjacent", "frontier"])
ANTI_THESIS_STATUSES = frozenset(["clear", "due", "recorded", "dismissed"])
FEEDBACK_TYPES = frozenset([
    "interrupt_verdict",
    "review_verdict",
    "candidate_verdict",
    "anti_thesis_verdict",
    "missed_signal",
])
FEEDBACK_VERDICTS = frozenset([
    "true_positive",
    "false_positive",
    "promote",
    "defer",
    "dismiss",
    "missed",
    "resolved",
])

TRIGGER_CODES = frozenset([
    "F1", "F2", "F3",
    "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8",
    "V1", "V2", "V3",
    "M1",
])

INTERRUPT_TRIGGERS = frozenset(["F1", "F3", "M1"])
REVIEW_TRIGGERS = frozenset(["F2", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "V1", "V2", "V3"])
ROUTES = frozenset(["interrupt", "review", "opportunity", "archive"])
POSITIVE_IMPACT_HINTS = ("positive", "upside", "improve", "enabler", "acceleration", "adoption", "expansion", "growth")

DRAFT_FIELDS = frozenset([
    "entity",
    "product",
    "event_type",
    "stage_from",
    "stage_to",
    "source_role",
    "source_tier",
    "evidence_text",
    "evidence_url",
    "evidence_date",
    "event_time",
    "novelty",
    "relevance",
    "impact",
    "confidence",
    "mapped_trigger",
    "candidate_thesis",
    "event_id",
    "schema_version",
    "first_seen_time",
    "processed_time",
    "root_claim_key",
    "independence_group",
])

REQUIRED_DRAFT_FIELDS = frozenset([
    "entity",
    "event_type",
    "source_role",
    "evidence_text",
    "novelty",
    "relevance",
    "impact",
    "confidence",
])

SPEC_ENTRY_FIELDS = frozenset([
    "sentinel_id",
    "entity",
    "product",
    "bucket_role",
    "entity_role",
    "linked_thesis_id",
    "linked_target_case_id",
    "grammar_key",
    "current_stage",
    "stage_entered_at",
    "current_confidence",
    "expected_next_stage",
    "expected_by",
    "evidence_text",
    "evidence_url",
    "evidence_date",
    "source_role",
    "trigger_code",
    "notes",
    "adjacency_terms",
    "anti_thesis_focus",
])


def _canonical_claim_text(raw: str) -> str:
    lowered = re.sub(r"https?://\S+", " ", raw.strip().lower())
    lowered = re.sub(r"[\W_]+", " ", lowered, flags=re.UNICODE)
    return re.sub(r"\s+", " ", lowered).strip()


def _root_claim_key_from_event(event: dict[str, Any]) -> str:
    return stable_id(
        "claim",
        "::".join(
            [
                str(event.get("entity") or ""),
                str(event.get("product") or ""),
                str(event.get("event_type") or ""),
                str(event.get("stage_from") or ""),
                str(event.get("stage_to") or ""),
                str(event.get("evidence_date") or ""),
                _canonical_claim_text(str(event.get("evidence_text") or "")),
            ]
        ),
    )


def _source_tier_for_event(event: dict[str, Any]) -> str:
    explicit = str(event.get("source_tier") or "").strip()
    if explicit:
        return explicit
    return SOURCE_ROLE_TO_TIER.get(str(event.get("source_role") or ""), "tertiary")


def _independence_group_from_event(event: dict[str, Any]) -> str:
    explicit = str(event.get("independence_group") or "").strip()
    if explicit:
        return explicit
    return str(event.get("root_claim_key") or _root_claim_key_from_event(event))


def _is_positive_impact(impact: str) -> bool:
    lowered = impact.lower()
    return any(token in lowered for token in POSITIVE_IMPACT_HINTS)


def _event_context_blob(event: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in [
            str(event.get("entity") or ""),
            str(event.get("product") or ""),
            str(event.get("candidate_thesis") or ""),
            str(event.get("impact") or ""),
            str(event.get("evidence_text") or ""),
        ]
        if part
    ).lower()


def _projection_terms(row: sqlite3.Row | dict[str, Any]) -> set[str]:
    record = dict(row) if isinstance(row, sqlite3.Row) else dict(row)
    notes = record.get("notes_json")
    payload = {}
    if notes:
        try:
            payload = json.loads(notes)
        except json.JSONDecodeError:
            payload = {}
    values: list[str] = [
        str(record.get("entity") or ""),
        str(record.get("product") or ""),
        str(record.get("grammar_key") or ""),
    ]
    adjacency_terms = payload.get("adjacency_terms") or payload.get("adjacent_terms") or []
    if isinstance(adjacency_terms, list):
        values.extend(str(item) for item in adjacency_terms if item)
    anti_terms = payload.get("anti_thesis_focus") or []
    if isinstance(anti_terms, list):
        values.extend(str(item) for item in anti_terms if item)
    text = " ".join(values).lower()
    ascii_tokens = {tok for tok in re.findall(r"[a-z0-9_+/.-]{2,}", text)}
    cjk_tokens = {tok for tok in re.findall(r"[\u4e00-\u9fff]{2,8}", text)}
    return ascii_tokens | cjk_tokens


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    candidate = raw.strip()
    if not candidate:
        return None
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        parsed_date = _parse_date(candidate)
        if parsed_date is None:
            return None
        dt = datetime.combine(parsed_date, time.min)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _iso_or_now(raw: str | None) -> str:
    dt = _parse_dt(raw)
    if dt is None:
        return utc_now_iso()
    return dt.isoformat()


def _event_time_from_draft(event: dict[str, Any]) -> str:
    if event.get("event_time"):
        return _iso_or_now(str(event["event_time"]))
    if event.get("evidence_date"):
        raw = str(event["evidence_date"])
        parsed_date = _parse_date(raw)
        if parsed_date is not None:
            return datetime.combine(parsed_date, time.min, tzinfo=timezone.utc).isoformat()
    return utc_now_iso()


def _route_from_trigger(mapped_trigger: str | None, candidate_thesis: str | None) -> str:
    if candidate_thesis:
        return "opportunity"
    if mapped_trigger in INTERRUPT_TRIGGERS:
        return "interrupt"
    if mapped_trigger in REVIEW_TRIGGERS:
        return "review"
    return "archive"


def _next_stage(stage: str | None) -> str | None:
    if not stage or stage not in STAGES:
        return None
    idx = STAGE_SEQUENCE.index(stage)
    if idx >= len(STAGE_SEQUENCE) - 1:
        return None
    return STAGE_SEQUENCE[idx + 1]


def _projection_id(entity: str, product: str | None, entity_role: str, bucket_role: str | None) -> str:
    return stable_id("projection", "::".join([entity, product or "", entity_role, bucket_role or ""]))


def _candidate_id(thesis_name: str) -> str:
    return stable_id("candidate", thesis_name)


def validate_event_draft(event: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    extra = set(event.keys()) - DRAFT_FIELDS
    if extra:
        errors.append(f"extra fields: {sorted(extra)}")

    for field in REQUIRED_DRAFT_FIELDS:
        if event.get(field) in {None, ""}:
            errors.append(f"{field} is required")

    if event.get("event_type") and event["event_type"] not in EVENT_TYPES:
        errors.append(f"invalid event_type: {event['event_type']!r}")
    if event.get("source_role") and event["source_role"] not in SOURCE_ROLES:
        errors.append(f"invalid source_role: {event['source_role']!r}")
    if event.get("source_tier") and event["source_tier"] not in SOURCE_TIERS:
        errors.append(f"invalid source_tier: {event['source_tier']!r}")
    if event.get("novelty") and event["novelty"] not in NOVELTY_LEVELS:
        errors.append(f"invalid novelty: {event['novelty']!r}")
    if event.get("relevance") and event["relevance"] not in RELEVANCE_LEVELS:
        errors.append(f"invalid relevance: {event['relevance']!r}")
    if event.get("confidence") and event["confidence"] not in CONFIDENCE_LEVELS:
        errors.append(f"invalid confidence: {event['confidence']!r}")
    if event.get("mapped_trigger") and event["mapped_trigger"] not in TRIGGER_CODES:
        errors.append(f"invalid mapped_trigger: {event['mapped_trigger']!r}")

    for field in ("stage_from", "stage_to"):
        val = event.get(field)
        if val is not None and val not in STAGES:
            errors.append(f"invalid {field}: {val!r}")

    stage_from = event.get("stage_from")
    stage_to = event.get("stage_to")
    if (stage_from is None) != (stage_to is None):
        errors.append("stage_from and stage_to must be both null or both non-null")
    if not event.get("root_claim_key"):
        errors.append("root_claim_key is required")
    if not event.get("independence_group"):
        errors.append("independence_group is required")

    return errors


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    now_iso = utc_now_iso()
    source_tier = _source_tier_for_event(event)
    root_claim_key = str(event.get("root_claim_key") or _root_claim_key_from_event(event))
    normalized = {
        "schema_version": str(event.get("schema_version") or SCHEMA_VERSION),
        "event_id": str(
            event.get("event_id")
            or stable_id(
                "evt",
                "::".join(
                    [
                        str(event.get("entity") or ""),
                        str(event.get("product") or ""),
                        str(event.get("event_type") or ""),
                        str(event.get("evidence_text") or ""),
                        _event_time_from_draft(event),
                    ]
                ),
            )
        ),
        "entity": str(event.get("entity") or ""),
        "product": event.get("product"),
        "event_type": str(event.get("event_type") or ""),
        "stage_from": event.get("stage_from"),
        "stage_to": event.get("stage_to"),
        "source_role": str(event.get("source_role") or ""),
        "source_tier": source_tier,
        "root_claim_key": root_claim_key,
        "independence_group": str(event.get("independence_group") or stable_id("indep", root_claim_key)),
        "evidence_text": str(event.get("evidence_text") or ""),
        "evidence_url": event.get("evidence_url"),
        "evidence_date": event.get("evidence_date"),
        "event_time": _event_time_from_draft(event),
        "first_seen_time": _iso_or_now(str(event.get("first_seen_time") or now_iso)),
        "processed_time": _iso_or_now(str(event.get("processed_time") or now_iso)),
        "novelty": str(event.get("novelty") or ""),
        "relevance": str(event.get("relevance") or ""),
        "impact": str(event.get("impact") or ""),
        "confidence": str(event.get("confidence") or ""),
        "mapped_trigger": event.get("mapped_trigger"),
        "candidate_thesis": event.get("candidate_thesis"),
    }
    return normalized


def validate_event(event: dict[str, Any]) -> list[str]:
    errors = validate_event_draft(event)
    if event.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version mismatch: expected {SCHEMA_VERSION!r}, got {event.get('schema_version')!r}")
    for field in ("event_time", "first_seen_time", "processed_time"):
        if not event.get(field):
            errors.append(f"{field} is required")
        elif _parse_dt(str(event.get(field))) is None:
            errors.append(f"{field} must be ISO datetime")
    if not event.get("event_id"):
        errors.append("event_id is required")
    return errors


def route_event(event: dict[str, Any]) -> str:
    source_tier = str(event.get("source_tier") or _source_tier_for_event(event))
    residual_class = str(event.get("residual_class") or ("frontier" if event.get("candidate_thesis") else "watch"))
    can_project_state = bool(event.get("state_applied", True))
    candidate = event.get("candidate_thesis")
    trigger = event.get("mapped_trigger")
    if candidate and residual_class in {"adjacent", "frontier"}:
        return "opportunity"
    if trigger in INTERRUPT_TRIGGERS and source_tier != "tertiary" and can_project_state:
        return "interrupt"
    if trigger in REVIEW_TRIGGERS:
        return "review"
    if event.get("event_type") == "stalled":
        return "review"
    if residual_class == "adjacent":
        return "review"
    if event.get("stage_from") and event.get("stage_to") and event.get("relevance") in {"direct", "adjacent"}:
        return "review"
    return "archive"


GOLDEN_FIXTURES = [
    {
        "schema_version": SCHEMA_VERSION,
        "event_id": "fixture-001",
        "entity": "金盘科技",
        "product": "AIDC 传统供配电",
        "event_type": "financial",
        "stage_from": None,
        "stage_to": None,
        "source_role": "company_filing",
        "evidence_text": "例行公告，无状态变化",
        "evidence_url": "https://cninfo.com.cn/example",
        "evidence_date": "2026-04-15",
        "event_time": "2026-04-15T00:00:00+00:00",
        "first_seen_time": "2026-04-15T10:00:00+08:00",
        "processed_time": "2026-04-15T10:01:00+08:00",
        "novelty": "low",
        "relevance": "direct",
        "impact": "neutral",
        "confidence": "high",
        "mapped_trigger": None,
        "candidate_thesis": None,
        "_expected_route": "archive",
        "_label": "no_change",
    },
    {
        "schema_version": SCHEMA_VERSION,
        "event_id": "fixture-002",
        "entity": "金盘科技",
        "product": "10kV/2.4MW SST",
        "event_type": "product_milestone",
        "stage_from": "prototype",
        "stage_to": "sample",
        "source_role": "company_filing",
        "evidence_text": "SST 进入送样",
        "evidence_url": "https://cninfo.com.cn/example2",
        "evidence_date": "2026-04-15",
        "event_time": "2026-04-15T00:00:00+00:00",
        "first_seen_time": "2026-04-15T10:00:00+08:00",
        "processed_time": "2026-04-15T10:01:00+08:00",
        "novelty": "high",
        "relevance": "direct",
        "impact": "option_thesis_positive",
        "confidence": "high",
        "mapped_trigger": "B1",
        "candidate_thesis": None,
        "_expected_route": "review",
        "_label": "stage_transition",
    },
    {
        "schema_version": SCHEMA_VERSION,
        "event_id": "fixture-003",
        "entity": "Eaton",
        "product": "SST (via Resilient)",
        "event_type": "competition",
        "stage_from": "early_prototype",
        "stage_to": "first_commercial_shipment",
        "source_role": "competitor_pr",
        "evidence_text": "Eaton 宣布 SST 商业交付",
        "evidence_url": "https://eaton.com/example",
        "evidence_date": "2026-04-15",
        "event_time": "2026-04-15T00:00:00+00:00",
        "first_seen_time": "2026-04-15T10:00:00+08:00",
        "processed_time": "2026-04-15T10:01:00+08:00",
        "novelty": "high",
        "relevance": "direct",
        "impact": "thesis_negative",
        "confidence": "high",
        "mapped_trigger": "F1",
        "candidate_thesis": None,
        "_expected_route": "interrupt",
        "_label": "competitor_sst",
    },
    {
        "schema_version": SCHEMA_VERSION,
        "event_id": "fixture-004",
        "entity": "Wolfspeed",
        "product": "10kV SiC MOSFET",
        "event_type": "candidate",
        "stage_from": None,
        "stage_to": None,
        "source_role": "media",
        "evidence_text": "多家 SiC 供应链扩产",
        "evidence_url": "https://example.com/wolfspeed",
        "evidence_date": "2026-04-15",
        "event_time": "2026-04-15T00:00:00+00:00",
        "first_seen_time": "2026-04-15T10:00:00+08:00",
        "processed_time": "2026-04-15T10:01:00+08:00",
        "novelty": "high",
        "relevance": "adjacent",
        "impact": "upstream_enabler",
        "confidence": "medium",
        "mapped_trigger": None,
        "candidate_thesis": "SiC_supply_chain_maturation",
        "_expected_route": "opportunity",
        "_label": "candidate_theme",
    },
    {
        "schema_version": SCHEMA_VERSION,
        "event_id": "fixture-005",
        "entity": "金盘科技",
        "product": "AIDC 传统供配电",
        "event_type": "financial",
        "stage_from": None,
        "stage_to": None,
        "source_role": "company_filing",
        "evidence_text": "经营现金流恶化",
        "evidence_url": "https://cninfo.com.cn/example3",
        "evidence_date": "2026-04-15",
        "event_time": "2026-04-15T00:00:00+00:00",
        "first_seen_time": "2026-04-15T10:00:00+08:00",
        "processed_time": "2026-04-15T10:01:00+08:00",
        "novelty": "high",
        "relevance": "direct",
        "impact": "constraint_negative",
        "confidence": "high",
        "mapped_trigger": "V2",
        "candidate_thesis": None,
        "_expected_route": "review",
        "_label": "constraint_breach",
    },
    {
        "schema_version": SCHEMA_VERSION,
        "event_id": "fixture-006",
        "entity": "Microsoft",
        "product": None,
        "event_type": "macro",
        "stage_from": None,
        "stage_to": None,
        "source_role": "company_filing",
        "evidence_text": "Hyperscaler capex 下调",
        "evidence_url": "https://microsoft.com/ir",
        "evidence_date": "2026-04-15",
        "event_time": "2026-04-15T00:00:00+00:00",
        "first_seen_time": "2026-04-15T10:00:00+08:00",
        "processed_time": "2026-04-15T10:01:00+08:00",
        "novelty": "high",
        "relevance": "adjacent",
        "impact": "thesis_negative",
        "confidence": "high",
        "mapped_trigger": "F3",
        "candidate_thesis": None,
        "_expected_route": "interrupt",
        "_label": "macro_capex_cut",
    },
    {
        "schema_version": SCHEMA_VERSION,
        "event_id": "fixture-007",
        "entity": "金盘科技",
        "product": None,
        "event_type": "financial",
        "stage_from": None,
        "stage_to": None,
        "source_role": "media",
        "evidence_text": "无关噪音",
        "evidence_url": "https://xueqiu.com/example",
        "evidence_date": "2026-04-15",
        "event_time": "2026-04-15T00:00:00+00:00",
        "first_seen_time": "2026-04-15T10:00:00+08:00",
        "processed_time": "2026-04-15T10:01:00+08:00",
        "novelty": "low",
        "relevance": "peripheral",
        "impact": "neutral",
        "confidence": "low",
        "mapped_trigger": None,
        "candidate_thesis": None,
        "_expected_route": "archive",
        "_label": "noise",
    },
]


def validate_fixtures() -> dict[str, Any]:
    results = []
    pass_count = 0
    fail_count = 0
    for fixture in GOLDEN_FIXTURES:
        label = fixture["_label"]
        expected_route = fixture["_expected_route"]
        event = normalize_event({k: v for k, v in fixture.items() if not k.startswith("_")})
        errors = validate_event(event)
        actual_route = route_event(event)
        passed = len(errors) == 0 and actual_route == expected_route
        if passed:
            pass_count += 1
        else:
            fail_count += 1
        results.append(
            {
                "label": label,
                "schema_valid": len(errors) == 0,
                "schema_errors": errors,
                "expected_route": expected_route,
                "actual_route": actual_route,
                "route_match": actual_route == expected_route,
                "passed": passed,
            }
        )
    return {"total": len(GOLDEN_FIXTURES), "passed": pass_count, "failed": fail_count, "results": results}


def load_sentinel_spec(spec_path: str | Path) -> dict[str, Any]:
    path = Path(spec_path)
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("sentinel spec must be a mapping")
    if "schema_version" not in data:
        data["schema_version"] = SCHEMA_VERSION
    if "sentinel" not in data:
        raise ValueError("sentinel spec missing top-level 'sentinel'")
    if not isinstance(data["sentinel"], list):
        raise ValueError("'sentinel' must be a list")
    return data


def validate_sentinel_spec(spec: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if str(spec.get("schema_version") or SCHEMA_VERSION) != SCHEMA_VERSION:
        errors.append(f"schema_version mismatch: expected {SCHEMA_VERSION!r}, got {spec.get('schema_version')!r}")
    seen_ids: set[str] = set()
    entries = spec.get("sentinel") or []
    for idx, entry in enumerate(entries):
        prefix = f"sentinel[{idx}]"
        if not isinstance(entry, dict):
            errors.append(f"{prefix} must be a mapping")
            continue
        extra = set(entry.keys()) - SPEC_ENTRY_FIELDS
        if extra:
            errors.append(f"{prefix} extra fields: {sorted(extra)}")
        if not entry.get("entity"):
            errors.append(f"{prefix}.entity is required")
        if not entry.get("entity_role") or entry["entity_role"] not in ENTITY_ROLES:
            errors.append(f"{prefix}.entity_role invalid: {entry.get('entity_role')!r}")
        if entry.get("bucket_role") is not None and entry.get("bucket_role") not in BUCKET_ROLES:
            errors.append(f"{prefix}.bucket_role invalid: {entry.get('bucket_role')!r}")
        if entry.get("entity_role") == "tracked" and entry.get("bucket_role") is None:
            errors.append(f"{prefix}.bucket_role is required for tracked entities")
        if not entry.get("source_role") or entry["source_role"] not in SOURCE_ROLES:
            errors.append(f"{prefix}.source_role invalid: {entry.get('source_role')!r}")
        if not entry.get("evidence_text"):
            errors.append(f"{prefix}.evidence_text is required")
        current_stage = entry.get("current_stage")
        if current_stage is not None and current_stage not in STAGES:
            errors.append(f"{prefix}.current_stage invalid: {current_stage!r}")
        expected_next_stage = entry.get("expected_next_stage")
        if expected_next_stage is not None and expected_next_stage not in STAGES:
            errors.append(f"{prefix}.expected_next_stage invalid: {expected_next_stage!r}")
        if entry.get("current_confidence") is not None and entry.get("current_confidence") not in CONFIDENCE_LEVELS:
            errors.append(f"{prefix}.current_confidence invalid: {entry.get('current_confidence')!r}")
        if entry.get("trigger_code") is not None and entry.get("trigger_code") not in TRIGGER_CODES:
            errors.append(f"{prefix}.trigger_code invalid: {entry.get('trigger_code')!r}")
        if entry.get("grammar_key") is not None and get_sector_grammar(str(entry.get("grammar_key") or "")) is None:
            errors.append(f"{prefix}.grammar_key invalid: {entry.get('grammar_key')!r}")
        if entry.get("adjacency_terms") is not None and not isinstance(entry.get("adjacency_terms"), list):
            errors.append(f"{prefix}.adjacency_terms must be a list when provided")
        if entry.get("anti_thesis_focus") is not None and not isinstance(entry.get("anti_thesis_focus"), list):
            errors.append(f"{prefix}.anti_thesis_focus must be a list when provided")
        sentinel_id = str(entry.get("sentinel_id") or _projection_id(
            str(entry.get("entity") or ""),
            entry.get("product"),
            str(entry.get("entity_role") or ""),
            entry.get("bucket_role"),
        ))
        if sentinel_id in seen_ids:
            errors.append(f"{prefix}.sentinel_id duplicated: {sentinel_id}")
        seen_ids.add(sentinel_id)
    return errors


def sync_sentinel_spec(conn: sqlite3.Connection, spec: dict[str, Any]) -> dict[str, Any]:
    errors = validate_sentinel_spec(spec)
    if errors:
        return {"ok": False, "error_count": len(errors), "errors": errors, "synced": 0}
    synced = 0
    updated = 0
    schema_version = str(spec.get("schema_version") or SCHEMA_VERSION)
    for entry in spec.get("sentinel") or []:
        projection_id = str(entry.get("sentinel_id") or _projection_id(
            str(entry.get("entity") or ""),
            entry.get("product"),
            str(entry.get("entity_role") or ""),
            entry.get("bucket_role"),
        ))
        row = {
            "projection_id": projection_id,
            "schema_version": schema_version,
            "entity": str(entry.get("entity") or ""),
            "product": entry.get("product"),
            "bucket_role": entry.get("bucket_role"),
            "entity_role": str(entry.get("entity_role") or ""),
            "linked_thesis_id": entry.get("linked_thesis_id"),
            "linked_target_case_id": entry.get("linked_target_case_id"),
            "grammar_key": entry.get("grammar_key"),
            "current_stage": entry.get("current_stage"),
            "stage_entered_at": entry.get("stage_entered_at") or entry.get("evidence_date") or utc_now_iso(),
            "current_confidence": entry.get("current_confidence") or "medium",
            "expected_next_stage": entry.get("expected_next_stage") or _next_stage(entry.get("current_stage")),
            "expected_by": entry.get("expected_by"),
            "last_event_id": None,
            "last_event_time": entry.get("stage_entered_at") or entry.get("evidence_date"),
            "last_seen_time": entry.get("evidence_date"),
            "last_route": None,
            "last_route_reason": None,
            "last_source_tier": _source_tier_for_event(entry),
            "last_independence_group": None,
            "trigger_code": entry.get("trigger_code"),
            "evidence_text": entry.get("evidence_text"),
            "evidence_url": entry.get("evidence_url"),
            "evidence_date": entry.get("evidence_date"),
            "source_role": entry.get("source_role"),
            "stall_status": "clear",
            "raw_event_count": 0,
            "independence_group_count": 0,
            "attention_capture_ratio": 0.0,
            "pending_anti_thesis_count": 0,
            "notes_json": json_dumps(
                {
                    **(entry.get("notes") or {}),
                    "adjacency_terms": entry.get("adjacency_terms") or [],
                    "anti_thesis_focus": entry.get("anti_thesis_focus") or [],
                }
            ),
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
        exists = select_one(conn, "SELECT projection_id FROM event_state_projections WHERE projection_id = ?", (projection_id,))
        conn.execute(
            """
            INSERT INTO event_state_projections(
              projection_id, schema_version, entity, product, bucket_role, entity_role,
              linked_thesis_id, linked_target_case_id, grammar_key, current_stage, stage_entered_at,
              current_confidence, expected_next_stage, expected_by, last_event_id, last_event_time,
              last_seen_time, last_route, last_route_reason, last_source_tier, last_independence_group,
              trigger_code, evidence_text, evidence_url, evidence_date, source_role, stall_status,
              raw_event_count, independence_group_count, attention_capture_ratio, pending_anti_thesis_count,
              notes_json, created_at, updated_at
            ) VALUES (
              :projection_id, :schema_version, :entity, :product, :bucket_role, :entity_role,
              :linked_thesis_id, :linked_target_case_id, :grammar_key, :current_stage, :stage_entered_at,
              :current_confidence, :expected_next_stage, :expected_by, :last_event_id, :last_event_time,
              :last_seen_time, :last_route, :last_route_reason, :last_source_tier, :last_independence_group,
              :trigger_code, :evidence_text, :evidence_url, :evidence_date, :source_role, :stall_status,
              :raw_event_count, :independence_group_count, :attention_capture_ratio, :pending_anti_thesis_count,
              :notes_json, :created_at, :updated_at
            )
            ON CONFLICT(projection_id) DO UPDATE SET
              schema_version=excluded.schema_version,
              entity=excluded.entity,
              product=excluded.product,
              bucket_role=excluded.bucket_role,
              entity_role=excluded.entity_role,
              linked_thesis_id=excluded.linked_thesis_id,
              linked_target_case_id=excluded.linked_target_case_id,
              grammar_key=excluded.grammar_key,
              current_stage=excluded.current_stage,
              stage_entered_at=excluded.stage_entered_at,
              current_confidence=excluded.current_confidence,
              expected_next_stage=excluded.expected_next_stage,
              expected_by=excluded.expected_by,
              last_source_tier=excluded.last_source_tier,
              trigger_code=excluded.trigger_code,
              evidence_text=excluded.evidence_text,
              evidence_url=excluded.evidence_url,
              evidence_date=excluded.evidence_date,
              source_role=excluded.source_role,
              notes_json=excluded.notes_json,
              updated_at=excluded.updated_at
            """,
            row,
        )
        insert_event(
            conn,
            stable_id("evt", "::".join(["sentinel_spec", projection_id, row["updated_at"]])),
            "event_state_projection",
            projection_id,
            "sentinel.spec_synced",
            {
                "entity": row["entity"],
                "product": row["product"],
                "bucket_role": row["bucket_role"],
                "entity_role": row["entity_role"],
            },
        )
        synced += 1
        if exists:
            updated += 1
    conn.commit()
    return {"ok": True, "synced": synced, "updated": updated, "error_count": 0, "errors": []}


def _find_projection(conn: sqlite3.Connection, event: dict[str, Any]) -> sqlite3.Row | None:
    entity = event["entity"]
    product = event.get("product")
    if product is not None:
        row = select_one(
            conn,
            "SELECT * FROM event_state_projections WHERE entity = ? AND product = ?",
            (entity, product),
        )
        if row is not None:
            return row
    rows = list_rows(conn, "SELECT * FROM event_state_projections WHERE entity = ?", (entity,))
    if not rows:
        return None
    if product is None and len(rows) == 1:
        return rows[0]
    for row in rows:
        if (row["product"] or "") == (product or ""):
            return row
    return None


def _find_adjacent_projections(
    conn: sqlite3.Connection,
    event: dict[str, Any],
    *,
    exclude_projection_ids: set[str] | None = None,
) -> list[sqlite3.Row]:
    exclude_projection_ids = exclude_projection_ids or set()
    context_blob = _event_context_blob(event)
    same_entity_rows = list_rows(conn, "SELECT * FROM event_state_projections WHERE entity = ?", (event["entity"],))
    if same_entity_rows:
        return [row for row in same_entity_rows if row["projection_id"] not in exclude_projection_ids]
    rows = list_rows(conn, "SELECT * FROM event_state_projections ORDER BY updated_at DESC", ())
    hits: list[sqlite3.Row] = []
    for row in rows:
        if row["projection_id"] in exclude_projection_ids:
            continue
        terms = _projection_terms(row)
        if any(term and term in context_blob for term in terms):
            hits.append(row)
    return hits[:5]


def _upsert_independence_group(conn: sqlite3.Connection, event: dict[str, Any]) -> dict[str, Any]:
    group_id = event["independence_group"]
    existing = select_one(conn, "SELECT * FROM event_independence_groups WHERE group_id = ?", (group_id,))
    now_iso = utc_now_iso()
    if existing is None:
        conn.execute(
            """
            INSERT INTO event_independence_groups(
              group_id, schema_version, root_claim_key, entity, product, source_tier, source_role,
              event_count, first_event_id, last_event_id, first_event_time, last_event_time,
              representative_evidence_text, representative_evidence_url, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                group_id,
                event["schema_version"],
                event["root_claim_key"],
                event["entity"],
                event.get("product"),
                event["source_tier"],
                event["source_role"],
                1,
                event["event_id"],
                event["event_id"],
                event["event_time"],
                event["event_time"],
                event["evidence_text"],
                event.get("evidence_url"),
                now_iso,
                now_iso,
            ),
        )
        group_size = 1
        is_new_group = True
    else:
        group_size = int(existing["event_count"]) + 1
        conn.execute(
            """
            UPDATE event_independence_groups
            SET event_count = ?,
                last_event_id = ?,
                last_event_time = ?,
                representative_evidence_text = ?,
                representative_evidence_url = COALESCE(?, representative_evidence_url),
                updated_at = ?
            WHERE group_id = ?
            """,
            (
                group_size,
                event["event_id"],
                event["event_time"],
                event["evidence_text"],
                event.get("evidence_url"),
                now_iso,
                group_id,
            ),
        )
        is_new_group = False
    aggregate = select_one(
        conn,
        """
        SELECT COUNT(*) AS group_count,
               SUM(CASE WHEN source_tier = 'primary' THEN 1 ELSE 0 END) AS primary_groups,
               SUM(CASE WHEN source_tier = 'secondary' THEN 1 ELSE 0 END) AS secondary_groups,
               SUM(CASE WHEN source_tier = 'tertiary' THEN 1 ELSE 0 END) AS tertiary_groups
        FROM event_independence_groups
        WHERE root_claim_key = ?
        """,
        (event["root_claim_key"],),
    )
    return {
        "group_id": group_id,
        "group_size": group_size,
        "is_new_group": is_new_group,
        "group_count": int(aggregate["group_count"] or 0),
        "primary_groups": int(aggregate["primary_groups"] or 0),
        "secondary_groups": int(aggregate["secondary_groups"] or 0),
        "tertiary_groups": int(aggregate["tertiary_groups"] or 0),
    }


def _classify_residual(
    conn: sqlite3.Connection,
    event: dict[str, Any],
    projection: sqlite3.Row | None,
) -> dict[str, Any]:
    if projection is not None:
        return {
            "residual_class": "watch",
            "residual_target": str(projection["projection_id"]),
            "adjacent_projection_ids": [str(projection["projection_id"])],
        }
    adjacent_rows = _find_adjacent_projections(conn, event)
    if adjacent_rows:
        ids = [str(row["projection_id"]) for row in adjacent_rows]
        return {
            "residual_class": "adjacent",
            "residual_target": ",".join(ids[:3]),
            "adjacent_projection_ids": ids,
        }
    return {
        "residual_class": "frontier",
        "residual_target": None,
        "adjacent_projection_ids": [],
    }


def _can_apply_state(
    event: dict[str, Any],
    projection: sqlite3.Row | None,
    corroboration: dict[str, Any],
) -> bool:
    if event["event_type"] == "stalled":
        return projection is not None
    if projection is None:
        return False
    source_tier = event["source_tier"]
    if source_tier == "primary":
        return True
    if source_tier == "secondary":
        return corroboration["primary_groups"] > 0 or corroboration["group_count"] >= 2
    return False


def _route_policy(
    event: dict[str, Any],
    *,
    residual_class: str,
    can_apply_state: bool,
) -> tuple[str, str]:
    candidate = event.get("candidate_thesis")
    trigger = event.get("mapped_trigger")
    source_tier = event["source_tier"]
    if candidate and residual_class in {"adjacent", "frontier"}:
        if source_tier == "tertiary" and event["novelty"] == "low":
            return "archive", "low_signal_frontier_candidate"
        return "opportunity", f"{residual_class}_candidate"
    if event.get("event_type") == "stalled":
        return "review", "missing_milestone"
    if trigger in INTERRUPT_TRIGGERS and source_tier != "tertiary" and can_apply_state:
        return "interrupt", "thesis_level_trigger"
    if trigger in REVIEW_TRIGGERS:
        if not can_apply_state and source_tier != "primary":
            return "review", "needs_corroboration"
        return "review", "review_trigger"
    if residual_class == "adjacent":
        return "review", "adjacent_signal"
    if event.get("stage_from") and event.get("stage_to") and event.get("relevance") in {"direct", "adjacent"}:
        return "review", "state_transition"
    if residual_class == "frontier":
        return "archive", "unmapped_frontier_noise"
    return "archive", "no_material_delta"


def classify_event(conn: sqlite3.Connection, draft: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_event(draft)
    errors = validate_event(normalized)
    if errors:
        return {"ok": False, "normalized": normalized, "errors": errors}
    projection = _find_projection(conn, normalized)
    residual = _classify_residual(conn, normalized, projection)
    existing_group = select_one(
        conn,
        """
        SELECT COUNT(*) AS group_count,
               SUM(CASE WHEN source_tier = 'primary' THEN 1 ELSE 0 END) AS primary_groups,
               SUM(CASE WHEN source_tier = 'secondary' THEN 1 ELSE 0 END) AS secondary_groups,
               SUM(CASE WHEN source_tier = 'tertiary' THEN 1 ELSE 0 END) AS tertiary_groups
        FROM event_independence_groups
        WHERE root_claim_key = ?
        """,
        (normalized["root_claim_key"],),
    )
    corroboration = {
        "group_id": normalized["independence_group"],
        "group_size": 1,
        "is_new_group": select_one(
            conn,
            "SELECT group_id FROM event_independence_groups WHERE group_id = ?",
            (normalized["independence_group"],),
        )
        is None,
        "group_count": int(existing_group["group_count"] or 0) + (
            1
            if select_one(conn, "SELECT group_id FROM event_independence_groups WHERE group_id = ?", (normalized["independence_group"],))
            is None
            else 0
        ),
        "primary_groups": int(existing_group["primary_groups"] or 0),
        "secondary_groups": int(existing_group["secondary_groups"] or 0),
        "tertiary_groups": int(existing_group["tertiary_groups"] or 0),
    }
    if normalized["source_tier"] == "primary" and corroboration["is_new_group"]:
        corroboration["primary_groups"] += 1
    elif normalized["source_tier"] == "secondary" and corroboration["is_new_group"]:
        corroboration["secondary_groups"] += 1
    elif normalized["source_tier"] == "tertiary" and corroboration["is_new_group"]:
        corroboration["tertiary_groups"] += 1
    can_apply_state = _can_apply_state(normalized, projection, corroboration)
    route, route_reason = _route_policy(
        normalized,
        residual_class=residual["residual_class"],
        can_apply_state=can_apply_state,
    )
    return {
        "ok": True,
        "normalized": normalized,
        "projection_id": projection["projection_id"] if projection is not None else None,
        "residual_class": residual["residual_class"],
        "residual_target": residual["residual_target"],
        "adjacent_projection_ids": residual["adjacent_projection_ids"],
        "corroboration": corroboration,
        "can_apply_state": can_apply_state,
        "route": route,
        "route_reason": route_reason,
    }


def _ensure_anti_thesis_check(
    conn: sqlite3.Connection,
    *,
    object_type: str,
    object_id: str,
    target_label: str,
    trigger_event_id: str,
    due_reason: str,
    focus_terms: list[str] | None = None,
) -> str:
    check_id = stable_id("anti", "::".join([object_type, object_id]))
    focus_line = ", ".join(item for item in (focus_terms or []) if item) or "竞争加速、兑现延迟、约束破裂"
    prompt = (
        f"请对 {target_label} 做反面论证搜索。\n"
        f"目标对象: {object_type}:{object_id}\n"
        f"触发原因: {due_reason}\n"
        f"优先反证焦点: {focus_line}\n"
        "要求输出: 1) 最强反证 2) timing vs thesis 区分 3) 如果成立应如何降级。"
    )
    existing = select_one(conn, "SELECT check_id, status FROM anti_thesis_checks WHERE check_id = ?", (check_id,))
    now_iso = utc_now_iso()
    if existing is None:
        conn.execute(
            """
            INSERT INTO anti_thesis_checks(
              check_id, schema_version, object_type, object_id, target_label, status,
              due_reason, trigger_event_id, prompt, result_summary, contradiction_score,
              created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'due', ?, ?, ?, NULL, NULL, ?, ?)
            """,
            (check_id, SCHEMA_VERSION, object_type, object_id, target_label, due_reason, trigger_event_id, prompt, now_iso, now_iso),
        )
    elif existing["status"] != "recorded":
        conn.execute(
            """
            UPDATE anti_thesis_checks
            SET status = 'due',
                due_reason = ?,
                trigger_event_id = ?,
                prompt = ?,
                updated_at = ?
            WHERE check_id = ?
            """,
            (due_reason, trigger_event_id, prompt, now_iso, check_id),
        )
    insert_event(
        conn,
        stable_id("evt", "::".join(["anti_thesis", check_id, trigger_event_id])),
        "anti_thesis_check",
        check_id,
        "event_mining.anti_thesis_due",
        {"object_type": object_type, "object_id": object_id, "target_label": target_label},
    )
    return check_id


def _upsert_candidate(
    conn: sqlite3.Connection,
    event: dict[str, Any],
    route: str,
    *,
    residual_class: str,
    adjacent_projection_ids: list[str],
    corroboration: dict[str, Any],
) -> str | None:
    thesis_name = event.get("candidate_thesis")
    if not thesis_name:
        return None
    candidate_id = _candidate_id(str(thesis_name))
    existing = select_one(conn, "SELECT * FROM opportunity_candidates WHERE candidate_id = ?", (candidate_id,))
    novelty_score = {"low": 0.2, "medium": 0.6, "high": 1.0}[event["novelty"]]
    corroboration_score = float(min(5.0, max(1.0, corroboration["group_count"])))
    investability_score = {"peripheral": 0.1, "adjacent": 0.5, "direct": 0.9}[event["relevance"]]
    raw_event_count = 1
    independence_group_count = 1 if corroboration["is_new_group"] else 0
    attention_capture_ratio = float(raw_event_count) / float(max(independence_group_count, 1))
    anti_thesis_status = "due" if route == "opportunity" and _is_positive_impact(event["impact"]) else "clear"
    if existing is None:
        row = {
            "candidate_id": candidate_id,
            "schema_version": SCHEMA_VERSION,
            "thesis_name": thesis_name,
            "status": "open",
            "route": route,
            "residual_class": residual_class,
            "adjacent_projection_ids_json": json_dumps(adjacent_projection_ids),
            "cluster_score": novelty_score if corroboration["is_new_group"] else 0.1,
            "persistence_score": 1.0,
            "corroboration_score": corroboration_score,
            "investability_score": investability_score,
            "raw_event_count": raw_event_count,
            "independence_group_count": independence_group_count,
            "attention_capture_ratio": attention_capture_ratio,
            "anti_thesis_status": anti_thesis_status,
            "last_source_tier": event["source_tier"],
            "earliest_event_time": event["event_time"],
            "latest_event_time": event["event_time"],
            "last_event_id": event["event_id"],
            "next_proving_milestone": None,
            "note": event["evidence_text"],
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
        columns = ", ".join(sorted(row.keys()))
        placeholders = ", ".join(f":{key}" for key in sorted(row.keys()))
        conn.execute(f"INSERT INTO opportunity_candidates({columns}) VALUES ({placeholders})", row)
    else:
        raw_event_count = int(existing["raw_event_count"] or 0) + 1
        independence_group_count = int(existing["independence_group_count"] or 0) + (1 if corroboration["is_new_group"] else 0)
        attention_capture_ratio = float(raw_event_count) / float(max(independence_group_count, 1))
        cluster_score = float(existing["cluster_score"]) + (novelty_score if corroboration["is_new_group"] else 0.05)
        persistence_score = float(existing["persistence_score"]) + (0.5 if corroboration["is_new_group"] else 0.0)
        corroboration = min(5.0, max(float(existing["corroboration_score"]), corroboration_score))
        investability = min(5.0, max(float(existing["investability_score"]), investability_score))
        conn.execute(
            """
            UPDATE opportunity_candidates
            SET route = ?,
                residual_class = ?,
                adjacent_projection_ids_json = ?,
                cluster_score = ?,
                persistence_score = ?,
                corroboration_score = ?,
                investability_score = ?,
                raw_event_count = ?,
                independence_group_count = ?,
                attention_capture_ratio = ?,
                anti_thesis_status = CASE
                  WHEN anti_thesis_status = 'recorded' THEN anti_thesis_status
                  WHEN ? = 'due' THEN 'due'
                  ELSE anti_thesis_status
                END,
                last_source_tier = ?,
                latest_event_time = ?,
                last_event_id = ?,
                note = ?,
                updated_at = ?
            WHERE candidate_id = ?
            """,
            (
                route,
                residual_class,
                json_dumps(adjacent_projection_ids),
                cluster_score,
                persistence_score,
                corroboration,
                investability,
                raw_event_count,
                independence_group_count,
                attention_capture_ratio,
                anti_thesis_status,
                event["source_tier"],
                event["event_time"],
                event["event_id"],
                event["evidence_text"],
                utc_now_iso(),
                candidate_id,
            ),
        )
    insert_event(
        conn,
        stable_id("evt", "::".join(["candidate", candidate_id, event["event_id"]])),
        "opportunity_candidate",
        candidate_id,
        "event_mining.candidate_updated",
        {"thesis_name": thesis_name, "event_id": event["event_id"], "route": route},
    )
    if anti_thesis_status == "due":
        _ensure_anti_thesis_check(
            conn,
            object_type="candidate",
            object_id=candidate_id,
            target_label=str(thesis_name),
            trigger_event_id=event["event_id"],
            due_reason="positive_candidate_signal",
            focus_terms=[str(thesis_name)],
        )
    return candidate_id


def _apply_event_to_projection(
    conn: sqlite3.Connection,
    projection: sqlite3.Row,
    event: dict[str, Any],
    route: str,
    *,
    route_reason: str,
    can_apply_state: bool,
    corroboration: dict[str, Any],
) -> str:
    current_stage = projection["current_stage"]
    next_stage = projection["expected_next_stage"]
    stage_changed = can_apply_state and bool(event.get("stage_to")) and event.get("stage_to") != current_stage
    updated_stage = event.get("stage_to") if stage_changed else current_stage
    stage_entered_at = event["event_time"] if stage_changed else projection["stage_entered_at"]
    updated_next_stage = _next_stage(updated_stage) if stage_changed else next_stage
    updated_expected_by = None if stage_changed else projection["expected_by"]
    stall_status = "overdue" if event["event_type"] == "stalled" else projection["stall_status"]
    raw_event_count = int(projection["raw_event_count"] or 0) + 1
    independence_group_count = int(projection["independence_group_count"] or 0) + (1 if corroboration["is_new_group"] else 0)
    attention_capture_ratio = float(raw_event_count) / float(max(independence_group_count, 1))
    notes = json.loads(projection["notes_json"]) if projection["notes_json"] else {}
    anti_focus = notes.get("anti_thesis_focus") if isinstance(notes, dict) else []
    conn.execute(
        """
        UPDATE event_state_projections
        SET current_stage = ?,
            stage_entered_at = ?,
            current_confidence = ?,
            expected_next_stage = ?,
            expected_by = ?,
            last_event_id = ?,
            last_event_time = ?,
            last_seen_time = ?,
            last_route = ?,
            last_route_reason = ?,
            last_source_tier = ?,
            last_independence_group = ?,
            trigger_code = COALESCE(?, trigger_code),
            evidence_text = ?,
            evidence_url = ?,
            evidence_date = ?,
            source_role = ?,
            stall_status = ?,
            raw_event_count = ?,
            independence_group_count = ?,
            attention_capture_ratio = ?,
            pending_anti_thesis_count = CASE
              WHEN pending_anti_thesis_count > 0 THEN pending_anti_thesis_count
              ELSE pending_anti_thesis_count
            END,
            updated_at = ?
        WHERE projection_id = ?
        """,
        (
            updated_stage,
            stage_entered_at,
            event["confidence"] if can_apply_state else projection["current_confidence"],
            updated_next_stage,
            updated_expected_by,
            event["event_id"],
            event["event_time"],
            event["first_seen_time"],
            route,
            route_reason,
            event["source_tier"],
            event["independence_group"],
            event.get("mapped_trigger"),
            event["evidence_text"],
            event.get("evidence_url"),
            event.get("evidence_date"),
            event["source_role"],
            stall_status,
            raw_event_count,
            independence_group_count,
            attention_capture_ratio,
            utc_now_iso(),
            projection["projection_id"],
        ),
    )
    payload = {
        "entity": projection["entity"],
        "product": projection["product"],
        "event_id": event["event_id"],
        "stage_before": current_stage,
        "stage_after": updated_stage,
        "route": route,
        "route_reason": route_reason,
        "event_type": event["event_type"],
        "state_applied": can_apply_state,
    }
    insert_event(
        conn,
        stable_id("evt", "::".join(["projection", projection["projection_id"], event["event_id"]])),
        "event_state_projection",
        projection["projection_id"],
        "event_mining.state_updated",
        payload,
    )
    if route in {"review", "interrupt"} and _is_positive_impact(event["impact"]) and projection["bucket_role"] in {"core", "option", "alternative"}:
        _ensure_anti_thesis_check(
            conn,
            object_type="projection",
            object_id=str(projection["projection_id"]),
            target_label=f"{projection['entity']} / {projection['product'] or projection['bucket_role']}",
            trigger_event_id=event["event_id"],
            due_reason=route_reason,
            focus_terms=[str(item) for item in anti_focus] if isinstance(anti_focus, list) else [],
        )
        conn.execute(
            "UPDATE event_state_projections SET pending_anti_thesis_count = 1 WHERE projection_id = ?",
            (projection["projection_id"],),
        )
    return str(projection["projection_id"])


def import_events(conn: sqlite3.Connection, events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    imported = 0
    skipped = 0
    failed = 0
    results: list[dict[str, Any]] = []
    for draft in events:
        normalized = normalize_event(draft)
        errors = validate_event(normalized)
        if errors:
            failed += 1
            results.append({"event_id": normalized.get("event_id"), "ok": False, "errors": errors})
            continue
        exists = select_one(conn, "SELECT event_id FROM event_mining_events WHERE event_id = ?", (normalized["event_id"],))
        if exists is not None:
            skipped += 1
            results.append({"event_id": normalized["event_id"], "ok": True, "status": "skipped_duplicate"})
            continue
        projection = _find_projection(conn, normalized)
        residual = _classify_residual(conn, normalized, projection)
        normalized["residual_class"] = residual["residual_class"]
        corroboration = _upsert_independence_group(conn, normalized)
        can_apply_state = _can_apply_state(normalized, projection, corroboration)
        route, route_reason = _route_policy(
            normalized,
            residual_class=residual["residual_class"],
            can_apply_state=can_apply_state,
        )
        projection_id = None
        if projection is not None:
            projection_id = _apply_event_to_projection(
                conn,
                projection,
                normalized,
                route,
                route_reason=route_reason,
                can_apply_state=can_apply_state,
                corroboration=corroboration,
            )
        candidate_id = _upsert_candidate(
            conn,
            normalized,
            route,
            residual_class=residual["residual_class"],
            adjacent_projection_ids=residual["adjacent_projection_ids"],
            corroboration=corroboration,
        )
        payload = {
            "event_row_id": stable_id("evtrow", normalized["event_id"]),
            "schema_version": normalized["schema_version"],
            "event_id": normalized["event_id"],
            "entity": normalized["entity"],
            "product": normalized.get("product"),
            "event_type": normalized["event_type"],
            "stage_from": normalized.get("stage_from"),
            "stage_to": normalized.get("stage_to"),
            "source_role": normalized["source_role"],
            "source_tier": normalized["source_tier"],
            "root_claim_key": normalized["root_claim_key"],
            "independence_group": normalized["independence_group"],
            "evidence_text": normalized["evidence_text"],
            "evidence_url": normalized.get("evidence_url"),
            "evidence_date": normalized.get("evidence_date"),
            "event_time": normalized["event_time"],
            "first_seen_time": normalized["first_seen_time"],
            "processed_time": normalized["processed_time"],
            "novelty": normalized["novelty"],
            "relevance": normalized["relevance"],
            "impact": normalized["impact"],
            "confidence": normalized["confidence"],
            "mapped_trigger": normalized.get("mapped_trigger"),
            "candidate_thesis": normalized.get("candidate_thesis"),
            "residual_class": residual["residual_class"],
            "residual_target": residual["residual_target"],
            "route_reason": route_reason,
            "state_applied": 1 if can_apply_state and projection is not None else 0,
            "dedup_group_size": corroboration["group_size"],
            "corroboration_count": corroboration["group_count"],
            "route": route,
            "projection_id": projection_id,
            "candidate_id": candidate_id,
            "raw_event_json": json_dumps(normalized),
            "created_at": utc_now_iso(),
        }
        columns = ", ".join(sorted(payload.keys()))
        placeholders = ", ".join(f":{key}" for key in sorted(payload.keys()))
        conn.execute(f"INSERT INTO event_mining_events({columns}) VALUES ({placeholders})", payload)
        insert_event(
            conn,
            stable_id("evt", "::".join(["event_row", normalized["event_id"]])),
            "event_mining_event",
            normalized["event_id"],
            "event_mining.imported",
            {
                "route": route,
                "route_reason": route_reason,
                "projection_id": projection_id,
                "candidate_id": candidate_id,
                "residual_class": residual["residual_class"],
                "source_tier": normalized["source_tier"],
                "independence_group": normalized["independence_group"],
                "state_applied": bool(can_apply_state and projection is not None),
            },
        )
        imported += 1
        results.append(
            {
                "event_id": normalized["event_id"],
                "ok": True,
                "status": "imported",
                "route": route,
                "route_reason": route_reason,
                "projection_id": projection_id,
                "candidate_id": candidate_id,
                "residual_class": residual["residual_class"],
                "source_tier": normalized["source_tier"],
                "independence_group": normalized["independence_group"],
                "corroboration_count": corroboration["group_count"],
            }
        )
    conn.commit()
    return {"ok": failed == 0, "imported": imported, "skipped": skipped, "failed": failed, "results": results}


def emit_stalled_events(conn: sqlite3.Connection, *, as_of: str | None = None) -> dict[str, Any]:
    as_of_dt = _parse_dt(as_of) or datetime.now(timezone.utc)
    emitted: list[dict[str, Any]] = []
    for row in list_rows(
        conn,
        """
        SELECT * FROM event_state_projections
        WHERE expected_by IS NOT NULL
        ORDER BY expected_by ASC, updated_at ASC
        """,
    ):
        expected_by = _parse_date(row["expected_by"])
        if expected_by is None or expected_by >= as_of_dt.date():
            continue
        if row["current_stage"] in FINAL_STAGES:
            continue
        latest_stall = select_one(
            conn,
            """
            SELECT event_id
            FROM event_mining_events
            WHERE projection_id = ? AND event_type = 'stalled'
            ORDER BY processed_time DESC
            LIMIT 1
            """,
            (row["projection_id"],),
        )
        if latest_stall is not None:
            continue
        event = normalize_event(
            {
                "entity": row["entity"],
                "product": row["product"],
                "event_type": "stalled",
                "stage_from": None,
                "stage_to": None,
                "source_role": row["source_role"] or "company_filing",
                "evidence_text": f"Expected {row['expected_next_stage'] or 'next milestone'} by {row['expected_by']}, but state remains {row['current_stage'] or 'unknown'}",
                "evidence_url": row["evidence_url"],
                "evidence_date": row["expected_by"],
                "event_time": datetime.combine(expected_by, time.min, tzinfo=timezone.utc).isoformat(),
                "novelty": "medium",
                "relevance": "direct" if row["entity_role"] == "tracked" else "adjacent",
                "impact": "timing_negative",
                "confidence": row["current_confidence"] or "medium",
                "mapped_trigger": row["trigger_code"],
                "candidate_thesis": None,
            }
        )
        import_result = import_events(conn, [event])
        if import_result["imported"]:
            emitted.append({"projection_id": row["projection_id"], "event_id": event["event_id"]})
    return {"ok": True, "emitted": len(emitted), "items": emitted, "as_of": as_of_dt.isoformat()}


def record_anti_thesis_result(
    conn: sqlite3.Connection,
    *,
    object_type: str,
    object_id: str,
    verdict: str,
    result_summary: str,
    contradiction_score: float | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    check_id = stable_id("anti", "::".join([object_type, object_id]))
    existing = select_one(conn, "SELECT * FROM anti_thesis_checks WHERE check_id = ?", (check_id,))
    if existing is None:
        return {"ok": False, "error": "anti_thesis_check_not_found", "check_id": check_id}
    now_iso = utc_now_iso()
    conn.execute(
        """
        UPDATE anti_thesis_checks
        SET status = 'recorded',
            result_summary = ?,
            contradiction_score = ?,
            updated_at = ?
        WHERE check_id = ?
        """,
        (result_summary, contradiction_score, now_iso, check_id),
    )
    if object_type == "candidate":
        conn.execute(
            "UPDATE opportunity_candidates SET anti_thesis_status = 'recorded', updated_at = ? WHERE candidate_id = ?",
            (now_iso, object_id),
        )
    if object_type == "projection":
        conn.execute(
            "UPDATE event_state_projections SET pending_anti_thesis_count = 0, updated_at = ? WHERE projection_id = ?",
            (now_iso, object_id),
        )
    record_feedback(
        conn,
        object_type=object_type,
        object_id=object_id,
        feedback_type="anti_thesis_verdict",
        verdict=verdict,
        score=contradiction_score,
        note=note or result_summary,
        metadata={"check_id": check_id},
    )
    conn.commit()
    return {"ok": True, "check_id": check_id, "object_type": object_type, "object_id": object_id}


def record_feedback(
    conn: sqlite3.Connection,
    *,
    object_type: str,
    object_id: str,
    feedback_type: str,
    verdict: str,
    score: float | None = None,
    note: str | None = None,
    related_event_id: str | None = None,
    related_candidate_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if feedback_type not in FEEDBACK_TYPES:
        return {"ok": False, "error": "invalid_feedback_type", "feedback_type": feedback_type}
    if verdict not in FEEDBACK_VERDICTS:
        return {"ok": False, "error": "invalid_verdict", "verdict": verdict}
    feedback_id = stable_id("feedback", "::".join([object_type, object_id, feedback_type, verdict, utc_now_iso()]))
    conn.execute(
        """
        INSERT INTO event_mining_feedback(
          feedback_id, schema_version, object_type, object_id, feedback_type, verdict,
          score, note, related_event_id, related_candidate_id, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            feedback_id,
            SCHEMA_VERSION,
            object_type,
            object_id,
            feedback_type,
            verdict,
            score,
            note,
            related_event_id,
            related_candidate_id,
            json_dumps(metadata or {}),
            utc_now_iso(),
        ),
    )
    if object_type == "candidate" and feedback_type == "candidate_verdict":
        status = {
            "promote": "promoted",
            "defer": "deferred",
            "dismiss": "dismissed",
        }.get(verdict)
        if status:
            conn.execute(
                "UPDATE opportunity_candidates SET status = ?, updated_at = ? WHERE candidate_id = ?",
                (status, utc_now_iso(), object_id),
            )
    insert_event(
        conn,
        stable_id("evt", "::".join(["event_feedback", feedback_id])),
        object_type,
        object_id,
        "event_mining.feedback_recorded",
        {"feedback_type": feedback_type, "verdict": verdict, "score": score},
    )
    return {"ok": True, "feedback_id": feedback_id}


def default_spec_path(root: str | Path) -> Path:
    root_path = Path(root)
    for rel in DEFAULT_SPEC_PATHS:
        candidate = root_path / rel
        if candidate.exists():
            return candidate
    return root_path / DEFAULT_SPEC_PATHS[0]


def build_spec_prompt_context(spec: dict[str, Any]) -> str:
    lines: list[str] = []
    grammar_keys: list[str] = []
    for entry in spec.get("sentinel") or []:
        entity = str(entry.get("entity") or "").strip()
        product = str(entry.get("product") or "").strip()
        bucket_role = str(entry.get("bucket_role") or "").strip() or "unmapped"
        entity_role = str(entry.get("entity_role") or "").strip() or "unknown"
        current_stage = str(entry.get("current_stage") or "").strip() or "unknown"
        expected_next_stage = str(entry.get("expected_next_stage") or "").strip() or "-"
        trigger_code = str(entry.get("trigger_code") or "").strip() or "-"
        evidence_text = str(entry.get("evidence_text") or "").strip()
        grammar_key = str(entry.get("grammar_key") or "").strip()
        if grammar_key:
            grammar_keys.append(grammar_key)
        parts = [
            entity,
            product or "-",
            f"bucket={bucket_role}",
            f"entity_role={entity_role}",
            f"stage={current_stage}",
            f"next={expected_next_stage}",
            f"trigger={trigger_code}",
        ]
        if grammar_key:
            parts.append(f"grammar={grammar_key}")
        if evidence_text:
            parts.append(f"evidence={evidence_text}")
        lines.append(" | ".join(parts))
    if not lines:
        return ""
    sections = [
        "## Source Policy Snapshot",
        "\n".join(source_policy_prompt_lines()),
    ]
    grammar_lines = grammar_prompt_lines(grammar_keys)
    if grammar_lines:
        sections.extend(["## Sector Grammar Hints", "\n".join(grammar_lines)])
    sections.extend(["## Current Sentinel Context", "\n".join(lines)])
    return "\n\n".join(section for section in sections if section)


def build_extraction_prompt(raw_text: str, sentinel_context: str | None = None) -> str:
    return f"""你是一个金融事件结构化抽取器。

## 任务
从原始文本中，提取所有与已知 watch/adjacent theme 相关的事件草稿。

注意：
- 你输出的是 **draft event JSON**
- `route` 由系统计算，不要输出
- `event_id` / `first_seen_time` / `processed_time` / `source_tier` / `root_claim_key` / `independence_group` 可留空

## Draft Event Schema

```json
{{
  "entity": "公司/组织",
  "product": "产品/技术或 null",
  "event_type": "product_milestone | competition | customer | macro | financial | stalled | candidate",
  "stage_from": "11阶段词表中的值或 null",
  "stage_to": "11阶段词表中的值或 null",
  "source_role": "company_filing | competitor_pr | conference | kol_digest | patent | media | hiring | customer_signal | regulator",
  "source_tier": "primary | secondary | tertiary 或 null",
  "evidence_text": "一句 grounded 事实",
  "evidence_url": "来源URL 或 null",
  "evidence_date": "YYYY-MM-DD 或 null",
  "event_time": "ISO datetime 或 null",
  "novelty": "high | medium | low",
  "relevance": "direct | adjacent | peripheral",
  "impact": "简短影响",
  "confidence": "high | medium | low",
  "mapped_trigger": "F1/F2/F3/B1-B8/V1-V3/M1 或 null",
  "candidate_thesis": "仅当 residual discovery 成立时非 null",
  "root_claim_key": "可留空；如果能明确同一根事实可填写",
  "independence_group": "可留空；如果能识别转载/复述共享同一底层来源可填写"
}}
```

## Stage Vocabulary
concept → early_prototype → prototype → sample → customer_validation →
qualification → pilot → first_commercial_shipment →
repeat_order → capacity_expansion → mass_adoption

## Rules
- `stage_from` 和 `stage_to` 要么一起填，要么一起为 null
- 只提取文本里明确存在的事实，不要补全
- 遇到 tertiary source，只允许作为 discovery / corroboration 线索，不要把它当成可以直接改 state 的一级证据
- 行业语法优先于泛化情绪词。优先识别商业化阶段、订单兑现、客户验证、约束恶化、里程碑缺失
- 如果没有相关事件，输出 `[]`
- 只输出 JSON 数组

{f"## 当前 Sentinel Context{chr(10)}{sentinel_context}" if sentinel_context else ""}

## Raw Text
{raw_text}
"""


if __name__ == "__main__":
    import sys

    result = validate_fixtures()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    sys.exit(0 if result["failed"] == 0 else 1)
