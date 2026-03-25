from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from .db import list_rows, select_one
from .graph import build_graph_from_db, detect_conflicts, find_broken_support_chains
from .utils import stable_id


THEME_STATUS_ORDER = {
    "priority": 0,
    "tracking": 1,
    "scouting": 2,
    "cooling": 3,
    "archived": 4,
}

THESIS_STATUS_ORDER = {
    "active": 0,
    "evidence_backed": 1,
    "framed": 2,
    "seed": 3,
    "paused": 4,
    "invalidated": 5,
    "expired": 6,
    "archived": 7,
}

ROUTE_TYPE_ORDER = {
    "corroboration_needed": 0,
    "thesis_input": 1,
    "thesis_seed": 2,
    "counter_search": 3,
    "monitor_candidate": 4,
}

DECISION_ACTION_ORDER = {
    "trim": 0,
    "exit": 1,
    "starter": 2,
    "add": 3,
    "prepare": 4,
    "observe": 5,
}

DECISION_ALIGNMENT_ORDER = {
    "aligned": 0,
    "drift": 1,
    "missing": 2,
    "superseded": 3,
}

SOURCE_VALIDATION_ORDER = {
    "partially_validated": 0,
    "validated": 1,
    "pending_validation": 2,
    "thesis_seeding": 3,
    "first_hand_feed": 4,
    "watching": 5,
}

SOURCE_CONFIDENCE_ORDER = {
    "grounded": 0,
    "developing": 1,
    "fragile": 2,
}

VIEWPOINT_STATUS_ORDER = {
    "partially_validated": 0,
    "validated": 1,
    "open": 2,
    "contradicted": 3,
    "expired": 4,
}

REVIEW_FRESHNESS_ORDER = {
    "missing": 0,
    "stale": 1,
    "aging": 2,
    "fresh": 3,
}

REVIEW_PRIORITY_ORDER = {
    "fresh": 0,
    "aging": 1,
    "stale": 2,
    "missing": 3,
}

REVIEW_REMEDIATION_GAP_ORDER = {
    "missing_initial_review": 0,
    "refresh_required": 1,
    "none": 2,
}

ROUTE_NORMALIZATION_ACTION_ORDER = {
    "accept_first_hand_input": 0,
    "supersede_foundational_seed": 1,
    "supersede_low_signal_corroboration": 2,
}

VOICE_MEMO_TRIAGE_ORDER = {
    "matched_thesis": 0,
    "candidate_seed": 1,
    "low_signal": 2,
}


def _json_loads(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def _parse_iso_date(raw: str | None) -> str:
    if not raw:
        return ""
    return raw[:10]


def _date_from_iso(raw: str | None) -> datetime.date | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(_parse_iso_date(raw)).date()
    except ValueError:
        return None


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _cutoff_iso(days: int) -> str:
    safe_days = max(days, 1)
    return (datetime.now(timezone.utc) - timedelta(days=safe_days - 1)).date().isoformat()


def _sort_key_by_order(value: str, order: dict[str, int]) -> tuple[int, str]:
    return (order.get(value, 99), value)


def summary_counts(conn: Any) -> dict[str, int]:
    return {
        "sources": int(select_one(conn, "SELECT COUNT(*) AS n FROM sources", ())["n"]),
        "artifacts": int(select_one(conn, "SELECT COUNT(*) AS n FROM artifacts", ())["n"]),
        "claims": int(select_one(conn, "SELECT COUNT(*) AS n FROM claims", ())["n"]),
        "themes": int(select_one(conn, "SELECT COUNT(*) AS n FROM themes", ())["n"]),
        "theses": int(select_one(conn, "SELECT COUNT(*) AS n FROM theses", ())["n"]),
        "targets": int(select_one(conn, "SELECT COUNT(*) AS n FROM targets", ())["n"]),
        "target_cases": int(select_one(conn, "SELECT COUNT(*) AS n FROM target_cases", ())["n"]),
        "timing_plans": int(select_one(conn, "SELECT COUNT(*) AS n FROM timing_plans", ())["n"]),
        "monitors": int(select_one(conn, "SELECT COUNT(*) AS n FROM monitors", ())["n"]),
        "reviews": int(select_one(conn, "SELECT COUNT(*) AS n FROM reviews", ())["n"]),
        "validation_cases": int(select_one(conn, "SELECT COUNT(*) AS n FROM validation_cases", ())["n"]),
        "source_viewpoints": int(select_one(conn, "SELECT COUNT(*) AS n FROM source_viewpoints", ())["n"]),
        "source_feedback_entries": int(select_one(conn, "SELECT COUNT(*) AS n FROM source_feedback_entries", ())["n"]),
        "operator_decisions": int(select_one(conn, "SELECT COUNT(*) AS n FROM operator_decisions", ())["n"]),
        "event_mining_events": int(select_one(conn, "SELECT COUNT(*) AS n FROM event_mining_events", ())["n"]),
        "event_state_projections": int(select_one(conn, "SELECT COUNT(*) AS n FROM event_state_projections", ())["n"]),
        "opportunity_candidates": int(select_one(conn, "SELECT COUNT(*) AS n FROM opportunity_candidates", ())["n"]),
        "anti_thesis_checks": int(select_one(conn, "SELECT COUNT(*) AS n FROM anti_thesis_checks", ())["n"]),
        "event_mining_feedback": int(select_one(conn, "SELECT COUNT(*) AS n FROM event_mining_feedback", ())["n"]),
    }


def _load_context(conn: Any) -> dict[str, Any]:
    themes = [dict(row) for row in list_rows(conn, "SELECT * FROM themes")]
    theses = [dict(row) for row in list_rows(conn, "SELECT * FROM theses")]
    thesis_versions = [dict(row) for row in list_rows(conn, "SELECT * FROM thesis_versions")]
    targets = [dict(row) for row in list_rows(conn, "SELECT * FROM targets")]
    target_cases = [dict(row) for row in list_rows(conn, "SELECT * FROM target_cases")]
    timing_plans = [dict(row) for row in list_rows(conn, "SELECT * FROM timing_plans")]
    monitors = [dict(row) for row in list_rows(conn, "SELECT * FROM monitors")]
    reviews = [dict(row) for row in list_rows(conn, "SELECT * FROM reviews")]
    artifacts = [dict(row) for row in list_rows(conn, "SELECT * FROM artifacts ORDER BY captured_at DESC")]

    theme_by_id = {row["theme_id"]: row for row in themes}
    version_by_id = {row["thesis_version_id"]: row for row in thesis_versions}
    target_by_id = {row["target_id"]: row for row in targets}

    target_cases_by_version: dict[str, list[dict[str, Any]]] = {}
    for row in target_cases:
        target_cases_by_version.setdefault(row["thesis_version_id"], []).append(row)

    timing_by_target_case = {row["target_case_id"]: row for row in timing_plans}

    monitors_by_owner: dict[str, list[dict[str, Any]]] = {}
    for row in monitors:
        monitors_by_owner.setdefault(row["owner_object_id"], []).append(row)

    reviews_by_owner: dict[str, list[dict[str, Any]]] = {}
    for row in reviews:
        reviews_by_owner.setdefault(row["owner_object_id"], []).append(row)
    for rows in reviews_by_owner.values():
        rows.sort(key=lambda item: (item["review_date"], item["created_at"]), reverse=True)

    return {
        "themes": themes,
        "theses": theses,
        "artifacts": artifacts,
        "theme_by_id": theme_by_id,
        "version_by_id": version_by_id,
        "target_by_id": target_by_id,
        "target_cases_by_version": target_cases_by_version,
        "timing_by_target_case": timing_by_target_case,
        "monitors_by_owner": monitors_by_owner,
        "reviews_by_owner": reviews_by_owner,
    }


def _artifact_source_map(conn: Any, artifact_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not artifact_ids:
        return {}
    placeholders = ",".join("?" for _ in artifact_ids)
    rows = list_rows(
        conn,
        f"""
        SELECT a.artifact_id, a.title, s.source_id, s.name AS source_name, s.source_type, s.primaryness
        FROM artifacts a
        JOIN sources s ON s.source_id = a.source_id
        WHERE a.artifact_id IN ({placeholders})
        """,
        tuple(artifact_ids),
    )
    return {row["artifact_id"]: dict(row) for row in rows}


def _claims_for_artifacts(conn: Any, artifact_ids: list[str]) -> list[dict[str, Any]]:
    if not artifact_ids:
        return []
    placeholders = ",".join("?" for _ in artifact_ids)
    return [dict(row) for row in list_rows(conn, f"SELECT * FROM claims WHERE artifact_id IN ({placeholders})", tuple(artifact_ids))]


def _claim_domain_warnings(claim: dict[str, Any]) -> list[dict[str, Any]]:
    payload = _json_loads(claim.get("domain_check_json"), {})
    if isinstance(payload, dict):
        warnings = payload.get("warnings", [])
        return warnings if isinstance(warnings, list) else []
    return []


def _claim_has_fatal_domain_warning(claim: dict[str, Any]) -> bool:
    return any(str(item.get("severity", "")).upper() == "FATAL" for item in _claim_domain_warnings(claim))


def _claim_provenance_complete(claim: dict[str, Any], source_row: dict[str, Any] | None) -> bool:
    if source_row and source_row.get("primaryness") == "personal":
        return True
    if not (claim.get("data_date") or "").strip():
        return False
    review_status = str(claim.get("review_status", "")).strip()
    if review_status and review_status != "unreviewed":
        return True
    if source_row and source_row.get("primaryness") == "first_hand":
        return True
    return False


def _linked_sources_for_thesis(conn: Any, thesis: dict[str, Any], source_index: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    linked: dict[str, dict[str, Any]] = {}
    for row in _artifact_source_map(conn, thesis["version"].get("created_from_artifacts", [])).values():
        source_id = row["source_id"]
        if source_id in linked:
            continue
        counts = source_index.get(source_id, {})
        validation_state = _source_validation_state(
            primaryness=row["primaryness"],
            source_type=row["source_type"],
            pending_route_count=counts.get("pending_route_count", 0),
            validated_case_count=counts.get("validated_case_count", 0),
            corroborated_accept_count=counts.get("corroborated_accept_count", 0),
        )
        linked[source_id] = {
            "source_id": source_id,
            "name": row["source_name"],
            "source_type": row["source_type"],
            "primaryness": row["primaryness"],
            "lane": _source_lane(row["source_type"], row["primaryness"]),
            "source_trust_tier": _source_trust_tier(row["source_type"], row["primaryness"]),
            "validation_state": validation_state,
            "source_priority_score": counts.get("source_priority_score", 0),
            "source_priority_label": counts.get("source_priority_label", "unproven"),
            "source_display_label": _source_display_label(
                primaryness=row["primaryness"],
                source_type=row["source_type"],
                source_priority_label=counts.get("source_priority_label", "unproven"),
            ),
            "feedback_freshness": counts.get("feedback_freshness", "missing"),
            "latest_feedback_type": counts.get("latest_feedback_type", ""),
            "latest_feedback_age_days": counts.get("latest_feedback_age_days"),
            "latest_viewpoint_status": counts.get("latest_viewpoint_status"),
            "latest_viewpoint_stance": counts.get("latest_viewpoint_stance"),
        }
    items = list(linked.values())
    items.sort(
        key=lambda item: (
            0 if item["primaryness"] == "first_hand" else 1,
            0 if item["primaryness"] == "second_hand" else 1,
            -(item["source_priority_score"]),
            SOURCE_VALIDATION_ORDER.get(item["validation_state"], 99),
            item["name"],
        )
    )
    return items


def _source_rows_by_ids(conn: Any, source_ids: list[str]) -> list[dict[str, Any]]:
    if not source_ids:
        return []
    unique_ids = list(dict.fromkeys(source_ids))
    placeholders = ",".join("?" for _ in unique_ids)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT source_id, name, source_type, primaryness
            FROM sources
            WHERE source_id IN ({placeholders})
            """,
            tuple(unique_ids),
        )
    ]
    row_by_id = {row["source_id"]: row for row in rows}
    items: list[dict[str, Any]] = []
    for source_id in unique_ids:
        row = row_by_id.get(source_id)
        if row is None:
            continue
        row["source_display_label"] = _source_display_label(
            primaryness=row["primaryness"],
            source_type=row["source_type"],
            source_priority_label="unproven",
        )
        row["source_priority_label"] = "unproven"
        row["feedback_freshness"] = "missing"
        items.append(row)
    return items


def _latest_operator_decisions(conn: Any, target_case_ids: list[str] | None = None) -> dict[str, dict[str, Any]]:
    where = ["od.status = 'active'"]
    params: list[Any] = []
    if target_case_ids:
        placeholders = ",".join("?" for _ in target_case_ids)
        where.append(f"od.target_case_id IN ({placeholders})")
        params.extend(target_case_ids)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT od.decision_id, od.target_case_id, od.thesis_id, od.decision_date, od.action_state,
                   od.confidence, od.rationale, od.source_ids_json, od.review_id, od.status,
                   od.supersedes_decision_id, od.created_at,
                   t.title AS thesis_title,
                   tg.ticker_or_symbol
            FROM operator_decisions od
            JOIN theses t ON t.thesis_id = od.thesis_id
            JOIN target_cases tc ON tc.target_case_id = od.target_case_id
            JOIN targets tg ON tg.target_id = tc.target_id
            WHERE {' AND '.join(where)}
            ORDER BY od.decision_date DESC, od.created_at DESC
            """,
            tuple(params),
        )
    ]
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        target_case_id = row["target_case_id"]
        if target_case_id in latest:
            continue
        source_ids = _json_loads(row.get("source_ids_json"), [])
        linked_sources = _source_rows_by_ids(conn, source_ids)
        row["source_ids"] = source_ids
        row["linked_sources"] = linked_sources
        row["source_surface"] = _source_support_surface(linked_sources)
        row["rationale_preview"] = _preview(row.get("rationale"), 120)
        latest[target_case_id] = row
    return latest


def _decision_source_support(linked_sources: list[dict[str, Any]]) -> dict[str, Any]:
    if not linked_sources:
        return {
            "source_confidence": "fragile",
            "source_confidence_reason": "当前 thesis 还没有挂接可用来源。",
            "needs_source_revisit": False,
            "linked_source_count": 0,
            "first_hand_source_count": 0,
            "second_hand_source_count": 0,
            "personal_source_count": 0,
            "linked_sources": [],
        }

    first_hand_sources = [item for item in linked_sources if item["primaryness"] == "first_hand"]
    second_hand_sources = [item for item in linked_sources if item["primaryness"] == "second_hand"]
    personal_sources = [item for item in linked_sources if item["primaryness"] == "personal"]
    trusted_second_hand = [
        item
        for item in second_hand_sources
        if item["feedback_freshness"] in {"fresh", "aging"}
        and (
            item["source_priority_label"] in {"watch", "high_priority"}
            or item["validation_state"] in {"validated", "partially_validated"}
        )
    ]
    revisit_sources = [item for item in second_hand_sources if item["feedback_freshness"] in {"aging", "stale"}]
    if first_hand_sources:
        confidence = "grounded"
        reason = "已挂一手来源锚点。"
        if trusted_second_hand:
            reason = "已挂一手来源锚点，同时有经过跟踪的二手来源补充上下文。"
    elif trusted_second_hand:
        confidence = "developing"
        reason = "当前主要依赖二手来源，但至少有一条仍在持续跟踪且反馈较新的来源。"
    elif second_hand_sources and personal_sources:
        confidence = "fragile"
        reason = "当前主要依赖个人备忘和二手来源，仍缺少一手来源锚点。"
    elif second_hand_sources:
        confidence = "fragile"
        reason = "当前主要依赖二手来源，仍缺少一手来源锚点。"
    else:
        confidence = "fragile"
        reason = "当前主要是个人想法，尚未接一手或稳定外部来源。"
    return {
        "source_confidence": confidence,
        "source_confidence_reason": reason,
        "needs_source_revisit": bool(revisit_sources),
        "linked_source_count": len(linked_sources),
        "first_hand_source_count": len(first_hand_sources),
        "second_hand_source_count": len(second_hand_sources),
        "personal_source_count": len(personal_sources),
        "linked_sources": linked_sources[:4],
    }


def _source_support_surface(linked_sources: list[dict[str, Any]], limit: int = 2) -> str:
    if not linked_sources:
        return "暂无来源"
    parts: list[str] = []
    for item in linked_sources[:limit]:
        if item["primaryness"] == "first_hand":
            tag = item.get("source_display_label") or item.get("source_trust_tier") or "anchor"
        elif item["primaryness"] == "personal":
            tag = "personal"
        else:
            tag = f"{item['source_priority_label']}/{item['feedback_freshness']}"
        parts.append(f"{item['name']}({tag})")
    return "、".join(parts)


def _source_lane(source_type: str | None, primaryness: str | None) -> str:
    if primaryness == "personal":
        return "personal"
    if primaryness == "second_hand" or source_type == "kol":
        return "second_hand"
    return "official"


def _source_trust_tier(source_type: str | None, primaryness: str | None) -> str:
    if primaryness == "personal":
        return "personal"
    if source_type in {"official_disclosure", "exchange", "governance"}:
        return "anchor"
    if source_type == "paper":
        return "reference"
    if primaryness == "first_hand":
        return "reference"
    return "derived"


def _source_display_label(*, primaryness: str | None, source_type: str | None, source_priority_label: str) -> str:
    if primaryness == "first_hand":
        return _source_trust_tier(source_type, primaryness)
    if primaryness == "personal":
        return "personal"
    return source_priority_label


def _decision_action(desired_posture: str | None, target_case_status: str, thesis_status: str) -> str:
    if desired_posture == "observe":
        return "observe"
    if desired_posture == "prepare":
        return "prepare"
    if desired_posture == "starter":
        return "starter"
    if desired_posture == "add_on_confirmation":
        return "add"
    if desired_posture == "exit_watch":
        return "exit"
    if desired_posture == "avoid":
        if target_case_status in {"actionable", "active"} or thesis_status == "active":
            return "trim"
        return "observe"
    return "observe"


def _preview(text: str | None, limit: int = 96) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "…"


def _keyword_tokens(text: str | None) -> set[str]:
    if not text:
        return set()
    normalized = text.lower()
    tokens = {item for item in re.findall(r"[a-z][a-z0-9_.+-]{1,}", normalized) if len(item) >= 2}
    tokens.update(item for item in re.findall(r"[\u4e00-\u9fff]{2,}", text))
    return tokens


def _token_in_text(token: str, text: str) -> bool:
    if not token or not text:
        return False
    if re.search(r"[\u4e00-\u9fff]", token):
        return token in text
    return re.search(rf"(?<![a-z0-9]){re.escape(token.lower())}(?![a-z0-9])", text.lower()) is not None


def _review_freshness(review_date: str | None) -> tuple[str, int | None]:
    date_value = _date_from_iso(review_date)
    if date_value is None:
        return ("missing", None)
    age_days = (datetime.now(timezone.utc).date() - date_value).days
    if age_days <= 21:
        return ("fresh", age_days)
    if age_days <= 60:
        return ("aging", age_days)
    return ("stale", age_days)


def _build_effective_review(
    *,
    target_case_review: dict[str, Any] | None,
    thesis_review: dict[str, Any] | None,
) -> dict[str, Any]:
    if target_case_review is not None:
        source = "target_case_review"
        review = target_case_review
    elif thesis_review is not None:
        source = "thesis_fallback"
        review = thesis_review
    else:
        source = "none"
        review = None
    freshness, age_days = _review_freshness((review or {}).get("review_date"))
    if source == "none":
        gap_type = "missing_initial_review"
    elif freshness == "stale":
        gap_type = "refresh_required"
    else:
        gap_type = "none"
    return {
        "review": review,
        "source": source,
        "freshness": freshness,
        "age_days": age_days,
        "has_review": review is not None,
        "uses_fallback": source == "thesis_fallback",
        "gap_type": gap_type,
    }


def _review_surface_label(*, freshness: str, source: str) -> str:
    if source == "thesis_fallback":
        return f"{freshness} via thesis_fallback"
    return freshness


def _review_remediation_action(item: dict[str, Any]) -> str:
    gap_type = item.get("effective_review_gap_type")
    if gap_type == "missing_initial_review":
        return "create_target_case_review"
    if gap_type == "refresh_required":
        return "refresh_target_case_review"
    if item.get("effective_review_source") == "thesis_fallback":
        return "backfill_direct_target_review"
    return "none"


def _review_remediation_priority_label(*, queue_kind: str, action_state: str) -> str:
    if queue_kind == "blocking":
        return "p0" if action_state != "observe" else "p1"
    return "p1" if action_state != "observe" else "p2"


def _review_remediation_reason(item: dict[str, Any]) -> str:
    gap_type = item.get("effective_review_gap_type")
    age_days = item.get("review_age_days")
    if gap_type == "missing_initial_review":
        return "缺少 direct target_case review，当前不应算 ready。"
    if gap_type == "refresh_required":
        age_label = "unknown" if age_days is None else f"{age_days}d"
        return f"现有 review 已 stale（{age_label}），需要刷新 target case 视角。"
    return "当前仍依赖 thesis_fallback，建议补一条 direct target_case review。"


def _review_recipe(item: dict[str, Any]) -> dict[str, Any]:
    target = item.get("target") or {}
    return {
        "command": "create-review",
        "args": {
            "owner-object-type": "target_case",
            "owner-object-id": item["target_case_id"],
            "review-date": _today_iso(),
            "result": "unresolved",
        },
        "hints": {
            "what_we_believed": item["thesis_title"],
            "what_happened": item.get("reason") or "",
            "source_attribution": target.get("ticker_or_symbol") or item["target_case_id"],
        },
    }


def _track_record_score(
    *,
    validated_viewpoint_count: int,
    partially_validated_viewpoint_count: int,
    contradicted_viewpoint_count: int,
    validated_case_count: int,
) -> tuple[int, str]:
    score = (
        validated_viewpoint_count * 3
        + partially_validated_viewpoint_count * 1
        + min(validated_case_count, 3)
        - contradicted_viewpoint_count * 3
    )
    if score >= 6:
        label = "strong"
    elif score >= 2:
        label = "emerging"
    elif score <= -2:
        label = "weak"
    else:
        label = "mixed"
    return score, label


def _source_priority_label(priority_score: int) -> str:
    if priority_score >= 5:
        return "high_priority"
    if priority_score >= 2:
        return "watch"
    if priority_score <= -2:
        return "deprioritized"
    return "unproven"


def _source_feedback_freshness(created_at: str | None) -> tuple[str, int | None]:
    date_value = _date_from_iso(created_at)
    if date_value is None:
        return ("missing", None)
    age_days = (datetime.now(timezone.utc).date() - date_value).days
    if age_days <= 45:
        return ("fresh", age_days)
    if age_days <= 120:
        return ("aging", age_days)
    return ("stale", age_days)


def _source_feedback_multiplier(freshness: str) -> float:
    if freshness == "fresh":
        return 1.0
    if freshness == "aging":
        return 0.5
    return 0.0


def _source_feedback_rollup(conn: Any) -> dict[str, dict[str, Any]]:
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT source_feedback_id, source_id, feedback_type, weight, note, created_at
            FROM source_feedback_entries
            ORDER BY created_at DESC
            """,
        )
    ]
    rollup: dict[str, dict[str, Any]] = {}
    for row in rows:
        freshness, age_days = _source_feedback_freshness(row.get("created_at"))
        effective_weight = float(row["weight"]) * _source_feedback_multiplier(freshness)
        bucket = rollup.setdefault(
            row["source_id"],
            {
                "feedback_count": 0,
                "operator_feedback_score": 0,
                "effective_operator_feedback_score": 0.0,
                "positive_feedback_count": 0,
                "negative_feedback_count": 0,
                "fresh_feedback_count": 0,
                "aging_feedback_count": 0,
                "stale_feedback_count": 0,
                "latest_feedback_type": "",
                "latest_feedback_note": "",
                "latest_feedback_created_at": "",
                "latest_feedback_age_days": None,
                "feedback_freshness": "missing",
            },
        )
        bucket["feedback_count"] += 1
        bucket["operator_feedback_score"] += int(row["weight"])
        bucket["effective_operator_feedback_score"] += effective_weight
        if row["weight"] > 0:
            bucket["positive_feedback_count"] += 1
        elif row["weight"] < 0:
            bucket["negative_feedback_count"] += 1
        bucket[f"{freshness}_feedback_count"] += 1
        if not bucket["latest_feedback_created_at"]:
            bucket["latest_feedback_type"] = row["feedback_type"]
            bucket["latest_feedback_note"] = row["note"] or ""
            bucket["latest_feedback_created_at"] = row["created_at"] or ""
            bucket["latest_feedback_age_days"] = age_days
            bucket["feedback_freshness"] = freshness
    for bucket in rollup.values():
        bucket["effective_operator_feedback_score"] = round(bucket["effective_operator_feedback_score"], 2)
    return rollup


def _keyword_hits(text: str, keywords: tuple[str, ...]) -> int:
    lowered = (text or "").lower()
    return sum(1 for keyword in keywords if keyword in lowered)


def _suggest_viewpoint_status(validated_count: int, contradicted_count: int, pending_count: int) -> str:
    if contradicted_count > 0 and validated_count == 0:
        return "contradicted"
    if validated_count > 0 and pending_count > 0:
        return "partially_validated"
    if validated_count > 0:
        return "validated"
    return "open"


def _suggest_viewpoint_stance(validated_texts: list[str], pending_texts: list[str], contradicted_texts: list[str]) -> str:
    positive_keywords = (
        "增长",
        "受益",
        "机会",
        "有望",
        "景气",
        "量产",
        "扩张",
        "订单",
        "修复",
        "改善",
        "继续",
        "突破",
        "催化",
    )
    negative_keywords = (
        "风险",
        "扰动",
        "战争",
        "下滑",
        "恶化",
        "断掉",
        "观望",
        "减仓",
        "收仓",
        "不确定",
        "压力",
        "拖累",
        "回落",
    )
    all_texts = [*validated_texts, *pending_texts, *contradicted_texts]
    positive_score = sum(_keyword_hits(text, positive_keywords) for text in all_texts)
    negative_score = sum(_keyword_hits(text, negative_keywords) for text in all_texts)
    if contradicted_texts and not validated_texts:
        return "bearish" if negative_score >= positive_score else "cautious"
    if positive_score > 0 and negative_score > 0:
        return "cautious_bullish" if positive_score >= negative_score else "mixed"
    if positive_score > 0:
        return "cautious_bullish" if pending_texts else "bullish"
    if negative_score > 0:
        return "cautious" if validated_texts or pending_texts else "bearish"
    if validated_texts and pending_texts:
        return "cautious_bullish"
    if validated_texts:
        return "neutral"
    return "cautious" if pending_texts else "neutral"


def _suggest_horizon_label(horizon_months: int | None) -> str:
    if not horizon_months:
        return ""
    if horizon_months <= 3:
        return "0_3_months"
    if horizon_months <= 6:
        return "3_6_months"
    if horizon_months <= 12:
        return "6_12_months"
    return "12_plus_months"


def _suggest_viewpoint_summary(
    *,
    thesis_title: str,
    status: str,
    stance: str,
    validated_preview: str,
    pending_preview: str,
    contradicted_preview: str,
) -> str:
    basis = thesis_title or "当前 thesis"
    stance_label = {
        "bullish": "偏多",
        "cautious_bullish": "谨慎偏多",
        "neutral": "中性",
        "cautious": "谨慎",
        "bearish": "偏空",
        "mixed": "多空交织",
    }.get(stance, "待判断")
    if status == "contradicted":
        preview = contradicted_preview or validated_preview or pending_preview or "待补关键证据"
        return f"围绕{basis}，该来源本次观点当前已被反驳；关键被证伪点：{preview}。"
    if validated_preview and pending_preview:
        return f"围绕{basis}，该来源本次给出{stance_label}线索；已验证：{validated_preview}；待继续核验：{pending_preview}。"
    if validated_preview:
        return f"围绕{basis}，该来源本次给出{stance_label}线索；当前已验证的核心判断：{validated_preview}。"
    if pending_preview:
        return f"围绕{basis}，该来源本次给出{stance_label}线索；当前最需要继续核验的是：{pending_preview}。"
    return f"围绕{basis}，该来源本次给出{stance_label}观点，仍需继续验证。"


def _pick_pending_viewpoint_preview(conn: Any, artifact_id: str, thesis_title: str) -> str:
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT c.claim_text
            FROM claim_routes cr
            JOIN claims c ON c.claim_id = cr.claim_id
            WHERE cr.artifact_id = ?
              AND cr.route_type = 'corroboration_needed'
              AND cr.status = 'pending'
            ORDER BY cr.created_at DESC
            LIMIT 20
            """,
            (artifact_id,),
        )
    ]
    if not rows:
        return ""
    thesis_tokens = [token for token in re.split(r"[^0-9a-zA-Z\u4e00-\u9fff]+", (thesis_title or "").lower()) if len(token) >= 2]
    domain_keywords = (
        "ai",
        "融资",
        "算力",
        "国产",
        "订单",
        "扩产",
        "景气",
        "需求",
        "市场",
        "技术",
        "监管",
        "安全",
        "金刚石",
        "散热",
        "tvl",
        "fees",
        "revenue",
    )
    ranked = []
    for row in rows:
        text = row["claim_text"]
        lowered = text.lower()
        score = sum(1 for token in thesis_tokens if token in lowered)
        score += _keyword_hits(text, domain_keywords)
        score += 1 if 16 <= len(text) <= 80 else 0
        ranked.append((score, -len(text), _preview(text, 72)))
    ranked.sort(reverse=True)
    return ranked[0][2]


def _source_validation_state(
    *,
    primaryness: str,
    source_type: str,
    pending_route_count: int,
    validated_case_count: int,
    corroborated_accept_count: int,
) -> str:
    lane = _source_lane(source_type, primaryness)
    if lane == "personal":
        return "thesis_seeding"
    if lane == "official":
        return "first_hand_feed"
    if validated_case_count > 0 and pending_route_count > 0:
        return "partially_validated"
    if validated_case_count > 0:
        return "validated"
    if corroborated_accept_count > 0 and pending_route_count > 0:
        return "partially_validated"
    if corroborated_accept_count > 0:
        return "validated"
    if pending_route_count > 0:
        return "pending_validation"
    return "watching"


def _thesis_validation_state(conn: Any, thesis: dict[str, Any]) -> str:
    if thesis["status"] == "active":
        return "active"
    if thesis["status"] == "evidence_backed":
        return "evidence_backed"
    artifact_ids = thesis["version"].get("created_from_artifacts", [])
    artifact_sources = _artifact_source_map(conn, artifact_ids)
    if any(row["primaryness"] == "personal" for row in artifact_sources.values()):
        return "personal_seed"
    if thesis["promotion_gate"]["requires_corroborated_first_hand"]:
        return "needs_corroboration"
    return "first_hand_feed"


def _decision_reason(thesis: dict[str, Any], target_case: dict[str, Any], action_state: str) -> str:
    timing = target_case.get("timing_plan") or {}
    parts: list[str] = []
    if target_case["monitor_summary"]["alerted_count"] > 0:
        parts.append("monitor 已触发")
    if thesis["status"] == "active":
        parts.append("thesis 已 active")
    elif thesis["status"] == "evidence_backed":
        parts.append("thesis 已 evidence-backed")
    elif thesis["promotion_gate"]["requires_corroborated_first_hand"]:
        parts.append("仍带二手线索核验语义")
    catalyst = next((item for item in timing.get("catalysts", []) if item), "")
    if catalyst:
        parts.append(f"催化剂={catalyst}")
    if action_state in {"trim", "exit"} and thesis["promotion_gate"]["active_missing"]:
        parts.append(f"active gate 缺口={','.join(thesis['promotion_gate']['active_missing'])}")
    elif thesis["promotion_gate"]["missing"]:
        parts.append(f"缺口={','.join(thesis['promotion_gate']['missing'])}")
    return "；".join(parts)


def _verification_priority(item: dict[str, Any]) -> tuple[int, int, float, str]:
    claim_type_order = {
        "fact": 0,
        "catalyst": 1,
        "forecast": 2,
        "risk": 3,
        "counterpoint": 4,
        "viewpoint": 5,
    }
    text = item.get("claim_text", "") or ""
    noise_fragments = [
        "希望这场",
        "生命与和平",
        "不是没本事",
        "再也回不来了",
        "补齐语境",
        "真的看不清",
    ]
    noise_penalty = 1 if any(fragment in text for fragment in noise_fragments) else 0
    candidate_bonus = 0 if item.get("candidate_theses") else 1
    return (
        noise_penalty,
        candidate_bonus,
        claim_type_order.get(item.get("claim_type", ""), 99),
        -(item.get("confidence") or 0.0),
    )


def _is_high_signal_verification_claim(item: dict[str, Any]) -> bool:
    text = item.get("claim_text", "") or ""
    claim_type = item.get("claim_type", "")
    template_noise = [
        "本 digest 为自动流水线产物",
        "优先阅读完整转写",
        "无需人工全片回看",
        "若视频为情绪/观点类口播",
        "补齐语境",
    ]
    if any(fragment in text for fragment in template_noise):
        return False
    signal_keywords = [
        "算力",
        "订单",
        "公告",
        "业绩",
        "财报",
        "扩产",
        "需求",
        "估值",
        "市场规模",
        "科技成长股",
        "金属化工股",
        "TVL",
        "fees",
        "revenue",
        "filing",
        "guidance",
        "capex",
    ]
    if claim_type in {"catalyst", "risk", "forecast"} and len(text) >= 18:
        return True
    return bool(re.search(r"\d", text)) or any(keyword in text for keyword in signal_keywords)


def _collect_verification_candidates(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    route_workbench = build_route_workbench(conn, limit=limit)
    candidates = [
        {
            "route_id": item["route_id"],
            "source_id": item["source_id"],
            "source_name": item["source_name"],
            "artifact_id": item["artifact_id"],
            "artifact_title": item["artifact_title"],
            "route_type": item["route_type"],
            "claim_type": item["claim_type"],
            "claim_preview": _preview(item["claim_text"], 120),
            "candidate_theses": item["candidate_theses"][:3],
            "suggested_action": item["suggested_action"],
            "confidence": item["confidence"],
        }
        for item in route_workbench["items"]
        if item["route_type"] == "corroboration_needed" and item["claim_type"] != "viewpoint" and _is_high_signal_verification_claim(item)
    ]
    candidates.sort(key=_verification_priority)
    return candidates


def _build_promotion_gate(
    conn: Any,
    thesis_id: str,
    current_version_id: str,
    version: dict[str, Any],
    target_case_count: int,
) -> dict[str, Any]:
    artifact_ids = _json_loads(version.get("created_from_artifacts_json"), [])
    artifact_source_map = _artifact_source_map(conn, artifact_ids)
    claims = _claims_for_artifacts(conn, artifact_ids)
    first_hand_rows = [row for row in artifact_source_map.values() if row["primaryness"] == "first_hand"]
    second_hand_rows = [
        row for row in artifact_source_map.values() if row["primaryness"] == "second_hand" or row["source_type"] == "kol"
    ]
    counterpoint_rows: list[dict[str, Any]] = []
    counter_route_rows: list[dict[str, Any]] = []
    pending_corroboration_rows: list[dict[str, Any]] = []
    if artifact_ids:
        placeholders = ",".join("?" for _ in artifact_ids)
        counterpoint_rows = [
            dict(row)
            for row in list_rows(
                conn,
                f"SELECT claim_id FROM claims WHERE artifact_id IN ({placeholders}) AND claim_type = 'counterpoint'",
                tuple(artifact_ids),
            )
        ]
        counter_route_rows = [
            dict(row)
            for row in list_rows(
                conn,
                f"SELECT route_id FROM claim_routes WHERE artifact_id IN ({placeholders}) AND route_type = 'counter_search' AND status IN ('pending', 'accepted')",
                tuple(artifact_ids),
            )
        ]
        pending_corroboration_rows = [
            dict(row)
            for row in list_rows(
                conn,
                f"SELECT route_id FROM claim_routes WHERE artifact_id IN ({placeholders}) AND route_type = 'corroboration_needed' AND status = 'pending'",
                tuple(artifact_ids),
            )
        ]
    accepted_corroboration_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT DISTINCT cr.route_id, a.artifact_id
            FROM claim_routes cr
            LEFT JOIN claim_route_links target_link
              ON target_link.route_id = cr.route_id
             AND target_link.linked_object_type IN ('thesis', 'thesis_version')
            LEFT JOIN claim_route_links evidence_link
              ON evidence_link.route_id = cr.route_id
             AND evidence_link.linked_object_type = 'artifact'
            LEFT JOIN artifacts a ON a.artifact_id = evidence_link.linked_object_id
            LEFT JOIN sources s ON s.source_id = a.source_id
            WHERE cr.route_type = 'corroboration_needed'
              AND cr.status = 'accepted'
              AND (
                    (cr.target_object_type = 'thesis' AND cr.target_object_id = ?)
                 OR (cr.target_object_type = 'thesis_version' AND cr.target_object_id = ?)
                 OR target_link.linked_object_id = ?
                 OR target_link.linked_object_id = ?
              )
              AND s.primaryness = 'first_hand'
            """,
            (thesis_id, current_version_id, thesis_id, current_version_id),
        )
    ]
    validated_corroboration_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT validation_case_id
            FROM validation_cases
            WHERE verdict = 'validated'
              AND (
                    thesis_id = ?
                 OR thesis_version_id = ?
              )
            """,
            (thesis_id, current_version_id),
        )
    ]
    accepted_corroboration_count = len(accepted_corroboration_rows)
    validated_corroboration_count = len(validated_corroboration_rows)
    pending_corroboration_count = len(pending_corroboration_rows)
    requires_corroborated_first_hand = len(second_hand_rows) > 0
    fatal_domain_warning_count = sum(1 for claim in claims if _claim_has_fatal_domain_warning(claim))
    incomplete_provenance_count = sum(
        1
        for claim in claims
        if not _claim_provenance_complete(claim, artifact_source_map.get(claim.get("artifact_id", "")))
    )
    unresolved_conflicts = [
        item
        for item in detect_conflicts(build_graph_from_db(conn, thesis_id=thesis_id))
        if not item["resolved"]
    ]
    checks = {
        "has_first_hand_artifact": len(first_hand_rows) > 0,
        "has_invalidator": bool((version.get("invalidators") or "").strip()),
        "has_target_mapping": target_case_count > 0,
        "has_counter_material": bool((version.get("counter_case") or "").strip()) or len(counterpoint_rows) > 0 or len(counter_route_rows) > 0,
        "no_fatal_domain_warnings": fatal_domain_warning_count == 0,
    }
    if requires_corroborated_first_hand:
        checks["has_corroborated_first_hand"] = validated_corroboration_count > 0
    missing = [name for name, ok in checks.items() if not ok]
    active_checks: dict[str, bool] = {}
    active_checks["claim_provenance_complete"] = incomplete_provenance_count == 0
    active_checks["no_unresolved_conflicts"] = len(unresolved_conflicts) == 0
    if requires_corroborated_first_hand:
        active_checks["corroboration_debt_under_control"] = pending_corroboration_count <= max(5, validated_corroboration_count * 5)
    active_missing = [name for name, ok in active_checks.items() if not ok]
    corroboration_debt_ratio = None
    if requires_corroborated_first_hand:
        corroboration_debt_ratio = round(pending_corroboration_count / max(validated_corroboration_count, 1), 2)
    return {
        "checks": checks,
        "missing": missing,
        "can_promote_to_evidence_backed": not missing,
        "active_checks": active_checks,
        "active_missing": active_missing,
        "can_recommend_active": not missing and not active_missing,
        "requires_corroborated_first_hand": requires_corroborated_first_hand,
        "first_hand_artifact_count": len(first_hand_rows),
        "second_hand_artifact_count": len(second_hand_rows),
        "accepted_corroboration_count": accepted_corroboration_count,
        "validated_corroboration_count": validated_corroboration_count,
        "pending_corroboration_count": pending_corroboration_count,
        "corroboration_debt_ratio": corroboration_debt_ratio,
        "target_case_count": target_case_count,
        "fatal_domain_warning_count": fatal_domain_warning_count,
        "incomplete_provenance_count": incomplete_provenance_count,
        "unresolved_conflict_count": len(unresolved_conflicts),
        "unresolved_conflicts": unresolved_conflicts[:8],
    }


def build_thesis_board(conn: Any) -> dict[str, Any]:
    ctx = _load_context(conn)
    items: list[dict[str, Any]] = []
    for thesis in sorted(
        ctx["theses"],
        key=lambda item: (_sort_key_by_order(item["status"], THESIS_STATUS_ORDER), item["created_at"]),
    ):
        theme_ids = _json_loads(thesis.get("theme_ids_json"), [])
        version = ctx["version_by_id"].get(thesis.get("current_version_id"), {})
        target_cases = []
        alert_count = 0
        monitor_count = 0
        thesis_reviews = ctx["reviews_by_owner"].get(thesis["thesis_id"], [])
        latest_thesis_review = thesis_reviews[0] if thesis_reviews else None
        for target_case in ctx["target_cases_by_version"].get(thesis.get("current_version_id"), []):
            target = ctx["target_by_id"].get(target_case["target_id"], {})
            timing = ctx["timing_by_target_case"].get(target_case["target_case_id"])
            monitors = ctx["monitors_by_owner"].get(target_case["target_case_id"], [])
            reviews = ctx["reviews_by_owner"].get(target_case["target_case_id"], [])
            target_alert_count = sum(1 for row in monitors if row["status"] == "alerted")
            alert_count += target_alert_count
            monitor_count += len(monitors)
            latest_target_case_review = reviews[0] if reviews else None
            effective_review = _build_effective_review(
                target_case_review=latest_target_case_review,
                thesis_review=latest_thesis_review,
            )
            target_cases.append(
                {
                    "target_case_id": target_case["target_case_id"],
                    "status": target_case["status"],
                    "exposure_type": target_case["exposure_type"],
                    "capture_link_strength": target_case["capture_link_strength"],
                    "target": {
                        "target_id": target.get("target_id"),
                        "ticker_or_symbol": target.get("ticker_or_symbol"),
                        "asset_class": target.get("asset_class"),
                        "venue": target.get("venue"),
                        "currency": target.get("currency"),
                    },
                    "timing_plan": {
                        "timing_plan_id": timing["timing_plan_id"],
                        "desired_posture": timing["desired_posture"],
                        "window_type": timing["window_type"],
                        "catalysts": _json_loads(timing.get("catalysts_json"), []),
                        "confirmation_signals": _json_loads(timing.get("confirmation_signals_json"), []),
                        "preconditions": _json_loads(timing.get("preconditions_json"), []),
                        "invalidators": _json_loads(timing.get("invalidators_json"), []),
                    }
                    if timing
                    else None,
                    "monitor_summary": {
                        "count": len(monitors),
                        "alerted_count": target_alert_count,
                    },
                    "latest_review": latest_target_case_review,
                    "effective_review": effective_review,
                }
            )
        promotion_gate = _build_promotion_gate(
            conn,
            thesis["thesis_id"],
            thesis.get("current_version_id"),
            version,
            len(target_cases),
        )
        items.append(
            {
                "thesis_id": thesis["thesis_id"],
                "title": thesis["title"],
                "status": thesis["status"],
                "horizon_months": thesis["horizon_months"],
                "owner": thesis.get("owner"),
                "themes": [
                    {
                        "theme_id": theme_id,
                        "name": ctx["theme_by_id"].get(theme_id, {}).get("name"),
                        "importance_status": ctx["theme_by_id"].get(theme_id, {}).get("importance_status"),
                    }
                    for theme_id in theme_ids
                ],
                "version": {
                    "thesis_version_id": version.get("thesis_version_id"),
                    "statement": version.get("statement"),
                    "mechanism_chain": version.get("mechanism_chain"),
                    "why_now": version.get("why_now"),
                    "base_case": version.get("base_case"),
                    "counter_case": version.get("counter_case"),
                    "invalidators": version.get("invalidators"),
                    "created_from_artifacts": _json_loads(version.get("created_from_artifacts_json"), []),
                },
                "promotion_gate": promotion_gate,
                "target_case_count": len(target_cases),
                "monitor_count": monitor_count,
                "alerted_monitor_count": alert_count,
                "latest_review": latest_thesis_review,
                "target_cases": target_cases,
            }
        )
    return {"summary": summary_counts(conn), "items": items}


def build_theme_map(conn: Any) -> dict[str, Any]:
    ctx = _load_context(conn)
    board = build_thesis_board(conn)
    thesis_by_theme: dict[str, list[dict[str, Any]]] = {}
    for item in board["items"]:
        for theme in item["themes"]:
            thesis_by_theme.setdefault(theme["theme_id"], []).append(item)
    items = []
    for theme in sorted(
        ctx["themes"],
        key=lambda item: (_sort_key_by_order(item["importance_status"], THEME_STATUS_ORDER), item["name"]),
    ):
        theses = thesis_by_theme.get(theme["theme_id"], [])
        target_case_count = sum(item["target_case_count"] for item in theses)
        alert_count = sum(item["alerted_monitor_count"] for item in theses)
        items.append(
            {
                "theme_id": theme["theme_id"],
                "name": theme["name"],
                "importance_status": theme["importance_status"],
                "why_it_matters": theme.get("why_it_matters"),
                "maturity_stage": theme.get("maturity_stage"),
                "commercialization_paths": theme.get("commercialization_paths"),
                "thesis_count": len(theses),
                "active_thesis_count": sum(1 for item in theses if item["status"] == "active"),
                "target_case_count": target_case_count,
                "alerted_monitor_count": alert_count,
                "theses": [
                    {
                        "thesis_id": item["thesis_id"],
                        "title": item["title"],
                        "status": item["status"],
                    }
                    for item in theses
                ],
            }
        )
    return {"summary": summary_counts(conn), "items": items}


def build_watch_board(conn: Any) -> dict[str, Any]:
    board = build_thesis_board(conn)
    sentinel_board = build_sentinel_board(conn, limit=40)
    projection_items = sentinel_board["items"]
    posture_order = {"prepare": 0, "starter": 1, "add_on_confirmation": 2, "observe": 3, "avoid": 4, "exit_watch": 5}
    items = []
    for thesis in board["items"]:
        for target_case in thesis["target_cases"]:
            timing = target_case.get("timing_plan") or {}
            target_ref = target_case.get("target")
            target_text = json.dumps(target_ref, ensure_ascii=False) if isinstance(target_ref, dict) else str(target_ref or "")
            matched_projections = [
                projection
                for projection in projection_items
                if projection.get("linked_target_case_id") == target_case["target_case_id"]
                or projection.get("linked_thesis_id") == thesis["thesis_id"]
                or (projection.get("entity") and projection["entity"] in target_text)
            ]
            items.append(
                {
                    "target_case_id": target_case["target_case_id"],
                    "thesis_id": thesis["thesis_id"],
                    "thesis_title": thesis["title"],
                    "thesis_status": thesis["status"],
                    "themes": thesis["themes"],
                    "target": target_case["target"],
                    "target_case_status": target_case["status"],
                    "desired_posture": timing.get("desired_posture"),
                    "window_type": timing.get("window_type"),
                    "catalysts": timing.get("catalysts", []),
                    "confirmation_signals": timing.get("confirmation_signals", []),
                    "monitor_count": target_case["monitor_summary"]["count"],
                    "alerted_monitor_count": target_case["monitor_summary"]["alerted_count"],
                    "latest_review": (target_case.get("effective_review") or {}).get("review"),
                    "effective_review_source": (target_case.get("effective_review") or {}).get("source"),
                    "review_freshness": (target_case.get("effective_review") or {}).get("freshness"),
                    "event_mining": {
                        "projection_count": len(matched_projections),
                        "interrupt_count": sum(1 for projection in matched_projections if projection.get("last_route") == "interrupt"),
                        "overdue_count": sum(1 for projection in matched_projections if projection.get("stall_status") == "overdue"),
                        "anti_thesis_due_count": sum(
                            1 for projection in matched_projections if int(projection.get("pending_anti_thesis_count") or 0) > 0
                        ),
                        "items": matched_projections[:4],
                    },
                }
            )
    items.sort(
        key=lambda item: (
            0 if item["alerted_monitor_count"] else 1,
            posture_order.get(item["desired_posture"] or "", 99),
            item["thesis_title"],
        )
    )
    return {
        "summary": {
            **summary_counts(conn),
            "watch_items": len(items),
            "alerted_watch_items": sum(1 for item in items if item["alerted_monitor_count"] > 0),
            "event_projection_items": sentinel_board["summary"]["projection_items"],
            "event_interrupt_items": sentinel_board["summary"]["interrupt_items"],
            "event_overdue_items": sentinel_board["summary"]["overdue_items"],
            "event_anti_thesis_due_items": sentinel_board["summary"]["pending_anti_thesis_items"],
        },
        "event_mining": sentinel_board,
        "items": items,
    }


def build_event_ledger(conn: Any, *, limit: int = 20, route: str = "") -> dict[str, Any]:
    where = ""
    params: list[Any] = []
    if route:
        where = "WHERE route = ?"
        params.append(route)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT event_id, entity, product, event_type, stage_from, stage_to,
                   source_role, source_tier, root_claim_key, independence_group,
                   evidence_text, evidence_url, evidence_date,
                   event_time, first_seen_time, processed_time, novelty, relevance,
                   impact, confidence, mapped_trigger, candidate_thesis,
                   residual_class, residual_target, route_reason, state_applied,
                   dedup_group_size, corroboration_count, route, projection_id, candidate_id
            FROM event_mining_events
            {where}
            ORDER BY processed_time DESC, created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        )
    ]
    route_counts = {
        route_name: select_one(
            conn,
            "SELECT COUNT(*) AS cnt FROM event_mining_events WHERE route = ?",
            (route_name,),
        )["cnt"]
        for route_name in ("interrupt", "review", "opportunity", "archive")
    }
    return {
        "summary": {
            **summary_counts(conn),
            "event_items": len(rows),
            "interrupt_items": route_counts["interrupt"],
            "review_items": route_counts["review"],
            "opportunity_items": route_counts["opportunity"],
            "archive_items": route_counts["archive"],
            "unique_independence_groups": int(
                select_one(conn, "SELECT COUNT(DISTINCT independence_group) AS cnt FROM event_mining_events", ())["cnt"]
            ),
            "filter_route": route or None,
        },
        "items": rows,
    }


def build_sentinel_board(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT p.*,
                   (
                     SELECT COUNT(*)
                     FROM event_mining_events e
                     WHERE e.projection_id = p.projection_id
                   ) AS event_count,
                   (
                     SELECT e.route
                     FROM event_mining_events e
                     WHERE e.projection_id = p.projection_id
                     ORDER BY e.processed_time DESC, e.created_at DESC
                     LIMIT 1
                   ) AS latest_event_route
            FROM event_state_projections p
            ORDER BY
              CASE p.stall_status WHEN 'overdue' THEN 0 ELSE 1 END,
              CASE p.last_route WHEN 'interrupt' THEN 0 WHEN 'review' THEN 1 ELSE 2 END,
              p.updated_at DESC,
              p.entity ASC
            LIMIT ?
            """,
            (limit,),
        )
    ]
    for row in rows:
        row["notes"] = _json_loads(row.get("notes_json"), {})
    return {
            "summary": {
                **summary_counts(conn),
                "projection_items": len(rows),
                "interrupt_items": sum(1 for row in rows if row.get("last_route") == "interrupt"),
                "review_items": sum(1 for row in rows if row.get("last_route") == "review"),
                "overdue_items": sum(1 for row in rows if row.get("stall_status") == "overdue"),
                "pending_anti_thesis_items": sum(1 for row in rows if int(row.get("pending_anti_thesis_count") or 0) > 0),
            },
        "items": rows,
    }


def build_opportunity_inbox(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT candidate_id, schema_version, thesis_name, status, route, residual_class,
                   adjacent_projection_ids_json, cluster_score,
                   persistence_score, corroboration_score, investability_score,
                   raw_event_count, independence_group_count, attention_capture_ratio,
                   anti_thesis_status, last_source_tier,
                   earliest_event_time, latest_event_time, last_event_id,
                   next_proving_milestone, note, created_at, updated_at
            FROM opportunity_candidates
            ORDER BY updated_at DESC, cluster_score DESC, investability_score DESC
            LIMIT ?
            """,
            (limit,),
        )
    ]
    return {
        "summary": {
            **summary_counts(conn),
            "candidate_items": len(rows),
            "open_items": sum(1 for row in rows if row.get("status") == "open"),
            "anti_thesis_due_items": sum(1 for row in rows if row.get("anti_thesis_status") == "due"),
        },
        "items": rows,
    }


def _theme_radar_score(row: dict[str, Any]) -> float:
    base = (
        float(row.get("cluster_score") or 0.0) * 0.35
        + float(row.get("persistence_score") or 0.0) * 0.20
        + float(row.get("corroboration_score") or 0.0) * 0.20
        + float(row.get("investability_score") or 0.0) * 0.25
    )
    attention_capture_ratio = float(row.get("attention_capture_ratio") or 0.0)
    noise_penalty = max(0.0, attention_capture_ratio - 1.0) * 0.35
    source_bonus = {"primary": 0.25, "secondary": 0.1, "tertiary": -0.15}.get(
        str(row.get("last_source_tier") or ""),
        0.0,
    )
    anti_penalty = {"due": 0.25, "recorded": 0.1, "dismissed": -0.05}.get(
        str(row.get("anti_thesis_status") or ""),
        0.0,
    )
    return round(base + source_bonus - noise_penalty - anti_penalty, 3)


def build_theme_radar_board(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT candidate_id, schema_version, thesis_name, status, route, residual_class,
                   adjacent_projection_ids_json, cluster_score, persistence_score,
                   corroboration_score, investability_score, raw_event_count,
                   independence_group_count, attention_capture_ratio, anti_thesis_status,
                   last_source_tier, earliest_event_time, latest_event_time, last_event_id,
                   next_proving_milestone, note, created_at, updated_at
            FROM opportunity_candidates
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (max(limit * 3, limit),),
        )
    ]
    items: list[dict[str, Any]] = []
    for row in rows:
        ranking_score = _theme_radar_score(row)
        next_action = "keep_scanning"
        if str(row.get("status") or "") == "open" and ranking_score >= 1.25:
            next_action = "prepare_candidate"
        elif str(row.get("anti_thesis_status") or "") == "due":
            next_action = "anti_thesis_due"
        elif float(row.get("attention_capture_ratio") or 0.0) > 2.0:
            next_action = "noise_check"
        items.append(
            {
                **row,
                "ranking_score": ranking_score,
                "next_action": next_action,
            }
        )
    items = sorted(
        items,
        key=lambda row: (
            -float(row.get("ranking_score") or 0.0),
            {"open": 0, "promoted": 1, "deferred": 2, "dismissed": 3}.get(str(row.get("status") or ""), 9),
            str(row.get("updated_at") or ""),
        ),
    )[:limit]
    return {
        "summary": {
            **summary_counts(conn),
            "radar_items": len(items),
            "prepare_candidates": sum(1 for row in items if row.get("next_action") == "prepare_candidate"),
            "anti_thesis_due_items": sum(1 for row in items if row.get("anti_thesis_status") == "due"),
            "avg_ranking_score": (
                round(sum(float(row.get("ranking_score") or 0.0) for row in items) / len(items), 3)
                if items
                else None
            ),
        },
        "items": items,
    }


def build_anti_thesis_board(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT check_id, object_type, object_id, target_label, status, due_reason,
                   trigger_event_id, prompt, result_summary, contradiction_score,
                   created_at, updated_at
            FROM anti_thesis_checks
            ORDER BY CASE status WHEN 'due' THEN 0 WHEN 'recorded' THEN 1 ELSE 2 END,
                     updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )
    ]
    return {
        "summary": {
            **summary_counts(conn),
            "anti_thesis_items": len(rows),
            "due_items": sum(1 for row in rows if row.get("status") == "due"),
            "recorded_items": sum(1 for row in rows if row.get("status") == "recorded"),
        },
        "items": rows,
    }


def build_event_evaluation_board(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT feedback_id, object_type, object_id, feedback_type, verdict, score,
                   note, related_event_id, related_candidate_id, metadata_json, created_at
            FROM event_mining_feedback
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
    ]
    interrupt_tp = int(
        select_one(
            conn,
            "SELECT COUNT(*) AS cnt FROM event_mining_feedback WHERE feedback_type = 'interrupt_verdict' AND verdict = 'true_positive'",
            (),
        )["cnt"]
    )
    interrupt_fp = int(
        select_one(
            conn,
            "SELECT COUNT(*) AS cnt FROM event_mining_feedback WHERE feedback_type = 'interrupt_verdict' AND verdict = 'false_positive'",
            (),
        )["cnt"]
    )
    candidate_promote = int(
        select_one(
            conn,
            "SELECT COUNT(*) AS cnt FROM event_mining_feedback WHERE feedback_type = 'candidate_verdict' AND verdict = 'promote'",
            (),
        )["cnt"]
    )
    candidate_dismiss = int(
        select_one(
            conn,
            "SELECT COUNT(*) AS cnt FROM event_mining_feedback WHERE feedback_type = 'candidate_verdict' AND verdict = 'dismiss'",
            (),
        )["cnt"]
    )
    missed_signals = int(
        select_one(
            conn,
            "SELECT COUNT(*) AS cnt FROM event_mining_feedback WHERE feedback_type = 'missed_signal'",
            (),
        )["cnt"]
    )
    avg_lag_hours = select_one(
        conn,
        """
        SELECT AVG((julianday(first_seen_time) - julianday(event_time)) * 24.0) AS lag_hours
        FROM event_mining_events
        """,
        (),
    )["lag_hours"]
    return {
        "summary": {
            **summary_counts(conn),
            "feedback_items": len(rows),
            "interrupt_precision": (
                float(interrupt_tp) / float(interrupt_tp + interrupt_fp)
                if (interrupt_tp + interrupt_fp) > 0
                else None
            ),
            "interrupt_true_positive": interrupt_tp,
            "interrupt_false_positive": interrupt_fp,
            "candidate_promote": candidate_promote,
            "candidate_dismiss": candidate_dismiss,
            "missed_signals": missed_signals,
            "avg_detection_lag_hours": round(float(avg_lag_hours), 2) if avg_lag_hours is not None else None,
        },
        "items": rows,
    }


def build_target_case_dashboard(conn: Any) -> dict[str, Any]:
    board = build_thesis_board(conn)
    posture_order = {"prepare": 0, "starter": 1, "add_on_confirmation": 2, "observe": 3, "avoid": 4, "exit_watch": 5}
    items: list[dict[str, Any]] = []
    for thesis in board["items"]:
        for target_case in thesis["target_cases"]:
            timing = target_case.get("timing_plan")
            monitor_summary = target_case["monitor_summary"]
            latest_review = target_case.get("latest_review")
            effective_review = target_case.get("effective_review") or _build_effective_review(
                target_case_review=latest_review,
                thesis_review=thesis.get("latest_review"),
            )
            review_freshness = effective_review["freshness"]
            review_age_days = effective_review["age_days"]
            actionability_gate = {
                "has_timing_plan": timing is not None,
                "has_monitor": monitor_summary["count"] > 0,
                "has_direct_review": latest_review is not None,
                "has_effective_review": effective_review["has_review"],
                "has_recent_review": effective_review["has_review"],
                "has_current_review": review_freshness in {"fresh", "aging"},
                "has_fresh_review": review_freshness == "fresh",
            }
            actionability_gate["is_ready"] = (
                actionability_gate["has_timing_plan"]
                and actionability_gate["has_monitor"]
                and actionability_gate["has_current_review"]
            )
            items.append(
                {
                    "target_case_id": target_case["target_case_id"],
                    "status": target_case["status"],
                    "thesis_id": thesis["thesis_id"],
                    "thesis_title": thesis["title"],
                    "thesis_status": thesis["status"],
                    "themes": thesis["themes"],
                    "target": target_case["target"],
                    "exposure_type": target_case["exposure_type"],
                    "capture_link_strength": target_case["capture_link_strength"],
                    "timing_plan": timing,
                    "monitor_summary": monitor_summary,
                    "latest_review": latest_review,
                    "effective_review": effective_review["review"],
                    "effective_review_source": effective_review["source"],
                    "effective_review_gap_type": effective_review["gap_type"],
                    "review_freshness": review_freshness,
                    "review_age_days": review_age_days,
                    "actionability_gate": actionability_gate,
                }
            )
    items.sort(
        key=lambda item: (
            0 if item["monitor_summary"]["alerted_count"] else 1,
            posture_order.get((item["timing_plan"] or {}).get("desired_posture") or "", 99),
            item["thesis_title"],
            item["target"]["ticker_or_symbol"] or "",
        )
    )
    return {
        "summary": {
            **summary_counts(conn),
            "target_case_items": len(items),
            "actionable_or_active": sum(1 for item in items if item["status"] in {"actionable", "active"}),
            "ready_items": sum(1 for item in items if item["actionability_gate"]["is_ready"]),
            "fallback_review_items": sum(1 for item in items if item["effective_review_source"] == "thesis_fallback"),
            "missing_timing_plan": sum(1 for item in items if not item["actionability_gate"]["has_timing_plan"]),
            "missing_monitor": sum(1 for item in items if not item["actionability_gate"]["has_monitor"]),
            "missing_recent_review": sum(1 for item in items if not item["actionability_gate"]["has_recent_review"]),
            "missing_effective_review": sum(1 for item in items if not item["actionability_gate"]["has_effective_review"]),
            "missing_current_review": sum(1 for item in items if not item["actionability_gate"]["has_current_review"]),
            "missing_fresh_review": sum(1 for item in items if not item["actionability_gate"]["has_fresh_review"]),
        },
        "items": items,
    }


def build_thesis_focus(conn: Any, thesis_id: str, *, limit: int = 20) -> dict[str, Any]:
    board = build_thesis_board(conn)
    thesis = next((item for item in board["items"] if item["thesis_id"] == thesis_id), None)
    if thesis is None:
        return {
            "summary": {**summary_counts(conn), "found": False},
            "thesis_id": thesis_id,
            "error": "thesis_not_found",
        }

    version = thesis["version"]
    version_id = version.get("thesis_version_id")
    created_artifact_ids = version.get("created_from_artifacts", [])
    source_board = build_source_board(conn)
    source_index = {item["source_id"]: item for item in source_board["items"]}
    linked_sources = _linked_sources_for_thesis(conn, thesis, source_index)

    artifact_map = _artifact_source_map(conn, created_artifact_ids)
    thesis_source_support = _decision_source_support(linked_sources)
    provenance: list[dict[str, Any]] = []
    for artifact_id in created_artifact_ids:
        row = artifact_map.get(artifact_id)
        if row is None:
            continue
        artifact = select_one(
            conn,
            """
            SELECT artifact_id, artifact_kind, title, captured_at, published_at, status, metadata_json
            FROM artifacts
            WHERE artifact_id = ?
            """,
            (artifact_id,),
        )
        artifact_row = dict(artifact) if artifact is not None else {}
        provenance.append(
            {
                "artifact_id": artifact_id,
                "artifact_kind": artifact_row.get("artifact_kind"),
                "title": row.get("title") or artifact_row.get("title"),
                "captured_at": artifact_row.get("captured_at"),
                "published_at": artifact_row.get("published_at"),
                "status": artifact_row.get("status"),
                "source": {
                    "source_id": row["source_id"],
                    "name": row["source_name"],
                    "source_type": row["source_type"],
                    "primaryness": row["primaryness"],
                    "source_display_label": source_index.get(row["source_id"], {}).get("source_display_label")
                    or _source_display_label(
                        primaryness=row["primaryness"],
                        source_type=row["source_type"],
                        source_priority_label="unproven",
                    ),
                },
            }
        )

    thesis_focus_target_cases: list[dict[str, Any]] = []
    target_case_ids: list[str] = []
    timing_plan_ids: list[str] = []
    monitor_ids: list[str] = []
    review_ids: list[str] = []
    for target_case in thesis["target_cases"]:
        target_case_ids.append(target_case["target_case_id"])
        timing = target_case.get("timing_plan") or {}
        if timing.get("timing_plan_id"):
            timing_plan_ids.append(timing["timing_plan_id"])
        latest_review = target_case.get("latest_review")
        if latest_review and latest_review.get("review_id"):
            review_ids.append(latest_review["review_id"])
        monitors = [
            dict(row)
            for row in list_rows(
                conn,
                """
                SELECT monitor_id, monitor_type, metric_name, comparator, threshold_value, latest_value, status, last_checked_at
                FROM monitors
                WHERE owner_object_type = 'target_case' AND owner_object_id = ?
                ORDER BY created_at
                """,
                (target_case["target_case_id"],),
            )
        ]
        monitor_ids.extend([row["monitor_id"] for row in monitors])
        current_decision = _latest_operator_decisions(conn, [target_case["target_case_id"]]).get(target_case["target_case_id"])
        target_case_source_support = (
            _decision_source_support(current_decision.get("linked_sources", []))
            if current_decision
            else thesis_source_support
        )
        thesis_focus_target_cases.append(
            {
                **target_case,
                "monitors": monitors,
                "current_decision": current_decision,
                "source_confidence": target_case_source_support["source_confidence"],
                "source_confidence_reason": target_case_source_support["source_confidence_reason"],
            }
        )

    validation_board = build_validation_board(conn, thesis_id=thesis_id, limit=max(limit * 4, 50))
    if target_case_ids:
        review_rows = [
            dict(row)
            for row in list_rows(
                conn,
                f"""
                SELECT review_id, owner_object_type, owner_object_id, review_date, what_we_believed, what_happened,
                       result, source_attribution, source_ids_json, claim_ids_json, lessons, created_at
                FROM reviews
                WHERE owner_object_id = ?
                   OR owner_object_id IN ({",".join("?" for _ in target_case_ids)})
                ORDER BY review_date DESC, created_at DESC
                """,
                (thesis_id, *target_case_ids),
            )
        ]
    else:
        review_rows = [
            dict(row)
            for row in list_rows(
                conn,
                """
                SELECT review_id, owner_object_type, owner_object_id, review_date, what_we_believed, what_happened,
                       result, source_attribution, source_ids_json, claim_ids_json, lessons, created_at
                FROM reviews
                WHERE owner_object_id = ?
                ORDER BY review_date DESC, created_at DESC
                """,
                (thesis_id,),
            )
        ]
    reviews = []
    for row in review_rows[: max(limit * 2, 20)]:
        row["source_ids"] = _json_loads(row.get("source_ids_json"), [])
        row["claim_ids"] = _json_loads(row.get("claim_ids_json"), [])
        reviews.append(row)
        review_ids.append(row["review_id"])

    route_where = [
        "(cr.target_object_type = 'thesis' AND cr.target_object_id = ?)",
        "(cr.target_object_type = 'thesis_version' AND cr.target_object_id = ?)",
        "(crl.linked_object_type = 'thesis' AND crl.linked_object_id = ?)",
        "(crl.linked_object_type = 'thesis_version' AND crl.linked_object_id = ?)",
    ]
    route_params: list[Any] = [thesis_id, version_id, thesis_id, version_id]
    if target_case_ids:
        placeholders = ",".join("?" for _ in target_case_ids)
        route_where.append(f"(crl.linked_object_type = 'target_case' AND crl.linked_object_id IN ({placeholders}))")
        route_params.extend(target_case_ids)
    route_rows = [
        dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT DISTINCT cr.route_id, cr.claim_id, cr.artifact_id, cr.route_type, cr.status, cr.reason,
                   cr.target_object_type, cr.target_object_id, c.claim_type, c.claim_text, c.confidence,
                   c.speaker, c.timecode_or_span, a.title AS artifact_title, a.captured_at,
                   s.source_id, s.name AS source_name, s.source_type, s.primaryness
            FROM claim_routes cr
            JOIN claims c ON c.claim_id = cr.claim_id
            JOIN artifacts a ON a.artifact_id = cr.artifact_id
            JOIN sources s ON s.source_id = a.source_id
            LEFT JOIN claim_route_links crl ON crl.route_id = cr.route_id
            WHERE {' OR '.join(route_where)}
            ORDER BY a.captured_at DESC, cr.created_at DESC
            LIMIT ?
            """,
            (*route_params, max(limit * 6, 80)),
        )
    ]
    route_ids = [row["route_id"] for row in route_rows]
    route_links_by_route: dict[str, list[dict[str, Any]]] = {}
    if route_ids:
        placeholders = ",".join("?" for _ in route_ids)
        for row in list_rows(
            conn,
            f"""
            SELECT route_id, link_kind, linked_object_type, linked_object_id, note, metadata_json, created_at
            FROM claim_route_links
            WHERE route_id IN ({placeholders})
            ORDER BY created_at
            """,
            tuple(route_ids),
        ):
            route_links_by_route.setdefault(row["route_id"], []).append(dict(row))
    claims = []
    claim_ids: list[str] = []
    for row in route_rows:
        row["route_links"] = route_links_by_route.get(row["route_id"], [])
        row["claim_preview"] = _preview(row.get("claim_text"), 160)
        claim_ids.append(row["claim_id"])
        claims.append(row)

    source_viewpoints = [
        dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT source_viewpoint_id, source_id, artifact_id, thesis_id, target_case_id, summary, stance,
                   horizon_label, status, validation_case_ids_json, resolution_review_id, created_at
            FROM source_viewpoints
            WHERE thesis_id = ?
               {"OR target_case_id IN (" + ",".join("?" for _ in target_case_ids) + ")" if target_case_ids else ""}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (thesis_id, *target_case_ids, max(limit * 2, 20)),
        )
    ]
    source_viewpoint_ids = []
    for row in source_viewpoints:
        row["validation_case_ids"] = _json_loads(row.get("validation_case_ids_json"), [])
        source_viewpoint_ids.append(row["source_viewpoint_id"])

    if monitor_ids:
        monitor_events = [
            dict(row)
            for row in list_rows(
                conn,
                f"""
                SELECT me.monitor_event_id, me.monitor_id, me.observed_value, me.outcome, me.detail_json, me.created_at,
                       m.owner_object_type, m.owner_object_id, m.monitor_type, m.metric_name, m.status
                FROM monitor_events me
                JOIN monitors m ON m.monitor_id = me.monitor_id
                WHERE m.monitor_id IN ({",".join("?" for _ in monitor_ids)})
                ORDER BY me.created_at DESC
                LIMIT ?
                """,
                (*monitor_ids, max(limit * 3, 30)),
            )
        ]
    else:
        monitor_events = []
    for row in monitor_events:
        row["detail"] = _json_loads(row.get("detail_json"), {})

    decision_journal = build_decision_journal(conn, days=3650, limit=max(limit * 4, 40), thesis_id=thesis_id)
    active_decisions = [item for item in decision_journal["items"] if item["status"] == "active"]
    validation_items = validation_board["items"]
    validation_case_ids = [item["validation_case_id"] for item in validation_items]
    matched_patterns = build_pattern_library(conn, thesis_id=thesis_id, limit=max(limit, 12))

    timeline_object_ids = list(
        dict.fromkeys(
            [
                thesis_id,
                version_id,
                *created_artifact_ids,
                *route_ids,
                *target_case_ids,
                *timing_plan_ids,
                *monitor_ids,
                *review_ids,
                *validation_case_ids,
                *source_viewpoint_ids,
                *[item["decision_id"] for item in decision_journal["items"]],
            ]
        )
    )
    timeline = []
    for row in list_rows(
        conn,
        f"""
        SELECT event_id, object_type, object_id, event_type, payload_json, created_at
        FROM events
        WHERE object_id IN ({",".join("?" for _ in timeline_object_ids)})
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (*timeline_object_ids, max(limit * 6, 60)),
    ):
        item = dict(row)
        item["payload"] = _json_loads(item.get("payload_json"), {})
        timeline.append(item)

    return {
        "summary": {
            **summary_counts(conn),
            "found": True,
            "claim_items": len(claims),
            "validation_items": len(validation_items),
            "target_case_items": len(thesis_focus_target_cases),
            "active_decisions": len(active_decisions),
            "review_items": len(reviews),
            "source_viewpoint_items": len(source_viewpoints),
            "matched_pattern_items": len(matched_patterns["items"]),
            "monitor_event_items": len(monitor_events),
            "timeline_items": len(timeline),
        },
        "thesis_id": thesis_id,
        "thesis": {
            "thesis_id": thesis["thesis_id"],
            "title": thesis["title"],
            "status": thesis["status"],
            "horizon_months": thesis["horizon_months"],
            "owner": thesis.get("owner"),
            "version": version,
            "promotion_gate": thesis["promotion_gate"],
        },
        "themes": thesis["themes"],
        "provenance": {
            "artifact_count": len(provenance),
            "source_count": len({item["source"]["source_id"] for item in provenance}),
            "items": provenance,
        },
        "linked_sources": linked_sources,
        "claims": claims[: max(limit * 3, 30)],
        "validations": validation_items[: max(limit * 2, 20)],
        "target_cases": thesis_focus_target_cases,
        "decisions": {
            "active_entries": active_decisions,
            "history": decision_journal["items"][: max(limit * 2, 20)],
            "summary": decision_journal["summary"],
        },
        "reviews": reviews[: max(limit * 2, 20)],
        "matched_patterns": matched_patterns["items"],
        "source_viewpoints": source_viewpoints[: max(limit * 2, 20)],
        "monitor_events": monitor_events[: max(limit * 2, 20)],
        "timeline": timeline[: max(limit * 3, 30)],
    }


def build_review_remediation_queue(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    dashboard = build_decision_dashboard(conn, days=7, limit=1000)
    decision_items = dashboard["priority_actions"] + dashboard["observe_actions"]
    queue_items: list[dict[str, Any]] = []
    for item in decision_items:
        gap_type = item["effective_review_gap_type"]
        queue_kind = ""
        if gap_type in {"missing_initial_review", "refresh_required"}:
            queue_kind = "blocking"
        elif item["effective_review_source"] == "thesis_fallback":
            queue_kind = "backfill"
        if not queue_kind:
            continue
        remediation_action = _review_remediation_action(item)
        priority = _review_remediation_priority_label(queue_kind=queue_kind, action_state=item["action_state"])
        queue_items.append(
            {
                "target_case_id": item["target_case_id"],
                "target": item["target"],
                "thesis_id": item["thesis_id"],
                "thesis_title": item["thesis_title"],
                "action_state": item["action_state"],
                "validation_state": item["validation_state"],
                "review_freshness": item["review_freshness"],
                "review_age_days": item["review_age_days"],
                "effective_review_source": item["effective_review_source"],
                "effective_review_gap_type": gap_type,
                "alerted_monitor_count": item["alerted_monitor_count"],
                "queue_kind": queue_kind,
                "priority": priority,
                "blocks_ready": queue_kind == "blocking",
                "remediation_action": remediation_action,
                "reason": _review_remediation_reason(item),
                "review_recipe": _review_recipe(item),
            }
        )
    queue_items.sort(
        key=lambda item: (
            {"p0": 0, "p1": 1, "p2": 2}.get(item["priority"], 9),
            REVIEW_REMEDIATION_GAP_ORDER.get(item["effective_review_gap_type"], 99),
            DECISION_ACTION_ORDER.get(item["action_state"], 99),
            0 if item["alerted_monitor_count"] else 1,
            item["thesis_title"],
            item["target"]["ticker_or_symbol"] or "",
        )
    )
    return {
        "summary": {
            **summary_counts(conn),
            "queue_items": len(queue_items),
            "blocking_items": sum(1 for item in queue_items if item["queue_kind"] == "blocking"),
            "backfill_items": sum(1 for item in queue_items if item["queue_kind"] == "backfill"),
            "missing_initial_review": sum(1 for item in queue_items if item["effective_review_gap_type"] == "missing_initial_review"),
            "refresh_required": sum(1 for item in queue_items if item["effective_review_gap_type"] == "refresh_required"),
            "p0_items": sum(1 for item in queue_items if item["priority"] == "p0"),
            "p1_items": sum(1 for item in queue_items if item["priority"] == "p1"),
            "p2_items": sum(1 for item in queue_items if item["priority"] == "p2"),
        },
        "blocking_items": [item for item in queue_items if item["queue_kind"] == "blocking"][:limit],
        "backfill_items": [item for item in queue_items if item["queue_kind"] == "backfill"][:limit],
        "items": queue_items[:limit],
    }


def build_intake_inbox(conn: Any) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT cr.route_id, cr.route_type, cr.status, cr.reason,
                   c.claim_id, c.claim_type, c.confidence, c.claim_text,
                   a.artifact_id, a.title AS artifact_title, a.artifact_kind, a.captured_at,
                   s.source_id, s.name AS source_name, s.source_type, s.primaryness
            FROM claim_routes cr
            JOIN claims c ON c.claim_id = cr.claim_id
            JOIN artifacts a ON a.artifact_id = cr.artifact_id
            JOIN sources s ON s.source_id = a.source_id
            WHERE cr.status = 'pending'
            ORDER BY a.captured_at DESC, cr.created_at DESC
            """,
        )
    ]
    by_artifact: dict[str, dict[str, Any]] = {}
    route_type_counts: dict[str, int] = {}
    for row in rows:
        route_type_counts[row["route_type"]] = route_type_counts.get(row["route_type"], 0) + 1
        bucket = by_artifact.setdefault(
            row["artifact_id"],
            {
                "artifact_id": row["artifact_id"],
                "artifact_title": row["artifact_title"],
                "artifact_kind": row["artifact_kind"],
                "captured_at": row["captured_at"],
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "source_type": row["source_type"],
                "primaryness": row["primaryness"],
                "pending_route_count": 0,
                "route_type_counts": {},
                "sample_claims": [],
            },
        )
        bucket["pending_route_count"] += 1
        bucket["route_type_counts"][row["route_type"]] = bucket["route_type_counts"].get(row["route_type"], 0) + 1
        if len(bucket["sample_claims"]) < 3:
            bucket["sample_claims"].append(
                {
                    "claim_id": row["claim_id"],
                    "claim_type": row["claim_type"],
                    "route_type": row["route_type"],
                    "confidence": row["confidence"],
                    "claim_text": row["claim_text"],
                }
            )
    items = sorted(by_artifact.values(), key=lambda item: (item["captured_at"], item["pending_route_count"]), reverse=True)
    return {
        "summary": {
            **summary_counts(conn),
            "pending_routes": len(rows),
            "pending_artifacts": len(items),
            "by_route_type": route_type_counts,
        },
        "items": items[:20],
    }


def build_route_workbench(
    conn: Any,
    *,
    status: str = "pending",
    route_type: str = "",
    source_id: str = "",
    thesis_id: str = "",
    limit: int = 80,
) -> dict[str, Any]:
    thesis_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT t.thesis_id, t.title, t.current_version_id, tv.created_from_artifacts_json
            FROM theses t
            JOIN thesis_versions tv ON tv.thesis_version_id = t.current_version_id
            ORDER BY t.created_at
            """,
        )
    ]
    thesis_candidates_by_artifact: dict[str, list[dict[str, Any]]] = {}
    for thesis in thesis_rows:
        for artifact_id in _json_loads(thesis.get("created_from_artifacts_json"), []):
            thesis_candidates_by_artifact.setdefault(artifact_id, []).append(
                {
                    "thesis_id": thesis["thesis_id"],
                    "title": thesis["title"],
                    "current_version_id": thesis["current_version_id"],
                }
            )
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT cr.route_id, cr.claim_id, cr.artifact_id, cr.route_type, cr.status, cr.reason, cr.target_object_type, cr.target_object_id,
                   COUNT(DISTINCT crl.route_link_id) AS link_count,
                   c.claim_type, c.confidence, c.claim_text,
                   a.title AS artifact_title, a.artifact_kind, a.captured_at,
                   s.source_id, s.name AS source_name, s.source_type, s.primaryness
            FROM claim_routes cr
            JOIN claims c ON c.claim_id = cr.claim_id
            JOIN artifacts a ON a.artifact_id = cr.artifact_id
            JOIN sources s ON s.source_id = a.source_id
            LEFT JOIN claim_route_links crl ON crl.route_id = cr.route_id
            WHERE cr.status = ?
            GROUP BY cr.route_id, cr.claim_id, cr.artifact_id, cr.route_type, cr.status, cr.reason, cr.target_object_type, cr.target_object_id,
                     c.claim_type, c.confidence, c.claim_text, a.title, a.artifact_kind, a.captured_at,
                     s.source_id, s.name, s.source_type, s.primaryness
            ORDER BY a.captured_at DESC, cr.created_at DESC
            """,
            (status,),
        )
    ]
    items: list[dict[str, Any]] = []
    batch_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        if route_type and row["route_type"] != route_type:
            continue
        if source_id and row["source_id"] != source_id:
            continue
        candidate_theses = thesis_candidates_by_artifact.get(row["artifact_id"], [])
        if thesis_id and not any(item["thesis_id"] == thesis_id for item in candidate_theses):
            continue
        lane = "personal" if row["primaryness"] == "personal" else "second_hand" if row["primaryness"] == "second_hand" or row["source_type"] == "kol" else "official"
        suggested_action = {
            "corroboration_needed": "attach_corroboration",
            "thesis_input": "attach_to_thesis",
            "thesis_seed": "open_or_attach_thesis",
            "counter_search": "attach_counter_material",
            "monitor_candidate": "create_monitor",
        }.get(row["route_type"], "review")
        item = {
            **row,
            "lane": lane,
            "candidate_theses": candidate_theses,
            "suggested_action": suggested_action,
            "has_unique_thesis_candidate": len(candidate_theses) == 1,
        }
        items.append(item)
        if row["status"] != "pending" or len(candidate_theses) != 1:
            continue
        group_key = (row["route_type"], row["source_id"], candidate_theses[0]["thesis_id"])
        bucket = batch_groups.setdefault(
            group_key,
            {
                "route_type": row["route_type"],
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "thesis_id": candidate_theses[0]["thesis_id"],
                "thesis_title": candidate_theses[0]["title"],
                "count": 0,
                "sample_route_ids": [],
            },
        )
        bucket["count"] += 1
        if len(bucket["sample_route_ids"]) < 5:
            bucket["sample_route_ids"].append(row["route_id"])
    items.sort(
        key=lambda item: (
            ROUTE_TYPE_ORDER.get(item["route_type"], 99),
            0 if item["has_unique_thesis_candidate"] else 1,
            -(item["confidence"] or 0),
            item["artifact_title"] or "",
        )
    )
    by_type: dict[str, int] = {}
    for item in items:
        by_type[item["route_type"]] = by_type.get(item["route_type"], 0) + 1
    return {
        "summary": {
            **summary_counts(conn),
            "status": status,
            "route_items": len(items),
            "pending_route_count": sum(1 for item in items if item["status"] == "pending"),
            "accepted_route_count": sum(1 for item in items if item["status"] == "accepted"),
            "routes_without_links": sum(1 for item in items if item["link_count"] == 0),
            "pending_corroboration_count": sum(
                1 for item in items if item["status"] == "pending" and item["route_type"] == "corroboration_needed"
            ),
            "ready_for_batch_accept": sum(
                1
                for item in items
                if item["status"] == "pending" and item["has_unique_thesis_candidate"] and item["route_type"] in {"corroboration_needed", "thesis_input", "thesis_seed"}
            ),
            "by_route_type": by_type,
        },
        "batch_recipes": sorted(
            batch_groups.values(),
            key=lambda item: (-item["count"], ROUTE_TYPE_ORDER.get(item["route_type"], 99), item["thesis_title"]),
        )[:20],
        "items": items[:limit],
    }


def build_route_normalization_queue(conn: Any, limit: int = 80) -> dict[str, Any]:
    route_workbench = build_route_workbench(conn, limit=max(limit, 500))
    items: list[dict[str, Any]] = []
    batch_groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for item in route_workbench["items"]:
        if item["status"] != "pending":
            continue

        action = ""
        reason = ""
        thesis_id = item["candidate_theses"][0]["thesis_id"] if item["has_unique_thesis_candidate"] else ""
        if item["route_type"] == "thesis_input" and item["has_unique_thesis_candidate"]:
            action = "accept_first_hand_input"
            reason = "一手 thesis_input 已映射到唯一 thesis，应显式标记为已吸收。"
        elif item["route_type"] == "thesis_seed" and item["primaryness"] == "personal":
            action = "supersede_foundational_seed"
            reason = "基础语音备忘录 claim 已由当前 theses 承接，不应长期停留在 pending。"
        elif item["route_type"] == "corroboration_needed" and not _is_high_signal_verification_claim(item):
            action = "supersede_low_signal_corroboration"
            reason = "低信号二手残余项不再进入决策面，应从 backlog 中归档。"
        else:
            continue

        batch_key = (action, item["source_id"], thesis_id, item["artifact_id"])
        group = batch_groups.setdefault(
            batch_key,
            {
                "normalization_action": action,
                "source_id": item["source_id"],
                "source_name": item["source_name"],
                "thesis_id": thesis_id,
                "thesis_title": item["candidate_theses"][0]["title"] if item["has_unique_thesis_candidate"] else "",
                "artifact_id": item["artifact_id"],
                "artifact_title": item["artifact_title"],
                "count": 0,
                "route_ids": [],
                "sample_claim_previews": [],
                "reason": reason,
            },
        )
        group["count"] += 1
        group["route_ids"].append(item["route_id"])
        if len(group["sample_claim_previews"]) < 3:
            group["sample_claim_previews"].append(_preview(item["claim_text"], 96))

        items.append(
            {
                "route_id": item["route_id"],
                "route_type": item["route_type"],
                "source_id": item["source_id"],
                "source_name": item["source_name"],
                "artifact_id": item["artifact_id"],
                "artifact_title": item["artifact_title"],
                "claim_id": item["claim_id"],
                "claim_type": item["claim_type"],
                "claim_preview": _preview(item["claim_text"], 120),
                "confidence": item["confidence"],
                "candidate_theses": item["candidate_theses"][:3],
                "normalization_action": action,
                "reason": reason,
            }
        )

    batches: list[dict[str, Any]] = []
    for group in batch_groups.values():
        if group["normalization_action"] == "accept_first_hand_input":
            group["recipe"] = {
                "command": "apply-route-batch",
                "args": {
                    "status": "accepted",
                    "link-object-type": "thesis",
                    "link-object-id": group["thesis_id"],
                    "route-id": group["route_ids"],
                },
                "hints": {
                    "note": "incorporated first-hand thesis input",
                },
            }
        else:
            note = (
                "superseded low-signal corroboration backlog"
                if group["normalization_action"] == "supersede_low_signal_corroboration"
                else "superseded foundational thesis seed already captured by active theses"
            )
            group["recipe"] = {
                "command": "set-route-status-batch",
                "args": {
                    "status": "superseded",
                    "route-id": group["route_ids"],
                    "note": note,
                },
                "hints": {
                    "note": note,
                },
            }
        batches.append(group)

    batches.sort(
        key=lambda item: (
            ROUTE_NORMALIZATION_ACTION_ORDER.get(item["normalization_action"], 99),
            -item["count"],
            item["source_name"],
        )
    )
    items.sort(
        key=lambda item: (
            ROUTE_NORMALIZATION_ACTION_ORDER.get(item["normalization_action"], 99),
            ROUTE_TYPE_ORDER.get(item["route_type"], 99),
            -(item["confidence"] or 0.0),
            item["source_name"],
        )
    )
    by_action: dict[str, int] = {}
    for item in items:
        by_action[item["normalization_action"]] = by_action.get(item["normalization_action"], 0) + 1
    return {
        "summary": {
            **summary_counts(conn),
            "queue_items": len(items),
            "batch_items": len(batches),
            "routes_to_accept": by_action.get("accept_first_hand_input", 0),
            "routes_to_supersede": by_action.get("supersede_foundational_seed", 0)
            + by_action.get("supersede_low_signal_corroboration", 0),
            "by_action": by_action,
        },
        "items": items[:limit],
        "batches": batches[:20],
    }


def _voice_memo_domain_signal(text: str) -> bool:
    keywords = {
        "ai",
        "算力",
        "具身智能",
        "机器人",
        "商业航天",
        "航天",
        "美股",
        "a股",
        "web3",
        "区块链",
        "链上",
        "投资",
        "标的",
        "赛道",
        "增长",
        "机会",
        "催化",
        "融资",
        "订单",
        "主题",
        "thesis",
    }
    lowered = text.lower()
    return any(_token_in_text(token, lowered) for token in keywords)


def _voice_memo_is_methodology_or_process(text: str) -> bool:
    lowered = text.lower()
    cn_markers = (
        "投资方法论",
        "完整链条",
        "知识框架",
        "系统分析",
        "获取信息",
        "消化视频",
        "梳理现成框架",
        "快速获取信息",
        "工作流",
        "投研助手",
        "建立新的个人投研框架",
        "目标不是做一个自动给出股票答案的系统",
        "不是做数学题",
    )
    if any(marker in text for marker in cn_markers):
        return True
    en_markers = ("workflow", "framework", "methodology", "system analysis")
    return any(marker in lowered for marker in en_markers)


def _voice_memo_is_broad_watchlist(text: str, candidate_count: int) -> bool:
    broad_markers = ("继续关注", "纳入长期跟踪", "都需要去关注", "都要去了解", "几个赛道", "几个方向")
    return candidate_count >= 2 and any(marker in text for marker in broad_markers)


def _voice_memo_seed_title(text: str) -> str:
    cleaned = re.sub(r"[，。；：,.!?！？]+", " ", text).strip()
    compact = " ".join(cleaned.split())
    if len(compact) <= 48:
        return compact
    return compact[:48].rstrip() + "…"


def _thesis_hint_terms(thesis: dict[str, Any]) -> list[str]:
    alias_map = {
        "ai": ["ai", "人工智能"],
        "infra": ["infra", "基础设施", "算力"],
        "inference": ["inference", "推理"],
        "embodied": ["embodied", "具身智能", "机器人"],
        "onchain": ["onchain", "链上", "web3", "区块链", "defi"],
        "finance": ["finance", "金融", "defi"],
        "china": ["china", "中国", "国产"],
        "compute": ["compute", "算力", "计算"],
    }
    tokens: list[str] = []
    for value in [thesis.get("thesis_id"), thesis.get("title"), thesis.get("statement"), *[item.get("name") for item in thesis.get("themes", [])]]:
        for token in _keyword_tokens(value):
            if token not in tokens:
                tokens.append(token)
    thesis_id = str(thesis.get("thesis_id") or "")
    for part in thesis_id.split("_"):
        for token in alias_map.get(part, []):
            if token not in tokens:
                tokens.append(token)
    return tokens


def _score_voice_memo_claim_against_thesis(claim_text: str, thesis: dict[str, Any]) -> dict[str, Any]:
    terms = _thesis_hint_terms(thesis)
    for value in [
        thesis.get("why_now"),
        thesis.get("base_case"),
        thesis.get("counter_case"),
        *[
            item.get("target", {}).get("ticker_or_symbol")
            for item in thesis.get("target_cases", [])
            if item.get("target")
        ],
    ]:
        for token in _keyword_tokens(value):
            if token not in terms:
                terms.append(token)
    matches = [token for token in terms if _token_in_text(token, claim_text)]
    score = 0
    if thesis.get("title") and _token_in_text(str(thesis["title"]), claim_text):
        score += 4
    score += min(len(matches), 6)
    reasons = []
    if matches:
        reasons.append("matched_terms=" + ", ".join(matches[:5]))
    for theme in thesis.get("themes", []):
        theme_name = theme.get("name") or ""
        if theme_name and _token_in_text(theme_name, claim_text):
            score += 2
            reasons.append(f"theme={theme_name}")
    return {
        "thesis_id": thesis["thesis_id"],
        "title": thesis["title"],
        "status": thesis["status"],
        "score": score,
        "reasons": reasons,
    }


def build_voice_memo_triage(conn: Any, artifact_id: str, limit: int = 80) -> dict[str, Any]:
    artifact = select_one(
        conn,
        """
        SELECT a.artifact_id, a.title, a.artifact_kind, a.captured_at, a.source_id,
               s.name AS source_name, s.source_type, s.primaryness
        FROM artifacts a
        JOIN sources s ON s.source_id = a.source_id
        WHERE a.artifact_id = ?
        """,
        (artifact_id,),
    )
    if artifact is None:
        return {"ok": False, "error": f"artifact not found: {artifact_id}"}
    artifact_row = dict(artifact)
    if artifact_row["primaryness"] != "personal":
        return {"ok": False, "error": f"artifact is not a personal voice memo lane: {artifact_id}"}

    thesis_board = build_thesis_board(conn)
    route_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT cr.route_id, cr.route_type, cr.status, cr.reason,
                   c.claim_id, c.claim_type, c.claim_text, c.confidence
            FROM claim_routes cr
            JOIN claims c ON c.claim_id = cr.claim_id
            WHERE cr.artifact_id = ? AND cr.route_type = 'thesis_seed' AND cr.status = 'pending'
            ORDER BY c.created_at, c.claim_id
            """,
            (artifact_id,),
        )
    ]
    items = []
    outcome_counts = {"matched_thesis": 0, "candidate_seed": 0, "low_signal": 0}
    matched_thesis_ids: set[str] = set()
    for row in route_rows:
        claim_text = row["claim_text"]
        candidates = [
            _score_voice_memo_claim_against_thesis(claim_text, thesis)
            for thesis in thesis_board["items"]
        ]
        candidates = [item for item in candidates if item["score"] > 0]
        candidates.sort(key=lambda item: (-item["score"], THESIS_STATUS_ORDER.get(item["status"], 99), item["title"]))
        top = candidates[0] if candidates else None
        second_score = candidates[1]["score"] if len(candidates) > 1 else -1
        if top and top["score"] >= 2 and top["score"] >= second_score + 1:
            outcome = "matched_thesis"
            recommended_action = "attach_to_existing_thesis"
            matched_thesis_ids.add(top["thesis_id"])
            recipe = {
                "command": "apply-route",
                "args": {
                    "route-id": row["route_id"],
                    "status": "accepted",
                    "link-object-type": "thesis",
                    "link-object-id": top["thesis_id"],
                    "link-kind": "feeds",
                    "note": "voice memo triage matched to existing thesis",
                },
            }
        elif _voice_memo_is_methodology_or_process(claim_text) or _voice_memo_is_broad_watchlist(claim_text, len(candidates)):
            outcome = "low_signal"
            recommended_action = "reject_route"
            recipe = {
                "command": "set-route-status",
                "args": {
                    "route-id": row["route_id"],
                    "status": "rejected",
                    "note": "voice memo triage methodology/process claim",
                },
            }
        elif _voice_memo_domain_signal(claim_text) or row["claim_type"] in {"forecast", "risk", "catalyst"}:
            outcome = "candidate_seed"
            recommended_action = "open_seed_thesis"
            recipe = {
                "command": "create-thesis",
                "args": {
                    "status": "seed",
                    "title": _voice_memo_seed_title(claim_text),
                    "statement": claim_text,
                    "mechanism-chain": "待补机制链",
                    "artifact-id": [artifact_id],
                    "owner": "human",
                },
                "hints": {
                    "route_id": row["route_id"],
                    "note": "create seed thesis from voice memo claim, then supersede the original route if adopted",
                },
            }
        else:
            outcome = "low_signal"
            recommended_action = "reject_route"
            recipe = {
                "command": "set-route-status",
                "args": {
                    "route-id": row["route_id"],
                    "status": "rejected",
                    "note": "voice memo triage low signal",
                },
            }
        outcome_counts[outcome] += 1
        items.append(
            {
                **row,
                "triage_outcome": outcome,
                "recommended_action": recommended_action,
                "thesis_candidates": candidates[:3],
                "recipe": recipe,
                "claim_preview": _preview(claim_text, 120),
            }
        )

    items.sort(
        key=lambda item: (
            VOICE_MEMO_TRIAGE_ORDER.get(item["triage_outcome"], 99),
            -(item["thesis_candidates"][0]["score"] if item["thesis_candidates"] else 0),
            -(item["confidence"] or 0.0),
            item["claim_id"],
        )
    )
    return {
        "ok": True,
        "artifact": artifact_row,
        "summary": {
            **summary_counts(conn),
            "route_items": len(route_rows),
            "matched_thesis_count": outcome_counts["matched_thesis"],
            "candidate_seed_count": outcome_counts["candidate_seed"],
            "low_signal_count": outcome_counts["low_signal"],
            "matched_thesis_ids": sorted(matched_thesis_ids),
        },
        "items": items[:limit],
    }


def _owner_label(owner_object_id: str, ctx: dict[str, Any]) -> dict[str, Any]:
    thesis = next((row for row in ctx["theses"] if row["thesis_id"] == owner_object_id), None)
    if thesis:
        return {"owner_object_type": "thesis", "title": thesis["title"], "status": thesis["status"]}
    for target_cases in ctx["target_cases_by_version"].values():
        for target_case in target_cases:
            if target_case["target_case_id"] != owner_object_id:
                continue
            target = ctx["target_by_id"].get(target_case["target_id"], {})
            return {
                "owner_object_type": "target_case",
                "title": target.get("ticker_or_symbol") or target_case["target_case_id"],
                "status": target_case["status"],
            }
    theme = ctx["theme_by_id"].get(owner_object_id)
    if theme:
        return {"owner_object_type": "theme", "title": theme["name"], "status": theme["importance_status"]}
    return {"owner_object_type": "other", "title": owner_object_id, "status": ""}


def build_review_board(conn: Any) -> dict[str, Any]:
    ctx = _load_context(conn)
    today = _today_iso()
    source_name_by_id = {
        row["source_id"]: row["name"]
        for row in list_rows(conn, "SELECT source_id, name FROM sources")
    }
    reviews = [
        dict(row)
        for row in list_rows(
            conn,
            "SELECT * FROM reviews ORDER BY review_date DESC, created_at DESC",
        )
    ]
    by_result: dict[str, int] = {}
    items: list[dict[str, Any]] = []
    due_items: list[dict[str, Any]] = []
    for row in reviews:
        by_result[row["result"]] = by_result.get(row["result"], 0) + 1
        owner_type = row.get("owner_object_type") or ""
        owner = _owner_label(row["owner_object_id"], ctx) if not owner_type else None
        if owner_type == "thesis":
            thesis = next((item for item in ctx["theses"] if item["thesis_id"] == row["owner_object_id"]), None)
            owner = {"owner_object_type": "thesis", "title": thesis["title"] if thesis else row["owner_object_id"], "status": thesis["status"] if thesis else ""}
        elif owner_type == "target_case":
            owner = _owner_label(row["owner_object_id"], ctx)
        elif owner_type == "theme":
            theme = ctx["theme_by_id"].get(row["owner_object_id"])
            owner = {"owner_object_type": "theme", "title": theme["name"] if theme else row["owner_object_id"], "status": theme["importance_status"] if theme else ""}
        elif owner_type:
            owner = {"owner_object_type": owner_type, "title": row["owner_object_id"], "status": ""}
        source_ids = _json_loads(row.get("source_ids_json"), [])
        claim_ids = _json_loads(row.get("claim_ids_json"), [])
        item = {
            "review_id": row["review_id"],
            "owner_object_type": owner["owner_object_type"],
            "owner_object_id": row["owner_object_id"],
            "owner": owner,
            "review_date": row["review_date"],
            "result": row["result"],
            "what_we_believed": row["what_we_believed"],
            "what_happened": row["what_happened"],
            "source_attribution": row["source_attribution"],
            "source_ids": source_ids,
            "source_names": [source_name_by_id.get(source_id, source_id) for source_id in source_ids],
            "claim_ids": claim_ids,
            "lessons": row["lessons"],
        }
        items.append(item)
        if row["review_date"] <= today:
            due_items.append(item)
    return {
        "summary": {
            **summary_counts(conn),
            "review_items": len(items),
            "due_reviews": len(due_items),
            "by_result": by_result,
        },
        "due_reviews": due_items[:20],
        "recent_reviews": items[:20],
    }


def build_playbook_board(conn: Any) -> dict[str, Any]:
    review_board = build_review_board(conn)
    thesis_board = build_thesis_board(conn)
    lesson_counts: dict[str, dict[str, Any]] = {}
    source_counts: dict[str, dict[str, Any]] = {}
    promotion_gap_counts: dict[str, int] = {}
    for item in review_board["recent_reviews"]:
        lesson = (item.get("lessons") or "").strip()
        if lesson:
            bucket = lesson_counts.setdefault(
                lesson,
                {
                    "lesson": lesson,
                    "count": 0,
                    "latest_review_date": item["review_date"],
                    "results": {},
                    "examples": [],
                },
            )
            bucket["count"] += 1
            bucket["results"][item["result"]] = bucket["results"].get(item["result"], 0) + 1
            if item["review_date"] > bucket["latest_review_date"]:
                bucket["latest_review_date"] = item["review_date"]
            if len(bucket["examples"]) < 3:
                bucket["examples"].append(
                    {
                        "review_id": item["review_id"],
                        "owner_object_id": item["owner_object_id"],
                        "result": item["result"],
                    }
                )
        source_attribution = (item.get("source_attribution") or "").strip()
        structured_sources = item.get("source_names") or []
        source_key = ", ".join(structured_sources) if structured_sources else source_attribution
        if source_key:
            bucket = source_counts.setdefault(
                source_key,
                {
                    "source_attribution": source_key,
                    "count": 0,
                    "results": {},
                },
            )
            bucket["count"] += 1
            bucket["results"][item["result"]] = bucket["results"].get(item["result"], 0) + 1
    for thesis in thesis_board["items"]:
        for missing in thesis["promotion_gate"]["missing"]:
            promotion_gap_counts[missing] = promotion_gap_counts.get(missing, 0) + 1
    return {
        "summary": {
            **review_board["summary"],
            "lesson_patterns": len(lesson_counts),
            "source_patterns": len(source_counts),
            "promotion_gap_patterns": promotion_gap_counts,
        },
        "top_lessons": sorted(lesson_counts.values(), key=lambda item: (-item["count"], item["lesson"]))[:12],
        "source_patterns": sorted(source_counts.values(), key=lambda item: (-item["count"], item["source_attribution"]))[:12],
        "promotion_gaps": sorted(
            [{"gap": gap, "count": count} for gap, count in promotion_gap_counts.items()],
            key=lambda item: (-item["count"], item["gap"]),
        ),
    }


def _pattern_match_score(pattern: dict[str, Any], thesis: dict[str, Any]) -> dict[str, Any]:
    trigger_terms = pattern.get("trigger_terms") or []
    thesis_terms = _thesis_hint_terms(thesis)
    thesis_text = " ".join(
        [
            thesis.get("title") or "",
            thesis.get("statement") or "",
            " ".join(item.get("name") or "" for item in thesis.get("themes", [])),
        ]
    )
    matches = [term for term in trigger_terms if _token_in_text(term, thesis_text) or term in thesis_terms]
    source_thesis_ids = pattern.get("source_thesis_ids") or []
    score = len(matches)
    if thesis["thesis_id"] in source_thesis_ids:
        score += 2
    return {
        "score": score,
        "matches": matches[:6],
    }


def build_pattern_library(conn: Any, thesis_id: str = "", limit: int = 20) -> dict[str, Any]:
    thesis = None
    if thesis_id:
        thesis = next((item for item in build_thesis_board(conn)["items"] if item["thesis_id"] == thesis_id), None)
        if thesis is None:
            return {"summary": {**summary_counts(conn), "found": False}, "items": []}
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT pattern_id, pattern_kind, label, description, trigger_terms_json,
                   source_review_ids_json, source_thesis_ids_json, status, created_at
            FROM patterns
            WHERE status = 'active'
            ORDER BY created_at DESC
            """,
        )
    ]
    items = []
    for row in rows:
        row["trigger_terms"] = _json_loads(row.get("trigger_terms_json"), [])
        row["source_review_ids"] = _json_loads(row.get("source_review_ids_json"), [])
        row["source_thesis_ids"] = _json_loads(row.get("source_thesis_ids_json"), [])
        if thesis is not None:
            match = _pattern_match_score(row, thesis)
            if match["score"] <= 0:
                continue
            row["match_score"] = match["score"]
            row["match_terms"] = match["matches"]
        items.append(row)
    if thesis is not None:
        items.sort(key=lambda item: (-item["match_score"], item["label"]))
    else:
        items.sort(key=lambda item: (item["pattern_kind"], item["label"]))
    return {
        "summary": {
            **summary_counts(conn),
            "found": thesis is not None if thesis_id else True,
            "pattern_items": len(items),
            "thesis_id": thesis_id,
        },
        "items": items[:limit],
    }


def build_source_board(conn: Any) -> dict[str, Any]:
    feedback_rollup = _source_feedback_rollup(conn)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT s.source_id, s.name, s.source_type, s.primaryness, s.jurisdiction, s.language,
                   COUNT(DISTINCT a.artifact_id) AS artifact_count,
                   COUNT(DISTINCT c.claim_id) AS claim_count,
                   COUNT(DISTINCT CASE WHEN cr.status = 'pending' THEN cr.route_id END) AS pending_route_count,
                   COUNT(DISTINCT CASE WHEN cr.status = 'accepted' THEN cr.route_id END) AS accepted_route_count,
                   COUNT(DISTINCT CASE WHEN cr.route_type = 'corroboration_needed' THEN cr.route_id END) AS corroboration_route_count,
                   COUNT(DISTINCT CASE WHEN cr.route_type = 'thesis_seed' THEN cr.route_id END) AS thesis_seed_route_count,
                   COUNT(DISTINCT CASE WHEN cr.route_type = 'thesis_input' THEN cr.route_id END) AS thesis_input_route_count,
                   COUNT(DISTINCT CASE WHEN cr.status = 'accepted' AND cr.route_type = 'corroboration_needed' AND crl.route_link_id IS NOT NULL THEN cr.route_id END) AS corroborated_accept_count,
                   COUNT(DISTINCT CASE WHEN vc.verdict = 'validated' THEN vc.validation_case_id END) AS validated_case_count,
                   COUNT(DISTINCT sv.source_viewpoint_id) AS source_viewpoint_count,
                   COUNT(DISTINCT CASE WHEN sv.status = 'open' THEN sv.source_viewpoint_id END) AS open_viewpoint_count,
                   COUNT(DISTINCT CASE WHEN sv.status = 'partially_validated' THEN sv.source_viewpoint_id END) AS partially_validated_viewpoint_count,
                   COUNT(DISTINCT CASE WHEN sv.status = 'validated' THEN sv.source_viewpoint_id END) AS validated_viewpoint_count,
                   COUNT(DISTINCT CASE WHEN sv.status = 'contradicted' THEN sv.source_viewpoint_id END) AS contradicted_viewpoint_count,
                   (
                     SELECT sv2.summary
                     FROM source_viewpoints sv2
                     WHERE sv2.source_id = s.source_id
                     ORDER BY sv2.created_at DESC
                     LIMIT 1
                   ) AS latest_viewpoint_summary,
                   (
                     SELECT sv2.status
                     FROM source_viewpoints sv2
                     WHERE sv2.source_id = s.source_id
                     ORDER BY sv2.created_at DESC
                     LIMIT 1
                   ) AS latest_viewpoint_status,
                   (
                     SELECT sv2.stance
                     FROM source_viewpoints sv2
                     WHERE sv2.source_id = s.source_id
                     ORDER BY sv2.created_at DESC
                     LIMIT 1
                   ) AS latest_viewpoint_stance
            FROM sources s
            LEFT JOIN artifacts a ON a.source_id = s.source_id
            LEFT JOIN claims c ON c.artifact_id = a.artifact_id
            LEFT JOIN claim_routes cr ON cr.claim_id = c.claim_id
            LEFT JOIN claim_route_links crl ON crl.route_id = cr.route_id
            LEFT JOIN validation_cases vc ON vc.claim_id = c.claim_id
            LEFT JOIN source_viewpoints sv ON sv.source_id = s.source_id
            GROUP BY s.source_id, s.name, s.source_type, s.primaryness, s.jurisdiction, s.language
            ORDER BY accepted_route_count DESC, pending_route_count DESC, artifact_count DESC, s.name
            """,
        )
    ]
    items: list[dict[str, Any]] = []
    for row in rows:
        feedback = feedback_rollup.get(row["source_id"], {})
        track_record_score, track_record_label = _track_record_score(
            validated_viewpoint_count=row["validated_viewpoint_count"],
            partially_validated_viewpoint_count=row["partially_validated_viewpoint_count"],
            contradicted_viewpoint_count=row["contradicted_viewpoint_count"],
            validated_case_count=row["validated_case_count"],
        )
        operator_feedback_score = int(feedback.get("operator_feedback_score") or 0)
        effective_operator_feedback_score = float(feedback.get("effective_operator_feedback_score") or 0.0)
        source_priority_score = round(track_record_score + effective_operator_feedback_score, 2)
        items.append(
            {
                **row,
                "source_trust_tier": _source_trust_tier(row["source_type"], row["primaryness"]),
                "track_record_score": track_record_score,
                "track_record_label": track_record_label,
                "feedback_count": int(feedback.get("feedback_count") or 0),
                "operator_feedback_score": operator_feedback_score,
                "effective_operator_feedback_score": effective_operator_feedback_score,
                "positive_feedback_count": int(feedback.get("positive_feedback_count") or 0),
                "negative_feedback_count": int(feedback.get("negative_feedback_count") or 0),
                "fresh_feedback_count": int(feedback.get("fresh_feedback_count") or 0),
                "aging_feedback_count": int(feedback.get("aging_feedback_count") or 0),
                "stale_feedback_count": int(feedback.get("stale_feedback_count") or 0),
                "latest_feedback_type": feedback.get("latest_feedback_type") or "",
                "latest_feedback_note": feedback.get("latest_feedback_note") or "",
                "latest_feedback_created_at": feedback.get("latest_feedback_created_at") or "",
                "latest_feedback_age_days": feedback.get("latest_feedback_age_days"),
                "feedback_freshness": feedback.get("feedback_freshness") or "missing",
                "source_priority_score": source_priority_score,
                "source_priority_label": _source_priority_label(source_priority_score),
                "source_display_label": _source_display_label(
                    primaryness=row["primaryness"],
                    source_type=row["source_type"],
                    source_priority_label=_source_priority_label(source_priority_score),
                ),
            }
        )
    return {
        "summary": {
            **summary_counts(conn),
            "source_items": len(rows),
            "sources_with_accepted_routes": sum(1 for row in rows if row["accepted_route_count"] > 0),
            "sources_with_corroborated_accepts": sum(1 for row in rows if row["corroborated_accept_count"] > 0),
            "sources_with_validated_cases": sum(1 for row in rows if row["validated_case_count"] > 0),
            "sources_with_viewpoints": sum(1 for row in rows if row["source_viewpoint_count"] > 0),
            "sources_with_feedback": sum(1 for row in items if row["feedback_count"] > 0),
            "high_priority_sources": sum(1 for row in items if row["source_priority_label"] == "high_priority"),
            "fresh_feedback_sources": sum(1 for row in items if row["feedback_freshness"] == "fresh"),
            "aging_feedback_sources": sum(1 for row in items if row["feedback_freshness"] == "aging"),
            "stale_feedback_sources": sum(1 for row in items if row["feedback_freshness"] == "stale"),
            "fresh_feedback_entries": sum(row["fresh_feedback_count"] for row in items),
            "aging_feedback_entries": sum(row["aging_feedback_count"] for row in items),
            "stale_feedback_entries": sum(row["stale_feedback_count"] for row in items),
        },
        "items": items,
    }


def build_source_track_record(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    board = build_source_board(conn)
    items = sorted(
        board["items"],
        key=lambda item: (
            -(item["source_priority_score"]),
            -(item["effective_operator_feedback_score"]),
            -(item["operator_feedback_score"]),
            -(item["track_record_score"]),
            -(item["validated_viewpoint_count"]),
            -(item["partially_validated_viewpoint_count"]),
            -(item["validated_case_count"]),
            item["name"],
        ),
    )
    return {
        "summary": {
            **board["summary"],
            "track_record_items": len(items),
            "strong_track_record_items": sum(1 for item in items if item["track_record_label"] == "strong"),
            "weak_track_record_items": sum(1 for item in items if item["track_record_label"] == "weak"),
            "feedback_backed_items": sum(1 for item in items if item["feedback_count"] > 0),
            "high_priority_track_items": sum(1 for item in items if item["source_priority_label"] == "high_priority"),
            "fresh_feedback_items": sum(1 for item in items if item["feedback_freshness"] == "fresh"),
            "aging_feedback_items": sum(1 for item in items if item["feedback_freshness"] == "aging"),
            "stale_feedback_items": sum(1 for item in items if item["feedback_freshness"] == "stale"),
            "fresh_feedback_entries": sum(item["fresh_feedback_count"] for item in items),
            "aging_feedback_entries": sum(item["aging_feedback_count"] for item in items),
            "stale_feedback_entries": sum(item["stale_feedback_count"] for item in items),
        },
        "items": items[:limit],
    }


def build_source_feedback_workbench(conn: Any, *, source_id: str = "", limit: int = 20) -> dict[str, Any]:
    params: list[Any] = []
    where = [
        "sv.status IN ('partially_validated', 'validated', 'contradicted')",
        "COALESCE(fb.feedback_count, 0) = 0",
    ]
    if source_id:
        where.append("sv.source_id = ?")
        params.append(source_id)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT sv.source_viewpoint_id, sv.source_id, sv.artifact_id, sv.thesis_id, sv.target_case_id,
                   sv.summary, sv.stance, sv.status, sv.horizon_label, sv.validation_case_ids_json,
                   s.name AS source_name, s.source_type, s.primaryness,
                   a.title AS artifact_title,
                   COALESCE(fb.feedback_count, 0) AS feedback_count
            FROM source_viewpoints sv
            JOIN sources s ON s.source_id = sv.source_id
            JOIN artifacts a ON a.artifact_id = sv.artifact_id
            LEFT JOIN (
                SELECT source_viewpoint_id, COUNT(*) AS feedback_count
                FROM source_feedback_entries
                WHERE source_viewpoint_id IS NOT NULL
                GROUP BY source_viewpoint_id
            ) fb ON fb.source_viewpoint_id = sv.source_viewpoint_id
            WHERE {' AND '.join(where)}
            ORDER BY sv.created_at DESC
            """,
            tuple(params),
        )
    ]
    items: list[dict[str, Any]] = []
    for row in rows:
        suggested_feedback_type = "useful_context"
        if row["status"] == "validated":
            suggested_feedback_type = "high_signal"
        elif row["status"] == "contradicted":
            suggested_feedback_type = "misleading"
        items.append(
            {
                **row,
                "validation_case_ids": _json_loads(row.get("validation_case_ids_json"), []),
                "suggested_feedback_type": suggested_feedback_type,
                "suggested_note": (
                    "来源观点已被验证，建议标记为高价值信号。"
                    if suggested_feedback_type == "high_signal"
                    else "来源观点已被事实反驳，建议记为误导性信号。"
                    if suggested_feedback_type == "misleading"
                    else "来源提供了有用线索，但仍需继续跟踪。"
                ),
            }
        )
    items.sort(
        key=lambda item: (
            VIEWPOINT_STATUS_ORDER.get(item["status"], 99),
            item["source_name"],
            item["artifact_title"],
        )
    )
    return {
        "summary": {
            **summary_counts(conn),
            "candidate_items": len(items),
            "validated_candidates": sum(1 for item in items if item["status"] == "validated"),
            "partially_validated_candidates": sum(1 for item in items if item["status"] == "partially_validated"),
            "contradicted_candidates": sum(1 for item in items if item["status"] == "contradicted"),
        },
        "items": items[:limit],
    }


def build_source_revisit_workbench(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    track = build_source_track_record(conn, limit=200)
    items: list[dict[str, Any]] = []
    for item in track["items"]:
        if item["feedback_freshness"] not in {"aging", "stale"}:
            continue
        priority = "p1" if item["source_priority_label"] == "high_priority" else "p2"
        suggested_feedback_type = "high_signal" if item["source_priority_label"] == "high_priority" else "useful_context"
        suggested_note = (
            "该来源依旧值得高优先跟踪，补一条新的高价值反馈。"
            if suggested_feedback_type == "high_signal"
            else "重新确认该来源当前仍提供有用上下文，避免老反馈继续主导排序。"
        )
        reason = (
            f"最近 feedback 已 {item['feedback_freshness']}，"
            f"距离上次来源判断 {item['latest_feedback_age_days']}d，需要重新确认这条来源当前是否仍值得优先跟踪。"
        )
        items.append(
            {
                "source_id": item["source_id"],
                "name": item["name"],
                "source_type": item["source_type"],
                "primaryness": item["primaryness"],
                "source_priority_label": item["source_priority_label"],
                "source_priority_score": item["source_priority_score"],
                "feedback_freshness": item["feedback_freshness"],
                "latest_feedback_type": item["latest_feedback_type"],
                "latest_feedback_age_days": item["latest_feedback_age_days"],
                "effective_operator_feedback_score": item["effective_operator_feedback_score"],
                "suggested_action": "refresh_source_feedback",
                "suggested_feedback_type": suggested_feedback_type,
                "suggested_note": suggested_note,
                "feedback_recipe": {
                    "command": "record-source-feedback",
                    "args": {
                        "source-id": item["source_id"],
                        "feedback-type": suggested_feedback_type,
                    },
                    "hints": {
                        "note": suggested_note,
                    },
                },
                "priority": priority,
                "reason": reason,
            }
        )
    items.sort(
        key=lambda item: (
            {"p1": 0, "p2": 1}.get(item["priority"], 9),
            0 if item["feedback_freshness"] == "stale" else 1,
            -(item["latest_feedback_age_days"] or 0),
            item["name"],
        )
    )
    return {
        "summary": {
            **summary_counts(conn),
            "queue_items": len(items),
            "aging_items": sum(1 for item in items if item["feedback_freshness"] == "aging"),
            "stale_items": sum(1 for item in items if item["feedback_freshness"] == "stale"),
            "p1_items": sum(1 for item in items if item["priority"] == "p1"),
            "p2_items": sum(1 for item in items if item["priority"] == "p2"),
        },
        "items": items[:limit],
    }


def build_source_viewpoint_workbench(
    conn: Any,
    *,
    source_id: str = "",
    include_existing: bool = False,
    limit: int = 20,
) -> dict[str, Any]:
    thesis_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT t.thesis_id, t.title, t.horizon_months, t.current_version_id
            FROM theses t
            ORDER BY t.created_at
            """,
        )
    ]
    thesis_meta = {row["thesis_id"]: row for row in thesis_rows}
    target_case_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT t.thesis_id, tc.target_case_id
            FROM theses t
            JOIN target_cases tc ON tc.thesis_version_id = t.current_version_id
            ORDER BY tc.created_at
            """,
        )
    ]
    target_case_by_thesis: dict[str, list[str]] = {}
    for row in target_case_rows:
        target_case_by_thesis.setdefault(row["thesis_id"], []).append(row["target_case_id"])

    existing_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT source_id, artifact_id, COALESCE(thesis_id, '') AS thesis_key,
                   COUNT(*) AS existing_viewpoint_count,
                   GROUP_CONCAT(source_viewpoint_id) AS source_viewpoint_ids
            FROM source_viewpoints
            GROUP BY source_id, artifact_id, COALESCE(thesis_id, '')
            """,
        )
    ]
    existing_by_key = {
        (row["source_id"], row["artifact_id"], row["thesis_key"]): {
            "existing_viewpoint_count": row["existing_viewpoint_count"],
            "source_viewpoint_ids": [item for item in (row["source_viewpoint_ids"] or "").split(",") if item],
        }
        for row in existing_rows
    }
    pending_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT a.source_id, cr.artifact_id, COUNT(DISTINCT cr.route_id) AS pending_corroboration_count
            FROM claim_routes cr
            JOIN artifacts a ON a.artifact_id = cr.artifact_id
            JOIN sources s ON s.source_id = a.source_id
            WHERE cr.route_type = 'corroboration_needed'
              AND cr.status = 'pending'
              AND (s.primaryness IN ('second_hand', 'personal') OR s.source_type = 'kol')
            GROUP BY a.source_id, cr.artifact_id
            """,
        )
    ]
    pending_by_key = {
        (row["source_id"], row["artifact_id"]): int(row["pending_corroboration_count"])
        for row in pending_rows
    }
    validation_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT vc.validation_case_id, vc.source_id, vc.thesis_id, vc.verdict, vc.created_at,
                   c.artifact_id, c.claim_text,
                   a.title AS artifact_title,
                   s.name AS source_name, s.source_type, s.primaryness,
                   t.title AS thesis_title
            FROM validation_cases vc
            JOIN claims c ON c.claim_id = vc.claim_id
            JOIN artifacts a ON a.artifact_id = c.artifact_id
            JOIN sources s ON s.source_id = vc.source_id
            LEFT JOIN theses t ON t.thesis_id = vc.thesis_id
            WHERE (s.primaryness IN ('second_hand', 'personal') OR s.source_type = 'kol')
            ORDER BY vc.created_at DESC
            """,
        )
    ]
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in validation_rows:
        if source_id and row["source_id"] != source_id:
            continue
        thesis_key = row["thesis_id"] or ""
        group_key = (row["source_id"], row["artifact_id"], thesis_key)
        bucket = grouped.setdefault(
            group_key,
            {
                "source_id": row["source_id"],
                "source_name": row["source_name"],
                "source_type": row["source_type"],
                "primaryness": row["primaryness"],
                "artifact_id": row["artifact_id"],
                "artifact_title": row["artifact_title"],
                "thesis_id": row["thesis_id"] or "",
                "thesis_title": row["thesis_title"] or "",
                "validation_case_ids": [],
                "validated_texts": [],
                "contradicted_texts": [],
                "partial_texts": [],
                "latest_validation_at": row["created_at"],
            },
        )
        bucket["validation_case_ids"].append(row["validation_case_id"])
        if row["created_at"] > bucket["latest_validation_at"]:
            bucket["latest_validation_at"] = row["created_at"]
        preview = _preview(row["claim_text"], 72)
        if row["verdict"] == "validated":
            bucket["validated_texts"].append(preview)
        elif row["verdict"] == "contradicted":
            bucket["contradicted_texts"].append(preview)
        else:
            bucket["partial_texts"].append(preview)
    items: list[dict[str, Any]] = []
    for key, bucket in grouped.items():
        existing_meta = existing_by_key.get(key, {"existing_viewpoint_count": 0, "source_viewpoint_ids": []})
        if not include_existing and existing_meta["existing_viewpoint_count"] > 0:
            continue
        pending_count = pending_by_key.get((bucket["source_id"], bucket["artifact_id"]), 0)
        validated_count = len(bucket["validated_texts"])
        contradicted_count = len(bucket["contradicted_texts"])
        suggested_status = _suggest_viewpoint_status(validated_count, contradicted_count, pending_count)
        suggested_stance = _suggest_viewpoint_stance(
            bucket["validated_texts"],
            bucket["partial_texts"] + ([bucket["validated_texts"][0]] if pending_count > 0 and bucket["validated_texts"] else []),
            bucket["contradicted_texts"],
        )
        thesis_info = thesis_meta.get(bucket["thesis_id"], {})
        target_case_ids = target_case_by_thesis.get(bucket["thesis_id"], [])
        suggested_target_case_id = target_case_ids[0] if len(target_case_ids) == 1 else ""
        validated_preview = bucket["validated_texts"][0] if bucket["validated_texts"] else ""
        contradicted_preview = bucket["contradicted_texts"][0] if bucket["contradicted_texts"] else ""
        pending_preview = bucket["partial_texts"][0] if bucket["partial_texts"] else ""
        if not pending_preview and pending_count > 0:
            pending_preview = _pick_pending_viewpoint_preview(conn, bucket["artifact_id"], bucket["thesis_title"])
        items.append(
            {
                **bucket,
                "validated_case_count": validated_count,
                "contradicted_case_count": contradicted_count,
                "partial_case_count": len(bucket["partial_texts"]),
                "pending_corroboration_count": pending_count,
                "existing_viewpoint_count": existing_meta["existing_viewpoint_count"],
                "source_viewpoint_ids": existing_meta["source_viewpoint_ids"],
                "suggested_status": suggested_status,
                "suggested_stance": suggested_stance,
                "suggested_horizon_label": _suggest_horizon_label(thesis_info.get("horizon_months")),
                "suggested_target_case_id": suggested_target_case_id,
                "suggested_summary": _suggest_viewpoint_summary(
                    thesis_title=bucket["thesis_title"],
                    status=suggested_status,
                    stance=suggested_stance,
                    validated_preview=validated_preview,
                    pending_preview=pending_preview,
                    contradicted_preview=contradicted_preview,
                ),
                "validated_preview": validated_preview,
                "pending_preview": pending_preview,
                "contradicted_preview": contradicted_preview,
            }
        )
    items.sort(
        key=lambda item: (
            0 if item["existing_viewpoint_count"] == 0 else 1,
            VIEWPOINT_STATUS_ORDER.get(item["suggested_status"], 99),
            -item["validated_case_count"],
            -item["pending_corroboration_count"],
            item["latest_validation_at"],
            item["source_name"],
        ),
        reverse=False,
    )
    return {
        "summary": {
            **summary_counts(conn),
            "candidate_items": len(items),
            "missing_viewpoints": sum(1 for item in items if item["existing_viewpoint_count"] == 0),
            "include_existing": include_existing,
        },
        "items": items[:limit],
    }


def build_validation_board(conn: Any, *, verdict: str = "", thesis_id: str = "", source_id: str = "", limit: int = 50) -> dict[str, Any]:
    where = ["1 = 1"]
    params: list[Any] = []
    if verdict:
        where.append("vc.verdict = ?")
        params.append(verdict)
    if thesis_id:
        where.append("vc.thesis_id = ?")
        params.append(thesis_id)
    if source_id:
        where.append("vc.source_id = ?")
        params.append(source_id)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT vc.validation_case_id, vc.route_id, vc.claim_id, vc.thesis_id, vc.thesis_version_id, vc.source_id,
                   vc.verdict, vc.evidence_artifact_ids_json, vc.rationale, vc.validator, vc.validator_model, vc.expires_at, vc.created_at,
                   c.claim_type, c.claim_text,
                   t.title AS thesis_title,
                   s.name AS source_name,
                   a.title AS artifact_title
            FROM validation_cases vc
            JOIN claims c ON c.claim_id = vc.claim_id
            LEFT JOIN theses t ON t.thesis_id = vc.thesis_id
            LEFT JOIN sources s ON s.source_id = vc.source_id
            LEFT JOIN artifacts a ON a.artifact_id = c.artifact_id
            WHERE {' AND '.join(where)}
            ORDER BY vc.created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        )
    ]
    by_verdict: dict[str, int] = {}
    for row in rows:
        by_verdict[row["verdict"]] = by_verdict.get(row["verdict"], 0) + 1
        row["evidence_artifact_ids"] = _json_loads(row.get("evidence_artifact_ids_json"), [])
        row["claim_preview"] = _preview(row.get("claim_text"), 120)
    return {
        "summary": {
            **summary_counts(conn),
            "validation_items": len(rows),
            "by_verdict": by_verdict,
        },
        "items": rows,
    }


def build_decision_dashboard(conn: Any, *, days: int = 7, limit: int = 12) -> dict[str, Any]:
    thesis_board = build_thesis_board(conn)
    route_workbench = build_route_workbench(conn, status="pending", limit=max(limit * 3, 24))
    validation_board = build_validation_board(conn, verdict="validated", limit=max(limit * 2, 20))
    source_board = build_source_board(conn)
    target_case_ids = [
        target_case["target_case_id"]
        for thesis in thesis_board["items"]
        for target_case in thesis["target_cases"]
    ]
    latest_operator_decisions = _latest_operator_decisions(conn, target_case_ids)
    cutoff = _cutoff_iso(days)
    source_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT s.source_id, s.name, s.source_type, s.primaryness,
                   (
                     SELECT a2.title
                     FROM artifacts a2
                     WHERE a2.source_id = s.source_id
                     ORDER BY a2.captured_at DESC, a2.created_at DESC
                     LIMIT 1
                   ) AS latest_artifact_title,
                   (
                     SELECT a2.captured_at
                     FROM artifacts a2
                     WHERE a2.source_id = s.source_id
                     ORDER BY a2.captured_at DESC, a2.created_at DESC
                     LIMIT 1
                   ) AS latest_captured_at
            FROM sources s
            ORDER BY s.name
            """,
        )
    ]
    source_to_theses: dict[str, list[dict[str, str]]] = {}
    for thesis in thesis_board["items"]:
        linked: dict[str, dict[str, str]] = {}
        for row in _artifact_source_map(conn, thesis["version"].get("created_from_artifacts", [])).values():
            linked[row["source_id"]] = {
                "thesis_id": thesis["thesis_id"],
                "title": thesis["title"],
                "status": thesis["status"],
            }
        for source_id, payload in linked.items():
            source_to_theses.setdefault(source_id, []).append(payload)
    source_index = {row["source_id"]: row for row in source_board["items"]}
    source_focus: list[dict[str, Any]] = []
    for row in source_rows:
        counts = source_index.get(row["source_id"], {})
        latest_date = _parse_iso_date(row.get("latest_captured_at"))
        if not (
            latest_date >= cutoff
            or counts.get("pending_route_count", 0) > 0
            or counts.get("accepted_route_count", 0) > 0
            or row["primaryness"] == "personal"
        ):
            continue
        validation_state = _source_validation_state(
            primaryness=row["primaryness"],
            source_type=row["source_type"],
            pending_route_count=counts.get("pending_route_count", 0),
            validated_case_count=counts.get("validated_case_count", 0),
            corroborated_accept_count=counts.get("corroborated_accept_count", 0),
        )
        source_focus.append(
            {
                "source_id": row["source_id"],
                "name": row["name"],
                "lane": _source_lane(row["source_type"], row["primaryness"]),
                "source_type": row["source_type"],
                "primaryness": row["primaryness"],
                "source_trust_tier": counts.get("source_trust_tier") or _source_trust_tier(row["source_type"], row["primaryness"]),
                "latest_artifact_title": row.get("latest_artifact_title"),
                "latest_captured_at": row.get("latest_captured_at"),
                "validation_state": validation_state,
                "pending_route_count": counts.get("pending_route_count", 0),
                "accepted_route_count": counts.get("accepted_route_count", 0),
                "corroborated_accept_count": counts.get("corroborated_accept_count", 0),
                "validated_case_count": counts.get("validated_case_count", 0),
                "source_viewpoint_count": counts.get("source_viewpoint_count", 0),
                "latest_viewpoint_summary": counts.get("latest_viewpoint_summary"),
                "latest_viewpoint_status": counts.get("latest_viewpoint_status"),
                "latest_viewpoint_stance": counts.get("latest_viewpoint_stance"),
                "operator_feedback_score": counts.get("operator_feedback_score", 0),
                "effective_operator_feedback_score": counts.get("effective_operator_feedback_score", 0.0),
                "source_priority_score": counts.get("source_priority_score", 0),
                "source_priority_label": counts.get("source_priority_label", "unproven"),
                "source_display_label": counts.get("source_display_label")
                or _source_display_label(
                    primaryness=row["primaryness"],
                    source_type=row["source_type"],
                    source_priority_label=counts.get("source_priority_label", "unproven"),
                ),
                "latest_feedback_type": counts.get("latest_feedback_type", ""),
                "feedback_freshness": counts.get("feedback_freshness", "missing"),
                "linked_theses": source_to_theses.get(row["source_id"], [])[:4],
            }
        )
    source_focus.sort(
        key=lambda item: (
            0 if item["lane"] in {"personal", "second_hand"} else 1,
            -(item["source_priority_score"]),
            SOURCE_VALIDATION_ORDER.get(item["validation_state"], 99),
            -(item["validated_case_count"]),
            -(item["corroborated_accept_count"]),
            -(item["pending_route_count"]),
            item["latest_captured_at"] or "",
            item["name"],
        )
    )

    decision_items: list[dict[str, Any]] = []
    for thesis in thesis_board["items"]:
        validation_state = _thesis_validation_state(conn, thesis)
        source_support = _decision_source_support(_linked_sources_for_thesis(conn, thesis, source_index))
        for target_case in thesis["target_cases"]:
            timing = target_case.get("timing_plan") or {}
            effective_review = target_case.get("effective_review") or _build_effective_review(
                target_case_review=target_case.get("latest_review"),
                thesis_review=thesis.get("latest_review"),
            )
            review_freshness = effective_review["freshness"]
            review_age_days = effective_review["age_days"]
            action_state = _decision_action(
                timing.get("desired_posture"),
                target_case["status"],
                thesis["status"],
            )
            recorded_decision = latest_operator_decisions.get(target_case["target_case_id"])
            decision_record_state = "missing"
            if recorded_decision is not None:
                decision_record_state = "aligned" if recorded_decision["action_state"] == action_state else "drift"
            reason = _decision_reason(thesis, target_case, action_state)
            if effective_review["gap_type"] == "missing_initial_review":
                reason = f"{reason}；缺少可用 review".strip("；")
            elif effective_review["gap_type"] == "refresh_required" and review_age_days is not None:
                reason = f"{reason}；review 已旧({review_age_days}d)".strip("；")
            if source_support["source_confidence"] == "fragile":
                reason = f"{reason}；来源侧仍偏脆弱".strip("；")
            elif source_support["needs_source_revisit"]:
                reason = f"{reason}；来源反馈待复查".strip("；")
            decision_items.append(
                {
                    "target_case_id": target_case["target_case_id"],
                    "target": target_case["target"],
                    "thesis_id": thesis["thesis_id"],
                    "thesis_title": thesis["title"],
                    "thesis_status": thesis["status"],
                    "themes": thesis["themes"],
                    "action_state": action_state,
                    "raw_posture": timing.get("desired_posture"),
                    "validation_state": validation_state,
                    "target_case_status": target_case["status"],
                    "exposure_type": target_case["exposure_type"],
                    "capture_link_strength": target_case["capture_link_strength"],
                    "monitor_count": target_case["monitor_summary"]["count"],
                    "alerted_monitor_count": target_case["monitor_summary"]["alerted_count"],
                    "latest_review": effective_review["review"],
                    "effective_review": effective_review["review"],
                    "effective_review_source": effective_review["source"],
                    "effective_review_gap_type": effective_review["gap_type"],
                    "review_freshness": review_freshness,
                    "review_age_days": review_age_days,
                    "source_confidence": source_support["source_confidence"],
                    "source_confidence_reason": source_support["source_confidence_reason"],
                    "needs_source_revisit": source_support["needs_source_revisit"],
                    "latest_recorded_decision": recorded_decision,
                    "recorded_action_state": recorded_decision["action_state"] if recorded_decision else "",
                    "recorded_decision_date": recorded_decision["decision_date"] if recorded_decision else "",
                    "recorded_decision_confidence": recorded_decision["confidence"] if recorded_decision else None,
                    "recorded_decision_state": decision_record_state,
                    "linked_source_count": source_support["linked_source_count"],
                    "first_hand_source_count": source_support["first_hand_source_count"],
                    "second_hand_source_count": source_support["second_hand_source_count"],
                    "personal_source_count": source_support["personal_source_count"],
                    "linked_sources": source_support["linked_sources"],
                    "catalysts": timing.get("catalysts", []),
                    "confirmation_signals": timing.get("confirmation_signals", []),
                    "preconditions": timing.get("preconditions", []),
                    "invalidators": timing.get("invalidators", []),
                    "promotion_gate": thesis["promotion_gate"],
                    "reason": reason,
                }
            )
    decision_items.sort(
        key=lambda item: (
            DECISION_ACTION_ORDER.get(item["action_state"], 99),
            REVIEW_PRIORITY_ORDER.get(item["review_freshness"], 99),
            SOURCE_CONFIDENCE_ORDER.get(item["source_confidence"], 99),
            0 if item["alerted_monitor_count"] else 1,
            _sort_key_by_order(item["thesis_status"], THESIS_STATUS_ORDER),
            item["thesis_title"],
            item["target"]["ticker_or_symbol"] or "",
        )
    )
    verification_candidates = _collect_verification_candidates(conn, limit=max(limit * 10, 80))
    verification_queue = verification_candidates[:limit]
    verified_updates = [
        {
            "validation_case_id": item["validation_case_id"],
            "route_id": item["route_id"],
            "source_name": item["source_name"],
            "artifact_title": item["artifact_title"],
            "claim_preview": item["claim_preview"],
            "thesis_id": item["thesis_id"],
            "thesis_title": item["thesis_title"],
            "evidence_artifact_ids": item["evidence_artifact_ids"],
            "verdict": item["verdict"],
        }
        for item in validation_board["items"]
    ][:limit]
    priority_actions = [item for item in decision_items if item["action_state"] != "observe"][:limit]
    observe_actions = [item for item in decision_items if item["action_state"] == "observe"][:limit]
    review_attention = [
        item
        for item in decision_items
        if item["review_freshness"] in {"missing", "stale"}
    ][:limit]
    source_guard = [
        item
        for item in decision_items
        if item["source_confidence"] != "grounded" or item["needs_source_revisit"]
    ][:limit]
    return {
        "summary": {
            **summary_counts(conn),
            "days": days,
            "decision_items": len(decision_items),
            "priority_actions": len(priority_actions),
            "observe_actions": len(observe_actions),
            "verification_queue": len(verification_queue),
            "source_focus_items": len(source_focus),
            "validated_source_items": sum(
                1 for item in source_focus if item["validation_state"] in {"validated", "partially_validated", "first_hand_feed"}
            ),
            "validated_case_count": validation_board["summary"]["validation_items"],
            "fresh_review_items": sum(1 for item in decision_items if item["review_freshness"] == "fresh"),
            "aging_review_items": sum(1 for item in decision_items if item["review_freshness"] == "aging"),
            "stale_review_items": sum(1 for item in decision_items if item["review_freshness"] == "stale"),
            "missing_review_items": sum(1 for item in decision_items if item["review_freshness"] == "missing"),
            "review_attention_items": len(review_attention),
            "grounded_source_items": sum(1 for item in decision_items if item["source_confidence"] == "grounded"),
            "developing_source_items": sum(1 for item in decision_items if item["source_confidence"] == "developing"),
            "fragile_source_items": sum(1 for item in decision_items if item["source_confidence"] == "fragile"),
            "source_guard_items": len(source_guard),
            "recorded_decision_items": sum(1 for item in decision_items if item["recorded_decision_state"] != "missing"),
            "aligned_recorded_decisions": sum(1 for item in decision_items if item["recorded_decision_state"] == "aligned"),
            "drifted_recorded_decisions": sum(1 for item in decision_items if item["recorded_decision_state"] == "drift"),
            "missing_recorded_decisions": sum(1 for item in decision_items if item["recorded_decision_state"] == "missing"),
        },
        "source_focus": source_focus[:limit],
        "priority_actions": priority_actions,
        "observe_actions": observe_actions,
        "verification_queue": verification_queue,
        "verified_updates": verified_updates,
        "review_attention": review_attention,
        "source_guard": source_guard,
    }


def build_decision_journal(
    conn: Any,
    *,
    days: int = 30,
    limit: int = 20,
    thesis_id: str = "",
    target_case_id: str = "",
) -> dict[str, Any]:
    dashboard = build_decision_dashboard(conn, days=max(days, 7), limit=max(limit * 2, 20))
    dashboard_by_target_case = {
        item["target_case_id"]: item
        for item in dashboard["priority_actions"] + dashboard["observe_actions"]
    }
    where = ["od.decision_date >= ?"]
    params: list[Any] = [_cutoff_iso(days)]
    if thesis_id:
        where.append("od.thesis_id = ?")
        params.append(thesis_id)
    if target_case_id:
        where.append("od.target_case_id = ?")
        params.append(target_case_id)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT od.decision_id, od.target_case_id, od.thesis_id, od.decision_date, od.action_state,
                   od.confidence, od.rationale, od.source_ids_json, od.review_id, od.status,
                   od.supersedes_decision_id, od.created_at,
                   t.title AS thesis_title,
                   tg.ticker_or_symbol,
                   tg.asset_class,
                   tc.status AS target_case_status
            FROM operator_decisions od
            JOIN theses t ON t.thesis_id = od.thesis_id
            JOIN target_cases tc ON tc.target_case_id = od.target_case_id
            JOIN targets tg ON tg.target_id = tc.target_id
            WHERE {' AND '.join(where)}
            ORDER BY od.decision_date DESC, od.created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        )
    ]
    items: list[dict[str, Any]] = []
    for row in rows:
        current_item = dashboard_by_target_case.get(row["target_case_id"])
        source_ids = _json_loads(row.get("source_ids_json"), [])
        linked_sources = _source_rows_by_ids(conn, source_ids)
        current_action_state = current_item["action_state"] if current_item else ""
        alignment = "superseded" if row["status"] == "superseded" else "missing"
        if row["status"] == "active" and current_action_state:
            alignment = "aligned" if current_action_state == row["action_state"] else "drift"
        items.append(
            {
                **row,
                "source_ids": source_ids,
                "linked_sources": linked_sources,
                "source_surface": _source_support_surface(linked_sources),
                "rationale_preview": _preview(row.get("rationale"), 140),
                "current_action_state": current_action_state,
                "alignment": alignment,
                "review_freshness": current_item["review_freshness"] if current_item else "missing",
                "source_confidence": current_item["source_confidence"] if current_item else "fragile",
            }
        )
    return {
        "summary": {
            **summary_counts(conn),
            "days": days,
            "decision_entries": len(items),
            "active_entries": sum(1 for item in items if item["status"] == "active"),
            "superseded_entries": sum(1 for item in items if item["status"] == "superseded"),
            "aligned_entries": sum(1 for item in items if item["alignment"] == "aligned"),
            "drift_entries": sum(1 for item in items if item["alignment"] == "drift"),
            "missing_alignment_entries": sum(1 for item in items if item["alignment"] == "missing"),
        },
        "items": items,
    }


def build_decision_maintenance_queue(conn: Any, *, days: int = 30, limit: int = 20) -> dict[str, Any]:
    dashboard = build_decision_dashboard(conn, days=max(days, 7), limit=max(limit * 2, 20))
    items: list[dict[str, Any]] = []
    for item in dashboard["priority_actions"] + dashboard["observe_actions"]:
        if item["recorded_decision_state"] == "aligned":
            continue
        if item["recorded_decision_state"] == "missing":
            priority = "p1" if item["action_state"] != "observe" else "p2"
            reason = "当前 decision item 已形成稳定动作语义，但还没有 operator 真实决策记录。"
        else:
            priority = "p0" if item["action_state"] != "observe" else "p1"
            reason = (
                f"当前派生动作 `{item['action_state']}` 与已记录动作 "
                f"`{item['recorded_action_state'] or 'unknown'}` 不一致，建议刷新 decision。"
            )
        review_id = (item.get("effective_review") or {}).get("review_id") or ""
        source_ids = [source["source_id"] for source in item.get("linked_sources", [])[:2]]
        recipe_args = {
            "target-case-id": item["target_case_id"],
            "action-state": item["action_state"],
        }
        if review_id:
            recipe_args["review-id"] = review_id
        items.append(
            {
                "priority": priority,
                "target_case_id": item["target_case_id"],
                "target": item["target"],
                "thesis_id": item["thesis_id"],
                "thesis_title": item["thesis_title"],
                "action_state": item["action_state"],
                "recorded_decision_state": item["recorded_decision_state"],
                "recorded_action_state": item["recorded_action_state"],
                "recorded_decision_date": item["recorded_decision_date"],
                "review_freshness": item["review_freshness"],
                "source_confidence": item["source_confidence"],
                "reason": reason,
                "recipe": {
                    "command": "record-decision",
                    "args": recipe_args,
                    "hints": {
                        "source_ids": source_ids,
                        "rationale": item["reason"],
                    },
                },
            }
        )
    items.sort(
        key=lambda item: (
            0 if item["priority"] == "p0" else 1 if item["priority"] == "p1" else 2,
            DECISION_ACTION_ORDER.get(item["action_state"], 99),
            DECISION_ALIGNMENT_ORDER.get(item["recorded_decision_state"], 99),
            item["thesis_title"],
            item["target"]["ticker_or_symbol"] or "",
        )
    )
    return {
        "summary": {
            **summary_counts(conn),
            "days": days,
            "queue_items": len(items),
            "missing_items": sum(1 for item in items if item["recorded_decision_state"] == "missing"),
            "drift_items": sum(1 for item in items if item["recorded_decision_state"] == "drift"),
        },
        "items": items[:limit],
    }


def build_source_remediation_queue(conn: Any, *, days: int = 30, limit: int = 20) -> dict[str, Any]:
    dashboard = build_decision_dashboard(conn, days=days, limit=max(limit * 2, 20))
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in dashboard["priority_actions"] + dashboard["observe_actions"]:
        remediation_action = ""
        priority = "p2"
        reason = item["source_confidence_reason"]
        if item["source_confidence"] in {"fragile", "developing"}:
            remediation_action = "attach_first_hand_artifact"
            priority = "p1" if item["action_state"] != "observe" else "p2"
            if item["source_confidence"] == "developing":
                reason = f"{reason} 建议补一手来源，把二手判断升级成更稳的 thesis 输入。"
            else:
                reason = f"{reason} 建议先补一手锚点，再决定是否提升动作优先级。"
        elif item["needs_source_revisit"]:
            remediation_action = "refresh_source_feedback"
            priority = "p2"
            reason = "linked second-hand source 里有 aging/stale feedback，建议先刷新来源反馈。"
        else:
            continue
        key = (item["target_case_id"], remediation_action)
        if key in seen:
            continue
        seen.add(key)
        second_hand_sources = [source for source in item["linked_sources"] if source["primaryness"] == "second_hand"]
        recipe: dict[str, Any]
        if remediation_action == "refresh_source_feedback" and second_hand_sources:
            source = second_hand_sources[0]
            suggested_feedback_type = "high_signal" if source["source_priority_label"] == "high_priority" else "useful_context"
            recipe = {
                "command": "record-source-feedback",
                "args": {
                    "source-id": source["source_id"],
                    "feedback-type": suggested_feedback_type,
                },
                "hints": {
                    "note": f"重新确认 {source['name']} 当前是否仍值得持续跟踪。",
                },
            }
        else:
            recipe = {
                "command": "remediate-thesis",
                "args": {
                    "thesis-id": item["thesis_id"],
                    "action": "attach_first_hand_artifact",
                },
                "hints": {
                    "needs_artifact_id": True,
                    "source_hint": "优先补官方披露、论文原文、治理提案或 protocol docs 这类一手来源。",
                },
            }
        items.append(
            {
                "priority": priority,
                "remediation_action": remediation_action,
                "target_case_id": item["target_case_id"],
                "target": item["target"],
                "thesis_id": item["thesis_id"],
                "thesis_title": item["thesis_title"],
                "action_state": item["action_state"],
                "source_confidence": item["source_confidence"],
                "needs_source_revisit": item["needs_source_revisit"],
                "linked_sources": item["linked_sources"],
                "reason": reason,
                "recipe": recipe,
            }
        )
    items.sort(
        key=lambda item: (
            0 if item["priority"] == "p1" else 1,
            DECISION_ACTION_ORDER.get(item["action_state"], 99),
            SOURCE_CONFIDENCE_ORDER.get(item["source_confidence"], 99),
            item["thesis_title"],
            item["target"]["ticker_or_symbol"] or "",
        )
    )
    return {
        "summary": {
            **summary_counts(conn),
            "days": days,
            "queue_items": len(items),
            "p1_items": sum(1 for item in items if item["priority"] == "p1"),
            "p2_items": sum(1 for item in items if item["priority"] == "p2"),
            "attach_first_hand_artifact": sum(1 for item in items if item["remediation_action"] == "attach_first_hand_artifact"),
            "refresh_source_feedback": sum(1 for item in items if item["remediation_action"] == "refresh_source_feedback"),
        },
        "items": items[:limit],
    }


def build_verification_remediation_queue(conn: Any, *, days: int = 30, limit: int = 20) -> dict[str, Any]:
    verification_candidates = _collect_verification_candidates(conn, limit=max(limit * 10, 80))
    thesis_board = build_thesis_board(conn)
    thesis_map = {item["thesis_id"]: item for item in thesis_board["items"]}
    items: list[dict[str, Any]] = []
    for item in verification_candidates:
        candidate_theses = item.get("candidate_theses", [])
        if len(candidate_theses) == 1:
            thesis_id = candidate_theses[0]["thesis_id"]
            thesis = thesis_map.get(thesis_id)
        else:
            thesis_id = ""
            thesis = None
        first_hand_artifacts: list[str] = []
        if thesis:
            source_map = _artifact_source_map(conn, thesis["version"].get("created_from_artifacts", []))
            first_hand_artifacts = [
                artifact_id
                for artifact_id, artifact in source_map.items()
                if artifact["primaryness"] == "first_hand"
            ]
        if thesis_id and first_hand_artifacts:
            remediation_action = "accept_corroboration_with_evidence"
            priority = "p1"
            reason = "该线索已挂到唯一 thesis，且 thesis 已有一手锚点，可以直接完成一次 corroboration 闭环。"
            recipe = {
                "command": "apply-route",
                "args": {
                    "route-id": item["route_id"],
                    "status": "accepted",
                    "link-object-type": "thesis",
                    "link-object-id": thesis_id,
                },
                "hints": {
                    "suggested_evidence_artifact_ids": first_hand_artifacts,
                    "note": "accepted corroboration with first-hand evidence",
                },
            }
        elif thesis_id:
            remediation_action = "find_first_hand_evidence"
            priority = "p2"
            reason = "该线索已挂到唯一 thesis，但 thesis 还没有可复用的一手锚点，先补官方证据。"
            recipe = {
                "command": "remediate-thesis",
                "args": {
                    "thesis-id": thesis_id,
                    "action": "attach_first_hand_artifact",
                },
                "hints": {
                    "route_id_after_anchor": item["route_id"],
                    "source_hint": "优先补官方披露、治理提案、论文原文或 protocol docs。",
                },
            }
        else:
            remediation_action = "clarify_thesis_mapping"
            priority = "p2"
            reason = "这条核验线索还没有唯一 thesis 候选，先确认它应该落到哪条 thesis。"
            recipe = {
                "command": "route-workbench",
                "args": {
                    "status": "pending",
                    "route-type": "corroboration_needed",
                    "source-id": item["source_id"],
                },
                "hints": {
                    "route_id": item["route_id"],
                },
            }
        items.append(
            {
                **item,
                "priority": priority,
                "remediation_action": remediation_action,
                "thesis_id": thesis_id,
                "thesis_title": thesis["title"] if thesis else "",
                "suggested_evidence_artifact_ids": first_hand_artifacts,
                "recipe": recipe,
                "reason": reason,
            }
        )
    items.sort(
        key=lambda item: (
            0 if item["priority"] == "p1" else 1,
            THESIS_STATUS_ORDER.get(
                thesis_map.get(item["thesis_id"], {}).get("status", ""),
                99,
            ),
            -(item.get("confidence") or 0.0),
            item["source_name"],
            item["claim_preview"],
        )
    )
    return {
        "summary": {
            **summary_counts(conn),
            "days": days,
            "queue_items": len(items),
            "p1_items": sum(1 for item in items if item["priority"] == "p1"),
            "p2_items": sum(1 for item in items if item["priority"] == "p2"),
            "accept_corroboration_with_evidence": sum(
                1 for item in items if item["remediation_action"] == "accept_corroboration_with_evidence"
            ),
            "find_first_hand_evidence": sum(
                1 for item in items if item["remediation_action"] == "find_first_hand_evidence"
            ),
            "clarify_thesis_mapping": sum(
                1 for item in items if item["remediation_action"] == "clarify_thesis_mapping"
            ),
        },
        "items": items[:limit],
    }


def build_verification_remediation_batches(conn: Any, *, days: int = 30, limit: int = 20) -> dict[str, Any]:
    queue = build_verification_remediation_queue(conn, days=days, limit=max(limit * 8, 40))
    groups: dict[str, dict[str, Any]] = {}
    for item in queue["items"]:
        evidence_artifact_ids = tuple(item.get("suggested_evidence_artifact_ids") or [])
        batch_key = stable_id(
            "vbatch",
            "::".join(
                [
                    item["remediation_action"],
                    item["source_id"],
                    item.get("thesis_id", ""),
                    ",".join(evidence_artifact_ids),
                ]
            ),
        )
        group = groups.setdefault(
            batch_key,
            {
                "batch_id": batch_key,
                "priority": item["priority"],
                "remediation_action": item["remediation_action"],
                "source_id": item["source_id"],
                "source_name": item["source_name"],
                "thesis_id": item.get("thesis_id", ""),
                "thesis_title": item.get("thesis_title", ""),
                "suggested_evidence_artifact_ids": list(evidence_artifact_ids),
                "route_count": 0,
                "route_ids": [],
                "sample_claim_previews": [],
                "max_confidence": 0.0,
            },
        )
        group["route_count"] += 1
        group["route_ids"].append(item["route_id"])
        if len(group["sample_claim_previews"]) < 3:
            group["sample_claim_previews"].append(item["claim_preview"])
        group["max_confidence"] = max(group["max_confidence"], item.get("confidence") or 0.0)

    items: list[dict[str, Any]] = []
    for group in groups.values():
        if group["remediation_action"] == "accept_corroboration_with_evidence" and group["thesis_id"]:
            recipe = {
                "command": "apply-route-batch",
                "args": {
                    "route-id": group["route_ids"],
                    "status": "accepted",
                    "link-object-type": "thesis",
                    "link-object-id": group["thesis_id"],
                },
                "hints": {
                    "suggested_evidence_artifact_ids": group["suggested_evidence_artifact_ids"],
                    "note": "bulk accepted corroboration with shared first-hand evidence",
                },
            }
            reason = "同一来源/同一 thesis/同一一手锚点的 pending corroboration 可以批量收敛。"
        else:
            recipe = {
                "command": "verification-remediation-queue",
                "args": {
                    "days": days,
                    "limit": limit,
                },
                "hints": {
                    "source_id": group["source_id"],
                    "thesis_id": group["thesis_id"],
                },
            }
            reason = "该组仍需逐条人工确认，暂不建议直接批量 apply。"
        items.append(
            {
                **group,
                "reason": reason,
                "recipe": recipe,
            }
        )

    items.sort(
        key=lambda item: (
            0 if item["priority"] == "p1" else 1,
            -item["route_count"],
            item["source_name"],
            item["thesis_title"] or item["thesis_id"],
        )
    )
    return {
        "summary": {
            **summary_counts(conn),
            "days": days,
            "batch_items": len(items),
            "batched_routes": sum(item["route_count"] for item in items),
            "multi_route_batches": sum(1 for item in items if item["route_count"] > 1),
            "accept_corroboration_with_evidence": sum(
                1 for item in items if item["remediation_action"] == "accept_corroboration_with_evidence"
            ),
            "find_first_hand_evidence": sum(
                1 for item in items if item["remediation_action"] == "find_first_hand_evidence"
            ),
            "clarify_thesis_mapping": sum(
                1 for item in items if item["remediation_action"] == "clarify_thesis_mapping"
            ),
        },
        "items": items[:limit],
    }


def _validation_update_score(item: dict[str, Any]) -> tuple[int, int]:
    text = item.get("claim_preview", "") or ""
    lowered = text.lower()
    keyword_bonus = sum(
        1
        for keyword in ("ai", "算力", "订单", "扩产", "业绩", "融资", "市场规模", "tvl", "fees", "revenue")
        if keyword in lowered
    )
    digit_bonus = 2 if re.search(r"\d", text) else 0
    length_bonus = 1 if 18 <= len(text) <= 120 else 0
    verdict_bonus = 1 if item.get("verdict") == "validated" else 0
    return (digit_bonus + keyword_bonus + length_bonus + verdict_bonus, len(text))


def _compact_verified_updates(items: list[dict[str, Any]], *, limit: int = 5) -> tuple[list[dict[str, Any]], int]:
    selected: list[dict[str, Any]] = []
    per_group: dict[tuple[str, str], int] = {}
    ranked = sorted(
        items,
        key=lambda item: (
            -_validation_update_score(item)[0],
            -_validation_update_score(item)[1],
            item.get("source_name", ""),
            item.get("thesis_title", "") or item.get("thesis_id", ""),
        ),
    )
    max_items = max(1, min(limit, 5))
    for item in ranked:
        group_key = (item.get("source_name", ""), item.get("thesis_title", "") or item.get("thesis_id", ""))
        if per_group.get(group_key, 0) >= 2:
            continue
        selected.append(item)
        per_group[group_key] = per_group.get(group_key, 0) + 1
        if len(selected) >= max_items:
            break
    return selected, max(0, len(items) - len(selected))


def _decision_output_level(
    item: dict[str, Any],
    gate_by_thesis: dict[str, dict[str, Any]],
) -> tuple[str, str]:
    gate = gate_by_thesis.get(item["thesis_id"], {})
    if item["action_state"] != "observe" and gate.get("can_recommend_active"):
        return (
            "Level 3",
            "Trade Recommendation",
        )
    if item["action_state"] != "observe" or item["thesis_status"] in {"evidence_backed", "active"}:
        return (
            "Level 2",
            "Framework",
        )
    return (
        "Level 1",
        "Observation",
    )


def build_weekly_decision_note(conn: Any, *, days: int = 7, limit: int = 8) -> dict[str, Any]:
    dashboard = build_decision_dashboard(conn, days=days, limit=limit)
    gate_report = build_thesis_gate_report(conn)
    gate_by_thesis = {item["thesis_id"]: item for item in gate_report["items"]}
    decision_journal = build_decision_journal(conn, days=max(days, 14), limit=limit)
    decision_maintenance = build_decision_maintenance_queue(conn, days=max(days, 14), limit=limit)
    verification_remediation = build_verification_remediation_queue(conn, limit=limit)
    verification_batches = build_verification_remediation_batches(conn, limit=limit)
    review_remediation = build_review_remediation_queue(conn, limit=limit)
    source_remediation = build_source_remediation_queue(conn, limit=limit)
    source_revisit = build_source_revisit_workbench(conn, limit=limit)
    compact_verified_updates, hidden_verified_updates = _compact_verified_updates(dashboard["verified_updates"], limit=limit)
    as_of = _today_iso()
    lines = [
        "# Weekly Decision Note",
        "",
        f"- As of: `{as_of}`",
        f"- Window: 最近 `{days}` 天",
        f"- Priority actions: `{dashboard['summary']['priority_actions']}`",
        f"- Verification queue: `{dashboard['summary']['verification_queue']}`",
        "",
        "## Executive Summary",
    ]
    if dashboard["priority_actions"]:
        for item in dashboard["priority_actions"][:3]:
            level_tag, level_label = _decision_output_level(item, gate_by_thesis)
            review_label = _review_surface_label(
                freshness=item["review_freshness"],
                source=item["effective_review_source"],
            )
            lines.append(
                f"- `{level_tag}` `{item['action_state']}` `{item['target']['ticker_or_symbol']}`: {item['thesis_title']}。"
                f"`class={level_label}` / "
                f"`review={review_label}` / `source={item['source_confidence']}`。{item['reason']}"
            )
    else:
        lines.append("- 本周没有进入高优先动作层的标的，先继续验证。")

    lines.extend(["", "## Source Tracking"])
    if dashboard["source_focus"]:
        for item in dashboard["source_focus"][:limit]:
            linked = ", ".join(thesis["title"] for thesis in item["linked_theses"][:2]) or "暂无已挂 thesis"
            latest = item["latest_artifact_title"] or "无最新 artifact"
            viewpoint = item.get("latest_viewpoint_summary") or "暂无显式观点摘要"
            viewpoint_status = item.get("latest_viewpoint_status") or item["validation_state"]
            source_label_key = "trust" if item["primaryness"] == "first_hand" else "priority"
            source_label_value = item.get("source_display_label") or item["source_priority_label"]
            lines.append(
                f"- `{item['name']}` `{item['validation_state']}`: 最新输入《{latest}》，"
                f"`accepted={item['accepted_route_count']}` / `pending={item['pending_route_count']}`，"
                f"`{source_label_key}={source_label_value}` / `feedback={item['feedback_freshness']}`，"
                f"关联 thesis: {linked}。最新观点 `{viewpoint_status}`: {viewpoint}"
            )
    else:
        lines.append("- 本周没有进入来源跟踪面的新增输入。")

    lines.extend(["", "## Validation Changes"])
    if compact_verified_updates:
        for item in compact_verified_updates:
            thesis_label = item.get("thesis_title") or item.get("thesis_id") or "待人工挂 thesis"
            lines.append(
                f"- `{item['source_name']}` 已形成 `{item['verdict']}` validation case，并落到 {thesis_label}: {item['claim_preview']}"
            )
        if hidden_verified_updates:
            lines.append(f"- 其余 `{hidden_verified_updates}` 条 validation 已写入 ledger，按需再看 `validation-board`。")
    else:
        lines.append("- 本周没有新增的 validation case。")

    lines.extend(["", "## Decision Actions"])
    if dashboard["priority_actions"]:
        for item in dashboard["priority_actions"][:limit]:
            level_tag, level_label = _decision_output_level(item, gate_by_thesis)
            catalyst = item["catalysts"][0] if item["catalysts"] else "待补催化剂"
            review_label = _review_surface_label(
                freshness=item["review_freshness"],
                source=item["effective_review_source"],
            )
            lines.append(
                f"- `{level_tag}` `{item['action_state']}` `{item['target']['ticker_or_symbol']}` / {item['thesis_title']}: "
                f"`class={level_label}`，"
                f"`validation={item['validation_state']}`，`review={review_label}`，`source={item['source_confidence']}`，"
                f"`operator={item['recorded_action_state'] or 'unrecorded'}`，`raw_posture={item['raw_posture']}`，催化剂 `{catalyst}`。{item['reason']}"
            )
    else:
        lines.append("- 当前没有值得立即推进的 `prepare/starter/add/trim/exit` 项。")

    lines.extend(["", "## Observe / Do Not Rush"])
    if dashboard["observe_actions"]:
        for item in dashboard["observe_actions"][:limit]:
            level_tag, level_label = _decision_output_level(item, gate_by_thesis)
            review_label = _review_surface_label(
                freshness=item["review_freshness"],
                source=item["effective_review_source"],
            )
            lines.append(
                f"- `{level_tag}` `observe` `{item['target']['ticker_or_symbol']}` / {item['thesis_title']}: "
                f"`class={level_label}`，"
                f"`validation={item['validation_state']}`，`review={review_label}`，`source={item['source_confidence']}`，"
                f"`operator={item['recorded_action_state'] or 'unrecorded'}`，{item['reason']}"
            )
    else:
        lines.append("- 当前没有需要单独列出的 observe 项。")

    system_health_labels: list[str] = []

    if decision_journal["items"]:
        lines.extend(["", "## Recorded Decisions"])
        for item in decision_journal["items"][:limit]:
            lines.append(
                f"- `{item['decision_date']}` `{item['action_state']}` `{item['ticker_or_symbol']}` / {item['thesis_title']}: "
                f"`status={item['status']}`，`alignment={item['alignment']}`，`review={item['review_freshness']}`，"
                f"`source={item['source_confidence']}`。{item['rationale_preview'] or '未填写 rationale'}"
            )
    else:
        system_health_labels.append("decision journal=0")

    if decision_maintenance["items"]:
        lines.extend(["", "## Decision Maintenance"])
        for item in decision_maintenance["items"][:limit]:
            lines.append(
                f"- `{item['priority']}` `{item['target']['ticker_or_symbol']}` / {item['thesis_title']}: "
                f"`current={item['action_state']}`，`recorded={item['recorded_action_state'] or 'none'}`，"
                f"`state={item['recorded_decision_state']}`。{item['reason']}"
            )
    else:
        system_health_labels.append("decision maintenance=0")

    if dashboard["verification_queue"]:
        lines.extend(["", "## Verification Queue"])
        for item in dashboard["verification_queue"][:limit]:
            theses = ", ".join(candidate["title"] for candidate in item["candidate_theses"][:2]) or "待人工挂 thesis"
            lines.append(
                f"- `{item['source_name']}` -> {theses}: {item['claim_preview']} "
                f"(建议 `{item['suggested_action']}`)"
            )
    else:
        system_health_labels.append("verification queue=0")

    if verification_remediation["items"]:
        lines.extend(["", "## Verification Remediation Queue"])
        for item in verification_remediation["items"][:limit]:
            thesis_label = item["thesis_title"] or item["thesis_id"] or "待人工挂 thesis"
            lines.append(
                f"- `{item['priority']}` `{item['remediation_action']}` "
                f"`{item['source_name']}` -> {thesis_label}: {item['claim_preview']}"
            )
    else:
        system_health_labels.append("verification remediation=0")

    if verification_batches["items"]:
        lines.extend(["", "## Verification Batch Opportunities"])
        for item in verification_batches["items"][:limit]:
            thesis_label = item["thesis_title"] or item["thesis_id"] or "待人工挂 thesis"
            lines.append(
                f"- `{item['priority']}` `{item['remediation_action']}` "
                f"`{item['source_name']}` -> {thesis_label}: "
                f"`routes={item['route_count']}`，`evidence={','.join(item['suggested_evidence_artifact_ids']) or 'none'}`"
            )
    else:
        system_health_labels.append("verification batches=0")

    if dashboard["review_attention"]:
        lines.extend(["", "## Review Freshness"])
        for item in dashboard["review_attention"][:limit]:
            age = "missing" if item["review_age_days"] is None else f"{item['review_age_days']}d"
            source = item["effective_review_source"]
            gap = item["effective_review_gap_type"]
            lines.append(
                f"- `{item['target']['ticker_or_symbol']}` / {item['thesis_title']}: "
                f"`review={item['review_freshness']}` (`{age}`，`source={source}`，`gap={gap}`)"
            )
    else:
        system_health_labels.append("review freshness=healthy")

    if dashboard["source_guard"]:
        lines.extend(["", "## Source Guard"])
        for item in dashboard["source_guard"][:limit]:
            lines.append(
                f"- `{item['target']['ticker_or_symbol']}` / {item['thesis_title']}: "
                f"`source={item['source_confidence']}`，`revisit={item['needs_source_revisit']}`。"
                f"{item['source_confidence_reason']} 主要来源: {_source_support_surface(item['linked_sources'])}"
            )
    else:
        system_health_labels.append("source guard=healthy")

    if source_remediation["items"]:
        lines.extend(["", "## Source Remediation Queue"])
        for item in source_remediation["items"][:limit]:
            lines.append(
                f"- `{item['priority']}` `{item['remediation_action']}` "
                f"`{item['target']['ticker_or_symbol']}` / {item['thesis_title']}: "
                f"`source={item['source_confidence']}`。{item['reason']}"
            )
    else:
        system_health_labels.append("source remediation=0")

    if review_remediation["items"]:
        lines.extend(["", "## Review Remediation Queue"])
        for item in review_remediation["items"][:limit]:
            lines.append(
                f"- `{item['priority']}` `{item['remediation_action']}` "
                f"`{item['target']['ticker_or_symbol']}` / {item['thesis_title']}: "
                f"`review={item['review_freshness']}`，`source={item['effective_review_source']}`，"
                f"`gap={item['effective_review_gap_type']}`。{item['reason']}"
            )
    else:
        system_health_labels.append("review remediation=0")

    if source_revisit["items"]:
        lines.extend(["", "## Source Revisit Queue"])
        for item in source_revisit["items"][:limit]:
            lines.append(
                f"- `{item['priority']}` `{item['name']}`: "
                f"`feedback={item['feedback_freshness']}`，`latest={item['latest_feedback_type']}`，"
                f"`priority={item['source_priority_label']}`。{item['reason']}"
            )
    else:
        system_health_labels.append("source revisit=0")

    if system_health_labels:
        lines.extend(["", "## System Health"])
        lines.append(f"- 当前 operator 健康状态：{', '.join(system_health_labels)}。")

    lines.extend(["", "## Next Week Watch"])
    next_watch_items = dashboard["priority_actions"][:3] + dashboard["verification_queue"][:3]
    if next_watch_items:
        seen_watch: set[str] = set()
        for item in next_watch_items[:6]:
            if "target" in item:
                watch = item["confirmation_signals"][0] if item["confirmation_signals"] else item["reason"]
                line = f"- `{item['target']['ticker_or_symbol']}`: {watch}"
            else:
                line = f"- `{item['source_name']}`: 完成一手 corroboration，并决定是否挂入目标 thesis"
            if line in seen_watch:
                continue
            seen_watch.add(line)
            lines.append(line)
    else:
        lines.append("- 下周先维持当前 thesis 跟踪节奏。")

    markdown = "\n".join(lines).rstrip() + "\n"
    return {
        "summary": dashboard["summary"],
        "as_of": as_of,
        "days": days,
        "markdown": markdown,
        "sections": {
            "source_focus": dashboard["source_focus"][:limit],
            "priority_actions": dashboard["priority_actions"][:limit],
            "observe_actions": dashboard["observe_actions"][:limit],
            "decision_journal": decision_journal["items"][:limit],
            "decision_maintenance": decision_maintenance["items"][:limit],
            "verification_queue": dashboard["verification_queue"][:limit],
            "verification_remediation": verification_remediation["items"][:limit],
            "verification_batches": verification_batches["items"][:limit],
            "review_attention": dashboard["review_attention"][:limit],
            "source_guard": dashboard["source_guard"][:limit],
            "source_remediation": source_remediation["items"][:limit],
            "review_remediation": review_remediation["items"][:limit],
            "source_revisit": source_revisit["items"][:limit],
        },
    }


def build_promotion_wizard(conn: Any) -> dict[str, Any]:
    thesis_board = build_thesis_board(conn)
    recommendation_map = {
        "has_first_hand_artifact": "attach_first_hand_artifact",
        "has_invalidator": "add_invalidator",
        "has_target_mapping": "create_target_case",
        "has_counter_material": "add_counter_material",
        "corroboration_debt_under_control": "resolve_pending_corroboration",
    }
    items: list[dict[str, Any]] = []
    for thesis in thesis_board["items"]:
        gate = thesis["promotion_gate"]
        recommendations = [recommendation_map[item] for item in gate["missing"] if item in recommendation_map]
        active_recommendations = [recommendation_map[item] for item in gate["active_missing"] if item in recommendation_map]
        recommended_next_status = None
        if gate["can_promote_to_evidence_backed"] and thesis["status"] in {"seed", "framed"}:
            recommended_next_status = "evidence_backed"
        elif gate["can_promote_to_evidence_backed"] and gate["can_recommend_active"] and thesis["status"] == "evidence_backed":
            recommended_next_status = "active"
        items.append(
            {
                "thesis_id": thesis["thesis_id"],
                "title": thesis["title"],
                "current_status": thesis["status"],
                "recommended_next_status": recommended_next_status,
                "missing": gate["missing"],
                "active_missing": gate["active_missing"],
                "recommendations": recommendations,
                "active_recommendations": active_recommendations,
                "action_hints": [
                    {
                        "action": action,
                        "thesis_id": thesis["thesis_id"],
                        "requires_route_link": action == "attach_first_hand_artifact" and gate["requires_corroborated_first_hand"],
                    }
                    for action in recommendations
                ],
                "pending_corroboration_count": gate["pending_corroboration_count"],
                "accepted_corroboration_count": gate["accepted_corroboration_count"],
                "validated_corroboration_count": gate["validated_corroboration_count"],
                "corroboration_debt_ratio": gate["corroboration_debt_ratio"],
                "requires_corroborated_first_hand": gate["requires_corroborated_first_hand"],
                "target_case_count": gate["target_case_count"],
            }
        )
    return {
        "summary": {
            **summary_counts(conn),
            "wizard_items": len(items),
            "ready_to_promote": sum(1 for item in items if item["recommended_next_status"]),
        },
        "items": items,
    }


def build_today_cockpit(conn: Any) -> dict[str, Any]:
    board = build_thesis_board(conn)
    theme_map = build_theme_map(conn)
    watch_board = build_watch_board(conn)
    sentinel_board = build_sentinel_board(conn, limit=8)
    opportunity_inbox = build_opportunity_inbox(conn, limit=8)
    theme_radar = build_theme_radar_board(conn, limit=8)
    event_ledger = build_event_ledger(conn, limit=12)
    anti_thesis_board = build_anti_thesis_board(conn, limit=8)
    evaluation_board = build_event_evaluation_board(conn, limit=8)
    intake_inbox = build_intake_inbox(conn)
    review_board = build_review_board(conn)
    route_workbench = build_route_workbench(conn, limit=12)
    source_viewpoint_workbench = build_source_viewpoint_workbench(conn, limit=8)
    source_feedback_workbench = build_source_feedback_workbench(conn, limit=8)
    source_revisit_workbench = build_source_revisit_workbench(conn, limit=8)
    decision_dashboard = build_decision_dashboard(conn, days=7, limit=8)
    decision_journal = build_decision_journal(conn, days=30, limit=8)
    decision_maintenance = build_decision_maintenance_queue(conn, days=30, limit=8)
    verification_remediation = build_verification_remediation_queue(conn, limit=8)
    verification_batches = build_verification_remediation_batches(conn, limit=8)
    route_normalization = build_route_normalization_queue(conn, limit=8)
    source_remediation = build_source_remediation_queue(conn, limit=8)
    review_remediation = build_review_remediation_queue(conn, limit=8)
    ctx = _load_context(conn)
    today = _today_iso()
    alerted_monitors = [
        {
            "monitor_id": row["monitor_id"],
            "owner_object_type": row["owner_object_type"],
            "owner_object_id": row["owner_object_id"],
            "monitor_type": row["monitor_type"],
            "metric_name": row["metric_name"],
            "latest_value": row["latest_value"],
            "threshold_value": row["threshold_value"],
            "last_checked_at": row["last_checked_at"],
        }
        for row in list_rows(
            conn,
            "SELECT * FROM monitors WHERE status = 'alerted' ORDER BY last_checked_at DESC, created_at DESC",
        )
    ]
    due_reviews = [
        {
            "review_id": row["review_id"],
            "owner_object_id": row["owner_object_id"],
            "review_date": row["review_date"],
            "result": row["result"],
            "lessons": row["lessons"],
        }
        for row in list_rows(
            conn,
            "SELECT * FROM reviews WHERE review_date <= ? ORDER BY review_date DESC, created_at DESC",
            (today,),
        )
    ]
    pending_routes = [
        {
            "route_id": row["route_id"],
            "claim_id": row["claim_id"],
            "artifact_id": row["artifact_id"],
            "route_type": row["route_type"],
            "target_object_type": row["target_object_type"],
            "target_object_id": row["target_object_id"],
            "reason": row["reason"],
        }
        for row in list_rows(
            conn,
            "SELECT * FROM claim_routes WHERE status = 'pending' ORDER BY created_at DESC LIMIT 12",
        )
    ]
    recent_artifacts = [
        {
            "artifact_id": row["artifact_id"],
            "title": row["title"],
            "artifact_kind": row["artifact_kind"],
            "captured_at": row["captured_at"],
            "published_at": row["published_at"],
            "status": row["status"],
        }
        for row in ctx["artifacts"][:8]
    ]
    active_theses = [
        item
        for item in board["items"]
        if item["status"] in {"active", "evidence_backed", "framed", "seed"}
    ][:6]
    key_themes = [
        item for item in theme_map["items"] if item["importance_status"] in {"priority", "tracking", "scouting"}
    ][:6]
    return {
        "summary": summary_counts(conn),
        "focus": {
            "key_themes": key_themes,
            "active_theses": active_theses,
            "watch_items": watch_board["items"][:8],
            "event_mining_summary": {
                "sentinel": sentinel_board["summary"],
                "opportunity": opportunity_inbox["summary"],
                "theme_radar": theme_radar["summary"],
                "ledger": event_ledger["summary"],
                "anti_thesis": anti_thesis_board["summary"],
                "evaluation": evaluation_board["summary"],
            },
            "sentinel_items": sentinel_board["items"][:6],
            "opportunity_items": opportunity_inbox["items"][:6],
            "theme_radar_items": theme_radar["items"][:6],
            "recent_event_items": event_ledger["items"][:8],
            "anti_thesis_summary": anti_thesis_board["summary"],
            "anti_thesis_items": anti_thesis_board["items"][:6],
            "event_evaluation_summary": evaluation_board["summary"],
            "event_evaluation_items": evaluation_board["items"][:6],
            "alerted_monitors": alerted_monitors[:8],
            "due_reviews": due_reviews[:8],
            "review_summary": review_board["summary"],
            "intake_summary": intake_inbox["summary"],
            "intake_items": intake_inbox["items"][:6],
            "route_workbench_summary": route_workbench["summary"],
            "route_batch_recipes": route_workbench["batch_recipes"][:8],
            "route_workbench_items": route_workbench["items"][:8],
            "route_normalization_summary": route_normalization["summary"],
            "route_normalization_batches": route_normalization["batches"][:6],
            "route_normalization_items": route_normalization["items"][:6],
            "source_viewpoint_workbench_summary": source_viewpoint_workbench["summary"],
            "source_viewpoint_items": source_viewpoint_workbench["items"][:6],
            "source_feedback_workbench_summary": source_feedback_workbench["summary"],
            "source_feedback_items": source_feedback_workbench["items"][:6],
            "source_revisit_workbench_summary": source_revisit_workbench["summary"],
            "source_revisit_items": source_revisit_workbench["items"][:6],
            "decision_summary": decision_dashboard["summary"],
            "decision_journal_summary": decision_journal["summary"],
            "recent_decisions": decision_journal["items"][:6],
            "decision_maintenance_summary": decision_maintenance["summary"],
            "decision_maintenance_items": decision_maintenance["items"][:6],
            "priority_actions": decision_dashboard["priority_actions"][:6],
            "source_guard_items": decision_dashboard["source_guard"][:6],
            "source_remediation_summary": source_remediation["summary"],
            "source_remediation_items": source_remediation["items"][:6],
            "review_remediation_summary": review_remediation["summary"],
            "review_remediation_items": review_remediation["items"][:6],
            "source_focus": decision_dashboard["source_focus"][:6],
            "verification_queue": decision_dashboard["verification_queue"][:6],
            "verification_remediation_summary": verification_remediation["summary"],
            "verification_remediation_items": verification_remediation["items"][:6],
            "verification_batch_summary": verification_batches["summary"],
            "verification_batch_items": verification_batches["items"][:6],
            "validated_updates": decision_dashboard["verified_updates"][:6],
            "pending_routes": pending_routes,
        },
        "recent_artifacts": recent_artifacts,
    }


def _compact_integration_target(item: dict[str, Any]) -> dict[str, Any]:
    target = item.get("target") or {}
    return {
        "target_case_id": item.get("target_case_id"),
        "thesis_id": item.get("thesis_id"),
        "thesis_title": item.get("thesis_title"),
        "ticker_or_symbol": target.get("ticker_or_symbol") or item.get("ticker_or_symbol") or "",
        "asset_class": target.get("asset_class") or item.get("asset_class") or "",
        "action_state": item.get("action_state") or item.get("desired_posture") or "",
        "raw_posture": item.get("raw_posture") or "",
        "validation_state": item.get("validation_state") or "",
        "source_confidence": item.get("source_confidence") or "",
        "source_confidence_reason": item.get("source_confidence_reason") or "",
        "review_freshness": item.get("review_freshness") or "",
        "effective_review_source": item.get("effective_review_source") or "",
        "recorded_action_state": item.get("recorded_action_state") or "",
        "reason": item.get("reason") or "",
        "catalysts": item.get("catalysts") or [],
        "confirmation_signals": item.get("confirmation_signals") or [],
        "linked_sources": [
            {
                "source_id": source.get("source_id"),
                "name": source.get("name"),
                "primaryness": source.get("primaryness"),
                "source_type": source.get("source_type"),
                "source_display_label": source.get("source_display_label"),
            }
            for source in (item.get("linked_sources") or [])[:4]
        ],
    }


def _compact_integration_source(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_id": item.get("source_id"),
        "name": item.get("name"),
        "source_type": item.get("source_type"),
        "primaryness": item.get("primaryness"),
        "validation_state": item.get("validation_state"),
        "source_display_label": item.get("source_display_label"),
        "source_priority_label": item.get("source_priority_label"),
        "feedback_freshness": item.get("feedback_freshness"),
        "latest_artifact_title": item.get("latest_artifact_title"),
        "latest_viewpoint_status": item.get("latest_viewpoint_status"),
        "latest_viewpoint_summary": item.get("latest_viewpoint_summary"),
        "pending_route_count": item.get("pending_route_count"),
        "accepted_route_count": item.get("accepted_route_count"),
        "linked_theses": [
            {
                "thesis_id": thesis.get("thesis_id"),
                "title": thesis.get("title"),
                "status": thesis.get("status"),
            }
            for thesis in (item.get("linked_theses") or [])[:4]
        ],
    }


def _compact_integration_thesis(item: dict[str, Any]) -> dict[str, Any]:
    gate = item.get("promotion_gate") or {}
    return {
        "thesis_id": item.get("thesis_id"),
        "title": item.get("title"),
        "status": item.get("status"),
        "validation_state": item.get("validation_state"),
        "horizon_months": item.get("horizon_months"),
        "theme_count": len(item.get("themes") or []),
        "target_case_count": len(item.get("target_cases") or []),
        "gate_missing": gate.get("missing", [])[:6],
        "gate_active_missing": gate.get("active_missing", [])[:6],
        "pending_corroboration_count": gate.get("pending_corroboration_count", 0),
        "validated_corroboration_count": gate.get("validated_corroboration_count", 0),
    }


def _compact_integration_validation(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "validation_case_id": item.get("validation_case_id"),
        "verdict": item.get("verdict"),
        "claim_preview": item.get("claim_preview") or _preview(item.get("claim_text"), 120),
        "source_name": item.get("source_name") or "",
        "thesis_id": item.get("thesis_id") or "",
        "thesis_title": item.get("thesis_title") or "",
        "rationale": item.get("rationale") or "",
        "evidence_artifact_ids": item.get("evidence_artifact_ids") or [],
    }


def _compact_integration_review(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "review_id": item.get("review_id"),
        "owner_object_type": item.get("owner_object_type"),
        "owner_object_id": item.get("owner_object_id"),
        "review_date": item.get("review_date"),
        "result": item.get("result"),
        "lessons": item.get("lessons") or "",
        "source_attribution": item.get("source_attribution") or "",
    }


def _compact_integration_pattern(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "pattern_id": item.get("pattern_id"),
        "pattern_kind": item.get("pattern_kind"),
        "label": item.get("label"),
        "description": item.get("description"),
        "review_id": item.get("review_id"),
        "trigger_terms": item.get("trigger_terms") or [],
    }


def _integration_graph_checks(conn: Any, *, thesis_id: str) -> dict[str, Any]:
    graph = build_graph_from_db(conn, thesis_id=thesis_id)
    conflicts = detect_conflicts(graph)
    unresolved_conflicts = [item for item in conflicts if not item.get("resolved")]
    broken_support_chains = find_broken_support_chains(graph)
    return {
        "conflict_count": len(conflicts),
        "unresolved_conflict_count": len(unresolved_conflicts),
        "broken_support_chain_count": len(broken_support_chains),
        "conflicts": unresolved_conflicts[:10],
        "broken_support_chains": broken_support_chains[:10],
    }


def build_integration_snapshot(
    conn: Any,
    *,
    scope: str,
    thesis_id: str = "",
    days: int = 7,
    limit: int = 6,
) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    scope_key = scope.strip().lower()
    safe_limit = max(1, min(limit, 20))
    if scope_key == "today":
        cockpit = build_today_cockpit(conn)
        focus = cockpit["focus"]
        return {
            "ok": True,
            "scope": "today",
            "generated_at": generated_at,
            "summary": cockpit["summary"],
            "queue_summary": {
                "verification_queue": focus["decision_summary"]["verification_queue"],
                "decision_maintenance": focus["decision_maintenance_summary"]["queue_items"],
                "review_remediation": focus["review_remediation_summary"]["queue_items"],
                "source_remediation": focus["source_remediation_summary"]["queue_items"],
                "source_revisit": focus["source_revisit_workbench_summary"]["queue_items"],
                "route_normalization": focus["route_normalization_summary"]["queue_items"],
            },
            "top_theses": [_compact_integration_thesis(item) for item in focus["active_theses"][:safe_limit]],
            "priority_targets": [_compact_integration_target(item) for item in focus["priority_actions"][:safe_limit]],
            "source_focus": [_compact_integration_source(item) for item in focus["source_focus"][:safe_limit]],
            "validated_updates": [
                _compact_integration_validation(item) for item in focus["validated_updates"][:safe_limit]
            ],
            "recent_artifacts": cockpit["recent_artifacts"][:safe_limit],
        }
    if scope_key == "weekly":
        weekly = build_weekly_decision_note(conn, days=days, limit=safe_limit)
        sections = weekly["sections"]
        return {
            "ok": True,
            "scope": "weekly",
            "generated_at": generated_at,
            "summary": weekly["summary"],
            "days": weekly["days"],
            "as_of": weekly["as_of"],
            "markdown": weekly["markdown"],
            "sections": {
                "priority_actions": [_compact_integration_target(item) for item in sections["priority_actions"]],
                "observe_actions": [_compact_integration_target(item) for item in sections["observe_actions"]],
                "source_focus": [_compact_integration_source(item) for item in sections["source_focus"]],
                "validation_updates": [
                    _compact_integration_validation(item) for item in sections["verification_queue"]
                ],
                "decision_journal": [
                    {
                        "decision_id": item.get("decision_id"),
                        "decision_date": item.get("decision_date"),
                        "action_state": item.get("action_state"),
                        "ticker_or_symbol": item.get("ticker_or_symbol"),
                        "thesis_title": item.get("thesis_title"),
                        "alignment": item.get("alignment"),
                        "status": item.get("status"),
                    }
                    for item in sections["decision_journal"]
                ],
            },
        }
    if scope_key != "thesis":
        return {"ok": False, "error": f"unsupported scope: {scope}", "scope": scope_key}

    focus = build_thesis_focus(conn, thesis_id, limit=max(safe_limit, 8))
    if not focus["summary"]["found"]:
        return {"ok": False, "scope": "thesis", "thesis_id": thesis_id, "error": focus.get("error", "thesis_not_found")}
    thesis = focus["thesis"]
    gate_report = build_thesis_gate_report(conn, thesis_id=thesis_id)
    gate_item = gate_report["items"][0] if gate_report["items"] else {}
    return {
        "ok": True,
        "scope": "thesis",
        "generated_at": generated_at,
        "summary": focus["summary"],
        "thesis_id": thesis_id,
        "thesis": {
            "thesis_id": thesis["thesis_id"],
            "title": thesis["title"],
            "status": thesis["status"],
            "horizon_months": thesis["horizon_months"],
            "owner": thesis.get("owner"),
            "statement": thesis["version"].get("statement"),
            "mechanism_chain": thesis["version"].get("mechanism_chain"),
            "why_now": thesis["version"].get("why_now"),
            "base_case": thesis["version"].get("base_case"),
            "counter_case": thesis["version"].get("counter_case"),
            "invalidators": thesis["version"].get("invalidators"),
            "promotion_gate": thesis["promotion_gate"],
            "gate_report": {
                "missing": gate_item.get("missing", []),
                "active_missing": gate_item.get("active_missing", []),
                "fatal_domain_warning_count": gate_item.get("fatal_domain_warning_count", 0),
                "incomplete_provenance_count": gate_item.get("incomplete_provenance_count", 0),
                "unresolved_conflict_count": gate_item.get("unresolved_conflict_count", 0),
            },
        },
        "themes": focus["themes"][:safe_limit],
        "linked_sources": [_compact_integration_source(item) for item in focus["linked_sources"][:safe_limit]],
        "provenance": focus["provenance"],
        "target_cases": [_compact_integration_target(item) for item in focus["target_cases"][:safe_limit]],
        "validations": [_compact_integration_validation(item) for item in focus["validations"][: max(safe_limit, 4)]],
        "reviews": [_compact_integration_review(item) for item in focus["reviews"][: max(safe_limit, 4)]],
        "patterns": [_compact_integration_pattern(item) for item in focus["matched_patterns"][:safe_limit]],
        "decisions": {
            "summary": focus["decisions"]["summary"],
            "active_entries": [
                {
                    "decision_id": item.get("decision_id"),
                    "decision_date": item.get("decision_date"),
                    "action_state": item.get("action_state"),
                    "target_case_id": item.get("target_case_id"),
                    "ticker_or_symbol": item.get("ticker_or_symbol"),
                    "alignment": item.get("alignment"),
                    "status": item.get("status"),
                    "review_freshness": item.get("review_freshness"),
                    "source_confidence": item.get("source_confidence"),
                }
                for item in focus["decisions"]["active_entries"][:safe_limit]
            ],
        },
        "source_viewpoints": [
            {
                "source_viewpoint_id": item.get("source_viewpoint_id"),
                "source_id": item.get("source_id"),
                "artifact_id": item.get("artifact_id"),
                "summary": item.get("summary"),
                "stance": item.get("stance"),
                "status": item.get("status"),
                "horizon_label": item.get("horizon_label"),
            }
            for item in focus["source_viewpoints"][:safe_limit]
        ],
        "graph_checks": _integration_graph_checks(conn, thesis_id=thesis_id),
    }


def build_thesis_gate_report(conn: Any, thesis_id: str = "") -> dict[str, Any]:
    board = build_thesis_board(conn)
    items = []
    for thesis in board["items"]:
        if thesis_id and thesis["thesis_id"] != thesis_id:
            continue
        gate = thesis["promotion_gate"]
        items.append(
            {
                "thesis_id": thesis["thesis_id"],
                "title": thesis["title"],
                "status": thesis["status"],
                "current_version_id": thesis["version"]["thesis_version_id"],
                "checks": gate["checks"],
                "missing": gate["missing"],
                "can_promote_to_evidence_backed": gate["can_promote_to_evidence_backed"],
                "active_checks": gate["active_checks"],
                "active_missing": gate["active_missing"],
                "can_recommend_active": gate["can_recommend_active"],
                "pending_corroboration_count": gate["pending_corroboration_count"],
                "accepted_corroboration_count": gate["accepted_corroboration_count"],
                "validated_corroboration_count": gate["validated_corroboration_count"],
                "corroboration_debt_ratio": gate["corroboration_debt_ratio"],
                "requires_corroborated_first_hand": gate["requires_corroborated_first_hand"],
                "first_hand_artifact_count": gate["first_hand_artifact_count"],
                "target_case_count": gate["target_case_count"],
                "fatal_domain_warning_count": gate["fatal_domain_warning_count"],
                "incomplete_provenance_count": gate["incomplete_provenance_count"],
                "unresolved_conflict_count": gate["unresolved_conflict_count"],
                "unresolved_conflicts": gate["unresolved_conflicts"],
            }
        )
    return {
        "summary": {
            "theses": len(items),
            "ready_to_promote": sum(1 for item in items if item["can_promote_to_evidence_backed"]),
        },
        "items": items,
    }
