from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from .db import list_rows, select_one
from .sector_grammars import get_sector_grammar
from .sentinel import STAGE_SEQUENCE
from .source_policy import get_source_policy


DEFAULT_QUALITY_POLICY = {
    "min_evidence_quality": 0.72,
    "max_constraint_burden": 0.45,
    "core_prepare_min_stage": "repeat_order",
    "alternative_prepare_min_stage": "repeat_order",
    "option_prepare_min_stage": "customer_validation",
}

TIME_STOP_DEFAULTS: dict[str, dict[str, Any]] = {
    "catalyst": {"review_after_days": 30, "suspend_after_days": 60, "archive_after_days": 120},
    "commercialization": {"review_after_days": 90, "suspend_after_days": 180, "archive_after_days": 365},
    "infrastructure": {"stage_checkpoint": True, "max_stage_dwell_days": 120},
}

DILIGENCE_BUDGET_DEFAULTS = {
    "max_active_themes": 5,
    "max_expressions_per_theme": 4,
    "max_candidates_per_theme": 3,
    "weekly_deep_review_capacity": 6,
}

EVIDENCE_QUALITY_METHOD = "heuristic_band_v1"
EVIDENCE_QUALITY_BANDS = (
    ("strong", 0.78, 0.85),
    ("moderate", 0.62, 0.68),
    ("weak", 0.0, 0.48),
)

SOURCE_TIER_WEIGHT = {
    "primary": 1.0,
    "secondary": 0.65,
    "tertiary": 0.35,
}

CONFIDENCE_WEIGHT = {
    "high": 1.0,
    "medium": 0.78,
    "low": 0.55,
}

READINESS_ORDER = {
    "prepare_candidate": 0,
    "review_required": 1,
    "watch_only": 2,
    "watch_only_option": 3,
    "benchmark": 4,
    "competitor_watch": 5,
    "archive_pending_confirmation": 6,
    "suspended": 7,
}


def _json_loads(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def _stage_rank(stage: str | None) -> int:
    if not stage:
        return -1
    try:
        return STAGE_SEQUENCE.index(stage)
    except ValueError:
        return -1


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _resolve_time_stop_policy(frame: dict[str, Any]) -> dict[str, Any]:
    policy = dict(frame.get("time_stop_policy") or {})
    thesis_type = str(policy.get("thesis_type") or "commercialization").strip().lower()
    resolved = dict(TIME_STOP_DEFAULTS.get(thesis_type, TIME_STOP_DEFAULTS["commercialization"]))
    resolved.update(policy)
    resolved["thesis_type"] = thesis_type
    return resolved


def _resolve_diligence_budget(spec: dict[str, Any]) -> dict[str, Any]:
    budget = dict(DILIGENCE_BUDGET_DEFAULTS)
    budget.update(dict(spec.get("diligence_budget") or {}))
    return budget


def _impact_is_negative(impact: str | None) -> bool:
    lowered = str(impact or "").lower()
    return "negative" in lowered or "downside" in lowered or "risk" in lowered


def _trigger_is_constraint(trigger: str | None) -> bool:
    return str(trigger or "").startswith("V")


def _trigger_is_thesis_level(trigger: str | None) -> bool:
    return str(trigger or "") in {"F1", "F3", "M1"}


def _trigger_is_timing_level(trigger: str | None) -> bool:
    return str(trigger or "") in {"F2", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8"}


def _projection_event_rows(conn: Any, projection_id: str, *, limit: int = 12) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT event_id, event_type, impact, confidence, mapped_trigger, route, route_reason,
                   source_role, source_tier, evidence_text, evidence_url, evidence_date,
                   event_time, residual_class, state_applied, candidate_id,
                   root_claim_key, independence_group
            FROM event_mining_events
            WHERE projection_id = ?
            ORDER BY event_time DESC, processed_time DESC
            LIMIT ?
            """,
            (projection_id, limit),
        )
    ]


def _candidate_rows_for_projection_ids(conn: Any, projection_ids: list[str]) -> list[dict[str, Any]]:
    if not projection_ids:
        return []
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT candidate_id, thesis_name, status, route, residual_class,
                   adjacent_projection_ids_json, cluster_score, persistence_score,
                   corroboration_score, investability_score, raw_event_count,
                   independence_group_count, attention_capture_ratio, anti_thesis_status,
                   next_proving_milestone, note, latest_event_time
            FROM opportunity_candidates
            ORDER BY updated_at DESC, cluster_score DESC, investability_score DESC
            """,
            (),
        )
    ]
    filtered: list[dict[str, Any]] = []
    anchors = set(projection_ids)
    for row in rows:
        linked = set(_json_loads(row.get("adjacent_projection_ids_json"), []))
        if linked & anchors:
            filtered.append(row)
    return filtered


def _anti_thesis_row(conn: Any, object_type: str, object_id: str) -> dict[str, Any] | None:
    row = select_one(
        conn,
        """
        SELECT check_id, status, due_reason, result_summary, contradiction_score, updated_at
        FROM anti_thesis_checks
        WHERE object_type = ? AND object_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (object_type, object_id),
    )
    return dict(row) if row is not None else None


def _feedback_rows(conn: Any, object_type: str, object_id: str, *, limit: int = 6) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT feedback_type, verdict, score, note, created_at
            FROM event_mining_feedback
            WHERE object_type = ? AND object_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (object_type, object_id, limit),
        )
    ]


def _evidence_quality(row: dict[str, Any]) -> tuple[float, str]:
    base = SOURCE_TIER_WEIGHT.get(str(row.get("last_source_tier") or ""), 0.35)
    confidence = CONFIDENCE_WEIGHT.get(str(row.get("current_confidence") or ""), 0.55)
    corroboration = min(1.3, 1.0 + 0.12 * max(int(row.get("independence_group_count") or 0) - 1, 0))
    attention_ratio = float(row.get("attention_capture_ratio") or 0.0)
    attention_penalty = 1.0 / max(1.0, attention_ratio)
    anti_penalty = 0.85 if int(row.get("pending_anti_thesis_count") or 0) > 0 else 1.0
    raw_score = min(1.0, base * confidence * corroboration * attention_penalty * anti_penalty)
    for band, threshold, band_score in EVIDENCE_QUALITY_BANDS:
        if raw_score >= threshold:
            return round(band_score, 2), band
    return 0.48, "weak"


def _event_decay_weight(event: dict[str, Any], *, as_of_date: date | None) -> float:
    event_date = _parse_date(event.get("event_time")) or _parse_date(event.get("evidence_date"))
    if as_of_date is None or event_date is None:
        return 1.0
    age_days = max((as_of_date - event_date).days, 0)
    if age_days <= 45:
        return 1.0
    if age_days <= 120:
        return 0.72
    if age_days <= 240:
        return 0.48
    return 0.32


def _event_constraint_weight(event: dict[str, Any], *, as_of_date: date | None) -> float:
    if _trigger_is_thesis_level(event.get("mapped_trigger")) and _impact_is_negative(event.get("impact")):
        base = 0.25
    elif _trigger_is_constraint(event.get("mapped_trigger")):
        base = 0.22
    elif event.get("event_type") == "stalled":
        base = 0.18
    elif _impact_is_negative(event.get("impact")):
        base = 0.08
    else:
        base = 0.0
    if base == 0.0:
        return 0.0
    return round(base * _event_decay_weight(event, as_of_date=as_of_date), 4)


def _constraint_burden(row: dict[str, Any], events: list[dict[str, Any]], *, as_of: str) -> float:
    burden = 0.0
    if row.get("stall_status") == "overdue":
        burden += 0.25
    if int(row.get("pending_anti_thesis_count") or 0) > 0:
        burden += 0.10
    if float(row.get("attention_capture_ratio") or 0.0) > 1.2:
        burden += 0.05
    as_of_date = _parse_date(as_of)
    weighted_groups: dict[str, float] = {}
    for event in events:
        event_weight = _event_constraint_weight(event, as_of_date=as_of_date)
        if event_weight <= 0.0:
            continue
        group_key = str(
            event.get("independence_group")
            or event.get("root_claim_key")
            or event.get("event_id")
            or "ungrouped"
        )
        weighted_groups[group_key] = max(weighted_groups.get(group_key, 0.0), event_weight)
    burden += sum(weighted_groups.values())
    return round(min(1.0, burden), 2)


def _recent_signal_buckets(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    positive: list[dict[str, Any]] = []
    risk: list[dict[str, Any]] = []
    for event in events:
        if _impact_is_negative(event.get("impact")) or event.get("event_type") == "stalled":
            risk.append(event)
        else:
            positive.append(event)
    return positive[:3], risk[:3]


def _time_stop_adjustment(
    expressions: list[dict[str, Any]],
    *,
    as_of_date: date | None,
    time_stop_policy: dict[str, Any],
) -> None:
    """Mutate expressions in-place, adding time_stop_action/reason fields.

    Archive action sets ``archive_pending_confirmation`` (needs human approval).
    """
    is_stage_checkpoint = bool(time_stop_policy.get("stage_checkpoint"))
    for expr in expressions:
        if expr.get("entity_role") != "tracked":
            continue
        expr["time_stop_action"] = None
        expr["time_stop_reason"] = None

        if is_stage_checkpoint:
            dwell = int(expr.get("stage_dwell_days") or 0)
            max_dwell = int(time_stop_policy.get("max_stage_dwell_days", 120))
            if dwell > max_dwell:
                expr["time_stop_action"] = "review_required"
                expr["time_stop_reason"] = (
                    f"阶段 {expr.get('current_stage') or '?'} 已停留 {dwell}d，"
                    f"超过阈值 {max_dwell}d"
                )
        else:
            days_left = expr.get("milestone_days_left")
            if days_left is None or days_left >= 0:
                continue
            days_overdue = abs(days_left)
            archive_threshold = int(time_stop_policy.get("archive_after_days", 180))
            suspend_threshold = int(time_stop_policy.get("suspend_after_days", 90))
            review_threshold = int(time_stop_policy.get("review_after_days", 30))
            stage_label = expr.get("expected_next_stage") or "next milestone"
            if days_overdue >= archive_threshold:
                expr["time_stop_action"] = "archive_pending_confirmation"
                expr["time_stop_reason"] = (
                    f"超期 {days_overdue}d 未达到 {stage_label}，建议归档（需人工确认）"
                )
            elif days_overdue >= suspend_threshold:
                expr["time_stop_action"] = "suspended"
                expr["time_stop_reason"] = f"超期 {days_overdue}d 未达到 {stage_label}，自动暂停"
            elif days_overdue >= review_threshold:
                expr["time_stop_action"] = "review_required"
                expr["time_stop_reason"] = f"超期 {days_overdue}d 未达到 {stage_label}，需复核"


def _slot_justification(
    expression: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    """Why does this expression still deserve a watch slot?"""
    why_still: str | None = None
    cost_flag = False
    competing: list[dict[str, Any]] = []

    eq = float(expression.get("evidence_quality") or 0)
    eq_band = str(expression.get("evidence_quality_band") or "weak")
    days_left = expression.get("milestone_days_left")

    # Auto-generate justification
    if eq_band == "strong":
        why_still = "证据质量 strong，继续跟踪有价值。"
    elif days_left is not None and days_left > 0 and days_left <= 90:
        why_still = f"里程碑 {days_left}d 内到期，接近验证窗口。"
    elif eq_band == "moderate":
        why_still = "证据质量 moderate，需要进一步佐证。"

    # Check opportunity cost: any candidate outscores this expression?
    for cand in candidates:
        cand_score = float(cand.get("investability_score") or 0)
        if cand_score > eq:
            cost_flag = True
            competing.append({
                "candidate_id": cand.get("candidate_id"),
                "thesis_name": cand.get("thesis_name"),
                "investability_score": cand_score,
                "reason_blocked": "diligence budget 已满" if cand.get("status") != "promoted" else "未晋升",
            })

    # Force justification for weak quality or overdue
    needs_justification = eq_band == "weak" or (days_left is not None and days_left < -30)
    if needs_justification and why_still is None:
        why_still = "⚠ 证据质量 weak 或里程碑已超期，需要说明继续关注的理由。"

    return {
        "why_still_watching": why_still,
        "opportunity_cost_flag": cost_flag,
        "competing_candidates": competing[:3],
        "needs_justification": needs_justification,
    }


def _diligence_budget_warnings(
    expression_count: int,
    candidate_count: int,
    budget: dict[str, Any],
) -> list[str]:
    """Check counts against diligence budget limits."""
    warnings: list[str] = []
    max_expr = int(budget.get("max_expressions_per_theme", 4))
    max_cand = int(budget.get("max_candidates_per_theme", 3))
    if expression_count > max_expr:
        warnings.append(
            f"⚠ 当前 {expression_count} 个表达超出 diligence 预算上限 {max_expr}，"
            f"请降级或归档 {expression_count - max_expr} 个。"
        )
    if candidate_count > max_cand:
        warnings.append(
            f"⚠ 当前 {candidate_count} 个候选超出预算上限 {max_cand}，"
            f"请促进或淘汰 {candidate_count - max_cand} 个。"
        )
    return warnings


def _recommended_action(
    row: dict[str, Any],
    *,
    evidence_quality: float,
    constraint_burden: float,
    quality_policy: dict[str, Any],
    has_thesis_falsifier: bool,
    time_stop_action: str | None = None,
) -> str:
    if row.get("entity_role") != "tracked":
        return "competitor_watch" if row.get("entity_role") == "competitor" else "benchmark"
    if has_thesis_falsifier:
        return "suspended"
    if time_stop_action:
        return time_stop_action
    
    bucket = row.get("bucket_role")
    stage_rank = _stage_rank(str(row.get("current_stage") or ""))
    if bucket == "option":
        min_stage = _stage_rank(str(quality_policy.get("option_prepare_min_stage")))
        if stage_rank >= min_stage and evidence_quality >= quality_policy["min_evidence_quality"] and constraint_burden <= 0.35:
            return "prepare_candidate"
        return "watch_only_option"
    if bucket in {"core", "alternative"}:
        key = "core_prepare_min_stage" if bucket == "core" else "alternative_prepare_min_stage"
        min_stage = _stage_rank(str(quality_policy.get(key)))
        if stage_rank >= min_stage and evidence_quality >= quality_policy["min_evidence_quality"] and constraint_burden <= quality_policy["max_constraint_burden"]:
            return "prepare_candidate"
        return "watch_only"
    return "benchmark"


def _min_prepare_stage(bucket_role: str | None, quality_policy: dict[str, Any]) -> str | None:
    bucket = str(bucket_role or "")
    if bucket == "option":
        return str(quality_policy.get("option_prepare_min_stage") or "")
    if bucket == "alternative":
        return str(quality_policy.get("alternative_prepare_min_stage") or "")
    if bucket == "core":
        return str(quality_policy.get("core_prepare_min_stage") or "")
    return None


def _expression_card(
    conn: Any,
    spec_entry: dict[str, Any],
    row: dict[str, Any],
    quality_policy: dict[str, Any],
    *,
    as_of: str,
    time_stop_action: str | None = None,
    time_stop_reason: str | None = None,
) -> dict[str, Any]:
    events = _projection_event_rows(conn, str(row["projection_id"]), limit=50)
    anti_row = _anti_thesis_row(conn, "projection", str(row["projection_id"]))
    feedback = _feedback_rows(conn, "projection", str(row["projection_id"]))
    evidence_quality, evidence_quality_band = _evidence_quality(row)
    positive_events, risk_events = _recent_signal_buckets(events)
    has_thesis_falsifier = any(
        _trigger_is_thesis_level(event.get("mapped_trigger")) and _impact_is_negative(event.get("impact"))
        for event in events
    )
    constraint_burden = _constraint_burden(row, events, as_of=as_of)
    expected_by = _parse_date(row.get("expected_by"))
    as_of_date = _parse_date(as_of)
    milestone_days_left = None
    if expected_by is not None and as_of_date is not None:
        milestone_days_left = (expected_by - as_of_date).days
    # Compute stage dwell days from stage_entered_at
    stage_entered = _parse_date(row.get("stage_entered_at") or spec_entry.get("stage_entered_at"))
    stage_dwell_days = (as_of_date - stage_entered).days if as_of_date and stage_entered else 0
    recommended_action = _recommended_action(
        row,
        evidence_quality=evidence_quality,
        constraint_burden=constraint_burden,
        quality_policy=quality_policy,
        has_thesis_falsifier=has_thesis_falsifier,
        time_stop_action=time_stop_action,
    )
    notes = _json_loads(row.get("notes_json"), {})
    grammar = get_sector_grammar(str(row.get("grammar_key") or spec_entry.get("grammar_key") or ""))
    source_policy = get_source_policy(str(row.get("source_role") or spec_entry.get("source_role") or ""))
    return {
        "projection_id": row["projection_id"],
        "entity": row["entity"],
        "product": row.get("product"),
        "bucket_role": row.get("bucket_role"),
        "entity_role": row.get("entity_role"),
        "grammar_key": row.get("grammar_key") or spec_entry.get("grammar_key"),
        "grammar_label": grammar.label if grammar else None,
        "grammar_description": grammar.description if grammar else None,
        "current_stage": row.get("current_stage"),
        "expected_next_stage": row.get("expected_next_stage"),
        "expected_by": row.get("expected_by"),
        "milestone_days_left": milestone_days_left,
        "stage_dwell_days": stage_dwell_days,
        "current_confidence": row.get("current_confidence"),
        "last_source_tier": row.get("last_source_tier"),
        "source_policy": {
            "source_role": source_policy.source_role,
            "source_tier": source_policy.source_tier,
            "adapter_family": source_policy.adapter_family,
            "state_authority": source_policy.state_authority,
            "interrupt_eligible": source_policy.interrupt_eligible,
            "description": source_policy.description,
        },
        "attention_capture_ratio": float(row.get("attention_capture_ratio") or 0.0),
        "evidence_quality": evidence_quality,
        "evidence_quality_band": evidence_quality_band,
        "evidence_quality_method": EVIDENCE_QUALITY_METHOD,
        "constraint_burden": constraint_burden,
        "recommended_action": recommended_action,
        "time_stop_action": time_stop_action,
        "time_stop_reason": time_stop_reason,
        "current_action": notes.get("current_action") or spec_entry.get("notes", {}).get("current_action"),
        "expression_role": notes.get("expression_role") or spec_entry.get("notes", {}).get("expression_role"),
        "why_not_now": notes.get("why_not_now") or spec_entry.get("notes", {}).get("why_not_now"),
        "upgrade_requirements": notes.get("upgrade_requirements") or spec_entry.get("notes", {}).get("upgrade_requirements") or [],
        "milestone_economics": notes.get("milestone_economics") or spec_entry.get("notes", {}).get("milestone_economics") or {},
        "positive_events": positive_events,
        "risk_events": risk_events,
        "anti_thesis": anti_row,
        "feedback": feedback,
        "thesis_falsifier_active": has_thesis_falsifier,
        "stall_status": row.get("stall_status"),
        "pending_anti_thesis_count": int(row.get("pending_anti_thesis_count") or 0),
        "notes": notes,
    }


def _theme_quality_gaps(
    expressions: list[dict[str, Any]],
    frame: dict[str, Any],
    candidates: list[dict[str, Any]],
    diligence_budget_status: dict[str, Any],
) -> list[str]:
    gaps: list[str] = []
    if not frame.get("why_mispriced"):
        gaps.append("theme 缺少 why_mispriced 明确陈述，仍然更像状态跟踪而非资本判断。")
    if any(item["bucket_role"] == "option" and item["recommended_action"] == "watch_only_option" for item in expressions):
        gaps.append("option 表达仍未跨过 prepare 门槛，不能把远期期权误当当前核心 thesis。")
    if any(item["constraint_burden"] >= 0.4 for item in expressions if item["entity_role"] == "tracked"):
        gaps.append("至少一个 tracked expression 的 constraint burden 偏高，需要先解决现金/兑现/时程问题。")
    if any(item["evidence_quality"] < 0.65 for item in expressions if item["entity_role"] == "tracked"):
        gaps.append("至少一个 tracked expression 的 evidence quality 仍低，primary/secondary 佐证不足。")
    if candidates and all(item.get("anti_thesis_status") == "due" for item in candidates):
        gaps.append("adjacent candidate 仍停留在反证待办阶段，不能提前升级成正式 watch。")
    if diligence_budget_status.get("over_budget"):
        gaps.extend(diligence_budget_status.get("warnings") or [])
    return gaps


def _theme_posture(expressions: list[dict[str, Any]]) -> str:
    tracked = [item for item in expressions if item["entity_role"] == "tracked"]
    if any(item["recommended_action"] == "suspended" for item in tracked):
        return "suspended"
    if any(item["recommended_action"] in {"review_required", "archive_pending_confirmation"} for item in tracked):
        return "review_required"
    if any(item["recommended_action"] == "prepare_candidate" for item in tracked):
        return "watch_with_prepare_candidate"
    return "watch_only"


def _build_decision_card(
    expressions: list[dict[str, Any]],
    *,
    posture: str,
    frame: dict[str, Any],
    next_gates: list[str],
    quality_policy: dict[str, Any],
) -> dict[str, Any]:
    tracked = [item for item in expressions if item["entity_role"] == "tracked"]
    best = tracked[0] if tracked else None
    if best is None:
        return {
            "decision_now": "no_actionable_expression",
            "best_expression_projection_id": None,
            "best_expression_reason": "当前没有可判断的 tracked expression。",
            "why_not_investable_yet": ["缺少可投资的 tracked expression。"],
            "forcing_events": list(next_gates[:4]),
            "slot_justification": {
                "why_still_watching": None,
                "human_justification_required": False,
                "opportunity_cost_flag": False,
                "competing_candidates": [],
            },
        }

    best_reason_parts = [
        f"{best['entity']} / {best.get('product') or 'n/a'} 当前位于 {best.get('current_stage') or 'n/a'}",
        f"证据质量 {best['evidence_quality_band']} ({best['evidence_quality_method']})",
        f"约束负担 {best['constraint_burden']:.2f}",
    ]
    if best.get("expression_role"):
        best_reason_parts.append(str(best["expression_role"]))

    why_not_now: list[str] = []
    for item in tracked:
        min_stage = _min_prepare_stage(item.get("bucket_role"), quality_policy)
        min_stage_rank = _stage_rank(min_stage)
        current_stage_rank = _stage_rank(str(item.get("current_stage") or ""))
        if item.get("thesis_falsifier_active"):
            msg = f"{item['entity']} 已命中 thesis-level falsifier，当前应先暂停而不是推进资本动作。"
            if msg not in why_not_now:
                why_not_now.append(msg)
        if min_stage_rank >= 0 and current_stage_rank < min_stage_rank:
            msg = f"{item['entity']} 当前阶段仍低于 `{min_stage}` 的 prepare 门槛。"
            if msg not in why_not_now:
                why_not_now.append(msg)
        if item["recommended_action"] == "watch_only_option":
            msg = "option 仍未跨过可资本化门槛，不能把远期期权当作当前主表达。"
            if msg not in why_not_now:
                why_not_now.append(msg)
        if item["constraint_burden"] >= 0.4:
            msg = f"{item['entity']} 仍有较高 constraint burden，需要先解决兑现/现金/时程问题。"
            if msg not in why_not_now:
                why_not_now.append(msg)
        if item["evidence_quality_band"] != "strong":
            msg = f"{item['entity']} 的证据带宽仍是 {item['evidence_quality_band']}，当前更适合 watch 而非 prepare。"
            if msg not in why_not_now:
                why_not_now.append(msg)
        if item.get("pending_anti_thesis_count", 0) > 0:
            msg = f"{item['entity']} 仍有待处理的 anti-thesis 检查，不能在反证未收口前升级。"
            if msg not in why_not_now:
                why_not_now.append(msg)
        if item.get("stall_status") == "overdue":
            msg = f"{item['entity']} 的里程碑已 overdue，当前需要先验证兑现时程。"
            if msg not in why_not_now:
                why_not_now.append(msg)
        if item.get("time_stop_reason"):
            msg = f"{item['entity']} 命中 time stop：{item['time_stop_reason']}。"
            if msg not in why_not_now:
                why_not_now.append(msg)
        reason = item.get("why_not_now")
        if reason and f"{item['entity']} spec context: {reason}" not in why_not_now:
            why_not_now.append(f"{item['entity']} spec context: {reason}")
    if not why_not_now and posture != "suspended":
        why_not_now.append("当前主题仍以 watch 为主，尚未满足把注意力升级为资本动作的门槛。")

    if any(item["recommended_action"] == "archive_pending_confirmation" for item in tracked):
        decision_now = "human_confirm_archive"
    elif posture == "suspended":
        decision_now = "suspend_theme"
    elif posture == "review_required":
        decision_now = "review_required"
    elif best["recommended_action"] == "prepare_candidate":
        decision_now = "prepare_best_expression"
    else:
        decision_now = "stay_watch_only"

    forcing_events = list(next_gates[:5])
    forcing_events.extend(str(item) for item in (frame.get("thesis_level_falsifiers") or []) if item not in forcing_events)

    return {
        "decision_now": decision_now,
        "best_expression_projection_id": best["projection_id"],
        "best_expression_reason": "；".join(best_reason_parts),
        "why_not_investable_yet": why_not_now,
        "forcing_events": forcing_events[:6],
    }


def _expression_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        READINESS_ORDER.get(item["recommended_action"], 99),
        -float(item["evidence_quality"]),
        -_stage_rank(str(item.get("current_stage") or "")),
        float(item["constraint_burden"]),
        str(item["entity"]),
    )


def build_theme_investment_report(
    conn: Any,
    spec: dict[str, Any],
    *,
    theme_slug: str,
    as_of: str,
) -> dict[str, Any]:
    frame = dict(spec.get("theme") or {})
    quality_policy = {**DEFAULT_QUALITY_POLICY, **dict(spec.get("quality_policy") or {})}
    # --- Time stop policy: spec-defined, fallback to thesis-type defaults ---
    raw_ts_policy = frame.get("time_stop_policy") or {}
    thesis_type = str(raw_ts_policy.get("thesis_type") or "commercialization")
    time_stop_policy = {**TIME_STOP_DEFAULTS.get(thesis_type, TIME_STOP_DEFAULTS["commercialization"]), **raw_ts_policy}
    # --- Diligence budget ---
    diligence_budget = {**DILIGENCE_BUDGET_DEFAULTS, **dict(spec.get("diligence_budget") or {})}

    spec_entries = list(spec.get("sentinel") or [])
    projection_ids = [str(entry.get("sentinel_id")) for entry in spec_entries if entry.get("sentinel_id")]
    rows = {
        str(row["projection_id"]): dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT *
            FROM event_state_projections
            WHERE projection_id IN ({", ".join("?" for _ in projection_ids)})
            """,
            tuple(projection_ids),
        )
    } if projection_ids else {}

    # --- Phase 1: build raw cards with milestone info ---
    raw_cards: list[dict[str, Any]] = []
    for entry in spec_entries:
        projection_id = str(entry.get("sentinel_id"))
        row = rows.get(projection_id)
        if row is None:
            continue
        raw_cards.append(_expression_card(conn, entry, row, quality_policy, as_of=as_of))

    # --- Phase 2: apply time stop adjustment ---
    as_of_date = _parse_date(as_of)
    _time_stop_adjustment(raw_cards, as_of_date=as_of_date, time_stop_policy=time_stop_policy)

    # --- Phase 3: rebuild recommended_action with time stop overrides ---
    for card in raw_cards:
        if card.get("time_stop_action"):
            card["recommended_action"] = _recommended_action(
                card,
                evidence_quality=card["evidence_quality"],
                constraint_burden=card["constraint_burden"],
                quality_policy=quality_policy,
                has_thesis_falsifier=card.get("thesis_falsifier_active", False),
                time_stop_action=card["time_stop_action"],
            )

    expression_cards = sorted(raw_cards, key=_expression_sort_key)
    candidates = _candidate_rows_for_projection_ids(conn, projection_ids)

    # --- Phase 4: slot justification ---
    for card in expression_cards:
        if card.get("entity_role") == "tracked":
            card["slot_justification"] = _slot_justification(card, candidates)
        else:
            card["slot_justification"] = None

    # --- Phase 5: diligence budget warnings ---
    tracked_count = sum(1 for c in expression_cards if c.get("entity_role") == "tracked")
    budget_warnings = _diligence_budget_warnings(tracked_count, len(candidates), diligence_budget)
    diligence_budget_status = {
        "over_budget": len(budget_warnings) > 0,
        "tracked_expression_count": tracked_count,
        "candidate_count": len(candidates),
        "warnings": budget_warnings,
    }

    # --- Phase 6: time stop summary ---
    time_stop_triggered = [
        {"projection_id": c["projection_id"], "entity": c["entity"],
         "action": c["time_stop_action"], "reason": c["time_stop_reason"]}
        for c in expression_cards if c.get("time_stop_action")
    ]

    posture = _theme_posture(expression_cards)
    best_expression = next(
        (item for item in expression_cards if item["entity_role"] == "tracked"),
        expression_cards[0] if expression_cards else None,
    )
    next_gates: list[str] = []
    for item in expression_cards:
        for gate in item.get("upgrade_requirements") or []:
            if gate not in next_gates:
                next_gates.append(str(gate))
    for gate in frame.get("capital_gate") or []:
        if gate not in next_gates:
            next_gates.append(str(gate))
    quality_gaps = _theme_quality_gaps(expression_cards, frame, candidates, diligence_budget_status)
    decision_card = _build_decision_card(
        expression_cards,
        posture=posture,
        frame=frame,
        next_gates=next_gates,
        quality_policy=quality_policy,
    )

    return {
        "theme": {
            "theme_slug": theme_slug,
            "title": frame.get("title") or theme_slug,
            "investor_question": frame.get("investor_question"),
            "thesis_statement": frame.get("thesis_statement"),
            "why_now": frame.get("why_now"),
            "why_mispriced": frame.get("why_mispriced"),
            "current_posture": frame.get("current_posture") or posture,
            "capital_gate": frame.get("capital_gate") or [],
            "stop_rule": frame.get("stop_rule") or [],
            "time_stop_policy": time_stop_policy,
            "diligence_budget": diligence_budget,
            "thesis_level_falsifiers": frame.get("thesis_level_falsifiers") or [],
            "timing_level_falsifiers": frame.get("timing_level_falsifiers") or [],
            "as_of": as_of,
            "time_stop_policy": time_stop_policy,
            "diligence_budget": diligence_budget,
        },
        "summary": {
            "recommended_posture": posture,
            "tracked_expression_count": tracked_count,
            "prepare_candidates": [
                item["projection_id"] for item in expression_cards if item["recommended_action"] == "prepare_candidate"
            ],
            "watch_only_expressions": [
                item["projection_id"] for item in expression_cards if item["recommended_action"] in {"watch_only", "watch_only_option"}
            ],
            "best_expression": {
                "projection_id": best_expression["projection_id"],
                "entity": best_expression["entity"],
                "product": best_expression["product"],
                "recommended_action": best_expression["recommended_action"],
                "evidence_quality": best_expression["evidence_quality"],
                "evidence_quality_band": best_expression["evidence_quality_band"],
                "constraint_burden": best_expression["constraint_burden"],
            }
            if best_expression
            else None,
            "quality_gap_count": len(quality_gaps),
            "candidate_count": len(candidates),
            "diligence_budget_status": diligence_budget_status,
        },
        "decision_card": decision_card,
        "expressions": expression_cards,
        "candidates": candidates,
        "quality_gaps": quality_gaps,
        "next_diligence_gates": next_gates,
        "diligence_budget_warnings": budget_warnings,
        "time_stop_summary": time_stop_triggered,
    }


def render_theme_investment_report(report: dict[str, Any]) -> str:
    theme = report["theme"]
    summary = report["summary"]
    lines = [
        f"# {theme['title']} — Theme Investment Report",
        "",
        f"> theme_slug: `{theme['theme_slug']}`",
        f"> as_of: `{theme['as_of']}`",
        f"> recommended_posture: `{summary['recommended_posture']}`",
        "",
        "## 1. Investor Frame",
        "",
        f"- Investor question: {theme.get('investor_question') or '未定义'}",
        f"- Thesis statement: {theme.get('thesis_statement') or '未定义'}",
        f"- Why now: {theme.get('why_now') or '未定义'}",
        f"- Why mispriced / why not yet: {theme.get('why_mispriced') or '未定义'}",
        f"- Time stop policy: `{json.dumps(theme.get('time_stop_policy') or {}, ensure_ascii=False, sort_keys=True)}`",
        f"- Diligence budget: `{json.dumps(theme.get('diligence_budget') or {}, ensure_ascii=False, sort_keys=True)}`",
        "",
        "## 2. Best Expression",
        "",
    ]
    best = summary.get("best_expression")
    if best:
        lines.extend(
            [
                f"- Projection: `{best['projection_id']}`",
                f"- Entity / product: {best['entity']} / {best.get('product') or 'n/a'}",
                f"- Action: `{best['recommended_action']}`",
                f"- Evidence quality: `{best['evidence_quality_band']}` (`{best['evidence_quality']}` / `{EVIDENCE_QUALITY_METHOD}`)",
                f"- Constraint burden: `{best['constraint_burden']}`",
            ]
        )
    else:
        lines.append("- 无 tracked expression")
    lines.extend(
        [
            "",
            "## 3. Decision Card",
            "",
            f"- Decision now: `{report['decision_card']['decision_now']}`",
            f"- Best expression reason: {report['decision_card']['best_expression_reason']}",
            "- Why not investable yet:",
        ]
    )
    for reason in report["decision_card"]["why_not_investable_yet"] or ["当前无额外阻碍。"]:
        lines.append(f"  - {reason}")
    lines.extend(
        [
            "- Forcing events:",
        ]
    )
    for event in report["decision_card"]["forcing_events"] or ["无强制事件。"]:
        lines.append(f"  - {event}")
    slot_justification = report["decision_card"].get("slot_justification") or {}
    lines.extend(
        [
            "- Slot justification:",
            f"  - why_still_watching: {slot_justification.get('why_still_watching') or '需要人工填写 / 当前为空'}",
            f"  - human_justification_required: `{bool(slot_justification.get('human_justification_required'))}`",
            f"  - opportunity_cost_flag: `{bool(slot_justification.get('opportunity_cost_flag'))}`",
        ]
    )
    for item in slot_justification.get("competing_candidates") or []:
        lines.append(
            f"    - competing `{item.get('candidate_id')}` / {item.get('thesis_name') or 'n/a'} / "
            f"investability={item.get('investability_score')} / blocked={item.get('reason_blocked') or 'n/a'}"
        )
    lines.extend(
        [
            "",
            "## 4. Expression Ranking",
            "",
            "| Projection | Role | Stage | Action | Evidence | Constraint |",
            "|---|---|---|---|---:|---:|",
        ]
    )
    for item in report["expressions"]:
        lines.append(
            f"| `{item['projection_id']}` | {item.get('bucket_role') or item.get('entity_role')} | "
            f"{item.get('current_stage') or 'n/a'} | `{item['recommended_action']}` | "
            f"{item['evidence_quality_band']}/{item['evidence_quality']:.2f} | {item['constraint_burden']:.2f} |"
        )
    lines.append("")
    for item in report["expressions"]:
        source_policy = item.get("source_policy") or {}
        lines.append(
            f"- `{item['projection_id']}` grammar=`{item.get('grammar_label') or item.get('grammar_key') or '-'}`; "
            f"source=`{source_policy.get('source_role') or '-'}:{source_policy.get('source_tier') or '-'}`; "
            f"authority=`{source_policy.get('state_authority') or '-'}`"
        )
    lines.extend(["", "## 5. Quality Gaps", ""])
    if report["quality_gaps"]:
        for gap in report["quality_gaps"]:
            lines.append(f"- {gap}")
    else:
        lines.append("- 无显著 quality gap。")
    lines.extend(["", "## 6. Diligence Budget", ""])
    budget_status = summary.get("diligence_budget_status") or {}
    lines.append(f"- over_budget: `{bool(budget_status.get('over_budget'))}`")
    lines.append(f"- tracked_expression_count: `{budget_status.get('tracked_expression_count')}`")
    lines.append(f"- candidate_count: `{budget_status.get('candidate_count')}`")
    for warning in budget_status.get("warnings") or ["无预算告警。"]:
        lines.append(f"- {warning}")
    lines.extend(["", "## 7. Next Diligence Gates", ""])
    for gate in report["next_diligence_gates"] or ["无新增 diligence gate"]:
        lines.append(f"- {gate}")
    lines.extend(["", "## 8. Candidates", ""])
    if report["candidates"]:
        for item in report["candidates"]:
            lines.append(
                f"- `{item['candidate_id']}` / {item['thesis_name']} / route={item['route']} / "
                f"anti_thesis={item['anti_thesis_status']} / investability={item['investability_score']}"
            )
    else:
        lines.append("- 无相关 candidate。")
    # --- §8 Time Stop ---
    time_stops = report.get("time_stop_summary") or []
    lines.extend(["", "## 8. Time Stop", ""])
    if time_stops:
        for ts in time_stops:
            lines.append(f"- `{ts['projection_id']}` ({ts['entity']}): **{ts['action']}** — {ts['reason']}")
    else:
        lines.append("- 无 time stop 触发。")
    # --- §9 Diligence Budget ---
    budget_warnings = report.get("diligence_budget_warnings") or []
    lines.extend(["", "## 9. Diligence Budget", ""])
    if budget_warnings:
        for w in budget_warnings:
            lines.append(f"- {w}")
    else:
        budget = report.get("theme", {}).get("diligence_budget") or {}
        lines.append(
            f"- ✅ 在预算范围内 (表达 {report['summary']['tracked_expression_count']}"
            f"/{budget.get('max_expressions_per_theme', '?')}, "
            f"候选 {report['summary']['candidate_count']}"
            f"/{budget.get('max_candidates_per_theme', '?')})。"
        )
    # --- §10 Slot Justification ---
    lines.extend(["", "## 10. Slot Justification", ""])
    has_justification = False
    for item in report["expressions"]:
        sj = item.get("slot_justification")
        if sj is None:
            continue
        has_justification = True
        flag = " 🔴 opportunity_cost" if sj.get("opportunity_cost_flag") else ""
        just = " ⚠ needs_justification" if sj.get("needs_justification") else ""
        lines.append(f"- `{item['projection_id']}` ({item['entity']}){flag}{just}")
        if sj.get("why_still_watching"):
            lines.append(f"  - why: {sj['why_still_watching']}")
        for comp in sj.get("competing_candidates") or []:
            lines.append(
                f"  - competing: `{comp['candidate_id']}` / {comp['thesis_name']} "
                f"(investability={comp['investability_score']:.2f})"
            )
    if not has_justification:
        lines.append("- 无 tracked expression 需要 slot justification。")
    return "\n".join(lines) + "\n"
