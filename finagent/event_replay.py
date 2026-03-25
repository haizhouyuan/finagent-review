from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from .db import connect, init_db
from .paths import ensure_runtime_dirs, resolve_paths
from .sentinel import emit_stalled_events, import_events, load_sentinel_spec, sync_sentinel_spec, validate_sentinel_spec
from .theme_report import build_theme_investment_report
from .views import (
    build_anti_thesis_board,
    build_event_evaluation_board,
    build_event_ledger,
    build_opportunity_inbox,
    build_sentinel_board,
    build_theme_radar_board,
    build_today_cockpit,
)


@dataclass(frozen=True)
class ReplayInputs:
    spec_path: Path
    events_path: Path
    as_of: str
    theme_slug: str


def _event_summary(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for item in items:
        summary.append(
            {
                "event_id": item.get("event_id"),
                "route": item.get("route"),
                "route_reason": item.get("route_reason"),
                "projection_id": item.get("projection_id"),
                "candidate_id": item.get("candidate_id"),
                "state_applied": item.get("state_applied"),
                "residual_class": item.get("residual_class"),
                "source_tier": item.get("source_tier"),
                "independence_group": item.get("independence_group"),
                "corroboration_count": item.get("corroboration_count"),
            }
        )
    return summary


def replay_theme_run(inputs: ReplayInputs) -> dict[str, Any]:
    with TemporaryDirectory(prefix="finagent_event_replay_") as raw_tmp:
        run_root = Path(raw_tmp)
        paths = resolve_paths(run_root)
        ensure_runtime_dirs(paths)
        conn = connect(paths.db_path)
        init_db(conn)

        spec = load_sentinel_spec(inputs.spec_path)
        errors = validate_sentinel_spec(spec)
        if errors:
            return {"ok": False, "errors": errors}

        sync_result = sync_sentinel_spec(conn, spec)
        events = json.loads(inputs.events_path.read_text(encoding="utf-8"))
        import_result = import_events(conn, events)
        stall_result = emit_stalled_events(conn, as_of=inputs.as_of)
        theme_report = build_theme_investment_report(conn, spec, theme_slug=inputs.theme_slug, as_of=inputs.as_of)

        return {
            "ok": True,
            "sync_result": sync_result,
            "import_result": import_result,
            "stall_result": stall_result,
            "event_ledger": build_event_ledger(conn, limit=50),
            "sentinel_board": build_sentinel_board(conn, limit=20),
            "opportunity_inbox": build_opportunity_inbox(conn, limit=20),
            "theme_radar_board": build_theme_radar_board(conn, limit=20),
            "anti_thesis_board": build_anti_thesis_board(conn, limit=20),
            "event_evaluation_board": build_event_evaluation_board(conn, limit=20),
            "today_cockpit": build_today_cockpit(conn),
            "theme_investment_report": theme_report,
            "route_events": _event_summary(import_result.get("results", [])),
        }


def validate_theme_replay(
    *,
    reference_import_result: dict[str, Any],
    reference_stall_result: dict[str, Any],
    replay_payload: dict[str, Any],
) -> dict[str, Any]:
    if not replay_payload.get("ok"):
        return {"ok": False, "error": "replay_failed", "payload": replay_payload}

    expected_events = _event_summary(reference_import_result.get("results", []))
    actual_events = replay_payload.get("route_events") or []
    by_event_id = {str(item.get("event_id") or ""): item for item in actual_events}
    mismatches: list[dict[str, Any]] = []
    for expected in expected_events:
        event_id = str(expected.get("event_id") or "")
        actual = by_event_id.get(event_id)
        if actual is None:
            mismatches.append({"event_id": event_id, "error": "missing_in_replay"})
            continue
        for key in (
            "route",
            "route_reason",
            "projection_id",
            "candidate_id",
            "state_applied",
            "residual_class",
            "source_tier",
            "independence_group",
            "corroboration_count",
        ):
            if actual.get(key) != expected.get(key):
                mismatches.append(
                    {
                        "event_id": event_id,
                        "field": key,
                        "expected": expected.get(key),
                        "actual": actual.get(key),
                    }
                )
    replay_stalled = int((replay_payload.get("stall_result") or {}).get("emitted") or 0)
    expected_stalled = int(reference_stall_result.get("emitted") or 0)
    if replay_stalled != expected_stalled:
        mismatches.append(
            {
                "field": "stall_result.emitted",
                "expected": expected_stalled,
                "actual": replay_stalled,
            }
        )
    return {
        "ok": len(mismatches) == 0,
        "expected_event_count": len(expected_events),
        "replay_event_count": len(actual_events),
        "expected_stalled": expected_stalled,
        "replay_stalled": replay_stalled,
        "mismatches": mismatches,
    }
