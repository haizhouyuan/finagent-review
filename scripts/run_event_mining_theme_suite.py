from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finagent.db import connect, init_db
from finagent.event_replay import ReplayInputs, replay_theme_run, validate_theme_replay
from finagent.event_runs import record_event_mining_run
from finagent.paths import ensure_runtime_dirs, resolve_paths
from finagent.sentinel import (
    emit_stalled_events,
    import_events,
    load_sentinel_spec,
    sync_sentinel_spec,
    validate_sentinel_spec,
)
from finagent.theme_report import build_theme_investment_report, render_theme_investment_report
from finagent.views import (
    build_anti_thesis_board,
    build_event_evaluation_board,
    build_event_ledger,
    build_opportunity_inbox,
    build_sentinel_board,
    build_theme_radar_board,
    build_today_cockpit,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--spec", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--theme-slug", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_root = Path(args.run_root).resolve()
    spec_path = Path(args.spec).resolve()
    events_path = Path(args.events).resolve()

    paths = resolve_paths(run_root)
    ensure_runtime_dirs(paths)
    conn = connect(paths.db_path)
    init_db(conn)

    spec = load_sentinel_spec(spec_path)
    errors = validate_sentinel_spec(spec)
    if errors:
        raise SystemExit(f"invalid sentinel spec: {errors}")

    sync_result = sync_sentinel_spec(conn, spec)
    events = json.loads(events_path.read_text(encoding="utf-8"))
    import_result = import_events(conn, events)
    route_validation = {
        "theme_slug": args.theme_slug,
        "events": [
            {
                "event_id": item.get("event_id"),
                "ok": item.get("ok"),
                "route": item.get("route"),
                "route_reason": item.get("route_reason"),
                "projection_id": item.get("projection_id"),
                "candidate_id": item.get("candidate_id"),
                "state_applied": item.get("state_applied"),
                "residual_class": item.get("residual_class"),
                "independence_group": item.get("independence_group"),
                "corroboration_count": item.get("corroboration_count"),
            }
            for item in import_result.get("results", [])
        ],
    }
    stall_result = emit_stalled_events(conn, as_of=args.as_of)

    report_dir = paths.state_dir / "theme_run_reports" / args.theme_slug
    _write_json(report_dir / "route_validation.json", route_validation)
    _write_json(report_dir / "import_result.json", import_result)
    _write_json(report_dir / "stall_result.json", stall_result)
    _write_json(report_dir / "event_ledger.json", build_event_ledger(conn, limit=50))
    _write_json(report_dir / "sentinel_board.json", build_sentinel_board(conn, limit=20))
    _write_json(report_dir / "opportunity_inbox.json", build_opportunity_inbox(conn, limit=20))
    _write_json(report_dir / "theme_radar_board.json", build_theme_radar_board(conn, limit=20))
    _write_json(report_dir / "anti_thesis_board.json", build_anti_thesis_board(conn, limit=20))
    _write_json(report_dir / "event_evaluation_board.json", build_event_evaluation_board(conn, limit=20))
    _write_json(report_dir / "today_cockpit.json", build_today_cockpit(conn))
    theme_report = build_theme_investment_report(conn, spec, theme_slug=args.theme_slug, as_of=args.as_of)
    _write_json(report_dir / "theme_investment_report.json", theme_report)
    _write_json(
        report_dir / "expression_scorecard.json",
        {
            "theme_slug": args.theme_slug,
            "recommended_posture": theme_report["summary"]["recommended_posture"],
            "best_expression": theme_report["summary"]["best_expression"],
            "expressions": theme_report["expressions"],
        },
    )
    replay_payload = replay_theme_run(
        ReplayInputs(
            spec_path=spec_path,
            events_path=events_path,
            as_of=args.as_of,
            theme_slug=args.theme_slug,
        )
    )
    replay_validation = validate_theme_replay(
        reference_import_result=import_result,
        reference_stall_result=stall_result,
        replay_payload=replay_payload,
    )
    _write_json(report_dir / "replay_validation.json", replay_validation)
    (report_dir / "theme_investment_report.md").write_text(
        render_theme_investment_report(theme_report),
        encoding="utf-8",
    )

    summary = {
        "ok": True,
        "theme_slug": args.theme_slug,
        "run_root": str(run_root),
        "db_path": str(paths.db_path),
        "spec_path": str(spec_path),
        "events_path": str(events_path),
        "synced": sync_result.get("synced", 0),
        "imported": import_result.get("imported", 0),
        "stalled_emitted": stall_result.get("emitted", 0),
        "report_dir": str(report_dir),
        "recommended_posture": theme_report["summary"]["recommended_posture"],
        "best_expression": theme_report["summary"]["best_expression"],
        "replay_ok": replay_validation.get("ok"),
    }
    run_record = record_event_mining_run(
        conn,
        engine="event_mining.theme_suite",
        run_slug=args.theme_slug,
        schema_version=str(spec.get("schema_version") or "unknown"),
        input_refs={
            "theme_slug": args.theme_slug,
            "run_root": str(run_root),
            "spec_path": str(spec_path),
            "events_path": str(events_path),
            "as_of": args.as_of,
            "report_dir": str(report_dir),
            "kind": "theme_suite",
        },
        output_ref=str(report_dir / "summary.json"),
        summary=summary,
    )
    registry_paths = resolve_paths(REPO_ROOT)
    ensure_runtime_dirs(registry_paths)
    registry_conn = connect(registry_paths.db_path)
    init_db(registry_conn)
    record_event_mining_run(
        registry_conn,
        engine="event_mining.theme_suite",
        run_slug=args.theme_slug,
        schema_version=str(spec.get("schema_version") or "unknown"),
        input_refs={
            "theme_slug": args.theme_slug,
            "run_root": str(run_root),
            "spec_path": str(spec_path),
            "events_path": str(events_path),
            "as_of": args.as_of,
            "report_dir": str(report_dir),
            "kind": "theme_suite",
            "registry_scope": "repo_root",
        },
        output_ref=str(report_dir / "summary.json"),
        summary=summary,
    )
    summary["analysis_run_id"] = run_record["analysis_run_id"]
    _write_json(report_dir / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
