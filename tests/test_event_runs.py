from __future__ import annotations

from pathlib import Path

from finagent.db import connect, init_db
from finagent.event_runs import compare_event_mining_runs, list_event_mining_runs, record_event_mining_run


def test_record_and_list_event_mining_runs(fresh_db) -> None:
    result = record_event_mining_run(
        fresh_db,
        engine="event_mining.theme_suite",
        run_slug="transformer_v1",
        schema_version="3.0",
        input_refs={
            "kind": "theme_suite",
            "theme_slug": "transformer_v1",
            "report_dir": "/tmp/report",
        },
        output_ref="/tmp/report/summary.json",
        summary={
            "recommended_posture": "watch_with_prepare_candidate",
            "best_expression": "sntl_xidian_alt",
            "imported": 5,
            "stalled_emitted": 1,
        },
    )
    assert result["ok"] is True
    items = list_event_mining_runs(fresh_db, limit=10)
    assert len(items) == 1
    assert items[0]["engine"] == "event_mining.theme_suite"
    assert items[0]["summary"]["best_expression"] == "sntl_xidian_alt"


def test_compare_event_mining_runs_returns_summary_fields(fresh_db) -> None:
    first = record_event_mining_run(
        fresh_db,
        engine="event_mining.theme_suite",
        run_slug="transformer_v1",
        schema_version="3.0",
        input_refs={"kind": "theme_suite"},
        output_ref="/tmp/report_a/summary.json",
        summary={"recommended_posture": "watch_only", "best_expression": "sntl_jinpan_core", "imported": 3},
    )
    second = record_event_mining_run(
        fresh_db,
        engine="event_mining.kol_suite",
        run_slug="kol_v1",
        schema_version="3.0",
        input_refs={"kind": "kol_suite"},
        output_ref="/tmp/report_b/summary.json",
        summary={"recommended_posture": None, "best_expression": None, "imported": 5},
    )
    payload = compare_event_mining_runs(fresh_db, [first["analysis_run_id"], second["analysis_run_id"]])
    assert payload["ok"] is True
    assert len(payload["items"]) == 2
    assert {item["engine"] for item in payload["items"]} == {"event_mining.theme_suite", "event_mining.kol_suite"}


def test_record_event_mining_run_persists_across_connections(tmp_path: Path) -> None:
    db_path = tmp_path / "finagent.sqlite"
    conn = connect(db_path)
    init_db(conn)
    record_event_mining_run(
        conn,
        engine="event_mining.theme_suite",
        run_slug="persisted_v1",
        schema_version="3.0",
        input_refs={"kind": "theme_suite"},
        output_ref="/tmp/persisted/summary.json",
        summary={"recommended_posture": "watch_only", "best_expression": "sntl_demo"},
    )
    other = connect(db_path)
    init_db(other)
    items = list_event_mining_runs(other, limit=5)
    assert len(items) == 1
    assert items[0]["run_slug"] == "persisted_v1"
