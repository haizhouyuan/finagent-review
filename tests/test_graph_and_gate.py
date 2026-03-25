from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from finagent.cli import _run_monitors_with_conn, cmd_review_claim
from finagent.db import connect, init_db, insert_claim, insert_row
from finagent.paths import ensure_runtime_dirs, resolve_paths
from finagent.graph import build_graph_from_db, detect_conflicts
from finagent.utils import json_dumps, make_id
from finagent.views import build_integration_snapshot, build_thesis_gate_report


def _build_sample_db(tmp_path: Path):
    db_path = tmp_path / "finagent.sqlite"
    conn = connect(db_path)
    init_db(conn)

    insert_row(
        conn,
        "sources",
        {
            "source_id": "src_official",
            "source_type": "official_disclosure",
            "name": "Official Filing",
            "primaryness": "first_hand",
            "jurisdiction": "US",
            "language": "en",
            "base_uri": "",
            "credibility_policy": "",
            "track_record_stats_json": json_dumps({}),
        },
    )
    insert_row(
        conn,
        "sources",
        {
            "source_id": "src_kol",
            "source_type": "kol",
            "name": "KOL",
            "primaryness": "second_hand",
            "jurisdiction": "CN",
            "language": "zh",
            "base_uri": "",
            "credibility_policy": "",
            "track_record_stats_json": json_dumps({}),
        },
    )
    insert_row(
        conn,
        "artifacts",
        {
            "artifact_id": "art_fh",
            "source_id": "src_official",
            "artifact_kind": "filing",
            "title": "Official HBM filing",
            "captured_at": "2026-03-01T00:00:00+00:00",
            "published_at": "2026-03-01T00:00:00+00:00",
            "language": "en",
            "uri": "",
            "raw_path": "",
            "normalized_text_path": "",
            "content_hash": "h1",
            "status": "extracted",
            "metadata_json": json_dumps({}),
        },
    )
    insert_row(
        conn,
        "artifacts",
        {
            "artifact_id": "art_sh",
            "source_id": "src_kol",
            "artifact_kind": "video_digest",
            "title": "KOL HBM take",
            "captured_at": "2026-03-02T00:00:00+00:00",
            "published_at": "2026-03-02T00:00:00+00:00",
            "language": "zh",
            "uri": "",
            "raw_path": "",
            "normalized_text_path": "",
            "content_hash": "h2",
            "status": "extracted",
            "metadata_json": json_dumps({}),
        },
    )
    insert_claim(
        conn,
        {
            "claim_id": "clm_positive",
            "artifact_id": "art_fh",
            "speaker": "official",
            "timecode_or_span": "0",
            "claim_text": "HBM 市场空间 100亿，但被写成 100 Billion，同时供给仍然短缺并持续扩产。",
            "claim_type": "fact",
            "confidence": 0.82,
            "linked_entity_ids_json": json_dumps([]),
            "data_date": "2026-03-01",
            "review_status": "unreviewed",
            "review_metadata_json": json_dumps({}),
            "domain_check_json": json_dumps(
                {
                    "passed": False,
                    "warnings": [
                        {
                            "code": "YI_BILLION_TRANSLATION",
                            "severity": "FATAL",
                            "message": "bad translation",
                        }
                    ],
                }
            ),
            "freshness_status": "fresh",
            "status": "candidate",
        },
    )
    insert_claim(
        conn,
        {
            "claim_id": "clm_negative",
            "artifact_id": "art_sh",
            "speaker": "kol",
            "timecode_or_span": "1",
            "claim_text": "HBM 供给正在缓解，价格会下跌，稀缺溢价难以持续。",
            "claim_type": "forecast",
            "confidence": 0.62,
            "linked_entity_ids_json": json_dumps([]),
            "data_date": "",
            "review_status": "unreviewed",
            "review_metadata_json": json_dumps({}),
            "domain_check_json": json_dumps({"passed": True, "warnings": []}),
            "freshness_status": "unknown",
            "status": "candidate",
        },
    )
    insert_row(
        conn,
        "theses",
        {
            "thesis_id": "thesis_hbm",
            "title": "HBM tightness thesis",
            "status": "evidence_backed",
            "horizon_months": 12,
            "theme_ids_json": json_dumps([]),
            "current_version_id": "ver_hbm",
            "owner": "tester",
        },
    )
    insert_row(
        conn,
        "thesis_versions",
        {
            "thesis_version_id": "ver_hbm",
            "thesis_id": "thesis_hbm",
            "statement": "HBM tightness persists",
            "mechanism_chain": "Supply stays tight -> pricing strong",
            "why_now": "AI demand persists",
            "base_case": "tight market",
            "counter_case": "supply catches up",
            "invalidators": "oversupply",
            "required_followups": "",
            "human_conviction": 0.7,
            "created_from_artifacts_json": json_dumps(["art_fh", "art_sh"]),
        },
    )
    insert_row(
        conn,
        "entities",
        {
            "entity_id": "ent_mu",
            "entity_type": "company",
            "canonical_name": "Micron",
            "aliases_json": json_dumps(["MU"]),
            "tickers_or_symbols_json": json_dumps(["MU"]),
            "jurisdiction": "US",
            "external_ids_json": json_dumps({}),
        },
    )
    insert_row(
        conn,
        "targets",
        {
            "target_id": "tgt_mu",
            "entity_id": "ent_mu",
            "asset_class": "equity",
            "venue": "NASDAQ",
            "ticker_or_symbol": "MU",
            "currency": "USD",
            "liquidity_bucket": "large",
        },
    )
    insert_row(
        conn,
        "target_cases",
        {
            "target_case_id": "tc_mu",
            "thesis_version_id": "ver_hbm",
            "target_id": "tgt_mu",
            "exposure_type": "direct",
            "capture_link_strength": 0.8,
            "key_metrics_json": json_dumps(["HBM ASP"]),
            "valuation_context": "",
            "risks": "",
            "status": "actionable",
        },
    )
    insert_row(
        conn,
        "validation_cases",
        {
            "validation_case_id": "vc_hbm",
            "route_id": None,
            "claim_id": "clm_negative",
            "thesis_id": "thesis_hbm",
            "thesis_version_id": "ver_hbm",
            "source_id": "src_official",
            "verdict": "validated",
            "evidence_artifact_ids_json": json_dumps(["art_fh"]),
            "rationale": "official anchor added",
            "validator": "human",
            "validator_model": "",
            "expires_at": "",
        },
    )
    conn.commit()
    return conn


def test_conflict_detector_finds_unresolved_conflict(tmp_path: Path) -> None:
    conn = _build_sample_db(tmp_path)
    graph = build_graph_from_db(conn, thesis_id="thesis_hbm")
    conflicts = detect_conflicts(graph)
    assert conflicts
    assert any(not item["resolved"] for item in conflicts)


def test_gate_report_includes_new_contract_checks(tmp_path: Path) -> None:
    conn = _build_sample_db(tmp_path)
    report = build_thesis_gate_report(conn, thesis_id="thesis_hbm")
    item = report["items"][0]
    assert "no_fatal_domain_warnings" in item["missing"]
    assert "claim_provenance_complete" in item["active_missing"]
    assert "no_unresolved_conflicts" in item["active_missing"]
    assert item["fatal_domain_warning_count"] == 1
    assert item["incomplete_provenance_count"] == 1
    assert item["unresolved_conflict_count"] >= 1


def test_integration_snapshot_today_compacts_dashboard_for_external_orchestrators(tmp_path: Path) -> None:
    conn = _build_sample_db(tmp_path)
    snapshot = build_integration_snapshot(conn, scope="today", limit=3)
    assert snapshot["ok"] is True
    assert snapshot["scope"] == "today"
    assert "queue_summary" in snapshot
    assert "top_theses" in snapshot
    assert any(item["thesis_id"] == "thesis_hbm" for item in snapshot["top_theses"])


def test_integration_snapshot_thesis_includes_graph_checks(tmp_path: Path) -> None:
    conn = _build_sample_db(tmp_path)
    snapshot = build_integration_snapshot(conn, scope="thesis", thesis_id="thesis_hbm", limit=3)
    assert snapshot["ok"] is True
    assert snapshot["scope"] == "thesis"
    assert snapshot["thesis"]["thesis_id"] == "thesis_hbm"
    assert snapshot["graph_checks"]["unresolved_conflict_count"] >= 1
    assert snapshot["thesis"]["gate_report"]["incomplete_provenance_count"] == 1


def test_claim_freshness_monitor_alerts_on_old_or_unknown_claims(tmp_path: Path) -> None:
    conn = _build_sample_db(tmp_path)
    insert_row(
        conn,
        "monitors",
        {
            "monitor_id": make_id("mon", "thesis_hbm"),
            "owner_object_type": "thesis",
            "owner_object_id": "thesis_hbm",
            "monitor_type": "claim_freshness",
            "metric_name": "max_claim_age_days",
            "comparator": "gte",
            "threshold_value": 1.0,
            "latest_value": None,
            "query_or_rule": json_dumps(
                {
                    "kind": "claim_freshness",
                    "thesis_id": "thesis_hbm",
                    "threshold_days": 180,
                }
            ),
            "status": "live",
            "last_checked_at": "",
        },
    )
    conn.commit()
    result = _run_monitors_with_conn(conn)
    assert result["monitor_count"] == 1
    assert result["results"][0]["metric_name"] == "max_claim_age_days"
    assert result["results"][0]["status"] == "alerted"


def test_review_claim_command_updates_provenance_fields(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    paths = resolve_paths(root)
    ensure_runtime_dirs(paths)
    conn = connect(paths.db_path)
    init_db(conn)
    insert_row(
        conn,
        "sources",
        {
            "source_id": "src_personal",
            "source_type": "personal",
            "name": "Voice Memo",
            "primaryness": "personal",
            "jurisdiction": "CN",
            "language": "zh",
            "base_uri": "",
            "credibility_policy": "",
            "track_record_stats_json": json_dumps({}),
        },
    )
    insert_row(
        conn,
        "artifacts",
        {
            "artifact_id": "art_memo",
            "source_id": "src_personal",
            "artifact_kind": "note",
            "title": "Memo",
            "captured_at": "2026-03-11T00:00:00+00:00",
            "published_at": "2026-03-11T00:00:00+00:00",
            "language": "zh",
            "uri": "",
            "raw_path": "",
            "normalized_text_path": "",
            "content_hash": "memo",
            "status": "extracted",
            "metadata_json": json_dumps({}),
        },
    )
    insert_claim(
        conn,
        {
            "claim_id": "clm_review",
            "artifact_id": "art_memo",
            "speaker": "self",
            "timecode_or_span": "0",
            "claim_text": "需要确认这条观点是否被后续证据支持。",
            "claim_type": "viewpoint",
            "confidence": 0.7,
            "linked_entity_ids_json": json_dumps([]),
            "data_date": "2026-03-11",
            "review_status": "unreviewed",
            "review_metadata_json": json_dumps({}),
            "domain_check_json": json_dumps({"passed": True, "warnings": []}),
            "freshness_status": "fresh",
            "status": "candidate",
        },
    )
    conn.commit()
    exit_code = cmd_review_claim(
        SimpleNamespace(
            root=str(root),
            claim_id="clm_review",
            status="confirmed",
            reviewer="human",
            review_date="2026-03-11",
            evidence=["https://example.com/filing"],
            correction=["clarified wording"],
            note="manual review complete",
        )
    )
    assert exit_code == 0
    row = conn.execute(
        "SELECT review_status, review_metadata_json FROM claims WHERE claim_id = ?",
        ("clm_review",),
    ).fetchone()
    assert row["review_status"] == "confirmed"
    metadata = row["review_metadata_json"]
    assert "manual review complete" in metadata
    assert "https://example.com/filing" in metadata
