from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finagent.sentinel import (
    SCHEMA_VERSION,
    classify_event,
    emit_stalled_events,
    import_events,
    load_sentinel_spec,
    record_anti_thesis_result,
    record_feedback,
    sync_sentinel_spec,
    validate_fixtures,
    validate_sentinel_spec,
)
from finagent.views import build_today_cockpit


def _write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_bundled_sentinel_spec_and_fixtures_are_valid() -> None:
    spec = load_sentinel_spec(REPO_ROOT / "specs" / "sentinel_v2.yaml")
    assert validate_sentinel_spec(spec) == []
    fixture_report = validate_fixtures()
    assert fixture_report["failed"] == 0
    assert fixture_report["passed"] == fixture_report["total"]


def test_validate_sentinel_spec_rejects_unknown_grammar_key() -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "sentinel": [
            {
                "entity": "Test",
                "product": "Unknown",
                "bucket_role": "core",
                "entity_role": "tracked",
                "grammar_key": "totally_unknown_grammar",
                "current_stage": "prototype",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "evidence_text": "样机",
                "source_role": "company_filing",
            }
        ],
    }
    errors = validate_sentinel_spec(spec)
    assert any("grammar_key invalid" in item for item in errors)


def test_import_events_updates_projection_and_candidate(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "sentinel": [
            {
                "sentinel_id": "sntl_test_power",
                "entity": "Test Corp",
                "product": "Rack Power Train",
                "bucket_role": "core",
                "entity_role": "tracked",
                "linked_thesis_id": "thesis_test",
                "linked_target_case_id": "tc_test",
                "grammar_key": "power_equipment_product_progress",
                "current_stage": "prototype",
                "stage_entered_at": "2026-03-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "expected_by": "2026-05-01",
                "evidence_text": "样机完成",
                "evidence_url": None,
                "evidence_date": "2026-03-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "notes": {"layer": "core"},
            }
        ],
    }
    sync_result = sync_sentinel_spec(fresh_db, spec)
    assert sync_result["ok"] is True

    event_result = import_events(
        fresh_db,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Test Corp",
                "product": "Rack Power Train",
                "event_type": "product_milestone",
                "stage_from": "prototype",
                "stage_to": "sample",
                "source_role": "company_filing",
                "evidence_text": "开始送样",
                "evidence_url": "https://example.com/sample",
                "evidence_date": "2026-04-01",
                "event_time": "2026-04-01T00:00:00+00:00",
                "first_seen_time": "2026-04-01T08:00:00+08:00",
                "processed_time": "2026-04-01T08:01:00+08:00",
                "novelty": "high",
                "relevance": "direct",
                "impact": "core_positive",
                "confidence": "high",
                "mapped_trigger": "B1",
                "candidate_thesis": None,
            },
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Upstream SiC Vendor",
                "product": "10kV SiC Module",
                "event_type": "candidate",
                "stage_from": None,
                "stage_to": None,
                "source_role": "media",
                "evidence_text": "多家供应链同步扩产",
                "evidence_url": "https://example.com/sic",
                "evidence_date": "2026-04-02",
                "event_time": "2026-04-02T00:00:00+00:00",
                "first_seen_time": "2026-04-02T08:00:00+08:00",
                "processed_time": "2026-04-02T08:01:00+08:00",
                "novelty": "high",
                "relevance": "adjacent",
                "impact": "upstream_enabler",
                "confidence": "medium",
                "mapped_trigger": None,
                "candidate_thesis": "SiC_supply_chain_maturation",
            },
        ],
    )
    assert event_result["ok"] is True
    assert event_result["imported"] == 2

    projection = fresh_db.execute(
        "SELECT current_stage, last_route, stall_status, linked_target_case_id FROM event_state_projections WHERE projection_id = ?",
        ("sntl_test_power",),
    ).fetchone()
    assert projection is not None
    assert projection["current_stage"] == "sample"
    assert projection["last_route"] == "review"
    assert projection["stall_status"] == "clear"
    assert projection["linked_target_case_id"] == "tc_test"

    candidate = fresh_db.execute(
        "SELECT thesis_name, route FROM opportunity_candidates WHERE thesis_name = ?",
        ("SiC_supply_chain_maturation",),
    ).fetchone()
    assert candidate is not None
    assert candidate["route"] == "opportunity"


def test_emit_stalled_events_creates_review_event(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "sentinel": [
            {
                "sentinel_id": "sntl_stall_case",
                "entity": "Milestone Corp",
                "product": "Grid Module",
                "bucket_role": "option",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_product_progress",
                "current_stage": "prototype",
                "stage_entered_at": "2026-01-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "expected_by": "2026-02-01",
                "evidence_text": "样机已完成",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "notes": {},
            }
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    stalled = emit_stalled_events(fresh_db, as_of="2026-03-14T00:00:00+00:00")
    assert stalled["emitted"] == 1

    event_row = fresh_db.execute(
        "SELECT event_type, route, mapped_trigger FROM event_mining_events WHERE projection_id = ?",
        ("sntl_stall_case",),
    ).fetchone()
    assert event_row is not None
    assert event_row["event_type"] == "stalled"
    assert event_row["route"] == "review"
    assert event_row["mapped_trigger"] == "B1"


def test_event_mining_cli_and_views(seeded_env, cli) -> None:
    root = seeded_env["root"]
    spec_path = root / "specs" / "sentinel_v2.yaml"
    spec_payload = {
        "schema_version": SCHEMA_VERSION,
        "sentinel": [
            {
                "sentinel_id": "sntl_cli_test",
                "entity": "Test Corp",
                "product": "Rack Power Train",
                "bucket_role": "core",
                "entity_role": "tracked",
                "linked_thesis_id": seeded_env["thesis_id"],
                "linked_target_case_id": "tc_test",
                "grammar_key": "power_equipment_product_progress",
                "current_stage": "prototype",
                "stage_entered_at": "2026-03-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "expected_by": "2026-05-01",
                "evidence_text": "样机完成",
                "evidence_url": None,
                "evidence_date": "2026-03-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "notes": {"layer": "core"},
            }
        ],
    }
    _write_json(spec_path, spec_payload)

    sync_result = cli("sentinel-sync", "--spec", str(spec_path))
    assert sync_result["ok"] is True
    assert sync_result["synced"] == 1

    events_path = root / "imports" / "event_batch.json"
    _write_json(
        events_path,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Test Corp",
                "product": "Rack Power Train",
                "event_type": "product_milestone",
                "stage_from": "prototype",
                "stage_to": "sample",
                "source_role": "company_filing",
                "evidence_text": "开始送样",
                "evidence_url": "https://example.com/sample",
                "evidence_date": "2026-04-01",
                "event_time": "2026-04-01T00:00:00+00:00",
                "first_seen_time": "2026-04-01T08:00:00+08:00",
                "processed_time": "2026-04-01T08:01:00+08:00",
                "novelty": "high",
                "relevance": "direct",
                "impact": "core_positive",
                "confidence": "high",
                "mapped_trigger": "B1",
                "candidate_thesis": None,
            },
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Upstream SiC Vendor",
                "product": "10kV SiC Module",
                "event_type": "candidate",
                "stage_from": None,
                "stage_to": None,
                "source_role": "media",
                "evidence_text": "多家供应链同步扩产",
                "evidence_url": "https://example.com/sic",
                "evidence_date": "2026-04-02",
                "event_time": "2026-04-02T00:00:00+00:00",
                "first_seen_time": "2026-04-02T08:00:00+08:00",
                "processed_time": "2026-04-02T08:01:00+08:00",
                "novelty": "high",
                "relevance": "adjacent",
                "impact": "upstream_enabler",
                "confidence": "medium",
                "mapped_trigger": None,
                "candidate_thesis": "SiC_supply_chain_maturation",
            },
        ],
    )

    validate_result = cli("event-validate", "--path", str(events_path))
    assert validate_result["ok"] is True
    assert validate_result["valid_count"] == 2

    import_result = cli("event-import", "--path", str(events_path), "--spec", str(spec_path))
    assert import_result["ok"] is True
    assert import_result["import_result"]["imported"] == 2
    assert import_result["spec_sync"]["synced"] == 1

    ledger = cli("event-ledger", "--route", "review")
    assert ledger["summary"]["review_items"] >= 1

    sentinel_board = cli("sentinel-board")
    assert sentinel_board["summary"]["projection_items"] >= 1

    opportunity_inbox = cli("opportunity-inbox")
    assert opportunity_inbox["summary"]["candidate_items"] >= 1

    watch_board = cli("watch-board")
    watch_item = next(item for item in watch_board["items"] if item["target_case_id"] == "tc_test")
    assert "event_mining" in watch_item
    assert watch_item["event_mining"]["projection_count"] >= 1


def test_secondary_signal_requires_corroboration_before_state_change(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "sentinel": [
            {
                "sentinel_id": "sntl_secondary_gate",
                "entity": "GridEdge",
                "product": "Rack SST",
                "bucket_role": "option",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_product_progress",
                "current_stage": "prototype",
                "stage_entered_at": "2026-03-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "expected_by": "2026-06-01",
                "evidence_text": "样机完成",
                "evidence_url": None,
                "evidence_date": "2026-03-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "adjacency_terms": ["SST", "solid-state transformer"],
                "anti_thesis_focus": ["竞争先发窗口", "送样转量产"],
                "notes": {},
            }
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True

    draft_one = {
        "schema_version": SCHEMA_VERSION,
        "entity": "GridEdge",
        "product": "Rack SST",
        "event_type": "product_milestone",
        "stage_from": "prototype",
        "stage_to": "sample",
        "source_role": "conference",
        "source_tier": "secondary",
        "root_claim_key": "claim_gridedge_sample",
        "independence_group": "indep_conf_a",
        "evidence_text": "大会纪要称已开始送样",
        "evidence_url": "https://example.com/conf-a",
        "evidence_date": "2026-04-01",
        "event_time": "2026-04-01T00:00:00+00:00",
        "first_seen_time": "2026-04-01T08:00:00+08:00",
        "processed_time": "2026-04-01T08:01:00+08:00",
        "novelty": "high",
        "relevance": "direct",
        "impact": "option_positive",
        "confidence": "medium",
        "mapped_trigger": "B1",
        "candidate_thesis": None,
    }
    first = classify_event(fresh_db, draft_one)
    assert first["ok"] is True
    assert first["can_apply_state"] is False
    assert first["route"] == "review"

    import_first = import_events(fresh_db, [draft_one])
    assert import_first["imported"] == 1
    projection = fresh_db.execute(
        "SELECT current_stage, raw_event_count, independence_group_count FROM event_state_projections WHERE projection_id = ?",
        ("sntl_secondary_gate",),
    ).fetchone()
    assert projection is not None
    assert projection["current_stage"] == "prototype"
    assert projection["raw_event_count"] == 1
    assert projection["independence_group_count"] == 1

    draft_two = dict(draft_one)
    draft_two["evidence_text"] = "第二个独立渠道确认已送样"
    draft_two["evidence_url"] = "https://example.com/conf-b"
    draft_two["first_seen_time"] = "2026-04-02T08:00:00+08:00"
    draft_two["processed_time"] = "2026-04-02T08:01:00+08:00"
    draft_two["independence_group"] = "indep_conf_b"
    draft_two["event_id"] = "evt_secondary_confirmed"

    second = classify_event(fresh_db, draft_two)
    assert second["ok"] is True
    assert second["can_apply_state"] is True
    assert second["corroboration"]["group_count"] == 2

    import_second = import_events(fresh_db, [draft_two])
    assert import_second["imported"] == 1
    projection_after = fresh_db.execute(
        """
        SELECT current_stage, last_source_tier, attention_capture_ratio, last_independence_group
        FROM event_state_projections
        WHERE projection_id = ?
        """,
        ("sntl_secondary_gate",),
    ).fetchone()
    assert projection_after is not None
    assert projection_after["current_stage"] == "sample"
    assert projection_after["last_source_tier"] == "secondary"
    assert projection_after["last_independence_group"] == "indep_conf_b"
    assert projection_after["attention_capture_ratio"] == 1.0


def test_attention_capture_ratio_rises_when_same_group_repeats(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "sentinel": [
            {
                "sentinel_id": "sntl_attention_noise",
                "entity": "Noise Grid",
                "product": "Transformer",
                "bucket_role": "core",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_commercialization",
                "current_stage": "repeat_order",
                "stage_entered_at": "2026-01-01T00:00:00+00:00",
                "current_confidence": "high",
                "expected_next_stage": "capacity_expansion",
                "expected_by": "2026-09-30",
                "evidence_text": "基础订单兑现",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B2",
                "notes": {},
            }
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    first = {
        "schema_version": SCHEMA_VERSION,
        "entity": "Noise Grid",
        "product": "Transformer",
        "event_type": "financial",
        "stage_from": None,
        "stage_to": None,
        "source_role": "company_filing",
        "evidence_text": "订单兑现继续",
        "evidence_url": "https://example.com/noise-a",
        "evidence_date": "2026-04-01",
        "event_time": "2026-04-01T00:00:00+00:00",
        "first_seen_time": "2026-04-01T08:00:00+08:00",
        "processed_time": "2026-04-01T08:01:00+08:00",
        "novelty": "high",
        "relevance": "direct",
        "impact": "core_positive",
        "confidence": "high",
        "mapped_trigger": "B2",
        "candidate_thesis": None,
        "root_claim_key": "noise-grid::order::repeat",
        "independence_group": "noise_group_shared",
    }
    second = dict(first)
    second["event_id"] = "evt_noise_repeat"
    second["evidence_text"] = "同一事实被媒体复述"
    second["evidence_url"] = "https://example.com/noise-b"
    second["source_role"] = "media"
    second["source_tier"] = "tertiary"
    second["first_seen_time"] = "2026-04-02T08:00:00+08:00"
    second["processed_time"] = "2026-04-02T08:01:00+08:00"

    assert import_events(fresh_db, [first])["imported"] == 1
    assert import_events(fresh_db, [second])["imported"] == 1

    projection = fresh_db.execute(
        """
        SELECT raw_event_count, independence_group_count, attention_capture_ratio
        FROM event_state_projections
        WHERE projection_id = ?
        """,
        ("sntl_attention_noise",),
    ).fetchone()
    assert projection is not None
    assert projection["raw_event_count"] == 2
    assert projection["independence_group_count"] == 1
    assert projection["attention_capture_ratio"] == 2.0


def test_kol_digest_events_stay_tertiary_and_do_not_apply_projection_state(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "sentinel": [
            {
                "sentinel_id": "sntl_kol_test",
                "entity": "Innolight",
                "product": "CPO silicon photonics",
                "bucket_role": "option",
                "entity_role": "tracked",
                "linked_thesis_id": "thesis_sip",
                "linked_target_case_id": "tc_sip",
                "grammar_key": "silicon_photonics_cpo_progress",
                "current_stage": "prototype",
                "stage_entered_at": "2026-03-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "expected_by": "2026-06-01",
                "evidence_text": "CPO 原型推进中",
                "evidence_url": None,
                "evidence_date": "2026-03-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "notes": {"layer": "option"},
            }
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    result = import_events(
        fresh_db,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Innolight",
                "product": "CPO silicon photonics",
                "event_type": "product_milestone",
                "stage_from": "prototype",
                "stage_to": "sample",
                "source_role": "kol_digest",
                "evidence_text": "Bernstein 认为 CPO 样片已通过关键客户验证",
                "evidence_url": "https://example.com/bernstein-sip",
                "evidence_date": "2026-03-15",
                "event_time": "2026-03-15T00:00:00+00:00",
                "first_seen_time": "2026-03-15T08:00:00+08:00",
                "processed_time": "2026-03-15T08:01:00+08:00",
                "novelty": "high",
                "relevance": "direct",
                "impact": "option_positive",
                "confidence": "medium",
                "mapped_trigger": "B1",
                "candidate_thesis": None,
            }
        ],
    )
    assert result["ok"] is True
    row = fresh_db.execute(
        """
        SELECT source_tier, route, projection_id, state_applied
        FROM event_mining_events
        WHERE projection_id = 'sntl_kol_test'
        ORDER BY processed_time DESC, created_at DESC
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    assert row["source_tier"] == "tertiary"
    assert row["route"] == "review"
    assert row["state_applied"] == 0

    projection = fresh_db.execute(
        "SELECT current_stage, last_source_tier, current_confidence FROM event_state_projections WHERE projection_id = 'sntl_kol_test'"
    ).fetchone()
    assert projection is not None
    assert projection["current_stage"] == "prototype"
    assert projection["last_source_tier"] == "tertiary"
    assert projection["current_confidence"] == "medium"


def test_candidate_anti_thesis_and_feedback_loop(fresh_db) -> None:
    result = import_events(
        fresh_db,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Orbital Supplier",
                "product": "Methalox turbopump",
                "event_type": "candidate",
                "stage_from": None,
                "stage_to": None,
                "source_role": "media",
                "evidence_text": "上游涡轮泵与低温阀门扩产，商业航天供应链成熟加速",
                "evidence_url": "https://example.com/space-supply",
                "evidence_date": "2026-04-03",
                "event_time": "2026-04-03T00:00:00+00:00",
                "first_seen_time": "2026-04-03T08:00:00+08:00",
                "processed_time": "2026-04-03T08:01:00+08:00",
                "novelty": "high",
                "relevance": "adjacent",
                "impact": "supply_chain_positive",
                "confidence": "medium",
                "mapped_trigger": None,
                "candidate_thesis": "commercial_space_supply_chain_maturation",
            }
        ],
    )
    assert result["imported"] == 1
    candidate_id = fresh_db.execute(
        "SELECT candidate_id FROM opportunity_candidates WHERE thesis_name = ?",
        ("commercial_space_supply_chain_maturation",),
    ).fetchone()["candidate_id"]

    candidate = fresh_db.execute(
        """
        SELECT anti_thesis_status, status, residual_class, raw_event_count, independence_group_count
        FROM opportunity_candidates
        WHERE candidate_id = ?
        """,
        (candidate_id,),
    ).fetchone()
    assert candidate is not None
    assert candidate["anti_thesis_status"] == "due"
    assert candidate["status"] == "open"
    assert candidate["residual_class"] == "frontier"
    assert candidate["raw_event_count"] == 1
    assert candidate["independence_group_count"] == 1

    anti = fresh_db.execute(
        "SELECT status, target_label FROM anti_thesis_checks WHERE object_type = 'candidate' AND object_id = ?",
        (candidate_id,),
    ).fetchone()
    assert anti is not None
    assert anti["status"] == "due"

    logged = record_anti_thesis_result(
        fresh_db,
        object_type="candidate",
        object_id=candidate_id,
        verdict="resolved",
        result_summary="竞争对手量产仍不足，反证未击穿 thesis",
        contradiction_score=0.3,
    )
    assert logged["ok"] is True

    feedback = record_feedback(
        fresh_db,
        object_type="candidate",
        object_id=candidate_id,
        feedback_type="candidate_verdict",
        verdict="promote",
        score=0.9,
        note="进入 prepare",
    )
    assert feedback["ok"] is True
    fresh_db.commit()

    updated = fresh_db.execute(
        "SELECT anti_thesis_status, status FROM opportunity_candidates WHERE candidate_id = ?",
        (candidate_id,),
    ).fetchone()
    assert updated is not None
    assert updated["anti_thesis_status"] == "recorded"
    assert updated["status"] == "promoted"

    cockpit = build_today_cockpit(fresh_db)
    assert "event_mining_summary" in cockpit["focus"]
    assert cockpit["focus"]["event_mining_summary"]["opportunity"]["candidate_items"] >= 1
    assert len(cockpit["focus"]["recent_event_items"]) >= 1


def test_daily_refresh_syncs_sentinel_and_emits_stalls(cli_root, cli) -> None:
    spec_path = cli_root / "specs" / "sentinel_v2.yaml"
    _write_json(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "sentinel": [
                {
                    "sentinel_id": "sntl_daily_stall",
                    "entity": "Daily Refresh Corp",
                    "product": "Grid Module",
                    "bucket_role": "option",
                    "entity_role": "tracked",
                    "linked_thesis_id": None,
                    "linked_target_case_id": None,
                    "grammar_key": "power_equipment_product_progress",
                    "current_stage": "prototype",
                    "stage_entered_at": "2026-01-01T00:00:00+00:00",
                    "current_confidence": "medium",
                    "expected_next_stage": "sample",
                    "expected_by": "2026-02-01",
                    "evidence_text": "样机完成",
                    "evidence_url": None,
                    "evidence_date": "2026-01-01",
                    "source_role": "company_filing",
                    "trigger_code": "B1",
                    "notes": {},
                }
            ],
        },
    )
    result = cli(
        "daily-refresh",
        "--skip-fetch",
        "--skip-monitors",
        "--as-of",
        "2026-03-14T00:00:00+00:00",
    )
    assert result["ok"] is True
    assert result["sentinel_sync"]["synced"] == 1
    assert result["stalled_result"]["emitted"] == 1
    assert result["refresh_summary"]["stalled_emitted"] == 1
