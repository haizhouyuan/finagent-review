from __future__ import annotations

import json
from pathlib import Path

from finagent.event_replay import ReplayInputs, replay_theme_run, validate_theme_replay
from finagent.sentinel import SCHEMA_VERSION, import_events, sync_sentinel_spec


def _write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_replay_theme_run_matches_reference_import(tmp_path: Path, fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "sentinel": [
            {
                "sentinel_id": "sntl_test_power",
                "entity": "Test Corp",
                "product": "Rack Power Train",
                "bucket_role": "core",
                "entity_role": "tracked",
                "grammar_key": "power_equipment_product_progress",
                "current_stage": "prototype",
                "stage_entered_at": "2026-03-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "expected_by": "2026-05-01",
                "evidence_text": "样机完成",
                "evidence_date": "2026-03-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "notes": {"adjacency_terms": ["SiC"]},
            }
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    events = [
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
    ]
    reference_import = import_events(fresh_db, events)
    reference_stall = {"emitted": 0}

    spec_path = _write_json(tmp_path / "spec.json", spec)
    events_path = _write_json(tmp_path / "events.json", events)
    replay_payload = replay_theme_run(
        ReplayInputs(
            spec_path=spec_path,
            events_path=events_path,
            as_of="2026-04-15T00:00:00+00:00",
            theme_slug="test_theme",
        )
    )
    validation = validate_theme_replay(
        reference_import_result=reference_import,
        reference_stall_result=reference_stall,
        replay_payload=replay_payload,
    )
    assert validation["ok"] is True
    assert validation["expected_event_count"] == 2
    assert replay_payload["theme_radar_board"]["summary"]["radar_items"] >= 1
