from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finagent.sentinel import SCHEMA_VERSION, emit_stalled_events, import_events, sync_sentinel_spec
from finagent.theme_report import build_theme_investment_report, render_theme_investment_report


def test_theme_report_ranks_alternative_over_option_when_option_is_unproven(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "theme": {
            "title": "Transformer",
            "investor_question": "当前哪个表达最接近 prepare？",
            "thesis_statement": "电力设备景气延续，但表达分层很重要。",
            "why_now": "AIDC 与电网扩张同时推进。",
            "why_mispriced": "市场把 option 想象和 core 兑现混在一起定价。",
            "current_posture": "watch_only",
            "capital_gate": ["core 兑现 + 约束缓解"],
        },
        "quality_policy": {
            "max_constraint_burden": 0.2,
        },
        "sentinel": [
            {
                "sentinel_id": "sntl_core",
                "entity": "Core Grid",
                "product": "AIDC power train",
                "bucket_role": "core",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_commercialization",
                "current_stage": "repeat_order",
                "stage_entered_at": "2026-01-01T00:00:00+00:00",
                "current_confidence": "high",
                "expected_next_stage": "capacity_expansion",
                "expected_by": "2026-10-01",
                "evidence_text": "AIDC 订单持续兑现",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B2",
                "notes": {
                    "expression_role": "direct core expression",
                    "current_action": "watch",
                    "upgrade_requirements": ["OCF 转正"],
                    "why_not_now": "constraint 仍在",
                },
            },
            {
                "sentinel_id": "sntl_alt",
                "entity": "Alt Grid",
                "product": "UHV equipment",
                "bucket_role": "alternative",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_capacity_and_delivery",
                "current_stage": "capacity_expansion",
                "stage_entered_at": "2026-01-01T00:00:00+00:00",
                "current_confidence": "high",
                "expected_next_stage": "mass_adoption",
                "expected_by": "2026-12-31",
                "evidence_text": "低估值表达进入扩产兑现",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B3",
                "notes": {
                    "expression_role": "lower multiple alternative",
                    "current_action": "prepare",
                    "upgrade_requirements": ["利润兑现维持"],
                },
            },
            {
                "sentinel_id": "sntl_option",
                "entity": "Core Grid",
                "product": "SST option",
                "bucket_role": "option",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_product_progress",
                "current_stage": "prototype",
                "stage_entered_at": "2026-01-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "expected_by": "2026-09-30",
                "evidence_text": "样机完成",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "notes": {
                    "expression_role": "long dated option",
                    "current_action": "watch",
                    "upgrade_requirements": ["进入 customer_validation"],
                    "why_not_now": "仍处于 prototype",
                },
            },
        ],
    }
    sync = sync_sentinel_spec(fresh_db, spec)
    assert sync["ok"] is True
    imported = import_events(
        fresh_db,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Core Grid",
                "product": "AIDC power train",
                "event_type": "financial",
                "stage_from": None,
                "stage_to": None,
                "source_role": "company_filing",
                "evidence_text": "经营现金流仍承压",
                "evidence_url": "https://example.com/core-constraint",
                "evidence_date": "2026-04-01",
                "event_time": "2026-04-01T00:00:00+00:00",
                "first_seen_time": "2026-04-01T08:00:00+08:00",
                "processed_time": "2026-04-01T08:01:00+08:00",
                "novelty": "high",
                "relevance": "direct",
                "impact": "constraint_negative",
                "confidence": "high",
                "mapped_trigger": "V2",
                "candidate_thesis": None,
            },
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Alt Grid",
                "product": "UHV equipment",
                "event_type": "product_milestone",
                "stage_from": "capacity_expansion",
                "stage_to": "mass_adoption",
                "source_role": "company_filing",
                "evidence_text": "交付与利润继续兑现",
                "evidence_url": "https://example.com/alt",
                "evidence_date": "2026-04-02",
                "event_time": "2026-04-02T00:00:00+00:00",
                "first_seen_time": "2026-04-02T08:00:00+08:00",
                "processed_time": "2026-04-02T08:01:00+08:00",
                "novelty": "high",
                "relevance": "direct",
                "impact": "alternative_positive",
                "confidence": "high",
                "mapped_trigger": "B3",
                "candidate_thesis": None,
            },
        ],
    )
    assert imported["imported"] == 2

    report = build_theme_investment_report(
        fresh_db,
        spec,
        theme_slug="transformer",
        as_of="2026-04-15T00:00:00+00:00",
    )
    assert report["summary"]["recommended_posture"] == "watch_with_prepare_candidate"
    assert report["summary"]["best_expression"]["projection_id"] == "sntl_alt"
    assert report["summary"]["best_expression"]["evidence_quality_band"] == "strong"
    assert report["decision_card"]["decision_now"] == "prepare_best_expression"
    assert report["decision_card"]["best_expression_projection_id"] == "sntl_alt"
    assert any("Core Grid" in reason or "constraint" in reason for reason in report["decision_card"]["why_not_investable_yet"])
    actions = {item["projection_id"]: item["recommended_action"] for item in report["expressions"]}
    assert actions["sntl_alt"] == "prepare_candidate"
    assert actions["sntl_option"] == "watch_only_option"
    assert any("option 表达仍未跨过 prepare 门槛" in gap for gap in report["quality_gaps"])

    markdown = render_theme_investment_report(report)
    assert "Why mispriced / why not yet" in markdown
    assert "## 3. Decision Card" in markdown
    assert "Why not investable yet" in markdown
    assert "`sntl_alt`" in markdown
    assert "strong" in markdown


def test_theme_report_flags_overdue_option_and_due_candidate(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "theme": {
            "title": "Commercial Space",
            "investor_question": "Neutron 延迟是否改变资本动作？",
            "thesis_statement": "空间系统是 core，发射是 option。",
            "why_now": "国防空间系统兑现快于发射平台。",
            "why_mispriced": "市场常把发射 option 的波动错映射到空间系统 core。",
            "current_posture": "watch_only",
        },
        "sentinel": [
            {
                "sentinel_id": "sntl_neutron",
                "entity": "Rocket Lab",
                "product": "Neutron",
                "bucket_role": "option",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "commercial_space_launch_progress",
                "current_stage": "prototype",
                "stage_entered_at": "2025-01-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "qualification",
                "expected_by": "2025-12-31",
                "evidence_text": "发射前准备持续中",
                "evidence_url": None,
                "evidence_date": "2025-01-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "notes": {
                    "expression_role": "launch option",
                    "current_action": "watch",
                    "upgrade_requirements": ["首飞成功"],
                },
            }
        ],
    }
    sync = sync_sentinel_spec(fresh_db, spec)
    assert sync["ok"] is True
    stalled = emit_stalled_events(fresh_db, as_of="2026-04-15T00:00:00+00:00")
    assert stalled["emitted"] == 1
    imported = import_events(
        fresh_db,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Rocket Lab",
                "product": "Defense payload stack",
                "event_type": "candidate",
                "stage_from": None,
                "stage_to": None,
                "source_role": "media",
                "evidence_text": "Rocket Lab 在 defense payload stack 上的垂直整合机会增强，国防载荷链有望独立成线",
                "evidence_url": "https://example.com/payload",
                "evidence_date": "2026-04-10",
                "event_time": "2026-04-10T00:00:00+00:00",
                "first_seen_time": "2026-04-10T08:00:00+08:00",
                "processed_time": "2026-04-10T08:01:00+08:00",
                "novelty": "high",
                "relevance": "adjacent",
                "impact": "adjacent_enabler",
                "confidence": "medium",
                "mapped_trigger": None,
                "candidate_thesis": "defense_payload_verticalization",
            }
        ],
    )
    assert imported["imported"] == 1

    report = build_theme_investment_report(
        fresh_db,
        spec,
        theme_slug="commercial_space",
        as_of="2026-04-15T00:00:00+00:00",
    )
    expression = report["expressions"][0]
    assert expression["projection_id"] == "sntl_neutron"
    assert expression["recommended_action"] == "review_required"
    assert expression["constraint_burden"] > 0.0
    assert report["decision_card"]["decision_now"] == "review_required"
    assert any("time stop" in reason or "里程碑已 overdue" in reason for reason in report["decision_card"]["why_not_investable_yet"])
    assert report["candidates"][0]["anti_thesis_status"] == "due"


def test_theme_report_prefers_lower_constraint_when_readiness_ties(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "theme": {
            "title": "Tie Break",
            "investor_question": "同一 readiness 下谁更优？",
            "thesis_statement": "低约束表达应该优先。",
            "why_now": "两家公司 stage 一样。",
            "why_mispriced": "约束差异决定动作优先级。",
        },
        "sentinel": [
            {
                "sentinel_id": "sntl_low",
                "entity": "Low Constraint",
                "product": "Grid Rack",
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
                "evidence_text": "订单兑现",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B2",
                "notes": {"expression_role": "clean core"},
            },
            {
                "sentinel_id": "sntl_high",
                "entity": "High Constraint",
                "product": "Grid Rack",
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
                "evidence_text": "订单兑现但现金承压",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B2",
                "notes": {"expression_role": "burdened core"},
            },
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    imported = import_events(
        fresh_db,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "High Constraint",
                "product": "Grid Rack",
                "event_type": "financial",
                "stage_from": None,
                "stage_to": None,
                "source_role": "company_filing",
                "evidence_text": "营运资本恶化",
                "evidence_url": "https://example.com/high-constraint",
                "evidence_date": "2026-04-05",
                "event_time": "2026-04-05T00:00:00+00:00",
                "first_seen_time": "2026-04-05T08:00:00+08:00",
                "processed_time": "2026-04-05T08:01:00+08:00",
                "novelty": "high",
                "relevance": "direct",
                "impact": "constraint_negative",
                "confidence": "high",
                "mapped_trigger": "V2",
                "candidate_thesis": None,
            }
        ],
    )
    assert imported["imported"] == 1
    report = build_theme_investment_report(fresh_db, spec, theme_slug="tie_break", as_of="2026-04-15T00:00:00+00:00")
    assert report["summary"]["best_expression"]["projection_id"] == "sntl_low"
    burdens = {item["projection_id"]: item["constraint_burden"] for item in report["expressions"]}
    assert burdens["sntl_high"] > burdens["sntl_low"]


def test_theme_report_constraint_burden_dedups_same_group_and_decays_old_events(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "theme": {
            "title": "Constraint Dedup",
            "investor_question": "重复负面是否会把约束打满？",
            "thesis_statement": "去重和时间衰减应避免无界累加。",
            "why_now": "风险事件出现多个转载。",
            "why_mispriced": "不能把同一根事实重复当成多次独立约束。",
        },
        "sentinel": [
            {
                "sentinel_id": "sntl_decay",
                "entity": "Decay Power",
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
                "evidence_text": "兑现中",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B2",
                "notes": {},
            }
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    imported = import_events(
        fresh_db,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Decay Power",
                "product": "Transformer",
                "event_type": "financial",
                "stage_from": None,
                "stage_to": None,
                "source_role": "company_filing",
                "source_tier": "primary",
                "evidence_text": "去年应收恶化（渠道A）",
                "evidence_url": "https://example.com/decay-a",
                "evidence_date": "2025-01-01",
                "event_time": "2025-01-01T00:00:00+00:00",
                "first_seen_time": "2026-04-01T08:00:00+08:00",
                "processed_time": "2026-04-01T08:01:00+08:00",
                "novelty": "medium",
                "relevance": "direct",
                "impact": "constraint_negative",
                "confidence": "high",
                "mapped_trigger": "V2",
                "candidate_thesis": None,
                "root_claim_key": "decay::constraint::ar",
                "independence_group": "decay_group_shared",
            },
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Decay Power",
                "product": "Transformer",
                "event_type": "financial",
                "stage_from": None,
                "stage_to": None,
                "source_role": "media",
                "source_tier": "tertiary",
                "evidence_text": "去年应收恶化（转载B）",
                "evidence_url": "https://example.com/decay-b",
                "evidence_date": "2025-01-02",
                "event_time": "2025-01-02T00:00:00+00:00",
                "first_seen_time": "2026-04-02T08:00:00+08:00",
                "processed_time": "2026-04-02T08:01:00+08:00",
                "novelty": "medium",
                "relevance": "direct",
                "impact": "constraint_negative",
                "confidence": "medium",
                "mapped_trigger": "V2",
                "candidate_thesis": None,
                "root_claim_key": "decay::constraint::ar",
                "independence_group": "decay_group_shared",
            },
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Decay Power",
                "product": "Transformer",
                "event_type": "financial",
                "stage_from": None,
                "stage_to": None,
                "source_role": "company_filing",
                "source_tier": "primary",
                "evidence_text": "最近 OCF 再次恶化",
                "evidence_url": "https://example.com/decay-c",
                "evidence_date": "2026-04-10",
                "event_time": "2026-04-10T00:00:00+00:00",
                "first_seen_time": "2026-04-10T08:00:00+08:00",
                "processed_time": "2026-04-10T08:01:00+08:00",
                "novelty": "high",
                "relevance": "direct",
                "impact": "constraint_negative",
                "confidence": "high",
                "mapped_trigger": "V2",
                "candidate_thesis": None,
                "root_claim_key": "decay::constraint::ocf",
                "independence_group": "decay_group_recent",
            },
        ],
    )
    assert imported["imported"] == 3
    report = build_theme_investment_report(fresh_db, spec, theme_slug="constraint_decay", as_of="2026-04-15T00:00:00+00:00")
    expression = report["expressions"][0]
    assert expression["projection_id"] == "sntl_decay"
    assert expression["constraint_burden"] < 0.5
    assert expression["constraint_burden"] >= 0.3


def test_theme_report_generates_rule_based_blockers_without_spec_reason(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "theme": {
            "title": "Rule Blockers",
            "investor_question": "没有 spec 文案时能否自动阻止升级？",
            "thesis_statement": "阶段门槛应来自规则。",
            "why_now": "商业化仍早。",
            "why_mispriced": "市场容易把 prototype 当成量产。",
        },
        "sentinel": [
            {
                "sentinel_id": "sntl_rule",
                "entity": "Rule Power",
                "product": "SST",
                "bucket_role": "option",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_product_progress",
                "current_stage": "prototype",
                "stage_entered_at": "2026-01-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "expected_by": "2026-09-30",
                "evidence_text": "样机完成",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "notes": {},
            }
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    report = build_theme_investment_report(fresh_db, spec, theme_slug="rule_blockers", as_of="2026-04-15T00:00:00+00:00")
    reasons = report["decision_card"]["why_not_investable_yet"]
    assert any("prepare 门槛" in reason for reason in reasons)
    assert any("option 仍未跨过可资本化门槛" in reason for reason in reasons)


def test_theme_report_suspends_on_thesis_level_negative_event(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "theme": {
            "title": "Suspend Theme",
            "investor_question": "thesis-level falsifier 是否会暂停？",
            "thesis_statement": "宏观 capex 支撑主题。",
            "why_now": "当前看起来还行。",
            "why_mispriced": "市场没计入 capex 下调风险。",
            "thesis_level_falsifiers": ["Hyperscaler capex 下调 >20%"],
        },
        "sentinel": [
            {
                "sentinel_id": "sntl_macro",
                "entity": "Macro Grid",
                "product": "Power Rack",
                "bucket_role": "core",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_commercialization",
                "current_stage": "repeat_order",
                "stage_entered_at": "2026-01-01T00:00:00+00:00",
                "current_confidence": "high",
                "expected_next_stage": "capacity_expansion",
                "expected_by": "2026-10-01",
                "evidence_text": "订单兑现",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B2",
                "notes": {},
            }
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    imported = import_events(
        fresh_db,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Macro Grid",
                "product": "Power Rack",
                "event_type": "macro",
                "stage_from": None,
                "stage_to": None,
                "source_role": "customer_signal",
                "evidence_text": "Hyperscaler capex 下调 25%",
                "evidence_url": "https://example.com/f3",
                "evidence_date": "2026-04-12",
                "event_time": "2026-04-12T00:00:00+00:00",
                "first_seen_time": "2026-04-12T08:00:00+08:00",
                "processed_time": "2026-04-12T08:01:00+08:00",
                "novelty": "high",
                "relevance": "direct",
                "impact": "macro_negative",
                "confidence": "high",
                "mapped_trigger": "F3",
                "candidate_thesis": None,
            }
        ],
    )
    assert imported["imported"] == 1
    report = build_theme_investment_report(fresh_db, spec, theme_slug="suspend_theme", as_of="2026-04-15T00:00:00+00:00")
    assert report["summary"]["recommended_posture"] == "suspended"
    assert report["decision_card"]["decision_now"] == "suspend_theme"


def test_theme_report_time_stop_requires_human_archive_confirmation(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "theme": {
            "title": "Catalyst Time Stop",
            "investor_question": "超期催化剂是否需要人工确认归档？",
            "thesis_statement": "明确催化剂超期后应退出 active watch。",
            "why_now": "催化剂已经明显失约。",
            "why_mispriced": "市场仍在把超期当延期而不是失败。",
            "time_stop_policy": {
                "thesis_type": "catalyst",
                "review_after_days": 30,
                "suspend_after_days": 60,
                "archive_after_days": 120,
            },
        },
        "sentinel": [
            {
                "sentinel_id": "sntl_catalyst",
                "entity": "Catalyst Grid",
                "product": "Near-term program",
                "bucket_role": "core",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_commercialization",
                "current_stage": "prototype",
                "stage_entered_at": "2025-10-01T00:00:00+00:00",
                "current_confidence": "medium",
                "expected_next_stage": "sample",
                "expected_by": "2026-01-01",
                "evidence_text": "计划应在年初进入 sample",
                "evidence_url": None,
                "evidence_date": "2025-10-01",
                "source_role": "company_filing",
                "trigger_code": "B1",
                "notes": {},
            }
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    stalled = emit_stalled_events(fresh_db, as_of="2026-05-15T00:00:00+00:00")
    assert stalled["emitted"] == 1

    report = build_theme_investment_report(
        fresh_db,
        spec,
        theme_slug="catalyst_time_stop",
        as_of="2026-05-15T00:00:00+00:00",
    )
    expression = report["expressions"][0]
    assert expression["recommended_action"] == "archive_pending_confirmation"
    assert expression["time_stop_action"] == "archive_pending_confirmation"
    assert "归档" in str(expression["time_stop_reason"])
    assert report["summary"]["recommended_posture"] == "review_required"
    assert report["decision_card"]["decision_now"] == "human_confirm_archive"


def test_theme_report_budget_and_slot_justification_flags_pressure(fresh_db) -> None:
    spec = {
        "schema_version": SCHEMA_VERSION,
        "theme": {
            "title": "Budget Pressure",
            "investor_question": "预算超限时是否会逼出 slot justification？",
            "thesis_statement": "弱表达不应无限占用 diligence capital。",
            "why_now": "候选开始挤占有限注意力。",
            "why_mispriced": "最弱表达并不值得继续占 slot。",
        },
        "diligence_budget": {
            "max_expressions_per_theme": 1,
            "max_candidates_per_theme": 1,
        },
        "sentinel": [
            {
                "sentinel_id": "sntl_strong",
                "entity": "Strong Grid",
                "product": "Core Rack",
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
                "evidence_text": "核心订单兑现",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "company_filing",
                "trigger_code": "B2",
                "notes": {"expression_role": "best core"},
            },
            {
                "sentinel_id": "sntl_weak",
                "entity": "Weak Grid",
                "product": "Speculative Rack",
                "bucket_role": "core",
                "entity_role": "tracked",
                "linked_thesis_id": None,
                "linked_target_case_id": None,
                "grammar_key": "power_equipment_commercialization",
                "current_stage": "repeat_order",
                "stage_entered_at": "2026-01-01T00:00:00+00:00",
                "current_confidence": "low",
                "expected_next_stage": "capacity_expansion",
                "expected_by": "2026-09-30",
                "evidence_text": "只有弱媒体信号",
                "evidence_url": None,
                "evidence_date": "2026-01-01",
                "source_role": "media",
                "trigger_code": "B2",
                "notes": {},
            },
        ],
    }
    assert sync_sentinel_spec(fresh_db, spec)["ok"] is True
    imported = import_events(
        fresh_db,
        [
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Weak Grid",
                "product": "Speculative Rack",
                "event_type": "candidate",
                "stage_from": None,
                "stage_to": None,
                "source_role": "media",
                "evidence_text": "adjacent candidate one",
                "evidence_url": "https://example.com/candidate-1",
                "evidence_date": "2026-04-10",
                "event_time": "2026-04-10T00:00:00+00:00",
                "first_seen_time": "2026-04-10T08:00:00+08:00",
                "processed_time": "2026-04-10T08:01:00+08:00",
                "novelty": "high",
                "relevance": "adjacent",
                "impact": "adjacent_enabler",
                "confidence": "high",
                "mapped_trigger": None,
                "candidate_thesis": "candidate_one",
            },
            {
                "schema_version": SCHEMA_VERSION,
                "entity": "Weak Grid",
                "product": "Speculative Rack",
                "event_type": "candidate",
                "stage_from": None,
                "stage_to": None,
                "source_role": "media",
                "evidence_text": "adjacent candidate two",
                "evidence_url": "https://example.com/candidate-2",
                "evidence_date": "2026-04-12",
                "event_time": "2026-04-12T00:00:00+00:00",
                "first_seen_time": "2026-04-12T08:00:00+08:00",
                "processed_time": "2026-04-12T08:01:00+08:00",
                "novelty": "high",
                "relevance": "adjacent",
                "impact": "adjacent_enabler",
                "confidence": "high",
                "mapped_trigger": None,
                "candidate_thesis": "candidate_two",
            },
        ],
    )
    assert imported["imported"] == 2

    report = build_theme_investment_report(
        fresh_db,
        spec,
        theme_slug="budget_pressure",
        as_of="2026-04-15T00:00:00+00:00",
    )
    budget_status = report["summary"]["diligence_budget_status"]
    assert budget_status["over_budget"] is True
    assert any("2 个表达超出 diligence 预算上限 1" in warning for warning in budget_status["warnings"])
    assert any("2 个候选超出预算上限 1" in warning for warning in budget_status["warnings"])
    
    # Verify that at least one of the expressions flagged slot justification
    needs_justification = False
    has_opportunity_cost = False
    competing_candidates_count = 0
    for expr in report["expressions"]:
        slot = expr.get("slot_justification")
        if slot:
            if slot.get("needs_justification"):
                needs_justification = True
            if slot.get("opportunity_cost_flag"):
                has_opportunity_cost = True
            competing_candidates_count += len(slot.get("competing_candidates", []))
            
    assert needs_justification is True
    assert has_opportunity_cost is True
    assert competing_candidates_count >= 1
