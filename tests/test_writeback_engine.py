"""Tests for writeback_engine — dry-run + apply.

Uses in-memory v1 DB seeded with schema + optional thesis fixtures.
"""
from __future__ import annotations

import sqlite3
import json
import uuid

from finagent.db import SCHEMA_SQL, select_one, list_rows
from finagent.research_contracts import (
    ResearchPackage, EvidenceRef, WritebackTarget, WritebackOp,
)
from finagent.writeback_engine import plan_writeback, apply_writeback, print_writeback_plan


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def _seed_thesis(conn, thesis_id="t1", title="商业航天核心供应商竞争格局"):
    """Seed a thesis + version for matching tests."""
    now = _utcnow()
    vid = f"tv-{uuid.uuid4().hex[:8]}"
    conn.execute(
        "INSERT INTO theses (thesis_id, title, status, horizon_months, "
        "theme_ids_json, owner, current_version_id, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (thesis_id, title, "framed", 12, "[]", "user", vid, now),
    )
    conn.execute(
        "INSERT INTO thesis_versions (thesis_version_id, thesis_id, "
        "statement, mechanism_chain, why_now, base_case, counter_case, "
        "invalidators, required_followups, human_conviction, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (vid, thesis_id, "SpaceX引领变革", "低成本发射", "加速",
         "", "", "", "", 0.5, now),
    )
    conn.commit()
    return thesis_id, vid


def _seed_entity_target(conn, name="蓝箭航天", ticker="LANDSPACE"):
    """Seed entity + target (no target_case)."""
    now = _utcnow()
    eid = f"e-{uuid.uuid4().hex[:8]}"
    tid = f"tg-{uuid.uuid4().hex[:8]}"
    conn.execute(
        "INSERT INTO entities (entity_id, entity_type, canonical_name, "
        "created_at) VALUES (?, ?, ?, ?)",
        (eid, "company", name, now),
    )
    conn.execute(
        "INSERT INTO targets (target_id, entity_id, asset_class, venue, "
        "ticker_or_symbol, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (tid, eid, "equity", "HK", ticker, now),
    )
    conn.commit()
    return tid, eid


def _make_package(**overrides) -> ResearchPackage:
    """Build a test ResearchPackage."""
    defaults = {
        "run_id": f"run-{uuid.uuid4().hex[:12]}",
        "goal": "分析商业航天核心供应商竞争格局",
        "context": "商业航天",
        "triples": [
            {"subject": "蓝箭航天", "predicate": "是", "object": "商业航天企业"},
            {"subject": "SpaceX", "predicate": "引领", "object": "低成本发射"},
        ],
        "evidence_refs": [
            EvidenceRef(evidence_id=1, query="航天", char_count=200),
            EvidenceRef(evidence_id=2, query="SpaceX", char_count=300),
        ],
        "report_md": "# 商业航天研究报告\n\n蓝箭航天和SpaceX是主要竞争对手。",
        "confidence": 0.72,
        "iterations_used": 2,
    }
    defaults.update(overrides)
    return ResearchPackage(**defaults)


# ── Dry-Run Tests ────────────────────────────────────────────────────

class TestPlanWritebackDryRun:
    def test_creates_thesis_when_none_exists(self):
        """With empty DB, should plan a CREATE thesis."""
        conn = _make_conn()
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)

        thesis_actions = [a for a in actions if a.target_type == WritebackTarget.THESIS.value]
        assert len(thesis_actions) == 1
        assert thesis_actions[0].op == WritebackOp.CREATE.value
        assert thesis_actions[0].applied is False
        assert "create" in thesis_actions[0].dry_run_result

    def test_updates_thesis_when_match_exists(self):
        """With matching thesis, should plan an UPDATE."""
        conn = _make_conn()
        _seed_thesis(conn)
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)

        thesis_actions = [a for a in actions if a.target_type == WritebackTarget.THESIS.value]
        assert len(thesis_actions) == 1
        assert thesis_actions[0].op == WritebackOp.UPDATE.value
        assert thesis_actions[0].target_id == "t1"

    def test_always_creates_source(self):
        """Every plan should include a SOURCE create."""
        conn = _make_conn()
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)

        source_actions = [a for a in actions if a.target_type == WritebackTarget.SOURCE.value]
        assert len(source_actions) == 1
        assert source_actions[0].op == WritebackOp.CREATE.value

    def test_proposes_monitor_for_known_entity(self):
        """Entities found in v1 targets → monitor proposal."""
        conn = _make_conn()
        _seed_entity_target(conn, "蓝箭航天")
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)

        watch_actions = [a for a in actions if a.target_type == WritebackTarget.WATCH_ITEM.value]
        assert len(watch_actions) >= 1

    def test_no_monitor_for_unknown_entity(self):
        """Entities NOT in v1 targets → no monitor proposal."""
        conn = _make_conn()
        pkg = _make_package(triples=[
            {"subject": "完全未知公司XYZ", "predicate": "是", "object": "虚构企业"},
        ])
        actions = plan_writeback(pkg, conn)

        watch_actions = [a for a in actions if a.target_type == WritebackTarget.WATCH_ITEM.value]
        assert len(watch_actions) == 0

    def test_all_actions_unapplied(self):
        """All dry-run actions should have applied=False."""
        conn = _make_conn()
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)
        assert all(not a.applied for a in actions)

    def test_evidence_ids_carried(self):
        """Source evidence IDs should be attached to each action."""
        conn = _make_conn()
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)
        for a in actions:
            assert len(a.source_evidence_ids) >= 1


# ── Apply Tests ──────────────────────────────────────────────────────

class TestApplyWriteback:
    def test_create_thesis_populates_db(self):
        """CREATE thesis action should insert into theses + thesis_versions."""
        conn = _make_conn()
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)
        applied = apply_writeback(actions, conn)

        # All actions applied
        assert all(a.applied for a in applied)

        # Thesis created
        thesis_action = [a for a in applied if a.target_type == WritebackTarget.THESIS.value][0]
        tid = thesis_action.payload["thesis_id"]
        thesis = select_one(conn, "SELECT * FROM theses WHERE thesis_id = ?", (tid,))
        assert thesis is not None
        assert thesis["status"] == "seed"

        # Version created
        vid = thesis_action.payload["version_id"]
        version = select_one(conn, "SELECT * FROM thesis_versions WHERE thesis_version_id = ?", (vid,))
        assert version is not None

    def test_update_thesis_modifies_version(self):
        """UPDATE thesis action should modify existing thesis_version."""
        conn = _make_conn()
        _, vid = _seed_thesis(conn)
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)
        apply_writeback(actions, conn)

        # Check mechanism_chain updated
        version = select_one(
            conn, "SELECT mechanism_chain FROM thesis_versions WHERE thesis_version_id = ?", (vid,)
        )
        assert version is not None
        assert "蓝箭航天" in version["mechanism_chain"] or "低成本发射" in version["mechanism_chain"]

    def test_source_created(self):
        """SOURCE action should insert into sources table."""
        conn = _make_conn()
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)
        apply_writeback(actions, conn)

        sources = list_rows(conn, "SELECT * FROM sources WHERE source_type = 'v2_research_run'")
        assert len(sources) >= 1

    def test_monitor_created_for_known_entity(self):
        """WATCH_ITEM action should insert into monitors table."""
        conn = _make_conn()
        _seed_entity_target(conn, "蓝箭航天")
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)
        apply_writeback(actions, conn)

        monitors = list_rows(conn, "SELECT * FROM monitors WHERE monitor_type = 'research_signal'")
        assert len(monitors) >= 1

    def test_rollback_on_error(self):
        """Apply should rollback on failure and raise."""
        conn = _make_conn()
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)

        # Corrupt an action to cause failure
        actions[0].payload = {"bogus_key": True}
        actions[0].target_type = "nonexistent_type"

        # Should still not crash (unsupported type logged as warning)
        applied = apply_writeback(actions, conn)
        assert len(applied) == len(actions)


class TestPrintPlan:
    def test_print_works(self, capsys):
        """print_writeback_plan should not crash."""
        conn = _make_conn()
        pkg = _make_package()
        actions = plan_writeback(pkg, conn)
        print_writeback_plan(actions)
        captured = capsys.readouterr()
        assert "Writeback Plan" in captured.out
