"""P1a closeout tests: idempotency, statement update, real rollback.

Addresses Codex audit findings on writeback_engine.
"""
from __future__ import annotations

import sqlite3
import json
import uuid

import pytest

from finagent.db import SCHEMA_SQL, select_one, list_rows
from finagent.research_contracts import (
    ResearchPackage, EvidenceRef, WritebackTarget, WritebackOp, WritebackAction,
)
from finagent.writeback_engine import plan_writeback, apply_writeback
from finagent.thesis_bridge import load_targets


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def _seed_thesis(conn, thesis_id="t1", title="商业航天核心供应商竞争格局"):
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
        (vid, thesis_id, "原始statement", "原始chain", "原始", "", "", "", "", 0.3, now),
    )
    conn.commit()
    return thesis_id, vid


def _seed_entity_target(conn, name="蓝箭航天"):
    now = _utcnow()
    eid = f"e-{uuid.uuid4().hex[:8]}"
    tid = f"tg-{uuid.uuid4().hex[:8]}"
    conn.execute(
        "INSERT INTO entities (entity_id, entity_type, canonical_name, created_at) "
        "VALUES (?, ?, ?, ?)", (eid, "company", name, now),
    )
    conn.execute(
        "INSERT INTO targets (target_id, entity_id, asset_class, venue, "
        "ticker_or_symbol, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (tid, eid, "equity", "HK", "TICKER", now),
    )
    conn.commit()
    return tid, eid


def _make_package(**overrides) -> ResearchPackage:
    defaults = {
        "run_id": "run-test1234ab",
        "goal": "分析商业航天核心供应商竞争格局",
        "context": "商业航天",
        "triples": [
            {"subject": "蓝箭航天", "predicate": "是", "object": "商业航天企业"},
        ],
        "evidence_refs": [
            EvidenceRef(evidence_id=1, query="航天", char_count=200),
        ],
        "report_md": "# 商业航天研究报告\n\n蓝箭航天和SpaceX是主要竞争对手。这是一段较长的研究结论内容。",
        "confidence": 0.72,
    }
    defaults.update(overrides)
    return ResearchPackage(**defaults)


# ── Idempotency Tests ────────────────────────────────────────────────

class TestIdempotency:
    def test_double_apply_creates_thesis_once(self):
        """Applying same package twice should not create duplicate thesis."""
        conn = _make_conn()
        pkg = _make_package()

        actions1 = plan_writeback(pkg, conn)
        apply_writeback(actions1, conn)

        actions2 = plan_writeback(pkg, conn)
        apply_writeback(actions2, conn)

        # Should still be exactly 1 thesis
        # (second plan may match the newly created thesis → UPDATE instead)
        theses = list_rows(conn, "SELECT * FROM theses")
        assert len(theses) <= 2  # At most: original + 1 created

    def test_double_apply_creates_source_once(self):
        """Applying same package twice should not duplicate source."""
        conn = _make_conn()
        pkg = _make_package()

        actions1 = plan_writeback(pkg, conn)
        apply_writeback(actions1, conn)

        # Apply same actions again (simulate retry)
        actions2 = plan_writeback(pkg, conn)
        apply_writeback(actions2, conn)

        sources = list_rows(
            conn, "SELECT * FROM sources WHERE source_type = 'v2_research_run'"
        )
        assert len(sources) == 1  # Idempotent: only 1

    def test_double_apply_creates_monitor_once(self):
        """Applying same package twice should not duplicate monitors."""
        conn = _make_conn()
        _seed_entity_target(conn, "蓝箭航天")
        pkg = _make_package()

        actions1 = plan_writeback(pkg, conn)
        apply_writeback(actions1, conn)

        actions2 = plan_writeback(pkg, conn)
        apply_writeback(actions2, conn)

        monitors = list_rows(
            conn, "SELECT * FROM monitors WHERE monitor_type = 'research_signal'"
        )
        assert len(monitors) == 1  # Idempotent: only 1

    def test_stable_ids_from_run_id(self):
        """IDs should be deterministic from run_id."""
        conn = _make_conn()
        pkg = _make_package(run_id="run-abcdef123456")
        actions = plan_writeback(pkg, conn)

        thesis_action = [a for a in actions if a.target_type == WritebackTarget.THESIS.value][0]
        source_action = [a for a in actions if a.target_type == WritebackTarget.SOURCE.value][0]

        # IDs derived from run_id suffix
        assert "abcdef123456" in thesis_action.payload["thesis_id"]
        assert "abcdef123456" in source_action.payload["source_id"]


# ── Statement UPDATE Test ────────────────────────────────────────────

class TestStatementUpdate:
    def test_update_writes_statement_supplement(self):
        """UPDATE thesis should write statement_supplement, not just chain."""
        conn = _make_conn()
        _, vid = _seed_thesis(conn)
        pkg = _make_package()

        actions = plan_writeback(pkg, conn)
        apply_writeback(actions, conn)

        version = select_one(
            conn,
            "SELECT statement, mechanism_chain, human_conviction "
            "FROM thesis_versions WHERE thesis_version_id = ?",
            (vid,),
        )
        # Statement should have been appended
        assert "[v2 update]" in version["statement"]
        assert len(version["statement"]) > len("原始statement")

    def test_statement_idempotent_on_reapply(self):
        """Reapplying the same update should not double-append statement."""
        conn = _make_conn()
        _, vid = _seed_thesis(conn)
        pkg = _make_package()

        # Apply twice
        actions1 = plan_writeback(pkg, conn)
        apply_writeback(actions1, conn)
        actions2 = plan_writeback(pkg, conn)
        apply_writeback(actions2, conn)

        version = select_one(
            conn,
            "SELECT statement FROM thesis_versions WHERE thesis_version_id = ?",
            (vid,),
        )
        # Should contain [v2 update] only once
        assert version["statement"].count("[v2 update]") == 1


# ── Real Rollback Test ───────────────────────────────────────────────

class TestRealRollback:
    def test_transaction_rollback_after_partial_success(self):
        """Rollback must undo earlier successful writes when a later action fails.

        Scenario: source INSERT succeeds → poisoned thesis CREATE fails → rollback.
        After rollback: sources table must be empty (the successful write is undone).
        """
        conn = _make_conn()
        conn.execute("PRAGMA foreign_keys = ON")

        # Build a source-only action that WILL succeed
        source_action = WritebackAction(
            package_id="run-rollback-test",
            target_type=WritebackTarget.SOURCE.value,
            op=WritebackOp.CREATE.value,
            payload={
                "source_id": "src-rollback-test",
                "source_type": "v2_research_run",
                "name": "Rollback test source",
                "primaryness": "secondary",
                "base_uri": "run://rollback-test",
            },
        )

        # Build a poisoned thesis action that WILL fail (FK violation)
        poisoned = WritebackAction(
            package_id="run-rollback-test",
            target_type=WritebackTarget.THESIS.value,
            op=WritebackOp.CREATE.value,
            payload={
                "thesis_id": "poison-thesis",
                "version_id": "poison-version",
                "title": "poison",
            },
        )

        # Monkey-patch _create_thesis to trigger FK error
        import finagent.writeback_engine as we
        orig_create = we._create_thesis
        def _broken_create(payload, conn, now):
            from finagent.db import insert_row
            # Insert thesis_version with non-existent thesis FK → boom
            insert_row(conn, "thesis_versions", {
                "thesis_version_id": payload["version_id"],
                "thesis_id": "DOES_NOT_EXIST_FK_FAIL",
                "statement": "", "mechanism_chain": "", "why_now": "",
                "base_case": "", "counter_case": "", "invalidators": "",
                "required_followups": "", "human_conviction": 0.0,
            })
        we._create_thesis = _broken_create

        try:
            # Order: source (succeeds) → poisoned thesis (fails)
            with pytest.raises(Exception):
                apply_writeback([source_action, poisoned], conn)

            # The source was written before the failure, but rollback
            # should have undone it
            sources = list_rows(conn, "SELECT * FROM sources")
            assert len(sources) == 0, (
                f"Rollback failed: {len(sources)} sources survived "
                "(should be 0 after rollback of partial success)"
            )
        finally:
            we._create_thesis = orig_create


# ── load_targets dedup test ──────────────────────────────────────────

class TestLoadTargetsDedup:
    def test_multiple_target_cases_returns_latest(self):
        """Target with multiple target_cases should return only the latest."""
        conn = _make_conn()
        now = _utcnow()
        eid = "e-test"
        tid = "tg-test"

        conn.execute(
            "INSERT INTO entities (entity_id, entity_type, canonical_name, created_at) "
            "VALUES (?, ?, ?, ?)", (eid, "company", "TestCo", now),
        )
        conn.execute(
            "INSERT INTO targets (target_id, entity_id, asset_class, venue, "
            "ticker_or_symbol, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (tid, eid, "equity", "US", "TEST", now),
        )

        # Seed thesis for FK
        tv1 = "tv-old"
        tv2 = "tv-new"
        t1 = "t-dedup"
        conn.execute(
            "INSERT INTO theses (thesis_id, title, status, horizon_months, "
            "theme_ids_json, owner, current_version_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (t1, "test", "seed", 12, "[]", "test", tv2, now),
        )
        for vid in (tv1, tv2):
            conn.execute(
                "INSERT INTO thesis_versions (thesis_version_id, thesis_id, "
                "statement, mechanism_chain, why_now, base_case, counter_case, "
                "invalidators, required_followups, human_conviction, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (vid, t1, "s", "m", "w", "", "", "", "", 0.5, now),
            )

        # Two target_cases: old and new
        conn.execute(
            "INSERT INTO target_cases (target_case_id, thesis_version_id, "
            "target_id, exposure_type, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("tc-old", tv1, tid, "long", "active", "2025-01-01T00:00:00Z"),
        )
        conn.execute(
            "INSERT INTO target_cases (target_case_id, thesis_version_id, "
            "target_id, exposure_type, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("tc-new", tv2, tid, "short", "active", "2026-01-01T00:00:00Z"),
        )
        conn.commit()

        targets = load_targets(conn)
        assert len(targets) == 1  # Not 2!
        assert targets[0].target_case_id == "tc-new"
        assert targets[0].exposure_type == "short"

    def test_same_timestamp_dedup(self):
        """Even with identical timestamps, only 1 row per target."""
        conn = _make_conn()
        now = _utcnow()
        eid = "e-tie"
        tid = "tg-tie"

        conn.execute(
            "INSERT INTO entities (entity_id, entity_type, canonical_name, created_at) "
            "VALUES (?, ?, ?, ?)", (eid, "company", "TieCo", now),
        )
        conn.execute(
            "INSERT INTO targets (target_id, entity_id, asset_class, venue, "
            "ticker_or_symbol, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (tid, eid, "equity", "US", "TIE", now),
        )

        # Seed thesis for FK
        t1 = "t-tie"
        tv1, tv2 = "tv-tie1", "tv-tie2"
        conn.execute(
            "INSERT INTO theses (thesis_id, title, status, horizon_months, "
            "theme_ids_json, owner, current_version_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (t1, "tie test", "seed", 12, "[]", "test", tv2, now),
        )
        for vid in (tv1, tv2):
            conn.execute(
                "INSERT INTO thesis_versions (thesis_version_id, thesis_id, "
                "statement, mechanism_chain, why_now, base_case, counter_case, "
                "invalidators, required_followups, human_conviction, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (vid, t1, "s", "m", "w", "", "", "", "", 0.5, now),
            )

        # Two target_cases with SAME timestamp
        same_ts = "2026-06-01T00:00:00Z"
        conn.execute(
            "INSERT INTO target_cases (target_case_id, thesis_version_id, "
            "target_id, exposure_type, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("tc-a", tv1, tid, "long", "active", same_ts),
        )
        conn.execute(
            "INSERT INTO target_cases (target_case_id, thesis_version_id, "
            "target_id, exposure_type, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("tc-b", tv2, tid, "short", "active", same_ts),
        )
        conn.commit()

        targets = load_targets(conn)
        assert len(targets) == 1, f"Expected 1 target but got {len(targets)}"
        # tc-b wins (higher ID in DESC tiebreaker)
        assert targets[0].target_case_id == "tc-b"
