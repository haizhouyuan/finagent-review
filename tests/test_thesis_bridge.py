"""Tests for thesis_bridge — read-only v1 → v2 bridge.

Uses in-memory SQLite seeded with v1 schema + fixture data.
"""
from __future__ import annotations

import sqlite3
import json
import uuid

from finagent.db import SCHEMA_SQL
from finagent.thesis_bridge import (
    ThesisView, TargetView, ClaimView,
    load_theses, load_targets, load_claims,
    get_thesis, find_matching_thesis,
)


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _make_conn() -> sqlite3.Connection:
    """Create in-memory v1 DB with schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def _seed_thesis(conn, thesis_id="t1", title="商业航天投资论文",
                 status="framed", version_statement="SpaceX引领商业航天变革"):
    """Seed a thesis with one version."""
    now = _utcnow()
    vid = f"tv-{uuid.uuid4().hex[:8]}"
    conn.execute(
        "INSERT INTO theses (thesis_id, title, status, horizon_months, "
        "theme_ids_json, owner, current_version_id, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (thesis_id, title, status, 12, json.dumps(["th1"]), "user", vid, now),
    )
    conn.execute(
        "INSERT INTO thesis_versions (thesis_version_id, thesis_id, "
        "statement, mechanism_chain, why_now, base_case, counter_case, "
        "invalidators, required_followups, human_conviction, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (vid, thesis_id, version_statement, "低成本发射→商业化", "市场加速",
         "base case text", "counter case text", "技术风险", "", 0.75, now),
    )
    conn.commit()
    return thesis_id, vid


def _seed_target(conn, entity_name="蓝箭航天", ticker="LANDSPACE",
                 thesis_version_id=None):
    """Seed entity + target + optional target_case."""
    now = _utcnow()
    eid = f"e-{uuid.uuid4().hex[:8]}"
    tid = f"tg-{uuid.uuid4().hex[:8]}"

    conn.execute(
        "INSERT INTO entities (entity_id, entity_type, canonical_name, "
        "created_at) VALUES (?, ?, ?, ?)",
        (eid, "company", entity_name, now),
    )
    conn.execute(
        "INSERT INTO targets (target_id, entity_id, asset_class, venue, "
        "ticker_or_symbol, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (tid, eid, "equity", "HK", ticker, now),
    )

    tcid = ""
    if thesis_version_id:
        tcid = f"tc-{uuid.uuid4().hex[:8]}"
        conn.execute(
            "INSERT INTO target_cases (target_case_id, thesis_version_id, "
            "target_id, exposure_type, capture_link_strength, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (tcid, thesis_version_id, tid, "long", 0.8, "active", now),
        )

    conn.commit()
    return tid, eid, tcid


def _seed_claim(conn, text="蓝箭航天2025年完成10次发射"):
    """Seed source + artifact + claim."""
    now = _utcnow()
    sid = f"s-{uuid.uuid4().hex[:8]}"
    aid = f"a-{uuid.uuid4().hex[:8]}"
    cid = f"c-{uuid.uuid4().hex[:8]}"

    conn.execute(
        "INSERT INTO sources (source_id, source_type, name, primaryness, "
        "created_at) VALUES (?, ?, ?, ?, ?)",
        (sid, "web", "test_source", "secondary", now),
    )
    conn.execute(
        "INSERT INTO artifacts (artifact_id, source_id, artifact_kind, "
        "title, captured_at, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (aid, sid, "article", "test article", now, "processed", now),
    )
    conn.execute(
        "INSERT INTO claims (claim_id, artifact_id, claim_text, claim_type, "
        "confidence, review_status, status, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (cid, aid, text, "factual", 0.9, "unreviewed", "active", now),
    )
    conn.commit()
    return cid


# ── Tests ────────────────────────────────────────────────────────────

class TestLoadTheses:
    def test_empty_db(self):
        conn = _make_conn()
        assert load_theses(conn) == []

    def test_loads_thesis_with_version(self):
        conn = _make_conn()
        _seed_thesis(conn, thesis_id="t1", title="商业航天")
        theses = load_theses(conn)
        assert len(theses) == 1
        t = theses[0]
        assert isinstance(t, ThesisView)
        assert t.thesis_id == "t1"
        assert t.title == "商业航天"
        assert t.status == "framed"
        assert t.statement == "SpaceX引领商业航天变革"
        assert t.mechanism_chain == "低成本发射→商业化"
        assert t.conviction == 0.75
        assert "th1" in t.theme_ids

    def test_multiple_theses(self):
        conn = _make_conn()
        _seed_thesis(conn, "t1", "航天")
        _seed_thesis(conn, "t2", "新能源")
        assert len(load_theses(conn)) == 2


class TestLoadTargets:
    def test_empty_db(self):
        conn = _make_conn()
        assert load_targets(conn) == []

    def test_target_with_entity(self):
        conn = _make_conn()
        _, vid = _seed_thesis(conn)
        tid, eid, tcid = _seed_target(conn, "蓝箭航天", "LANDSPACE", vid)
        targets = load_targets(conn)
        assert len(targets) == 1
        tg = targets[0]
        assert isinstance(tg, TargetView)
        assert tg.entity_name == "蓝箭航天"
        assert tg.ticker == "LANDSPACE"
        assert tg.exposure_type == "long"
        assert tg.capture_link_strength == 0.8

    def test_target_without_case(self):
        conn = _make_conn()
        _seed_target(conn, "SpaceX", "SPACEX")
        targets = load_targets(conn)
        assert len(targets) == 1
        assert targets[0].target_case_id == ""


class TestLoadClaims:
    def test_empty_db(self):
        conn = _make_conn()
        assert load_claims(conn) == []

    def test_loads_claims(self):
        conn = _make_conn()
        cid = _seed_claim(conn, "蓝箭航天发射")
        claims = load_claims(conn)
        assert len(claims) == 1
        assert claims[0].claim_text == "蓝箭航天发射"
        assert claims[0].confidence == 0.9

    def test_filter_by_review_status(self):
        conn = _make_conn()
        _seed_claim(conn, "claim1")
        claims = load_claims(conn, review_status="reviewed")
        assert len(claims) == 0
        claims = load_claims(conn, review_status="unreviewed")
        assert len(claims) == 1


class TestGetThesis:
    def test_existing(self):
        conn = _make_conn()
        _seed_thesis(conn, "t1", "航天")
        t = get_thesis(conn, "t1")
        assert t is not None
        assert t.thesis_id == "t1"

    def test_not_found(self):
        conn = _make_conn()
        assert get_thesis(conn, "nonexistent") is None


class TestFindMatchingThesis:
    def test_matches_title(self):
        conn = _make_conn()
        _seed_thesis(conn, "t1", "商业航天核心供应商竞争格局")
        result = find_matching_thesis(conn, "分析商业航天核心供应商")
        assert result is not None
        assert result.thesis_id == "t1"

    def test_context_boost(self):
        conn = _make_conn()
        _seed_thesis(conn, "t1", "商业航天供应商")
        _seed_thesis(conn, "t2", "新能源汽车电池")
        result = find_matching_thesis(conn, "供应商分析", context="商业航天")
        assert result is not None
        assert result.thesis_id == "t1"

    def test_no_match(self):
        conn = _make_conn()
        _seed_thesis(conn, "t1", "航天")
        result = find_matching_thesis(conn, "完全不相关的量子计算主题")
        assert result is None

    def test_skips_archived(self):
        conn = _make_conn()
        _seed_thesis(conn, "t1", "商业航天核心供应商竞争格局", status="archived")
        result = find_matching_thesis(conn, "分析商业航天核心供应商")
        assert result is None
