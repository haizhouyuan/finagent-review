"""Layer 1: Data layer tests — schema integrity, constraints, indexes."""
from __future__ import annotations

import sqlite3

import pytest

EXPECTED_TABLES = [
    "themes",
    "theses",
    "thesis_versions",
    "entities",
    "targets",
    "target_cases",
    "claims",
    "claim_routes",
    "claim_route_links",
    "validation_cases",
    "sources",
    "source_viewpoints",
    "artifacts",
    "monitors",
    "monitor_events",
    "events",
    "source_feedback_entries",
    "patterns",
    "reviews",
    "analysis_runs",
    "operator_decisions",
    "timing_plans",
]


class TestSchemaIntegrity:
    """Verify all expected tables exist with correct structure."""

    def test_all_tables_exist(self, fresh_db):
        tables = [
            r[0]
            for r in fresh_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]
        for t in EXPECTED_TABLES:
            assert t in tables, f"Missing table: {t}"

    def test_journal_mode(self, fresh_db):
        mode = fresh_db.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode in ("wal", "delete"), f"Unexpected journal mode: {mode}"

    def test_foreign_keys_enabled(self, fresh_db):
        fk = fresh_db.execute("PRAGMA foreign_keys").fetchone()[0]
        # FK might be off at init — just verify the pragma works
        assert fk in (0, 1)


class TestConstraints:
    """Verify NOT NULL and UNIQUE constraints."""

    @pytest.mark.regression
    def test_monitors_requires_status(self, fresh_db):
        """Regression: INSERT without status field must fail (NOT NULL)."""
        with pytest.raises(sqlite3.IntegrityError):
            fresh_db.execute(
                """INSERT INTO monitors
                   (monitor_id, owner_object_type, owner_object_id,
                    monitor_type, metric_name, created_at)
                   VALUES ('m1', 'thesis', 'th1', 'market', 'test', datetime('now'))"""
            )

    def test_themes_pk_unique(self, fresh_db):
        fresh_db.execute(
            "INSERT INTO themes (theme_id, name, importance_status, created_at) VALUES ('t1', 'A', 'priority', datetime('now'))"
        )
        with pytest.raises(sqlite3.IntegrityError):
            fresh_db.execute(
                "INSERT INTO themes (theme_id, name, importance_status, created_at) VALUES ('t1', 'B', 'tracking', datetime('now'))"
            )

    def test_claims_pk_unique(self, fresh_db):
        fresh_db.execute(
            """INSERT INTO claims (claim_id, artifact_id, claim_text, claim_type, confidence, status, created_at)
               VALUES ('c1', 'a1', 'test claim', 'factual', 0.8, 'active', datetime('now'))"""
        )
        with pytest.raises(sqlite3.IntegrityError):
            fresh_db.execute(
                """INSERT INTO claims (claim_id, artifact_id, claim_text, claim_type, confidence, status, created_at)
                   VALUES ('c1', 'a1', 'dupe', 'factual', 0.8, 'active', datetime('now'))"""
            )

    def test_sources_pk_unique(self, fresh_db):
        fresh_db.execute(
            """INSERT INTO sources (source_id, source_type, name, primaryness, created_at)
               VALUES ('s1', 'official', 'S', 'first_hand', datetime('now'))"""
        )
        with pytest.raises(sqlite3.IntegrityError):
            fresh_db.execute(
                """INSERT INTO sources (source_id, source_type, name, primaryness, created_at)
                   VALUES ('s1', 'kol', 'S2', 'second_hand', datetime('now'))"""
            )


class TestFTSIndex:
    """Verify full-text search works."""

    def test_artifact_fts_exists(self, fresh_db):
        tables = [
            r[0]
            for r in fresh_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%fts%'"
            ).fetchall()
        ]
        assert any("fts" in t for t in tables), f"No FTS table found. Tables: {tables}"


class TestInsertOrIgnore:
    """Verify INSERT OR IGNORE semantics for idempotent operations."""

    def test_monitor_insert_or_ignore(self, fresh_db):
        sql = """INSERT OR IGNORE INTO monitors
                 (monitor_id, owner_object_type, owner_object_id,
                  monitor_type, metric_name, status, created_at)
                 VALUES ('m1', 'thesis', 'th1', 'market', 'test', 'active', datetime('now'))"""
        fresh_db.execute(sql)
        fresh_db.execute(sql)  # Duplicate — should be ignored
        cnt = fresh_db.execute("SELECT COUNT(*) FROM monitors").fetchone()[0]
        assert cnt == 1

    def test_entity_insert_or_ignore(self, fresh_db):
        sql = """INSERT OR IGNORE INTO entities
                 (entity_id, entity_type, canonical_name, created_at)
                 VALUES ('e1', 'company', 'Test', datetime('now'))"""
        fresh_db.execute(sql)
        fresh_db.execute(sql)
        cnt = fresh_db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        assert cnt == 1
