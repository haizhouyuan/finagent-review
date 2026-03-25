"""Tests for finagent.agents.evidence_store — provenance fields + migration."""
from __future__ import annotations

import os
import sqlite3
import tempfile

from finagent.agents.evidence_store import EvidenceStore


class TestEvidenceStoreMigration:
    """Verify that new provenance columns are added to existing databases."""

    def test_new_db_has_provenance_columns(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.db")
            store = EvidenceStore(db)
            cols = {
                row[1]
                for row in store.conn.execute(
                    "PRAGMA table_info(evidence_store)"
                ).fetchall()
            }
            assert "source_tier" in cols
            assert "source_uri" in cols
            assert "published_at" in cols
            store.close()

    def test_migration_adds_missing_columns(self):
        """Simulate an old database without provenance columns."""
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "old.db")
            # Create table with old schema (no provenance columns)
            conn = sqlite3.connect(db_path)
            conn.execute("""
                CREATE TABLE evidence_store (
                    evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query TEXT NOT NULL,
                    raw_text TEXT NOT NULL,
                    char_count INTEGER NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'web_search',
                    created_at TEXT NOT NULL
                )
            """)
            conn.commit()
            conn.close()

            # Now open with EvidenceStore — should migrate
            store = EvidenceStore(db_path)
            cols = {
                row[1]
                for row in store.conn.execute(
                    "PRAGMA table_info(evidence_store)"
                ).fetchall()
            }
            assert "source_tier" in cols
            assert "source_uri" in cols
            assert "published_at" in cols
            store.close()


class TestEvidenceStoreProvenance:
    def test_store_with_provenance(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.db")
            store = EvidenceStore(db)

            ref = store.store(
                query="AI供应链",
                raw_text="蓝箭航天完成B轮融资" * 10,
                source_type="web_search",
                source_tier="secondary",
                source_uri="https://news.example.com/article/123",
                published_at="2025-03-15",
            )

            assert ref["source_tier"] == "secondary"
            assert ref["source_uri"] == "https://news.example.com/article/123"
            assert ref["published_at"] == "2025-03-15"
            assert ref["evidence_id"] is not None
            assert ref["char_count"] > 0

            # Verify raw text retrievable
            text = store.fetch(ref["evidence_id"])
            assert "蓝箭航天" in text

            store.close()

    def test_store_backward_compat(self):
        """store() without new params defaults to 'unverified'."""
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.db")
            store = EvidenceStore(db)

            ref = store.store(query="test", raw_text="hello world" * 10)
            assert ref["source_tier"] == "unverified"
            assert ref["source_uri"] == ""
            assert ref["published_at"] == ""

            store.close()

    def test_multiple_stores(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.db")
            store = EvidenceStore(db)

            r1 = store.store("q1", "text1" * 20, source_tier="primary")
            r2 = store.store("q2", "text2" * 20, source_tier="aggregated")

            assert r1["evidence_id"] != r2["evidence_id"]
            assert r1["source_tier"] == "primary"
            assert r2["source_tier"] == "aggregated"

            store.close()

    def test_search_whole_query_and_no_raw_text_in_result(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.db")
            store = EvidenceStore(db)
            store.store(
                "九号供应链",
                "九号两轮车正在强化铝轮毂供应链，金谷是核心供应商。",
                source_tier="secondary",
            )

            results = store.search("九号两轮车正在强化铝轮毂供应链")
            assert len(results) == 1
            assert "raw_text" not in results[0]
            assert results[0]["query"] == "九号供应链"
            assert results[0]["_score"] >= 1.0
            store.close()

    def test_search_cjk_token_fallback(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.db")
            store = EvidenceStore(db)
            store.store(
                "九号铝轮毂",
                "九号 Fz 系列采用铝合金轮毂，并由金谷负责供应体系。",
            )
            store.store(
                "无关结果",
                "商业航天发射服务与两轮车无关。",
            )

            results = store.search("九号铝轮毂供应链")
            assert len(results) >= 1
            assert results[0]["query"] == "九号铝轮毂"
            store.close()
