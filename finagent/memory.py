"""Two-wheeler domain memory: episodic capture + semantic promotion."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from finagent.db import connect, init_db
from finagent.paths import ensure_runtime_dirs, resolve_paths


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class MemoryTier(str, Enum):
    WORKING = "working"
    EPISODIC = "episodic"
    SEMANTIC = "semantic"


WORKING_CATEGORY = "working_note"

EPISODIC_CATEGORIES = {
    "brand_observation",
    "product_spec",
    "price_change",
    "market_event",
    "field_research",
    "competitor_move",
    "supply_chain",
    "research_finding",
}

SEMANTIC_CATEGORIES = {
    "brand_positioning",
    "market_structure",
    "price_band",
    "technology_trend",
    "supply_chain_map",
    "regulatory",
}

_MEMORY_DDL = """\
CREATE TABLE IF NOT EXISTS memory_records (
    record_id TEXT PRIMARY KEY,
    tier TEXT NOT NULL,
    category TEXT NOT NULL,
    content TEXT NOT NULL,
    structured_data_json TEXT NOT NULL DEFAULT '{}',
    source_run_id TEXT NOT NULL DEFAULT '',
    source_type TEXT NOT NULL DEFAULT 'extractor',
    confidence REAL NOT NULL DEFAULT 0.7,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    access_count INTEGER NOT NULL DEFAULT 0,
    promoted_from TEXT
);

CREATE INDEX IF NOT EXISTS idx_memory_records_tier
    ON memory_records(tier);
CREATE INDEX IF NOT EXISTS idx_memory_records_category_tier
    ON memory_records(category, tier);
CREATE INDEX IF NOT EXISTS idx_memory_records_source_run
    ON memory_records(source_run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_semantic_unique
    ON memory_records(tier, category, content)
    WHERE tier = 'semantic';
"""


@dataclass(frozen=True)
class MemoryRecord:
    record_id: str
    tier: MemoryTier
    category: str
    content: str
    structured_data: dict[str, Any]
    source_run_id: str
    source_type: str
    confidence: float
    created_at: str
    updated_at: str
    access_count: int = 0
    promoted_from: str | None = None


class MemoryManager:
    """Backed by memory_records table in state/finagent.sqlite."""

    def __init__(
        self,
        conn: sqlite3.Connection | None = None,
        *,
        db_path: str | Path | None = None,
    ):
        self._owns_conn = conn is None
        if conn is None:
            if db_path is None:
                paths = resolve_paths()
                ensure_runtime_dirs(paths)
                db_path = paths.db_path
            self.conn = connect(Path(db_path))
            init_db(self.conn)
        else:
            self.conn = conn
            self.conn.row_factory = sqlite3.Row
        self._ensure_table()

    def close(self) -> None:
        if self._owns_conn:
            self.conn.close()

    def _ensure_table(self) -> None:
        self.conn.executescript(_MEMORY_DDL)
        self.conn.commit()

    def store_episodic(
        self,
        category: str,
        content: str,
        *,
        run_id: str,
        source_type: str = "extractor",
        confidence: float = 0.7,
        structured_data: dict[str, Any] | None = None,
    ) -> str:
        self._validate_category(category, MemoryTier.EPISODIC)
        return self._insert_record(
            tier=MemoryTier.EPISODIC,
            category=category,
            content=content,
            structured_data=structured_data,
            source_run_id=run_id,
            source_type=source_type,
            confidence=confidence,
        )

    def store_working(self, content: str, *, run_id: str) -> str:
        return self._insert_record(
            tier=MemoryTier.WORKING,
            category=WORKING_CATEGORY,
            content=content,
            structured_data=None,
            source_run_id=run_id,
            source_type="orchestrator",
            confidence=0.5,
        )

    def recall(
        self,
        query: str,
        *,
        tier: MemoryTier | str | None = None,
        limit: int = 10,
    ) -> list[MemoryRecord]:
        clauses: list[str] = []
        params: list[Any] = []

        if tier is not None:
            normalized_tier = self._normalize_tier(tier)
            clauses.append("tier = ?")
            params.append(normalized_tier.value)

        query = query.strip()
        if query:
            clauses.append("(content LIKE ? OR structured_data_json LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.conn.execute(
            f"""
            SELECT * FROM memory_records
            {where_sql}
            ORDER BY confidence DESC, updated_at DESC, created_at DESC
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()

        records = [self._row_to_record(row) for row in rows]
        self._bump_access([record.record_id for record in records])
        records = [
            MemoryRecord(
                **{
                    **record.__dict__,
                    "access_count": record.access_count + 1,
                },
            )
            for record in records
        ]
        return records

    def get_by_category(
        self,
        category: str,
        *,
        tier: MemoryTier | str | None = None,
    ) -> list[MemoryRecord]:
        clauses = ["category = ?"]
        params: list[Any] = [category]
        if tier is not None:
            normalized_tier = self._normalize_tier(tier)
            clauses.append("tier = ?")
            params.append(normalized_tier.value)
        rows = self.conn.execute(
            f"""
            SELECT * FROM memory_records
            WHERE {' AND '.join(clauses)}
            ORDER BY confidence DESC, updated_at DESC, created_at DESC
            """,
            params,
        ).fetchall()
        records = [self._row_to_record(row) for row in rows]
        self._bump_access([record.record_id for record in records])
        records = [
            MemoryRecord(
                **{
                    **record.__dict__,
                    "access_count": record.access_count + 1,
                },
            )
            for record in records
        ]
        return records

    def count_by_tier(self) -> dict[str, int]:
        counts = {tier.value: 0 for tier in MemoryTier}
        rows = self.conn.execute(
            "SELECT tier, COUNT(*) AS total FROM memory_records GROUP BY tier"
        ).fetchall()
        for row in rows:
            counts[row["tier"]] = int(row["total"])
        return counts

    def promote_to_semantic(
        self,
        episodic_ids: list[str],
        semantic_content: str,
        semantic_category: str,
        confidence: float,
        *,
        structured_data: dict[str, Any] | None = None,
        supersedes: str | None = None,
    ) -> str:
        self._validate_category(semantic_category, MemoryTier.SEMANTIC)
        existing = self.conn.execute(
            """
            SELECT record_id
            FROM memory_records
            WHERE tier = ? AND category = ? AND content = ?
            """,
            (MemoryTier.SEMANTIC.value, semantic_category, semantic_content),
        ).fetchone()
        if existing:
            return str(existing["record_id"])

        source_run_id = ""
        if episodic_ids:
            placeholders = ",".join("?" for _ in episodic_ids)
            row = self.conn.execute(
                f"""
                SELECT source_run_id
                FROM memory_records
                WHERE record_id IN ({placeholders})
                ORDER BY created_at ASC
                LIMIT 1
                """,
                episodic_ids,
            ).fetchone()
            source_run_id = row["source_run_id"] if row else ""

        payload = dict(structured_data or {})
        payload.setdefault("evidence_ids", list(episodic_ids))
        if supersedes:
            payload.setdefault("supersedes", supersedes)

        return self._insert_record(
            tier=MemoryTier.SEMANTIC,
            category=semantic_category,
            content=semantic_content,
            structured_data=payload,
            source_run_id=source_run_id,
            source_type="promotion",
            confidence=confidence,
            promoted_from=",".join(episodic_ids) if episodic_ids else None,
        )

    def expire_working(self) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        cursor = self.conn.execute(
            """
            DELETE FROM memory_records
            WHERE tier = ? AND created_at < ?
            """,
            (MemoryTier.WORKING.value, cutoff),
        )
        self.conn.commit()
        return int(cursor.rowcount or 0)

    def _insert_record(
        self,
        *,
        tier: MemoryTier,
        category: str,
        content: str,
        structured_data: dict[str, Any] | None,
        source_run_id: str,
        source_type: str,
        confidence: float,
        promoted_from: str | None = None,
    ) -> str:
        record_id = f"mem-{uuid.uuid4().hex[:12]}"
        now = _utcnow()
        self.conn.execute(
            """
            INSERT INTO memory_records (
                record_id, tier, category, content, structured_data_json,
                source_run_id, source_type, confidence, created_at,
                updated_at, access_count, promoted_from
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                record_id,
                tier.value,
                category,
                content,
                json.dumps(structured_data or {}, ensure_ascii=False),
                source_run_id,
                source_type,
                float(confidence),
                now,
                now,
                promoted_from,
            ),
        )
        self.conn.commit()
        return record_id

    def _bump_access(self, record_ids: list[str]) -> None:
        if not record_ids:
            return
        placeholders = ",".join("?" for _ in record_ids)
        self.conn.execute(
            f"""
            UPDATE memory_records
            SET access_count = access_count + 1,
                updated_at = ?
            WHERE record_id IN ({placeholders})
            """,
            [_utcnow(), *record_ids],
        )
        self.conn.commit()

    def _row_to_record(self, row: sqlite3.Row) -> MemoryRecord:
        tier_value = row["tier"]
        try:
            tier = MemoryTier(tier_value)
        except ValueError:
            tier = MemoryTier.EPISODIC
        return MemoryRecord(
            record_id=row["record_id"],
            tier=tier,
            category=row["category"],
            content=row["content"],
            structured_data=json.loads(row["structured_data_json"] or "{}"),
            source_run_id=row["source_run_id"] or "",
            source_type=row["source_type"] or "",
            confidence=float(row["confidence"] or 0.0),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            access_count=int(row["access_count"] or 0),
            promoted_from=row["promoted_from"],
        )

    def _validate_category(self, category: str, tier: MemoryTier) -> None:
        allowed = (
            EPISODIC_CATEGORIES
            if tier is MemoryTier.EPISODIC
            else SEMANTIC_CATEGORIES
        )
        if category not in allowed:
            raise ValueError(f"unsupported {tier.value} category: {category}")

    def _normalize_tier(self, tier: MemoryTier | str) -> MemoryTier:
        if isinstance(tier, MemoryTier):
            return tier
        return MemoryTier(tier)
