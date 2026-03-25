"""Evidence Store — local storage for raw search results.

Implements the "reference passing" pattern: raw text is stored in
SQLite and only lightweight metadata (evidence_id + char count) flows
through the LangGraph State.

This prevents the state dict from ballooning with multi-KB raw text
on every iteration — the #1 cause of Token budget blow-ups.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_UTCNOW = lambda: datetime.now(timezone.utc).isoformat()

_BRAND_TERMS = {
    "雅迪", "爱玛", "九号", "台铃", "小牛", "新日", "绿源", "金谷", "春风",
}
_COMPONENT_TERMS = {
    "铝轮毂", "铝合金轮毂", "钢轮毂", "轮毂", "轮毬", "车架", "电机", "电池",
    "控制器", "制动", "灯具",
}
_DOMAIN_DICT = _BRAND_TERMS | _COMPONENT_TERMS | {
    "供应链", "竞争", "新国标", "石墨烯", "钠离子", "两轮车", "电动车",
}
_TERM_SYNONYMS = {
    "铝轮": ("铝轮毂", "铝合金轮毂"),
    "铝轮毂": ("铝合金轮毂", "轮毂"),
    "轮毂": ("铝轮毂", "钢轮毂"),
    "供应链": ("供应商", "配套"),
    "供应商": ("配套",),
    "九号": ("ninebot", "segway"),
    "雅迪": ("yadea",),
    "爱玛": ("aima",),
    "台铃": ("tailg",),
    "小牛": ("niu",),
}

_EVIDENCE_DDL = """\
CREATE TABLE IF NOT EXISTS evidence_store (
    evidence_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    query        TEXT NOT NULL,
    raw_text     TEXT NOT NULL,
    char_count   INTEGER NOT NULL,
    source_type  TEXT NOT NULL DEFAULT 'web_search',
    source_tier  TEXT NOT NULL DEFAULT 'unverified',
    source_uri   TEXT NOT NULL DEFAULT '',
    published_at TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL
);
"""

# Migration columns for databases created before the provenance enhancement
_EVIDENCE_MIGRATIONS: list[tuple[str, str]] = [
    ("source_tier", "TEXT NOT NULL DEFAULT 'unverified'"),
    ("source_uri", "TEXT NOT NULL DEFAULT ''"),
    ("published_at", "TEXT NOT NULL DEFAULT ''"),
    ("run_id", "TEXT NOT NULL DEFAULT ''"),
]


def _dedupe_keep_order(tokens: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for token in tokens:
        cleaned = token.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def _tokenize_cjk(query: str) -> list[str]:
    """CJK-aware tokenization: dictionary match first, then bigrams."""
    tokens: list[str] = []
    remaining = query.strip()

    for term in sorted(_DOMAIN_DICT, key=len, reverse=True):
        while term in remaining:
            tokens.append(term)
            remaining = remaining.replace(term, " ", 1)

    for segment in remaining.split():
        segment = segment.strip()
        if len(segment) < 2:
            continue
        if segment.isascii():
            tokens.append(segment)
            continue
        for idx in range(len(segment) - 1):
            tokens.append(segment[idx:idx + 2])
        if len(segment) >= 3:
            for idx in range(len(segment) - 2):
                tokens.append(segment[idx:idx + 3])

    expanded: list[str] = []
    for token in tokens:
        expanded.append(token)
        for synonym in _TERM_SYNONYMS.get(token, ()):
            expanded.append(synonym)

    compact = "".join(ch for ch in query if not ch.isspace())
    if compact and compact != query:
        expanded.append(compact)

    tokens = _dedupe_keep_order(expanded)
    return tokens if tokens else [query]


class EvidenceStore:
    """Local SQLite-backed store for raw search results.

    Only metadata (evidence_id, query, char_count) should flow into
    the LangGraph state.  The Extractor fetches the actual text
    from this store by ID when it needs it.
    """

    def __init__(self, db_path: str | Path | None = None):
        if db_path is None:
            from finagent.paths import resolve_paths
            db_path = resolve_paths().state_dir / "evidence.db"
        self._db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self.active_run_id: str = ""   # Set by orchestrator for run-scoped tagging
        self._ensure_table()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
        return self._conn

    def _ensure_table(self) -> None:
        self.conn.executescript(_EVIDENCE_DDL)
        # Migrate existing databases: add new columns if missing
        existing = {
            row[1] for row in
            self.conn.execute("PRAGMA table_info(evidence_store)").fetchall()
        }
        for col_name, col_sql in _EVIDENCE_MIGRATIONS:
            if col_name not in existing:
                self.conn.execute(
                    f"ALTER TABLE evidence_store ADD COLUMN {col_name} {col_sql}"
                )
        self.conn.commit()

    def store(
        self,
        query: str,
        raw_text: str,
        source_type: str = "web_search",
        source_tier: str = "unverified",
        source_uri: str = "",
        published_at: str = "",
    ) -> dict[str, Any]:
        """Store raw text and return lightweight metadata dict.

        The returned dict is what goes into the LangGraph state —
        it contains ONLY the ID and provenance metadata, NOT the raw text.
        Compatible with ``EvidenceRef.to_dict()`` from research_contracts.
        """
        now = _UTCNOW()
        run_id = self.active_run_id or ""
        cursor = self.conn.execute(
            """
            INSERT INTO evidence_store
                (query, raw_text, char_count, source_type,
                 source_tier, source_uri, published_at, run_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (query, raw_text, len(raw_text), source_type,
             source_tier, source_uri, published_at, run_id, now),
        )
        self.conn.commit()
        eid = cursor.lastrowid

        return {
            "evidence_id": eid,
            "query": query,
            "char_count": len(raw_text),
            "source_type": source_type,
            "source_tier": source_tier,
            "source_uri": source_uri,
            "published_at": published_at,
        }

    def fetch(self, evidence_id: int) -> str:
        """Fetch raw text by evidence_id."""
        row = self.conn.execute(
            "SELECT raw_text FROM evidence_store WHERE evidence_id = ?",
            (evidence_id,),
        ).fetchone()
        return row["raw_text"] if row else ""

    def fetch_batch(self, evidence_ids: list[int]) -> dict[int, str]:
        """Fetch multiple evidence texts at once."""
        if not evidence_ids:
            return {}
        placeholders = ",".join("?" * len(evidence_ids))
        rows = self.conn.execute(
            f"SELECT evidence_id, raw_text FROM evidence_store WHERE evidence_id IN ({placeholders})",
            evidence_ids,
        ).fetchall()
        return {row["evidence_id"]: row["raw_text"] for row in rows}

    def list_all(self, *, run_id: str | None = None) -> list[dict[str, Any]]:
        """List evidence entries (metadata only, no raw text).

        If ``run_id`` is given, returns only evidence tagged for that run.
        Otherwise returns all entries.
        """
        if run_id:
            rows = self.conn.execute(
                "SELECT evidence_id, query, char_count, source_type, "
                "source_tier, source_uri, published_at "
                "FROM evidence_store WHERE run_id = ? ORDER BY evidence_id",
                (run_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT evidence_id, query, char_count, source_type, "
                "source_tier, source_uri, published_at "
                "FROM evidence_store ORDER BY evidence_id",
            ).fetchall()
        return [
            {
                "id": r["evidence_id"],
                "query": r["query"],
                "char_count": r["char_count"],
                "source_type": r["source_type"],
                "source_tier": r["source_tier"] or "unverified",
                "source_uri": r["source_uri"] or "",
                "published_at": r["published_at"] or "",
            }
            for r in rows
        ]

    def search(
        self,
        query: str,
        *,
        run_id: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """CJK-aware keyword search on raw evidence text via SQL LIKE.

        Returns metadata refs only. Raw text stays in SQLite and can be
        fetched later with ``fetch()`` / ``fetch_batch()``.
        """
        query = query.strip()
        if not query:
            return []

        base_clauses: list[str] = []
        base_params: list[Any] = []
        if run_id:
            base_clauses.append("run_id = ?")
            base_params.append(run_id)

        exact_clauses = list(base_clauses) + ["raw_text LIKE ?"]
        exact_params = list(base_params) + [f"%{query}%"]
        exact_rows = self.conn.execute(
            f"""
            SELECT evidence_id, query, char_count, source_type, source_tier,
                   source_uri, published_at, raw_text
            FROM evidence_store
            WHERE {' AND '.join(exact_clauses)}
            ORDER BY evidence_id DESC
            LIMIT ?
            """,
            exact_params + [limit],
        ).fetchall()

        if len(exact_rows) >= min(limit, 3):
            return self._format_results(exact_rows, {row["evidence_id"]: 10.0 for row in exact_rows})

        tokens = _tokenize_cjk(query)[:6]
        if not tokens:
            return self._format_results(exact_rows, {row["evidence_id"]: 10.0 for row in exact_rows})

        token_clause = " OR ".join("raw_text LIKE ?" for _ in tokens)
        token_rows = self.conn.execute(
            f"""
            SELECT evidence_id, query, char_count, source_type, source_tier,
                   source_uri, published_at, raw_text
            FROM evidence_store
            WHERE {' AND '.join(base_clauses + [f'({token_clause})'])}
            ORDER BY evidence_id DESC
            LIMIT ?
            """,
            base_params + [f"%{token}%" for token in tokens] + [max(limit * 4, 20)],
        ).fetchall()

        scored_rows: dict[int, sqlite3.Row] = {
            row["evidence_id"]: row for row in exact_rows
        }
        scores = {row["evidence_id"]: 10.0 for row in exact_rows}
        for row in token_rows:
            raw_text = row["raw_text"] or ""
            score = 0.0
            if query in raw_text:
                score += 6.0
            score += sum(1.0 for token in tokens if token in raw_text)
            if score <= 0:
                continue
            evidence_id = row["evidence_id"]
            if evidence_id not in scores or score > scores[evidence_id]:
                scored_rows[evidence_id] = row
                scores[evidence_id] = score

        ranked_rows = sorted(
            scored_rows.values(),
            key=lambda row: (-scores[row["evidence_id"]], -row["evidence_id"]),
        )
        return self._format_results(ranked_rows[:limit], scores)

    def _format_results(
        self,
        rows: list[sqlite3.Row],
        scores: dict[int, float],
    ) -> list[dict[str, Any]]:
        return [
            {
                "id": row["evidence_id"],
                "query": row["query"],
                "char_count": row["char_count"],
                "source_type": row["source_type"],
                "source_tier": row["source_tier"] or "unverified",
                "source_uri": row["source_uri"] or "",
                "published_at": row["published_at"] or "",
                "_score": float(scores.get(row["evidence_id"], 0.0)),
            }
            for row in rows
        ]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
