"""Research run ledger — persistent tracking of v2 research sessions.

Provides SQLite-backed storage for:
  - research_runs: lifecycle of each research session
  - research_run_steps: per-node execution records
  - research_run_artifacts: output files linked to each run

This module is the "run history" layer. It records WHAT happened,
while the LangGraph checkpointer (Commit 4) records WHERE to resume.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .research_contracts import ResearchRun, RunStatus

logger = logging.getLogger(__name__)

_UTCNOW = lambda: datetime.now(timezone.utc).isoformat()

# ── Schema ───────────────────────────────────────────────────────────

_LEDGER_DDL = """\
CREATE TABLE IF NOT EXISTS research_runs (
    run_id              TEXT PRIMARY KEY,
    goal                TEXT NOT NULL,
    context             TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'queued',
    llm_backend         TEXT NOT NULL DEFAULT 'mock',
    max_iterations      INTEGER NOT NULL DEFAULT 10,
    token_budget        INTEGER NOT NULL DEFAULT 50000,
    confidence_threshold REAL NOT NULL DEFAULT 0.85,
    current_iteration   INTEGER NOT NULL DEFAULT 0,
    total_triples       INTEGER NOT NULL DEFAULT 0,
    confidence_score    REAL NOT NULL DEFAULT 0.0,
    token_budget_remaining INTEGER NOT NULL DEFAULT 50000,
    termination_reason  TEXT NOT NULL DEFAULT '',
    error               TEXT NOT NULL DEFAULT '',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    completed_at        TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS research_run_steps (
    step_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL REFERENCES research_runs(run_id),
    node_name   TEXT NOT NULL,
    iteration   INTEGER NOT NULL DEFAULT 0,
    input_keys  TEXT NOT NULL DEFAULT '[]',
    output_keys TEXT NOT NULL DEFAULT '[]',
    token_cost_est INTEGER NOT NULL DEFAULT 0,
    started_at  TEXT NOT NULL,
    ended_at    TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_run_steps_run_id
    ON research_run_steps(run_id, step_id);

CREATE TABLE IF NOT EXISTS research_run_artifacts (
    artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL REFERENCES research_runs(run_id),
    step_id     INTEGER,
    kind        TEXT NOT NULL,
    path        TEXT NOT NULL DEFAULT '',
    data_json   TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_artifacts_run_id
    ON research_run_artifacts(run_id);
"""


# ── ResearchLedger ───────────────────────────────────────────────────

class ResearchLedger:
    """SQLite-backed ledger for research run history.

    Usage:
        ledger = ResearchLedger("state/research.sqlite")
        run = ledger.create_run(goal="...", llm_backend="mock")
        ledger.update_run(run.run_id, status="running")
        step_id = ledger.record_step(run.run_id, "planner", iteration=1)
        ledger.complete_step(step_id)
        ledger.complete_run(run.run_id, ...)
    """

    def __init__(self, db_path: str | Path | None = None):
        if db_path is None:
            from .paths import resolve_paths
            db_path = resolve_paths().research_db_path
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        self._ensure_tables()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _ensure_tables(self) -> None:
        self.conn.executescript(_LEDGER_DDL)
        self.conn.commit()

    # ── Run lifecycle ────────────────────────────────────────────────

    def create_run(
        self,
        goal: str,
        context: str = "",
        llm_backend: str = "mock",
        max_iterations: int = 10,
        token_budget: int = 50_000,
        confidence_threshold: float = 0.85,
    ) -> ResearchRun:
        """Create a new run record and return it."""
        run = ResearchRun(
            goal=goal,
            context=context,
            llm_backend=llm_backend,
            max_iterations=max_iterations,
            token_budget=token_budget,
            token_budget_remaining=token_budget,
            confidence_threshold=confidence_threshold,
        )
        self.conn.execute(
            """
            INSERT INTO research_runs (
                run_id, goal, context, status, llm_backend,
                max_iterations, token_budget, confidence_threshold,
                token_budget_remaining, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run.run_id, run.goal, run.context, run.status,
                run.llm_backend, run.max_iterations, run.token_budget,
                run.confidence_threshold, run.token_budget_remaining,
                run.created_at, run.updated_at,
            ),
        )
        self.conn.commit()
        logger.info("created research run %s: %s", run.run_id, goal)
        return run

    def update_run(self, run_id: str, **updates: Any) -> None:
        """Update run fields (status, iteration, triples, confidence, etc)."""
        updates["updated_at"] = _UTCNOW()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [run_id]
        self.conn.execute(
            f"UPDATE research_runs SET {set_clause} WHERE run_id = ?",
            values,
        )
        self.conn.commit()

    def complete_run(
        self,
        run_id: str,
        *,
        status: str = RunStatus.COMPLETED.value,
        total_triples: int = 0,
        confidence_score: float = 0.0,
        termination_reason: str = "",
        error: str = "",
    ) -> None:
        """Mark a run as completed/failed."""
        self.update_run(
            run_id,
            status=status,
            total_triples=total_triples,
            confidence_score=confidence_score,
            termination_reason=termination_reason,
            error=error,
            completed_at=_UTCNOW(),
        )
        logger.info("completed run %s: %s (%s)", run_id, status, termination_reason)

    def get_run(self, run_id: str) -> ResearchRun | None:
        """Fetch a run by ID."""
        row = self.conn.execute(
            "SELECT * FROM research_runs WHERE run_id = ?", (run_id,),
        ).fetchone()
        if row is None:
            return None
        return ResearchRun.from_dict(dict(row))

    def list_runs(
        self,
        *,
        status: str | None = None,
        limit: int = 20,
    ) -> list[ResearchRun]:
        """List recent runs, optionally filtered by status."""
        if status:
            rows = self.conn.execute(
                "SELECT * FROM research_runs WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM research_runs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [ResearchRun.from_dict(dict(r)) for r in rows]

    # ── Step tracking ────────────────────────────────────────────────

    def record_step(
        self,
        run_id: str,
        node_name: str,
        iteration: int = 0,
        input_keys: list[str] | None = None,
    ) -> int:
        """Record a step starting. Returns step_id."""
        cursor = self.conn.execute(
            """
            INSERT INTO research_run_steps (
                run_id, node_name, iteration, input_keys, started_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id, node_name, iteration,
                json.dumps(input_keys or []),
                _UTCNOW(),
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    def complete_step(
        self,
        step_id: int,
        output_keys: list[str] | None = None,
        token_cost_est: int = 0,
    ) -> None:
        """Mark a step as completed."""
        self.conn.execute(
            """
            UPDATE research_run_steps
            SET ended_at = ?, output_keys = ?, token_cost_est = ?
            WHERE step_id = ?
            """,
            (
                _UTCNOW(),
                json.dumps(output_keys or []),
                token_cost_est,
                step_id,
            ),
        )
        self.conn.commit()

    def get_steps(self, run_id: str) -> list[dict[str, Any]]:
        """Get all steps for a run."""
        rows = self.conn.execute(
            "SELECT * FROM research_run_steps WHERE run_id = ? ORDER BY step_id",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Artifact tracking ────────────────────────────────────────────

    def record_artifact(
        self,
        run_id: str,
        kind: str,
        path: str = "",
        data: dict[str, Any] | None = None,
        step_id: int | None = None,
    ) -> int:
        """Record an artifact (report, graph snapshot, package, etc)."""
        cursor = self.conn.execute(
            """
            INSERT INTO research_run_artifacts (
                run_id, step_id, kind, path, data_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, step_id, kind, path,
                json.dumps(data or {}),
                _UTCNOW(),
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        """Get all artifacts for a run."""
        rows = self.conn.execute(
            "SELECT * FROM research_run_artifacts WHERE run_id = ? ORDER BY artifact_id",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Cleanup ──────────────────────────────────────────────────────

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
