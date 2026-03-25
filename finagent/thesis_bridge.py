"""Read bridge: v1 thesis OS → lightweight views for v2.

Provides read-only access to v1 thesis, target, and claim objects
as simple dataclasses that v2 writeback engine can consume.

No v1 writes. No mutations. No imports beyond db.py helpers.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any

from .db import select_one, list_rows

import json
import logging

logger = logging.getLogger(__name__)


# ── View Dataclasses ─────────────────────────────────────────────────

@dataclass(frozen=True)
class ThesisView:
    """Lightweight read-only view of a v1 thesis + current version."""
    thesis_id: str = ""
    title: str = ""
    status: str = ""                  # seed/framed/evidence_backed/active/...
    horizon_months: int = 0
    theme_ids: list[str] = field(default_factory=list)
    owner: str = ""

    # From current thesis_version
    version_id: str = ""
    statement: str = ""
    mechanism_chain: str = ""
    why_now: str = ""
    base_case: str = ""
    counter_case: str = ""
    invalidators: str = ""
    conviction: float = 0.0

    created_at: str = ""


@dataclass(frozen=True)
class TargetView:
    """Lightweight read-only view of a v1 target + target_case + entity."""
    target_id: str = ""
    entity_id: str = ""
    entity_name: str = ""
    ticker: str = ""
    asset_class: str = ""
    venue: str = ""

    # From target_case (if linked to a thesis_version)
    target_case_id: str = ""
    thesis_version_id: str = ""
    exposure_type: str = ""
    capture_link_strength: float = 0.0
    status: str = ""


@dataclass(frozen=True)
class ClaimView:
    """Lightweight read-only view of a v1 claim."""
    claim_id: str = ""
    artifact_id: str = ""
    claim_text: str = ""
    claim_type: str = ""
    confidence: float = 0.0
    review_status: str = "unreviewed"
    entity_ids: list[str] = field(default_factory=list)


# ── Loaders ──────────────────────────────────────────────────────────

def _parse_json_list(raw: str | None) -> list[str]:
    """Safely parse a JSON list from a TEXT column."""
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except (json.JSONDecodeError, TypeError):
        pass
    return []


def load_theses(conn: sqlite3.Connection) -> list[ThesisView]:
    """Load all theses with their current version info.

    Joins theses + thesis_versions on current_version_id.
    Returns empty list if no theses exist.
    """
    rows = list_rows(
        conn,
        """
        SELECT t.thesis_id, t.title, t.status, t.horizon_months,
               t.theme_ids_json, t.owner, t.current_version_id, t.created_at,
               tv.thesis_version_id, tv.statement, tv.mechanism_chain,
               tv.why_now, tv.base_case, tv.counter_case,
               tv.invalidators, tv.human_conviction
        FROM theses t
        LEFT JOIN thesis_versions tv
            ON t.current_version_id = tv.thesis_version_id
        ORDER BY t.created_at DESC
        """,
    )

    result: list[ThesisView] = []
    for r in rows:
        result.append(ThesisView(
            thesis_id=r["thesis_id"],
            title=r["title"],
            status=r["status"],
            horizon_months=r["horizon_months"] or 0,
            theme_ids=_parse_json_list(r["theme_ids_json"]),
            owner=r["owner"] or "",
            version_id=r["thesis_version_id"] or "",
            statement=r["statement"] or "",
            mechanism_chain=r["mechanism_chain"] or "",
            why_now=r["why_now"] or "",
            base_case=r["base_case"] or "",
            counter_case=r["counter_case"] or "",
            invalidators=r["invalidators"] or "",
            conviction=r["human_conviction"] or 0.0,
            created_at=r["created_at"] or "",
        ))
    return result


def load_targets(conn: sqlite3.Connection) -> list[TargetView]:
    """Load all targets with entity info and latest target_case.

    Joins targets + entities. For target_cases, picks the most recent
    (by created_at) per target to avoid duplicate rows.
    """
    rows = list_rows(
        conn,
        """
        SELECT tg.target_id, tg.entity_id, tg.asset_class, tg.venue,
               tg.ticker_or_symbol,
               e.canonical_name,
               tc.target_case_id, tc.thesis_version_id,
               tc.exposure_type, tc.capture_link_strength, tc.status
        FROM targets tg
        JOIN entities e ON tg.entity_id = e.entity_id
        LEFT JOIN target_cases tc ON tg.target_id = tc.target_id
            AND tc.target_case_id = (
                SELECT tc2.target_case_id
                FROM target_cases tc2
                WHERE tc2.target_id = tg.target_id
                ORDER BY tc2.created_at DESC, tc2.target_case_id DESC
                LIMIT 1
            )
        ORDER BY tg.created_at DESC
        """,
    )

    result: list[TargetView] = []
    for r in rows:
        result.append(TargetView(
            target_id=r["target_id"],
            entity_id=r["entity_id"],
            entity_name=r["canonical_name"] or "",
            ticker=r["ticker_or_symbol"] or "",
            asset_class=r["asset_class"] or "",
            venue=r["venue"] or "",
            target_case_id=r["target_case_id"] or "",
            thesis_version_id=r["thesis_version_id"] or "",
            exposure_type=r["exposure_type"] or "",
            capture_link_strength=r["capture_link_strength"] or 0.0,
            status=r["status"] or "",
        ))
    return result


def load_claims(
    conn: sqlite3.Connection,
    *,
    limit: int = 100,
    review_status: str | None = None,
) -> list[ClaimView]:
    """Load claims, optionally filtered by review status."""
    sql = "SELECT * FROM claims ORDER BY created_at DESC LIMIT ?"
    params: tuple = (limit,)
    if review_status:
        sql = "SELECT * FROM claims WHERE review_status = ? ORDER BY created_at DESC LIMIT ?"
        params = (review_status, limit)

    rows = list_rows(conn, sql, params)
    result: list[ClaimView] = []
    for r in rows:
        result.append(ClaimView(
            claim_id=r["claim_id"],
            artifact_id=r["artifact_id"],
            claim_text=r["claim_text"],
            claim_type=r["claim_type"],
            confidence=r["confidence"] or 0.0,
            review_status=r["review_status"] or "unreviewed",
            entity_ids=_parse_json_list(r["linked_entity_ids_json"]),
        ))
    return result


def get_thesis(conn: sqlite3.Connection, thesis_id: str) -> ThesisView | None:
    """Load a single thesis by ID."""
    row = select_one(
        conn,
        """
        SELECT t.thesis_id, t.title, t.status, t.horizon_months,
               t.theme_ids_json, t.owner, t.current_version_id, t.created_at,
               tv.thesis_version_id, tv.statement, tv.mechanism_chain,
               tv.why_now, tv.base_case, tv.counter_case,
               tv.invalidators, tv.human_conviction
        FROM theses t
        LEFT JOIN thesis_versions tv
            ON t.current_version_id = tv.thesis_version_id
        WHERE t.thesis_id = ?
        """,
        (thesis_id,),
    )
    if row is None:
        return None

    return ThesisView(
        thesis_id=row["thesis_id"],
        title=row["title"],
        status=row["status"],
        horizon_months=row["horizon_months"] or 0,
        theme_ids=_parse_json_list(row["theme_ids_json"]),
        owner=row["owner"] or "",
        version_id=row["thesis_version_id"] or "",
        statement=row["statement"] or "",
        mechanism_chain=row["mechanism_chain"] or "",
        why_now=row["why_now"] or "",
        base_case=row["base_case"] or "",
        counter_case=row["counter_case"] or "",
        invalidators=row["invalidators"] or "",
        conviction=row["human_conviction"] or 0.0,
        created_at=row["created_at"] or "",
    )


def _bigrams(text: str) -> set[str]:
    """Extract character bigrams — works for CJK and Latin."""
    text = text.lower().replace(" ", "")
    return {text[i:i+2] for i in range(len(text) - 1)} if len(text) >= 2 else {text}


def find_matching_thesis(
    conn: sqlite3.Connection,
    goal: str,
    context: str = "",
) -> ThesisView | None:
    """Find a thesis whose title or statement fuzzy-matches the research goal.

    Uses bigram overlap for CJK-friendly matching.
    Returns the best match or None if no reasonable match found.
    """
    theses = load_theses(conn)
    if not theses:
        return None

    goal_bi = _bigrams(goal)
    context_lower = context.lower() if context else ""

    best: ThesisView | None = None
    best_score: float = 0.0

    for t in theses:
        # Skip archived/invalidated theses
        if t.status in ("archived", "invalidated", "expired"):
            continue

        title_bi = _bigrams(t.title)
        statement_bi = _bigrams(t.statement)

        # Jaccard-like overlap score
        title_overlap = len(goal_bi & title_bi) / max(len(goal_bi | title_bi), 1)
        statement_overlap = len(goal_bi & statement_bi) / max(len(goal_bi | statement_bi), 1)

        # Title weighted 2x
        score = title_overlap * 2.0 + statement_overlap

        # Context bonus
        if context_lower and context_lower in t.title.lower():
            score += 1.0

        if score > best_score:
            best_score = score
            best = t

    # Require minimum Jaccard overlap
    if best_score < 0.3:
        return None

    return best

