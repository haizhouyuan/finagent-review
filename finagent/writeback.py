"""Writeback receiver — accept validated outcomes from finbot.

Enables bidirectional integration: finbot pushes claim validation outcomes,
source quality feedback, and expression performance back to finagent's
evidence system. Data is persisted as JSON files under the writeback directory.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_WRITEBACK_DIR = Path("data/writeback")


def _ensure_dir(path: Path) -> Path:
    """Ensure the directory exists and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_filename(value: str) -> str:
    """Sanitize a string for use as a filename component."""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in value)[:80]


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Atomically write JSON to file."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.rename(path)


def ingest_claim_outcome(
    payload: dict[str, Any],
    *,
    writeback_dir: Path | None = None,
) -> dict[str, Any]:
    """Accept a claim validation outcome from finbot.

    Args:
        payload: {
            candidate_id: str,
            claim_id: str,
            claim_text: str,
            outcome: 'validated' | 'contradicted' | 'neutral',
            evidence_source: str (optional),
            confidence: float (optional),
        }
        writeback_dir: Override the default writeback directory.

    Returns:
        {ok: True, path: str} on success.
    """
    root = _ensure_dir((writeback_dir or DEFAULT_WRITEBACK_DIR) / "claim_outcomes")

    candidate_id = str(payload.get("candidate_id", "unknown"))
    claim_id = str(payload.get("claim_id", "unknown"))
    outcome = str(payload.get("outcome", "neutral"))

    record = {
        "type": "claim_outcome",
        "candidate_id": candidate_id,
        "claim_id": claim_id,
        "claim_text": str(payload.get("claim_text", "")),
        "outcome": outcome,
        "evidence_source": str(payload.get("evidence_source", "")),
        "confidence": float(payload.get("confidence", 0.5)),
        "ingested_at": time.time(),
        "ingested_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    filename = f"{_safe_filename(candidate_id)}_{_safe_filename(claim_id)}_{int(time.time())}.json"
    path = root / filename
    _write_json(path, record)
    log.info("Ingested claim outcome: %s -> %s", claim_id, outcome)
    return {"ok": True, "path": str(path)}


def ingest_source_feedback(
    payload: dict[str, Any],
    *,
    writeback_dir: Path | None = None,
) -> dict[str, Any]:
    """Accept source quality feedback from finbot.

    Args:
        payload: {
            source_id: str,
            source_name: str,
            quality_score: float,
            quality_band: str,
            elo_rating: float (optional),
            trend_label: str,
            supported_claim_count: int,
            contradicted_claim_count: int,
        }
    """
    root = _ensure_dir((writeback_dir or DEFAULT_WRITEBACK_DIR) / "source_feedback")

    source_id = str(payload.get("source_id", "unknown"))

    record = {
        "type": "source_feedback",
        "source_id": source_id,
        "source_name": str(payload.get("source_name", "")),
        "quality_score": float(payload.get("quality_score", 0.0)),
        "quality_band": str(payload.get("quality_band", "")),
        "elo_rating": payload.get("elo_rating"),
        "trend_label": str(payload.get("trend_label", "")),
        "supported_claim_count": int(payload.get("supported_claim_count", 0)),
        "contradicted_claim_count": int(payload.get("contradicted_claim_count", 0)),
        "ingested_at": time.time(),
        "ingested_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    filename = f"{_safe_filename(source_id)}_{int(time.time())}.json"
    path = root / filename
    _write_json(path, record)
    log.info("Ingested source feedback: %s -> %s", source_id, payload.get("quality_band"))
    return {"ok": True, "path": str(path)}


def ingest_expression_outcome(
    payload: dict[str, Any],
    *,
    writeback_dir: Path | None = None,
) -> dict[str, Any]:
    """Accept expression performance feedback from finbot.

    Args:
        payload: {
            candidate_id: str,
            expression: str,
            performance: 'outperformed' | 'underperformed' | 'neutral',
            notes: str (optional),
        }
    """
    root = _ensure_dir((writeback_dir or DEFAULT_WRITEBACK_DIR) / "expression_outcomes")

    candidate_id = str(payload.get("candidate_id", "unknown"))
    expression = str(payload.get("expression", "unknown"))

    record = {
        "type": "expression_outcome",
        "candidate_id": candidate_id,
        "expression": expression,
        "performance": str(payload.get("performance", "neutral")),
        "notes": str(payload.get("notes", "")),
        "ingested_at": time.time(),
        "ingested_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    filename = f"{_safe_filename(candidate_id)}_{_safe_filename(expression[:20])}_{int(time.time())}.json"
    path = root / filename
    _write_json(path, record)
    log.info("Ingested expression outcome: %s -> %s", expression[:40], payload.get("performance"))
    return {"ok": True, "path": str(path)}


def list_writeback_entries(
    entry_type: str = "claim_outcomes",
    *,
    writeback_dir: Path | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """List recent writeback entries of a given type."""
    root = (writeback_dir or DEFAULT_WRITEBACK_DIR) / entry_type
    if not root.exists():
        return []
    entries: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.json"), reverse=True)[:limit]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            entries.append(data)
        except (json.JSONDecodeError, OSError):
            continue
    return entries
