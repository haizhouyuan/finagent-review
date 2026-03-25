"""Tests for finagent.writeback."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from finagent.writeback import (
    ingest_claim_outcome,
    ingest_expression_outcome,
    ingest_source_feedback,
    list_writeback_entries,
)


class TestIngestClaimOutcome:
    def test_basic(self, tmp_path):
        result = ingest_claim_outcome(
            {"candidate_id": "cand_1", "claim_id": "clm_1", "outcome": "validated"},
            writeback_dir=tmp_path,
        )
        assert result["ok"] is True
        path = Path(result["path"])
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["outcome"] == "validated"
        assert data["candidate_id"] == "cand_1"

    def test_default_values(self, tmp_path):
        result = ingest_claim_outcome({}, writeback_dir=tmp_path)
        assert result["ok"] is True
        data = json.loads(Path(result["path"]).read_text())
        assert data["candidate_id"] == "unknown"
        assert data["outcome"] == "neutral"


class TestIngestSourceFeedback:
    def test_basic(self, tmp_path):
        result = ingest_source_feedback(
            {
                "source_id": "src_1",
                "source_name": "Test KOL",
                "quality_score": 0.85,
                "quality_band": "core",
                "elo_rating": 1650.0,
            },
            writeback_dir=tmp_path,
        )
        assert result["ok"] is True
        data = json.loads(Path(result["path"]).read_text())
        assert data["elo_rating"] == 1650.0
        assert data["quality_band"] == "core"


class TestIngestExpressionOutcome:
    def test_basic(self, tmp_path):
        result = ingest_expression_outcome(
            {
                "candidate_id": "cand_1",
                "expression": "NVIDIA Call Options",
                "performance": "outperformed",
            },
            writeback_dir=tmp_path,
        )
        assert result["ok"] is True
        data = json.loads(Path(result["path"]).read_text())
        assert data["performance"] == "outperformed"


class TestListWritebackEntries:
    def test_empty(self, tmp_path):
        entries = list_writeback_entries("claim_outcomes", writeback_dir=tmp_path)
        assert entries == []

    def test_with_data(self, tmp_path):
        ingest_claim_outcome(
            {"candidate_id": "c1", "claim_id": "cl1", "outcome": "validated"},
            writeback_dir=tmp_path,
        )
        ingest_claim_outcome(
            {"candidate_id": "c2", "claim_id": "cl2", "outcome": "contradicted"},
            writeback_dir=tmp_path,
        )
        entries = list_writeback_entries("claim_outcomes", writeback_dir=tmp_path)
        assert len(entries) == 2
