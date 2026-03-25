"""Layer 3b: Regression tests for specific known bugs encountered during pipeline execution."""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _cli(root: Path, *args: str) -> dict:
    r = subprocess.run(
        [sys.executable, "-m", "finagent.cli", "--root", str(root), *args],
        capture_output=True, text=True, cwd=REPO_ROOT, timeout=30,
    )
    if r.returncode != 0:
        raise RuntimeError(f"CLI failed ({r.returncode}): {r.stderr[:400]}")
    return json.loads(r.stdout.strip())


@pytest.mark.regression
class TestRegressionMonitorSchema:
    """Regression: monitors table requires status NOT NULL."""

    def test_direct_insert_without_status_fails(self, fresh_db):
        with pytest.raises(sqlite3.IntegrityError):
            fresh_db.execute(
                """INSERT INTO monitors
                   (monitor_id, owner_object_type, owner_object_id,
                    monitor_type, metric_name, created_at)
                   VALUES ('m_bad', 'thesis', 'th1', 'market', 'x', datetime('now'))"""
            )

    def test_direct_insert_with_status_succeeds(self, fresh_db):
        fresh_db.execute(
            """INSERT INTO monitors
               (monitor_id, owner_object_type, owner_object_id,
                monitor_type, metric_name, status, created_at)
               VALUES ('m_ok', 'thesis', 'th1', 'market', 'x', 'active', datetime('now'))"""
        )
        cnt = fresh_db.execute("SELECT COUNT(*) FROM monitors WHERE monitor_id='m_ok'").fetchone()[0]
        assert cnt == 1


@pytest.mark.regression
class TestRegressionGateArtifacts:
    """Regression: thesis_versions.created_from_artifacts_json must be populated."""

    def test_empty_artifacts_json_means_no_first_hand(self, cli_root, tmp_path):
        txt = tmp_path / "data.txt"
        txt.write_text("Test data for gate regression.")

        _cli(cli_root, "create-source", "--source-id", "src_r",
             "--source-type", "official_disclosure", "--name", "R Source",
             "--primaryness", "first_hand", "--language", "en")
        _cli(cli_root, "ingest-text",
             "--source-id", "src_r", "--source-type", "official_disclosure",
             "--source-name", "R Source", "--primaryness", "first_hand",
             "--path", str(txt), "--artifact-id", "art_r",
             "--artifact-kind", "text_note", "--title", "R Data", "--language", "en")
        _cli(cli_root, "create-theme", "--theme-id", "th_r", "--name", "R",
             "--why-it-matters", "x", "--maturity-stage", "growth",
             "--importance-status", "priority")

        # Create thesis WITHOUT --artifact-id → created_from_artifacts_json = []
        _cli(cli_root, "create-thesis", "--thesis-id", "thesis_r",
             "--thesis-version-id", "thv_r_v1",
             "--title", "Regression test thesis", "--status", "framed",
             "--horizon-months", "12", "--theme-id", "th_r",
             "--owner", "human", "--statement", "Regression test",
             "--mechanism-chain", "A -> B",
             "--base-case", "B", "--counter-case", "C",
             "--invalidators", "I", "--human-conviction", "0.7")

        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_r")
        item = gate["items"][0]

        # Gate must report has_first_hand_artifact=False because artifacts not linked
        assert item["checks"]["has_first_hand_artifact"] is False
        assert "has_first_hand_artifact" in item.get("missing", [])


@pytest.mark.regression
class TestRegressionTargetCaseKeyMetric:
    """Regression: --key-metric must use key=value format."""

    def test_key_metric_format(self, cli_root):
        _cli(cli_root, "create-entity", "--entity-id", "ent_km",
             "--entity-type", "company", "--name", "KM Corp",
             "--symbol", "KM", "--jurisdiction", "US")
        _cli(cli_root, "create-target", "--target-id", "tgt_km",
             "--entity-id", "ent_km", "--asset-class", "us_equity",
             "--venue", "NYSE", "--ticker-or-symbol", "KM", "--currency", "USD")
        _cli(cli_root, "create-theme", "--theme-id", "th_km", "--name", "KM",
             "--why-it-matters", "x", "--maturity-stage", "growth",
             "--importance-status", "priority")
        _cli(cli_root, "create-thesis", "--thesis-id", "thesis_km",
             "--thesis-version-id", "thv_km_v1",
             "--title", "KM thesis", "--status", "framed",
             "--horizon-months", "12", "--theme-id", "th_km",
             "--owner", "human", "--statement", "KM test",
             "--mechanism-chain", "A -> B",
             "--base-case", "B", "--counter-case", "C",
             "--invalidators", "I", "--human-conviction", "0.7")

        result = _cli(cli_root, "create-target-case",
                       "--target-case-id", "tc_km",
                       "--thesis-version-id", "thv_km_v1",
                       "--target-id", "tgt_km",
                       "--exposure-type", "direct",
                       "--capture-link-strength", "0.8",
                       "--key-metric", "revenue_growth=YoY revenue growth")
        assert "target_case_id" in result


@pytest.mark.regression
class TestRegressionFreshnessTimezone:
    """Regression: offset-naive vs offset-aware datetime strings."""

    def test_freshness_handles_naive_iso(self):
        from finagent.contracts.freshness import freshness_status_for_date
        # Naive ISO without timezone info
        naive = "2026-03-01T12:00:00"
        label = freshness_status_for_date(naive)
        assert label in ("fresh", "aging", "stale", "unknown")

    def test_freshness_handles_utc_iso(self):
        from finagent.contracts.freshness import freshness_status_for_date
        aware = "2026-03-01T12:00:00+00:00"
        label = freshness_status_for_date(aware)
        assert label in ("fresh", "aging", "stale", "unknown")

    def test_freshness_handles_china_tz(self):
        from finagent.contracts.freshness import freshness_status_for_date
        china = "2026-03-01T20:00:00+08:00"
        label = freshness_status_for_date(china)
        assert label in ("fresh", "aging", "stale", "unknown")
