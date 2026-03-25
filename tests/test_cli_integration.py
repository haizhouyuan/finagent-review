"""Layer 3a: CLI CRUD and View integration tests."""
from __future__ import annotations

import json
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


# ═══════════════════════════════════════════════════════════════════
# CRUD Commands
# ═══════════════════════════════════════════════════════════════════

class TestCLIInit:
    def test_init_creates_db(self, cli_root):
        assert (cli_root / "state" / "finagent.sqlite").exists()


class TestCLICreateSource:
    def test_create_source(self, cli_root):
        result = _cli(cli_root, "create-source",
                       "--source-id", "src_1",
                       "--source-type", "official_disclosure",
                       "--name", "Test Source",
                       "--primaryness", "first_hand",
                       "--language", "en")
        assert result["source_id"] == "src_1"

    def test_create_source_idempotent(self, cli_root):
        """Creating same source twice should not crash."""
        args = ("create-source", "--source-id", "src_2",
                "--source-type", "kol", "--name", "KOL",
                "--primaryness", "second_hand", "--language", "zh")
        _cli(cli_root, *args)
        # Second call should succeed (IGNORE or update)
        _cli(cli_root, *args)


class TestCLICreateTheme:
    def test_create_theme(self, cli_root):
        result = _cli(cli_root, "create-theme",
                       "--theme-id", "th1",
                       "--name", "AI Infra",
                       "--why-it-matters", "Critical for compute",
                       "--maturity-stage", "growth",
                       "--importance-status", "priority")
        assert result["theme_id"] == "th1"


class TestCLICreateEntity:
    def test_create_entity(self, cli_root):
        result = _cli(cli_root, "create-entity",
                       "--entity-id", "ent_1",
                       "--entity-type", "company",
                       "--name", "NVIDIA",
                       "--alias", "NVDA",
                       "--symbol", "NVDA",
                       "--jurisdiction", "US")
        assert result["entity_id"] == "ent_1"


class TestCLICreateTarget:
    def test_create_target(self, cli_root):
        _cli(cli_root, "create-entity", "--entity-id", "ent_t",
             "--entity-type", "company", "--name", "T Corp",
             "--symbol", "TC", "--jurisdiction", "US")
        result = _cli(cli_root, "create-target",
                       "--target-id", "tgt_1",
                       "--entity-id", "ent_t",
                       "--asset-class", "us_equity",
                       "--venue", "NYSE",
                       "--ticker-or-symbol", "TC",
                       "--currency", "USD")
        assert result["target_id"] == "tgt_1"


class TestCLIIngestAndExtract:
    def test_ingest_text_and_extract(self, cli_root, tmp_path):
        txt = tmp_path / "note.txt"
        txt.write_text("Revenue grew 30% YoY to $25B. Margins expanded to 55%.")

        _cli(cli_root, "create-source", "--source-id", "src_ie",
             "--source-type", "official_disclosure", "--name", "IE Source",
             "--primaryness", "first_hand", "--language", "en")

        result = _cli(cli_root, "ingest-text",
                       "--source-id", "src_ie",
                       "--source-type", "official_disclosure",
                       "--source-name", "IE Source",
                       "--primaryness", "first_hand",
                       "--path", str(txt),
                       "--artifact-id", "art_ie",
                       "--artifact-kind", "text_note",
                       "--title", "Test Note",
                       "--language", "en")
        assert result["artifact_id"] == "art_ie"

        claims = _cli(cli_root, "extract-claims",
                       "--artifact-id", "art_ie",
                       "--speaker", "CEO")
        assert claims["claim_count"] >= 1


class TestCLICreateMonitor:
    @pytest.mark.regression
    def test_create_monitor_requires_all_fields(self, cli_root):
        """Monitor creation must work via CLI with all required fields."""
        _cli(cli_root, "create-theme", "--theme-id", "th_m", "--name", "M",
             "--why-it-matters", "x", "--maturity-stage", "growth",
             "--importance-status", "tracking")
        _cli(cli_root, "create-thesis", "--thesis-id", "thesis_m",
             "--thesis-version-id", "thv_m_v1",
             "--title", "Monitor test thesis", "--status", "seed",
             "--horizon-months", "6", "--theme-id", "th_m",
             "--owner", "human", "--statement", "Monitor test",
             "--mechanism-chain", "A -> B",
             "--base-case", "B", "--counter-case", "C",
             "--invalidators", "I", "--human-conviction", "0.5")
        result = _cli(cli_root, "create-monitor",
                       "--owner-object-type", "thesis",
                       "--owner-object-id", "thesis_m",
                       "--monitor-type", "claim_freshness",
                       "--comparator", "lte",
                       "--threshold-value", "180")
        assert "monitor_id" in result


# ═══════════════════════════════════════════════════════════════════
# View Commands
# ═══════════════════════════════════════════════════════════════════

class TestCLIViews:
    """All view commands should return valid JSON without crashing."""

    def test_board(self, seeded_env, cli):
        result = cli("board")
        assert "items" in result or isinstance(result, dict)

    def test_thesis_board(self, seeded_env, cli):
        result = cli("thesis-board")
        assert isinstance(result, dict)

    def test_thesis_focus(self, seeded_env, cli):
        result = cli("thesis-focus", "--thesis-id", seeded_env["thesis_id"])
        assert isinstance(result, dict)

    def test_thesis_gate_report(self, seeded_env, cli):
        result = cli("thesis-gate-report", "--thesis-id", seeded_env["thesis_id"])
        assert "items" in result
        assert len(result["items"]) > 0

    def test_today_cockpit(self, seeded_env, cli):
        result = cli("today-cockpit")
        assert isinstance(result, dict)

    def test_routing_board(self, seeded_env, cli):
        result = cli("routing-board")
        assert isinstance(result, dict)

    def test_source_board(self, seeded_env, cli):
        result = cli("source-board")
        assert isinstance(result, dict)

    def test_theme_map(self, seeded_env, cli):
        result = cli("theme-map")
        assert isinstance(result, dict)

    def test_target_case_dashboard(self, seeded_env, cli):
        result = cli("target-case-dashboard")
        assert isinstance(result, dict)

    def test_promotion_wizard(self, seeded_env, cli):
        result = cli("promotion-wizard")
        assert isinstance(result, dict)

    def test_validation_board(self, seeded_env, cli):
        result = cli("validation-board")
        assert isinstance(result, dict)

    def test_watch_board(self, seeded_env, cli):
        result = cli("watch-board")
        assert isinstance(result, dict)

    def test_review_board(self, seeded_env, cli):
        result = cli("review-board")
        assert isinstance(result, dict)

    def test_playbook_board(self, seeded_env, cli):
        result = cli("playbook-board")
        assert isinstance(result, dict)

    def test_source_track_record(self, seeded_env, cli):
        result = cli("source-track-record")
        assert isinstance(result, dict)

    def test_corroboration_queue(self, seeded_env, cli):
        result = cli("corroboration-queue")
        assert isinstance(result, dict)
