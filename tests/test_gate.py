"""Layer 2c: Gate logic tests — thesis promotion gate checks."""
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
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=30,
    )
    if r.returncode != 0:
        raise RuntimeError(f"CLI failed ({r.returncode}): {r.stderr[:300]}")
    return json.loads(r.stdout.strip())


def _setup_thesis_with_artifacts(root: Path, tmp_path: Path, *,
                                  has_first_hand: bool = True,
                                  artifact_in_version: bool = True) -> str:
    """Create a complete thesis with optional first-hand artifact linkage."""
    txt = tmp_path / "data.txt"
    txt.write_text("Revenue: $25B. CoWoS capacity doubling. HBM demand strong.")

    primaryness = "first_hand" if has_first_hand else "second_hand"

    _cli(root, "create-source", "--source-id", "src_g",
         "--source-type", "official_disclosure", "--name", "Gate Test Source",
         "--primaryness", primaryness, "--language", "en")

    _cli(root, "ingest-text",
         "--source-id", "src_g", "--source-type", "official_disclosure",
         "--source-name", "Gate Test Source", "--primaryness", primaryness,
         "--path", str(txt), "--artifact-id", "art_g",
         "--artifact-kind", "text_note", "--title", "Gate Data", "--language", "en")

    _cli(root, "extract-claims", "--artifact-id", "art_g", "--speaker", "IR")

    _cli(root, "create-theme", "--theme-id", "theme_g", "--name", "Gate Theme",
         "--why-it-matters", "test", "--maturity-stage", "growth",
         "--importance-status", "priority")

    artifact_args = ["--artifact-id", "art_g"] if artifact_in_version else []
    _cli(root, "create-thesis", "--thesis-id", "thesis_g",
         "--thesis-version-id", "thv_g_v1",
         "--title", "Gate test thesis", "--status", "framed",
         "--horizon-months", "12", "--theme-id", "theme_g",
         *artifact_args,
         "--owner", "human",
         "--statement", "Gate test statement",
         "--mechanism-chain", "A -> B",
         "--base-case", "Base", "--counter-case", "Counter",
         "--invalidators", "Inv",
         "--human-conviction", "0.7")
    return "thesis_g"


@pytest.mark.gate
class TestPromotionGate:
    """Test _build_promotion_gate via thesis-gate-report CLI."""

    def test_gate_all_pass_with_first_hand(self, cli_root, tmp_path):
        """With first-hand artifact in version → has_first_hand_artifact=True."""
        _setup_thesis_with_artifacts(cli_root, tmp_path, has_first_hand=True, artifact_in_version=True)

        # Add entity + target + target-case
        _cli(cli_root, "create-entity", "--entity-id", "ent_g", "--entity-type", "company",
             "--name", "Gate Corp", "--symbol", "GC", "--jurisdiction", "US")
        _cli(cli_root, "create-target", "--target-id", "tgt_g", "--entity-id", "ent_g",
             "--asset-class", "us_equity", "--venue", "NYSE", "--ticker-or-symbol", "GC", "--currency", "USD")
        _cli(cli_root, "create-target-case", "--target-case-id", "tc_g",
             "--thesis-version-id", "thv_g_v1", "--target-id", "tgt_g",
             "--exposure-type", "direct", "--capture-link-strength", "0.8",
             "--key-metric", "rev=Revenue growth")

        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_g")
        item = gate["items"][0]
        assert item["checks"]["has_first_hand_artifact"] is True
        assert item["first_hand_artifact_count"] >= 1

    @pytest.mark.regression
    def test_gate_fails_without_artifact_link(self, cli_root, tmp_path):
        """Regression: created_from_artifacts_json=[] → has_first_hand_artifact=False."""
        _setup_thesis_with_artifacts(cli_root, tmp_path, has_first_hand=True, artifact_in_version=False)

        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_g")
        item = gate["items"][0]
        assert item["checks"]["has_first_hand_artifact"] is False

    def test_gate_fails_without_first_hand_source(self, cli_root, tmp_path):
        """Second-hand source → has_first_hand_artifact=False."""
        _setup_thesis_with_artifacts(cli_root, tmp_path, has_first_hand=False, artifact_in_version=True)

        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_g")
        item = gate["items"][0]
        assert item["checks"]["has_first_hand_artifact"] is False

    def test_gate_target_mapping_with_case(self, cli_root, tmp_path):
        """Target case exists → has_target_mapping=True."""
        _setup_thesis_with_artifacts(cli_root, tmp_path)

        _cli(cli_root, "create-entity", "--entity-id", "ent_g2", "--entity-type", "company",
             "--name", "Corp G2", "--symbol", "G2", "--jurisdiction", "US")
        _cli(cli_root, "create-target", "--target-id", "tgt_g2", "--entity-id", "ent_g2",
             "--asset-class", "us_equity", "--venue", "NYSE", "--ticker-or-symbol", "G2", "--currency", "USD")
        _cli(cli_root, "create-target-case", "--target-case-id", "tc_g2",
             "--thesis-version-id", "thv_g_v1", "--target-id", "tgt_g2",
             "--exposure-type", "direct", "--capture-link-strength", "0.9",
             "--key-metric", "margin=Gross margin")

        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_g")
        item = gate["items"][0]
        assert item["checks"]["has_target_mapping"] is True
        assert item["target_case_count"] >= 1

    def test_gate_missing_fields(self, cli_root, tmp_path):
        """No target case → missing has_target_mapping."""
        _setup_thesis_with_artifacts(cli_root, tmp_path)

        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_g")
        item = gate["items"][0]
        missing = item.get("missing", [])
        assert "has_target_mapping" in missing
