"""Shared fixtures for finagent test suite."""
from __future__ import annotations

import json
import subprocess
import sys
import sqlite3
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Core fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cli_root(tmp_path):
    """Initialize a fresh finagent root and return the path."""
    root = tmp_path / "finagent"
    root.mkdir()
    subprocess.run(
        [sys.executable, "-m", "finagent.cli", "--root", str(root), "init"],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
    )
    return root


@pytest.fixture
def fresh_db(cli_root):
    """Fresh sqlite connection with Row factory."""
    conn = sqlite3.connect(cli_root / "state" / "finagent.sqlite", timeout=5)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def cli(cli_root):
    """CLI runner helper — returns callable(*args) → parsed JSON."""
    def _run(*args: str) -> dict:
        r = subprocess.run(
            [sys.executable, "-m", "finagent.cli", "--root", str(cli_root), *args],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=30,
        )
        if r.returncode != 0:
            raise RuntimeError(f"CLI failed ({r.returncode}): {r.stderr[:300]}")
        return json.loads(r.stdout.strip())
    return _run


@pytest.fixture
def test_data_dir():
    """Path to test fixture data files."""
    d = Path(__file__).parent / "fixtures"
    d.mkdir(exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# Seeded environment (pre-built thesis pipeline)
# ---------------------------------------------------------------------------

@pytest.fixture
def seeded_env(cli, cli_root, tmp_path):
    """Environment with a source, artifact, claims, theme, thesis, entity, target, target-case."""
    # Create test data file
    txt = tmp_path / "test_earnings.txt"
    txt.write_text(
        "TSMC Q1 2025 Revenue: $25.8B (+35% YoY)\n"
        "CoWoS capacity doubling in 2025\n"
        "Advanced packaging CapEx: $12B planned\n"
        "HBM demand exceeding supply through 2026\n"
        "DDR5 penetration crossing 50% in servers\n"
        "CoWoS pricing stable with slight upward pressure\n"
        "Customer backlog extends into 2027\n"
    )

    # Source
    cli(
        "create-source",
        "--source-id", "src_test_ir",
        "--source-type", "official_disclosure",
        "--name", "Test Corp IR",
        "--primaryness", "first_hand",
        "--language", "en",
    )

    # Ingest
    cli(
        "ingest-text",
        "--source-id", "src_test_ir",
        "--source-type", "official_disclosure",
        "--source-name", "Test Corp IR",
        "--primaryness", "first_hand",
        "--path", str(txt),
        "--artifact-id", "art_test_earnings",
        "--artifact-kind", "text_note",
        "--title", "Test Earnings Q1 2025",
        "--language", "en",
    )

    # Extract
    result = cli("extract-claims", "--artifact-id", "art_test_earnings", "--speaker", "CEO")

    # Theme
    cli(
        "create-theme",
        "--theme-id", "theme_test",
        "--name", "Test Theme",
        "--why-it-matters", "Test importance",
        "--maturity-stage", "growth",
        "--importance-status", "priority",
    )

    # Thesis
    cli(
        "create-thesis",
        "--thesis-id", "thesis_test",
        "--thesis-version-id", "thv_test_v1",
        "--title", "Test thesis statement",
        "--status", "framed",
        "--horizon-months", "12",
        "--theme-id", "theme_test",
        "--artifact-id", "art_test_earnings",
        "--owner", "human",
        "--statement", "Test statement for validation",
        "--mechanism-chain", "A -> B -> C",
        "--base-case", "Test base case",
        "--counter-case", "Test counter case",
        "--invalidators", "Test invalidators",
        "--human-conviction", "0.75",
    )

    # Entity + Target
    cli(
        "create-entity",
        "--entity-id", "ent_test",
        "--entity-type", "company",
        "--name", "Test Corp",
        "--alias", "TST",
        "--symbol", "TST",
        "--jurisdiction", "US",
    )
    cli(
        "create-target",
        "--target-id", "tgt_test",
        "--entity-id", "ent_test",
        "--asset-class", "us_equity",
        "--venue", "NASDAQ",
        "--ticker-or-symbol", "TST",
        "--currency", "USD",
    )

    # Target-case
    cli(
        "create-target-case",
        "--target-case-id", "tc_test",
        "--thesis-version-id", "thv_test_v1",
        "--target-id", "tgt_test",
        "--exposure-type", "direct",
        "--capture-link-strength", "0.8",
        "--key-metric", "revenue_growth=YoY revenue growth",
    )

    return {
        "root": cli_root,
        "claim_count": result.get("claim_count", 0),
        "artifact_id": "art_test_earnings",
        "thesis_id": "thesis_test",
        "thesis_version_id": "thv_test_v1",
    }
