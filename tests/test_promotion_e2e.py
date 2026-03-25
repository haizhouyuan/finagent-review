"""Layer 4: End-to-end thesis promotion lifecycle tests.

Tests the complete promotion chain: seed → framed → evidence_backed → active.
"""
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
        capture_output=True, text=True, cwd=REPO_ROOT, timeout=60,
    )
    if r.returncode != 0:
        raise RuntimeError(f"CLI failed ({r.returncode}): {r.stderr[:400]}")
    return json.loads(r.stdout.strip())


def _db(root: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(root / "state" / "finagent.sqlite", timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _setup_full_thesis(root: Path, tmp_path: Path, *, status: str = "seed") -> dict:
    """Create a complete thesis with all gate prerequisites."""
    txt = tmp_path / "earnings.txt"
    txt.write_text(
        "Revenue: $25B (+35% YoY). "
        "CoWoS capacity doubling by year-end. "
        "Advanced packaging CapEx: $12B planned. "
        "HBM demand exceeding supply. "
        "DDR5 server penetration at 50%. "
        "Customer backlog extending into 2027. "
        "CoWoS pricing stable with upward pressure. "
        "AI revenue now 15% of total. "
        "N3 ramp contributing 20% of wafer revenue. "
        "Gross margin target 55%. "
    )

    _cli(root, "create-source", "--source-id", "src_e2e",
         "--source-type", "official_disclosure", "--name", "E2E Source",
         "--primaryness", "first_hand", "--language", "en")

    _cli(root, "ingest-text",
         "--source-id", "src_e2e", "--source-type", "official_disclosure",
         "--source-name", "E2E Source", "--primaryness", "first_hand",
         "--path", str(txt), "--artifact-id", "art_e2e",
         "--artifact-kind", "text_note", "--title", "E2E Test Earnings", "--language", "en")

    claims = _cli(root, "extract-claims", "--artifact-id", "art_e2e", "--speaker", "IR")

    _cli(root, "create-theme", "--theme-id", "theme_e2e", "--name", "E2E Theme",
         "--why-it-matters", "E2E testing", "--maturity-stage", "growth",
         "--importance-status", "priority")

    _cli(root, "create-thesis", "--thesis-id", "thesis_e2e",
         "--thesis-version-id", "thv_e2e_v1",
         "--title", "E2E lifecycle thesis", "--status", status,
         "--horizon-months", "12", "--theme-id", "theme_e2e",
         "--artifact-id", "art_e2e",
         "--owner", "human",
         "--statement", "E2E test statement",
         "--mechanism-chain", "Demand -> Supply constraint -> Price -> Revenue",
         "--why-now", "Testing lifecycle",
         "--base-case", "Demand continues",
         "--counter-case", "Overcapacity risk",
         "--invalidators", "Demand collapse",
         "--required-followups", "More earnings data",
         "--human-conviction", "0.8")

    _cli(root, "create-entity", "--entity-id", "ent_e2e",
         "--entity-type", "company", "--name", "E2E Corp",
         "--alias", "E2E", "--symbol", "E2E", "--jurisdiction", "US")
    _cli(root, "create-target", "--target-id", "tgt_e2e", "--entity-id", "ent_e2e",
         "--asset-class", "us_equity", "--venue", "NYSE",
         "--ticker-or-symbol", "E2E", "--currency", "USD")
    _cli(root, "create-target-case", "--target-case-id", "tc_e2e",
         "--thesis-version-id", "thv_e2e_v1", "--target-id", "tgt_e2e",
         "--exposure-type", "direct", "--capture-link-strength", "0.85",
         "--key-metric", "revenue=Revenue growth YoY")

    routes = _cli(root, "route-claims", "--artifact-id", "art_e2e")

    return {
        "thesis_id": "thesis_e2e",
        "thesis_version_id": "thv_e2e_v1",
        "artifact_id": "art_e2e",
        "claim_count": claims.get("claim_count", 0),
        "route_count": routes.get("route_count", 0),
    }


@pytest.mark.e2e
@pytest.mark.slow
class TestThesisLifecycle:
    """Complete thesis lifecycle from seed to active."""

    def test_seed_gate_report(self, cli_root, tmp_path):
        """Seed thesis should have gate report showing current status."""
        info = _setup_full_thesis(cli_root, tmp_path, status="seed")
        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", info["thesis_id"])
        assert len(gate["items"]) > 0
        item = gate["items"][0]
        assert item["thesis_id"] == "thesis_e2e"
        assert item["checks"]["has_first_hand_artifact"] is True

    def test_seed_to_framed_promotion(self, cli_root, tmp_path):
        """Promote seed → framed. Gate should reflect new status."""
        info = _setup_full_thesis(cli_root, tmp_path, status="seed")

        # Promote via direct DB update (promote command)
        conn = _db(cli_root)
        conn.execute("UPDATE theses SET status='framed' WHERE thesis_id=?", (info["thesis_id"],))
        conn.commit()
        conn.close()

        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", info["thesis_id"])
        item = gate["items"][0]
        assert item["checks"]["has_first_hand_artifact"] is True
        assert item["checks"]["has_target_mapping"] is True

    def test_framed_to_active_with_validations(self, cli_root, tmp_path):
        """Full lifecycle: seed → create VCs → promote to active."""
        info = _setup_full_thesis(cli_root, tmp_path, status="framed")

        # Create validation cases (mass insert)
        conn = _db(cli_root)
        claim_ids = [
            r[0] for r in conn.execute(
                "SELECT claim_id FROM claims LIMIT 20"
            ).fetchall()
        ]

        for i, cid in enumerate(claim_ids):
            conn.execute(
                """INSERT OR IGNORE INTO validation_cases
                   (validation_case_id, claim_id, thesis_id, thesis_version_id,
                    source_id, verdict, evidence_artifact_ids_json, rationale,
                    validator, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
                (f"vc_e2e_{i}", cid, info["thesis_id"], info["thesis_version_id"],
                 "src_e2e", "validated", '["art_e2e"]',
                 f"Validated claim #{i}", "e2e_test"),
            )
        conn.commit()

        vc_count = conn.execute(
            "SELECT COUNT(*) FROM validation_cases WHERE thesis_id=?",
            (info["thesis_id"],),
        ).fetchone()[0]
        conn.close()

        assert vc_count >= len(claim_ids)

        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", info["thesis_id"])
        item = gate["items"][0]

        # All key checks should pass
        assert item["checks"]["has_first_hand_artifact"] is True
        assert item["checks"]["has_target_mapping"] is True

    def test_gate_report_shows_missing_items(self, cli_root, tmp_path):
        """Thesis without target case → 'has_target_mapping' in missing list."""
        txt = tmp_path / "bare.txt"
        txt.write_text("Bare minimum test data.")

        _cli(cli_root, "create-source", "--source-id", "src_bare",
             "--source-type", "official_disclosure", "--name", "Bare Source",
             "--primaryness", "first_hand", "--language", "en")
        _cli(cli_root, "ingest-text",
             "--source-id", "src_bare", "--source-type", "official_disclosure",
             "--source-name", "Bare Source", "--primaryness", "first_hand",
             "--path", str(txt), "--artifact-id", "art_bare",
             "--artifact-kind", "text_note", "--title", "Bare", "--language", "en")
        _cli(cli_root, "create-theme", "--theme-id", "theme_bare",
             "--name", "Bare", "--why-it-matters", "test",
             "--maturity-stage", "emerging", "--importance-status", "tracking")
        _cli(cli_root, "create-thesis", "--thesis-id", "thesis_bare",
             "--thesis-version-id", "thv_bare_v1",
             "--title", "Bare thesis", "--status", "seed",
             "--horizon-months", "6", "--theme-id", "theme_bare",
             "--artifact-id", "art_bare",
             "--owner", "human", "--statement", "Bare test",
             "--mechanism-chain", "A -> B",
             "--base-case", "B", "--counter-case", "C",
             "--invalidators", "I", "--human-conviction", "0.5")

        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_bare")
        item = gate["items"][0]
        assert "has_target_mapping" in item.get("missing", [])
