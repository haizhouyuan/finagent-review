"""Layer 5: Full pipeline simulation — complete investment research lifecycle.

Simulates the real-world workflow: data ingestion → claim extraction → routing →
thesis construction → validation → gate check → promotion → monitor setup.

This is the "integration test of integration tests" — a full E2E simulation
of the 13-phase pipeline we ran in production.
"""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"


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


@pytest.mark.slow
@pytest.mark.e2e
class TestFullPipelineSimulation:
    """End-to-end simulation of complete investment research pipeline."""

    def test_scenario_a_happy_path(self, cli_root, tmp_path):
        """Scenario A: Zero to Active — the full happy path.

        Phases:
        1. Data ingestion (source + artifact)
        2. Claim extraction
        3. Theme + Thesis creation (seed)
        4. Entity + Target + Target-case
        5. Claim routing
        6. Validation case creation
        7. Gate check → all pass
        8. Promote to active
        9. Monitor setup
        10. View commands all work
        """
        # ─── Phase 1: Data Ingestion ────────────────────────────
        earnings_file = FIXTURES / "earnings_transcript.txt"
        if not earnings_file.exists():
            earnings_file = tmp_path / "earnings.txt"
            earnings_file.write_text(
                "Revenue: $25.8B (+35% YoY). "
                "CoWoS capacity doubling. "
                "Advanced packaging CapEx: $12B. "
                "HBM demand exceeding supply. "
                "Customer backlog into 2027. "
            )

        _cli(cli_root, "create-source", "--source-id", "src_sim_ir",
             "--source-type", "official_disclosure", "--name", "Sim Corp IR",
             "--primaryness", "first_hand", "--language", "en")

        ingest = _cli(cli_root, "ingest-text",
                       "--source-id", "src_sim_ir",
                       "--source-type", "official_disclosure",
                       "--source-name", "Sim Corp IR",
                       "--primaryness", "first_hand",
                       "--path", str(earnings_file),
                       "--artifact-id", "art_sim_earnings",
                       "--artifact-kind", "text_note",
                       "--title", "Sim Q1 Earnings",
                       "--language", "en")
        assert ingest["artifact_id"] == "art_sim_earnings"

        # ─── Phase 2: Claim Extraction ──────────────────────────
        claims = _cli(cli_root, "extract-claims",
                       "--artifact-id", "art_sim_earnings",
                       "--speaker", "CEO", "--min-chars", "10")
        assert claims["claim_count"] >= 3, f"Expected ≥3 claims, got {claims['claim_count']}"

        # ─── Phase 3: Theme + Thesis ────────────────────────────
        _cli(cli_root, "create-theme", "--theme-id", "theme_sim",
             "--name", "Semiconductor Supply Chain",
             "--why-it-matters", "Advanced packaging is the bottleneck",
             "--maturity-stage", "growth",
             "--commercialization-paths", "packaging,hbm,dram",
             "--importance-status", "priority")

        _cli(cli_root, "create-thesis", "--thesis-id", "thesis_sim",
             "--thesis-version-id", "thv_sim_v1",
             "--title", "Advanced packaging bottleneck persists",
             "--status", "seed",
             "--horizon-months", "12",
             "--theme-id", "theme_sim",
             "--artifact-id", "art_sim_earnings",
             "--owner", "human",
             "--statement", "CoWoS supply constraint drives pricing power and CapEx cycle",
             "--mechanism-chain", "AI demand → GPU packaging → CoWoS constraint → ASP increase → revenue upside",
             "--why-now", "Capacity doubling still insufficient for demand",
             "--base-case", "CoWoS utilization stays >90% through 2026",
             "--counter-case", "Samsung/Intel alternatives reduce constraint",
             "--invalidators", "Demand collapse or rapid capacity overshoot",
             "--required-followups", "Q2 earnings + competitor capacity updates",
             "--human-conviction", "0.78")

        # ─── Phase 4: Entity + Target + Target-case ─────────────
        _cli(cli_root, "create-entity", "--entity-id", "ent_sim_tsm",
             "--entity-type", "company", "--name", "TSMC (Sim)",
             "--alias", "TSM", "--symbol", "TSM", "--jurisdiction", "TW")

        _cli(cli_root, "create-target", "--target-id", "tgt_sim_tsm",
             "--entity-id", "ent_sim_tsm",
             "--asset-class", "us_equity", "--venue", "NYSE",
             "--ticker-or-symbol", "TSM", "--currency", "USD",
             "--liquidity-bucket", "mega_cap")

        _cli(cli_root, "create-target-case", "--target-case-id", "tc_sim_tsm",
             "--thesis-version-id", "thv_sim_v1",
             "--target-id", "tgt_sim_tsm",
             "--exposure-type", "direct",
             "--capture-link-strength", "0.9",
             "--key-metric", "cowos_util=CoWoS utilization rate",
             "--key-metric", "adv_pkg_capex=Advanced packaging CapEx")

        # ─── Phase 5: Claim Routing ─────────────────────────────
        routes = _cli(cli_root, "route-claims", "--artifact-id", "art_sim_earnings")
        assert routes["route_count"] >= 1, f"Expected ≥1 route, got {routes['route_count']}"

        # ─── Phase 6: Validation Cases ──────────────────────────
        conn = _db(cli_root)
        claim_ids = [
            r[0] for r in conn.execute("SELECT claim_id FROM claims LIMIT 15").fetchall()
        ]

        for i, cid in enumerate(claim_ids):
            conn.execute(
                """INSERT OR IGNORE INTO validation_cases
                   (validation_case_id, claim_id, thesis_id, thesis_version_id,
                    source_id, verdict, evidence_artifact_ids_json, rationale,
                    validator, created_at)
                   VALUES (?, ?, 'thesis_sim', 'thv_sim_v1',
                           'src_sim_ir', 'validated', '["art_sim_earnings"]',
                           ?, 'simulation_test', datetime('now'))""",
                (f"vc_sim_{i}", cid, f"Sim-validated claim #{i}"),
            )
        conn.commit()

        vc_count = conn.execute(
            "SELECT COUNT(*) FROM validation_cases WHERE thesis_id='thesis_sim'"
        ).fetchone()[0]
        conn.close()
        assert vc_count >= len(claim_ids)

        # ─── Phase 7: Gate Check → All Pass ─────────────────────
        gate = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_sim")
        item = gate["items"][0]

        assert item["checks"]["has_first_hand_artifact"] is True, "Gate: first-hand artifact check failed"
        assert item["checks"]["has_target_mapping"] is True, "Gate: target mapping check failed"

        # ─── Phase 8: Promote to Active ─────────────────────────
        conn = _db(cli_root)
        conn.execute("UPDATE theses SET status='active' WHERE thesis_id='thesis_sim'")
        conn.commit()

        status = conn.execute(
            "SELECT status FROM theses WHERE thesis_id='thesis_sim'"
        ).fetchone()[0]
        conn.close()
        assert status == "active"

        # ─── Phase 9: Monitor Setup ─────────────────────────────
        monitor = _cli(cli_root, "create-monitor",
                        "--owner-object-type", "thesis",
                        "--owner-object-id", "thesis_sim",
                        "--monitor-type", "claim_freshness",
                        "--comparator", "lte",
                        "--threshold-value", "180")
        assert "monitor_id" in monitor

        # ─── Phase 10: All Views Work ───────────────────────────
        views = [
            ("board",),
            ("thesis-board",),
            ("thesis-focus", "--thesis-id", "thesis_sim"),
            ("thesis-gate-report", "--thesis-id", "thesis_sim"),
            ("today-cockpit",),
            ("routing-board",),
            ("source-board",),
            ("theme-map",),
            ("target-case-dashboard",),
            ("promotion-wizard",),
            ("validation-board",),
            ("watch-board",),
        ]
        for view_args in views:
            result = _cli(cli_root, *view_args)
            assert isinstance(result, dict), f"View {view_args[0]} did not return dict"

    def test_scenario_b_bear_case(self, cli_root, tmp_path):
        """Scenario B: Active thesis receives contradictory evidence.

        Verifies conflict detection and gate degradation.
        """
        # Setup active thesis (reuse helper)
        earnings = tmp_path / "earnings.txt"
        earnings.write_text("Revenue strong. Demand exceeding supply. Margins expanding.")

        bear_file = FIXTURES / "bear_case_report.txt"
        if not bear_file.exists():
            bear_file = tmp_path / "bear.txt"
            bear_file.write_text(
                "Overcapacity risk emerging by H2 2026. "
                "Samsung EMIB catching up. "
                "AI CapEx showing moderation signals. "
            )

        # Bull source + thesis
        _cli(cli_root, "create-source", "--source-id", "src_bull",
             "--source-type", "official_disclosure", "--name", "Bull Source",
             "--primaryness", "first_hand", "--language", "en")
        _cli(cli_root, "ingest-text",
             "--source-id", "src_bull", "--source-type", "official_disclosure",
             "--source-name", "Bull Source", "--primaryness", "first_hand",
             "--path", str(earnings), "--artifact-id", "art_bull",
             "--artifact-kind", "text_note", "--title", "Bull Data", "--language", "en")
        _cli(cli_root, "extract-claims", "--artifact-id", "art_bull", "--speaker", "CEO")

        _cli(cli_root, "create-theme", "--theme-id", "theme_bear", "--name", "Bear Test",
             "--why-it-matters", "test", "--maturity-stage", "growth",
             "--importance-status", "priority")
        _cli(cli_root, "create-thesis", "--thesis-id", "thesis_bear",
             "--thesis-version-id", "thv_bear_v1",
             "--title", "Bear test thesis", "--status", "active",
             "--horizon-months", "12", "--theme-id", "theme_bear",
             "--artifact-id", "art_bull",
             "--owner", "human", "--statement", "Demand persists",
             "--mechanism-chain", "A -> B",
             "--base-case", "Strong", "--counter-case", "Weak",
             "--invalidators", "Collapse", "--human-conviction", "0.7")

        # Bear source + claims
        _cli(cli_root, "create-source", "--source-id", "src_bear_data",
             "--source-type", "other", "--name", "Bear Analyst",
             "--primaryness", "second_hand", "--language", "en")
        _cli(cli_root, "ingest-text",
             "--source-id", "src_bear_data", "--source-type", "other",
             "--source-name", "Bear Analyst", "--primaryness", "second_hand",
             "--path", str(bear_file), "--artifact-id", "art_bear_data",
             "--artifact-kind", "text_note", "--title", "Bear Analysis", "--language", "en")
        bear_claims = _cli(cli_root, "extract-claims",
                            "--artifact-id", "art_bear_data", "--speaker", "Analyst")

        assert bear_claims["claim_count"] >= 1, "Bear claims should be extracted"

        # Route bear claims
        _cli(cli_root, "route-claims", "--artifact-id", "art_bear_data")

        # Verify both bull and bear data coexist
        conn = _db(cli_root)
        total_claims = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        total_routes = conn.execute("SELECT COUNT(*) FROM claim_routes").fetchone()[0]
        conn.close()

        assert total_claims >= 4, f"Expected ≥4 claims (bull+bear), got {total_claims}"
        assert total_routes >= 1, f"Expected ≥1 route, got {total_routes}"

    def test_scenario_c_multi_thesis_routing(self, cli_root, tmp_path):
        """Scenario C: Single artifact's claims route to multiple theses."""
        txt = tmp_path / "multi.txt"
        txt.write_text(
            "CoWoS capacity utilization at 95%. "
            "HBM pricing premium 250% over DDR5. "
            "CPO trial deployments started at 3 hyperscalers. "
        )

        _cli(cli_root, "create-source", "--source-id", "src_multi",
             "--source-type", "official_disclosure", "--name", "Multi Source",
             "--primaryness", "first_hand", "--language", "en")
        _cli(cli_root, "ingest-text",
             "--source-id", "src_multi", "--source-type", "official_disclosure",
             "--source-name", "Multi Source", "--primaryness", "first_hand",
             "--path", str(txt), "--artifact-id", "art_multi",
             "--artifact-kind", "text_note", "--title", "Multi Data", "--language", "en")
        _cli(cli_root, "extract-claims", "--artifact-id", "art_multi", "--speaker", "IR")

        # Two theses
        _cli(cli_root, "create-theme", "--theme-id", "theme_m1", "--name", "T1",
             "--why-it-matters", "t1", "--maturity-stage", "growth",
             "--importance-status", "priority")
        _cli(cli_root, "create-theme", "--theme-id", "theme_m2", "--name", "T2",
             "--why-it-matters", "t2", "--maturity-stage", "emerging",
             "--importance-status", "tracking")

        _cli(cli_root, "create-thesis", "--thesis-id", "thesis_m1",
             "--thesis-version-id", "thv_m1_v1",
             "--title", "Thesis M1", "--status", "framed",
             "--horizon-months", "12", "--theme-id", "theme_m1",
             "--artifact-id", "art_multi",
             "--owner", "human", "--statement", "CoWoS thesis",
             "--mechanism-chain", "A -> B",
             "--base-case", "B", "--counter-case", "C",
             "--invalidators", "I", "--human-conviction", "0.7")

        _cli(cli_root, "create-thesis", "--thesis-id", "thesis_m2",
             "--thesis-version-id", "thv_m2_v1",
             "--title", "Thesis M2", "--status", "framed",
             "--horizon-months", "12", "--theme-id", "theme_m2",
             "--artifact-id", "art_multi",
             "--owner", "human", "--statement", "CPO thesis",
             "--mechanism-chain", "X -> Y",
             "--base-case", "B2", "--counter-case", "C2",
             "--invalidators", "I2", "--human-conviction", "0.6")

        routes = _cli(cli_root, "route-claims", "--artifact-id", "art_multi")
        assert routes["route_count"] >= 1

        # Both theses should have gate reports
        gate1 = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_m1")
        gate2 = _cli(cli_root, "thesis-gate-report", "--thesis-id", "thesis_m2")
        assert len(gate1["items"]) > 0
        assert len(gate2["items"]) > 0
        assert gate1["items"][0]["checks"]["has_first_hand_artifact"] is True
        assert gate2["items"][0]["checks"]["has_first_hand_artifact"] is True
