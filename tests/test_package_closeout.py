"""Tests for Codex-identified package closeout issues.

Verifies:
  1. Multi-run evidence isolation (no cross-contamination)
  2. HITL -> resume produces ResearchPackage
  3. Package report_md is non-empty after backfill
"""
from __future__ import annotations

import os
import tempfile

from finagent.agents.orchestrator import run_research, resume_research
from finagent.research_contracts import ResearchPackage
from finagent.research_ledger import ResearchLedger
from finagent.llm_adapter import create_llm_adapter


def _make_stores(tmpdir: str):
    """Create shared stores for multi-run tests (same DB = realistic)."""
    from finagent.graph_v2.store import GraphStore
    from finagent.agents.evidence_store import EvidenceStore

    db = os.path.join(tmpdir, "research.sqlite")
    return GraphStore(db), EvidenceStore(os.path.join(tmpdir, "evidence.db")), db


class TestMultiRunEvidenceIsolation:
    def test_package_evidence_scoped_to_run(self):
        """Two runs on the same DB produce isolated evidence in their packages."""
        with tempfile.TemporaryDirectory() as td:
            graph_store, evidence_store, db = _make_stores(td)
            ledger = ResearchLedger(db)
            llm_fn = create_llm_adapter("mock")
            search_fn = lambda q: f"Mock: {q}。蓝箭航天和星河动力是主要竞争对手。"

            # Run 1
            final1 = run_research(
                "商业航天供应商",
                context="航天",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=graph_store,
                evidence_store=evidence_store,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
                llm_backend="mock",
            )

            pkg1 = final1["research_package"]
            run1_id = final1["run_id"]
            refs1_count = len(pkg1.evidence_refs)

            # Run 2 — same stores, different topic
            final2 = run_research(
                "新能源汽车电池",
                context="新能源",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=graph_store,
                evidence_store=evidence_store,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
                llm_backend="mock",
            )

            pkg2 = final2["research_package"]
            refs2_count = len(pkg2.evidence_refs)

            # CRITICAL: Pkg2 should NOT contain Pkg1's evidence
            # Without the run_id fix, refs2_count == refs1_count + new_refs
            # With the fix, each package only gets its own run's evidence
            all_evidence = evidence_store.list_all()
            run1_evidence = evidence_store.list_all(run_id=run1_id)
            run2_evidence = evidence_store.list_all(run_id=final2["run_id"])

            # Total should be sum of per-run
            assert len(all_evidence) == len(run1_evidence) + len(run2_evidence)

            # Each package should only have its own evidence
            assert refs1_count == len(run1_evidence)
            assert refs2_count == len(run2_evidence)

            # Package evidence should NOT grow with additional runs
            assert refs2_count <= refs1_count + 5  # Reasonable bound

            graph_store.close()
            evidence_store.close()
            ledger.close()


class TestHITLResumePackage:
    def test_resume_after_hitl_has_package(self):
        """Resume after HITL produces a ResearchPackage."""
        with tempfile.TemporaryDirectory() as td:
            graph_store, evidence_store, db = _make_stores(td)
            ledger = ResearchLedger(db)
            llm_fn = create_llm_adapter("mock")
            search_fn = lambda q: f"Mock: {q}。蓝箭航天和星河动力是主要竞争对手。"

            # Step 1: Run with HITL → interrupts
            final1 = run_research(
                "商业航天核心供应商",
                context="航天",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=graph_store,
                evidence_store=evidence_store,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
                llm_backend="mock",
                hitl_enabled=True,
            )

            run_id = final1["run_id"]
            assert final1.get("hitl_interrupted") is True
            assert final1.get("research_package") is None  # No package on HITL

            # Step 2: Resume → should complete AND produce package
            final2 = resume_research(
                run_id,
                ledger=ledger,
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=graph_store,
                evidence_store=evidence_store,
                verbose=False,
            )

            package = final2.get("research_package")
            assert package is not None, "resume should produce ResearchPackage"
            assert isinstance(package, ResearchPackage)
            assert package.run_id == run_id

            graph_store.close()
            evidence_store.close()
            ledger.close()


class TestPackageReportMd:
    def test_package_report_md_populated_via_backfill(self):
        """Package.report_md is empty in orchestrator but backfilled in CLI.

        This test verifies the orchestrator-level package has empty report_md
        (since report is generated after package assembly), and that the
        backfill mechanism works.
        """
        with tempfile.TemporaryDirectory() as td:
            graph_store, evidence_store, db = _make_stores(td)
            ledger = ResearchLedger(db)
            llm_fn = create_llm_adapter("mock")
            search_fn = lambda q: f"Mock: {q}"

            final = run_research(
                "test",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=graph_store,
                evidence_store=evidence_store,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
            )

            package = final["research_package"]

            # Orchestrator-level: report_md is empty (by design)
            assert package.report_md == ""

            # Simulate CLI backfill
            from finagent.agents.synthesizer import synthesize_report
            report = synthesize_report(final, graph_store=graph_store)
            package.report_md = report

            assert len(package.report_md) > 0, "report_md should be non-empty after backfill"

            # Verify it survives round-trip
            d = package.to_dict()
            assert len(d["report_md"]) > 0
            pkg2 = ResearchPackage.from_dict(d)
            assert pkg2.report_md == package.report_md

            graph_store.close()
            evidence_store.close()
            ledger.close()
