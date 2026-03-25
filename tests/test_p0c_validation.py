"""P0c Validation Pack — E2E sample runs.

Two validation scenarios per cutover schedule:

1. Full chain without HITL:
   research → ledger → package → package.json → report

2. Full chain with HITL:
   research --hitl → awaiting_human → resume → package → package.json → report
"""
from __future__ import annotations

import json
import os
import tempfile

from finagent.agents.orchestrator import run_research, resume_research
from finagent.agents.synthesizer import synthesize_report
from finagent.research_contracts import ResearchPackage, RunStatus
from finagent.research_ledger import ResearchLedger
from finagent.llm_adapter import create_llm_adapter


def _make_env(tmpdir: str):
    """Create full environment for validation run."""
    from finagent.graph_v2.store import GraphStore
    from finagent.agents.evidence_store import EvidenceStore

    db = os.path.join(tmpdir, "research.sqlite")
    gs = GraphStore(db)
    es = EvidenceStore(os.path.join(tmpdir, "evidence.db"))
    ledger = ResearchLedger(db)
    llm_fn = create_llm_adapter("mock")
    search_fn = lambda q: f"Mock search for: {q}。蓝箭航天(LandSpace)和星河动力(Galactic Energy)是商业航天主要竞争对手。SpaceX Falcon 9 是全球标杆。"
    return gs, es, ledger, llm_fn, search_fn, db


class TestP0cSample1_FullChainNoHITL:
    """Scenario 1: Complete research run without HITL."""

    def test_run_produces_complete_package(self):
        """Full chain: research → ledger → package → report."""
        with tempfile.TemporaryDirectory() as td:
            gs, es, ledger, llm_fn, search_fn, db = _make_env(td)

            # Run research
            final = run_research(
                "分析商业航天核心供应商竞争格局",
                context="商业航天",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                max_iterations=2,
                verbose=False,
                ledger=ledger,
                llm_backend="mock",
            )

            run_id = final["run_id"]

            # ── Verify: Ledger ──
            run = ledger.get_run(run_id)
            assert run.status == "completed"
            assert run.current_iteration >= 1
            assert run.total_triples >= 0

            # ── Verify: Steps tracked ──
            steps = ledger.get_steps(run_id)
            assert len(steps) >= 4, f"Expected at least 4 steps (planner/searcher/extractor/evaluator), got {len(steps)}"

            # ── Verify: Package ──
            package = final.get("research_package")
            assert isinstance(package, ResearchPackage)
            assert package.run_id == run_id
            assert package.goal == "分析商业航天核心供应商竞争格局"
            assert package.context == "商业航天"
            assert package.iterations_used >= 1

            # ── Verify: Evidence scoped to run ──
            run_evidence = es.list_all(run_id=run_id)
            assert len(package.evidence_refs) == len(run_evidence)

            # ── Verify: Report generation ──
            report = synthesize_report(final, graph_store=gs)
            assert len(report) > 0

            # ── Verify: Package JSON round-trip ──
            package.report_md = report  # Backfill as CLI does
            d = package.to_dict()
            js = json.dumps(d, ensure_ascii=False, indent=2)
            pkg_restored = ResearchPackage.from_dict(json.loads(js))
            assert pkg_restored.run_id == run_id
            assert len(pkg_restored.report_md) > 0
            assert len(pkg_restored.evidence_refs) == len(package.evidence_refs)

            # ── Verify: package.json writeable ──
            pkg_path = os.path.join(td, "package.json")
            with open(pkg_path, "w") as f:
                f.write(js)
            assert os.path.getsize(pkg_path) > 100

            gs.close()
            es.close()
            ledger.close()

    def test_research_status_shows_run(self):
        """research-status and research-list work after run."""
        with tempfile.TemporaryDirectory() as td:
            gs, es, ledger, llm_fn, search_fn, db = _make_env(td)

            final = run_research(
                "test query",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
            )

            run_id = final["run_id"]

            # Status
            run = ledger.get_run(run_id)
            assert run is not None
            assert run.status == "completed"
            assert run.goal == "test query"

            # List
            runs = ledger.list_runs(limit=10)
            assert any(r.run_id == run_id for r in runs)

            gs.close()
            es.close()
            ledger.close()


class TestP0cSample2_HITLFullChain:
    """Scenario 2: Research with HITL pause → resume → complete."""

    def test_hitl_to_resume_produces_package(self):
        """Full HITL chain: research --hitl → pause → resume → package."""
        with tempfile.TemporaryDirectory() as td:
            gs, es, ledger, llm_fn, search_fn, db = _make_env(td)

            # Step 1: Research with HITL → should pause
            final1 = run_research(
                "分析商业航天核心供应商竞争格局",
                context="商业航天",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                max_iterations=2,
                verbose=False,
                ledger=ledger,
                llm_backend="mock",
                hitl_enabled=True,
            )

            run_id = final1["run_id"]

            # ── Verify: HITL interrupt ──
            assert final1.get("hitl_interrupted") is True
            assert "extractor" in final1.get("hitl_next_node", [])
            assert final1.get("research_package") is None

            run = ledger.get_run(run_id)
            assert run.status == "awaiting_human"

            # Step 2: Resume → should complete
            final2 = resume_research(
                run_id,
                ledger=ledger,
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                verbose=False,
            )

            # ── Verify: Completed ──
            run = ledger.get_run(run_id)
            assert run.status == "completed"

            # ── Verify: Package produced after resume ──
            package = final2.get("research_package")
            assert isinstance(package, ResearchPackage)
            assert package.run_id == run_id

            # ── Verify: Evidence scoped ──
            run_evidence = es.list_all(run_id=run_id)
            assert len(package.evidence_refs) == len(run_evidence)

            # ── Verify: Report ──
            report = synthesize_report(final2, graph_store=gs)
            assert len(report) > 0

            # ── Verify: Package JSON ──
            package.report_md = report
            d = package.to_dict()
            assert len(d["report_md"]) > 0
            assert d["run_id"] == run_id

            gs.close()
            es.close()
            ledger.close()

    def test_multi_run_isolation(self):
        """Two runs on same DB have isolated evidence in packages."""
        with tempfile.TemporaryDirectory() as td:
            gs, es, ledger, llm_fn, search_fn, db = _make_env(td)

            # Run 1
            f1 = run_research(
                "商业航天",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
            )
            pkg1 = f1["research_package"]

            # Run 2
            f2 = run_research(
                "新能源汽车",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
            )
            pkg2 = f2["research_package"]

            # Evidence isolation
            total = es.list_all()
            r1_ev = es.list_all(run_id=f1["run_id"])
            r2_ev = es.list_all(run_id=f2["run_id"])

            assert len(total) == len(r1_ev) + len(r2_ev)
            assert len(pkg1.evidence_refs) == len(r1_ev)
            assert len(pkg2.evidence_refs) == len(r2_ev)

            # Packages have correct goals
            assert pkg1.goal == "商业航天"
            assert pkg2.goal == "新能源汽车"

            gs.close()
            es.close()
            ledger.close()
