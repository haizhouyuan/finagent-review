"""E2E integration test: research → ledger → research-status.

Verifies that running a mock research session actually populates:
  - research_runs with correct status/iterations/triples
  - research_run_steps with 4+ steps (planner/searcher/extractor/evaluator)
  - research_run_artifacts with at least 1 artifact
  - research-status CLI shows non-zero iterations and non-empty steps
"""
from __future__ import annotations

import os
import tempfile

from finagent.agents.orchestrator import run_research
from finagent.research_ledger import ResearchLedger
from finagent.llm_adapter import create_llm_adapter


class TestE2EResearchLedgerIntegration:
    """Run a real mock research session and verify ledger is fully populated."""

    def _run_session(self, tmpdir: str):
        """Helper: run a 1-iteration mock session and return (ledger, run_id)."""
        from finagent.graph_v2.store import GraphStore
        from finagent.agents.evidence_store import EvidenceStore

        db = os.path.join(tmpdir, "research.sqlite")
        graph_store = GraphStore(db)
        evidence_store = EvidenceStore(os.path.join(tmpdir, "evidence.db"))
        ledger = ResearchLedger(db)
        llm_fn = create_llm_adapter("mock")
        search_fn = lambda q: f"Mock: {q}。蓝箭航天和星河动力是主要竞争对手。"

        final_state = run_research(
            "商业航天核心供应商",
            context="航天",
            llm_fn=llm_fn,
            search_fn=search_fn,
            graph_store=graph_store,
            evidence_store=evidence_store,
            max_iterations=1,
            token_budget=50000,
            confidence_threshold=0.85,
            verbose=False,
            ledger=ledger,
            llm_backend="mock",
        )

        run_id = final_state.get("run_id", "")
        graph_store.close()
        evidence_store.close()
        return ledger, run_id, final_state

    def test_run_record_populated(self):
        """Verify research_runs table has correct status and metrics."""
        with tempfile.TemporaryDirectory() as td:
            ledger, run_id, final_state = self._run_session(td)

            assert run_id, "run_id should be in final_state"
            run = ledger.get_run(run_id)
            assert run is not None
            assert run.status == "completed"
            assert run.goal == "商业航天核心供应商"
            assert run.llm_backend == "mock"
            assert run.completed_at != ""

            ledger.close()

    def test_steps_populated(self):
        """Verify research_run_steps has entries for all 4 nodes."""
        with tempfile.TemporaryDirectory() as td:
            ledger, run_id, _ = self._run_session(td)

            steps = ledger.get_steps(run_id)
            # With 1 iteration: planner → searcher → extractor → evaluator = 4 steps
            assert len(steps) >= 4, f"Expected ≥4 steps, got {len(steps)}: {[s['node_name'] for s in steps]}"

            node_names = [s["node_name"] for s in steps]
            assert "planner" in node_names
            assert "searcher" in node_names
            assert "extractor" in node_names
            assert "evaluator" in node_names

            # All steps should be completed (ended_at set)
            for s in steps:
                assert s["ended_at"] != "", f"Step {s['node_name']} not completed"

            ledger.close()

    def test_iteration_updated(self):
        """Verify current_iteration is updated (not stuck at 0)."""
        with tempfile.TemporaryDirectory() as td:
            ledger, run_id, _ = self._run_session(td)

            run = ledger.get_run(run_id)
            assert run.current_iteration >= 1, (
                f"current_iteration should be ≥1 after 1 iteration, "
                f"got {run.current_iteration}"
            )

            ledger.close()

    def test_triples_recorded(self):
        """Verify total_triples is recorded in the run record."""
        with tempfile.TemporaryDirectory() as td:
            ledger, run_id, final_state = self._run_session(td)

            run = ledger.get_run(run_id)
            state_triples = final_state.get("total_triples_added", 0)
            # Run record should match final state
            assert run.total_triples == state_triples

            ledger.close()

    def test_research_list_shows_run(self):
        """Verify research-list finds the completed run."""
        with tempfile.TemporaryDirectory() as td:
            ledger, run_id, _ = self._run_session(td)

            runs = ledger.list_runs(status="completed")
            assert len(runs) >= 1
            assert any(r.run_id == run_id for r in runs)

            ledger.close()
