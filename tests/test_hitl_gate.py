"""Tests for HITL (Human-in-the-Loop) gate.

Verifies:
  - hitl_enabled=True interrupts before extractor
  - Run status set to awaiting_human on interrupt
  - Resume after HITL completes the run
  - Default behavior (hitl_enabled=False) unchanged
"""
from __future__ import annotations

import os
import tempfile

from finagent.agents.orchestrator import run_research, resume_research
from finagent.research_ledger import ResearchLedger
from finagent.llm_adapter import create_llm_adapter


def _make_stores(tmpdir: str):
    from finagent.graph_v2.store import GraphStore
    from finagent.agents.evidence_store import EvidenceStore

    db = os.path.join(tmpdir, "research.sqlite")
    return GraphStore(db), EvidenceStore(os.path.join(tmpdir, "evidence.db")), db


class TestHITLInterrupt:
    def test_hitl_interrupts_before_extractor(self):
        """With hitl_enabled=True, run pauses with awaiting_human status."""
        with tempfile.TemporaryDirectory() as td:
            graph_store, evidence_store, db = _make_stores(td)
            ledger = ResearchLedger(db)
            llm_fn = create_llm_adapter("mock")
            search_fn = lambda q: f"Mock: {q}。蓝箭航天和星河动力是主要竞争对手。"

            final = run_research(
                "商业航天核心供应商",
                context="航天",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=graph_store,
                evidence_store=evidence_store,
                max_iterations=2,
                verbose=False,
                ledger=ledger,
                llm_backend="mock",
                hitl_enabled=True,
            )

            run_id = final.get("run_id")
            assert run_id, "run_id should be in final state"

            # Should be interrupted
            assert final.get("hitl_interrupted") is True
            assert "extractor" in final.get("hitl_next_node", [])

            # Ledger should show awaiting_human
            run = ledger.get_run(run_id)
            assert run.status == "awaiting_human"

            # Iteration step should have advanced (planner + searcher ran)
            assert final.get("iteration_step", 0) >= 1

            graph_store.close()
            evidence_store.close()
            ledger.close()

    def test_no_hitl_by_default(self):
        """Default hitl_enabled=False completes normally."""
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

            assert final.get("hitl_interrupted") is not True
            run = ledger.get_run(final["run_id"])
            assert run.status == "completed"

            graph_store.close()
            evidence_store.close()
            ledger.close()


class TestHITLResume:
    def test_resume_after_hitl(self):
        """Resume a HITL-interrupted run completes it."""
        with tempfile.TemporaryDirectory() as td:
            graph_store, evidence_store, db = _make_stores(td)
            ledger = ResearchLedger(db)
            llm_fn = create_llm_adapter("mock")
            search_fn = lambda q: f"Mock: {q}。蓝箭航天和星河动力是主要竞争对手。"

            # Step 1: Run with HITL → should interrupt
            final = run_research(
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

            run_id = final["run_id"]
            assert final.get("hitl_interrupted") is True

            # Step 2: Resume → should complete
            resumed = resume_research(
                run_id,
                ledger=ledger,
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=graph_store,
                evidence_store=evidence_store,
                verbose=False,
            )

            assert resumed.get("run_id") == run_id
            run = ledger.get_run(run_id)
            assert run.status == "completed"

            graph_store.close()
            evidence_store.close()
            ledger.close()
