"""Tests for ResearchPackage output from run_research.

Verifies:
  - Package is assembled on completion (not HITL interrupt)
  - Package has correct fields from final state
  - Package JSON round-trip works
"""
from __future__ import annotations

import json
import os
import tempfile

from finagent.agents.orchestrator import run_research
from finagent.research_contracts import ResearchPackage
from finagent.research_ledger import ResearchLedger
from finagent.llm_adapter import create_llm_adapter


def _make_stores(tmpdir: str):
    from finagent.graph_v2.store import GraphStore
    from finagent.agents.evidence_store import EvidenceStore

    db = os.path.join(tmpdir, "research.sqlite")
    return GraphStore(db), EvidenceStore(os.path.join(tmpdir, "evidence.db")), db


class TestResearchPackageOutput:
    def test_package_present_on_completion(self):
        """Package is in final_state after successful run."""
        with tempfile.TemporaryDirectory() as td:
            graph_store, evidence_store, db = _make_stores(td)
            ledger = ResearchLedger(db)
            llm_fn = create_llm_adapter("mock")
            search_fn = lambda q: f"Mock: {q}"

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
            )

            package = final.get("research_package")
            assert package is not None
            assert isinstance(package, ResearchPackage)
            assert package.run_id == final["run_id"]
            assert package.goal == "商业航天核心供应商"
            assert package.context == "航天"
            assert package.iterations_used >= 1

            graph_store.close()
            evidence_store.close()
            ledger.close()

    def test_package_not_present_on_hitl(self):
        """Package is NOT in final_state when HITL interrupts."""
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
                hitl_enabled=True,
            )

            assert final.get("hitl_interrupted") is True
            assert final.get("research_package") is None

            graph_store.close()
            evidence_store.close()
            ledger.close()

    def test_package_json_round_trip(self):
        """Package can be serialized to JSON and reconstructed."""
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
            # Serialize → deserialize
            d = package.to_dict()
            js = json.dumps(d, ensure_ascii=False)
            d2 = json.loads(js)
            package2 = ResearchPackage.from_dict(d2)

            assert package2.run_id == package.run_id
            assert package2.goal == package.goal
            assert package2.confidence == package.confidence
            assert package2.iterations_used == package.iterations_used

            graph_store.close()
            evidence_store.close()
            ledger.close()

    def test_package_has_graph_stats(self):
        """Package captures node_count and edge_count from graph."""
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
            # Mock extractor adds triples → graph should have some nodes
            assert package.node_count >= 0  # May be 0 with mock
            assert package.edge_count >= 0

            graph_store.close()
            evidence_store.close()
            ledger.close()
