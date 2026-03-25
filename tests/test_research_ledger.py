"""Tests for finagent.research_ledger — run lifecycle, steps, artifacts."""
from __future__ import annotations

import os
import tempfile

from finagent.research_ledger import ResearchLedger
from finagent.research_contracts import RunStatus


class TestResearchLedgerRunLifecycle:
    def test_create_and_get_run(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.sqlite")
            ledger = ResearchLedger(db)

            run = ledger.create_run(goal="AI供应链", context="科技", llm_backend="mock")
            assert run.run_id.startswith("run-")
            assert run.status == "queued"
            assert run.goal == "AI供应链"

            fetched = ledger.get_run(run.run_id)
            assert fetched is not None
            assert fetched.run_id == run.run_id
            assert fetched.goal == run.goal

            ledger.close()

    def test_update_run(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.sqlite")
            ledger = ResearchLedger(db)

            run = ledger.create_run(goal="test")
            ledger.update_run(run.run_id, status="running", current_iteration=1)

            fetched = ledger.get_run(run.run_id)
            assert fetched.status == "running"
            assert fetched.current_iteration == 1

            ledger.close()

    def test_complete_run_success(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.sqlite")
            ledger = ResearchLedger(db)

            run = ledger.create_run(goal="test")
            ledger.update_run(run.run_id, status="running")
            ledger.complete_run(
                run.run_id,
                status="completed",
                total_triples=15,
                confidence_score=0.92,
                termination_reason="confidence_reached",
            )

            fetched = ledger.get_run(run.run_id)
            assert fetched.status == "completed"
            assert fetched.total_triples == 15
            assert fetched.confidence_score == 0.92
            assert fetched.completed_at != ""

            ledger.close()

    def test_complete_run_failure(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.sqlite")
            ledger = ResearchLedger(db)

            run = ledger.create_run(goal="test")
            ledger.complete_run(
                run.run_id,
                status="failed",
                error="LLM timeout",
                termination_reason="error",
            )

            fetched = ledger.get_run(run.run_id)
            assert fetched.status == "failed"
            assert fetched.error == "LLM timeout"

            ledger.close()

    def test_list_runs(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.sqlite")
            ledger = ResearchLedger(db)

            ledger.create_run(goal="run1")
            ledger.create_run(goal="run2")
            run3 = ledger.create_run(goal="run3")
            ledger.complete_run(run3.run_id, status="completed")

            all_runs = ledger.list_runs()
            assert len(all_runs) == 3

            completed_runs = ledger.list_runs(status="completed")
            assert len(completed_runs) == 1
            assert completed_runs[0].goal == "run3"

            ledger.close()

    def test_get_run_not_found(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.sqlite")
            ledger = ResearchLedger(db)

            result = ledger.get_run("run-nonexistent")
            assert result is None

            ledger.close()


class TestResearchLedgerSteps:
    def test_record_and_complete_step(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.sqlite")
            ledger = ResearchLedger(db)

            run = ledger.create_run(goal="test")
            step_id = ledger.record_step(
                run.run_id, "planner", iteration=1,
                input_keys=["goal", "context"],
            )
            assert step_id is not None

            ledger.complete_step(
                step_id,
                output_keys=["queries", "confidence"],
                token_cost_est=500,
            )

            steps = ledger.get_steps(run.run_id)
            assert len(steps) == 1
            assert steps[0]["node_name"] == "planner"
            assert steps[0]["iteration"] == 1
            assert steps[0]["ended_at"] != ""

            ledger.close()

    def test_multiple_steps(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.sqlite")
            ledger = ResearchLedger(db)

            run = ledger.create_run(goal="test")
            s1 = ledger.record_step(run.run_id, "planner", iteration=1)
            ledger.complete_step(s1)
            s2 = ledger.record_step(run.run_id, "searcher", iteration=1)
            ledger.complete_step(s2)
            s3 = ledger.record_step(run.run_id, "extractor", iteration=1)
            ledger.complete_step(s3)

            steps = ledger.get_steps(run.run_id)
            assert len(steps) == 3
            assert [s["node_name"] for s in steps] == ["planner", "searcher", "extractor"]

            ledger.close()


class TestResearchLedgerArtifacts:
    def test_record_artifact(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "test.sqlite")
            ledger = ResearchLedger(db)

            run = ledger.create_run(goal="test")
            aid = ledger.record_artifact(
                run.run_id,
                kind="report",
                path="/tmp/report.md",
                data={"length": 5000},
            )
            assert aid is not None

            artifacts = ledger.get_artifacts(run.run_id)
            assert len(artifacts) == 1
            assert artifacts[0]["kind"] == "report"
            assert artifacts[0]["path"] == "/tmp/report.md"

            ledger.close()
