from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "sync_finagent_review_repo.py"
SPEC = importlib.util.spec_from_file_location("sync_finagent_review_repo", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_curated_paths_include_key_finagent_and_reference_files():
    rel_paths = {MODULE._safe_rel_path(path) for path in MODULE.curated_paths()}

    assert "finagent/openbb_adapter.py" in rel_paths
    assert "finagent/agents/orchestrator.py" in rel_paths
    assert "docs/research/2026-03-25_external_reference_repo_bundle_v1.md" in rel_paths
    assert (
        "external_research_repos/OpenBB/openbb_platform/core/openbb_core/app/provider_interface.py"
        in rel_paths
    )
    assert "external_research_repos/qlib/qlib/workflow/recorder.py" in rel_paths
    assert "external_research_repos/TradingAgents/tradingagents/graph/trading_graph.py" in rel_paths
    assert not any(path.startswith("external_research_repos/FinGPT/") for path in rel_paths)


def test_sync_source_files_and_context_generation(tmp_path):
    stats = MODULE.sync_source_files(tmp_path)
    assert stats["files"] > 0

    assert (tmp_path / "finagent" / "openbb_adapter.py").is_file()
    assert (tmp_path / "docs" / "research" / "2026-03-25_external_reference_repo_bundle_v1.md").is_file()
    assert (
        tmp_path
        / "external_research_repos"
        / "TradingAgents"
        / "tradingagents"
        / "graph"
        / "trading_graph.py"
    ).is_file()

    MODULE.generate_review_context(
        tmp_path,
        branch_name="review-test",
        review_instructions="Focus on borrow-vs-avoid decisions.",
        source_commit_hash="deadbeef",
    )

    payload = json.loads((tmp_path / "REVIEW_SOURCE.json").read_text(encoding="utf-8"))
    context = (tmp_path / "REVIEW_CONTEXT.md").read_text(encoding="utf-8")

    assert payload["source_commit"] == "deadbeef"
    assert payload["review_branch"] == "review-test"
    assert "OpenBB" in context
    assert "qlib" in context
    assert "TradingAgents" in context
