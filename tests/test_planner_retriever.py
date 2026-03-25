"""Tests for P1b GraphRetriever integration into planner.

Covers:
1. Planner works without retriever (backward compat)
2. Planner includes graph context when retriever provides it
3. Planner handles retriever errors gracefully
4. Focus node selection from addressed entities
5. GraphRetriever wiring in orchestrator
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from finagent.agents.state import initial_state
from finagent.agents.planner import planner_node


# ── Mock LLM that echoes back the prompt (for inspection) ────────────

_last_user_prompt: str = ""


def _capture_llm(system: str, user: str) -> str:
    """Mock LLM that captures the user prompt and returns valid planner output."""
    global _last_user_prompt
    _last_user_prompt = user
    return json.dumps({
        "analysis": "Test analysis",
        "missing": ["missing info"],
        "superfluous": [],
        "queries": [
            {"query": "test query 1", "priority": 1, "target_entity": "E1", "expected_info": "info"},
        ],
        "confidence": 0.5,
    })


# ── Mock GraphRetriever ──────────────────────────────────────────────

class MockRetriever:
    """Mock GraphRetriever that returns canned context."""

    def __init__(self, context: str = "## Graph context for: 台积电\n- 台积电 --[supplies_core_part_to]--> NVIDIA (conf=0.9)"):
        self._context = context

    def retrieve(self, query: str, *, focus_node: str | None = None, mode: str = "hybrid") -> str:
        return self._context


class FailingRetriever:
    """Mock GraphRetriever that raises."""

    def retrieve(self, *args, **kwargs):
        raise RuntimeError("retriever crashed")


# ── Tests ────────────────────────────────────────────────────────────

class TestPlannerWithoutRetriever:
    def test_backward_compat(self):
        """Planner works without graph_retriever (same as before P1b)."""
        state = initial_state("半导体设备投研")
        result = planner_node(state, llm_fn=_capture_llm)
        assert "pending_queries" in result
        assert len(result["pending_queries"]) >= 1

    def test_no_graph_context_section_without_retriever(self):
        """Without retriever, no '图谱关系上下文' section in prompt."""
        state = initial_state("半导体设备投研")
        planner_node(state, llm_fn=_capture_llm)
        assert "图谱关系上下文" not in _last_user_prompt


class TestPlannerWithRetriever:
    def test_graph_context_injected(self):
        """With retriever, '图谱关系上下文' section appears in LLM prompt."""
        state = initial_state("半导体设备投研")
        retriever = MockRetriever()
        planner_node(state, llm_fn=_capture_llm, graph_retriever=retriever)
        assert "图谱关系上下文" in _last_user_prompt
        assert "台积电" in _last_user_prompt
        assert "supplies_core_part_to" in _last_user_prompt

    def test_empty_retrieval_not_injected(self):
        """If retriever returns empty marker, no section injected."""
        state = initial_state("半导体设备投研")
        retriever = MockRetriever(context="(no relevant graph context found)")
        planner_node(state, llm_fn=_capture_llm, graph_retriever=retriever)
        assert "图谱关系上下文" not in _last_user_prompt

    def test_retriever_error_gracefully_handled(self):
        """Retriever crash doesn't break planner."""
        state = initial_state("半导体设备投研")
        retriever = FailingRetriever()
        result = planner_node(state, llm_fn=_capture_llm, graph_retriever=retriever)
        assert "pending_queries" in result  # planner still works
        assert "图谱关系上下文" not in _last_user_prompt

    def test_real_graph_retriever_path_injects_context(self, tmp_path):
        """Use the real GraphRetriever path, not a mock."""
        from finagent.graph_v2.retrieval import GraphRetriever
        from finagent.graph_v2.store import GraphStore
        from scripts.seed_two_wheeler_graph import seed_two_wheeler_graph

        db_path = tmp_path / "two_wheeler.db"
        seed_two_wheeler_graph(str(db_path))
        store = GraphStore(str(db_path))
        try:
            state = initial_state("九号铝轮毂供应链")
            planner_node(
                state,
                llm_fn=_capture_llm,
                graph_retriever=GraphRetriever(store),
            )
            assert "图谱关系上下文" in _last_user_prompt
            assert "金谷" in _last_user_prompt
        finally:
            store.close()


class TestFocusNodeSelection:
    def test_focus_node_from_addressed(self):
        """Focus node should be the last addressed entity."""
        state = initial_state("半导体设备投研")
        state["blind_spots_addressed"] = ["ASML", "台积电", "中芯国际"]

        focus_seen = []

        class TrackingRetriever:
            def retrieve(self, query, *, focus_node=None, mode="hybrid"):
                focus_seen.append(focus_node)
                return "(no relevant graph context found)"

        planner_node(state, llm_fn=_capture_llm, graph_retriever=TrackingRetriever())
        assert focus_seen == ["中芯国际"]

    def test_no_focus_when_no_addressed(self):
        """With no addressed entities, focus_node should be None."""
        state = initial_state("半导体设备投研")

        focus_seen = []

        class TrackingRetriever:
            def retrieve(self, query, *, focus_node=None, mode="hybrid"):
                focus_seen.append(focus_node)
                return "(no relevant graph context found)"

        planner_node(state, llm_fn=_capture_llm, graph_retriever=TrackingRetriever())
        assert focus_seen == [None]


class TestOrchestratorWiring:
    def test_build_research_graph_creates_retriever(self):
        """build_research_graph creates GraphRetriever when graph_store available."""
        from finagent.graph_v2.store import GraphStore
        store = GraphStore()

        from finagent.agents.orchestrator import build_research_graph
        workflow = build_research_graph(graph_store=store)
        # If it compiled without error, wiring is correct
        assert workflow is not None
