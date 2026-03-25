"""Tests for agents — Phase 2 Agent Orchestration Layer.

Updated for architectural hardening fixes:
- Fix #1: EvidenceStore reference passing
- Fix #2: Front-loaded entity resolution
- Fix #3: exact_quote validation
- Fix #4: Confidence decay
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest


# ── State tests ─────────────────────────────────────────────────────


def test_initial_state():
    from finagent.agents.state import initial_state
    s = initial_state("test goal", context="电力设备", max_iterations=5, token_budget=10000)
    assert s["research_goal"] == "test goal"
    assert s["context"] == "电力设备"
    assert s["max_iterations"] == 5
    assert s["token_budget_remaining"] == 10000
    assert s["should_continue"] is True
    assert s["termination_reason"] is None


# ── Safety tests ────────────────────────────────────────────────────


def test_safety_continue():
    from finagent.agents.safety import SafetyGuard, SafetyVerdict
    guard = SafetyGuard(max_iterations=10, token_budget=50000)
    state = {
        "iteration_step": 2,
        "max_iterations": 10,
        "token_budget_remaining": 40000,
        "new_triples": [{"head": "A", "tail": "B"}],
        "pending_queries": ["q1"],
    }
    assert guard.check(state) == SafetyVerdict.CONTINUE


def test_safety_halt_iteration():
    from finagent.agents.safety import SafetyGuard, SafetyVerdict
    guard = SafetyGuard(max_iterations=3)
    state = {
        "iteration_step": 3,
        "max_iterations": 3,
        "token_budget_remaining": 10000,
        "new_triples": [],
        "pending_queries": [],
    }
    assert guard.check(state) == SafetyVerdict.HALT


def test_safety_halt_budget():
    from finagent.agents.safety import SafetyGuard, SafetyVerdict
    guard = SafetyGuard(token_budget=50000)
    state = {
        "iteration_step": 1,
        "max_iterations": 10,
        "token_budget_remaining": 0,
        "new_triples": [{"x": 1}],
        "pending_queries": [],
    }
    assert guard.check(state) == SafetyVerdict.HALT


def test_safety_warn_approaching_limit():
    from finagent.agents.safety import SafetyGuard, SafetyVerdict
    guard = SafetyGuard(max_iterations=3)
    state = {
        "iteration_step": 2,
        "max_iterations": 3,
        "token_budget_remaining": 40000,
        "new_triples": [{"x": 1}],
        "pending_queries": ["q1"],
    }
    assert guard.check(state) == SafetyVerdict.WARN


def test_safety_stuck_detection():
    from finagent.agents.safety import SafetyGuard, SafetyVerdict
    guard = SafetyGuard(stuck_rounds_limit=2)
    state_base = {
        "iteration_step": 1,
        "max_iterations": 10,
        "token_budget_remaining": 40000,
        "new_triples": [],
        "pending_queries": ["q1"],
    }
    guard.check(state_base)
    result = guard.check(state_base)
    assert result == SafetyVerdict.HALT


# ── Evidence Store tests ────────────────────────────────────────────

@pytest.fixture
def tmp_evidence_store():
    from finagent.agents.evidence_store import EvidenceStore
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = EvidenceStore(db_path)
        yield store
        store.close()
    finally:
        os.unlink(db_path)


def test_evidence_store_roundtrip(tmp_evidence_store):
    """Store raw text, get back only metadata ref, then fetch by ID."""
    raw_text = "蓝箭航天是一家商业火箭公司" * 100
    ref = tmp_evidence_store.store("蓝箭航天", raw_text, "web_search")

    # Ref should NOT contain raw text
    assert "text" not in ref
    assert ref["evidence_id"] is not None
    assert ref["char_count"] == len(raw_text)
    assert ref["query"] == "蓝箭航天"

    # Fetch by ID should return full text
    fetched = tmp_evidence_store.fetch(ref["evidence_id"])
    assert fetched == raw_text


def test_evidence_store_batch_fetch(tmp_evidence_store):
    ids = []
    for i in range(5):
        ref = tmp_evidence_store.store(f"q{i}", f"result_{i}", "test")
        ids.append(ref["evidence_id"])

    batch = tmp_evidence_store.fetch_batch(ids)
    assert len(batch) == 5
    assert batch[ids[2]] == "result_2"


# ── Planner tests ───────────────────────────────────────────────────

@pytest.fixture
def tmp_graph_store():
    from finagent.graph_v2.store import GraphStore
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = GraphStore(db_path)
        yield store
        store.close()
    finally:
        os.unlink(db_path)


def test_planner_fallback_no_llm(tmp_graph_store):
    from finagent.agents.planner import planner_node
    from finagent.agents.state import initial_state

    state = initial_state("商业航天产业链分析")
    result = planner_node(state, graph_store=tmp_graph_store)

    assert "pending_queries" in result
    assert len(result["pending_queries"]) >= 1
    assert result.get("iteration_step", 0) >= 1


def test_planner_with_mock_llm(tmp_graph_store):
    import json
    from finagent.agents.planner import planner_node
    from finagent.agents.state import initial_state

    def mock_llm(system: str, user: str) -> str:
        return json.dumps({
            "analysis": "图谱为空",
            "missing": ["核心企业"],
            "superfluous": [],
            "queries": [
                {"query": "蓝箭航天 供应商", "priority": 1},
                {"query": "星河动力 竞争", "priority": 2},
            ],
            "confidence": 0.2,
        }, ensure_ascii=False)

    state = initial_state("商业航天")
    result = planner_node(state, llm_fn=mock_llm, graph_store=tmp_graph_store)

    assert len(result["pending_queries"]) == 2
    assert result["confidence_score"] == 0.2


# ── Searcher tests ──────────────────────────────────────────────────


def test_searcher_with_evidence_store(tmp_evidence_store):
    """Searcher should store text in EvidenceStore, not in state."""
    from finagent.agents.searcher import searcher_node
    from finagent.agents.state import initial_state

    def mock_search(query: str) -> str:
        return f"搜索结果: {query} 蓝箭航天是一家火箭公司。" * 10

    state = initial_state("test")
    state["pending_queries"] = ["蓝箭航天 供应链", "星河动力 竞争"]
    result = searcher_node(state, search_fn=mock_search,
                            evidence_store=tmp_evidence_store)

    assert len(result["gathered_evidence"]) == 2
    assert result["pending_queries"] == []

    # Verify: refs should NOT contain raw text
    for ref in result["gathered_evidence"]:
        assert "text" not in ref or "_text" not in ref
        assert ref["evidence_id"] is not None
        # Verify we can fetch the text back
        text = tmp_evidence_store.fetch(ref["evidence_id"])
        assert "蓝箭航天" in text


def test_searcher_dedup():
    from finagent.agents.searcher import searcher_node
    from finagent.agents.state import initial_state

    def mock_search(query: str) -> str:
        return f"result for {query}"

    state = initial_state("test")
    state["pending_queries"] = ["query A"]
    state["completed_queries"] = ["query A"]
    result = searcher_node(state, search_fn=mock_search)
    assert len(result["gathered_evidence"]) == 0


def test_searcher_inline_fallback():
    """Without evidence_store, refs should include inline _text."""
    from finagent.agents.searcher import searcher_node
    from finagent.agents.state import initial_state

    def mock_search(query: str) -> str:
        return f"result for {query} — enough text to pass the 50-char minimum threshold."

    state = initial_state("test")
    state["pending_queries"] = ["q1"]
    result = searcher_node(state, search_fn=mock_search)  # No evidence_store

    refs = result["gathered_evidence"]
    assert len(refs) == 1
    assert refs[0]["evidence_id"] is None
    assert "_text" in refs[0]  # Inline fallback


# ── Extractor tests ─────────────────────────────────────────────────


def test_extractor_exact_quote_validation(tmp_graph_store):
    """Triples with exact_quote matching source text pass; others are rejected."""
    import json
    from finagent.agents.extractor import extractor_node
    from finagent.agents.state import initial_state

    source_text = "蓝箭航天和星河动力是国内两大民营火箭企业，形成直接竞争关系。双方均瞄准商业发射市场，在中型液体火箭领域展开激烈角逐。"

    good_triple = {
        "head": "蓝箭航天", "head_type": "company",
        "relation": "competes_with",
        "tail": "星河动力", "tail_type": "company",
        "exact_quote": "蓝箭航天和星河动力是国内两大民营火箭企业，形成直接竞争关系",
        "confidence": 0.9, "valid_from": "2023-01",
    }
    bad_triple = {
        "head": "蓝箭航天", "head_type": "company",
        "relation": "invested_by",
        "tail": "红杉资本", "tail_type": "company",
        "exact_quote": "红杉资本领投蓝箭航天C轮融资",  # NOT in source text!
        "confidence": 0.8, "valid_from": "2023-01",
    }

    def mock_llm(system: str, user: str) -> str:
        return json.dumps([good_triple, bad_triple], ensure_ascii=False)

    state = initial_state("test")
    state["gathered_evidence"] = [{
        "evidence_id": None,
        "query": "test",
        "char_count": len(source_text),
        "_text": source_text,
    }]

    result = extractor_node(state, llm_fn=mock_llm, graph_store=tmp_graph_store)

    # Only the good triple should survive
    assert result["total_triples_added"] == 1
    assert tmp_graph_store.has_node("蓝箭航天")
    assert tmp_graph_store.has_node("星河动力")
    # Bad triple's entity should NOT be in graph
    assert not tmp_graph_store.has_node("红杉资本")


def test_extractor_front_loaded_entities(tmp_graph_store):
    """Candidate entities from the graph should appear in the prompt."""
    import json
    from finagent.graph_v2.ontology import NodeType
    from finagent.agents.extractor import extractor_node
    from finagent.agents.state import initial_state

    # Pre-seed an entity
    tmp_graph_store.add_node("蓝箭航天", NodeType.COMPANY, "蓝箭航天")

    captured_prompts = []
    source_text = "蓝箭航天提供发射服务。" * 10

    def mock_llm(system: str, user: str) -> str:
        captured_prompts.append(system)
        return json.dumps([{
            "head": "蓝箭航天", "head_type": "company",
            "relation": "manufactures", "tail": "朱雀二号", "tail_type": "space_system",
            "exact_quote": "蓝箭航天提供发射服务",
            "confidence": 0.9, "valid_from": "2024",
        }], ensure_ascii=False)

    state = initial_state("test")
    state["gathered_evidence"] = [{
        "evidence_id": None, "query": "test",
        "char_count": len(source_text), "_text": source_text,
    }]

    extractor_node(state, llm_fn=mock_llm, graph_store=tmp_graph_store)

    # Verify candidate entities were injected into the prompt
    assert len(captured_prompts) >= 1
    assert "蓝箭航天" in captured_prompts[0]
    assert "已有实体列表" in captured_prompts[0]


# ── Evaluator tests ─────────────────────────────────────────────────


def test_evaluator_continue(tmp_graph_store):
    from finagent.agents.evaluator import evaluator_node
    from finagent.agents.safety import SafetyGuard
    from finagent.agents.state import initial_state

    guard = SafetyGuard(max_iterations=10)
    state = initial_state("test")
    state["iteration_step"] = 1
    state["new_triples"] = [{"head": "A", "tail": "B"}]
    state["total_triples_added"] = 5

    result = evaluator_node(state, safety_guard=guard, graph_store=tmp_graph_store)
    assert result["should_continue"] is True


def test_evaluator_halt_on_limit():
    from finagent.agents.evaluator import evaluator_node
    from finagent.agents.safety import SafetyGuard
    from finagent.agents.state import initial_state

    guard = SafetyGuard(max_iterations=2)
    state = initial_state("test", max_iterations=2)
    state["iteration_step"] = 2

    result = evaluator_node(state, safety_guard=guard)
    assert result["should_continue"] is False
    assert result["termination_reason"] is not None


# ── Orchestrator tests ──────────────────────────────────────────────


def test_build_research_graph():
    from finagent.agents.orchestrator import build_research_graph
    workflow = build_research_graph()
    graph = workflow.compile()
    assert graph is not None


def test_run_research_minimal(tmp_graph_store):
    """Run a minimal research loop with mocks (max 2 iterations)."""
    import json
    from finagent.agents.orchestrator import run_research

    source_text = "EntityX和BaseEntity是同行竞争关系，在产业链中互为替代。"
    call_count = {"n": 0}

    def mock_llm(system: str, user: str) -> str:
        call_count["n"] += 1
        if "规划师" in system or "planner" in system.lower():
            return json.dumps({
                "analysis": "needs data",
                "missing": ["info"],
                "superfluous": [],
                "queries": [{"query": f"query_{call_count['n']}", "priority": 1}],
                "confidence": 0.5,
            })
        # Extractor — with exact_quote that matches source text
        return json.dumps([{
            "head": f"EntityX", "head_type": "company",
            "relation": "competes_with", "tail": "BaseEntity", "tail_type": "company",
            "exact_quote": "EntityX和BaseEntity是同行竞争关系",
            "confidence": 0.8, "valid_from": "2024-01",
        }])

    def mock_search(q: str) -> str:
        return source_text

    result = run_research(
        "test research",
        llm_fn=mock_llm,
        search_fn=mock_search,
        graph_store=tmp_graph_store,
        max_iterations=2,
        token_budget=100_000,
        verbose=False,
    )

    assert result["iteration_step"] >= 1
    assert result["termination_reason"] is not None


# ── Confidence Decay tests ──────────────────────────────────────────


def test_confidence_decay_recent_edge(tmp_graph_store):
    """A very recent edge should have near-original confidence."""
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.temporal import TemporalQuery

    tmp_graph_store.add_node("A", NodeType.COMPANY, "A")
    tmp_graph_store.add_node("B", NodeType.COMPANY, "B")
    tmp_graph_store.add_edge("A", "B", EdgeType.SUPPLIES_CORE_PART_TO,
                              valid_from="2026-03-01", confidence=0.9, source="test")

    tq = TemporalQuery(tmp_graph_store)
    edge = {"valid_from": "2026-03-01", "confidence": 0.9}
    decayed = tq.confidence_decay(edge, as_of="2026-03-22")
    assert decayed > 0.85  # Only 21 days old


def test_confidence_decay_old_edge(tmp_graph_store):
    """A 3-year-old edge should have significantly decayed confidence."""
    from finagent.graph_v2.temporal import TemporalQuery

    tq = TemporalQuery(tmp_graph_store)
    edge = {"valid_from": "2023-01-01", "confidence": 0.9}
    decayed = tq.confidence_decay(edge, as_of="2026-03-22", half_life_days=365)
    # 3+ years → ~3 half-lives → 0.9 * (1/8) ≈ 0.11
    assert decayed < 0.2


def test_stale_edges(tmp_graph_store):
    """stale_edges should find old edges below threshold."""
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.temporal import TemporalQuery

    tmp_graph_store.add_node("X", NodeType.COMPANY, "X")
    tmp_graph_store.add_node("Y", NodeType.COMPANY, "Y")
    tmp_graph_store.add_edge("X", "Y", EdgeType.COMPETES_WITH,
                              valid_from="2020-01-01", confidence=0.7, source="test")

    tq = TemporalQuery(tmp_graph_store)
    stale = tq.stale_edges(as_of="2026-03-22", threshold=0.3)
    assert len(stale) >= 1
    assert stale[0]["decayed_confidence"] < 0.3


# ── Synthesizer tests ──────────────────────────────────────────────


def test_synthesize_empty_report():
    from finagent.agents.synthesizer import synthesize_report
    from finagent.agents.state import initial_state

    state = initial_state("test")
    state["termination_reason"] = "test"
    report = synthesize_report(state)
    assert "投研图谱分析报告" in report
    assert "test" in report


def test_synthesize_with_graph(tmp_graph_store):
    from finagent.agents.synthesizer import synthesize_report
    from finagent.agents.state import initial_state
    from finagent.graph_v2.ontology import NodeType, EdgeType

    tmp_graph_store.add_node("蓝箭航天", NodeType.COMPANY, "蓝箭航天")
    tmp_graph_store.add_node("星河动力", NodeType.COMPANY, "星河动力")
    tmp_graph_store.add_edge("蓝箭航天", "星河动力", EdgeType.COMPETES_WITH,
                              valid_from="2024-01-01", source="test")

    state = initial_state("商业航天产业链")
    state["total_triples_added"] = 1
    state["confidence_score"] = 0.75
    state["termination_reason"] = "test complete"

    report = synthesize_report(state, graph_store=tmp_graph_store)
    assert "蓝箭航天" in report
    assert "核心实体" in report
