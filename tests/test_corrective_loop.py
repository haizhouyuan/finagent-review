from __future__ import annotations

import os
import tempfile

from finagent.agents.evidence_store import EvidenceStore
from finagent.agents.searcher import searcher_node
from finagent.agents.state import initial_state
from finagent.retrieval_stack import RetrievalResult


def test_good_query_passes_once():
    calls: list[str] = []

    def search_fn(query: str) -> str:
        calls.append(query)
        return "九号 Fz 系列采用铝合金轮毂，金谷进入其供应链，覆盖门店销量信息。" * 3

    def llm_fn(system: str, user: str) -> str:
        if "只回答 sufficient 或 insufficient" in system:
            return "sufficient"
        return "九号铝轮毂供应链"

    with tempfile.TemporaryDirectory() as td:
        store = EvidenceStore(os.path.join(td, "evidence.db"))
        state = initial_state("两轮车")
        state["pending_queries"] = ["九号供应链"]

        result = searcher_node(
            state,
            search_fn=search_fn,
            evidence_store=store,
            llm_fn=llm_fn,
        )

        assert calls == ["九号供应链"]
        assert len(result["gathered_evidence"]) == 1
        assert result["completed_queries"] == ["九号供应链"]
        store.close()


def test_bad_query_retries_until_sufficient():
    calls: list[str] = []

    def search_fn(query: str) -> str:
        calls.append(query)
        if query == "差查询":
            return "九号门店有车，信息很少，暂时只有零散描述，没有供应链细节。" * 2
        return "九号 Fz 系列采用铝合金轮毂，金谷进入其供应链，竞品对比完整。" * 3

    def llm_fn(system: str, user: str) -> str:
        if "只回答 sufficient 或 insufficient" in system:
            return "insufficient" if "信息很少" in user else "sufficient"
        return "九号铝轮毂供应链"

    with tempfile.TemporaryDirectory() as td:
        store = EvidenceStore(os.path.join(td, "evidence.db"))
        state = initial_state("两轮车")
        state["pending_queries"] = ["差查询"]

        result = searcher_node(
            state,
            search_fn=search_fn,
            evidence_store=store,
            llm_fn=llm_fn,
            max_retries=2,
        )

        assert calls == ["差查询", "九号铝轮毂供应链"]
        assert len(result["gathered_evidence"]) == 1
        assert result["completed_queries"] == ["差查询", "九号铝轮毂供应链"]
        store.close()


def test_max_retries_gracefully_degrades():
    calls: list[str] = []

    def search_fn(query: str) -> str:
        calls.append(query)
        return "九号门店有车，信息很少，暂时只有零散描述，没有供应链细节。" * 2

    def llm_fn(system: str, user: str) -> str:
        if "只回答 sufficient 或 insufficient" in system:
            return "insufficient"
        return "九号铝轮毂供应链"

    with tempfile.TemporaryDirectory() as td:
        store = EvidenceStore(os.path.join(td, "evidence.db"))
        state = initial_state("两轮车")
        state["pending_queries"] = ["差查询"]

        result = searcher_node(
            state,
            search_fn=search_fn,
            evidence_store=store,
            llm_fn=llm_fn,
            max_retries=2,
        )

        assert calls == ["差查询", "九号铝轮毂供应链"]
        assert len(result["gathered_evidence"]) == 1
        assert result["completed_queries"] == ["差查询", "九号铝轮毂供应链"]
        store.close()


def test_no_llm_no_retry():
    calls: list[str] = []

    def search_fn(query: str) -> str:
        calls.append(query)
        return "雅迪价格带集中在 3999-4999 元，冠能系列持续上量。" * 3

    state = initial_state("两轮车")
    state["pending_queries"] = ["雅迪价格带"]

    result = searcher_node(state, search_fn=search_fn, llm_fn=None)
    assert calls == ["雅迪价格带"]
    assert len(result["gathered_evidence"]) == 1
    assert result["completed_queries"] == ["雅迪价格带"]


def test_retrieval_stack_path_keeps_refs_contract():
    class StubRetrievalStack:
        def search(self, query: str, *, top_k: int = 5):
            return [
                RetrievalResult(
                    source="memory",
                    query=query,
                    content="九号门店反馈其铝轮毂供应链稳定，金谷供货持续，"
                    "并且轮毂规格与竞品差异清晰，适合作为检索上下文。" * 2,
                    score=1.0,
                    metadata={},
                )
            ]

    state = initial_state("两轮车")
    state["pending_queries"] = ["九号供应链"]

    result = searcher_node(state, retrieval_stack=StubRetrievalStack())
    assert len(result["gathered_evidence"]) == 1
    assert "_text" in result["gathered_evidence"][0]
    assert "raw_text" not in result["gathered_evidence"][0]
