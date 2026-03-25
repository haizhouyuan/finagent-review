from __future__ import annotations

import json
import sqlite3

from finagent.agents.evidence_store import EvidenceStore
from finagent.memory import MemoryManager
from finagent.retrieval_stack import RetrievalResult, RetrievalStack
from finagent.graph_v2.store import GraphStore
from scripts.seed_two_wheeler_graph import seed_two_wheeler_graph


def _memory(tmp_path):
    conn = sqlite3.connect(tmp_path / "state.sqlite")
    conn.row_factory = sqlite3.Row
    return MemoryManager(conn), conn


def test_retrieve_from_multiple_sources_without_llm(tmp_path):
    graph_db = tmp_path / "graph.db"
    evidence_db = tmp_path / "evidence.db"
    seed_two_wheeler_graph(str(graph_db))

    memory, conn = _memory(tmp_path)
    memory.store_episodic(
        "supply_chain",
        "金谷持续进入九号铝轮毂供应链。",
        run_id="run-1",
        confidence=0.9,
        structured_data={"supplier": "金谷", "customer": "九号"},
    )

    evidence = EvidenceStore(str(evidence_db))
    evidence.store(
        "九号供应链",
        "九号 Fz 系列采用铝合金轮毂，并由金谷进入核心供应链。",
        source_tier="secondary",
    )

    graph_store = GraphStore(str(graph_db))
    try:
        stack = RetrievalStack(
            graph_store=graph_store,
            memory=memory,
            evidence_store=evidence,
        )
        results = stack.search("九号铝轮毂供应链", top_k=5)
        sources = {result.source for result in results}
        assert {"memory", "graph", "evidence"}.issubset(sources)

        context = stack.retrieve("九号铝轮毂供应链", top_k=5, max_chars=1200)
        assert "金谷" in context
        assert len(context) <= 1200
    finally:
        graph_store.close()
        evidence.close()
        conn.close()


def test_compress_respects_budget(tmp_path):
    graph_db = tmp_path / "graph.db"
    seed_two_wheeler_graph(str(graph_db))

    graph_store = GraphStore(str(graph_db))
    try:
        stack = RetrievalStack(graph_store=graph_store)
        context = stack.retrieve("九号铝轮毂供应链", top_k=3, max_chars=180)
        assert len(context) <= 180
        assert context.startswith("Query:")
    finally:
        graph_store.close()


def test_single_source_mode_works(tmp_path):
    evidence = EvidenceStore(str(tmp_path / "evidence.db"))
    evidence.store(
        "雅迪价格",
        "雅迪冠能产品价格带集中在 3999-4999 元。",
    )

    try:
        stack = RetrievalStack(evidence_store=evidence)
        results = stack.search("雅迪价格带", top_k=3)
        assert len(results) == 1
        assert results[0].source == "evidence"
        context = stack.retrieve("雅迪价格带", top_k=3, max_chars=300)
        assert "3999-4999" in context
    finally:
        evidence.close()


def test_evidence_search_has_stronger_cjk_recall(tmp_path):
    evidence = EvidenceStore(str(tmp_path / "evidence.db"))
    evidence.store(
        "九号供应链",
        "Ninebot Fz 系列采用铝合金轮毂，由核心配套供应商供货。",
    )

    try:
        refs = evidence.search("九号铝轮供应链", limit=3)
        assert len(refs) == 1
        assert refs[0]["_score"] > 0
    finally:
        evidence.close()


def test_rewrite_and_rerank_are_feature_gated(tmp_path):
    evidence = EvidenceStore(str(tmp_path / "evidence.db"))
    evidence.store(
        "九号供应链",
        "Ninebot Fz 系列采用铝合金轮毂，由金谷进入核心配套。",
    )
    evidence.store(
        "泛供应链",
        "两轮车供应链观察，提到配套但没有品牌和轮毂细节。",
    )

    calls: list[tuple[str, str]] = []

    def llm_fn(system: str, user: str) -> str:
        calls.append((system, user))
        if "扩展成最多3条" in system:
            return '["九号铝轮供应链"]'
        payload = json.loads(user)
        scores = []
        for item in payload["results"]:
            preview = item["preview"]
            score = 9.0 if ("Ninebot" in preview or "九号" in preview) else 1.0
            scores.append({"idx": item["idx"], "score": score})
        return json.dumps({"scores": scores}, ensure_ascii=False)

    try:
        plain = RetrievalStack(evidence_store=evidence, llm_fn=llm_fn)
        plain.search("九号铝轮供应链", top_k=2)
        assert calls == []

        gated = RetrievalStack(
            evidence_store=evidence,
            llm_fn=llm_fn,
            enable_query_rewrite=True,
            enable_llm_rerank=True,
        )
        gated_results = gated.search("九号铝轮供应链", top_k=2)
        assert len(calls) == 2
        assert gated_results[0].metadata["query"] == "九号供应链"
    finally:
        evidence.close()


def test_light_rerank_prefers_exact_content(tmp_path):
    stack = RetrievalStack(enable_light_rerank=True)
    results = [
        RetrievalResult(
            source="evidence",
            query="供应链观察",
            content="两轮车供应链观察，提到配套但没有九号轮毂细节。",
            score=2.0,
            metadata={"id": 1},
        ),
        RetrievalResult(
            source="evidence",
            query="九号铝轮供应链",
            content="九号铝轮供应链已经由金谷进入核心配套。",
            score=1.0,
            metadata={"id": 2},
        ),
    ]

    ranked = stack._light_rerank("九号铝轮供应链", results, top_k=2)
    assert ranked[0].metadata["id"] == 2
