from __future__ import annotations

from finagent.graph_v2.retrieval import GraphRetriever
from finagent.graph_v2.store import GraphStore
from scripts.graph_hygiene_report import report
from scripts.seed_two_wheeler_graph import seed_two_wheeler_graph


def test_seed_graph_stats_and_hygiene(tmp_path):
    db_path = tmp_path / "two_wheeler.db"

    stats = seed_two_wheeler_graph(str(db_path))
    metrics = report(str(db_path))

    assert stats["total_nodes"] >= 30
    assert stats["total_edges"] >= 60
    assert metrics["nodes"] == stats["total_nodes"]
    assert metrics["edges"] == stats["total_edges"]
    assert metrics["orphans"] == []
    assert metrics["alias_count"] >= 20


def test_seed_is_idempotent(tmp_path):
    db_path = tmp_path / "two_wheeler.db"

    first = seed_two_wheeler_graph(str(db_path))
    second = seed_two_wheeler_graph(str(db_path))

    assert second["total_nodes"] == first["total_nodes"]
    assert second["total_edges"] == first["total_edges"]


def test_alias_upstream_competitors_and_graph_context(tmp_path):
    db_path = tmp_path / "two_wheeler.db"
    seed_two_wheeler_graph(str(db_path))

    store = GraphStore(str(db_path))
    try:
        assert store.resolve_alias("雅迪") == "yadea"
        assert store.resolve_alias("JG") == "jinggu"

        upstream = {row["node_id"] for row in store.upstream_of("yadea")}
        assert "jinggu" in upstream

        competitors = {row["node_id"] for row in store.competitors_of("yadea")}
        assert {"aima", "ninebot", "tailg", "niu"}.issubset(competitors)

        retriever = GraphRetriever(store)
        # Blueprint correction: current GraphRetriever needs a focus_node for
        # reliable local Chinese context, so we assert via the real interface.
        context = retriever.retrieve("九号铝轮毂供应链", focus_node="ninebot", mode="local")
        assert "金谷" in context
        assert "铝合金轮毂" in context
    finally:
        store.close()
