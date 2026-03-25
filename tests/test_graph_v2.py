"""Tests for graph_v2 — Phase 1 Knowledge Graph Core.

Tests ontology, store, entity_resolver, temporal, retrieval,
topology, blind_spots, and migration.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from datetime import date
from pathlib import Path

import pytest

# ── Ontology tests ──────────────────────────────────────────────────


def test_node_type_enum():
    from finagent.graph_v2.ontology import NodeType
    assert NodeType.COMPANY.value == "company"
    assert NodeType("company") == NodeType.COMPANY
    assert len(NodeType) >= 10


def test_edge_type_enum():
    from finagent.graph_v2.ontology import EdgeType
    assert EdgeType.SUPPLIES_CORE_PART_TO.value == "supplies_core_part_to"
    assert len(EdgeType) >= 15


def test_resolve_edge_type_exact():
    from finagent.graph_v2.ontology import resolve_edge_type, EdgeType
    assert resolve_edge_type("competes_with") == EdgeType.COMPETES_WITH
    assert resolve_edge_type("manufactures") == EdgeType.MANUFACTURES


def test_resolve_edge_type_alias():
    from finagent.graph_v2.ontology import resolve_edge_type, EdgeType
    assert resolve_edge_type("供应") == EdgeType.SUPPLIES_CORE_PART_TO
    assert resolve_edge_type("竞争") == EdgeType.COMPETES_WITH
    assert resolve_edge_type("supplies_to") == EdgeType.SUPPLIES_CORE_PART_TO


def test_resolve_edge_type_fallback():
    from finagent.graph_v2.ontology import resolve_edge_type, EdgeType
    assert resolve_edge_type("some_unknown_relation") == EdgeType.RELATED_TO


def test_node_schema_validation():
    from finagent.graph_v2.ontology import NODE_SCHEMAS, NodeType
    schema = NODE_SCHEMAS[NodeType.COMPANY]
    assert schema.validate({"label": "Test"}) == []
    errors = schema.validate({})
    assert any("label" in e for e in errors)


def test_edge_schema_validation():
    from finagent.graph_v2.ontology import EdgeSchema
    good = {
        "edge_type": "competes_with",
        "valid_from": "2024-01-01",
        "confidence": 0.8,
        "source": "test",
    }
    assert EdgeSchema.validate(good) == []
    bad = {"confidence": 1.5}
    errors = EdgeSchema.validate(bad)
    assert len(errors) >= 3  # missing edge_type, valid_from, source + bad confidence


def test_ontology_prompt_block():
    from finagent.graph_v2.ontology import ontology_prompt_block
    text = ontology_prompt_block()
    assert "知识图谱本体定义" in text
    assert "company" in text
    assert "supplies_core_part_to" in text


# ── Store tests ─────────────────────────────────────────────────────


@pytest.fixture
def tmp_store():
    """Create a temporary GraphStore for testing."""
    from finagent.graph_v2.store import GraphStore
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = GraphStore(db_path)
        yield store
        store.close()
    finally:
        os.unlink(db_path)


def test_store_add_node(tmp_store):
    from finagent.graph_v2.ontology import NodeType
    nid = tmp_store.add_node("蓝箭航天", NodeType.COMPANY, "蓝箭航天",
                             attrs={"ticker": "688245.SH"})
    assert nid == "蓝箭航天"
    assert tmp_store.has_node("蓝箭航天")

    node = tmp_store.get_node("蓝箭航天")
    assert node is not None
    assert node["node_type"] == "company"
    assert node["label"] == "蓝箭航天"


def test_store_add_edge(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    tmp_store.add_node("A", NodeType.COMPANY, "Company A")
    tmp_store.add_node("B", NodeType.COMPANY, "Company B")

    edge_id = tmp_store.add_edge(
        "A", "B", EdgeType.SUPPLIES_CORE_PART_TO,
        valid_from="2024-01-01",
        confidence=0.9,
        source="test",
        evidence="A supplies parts to B",
    )
    assert edge_id > 0

    edges = tmp_store.edges_between("A", "B")
    assert len(edges) == 1
    assert edges[0]["edge_type"] == "supplies_core_part_to"
    assert edges[0]["confidence"] == 0.9


def test_store_auto_create_nodes(tmp_store):
    from finagent.graph_v2.ontology import EdgeType
    # Edge between non-existent nodes should auto-create them
    tmp_store.add_edge(
        "NewA", "NewB", EdgeType.COMPETES_WITH,
        valid_from="2024-01-01", source="test",
    )
    assert tmp_store.has_node("NewA")
    assert tmp_store.has_node("NewB")
    node = tmp_store.get_node("NewA")
    assert node["_auto_created"] is True


def test_store_merge_edge(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    tmp_store.add_node("A", NodeType.COMPANY, "A")
    tmp_store.add_node("B", NodeType.COMPANY, "B")

    # First add
    id1 = tmp_store.merge_edge("A", "B", EdgeType.SUPPLIES_CORE_PART_TO,
                                valid_from="2024-01-01", confidence=0.7,
                                source="source1")

    # Second add with same type — should merge (update if higher confidence)
    id2 = tmp_store.merge_edge("A", "B", EdgeType.SUPPLIES_CORE_PART_TO,
                                valid_from="2024-01-01", confidence=0.9,
                                source="source2")

    assert id1 == id2  # Same edge
    edges = tmp_store.edges_between("A", "B")
    assert len(edges) == 1
    assert edges[0]["confidence"] == 0.9  # Updated to higher confidence


def test_store_upstream_downstream(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    tmp_store.add_node("Supplier", NodeType.COMPANY, "Supplier Inc")
    tmp_store.add_node("Customer", NodeType.COMPANY, "Customer Corp")
    tmp_store.add_edge("Supplier", "Customer", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-01-01", source="test")

    upstream = tmp_store.upstream_of("Customer")
    assert len(upstream) == 1
    assert upstream[0]["node_id"] == "Supplier"

    downstream = tmp_store.downstream_of("Supplier")
    assert len(downstream) == 1
    assert downstream[0]["node_id"] == "Customer"


def test_store_alias(tmp_store):
    from finagent.graph_v2.ontology import NodeType
    tmp_store.add_node("蓝箭航天", NodeType.COMPANY, "蓝箭航天")
    tmp_store.add_alias("landspace", "蓝箭航天")
    tmp_store.add_alias("688245", "蓝箭航天", "ticker")

    assert tmp_store.resolve_alias("landspace") == "蓝箭航天"
    assert tmp_store.resolve_alias("688245") == "蓝箭航天"
    assert tmp_store.resolve_alias("nonexistent") is None


def test_store_stats(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    tmp_store.add_node("A", NodeType.COMPANY, "A")
    tmp_store.add_node("B", NodeType.COMPANY, "B")
    tmp_store.add_edge("A", "B", EdgeType.COMPETES_WITH,
                       valid_from="2024-01-01", source="test")

    stats = tmp_store.stats()
    assert stats["total_nodes"] == 2
    assert stats["total_edges"] == 1
    assert stats["node_types"]["company"] == 2


def test_store_search_nodes(tmp_store):
    from finagent.graph_v2.ontology import NodeType
    tmp_store.add_node("蓝箭航天", NodeType.COMPANY, "蓝箭航天")
    tmp_store.add_node("星河动力", NodeType.COMPANY, "星河动力")
    tmp_store.add_node("商业航天", NodeType.SECTOR, "商业航天")

    results = tmp_store.search_nodes("航天")
    assert len(results) == 2  # 蓝箭航天 + 商业航天
    assert any(r["node_id"] == "蓝箭航天" for r in results)

    results = tmp_store.search_nodes("航天", node_type=NodeType.COMPANY)
    assert len(results) == 1


def test_store_remove_node(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    tmp_store.add_node("A", NodeType.COMPANY, "A")
    tmp_store.add_node("B", NodeType.COMPANY, "B")
    tmp_store.add_edge("A", "B", EdgeType.COMPETES_WITH,
                       valid_from="2024-01-01", source="test")

    assert tmp_store.remove_node("A") is True
    assert not tmp_store.has_node("A")
    assert tmp_store.edges_between("A", "B") == []


# ── Entity Resolver tests ──────────────────────────────────────────


def test_resolver_exact_alias(tmp_store):
    from finagent.graph_v2.ontology import NodeType
    from finagent.graph_v2.entity_resolver import EntityResolver

    tmp_store.add_node("蓝箭航天", NodeType.COMPANY, "蓝箭航天")
    tmp_store.add_alias("landspace", "蓝箭航天")

    resolver = EntityResolver(tmp_store)
    result = resolver.resolve("landspace")
    assert result.canonical_id == "蓝箭航天"
    assert result.confidence == 1.0
    assert result.is_new is False


def test_resolver_builtin_alias(tmp_store):
    from finagent.graph_v2.entity_resolver import EntityResolver
    resolver = EntityResolver(tmp_store)
    result = resolver.resolve("spacex")
    assert result.canonical_id == "SpaceX"
    assert result.confidence >= 0.9
    assert result.method == "builtin_alias"


def test_resolver_new_entity(tmp_store):
    from finagent.graph_v2.entity_resolver import EntityResolver
    resolver = EntityResolver(tmp_store)
    result = resolver.resolve("完全未知的新公司")
    assert result.is_new is True
    assert result.confidence == 0.0


def test_resolver_resolve_or_create(tmp_store):
    from finagent.graph_v2.entity_resolver import EntityResolver
    resolver = EntityResolver(tmp_store)
    nid = resolver.resolve_or_create("新实体公司", "company")
    assert tmp_store.has_node(nid)


def test_resolver_chinese_normalization(tmp_store):
    from finagent.graph_v2.ontology import NodeType
    from finagent.graph_v2.entity_resolver import EntityResolver

    tmp_store.add_node("蓝箭航天", NodeType.COMPANY, "蓝箭航天")
    resolver = EntityResolver(tmp_store)
    # "浙江蓝箭航天" should normalize to "蓝箭航天"
    result = resolver.resolve("浙江蓝箭航天")
    assert not result.is_new
    assert "蓝箭航天" in result.canonical_id


# ── Temporal query tests ────────────────────────────────────────────


def test_temporal_snapshot(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.temporal import TemporalQuery

    tmp_store.add_node("A", NodeType.COMPANY, "A")
    tmp_store.add_node("B", NodeType.COMPANY, "B")

    # Edge active from 2024-01 to 2024-06
    tmp_store.add_edge("A", "B", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-01-01", valid_until="2024-06-30",
                       source="test")
    # Edge active from 2024-07 onwards
    tmp_store.add_edge("A", "B", EdgeType.COMPETES_WITH,
                       valid_from="2024-07-01", source="test")

    tq = TemporalQuery(tmp_store)

    # March 2024: should see supply edge only
    snap_mar = tq.snapshot_at("2024-03-15")
    assert snap_mar.number_of_edges() == 1

    # August 2024: should see compete edge only
    snap_aug = tq.snapshot_at("2024-08-01")
    assert snap_aug.number_of_edges() == 1


def test_temporal_edge_history(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.temporal import TemporalQuery

    tmp_store.add_node("X", NodeType.COMPANY, "X")
    tmp_store.add_node("Y", NodeType.COMPANY, "Y")
    tmp_store.add_edge("X", "Y", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2023-01-01", valid_until="2024-06-30",
                       source="test")
    tmp_store.add_edge("X", "Y", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-07-01", source="test2")

    tq = TemporalQuery(tmp_store)
    history = tq.edge_history("X", "Y")
    assert len(history) >= 2


def test_temporal_active_edges(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.temporal import TemporalQuery

    tmp_store.add_node("P", NodeType.COMPANY, "P")
    tmp_store.add_node("Q", NodeType.COMPANY, "Q")
    # Expired edge
    tmp_store.add_edge("P", "Q", EdgeType.PARTNERS_WITH,
                       valid_from="2020-01-01", valid_until="2023-12-31",
                       source="test")
    # Active edge
    tmp_store.add_edge("P", "Q", EdgeType.COMPETES_WITH,
                       valid_from="2024-01-01", source="test")

    tq = TemporalQuery(tmp_store)
    active = tq.active_edges("P", as_of="2024-06-01")
    assert len(active) == 1
    assert active[0]["edge_type"] == "competes_with"


# ── Topology tests ──────────────────────────────────────────────────


def test_topology_betweenness(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.topology import TopologyAnalyzer

    # Create a chain: A → B → C
    for n in "ABC":
        tmp_store.add_node(n, NodeType.COMPANY, n)
    tmp_store.add_edge("A", "B", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-01-01", source="test")
    tmp_store.add_edge("B", "C", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-01-01", source="test")

    topo = TopologyAnalyzer(tmp_store)
    bc = topo.betweenness_centrality()
    # B should have highest betweenness (it's in the middle)
    assert bc[0]["node_id"] == "B"


def test_topology_pagerank(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.topology import TopologyAnalyzer

    for n in "ABCD":
        tmp_store.add_node(n, NodeType.COMPANY, n)
    # Multiple nodes point to C
    tmp_store.add_edge("A", "C", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-01-01", source="test")
    tmp_store.add_edge("B", "C", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-01-01", source="test")
    tmp_store.add_edge("D", "C", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-01-01", source="test")

    topo = TopologyAnalyzer(tmp_store)
    pr = topo.pagerank()
    # C should have highest PageRank
    assert pr[0]["node_id"] == "C"


def test_topology_hub_nodes(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.topology import TopologyAnalyzer

    # Create a hub node with many connections
    tmp_store.add_node("Hub", NodeType.COMPANY, "Hub")
    for i in range(6):
        nid = f"Spoke{i}"
        tmp_store.add_node(nid, NodeType.COMPANY, nid)
        tmp_store.add_edge("Hub", nid, EdgeType.SUPPLIES_CORE_PART_TO,
                           valid_from="2024-01-01", source="test")

    topo = TopologyAnalyzer(tmp_store)
    hubs = topo.hub_nodes(min_degree=5)
    assert len(hubs) >= 1
    assert hubs[0]["node_id"] == "Hub"


# ── Blind Spot tests ───────────────────────────────────────────────


def test_blind_spot_missing_entity(tmp_store):
    from finagent.graph_v2.ontology import EdgeType
    from finagent.graph_v2.blind_spots import BlindSpotClassifier, BlindSpotType

    # Create an auto-created node (via edge auto-creation)
    tmp_store.add_edge("KnownCompany", "AutoNode", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-01-01", source="test")

    classifier = BlindSpotClassifier(tmp_store)
    spots = classifier.find_by_type(BlindSpotType.MISSING_ENTITY)
    auto_spots = [s for s in spots if s.node_id == "AutoNode"]
    assert len(auto_spots) >= 1
    assert auto_spots[0].context["auto_created"] is True


def test_blind_spot_missing_attribute(tmp_store):
    from finagent.graph_v2.ontology import NodeType
    from finagent.graph_v2.blind_spots import BlindSpotClassifier, BlindSpotType

    # Company without ticker
    tmp_store.add_node("SomeCompany", NodeType.COMPANY, "SomeCompany")

    classifier = BlindSpotClassifier(tmp_store)
    spots = classifier.find_by_type(BlindSpotType.MISSING_ATTRIBUTE)
    assert any(s.node_id == "SomeCompany" for s in spots)


def test_blind_spot_summary(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.blind_spots import BlindSpotClassifier

    tmp_store.add_node("A", NodeType.COMPANY, "A")
    tmp_store.add_edge("A", "AutoB", EdgeType.COMPETES_WITH,
                       valid_from="2024-01-01", source="test")

    classifier = BlindSpotClassifier(tmp_store)
    summary = classifier.summary()
    assert summary["total"] >= 1
    assert "by_type" in summary


# ── Retrieval tests ─────────────────────────────────────────────────


def test_retrieval_local(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.retrieval import GraphRetriever

    tmp_store.add_node("Center", NodeType.COMPANY, "Center Corp")
    tmp_store.add_node("N1", NodeType.COMPANY, "Neighbor 1")
    tmp_store.add_node("N2", NodeType.COMPANY, "Neighbor 2")
    tmp_store.add_edge("N1", "Center", EdgeType.SUPPLIES_CORE_PART_TO,
                       valid_from="2024-01-01", confidence=0.9, source="test")
    tmp_store.add_edge("Center", "N2", EdgeType.CUSTOMER_OF,
                       valid_from="2024-01-01", confidence=0.8, source="test")

    retriever = GraphRetriever(tmp_store)
    results = retriever.local_retrieve("Center", max_depth=1)
    assert len(results) >= 2


def test_retrieval_budget_enforcement(tmp_store):
    from finagent.graph_v2.ontology import NodeType, EdgeType
    from finagent.graph_v2.retrieval import GraphRetriever

    # Create many nodes to generate lots of context
    for i in range(20):
        tmp_store.add_node(f"Node{i}", NodeType.COMPANY, f"Very Long Company Name Number {i}")
        if i > 0:
            tmp_store.add_edge(f"Node{i-1}", f"Node{i}", EdgeType.SUPPLIES_CORE_PART_TO,
                               valid_from="2024-01-01", source="test")

    retriever = GraphRetriever(tmp_store, max_context_chars=500)
    text = retriever.retrieve("test", focus_node="Node10", mode="local")
    assert len(text) <= 600  # Allow small overflow for truncation marker


# ── Migration tests ─────────────────────────────────────────────────


def test_migration_v1_to_v2(tmp_store, tmp_path):
    from finagent.graph_v2.migration import migrate_v1_to_v2

    # Create a mock v1 JSON file
    v1_data = {
        "meta": {"version": "1.0"},
        "nodes": [
            {"id": "蓝箭航天", "node_type": "company", "ticker": "688245.SH"},
            {"id": "星河动力", "node_type": "company"},
            {"id": "商业航天", "node_type": "sector"},
        ],
        "edges": [
            {"source": "蓝箭航天", "target": "商业航天", "edge_type": "belongs_to"},
            {"source": "星河动力", "target": "商业航天", "edge_type": "belongs_to"},
            {"source": "蓝箭航天", "target": "星河动力", "edge_type": "competes_with",
             "confidence": 0.85},
        ],
    }
    v1_path = tmp_path / "industry_chain.json"
    v1_path.write_text(json.dumps(v1_data, ensure_ascii=False), encoding="utf-8")

    result = migrate_v1_to_v2(v1_path=v1_path, store=tmp_store)

    assert result["nodes_migrated"] == 3
    assert result["edges_migrated"] == 3
    assert tmp_store.has_node("蓝箭航天")
    assert tmp_store.has_node("星河动力")
    assert tmp_store.has_node("商业航天")

    # Check edge migration
    edges = tmp_store.edges_between("蓝箭航天", "星河动力")
    assert len(edges) == 1
    assert edges[0]["edge_type"] == "competes_with"
