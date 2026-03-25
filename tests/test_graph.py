"""Layer 2b: Graph module tests — conflict detection via nx.DiGraph."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import networkx as nx

from finagent.graph.schema import NodeType, EdgeType
from finagent.graph.conflict_detector import detect_conflicts


def _build_graph_with_claims(
    entity_id: str = "ent_test",
    claims: list[dict] | None = None,
) -> nx.DiGraph:
    """Helper to build a test graph with claims linked to an entity."""
    g = nx.DiGraph()
    g.add_node(entity_id, node_type=NodeType.ENTITY.value, name="Test Entity")

    if claims is None:
        claims = []

    for claim in claims:
        g.add_node(
            claim["claim_id"],
            node_type=NodeType.CLAIM.value,
            direction=claim.get("direction", "neutral"),
            review_status=claim.get("review_status", "unreviewed"),
            text=claim.get("text", ""),
        )
        g.add_edge(
            claim["claim_id"],
            entity_id,
            edge_type=EdgeType.ABOUT.value,
        )
    return g


class TestConflictDetector:
    """Test detect_conflicts with networkx DiGraph."""

    def test_no_conflict_same_direction(self):
        """Two positive claims → no conflict."""
        g = _build_graph_with_claims(claims=[
            {"claim_id": "c1", "direction": "positive", "text": "Revenue growing"},
            {"claim_id": "c2", "direction": "positive", "text": "Margins expanding"},
        ])
        conflicts = detect_conflicts(g)
        assert len(conflicts) == 0

    def test_conflict_positive_vs_negative(self):
        """Positive and negative claims about same entity → conflict."""
        g = _build_graph_with_claims(claims=[
            {"claim_id": "c1", "direction": "positive", "text": "Demand strong"},
            {"claim_id": "c2", "direction": "negative", "text": "Overcapacity risk"},
        ])
        conflicts = detect_conflicts(g)
        assert len(conflicts) == 1
        assert conflicts[0]["entity_id"] == "ent_test"
        assert conflicts[0]["left_claim_id"] == "c1"
        assert conflicts[0]["right_claim_id"] == "c2"

    def test_refuted_claim_still_listed(self):
        """Refuted claims still appear in conflicts but marked resolved."""
        g = _build_graph_with_claims(claims=[
            {"claim_id": "c1", "direction": "positive", "text": "Bull claim", "review_status": "unreviewed"},
            {"claim_id": "c2", "direction": "negative", "text": "Bear claim", "review_status": "refuted"},
        ])
        conflicts = detect_conflicts(g)
        assert len(conflicts) == 1
        assert conflicts[0]["resolved"] is True

    def test_different_entities_no_conflict(self):
        """Positive and negative on different entities → no conflict."""
        g = nx.DiGraph()
        g.add_node("ent_a", node_type=NodeType.ENTITY.value, name="Entity A")
        g.add_node("ent_b", node_type=NodeType.ENTITY.value, name="Entity B")
        g.add_node("c1", node_type=NodeType.CLAIM.value, direction="positive",
                   review_status="unreviewed", text="Bull on A")
        g.add_node("c2", node_type=NodeType.CLAIM.value, direction="negative",
                   review_status="unreviewed", text="Bear on B")
        g.add_edge("c1", "ent_a", edge_type=EdgeType.ABOUT.value)
        g.add_edge("c2", "ent_b", edge_type=EdgeType.ABOUT.value)

        conflicts = detect_conflicts(g)
        assert len(conflicts) == 0

    def test_neutral_claims_no_conflict(self):
        """Neutral claims should not generate conflicts."""
        g = _build_graph_with_claims(claims=[
            {"claim_id": "c1", "direction": "neutral", "text": "Observation 1"},
            {"claim_id": "c2", "direction": "positive", "text": "Bull"},
        ])
        conflicts = detect_conflicts(g)
        assert len(conflicts) == 0

    def test_multiple_conflicts(self):
        """Multiple positive vs multiple negative → all pairs detected."""
        g = _build_graph_with_claims(claims=[
            {"claim_id": "c1", "direction": "positive", "text": "Bull 1"},
            {"claim_id": "c2", "direction": "positive", "text": "Bull 2"},
            {"claim_id": "c3", "direction": "negative", "text": "Bear 1"},
        ])
        conflicts = detect_conflicts(g)
        assert len(conflicts) == 2  # c1-c3 and c2-c3

    def test_empty_graph(self):
        """Empty graph → no conflicts."""
        g = nx.DiGraph()
        conflicts = detect_conflicts(g)
        assert len(conflicts) == 0
