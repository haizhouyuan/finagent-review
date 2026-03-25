"""Network topology analysis for investment insight discovery.

Implements graph-theoretic metrics that reveal structural patterns
in the industry chain:

- **Betweenness centrality** → identifies "chokepoint" suppliers
  that sit on critical paths (e.g. 再升科技)
- **PageRank** → ranks entities by influence propagation
- **Community detection** → discovers industry clusters
- **Structural holes** → finds brokerage opportunities
"""

from __future__ import annotations

import logging
from typing import Any

import networkx as nx

from .store import GraphStore

logger = logging.getLogger(__name__)


class TopologyAnalyzer:
    """Network topology analysis engine."""

    def __init__(self, store: GraphStore):
        self.store = store

    @property
    def g(self) -> nx.DiGraph:
        return self.store.g

    # ── Centrality metrics ──────────────────────────────────────────

    def betweenness_centrality(
        self,
        *,
        top_k: int = 20,
        normalized: bool = True,
    ) -> list[dict[str, Any]]:
        """Compute betweenness centrality to find chokepoint nodes.

        High betweenness = many shortest paths pass through this node
        → potential single point of failure in supply chain.
        """
        if self.g.number_of_nodes() == 0:
            return []

        bc = nx.betweenness_centrality(self.g, normalized=normalized)
        ranked = sorted(bc.items(), key=lambda x: -x[1])[:top_k]

        results = []
        for node_id, score in ranked:
            node = self.store.get_node(node_id)
            results.append({
                "node_id": node_id,
                "label": node.get("label", node_id) if node else node_id,
                "node_type": node.get("node_type", "unknown") if node else "unknown",
                "betweenness": round(score, 4),
                "degree": self.g.degree(node_id),
                "in_degree": self.g.in_degree(node_id),
                "out_degree": self.g.out_degree(node_id),
            })

        return results

    def pagerank(
        self,
        *,
        top_k: int = 20,
        alpha: float = 0.85,
    ) -> list[dict[str, Any]]:
        """Compute PageRank to find most influential entities.

        High PageRank = entity that many important entities link to.
        """
        if self.g.number_of_nodes() == 0:
            return []

        pr = nx.pagerank(self.g, alpha=alpha)
        ranked = sorted(pr.items(), key=lambda x: -x[1])[:top_k]

        results = []
        for node_id, score in ranked:
            node = self.store.get_node(node_id)
            results.append({
                "node_id": node_id,
                "label": node.get("label", node_id) if node else node_id,
                "node_type": node.get("node_type", "unknown") if node else "unknown",
                "pagerank": round(score, 6),
            })

        return results

    def degree_distribution(self) -> dict[str, Any]:
        """Compute degree distribution statistics."""
        if self.g.number_of_nodes() == 0:
            return {"total_nodes": 0}

        degrees = [d for _, d in self.g.degree()]
        in_degrees = [d for _, d in self.g.in_degree()]
        out_degrees = [d for _, d in self.g.out_degree()]

        return {
            "total_nodes": self.g.number_of_nodes(),
            "total_edges": self.g.number_of_edges(),
            "avg_degree": round(sum(degrees) / len(degrees), 2),
            "max_degree": max(degrees),
            "avg_in_degree": round(sum(in_degrees) / len(in_degrees), 2),
            "max_in_degree": max(in_degrees),
            "avg_out_degree": round(sum(out_degrees) / len(out_degrees), 2),
            "max_out_degree": max(out_degrees),
        }

    # ── Community detection ─────────────────────────────────────────

    def detect_communities(self) -> list[dict[str, Any]]:
        """Detect communities using Louvain method.

        Returns list of communities with member details.
        """
        undirected = self.g.to_undirected()
        if undirected.number_of_nodes() == 0:
            return []

        try:
            communities = nx.community.louvain_communities(undirected, seed=42)
        except Exception:
            communities = list(nx.connected_components(undirected))

        results = []
        for cid, members in enumerate(communities):
            member_details = []
            for node_id in members:
                node = self.store.get_node(node_id)
                member_details.append({
                    "node_id": node_id,
                    "label": node.get("label", node_id) if node else node_id,
                    "node_type": node.get("node_type", "unknown") if node else "unknown",
                })

            results.append({
                "community_id": cid,
                "size": len(members),
                "members": member_details,
            })

        results.sort(key=lambda c: -c["size"])
        return results

    # ── Structural analysis ─────────────────────────────────────────

    def structural_holes(self, *, top_k: int = 10) -> list[dict[str, Any]]:
        """Find nodes that bridge otherwise disconnected communities.

        Uses Burt's constraint metric — lower constraint = more
        structural holes = more brokerage power.
        """
        undirected = self.g.to_undirected()
        if undirected.number_of_nodes() < 3:
            return []

        try:
            constraint = nx.constraint(undirected)
        except Exception:
            return []

        # Lower constraint = more structural holes
        ranked = sorted(
            [(n, c) for n, c in constraint.items() if c > 0],
            key=lambda x: x[1],
        )[:top_k]

        results = []
        for node_id, score in ranked:
            node = self.store.get_node(node_id)
            results.append({
                "node_id": node_id,
                "label": node.get("label", node_id) if node else node_id,
                "node_type": node.get("node_type", "unknown") if node else "unknown",
                "constraint": round(score, 4),
                "interpretation": "high brokerage potential" if score < 0.5 else "embedded in cluster",
            })

        return results

    def hub_nodes(self, *, min_degree: int = 5) -> list[dict[str, Any]]:
        """Identify hub nodes (high-degree connectors).

        These are potential "gravity well" nodes that need Adaptive-k
        pruning during retrieval.
        """
        results = []
        for node_id, degree in self.g.degree():
            if degree >= min_degree:
                node = self.store.get_node(node_id)
                results.append({
                    "node_id": node_id,
                    "label": node.get("label", node_id) if node else node_id,
                    "node_type": node.get("node_type", "unknown") if node else "unknown",
                    "degree": degree,
                    "in_degree": self.g.in_degree(node_id),
                    "out_degree": self.g.out_degree(node_id),
                })

        results.sort(key=lambda n: -n["degree"])
        return results

    # ── Supply chain specific ───────────────────────────────────────

    def critical_path_analysis(
        self,
        source: str,
        target: str,
    ) -> list[list[str]]:
        """Find all simple paths between two nodes.

        Useful for supply chain risk analysis: if all paths from raw
        material to final product pass through a single supplier,
        that supplier is a critical chokepoint.
        """
        if source not in self.g or target not in self.g:
            return []

        try:
            paths = list(nx.all_simple_paths(self.g, source, target, cutoff=5))
        except nx.NetworkXError:
            return []

        return paths

    def supply_chain_depth(self, node_id: str) -> dict[str, int]:
        """Calculate how many layers upstream and downstream a node has."""
        supply_types = {"supplies_core_part_to", "component_of", "launch_service_for"}

        def _count_depth(nid: str, direction: str, visited: set) -> int:
            if nid in visited:
                return 0
            visited.add(nid)
            max_d = 0

            if direction == "upstream":
                for edge in self.store.in_edges(nid):
                    if edge["edge_type"] in supply_types:
                        d = _count_depth(edge["source_id"], direction, visited)
                        max_d = max(max_d, d + 1)
            else:
                for edge in self.store.out_edges(nid):
                    if edge["edge_type"] in supply_types:
                        d = _count_depth(edge["target_id"], direction, visited)
                        max_d = max(max_d, d + 1)
            return max_d

        return {
            "upstream_depth": _count_depth(node_id, "upstream", set()),
            "downstream_depth": _count_depth(node_id, "downstream", set()),
        }

    # ── Summary report ──────────────────────────────────────────────

    def full_report(self) -> dict[str, Any]:
        """Generate a comprehensive topology report."""
        return {
            "degree_distribution": self.degree_distribution(),
            "top_betweenness": self.betweenness_centrality(top_k=10),
            "top_pagerank": self.pagerank(top_k=10),
            "communities": self.detect_communities(),
            "hub_nodes": self.hub_nodes(),
        }
