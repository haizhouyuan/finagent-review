"""Industry chain knowledge graph for investment research.

Provides a NetworkX-based directed graph with typed nodes and edges,
JSON persistence, and query utilities for exploring supply chains,
competitive landscapes, and technology dependencies.

Usage::

    from finagent.graph.industry_chain import IndustryChainGraph

    g = IndustryChainGraph.load()          # load existing or create empty
    g.add_company("蓝箭航天", ticker="688245.SH", sector="商业航天")
    g.add_supply("蓝箭航天", "千帆星座", evidence="招股书 p.42")
    g.save()

    # Explore
    g.upstream_of("千帆星座")     # all suppliers
    g.downstream_of("蓝箭航天")   # all customers
    g.competitors_of("蓝箭航天")  # competitive set
    g.blind_spots()               # nodes with low info density
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import networkx as nx

from .schema import EdgeType, NodeType

logger = logging.getLogger(__name__)

# Default persistence path
_DEFAULT_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "industry_chain.json"


class IndustryChainGraph:
    """Industry chain knowledge graph backed by NetworkX + JSON."""

    def __init__(self, graph: nx.DiGraph | None = None, path: str | Path | None = None):
        self.g = graph or nx.DiGraph()
        self.path = Path(path) if path else _DEFAULT_PATH
        self._dirty = False

    # -----------------------------------------------------------------
    # Persistence
    # -----------------------------------------------------------------

    def save(self, path: str | Path | None = None) -> Path:
        """Serialize graph to JSON."""
        p = Path(path) if path else self.path
        p.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "meta": {
                "version": "1.0",
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "node_count": self.g.number_of_nodes(),
                "edge_count": self.g.number_of_edges(),
            },
            "nodes": [
                {"id": n, **attrs}
                for n, attrs in self.g.nodes(data=True)
            ],
            "edges": [
                {"source": u, "target": v, **attrs}
                for u, v, attrs in self.g.edges(data=True)
            ],
        }
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        self._dirty = False
        logger.info("industry chain saved: %d nodes, %d edges → %s",
                     self.g.number_of_nodes(), self.g.number_of_edges(), p)
        return p

    @classmethod
    def load(cls, path: str | Path | None = None) -> "IndustryChainGraph":
        """Load graph from JSON, or create empty if not found."""
        p = Path(path) if path else _DEFAULT_PATH
        if not p.exists():
            logger.info("no existing graph at %s, creating empty", p)
            return cls(path=p)

        data = json.loads(p.read_text(encoding="utf-8"))
        g = nx.DiGraph()
        for node in data.get("nodes", []):
            nid = node.pop("id")
            g.add_node(nid, **node)
        for edge in data.get("edges", []):
            src = edge.pop("source")
            tgt = edge.pop("target")
            g.add_edge(src, tgt, **edge)
        logger.info("industry chain loaded: %d nodes, %d edges ← %s",
                     g.number_of_nodes(), g.number_of_edges(), p)
        return cls(graph=g, path=p)

    # -----------------------------------------------------------------
    # Node builders (typed convenience methods)
    # -----------------------------------------------------------------

    def _add_node(self, name: str, node_type: NodeType, **attrs: Any) -> str:
        """Add or update a node. Returns the node id (= name)."""
        existing = dict(self.g.nodes.get(name, {}))
        existing.update(attrs)
        existing["node_type"] = node_type.value
        existing.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        existing["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.g.add_node(name, **existing)
        self._dirty = True
        return name

    def add_company(self, name: str, *, ticker: str | None = None,
                    sector: str | None = None, listed: bool = True,
                    **attrs: Any) -> str:
        """Add a company node."""
        return self._add_node(name, NodeType.COMPANY,
                              ticker=ticker, sector=sector, listed=listed, **attrs)

    def add_product(self, name: str, **attrs: Any) -> str:
        return self._add_node(name, NodeType.PRODUCT_LINE, **attrs)

    def add_material(self, name: str, **attrs: Any) -> str:
        return self._add_node(name, NodeType.MATERIAL, **attrs)

    def add_project(self, name: str, **attrs: Any) -> str:
        return self._add_node(name, NodeType.PROJECT, **attrs)

    def add_technology(self, name: str, **attrs: Any) -> str:
        return self._add_node(name, NodeType.TECHNOLOGY, **attrs)

    def add_subsystem(self, name: str, **attrs: Any) -> str:
        return self._add_node(name, NodeType.SUBSYSTEM, **attrs)

    def add_sector(self, name: str, **attrs: Any) -> str:
        return self._add_node(name, NodeType.SECTOR, **attrs)

    # -----------------------------------------------------------------
    # Edge builders
    # -----------------------------------------------------------------

    def _add_edge(self, src: str, tgt: str, edge_type: EdgeType, **attrs: Any) -> None:
        """Add a typed edge. Auto-creates skeleton nodes if missing."""
        for n in (src, tgt):
            if n not in self.g:
                self.g.add_node(n, node_type=NodeType.ENTITY.value,
                                created_at=datetime.now(timezone.utc).isoformat(),
                                _auto_created=True)
        attrs["edge_type"] = edge_type.value
        attrs.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        self.g.add_edge(src, tgt, **attrs)
        self._dirty = True

    def add_supply(self, supplier: str, customer: str, **attrs: Any) -> None:
        """supplier → customer (A supplies to B)."""
        self._add_edge(supplier, customer, EdgeType.SUPPLIES_TO, **attrs)

    def add_competition(self, a: str, b: str, **attrs: Any) -> None:
        """Bidirectional competition."""
        self._add_edge(a, b, EdgeType.COMPETES_WITH, **attrs)
        self._add_edge(b, a, EdgeType.COMPETES_WITH, **attrs)

    def add_manufacture(self, company: str, product: str, **attrs: Any) -> None:
        self._add_edge(company, product, EdgeType.MANUFACTURES, **attrs)

    def add_component(self, part: str, whole: str, **attrs: Any) -> None:
        """part is a component of whole."""
        self._add_edge(part, whole, EdgeType.COMPONENT_OF, **attrs)

    def add_enables(self, tech: str, product_or_project: str, **attrs: Any) -> None:
        self._add_edge(tech, product_or_project, EdgeType.ENABLES, **attrs)

    def add_belongs_to(self, company: str, sector: str, **attrs: Any) -> None:
        self._add_edge(company, sector, EdgeType.BELONGS_TO, **attrs)

    def add_investment(self, investor: str, company: str, **attrs: Any) -> None:
        self._add_edge(company, investor, EdgeType.INVESTED_BY, **attrs)

    def add_partnership(self, a: str, b: str, **attrs: Any) -> None:
        self._add_edge(a, b, EdgeType.PARTNERS_WITH, **attrs)
        self._add_edge(b, a, EdgeType.PARTNERS_WITH, **attrs)

    def add_triple(self, head: str, relation: str, tail: str, **attrs: Any) -> None:
        """Add a triple from LLM extraction (string relation → EdgeType)."""
        try:
            edge_type = EdgeType(relation)
        except ValueError:
            # Try matching by name
            for et in EdgeType:
                if et.name.lower() == relation.lower() or et.value == relation:
                    edge_type = et
                    break
            else:
                logger.warning("unknown edge type '%s', using ABOUT", relation)
                edge_type = EdgeType.ABOUT
        self._add_edge(head, tail, edge_type, **attrs)

    # -----------------------------------------------------------------
    # Query utilities
    # -----------------------------------------------------------------

    def upstream_of(self, node: str) -> list[dict[str, Any]]:
        """Find all suppliers / upstream nodes of a given node."""
        results = []
        for src, _, data in self.g.in_edges(node, data=True):
            if data.get("edge_type") == EdgeType.SUPPLIES_TO.value:
                results.append({"node": src, **dict(self.g.nodes[src]), "edge": data})
        return results

    def downstream_of(self, node: str) -> list[dict[str, Any]]:
        """Find all customers / downstream nodes of a given node."""
        results = []
        for _, tgt, data in self.g.out_edges(node, data=True):
            if data.get("edge_type") == EdgeType.SUPPLIES_TO.value:
                results.append({"node": tgt, **dict(self.g.nodes[tgt]), "edge": data})
        return results

    def competitors_of(self, node: str) -> list[dict[str, Any]]:
        """Find competitors."""
        results = []
        for _, tgt, data in self.g.out_edges(node, data=True):
            if data.get("edge_type") == EdgeType.COMPETES_WITH.value:
                results.append({"node": tgt, **dict(self.g.nodes[tgt])})
        return results

    def products_of(self, company: str) -> list[dict[str, Any]]:
        """Find all products a company manufactures."""
        results = []
        for _, tgt, data in self.g.out_edges(company, data=True):
            if data.get("edge_type") == EdgeType.MANUFACTURES.value:
                results.append({"node": tgt, **dict(self.g.nodes[tgt])})
        return results

    def companies_in_sector(self, sector: str) -> list[dict[str, Any]]:
        """Find all companies belonging to a sector."""
        results = []
        for src, _, data in self.g.in_edges(sector, data=True):
            if data.get("edge_type") == EdgeType.BELONGS_TO.value:
                results.append({"node": src, **dict(self.g.nodes[src])})
        return results

    def neighbors(self, node: str, max_depth: int = 1) -> nx.DiGraph:
        """Return subgraph around a node within max_depth hops."""
        if node not in self.g:
            return nx.DiGraph()
        nodes = {node}
        frontier = {node}
        for _ in range(max_depth):
            next_frontier = set()
            for n in frontier:
                next_frontier.update(self.g.predecessors(n))
                next_frontier.update(self.g.successors(n))
            nodes.update(next_frontier)
            frontier = next_frontier
        return self.g.subgraph(nodes).copy()

    def blind_spots(self) -> list[dict[str, Any]]:
        """Find nodes with low info density (auto-created or low degree).

        These are promising candidates for deeper research.
        """
        spots = []
        for n, attrs in self.g.nodes(data=True):
            degree = self.g.degree(n)
            is_auto = attrs.get("_auto_created", False)
            has_ticker = bool(attrs.get("ticker"))
            # Score: lower is more blind
            info_score = degree + (2 if has_ticker else 0) + (0 if is_auto else 1)
            if info_score <= 2:
                spots.append({
                    "node": n,
                    "node_type": attrs.get("node_type", "unknown"),
                    "degree": degree,
                    "auto_created": is_auto,
                    "has_ticker": has_ticker,
                    "info_score": info_score,
                })
        return sorted(spots, key=lambda x: x["info_score"])

    # -----------------------------------------------------------------
    # Stats
    # -----------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        """Return graph statistics."""
        type_counts: dict[str, int] = {}
        for _, attrs in self.g.nodes(data=True):
            t = attrs.get("node_type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1
        edge_counts: dict[str, int] = {}
        for _, _, attrs in self.g.edges(data=True):
            t = attrs.get("edge_type", "unknown")
            edge_counts[t] = edge_counts.get(t, 0) + 1
        return {
            "total_nodes": self.g.number_of_nodes(),
            "total_edges": self.g.number_of_edges(),
            "node_types": type_counts,
            "edge_types": edge_counts,
            "blind_spot_count": len(self.blind_spots()),
        }

    def to_mermaid(self, subgraph: nx.DiGraph | None = None) -> str:
        """Export graph (or subgraph) as Mermaid flowchart."""
        g = subgraph or self.g
        lines = ["graph LR"]
        # Node shapes by type
        shapes = {
            "company": ('["{label}"]', ":::company"),
            "product_line": ('("{label}")', ":::product"),
            "material": ('[/"{label}"/]', ":::material"),
            "project": ('[("{label}")]', ":::project"),
            "technology": ('{{"{label}"}}', ":::tech"),
            "sector": ('[["{label}"]]', ":::sector"),
        }
        node_ids: dict[str, str] = {}
        for i, (n, attrs) in enumerate(g.nodes(data=True)):
            safe_id = f"n{i}"
            node_ids[n] = safe_id
            ntype = attrs.get("node_type", "entity")
            shape, cls = shapes.get(ntype, (f'["{n}"]', ""))
            label = shape.replace("{label}", n.replace('"', "'"))
            lines.append(f"    {safe_id}{label}{cls}")

        # Edge labels
        edge_labels = {
            "supplies_to": "供应",
            "competes_with": "竞争",
            "manufactures": "制造",
            "component_of": "组件",
            "enables": "使能",
            "belongs_to": "属于",
            "invested_by": "投资",
            "partners_with": "合作",
            "customer_of": "客户",
        }
        for u, v, attrs in g.edges(data=True):
            uid = node_ids.get(u, u)
            vid = node_ids.get(v, v)
            etype = attrs.get("edge_type", "")
            label = edge_labels.get(etype, etype)
            lines.append(f"    {uid} -->|{label}| {vid}")

        return "\n".join(lines)
