"""Three-class blind spot classifier for directed graph exploration.

Upgrades the v1 single-type blind spot detection to three distinct modes:

1. **Missing Entity** — A logically expected node doesn't exist.
   Example: "千帆星座 has launch services but no ground station operator node."
2. **Missing Relation** — Two known entities likely have a relationship
   that hasn't been documented.
   Example: "蓝箭航天 and 铖昌科技 are both in 商业航天 but have no edge."
3. **Missing Attribute** — A node exists but lacks critical quantitative
   fields (ticker, financials, capacity, etc.).
   Example: "星河动力 has no ticker, no founded date, no revenue data."

Each blind spot gets a priority score combining:
  - Structural importance (degree centrality)
  - Information density (attribute completeness)
  - Staleness (time since last update)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any

from .store import GraphStore
from .ontology import NodeType, EdgeType, NODE_SCHEMAS

logger = logging.getLogger(__name__)


class BlindSpotType(str, Enum):
    MISSING_ENTITY = "missing_entity"        # 逻辑链中主体不存在
    MISSING_RELATION = "missing_relation"    # 已知实体间联系未证实
    MISSING_ATTRIBUTE = "missing_attribute"  # 缺乏量化指标


class BlindSpot:
    """A single blind spot in the knowledge graph."""

    __slots__ = ("spot_type", "node_id", "description", "priority", "context")

    def __init__(
        self,
        spot_type: BlindSpotType,
        node_id: str,
        description: str,
        priority: float,
        context: dict[str, Any] | None = None,
    ):
        self.spot_type = spot_type
        self.node_id = node_id
        self.description = description
        self.priority = priority
        self.context = context or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.spot_type.value,
            "node_id": self.node_id,
            "description": self.description,
            "priority": round(self.priority, 3),
            "context": self.context,
        }

    def __repr__(self) -> str:
        return f"BlindSpot({self.spot_type.value}, {self.node_id!r}, pri={self.priority:.2f})"


class BlindSpotClassifier:
    """Detects and classifies blind spots in the knowledge graph."""

    def __init__(self, store: GraphStore):
        self.store = store

    def find_all(
        self,
        *,
        max_results: int = 50,
        min_priority: float = 0.0,
    ) -> list[BlindSpot]:
        """Find all blind spots across all three categories, sorted by priority."""
        spots: list[BlindSpot] = []

        spots.extend(self._find_missing_entities())
        spots.extend(self._find_missing_relations())
        spots.extend(self._find_missing_attributes())

        # Filter and sort
        spots = [s for s in spots if s.priority >= min_priority]
        spots.sort(key=lambda s: -s.priority)

        return spots[:max_results]

    def find_by_type(self, spot_type: BlindSpotType) -> list[BlindSpot]:
        """Find blind spots of a specific type."""
        if spot_type == BlindSpotType.MISSING_ENTITY:
            return sorted(self._find_missing_entities(), key=lambda s: -s.priority)
        elif spot_type == BlindSpotType.MISSING_RELATION:
            return sorted(self._find_missing_relations(), key=lambda s: -s.priority)
        elif spot_type == BlindSpotType.MISSING_ATTRIBUTE:
            return sorted(self._find_missing_attributes(), key=lambda s: -s.priority)
        return []

    # ── Missing Entity detection ────────────────────────────────────

    def _find_missing_entities(self) -> list[BlindSpot]:
        """Find nodes with low info density (auto-created, low degree).

        These are entities that were auto-created when an edge
        referenced them but haven't been properly researched.
        """
        spots = []
        for nid, attrs in self.store.g.nodes(data=True):
            is_auto = attrs.get("_auto_created", False)
            degree = self.store.g.degree(nid)
            has_ticker = bool(attrs.get("ticker"))
            node_type = attrs.get("node_type", "entity")

            # Calculate info density score
            info_score = degree + (2 if has_ticker else 0) + (0 if is_auto else 1)

            if info_score <= 2:
                # Priority: auto-created nodes with some edges are more
                # interesting than completely isolated ones
                priority = 1.0 if is_auto and degree >= 1 else 0.5
                priority += min(degree * 0.1, 0.5)  # More edges = more important

                spots.append(BlindSpot(
                    BlindSpotType.MISSING_ENTITY,
                    nid,
                    f"Low-info entity (auto={is_auto}, degree={degree}, type={node_type})",
                    priority,
                    context={
                        "auto_created": is_auto,
                        "degree": degree,
                        "node_type": node_type,
                        "info_score": info_score,
                    },
                ))

        return spots

    # ── Missing Relation detection ──────────────────────────────────

    def _find_missing_relations(self) -> list[BlindSpot]:
        """Find pairs of entities that likely should be related.

        Heuristics:
        1. Same sector but no direct edges
        2. Both connected to a common neighbor but not to each other
        3. Complementary node types (company ↔ component, no edge)
        """
        spots = []

        # Heuristic 1: Same sector, no direct edge
        sectors: dict[str, list[str]] = {}
        for nid, attrs in self.store.g.nodes(data=True):
            for edge in self.store.out_edges(nid, edge_type=EdgeType.BELONGS_TO):
                sector = edge["target_id"]
                sectors.setdefault(sector, []).append(nid)

        for sector, members in sectors.items():
            if len(members) < 2:
                continue
            for i, a in enumerate(members):
                for b in members[i+1:]:
                    # Check if they're already connected
                    has_edge = (
                        self.store.edges_between(a, b) or
                        self.store.edges_between(b, a)
                    )
                    if not has_edge:
                        a_node = self.store.get_node(a)
                        b_node = self.store.get_node(b)
                        a_label = a_node.get("label", a) if a_node else a
                        b_label = b_node.get("label", b) if b_node else b
                        spots.append(BlindSpot(
                            BlindSpotType.MISSING_RELATION,
                            a,  # Primary node
                            f"Possible missing relation: {a_label} ↔ {b_label} (both in {sector})",
                            0.6,
                            context={
                                "entity_a": a,
                                "entity_b": b,
                                "sector": sector,
                                "heuristic": "same_sector",
                            },
                        ))
                        if len(spots) > 100:  # Cap to prevent explosion
                            break
                if len(spots) > 100:
                    break

        # Heuristic 2: Common neighbors with no direct edge
        # (expensive — only for smaller graphs)
        if self.store.g.number_of_nodes() < 200:
            companies = self.store.nodes_by_type(NodeType.COMPANY)
            for i, a in enumerate(companies):
                a_neighbors = set(self.store.g.predecessors(a)) | set(self.store.g.successors(a))
                for b in companies[i+1:]:
                    b_neighbors = set(self.store.g.predecessors(b)) | set(self.store.g.successors(b))
                    common = a_neighbors & b_neighbors
                    if len(common) >= 2 and not (
                        self.store.edges_between(a, b) or self.store.edges_between(b, a)
                    ):
                        a_label = (self.store.get_node(a) or {}).get("label", a)
                        b_label = (self.store.get_node(b) or {}).get("label", b)
                        spots.append(BlindSpot(
                            BlindSpotType.MISSING_RELATION,
                            a,
                            f"Possible missing relation: {a_label} ↔ {b_label} ({len(common)} common neighbors)",
                            0.5 + min(len(common) * 0.1, 0.3),
                            context={
                                "entity_a": a,
                                "entity_b": b,
                                "common_neighbors": list(common)[:5],
                                "heuristic": "common_neighbors",
                            },
                        ))

        return spots

    # ── Missing Attribute detection ─────────────────────────────────

    def _find_missing_attributes(self) -> list[BlindSpot]:
        """Find nodes with incomplete attributes according to their type schema."""
        spots = []

        # Critical attributes by node type
        critical_attrs: dict[str, list[str]] = {
            "company": ["ticker", "sector"],
            "space_system": ["operator", "status"],
            "component": ["supplier", "criticality"],
            "project": ["operator", "status", "scale"],
        }

        for nid, attrs in self.store.g.nodes(data=True):
            node_type = attrs.get("node_type", "entity")
            required = critical_attrs.get(node_type, [])
            if not required:
                continue

            missing = [
                attr for attr in required
                if not attrs.get(attr)
            ]

            if missing:
                completeness = 1.0 - len(missing) / len(required)
                degree = self.store.g.degree(nid)
                label = attrs.get("label", nid)

                # Higher priority for well-connected but underdocumented nodes
                priority = (1.0 - completeness) * 0.5 + min(degree * 0.05, 0.3)

                spots.append(BlindSpot(
                    BlindSpotType.MISSING_ATTRIBUTE,
                    nid,
                    f"Missing attrs for {label} [{node_type}]: {', '.join(missing)}",
                    priority,
                    context={
                        "node_type": node_type,
                        "missing_attrs": missing,
                        "completeness": round(completeness, 2),
                        "degree": degree,
                    },
                ))

        return spots

    # ── Summary ─────────────────────────────────────────────────────

    def summary(self) -> dict[str, Any]:
        """Get a summary of all blind spots by type."""
        all_spots = self.find_all(max_results=200)
        by_type: dict[str, int] = {}
        for s in all_spots:
            by_type[s.spot_type.value] = by_type.get(s.spot_type.value, 0) + 1

        return {
            "total": len(all_spots),
            "by_type": by_type,
            "top_5": [s.to_dict() for s in all_spots[:5]],
        }
