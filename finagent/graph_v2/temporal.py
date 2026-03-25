"""Temporal knowledge graph queries.

Implements time-aware graph operations so that the KG can answer
questions like:
  - "Who supplied 蓝箭航天 in 2024 Q3?"
  - "When did 铖昌科技 start supplying 垣信卫星?"
  - "Show me the supply chain evolution of 千帆星座"

Core concept from FinDKG research: every edge has ``valid_from`` and
an optional ``valid_until``.  NULL ``valid_until`` means "still active".
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import networkx as nx

from .store import GraphStore

logger = logging.getLogger(__name__)


def _parse_date(raw: str | None) -> date | None:
    """Parse date from various formats."""
    if not raw or raw == "unknown":
        return None
    try:
        return date.fromisoformat(raw[:10])
    except (ValueError, TypeError):
        return None


def _date_in_range(
    check_date: date,
    valid_from: str | None,
    valid_until: str | None,
) -> bool:
    """Check if a date falls within [valid_from, valid_until]."""
    vf = _parse_date(valid_from)
    vu = _parse_date(valid_until)

    if vf and check_date < vf:
        return False
    if vu and check_date > vu:
        return False
    return True


class TemporalQuery:
    """Time-aware query engine for the knowledge graph."""

    def __init__(self, store: GraphStore):
        self.store = store

    def snapshot_at(self, snapshot_date: date | str) -> nx.DiGraph:
        """Return a graph snapshot valid at a specific date.

        Only includes edges whose [valid_from, valid_until] range
        covers the snapshot_date.  Nodes with no valid edges at
        that date are excluded.
        """
        if isinstance(snapshot_date, str):
            snapshot_date = date.fromisoformat(snapshot_date[:10])

        g = nx.DiGraph()
        active_nodes: set[str] = set()

        # Filter edges by date
        for row in self.store.conn.execute("SELECT * FROM kg_edges"):
            if _date_in_range(snapshot_date, row["valid_from"], row["valid_until"]):
                src, tgt = row["source_id"], row["target_id"]
                g.add_edge(src, tgt, **dict(row))
                active_nodes.add(src)
                active_nodes.add(tgt)

        # Copy node attributes
        for node_id in active_nodes:
            node = self.store.get_node(node_id)
            if node:
                g.add_node(node_id, **node)

        logger.info(
            "snapshot at %s: %d nodes, %d edges",
            snapshot_date, g.number_of_nodes(), g.number_of_edges(),
        )
        return g

    def edge_history(
        self,
        source_id: str,
        target_id: str,
        *,
        edge_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get the complete history of edges between two nodes.

        Returns edges sorted by valid_from ascending.
        """
        sql = """
            SELECT * FROM kg_edges
            WHERE source_id = ? AND target_id = ?
        """
        params: list[Any] = [source_id, target_id]

        if edge_type:
            sql += " AND edge_type = ?"
            params.append(edge_type)

        sql += " ORDER BY valid_from ASC"

        return [dict(row) for row in self.store.conn.execute(sql, params)]

    def node_timeline(self, node_id: str) -> list[dict[str, Any]]:
        """Get all edges involving a node, sorted by time.

        Useful for reconstructing the "story" of an entity:
        when relationships formed, changed, or ended.
        """
        sql = """
            SELECT *, 'out' as direction FROM kg_edges WHERE source_id = ?
            UNION ALL
            SELECT *, 'in' as direction FROM kg_edges WHERE target_id = ?
            ORDER BY valid_from ASC
        """
        return [dict(row) for row in self.store.conn.execute(sql, (node_id, node_id))]

    def active_edges(
        self,
        node_id: str,
        *,
        as_of: date | str | None = None,
    ) -> list[dict[str, Any]]:
        """Get all currently active edges for a node.

        An edge is "active" if valid_until IS NULL or valid_until > as_of.
        """
        if as_of is None:
            as_of = date.today()
        elif isinstance(as_of, str):
            as_of = date.fromisoformat(as_of[:10])

        as_of_str = as_of.isoformat()

        sql = """
            SELECT * FROM kg_edges
            WHERE (source_id = ? OR target_id = ?)
              AND valid_from <= ?
              AND (valid_until IS NULL OR valid_until > ?)
            ORDER BY confidence DESC
        """
        return [
            dict(row)
            for row in self.store.conn.execute(
                sql, (node_id, node_id, as_of_str, as_of_str)
            )
        ]

    def relationship_changes(
        self,
        *,
        since: date | str,
        until: date | str | None = None,
        edge_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Find edges that started or ended within a time window.

        Useful for "what changed since last review?" reports.
        """
        if isinstance(since, str):
            since = date.fromisoformat(since[:10])
        if until is None:
            until = date.today()
        elif isinstance(until, str):
            until = date.fromisoformat(until[:10])

        since_str = since.isoformat()
        until_str = until.isoformat()

        sql = """
            SELECT *, 'started' as change_type FROM kg_edges
            WHERE valid_from BETWEEN ? AND ?
        """
        params: list[Any] = [since_str, until_str]

        if edge_type:
            sql += " AND edge_type = ?"
            params.append(edge_type)

        sql += """
            UNION ALL
            SELECT *, 'ended' as change_type FROM kg_edges
            WHERE valid_until BETWEEN ? AND ?
        """
        params.extend([since_str, until_str])

        if edge_type:
            sql += " AND edge_type = ?"
            params.append(edge_type)

        sql += " ORDER BY valid_from ASC"

        return [dict(row) for row in self.store.conn.execute(sql, params)]

    def supply_chain_at(
        self,
        node_id: str,
        *,
        as_of: date | str | None = None,
        direction: str = "upstream",
        max_depth: int = 3,
    ) -> list[dict[str, Any]]:
        """Trace the supply chain at a specific point in time.

        ``direction`` can be "upstream" (suppliers) or "downstream" (customers).
        """
        if as_of is None:
            as_of = date.today()
        elif isinstance(as_of, str):
            as_of = date.fromisoformat(as_of[:10])

        supply_types = {"supplies_core_part_to", "component_of", "launch_service_for"}
        visited: set[str] = set()
        results: list[dict[str, Any]] = []
        frontier = {node_id}

        for depth in range(max_depth):
            next_frontier: set[str] = set()
            for current in frontier:
                if current in visited:
                    continue
                visited.add(current)

                if direction == "upstream":
                    edges = self.active_edges(current, as_of=as_of)
                    for e in edges:
                        if e["target_id"] == current and e["edge_type"] in supply_types:
                            results.append({
                                "depth": depth + 1,
                                "supplier": e["source_id"],
                                "customer": current,
                                **e,
                            })
                            next_frontier.add(e["source_id"])
                else:
                    edges = self.active_edges(current, as_of=as_of)
                    for e in edges:
                        if e["source_id"] == current and e["edge_type"] in supply_types:
                            results.append({
                                "depth": depth + 1,
                                "supplier": current,
                                "customer": e["target_id"],
                                **e,
                            })
                            next_frontier.add(e["target_id"])

            frontier = next_frontier - visited
            if not frontier:
                break

        return results

    # ── Fix #4: Confidence decay ────────────────────────────────────

    def confidence_decay(
        self,
        edge: dict[str, Any],
        *,
        as_of: date | str | None = None,
        half_life_days: int = 365,
    ) -> float:
        """Apply time-based confidence decay to an edge.

        Edges lose confidence weight based on their age.  A 2023-era
        "supply relationship" assessed in 2026 should automatically
        have lower weight in topology analysis, forcing the system to
        trigger blind spot re-exploration.

        Uses exponential decay: weight = confidence * 2^(-age/half_life)

        Args:
            edge: Edge dict with 'valid_from' and 'confidence'.
            as_of: Reference date for decay calculation.
            half_life_days: Days until confidence halves (default 1 year).

        Returns:
            Decayed confidence value (0.0 - 1.0).
        """
        import math

        if as_of is None:
            as_of = date.today()
        elif isinstance(as_of, str):
            as_of = date.fromisoformat(as_of[:10])

        original_conf = float(edge.get("confidence", 0.7))
        valid_from = _parse_date(str(edge.get("valid_from", "")))

        if not valid_from:
            return original_conf * 0.5  # Unknown date = heavy penalty

        age_days = (as_of - valid_from).days
        if age_days <= 0:
            return original_conf

        decay_factor = math.pow(2.0, -age_days / half_life_days)
        return max(0.05, original_conf * decay_factor)

    def edges_with_decay(
        self,
        node_id: str,
        *,
        as_of: date | str | None = None,
        half_life_days: int = 365,
        min_decayed_confidence: float = 0.3,
    ) -> list[dict[str, Any]]:
        """Get active edges with confidence decay applied.

        Edges below min_decayed_confidence are flagged as "stale"
        and should trigger blind spot re-exploration.
        """
        if as_of is None:
            as_of = date.today()

        active = self.active_edges(node_id, as_of=as_of)
        enriched = []
        for edge in active:
            decayed = self.confidence_decay(
                edge, as_of=as_of, half_life_days=half_life_days,
            )
            edge_copy = dict(edge)
            edge_copy["decayed_confidence"] = round(decayed, 4)
            edge_copy["is_stale"] = decayed < min_decayed_confidence
            enriched.append(edge_copy)

        return enriched

    def stale_edges(
        self,
        *,
        as_of: date | str | None = None,
        half_life_days: int = 365,
        threshold: float = 0.3,
    ) -> list[dict[str, Any]]:
        """Find all edges whose decayed confidence is below threshold.

        These are candidates for re-research via blind spot activation.
        """
        if as_of is None:
            as_of = date.today()
        elif isinstance(as_of, str):
            as_of = date.fromisoformat(as_of[:10])

        stale = []
        for row in self.store.conn.execute("SELECT * FROM kg_edges"):
            edge = dict(row)
            decayed = self.confidence_decay(
                edge, as_of=as_of, half_life_days=half_life_days,
            )
            if decayed < threshold:
                edge["decayed_confidence"] = round(decayed, 4)
                stale.append(edge)

        stale.sort(key=lambda e: e["decayed_confidence"])
        return stale

