"""Graph storage engine for the v2 knowledge graph.

Provides a unified ``GraphStore`` that combines:
- **NetworkX** for fast in-memory traversal and topology analysis
- **SQLite** for durable persistence (reuses ``finagent.db``)

Design goals:
  - Single-source-of-truth: SQLite is the canonical store; NetworkX
    is a hot cache rebuilt on ``load()``.
  - Incremental updates: ``add_node`` / ``add_edge`` / ``merge_edge``
    write‑through to both backends in a single call.
  - Temporal awareness: every edge row stores ``valid_from`` and
    ``valid_until`` so we can reconstruct snapshots at any date.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

import networkx as nx

from .ontology import EdgeSchema, EdgeType, NodeSchema, NodeType, NODE_SCHEMAS, resolve_edge_type

logger = logging.getLogger(__name__)

_UTCNOW = lambda: datetime.now(timezone.utc).isoformat()


# ── SQLite DDL ──────────────────────────────────────────────────────

_KG_DDL = """\
CREATE TABLE IF NOT EXISTS kg_nodes (
    node_id     TEXT PRIMARY KEY,
    node_type   TEXT NOT NULL,
    label       TEXT NOT NULL,
    attrs_json  TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kg_edges (
    edge_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id   TEXT NOT NULL,
    target_id   TEXT NOT NULL,
    edge_type   TEXT NOT NULL,
    valid_from  TEXT NOT NULL,
    valid_until TEXT,
    confidence  REAL NOT NULL DEFAULT 0.7,
    source      TEXT NOT NULL DEFAULT 'unknown',
    evidence    TEXT,
    evidence_url TEXT,
    attrs_json  TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES kg_nodes(node_id),
    FOREIGN KEY (target_id) REFERENCES kg_nodes(node_id)
);

CREATE INDEX IF NOT EXISTS idx_kg_edges_source ON kg_edges(source_id);
CREATE INDEX IF NOT EXISTS idx_kg_edges_target ON kg_edges(target_id);
CREATE INDEX IF NOT EXISTS idx_kg_edges_type   ON kg_edges(edge_type);
CREATE INDEX IF NOT EXISTS idx_kg_nodes_type   ON kg_nodes(node_type);

CREATE TABLE IF NOT EXISTS kg_entity_aliases (
    alias       TEXT PRIMARY KEY,
    canonical_id TEXT NOT NULL,
    alias_type  TEXT NOT NULL DEFAULT 'name',
    created_at  TEXT NOT NULL,
    FOREIGN KEY (canonical_id) REFERENCES kg_nodes(node_id)
);
"""


class GraphStore:
    """Unified graph store: NetworkX in-memory + SQLite persistent."""

    def __init__(self, db_path: str | Path | None = None):
        self._db_path = Path(db_path) if db_path else (
            Path(__file__).resolve().parent.parent.parent / "finagent.db"
        )
        self._conn: sqlite3.Connection | None = None
        self.g = nx.DiGraph()
        self._ensure_tables()
        self._load_from_db()

    # ── Connection management ───────────────────────────────────────

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _ensure_tables(self) -> None:
        self.conn.executescript(_KG_DDL)
        self.conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── Load from SQLite into NetworkX ──────────────────────────────

    def _load_from_db(self) -> None:
        """Rebuild the in-memory NetworkX graph from SQLite."""
        self.g.clear()

        for row in self.conn.execute("SELECT * FROM kg_nodes"):
            attrs = json.loads(row["attrs_json"])
            attrs["node_type"] = row["node_type"]
            attrs["label"] = row["label"]
            attrs["created_at"] = row["created_at"]
            attrs["updated_at"] = row["updated_at"]
            self.g.add_node(row["node_id"], **attrs)

        for row in self.conn.execute("SELECT * FROM kg_edges"):
            attrs = json.loads(row["attrs_json"])
            attrs["edge_id"] = row["edge_id"]
            attrs["edge_type"] = row["edge_type"]
            attrs["valid_from"] = row["valid_from"]
            attrs["valid_until"] = row["valid_until"]
            attrs["confidence"] = row["confidence"]
            attrs["source"] = row["source"]
            attrs["evidence"] = row["evidence"]
            attrs["evidence_url"] = row["evidence_url"]
            attrs["created_at"] = row["created_at"]
            attrs["updated_at"] = row["updated_at"]
            # NetworkX multigraph key = edge_id for multiple edges
            # between same pair
            self.g.add_edge(
                row["source_id"], row["target_id"],
                key=row["edge_id"], **attrs,
            )

        logger.info(
            "graph_v2 loaded: %d nodes, %d edges",
            self.g.number_of_nodes(), self.g.number_of_edges(),
        )

    def reload(self) -> None:
        """Force reload from SQLite."""
        # Switch to MultiDiGraph to support multiple edges between same nodes
        old_type = type(self.g)
        self.g = nx.MultiDiGraph()
        self._load_from_db()

    # ── Node operations ─────────────────────────────────────────────

    def add_node(
        self,
        node_id: str,
        node_type: NodeType | str,
        label: str,
        *,
        attrs: dict[str, Any] | None = None,
        _skip_validate: bool = False,
    ) -> str:
        """Add or update a node. Returns node_id."""
        if isinstance(node_type, str):
            try:
                node_type = NodeType(node_type)
            except ValueError:
                node_type = NodeType.ENTITY

        extra = dict(attrs or {})
        extra["label"] = label
        now = _UTCNOW()

        # Validate against schema
        if not _skip_validate:
            schema = NODE_SCHEMAS.get(node_type)
            if schema:
                errors = schema.validate(extra)
                if errors:
                    logger.warning("node %s validation: %s", node_id, errors)

        attrs_json = json.dumps(
            {k: v for k, v in extra.items() if k not in ("label",)},
            ensure_ascii=False,
        )

        self.conn.execute(
            """
            INSERT INTO kg_nodes (node_id, node_type, label, attrs_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
              node_type = excluded.node_type,
              label = excluded.label,
              attrs_json = excluded.attrs_json,
              updated_at = excluded.updated_at
            """,
            (node_id, node_type.value, label, attrs_json, now, now),
        )
        self.conn.commit()

        # Update in-memory graph
        all_attrs = dict(extra)
        all_attrs["node_type"] = node_type.value
        all_attrs["label"] = label
        all_attrs["created_at"] = now
        all_attrs["updated_at"] = now
        self.g.add_node(node_id, **all_attrs)

        return node_id

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Get node attributes, or None if not found."""
        if node_id in self.g:
            return dict(self.g.nodes[node_id])
        return None

    def has_node(self, node_id: str) -> bool:
        return node_id in self.g

    def remove_node(self, node_id: str) -> bool:
        """Remove a node and all its edges. Returns True if existed."""
        if node_id not in self.g:
            return False
        self.g.remove_node(node_id)
        self.conn.execute("DELETE FROM kg_edges WHERE source_id = ? OR target_id = ?", (node_id, node_id))
        self.conn.execute("DELETE FROM kg_nodes WHERE node_id = ?", (node_id,))
        self.conn.execute("DELETE FROM kg_entity_aliases WHERE canonical_id = ?", (node_id,))
        self.conn.commit()
        return True

    def nodes_by_type(self, node_type: NodeType | str) -> list[str]:
        """Return all node IDs of a given type."""
        nt = node_type.value if isinstance(node_type, NodeType) else node_type
        return [n for n, d in self.g.nodes(data=True) if d.get("node_type") == nt]

    def search_nodes(self, query: str, *, node_type: NodeType | str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        """Search nodes by label (substring match). Returns dicts with id + attrs."""
        q = query.lower()
        results = []
        for n, d in self.g.nodes(data=True):
            if node_type:
                nt = node_type.value if isinstance(node_type, NodeType) else node_type
                if d.get("node_type") != nt:
                    continue
            label = str(d.get("label", n)).lower()
            if q in label or q in n.lower():
                results.append({"node_id": n, **d})
                if len(results) >= limit:
                    break
        return results

    # ── Edge operations ─────────────────────────────────────────────

    def add_edge(
        self,
        source_id: str,
        target_id: str,
        edge_type: EdgeType | str,
        *,
        valid_from: str,
        confidence: float = 0.7,
        source: str = "unknown",
        evidence: str | None = None,
        evidence_url: str | None = None,
        valid_until: str | None = None,
        attrs: dict[str, Any] | None = None,
        auto_create_nodes: bool = True,
    ) -> int:
        """Add an edge. Auto-creates skeleton nodes if missing.

        Returns the edge_id.
        """
        if isinstance(edge_type, str):
            edge_type = resolve_edge_type(edge_type)

        now = _UTCNOW()

        # Auto-create missing nodes
        if auto_create_nodes:
            for nid in (source_id, target_id):
                if not self.has_node(nid):
                    self.add_node(
                        nid, NodeType.ENTITY, nid,
                        attrs={"_auto_created": True},
                        _skip_validate=True,
                    )

        extra = dict(attrs or {})
        attrs_json = json.dumps(extra, ensure_ascii=False)

        cursor = self.conn.execute(
            """
            INSERT INTO kg_edges
              (source_id, target_id, edge_type, valid_from, valid_until,
               confidence, source, evidence, evidence_url, attrs_json,
               created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (source_id, target_id, edge_type.value, valid_from, valid_until,
             confidence, source, evidence, evidence_url, attrs_json,
             now, now),
        )
        self.conn.commit()
        edge_id = cursor.lastrowid

        # Update in-memory graph
        edge_attrs = dict(extra)
        edge_attrs.update({
            "edge_id": edge_id,
            "edge_type": edge_type.value,
            "valid_from": valid_from,
            "valid_until": valid_until,
            "confidence": confidence,
            "source": source,
            "evidence": evidence,
            "evidence_url": evidence_url,
            "created_at": now,
            "updated_at": now,
        })

        if isinstance(self.g, nx.MultiDiGraph):
            self.g.add_edge(source_id, target_id, key=edge_id, **edge_attrs)
        else:
            self.g.add_edge(source_id, target_id, **edge_attrs)

        return edge_id

    def merge_edge(
        self,
        source_id: str,
        target_id: str,
        edge_type: EdgeType | str,
        *,
        valid_from: str,
        confidence: float = 0.7,
        source: str = "unknown",
        evidence: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Add an edge if no identical (source, target, type) exists,
        or update confidence/evidence if a matching edge already exists
        with higher confidence.

        This is the primary ingestion method for incremental updates —
        it implements LightRAG-style "merge, don't rebuild".
        """
        if isinstance(edge_type, str):
            edge_type = resolve_edge_type(edge_type)

        # Check for existing edge
        existing = self.conn.execute(
            """
            SELECT edge_id, confidence FROM kg_edges
            WHERE source_id = ? AND target_id = ? AND edge_type = ?
              AND (valid_until IS NULL OR valid_until > ?)
            ORDER BY confidence DESC LIMIT 1
            """,
            (source_id, target_id, edge_type.value, valid_from),
        ).fetchone()

        if existing:
            old_conf = existing["confidence"]
            if confidence > old_conf:
                # Update with higher confidence
                now = _UTCNOW()
                self.conn.execute(
                    """
                    UPDATE kg_edges SET confidence = ?, source = ?,
                      evidence = COALESCE(?, evidence), updated_at = ?
                    WHERE edge_id = ?
                    """,
                    (confidence, source, evidence, now, existing["edge_id"]),
                )
                self.conn.commit()
                logger.debug(
                    "merged edge %s→%s (%s): conf %.2f→%.2f",
                    source_id, target_id, edge_type.value, old_conf, confidence,
                )
            return existing["edge_id"]

        # No existing edge — add new
        return self.add_edge(
            source_id, target_id, edge_type,
            valid_from=valid_from,
            confidence=confidence,
            source=source,
            evidence=evidence,
            **kwargs,
        )

    def edges_between(
        self,
        source_id: str,
        target_id: str,
        *,
        edge_type: EdgeType | str | None = None,
    ) -> list[dict[str, Any]]:
        """Get all edges between two nodes, optionally filtered by type."""
        sql = "SELECT * FROM kg_edges WHERE source_id = ? AND target_id = ?"
        params: list[Any] = [source_id, target_id]
        if edge_type:
            et = edge_type.value if isinstance(edge_type, EdgeType) else edge_type
            sql += " AND edge_type = ?"
            params.append(et)
        return [dict(row) for row in self.conn.execute(sql, params)]

    def in_edges(self, node_id: str, *, edge_type: EdgeType | str | None = None) -> list[dict[str, Any]]:
        """Get all incoming edges to a node."""
        sql = "SELECT * FROM kg_edges WHERE target_id = ?"
        params: list[Any] = [node_id]
        if edge_type:
            et = edge_type.value if isinstance(edge_type, EdgeType) else edge_type
            sql += " AND edge_type = ?"
            params.append(et)
        return [dict(row) for row in self.conn.execute(sql, params)]

    def out_edges(self, node_id: str, *, edge_type: EdgeType | str | None = None) -> list[dict[str, Any]]:
        """Get all outgoing edges from a node."""
        sql = "SELECT * FROM kg_edges WHERE source_id = ?"
        params: list[Any] = [node_id]
        if edge_type:
            et = edge_type.value if isinstance(edge_type, EdgeType) else edge_type
            sql += " AND edge_type = ?"
            params.append(et)
        return [dict(row) for row in self.conn.execute(sql, params)]

    # ── Query utilities ─────────────────────────────────────────────

    def upstream_of(self, node_id: str) -> list[dict[str, Any]]:
        """Find all suppliers / upstream nodes."""
        supply_types = {
            EdgeType.SUPPLIES_CORE_PART_TO.value,
            EdgeType.COMPONENT_OF.value,
            EdgeType.LAUNCH_SERVICE_FOR.value,
        }
        results = []
        for edge in self.in_edges(node_id):
            if edge["edge_type"] in supply_types:
                node = self.get_node(edge["source_id"])
                if node:
                    results.append({
                        "node_id": edge["source_id"],
                        **node,
                        "edge": edge,
                    })
        return results

    def downstream_of(self, node_id: str) -> list[dict[str, Any]]:
        """Find all customers / downstream nodes."""
        supply_types = {
            EdgeType.SUPPLIES_CORE_PART_TO.value,
            EdgeType.CUSTOMER_OF.value,
            EdgeType.LAUNCH_SERVICE_FOR.value,
        }
        results = []
        for edge in self.out_edges(node_id):
            if edge["edge_type"] in supply_types:
                node = self.get_node(edge["target_id"])
                if node:
                    results.append({
                        "node_id": edge["target_id"],
                        **node,
                        "edge": edge,
                    })
        return results

    def competitors_of(self, node_id: str) -> list[dict[str, Any]]:
        """Find competitors."""
        results = []
        for edge in self.out_edges(node_id, edge_type=EdgeType.COMPETES_WITH):
            node = self.get_node(edge["target_id"])
            if node:
                results.append({"node_id": edge["target_id"], **node})
        return results

    def neighbors(self, node_id: str, *, max_depth: int = 1) -> nx.DiGraph:
        """Return subgraph around a node within max_depth hops."""
        if node_id not in self.g:
            return nx.DiGraph()
        nodes = {node_id}
        frontier = {node_id}
        for _ in range(max_depth):
            next_frontier: set[str] = set()
            for n in frontier:
                next_frontier.update(self.g.predecessors(n))
                next_frontier.update(self.g.successors(n))
            nodes.update(next_frontier)
            frontier = next_frontier
        return self.g.subgraph(nodes).copy()

    # ── Alias management ────────────────────────────────────────────

    def add_alias(self, alias: str, canonical_id: str, alias_type: str = "name") -> None:
        """Register an entity alias."""
        now = _UTCNOW()
        self.conn.execute(
            """
            INSERT INTO kg_entity_aliases (alias, canonical_id, alias_type, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(alias) DO UPDATE SET canonical_id = excluded.canonical_id
            """,
            (alias.lower().strip(), canonical_id, alias_type, now),
        )
        self.conn.commit()

    def resolve_alias(self, name: str) -> str | None:
        """Resolve an alias to its canonical node_id, or None."""
        row = self.conn.execute(
            "SELECT canonical_id FROM kg_entity_aliases WHERE alias = ?",
            (name.lower().strip(),),
        ).fetchone()
        return row["canonical_id"] if row else None

    # ── Statistics ──────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Return graph statistics."""
        node_types: dict[str, int] = {}
        for _, d in self.g.nodes(data=True):
            t = d.get("node_type", "unknown")
            node_types[t] = node_types.get(t, 0) + 1

        edge_types: dict[str, int] = {}
        total_edges = 0
        for row in self.conn.execute("SELECT edge_type, COUNT(*) as cnt FROM kg_edges GROUP BY edge_type"):
            edge_types[row["edge_type"]] = row["cnt"]
            total_edges += row["cnt"]

        auto_created = sum(
            1 for _, d in self.g.nodes(data=True) if d.get("_auto_created")
        )

        return {
            "total_nodes": self.g.number_of_nodes(),
            "total_edges": total_edges,
            "node_types": node_types,
            "edge_types": edge_types,
            "auto_created_nodes": auto_created,
        }

    # ── Export ───────────────────────────────────────────────────────

    def to_json(self) -> dict[str, Any]:
        """Export graph as JSON (for backup/migration)."""
        nodes = []
        for n, d in self.g.nodes(data=True):
            nodes.append({"node_id": n, **d})

        edges = []
        for row in self.conn.execute("SELECT * FROM kg_edges"):
            edges.append(dict(row))

        return {
            "meta": {
                "version": "2.0",
                "exported_at": _UTCNOW(),
                **self.stats(),
            },
            "nodes": nodes,
            "edges": edges,
        }

    def to_mermaid(self, subgraph: nx.DiGraph | None = None) -> str:
        """Export graph as Mermaid flowchart."""
        g = subgraph or self.g
        lines = ["graph LR"]

        node_ids: dict[str, str] = {}
        shapes = {
            "company": '["{}"]',
            "space_system": '("{}"]',
            "component": '[/"{}"/]',
            "project": '["{}")]',
            "technology": '{{"{}"}}',
            "sector": '[["{}"]]',
        }

        for i, (n, attrs) in enumerate(g.nodes(data=True)):
            safe_id = f"n{i}"
            node_ids[n] = safe_id
            label = str(attrs.get("label", n)).replace('"', "'")
            ntype = attrs.get("node_type", "entity")
            shape = shapes.get(ntype, '["{label}"]').format(label)
            lines.append(f"    {safe_id}{shape}")

        edge_labels = {
            "supplies_core_part_to": "供应",
            "competes_with": "竞争",
            "manufactures": "制造",
            "component_of": "组件",
            "enables": "使能",
            "belongs_to": "属于",
            "invested_by": "投资",
            "partners_with": "合作",
            "customer_of": "客户",
            "launch_service_for": "发射服务",
            "bid_won_contract": "中标",
            "operates": "运营",
            "controls": "控股",
            "technology_benchmark": "对标",
            "regulates": "约束",
            "related_to": "关联",
        }

        for u, v, data in g.edges(data=True):
            uid = node_ids.get(u, u)
            vid = node_ids.get(v, v)
            etype = data.get("edge_type", "")
            label = edge_labels.get(etype, etype)
            lines.append(f"    {uid} -->|{label}| {vid}")

        return "\n".join(lines)
