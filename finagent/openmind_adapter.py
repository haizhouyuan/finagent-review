"""OpenMind integration hooks for finagent.

Exports finagent's thesis/claim/entity graph into OpenMind-compatible
node and edge format for cognitive substrate ingestion.

Usage:
    from finagent.openmind_adapter import export_to_openmind
    nodes, edges = export_to_openmind(conn)
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any


def _row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


def export_theses(conn: sqlite3.Connection) -> list[dict]:
    """Export theses as OpenMind nodes."""
    rows = conn.execute(
        "SELECT thesis_id, title, status, created_at FROM theses"
    ).fetchall()
    return [
        {
            "node_type": "thesis",
            "node_id": f"finagent:thesis:{r['thesis_id']}",
            "label": r["title"],
            "properties": {
                "status": r["status"],
                "source_system": "finagent",
                "created_at": r["created_at"],
            },
        }
        for r in rows
    ]


def export_entities(conn: sqlite3.Connection) -> list[dict]:
    """Export entities as OpenMind nodes."""
    rows = conn.execute(
        "SELECT entity_id, canonical_name, entity_type, jurisdiction FROM entities"
    ).fetchall()
    return [
        {
            "node_type": "entity",
            "node_id": f"finagent:entity:{r['entity_id']}",
            "label": r["canonical_name"],
            "properties": {
                "entity_type": r["entity_type"],
                "jurisdiction": r["jurisdiction"],
                "source_system": "finagent",
            },
        }
        for r in rows
    ]


def export_claims(conn: sqlite3.Connection, limit: int = 500) -> list[dict]:
    """Export claims as OpenMind nodes (capped for performance)."""
    rows = conn.execute(
        "SELECT claim_id, claim_text, claim_type, confidence, data_date, freshness_status "
        "FROM claims ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "node_type": "claim",
            "node_id": f"finagent:claim:{r['claim_id']}",
            "label": r["claim_text"][:120],
            "properties": {
                "claim_type": r["claim_type"],
                "confidence": r["confidence"],
                "data_date": r["data_date"],
                "freshness_status": r["freshness_status"],
                "source_system": "finagent",
            },
        }
        for r in rows
    ]


def export_edges(conn: sqlite3.Connection) -> list[dict]:
    """Export relationships (claim_routes, target_cases) as OpenMind edges."""
    edges = []

    # Claim → Thesis routes
    routes = conn.execute(
        "SELECT route_id, claim_id, target_object_type, target_object_id, route_type, status "
        "FROM claim_routes WHERE target_object_type = 'thesis'"
    ).fetchall()
    for r in routes:
        edges.append({
            "edge_type": r["route_type"],
            "edge_id": f"finagent:route:{r['route_id']}",
            "source_id": f"finagent:claim:{r['claim_id']}",
            "target_id": f"finagent:thesis:{r['target_object_id']}",
            "properties": {"status": r["status"], "source_system": "finagent"},
        })

    # Target-case → Entity edges
    tc_rows = conn.execute(
        "SELECT tc.target_case_id, tc.target_id, t.entity_id, tc.exposure_type "
        "FROM target_cases tc JOIN targets t ON tc.target_id = t.target_id"
    ).fetchall()
    for r in tc_rows:
        edges.append({
            "edge_type": "target_exposure",
            "edge_id": f"finagent:tc_edge:{r['target_case_id']}",
            "source_id": f"finagent:entity:{r['entity_id']}",
            "target_id": f"finagent:thesis:{r['target_case_id']}",
            "properties": {"exposure_type": r["exposure_type"], "source_system": "finagent"},
        })

    return edges


def export_to_openmind(
    conn: sqlite3.Connection,
    *,
    claim_limit: int = 500,
) -> dict[str, Any]:
    """Full export for OpenMind ingestion.

    Returns a dict with 'nodes' and 'edges' lists, ready for
    OpenMind's graph import API or JSONL file export.
    """
    nodes = []
    nodes.extend(export_theses(conn))
    nodes.extend(export_entities(conn))
    nodes.extend(export_claims(conn, limit=claim_limit))

    edges = export_edges(conn)

    return {
        "format": "openmind_v1",
        "source_system": "finagent",
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


def export_to_jsonl(conn: sqlite3.Connection, output_path: str, *, claim_limit: int = 500) -> int:
    """Export to JSONL file for OpenMind batch import."""
    data = export_to_openmind(conn, claim_limit=claim_limit)
    count = 0
    with open(output_path, "w") as f:
        for node in data["nodes"]:
            f.write(json.dumps({"type": "node", **node}, ensure_ascii=False) + "\n")
            count += 1
        for edge in data["edges"]:
            f.write(json.dumps({"type": "edge", **edge}, ensure_ascii=False) + "\n")
            count += 1
    return count
