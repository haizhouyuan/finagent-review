"""Graph hygiene metrics for the two-wheeler pilot."""

from __future__ import annotations

import argparse
from typing import Any

from finagent.graph_v2.store import GraphStore


def report(db_path: str = "finagent.db") -> dict[str, Any]:
    store = GraphStore(db_path)
    try:
        stats = store.stats()
        orphan_nodes = [node for node in store.g.nodes() if store.g.degree(node) == 0]
        alias_count = store.conn.execute(
            "SELECT COUNT(*) FROM kg_entity_aliases"
        ).fetchone()[0]
        return {
            "nodes": stats["total_nodes"],
            "edges": stats["total_edges"],
            "orphans": orphan_nodes,
            "alias_count": int(alias_count),
            "node_types": stats["node_types"],
            "edge_types": stats["edge_types"],
        }
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="finagent.db")
    args = parser.parse_args()
    metrics = report(args.db_path)
    print(f"Nodes: {metrics['nodes']}")
    print(f"Edges: {metrics['edges']}")
    print(f"Orphans: {len(metrics['orphans'])}")
    print(f"Aliases: {metrics['alias_count']}")
    print(f"Node types: {metrics['node_types']}")
    print(f"Edge types: {metrics['edge_types']}")


if __name__ == "__main__":
    main()
