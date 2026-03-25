"""Migration tool: convert v1 industry_chain.json to v2 graph_v2 schema.

Maps v1 node types to v2 NodeType enum, adds temporal fields,
and moves data from JSON file to the SQLite-backed GraphStore.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from .ontology import EdgeType, NodeType, resolve_edge_type
from .store import GraphStore

logger = logging.getLogger(__name__)

# Default v1 data path
_V1_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "industry_chain.json"

# Map v1 node_type strings to v2 NodeType
_NODE_TYPE_MAP: dict[str, NodeType] = {
    "company": NodeType.COMPANY,
    "product_line": NodeType.SPACE_SYSTEM,
    "material": NodeType.COMPONENT,
    "project": NodeType.PROJECT,
    "technology": NodeType.TECHNOLOGY,
    "subsystem": NodeType.COMPONENT,
    "standard": NodeType.POLICY,
    "sector": NodeType.SECTOR,
    "entity": NodeType.ENTITY,
    "claim": NodeType.ENTITY,      # v1 claim graph — keep as generic entity
    "evidence": NodeType.ENTITY,
    "review": NodeType.ENTITY,
    "thesis": NodeType.ENTITY,
}

# Map v1 edge_type strings to v2 EdgeType
_EDGE_TYPE_MAP: dict[str, EdgeType] = {
    "supplies_to": EdgeType.SUPPLIES_CORE_PART_TO,
    "customer_of": EdgeType.CUSTOMER_OF,
    "competes_with": EdgeType.COMPETES_WITH,
    "manufactures": EdgeType.MANUFACTURES,
    "component_of": EdgeType.COMPONENT_OF,
    "enables": EdgeType.ENABLES,
    "regulates": EdgeType.REGULATES,
    "invested_by": EdgeType.INVESTED_BY,
    "partners_with": EdgeType.PARTNERS_WITH,
    "belongs_to": EdgeType.BELONGS_TO,
    "about": EdgeType.RELATED_TO,
    "supported_by": EdgeType.RELATED_TO,
    "reviewed_by": EdgeType.RELATED_TO,
    "depends_on": EdgeType.RELATED_TO,
    "conflicts_with": EdgeType.COMPETES_WITH,
}


def migrate_v1_to_v2(
    *,
    v1_path: str | Path | None = None,
    store: GraphStore | None = None,
    default_valid_from: str = "2024-01-01",
    default_source: str = "v1_migration",
) -> dict[str, Any]:
    """Migrate v1 industry_chain.json into v2 GraphStore.

    Args:
        v1_path: Path to v1 JSON file.
        store: Target v2 GraphStore (creates new if None).
        default_valid_from: Default temporal start for edges without dates.
        default_source: Source provenance tag for migrated data.

    Returns:
        Migration statistics dict.
    """
    path = Path(v1_path) if v1_path else _V1_PATH
    if not path.exists():
        logger.warning("v1 data not found at %s", path)
        return {"error": f"v1 file not found: {path}", "nodes_migrated": 0, "edges_migrated": 0}

    if store is None:
        store = GraphStore()

    data = json.loads(path.read_text(encoding="utf-8"))
    v1_nodes = data.get("nodes", [])
    v1_edges = data.get("edges", [])

    nodes_migrated = 0
    nodes_skipped = 0
    edges_migrated = 0
    edges_skipped = 0

    # Migrate nodes
    for node in v1_nodes:
        node_id = str(node.get("id", ""))
        if not node_id:
            nodes_skipped += 1
            continue

        v1_type = str(node.get("node_type", "entity"))
        v2_type = _NODE_TYPE_MAP.get(v1_type, NodeType.ENTITY)

        # Extract label (use id as fallback)
        label = node_id

        # Collect remaining attributes
        skip_keys = {"id", "node_type", "created_at", "updated_at", "_auto_created"}
        attrs = {k: v for k, v in node.items() if k not in skip_keys and v is not None}

        store.add_node(
            node_id, v2_type, label,
            attrs=attrs,
            _skip_validate=True,
        )

        # Register aliases
        if node.get("alias"):
            store.add_alias(str(node["alias"]).lower(), node_id, "alias")
        if node.get("ticker"):
            store.add_alias(str(node["ticker"]).lower(), node_id, "ticker")

        nodes_migrated += 1

    # Migrate edges
    for edge in v1_edges:
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        if not source or not target:
            edges_skipped += 1
            continue

        v1_etype = str(edge.get("edge_type", "related_to"))
        v2_etype = _EDGE_TYPE_MAP.get(v1_etype)
        if v2_etype is None:
            v2_etype = resolve_edge_type(v1_etype)

        # Try to extract temporal info from v1 edge
        valid_from = str(edge.get("created_at", default_valid_from))[:10]
        confidence = float(edge.get("confidence", 0.7))
        evidence = edge.get("evidence", "")
        source_tag = edge.get("source", default_source)
        if source_tag in (source, target):  # Avoid overwriting source/target
            source_tag = default_source

        store.merge_edge(
            source, target, v2_etype,
            valid_from=valid_from,
            confidence=confidence,
            source=source_tag if isinstance(source_tag, str) else default_source,
            evidence=evidence if isinstance(evidence, str) else None,
        )
        edges_migrated += 1

    result = {
        "v1_path": str(path),
        "nodes_migrated": nodes_migrated,
        "nodes_skipped": nodes_skipped,
        "edges_migrated": edges_migrated,
        "edges_skipped": edges_skipped,
        "v2_stats": store.stats(),
    }

    logger.info(
        "migration complete: %d nodes, %d edges migrated",
        nodes_migrated, edges_migrated,
    )
    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    result = migrate_v1_to_v2()
    print(json.dumps(result, ensure_ascii=False, indent=2))
