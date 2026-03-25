from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

import networkx as nx

from .schema import EdgeType, NodeType, build_graph_from_rows, infer_entities


def build_graph_from_db(conn: Any, *, thesis_id: str = "") -> nx.DiGraph:
    thesis_filter = ""
    params: tuple[Any, ...] = ()
    if thesis_id:
        thesis_filter = "WHERE thesis_id = ?"
        params = (thesis_id,)

    theses = [dict(row) for row in conn.execute(f"SELECT * FROM theses {thesis_filter}", params).fetchall()]
    thesis_ids = [row["thesis_id"] for row in theses]
    thesis_versions = [
        dict(row)
        for row in conn.execute(
            f"SELECT * FROM thesis_versions WHERE thesis_id IN ({','.join('?' for _ in thesis_ids)})",
            tuple(thesis_ids),
        ).fetchall()
    ] if thesis_ids else []
    thesis_claim_map: dict[str, list[str]] = {}
    claim_ids_by_artifact: dict[str, list[str]] = defaultdict(list)
    claims: list[dict[str, Any]] = []
    artifact_ids: set[str] = set()
    for version in thesis_versions:
        created_from_artifacts = []
        if version.get("created_from_artifacts_json"):
            try:
                created_from_artifacts = json.loads(version["created_from_artifacts_json"])
            except json.JSONDecodeError:
                created_from_artifacts = []
        artifact_ids.update(str(item) for item in created_from_artifacts if item)
    if artifact_ids:
        placeholders = ",".join("?" for _ in artifact_ids)
        claims = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM claims WHERE artifact_id IN ({placeholders})",
                tuple(sorted(artifact_ids)),
            ).fetchall()
        ]
        for claim in claims:
            claim_ids_by_artifact[str(claim["artifact_id"])].append(str(claim["claim_id"]))
    for version in thesis_versions:
        thesis_claims: list[str] = []
        created_from_artifacts = []
        if version.get("created_from_artifacts_json"):
            try:
                created_from_artifacts = json.loads(version["created_from_artifacts_json"])
            except json.JSONDecodeError:
                created_from_artifacts = []
        for artifact_id in created_from_artifacts:
            thesis_claims.extend(claim_ids_by_artifact.get(str(artifact_id), []))
        thesis_claim_map[str(version["thesis_id"])] = sorted(set(thesis_claims))

    reviews = [dict(row) for row in conn.execute("SELECT * FROM reviews").fetchall()]
    return build_graph_from_rows(claims, theses, reviews, thesis_claim_map=thesis_claim_map)


def detect_conflicts(graph: nx.DiGraph) -> list[dict[str, Any]]:
    claims_by_entity: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for claim_id, attrs in graph.nodes(data=True):
        if attrs.get("node_type") != NodeType.CLAIM.value:
            continue
        entity_ids = [
            target
            for _, target, edge_attrs in graph.out_edges(claim_id, data=True)
            if edge_attrs.get("edge_type") == EdgeType.ABOUT.value and graph.nodes[target].get("node_type") == NodeType.ENTITY.value
        ]
        for entity_id in entity_ids:
            claims_by_entity[entity_id].append(
                {
                    "claim_id": claim_id,
                    "direction": attrs.get("direction", "neutral"),
                    "review_status": attrs.get("review_status", "unreviewed"),
                    "text": attrs.get("text", ""),
                }
            )

    conflicts: list[dict[str, Any]] = []
    for entity_id, claim_rows in claims_by_entity.items():
        positive = [row for row in claim_rows if row["direction"] == "positive"]
        negative = [row for row in claim_rows if row["direction"] == "negative"]
        for left in positive:
            for right in negative:
                resolved = left["review_status"] == "refuted" or right["review_status"] == "refuted"
                conflict_id = f"conflict::{entity_id}::{left['claim_id']}::{right['claim_id']}"
                graph.add_edge(left["claim_id"], right["claim_id"], edge_type=EdgeType.CONFLICTS_WITH.value, entity_id=entity_id)
                conflicts.append(
                    {
                        "conflict_id": conflict_id,
                        "entity_id": entity_id,
                        "left_claim_id": left["claim_id"],
                        "right_claim_id": right["claim_id"],
                        "resolved": resolved,
                        "left_text": left["text"],
                        "right_text": right["text"],
                    }
                )
    return conflicts


def find_broken_support_chains(graph: nx.DiGraph) -> list[dict[str, Any]]:
    broken: list[dict[str, Any]] = []
    for thesis_id, attrs in graph.nodes(data=True):
        if attrs.get("node_type") != NodeType.THESIS.value:
            continue
        thesis_text = attrs.get("title", "")
        thesis_entities = set(infer_entities(thesis_text))
        dependent_claims = [
            target
            for _, target, edge_attrs in graph.out_edges(thesis_id, data=True)
            if edge_attrs.get("edge_type") == EdgeType.DEPENDS_ON.value
        ]
        if not dependent_claims:
            continue
        claim_entities = set()
        for claim_id in dependent_claims:
            claim_entities.update(
                target
                for _, target, edge_attrs in graph.out_edges(claim_id, data=True)
                if edge_attrs.get("edge_type") == EdgeType.ABOUT.value
            )
        if thesis_entities and claim_entities and thesis_entities.isdisjoint(claim_entities):
            broken.append(
                {
                    "thesis_id": thesis_id,
                    "thesis_entities": sorted(thesis_entities),
                    "claim_entities": sorted(claim_entities),
                    "reason": "thesis entity set does not overlap with supporting claim entities",
                }
            )
    return broken
