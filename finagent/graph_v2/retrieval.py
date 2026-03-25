"""Dual-mode graph retrieval engine.

Implements the two retrieval strategies identified in the Deep Research:

1. **Local retrieval** — N-hop BFS around a target node with
   Adaptive-k pruning to prevent "gravity well" explosion (e.g. when
   traversing around a hub node like SpaceX with 100+ edges).

2. **Global retrieval** — Community-level summaries using Louvain
   partitioning.  Each community gets an LLM-generated summary that
   describes the subgraph's meaning in natural language.

3. **Hybrid** — Union of local + global results with confidence-weighted
   ranking and a fixed token budget to prevent context dilution.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Callable

import networkx as nx

from .store import GraphStore

logger = logging.getLogger(__name__)

# Maximum context chars to prevent context dilution (research finding)
DEFAULT_MAX_CONTEXT_CHARS = 6000


def _keyword_tokens(text: str) -> set[str]:
    """Tokenize ASCII and CJK text for lightweight overlap scoring."""
    normalized = text.lower().strip()
    if not normalized:
        return set()

    tokens = {
        token
        for token in re.split(r"\s+", normalized)
        if token
    }
    tokens.update(re.findall(r"[a-z0-9]+", normalized))

    cjk_only = "".join(ch for ch in normalized if "\u4e00" <= ch <= "\u9fff")
    if cjk_only:
        for idx in range(len(cjk_only) - 1):
            tokens.add(cjk_only[idx:idx + 2])

    tokens.add(normalized)
    return tokens


class GraphRetriever:
    """Retrieval engine for graph-augmented generation (GraphRAG)."""

    def __init__(
        self,
        store: GraphStore,
        *,
        max_context_chars: int = DEFAULT_MAX_CONTEXT_CHARS,
    ):
        self.store = store
        self.max_context_chars = max_context_chars
        self._community_summaries: dict[int, str] = {}

    # ── Local retrieval ─────────────────────────────────────────────

    def local_retrieve(
        self,
        node_id: str,
        *,
        max_depth: int = 2,
        adaptive_k: int = 15,
        min_confidence: float = 0.5,
    ) -> list[dict[str, Any]]:
        """BFS neighborhood retrieval with Adaptive-k pruning.

        The ``adaptive_k`` parameter caps the number of edges expanded
        per node to prevent "gravity well" explosion around hub nodes.
        Edges are ranked by confidence; only the top-k are traversed.
        """
        if not self.store.has_node(node_id):
            return []

        visited: set[str] = set()
        results: list[dict[str, Any]] = []
        frontier = [(node_id, 0)]

        while frontier:
            current, depth = frontier.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)

            # Get edges, sorted by confidence (adaptive-k)
            out_edges = self.store.out_edges(current)
            in_edges = self.store.in_edges(current)
            all_edges = out_edges + in_edges

            # Filter by confidence
            all_edges = [
                e for e in all_edges
                if (e.get("confidence") or 0) >= min_confidence
            ]

            # Sort by confidence descending, take top-k
            all_edges.sort(key=lambda e: e.get("confidence", 0), reverse=True)
            pruned = all_edges[:adaptive_k]

            for edge in pruned:
                # Determine the "other" node
                other = edge["target_id"] if edge["source_id"] == current else edge["source_id"]
                other_node = self.store.get_node(other)

                results.append({
                    "source": edge["source_id"],
                    "target": edge["target_id"],
                    "edge_type": edge["edge_type"],
                    "confidence": edge.get("confidence", 0),
                    "evidence": edge.get("evidence", ""),
                    "valid_from": edge.get("valid_from", ""),
                    "depth": depth,
                    "source_label": (self.store.get_node(edge["source_id"]) or {}).get("label", edge["source_id"]),
                    "target_label": (self.store.get_node(edge["target_id"]) or {}).get("label", edge["target_id"]),
                })

                if other not in visited and depth < max_depth:
                    frontier.append((other, depth + 1))

        # Sort by depth (closest first), then confidence
        results.sort(key=lambda r: (r["depth"], -r["confidence"]))
        return results

    # ── Global retrieval ────────────────────────────────────────────

    def detect_communities(self) -> dict[str, int]:
        """Run Louvain community detection on the undirected projection.

        Returns a mapping of node_id → community_id.
        """
        undirected = self.store.g.to_undirected()
        if undirected.number_of_nodes() == 0:
            return {}

        try:
            communities = nx.community.louvain_communities(undirected, seed=42)
        except Exception:
            # Fallback: each connected component is a community
            communities = list(nx.connected_components(undirected))

        mapping: dict[str, int] = {}
        for cid, members in enumerate(communities):
            for node in members:
                mapping[node] = cid

        logger.info("detected %d communities from %d nodes", len(communities), undirected.number_of_nodes())
        return mapping

    def community_context(
        self,
        community_id: int,
        community_map: dict[str, int],
    ) -> str:
        """Generate a text description of a community's subgraph.

        This is a simple rule-based approach. For production, replace
        with LLM-generated summaries.
        """
        members = [n for n, c in community_map.items() if c == community_id]
        if not members:
            return ""

        lines = [f"Community {community_id} ({len(members)} entities):"]

        # List members with their types
        for nid in members[:20]:  # Cap at 20 for readability
            node = self.store.get_node(nid)
            if node:
                ntype = node.get("node_type", "unknown")
                label = node.get("label", nid)
                lines.append(f"  - [{ntype}] {label}")

        # List intra-community edges
        subgraph = self.store.g.subgraph(members)
        edge_count = 0
        for u, v, d in subgraph.edges(data=True):
            if edge_count >= 15:  # Cap edge listing
                lines.append(f"  ... and {subgraph.number_of_edges() - 15} more edges")
                break
            etype = d.get("edge_type", "related_to")
            u_label = self.store.g.nodes[u].get("label", u)
            v_label = self.store.g.nodes[v].get("label", v)
            lines.append(f"  {u_label} --[{etype}]--> {v_label}")
            edge_count += 1

        return "\n".join(lines)

    def global_retrieve(
        self,
        query: str,
        *,
        community_map: dict[str, int] | None = None,
        top_k: int = 3,
    ) -> list[dict[str, Any]]:
        """Retrieve relevant communities based on query keywords.

        Uses keyword overlap scoring (lightweight; upgrade to embeddings
        for production).
        """
        if community_map is None:
            community_map = self.detect_communities()

        if not community_map:
            return []

        query_tokens = _keyword_tokens(query)
        community_ids = set(community_map.values())

        scored: list[tuple[int, float, str]] = []
        for cid in community_ids:
            members = [n for n, c in community_map.items() if c == cid]
            # Score = keyword overlap with member labels
            member_text = " ".join(
                str((self.store.get_node(n) or {}).get("label", n)).lower()
                for n in members
            )
            member_tokens = _keyword_tokens(member_text)
            overlap = len(query_tokens & member_tokens)
            if overlap > 0:
                context = self.community_context(cid, community_map)
                scored.append((cid, overlap, context))

        scored.sort(key=lambda x: -x[1])
        return [
            {"community_id": cid, "score": score, "context": ctx}
            for cid, score, ctx in scored[:top_k]
        ]

    # ── Hybrid retrieval ────────────────────────────────────────────

    def retrieve(
        self,
        query: str,
        *,
        focus_node: str | None = None,
        mode: str = "hybrid",  # local | global | hybrid
        max_depth: int = 2,
        adaptive_k: int = 15,
    ) -> str:
        """Main retrieval method — produces context text for LLM prompting.

        Enforces a fixed token budget (``max_context_chars``) to prevent
        context dilution.

        Args:
            query: The research question or search intent.
            focus_node: Optional node to center local retrieval around.
            mode: "local", "global", or "hybrid".
            max_depth: BFS depth for local retrieval.
            adaptive_k: Max edges per node for local retrieval.

        Returns:
            A formatted context string ready for LLM consumption.
        """
        parts: list[str] = []
        budget = self.max_context_chars

        if mode in ("local", "hybrid") and focus_node:
            local_results = self.local_retrieve(
                focus_node, max_depth=max_depth, adaptive_k=adaptive_k,
            )
            if local_results:
                local_text = self._format_local_results(local_results, focus_node)
                if len(local_text) > budget // 2 and mode == "hybrid":
                    local_text = local_text[:budget // 2] + "\n[...truncated...]"
                parts.append(local_text)
                budget -= len(local_text)

        if mode in ("global", "hybrid") and budget > 200:
            global_results = self.global_retrieve(query)
            if global_results:
                global_text = "\n\n".join(r["context"] for r in global_results)
                if len(global_text) > budget:
                    global_text = global_text[:budget] + "\n[...truncated...]"
                parts.append(global_text)

        result = "\n\n---\n\n".join(parts) if parts else "(no relevant graph context found)"

        # Final budget enforcement
        if len(result) > self.max_context_chars:
            result = result[:self.max_context_chars] + "\n[...context truncated to budget...]"

        return result

    def _format_local_results(
        self,
        results: list[dict[str, Any]],
        focus: str,
    ) -> str:
        """Format local retrieval results into readable context."""
        focus_node = self.store.get_node(focus)
        focus_label = focus_node.get("label", focus) if focus_node else focus

        lines = [f"## Graph context for: {focus_label}"]

        for r in results:
            src = r.get("source_label", r["source"])
            tgt = r.get("target_label", r["target"])
            etype = r["edge_type"]
            conf = r.get("confidence", "?")
            ev = r.get("evidence", "")
            depth = r.get("depth", 0)

            indent = "  " * depth
            line = f"{indent}- {src} --[{etype}]--> {tgt} (conf={conf})"
            if ev:
                line += f"  证据: {ev}"
            lines.append(line)

        return "\n".join(lines)
