"""LangGraph state definition for the research workflow.

The state flows through: Planner → Searcher → Extractor → Evaluator
and accumulates evidence, graph updates, and confidence scores.
"""

from __future__ import annotations

from typing import Any, TypedDict, Annotated

try:
    from langgraph.graph.message import add_messages
except ModuleNotFoundError:
    def add_messages(existing: list, new: list) -> list:
        return list(existing or []) + list(new or [])


# ── Custom reducers for competitive data (dedup by ID) ───────────────

def _merge_by_asset_id(
    existing: list[dict[str, Any]],
    new: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge image assets, dedup by asset_id (latest wins)."""
    merged: dict[str, dict[str, Any]] = {}
    for item in (existing or []):
        aid = item.get("asset_id", "")
        if aid:
            merged[aid] = item
    for item in (new or []):
        aid = item.get("asset_id", "")
        if aid:
            merged[aid] = item
    return list(merged.values())


def _merge_by_sku_id(
    existing: list[dict[str, Any]],
    new: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge SKU records, dedup by sku_id (latest wins)."""
    merged: dict[str, dict[str, Any]] = {}
    for item in (existing or []):
        sid = item.get("sku_id", "")
        if sid:
            merged[sid] = item
    for item in (new or []):
        sid = item.get("sku_id", "")
        if sid:
            merged[sid] = item
    return list(merged.values())


class ResearchState(TypedDict):
    """Central state passed between all agent nodes.

    Fields:
        messages: LangGraph message bus (with add_messages reducer).
        research_goal: What we're trying to learn (anchors all agents).
        context: Domain context (e.g. "商业航天", "电力设备").
        iteration_step: Current loop iteration (monotonically increasing).
        max_iterations: Hard cap on iterations (safety).
        token_budget_remaining: Remaining token budget (decremented each round).
        knowledge_graph_summary: Compact text summary of current graph state.
        pending_queries: Queue of micro-queries to execute.
        completed_queries: Queries already executed (for dedup).
        gathered_evidence: Evidence chunks collected by Searcher.
        new_triples: Triples extracted by Extractor in this round.
        total_triples_added: Running total of triples added to graph.
        blind_spots_addressed: Nodes that have been explored.
        confidence_score: Evaluator's assessment of research completeness.
        should_continue: Whether to continue iterating.
        termination_reason: Why research stopped (null = still running).
        errors: Any errors encountered during processing.
        semantic_promotions: Semantic memory records created in the latest consolidation pass.
        memory_counts: Current memory tier counts snapshot.
        image_assets: Product image assets extracted from evidence (dedup by asset_id).
        sku_records: Product SKU records extracted from evidence (dedup by sku_id).
    """

    messages: Annotated[list, add_messages]
    run_id: str
    research_goal: str
    context: str
    iteration_step: int
    max_iterations: int
    token_budget_remaining: int
    knowledge_graph_summary: str
    pending_queries: list[str]
    completed_queries: list[str]
    gathered_evidence: list[dict[str, Any]]
    new_triples: list[dict[str, Any]]
    total_triples_added: int
    blind_spots_addressed: list[str]
    confidence_score: float
    should_continue: bool
    termination_reason: str | None
    errors: list[str]
    semantic_promotions: list[str]
    memory_counts: dict[str, int]
    image_assets: Annotated[list[dict[str, Any]], _merge_by_asset_id]
    sku_records: Annotated[list[dict[str, Any]], _merge_by_sku_id]


def initial_state(
    research_goal: str,
    *,
    context: str = "商业航天",
    max_iterations: int = 10,
    token_budget: int = 50_000,
) -> ResearchState:
    """Create the initial state for a research session."""
    return ResearchState(
        messages=[],
        run_id="",
        research_goal=research_goal,
        context=context,
        iteration_step=0,
        max_iterations=max_iterations,
        token_budget_remaining=token_budget,
        knowledge_graph_summary="",
        pending_queries=[],
        completed_queries=[],
        gathered_evidence=[],
        new_triples=[],
        total_triples_added=0,
        blind_spots_addressed=[],
        confidence_score=0.0,
        should_continue=True,
        termination_reason=None,
        errors=[],
        semantic_promotions=[],
        memory_counts={},
        image_assets=[],
        sku_records=[],
    )
