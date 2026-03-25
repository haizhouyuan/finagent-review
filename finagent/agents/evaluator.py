"""Evaluator agent — assesses research completeness and decides termination.

Three-dimensional termination logic:
  1. Iteration limit reached → HALT
  2. Token budget exhausted → HALT
  3. Confidence ≥ threshold + consecutive no-progress → NORMAL STOP
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from .state import ResearchState
from .safety import SafetyGuard, SafetyVerdict

logger = logging.getLogger(__name__)


def evaluator_node(
    state: ResearchState,
    *,
    safety_guard: SafetyGuard | None = None,
    graph_store: Any | None = None,
    confidence_threshold: float = 0.85,
) -> dict[str, Any]:
    """Evaluator agent node for LangGraph.

    Decides whether research is sufficient or should continue.
    """
    if safety_guard is None:
        safety_guard = SafetyGuard()

    step = state.get("iteration_step", 0)
    total_added = state.get("total_triples_added", 0)
    new_triples = state.get("new_triples", [])
    confidence = state.get("confidence_score", 0.0)
    goal = state.get("research_goal", "")

    # Run safety check
    verdict = safety_guard.check(state)

    if verdict == SafetyVerdict.HALT:
        reason = _determine_halt_reason(state, safety_guard)
        logger.info(
            "evaluator: HALT at step %d — %s (conf=%.2f, triples=%d)",
            step, reason, confidence, total_added,
        )
        return {
            "should_continue": False,
            "termination_reason": reason,
        }

    # Calculate updated confidence
    new_confidence = _compute_confidence(state, graph_store)

    # Normal termination: high confidence + no new triples
    if new_confidence >= confidence_threshold and len(new_triples) == 0:
        logger.info(
            "evaluator: research complete at step %d (conf=%.2f ≥ %.2f, no new triples)",
            step, new_confidence, confidence_threshold,
        )
        return {
            "confidence_score": new_confidence,
            "should_continue": False,
            "termination_reason": f"research complete (confidence={new_confidence:.2f})",
        }

    # Generate knowledge graph summary for next planning round
    kg_summary = ""
    if graph_store:
        kg_summary = _generate_kg_summary(graph_store)

    logger.info(
        "evaluator: continue (step=%d, conf=%.2f, new=%d, total=%d)",
        step, new_confidence, len(new_triples), total_added,
    )

    return {
        "confidence_score": new_confidence,
        "should_continue": True,
        "knowledge_graph_summary": kg_summary,
    }


def _determine_halt_reason(state: dict[str, Any], guard: SafetyGuard) -> str:
    """Determine the specific reason for halting."""
    step = state.get("iteration_step", 0)
    max_iter = state.get("max_iterations", guard.max_iterations)
    budget = state.get("token_budget_remaining", 0)

    if step >= max_iter:
        return f"iteration limit ({step}/{max_iter})"
    if budget <= 0:
        return f"token budget exhausted"
    return f"safety guard halt (stuck={guard._no_progress_count})"


def _compute_confidence(
    state: dict[str, Any],
    graph_store: Any | None,
) -> float:
    """Compute research confidence score based on multiple signals."""
    current = state.get("confidence_score", 0.0)
    new_triples = state.get("new_triples", [])
    total = state.get("total_triples_added", 0)
    step = state.get("iteration_step", 0)

    # Base confidence grows with total triples (diminishing returns)
    if total > 0:
        triple_factor = min(1.0, total / 50)  # Saturates around 50 triples
    else:
        triple_factor = 0.0

    # New triples this round indicate progress
    if new_triples:
        progress_factor = min(0.3, len(new_triples) * 0.03)
    else:
        progress_factor = -0.05  # Slight penalty for no progress

    # Graph coverage from blind spots
    coverage_factor = 0.0
    if graph_store:
        try:
            from finagent.graph_v2.blind_spots import BlindSpotClassifier
            classifier = BlindSpotClassifier(graph_store)
            spots = classifier.find_all(max_results=100)
            total_nodes = graph_store.g.number_of_nodes()
            if total_nodes > 0:
                # Fewer blind spots per node = higher coverage
                blind_ratio = len(spots) / total_nodes
                coverage_factor = max(0, 1.0 - blind_ratio * 2)
        except Exception:
            pass

    new_confidence = (
        triple_factor * 0.4
        + coverage_factor * 0.4
        + progress_factor
        + current * 0.1  # Momentum from previous estimate
    )

    return max(0.0, min(1.0, new_confidence))


def _generate_kg_summary(graph_store: Any) -> str:
    """Generate a compact text summary of the current graph state."""
    try:
        stats = graph_store.stats()
        lines = [
            f"图谱概况: {stats['total_nodes']} 节点, {stats['total_edges']} 边",
        ]

        # Node type breakdown
        for nt, count in sorted(stats["node_types"].items(), key=lambda x: -x[1]):
            lines.append(f"  {nt}: {count}")

        # Edge type breakdown
        if stats["edge_types"]:
            lines.append("关系类型:")
            for et, count in sorted(stats["edge_types"].items(), key=lambda x: -x[1])[:5]:
                lines.append(f"  {et}: {count}")

        # Top entities by degree
        top_nodes = sorted(
            graph_store.g.degree(),
            key=lambda x: -x[1],
        )[:10]
        if top_nodes:
            lines.append("核心实体:")
            for nid, degree in top_nodes:
                label = (graph_store.get_node(nid) or {}).get("label", nid)
                lines.append(f"  {label} (度={degree})")

        return "\n".join(lines)
    except Exception as exc:
        logger.warning("kg summary generation failed: %s", exc)
        return f"(图谱概况生成失败: {exc})"
