"""Multi-dimensional safety guard for the agent loop.

Implements three defensive layers from the Deep Research findings:
  1. Iteration / recursion limit (hard cap)
  2. Token budget enforcement
  3. Semantic deduplication / stuck-agent detection
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class SafetyVerdict(str, Enum):
    """Three-level safety signal."""
    CONTINUE = "continue"     # All clear
    WARN = "warn"             # Approaching limits
    HALT = "halt"             # Must stop immediately


class SafetyGuard:
    """Centralized safety mechanism for the research loop.

    Checks are evaluated in order of severity:
      1. Hard iteration limit
      2. Token budget exhaustion
      3. Consecutive duplicate query detection (stuck agent)
      4. Max triples per round (output sanity)
    """

    def __init__(
        self,
        *,
        max_iterations: int = 10,
        token_budget: int = 50_000,
        max_triples_per_round: int = 30,
        semantic_dup_threshold: float = 0.85,
        stuck_rounds_limit: int = 3,
        warn_budget_pct: float = 0.2,
    ):
        self.max_iterations = max_iterations
        self.token_budget = token_budget
        self.max_triples_per_round = max_triples_per_round
        self.semantic_dup_threshold = semantic_dup_threshold
        self.stuck_rounds_limit = stuck_rounds_limit
        self.warn_budget_pct = warn_budget_pct

        # Track consecutive rounds with no new triples
        self._no_progress_count = 0
        self._last_query_set: set[str] = set()

    def check(self, state: dict[str, Any]) -> SafetyVerdict:
        """Evaluate the current state against all safety conditions.

        Args:
            state: The current ResearchState dict.

        Returns:
            SafetyVerdict: CONTINUE, WARN, or HALT.
        """
        verdict = SafetyVerdict.CONTINUE
        reasons: list[str] = []

        # 1. Hard iteration limit
        step = state.get("iteration_step", 0)
        max_iter = state.get("max_iterations", self.max_iterations)
        if step >= max_iter:
            reasons.append(f"iteration limit reached ({step}/{max_iter})")
            verdict = SafetyVerdict.HALT
        elif step >= max_iter - 1:
            reasons.append(f"approaching iteration limit ({step}/{max_iter})")
            if verdict == SafetyVerdict.CONTINUE:
                verdict = SafetyVerdict.WARN

        # 2. Token budget
        budget = state.get("token_budget_remaining", self.token_budget)
        if budget <= 0:
            reasons.append(f"token budget exhausted ({budget})")
            verdict = SafetyVerdict.HALT
        elif budget < self.token_budget * self.warn_budget_pct:
            reasons.append(f"token budget low ({budget}/{self.token_budget})")
            if verdict == SafetyVerdict.CONTINUE:
                verdict = SafetyVerdict.WARN

        # 3. Stuck agent detection
        new_triples = state.get("new_triples", [])
        if len(new_triples) == 0:
            self._no_progress_count += 1
        else:
            self._no_progress_count = 0

        if self._no_progress_count >= self.stuck_rounds_limit:
            reasons.append(
                f"no new triples for {self._no_progress_count} consecutive rounds"
            )
            verdict = SafetyVerdict.HALT

        # 4. Query deduplication
        current_queries = set(state.get("pending_queries", []))
        if current_queries and current_queries == self._last_query_set:
            reasons.append("identical queries to previous round")
            if verdict == SafetyVerdict.CONTINUE:
                verdict = SafetyVerdict.WARN
        self._last_query_set = current_queries

        # 5. Output sanity
        if len(new_triples) > self.max_triples_per_round:
            reasons.append(
                f"too many triples in one round ({len(new_triples)} > "
                f"{self.max_triples_per_round})"
            )
            if verdict == SafetyVerdict.CONTINUE:
                verdict = SafetyVerdict.WARN

        if reasons:
            level = logging.WARNING if verdict == SafetyVerdict.HALT else logging.INFO
            logger.log(level, "safety check: %s → %s", reasons, verdict.value)

        return verdict

    def deduct_tokens(self, count: int, state: dict[str, Any]) -> dict[str, Any]:
        """Deduct tokens from the budget. Returns updated state fields."""
        remaining = state.get("token_budget_remaining", self.token_budget)
        new_remaining = max(0, remaining - count)
        return {"token_budget_remaining": new_remaining}

    def reset(self) -> None:
        """Reset internal counters for a new research session."""
        self._no_progress_count = 0
        self._last_query_set = set()

    @property
    def status_summary(self) -> str:
        """Human-readable status string."""
        return (
            f"SafetyGuard(max_iter={self.max_iterations}, "
            f"budget={self.token_budget}, "
            f"no_progress={self._no_progress_count}"
            f"/{self.stuck_rounds_limit})"
        )
