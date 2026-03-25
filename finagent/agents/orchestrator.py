"""LangGraph DAG orchestrator for the research workflow.

Wires the four agent nodes into a state machine:

    START → Planner → Searcher → Extractor → Evaluator
                                                  ↓
                                     ┌────────────┴──────────┐
                                     │ should_continue?      │
                                     ├──────────┬───────────┤
                                     │ True     │ False      │
                                     ↓          ↓            │
                               Planner    Synthesizer ──→ END

⚠️ Architectural hardening applied:
 - EvidenceStore wired through for reference passing
 - No raw text flows through LangGraph State
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Callable

try:
    from langgraph.graph import StateGraph, END
    _LANGGRAPH_AVAILABLE = True
except ModuleNotFoundError:
    _LANGGRAPH_AVAILABLE = False
    END = "__end__"

    class _SimpleGraphState:
        def __init__(self, next_nodes: list[str] | tuple[str, ...] | None = None):
            self.next = tuple(next_nodes or [])

    class _SimpleCheckpointer:
        def __init__(self):
            self._store: dict[str, dict[str, Any]] = {}

        def get(self, thread_id: str) -> dict[str, Any] | None:
            payload = self._store.get(thread_id)
            if payload is None:
                return None
            return {
                "state": dict(payload["state"]),
                "next": list(payload["next"]),
            }

        def put(self, thread_id: str, state: dict[str, Any], next_nodes: list[str]) -> None:
            self._store[thread_id] = {
                "state": dict(state),
                "next": list(next_nodes),
            }

    _SIMPLE_CHECKPOINTS: dict[str, _SimpleCheckpointer] = {}

    class _SimpleCompiledGraph:
        def __init__(
            self,
            nodes: dict[str, Callable[[dict[str, Any]], dict[str, Any]]],
            edges: dict[str, str],
            conditional_edges: dict[str, tuple[Callable[[dict[str, Any]], str], dict[str, str]]],
            entry_point: str,
            *,
            checkpointer: _SimpleCheckpointer | None = None,
            interrupt_before: list[str] | None = None,
        ):
            self._nodes = nodes
            self._edges = edges
            self._conditional_edges = conditional_edges
            self._entry_point = entry_point
            self._checkpointer = checkpointer
            self._interrupt_before = set(interrupt_before or [])
            self._last_state: dict[str, _SimpleGraphState] = {}

        def invoke(self, state: dict[str, Any] | None, config: dict[str, Any] | None = None):
            config = config or {}
            thread_id = config.get("configurable", {}).get("thread_id", "default")

            if state is None:
                checkpoint = self._checkpointer.get(thread_id) if self._checkpointer else None
                if checkpoint is None:
                    raise ValueError(f"no checkpoint for thread_id={thread_id}")
                current_state = dict(checkpoint["state"])
                next_nodes = list(checkpoint["next"]) or [self._entry_point]
            else:
                current_state = dict(state)
                next_nodes = [self._entry_point]

            while next_nodes:
                node_name = next_nodes[0]
                if node_name in self._interrupt_before:
                    if self._checkpointer is not None:
                        self._checkpointer.put(thread_id, current_state, next_nodes)
                    self._last_state[thread_id] = _SimpleGraphState(next_nodes)
                    return current_state

                next_nodes.pop(0)
                result = self._nodes[node_name](current_state)
                if isinstance(result, dict):
                    current_state.update(result)

                if node_name in self._conditional_edges:
                    chooser, mapping = self._conditional_edges[node_name]
                    choice = chooser(current_state)
                    next_node = mapping[choice]
                    next_nodes = [] if next_node == END else [next_node]
                else:
                    next_node = self._edges.get(node_name)
                    next_nodes = [] if next_node in (None, END) else [next_node]

                if self._checkpointer is not None:
                    self._checkpointer.put(thread_id, current_state, next_nodes)

            self._last_state[thread_id] = _SimpleGraphState([])
            return current_state

        def get_state(self, config: dict[str, Any] | None = None):
            config = config or {}
            thread_id = config.get("configurable", {}).get("thread_id", "default")
            if self._checkpointer is not None:
                checkpoint = self._checkpointer.get(thread_id)
                if checkpoint is not None:
                    return _SimpleGraphState(checkpoint["next"])
            return self._last_state.get(thread_id, _SimpleGraphState([]))

    class StateGraph:
        def __init__(self, _state_type: Any):
            self._nodes: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {}
            self._edges: dict[str, str] = {}
            self._conditional_edges: dict[str, tuple[Callable[[dict[str, Any]], str], dict[str, str]]] = {}
            self._entry_point = ""

        def add_node(self, name: str, fn: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
            self._nodes[name] = fn

        def set_entry_point(self, name: str) -> None:
            self._entry_point = name

        def add_edge(self, source: str, target: str) -> None:
            self._edges[source] = target

        def add_conditional_edges(
            self,
            source: str,
            chooser: Callable[[dict[str, Any]], str],
            mapping: dict[str, str],
        ) -> None:
            self._conditional_edges[source] = (chooser, mapping)

        def compile(self, checkpointer: Any | None = None, interrupt_before: list[str] | None = None):
            return _SimpleCompiledGraph(
                self._nodes,
                self._edges,
                self._conditional_edges,
                self._entry_point,
                checkpointer=checkpointer,
                interrupt_before=interrupt_before,
            )

from .state import ResearchState, initial_state
from .safety import SafetyGuard
from .evidence_store import EvidenceStore
from .planner import planner_node
from .searcher import searcher_node
from .extractor import extractor_node
from .evaluator import evaluator_node

from finagent.research_contracts import ResearchPackage, EvidenceRef

logger = logging.getLogger(__name__)


def build_research_graph(
    *,
    llm_fn: Callable[[str, str], str] | None = None,
    search_fn: Callable[[str], str] | None = None,
    graph_store: Any | None = None,
    evidence_store: EvidenceStore | None = None,
    memory_manager: Any | None = None,
    retrieval_stack: Any | None = None,
    safety_guard: SafetyGuard | None = None,
    confidence_threshold: float = 0.85,
    ledger: Any | None = None,
    run_id: str = "",
    enable_loop_consolidation: bool = False,
) -> StateGraph:
    """Build the LangGraph research workflow.

    Args:
        llm_fn: Shared LLM call function (system, user) → str.
        search_fn: Web search function (query) → str.
        graph_store: GraphStore instance for reading/writing KG.
        evidence_store: EvidenceStore for raw text storage.
        safety_guard: SafetyGuard instance (creates default if None).
        confidence_threshold: Evaluator's "done" threshold.
        ledger: Optional ResearchLedger for step tracking.
        run_id: Run ID for step tracking (required if ledger is set).

    Returns:
        A compiled LangGraph StateGraph ready to invoke.
    """
    if safety_guard is None:
        safety_guard = SafetyGuard()

    workflow = StateGraph(ResearchState)

    # ── Step tracking wrapper ───────────────────────────────────
    def _tracked(node_name: str, fn: Callable):
        """Wrap a node function with ledger step tracking."""
        def wrapper(state: ResearchState) -> dict[str, Any]:
            iteration = state.get("iteration_step", 0)
            step_id = None
            if ledger and run_id:
                step_id = ledger.record_step(
                    run_id, node_name, iteration=iteration,
                )
            result = fn(state)
            if ledger and run_id and step_id is not None:
                output_keys = list(result.keys()) if isinstance(result, dict) else []
                ledger.complete_step(step_id, output_keys=output_keys)
                # Sync iteration + triples to run record after each node
                new_iter = result.get("iteration_step", iteration)
                ledger.update_run(
                    run_id,
                    current_iteration=new_iter,
                    total_triples=result.get(
                        "total_triples_added",
                        state.get("total_triples_added", 0),
                    ),
                )
            return result
        return wrapper

    # ── Build optional GraphRetriever for planner (P1b) ──────────
    _graph_retriever = None
    if graph_store is not None:
        try:
            from finagent.graph_v2.retrieval import GraphRetriever
            _graph_retriever = GraphRetriever(graph_store)
        except Exception as exc:
            logger.warning("failed to create GraphRetriever: %s", exc)

    # ── Define nodes ────────────────────────────────────────────

    def _planner(state: ResearchState) -> dict[str, Any]:
        return planner_node(
            state, llm_fn=llm_fn, graph_store=graph_store,
            graph_retriever=_graph_retriever,
            memory_manager=memory_manager,
        )

    def _searcher(state: ResearchState) -> dict[str, Any]:
        return searcher_node(
            state, search_fn=search_fn,
            evidence_store=evidence_store,
            retrieval_stack=retrieval_stack,
            llm_fn=llm_fn,
        )

    def _extractor(state: ResearchState) -> dict[str, Any]:
        return extractor_node(
            state, llm_fn=llm_fn, graph_store=graph_store,
            evidence_store=evidence_store,
            memory_manager=memory_manager,
        )

    def _consolidator(state: ResearchState) -> dict[str, Any]:
        return _run_memory_consolidation(memory_manager, llm_fn=llm_fn)

    def _evaluator(state: ResearchState) -> dict[str, Any]:
        return evaluator_node(
            state, safety_guard=safety_guard,
            graph_store=graph_store,
            confidence_threshold=confidence_threshold,
        )

    workflow.add_node("planner", _tracked("planner", _planner))
    workflow.add_node("searcher", _tracked("searcher", _searcher))
    workflow.add_node("extractor", _tracked("extractor", _extractor))
    if enable_loop_consolidation and memory_manager is not None:
        workflow.add_node("consolidator", _tracked("consolidator", _consolidator))
    workflow.add_node("evaluator", _tracked("evaluator", _evaluator))

    # ── Define edges ────────────────────────────────────────────

    workflow.set_entry_point("planner")

    workflow.add_edge("planner", "searcher")
    workflow.add_edge("searcher", "extractor")
    if enable_loop_consolidation and memory_manager is not None:
        workflow.add_edge("extractor", "consolidator")
        workflow.add_edge("consolidator", "evaluator")
    else:
        workflow.add_edge("extractor", "evaluator")

    # Conditional routing from evaluator
    def _should_continue(state: ResearchState) -> str:
        if state.get("should_continue", False):
            return "planner"
        return END

    workflow.add_conditional_edges(
        "evaluator",
        _should_continue,
        {"planner": "planner", END: END},
    )

    return workflow


def _run_memory_consolidation(
    memory_manager: Any | None,
    *,
    llm_fn: Callable[[str, str], str] | None = None,
) -> dict[str, Any]:
    """Run semantic promotion and return lightweight state updates."""
    if memory_manager is None:
        return {}

    from finagent.memory_consolidation import (
        execute_promotion,
        find_promotion_candidates,
    )

    candidates = find_promotion_candidates(memory_manager, llm_fn=llm_fn)
    promoted_ids = execute_promotion(
        memory_manager,
        candidates,
        dry_run=False,
    )
    return {
        "semantic_promotions": promoted_ids,
        "memory_counts": memory_manager.count_by_tier(),
    }


def _make_checkpointer(db_path: str | None = None):
    """Create a SqliteSaver checkpointer.

    Falls back to MemorySaver if sqlite checkpointer unavailable.
    """
    if not _LANGGRAPH_AVAILABLE:
        if db_path is None:
            from finagent.paths import resolve_paths
            db_path = str(resolve_paths().state_dir / "checkpoints.sqlite")
        saver = _SIMPLE_CHECKPOINTS.setdefault(db_path, _SimpleCheckpointer())
        return saver, db_path
    try:
        from langgraph.checkpoint.sqlite import SqliteSaver
        import sqlite3
        if db_path is None:
            from finagent.paths import resolve_paths
            db_path = str(resolve_paths().state_dir / "checkpoints.sqlite")
        conn = sqlite3.connect(db_path, check_same_thread=False)
        return SqliteSaver(conn), db_path
    except ImportError:
        logger.warning("langgraph-checkpoint-sqlite not installed, using MemorySaver")
        from langgraph.checkpoint.memory import MemorySaver
        return MemorySaver(), None


def _assemble_package(
    final_state: dict[str, Any],
    *,
    run_id: str = "",
    graph_store: Any | None = None,
    evidence_store: EvidenceStore | None = None,
) -> ResearchPackage:
    """Build a ResearchPackage from completed research state."""
    pkg_run_id = run_id or final_state.get("run_id", "")

    # Collect evidence refs from evidence store — scoped to this run
    evidence_refs: list[EvidenceRef] = []
    if evidence_store is not None:
        try:
            rows = evidence_store.list_all(run_id=pkg_run_id if pkg_run_id else None)
            for row in rows:
                evidence_refs.append(EvidenceRef(
                    evidence_id=row.get("id"),
                    query=row.get("query", ""),
                    char_count=row.get("char_count", 0),
                    source_type=row.get("source_type", "web_search"),
                    source_tier=row.get("source_tier", "unverified"),
                    source_uri=row.get("source_uri", ""),
                    published_at=row.get("published_at", ""),
                ))
        except Exception:
            pass

    # Graph stats
    node_count = edge_count = 0
    if graph_store is not None:
        try:
            stats = graph_store.stats()
            node_count = stats.get("total_nodes", 0)
            edge_count = stats.get("total_edges", 0)
        except Exception:
            pass

    return ResearchPackage(
        run_id=pkg_run_id,
        goal=final_state.get("research_goal", ""),
        context=final_state.get("context", ""),
        triples=final_state.get("new_triples", []),
        evidence_refs=evidence_refs,
        image_assets=final_state.get("image_assets", []),
        sku_records=final_state.get("sku_records", []),
        node_count=node_count,
        edge_count=edge_count,
        confidence=final_state.get("confidence_score", 0.0),
        blind_spots=[],
        iterations_used=final_state.get("iteration_step", 0),
        token_cost_est=50_000 - final_state.get("token_budget_remaining", 0),
    )


def run_research(
    goal: str,
    *,
    context: str = "商业航天",
    llm_fn: Callable[[str, str], str] | None = None,
    search_fn: Callable[[str], str] | None = None,
    graph_store: Any | None = None,
    evidence_store: EvidenceStore | None = None,
    memory_manager: Any | None = None,
    retrieval_stack: Any | None = None,
    max_iterations: int = 10,
    token_budget: int = 50_000,
    confidence_threshold: float = 0.85,
    verbose: bool = True,
    ledger: Any | None = None,
    llm_backend: str = "mock",
    checkpointer: Any | None = None,
    hitl_enabled: bool = False,
    enable_loop_consolidation: bool = False,
    enable_retrieval_query_rewrite: bool = False,
    enable_retrieval_llm_rerank: bool = False,
    enable_retrieval_light_rerank: bool = False,
) -> dict[str, Any]:
    """High-level API: run a complete research session.

    Returns the final state dict with all accumulated results.
    If ``ledger`` is provided (ResearchLedger), creates and updates
    a persistent run record for tracking and resume.
    If ``checkpointer`` is provided, uses it; otherwise creates a
    SqliteSaver from ``state/checkpoints.sqlite``.
    """
    # ── Create run record ────────────────────────────────────────
    run = None
    effective_run_id = f"run-{uuid.uuid4().hex[:12]}"
    if ledger is not None:
        run = ledger.create_run(
            goal=goal,
            context=context,
            llm_backend=llm_backend,
            max_iterations=max_iterations,
            token_budget=token_budget,
            confidence_threshold=confidence_threshold,
        )
        ledger.update_run(run.run_id, status="running")
        effective_run_id = run.run_id

    # ── Tag evidence with run_id ────────────────────────────────
    if evidence_store is not None:
        evidence_store.active_run_id = effective_run_id

    if retrieval_stack is None and any(
        item is not None for item in (graph_store, memory_manager, evidence_store)
    ):
        try:
            from finagent.retrieval_stack import RetrievalStack

            retrieval_stack = RetrievalStack(
                graph_store=graph_store,
                memory=memory_manager,
                evidence_store=evidence_store,
                llm_fn=llm_fn,
                enable_query_rewrite=enable_retrieval_query_rewrite,
                enable_llm_rerank=enable_retrieval_llm_rerank,
                enable_light_rerank=enable_retrieval_light_rerank,
            )
        except Exception as exc:
            logger.warning("failed to create RetrievalStack: %s", exc)

    # ── Checkpointer ────────────────────────────────────────────
    if checkpointer is None:
        checkpointer, _ = _make_checkpointer()

    safety = SafetyGuard(
        max_iterations=max_iterations,
        token_budget=token_budget,
    )

    workflow = build_research_graph(
        llm_fn=llm_fn,
        search_fn=search_fn,
        graph_store=graph_store,
        evidence_store=evidence_store,
        memory_manager=memory_manager,
        retrieval_stack=retrieval_stack,
        safety_guard=safety,
        confidence_threshold=confidence_threshold,
        ledger=ledger,
        run_id=run.run_id if run else "",
        enable_loop_consolidation=enable_loop_consolidation,
    )

    thread_id = run.run_id if run else "default"
    config = {"configurable": {"thread_id": thread_id}}
    interrupt = ["extractor"] if hitl_enabled else None
    graph = workflow.compile(
        checkpointer=checkpointer,
        interrupt_before=interrupt,
    )

    state = initial_state(
        goal,
        context=context,
        max_iterations=max_iterations,
        token_budget=token_budget,
    )
    state["run_id"] = effective_run_id

    if verbose:
        run_label = f" [{effective_run_id}]" if effective_run_id else ""
        print(f"🚀 研究启动{run_label}: {goal}")
        print(f"   领域: {context}, 最大迭代: {max_iterations}, Token预算: {token_budget}")

    try:
        final_state = graph.invoke(state, config)
    except Exception as exc:
        logger.error("research loop failed: %s", exc)
        final_state = dict(state)
        final_state["termination_reason"] = f"error: {exc}"
        final_state["errors"] = state.get("errors", []) + [str(exc)]
        if ledger and run:
            ledger.complete_run(
                run.run_id,
                status="failed",
                error=str(exc),
                termination_reason=f"error: {exc}",
            )

    else:
        # Check if graph was interrupted by HITL gate
        graph_state = graph.get_state(config)
        is_interrupted = bool(graph_state.next) if graph_state else False

        if is_interrupted and ledger and run:
            # HITL interrupt — mark as awaiting_human
            ledger.update_run(run.run_id, status="awaiting_human")
            final_state["hitl_interrupted"] = True
            final_state["hitl_next_node"] = list(graph_state.next)
            if verbose:
                evidence = final_state.get("gathered_evidence", [])
                print(f"\n⏸️  HITL Gate: 研究暂停，等待人工审核")
                print(f"   收集证据: {len(evidence)} 条")
                print(f"   下一步: {graph_state.next}")
                print(f"   使用 research-resume {run.run_id} 继续")
        elif ledger and run:
            ledger.complete_run(
                run.run_id,
                status="completed",
                total_triples=final_state.get("total_triples_added", 0),
                confidence_score=final_state.get("confidence_score", 0.0),
                termination_reason=final_state.get("termination_reason", "normal"),
            )

    # Inject run_id into final state for downstream consumers
    final_state["run_id"] = effective_run_id

    if (
        memory_manager is not None
        and not final_state.get("hitl_interrupted")
        and not enable_loop_consolidation
    ):
        try:
            final_state.update(
                _run_memory_consolidation(memory_manager, llm_fn=llm_fn)
            )
        except Exception as exc:
            logger.warning("memory consolidation skipped: %s", exc)

    # Assemble ResearchPackage on successful completion (not HITL interrupt)
    if not final_state.get("hitl_interrupted"):
        package = _assemble_package(
            final_state,
            run_id=effective_run_id,
            graph_store=graph_store,
            evidence_store=evidence_store,
        )
        final_state["research_package"] = package

    if verbose:
        print(f"\n{'='*60}")
        print(f"🏁 研究完成")
        if run:
            print(f"   Run ID: {run.run_id}")
        print(f"   迭代次数: {final_state.get('iteration_step', 0)}")
        print(f"   总三元组: {final_state.get('total_triples_added', 0)}")
        print(f"   置信度: {final_state.get('confidence_score', 0):.2f}")
        print(f"   终止原因: {final_state.get('termination_reason', 'unknown')}")
        if final_state.get("errors"):
            print(f"   错误: {final_state['errors']}")

    return final_state


def resume_research(
    run_id: str,
    *,
    ledger: Any,
    llm_fn: Callable[[str, str], str] | None = None,
    search_fn: Callable[[str], str] | None = None,
    graph_store: Any | None = None,
    evidence_store: EvidenceStore | None = None,
    memory_manager: Any | None = None,
    retrieval_stack: Any | None = None,
    checkpointer: Any | None = None,
    verbose: bool = True,
    enable_loop_consolidation: bool = False,
    enable_retrieval_query_rewrite: bool = False,
    enable_retrieval_llm_rerank: bool = False,
    enable_retrieval_light_rerank: bool = False,
) -> dict[str, Any]:
    """Resume a previously interrupted/paused research run.

    Loads run metadata from the ledger, re-creates the research graph
    with the same checkpointer, and calls graph.invoke(None) to
    resume from the last checkpoint.

    Returns the final state dict.
    """
    run = ledger.get_run(run_id)
    if run is None:
        raise ValueError(f"Run not found: {run_id}")

    if run.status == "completed":
        raise ValueError(f"Run {run_id} already completed")

    # ── Checkpointer ────────────────────────────────────────────
    if checkpointer is None:
        checkpointer, _ = _make_checkpointer()

    ledger.update_run(run_id, status="running")

    # ── Tag evidence with run_id ────────────────────────────────
    if evidence_store is not None:
        evidence_store.active_run_id = run_id

    if retrieval_stack is None and any(
        item is not None for item in (graph_store, memory_manager, evidence_store)
    ):
        try:
            from finagent.retrieval_stack import RetrievalStack

            retrieval_stack = RetrievalStack(
                graph_store=graph_store,
                memory=memory_manager,
                evidence_store=evidence_store,
                llm_fn=llm_fn,
                enable_query_rewrite=enable_retrieval_query_rewrite,
                enable_llm_rerank=enable_retrieval_llm_rerank,
                enable_light_rerank=enable_retrieval_light_rerank,
            )
        except Exception as exc:
            logger.warning("failed to create RetrievalStack: %s", exc)

    safety = SafetyGuard(
        max_iterations=run.max_iterations,
        token_budget=run.token_budget,
    )

    workflow = build_research_graph(
        llm_fn=llm_fn,
        search_fn=search_fn,
        graph_store=graph_store,
        evidence_store=evidence_store,
        memory_manager=memory_manager,
        retrieval_stack=retrieval_stack,
        safety_guard=safety,
        confidence_threshold=run.confidence_threshold,
        ledger=ledger,
        run_id=run_id,
        enable_loop_consolidation=enable_loop_consolidation,
    )

    config = {"configurable": {"thread_id": run_id}}
    graph = workflow.compile(checkpointer=checkpointer)

    if verbose:
        print(f"🔄 研究恢复 [{run_id}]: {run.goal}")
        print(f"   迭代: {run.current_iteration}/{run.max_iterations}")

    try:
        # invoke(None) resumes from last checkpoint
        final_state = graph.invoke(None, config)
    except Exception as exc:
        logger.error("resume failed: %s", exc)
        final_state = {"termination_reason": f"error: {exc}", "errors": [str(exc)]}
        ledger.complete_run(
            run_id,
            status="failed",
            error=str(exc),
            termination_reason=f"error: {exc}",
        )
    else:
        ledger.complete_run(
            run_id,
            status="completed",
            total_triples=final_state.get("total_triples_added", 0),
            confidence_score=final_state.get("confidence_score", 0.0),
            termination_reason=final_state.get("termination_reason", "normal"),
        )

    final_state["run_id"] = run_id
    if memory_manager is not None and not enable_loop_consolidation:
        try:
            final_state.update(
                _run_memory_consolidation(memory_manager, llm_fn=llm_fn)
            )
        except Exception as exc:
            logger.warning("memory consolidation skipped on resume: %s", exc)

    # Assemble ResearchPackage on successful completion
    if final_state.get("termination_reason", "") and "error" not in final_state.get("termination_reason", ""):
        package = _assemble_package(
            final_state,
            run_id=run_id,
            graph_store=graph_store,
            evidence_store=evidence_store,
        )
        final_state["research_package"] = package

    if verbose:
        print(f"\n{'='*60}")
        print(f"🏁 研究恢复完成")
        print(f"   Run ID: {run_id}")
        print(f"   迭代次数: {final_state.get('iteration_step', 0)}")
        print(f"   总三元组: {final_state.get('total_triples_added', 0)}")
        print(f"   置信度: {final_state.get('confidence_score', 0):.2f}")
        print(f"   终止原因: {final_state.get('termination_reason', 'unknown')}")

    return final_state
