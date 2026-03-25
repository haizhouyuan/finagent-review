#!/usr/bin/env python3
"""Finagent V2 — 图谱驱动投研引擎 CLI.

Usage:
    python -m finagent.cli_research "商业航天固体火箭核心供应商" --llm mock
    python -m finagent.cli_research "中国星网产业链上游" --llm chatgptrest --iterations 5
    python -m finagent.cli_research graph-stats
    python -m finagent.cli_research blind-spots
    python -m finagent.cli_research parse /path/to/report.md --query "供应链"
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

# ── LLM Adapter (delegates to finagent.llm_adapter) ─────────────────


def _resolve_llm(backend: str, **kwargs):
    """Resolve LLM backend from CLI argument."""
    from finagent.llm_adapter import create_llm_adapter, auto_detect_adapter

    if backend == "auto":
        return auto_detect_adapter(**kwargs)
    return create_llm_adapter(backend, **kwargs)


def _make_web_search():
    """Web search using curl (with proxy support)."""
    import subprocess

    def web_search(query: str) -> str:
        from finagent.parsers.text_cleaner import TextCleaner
        try:
            result = subprocess.run(
                ["curl", "-s", "--connect-timeout", "8", "-m", "15",
                 f"https://www.google.com/search?q={query}&hl=zh-CN&num=5",
                 "-H", "User-Agent: Mozilla/5.0"],
                capture_output=True, timeout=20,
            )
            raw = result.stdout.decode("utf-8", errors="replace")
            return TextCleaner().clean(raw)
        except Exception:
            return ""

    return web_search


# ── Infrastructure Setup ────────────────────────────────────────────


def _setup_stores(db_path: str | None = None):
    """Initialize GraphStore and EvidenceStore.

    Uses finagent.paths canonical state root by default.
    """
    from finagent.graph_v2.store import GraphStore
    from finagent.agents.evidence_store import EvidenceStore
    from finagent.paths import resolve_paths, ensure_runtime_dirs

    if db_path is None:
        paths = resolve_paths()
        ensure_runtime_dirs(paths)
        db_path = str(paths.research_db_path)
    else:
        os.makedirs(Path(db_path).parent, exist_ok=True)

    evidence_db = str(Path(db_path).parent / "evidence.db")

    graph_store = GraphStore(db_path)
    evidence_store = EvidenceStore(evidence_db)

    return graph_store, evidence_store


# ── Command Handlers ────────────────────────────────────────────────


def cmd_research(args: argparse.Namespace) -> int:
    """Run a full research session via LangGraph."""
    from finagent.agents.orchestrator import run_research
    from finagent.agents.synthesizer import synthesize_report
    from finagent.research_ledger import ResearchLedger

    # Setup LLM via adapter layer
    try:
        llm_fn = _resolve_llm(args.llm)
    except (ValueError, RuntimeError) as exc:
        print(f"❌ LLM 初始化失败: {exc}")
        return 1

    if args.llm == "mock":
        search_fn = lambda q: f"Mock search result for: {q}。蓝箭航天和星河动力是主要竞争对手。"
    else:
        search_fn = _make_web_search()

    # Setup stores + ledger
    graph_store, evidence_store = _setup_stores(args.db)
    ledger = ResearchLedger(args.db)

    print(f"\n{'='*60}")
    print(f"🚀  Finagent V2 — 图谱驱动投研引擎")
    print(f"{'='*60}")
    print(f"🎯 目标: {args.query}")
    print(f"⚙️ 引擎: {args.llm} | 深度: {args.iterations} 轮 | 预算: {args.budget} tokens")
    print(f"📂 图谱: {args.db or 'state/research.sqlite'}")
    print(f"{'='*60}\n")

    try:
        final_state = run_research(
            args.query,
            context=args.context,
            llm_fn=llm_fn,
            search_fn=search_fn,
            graph_store=graph_store,
            evidence_store=evidence_store,
            max_iterations=args.iterations,
            token_budget=args.budget,
            confidence_threshold=args.confidence,
            verbose=True,
            ledger=ledger,
            llm_backend=args.llm,
            hitl_enabled=getattr(args, 'hitl', False),
        )

        run_id = final_state.get("run_id", "")

        # HITL interrupt — skip report, inform user
        if final_state.get("hitl_interrupted"):
            if run_id:
                print(f"\n📋 Run ID: {run_id}")
            print(f"使用 `finagent-research research-resume {run_id}` 审核后继续")
            return 0

        # Generate report
        report = synthesize_report(final_state, graph_store=graph_store)

        # Record report as artifact
        if run_id:
            ledger.record_artifact(run_id, "report", data={"length": len(report)})

        # Output
        if args.output:
            out_path = Path(args.output)
            out_path.write_text(report, encoding="utf-8")
            print(f"\n📄 报告已保存: {out_path}")
            if run_id:
                ledger.record_artifact(run_id, "report_file", path=str(out_path))
        else:
            print(f"\n{report}")

        # Print graph summary
        stats = graph_store.stats()
        print(f"\n📊 图谱现状: {stats['total_nodes']} 节点, {stats['total_edges']} 边")
        if run_id:
            print(f"📋 Run ID: {run_id}")

        # Serialize ResearchPackage (backfill report_md with synthesized report)
        package = final_state.get("research_package")
        if package and run_id:
            import json
            from finagent.paths import resolve_paths

            package.report_md = report  # Backfill after synthesis

            pkg_dir = resolve_paths().state_dir / "runs" / run_id
            pkg_dir.mkdir(parents=True, exist_ok=True)
            pkg_path = pkg_dir / "package.json"
            pkg_path.write_text(
                json.dumps(package.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            ledger.record_artifact(run_id, "research_package", path=str(pkg_path))
            print(f"📦 Package: {pkg_path}")
            print(f"   证据: {len(package.evidence_refs)} refs, "
                  f"三元组: {len(package.triples)}, "
                  f"置信度: {package.confidence:.2f}")

    except Exception as exc:
        print(f"\n❌ 研究失败: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        graph_store.close()
        evidence_store.close()
        ledger.close()

    return 0


def cmd_parse(args: argparse.Namespace) -> int:
    """Parse a document through the cleaner + chunker pipeline."""
    from finagent.parsers.document_parser import DocumentParser

    path = Path(args.path)
    if not path.exists():
        print(f"❌ File not found: {path}")
        return 1

    graph_store, evidence_store = _setup_stores(args.db)

    try:
        parser = DocumentParser(
            evidence_store=evidence_store,
            max_chars_per_chunk=args.chunk_size,
        )

        refs = parser.parse_file(path, query=args.query or path.stem)

        print(f"\n📖 Parsed: {path.name}")
        print(f"   Chunks: {len(refs)}")
        for i, ref in enumerate(refs):
            eid = ref.get("evidence_id")
            chars = ref.get("char_count", 0)
            heading = ref.get("heading", "")
            print(f"   [{i+1}] {chars} chars"
                  f"{f'  [{heading[:40]}]' if heading else ''}"
                  f"  (id={eid})" if eid else "")

        print(f"\n✅ {len(refs)} chunks stored in EvidenceStore")

    except Exception as exc:
        print(f"❌ Parse failed: {exc}")
        return 1
    finally:
        graph_store.close()
        evidence_store.close()

    return 0


def cmd_graph_stats(args: argparse.Namespace) -> int:
    """Show current knowledge graph statistics."""
    graph_store, evidence_store = _setup_stores(args.db)

    try:
        stats = graph_store.stats()

        print(f"\n📊 Knowledge Graph Statistics")
        print(f"{'='*40}")
        print(f"  Total nodes: {stats['total_nodes']}")
        print(f"  Total edges: {stats['total_edges']}")

        if stats.get("node_types"):
            print(f"\n  Node types:")
            for nt, count in sorted(stats["node_types"].items(), key=lambda x: -x[1]):
                print(f"    {nt}: {count}")

        if stats.get("edge_types"):
            print(f"\n  Edge types:")
            for et, count in sorted(stats["edge_types"].items(), key=lambda x: -x[1]):
                print(f"    {et}: {count}")

        # Top nodes by degree
        if graph_store.g.number_of_nodes() > 0:
            top = sorted(graph_store.g.degree(), key=lambda x: -x[1])[:10]
            print(f"\n  Top nodes (by degree):")
            for nid, deg in top:
                node = graph_store.get_node(nid) or {}
                label = node.get("label", nid)
                print(f"    {label} (degree={deg})")

    except Exception as exc:
        print(f"❌ Error: {exc}")
        return 1
    finally:
        graph_store.close()
        evidence_store.close()

    return 0


def cmd_blind_spots(args: argparse.Namespace) -> int:
    """Show blind spots in the current knowledge graph."""
    graph_store, evidence_store = _setup_stores(args.db)

    try:
        from finagent.graph_v2.blind_spots import BlindSpotClassifier
        classifier = BlindSpotClassifier(graph_store)
        spots = classifier.find_all(max_results=args.limit)

        if not spots:
            print("\n✅ No blind spots detected — graph looks comprehensive!")
            return 0

        print(f"\n🔍 Blind Spots ({len(spots)} found)")
        print(f"{'='*50}")
        for i, s in enumerate(spots):
            print(f"  [{i+1}] [{s.spot_type.value}] {s.description}")
            print(f"      Priority: {s.priority:.2f}")

    except Exception as exc:
        print(f"❌ Error: {exc}")
        return 1
    finally:
        graph_store.close()
        evidence_store.close()

    return 0


def cmd_stale(args: argparse.Namespace) -> int:
    """Show stale edges that need re-research."""
    graph_store, evidence_store = _setup_stores(args.db)

    try:
        from finagent.graph_v2.temporal import TemporalQuery
        tq = TemporalQuery(graph_store)
        stale = tq.stale_edges(threshold=args.threshold)

        if not stale:
            print("\n✅ No stale edges — data is fresh!")
            return 0

        print(f"\n⏰ Stale Edges ({len(stale)} found, threshold={args.threshold})")
        print(f"{'='*60}")
        for edge in stale[:20]:
            print(f"  {edge.get('source', '?')} → {edge.get('target', '?')}")
            print(f"    decayed_confidence={edge.get('decayed_confidence', '?'):.3f}, "
                  f"age={edge.get('age_days', '?')} days")

    except Exception as exc:
        print(f"❌ Error: {exc}")
        return 1
    finally:
        graph_store.close()
        evidence_store.close()

    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Export the knowledge graph as JSON."""
    graph_store, evidence_store = _setup_stores(args.db)

    try:
        import networkx as nx
        data = nx.node_link_data(graph_store.g)

        if args.output:
            Path(args.output).write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8",
            )
            print(f"📦 Exported to {args.output}")
        else:
            print(json.dumps(data, ensure_ascii=False, indent=2))

    except Exception as exc:
        print(f"❌ Error: {exc}")
        return 1
    finally:
        graph_store.close()
        evidence_store.close()

    return 0


def cmd_research_status(args: argparse.Namespace) -> int:
    """Show status of a research run."""
    from finagent.research_ledger import ResearchLedger

    ledger = ResearchLedger(args.db)
    try:
        run = ledger.get_run(args.run_id)
        if run is None:
            print(f"❌ Run not found: {args.run_id}")
            return 1

        print(f"\n{'='*60}")
        print(f"📋 Research Run: {run.run_id}")
        print(f"{'='*60}")
        print(f"   Goal:       {run.goal}")
        print(f"   Context:    {run.context}")
        print(f"   Status:     {run.status}")
        print(f"   LLM:        {run.llm_backend}")
        print(f"   Iterations: {run.current_iteration}/{run.max_iterations}")
        print(f"   Triples:    {run.total_triples}")
        print(f"   Confidence: {run.confidence_score:.2f}")
        print(f"   Reason:     {run.termination_reason or '-'}")
        print(f"   Created:    {run.created_at}")
        if run.completed_at:
            print(f"   Completed:  {run.completed_at}")
        if run.error:
            print(f"   Error:      {run.error}")

        steps = ledger.get_steps(args.run_id)
        if steps:
            print(f"\n   Steps ({len(steps)}):")
            for s in steps:
                ended = s['ended_at'] or 'running...'
                print(f"     [{s['step_id']}] {s['node_name']} iter={s['iteration']} "
                      f"tokens~{s['token_cost_est']} | {s['started_at']} → {ended}")

        artifacts = ledger.get_artifacts(args.run_id)
        if artifacts:
            print(f"\n   Artifacts ({len(artifacts)}):")
            for a in artifacts:
                print(f"     [{a['artifact_id']}] {a['kind']} {a['path'] or ''} {a['created_at']}")

        print()
    finally:
        ledger.close()
    return 0


def cmd_research_list(args: argparse.Namespace) -> int:
    """List recent research runs."""
    from finagent.research_ledger import ResearchLedger

    ledger = ResearchLedger(args.db)
    try:
        runs = ledger.list_runs(status=args.status, limit=args.limit)
        if not runs:
            print("No runs found.")
            return 0

        print(f"\n{'Run ID':<20} {'Status':<12} {'Triples':>8} {'Conf':>6} {'Goal'}")
        print("-" * 80)
        for r in runs:
            print(f"{r.run_id:<20} {r.status:<12} {r.total_triples:>8} "
                  f"{r.confidence_score:>5.2f} {r.goal[:30]}")
        print()
    finally:
        ledger.close()
    return 0


def cmd_research_resume(args: argparse.Namespace) -> int:
    """Resume an interrupted/paused research run."""
    from finagent.agents.orchestrator import resume_research
    from finagent.agents.synthesizer import synthesize_report
    from finagent.research_ledger import ResearchLedger

    # Setup LLM via adapter layer
    try:
        llm_fn = _resolve_llm(args.llm)
    except (ValueError, RuntimeError) as exc:
        print(f"❌ LLM 初始化失败: {exc}")
        return 1

    if args.llm == "mock":
        search_fn = lambda q: f"Mock search result for: {q}。蓝箭航天和星河动力是主要竞争对手。"
    else:
        search_fn = _make_web_search()

    graph_store, evidence_store = _setup_stores(args.db)
    ledger = ResearchLedger(args.db)

    try:
        final_state = resume_research(
            args.run_id,
            ledger=ledger,
            llm_fn=llm_fn,
            search_fn=search_fn,
            graph_store=graph_store,
            evidence_store=evidence_store,
            verbose=True,
        )

        # Generate report
        report = synthesize_report(final_state, graph_store=graph_store)
        run_id = final_state.get("run_id", args.run_id)
        ledger.record_artifact(run_id, "report", data={"length": len(report)})

        if args.output:
            out_path = Path(args.output)
            out_path.write_text(report, encoding="utf-8")
            print(f"\n📄 报告已保存: {out_path}")
            ledger.record_artifact(run_id, "report_file", path=str(out_path))
        else:
            print(f"\n{report}")

        # Serialize ResearchPackage (backfill report_md)
        package = final_state.get("research_package")
        if package and run_id:
            import json
            from finagent.paths import resolve_paths

            package.report_md = report  # Backfill after synthesis

            pkg_dir = resolve_paths().state_dir / "runs" / run_id
            pkg_dir.mkdir(parents=True, exist_ok=True)
            pkg_path = pkg_dir / "package.json"
            pkg_path.write_text(
                json.dumps(package.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            ledger.record_artifact(run_id, "research_package", path=str(pkg_path))
            print(f"📦 Package: {pkg_path}")

    except ValueError as exc:
        print(f"❌ {exc}")
        return 1
    except Exception as exc:
        print(f"\n❌ 恢复失败: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        graph_store.close()
        evidence_store.close()
        ledger.close()

    return 0


# ── CLI Builder ─────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="finagent-research",
        description="Finagent V2 — 图谱驱动投研引擎",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "商业航天固体火箭核心供应商" --llm mock
  %(prog)s "中国星网产业链" --llm chatgptrest --iterations 5
  %(prog)s graph-stats
  %(prog)s blind-spots
  %(prog)s parse tests/fixtures/commercial_aerospace_report.md
  %(prog)s export --output graph.json
  %(prog)s stale --threshold 0.3
""",
    )

    sub = parser.add_subparsers(dest="command")

    # ── research (default command via positional arg) ─────────────
    p_research = sub.add_parser("research", help="Run a research session")
    p_research.add_argument("query", help="研究目标 (e.g., '商业航天核心供应商')")
    p_research.add_argument(
        "--llm",
        choices=["mock", "openai", "openai-compatible", "chatgptrest", "auto"],
        default="mock",
        help="LLM backend: mock (free testing), openai (direct API), "
             "openai-compatible (vLLM/Ollama/etc), chatgptrest (/v3/agent/turn), "
             "auto (detect best available)",
    )
    p_research.add_argument("--context", default="商业航天", help="领域上下文")
    p_research.add_argument("--iterations", type=int, default=3, help="最大迭代轮数")
    p_research.add_argument("--budget", type=int, default=50000, help="Token预算")
    p_research.add_argument("--confidence", type=float, default=0.85, help="置信度阈值")
    p_research.add_argument("--output", "-o", default="", help="报告输出路径")
    p_research.add_argument("--db", default=None, help="图谱数据库路径")
    p_research.add_argument("--hitl", action="store_true", default=False,
                            help="启用 HITL Gate: 在提取前暂停等待人工审核")
    p_research.set_defaults(func=cmd_research)

    # ── research-status ───────────────────────────────────────────
    p_status = sub.add_parser("research-status", help="Show status of a research run")
    p_status.add_argument("run_id", help="Run ID (e.g., run-abc123def456)")
    p_status.add_argument("--db", default=None)
    p_status.set_defaults(func=cmd_research_status)

    # ── research-list ─────────────────────────────────────────────
    p_list = sub.add_parser("research-list", help="List recent research runs")
    p_list.add_argument("--status", default=None, help="Filter by status")
    p_list.add_argument("--limit", type=int, default=20, help="Max results")
    p_list.add_argument("--db", default=None)
    p_list.set_defaults(func=cmd_research_list)

    # ── research-resume ───────────────────────────────────────────
    p_resume = sub.add_parser("research-resume", help="Resume an interrupted research run")
    p_resume.add_argument("run_id", help="Run ID to resume")
    p_resume.add_argument(
        "--llm",
        choices=["mock", "openai", "openai-compatible", "chatgptrest", "auto"],
        default="mock",
        help="LLM backend for resumed run",
    )
    p_resume.add_argument("--output", "-o", default="", help="报告输出路径")
    p_resume.add_argument("--db", default=None)
    p_resume.set_defaults(func=cmd_research_resume)

    # ── parse ─────────────────────────────────────────────────────
    p_parse = sub.add_parser("parse", help="Parse a document into EvidenceStore")
    p_parse.add_argument("path", help="文档路径 (.md, .txt, .html)")
    p_parse.add_argument("--query", default="", help="关联查询")
    p_parse.add_argument("--chunk-size", type=int, default=3000, help="最大切块字符数")
    p_parse.add_argument("--db", default=None)
    p_parse.set_defaults(func=cmd_parse)

    # ── graph-stats ───────────────────────────────────────────────
    p_stats = sub.add_parser("graph-stats", help="Show graph statistics")
    p_stats.add_argument("--db", default=None)
    p_stats.set_defaults(func=cmd_graph_stats)

    # ── blind-spots ───────────────────────────────────────────────
    p_spots = sub.add_parser("blind-spots", help="Show blind spots")
    p_spots.add_argument("--limit", type=int, default=20)
    p_spots.add_argument("--db", default=None)
    p_spots.set_defaults(func=cmd_blind_spots)

    # ── stale ─────────────────────────────────────────────────────
    p_stale = sub.add_parser("stale", help="Show stale edges needing re-research")
    p_stale.add_argument("--threshold", type=float, default=0.3)
    p_stale.add_argument("--db", default=None)
    p_stale.set_defaults(func=cmd_stale)

    # ── export ────────────────────────────────────────────────────
    p_export = sub.add_parser("export", help="Export graph as JSON")
    p_export.add_argument("--output", "-o", default="")
    p_export.add_argument("--db", default=None)
    p_export.set_defaults(func=cmd_export)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        # Default: treat first positional as research query
        if args.command:
            # Direct query without subcommand
            return cmd_research(argparse.Namespace(
                query=args.command, llm="mock", context="商业航天",
                iterations=3, budget=50000, confidence=0.85,
                output="", db=None,
            ))
        parser.print_help()
        return 0

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s: %(message)s",
    )

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
