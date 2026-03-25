#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finagent.db import connect, init_db  # noqa: E402
from finagent.graph import build_graph_from_db, detect_conflicts, find_broken_support_chains  # noqa: E402
from finagent.paths import resolve_paths  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Build minimal claim graph and export conflict report.")
    parser.add_argument("--root", default="", help="Alternative repo root")
    parser.add_argument("--thesis-id", default="", help="Optional thesis scope")
    parser.add_argument("--output", required=True, help="Path to JSON report")
    args = parser.parse_args()

    root = Path(args.root).resolve() if args.root else None
    paths = resolve_paths(root)
    conn = connect(paths.db_path)
    init_db(conn)

    graph = build_graph_from_db(conn, thesis_id=args.thesis_id)
    conflicts = detect_conflicts(graph)
    broken = find_broken_support_chains(graph)
    stale_claims = [
        {
            "claim_id": node_id,
            "text": attrs.get("text", ""),
            "review_status": attrs.get("review_status", "unreviewed"),
        }
        for node_id, attrs in graph.nodes(data=True)
        if attrs.get("node_type") == "claim" and attrs.get("review_status") == "unreviewed"
    ]
    report = {
        "db_path": str(paths.db_path),
        "thesis_id": args.thesis_id,
        "summary": {
            "nodes": graph.number_of_nodes(),
            "edges": graph.number_of_edges(),
            "conflicts": len(conflicts),
            "unresolved_conflicts": sum(1 for item in conflicts if not item["resolved"]),
            "broken_support_chains": len(broken),
            "unreviewed_claims": len(stale_claims),
        },
        "conflicts": conflicts,
        "broken_support_chains": broken,
        "stale_claims": stale_claims[:50],
    }

    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"ok": True, "output": str(output_path), **report["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
