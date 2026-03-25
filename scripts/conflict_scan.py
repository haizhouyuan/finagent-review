#!/usr/bin/env python3
"""Conflict detection and resolution workflow.

Scans all active theses for conflicting claims and produces a structured
report. Optionally triggers dual-model arbitration via ChatgptREST.

Usage:
    python scripts/conflict_scan.py --root /path/to/finagent
    python scripts/conflict_scan.py --root /path/to/finagent --resolve
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


def _connect(root: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(root / "state" / "finagent.sqlite", timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _find_claim_conflicts(conn: sqlite3.Connection) -> list[dict]:
    """Detect claims with opposing stances on the same entity/topic."""
    # Get all claims with their routes to theses
    rows = conn.execute("""
        SELECT c.claim_id, c.claim_text, c.claim_type, c.confidence,
               cr.target_object_id AS thesis_id, cr.route_type,
               a.source_id, s.primaryness
        FROM claims c
        JOIN claim_routes cr ON c.claim_id = cr.claim_id
        JOIN artifacts a ON c.artifact_id = a.artifact_id
        JOIN sources s ON a.source_id = s.source_id
        WHERE c.status = 'candidate' AND cr.status = 'pending'
          AND cr.target_object_type = 'thesis'
    """).fetchall()

    # Group by thesis
    thesis_claims: dict[str, list[dict]] = {}
    for r in rows:
        tid = r["thesis_id"]
        if tid not in thesis_claims:
            thesis_claims[tid] = []
        thesis_claims[tid].append(dict(r))

    conflicts = []

    # Simple heuristic: look for claims with opposing risk signals
    positive_keywords = {"growth", "increase", "strong", "exceeding", "expanding", "doubling", "premium"}
    negative_keywords = {"decline", "risk", "compression", "erosion", "insufficient", "stuck", "impaired", "downside"}

    for thesis_id, claims in thesis_claims.items():
        positive = [c for c in claims if any(k in c["claim_text"].lower() for k in positive_keywords)]
        negative = [c for c in claims if any(k in c["claim_text"].lower() for k in negative_keywords)]

        if positive and negative:
            conflicts.append({
                "thesis_id": thesis_id,
                "positive_claims": [{"claim_id": c["claim_id"], "text": c["claim_text"][:100]} for c in positive[:3]],
                "negative_claims": [{"claim_id": c["claim_id"], "text": c["claim_text"][:100]} for c in negative[:3]],
                "severity": "high" if len(positive) >= 2 and len(negative) >= 2 else "medium",
            })

    return conflicts


def _scan_cross_thesis_conflicts(conn: sqlite3.Connection) -> list[dict]:
    """Detect logical contradictions across different theses."""
    theses = conn.execute(
        "SELECT thesis_id, title, status FROM theses WHERE status IN ('active', 'evidence_backed', 'framed')"
    ).fetchall()

    cross_conflicts = []
    thesis_list = [dict(t) for t in theses]

    # Check for theses on the same entity with opposing stances
    for i, t1 in enumerate(thesis_list):
        for t2 in thesis_list[i + 1:]:
            # Check if both theses reference overlapping routes
            overlap = conn.execute("""
                SELECT COUNT(*) FROM claim_routes cr1
                JOIN claim_routes cr2 ON cr1.claim_id = cr2.claim_id
                WHERE cr1.target_object_id = ? AND cr2.target_object_id = ?
            """, (t1["thesis_id"], t2["thesis_id"])).fetchone()[0]

            if overlap > 0:
                cross_conflicts.append({
                    "thesis_1": t1["thesis_id"],
                    "thesis_2": t2["thesis_id"],
                    "shared_claims": overlap,
                    "note": f"{t1['title']} vs {t2['title']}",
                })

    return cross_conflicts


def main():
    parser = argparse.ArgumentParser(description="Conflict scan & resolution workflow")
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--resolve", action="store_true",
                        help="Attempt dual-model arbitration for detected conflicts")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    conn = _connect(args.root)

    # Intra-thesis conflicts
    intra = _find_claim_conflicts(conn)
    # Cross-thesis conflicts
    cross = _scan_cross_thesis_conflicts(conn)

    result = {
        "intra_thesis_conflicts": intra,
        "cross_thesis_conflicts": cross,
        "total_intra": len(intra),
        "total_cross": len(cross),
        "severity_summary": {
            "high": sum(1 for c in intra if c["severity"] == "high"),
            "medium": sum(1 for c in intra if c["severity"] == "medium"),
        },
    }

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(f"\n{'='*60}")
        print(f"CONFLICT SCAN REPORT")
        print(f"{'='*60}")
        print(f"\nIntra-thesis conflicts: {len(intra)}")
        for c in intra:
            print(f"\n  [{c['severity'].upper()}] {c['thesis_id']}")
            print(f"    + {len(c['positive_claims'])} positive claims")
            print(f"    - {len(c['negative_claims'])} negative claims")
        print(f"\nCross-thesis conflicts: {len(cross)}")
        for c in cross:
            print(f"\n  {c['thesis_1']} ↔ {c['thesis_2']}: {c['shared_claims']} shared claims")
        print(f"\n{'='*60}")

    if args.resolve and intra:
        print("\n[RESOLVE] Would trigger dual-model arbitration via ChatgptREST...")
        # Future: integrate with chatgptrest_consult for dual-model review
        for conflict in intra:
            if conflict["severity"] == "high":
                question = (
                    f"Thesis '{conflict['thesis_id']}' has conflicting evidence:\n"
                    f"POSITIVE: {[c['text'] for c in conflict['positive_claims']]}\n"
                    f"NEGATIVE: {[c['text'] for c in conflict['negative_claims']]}\n"
                    f"Assess: which position is better supported? Should the thesis be maintained, modified, or invalidated?"
                )
                print(f"  → Would submit to dual-model: {question[:150]}...")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
