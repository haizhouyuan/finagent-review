#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finagent.db import connect, init_db, list_rows, select_one  # noqa: E402
from finagent.paths import resolve_paths  # noqa: E402
from finagent.utils import domain_check_claim, json_dumps  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill claim provenance/domain fields.")
    parser.add_argument("--root", default="", help="Alternative repo root")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).resolve() if args.root else None
    paths = resolve_paths(root)
    conn = connect(paths.db_path)
    init_db(conn)
    claims = [dict(row) for row in list_rows(conn, "SELECT claim_id, artifact_id, claim_text, status FROM claims")]

    report = {
        "db_path": str(paths.db_path),
        "claims": len(claims),
        "updated": 0,
        "with_data_date": 0,
        "quarantined": 0,
        "failed_domain_checks": 0,
    }
    samples: list[dict[str, object]] = []

    for claim in claims:
        artifact = select_one(
            conn,
            "SELECT COALESCE(published_at, captured_at, '') AS claim_date FROM artifacts WHERE artifact_id = ?",
            (claim["artifact_id"],),
        )
        claim_date = artifact["claim_date"] if artifact else ""
        domain_result = domain_check_claim(claim["claim_text"], claim_date=claim_date)
        if domain_result["data_date"]:
            report["with_data_date"] += 1
        if domain_result["quarantine"]:
            report["quarantined"] += 1
            report["failed_domain_checks"] += 1
        if len(samples) < 5:
            samples.append(
                {
                    "claim_id": claim["claim_id"],
                    "data_date": domain_result["data_date"],
                    "freshness_status": domain_result["freshness_status"],
                    "warnings": len(domain_result["warnings"]),
                    "quarantine": domain_result["quarantine"],
                }
            )
        if args.dry_run:
            continue
        conn.execute(
            """
            UPDATE claims
            SET data_date = ?, domain_check_json = ?, freshness_status = ?, status = ?
            WHERE claim_id = ?
            """,
            (
                domain_result["data_date"],
                json_dumps(domain_result),
                domain_result["freshness_status"],
                "quarantined" if domain_result["quarantine"] else claim.get("status", "candidate"),
                claim["claim_id"],
            ),
        )
        report["updated"] += 1

    if not args.dry_run:
        conn.commit()
    report["sample"] = samples
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
