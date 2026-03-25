#!/usr/bin/env python3
"""Incrementally refresh two-wheeler state tables and graph seed."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finagent.two_wheeler_refresh import (
    DEFAULT_CHANGELOG_DIR,
    DEFAULT_CATALOG_PATH,
    DEFAULT_GRAPH_DB,
    DEFAULT_RUN_ID,
    DEFAULT_STATE_DB,
    refresh_two_wheeler_data,
    write_refresh_changelog,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-db", type=Path, default=DEFAULT_STATE_DB)
    parser.add_argument("--graph-db", type=Path, default=DEFAULT_GRAPH_DB)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--catalog-path", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--changelog-path",
        type=Path,
        default=None,
        help="Optional JSON changelog output path; defaults to state/two_wheeler_refresh/<run>.json",
    )
    parser.add_argument(
        "--skip-missing-assets",
        action="store_true",
        help="Skip missing local image files instead of failing",
    )
    args = parser.parse_args()

    summary = refresh_two_wheeler_data(
        state_db_path=args.state_db,
        graph_db_path=args.graph_db,
        run_id=args.run_id,
        strict_assets=not args.skip_missing_assets,
        dry_run=args.dry_run,
        catalog_path=args.catalog_path,
    )

    print("═══ Two-Wheeler Refresh ═══")
    print(f"Run ID: {summary['run_id']}")
    print(f"Dry run: {summary['dry_run']}")
    print(f"Asset created: {len(summary['state']['asset_ledger']['created'])}")
    print(f"Asset updated: {len(summary['state']['asset_ledger']['updated'])}")
    print(f"Asset unchanged: {len(summary['state']['asset_ledger']['unchanged'])}")
    print(f"SKU created: {len(summary['state']['sku_catalog']['created'])}")
    print(f"SKU updated: {len(summary['state']['sku_catalog']['updated'])}")
    print(f"SKU unchanged: {len(summary['state']['sku_catalog']['unchanged'])}")
    print(f"Graph nodes created: {len(summary['graph']['nodes']['created'])}")
    print(f"Graph edges created: {len(summary['graph']['edges']['created'])}")
    print(f"Graph aliases created: {len(summary['graph']['aliases']['created'])}")

    changelog_path = args.changelog_path
    if changelog_path is None:
        suffix = "-dry-run" if args.dry_run else ""
        changelog_path = DEFAULT_CHANGELOG_DIR / f"{args.run_id}{suffix}.json"
    path = write_refresh_changelog(summary, changelog_path)
    print(f"Catalog: {summary['catalog_path'] or '(built-in defaults)'}")
    print(f"Changelog: {path}")


if __name__ == "__main__":
    main()
