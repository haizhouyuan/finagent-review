#!/usr/bin/env python3
"""Seed competitive writeback tables with the shared two-wheeler catalog."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finagent.db import connect, init_db, list_rows
from finagent.two_wheeler_refresh import (
    DEFAULT_CATALOG_PATH,
    DEFAULT_STATE_DB,
    REPO_ROOT,
    apply_competitive_refresh,
    build_research_package,
    load_catalog,
    preview_competitive_refresh,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_STATE_DB, help="Path to finagent.sqlite")
    parser.add_argument("--catalog-path", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--dry-run", action="store_true", help="Plan only, don't write")
    args = parser.parse_args()

    conn = connect(args.db)
    init_db(conn)
    try:
        catalog = load_catalog(args.catalog_path)
        package = build_research_package(repo_root=REPO_ROOT, catalog=catalog)
        actions, preview = preview_competitive_refresh(conn, package)

        print("═══ Two-Wheeler Competitive Seed ═══")
        print(f"DB: {args.db}")
        print(f"Catalog: {args.catalog_path}")
        print(f"Images: {len(package.image_assets)}")
        print(f"SKUs: {len(package.sku_records)}")
        print(f"Planned actions: {preview['planned_actions']}")
        print(f"Apply actions: {preview['applied_actions']}")
        print(f"Create assets: {len(preview['asset_ledger']['created'])}")
        print(f"Update assets: {len(preview['asset_ledger']['updated'])}")
        print(f"Create SKUs: {len(preview['sku_catalog']['created'])}")
        print(f"Update SKUs: {len(preview['sku_catalog']['updated'])}")

        if args.dry_run:
            print("\n[DRY RUN] Skipping apply.")
            return

        summary = apply_competitive_refresh(conn, package)
        print("\n─── Verification ───")
        print(f"asset_ledger: {summary['row_counts']['asset_ledger']} rows")
        print(f"sku_catalog: {summary['row_counts']['sku_catalog']} rows")

        print("\n1. 九号全系 SKU:")
        for row in list_rows(
            conn,
            "SELECT model, price_range, frame_type FROM sku_catalog WHERE brand='九号' ORDER BY model",
        ):
            print(f"   {row['model']}: {row['price_range']} / {row['frame_type']}")

        print("\n2. 车架类型分布:")
        for row in list_rows(
            conn,
            "SELECT frame_type, COUNT(*) as cnt FROM sku_catalog GROUP BY frame_type ORDER BY cnt DESC",
        ):
            print(f"   {row['frame_type']}: {row['cnt']} models")

        print("\n3. 官方高质量图片:")
        for row in list_rows(
            conn,
            "SELECT brand, product_line FROM asset_ledger WHERE is_official=1 ORDER BY brand",
        ):
            print(f"   {row['brand']}: {row['product_line']}")

        if actions:
            print("\n✓ Competitive refresh applied without thesis/source/monitor targets")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
