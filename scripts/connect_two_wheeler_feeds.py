#!/usr/bin/env python3
"""Normalize raw two-wheeler export files into a collector-ready observation bundle."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finagent.two_wheeler_catalog import DEFAULT_SOURCE_DIR  # noqa: E402
from finagent.two_wheeler_feed_connector import connect_two_wheeler_feeds  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_dir", type=Path, help="Directory containing raw export files")
    parser.add_argument("bundle_dir", type=Path, help="Output directory for collector-ready files")
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help="Optional source graph directory used to resolve current aliases/nodes",
    )
    parser.add_argument(
        "--delta-path",
        type=Path,
        default=None,
        help="Optional path to write a collected delta JSON after bundle generation",
    )
    args = parser.parse_args()

    summary = connect_two_wheeler_feeds(
        args.raw_dir,
        args.bundle_dir,
        run_id=args.run_id,
        delta_path=args.delta_path,
        source_dir=args.source_dir or DEFAULT_SOURCE_DIR,
    )

    print("═══ Two-Wheeler Feed Connect ═══")
    print(f"Run ID: {summary['run_id']}")
    print(f"Raw dir: {summary['raw_dir']}")
    print(f"Bundle dir: {summary['bundle_dir']}")
    print(f"Source dir: {summary['source_dir']}")
    print(f"Inputs used: {len(summary['used_inputs'])}")
    print(f"Files written: {len(summary['written_outputs'])}")
    print(f"Image assets: {summary['counts']['image_assets']}")
    print(f"SKUs: {summary['counts']['sku_records']}")
    print(f"Graph aliases: {summary['counts']['graph_aliases']}")
    print(f"Graph edges: {summary['counts']['graph_edges']}")
    if summary["delta_path"]:
        print(f"Delta: {summary['delta_path']}")


if __name__ == "__main__":
    main()
