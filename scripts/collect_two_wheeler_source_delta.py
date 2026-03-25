#!/usr/bin/env python3
"""Collect structured two-wheeler observations into a delta payload."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finagent.two_wheeler_delta_collector import (  # noqa: E402
    collect_two_wheeler_source_delta,
    write_two_wheeler_source_delta,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", type=Path, help="Directory containing structured observation files")
    parser.add_argument("output_path", type=Path, help="Where to write the collected delta JSON")
    parser.add_argument("--run-id", default=None, help="Override run id in the delta payload")
    args = parser.parse_args()

    payload = collect_two_wheeler_source_delta(args.input_dir, run_id=args.run_id)
    output_path = write_two_wheeler_source_delta(payload, args.output_path)

    summary = payload["collector_summary"]
    print("═══ Two-Wheeler Delta Collect ═══")
    print(f"Run ID: {payload['run_id']}")
    print(f"Input dir: {summary['input_dir']}")
    print(f"Output: {output_path}")
    print(f"Files used: {len(summary['used_files'])}")
    print(f"Image assets: {summary['counts']['image_assets']}")
    print(f"SKUs: {summary['counts']['sku_records']}")
    print(f"Graph aliases: {summary['counts']['graph_aliases']}")
    print(f"Graph edges: {summary['counts']['graph_edges']}")
    print(f"Graph nodes: {summary['counts']['graph_nodes']}")


if __name__ == "__main__":
    main()
