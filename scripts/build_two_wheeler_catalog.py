#!/usr/bin/env python3
"""Build the shared two-wheeler catalog from append-friendly source files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finagent.two_wheeler_catalog import (  # noqa: E402
    DEFAULT_OUTPUT_PATH,
    DEFAULT_SOURCE_DIR,
    build_two_wheeler_catalog,
    write_two_wheeler_catalog,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate the generated catalog matches the output file without writing",
    )
    args = parser.parse_args()

    catalog = build_two_wheeler_catalog(args.source_dir)
    rendered = json.dumps(catalog, ensure_ascii=False, indent=2) + "\n"

    if args.check:
        on_disk = args.output_path.read_text(encoding="utf-8") if args.output_path.exists() else ""
        if on_disk != rendered:
            print(f"Catalog drift detected: {args.output_path}")
            raise SystemExit(1)
        print(f"Catalog is up to date: {args.output_path}")
        return

    output_path = write_two_wheeler_catalog(catalog, args.output_path)
    print("═══ Two-Wheeler Catalog Build ═══")
    print(f"Source dir: {args.source_dir}")
    print(f"Output: {output_path}")
    print(f"Image assets: {len(catalog['image_assets'])}")
    print(f"SKUs: {len(catalog['sku_records'])}")
    print(f"Graph aliases: {len(catalog['graph']['aliases'])}")
    print(f"Graph edges: {len(catalog['graph']['edges'])}")


if __name__ == "__main__":
    main()
