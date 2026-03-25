#!/usr/bin/env python3
"""Import external two-wheeler source deltas, update source files, and rebuild catalog."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finagent.two_wheeler_catalog import (  # noqa: E402
    DEFAULT_OUTPUT_PATH,
    DEFAULT_SOURCE_CHANGELOG_DIR,
    DEFAULT_SOURCE_DIR,
    apply_two_wheeler_source_delta,
    write_source_delta_changelog,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("delta_path", type=Path, help="JSON delta payload")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--catalog-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--run-id", default=None, help="Override run id written to changelog")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--changelog-path",
        type=Path,
        default=None,
        help="Optional changelog output path; defaults to state/two_wheeler_source_updates/<run>.json",
    )
    args = parser.parse_args()

    payload = json.loads(args.delta_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"invalid delta payload: {args.delta_path}")
    if args.run_id:
        payload["run_id"] = args.run_id
    payload.setdefault("run_id", args.delta_path.stem)

    summary = apply_two_wheeler_source_delta(
        payload,
        source_dir=args.source_dir,
        catalog_path=args.catalog_path,
        dry_run=args.dry_run,
    )

    changelog_path = args.changelog_path
    if changelog_path is None:
        suffix = "-dry-run" if args.dry_run else ""
        changelog_path = DEFAULT_SOURCE_CHANGELOG_DIR / f"{summary['run_id']}{suffix}.json"
    written = write_source_delta_changelog(summary, changelog_path)

    print("═══ Two-Wheeler Source Delta Import ═══")
    print(f"Run ID: {summary['run_id']}")
    print(f"Dry run: {summary['dry_run']}")
    print(f"Source dir: {summary['source_dir']}")
    print(f"Catalog: {summary['catalog_path']}")
    print(f"Meta updated fields: {len(summary['meta']['updated_fields'])}")
    print(f"Asset created: {len(summary['image_assets']['created'])}")
    print(f"Asset updated: {len(summary['image_assets']['updated'])}")
    print(f"SKU created: {len(summary['sku_records']['created'])}")
    print(f"SKU updated: {len(summary['sku_records']['updated'])}")
    print(f"Alias created: {len(summary['graph']['aliases']['created'])}")
    print(f"Alias updated: {len(summary['graph']['aliases']['updated'])}")
    print(f"Edge created: {len(summary['graph']['edges']['created'])}")
    print(f"Edge updated: {len(summary['graph']['edges']['updated'])}")
    print(f"Changelog: {written}")


if __name__ == "__main__":
    main()
