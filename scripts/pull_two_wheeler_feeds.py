#!/usr/bin/env python3
"""Pull raw two-wheeler export files and run the connector chain."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finagent.two_wheeler_catalog import DEFAULT_SOURCE_DIR  # noqa: E402
from finagent.two_wheeler_feed_pull import (  # noqa: E402
    DEFAULT_PULL_CHANGELOG_DIR,
    DEFAULT_PULL_MANIFEST,
    run_two_wheeler_feed_pull,
    write_feed_pull_changelog,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_PULL_MANIFEST)
    parser.add_argument("raw_dir", type=Path, help="Output directory for pulled raw exports")
    parser.add_argument("bundle_dir", type=Path, help="Output directory for connector-ready bundle files")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--delta-path", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--changelog-path",
        type=Path,
        default=None,
        help="Optional changelog output path; defaults to state/two_wheeler_feed_pull/<run>.json",
    )
    args = parser.parse_args()

    summary = run_two_wheeler_feed_pull(
        args.manifest_path,
        args.raw_dir,
        args.bundle_dir,
        run_id=args.run_id,
        delta_path=args.delta_path,
        source_dir=args.source_dir,
        dry_run=args.dry_run,
    )
    run_id = summary["run_id"]
    changelog_path = args.changelog_path or DEFAULT_PULL_CHANGELOG_DIR / (
        f"{run_id}{'-dry-run' if args.dry_run else ''}.json"
    )
    written = write_feed_pull_changelog(summary, changelog_path)

    print("═══ Two-Wheeler Feed Pull ═══")
    print(f"Run ID: {run_id}")
    print(f"Manifest: {summary['manifest_path']}")
    print(f"Dry run: {summary['dry_run']}")
    print(f"Pulled files: {len(summary['pull']['pulled'])}")
    print(f"Skipped optional: {len(summary['pull']['skipped'])}")
    print(f"Raw dir: {summary['raw_dir']}")
    print(f"Bundle dir: {summary['bundle_dir']}")
    print(f"Source dir: {summary['source_dir']}")
    if summary["delta_path"]:
        print(f"Delta: {summary['delta_path']}")
    if summary["connector"] is not None:
        print(f"Connector files written: {len(summary['connector']['written_outputs'])}")
    print(f"Changelog: {written}")


if __name__ == "__main__":
    main()
