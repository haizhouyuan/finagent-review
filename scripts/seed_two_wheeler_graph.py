"""Seed a two-wheeler pilot graph using the shared refresh catalog."""

from __future__ import annotations

import argparse
import sys
from typing import Any

from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finagent.graph_v2.store import GraphStore
from finagent.two_wheeler_refresh import DEFAULT_CATALOG_PATH, apply_graph_refresh, load_catalog


def seed_two_wheeler_graph(
    db_path: str = "finagent.db",
    *,
    catalog_path: str | Path | None = DEFAULT_CATALOG_PATH,
) -> dict[str, Any]:
    store = GraphStore(db_path)
    try:
        apply_graph_refresh(store, catalog=load_catalog(catalog_path))
        return store.stats()
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="finagent.db")
    parser.add_argument("--catalog-path", default=str(DEFAULT_CATALOG_PATH))
    args = parser.parse_args()
    stats = seed_two_wheeler_graph(args.db_path, catalog_path=args.catalog_path)
    print(f"Seeded: {stats['total_nodes']} nodes, {stats['total_edges']} edges")


if __name__ == "__main__":
    main()
