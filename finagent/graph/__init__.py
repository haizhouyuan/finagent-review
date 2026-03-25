"""Legacy v1 knowledge graph package.

.. deprecated::
    This package is the v1 graph implementation. The canonical (production)
    graph engine is ``finagent.graph_v2``.

    - ``graph/`` (this package): v1 sentinel-era graph (IndustryChainGraph,
      discovery loop, conflict detector). Retained for backward compatibility
      and v1→v2 migration support only.
    - ``graph_v2/``: v2 canonical graph engine (NetworkX+SQLite store,
      temporal queries, blind-spot classifier, entity resolver, topology
      analysis). All new development targets graph_v2.

    Migration path: ``finagent.graph_v2.migration.migrate_v1_to_v2()``
"""

from .conflict_detector import build_graph_from_db, detect_conflicts, find_broken_support_chains

__all__ = [
    "build_graph_from_db",
    "detect_conflicts",
    "find_broken_support_chains",
]
