# Two-Wheeler Pilot Walkthrough

Date: 2026-03-23

## Goal

Implement the reviewed two-wheeler pilot blueprint on top of the existing `finagent` architecture, without new pip dependencies and without breaking the refs-only `gathered_evidence` contract.

Scope delivered:

- P1 memory core: episodic / semantic memory + consolidation
- P2 two-wheeler graph seed + hygiene
- P3 unified retrieval substrate: memory + graph + evidence
- P4 synchronous corrective retrieval loop in `searcher`
- P5 wiring into orchestrator / extractor / planner + pilot E2E
- Loop-in consolidation experiment: same-run semantic visibility behind a flag
- Incremental refresh scripts for real two-wheeler asset / SKU / graph data
- Feature-gated retrieval quality boosts for Chinese recall, rewrite, and rerank
- File-backed source-to-catalog build chain for two-wheeler refresh inputs
- External source-delta import path for SKU / supplier observation updates
- Structured delta collector for CSV/JSON observation bundles
- CI guard for source-to-catalog drift and two-wheeler integrity checks
- Raw feed connector for back-office SKU, supplier observations, and field media exports
- Source-backed feed connector lookup that follows current graph aliases/nodes
- Manifest-driven feed pull runner for file/glob/HTTP sources
- Authenticated HTTP feed pulls for bearer/basic/api-key integrations
- Native `langgraph` runtime verification wired into CI

## Commits

- `4d051a3` `Add two-wheeler memory core`
- `8d15161` `Seed two-wheeler graph and hygiene`
- `9ca89c8` `Add unified retrieval substrate`
- `9efe200` `Add corrective retrieval loop`
- `c70b2f4` `Wire pilot memory and retrieval stack`
- `bcb7b6b` `Add two-wheeler pilot walkthrough`
- `450e009` `Fix real GraphRetriever planner path`
- `09a981b` `Connect runtime memory consolidation`
- `a07754e` `Update pilot walkthrough for native runtime verification`
- `5433d41` `Add feature-gated loop consolidation`
- `953d2aa` `Add two-wheeler incremental refresh scripts`
- `46311dc` `Add feature-gated retrieval quality boosts`
- `7df2c91` `Externalize two-wheeler refresh catalog`
- `ec32ba1` `Add two-wheeler catalog build chain`
- `0c459d0` `Add two-wheeler source delta import`
- `67e4dc5` `Add two-wheeler delta collector`
- `ebd49bc` `Add two-wheeler catalog integrity CI step`
- `12c84cb` `Add two-wheeler feed connector`
- `117c113` `Add feed connector coverage to CI`
- `150166f` `Prefer source-backed graph lookup in feed connector`
- `850b6e2` `Add two-wheeler feed pull runner`
- `82543e7` `Add feed pull coverage to CI`
- `f5db04f` `Add authenticated feed pull support`
- `216cc96` `Add native langgraph CI verification`

## Key Files

- `finagent/memory.py`
- `finagent/memory_consolidation.py`
- `scripts/seed_two_wheeler_graph.py`
- `scripts/graph_hygiene_report.py`
- `finagent/agents/evidence_store.py`
- `finagent/retrieval_stack.py`
- `finagent/two_wheeler_refresh.py`
- `finagent/two_wheeler_catalog.py`
- `finagent/two_wheeler_delta_collector.py`
- `finagent/two_wheeler_feed_connector.py`
- `finagent/two_wheeler_feed_pull.py`
- `data/two_wheeler/catalog.json`
- `data/two_wheeler/feed_pull_manifest.template.json`
- `data/two_wheeler/sources/meta.json`
- `data/two_wheeler/sources/image_assets.json`
- `data/two_wheeler/sources/sku_catalog.json`
- `data/two_wheeler/sources/graph_observations.json`
- `finagent/agents/searcher.py`
- `finagent/agents/orchestrator.py`
- `finagent/agents/extractor.py`
- `finagent/agents/planner.py`
- `scripts/build_two_wheeler_catalog.py`
- `.github/workflows/ci.yml`
- `scripts/pull_two_wheeler_feeds.py`
- `scripts/connect_two_wheeler_feeds.py`
- `scripts/collect_two_wheeler_source_delta.py`
- `scripts/import_two_wheeler_source_delta.py`
- `scripts/refresh_two_wheeler_data.py`
- `tests/test_memory.py`
- `tests/test_memory_consolidation.py`
- `tests/test_two_wheeler_graph.py`
- `tests/test_two_wheeler_refresh.py`
- `tests/test_two_wheeler_catalog_builder.py`
- `tests/test_two_wheeler_delta_collector.py`
- `tests/test_two_wheeler_feed_connector.py`
- `tests/test_two_wheeler_feed_pull.py`
- `tests/test_two_wheeler_source_delta.py`
- `tests/test_native_langgraph_runtime.py`
- `tests/test_retrieval_stack.py`
- `tests/test_corrective_loop.py`
- `tests/test_pilot_e2e.py`

## Blueprint Corrections

The implementation followed real repository interfaces where they diverged from blueprint examples:

- `GraphRetriever` Chinese validation was asserted through the real `focus_node` local-retrieval path instead of assuming bare-query global retrieval would work.
- `EvidenceStore.search()` reads `raw_text` directly from SQLite and still returns only refs/metadata.
- `run_research()` now issues a real `run_id` even without a ledger so memory and package outputs stay traceable.
- No manager objects were put into LangGraph state. Wiring stayed in orchestrator closures / keyword args.
- Retrieval enhancements are behind explicit flags (`enable_retrieval_query_rewrite`, `enable_retrieval_llm_rerank`, `enable_retrieval_light_rerank`) instead of auto-enabling whenever `llm_fn` is present.
- Two-wheeler refresh now prefers file-backed catalog data (`data/two_wheeler/catalog.json` or `--catalog-path`) and only falls back to built-in defaults when no catalog file is available.

## Validation

Primary validation bundle:

- `tests/test_memory.py`
- `tests/test_memory_consolidation.py`
- `tests/test_two_wheeler_graph.py`
- `tests/test_two_wheeler_refresh.py`
- `tests/test_retrieval_stack.py`
- `tests/test_corrective_loop.py`
- `tests/test_pilot_e2e.py`

Related regressions:

- `tests/test_evidence_store.py`
- `tests/test_competitive_extraction.py`
- `tests/test_planner_retriever.py`
- `tests/test_research_package.py`
- `tests/test_shared_pipeline_e2e.py`
- `tests/test_real_seed_validation.py`

Combined result:

- Loop-consolidation bundle: `75 passed in 8.37s`
- Refresh-script bundle: `78 passed in 11.25s`
- Final fallback runtime bundle: `81 passed in 11.54s`
- Final native `langgraph` runtime bundle in local `.venv-native`: `81 passed in 11.86s`
- File-backed catalog refresh regressions: `24 passed in 4.87s`
- Catalog build-chain regressions: `27 passed in 5.38s`
- Expanded fallback regression bundle after catalog build chain: `85 passed in 9.53s`
- Expanded fallback regression bundle after source-delta import: `87 passed in 10.00s`
- Expanded fallback regression bundle after collector + CI guard: `89 passed in 10.21s`
- Expanded fallback regression bundle after raw feed connector: `91 passed in 10.64s`
- Expanded fallback regression bundle after source-backed connector lookup: `92 passed in 12.52s`
- Expanded fallback regression bundle after feed pull runner: `95 passed in 10.66s`
- Expanded fallback regression bundle after authenticated feed pulls + native CI wiring: `96 passed, 1 skipped in 15.41s`
- Expanded native `langgraph` regression bundle after authenticated feed pulls + native CI wiring in local `.venv-native`: `97 passed in 15.57s`
- CLI smoke: `scripts/refresh_two_wheeler_data.py --dry-run` and `scripts/seed_competitive_data.py --dry-run` both run directly and emit changelog / catalog info
- Catalog builder check: `scripts/build_two_wheeler_catalog.py --check` validates repo sources and generated `catalog.json` are in sync
- Source delta CLI smoke: `scripts/import_two_wheeler_source_delta.py --help` and dry-run test both pass
- Collector CLI smoke: `scripts/collect_two_wheeler_source_delta.py --help` passes and the collected delta can feed the real import chain
- Feed connector CLI smoke: `scripts/connect_two_wheeler_feeds.py --help` passes and the connector can emit a delta directly via `--delta-path`
- Feed connector now resolves supplier/customer/canonical names from the current file-backed source graph first, then falls back to built-in defaults
- Feed pull CLI smoke: `scripts/pull_two_wheeler_feeds.py --help` passes and supports manifest-driven file / glob / HTTP pulls with changelog output
- Feed pull auth coverage now proves bearer/basic/api-key headers against a real local HTTP server fixture
- GitHub Actions CI now verifies native `langgraph` imports plus `tests/test_native_langgraph_runtime.py`, and also runs `scripts/build_two_wheeler_catalog.py --check` plus the two-wheeler catalog / feed pull / feed connector / collector / import / refresh / graph test bundle on push and PR

## What The Pilot Now Proves

1. `run_research -> episodic memory growth`
2. `run_research -> loop consolidation -> same-run semantic visibility` when `enable_loop_consolidation=True`
3. `run_research -> runtime consolidation -> semantic growth`
4. `package -> competitive-only writeback` writes `asset_ledger` / `sku_catalog` without polluting `theses` / `sources` / `monitors`
5. `refresh_two_wheeler_data.py` can incrementally refresh asset ledger, SKU catalog, graph seed, aliases, and changelog without duplicate inflation
6. Two-wheeler refresh inputs can now live in `data/two_wheeler/catalog.json` or any external `--catalog-path`, so catalog updates no longer require Python code edits
7. The default two-wheeler catalog is now generated from append-friendly source files under `data/two_wheeler/sources`, with deterministic dedupe rules for assets, SKUs, aliases, and graph observations
8. External JSON deltas can now update `data/two_wheeler/sources` and rebuild `catalog.json` in one step, with dry-run preview and changelog output
9. Structured CSV/JSON observation bundles can now be collected into the new delta format, so upstream SKU / supplier feeds no longer need to hand-author delta JSON
10. CI now blocks drift between source files and generated catalog, and exercises the two-wheeler collector/import/build path automatically
11. Raw export files from SKU back-office, supplier observation sheets, and field media sheets can now be normalized into the collector bundle and optionally emitted straight to delta JSON
12. Feed connector node and alias resolution now tracks the current file-backed source graph, so new business nodes/aliases can be adopted without duplicating them into Python defaults
13. Scheduled or manual pull jobs can now fetch raw exports from local files, latest matching globs, or HTTP endpoints before entering the connector/delta chain
14. Authenticated HTTP pull jobs can now attach bearer/basic/api-key credentials from manifest/env config without changing code
15. Native `langgraph` runtime is now verified both locally and in CI instead of relying only on the fallback runner

## Remaining Risks

- The repo's default system Python still does not ship with `langgraph`; CI and local `.venv-native` now verify the native path, but team machines still need that runtime installed explicitly.
- Semantic promotion is heuristic-first. It is feature-gated to work without LLM, but production quality will improve once domain-specific promotion prompts are added on top.
- Evidence retrieval is still keyword / LIKE based. Chinese recall, light rerank, and LLM rewrite/rerank are stronger now, but this is still not embedding-grade retrieval.
- The default two-wheeler catalog is now generated from file-backed source inputs, external deltas can be imported, a structured collector exists, raw export connectors exist, and authenticated pull runners exist, but there is still no system-specific live integration to a supplier backend or SKU console.

## Next Steps

- Decide whether semantic consolidation should move deeper into the loop so later intra-run planner passes can consume newly promoted semantic memory even earlier than the current extractor->planner gap.
- Replace the new pull runner's generic file/glob/HTTP + auth manifest template with real supplier-system, field-collection, or SKU-console connectors.
- If retrieval quality becomes the bottleneck again, add a stronger rerank policy or embedding retrieval behind the same feature-gate boundary instead of replacing the current substrate.
