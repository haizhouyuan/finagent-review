"""Build and update the file-backed two-wheeler catalog."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE_DIR = REPO_ROOT / "data" / "two_wheeler" / "sources"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data" / "two_wheeler" / "catalog.json"
DEFAULT_SOURCE_CHANGELOG_DIR = REPO_ROOT / "state" / "two_wheeler_source_updates"

SOURCE_FILES = {
    "meta": "meta.json",
    "image_assets": "image_assets.json",
    "sku_records": "sku_catalog.json",
    "graph": "graph_observations.json",
}


def _read_json(path: Path, *, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _read_rows(path: Path, *, key: str) -> list[dict[str, Any]]:
    payload = _read_json(path, default=[])
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get(key), list):
        rows = payload[key]
    else:
        raise ValueError(f"expected list payload for {path}")
    return [dict(row) for row in rows]


def _dedupe_last_write(rows: Iterable[dict[str, Any]], *, key_field: str) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for raw_row in rows:
        row = dict(raw_row)
        key = str(row[key_field])
        if key not in merged:
            order.append(key)
        merged[key] = row
    return [merged[key] for key in order]


def _edge_key(row: dict[str, Any]) -> str:
    return "|".join(
        str(row[field])
        for field in ("source_id", "target_id", "edge_type", "valid_from")
    )


def _dedupe_edges(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for raw_row in rows:
        row = dict(raw_row)
        row.setdefault("valid_from", "2025-01-01")
        key = _edge_key(row)
        if key not in merged:
            order.append(key)
            merged[key] = row
            continue

        current = merged[key]
        current_confidence = float(current.get("confidence", 0.0) or 0.0)
        candidate_confidence = float(row.get("confidence", 0.0) or 0.0)
        if candidate_confidence >= current_confidence:
            merged[key] = row
    return [merged[key] for key in order]


def _read_aliases(path: Path) -> dict[str, str]:
    payload = _read_json(path, default={})
    alias_payload = payload.get("aliases", payload) if isinstance(payload, dict) else payload
    return _normalize_aliases(alias_payload, context=str(path))


def _read_meta(path: Path) -> dict[str, Any]:
    payload = _read_json(path, default={})
    if not isinstance(payload, dict):
        raise ValueError(f"invalid meta payload for {path}")
    return dict(payload)


def _normalize_aliases(alias_payload: Any, *, context: str) -> dict[str, str]:
    if isinstance(alias_payload, dict):
        return {str(alias): str(node_id) for alias, node_id in alias_payload.items()}
    if isinstance(alias_payload, list):
        aliases: dict[str, str] = {}
        for row in alias_payload:
            alias = row.get("alias")
            node_id = row.get("node_id")
            if alias is None or node_id is None:
                raise ValueError(f"invalid alias row in {context}: {row}")
            aliases[str(alias)] = str(node_id)
        return aliases
    raise ValueError(f"invalid aliases payload for {context}")


def load_two_wheeler_sources(source_dir: str | Path = DEFAULT_SOURCE_DIR) -> dict[str, Any]:
    source_dir = Path(source_dir)
    graph_path = source_dir / SOURCE_FILES["graph"]
    graph_payload = _read_json(graph_path, default={})
    if graph_payload and not isinstance(graph_payload, dict):
        raise ValueError(f"invalid graph payload for {graph_path}")

    return {
        "meta": _read_meta(source_dir / SOURCE_FILES["meta"]),
        "image_assets": _read_rows(source_dir / SOURCE_FILES["image_assets"], key="image_assets"),
        "sku_records": _read_rows(source_dir / SOURCE_FILES["sku_records"], key="sku_records"),
        "graph": {
            "nodes": (
                _read_rows(graph_path, key="nodes")
                if isinstance(graph_payload, dict) and "nodes" in graph_payload
                else []
            ),
            "edges": (
                _read_rows(graph_path, key="edges")
                if isinstance(graph_payload, dict) and "edges" in graph_payload
                else []
            ),
            "aliases": _read_aliases(graph_path) if graph_payload else {},
        },
    }


def build_two_wheeler_catalog_from_sources(source_payload: dict[str, Any]) -> dict[str, Any]:
    meta = dict(source_payload.get("meta", {}))
    image_assets = _dedupe_last_write(source_payload.get("image_assets", []), key_field="asset_id")
    sku_records = _dedupe_last_write(source_payload.get("sku_records", []), key_field="sku_id")
    graph = dict(source_payload.get("graph", {}))
    graph_nodes = _dedupe_last_write(
        graph.get("nodes", []),
        key_field="node_id",
    )
    graph_edges = _dedupe_edges(graph.get("edges", []))
    graph_aliases = _normalize_aliases(graph.get("aliases", {}), context="source_payload.graph.aliases")

    catalog: dict[str, Any] = {
        "goal": meta.get("goal", ""),
        "context": meta.get("context", ""),
        "triples": list(meta.get("triples", [])),
        "evidence_refs": list(meta.get("evidence_refs", [])),
        "report_md": meta.get("report_md", ""),
        "confidence": meta.get("confidence", 0.0),
        "image_assets": image_assets,
        "sku_records": sku_records,
        "graph": {
            "aliases": graph_aliases,
            "edges": graph_edges,
        },
    }
    if graph_nodes:
        catalog["graph"]["nodes"] = graph_nodes
    return catalog


def build_two_wheeler_catalog(
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
) -> dict[str, Any]:
    return build_two_wheeler_catalog_from_sources(load_two_wheeler_sources(source_dir))


def write_two_wheeler_catalog(
    catalog: dict[str, Any],
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    output_path = Path(output_path)
    return _write_json(output_path, catalog)


def write_two_wheeler_sources(
    source_payload: dict[str, Any],
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
) -> dict[str, str]:
    source_dir = Path(source_dir)
    paths = {
        "meta": str(_write_json(source_dir / SOURCE_FILES["meta"], source_payload.get("meta", {}))),
        "image_assets": str(
            _write_json(source_dir / SOURCE_FILES["image_assets"], list(source_payload.get("image_assets", [])))
        ),
        "sku_records": str(
            _write_json(source_dir / SOURCE_FILES["sku_records"], list(source_payload.get("sku_records", [])))
        ),
    }
    graph_payload = {
        "aliases": dict(source_payload.get("graph", {}).get("aliases", {})),
        "edges": list(source_payload.get("graph", {}).get("edges", [])),
    }
    graph_nodes = list(source_payload.get("graph", {}).get("nodes", []))
    if graph_nodes:
        graph_payload["nodes"] = graph_nodes
    paths["graph"] = str(_write_json(source_dir / SOURCE_FILES["graph"], graph_payload))
    return paths


def _merge_keyed_rows(
    existing_rows: Iterable[dict[str, Any]],
    incoming_rows: Iterable[dict[str, Any]],
    *,
    key_field: str,
    summary_key: str,
) -> tuple[list[dict[str, Any]], dict[str, list[Any]]]:
    merged = {str(row[key_field]): dict(row) for row in existing_rows}
    order = [str(row[key_field]) for row in existing_rows]
    summary: dict[str, list[Any]] = {"created": [], "updated": [], "unchanged": []}

    for raw_row in incoming_rows:
        row = dict(raw_row)
        key = str(row[key_field])
        current = merged.get(key)
        if current is None:
            merged[key] = row
            order.append(key)
            summary["created"].append({summary_key: key})
            continue
        if current == row:
            summary["unchanged"].append({summary_key: key})
            continue
        changed_fields = sorted(
            field
            for field in (set(current) | set(row))
            if current.get(field) != row.get(field)
        )
        merged[key] = row
        summary["updated"].append({summary_key: key, "fields": changed_fields})

    return [merged[key] for key in order], summary


def _merge_aliases(
    existing_aliases: dict[str, str],
    incoming_aliases: dict[str, str],
) -> tuple[dict[str, str], dict[str, list[Any]]]:
    merged = {str(alias): str(node_id) for alias, node_id in existing_aliases.items()}
    summary: dict[str, list[Any]] = {"created": [], "updated": [], "unchanged": []}
    for alias, node_id in incoming_aliases.items():
        alias_key = str(alias)
        node_value = str(node_id)
        current = merged.get(alias_key)
        if current is None:
            merged[alias_key] = node_value
            summary["created"].append({"alias": alias_key})
        elif current == node_value:
            summary["unchanged"].append({"alias": alias_key})
        else:
            merged[alias_key] = node_value
            summary["updated"].append({"alias": alias_key, "node_id": [current, node_value]})
    return merged, summary


def _merge_edges(
    existing_rows: Iterable[dict[str, Any]],
    incoming_rows: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, list[Any]]]:
    normalized_existing = [dict(row) for row in existing_rows]
    merged = {_edge_key(row): row for row in normalized_existing}
    order = [_edge_key(row) for row in normalized_existing]
    summary: dict[str, list[Any]] = {"created": [], "updated": [], "unchanged": []}

    for raw_row in incoming_rows:
        row = dict(raw_row)
        row.setdefault("valid_from", "2025-01-01")
        key = _edge_key(row)
        current = merged.get(key)
        edge_name = f"{row['source_id']}->{row['target_id']}:{row['edge_type']}"
        if current is None:
            merged[key] = row
            order.append(key)
            summary["created"].append({"edge": edge_name})
            continue
        if current == row:
            summary["unchanged"].append({"edge": edge_name})
            continue
        changed_fields = sorted(
            field
            for field in (set(current) | set(row))
            if current.get(field) != row.get(field)
        )
        merged[key] = row
        summary["updated"].append({"edge": edge_name, "fields": changed_fields})
    return [merged[key] for key in order], summary


def apply_two_wheeler_source_delta(
    delta_payload: dict[str, Any],
    *,
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
    catalog_path: str | Path = DEFAULT_OUTPUT_PATH,
    dry_run: bool = False,
) -> dict[str, Any]:
    current_sources = load_two_wheeler_sources(source_dir)
    meta = dict(current_sources.get("meta", {}))
    meta_patch = dict(delta_payload.get("meta_patch", {}))
    changed_meta_fields = sorted(
        field
        for field in meta_patch
        if meta.get(field) != meta_patch.get(field)
    )
    meta.update(meta_patch)

    image_assets, image_summary = _merge_keyed_rows(
        current_sources.get("image_assets", []),
        delta_payload.get("image_assets", []),
        key_field="asset_id",
        summary_key="asset_id",
    )
    sku_records, sku_summary = _merge_keyed_rows(
        current_sources.get("sku_records", []),
        delta_payload.get("sku_records", []),
        key_field="sku_id",
        summary_key="sku_id",
    )
    graph_payload = dict(delta_payload.get("graph", {}))
    graph_aliases, alias_summary = _merge_aliases(
        dict(current_sources.get("graph", {}).get("aliases", {})),
        _normalize_aliases(graph_payload.get("aliases", {}), context="delta_payload.graph.aliases"),
    )
    graph_edges, edge_summary = _merge_edges(
        current_sources.get("graph", {}).get("edges", []),
        graph_payload.get("edges", []),
    )
    graph_nodes, node_summary = _merge_keyed_rows(
        current_sources.get("graph", {}).get("nodes", []),
        graph_payload.get("nodes", []),
        key_field="node_id",
        summary_key="node_id",
    )

    updated_sources = {
        "meta": meta,
        "image_assets": image_assets,
        "sku_records": sku_records,
        "graph": {
            "aliases": graph_aliases,
            "edges": graph_edges,
            "nodes": graph_nodes,
        },
    }
    catalog = build_two_wheeler_catalog_from_sources(updated_sources)

    if not dry_run:
        write_two_wheeler_sources(updated_sources, source_dir)
        write_two_wheeler_catalog(catalog, catalog_path)

    return {
        "run_id": str(delta_payload.get("run_id", "source-delta")),
        "dry_run": dry_run,
        "source_dir": str(Path(source_dir)),
        "catalog_path": str(Path(catalog_path)),
        "source_files": {
            key: str(Path(source_dir) / filename)
            for key, filename in SOURCE_FILES.items()
        },
        "meta": {
            "updated_fields": changed_meta_fields,
        },
        "image_assets": image_summary,
        "sku_records": sku_summary,
        "graph": {
            "aliases": alias_summary,
            "edges": edge_summary,
            "nodes": node_summary,
        },
        "catalog_counts": {
            "image_assets": len(catalog["image_assets"]),
            "sku_records": len(catalog["sku_records"]),
            "graph_aliases": len(catalog["graph"]["aliases"]),
            "graph_edges": len(catalog["graph"]["edges"]),
            "graph_nodes": len(catalog["graph"].get("nodes", [])),
        },
        "dedupe_rules": [
            "image_assets: asset_id last-write-wins",
            "sku_records: sku_id last-write-wins",
            "graph.aliases: alias last-write-wins",
            "graph.edges: source_id+target_id+edge_type+valid_from upsert",
            "graph.nodes: node_id last-write-wins",
        ],
    }


def write_source_delta_changelog(
    summary: dict[str, Any],
    output_path: str | Path,
) -> Path:
    return _write_json(Path(output_path), summary)
