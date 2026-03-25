"""Collect structured two-wheeler observations into a source-delta payload."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from finagent.two_wheeler_catalog import _write_json

CSV_FILENAMES = {
    "image_assets": ("image_assets.csv",),
    "sku_records": ("sku_records.csv",),
    "graph_aliases": ("graph_aliases.csv",),
    "graph_edges": ("graph_edges.csv",),
    "graph_nodes": ("graph_nodes.csv",),
}
JSON_FILENAMES = {
    "meta_patch": ("meta_patch.json",),
    "image_assets": ("image_assets.json",),
    "sku_records": ("sku_records.json",),
    "graph": ("graph_observations.json", "supplier_observations.json"),
}
LIST_FIELDS = {
    "image_assets": (),
    "sku_records": ("style_tags", "evidence_sources"),
    "graph_edges": (),
    "graph_nodes": (),
}
BOOL_FIELDS = {"is_official"}
FLOAT_FIELDS = {"confidence"}
INT_FIELDS = {"char_count"}


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _split_list(value: str) -> list[str]:
    return [item.strip() for item in value.split("|") if item.strip()]


def _normalize_scalar(field: str, value: str) -> Any:
    if value == "":
        return ""
    if field in BOOL_FIELDS:
        return value.strip().lower() in {"1", "true", "yes", "y"}
    if field in FLOAT_FIELDS:
        return float(value)
    if field in INT_FIELDS:
        return int(value)
    return value


def _normalize_csv_row(row: dict[str, str], *, list_fields: tuple[str, ...]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field, value in row.items():
        if field is None:
            continue
        field_name = field.strip()
        if not field_name:
            continue
        raw_value = value.strip() if isinstance(value, str) else value
        if raw_value == "" and field_name not in list_fields:
            continue
        if field_name in list_fields:
            normalized[field_name] = _split_list(str(raw_value))
        else:
            normalized[field_name] = _normalize_scalar(field_name, str(raw_value))
    return normalized


def _read_csv_rows(path: Path, *, kind: str) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            _normalize_csv_row(row, list_fields=tuple(LIST_FIELDS.get(kind, ())))
            for row in reader
        ]


def _unwrap_rows(payload: Any, *, key: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload]
    if isinstance(payload, dict) and isinstance(payload.get(key), list):
        return [dict(row) for row in payload[key]]
    raise ValueError(f"expected list payload for {key}")


def _collect_json_rows(input_dir: Path, *, kind: str) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    used_files: list[str] = []
    for filename in JSON_FILENAMES.get(kind, ()):
        path = input_dir / filename
        if not path.exists():
            continue
        payload = _read_json(path)
        rows.extend(_unwrap_rows(payload, key=kind))
        used_files.append(str(path))
    return rows, used_files


def _collect_csv_rows(input_dir: Path, *, kind: str) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    used_files: list[str] = []
    for filename in CSV_FILENAMES.get(kind, ()):
        path = input_dir / filename
        if not path.exists():
            continue
        rows.extend(_read_csv_rows(path, kind=kind))
        used_files.append(str(path))
    return rows, used_files


def _collect_meta_patch(input_dir: Path) -> tuple[dict[str, Any], list[str]]:
    path = input_dir / "meta_patch.json"
    if not path.exists():
        return {}, []
    payload = _read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"invalid meta patch payload: {path}")
    return dict(payload), [str(path)]


def _collect_graph_payload(input_dir: Path) -> tuple[dict[str, Any], list[str]]:
    graph: dict[str, Any] = {"aliases": {}, "edges": [], "nodes": []}
    used_files: list[str] = []

    for filename in JSON_FILENAMES["graph"]:
        path = input_dir / filename
        if not path.exists():
            continue
        payload = _read_json(path)
        if not isinstance(payload, dict):
            raise ValueError(f"invalid graph payload: {path}")
        if isinstance(payload.get("aliases"), dict):
            graph["aliases"].update({str(alias): str(node_id) for alias, node_id in payload["aliases"].items()})
        elif isinstance(payload.get("aliases"), list):
            for row in payload["aliases"]:
                graph["aliases"][str(row["alias"])] = str(row["node_id"])
        if isinstance(payload.get("edges"), list):
            graph["edges"].extend(dict(row) for row in payload["edges"])
        if isinstance(payload.get("nodes"), list):
            graph["nodes"].extend(dict(row) for row in payload["nodes"])
        used_files.append(str(path))

    alias_rows, alias_files = _collect_csv_rows(input_dir, kind="graph_aliases")
    for row in alias_rows:
        graph["aliases"][str(row["alias"])] = str(row["node_id"])
    used_files.extend(alias_files)

    edge_rows, edge_files = _collect_csv_rows(input_dir, kind="graph_edges")
    graph["edges"].extend(edge_rows)
    used_files.extend(edge_files)

    node_rows, node_files = _collect_csv_rows(input_dir, kind="graph_nodes")
    graph["nodes"].extend(node_rows)
    used_files.extend(node_files)

    return graph, used_files


def collect_two_wheeler_source_delta(
    input_dir: str | Path,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    input_dir = Path(input_dir)

    meta_patch, meta_files = _collect_meta_patch(input_dir)
    image_assets_json, image_asset_json_files = _collect_json_rows(input_dir, kind="image_assets")
    image_assets_csv, image_asset_csv_files = _collect_csv_rows(input_dir, kind="image_assets")
    sku_rows_json, sku_json_files = _collect_json_rows(input_dir, kind="sku_records")
    sku_rows_csv, sku_csv_files = _collect_csv_rows(input_dir, kind="sku_records")
    graph_payload, graph_files = _collect_graph_payload(input_dir)

    payload = {
        "run_id": run_id or input_dir.name,
        "meta_patch": meta_patch,
        "image_assets": [*image_assets_json, *image_assets_csv],
        "sku_records": [*sku_rows_json, *sku_rows_csv],
        "graph": graph_payload,
    }
    payload["collector_summary"] = {
        "input_dir": str(input_dir),
        "used_files": [
            *meta_files,
            *image_asset_json_files,
            *image_asset_csv_files,
            *sku_json_files,
            *sku_csv_files,
            *graph_files,
        ],
        "counts": {
            "meta_patch_fields": len(meta_patch),
            "image_assets": len(payload["image_assets"]),
            "sku_records": len(payload["sku_records"]),
            "graph_aliases": len(payload["graph"]["aliases"]),
            "graph_edges": len(payload["graph"]["edges"]),
            "graph_nodes": len(payload["graph"]["nodes"]),
        },
    }
    return payload


def write_two_wheeler_source_delta(
    payload: dict[str, Any],
    output_path: str | Path,
) -> Path:
    return _write_json(Path(output_path), payload)
