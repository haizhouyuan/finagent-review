"""Normalize raw two-wheeler export files into a collector-ready observation bundle."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from finagent.two_wheeler_catalog import DEFAULT_SOURCE_DIR, _write_json, load_two_wheeler_sources
from finagent.two_wheeler_delta_collector import (
    collect_two_wheeler_source_delta,
    write_two_wheeler_source_delta,
)
from finagent.two_wheeler_refresh import default_graph_aliases, default_graph_nodes

RAW_EXPORT_FILES = {
    "sku_backoffice": "sku_backoffice.csv",
    "supplier_observations": "supplier_observations.csv",
    "field_media": "field_media.csv",
    "alias_map": "alias_map.csv",
    "meta_patch": "meta_patch.json",
}
BUNDLE_FILES = {
    "sku_records": "sku_records.csv",
    "graph_edges": "graph_edges.csv",
    "graph_aliases": "graph_aliases.csv",
    "image_assets": "image_assets.json",
    "meta_patch": "meta_patch.json",
}
SKU_FIELDS = [
    "sku_id",
    "brand",
    "series",
    "model",
    "positioning",
    "price_range",
    "wheel_diameter",
    "frame_type",
    "motor_type",
    "battery_platform",
    "brake_config",
    "target_audience",
    "style_tags",
    "evidence_sources",
]
EDGE_FIELDS = [
    "source_id",
    "target_id",
    "edge_type",
    "valid_from",
    "confidence",
    "source",
    "evidence",
]
ALIAS_FIELDS = ["alias", "node_id"]


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv_rows(path: Path, *, fieldnames: list[str], rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    return path


def _normalize_key(value: str) -> str:
    return value.strip().lower()


def _build_node_lookup(source_dir: str | Path = DEFAULT_SOURCE_DIR) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for alias, node_id in default_graph_aliases().items():
        lookup[_normalize_key(str(alias))] = str(node_id)
    for node in default_graph_nodes():
        lookup[_normalize_key(str(node["node_id"]))] = str(node["node_id"])
        lookup[_normalize_key(str(node["label"]))] = str(node["node_id"])

    source_payload = load_two_wheeler_sources(source_dir)
    for alias, node_id in source_payload.get("graph", {}).get("aliases", {}).items():
        lookup[_normalize_key(str(alias))] = str(node_id)
    for node in source_payload.get("graph", {}).get("nodes", []):
        node_id = str(node["node_id"])
        lookup[_normalize_key(node_id)] = node_id
        label = str(node.get("label", "")).strip()
        if label:
            lookup[_normalize_key(label)] = node_id
    return lookup


def _resolve_node_id(
    row: dict[str, str],
    *,
    lookup: dict[str, str],
    id_fields: tuple[str, ...],
    name_fields: tuple[str, ...],
) -> str:
    for field in id_fields:
        value = row.get(field, "").strip()
        if value:
            return value
    for field in name_fields:
        value = row.get(field, "").strip()
        if not value:
            continue
        resolved = lookup.get(_normalize_key(value))
        if resolved:
            return resolved
    raise ValueError(f"unable to resolve node id from row: {row}")


def _split_tags(value: str) -> str:
    items = [part.strip() for chunk in value.replace(",", "|").split("|") for part in [chunk] if part.strip()]
    return "|".join(items)


def _build_price_range(row: dict[str, str]) -> str:
    if row.get("price_range", "").strip():
        return row["price_range"].strip()
    low = row.get("price_min", "").strip()
    high = row.get("price_max", "").strip()
    if low and high:
        return f"{low}-{high}"
    return low or high


def _as_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _normalize_sku_rows(raw_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        rows.append(
            {
                "sku_id": row.get("sku_id", "").strip() or row.get("sku_code", "").strip(),
                "brand": row.get("brand", "").strip() or row.get("brand_name", "").strip(),
                "series": row.get("series", "").strip() or row.get("series_name", "").strip(),
                "model": row.get("model", "").strip() or row.get("display_name", "").strip(),
                "positioning": row.get("positioning", "").strip(),
                "price_range": _build_price_range(row),
                "wheel_diameter": row.get("wheel_diameter", "").strip() or row.get("wheel_size", "").strip(),
                "frame_type": row.get("frame_type", "").strip() or row.get("frame_desc", "").strip(),
                "motor_type": row.get("motor_type", "").strip() or row.get("motor_desc", "").strip(),
                "battery_platform": row.get("battery_platform", "").strip() or row.get("battery_desc", "").strip(),
                "brake_config": row.get("brake_config", "").strip() or row.get("brake_desc", "").strip(),
                "target_audience": row.get("target_audience", "").strip() or row.get("audience", "").strip(),
                "style_tags": _split_tags(row.get("style_tags", "").strip()),
                "evidence_sources": _split_tags(row.get("evidence_sources", "").strip() or row.get("evidence_channels", "").strip()),
            }
        )
    return rows


def _normalize_edge_rows(raw_rows: list[dict[str, str]], *, lookup: dict[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        rows.append(
            {
                "source_id": _resolve_node_id(
                    row,
                    lookup=lookup,
                    id_fields=("source_id", "supplier_id"),
                    name_fields=("source_name", "supplier_name"),
                ),
                "target_id": _resolve_node_id(
                    row,
                    lookup=lookup,
                    id_fields=("target_id", "customer_id"),
                    name_fields=("target_name", "customer_name"),
                ),
                "edge_type": row.get("edge_type", "").strip() or row.get("relation_type", "").strip(),
                "valid_from": row.get("valid_from", "").strip() or row.get("observed_on", "").strip() or "2025-01-01",
                "confidence": row.get("confidence", "").strip() or row.get("confidence_score", "").strip(),
                "source": row.get("source", "").strip() or "supplier_export",
                "evidence": row.get("evidence", "").strip() or row.get("evidence_note", "").strip(),
            }
        )
    return rows


def _normalize_alias_rows(raw_rows: list[dict[str, str]], *, lookup: dict[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        node_id = _resolve_node_id(
            row,
            lookup=lookup,
            id_fields=("node_id",),
            name_fields=("target_name", "canonical_name"),
        )
        rows.append({"alias": row.get("alias", "").strip(), "node_id": node_id})
    return rows


def _normalize_image_rows(raw_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        rows.append(
            {
                "asset_id": row.get("asset_id", "").strip() or row.get("media_id", "").strip(),
                "brand": row.get("brand", "").strip() or row.get("brand_name", "").strip(),
                "product_line": row.get("product_line", "").strip() or row.get("line_name", "").strip(),
                "category": row.get("category", "").strip() or row.get("media_category", "").strip(),
                "source_url": row.get("source_url", "").strip() or row.get("origin_url", "").strip(),
                "local_path": row.get("local_path", "").strip() or row.get("relative_path", "").strip(),
                "acquisition_date": row.get("acquisition_date", "").strip() or row.get("captured_on", "").strip(),
                "is_official": _as_bool(row.get("is_official", "").strip() or row.get("official_flag", "").strip()),
                "quality_grade": row.get("quality_grade", "").strip() or row.get("quality", "").strip(),
                "visible_content": row.get("visible_content", "").strip() or row.get("visible_notes", "").strip(),
                "supports_conclusion": row.get("supports_conclusion", "").strip() or row.get("supports", "").strip(),
                "prohibits_conclusion": row.get("prohibits_conclusion", "").strip() or row.get("prohibits", "").strip(),
            }
        )
    return rows


def connect_two_wheeler_feeds(
    raw_dir: str | Path,
    bundle_dir: str | Path,
    *,
    run_id: str | None = None,
    delta_path: str | Path | None = None,
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
) -> dict[str, Any]:
    raw_dir = Path(raw_dir)
    bundle_dir = Path(bundle_dir)
    source_dir = Path(source_dir)
    lookup = _build_node_lookup(source_dir)
    used_inputs: list[str] = []
    written_outputs: list[str] = []
    counts = {
        "meta_patch_fields": 0,
        "image_assets": 0,
        "sku_records": 0,
        "graph_aliases": 0,
        "graph_edges": 0,
    }

    meta_path = raw_dir / RAW_EXPORT_FILES["meta_patch"]
    if meta_path.exists():
        meta_patch = json.loads(meta_path.read_text(encoding="utf-8"))
        counts["meta_patch_fields"] = len(meta_patch)
        used_inputs.append(str(meta_path))
        written_outputs.append(str(_write_json(bundle_dir / BUNDLE_FILES["meta_patch"], meta_patch)))

    sku_path = raw_dir / RAW_EXPORT_FILES["sku_backoffice"]
    if sku_path.exists():
        sku_rows = _normalize_sku_rows(_read_csv_rows(sku_path))
        counts["sku_records"] = len(sku_rows)
        used_inputs.append(str(sku_path))
        written_outputs.append(
            str(_write_csv_rows(bundle_dir / BUNDLE_FILES["sku_records"], fieldnames=SKU_FIELDS, rows=sku_rows))
        )

    supplier_path = raw_dir / RAW_EXPORT_FILES["supplier_observations"]
    if supplier_path.exists():
        edge_rows = _normalize_edge_rows(_read_csv_rows(supplier_path), lookup=lookup)
        counts["graph_edges"] = len(edge_rows)
        used_inputs.append(str(supplier_path))
        written_outputs.append(
            str(_write_csv_rows(bundle_dir / BUNDLE_FILES["graph_edges"], fieldnames=EDGE_FIELDS, rows=edge_rows))
        )

    alias_path = raw_dir / RAW_EXPORT_FILES["alias_map"]
    if alias_path.exists():
        alias_rows = _normalize_alias_rows(_read_csv_rows(alias_path), lookup=lookup)
        counts["graph_aliases"] = len(alias_rows)
        used_inputs.append(str(alias_path))
        written_outputs.append(
            str(_write_csv_rows(bundle_dir / BUNDLE_FILES["graph_aliases"], fieldnames=ALIAS_FIELDS, rows=alias_rows))
        )

    field_media_path = raw_dir / RAW_EXPORT_FILES["field_media"]
    if field_media_path.exists():
        image_rows = _normalize_image_rows(_read_csv_rows(field_media_path))
        counts["image_assets"] = len(image_rows)
        used_inputs.append(str(field_media_path))
        written_outputs.append(str(_write_json(bundle_dir / BUNDLE_FILES["image_assets"], image_rows)))

    delta_written = None
    if delta_path is not None:
        payload = collect_two_wheeler_source_delta(bundle_dir, run_id=run_id)
        delta_written = str(write_two_wheeler_source_delta(payload, delta_path))

    return {
        "run_id": run_id or Path(raw_dir).name,
        "raw_dir": str(raw_dir),
        "bundle_dir": str(bundle_dir),
        "source_dir": str(source_dir),
        "delta_path": delta_written or "",
        "used_inputs": used_inputs,
        "written_outputs": written_outputs,
        "counts": counts,
    }
