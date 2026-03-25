from __future__ import annotations

import json
import subprocess
import sys

from finagent.two_wheeler_catalog import (
    apply_two_wheeler_source_delta,
    build_two_wheeler_catalog,
    load_two_wheeler_sources,
    write_source_delta_changelog,
    write_two_wheeler_sources,
)


def _base_sources() -> dict:
    return {
        "meta": {
            "goal": "两轮车增量观察",
            "context": "两轮车",
            "triples": [],
            "evidence_refs": [],
            "report_md": "base",
            "confidence": 0.8,
        },
        "image_assets": [
            {
                "asset_id": "img-ninebot-fz",
                "brand": "九号",
                "quality_grade": "medium",
                "local_path": "data/competitive_assets/photos/fz.jpg",
            }
        ],
        "sku_records": [
            {
                "sku_id": "sku-ninebot-fz3",
                "brand": "九号",
                "model": "Fz3 110",
                "price_range": "6299-7599",
            }
        ],
        "graph": {
            "aliases": {"Ninebot Fz3": "ninebot_fz"},
            "edges": [
                {
                    "source_id": "jinggu",
                    "target_id": "ninebot",
                    "edge_type": "supplies_core_part_to",
                    "valid_from": "2025-01-01",
                    "confidence": 0.92,
                    "source": "baseline",
                }
            ],
            "nodes": [],
        },
    }


def test_apply_two_wheeler_source_delta_updates_sources_and_catalog(tmp_path):
    source_dir = tmp_path / "sources"
    catalog_path = tmp_path / "catalog.json"
    write_two_wheeler_sources(_base_sources(), source_dir)

    delta = {
        "run_id": "supplier-refresh-01",
        "meta_patch": {
            "confidence": 0.86,
            "report_md": "updated",
        },
        "image_assets": [
            {
                "asset_id": "img-ninebot-fz-detail",
                "brand": "九号",
                "quality_grade": "high",
                "local_path": "data/competitive_assets/photos/fz-detail.jpg",
            }
        ],
        "sku_records": [
            {
                "sku_id": "sku-ninebot-fz3",
                "brand": "九号",
                "model": "Fz3 120",
                "price_range": "6499-7799",
            }
        ],
        "graph": {
            "aliases": [
                {"alias": "九号Fz3 120", "node_id": "ninebot_fz"},
            ],
            "edges": [
                {
                    "source_id": "jinggu",
                    "target_id": "ninebot",
                    "edge_type": "supplies_core_part_to",
                    "valid_from": "2025-01-01",
                    "confidence": 0.96,
                    "source": "supplier-refresh",
                    "evidence": "最新铝轮毂配套观察",
                }
            ],
        },
    }

    summary = apply_two_wheeler_source_delta(
        delta,
        source_dir=source_dir,
        catalog_path=catalog_path,
    )

    assert summary["meta"]["updated_fields"] == ["confidence", "report_md"]
    assert summary["image_assets"]["created"] == [{"asset_id": "img-ninebot-fz-detail"}]
    assert summary["sku_records"]["updated"] == [
        {"sku_id": "sku-ninebot-fz3", "fields": ["model", "price_range"]}
    ]
    assert summary["graph"]["aliases"]["created"] == [{"alias": "九号Fz3 120"}]
    assert summary["graph"]["edges"]["updated"] == [
        {
            "edge": "jinggu->ninebot:supplies_core_part_to",
            "fields": ["confidence", "evidence", "source"],
        }
    ]

    sources = load_two_wheeler_sources(source_dir)
    assert sources["meta"]["confidence"] == 0.86
    assert sources["sku_records"][0]["model"] == "Fz3 120"
    assert sources["graph"]["aliases"]["九号Fz3 120"] == "ninebot_fz"
    assert build_two_wheeler_catalog(source_dir) == json.loads(catalog_path.read_text(encoding="utf-8"))

    changelog_path = write_source_delta_changelog(summary, tmp_path / "source-delta.json")
    payload = json.loads(changelog_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "supplier-refresh-01"
    assert payload["catalog_counts"]["graph_edges"] == 1


def test_import_two_wheeler_source_delta_cli_dry_run_preserves_files(tmp_path):
    source_dir = tmp_path / "sources"
    catalog_path = tmp_path / "catalog.json"
    delta_path = tmp_path / "delta.json"
    changelog_path = tmp_path / "delta-changelog.json"

    write_two_wheeler_sources(_base_sources(), source_dir)
    catalog_path.write_text(
        json.dumps(build_two_wheeler_catalog(source_dir), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    before_sources = load_two_wheeler_sources(source_dir)
    before_catalog = catalog_path.read_text(encoding="utf-8")

    delta_path.write_text(
        json.dumps(
            {
                "run_id": "dry-run-delta",
                "meta_patch": {"confidence": 0.9},
                "sku_records": [
                    {
                        "sku_id": "sku-ninebot-fz3",
                        "brand": "九号",
                        "model": "Fz3 Max",
                        "price_range": "6999-7299",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/import_two_wheeler_source_delta.py",
            str(delta_path),
            "--source-dir",
            str(source_dir),
            "--catalog-path",
            str(catalog_path),
            "--changelog-path",
            str(changelog_path),
            "--dry-run",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert "Dry run: True" in result.stdout
    assert load_two_wheeler_sources(source_dir) == before_sources
    assert catalog_path.read_text(encoding="utf-8") == before_catalog

    changelog = json.loads(changelog_path.read_text(encoding="utf-8"))
    assert changelog["dry_run"] is True
    assert changelog["sku_records"]["updated"] == [
        {"sku_id": "sku-ninebot-fz3", "fields": ["model", "price_range"]}
    ]
