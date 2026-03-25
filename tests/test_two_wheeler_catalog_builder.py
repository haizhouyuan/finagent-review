from __future__ import annotations

import json
import subprocess
import sys

from finagent.two_wheeler_catalog import (
    DEFAULT_OUTPUT_PATH,
    DEFAULT_SOURCE_DIR,
    build_two_wheeler_catalog,
)
from finagent.two_wheeler_refresh import load_catalog


def test_build_two_wheeler_catalog_matches_repo_catalog():
    assert build_two_wheeler_catalog(DEFAULT_SOURCE_DIR) == load_catalog(DEFAULT_OUTPUT_PATH)


def test_build_two_wheeler_catalog_dedupes_incremental_rows(tmp_path):
    source_dir = tmp_path / "sources"
    source_dir.mkdir(parents=True)

    (source_dir / "meta.json").write_text(
        json.dumps(
            {
                "goal": "增量刷新",
                "context": "两轮车",
                "triples": [],
                "evidence_refs": [],
                "report_md": "增量 catalog",
                "confidence": 0.9,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (source_dir / "image_assets.json").write_text(
        json.dumps(
            [
                {"asset_id": "img-1", "brand": "九号", "quality_grade": "medium"},
                {"asset_id": "img-1", "brand": "九号", "quality_grade": "high"},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (source_dir / "sku_catalog.json").write_text(
        json.dumps(
            [
                {"sku_id": "sku-1", "model": "旧款", "price_range": "4999-5199"},
                {"sku_id": "sku-1", "model": "新款", "price_range": "5299-5499"},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (source_dir / "graph_observations.json").write_text(
        json.dumps(
            {
                "aliases": [
                    {"alias": "Ninebot Fz3", "node_id": "ninebot_fz_old"},
                    {"alias": "Ninebot Fz3", "node_id": "ninebot_fz"},
                ],
                "edges": [
                    {
                        "source_id": "jinggu",
                        "target_id": "ninebot",
                        "edge_type": "supplies_core_part_to",
                        "valid_from": "2025-01-01",
                        "confidence": 0.82,
                        "source": "batch-1",
                    },
                    {
                        "source_id": "jinggu",
                        "target_id": "ninebot",
                        "edge_type": "supplies_core_part_to",
                        "valid_from": "2025-01-01",
                        "confidence": 0.95,
                        "source": "batch-2",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    catalog = build_two_wheeler_catalog(source_dir)
    assert catalog["image_assets"] == [{"asset_id": "img-1", "brand": "九号", "quality_grade": "high"}]
    assert catalog["sku_records"] == [{"sku_id": "sku-1", "model": "新款", "price_range": "5299-5499"}]
    assert catalog["graph"]["aliases"] == {"Ninebot Fz3": "ninebot_fz"}
    assert catalog["graph"]["edges"] == [
        {
            "source_id": "jinggu",
            "target_id": "ninebot",
            "edge_type": "supplies_core_part_to",
            "valid_from": "2025-01-01",
            "confidence": 0.95,
            "source": "batch-2",
        }
    ]


def test_catalog_builder_cli_check_uses_repo_defaults():
    result = subprocess.run(
        [sys.executable, "scripts/build_two_wheeler_catalog.py", "--check"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    assert "Catalog is up to date" in result.stdout
