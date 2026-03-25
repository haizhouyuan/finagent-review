from __future__ import annotations

import json
import subprocess
import sys

from finagent.two_wheeler_catalog import apply_two_wheeler_source_delta, load_two_wheeler_sources
from finagent.two_wheeler_delta_collector import collect_two_wheeler_source_delta


def test_collect_two_wheeler_source_delta_from_csv_and_json(tmp_path):
    input_dir = tmp_path / "collector"
    input_dir.mkdir(parents=True)

    (input_dir / "meta_patch.json").write_text(
        json.dumps({"confidence": 0.91, "report_md": "collected"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (input_dir / "sku_records.csv").write_text(
        "sku_id,brand,model,price_range,style_tags,evidence_sources\n"
        "sku-ninebot-fz3,九号,Fz3 120,6499-7799,运动|智能,官网|门店\n",
        encoding="utf-8",
    )
    (input_dir / "graph_aliases.csv").write_text(
        "alias,node_id\n九号Fz3 120,ninebot_fz\n",
        encoding="utf-8",
    )
    (input_dir / "graph_edges.csv").write_text(
        "source_id,target_id,edge_type,valid_from,confidence,source,evidence\n"
        "jinggu,ninebot,supplies_core_part_to,2025-01-01,0.97,collector,最新供应链观察\n",
        encoding="utf-8",
    )
    (input_dir / "image_assets.json").write_text(
        json.dumps(
            [
                {
                    "asset_id": "img-ninebot-fz3-120",
                    "brand": "九号",
                    "quality_grade": "high",
                    "local_path": "data/competitive_assets/photos/fz3-120.jpg",
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = collect_two_wheeler_source_delta(input_dir, run_id="collector-run")

    assert payload["run_id"] == "collector-run"
    assert payload["meta_patch"] == {"confidence": 0.91, "report_md": "collected"}
    assert payload["image_assets"][0]["asset_id"] == "img-ninebot-fz3-120"
    assert payload["sku_records"] == [
        {
            "sku_id": "sku-ninebot-fz3",
            "brand": "九号",
            "model": "Fz3 120",
            "price_range": "6499-7799",
            "style_tags": ["运动", "智能"],
            "evidence_sources": ["官网", "门店"],
        }
    ]
    assert payload["graph"]["aliases"] == {"九号Fz3 120": "ninebot_fz"}
    assert payload["graph"]["edges"] == [
        {
            "source_id": "jinggu",
            "target_id": "ninebot",
            "edge_type": "supplies_core_part_to",
            "valid_from": "2025-01-01",
            "confidence": 0.97,
            "source": "collector",
            "evidence": "最新供应链观察",
        }
    ]
    assert len(payload["collector_summary"]["used_files"]) == 5


def test_collect_and_import_two_wheeler_source_delta_chain(tmp_path):
    input_dir = tmp_path / "collector"
    source_dir = tmp_path / "sources"
    catalog_path = tmp_path / "catalog.json"
    delta_path = tmp_path / "delta.json"
    input_dir.mkdir(parents=True)

    (input_dir / "sku_records.csv").write_text(
        "sku_id,brand,model,price_range\nsku-ninebot-fz3,九号,Fz3 Max,6999-7299\n",
        encoding="utf-8",
    )
    (input_dir / "graph_aliases.csv").write_text(
        "alias,node_id\n九号Fz3 Max,ninebot_fz\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/collect_two_wheeler_source_delta.py",
            str(input_dir),
            str(delta_path),
            "--run-id",
            "collector-chain",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    assert "SKUs: 1" in result.stdout

    payload = json.loads(delta_path.read_text(encoding="utf-8"))
    summary = apply_two_wheeler_source_delta(
        payload,
        source_dir=source_dir,
        catalog_path=catalog_path,
    )
    sources = load_two_wheeler_sources(source_dir)

    assert summary["run_id"] == "collector-chain"
    assert sources["sku_records"][0]["model"] == "Fz3 Max"
    assert sources["graph"]["aliases"]["九号Fz3 Max"] == "ninebot_fz"
