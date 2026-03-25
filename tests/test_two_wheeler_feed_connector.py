from __future__ import annotations

import json
import subprocess
import sys

from finagent.two_wheeler_catalog import write_two_wheeler_sources
from finagent.two_wheeler_delta_collector import collect_two_wheeler_source_delta
from finagent.two_wheeler_feed_connector import connect_two_wheeler_feeds


def _write_raw_exports(raw_dir):
    raw_dir.mkdir(parents=True)
    (raw_dir / "sku_backoffice.csv").write_text(
        "sku_code,brand_name,series_name,display_name,positioning,price_min,price_max,wheel_size,frame_desc,motor_desc,battery_desc,brake_desc,audience,style_tags,evidence_channels\n"
        "sku-ninebot-fz3-120,九号,Fz,Fz3 120,中高端,6499,7799,14寸,双管一体,轮毂电机 1300W,72V32Ah 锂电,前碟后碟,年轻男性/通勤,运动|智能,官网|门店\n",
        encoding="utf-8",
    )
    (raw_dir / "supplier_observations.csv").write_text(
        "supplier_name,customer_name,relation_type,observed_on,confidence_score,evidence_note,source\n"
        "金谷,九号,supplies_core_part_to,2025-04-01,0.96,4月轮毂配套观察,field_export\n",
        encoding="utf-8",
    )
    (raw_dir / "alias_map.csv").write_text(
        "alias,canonical_name\n九号Fz3 120,Fz系列\n",
        encoding="utf-8",
    )
    (raw_dir / "field_media.csv").write_text(
        "media_id,brand_name,line_name,media_category,origin_url,relative_path,captured_on,official_flag,quality,visible_notes,supports,prohibits\n"
        "img-ninebot-fz3-120,九号,Fz系列,field_research,https://example.com/fz3-120,data/competitive_assets/photos/fz3-120.jpg,2026-03-24,false,high,门店实拍,Fz3 120 新款轮毂细节,\n",
        encoding="utf-8",
    )
    (raw_dir / "meta_patch.json").write_text(
        json.dumps({"confidence": 0.87, "report_md": "connector-run"}, ensure_ascii=False),
        encoding="utf-8",
    )


def _write_source_graph(source_dir, *, aliases=None, nodes=None):
    write_two_wheeler_sources(
        {
            "meta": {},
            "image_assets": [],
            "sku_records": [],
            "graph": {
                "aliases": aliases or {},
                "edges": [],
                "nodes": nodes or [],
            },
        },
        source_dir,
    )


def test_connect_two_wheeler_feeds_builds_bundle_and_collectable_delta(tmp_path):
    raw_dir = tmp_path / "raw"
    bundle_dir = tmp_path / "bundle"
    _write_raw_exports(raw_dir)

    summary = connect_two_wheeler_feeds(raw_dir, bundle_dir, run_id="feed-run")
    assert summary["run_id"] == "feed-run"
    assert summary["counts"] == {
        "meta_patch_fields": 2,
        "image_assets": 1,
        "sku_records": 1,
        "graph_aliases": 1,
        "graph_edges": 1,
    }

    payload = collect_two_wheeler_source_delta(bundle_dir, run_id="feed-run")
    assert payload["sku_records"] == [
        {
            "sku_id": "sku-ninebot-fz3-120",
            "brand": "九号",
            "series": "Fz",
            "model": "Fz3 120",
            "positioning": "中高端",
            "price_range": "6499-7799",
            "wheel_diameter": "14寸",
            "frame_type": "双管一体",
            "motor_type": "轮毂电机 1300W",
            "battery_platform": "72V32Ah 锂电",
            "brake_config": "前碟后碟",
            "target_audience": "年轻男性/通勤",
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
            "valid_from": "2025-04-01",
            "confidence": 0.96,
            "source": "field_export",
            "evidence": "4月轮毂配套观察",
        }
    ]
    assert payload["meta_patch"] == {"confidence": 0.87, "report_md": "connector-run"}
    assert payload["image_assets"][0]["is_official"] is False


def test_connect_two_wheeler_feeds_cli_can_write_delta(tmp_path):
    raw_dir = tmp_path / "raw"
    bundle_dir = tmp_path / "bundle"
    delta_path = tmp_path / "delta.json"
    _write_raw_exports(raw_dir)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/connect_two_wheeler_feeds.py",
            str(raw_dir),
            str(bundle_dir),
            "--run-id",
            "feed-cli",
            "--delta-path",
            str(delta_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert "Delta:" in result.stdout

    payload = json.loads(delta_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "feed-cli"
    assert payload["collector_summary"]["counts"]["sku_records"] == 1


def test_connect_two_wheeler_feeds_prefers_current_source_graph_lookup(tmp_path):
    raw_dir = tmp_path / "raw"
    bundle_dir = tmp_path / "bundle"
    source_dir = tmp_path / "sources"
    raw_dir.mkdir(parents=True)
    _write_source_graph(
        source_dir,
        aliases={
            "未来供应商": "future_supplier",
            "星云S1": "future_series",
        },
        nodes=[
            {"node_id": "future_supplier", "label": "未来轮毂厂", "node_type": "company"},
            {"node_id": "future_series", "label": "星云系列", "node_type": "project"},
        ],
    )
    (raw_dir / "supplier_observations.csv").write_text(
        "supplier_name,customer_name,relation_type,observed_on,confidence_score,evidence_note,source\n"
        "未来供应商,星云S1,supplies_core_part_to,2025-05-01,0.93,新业务节点观察,partner_feed\n",
        encoding="utf-8",
    )
    (raw_dir / "alias_map.csv").write_text(
        "alias,canonical_name\n星云S1 Pro,星云系列\n",
        encoding="utf-8",
    )

    summary = connect_two_wheeler_feeds(
        raw_dir,
        bundle_dir,
        run_id="source-growth",
        source_dir=source_dir,
    )
    payload = collect_two_wheeler_source_delta(bundle_dir, run_id="source-growth")

    assert summary["source_dir"] == str(source_dir)
    assert payload["graph"]["edges"] == [
        {
            "source_id": "future_supplier",
            "target_id": "future_series",
            "edge_type": "supplies_core_part_to",
            "valid_from": "2025-05-01",
            "confidence": 0.93,
            "source": "partner_feed",
            "evidence": "新业务节点观察",
        }
    ]
    assert payload["graph"]["aliases"] == {"星云S1 Pro": "future_series"}
