from __future__ import annotations

import json

from finagent.db import connect, init_db, select_one
from finagent.graph_v2.ontology import EdgeType
from finagent.graph_v2.store import GraphStore
from finagent.research_contracts import ImageAssetRef, SkuRecord
from finagent.two_wheeler_refresh import (
    REPO_ROOT,
    VALID_FROM,
    apply_competitive_refresh,
    apply_graph_refresh,
    build_research_package,
    default_graph_edges,
    refresh_two_wheeler_data,
    write_refresh_changelog,
)


def test_competitive_refresh_is_incremental(tmp_path):
    conn = connect(tmp_path / "state.sqlite")
    init_db(conn)
    try:
        package = build_research_package(run_id="run-refresh-1", repo_root=REPO_ROOT)
        first = apply_competitive_refresh(conn, package)

        assert len(first["asset_ledger"]["created"]) == len(package.image_assets)
        assert len(first["sku_catalog"]["created"]) == len(package.sku_records)

        second = apply_competitive_refresh(
            conn,
            build_research_package(run_id="run-refresh-2", repo_root=REPO_ROOT),
        )
        assert second["applied_actions"] == 0
        assert len(second["asset_ledger"]["unchanged"]) == len(package.image_assets)
        assert len(second["sku_catalog"]["unchanged"]) == len(package.sku_records)

        asset0 = ImageAssetRef(**{**package.image_assets[0].to_dict(), "quality_grade": "high"})
        sku0 = SkuRecord(**{**package.sku_records[0].to_dict(), "price_range": "6399-7699"})
        updated_assets = [asset0, *package.image_assets[1:]]
        updated_skus = [sku0, *package.sku_records[1:]]
        third = apply_competitive_refresh(
            conn,
            build_research_package(
                run_id="run-refresh-3",
                repo_root=REPO_ROOT,
                image_assets=updated_assets,
                sku_records=updated_skus,
            ),
        )

        assert third["row_counts"]["asset_ledger"] == len(package.image_assets)
        assert third["row_counts"]["sku_catalog"] == len(package.sku_records)
        assert third["asset_ledger"]["updated"] == [
            {"asset_id": asset0.asset_id, "fields": ["quality_grade"]}
        ]
        assert third["sku_catalog"]["updated"] == [
            {"sku_id": sku0.sku_id, "fields": ["price_range"]}
        ]

        asset_row = select_one(conn, "SELECT * FROM asset_ledger WHERE asset_id = ?", (asset0.asset_id,))
        sku_row = select_one(conn, "SELECT * FROM sku_catalog WHERE sku_id = ?", (sku0.sku_id,))
        assert asset_row["quality_grade"] == "high"
        assert asset_row["last_run_id"] == "run-refresh-3"
        assert sku_row["price_range"] == "6399-7699"
        assert sku_row["last_run_id"] == "run-refresh-3"
    finally:
        conn.close()


def test_graph_refresh_is_incremental(tmp_path):
    store = GraphStore(tmp_path / "graph.sqlite")
    try:
        first = apply_graph_refresh(store)
        assert len(first["nodes"]["created"]) >= 30
        assert len(first["edges"]["created"]) >= 60
        assert first["orphans_after"] == []

        second = apply_graph_refresh(store)
        assert second["nodes"]["created"] == []
        assert second["nodes"]["updated"] == []
        assert second["edges"]["created"] == []
        assert second["edges"]["updated"] == []

        upgraded_edge = {
            "source_id": "jinggu",
            "target_id": "ninebot",
            "edge_type": EdgeType.SUPPLIES_CORE_PART_TO,
            "valid_from": VALID_FROM,
            "confidence": 0.97,
            "source": "two_wheeler_refresh",
            "evidence": "refresh-upgrade",
        }
        third = apply_graph_refresh(
            store,
            edges=[*default_graph_edges(), upgraded_edge],
            aliases={"Ninebot Fz3 Max": "ninebot_fz"},
        )
        assert "Ninebot Fz3 Max" in third["aliases"]["created"]
        assert third["orphans_after"] == []
        assert any(
            item["edge"] == "jinggu->ninebot:supplies_core_part_to"
            and item["confidence"] == [0.92, 0.97]
            for item in third["edges"]["updated"]
        )
        assert store.resolve_alias("Ninebot Fz3 Max") == "ninebot_fz"

        edge = max(
            store.edges_between("jinggu", "ninebot", edge_type=EdgeType.SUPPLIES_CORE_PART_TO),
            key=lambda row: float(row["confidence"]),
        )
        assert float(edge["confidence"]) == 0.97
    finally:
        store.close()


def test_unified_refresh_writes_changelog(tmp_path):
    summary = refresh_two_wheeler_data(
        state_db_path=tmp_path / "state.sqlite",
        graph_db_path=tmp_path / "graph.sqlite",
        run_id="run-refresh-full",
        repo_root=REPO_ROOT,
    )
    changelog_path = write_refresh_changelog(summary, tmp_path / "refresh.json")
    payload = json.loads(changelog_path.read_text(encoding="utf-8"))

    assert payload["run_id"] == "run-refresh-full"
    assert payload["state"]["row_counts"]["asset_ledger"] == 4
    assert payload["state"]["row_counts"]["sku_catalog"] == 7
    assert payload["graph"]["orphans_after"] == []
    assert len(payload["dedup_rules"]) >= 5


def test_refresh_can_load_external_catalog(tmp_path):
    repo_root = tmp_path / "repo"
    asset_path = repo_root / "photos" / "custom.jpg"
    asset_path.parent.mkdir(parents=True, exist_ok=True)
    asset_path.write_bytes(b"jpg")

    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "goal": "外部 catalog 刷新",
                "context": "两轮车",
                "image_assets": [
                    {
                        "asset_id": "img-custom-ninebot",
                        "brand": "九号",
                        "product_line": "Fz3",
                        "category": "exterior",
                        "source_url": "https://example.com/fz3",
                        "local_path": "photos/custom.jpg",
                        "acquisition_date": "2026-03-24",
                        "is_official": True,
                        "quality_grade": "high",
                        "visible_content": "自定义图片",
                        "supports_conclusion": "外部 catalog 覆盖",
                        "prohibits_conclusion": ""
                    }
                ],
                "sku_records": [
                    {
                        "sku_id": "sku-custom-ninebot",
                        "brand": "九号",
                        "series": "Fz",
                        "model": "Fz3 Custom",
                        "positioning": "中高端",
                        "price_range": "6999-7299",
                        "wheel_diameter": "14寸",
                        "frame_type": "双管一体",
                        "motor_type": "轮毂电机 1300W",
                        "battery_platform": "72V32Ah 锂电",
                        "brake_config": "前碟后碟",
                        "target_audience": "高端通勤",
                        "style_tags": ["运动", "智能"],
                        "evidence_sources": ["官网"]
                    }
                ],
                "graph": {
                    "aliases": {
                        "自定义九号Fz3": "ninebot_fz"
                    },
                    "edges": [
                        {
                            "source_id": "jinggu",
                            "target_id": "ninebot",
                            "edge_type": "supplies_core_part_to",
                            "valid_from": "2025-01-01",
                            "confidence": 0.95,
                            "source": "custom_catalog",
                            "evidence": "外部 catalog 供应链观察"
                        }
                    ]
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    summary = refresh_two_wheeler_data(
        state_db_path=tmp_path / "state.sqlite",
        graph_db_path=tmp_path / "graph.sqlite",
        run_id="run-refresh-catalog",
        repo_root=repo_root,
        catalog_path=catalog_path,
    )
    assert summary["catalog_path"] == str(catalog_path)
    assert summary["state"]["row_counts"]["asset_ledger"] == 1
    assert summary["state"]["row_counts"]["sku_catalog"] == 1

    store = GraphStore(tmp_path / "graph.sqlite")
    try:
        assert store.resolve_alias("自定义九号Fz3") == "ninebot_fz"
        edge = max(
            store.edges_between("jinggu", "ninebot", edge_type=EdgeType.SUPPLIES_CORE_PART_TO),
            key=lambda row: float(row["confidence"]),
        )
        assert float(edge["confidence"]) == 0.95
        assert edge["source"] == "custom_catalog"
    finally:
        store.close()
