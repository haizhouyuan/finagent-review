"""E2E test: shared package pipeline.

ResearchPackage with image_assets + sku_records → plan_writeback → apply_writeback.
Covers CREATE, UPDATE, round-trip, and auto-planning.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from finagent.db import SCHEMA_SQL, select_one, list_rows, init_db
from finagent.research_contracts import (
    ResearchPackage, EvidenceRef, ImageAssetRef, SkuRecord,
    WritebackAction, WritebackTarget, WritebackOp,
)
from finagent.writeback_engine import plan_writeback, apply_writeback


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def _make_package_with_competitive_data(**overrides) -> ResearchPackage:
    defaults = dict(
        run_id="run-comp-e2e-01",
        goal="两轮车竞品结构分析",
        context="两轮车",
        triples=[
            {"subject": "九号", "predicate": "生产", "object": "电动两轮车"},
        ],
        evidence_refs=[
            EvidenceRef(evidence_id=1, query="九号Fz3", char_count=300),
        ],
        report_md="# 竞品分析\n\n九号Fz3采用双管车架。",
        confidence=0.75,
        image_assets=[
            ImageAssetRef(
                asset_id="img-fz3-side",
                brand="九号",
                product_line="Fz3 110",
                category="exterior",
                source_url="https://www.ninebot.com/fz3.jpg",
                local_path="photos/01_九号_Fz3.jpg",
                acquisition_date="2026-03-22",
                is_official=True,
                quality_grade="high",
                visible_content="整车侧面45度角",
                supports_conclusion="双管车架结构确认",
                prohibits_conclusion="底盘细节不可判定",
            ),
            ImageAssetRef(
                asset_id="img-fz3-frame",
                brand="九号",
                product_line="Fz3 110",
                category="structure",
                source_url="https://www.ninebot.com/fz3-frame.jpg",
                local_path="photos/02_九号_Fz3_车架.jpg",
                is_official=True,
                quality_grade="high",
                visible_content="双管车架焊接结构",
            ),
        ],
        sku_records=[
            SkuRecord(
                sku_id="sku-fz3-110",
                brand="九号",
                series="Fz",
                model="Fz3 110",
                positioning="中高端",
                price_range="6000-8000",
                wheel_diameter="14寸",
                frame_type="双管",
                motor_type="轮毂电机",
                battery_platform="72V30Ah",
                brake_config="前碟后鼓",
                target_audience="年轻男性",
                style_tags=("运动", "机甲"),
                evidence_sources=("官网", "实地"),
            ),
        ],
    )
    defaults.update(overrides)
    return ResearchPackage(**defaults)


# ── E2E: Package → Plan → Apply ─────────────────────────────────────

class TestSharedPipelineE2E:
    def test_plan_generates_competitive_actions(self):
        """plan_writeback should auto-generate asset + SKU actions from package."""
        conn = _make_conn()
        pkg = _make_package_with_competitive_data()
        actions = plan_writeback(pkg, conn)

        asset_actions = [a for a in actions if a.target_type == WritebackTarget.ASSET_LEDGER.value]
        sku_actions = [a for a in actions if a.target_type == WritebackTarget.SKU_CATALOG.value]

        assert len(asset_actions) == 2  # 2 images
        assert len(sku_actions) == 1    # 1 SKU
        # All CREATE on first run
        assert all(a.op == WritebackOp.CREATE.value for a in asset_actions)
        assert all(a.op == WritebackOp.CREATE.value for a in sku_actions)

    def test_full_pipeline_create(self):
        """Full pipeline: package → plan → apply → verify DB."""
        conn = _make_conn()
        pkg = _make_package_with_competitive_data()

        actions = plan_writeback(pkg, conn)
        apply_writeback(actions, conn)

        # Verify asset_ledger
        assets = list_rows(conn, "SELECT * FROM asset_ledger ORDER BY asset_id")
        assert len(assets) == 2
        side = [a for a in assets if a["asset_id"] == "img-fz3-side"][0]
        assert side["brand"] == "九号"
        assert side["product_line"] == "Fz3 110"
        assert side["is_official"] == 1
        assert side["quality_grade"] == "high"
        assert side["run_id"] == "run-comp-e2e-01"

        # Verify sku_catalog
        skus = list_rows(conn, "SELECT * FROM sku_catalog")
        assert len(skus) == 1
        sku = skus[0]
        assert sku["brand"] == "九号"
        assert sku["frame_type"] == "双管"
        assert json.loads(sku["style_tags_json"]) == ["运动", "机甲"]
        assert json.loads(sku["evidence_sources_json"]) == ["官网", "实地"]

    def test_pipeline_update_on_second_run(self):
        """Second run with same IDs should plan UPDATE, not CREATE."""
        conn = _make_conn()
        pkg = _make_package_with_competitive_data()

        # First run: CREATE
        actions1 = plan_writeback(pkg, conn)
        apply_writeback(actions1, conn)

        # Second run: should be UPDATE
        pkg2 = _make_package_with_competitive_data(
            run_id="run-comp-e2e-02",
            image_assets=[
                ImageAssetRef(
                    asset_id="img-fz3-side",  # same ID
                    brand="九号",
                    product_line="Fz3 110",
                    category="exterior",
                    quality_grade="medium",  # changed
                    visible_content="整车侧面（补充：可见前轮细节）",
                ),
            ],
            sku_records=[
                SkuRecord(
                    sku_id="sku-fz3-110",  # same ID
                    brand="九号",
                    series="Fz",
                    model="Fz3 110",
                    price_range="5800-7500",  # updated
                    frame_type="双管",
                    style_tags=("运动", "机甲", "通勤升级"),  # updated
                ),
            ],
        )
        actions2 = plan_writeback(pkg2, conn)

        asset_actions = [a for a in actions2 if a.target_type == WritebackTarget.ASSET_LEDGER.value]
        sku_actions = [a for a in actions2 if a.target_type == WritebackTarget.SKU_CATALOG.value]
        assert len(asset_actions) == 1
        assert asset_actions[0].op == WritebackOp.UPDATE.value
        assert len(sku_actions) == 1
        assert sku_actions[0].op == WritebackOp.UPDATE.value

        # Apply UPDATE
        apply_writeback(actions2, conn)

        # Verify asset was updated
        asset = select_one(conn, "SELECT * FROM asset_ledger WHERE asset_id = ?", ("img-fz3-side",))
        assert asset["quality_grade"] == "medium"
        assert "前轮细节" in asset["visible_content"]

        # Verify SKU was updated
        sku = select_one(conn, "SELECT * FROM sku_catalog WHERE sku_id = ?", ("sku-fz3-110",))
        assert sku["price_range"] == "5800-7500"
        assert json.loads(sku["style_tags_json"]) == ["运动", "机甲", "通勤升级"]

    def test_package_round_trip_with_competitive_data(self):
        """ResearchPackage with assets/SKUs survives to_dict → from_dict."""
        pkg = _make_package_with_competitive_data()
        d = pkg.to_dict()
        restored = ResearchPackage.from_dict(d)

        assert len(restored.image_assets) == 2
        assert isinstance(restored.image_assets[0], ImageAssetRef)
        assert restored.image_assets[0].brand == "九号"
        assert restored.image_assets[0].is_official is True

        assert len(restored.sku_records) == 1
        assert isinstance(restored.sku_records[0], SkuRecord)
        assert restored.sku_records[0].frame_type == "双管"
        assert restored.sku_records[0].style_tags == ("运动", "机甲")

    def test_mixed_thesis_and_competitive(self):
        """Package with both thesis goal and competitive data generates all action types."""
        conn = _make_conn()
        pkg = _make_package_with_competitive_data()
        actions = plan_writeback(pkg, conn)

        types = {a.target_type for a in actions}
        # Should have thesis (CREATE), source, and competitive targets
        assert WritebackTarget.THESIS.value in types
        assert WritebackTarget.SOURCE.value in types
        assert WritebackTarget.ASSET_LEDGER.value in types
        assert WritebackTarget.SKU_CATALOG.value in types

    def test_competitive_only_mode(self):
        """target_families={'competitive'} suppresses thesis/source planning."""
        conn = _make_conn()
        pkg = _make_package_with_competitive_data()
        actions = plan_writeback(pkg, conn, target_families={"competitive"})

        types = {a.target_type for a in actions}
        assert WritebackTarget.THESIS.value not in types
        assert WritebackTarget.SOURCE.value not in types
        assert WritebackTarget.WATCH_ITEM.value not in types
        # Only competitive targets
        assert WritebackTarget.ASSET_LEDGER.value in types
        assert WritebackTarget.SKU_CATALOG.value in types

    def test_thesis_only_mode(self):
        """target_families={'thesis'} suppresses competitive planning."""
        conn = _make_conn()
        pkg = _make_package_with_competitive_data()
        actions = plan_writeback(pkg, conn, target_families={"thesis"})

        types = {a.target_type for a in actions}
        assert WritebackTarget.THESIS.value in types
        assert WritebackTarget.SOURCE.value in types
        assert WritebackTarget.ASSET_LEDGER.value not in types
        assert WritebackTarget.SKU_CATALOG.value not in types

    def test_update_provenance_tracking(self):
        """UPDATE writes updated_at and last_run_id for provenance."""
        conn = _make_conn()
        pkg = _make_package_with_competitive_data()

        # First run: CREATE
        actions1 = plan_writeback(pkg, conn)
        apply_writeback(actions1, conn)

        # Verify no updated_at / last_run_id yet
        asset = select_one(conn, "SELECT * FROM asset_ledger WHERE asset_id = ?", ("img-fz3-side",))
        assert asset["updated_at"] is None
        assert asset["last_run_id"] is None

        # Second run: UPDATE
        pkg2 = _make_package_with_competitive_data(
            run_id="run-comp-e2e-02",
            image_assets=[
                ImageAssetRef(
                    asset_id="img-fz3-side",
                    brand="九号",
                    product_line="Fz3 110",
                    quality_grade="medium",
                    visible_content="补充信息",
                ),
            ],
            sku_records=[
                SkuRecord(
                    sku_id="sku-fz3-110",
                    brand="九号",
                    series="Fz",
                    model="Fz3 110",
                    price_range="5500-7000",
                ),
            ],
        )
        actions2 = plan_writeback(pkg2, conn, target_families={"competitive"})
        apply_writeback(actions2, conn)

        # Verify provenance was written
        asset = select_one(conn, "SELECT * FROM asset_ledger WHERE asset_id = ?", ("img-fz3-side",))
        assert asset["updated_at"] is not None
        assert asset["last_run_id"] == "run-comp-e2e-02"

        sku = select_one(conn, "SELECT * FROM sku_catalog WHERE sku_id = ?", ("sku-fz3-110",))
        assert sku["updated_at"] is not None
        assert sku["last_run_id"] == "run-comp-e2e-02"

    def test_old_schema_migration_then_update(self):
        """DB created with old schema (no updated_at/last_run_id) migrates via init_db()."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        # Simulate old schema: asset_ledger/sku_catalog WITHOUT updated_at/last_run_id
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS asset_ledger (
          asset_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL DEFAULT '',
          brand TEXT DEFAULT '',
          product_line TEXT DEFAULT '',
          category TEXT DEFAULT '',
          source_url TEXT DEFAULT '',
          local_path TEXT DEFAULT '',
          acquisition_date TEXT DEFAULT '',
          is_official INTEGER DEFAULT 0,
          quality_grade TEXT DEFAULT '',
          visible_content TEXT DEFAULT '',
          supports_conclusion TEXT DEFAULT '',
          prohibits_conclusion TEXT DEFAULT '',
          created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS sku_catalog (
          sku_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL DEFAULT '',
          brand TEXT DEFAULT '',
          series TEXT DEFAULT '',
          model TEXT DEFAULT '',
          positioning TEXT DEFAULT '',
          price_range TEXT DEFAULT '',
          wheel_diameter TEXT DEFAULT '',
          frame_type TEXT DEFAULT '',
          motor_type TEXT DEFAULT '',
          battery_platform TEXT DEFAULT '',
          brake_config TEXT DEFAULT '',
          target_audience TEXT DEFAULT '',
          style_tags_json TEXT DEFAULT '[]',
          evidence_sources_json TEXT DEFAULT '[]',
          created_at TEXT DEFAULT (datetime('now'))
        );
        """)

        # Verify old columns don't include updated_at/last_run_id
        cols_asset = {r["name"] for r in conn.execute("PRAGMA table_info(asset_ledger)").fetchall()}
        assert "updated_at" not in cols_asset
        assert "last_run_id" not in cols_asset

        # Run full init_db — should add missing columns
        init_db(conn)

        # Verify columns now exist
        cols_asset = {r["name"] for r in conn.execute("PRAGMA table_info(asset_ledger)").fetchall()}
        assert "updated_at" in cols_asset
        assert "last_run_id" in cols_asset
        cols_sku = {r["name"] for r in conn.execute("PRAGMA table_info(sku_catalog)").fetchall()}
        assert "updated_at" in cols_sku
        assert "last_run_id" in cols_sku

        # INSERT an asset, then UPDATE — should succeed with provenance
        conn.execute(
            "INSERT INTO asset_ledger (asset_id, run_id, brand) VALUES (?, ?, ?)",
            ("img-migrate-test", "run-old", "TestBrand"),
        )
        pkg = _make_package_with_competitive_data(
            run_id="run-new",
            image_assets=[
                ImageAssetRef(
                    asset_id="img-migrate-test",
                    brand="TestBrand",
                    quality_grade="high",
                    visible_content="migrated",
                ),
            ],
            sku_records=[],
        )
        actions = plan_writeback(pkg, conn, target_families={"competitive"})
        apply_writeback(actions, conn)

        row = select_one(conn, "SELECT * FROM asset_ledger WHERE asset_id = ?", ("img-migrate-test",))
        assert row["quality_grade"] == "high"
        assert row["updated_at"] is not None
        assert row["last_run_id"] == "run-new"
