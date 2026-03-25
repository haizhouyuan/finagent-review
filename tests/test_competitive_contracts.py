"""Tests for competitive research contracts and writeback targets.

Covers: ImageAssetRef, SkuRecord, asset_ledger, sku_catalog.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from finagent.db import SCHEMA_SQL, select_one, list_rows
from finagent.research_contracts import (
    ImageAssetRef, SkuRecord, WritebackAction, WritebackTarget, WritebackOp,
)
from finagent.writeback_engine import apply_writeback


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


# ── Contract Round-Trip Tests ────────────────────────────────────────

class TestImageAssetRef:
    def test_round_trip(self):
        ref = ImageAssetRef(
            asset_id="img-001",
            brand="九号",
            product_line="Fz3 110",
            category="exterior",
            source_url="https://example.com/fz3.jpg",
            local_path="photos/01_九号_Fz3.jpg",
            acquisition_date="2026-03-22",
            is_official=True,
            quality_grade="high",
            visible_content="整车侧面",
            supports_conclusion="双管车架结构确认",
            prohibits_conclusion="底盘细节不可判定",
        )
        d = ref.to_dict()
        restored = ImageAssetRef.from_dict(d)
        assert restored == ref
        assert restored.is_official is True
        assert restored.brand == "九号"

    def test_frozen(self):
        ref = ImageAssetRef(asset_id="img-x", brand="test")
        with pytest.raises(AttributeError):
            ref.brand = "changed"  # type: ignore


class TestSkuRecord:
    def test_round_trip(self):
        sku = SkuRecord(
            sku_id="sku-001",
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
        )
        d = sku.to_dict()
        # dict converts tuples to lists
        assert isinstance(d["style_tags"], list)
        assert d["style_tags"] == ["运动", "机甲"]

        restored = SkuRecord.from_dict(d)
        assert restored == sku
        assert isinstance(restored.style_tags, tuple)

    def test_frozen(self):
        sku = SkuRecord(sku_id="sku-x", brand="test")
        with pytest.raises(AttributeError):
            sku.brand = "changed"  # type: ignore


# ── Writeback Handlers ──────────────────────────────────────────────

class TestAssetLedgerWriteback:
    def test_insert_asset(self):
        conn = _make_conn()
        action = WritebackAction(
            package_id="run-test-comp",
            target_type=WritebackTarget.ASSET_LEDGER.value,
            op=WritebackOp.CREATE.value,
            payload={
                "asset_id": "img-wb-001",
                "brand": "九号",
                "product_line": "Fz3 110",
                "category": "structure",
                "source_url": "https://example.com/fz3-frame.jpg",
                "local_path": "photos/fz3_frame.jpg",
                "is_official": True,
                "quality_grade": "high",
                "visible_content": "双管车架焊接结构",
            },
        )
        apply_writeback([action], conn)

        row = select_one(conn, "SELECT * FROM asset_ledger WHERE asset_id = ?", ("img-wb-001",))
        assert row is not None
        assert row["brand"] == "九号"
        assert row["product_line"] == "Fz3 110"
        assert row["is_official"] == 1
        assert row["run_id"] == "run-test-comp"

    def test_idempotent_insert(self):
        conn = _make_conn()
        action = WritebackAction(
            package_id="run-idem",
            target_type=WritebackTarget.ASSET_LEDGER.value,
            op=WritebackOp.CREATE.value,
            payload={
                "asset_id": "img-idem",
                "brand": "绿源",
                "category": "exterior",
            },
        )
        apply_writeback([action], conn)
        apply_writeback([action], conn)

        rows = list_rows(conn, "SELECT * FROM asset_ledger WHERE asset_id = 'img-idem'")
        assert len(rows) == 1


class TestSkuCatalogWriteback:
    def test_insert_sku(self):
        conn = _make_conn()
        action = WritebackAction(
            package_id="run-test-sku",
            target_type=WritebackTarget.SKU_CATALOG.value,
            op=WritebackOp.CREATE.value,
            payload={
                "sku_id": "sku-wb-001",
                "brand": "九号",
                "series": "Fz",
                "model": "Fz3 110",
                "positioning": "中高端",
                "price_range": "6000-8000",
                "wheel_diameter": "14寸",
                "frame_type": "双管",
                "motor_type": "轮毂电机",
                "style_tags": ["运动", "机甲"],
                "evidence_sources": ["官网"],
            },
        )
        apply_writeback([action], conn)

        row = select_one(conn, "SELECT * FROM sku_catalog WHERE sku_id = ?", ("sku-wb-001",))
        assert row is not None
        assert row["brand"] == "九号"
        assert row["frame_type"] == "双管"
        assert row["run_id"] == "run-test-sku"
        # JSON fields
        tags = json.loads(row["style_tags_json"])
        assert tags == ["运动", "机甲"]
        sources = json.loads(row["evidence_sources_json"])
        assert sources == ["官网"]

    def test_idempotent_sku(self):
        conn = _make_conn()
        action = WritebackAction(
            package_id="run-sku-idem",
            target_type=WritebackTarget.SKU_CATALOG.value,
            op=WritebackOp.CREATE.value,
            payload={
                "sku_id": "sku-idem",
                "brand": "绿源",
                "series": "X",
                "model": "X30",
            },
        )
        apply_writeback([action], conn)
        apply_writeback([action], conn)

        rows = list_rows(conn, "SELECT * FROM sku_catalog WHERE sku_id = 'sku-idem'")
        assert len(rows) == 1


class TestMixedWriteback:
    def test_thesis_and_competitive_in_same_batch(self):
        """v1 thesis + v2 competitive actions can coexist in one apply."""
        conn = _make_conn()
        actions = [
            # v1: source
            WritebackAction(
                package_id="run-mixed",
                target_type=WritebackTarget.SOURCE.value,
                op=WritebackOp.CREATE.value,
                payload={
                    "source_id": "src-mixed-001",
                    "source_type": "v2_research_run",
                    "name": "Mixed test",
                    "primaryness": "secondary",
                },
            ),
            # v2: asset
            WritebackAction(
                package_id="run-mixed",
                target_type=WritebackTarget.ASSET_LEDGER.value,
                op=WritebackOp.CREATE.value,
                payload={
                    "asset_id": "img-mixed-001",
                    "brand": "小牛",
                    "category": "wheel",
                },
            ),
            # v2: SKU
            WritebackAction(
                package_id="run-mixed",
                target_type=WritebackTarget.SKU_CATALOG.value,
                op=WritebackOp.CREATE.value,
                payload={
                    "sku_id": "sku-mixed-001",
                    "brand": "小牛",
                    "series": "NQi",
                    "model": "NQi GTs",
                },
            ),
        ]
        apply_writeback(actions, conn)

        assert select_one(conn, "SELECT * FROM sources WHERE source_id = ?", ("src-mixed-001",))
        assert select_one(conn, "SELECT * FROM asset_ledger WHERE asset_id = ?", ("img-mixed-001",))
        assert select_one(conn, "SELECT * FROM sku_catalog WHERE sku_id = ?", ("sku-mixed-001",))
