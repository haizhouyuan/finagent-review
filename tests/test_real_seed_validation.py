"""Two-wheeler real seed: competitive writeback validation.

Constructs ImageAssetRef[] and SkuRecord[] from real two-wheeler market
research data, then validates the competitive-only writeback pipeline.

Acceptance criteria:
1. Real data writes to asset_ledger + sku_catalog ✓
2. target_families={"competitive"} only — no thesis/source pollution ✓
3. Idempotent: double-apply produces no duplicates ✓
4. Queryable: 3 practical queries return useful results ✓
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from finagent.db import SCHEMA_SQL, init_db, select_one, list_rows
from finagent.research_contracts import (
    ResearchPackage, EvidenceRef, ImageAssetRef, SkuRecord,
    WritebackTarget, WritebackOp,
)
from finagent.writeback_engine import plan_writeback, apply_writeback


# ── Real two-wheeler product data ────────────────────────────────────

REAL_IMAGE_ASSETS = [
    ImageAssetRef(
        asset_id="img-ninebot-fz3-side",
        brand="九号",
        product_line="Fz3 110",
        category="exterior",
        source_url="https://www.ninebot.com/product/fz3",
        local_path="photos/九号_Fz3_110_侧面.webp",
        acquisition_date="2026-03-22",
        is_official=True,
        quality_grade="high",
        visible_content="整车侧面45度，双管车架结构清晰，14寸轮毂",
        supports_conclusion="采用双管一体式车架，铝合金轮毂",
        prohibits_conclusion="底盘走线细节不可判定",
    ),
    ImageAssetRef(
        asset_id="img-yadea-dm6-front",
        brand="雅迪",
        product_line="冠能DM6",
        category="exterior",
        source_url="https://www.yadea.com/product/dm6",
        local_path="photos/雅迪_冠能DM6_正面.webp",
        acquisition_date="2026-03-22",
        is_official=True,
        quality_grade="high",
        visible_content="正面45度角，前碟刹清晰，10寸铝合金轮毂",
        supports_conclusion="前碟后鼓制动，铝合金轮毂确认",
    ),
    ImageAssetRef(
        asset_id="img-aima-a500-side",
        brand="爱玛",
        product_line="A500",
        category="exterior",
        source_url="https://www.aima.com/product/a500",
        local_path="photos/爱玛_A500_侧面.webp",
        acquisition_date="2026-03-22",
        is_official=True,
        quality_grade="medium",
        visible_content="整车侧面，单管车架，12寸钢轮毂",
        supports_conclusion="采用单管简易车架，钢制轮毂",
    ),
    ImageAssetRef(
        asset_id="img-tailing-n9-frame",
        brand="台铃",
        product_line="N9",
        category="structure",
        source_url="https://www.tailg.com.cn/product/n9",
        local_path="photos/台铃_N9_车架特写.webp",
        acquisition_date="2026-03-22",
        is_official=False,
        quality_grade="medium",
        visible_content="车架焊接结构特写，双管构型",
        supports_conclusion="车架采用双管结构，焊接工艺一般",
    ),
    ImageAssetRef(
        asset_id="img-niu-nqi-sport",
        brand="小牛",
        product_line="NQi Sport",
        category="exterior",
        source_url="https://www.niu.com/nqi-sport",
        local_path="photos/小牛_NQi_Sport_侧面.webp",
        acquisition_date="2026-03-22",
        is_official=True,
        quality_grade="high",
        visible_content="整车侧面运动造型，铝合金Y型轮毂",
        supports_conclusion="Y型五辐铝合金轮毂，运动定位明确",
    ),
]

REAL_SKU_RECORDS = [
    SkuRecord(
        sku_id="sku-ninebot-fz3-110",
        brand="九号",
        series="Fz",
        model="Fz3 110",
        positioning="中高端",
        price_range="6299-7599",
        wheel_diameter="14寸",
        frame_type="双管一体",
        motor_type="轮毂电机 1200W",
        battery_platform="72V30Ah 锂电",
        brake_config="前碟后碟",
        target_audience="年轻男性/通勤",
        style_tags=("运动", "机甲", "智能"),
        evidence_sources=("官网", "线下门店"),
    ),
    SkuRecord(
        sku_id="sku-yadea-dm6",
        brand="雅迪",
        series="冠能",
        model="冠能DM6",
        positioning="中高端",
        price_range="4999-6599",
        wheel_diameter="10寸",
        frame_type="单管加强",
        motor_type="轮毂电机 800W",
        battery_platform="60V24Ah 石墨烯",
        brake_config="前碟后鼓",
        target_audience="通勤白领",
        style_tags=("时尚", "长续航", "石墨烯"),
        evidence_sources=("官网", "京东", "线下门店"),
    ),
    SkuRecord(
        sku_id="sku-aima-a500",
        brand="爱玛",
        series="A",
        model="A500",
        positioning="中端",
        price_range="3299-4299",
        wheel_diameter="12寸",
        frame_type="单管",
        motor_type="轮毂电机 600W",
        battery_platform="48V20Ah 铅酸",
        brake_config="前鼓后鼓",
        target_audience="大众通勤",
        style_tags=("实用", "性价比"),
        evidence_sources=("官网", "天猫"),
    ),
    SkuRecord(
        sku_id="sku-tailing-n9",
        brand="台铃",
        series="N",
        model="N9",
        positioning="中高端",
        price_range="4599-5999",
        wheel_diameter="14寸",
        frame_type="双管",
        motor_type="轮毂电机 1000W",
        battery_platform="72V22Ah 锂电",
        brake_config="前碟后鼓",
        target_audience="城际通勤",
        style_tags=("省电", "超远续航"),
        evidence_sources=("官网", "抖音直播"),
    ),
    SkuRecord(
        sku_id="sku-niu-nqi-sport",
        brand="小牛",
        series="NQi",
        model="NQi Sport",
        positioning="中高端",
        price_range="5599-7999",
        wheel_diameter="14寸",
        frame_type="双管一体",
        motor_type="中置电机 1200W",
        battery_platform="72V35Ah 锂电",
        brake_config="前碟后碟",
        target_audience="年轻男性",
        style_tags=("运动", "科技", "APP控车"),
        evidence_sources=("官网", "小红书测评"),
    ),
    SkuRecord(
        sku_id="sku-yadea-t5",
        brand="雅迪",
        series="DE",
        model="DE1 T5",
        positioning="低端",
        price_range="1999-2599",
        wheel_diameter="10寸",
        frame_type="单管简易",
        motor_type="轮毂电机 350W",
        battery_platform="48V12Ah 铅酸",
        brake_config="前鼓后鼓",
        target_audience="学生/短途代步",
        style_tags=("入门", "轻便"),
        evidence_sources=("拼多多", "线下"),
    ),
    SkuRecord(
        sku_id="sku-ninebot-e2-plus",
        brand="九号",
        series="E",
        model="E2 Plus",
        positioning="中端",
        price_range="3499-3999",
        wheel_diameter="10寸",
        frame_type="单管",
        motor_type="轮毂电机 400W",
        battery_platform="48V24Ah 锂电",
        brake_config="前碟后鼓",
        target_audience="女性通勤",
        style_tags=("轻巧", "智能", "时尚"),
        evidence_sources=("官网", "小红书"),
    ),
]


def _make_seed_package(run_id: str = "run-seed-2wheeler-01") -> ResearchPackage:
    return ResearchPackage(
        run_id=run_id,
        goal="两轮车竞品车身结构与轮毂技术对标",
        context="两轮车",
        triples=[
            {"subject": "九号Fz3", "predicate": "采用", "object": "双管一体式车架"},
            {"subject": "雅迪冠能DM6", "predicate": "配备", "object": "石墨烯电池"},
            {"subject": "小牛NQi", "predicate": "使用", "object": "中置电机"},
        ],
        evidence_refs=[
            EvidenceRef(evidence_id=1, query="九号Fz3车架", char_count=500),
            EvidenceRef(evidence_id=2, query="雅迪冠能技术参数", char_count=400),
        ],
        report_md="# 两轮车竞品对标\n\n5品牌7车型对标分析。",
        confidence=0.80,
        image_assets=REAL_IMAGE_ASSETS,
        sku_records=REAL_SKU_RECORDS,
    )


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


# ── Acceptance Criterion 1: Real data writes successfully ────────────

class TestRealSeedWrite:
    def test_seed_writes_all_assets_and_skus(self):
        """5 images + 7 SKUs from real brands write to DB."""
        conn = _make_conn()
        pkg = _make_seed_package()
        actions = plan_writeback(pkg, conn, target_families={"competitive"})
        apply_writeback(actions, conn)

        assets = list_rows(conn, "SELECT * FROM asset_ledger ORDER BY brand")
        assert len(assets) == 5

        skus = list_rows(conn, "SELECT * FROM sku_catalog ORDER BY brand, model")
        assert len(skus) == 7

        # Spot-check 九号 Fz3
        fz3_asset = select_one(conn, "SELECT * FROM asset_ledger WHERE asset_id = ?",
                               ("img-ninebot-fz3-side",))
        assert fz3_asset["brand"] == "九号"
        assert fz3_asset["quality_grade"] == "high"
        assert "双管" in fz3_asset["supports_conclusion"]

        fz3_sku = select_one(conn, "SELECT * FROM sku_catalog WHERE sku_id = ?",
                             ("sku-ninebot-fz3-110",))
        assert fz3_sku["brand"] == "九号"
        assert fz3_sku["frame_type"] == "双管一体"
        assert "机甲" in json.loads(fz3_sku["style_tags_json"])


# ── Acceptance Criterion 2: competitive-only, no thesis pollution ────

class TestNoPollution:
    def test_no_thesis_or_source_created(self):
        """competitive-only seed does NOT create thesis/source/monitor."""
        conn = _make_conn()
        pkg = _make_seed_package()
        actions = plan_writeback(pkg, conn, target_families={"competitive"})
        apply_writeback(actions, conn)

        # Verify no thesis OS records
        theses = list_rows(conn, "SELECT * FROM theses")
        assert len(theses) == 0
        sources = list_rows(conn, "SELECT * FROM sources")
        assert len(sources) == 0
        monitors = list_rows(conn, "SELECT * FROM monitors")
        assert len(monitors) == 0

    def test_action_types_are_only_competitive(self):
        """All planned actions are ASSET_LEDGER or SKU_CATALOG."""
        conn = _make_conn()
        pkg = _make_seed_package()
        actions = plan_writeback(pkg, conn, target_families={"competitive"})

        types = {a.target_type for a in actions}
        assert types == {WritebackTarget.ASSET_LEDGER.value, WritebackTarget.SKU_CATALOG.value}


# ── Acceptance Criterion 3: Idempotent ───────────────────────────────

class TestIdempotency:
    def test_double_apply_no_duplicates(self):
        """Applying same seed twice produces no duplicates."""
        conn = _make_conn()
        pkg = _make_seed_package()

        # First apply
        actions1 = plan_writeback(pkg, conn, target_families={"competitive"})
        apply_writeback(actions1, conn)

        # Second apply (same data, same run_id)
        actions2 = plan_writeback(pkg, conn, target_families={"competitive"})
        apply_writeback(actions2, conn)

        assets = list_rows(conn, "SELECT * FROM asset_ledger")
        assert len(assets) == 5  # no dupes

        skus = list_rows(conn, "SELECT * FROM sku_catalog")
        assert len(skus) == 7  # no dupes

    def test_second_run_updates_not_duplicates(self):
        """Second run with new run_id plans UPDATE for existing records."""
        conn = _make_conn()
        pkg1 = _make_seed_package()
        actions1 = plan_writeback(pkg1, conn, target_families={"competitive"})
        apply_writeback(actions1, conn)

        pkg2 = _make_seed_package(run_id="run-seed-2wheeler-02")
        actions2 = plan_writeback(pkg2, conn, target_families={"competitive"})

        # All should be UPDATE since records exist
        assert all(a.op == WritebackOp.UPDATE.value for a in actions2)


# ── Acceptance Criterion 4: Queryable results ────────────────────────

class TestQueryable:
    @pytest.fixture(autouse=True)
    def _seed(self):
        self.conn = _make_conn()
        pkg = _make_seed_package()
        actions = plan_writeback(pkg, conn=self.conn, target_families={"competitive"})
        apply_writeback(actions, self.conn)

    def test_query_by_brand(self):
        """Query: all SKUs for brand '九号'."""
        rows = list_rows(self.conn,
                         "SELECT model, price_range, frame_type FROM sku_catalog WHERE brand = ? ORDER BY model",
                         ("九号",))
        assert len(rows) == 2  # Fz3 110 + E2 Plus
        models = [r["model"] for r in rows]
        assert "Fz3 110" in models
        assert "E2 Plus" in models

    def test_query_frame_type_distribution(self):
        """Query: frame type distribution across brands."""
        rows = list_rows(
            self.conn,
            """SELECT frame_type, COUNT(*) as cnt, GROUP_CONCAT(brand || ' ' || model, ', ') as models
               FROM sku_catalog
               GROUP BY frame_type
               ORDER BY cnt DESC""",
        )
        assert len(rows) >= 2  # at least 双管 and 单管 families
        # 双管 variants should be the most common among mid-high end
        frame_types = {r["frame_type"]: r["cnt"] for r in rows}
        assert "双管一体" in frame_types or "双管" in frame_types

    def test_query_high_quality_official_assets(self):
        """Query: high-quality official product photos."""
        rows = list_rows(
            self.conn,
            """SELECT brand, product_line, visible_content
               FROM asset_ledger
               WHERE quality_grade = 'high' AND is_official = 1
               ORDER BY brand""",
        )
        assert len(rows) >= 3
        brands = {r["brand"] for r in rows}
        assert "九号" in brands
        assert "雅迪" in brands
        assert "小牛" in brands
