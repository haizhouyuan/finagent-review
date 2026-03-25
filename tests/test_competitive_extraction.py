"""Tests for agent graph competitive data extraction.

Covers:
1. State reducers: dedup by asset_id / sku_id
2. Keyword gate: competitive extraction only fires on product evidence
3. Extractor produces competitive data from product evidence
4. Extractor skips competitive extraction for non-product evidence
5. End-to-end: extractor_node returns image_assets/sku_records
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from finagent.agents.state import (
    ResearchState, initial_state,
    _merge_by_asset_id, _merge_by_sku_id,
)
from finagent.agents.extractor import (
    extractor_node,
    _has_competitive_signal,
    _extract_competitive_data,
    _parse_competitive_json,
    COMPETITIVE_KEYWORDS,
)


# ── State reducer tests ──────────────────────────────────────────────

class TestStateReducers:
    def test_merge_by_asset_id_dedup(self):
        """Duplicate asset_id → latest wins."""
        existing = [{"asset_id": "img-a", "brand": "old"}]
        new = [{"asset_id": "img-a", "brand": "new"}, {"asset_id": "img-b", "brand": "b"}]
        result = _merge_by_asset_id(existing, new)
        assert len(result) == 2
        by_id = {r["asset_id"]: r for r in result}
        assert by_id["img-a"]["brand"] == "new"  # latest wins
        assert by_id["img-b"]["brand"] == "b"

    def test_merge_by_sku_id_dedup(self):
        """Duplicate sku_id → latest wins."""
        existing = [{"sku_id": "sku-x", "model": "v1"}]
        new = [{"sku_id": "sku-x", "model": "v2"}]
        result = _merge_by_sku_id(existing, new)
        assert len(result) == 1
        assert result[0]["model"] == "v2"

    def test_merge_empty(self):
        """Empty lists don't crash."""
        assert _merge_by_asset_id([], []) == []
        assert _merge_by_sku_id([], []) == []
        assert _merge_by_asset_id(None, []) == []

    def test_initial_state_has_competitive_fields(self):
        """initial_state includes empty competitive fields."""
        state = initial_state("test goal")
        assert state["image_assets"] == []
        assert state["sku_records"] == []


# ── Keyword gate tests ───────────────────────────────────────────────

class TestKeywordGate:
    def test_product_text_triggers(self):
        """Text with 2+ product keywords → True."""
        text = "雅迪冠能DM6的车架采用双管结构"
        assert _has_competitive_signal(text) is True

    def test_generic_text_skipped(self):
        """Text without product keywords → False."""
        text = "The Federal Reserve raised interest rates by 25 basis points."
        assert _has_competitive_signal(text) is False

    def test_single_keyword_not_enough(self):
        """Only 1 keyword is not enough."""
        text = "这款电池容量很大"
        assert _has_competitive_signal(text) is False

    def test_english_keywords_work(self):
        """English keywords also trigger."""
        text = "The new product model features an aluminum wheel frame design"
        assert _has_competitive_signal(text) is True

    def test_brand_names_trigger(self):
        """Brand names count as keywords."""
        text = "Yadea 的 Ninebot 竞品分析"
        assert _has_competitive_signal(text) is True


# ── Competitive JSON parsing ─────────────────────────────────────────

class TestCompetitiveJsonParsing:
    def test_clean_json(self):
        data = _parse_competitive_json('{"image_assets": [], "sku_records": [{"sku_id": "a"}]}')
        assert data["sku_records"][0]["sku_id"] == "a"

    def test_code_fenced_json(self):
        raw = '```json\n{"image_assets": [{"asset_id": "x"}], "sku_records": []}\n```'
        data = _parse_competitive_json(raw)
        assert data["image_assets"][0]["asset_id"] == "x"

    def test_garbage_returns_empty(self):
        assert _parse_competitive_json("not json at all") == {}


# ── Mock LLM for competitive extraction ──────────────────────────────

def _mock_competitive_llm(system: str, user: str) -> str:
    """Mock LLM that returns competitive data when prompt contains product text."""
    if "竞品" in system or "competitive" in system.lower():
        return json.dumps({
            "image_assets": [
                {
                    "asset_id": "img-yadea-dm6-side",
                    "brand": "雅迪",
                    "product_line": "冠能",
                    "category": "exterior",
                    "visible_content": "冠能DM6侧面图",
                    "supports_conclusion": "10寸铝合金轮毂",
                }
            ],
            "sku_records": [
                {
                    "sku_id": "sku-yadea-dm6",
                    "brand": "雅迪",
                    "series": "冠能",
                    "model": "冠能DM6",
                    "positioning": "中高端",
                    "price_range": "4999-6599",
                    "wheel_diameter": "10寸",
                    "frame_type": "单管加强",
                    "motor_type": "轮毂电机 800W",
                    "battery_platform": "60V24Ah 石墨烯",
                    "brake_config": "前碟后鼓",
                    "target_audience": "通勤白领",
                    "style_tags": ["时尚", "长续航"],
                }
            ],
        }, ensure_ascii=False)
    # For triple extraction
    return "[]"


def _mock_triple_and_competitive_llm(system: str, user: str) -> str:
    """Mock LLM that handles both triple extraction and competitive extraction."""
    if "竞品" in system:
        return _mock_competitive_llm(system, user)
    # Triple extraction
    return json.dumps([{
        "head": "雅迪",
        "head_type": "company",
        "relation": "manufactures",
        "tail": "冠能DM6",
        "tail_type": "entity",
        "exact_quote": "雅迪冠能DM6",
        "confidence": 0.9,
        "valid_from": "2026",
    }])


# ── Extraction function tests ────────────────────────────────────────

class TestCompetitiveExtraction:
    def test_extract_produces_data(self):
        """_extract_competitive_data returns images and SKUs from product text."""
        text = "雅迪冠能DM6的车架采用单管加强结构，配备10寸铝合金轮毂，售价4999-6599元"
        images, skus = _extract_competitive_data(text, _mock_competitive_llm)
        assert len(images) == 1
        assert images[0]["asset_id"] == "img-yadea-dm6-side"
        assert len(skus) == 1
        assert skus[0]["sku_id"] == "sku-yadea-dm6"
        assert skus[0]["price_range"] == "4999-6599"


# ── extractor_node integration tests ─────────────────────────────────

class TestExtractorNodeCompetitive:
    def test_extractor_returns_competitive_data(self):
        """extractor_node returns image_assets/sku_records for product evidence."""
        state = initial_state("两轮车竞品对标")
        state["gathered_evidence"] = [
            {
                "evidence_id": None,
                "_text": "雅迪冠能DM6采用单管加强车架，10寸铝合金轮毂，售价4999-6599元。"
                         "配备石墨烯电池，续航强劲。",
            }
        ]

        result = extractor_node(
            state,
            llm_fn=_mock_triple_and_competitive_llm,
        )

        assert "image_assets" in result
        assert "sku_records" in result
        assert len(result["image_assets"]) == 1
        assert len(result["sku_records"]) == 1
        assert result["sku_records"][0]["brand"] == "雅迪"

    def test_extractor_skips_non_product_evidence(self):
        """extractor_node returns empty competitive data for non-product evidence."""
        state = initial_state("半导体设备投研")
        state["gathered_evidence"] = [
            {
                "evidence_id": None,
                "_text": "The Federal Reserve held interest rates steady at 5.25-5.50%. "
                         "Bond yields fell in response to the dovish forward guidance.",
            }
        ]

        result = extractor_node(
            state,
            llm_fn=_mock_triple_and_competitive_llm,
        )

        assert result.get("image_assets", []) == []
        assert result.get("sku_records", []) == []

    def test_extractor_no_llm_returns_empty(self):
        """Without LLM, competitive fields are still present but empty."""
        state = initial_state("test")
        state["gathered_evidence"] = [{"_text": "雅迪冠能DM6 车架信息"}]

        result = extractor_node(state)
        assert result.get("image_assets", []) == []
        assert result.get("sku_records", []) == []
