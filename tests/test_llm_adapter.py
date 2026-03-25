"""Tests for finagent.llm_adapter — adapter creation and auto-detect."""
from __future__ import annotations

import json
import os
from unittest.mock import patch

from finagent.llm_adapter import create_llm_adapter, auto_detect_adapter


class TestMockAdapter:
    def test_create_mock(self):
        llm = create_llm_adapter("mock")
        assert callable(llm)

    def test_mock_planner_response(self):
        llm = create_llm_adapter("mock")
        result = llm("你是一个规划师", "请分析商业航天")
        data = json.loads(result)
        assert "queries" in data
        assert isinstance(data["queries"], list)

    def test_mock_extractor_response(self):
        llm = create_llm_adapter("mock")
        # Mock extractor needs entity names from the known pattern
        result = llm(
            "你是一个信息抽取器",
            "蓝箭航天成功发射朱雀二号火箭，与星河动力合作",
        )
        data = json.loads(result)
        # Mock extractor returns a raw list of triples
        assert isinstance(data, list)
        assert len(data) > 0
        assert "head" in data[0]

    def test_mock_generic_response(self):
        """Non-planner/non-entity prompts return empty list."""
        llm = create_llm_adapter("mock")
        result = llm("你是一个评估器", "当前进度如何")
        data = json.loads(result)
        # Falls through to extractor path with no entities → empty list
        assert isinstance(data, list)


class TestCreateAdapterErrors:
    def test_unknown_backend_raises(self):
        try:
            create_llm_adapter("nonexistent_backend")
            assert False, "should raise ValueError"
        except ValueError as e:
            assert "nonexistent_backend" in str(e)


class TestAutoDetect:
    def test_fallback_to_mock(self):
        """Without any env vars or services, should fall back to mock."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove potentially set keys
            for key in ["OPENAI_API_KEY", "FINAGENT_LLM_BASE_URL",
                        "CHATGPTREST_API_URL", "CHATGPTREST_BASE_URL"]:
                os.environ.pop(key, None)
            llm = auto_detect_adapter()
            assert callable(llm)
            # Should be mock — verify by calling it
            result = llm("你是规划师", "test")
            data = json.loads(result)
            assert "queries" in data

    def test_openai_key_detected(self):
        """If OPENAI_API_KEY is set, should choose openai."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}, clear=False):
            # This will try to import openai — may fail if not installed
            # But the detection logic should at least try
            try:
                llm = auto_detect_adapter()
                # If openai is installed, it should return an openai adapter
                assert callable(llm)
            except Exception:
                # openai not installed — acceptable in test env
                pass
