"""Tests for activate_two_wheeler_pipeline.py components.

Covers: SessionLedger, _parse_sse_events, _detect_role, resume logic.
Does NOT require network / MCP / ChatgptREST — all tests are offline.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.activate_two_wheeler_pipeline import (
    SessionLedger,
    _detect_role,
    _parse_sse_events,
    _extract_inner_json,
)


# ═══════════════════════════════════════════════════════════════════
# SessionLedger
# ═══════════════════════════════════════════════════════════════════

class TestSessionLedger:
    def test_log_and_load_pending(self, tmp_path):
        path = tmp_path / "ledger.jsonl"
        lg = SessionLedger(path)

        lg.log("sess_a", status="submitted", role="planner")
        lg.log("sess_b", status="submitted", role="extractor")
        # sess_a completes
        lg.log("sess_a", status="completed", role="planner", answer_len=5000)

        pending = lg.load_pending()
        assert len(pending) == 1
        assert pending[0]["session_id"] == "sess_b"
        assert pending[0]["status"] == "submitted"

    def test_load_pending_with_polling(self, tmp_path):
        path = tmp_path / "ledger.jsonl"
        lg = SessionLedger(path)

        lg.log("sess_x", status="submitted", role="planner")
        lg.log("sess_x", status="polling", role="planner")
        # Still pending (polling, not completed)
        pending = lg.load_pending()
        assert len(pending) == 1
        assert pending[0]["status"] == "polling"

    def test_load_pending_timeout_not_pending(self, tmp_path):
        path = tmp_path / "ledger.jsonl"
        lg = SessionLedger(path)

        lg.log("sess_t", status="submitted", role="planner")
        lg.log("sess_t", status="timeout", role="planner")
        # Timeout is a terminal state, not pending
        pending = lg.load_pending()
        assert len(pending) == 0

    def test_load_pending_empty_file(self, tmp_path):
        path = tmp_path / "ledger.jsonl"
        path.write_text("")
        lg = SessionLedger(path)
        assert lg.load_pending() == []

    def test_load_pending_no_file(self, tmp_path):
        path = tmp_path / "nonexistent.jsonl"
        lg = SessionLedger(path)
        assert lg.load_pending() == []

    def test_log_writes_valid_jsonl(self, tmp_path):
        path = tmp_path / "ledger.jsonl"
        lg = SessionLedger(path)
        lg.log("s1", status="submitted", role="planner", message_preview="hello")
        lg.log("s1", status="completed", answer_len=100)

        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            parsed = json.loads(line)
            assert "ts" in parsed
            assert "session_id" in parsed

    def test_multiple_sessions_mixed_states(self, tmp_path):
        path = tmp_path / "ledger.jsonl"
        lg = SessionLedger(path)

        lg.log("a", status="submitted", role="planner")
        lg.log("b", status="submitted", role="extractor")
        lg.log("c", status="submitted", role="evaluator")
        lg.log("a", status="completed", answer_len=1000)
        lg.log("c", status="failed", error="timeout")

        pending = lg.load_pending()
        assert len(pending) == 1
        assert pending[0]["session_id"] == "b"

    def test_message_preview_with_newlines_stays_single_line(self, tmp_path):
        """Regression: real prompts contain newlines that break JSONL."""
        path = tmp_path / "ledger.jsonl"
        lg = SessionLedger(path)

        # Simulate a real multi-line Chinese prompt like the planner sends
        prompt = (
            "你是一个投研图谱探索规划师。\n"
            "你的任务是分析当前知识图谱的状态。\n"
            '## 输出格式（严格JSON）：\n```json\n{"analysis": "..."}\n```'
        )
        lg.log("sess_cn", status="submitted", role="planner",
               message_preview=prompt)

        # Every line in the file must be valid JSON
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 1, f"Expected 1 JSONL line, got {len(lines)}"
        parsed = json.loads(lines[0])
        assert parsed["session_id"] == "sess_cn"
        assert "\n" not in parsed["message_preview"]

    def test_load_pending_survives_sanitized_content(self, tmp_path):
        """Ensure load_pending works after writing sanitized content."""
        path = tmp_path / "ledger.jsonl"
        lg = SessionLedger(path)

        lg.log("s1", status="submitted", role="planner",
               message_preview="line1\nline2\nline3")
        lg.log("s2", status="submitted", role="extractor",
               message_preview="no newlines here")

        pending = lg.load_pending()
        assert len(pending) == 2


# ═══════════════════════════════════════════════════════════════════
# SSE parsing
# ═══════════════════════════════════════════════════════════════════

class TestParseSSE:
    def test_single_line_event(self):
        raw = 'event: message\ndata: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}\n\n'
        events = _parse_sse_events(raw)
        assert len(events) == 1
        assert events[0]["result"]["ok"] is True

    def test_multi_line_data(self):
        raw = (
            'event: message\n'
            'data: {"jsonrpc":"2.0",\n'
            'data: "id":1,\n'
            'data: "result":{"ok":true}}\n'
            '\n'
        )
        events = _parse_sse_events(raw)
        assert len(events) == 1
        assert events[0]["result"]["ok"] is True

    def test_multiple_events(self):
        raw = (
            'data: {"jsonrpc":"2.0","id":1,"result":{"a":1}}\n\n'
            'data: {"jsonrpc":"2.0","id":2,"result":{"b":2}}\n\n'
        )
        events = _parse_sse_events(raw)
        assert len(events) == 2

    def test_trailing_data_no_blank_line(self):
        raw = 'data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}'
        events = _parse_sse_events(raw)
        assert len(events) == 1

    def test_empty_input(self):
        assert _parse_sse_events("") == []

    def test_malformed_json_skipped(self):
        raw = 'data: {not valid json}\n\ndata: {"ok":true}\n\n'
        events = _parse_sse_events(raw)
        assert len(events) == 1
        assert events[0]["ok"] is True


# ═══════════════════════════════════════════════════════════════════
# Inner JSON extraction
# ═══════════════════════════════════════════════════════════════════

class TestExtractInner:
    def test_standard_mcp_content(self):
        mcp = {"content": [{"type": "text", "text": '{"ok":true,"session_id":"s1"}'}]}
        inner = _extract_inner_json(mcp)
        assert inner["ok"] is True
        assert inner["session_id"] == "s1"

    def test_non_json_text(self):
        mcp = {"content": [{"type": "text", "text": "plain text answer"}]}
        inner = _extract_inner_json(mcp)
        assert "_raw" in inner

    def test_no_content_key(self):
        mcp = {"status": "ok"}
        inner = _extract_inner_json(mcp)
        assert inner == mcp

    def test_none_input(self):
        assert _extract_inner_json(None) is None


# ═══════════════════════════════════════════════════════════════════
# Role detection
# ═══════════════════════════════════════════════════════════════════

class TestDetectRole:
    def test_planner(self):
        assert _detect_role("你是投研图谱探索规划师，请分析当前图谱") == "planner"

    def test_competitive_extractor(self):
        assert _detect_role("你是竞品分析助手，请从以下证据中提取") == "competitive_extractor"

    def test_triple_extractor(self):
        assert _detect_role("你是知识图谱构建助手") == "triple_extractor"

    def test_evaluator_cn(self):
        assert _detect_role("请评估当前研究进度") == "evaluator"

    def test_evaluator_decision(self):
        assert _detect_role("请做出决策判断") == "evaluator"

    def test_unknown(self):
        assert _detect_role("hello world") == "unknown"

    def test_sufficiency_evaluator_en(self):
        prompt = "判断这批检索结果是否足以支撑当前查询。只回答 sufficient 或 insufficient。"
        assert _detect_role(prompt) == "sufficiency_evaluator"

    def test_sufficiency_evaluator_cn(self):
        assert _detect_role("请判断检索结果质量") == "sufficiency_evaluator"

    def test_sufficiency_evaluator_keyword(self):
        assert _detect_role("Is this sufficient or insufficient?") == "sufficiency_evaluator"
