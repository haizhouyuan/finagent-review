"""Tests for fetch_historical and openbb_mcp_server.

Addresses Codex review finding: new code had zero test coverage.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from unittest import mock

import pytest
import pandas as pd

from finagent.market_data import (
    fetch_historical,
    fetch_market_snapshot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_cn_hist_df():
    return pd.DataFrame([
        {"日期": "2026-03-18", "开盘": 107.49, "最高": 111.86,
         "最低": 105.50, "收盘": 111.86, "成交量": 58460},
        {"日期": "2026-03-19", "开盘": 108.65, "最高": 117.33,
         "最低": 108.52, "收盘": 113.76, "成交量": 85444},
    ])


@contextmanager
def _noop_ctx():
    """No-op context manager to replace _ak_connection_ctx in tests."""
    yield


# ---------------------------------------------------------------------------
# fetch_historical tests
# ---------------------------------------------------------------------------

class TestFetchHistoricalCN:
    @mock.patch("finagent.market_data._ak_connection_ctx", _noop_ctx)
    @mock.patch("finagent.market_data._lazy_ak")
    def test_cn_historical_basic(self, mock_lazy_ak):
        fake_ak = mock_lazy_ak.return_value
        fake_ak.stock_zh_a_hist.return_value = _mock_cn_hist_df()

        result = fetch_historical("001270.SZ", market="CN", period="daily")

        assert len(result) == 2
        assert result[0]["date"] == "2026-03-18"
        assert result[0]["close"] == 111.86
        assert result[1]["volume"] == 85444
        assert "open" in result[0]
        assert "high" in result[0]
        assert "low" in result[0]


class TestFetchHistoricalInvalid:
    def test_invalid_market_raises(self):
        with pytest.raises(ValueError, match="Invalid market"):
            fetch_historical("AAPL", market="JP")


class TestFetchHistoricalUSNoYfinance:
    @mock.patch("finagent.market_data._lazy_yf", side_effect=ImportError("No module named 'yfinance'"))
    def test_us_without_yfinance_gives_clear_error(self, _):
        with pytest.raises(ValueError, match="requires yfinance"):
            fetch_historical("RKLB", market="US")


# ---------------------------------------------------------------------------
# MCP server tests
# ---------------------------------------------------------------------------

class TestMCPToolsList:
    def test_tools_list_returns_3_tools(self):
        from finagent.openbb_mcp_server import _TOOLS
        assert len(_TOOLS) == 3
        names = {t["name"] for t in _TOOLS}
        assert names == {"market_snapshot", "market_batch_snapshot", "stock_historical"}

    def test_all_tools_have_required_fields(self):
        from finagent.openbb_mcp_server import _TOOLS
        for tool in _TOOLS:
            assert "name" in tool
            assert "description" in tool
            assert "inputSchema" in tool
            assert tool["inputSchema"]["type"] == "object"


class TestMCPHandleToolCall:
    @mock.patch("finagent.market_data.fetch_market_snapshot")
    def test_market_snapshot_dispatch(self, mock_snap):
        mock_snap.return_value = {
            "ticker": "002025.SZ", "name": "航天电器", "market": "CN",
            "currency": "CNY", "price": 38.5, "market_cap": 230.0,
            "pe_ttm": 42.1, "pb": 5.8, "ps_ttm": None, "ev_ebitda": None,
            "volume": 1200000, "price_change_pct": 2.35,
            "snapshot_at": "2026-03-20T00:00:00Z",
            "source": "test", "freshness": "realtime",
            "fetch_elapsed_ms": 0.1,
        }

        from finagent.openbb_mcp_server import _handle_tool_call
        result = _handle_tool_call("market_snapshot", {
            "ticker": "002025.SZ", "market": "CN"
        })

        assert result["price"] == 38.5
        assert result["market"] == "CN"
        mock_snap.assert_called_once_with("002025.SZ", market="CN")

    @mock.patch("finagent.market_data._ak_connection_ctx", _noop_ctx)
    @mock.patch("finagent.market_data._lazy_ak")
    def test_stock_historical_dispatch(self, mock_lazy_ak):
        fake_ak = mock_lazy_ak.return_value
        fake_ak.stock_zh_a_hist.return_value = _mock_cn_hist_df()

        from finagent.openbb_mcp_server import _handle_tool_call
        result = _handle_tool_call("stock_historical", {
            "ticker": "001270.SZ", "market": "CN", "period": "daily"
        })

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["close"] == 111.86

    def test_unknown_tool_raises(self):
        from finagent.openbb_mcp_server import _handle_tool_call
        with pytest.raises(ValueError, match="Unknown tool"):
            _handle_tool_call("nonexistent_tool", {})


class TestMCPProtocol:
    """Test the JSON-RPC message helpers."""

    def test_make_response(self):
        from finagent.openbb_mcp_server import _make_response
        resp = _make_response(42, {"foo": "bar"})
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 42
        assert resp["result"] == {"foo": "bar"}

    def test_make_error(self):
        from finagent.openbb_mcp_server import _make_error
        resp = _make_error(1, -32601, "Method not found")
        assert resp["error"]["code"] == -32601
        assert resp["error"]["message"] == "Method not found"


class TestMCPNoOpenBBDependency:
    """Verify MCP server imports without openbb or yfinance."""

    def test_import_succeeds_without_optional_deps(self):
        """The module should load even if openbb/yfinance are not installed."""
        import importlib
        mod = importlib.import_module("finagent.openbb_mcp_server")
        assert hasattr(mod, "main")
        assert hasattr(mod, "_TOOLS")
        assert len(mod._TOOLS) == 3
