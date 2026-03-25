"""Tests for finagent.market_data — unified market data adapter."""

from __future__ import annotations

import contextlib
import json
from types import SimpleNamespace
from unittest import mock

import pytest
import pandas as pd

from finagent.market_data import (
    _normalize_cn_ticker,
    _normalize_hk_ticker,
    _normalize_us_ticker,
    _safe_float,
    fetch_market_snapshot,
    fetch_multi_snapshots,
    _spot_cache,
)


@pytest.fixture(autouse=True)
def _clear_spot_cache():
    """Clear the spot data cache before each test to avoid stale data."""
    _spot_cache.clear()
    yield
    _spot_cache.clear()


# ---------------------------------------------------------------------------
# Ticker normalisation
# ---------------------------------------------------------------------------

class TestTickerNormalisation:
    def test_cn_strip_sz_suffix(self):
        assert _normalize_cn_ticker("002025.SZ") == "002025"

    def test_cn_strip_sh_suffix(self):
        assert _normalize_cn_ticker("600519.SH") == "600519"

    def test_cn_strip_bj_suffix(self):
        assert _normalize_cn_ticker("430047.BJ") == "430047"

    def test_cn_case_insensitive(self):
        assert _normalize_cn_ticker("002025.sz") == "002025"

    def test_cn_bare_code_passthrough(self):
        assert _normalize_cn_ticker("002025") == "002025"

    def test_hk_strip_suffix_pad(self):
        assert _normalize_hk_ticker("700.HK") == "00700"

    def test_hk_five_digit(self):
        assert _normalize_hk_ticker("09988.HK") == "09988"

    def test_us_uppercase(self):
        assert _normalize_us_ticker("nvda") == "NVDA"
        assert _normalize_us_ticker("  tsla ") == "TSLA"


class TestSafeFloat:
    def test_normal(self):
        assert _safe_float(42.5) == 42.5

    def test_string(self):
        assert _safe_float("3.14") == 3.14

    def test_dash(self):
        assert _safe_float("-") is None

    def test_empty(self):
        assert _safe_float("") is None

    def test_none(self):
        assert _safe_float(None) is None

    def test_with_default(self):
        assert _safe_float(None, default=0.0) == 0.0


# ---------------------------------------------------------------------------
# fetch_market_snapshot with mocked akshare
# ---------------------------------------------------------------------------

def _mock_cn_spot_df():
    return pd.DataFrame([
        {
            "代码": "002025",
            "名称": "航天电器",
            "最新价": 38.5,
            "总市值": 2.3e10,
            "市盈率-动态": 42.1,
            "市净率": 5.8,
            "成交量": 1200000,
            "涨跌幅": 2.35,
        }
    ])


def _mock_hk_spot_df():
    return pd.DataFrame([
        {
            "代码": "00700",
            "名称": "腾讯控股",
            "最新价": 380.0,
            "总市值": 3.6e12,
            "市盈率": 22.5,
            "涨跌幅": -0.8,
        }
    ])


def _mock_us_spot_df():
    return pd.DataFrame([
        {
            "代码": "105.NVDA",
            "名称": "英伟达",
            "最新价": 880.0,
            "总市值": 2.15e12,
            "市盈率": 65.0,
            "涨跌幅": 1.2,
        }
    ])


class TestFetchCNSnapshot:
    @mock.patch("finagent.market_data._fetch_cn_snapshot_sina", side_effect=ConnectionError("test"))
    @mock.patch("finagent.market_data._ak_connection_ctx", return_value=contextlib.nullcontext())
    @mock.patch("finagent.market_data._patch_akshare_session")
    @mock.patch("finagent.market_data._lazy_ak")
    def test_basic_cn_snapshot(self, mock_lazy_ak, _mock_patch, _mock_ctx, _mock_sina):
        fake_ak = mock_lazy_ak.return_value
        fake_ak.stock_zh_a_spot_em.return_value = _mock_cn_spot_df()

        snap = fetch_market_snapshot("002025.SZ", market="CN")

        assert snap["ticker"] == "002025.SZ"
        assert snap["market"] == "CN"
        assert snap["currency"] == "CNY"
        assert snap["price"] == 38.5
        assert snap["market_cap"] == 230.0  # 2.3e10 / 1e8 = 230 亿元
        assert snap["pe_ttm"] == 42.1
        assert snap["pb"] == 5.8
        assert snap["price_change_pct"] == 2.35
        assert snap["source"] == "akshare/eastmoney"
        assert snap["freshness"] == "realtime"
        assert "snapshot_at" in snap
        assert "fetch_elapsed_ms" in snap


class TestFetchHKSnapshot:
    @mock.patch("finagent.market_data._ak_connection_ctx", return_value=contextlib.nullcontext())
    @mock.patch("finagent.market_data._patch_akshare_session")
    @mock.patch("finagent.market_data._lazy_ak")
    def test_basic_hk_snapshot(self, mock_lazy_ak, _mock_patch, _mock_ctx):
        fake_ak = mock_lazy_ak.return_value
        fake_ak.stock_hk_spot_em.return_value = _mock_hk_spot_df()

        snap = fetch_market_snapshot("700.HK", market="HK")

        assert snap["ticker"] == "700.HK"
        assert snap["market"] == "HK"
        assert snap["currency"] == "HKD"
        assert snap["price"] == 380.0
        assert snap["market_cap"] == 36000.0  # 3.6e12 / 1e8
        assert snap["pe_ttm"] == 22.5


class TestFetchUSSnapshot:
    @mock.patch("finagent.market_data._lazy_ak")
    def test_basic_us_snapshot(self, mock_lazy_ak):
        fake_ak = mock_lazy_ak.return_value
        fake_ak.stock_us_spot_em.return_value = _mock_us_spot_df()

        snap = fetch_market_snapshot("NVDA", market="US")

        assert snap["ticker"] == "NVDA"
        assert snap["market"] == "US"
        assert snap["currency"] == "USD"
        assert snap["price"] == 880.0
        assert snap["pe_ttm"] == 65.0


class TestInvalidMarket:
    def test_invalid_market_raises(self):
        with pytest.raises(ValueError, match="Invalid market"):
            fetch_market_snapshot("AAPL", market="JP")


class TestFetchMultiSnapshots:
    @mock.patch("finagent.market_data._fetch_cn_snapshot_sina", side_effect=ConnectionError("test"))
    @mock.patch("finagent.market_data._ak_connection_ctx", return_value=contextlib.nullcontext())
    @mock.patch("finagent.market_data._patch_akshare_session")
    @mock.patch("finagent.market_data._lazy_ak")
    def test_multi_with_mixed_results(self, mock_lazy_ak, _mock_patch, _mock_ctx, _mock_sina):
        fake_ak = mock_lazy_ak.return_value
        fake_ak.stock_zh_a_spot_em.return_value = _mock_cn_spot_df()
        fake_ak.stock_hk_spot_em.return_value = pd.DataFrame()  # empty → error

        results = fetch_multi_snapshots([
            {"ticker": "002025.SZ", "market": "CN"},
            {"ticker": "99999.HK", "market": "HK"},
        ])

        assert len(results) == 2
        assert results[0]["price"] == 38.5
        assert "error" in results[1]

    def test_missing_ticker(self):
        results = fetch_multi_snapshots([{"ticker": "", "market": "CN"}])
        assert results[0]["error"] == "missing_ticker_or_market"
