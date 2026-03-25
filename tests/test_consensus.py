"""Tests for finagent.consensus."""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from finagent.consensus import (
    consensus_divergence,
    fetch_consensus_cn,
    fetch_consensus_estimates,
    fetch_consensus_hk,
    fetch_consensus_us,
)


class TestFetchConsensusEstimates:
    def test_unsupported_market(self):
        with pytest.raises(ValueError, match="Unsupported market"):
            fetch_consensus_estimates("AAPL", "JP")

    def test_hk_stub(self):
        result = fetch_consensus_hk("00700.HK")
        assert result["market"] == "HK"
        assert result["source"] == "stub"

    def test_us_stub(self):
        result = fetch_consensus_us("NVDA")
        assert result["market"] == "US"
        assert result["source"] == "stub"


class TestFetchConsensusCN:
    @patch("finagent.consensus._AKSHARE_AVAILABLE", False)
    def test_no_akshare(self):
        result = fetch_consensus_cn("002025")
        assert result is None

    @patch("finagent.consensus._AKSHARE_AVAILABLE", True)
    @patch("finagent.consensus.ak")
    def test_with_mock_akshare(self, mock_ak):
        import pandas as pd

        mock_ak.stock_profit_forecast_em.return_value = pd.DataFrame(
            [{"预测每股收益": 1.5, "预测营业收入": 100.0, "预测机构数": 15}]
        )
        mock_ak.stock_comment_em.return_value = pd.DataFrame(
            [{"代码": "002025", "综合得分": 8.5, "买入": 10, "增持": 3, "中性": 2, "减持": 0, "卖出": 0}]
        )

        result = fetch_consensus_cn("002025.SZ")
        assert result is not None
        assert result["consensus_eps"] == 1.5
        assert result["consensus_revenue"] == 100.0
        assert result["analyst_count"] == 15
        assert result["buy_count"] == 13  # 10 + 3
        assert result["comprehensive_score"] == 8.5


class TestConsensusDivergence:
    def test_no_consensus(self):
        result = consensus_divergence("buy", None)
        assert result["has_consensus"] is False

    def test_no_coverage(self):
        result = consensus_divergence("buy", {"buy_count": 0, "hold_count": 0, "sell_count": 0})
        assert result["has_consensus"] is False

    def test_contrarian_bullish(self):
        consensus = {"buy_count": 2, "hold_count": 1, "sell_count": 7}
        result = consensus_divergence("看多 accumulate", consensus)
        assert result["divergence_type"] == "contrarian_bullish"

    def test_contrarian_bearish(self):
        consensus = {"buy_count": 8, "hold_count": 1, "sell_count": 1}
        result = consensus_divergence("减持 reduce position", consensus)
        assert result["divergence_type"] == "contrarian_bearish"

    def test_consensus_bullish(self):
        consensus = {"buy_count": 8, "hold_count": 1, "sell_count": 1}
        result = consensus_divergence("增持 accumulate", consensus)
        assert result["divergence_type"] == "consensus_bullish"

    def test_aligned(self):
        consensus = {"buy_count": 3, "hold_count": 4, "sell_count": 3}
        result = consensus_divergence("monitoring", consensus)
        assert result["divergence_type"] == "aligned"
