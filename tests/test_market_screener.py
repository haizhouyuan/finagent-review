"""Tests for finagent.market_screener — cross-market stock screener."""

from __future__ import annotations

import pytest
from finagent.market_screener import (
    _value_filter,
    _momentum_filter,
    _growth_filter,
    _contrarian_filter,
    screen_market,
    screen_all_markets,
    VALID_STRATEGIES,
)


# ---------------------------------------------------------------------------
# Strategy filter unit tests
# ---------------------------------------------------------------------------

class TestValueFilter:
    def test_low_pe_scores_high(self) -> None:
        score = _value_filter({"pe_ttm": 5.0, "pb": 1.0})
        assert score is not None
        assert score > 0

    def test_high_pe_scores_low(self) -> None:
        low = _value_filter({"pe_ttm": 50.0, "pb": 3.0})
        high = _value_filter({"pe_ttm": 5.0, "pb": 1.0})
        assert low is not None and high is not None
        assert high > low

    def test_negative_pe_rejected(self) -> None:
        assert _value_filter({"pe_ttm": -5.0, "pb": 1.0}) is None

    def test_none_pe_rejected(self) -> None:
        assert _value_filter({"pe_ttm": None, "pb": 1.0}) is None

    def test_zero_pe_rejected(self) -> None:
        assert _value_filter({"pe_ttm": 0, "pb": 1.0}) is None

    def test_extreme_pe_rejected(self) -> None:
        assert _value_filter({"pe_ttm": 300, "pb": 1.0}) is None


class TestMomentumFilter:
    def test_positive_change_accepted(self) -> None:
        score = _momentum_filter({"price_change_pct": 5.0, "volume": 1e7})
        assert score is not None and score > 0

    def test_weak_change_rejected(self) -> None:
        assert _momentum_filter({"price_change_pct": 0.5}) is None

    def test_none_change_rejected(self) -> None:
        assert _momentum_filter({"price_change_pct": None}) is None


class TestGrowthFilter:
    def test_moderate_pe_with_momentum(self) -> None:
        score = _growth_filter({"pe_ttm": 20.0, "price_change_pct": 3.0, "market_cap": 50.0})
        assert score is not None and score > 0

    def test_micro_cap_rejected(self) -> None:
        assert _growth_filter({"pe_ttm": 20.0, "price_change_pct": 3.0, "market_cap": 5.0}) is None


class TestContrarianFilter:
    def test_beaten_down_accepted(self) -> None:
        score = _contrarian_filter({"pe_ttm": 10.0, "price_change_pct": -5.0})
        assert score is not None and score > 0

    def test_positive_change_rejected(self) -> None:
        assert _contrarian_filter({"pe_ttm": 10.0, "price_change_pct": 2.0}) is None


# ---------------------------------------------------------------------------
# Integration-level tests (with mocked akshare)
# ---------------------------------------------------------------------------

class TestScreenMarket:
    def test_invalid_market_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid market"):
            screen_market(market="INVALID", strategy="value")

    def test_invalid_strategy_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid strategy"):
            screen_market(market="CN", strategy="nonexistent")

    def test_valid_strategies(self) -> None:
        assert "value" in VALID_STRATEGIES
        assert "momentum" in VALID_STRATEGIES
        assert "growth" in VALID_STRATEGIES
        assert "contrarian" in VALID_STRATEGIES
