"""Sell-side consensus estimates — fetch analyst ratings and profit forecasts.

Provides a consensus anchor so finbot can evaluate whether its thesis
aligns with or diverges from market consensus. When finbot's judgment
differs from consensus, the system flags "where you disagree with the
market" — a key investment signal.

Uses akshare for A-share data. HK/US stubs return None (can be expanded).
"""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

_AKSHARE_AVAILABLE = False
try:
    import akshare as ak  # type: ignore[import-untyped]

    _AKSHARE_AVAILABLE = True
except ImportError:
    ak = None  # type: ignore[assignment]


def _safe_float(val: Any, default: float | None = None) -> float | None:
    """Convert value to float, returning default on failure."""
    if val is None:
        return default
    try:
        f = float(val)
        if f != f:  # NaN check
            return default
        return f
    except (ValueError, TypeError):
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    """Convert value to int, returning default on failure."""
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def fetch_consensus_cn(ticker: str) -> dict[str, Any] | None:
    """Fetch sell-side consensus for A-shares via akshare.

    Uses:
    - ak.stock_profit_forecast_em() for EPS/revenue forecasts
    - ak.stock_comment_em() for analyst ratings

    Args:
        ticker: A-share ticker code, e.g. "002025" (without .SZ suffix).

    Returns:
        Consensus dict or None if data unavailable.
    """
    if not _AKSHARE_AVAILABLE:
        log.warning("akshare not available — cannot fetch consensus")
        return None

    code = ticker.split(".")[0]  # Strip .SZ / .SH suffix

    result: dict[str, Any] = {
        "ticker": ticker,
        "market": "CN",
        "consensus_eps": None,
        "consensus_revenue": None,
        "consensus_target_price": None,
        "analyst_count": 0,
        "buy_count": 0,
        "hold_count": 0,
        "sell_count": 0,
        "comprehensive_score": None,
        "source": "akshare",
    }

    # Profit forecast
    try:
        df = ak.stock_profit_forecast_em(symbol=code)
        if df is not None and not df.empty:
            # Use the first (current year) forecast
            row = df.iloc[0]
            # Column names vary by akshare version; try common ones
            for eps_col in ["预测每股收益", "每股收益", "EPS"]:
                if eps_col in df.columns:
                    result["consensus_eps"] = _safe_float(row.get(eps_col))
                    break
            for rev_col in ["预测营业收入", "营业收入", "营收"]:
                if rev_col in df.columns:
                    result["consensus_revenue"] = _safe_float(row.get(rev_col))
                    break
            result["analyst_count"] = _safe_int(row.get("预测机构数", row.get("机构数", 0)))
    except Exception as exc:
        log.debug("Profit forecast fetch failed for %s: %s", code, exc)

    # Analyst comments / ratings
    try:
        df = ak.stock_comment_em()
        if df is not None and not df.empty:
            # Filter to our stock
            match_rows = df[df["代码"].astype(str) == code]
            if not match_rows.empty:
                row = match_rows.iloc[0]
                result["comprehensive_score"] = _safe_float(row.get("综合得分"))
                # Buy/hold/sell counts from rating columns
                for col in ["买入", "增持"]:
                    if col in df.columns:
                        result["buy_count"] += _safe_int(row.get(col))
                for col in ["中性", "持有"]:
                    if col in df.columns:
                        result["hold_count"] += _safe_int(row.get(col))
                for col in ["减持", "卖出"]:
                    if col in df.columns:
                        result["sell_count"] += _safe_int(row.get(col))
    except Exception as exc:
        log.debug("Analyst comment fetch failed for %s: %s", code, exc)

    return result


def fetch_consensus_hk(ticker: str) -> dict[str, Any] | None:
    """Stub for HK consensus — to be expanded with HK data sources."""
    return {
        "ticker": ticker,
        "market": "HK",
        "consensus_eps": None,
        "consensus_revenue": None,
        "consensus_target_price": None,
        "analyst_count": 0,
        "buy_count": 0,
        "hold_count": 0,
        "sell_count": 0,
        "source": "stub",
    }


def fetch_consensus_us(ticker: str) -> dict[str, Any] | None:
    """Stub for US consensus — to be expanded with US data sources."""
    return {
        "ticker": ticker,
        "market": "US",
        "consensus_eps": None,
        "consensus_revenue": None,
        "consensus_target_price": None,
        "analyst_count": 0,
        "buy_count": 0,
        "hold_count": 0,
        "sell_count": 0,
        "source": "stub",
    }


MARKET_DISPATCHERS = {
    "CN": fetch_consensus_cn,
    "HK": fetch_consensus_hk,
    "US": fetch_consensus_us,
}


def fetch_consensus_estimates(ticker: str, market: str) -> dict[str, Any] | None:
    """Fetch sell-side consensus estimates for any supported market.

    Args:
        ticker: Ticker symbol (e.g. "002025.SZ", "00700.HK", "NVDA").
        market: Market identifier ("CN", "HK", "US").

    Returns:
        Consensus dict or None if market unsupported.

    Raises:
        ValueError: If market is not supported.
    """
    market = market.upper()
    dispatcher = MARKET_DISPATCHERS.get(market)
    if dispatcher is None:
        raise ValueError(f"Unsupported market: {market!r}. Supported: {sorted(MARKET_DISPATCHERS)}")
    return dispatcher(ticker)


def consensus_divergence(
    finbot_decision: str,
    consensus: dict[str, Any] | None,
) -> dict[str, Any]:
    """Analyze divergence between finbot's thesis and market consensus.

    Returns a divergence assessment that can be injected into decision lane prompts.
    """
    if consensus is None:
        return {"has_consensus": False, "divergence_note": "No consensus data available"}

    total = consensus.get("buy_count", 0) + consensus.get("hold_count", 0) + consensus.get("sell_count", 0)
    if total == 0:
        return {"has_consensus": False, "divergence_note": "No analyst coverage"}

    buy_pct = consensus.get("buy_count", 0) / total * 100
    sell_pct = consensus.get("sell_count", 0) / total * 100

    # Simple divergence detection
    decision_lower = (finbot_decision or "").lower()
    is_bullish = any(w in decision_lower for w in ["buy", "accumulate", "看多", "加仓", "增持", "建仓"])
    is_bearish = any(w in decision_lower for w in ["sell", "reduce", "看空", "减仓", "减持", "回避"])

    divergence_type = "aligned"
    if is_bullish and sell_pct > 30:
        divergence_type = "contrarian_bullish"
    elif is_bearish and buy_pct > 70:
        divergence_type = "contrarian_bearish"
    elif is_bullish and buy_pct > 70:
        divergence_type = "consensus_bullish"
    elif is_bearish and sell_pct > 30:
        divergence_type = "consensus_bearish"

    return {
        "has_consensus": True,
        "analyst_count": total,
        "buy_pct": round(buy_pct, 1),
        "sell_pct": round(sell_pct, 1),
        "divergence_type": divergence_type,
        "divergence_note": _divergence_note(divergence_type, buy_pct, sell_pct, total),
    }


def _divergence_note(divergence_type: str, buy_pct: float, sell_pct: float, total: int) -> str:
    """Generate human-readable divergence note."""
    if divergence_type == "contrarian_bullish":
        return f"你的看多判断与市场共识存在分歧：{total}位分析师中有{sell_pct:.0f}%给出卖出评级。请确认你的独立判断依据。"
    if divergence_type == "contrarian_bearish":
        return f"你的看空判断与市场共识存在分歧：{total}位分析师中有{buy_pct:.0f}%给出买入评级。请确认你的风险评估。"
    if divergence_type == "consensus_bullish":
        return f"你的看法与市场共识一致（{buy_pct:.0f}%看多）。注意拥挤交易风险。"
    if divergence_type == "consensus_bearish":
        return f"你的看法与市场共识一致（{sell_pct:.0f}%看空）。注意超卖反弹风险。"
    return f"共{total}位分析师覆盖：买入{buy_pct:.0f}% / 卖出{sell_pct:.0f}%"
