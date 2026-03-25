"""Cross-market stock screener — surfaces new opportunities beyond the watchlist.

Uses akshare spot endpoints to scan full market listings and filter by
quantitative criteria (PE, momentum, market cap, volume).

Usage::

    from finagent.market_screener import screen_market

    results = screen_market(market="CN", strategy="value")
    results = screen_market(market="HK", strategy="momentum")
    results = screen_market(market="US", strategy="growth", limit=20)

CLI::

    python -m finagent.cli market-screen --market CN --strategy value --limit 15
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Strategy filters
# ---------------------------------------------------------------------------

def _value_filter(row: dict[str, Any]) -> float | None:
    """Score stocks by value metrics: low PE + low PB = higher score."""
    pe = row.get("pe_ttm")
    pb = row.get("pb")
    if pe is None or pe <= 0 or pe > 200:
        return None
    if pb is not None and pb <= 0:
        return None
    # Simple value score: lower PE and PB = higher rank
    score = 100.0 / pe
    if pb and pb > 0:
        score += 20.0 / pb
    return score


def _momentum_filter(row: dict[str, Any]) -> float | None:
    """Score stocks by momentum: positive change + volume surge."""
    change = row.get("price_change_pct")
    volume = row.get("volume")
    if change is None:
        return None
    if change < 1.0:  # At least 1% positive movement
        return None
    score = change
    if volume and volume > 0:
        score += min(volume / 1e6, 10.0)  # Volume bonus capped
    return score


def _growth_filter(row: dict[str, Any]) -> float | None:
    """Score stocks for growth: moderate PE + strong momentum."""
    pe = row.get("pe_ttm")
    change = row.get("price_change_pct")
    market_cap = row.get("market_cap")
    if pe is None or pe <= 0 or pe > 100:
        return None
    if market_cap is not None and market_cap < 10:  # Skip micro-caps (< 10亿)
        return None
    score = 50.0 / pe  # Moderate value component
    if change and change > 0:
        score += change * 2.0  # Stronger momentum weight
    return score


def _contrarian_filter(row: dict[str, Any]) -> float | None:
    """Score stocks for contrarian: beaten-down + reasonable PE."""
    pe = row.get("pe_ttm")
    change = row.get("price_change_pct")
    if pe is None or pe <= 0 or pe > 50:
        return None
    if change is None or change > -2.0:  # Must be significantly down
        return None
    # More beaten-down + lower PE = higher score
    score = abs(change) + (30.0 / pe)
    return score


_STRATEGY_MAP: dict[str, Any] = {
    "value": _value_filter,
    "momentum": _momentum_filter,
    "growth": _growth_filter,
    "contrarian": _contrarian_filter,
}

VALID_STRATEGIES = set(_STRATEGY_MAP.keys())


# ---------------------------------------------------------------------------
# Market data helpers
# ---------------------------------------------------------------------------

def _safe_float(v: Any, *, default: float | None = None) -> float | None:
    if v is None or v == "" or v == "-":
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _lazy_ak():
    import akshare as ak  # type: ignore[import-untyped]
    return ak


def _scan_cn_market() -> list[dict[str, Any]]:
    """Fetch full A-share spot data and normalize to screening format."""
    ak = _lazy_ak()
    df = ak.stock_zh_a_spot_em()
    results = []
    for _, r in df.iterrows():
        price = _safe_float(r.get("最新价"))
        if not price or price <= 0:
            continue
        results.append({
            "ticker": str(r.get("代码", "")),
            "name": str(r.get("名称", "")),
            "market": "CN",
            "price": price,
            "market_cap": round(_safe_float(r.get("总市值"), default=0) / 1e8, 2),
            "pe_ttm": _safe_float(r.get("市盈率-动态")),
            "pb": _safe_float(r.get("市净率")),
            "volume": _safe_float(r.get("成交量")),
            "price_change_pct": _safe_float(r.get("涨跌幅")),
            "turnover_rate": _safe_float(r.get("换手率")),
        })
    return results


def _scan_hk_market() -> list[dict[str, Any]]:
    """Fetch HK spot data and normalize."""
    ak = _lazy_ak()
    df = ak.stock_hk_spot_em()
    results = []
    for _, r in df.iterrows():
        price = _safe_float(r.get("最新价"))
        if not price or price <= 0:
            continue
        results.append({
            "ticker": str(r.get("代码", "")),
            "name": str(r.get("名称", "")),
            "market": "HK",
            "price": price,
            "market_cap": round(_safe_float(r.get("总市值"), default=0) / 1e8, 2),
            "pe_ttm": _safe_float(r.get("市盈率")),
            "pb": None,
            "volume": _safe_float(r.get("成交量")),
            "price_change_pct": _safe_float(r.get("涨跌幅")),
            "turnover_rate": None,
        })
    return results


def _scan_us_market() -> list[dict[str, Any]]:
    """Fetch US spot data and normalize."""
    ak = _lazy_ak()
    df = ak.stock_us_spot_em()
    results = []
    for _, r in df.iterrows():
        price = _safe_float(r.get("最新价", r.get("price")))
        if not price or price <= 0:
            continue
        code = str(r.get("代码", r.get("code", "")))
        # akshare US codes are like "105.NVDA" — extract the symbol
        symbol = code.split(".")[-1] if "." in code else code
        results.append({
            "ticker": symbol,
            "name": str(r.get("名称", r.get("name", ""))),
            "market": "US",
            "price": price,
            "market_cap": round(_safe_float(r.get("总市值", r.get("market_cap")), default=0) / 1e8, 2),
            "pe_ttm": _safe_float(r.get("市盈率", r.get("pe"))),
            "pb": None,
            "volume": _safe_float(r.get("成交量")),
            "price_change_pct": _safe_float(r.get("涨跌幅", r.get("change_pct"))),
            "turnover_rate": None,
        })
    return results


_MARKET_SCANNERS = {
    "CN": _scan_cn_market,
    "HK": _scan_hk_market,
    "US": _scan_us_market,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def screen_market(
    *,
    market: str = "CN",
    strategy: str = "value",
    limit: int = 15,
    min_market_cap: float = 10.0,  # 亿
) -> dict[str, Any]:
    """Screen a market for opportunities using a quantitative strategy.

    Args:
        market: "CN" | "HK" | "US"
        strategy: "value" | "momentum" | "growth" | "contrarian"
        limit: Max number of results
        min_market_cap: Minimum market cap in 亿 (default 10)

    Returns:
        {
            "ok": True,
            "market": "CN",
            "strategy": "value",
            "candidates": [...],
            "scanned_count": 5000,
            "elapsed_ms": 1234.5,
        }
    """
    market = market.upper().strip()
    strategy = strategy.lower().strip()

    if market not in _MARKET_SCANNERS:
        raise ValueError(f"Invalid market '{market}'. Must be one of {set(_MARKET_SCANNERS.keys())}")
    if strategy not in _STRATEGY_MAP:
        raise ValueError(f"Invalid strategy '{strategy}'. Must be one of {VALID_STRATEGIES}")

    t0 = time.monotonic()

    # Fetch full market data
    scanner = _MARKET_SCANNERS[market]
    all_stocks = scanner()
    scanned_count = len(all_stocks)

    # Apply market cap filter
    if min_market_cap > 0:
        all_stocks = [s for s in all_stocks if (s.get("market_cap") or 0) >= min_market_cap]

    # Score and rank
    filter_fn = _STRATEGY_MAP[strategy]
    scored: list[tuple[float, dict[str, Any]]] = []
    for stock in all_stocks:
        score = filter_fn(stock)
        if score is not None and score > 0:
            scored.append((score, stock))

    # Sort by score descending
    scored.sort(key=lambda x: x[0], reverse=True)

    # Build results
    candidates = []
    for score, stock in scored[:limit]:
        candidates.append({
            **stock,
            "screen_score": round(score, 3),
            "strategy": strategy,
        })

    elapsed = time.monotonic() - t0
    return {
        "ok": True,
        "market": market,
        "strategy": strategy,
        "candidates": candidates,
        "scanned_count": scanned_count,
        "passed_filter_count": len(scored),
        "returned_count": len(candidates),
        "elapsed_ms": round(elapsed * 1000, 1),
    }


def screen_all_markets(
    *,
    strategy: str = "value",
    limit_per_market: int = 10,
    min_market_cap: float = 10.0,
    markets: list[str] | None = None,
) -> dict[str, Any]:
    """Screen multiple markets and combine results.

    Args:
        strategy: Screening strategy to apply
        limit_per_market: Max candidates per market
        min_market_cap: Minimum market cap filter
        markets: Markets to scan (default: ["CN", "HK", "US"])

    Returns:
        Combined results with per-market breakdown.
    """
    if markets is None:
        markets = ["CN", "HK", "US"]

    t0 = time.monotonic()
    all_candidates: list[dict[str, Any]] = []
    market_results: dict[str, Any] = {}

    for mkt in markets:
        try:
            result = screen_market(
                market=mkt,
                strategy=strategy,
                limit=limit_per_market,
                min_market_cap=min_market_cap,
            )
            all_candidates.extend(result["candidates"])
            market_results[mkt] = {
                "ok": True,
                "scanned": result["scanned_count"],
                "passed": result["passed_filter_count"],
                "returned": result["returned_count"],
            }
        except Exception as exc:
            logger.warning("Failed to screen %s: %s", mkt, exc)
            market_results[mkt] = {"ok": False, "error": str(exc)}

    # Sort all candidates by score
    all_candidates.sort(key=lambda x: x.get("screen_score", 0), reverse=True)

    elapsed = time.monotonic() - t0
    return {
        "ok": True,
        "strategy": strategy,
        "markets_scanned": list(market_results.keys()),
        "market_results": market_results,
        "candidates": all_candidates,
        "total_candidates": len(all_candidates),
        "elapsed_ms": round(elapsed * 1000, 1),
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def cli_market_screen(args: Any) -> None:
    """CLI handler for ``finagent market-screen``."""
    market = str(getattr(args, "market", "CN")).strip().upper()
    strategy = str(getattr(args, "strategy", "value")).strip().lower()
    limit = int(getattr(args, "limit", 15))
    min_cap = float(getattr(args, "min_market_cap", 10.0))
    all_markets = getattr(args, "all_markets", False)

    try:
        if all_markets:
            result = screen_all_markets(
                strategy=strategy,
                limit_per_market=limit,
                min_market_cap=min_cap,
            )
        else:
            result = screen_market(
                market=market,
                strategy=strategy,
                limit=limit,
                min_market_cap=min_cap,
            )
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
