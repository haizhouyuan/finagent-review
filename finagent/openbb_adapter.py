"""OpenBB + AKShare unified data adapter for finagent.

Wraps the OpenBB SDK (``from openbb import obb``) behind simple function
calls that return plain dicts / lists.  If OpenBB is not installed or
the call fails we fall back to direct akshare where possible.

Usage::

    from finagent.openbb_adapter import fetch_financials, fetch_key_metrics

All heavy imports (``openbb``, ``akshare``) are lazy to keep module load fast.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy import helpers
# ---------------------------------------------------------------------------

_obb = None  # cached openbb.obb singleton


def _lazy_obb():
    """Return the global ``obb`` object, importing on first call."""
    global _obb
    if _obb is None:
        from openbb import obb  # type: ignore[import-untyped]
        _obb = obb
    return _obb


def _lazy_ak():
    import akshare as ak  # type: ignore[import-untyped]
    return ak


def _safe(v: Any, *, default=None):
    """Coerce value to a safe Python scalar."""
    if v is None or v == "" or v == "-":
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return v


def _result_to_dicts(result) -> list[dict[str, Any]]:
    """Convert OBBject.results to plain list[dict]."""
    out: list[dict[str, Any]] = []
    for item in (result.results or []):
        if hasattr(item, "model_dump"):
            d = item.model_dump()
        elif hasattr(item, "__dict__"):
            d = dict(item.__dict__)
        else:
            d = dict(item)
        # Convert date objects to ISO strings
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = v.isoformat()
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_financials(
    ticker: str,
    *,
    market: str = "CN",
    statement: str = "income",
    period: str = "annual",
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Fetch financial statements (income / balance_sheet / cash_flow).

    Args:
        ticker: Stock code (e.g. ``"000002"`` for A-share)
        market: ``"CN"`` | ``"HK"`` | ``"US"``
        statement: ``"income"`` | ``"balance_sheet"`` | ``"cash_flow"``
        period: ``"annual"`` | ``"quarter"``
        limit: Number of periods to return

    Returns:
        List of dicts with financial line items.
    """
    obb = _lazy_obb()
    sym = _normalize_symbol(ticker, market)

    func_map = {
        "income": obb.equity.fundamental.income,
        "balance_sheet": obb.equity.fundamental.balance,
        "cash_flow": obb.equity.fundamental.cash,
    }
    func = func_map.get(statement)
    if func is None:
        raise ValueError(f"Invalid statement type: {statement}")

    result = func(symbol=sym, provider="akshare", period=period, limit=limit)
    return _result_to_dicts(result)


def fetch_key_metrics(
    ticker: str,
    *,
    market: str = "CN",
) -> dict[str, Any]:
    """Fetch key financial metrics (PE, PB, PS, ROE, ROA, etc.).

    Returns:
        Dict with metrics, or empty dict on failure.
    """
    obb = _lazy_obb()
    sym = _normalize_symbol(ticker, market)

    try:
        result = obb.equity.fundamental.metrics(symbol=sym, provider="akshare")
        items = _result_to_dicts(result)
        return items[0] if items else {}
    except Exception as exc:
        logger.warning("key_metrics via OpenBB failed for %s: %s", ticker, exc)
        return {}


def fetch_company_news(
    ticker: str,
    *,
    market: str = "CN",
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Fetch recent company news.

    Returns:
        List of dicts with ``title``, ``date``, ``url``, ``source`` keys.
    """
    obb = _lazy_obb()
    sym = _normalize_symbol(ticker, market)

    try:
        result = obb.news.company(symbol=sym, provider="akshare", limit=limit)
        return _result_to_dicts(result)
    except Exception as exc:
        logger.warning("company_news via OpenBB failed for %s: %s", ticker, exc)
        return []


def fetch_company_profile(
    ticker: str,
    *,
    market: str = "CN",
) -> dict[str, Any]:
    """Fetch company profile / basic info.

    Returns:
        Dict with company details (name, industry, description, etc.).
    """
    obb = _lazy_obb()
    sym = _normalize_symbol(ticker, market)

    try:
        result = obb.equity.profile(symbol=sym, provider="akshare")
        items = _result_to_dicts(result)
        return items[0] if items else {}
    except Exception as exc:
        logger.warning("company_profile via OpenBB failed for %s: %s", ticker, exc)
        return {}


def fetch_dividends(
    ticker: str,
    *,
    market: str = "CN",
) -> list[dict[str, Any]]:
    """Fetch historical dividend records.

    Returns:
        List of dicts with dividend data.
    """
    obb = _lazy_obb()
    sym = _normalize_symbol(ticker, market)

    try:
        result = obb.equity.fundamental.dividends(symbol=sym, provider="akshare")
        return _result_to_dicts(result)
    except Exception as exc:
        logger.warning("dividends via OpenBB failed for %s: %s", ticker, exc)
        return []


def fetch_equity_quote(
    ticker: str,
    *,
    market: str = "CN",
) -> dict[str, Any]:
    """Fetch real-time equity quote via OpenBB.

    Returns:
        Dict with quote data (price, volume, change, etc.).
    """
    obb = _lazy_obb()
    sym = _normalize_symbol(ticker, market)

    try:
        result = obb.equity.price.quote(symbol=sym, provider="akshare")
        items = _result_to_dicts(result)
        return items[0] if items else {}
    except Exception as exc:
        logger.warning("equity_quote via OpenBB failed for %s: %s", ticker, exc)
        return {}


def fetch_price_performance(
    ticker: str,
    *,
    market: str = "CN",
) -> dict[str, Any]:
    """Fetch price performance (1W/1M/3M/6M/1Y returns).

    Returns:
        Dict with performance metrics.
    """
    obb = _lazy_obb()
    sym = _normalize_symbol(ticker, market)

    try:
        result = obb.equity.price.performance(symbol=sym, provider="akshare")
        items = _result_to_dicts(result)
        return items[0] if items else {}
    except Exception as exc:
        logger.warning("price_performance via OpenBB failed for %s: %s", ticker, exc)
        return {}


def screen_equities(
    *,
    market: str = "CN",
    **filters,
) -> list[dict[str, Any]]:
    """Screen equities using OpenBB screener.

    Args:
        market: ``"CN"`` | ``"HK"`` | ``"US"``
        **filters: Passed to obb.equity.screener

    Returns:
        List of matching equity dicts.
    """
    obb = _lazy_obb()

    try:
        result = obb.equity.screener(provider="akshare", **filters)
        return _result_to_dicts(result)
    except Exception as exc:
        logger.warning("equity_screener via OpenBB failed: %s", exc)
        return []


def fetch_fund_holdings(
    symbol: str,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Fetch fund / ETF holdings.

    Args:
        symbol: Fund or ETF symbol code

    Returns:
        List of holding dicts.
    """
    obb = _lazy_obb()

    try:
        result = obb.etf.holdings(symbol=symbol, provider="akshare", limit=limit)
        return _result_to_dicts(result)
    except Exception as exc:
        logger.warning("fund_holdings via OpenBB failed for %s: %s", symbol, exc)
        return []


def search_equities(
    query: str,
    *,
    market: str = "CN",
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Search for equities by name or keyword.

    Returns:
        List of matching equities with symbol, name, exchange info.
    """
    obb = _lazy_obb()

    try:
        result = obb.equity.search(query=query, provider="akshare")
        items = _result_to_dicts(result)
        return items[:limit]
    except Exception as exc:
        logger.warning("equity_search via OpenBB failed for '%s': %s", query, exc)
        return []


# ---------------------------------------------------------------------------
# Symbol normalisation
# ---------------------------------------------------------------------------

def _normalize_symbol(ticker: str, market: str) -> str:
    """Normalize ticker for OpenBB/akshare consumption."""
    ticker = ticker.strip()
    market = market.upper().strip()

    if market == "CN":
        # Strip .SZ / .SH suffix — akshare uses bare 6-digit codes
        import re
        m = re.match(r"^(\d{6})\.(SZ|SH|BJ)$", ticker, re.IGNORECASE)
        return m.group(1) if m else ticker
    elif market == "HK":
        import re
        m = re.match(r"^(\d{1,5})\.HK$", ticker, re.IGNORECASE)
        code = m.group(1) if m else ticker
        return code.zfill(5)
    else:  # US
        return ticker.upper()
