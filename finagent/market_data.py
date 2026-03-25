"""Unified market data adapter — fetches real-time or delayed snapshots from
A-share, Hong-Kong, and US markets via akshare.

Usage::

    from finagent.market_data import fetch_market_snapshot

    snap = fetch_market_snapshot("002025.SZ", market="CN")
    snap = fetch_market_snapshot("00700.HK", market="HK")
    snap = fetch_market_snapshot("NVDA",      market="US")

CLI::

    python -m finagent.cli market-snapshot --ticker 002025.SZ --market CN
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ticker normalisation
# ---------------------------------------------------------------------------

_CN_SUFFIX_RE = re.compile(r"^(\d{6})\.(SZ|SH|BJ)$", re.IGNORECASE)
_HK_SUFFIX_RE = re.compile(r"^(\d{1,5})\.HK$", re.IGNORECASE)

VALID_MARKETS = {"CN", "HK", "US"}


def _normalize_cn_ticker(raw: str) -> str:
    """Strip the exchange suffix – akshare uses bare 6-digit codes."""
    m = _CN_SUFFIX_RE.match(raw.strip())
    return m.group(1) if m else raw.strip()


def _normalize_hk_ticker(raw: str) -> str:
    """Strip .HK suffix, zero-pad to 5 digits for akshare."""
    m = _HK_SUFFIX_RE.match(raw.strip())
    if m:
        return m.group(1).zfill(5)
    return raw.strip().zfill(5)


def _normalize_us_ticker(raw: str) -> str:
    return raw.strip().upper()


def _safe_float(v: Any, *, default: float | None = None) -> float | None:
    if v is None or v == "" or v == "-":
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Anti-rate-limiting infrastructure
# ---------------------------------------------------------------------------

import contextlib
import os as _os
import random
import threading

# --- UA pool (rotate to avoid fingerprint detection) ---
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
]


def _random_ua() -> str:
    return random.choice(_UA_POOL)


# --- Spot data cache (avoid re-downloading full table within 60s) ---
_spot_cache: dict[str, tuple[float, Any]] = {}  # key -> (ts, dataframe)
_spot_lock = threading.Lock()
_SPOT_TTL = 60  # seconds


def _get_cached_spot(key: str):
    """Return cached DataFrame if still fresh, else None."""
    with _spot_lock:
        entry = _spot_cache.get(key)
        if entry and (time.time() - entry[0]) < _SPOT_TTL:
            logger.debug("cache hit: %s (age %.1fs)", key, time.time() - entry[0])
            return entry[1]
    return None


def _set_cached_spot(key: str, df):
    with _spot_lock:
        _spot_cache[key] = (time.time(), df)


# --- Request delay (random 0.5–2s between akshare calls) ---
_last_ak_call = 0.0
_ak_lock = threading.Lock()


def _throttle_ak():
    """Enforce minimum delay between akshare API calls to avoid rate-limiting."""
    global _last_ak_call
    with _ak_lock:
        elapsed = time.time() - _last_ak_call
        delay = random.uniform(0.5, 2.0)
        if elapsed < delay:
            wait = delay - elapsed
            logger.debug("throttle: sleeping %.2fs", wait)
            time.sleep(wait)
        _last_ak_call = time.time()


# --- Connection strategy: proxy pool → direct (strip mihomo) ---
@contextlib.contextmanager
def _strip_proxy():
    """Temporarily remove proxy env vars so akshare connects directly."""
    saved = {}
    for k in list(_os.environ):
        if "proxy" in k.lower():
            saved[k] = _os.environ.pop(k)
    try:
        yield
    finally:
        _os.environ.update(saved)


@contextlib.contextmanager
def _ak_connection_ctx():
    """Smart connection context for akshare calls.

    Strategy:
      1. If proxy pool has proxies → inject them into env vars
      2. Else → strip all proxies (direct connection)
    """
    try:
        from finagent.proxy_pool import get_proxy
        pool_proxy = get_proxy()
    except ImportError:
        pool_proxy = None

    if pool_proxy:
        # Use pool proxy: set env vars so akshare's requests picks it up
        saved = {}
        for k in list(_os.environ):
            if "proxy" in k.lower():
                saved[k] = _os.environ.pop(k)
        _os.environ["http_proxy"] = pool_proxy
        _os.environ["https_proxy"] = pool_proxy
        logger.debug("using pool proxy: %s", pool_proxy)
        try:
            yield
        except Exception:
            # Mark proxy as bad on failure
            try:
                from finagent.proxy_pool import mark_bad
                mark_bad(pool_proxy)
            except ImportError:
                pass
            raise
        finally:
            _os.environ.pop("http_proxy", None)
            _os.environ.pop("https_proxy", None)
            _os.environ.update(saved)
    else:
        # No pool proxy → strip mihomo proxy, go direct
        with _strip_proxy():
            yield


# --- Patch akshare requests with browser-like headers ---
_ak_patched = False


def _patch_akshare_session():
    """Monkey-patch akshare's underlying requests to use browser UA & Referer.

    akshare uses plain `requests.get()` with default python UA, which
    eastmoney easily fingerprints. This patches the session headers.
    """
    global _ak_patched
    if _ak_patched:
        return
    try:
        import requests
        _orig_get = requests.get

        def _patched_get(url, *args, **kwargs):
            headers = kwargs.get("headers", {}) or {}
            if "User-Agent" not in headers and "user-agent" not in headers:
                headers["User-Agent"] = _random_ua()
            if "Referer" not in headers and "referer" not in headers:
                if "eastmoney" in str(url):
                    headers["Referer"] = "https://quote.eastmoney.com/"
            kwargs["headers"] = headers
            return _orig_get(url, *args, **kwargs)

        requests.get = _patched_get
        _ak_patched = True
        logger.info("akshare session patched with browser UA/Referer")
    except Exception as exc:
        logger.warning("failed to patch akshare session: %s", exc)


# ---------------------------------------------------------------------------
# Market-specific fetchers
# ---------------------------------------------------------------------------

def _lazy_ak():
    """Lazy-import akshare so the module can load without it installed."""
    import akshare as ak  # type: ignore[import-untyped]
    return ak


def _lazy_yf():
    """Lazy-import yfinance so the module can load without it installed."""
    import yfinance as yf  # type: ignore[import-untyped]
    return yf


def _cn_ticker_to_yf(raw: str) -> str:
    """Convert A-share code to yfinance format (000002 -> 000002.SZ, 600501 -> 600501.SS)."""
    code = _normalize_cn_ticker(raw)
    return f"{code}.SS" if code.startswith("6") else f"{code}.SZ"


def _hk_ticker_to_yf(raw: str) -> str:
    """Convert HK code to yfinance format (00700 -> 0700.HK)."""
    code = _normalize_hk_ticker(raw)
    return f"{code.lstrip('0') or '0'}.HK"


# ---------------------------------------------------------------------------
# Direct Sina finance API fetcher (bypasses akshare — no WAF issues)
# ---------------------------------------------------------------------------

import subprocess


def _cn_code_to_sina(raw: str) -> str:
    """Convert bare 6-digit code to Sina format (sh600519, sz002025)."""
    code = _normalize_cn_ticker(raw)
    prefix = "sh" if code.startswith(("6", "9", "5")) else "sz"
    return f"{prefix}{code}"


def _fetch_cn_snapshot_sina(ticker: str) -> dict[str, Any]:
    """A-share snapshot via Sina's hq.sinajs.cn API (no anti-scraping).

    Format: var hq_str_sh600519="贵州茅台,open,prev_close,price,high,low,
    bid,ask,volume,amount,b1v,b1p,b2v,b2p,b3v,b3p,b4v,b4p,b5v,b5p,
    a1v,a1p,a2v,a2p,a3v,a3p,a4v,a4p,a5v,a5p,date,time,status";
    """
    code = _normalize_cn_ticker(ticker)
    sina_code = _cn_code_to_sina(ticker)

    try:
        env = {k: v for k, v in _os.environ.items() if "proxy" not in k.lower()}
        env["PATH"] = _os.environ.get("PATH", "/usr/bin")
        result = subprocess.run(
            ["curl", "-s", "--noproxy", "*", "--connect-timeout", "5", "-m", "10",
             "-H", "Referer: https://finance.sina.com.cn",
             f"https://hq.sinajs.cn/list={sina_code}"],
            capture_output=True, timeout=15, env=env,
        )
        raw_text = result.stdout.decode("gb18030", errors="replace").strip()
    except Exception as exc:
        raise ConnectionError(f"Sina curl failed for {sina_code}: {exc}") from exc

    if not raw_text or '="";' in raw_text:
        raise ValueError(f"Sina returned empty data for {sina_code}")

    # Parse: var hq_str_sh600519="name,open,prev,price,...";
    try:
        data_str = raw_text.split('"')[1]
        fields = data_str.split(",")
    except (IndexError, ValueError) as exc:
        raise ValueError(f"Sina parse error for {sina_code}: {raw_text[:100]}") from exc

    if len(fields) < 32:
        raise ValueError(f"Sina unexpected field count {len(fields)} for {sina_code}")

    name = fields[0]
    price = _safe_float(fields[3])
    prev_close = _safe_float(fields[2])
    volume = _safe_float(fields[8])
    amount = _safe_float(fields[9])

    change_pct = None
    if price and prev_close and prev_close > 0:
        change_pct = round((price - prev_close) / prev_close * 100, 2)

    # Sina doesn't provide market cap or PE — leave as None for yfinance to fill
    return {
        "ticker": ticker,
        "name": name,
        "market": "CN",
        "currency": "CNY",
        "price": price,
        "market_cap": None,  # Sina doesn't provide
        "pe_ttm": None,
        "pb": None,
        "ps_ttm": None,
        "ev_ebitda": None,
        "volume": volume,
        "price_change_pct": change_pct,
        "snapshot_at": _now_iso(),
        "source": "sina",
        "freshness": "realtime",
    }


def _fetch_cn_snapshot_akshare(ticker: str) -> dict[str, Any]:
    """A-share snapshot via akshare's eastmoney real-time interface."""
    _patch_akshare_session()
    ak = _lazy_ak()
    code = _normalize_cn_ticker(ticker)

    # Use cached spot table if fresh (avoid re-downloading 5000+ stocks)
    df = _get_cached_spot("cn_spot")
    if df is None:
        _throttle_ak()
        with _ak_connection_ctx():
            df = ak.stock_zh_a_spot_em()
        _set_cached_spot("cn_spot", df)

    row = df[df["代码"] == code]
    if row.empty:
        raise ValueError(f"A-share ticker {code} not found in spot data")

    r = row.iloc[0]
    price = _safe_float(r.get("最新价"))
    market_cap = _safe_float(r.get("总市值"))
    pe = _safe_float(r.get("市盈率-动态"))
    pb = _safe_float(r.get("市净率"))
    volume = _safe_float(r.get("成交量"))
    change_pct = _safe_float(r.get("涨跌幅"))
    name = str(r.get("名称", ""))

    return {
        "ticker": ticker,
        "name": name,
        "market": "CN",
        "currency": "CNY",
        "price": price,
        "market_cap": round(market_cap / 1e8, 2) if market_cap else None,
        "pe_ttm": pe,
        "pb": pb,
        "ps_ttm": None,
        "ev_ebitda": None,
        "volume": volume,
        "price_change_pct": change_pct,
        "snapshot_at": _now_iso(),
        "source": "akshare/eastmoney",
        "freshness": "realtime",
    }


def _fetch_cn_snapshot_yfinance(ticker: str) -> dict[str, Any]:
    """A-share snapshot via yfinance (fallback when eastmoney is blocked)."""
    yf = _lazy_yf()
    symbol = _cn_ticker_to_yf(ticker)
    tk = yf.Ticker(symbol)
    info = tk.info
    if not info or info.get("regularMarketPrice") is None:
        raise ValueError(f"yfinance returned no data for {symbol}")

    price = _safe_float(info.get("regularMarketPrice"))
    prev_close = _safe_float(info.get("regularMarketPreviousClose"))
    change_pct = None
    if price is not None and prev_close:
        change_pct = round((price - prev_close) / prev_close * 100, 2)
    market_cap_raw = _safe_float(info.get("marketCap"))

    return {
        "ticker": ticker,
        "name": info.get("shortName", info.get("longName", "")),
        "market": "CN",
        "currency": "CNY",
        "price": price,
        "market_cap": round(market_cap_raw / 1e8, 2) if market_cap_raw else None,
        "pe_ttm": _safe_float(info.get("trailingPE")),
        "pb": _safe_float(info.get("priceToBook")),
        "ps_ttm": _safe_float(info.get("priceToSalesTrailing12Months")),
        "ev_ebitda": _safe_float(info.get("enterpriseToEbitda")),
        "volume": _safe_float(info.get("regularMarketVolume")),
        "price_change_pct": change_pct,
        "snapshot_at": _now_iso(),
        "source": "yfinance",
        "freshness": "delayed",
    }


def _fetch_cn_snapshot(ticker: str) -> dict[str, Any]:
    """A-share snapshot — Sina first, akshare(eastmoney) second, yfinance last."""
    # 1. Try Sina (direct curl, no WAF issues)
    try:
        return _fetch_cn_snapshot_sina(ticker)
    except Exception as sina_exc:
        logger.info("Sina CN failed for %s (%s), trying akshare", ticker, sina_exc)

    # 2. Try akshare/eastmoney (may be IP-blocked)
    try:
        return _fetch_cn_snapshot_akshare(ticker)
    except Exception as ak_exc:
        logger.info("akshare CN failed for %s (%s), falling back to yfinance", ticker, ak_exc)

    # 3. Final fallback: yfinance (delayed data, no CN names)
    return _fetch_cn_snapshot_yfinance(ticker)


def _fetch_hk_snapshot_akshare(ticker: str) -> dict[str, Any]:
    """Hong Kong stock snapshot via akshare."""
    _patch_akshare_session()
    ak = _lazy_ak()
    code = _normalize_hk_ticker(ticker)

    df = _get_cached_spot("hk_spot")
    if df is None:
        _throttle_ak()
        with _ak_connection_ctx():
            df = ak.stock_hk_spot_em()
        _set_cached_spot("hk_spot", df)

    row = df[df["代码"] == code]
    if row.empty:
        raise ValueError(f"HK ticker {code} not found in spot data")

    r = row.iloc[0]
    price = _safe_float(r.get("最新价"))
    market_cap = _safe_float(r.get("总市值"))
    pe = _safe_float(r.get("市盈率"))
    change_pct = _safe_float(r.get("涨跌幅"))
    name = str(r.get("名称", ""))

    return {
        "ticker": ticker,
        "name": name,
        "market": "HK",
        "currency": "HKD",
        "price": price,
        "market_cap": round(market_cap / 1e8, 2) if market_cap else None,
        "pe_ttm": pe,
        "pb": None,
        "ps_ttm": None,
        "ev_ebitda": None,
        "volume": None,
        "price_change_pct": change_pct,
        "snapshot_at": _now_iso(),
        "source": "akshare/eastmoney",
        "freshness": "realtime",
    }


def _fetch_hk_snapshot_yfinance(ticker: str) -> dict[str, Any]:
    """HK snapshot via yfinance (fallback)."""
    yf = _lazy_yf()
    symbol = _hk_ticker_to_yf(ticker)
    tk = yf.Ticker(symbol)
    info = tk.info
    if not info or info.get("regularMarketPrice") is None:
        raise ValueError(f"yfinance returned no data for {symbol}")

    price = _safe_float(info.get("regularMarketPrice"))
    prev_close = _safe_float(info.get("regularMarketPreviousClose"))
    change_pct = None
    if price is not None and prev_close:
        change_pct = round((price - prev_close) / prev_close * 100, 2)
    market_cap_raw = _safe_float(info.get("marketCap"))

    return {
        "ticker": ticker,
        "name": info.get("shortName", info.get("longName", "")),
        "market": "HK",
        "currency": "HKD",
        "price": price,
        "market_cap": round(market_cap_raw / 1e8, 2) if market_cap_raw else None,
        "pe_ttm": _safe_float(info.get("trailingPE")),
        "pb": _safe_float(info.get("priceToBook")),
        "ps_ttm": _safe_float(info.get("priceToSalesTrailing12Months")),
        "ev_ebitda": _safe_float(info.get("enterpriseToEbitda")),
        "volume": _safe_float(info.get("regularMarketVolume")),
        "price_change_pct": change_pct,
        "snapshot_at": _now_iso(),
        "source": "yfinance",
        "freshness": "delayed",
    }


def _fetch_hk_snapshot(ticker: str) -> dict[str, Any]:
    """HK snapshot — akshare first, yfinance fallback."""
    try:
        return _fetch_hk_snapshot_akshare(ticker)
    except Exception as ak_exc:
        logger.info("akshare HK failed for %s (%s), falling back to yfinance", ticker, ak_exc)
        return _fetch_hk_snapshot_yfinance(ticker)


def _fetch_us_snapshot_yfinance(ticker: str) -> dict[str, Any]:
    """US stock snapshot via yfinance — richer fields than akshare."""
    yf = _lazy_yf()
    symbol = _normalize_us_ticker(ticker)

    tk = yf.Ticker(symbol)
    info = tk.info
    if not info or info.get("regularMarketPrice") is None:
        raise ValueError(f"yfinance returned no data for {symbol}")

    price = _safe_float(info.get("regularMarketPrice"))
    prev_close = _safe_float(info.get("regularMarketPreviousClose"))
    change_pct = None
    if price is not None and prev_close:
        change_pct = round((price - prev_close) / prev_close * 100, 2)

    market_cap_raw = _safe_float(info.get("marketCap"))

    return {
        "ticker": ticker,
        "name": info.get("shortName", info.get("longName", "")),
        "market": "US",
        "currency": "USD",
        "price": price,
        "market_cap": round(market_cap_raw / 1e8, 2) if market_cap_raw else None,
        "pe_ttm": _safe_float(info.get("trailingPE")),
        "pb": _safe_float(info.get("priceToBook")),
        "ps_ttm": _safe_float(info.get("priceToSalesTrailing12Months")),
        "ev_ebitda": _safe_float(info.get("enterpriseToEbitda")),
        "volume": _safe_float(info.get("regularMarketVolume")),
        "price_change_pct": change_pct,
        "year_high": _safe_float(info.get("fiftyTwoWeekHigh")),
        "year_low": _safe_float(info.get("fiftyTwoWeekLow")),
        "dividend_yield": _safe_float(info.get("dividendYield")),
        "snapshot_at": _now_iso(),
        "source": "yfinance",
        "freshness": "realtime",
    }


def _fetch_us_snapshot_akshare(ticker: str) -> dict[str, Any]:
    """US stock snapshot via akshare (fallback)."""
    ak = _lazy_ak()

    symbol = _normalize_us_ticker(ticker)

    try:
        df = ak.stock_us_spot_em()
        code_col = "代码" if "代码" in df.columns else "code"
        if code_col in df.columns:
            mask = df[code_col].astype(str).str.upper().str.endswith(f".{symbol}")
            if not mask.any():
                mask = df[code_col].astype(str).str.upper() == symbol
            row = df[mask]
        else:
            row = df[df.iloc[:, 1].astype(str).str.upper() == symbol]

        if row.empty:
            raise ValueError(f"US ticker {symbol} not found")

        r = row.iloc[0]
        price = _safe_float(r.get("最新价", r.get("price")))
        market_cap = _safe_float(r.get("总市值", r.get("market_cap")))
        pe = _safe_float(r.get("市盈率", r.get("pe")))
        change_pct = _safe_float(r.get("涨跌幅", r.get("change_pct")))
        name = str(r.get("名称", r.get("name", "")))
    except Exception as exc:
        logger.warning("stock_us_spot_em failed for %s: %s", symbol, exc)
        raise ValueError(f"US ticker {symbol} lookup failed: {exc}") from exc

    return {
        "ticker": ticker,
        "name": name,
        "market": "US",
        "currency": "USD",
        "price": price,
        "market_cap": round(market_cap / 1e8, 2) if market_cap else None,
        "pe_ttm": pe,
        "pb": None,
        "ps_ttm": None,
        "ev_ebitda": None,
        "volume": None,
        "price_change_pct": change_pct,
        "snapshot_at": _now_iso(),
        "source": "akshare/eastmoney",
        "freshness": "delayed",
    }


def _fetch_us_snapshot(ticker: str) -> dict[str, Any]:
    """US stock snapshot — yfinance first, akshare fallback."""
    try:
        return _fetch_us_snapshot_yfinance(ticker)
    except Exception as yf_exc:
        logger.info("yfinance failed for %s (%s), falling back to akshare", ticker, yf_exc)
        return _fetch_us_snapshot_akshare(ticker)


# ---------------------------------------------------------------------------
# Historical price helper
# ---------------------------------------------------------------------------

def fetch_historical(
    ticker: str,
    *,
    market: str,
    start_date: str | None = None,
    end_date: str | None = None,
    period: str = "daily",
) -> list[dict[str, Any]]:
    """Fetch historical OHLCV data as a list of dicts.

    Args:
        ticker: Stock ticker
        market: "CN" | "HK" | "US"
        start_date: "YYYY-MM-DD" (default: 1 year ago)
        end_date: "YYYY-MM-DD" (default: today)
        period: "daily" | "weekly" | "monthly"

    Returns:
        List of {date, open, high, low, close, volume} dicts.
    """
    market = market.upper().strip()
    if market not in VALID_MARKETS:
        raise ValueError(f"Invalid market '{market}'")

    if market == "CN":
        return _fetch_historical_cn(ticker, start_date=start_date,
                                     end_date=end_date, period=period)
    elif market == "US":
        try:
            return _fetch_historical_yf(ticker, market="US",
                                         start_date=start_date, end_date=end_date,
                                         period=period)
        except ImportError:
            raise ValueError(
                "US historical data requires yfinance. "
                "Install with: pip install yfinance"
            )
    else:  # HK
        return _fetch_historical_hk(ticker, start_date=start_date,
                                     end_date=end_date, period=period)


def _fetch_historical_yf(
    ticker: str, *, market: str,
    start_date: str | None, end_date: str | None, period: str,
) -> list[dict[str, Any]]:
    """Fetch historical OHLCV via yfinance."""
    yf = _lazy_yf()
    if market == "CN":
        symbol = _cn_ticker_to_yf(ticker)
    elif market == "HK":
        symbol = _hk_ticker_to_yf(ticker)
    else:
        symbol = _normalize_us_ticker(ticker)
    tk = yf.Ticker(symbol)
    interval = {"daily": "1d", "weekly": "1wk", "monthly": "1mo"}.get(period, "1d")
    df = tk.history(start=start_date, end=end_date, interval=interval)
    if df.empty:
        raise ValueError(f"yfinance returned no historical data for {symbol}")
    return [
        {
            "date": idx.strftime("%Y-%m-%d"),
            "open": round(row["Open"], 4),
            "high": round(row["High"], 4),
            "low": round(row["Low"], 4),
            "close": round(row["Close"], 4),
            "volume": int(row["Volume"]),
        }
        for idx, row in df.iterrows()
    ]


def _fetch_historical_cn(
    ticker: str, *, start_date: str | None,
    end_date: str | None, period: str,
) -> list[dict[str, Any]]:
    """CN historical: akshare first (eastmoney), yfinance fallback."""
    try:
        ak = _lazy_ak()
        code = _normalize_cn_ticker(ticker)
        kwargs: dict[str, Any] = {"symbol": code, "period": period, "adjust": "qfq"}
        if start_date:
            kwargs["start_date"] = start_date.replace("-", "")
        if end_date:
            kwargs["end_date"] = end_date.replace("-", "")
        with _ak_connection_ctx():
            df = ak.stock_zh_a_hist(**kwargs)
        if df.empty:
            raise ValueError("akshare returned empty dataframe")
        return [
            {
                "date": str(row.get("日期", "")),
                "open": _safe_float(row.get("开盘")),
                "high": _safe_float(row.get("最高")),
                "low": _safe_float(row.get("最低")),
                "close": _safe_float(row.get("收盘")),
                "volume": _safe_float(row.get("成交量")),
            }
            for _, row in df.iterrows()
        ]
    except Exception as exc:
        logger.info("akshare CN hist failed for %s (%s), falling back to yfinance", ticker, exc)
        return _fetch_historical_yf(ticker, market="CN",
                                     start_date=start_date, end_date=end_date,
                                     period=period)


def _fetch_historical_hk(
    ticker: str, *, start_date: str | None,
    end_date: str | None, period: str,
) -> list[dict[str, Any]]:
    """HK historical: akshare first, yfinance fallback."""
    try:
        ak = _lazy_ak()
        code = _normalize_hk_ticker(ticker)
        with _ak_connection_ctx():
            df = ak.stock_hk_hist(symbol=code, period=period, adjust="qfq")
        if df.empty:
            raise ValueError("akshare returned empty dataframe")
        return [
            {
                "date": str(row.get("日期", "")),
                "open": _safe_float(row.get("开盘")),
                "high": _safe_float(row.get("最高")),
                "low": _safe_float(row.get("最低")),
                "close": _safe_float(row.get("收盘")),
                "volume": _safe_float(row.get("成交量")),
            }
            for _, row in df.iterrows()
        ]
    except Exception as exc:
        logger.info("akshare HK hist failed for %s (%s), falling back to yfinance", ticker, exc)
        return _fetch_historical_yf(ticker, market="HK",
                                     start_date=start_date, end_date=end_date,
                                     period=period)


# ---------------------------------------------------------------------------
# Index snapshots (Sina — no WAF)
# ---------------------------------------------------------------------------

# Common Chinese market indices
_INDEX_MAP = {
    "000001": ("s_sh000001", "上证指数"),
    "399001": ("s_sz399001", "深证成指"),
    "399006": ("s_sz399006", "创业板指"),
    "000688": ("s_sh000688", "科创50"),
    "000300": ("s_sh000300", "沪深300"),
    "000016": ("s_sh000016", "上证50"),
    "000905": ("s_sh000905", "中证500"),
    "000852": ("s_sh000852", "中证1000"),
    "HSI":    ("hkHSI",      "恒生指数"),
    "HSCEI":  ("hkHSCEI",    "国企指数"),
}


def fetch_index_snapshot(index_code: str = "000001") -> dict[str, Any]:
    """Fetch a market index snapshot via Sina.

    Args:
        index_code: Index code (e.g. "000001" for 上证, "399006" for 创业板,
                    "HSI" for 恒生). See _INDEX_MAP for supported codes.

    Returns:
        Dict with name, price, change, change_pct, volume, amount.
    """
    code = index_code.strip().upper()
    if code not in _INDEX_MAP:
        # Try lowercase
        code = index_code.strip()
    if code not in _INDEX_MAP:
        raise ValueError(f"Unknown index code '{index_code}'. "
                         f"Supported: {list(_INDEX_MAP.keys())}")

    sina_code, cn_name = _INDEX_MAP[code]
    try:
        env = {k: v for k, v in _os.environ.items() if "proxy" not in k.lower()}
        env["PATH"] = _os.environ.get("PATH", "/usr/bin")
        result = subprocess.run(
            ["curl", "-s", "--noproxy", "*", "--connect-timeout", "5", "-m", "10",
             "-H", "Referer: https://finance.sina.com.cn",
             f"https://hq.sinajs.cn/list={sina_code}"],
            capture_output=True, timeout=15, env=env,
        )
        raw = result.stdout.decode("gb18030", errors="replace").strip()
    except Exception as exc:
        raise ConnectionError(f"Sina index fetch failed: {exc}") from exc

    if not raw or '="";' in raw:
        raise ValueError(f"Sina returned empty for index {code}")

    # Sina index format (simplified): name,price,change,change_pct,volume,amount
    try:
        data_str = raw.split('"')[1]
        fields = data_str.split(",")
    except (IndexError, ValueError) as exc:
        raise ValueError(f"Sina index parse error: {raw[:100]}") from exc

    return {
        "index_code": index_code,
        "name": fields[0] if fields else cn_name,
        "price": _safe_float(fields[1]) if len(fields) > 1 else None,
        "change": _safe_float(fields[2]) if len(fields) > 2 else None,
        "change_pct": _safe_float(fields[3]) if len(fields) > 3 else None,
        "volume": _safe_float(fields[4]) if len(fields) > 4 else None,
        "amount": _safe_float(fields[5]) if len(fields) > 5 else None,
        "snapshot_at": _now_iso(),
        "source": "sina",
    }


def fetch_all_indices() -> list[dict[str, Any]]:
    """Fetch snapshots for all major indices."""
    results = []
    for code in _INDEX_MAP:
        try:
            results.append(fetch_index_snapshot(code))
        except Exception as exc:
            logger.warning("Failed to fetch index %s: %s", code, exc)
            results.append({"index_code": code, "error": str(exc)})
    return results


# ---------------------------------------------------------------------------
# A-share Financial Statements (Sina finance API)
# ---------------------------------------------------------------------------

def fetch_financials_cn(
    ticker: str,
    *,
    report_type: str = "income",
) -> list[dict[str, Any]]:
    """Fetch A-share financial statements via yfinance.

    Args:
        ticker: A-share ticker (e.g. "600519.SH")
        report_type: "income" | "balance" | "cashflow"

    Returns:
        List of dicts, each representing one reporting period with
        date and financial line items.
    """
    yf = _lazy_yf()
    symbol = _cn_ticker_to_yf(ticker)
    tk = yf.Ticker(symbol)

    type_map = {
        "income":   "income_stmt",
        "profit":   "income_stmt",
        "balance":  "balance_sheet",
        "cashflow": "cashflow",
    }
    if report_type not in type_map:
        raise ValueError(f"Invalid report_type '{report_type}'. "
                         f"Use: {list(type_map.keys())}")

    attr_name = type_map[report_type]

    with _strip_proxy():
        df = getattr(tk, attr_name)

    if df is None or df.empty:
        raise ValueError(f"yfinance returned no {report_type} data for {symbol}")

    records = []
    for col in df.columns:
        row_data: dict[str, Any] = {
            "report_date": col.strftime("%Y-%m-%d") if hasattr(col, "strftime") else str(col),
            "ticker": ticker,
        }
        for idx, val in df[col].items():
            field = str(idx)
            if val is not None and str(val) != "nan":
                try:
                    row_data[field] = float(val)
                except (ValueError, TypeError):
                    row_data[field] = val
        records.append(row_data)

    return records


# ---------------------------------------------------------------------------
# Sector / Industry Classification (Sina)
# ---------------------------------------------------------------------------

# Major Sina sector categories
_SECTOR_MAP = {
    "new_blhy": "玻璃行业", "new_cbzz": "船舶制造", "new_dlhy": "电力行业",
    "new_fzhy": "纺织行业", "new_gthy": "钢铁行业", "new_gjhy": "公交行业",
    "new_gshy": "供水供气", "new_gthy": "港口航运", "new_hbhj": "环保行业",
    "new_hghy": "化工行业", "new_hkhy": "航空行业", "new_jchy": "建材行业",
    "new_jdhy": "家电行业", "new_jjhy": "家具行业", "new_jxhy": "机械行业",
    "new_jzhy": "建筑行业", "new_lyhy": "旅游行业", "new_mtsx": "煤炭石油",
    "new_nlmy": "农林牧渔", "new_qchy": "汽车行业", "new_sphy": "食品行业",
    "new_swhy": "商业百货", "new_tyhy": "体育行业", "new_wlhy": "物流行业",
    "new_wrhy": "外贸行业", "new_xxhy": "信息技术", "new_ylhy": "医疗行业",
    "new_yyhy": "医药行业", "new_yshy": "有色金属", "new_zhhy": "综合行业",
    "new_zjhy": "造纸行业", "new_jrhy": "金融行业", "new_dcgy": "电池概念",
    "new_bxy": "半导体",
}


def fetch_sector_list() -> list[dict[str, str]]:
    """Return list of available sector/industry categories."""
    return [{"code": k, "name": v} for k, v in _SECTOR_MAP.items()]


def fetch_sector_stocks(sector_code: str) -> list[dict[str, Any]]:
    """Fetch stocks belonging to a sector via Sina.

    Args:
        sector_code: Sector code from fetch_sector_list() (e.g. "new_yyhy")

    Returns:
        List of {ticker, name, price, change_pct} dicts.
    """
    if sector_code not in _SECTOR_MAP:
        raise ValueError(f"Unknown sector code '{sector_code}'. "
                         f"Use fetch_sector_list() for valid codes.")

    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=200&sort=symbol&asc=1&node={sector_code}&symbol=&_s_r_a=auto"

    try:
        env = {k: v for k, v in _os.environ.items() if "proxy" not in k.lower()}
        env["PATH"] = _os.environ.get("PATH", "/usr/bin")
        result = subprocess.run(
            ["curl", "-s", "--noproxy", "*", "--connect-timeout", "5", "-m", "15",
             "-H", "Referer: https://finance.sina.com.cn",
             url],
            capture_output=True, timeout=20, env=env,
        )
        raw = result.stdout.decode("utf-8", errors="replace").strip()
    except Exception as exc:
        raise ConnectionError(f"Sina sector fetch failed: {exc}") from exc

    if not raw or raw == "null":
        raise ValueError(f"Sina returned empty sector data for {sector_code}")

    # Sina returns JSON array (with single quotes — need to fix)
    import ast
    try:
        data = ast.literal_eval(raw)
    except Exception:
        data = json.loads(raw.replace("'", '"'))

    stocks = []
    for item in data:
        stocks.append({
            "ticker": str(item.get("symbol", "")),
            "name": str(item.get("name", "")),
            "price": _safe_float(item.get("trade")),
            "change_pct": _safe_float(item.get("changepercent")),
            "volume": _safe_float(item.get("volume")),
            "market_cap": _safe_float(item.get("mktcap")),
            "pe": _safe_float(item.get("per")),
            "pb": _safe_float(item.get("pb")),
        })
    return stocks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_MARKET_FETCHERS = {
    "CN": _fetch_cn_snapshot,
    "HK": _fetch_hk_snapshot,
    "US": _fetch_us_snapshot,
}


def fetch_market_snapshot(ticker: str, *, market: str) -> dict[str, Any]:
    """Fetch a standardised market snapshot for a given ticker.

    Args:
        ticker: Stock ticker (e.g. "002025.SZ", "00700.HK", "NVDA")
        market: "CN" | "HK" | "US"

    Returns:
        Dict with standardised fields (price, market_cap, pe_ttm, etc.)

    Raises:
        ValueError: if market is invalid or ticker not found
    """
    market = market.upper().strip()
    if market not in VALID_MARKETS:
        raise ValueError(f"Invalid market '{market}'. Must be one of {VALID_MARKETS}")

    fetcher = _MARKET_FETCHERS[market]
    t0 = time.monotonic()
    try:
        snapshot = fetcher(ticker)
    except Exception:
        logger.exception("Failed to fetch market snapshot for %s (%s)", ticker, market)
        raise

    elapsed = time.monotonic() - t0
    snapshot["fetch_elapsed_ms"] = round(elapsed * 1000, 1)
    return snapshot


def fetch_multi_snapshots(
    tickers: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Fetch snapshots for multiple tickers.

    Args:
        tickers: List of {"ticker": "...", "market": "..."} dicts.

    Returns:
        List of snapshot dicts (includes "error" key on failure).
    """
    results: list[dict[str, Any]] = []
    for item in tickers:
        t = str(item.get("ticker", "")).strip()
        m = str(item.get("market", "")).strip().upper()
        if not t or not m:
            results.append({"ticker": t, "market": m, "error": "missing_ticker_or_market"})
            continue
        try:
            snap = fetch_market_snapshot(t, market=m)
            results.append(snap)
        except Exception as exc:
            results.append({"ticker": t, "market": m, "error": str(exc)})
    return results


# ---------------------------------------------------------------------------
# CLI entry point (called from finagent.cli)
# ---------------------------------------------------------------------------

def cli_market_snapshot(args: Any) -> None:
    """CLI handler for ``finagent market-snapshot``."""
    ticker = str(getattr(args, "ticker", "")).strip()
    market = str(getattr(args, "market", "CN")).strip().upper()

    if not ticker:
        print(json.dumps({"error": "ticker is required"}, ensure_ascii=False))
        return

    try:
        snap = fetch_market_snapshot(ticker, market=market)
        print(json.dumps(snap, ensure_ascii=False, indent=2))
    except Exception as exc:
        print(json.dumps({"error": str(exc), "ticker": ticker, "market": market}, ensure_ascii=False))
