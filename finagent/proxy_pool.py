"""Domestic proxy pool for China finance data scraping.

Provides rotating proxy IPs from configurable providers to bypass
IP-based rate limiting from eastmoney, 10jqka, and other domestic
financial data sources.

Supported providers:
  - ``env``    : Read proxy list from environment variable FINAGENT_PROXY_LIST
  - ``file``   : Read proxy list from a text file (one per line)
  - ``kuaidaili`` : (stub) Tunnel proxy from 快代理
  - ``qingguo``   : (stub) Tunnel proxy from 青果网络

Usage::

    from finagent.proxy_pool import get_proxy, mark_bad, proxy_context

    # Get a random working proxy
    p = get_proxy()  # -> "http://1.2.3.4:8080" or None

    # Use as context manager with requests
    with proxy_context() as proxies:
        requests.get(url, proxies=proxies, timeout=10)

    # Mark a proxy as bad (will be temporarily blacklisted)
    mark_bad("http://1.2.3.4:8080")

Configuration via environment variables:
    FINAGENT_PROXY_PROVIDER  : "env" | "file" | "kuaidaili" | "qingguo"  (default: "env")
    FINAGENT_PROXY_LIST      : comma-separated proxy URLs (for provider=env)
    FINAGENT_PROXY_FILE      : path to proxy list file (for provider=file)
    FINAGENT_PROXY_TUNNEL    : tunnel proxy URL (for kuaidaili/qingguo)
"""

from __future__ import annotations

import contextlib
import logging
import os
import random
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Proxy registry
# ---------------------------------------------------------------------------

_proxy_list: list[str] = []
_bad_proxies: dict[str, float] = {}  # proxy -> timestamp when marked bad
_lock = threading.Lock()
_BAD_TTL = 300  # seconds before a bad proxy is retried
_initialized = False


def _init_pool():
    """Initialize the proxy pool from configured provider."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    provider = os.environ.get("FINAGENT_PROXY_PROVIDER", "env").lower()

    if provider == "env":
        raw = os.environ.get("FINAGENT_PROXY_LIST", "")
        if raw.strip():
            _proxy_list.extend(
                p.strip() for p in raw.split(",") if p.strip()
            )
            logger.info("proxy pool: loaded %d proxies from env", len(_proxy_list))

    elif provider == "file":
        filepath = os.environ.get("FINAGENT_PROXY_FILE", "")
        if filepath and os.path.isfile(filepath):
            with open(filepath) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        _proxy_list.append(line)
            logger.info("proxy pool: loaded %d proxies from %s", len(_proxy_list), filepath)
        else:
            logger.warning("proxy pool: FINAGENT_PROXY_FILE not set or not found: %s", filepath)

    elif provider == "kuaidaili":
        tunnel = os.environ.get("FINAGENT_PROXY_TUNNEL", "")
        if tunnel:
            _proxy_list.append(tunnel)
            logger.info("proxy pool: using kuaidaili tunnel %s", tunnel)
        else:
            logger.warning("proxy pool: FINAGENT_PROXY_TUNNEL not set for kuaidaili")

    elif provider == "qingguo":
        tunnel = os.environ.get("FINAGENT_PROXY_TUNNEL", "")
        if tunnel:
            _proxy_list.append(tunnel)
            logger.info("proxy pool: using qingguo tunnel %s", tunnel)
        else:
            logger.warning("proxy pool: FINAGENT_PROXY_TUNNEL not set for qingguo")

    else:
        logger.warning("proxy pool: unknown provider '%s'", provider)

    if not _proxy_list:
        logger.info("proxy pool: no proxies configured, will use direct connection")


def add_proxies(proxies: list[str]):
    """Dynamically add proxies to the pool at runtime."""
    with _lock:
        for p in proxies:
            if p not in _proxy_list:
                _proxy_list.append(p)
        logger.info("proxy pool: added %d proxies (total: %d)", len(proxies), len(_proxy_list))


def get_proxy() -> str | None:
    """Return a random non-bad proxy, or None if pool is empty."""
    _init_pool()
    with _lock:
        now = time.time()
        # Clean expired bad marks
        expired = [p for p, ts in _bad_proxies.items() if now - ts > _BAD_TTL]
        for p in expired:
            del _bad_proxies[p]

        candidates = [p for p in _proxy_list if p not in _bad_proxies]
        if not candidates:
            return None
        return random.choice(candidates)


def mark_bad(proxy: str):
    """Temporarily blacklist a proxy that failed."""
    with _lock:
        _bad_proxies[proxy] = time.time()
        logger.info("proxy pool: marked bad: %s (%d/%d available)",
                     proxy, len(_proxy_list) - len(_bad_proxies), len(_proxy_list))


def pool_stats() -> dict[str, Any]:
    """Return pool diagnostics."""
    _init_pool()
    with _lock:
        return {
            "total": len(_proxy_list),
            "bad": len(_bad_proxies),
            "available": len(_proxy_list) - len(_bad_proxies),
            "proxies": list(_proxy_list),
            "bad_list": dict(_bad_proxies),
        }


@contextlib.contextmanager
def proxy_context():
    """Context manager returning a proxies dict for requests.

    If no proxy available, yields empty dict (direct connection).
    On connection error, marks the proxy as bad.
    """
    proxy = get_proxy()
    if proxy:
        proxies = {"http": proxy, "https": proxy}
        logger.debug("proxy_context: using %s", proxy)
    else:
        proxies = {}
        logger.debug("proxy_context: direct (no proxy available)")

    try:
        yield proxies
    except Exception:
        if proxy:
            mark_bad(proxy)
        raise


def make_proxied_request(url: str, *, max_retries: int = 3, timeout: int = 10,
                         headers: dict | None = None) -> Any:
    """Make a GET request with automatic proxy rotation and retry.

    Tries proxy first, falls back to direct on failure.
    Returns the response object.
    """
    import requests as _requests

    last_exc = None

    for attempt in range(max_retries):
        proxy = get_proxy()
        proxies = {"http": proxy, "https": proxy} if proxy else {}
        try:
            r = _requests.get(url, proxies=proxies, headers=headers,
                              timeout=timeout, verify=True)
            r.raise_for_status()
            return r
        except Exception as exc:
            last_exc = exc
            if proxy:
                mark_bad(proxy)
            logger.debug("proxied request attempt %d/%d failed: %s",
                         attempt + 1, max_retries, exc)
            time.sleep(random.uniform(0.5, 1.5))

    # Final retry: direct connection (no proxy)
    try:
        r = _requests.get(url, timeout=timeout, headers=headers, verify=True)
        r.raise_for_status()
        return r
    except Exception as exc:
        raise ConnectionError(
            f"All {max_retries} proxy attempts + direct failed for {url}"
        ) from (last_exc or exc)
