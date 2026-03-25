"""Market-Data MCP Tool Server for finagent.

Exposes finagent's unified market data layer (akshare-based) as MCP tools
so that any AI agent (Antigravity, Claude, Gemini CLI) can query CN/HK/US
stock data via the MCP protocol.

Runtime: /usr/bin/python3 (system Python, has akshare + pandas).
US real-time snapshots use akshare; historical US data degrades gracefully
if yfinance is not installed.

Run as stdio server::

    python -m finagent.openbb_mcp_server

Register in MCP config::

    {
      "mcpServers": {
        "finagent-market": {
          "command": "/usr/bin/python3",
          "args": ["-m", "finagent.openbb_mcp_server"],
          "cwd": "/vol1/1000/projects/finagent"
        }
      }
    }
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tool definitions — only tools that work on system Python (akshare-based)
# ---------------------------------------------------------------------------

_TOOLS = [
    {
        "name": "market_snapshot",
        "description": (
            "Get a real-time market snapshot for a single stock. "
            "Supports CN (A-share, e.g. '002025.SZ'), HK (e.g. '700.HK'), "
            "and US (e.g. 'RKLB') markets. Returns price, market_cap (亿), "
            "PE, PB, volume, change%, and more. "
            "US stocks use yfinance if available, akshare otherwise."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "Stock ticker, e.g. '001270.SZ', '00700.HK', 'RKLB'",
                },
                "market": {
                    "type": "string",
                    "enum": ["CN", "HK", "US"],
                    "description": "Market: CN for A-share, HK for Hong Kong, US for US stocks",
                },
            },
            "required": ["ticker", "market"],
        },
    },
    {
        "name": "market_batch_snapshot",
        "description": (
            "Get real-time snapshots for multiple stocks in one call. "
            "Each item needs ticker and market. Returns list of snapshots."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "tickers": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "ticker": {"type": "string"},
                            "market": {"type": "string", "enum": ["CN", "HK", "US"]},
                        },
                        "required": ["ticker", "market"],
                    },
                    "description": "List of {ticker, market} objects",
                },
            },
            "required": ["tickers"],
        },
    },
    {
        "name": "stock_historical",
        "description": (
            "Get historical daily/weekly/monthly OHLCV data for a stock. "
            "CN and HK use akshare (前复权). US uses yfinance if installed, "
            "otherwise returns error. Pass start_date/end_date as YYYY-MM-DD."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "Stock ticker"},
                "market": {"type": "string", "enum": ["CN", "HK", "US"]},
                "start_date": {
                    "type": "string",
                    "description": "Start date YYYY-MM-DD (optional, default 1yr ago)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date YYYY-MM-DD (optional, default today)",
                },
                "period": {
                    "type": "string",
                    "enum": ["daily", "weekly", "monthly"],
                    "description": "Bar period (default: daily)",
                },
            },
            "required": ["ticker", "market"],
        },
    },
]


def _handle_tool_call(name: str, arguments: dict[str, Any]) -> Any:
    """Dispatch a tool call to the appropriate finagent function."""
    from finagent.market_data import (
        fetch_market_snapshot,
        fetch_multi_snapshots,
        fetch_historical,
    )

    if name == "market_snapshot":
        return fetch_market_snapshot(
            arguments["ticker"], market=arguments["market"]
        )
    elif name == "market_batch_snapshot":
        return fetch_multi_snapshots(arguments["tickers"])
    elif name == "stock_historical":
        return fetch_historical(
            arguments["ticker"],
            market=arguments["market"],
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
            period=arguments.get("period", "daily"),
        )
    else:
        raise ValueError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# JSON-RPC 2.0 stdio loop
# ---------------------------------------------------------------------------

def _send(msg: dict) -> None:
    """Write a JSON-RPC message to stdout."""
    raw = json.dumps(msg, ensure_ascii=False)
    sys.stdout.write(raw + "\n")
    sys.stdout.flush()


def _make_response(req_id: Any, result: Any) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _make_error(req_id: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def main() -> None:
    """Run the MCP server on stdin/stdout."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        stream=sys.stderr,  # logs go to stderr, JSON-RPC goes to stdout
    )
    logger.info("finagent-market MCP server starting (stdio, %d tools)", len(_TOOLS))

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            _send(_make_error(None, -32700, "Parse error"))
            continue

        req_id = msg.get("id")
        method = msg.get("method", "")

        # --- MCP lifecycle ---
        if method == "initialize":
            _send(_make_response(req_id, {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "finagent-market",
                    "version": "2.1.0",
                },
            }))

        elif method == "notifications/initialized":
            pass  # no response needed

        elif method == "tools/list":
            _send(_make_response(req_id, {"tools": _TOOLS}))

        elif method == "tools/call":
            params = msg.get("params", {})
            tool_name = params.get("name", "")
            tool_args = params.get("arguments", {})
            try:
                result = _handle_tool_call(tool_name, tool_args)
                content_text = json.dumps(result, ensure_ascii=False, default=str)
                _send(_make_response(req_id, {
                    "content": [{"type": "text", "text": content_text}],
                }))
            except Exception as exc:
                logger.exception("Tool %s failed", tool_name)
                _send(_make_response(req_id, {
                    "content": [{"type": "text", "text": f"Error: {exc}"}],
                    "isError": True,
                }))

        elif method == "ping":
            _send(_make_response(req_id, {}))

        else:
            _send(_make_error(req_id, -32601, f"Method not found: {method}"))


if __name__ == "__main__":
    main()
