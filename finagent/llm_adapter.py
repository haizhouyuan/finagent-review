"""LLM Adapter Layer for finagent v2.

Architecture boundary:
  - finagent owns: research DAG, evidence, graph, thesis, products
  - LLM adapters own: model execution, format compliance
  - ChatgptREST is OPTIONAL: only when platform governance/telemetry is needed

All adapters implement the same interface:
    llm_fn(system_prompt: str, user_prompt: str) -> str

Usage:
    from finagent.llm_adapter import create_llm_adapter

    # Direct LLM (default, recommended for internal research nodes)
    llm = create_llm_adapter("openai", model="gpt-4o-mini")
    llm = create_llm_adapter("openai-compatible", base_url="...", api_key="...")

    # ChatgptREST (optional, for platform governance)
    llm = create_llm_adapter("chatgptrest", goal_hint="research")

    # Mock (testing, zero cost)
    llm = create_llm_adapter("mock")
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Type alias for the LLM function interface
LLMFunction = Callable[[str, str], str]


# ── Registry ────────────────────────────────────────────────────────

_ADAPTERS: dict[str, Callable[..., LLMFunction]] = {}


def register(name: str):
    """Decorator to register an LLM adapter factory."""
    def wrapper(fn: Callable[..., LLMFunction]) -> Callable[..., LLMFunction]:
        _ADAPTERS[name] = fn
        return fn
    return wrapper


def create_llm_adapter(backend: str, **kwargs) -> LLMFunction:
    """Create an LLM adapter by backend name.

    Args:
        backend: One of "openai", "openai-compatible", "chatgptrest", "mock"
        **kwargs: Backend-specific configuration

    Returns:
        A function(system_prompt, user_prompt) -> str
    """
    factory = _ADAPTERS.get(backend)
    if factory is None:
        available = ", ".join(sorted(_ADAPTERS.keys()))
        raise ValueError(
            f"Unknown LLM backend: {backend!r}. Available: {available}"
        )
    return factory(**kwargs)


def list_backends() -> list[str]:
    """Return list of registered backend names."""
    return sorted(_ADAPTERS.keys())


# ── OpenAI-Compatible Adapter ────────────────────────────────────────


@register("openai")
def _make_openai_adapter(
    *,
    model: str = "",
    api_key: str = "",
    base_url: str = "",
    temperature: float = 0.1,
    max_tokens: int = 4096,
    timeout: int = 120,
) -> LLMFunction:
    """Direct OpenAI API adapter.

    Env vars:
        OPENAI_API_KEY: API key (required if api_key not passed)
        OPENAI_BASE_URL: Base URL (optional, for proxies)
        FINAGENT_LLM_MODEL: Model name (default: gpt-4o-mini)
    """
    _key = api_key or os.environ.get("OPENAI_API_KEY", "")
    _base = base_url or os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
    _model = model or os.environ.get("FINAGENT_LLM_MODEL", "gpt-4o-mini")

    def llm_fn(system: str, user: str) -> str:
        if not _key:
            raise RuntimeError(
                "OPENAI_API_KEY not set. Pass api_key= or set the env var."
            )
        import requests
        resp = requests.post(
            f"{_base.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": _model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    return llm_fn


@register("openai-compatible")
def _make_openai_compatible_adapter(
    *,
    base_url: str = "",
    api_key: str = "",
    model: str = "",
    temperature: float = 0.1,
    max_tokens: int = 4096,
    timeout: int = 120,
) -> LLMFunction:
    """OpenAI-compatible API adapter (works with vLLM, Ollama, LiteLLM, etc).

    Env vars:
        FINAGENT_LLM_BASE_URL: API base URL (required)
        FINAGENT_LLM_API_KEY: API key (optional, some local servers don't need it)
        FINAGENT_LLM_MODEL: Model name
    """
    _base = base_url or os.environ.get("FINAGENT_LLM_BASE_URL", "")
    _key = api_key or os.environ.get("FINAGENT_LLM_API_KEY", "sk-placeholder")
    _model = model or os.environ.get("FINAGENT_LLM_MODEL", "default")

    if not _base:
        raise ValueError(
            "base_url is required for openai-compatible adapter. "
            "Set FINAGENT_LLM_BASE_URL or pass base_url=."
        )

    # Reuse the OpenAI adapter with custom base URL
    return _make_openai_adapter(
        model=_model, api_key=_key, base_url=_base,
        temperature=temperature, max_tokens=max_tokens, timeout=timeout,
    )


# ── ChatgptREST Adapter (Optional) ──────────────────────────────────

# Token/URL resolution helpers — mirrors event_extraction.py pattern

from pathlib import Path


def _load_chatgptrest_env_file(path: Path) -> dict[str, str]:
    """Parse KEY=VALUE pairs from an env file."""
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values


def _load_chatgptrest_env_fallback() -> dict[str, str]:
    """Load ChatgptREST config from env files (same chain as event_extraction.py)."""
    explicit = os.environ.get("CHATGPTREST_CREDENTIALS_ENV", "").strip()
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    candidates.extend([
        Path.home() / ".config" / "chatgptrest" / "chatgptrest.env",
        Path("/vol1/maint/MAIN/secrets/credentials.env"),
    ])
    merged: dict[str, str] = {}
    for path in candidates:
        values = _load_chatgptrest_env_file(path)
        for key in ("CHATGPTREST_BASE_URL", "CHATGPTREST_URL", "CHATGPTREST_API_TOKEN"):
            if key in values and key not in merged:
                merged[key] = values[key]
    return merged


def _resolve_chatgptrest_token() -> str:
    """Resolve ChatgptREST API token from env vars and env files."""
    token = os.environ.get("CHATGPTREST_API_TOKEN", "").strip()
    if token:
        return token
    fallback = _load_chatgptrest_env_fallback()
    return fallback.get("CHATGPTREST_API_TOKEN", "").strip()


@register("chatgptrest")
def _make_chatgptrest_adapter(
    *,
    base_url: str = "",
    api_token: str = "",
    goal_hint: str = "research",
    depth: str = "standard",
    timeout: int = 300,
    client_name: str = "finagent-research",
) -> LLMFunction:
    """ChatgptREST adapter via /v3/agent/turn.

    Uses the public agent facade, not deprecated internal endpoints.
    Only appropriate when you need platform governance, routing, and telemetry.

    Auth token resolution order:
      1. Explicit `api_token` kwarg
      2. CHATGPTREST_API_TOKEN env var
      3. Env files: ~/.config/chatgptrest/chatgptrest.env,
         /vol1/maint/MAIN/secrets/credentials.env

    WARNING: /v3/agent/turn is a user-level front door with
    intake/clarify/session semantics. For structured internal reasoning,
    prefer "openai" or "openai-compatible" backends.

    Env vars:
        CHATGPTREST_API_URL / CHATGPTREST_BASE_URL: API base URL
        CHATGPTREST_API_TOKEN: Bearer token for authentication
    """
    _base = (
        base_url
        or os.environ.get("CHATGPTREST_API_URL", "").strip()
        or os.environ.get("CHATGPTREST_BASE_URL", "").strip()
    )
    _token = api_token or _resolve_chatgptrest_token()

    if not _base:
        # Try env file fallback for base URL too
        fallback = _load_chatgptrest_env_fallback()
        _base = (
            fallback.get("CHATGPTREST_BASE_URL", "").strip()
            or fallback.get("CHATGPTREST_URL", "").strip()
            or "http://127.0.0.1:18711"
        )

    def llm_fn(system: str, user: str) -> str:
        import requests

        # Combine system+user into a single message for the agent turn
        message = f"[System Instructions]\n{system}\n\n[Task]\n{user}"

        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "X-Client-Name": client_name,
        }
        if _token:
            headers["Authorization"] = f"Bearer {_token}"

        resp = requests.post(
            f"{_base.rstrip('/')}/v3/agent/turn",
            headers=headers,
            json={
                "message": message,
                "goal_hint": goal_hint,
                "depth": depth,
                "timeout_seconds": timeout,
                "client": {"name": client_name},
            },
            timeout=timeout + 30,  # HTTP timeout > agent timeout
        )

        # Auth failure: raise immediately, don't silently return empty
        if resp.status_code == 401:
            raise RuntimeError(
                "ChatgptREST 401 Unauthorized: set CHATGPTREST_API_TOKEN "
                "or add token to ~/.config/chatgptrest/chatgptrest.env"
            )
        resp.raise_for_status()
        data = resp.json()

        status = data.get("status", "")
        if status == "completed":
            answer = data.get("answer", "")
            if answer:
                return answer
            raise RuntimeError(
                f"ChatgptREST completed but returned empty answer "
                f"(run_id={data.get('run_id')})"
            )

        # Fail-closed: non-completed statuses are errors for internal
        # research nodes. Do NOT silently return empty string.
        recovery = data.get("recovery_status", {})
        raise RuntimeError(
            f"ChatgptREST returned non-completed status={status!r} "
            f"(run_id={data.get('run_id')}, "
            f"recovery={recovery.get('final_state', 'unknown')}). "
            f"For internal research nodes, consider using 'openai' backend instead."
        )

    return llm_fn


# ── Mock Adapter ─────────────────────────────────────────────────────


@register("mock")
def _make_mock_adapter(**kwargs) -> LLMFunction:
    """Mock LLM for pipeline testing without API costs.

    Detects planner vs extractor prompts and returns appropriately
    formatted responses.
    """

    def mock_llm(system: str, user: str) -> str:
        # Detect planner prompt (has 规划师 or queries schema)
        if "规划师" in system or "queries" in system:
            return json.dumps({
                "analysis": "图谱为空，需要从基础信息开始收集",
                "missing": ["核心企业列表", "供应链关系"],
                "superfluous": [],
                "queries": [
                    {"query": "商业航天 核心供应商 产业链", "priority": 1,
                     "target_entity": "商业航天", "expected_info": "核心供应商名单"},
                    {"query": "蓝箭航天 星河动力 供应链 零部件", "priority": 2,
                     "target_entity": "蓝箭航天", "expected_info": "零部件供应商"},
                ],
                "confidence": 0.3,
            }, ensure_ascii=False)

        # Extractor prompt: return triples from entity mentions in text
        text = user
        triples = []
        entity_pattern = re.compile(
            r"(蓝箭航天|星河动力|中科宇航|航天电器|西部超导|铖昌科技"
            r"|千帆星座|垣信卫星|银河航天|SpaceX)"
        )
        entities = entity_pattern.findall(text)
        seen: set[tuple[str, str]] = set()
        for i in range(len(entities)):
            for j in range(i + 1, min(i + 3, len(entities))):
                pair = (entities[i], entities[j])
                if pair not in seen and entities[i] != entities[j]:
                    seen.add(pair)
                    start = text.find(entities[i])
                    end = text.find(entities[j], start) + len(entities[j])
                    if start >= 0 and end > start:
                        quote = text[start:min(end + 10, len(text))].strip()[:80]
                    else:
                        quote = entities[i]
                    triples.append({
                        "head": entities[i], "head_type": "company",
                        "relation": "related_to",
                        "tail": entities[j], "tail_type": "company",
                        "exact_quote": quote,
                        "confidence": 0.8, "valid_from": "2024",
                    })
        return json.dumps(triples[:5], ensure_ascii=False)

    return mock_llm


# ── Auto-Detect ──────────────────────────────────────────────────────


def auto_detect_adapter(**kwargs) -> LLMFunction:
    """Auto-detect the best available LLM adapter.

    Priority:
      1. OPENAI_API_KEY set → "openai"
      2. FINAGENT_LLM_BASE_URL set → "openai-compatible"
      3. ChatgptREST reachable AND authenticated → "chatgptrest"
      4. Fall back to "mock"
    """
    if os.environ.get("OPENAI_API_KEY"):
        logger.info("Auto-detected: OpenAI (OPENAI_API_KEY set)")
        return create_llm_adapter("openai", **kwargs)

    if os.environ.get("FINAGENT_LLM_BASE_URL"):
        logger.info("Auto-detected: OpenAI-compatible (FINAGENT_LLM_BASE_URL set)")
        return create_llm_adapter("openai-compatible", **kwargs)

    # Try ChatgptREST — must pass BOTH health check AND auth check
    try:
        import requests
        base = (
            os.environ.get("CHATGPTREST_API_URL", "").strip()
            or os.environ.get("CHATGPTREST_BASE_URL", "").strip()
        )
        if not base:
            fallback = _load_chatgptrest_env_fallback()
            base = (
                fallback.get("CHATGPTREST_BASE_URL", "").strip()
                or fallback.get("CHATGPTREST_URL", "").strip()
                or "http://127.0.0.1:18711"
            )

        # Step 1: health check
        resp = requests.get(f"{base.rstrip('/')}/healthz", timeout=2)
        if not resp.ok:
            raise ConnectionError("healthz not ok")

        # Step 2: auth check — resolve token and verify we can authenticate
        token = _resolve_chatgptrest_token()
        if not token:
            logger.info(
                "Auto-detect: ChatgptREST reachable but no API token found. "
                "Skipping (would get 401). Set CHATGPTREST_API_TOKEN."
            )
            raise ConnectionError("no token")

        # Step 3: verify token works with a lightweight authenticated endpoint
        headers = {"Authorization": f"Bearer {token}"}
        status_resp = requests.get(
            f"{base.rstrip('/')}/v1/ops/status",
            headers=headers, timeout=3,
        )
        if status_resp.status_code == 401:
            logger.warning(
                "Auto-detect: ChatgptREST reachable but token rejected (401). "
                "Skipping."
            )
            raise ConnectionError("401")

        logger.info("Auto-detected: ChatgptREST (health + auth verified)")
        return create_llm_adapter("chatgptrest", api_token=token, **kwargs)

    except Exception:
        pass

    logger.warning("Auto-detected: Mock (no LLM provider available)")
    return create_llm_adapter("mock")
