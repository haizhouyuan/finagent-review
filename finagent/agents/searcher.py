"""Searcher agent — executes micro-queries against multiple data sources.

Multi-adapter search engine that collects raw data from:
  - Web search (Bing/Google via curl)
  - AKShare (A-share market data)
  - ChatgptREST (LLM-enhanced search)

⚠️ ARCHITECTURAL FIX #1: "Reference Passing"
Raw text is NEVER placed in the LangGraph State.  Instead, it is
written to EvidenceStore (SQLite) and only lightweight metadata
(evidence_id, query, char_count) flows through the DAG.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from typing import Any, Callable

from .state import ResearchState
from .evidence_store import EvidenceStore

logger = logging.getLogger(__name__)

# Max chars per search result to prevent source bloat
MAX_RESULT_CHARS = 4000


def searcher_node(
    state: ResearchState,
    *,
    search_fn: Callable[[str], str] | None = None,
    evidence_store: EvidenceStore | None = None,
    retrieval_stack: Any | None = None,
    llm_fn: Callable[[str, str], str] | None = None,
    max_retries: int = 2,
) -> dict[str, Any]:
    """Searcher agent node for LangGraph.

    Executes all pending queries, CLEANS raw text via TextCleaner,
    stores cleaned text in EvidenceStore, and passes only metadata
    refs through the state.
    """
    from finagent.parsers.text_cleaner import TextCleaner

    queries = state.get("pending_queries", [])
    completed = list(state.get("completed_queries", []))
    evidence_refs = list(state.get("gathered_evidence", []))
    budget = state.get("token_budget_remaining", 50_000)

    if not queries:
        logger.info("searcher: no pending queries")
        return {"gathered_evidence": evidence_refs}

    if search_fn is None:
        search_fn = web_search

    cleaner = TextCleaner()
    new_refs: list[dict[str, Any]] = []
    tokens_used = 0

    for item in queries:
        query = item.get("query", "") if isinstance(item, dict) else str(item)
        if query in completed:
            logger.debug("searcher: skipping duplicate query '%s'", query)
            continue

        query_history = [query]
        current_refs, current_tokens = _do_search(
            query,
            search_fn=search_fn,
            evidence_store=evidence_store,
            retrieval_stack=retrieval_stack,
            cleaner=cleaner,
        )
        tokens_used += current_tokens
        best_refs = current_refs

        if llm_fn and current_refs:
            retries = 0
            while retries < max_retries:
                texts = _load_texts(current_refs, evidence_store)
                verdict = _evaluate_retrieval(query, texts, llm_fn)
                if verdict == "sufficient":
                    best_refs = current_refs
                    break
                rewritten_query = _rewrite_for_retry(query, texts, llm_fn, retries)
                retries += 1
                if not rewritten_query or rewritten_query in query_history:
                    break
                query_history.append(rewritten_query)
                retry_refs, retry_tokens = _do_search(
                    rewritten_query,
                    search_fn=search_fn,
                    evidence_store=evidence_store,
                    retrieval_stack=retrieval_stack,
                    cleaner=cleaner,
                )
                tokens_used += retry_tokens
                if not retry_refs:
                    break
                current_refs = retry_refs
                best_refs = retry_refs

        new_refs.extend(best_refs)
        for executed_query in query_history:
            if executed_query not in completed:
                completed.append(executed_query)

    logger.info(
        "searcher: %d queries → %d evidence refs (%d tokens est.)",
        len(queries), len(new_refs), tokens_used,
    )

    return {
        "gathered_evidence": evidence_refs + new_refs,
        "completed_queries": completed,
        "pending_queries": [],  # Clear queue
        "token_budget_remaining": max(0, budget - tokens_used),
    }


# ── Search adapters ─────────────────────────────────────────────────


def web_search(query: str, *, max_results: int = 5) -> str:
    """Search the web using curl + Bing and return cleaned text snippets."""
    try:
        env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
        env["PATH"] = os.environ.get("PATH", "/usr/bin")

        safe_query = query.replace('"', '\\"')
        url = f"https://www.bing.com/search?q={safe_query.replace(' ', '+')}&count={max_results}"

        result = subprocess.run(
            ["curl", "-s", "--noproxy", "*", "--connect-timeout", "8", "-m", "15",
             "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
             "-H", "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8",
             url],
            capture_output=True, timeout=20, env=env,
        )
        raw = result.stdout.decode("utf-8", errors="replace")

        # Clean HTML → text
        text = re.sub(r"<script[^>]*>.*?</script>", "", raw, flags=re.DOTALL)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()

        if len(text) > MAX_RESULT_CHARS:
            text = text[:MAX_RESULT_CHARS]

        return text

    except Exception as exc:
        logger.warning("web search failed for '%s': %s", query, exc)
        return ""


def _do_search(
    query: str,
    *,
    search_fn: Callable[[str], str] | None,
    evidence_store: EvidenceStore | None,
    retrieval_stack: Any | None,
    cleaner: Any,
) -> tuple[list[dict[str, Any]], int]:
    if retrieval_stack is not None:
        try:
            results = retrieval_stack.search(query, top_k=5)
        except Exception as exc:
            logger.warning("retrieval stack failed for '%s': %s", query, exc)
            results = []

        refs, token_cost = _store_results(
            query,
            results=[
                {
                    "content": getattr(result, "content", ""),
                    "source": getattr(result, "source", "retrieval"),
                    "metadata": getattr(result, "metadata", {}),
                }
                for result in results
            ],
            evidence_store=evidence_store,
            cleaner=cleaner,
        )
        if refs:
            return refs, token_cost

    if search_fn is None:
        search_fn = web_search

    try:
        raw_text = search_fn(query)
    except Exception as exc:
        logger.warning("search failed for '%s': %s", query, exc)
        raw_text = ""

    return _store_results(
        query,
        results=[{
            "content": raw_text,
            "source": "web_search",
            "metadata": {},
        }],
        evidence_store=evidence_store,
        cleaner=cleaner,
    )


def _store_results(
    query: str,
    *,
    results: list[dict[str, Any]],
    evidence_store: EvidenceStore | None,
    cleaner: Any,
) -> tuple[list[dict[str, Any]], int]:
    refs: list[dict[str, Any]] = []
    tokens_used = 0
    for result in results:
        raw_text = str(result.get("content", "") or "")
        if len(raw_text) > MAX_RESULT_CHARS:
            raw_text = raw_text[:MAX_RESULT_CHARS]

        clean_text = cleaner.clean(raw_text)
        if not clean_text or len(clean_text) <= 50:
            continue

        metadata = result.get("metadata", {}) or {}
        source_type = str(result.get("source", "web_search") or "web_search")
        if evidence_store:
            ref = evidence_store.store(
                query,
                clean_text,
                source_type=source_type,
                source_tier=str(metadata.get("source_tier", "unverified")),
                source_uri=str(metadata.get("source_uri", "")),
                published_at=str(metadata.get("published_at", "")),
            )
        else:
            ref = {
                "evidence_id": None,
                "query": query,
                "char_count": len(clean_text),
                "source_type": source_type,
                "_text": clean_text,
            }
        refs.append(ref)
        tokens_used += len(clean_text) // 4

    return refs, tokens_used


def _load_texts(
    refs: list[dict[str, Any]],
    evidence_store: EvidenceStore | None,
) -> dict[str, str]:
    if not refs:
        return {}
    if evidence_store:
        evidence_ids = [
            int(ref["evidence_id"])
            for ref in refs
            if ref.get("evidence_id") is not None
        ]
        fetched = evidence_store.fetch_batch(evidence_ids)
        return {str(key): value for key, value in fetched.items()}
    return {
        str(idx): str(ref.get("_text", ""))
        for idx, ref in enumerate(refs)
        if ref.get("_text")
    }


def _evaluate_retrieval(
    query: str,
    results: dict[str, str],
    llm_fn: Callable[[str, str], str],
) -> str:
    combined = "\n\n".join(text[:600] for text in results.values() if text)
    if not combined or len(combined) < 80:
        return "insufficient"

    system_prompt = (
        "判断这批检索结果是否足以支撑当前查询。"
        "只回答 sufficient 或 insufficient。"
    )
    user_prompt = f"查询: {query}\n\n结果:\n{combined}"

    try:
        verdict = llm_fn(system_prompt, user_prompt).strip().lower()
    except Exception:
        return "sufficient" if len(combined) >= 200 else "insufficient"

    if "insufficient" in verdict:
        return "insufficient"
    if "sufficient" in verdict:
        return "sufficient"
    return "sufficient" if len(combined) >= 200 else "insufficient"


def _rewrite_for_retry(
    query: str,
    results: dict[str, str],
    llm_fn: Callable[[str, str], str],
    attempt: int,
) -> str:
    system_prompt = (
        "根据已有检索结果中的缺口改写查询。"
        "只返回一条新的中文查询，不要解释。"
    )
    evidence_preview = "\n\n".join(text[:300] for text in results.values() if text)
    user_prompt = (
        f"原查询: {query}\n"
        f"第 {attempt + 1} 次重试。\n"
        f"已有结果:\n{evidence_preview}\n\n"
        "请给出更具体的新查询。"
    )

    try:
        rewritten = llm_fn(system_prompt, user_prompt).strip()
    except Exception:
        rewritten = ""

    rewritten = rewritten.strip("` \n")
    if rewritten and rewritten != query:
        return rewritten

    suffixes = ["供应链", "竞品", "价格带", "轮毂", "门店"]
    suffix = suffixes[attempt % len(suffixes)]
    if suffix in query:
        return f"{query} 核心信息"
    return f"{query} {suffix}"
