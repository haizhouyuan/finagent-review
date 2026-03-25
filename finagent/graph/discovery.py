"""Graph-driven discovery loop for investment research.

Automatically identifies blind spots in the industry chain graph,
searches for information about them, extracts triples via LLM, and
updates the graph — creating a self-expanding knowledge network.

Usage::

    from finagent.graph.discovery import run_discovery_loop
    from finagent.graph.industry_chain import IndustryChainGraph

    g = IndustryChainGraph.load()
    results = run_discovery_loop(g, max_iterations=3)
    g.save()
"""

from __future__ import annotations

import json
import logging
import subprocess
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable

from .builder import extract_triples, build_from_triples
from .industry_chain import IndustryChainGraph

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Search adapters
# ------------------------------------------------------------------

def web_search(query: str, *, max_results: int = 5) -> str:
    """Search the web using curl + search engine, return combined text.

    Uses Bing/Google search API or falls back to a simple curl scrape.
    For production, wire this to your preferred search API.
    """
    # Use the search_web-style approach: curl a search engine
    try:
        env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
        env["PATH"] = os.environ.get("PATH", "/usr/bin")

        # Use Bing search (more lenient than Google for scraping)
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

        # Extract text snippets from search results
        # Remove HTML tags and get meaningful text
        text = re.sub(r"<script[^>]*>.*?</script>", "", raw, flags=re.DOTALL)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()

        # Take first 4000 chars of meaningful content
        if len(text) > 4000:
            text = text[:4000]

        return text

    except Exception as exc:
        logger.warning("web search failed for '%s': %s", query, exc)
        return ""


def search_with_context(target: str, context: str = "商业航天") -> str:
    """Generate a contextual search query and execute it."""
    queries = [
        f"{target} {context} 产业链 供应商 客户",
        f"{target} 核心技术 竞争格局 研报",
    ]
    combined = []
    for q in queries:
        result = web_search(q)
        if result:
            combined.append(f"[搜索: {q}]\n{result}")
    return "\n\n".join(combined)


# ------------------------------------------------------------------
# LLM adapters
# ------------------------------------------------------------------

def make_chatgptrest_llm_fn() -> Callable[[str, str], str]:
    """Create an LLM function that uses ChatgptREST advisor.

    Returns a function(system_prompt, user_prompt) -> str.
    """
    def llm_fn(system_prompt: str, user_prompt: str) -> str:
        """Call ChatgptREST advisor for triple extraction."""
        try:
            # Import here to avoid circular deps
            import requests as _requests

            combined = f"{system_prompt}\n\n---\n\n{user_prompt}"
            # Use advisor_ask via HTTP (not MCP, to avoid dependency)
            resp = _requests.post(
                "http://127.0.0.1:18712/api/ask",
                json={
                    "question": combined,
                    "preset": "auto",
                    "idempotency_key": f"discovery-{datetime.now(timezone.utc).isoformat()}",
                },
                timeout=120,
            )
            if resp.ok:
                data = resp.json()
                job_id = data.get("job_id")
                if job_id:
                    # Poll for result
                    import time
                    for _ in range(60):
                        time.sleep(2)
                        status_resp = _requests.get(
                            f"http://127.0.0.1:18712/api/job/{job_id}",
                            timeout=10,
                        )
                        if status_resp.ok:
                            job = status_resp.json()
                            if job.get("status") == "completed":
                                answer = _requests.get(
                                    f"http://127.0.0.1:18712/api/answer/{job_id}",
                                    params={"offset": 0, "max_chars": 8000},
                                    timeout=10,
                                )
                                if answer.ok:
                                    return answer.json().get("text", "")
                            elif job.get("status") in ("failed", "cancelled"):
                                break
                    logger.warning("ChatgptREST job %s did not complete", job_id)
            return ""
        except Exception as exc:
            logger.error("ChatgptREST LLM call failed: %s", exc)
            return ""

    return llm_fn


def make_simple_llm_fn() -> Callable[[str, str], str]:
    """Create a simple LLM function using subprocess + curl to OpenAI API.

    Uses OPENAI_API_KEY from env if available.
    """
    def llm_fn(system_prompt: str, user_prompt: str) -> str:
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            logger.warning("OPENAI_API_KEY not set, LLM extraction unavailable")
            return ""

        payload = json.dumps({
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
        })

        try:
            result = subprocess.run(
                ["curl", "-s", "--connect-timeout", "10", "-m", "60",
                 "-H", "Content-Type: application/json",
                 "-H", f"Authorization: Bearer {api_key}",
                 "-d", payload,
                 "https://api.openai.com/v1/chat/completions"],
                capture_output=True, timeout=65,
            )
            data = json.loads(result.stdout.decode())
            return data["choices"][0]["message"]["content"]
        except Exception as exc:
            logger.error("OpenAI API call failed: %s", exc)
            return ""

    return llm_fn


# ------------------------------------------------------------------
# Discovery Loop
# ------------------------------------------------------------------

class DiscoveryResult:
    """Result from one iteration of the discovery loop."""

    def __init__(self, target: str, triples: list[dict], search_text_len: int):
        self.target = target
        self.triples = triples
        self.search_text_len = search_text_len
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def __repr__(self) -> str:
        return (f"DiscoveryResult(target='{self.target}', "
                f"triples={len(self.triples)}, "
                f"search_chars={self.search_text_len})")


def run_discovery_loop(
    graph: IndustryChainGraph,
    *,
    llm_fn: Callable[[str, str], str] | None = None,
    search_fn: Callable[[str], str] | None = None,
    context: str = "商业航天",
    max_iterations: int = 3,
    min_confidence: float = 0.5,
    skip_types: set[str] | None = None,
    verbose: bool = True,
) -> list[DiscoveryResult]:
    """Run the graph-driven discovery loop.

    1. Find blind spots (nodes with low info density)
    2. Search web for information about the blind spot
    3. Use LLM to extract industry chain triples
    4. Add triples to graph
    5. Repeat until no more blind spots or max_iterations reached

    Args:
        graph: IndustryChainGraph instance to expand.
        llm_fn: Function(system, user) -> str for LLM calls.
        search_fn: Function(query) -> str for web search.
        context: Domain context for search queries (e.g. "商业航天").
        max_iterations: Maximum number of blind spots to process.
        min_confidence: Minimum confidence for triple inclusion.
        skip_types: Node types to skip (e.g. {"sector"} to skip sector nodes).
        verbose: Print progress to stdout.

    Returns:
        List of DiscoveryResult objects.
    """
    if search_fn is None:
        search_fn = lambda target: search_with_context(target, context)
    if skip_types is None:
        skip_types = {"sector"}  # Sectors are abstract, not worth searching

    results: list[DiscoveryResult] = []
    processed: set[str] = set()

    def _log(msg: str) -> None:
        if verbose:
            print(msg)
        logger.info(msg)

    _log(f"🚀 启动图谱驱动的深挖引擎 (Discovery Loop)")
    _log(f"   context={context}, max_iterations={max_iterations}")
    _log(f"   当前图谱: {graph.g.number_of_nodes()} nodes, "
         f"{graph.g.number_of_edges()} edges")

    for i in range(max_iterations):
        # 1. Find blind spots
        spots = graph.blind_spots()

        # Filter: skip already processed, skip certain types
        candidates = [
            s for s in spots
            if s["node"] not in processed
            and s.get("node_type", "") not in skip_types
        ]

        if not candidates:
            _log("✅ 没有更多可探索的盲区，图谱已充分填充！")
            break

        target = candidates[0]
        target_name = target["node"]
        processed.add(target_name)

        _log(f"\n🔍 [迭代 {i+1}/{max_iterations}] 盲区: {target_name} "
             f"(score={target['info_score']}, type={target['node_type']})")

        # 2. Search for information
        _log(f"   📡 搜索中: {target_name} {context} ...")
        search_text = search_fn(target_name)

        if not search_text or len(search_text) < 50:
            _log(f"   ⚠️ 搜索结果太少，跳过")
            results.append(DiscoveryResult(target_name, [], len(search_text or "")))
            continue

        _log(f"   📄 获取到 {len(search_text)} 字符的搜索结果")

        # 3. LLM extraction (if llm_fn provided)
        if llm_fn is None:
            _log(f"   ⚠️ 未配置 LLM，跳过三元组提取（仅搜索）")
            results.append(DiscoveryResult(target_name, [], len(search_text)))
            continue

        _log(f"   🤖 大模型提取三元组中...")
        triples = extract_triples(search_text, llm_fn=llm_fn)

        if not triples:
            _log(f"   ⚠️ 未提取到有效三元组")
            results.append(DiscoveryResult(target_name, [], len(search_text)))
            continue

        # 4. Inject into graph
        added = build_from_triples(
            graph, triples,
            source=f"discovery-loop-{i+1}:{target_name}",
            min_confidence=min_confidence,
        )

        _log(f"   ✅ 成功注入 {added}/{len(triples)} 条新关系！")

        # Show what was discovered
        for t in triples[:5]:
            _log(f"      {t['head']} --[{t['relation']}]--> {t['tail']} "
                 f"(confidence={t.get('confidence', '?')})")
        if len(triples) > 5:
            _log(f"      ... 还有 {len(triples)-5} 条关系")

        results.append(DiscoveryResult(target_name, triples, len(search_text)))

        # 5. Save after each iteration
        graph.save()
        _log(f"   💾 图谱已保存: {graph.g.number_of_nodes()} nodes, "
             f"{graph.g.number_of_edges()} edges")

    # Summary
    total_triples = sum(len(r.triples) for r in results)
    _log(f"\n{'='*60}")
    _log(f"🏁 Discovery Loop 完成")
    _log(f"   探索了 {len(results)} 个盲区")
    _log(f"   共提取 {total_triples} 条新关系")
    _log(f"   图谱: {graph.g.number_of_nodes()} nodes, "
         f"{graph.g.number_of_edges()} edges")
    _log(f"   剩余盲区: {len(graph.blind_spots())}")

    return results


def run_discovery_demo(
    graph: IndustryChainGraph | None = None,
    *,
    context: str = "商业航天",
    max_iterations: int = 2,
) -> list[DiscoveryResult]:
    """Run a demo discovery loop using web search only (no LLM).

    Useful for testing the search pipeline before wiring up LLM.
    """
    if graph is None:
        graph = IndustryChainGraph.load()

    def demo_search(target: str) -> str:
        """Search and return raw text."""
        return search_with_context(target, context)

    results = run_discovery_loop(
        graph,
        search_fn=demo_search,
        llm_fn=None,  # No LLM, just search
        context=context,
        max_iterations=max_iterations,
        verbose=True,
    )

    return results
