"""Unified retrieval: memory + graph + evidence -> compress."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from finagent.agents.evidence_store import _tokenize_cjk
from finagent.graph_v2.retrieval import GraphRetriever


@dataclass(frozen=True)
class RetrievalResult:
    source: str
    query: str
    content: str
    score: float
    metadata: dict[str, Any] = field(default_factory=dict)


class RetrievalStack:
    def __init__(
        self,
        *,
        graph_store=None,
        memory=None,
        evidence_store=None,
        llm_fn=None,
        enable_query_rewrite: bool = False,
        enable_llm_rerank: bool = False,
        enable_light_rerank: bool = False,
    ):
        self.graph_store = graph_store
        self.graph_retriever = GraphRetriever(graph_store) if graph_store else None
        self.memory = memory
        self.evidence_store = evidence_store
        self.llm_fn = llm_fn
        self.enable_query_rewrite = enable_query_rewrite
        self.enable_llm_rerank = enable_llm_rerank
        self.enable_light_rerank = enable_light_rerank

    def retrieve(self, query: str, *, top_k: int = 5, max_chars: int = 4000) -> str:
        queries = self._queries_for(query)
        results = self._search(queries)
        ranked = self._rank(query, results, top_k)
        return self._compress(query, ranked, max_chars)

    def search(self, query: str, *, top_k: int = 5) -> list[RetrievalResult]:
        return self._rank(query, self._search(self._queries_for(query)), top_k)

    def _queries_for(self, query: str) -> list[str]:
        if self.llm_fn and self.enable_query_rewrite:
            return self._rewrite(query)
        return [query]

    def _rewrite(self, query: str) -> list[str]:
        prompt = (
            "将用户查询扩展成最多3条更适合检索的子查询，返回 JSON 数组，"
            '例如 ["query1", "query2"]。'
        )
        try:
            raw = self.llm_fn(prompt, query)
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                expanded = [str(item).strip() for item in parsed if str(item).strip()]
                if expanded:
                    return _dedupe_queries([query, *expanded])
        except Exception:
            pass
        return [query]

    def _search(self, queries: list[str]) -> list[RetrievalResult]:
        results: list[RetrievalResult] = []
        results.extend(self._memory_results(queries))
        results.extend(self._graph_results(queries))
        results.extend(self._evidence_results(queries))
        return results

    def _memory_results(self, queries: list[str]) -> list[RetrievalResult]:
        if self.memory is None:
            return []
        dedup: dict[str, RetrievalResult] = {}
        for query in queries:
            for record in self.memory.recall(query, limit=8):
                result = RetrievalResult(
                    source="memory",
                    query=query,
                    content=record.content,
                    score=record.confidence + 0.2,
                    metadata={
                        "record_id": record.record_id,
                        "tier": record.tier.value,
                        "category": record.category,
                    },
                )
                dedup[record.record_id] = _pick_better(dedup.get(record.record_id), result)
        return list(dedup.values())

    def _graph_results(self, queries: list[str]) -> list[RetrievalResult]:
        if self.graph_store is None or self.graph_retriever is None:
            return []

        dedup: dict[str, RetrievalResult] = {}
        for query in queries:
            focus_nodes = []
            for token in _dedupe_queries([query, *_tokenize_cjk(query)]):
                resolved = self.graph_store.resolve_alias(token)
                if resolved:
                    focus_nodes.append(resolved)
                for node in self.graph_store.search_nodes(token, limit=3):
                    focus_nodes.append(node["node_id"])

            seen_focus = _dedupe_queries(focus_nodes)
            for node_id in seen_focus[:4]:
                context = self.graph_retriever.retrieve(query, focus_node=node_id, mode="local")
                if not context or context == "(no relevant graph context found)":
                    continue
                label = (self.graph_store.get_node(node_id) or {}).get("label", node_id)
                result = RetrievalResult(
                    source="graph",
                    query=query,
                    content=context,
                    score=1.5 + float(label in query or query in label),
                    metadata={"node_id": node_id, "label": label},
                )
                dedup[node_id] = _pick_better(dedup.get(node_id), result)

            if not dedup:
                context = self.graph_retriever.retrieve(query, mode="global")
                if context and context != "(no relevant graph context found)":
                    dedup[f"global:{query}"] = RetrievalResult(
                        source="graph",
                        query=query,
                        content=context,
                        score=1.0,
                        metadata={"mode": "global"},
                    )

        return list(dedup.values())

    def _evidence_results(self, queries: list[str]) -> list[RetrievalResult]:
        if self.evidence_store is None:
            return []
        dedup: dict[int, RetrievalResult] = {}
        for query in queries:
            refs = self.evidence_store.search(query, limit=8)
            texts = self.evidence_store.fetch_batch([ref["id"] for ref in refs])
            for ref in refs:
                evidence_id = ref["id"]
                text = texts.get(evidence_id, "")
                if not text:
                    continue
                result = RetrievalResult(
                    source="evidence",
                    query=query,
                    content=text[:1200],
                    score=float(ref.get("_score", 0.0)),
                    metadata={k: v for k, v in ref.items() if k != "_score"},
                )
                dedup[evidence_id] = _pick_better(dedup.get(evidence_id), result)
        return list(dedup.values())

    def _rerank(
        self,
        query: str,
        results: list[RetrievalResult],
        top_k: int,
    ) -> list[RetrievalResult]:
        if not results:
            return []

        previews = [
            {
                "idx": idx,
                "source": result.source,
                "score": result.score,
                "preview": result.content[:180],
            }
            for idx, result in enumerate(results)
        ]
        prompt = (
            "根据与查询的相关性为候选结果打分，返回 JSON 对象 "
            '如 {"scores": [{"idx": 0, "score": 1.0}] }。'
        )
        try:
            raw = self.llm_fn(prompt, json.dumps({"query": query, "results": previews}, ensure_ascii=False))
            parsed = json.loads(raw)
            score_map = {
                int(item["idx"]): float(item["score"])
                for item in parsed.get("scores", [])
            }
            reranked = [
                RetrievalResult(
                    source=result.source,
                    query=result.query,
                    content=result.content,
                    score=score_map.get(idx, result.score),
                    metadata=result.metadata,
                )
                for idx, result in enumerate(results)
            ]
            return self._sort_by_score(reranked, top_k)
        except Exception:
            return self._sort_by_score(results, top_k)

    def _light_rerank(
        self,
        query: str,
        results: list[RetrievalResult],
        top_k: int,
    ) -> list[RetrievalResult]:
        tokens = _dedupe_queries([query, *_tokenize_cjk(query)])
        reranked: list[RetrievalResult] = []
        for result in results:
            boost = 0.0
            content = result.content
            if query in content:
                boost += 1.5
            for token in tokens[:10]:
                if len(token) < 2:
                    continue
                if token in content:
                    boost += 0.25 if len(token) <= 3 else 0.4
            if result.source == "memory":
                boost += 0.1
            reranked.append(
                RetrievalResult(
                    source=result.source,
                    query=result.query,
                    content=result.content,
                    score=result.score + boost,
                    metadata=result.metadata,
                )
            )
        return self._sort_by_score(reranked, top_k)

    def _rank(
        self,
        query: str,
        results: list[RetrievalResult],
        top_k: int,
    ) -> list[RetrievalResult]:
        if self.llm_fn and self.enable_llm_rerank:
            return self._rerank(query, results, top_k)
        if self.enable_light_rerank:
            return self._light_rerank(query, results, top_k)
        return self._sort_by_score(results, top_k)

    def _sort_by_score(
        self,
        results: list[RetrievalResult],
        top_k: int,
    ) -> list[RetrievalResult]:
        ranked = sorted(
            results,
            key=lambda result: (-result.score, result.source, result.query),
        )
        if top_k <= 0:
            return []

        selected: list[RetrievalResult] = []
        used_ids: set[tuple[str, str, str]] = set()
        seen_sources: set[str] = set()

        for result in ranked:
            key = (result.source, result.query, result.content[:80])
            if result.source in seen_sources:
                continue
            seen_sources.add(result.source)
            used_ids.add(key)
            selected.append(result)
            if len(selected) >= top_k:
                return selected

        for result in ranked:
            key = (result.source, result.query, result.content[:80])
            if key in used_ids:
                continue
            used_ids.add(key)
            selected.append(result)
            if len(selected) >= top_k:
                break
        return selected

    def _compress(
        self,
        query: str,
        results: list[RetrievalResult],
        max_chars: int,
    ) -> str:
        header = f"Query: {query}\n"
        if max_chars <= len(header):
            return header[:max_chars]

        parts = [header]
        remaining = max_chars - len(header)

        for result in results:
            meta = ", ".join(
                f"{key}={value}"
                for key, value in result.metadata.items()
                if key in {"tier", "category", "label", "query", "source_type", "published_at"}
                and value
            )
            block = f"[{result.source} score={result.score:.2f}"
            if meta:
                block += f" {meta}"
            block += f"]\n{result.content.strip()}\n"
            if len(block) <= remaining:
                parts.append(block)
                remaining -= len(block)
                continue
            if remaining <= 16:
                break
            parts.append(block[:remaining])
            remaining = 0
            break

        return "".join(parts)[:max_chars]


def _pick_better(
    current: RetrievalResult | None,
    candidate: RetrievalResult,
) -> RetrievalResult:
    if current is None:
        return candidate
    if candidate.score > current.score:
        return candidate
    return current


def _dedupe_queries(queries: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for query in queries:
        cleaned = str(query).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered
