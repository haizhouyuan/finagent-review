#!/usr/bin/env python3
"""Extraction Benchmark — 受控单步"炼丹"测试.

Measures the exact_quote pass rate of the full pipeline:
  DocumentParser → Extractor (via ChatgptREST) → exact_quote validation

Usage:
    # With ChatgptREST (real LLM):
    .venv/bin/python scripts/run_extraction_benchmark.py

    # With mock LLM (for testing the script itself):
    .venv/bin/python scripts/run_extraction_benchmark.py --mock

    # With custom fixture:
    .venv/bin/python scripts/run_extraction_benchmark.py --fixture path/to/report.md
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finagent.parsers.document_parser import DocumentParser
from finagent.parsers.text_cleaner import TextCleaner
from finagent.parsers.chunker import SemanticChunker
from finagent.agents.evidence_store import EvidenceStore
from finagent.agents.extractor import (
    _build_extraction_prompt,
    _parse_json_array,
    _validate_exact_quotes,
    _validate_triples,
)
from finagent.graph_v2.store import GraphStore
from finagent.graph_v2.ontology import NodeType

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_FIXTURE = Path(__file__).resolve().parent.parent / "tests/fixtures/commercial_aerospace_report.md"


# ── LLM Adapters ────────────────────────────────────────────────────


def chatgptrest_llm(system: str, user: str) -> str:
    """Call ChatgptREST via its HTTP API for LLM extraction."""
    import requests

    url = "http://127.0.0.1:18712"
    idempotency_key = f"bench-{int(time.time()*1000)}-{hash(user[:50])}"

    # Combine system + user into a single question for ChatgptREST
    question = f"[System Prompt]\n{system}\n\n[User Request]\n{user}"

    # Submit job
    try:
        resp = requests.post(
            f"{url}/api/v1/chatgpt/ask",
            json={
                "idempotency_key": idempotency_key,
                "question": question,
                "preset": "auto",
            },
            timeout=10,
        )
        resp.raise_for_status()
        job_id = resp.json().get("job_id")
    except Exception as exc:
        logger.error("ChatgptREST submit failed: %s", exc)
        return "[]"

    if not job_id:
        logger.error("No job_id returned")
        return "[]"

    # Wait for completion (poll with exponential backoff)
    for attempt in range(30):
        time.sleep(min(3 + attempt * 2, 15))
        try:
            resp = requests.get(f"{url}/api/v1/job/{job_id}", timeout=5)
            data = resp.json()
            status = data.get("status", "")
            if status == "completed":
                # Read answer
                resp2 = requests.get(
                    f"{url}/api/v1/answer/{job_id}",
                    params={"offset": 0, "max_chars": 16000},
                    timeout=5,
                )
                return resp2.json().get("content", "[]")
            elif status in ("failed", "cancelled"):
                logger.error("Job %s %s", job_id, status)
                return "[]"
        except Exception as exc:
            logger.warning("Poll attempt %d failed: %s", attempt, exc)

    logger.error("Job %s timed out", job_id)
    return "[]"


def mock_llm(system: str, user: str) -> str:
    """Mock LLM that returns semi-realistic extraction results.

    For testing the benchmark script itself without real LLM costs.
    Uses simple regex patterns to find entity relationships in text.
    """
    text = user.split("---")[1] if "---" in user else user

    triples = []
    # Pattern: look for "X是Y的Z" or "X为Y提供Z"
    patterns = [
        (r"([\u4e00-\u9fff]{2,8})(?:是|为)([\u4e00-\u9fff]{2,8}(?:航天|动力|宇航|科技|电器|超导))[^\u3002]*?的", "related_to"),
        (r"([\u4e00-\u9fff]{2,8}(?:航天|动力|宇航|科技|电器|超导))(?:的|为).{0,10}([\u4e00-\u9fff]{2,8}(?:航天|动力|宇航|科技|电器|超导))", "related_to"),
    ]

    # Extract entity pairs from the text directly
    entity_pattern = re.compile(r"(蓝箭航天|星河动力|中科宇航|航天电器|西部超导|铖昌科技|千帆星座|垣信卫星|银河航天|SpaceX)")
    entities = entity_pattern.findall(text)

    # Create some triples based on text proximity
    seen_pairs = set()
    sentences = re.split(r"[。！？]", text)
    for sent in sentences:
        ents = entity_pattern.findall(sent)
        if len(ents) >= 2:
            for i in range(len(ents)):
                for j in range(i+1, min(i+3, len(ents))):
                    pair = (ents[i], ents[j])
                    if pair not in seen_pairs and ents[i] != ents[j]:
                        seen_pairs.add(pair)
                        # Find a quote from the sentence
                        quote_start = sent.find(ents[i])
                        quote_end = sent.find(ents[j]) + len(ents[j])
                        if quote_start >= 0 and quote_end > quote_start:
                            exact_quote = sent[quote_start:min(quote_end+10, len(sent))].strip()
                        else:
                            exact_quote = sent.strip()[:80]

                        triples.append({
                            "head": ents[i],
                            "head_type": "company",
                            "relation": "related_to",
                            "tail": ents[j],
                            "tail_type": "company",
                            "exact_quote": exact_quote,
                            "confidence": 0.8,
                            "valid_from": "2024",
                        })

    return json.dumps(triples[:5], ensure_ascii=False)


# ── Benchmark Runner ────────────────────────────────────────────────


def run_benchmark(
    fixture_path: Path,
    *,
    use_mock: bool = False,
    max_chars_per_chunk: int = 3000,
    overlap_chars: int = 200,
) -> dict[str, Any]:
    """Run the extraction benchmark.

    Returns:
        Dict with metrics: total_chunks, total_extracted, total_validated,
        total_rejected, pass_rate, rejected_details.
    """
    print(f"\n{'='*60}")
    print(f"🧪 Extraction Benchmark")
    print(f"{'='*60}")
    print(f"📄 Fixture: {fixture_path}")
    print(f"🤖 LLM: {'Mock' if use_mock else 'ChatgptREST'}")
    print(f"📏 Chunk size: {max_chars_per_chunk} chars, overlap: {overlap_chars}")
    print(f"{'='*60}\n")

    # Setup
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        evidence_db = f.name
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        graph_db = f.name

    try:
        evidence_store = EvidenceStore(evidence_db)
        graph_store = GraphStore(graph_db)
        parser = DocumentParser(
            evidence_store=evidence_store,
            max_chars_per_chunk=max_chars_per_chunk,
            overlap_chars=overlap_chars,
        )

        # Step 1: Parse fixture
        print("📖 Step 1: Parsing fixture...")
        refs = parser.parse_file(fixture_path, query="商业航天产业链")
        print(f"   → {len(refs)} chunks created\n")

        if not refs:
            print("❌ No chunks created! Check the fixture file.")
            return {"error": "no chunks"}

        # Get candidate entities from graph (starts empty, grows per chunk)
        llm_fn = mock_llm if use_mock else chatgptrest_llm

        # Metrics
        total_extracted = 0
        total_validated = 0
        total_rejected = 0
        total_ingested = 0
        rejected_details: list[dict[str, Any]] = []
        all_validated_triples: list[dict[str, Any]] = []

        # Step 2: Process each chunk
        print("🔬 Step 2: Extracting triples per chunk...\n")

        for i, ref in enumerate(refs):
            # Fetch text
            eid = ref.get("evidence_id")
            if eid is not None:
                text = evidence_store.fetch(eid)
            else:
                text = ref.get("_text", "")

            if not text or len(text) < 50:
                continue

            heading = ref.get("heading", "")
            print(f"  Chunk {i+1}/{len(refs)} ({len(text)} chars)"
                  f"{f' [{heading[:30]}]' if heading else ''}")

            # Get candidate entities for front-loaded resolution
            candidate_entities = [
                (graph_store.get_node(n) or {}).get("label", n)
                for n in list(graph_store.g.nodes())[:100]
            ]

            # Build prompt and call LLM
            system_prompt = _build_extraction_prompt(candidate_entities or None)
            user_prompt = f"请从以下文本提取产业链三元组：\n\n---\n{text[:6000]}\n---\n\n只返回JSON数组。"

            try:
                raw_response = llm_fn(system_prompt, user_prompt)
                raw_triples = _parse_json_array(raw_response)
            except Exception as exc:
                print(f"    ⚠️  LLM call failed: {exc}")
                continue

            n_extracted = len(raw_triples)
            total_extracted += n_extracted

            # exact_quote validation
            validated = _validate_exact_quotes(raw_triples, text)
            n_validated = len(validated)
            total_validated += n_validated

            # Track rejections
            n_rejected = n_extracted - n_validated
            total_rejected += n_rejected

            if n_rejected > 0:
                rejected_quotes = [
                    t for t in raw_triples
                    if t not in validated
                ]
                for rq in rejected_quotes:
                    rejected_details.append({
                        "chunk": i + 1,
                        "head": rq.get("head", "?"),
                        "tail": rq.get("tail", "?"),
                        "relation": rq.get("relation", "?"),
                        "exact_quote": str(rq.get("exact_quote", ""))[:60],
                        "reason": "exact_quote not in source",
                    })

            # Structural validation
            valid_triples = _validate_triples(validated)

            # Ingest into graph
            for t in valid_triples:
                try:
                    head = t["head"]
                    tail = t["tail"]
                    if not graph_store.has_node(head):
                        ht_str = t.get("head_type", "entity")
                        try:
                            ht = NodeType(ht_str)
                        except ValueError:
                            ht = NodeType.ENTITY
                        graph_store.add_node(head, ht, head)
                    if not graph_store.has_node(tail):
                        tt_str = t.get("tail_type", "entity")
                        try:
                            tt = NodeType(tt_str)
                        except ValueError:
                            tt = NodeType.ENTITY
                        graph_store.add_node(tail, tt, tail)
                    total_ingested += 1
                    all_validated_triples.append(t)
                except Exception:
                    pass

            status = "✅" if n_rejected == 0 else "⚠️"
            print(f"    {status} extracted={n_extracted}, "
                  f"validated={n_validated}, rejected={n_rejected}")

        # Step 3: Report
        pass_rate = total_validated / max(total_extracted, 1)
        print(f"\n{'='*60}")
        print(f"📊 BENCHMARK RESULTS")
        print(f"{'='*60}")
        print(f"  Chunks processed:        {len(refs)}")
        print(f"  Total triples extracted:  {total_extracted}")
        print(f"  Total validated:          {total_validated}")
        print(f"  Total rejected:           {total_rejected}")
        print(f"  Total ingested to graph:  {total_ingested}")
        print(f"  Graph nodes:              {graph_store.g.number_of_nodes()}")
        print(f"  ────────────────────────")
        print(f"  📈 PASS RATE:            {pass_rate:.1%}")
        print(f"{'='*60}")

        if pass_rate >= 0.85:
            print(f"\n  🎉 PASS RATE ≥ 85% — Ready for Phase 4!")
        elif pass_rate >= 0.60:
            print(f"\n  ⚠️  PASS RATE 60-85% — Needs tuning")
        else:
            print(f"\n  ❌ PASS RATE < 60% — Prompt/Parser needs rework")

        # Print validated triples
        if all_validated_triples:
            print(f"\n📋 Validated Triples ({len(all_validated_triples)}):")
            for t in all_validated_triples[:20]:
                print(f"  {t['head']} --[{t['relation']}]--> {t['tail']}"
                      f"  (conf={t.get('confidence', '?')})")

        # Print rejection analysis
        if rejected_details:
            print(f"\n🔍 Rejection Analysis ({len(rejected_details)} cases):")
            for rd in rejected_details[:10]:
                print(f"  Chunk {rd['chunk']}: {rd['head']} → {rd['tail']}")
                print(f"    quote: \"{rd['exact_quote']}\"")
                print(f"    reason: {rd['reason']}")

        result = {
            "total_chunks": len(refs),
            "total_extracted": total_extracted,
            "total_validated": total_validated,
            "total_rejected": total_rejected,
            "total_ingested": total_ingested,
            "graph_nodes": graph_store.g.number_of_nodes(),
            "pass_rate": round(pass_rate, 4),
            "rejected_details": rejected_details,
        }

        # Save result to JSON
        result_path = Path(__file__).parent.parent / "tests" / "benchmark_result.json"
        with open(result_path, "w") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Results saved to: {result_path}")

        return result

    finally:
        evidence_store.close()
        graph_store.close()
        os.unlink(evidence_db)
        os.unlink(graph_db)


# ── CLI ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Extraction Benchmark")
    parser.add_argument(
        "--fixture", type=Path, default=DEFAULT_FIXTURE,
        help="Path to fixture file (default: commercial_aerospace_report.md)",
    )
    parser.add_argument(
        "--mock", action="store_true",
        help="Use mock LLM instead of ChatgptREST",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=3000,
        help="Max chars per chunk (default: 3000)",
    )
    parser.add_argument(
        "--overlap", type=int, default=200,
        help="Overlap chars between chunks (default: 200)",
    )

    args = parser.parse_args()

    if not args.fixture.exists():
        print(f"❌ Fixture not found: {args.fixture}")
        sys.exit(1)

    run_benchmark(
        args.fixture,
        use_mock=args.mock,
        max_chars_per_chunk=args.chunk_size,
        overlap_chars=args.overlap,
    )


if __name__ == "__main__":
    main()
