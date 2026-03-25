#!/usr/bin/env python3
"""Cross-KOL Signal Analysis — detect consensus, divergence, and actionable signals."""
import sqlite3, json, sys, os
from collections import defaultdict

ROOT = os.environ.get("FINAGENT_ROOT", "/vol1/1000/projects/finagent")
DB = os.path.join(ROOT, "state", "finagent.sqlite")

def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # Map artifact → source name
    art_sources = {}
    for r in conn.execute("SELECT artifact_id, title FROM artifacts"):
        art_sources[r["artifact_id"]] = r["title"]

    # Get all claims with their source artifacts
    claims_by_entity = defaultdict(list)
    for r in conn.execute("""
        SELECT c.claim_id, c.claim_text, c.claim_type, c.artifact_id, c.confidence
        FROM claims c
        WHERE length(c.claim_text) > 20
    """):
        text = r["claim_text"].lower()
        # Tag entities mentioned
        entity_tags = []
        for ent, keywords in [
            ("NVIDIA", ["nvidia", "nvda", "nvid", "黄仁勋", "jensen"]),
            ("TSMC", ["tsmc", "台积电", "2330"]),
            ("Tesla", ["tesla", "tsla", "特斯拉", "fsd"]),
            ("Apple", ["apple", "aapl", "苹果", "iphone"]),
            ("AI_CapEx", ["capex", "ai infrastructure", "ai基建", "算力", "数据中心"]),
            ("HBM", ["hbm", "hbm3", "hbm4"]),
            ("Copper", ["copper", "铜"]),
            ("Gold", ["gold", "黄金"]),
            ("China_Semi", ["smic", "中芯", "国产替代", "国产化"]),
        ]:
            if any(kw in text for kw in keywords):
                entity_tags.append(ent)

        for ent in entity_tags:
            claims_by_entity[ent].append({
                "text": r["claim_text"][:120],
                "type": r["claim_type"],
                "source": art_sources.get(r["artifact_id"], "unknown")[:40],
                "confidence": r["confidence"],
            })

    # Output analysis
    output = {"cross_kol_signals": [], "consensus_topics": [], "divergence_topics": []}

    for entity, claims in sorted(claims_by_entity.items(), key=lambda x: -len(x[1])):
        sources = set(c["source"] for c in claims)
        if len(sources) < 2:
            continue

        bullish = [c for c in claims if c["type"] in ("datapoint", "forward_looking", "quantitative")]
        bearish = [c for c in claims if c["type"] in ("bearish", "risk", "warning")]

        signal = {
            "entity": entity,
            "total_claims": len(claims),
            "sources_count": len(sources),
            "sources": list(sources),
            "bullish_count": len(bullish),
            "bearish_count": len(bearish),
        }

        if bullish and bearish:
            signal["divergence"] = True
            signal["sample_bullish"] = bullish[0]["text"]
            signal["sample_bearish"] = bearish[0]["text"] if bearish else ""
            output["divergence_topics"].append(signal)
        elif len(sources) >= 3:
            signal["divergence"] = False
            output["consensus_topics"].append(signal)

        output["cross_kol_signals"].append(signal)

    print(json.dumps(output, indent=2, ensure_ascii=False))
    return output

if __name__ == "__main__":
    main()
