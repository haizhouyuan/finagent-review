#!/usr/bin/env python3
"""P1: Normalize + Deduplicate Claims

Reads all claims JSONL files, normalizes schema, deduplicates by claim_id,
and outputs a clean master JSONL file.
"""
import json, glob, os, hashlib, sys

SCHEMA_FIELDS = {
    "claim_id": "",
    "ticker": "",
    "company": "",
    "segment": "unknown",
    "claim_type": "unknown",
    "claim_text": "",
    "numbers": [],
    "time_ref": None,
    "source": {},
    "confidence": "medium",
    "verification_status": "unverified",
}

SEGMENT_MAP = {
    "Semiconductor": "Semiconductor",
    "AI_Compute": "AI_Compute",
    "Memory": "Memory",
    "Equipment": "Equipment",
    "Materials": "Materials",
    "Testing": "Testing",
    "Packaging": "Packaging",
}

def normalize_claim(raw: dict, source_file: str) -> dict:
    """Normalize a claim to canonical schema."""
    claim = {}
    for field, default in SCHEMA_FIELDS.items():
        claim[field] = raw.get(field, default)
    
    # Fix missing segment from claim_text keywords
    if claim["segment"] in ("unknown", ""):
        text = claim.get("claim_text", "") + " " + claim.get("company", "")
        if any(k in text for k in ["HBM", "DRAM", "NAND", "存储", "内存", "Memory"]):
            claim["segment"] = "Memory"
        elif any(k in text for k in ["刻蚀", "沉积", "设备", "Equipment", "光刻机"]):
            claim["segment"] = "Equipment"
        elif any(k in text for k in ["封装", "Packaging", "CoWoS", "先进封装"]):
            claim["segment"] = "Packaging"
        elif any(k in text for k in ["光刻胶", "材料", "靶材", "Materials"]):
            claim["segment"] = "Materials"
        elif any(k in text for k in ["测试", "Testing", "探针"]):
            claim["segment"] = "Testing"
        elif any(k in text for k in ["AI", "GPU", "算力", "Compute", "芯片"]):
            claim["segment"] = "AI_Compute"
        elif any(k in text for k in ["晶圆", "制程", "纳米", "nm"]):
            claim["segment"] = "Semiconductor"
    
    # Fix missing claim_type from keywords
    if claim["claim_type"] in ("unknown", ""):
        text = claim.get("claim_text", "")
        if any(k in text for k in ["资本开支", "capex", "投资", "投入"]):
            claim["claim_type"] = "capex"
        elif any(k in text for k in ["产能", "capacity", "万片", "片/月"]):
            claim["claim_type"] = "capacity"
        elif any(k in text for k in ["订单", "order", "合同"]):
            claim["claim_type"] = "order"
        elif any(k in text for k in ["国产化", "localization", "替代"]):
            claim["claim_type"] = "localization"
        elif any(k in text for k in ["制裁", "管制", "禁令", "policy"]):
            claim["claim_type"] = "policy"
        elif any(k in text for k in ["市占率", "份额", "share"]):
            claim["claim_type"] = "market_share"
        elif any(k in text for k in ["技术", "制程", "工艺", "technology"]):
            claim["claim_type"] = "technology"
        elif any(k in text for k in ["涨价", "价格", "cost"]):
            claim["claim_type"] = "unit_cost"
    
    # Fix missing confidence
    if claim["confidence"] in ("unknown", ""):
        claim["confidence"] = "medium"
    
    # Ensure source has episode info
    if not claim["source"]:
        claim["source"] = {"source_file": source_file}
    
    # Generate claim_id if missing
    if not claim["claim_id"]:
        text_hash = hashlib.md5(claim["claim_text"].encode()).hexdigest()[:8]
        claim["claim_id"] = f"auto_{text_hash}"
    
    return claim

def dedup_key(claim: dict) -> str:
    """Generate dedup key from claim text (fuzzy)."""
    text = claim.get("claim_text", "").strip()
    # Normalize whitespace and common variations
    text = " ".join(text.split())
    return text[:100]  # First 100 chars for fuzzy match

def main():
    input_dir = "research/claims/extracted_raw"
    output_file = "research/claims/claims_master.jsonl"
    
    all_claims = []
    seen_keys = set()
    dupes = 0
    normalized = 0
    
    for fp in sorted(glob.glob(f"{input_dir}/*.jsonl")):
        with open(fp) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except:
                    continue
                
                claim = normalize_claim(raw, os.path.basename(fp))
                key = dedup_key(claim)
                
                if key in seen_keys:
                    dupes += 1
                    continue
                
                seen_keys.add(key)
                all_claims.append(claim)
                normalized += 1
    
    # Write master JSONL
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        for c in all_claims:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")
    
    # Stats
    from collections import Counter
    segments = Counter(c["segment"] for c in all_claims)
    types = Counter(c["claim_type"] for c in all_claims)
    conf = Counter(c["confidence"] for c in all_claims)
    
    print(f"Normalized: {normalized}, Duplicates removed: {dupes}")
    print(f"Output: {output_file}")
    print(f"\nSegments: {dict(segments.most_common())}")
    print(f"Types: {dict(types.most_common())}")
    print(f"Confidence: {dict(conf.most_common())}")

if __name__ == "__main__":
    main()
