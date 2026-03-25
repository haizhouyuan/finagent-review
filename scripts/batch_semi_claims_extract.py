#!/usr/bin/env python3
"""
Batch claims extraction from 福总 semi/AI episodes.
Reads evidence_compact.md files and builds prompts for LLM structured extraction.

Usage:
    python3 batch_semi_claims_extract.py --list         # List eligible episodes
    python3 batch_semi_claims_extract.py --extract N    # Extract from top N episodes
    python3 batch_semi_claims_extract.py --merge        # Merge all extracted claims into JSONL
"""

import os
import re
import json
import hashlib
import argparse
from pathlib import Path

CODEXREAD_BASE = "/vol1/1000/projects/codexread"
EPISODES_DIR = f"{CODEXREAD_BASE}/archives/topics/bili_up_3546976515786791/review_pack/episodes"
CLAIMS_OUTPUT = "/vol1/1000/projects/finagent/research/claims"

SEMI_KEYWORDS = [
    '半导体', '芯片', '晶圆', '封装', 'EDA', '光刻', '存储', 'HBM', 'DRAM', 'NAND',
    'SMIC', '中芯', '台积电', 'TSMC', '英伟达', 'NVIDIA', '华为', 'AMD', 'Intel',
    '三星', 'SK海力士', 'SK Hynix', '长存', '兆易', '韦尔', '北方华创', '中微',
    '寒武纪', '海光', 'GPU', 'AI算力', '算力', '液冷', '光模块', 'CPO', '英飞凌', '恩智浦',
    '拓荆', '盛美', '北方华创', '中微', '华海清科', '碳化硅', '光刻胶', '抛光垫',
    '探针', '分选机', '先进封装', 'CoWoS'
]

EXTENDED_SEGMENTS = [
    "Semiconductor", "AI_Compute", "Memory", "Optics", "Cooling",
    "EDA", "Equipment", "Materials", "Testing", "Packaging",
    "SiC_GaN", "Photolithography", "Other_Tech"
]


def find_semi_episodes(min_keywords=3):
    """Find episodes with significant semiconductor/AI content."""
    results = []
    if not os.path.isdir(EPISODES_DIR):
        print(f"Episodes dir not found: {EPISODES_DIR}")
        return results

    for ep in sorted(os.listdir(EPISODES_DIR)):
        ec_path = os.path.join(EPISODES_DIR, ep, "evidence_compact.md")
        if not os.path.isfile(ec_path):
            continue
        text = open(ec_path, encoding='utf-8', errors='ignore').read()
        matched = [kw for kw in SEMI_KEYWORDS if kw in text]
        if len(matched) >= min_keywords:
            # Extract BV ID and date from ep name
            parts = ep.split('_')
            date = parts[2] if len(parts) > 2 else "unknown"
            bvid = parts[3] if len(parts) > 3 else "unknown"
            results.append({
                'episode_id': ep,
                'date': date,
                'bvid': bvid,
                'keyword_count': len(matched),
                'keywords': matched[:10],
                'text_length': len(text),
                'path': ec_path,
            })

    results.sort(key=lambda x: x['keyword_count'], reverse=True)
    return results


def build_claims_prompt(episode_data: dict, text: str) -> str:
    """Build a structured claims extraction prompt for an episode."""
    # Truncate to transcript highlights + first OCR section for manageable prompt
    sections = text.split("## OCR Numeric Hits")
    transcript = sections[0][:8000]  # Transcript highlights
    ocr = sections[1][:4000] if len(sections) > 1 else ""

    return f"""请从以下福总视频的ASR转写+OCR数据中，提取半导体/AI相关的结构化投资Claims。

**Schema**（每个claim一个JSON对象）：
{{
  "claim_id": "自动生成hash",
  "ticker": "股票代码或空",
  "company": "公司名称",
  "segment": "{' | '.join(EXTENDED_SEGMENTS)}",
  "claim_type": "capex|capacity|market_share|unit_cost|localization|supply_chain|technology|order|policy|demand|competitor|other",
  "claim_text": "具体可核验的声明（中文）",
  "numbers": [{{"name": "指标名", "value": 数值, "unit": "单位", "context": "上下文"}}],
  "time_ref": "YYYY-MM 或 YYYY-Qn 或 null",
  "source": {{"episode_id": "{episode_data['episode_id']}", "bvid": "{episode_data['bvid']}", "episode_date": "{episode_data['date']}"}},
  "confidence": "high|medium|low",
  "verification_status": "unverified",
  "verification_questions": ["核验问题"],
  "suggested_sources": ["建议核验入口"]
}}

**只提取**半导体、AI算力、存储、封装、设备、材料相关的claims。
**忽略**宏观政策、股市评论、商业航天内容。
每个claim独占一行JSON。至少提取10个claims。

---
**Transcript Highlights:**
{transcript}

**OCR Numeric Hits (Selected):**
{ocr}
"""


def generate_claim_id(claim_text: str, episode_id: str, ticker: str = "") -> str:
    """Generate stable claim ID."""
    raw = f"{episode_id}_{claim_text}_{ticker}"
    return "c_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def list_episodes(args):
    """List all eligible semi/AI episodes."""
    episodes = find_semi_episodes(min_keywords=args.min_kw)
    print(f"Found {len(episodes)} episodes with {args.min_kw}+ semi/AI keywords:\n")
    for i, ep in enumerate(episodes[:50]):
        print(f"{i+1:3d}. {ep['episode_id']} | kw={ep['keyword_count']:2d} | {ep['text_length']:6d} chars | {ep['keywords'][:5]}")

    print(f"\nTotal: {len(episodes)} episodes")
    print(f"Total text: {sum(e['text_length'] for e in episodes):,} chars")


def extract_claims(args):
    """Generate extraction prompts for top N episodes."""
    episodes = find_semi_episodes(min_keywords=3)
    top_n = episodes[:args.extract]

    os.makedirs(CLAIMS_OUTPUT, exist_ok=True)
    prompts_dir = os.path.join(CLAIMS_OUTPUT, "extraction_prompts")
    os.makedirs(prompts_dir, exist_ok=True)

    for ep in top_n:
        text = open(ep['path'], encoding='utf-8', errors='ignore').read()
        prompt = build_claims_prompt(ep, text)
        out_path = os.path.join(prompts_dir, f"prompt_{ep['episode_id']}.txt")
        with open(out_path, 'w') as f:
            f.write(prompt)
        print(f"Written: {out_path} ({len(prompt):,} chars)")

    print(f"\n{len(top_n)} prompts generated in {prompts_dir}")
    print("Submit each prompt to ChatGPT Pro or Gemini Deep Think for extraction.")


def merge_claims(args):
    """Merge all extracted claim files into a single JSONL."""
    claims_dir = os.path.join(CLAIMS_OUTPUT, "extracted_raw")
    if not os.path.isdir(claims_dir):
        print(f"No extracted_raw directory found at {claims_dir}")
        return

    all_claims = []
    errors = []
    for f in sorted(os.listdir(claims_dir)):
        if not f.endswith('.json') and not f.endswith('.jsonl'):
            continue
        filepath = os.path.join(claims_dir, f)
        with open(filepath, 'r') as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                try:
                    claim = json.loads(line)
                    if 'claim_id' not in claim:
                        claim['claim_id'] = generate_claim_id(
                            claim.get('claim_text', ''),
                            claim.get('source', {}).get('episode_id', '')
                        )
                    all_claims.append(claim)
                except json.JSONDecodeError as e:
                    errors.append(f"{f}:{line_no}: {e}")

    out_path = os.path.join(CLAIMS_OUTPUT, "claims_semi_v1.jsonl")
    with open(out_path, 'w') as f:
        for claim in all_claims:
            f.write(json.dumps(claim, ensure_ascii=False) + '\n')

    print(f"Merged {len(all_claims)} claims → {out_path}")
    if errors:
        print(f"Parse errors: {len(errors)}")
        for err in errors[:10]:
            print(f"  {err}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Batch semi/AI claims extraction")
    parser.add_argument('--list', action='store_true', help='List eligible episodes')
    parser.add_argument('--extract', type=int, help='Generate extraction prompts for top N episodes')
    parser.add_argument('--merge', action='store_true', help='Merge extracted claims into JSONL')
    parser.add_argument('--min-kw', type=int, default=3, help='Minimum keyword matches (default: 3)')

    args = parser.parse_args()

    if args.list:
        list_episodes(args)
    elif args.extract:
        extract_claims(args)
    elif args.merge:
        merge_claims(args)
    else:
        parser.print_help()
