#!/usr/bin/env python3
"""P2: Batch semi/AI claims extraction from codexread ASR transcripts.

Finds semi/AI episodes in codexread, extracts transcript text,
generates Gemini-ready prompts with embedded transcript, and can submit via ChatgptREST.
"""
import json, os, glob, re, sys
from pathlib import Path

CODEXREAD_VA = "/vol1/1000/projects/codexread/state/video-analyses"
FINAGENT_BASE = "/vol1/1000/projects/finagent"
PROMPTS_DIR = f"{FINAGENT_BASE}/research/claims/extraction_prompts_v2"

SEMI_KEYWORDS = [
    "半导体", "芯片", "晶圆", "台积电", "TSMC", "中芯", "SMIC",
    "光刻", "刻蚀", "沉积", "封装", "HBM", "DRAM", "NAND", "存储",
    "AI算力", "GPU", "英伟达", "NVIDIA", "AMD", "设备", "材料",
    "国产替代", "制裁", "先进制程", "2纳米", "3纳米", "CoWoS",
    "长鑫", "长存", "北方华创", "中微", "拓荆", "华海清科",
    "探针", "光刻胶", "EUV", "DUV", "ASML", "测试",
]

def load_transcript(bvid_dir: str, max_chars: int = 8000) -> str:
    """Load transcript text from codexread video-analysis dir."""
    json_path = os.path.join(bvid_dir, "transcript.json")
    if not os.path.exists(json_path):
        return ""
    
    with open(json_path) as f:
        segments = json.load(f)
    
    texts = [seg.get("text", "") for seg in segments if seg.get("text")]
    full_text = " ".join(texts)
    return full_text[:max_chars]

def count_keywords(text: str) -> int:
    """Count semi/AI keyword hits."""
    count = 0
    for kw in SEMI_KEYWORDS:
        count += text.lower().count(kw.lower())
    return count

def find_semi_episodes(min_keywords: int = 5) -> list:
    """Find all semi/AI episodes with transcripts in codexread."""
    episodes = []
    
    for d in sorted(glob.glob(f"{CODEXREAD_VA}/bili_*")):
        dirname = os.path.basename(d)
        # Extract BVID
        parts = dirname.split("_")
        if len(parts) < 4:
            continue
        bvid = parts[-1]
        date = parts[-2] if len(parts) >= 3 else ""
        
        transcript = load_transcript(d)
        if not transcript:
            continue
        
        kw_count = count_keywords(transcript)
        if kw_count >= min_keywords:
            episodes.append({
                "dir": d,
                "dirname": dirname,
                "bvid": bvid,
                "date": date,
                "kw_count": kw_count,
                "transcript_chars": len(transcript),
            })
    
    episodes.sort(key=lambda x: x["kw_count"], reverse=True)
    return episodes

def generate_prompt(episode: dict) -> str:
    """Generate extraction prompt with embedded transcript."""
    transcript = load_transcript(episode["dir"], max_chars=8000)
    
    prompt = f"""请从以下福总视频转录文本中提取半导体/AI投资Claims。

## 视频信息
- BVID: {episode['bvid']}
- 日期: {episode['date']}
- 关键词命中: {episode['kw_count']}次

## 输出格式
每个claim一行JSON，严格遵循以下schema：
{{"claim_id":"{{bvid}}_C001","ticker":"股票代码","company":"公司名","segment":"Semiconductor|AI_Compute|Memory|Equipment|Materials|Testing|Packaging","claim_type":"capex|capacity|market_share|unit_cost|localization|supply_chain|technology|order|policy|demand","claim_text":"具体声明","numbers":[{{"name":"","value":0,"unit":"","context":""}}],"time_ref":"","source":{{"episode_id":"","bvid":"{episode['bvid']}","episode_date":"{episode['date']}"}},"confidence":"high|medium|low","verification_status":"unverified"}}

## 提取要求
1. 只提取有具体数字或可核验事实的声明
2. 每个claim必须包含segment和claim_type
3. numbers数组中包含所有提到的数字
4. confidence: high=有明确数据源, medium=行业共识, low=个人推测
5. 至少提取5个claims

## 转录文本（前8000字符）
{transcript}
"""
    return prompt

def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "list"
    
    if cmd == "list":
        episodes = find_semi_episodes(min_keywords=5)
        print(f"Found {len(episodes)} semi/AI episodes with transcripts\n")
        for i, ep in enumerate(episodes):
            print(f"  {i+1:2d}. {ep['bvid']} ({ep['date']}) kw={ep['kw_count']:3d} chars={ep['transcript_chars']:5d}")
    
    elif cmd == "prompts":
        episodes = find_semi_episodes(min_keywords=5)
        os.makedirs(PROMPTS_DIR, exist_ok=True)
        for ep in episodes:
            prompt = generate_prompt(ep)
            out = f"{PROMPTS_DIR}/prompt_{ep['bvid']}.txt"
            with open(out, "w") as f:
                f.write(prompt)
        print(f"Generated {len(episodes)} prompts in {PROMPTS_DIR}")
    
    elif cmd == "show":
        # Show prompt for a specific BVID
        bvid = sys.argv[2] if len(sys.argv) > 2 else None
        if not bvid:
            print("Usage: batch_extract_v2.py show <BVID>")
            return
        episodes = find_semi_episodes(min_keywords=0)
        for ep in episodes:
            if ep["bvid"] == bvid:
                print(generate_prompt(ep))
                return
        print(f"BVID {bvid} not found")
    
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: batch_extract_v2.py [list|prompts|show <BVID>]")

if __name__ == "__main__":
    main()
