#!/usr/bin/env python3
"""Batch ingest 福总 transcripts into finagent DB via intake-kol-digest + extract-claims.

Usage:
    python3 scripts/batch_ingest_fuzong.py --list          # List eligible transcripts
    python3 scripts/batch_ingest_fuzong.py --ingest N      # Ingest top N episodes
    python3 scripts/batch_ingest_fuzong.py --extract-all   # Extract claims from all unprocessed artifacts
"""

import os
import sys
import json
import subprocess
import argparse
from pathlib import Path

CODEXREAD_VA = "/vol1/1000/projects/codexread/state/video-analyses"
FINAGENT_ROOT = "/vol1/1000/projects/finagent"
TRANSCRIPT_STAGING = f"{FINAGENT_ROOT}/research/transcripts_staging"

SEMI_KEYWORDS = [
    "半导体", "芯片", "晶圆", "台积电", "TSMC", "中芯", "SMIC",
    "光刻", "刻蚀", "沉积", "封装", "HBM", "DRAM", "NAND", "存储",
    "AI算力", "GPU", "英伟达", "NVIDIA", "AMD", "设备", "材料",
    "国产替代", "制裁", "先进制程", "2纳米", "3纳米", "CoWoS",
    "长鑫", "长存", "北方华创", "中微", "拓荆", "华海清科",
    "探针", "光刻胶", "EUV", "DUV", "ASML", "测试",
]


def load_transcript(bvid_dir: str) -> str:
    json_path = os.path.join(bvid_dir, "transcript.json")
    if not os.path.exists(json_path):
        return ""
    with open(json_path) as f:
        segments = json.load(f)
    texts = [seg.get("text", "") for seg in segments if seg.get("text")]
    return " ".join(texts)


def find_episodes(min_kw=5):
    import glob
    episodes = []
    for d in sorted(glob.glob(f"{CODEXREAD_VA}/bili_*")):
        dirname = os.path.basename(d)
        parts = dirname.split("_")
        if len(parts) < 4:
            continue
        bvid = parts[-1]
        date = parts[-2] if len(parts) >= 3 else ""
        transcript = load_transcript(d)
        if not transcript:
            continue
        kw_count = sum(transcript.lower().count(kw.lower()) for kw in SEMI_KEYWORDS)
        if kw_count >= min_kw:
            episodes.append({
                "dir": d, "dirname": dirname, "bvid": bvid,
                "date": date, "kw_count": kw_count, "chars": len(transcript),
            })
    episodes.sort(key=lambda x: x["kw_count"], reverse=True)
    return episodes


def stage_transcript(ep: dict) -> str:
    """Write transcript to staging dir for intake."""
    os.makedirs(TRANSCRIPT_STAGING, exist_ok=True)
    transcript = load_transcript(ep["dir"])
    out_path = os.path.join(TRANSCRIPT_STAGING, f"fuzong_{ep['date']}_{ep['bvid']}.txt")
    with open(out_path, 'w') as f:
        f.write(f"# 福总 半导体/AI 视频转录\n")
        f.write(f"# BVID: {ep['bvid']}\n")
        f.write(f"# 日期: {ep['date']}\n")
        f.write(f"# 关键词命中: {ep['kw_count']}次\n\n")
        f.write(transcript)
    return out_path


def ingest_episode(ep: dict) -> dict:
    """Ingest a single episode into finagent DB."""
    staged_path = stage_transcript(ep)
    bvid = ep["bvid"]
    date = ep["date"]
    
    # Format date for published_at
    pub_date = f"{date[:4]}-{date[4:6]}-{date[6:]}" if len(date) == 8 else ""
    
    cmd = [
        "python3", "-m", "finagent.cli", "--root", ".",
        "intake-kol-digest",
        "--path", staged_path,
        "--artifact-id", f"fuzong_{bvid}",
        "--title", f"福总 半导体/AI {date} ({bvid})",
        "--artifact-kind", "video_transcript",
        "--source-id", "bili_3546976515786791",
        "--source-type", "kol",
        "--source-name", "福总_半导体AI",
        "--primaryness", "second_hand",
        "--jurisdiction", "CN",
        "--language", "zh",
        "--uri", f"https://www.bilibili.com/video/{bvid}",
        "--base-uri", "https://www.bilibili.com",
        "--speaker", "福总",
        "--min-chars", "50",
    ]
    if pub_date:
        cmd.extend(["--published-at", pub_date])
    
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=FINAGENT_ROOT)
    if result.returncode != 0:
        return {"ok": False, "bvid": bvid, "error": result.stderr.strip()[-200:]}
    
    try:
        out = json.loads(result.stdout.strip())
        return {"ok": True, "bvid": bvid, **out}
    except json.JSONDecodeError:
        return {"ok": True, "bvid": bvid, "raw": result.stdout.strip()[:200]}


def extract_claims_for_artifact(artifact_id: str) -> dict:
    """Run extract-claims on an artifact."""
    cmd = [
        "python3", "-m", "finagent.cli", "--root", FINAGENT_ROOT,
        "extract-claims",
        "--artifact-id", artifact_id,
        "--speaker", "福总",
        "--min-chars", "100",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=FINAGENT_ROOT)
    if result.returncode != 0:
        return {"ok": False, "artifact_id": artifact_id, "error": result.stderr.strip()[-200:]}
    try:
        out = json.loads(result.stdout.strip())
        return {"ok": True, "artifact_id": artifact_id, **out}
    except json.JSONDecodeError:
        return {"ok": True, "artifact_id": artifact_id, "raw": result.stdout.strip()[:200]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--ingest", type=int, help="Ingest top N episodes")
    parser.add_argument("--extract-all", action="store_true")
    parser.add_argument("--min-kw", type=int, default=5)
    args = parser.parse_args()

    if args.list:
        episodes = find_episodes(args.min_kw)
        print(f"Found {len(episodes)} episodes with {args.min_kw}+ keyword hits\n")
        for i, ep in enumerate(episodes[:50]):
            print(f"  {i+1:2d}. {ep['bvid']} ({ep['date']}) kw={ep['kw_count']:3d} chars={ep['chars']:5d}")
    
    elif args.ingest:
        episodes = find_episodes(args.min_kw)
        top_n = episodes[:args.ingest]
        print(f"Ingesting top {len(top_n)} episodes...\n")
        
        results = {"ok": 0, "fail": 0, "errors": []}
        for i, ep in enumerate(top_n):
            print(f"  [{i+1}/{len(top_n)}] {ep['bvid']} ({ep['date']}) kw={ep['kw_count']}...", end=" ", flush=True)
            r = ingest_episode(ep)
            if r.get("ok"):
                results["ok"] += 1
                print("✓")
            else:
                results["fail"] += 1
                results["errors"].append(r)
                print(f"✗ {r.get('error','')[:80]}")
        
        print(f"\nDone: {results['ok']} ok, {results['fail']} failed")
        if results["errors"]:
            print(f"Errors: {json.dumps(results['errors'][:5], indent=2, ensure_ascii=False)}")
    
    elif args.extract_all:
        # Find all fuzong artifacts that haven't been extracted
        import sqlite3
        db_path = os.path.join(FINAGENT_ROOT, "state", "finagent.db")
        if not os.path.exists(db_path):
            db_path = os.path.join(FINAGENT_ROOT, "finagent.db")
        
        # Just run extract on artifacts matching fuzong_*
        print("Looking for fuzong artifacts to extract claims from...")
        # This is a simplified approach — just list what we ingested
        for f in sorted(os.listdir(TRANSCRIPT_STAGING)):
            if f.startswith("fuzong_"):
                bvid = f.replace("fuzong_", "").replace(".txt", "").split("_")[-1]
                aid = f"fuzong_{bvid}"
                print(f"  Extracting from {aid}...", end=" ", flush=True)
                r = extract_claims_for_artifact(aid)
                if r.get("ok"):
                    print(f"✓ {r}")
                else:
                    print(f"✗ {r.get('error','')[:80]}")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
