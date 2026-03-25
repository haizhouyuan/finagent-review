#!/usr/bin/env python3
"""Claims→Thesis Aggregation: Group claims into investment theses.

Reads claims_master.jsonl, groups by segment/ticker/theme, and generates
thesis-level investment insights with cross-references.
"""
import json, os
from collections import defaultdict, Counter

def load_claims(path="research/claims/claims_master.jsonl"):
    claims = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                claims.append(json.loads(line))
            except:
                continue
    return claims

def extract_themes(claims):
    """Group claims into investment themes."""
    themes = defaultdict(list)
    
    for c in claims:
        text = c.get("claim_text", "")
        segment = c.get("segment", "unknown")
        ticker = c.get("ticker", "")
        company = c.get("company", "")
        
        # Theme assignment based on content
        if any(k in text for k in ["国产化率", "国产替代", "localization"]):
            themes["国产替代加速"].append(c)
        if any(k in text for k in ["HBM", "高带宽内存"]):
            themes["HBM供需紧张"].append(c)
        if any(k in text for k in ["长鑫", "CXMT", "长存", "YMTC"]):
            themes["中国存储崛起"].append(c)
        if any(k in text for k in ["台积电", "TSMC", "2纳米", "3纳米", "先进制程"]):
            themes["先进制程竞赛"].append(c)
        if any(k in text for k in ["CoWoS", "先进封装", "封装"]):
            themes["封装产能瓶颈"].append(c)
        if any(k in text for k in ["光刻胶", "材料", "靶材"]):
            themes["材料国产化"].append(c)
        if any(k in text for k in ["大基金", "制裁", "管制"]):
            themes["政策驱动"].append(c)
        if any(k in text for k in ["设备", "北方华创", "中微", "拓荆"]):
            themes["设备国产化"].append(c)
        if any(k in text for k in ["订单", "产能", "扩产"]):
            themes["扩产周期"].append(c)
        if any(k in text for k in ["DDR5", "DRAM", "涨价"]):
            themes["存储涨价周期"].append(c)
    
    return themes

def generate_thesis(theme_name, claims):
    """Generate a thesis from a group of claims."""
    # Extract key numbers
    numbers = []
    tickers = set()
    companies = set()
    sources = set()
    
    for c in claims:
        if c.get("ticker"):
            tickers.add(c["ticker"])
        if c.get("company"):
            companies.add(c["company"])
        nums = c.get("numbers", [])
        if isinstance(nums, list):
            for n in nums:
                if isinstance(n, dict):
                    numbers.append(f"{n.get('name','')}: {n.get('value','')}{n.get('unit','')}")
                elif isinstance(n, (int, float)):
                    numbers.append(str(n))
        src = c.get("source", {})
        if isinstance(src, dict) and src.get("bvid"):
            sources.add(src["bvid"])
    
    confidence_dist = Counter(c.get("confidence", "unknown") for c in claims)
    
    thesis = {
        "theme": theme_name,
        "claim_count": len(claims),
        "tickers": sorted(tickers),
        "companies": sorted(companies),
        "confidence": confidence_dist.most_common(),
        "key_numbers": numbers[:10],
        "source_count": len(sources),
        "representative_claims": [c["claim_text"][:100] for c in claims[:5]],
    }
    return thesis

def main():
    claims = load_claims()
    themes = extract_themes(claims)
    
    print(f"Total claims: {len(claims)}")
    print(f"Themes identified: {len(themes)}")
    print()
    
    # Sort by claim count
    sorted_themes = sorted(themes.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Generate thesis report
    report = []
    report.append("# Semi/AI Investment Theses — Auto-Generated from KOL Claims")
    report.append(f"\n日期：2026-03-11")
    report.append(f"数据源：{len(claims)} normalized claims from 福总+CSI+SemiAnalysis")
    report.append("\n---\n")
    
    for theme_name, theme_claims in sorted_themes:
        thesis = generate_thesis(theme_name, theme_claims)
        
        report.append(f"## {theme_name} ({thesis['claim_count']} claims)")
        report.append("")
        
        if thesis["tickers"]:
            report.append(f"**标的**: {', '.join(thesis['tickers'])}")
        if thesis["companies"]:
            report.append(f"**公司**: {', '.join(thesis['companies'])}")
        report.append(f"**置信度**: {dict(thesis['confidence'])}")
        report.append(f"**数据源**: {thesis['source_count']} episodes")
        report.append("")
        
        if thesis["key_numbers"]:
            report.append("**关键数据**:")
            for n in thesis["key_numbers"][:5]:
                report.append(f"- {n}")
        report.append("")
        
        report.append("**代表性Claims**:")
        for ct in thesis["representative_claims"]:
            report.append(f"- {ct}...")
        report.append("")
        report.append("---\n")
    
    # Write report
    out = "docs/2026-03-11_auto_thesis_report.md"
    with open(out, "w") as f:
        f.write("\n".join(report))
    
    print(f"Thesis report: {out}")
    for theme_name, theme_claims in sorted_themes:
        print(f"  {theme_name}: {len(theme_claims)} claims")

if __name__ == "__main__":
    main()
