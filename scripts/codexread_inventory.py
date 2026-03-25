#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class FileRow:
    path: str
    bytes: int
    ext: str
    top: str
    top2: str


def run(cmd: list[str], *, cwd: Path) -> str:
    return subprocess.check_output(cmd, cwd=str(cwd)).decode("utf-8", errors="replace")


def du_top(repo: Path) -> list[dict]:
    # `du -sh repo/*` is stable and fast enough for this use.
    proc = subprocess.run(
        ["bash", "-lc", "du -sh * 2>/dev/null | sort -hr"],
        cwd=str(repo),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    rows = []
    for line in proc.stdout.splitlines():
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        size, name = parts
        rows.append({"name": name.strip(), "size": size.strip()})
    return rows


def count_all_files(repo: Path) -> int:
    # Avoid .git traversal.
    proc = subprocess.run(
        ["bash", "-lc", "find . -path './.git' -prune -o -type f -print | wc -l"],
        cwd=str(repo),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        text=True,
    )
    return int(proc.stdout.strip())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--repo",
        default="/vol1/1000/projects/codexread",
        help="Path to the codexread repo",
    )
    ap.add_argument(
        "--out-dir",
        default="/vol1/1000/projects/finagent/docs/codexread",
        help="Directory to write inventory outputs into",
    )
    ap.add_argument(
        "--date",
        default=None,
        help="YYYY-MM-DD (defaults to today)",
    )
    args = ap.parse_args()

    repo = Path(args.repo).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not (repo / ".git").exists():
        raise SystemExit(f"Not a git repo: {repo}")

    day = args.date or dt.date.today().isoformat()
    ts = dt.datetime.now(dt.timezone.utc).isoformat()

    tracked_raw = subprocess.check_output(["git", "ls-files", "-z"], cwd=str(repo))
    tracked = [
        p.decode("utf-8", errors="replace")
        for p in tracked_raw.split(b"\x00")
        if p
    ]

    git_ls_files_human = subprocess.check_output(["git", "ls-files"], cwd=str(repo))
    git_ls_files_quoted_output_lines = sum(
        1 for line in git_ls_files_human.splitlines() if line.startswith(b"\"")
    )

    rows: list[FileRow] = []
    missing_on_disk: list[str] = []
    for p in tracked:
        fp = repo / p
        try:
            st = fp.stat()
        except FileNotFoundError:
            # Keep going; repo might have sparse checkout / odd state.
            missing_on_disk.append(p)
            continue
        ext = fp.suffix.lower().lstrip(".")
        parts = p.split("/")
        top = parts[0] if parts else ""
        top2 = "/".join(parts[:2]) if len(parts) >= 2 else top
        rows.append(FileRow(path=p, bytes=st.st_size, ext=ext, top=top, top2=top2))

    total_files = count_all_files(repo)

    # Aggregate stats
    def counter(values: list[str]) -> list[dict]:
        m: dict[str, int] = {}
        for v in values:
            m[v] = m.get(v, 0) + 1
        return [{"key": k, "count": v} for k, v in sorted(m.items(), key=lambda kv: (-kv[1], kv[0]))]

    ext_counts = counter([r.ext or "(none)" for r in rows])
    top_counts = counter([r.top or "(root)" for r in rows])
    top2_counts = counter([r.top2 or "(root)" for r in rows])

    largest = sorted(rows, key=lambda r: r.bytes, reverse=True)[:60]
    non_ascii_paths = [p for p in tracked if any(ord(ch) > 127 for ch in p)]
    whitespace_paths = [p for p in tracked if any(ch.isspace() for ch in p)]

    # Curated pointers for investing/finagent usage.
    entry_files = [
        "AGENTS.md",
        "README.md",
        "spec.md",
        "investing-monitoring-spec.md",
        "signals-alerts-spec.md",
        "decision-package-spec.md",
        "control-plane-spec.md",
        "workflow-graphs-spec.md",
        "entity-resolution-spec.md",
        "source-registry-spec.md",
        "topic-archive-spec.md",
        "tasks-mcp-spec.md",
        "docs/investing/README.md",
        "archives/investing/README.md",
        "archives/investing/convergence.md",
        "archives/investing/watchlist.md",
        "archives/investing/universe.json",
        "archives/investing/source_registry.yaml",
        "archives/investing/security_master.json",
    ]
    entry_files_present = [p for p in entry_files if (repo / p).exists()]

    recommended_allow = [
        "docs/",
        "archives/investing/",
        "archives/topics/",
        "templates/",
        "contracts/",
        "scripts/",
        "spec.md",
        "investing-monitoring-spec.md",
        "signals-alerts-spec.md",
        "decision-package-spec.md",
        "workflow-graphs-spec.md",
        "control-plane-spec.md",
    ]
    recommended_deny = [
        ".git/",
        ".venv/",
        "__pycache__/",
        "state/",
        "imports/",
        "artifacts/",
        "logs/",
        "exports/",
        ".specstory/",
    ]

    payload = {
        "schema": 2,
        "generatedAtUtc": ts,
        "date": day,
        "repo": str(repo),
        "counts": {
            "trackedFilesFromGit": len(tracked),
            "trackedFilesOnDisk": len(rows),
            "trackedFilesMissingOnDisk": len(missing_on_disk),
            "allFiles": total_files,
        },
        "duTopLevel": du_top(repo),
        "tracked": {
            "extCounts": ext_counts,
            "topCounts": top_counts,
            "top2Counts": top2_counts,
            "nonAsciiPathsCount": len(non_ascii_paths),
            "whitespacePathsCount": len(whitespace_paths),
            "gitLsFilesQuotedOutputLines": git_ls_files_quoted_output_lines,
        },
        "largestTracked": [
            {"path": r.path, "bytes": r.bytes, "mb": round(r.bytes / 1024 / 1024, 2)} for r in largest
        ],
        "nonAsciiPathsSample": non_ascii_paths[:80],
        "whitespacePathsSample": whitespace_paths[:80],
        "missingOnDiskSample": missing_on_disk[:80],
        "entryFilesPresent": entry_files_present,
        "indexingGuidance": {
            "recommendedAllow": recommended_allow,
            "recommendedDeny": recommended_deny,
        },
        "notes": [
            "Disk contains large non-tracked data under state/ and imports/; avoid indexing those for long-term understanding.",
            "Some tracked paths contain non-ASCII characters; tools like `git ls-files` may display those paths quoted depending on settings.",
        ],
    }

    json_path = out_dir / f"codexread_inventory_{day}.json"
    tsv_path = out_dir / f"codexread_tracked_files_{day}.tsv"
    md_path = out_dir / f"codexread_inventory_{day}.md"

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    with tsv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["path", "bytes", "mb", "ext", "top", "top2"])
        for r in sorted(rows, key=lambda x: x.path):
            w.writerow([r.path, r.bytes, f"{r.bytes/1024/1024:.4f}", r.ext, r.top, r.top2])

    md_lines = []
    md_lines.append(f"# codexread 资产盘点 {day}")
    md_lines.append("")
    md_lines.append(f"- 生成时间 UTC：`{ts}`")
    md_lines.append(f"- 仓库：`{repo}`")
    md_lines.append(f"- 输出目录：`{out_dir}`")
    md_lines.append("")
    md_lines.append("## 关键结论")
    md_lines.append("")
    md_lines.append(f"- git 跟踪文件（git 列表）：`{len(tracked)}`")
    md_lines.append(f"- git 跟踪文件（磁盘存在）：`{len(rows)}`")
    md_lines.append(f"- git 跟踪但磁盘缺失：`{len(missing_on_disk)}`")
    md_lines.append(f"- 磁盘总文件数（排除 .git）：`{total_files}`")
    md_lines.append("- 高体量目录主要集中在 `state/` 与 `imports/`，不适合作为长期理解/索引输入。")
    md_lines.append(f"- 路径含非 ASCII 字符：`{len(non_ascii_paths)}`（部分工具可能显示为 quoted output）")
    md_lines.append(f"- 路径含空白字符（空格/制表等）：`{len(whitespace_paths)}`")
    md_lines.append("")
    md_lines.append("## finagent 索引建议")
    md_lines.append("")
    md_lines.append("建议纳入（高信噪比）：")
    md_lines.extend([f"- `{p}`" for p in recommended_allow])
    md_lines.append("")
    md_lines.append("建议排除（运行态/原始输入/噪声/敏感）：")
    md_lines.extend([f"- `{p}`" for p in recommended_deny])
    md_lines.append("")
    md_lines.append("## 投研入口文件（存在即优先读）")
    md_lines.append("")
    md_lines.extend([f"- `{p}`" for p in entry_files_present])
    md_lines.append("")
    md_lines.append("## 报表文件")
    md_lines.append("")
    md_lines.append(f"- 机器可读清单：`{json_path.name}`")
    md_lines.append(f"- 跟踪文件明细：`{tsv_path.name}`（TSV，含大小与目录归类）")
    md_lines.append("")
    md_lines.append("## 最大的跟踪文件（Top 20）")
    md_lines.append("")
    for r in largest[:20]:
        md_lines.append(f"- `{r.path}`：{r.bytes/1024/1024:.2f} MB")
    md_lines.append("")
    md_lines.append("## Top-level 磁盘占用（du）")
    md_lines.append("")
    for row in payload["duTopLevel"][:25]:
        md_lines.append(f"- `{row['name']}`：{row['size']}")
    md_lines.append("")

    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"Wrote: {md_path}")
    print(f"Wrote: {json_path}")
    print(f"Wrote: {tsv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
