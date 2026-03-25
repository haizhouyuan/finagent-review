#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class CommitInfo:
    sha: str
    unix_ts: int
    author: str
    subject: str


_GIT_LOG_HDR_RE = re.compile(r"^[0-9a-f]{40}\t\d+\t")


def _decode(b: bytes) -> str:
    # Preserve odd bytes as much as possible.
    return b.decode("utf-8", errors="surrogateescape")


def git_ls_files(repo: Path) -> list[str]:
    raw = subprocess.check_output(["git", "ls-files", "-z"], cwd=str(repo))
    return [_decode(p) for p in raw.split(b"\x00") if p]


def git_log_name_only(repo: Path, *, reverse: bool) -> Iterable[tuple[CommitInfo, list[str]]]:
    cmd = ["git", "log", "--name-only", "--pretty=format:%H\t%ct\t%an\t%s"]
    if reverse:
        cmd.insert(2, "--reverse")
    raw = subprocess.check_output(cmd, cwd=str(repo))
    text = _decode(raw)

    current_commit: CommitInfo | None = None
    current_files: list[str] = []

    def flush() -> tuple[CommitInfo, list[str]] | None:
        nonlocal current_commit, current_files
        if current_commit is None:
            return None
        item = (current_commit, current_files)
        current_commit = None
        current_files = []
        return item

    for line in text.splitlines():
        if _GIT_LOG_HDR_RE.match(line):
            maybe = flush()
            if maybe is not None:
                yield maybe
            sha, ts_s, author, subject = line.split("\t", 3)
            current_commit = CommitInfo(sha=sha, unix_ts=int(ts_s), author=author, subject=subject)
            continue

        if line.strip() == "":
            continue

        current_files.append(line)

    maybe = flush()
    if maybe is not None:
        yield maybe


def extract_markdown_structure(text: str) -> dict[str, Any]:
    headings: list[str] = []
    h1: str | None = None
    for line in text.splitlines():
        if not line.startswith("#"):
            continue
        m = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if not m:
            continue
        level = len(m.group(1))
        title = m.group(2).strip()
        if level == 1 and h1 is None:
            h1 = title
        headings.append(f"{'#' * level} {title}")
        if len(headings) >= 24:
            break
    return {"h1": h1, "headings": headings}


def extract_python_structure(text: str) -> dict[str, Any]:
    # Avoid heavy parsing dependencies; a simple regex is enough for inventory.
    defs: list[str] = []
    imports: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("import ") or s.startswith("from "):
            imports.append(s)
            if len(imports) >= 30:
                break
    for line in text.splitlines():
        m = re.match(r"^(class|def)\s+([A-Za-z_][A-Za-z0-9_]*)\b", line)
        if not m:
            continue
        defs.append(f"{m.group(1)} {m.group(2)}")
        if len(defs) >= 40:
            break
    return {"defs": defs, "imports": imports}


def extract_json_structure(text: str) -> dict[str, Any]:
    try:
        obj = json.loads(text)
    except Exception:
        return {"parseError": True}
    if isinstance(obj, dict):
        keys = list(obj.keys())
        return {"type": "object", "keys": keys[:60], "keysCount": len(keys)}
    if isinstance(obj, list):
        return {"type": "array", "length": len(obj)}
    return {"type": type(obj).__name__}


def extract_keyvalue_structure(text: str) -> dict[str, Any]:
    keys: list[str] = []
    for line in text.splitlines():
        if not line or line.lstrip().startswith("#"):
            continue
        m = re.match(r"^([A-Za-z0-9_.-]+)\s*[:=]\s*.+$", line)
        if not m:
            continue
        keys.append(m.group(1))
        if len(keys) >= 60:
            break
    return {"keys": keys, "keysCount": len(keys)}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_read_text(path: Path, *, max_bytes: int = 512 * 1024) -> str:
    b = path.read_bytes()
    if len(b) > max_bytes:
        b = b[:max_bytes]
    return _decode(b)


def classify_path(p: str) -> str:
    # Coarse purpose classification, for navigation & filtering.
    if p.startswith("mcp-servers/"):
        return "mcp"
    if p.startswith("apps/"):
        return "app"
    if p.startswith("scripts/"):
        return "script"
    if p.startswith("skills-src/"):
        return "skill"
    if p.startswith("docs/"):
        return "doc"
    if p.startswith("archives/"):
        return "archive"
    if p.startswith("templates/"):
        return "template"
    if p.startswith("contracts/"):
        return "contract"
    if p.startswith("ref/"):
        return "ref"
    if p.startswith("ops/"):
        return "ops"
    if p.startswith("examples/"):
        return "example"
    return "root"


def extract_structure(path: str, text: str) -> dict[str, Any]:
    ext = Path(path).suffix.lower()
    if ext == ".md":
        return extract_markdown_structure(text)
    if ext == ".py":
        return extract_python_structure(text)
    if ext == ".json":
        return extract_json_structure(text)
    if ext in {".yaml", ".yml", ".toml", ".ini", ".cfg"}:
        return extract_keyvalue_structure(text)
    if ext in {".sh", ".service", ".timer"}:
        return extract_keyvalue_structure(text)
    return {}


def build_backtick_reference_index(repo: Path, *, source_files: list[str], tracked_set: set[str]) -> dict[str, list[str]]:
    # Maps tracked_path -> list of source docs that mention it in backticks.
    refs: dict[str, list[str]] = {}
    token_re = re.compile(r"`([^`]{1,500})`")
    for src in source_files:
        p = repo / src
        if not p.exists():
            continue
        text = safe_read_text(p, max_bytes=1024 * 1024)
        for m in token_re.finditer(text):
            token = m.group(1).strip()
            # Normalize: strip leading ./ that might appear in docs.
            if token.startswith("./"):
                token = token[2:]
            if token in tracked_set:
                refs.setdefault(token, []).append(src)
    return refs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="/vol1/1000/projects/codexread")
    ap.add_argument("--out-dir", default="/vol1/1000/projects/finagent/docs/codexread")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (defaults to today)")
    ap.add_argument("--hash", action="store_true", help="Compute sha256 for each tracked file (slower)")
    args = ap.parse_args()

    repo = Path(args.repo).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not (repo / ".git").exists():
        raise SystemExit(f"Not a git repo: {repo}")

    day = args.date or dt.date.today().isoformat()
    ts = dt.datetime.now(dt.timezone.utc).isoformat()

    tracked = git_ls_files(repo)
    tracked_set = set(tracked)

    # Git lineage (fast, path-based; does not follow renames).
    last: dict[str, CommitInfo] = {}
    first: dict[str, CommitInfo] = {}
    counts: dict[str, int] = {p: 0 for p in tracked}

    for commit, files in git_log_name_only(repo, reverse=False):
        for f in files:
            if f not in tracked_set:
                continue
            if f not in last:
                last[f] = commit
            counts[f] += 1

    for commit, files in git_log_name_only(repo, reverse=True):
        for f in files:
            if f not in tracked_set:
                continue
            if f not in first:
                first[f] = commit

    # Reference hints from root docs/specs.
    reference_sources = [
        "README.md",
        "spec.md",
        "AGENTS.md",
        "docs/investing/README.md",
        "archives/investing/README.md",
    ]
    referenced_by = build_backtick_reference_index(
        repo, source_files=reference_sources, tracked_set=tracked_set
    )

    # Produce per-file catalog.
    jsonl_path = out_dir / f"codexread_deep_catalog_{day}.jsonl"
    tsv_path = out_dir / f"codexread_deep_catalog_{day}.tsv"
    md_path = out_dir / f"codexread_deep_catalog_{day}.md"

    # TSV header chosen for easy filtering/grep/Excel.
    tsv_header = [
        "path",
        "kind",
        "bytes",
        "lines",
        "ext",
        "top",
        "top2",
        "h1",
        "defs",
        "keys",
        "first_commit",
        "first_date_utc",
        "last_commit",
        "last_date_utc",
        "commit_count",
        "referenced_by",
        "sha256",
    ]

    # Prepare md index grouped by top2.
    groups: dict[str, list[dict[str, Any]]] = {}

    with jsonl_path.open("w", encoding="utf-8") as jf, tsv_path.open("w", encoding="utf-8") as tf:
        tf.write("\t".join(tsv_header) + "\n")

        for p in tracked:
            fp = repo / p
            st = fp.stat()
            ext = fp.suffix.lower().lstrip(".")
            parts = p.split("/")
            top = parts[0] if parts else ""
            top2 = "/".join(parts[:2]) if len(parts) >= 2 else top
            kind = classify_path(p)

            sha256 = sha256_file(fp) if args.hash else ""

            lines = 0
            h1 = ""
            defs: list[str] = []
            keys: list[str] = []
            structure: dict[str, Any] = {}

            # Only do structural extraction for plausible text files.
            if st.st_size <= 2 * 1024 * 1024 and fp.is_file():
                try:
                    text = safe_read_text(fp, max_bytes=1024 * 1024)
                    lines = text.count("\n") + (1 if text else 0)
                    structure = extract_structure(p, text)
                    h1 = (structure.get("h1") or "") if isinstance(structure, dict) else ""
                    if isinstance(structure, dict):
                        if "defs" in structure and isinstance(structure["defs"], list):
                            defs = [str(x) for x in structure["defs"][:20]]
                        if "keys" in structure and isinstance(structure["keys"], list):
                            keys = [str(x) for x in structure["keys"][:20]]
                except Exception:
                    pass

            fc = first.get(p)
            lc = last.get(p)

            record: dict[str, Any] = {
                "path": p,
                "kind": kind,
                "bytes": st.st_size,
                "lines": lines,
                "ext": ext,
                "top": top,
                "top2": top2,
                "structure": structure,
                "git": {
                    "first": asdict_commit(fc),
                    "last": asdict_commit(lc),
                    "count": counts.get(p, 0),
                },
                "referencedBy": referenced_by.get(p, []),
                "sha256": sha256 or None,
            }
            jf.write(json.dumps(record, ensure_ascii=False) + "\n")

            groups.setdefault(top2 or "(root)", []).append(
                {
                    "path": p,
                    "bytes": st.st_size,
                    "lines": lines,
                    "kind": kind,
                    "h1": h1,
                    "defs": defs,
                    "keys": keys,
                    "first": fc,
                    "last": lc,
                    "count": counts.get(p, 0),
                    "ref": referenced_by.get(p, []),
                }
            )

            tf.write(
                "\t".join(
                    [
                        p,
                        kind,
                        str(st.st_size),
                        str(lines),
                        ext,
                        top,
                        top2,
                        h1.replace("\t", " "),
                        ", ".join(defs).replace("\t", " "),
                        ", ".join(keys).replace("\t", " "),
                        (fc.sha if fc else ""),
                        (fmt_utc(fc.unix_ts) if fc else ""),
                        (lc.sha if lc else ""),
                        (fmt_utc(lc.unix_ts) if lc else ""),
                        str(counts.get(p, 0)),
                        ",".join(referenced_by.get(p, [])),
                        sha256,
                    ]
                )
                + "\n"
            )

    # Write a navigable MD catalog (grouped, but kept compact).
    md_lines: list[str] = []
    md_lines.append(f"# codexread 深度目录（Deep Catalog）{day}")
    md_lines.append("")
    md_lines.append(f"- 生成时间 UTC：`{ts}`")
    md_lines.append(f"- 仓库：`{repo}`")
    md_lines.append(f"- 文件总数（git tracked）：`{len(tracked)}`")
    md_lines.append("")
    md_lines.append("说明：本目录面向“每个文件是什么 + 结构/入口 + git 来龙去脉（按路径）”。")
    md_lines.append("")
    md_lines.append("产物：")
    md_lines.append(f"- 逐文件 JSONL：`{jsonl_path.name}`")
    md_lines.append(f"- 逐文件 TSV：`{tsv_path.name}`")
    md_lines.append("")
    md_lines.append("## 分组（按 top2）")
    md_lines.append("")

    for group in sorted(groups.keys()):
        items = sorted(groups[group], key=lambda x: x["path"])
        md_lines.append(f"### `{group}`（{len(items)}）")
        md_lines.append("")
        for it in items:
            meta: list[str] = []
            meta.append(it["kind"])
            meta.append(f"{it['bytes']/1024:.1f} KB")
            if it["lines"]:
                meta.append(f"{it['lines']} lines")
            if it.get("h1"):
                meta.append(f"H1={it['h1']}")
            if it.get("defs"):
                meta.append("defs=" + "; ".join(it["defs"][:6]))
            if it.get("keys"):
                meta.append("keys=" + ", ".join(it["keys"][:8]))
            if it.get("ref"):
                meta.append("ref=" + ",".join(it["ref"]))
            if it.get("last"):
                lc: CommitInfo = it["last"]
                meta.append(f"last={fmt_utc(lc.unix_ts)} {lc.author}")
            if it.get("first"):
                fc: CommitInfo = it["first"]
                meta.append(f"first={fmt_utc(fc.unix_ts)} {fc.author}")
            if it.get("count") is not None:
                meta.append(f"commits={it['count']}")
            md_lines.append(f"- `{it['path']}` — " + " · ".join(meta))
        md_lines.append("")

    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"Wrote: {md_path}")
    print(f"Wrote: {jsonl_path}")
    print(f"Wrote: {tsv_path}")
    return 0


def fmt_utc(unix_ts: int) -> str:
    return dt.datetime.fromtimestamp(unix_ts, tz=dt.timezone.utc).strftime("%Y-%m-%d")


def asdict_commit(c: CommitInfo | None) -> dict[str, Any] | None:
    if c is None:
        return None
    return {"sha": c.sha, "unix_ts": c.unix_ts, "author": c.author, "subject": c.subject}


if __name__ == "__main__":
    raise SystemExit(main())

