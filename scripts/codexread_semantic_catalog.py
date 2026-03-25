#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _safe_read_text(path: Path, *, max_bytes: int = 256 * 1024) -> str:
    b = path.read_bytes()
    if len(b) > max_bytes:
        b = b[:max_bytes]
    return b.decode("utf-8", errors="replace")


def _first_sentence(text: str) -> str:
    t = " ".join(text.strip().split())
    if not t:
        return ""
    for sep in ["。", ".", "!", "？", "?"]:
        i = t.find(sep)
        if i != -1 and i <= 140:
            return t[: i + 1].strip()
    return (t[:140] + "…") if len(t) > 140 else t


def _python_module_docstring(text: str) -> str:
    # Very lightweight: grab first triple-quoted block after shebang/encoding.
    lines = text.splitlines()
    i = 0
    if lines and lines[0].startswith("#!"):
        i = 1
    # Skip encoding / future imports / empty lines / comments
    while i < len(lines):
        s = lines[i].strip()
        if s == "" or s.startswith("#") or s.startswith("from __future__"):
            i += 1
            continue
        break
    if i >= len(lines):
        return ""

    rest = "\n".join(lines[i:])
    m = re.match(r"^(?P<q>\"\"\"|''')(?P<body>.*?)(?P=q)", rest, flags=re.DOTALL)
    if not m:
        return ""
    body = m.group("body").strip()
    return _first_sentence(body)


def _argparse_description(text: str) -> str:
    # Best effort: find description="..."
    m = re.search(
        r"ArgumentParser\([\s\S]{0,4000}?description\s*=\s*(?P<q>'|\")(?P<d>[\s\S]{1,400}?)(?P=q)",
        text,
    )
    if not m:
        return ""
    d = m.group("d").strip()
    return _first_sentence(d)


def _top_comment(text: str) -> str:
    lines = text.splitlines()
    if lines and lines[0].startswith("#!"):
        lines = lines[1:]
    for line in lines[:50]:
        s = line.strip()
        if not s:
            continue
        if s.startswith("#"):
            s2 = s.lstrip("#").strip()
            if s2:
                return _first_sentence(s2)
            continue
        break
    return ""


def _basename(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def _mcp_name(path: str) -> str:
    parts = path.split("/")
    return parts[1] if len(parts) >= 2 else ""


MCP_PURPOSE = {
    "tasks": "任务管理（SQLite 存储、创建/查询/更新任务）",
    "video_pipeline": "视频流水线（ASR/OCR/抽帧/证据包）",
    "tmux_orchestrator": "tmux worker 调度（确定性作业派发与记录）",
    "websearch_router": "搜索路由（多 provider 的结构化 SERP 后端）",
    "glm_router": "GLM 路由/写文件工具（free→paid 回退，结构化加工）",
    "source_pack": "证据包抓取（URL 抓取→落盘 manifest/raw/text）",
    "mem0_memory": "mem0 记忆 MCP 适配（存储/检索/更新）",
}


def _kind(path: str) -> str:
    if path.startswith("mcp-servers/"):
        return "mcp"
    if path.startswith("apps/"):
        return "app"
    if path.startswith("scripts/"):
        return "script"
    if path.startswith("skills-src/"):
        return "skill"
    if path.startswith("docs/"):
        return "doc"
    if path.startswith("archives/"):
        return "archive"
    if path.startswith("templates/"):
        return "template"
    if path.startswith("contracts/"):
        return "contract"
    if path.startswith("ref/"):
        return "ref"
    if path.startswith("ops/"):
        return "ops"
    if path.startswith("examples/"):
        return "example"
    if path.startswith("workflows/"):
        return "workflow"
    return "root"


def summarize(repo: Path, rec: dict[str, Any]) -> tuple[str, str]:
    """
    Returns (summary, source) where source indicates what evidence was used:
    docstring|argparse|heading|comment|path
    """
    path = rec["path"]
    kind = _kind(path)
    ext = ("." + rec.get("ext", "")) if rec.get("ext") else Path(path).suffix.lower()
    h1 = ""
    structure = rec.get("structure") or {}
    if isinstance(structure, dict):
        h1 = (structure.get("h1") or "").strip()

    # High-level path heuristics first.
    if path == ".env.example":
        return "环境变量示例（本仓库本地运行所需 key/配置的占位模板）", "path"

    if path == ".gitignore":
        return "Git 忽略规则（避免把运行态/大文件/密钥等提交进仓库）", "path"

    base = _basename(path)
    if base == ".gitignore" and path != ".gitignore":
        parent = path.rsplit("/", 1)[0] + "/"
        return f"目录 `{parent}` 的 gitignore（忽略输出/运行态/大文件）", "path"

    if path == ".github/CODEOWNERS":
        return "GitHub CODEOWNERS（代码所有权/审阅责任分配规则）", "path"

    if path == ".github/pull_request_template.md":
        return "GitHub PR 模板（提交说明清单）", "path"

    if path == "PLAN.md":
        return "项目计划/路线图（阶段目标、任务拆解与推进记录）", "path"

    if path.startswith("ChatgptREST_") and path.endswith(".md"):
        # Avoid noisy first-lines; prefer a stable semantic label.
        return f"ChatgptREST 相关文档/记录：`{_basename(path)}`", "path"

    if kind == "mcp":
        name = _mcp_name(path)
        purpose = MCP_PURPOSE.get(name, f"{name} MCP")
        base = _basename(path)
        if base == "README.md":
            return f"{name}：MCP 使用说明（{purpose}）", "path"
        if base == "server.py":
            return f"{name}：MCP server 实现（{purpose}）", "path"
        return f"{name}：MCP 内部模块 `{base}`（{purpose}）", "path"

    if path.startswith("apps/dashboard/"):
        base = _basename(path)
        if base == "app.py":
            return "dashboard：主应用（只读浏览 topics/digests/tasks 等）", "path"
        if base == "run.py":
            return "dashboard：启动入口（env 加载/鉴权配置/启动参数）", "path"
        if base == "requirements.txt":
            return "dashboard：Python 依赖清单", "path"
        if path.startswith("apps/dashboard/templates/"):
            return f"dashboard：HTML 模板 `{base}`（页面渲染）", "path"
        if path.startswith("apps/dashboard/static/"):
            return f"dashboard：静态资源 `{base}`（样式/前端资源）", "path"
        if h1:
            return f"dashboard：{h1}", "heading"
        return f"dashboard：组件/视图 `{base}`", "path"

    if path.startswith("apps/control_daemon/"):
        base = _basename(path)
        if base == "README.md" and h1:
            return f"control_daemon：{h1}", "heading"
        if base == "__main__.py":
            return "control_daemon：模块入口（启动控制面服务）", "path"
        if base == "config.py":
            return "control_daemon：配置加载（env → ControlDaemonConfig）", "path"
        if base == "job_store.py":
            return "control_daemon：作业/幂等存储（claim、run、idempotency 等）", "path"
        if base == "runner.py":
            return "control_daemon：作业执行器（安全加载 allowed scripts、启动进程、回写状态）", "path"
        if base == "server.py":
            return "control_daemon：HTTP API 服务端（提交/查询/租约/队列控制）", "path"
        if base == "tmux_backend.py":
            return "control_daemon：tmux 后端（派发命令、采集输出、加锁与隔离）", "path"
        if base == "types.py":
            return "control_daemon：类型定义（Job/Run 数据结构与协议）", "path"
        if base.startswith("test_") and base.endswith(".py"):
            return "control_daemon：单元测试（fake 后端 + 关键路径覆盖）", "path"
        if h1:
            return f"control_daemon：{h1}", "heading"
        return f"control_daemon：控制面组件 `{base}`（作业调度/控制平面）", "path"

    if path.startswith("apps/repair_daemon/"):
        base = _basename(path)
        if base == "package.json":
            return "repair_daemon：Node/TS 服务包配置（依赖、scripts、入口）", "path"
        if base == "package-lock.json":
            return "repair_daemon：依赖锁定文件（npm lockfile）", "path"
        if base == "tsconfig.json":
            return "repair_daemon：TypeScript 编译配置", "path"
        if path.startswith("apps/repair_daemon/src/"):
            if base == "cli.ts":
                return "repair_daemon：CLI 入口（命令行调度/诊断/修复动作）", "path"
            if base == "config.ts":
                return "repair_daemon：配置加载（env/默认值/路径约定）", "path"
            if base == "file_lock.ts":
                return "repair_daemon：文件锁（并发安全写入/队列互斥）", "path"
            if base == "fs_utils.ts":
                return "repair_daemon：文件系统工具（读写/原子操作/路径安全）", "path"
            if base == "index.ts":
                return "repair_daemon：模块导出/聚合入口（library entry）", "path"
            if base == "job_events.ts":
                return "repair_daemon：作业事件模型（状态流/事件记录）", "path"
            if base == "job_queue.ts":
                return "repair_daemon：作业队列（入队/出队/重试/调度）", "path"
            if base == "job_store.ts":
                return "repair_daemon：作业存储（持久化/幂等/索引）", "path"
            if base == "paths.ts":
                return "repair_daemon：路径约定（state/logs/artifacts 等目录映射）", "path"
            if base == "repair_plan.ts":
                return "repair_daemon：修复计划生成（从失败/缺口推导可执行 repair plan）", "path"
            if base == "rules.ts":
                return "repair_daemon：规则集（何时修复/如何重试/兜底策略）", "path"
            if base == "server.ts":
                return "repair_daemon：服务端入口（HTTP/控制面接口，触发修复/查询状态）", "path"
            if base == "thread_key.ts":
                return "repair_daemon：线程/会话 key 生成（稳定关联对话/作业）", "path"
            if base == "threads.ts":
                return "repair_daemon：线程/会话管理（创建/复用/映射）", "path"
            if base == "types.ts":
                return "repair_daemon：类型定义（Job/Plan/事件结构）", "path"
            return f"repair_daemon：TypeScript 模块 `{base}`", "path"
        if h1:
            return f"repair_daemon：{h1}", "heading"
        return f"repair_daemon：修复/重试辅助组件 `{base}`", "path"

    if path.startswith("archives/investing/"):
        base = _basename(path)
        if base == "README.md":
            return "archives/investing：投研全局收敛层入口说明（universe/watchlist/decisions 等）", "path"
        if base == "universe.json":
            return "archives/investing：全局候选池（机器可读 universe）", "path"
        if base == "watchlist.md":
            return "archives/investing：当前优先跟踪清单（人可读 watchlist）", "path"
        if base == "security_master.json":
            return "archives/investing：标的主数据表（entity_key + identifiers/aliases）", "path"
        if base == "source_registry.yaml":
            return "archives/investing：信息源注册表与监控策略（source registry）", "path"
        if base == "convergence.md":
            return "archives/investing：收敛状态快照（解释版本/进度/现状）", "path"
        if path.startswith("archives/investing/decisions/"):
            return f"archives/investing：投资决策包 `{base}`（人工审计文档）", "path"
        if h1:
            return f"archives/investing：{h1}", "heading"
        return f"archives/investing：投研收敛层文件 `{base}`", "path"

    if path == "archives/topics/README.md":
        return "archives/topics：主题档案入口说明（topic 目录结构与约定）", "path"

    if kind == "skill" and path.endswith("SKILL.md"):
        skill = path.split("/")[1] if len(path.split("/")) > 1 else _basename(path)
        return f"skill：{skill} 的工作流说明/约束（供 Codex 调用）", "path"

    if kind == "contract":
        if path == "contracts/README.md" and h1:
            return f"contracts：{h1}", "heading"
        if path.startswith("contracts/schemas/") and path.endswith(".schema.json"):
            name = base.removesuffix(".schema.json")
            return f"contracts：JSON Schema（{name}）", "path"
        if path.startswith("contracts/fixtures/"):
            return f"contracts：fixture 示例（{base}）", "path"
        return f"contracts：契约相关文件 `{base}`", "path"

    if kind == "template":
        if path == "templates/README.md" and h1:
            return f"templates：{h1}", "heading"
        if path == "templates/monitoring.yaml":
            return "templates：监控配置模板（monitoring.yaml）", "path"
        if path == "templates/robot-update-package.json":
            return "templates：机器人变更包模板（robot-update-package.json）", "path"
        if path == "templates/creator_registry.json":
            return "templates：创作者/频道 registry 模板（creator_registry.json）", "path"
        if base in {"digest.md", "research.md"}:
            return f"templates：文档模板 `{base}`（结构化撰写骨架）", "path"
        if path.startswith("templates/prompts/pro_workers/"):
            return f"templates：Pro worker prompt `{base}`", "path"
        if path.startswith("templates/topic/"):
            return f"templates：topic 模板 `{base}`（新建 topic 档案骨架）", "path"
        if h1:
            return f"templates：{h1}", "heading"
        return f"templates：模板文件 `{base}`", "path"

    # Content-based extraction (for scripts/docs), then fallback to headings/path.
    fp = repo / path
    if fp.exists() and fp.is_file():
        try:
            text = _safe_read_text(fp)
        except Exception:
            text = ""

        if ext == ".py":
            doc = _python_module_docstring(text)
            if doc:
                return doc, "docstring"
            desc = _argparse_description(text)
            if desc:
                return desc, "argparse"
            c = _top_comment(text)
            if c:
                return c, "comment"

        if ext in {".sh", ".service", ".timer"}:
            c = _top_comment(text)
            if c:
                return c, "comment"

        if ext == ".md":
            if h1:
                # For docs, H1 is usually the best one-line.
                return h1, "heading"
            # Fallback: first non-empty paragraph line (skip YAML front matter / separators).
            lines = text.splitlines()
            i = 0
            if lines and lines[0].strip() == "---":
                i = 1
                while i < len(lines) and lines[i].strip() != "---":
                    i += 1
                if i < len(lines) and lines[i].strip() == "---":
                    i += 1
            for line in lines[i:]:
                s = line.strip()
                if not s or s.startswith("#") or s in {"---", "..."}:
                    continue
                return _first_sentence(s), "content"

    # Path-only fallbacks.
    if base.endswith("-spec.md") or base.endswith("_spec.md") or "spec" in base.lower():
        return f"规格/契约文档：`{base}`", "path"
    if base == "README.md" and "/" in path:
        return f"目录入口说明：`{path.rsplit('/',1)[0]}/`", "path"
    if kind == "script":
        return f"脚本：`{base}`（工作流自动化/批处理）", "path"
    if kind == "doc":
        return f"文档：`{base}`", "path"
    return f"`{base}`（{kind}）", "path"


def include_path(path: str) -> bool:
    # Default policy: include high-signal; exclude `archives/topics/*` except README.
    if path.startswith("archives/topics/") and path != "archives/topics/README.md":
        return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="/vol1/1000/projects/codexread")
    ap.add_argument("--out-dir", default="/vol1/1000/projects/finagent/docs/codexread")
    ap.add_argument("--date", default=None)
    ap.add_argument(
        "--deep-catalog-jsonl",
        default="/vol1/1000/projects/finagent/docs/codexread/codexread_deep_catalog_2026-02-18.jsonl",
        help="Path to codexread_deep_catalog_YYYY-MM-DD.jsonl",
    )
    args = ap.parse_args()

    repo = Path(args.repo).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    day = args.date or dt.date.today().isoformat()
    ts = dt.datetime.now(dt.timezone.utc).isoformat()

    deep_path = Path(args.deep_catalog_jsonl).expanduser().resolve()
    rows = _read_jsonl(deep_path)

    included = [r for r in rows if include_path(r["path"])]

    # Output paths
    tsv_path = out_dir / f"codexread_semantic_catalog_{day}.tsv"
    md_path = out_dir / f"codexread_semantic_catalog_{day}.md"
    jsonl_path = out_dir / f"codexread_semantic_catalog_{day}.jsonl"

    header = [
        "path",
        "kind",
        "summary",
        "summary_source",
        "bytes",
        "lines",
        "ext",
        "top2",
        "h1",
        "defs",
        "keys",
        "first_date_utc",
        "first_author",
        "first_subject",
        "last_date_utc",
        "last_author",
        "last_subject",
        "commit_count",
        "referenced_by",
    ]

    # Group for MD navigation.
    groups: dict[str, list[dict[str, Any]]] = {}

    with tsv_path.open("w", encoding="utf-8", newline="") as tf, jsonl_path.open("w", encoding="utf-8") as jf:
        w = csv.writer(tf, delimiter="\t", lineterminator="\n")
        w.writerow(header)

        for rec in included:
            path = rec["path"]
            kind = _kind(path)
            summary, source = summarize(repo, rec)

            structure = rec.get("structure") or {}
            h1 = structure.get("h1") if isinstance(structure, dict) else ""
            defs = ""
            keys = ""
            if isinstance(structure, dict):
                if isinstance(structure.get("defs"), list):
                    defs = ", ".join(str(x) for x in structure["defs"][:20])
                if isinstance(structure.get("keys"), list):
                    keys = ", ".join(str(x) for x in structure["keys"][:20])

            git = rec.get("git") or {}
            first = git.get("first") or {}
            last = git.get("last") or {}

            first_date = (
                dt.datetime.fromtimestamp(first["unix_ts"], tz=dt.timezone.utc).strftime("%Y-%m-%d")
                if isinstance(first, dict) and first.get("unix_ts")
                else ""
            )
            last_date = (
                dt.datetime.fromtimestamp(last["unix_ts"], tz=dt.timezone.utc).strftime("%Y-%m-%d")
                if isinstance(last, dict) and last.get("unix_ts")
                else ""
            )

            ref_by = rec.get("referencedBy") or []
            if not isinstance(ref_by, list):
                ref_by = []

            row = [
                path,
                kind,
                summary,
                source,
                rec.get("bytes", ""),
                rec.get("lines", ""),
                rec.get("ext", ""),
                rec.get("top2", ""),
                h1 or "",
                defs,
                keys,
                first_date,
                first.get("author", "") if isinstance(first, dict) else "",
                first.get("subject", "") if isinstance(first, dict) else "",
                last_date,
                last.get("author", "") if isinstance(last, dict) else "",
                last.get("subject", "") if isinstance(last, dict) else "",
                git.get("count", "") if isinstance(git, dict) else "",
                ",".join(ref_by),
            ]
            w.writerow(row)

            enriched = dict(rec)
            enriched["semantic"] = {"summary": summary, "source": source}
            jf.write(json.dumps(enriched, ensure_ascii=False) + "\n")

            groups.setdefault(rec.get("top2") or "(root)", []).append(
                {
                    "path": path,
                    "kind": kind,
                    "summary": summary,
                    "source": source,
                    "last_date": last_date,
                    "last_author": last.get("author", "") if isinstance(last, dict) else "",
                    "commit_count": git.get("count", 0) if isinstance(git, dict) else 0,
                }
            )

    # MD
    md_lines: list[str] = []
    md_lines.append(f"# codexread 语义目录（Semantic Catalog）{day}")
    md_lines.append("")
    md_lines.append(f"- 生成时间 UTC：`{ts}`")
    md_lines.append(f"- 仓库：`{repo}`")
    md_lines.append(f"- 收录文件数：`{len(included)}`（默认排除 `archives/topics/*`，仅保留 `archives/topics/README.md`）")
    md_lines.append("")
    md_lines.append("产物：")
    md_lines.append(f"- TSV：`{tsv_path.name}`（可筛选/排序）")
    md_lines.append(f"- JSONL：`{jsonl_path.name}`（含 deep catalog 原字段 + semantic.summary）")
    md_lines.append("")

    for group in sorted(groups.keys()):
        items = sorted(groups[group], key=lambda x: x["path"])
        md_lines.append(f"## `{group}`（{len(items)}）")
        md_lines.append("")
        for it in items:
            md_lines.append(
                f"- `{it['path']}` — {it['summary']} · src={it['source']} · last={it['last_date']} {it['last_author']} · commits={it['commit_count']}"
            )
        md_lines.append("")

    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"Wrote: {md_path}")
    print(f"Wrote: {tsv_path}")
    print(f"Wrote: {jsonl_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
