#!/usr/bin/env python3
"""Sync a curated finagent review pack to a dedicated review repo.

This mirrors the ChatgptREST public-review-repo workflow, but keeps the
payload intentionally focused:

- current finagent source / scripts / tests
- key walkthrough / blueprint / research docs
- a curated external-reference skeleton for OpenBB / qlib / TradingAgents

The result can be used for local Gemini/ChatGPT code review imports, or pushed
to a dedicated public review repo branch.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REVIEW_REPO = "finagent-review"
DEFAULT_REPO_DIR = Path("/tmp/finagent-review")
DEFAULT_IMPORT_BRANCH = "main"

SOURCE_DIRS = [
    "finagent",
    "scripts",
    "tests",
]

ROOT_FILES = [
    "README.md",
    "pyproject.toml",
    "pytest.ini",
]

DOC_FILES = [
    "docs/research/2026-03-25_external_reference_repo_bundle_v1.md",
    "docs/blueprints/2026-03-23_two_wheeler_pilot_blueprint_v2.md",
    "docs/2026-03-23_two_wheeler_pilot_walkthrough.md",
    "docs/2026-03-15_event_mining_benchmark_refactor_walkthrough_v1.md",
    "docs/reviews/2026-03-15_public_info_engine_benchmark_review_v1.md",
]

REFERENCE_FILES = [
    "external_research_repos/OpenBB/README.md",
    "external_research_repos/OpenBB/openbb_platform/README.md",
    "external_research_repos/OpenBB/openbb_platform/core/openbb_core/app/provider_interface.py",
    "external_research_repos/OpenBB/openbb_platform/core/openbb_core/app/query.py",
    "external_research_repos/OpenBB/openbb_platform/core/openbb_core/provider/abstract/fetcher.py",
    "external_research_repos/OpenBB/openbb_platform/core/openbb_core/provider/abstract/provider.py",
    "external_research_repos/OpenBB/openbb_platform/core/openbb_core/provider/query_executor.py",
    "external_research_repos/OpenBB/openbb_platform/core/openbb_core/provider/registry.py",
    "external_research_repos/OpenBB/openbb_platform/core/openbb_core/provider/standard_models/equity_quote.py",
    "external_research_repos/OpenBB/openbb_platform/providers/yfinance/openbb_yfinance/models/equity_quote.py",
    "external_research_repos/qlib/README.md",
    "external_research_repos/qlib/examples/workflow_by_code.py",
    "external_research_repos/qlib/qlib/data/pit.py",
    "external_research_repos/qlib/qlib/backtest/backtest.py",
    "external_research_repos/qlib/qlib/workflow/__init__.py",
    "external_research_repos/qlib/qlib/workflow/exp.py",
    "external_research_repos/qlib/qlib/workflow/expm.py",
    "external_research_repos/qlib/qlib/workflow/recorder.py",
    "external_research_repos/qlib/qlib/workflow/record_temp.py",
    "external_research_repos/qlib/qlib/workflow/task/manage.py",
    "external_research_repos/TradingAgents/README.md",
    "external_research_repos/TradingAgents/tradingagents/default_config.py",
    "external_research_repos/TradingAgents/tradingagents/graph/trading_graph.py",
    "external_research_repos/TradingAgents/tradingagents/graph/setup.py",
    "external_research_repos/TradingAgents/tradingagents/graph/conditional_logic.py",
    "external_research_repos/TradingAgents/tradingagents/graph/reflection.py",
    "external_research_repos/TradingAgents/tradingagents/agents/utils/agent_states.py",
    "external_research_repos/TradingAgents/tradingagents/agents/managers/research_manager.py",
    "external_research_repos/TradingAgents/tradingagents/agents/managers/portfolio_manager.py",
]

EXCLUDE_DIRS = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    ".venv-native",
    "node_modules",
    "artifacts",
    "archives",
    "imports",
    "state",
    ".gitnexus",
    "external_research_repos",
}
EXCLUDE_SUFFIXES = {".pyc", ".pyo", ".db", ".sqlite", ".sqlite3", ".wal", ".shm", ".zip"}


def _run(cmd: list[str], *, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd or REPO_ROOT),
        check=check,
        capture_output=True,
        text=True,
    )


def source_commit() -> str:
    result = _run(["git", "rev-parse", "HEAD"])
    return result.stdout.strip()


def public_source_repo_url() -> str:
    try:
        raw = _run(["git", "remote", "get-url", "origin"]).stdout.strip()
    except Exception:
        return str(REPO_ROOT)
    if raw.startswith("git@github.com:"):
        slug = raw.removeprefix("git@github.com:")
        if slug.endswith(".git"):
            slug = slug[:-4]
        return f"https://github.com/{slug}"
    if raw.startswith("https://github.com/"):
        return raw[:-4] if raw.endswith(".git") else raw
    return raw or str(REPO_ROOT)


def _safe_rel_path(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


def is_excluded(path: Path) -> bool:
    rel = _safe_rel_path(path)
    if any(part in EXCLUDE_DIRS for part in path.parts):
        return True
    if path.suffix in EXCLUDE_SUFFIXES:
        return True
    if rel.startswith("external_research_repos/"):
        return rel not in REFERENCE_FILES
    return False


def copy_file(src: Path, dst_root: Path) -> int:
    rel = src.relative_to(REPO_ROOT)
    dst = dst_root / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return src.stat().st_size


def curated_paths() -> list[Path]:
    paths: list[Path] = []
    for rel_dir in SOURCE_DIRS:
        src_dir = REPO_ROOT / rel_dir
        if not src_dir.is_dir():
            continue
        for path in sorted(src_dir.rglob("*")):
            if not path.is_file():
                continue
            if is_excluded(path):
                continue
            paths.append(path)
    for rel_path in ROOT_FILES + DOC_FILES + REFERENCE_FILES:
        path = REPO_ROOT / rel_path
        if path.is_file():
            paths.append(path)
    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[Path] = []
    for path in paths:
        rel = _safe_rel_path(path)
        if rel in seen:
            continue
        seen.add(rel)
        deduped.append(path)
    return deduped


def sync_source_files(dst_dir: Path) -> dict[str, Any]:
    stats = {"files": 0, "bytes": 0}
    for src in curated_paths():
        stats["bytes"] += copy_file(src, dst_dir)
        stats["files"] += 1
    return stats


def generate_review_context(
    dst_dir: Path,
    *,
    branch_name: str,
    review_instructions: str,
    source_commit_hash: str,
) -> None:
    review_context = f"""# Finagent Review Context

## Source

- source repo: `{public_source_repo_url()}`
- source commit: `{source_commit_hash}`
- review branch: `{branch_name}`

## Review Goal

Review the latest `finagent` architecture against the local external reference repos,
with emphasis on:

1. OpenBB as data-bus / provider-contract reference
2. qlib as workflow / recorder / replay / leakage-discipline reference
3. TradingAgents as committee-review / debate-loop reference

## Required Judgment

- What should `finagent` borrow directly?
- What should `finagent` explicitly avoid?
- Where should changes land incrementally in the existing architecture?
- What should be deferred because it would be dog-tail accretion rather than leverage?

## Current Finagent Focus Areas

- discovery loop / research orchestration
- graph + evidence + memory retrieval
- event / claim / chronology discipline
- thesis evolution / writeback / review discipline
- recent two-wheeler pilot closure

## Notes

- External reference repos are intentionally curated here, not mirrored in full.
- Treat them as reference parts, not replacement architectures.

## Review Instructions

{review_instructions}
"""
    (dst_dir / "REVIEW_CONTEXT.md").write_text(review_context, encoding="utf-8")
    (dst_dir / "REVIEW_SOURCE.json").write_text(
        json.dumps(
            {
                "source_repo": public_source_repo_url(),
                "source_commit": source_commit_hash,
                "source_commit_url": f"{public_source_repo_url()}/commit/{source_commit_hash}",
                "review_branch": branch_name,
                "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def ensure_repo(repo_dir: Path) -> None:
    repo_dir.mkdir(parents=True, exist_ok=True)
    if (repo_dir / ".git").exists():
        return
    _run(["git", "init", "-b", DEFAULT_IMPORT_BRANCH], cwd=repo_dir)
    (repo_dir / "README.md").write_text("# finagent review repo\n", encoding="utf-8")
    _run(["git", "add", "README.md"], cwd=repo_dir)
    _run(["git", "commit", "-m", "Initialize finagent review repo"], cwd=repo_dir)


def clear_repo_contents(repo_dir: Path) -> None:
    for child in repo_dir.iterdir():
        if child.name == ".git":
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def create_github_repo(repo_name: str, repo_dir: Path) -> None:
    _run(
        [
            "gh",
            "repo",
            "create",
            f"haizhouyuan/{repo_name}",
            "--public",
            "--source",
            str(repo_dir),
            "--remote",
            "origin",
            "--push",
        ],
        cwd=REPO_ROOT,
    )


def commit_review_bundle(repo_dir: Path, branch_name: str, source_commit_hash: str) -> None:
    _run(["git", "checkout", "-B", branch_name], cwd=repo_dir)
    _run(["git", "add", "."], cwd=repo_dir)
    status = _run(["git", "status", "--short"], cwd=repo_dir)
    if not status.stdout.strip():
        return
    _run(
        [
            "git",
            "commit",
            "-m",
            f"Sync finagent review bundle from {source_commit_hash[:12]}",
        ],
        cwd=repo_dir,
    )


def push_review_bundle(repo_dir: Path, branch_name: str) -> None:
    _run(["git", "push", "-u", "origin", branch_name, "--force"], cwd=repo_dir)
    _run(["git", "push", "origin", f"{branch_name}:{DEFAULT_IMPORT_BRANCH}", "--force"], cwd=repo_dir)


def sync_review_repo(
    *,
    repo_dir: Path,
    branch_name: str,
    review_instructions: str,
    create: bool,
    repo_name: str,
    push: bool,
) -> dict[str, Any]:
    if create:
        ensure_repo(repo_dir)
        try:
            remotes = _run(["git", "remote"], cwd=repo_dir).stdout.split()
        except Exception:
            remotes = []
        if "origin" not in remotes:
            create_github_repo(repo_name, repo_dir)

    ensure_repo(repo_dir)
    clear_repo_contents(repo_dir)
    stats = sync_source_files(repo_dir)
    commit_hash = source_commit()
    generate_review_context(
        repo_dir,
        branch_name=branch_name,
        review_instructions=review_instructions,
        source_commit_hash=commit_hash,
    )
    commit_review_bundle(repo_dir, branch_name, commit_hash)
    if push:
        push_review_bundle(repo_dir, branch_name)
    return {
        "repo_dir": str(repo_dir),
        "repo_name": repo_name,
        "branch_name": branch_name,
        "source_commit": commit_hash,
        "files": stats["files"],
        "bytes": stats["bytes"],
        "pushed": push,
    }


def default_branch_name() -> str:
    return "review-" + dt.datetime.now().strftime("%Y%m%d-%H%M%S")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sync", action="store_true", help="Sync the curated review bundle")
    parser.add_argument("--create", action="store_true", help="Create the dedicated GitHub review repo if missing")
    parser.add_argument("--push", action="store_true", help="Push the synced review branch to origin")
    parser.add_argument("--repo-name", default=DEFAULT_REVIEW_REPO, help="Dedicated review repo name")
    parser.add_argument("--repo-dir", type=Path, default=DEFAULT_REPO_DIR, help="Local checkout path for review repo")
    parser.add_argument("--branch-name", default="", help="Review branch name")
    parser.add_argument(
        "--review-instructions",
        default=(
            "Compare finagent against OpenBB / qlib / TradingAgents for architecture leverage. "
            "Prioritize systemic borrow-vs-avoid decisions over cosmetic suggestions."
        ),
        help="Instructions embedded into REVIEW_CONTEXT.md",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.sync:
        raise SystemExit("use --sync")
    summary = sync_review_repo(
        repo_dir=args.repo_dir,
        branch_name=args.branch_name or default_branch_name(),
        review_instructions=args.review_instructions,
        create=args.create,
        repo_name=args.repo_name,
        push=args.push,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
