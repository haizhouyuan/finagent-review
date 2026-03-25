from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RuntimePaths:
    root: Path
    state_dir: Path
    imports_dir: Path
    raw_dir: Path
    text_dir: Path
    cache_dir: Path
    db_path: Path
    # v2 research paths
    research_db_path: Path         # state/research.sqlite
    research_runs_dir: Path        # state/runs/


def resolve_paths(root: Path | None = None) -> RuntimePaths:
    repo_root = (root or Path(__file__).resolve().parents[1]).resolve()
    state_dir = repo_root / "state"
    imports_dir = repo_root / "imports"
    artifacts_dir = state_dir / "artifacts"
    raw_dir = artifacts_dir / "raw"
    text_dir = artifacts_dir / "text"
    cache_dir = state_dir / "cache"
    db_path = state_dir / "finagent.sqlite"
    research_db_path = state_dir / "research.sqlite"
    research_runs_dir = state_dir / "runs"
    return RuntimePaths(
        root=repo_root,
        state_dir=state_dir,
        imports_dir=imports_dir,
        raw_dir=raw_dir,
        text_dir=text_dir,
        cache_dir=cache_dir,
        db_path=db_path,
        research_db_path=research_db_path,
        research_runs_dir=research_runs_dir,
    )


def ensure_runtime_dirs(paths: RuntimePaths) -> None:
    for path in (
        paths.state_dir,
        paths.imports_dir,
        paths.raw_dir,
        paths.text_dir,
        paths.cache_dir,
        paths.research_runs_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
