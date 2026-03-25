"""Tests for finagent.paths — state root resolution."""
from __future__ import annotations

from finagent.paths import resolve_paths, RuntimePaths


class TestResolvePaths:
    def test_canonical_state_root(self):
        p = resolve_paths()
        assert p.state_dir.name == "state"
        assert p.db_path.name == "finagent.sqlite"

    def test_research_paths(self):
        p = resolve_paths()
        assert p.research_db_path.name == "research.sqlite"
        assert p.research_db_path.parent == p.state_dir
        assert p.research_runs_dir.name == "runs"
        assert p.research_runs_dir.parent == p.state_dir

    def test_all_paths_under_state_dir(self):
        p = resolve_paths()
        assert str(p.db_path).startswith(str(p.state_dir))
        assert str(p.research_db_path).startswith(str(p.state_dir))
        assert str(p.cache_dir).startswith(str(p.state_dir))

    def test_custom_root(self):
        from pathlib import Path
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            p = resolve_paths(root=Path(td))
            assert p.root == Path(td)
            assert p.state_dir == Path(td) / "state"
            assert p.research_db_path == Path(td) / "state" / "research.sqlite"
