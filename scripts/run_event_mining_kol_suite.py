from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finagent.db import connect, init_db
from finagent.event_runs import record_event_mining_run
from finagent.paths import ensure_runtime_dirs, resolve_paths


KOL_FIXTURES = [
    {
        "path": "tests/fixtures/kol_bernstein_silicon_photonics.txt",
        "source_id": "src_kol_bernstein",
        "source_name": "Bernstein Silicon Photonics",
        "title": "Bernstein — Silicon Photonics",
        "language": "en",
        "jurisdiction": "global",
        "published_at": "2026-03-08",
    },
    {
        "path": "tests/fixtures/kol_semianalysis_hbm4_race.txt",
        "source_id": "src_semianalysis",
        "source_name": "SemiAnalysis",
        "title": "SemiAnalysis — HBM4 Race",
        "language": "en",
        "jurisdiction": "global",
        "published_at": "2026-03-10",
    },
    {
        "path": "tests/fixtures/kol_trendforce_dram_q1_2026.txt",
        "source_id": "src_kol_trendforce",
        "source_name": "TrendForce DRAM Tracker",
        "title": "TrendForce — DRAM Price Tracker 2026Q1",
        "language": "en",
        "jurisdiction": "global",
        "published_at": "2026-03-10",
    },
    {
        "path": "tests/fixtures/kol_guosheng_transformer.txt",
        "source_id": "src_kol_guosheng_power",
        "source_name": "国盛证券电力设备组",
        "title": "国盛证券 — 变压器深度",
        "language": "zh",
        "jurisdiction": "CN",
        "published_at": "2026-03-01",
    },
    {
        "path": "tests/fixtures/kol_spacenews_commercial_space.txt",
        "source_id": "src_kol_spacenews",
        "source_name": "SpaceNews Commercial Space",
        "title": "SpaceNews — Satellite Internet Race",
        "language": "en",
        "jurisdiction": "global",
        "published_at": "2026-03-01",
    },
]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_json(root: Path, *args: str) -> dict:
    cmd = [sys.executable, "-m", "finagent.cli", "--root", str(root), *args]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout={proc.stdout}\nstderr={proc.stderr}")
    return json.loads(proc.stdout)


def _run_cross_kol_analysis(root: Path) -> dict:
    env = dict(os.environ)
    env["FINAGENT_ROOT"] = str(root)
    proc = subprocess.run(
        [sys.executable, "scripts/cross_kol_analysis.py"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"cross_kol_analysis failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
    return json.loads(proc.stdout)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run isolated KOL ingestion + routing + cross-KOL analysis suite.")
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--suite-slug", default="kol_signal_expansion")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_root = Path(args.run_root).resolve()
    paths = resolve_paths(run_root)
    ensure_runtime_dirs(paths)
    conn = connect(paths.db_path)
    init_db(conn)

    ingested: list[dict[str, object]] = []
    for fixture in KOL_FIXTURES:
        fixture_path = (REPO_ROOT / fixture["path"]).resolve()
        result = _run_json(
            run_root,
            "intake-kol-digest",
            "--path",
            str(fixture_path),
            "--artifact-id",
            f"art_{fixture['source_id']}",
            "--title",
            str(fixture["title"]),
            "--artifact-kind",
            "text_note",
            "--source-id",
            str(fixture["source_id"]),
            "--source-type",
            "kol",
            "--source-name",
            str(fixture["source_name"]),
            "--primaryness",
            "second_hand",
            "--jurisdiction",
            str(fixture["jurisdiction"]),
            "--language",
            str(fixture["language"]),
            "--published-at",
            str(fixture["published_at"]),
            "--speaker",
            "kol",
            "--min-chars",
            "40",
        )
        ingested.append(
            {
                "fixture": fixture["path"],
                "source_id": fixture["source_id"],
                "artifact_id": result.get("artifact_id"),
                "claim_count": result.get("claim_count"),
                "route_count": result.get("route_count"),
            }
        )

    source_board = _run_json(run_root, "source-board")
    source_track = _run_json(run_root, "source-track-record", "--limit", "20")
    route_workbench = _run_json(run_root, "route-workbench", "--status", "pending", "--limit", "120")
    route_normalization = _run_json(run_root, "route-normalization-queue", "--limit", "120")
    source_viewpoint = _run_json(run_root, "source-viewpoint-workbench", "--include-existing", "--limit", "40")
    cross_kol = _run_cross_kol_analysis(run_root)

    report_dir = paths.state_dir / "kol_run_reports" / args.suite_slug
    _write_json(report_dir / "ingest_results.json", {"items": ingested})
    _write_json(report_dir / "source_board.json", source_board)
    _write_json(report_dir / "source_track_record.json", source_track)
    _write_json(report_dir / "route_workbench_pending.json", route_workbench)
    _write_json(report_dir / "route_normalization_queue.json", route_normalization)
    _write_json(report_dir / "source_viewpoint_workbench.json", source_viewpoint)
    _write_json(report_dir / "cross_kol_analysis.json", cross_kol)

    summary = {
        "ok": True,
        "suite_slug": args.suite_slug,
        "run_root": str(run_root),
        "report_dir": str(report_dir),
        "ingested_sources": len(ingested),
        "total_claims": sum(int(item.get("claim_count") or 0) for item in ingested),
        "pending_routes": int(route_workbench.get("summary", {}).get("pending_count", 0)),
        "normalization_batches": int(route_normalization.get("summary", {}).get("batch_count", 0)),
        "consensus_topics": len(cross_kol.get("consensus_topics") or []),
        "divergence_topics": len(cross_kol.get("divergence_topics") or []),
    }
    run_record = record_event_mining_run(
        conn=conn,
        engine="event_mining.kol_suite",
        run_slug=args.suite_slug,
        schema_version="3.0",
        input_refs={
            "suite_slug": args.suite_slug,
            "run_root": str(run_root),
            "report_dir": str(report_dir),
            "kind": "kol_suite",
            "fixtures": [fixture["path"] for fixture in KOL_FIXTURES],
        },
        output_ref=str(report_dir / "summary.json"),
        summary=summary,
    )
    registry_paths = resolve_paths(REPO_ROOT)
    ensure_runtime_dirs(registry_paths)
    registry_conn = connect(registry_paths.db_path)
    init_db(registry_conn)
    record_event_mining_run(
        conn=registry_conn,
        engine="event_mining.kol_suite",
        run_slug=args.suite_slug,
        schema_version="3.0",
        input_refs={
            "suite_slug": args.suite_slug,
            "run_root": str(run_root),
            "report_dir": str(report_dir),
            "kind": "kol_suite",
            "fixtures": [fixture["path"] for fixture in KOL_FIXTURES],
            "registry_scope": "repo_root",
        },
        output_ref=str(report_dir / "summary.json"),
        summary=summary,
    )
    summary["analysis_run_id"] = run_record["analysis_run_id"]
    _write_json(report_dir / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
