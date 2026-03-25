#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SMOKE_ROOT = REPO_ROOT / "state" / "smoke_runs" / "voice_memo_audio"
RESULT_JSON = REPO_ROOT / "docs" / "runs" / "2026-03-07_voice_memo_audio_smoke_result.json"
RESULT_MD = REPO_ROOT / "docs" / "runs" / "2026-03-07_voice_memo_audio_smoke.md"
SAMPLE_AUDIO = Path(
    "/vol1/1000/projects/storyplay/public/audio/2025年11月28日 14点27分.m4a_0000157120_0000621760.wav"
)


def run_json(args: list[str]) -> dict:
    cmd = [sys.executable, "-m", "finagent.cli", "--root", str(SMOKE_ROOT), *args]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout={proc.stdout}\nstderr={proc.stderr}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid json from {' '.join(cmd)}: {proc.stdout}") from exc


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def fetch_counts() -> dict[str, int]:
    conn = sqlite3.connect(str(SMOKE_ROOT / "state" / "finagent.sqlite"))
    conn.row_factory = sqlite3.Row
    try:
        return {
            table: conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
            for table in ("sources", "artifacts", "claims", "analysis_runs")
        }
    finally:
        conn.close()


def main() -> int:
    if SMOKE_ROOT.exists():
        shutil.rmtree(SMOKE_ROOT)
    SMOKE_ROOT.mkdir(parents=True, exist_ok=True)

    init = run_json(["init"])
    intake = run_json(
        [
            "intake-voice-memo-audio",
            "--audio-path",
            str(SAMPLE_AUDIO),
            "--title",
            "Voice memo audio smoke sample",
            "--device",
            "cpu",
            "--cleanup-remote",
            "--speaker",
            "user_memo",
            "--min-chars",
            "10",
        ]
    )
    board = run_json(["board"])
    counts = fetch_counts()

    assert_true(init["ok"] is True, "init should succeed")
    assert_true(intake["ok"] is True, "intake should succeed")
    assert_true(intake["char_count"] > 0, "transcript should not be empty")
    assert_true(intake["claim_count"] >= 1, "voice memo intake should produce at least one claim")
    assert_true(counts["sources"] >= 1, "should persist at least one source")
    assert_true(counts["artifacts"] >= 1, "should persist at least one artifact")
    assert_true(counts["claims"] >= 1, "should persist at least one claim")
    assert_true(counts["analysis_runs"] >= 2, "should record transcription + extraction analysis runs")

    payload = {
        "ok": True,
        "sample_audio": str(SAMPLE_AUDIO),
        "init": init,
        "intake": intake,
        "board": board,
        "counts": counts,
    }
    RESULT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    RESULT_MD.write_text(
        "\n".join(
            [
                "# 2026-03-07 Voice Memo Audio Smoke",
                "",
                "## Result",
                "",
                f"- sample_audio: `{SAMPLE_AUDIO}`",
                f"- artifact_id: `{intake['artifact_id']}`",
                f"- source_id: `{intake['source_id']}`",
                f"- char_count: `{intake['char_count']}`",
                f"- claim_count: `{intake['claim_count']}`",
                "",
                "preview:",
                "",
                f"> {intake['preview']}",
                "",
                "counts:",
                "",
                f"- sources: `{counts['sources']}`",
                f"- artifacts: `{counts['artifacts']}`",
                f"- claims: `{counts['claims']}`",
                f"- analysis_runs: `{counts['analysis_runs']}`",
            ]
        ),
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
