from __future__ import annotations

import json
from typing import Any

from .db import insert_row, list_rows
from .utils import json_dumps, stable_id, utc_now_iso


def record_event_mining_run(
    conn: Any,
    *,
    engine: str,
    run_slug: str,
    schema_version: str,
    input_refs: dict[str, Any],
    output_ref: str,
    summary: dict[str, Any],
) -> dict[str, Any]:
    analysis_run_id = stable_id("run", "::".join([engine, run_slug, output_ref]))
    conn.execute("DELETE FROM analysis_runs WHERE analysis_run_id = ?", (analysis_run_id,))
    payload = {
        **dict(input_refs),
        "run_slug": run_slug,
        "summary": dict(summary),
        "recorded_at": utc_now_iso(),
    }
    insert_row(
        conn,
        "analysis_runs",
        {
            "analysis_run_id": analysis_run_id,
            "engine": engine,
            "prompt_version": schema_version,
            "input_refs_json": json_dumps(payload),
            "output_ref": output_ref,
            "schema_valid": 1,
        },
    )
    conn.commit()
    return {"ok": True, "analysis_run_id": analysis_run_id}


def list_event_mining_runs(conn: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT analysis_run_id, engine, prompt_version, input_refs_json, output_ref, created_at
            FROM analysis_runs
            WHERE engine LIKE 'event_mining.%'
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
    ]
    items: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(row["input_refs_json"]) if row.get("input_refs_json") else {}
        summary = payload.get("summary") if isinstance(payload, dict) else {}
        items.append(
            {
                "analysis_run_id": row["analysis_run_id"],
                "engine": row["engine"],
                "schema_version": row.get("prompt_version"),
                "run_slug": payload.get("run_slug"),
                "output_ref": row.get("output_ref"),
                "created_at": row.get("created_at"),
                "summary": summary if isinstance(summary, dict) else {},
                "input_refs": payload,
            }
        )
    return items


def compare_event_mining_runs(conn: Any, run_ids: list[str]) -> dict[str, Any]:
    if not run_ids:
        return {"ok": True, "items": []}
    placeholders = ", ".join("?" for _ in run_ids)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            f"""
            SELECT analysis_run_id, engine, prompt_version, input_refs_json, output_ref, created_at
            FROM analysis_runs
            WHERE analysis_run_id IN ({placeholders})
            ORDER BY created_at DESC
            """,
            tuple(run_ids),
        )
    ]
    items: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(row["input_refs_json"]) if row.get("input_refs_json") else {}
        summary = payload.get("summary") if isinstance(payload, dict) else {}
        items.append(
            {
                "analysis_run_id": row["analysis_run_id"],
                "engine": row["engine"],
                "schema_version": row.get("prompt_version"),
                "run_slug": payload.get("run_slug"),
                "output_ref": row.get("output_ref"),
                "created_at": row.get("created_at"),
                "recommended_posture": summary.get("recommended_posture"),
                "best_expression": summary.get("best_expression"),
                "imported": summary.get("imported"),
                "stalled_emitted": summary.get("stalled_emitted"),
            }
        )
    return {"ok": True, "items": items}
