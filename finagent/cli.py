from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__
from .adapters import (
    AdapterError,
    FetchedArtifact,
    fetch_cninfo_announcements,
    fetch_defillama_protocol,
    fetch_openalex_search,
    fetch_sec_submissions,
    transcribe_audio_with_homepc_funasr,
)
from .db import (
    artifact_text,
    connect,
    init_db,
    insert_artifact,
    insert_claim,
    insert_event,
    insert_row,
    list_rows,
    select_one,
    upsert_source,
)
from .paths import ensure_runtime_dirs, resolve_paths
from .event_runs import compare_event_mining_runs, list_event_mining_runs
from .event_replay import ReplayInputs, replay_theme_run, validate_theme_replay
from .sector_grammars import list_sector_grammars
from .sentinel import (
    FEEDBACK_TYPES,
    FEEDBACK_VERDICTS,
    build_spec_prompt_context,
    build_extraction_prompt,
    classify_event,
    default_spec_path,
    emit_stalled_events,
    import_events,
    load_sentinel_spec,
    normalize_event,
    record_anti_thesis_result,
    record_feedback,
    route_event,
    sync_sentinel_spec,
    validate_event,
    validate_fixtures,
    validate_sentinel_spec,
)
from .source_adapters import execute_refresh_spec, infer_refresh_spec_from_artifact, list_source_adapters
from .source_policy import list_source_policies
from .contracts.freshness import freshness_status_for_date
from .utils import (
    domain_check_claim,
    infer_claim_confidence,
    infer_claim_type,
    json_dumps,
    make_id,
    sha256_text,
    slugify,
    stable_id,
    split_sentences,
    utc_now_iso,
)
from .views import (
    build_anti_thesis_board,
    build_integration_snapshot,
    build_decision_maintenance_queue,
    build_decision_journal,
    build_decision_dashboard,
    build_event_ledger,
    build_event_evaluation_board,
    build_intake_inbox,
    build_opportunity_inbox,
    build_playbook_board,
    build_pattern_library,
    build_promotion_wizard,
    build_route_normalization_queue,
    build_route_workbench,
    build_review_board,
    build_review_remediation_queue,
    build_source_board,
    build_source_feedback_workbench,
    build_verification_remediation_batches,
    build_verification_remediation_queue,
    build_source_remediation_queue,
    build_source_revisit_workbench,
    build_source_track_record,
    build_source_viewpoint_workbench,
    build_sentinel_board,
    build_target_case_dashboard,
    build_theme_radar_board,
    build_thesis_focus,
    build_thesis_gate_report,
    build_theme_map,
    build_thesis_board,
    build_today_cockpit,
    build_validation_board,
    build_voice_memo_triage,
    build_watch_board,
    build_weekly_decision_note,
)


SOURCE_TYPES = [
    "official_disclosure",
    "exchange",
    "paper",
    "dashboard",
    "kol",
    "personal",
    "governance",
    "news",
    "other",
]
PRIMARYNESS_CHOICES = ["first_hand", "second_hand", "personal"]
ARTIFACT_KINDS = [
    "text_note",
    "audio_transcript",
    "video_transcript",
    "video_digest",
    "pdf",
    "html",
    "json",
    "dashboard_snapshot",
    "paper_metadata",
    "other",
]
ENTITY_TYPES = ["company", "protocol", "token", "chain", "person", "product", "technology", "policy_body", "index", "etf", "other"]
THEME_IMPORTANCE_CHOICES = ["scouting", "tracking", "priority", "cooling", "archived"]
THESIS_STATUS_CHOICES = ["seed", "framed", "evidence_backed", "active", "paused", "invalidated", "expired", "archived"]
TARGET_ASSET_CLASSES = ["a_share_equity", "us_equity", "token", "etf", "basket", "other"]
EXPOSURE_TYPE_CHOICES = ["direct", "enabler", "proxy", "hedge", "basket_member"]
TARGET_CASE_STATUS_CHOICES = ["candidate", "covered", "actionable", "active", "closed"]
DESIRED_POSTURE_CHOICES = ["observe", "prepare", "starter", "add_on_confirmation", "avoid", "exit_watch"]
MONITOR_TYPE_CHOICES = ["official", "market", "onchain", "narrative", "manual_checklist", "claim_freshness"]
MONITOR_COMPARATOR_CHOICES = ["gte", "lte"]
CLAIM_REVIEW_STATUS_CHOICES = ["unreviewed", "reviewed", "confirmed", "refuted", "needs_correction"]
REVIEW_RESULT_CHOICES = [
    "right_logic_right_timing",
    "right_logic_wrong_timing",
    "right_theme_wrong_target",
    "wrong_logic",
    "thesis_changed",
    "unresolved",
]
REVIEW_OWNER_OBJECT_TYPES = ["thesis", "target_case", "theme", "other"]
DECISION_ACTION_CHOICES = ["observe", "prepare", "starter", "add", "trim", "exit"]
DECISION_STATUS_CHOICES = ["active", "superseded", "archived"]
ROUTE_STATUS_CHOICES = ["pending", "accepted", "rejected", "superseded"]
ROUTE_TYPE_CHOICES = ["thesis_seed", "thesis_input", "corroboration_needed", "counter_search", "monitor_candidate"]
ROUTE_LINK_OBJECT_TYPES = ["theme", "thesis", "thesis_version", "target_case", "artifact", "claim", "monitor"]
ROUTE_LINK_KIND_CHOICES = ["feeds", "corroborated_by", "contradicted_by", "maps_to", "opens"]
VALIDATION_VERDICT_CHOICES = ["validated", "contradicted", "partial", "needs_followup"]
VIEWPOINT_STANCE_CHOICES = ["bullish", "cautious_bullish", "neutral", "cautious", "bearish", "mixed"]
VIEWPOINT_STATUS_CHOICES = ["open", "partially_validated", "validated", "contradicted", "expired"]
SOURCE_FEEDBACK_TYPE_TO_WEIGHT = {
    "high_signal": 3,
    "useful_context": 1,
    "noise": -1,
    "misleading": -3,
}


def _repo_paths(root: str | None) -> tuple[Path, Any]:
    paths = resolve_paths(Path(root).resolve() if root else None)
    ensure_runtime_dirs(paths)
    return paths.root, paths


def _write_artifact_files(paths: Any, artifact_id: str, raw_text: str, normalized_text: str, raw_suffix: str = "txt") -> tuple[Path, Path]:
    raw_path = paths.raw_dir / f"{artifact_id}.{raw_suffix}"
    text_path = paths.text_dir / f"{artifact_id}.txt"
    raw_path.write_text(raw_text, encoding="utf-8")
    text_path.write_text(normalized_text, encoding="utf-8")
    return raw_path, text_path


def _ingest_text_artifact(
    conn: Any,
    paths: Any,
    *,
    source_id: str,
    source_type: str,
    source_name: str,
    primaryness: str,
    input_path: Path,
    artifact_id: str,
    artifact_kind: str,
    title: str,
    published_at: str,
    language: str,
    uri: str,
    jurisdiction: str,
    base_uri: str,
) -> dict[str, Any]:
    content = input_path.read_text(encoding="utf-8")
    _ensure_source(
        conn,
        source_id,
        source_type,
        source_name,
        primaryness,
        jurisdiction,
        language,
        base_uri,
    )
    _delete_route_state_for_artifact(conn, artifact_id)
    conn.execute("DELETE FROM claims WHERE artifact_id = ?", (artifact_id,))
    conn.execute("DELETE FROM artifact_fts WHERE artifact_id = ?", (artifact_id,))
    conn.execute("DELETE FROM artifacts WHERE artifact_id = ?", (artifact_id,))
    conn.execute("DELETE FROM analysis_runs WHERE output_ref = ?", (artifact_id,))
    raw_path, text_path = _write_artifact_files(paths, artifact_id, content, content)
    insert_artifact(
        conn,
        {
            "artifact_id": artifact_id,
            "source_id": source_id,
            "artifact_kind": artifact_kind,
            "title": title,
            "captured_at": utc_now_iso(),
            "published_at": published_at,
            "language": language,
            "uri": uri,
            "raw_path": str(raw_path),
            "normalized_text_path": str(text_path),
            "content_hash": sha256_text(content),
            "status": "captured",
            "metadata_json": json_dumps({"input_path": str(input_path)}),
        },
        content=content,
    )
    insert_event(conn, make_id("evt"), "artifact", artifact_id, "artifact_ingested", {"path": str(input_path)})
    return {"artifact_id": artifact_id, "content": content}


def cmd_init(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps({"ok": True, "db_path": str(paths.db_path)}))
    return 0


def cmd_create_source(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    source_id = args.source_id or make_id("src", args.name)
    upsert_source(
        conn,
        {
            "source_id": source_id,
            "source_type": args.source_type,
            "name": args.name,
            "primaryness": args.primaryness,
            "jurisdiction": args.jurisdiction,
            "language": args.language,
            "base_uri": args.base_uri,
            "credibility_policy": args.credibility_policy,
        },
    )
    insert_event(conn, make_id("evt"), "source", source_id, "source_upserted", {"name": args.name})
    conn.commit()
    print(json_dumps({"ok": True, "source_id": source_id}))
    return 0


def _ensure_source(
    conn: Any,
    source_id: str,
    source_type: str,
    name: str,
    primaryness: str,
    jurisdiction: str | None,
    language: str | None,
    base_uri: str | None,
) -> None:
    existing = select_one(conn, "SELECT credibility_policy FROM sources WHERE source_id = ?", (source_id,))
    upsert_source(
        conn,
        {
            "source_id": source_id,
            "source_type": source_type,
            "name": name,
            "primaryness": primaryness,
            "jurisdiction": jurisdiction,
            "language": language,
            "base_uri": base_uri,
            "credibility_policy": existing["credibility_policy"] if existing else "",
        },
    )


def _json_load_list(raw: str | None) -> list[Any]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return value if isinstance(value, list) else []


def _json_load_dict(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _load_json_payload(path: str) -> Any:
    if path == "-":
        raw = sys.stdin.read()
    else:
        raw = Path(path).read_text(encoding="utf-8")
    return json.loads(raw)


def _load_event_batch(path: str) -> list[dict[str, Any]]:
    payload = _load_json_payload(path)
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list) and all(isinstance(item, dict) for item in payload):
        return payload
    raise AdapterError("event payload must be a JSON object or an array of JSON objects")


def _resolve_io_path(root: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def _append_unique_items(raw: str | None, values: list[str]) -> str:
    merged = list(_json_load_list(raw))
    for value in values:
        if value and value not in merged:
            merged.append(value)
    return json_dumps(merged)


def _delete_route_state_for_artifact(conn: Any, artifact_id: str) -> None:
    conn.execute(
        "DELETE FROM claim_route_links WHERE route_id IN (SELECT route_id FROM claim_routes WHERE artifact_id = ?)",
        (artifact_id,),
    )
    conn.execute("DELETE FROM claim_routes WHERE artifact_id = ?", (artifact_id,))


def _resolve_thesis_version_id(conn: Any, object_type: str, object_id: str) -> str | None:
    if object_type == "thesis_version":
        row = select_one(conn, "SELECT thesis_version_id FROM thesis_versions WHERE thesis_version_id = ?", (object_id,))
        return row["thesis_version_id"] if row else None
    if object_type == "thesis":
        row = select_one(conn, "SELECT current_version_id FROM theses WHERE thesis_id = ?", (object_id,))
        return row["current_version_id"] if row else None
    return None


def _get_thesis_and_version(conn: Any, thesis_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    thesis = select_one(conn, "SELECT * FROM theses WHERE thesis_id = ?", (thesis_id,))
    if thesis is None:
        raise AdapterError(f"thesis not found: {thesis_id}")
    version = select_one(conn, "SELECT * FROM thesis_versions WHERE thesis_version_id = ?", (thesis["current_version_id"],))
    if version is None:
        raise AdapterError(f"thesis version not found: {thesis['current_version_id']}")
    return dict(thesis), dict(version)


def _append_artifacts_to_thesis_version(conn: Any, thesis_version_id: str, artifact_ids: list[str]) -> list[str]:
    if not artifact_ids:
        return []
    for artifact_id in artifact_ids:
        artifact = select_one(conn, "SELECT artifact_id FROM artifacts WHERE artifact_id = ?", (artifact_id,))
        if artifact is None:
            raise AdapterError(f"artifact not found: {artifact_id}")
    version = select_one(conn, "SELECT created_from_artifacts_json FROM thesis_versions WHERE thesis_version_id = ?", (thesis_version_id,))
    existing = _json_load_list(version["created_from_artifacts_json"] if version else "[]")
    merged = list(existing)
    appended: list[str] = []
    for artifact_id in artifact_ids:
        if artifact_id not in merged:
            merged.append(artifact_id)
            appended.append(artifact_id)
    conn.execute(
        "UPDATE thesis_versions SET created_from_artifacts_json = ? WHERE thesis_version_id = ?",
        (json_dumps(merged), thesis_version_id),
    )
    return appended


def _append_text_field(current: str | None, addition: str) -> str:
    base = (current or "").strip()
    extra = addition.strip()
    if not extra:
        return base
    if not base:
        return extra
    if extra in base:
        return base
    return f"{base}\n- {extra}"


def _thesis_has_seed_semantics(thesis: dict[str, Any], version: dict[str, Any]) -> bool:
    title = str(thesis.get("title") or "").lower()
    statement = str(version.get("statement") or "").lower()
    why_now = str(version.get("why_now") or "").lower()
    markers = (
        "seed",
        "还不能直接提升为 active thesis",
        "还不能直接提升为active thesis",
        "值得归档",
        "后续核验",
        "待补",
        "k o l 提供线索",
        "kol 提供线索",
    )
    return any(marker in title or marker in statement or marker in why_now for marker in markers)


def _clone_thesis_version(
    conn: Any,
    *,
    thesis: dict[str, Any],
    version: dict[str, Any],
    new_version_id: str,
    overrides: dict[str, Any],
) -> None:
    payload = dict(version)
    payload["thesis_version_id"] = new_version_id
    for key, value in overrides.items():
        payload[key] = value
    insert_row(conn, "thesis_versions", payload)


def _rebind_monitors_for_artifact_refresh(conn: Any, previous_artifact_id: str, refreshed_artifact_id: str) -> list[str]:
    if not previous_artifact_id or previous_artifact_id == refreshed_artifact_id:
        return []
    rebound_ids: list[str] = []
    rows = list_rows(conn, "SELECT monitor_id, query_or_rule FROM monitors")
    for row in rows:
        rule = _json_load_dict(row["query_or_rule"])
        if rule.get("kind") != "artifact_metric":
            continue
        if rule.get("artifact_id") != previous_artifact_id:
            continue
        rule["artifact_id"] = refreshed_artifact_id
        conn.execute("UPDATE monitors SET query_or_rule = ? WHERE monitor_id = ?", (json_dumps(rule), row["monitor_id"]))
        insert_event(
            conn,
            make_id("evt"),
            "monitor",
            row["monitor_id"],
            "monitor_rebound_to_refreshed_artifact",
            {"from_artifact_id": previous_artifact_id, "to_artifact_id": refreshed_artifact_id},
        )
        rebound_ids.append(row["monitor_id"])
    return rebound_ids


def cmd_ingest_text(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    input_path = Path(args.path).resolve()
    artifact_id = args.artifact_id or make_id("art", input_path.stem)
    _ingest_text_artifact(
        conn,
        paths,
        source_id=args.source_id,
        source_type=args.source_type,
        source_name=args.source_name,
        primaryness=args.primaryness,
        input_path=input_path,
        artifact_id=artifact_id,
        artifact_kind=args.artifact_kind,
        title=args.title or input_path.stem,
        published_at=args.published_at,
        language=args.language,
        uri=args.uri,
        jurisdiction=args.jurisdiction,
        base_uri=args.base_uri,
    )
    conn.commit()
    print(json_dumps({"ok": True, "artifact_id": artifact_id}))
    return 0


def _ingest_fetched(
    conn: Any,
    paths: Any,
    source_id: str,
    source_type: str,
    source_name: str,
    primaryness: str,
    jurisdiction: str | None,
    language: str | None,
    artifact_kind: str,
    fetched: FetchedArtifact,
    artifact_label: str,
    raw_suffix: str = "json",
) -> str:
    artifact_id = make_id("art", artifact_label)
    _ensure_source(
        conn,
        source_id,
        source_type,
        source_name,
        primaryness,
        jurisdiction,
        language,
        fetched.uri,
    )
    raw_path, text_path = _write_artifact_files(paths, artifact_id, fetched.raw_text, fetched.normalized_text, raw_suffix=raw_suffix)
    insert_artifact(
        conn,
        {
            "artifact_id": artifact_id,
            "source_id": source_id,
            "artifact_kind": artifact_kind,
            "title": fetched.title,
            "captured_at": utc_now_iso(),
            "published_at": fetched.published_at,
            "language": language,
            "uri": fetched.uri,
            "raw_path": str(raw_path),
            "normalized_text_path": str(text_path),
            "content_hash": sha256_text(fetched.raw_text),
            "status": "captured",
            "metadata_json": json_dumps(fetched.metadata),
        },
        content=fetched.normalized_text,
    )
    insert_event(conn, make_id("evt"), "artifact", artifact_id, "artifact_ingested", {"uri": fetched.uri})
    return artifact_id


def cmd_fetch_sec_submissions(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    try:
        fetched = fetch_sec_submissions(args.ticker)
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    artifact_id = _ingest_fetched(
        conn,
        paths,
        source_id="src_sec_edgar",
        source_type="official_disclosure",
        source_name="SEC EDGAR",
        primaryness="first_hand",
        jurisdiction="US",
        language="en",
        artifact_kind="json",
        fetched=fetched,
        artifact_label=f"sec_{args.ticker}",
    )
    conn.commit()
    print(json_dumps({"ok": True, "artifact_id": artifact_id, "uri": fetched.uri}))
    return 0


def cmd_fetch_openalex(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    fetched = fetch_openalex_search(args.query, per_page=args.per_page)
    artifact_id = _ingest_fetched(
        conn,
        paths,
        source_id="src_openalex",
        source_type="paper",
        source_name="OpenAlex",
        primaryness="first_hand",
        jurisdiction="global",
        language="en",
        artifact_kind="paper_metadata",
        fetched=fetched,
        artifact_label=f"openalex_{slugify(args.query)}",
    )
    conn.commit()
    print(json_dumps({"ok": True, "artifact_id": artifact_id, "uri": fetched.uri}))
    return 0


def cmd_fetch_defillama(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    fetched = fetch_defillama_protocol(args.slug)
    artifact_id = _ingest_fetched(
        conn,
        paths,
        source_id="src_defillama",
        source_type="dashboard",
        source_name="DefiLlama",
        primaryness="second_hand",
        jurisdiction="global",
        language="en",
        artifact_kind="dashboard_snapshot",
        fetched=fetched,
        artifact_label=f"defillama_{args.slug}",
    )
    conn.commit()
    print(json_dumps({"ok": True, "artifact_id": artifact_id, "uri": fetched.uri}))
    return 0


def cmd_market_snapshot(args: argparse.Namespace) -> int:
    from .market_data import fetch_market_snapshot

    ticker = str(getattr(args, "ticker", "")).strip()
    market = str(getattr(args, "market", "CN")).strip().upper()
    if not ticker:
        print(json_dumps({"ok": False, "error": "ticker is required"}))
        return 1
    try:
        snap = fetch_market_snapshot(ticker, market=market)
        print(json_dumps({"ok": True, **snap}))
    except Exception as exc:
        print(json_dumps({"ok": False, "error": str(exc), "ticker": ticker, "market": market}))
        return 1
    return 0


def cmd_market_screen(args: argparse.Namespace) -> int:
    from .market_screener import cli_market_screen

    cli_market_screen(args)
    return 0


def cmd_consensus(args: argparse.Namespace) -> int:
    from .consensus import fetch_consensus_estimates

    ticker = str(getattr(args, "ticker", "")).strip()
    market = str(getattr(args, "market", "CN")).strip().upper()
    if not ticker:
        print(json_dumps({"ok": False, "error": "ticker is required"}))
        return 1
    try:
        result = fetch_consensus_estimates(ticker, market=market)
        print(json_dumps({"ok": True, **(result or {})}))
    except Exception as exc:
        print(json_dumps({"ok": False, "error": str(exc), "ticker": ticker, "market": market}))
        return 1
    return 0


def cmd_writeback(args: argparse.Namespace) -> int:
    from .writeback import ingest_claim_outcome, ingest_source_feedback, ingest_expression_outcome

    wb_type = str(getattr(args, "type", "")).strip()
    payload_str = str(getattr(args, "payload", "{}")).strip()
    wb_dir = Path(getattr(args, "writeback_dir", "")) if getattr(args, "writeback_dir", "") else None

    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError as exc:
        print(json_dumps({"ok": False, "error": f"Invalid JSON payload: {exc}"}))
        return 1

    dispatchers = {
        "claim_outcome": ingest_claim_outcome,
        "source_feedback": ingest_source_feedback,
        "expression_outcome": ingest_expression_outcome,
    }
    handler = dispatchers.get(wb_type)
    if handler is None:
        print(json_dumps({"ok": False, "error": f"Unknown writeback type: {wb_type}. Supported: {sorted(dispatchers)}"}))
        return 1

    kwargs = {"writeback_dir": wb_dir} if wb_dir else {}
    result = handler(payload, **kwargs)
    print(json_dumps(result))
    return 0


def cmd_fetch_cninfo_announcements(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    try:
        fetched = fetch_cninfo_announcements(
            args.ticker,
            search_key=args.search_key or None,
            lookback_days=args.lookback_days,
            limit=args.limit,
        )
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    artifact_id = _ingest_fetched(
        conn,
        paths,
        source_id="src_cninfo",
        source_type="official_disclosure",
        source_name="CNINFO",
        primaryness="first_hand",
        jurisdiction="CN",
        language="zh",
        artifact_kind="json",
        fetched=fetched,
        artifact_label=f"cninfo_{slugify(args.ticker)}",
    )
    conn.commit()
    print(json_dumps({"ok": True, "artifact_id": artifact_id, "uri": fetched.uri}))
    return 0


def _build_daily_refresh_plan(conn: Any, limit: int | None = None) -> dict[str, Any]:
    artifact_rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT a.artifact_id, a.source_id, a.title, a.uri, a.metadata_json, a.captured_at,
                   s.name AS source_name, s.source_type, s.primaryness, s.jurisdiction, s.language
            FROM artifacts a
            JOIN sources s ON s.source_id = a.source_id
            ORDER BY a.captured_at DESC, a.created_at DESC
            """,
        )
    ]
    refresh_specs: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    refreshable_source_ids: set[str] = set()
    for row in artifact_rows:
        spec = infer_refresh_spec_from_artifact(row)
        if spec is None:
            continue
        key = (spec["source_id"], spec["kind"], spec["refresh_key"])
        if key in seen:
            continue
        seen.add(key)
        refreshable_source_ids.add(spec["source_id"])
        refresh_specs.append(spec)
        if limit is not None and len(refresh_specs) >= limit:
            break
    skipped_sources = []
    for row in list_rows(
        conn,
        """
        SELECT s.source_id, s.name, s.source_type, s.primaryness
        FROM sources s
        ORDER BY s.source_id
        """,
    ):
        source_id = row["source_id"]
        if source_id in refreshable_source_ids:
            continue
        skipped_sources.append(
            {
                "source_id": source_id,
                "source_name": row["name"],
                "source_type": row["source_type"],
                "primaryness": row["primaryness"],
                "reason": "no supported refresh adapter or tracked artifact context",
            }
        )
    return {
        "refresh_specs": refresh_specs,
        "skipped_sources": skipped_sources,
    }


def _daily_refresh_artifact_label(base_label: str) -> str:
    ts = utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "z").replace("T", "_")
    return f"{base_label}_{ts}"


def _execute_refresh_spec(conn: Any, paths: Any, spec: dict[str, Any]) -> dict[str, Any]:
    fetched = execute_refresh_spec(spec)
    artifact_id = _ingest_fetched(
        conn,
        paths,
        source_id=spec["source_id"],
        source_type=spec["source_type"],
        source_name=spec["source_name"],
        primaryness=spec["primaryness"],
        jurisdiction=spec["jurisdiction"],
        language=spec["language"],
        artifact_kind=spec["artifact_kind"],
        fetched=fetched,
        artifact_label=_daily_refresh_artifact_label(spec["artifact_label"]),
    )
    rebound_monitor_ids = _rebind_monitors_for_artifact_refresh(conn, spec.get("artifact_id", ""), artifact_id)
    conn.commit()
    return {
        "status": "success",
        "source_id": spec["source_id"],
        "source_name": spec["source_name"],
        "kind": spec["kind"],
        "refresh_key": spec["refresh_key"],
        "artifact_id": artifact_id,
        "previous_artifact_id": spec.get("artifact_id", ""),
        "rebound_monitor_ids": rebound_monitor_ids,
        "uri": fetched.uri,
        "title": fetched.title,
    }


def _run_monitors_with_conn(conn: Any) -> dict[str, Any]:
    rows = list_rows(conn, "SELECT * FROM monitors WHERE status IN ('live', 'alerted')")
    results = []
    for row in rows:
        rule = json.loads(row["query_or_rule"] or "{}")
        value = None
        if rule.get("kind") == "artifact_metric":
            value = _load_artifact_metric(conn, rule.get("artifact_id", ""), rule.get("metric_name", ""))
        elif rule.get("kind") == "claim_freshness":
            value = _load_claim_freshness_metric(
                conn,
                rule.get("thesis_id", ""),
                int(rule.get("threshold_days") or row["threshold_value"] or 180),
            )
        comparator = row["comparator"]
        threshold = row["threshold_value"]
        outcome = "no_data"
        new_status = row["status"]
        if value is not None and comparator and threshold is not None:
            if comparator == "gte":
                matched = value >= float(threshold)
            elif comparator == "lte":
                matched = value <= float(threshold)
            else:
                matched = False
            outcome = "threshold_met" if matched else "threshold_not_met"
            new_status = "alerted" if matched else "live"
        conn.execute(
            "UPDATE monitors SET latest_value = ?, status = ?, last_checked_at = ? WHERE monitor_id = ?",
            (value, new_status, utc_now_iso(), row["monitor_id"]),
        )
        insert_row(
            conn,
            "monitor_events",
            {
                "monitor_event_id": make_id("mevt", row["monitor_id"]),
                "monitor_id": row["monitor_id"],
                "observed_value": value,
                "outcome": outcome,
                "detail_json": json_dumps(rule),
            },
        )
        results.append(
            {
                "monitor_id": row["monitor_id"],
                "metric_name": row["metric_name"],
                "value": value,
                "threshold": threshold,
                "status": new_status,
                "outcome": outcome,
            }
        )
    conn.commit()
    return {"ok": True, "monitor_count": len(results), "results": results}


def _should_skip_claim_sentence(sentence: str, artifact_kind: str) -> bool:
    text = sentence.strip()
    lowered = text.lower()
    if not text:
        return True
    if text in {"```", "---"}:
        return True
    if text.startswith("#"):
        return True
    if text.startswith("|") and text.count("|") >= 2:
        return True
    if re.fullmatch(r"[-:| ]{3,}", text):
        return True
    if re.match(r'^"[A-Za-z0-9_]+":', text):
        return True
    if re.match(r"^[A-Za-z0-9_]+:\s+", text) and any(
        key in text for key in ["timecode", "source_path", "published_at", "digest_stem", "artifact_id", "source", "notes"]
    ):
        return True
    if artifact_kind == "video_digest":
        if text.startswith(("- [", "[", "http://", "https://")):
            return True
        if "bilibili_BV" in text or "BV1" in text and "http" in text:
            return True
        if text.startswith(("source_path:", "published_at:", "digest_stem:")):
            return True
        if any(token in lowered for token in ["topic_id:", "analysis_id:", "schema_version", "signal_candidates", "evidence_refs"]):
            return True
        if any(token in lowered for token in [".jpg", ".png", "verify_status", "window_start", "window_end", "entity_key", "signal_type"]):
            return True
        if "|" in text and text.count("|") >= 2:
            return True
        if "如疑似误识别" in text or "待人工复核" in text:
            return True
    return False


def _clean_video_digest_line(item: str) -> str:
    cleaned = item.strip()
    cleaned = re.sub(r"^\[[^\]]+\]\s*", "", cleaned)
    cleaned = re.sub(r"^\d{2}:\d{2}:\d{2}\.\d{3}(?:-\d{2}:\d{2}:\d{2}\.\d{3})?\s*", "", cleaned)
    cleaned = re.sub(r"^\d{6}\.(?:jpg|png)\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"https?://\S+", "", cleaned)
    cleaned = re.sub(r"`+", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" |-")


def _claim_input_text(text: str, artifact_kind: str) -> str:
    if artifact_kind != "video_digest":
        return text
    cleaned = re.sub(r"^---\n.*?\n---\n", "", text, count=1, flags=re.S)
    cleaned = re.sub(r"<!--.*?-->", "", cleaned, flags=re.S)
    allowed_sections = {"核心观点", "关键证据/数据点（可引用来源）", "反驳点/局限性"}
    current_section = ""
    selected_lines: list[str] = []
    for raw_line in cleaned.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("## "):
            current_section = line[3:].strip()
            continue
        if current_section not in allowed_sections:
            continue
        if not line.startswith("- "):
            continue
        item = line[2:].strip()
        item = _clean_video_digest_line(item)
        if not item or any(marker in item for marker in ("待补齐", "待整理", "待人工复核")):
            continue
        selected_lines.append(item)
    return "\n".join(selected_lines) if selected_lines else cleaned


def _claim_fingerprint(text: str) -> str:
    normalized = text.lower()
    normalized = re.sub(r"https?://\S+", "", normalized)
    normalized = re.sub(r"\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:-\d{2}:\d{2}:\d{2}(?:\.\d+)?)?", "", normalized)
    normalized = re.sub(r"\b\d{6}\.(?:jpg|png)\b", "", normalized)
    normalized = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", normalized)
    return normalized


def _extract_claims_for_artifact(conn: Any, artifact_id: str, speaker: str, min_chars: int) -> list[dict[str, Any]]:
    artifact = select_one(
        conn,
        """
        SELECT a.artifact_id, a.title, a.status, a.artifact_kind, s.primaryness,
               COALESCE(a.published_at, a.captured_at, '') AS claim_date
        FROM artifacts a
        JOIN sources s ON s.source_id = a.source_id
        WHERE a.artifact_id = ?
        """,
        (artifact_id,),
    )
    if artifact is None:
        raise AdapterError(f"artifact not found: {artifact_id}")
    text = artifact_text(conn, artifact_id)
    if not text:
        raise AdapterError("artifact has no indexed text")
    text = _claim_input_text(text, artifact["artifact_kind"])
    _delete_route_state_for_artifact(conn, artifact_id)
    conn.execute("DELETE FROM claims WHERE artifact_id = ?", (artifact_id,))
    conn.execute("DELETE FROM analysis_runs WHERE analysis_run_id = ?", (stable_id("run", f"extract_{artifact_id}"),))
    claims = []
    seen_fingerprints: set[str] = set()
    for idx, sentence in enumerate(split_sentences(text)):
        sentence = re.sub(r"\s+", " ", sentence).strip()
        if len(sentence) < min_chars:
            continue
        if _should_skip_claim_sentence(sentence, artifact["artifact_kind"]):
            continue
        fingerprint = _claim_fingerprint(sentence)
        if len(fingerprint) >= 12:
            if fingerprint in seen_fingerprints:
                continue
            seen_fingerprints.add(fingerprint)
        claim_id = stable_id("clm", f"{artifact_id}_{idx}")
        claim_type = infer_claim_type(sentence)
        confidence = infer_claim_confidence(artifact["primaryness"], claim_type)
        domain_result = domain_check_claim(sentence, claim_date=artifact["claim_date"])
        claim_status = "quarantined" if domain_result["quarantine"] else "candidate"
        insert_claim(
            conn,
            {
                "claim_id": claim_id,
                "artifact_id": artifact_id,
                "speaker": speaker,
                "timecode_or_span": str(idx),
                "claim_text": sentence,
                "claim_type": claim_type,
                "confidence": confidence,
                "linked_entity_ids_json": json_dumps([]),
                "data_date": domain_result["data_date"],
                "review_status": "unreviewed",
                "review_metadata_json": json_dumps({}),
                "domain_check_json": json_dumps(domain_result),
                "freshness_status": domain_result["freshness_status"],
                "status": claim_status,
            },
        )
        claims.append(
            {
                "claim_id": claim_id,
                "claim_type": claim_type,
                "confidence": confidence,
                "status": claim_status,
                "data_date": domain_result["data_date"],
                "freshness_status": domain_result["freshness_status"],
                "warning_count": len(domain_result["warnings"]),
                "text": sentence,
            }
        )
    conn.execute("UPDATE artifacts SET status = ? WHERE artifact_id = ?", ("extracted", artifact_id))
    insert_event(conn, make_id("evt"), "artifact", artifact_id, "claims_extracted", {"claim_count": len(claims)})
    insert_row(
        conn,
        "analysis_runs",
        {
            "analysis_run_id": stable_id("run", f"extract_{artifact_id}"),
            "engine": "rule_based_claim_extractor_v1",
            "input_refs_json": json_dumps([artifact_id]),
            "output_ref": artifact_id,
            "schema_valid": 1,
        },
    )
    return claims


def _ingest_homepc_transcription_artifact(conn: Any, paths: Any, args: argparse.Namespace, *, artifact_id: str, source_id: str) -> dict[str, Any]:
    result = transcribe_audio_with_homepc_funasr(
        args.audio_path,
        host=args.host,
        env_name=args.env_name,
        modelscope_cache=args.modelscope_cache,
        remote_root_base=args.remote_root_base,
        device=args.device,
        timeout_seconds=args.timeout_seconds,
        cleanup_remote=args.cleanup_remote,
    )
    _delete_route_state_for_artifact(conn, artifact_id)
    conn.execute("DELETE FROM claims WHERE artifact_id = ?", (artifact_id,))
    conn.execute("DELETE FROM artifact_fts WHERE artifact_id = ?", (artifact_id,))
    conn.execute("DELETE FROM artifacts WHERE artifact_id = ?", (artifact_id,))
    conn.execute("DELETE FROM analysis_runs WHERE analysis_run_id = ?", (stable_id("run", f"homepc_funasr_{artifact_id}"),))
    _ensure_source(
        conn,
        source_id,
        args.source_type,
        args.source_name,
        args.primaryness,
        args.jurisdiction,
        args.language,
        args.host,
    )
    raw_path, text_path = _write_artifact_files(
        paths,
        artifact_id,
        result.raw_text,
        result.transcript_text,
        raw_suffix="json",
    )
    insert_artifact(
        conn,
        {
            "artifact_id": artifact_id,
            "source_id": source_id,
            "artifact_kind": args.artifact_kind,
            "title": args.title or result.title,
            "captured_at": utc_now_iso(),
            "published_at": args.published_at,
            "language": args.language,
            "uri": str(Path(args.audio_path).resolve()),
            "raw_path": str(raw_path),
            "normalized_text_path": str(text_path),
            "content_hash": sha256_text(result.raw_text),
            "status": "captured",
            "metadata_json": json_dumps(result.metadata),
        },
        content=result.transcript_text,
    )
    insert_event(conn, make_id("evt"), "artifact", artifact_id, "artifact_ingested", {"path": str(Path(args.audio_path).resolve())})
    insert_row(
        conn,
        "analysis_runs",
        {
            "analysis_run_id": stable_id("run", f"homepc_funasr_{artifact_id}"),
            "engine": result.metadata.get("engine", "homepc_funasr"),
            "input_refs_json": json_dumps([str(Path(args.audio_path).resolve())]),
            "output_ref": artifact_id,
            "schema_valid": 1,
        },
    )
    return {
        "artifact_id": artifact_id,
        "source_id": source_id,
        "result": result,
    }


def cmd_transcribe_homepc_funasr(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    source_id = args.source_id or "src_homepc_funasr"
    artifact_id = args.artifact_id or stable_id("art", f"homepc_funasr_{Path(args.audio_path).resolve()}")
    try:
        payload = _ingest_homepc_transcription_artifact(conn, paths, args, artifact_id=artifact_id, source_id=source_id)
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    conn.commit()
    print(
        json_dumps(
            {
                "ok": True,
                "artifact_id": artifact_id,
                "source_id": source_id,
                "char_count": payload["result"].metadata.get("char_count", len(payload["result"].transcript_text)),
                "preview": payload["result"].transcript_text[:240],
            }
        )
    )
    return 0


def cmd_intake_voice_memo_audio(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    source_id = args.source_id or "src_voice_memo_homepc"
    artifact_id = args.artifact_id or stable_id("art", f"voice_memo_{Path(args.audio_path).resolve()}")
    try:
        payload = _ingest_homepc_transcription_artifact(conn, paths, args, artifact_id=artifact_id, source_id=source_id)
        claims = _extract_claims_for_artifact(conn, artifact_id, args.speaker, args.min_chars)
        routes = _route_claims_for_artifact(conn, artifact_id)
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    conn.commit()
    print(
        json_dumps(
            {
                "ok": True,
                "artifact_id": artifact_id,
                "source_id": source_id,
                "char_count": payload["result"].metadata.get("char_count", len(payload["result"].transcript_text)),
                "claim_count": len(claims),
                "route_count": len(routes),
                "preview": payload["result"].transcript_text[:240],
                "claims": claims[:10],
            }
        )
    )
    return 0


def cmd_intake_kol_digest(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    input_path = Path(args.path).resolve()
    source_id = args.source_id or stable_id("src", f"kol_{args.source_name}")
    artifact_id = args.artifact_id or stable_id("art", f"kol_digest_{input_path}")
    try:
        _ingest_text_artifact(
            conn,
            paths,
            source_id=source_id,
            source_type=args.source_type,
            source_name=args.source_name,
            primaryness=args.primaryness,
            input_path=input_path,
            artifact_id=artifact_id,
            artifact_kind=args.artifact_kind,
            title=args.title or input_path.stem,
            published_at=args.published_at,
            language=args.language,
            uri=args.uri,
            jurisdiction=args.jurisdiction,
            base_uri=args.base_uri,
        )
        claims = _extract_claims_for_artifact(conn, artifact_id, args.speaker, args.min_chars)
        routes = _route_claims_for_artifact(conn, artifact_id)
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    conn.commit()
    print(
        json_dumps(
            {
                "ok": True,
                "artifact_id": artifact_id,
                "source_id": source_id,
                "claim_count": len(claims),
                "route_count": len(routes),
                "claims": claims[:10],
            }
        )
    )
    return 0


def cmd_extract_claims(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    try:
        claims = _extract_claims_for_artifact(conn, args.artifact_id, args.speaker, args.min_chars)
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    conn.commit()
    print(json_dumps({"ok": True, "artifact_id": args.artifact_id, "claim_count": len(claims), "claims": claims[:10]}))
    return 0


def _decide_claim_routes(source_type: str, primaryness: str, claim_type: str) -> list[tuple[str, str]]:
    if primaryness == "personal":
        if claim_type == "counterpoint":
            return [("counter_search", "personal lane counterpoint should explicitly challenge an existing thesis")]
        routes = [("thesis_seed", "personal lane claim should become a thesis seed candidate")]
        if claim_type == "catalyst":
            routes.append(("monitor_candidate", "personal lane catalyst may deserve later monitor setup"))
        return routes
    if source_type == "kol" or primaryness == "second_hand":
        routes = [("corroboration_needed", "second-hand or KOL claim needs corroboration before evidence promotion")]
        if claim_type == "counterpoint":
            routes.append(("counter_search", "second-hand counterpoint should trigger explicit contradiction review"))
        return routes
    if claim_type == "counterpoint":
        return [("counter_search", "first-hand counterpoint should stress test the thesis")]
    routes = [("thesis_input", "first-hand claim can feed thesis updates once reviewed")]
    if claim_type == "catalyst":
        routes.append(("monitor_candidate", "first-hand catalyst may deserve a monitor candidate"))
    return routes


def _route_claims_for_artifact(conn: Any, artifact_id: str) -> list[dict[str, Any]]:
    artifact = select_one(
        conn,
        """
        SELECT a.artifact_id, a.source_id, s.source_type, s.primaryness
        FROM artifacts a
        JOIN sources s ON s.source_id = a.source_id
        WHERE a.artifact_id = ?
        """,
        (artifact_id,),
    )
    if artifact is None:
        raise AdapterError(f"artifact not found: {artifact_id}")
    claims = [
        dict(row)
        for row in list_rows(
        conn,
        "SELECT claim_id, claim_type, claim_text FROM claims WHERE artifact_id = ? AND status != 'quarantined' ORDER BY created_at, claim_id",
        (artifact_id,),
    )
    ]
    if not claims:
        raise AdapterError(f"no claims found for artifact: {artifact_id}")
    _delete_route_state_for_artifact(conn, artifact_id)
    conn.execute("DELETE FROM analysis_runs WHERE analysis_run_id = ?", (stable_id("run", f"route_{artifact_id}"),))
    routed: list[dict[str, Any]] = []
    for claim in claims:
        for route_type, reason in _decide_claim_routes(artifact["source_type"], artifact["primaryness"], claim["claim_type"]):
            route_id = stable_id("route", f"{claim['claim_id']}::{route_type}")
            metadata = {
                "source_type": artifact["source_type"],
                "primaryness": artifact["primaryness"],
                "claim_type": claim["claim_type"],
            }
            insert_row(
                conn,
                "claim_routes",
                {
                    "route_id": route_id,
                    "claim_id": claim["claim_id"],
                    "artifact_id": artifact_id,
                    "route_type": route_type,
                    "target_object_type": "artifact",
                    "target_object_id": artifact_id,
                    "reason": reason,
                    "status": "pending",
                    "metadata_json": json_dumps(metadata),
                },
            )
            routed.append(
                {
                    "route_id": route_id,
                    "claim_id": claim["claim_id"],
                    "route_type": route_type,
                    "reason": reason,
                }
            )
    insert_event(conn, make_id("evt"), "artifact", artifact_id, "claims_routed", {"route_count": len(routed)})
    insert_row(
        conn,
        "analysis_runs",
        {
            "analysis_run_id": stable_id("run", f"route_{artifact_id}"),
            "engine": "rule_based_claim_router_v1",
            "input_refs_json": json_dumps([artifact_id]),
            "output_ref": artifact_id,
            "schema_valid": 1,
        },
    )
    return routed


def cmd_route_claims(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    try:
        routes = _route_claims_for_artifact(conn, args.artifact_id)
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    counts: dict[str, int] = {}
    for route in routes:
        counts[route["route_type"]] = counts.get(route["route_type"], 0) + 1
    conn.commit()
    print(json_dumps({"ok": True, "artifact_id": args.artifact_id, "route_count": len(routes), "route_type_counts": counts, "routes": routes[:10]}))
    return 0


def cmd_set_route_status(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    try:
        payload = _set_route_status(conn, route_id=args.route_id, status=args.status, note=args.note)
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    conn.commit()
    print(json_dumps({"ok": True, **payload}))
    return 0


def _set_route_status(conn: Any, *, route_id: str, status: str, note: str) -> dict[str, Any]:
    row = select_one(conn, "SELECT route_id, status, metadata_json FROM claim_routes WHERE route_id = ?", (route_id,))
    if row is None:
        raise AdapterError(f"route not found: {route_id}")
    metadata = json.loads(row["metadata_json"] or "{}")
    if note:
        metadata["resolution_note"] = note
    conn.execute(
        "UPDATE claim_routes SET status = ?, metadata_json = ? WHERE route_id = ?",
        (status, json_dumps(metadata), route_id),
    )
    insert_event(conn, make_id("evt"), "claim_route", route_id, "claim_route_status_updated", {"status": status})
    return {"route_id": route_id, "from_status": row["status"], "status": status}


def cmd_set_route_status_batch(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    route_ids = list(dict.fromkeys(args.route_id or []))
    if not route_ids:
        print(json_dumps({"ok": False, "error": "set-route-status-batch requires at least one --route-id"}))
        return 1
    updated: list[dict[str, Any]] = []
    try:
        for route_id in route_ids:
            updated.append(_set_route_status(conn, route_id=route_id, status=args.status, note=args.note))
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc), "updated_count": len(updated)}))
        return 1
    conn.commit()
    print(json_dumps({"ok": True, "updated_count": len(updated), "route_ids": route_ids, "routes": updated[:20]}))
    return 0


def cmd_routing_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT cr.route_id, cr.claim_id, cr.artifact_id, cr.route_type, cr.status, cr.reason,
                   COUNT(crl.route_link_id) AS link_count,
                   c.claim_type, c.claim_text, a.title AS artifact_title
            FROM claim_routes cr
            JOIN claims c ON c.claim_id = cr.claim_id
            JOIN artifacts a ON a.artifact_id = cr.artifact_id
            LEFT JOIN claim_route_links crl ON crl.route_id = cr.route_id
            GROUP BY cr.route_id, cr.claim_id, cr.artifact_id, cr.route_type, cr.status, cr.reason,
                     c.claim_type, c.claim_text, a.title
            ORDER BY cr.created_at DESC
            """,
        )
    ]
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["route_type"]] = counts.get(row["route_type"], 0) + 1
    print(json_dumps({"summary": {"routes": len(rows), "by_type": counts}, "items": rows[:60]}))
    return 0


def cmd_corroboration_queue(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            """
            SELECT cr.route_id, cr.status, cr.reason, COUNT(crl.route_link_id) AS link_count,
                   c.claim_id, c.claim_type, c.confidence, c.claim_text,
                   a.artifact_id, a.title AS artifact_title, s.name AS source_name, s.source_type, s.primaryness
            FROM claim_routes cr
            JOIN claims c ON c.claim_id = cr.claim_id
            JOIN artifacts a ON a.artifact_id = cr.artifact_id
            JOIN sources s ON s.source_id = a.source_id
            LEFT JOIN claim_route_links crl ON crl.route_id = cr.route_id
            WHERE cr.route_type = 'corroboration_needed' AND cr.status = ?
            GROUP BY cr.route_id, cr.status, cr.reason, c.claim_id, c.claim_type, c.confidence, c.claim_text,
                     a.artifact_id, a.title, s.name, s.source_type, s.primaryness
            ORDER BY a.captured_at DESC, cr.created_at DESC
            """,
            (args.status,),
        )
    ]
    print(json_dumps({"summary": {"routes": len(rows), "status": args.status}, "items": rows[:80]}))
    return 0


def cmd_route_workbench(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(
        json_dumps(
            build_route_workbench(
                conn,
                status=args.status,
                route_type=args.route_type,
                source_id=args.source_id,
                thesis_id=args.thesis_id,
                limit=args.limit,
            )
        )
    )
    return 0


def cmd_route_normalization_queue(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_route_normalization_queue(conn, limit=args.limit)))
    return 0


def cmd_thesis_gate_report(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_thesis_gate_report(conn, args.thesis_id)))
    return 0



def _default_link_kind(route_type: str, linked_object_type: str) -> str:
    if linked_object_type == "artifact":
        if route_type == "counter_search":
            return "contradicted_by"
        return "corroborated_by"
    if route_type in {"thesis_seed", "thesis_input", "counter_search"}:
        return "feeds"
    if route_type == "monitor_candidate":
        return "opens"
    return "maps_to"


def _create_route_link(
    conn: Any,
    *,
    route_id: str,
    link_kind: str,
    linked_object_type: str,
    linked_object_id: str,
    note: str,
    metadata: dict[str, Any] | None = None,
) -> str:
    route_link_id = stable_id("rlnk", f"{route_id}::{link_kind}::{linked_object_type}::{linked_object_id}")
    insert_row(
        conn,
        "claim_route_links",
        {
            "route_link_id": route_link_id,
            "route_id": route_id,
            "link_kind": link_kind,
            "linked_object_type": linked_object_type,
            "linked_object_id": linked_object_id,
            "note": note,
            "metadata_json": json_dumps(metadata or {}),
        },
    )
    return route_link_id


def _create_validation_case(
    conn: Any,
    *,
    route: sqlite3.Row | dict[str, Any],
    verdict: str,
    thesis_id: str,
    thesis_version_id: str | None,
    evidence_artifact_ids: list[str],
    rationale: str,
    validator: str = "system",
    validator_model: str = "deterministic_route_resolution",
) -> str:
    route_row = dict(route)
    validation_case_id = stable_id(
        "vcase",
        f"{route_row['route_id']}::{verdict}::{thesis_id}::{thesis_version_id or ''}::{','.join(sorted(evidence_artifact_ids))}",
    )
    conn.execute(
        """
        DELETE FROM validation_cases
        WHERE route_id = ? AND verdict = ? AND thesis_id = ? AND COALESCE(thesis_version_id, '') = COALESCE(?, '')
        """,
        (route_row["route_id"], verdict, thesis_id, thesis_version_id),
    )
    insert_row(
        conn,
        "validation_cases",
        {
            "validation_case_id": validation_case_id,
            "route_id": route_row["route_id"],
            "claim_id": route_row["claim_id"],
            "thesis_id": thesis_id,
            "thesis_version_id": thesis_version_id,
            "source_id": route_row.get("source_id"),
            "verdict": verdict,
            "evidence_artifact_ids_json": json_dumps(evidence_artifact_ids),
            "rationale": rationale,
            "validator": validator,
            "validator_model": validator_model,
            "expires_at": "",
        },
    )
    insert_event(
        conn,
        make_id("evt"),
        "validation_case",
        validation_case_id,
        "validation_case_created",
        {
            "route_id": route_row["route_id"],
            "claim_id": route_row["claim_id"],
            "verdict": verdict,
            "thesis_id": thesis_id,
            "thesis_version_id": thesis_version_id,
            "evidence_artifact_ids": evidence_artifact_ids,
        },
    )
    return validation_case_id


def _apply_route_resolution(
    conn: Any,
    *,
    route_id: str,
    status: str,
    link_object_type: str,
    link_object_id: str,
    link_kind: str,
    evidence_artifact_ids: list[str],
    note: str,
) -> dict[str, Any]:
    route = select_one(
        conn,
        """
        SELECT cr.route_id, cr.claim_id, cr.artifact_id, cr.route_type, cr.status, cr.metadata_json,
               s.source_id, s.primaryness, s.source_type
        FROM claim_routes cr
        JOIN artifacts a ON a.artifact_id = cr.artifact_id
        JOIN sources s ON s.source_id = a.source_id
        WHERE cr.route_id = ?
        """,
        (route_id,),
    )
    if route is None:
        raise AdapterError(f"route not found: {route_id}")
    if bool(link_object_type) != bool(link_object_id):
        raise AdapterError("apply-route requires --link-object-type and --link-object-id together")
    if not link_object_id and not evidence_artifact_ids:
        raise AdapterError("apply-route requires --link-object-id or --evidence-artifact-id")
    metadata = json.loads(route["metadata_json"] or "{}")
    if note:
        metadata["resolution_note"] = note
    linked_payloads: list[dict[str, Any]] = []
    if link_object_id:
        resolved_link_kind = link_kind or _default_link_kind(route["route_type"], link_object_type)
        route_link_id = _create_route_link(
            conn,
            route_id=route_id,
            link_kind=resolved_link_kind,
            linked_object_type=link_object_type,
            linked_object_id=link_object_id,
            note=note,
            metadata={"applied_via": "apply-route"},
        )
        linked_payloads.append(
            {
                "route_link_id": route_link_id,
                "link_kind": resolved_link_kind,
                "linked_object_type": link_object_type,
                "linked_object_id": link_object_id,
            }
        )
    for artifact_id in evidence_artifact_ids:
        artifact = select_one(conn, "SELECT artifact_id FROM artifacts WHERE artifact_id = ?", (artifact_id,))
        if artifact is None:
            raise AdapterError(f"artifact not found: {artifact_id}")
        route_link_id = _create_route_link(
            conn,
            route_id=route_id,
            link_kind="corroborated_by",
            linked_object_type="artifact",
            linked_object_id=artifact_id,
            note=note,
            metadata={"applied_via": "apply-route"},
        )
        linked_payloads.append(
            {
                "route_link_id": route_link_id,
                "link_kind": "corroborated_by",
                "linked_object_type": "artifact",
                "linked_object_id": artifact_id,
            }
        )
    thesis_version_id = _resolve_thesis_version_id(conn, link_object_type, link_object_id) if link_object_id else None
    resolved_thesis_id = ""
    if link_object_type == "thesis":
        resolved_thesis_id = link_object_id
    elif thesis_version_id:
        thesis_row = select_one(
            conn,
            "SELECT thesis_id FROM thesis_versions WHERE thesis_version_id = ?",
            (thesis_version_id,),
        )
        resolved_thesis_id = thesis_row["thesis_id"] if thesis_row else ""
    attached_artifact_ids: list[str] = []
    if thesis_version_id:
        version = select_one(
            conn,
            "SELECT created_from_artifacts_json FROM thesis_versions WHERE thesis_version_id = ?",
            (thesis_version_id,),
        )
        route_artifact_ids = [route["artifact_id"]]
        all_artifact_ids = route_artifact_ids + evidence_artifact_ids
        conn.execute(
            "UPDATE thesis_versions SET created_from_artifacts_json = ? WHERE thesis_version_id = ?",
            (_append_unique_items(version["created_from_artifacts_json"] if version else "[]", all_artifact_ids), thesis_version_id),
        )
        attached_artifact_ids = all_artifact_ids
        insert_event(
            conn,
            make_id("evt"),
            "thesis_version",
            thesis_version_id,
            "thesis_version_artifacts_extended",
            {"artifact_ids": all_artifact_ids, "route_id": route_id},
        )
    metadata["linked_objects"] = linked_payloads
    if attached_artifact_ids:
        metadata["attached_artifact_ids"] = attached_artifact_ids
    validation_case_id = ""
    if status == "accepted" and resolved_thesis_id and evidence_artifact_ids:
        if route["route_type"] == "corroboration_needed":
            validation_case_id = _create_validation_case(
                conn,
                route=route,
                verdict="validated",
                thesis_id=resolved_thesis_id,
                thesis_version_id=thesis_version_id,
                evidence_artifact_ids=evidence_artifact_ids,
                rationale=note or "accepted corroboration route with first-hand evidence",
            )
        elif route["route_type"] == "counter_search":
            validation_case_id = _create_validation_case(
                conn,
                route=route,
                verdict="contradicted",
                thesis_id=resolved_thesis_id,
                thesis_version_id=thesis_version_id,
                evidence_artifact_ids=evidence_artifact_ids,
                rationale=note or "accepted counter-search route with evidence",
            )
    if validation_case_id:
        metadata["validation_case_id"] = validation_case_id
    conn.execute(
        """
        UPDATE claim_routes
        SET status = ?, target_object_type = ?, target_object_id = ?, metadata_json = ?
        WHERE route_id = ?
        """,
        (
            status,
            link_object_type or ("artifact" if evidence_artifact_ids else None),
            link_object_id or (evidence_artifact_ids[0] if evidence_artifact_ids else route["artifact_id"]),
            json_dumps(metadata),
            route_id,
        ),
    )
    insert_event(
        conn,
        make_id("evt"),
        "claim_route",
        route_id,
        "claim_route_applied",
        {
            "status": status,
            "link_object_type": link_object_type,
            "link_object_id": link_object_id,
            "evidence_artifact_ids": evidence_artifact_ids,
            "validation_case_id": validation_case_id or None,
        },
    )
    return {
        "route_id": route_id,
        "status": status,
        "links": linked_payloads,
        "attached_artifact_ids": attached_artifact_ids,
        "thesis_version_id": thesis_version_id,
        "validation_case_id": validation_case_id or None,
    }


def cmd_apply_route(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    try:
        payload = _apply_route_resolution(
            conn,
            route_id=args.route_id,
            status=args.status,
            link_object_type=args.link_object_type,
            link_object_id=args.link_object_id,
            link_kind=args.link_kind,
            evidence_artifact_ids=args.evidence_artifact_id or [],
            note=args.note,
        )
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    conn.commit()
    print(json_dumps({"ok": True, **payload}))
    return 0


def cmd_apply_route_batch(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    route_ids = list(dict.fromkeys(args.route_id or []))
    if not route_ids:
        print(json_dumps({"ok": False, "error": "apply-route-batch requires at least one --route-id"}))
        return 1
    applied: list[dict[str, Any]] = []
    try:
        for route_id in route_ids:
            applied.append(
                _apply_route_resolution(
                    conn,
                    route_id=route_id,
                    status=args.status,
                    link_object_type=args.link_object_type,
                    link_object_id=args.link_object_id,
                    link_kind=args.link_kind,
                    evidence_artifact_ids=args.evidence_artifact_id or [],
                    note=args.note,
                )
            )
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc), "applied_count": len(applied)}))
        return 1
    conn.commit()
    print(json_dumps({"ok": True, "applied_count": len(applied), "route_ids": route_ids, "routes": applied[:20]}))
    return 0


def cmd_bulk_apply_routes(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    if not args.artifact_id and not args.route_type and not args.source_id and not args.candidate_thesis_id:
        print(
            json_dumps(
                {
                    "ok": False,
                    "error": "bulk-apply-routes requires --artifact-id, --route-type, --source-id, or --candidate-thesis-id to avoid accidental global updates",
                }
            )
        )
        return 1
    if args.candidate_thesis_id and not args.link_object_id:
        args.link_object_type = "thesis"
        args.link_object_id = args.candidate_thesis_id
    if args.source_id or args.candidate_thesis_id:
        workbench = build_route_workbench(
            conn,
            status=args.from_status,
            route_type=args.route_type,
            source_id=args.source_id,
            thesis_id=args.candidate_thesis_id,
            limit=args.limit,
        )
        rows = [{"route_id": item["route_id"]} for item in workbench["items"][: args.limit]]
    else:
        where = ["status = ?"]
        params: list[Any] = [args.from_status]
        if args.artifact_id:
            where.append("artifact_id = ?")
            params.append(args.artifact_id)
        if args.route_type:
            where.append("route_type = ?")
            params.append(args.route_type)
        rows = [
            dict(row)
            for row in list_rows(
                conn,
                f"SELECT route_id FROM claim_routes WHERE {' AND '.join(where)} ORDER BY created_at LIMIT ?",
                (*params, args.limit),
            )
        ]
    applied: list[dict[str, Any]] = []
    for row in rows:
        applied.append(
            _apply_route_resolution(
                conn,
                route_id=row["route_id"],
                status=args.new_status,
                link_object_type=args.link_object_type,
                link_object_id=args.link_object_id,
                link_kind=args.link_kind,
                evidence_artifact_ids=args.evidence_artifact_id or [],
                note=args.note,
            )
        )
    conn.commit()
    print(json_dumps({"ok": True, "applied_count": len(applied), "routes": applied[:20]}))
    return 0


def cmd_create_entity(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    entity_id = args.entity_id or make_id("ent", args.name)
    insert_row(
        conn,
        "entities",
        {
            "entity_id": entity_id,
            "entity_type": args.entity_type,
            "canonical_name": args.name,
            "aliases_json": json_dumps(args.alias or []),
            "tickers_or_symbols_json": json_dumps(args.symbol or []),
            "jurisdiction": args.jurisdiction,
            "external_ids_json": json_dumps({}),
        },
    )
    insert_event(conn, make_id("evt"), "entity", entity_id, "entity_created", {"name": args.name})
    conn.commit()
    print(json_dumps({"ok": True, "entity_id": entity_id}))
    return 0


def cmd_create_theme(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    theme_id = args.theme_id or make_id("theme", args.name)
    insert_row(
        conn,
        "themes",
        {
            "theme_id": theme_id,
            "name": args.name,
            "why_it_matters": args.why_it_matters,
            "maturity_stage": args.maturity_stage,
            "commercialization_paths": args.commercialization_paths,
            "importance_status": args.importance_status,
        },
    )
    insert_event(conn, make_id("evt"), "theme", theme_id, "theme_created", {"name": args.name})
    conn.commit()
    print(json_dumps({"ok": True, "theme_id": theme_id}))
    return 0


def cmd_create_thesis(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    thesis_id = args.thesis_id or make_id("thesis", args.title)
    thesis_version_id = args.thesis_version_id or make_id("thv", args.title)
    theme_ids = args.theme_id or []
    artifact_ids = args.artifact_id or []
    insert_row(
        conn,
        "theses",
        {
            "thesis_id": thesis_id,
            "title": args.title,
            "status": args.status,
            "horizon_months": args.horizon_months,
            "theme_ids_json": json_dumps(theme_ids),
            "current_version_id": thesis_version_id,
            "owner": args.owner,
        },
    )
    insert_row(
        conn,
        "thesis_versions",
        {
            "thesis_version_id": thesis_version_id,
            "thesis_id": thesis_id,
            "statement": args.statement,
            "mechanism_chain": args.mechanism_chain,
            "why_now": args.why_now,
            "base_case": args.base_case,
            "counter_case": args.counter_case,
            "invalidators": args.invalidators,
            "required_followups": args.required_followups,
            "human_conviction": args.human_conviction,
            "created_from_artifacts_json": json_dumps(artifact_ids),
        },
    )
    insert_event(conn, make_id("evt"), "thesis", thesis_id, "thesis_created", {"version_id": thesis_version_id})
    conn.commit()
    print(json_dumps({"ok": True, "thesis_id": thesis_id, "thesis_version_id": thesis_version_id}))
    return 0


def cmd_create_target(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    target_id = args.target_id or make_id("target", args.ticker_or_symbol)
    insert_row(
        conn,
        "targets",
        {
            "target_id": target_id,
            "entity_id": args.entity_id,
            "asset_class": args.asset_class,
            "venue": args.venue,
            "ticker_or_symbol": args.ticker_or_symbol,
            "currency": args.currency,
            "liquidity_bucket": args.liquidity_bucket,
        },
    )
    insert_event(conn, make_id("evt"), "target", target_id, "target_created", {"ticker": args.ticker_or_symbol})
    conn.commit()
    print(json_dumps({"ok": True, "target_id": target_id}))
    return 0


def cmd_create_target_case(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    target_case_id = args.target_case_id or make_id("tc", args.target_id)
    metrics = {}
    if args.key_metric:
        for item in args.key_metric:
            key, value = item.split("=", 1)
            metrics[key] = value
    insert_row(
        conn,
        "target_cases",
        {
            "target_case_id": target_case_id,
            "thesis_version_id": args.thesis_version_id,
            "target_id": args.target_id,
            "exposure_type": args.exposure_type,
            "capture_link_strength": args.capture_link_strength,
            "key_metrics_json": json_dumps(metrics),
            "valuation_context": args.valuation_context,
            "risks": args.risks,
            "status": args.status,
        },
    )
    insert_event(conn, make_id("evt"), "target_case", target_case_id, "target_case_created", {"target_id": args.target_id})
    conn.commit()
    print(json_dumps({"ok": True, "target_case_id": target_case_id}))
    return 0


def cmd_create_timing_plan(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    timing_plan_id = args.timing_plan_id or make_id("timing", args.target_case_id)
    insert_row(
        conn,
        "timing_plans",
        {
            "timing_plan_id": timing_plan_id,
            "target_case_id": args.target_case_id,
            "window_type": args.window_type,
            "catalysts_json": json_dumps(args.catalyst or []),
            "confirmation_signals_json": json_dumps(args.confirmation_signal or []),
            "preconditions_json": json_dumps(args.precondition or []),
            "invalidators_json": json_dumps(args.invalidator or []),
            "desired_posture": args.desired_posture,
        },
    )
    insert_event(conn, make_id("evt"), "timing_plan", timing_plan_id, "timing_plan_created", {"target_case_id": args.target_case_id})
    conn.commit()
    print(json_dumps({"ok": True, "timing_plan_id": timing_plan_id}))
    return 0


def cmd_create_monitor(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    if args.monitor_type != "claim_freshness" and (not args.artifact_id or not args.metric_name):
        raise AdapterError("artifact_metric monitor requires --artifact-id and --metric-name")
    metric_name = args.metric_name or ("max_claim_age_days" if args.monitor_type == "claim_freshness" else "")
    monitor_id = args.monitor_id or make_id("mon", metric_name or args.owner_object_id)
    if args.monitor_type == "claim_freshness":
        rule = {
            "kind": "claim_freshness",
            "thesis_id": args.owner_object_id,
            "threshold_days": int(args.threshold_value),
        }
    else:
        rule = {
            "kind": "artifact_metric",
            "artifact_id": args.artifact_id,
            "metric_name": metric_name,
        }
    insert_row(
        conn,
        "monitors",
        {
            "monitor_id": monitor_id,
            "owner_object_type": args.owner_object_type,
            "owner_object_id": args.owner_object_id,
            "monitor_type": args.monitor_type,
            "metric_name": metric_name,
            "comparator": args.comparator,
            "threshold_value": args.threshold_value,
            "latest_value": None,
            "query_or_rule": json_dumps(rule),
            "status": "live",
        },
    )
    insert_event(conn, make_id("evt"), "monitor", monitor_id, "monitor_created", rule)
    conn.commit()
    print(json_dumps({"ok": True, "monitor_id": monitor_id}))
    return 0


def _load_artifact_metric(conn: Any, artifact_id: str, metric_name: str) -> float | None:
    row = select_one(conn, "SELECT metadata_json FROM artifacts WHERE artifact_id = ?", (artifact_id,))
    if row is None or not row["metadata_json"]:
        return None
    payload = json.loads(row["metadata_json"])
    metrics = payload.get("metrics", {})
    value = metrics.get(metric_name)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _load_claim_freshness_metric(conn: Any, thesis_id: str, threshold_days: int) -> float | None:
    # NOTE: we intentionally recalculate freshness_status at runtime via
    # freshness_status_for_date() rather than trusting the snapshot stored in
    # claims.freshness_status.  The DB column captures staleness at insertion
    # time; this function must reflect *current* age so that monitors alert
    # as data grows stale even without claim re-ingestion.
    thesis = select_one(conn, "SELECT current_version_id FROM theses WHERE thesis_id = ?", (thesis_id,))
    if thesis is None or not thesis["current_version_id"]:
        return None
    version = select_one(
        conn,
        "SELECT created_from_artifacts_json FROM thesis_versions WHERE thesis_version_id = ?",
        (thesis["current_version_id"],),
    )
    artifact_ids = _json_load_list(version["created_from_artifacts_json"] if version else "")
    if not artifact_ids:
        return None
    placeholders = ",".join("?" for _ in artifact_ids)
    rows = [
        dict(row)
        for row in list_rows(
            conn,
            f"SELECT data_date, freshness_status FROM claims WHERE artifact_id IN ({placeholders}) AND status != 'quarantined'",
            tuple(artifact_ids),
        )
    ]
    if not rows:
        return None
    status_score = {"fresh": 0, "aging": 1, "stale": 2, "unknown": 3}
    max_score = 0
    for row in rows:
        freshness_status = freshness_status_for_date(row.get("data_date") or "")
        if freshness_status == "fresh" and row.get("data_date"):
            try:
                age_days = (datetime.now(timezone.utc).date() - datetime.fromisoformat(row["data_date"]).date()).days
            except ValueError:
                age_days = None
            if age_days is not None and age_days > threshold_days:
                freshness_status = "aging" if age_days <= threshold_days * 2 else "stale"
        max_score = max(max_score, status_score.get(freshness_status, 3))
    return float(max_score)


def cmd_run_monitors(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(_run_monitors_with_conn(conn)))
    return 0


def cmd_sentinel_validate(args: argparse.Namespace) -> int:
    root, _ = _repo_paths(args.root)
    spec_path = _resolve_io_path(root, args.spec) if args.spec else default_spec_path(root)
    spec = load_sentinel_spec(spec_path)
    spec_errors = validate_sentinel_spec(spec)
    fixture_report = validate_fixtures()
    payload = {
        "ok": len(spec_errors) == 0 and fixture_report["failed"] == 0,
        "schema_version": spec.get("schema_version"),
        "spec_path": str(spec_path),
        "spec_error_count": len(spec_errors),
        "spec_errors": spec_errors,
        "fixture_report": fixture_report,
    }
    print(json_dumps(payload))
    return 0 if payload["ok"] else 1


def cmd_sentinel_sync(args: argparse.Namespace) -> int:
    root, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    spec_path = _resolve_io_path(root, args.spec) if args.spec else default_spec_path(root)
    spec = load_sentinel_spec(spec_path)
    result = sync_sentinel_spec(conn, spec)
    payload = {
        "spec_path": str(spec_path),
        **result,
    }
    print(json_dumps(payload))
    return 0 if result.get("ok") else 1


def cmd_sentinel_check_stalls(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    result = emit_stalled_events(conn, as_of=args.as_of or None)
    print(json_dumps(result))
    return 0 if result.get("ok") else 1


def cmd_event_prompt(args: argparse.Namespace) -> int:
    root, _ = _repo_paths(args.root)
    raw_path = _resolve_io_path(root, args.path)
    raw_text = raw_path.read_text(encoding="utf-8")
    sentinel_context = None
    if not args.no_context:
        spec_path = _resolve_io_path(root, args.spec) if args.spec else default_spec_path(root)
        if spec_path.exists():
            spec = load_sentinel_spec(spec_path)
            sentinel_context = build_spec_prompt_context(spec)
    payload = {
        "ok": True,
        "path": str(raw_path),
        "prompt": build_extraction_prompt(raw_text, sentinel_context=sentinel_context),
    }
    print(json_dumps(payload))
    return 0


def cmd_event_run_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    items = list_event_mining_runs(conn, limit=args.limit)
    summary = {
        "run_count": len(items),
        "engines": sorted({item["engine"] for item in items}),
    }
    print(json_dumps({"summary": summary, "items": items}))
    return 0


def cmd_event_run_compare(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    payload = compare_event_mining_runs(conn, run_ids=list(args.run_id or []))
    print(json_dumps(payload))
    return 0


def cmd_event_source_policy(args: argparse.Namespace) -> int:
    payload = {
        "count": len(list_source_policies()),
        "items": list_source_policies(),
    }
    print(json_dumps(payload))
    return 0


def cmd_event_sector_grammars(args: argparse.Namespace) -> int:
    items = list_sector_grammars()
    payload = {
        "count": len(items),
        "items": items,
    }
    print(json_dumps(payload))
    return 0


def cmd_event_source_adapters(args: argparse.Namespace) -> int:
    items = list_source_adapters()
    payload = {
        "count": len(items),
        "items": items,
    }
    print(json_dumps(payload))
    return 0


def cmd_event_validate(args: argparse.Namespace) -> int:
    root, _paths = _repo_paths(args.root)
    batch = _load_event_batch(str(_resolve_io_path(root, args.path)) if args.path != "-" else args.path)
    results: list[dict[str, Any]] = []
    for draft in batch:
        normalized = normalize_event(draft)
        errors = validate_event(normalized)
        results.append(
            {
                "event_id": normalized.get("event_id"),
                "ok": len(errors) == 0,
                "errors": errors,
                "route": route_event(normalized) if len(errors) == 0 else None,
                "normalized": normalized if args.include_normalized else None,
            }
        )
    payload = {
        "ok": all(item["ok"] for item in results),
        "count": len(results),
        "valid_count": sum(1 for item in results if item["ok"]),
        "invalid_count": sum(1 for item in results if not item["ok"]),
        "results": results,
    }
    print(json_dumps(payload))
    return 0 if payload["ok"] else 1


def cmd_event_route_validate(args: argparse.Namespace) -> int:
    root, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    spec_sync = None
    if not args.skip_sentinel_sync:
        spec_path = _resolve_io_path(root, args.spec) if args.spec else default_spec_path(root)
        if spec_path.exists():
            spec_sync = {"spec_path": str(spec_path), **sync_sentinel_spec(conn, load_sentinel_spec(spec_path))}
        else:
            spec_sync = {"ok": True, "spec_path": str(spec_path), "synced": 0, "updated": 0, "warning": "spec_not_found"}
    batch = _load_event_batch(str(_resolve_io_path(root, args.path)) if args.path != "-" else args.path)
    results = [classify_event(conn, draft) for draft in batch]
    fixture_report = validate_fixtures() if args.with_fixtures else None
    payload = {
        "ok": all(item.get("ok") for item in results) and (spec_sync is None or spec_sync.get("ok", False)),
        "count": len(results),
        "results": results,
        "spec_sync": spec_sync,
        "fixture_report": fixture_report,
    }
    print(json_dumps(payload))
    return 0 if payload["ok"] else 1


def cmd_event_import(args: argparse.Namespace) -> int:
    root, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    spec_sync = None
    if not args.skip_sentinel_sync:
        spec_path = _resolve_io_path(root, args.spec) if args.spec else default_spec_path(root)
        if spec_path.exists():
            spec_sync = {"spec_path": str(spec_path), **sync_sentinel_spec(conn, load_sentinel_spec(spec_path))}
        else:
            spec_sync = {"ok": True, "spec_path": str(spec_path), "synced": 0, "updated": 0, "warning": "spec_not_found"}
    batch = _load_event_batch(str(_resolve_io_path(root, args.path)) if args.path != "-" else args.path)
    result = import_events(conn, batch)
    stalled = emit_stalled_events(conn, as_of=args.as_of or None) if args.emit_stalls else None
    payload = {
        "ok": result.get("ok", False) and (spec_sync is None or spec_sync.get("ok", False)) and (stalled is None or stalled.get("ok", False)),
        "import_result": result,
        "spec_sync": spec_sync,
        "stalled_result": stalled,
    }
    print(json_dumps(payload))
    return 0 if payload["ok"] else 1


def cmd_anti_thesis_prompt(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    row = select_one(
        conn,
        """
        SELECT *
        FROM anti_thesis_checks
        WHERE object_type = ? AND object_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (args.object_type, args.object_id),
    )
    if row is None:
        print(json_dumps({"ok": False, "error": "anti_thesis_check_not_found"}))
        return 1
    print(json_dumps({"ok": True, "item": dict(row)}))
    return 0


def cmd_anti_thesis_log(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    result = record_anti_thesis_result(
        conn,
        object_type=args.object_type,
        object_id=args.object_id,
        verdict=args.verdict,
        result_summary=args.result_summary,
        contradiction_score=args.contradiction_score,
        note=args.note,
    )
    print(json_dumps(result))
    return 0 if result.get("ok") else 1


def cmd_anti_thesis_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_anti_thesis_board(conn, limit=args.limit)))
    return 0


def cmd_event_feedback_record(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    result = record_feedback(
        conn,
        object_type=args.object_type,
        object_id=args.object_id,
        feedback_type=args.feedback_type,
        verdict=args.verdict,
        score=args.score,
        note=args.note,
        related_event_id=args.related_event_id,
        related_candidate_id=args.related_candidate_id,
    )
    conn.commit()
    print(json_dumps(result))
    return 0 if result.get("ok") else 1


def cmd_event_evaluation_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_event_evaluation_board(conn, limit=args.limit)))
    return 0


def cmd_event_ledger(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_event_ledger(conn, limit=args.limit, route=args.route)))
    return 0


def cmd_sentinel_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_sentinel_board(conn, limit=args.limit)))
    return 0


def cmd_opportunity_inbox(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_opportunity_inbox(conn, limit=args.limit)))
    return 0


def cmd_theme_radar_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_theme_radar_board(conn, limit=args.limit)))
    return 0


def cmd_event_replay_validate(args: argparse.Namespace) -> int:
    root, paths = _repo_paths(args.root)
    spec_path = _resolve_io_path(root, args.spec)
    events_path = _resolve_io_path(root, args.events)
    replay_payload = replay_theme_run(
        ReplayInputs(
            spec_path=spec_path,
            events_path=events_path,
            as_of=args.as_of,
            theme_slug=args.theme_slug,
        )
    )
    if args.reference_run_root:
        reference_paths = resolve_paths(_resolve_io_path(root, args.reference_run_root))
        reference_conn = connect(reference_paths.db_path)
        init_db(reference_conn)
        reference_import = json.loads(
            (reference_paths.state_dir / "theme_run_reports" / args.theme_slug / "import_result.json").read_text(encoding="utf-8")
        )
        reference_stall = json.loads(
            (reference_paths.state_dir / "theme_run_reports" / args.theme_slug / "stall_result.json").read_text(encoding="utf-8")
        )
        replay_payload["validation"] = validate_theme_replay(
            reference_import_result=reference_import,
            reference_stall_result=reference_stall,
            replay_payload=replay_payload,
        )
    print(json_dumps(replay_payload))
    return 0


def cmd_daily_refresh(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    sentinel_sync = None
    if not args.skip_sentinel_sync:
        spec_path = _resolve_io_path(paths.root, args.spec) if args.spec else default_spec_path(paths.root)
        if spec_path.exists():
            sentinel_sync = {"spec_path": str(spec_path), **sync_sentinel_spec(conn, load_sentinel_spec(spec_path))}
        else:
            sentinel_sync = {"ok": True, "spec_path": str(spec_path), "synced": 0, "updated": 0, "warning": "spec_not_found"}
    plan = _build_daily_refresh_plan(conn, limit=args.limit if args.limit and args.limit > 0 else None)
    fetch_results = []
    if args.skip_fetch:
        for spec in plan["refresh_specs"]:
            fetch_results.append(
                {
                    "status": "skipped",
                    "source_id": spec["source_id"],
                    "source_name": spec["source_name"],
                    "kind": spec["kind"],
                    "refresh_key": spec["refresh_key"],
                    "reason": "skip_fetch enabled",
                }
            )
    else:
        for spec in plan["refresh_specs"]:
            try:
                fetch_results.append(_execute_refresh_spec(conn, paths, spec))
            except AdapterError as exc:
                fetch_results.append(
                    {
                        "status": "failed",
                        "source_id": spec["source_id"],
                        "source_name": spec["source_name"],
                        "kind": spec["kind"],
                        "refresh_key": spec["refresh_key"],
                        "reason": str(exc),
                    }
                )
            except Exception as exc:  # pragma: no cover - defensive fail-soft
                fetch_results.append(
                    {
                        "status": "failed",
                        "source_id": spec["source_id"],
                        "source_name": spec["source_name"],
                        "kind": spec["kind"],
                        "refresh_key": spec["refresh_key"],
                        "reason": f"unexpected error: {exc}",
                    }
                )
    if args.skip_monitors:
        monitor_results = {
            "ok": True,
            "monitor_count": 0,
            "results": [],
            "skipped": True,
            "reason": "skip_monitors enabled",
        }
    else:
        monitor_results = _run_monitors_with_conn(conn)
    stalled_result = emit_stalled_events(conn, as_of=args.as_of or None) if not args.skip_stalls else {
        "ok": True,
        "emitted": 0,
        "items": [],
        "as_of": args.as_of or utc_now_iso(),
        "skipped": True,
        "reason": "skip_stalls enabled",
    }
    today_cockpit = build_today_cockpit(conn)
    refresh_summary = {
        "refreshable_specs": len(plan["refresh_specs"]),
        "skipped_sources": len(plan["skipped_sources"]),
        "fetch_success": sum(1 for item in fetch_results if item["status"] == "success"),
        "fetch_failed": sum(1 for item in fetch_results if item["status"] == "failed"),
        "fetch_skipped": sum(1 for item in fetch_results if item["status"] == "skipped"),
        "monitor_count": monitor_results["monitor_count"],
        "monitor_skipped": bool(monitor_results.get("skipped")),
        "sentinel_synced": 0 if sentinel_sync is None else sentinel_sync.get("synced", 0),
        "sentinel_sync_updated": 0 if sentinel_sync is None else sentinel_sync.get("updated", 0),
        "stalled_emitted": stalled_result.get("emitted", 0),
        "stalled_skipped": bool(stalled_result.get("skipped")),
    }
    payload = {
        "ok": (sentinel_sync is None or sentinel_sync.get("ok", False))
        and monitor_results.get("ok", False)
        and stalled_result.get("ok", False),
        "refresh_summary": refresh_summary,
        "fetch_results": fetch_results,
        "skipped_sources": plan["skipped_sources"],
        "sentinel_sync": sentinel_sync,
        "monitor_results": monitor_results,
        "stalled_result": stalled_result,
        "today_cockpit": today_cockpit,
    }
    out_path = None
    if args.out:
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = paths.root / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json_dumps(payload), encoding="utf-8")
        payload["out_path"] = str(out_path)
    print(json_dumps(payload))
    return 0


def cmd_create_validation_case(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    validation_case_id = args.validation_case_id or make_id("vcase", args.claim_id)
    row: dict[str, Any] = {
        "validation_case_id": validation_case_id,
        "claim_id": args.claim_id,
        "verdict": args.verdict,
        "evidence_artifact_ids_json": json_dumps(args.evidence_artifact_id or []),
    }
    # Optional FK-referenced and nullable fields — only include when non-None
    # to avoid FK constraint failures on NULL references.
    for key, val in [
        ("route_id", args.route_id),
        ("thesis_id", args.thesis_id),
        ("thesis_version_id", args.thesis_version_id),
        ("source_id", args.source_id),
        ("rationale", args.rationale),
        ("validator", args.validator),
        ("validator_model", args.validator_model),
        ("expires_at", args.expires_at),
    ]:
        if val is not None:
            row[key] = val
    insert_row(conn, "validation_cases", row)
    insert_event(
        conn,
        make_id("evt"),
        "validation_case",
        validation_case_id,
        "validation_case_created",
        {
            "claim_id": args.claim_id,
            "route_id": args.route_id,
            "thesis_id": args.thesis_id,
            "verdict": args.verdict,
        },
    )
    conn.commit()
    print(json_dumps({"ok": True, "validation_case_id": validation_case_id}))
    return 0


def cmd_review_claim(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    row = select_one(
        conn,
        "SELECT claim_id, review_metadata_json, review_status FROM claims WHERE claim_id = ?",
        (args.claim_id,),
    )
    if row is None:
        print(json_dumps({"ok": False, "error": f"claim not found: {args.claim_id}"}))
        return 1
    review_metadata = _json_load_dict(row["review_metadata_json"])
    review_metadata.update(
        {
            "reviewer": args.reviewer,
            "review_date": args.review_date or utc_now_iso()[:10],
            "status": args.status,
            "evidence": args.evidence or [],
            "corrections": args.correction or [],
            "note": args.note,
        }
    )
    conn.execute(
        """
        UPDATE claims
        SET review_status = ?, review_metadata_json = ?
        WHERE claim_id = ?
        """,
        (args.status, json_dumps(review_metadata), args.claim_id),
    )
    insert_event(
        conn,
        make_id("evt"),
        "claim",
        args.claim_id,
        "claim_review_recorded",
        {
            "status": args.status,
            "reviewer": args.reviewer,
            "evidence": args.evidence or [],
            "corrections": args.correction or [],
        },
    )
    conn.commit()
    print(
        json_dumps(
            {
                "ok": True,
                "claim_id": args.claim_id,
                "review_status": args.status,
                "review_metadata": review_metadata,
            }
        )
    )
    return 0


def cmd_create_source_viewpoint(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    source_viewpoint_id = args.source_viewpoint_id or make_id("svp", f"{args.source_id}_{args.stance}")
    insert_row(
        conn,
        "source_viewpoints",
        {
            "source_viewpoint_id": source_viewpoint_id,
            "source_id": args.source_id,
            "artifact_id": args.artifact_id,
            "thesis_id": args.thesis_id,
            "target_case_id": args.target_case_id,
            "summary": args.summary,
            "stance": args.stance,
            "horizon_label": args.horizon_label,
            "status": args.status,
            "validation_case_ids_json": json_dumps(args.validation_case_id or []),
            "resolution_review_id": args.resolution_review_id,
        },
    )
    insert_event(
        conn,
        make_id("evt"),
        "source_viewpoint",
        source_viewpoint_id,
        "source_viewpoint_created",
        {
            "source_id": args.source_id,
            "artifact_id": args.artifact_id,
            "thesis_id": args.thesis_id,
            "status": args.status,
        },
    )
    conn.commit()
    print(json_dumps({"ok": True, "source_viewpoint_id": source_viewpoint_id}))
    return 0


def _match_source_viewpoint_candidate(
    workbench: dict[str, Any],
    *,
    source_id: str,
    artifact_id: str,
    thesis_id: str,
) -> dict[str, Any] | None:
    matches = [
        item
        for item in workbench["items"]
        if item["source_id"] == source_id
        and item["artifact_id"] == artifact_id
        and ((thesis_id and item["thesis_id"] == thesis_id) or (not thesis_id))
    ]
    if not matches:
        return None
    if not thesis_id:
        unique_theses = {item["thesis_id"] for item in matches}
        if len(unique_theses) > 1:
            raise AdapterError("multiple thesis candidates matched; please pass --thesis-id")
    return matches[0]


def cmd_synthesize_source_viewpoint(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    try:
        workbench = build_source_viewpoint_workbench(
            conn,
            source_id=args.source_id,
            include_existing=True,
            limit=max(args.limit, 200),
        )
        candidate = _match_source_viewpoint_candidate(
            workbench,
            source_id=args.source_id,
            artifact_id=args.artifact_id,
            thesis_id=args.thesis_id,
        )
        if candidate is None:
            raise AdapterError("no source viewpoint candidate matched the provided source/artifact/thesis")
    except AdapterError as exc:
        print(json_dumps({"ok": False, "error": str(exc)}))
        return 1
    target_case_id = args.target_case_id or candidate.get("suggested_target_case_id") or ""
    source_viewpoint_id = args.source_viewpoint_id or stable_id(
        "svp",
        f"{args.source_id}::{args.artifact_id}::{candidate.get('thesis_id', '')}::{target_case_id}",
    )
    conn.execute(
        """
        DELETE FROM source_viewpoints
        WHERE source_id = ?
          AND artifact_id = ?
          AND COALESCE(thesis_id, '') = COALESCE(?, '')
          AND COALESCE(target_case_id, '') = COALESCE(?, '')
        """,
        (args.source_id, args.artifact_id, candidate.get("thesis_id") or "", target_case_id),
    )
    insert_row(
        conn,
        "source_viewpoints",
        {
            "source_viewpoint_id": source_viewpoint_id,
            "source_id": args.source_id,
            "artifact_id": args.artifact_id,
            "thesis_id": candidate.get("thesis_id") or "",
            "target_case_id": target_case_id,
            "summary": candidate["suggested_summary"],
            "stance": candidate["suggested_stance"],
            "horizon_label": candidate["suggested_horizon_label"],
            "status": candidate["suggested_status"],
            "validation_case_ids_json": json_dumps(candidate.get("validation_case_ids", [])),
            "resolution_review_id": args.resolution_review_id,
        },
    )
    insert_event(
        conn,
        make_id("evt"),
        "source_viewpoint",
        source_viewpoint_id,
        "source_viewpoint_synthesized",
        {
            "source_id": args.source_id,
            "artifact_id": args.artifact_id,
            "thesis_id": candidate.get("thesis_id") or "",
            "target_case_id": target_case_id,
            "status": candidate["suggested_status"],
            "stance": candidate["suggested_stance"],
            "validation_case_ids": candidate.get("validation_case_ids", []),
        },
    )
    insert_row(
        conn,
        "analysis_runs",
        {
            "analysis_run_id": stable_id("run", f"source_viewpoint_synth::{source_viewpoint_id}"),
            "engine": "deterministic_source_viewpoint_synthesizer_v1",
            "input_refs_json": json_dumps(
                [
                    args.source_id,
                    args.artifact_id,
                    candidate.get("thesis_id") or "",
                    *candidate.get("validation_case_ids", []),
                ]
            ),
            "output_ref": source_viewpoint_id,
            "schema_valid": 1,
        },
    )
    conn.commit()
    print(
        json_dumps(
            {
                "ok": True,
                "source_viewpoint_id": source_viewpoint_id,
                "source_id": args.source_id,
                "artifact_id": args.artifact_id,
                "thesis_id": candidate.get("thesis_id") or "",
                "target_case_id": target_case_id or None,
                "status": candidate["suggested_status"],
                "stance": candidate["suggested_stance"],
                "horizon_label": candidate["suggested_horizon_label"],
                "summary": candidate["suggested_summary"],
                "validation_case_ids": candidate.get("validation_case_ids", []),
            }
        )
    )
    return 0


def cmd_create_review(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    review_id = args.review_id or make_id("review", args.owner_object_id)
    insert_row(
        conn,
        "reviews",
        {
            "review_id": review_id,
            "owner_object_type": args.owner_object_type,
            "owner_object_id": args.owner_object_id,
            "review_date": args.review_date,
            "what_we_believed": args.what_we_believed,
            "what_happened": args.what_happened,
            "result": args.result,
            "source_attribution": args.source_attribution,
            "source_ids_json": json_dumps(args.source_id or []),
            "claim_ids_json": json_dumps(args.claim_id or []),
            "lessons": args.lessons,
        },
    )
    insert_event(
        conn,
        make_id("evt"),
        "review",
        review_id,
        "review_created",
        {
            "owner_object_type": args.owner_object_type,
            "owner_object_id": args.owner_object_id,
            "source_ids": args.source_id or [],
            "claim_ids": args.claim_id or [],
        },
    )
    conn.commit()
    print(json_dumps({"ok": True, "review_id": review_id}))
    return 0


def cmd_extract_pattern(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    review = select_one(
        conn,
        """
        SELECT review_id, owner_object_id, lessons
        FROM reviews
        WHERE review_id = ?
        """,
        (args.review_id,),
    )
    if review is None:
        print(json_dumps({"ok": False, "error": f"review not found: {args.review_id}"}))
        return 1
    pattern_id = args.pattern_id or make_id("ptrn", f"{args.review_id}_{args.label or review['owner_object_id']}")
    trigger_terms = args.trigger_term or []
    description = args.description or (review["lessons"] or "").strip()
    if not description:
        print(json_dumps({"ok": False, "error": "extract-pattern requires --description or a review with lessons"}))
        return 1
    thesis_id = args.thesis_id
    if not thesis_id and str(review["owner_object_id"]).startswith("thesis_"):
        thesis_id = review["owner_object_id"]
    label = args.label or description[:48].rstrip()
    insert_row(
        conn,
        "patterns",
        {
            "pattern_id": pattern_id,
            "pattern_kind": args.pattern_kind,
            "label": label,
            "description": description,
            "trigger_terms_json": json_dumps(trigger_terms),
            "source_review_ids_json": json_dumps([args.review_id]),
            "source_thesis_ids_json": json_dumps([thesis_id] if thesis_id else []),
            "status": "active",
        },
    )
    insert_event(
        conn,
        make_id("evt"),
        "pattern",
        pattern_id,
        "pattern_extracted",
        {"review_id": args.review_id, "pattern_kind": args.pattern_kind, "thesis_id": thesis_id, "trigger_terms": trigger_terms},
    )
    conn.commit()
    print(json_dumps({"ok": True, "pattern_id": pattern_id}))
    return 0


def cmd_pattern_library(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_pattern_library(conn, thesis_id=args.thesis_id, limit=args.limit)))
    return 0


def cmd_record_decision(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    target_case = select_one(
        conn,
        """
        SELECT tc.target_case_id, tv.thesis_id, t.title AS thesis_title
        FROM target_cases tc
        JOIN thesis_versions tv ON tv.thesis_version_id = tc.thesis_version_id
        JOIN theses t ON t.thesis_id = tv.thesis_id
        WHERE tc.target_case_id = ?
        """,
        (args.target_case_id,),
    )
    if target_case is None:
        print(json_dumps({"ok": False, "error": f"target_case not found: {args.target_case_id}"}))
        return 1
    thesis_id = target_case["thesis_id"]
    if args.thesis_id and args.thesis_id != thesis_id:
        print(
            json_dumps(
                {
                    "ok": False,
                    "error": f"thesis mismatch for target_case {args.target_case_id}: expected {thesis_id}, got {args.thesis_id}",
                }
            )
        )
        return 1
    if args.review_id and select_one(conn, "SELECT review_id FROM reviews WHERE review_id = ?", (args.review_id,)) is None:
        print(json_dumps({"ok": False, "error": f"review not found: {args.review_id}"}))
        return 1
    for source_id in args.source_id or []:
        if select_one(conn, "SELECT source_id FROM sources WHERE source_id = ?", (source_id,)) is None:
            print(json_dumps({"ok": False, "error": f"source not found: {source_id}"}))
            return 1
    superseded_decision_id = ""
    if args.status == "active":
        existing = select_one(
            conn,
            """
            SELECT decision_id
            FROM operator_decisions
            WHERE target_case_id = ? AND status = 'active'
            ORDER BY decision_date DESC, created_at DESC
            LIMIT 1
            """,
            (args.target_case_id,),
        )
        if existing is not None:
            superseded_decision_id = existing["decision_id"]
            conn.execute(
                "UPDATE operator_decisions SET status = 'superseded' WHERE decision_id = ?",
                (superseded_decision_id,),
            )
    decision_date = args.decision_date or utc_now_iso()[:10]
    decision_id = args.decision_id or make_id("dec", f"{args.target_case_id}_{decision_date}_{args.action_state}")
    insert_row(
        conn,
        "operator_decisions",
        {
            "decision_id": decision_id,
            "target_case_id": args.target_case_id,
            "thesis_id": thesis_id,
            "decision_date": decision_date,
            "action_state": args.action_state,
            "confidence": args.confidence,
            "rationale": args.rationale,
            "source_ids_json": json_dumps(args.source_id or []),
            "review_id": args.review_id or None,
            "status": args.status,
            "supersedes_decision_id": superseded_decision_id or None,
        },
    )
    insert_event(
        conn,
        make_id("evt"),
        "target_case",
        args.target_case_id,
        "operator_decision_recorded",
        {
            "decision_id": decision_id,
            "thesis_id": thesis_id,
            "action_state": args.action_state,
            "status": args.status,
            "supersedes_decision_id": superseded_decision_id or None,
        },
    )
    conn.commit()
    print(
        json_dumps(
            {
                "ok": True,
                "decision_id": decision_id,
                "target_case_id": args.target_case_id,
                "thesis_id": thesis_id,
                "action_state": args.action_state,
                "status": args.status,
                "superseded_decision_id": superseded_decision_id or None,
            }
        )
    )
    return 0


def cmd_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    payload = {
        "sources": select_one(conn, "SELECT COUNT(*) AS n FROM sources", ())["n"],
        "artifacts": select_one(conn, "SELECT COUNT(*) AS n FROM artifacts", ())["n"],
        "claims": select_one(conn, "SELECT COUNT(*) AS n FROM claims", ())["n"],
        "themes": select_one(conn, "SELECT COUNT(*) AS n FROM themes", ())["n"],
        "theses": select_one(conn, "SELECT COUNT(*) AS n FROM theses", ())["n"],
        "targets": select_one(conn, "SELECT COUNT(*) AS n FROM targets", ())["n"],
        "target_cases": select_one(conn, "SELECT COUNT(*) AS n FROM target_cases", ())["n"],
        "timing_plans": select_one(conn, "SELECT COUNT(*) AS n FROM timing_plans", ())["n"],
        "monitors": select_one(conn, "SELECT COUNT(*) AS n FROM monitors", ())["n"],
        "reviews": select_one(conn, "SELECT COUNT(*) AS n FROM reviews", ())["n"],
        "validation_cases": select_one(conn, "SELECT COUNT(*) AS n FROM validation_cases", ())["n"],
        "source_viewpoints": select_one(conn, "SELECT COUNT(*) AS n FROM source_viewpoints", ())["n"],
        "operator_decisions": select_one(conn, "SELECT COUNT(*) AS n FROM operator_decisions", ())["n"],
    }
    active_theses = list_rows(
        conn,
        "SELECT thesis_id, title, status FROM theses WHERE status = 'active' ORDER BY created_at DESC LIMIT 10",
    )
    payload["active_theses"] = [dict(row) for row in active_theses]
    print(json_dumps(payload))
    return 0


def cmd_today_cockpit(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_today_cockpit(conn)))
    return 0


def cmd_integration_snapshot(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    payload = build_integration_snapshot(
        conn,
        scope=args.scope,
        thesis_id=args.thesis_id,
        days=args.days,
        limit=args.limit,
    )
    if not payload.get("ok"):
        print(json_dumps(payload))
        return 1
    print(json_dumps(payload))
    return 0


def cmd_thesis_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_thesis_board(conn)))
    return 0


def cmd_thesis_focus(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    payload = build_thesis_focus(conn, args.thesis_id, limit=args.limit)
    if not payload["summary"]["found"]:
        print(json_dumps({"ok": False, "error": payload.get("error", "thesis_not_found"), "thesis_id": args.thesis_id}))
        return 1
    print(json_dumps(payload))
    return 0


def cmd_voice_memo_triage(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    payload = build_voice_memo_triage(conn, args.artifact_id, limit=args.limit)
    if not payload.get("ok"):
        print(json_dumps(payload))
        return 1
    print(json_dumps(payload))
    return 0


def cmd_theme_map(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_theme_map(conn)))
    return 0


def cmd_watch_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_watch_board(conn)))
    return 0


def cmd_target_case_dashboard(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_target_case_dashboard(conn)))
    return 0


def cmd_decision_dashboard(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_decision_dashboard(conn, days=args.days, limit=args.limit)))
    return 0


def cmd_decision_journal(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(
        json_dumps(
            build_decision_journal(
                conn,
                days=args.days,
                limit=args.limit,
                thesis_id=args.thesis_id,
                target_case_id=args.target_case_id,
            )
        )
    )
    return 0


def cmd_decision_maintenance_queue(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_decision_maintenance_queue(conn, days=args.days, limit=args.limit)))
    return 0


def cmd_intake_inbox(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_intake_inbox(conn)))
    return 0


def cmd_review_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_review_board(conn)))
    return 0


def cmd_review_remediation_queue(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_review_remediation_queue(conn, limit=args.limit)))
    return 0


def cmd_playbook_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_playbook_board(conn)))
    return 0


def cmd_source_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_source_board(conn)))
    return 0


def cmd_source_track_record(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_source_track_record(conn, limit=args.limit)))
    return 0


def cmd_source_feedback_workbench(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_source_feedback_workbench(conn, source_id=args.source_id, limit=args.limit)))
    return 0


def cmd_source_revisit_workbench(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_source_revisit_workbench(conn, limit=args.limit)))
    return 0


def cmd_source_remediation_queue(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_source_remediation_queue(conn, days=args.days, limit=args.limit)))
    return 0


def cmd_verification_remediation_queue(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_verification_remediation_queue(conn, days=args.days, limit=args.limit)))
    return 0


def cmd_verification_remediation_batches(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_verification_remediation_batches(conn, days=args.days, limit=args.limit)))
    return 0


def cmd_record_source_feedback(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    if select_one(conn, "SELECT source_id FROM sources WHERE source_id = ?", (args.source_id,)) is None:
        print(json_dumps({"ok": False, "error": f"source not found: {args.source_id}"}))
        return 1
    if args.source_viewpoint_id and select_one(
        conn, "SELECT source_viewpoint_id FROM source_viewpoints WHERE source_viewpoint_id = ?", (args.source_viewpoint_id,)
    ) is None:
        print(json_dumps({"ok": False, "error": f"source_viewpoint not found: {args.source_viewpoint_id}"}))
        return 1
    if args.review_id and select_one(conn, "SELECT review_id FROM reviews WHERE review_id = ?", (args.review_id,)) is None:
        print(json_dumps({"ok": False, "error": f"review not found: {args.review_id}"}))
        return 1
    if args.validation_case_id and select_one(
        conn, "SELECT validation_case_id FROM validation_cases WHERE validation_case_id = ?", (args.validation_case_id,)
    ) is None:
        print(json_dumps({"ok": False, "error": f"validation_case not found: {args.validation_case_id}"}))
        return 1
    source_feedback_id = args.source_feedback_id or make_id("sfb", f"{args.source_id}_{args.feedback_type}_{args.validation_case_id or args.source_viewpoint_id or args.review_id or utc_now_iso()}")
    weight = SOURCE_FEEDBACK_TYPE_TO_WEIGHT[args.feedback_type]
    insert_row(
        conn,
        "source_feedback_entries",
        {
            "source_feedback_id": source_feedback_id,
            "source_id": args.source_id,
            "source_viewpoint_id": args.source_viewpoint_id or None,
            "review_id": args.review_id or None,
            "validation_case_id": args.validation_case_id or None,
            "feedback_type": args.feedback_type,
            "weight": weight,
            "note": args.note,
            "created_at": args.created_at or utc_now_iso(),
        },
    )
    insert_event(
        conn,
        make_id("evt"),
        "source",
        args.source_id,
        "source_feedback_recorded",
        {
            "source_feedback_id": source_feedback_id,
            "feedback_type": args.feedback_type,
            "weight": weight,
            "source_viewpoint_id": args.source_viewpoint_id,
            "review_id": args.review_id,
            "validation_case_id": args.validation_case_id,
        },
    )
    conn.commit()
    source_track = next((item for item in build_source_track_record(conn, limit=100)["items"] if item["source_id"] == args.source_id), None)
    print(
        json_dumps(
            {
                "ok": True,
                "source_feedback_id": source_feedback_id,
                "source_id": args.source_id,
                "feedback_type": args.feedback_type,
                "weight": weight,
                "source_track_record": source_track,
            }
        )
    )
    return 0


def cmd_source_viewpoint_workbench(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(
        json_dumps(
            build_source_viewpoint_workbench(
                conn,
                source_id=args.source_id,
                include_existing=args.include_existing,
                limit=args.limit,
            )
        )
    )
    return 0


def cmd_validation_board(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_validation_board(conn, verdict=args.verdict, thesis_id=args.thesis_id, source_id=args.source_id, limit=args.limit)))
    return 0


def cmd_weekly_decision_note(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    payload = build_weekly_decision_note(conn, days=args.days, limit=args.limit)
    out_path = None
    if args.out:
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = paths.root / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload["markdown"], encoding="utf-8")
    if args.format == "markdown":
        print(payload["markdown"], end="")
    else:
        response = dict(payload)
        if out_path:
            response["out_path"] = str(out_path)
        print(json_dumps(response))
    return 0


def cmd_promotion_wizard(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    print(json_dumps(build_promotion_wizard(conn)))
    return 0


def cmd_promote_thesis(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    wizard = build_promotion_wizard(conn)
    thesis = next((item for item in wizard["items"] if item["thesis_id"] == args.thesis_id), None)
    if thesis is None:
        print(json_dumps({"ok": False, "error": f"thesis not found: {args.thesis_id}"}))
        return 1
    current_status = thesis["current_status"]
    if args.new_status == "evidence_backed":
        if current_status not in {"seed", "framed"}:
            print(json_dumps({"ok": False, "error": f"thesis status {current_status} cannot promote to evidence_backed"}))
            return 1
        if thesis["recommended_next_status"] != "evidence_backed":
            print(json_dumps({"ok": False, "error": "promotion gate not satisfied for evidence_backed", "missing": thesis["missing"]}))
            return 1
    if args.new_status == "active":
        if current_status != "evidence_backed":
            print(json_dumps({"ok": False, "error": f"thesis status {current_status} cannot promote to active"}))
            return 1
        if thesis["recommended_next_status"] != "active":
            print(json_dumps({"ok": False, "error": "promotion wizard does not recommend active", "missing": thesis["missing"]}))
            return 1
    thesis_row, version_row = _get_thesis_and_version(conn, args.thesis_id)
    thesis_version_id = thesis_row["current_version_id"]
    if args.new_status == "active":
        has_seed_semantics = _thesis_has_seed_semantics(thesis_row, version_row)
        has_reframe = any(
            [
                args.new_title,
                args.new_statement,
                args.new_mechanism_chain,
                args.new_why_now,
                args.new_base_case,
                args.new_counter_case,
                args.new_invalidators,
                args.new_required_followups,
                args.new_human_conviction is not None,
            ]
        )
        if has_seed_semantics and not (args.new_title and args.new_statement):
            print(
                json_dumps(
                    {
                        "ok": False,
                        "error": "active promotion requires reframe for seed-semantic thesis",
                        "required": ["--new-title", "--new-statement"],
                    }
                )
            )
            return 1
        if has_reframe:
            thesis_version_id = args.new_thesis_version_id or make_id("thv", f"{args.thesis_id}_{args.new_status}")
            overrides = {
                "statement": args.new_statement or version_row["statement"],
                "mechanism_chain": args.new_mechanism_chain or version_row["mechanism_chain"],
                "why_now": args.new_why_now or version_row["why_now"],
                "base_case": args.new_base_case or version_row["base_case"],
                "counter_case": args.new_counter_case or version_row["counter_case"],
                "invalidators": args.new_invalidators or version_row["invalidators"],
                "required_followups": args.new_required_followups or version_row["required_followups"],
                "human_conviction": (
                    args.new_human_conviction
                    if args.new_human_conviction is not None
                    else version_row["human_conviction"]
                ),
                "created_from_artifacts_json": version_row["created_from_artifacts_json"],
            }
            _clone_thesis_version(
                conn,
                thesis=thesis_row,
                version=version_row,
                new_version_id=thesis_version_id,
                overrides=overrides,
            )
            conn.execute(
                "UPDATE theses SET title = ?, current_version_id = ? WHERE thesis_id = ?",
                (args.new_title or thesis_row["title"], thesis_version_id, args.thesis_id),
            )
            insert_event(
                conn,
                make_id("evt"),
                "thesis_version",
                thesis_version_id,
                "thesis_reframed_for_promotion",
                {"thesis_id": args.thesis_id, "from_version_id": version_row["thesis_version_id"], "to_status": args.new_status},
            )
    conn.execute("UPDATE theses SET status = ? WHERE thesis_id = ?", (args.new_status, args.thesis_id))
    insert_event(
        conn,
        make_id("evt"),
        "thesis",
        args.thesis_id,
        "thesis_promoted",
        {"from_status": current_status, "to_status": args.new_status, "note": args.note, "thesis_version_id": thesis_version_id},
    )
    conn.commit()
    print(
        json_dumps(
            {
                "ok": True,
                "thesis_id": args.thesis_id,
                "from_status": current_status,
                "to_status": args.new_status,
                "thesis_version_id": thesis_version_id,
                "title": args.new_title or thesis_row["title"],
            }
        )
    )
    return 0


def cmd_remediate_thesis(args: argparse.Namespace) -> int:
    _, paths = _repo_paths(args.root)
    conn = connect(paths.db_path)
    init_db(conn)
    thesis, version = _get_thesis_and_version(conn, args.thesis_id)
    thesis_version_id = thesis["current_version_id"]
    payload: dict[str, Any] = {"ok": True, "thesis_id": args.thesis_id, "thesis_version_id": thesis_version_id, "action": args.action}
    if args.action == "attach_first_hand_artifact":
        if not args.artifact_id:
            print(json_dumps({"ok": False, "error": "attach_first_hand_artifact requires --artifact-id"}))
            return 1
        for artifact_id in args.artifact_id:
            artifact = select_one(
                conn,
                """
                SELECT a.artifact_id, s.primaryness
                FROM artifacts a
                JOIN sources s ON s.source_id = a.source_id
                WHERE a.artifact_id = ?
                """,
                (artifact_id,),
            )
            if artifact is None:
                print(json_dumps({"ok": False, "error": f"artifact not found: {artifact_id}"}))
                return 1
            if artifact["primaryness"] != "first_hand":
                print(json_dumps({"ok": False, "error": f"artifact is not first_hand: {artifact_id}"}))
                return 1
        if args.route_id:
            applied = []
            for route_id in args.route_id:
                applied.append(
                    _apply_route_resolution(
                        conn,
                        route_id=route_id,
                        status="accepted",
                        link_object_type="thesis",
                        link_object_id=args.thesis_id,
                        link_kind="",
                        evidence_artifact_ids=args.artifact_id,
                        note=args.note or "remediation_attach_first_hand_artifact",
                    )
                )
            payload["applied_routes"] = applied
        else:
            payload["attached_artifact_ids"] = _append_artifacts_to_thesis_version(conn, thesis_version_id, args.artifact_id)
        insert_event(conn, make_id("evt"), "thesis", args.thesis_id, "thesis_remediated", {"action": args.action, "artifact_ids": args.artifact_id})
    elif args.action == "add_invalidator":
        if not args.text:
            print(json_dumps({"ok": False, "error": "add_invalidator requires --text"}))
            return 1
        updated = _append_text_field(version.get("invalidators"), args.text)
        conn.execute("UPDATE thesis_versions SET invalidators = ? WHERE thesis_version_id = ?", (updated, thesis_version_id))
        payload["invalidators"] = updated
        insert_event(conn, make_id("evt"), "thesis_version", thesis_version_id, "thesis_invalidator_added", {"text": args.text})
    elif args.action == "add_counter_material":
        updated = version.get("counter_case") or ""
        if args.text:
            updated = _append_text_field(updated, args.text)
            conn.execute("UPDATE thesis_versions SET counter_case = ? WHERE thesis_version_id = ?", (updated, thesis_version_id))
        applied = []
        for route_id in args.route_id:
            applied.append(
                _apply_route_resolution(
                    conn,
                    route_id=route_id,
                    status="accepted",
                    link_object_type="thesis",
                    link_object_id=args.thesis_id,
                    link_kind="feeds",
                    evidence_artifact_ids=args.artifact_id or [],
                    note=args.note or "remediation_add_counter_material",
                )
            )
        if not args.text and not args.route_id:
            print(json_dumps({"ok": False, "error": "add_counter_material requires --text or --route-id"}))
            return 1
        payload["counter_case"] = updated
        if applied:
            payload["applied_routes"] = applied
        insert_event(conn, make_id("evt"), "thesis_version", thesis_version_id, "thesis_counter_material_added", {"text": args.text, "route_ids": args.route_id})
    elif args.action == "create_target_case":
        target_id = args.target_id
        entity_id = args.entity_id
        created: dict[str, str] = {}
        if not target_id:
            if not entity_id:
                if not args.entity_name:
                    print(json_dumps({"ok": False, "error": "create_target_case requires --target-id or --entity-id / --entity-name"}))
                    return 1
                entity_id = make_id("ent", args.entity_name)
                insert_row(
                    conn,
                    "entities",
                    {
                        "entity_id": entity_id,
                        "entity_type": args.entity_type,
                        "canonical_name": args.entity_name,
                        "aliases_json": json_dumps([]),
                        "tickers_or_symbols_json": json_dumps(args.symbol or []),
                        "jurisdiction": args.jurisdiction,
                        "external_ids_json": json_dumps({}),
                    },
                )
                created["entity_id"] = entity_id
            if not args.ticker_or_symbol:
                print(json_dumps({"ok": False, "error": "create_target_case requires --ticker-or-symbol when creating a new target"}))
                return 1
            target_id = args.target_id or make_id("target", args.ticker_or_symbol)
            insert_row(
                conn,
                "targets",
                {
                    "target_id": target_id,
                    "entity_id": entity_id,
                    "asset_class": args.asset_class,
                    "venue": args.venue,
                    "ticker_or_symbol": args.ticker_or_symbol,
                    "currency": args.currency,
                    "liquidity_bucket": args.liquidity_bucket,
                },
            )
            created["target_id"] = target_id
        else:
            existing_target = select_one(conn, "SELECT target_id FROM targets WHERE target_id = ?", (target_id,))
            if existing_target is None:
                print(json_dumps({"ok": False, "error": f"target not found: {target_id}"}))
                return 1
        target_case_id = args.target_case_id or make_id("tc", f"{args.thesis_id}_{target_id}")
        metrics = {}
        for item in args.key_metric or []:
            key, value = item.split("=", 1)
            metrics[key] = value
        insert_row(
            conn,
            "target_cases",
            {
                "target_case_id": target_case_id,
                "thesis_version_id": thesis_version_id,
                "target_id": target_id,
                "exposure_type": args.exposure_type,
                "capture_link_strength": args.capture_link_strength,
                "key_metrics_json": json_dumps(metrics),
                "valuation_context": args.valuation_context,
                "risks": args.risks,
                "status": args.target_case_status,
            },
        )
        created["target_case_id"] = target_case_id
        if args.window_type or args.desired_posture or args.catalyst or args.confirmation_signal or args.precondition or args.invalidator_item:
            timing_plan_id = args.timing_plan_id or make_id("timing", target_case_id)
            insert_row(
                conn,
                "timing_plans",
                {
                    "timing_plan_id": timing_plan_id,
                    "target_case_id": target_case_id,
                    "window_type": args.window_type,
                    "catalysts_json": json_dumps(args.catalyst or []),
                    "confirmation_signals_json": json_dumps(args.confirmation_signal or []),
                    "preconditions_json": json_dumps(args.precondition or []),
                    "invalidators_json": json_dumps(args.invalidator_item or []),
                    "desired_posture": args.desired_posture or "observe",
                },
            )
            created["timing_plan_id"] = timing_plan_id
        if args.monitor_metric_name:
            if not args.monitor_artifact_id:
                print(json_dumps({"ok": False, "error": "create_target_case monitor requires --monitor-artifact-id"}))
                return 1
            if not args.monitor_comparator:
                print(json_dumps({"ok": False, "error": "create_target_case monitor requires --monitor-comparator"}))
                return 1
            if args.monitor_threshold is None:
                print(json_dumps({"ok": False, "error": "create_target_case monitor requires --monitor-threshold"}))
                return 1
            monitor_id = args.monitor_id or make_id("mon", f"{target_case_id}_{args.monitor_metric_name}")
            rule = {"kind": "artifact_metric", "artifact_id": args.monitor_artifact_id, "metric_name": args.monitor_metric_name}
            insert_row(
                conn,
                "monitors",
                {
                    "monitor_id": monitor_id,
                    "owner_object_type": "target_case",
                    "owner_object_id": target_case_id,
                    "monitor_type": args.monitor_type,
                    "metric_name": args.monitor_metric_name,
                    "comparator": args.monitor_comparator,
                    "threshold_value": args.monitor_threshold,
                    "latest_value": None,
                    "query_or_rule": json_dumps(rule),
                    "status": "live",
                },
            )
            created["monitor_id"] = monitor_id
        payload.update(created)
        insert_event(conn, make_id("evt"), "thesis", args.thesis_id, "thesis_target_case_created_via_remediation", created)
    else:
        print(json_dumps({"ok": False, "error": f"unsupported remediation action: {args.action}"}))
        return 1
    conn.commit()
    payload["promotion_wizard"] = next(
        (item for item in build_promotion_wizard(conn)["items"] if item["thesis_id"] == args.thesis_id),
        None,
    )
    print(json_dumps(payload))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="finagent")
    parser.add_argument("--root", default="")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("create-source")
    p.add_argument("--source-id", default="")
    p.add_argument("--source-type", required=True, choices=SOURCE_TYPES)
    p.add_argument("--name", required=True)
    p.add_argument("--primaryness", required=True, choices=PRIMARYNESS_CHOICES)
    p.add_argument("--jurisdiction", default="")
    p.add_argument("--language", default="")
    p.add_argument("--base-uri", default="")
    p.add_argument("--credibility-policy", default="")
    p.set_defaults(func=cmd_create_source)

    p = sub.add_parser("ingest-text")
    p.add_argument("--source-id", required=True)
    p.add_argument("--source-type", required=True, choices=SOURCE_TYPES)
    p.add_argument("--source-name", required=True)
    p.add_argument("--primaryness", required=True, choices=PRIMARYNESS_CHOICES)
    p.add_argument("--path", required=True)
    p.add_argument("--artifact-id", default="")
    p.add_argument("--artifact-kind", default="text_note", choices=ARTIFACT_KINDS)
    p.add_argument("--title", default="")
    p.add_argument("--published-at", default="")
    p.add_argument("--language", default="zh")
    p.add_argument("--uri", default="")
    p.add_argument("--jurisdiction", default="")
    p.add_argument("--base-uri", default="")
    p.set_defaults(func=cmd_ingest_text)

    p = sub.add_parser("fetch-sec-submissions")
    p.add_argument("--ticker", required=True)
    p.set_defaults(func=cmd_fetch_sec_submissions)

    p = sub.add_parser("fetch-openalex")
    p.add_argument("--query", required=True)
    p.add_argument("--per-page", type=int, default=5)
    p.set_defaults(func=cmd_fetch_openalex)

    p = sub.add_parser("fetch-defillama")
    p.add_argument("--slug", required=True)
    p.set_defaults(func=cmd_fetch_defillama)

    p = sub.add_parser("fetch-cninfo-announcements")
    p.add_argument("--ticker", required=True)
    p.add_argument("--search-key", default="")
    p.add_argument("--lookback-days", type=int, default=45)
    p.add_argument("--limit", type=int, default=5)
    p.set_defaults(func=cmd_fetch_cninfo_announcements)

    p = sub.add_parser("market-snapshot")
    p.add_argument("--ticker", required=True)
    p.add_argument("--market", default="CN", choices=["CN", "HK", "US"])
    p.set_defaults(func=cmd_market_snapshot)

    p = sub.add_parser("market-screen")
    p.add_argument("--market", default="CN", choices=["CN", "HK", "US"])
    p.add_argument("--strategy", default="value", choices=["value", "momentum", "growth", "contrarian"])
    p.add_argument("--limit", type=int, default=15)
    p.add_argument("--min-market-cap", type=float, default=10.0)
    p.add_argument("--all-markets", action="store_true")
    p.set_defaults(func=cmd_market_screen)

    p = sub.add_parser("consensus")
    p.add_argument("--ticker", required=True)
    p.add_argument("--market", default="CN", choices=["CN", "HK", "US"])
    p.set_defaults(func=cmd_consensus)

    p = sub.add_parser("writeback")
    p.add_argument("--type", required=True, choices=["claim_outcome", "source_feedback", "expression_outcome"])
    p.add_argument("--payload", required=True, help="JSON payload string")
    p.add_argument("--writeback-dir", default="", help="Override writeback directory")
    p.set_defaults(func=cmd_writeback)

    p = sub.add_parser("transcribe-homepc-funasr")
    p.add_argument("--audio-path", required=True)
    p.add_argument("--artifact-id", default="")
    p.add_argument("--title", default="")
    p.add_argument("--published-at", default="")
    p.add_argument("--artifact-kind", default="audio_transcript", choices=ARTIFACT_KINDS)
    p.add_argument("--source-id", default="")
    p.add_argument("--source-type", default="personal", choices=SOURCE_TYPES)
    p.add_argument("--source-name", default="Home PC FunASR")
    p.add_argument("--primaryness", default="personal", choices=PRIMARYNESS_CHOICES)
    p.add_argument("--jurisdiction", default="")
    p.add_argument("--language", default="zh")
    p.add_argument("--host", default="yuanhaizhou@192.168.1.17")
    p.add_argument("--env-name", default="soulxpodcast")
    p.add_argument("--modelscope-cache", default="/home/yuanhaizhou/funasr_models")
    p.add_argument("--remote-root-base", default="/home/yuanhaizhou/finagent-runtime/homepc-funasr")
    p.add_argument("--device", default="cpu", choices=["cpu", "cuda"])
    p.add_argument("--timeout-seconds", type=int, default=180)
    p.add_argument("--cleanup-remote", action="store_true")
    p.set_defaults(func=cmd_transcribe_homepc_funasr)

    p = sub.add_parser("intake-voice-memo-audio")
    p.add_argument("--audio-path", required=True)
    p.add_argument("--artifact-id", default="")
    p.add_argument("--title", default="")
    p.add_argument("--published-at", default="")
    p.add_argument("--artifact-kind", default="audio_transcript", choices=ARTIFACT_KINDS)
    p.add_argument("--source-id", default="")
    p.add_argument("--source-type", default="personal", choices=SOURCE_TYPES)
    p.add_argument("--source-name", default="Voice Memo Inbox")
    p.add_argument("--primaryness", default="personal", choices=PRIMARYNESS_CHOICES)
    p.add_argument("--jurisdiction", default="")
    p.add_argument("--language", default="zh")
    p.add_argument("--host", default="yuanhaizhou@192.168.1.17")
    p.add_argument("--env-name", default="soulxpodcast")
    p.add_argument("--modelscope-cache", default="/home/yuanhaizhou/funasr_models")
    p.add_argument("--remote-root-base", default="/home/yuanhaizhou/finagent-runtime/homepc-funasr")
    p.add_argument("--device", default="cpu", choices=["cpu", "cuda"])
    p.add_argument("--timeout-seconds", type=int, default=180)
    p.add_argument("--cleanup-remote", action="store_true")
    p.add_argument("--speaker", default="user_memo")
    p.add_argument("--min-chars", type=int, default=18)
    p.set_defaults(func=cmd_intake_voice_memo_audio)

    p = sub.add_parser("intake-kol-digest")
    p.add_argument("--path", required=True)
    p.add_argument("--artifact-id", default="")
    p.add_argument("--title", default="")
    p.add_argument("--published-at", default="")
    p.add_argument("--artifact-kind", default="video_digest", choices=ARTIFACT_KINDS)
    p.add_argument("--source-id", default="")
    p.add_argument("--source-type", default="kol", choices=SOURCE_TYPES)
    p.add_argument("--source-name", default="KOL Digest")
    p.add_argument("--primaryness", default="second_hand", choices=PRIMARYNESS_CHOICES)
    p.add_argument("--jurisdiction", default="")
    p.add_argument("--language", default="zh")
    p.add_argument("--uri", default="")
    p.add_argument("--base-uri", default="")
    p.add_argument("--speaker", default="kol")
    p.add_argument("--min-chars", type=int, default=18)
    p.set_defaults(func=cmd_intake_kol_digest)

    p = sub.add_parser("extract-claims")
    p.add_argument("--artifact-id", required=True)
    p.add_argument("--speaker", default="")
    p.add_argument("--min-chars", type=int, default=20)
    p.set_defaults(func=cmd_extract_claims)

    p = sub.add_parser("route-claims")
    p.add_argument("--artifact-id", required=True)
    p.set_defaults(func=cmd_route_claims)

    p = sub.add_parser("set-route-status")
    p.add_argument("--route-id", required=True)
    p.add_argument("--status", required=True, choices=ROUTE_STATUS_CHOICES)
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_set_route_status)

    p = sub.add_parser("set-route-status-batch")
    p.add_argument("--route-id", action="append", default=[])
    p.add_argument("--status", required=True, choices=ROUTE_STATUS_CHOICES)
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_set_route_status_batch)

    p = sub.add_parser("apply-route")
    p.add_argument("--route-id", required=True)
    p.add_argument("--status", default="accepted", choices=ROUTE_STATUS_CHOICES)
    p.add_argument("--link-object-type", default="", choices=["", *ROUTE_LINK_OBJECT_TYPES])
    p.add_argument("--link-object-id", default="")
    p.add_argument("--link-kind", default="", choices=["", *ROUTE_LINK_KIND_CHOICES])
    p.add_argument("--evidence-artifact-id", action="append", default=[])
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_apply_route)

    p = sub.add_parser("apply-route-batch")
    p.add_argument("--route-id", action="append", default=[])
    p.add_argument("--status", default="accepted", choices=ROUTE_STATUS_CHOICES)
    p.add_argument("--link-object-type", default="", choices=["", *ROUTE_LINK_OBJECT_TYPES])
    p.add_argument("--link-object-id", default="")
    p.add_argument("--link-kind", default="", choices=["", *ROUTE_LINK_KIND_CHOICES])
    p.add_argument("--evidence-artifact-id", action="append", default=[])
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_apply_route_batch)

    p = sub.add_parser("bulk-apply-routes")
    p.add_argument("--artifact-id", default="")
    p.add_argument("--route-type", default="", choices=["", *ROUTE_TYPE_CHOICES])
    p.add_argument("--source-id", default="")
    p.add_argument("--candidate-thesis-id", default="")
    p.add_argument("--from-status", default="pending", choices=ROUTE_STATUS_CHOICES)
    p.add_argument("--new-status", default="accepted", choices=ROUTE_STATUS_CHOICES)
    p.add_argument("--link-object-type", default="", choices=["", *ROUTE_LINK_OBJECT_TYPES])
    p.add_argument("--link-object-id", default="")
    p.add_argument("--link-kind", default="", choices=["", *ROUTE_LINK_KIND_CHOICES])
    p.add_argument("--evidence-artifact-id", action="append", default=[])
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_bulk_apply_routes)

    p = sub.add_parser("routing-board")
    p.set_defaults(func=cmd_routing_board)

    p = sub.add_parser("route-workbench")
    p.add_argument("--status", default="pending", choices=ROUTE_STATUS_CHOICES)
    p.add_argument("--route-type", default="", choices=["", *ROUTE_TYPE_CHOICES])
    p.add_argument("--source-id", default="")
    p.add_argument("--thesis-id", default="")
    p.add_argument("--limit", type=int, default=80)
    p.set_defaults(func=cmd_route_workbench)

    p = sub.add_parser("route-normalization-queue")
    p.add_argument("--limit", type=int, default=80)
    p.set_defaults(func=cmd_route_normalization_queue)

    p = sub.add_parser("corroboration-queue")
    p.add_argument("--status", default="pending", choices=ROUTE_STATUS_CHOICES)
    p.set_defaults(func=cmd_corroboration_queue)

    p = sub.add_parser("thesis-gate-report")
    p.add_argument("--thesis-id", default="")
    p.set_defaults(func=cmd_thesis_gate_report)

    p = sub.add_parser("create-entity")
    p.add_argument("--entity-id", default="")
    p.add_argument("--entity-type", required=True, choices=ENTITY_TYPES)
    p.add_argument("--name", required=True)
    p.add_argument("--alias", action="append", default=[])
    p.add_argument("--symbol", action="append", default=[])
    p.add_argument("--jurisdiction", default="")
    p.set_defaults(func=cmd_create_entity)

    p = sub.add_parser("create-theme")
    p.add_argument("--theme-id", default="")
    p.add_argument("--name", required=True)
    p.add_argument("--why-it-matters", default="")
    p.add_argument("--maturity-stage", default="")
    p.add_argument("--commercialization-paths", default="")
    p.add_argument("--importance-status", default="tracking", choices=THEME_IMPORTANCE_CHOICES)
    p.set_defaults(func=cmd_create_theme)

    p = sub.add_parser("create-thesis")
    p.add_argument("--thesis-id", default="")
    p.add_argument("--thesis-version-id", default="")
    p.add_argument("--title", required=True)
    p.add_argument("--status", default="framed", choices=THESIS_STATUS_CHOICES)
    p.add_argument("--horizon-months", type=int, default=12)
    p.add_argument("--theme-id", action="append", default=[])
    p.add_argument("--artifact-id", action="append", default=[])
    p.add_argument("--owner", default="human")
    p.add_argument("--statement", required=True)
    p.add_argument("--mechanism-chain", required=True)
    p.add_argument("--why-now", default="")
    p.add_argument("--base-case", default="")
    p.add_argument("--counter-case", default="")
    p.add_argument("--invalidators", default="")
    p.add_argument("--required-followups", default="")
    p.add_argument("--human-conviction", type=float, default=0.6)
    p.set_defaults(func=cmd_create_thesis)

    p = sub.add_parser("create-target")
    p.add_argument("--target-id", default="")
    p.add_argument("--entity-id", required=True)
    p.add_argument("--asset-class", required=True, choices=TARGET_ASSET_CLASSES)
    p.add_argument("--venue", default="")
    p.add_argument("--ticker-or-symbol", required=True)
    p.add_argument("--currency", default="")
    p.add_argument("--liquidity-bucket", default="")
    p.set_defaults(func=cmd_create_target)

    p = sub.add_parser("create-target-case")
    p.add_argument("--target-case-id", default="")
    p.add_argument("--thesis-version-id", required=True)
    p.add_argument("--target-id", required=True)
    p.add_argument("--exposure-type", default="direct", choices=EXPOSURE_TYPE_CHOICES)
    p.add_argument("--capture-link-strength", type=float, default=0.7)
    p.add_argument("--key-metric", action="append", default=[])
    p.add_argument("--valuation-context", default="")
    p.add_argument("--risks", default="")
    p.add_argument("--status", default="actionable", choices=TARGET_CASE_STATUS_CHOICES)
    p.set_defaults(func=cmd_create_target_case)

    p = sub.add_parser("create-timing-plan")
    p.add_argument("--timing-plan-id", default="")
    p.add_argument("--target-case-id", required=True)
    p.add_argument("--window-type", default="")
    p.add_argument("--catalyst", action="append", default=[])
    p.add_argument("--confirmation-signal", action="append", default=[])
    p.add_argument("--precondition", action="append", default=[])
    p.add_argument("--invalidator", action="append", default=[])
    p.add_argument("--desired-posture", default="observe", choices=DESIRED_POSTURE_CHOICES)
    p.set_defaults(func=cmd_create_timing_plan)

    p = sub.add_parser("create-monitor")
    p.add_argument("--monitor-id", default="")
    p.add_argument("--owner-object-type", required=True)
    p.add_argument("--owner-object-id", required=True)
    p.add_argument("--monitor-type", required=True, choices=MONITOR_TYPE_CHOICES)
    p.add_argument("--artifact-id", default="")
    p.add_argument("--metric-name", default="")
    p.add_argument("--comparator", choices=MONITOR_COMPARATOR_CHOICES, required=True)
    p.add_argument("--threshold-value", type=float, required=True)
    p.set_defaults(func=cmd_create_monitor)

    p = sub.add_parser("run-monitors")
    p.set_defaults(func=cmd_run_monitors)

    p = sub.add_parser("sentinel-validate")
    p.add_argument("--spec", default="")
    p.set_defaults(func=cmd_sentinel_validate)

    p = sub.add_parser("sentinel-sync")
    p.add_argument("--spec", default="")
    p.set_defaults(func=cmd_sentinel_sync)

    p = sub.add_parser("sentinel-check-stalls")
    p.add_argument("--as-of", default="")
    p.set_defaults(func=cmd_sentinel_check_stalls)

    p = sub.add_parser("event-prompt")
    p.add_argument("--path", required=True)
    p.add_argument("--spec", default="")
    p.add_argument("--no-context", action="store_true")
    p.set_defaults(func=cmd_event_prompt)

    p = sub.add_parser("event-validate")
    p.add_argument("--path", required=True)
    p.add_argument("--include-normalized", action="store_true")
    p.set_defaults(func=cmd_event_validate)

    p = sub.add_parser("event-route-validate")
    p.add_argument("--path", required=True)
    p.add_argument("--spec", default="")
    p.add_argument("--skip-sentinel-sync", action="store_true")
    p.add_argument("--with-fixtures", action="store_true")
    p.set_defaults(func=cmd_event_route_validate)

    p = sub.add_parser("event-import")
    p.add_argument("--path", required=True)
    p.add_argument("--spec", default="")
    p.add_argument("--skip-sentinel-sync", action="store_true")
    p.add_argument("--emit-stalls", action="store_true")
    p.add_argument("--as-of", default="")
    p.set_defaults(func=cmd_event_import)

    p = sub.add_parser("event-ledger")
    p.add_argument("--route", default="", choices=["", "interrupt", "review", "opportunity", "archive"])
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_event_ledger)

    p = sub.add_parser("sentinel-board")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_sentinel_board)

    p = sub.add_parser("opportunity-inbox")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_opportunity_inbox)

    p = sub.add_parser("theme-radar-board")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_theme_radar_board)

    p = sub.add_parser("anti-thesis-board")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_anti_thesis_board)

    p = sub.add_parser("anti-thesis-prompt")
    p.add_argument("--object-type", required=True, choices=["projection", "candidate"])
    p.add_argument("--object-id", required=True)
    p.set_defaults(func=cmd_anti_thesis_prompt)

    p = sub.add_parser("anti-thesis-log")
    p.add_argument("--object-type", required=True, choices=["projection", "candidate"])
    p.add_argument("--object-id", required=True)
    p.add_argument("--verdict", required=True, choices=["resolved", "dismiss", "defer"])
    p.add_argument("--result-summary", required=True)
    p.add_argument("--contradiction-score", type=float, default=None)
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_anti_thesis_log)

    p = sub.add_parser("event-feedback-record")
    p.add_argument("--object-type", required=True, choices=["projection", "candidate", "event"])
    p.add_argument("--object-id", required=True)
    p.add_argument("--feedback-type", required=True, choices=sorted(FEEDBACK_TYPES))
    p.add_argument("--verdict", required=True, choices=sorted(FEEDBACK_VERDICTS))
    p.add_argument("--score", type=float, default=None)
    p.add_argument("--note", default="")
    p.add_argument("--related-event-id", default="")
    p.add_argument("--related-candidate-id", default="")
    p.set_defaults(func=cmd_event_feedback_record)

    p = sub.add_parser("event-evaluation-board")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_event_evaluation_board)

    p = sub.add_parser("event-run-board")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_event_run_board)

    p = sub.add_parser("event-run-compare")
    p.add_argument("--run-id", action="append", default=[])
    p.set_defaults(func=cmd_event_run_compare)

    p = sub.add_parser("event-source-policy")
    p.set_defaults(func=cmd_event_source_policy)

    p = sub.add_parser("event-sector-grammars")
    p.set_defaults(func=cmd_event_sector_grammars)

    p = sub.add_parser("event-source-adapters")
    p.set_defaults(func=cmd_event_source_adapters)

    p = sub.add_parser("event-replay-validate")
    p.add_argument("--spec", required=True)
    p.add_argument("--events", required=True)
    p.add_argument("--as-of", required=True)
    p.add_argument("--theme-slug", required=True)
    p.add_argument("--reference-run-root", default="")
    p.set_defaults(func=cmd_event_replay_validate)

    p = sub.add_parser("daily-refresh")
    p.add_argument("--skip-fetch", action="store_true")
    p.add_argument("--skip-monitors", action="store_true")
    p.add_argument("--skip-sentinel-sync", action="store_true")
    p.add_argument("--skip-stalls", action="store_true")
    p.add_argument("--spec", default="")
    p.add_argument("--as-of", default="")
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--out", default="")
    p.set_defaults(func=cmd_daily_refresh)

    p = sub.add_parser("create-validation-case")
    p.add_argument("--validation-case-id", default="")
    p.add_argument("--route-id", default="")
    p.add_argument("--claim-id", required=True)
    p.add_argument("--thesis-id", required=True)
    p.add_argument("--thesis-version-id", default="")
    p.add_argument("--source-id", default="")
    p.add_argument("--verdict", required=True, choices=VALIDATION_VERDICT_CHOICES)
    p.add_argument("--evidence-artifact-id", action="append", default=[])
    p.add_argument("--rationale", default="")
    p.add_argument("--validator", default="human")
    p.add_argument("--validator-model", default="")
    p.add_argument("--expires-at", default="")
    p.set_defaults(func=cmd_create_validation_case)

    p = sub.add_parser("review-claim")
    p.add_argument("--claim-id", required=True)
    p.add_argument("--status", required=True, choices=CLAIM_REVIEW_STATUS_CHOICES)
    p.add_argument("--reviewer", default="human")
    p.add_argument("--review-date", default="")
    p.add_argument("--evidence", action="append", default=[])
    p.add_argument("--correction", action="append", default=[])
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_review_claim)

    p = sub.add_parser("create-source-viewpoint")
    p.add_argument("--source-viewpoint-id", default="")
    p.add_argument("--source-id", required=True)
    p.add_argument("--artifact-id", required=True)
    p.add_argument("--thesis-id", default="")
    p.add_argument("--target-case-id", default="")
    p.add_argument("--summary", required=True)
    p.add_argument("--stance", required=True, choices=VIEWPOINT_STANCE_CHOICES)
    p.add_argument("--horizon-label", default="")
    p.add_argument("--status", default="open", choices=VIEWPOINT_STATUS_CHOICES)
    p.add_argument("--validation-case-id", action="append", default=[])
    p.add_argument("--resolution-review-id", default="")
    p.set_defaults(func=cmd_create_source_viewpoint)

    p = sub.add_parser("synthesize-source-viewpoint")
    p.add_argument("--source-viewpoint-id", default="")
    p.add_argument("--source-id", required=True)
    p.add_argument("--artifact-id", required=True)
    p.add_argument("--thesis-id", default="")
    p.add_argument("--target-case-id", default="")
    p.add_argument("--resolution-review-id", default="")
    p.add_argument("--limit", type=int, default=200)
    p.set_defaults(func=cmd_synthesize_source_viewpoint)

    p = sub.add_parser("create-review")
    p.add_argument("--review-id", default="")
    p.add_argument("--owner-object-type", required=True, choices=REVIEW_OWNER_OBJECT_TYPES)
    p.add_argument("--owner-object-id", required=True)
    p.add_argument("--review-date", required=True)
    p.add_argument("--what-we-believed", default="")
    p.add_argument("--what-happened", default="")
    p.add_argument("--result", required=True, choices=REVIEW_RESULT_CHOICES)
    p.add_argument("--source-attribution", default="")
    p.add_argument("--source-id", action="append", default=[])
    p.add_argument("--claim-id", action="append", default=[])
    p.add_argument("--lessons", default="")
    p.set_defaults(func=cmd_create_review)

    p = sub.add_parser("extract-pattern")
    p.add_argument("--pattern-id", default="")
    p.add_argument("--review-id", required=True)
    p.add_argument("--pattern-kind", default="lesson", choices=["lesson", "source", "promotion_gap"])
    p.add_argument("--label", default="")
    p.add_argument("--description", default="")
    p.add_argument("--trigger-term", action="append", default=[])
    p.add_argument("--thesis-id", default="")
    p.set_defaults(func=cmd_extract_pattern)

    p = sub.add_parser("record-decision")
    p.add_argument("--decision-id", default="")
    p.add_argument("--target-case-id", required=True)
    p.add_argument("--thesis-id", default="")
    p.add_argument("--decision-date", default="")
    p.add_argument("--action-state", required=True, choices=DECISION_ACTION_CHOICES)
    p.add_argument("--confidence", type=float, default=None)
    p.add_argument("--rationale", default="")
    p.add_argument("--source-id", action="append", default=[])
    p.add_argument("--review-id", default="")
    p.add_argument("--status", default="active", choices=DECISION_STATUS_CHOICES)
    p.set_defaults(func=cmd_record_decision)

    p = sub.add_parser("board")
    p.set_defaults(func=cmd_board)

    p = sub.add_parser("today-cockpit")
    p.set_defaults(func=cmd_today_cockpit)

    p = sub.add_parser("integration-snapshot")
    p.add_argument("--scope", default="today", choices=["today", "thesis", "weekly"])
    p.add_argument("--thesis-id", default="")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--limit", type=int, default=6)
    p.set_defaults(func=cmd_integration_snapshot)

    p = sub.add_parser("daily")
    p.add_argument("--skip-fetch", action="store_true")
    p.add_argument("--skip-monitors", action="store_true")
    p.add_argument("--skip-sentinel-sync", action="store_true")
    p.add_argument("--skip-stalls", action="store_true")
    p.add_argument("--spec", default="")
    p.add_argument("--as-of", default="")
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--out", default="")
    p.set_defaults(func=cmd_daily_refresh)

    p = sub.add_parser("thesis-board")
    p.set_defaults(func=cmd_thesis_board)

    p = sub.add_parser("thesis-focus")
    p.add_argument("--thesis-id", required=True)
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_thesis_focus)

    p = sub.add_parser("voice-memo-triage")
    p.add_argument("--artifact-id", required=True)
    p.add_argument("--limit", type=int, default=40)
    p.set_defaults(func=cmd_voice_memo_triage)

    p = sub.add_parser("focus")
    p.add_argument("--thesis-id", required=True)
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_thesis_focus)

    p = sub.add_parser("theme-map")
    p.set_defaults(func=cmd_theme_map)

    p = sub.add_parser("watch-board")
    p.set_defaults(func=cmd_watch_board)

    p = sub.add_parser("target-case-dashboard")
    p.set_defaults(func=cmd_target_case_dashboard)

    p = sub.add_parser("decision-dashboard")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--limit", type=int, default=12)
    p.set_defaults(func=cmd_decision_dashboard)

    p = sub.add_parser("decision-journal")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--thesis-id", default="")
    p.add_argument("--target-case-id", default="")
    p.set_defaults(func=cmd_decision_journal)

    p = sub.add_parser("decision-maintenance-queue")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_decision_maintenance_queue)

    p = sub.add_parser("intake-inbox")
    p.set_defaults(func=cmd_intake_inbox)

    p = sub.add_parser("review-board")
    p.set_defaults(func=cmd_review_board)

    p = sub.add_parser("review-remediation-queue")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_review_remediation_queue)

    p = sub.add_parser("playbook-board")
    p.set_defaults(func=cmd_playbook_board)

    p = sub.add_parser("pattern-library")
    p.add_argument("--thesis-id", default="")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_pattern_library)

    p = sub.add_parser("source-board")
    p.set_defaults(func=cmd_source_board)

    p = sub.add_parser("source-track-record")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_source_track_record)

    p = sub.add_parser("source-feedback-workbench")
    p.add_argument("--source-id", default="")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_source_feedback_workbench)

    p = sub.add_parser("source-revisit-workbench")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_source_revisit_workbench)

    p = sub.add_parser("source-remediation-queue")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_source_remediation_queue)

    p = sub.add_parser("verification-remediation-queue")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_verification_remediation_queue)

    p = sub.add_parser("verification-remediation-batches")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_verification_remediation_batches)

    p = sub.add_parser("record-source-feedback")
    p.add_argument("--source-feedback-id", default="")
    p.add_argument("--source-id", required=True)
    p.add_argument("--source-viewpoint-id", default="")
    p.add_argument("--review-id", default="")
    p.add_argument("--validation-case-id", default="")
    p.add_argument("--feedback-type", required=True, choices=sorted(SOURCE_FEEDBACK_TYPE_TO_WEIGHT))
    p.add_argument("--note", default="")
    p.add_argument("--created-at", default="")
    p.set_defaults(func=cmd_record_source_feedback)

    p = sub.add_parser("source-viewpoint-workbench")
    p.add_argument("--source-id", default="")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--include-existing", action="store_true")
    p.set_defaults(func=cmd_source_viewpoint_workbench)

    p = sub.add_parser("validation-board")
    p.add_argument("--verdict", default="", choices=["", *VALIDATION_VERDICT_CHOICES])
    p.add_argument("--thesis-id", default="")
    p.add_argument("--source-id", default="")
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=cmd_validation_board)

    p = sub.add_parser("weekly-decision-note")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--limit", type=int, default=8)
    p.add_argument("--out", default="")
    p.add_argument("--format", default="json", choices=["json", "markdown"])
    p.set_defaults(func=cmd_weekly_decision_note)

    p = sub.add_parser("weekly")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--limit", type=int, default=8)
    p.add_argument("--out", default="")
    p.add_argument("--format", default="json", choices=["json", "markdown"])
    p.set_defaults(func=cmd_weekly_decision_note)

    p = sub.add_parser("promotion-wizard")
    p.set_defaults(func=cmd_promotion_wizard)

    p = sub.add_parser("remediate-thesis")
    p.add_argument("--thesis-id", required=True)
    p.add_argument(
        "--action",
        required=True,
        choices=["attach_first_hand_artifact", "add_invalidator", "add_counter_material", "create_target_case"],
    )
    p.add_argument("--route-id", action="append", default=[])
    p.add_argument("--artifact-id", action="append", default=[])
    p.add_argument("--text", default="")
    p.add_argument("--note", default="")
    p.add_argument("--target-id", default="")
    p.add_argument("--entity-id", default="")
    p.add_argument("--entity-name", default="")
    p.add_argument("--entity-type", default="company", choices=ENTITY_TYPES)
    p.add_argument("--symbol", action="append", default=[])
    p.add_argument("--jurisdiction", default="")
    p.add_argument("--asset-class", default="other", choices=TARGET_ASSET_CLASSES)
    p.add_argument("--venue", default="")
    p.add_argument("--ticker-or-symbol", default="")
    p.add_argument("--currency", default="")
    p.add_argument("--liquidity-bucket", default="")
    p.add_argument("--target-case-id", default="")
    p.add_argument("--exposure-type", default="direct", choices=EXPOSURE_TYPE_CHOICES)
    p.add_argument("--capture-link-strength", type=float, default=0.7)
    p.add_argument("--key-metric", action="append", default=[])
    p.add_argument("--valuation-context", default="")
    p.add_argument("--risks", default="")
    p.add_argument("--target-case-status", default="candidate", choices=TARGET_CASE_STATUS_CHOICES)
    p.add_argument("--timing-plan-id", default="")
    p.add_argument("--window-type", default="")
    p.add_argument("--desired-posture", default="", choices=["", *DESIRED_POSTURE_CHOICES])
    p.add_argument("--catalyst", action="append", default=[])
    p.add_argument("--confirmation-signal", action="append", default=[])
    p.add_argument("--precondition", action="append", default=[])
    p.add_argument("--invalidator-item", action="append", default=[])
    p.add_argument("--monitor-id", default="")
    p.add_argument("--monitor-type", default="official", choices=MONITOR_TYPE_CHOICES)
    p.add_argument("--monitor-artifact-id", default="")
    p.add_argument("--monitor-metric-name", default="")
    p.add_argument("--monitor-comparator", default="", choices=["", *MONITOR_COMPARATOR_CHOICES])
    p.add_argument("--monitor-threshold", type=float)
    p.set_defaults(func=cmd_remediate_thesis)

    p = sub.add_parser("promote-thesis")
    p.add_argument("--thesis-id", required=True)
    p.add_argument("--new-status", required=True, choices=["evidence_backed", "active"])
    p.add_argument("--note", default="")
    p.add_argument("--new-thesis-version-id", default="")
    p.add_argument("--new-title", default="")
    p.add_argument("--new-statement", default="")
    p.add_argument("--new-mechanism-chain", default="")
    p.add_argument("--new-why-now", default="")
    p.add_argument("--new-base-case", default="")
    p.add_argument("--new-counter-case", default="")
    p.add_argument("--new-invalidators", default="")
    p.add_argument("--new-required-followups", default="")
    p.add_argument("--new-human-conviction", type=float)
    p.set_defaults(func=cmd_promote_thesis)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
