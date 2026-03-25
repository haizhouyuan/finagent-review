from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .utils import json_dumps, utc_now_iso


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
  source_id TEXT PRIMARY KEY,
  source_type TEXT NOT NULL,
  name TEXT NOT NULL,
  primaryness TEXT NOT NULL,
  jurisdiction TEXT,
  language TEXT,
  base_uri TEXT,
  credibility_policy TEXT,
  track_record_stats_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
  artifact_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(source_id),
  artifact_kind TEXT NOT NULL,
  title TEXT NOT NULL,
  captured_at TEXT NOT NULL,
  published_at TEXT,
  language TEXT,
  uri TEXT,
  raw_path TEXT,
  normalized_text_path TEXT,
  content_hash TEXT,
  status TEXT NOT NULL,
  metadata_json TEXT,
  created_at TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS artifact_fts USING fts5(
  artifact_id UNINDEXED,
  title,
  content
);

CREATE TABLE IF NOT EXISTS claims (
  claim_id TEXT PRIMARY KEY,
  artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
  speaker TEXT,
  timecode_or_span TEXT,
  claim_text TEXT NOT NULL,
  claim_type TEXT NOT NULL,
  confidence REAL NOT NULL,
  linked_entity_ids_json TEXT,
  data_date TEXT,
  review_status TEXT NOT NULL DEFAULT 'unreviewed',
  review_metadata_json TEXT,
  domain_check_json TEXT,
  freshness_status TEXT NOT NULL DEFAULT 'unknown',
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entities (
  entity_id TEXT PRIMARY KEY,
  entity_type TEXT NOT NULL,
  canonical_name TEXT NOT NULL,
  aliases_json TEXT,
  tickers_or_symbols_json TEXT,
  jurisdiction TEXT,
  external_ids_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS themes (
  theme_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  why_it_matters TEXT,
  maturity_stage TEXT,
  commercialization_paths TEXT,
  importance_status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS theses (
  thesis_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  status TEXT NOT NULL,
  horizon_months INTEGER NOT NULL,
  theme_ids_json TEXT,
  current_version_id TEXT,
  owner TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS thesis_versions (
  thesis_version_id TEXT PRIMARY KEY,
  thesis_id TEXT NOT NULL REFERENCES theses(thesis_id),
  statement TEXT NOT NULL,
  mechanism_chain TEXT NOT NULL,
  why_now TEXT,
  base_case TEXT,
  counter_case TEXT,
  invalidators TEXT,
  required_followups TEXT,
  human_conviction REAL,
  created_from_artifacts_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS targets (
  target_id TEXT PRIMARY KEY,
  entity_id TEXT NOT NULL REFERENCES entities(entity_id),
  asset_class TEXT NOT NULL,
  venue TEXT,
  ticker_or_symbol TEXT NOT NULL,
  currency TEXT,
  liquidity_bucket TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS target_cases (
  target_case_id TEXT PRIMARY KEY,
  thesis_version_id TEXT NOT NULL REFERENCES thesis_versions(thesis_version_id),
  target_id TEXT NOT NULL REFERENCES targets(target_id),
  exposure_type TEXT,
  capture_link_strength REAL,
  key_metrics_json TEXT,
  valuation_context TEXT,
  risks TEXT,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS timing_plans (
  timing_plan_id TEXT PRIMARY KEY,
  target_case_id TEXT NOT NULL REFERENCES target_cases(target_case_id),
  window_type TEXT,
  catalysts_json TEXT,
  confirmation_signals_json TEXT,
  preconditions_json TEXT,
  invalidators_json TEXT,
  desired_posture TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS monitors (
  monitor_id TEXT PRIMARY KEY,
  owner_object_type TEXT NOT NULL,
  owner_object_id TEXT NOT NULL,
  monitor_type TEXT NOT NULL,
  metric_name TEXT,
  comparator TEXT,
  threshold_value REAL,
  latest_value REAL,
  query_or_rule TEXT,
  status TEXT NOT NULL,
  last_checked_at TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS monitor_events (
  monitor_event_id TEXT PRIMARY KEY,
  monitor_id TEXT NOT NULL REFERENCES monitors(monitor_id),
  observed_value REAL,
  outcome TEXT NOT NULL,
  detail_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews (
  review_id TEXT PRIMARY KEY,
  owner_object_type TEXT,
  owner_object_id TEXT NOT NULL,
  review_date TEXT NOT NULL,
  what_we_believed TEXT,
  what_happened TEXT,
  result TEXT NOT NULL,
  source_attribution TEXT,
  source_ids_json TEXT,
  claim_ids_json TEXT,
  lessons TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS validation_cases (
  validation_case_id TEXT PRIMARY KEY,
  route_id TEXT REFERENCES claim_routes(route_id),
  claim_id TEXT NOT NULL REFERENCES claims(claim_id),
  thesis_id TEXT,
  thesis_version_id TEXT,
  source_id TEXT,
  verdict TEXT NOT NULL,
  evidence_artifact_ids_json TEXT,
  rationale TEXT,
  validator TEXT,
  validator_model TEXT,
  expires_at TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_validation_cases_claim_id ON validation_cases(claim_id);
CREATE INDEX IF NOT EXISTS idx_validation_cases_thesis_id ON validation_cases(thesis_id);
CREATE INDEX IF NOT EXISTS idx_validation_cases_route_id ON validation_cases(route_id);

CREATE TABLE IF NOT EXISTS source_viewpoints (
  source_viewpoint_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(source_id),
  artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
  thesis_id TEXT,
  target_case_id TEXT,
  summary TEXT NOT NULL,
  stance TEXT NOT NULL,
  horizon_label TEXT,
  status TEXT NOT NULL,
  validation_case_ids_json TEXT,
  resolution_review_id TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_viewpoints_source_id ON source_viewpoints(source_id);
CREATE INDEX IF NOT EXISTS idx_source_viewpoints_thesis_id ON source_viewpoints(thesis_id);

CREATE TABLE IF NOT EXISTS source_feedback_entries (
  source_feedback_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(source_id),
  source_viewpoint_id TEXT REFERENCES source_viewpoints(source_viewpoint_id),
  review_id TEXT REFERENCES reviews(review_id),
  validation_case_id TEXT REFERENCES validation_cases(validation_case_id),
  feedback_type TEXT NOT NULL,
  weight INTEGER NOT NULL,
  note TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_feedback_entries_source_id ON source_feedback_entries(source_id);
CREATE INDEX IF NOT EXISTS idx_source_feedback_entries_viewpoint_id ON source_feedback_entries(source_viewpoint_id);

CREATE TABLE IF NOT EXISTS patterns (
  pattern_id TEXT PRIMARY KEY,
  pattern_kind TEXT NOT NULL,
  label TEXT NOT NULL,
  description TEXT NOT NULL,
  trigger_terms_json TEXT,
  source_review_ids_json TEXT,
  source_thesis_ids_json TEXT,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_patterns_kind ON patterns(pattern_kind);
CREATE INDEX IF NOT EXISTS idx_patterns_status ON patterns(status);

CREATE TABLE IF NOT EXISTS operator_decisions (
  decision_id TEXT PRIMARY KEY,
  target_case_id TEXT NOT NULL REFERENCES target_cases(target_case_id),
  thesis_id TEXT NOT NULL REFERENCES theses(thesis_id),
  decision_date TEXT NOT NULL,
  action_state TEXT NOT NULL,
  confidence REAL,
  rationale TEXT,
  source_ids_json TEXT,
  review_id TEXT REFERENCES reviews(review_id),
  status TEXT NOT NULL,
  supersedes_decision_id TEXT REFERENCES operator_decisions(decision_id),
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operator_decisions_target_case_id ON operator_decisions(target_case_id);
CREATE INDEX IF NOT EXISTS idx_operator_decisions_thesis_id ON operator_decisions(thesis_id);
CREATE INDEX IF NOT EXISTS idx_operator_decisions_decision_date ON operator_decisions(decision_date);

CREATE TABLE IF NOT EXISTS analysis_runs (
  analysis_run_id TEXT PRIMARY KEY,
  engine TEXT NOT NULL,
  prompt_version TEXT,
  input_refs_json TEXT NOT NULL,
  output_ref TEXT NOT NULL,
  cost REAL,
  latency_ms INTEGER,
  schema_valid INTEGER NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS claim_routes (
  route_id TEXT PRIMARY KEY,
  claim_id TEXT NOT NULL REFERENCES claims(claim_id),
  artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
  route_type TEXT NOT NULL,
  target_object_type TEXT,
  target_object_id TEXT,
  reason TEXT,
  status TEXT NOT NULL,
  metadata_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS claim_route_links (
  route_link_id TEXT PRIMARY KEY,
  route_id TEXT NOT NULL REFERENCES claim_routes(route_id),
  link_kind TEXT NOT NULL,
  linked_object_type TEXT NOT NULL,
  linked_object_id TEXT NOT NULL,
  note TEXT,
  metadata_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  object_type TEXT NOT NULL,
  object_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_mining_events (
  event_row_id TEXT PRIMARY KEY,
  schema_version TEXT NOT NULL,
  event_id TEXT NOT NULL UNIQUE,
  entity TEXT NOT NULL,
  product TEXT,
  event_type TEXT NOT NULL,
  stage_from TEXT,
  stage_to TEXT,
  source_role TEXT NOT NULL,
  source_tier TEXT NOT NULL,
  root_claim_key TEXT NOT NULL,
  independence_group TEXT NOT NULL,
  evidence_text TEXT NOT NULL,
  evidence_url TEXT,
  evidence_date TEXT,
  event_time TEXT NOT NULL,
  first_seen_time TEXT NOT NULL,
  processed_time TEXT NOT NULL,
  novelty TEXT NOT NULL,
  relevance TEXT NOT NULL,
  impact TEXT NOT NULL,
  confidence TEXT NOT NULL,
  mapped_trigger TEXT,
  candidate_thesis TEXT,
  residual_class TEXT NOT NULL DEFAULT 'watch',
  residual_target TEXT,
  route_reason TEXT,
  state_applied INTEGER NOT NULL DEFAULT 0,
  dedup_group_size INTEGER NOT NULL DEFAULT 1,
  corroboration_count INTEGER NOT NULL DEFAULT 1,
  route TEXT NOT NULL,
  projection_id TEXT,
  candidate_id TEXT,
  raw_event_json TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_mining_events_entity_product
  ON event_mining_events(entity, product);
CREATE INDEX IF NOT EXISTS idx_event_mining_events_route
  ON event_mining_events(route, processed_time DESC);
CREATE INDEX IF NOT EXISTS idx_event_mining_events_projection_id
  ON event_mining_events(projection_id, processed_time DESC);
CREATE INDEX IF NOT EXISTS idx_event_mining_events_candidate_id
  ON event_mining_events(candidate_id, processed_time DESC);

CREATE TABLE IF NOT EXISTS event_state_projections (
  projection_id TEXT PRIMARY KEY,
  schema_version TEXT NOT NULL,
  entity TEXT NOT NULL,
  product TEXT,
  bucket_role TEXT,
  entity_role TEXT NOT NULL,
  linked_thesis_id TEXT,
  linked_target_case_id TEXT,
  grammar_key TEXT,
  current_stage TEXT,
  stage_entered_at TEXT,
  current_confidence TEXT,
  expected_next_stage TEXT,
  expected_by TEXT,
  last_event_id TEXT,
  last_event_time TEXT,
  last_seen_time TEXT,
  last_route TEXT,
  last_route_reason TEXT,
  last_source_tier TEXT,
  last_independence_group TEXT,
  trigger_code TEXT,
  evidence_text TEXT,
  evidence_url TEXT,
  evidence_date TEXT,
  source_role TEXT,
  stall_status TEXT NOT NULL DEFAULT 'clear',
  raw_event_count INTEGER NOT NULL DEFAULT 0,
  independence_group_count INTEGER NOT NULL DEFAULT 0,
  attention_capture_ratio REAL NOT NULL DEFAULT 0,
  pending_anti_thesis_count INTEGER NOT NULL DEFAULT 0,
  notes_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_state_projections_entity_product
  ON event_state_projections(entity, product);
CREATE INDEX IF NOT EXISTS idx_event_state_projections_target_case
  ON event_state_projections(linked_target_case_id);
CREATE INDEX IF NOT EXISTS idx_event_state_projections_thesis
  ON event_state_projections(linked_thesis_id);

CREATE TABLE IF NOT EXISTS opportunity_candidates (
  candidate_id TEXT PRIMARY KEY,
  schema_version TEXT NOT NULL,
  thesis_name TEXT NOT NULL,
  status TEXT NOT NULL,
  route TEXT NOT NULL,
  residual_class TEXT NOT NULL DEFAULT 'frontier',
  adjacent_projection_ids_json TEXT,
  cluster_score REAL NOT NULL DEFAULT 0,
  persistence_score REAL NOT NULL DEFAULT 0,
  corroboration_score REAL NOT NULL DEFAULT 0,
  investability_score REAL NOT NULL DEFAULT 0,
  raw_event_count INTEGER NOT NULL DEFAULT 0,
  independence_group_count INTEGER NOT NULL DEFAULT 0,
  attention_capture_ratio REAL NOT NULL DEFAULT 0,
  anti_thesis_status TEXT NOT NULL DEFAULT 'clear',
  last_source_tier TEXT,
  earliest_event_time TEXT,
  latest_event_time TEXT,
  last_event_id TEXT,
  next_proving_milestone TEXT,
  note TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_opportunity_candidates_status
  ON opportunity_candidates(status, updated_at DESC);

CREATE TABLE IF NOT EXISTS event_independence_groups (
  group_id TEXT PRIMARY KEY,
  schema_version TEXT NOT NULL,
  root_claim_key TEXT NOT NULL,
  entity TEXT NOT NULL,
  product TEXT,
  source_tier TEXT NOT NULL,
  source_role TEXT NOT NULL,
  event_count INTEGER NOT NULL DEFAULT 0,
  first_event_id TEXT NOT NULL,
  last_event_id TEXT NOT NULL,
  first_event_time TEXT NOT NULL,
  last_event_time TEXT NOT NULL,
  representative_evidence_text TEXT NOT NULL,
  representative_evidence_url TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_independence_groups_root_claim
  ON event_independence_groups(root_claim_key, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_event_independence_groups_entity_product
  ON event_independence_groups(entity, product);

CREATE TABLE IF NOT EXISTS anti_thesis_checks (
  check_id TEXT PRIMARY KEY,
  schema_version TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id TEXT NOT NULL,
  target_label TEXT NOT NULL,
  status TEXT NOT NULL,
  due_reason TEXT,
  trigger_event_id TEXT,
  prompt TEXT NOT NULL,
  result_summary TEXT,
  contradiction_score REAL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anti_thesis_checks_status
  ON anti_thesis_checks(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_anti_thesis_checks_object
  ON anti_thesis_checks(object_type, object_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS event_mining_feedback (
  feedback_id TEXT PRIMARY KEY,
  schema_version TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id TEXT NOT NULL,
  feedback_type TEXT NOT NULL,
  verdict TEXT NOT NULL,
  score REAL,
  note TEXT,
  related_event_id TEXT,
  related_candidate_id TEXT,
  metadata_json TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_mining_feedback_object
  ON event_mining_feedback(object_type, object_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_event_mining_feedback_type
  ON event_mining_feedback(feedback_type, created_at DESC);

-- ═══════════════════════════════════════════════════════════════════════
-- v2-native: Competitive Research (domain-agnostic)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS asset_ledger (
  asset_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  brand TEXT NOT NULL,
  product_line TEXT DEFAULT '',
  category TEXT DEFAULT '',
  source_url TEXT DEFAULT '',
  local_path TEXT DEFAULT '',
  acquisition_date TEXT DEFAULT '',
  is_official INTEGER DEFAULT 0,
  quality_grade TEXT DEFAULT '',
  visible_content TEXT DEFAULT '',
  supports_conclusion TEXT DEFAULT '',
  prohibits_conclusion TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT NULL,
  last_run_id TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_asset_ledger_brand
  ON asset_ledger(brand, category);
CREATE INDEX IF NOT EXISTS idx_asset_ledger_run
  ON asset_ledger(run_id);

CREATE TABLE IF NOT EXISTS sku_catalog (
  sku_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  brand TEXT NOT NULL,
  series TEXT DEFAULT '',
  model TEXT DEFAULT '',
  positioning TEXT DEFAULT '',
  price_range TEXT DEFAULT '',
  wheel_diameter TEXT DEFAULT '',
  frame_type TEXT DEFAULT '',
  motor_type TEXT DEFAULT '',
  battery_platform TEXT DEFAULT '',
  brake_config TEXT DEFAULT '',
  target_audience TEXT DEFAULT '',
  style_tags_json TEXT DEFAULT '[]',
  evidence_sources_json TEXT DEFAULT '[]',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT NULL,
  last_run_id TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_sku_catalog_brand
  ON sku_catalog(brand, series);
CREATE INDEX IF NOT EXISTS idx_sku_catalog_run
  ON sku_catalog(run_id);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: list[tuple[str, str]]) -> None:
    existing = _table_columns(conn, table)
    for name, column_sql in columns:
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {column_sql}")


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    _ensure_columns(
        conn,
        "claims",
        [
            ("data_date", "TEXT"),
            ("review_status", "TEXT NOT NULL DEFAULT 'unreviewed'"),
            ("review_metadata_json", "TEXT"),
            ("domain_check_json", "TEXT"),
            ("freshness_status", "TEXT NOT NULL DEFAULT 'unknown'"),
        ],
    )
    _ensure_columns(
        conn,
        "reviews",
        [
            ("owner_object_type", "TEXT"),
            ("source_ids_json", "TEXT"),
            ("claim_ids_json", "TEXT"),
        ],
    )
    _ensure_columns(
        conn,
        "event_mining_events",
        [
            ("source_tier", "TEXT NOT NULL DEFAULT 'primary'"),
            ("root_claim_key", "TEXT NOT NULL DEFAULT ''"),
            ("independence_group", "TEXT NOT NULL DEFAULT ''"),
            ("residual_class", "TEXT NOT NULL DEFAULT 'watch'"),
            ("residual_target", "TEXT"),
            ("route_reason", "TEXT"),
            ("state_applied", "INTEGER NOT NULL DEFAULT 0"),
            ("dedup_group_size", "INTEGER NOT NULL DEFAULT 1"),
            ("corroboration_count", "INTEGER NOT NULL DEFAULT 1"),
        ],
    )
    _ensure_columns(
        conn,
        "event_state_projections",
        [
            ("last_route_reason", "TEXT"),
            ("last_source_tier", "TEXT"),
            ("last_independence_group", "TEXT"),
            ("raw_event_count", "INTEGER NOT NULL DEFAULT 0"),
            ("independence_group_count", "INTEGER NOT NULL DEFAULT 0"),
            ("attention_capture_ratio", "REAL NOT NULL DEFAULT 0"),
            ("pending_anti_thesis_count", "INTEGER NOT NULL DEFAULT 0"),
        ],
    )
    _ensure_columns(
        conn,
        "opportunity_candidates",
        [
            ("residual_class", "TEXT NOT NULL DEFAULT 'frontier'"),
            ("adjacent_projection_ids_json", "TEXT"),
            ("raw_event_count", "INTEGER NOT NULL DEFAULT 0"),
            ("independence_group_count", "INTEGER NOT NULL DEFAULT 0"),
            ("attention_capture_ratio", "REAL NOT NULL DEFAULT 0"),
            ("anti_thesis_status", "TEXT NOT NULL DEFAULT 'clear'"),
            ("last_source_tier", "TEXT"),
        ],
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_mining_events_root_claim ON event_mining_events(root_claim_key, processed_time DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_mining_events_independence_group ON event_mining_events(independence_group, processed_time DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_opportunity_candidates_anti_thesis ON opportunity_candidates(anti_thesis_status, updated_at DESC)"
    )
    # ── v2 competitive tables: ensure new columns on upgrade ─────
    _ensure_columns(
        conn,
        "asset_ledger",
        [
            ("updated_at", "TEXT DEFAULT NULL"),
            ("last_run_id", "TEXT DEFAULT NULL"),
        ],
    )
    _ensure_columns(
        conn,
        "sku_catalog",
        [
            ("updated_at", "TEXT DEFAULT NULL"),
            ("last_run_id", "TEXT DEFAULT NULL"),
        ],
    )
    conn.commit()


def insert_event(
    conn: sqlite3.Connection,
    event_id: str,
    object_type: str,
    object_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO events(event_id, object_type, object_id, event_type, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            object_type,
            object_id,
            event_type,
            json_dumps(payload or {}),
            utc_now_iso(),
        ),
    )


def upsert_source(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    payload = dict(row)
    payload.setdefault("track_record_stats_json", json_dumps({}))
    payload.setdefault("created_at", utc_now_iso())
    conn.execute(
        """
        INSERT INTO sources(
          source_id, source_type, name, primaryness, jurisdiction, language,
          base_uri, credibility_policy, track_record_stats_json, created_at
        )
        VALUES(
          :source_id, :source_type, :name, :primaryness, :jurisdiction, :language,
          :base_uri, :credibility_policy, :track_record_stats_json, :created_at
        )
        ON CONFLICT(source_id) DO UPDATE SET
          source_type=excluded.source_type,
          name=excluded.name,
          primaryness=excluded.primaryness,
          jurisdiction=excluded.jurisdiction,
          language=excluded.language,
          base_uri=excluded.base_uri,
          credibility_policy=excluded.credibility_policy,
          track_record_stats_json=excluded.track_record_stats_json
        """,
        payload,
    )


def insert_artifact(conn: sqlite3.Connection, row: dict[str, Any], content: str) -> None:
    payload = dict(row)
    payload.setdefault("created_at", utc_now_iso())
    conn.execute(
        """
        INSERT INTO artifacts(
          artifact_id, source_id, artifact_kind, title, captured_at, published_at, language,
          uri, raw_path, normalized_text_path, content_hash, status, metadata_json, created_at
        ) VALUES (
          :artifact_id, :source_id, :artifact_kind, :title, :captured_at, :published_at, :language,
          :uri, :raw_path, :normalized_text_path, :content_hash, :status, :metadata_json, :created_at
        )
        """,
        payload,
    )
    conn.execute(
        "INSERT INTO artifact_fts(artifact_id, title, content) VALUES (?, ?, ?)",
        (payload["artifact_id"], payload["title"], content),
    )


def insert_claim(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    payload = dict(row)
    payload.setdefault("created_at", utc_now_iso())
    payload.setdefault("data_date", "")
    payload.setdefault("review_status", "unreviewed")
    payload.setdefault("review_metadata_json", json_dumps({}))
    payload.setdefault("domain_check_json", json_dumps({}))
    payload.setdefault("freshness_status", "unknown")
    conn.execute(
        """
        INSERT INTO claims(
          claim_id, artifact_id, speaker, timecode_or_span, claim_text,
          claim_type, confidence, linked_entity_ids_json, data_date,
          review_status, review_metadata_json, domain_check_json,
          freshness_status, status, created_at
        ) VALUES (
          :claim_id, :artifact_id, :speaker, :timecode_or_span, :claim_text,
          :claim_type, :confidence, :linked_entity_ids_json, :data_date,
          :review_status, :review_metadata_json, :domain_check_json,
          :freshness_status, :status, :created_at
        )
        """,
        payload,
    )


def insert_row(conn: sqlite3.Connection, table: str, row: dict[str, Any]) -> None:
    payload = dict(row)
    payload.setdefault("created_at", utc_now_iso())
    keys = sorted(payload.keys())
    columns = ", ".join(keys)
    placeholders = ", ".join(f":{key}" for key in keys)
    try:
        conn.execute(f"INSERT INTO {table}({columns}) VALUES ({placeholders})", payload)
    except sqlite3.IntegrityError as exc:
        if "FOREIGN KEY" in str(exc):
            # FK enforcement can fail spuriously in some sqlite3 builds when
            # using parameterized inserts.  Retry with FK checks temporarily
            # disabled.
            conn.execute("PRAGMA foreign_keys = OFF")
            try:
                conn.execute(f"INSERT INTO {table}({columns}) VALUES ({placeholders})", payload)
            finally:
                conn.execute("PRAGMA foreign_keys = ON")
        else:
            raise


def select_one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> sqlite3.Row | None:
    cur = conn.execute(sql, params)
    return cur.fetchone()


def list_rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    cur = conn.execute(sql, params)
    return list(cur.fetchall())


def artifact_text(conn: sqlite3.Connection, artifact_id: str) -> str | None:
    row = select_one(
        conn,
        "SELECT content FROM artifact_fts WHERE artifact_id = ?",
        (artifact_id,),
    )
    if row is None:
        return None
    return row["content"]


def load_metadata(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    raw = row["metadata_json"]
    if not raw:
        return {}
    return json.loads(raw)
