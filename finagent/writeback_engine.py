"""Writeback engine: ResearchPackage → WritebackAction[].

Two modes:
  plan_writeback()    — dry-run: generates actions without touching DB
  apply_writeback()   — execute: runs planned actions against v1 DB

v1 targets: thesis + thesis_version, source, monitor.
v2-native targets: asset_ledger, sku_catalog (competitive research).
"""
from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

from .db import insert_row, select_one
from .research_contracts import (
    ResearchPackage, WritebackAction, WritebackTarget, WritebackOp,
)
from .thesis_bridge import find_matching_thesis, ThesisView

logger = logging.getLogger(__name__)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


# ── Dry-Run Planner ─────────────────────────────────────────────────

def plan_writeback(
    package: ResearchPackage,
    conn: sqlite3.Connection,
    *,
    target_families: set[str] | None = None,
) -> list[WritebackAction]:
    """Generate WritebackActions from a ResearchPackage (dry-run).

    Does NOT modify the database. Returns a list of planned actions
    with ``applied=False`` and ``dry_run_result="planned"``.

    Args:
        target_families: Optional set of target families to plan for.
            - "thesis"      → thesis/source/monitor (v1 thesis OS)
            - "competitive"  → asset_ledger/sku_catalog (v2 competitive)
            If None, plans ALL families. Use {"competitive"} for
            competitive-only runs to avoid polluting v1 thesis OS.

    Strategy:
    1. Find matching thesis → UPDATE or CREATE (if "thesis" family)
    2. For entity triples → propose target_case if entity has target
    3. For package itself → record as v1 analysis_run
    4. For image_assets → ASSET_LEDGER (if "competitive" family)
    5. For sku_records → SKU_CATALOG (if "competitive" family)
    """
    actions: list[WritebackAction] = []
    evidence_ids = [r.evidence_id for r in package.evidence_refs if r.evidence_id]

    plan_thesis = target_families is None or "thesis" in target_families
    plan_competitive = target_families is None or "competitive" in target_families

    # ── 1. Thesis mapping ────────────────────────────────────────
    if plan_thesis:
        existing = find_matching_thesis(conn, package.goal, package.context)

        if existing:
            # UPDATE existing thesis_version with new insights
            actions.append(WritebackAction(
                package_id=package.run_id,
                target_type=WritebackTarget.THESIS.value,
                target_id=existing.thesis_id,
                op=WritebackOp.UPDATE.value,
                payload={
                    "thesis_id": existing.thesis_id,
                    "version_id": existing.version_id,
                    "statement_supplement": _build_statement_supplement(package),
                    "mechanism_chain_supplement": _extract_mechanism_chain(package),
                    "confidence_delta": package.confidence,
                    "source_run_id": package.run_id,
                },
                confidence=package.confidence,
                source_evidence_ids=evidence_ids,
                dry_run_result="planned: update existing thesis",
            ))
        else:
            # CREATE new thesis + thesis_version
            # Stable IDs derived from run_id for idempotency
            new_thesis_id = f"thesis-{package.run_id[-12:]}"
            new_version_id = f"tv-{package.run_id[-12:]}"
            actions.append(WritebackAction(
                package_id=package.run_id,
                target_type=WritebackTarget.THESIS.value,
                target_id="",  # Will be set on apply
                op=WritebackOp.CREATE.value,
                payload={
                    "thesis_id": new_thesis_id,
                    "version_id": new_version_id,
                    "title": package.goal[:100],
                    "status": "seed",
                    "horizon_months": 12,
                    "statement": _build_statement_supplement(package),
                    "mechanism_chain": _extract_mechanism_chain(package),
                    "why_now": f"v2 research ({package.created_at[:10]})",
                    "base_case": "",
                    "counter_case": "",
                    "invalidators": "",
                    "human_conviction": min(package.confidence, 0.6),
                    "source_run_id": package.run_id,
                },
                confidence=package.confidence,
                source_evidence_ids=evidence_ids,
                dry_run_result="planned: create new thesis",
            ))

    # ── 2. Source recording (stable ID from run_id for idempotency)
    if plan_thesis:
        source_id = f"src-{package.run_id[-12:]}"
        actions.append(WritebackAction(
            package_id=package.run_id,
            target_type=WritebackTarget.SOURCE.value,
            target_id="",
            op=WritebackOp.CREATE.value,
            payload={
                "source_id": source_id,
                "source_type": "v2_research_run",
                "name": f"Research: {package.goal[:60]}",
                "primaryness": "secondary",
                "base_uri": f"run://{package.run_id}",
            },
            confidence=package.confidence,
            source_evidence_ids=evidence_ids,
            dry_run_result="planned: record as v1 source",
        ))

    # ── 3. Entity-based target_case proposals ────────────────────
    if plan_thesis:
        entities_seen: set[str] = set()
        for triple in (package.triples or []):
            subject = triple.get("subject", "")
            obj = triple.get("object", "")
            for entity_name in (subject, obj):
                if not entity_name or entity_name in entities_seen:
                    continue
                entities_seen.add(entity_name)

                # Check if entity exists as a target in v1
                row = select_one(
                    conn,
                    """
                    SELECT tg.target_id, e.entity_id, e.canonical_name
                    FROM targets tg
                    JOIN entities e ON tg.entity_id = e.entity_id
                    WHERE e.canonical_name = ? OR e.canonical_name LIKE ?
                    """,
                    (entity_name, f"%{entity_name}%"),
                )

                if row:
                    # Stable monitor_id from run_id + target for idempotency
                    mon_id = f"mon-{package.run_id[-8:]}-{row['target_id'][-8:]}"
                    actions.append(WritebackAction(
                        package_id=package.run_id,
                        target_type=WritebackTarget.WATCH_ITEM.value,
                        target_id=row["target_id"],
                        op=WritebackOp.CREATE.value,
                        payload={
                            "monitor_id": mon_id,
                            "monitor_type": "research_signal",
                            "metric_name": f"v2_research_{package.run_id[:8]}",
                            "owner_object_type": "target",
                            "owner_object_id": row["target_id"],
                            "query_or_rule": f"Research finding: {entity_name}",
                        },
                        confidence=package.confidence,
                        source_evidence_ids=evidence_ids,
                        dry_run_result=f"planned: monitor for entity {entity_name}",
                    ))

    # ── 4. Image asset actions (from package.image_assets) ────────
    if plan_competitive:
        for asset in getattr(package, 'image_assets', None) or []:
            asset_id = getattr(asset, 'asset_id', '') or asset.get('asset_id', '') if isinstance(asset, dict) else asset.asset_id
            if not asset_id:
                continue
            asset_dict = asset.to_dict() if hasattr(asset, 'to_dict') else dict(asset)
            # Check if asset already exists → UPDATE, else CREATE
            existing_asset = select_one(
                conn, "SELECT asset_id FROM asset_ledger WHERE asset_id = ?", (asset_id,),
            )
            op = WritebackOp.UPDATE.value if existing_asset else WritebackOp.CREATE.value
            actions.append(WritebackAction(
                package_id=package.run_id,
                target_type=WritebackTarget.ASSET_LEDGER.value,
                target_id=asset_id if existing_asset else "",
                op=op,
                payload=asset_dict,
                confidence=package.confidence,
                source_evidence_ids=evidence_ids,
                dry_run_result=f"planned: {op} asset {asset_id}",
            ))

    # ── 5. SKU record actions (from package.sku_records) ──────────
    if plan_competitive:
        for sku in getattr(package, 'sku_records', None) or []:
            sku_id = getattr(sku, 'sku_id', '') or sku.get('sku_id', '') if isinstance(sku, dict) else sku.sku_id
            if not sku_id:
                continue
            sku_dict = sku.to_dict() if hasattr(sku, 'to_dict') else dict(sku)
            existing_sku = select_one(
                conn, "SELECT sku_id FROM sku_catalog WHERE sku_id = ?", (sku_id,),
            )
            op = WritebackOp.UPDATE.value if existing_sku else WritebackOp.CREATE.value
            actions.append(WritebackAction(
                package_id=package.run_id,
                target_type=WritebackTarget.SKU_CATALOG.value,
                target_id=sku_id if existing_sku else "",
                op=op,
                payload=sku_dict,
                confidence=package.confidence,
                source_evidence_ids=evidence_ids,
                dry_run_result=f"planned: {op} SKU {sku_id}",
            ))

    return actions


def _build_statement_supplement(package: ResearchPackage) -> str:
    """Build thesis statement from package report (first 500 chars)."""
    if package.report_md:
        # Use first paragraph as statement
        lines = package.report_md.strip().split("\n")
        for line in lines:
            clean = line.strip().lstrip("#").strip()
            if clean and len(clean) > 20:
                return clean[:500]
    return package.goal


def _extract_mechanism_chain(package: ResearchPackage) -> str:
    """Extract mechanism chain from triples."""
    if not package.triples:
        return ""
    chains = []
    for t in package.triples[:5]:  # Top 5 triples
        subj = t.get("subject", "")
        pred = t.get("predicate", "")
        obj = t.get("object", "")
        if subj and pred and obj:
            chains.append(f"{subj} → {pred} → {obj}")
    return "; ".join(chains)


# ── Apply ────────────────────────────────────────────────────────────

def apply_writeback(
    actions: list[WritebackAction],
    conn: sqlite3.Connection,
) -> list[WritebackAction]:
    """Execute planned WritebackActions against the v1 database.

    Wraps in a transaction — all-or-nothing.
    Returns actions with ``applied=True`` and ``applied_at`` set.
    """
    applied: list[WritebackAction] = []
    now = _utcnow()

    try:
        for action in actions:
            if action.applied:
                applied.append(action)
                continue

            _execute_action(action, conn)

            # Mark as applied (WritebackAction is not frozen)
            action.applied = True
            action.applied_at = now
            action.dry_run_result = f"applied: {action.dry_run_result}"
            applied.append(action)

        conn.commit()
    except Exception as exc:
        conn.rollback()
        logger.error("writeback failed, rolled back: %s", exc)
        raise

    return applied


def _execute_action(action: WritebackAction, conn: sqlite3.Connection) -> None:
    """Execute a single WritebackAction."""
    payload = action.payload
    now = _utcnow()

    if action.target_type == WritebackTarget.THESIS.value:
        if action.op == WritebackOp.CREATE.value:
            _create_thesis(payload, conn, now)
        elif action.op == WritebackOp.UPDATE.value:
            _update_thesis(payload, conn, now)

    elif action.target_type == WritebackTarget.SOURCE.value:
        if action.op == WritebackOp.CREATE.value:
            # Idempotent: skip if source_id already exists
            existing = select_one(
                conn, "SELECT source_id FROM sources WHERE source_id = ?",
                (payload["source_id"],),
            )
            if not existing:
                insert_row(conn, "sources", {
                    "source_id": payload["source_id"],
                    "source_type": payload["source_type"],
                    "name": payload["name"],
                    "primaryness": payload["primaryness"],
                    "base_uri": payload.get("base_uri", ""),
                })

    elif action.target_type == WritebackTarget.WATCH_ITEM.value:
        if action.op == WritebackOp.CREATE.value:
            mon_id = payload.get("monitor_id") or _new_id("mon-")
            # Idempotent: skip if monitor_id already exists
            existing = select_one(
                conn, "SELECT monitor_id FROM monitors WHERE monitor_id = ?",
                (mon_id,),
            )
            if not existing:
                insert_row(conn, "monitors", {
                    "monitor_id": mon_id,
                    "owner_object_type": payload["owner_object_type"],
                    "owner_object_id": payload["owner_object_id"],
                    "monitor_type": payload["monitor_type"],
                    "metric_name": payload.get("metric_name", ""),
                    "query_or_rule": payload.get("query_or_rule", ""),
                    "status": "active",
                })

    # ── v2-native: competitive research targets ──────────────────
    elif action.target_type == WritebackTarget.ASSET_LEDGER.value:
        asset_id = payload.get("asset_id", "")
        if action.op == WritebackOp.CREATE.value:
            existing = select_one(
                conn, "SELECT asset_id FROM asset_ledger WHERE asset_id = ?",
                (asset_id,),
            )
            if not existing:
                insert_row(conn, "asset_ledger", {
                    "asset_id": asset_id,
                    "run_id": action.package_id,
                    "brand": payload.get("brand", ""),
                    "product_line": payload.get("product_line", ""),
                    "category": payload.get("category", ""),
                    "source_url": payload.get("source_url", ""),
                    "local_path": payload.get("local_path", ""),
                    "acquisition_date": payload.get("acquisition_date", ""),
                    "is_official": 1 if payload.get("is_official") else 0,
                    "quality_grade": payload.get("quality_grade", ""),
                    "visible_content": payload.get("visible_content", ""),
                    "supports_conclusion": payload.get("supports_conclusion", ""),
                    "prohibits_conclusion": payload.get("prohibits_conclusion", ""),
                })
        elif action.op == WritebackOp.UPDATE.value:
            # Update mutable fields: quality_grade, visible_content, conclusions
            updates = []
            params = []
            for col in ("quality_grade", "visible_content", "supports_conclusion",
                        "prohibits_conclusion", "local_path", "source_url"):
                val = payload.get(col)
                if val:
                    updates.append(f"{col} = ?")
                    params.append(val)
            if payload.get("is_official") is not None:
                updates.append("is_official = ?")
                params.append(1 if payload["is_official"] else 0)
            if updates:
                # Always record provenance
                now = datetime.now(timezone.utc).isoformat()
                updates.append("updated_at = ?")
                params.append(now)
                updates.append("last_run_id = ?")
                params.append(action.package_id)
                params.append(asset_id)
                conn.execute(
                    f"UPDATE asset_ledger SET {', '.join(updates)} WHERE asset_id = ?",
                    params,
                )

    elif action.target_type == WritebackTarget.SKU_CATALOG.value:
        sku_id = payload.get("sku_id", "")
        if action.op == WritebackOp.CREATE.value:
            existing = select_one(
                conn, "SELECT sku_id FROM sku_catalog WHERE sku_id = ?",
                (sku_id,),
            )
            if not existing:
                import json as _json
                insert_row(conn, "sku_catalog", {
                    "sku_id": sku_id,
                    "run_id": action.package_id,
                    "brand": payload.get("brand", ""),
                    "series": payload.get("series", ""),
                    "model": payload.get("model", ""),
                    "positioning": payload.get("positioning", ""),
                    "price_range": payload.get("price_range", ""),
                    "wheel_diameter": payload.get("wheel_diameter", ""),
                    "frame_type": payload.get("frame_type", ""),
                    "motor_type": payload.get("motor_type", ""),
                    "battery_platform": payload.get("battery_platform", ""),
                    "brake_config": payload.get("brake_config", ""),
                    "target_audience": payload.get("target_audience", ""),
                    "style_tags_json": _json.dumps(payload.get("style_tags", []), ensure_ascii=False),
                    "evidence_sources_json": _json.dumps(payload.get("evidence_sources", []), ensure_ascii=False),
                })
        elif action.op == WritebackOp.UPDATE.value:
            import json as _json
            # Update mutable fields only if provided and non-empty
            updates = []
            params = []
            for col in ("positioning", "price_range", "wheel_diameter", "frame_type",
                        "motor_type", "battery_platform", "brake_config", "target_audience"):
                val = payload.get(col)
                if val:
                    updates.append(f"{col} = ?")
                    params.append(val)
            for json_col, key in (("style_tags_json", "style_tags"), ("evidence_sources_json", "evidence_sources")):
                val = payload.get(key)
                if val:
                    updates.append(f"{json_col} = ?")
                    params.append(_json.dumps(val, ensure_ascii=False))
            if updates:
                # Always record provenance
                now = datetime.now(timezone.utc).isoformat()
                updates.append("updated_at = ?")
                params.append(now)
                updates.append("last_run_id = ?")
                params.append(action.package_id)
                params.append(sku_id)
                conn.execute(
                    f"UPDATE sku_catalog SET {', '.join(updates)} WHERE sku_id = ?",
                    params,
                )

    else:
        logger.warning("unsupported writeback target: %s", action.target_type)


def _create_thesis(payload: dict, conn: sqlite3.Connection, now: str) -> None:
    """Create a new thesis + thesis_version. Idempotent via thesis_id."""
    thesis_id = payload["thesis_id"]
    version_id = payload["version_id"]

    # Idempotent: skip if thesis already exists
    existing = select_one(conn, "SELECT thesis_id FROM theses WHERE thesis_id = ?", (thesis_id,))
    if existing:
        logger.info("thesis %s already exists, skipping create", thesis_id)
        return

    # Create thesis first (thesis_versions references thesis via FK)
    insert_row(conn, "theses", {
        "thesis_id": thesis_id,
        "title": payload["title"],
        "status": payload.get("status", "seed"),
        "horizon_months": payload.get("horizon_months", 12),
        "theme_ids_json": "[]",
        "owner": "v2_research",
        "current_version_id": version_id,
    })

    # Then create thesis_version
    insert_row(conn, "thesis_versions", {
        "thesis_version_id": version_id,
        "thesis_id": thesis_id,
        "statement": payload.get("statement", ""),
        "mechanism_chain": payload.get("mechanism_chain", ""),
        "why_now": payload.get("why_now", ""),
        "base_case": payload.get("base_case", ""),
        "counter_case": payload.get("counter_case", ""),
        "invalidators": payload.get("invalidators", ""),
        "required_followups": "",
        "human_conviction": payload.get("human_conviction", 0.0),
    })


def _update_thesis(payload: dict, conn: sqlite3.Connection, now: str) -> None:
    """Update existing thesis_version with supplemental info.

    Writes: statement (append), mechanism_chain (append), human_conviction (max).
    """
    version_id = payload.get("version_id", "")
    if not version_id:
        logger.warning("update_thesis: no version_id in payload")
        return

    existing = select_one(
        conn,
        "SELECT statement, mechanism_chain, human_conviction FROM thesis_versions "
        "WHERE thesis_version_id = ?",
        (version_id,),
    )
    if not existing:
        logger.warning("update_thesis: version %s not found", version_id)
        return

    # Append statement supplement
    stmt_supplement = payload.get("statement_supplement", "")
    if stmt_supplement:
        current_stmt = existing["statement"] or ""
        # Only append if not already present (idempotency)
        if stmt_supplement not in current_stmt:
            updated_stmt = f"{current_stmt}\n\n[v2 update] {stmt_supplement}" if current_stmt else stmt_supplement
            conn.execute(
                "UPDATE thesis_versions SET statement = ? WHERE thesis_version_id = ?",
                (updated_stmt, version_id),
            )

    # Append to mechanism chain
    chain_supplement = payload.get("mechanism_chain_supplement", "")
    if chain_supplement:
        current_chain = existing["mechanism_chain"] or ""
        if chain_supplement not in current_chain:
            updated_chain = f"{current_chain}; {chain_supplement}" if current_chain else chain_supplement
            conn.execute(
                "UPDATE thesis_versions SET mechanism_chain = ? WHERE thesis_version_id = ?",
                (updated_chain, version_id),
            )

    # Update conviction if higher
    conf = payload.get("confidence_delta", 0.0)
    if conf > 0:
        conn.execute(
            "UPDATE thesis_versions SET human_conviction = MAX(COALESCE(human_conviction, 0), ?) "
            "WHERE thesis_version_id = ?",
            (conf, version_id),
        )


# ── Display ──────────────────────────────────────────────────────────

def print_writeback_plan(actions: list[WritebackAction]) -> None:
    """Print human-readable writebackplan summary."""
    print(f"\n📋 Writeback Plan ({len(actions)} actions)")
    print("=" * 60)
    for i, a in enumerate(actions, 1):
        status = "✅ applied" if a.applied else "📝 planned"
        print(f"  {i}. [{status}] {a.op.upper()} {a.target_type}")
        print(f"     {a.dry_run_result}")
        if a.target_id:
            print(f"     target: {a.target_id}")
        print(f"     confidence: {a.confidence:.2f}")
        print()
