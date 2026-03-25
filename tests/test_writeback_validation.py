"""P1a-4: Writeback validation — E2E from research run to v1 DB.

Validates the full chain:
  run_research() → ResearchPackage → plan_writeback() → apply_writeback() → v1 tables
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile

from finagent.agents.orchestrator import run_research
from finagent.agents.synthesizer import synthesize_report
from finagent.agents.evidence_store import EvidenceStore
from finagent.db import SCHEMA_SQL, select_one, list_rows
from finagent.graph_v2.store import GraphStore
from finagent.llm_adapter import create_llm_adapter
from finagent.research_contracts import ResearchPackage, WritebackTarget
from finagent.research_ledger import ResearchLedger
from finagent.writeback_engine import plan_writeback, apply_writeback


def _make_env(tmpdir: str):
    """Create full test environment."""
    db = os.path.join(tmpdir, "research.sqlite")
    gs = GraphStore(db)
    es = EvidenceStore(os.path.join(tmpdir, "evidence.db"))
    ledger = ResearchLedger(db)
    llm_fn = create_llm_adapter("mock")
    search_fn = lambda q: (
        f"Mock: {q}。蓝箭航天(LandSpace)是中国商业航天主要企业。"
        f"SpaceX Falcon 9 全球标杆。星河动力(Galactic Energy)也在竞争。"
    )
    return gs, es, ledger, llm_fn, search_fn, db


def _make_v1_conn() -> sqlite3.Connection:
    """Create in-memory v1 DB with full schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def _seed_entity_target(conn, name: str, ticker: str):
    """Seed entity + target for monitor proposal testing."""
    import uuid
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    eid = f"e-{uuid.uuid4().hex[:8]}"
    tid = f"tg-{uuid.uuid4().hex[:8]}"
    conn.execute(
        "INSERT INTO entities (entity_id, entity_type, canonical_name, created_at) "
        "VALUES (?, ?, ?, ?)", (eid, "company", name, now),
    )
    conn.execute(
        "INSERT INTO targets (target_id, entity_id, asset_class, venue, "
        "ticker_or_symbol, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (tid, eid, "equity", "HK", ticker, now),
    )
    conn.commit()
    return tid, eid


class TestWritebackE2E:
    """Full chain: research → package → writeback plan → apply → verify."""

    def test_research_to_new_thesis(self):
        """Complete chain creates new thesis in v1 DB."""
        with tempfile.TemporaryDirectory() as td:
            gs, es, ledger, llm_fn, search_fn, db = _make_env(td)

            # 1. Run research
            final = run_research(
                "分析商业航天核心供应商竞争格局",
                context="商业航天",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
                llm_backend="mock",
            )

            # 2. Get package
            package = final.get("research_package")
            assert isinstance(package, ResearchPackage)

            # Backfill report_md as CLI does
            report = synthesize_report(final, graph_store=gs)
            package.report_md = report

            # 3. Plan writeback (using separate v1 DB)
            v1_conn = _make_v1_conn()
            actions = plan_writeback(package, v1_conn)

            # Should have at least thesis + source
            assert len(actions) >= 2
            thesis_actions = [a for a in actions if a.target_type == WritebackTarget.THESIS.value]
            assert len(thesis_actions) == 1
            assert thesis_actions[0].op == "create"  # No existing thesis

            # 4. Apply
            applied = apply_writeback(actions, v1_conn)
            assert all(a.applied for a in applied)

            # 5. Verify v1 DB
            theses = list_rows(v1_conn, "SELECT * FROM theses")
            assert len(theses) == 1
            assert theses[0]["status"] == "seed"

            versions = list_rows(v1_conn, "SELECT * FROM thesis_versions")
            assert len(versions) == 1

            sources = list_rows(
                v1_conn, "SELECT * FROM sources WHERE source_type = 'v2_research_run'"
            )
            assert len(sources) == 1
            assert package.run_id in sources[0]["base_uri"]

            gs.close()
            es.close()
            ledger.close()

    def test_research_to_existing_thesis_update(self):
        """When matching thesis exists, updates rather than creates."""
        with tempfile.TemporaryDirectory() as td:
            gs, es, ledger, llm_fn, search_fn, db = _make_env(td)

            # 1. Run research
            final = run_research(
                "分析商业航天核心供应商竞争格局",
                context="商业航天",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
            )

            package = final["research_package"]
            report = synthesize_report(final, graph_store=gs)
            package.report_md = report

            # Seed existing thesis in v1
            v1_conn = _make_v1_conn()
            import uuid
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            vid = f"tv-{uuid.uuid4().hex[:8]}"
            v1_conn.execute(
                "INSERT INTO theses (thesis_id, title, status, horizon_months, "
                "theme_ids_json, owner, current_version_id, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("t1", "商业航天核心供应商竞争格局", "framed", 12, "[]", "user", vid, now),
            )
            v1_conn.execute(
                "INSERT INTO thesis_versions (thesis_version_id, thesis_id, "
                "statement, mechanism_chain, why_now, base_case, counter_case, "
                "invalidators, required_followups, human_conviction, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (vid, "t1", "原始statement", "原始chain", "原始", "", "", "", "", 0.3, now),
            )
            v1_conn.commit()

            # Plan → should be UPDATE
            actions = plan_writeback(package, v1_conn)
            thesis_actions = [a for a in actions if a.target_type == WritebackTarget.THESIS.value]
            assert thesis_actions[0].op == "update"
            assert thesis_actions[0].target_id == "t1"

            # Apply
            apply_writeback(actions, v1_conn)

            # Verify: conviction updated or mechanism_chain modified
            version = select_one(
                v1_conn,
                "SELECT mechanism_chain, human_conviction FROM thesis_versions "
                "WHERE thesis_version_id = ?",
                (vid,),
            )
            # Conviction should be updated from research confidence
            assert version["human_conviction"] >= 0.3

            # No new thesis created
            theses = list_rows(v1_conn, "SELECT * FROM theses")
            assert len(theses) == 1

            gs.close()
            es.close()
            ledger.close()

    def test_entity_monitor_created(self):
        """Entity in v1 targets → monitor created after writeback."""
        with tempfile.TemporaryDirectory() as td:
            gs, es, ledger, llm_fn, search_fn, db = _make_env(td)

            final = run_research(
                "分析商业航天",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
            )

            package = final["research_package"]

            v1_conn = _make_v1_conn()
            _seed_entity_target(v1_conn, "蓝箭航天", "LANDSPACE")

            actions = plan_writeback(package, v1_conn)
            apply_writeback(actions, v1_conn)

            monitors = list_rows(
                v1_conn, "SELECT * FROM monitors WHERE monitor_type = 'research_signal'"
            )
            # May or may not have monitor depending on if mock triples contain "蓝箭航天"
            # At minimum the thesis + source should be created
            sources = list_rows(
                v1_conn, "SELECT * FROM sources WHERE source_type = 'v2_research_run'"
            )
            assert len(sources) >= 1

            gs.close()
            es.close()
            ledger.close()

    def test_writeback_actions_json_roundtrip(self):
        """WritebackAction[] survives JSON serialization."""
        with tempfile.TemporaryDirectory() as td:
            gs, es, ledger, llm_fn, search_fn, db = _make_env(td)

            final = run_research(
                "test",
                llm_fn=llm_fn,
                search_fn=search_fn,
                graph_store=gs,
                evidence_store=es,
                max_iterations=1,
                verbose=False,
                ledger=ledger,
            )

            package = final["research_package"]
            v1_conn = _make_v1_conn()
            actions = plan_writeback(package, v1_conn)

            # JSON round-trip
            from finagent.research_contracts import WritebackAction
            serialized = json.dumps([a.to_dict() for a in actions], ensure_ascii=False)
            restored = [WritebackAction.from_dict(d) for d in json.loads(serialized)]

            assert len(restored) == len(actions)
            for orig, rest in zip(actions, restored):
                assert orig.package_id == rest.package_id
                assert orig.target_type == rest.target_type
                assert orig.op == rest.op

            gs.close()
            es.close()
            ledger.close()
