from __future__ import annotations

import json
import os
import sqlite3
import tempfile

from finagent.agents.evidence_store import EvidenceStore
from finagent.agents.orchestrator import run_research
from finagent.db import SCHEMA_SQL, list_rows
from finagent.graph_v2.store import GraphStore
from finagent.memory import MemoryManager
from finagent.writeback_engine import apply_writeback, plan_writeback


def _mock_two_wheeler_llm(system: str, user: str) -> str:
    if "规划师" in system or "planner" in system.lower():
        return json.dumps({
            "analysis": "need product evidence",
            "missing": ["product specs"],
            "superfluous": [],
            "queries": [
                {"query": "九号 Fz 系列 铝轮毂 供应链", "priority": 1},
            ],
            "confidence": 0.55,
        }, ensure_ascii=False)
    if "竞品分析助手" in system:
        return json.dumps({
            "image_assets": [
                {
                    "asset_id": "img-ninebot-fz-side",
                    "brand": "九号",
                    "product_line": "Fz",
                    "category": "exterior",
                    "source_url": "https://example.com/fz",
                    "visible_content": "Fz 系列铝轮毂侧视图",
                    "supports_conclusion": "九号使用铝轮毂",
                }
            ],
            "sku_records": [
                {
                    "sku_id": "sku-ninebot-fz3",
                    "brand": "九号",
                    "series": "Fz",
                    "model": "Fz3",
                    "positioning": "中高端",
                    "price_range": "6299-7599",
                    "wheel_diameter": "14寸",
                    "frame_type": "双管一体",
                    "motor_type": "轮毂电机 1200W",
                    "battery_platform": "72V30Ah 锂电",
                    "brake_config": "前碟后碟",
                    "target_audience": "年轻男性/通勤",
                    "style_tags": ["运动", "智能"],
                }
            ],
        }, ensure_ascii=False)
    return json.dumps([
        {
            "head": "金谷",
            "head_type": "company",
            "relation": "supplies_core_part_to",
            "tail": "九号",
            "tail_type": "company",
            "exact_quote": "金谷为九号 Fz 系列提供铝合金轮毂",
            "confidence": 0.88,
            "valid_from": "2026-03",
        }
    ], ensure_ascii=False)


def _mock_two_wheeler_search(query: str) -> str:
    return (
        "金谷为九号 Fz 系列提供铝合金轮毂，九号 Fz3 价格 6299-7599 元，"
        "采用双管一体车架、轮毂电机 1200W、前碟后碟配置。"
    ) * 3


class TestTwoWheelerPilotE2E:
    def test_research_produces_episodic(self):
        with tempfile.TemporaryDirectory() as td:
            graph_store = GraphStore(os.path.join(td, "graph.db"))
            evidence_store = EvidenceStore(os.path.join(td, "evidence.db"))
            conn = sqlite3.connect(os.path.join(td, "state.sqlite"))
            conn.row_factory = sqlite3.Row
            memory = MemoryManager(conn)

            final = run_research(
                "两轮车竞品结构与供应链试点",
                context="两轮车",
                llm_fn=_mock_two_wheeler_llm,
                search_fn=_mock_two_wheeler_search,
                graph_store=graph_store,
                evidence_store=evidence_store,
                memory_manager=memory,
                max_iterations=1,
                verbose=False,
            )

            assert final["run_id"].startswith("run-")
            assert memory.count_by_tier()["episodic"] > 0

            graph_store.close()
            evidence_store.close()
            conn.close()

    def test_runtime_consolidation_produces_semantic(self):
        with tempfile.TemporaryDirectory() as td:
            graph_store = GraphStore(os.path.join(td, "graph.db"))
            evidence_store = EvidenceStore(os.path.join(td, "evidence.db"))
            conn = sqlite3.connect(os.path.join(td, "state.sqlite"))
            conn.row_factory = sqlite3.Row
            memory = MemoryManager(conn)

            final = run_research(
                "两轮车竞品结构与供应链试点",
                context="两轮车",
                llm_fn=_mock_two_wheeler_llm,
                search_fn=_mock_two_wheeler_search,
                graph_store=graph_store,
                evidence_store=evidence_store,
                memory_manager=memory,
                max_iterations=1,
                verbose=False,
            )

            assert final.get("semantic_promotions")
            assert memory.count_by_tier()["semantic"] > 0
            assert final.get("memory_counts", {}).get("episodic", 0) >= 3

            graph_store.close()
            evidence_store.close()
            conn.close()

    def test_loop_consolidation_semantic_visible_same_run(self):
        planner_prompts: list[str] = []

        def llm_with_prompt_capture(system: str, user: str) -> str:
            if "规划师" in system or "planner" in system.lower():
                planner_prompts.append(user)
                return json.dumps({
                    "analysis": "need product evidence",
                    "missing": ["product specs"],
                    "superfluous": [],
                    "queries": [
                        {"query": "九号 Fz 系列 铝轮毂 供应链", "priority": 1},
                    ],
                    "confidence": 0.25,
                }, ensure_ascii=False)
            return _mock_two_wheeler_llm(system, user)

        with tempfile.TemporaryDirectory() as td:
            graph_store = GraphStore(os.path.join(td, "graph.db"))
            evidence_store = EvidenceStore(os.path.join(td, "evidence.db"))
            conn = sqlite3.connect(os.path.join(td, "state.sqlite"))
            conn.row_factory = sqlite3.Row
            memory = MemoryManager(conn)

            final = run_research(
                "两轮车竞品结构与供应链试点",
                context="两轮车",
                llm_fn=llm_with_prompt_capture,
                search_fn=_mock_two_wheeler_search,
                graph_store=graph_store,
                evidence_store=evidence_store,
                memory_manager=memory,
                max_iterations=2,
                verbose=False,
                enable_loop_consolidation=True,
            )

            assert len(planner_prompts) >= 2
            assert "已固化认知" in planner_prompts[1]
            assert "九号 在两轮车样本中持续出现" in planner_prompts[1]
            assert memory.count_by_tier()["semantic"] == 1
            assert final.get("semantic_promotions")

            graph_store.close()
            evidence_store.close()
            conn.close()

    def test_competitive_writeback_no_pollution(self):
        with tempfile.TemporaryDirectory() as td:
            graph_store = GraphStore(os.path.join(td, "graph.db"))
            evidence_store = EvidenceStore(os.path.join(td, "evidence.db"))
            conn = sqlite3.connect(os.path.join(td, "state.sqlite"))
            conn.row_factory = sqlite3.Row
            memory = MemoryManager(conn)

            final = run_research(
                "两轮车竞品结构与供应链试点",
                context="两轮车",
                llm_fn=_mock_two_wheeler_llm,
                search_fn=_mock_two_wheeler_search,
                graph_store=graph_store,
                evidence_store=evidence_store,
                memory_manager=memory,
                max_iterations=1,
                verbose=False,
            )

            pkg = final["research_package"]

            v1_conn = sqlite3.connect(":memory:")
            v1_conn.row_factory = sqlite3.Row
            v1_conn.executescript(SCHEMA_SQL)

            pre_theses = len(list_rows(v1_conn, "SELECT * FROM theses"))
            pre_sources = len(list_rows(v1_conn, "SELECT * FROM sources"))
            pre_monitors = len(list_rows(v1_conn, "SELECT * FROM monitors"))

            actions = plan_writeback(pkg, v1_conn, target_families={"competitive"})
            apply_writeback(actions, v1_conn)

            post_theses = len(list_rows(v1_conn, "SELECT * FROM theses"))
            post_sources = len(list_rows(v1_conn, "SELECT * FROM sources"))
            post_monitors = len(list_rows(v1_conn, "SELECT * FROM monitors"))
            assets = len(list_rows(v1_conn, "SELECT * FROM asset_ledger"))
            skus = len(list_rows(v1_conn, "SELECT * FROM sku_catalog"))

            assert assets > 0
            assert skus > 0
            assert pre_theses == post_theses
            assert pre_sources == post_sources
            assert pre_monitors == post_monitors

            graph_store.close()
            evidence_store.close()
            conn.close()
