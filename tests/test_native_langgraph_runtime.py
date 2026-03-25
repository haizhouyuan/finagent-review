from __future__ import annotations

import json
import sqlite3

import pytest

pytest.importorskip("langgraph")
pytest.importorskip("langgraph.checkpoint.sqlite")

from finagent.agents import orchestrator
from finagent.agents.evidence_store import EvidenceStore
from finagent.graph_v2.store import GraphStore
from finagent.memory import MemoryManager


def _native_mock_llm(system: str, user: str) -> str:
    if "规划师" in system or "planner" in system.lower():
        return json.dumps(
            {
                "analysis": "need product evidence",
                "missing": ["product specs"],
                "superfluous": [],
                "queries": [{"query": "九号 Fz 系列 铝轮毂 供应链", "priority": 1}],
                "confidence": 0.55,
            },
            ensure_ascii=False,
        )
    if "竞品分析助手" in system:
        return json.dumps(
            {
                "image_assets": [
                    {
                        "asset_id": "img-ninebot-native-runtime",
                        "brand": "九号",
                        "product_line": "Fz",
                        "category": "exterior",
                        "source_url": "https://example.com/native-runtime",
                        "visible_content": "Fz 系列铝轮毂侧视图",
                        "supports_conclusion": "九号使用铝轮毂",
                    }
                ],
                "sku_records": [
                    {
                        "sku_id": "sku-ninebot-native-runtime",
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
            },
            ensure_ascii=False,
        )
    return json.dumps(
        [
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
        ],
        ensure_ascii=False,
    )


def _native_mock_search(query: str) -> str:
    return (
        "金谷为九号 Fz 系列提供铝合金轮毂，九号 Fz3 价格 6299-7599 元，"
        "采用双管一体车架、轮毂电机 1200W、前碟后碟配置。"
    ) * 3


def test_run_research_uses_native_langgraph_runtime(tmp_path):
    assert orchestrator._LANGGRAPH_AVAILABLE is True

    workflow = orchestrator.build_research_graph()
    assert workflow.__class__.__module__.startswith("langgraph.")

    checkpointer, db_path = orchestrator._make_checkpointer(str(tmp_path / "checkpoints.sqlite"))
    assert db_path == str(tmp_path / "checkpoints.sqlite")
    assert checkpointer.__class__.__module__.startswith("langgraph.")

    graph_store = GraphStore(str(tmp_path / "graph.db"))
    evidence_store = EvidenceStore(str(tmp_path / "evidence.db"))
    conn = sqlite3.connect(str(tmp_path / "state.sqlite"))
    conn.row_factory = sqlite3.Row
    memory = MemoryManager(conn)

    try:
        final = orchestrator.run_research(
            "两轮车竞品结构与供应链试点",
            context="两轮车",
            llm_fn=_native_mock_llm,
            search_fn=_native_mock_search,
            graph_store=graph_store,
            evidence_store=evidence_store,
            memory_manager=memory,
            max_iterations=1,
            verbose=False,
        )
        assert final["run_id"].startswith("run-")
        assert final["iteration_step"] >= 1
        assert final["research_package"].run_id == final["run_id"]
        assert final["research_package"].image_assets
        assert final["research_package"].sku_records
        assert memory.count_by_tier()["episodic"] > 0
    finally:
        graph_store.close()
        evidence_store.close()
        conn.close()
