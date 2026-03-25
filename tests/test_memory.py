from __future__ import annotations

import sqlite3

import pytest

from finagent.memory import (
    EPISODIC_CATEGORIES,
    MemoryManager,
    MemoryTier,
    SEMANTIC_CATEGORIES,
    WORKING_CATEGORY,
)


def _memory(tmp_path):
    conn = sqlite3.connect(tmp_path / "finagent.sqlite")
    conn.row_factory = sqlite3.Row
    return MemoryManager(conn), conn


def test_store_and_recall_across_tiers(tmp_path):
    memory, conn = _memory(tmp_path)

    working_id = memory.store_working("本轮关注九号供应链", run_id="run-1")
    episodic_id = memory.store_episodic(
        "product_spec",
        "发现九号 Fz110 价格 4299-4999 元",
        run_id="run-1",
        confidence=0.88,
        structured_data={"brand": "九号", "price_range": "4299-4999"},
    )
    semantic_id = memory.promote_to_semantic(
        [episodic_id],
        "九号产品价格带正在上探。",
        "price_band",
        0.83,
        structured_data={"brands_involved": ["九号"]},
    )

    counts = memory.count_by_tier()
    assert counts["working"] == 1
    assert counts["episodic"] == 1
    assert counts["semantic"] == 1

    working = memory.get_by_category(WORKING_CATEGORY, tier=MemoryTier.WORKING)
    assert working[0].record_id == working_id

    episodic = memory.recall("九号", tier=MemoryTier.EPISODIC)
    assert episodic[0].record_id == episodic_id
    assert episodic[0].structured_data["price_range"] == "4299-4999"

    semantic = memory.recall("", tier=MemoryTier.SEMANTIC)
    assert semantic[0].record_id == semantic_id
    assert semantic[0].content == "九号产品价格带正在上探。"
    assert semantic[0].access_count >= 1

    conn.close()


def test_category_whitelist_enforced(tmp_path):
    memory, conn = _memory(tmp_path)

    assert "product_spec" in EPISODIC_CATEGORIES
    assert "price_band" in SEMANTIC_CATEGORIES

    with pytest.raises(ValueError):
        memory.store_episodic("bad_category", "x", run_id="run-1")

    with pytest.raises(ValueError):
        memory.promote_to_semantic([], "x", "bad_semantic", 0.9)

    conn.close()


def test_recall_matches_structured_data_and_tier_filter(tmp_path):
    memory, conn = _memory(tmp_path)
    memory.store_episodic(
        "supply_chain",
        "金谷为雅迪供应铝合金轮毂",
        run_id="run-1",
        confidence=0.86,
        structured_data={"supplier": "金谷", "customer": "雅迪", "component": "铝合金轮毂"},
    )
    memory.store_episodic(
        "market_event",
        "两轮车行业旺季启动",
        run_id="run-1",
        confidence=0.75,
    )

    recalls = memory.recall("铝合金轮毂", tier=MemoryTier.EPISODIC)
    assert len(recalls) == 1
    assert recalls[0].category == "supply_chain"

    conn.close()


def test_expire_working_removes_old_entries(tmp_path):
    memory, conn = _memory(tmp_path)
    memory.store_working("old working note", run_id="run-1")
    conn.execute(
        "UPDATE memory_records SET created_at = '2020-01-01T00:00:00+00:00' WHERE tier = ?",
        (MemoryTier.WORKING.value,),
    )
    conn.commit()

    expired = memory.expire_working()
    assert expired == 1
    assert memory.count_by_tier()["working"] == 0

    conn.close()
