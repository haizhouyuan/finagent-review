from __future__ import annotations

import sqlite3

from finagent.memory import MemoryManager, MemoryTier
from finagent.memory_consolidation import (
    SemanticCandidate,
    execute_promotion,
    find_promotion_candidates,
)


def _memory(tmp_path):
    conn = sqlite3.connect(tmp_path / "finagent.sqlite")
    conn.row_factory = sqlite3.Row
    return MemoryManager(conn), conn


def test_find_candidates_and_execute_with_dry_run_and_idempotency(tmp_path):
    memory, conn = _memory(tmp_path)

    ids = [
        memory.store_episodic(
            "product_spec",
            "九号 Fz110 价格 4299-4999 元",
            run_id="run-1",
            confidence=0.9,
            structured_data={"brand": "九号", "price_range": "4299-4999"},
        ),
        memory.store_episodic(
            "brand_observation",
            "九号门店主推智能化高端通勤车型",
            run_id="run-1",
            confidence=0.86,
            structured_data={"brand": "九号"},
        ),
        memory.store_episodic(
            "product_spec",
            "九号机械师 价格 4699-5399 元",
            run_id="run-1",
            confidence=0.91,
            structured_data={"brand": "九号", "price_range": "4699-5399"},
        ),
    ]
    assert len(ids) == 3

    candidates = find_promotion_candidates(memory)
    categories = {candidate.category for candidate in candidates}
    assert "brand_positioning" in categories
    assert "price_band" in categories

    previews = execute_promotion(memory, candidates, dry_run=True)
    assert len(previews) >= 2
    assert memory.count_by_tier()["semantic"] == 0

    promoted = execute_promotion(memory, candidates, dry_run=False)
    assert len(promoted) >= 2
    assert memory.count_by_tier()["semantic"] >= 2

    promoted_again = execute_promotion(memory, candidates, dry_run=False)
    assert promoted_again == promoted
    assert memory.count_by_tier()["semantic"] >= 2

    conn.close()


def test_execute_rejects_low_confidence_and_insufficient_support(tmp_path):
    memory, conn = _memory(tmp_path)
    episodic_id = memory.store_episodic(
        "market_event",
        "疑似价格战开始",
        run_id="run-1",
        confidence=0.82,
        structured_data={"brand": "雅迪"},
    )

    low_conf = SemanticCandidate(
        category="market_structure",
        conclusion="雅迪和爱玛竞争格局稳定。",
        evidence_ids=[episodic_id, episodic_id],
        brands_involved=["雅迪", "爱玛"],
        confidence=0.79,
        valid_from="2026-03-23",
    )
    not_enough = SemanticCandidate(
        category="brand_positioning",
        conclusion="雅迪定位中高端。",
        evidence_ids=[episodic_id],
        brands_involved=["雅迪"],
        confidence=0.9,
        valid_from="2026-03-23",
    )

    promoted = execute_promotion(memory, [low_conf, not_enough], dry_run=False)
    assert promoted == []
    assert memory.count_by_tier()["semantic"] == 0

    conn.close()


def test_find_candidates_for_supply_chain_and_technology(tmp_path):
    memory, conn = _memory(tmp_path)
    memory.store_episodic(
        "supply_chain",
        "金谷为雅迪供应铝轮毂。",
        run_id="run-1",
        confidence=0.9,
        structured_data={"supplier": "金谷", "customer": "雅迪"},
    )
    memory.store_episodic(
        "supply_chain",
        "金谷继续进入雅迪轮毂体系。",
        run_id="run-2",
        confidence=0.84,
        structured_data={"supplier": "金谷", "customer": "雅迪"},
    )
    memory.store_episodic(
        "research_finding",
        "石墨烯电池在两轮车新品中继续扩张。",
        run_id="run-1",
        confidence=0.9,
        structured_data={"technology": "石墨烯"},
    )
    memory.store_episodic(
        "research_finding",
        "石墨烯相关宣传仍是品牌重点。",
        run_id="run-2",
        confidence=0.88,
        structured_data={"technology": "石墨烯"},
    )
    memory.store_episodic(
        "research_finding",
        "石墨烯电池被多次用于长续航卖点。",
        run_id="run-3",
        confidence=0.85,
        structured_data={"technology": "石墨烯"},
    )

    candidates = find_promotion_candidates(memory)
    categories = {candidate.category for candidate in candidates}
    assert "supply_chain_map" in categories
    assert "technology_trend" in categories

    promoted = execute_promotion(memory, candidates, dry_run=False)
    semantics = memory.recall("", tier=MemoryTier.SEMANTIC)
    contents = {record.content for record in semantics}
    assert any("金谷" in content and "雅迪" in content for content in contents)
    assert any("石墨烯" in content for content in contents)
    assert len(promoted) >= 2

    conn.close()
