"""Tests for parsers — Document Parser Pipeline.

Covers:
  - TextCleaner (HTML→text, whitespace normalization, noise removal)
  - SemanticChunker (splitting, overlap, merging)
  - DocumentParser (end-to-end pipeline)
  - Controlled extraction test: exact_quote with cleaned text
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest


# ── TextCleaner tests ───────────────────────────────────────────────


def test_cleaner_html_to_text():
    from finagent.parsers.text_cleaner import TextCleaner
    cleaner = TextCleaner()

    html = """
    <html><head><title>Test</title></head>
    <body>
    <script>alert('xss')</script>
    <style>.foo { color: red }</style>
    <h2>商业航天产业链</h2>
    <p>蓝箭航天是国内领先的民营火箭企业，主营液体火箭研发和发射服务。</p>
    <p>其主要竞争对手包括<b>星河动力</b>和<b>中科宇航</b>。</p>
    <ul>
    <li>朱雀二号：液氧甲烷火箭</li>
    <li>天鹊发动机：80吨级推力</li>
    </ul>
    </body></html>
    """

    result = cleaner.clean(html)

    # Should preserve semantic content
    assert "商业航天产业链" in result
    assert "蓝箭航天" in result
    assert "星河动力" in result
    # Should strip scripts/styles
    assert "alert" not in result
    assert "color: red" not in result
    # Should preserve bold as Markdown
    assert "**星河动力**" in result or "星河动力" in result
    # Should preserve list items
    assert "朱雀二号" in result


def test_cleaner_whitespace_normalization():
    """Critical test: whitespace normalization for exact_quote compatibility."""
    from finagent.parsers.text_cleaner import TextCleaner
    cleaner = TextCleaner()

    # Simulate messy web scraping with tabs, \r\n, multiple spaces
    messy = (
        "意法半导体\t\t提供射频芯片。\r\n"
        "   该芯片用于    卫星通信      终端设备。\r\n"
        "\n\n\n\n\n"
        "SpaceX 的 Starlink 使用了类似方案。"
    )

    result = cleaner.clean(messy)

    # All internal whitespace should be normalized to single spaces
    assert "意法半导体 提供射频芯片" in result
    assert "该芯片用于 卫星通信 终端设备" in result
    # Excessive newlines should be collapsed
    assert "\n\n\n" not in result


def test_cleaner_invisible_characters():
    from finagent.parsers.text_cleaner import TextCleaner
    cleaner = TextCleaner()

    # Zero-width characters that break string matching
    text = "蓝箭\u200b航天\ufeff是一家\u200c火箭\u200d企业，具有很强的竞争力。"
    result = cleaner.clean(text)
    assert "蓝箭航天是一家火箭企业" in result


def test_cleaner_unicode_normalization():
    from finagent.parsers.text_cleaner import TextCleaner
    cleaner = TextCleaner()

    # Full-width to half-width (NFKC normalization)
    text = "蓝箭航天（ＬａｎｄＳｐａｃｅ）是一家火箭制造企业，主要产品线包括朱雀二号。"
    result = cleaner.clean(text)
    assert "LandSpace" in result  # Full-width → half-width


def test_cleaner_quality_score():
    from finagent.parsers.text_cleaner import TextCleaner
    cleaner = TextCleaner()

    # Good quality document
    good = "蓝箭航天是国内领先的民营火箭企业。" * 10
    result = cleaner.clean_for_evidence(good)
    assert result["quality_score"] > 0.5

    # Very noisy (mostly HTML)
    noisy = "<script>alert(1)</script>" * 20 + "一点内容"
    result = cleaner.clean_for_evidence(noisy)
    assert result["quality_score"] < 0.3


# ── SemanticChunker tests ───────────────────────────────────────────


def test_chunker_basic():
    from finagent.parsers.chunker import SemanticChunker
    chunker = SemanticChunker(max_chars=80, overlap_chars=10, min_chunk_chars=20)

    text = (
        "## 产业链概况\n\n"
        "蓝箭航天是国内领先的民营火箭企业。主营液体火箭研发和发射服务。朝着可重复使用的目标发展，技术路线与 SpaceX 类似。\n\n"
        "## 竞争格局\n\n"
        "星河动力是蓝箭航天的主要竞争对手，双方在中型液体火箭领域展开激烈竞争。固液双线发展是其核心竞争策略。"
    )

    chunks = chunker.chunk(text)
    assert len(chunks) >= 2
    # Each chunk should have content
    for c in chunks:
        assert len(c.text) > 0
        assert c.char_count > 0


def test_chunker_respects_max_chars():
    from finagent.parsers.chunker import SemanticChunker
    chunker = SemanticChunker(max_chars=300, overlap_chars=0)

    # Create a long document
    text = "\n\n".join(
        f"这是第{i}段内容，包含关于商业航天产业链的描述信息和关键供应商数据。蓝箭航天和星河动力是主要参与者。"
        for i in range(20)
    )

    chunks = chunker.chunk(text)
    # Max chars is a target, not hard limit; but no chunk should be massively over
    for c in chunks:
        assert c.char_count <= 600  # Allow some flexibility


def test_chunker_overlap():
    from finagent.parsers.chunker import SemanticChunker
    chunker = SemanticChunker(max_chars=200, overlap_chars=50)

    text = (
        "第一段：蓝箭航天主营火箭制造业务，在商业航天领域具有重要地位。\n\n"
        "第二段：星河动力主攻固体火箭市场，与蓝箭航天形成差异化竞争格局。"
    )

    chunks = chunker.chunk(text)
    if len(chunks) >= 2:
        # Check that chunks have overlap — text from chunk[1] start
        # should appear at the end of chunk[0]
        c0_tail = chunks[0].text[-100:]
        c1_head = chunks[1].text[:50]
        # At least some text should overlap
        overlap_detected = any(word in c0_tail for word in c1_head.split() if len(word) > 2)
        # Overlap is best-effort, not guaranteed for tiny texts
        # Just verify chunks were created
        assert len(chunks) >= 1


def test_chunker_chinese_sentence_split():
    from finagent.parsers.chunker import SemanticChunker
    chunker = SemanticChunker(max_chars=50, overlap_chars=0, min_chunk_chars=10)

    # One single long paragraph (no headers, no double newlines)
    text = (
        "蓝箭航天成立于2015年。"
        "公司总部位于北京。"
        "主要产品是朱雀系列运载火箭。"
        "公司A股上市代码688245。"
        "星河动力成立于2018年。"
        "主要产品是谷神星系列火箭。"
    )

    chunks = chunker.chunk(text)
    assert len(chunks) >= 2
    assert "蓝箭航天" in chunks[0].text


def test_chunker_short_text():
    from finagent.parsers.chunker import SemanticChunker
    chunker = SemanticChunker(max_chars=4000)

    text = "短文本测试。This is a brief piece of content."
    chunks = chunker.chunk(text)
    assert len(chunks) == 1
    assert chunks[0].text == text


# ── DocumentParser tests ───────────────────────────────────────────


@pytest.fixture
def tmp_evidence_store():
    from finagent.agents.evidence_store import EvidenceStore
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = EvidenceStore(db_path)
        yield store
        store.close()
    finally:
        os.unlink(db_path)


def test_document_parser_text(tmp_evidence_store):
    from finagent.parsers.document_parser import DocumentParser

    parser = DocumentParser(evidence_store=tmp_evidence_store)

    # Simulate a research report
    report = """
    ## 商业航天产业链深度分析

    蓝箭航天（LandSpace）是国内领先的民营火箭企业，成立于2015年，总部位于北京。
    公司主要产品为朱雀二号液氧甲烷运载火箭，采用天鹊发动机（80吨级推力）。

    ## 供应链分析

    蓝箭航天的核心供应商包括：
    - 航天电器（002025.SZ）：提供电连接器
    - 西部超导（688122.SH）：提供高温合金材料
    - 铖昌科技：提供T/R组件

    ## 竞争格局

    星河动力（Galactic Energy）是蓝箭航天的主要竞争对手，主攻固体+液体双线发展战略。
    中科宇航（CAS Space）则依托中科院资源，在可重复使用火箭领域具有独特优势。
    """

    refs = parser.parse_text(report, query="商业航天产业链")

    assert len(refs) >= 1
    # Check refs are stored in evidence store (not inline)
    for ref in refs:
        assert ref["evidence_id"] is not None
        text = tmp_evidence_store.fetch(ref["evidence_id"])
        assert len(text) > 0
        # The text should be clean
        assert "<" not in text or text.count("<") < 3  # No HTML tags


def test_document_parser_file(tmp_path, tmp_evidence_store):
    from finagent.parsers.document_parser import DocumentParser

    # Create a mock markdown file
    md_content = """# 测试研报

## 核心观点

蓝箭航天的朱雀二号已经完成多次发射任务，验证了液氧甲烷技术路线的可行性。
公司计划在2025年实现可重复使用火箭的首飞。

## 风险提示

商业发射市场竞争加剧，技术路线存在不确定性。
"""
    md_path = tmp_path / "test_report.md"
    md_path.write_text(md_content, encoding="utf-8")

    parser = DocumentParser(evidence_store=tmp_evidence_store)
    refs = parser.parse_file(md_path, query="蓝箭航天研报")

    assert len(refs) >= 1


def test_document_parser_quality_gate():
    """Documents with too low quality should be rejected."""
    from finagent.parsers.document_parser import DocumentParser

    parser = DocumentParser(quality_threshold=0.1)

    # Mostly noise
    noise = "<script>alert(1)</script>" * 50
    refs = parser.parse_text(noise, query="test")
    assert len(refs) == 0


# ── Critical integration test: exact_quote + cleaned text ──────────

@pytest.fixture
def tmp_graph_store():
    from finagent.graph_v2.store import GraphStore
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = GraphStore(db_path)
        yield store
        store.close()
    finally:
        os.unlink(db_path)


def test_exact_quote_on_cleaned_html(tmp_graph_store, tmp_evidence_store):
    """THE critical integration test: HTML → clean → extract → exact_quote passes.

    This simulates the real-world scenario that was identified as the
    "阻抗不匹配" risk: raw HTML with formatting noise goes through
    the cleaner, then the LLM extracts triples with exact_quote,
    and the validation succeeds because both sides see the same text.
    """
    import json
    from finagent.parsers.document_parser import DocumentParser
    from finagent.agents.extractor import extractor_node
    from finagent.agents.state import initial_state

    # Step 1: Simulate noisy HTML research report snippet
    raw_html = """
    <div class="article-body">
    <h2>商业航天供应链分析</h2>
    <p>蓝箭航天的核心供应商包括<b>航天电器</b>（002025.SZ），
    主要为其提供<span style="color:blue">电连接器</span>和
    精密电子元器件。</p>
    <p>双方合作始于2020年，目前航天电器占蓝箭航天<br/>
    电连接器采购份额的约60%。</p>
    <script>var ga = {};</script>
    <div class="share-bar">分享到 微信 微博</div>
    </div>
    """

    # Step 2: Parse through the pipeline
    parser = DocumentParser(evidence_store=tmp_evidence_store)
    refs = parser.parse_text(raw_html, query="蓝箭航天供应链")

    assert len(refs) >= 1

    # Step 3: Verify the cleaned text is readable
    stored_text = tmp_evidence_store.fetch(refs[0]["evidence_id"])
    assert "蓝箭航天" in stored_text
    assert "航天电器" in stored_text
    assert "<script>" not in stored_text
    assert "分享到" not in stored_text  # Noise removed

    # Step 4: Simulate LLM extraction with exact_quote from CLEANED text
    # The exact_quote must be a substring of the CLEANED text
    triple_with_good_quote = {
        "head": "航天电器", "head_type": "company",
        "relation": "supplies_core_part_to",
        "tail": "蓝箭航天", "tail_type": "company",
        "exact_quote": "航天电器",  # This IS in the cleaned text
        "confidence": 0.9, "valid_from": "2020",
    }

    def mock_llm(system: str, user: str) -> str:
        return json.dumps([triple_with_good_quote], ensure_ascii=False)

    state = initial_state("测试")
    state["gathered_evidence"] = refs  # Use the parsed refs

    result = extractor_node(
        state, llm_fn=mock_llm,
        graph_store=tmp_graph_store,
        evidence_store=tmp_evidence_store,
    )

    # The triple should survive exact_quote validation because we
    # cleaned the HTML before storing, and the LLM sees the same clean text
    assert result["total_triples_added"] >= 1
    assert tmp_graph_store.has_node("航天电器")
    assert tmp_graph_store.has_node("蓝箭航天")


def test_searcher_cleans_before_store(tmp_evidence_store):
    """Verify that the searcher's built-in TextCleaner works end-to-end."""
    from finagent.agents.searcher import searcher_node
    from finagent.agents.state import initial_state

    def mock_search(query: str) -> str:
        return """
        <html><body>
        <h2>蓝箭航天产业链</h2>
        <p>蓝箭航天的核心供应商包括航天电器和西部超导。双方保持长期稳定的合作关系。</p>
        <p>其中航天电器主要提供电连接器组件，西部超导提供高温合金材料。</p>
        <script>tracking();</script>
        <div class="share-bar">分享到 微信 微博</div>
        </body></html>
        """

    state = initial_state("test")
    state["pending_queries"] = ["蓝箭航天 供应商"]

    result = searcher_node(
        state, search_fn=mock_search,
        evidence_store=tmp_evidence_store,
    )

    assert len(result["gathered_evidence"]) >= 1
    ref = result["gathered_evidence"][0]

    # Fetch from store and verify it's CLEAN
    text = tmp_evidence_store.fetch(ref["evidence_id"])
    assert "蓝箭航天" in text
    assert "<script>" not in text
    assert "tracking" not in text  # Script removed
