"""LLM-driven triple extraction for industry chain graph construction.

Takes unstructured text (research reports, news, IPO prospectuses) and
extracts structured [Head, Relation, Tail] triples using LLM prompts.

Usage::

    from finagent.graph.builder import extract_triples, build_from_text
    from finagent.graph.industry_chain import IndustryChainGraph

    g = IndustryChainGraph.load()
    triples = extract_triples(text, llm_fn=my_llm_call)
    build_from_text(g, text, llm_fn=my_llm_call, source="招股书 p.42")
    g.save()
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Prompt engineering for triple extraction
# ------------------------------------------------------------------

EXTRACTION_SYSTEM_PROMPT = """\
你是一个产业链知识图谱构建助手。你的任务是从投研文本中提取结构化的三元组关系。

## 输出格式
返回一个 JSON 数组，每个元素是一个三元组对象：
```json
[
  {"head": "实体A", "relation": "关系类型", "tail": "实体B", "evidence": "原文证据", "confidence": 0.9}
]
```

## 允许的 relation 类型（必须严格使用以下值）：
- supplies_to: A 是 B 的供应商（A 供应产品/服务给 B）
- customer_of: A 是 B 的客户（A 采购 B 的产品/服务）
- competes_with: A 与 B 在同一领域竞争
- manufactures: A 制造/生产 B（公司制造产品）
- component_of: A 是 B 的组件/子系统（零部件→整机/系统）
- enables: A 技术使能 B（技术→产品/项目）
- regulates: A 标准/政策约束 B
- invested_by: A 被 B 投资/持股
- partners_with: A 与 B 合作/联合
- belongs_to: A 属于 B 板块/赛道

## 实体类型提示（head 和 tail 可以是）：
- 公司名（如：蓝箭航天、SpaceX、中国星网）
- 产品线（如：液氧甲烷发动机、SAR卫星）
- 原材料（如：T800碳纤维、特种合金）
- 重大工程（如：千帆星座、G60星链）
- 核心技术（如：可回收火箭、相控阵天线）
- 子系统（如：姿控系统、星载通信载荷）
- 行业板块（如：卫星互联网、发射服务）

## 规则
1. 只提取文本中明确提到或强烈暗示的关系，不要臆测
2. confidence: 0.9+ 表示文本明确说了，0.7-0.9 表示强烈暗示，0.5-0.7 表示推断
3. evidence 必须是原文中的关键短句（不超过 50 字）
4. 同一对实体可以有多种关系
5. 如果文本中没有可提取的产业链关系，返回空数组 []
"""

EXTRACTION_USER_TEMPLATE = """\
请从以下投研文本中提取产业链三元组关系：

---
{text}
---

返回 JSON 数组，每个元素包含 head, relation, tail, evidence, confidence。
注意：只返回 JSON，不要添加任何解释。
"""


def extract_triples(
    text: str,
    *,
    llm_fn: Callable[[str, str], str],
    max_text_len: int = 8000,
) -> list[dict[str, Any]]:
    """Extract industry chain triples from text using LLM.

    Args:
        text: Research report, news article, or prospectus text.
        llm_fn: Function(system_prompt, user_prompt) -> str (LLM response).
        max_text_len: Truncate text to this length.

    Returns:
        List of {head, relation, tail, evidence, confidence} dicts.
    """
    if not text or not text.strip():
        return []

    # Truncate overly long text
    if len(text) > max_text_len:
        text = text[:max_text_len] + "\n\n[... 文本已截断 ...]"

    user_prompt = EXTRACTION_USER_TEMPLATE.format(text=text)

    try:
        raw_response = llm_fn(EXTRACTION_SYSTEM_PROMPT, user_prompt)
    except Exception as exc:
        logger.error("LLM call failed for triple extraction: %s", exc)
        return []

    # Parse JSON from response
    triples = _parse_json_response(raw_response)

    # Validate and filter
    valid = []
    for t in triples:
        if not isinstance(t, dict):
            continue
        head = str(t.get("head", "")).strip()
        relation = str(t.get("relation", "")).strip()
        tail = str(t.get("tail", "")).strip()
        if not head or not relation or not tail:
            continue
        valid.append({
            "head": head,
            "relation": relation,
            "tail": tail,
            "evidence": str(t.get("evidence", "")),
            "confidence": float(t.get("confidence", 0.7)),
        })

    logger.info("extracted %d triples from text (%d chars)", len(valid), len(text))
    return valid


def _parse_json_response(raw: str) -> list[dict[str, Any]]:
    """Parse JSON array from LLM response, handling markdown fences."""
    text = raw.strip()

    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last fence lines
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        if isinstance(result, dict) and "triples" in result:
            return result["triples"]
        return [result] if isinstance(result, dict) else []
    except json.JSONDecodeError:
        # Try to find JSON array in the text
        import re
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        logger.warning("failed to parse LLM response as JSON: %s...", text[:200])
        return []


def build_from_triples(
    graph: Any,  # IndustryChainGraph
    triples: list[dict[str, Any]],
    *,
    source: str = "unknown",
    min_confidence: float = 0.5,
) -> int:
    """Ingest triples into the industry chain graph.

    Args:
        graph: IndustryChainGraph instance.
        triples: List of extracted triples.
        source: Source document identifier.
        min_confidence: Minimum confidence to include.

    Returns:
        Number of triples added.
    """
    added = 0
    for t in triples:
        if t.get("confidence", 0) < min_confidence:
            continue
        graph.add_triple(
            head=t["head"],
            relation=t["relation"],
            tail=t["tail"],
            evidence=t.get("evidence", ""),
            confidence=t.get("confidence", 0.7),
            source=source,
        )
        added += 1
    logger.info("added %d/%d triples from source '%s'", added, len(triples), source)
    return added


def build_from_text(
    graph: Any,  # IndustryChainGraph
    text: str,
    *,
    llm_fn: Callable[[str, str], str],
    source: str = "unknown",
    min_confidence: float = 0.5,
) -> list[dict[str, Any]]:
    """End-to-end: extract triples from text and add to graph.

    Returns the extracted triples.
    """
    triples = extract_triples(text, llm_fn=llm_fn)
    build_from_triples(graph, triples, source=source, min_confidence=min_confidence)
    return triples


# ------------------------------------------------------------------
# Commercial Aerospace Seed Data (手工 top-down 骨架)
# ------------------------------------------------------------------

def seed_commercial_aerospace(graph: Any) -> None:
    """Pre-populate commercial aerospace industry chain skeleton.

    Top-down structure based on expert knowledge:
    原材料 → 部件/子系统 → 总装(火箭/卫星) → 发射服务 → 应用(通信/遥感/导航)
    """
    # ---- Sectors ----
    graph.add_sector("商业航天")
    graph.add_sector("卫星互联网")
    graph.add_sector("发射服务")
    graph.add_sector("卫星制造")
    graph.add_sector("地面设备")
    graph.add_sector("卫星应用")

    # ---- Major Projects ----
    graph.add_project("千帆星座", alias="G60星链", operator="垣信卫星",
                      scale="超14000颗LEO卫星", status="发射中")
    graph.add_project("中国星网", alias="GW星座", operator="中国卫星网络集团",
                      scale="约13000颗LEO卫星", status="规划中")
    graph.add_project("鸿鹄星座", operator="北京驭航科技", status="规划中")

    # ---- Rocket Companies ----
    graph.add_company("蓝箭航天", ticker="688245.SH",
                      product="朱雀系列火箭", founded=2015)
    graph.add_company("星河动力", ticker=None,
                      product="谷神星/智神星火箭", founded=2018)
    graph.add_company("星际荣耀", ticker=None,
                      product="双曲线系列火箭", founded=2016)
    graph.add_company("中科宇航", ticker=None,
                      product="力箭系列火箭", founded=2018)
    graph.add_company("天兵科技", ticker=None,
                      product="天龙系列火箭", founded=2019)
    graph.add_company("东方空间", ticker=None,
                      product="引力系列火箭", founded=2020)
    graph.add_company("SpaceX", ticker=None, market="US",
                      product="Falcon9/Starship", note="全球标杆")

    # ---- State-owned Launch ----
    graph.add_company("航天科技集团", ticker=None, note="央企 CASC")
    graph.add_company("中国运载火箭技术研究院", alias="一院",
                      parent="航天科技集团")

    # ---- Satellite Manufacturers ----
    graph.add_company("长光卫星", ticker="688568.SH",
                      product="吉林一号SAR/光学卫星", sector="卫星制造")
    graph.add_company("微纳星空", ticker=None, product="微小卫星平台")
    graph.add_company("银河航天", ticker=None, product="低轨宽带通信卫星")
    graph.add_company("垣信卫星", ticker=None,
                      note="千帆星座运营商, 上海松江")
    graph.add_company("格思航天", ticker=None, product="卫星载荷/通信设备")

    # ---- Key Subsystems / Components ----
    graph.add_company("上海瀚讯", ticker="300762.SZ",
                      product="星载通信设备/T/R芯片")
    graph.add_company("航天电器", ticker="002025.SZ",
                      product="高端连接器/继电器")
    graph.add_company("铖昌科技", ticker="001270.SZ",
                      product="相控阵T/R芯片")
    graph.add_company("臻镭科技", ticker="688270.SH",
                      product="微波芯片/T/R组件")
    graph.add_company("航天环宇", ticker="688523.SH",
                      product="卫星结构件/复合材料")
    graph.add_company("创远信科", ticker="831167.BJ",
                      product="微波测试仪器")

    # ---- Ground Equipment ----
    graph.add_company("信科移动", ticker="688387.SH",
                      product="卫星通信终端/基站")
    graph.add_company("华力创通", ticker="300045.SZ",
                      product="卫星导航/仿真测试")
    graph.add_company("海格通信", ticker="002465.SZ",
                      product="北斗终端/卫星通信")

    # ---- Materials ----
    graph.add_material("T800碳纤维", supplier="中复神鹰/光威复材")
    graph.add_material("特种合金(高温)", supplier="钢研高纳/抚顺特钢")
    graph.add_material("碳碳复合材料", note="火箭喷管/热防护")

    # ---- Core Technologies ----
    graph.add_technology("可回收火箭技术", note="垂直着陆回收")
    graph.add_technology("液氧甲烷发动机", note="天鹊/TQ-12")
    graph.add_technology("相控阵天线技术", note="卫星通信核心")
    graph.add_technology("星间激光链路", note="星座组网关键")
    graph.add_technology("一箭多星技术", note="降低发射成本")

    # ---- Key Relationships ----
    # 供应关系
    graph.add_supply("航天电器", "蓝箭航天", evidence="连接器/继电器")
    graph.add_supply("航天电器", "星河动力", evidence="连接器/继电器")
    graph.add_supply("航天电器", "航天科技集团", evidence="传统配套")
    graph.add_supply("铖昌科技", "垣信卫星", evidence="T/R芯片→千帆卫星")
    graph.add_supply("上海瀚讯", "垣信卫星", evidence="星载通信设备")
    graph.add_supply("臻镭科技", "垣信卫星", evidence="微波芯片组件")
    graph.add_supply("航天环宇", "蓝箭航天", evidence="卫星结构件")
    graph.add_supply("格思航天", "垣信卫星", evidence="卫星载荷")

    # 制造关系
    graph.add_manufacture("蓝箭航天", "朱雀二号")
    graph.add_manufacture("蓝箭航天", "朱雀三号")
    graph.add_manufacture("星河动力", "谷神星一号")
    graph.add_manufacture("星河动力", "智神星一号")
    graph.add_manufacture("长光卫星", "吉林一号")
    graph.add_manufacture("银河航天", "低轨宽带卫星")
    graph.add_manufacture("垣信卫星", "千帆卫星")

    # 发射服务→项目
    graph.add_supply("蓝箭航天", "千帆星座", evidence="发射服务")
    graph.add_supply("星河动力", "千帆星座", evidence="发射服务承接")

    # 赛道归属
    for c in ["蓝箭航天", "星河动力", "星际荣耀", "中科宇航",
              "天兵科技", "东方空间"]:
        graph.add_belongs_to(c, "发射服务")
        graph.add_belongs_to(c, "商业航天")

    for c in ["长光卫星", "微纳星空", "银河航天", "垣信卫星", "格思航天"]:
        graph.add_belongs_to(c, "卫星制造")
        graph.add_belongs_to(c, "商业航天")

    for c in ["上海瀚讯", "铖昌科技", "臻镭科技", "航天环宇", "航天电器"]:
        graph.add_belongs_to(c, "商业航天")

    for c in ["信科移动", "华力创通", "海格通信"]:
        graph.add_belongs_to(c, "地面设备")
        graph.add_belongs_to(c, "商业航天")

    # 竞争关系
    graph.add_competition("蓝箭航天", "星河动力")
    graph.add_competition("蓝箭航天", "星际荣耀")
    graph.add_competition("蓝箭航天", "天兵科技")
    graph.add_competition("铖昌科技", "臻镭科技")
    graph.add_competition("千帆星座", "中国星网")

    # 技术使能
    graph.add_enables("可回收火箭技术", "朱雀三号")
    graph.add_enables("液氧甲烷发动机", "朱雀二号")
    graph.add_enables("相控阵天线技术", "千帆卫星")
    graph.add_enables("星间激光链路", "千帆星座")
    graph.add_enables("一箭多星技术", "谷神星一号")

    # 材料供应
    graph.add_supply("T800碳纤维", "航天环宇", evidence="复合材料原材料")
    graph.add_supply("特种合金(高温)", "蓝箭航天", evidence="发动机材料")

    logger.info("seeded commercial aerospace chain: %s", graph.stats())
