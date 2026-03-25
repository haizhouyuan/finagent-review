# 两轮车事业部试点蓝图 v5

> v4 → v5 修订：补齐孤立节点边、中文检索策略。
> 本 v5 已对齐现有 finagent 代码库真实接口，可交给 coding agent 直接开工。

---

## 0. 现有接口实况（coding agent 必须先读）

### GraphStore — 已完备，不需要新建持久化

| 接口 | 路径 | 说明 |
|------|------|------|
| `GraphStore(db_path)` | [store.py](file:///vol1/1000/projects/finagent/finagent/graph_v2/store.py) | 构造函数自动 `_ensure_tables()` + `_load_from_db()` |
| SQLite tables | `kg_nodes` / `kg_edges` / `kg_entity_aliases` | 已有 DDL，write-through |
| `add_node(id, type, label)` | L158 | Upsert 到 SQLite + NetworkX |
| `add_edge(src, tgt, type)` | L258 | 自动创建缺失节点 |
| `merge_edge(...)` | L332 | LightRAG 式去重，confidence 高覆盖低 |
| `add_alias(alias, canonical_id)` | L497 | 实体别名 |
| `resolve_alias(name)` | L510 | 别名解析 |
| `stats()` | L520 | 节点/边类型分布 |
| `search_nodes(q)` | L240 | label 子串匹配 |

**GraphStore 默认 db_path**: `finagent.db`（项目根目录），当前为空图（45KB，tables 存在但无数据）。
**不是** `state/finagent.sqlite`（那是 thesis OS 的库）。

### NodeType / EdgeType — 已有枚举

| 枚举 | 路径 | 已有值 |
|------|------|--------|
| `NodeType` | [ontology.py](file:///vol1/1000/projects/finagent/finagent/graph_v2/ontology.py) L24 | COMPANY, COMPONENT, TECHNOLOGY, SECTOR, POLICY, ENTITY, ... |
| `EdgeType` | ontology.py L43 | SUPPLIES_CORE_PART_TO, CUSTOMER_OF, MANUFACTURES, COMPONENT_OF, COMPETES_WITH, ENABLES, REGULATES, ... |

两轮车领域需要的边类型（如 `supplies_core_part_to`, `manufactures`, `competes_with`, `component_of`, `enables`, `regulates`, `customer_of`）**全部已存在**。不需要新增 EdgeType。

### EvidenceStore — 无 search()

| 接口 | 路径 | 说明 |
|------|------|------|
| `store(query, raw_text, source_type=, source_tier=, source_uri=, published_at=)` | [evidence_store.py](file:///vol1/1000/projects/finagent/finagent/agents/evidence_store.py) L86 | 写入，返回 lightweight metadata dict（不含 raw_text） |
| `fetch(id)` | L126 | 按 ID 取单条 |
| `fetch_batch(ids)` | L134 | 批量取 |
| `list_all(run_id=)` | L145 | 按 run_id 列出 |

**❌ 没有 `search(query)`**。需要新增。

### Orchestrator 依赖注入风格

```python
# 现有模式 (闭包捕获，不是 state 透传)
def _planner(state):
    return planner_node(state, llm_fn=llm_fn, graph_store=graph_store,
                        graph_retriever=_graph_retriever)

def _extractor(state):
    return extractor_node(state, llm_fn=llm_fn, graph_store=graph_store,
                          evidence_store=evidence_store)
# 注意：entity_resolver 当前不是 extractor_node 的显式参数，
# 而是 extractor 内部根据 graph_store 构建的。
```

**所有新依赖（memory_manager, retrieval_stack）必须走同样模式**。
**绝对不要** 把 manager 对象塞进 LangGraph state。

---

## P1: 试点记忆核心 (memory.py + memory_consolidation.py)

### 目标

两轮车领域的 episodic/semantic 持久记忆 + promotion 契约。

### [NEW] `finagent/memory.py` (~200 行)

```python
"""Two-wheeler domain memory: episodic capture + semantic promotion."""

class MemoryTier(str, Enum):
    WORKING = "working"
    EPISODIC = "episodic"
    SEMANTIC = "semantic"

# ── Category 白名单 ──
EPISODIC_CATEGORIES = {
    "brand_observation", "product_spec", "price_change",
    "market_event", "field_research", "competitor_move",
    "supply_chain", "research_finding",
}
SEMANTIC_CATEGORIES = {
    "brand_positioning", "market_structure", "price_band",
    "technology_trend", "supply_chain_map", "regulatory",
}

@dataclass
class MemoryRecord:
    record_id: str
    tier: MemoryTier
    category: str
    content: str
    structured_data: dict    # JSON-able
    source_run_id: str
    source_type: str         # "extractor" | "evaluator" | "human" | "seed"
    confidence: float
    created_at: str
    updated_at: str
    access_count: int = 0
    promoted_from: str | None = None

class MemoryManager:
    """Backed by memory_records table in state/finagent.sqlite."""

    def __init__(self, conn: sqlite3.Connection):
        # conn 是 state/finagent.sqlite 的连接
        self.conn = conn
        self._ensure_table()

    def _ensure_table(self): ...  # CREATE TABLE IF NOT EXISTS memory_records
    def store_episodic(self, category, content, *, run_id, source_type="extractor",
                       confidence=0.7, structured_data=None) -> str: ...
    def store_working(self, content, *, run_id) -> str: ...
    def recall(self, query: str, *, tier=None, limit=10) -> list[MemoryRecord]: ...
    def get_by_category(self, category, *, tier=None) -> list[MemoryRecord]: ...
    def count_by_tier(self) -> dict[str, int]: ...
    def promote_to_semantic(self, episodic_ids, semantic_content, semantic_category,
                            confidence) -> str: ...
    def expire_working(self) -> int: ...
```

**DB 位置**: `state/finagent.sqlite`（和 thesis OS 同一个连接）。
**recall 实现**: SQLite FTS5 on `content` column，或 LIKE 子串匹配（先做简单版，后续可加 FTS）。

### [NEW] `finagent/memory_consolidation.py` (~150 行)

```python
"""Semantic promotion engine with strict schema contract."""

@dataclass
class SemanticCandidate:
    category: str          # 必须在 SEMANTIC_CATEGORIES
    conclusion: str        # 一句话稳定结论
    evidence_ids: list[str]  # 支撑的 episodic record IDs
    brands_involved: list[str]
    confidence: float      # >= 0.8
    valid_from: str
    supersedes: str | None = None

PROMOTION_RULES = {
    "brand_positioning": 3,   # 至少 3 条 episodic
    "market_structure": 2,
    "price_band": 2,
    "technology_trend": 3,
    "supply_chain_map": 2,
    "regulatory": 1,
}

def find_promotion_candidates(memory: MemoryManager, *, llm_fn=None) -> list[SemanticCandidate]: ...
def execute_promotion(memory: MemoryManager, candidates: list[SemanticCandidate],
                      *, dry_run=True) -> list[str]: ...
```

### [NEW] `tests/test_memory.py` + `tests/test_memory_consolidation.py`

覆盖：
- 三层存储 / recall / category 白名单 / tier 计数
- promotion 规则：confidence < 0.8 拒绝 / support count 不足拒绝 / dry_run
- 幂等性：同内容不重复 promote

---

## P2: 两轮车领域图谱 seed + hygiene

### 目标

给 `finagent.db` 填入**可用的**两轮车知识图谱，加 alias，加可观测 hygiene metrics。

### [NEW] `scripts/seed_two_wheeler_graph.py` (~250 行)

使用 **现有** `GraphStore` API: `add_node()`, `add_edge()`, `merge_edge()`, `add_alias()`。

```python
from finagent.graph_v2.store import GraphStore
from finagent.graph_v2.ontology import NodeType, EdgeType

def seed_two_wheeler_graph(db_path: str = "finagent.db"):
    store = GraphStore(db_path)

    # ── 品牌节点 (~9) ──
    brands = {
        "yadea":    ("雅迪", NodeType.COMPANY),
        "aima":     ("爱玛", NodeType.COMPANY),
        "ninebot":  ("九号", NodeType.COMPANY),
        "tailg":    ("台铃", NodeType.COMPANY),
        "niu":      ("小牛", NodeType.COMPANY),
        "xinri":    ("新日", NodeType.COMPANY),
        "luyuan":   ("绿源", NodeType.COMPANY),
        "jinggu":   ("金谷/JG", NodeType.COMPANY),
        "chunfeng": ("春风动力", NodeType.COMPANY),
    }
    for nid, (label, ntype) in brands.items():
        store.add_node(nid, ntype, label)

    # ── 零部件节点 (~8) ──
    components = {
        "aluminum_wheel":   ("铝合金轮毂", NodeType.COMPONENT),
        "steel_wheel":      ("钢轮毂", NodeType.COMPONENT),
        "frame":            ("车架", NodeType.COMPONENT),
        "motor":            ("电机", NodeType.COMPONENT),
        "battery":          ("电池", NodeType.COMPONENT),
        "controller":       ("控制器", NodeType.COMPONENT),
        "brake_system":     ("制动系统", NodeType.COMPONENT),
        "lighting":         ("灯具系统", NodeType.COMPONENT),
    }
    for nid, (label, ntype) in components.items():
        store.add_node(nid, ntype, label)

    # ── 产品线节点 (~8) ──
    product_lines = {
        "yadea_guanneng":  ("冠能系列", NodeType.PROJECT),
        "yadea_dm":        ("DM系列", NodeType.PROJECT),
        "ninebot_fz":      ("Fz系列", NodeType.PROJECT),
        "aima_a_series":   ("A系列", NodeType.PROJECT),
        "tailg_n_series":  ("N系列", NodeType.PROJECT),
        "niu_nqi":         ("NQi系列", NodeType.PROJECT),
        "xinri_xc":        ("XC系列", NodeType.PROJECT),
        "luyuan_s_series": ("S系列", NodeType.PROJECT),
    }
    for nid, (label, ntype) in product_lines.items():
        store.add_node(nid, ntype, label)

    # ── 技术节点 (~4) ──
    techs = {
        "graphene_battery":   ("石墨烯电池", NodeType.TECHNOLOGY),
        "sodium_battery":     ("钠离子电池", NodeType.TECHNOLOGY),
        "hub_motor":          ("轮毂电机", NodeType.TECHNOLOGY),
        "mid_motor":          ("中置电机", NodeType.TECHNOLOGY),
    }
    for nid, (label, ntype) in techs.items():
        store.add_node(nid, ntype, label)

    # ── 政策/标准 (~2) ──
    store.add_node("gb17761", NodeType.POLICY, "新国标 GB17761-2018")
    store.add_node("ev_market_seg", NodeType.SECTOR, "两轮电动车市场")

    # ── 边 (~70) ──
    VF = "2025-01-01"  # valid_from

    # 品牌 → 产品线 (manufactures)
    for brand, pline in [("yadea","yadea_guanneng"),("yadea","yadea_dm"),
                         ("ninebot","ninebot_fz"),("aima","aima_a_series"),
                         ("tailg","tailg_n_series"),("niu","niu_nqi"),
                         ("xinri","xinri_xc"),("luyuan","luyuan_s_series")]:
        store.merge_edge(brand, pline, EdgeType.MANUFACTURES, valid_from=VF,
                         confidence=0.95, source="seed")

    # JG 供应关系 (supplies_core_part_to)
    for customer in ["yadea","aima","ninebot","tailg","niu","xinri","luyuan"]:
        store.merge_edge("jinggu", customer, EdgeType.SUPPLIES_CORE_PART_TO,
                         valid_from=VF, confidence=0.85, source="seed",
                         evidence="JG 铝轮毂供应")

    # 零部件 → 产品线 (component_of)
    for pline in ["yadea_guanneng","ninebot_fz","aima_a_series","tailg_n_series","niu_nqi"]:
        for comp in ["aluminum_wheel","motor","battery","controller","frame"]:
            store.merge_edge(comp, pline, EdgeType.COMPONENT_OF,
                             valid_from=VF, confidence=0.8, source="seed")

    # 竞争关系 (competes_with) — 双向写入
    # competitors_of() 只查 out_edges，所以必须写 a→b 和 b→a
    top_brands = ["yadea","aima","ninebot","tailg","niu"]
    for i, a in enumerate(top_brands):
        for b in top_brands[i+1:]:
            store.merge_edge(a, b, EdgeType.COMPETES_WITH,
                             valid_from=VF, confidence=0.9, source="seed")
            store.merge_edge(b, a, EdgeType.COMPETES_WITH,
                             valid_from=VF, confidence=0.9, source="seed")

    # 技术 → 产品线 (enables)
    store.merge_edge("graphene_battery", "yadea_guanneng", EdgeType.ENABLES,
                     valid_from=VF, confidence=0.8, source="seed")
    store.merge_edge("hub_motor", "ninebot_fz", EdgeType.ENABLES,
                     valid_from=VF, confidence=0.8, source="seed")
    store.merge_edge("sodium_battery", "tailg_n_series", EdgeType.ENABLES,
                     valid_from=VF, confidence=0.7, source="seed")
    store.merge_edge("mid_motor", "niu_nqi", EdgeType.ENABLES,
                     valid_from=VF, confidence=0.7, source="seed")

    # 钢轮 + 制动 + 灯具 → 产品线 (component_of)
    for pline in ["yadea_guanneng","aima_a_series","xinri_xc","luyuan_s_series"]:
        store.merge_edge("steel_wheel", pline, EdgeType.COMPONENT_OF,
                         valid_from=VF, confidence=0.7, source="seed")
    for pline in ["yadea_guanneng","ninebot_fz","aima_a_series","tailg_n_series","niu_nqi"]:
        for comp in ["brake_system", "lighting"]:
            store.merge_edge(comp, pline, EdgeType.COMPONENT_OF,
                             valid_from=VF, confidence=0.7, source="seed")

    # 春风动力 — 与 JG 合作、属于两轮车市场
    store.merge_edge("chunfeng", "jinggu", EdgeType.PARTNERS_WITH,
                     valid_from=VF, confidence=0.75, source="seed",
                     evidence="春风动力与金谷铝轮毬合作")
    store.merge_edge("chunfeng", "ev_market_seg", EdgeType.BELONGS_TO,
                     valid_from=VF, confidence=0.8, source="seed")

    # 新国标约束
    store.merge_edge("gb17761", "ev_market_seg", EdgeType.REGULATES,
                     valid_from="2019-04-15", confidence=0.99, source="seed")

    # ── 别名 ──
    aliases = {
        "雅迪": "yadea", "yadea": "yadea", "Yadea": "yadea",
        "爱玛": "aima", "aima": "aima", "Aima": "aima",
        "九号": "ninebot", "ninebot": "ninebot", "Ninebot": "ninebot", "segway": "ninebot",
        "台铃": "tailg", "tailg": "tailg", "Tailg": "tailg",
        "小牛": "niu", "niu": "niu", "NIU": "niu",
        "新日": "xinri", "绿源": "luyuan",
        "金谷": "jinggu", "JG": "jinggu", "jg": "jinggu",
        "春风": "chunfeng", "春风动力": "chunfeng",
    }
    for alias, canonical in aliases.items():
        store.add_alias(alias, canonical)

    stats = store.stats()
    print(f"Seeded: {stats['total_nodes']} nodes, {stats['total_edges']} edges")
    store.close()
```

### [NEW] `scripts/graph_hygiene_report.py` (~80 行)

```python
"""Graph hygiene metrics for observability."""
def report(db_path="finagent.db"):
    store = GraphStore(db_path)
    stats = store.stats()
    # Orphan nodes (degree 0)
    orphans = [n for n in store.g.nodes() if store.g.degree(n) == 0]
    # Alias coverage
    alias_count = store.conn.execute("SELECT count(*) FROM kg_entity_aliases").fetchone()[0]
    print(f"Nodes: {stats['total_nodes']}")
    print(f"Edges: {stats['total_edges']}")
    print(f"Orphans: {len(orphans)} ({len(orphans)/max(stats['total_nodes'],1)*100:.0f}%)")
    print(f"Aliases: {alias_count}")
    print(f"Node types: {stats['node_types']}")
    print(f"Edge types: {stats['edge_types']}")
```

### [NEW] `tests/test_two_wheeler_graph.py`

- Seed → `stats()` 报 ~34 nodes, ~100+ edges, **0 orphans**
- `resolve_alias("雅迪")` → `"yadea"`
- `GraphRetriever.retrieve("九号铝轮毂供应链")` 返回含 "金谷" 的上下文
- `store.upstream_of("yadea")` 返回 jinggu
- `store.competitors_of("yadea")` 返回其他品牌
- **所有节点都有至少 1 条边**（chunfeng/steel_wheel/brake_system/lighting/sodium_battery/mid_motor 已接入）

---

## P3: Retrieval substrate

### 目标

统一检索底座：memory + graph + evidence → search → compress。
rewrite / rerank 做成 feature-gated（有 LLM 时增强，没有时回退）。

### 前置：补 EvidenceStore.search()

#### [MODIFY] `finagent/agents/evidence_store.py`

> **关键接口事实**：`list_all()` 只返回 metadata（不含 raw_text），见 L146。
> `raw_text` 存在 `evidence_store` 表的 `raw_text` 列里。
> 所以 `search()` 必须直接走 SQL LIKE 查询，不能用 `list_all()` 后内存过滤。
>
> **中文查询策略**：两轮车试点大量查询是不带空格的中文串（如“九号铝轮毬供应链”），
> `split()` 对中文无效。必须用“整串 LIKE + CJK bigram 回退”两层策略。

```python
# ── 中文拆词 helpers ──

_BRAND_TERMS = {"雅迪","爱玛","九号","台铃","小牛","新日","绿源","金谷","春风"}
_COMPONENT_TERMS = {"铝轮毬","钢轮毬","车架","电机","电池","控制器","制动","灯具"}
_DOMAIN_DICT = _BRAND_TERMS | _COMPONENT_TERMS | {"供应链","竞争","新国标","石墨烯","钠离子"}

def _tokenize_cjk(query: str) -> list[str]:
    """CJK-aware tokenization: dictionary match first, then bigrams."""
    tokens = []
    # 1. 先抽离已知领域词汇
    remaining = query
    for term in sorted(_DOMAIN_DICT, key=len, reverse=True):
        if term in remaining:
            tokens.append(term)
            remaining = remaining.replace(term, " ", 1)
    # 2. 剩余部分用 bigram
    for segment in remaining.split():
        segment = segment.strip()
        if len(segment) >= 2:
            for i in range(len(segment) - 1):
                tokens.append(segment[i:i+2])
    return tokens if tokens else [query]  # 全部失败时回退整串

def search(self, query: str, *, run_id: str | None = None, limit: int = 10) -> list[dict]:
    """CJK-aware keyword search on raw evidence text via SQL LIKE.

    Returns metadata dicts (same 形态 as list_all)，加 _score 字段。
    不返回 raw_text（保持 reference-passing 约定）。
    需要原文时用 fetch(evidence_id)。

    检索策略：
    1. 先尝试整串 LIKE — 对无空格中文最有效
    2. 如果整串命中不足，回退到领域词典拆词 + CJK bigram
    """
    query = query.strip()
    if not query:
        return []

    base_conditions: list[str] = []
    base_params: list = []
    if run_id:
        base_conditions.append("run_id = ?")
        base_params.append(run_id)

    # ── Strategy 1: whole-query LIKE ──
    conds = base_conditions + ["raw_text LIKE ?"]
    params = base_params + [f"%{query}%"]
    rows = self.conn.execute(
        f"SELECT evidence_id, query, char_count, source_type, "
        f"source_tier, source_uri, published_at "
        f"FROM evidence_store WHERE {' AND '.join(conds)} "
        f"ORDER BY evidence_id DESC LIMIT ?",
        params + [limit],
    ).fetchall()

    if len(rows) >= 3:
        return self._format_results(rows, score=1.0)

    # ── Strategy 2: tokenize + multi-LIKE ──
    tokens = _tokenize_cjk(query)
    # 取前 4 个 token 避免过拟合
    token_conds = ["raw_text LIKE ?" for _ in tokens[:4]]
    token_params = [f"%{t}%" for t in tokens[:4]]
    all_conds = base_conditions + token_conds
    all_params = base_params + token_params
    rows2 = self.conn.execute(
        f"SELECT evidence_id, query, char_count, source_type, "
        f"source_tier, source_uri, published_at "
        f"FROM evidence_store WHERE {' AND '.join(all_conds)} "
        f"ORDER BY evidence_id DESC LIMIT ?",
        all_params + [limit],
    ).fetchall()

    # 合并去重
    seen = {r["evidence_id"] for r in rows}
    combined = list(rows) + [r for r in rows2 if r["evidence_id"] not in seen]
    return self._format_results(combined[:limit], score=len(tokens))

def _format_results(self, rows, *, score) -> list[dict]:
    return [
        {
            "id": r["evidence_id"],
            "query": r["query"],
            "char_count": r["char_count"],
            "source_type": r["source_type"],
            "source_tier": r["source_tier"] or "unverified",
            "source_uri": r["source_uri"] or "",
            "published_at": r["published_at"] or "",
            "_score": score,
        }
        for r in rows
    ]
```

> **RetrievalStack 消费方式**：`search()` 返回 metadata refs；
> 如需原文用于 rerank/compress，用 `fetch_batch([r["id"] for r in results])`。

### [NEW] `finagent/retrieval_stack.py` (~250 行)

```python
"""Unified retrieval: memory + graph + evidence → compress."""

@dataclass
class RetrievalResult:
    source: str      # "memory" | "graph" | "evidence"
    query: str
    content: str
    score: float
    metadata: dict = field(default_factory=dict)

class RetrievalStack:
    def __init__(self, *, graph_store=None, memory=None,
                 evidence_store=None, llm_fn=None):
        self.graph_retriever = GraphRetriever(graph_store) if graph_store else None
        self.memory = memory
        self.evidence_store = evidence_store
        self.llm_fn = llm_fn

    def retrieve(self, query, *, top_k=5, max_chars=4000) -> str:
        """Full pipeline: [rewrite] → search → [rerank] → compress."""
        # Stage 1: rewrite (feature-gated)
        queries = self._rewrite(query) if self.llm_fn else [query]
        # Stage 2: multi-source search
        results = self._search(queries)
        # Stage 3: rerank (feature-gated)
        ranked = self._rerank(query, results, top_k) if self.llm_fn else self._sort_by_score(results, top_k)
        # Stage 4: compress
        return self._compress(query, ranked, max_chars)

    def _rewrite(self, query) -> list[str]: ...      # LLM sub-query expansion
    def _search(self, queries) -> list[RetrievalResult]: ...  # memory + graph + evidence
    def _rerank(self, query, results, top_k) -> list[RetrievalResult]: ...  # LLM relevance scoring
    def _sort_by_score(self, results, top_k) -> list[RetrievalResult]: ...  # fallback sort
    def _compress(self, query, results, max_chars) -> str: ...  # budget-aware concat
```

**关键约束**：
- 没有 LLM 时，rewrite 退化为原始 query，rerank 退化为 score 排序
- 没有 memory/graph/evidence 时，对应 source 跳过（不报错）
- compress 硬限 `max_chars`

### [NEW] `tests/test_retrieval_stack.py`

- multi-source search 返回 memory + graph + evidence 结果
- 无 LLM 时 full pipeline 仍然工作
- compress 截断到 budget
- 单 source 模式工作

---

## P4: Corrective retrieval loop

### 目标

searcher 节点内的同步 evaluate-retry 循环。

### [MODIFY] `finagent/agents/searcher.py`

> **关键契约**：`gathered_evidence` 必须继续只传 evidence refs（lightweight metadata dicts），
> 不能传 raw text 或 `RetrievalResult` 对象。这是 reference-passing 设计的核心约束。
> corrective loop 只改变 query / 检索路径，不改变 state 契约。

```python
def searcher_node(state, *, search_fn=None, evidence_store=None,
                  retrieval_stack=None,    # 新增
                  llm_fn=None,             # 新增（用于 evaluate）
                  max_retries=2):
    queries = state.get("pending_queries", [])
    evidence_refs = list(state.get("gathered_evidence", []))

    for q_info in queries:
        query = q_info["query"] if isinstance(q_info, dict) else q_info

        # _do_search 执行搜索，调 evidence_store.store() 落盘，
        # 返回 evidence refs（和现有行为一致）
        new_refs = _do_search(query, search_fn=search_fn,
                              evidence_store=evidence_store)

        # Corrective loop (feature-gated on llm_fn)
        if llm_fn and new_refs:
            retries = 0
            while retries < max_retries:
                # Evaluate: 取原文做质量判断
                texts = evidence_store.fetch_batch([r["evidence_id"] for r in new_refs])
                verdict = _evaluate_retrieval(query, texts, llm_fn)
                if verdict == "sufficient":
                    break
                # Rewrite query and retry
                query = _rewrite_for_retry(query, texts, llm_fn, retries)
                new_refs = _do_search(query, search_fn=search_fn,
                                      evidence_store=evidence_store)
                retries += 1

        evidence_refs.extend(new_refs)  # 只追加 refs，不追加 raw text

    return {"gathered_evidence": evidence_refs, ...}

def _evaluate_retrieval(query, results, llm_fn) -> str:
    """Returns 'sufficient' | 'insufficient'."""
    ...

def _rewrite_for_retry(query, results, llm_fn, attempt) -> str:
    """LLM rewrites query based on what's missing."""
    ...
```

### [NEW] `tests/test_corrective_loop.py`

- 好 query → 一次过，`_evaluate` 返回 "sufficient"
- 差 query → 重试 ≤ 2 次
- max_retries 到了 → graceful degrade（用已有结果）
- 无 llm_fn → 不评估不重试

---

## P5: Wiring + E2E

### [MODIFY] `finagent/agents/orchestrator.py`

在 `build_research_graph()` 和 `run_research()` 中注入 memory + retrieval_stack：

```python
def build_research_graph(
    ...,
    memory_manager=None,         # 新增
    retrieval_stack=None,        # 新增
):
    ...
    def _extractor(state):
        return extractor_node(state, ..., memory_manager=memory_manager)

    def _searcher(state):
        return searcher_node(state, ..., retrieval_stack=retrieval_stack, llm_fn=llm_fn)
```

### [MODIFY] `finagent/agents/extractor.py`

增加 `memory_manager` keyword arg，竞品提取后写 episodic：

```python
def extractor_node(state, *, llm_fn, graph_store=None, evidence_store=None,
                   memory_manager=None):  # 新增
    ...
    # 竞品提取完成后
    if memory_manager and (all_image_assets or all_sku_records):
        for asset in all_image_assets:
            memory_manager.store_episodic(
                category="product_spec",
                content=f"发现 {asset.get('brand','')} 产品图片: {asset.get('visible_content','')}",
                run_id=state.get("run_id", ""), source_type="extractor",
                structured_data=asset,
            )
        for sku in all_sku_records:
            memory_manager.store_episodic(
                category="product_spec",
                content=f"发现 {sku.get('brand','')} {sku.get('model','')}: {sku.get('price_range','')}",
                run_id=state.get("run_id", ""), source_type="extractor",
                structured_data=sku,
            )
```

### [MODIFY] `finagent/agents/planner.py`

在 user prompt 中可选注入 semantic memory summary：

```python
# 在 GraphRetriever context 之后、blind spots 之前
memory_summary = ""
if memory_manager:
    # recall(tier=SEMANTIC) 返回 semantic tier 的全部记录
    semantics = memory_manager.recall("", tier=MemoryTier.SEMANTIC, limit=10)
    if semantics:
        memory_summary = "\n## 已固化认知\n" + "\n".join(
            f"- [{s.category}] {s.content}" for s in semantics
        )
```

> MemoryManager 接口说明：
> - `recall(query, *, tier=None, limit=10)` — 按 query 匹配 + 可选 tier 过滤
> - `get_by_category(category, *, tier=None)` — 按 category 名称过滤
> 这里用 `recall("", tier=SEMANTIC)` 是因为要拿全部 semantic 记录，不是按特定 category。

### [NEW] `tests/test_pilot_e2e.py`

必须证明的 3 条链：

```python
class TestTwoWheelerPilotE2E:
    def test_research_produces_episodic(self):
        """run_research → extractor → episodic memory growth."""
        ...
        assert memory.count_by_tier()["episodic"] > 0

    def test_consolidation_produces_semantic(self):
        """episodic → find_candidates → promote → semantic growth."""
        ...
        assert memory.count_by_tier()["semantic"] > 0

    def test_competitive_writeback_no_pollution(self):
        """package → competitive-only writeback → thesis/source/monitor unchanged."""
        pre_theses = ...  # count before
        apply_writeback(actions, conn)
        post_theses = ...  # count after
        assert pre_theses == post_theses
```

---

## 实施顺序

```
P1 ────→ P3 ────→ P4 ────→ P5
P2 ────↗                     │
  (并行)                    E2E
```

| 包 | 新文件 | 改文件 | 预计行数 | 依赖 |
|----|--------|--------|----------|------|
| P1 | `memory.py`, `memory_consolidation.py`, 2 test files | — | ~400 | 无 |
| P2 | `seed_two_wheeler_graph.py`, `graph_hygiene_report.py`, 1 test file | — | ~400 | 无 |
| P3 | `retrieval_stack.py`, 1 test file | `evidence_store.py` (+search) | ~350 | P1, P2 |
| P4 | 1 test file | `searcher.py` | ~200 | P3 |
| P5 | 1 test file | `orchestrator.py`, `extractor.py`, `planner.py` | ~200 | P1-P4 |

**每个包完成后必须 `git commit`。**

---

## 硬约束

1. **不新建 graph 持久化** — 用现有 `GraphStore(db_path)` 原样
2. **不改 NodeType / EdgeType** — 现有枚举足够
3. **依赖注入走 closure/keyword arg** — 不塞进 LangGraph state
4. **不引入新 pip 依赖** — sqlite3 + networkx + langgraph
5. **LLM rewrite/rerank/evaluate 全部 feature-gated** — 无 LLM 时 graceful fallback
6. **测试用 mock LLM** — 不真调

---

## 成功标准

```python
# 图谱
store = GraphStore("finagent.db")
assert store.g.number_of_nodes() >= 30
assert store.g.number_of_edges() >= 60
assert store.resolve_alias("雅迪") == "yadea"

# 记忆
assert memory.count_by_tier()["episodic"] >= 10
assert memory.count_by_tier()["semantic"] >= 2

# 检索
ctx = stack.retrieve("九号铝轮毂供应链竞争格局")
assert len(ctx) > 100

# 竞品不污染
assert theses_count_after == theses_count_before
```
