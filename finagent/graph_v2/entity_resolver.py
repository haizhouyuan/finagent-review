"""Entity resolution and deduplication for the v2 knowledge graph.

Handles the critical problem of entity ambiguity:
  - 蓝箭航天 = LandSpace = 688245.SH = 浙江蓝箭航天技术有限公司
  - SpaceX = Space Exploration Technologies Corp

Layers:
  1. **Exact alias lookup** — fast O(1) from ``kg_entity_aliases`` table
  2. **Ticker / code resolution** — A-share ticker → canonical ID
  3. **Fuzzy matching** — edit distance + pinyin similarity for Chinese names
  4. **LLM arbitration** — for low-confidence matches (future extension)
"""

from __future__ import annotations

import logging
import re
from typing import Any

from .store import GraphStore

logger = logging.getLogger(__name__)


# ── Chinese text utilities ──────────────────────────────────────────

def _normalize_chinese(text: str) -> str:
    """Normalize Chinese company names for matching."""
    # Remove common suffixes
    suffixes = [
        "股份有限公司", "有限责任公司", "有限公司", "集团有限公司",
        "集团股份有限公司", "科技有限公司", "技术有限公司",
        "科技股份有限公司", "集团", "公司", "股份",
        "Co., Ltd.", "Co.,Ltd.", "Corp.", "Inc.", "Ltd.",
        "Corporation", "Limited",
    ]
    result = text.strip()
    for suffix in suffixes:
        if result.endswith(suffix):
            result = result[:-len(suffix)].strip()
            break

    # Remove common prefixes (province/city)
    prefixes = [
        "北京", "上海", "深圳", "广州", "杭州", "南京", "武汉",
        "成都", "重庆", "天津", "西安", "长沙", "苏州", "无锡",
        "合肥", "海南", "浙江", "江苏", "广东", "湖南", "湖北",
        "四川", "山东", "河南", "福建", "安徽", "中国",
    ]
    for prefix in prefixes:
        if result.startswith(prefix) and len(result) > len(prefix) + 2:
            result = result[len(prefix):]
            break

    return result.strip()


def _normalize_ticker(ticker: str) -> str:
    """Normalize stock ticker to standard format."""
    t = ticker.strip().upper()
    # Handle formats: 688245, 688245.SH, SH688245, 688245.SS
    t = re.sub(r"^(SH|SZ|BJ|HK)", "", t)
    t = re.sub(r"\.(SH|SS|SZ|BJ|HK)$", "", t)
    return t


def _edit_distance(a: str, b: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(a) < len(b):
        return _edit_distance(b, a)
    if len(b) == 0:
        return len(a)

    prev_row = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr_row = [i + 1]
        for j, cb in enumerate(b):
            cost = 0 if ca == cb else 1
            curr_row.append(min(
                curr_row[j] + 1,       # insert
                prev_row[j + 1] + 1,   # delete
                prev_row[j] + cost,    # substitute
            ))
        prev_row = curr_row

    return prev_row[-1]


def _similarity_score(a: str, b: str) -> float:
    """Compute similarity score between two strings (0.0 - 1.0)."""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    max_len = max(len(a), len(b))
    dist = _edit_distance(a, b)
    return 1.0 - dist / max_len


# ── Built-in alias table for common A-share entities ────────────────

_BUILTIN_ALIASES: dict[str, str] = {
    # Aerospace
    "蓝箭航天": "蓝箭航天",
    "landspace": "蓝箭航天",
    "688245": "蓝箭航天",
    "蓝箭航天技术": "蓝箭航天",
    "浙江蓝箭航天": "蓝箭航天",
    "spacex": "SpaceX",
    "space exploration technologies": "SpaceX",
    "星河动力": "星河动力",
    "galactic energy": "星河动力",
    "长光卫星": "长光卫星",
    "688568": "长光卫星",
    "chang guang satellite": "长光卫星",
    "航天科技集团": "航天科技集团",
    "casc": "航天科技集团",
    "中国运载火箭技术研究院": "中国运载火箭技术研究院",
    "一院": "中国运载火箭技术研究院",
    "calt": "中国运载火箭技术研究院",
    # Components
    "航天电器": "航天电器",
    "002025": "航天电器",
    "铖昌科技": "铖昌科技",
    "001270": "铖昌科技",
    "臻镭科技": "臻镭科技",
    "688270": "臻镭科技",
    "上海瀚讯": "上海瀚讯",
    "300762": "上海瀚讯",
    "航天环宇": "航天环宇",
    "688523": "航天环宇",
}


class EntityResolver:
    """Multi-layer entity resolution engine.

    Resolution order:
      1. Exact alias lookup (DB + built-in table)
      2. Ticker-based resolution
      3. Normalized Chinese name matching
      4. Fuzzy matching with configurable threshold
    """

    def __init__(
        self,
        store: GraphStore,
        *,
        fuzzy_threshold: float = 0.80,
        auto_merge: bool = True,
    ):
        self.store = store
        self.fuzzy_threshold = fuzzy_threshold
        self.auto_merge = auto_merge
        self._pending_reviews: list[dict[str, Any]] = []

    def resolve(self, name: str, *, context: str = "") -> ResolveResult:
        """Resolve an entity name to a canonical node_id.

        Returns a ResolveResult with:
          - ``canonical_id``: the resolved node ID (may be the input itself
            if no match found)
          - ``confidence``: how confident we are in the match
          - ``method``: which resolution layer matched
          - ``is_new``: True if no match was found (entity is new)
        """
        cleaned = name.strip()
        if not cleaned:
            return ResolveResult(cleaned, 0.0, "empty", True)

        lowered = cleaned.lower()

        # Layer 1: Exact alias lookup (DB)
        db_match = self.store.resolve_alias(lowered)
        if db_match and self.store.has_node(db_match):
            return ResolveResult(db_match, 1.0, "db_alias", False)

        # Layer 1b: Built-in alias table
        if lowered in _BUILTIN_ALIASES:
            canonical = _BUILTIN_ALIASES[lowered]
            return ResolveResult(canonical, 0.98, "builtin_alias", False)

        # Layer 2: Ticker-based resolution
        ticker_match = self._resolve_by_ticker(cleaned)
        if ticker_match:
            return ticker_match

        # Layer 3: Normalized name matching
        norm_match = self._resolve_by_normalized_name(cleaned)
        if norm_match:
            return norm_match

        # Layer 4: Fuzzy matching
        fuzzy_match = self._resolve_by_fuzzy(cleaned)
        if fuzzy_match:
            return fuzzy_match

        # No match found — entity is new
        return ResolveResult(cleaned, 0.0, "no_match", True)

    def resolve_or_create(
        self,
        name: str,
        node_type: str = "entity",
        *,
        context: str = "",
        attrs: dict[str, Any] | None = None,
    ) -> str:
        """Resolve entity, creating it if new. Returns canonical node_id."""
        from .ontology import NodeType

        result = self.resolve(name, context=context)

        if result.is_new:
            # Create the node
            try:
                nt = NodeType(node_type)
            except ValueError:
                nt = NodeType.ENTITY
            node_id = self.store.add_node(
                name, nt, name,
                attrs=attrs or {},
            )
            # Register the lowered name as alias
            self.store.add_alias(name.lower(), node_id, "name")
            return node_id

        # Register alias if resolve was fuzzy or builtin
        if result.method in ("builtin_alias", "fuzzy") and result.confidence < 1.0:
            self.store.add_alias(name.lower(), result.canonical_id, "fuzzy")

        return result.canonical_id

    def _resolve_by_ticker(self, name: str) -> ResolveResult | None:
        """Try to match by stock ticker."""
        normalized = _normalize_ticker(name)
        if not re.match(r"^\d{6}$", normalized):
            return None

        # Search nodes for matching ticker
        for n, d in self.store.g.nodes(data=True):
            ticker = str(d.get("ticker", ""))
            if _normalize_ticker(ticker) == normalized:
                return ResolveResult(n, 0.95, "ticker", False)

        return None

    def _resolve_by_normalized_name(self, name: str) -> ResolveResult | None:
        """Try exact match after Chinese name normalization."""
        normalized = _normalize_chinese(name)
        if normalized == name:
            return None  # No normalization happened

        # Check if normalized name matches any node
        if self.store.has_node(normalized):
            return ResolveResult(normalized, 0.90, "normalized", False)

        # Check if normalized name matches any label
        for n, d in self.store.g.nodes(data=True):
            node_label = str(d.get("label", ""))
            if _normalize_chinese(node_label) == normalized:
                return ResolveResult(n, 0.88, "normalized_label", False)

        return None

    def _resolve_by_fuzzy(self, name: str) -> ResolveResult | None:
        """Fuzzy string matching against existing nodes."""
        best_match = None
        best_score = 0.0
        normalized = _normalize_chinese(name).lower()

        for n, d in self.store.g.nodes(data=True):
            label = _normalize_chinese(str(d.get("label", n))).lower()
            score = _similarity_score(normalized, label)

            if score > best_score and score >= self.fuzzy_threshold:
                best_score = score
                best_match = n

        if best_match:
            if self.auto_merge and best_score >= 0.90:
                return ResolveResult(best_match, best_score, "fuzzy_auto", False)
            else:
                # Queue for human review if below auto-merge threshold
                self._pending_reviews.append({
                    "input": name,
                    "candidate": best_match,
                    "score": best_score,
                })
                if best_score >= self.fuzzy_threshold:
                    return ResolveResult(best_match, best_score, "fuzzy", False)

        return None

    @property
    def pending_reviews(self) -> list[dict[str, Any]]:
        """Get pending fuzzy matches awaiting human confirmation."""
        return list(self._pending_reviews)

    def clear_pending(self) -> None:
        self._pending_reviews.clear()

    def bulk_register_aliases(self, aliases: dict[str, str]) -> int:
        """Register multiple aliases at once. Returns count registered."""
        count = 0
        for alias, canonical in aliases.items():
            if self.store.has_node(canonical):
                self.store.add_alias(alias.lower(), canonical, "bulk")
                count += 1
            else:
                logger.warning("bulk alias skip: canonical '%s' not in graph", canonical)
        return count


class ResolveResult:
    """Result from entity resolution."""

    __slots__ = ("canonical_id", "confidence", "method", "is_new")

    def __init__(self, canonical_id: str, confidence: float, method: str, is_new: bool):
        self.canonical_id = canonical_id
        self.confidence = confidence
        self.method = method
        self.is_new = is_new

    def __repr__(self) -> str:
        return (
            f"ResolveResult(id={self.canonical_id!r}, "
            f"conf={self.confidence:.2f}, method={self.method}, "
            f"new={self.is_new})"
        )
