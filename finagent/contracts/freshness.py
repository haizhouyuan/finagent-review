from __future__ import annotations

import re
from datetime import date, datetime, timezone

from .base import ContractWarning


_FULL_DATE_PATTERNS = [
    re.compile(r"\b(20\d{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12]\d|3[01])\b"),
    re.compile(r"(20\d{2})年(0?[1-9]|1[0-2])月(0?[1-9]|[12]\d|3[01])日"),
]
# Quarter: "2025 Q2", "2025Q2", "Q2 2025", "2025年Q2"
_QUARTER_PATTERN = re.compile(
    r"\b(20\d{2})\s*[Qq](1|2|3|4)\b"          # 2025Q2, 2025 Q2
    r"|(?:^|\W)[Qq](1|2|3|4)\s+(20\d{2})\b"   # Q2 2025
    r"|(20\d{2})年\s*[Qq](1|2|3|4)"            # 2025年Q2
)
# Half: "2025 H1", "H1 2025", "2025年H1"
_HALF_PATTERN = re.compile(
    r"\b(20\d{2})\s*[Hh](1|2)\b"               # 2025H1, 2025 H1
    r"|(?:^|\W)[Hh](1|2)\s+(20\d{2})\b"        # H1 2025
    r"|(20\d{2})年\s*[Hh](1|2)"                 # 2025年H1
)
_YEAR_PATTERN = re.compile(r"(?:^|\W)(20\d{2})(?:年|\b)")


def _safe_iso(year: int, month: int = 1, day: int = 1) -> str:
    return date(year, month, day).isoformat()


def extract_data_date(text: str, fallback_iso: str = "") -> str:
    for pattern in _FULL_DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            year, month, day = (int(match.group(i)) for i in range(1, 4))
            return _safe_iso(year, month, day)

    quarter_match = _QUARTER_PATTERN.search(text)
    if quarter_match:
        # groups: (1,2) = year-first, (3,4) = Q-first, (5,6) = Chinese
        year = int(quarter_match.group(1) or quarter_match.group(4) or quarter_match.group(5))
        quarter = int(quarter_match.group(2) or quarter_match.group(3) or quarter_match.group(6))
        month = {1: 3, 2: 6, 3: 9, 4: 12}[quarter]
        return _safe_iso(year, month, 1)

    half_match = _HALF_PATTERN.search(text)
    if half_match:
        year = int(half_match.group(1) or half_match.group(4) or half_match.group(5))
        half = int(half_match.group(2) or half_match.group(3) or half_match.group(6))
        month = 6 if half == 1 else 12
        return _safe_iso(year, month, 1)

    year_match = _YEAR_PATTERN.search(text)
    if year_match:
        return _safe_iso(int(year_match.group(1)), 1, 1)

    return fallback_iso


def freshness_status_for_date(data_date: str, *, today: date | None = None) -> str:
    if not data_date:
        return "unknown"
    current = today or datetime.now(timezone.utc).date()
    try:
        parsed = datetime.fromisoformat(data_date).date()
    except ValueError:
        return "unknown"
    age_days = (current - parsed).days
    if age_days <= 180:
        return "fresh"
    if age_days <= 365:
        return "aging"
    return "stale"


def check_data_freshness(data_date: str, *, threshold_days: int = 180, today: date | None = None) -> ContractWarning | None:
    if not data_date:
        return ContractWarning(
            code="DATA_DATE_MISSING",
            severity="MEDIUM",
            message="claim 缺少明确 data_date，后续无法做新鲜度判断",
            suggestion="在 claim 文本或 provenance 中补充数据时间点",
        )
    current = today or datetime.now(timezone.utc).date()
    try:
        parsed = datetime.fromisoformat(data_date).date()
    except ValueError:
        return ContractWarning(
            code="DATA_DATE_INVALID",
            severity="MEDIUM",
            message=f"无法解析 data_date={data_date}",
            suggestion="使用 ISO 日期格式 YYYY-MM-DD",
        )
    age_days = (current - parsed).days
    if age_days <= threshold_days:
        return None
    severity = "MEDIUM" if age_days <= 365 else "HIGH"
    status = "aging" if age_days <= 365 else "stale"
    return ContractWarning(
        code="DATA_STALENESS",
        severity=severity,
        message=f"claim 数据已 {status}（{age_days} 天）",
        evidence=f"data_date={data_date}",
        suggestion="更新更近的数据或在 thesis 中显式承认该数据已过期",
    )
