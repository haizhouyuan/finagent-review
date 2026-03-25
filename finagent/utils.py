from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from datetime import datetime, timezone

from .contracts import run_domain_contracts


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def make_id(prefix: str, label: str | None = None) -> str:
    token = uuid.uuid4().hex[:10]
    if not label:
        return f"{prefix}_{token}"
    cleaned = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    cleaned = cleaned[:32] or "item"
    return f"{prefix}_{cleaned}_{token}"


def stable_id(prefix: str, label: str, length: int = 10) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    cleaned = cleaned[:32] or "item"
    token = hashlib.sha256(label.encode("utf-8")).hexdigest()[:length]
    return f"{prefix}_{cleaned}_{token}"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def slugify(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_")
    return cleaned.lower()[:64] or f"item_{int(time.time())}"


def split_sentences(text: str) -> list[str]:
    normalized = re.sub(r"\r\n?", "\n", text)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    parts = re.split(r"(?<=[。！？!?\.])\s*|\n+", normalized)
    return [part.strip() for part in parts if part.strip()]


def infer_claim_type(text: str) -> str:
    lowered = text.lower()
    if any(word in text for word in ("风险", "担忧", "隐患")) or any(
        token in lowered for token in ("risk", "downside", "uncertain")
    ):
        return "risk"
    if any(word in text for word in ("催化", "发布", "上线", "财报", "投票")) or any(
        token in lowered for token in ("launch", "earnings", "vote", "approval", "catalyst")
    ):
        return "catalyst"
    if any(word in text for word in ("预计", "可能", "将", "会")) or any(
        token in lowered for token in ("will", "could", "expect", "forecast")
    ):
        return "forecast"
    if any(word in text for word in ("认为", "判断", "觉得", "看法")) or any(
        token in lowered for token in ("opinion", "believe", "view")
    ):
        return "viewpoint"
    if any(word in text for word in ("但是", "相反", "反而")) or any(
        token in lowered for token in ("however", "instead", "counter")
    ):
        return "counterpoint"
    return "fact"


def infer_claim_confidence(primaryness: str, claim_type: str) -> float:
    base = {
        "first_hand": 0.82,
        "personal": 0.72,
        "second_hand": 0.62,
    }.get(primaryness, 0.60)
    if claim_type in {"viewpoint", "forecast"}:
        base -= 0.08
    if claim_type == "risk":
        base -= 0.04
    return round(max(0.30, min(0.95, base)), 2)


def domain_check_claim(
    claim_text: str,
    *,
    numbers: list[dict[str, object]] | None = None,
    sector: str = "",
    claim_date: str = "",
) -> dict[str, object]:
    return run_domain_contracts(
        claim_text,
        numbers=numbers,
        sector=sector,
        claim_date=claim_date,
    )
