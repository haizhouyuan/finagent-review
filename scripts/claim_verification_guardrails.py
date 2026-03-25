#!/usr/bin/env python3
"""
claim_verification_guardrails.py

Manual guardrail scanner for thesis / claim markdown and JSONL files.
Primary domain checks now come from finagent.contracts; this script keeps
two extra narrative checks that are still useful for ad hoc reviews:
timestamp-gap and demand-elasticity.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finagent.contracts import ContractWarning, run_domain_contracts  # noqa: E402


@dataclass
class Warning:
    rule: str
    severity: str
    claim_text: str
    detail: str
    fix_suggestion: str


YEAR_PATTERN = re.compile(r"20(2[0-9])\s*(?:年|Q[1-4]|H[12])", re.UNICODE)
PRICE_SURGE_PATTERN = re.compile(
    r"[+＋]?\s*(\d{3,})\s*%|涨幅.*?(\d{3,})\s*%|(\d{3,})\s*%\s*(?:surge|spike|涨)",
    re.IGNORECASE | re.UNICODE,
)


def _convert_contract_warning(warning: ContractWarning, text: str) -> Warning:
    return Warning(
        rule=warning.code,
        severity=warning.severity,
        claim_text=text[:100],
        detail=warning.message,
        fix_suggestion=warning.suggestion or "",
    )


def check_timestamp_freshness(text: str) -> list[Warning]:
    years = [int(f"20{value}") for value in YEAR_PATTERN.findall(text)]
    if len(set(years)) < 2:
        return []
    min_year, max_year = min(years), max(years)
    gap = max_year - min_year
    if gap < 2:
        return []
    return [
        Warning(
            rule="TIMESTAMP_GAP",
            severity="MEDIUM",
            claim_text=text[:100],
            detail=f"Data from {min_year} compared with {max_year} ({gap} year gap).",
            fix_suggestion="Only compare claims and evidence from the same time window.",
        )
    ]


def check_demand_elasticity(text: str) -> list[Warning]:
    warnings: list[Warning] = []
    matches = PRICE_SURGE_PATTERN.findall(text)
    for groups in matches:
        pct = next((int(group) for group in groups if group), None)
        if not pct or pct <= 50:
            continue
        elasticity_discussed = bool(
            re.search(
                r"弹性|elastic|需求毁灭|destruction|substitut|替代|减配|砍单",
                text,
                re.IGNORECASE | re.UNICODE,
            )
        )
        if elasticity_discussed:
            continue
        warnings.append(
            Warning(
                rule="DEMAND_ELASTICITY_MISSING",
                severity="HIGH",
                claim_text=text[:100],
                detail=f"Price surge of {pct}% mentioned without demand elasticity analysis.",
                fix_suggestion="Add demand destruction / substitution scenario before using the claim.",
            )
        )
    return warnings


def run_checks(text: str) -> list[Warning]:
    domain_result = run_domain_contracts(text)
    warnings = [
        _convert_contract_warning(
            ContractWarning(
                code=str(item.get("code", "UNKNOWN")),
                severity=str(item.get("severity", "LOW")),
                message=str(item.get("message", "")),
                evidence=str(item.get("evidence", "")),
                suggestion=str(item.get("suggestion", "")),
            ),
            text,
        )
        for item in domain_result["warnings"]
    ]
    warnings.extend(check_timestamp_freshness(text))
    warnings.extend(check_demand_elasticity(text))
    return warnings


def _iter_inputs(content: str, *, file_path: str = "", field: str = "text") -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    if file_path.endswith(".jsonl"):
        for idx, line in enumerate(content.strip().splitlines(), 1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            text = obj.get(field, "") or obj.get("claim", "")
            if text:
                items.append((f"[line {idx}]", text))
        return items
    for idx, para in enumerate(content.split("\n\n"), 1):
        if len(para.strip()) >= 10:
            items.append((f"[para {idx}]", para))
    return items


def main() -> int:
    parser = argparse.ArgumentParser(description="Run verification guardrails on claims/thesis text")
    parser.add_argument("--file", "-f", help="JSONL or markdown file to check")
    parser.add_argument("--field", default="text", help="JSON field to inspect for JSONL input")
    args = parser.parse_args()

    content = Path(args.file).read_text() if args.file else sys.stdin.read()
    all_warnings: list[Warning] = []
    for label, text in _iter_inputs(content, file_path=args.file or "", field=args.field):
        warnings = run_checks(text)
        for warning in warnings:
            warning.claim_text = f"{label} {warning.claim_text}"
        all_warnings.extend(warnings)

    if not all_warnings:
        print("✅ No guardrail warnings found.")
        return 0

    severity_order = {"FATAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    all_warnings.sort(key=lambda item: severity_order.get(item.severity, 99))
    print(f"⚠️  {len(all_warnings)} guardrail warning(s) found:\n")
    for warning in all_warnings:
        icon = {"FATAL": "💀", "HIGH": "🔴", "MEDIUM": "⚠️", "LOW": "🟡"}
        print(f"{icon.get(warning.severity, '❓')} [{warning.severity}] {warning.rule}")
        print(f"   Context: {warning.claim_text}")
        print(f"   Issue: {warning.detail}")
        if warning.fix_suggestion:
            print(f"   Fix: {warning.fix_suggestion}")
        print()

    fatal_count = sum(1 for warning in all_warnings if warning.severity == "FATAL")
    if fatal_count:
        print(f"💀 {fatal_count} FATAL warning(s) — DO NOT proceed without fixing.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
