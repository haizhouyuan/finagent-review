from __future__ import annotations

from typing import Any

from .base import ContractWarning
from .freshness import check_data_freshness, extract_data_date, freshness_status_for_date
from .units import normalize_chinese_units, validate_unit_consistency
from .valuation import check_financial_formula, check_price_type, check_valuation_method


SEVERITY_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "FATAL": 3}


def has_fatal_warning(warnings: list[ContractWarning]) -> bool:
    return any(warning.severity == "FATAL" for warning in warnings)


def run_domain_contracts(
    claim_text: str,
    *,
    numbers: list[dict[str, Any]] | None = None,
    sector: str = "",
    claim_date: str = "",
) -> dict[str, Any]:
    normalized_text, unit_normalization_warnings = normalize_chinese_units(claim_text)
    warnings: list[ContractWarning] = []
    warnings.extend(unit_normalization_warnings)
    warnings.extend(validate_unit_consistency(normalized_text, numbers=numbers))
    warnings.extend(check_valuation_method(normalized_text, sector=sector))
    warnings.extend(check_financial_formula(normalized_text))
    warnings.extend(check_price_type(normalized_text))

    data_date = extract_data_date(normalized_text, fallback_iso=claim_date)
    freshness_warning = check_data_freshness(data_date)
    if freshness_warning:
        warnings.append(freshness_warning)
    warnings.sort(key=lambda item: (SEVERITY_ORDER.get(item.severity, 99), item.code, item.message))
    freshness_status = freshness_status_for_date(data_date)

    return {
        "passed": not has_fatal_warning(warnings),
        "quarantine": has_fatal_warning(warnings),
        "normalized_text": normalized_text,
        "warnings": [warning.to_dict() for warning in warnings],
        "data_date": data_date,
        "freshness_status": freshness_status,
    }


__all__ = [
    "ContractWarning",
    "SEVERITY_ORDER",
    "check_data_freshness",
    "check_financial_formula",
    "check_price_type",
    "check_valuation_method",
    "extract_data_date",
    "freshness_status_for_date",
    "has_fatal_warning",
    "normalize_chinese_units",
    "run_domain_contracts",
    "validate_unit_consistency",
]
