from __future__ import annotations

import re

from .base import ContractWarning


CYCLICAL_SECTORS = {"memory", "commodity", "shipping", "steel", "petrochemical"}
_SECTOR_HINTS = {
    "memory": ("dram", "ddr4", "ddr5", "hbm", "nand", "memory", "美光", "海力士", "cxmt", "长鑫", "三星"),
    "shipping": ("shipping", "freight", "航运", "运价"),
    "steel": ("steel", "螺纹钢", "钢材", "钢铁"),
    "petrochemical": ("petrochemical", "炼化", "石化", "原油"),
    "commodity": ("commodity", "大宗", "铜价", "铝价"),
}
_PE_PATTERN = re.compile(r"(?:P/?E\s*(?:of\s*)?|)(\d+(?:\.\d+)?)\s*(?:x|X|×|倍)\s*P/?E|\bP/?E\b\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_PRICE_PATTERN = re.compile(r"(?:\$[\d.]+|[\d.]+\s*(?:USD|美元|元))", re.IGNORECASE)
_PRICE_CHANGE_PATTERN = re.compile(r"\d+(?:\.\d+)?\s*%", re.IGNORECASE)
_MEMORY_PRICE_CONTEXT = re.compile(r"DDR[45]|HBM|DRAM|NAND|内存|颗粒|现货|合约|spot|contract", re.IGNORECASE)
_SPOT_KEYWORDS = re.compile(r"现货|spot|dxi|指数", re.IGNORECASE)
_CONTRACT_KEYWORDS = re.compile(r"合约|contract|长协|大宗", re.IGNORECASE)
_FCF_PATTERN = re.compile(r"FCF|free\s*cash\s*flow|自由现金流", re.IGNORECASE)
_FCF_WRONG_PATTERN = re.compile(r"(net\s*income|净利润|ni)\s*[-−]\s*(cap\s*ex|资本支出|capex)", re.IGNORECASE)


def _infer_sector(text: str, sector: str = "") -> str:
    if sector:
        return sector.lower().strip()
    lowered = text.lower()
    for candidate, hints in _SECTOR_HINTS.items():
        if any(hint in lowered for hint in hints):
            return candidate
    return ""


def check_valuation_method(text: str, sector: str = "") -> list[ContractWarning]:
    warnings: list[ContractWarning] = []
    normalized_sector = _infer_sector(text, sector=sector)
    if normalized_sector not in CYCLICAL_SECTORS:
        return warnings
    for match in _PE_PATTERN.finditer(text):
        value = float(match.group(1) or match.group(2))
        if value > 10:
            warnings.append(
                ContractWarning(
                    code="CYCLICAL_PE_TRAP",
                    severity="FATAL",
                    message=f"{normalized_sector} 属于周期板块，使用 {value:.1f}x P/E 风险过高",
                    evidence=text[:160],
                    suggestion="改用 P/B、EV/EBITDA 或 mid-cycle 口径，不要对 peak earnings 直接套高 P/E",
                )
            )
    return warnings


def check_financial_formula(text: str) -> list[ContractWarning]:
    if _FCF_PATTERN.search(text) and _FCF_WRONG_PATTERN.search(text):
        return [
            ContractWarning(
                code="FCF_FORMULA_ERROR",
                severity="FATAL",
                message="FCF 被写成 Net Income - CapEx；正确应为 OCF - CapEx",
                evidence=text[:160],
                suggestion="改用经营现金流 OCF 减资本开支 CapEx，并补 working capital 解释",
            )
        ]
    return []


def check_price_type(text: str) -> list[ContractWarning]:
    if not (_PRICE_PATTERN.search(text) or _PRICE_CHANGE_PATTERN.search(text)):
        return []
    if not _MEMORY_PRICE_CONTEXT.search(text):
        return []
    has_spot = bool(_SPOT_KEYWORDS.search(text))
    has_contract = bool(_CONTRACT_KEYWORDS.search(text))
    if has_spot or has_contract:
        return []
    return [
        ContractWarning(
            code="MEMORY_PRICE_TYPE_MISSING",
            severity="HIGH",
            message="内存价格引用未标注 spot / contract，价格口径不完整",
            evidence=text[:160],
            suggestion="所有 DDR/HBM/NAND 价格都必须标注现货或合约口径",
        )
    ]
