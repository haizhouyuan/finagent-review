from __future__ import annotations

import re
from typing import Any

from .base import ContractWarning


_CN_UNIT_PATTERN = re.compile(
    r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>万亿|亿元|亿美元|亿人民币|亿|万元|万美元|万|Gb|GB|gb|gbit|gbit)",
    re.IGNORECASE,
)
_YI_PATTERN = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*亿", re.IGNORECASE)
_BILLION_PATTERN = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*[Bb]illion\b")
_MILLION_PATTERN = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*[Mm]illion\b")
_GB_PATTERN = re.compile(r"(?P<value>\d+)\s*GB\b")
_Gb_PATTERN = re.compile(r"(?P<value>\d+)\s*Gb\b")
_MEMORY_CONTEXT_PATTERN = re.compile(r"DDR[45]|HBM|DRAM|NAND|LPDDR|die|module|chip|芯片|颗粒|模组", re.IGNORECASE)


def _format_scaled_number(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.3f}".rstrip("0").rstrip(".")


def normalize_chinese_units(text: str) -> tuple[str, list[ContractWarning]]:
    warnings: list[ContractWarning] = []

    def replace(match: re.Match[str]) -> str:
        raw_value = float(match.group("value"))
        raw_unit = match.group("unit")
        unit = raw_unit.lower()
        if unit == "万亿":
            return f"{_format_scaled_number(raw_value * 100)}B"
        if unit in {"亿元", "亿人民币"}:
            return f"{_format_scaled_number(raw_value / 10)}B CNY"
        if unit == "亿美元":
            return f"{_format_scaled_number(raw_value / 10)}B USD"
        if unit == "亿":
            return f"{_format_scaled_number(raw_value / 10)}B"
        if unit == "万元":
            return f"{_format_scaled_number(raw_value * 10)}K CNY"
        if unit == "万美元":
            return f"{_format_scaled_number(raw_value * 10)}K USD"
        if unit == "万":
            return f"{_format_scaled_number(raw_value * 10)}K"
        if unit in {"gb", "gbit"}:
            return f"{_format_scaled_number(raw_value / 8)}GB(eq) [{_format_scaled_number(raw_value)}Gb]"
        return raw_unit

    normalized = _CN_UNIT_PATTERN.sub(replace, text)

    yi_matches = [float(match.group("value")) for match in _YI_PATTERN.finditer(text)]
    billion_matches = [float(match.group("value")) for match in _BILLION_PATTERN.finditer(text)]
    million_matches = [float(match.group("value")) for match in _MILLION_PATTERN.finditer(text)]
    for yi_value in yi_matches:
        translated_billion = yi_value / 10
        for billion_value in billion_matches:
            if abs(billion_value - yi_value) < 1e-6:
                warnings.append(
                    ContractWarning(
                        code="YI_BILLION_TRANSLATION",
                        severity="FATAL",
                        message=f"{_format_scaled_number(yi_value)}亿 被错误映射为 {_format_scaled_number(billion_value)}B；正确应为 {_format_scaled_number(translated_billion)}B",
                        evidence=text[:160],
                        suggestion=f"将 {_format_scaled_number(billion_value)}B 改为 {_format_scaled_number(translated_billion)}B",
                    )
                )
        for million_value in million_matches:
            if abs(million_value - yi_value * 100) < 1e-6:
                warnings.append(
                    ContractWarning(
                        code="YI_MILLION_TRANSLATION",
                        severity="HIGH",
                        message=f"{_format_scaled_number(yi_value)}亿 与 {_format_scaled_number(million_value)}M 同时出现，请确认是否做了 /10 换算",
                        evidence=text[:160],
                        suggestion=f"确认 {_format_scaled_number(yi_value)}亿 是否应表达为 {_format_scaled_number(translated_billion)}B / {_format_scaled_number(translated_billion * 1000)}M",
                    )
                )
    return normalized, warnings


def validate_unit_consistency(claim_text: str, numbers: list[dict[str, Any]] | None = None) -> list[ContractWarning]:
    warnings: list[ContractWarning] = []
    numbers = numbers or []

    has_cn_billion = bool(_YI_PATTERN.search(claim_text))
    has_en_billion = bool(_BILLION_PATTERN.search(claim_text))
    if has_cn_billion and has_en_billion:
        warnings.append(
            ContractWarning(
                code="MIXED_CN_EN_BILLION_UNITS",
                severity="HIGH",
                message="同一 claim 同时出现 亿 与 Billion，请确认是否统一换算口径",
                evidence=claim_text[:160],
                suggestion="统一以 B / M 或中文口径表达，并显式标注币种",
            )
        )

    has_gb = bool(_GB_PATTERN.search(claim_text))
    has_gbit = bool(_Gb_PATTERN.search(claim_text))
    if has_gb and has_gbit:
        warnings.append(
            ContractWarning(
                code="GB_GB_MIXED",
                severity="HIGH",
                message="同一 claim 同时出现 GB 与 Gb，存在位/字节混淆风险",
                evidence=claim_text[:160],
                suggestion="明确芯片容量用 Gb，模组容量用 GB",
            )
        )
    if has_gb and _MEMORY_CONTEXT_PATTERN.search(claim_text):
        for match in _GB_PATTERN.finditer(claim_text):
            value = int(match.group("value"))
            if value in {8, 16, 32, 64}:
                warnings.append(
                    ContractWarning(
                        code="MEMORY_DIE_GB_CONFUSION",
                        severity="HIGH",
                        message=f"{value}GB 出现在 memory die / chip 上下文，常见 die 单位应为 {value}Gb",
                        evidence=claim_text[:160],
                        suggestion=f"若指单颗 die，请改为 {value}Gb；若指模组，请显式写明 module / DIMM",
                    )
                )

    for item in numbers:
        unit = str(item.get("unit", "")).strip()
        if unit in {"亿", "亿元", "亿美元"} and any(token in claim_text for token in ("B", "billion", "Billion")):
            warnings.append(
                ContractWarning(
                    code="NUMBER_LIST_UNIT_MISMATCH",
                    severity="MEDIUM",
                    message="numbers 列表与 claim 文本的单位口径不一致",
                    evidence=claim_text[:160],
                    suggestion="统一 numbers.unit 与正文单位表达",
                )
            )
            break
    return warnings
