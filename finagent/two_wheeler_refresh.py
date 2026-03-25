"""Two-wheeler catalog refresh helpers.

Keeps the pilot's real asset/SKU seed data and graph seed data in one
place, then exposes incremental refresh helpers that reuse the existing
writeback engine and GraphStore.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from finagent.db import connect, init_db, select_one
from finagent.graph_v2.ontology import EdgeType, NodeType
from finagent.graph_v2.store import GraphStore
from finagent.research_contracts import EvidenceRef, ImageAssetRef, ResearchPackage, SkuRecord
from finagent.writeback_engine import apply_writeback, plan_writeback

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_STATE_DB = REPO_ROOT / "state" / "finagent.sqlite"
DEFAULT_GRAPH_DB = REPO_ROOT / "finagent.db"
DEFAULT_CATALOG_PATH = REPO_ROOT / "data" / "two_wheeler" / "catalog.json"
DEFAULT_CHANGELOG_DIR = REPO_ROOT / "state" / "two_wheeler_refresh"
VALID_FROM = "2025-01-01"

DEFAULT_RUN_ID = "run-seed-2wheeler-real-01"
DEFAULT_GOAL = "两轮车竞品车身结构与轮毂技术对标"
DEFAULT_CONTEXT = "两轮车"
DEFAULT_TRIPLES = [
    {"subject": "九号Fz3", "predicate": "采用", "object": "双管一体式车架"},
    {"subject": "雅迪冠能DM6", "predicate": "配备", "object": "石墨烯电池"},
    {"subject": "小牛NQi", "predicate": "使用", "object": "中置电机"},
]
DEFAULT_EVIDENCE_REFS = [
    {"evidence_id": 1, "query": "九号Fz3车架结构", "char_count": 500},
    {"evidence_id": 2, "query": "雅迪冠能技术参数", "char_count": 400},
]

IMAGE_ASSET_DATA = [
    {
        "asset_id": "img-yadea-guanneng-official-screenshot",
        "brand": "雅迪",
        "product_line": "冠能系列",
        "category": "exterior",
        "source_url": "https://www.yadea.com.cn/product/guanneng",
        "local_path": "data/competitive_assets/photos/雅迪_冠能系列_官网截图.webp",
        "acquisition_date": "2026-03-22",
        "is_official": True,
        "quality_grade": "medium",
        "visible_content": "冠能系列产品官网截图，含多款车型外观",
        "supports_conclusion": "冠能系列定位中高端，10-14寸铝合金轮毂为主",
        "prohibits_conclusion": "",
    },
    {
        "asset_id": "img-ninebot-fz-official-screenshot",
        "brand": "九号",
        "product_line": "Fz系列",
        "category": "exterior",
        "source_url": "https://www.ninebot.com/product/fz",
        "local_path": "data/competitive_assets/photos/九号_Fz系列_官网截图.webp",
        "acquisition_date": "2026-03-22",
        "is_official": True,
        "quality_grade": "medium",
        "visible_content": "九号Fz系列产品官网截图，含Fz3等车型外观",
        "supports_conclusion": "Fz系列采用运动机甲风格，双管一体式车架",
        "prohibits_conclusion": "",
    },
    {
        "asset_id": "img-field-research-photo-01",
        "brand": "多品牌",
        "product_line": "实地调研",
        "category": "field_research",
        "source_url": "antigravity://brain/249f0306-84f6-4838-9000-796805913dcd/.tempmediaStorage/media_249f0306-84f6-4838-9000-796805913dcd_1774194816081.jpg",
        "local_path": "data/competitive_assets/photos/市场调研_产品实拍_01.jpg",
        "acquisition_date": "2026-03-22",
        "is_official": False,
        "quality_grade": "medium",
        "visible_content": "线下门店/展会实拍产品照片",
        "supports_conclusion": "实地调研一手素材",
        "prohibits_conclusion": "",
    },
    {
        "asset_id": "img-field-research-photo-02",
        "brand": "多品牌",
        "product_line": "实地调研",
        "category": "field_research",
        "source_url": "antigravity://brain/249f0306-84f6-4838-9000-796805913dcd/.tempmediaStorage/media_249f0306-84f6-4838-9000-796805913dcd_1774194816154.jpg",
        "local_path": "data/competitive_assets/photos/市场调研_产品实拍_02.jpg",
        "acquisition_date": "2026-03-22",
        "is_official": False,
        "quality_grade": "medium",
        "visible_content": "线下门店/展会实拍产品照片",
        "supports_conclusion": "实地调研一手素材",
        "prohibits_conclusion": "",
    },
]

SKU_RECORD_DATA = [
    {
        "sku_id": "sku-ninebot-fz3-110",
        "brand": "九号",
        "series": "Fz",
        "model": "Fz3 110",
        "positioning": "中高端",
        "price_range": "6299-7599",
        "wheel_diameter": "14寸",
        "frame_type": "双管一体",
        "motor_type": "轮毂电机 1200W",
        "battery_platform": "72V30Ah 锂电",
        "brake_config": "前碟后碟",
        "target_audience": "年轻男性/通勤",
        "style_tags": ("运动", "机甲", "智能"),
        "evidence_sources": ("官网", "线下门店"),
    },
    {
        "sku_id": "sku-yadea-dm6",
        "brand": "雅迪",
        "series": "冠能",
        "model": "冠能DM6",
        "positioning": "中高端",
        "price_range": "4999-6599",
        "wheel_diameter": "10寸",
        "frame_type": "单管加强",
        "motor_type": "轮毂电机 800W",
        "battery_platform": "60V24Ah 石墨烯",
        "brake_config": "前碟后鼓",
        "target_audience": "通勤白领",
        "style_tags": ("时尚", "长续航", "石墨烯"),
        "evidence_sources": ("官网", "京东", "线下门店"),
    },
    {
        "sku_id": "sku-aima-a500",
        "brand": "爱玛",
        "series": "A",
        "model": "A500",
        "positioning": "中端",
        "price_range": "3299-4299",
        "wheel_diameter": "12寸",
        "frame_type": "单管",
        "motor_type": "轮毂电机 600W",
        "battery_platform": "48V20Ah 铅酸",
        "brake_config": "前鼓后鼓",
        "target_audience": "大众通勤",
        "style_tags": ("实用", "性价比"),
        "evidence_sources": ("官网", "天猫"),
    },
    {
        "sku_id": "sku-tailing-n9",
        "brand": "台铃",
        "series": "N",
        "model": "N9",
        "positioning": "中高端",
        "price_range": "4599-5999",
        "wheel_diameter": "14寸",
        "frame_type": "双管",
        "motor_type": "轮毂电机 1000W",
        "battery_platform": "72V22Ah 锂电",
        "brake_config": "前碟后鼓",
        "target_audience": "城际通勤",
        "style_tags": ("省电", "超远续航"),
        "evidence_sources": ("官网", "抖音直播"),
    },
    {
        "sku_id": "sku-niu-nqi-sport",
        "brand": "小牛",
        "series": "NQi",
        "model": "NQi Sport",
        "positioning": "中高端",
        "price_range": "5599-7999",
        "wheel_diameter": "14寸",
        "frame_type": "双管一体",
        "motor_type": "中置电机 1200W",
        "battery_platform": "72V35Ah 锂电",
        "brake_config": "前碟后碟",
        "target_audience": "年轻男性",
        "style_tags": ("运动", "科技", "APP控车"),
        "evidence_sources": ("官网", "小红书测评"),
    },
    {
        "sku_id": "sku-yadea-de1-t5",
        "brand": "雅迪",
        "series": "DE",
        "model": "DE1 T5",
        "positioning": "低端",
        "price_range": "1999-2599",
        "wheel_diameter": "10寸",
        "frame_type": "单管简易",
        "motor_type": "轮毂电机 350W",
        "battery_platform": "48V12Ah 铅酸",
        "brake_config": "前鼓后鼓",
        "target_audience": "学生/短途代步",
        "style_tags": ("入门", "轻便"),
        "evidence_sources": ("拼多多", "线下"),
    },
    {
        "sku_id": "sku-ninebot-e2-plus",
        "brand": "九号",
        "series": "E",
        "model": "E2 Plus",
        "positioning": "中端",
        "price_range": "3499-3999",
        "wheel_diameter": "10寸",
        "frame_type": "单管",
        "motor_type": "轮毂电机 400W",
        "battery_platform": "48V24Ah 锂电",
        "brake_config": "前碟后鼓",
        "target_audience": "女性通勤",
        "style_tags": ("轻巧", "智能", "时尚"),
        "evidence_sources": ("官网", "小红书"),
    },
]

BRANDS = {
    "yadea": ("雅迪", NodeType.COMPANY),
    "aima": ("爱玛", NodeType.COMPANY),
    "ninebot": ("九号", NodeType.COMPANY),
    "tailg": ("台铃", NodeType.COMPANY),
    "niu": ("小牛", NodeType.COMPANY),
    "xinri": ("新日", NodeType.COMPANY),
    "luyuan": ("绿源", NodeType.COMPANY),
    "jinggu": ("金谷/JG", NodeType.COMPANY),
    "chunfeng": ("春风动力", NodeType.COMPANY),
}
COMPONENTS = {
    "aluminum_wheel": ("铝合金轮毂", NodeType.COMPONENT),
    "steel_wheel": ("钢轮毂", NodeType.COMPONENT),
    "frame": ("车架", NodeType.COMPONENT),
    "motor": ("电机", NodeType.COMPONENT),
    "battery": ("电池", NodeType.COMPONENT),
    "controller": ("控制器", NodeType.COMPONENT),
    "brake_system": ("制动系统", NodeType.COMPONENT),
    "lighting": ("灯具系统", NodeType.COMPONENT),
}
PRODUCT_LINES = {
    "yadea_guanneng": ("冠能系列", NodeType.PROJECT),
    "yadea_dm": ("DM系列", NodeType.PROJECT),
    "ninebot_fz": ("Fz系列", NodeType.PROJECT),
    "aima_a_series": ("A系列", NodeType.PROJECT),
    "tailg_n_series": ("N系列", NodeType.PROJECT),
    "niu_nqi": ("NQi系列", NodeType.PROJECT),
    "xinri_xc": ("XC系列", NodeType.PROJECT),
    "luyuan_s_series": ("S系列", NodeType.PROJECT),
}
TECHNOLOGIES = {
    "graphene_battery": ("石墨烯电池", NodeType.TECHNOLOGY),
    "sodium_battery": ("钠离子电池", NodeType.TECHNOLOGY),
    "hub_motor": ("轮毂电机", NodeType.TECHNOLOGY),
    "mid_motor": ("中置电机", NodeType.TECHNOLOGY),
}
POLICIES = {
    "gb17761": ("新国标 GB17761-2018", NodeType.POLICY),
    "ev_market_seg": ("两轮电动车市场", NodeType.SECTOR),
}
ALIASES = {
    "雅迪": "yadea",
    "yadea": "yadea",
    "Yadea": "yadea",
    "爱玛": "aima",
    "aima": "aima",
    "Aima": "aima",
    "九号": "ninebot",
    "ninebot": "ninebot",
    "Ninebot": "ninebot",
    "segway": "ninebot",
    "九号电动": "ninebot",
    "九号Fz": "ninebot_fz",
    "Fz系列": "ninebot_fz",
    "台铃": "tailg",
    "tailg": "tailg",
    "Tailg": "tailg",
    "台铃电动": "tailg",
    "台铃N系列": "tailg_n_series",
    "小牛": "niu",
    "niu": "niu",
    "NIU": "niu",
    "小牛电动": "niu",
    "小牛NQi": "niu_nqi",
    "雅迪冠能": "yadea_guanneng",
    "冠能系列": "yadea_guanneng",
    "雅迪DM": "yadea_dm",
    "DM系列": "yadea_dm",
    "爱玛A系列": "aima_a_series",
    "新日": "xinri",
    "xinri": "xinri",
    "XC系列": "xinri_xc",
    "绿源": "luyuan",
    "luyuan": "luyuan",
    "S系列": "luyuan_s_series",
    "金谷": "jinggu",
    "JG": "jinggu",
    "jg": "jinggu",
    "金谷轮毂": "jinggu",
    "春风": "chunfeng",
    "春风动力": "chunfeng",
    "cfmoto": "chunfeng",
}

ASSET_COMPARE_FIELDS = (
    "brand",
    "product_line",
    "category",
    "source_url",
    "local_path",
    "acquisition_date",
    "is_official",
    "quality_grade",
    "visible_content",
    "supports_conclusion",
    "prohibits_conclusion",
)
SKU_COMPARE_FIELDS = (
    "brand",
    "series",
    "model",
    "positioning",
    "price_range",
    "wheel_diameter",
    "frame_type",
    "motor_type",
    "battery_platform",
    "brake_config",
    "target_audience",
    "style_tags",
    "evidence_sources",
)


def load_catalog(catalog_path: str | Path | None = None) -> dict[str, Any]:
    path = DEFAULT_CATALOG_PATH if catalog_path is None else Path(catalog_path)
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid two-wheeler catalog payload: {path}")
    return payload


def _resolve_catalog(catalog: dict[str, Any] | None) -> dict[str, Any]:
    if catalog is not None:
        return catalog
    return load_catalog(None)


def _catalog_rows(
    catalog: dict[str, Any] | None,
    key: str,
    fallback: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    if catalog and isinstance(catalog.get(key), list):
        return [dict(row) for row in catalog[key]]
    return [dict(row) for row in fallback]


def default_graph_nodes(*, catalog: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    catalog = _resolve_catalog(catalog)
    nodes: list[dict[str, Any]] = []
    for bucket in (BRANDS, COMPONENTS, PRODUCT_LINES, TECHNOLOGIES, POLICIES):
        for node_id, (label, node_type) in bucket.items():
            nodes.append({"node_id": node_id, "label": label, "node_type": node_type})
    for node in ((catalog or {}).get("graph", {}) or {}).get("nodes", []):
        nodes.append(dict(node))
    return nodes


def default_graph_edges(*, catalog: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    catalog = _resolve_catalog(catalog)
    edges: list[dict[str, Any]] = []

    def add(
        source_id: str,
        target_id: str,
        edge_type: EdgeType,
        *,
        confidence: float,
        source: str = "two_wheeler_refresh",
        evidence: str | None = None,
    ) -> None:
        edges.append(
            {
                "source_id": source_id,
                "target_id": target_id,
                "edge_type": edge_type,
                "valid_from": VALID_FROM,
                "confidence": confidence,
                "source": source,
                "evidence": evidence,
            }
        )

    for brand, product_line in (
        ("yadea", "yadea_guanneng"),
        ("yadea", "yadea_dm"),
        ("ninebot", "ninebot_fz"),
        ("aima", "aima_a_series"),
        ("tailg", "tailg_n_series"),
        ("niu", "niu_nqi"),
        ("xinri", "xinri_xc"),
        ("luyuan", "luyuan_s_series"),
    ):
        add(brand, product_line, EdgeType.MANUFACTURES, confidence=0.95)

    for brand in BRANDS:
        add(brand, "ev_market_seg", EdgeType.BELONGS_TO, confidence=0.80)

    for product_line in PRODUCT_LINES:
        add(product_line, "ev_market_seg", EdgeType.BELONGS_TO, confidence=0.75)

    for customer in ("yadea", "aima", "ninebot", "tailg", "niu", "xinri", "luyuan"):
        add(
            "jinggu",
            customer,
            EdgeType.SUPPLIES_CORE_PART_TO,
            confidence=0.85,
            evidence="金谷供应铝轮毂",
        )

    for product_line in ("yadea_guanneng", "ninebot_fz", "aima_a_series", "tailg_n_series", "niu_nqi"):
        for component in ("aluminum_wheel", "motor", "battery", "controller", "frame"):
            add(component, product_line, EdgeType.COMPONENT_OF, confidence=0.80)

    for product_line in ("yadea_guanneng", "aima_a_series", "xinri_xc", "luyuan_s_series"):
        add("steel_wheel", product_line, EdgeType.COMPONENT_OF, confidence=0.72)

    for product_line in ("yadea_guanneng", "ninebot_fz", "aima_a_series", "tailg_n_series", "niu_nqi"):
        for component in ("brake_system", "lighting"):
            add(component, product_line, EdgeType.COMPONENT_OF, confidence=0.72)

    for technology, product_line, confidence in (
        ("graphene_battery", "yadea_guanneng", 0.82),
        ("hub_motor", "ninebot_fz", 0.82),
        ("sodium_battery", "tailg_n_series", 0.72),
        ("mid_motor", "niu_nqi", 0.72),
    ):
        add(technology, product_line, EdgeType.ENABLES, confidence=confidence)

    top_brands = ["yadea", "aima", "ninebot", "tailg", "niu"]
    for idx, source_id in enumerate(top_brands):
        for target_id in top_brands[idx + 1:]:
            add(source_id, target_id, EdgeType.COMPETES_WITH, confidence=0.90)
            add(target_id, source_id, EdgeType.COMPETES_WITH, confidence=0.90)

    add(
        "chunfeng",
        "jinggu",
        EdgeType.PARTNERS_WITH,
        confidence=0.76,
        evidence="春风动力与金谷轮毂合作",
    )
    edges.append(
        {
            "source_id": "gb17761",
            "target_id": "ev_market_seg",
            "edge_type": EdgeType.REGULATES,
            "valid_from": "2019-04-15",
            "confidence": 0.99,
            "source": "two_wheeler_refresh",
            "evidence": None,
        }
    )
    for edge in ((catalog or {}).get("graph", {}) or {}).get("edges", []):
        row = dict(edge)
        if isinstance(row.get("edge_type"), str):
            row["edge_type"] = EdgeType(row["edge_type"])
        row.setdefault("valid_from", VALID_FROM)
        row.setdefault("source", "two_wheeler_catalog")
        edges.append(row)
    return edges


def default_graph_aliases(*, catalog: dict[str, Any] | None = None) -> dict[str, str]:
    catalog = _resolve_catalog(catalog)
    merged = dict(ALIASES)
    graph_payload = ((catalog or {}).get("graph", {}) or {})
    merged.update({str(k): str(v) for k, v in dict(graph_payload.get("aliases", {})).items()})
    return merged


def build_image_assets(
    *,
    repo_root: Path = REPO_ROOT,
    strict: bool = True,
    asset_data: Iterable[dict[str, Any]] | None = None,
    catalog: dict[str, Any] | None = None,
) -> list[ImageAssetRef]:
    catalog = _resolve_catalog(catalog)
    assets: list[ImageAssetRef] = []
    rows = _catalog_rows(catalog, "image_assets", IMAGE_ASSET_DATA) if asset_data is None else list(asset_data)
    for meta in rows:
        disk_path = repo_root / meta["local_path"]
        if strict and not disk_path.exists():
            raise FileNotFoundError(f"missing image asset: {disk_path}")
        if not disk_path.exists():
            continue
        assets.append(ImageAssetRef(**dict(meta)))
    return assets


def build_sku_records(
    *,
    sku_data: Iterable[dict[str, Any]] | None = None,
    catalog: dict[str, Any] | None = None,
) -> list[SkuRecord]:
    catalog = _resolve_catalog(catalog)
    rows = _catalog_rows(catalog, "sku_records", SKU_RECORD_DATA) if sku_data is None else list(sku_data)
    return [SkuRecord(**dict(record)) for record in rows]


def build_research_package(
    *,
    run_id: str = DEFAULT_RUN_ID,
    repo_root: Path = REPO_ROOT,
    strict_assets: bool = True,
    image_assets: Iterable[ImageAssetRef] | None = None,
    sku_records: Iterable[SkuRecord] | None = None,
    catalog: dict[str, Any] | None = None,
) -> ResearchPackage:
    catalog = _resolve_catalog(catalog)
    resolved_images = (
        build_image_assets(repo_root=repo_root, strict=strict_assets, catalog=catalog)
        if image_assets is None
        else list(image_assets)
    )
    resolved_skus = build_sku_records(catalog=catalog) if sku_records is None else list(sku_records)
    goal = str((catalog or {}).get("goal", DEFAULT_GOAL))
    context = str((catalog or {}).get("context", DEFAULT_CONTEXT))
    triples = list((catalog or {}).get("triples", DEFAULT_TRIPLES))
    evidence_refs = [
        EvidenceRef(**row)
        for row in list((catalog or {}).get("evidence_refs", DEFAULT_EVIDENCE_REFS))
    ]
    report_md = str((catalog or {}).get("report_md", "# 两轮车竞品对标\n\n5品牌7车型对标分析，含实地调研图片。"))
    confidence = float((catalog or {}).get("confidence", 0.80))
    return ResearchPackage(
        run_id=run_id,
        goal=goal,
        context=context,
        triples=triples,
        evidence_refs=evidence_refs,
        report_md=report_md,
        confidence=confidence,
        image_assets=resolved_images,
        sku_records=resolved_skus,
    )


def _normalize_asset_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "brand": payload.get("brand", ""),
        "product_line": payload.get("product_line", ""),
        "category": payload.get("category", ""),
        "source_url": payload.get("source_url", ""),
        "local_path": payload.get("local_path", ""),
        "acquisition_date": payload.get("acquisition_date", ""),
        "is_official": bool(payload.get("is_official")),
        "quality_grade": payload.get("quality_grade", ""),
        "visible_content": payload.get("visible_content", ""),
        "supports_conclusion": payload.get("supports_conclusion", ""),
        "prohibits_conclusion": payload.get("prohibits_conclusion", ""),
    }


def _normalize_asset_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    record = dict(row)
    return {
        "brand": record.get("brand", ""),
        "product_line": record.get("product_line", ""),
        "category": record.get("category", ""),
        "source_url": record.get("source_url", ""),
        "local_path": record.get("local_path", ""),
        "acquisition_date": record.get("acquisition_date", ""),
        "is_official": bool(record.get("is_official")),
        "quality_grade": record.get("quality_grade", ""),
        "visible_content": record.get("visible_content", ""),
        "supports_conclusion": record.get("supports_conclusion", ""),
        "prohibits_conclusion": record.get("prohibits_conclusion", ""),
    }


def _normalize_sku_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "brand": payload.get("brand", ""),
        "series": payload.get("series", ""),
        "model": payload.get("model", ""),
        "positioning": payload.get("positioning", ""),
        "price_range": payload.get("price_range", ""),
        "wheel_diameter": payload.get("wheel_diameter", ""),
        "frame_type": payload.get("frame_type", ""),
        "motor_type": payload.get("motor_type", ""),
        "battery_platform": payload.get("battery_platform", ""),
        "brake_config": payload.get("brake_config", ""),
        "target_audience": payload.get("target_audience", ""),
        "style_tags": tuple(payload.get("style_tags", []) or ()),
        "evidence_sources": tuple(payload.get("evidence_sources", []) or ()),
    }


def _normalize_sku_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    record = dict(row)
    return {
        "brand": record.get("brand", ""),
        "series": record.get("series", ""),
        "model": record.get("model", ""),
        "positioning": record.get("positioning", ""),
        "price_range": record.get("price_range", ""),
        "wheel_diameter": record.get("wheel_diameter", ""),
        "frame_type": record.get("frame_type", ""),
        "motor_type": record.get("motor_type", ""),
        "battery_platform": record.get("battery_platform", ""),
        "brake_config": record.get("brake_config", ""),
        "target_audience": record.get("target_audience", ""),
        "style_tags": tuple(json.loads(record.get("style_tags_json", "[]") or "[]")),
        "evidence_sources": tuple(json.loads(record.get("evidence_sources_json", "[]") or "[]")),
    }


def _changed_fields(current: dict[str, Any], desired: dict[str, Any], fields: Iterable[str]) -> list[str]:
    return [field for field in fields if current.get(field) != desired.get(field)]


def preview_competitive_refresh(
    conn: sqlite3.Connection,
    package: ResearchPackage,
) -> tuple[list[Any], dict[str, Any]]:
    actions = plan_writeback(package, conn, target_families={"competitive"})
    filtered_actions: list[Any] = []
    summary = {
        "asset_ledger": {"created": [], "updated": [], "unchanged": []},
        "sku_catalog": {"created": [], "updated": [], "unchanged": []},
    }

    for action in actions:
        payload = action.payload
        if action.target_type == "asset_ledger":
            asset_id = payload.get("asset_id", "")
            row = select_one(conn, "SELECT * FROM asset_ledger WHERE asset_id = ?", (asset_id,))
            if row is None:
                summary["asset_ledger"]["created"].append(asset_id)
                filtered_actions.append(action)
                continue
            changed = _changed_fields(
                _normalize_asset_row(row),
                _normalize_asset_payload(payload),
                ASSET_COMPARE_FIELDS,
            )
            if changed:
                summary["asset_ledger"]["updated"].append({"asset_id": asset_id, "fields": changed})
                filtered_actions.append(action)
            else:
                summary["asset_ledger"]["unchanged"].append(asset_id)
        elif action.target_type == "sku_catalog":
            sku_id = payload.get("sku_id", "")
            row = select_one(conn, "SELECT * FROM sku_catalog WHERE sku_id = ?", (sku_id,))
            if row is None:
                summary["sku_catalog"]["created"].append(sku_id)
                filtered_actions.append(action)
                continue
            changed = _changed_fields(
                _normalize_sku_row(row),
                _normalize_sku_payload(payload),
                SKU_COMPARE_FIELDS,
            )
            if changed:
                summary["sku_catalog"]["updated"].append({"sku_id": sku_id, "fields": changed})
                filtered_actions.append(action)
            else:
                summary["sku_catalog"]["unchanged"].append(sku_id)

    summary["planned_actions"] = len(actions)
    summary["applied_actions"] = len(filtered_actions)
    return filtered_actions, summary


def apply_competitive_refresh(
    conn: sqlite3.Connection,
    package: ResearchPackage,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    actions, summary = preview_competitive_refresh(conn, package)
    if not dry_run and actions:
        apply_writeback(actions, conn)
        conn.commit()
    summary["row_counts"] = {
        "asset_ledger": conn.execute("SELECT COUNT(*) FROM asset_ledger").fetchone()[0],
        "sku_catalog": conn.execute("SELECT COUNT(*) FROM sku_catalog").fetchone()[0],
    }
    return summary


def preview_graph_refresh(
    store: GraphStore,
    *,
    nodes: Iterable[dict[str, Any]] | None = None,
    edges: Iterable[dict[str, Any]] | None = None,
    aliases: dict[str, str] | None = None,
    catalog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    node_rows = default_graph_nodes(catalog=catalog) if nodes is None else list(nodes)
    edge_rows = default_graph_edges(catalog=catalog) if edges is None else list(edges)
    alias_rows = default_graph_aliases(catalog=catalog)
    alias_rows.update(dict(aliases or {}))

    alias_count_before = store.conn.execute("SELECT COUNT(*) FROM kg_entity_aliases").fetchone()[0]
    summary = {
        "nodes": {"created": [], "updated": [], "unchanged": []},
        "edges": {"created": [], "updated": [], "unchanged": []},
        "aliases": {"created": [], "updated": [], "unchanged": []},
        "stats_before": store.stats(),
        "alias_count_before": int(alias_count_before),
    }

    for node in node_rows:
        node_id = node["node_id"]
        existing = store.get_node(node_id)
        desired = {
            "label": node["label"],
            "node_type": node["node_type"].value if isinstance(node["node_type"], NodeType) else str(node["node_type"]),
        }
        if existing is None:
            summary["nodes"]["created"].append(node_id)
            continue
        current = {"label": existing.get("label", ""), "node_type": existing.get("node_type", "")}
        changed = _changed_fields(current, desired, ("label", "node_type"))
        if changed:
            summary["nodes"]["updated"].append({"node_id": node_id, "fields": changed})
        else:
            summary["nodes"]["unchanged"].append(node_id)

    for alias, canonical in alias_rows.items():
        current = store.resolve_alias(alias)
        if current is None:
            summary["aliases"]["created"].append(alias)
        elif current != canonical:
            summary["aliases"]["updated"].append({"alias": alias, "canonical_id": canonical})
        else:
            summary["aliases"]["unchanged"].append(alias)

    for edge in edge_rows:
        edge_type = edge["edge_type"].value if isinstance(edge["edge_type"], EdgeType) else str(edge["edge_type"])
        current_edges = store.edges_between(edge["source_id"], edge["target_id"], edge_type=edge_type)
        edge_key = f"{edge['source_id']}->{edge['target_id']}:{edge_type}"
        if not current_edges:
            summary["edges"]["created"].append(edge_key)
            continue
        best_conf = max(float(row["confidence"]) for row in current_edges)
        if float(edge["confidence"]) > best_conf:
            summary["edges"]["updated"].append(
                {"edge": edge_key, "confidence": [best_conf, float(edge["confidence"])]}
            )
        else:
            summary["edges"]["unchanged"].append(edge_key)

    summary["stats_after_preview"] = {
        "total_nodes": summary["stats_before"]["total_nodes"] + len(summary["nodes"]["created"]),
        "total_edges": summary["stats_before"]["total_edges"] + len(summary["edges"]["created"]),
    }
    summary["alias_count_after_preview"] = (
        summary["alias_count_before"] + len(summary["aliases"]["created"])
    )
    return summary


def apply_graph_refresh(
    store: GraphStore,
    *,
    nodes: Iterable[dict[str, Any]] | None = None,
    edges: Iterable[dict[str, Any]] | None = None,
    aliases: dict[str, str] | None = None,
    dry_run: bool = False,
    catalog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    node_rows = default_graph_nodes(catalog=catalog) if nodes is None else list(nodes)
    edge_rows = default_graph_edges(catalog=catalog) if edges is None else list(edges)
    alias_rows = default_graph_aliases(catalog=catalog)
    alias_rows.update(dict(aliases or {}))

    summary = preview_graph_refresh(
        store,
        nodes=node_rows,
        edges=edge_rows,
        aliases=alias_rows,
        catalog=catalog,
    )
    if dry_run:
        return summary

    changed_nodes = set(summary["nodes"]["created"])
    changed_nodes.update(item["node_id"] for item in summary["nodes"]["updated"])
    for node in node_rows:
        if node["node_id"] not in changed_nodes:
            continue
        attrs = dict(node.get("attrs", {}))
        store.add_node(node["node_id"], node["node_type"], node["label"], attrs=attrs)

    changed_aliases = set(summary["aliases"]["created"])
    changed_aliases.update(item["alias"] for item in summary["aliases"]["updated"])
    for alias, canonical in alias_rows.items():
        if alias not in changed_aliases:
            continue
        store.add_alias(alias, canonical)

    changed_edges = set(summary["edges"]["created"])
    changed_edges.update(item["edge"] for item in summary["edges"]["updated"])
    for edge in edge_rows:
        edge_type = edge["edge_type"].value if isinstance(edge["edge_type"], EdgeType) else str(edge["edge_type"])
        edge_key = f"{edge['source_id']}->{edge['target_id']}:{edge_type}"
        if edge_key not in changed_edges:
            continue
        store.merge_edge(
            edge["source_id"],
            edge["target_id"],
            edge["edge_type"],
            valid_from=edge["valid_from"],
            confidence=float(edge["confidence"]),
            source=edge.get("source", "two_wheeler_refresh"),
            evidence=edge.get("evidence"),
        )

    alias_count_after = store.conn.execute("SELECT COUNT(*) FROM kg_entity_aliases").fetchone()[0]
    summary["stats_after"] = store.stats()
    summary["alias_count_after"] = int(alias_count_after)
    summary["orphans_after"] = [node_id for node_id in store.g.nodes() if store.g.degree(node_id) == 0]
    return summary


def refresh_two_wheeler_data(
    *,
    state_db_path: str | Path = DEFAULT_STATE_DB,
    graph_db_path: str | Path = DEFAULT_GRAPH_DB,
    run_id: str = DEFAULT_RUN_ID,
    repo_root: Path = REPO_ROOT,
    strict_assets: bool = True,
    dry_run: bool = False,
    catalog_path: str | Path | None = None,
    graph_aliases: dict[str, str] | None = None,
    graph_edges: Iterable[dict[str, Any]] | None = None,
    graph_nodes: Iterable[dict[str, Any]] | None = None,
    image_assets: Iterable[ImageAssetRef] | None = None,
    sku_records: Iterable[SkuRecord] | None = None,
) -> dict[str, Any]:
    conn = connect(Path(state_db_path))
    init_db(conn)
    store = GraphStore(graph_db_path)
    try:
        catalog = load_catalog(catalog_path)
        package = build_research_package(
            run_id=run_id,
            repo_root=repo_root,
            strict_assets=strict_assets,
            image_assets=image_assets,
            sku_records=sku_records,
            catalog=catalog,
        )
        state_summary = apply_competitive_refresh(conn, package, dry_run=dry_run)
        graph_summary = apply_graph_refresh(
            store,
            nodes=graph_nodes,
            edges=graph_edges,
            aliases=graph_aliases,
            dry_run=dry_run,
            catalog=catalog,
        )
        result = {
            "run_id": run_id,
            "dry_run": dry_run,
            "catalog_path": str(Path(catalog_path)) if catalog_path else (str(DEFAULT_CATALOG_PATH) if DEFAULT_CATALOG_PATH.exists() else ""),
            "state": state_summary,
            "graph": graph_summary,
            "dedup_rules": [
                "asset_ledger.asset_id primary key; no-op updates skipped by payload diff",
                "sku_catalog.sku_id primary key; no-op updates skipped by payload diff",
                "kg_nodes.node_id upsert; unchanged nodes are not rewritten",
                "kg_entity_aliases.alias primary key; unchanged aliases are not rewritten",
                "kg_edges use GraphStore.merge_edge(source_id,target_id,edge_type) to prevent duplicate live edges",
            ],
        }
        if not dry_run:
            result["pollution_check"] = {
                "theses": conn.execute("SELECT COUNT(*) FROM theses").fetchone()[0],
                "sources": conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0],
                "monitors": conn.execute("SELECT COUNT(*) FROM monitors").fetchone()[0],
            }
        return result
    finally:
        store.close()
        conn.close()


def write_refresh_changelog(summary: dict[str, Any], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
