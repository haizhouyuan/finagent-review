from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SectorGrammar:
    key: str
    label: str
    description: str
    stage_focus: tuple[str, ...]
    proving_cues: tuple[str, ...]
    constraint_cues: tuple[str, ...]


SECTOR_GRAMMARS: dict[str, SectorGrammar] = {
    "power_equipment_product_progress": SectorGrammar(
        key="power_equipment_product_progress",
        label="Power Equipment Product Progress",
        description="Track product commercialization milestones for transformers, SST, and related delivery equipment.",
        stage_focus=("prototype", "sample", "customer_validation", "pilot", "first_commercial_shipment"),
        proving_cues=("送样", "客户测试", "首单", "商业交付", "交期"),
        constraint_cues=("延期", "认证放开", "交付推迟", "毛利不兑现"),
    ),
    "power_equipment_commercialization": SectorGrammar(
        key="power_equipment_commercialization",
        label="Power Equipment Commercialization",
        description="Track order, revenue, backlog, gross margin, and customer adoption for power equipment scale-up.",
        stage_focus=("repeat_order", "capacity_expansion", "mass_adoption"),
        proving_cues=("订单", "在手订单", "收入占比", "毛利率", "交付"),
        constraint_cues=("回款恶化", "库存积压", "订单放缓", "利润不兑现"),
    ),
    "power_equipment_capacity_and_delivery": SectorGrammar(
        key="power_equipment_capacity_and_delivery",
        label="Power Equipment Capacity and Delivery",
        description="Track alternative expressions via capacity expansion, grid delivery, and backlog conversion.",
        stage_focus=("repeat_order", "capacity_expansion", "mass_adoption"),
        proving_cues=("招标", "中标", "扩产", "交付", "UHV"),
        constraint_cues=("招标延迟", "交付下滑", "产能利用率下降"),
    ),
    "ai_energy_generation_scaleup": SectorGrammar(
        key="ai_energy_generation_scaleup",
        label="AI Energy Generation Scale-up",
        description="Track generation buildout for AI power demand including turbines, gas, and baseload additions.",
        stage_focus=("customer_validation", "pilot", "first_commercial_shipment", "repeat_order"),
        proving_cues=("订单", "装机", "投运", "capex", "PPA"),
        constraint_cues=("capex下调", "交付瓶颈", "许可延迟", "项目取消"),
    ),
    "ai_energy_alternative_power": SectorGrammar(
        key="ai_energy_alternative_power",
        label="AI Energy Alternative Power",
        description="Track alternative power expressions such as backup, microgrid, or onsite generation suppliers.",
        stage_focus=("pilot", "first_commercial_shipment", "repeat_order"),
        proving_cues=("试点", "首单", "部署", "客户扩容"),
        constraint_cues=("客户流失", "成本超支", "利用率不足"),
    ),
    "ai_power_onsite_generation": SectorGrammar(
        key="ai_power_onsite_generation",
        label="AI Power Onsite Generation (Legacy Alias)",
        description="Backward-compatible alias for onsite generation and alternative datacenter power expressions.",
        stage_focus=("pilot", "first_commercial_shipment", "repeat_order"),
        proving_cues=("试点", "首单", "部署", "客户扩容"),
        constraint_cues=("客户流失", "成本超支", "利用率不足"),
    ),
    "ai_energy_power_generation_scaleup": SectorGrammar(
        key="ai_energy_power_generation_scaleup",
        label="AI Energy Power Generation Scale-up",
        description="Track benchmark or competitor generation buildout connected to AI datacenter power demand.",
        stage_focus=("pilot", "first_commercial_shipment", "capacity_expansion"),
        proving_cues=("扩产", "交付", "产线", "商用"),
        constraint_cues=("延迟", "订单取消", "资本约束"),
    ),
    "ai_energy_power_stack_benchmark": SectorGrammar(
        key="ai_energy_power_stack_benchmark",
        label="AI Energy Power Stack Benchmark",
        description="Track benchmark stack players across power controls, electrical rooms, and datacenter power architecture.",
        stage_focus=("concept", "early_prototype", "prototype", "pilot"),
        proving_cues=("方案", "架构", "合作", "样机"),
        constraint_cues=("路线切换", "标准变化", "生态排斥"),
    ),
    "commercial_space_payload_and_defense": SectorGrammar(
        key="commercial_space_payload_and_defense",
        label="Commercial Space Payload and Defense",
        description="Track monetization from payload, defense, and space systems programs rather than launch hype alone.",
        stage_focus=("customer_validation", "pilot", "first_commercial_shipment", "repeat_order"),
        proving_cues=("合同", "订单", "发射排期", "国防客户"),
        constraint_cues=("任务推迟", "合同滑移", "预算下调"),
    ),
    "commercial_space_launch_progress": SectorGrammar(
        key="commercial_space_launch_progress",
        label="Commercial Space Launch Progress",
        description="Track launch cadence, vehicle milestone timing, and missed milestones as explicit events.",
        stage_focus=("prototype", "sample", "customer_validation", "pilot", "first_commercial_shipment"),
        proving_cues=("首飞", "热试车", "发射窗口", "发射成功"),
        constraint_cues=("延期", "故障", "监管阻塞", "停飞"),
    ),
    "silicon_photonics_module_scaleup": SectorGrammar(
        key="silicon_photonics_module_scaleup",
        label="Silicon Photonics Module Scale-up",
        description="Track transceiver and optical module volume ramps, especially 800G and 1.6T commercialization.",
        stage_focus=("sample", "customer_validation", "first_commercial_shipment", "repeat_order", "capacity_expansion"),
        proving_cues=("800G", "1.6T", "量产", "客户导入", "capex"),
        constraint_cues=("ASP下行", "良率压力", "客户切换", "需求回落"),
    ),
    "silicon_photonics_cpo_progress": SectorGrammar(
        key="silicon_photonics_cpo_progress",
        label="Silicon Photonics CPO Progress",
        description="Track CPO as an option thesis with packaging and ecosystem milestones separated from current module ramps.",
        stage_focus=("early_prototype", "prototype", "sample", "customer_validation"),
        proving_cues=("CPO", "共封装", "样机", "联合验证"),
        constraint_cues=("生态延后", "客户未采用", "散热瓶颈", "标准未定"),
    ),
    "commodity_memory_constraint": SectorGrammar(
        key="commodity_memory_constraint",
        label="Commodity Memory Constraint",
        description="Track commodity memory pricing, utilization, and downside constraints for laggards or benchmark names.",
        stage_focus=("repeat_order", "capacity_expansion", "mass_adoption"),
        proving_cues=("价格回升", "稼动率", "库存去化", "产能调整"),
        constraint_cues=("价格下行", "库存反弹", "需求疲弱"),
    ),
    "memory_hbm4_transition": SectorGrammar(
        key="memory_hbm4_transition",
        label="Memory HBM4 Transition",
        description="Track HBM4 and advanced packaging transition milestones, especially logic die and packaging bottlenecks.",
        stage_focus=("prototype", "sample", "customer_validation", "pilot"),
        proving_cues=("HBM4", "验证", "封装", "logic die", "量产计划"),
        constraint_cues=("延迟", "封装瓶颈", "良率压力", "客户认证推迟"),
    ),
    "memory_hbm_scaleup": SectorGrammar(
        key="memory_hbm_scaleup",
        label="Memory HBM Scale-up",
        description="Track HBM shipment growth, attach rates, and packaging capacity as the core commercialization driver.",
        stage_focus=("first_commercial_shipment", "repeat_order", "capacity_expansion", "mass_adoption"),
        proving_cues=("HBM出货", "capex", "封装产能", "客户订单"),
        constraint_cues=("价格压力", "capex削减", "封装受限", "供需失衡"),
    ),
}


def get_sector_grammar(grammar_key: str | None) -> SectorGrammar | None:
    if not grammar_key:
        return None
    return SECTOR_GRAMMARS.get(str(grammar_key).strip())


def grammar_prompt_lines(grammar_keys: list[str] | tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for grammar_key in sorted(dict.fromkeys(str(item) for item in grammar_keys if item)):
        grammar = get_sector_grammar(grammar_key)
        if grammar is None:
            continue
        lines.append(
            f"- {grammar.key}: {grammar.description} "
            f"(stage_focus={', '.join(grammar.stage_focus)}; "
            f"prove_with={', '.join(grammar.proving_cues)}; "
            f"watch_constraints={', '.join(grammar.constraint_cues)})"
        )
    return lines


def list_sector_grammars() -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for key in sorted(SECTOR_GRAMMARS):
        grammar = SECTOR_GRAMMARS[key]
        items.append(
            {
                "key": grammar.key,
                "label": grammar.label,
                "description": grammar.description,
                "stage_focus": list(grammar.stage_focus),
                "proving_cues": list(grammar.proving_cues),
                "constraint_cues": list(grammar.constraint_cues),
            }
        )
    return items
