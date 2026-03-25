#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SMOKE_ROOT = REPO_ROOT / "state" / "smoke_runs" / "live_cases"
VOICE_MEMO_PATH = REPO_ROOT / "state" / "test_inputs" / "2026-02-24_investment_framework_rebuild_memo.txt"
VIDEO_DIGEST_PATH = Path(
    "/vol1/1000/projects/codexread/archives/topics/bili_up_3546976515786791/digests/2026-03-06_bilibili_BV1fpPHzDESK.md"
)


def run_json(args: list[str]) -> dict:
    cmd = [sys.executable, "-m", "finagent.cli", "--root", str(SMOKE_ROOT), *args]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout={proc.stdout}\nstderr={proc.stderr}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid json from {' '.join(cmd)}: {proc.stdout}") from exc


def fetch_board() -> dict:
    return run_json(["board"])


def fetch_today_cockpit() -> dict:
    return run_json(["today-cockpit"])


def fetch_thesis_board() -> dict:
    return run_json(["thesis-board"])


def fetch_thesis_focus(thesis_id: str, limit: int = 20) -> dict:
    return run_json(["thesis-focus", "--thesis-id", thesis_id, "--limit", str(limit)])


def fetch_daily_refresh(skip_fetch: bool = False, skip_monitors: bool = False, limit: int = 10) -> dict:
    args = ["daily-refresh", "--limit", str(limit)]
    if skip_fetch:
        args.append("--skip-fetch")
    if skip_monitors:
        args.append("--skip-monitors")
    return run_json(args)


def fetch_daily_shortcut(skip_fetch: bool = False, skip_monitors: bool = False, limit: int = 10) -> dict:
    args = ["daily", "--limit", str(limit)]
    if skip_fetch:
        args.append("--skip-fetch")
    if skip_monitors:
        args.append("--skip-monitors")
    return run_json(args)


def fetch_voice_memo_triage(artifact_id: str, limit: int = 40) -> dict:
    return run_json(["voice-memo-triage", "--artifact-id", artifact_id, "--limit", str(limit)])


def fetch_pattern_library(thesis_id: str = "", limit: int = 20) -> dict:
    args = ["pattern-library", "--limit", str(limit)]
    if thesis_id:
        args.extend(["--thesis-id", thesis_id])
    return run_json(args)


def fetch_focus_shortcut(thesis_id: str, limit: int = 20) -> dict:
    return run_json(["focus", "--thesis-id", thesis_id, "--limit", str(limit)])


def fetch_weekly_shortcut(days: int = 7, limit: int = 8) -> dict:
    return run_json(["weekly", "--days", str(days), "--limit", str(limit)])


def fetch_theme_map() -> dict:
    return run_json(["theme-map"])


def fetch_watch_board() -> dict:
    return run_json(["watch-board"])


def fetch_target_case_dashboard() -> dict:
    return run_json(["target-case-dashboard"])


def fetch_decision_dashboard(days: int = 7, limit: int = 12) -> dict:
    return run_json(["decision-dashboard", "--days", str(days), "--limit", str(limit)])


def fetch_decision_journal(days: int = 30, limit: int = 20, thesis_id: str = "", target_case_id: str = "") -> dict:
    args = ["decision-journal", "--days", str(days), "--limit", str(limit)]
    if thesis_id:
        args.extend(["--thesis-id", thesis_id])
    if target_case_id:
        args.extend(["--target-case-id", target_case_id])
    return run_json(args)


def fetch_decision_maintenance_queue(days: int = 30, limit: int = 20) -> dict:
    return run_json(["decision-maintenance-queue", "--days", str(days), "--limit", str(limit)])


def fetch_routing_board() -> dict:
    return run_json(["routing-board"])


def fetch_route_workbench(status: str = "pending", route_type: str = "", source_id: str = "", thesis_id: str = "", limit: int = 80) -> dict:
    args = ["route-workbench", "--status", status, "--limit", str(limit)]
    if route_type:
        args.extend(["--route-type", route_type])
    if source_id:
        args.extend(["--source-id", source_id])
    if thesis_id:
        args.extend(["--thesis-id", thesis_id])
    return run_json(args)


def fetch_route_normalization_queue(limit: int = 80) -> dict:
    return run_json(["route-normalization-queue", "--limit", str(limit)])


def fetch_intake_inbox() -> dict:
    return run_json(["intake-inbox"])


def fetch_review_board() -> dict:
    return run_json(["review-board"])


def fetch_review_remediation_queue(limit: int = 20) -> dict:
    return run_json(["review-remediation-queue", "--limit", str(limit)])


def fetch_playbook_board() -> dict:
    return run_json(["playbook-board"])


def fetch_source_board() -> dict:
    return run_json(["source-board"])


def fetch_source_track_record(limit: int = 20) -> dict:
    return run_json(["source-track-record", "--limit", str(limit)])


def fetch_source_feedback_workbench(source_id: str = "", limit: int = 20) -> dict:
    args = ["source-feedback-workbench", "--limit", str(limit)]
    if source_id:
        args.extend(["--source-id", source_id])
    return run_json(args)


def fetch_source_revisit_workbench(limit: int = 20) -> dict:
    return run_json(["source-revisit-workbench", "--limit", str(limit)])


def fetch_source_remediation_queue(days: int = 30, limit: int = 20) -> dict:
    return run_json(["source-remediation-queue", "--days", str(days), "--limit", str(limit)])


def fetch_verification_remediation_queue(days: int = 30, limit: int = 20) -> dict:
    return run_json(["verification-remediation-queue", "--days", str(days), "--limit", str(limit)])


def fetch_verification_remediation_batches(days: int = 30, limit: int = 20) -> dict:
    return run_json(["verification-remediation-batches", "--days", str(days), "--limit", str(limit)])


def fetch_source_viewpoint_workbench(source_id: str = "", include_existing: bool = False, limit: int = 20) -> dict:
    args = ["source-viewpoint-workbench", "--limit", str(limit)]
    if source_id:
        args.extend(["--source-id", source_id])
    if include_existing:
        args.append("--include-existing")
    return run_json(args)


def fetch_validation_board(verdict: str = "", thesis_id: str = "", source_id: str = "", limit: int = 50) -> dict:
    args = ["validation-board", "--limit", str(limit)]
    if verdict:
        args.extend(["--verdict", verdict])
    if thesis_id:
        args.extend(["--thesis-id", thesis_id])
    if source_id:
        args.extend(["--source-id", source_id])
    return run_json(args)


def fetch_promotion_wizard() -> dict:
    return run_json(["promotion-wizard"])


def fetch_corroboration_queue(status: str = "pending") -> dict:
    return run_json(["corroboration-queue", "--status", status])


def fetch_thesis_gate_report() -> dict:
    return run_json(["thesis-gate-report"])


def build_weekly_decision_note(out_path: Path, days: int = 7, limit: int = 8) -> dict:
    return run_json(
        [
            "weekly-decision-note",
            "--days",
            str(days),
            "--limit",
            str(limit),
            "--out",
            str(out_path),
        ]
    )


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def run_recipe_command(recipe: dict) -> dict:
    args = [recipe["command"]]
    recipe_args = recipe.get("args", {})
    for route_id in recipe_args.get("route-id", []):
        args.extend(["--route-id", route_id])
    for key in ("status", "link-object-type", "link-object-id", "link-kind"):
        value = recipe_args.get(key)
        if value:
            args.extend([f"--{key}", str(value)])
    hints = recipe.get("hints", {})
    for artifact_id in hints.get("suggested_evidence_artifact_ids", []):
        args.extend(["--evidence-artifact-id", artifact_id])
    note = hints.get("note") or recipe_args.get("note", "")
    if note:
        args.extend(["--note", note])
    return run_json(args)


def create_cases() -> dict:
    if SMOKE_ROOT.exists():
        shutil.rmtree(SMOKE_ROOT)
    SMOKE_ROOT.mkdir(parents=True, exist_ok=True)

    run_json(["init"])

    voice_artifact_id = "art_voice_memo_framework"
    video_artifact_id = "art_video_fuzong_latest_digest"

    run_json(
        [
            "ingest-text",
            "--source-id",
            "src_voice_memo",
            "--source-type",
            "personal",
            "--source-name",
            "Voice Memo Inbox",
            "--primaryness",
            "personal",
            "--artifact-id",
            voice_artifact_id,
            "--path",
            str(VOICE_MEMO_PATH),
            "--artifact-kind",
            "audio_transcript",
            "--title",
            "2026-02-24 投研框架重构语音备忘",
            "--language",
            "zh",
        ]
    )
    video_intake_result = run_json(
        [
            "intake-kol-digest",
            "--source-id",
            "src_kol_fuzong",
            "--source-name",
            "福总 机构一手调研",
            "--artifact-id",
            video_artifact_id,
            "--path",
            str(VIDEO_DIGEST_PATH),
            "--artifact-kind",
            "video_digest",
            "--title",
            "2026-03-06 福总最新 digest",
            "--language",
            "zh",
            "--speaker",
            "福总",
            "--min-chars",
            "18",
        ]
    )

    nvda_artifact_id = run_json(["fetch-sec-submissions", "--ticker", "NVDA"])["artifact_id"]
    tsla_artifact_id = run_json(["fetch-sec-submissions", "--ticker", "TSLA"])["artifact_id"]
    cninfo_artifact_id = run_json(
        ["fetch-cninfo-announcements", "--ticker", "300308.SZ", "--lookback-days", "60", "--limit", "3"]
    )["artifact_id"]
    openalex_artifact_id = run_json(["fetch-openalex", "--query", "embodied intelligence", "--per-page", "3"])[
        "artifact_id"
    ]
    aave_artifact_id = run_json(["fetch-defillama", "--slug", "aave"])["artifact_id"]

    extract_results = {
        voice_artifact_id: run_json(["extract-claims", "--artifact-id", voice_artifact_id, "--speaker", "user", "--min-chars", "18"]),
        video_artifact_id: video_intake_result,
        nvda_artifact_id: run_json(["extract-claims", "--artifact-id", nvda_artifact_id, "--speaker", "SEC", "--min-chars", "18"]),
        tsla_artifact_id: run_json(["extract-claims", "--artifact-id", tsla_artifact_id, "--speaker", "SEC", "--min-chars", "18"]),
        cninfo_artifact_id: run_json(
            ["extract-claims", "--artifact-id", cninfo_artifact_id, "--speaker", "CNINFO", "--min-chars", "18"]
        ),
        openalex_artifact_id: run_json(
            ["extract-claims", "--artifact-id", openalex_artifact_id, "--speaker", "OpenAlex", "--min-chars", "18"]
        ),
        aave_artifact_id: run_json(["extract-claims", "--artifact-id", aave_artifact_id, "--speaker", "DefiLlama", "--min-chars", "18"]),
    }
    routing_results = {
        voice_artifact_id: run_json(["route-claims", "--artifact-id", voice_artifact_id]),
        video_artifact_id: run_json(["route-claims", "--artifact-id", video_artifact_id]),
        nvda_artifact_id: run_json(["route-claims", "--artifact-id", nvda_artifact_id]),
        tsla_artifact_id: run_json(["route-claims", "--artifact-id", tsla_artifact_id]),
        cninfo_artifact_id: run_json(["route-claims", "--artifact-id", cninfo_artifact_id]),
        openalex_artifact_id: run_json(["route-claims", "--artifact-id", openalex_artifact_id]),
        aave_artifact_id: run_json(["route-claims", "--artifact-id", aave_artifact_id]),
    }
    accepted_route_id = next(
        route["route_id"]
        for route in routing_results[video_artifact_id]["routes"]
        if route["route_type"] == "corroboration_needed"
    )

    run_json(
        [
            "create-theme",
            "--theme-id",
            "theme_ai_infra",
            "--name",
            "AI Inference Infrastructure",
            "--why-it-matters",
            "AI 基础设施扩张会向 GPU、网络和相关供应链传导。",
            "--maturity-stage",
            "growth",
            "--commercialization-paths",
            "compute,gpu,networking",
            "--importance-status",
            "priority",
        ]
    )
    run_json(
        [
            "create-theme",
            "--theme-id",
            "theme_embodied",
            "--name",
            "Embodied Intelligence",
            "--why-it-matters",
            "具身智能会向机器人本体、核心零部件和软件栈传导。",
            "--maturity-stage",
            "emerging",
            "--commercialization-paths",
            "robotics,automation",
            "--importance-status",
            "tracking",
        ]
    )
    run_json(
        [
            "create-theme",
            "--theme-id",
            "theme_web3_finance",
            "--name",
            "On-chain Finance",
            "--why-it-matters",
            "链上金融协议和稳定币基础设施可能形成新的价值捕获层。",
            "--maturity-stage",
            "emerging",
            "--commercialization-paths",
            "defi,stablecoins,rails",
            "--importance-status",
            "tracking",
        ]
    )
    run_json(
        [
            "create-theme",
            "--theme-id",
            "theme_china_compute",
            "--name",
            "China Compute Chain",
            "--why-it-matters",
            "视频/KOL 里出现的国产算力和恒生科技叙事需要被单独归档为线索主题。",
            "--maturity-stage",
            "emerging",
            "--commercialization-paths",
            "domestic_compute,semis",
            "--importance-status",
            "scouting",
        ]
    )

    run_json(
        [
            "create-entity",
            "--entity-id",
            "ent_nvda",
            "--entity-type",
            "company",
            "--name",
            "NVIDIA",
            "--alias",
            "NVDA",
            "--symbol",
            "NVDA",
            "--jurisdiction",
            "US",
        ]
    )
    run_json(
        [
            "create-entity",
            "--entity-id",
            "ent_tsla",
            "--entity-type",
            "company",
            "--name",
            "Tesla",
            "--alias",
            "TSLA",
            "--symbol",
            "TSLA",
            "--jurisdiction",
            "US",
        ]
    )
    run_json(
        [
            "create-entity",
            "--entity-id",
            "ent_300308",
            "--entity-type",
            "company",
            "--name",
            "中际旭创",
            "--alias",
            "300308.SZ",
            "--symbol",
            "300308.SZ",
            "--jurisdiction",
            "CN",
        ]
    )
    run_json(
        [
            "create-entity",
            "--entity-id",
            "ent_aave",
            "--entity-type",
            "token",
            "--name",
            "Aave",
            "--alias",
            "AAVE",
            "--symbol",
            "AAVE",
            "--jurisdiction",
            "global",
        ]
    )
    run_json(
        [
            "create-entity",
            "--entity-id",
            "ent_embodied",
            "--entity-type",
            "technology",
            "--name",
            "Embodied Intelligence",
            "--alias",
            "具身智能",
            "--jurisdiction",
            "global",
        ]
    )
    run_json(
        [
            "create-entity",
            "--entity-id",
            "ent_china_compute",
            "--entity-type",
            "technology",
            "--name",
            "Domestic Compute",
            "--alias",
            "国产算力",
            "--jurisdiction",
            "CN",
        ]
    )

    run_json(
        [
            "create-thesis",
            "--thesis-id",
            "thesis_ai_infra",
            "--thesis-version-id",
            "thv_ai_infra_v1",
            "--title",
            "AI inference infra demand remains investable over 6-12 months",
            "--status",
            "framed",
            "--horizon-months",
            "12",
            "--theme-id",
            "theme_ai_infra",
            "--artifact-id",
            voice_artifact_id,
            "--artifact-id",
            nvda_artifact_id,
            "--owner",
            "human",
            "--statement",
            "未来 6-12 个月 AI 推理基础设施扩张仍会向 GPU 和相关供应链传导。",
            "--mechanism-chain",
            "模型部署扩大 -> 推理算力需求增长 -> GPU/网络资源需求扩张 -> 相关公司订单和叙事强化。",
            "--why-now",
            "语音备忘录明确把 AI 赛道作为重点，SEC 官方披露可持续补充验证。",
            "--base-case",
            "算力需求延续，核心受益方持续获得市场关注。",
            "--counter-case",
            "资本开支回落或竞争格局恶化使收益兑现弱于预期。",
            "--invalidators",
            "需求增速明显放缓，或关键公司披露出现持续性恶化。",
            "--required-followups",
            "补充官方财报、产能和订单验证。",
            "--human-conviction",
            "0.72",
        ]
    )
    run_json(
        [
            "create-thesis",
            "--thesis-id",
            "thesis_embodied",
            "--thesis-version-id",
            "thv_embodied_v1",
            "--title",
            "Embodied intelligence should move from concept to investable milestones",
            "--status",
            "active",
            "--horizon-months",
            "12",
            "--theme-id",
            "theme_embodied",
            "--artifact-id",
            voice_artifact_id,
            "--artifact-id",
            openalex_artifact_id,
            "--artifact-id",
            tsla_artifact_id,
            "--owner",
            "human",
            "--statement",
            "具身智能正在从研究与叙事阶段向可跟踪的商业化里程碑演进。",
            "--mechanism-chain",
            "论文与技术进展积累 -> 产业关注度提升 -> 产品化/量产节点出现 -> 相关标的获得业绩和估值弹性。",
            "--why-now",
            "语音备忘录把具身智能列为明确关注方向，OpenAlex 可用于持续扫描研究进展，SEC 披露可作为上市公司跟踪入口。",
            "--base-case",
            "未来 6-12 个月围绕机器人/自动化的商业化叙事继续强化。",
            "--counter-case",
            "技术里程碑无法转化为商业兑现，市场热度先行后退潮。",
            "--invalidators",
            "关键公司对相关业务不再强调，或技术进展停滞。",
            "--required-followups",
            "补充量产、订单、供应链验证。",
            "--human-conviction",
            "0.68",
        ]
    )
    run_json(
        [
            "create-thesis",
            "--thesis-id",
            "thesis_onchain_finance",
            "--thesis-version-id",
            "thv_onchain_finance_v1",
            "--title",
            "On-chain finance remains a tracking thesis for the next cycle",
            "--status",
            "framed",
            "--horizon-months",
            "12",
            "--theme-id",
            "theme_web3_finance",
            "--artifact-id",
            voice_artifact_id,
            "--artifact-id",
            aave_artifact_id,
            "--owner",
            "human",
            "--statement",
            "链上金融会在下一轮周期里提供新的价值捕获机会，但需要先把协议增长与代币捕获分离验证。",
            "--mechanism-chain",
            "市场周期修复 -> 协议使用与链上金融活动回升 -> 协议指标改善 -> 代币/相关资产是否捕获得到验证。",
            "--why-now",
            "语音备忘录明确提出补课 Web3，DefiLlama 可先提供协议层指标底盘。",
            "--base-case",
            "头部协议在费用、TVL 和使用上率先恢复。",
            "--counter-case",
            "协议增长存在，但代币价值捕获弱或被稀释抵消。",
            "--invalidators",
            "核心链上指标持续恶化，或监管/安全事件明显破坏预期。",
            "--required-followups",
            "补协议治理、代币经济学和官方文档。",
            "--human-conviction",
            "0.58",
        ]
    )
    run_json(
        [
            "create-thesis",
            "--thesis-id",
            "thesis_kol_china_compute_seed",
            "--thesis-version-id",
            "thv_kol_china_compute_v1",
            "--title",
            "KOL video seed on China compute chain",
            "--status",
            "seed",
            "--horizon-months",
            "6",
            "--theme-id",
            "theme_china_compute",
            "--artifact-id",
            video_artifact_id,
            "--owner",
            "human",
            "--statement",
            "福总最新视频里出现的国产算力线索值得归档，但还不能直接提升为 active thesis。",
            "--mechanism-chain",
            "KOL 提供线索 -> 触发后续官方核验 -> 若证据充分再升级为正式 thesis。",
            "--why-now",
            "这是 KOL/video lane 的真实入口测试。",
            "--base-case",
            "形成后续核验清单并找到对应官方材料。",
            "--counter-case",
            "视频叙事无法找到一手证据支撑。",
            "--invalidators",
            "相关线索被官方披露或后续事实否定。",
            "--required-followups",
            "补交易所公告、公司披露和产业链验证。",
            "--human-conviction",
            "0.43",
        ]
    )
    route_apply_result = run_json(
        [
            "apply-route",
            "--route-id",
            accepted_route_id,
            "--link-object-type",
            "thesis",
            "--link-object-id",
            "thesis_kol_china_compute_seed",
            "--evidence-artifact-id",
            cninfo_artifact_id,
            "--note",
            "smoke_link_kol_seed",
        ]
    )
    source_viewpoint_workbench_before = fetch_source_viewpoint_workbench(source_id="src_kol_fuzong")
    fuzong_viewpoint_candidate_before = next(
        item
        for item in source_viewpoint_workbench_before["items"]
        if item["source_id"] == "src_kol_fuzong" and item["artifact_id"] == video_artifact_id
    )
    source_viewpoint_result = run_json(
        [
            "synthesize-source-viewpoint",
            "--source-id",
            "src_kol_fuzong",
            "--artifact-id",
            video_artifact_id,
            "--thesis-id",
            "thesis_kol_china_compute_seed",
        ]
    )
    source_feedback_workbench_before = fetch_source_feedback_workbench(source_id="src_kol_fuzong")
    source_feedback_result = run_json(
        [
            "record-source-feedback",
            "--source-id",
            "src_kol_fuzong",
            "--source-viewpoint-id",
            source_viewpoint_result["source_viewpoint_id"],
            "--validation-case-id",
            route_apply_result["validation_case_id"],
            "--feedback-type",
            "high_signal",
            "--note",
            "首条福总观点已形成 validation case，列为高优先跟踪来源。",
        ]
    )
    source_feedback_old_result = run_json(
        [
            "record-source-feedback",
            "--source-id",
            "src_kol_fuzong",
            "--source-viewpoint-id",
            source_viewpoint_result["source_viewpoint_id"],
            "--feedback-type",
            "useful_context",
            "--note",
            "这条旧反馈只保留历史参考，不应长期抬高来源优先级。",
            "--created-at",
            "2025-10-01T00:00:00+00:00",
        ]
    )
    source_feedback_stale_defillama = run_json(
        [
            "record-source-feedback",
            "--source-id",
            "src_defillama",
            "--feedback-type",
            "useful_context",
            "--note",
            "旧的 Web3 来源反馈，需要重新确认是否仍值得持续跟踪。",
            "--created-at",
            "2025-09-15T00:00:00+00:00",
        ]
    )
    source_revisit_workbench_before = fetch_source_revisit_workbench()
    source_feedback_refresh_defillama = run_json(
        [
            "record-source-feedback",
            "--source-id",
            "src_defillama",
            "--feedback-type",
            "useful_context",
            "--note",
            "重新确认该来源当前仍提供有用上下文。",
        ]
    )
    source_feedback_workbench_after = fetch_source_feedback_workbench(source_id="src_kol_fuzong")
    source_revisit_workbench_after = fetch_source_revisit_workbench()
    source_viewpoint_workbench_after = fetch_source_viewpoint_workbench(source_id="src_kol_fuzong")
    source_viewpoint_workbench_existing = fetch_source_viewpoint_workbench(source_id="src_kol_fuzong", include_existing=True)

    run_json(
        [
            "create-target",
            "--target-id",
            "target_nvda",
            "--entity-id",
            "ent_nvda",
            "--asset-class",
            "us_equity",
            "--venue",
            "NASDAQ",
            "--ticker-or-symbol",
            "NVDA",
            "--currency",
            "USD",
            "--liquidity-bucket",
            "mega_cap",
        ]
    )
    run_json(
        [
            "create-target",
            "--target-id",
            "target_tsla",
            "--entity-id",
            "ent_tsla",
            "--asset-class",
            "us_equity",
            "--venue",
            "NASDAQ",
            "--ticker-or-symbol",
            "TSLA",
            "--currency",
            "USD",
            "--liquidity-bucket",
            "mega_cap",
        ]
    )
    run_json(
        [
            "create-target",
            "--target-id",
            "target_300308",
            "--entity-id",
            "ent_300308",
            "--asset-class",
            "a_share_equity",
            "--venue",
            "SZSE",
            "--ticker-or-symbol",
            "300308.SZ",
            "--currency",
            "CNY",
            "--liquidity-bucket",
            "large_cap",
        ]
    )
    run_json(
        [
            "create-target",
            "--target-id",
            "target_aave",
            "--entity-id",
            "ent_aave",
            "--asset-class",
            "token",
            "--venue",
            "onchain",
            "--ticker-or-symbol",
            "AAVE",
            "--currency",
            "USD",
            "--liquidity-bucket",
            "large",
        ]
    )

    run_json(
        [
            "create-target-case",
            "--target-case-id",
            "tc_nvda_ai_infra",
            "--thesis-version-id",
            "thv_ai_infra_v1",
            "--target-id",
            "target_nvda",
            "--exposure-type",
            "direct",
            "--capture-link-strength",
            "0.88",
            "--key-metric",
            "recent_filing_count=5",
            "--valuation-context",
            "需要结合财报和市场预期继续判断，不直接由单条 filings 决定。",
            "--risks",
            "Capex 回落、竞争加剧、估值透支。",
            "--status",
            "actionable",
        ]
    )
    run_json(
        [
            "create-target-case",
            "--target-case-id",
            "tc_tsla_embodied",
            "--thesis-version-id",
            "thv_embodied_v1",
            "--target-id",
            "target_tsla",
            "--exposure-type",
            "proxy",
            "--capture-link-strength",
            "0.62",
            "--key-metric",
            "recent_filing_count=5",
            "--valuation-context",
            "更偏具身智能/自动化代理表达，需持续验证业务相关性。",
            "--risks",
            "主题正确但标的映射错误。",
            "--status",
            "covered",
        ]
    )
    run_json(
        [
            "create-target-case",
            "--target-case-id",
            "tc_300308_ai_infra",
            "--thesis-version-id",
            "thv_ai_infra_v1",
            "--target-id",
            "target_300308",
            "--exposure-type",
            "direct",
            "--capture-link-strength",
            "0.78",
            "--key-metric",
            "recent_announcement_count=3",
            "--valuation-context",
            "A 股 AI infra 表达，需结合业绩预告和快报持续验证。",
            "--risks",
            "主题拥挤、业绩兑现不及预期、景气切换过快。",
            "--status",
            "actionable",
        ]
    )
    run_json(
        [
            "create-target-case",
            "--target-case-id",
            "tc_aave_onchain",
            "--thesis-version-id",
            "thv_onchain_finance_v1",
            "--target-id",
            "target_aave",
            "--exposure-type",
            "direct",
            "--capture-link-strength",
            "0.66",
            "--key-metric",
            "max_chain_tvl=21870777202",
            "--valuation-context",
            "需要把协议增长和代币捕获拆开看。",
            "--risks",
            "治理、代币经济学、监管与安全风险。",
            "--status",
            "covered",
        ]
    )

    run_json(
        [
            "create-timing-plan",
            "--timing-plan-id",
            "timing_nvda",
            "--target-case-id",
            "tc_nvda_ai_infra",
            "--window-type",
            "6_12_months",
            "--catalyst",
            "official_filings",
            "--confirmation-signal",
            "recent_filing_count>=1",
            "--precondition",
            "ai_capex_continues",
            "--invalidator",
            "filings_turn_negative",
            "--desired-posture",
            "prepare",
        ]
    )
    run_json(
        [
            "create-timing-plan",
            "--timing-plan-id",
            "timing_tsla",
            "--target-case-id",
            "tc_tsla_embodied",
            "--window-type",
            "6_12_months",
            "--catalyst",
            "technology_milestones",
            "--confirmation-signal",
            "research_and_company_disclosure_continue",
            "--precondition",
            "commercialization_signals_emerge",
            "--invalidator",
            "theme_cools_without_execution",
            "--desired-posture",
            "observe",
        ]
    )
    run_json(
        [
            "create-timing-plan",
            "--timing-plan-id",
            "timing_300308",
            "--target-case-id",
            "tc_300308_ai_infra",
            "--window-type",
            "6_12_months",
            "--catalyst",
            "official_announcements",
            "--confirmation-signal",
            "recent_announcement_count>=1",
            "--precondition",
            "ai_optics_demand_holds",
            "--invalidator",
            "guidance_or_orders_turn_negative",
            "--desired-posture",
            "prepare",
        ]
    )
    run_json(
        [
            "create-timing-plan",
            "--timing-plan-id",
            "timing_aave",
            "--target-case-id",
            "tc_aave_onchain",
            "--window-type",
            "6_12_months",
            "--catalyst",
            "protocol_metric_recovery",
            "--confirmation-signal",
            "max_chain_tvl>=20000000000",
            "--precondition",
            "activity_recovers",
            "--invalidator",
            "major_regulatory_or_security_event",
            "--desired-posture",
            "observe",
        ]
    )

    run_json(
        [
            "create-monitor",
            "--monitor-id",
            "mon_nvda_filings",
            "--owner-object-type",
            "target_case",
            "--owner-object-id",
            "tc_nvda_ai_infra",
            "--monitor-type",
            "official",
            "--artifact-id",
            nvda_artifact_id,
            "--metric-name",
            "recent_filing_count",
            "--comparator",
            "gte",
            "--threshold-value",
            "1",
        ]
    )
    run_json(
        [
            "create-monitor",
            "--monitor-id",
            "mon_tsla_filings",
            "--owner-object-type",
            "target_case",
            "--owner-object-id",
            "tc_tsla_embodied",
            "--monitor-type",
            "official",
            "--artifact-id",
            tsla_artifact_id,
            "--metric-name",
            "recent_filing_count",
            "--comparator",
            "gte",
            "--threshold-value",
            "1",
        ]
    )
    run_json(
        [
            "create-monitor",
            "--monitor-id",
            "mon_300308_announcements",
            "--owner-object-type",
            "target_case",
            "--owner-object-id",
            "tc_300308_ai_infra",
            "--monitor-type",
            "official",
            "--artifact-id",
            cninfo_artifact_id,
            "--metric-name",
            "recent_announcement_count",
            "--comparator",
            "gte",
            "--threshold-value",
            "1",
        ]
    )
    run_json(
        [
            "create-monitor",
            "--monitor-id",
            "mon_aave_tvl",
            "--owner-object-type",
            "target_case",
            "--owner-object-id",
            "tc_aave_onchain",
            "--monitor-type",
            "onchain",
            "--artifact-id",
            aave_artifact_id,
            "--metric-name",
            "max_chain_tvl",
            "--comparator",
            "gte",
            "--threshold-value",
            "20000000000",
        ]
    )

    monitor_run = run_json(["run-monitors"])

    run_json(
        [
            "create-review",
            "--review-id",
            "review_ai_infra_2026_03_07",
            "--owner-object-type",
            "thesis",
            "--owner-object-id",
            "thesis_ai_infra",
            "--review-date",
            "2026-03-07",
            "--what-we-believed",
            "AI 算力主题仍具可持续性。",
            "--what-happened",
            "建立了首轮官方监控入口，等待后续财报和订单证据。",
            "--result",
            "unresolved",
            "--source-attribution",
            "voice_memo + sec",
            "--source-id",
            "src_voice_memo",
            "--source-id",
            "src_sec_edgar",
            "--lessons",
            "thesis/timing/monitor 已拆开，后续补官方数据更高效。",
        ]
    )
    run_json(
        [
            "create-review",
            "--review-id",
            "review_embodied_2026_03_07",
            "--owner-object-type",
            "thesis",
            "--owner-object-id",
            "thesis_embodied",
            "--review-date",
            "2026-03-07",
            "--what-we-believed",
            "具身智能需要从技术跟踪走向商业验证。",
            "--what-happened",
            "已经接通研究发现和上市公司官方披露入口。",
            "--result",
            "unresolved",
            "--source-attribution",
            "voice_memo + openalex + sec",
            "--source-id",
            "src_voice_memo",
            "--source-id",
            "src_openalex",
            "--source-id",
            "src_sec_edgar",
            "--lessons",
            "主题正确不代表标的映射正确，target case 必须单独跟踪。",
        ]
    )
    run_json(
        [
            "create-review",
            "--review-id",
            "review_300308_2026_03_07",
            "--owner-object-type",
            "target_case",
            "--owner-object-id",
            "tc_300308_ai_infra",
            "--review-date",
            "2026-03-07",
            "--what-we-believed",
            "A 股 AI infra 需要有官方公告和业绩信号支撑。",
            "--what-happened",
            "CNINFO 公告链路已接通，中际旭创样例可被正式纳入 target case。",
            "--result",
            "unresolved",
            "--source-attribution",
            "cninfo",
            "--source-id",
            "src_cninfo",
            "--lessons",
            "A 股官方披露需要单独 adapter，不能只靠 KOL/video 叙事。",
        ]
    )
    run_json(
        [
            "create-review",
            "--review-id",
            "review_onchain_2026_03_07",
            "--owner-object-type",
            "thesis",
            "--owner-object-id",
            "thesis_onchain_finance",
            "--review-date",
            "2026-03-07",
            "--what-we-believed",
            "Web3 需要先做 tracking thesis 再逐步升级。",
            "--what-happened",
            "协议指标监控已接通，但官方治理材料还缺。",
            "--result",
            "unresolved",
            "--source-attribution",
            "voice_memo + defillama",
            "--source-id",
            "src_voice_memo",
            "--source-id",
            "src_defillama",
            "--lessons",
            "协议增长和代币捕获不能混为一谈。",
        ]
    )
    bulk_apply_result = run_json(
        [
            "bulk-apply-routes",
            "--source-id",
            "src_cninfo",
            "--route-type",
            "thesis_input",
            "--link-object-type",
            "thesis",
            "--link-object-id",
            "thesis_ai_infra",
            "--note",
            "smoke_bulk_cninfo",
        ]
    )
    route_workbench_pending = fetch_route_workbench()
    route_workbench_kol_pending = fetch_route_workbench(thesis_id="thesis_kol_china_compute_seed")
    voice_memo_triage_pending = fetch_voice_memo_triage("art_voice_memo_framework")
    promotion_wizard_before = fetch_promotion_wizard()
    kol_remediation = run_json(
        [
            "remediate-thesis",
            "--thesis-id",
            "thesis_kol_china_compute_seed",
            "--action",
            "create_target_case",
            "--target-id",
            "target_300308",
            "--target-case-id",
            "tc_300308_kol_compute",
            "--exposure-type",
            "proxy",
            "--capture-link-strength",
            "0.58",
            "--key-metric",
            "recent_announcement_count=3",
            "--valuation-context",
            "KOL 国产算力线索经 CNINFO 公告补强后，先作为 proxy target case 跟踪。",
            "--risks",
            "视频叙事与公司公告节奏不一致会削弱 thesis。",
            "--target-case-status",
            "candidate",
            "--window-type",
            "6_12_months",
            "--desired-posture",
            "prepare",
            "--catalyst",
            "订单/扩产公告",
            "--confirmation-signal",
            "CNINFO 公告持续验证国产算力链条",
            "--precondition",
            "后续官方披露继续强化 video thesis",
            "--invalidator-item",
            "相关公告否定视频中的主线判断",
            "--monitor-id",
            "mon_300308_kol_compute",
            "--monitor-type",
            "official",
            "--monitor-artifact-id",
            cninfo_artifact_id,
            "--monitor-metric-name",
            "recent_announcement_count",
            "--monitor-comparator",
            "gte",
            "--monitor-threshold",
            "1",
            "--note",
            "smoke_remediate_kol_target_case",
        ]
    )
    monitor_run_after_remediation = run_json(["run-monitors"])
    promotion_wizard_after_remediation = fetch_promotion_wizard()
    promote_to_evidence = run_json(
        ["promote-thesis", "--thesis-id", "thesis_ai_infra", "--new-status", "evidence_backed", "--note", "smoke_promote_1"]
    )
    promotion_wizard_mid = fetch_promotion_wizard()
    promote_to_active = run_json(
        ["promote-thesis", "--thesis-id", "thesis_ai_infra", "--new-status", "active", "--note", "smoke_promote_2"]
    )
    promote_kol_to_evidence = run_json(
        [
            "promote-thesis",
            "--thesis-id",
            "thesis_kol_china_compute_seed",
            "--new-status",
            "evidence_backed",
            "--note",
            "smoke_promote_kol_to_evidence",
        ]
    )
    decision_dashboard_before_source_remediation = fetch_decision_dashboard()
    source_remediation_queue_before = fetch_source_remediation_queue()
    aave_governance_path = SMOKE_ROOT / "inputs" / "aave_governance_update.txt"
    aave_governance_path.parent.mkdir(parents=True, exist_ok=True)
    aave_governance_path.write_text(
        "Aave governance proposal focus: refine risk parameters, maintain protocol treasury discipline, and preserve tokenholder-aligned incentives.",
        encoding="utf-8",
    )
    aave_governance_artifact = run_json(
        [
            "ingest-text",
            "--source-id",
            "src_aave_governance",
            "--source-type",
            "governance",
            "--source-name",
            "Aave Governance",
            "--primaryness",
            "first_hand",
            "--path",
            str(aave_governance_path),
            "--artifact-id",
            "art_aave_governance_anchor",
            "--artifact-kind",
            "text_note",
            "--title",
            "Aave governance anchor note",
            "--language",
            "en",
            "--uri",
            "https://governance.aave.com/",
            "--jurisdiction",
            "global",
        ]
    )
    onchain_source_remediation = run_json(
        [
            "remediate-thesis",
            "--thesis-id",
            "thesis_onchain_finance",
            "--action",
            "attach_first_hand_artifact",
            "--artifact-id",
            aave_governance_artifact["artifact_id"],
            "--note",
            "smoke_attach_aave_governance_anchor",
        ]
    )
    decision_dashboard_before_review_remediation = fetch_decision_dashboard()
    target_case_dashboard_before_review_remediation = fetch_target_case_dashboard()
    review_remediation_queue_before = fetch_review_remediation_queue()
    direct_review_kol_target = run_json(
        [
            "create-review",
            "--review-id",
            "review_tc_300308_kol_compute_direct",
            "--owner-object-type",
            "target_case",
            "--owner-object-id",
            "tc_300308_kol_compute",
            "--review-date",
            "2026-03-09",
            "--what-we-believed",
            "福总国产算力视频线索经过 CNINFO 公告补强后，可以先进入 prepare 跟踪。",
            "--what-happened",
            "CNINFO 侧至少已经有可挂接的一手公告，线索完成了第一轮 direct review。",
            "--result",
            "unresolved",
            "--source-attribution",
            "KOL video + CNINFO direct review",
            "--source-id",
            "src_kol_fuzong",
            "--source-id",
            "src_cninfo",
            "--lessons",
            "KOL 线索即使补到 evidence_backed，也要尽快补 direct target-case review，不能长期依赖 thesis fallback。",
        ]
    )
    extracted_pattern_kol_review = run_json(
        [
            "extract-pattern",
            "--pattern-id",
            "pattern_kol_direct_review_first",
            "--review-id",
            direct_review_kol_target["review_id"],
            "--pattern-kind",
            "lesson",
            "--label",
            "KOL 线索先补 direct review",
            "--description",
            "KOL 线索即使补到 thesis，也应尽快补 direct target-case review，避免长期依赖 thesis fallback。",
            "--trigger-term",
            "kol",
            "--trigger-term",
            "算力",
            "--trigger-term",
            "compute",
            "--thesis-id",
            "thesis_kol_china_compute_seed",
        ]
    )
    decision_dashboard_before_verification_remediation = fetch_decision_dashboard()
    verification_remediation_queue_before = fetch_verification_remediation_queue()
    thesis_gate_report_before_verification_remediation = fetch_thesis_gate_report()
    onchain_verification_item = next(
        item
        for item in verification_remediation_queue_before["items"]
        if item["thesis_id"] == "thesis_onchain_finance"
        and item["remediation_action"] == "accept_corroboration_with_evidence"
    )
    onchain_corroboration_resolution = run_json(
        [
            "apply-route",
            "--route-id",
            onchain_verification_item["route_id"],
            "--status",
            "accepted",
            "--link-object-type",
            "thesis",
            "--link-object-id",
            "thesis_onchain_finance",
            "--evidence-artifact-id",
            aave_governance_artifact["artifact_id"],
            "--note",
            "smoke_accept_onchain_corroboration_with_governance_anchor",
        ]
    )
    promotion_wizard_after_verification_resolution = fetch_promotion_wizard()
    promote_onchain_to_evidence = run_json(
        [
            "promote-thesis",
            "--thesis-id",
            "thesis_onchain_finance",
            "--new-status",
            "evidence_backed",
            "--note",
            "smoke_promote_onchain_to_evidence",
        ]
    )
    verification_remediation_batches_before = fetch_verification_remediation_batches()
    kol_verification_batch = next(
        item
        for item in verification_remediation_batches_before["items"]
        if item["source_id"] == "src_kol_fuzong"
        and item["thesis_id"] == "thesis_kol_china_compute_seed"
        and item["remediation_action"] == "accept_corroboration_with_evidence"
    )
    kol_bulk_verification_resolution = run_json(
        [
            "apply-route-batch",
            *sum((["--route-id", route_id] for route_id in kol_verification_batch["route_ids"]), []),
            "--status",
            "accepted",
            "--link-object-type",
            "thesis",
            "--link-object-id",
            "thesis_kol_china_compute_seed",
            "--evidence-artifact-id",
            cninfo_artifact_id,
            "--note",
            "smoke_batch_accept_kol_verification",
        ]
    )
    promotion_wizard_after_kol_verification_batch = fetch_promotion_wizard()
    thesis_gate_report_after_kol_verification_batch = fetch_thesis_gate_report()
    kol_gate_after_verification_batch = next(
        item
        for item in thesis_gate_report_after_kol_verification_batch["items"]
        if item["thesis_id"] == "thesis_kol_china_compute_seed"
    )
    if kol_gate_after_verification_batch["can_recommend_active"]:
        promote_kol_to_active_after_batch = run_json(
            [
                "promote-thesis",
                "--thesis-id",
                "thesis_kol_china_compute_seed",
                "--new-status",
                "active",
                "--new-thesis-version-id",
                "thv_kol_china_compute_active_v2",
                "--new-title",
                "China compute chain thesis validated from KOL signal and CNINFO anchor",
                "--new-statement",
                "福总视频中的国产算力线索已经补到 CNINFO 一手锚点，可以作为 active thesis 持续跟踪。",
                "--new-mechanism-chain",
                "KOL 提供早期线索 -> CNINFO 公告补一手锚点 -> 形成可跟踪的国产算力 active thesis。",
                "--new-why-now",
                "线索已经从视频叙事升级到一手公告验证，适合进入 active 跟踪面。",
                "--note",
                "smoke_promote_kol_to_active_after_verification_batch",
            ]
        )
    else:
        promote_kol_to_active_after_batch = {
            "ok": True,
            "skipped": True,
            "reason": "promotion wizard does not recommend active",
            "gate": kol_gate_after_verification_batch,
        }
    source_viewpoint_workbench_finish_line_before = fetch_source_viewpoint_workbench(include_existing=True, limit=20)
    finish_line_source_viewpoints = []
    for item in source_viewpoint_workbench_finish_line_before["items"]:
        if item["existing_viewpoint_count"] > 0:
            continue
        finish_line_source_viewpoints.append(
            run_json(
                [
                    "synthesize-source-viewpoint",
                    "--source-id",
                    item["source_id"],
                    "--artifact-id",
                    item["artifact_id"],
                    "--thesis-id",
                    item["thesis_id"],
                    "--target-case-id",
                    item["suggested_target_case_id"],
                ]
            )
        )
    verification_remediation_batches_finish_line_before = fetch_verification_remediation_batches(limit=20)
    finish_line_verification_batch_resolutions = [
        run_recipe_command(item["recipe"]) for item in verification_remediation_batches_finish_line_before["items"]
    ]
    review_remediation_queue_finish_line_before = fetch_review_remediation_queue(limit=20)
    finish_line_review_resolutions = []
    for item in review_remediation_queue_finish_line_before["items"]:
        recipe = item["review_recipe"]
        args = [recipe["command"]]
        for key, value in recipe["args"].items():
            if value:
                args.extend([f"--{key}", str(value)])
        hints = recipe.get("hints", {})
        if hints.get("what_we_believed"):
            args.extend(["--what-we-believed", hints["what_we_believed"]])
        if hints.get("what_happened"):
            args.extend(["--what-happened", hints["what_happened"]])
        if hints.get("source_attribution"):
            args.extend(["--source-attribution", hints["source_attribution"]])
        finish_line_review_resolutions.append(run_json(args))
    route_normalization_queue_before = fetch_route_normalization_queue(limit=100)
    finish_line_route_normalization = [run_recipe_command(batch["recipe"]) for batch in route_normalization_queue_before["batches"]]
    decision_maintenance_queue_before = fetch_decision_maintenance_queue(limit=20)

    recorded_decision_nvda = run_json(
        [
            "record-decision",
            "--target-case-id",
            "tc_nvda_ai_infra",
            "--decision-date",
            "2026-03-09",
            "--action-state",
            "prepare",
            "--confidence",
            "0.74",
            "--source-id",
            "src_sec_edgar",
            "--review-id",
            "review_ai_infra_2026_03_07",
            "--rationale",
            "AI infra thesis 已 active，NVDA 维持 prepare 跟踪，继续等财报与订单验证。",
        ]
    )
    recorded_decision_300308 = run_json(
        [
            "record-decision",
            "--target-case-id",
            "tc_300308_ai_infra",
            "--decision-date",
            "2026-03-09",
            "--action-state",
            "prepare",
            "--confidence",
            "0.71",
            "--source-id",
            "src_cninfo",
            "--review-id",
            "review_300308_2026_03_07",
            "--rationale",
            "A 股 AI infra 线已接官方公告，300308 先按 prepare 做正式跟踪。",
        ]
    )
    recorded_decision_kol_initial = run_json(
        [
            "record-decision",
            "--target-case-id",
            "tc_300308_kol_compute",
            "--decision-date",
            "2026-03-08",
            "--action-state",
            "observe",
            "--confidence",
            "0.44",
            "--source-id",
            "src_kol_fuzong",
            "--rationale",
            "KOL 国产算力线索刚补到一手证据前，先保守观察。",
        ]
    )
    recorded_decision_kol_current = run_json(
        [
            "record-decision",
            "--target-case-id",
            "tc_300308_kol_compute",
            "--decision-date",
            "2026-03-09",
            "--action-state",
            "prepare",
            "--confidence",
            "0.63",
            "--source-id",
            "src_kol_fuzong",
            "--source-id",
            "src_cninfo",
            "--review-id",
            "review_tc_300308_kol_compute_direct",
            "--rationale",
            "福总视频线索已补 direct review 和 CNINFO 锚点，升级到 prepare。",
        ]
    )
    recorded_decision_tsla = run_json(
        [
            "record-decision",
            "--target-case-id",
            "tc_tsla_embodied",
            "--decision-date",
            "2026-03-09",
            "--action-state",
            "observe",
            "--confidence",
            "0.56",
            "--source-id",
            "src_openalex",
            "--source-id",
            "src_sec_edgar",
            "--review-id",
            "review_embodied_2026_03_07",
            "--rationale",
            "具身智能 thesis 还成立，但 TSLA 更像观察位，不急于提动作。",
        ]
    )
    recorded_decision_aave = run_json(
        [
            "record-decision",
            "--target-case-id",
            "tc_aave_onchain",
            "--decision-date",
            "2026-03-09",
            "--action-state",
            "observe",
            "--confidence",
            "0.52",
            "--source-id",
            "src_defillama",
            "--source-id",
            "src_aave_governance",
            "--review-id",
            "review_onchain_2026_03_07",
            "--rationale",
            "链上金融 thesis 已 evidence_backed，但 AAVE 当前仍维持 observe。",
        ]
    )

    board = fetch_board()
    today_cockpit = fetch_today_cockpit()
    daily_refresh_skip_fetch = fetch_daily_refresh(skip_fetch=True, limit=10)
    daily_shortcut = fetch_daily_shortcut(skip_fetch=True, limit=10)
    daily_refresh_live = fetch_daily_refresh(limit=10)
    thesis_board = fetch_thesis_board()
    thesis_focus_ai_infra = fetch_thesis_focus("thesis_ai_infra")
    thesis_focus_kol = fetch_thesis_focus("thesis_kol_china_compute_seed")
    focus_shortcut_ai_infra = fetch_focus_shortcut("thesis_ai_infra", limit=20)
    voice_memo_triage_final = fetch_voice_memo_triage("art_voice_memo_framework")
    pattern_library_kol = fetch_pattern_library(thesis_id="thesis_kol_china_compute_seed", limit=20)
    theme_map = fetch_theme_map()
    watch_board = fetch_watch_board()
    target_case_dashboard = fetch_target_case_dashboard()
    decision_dashboard = fetch_decision_dashboard()
    decision_journal = fetch_decision_journal()
    decision_maintenance_queue = fetch_decision_maintenance_queue()
    review_remediation_queue = fetch_review_remediation_queue()
    intake_inbox = fetch_intake_inbox()
    review_board = fetch_review_board()
    playbook_board = fetch_playbook_board()
    source_board = fetch_source_board()
    source_track_record = fetch_source_track_record()
    route_normalization_queue_after = fetch_route_normalization_queue(limit=100)
    verification_remediation_batches_after = fetch_verification_remediation_batches()
    verification_remediation_queue_after = fetch_verification_remediation_queue()
    source_remediation_queue_after = fetch_source_remediation_queue()
    source_viewpoint_workbench_after_finish_line = fetch_source_viewpoint_workbench(include_existing=True, limit=20)
    validation_board = fetch_validation_board(verdict="validated")
    promotion_wizard = fetch_promotion_wizard()
    routing_board = fetch_routing_board()
    route_workbench_accepted_kol = fetch_route_workbench(status="accepted", thesis_id="thesis_kol_china_compute_seed")
    pending_corroboration_queue = fetch_corroboration_queue("pending")
    accepted_corroboration_queue = fetch_corroboration_queue("accepted")
    thesis_gate_report = fetch_thesis_gate_report()
    weekly_decision_note_path = SMOKE_ROOT / "exports" / "weekly_decision_note.md"
    weekly_decision_note = build_weekly_decision_note(weekly_decision_note_path)
    weekly_shortcut = fetch_weekly_shortcut(days=7, limit=8)
    conn = sqlite3.connect(SMOKE_ROOT / "state" / "finagent.sqlite")
    conn.row_factory = sqlite3.Row
    monitors = [
        dict(row)
        for row in conn.execute(
            "SELECT monitor_id, owner_object_id, metric_name, latest_value, status, query_or_rule FROM monitors ORDER BY monitor_id"
        ).fetchall()
    ]
    theses = [
        dict(row)
        for row in conn.execute("SELECT thesis_id, status, current_version_id FROM theses ORDER BY thesis_id").fetchall()
    ]
    claim_counts = {
        artifact_id: result["claim_count"]
        for artifact_id, result in extract_results.items()
    }
    conn.close()

    assert_true(board["sources"] == 7, f"expected 7 sources, got {board['sources']}")
    assert_true(board["artifacts"] == 8, f"expected 8 artifacts, got {board['artifacts']}")
    assert_true(board["claims"] == 55, f"expected 55 claims, got {board['claims']}")
    assert_true(board["themes"] == 4, f"expected 4 themes, got {board['themes']}")
    assert_true(board["theses"] == 4, f"expected 4 theses, got {board['theses']}")
    assert_true(board["targets"] == 4, f"expected 4 targets, got {board['targets']}")
    assert_true(board["target_cases"] == 5, f"expected 5 target cases, got {board['target_cases']}")
    assert_true(board["timing_plans"] == 5, f"expected 5 timing plans, got {board['timing_plans']}")
    assert_true(board["monitors"] == 5, f"expected 5 monitors, got {board['monitors']}")
    assert_true(board["reviews"] == 8, f"expected 8 reviews, got {board['reviews']}")
    assert_true(board["validation_cases"] == 12, f"expected 12 validation cases, got {board['validation_cases']}")
    assert_true(board["source_viewpoints"] == 2, f"expected 2 source viewpoints, got {board['source_viewpoints']}")
    assert_true(board["operator_decisions"] == 6, f"expected 6 operator decisions, got {board['operator_decisions']}")
    assert_true(all(count > 0 for count in claim_counts.values()), f"claim extraction failed: {claim_counts}")
    assert_true(all(result["route_count"] > 0 for result in routing_results.values()), f"claim routing failed: {routing_results}")
    assert_true(all(row["status"] == "alerted" for row in monitors), f"expected all monitors alerted, got {monitors}")

    thesis_status_map = {row["thesis_id"]: row["status"] for row in theses}
    assert_true(
        thesis_status_map == {
            "thesis_ai_infra": "active",
            "thesis_embodied": "active",
            "thesis_kol_china_compute_seed": "evidence_backed",
            "thesis_onchain_finance": "evidence_backed",
        },
        f"unexpected thesis statuses: {theses}",
    )

    assert_true(len(today_cockpit["focus"]["active_theses"]) >= 2, f"today cockpit missing active theses: {today_cockpit}")
    assert_true(len(today_cockpit["focus"]["alerted_monitors"]) == 5, f"today cockpit missing alerts: {today_cockpit}")
    assert_true(today_cockpit["focus"]["intake_summary"]["pending_routes"] == 0, f"intake queue should be empty: {today_cockpit}")
    assert_true(today_cockpit["focus"]["route_workbench_summary"]["route_items"] == 0, f"route workbench should be empty: {today_cockpit}")
    assert_true(today_cockpit["focus"]["route_normalization_summary"]["queue_items"] == 0, f"route normalization should be empty: {today_cockpit}")
    assert_true(today_cockpit["focus"]["verification_remediation_summary"]["queue_items"] == 0, f"verification remediation should be empty: {today_cockpit}")
    assert_true(today_cockpit["focus"]["verification_batch_summary"]["batch_items"] == 0, f"verification batches should be empty: {today_cockpit}")
    assert_true(today_cockpit["focus"]["review_remediation_summary"]["queue_items"] == 0, f"review remediation should be empty: {today_cockpit}")
    assert_true(today_cockpit["focus"]["source_remediation_summary"]["queue_items"] == 0, f"source remediation should be empty: {today_cockpit}")
    assert_true(today_cockpit["focus"]["source_revisit_workbench_summary"]["queue_items"] == 0, f"source revisit should be empty: {today_cockpit}")
    assert_true(today_cockpit["focus"]["source_viewpoint_workbench_summary"]["missing_viewpoints"] == 0, f"source viewpoint backlog should be empty: {today_cockpit}")
    assert_true(today_cockpit["focus"]["decision_summary"]["verification_queue"] == 0, f"decision summary verification queue should be zero: {today_cockpit}")
    assert_true(today_cockpit["focus"]["decision_journal_summary"]["decision_entries"] == 6, f"today cockpit decision journal mismatch: {today_cockpit}")
    assert_true(today_cockpit["focus"]["decision_journal_summary"]["drift_entries"] == 0, f"decision journal should have no drift: {today_cockpit}")
    assert_true(today_cockpit["focus"]["decision_maintenance_summary"]["queue_items"] == 0, f"decision maintenance should be empty after recording decisions: {today_cockpit}")
    assert_true(daily_refresh_skip_fetch["refresh_summary"]["refreshable_specs"] >= 5, f"daily refresh plan too small: {daily_refresh_skip_fetch}")
    assert_true(
        daily_refresh_skip_fetch["refresh_summary"]["fetch_skipped"] == daily_refresh_skip_fetch["refresh_summary"]["refreshable_specs"],
        f"daily refresh skip-fetch mismatch: {daily_refresh_skip_fetch}",
    )
    assert_true(daily_refresh_skip_fetch["monitor_results"]["monitor_count"] == 5, f"daily refresh monitor count mismatch: {daily_refresh_skip_fetch}")
    assert_true(
        len(daily_refresh_skip_fetch["today_cockpit"]["focus"]["active_theses"]) >= 2,
        f"daily refresh cockpit mismatch: {daily_refresh_skip_fetch}",
    )
    assert_true(
        daily_shortcut["refresh_summary"]["fetch_skipped"] == daily_refresh_skip_fetch["refresh_summary"]["fetch_skipped"],
        f"daily shortcut mismatch: {daily_shortcut}",
    )
    assert_true(daily_refresh_live["refresh_summary"]["fetch_success"] >= 4, f"daily refresh live should fetch refreshed artifacts: {daily_refresh_live}")
    refreshed_cninfo = next(
        (item for item in daily_refresh_live["fetch_results"] if item["status"] == "success" and item["source_id"] == "src_cninfo"),
        None,
    )
    assert_true(refreshed_cninfo is not None, f"daily refresh live missing CNINFO refresh result: {daily_refresh_live}")
    rebound_monitor = next((row for row in monitors if row["monitor_id"] == "mon_300308_kol_compute"), None)
    assert_true(rebound_monitor is not None, f"missing KOL monitor after refresh: {monitors}")
    rebound_rule = json.loads(rebound_monitor["query_or_rule"])
    assert_true(
        rebound_rule.get("artifact_id") == refreshed_cninfo["artifact_id"],
        f"daily refresh should rebind monitor to refreshed artifact: {rebound_monitor}",
    )

    assert_true(len(thesis_board["items"]) == 4, f"thesis board size mismatch: {thesis_board}")
    thesis_gate_map = {item["thesis_id"]: item["promotion_gate"] for item in thesis_board["items"]}
    assert_true(thesis_gate_map["thesis_ai_infra"]["can_recommend_active"] is True, f"ai infra gate mismatch: {thesis_board}")
    assert_true(
        thesis_gate_map["thesis_kol_china_compute_seed"]["can_recommend_active"] is False
        and "claim_provenance_complete" in thesis_gate_map["thesis_kol_china_compute_seed"]["active_missing"],
        f"kol gate mismatch: {thesis_board}",
    )
    assert_true(
        thesis_gate_map["thesis_onchain_finance"]["can_recommend_active"] is False
        and thesis_gate_map["thesis_onchain_finance"]["pending_corroboration_count"] == 0
        and "claim_provenance_complete" in thesis_gate_map["thesis_onchain_finance"]["active_missing"],
        f"on-chain gate mismatch after finish-line resolution: {thesis_board}",
    )
    assert_true(thesis_focus_ai_infra["summary"]["found"] is True, f"thesis focus should find thesis: {thesis_focus_ai_infra}")
    assert_true(thesis_focus_ai_infra["thesis"]["thesis_id"] == "thesis_ai_infra", f"thesis focus id mismatch: {thesis_focus_ai_infra}")
    assert_true(thesis_focus_ai_infra["provenance"]["source_count"] >= 1, f"thesis focus provenance mismatch: {thesis_focus_ai_infra}")
    assert_true(len(thesis_focus_ai_infra["target_cases"]) >= 1, f"thesis focus target cases missing: {thesis_focus_ai_infra}")
    assert_true(len(thesis_focus_ai_infra["decisions"]["active_entries"]) >= 1, f"thesis focus decisions missing: {thesis_focus_ai_infra}")
    assert_true(len(thesis_focus_ai_infra["timeline"]) >= 1, f"thesis focus timeline missing: {thesis_focus_ai_infra}")
    assert_true(
        any(item["source_confidence"] == "grounded" for item in thesis_focus_ai_infra["target_cases"]),
        f"thesis focus should expose grounded source confidence: {thesis_focus_ai_infra}",
    )
    assert_true(
        any((item.get("effective_review") or {}).get("freshness") == "fresh" for item in thesis_focus_ai_infra["target_cases"]),
        f"thesis focus should expose review freshness: {thesis_focus_ai_infra}",
    )
    assert_true(
        thesis_focus_kol["summary"]["matched_pattern_items"] >= 1,
        f"kol thesis focus should expose matched patterns: {thesis_focus_kol}",
    )
    assert_true(
        focus_shortcut_ai_infra["thesis"]["thesis_id"] == thesis_focus_ai_infra["thesis"]["thesis_id"],
        f"focus shortcut mismatch: {focus_shortcut_ai_infra}",
    )
    assert_true(voice_memo_triage_pending["summary"]["route_items"] >= 1, f"voice memo triage pending missing routes: {voice_memo_triage_pending}")
    assert_true(
        voice_memo_triage_pending["summary"]["matched_thesis_count"] >= 1,
        f"voice memo triage should match at least one existing thesis: {voice_memo_triage_pending}",
    )
    assert_true(
        any(
            item["triage_outcome"] == "low_signal" and "投资方法论" in item["claim_text"]
            for item in voice_memo_triage_pending["items"]
        ),
        f"voice memo triage should downgrade methodology claims: {voice_memo_triage_pending}",
    )
    assert_true(
        voice_memo_triage_pending["summary"]["low_signal_count"] >= 1,
        f"voice memo triage should classify some low-signal claims: {voice_memo_triage_pending}",
    )
    assert_true(
        voice_memo_triage_final["summary"]["route_items"] == 0,
        f"voice memo triage should ignore resolved routes at finish line: {voice_memo_triage_final}",
    )
    assert_true(pattern_library_kol["summary"]["pattern_items"] >= 1, f"pattern library should return matched items: {pattern_library_kol}")
    assert_true(
        weekly_shortcut["summary"]["decision_items"] == weekly_decision_note["summary"]["decision_items"],
        f"weekly shortcut mismatch: {weekly_shortcut}",
    )

    assert_true(len(theme_map["items"]) == 4, f"theme map size mismatch: {theme_map}")
    assert_true(any(item["theme_id"] == "theme_ai_infra" and item["thesis_count"] >= 1 for item in theme_map["items"]), f"theme map missing ai infra: {theme_map}")

    assert_true(watch_board["summary"]["watch_items"] == 5, f"watch board size mismatch: {watch_board}")
    assert_true(watch_board["summary"]["alerted_watch_items"] == 5, f"watch board alerts mismatch: {watch_board}")

    assert_true(target_case_dashboard["summary"]["target_case_items"] == 5, f"target case dashboard mismatch: {target_case_dashboard}")
    assert_true(target_case_dashboard["summary"]["ready_items"] == 5, f"target case readiness mismatch: {target_case_dashboard}")
    assert_true(target_case_dashboard["summary"]["missing_effective_review"] == 0, f"target case effective review mismatch: {target_case_dashboard}")
    assert_true(target_case_dashboard["summary"]["fallback_review_items"] == 0, f"fallback review should be cleared: {target_case_dashboard}")

    assert_true(decision_dashboard["summary"]["decision_items"] == 5, f"decision dashboard size mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["priority_actions"] == 3, f"decision dashboard priority action mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["observe_actions"] == 2, f"decision dashboard observe action mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["verification_queue"] == 0, f"decision dashboard verification queue should be zero: {decision_dashboard}")
    assert_true(
        thesis_focus_kol["thesis"]["title"] == "KOL video seed on China compute chain",
        f"KOL thesis title should remain seed/tracking scoped when active gate is blocked: {thesis_focus_kol}",
    )
    assert_true(
        "还不能直接提升为 active thesis" in (thesis_focus_kol["thesis"]["version"]["statement"] or ""),
        f"KOL thesis statement should keep seed semantics until active gate passes: {thesis_focus_kol}",
    )
    assert_true(decision_dashboard["summary"]["grounded_source_items"] == 5, f"decision dashboard grounded count mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["fragile_source_items"] == 0, f"decision dashboard fragile count mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["fresh_review_items"] == 5, f"decision dashboard fresh review mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["source_guard_items"] == 0, f"decision dashboard source guard should be empty: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["source_viewpoints"] == 2, f"decision dashboard source viewpoint mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["recorded_decision_items"] == 5, f"decision dashboard recorded decision mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["aligned_recorded_decisions"] == 5, f"decision dashboard alignment mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["drifted_recorded_decisions"] == 0, f"decision dashboard drift mismatch: {decision_dashboard}")
    assert_true(decision_dashboard["summary"]["missing_recorded_decisions"] == 0, f"decision dashboard missing decision mismatch: {decision_dashboard}")

    fuzong_focus = next(item for item in decision_dashboard["source_focus"] if item["source_id"] == "src_kol_fuzong")
    defillama_focus = next(item for item in decision_dashboard["source_focus"] if item["source_id"] == "src_defillama")
    assert_true(
        fuzong_focus["accepted_route_count"] == 10
        and fuzong_focus["pending_route_count"] == 0
        and fuzong_focus["latest_viewpoint_status"] == "partially_validated",
        f"kOL source focus mismatch after finish-line resolution: {decision_dashboard}",
    )
    assert_true(
        defillama_focus["accepted_route_count"] == 2
        and defillama_focus["pending_route_count"] == 0
        and defillama_focus["latest_viewpoint_status"] == "partially_validated",
        f"DefiLlama source focus mismatch after finish-line resolution: {decision_dashboard}",
    )
    assert_true(any(item["target"]["ticker_or_symbol"] == "300308.SZ" and item["action_state"] == "prepare" for item in decision_dashboard["priority_actions"]), f"decision dashboard missing prepare action for 300308.SZ: {decision_dashboard}")
    assert_true(any(item["target"]["ticker_or_symbol"] == "AAVE" and item["action_state"] == "observe" for item in decision_dashboard["observe_actions"]), f"decision dashboard missing observe action for AAVE: {decision_dashboard}")
    assert_true(all(item["recorded_decision_state"] == "aligned" for item in decision_dashboard["priority_actions"] + decision_dashboard["observe_actions"]), f"decision dashboard recorded decisions should align: {decision_dashboard}")

    assert_true(decision_journal["summary"]["decision_entries"] == 6, f"decision journal size mismatch: {decision_journal}")
    assert_true(decision_journal["summary"]["active_entries"] == 5, f"decision journal active entry mismatch: {decision_journal}")
    assert_true(decision_journal["summary"]["superseded_entries"] == 1, f"decision journal superseded mismatch: {decision_journal}")
    assert_true(decision_journal["summary"]["aligned_entries"] == 5, f"decision journal aligned mismatch: {decision_journal}")
    assert_true(decision_journal["summary"]["drift_entries"] == 0, f"decision journal drift mismatch: {decision_journal}")
    assert_true(any(item["target_case_id"] == "tc_300308_kol_compute" and item["status"] == "superseded" for item in decision_journal["items"]), f"decision journal should preserve superseded entry: {decision_journal}")
    assert_true(any(item["target_case_id"] == "tc_300308_kol_compute" and item["status"] == "active" and item["action_state"] == "prepare" for item in decision_journal["items"]), f"decision journal should preserve active replacement entry: {decision_journal}")
    assert_true(decision_maintenance_queue_before["summary"]["queue_items"] == 5, f"decision maintenance should start with five missing decisions: {decision_maintenance_queue_before}")
    assert_true(decision_maintenance_queue["summary"]["queue_items"] == 0, f"decision maintenance should be empty after recording decisions: {decision_maintenance_queue}")

    target_case_review_map = {
        item["target_case_id"]: (item["review_freshness"], item["effective_review_source"], item["effective_review_gap_type"])
        for item in target_case_dashboard["items"]
    }
    decision_review_map = {
        item["target_case_id"]: (item["review_freshness"], item["effective_review_source"], item["effective_review_gap_type"])
        for item in decision_dashboard["priority_actions"] + decision_dashboard["observe_actions"]
    }
    assert_true(target_case_review_map == decision_review_map, f"review contract mismatch across surfaces: {target_case_dashboard} {decision_dashboard}")

    assert_true(intake_inbox["summary"]["pending_routes"] == 0, f"intake inbox should be empty: {intake_inbox}")
    assert_true(review_board["summary"]["review_items"] == 8, f"review board mismatch: {review_board}")
    assert_true(review_board["summary"]["due_reviews"] == 8, f"review due count mismatch: {review_board}")
    assert_true(playbook_board["summary"]["lesson_patterns"] == 5, f"playbook board mismatch: {playbook_board}")

    assert_true(source_board["summary"]["source_items"] == 7, f"source board mismatch: {source_board}")
    assert_true(source_board["summary"]["sources_with_viewpoints"] == 2, f"source board viewpoint coverage mismatch: {source_board}")
    assert_true(source_board["summary"]["sources_with_feedback"] == 2, f"source board feedback coverage mismatch: {source_board}")
    fuzong_source = next(item for item in source_board["items"] if item["source_id"] == "src_kol_fuzong")
    defillama_source = next(item for item in source_board["items"] if item["source_id"] == "src_defillama")
    sec_source = next(item for item in source_board["items"] if item["source_id"] == "src_sec_edgar")
    cninfo_source = next(item for item in source_board["items"] if item["source_id"] == "src_cninfo")
    openalex_source = next(item for item in source_board["items"] if item["source_id"] == "src_openalex")
    aave_governance_source = next(item for item in source_board["items"] if item["source_id"] == "src_aave_governance")
    assert_true(
        fuzong_source["accepted_route_count"] == 10
        and fuzong_source["pending_route_count"] == 0
        and fuzong_source["source_display_label"] == "high_priority"
        and fuzong_source["latest_viewpoint_status"] == "partially_validated",
        f"fuzong source board mismatch: {source_board}",
    )
    assert_true(
        defillama_source["accepted_route_count"] == 2
        and defillama_source["pending_route_count"] == 0
        and defillama_source["source_display_label"] == "watch"
        and defillama_source["latest_viewpoint_status"] == "partially_validated",
        f"defillama source board mismatch: {source_board}",
    )
    assert_true(sec_source["source_display_label"] == "anchor", f"SEC semantics mismatch: {source_board}")
    assert_true(cninfo_source["source_display_label"] == "anchor", f"CNINFO semantics mismatch: {source_board}")
    assert_true(openalex_source["source_display_label"] == "reference", f"OpenAlex semantics mismatch: {source_board}")
    assert_true(aave_governance_source["source_display_label"] == "anchor", f"Aave governance semantics mismatch: {source_board}")

    fuzong_track = next(item for item in source_track_record["items"] if item["source_id"] == "src_kol_fuzong")
    defillama_track = next(item for item in source_track_record["items"] if item["source_id"] == "src_defillama")
    assert_true(source_track_record["summary"]["track_record_items"] == 7, f"source track record mismatch: {source_track_record}")
    assert_true(
        fuzong_track["track_record_score"] == 4
        and fuzong_track["source_priority_label"] == "high_priority"
        and fuzong_track["feedback_freshness"] == "fresh",
        f"fuzong track record mismatch: {source_track_record}",
    )
    assert_true(
        defillama_track["track_record_score"] == 3
        and defillama_track["source_priority_label"] == "watch"
        and defillama_track["feedback_freshness"] == "fresh",
        f"defillama track record mismatch: {source_track_record}",
    )

    assert_true(bulk_apply_result["applied_count"] == 4, f"bulk route apply mismatch: {bulk_apply_result}")
    assert_true(source_feedback_result["weight"] == 3, f"source feedback weight mismatch: {source_feedback_result}")
    assert_true(source_feedback_old_result["weight"] == 1, f"stale source feedback mismatch: {source_feedback_old_result}")
    assert_true(source_feedback_refresh_defillama["weight"] == 1, f"refresh source feedback mismatch: {source_feedback_refresh_defillama}")
    assert_true(source_feedback_stale_defillama["weight"] == 1, f"stale defillama feedback mismatch: {source_feedback_stale_defillama}")
    assert_true(source_viewpoint_result["status"] == "partially_validated", f"fuzong source viewpoint mismatch: {source_viewpoint_result}")
    assert_true(route_apply_result["validation_case_id"] is not None and cninfo_artifact_id in route_apply_result["attached_artifact_ids"], f"route apply mismatch: {route_apply_result}")
    assert_true(onchain_source_remediation["action"] == "attach_first_hand_artifact", f"source remediation action mismatch: {onchain_source_remediation}")
    assert_true(promote_to_evidence["to_status"] == "evidence_backed", f"ai infra evidence promotion failed: {promote_to_evidence}")
    assert_true(promote_to_active["to_status"] == "active", f"ai infra active promotion failed: {promote_to_active}")
    assert_true(promote_kol_to_evidence["to_status"] == "evidence_backed", f"kol evidence promotion failed: {promote_kol_to_evidence}")
    assert_true(
        promote_kol_to_active_after_batch.get("skipped") is True
        and "claim_provenance_complete" in promote_kol_to_active_after_batch["gate"]["active_missing"],
        f"kol active promotion should stay blocked: {promote_kol_to_active_after_batch}",
    )
    assert_true(promote_onchain_to_evidence["to_status"] == "evidence_backed", f"on-chain evidence promotion failed: {promote_onchain_to_evidence}")

    assert_true(source_viewpoint_workbench_finish_line_before["summary"]["missing_viewpoints"] >= 1, f"finish-line should start with missing viewpoints: {source_viewpoint_workbench_finish_line_before}")
    assert_true(len(finish_line_source_viewpoints) >= 1, f"finish-line should synthesize at least one extra viewpoint: {finish_line_source_viewpoints}")
    assert_true(verification_remediation_batches_finish_line_before["summary"]["batch_items"] >= 1, f"finish-line should start with verification batches: {verification_remediation_batches_finish_line_before}")
    assert_true(len(finish_line_verification_batch_resolutions) >= 1, f"finish-line should resolve verification batches: {finish_line_verification_batch_resolutions}")
    assert_true(review_remediation_queue_finish_line_before["summary"]["queue_items"] >= 1, f"finish-line should start with review remediation items: {review_remediation_queue_finish_line_before}")
    assert_true(len(finish_line_review_resolutions) == review_remediation_queue_finish_line_before["summary"]["queue_items"], f"finish-line review remediation mismatch: {finish_line_review_resolutions}")
    assert_true(route_normalization_queue_before["summary"]["queue_items"] >= 1, f"finish-line should start with route normalization backlog: {route_normalization_queue_before}")
    assert_true(len(finish_line_route_normalization) == route_normalization_queue_before["summary"]["batch_items"], f"finish-line route normalization mismatch: {finish_line_route_normalization}")
    assert_true(route_normalization_queue_after["summary"]["queue_items"] == 0, f"route normalization should be empty after finish-line run: {route_normalization_queue_after}")
    assert_true(verification_remediation_batches_after["summary"]["batch_items"] == 0, f"verification batches should be empty after finish-line run: {verification_remediation_batches_after}")
    assert_true(verification_remediation_queue_after["summary"]["queue_items"] == 0, f"verification remediation should be empty after finish-line run: {verification_remediation_queue_after}")
    assert_true(review_remediation_queue["summary"]["queue_items"] == 0, f"review remediation should be empty after finish-line run: {review_remediation_queue}")
    assert_true(source_remediation_queue_after["summary"]["queue_items"] == 0, f"source remediation should be empty after finish-line run: {source_remediation_queue_after}")
    assert_true(source_viewpoint_workbench_after_finish_line["summary"]["missing_viewpoints"] == 0, f"source viewpoint backlog should be empty after finish-line run: {source_viewpoint_workbench_after_finish_line}")
    assert_true(fetch_route_workbench(status="pending", limit=100)["summary"]["pending_route_count"] == 0, "pending route backlog should be zero at finish line")

    gate_by_thesis = {item["thesis_id"]: item for item in thesis_gate_report["items"]}
    assert_true(
        gate_by_thesis["thesis_kol_china_compute_seed"]["can_recommend_active"] is False
        and "claim_provenance_complete" in gate_by_thesis["thesis_kol_china_compute_seed"]["active_missing"],
        f"kol thesis gate mismatch: {thesis_gate_report}",
    )
    assert_true(
        gate_by_thesis["thesis_onchain_finance"]["pending_corroboration_count"] == 0
        and gate_by_thesis["thesis_onchain_finance"]["can_recommend_active"] is False
        and "claim_provenance_complete" in gate_by_thesis["thesis_onchain_finance"]["active_missing"],
        f"on-chain thesis gate mismatch: {thesis_gate_report}",
    )
    assert_true(validation_board["summary"]["validation_items"] == 12, f"validation board mismatch: {validation_board}")

    assert_true(weekly_decision_note_path.exists(), f"weekly decision note not written: {weekly_decision_note}")
    markdown = weekly_decision_note["markdown"]
    assert_true("## Source Tracking" in markdown and "福总 机构一手调研" in markdown and "DefiLlama" in markdown, f"weekly note missing source tracking: {weekly_decision_note}")
    assert_true("## Validation Changes" in markdown and "其余 `" in markdown and "条 validation 已写入 ledger" in markdown, f"weekly note validation compaction mismatch: {weekly_decision_note}")
    assert_true("## Decision Actions" in markdown and "## Observe / Do Not Rush" in markdown, f"weekly note action sections missing: {weekly_decision_note}")
    assert_true("## Recorded Decisions" in markdown and "`status=active`" in markdown and "`alignment=aligned`" in markdown, f"weekly note decision journal mismatch: {weekly_decision_note}")
    assert_true("## System Health" in markdown and "decision maintenance=0" in markdown and "verification queue=0" in markdown and "review remediation=0" in markdown, f"weekly note system health mismatch: {weekly_decision_note}")
    assert_true("## Decision Maintenance" not in markdown and "## Verification Queue" not in markdown and "## Verification Remediation Queue" not in markdown and "## Verification Batch Opportunities" not in markdown and "## Review Remediation Queue" not in markdown, f"weekly note should not show empty operator sections: {weekly_decision_note}")
    assert_true("trust=anchor" in markdown and "trust=reference" in markdown and "priority=high_priority" in markdown, f"weekly note source semantics mismatch: {weekly_decision_note}")
    assert_true("operator=prepare" in markdown and "operator=observe" in markdown, f"weekly note should expose recorded operator states: {weekly_decision_note}")

    return {
        "ok": True,
        "root": str(SMOKE_ROOT),
        "board": board,
        "artifacts": {
            "voice_memo": voice_artifact_id,
            "video_digest": video_artifact_id,
            "sec_nvda": nvda_artifact_id,
            "sec_tsla": tsla_artifact_id,
            "cninfo_300308": cninfo_artifact_id,
            "openalex": openalex_artifact_id,
            "defillama": aave_artifact_id,
            "aave_governance": aave_governance_artifact["artifact_id"],
        },
        "claim_counts": claim_counts,
        "monitors": monitors,
        "theses": theses,
        "monitor_run": monitor_run,
        "routing_results": routing_results,
        "route_apply_result": route_apply_result,
        "routing_board": routing_board,
        "route_workbench_pending": route_workbench_pending,
        "route_workbench_kol_pending": route_workbench_kol_pending,
        "route_workbench_accepted_kol": route_workbench_accepted_kol,
        "pending_corroboration_queue": pending_corroboration_queue,
        "accepted_corroboration_queue": accepted_corroboration_queue,
        "thesis_gate_report": thesis_gate_report,
        "today_cockpit": today_cockpit,
        "daily_refresh_skip_fetch": daily_refresh_skip_fetch,
        "daily_refresh_live": daily_refresh_live,
        "daily_shortcut": daily_shortcut,
        "thesis_board": thesis_board,
        "thesis_focus_ai_infra": thesis_focus_ai_infra,
        "thesis_focus_kol": thesis_focus_kol,
        "focus_shortcut_ai_infra": focus_shortcut_ai_infra,
        "voice_memo_triage_pending": voice_memo_triage_pending,
        "voice_memo_triage_final": voice_memo_triage_final,
        "pattern_library_kol": pattern_library_kol,
        "theme_map": theme_map,
        "watch_board": watch_board,
        "target_case_dashboard": target_case_dashboard,
        "decision_dashboard": decision_dashboard,
        "decision_journal": decision_journal,
        "decision_maintenance_queue": decision_maintenance_queue,
        "review_remediation_queue": review_remediation_queue,
        "intake_inbox": intake_inbox,
        "review_board": review_board,
        "playbook_board": playbook_board,
        "source_board": source_board,
        "source_track_record": source_track_record,
        "decision_dashboard_before_source_remediation": decision_dashboard_before_source_remediation,
        "source_remediation_queue_before": source_remediation_queue_before,
        "source_remediation_queue_after": source_remediation_queue_after,
        "aave_governance_artifact": aave_governance_artifact,
        "onchain_source_remediation": onchain_source_remediation,
        "decision_dashboard_before_verification_remediation": decision_dashboard_before_verification_remediation,
        "verification_remediation_queue_before": verification_remediation_queue_before,
        "verification_remediation_batches_before": verification_remediation_batches_before,
        "verification_remediation_batches_after": verification_remediation_batches_after,
        "verification_remediation_queue_after": verification_remediation_queue_after,
        "thesis_gate_report_before_verification_remediation": thesis_gate_report_before_verification_remediation,
        "promotion_wizard_after_verification_resolution": promotion_wizard_after_verification_resolution,
        "promotion_wizard_after_kol_verification_batch": promotion_wizard_after_kol_verification_batch,
        "onchain_verification_item": onchain_verification_item,
        "onchain_corroboration_resolution": onchain_corroboration_resolution,
        "promote_onchain_to_evidence": promote_onchain_to_evidence,
        "kol_verification_batch": kol_verification_batch,
        "kol_bulk_verification_resolution": kol_bulk_verification_resolution,
        "thesis_gate_report_after_kol_verification_batch": thesis_gate_report_after_kol_verification_batch,
        "promote_kol_to_active_after_batch": promote_kol_to_active_after_batch,
        "decision_dashboard_before_review_remediation": decision_dashboard_before_review_remediation,
        "target_case_dashboard_before_review_remediation": target_case_dashboard_before_review_remediation,
        "review_remediation_queue_before": review_remediation_queue_before,
        "direct_review_kol_target": direct_review_kol_target,
        "extracted_pattern_kol_review": extracted_pattern_kol_review,
        "source_revisit_workbench_before": source_revisit_workbench_before,
        "source_revisit_workbench_after": source_revisit_workbench_after,
        "source_feedback_workbench_before": source_feedback_workbench_before,
        "source_feedback_workbench_after": source_feedback_workbench_after,
        "source_viewpoint_workbench_before": source_viewpoint_workbench_before,
        "source_viewpoint_workbench_after": source_viewpoint_workbench_after,
        "source_viewpoint_workbench_existing": source_viewpoint_workbench_existing,
        "decision_maintenance_queue_before": decision_maintenance_queue_before,
        "validation_board": validation_board,
        "source_viewpoint_result": source_viewpoint_result,
        "source_feedback_result": source_feedback_result,
        "source_feedback_old_result": source_feedback_old_result,
        "source_feedback_stale_defillama": source_feedback_stale_defillama,
        "source_feedback_refresh_defillama": source_feedback_refresh_defillama,
        "promotion_wizard_before": promotion_wizard_before,
        "promotion_wizard_after_remediation": promotion_wizard_after_remediation,
        "promotion_wizard_mid": promotion_wizard_mid,
        "promotion_wizard": promotion_wizard,
        "bulk_apply_result": bulk_apply_result,
        "kol_remediation": kol_remediation,
        "promote_to_evidence": promote_to_evidence,
        "promote_to_active": promote_to_active,
        "promote_kol_to_evidence": promote_kol_to_evidence,
        "monitor_run_after_remediation": monitor_run_after_remediation,
        "recorded_decision_nvda": recorded_decision_nvda,
        "recorded_decision_300308": recorded_decision_300308,
        "recorded_decision_kol_initial": recorded_decision_kol_initial,
        "recorded_decision_kol_current": recorded_decision_kol_current,
        "recorded_decision_tsla": recorded_decision_tsla,
        "recorded_decision_aave": recorded_decision_aave,
        "weekly_decision_note": weekly_decision_note,
        "weekly_shortcut": weekly_shortcut,
    }


def main() -> int:
    result = create_cases()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
