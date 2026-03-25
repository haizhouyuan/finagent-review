# finagent

本目录是一个**独立的投研工作区**（代码 + 文档 + 产物），用于把“投研相关的长期资产”从 `planning/`、`codexread/` 等工程里解耦出来，便于：

- 单独配置 OpenClaw 的 `finagent`（独立 workspace / 记忆索引范围 / 并行 spawn 编排）。
- 将投研产物（结论、证据链、watchlist、决策包、复盘记录）集中归档与迭代。
- 对历史项目（例如 `codexread/`）做一次性盘点后，沉淀出可复用的“高信噪比入口”。

## 目录约定

- `docs/`：规则、入口、说明、盘点报告、runbook（建议可提交）。
- `archives/`：长期维护的投研档案（建议可提交）。
- `scripts/`：自动化脚本（建议可提交）。
- `imports/`：原始输入（不建议入 git；大文件/敏感内容放这里）。
- `state/`：运行态/缓存/中间产物（不建议入 git）。

## codexread 盘点产物

本轮对 `codexread/` 的“第一轮资产盘点”会落在：

- `docs/codexread/`

这些文件的目标是让后续 `finagent` **不需要重新扫全仓**，只要索引这份盘点结果 + 少量高信噪比目录，就能建立全貌理解与可检索入口。

## 当前已跑通

- `Home PC FunASR` 远端转写已接入 `finagent` CLI：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  transcribe-homepc-funasr \
  --audio-path /abs/path/to/audio.wav \
  --device cpu
```

- 更上层的语音备忘录 intake 也已接好：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  intake-voice-memo-audio \
  --audio-path /abs/path/to/audio.wav \
  --device cpu
```

- `KOL/video digest -> artifact -> claims` 也有正式入口：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  intake-kol-digest \
  --path /abs/path/to/digest.md \
  --source-name '福总 机构一手调研' \
  --speaker '福总'
```

- 当前默认策略：
  - 远端主机：`yuanhaizhou@192.168.1.17`
  - 远端环境：`soulxpodcast`
  - 模型缓存：`/home/yuanhaizhou/funasr_models`
  - 默认走 `cpu`，避免和 Home PC 上的 vLLM 双卡常驻冲突

## 当前新增的工作面

当前 CLI 已能输出第一批 derived views：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  today-cockpit

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  thesis-board

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  theme-map

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  watch-board

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  target-case-dashboard

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  intake-inbox

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  review-board

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  review-remediation-queue \
  --limit 20

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  playbook-board

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  source-board

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  promotion-wizard
```

这些视图对应蓝图里的第一批前台：

- `Today / Research Cockpit`
- `Decision Dashboard / Weekly Decision Note`
- `Thesis Board`
- `Theme Map / Industry Framework`
- `Timing / Watch Board`
- `TargetCase Dashboard`
- `Intake Inbox`
- `Review / Playbook Board`
- `Source / KOL track-record skeleton`

最新补上的日常入口：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  thesis-focus \
  --thesis-id thesis_ai_infra

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  voice-memo-triage \
  --artifact-id art_voice_memo_framework

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  pattern-library \
  --thesis-id thesis_kol_china_compute_seed

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  daily-refresh \
  --skip-fetch
```

以及更短的 workflow shortcuts：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  daily \
  --skip-fetch

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  weekly \
  --format markdown \
  --out docs/runs/latest_weekly_note.md

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  focus \
  --thesis-id thesis_ai_infra
```

给外部 orchestrator / OpenClaw / OpenMind 用的紧凑快照入口：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  integration-snapshot \
  --scope today \
  --limit 6

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  integration-snapshot \
  --scope thesis \
  --thesis-id memory-bifurcation \
  --limit 6
```

新增的 operator-facing 决策输出层：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  decision-dashboard

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  validation-board \
  --verdict validated

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  source-track-record \
  --limit 20

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  source-feedback-workbench \
  --limit 20

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  source-revisit-workbench \
  --limit 20

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  record-source-feedback \
  --source-id src_kol_fuzong \
  --source-viewpoint-id svp_xxx \
  --validation-case-id val_xxx \
  --feedback-type high_signal \
  --created-at 2026-03-09T00:00:00+00:00 \
  --note "这条来源观点已形成高价值验证。"

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  source-viewpoint-workbench \
  --limit 20

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  route-normalization-queue \
  --limit 40

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  synthesize-source-viewpoint \
  --source-id src_kol_fuzong \
  --artifact-id art_video_fuzong_latest_digest \
  --thesis-id thesis_kol_china_compute_seed

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  weekly-decision-note \
  --out docs/runs/latest_weekly_decision_note.md

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  record-decision \
  --target-case-id tc_300308_ai_infra \
  --action-state prepare \
  --confidence 0.72 \
  --source-id src_cninfo \
  --review-id review_300308_2026_03_07 \
  --rationale "公告链路已经跑通，先按 prepare 跟踪。"

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  decision-journal \
  --days 30 \
  --limit 20

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  decision-maintenance-queue \
  --days 30 \
  --limit 20
```

这层的目标不是多做一个摘要页，而是直接回答：

- 本周大神 / 来源说了什么
- 哪些观点已经被一手证据落地
- 对应哪些标的
- 当前更像 `observe / prepare / starter / add / trim / exit` 的哪一种动作
- 最近一次人工/operator 实际记录了什么动作
- 哪些 target case 还没有记录动作，或者记录动作已经漂移
- 下周最该继续验证什么

phase9 起，这层开始引入 `ValidationCase` 语义：

- `validation-board` 可直接查看哪些 claim 已经形成 `validated / contradicted / partial / needs_followup`
- `second_hand / kol` 的 gate 和 `source_focus` 会优先识别已落库的 validation case，而不是只看 route 是否被 accepted

phase10 起，这层开始引入 `SourceViewpoint` 语义：

- `create-source-viewpoint` 用来把“大神这次到底表达了什么观点”固化成可跟踪对象
- `source-track-record` 用来查看某个来源的 `open / partially_validated / validated / contradicted` 观点滚动结果
- `weekly-decision-note` 的 `Source Tracking` 优先展示最近的观点摘要和当前验证状态，而不是只看 artifact 标题

phase11 起，这层开始引入 operator throughput 工具：

- `source-viewpoint-workbench` 用来查看哪些来源/artifact/thesis 已经具备 validation 语义，但还没被归并成观点对象
- `synthesize-source-viewpoint` 用来从 `validation cases + pending corroboration` 自动生成一个稳定的 `SourceViewpoint`
- `today-cockpit` 会显示这个 workbench 的剩余候选，便于把“大神观点跟踪”真正做成收件箱工作流

phase13 起，这层统一了 `effective_review` 合同：

- `Decision Dashboard`
- `TargetCase Dashboard`
- `Weekly Decision Note`

现在都读取同一个 target-case 级 `effective_review`，并显式区分：

- `target_case_review`
- `thesis_fallback`
- `none`

phase14 起，这层开始把 review debt 前台化：

- `review-remediation-queue` 会区分 `blocking` 和 `backfill`
- `today-cockpit` 会直接暴露 `review_remediation_summary`
- `weekly-decision-note` 会单列 `Review Remediation Queue`

phase15 起，这层开始补 `source track record` 的 operator-grounded feedback：

- `record-source-feedback` 用来把人工判断沉淀成来源反馈
- `source-feedback-workbench` 用来列出已经具备 validation / viewpoint 语义、但还没补 feedback 的来源观点
- `source-revisit-workbench` 用来列出 feedback 已 aging / stale、需要重新确认优先级的来源
- revisit item 会带 `suggested_feedback_type / suggested_note / feedback_recipe`
- 反馈类型当前支持：
  - `high_signal`
  - `useful_context`
  - `noise`
  - `misleading`
- 来源反馈默认带时效衰减：
  - `fresh`
  - `aging`
  - `stale`
- `source-track-record` 不再只看 viewpoint/validation 启发式，还会额外暴露：
  - `operator_feedback_score`
  - `effective_operator_feedback_score`
  - `source_priority_score`
  - `source_priority_label`
- phase20 起，`decision-dashboard / weekly-decision-note` 会把来源可信度直接压进决策项：
  - `source_confidence`
  - `source_confidence_reason`
  - `needs_source_revisit`
  - `linked_sources`
  - `Source Guard`
- phase21 起，来源侧风险不只展示，还会落成 `source-remediation-queue`：
  - `attach_first_hand_artifact`
  - `refresh_source_feedback`
  - `today-cockpit` / `weekly-decision-note` 都会直接展示这些 operator next actions
- phase22 起，这条 queue 已经有前后态闭环：
  - 例如 `AAVE` 可以先因 `DefiLlama + Voice Memo` 被判成 `fragile`
  - 补入 `Aave Governance` 一手锚点后，会自动从 `Source Guard / Source Remediation Queue` 消失
- phase23 起，`review-remediation-queue` 也有了闭环验证：
  - `tc_300308_kol_compute` 会先因缺 direct review 进入 `blocking`
  - 补完 `target_case review` 后，会从 `blocking` 消失
  - 同时 `decision-dashboard / target-case-dashboard / weekly-decision-note` 会统一切到 `fresh / target_case_review`
- phase24 起，`verification-remediation-queue` 也有了闭环验证：
  - 当 second-hand claim 已挂到唯一 thesis，且 thesis 已具备一手锚点时，会给出 `accept_corroboration_with_evidence`
  - 例如 `DefiLlama -> thesis_onchain_finance` 会建议用 `art_aave_governance_anchor` 完成 accepted corroboration
  - 完成后该 route 会从 `Verification Remediation Queue` 消失，`validation_cases` 增加，`has_corroborated_first_hand` 清零
  - 对应 thesis 会从 `framed` 升到 `evidence_backed`
  - `today-cockpit / weekly-decision-note / thesis-gate-report` 会同步刷新
- phase25 起，`verification-remediation-batches` 也上线了：
  - 会把“同来源 / 同 thesis / 同一手锚点”的 remediation item 收敛成 batch
  - batch recipe 改为精确的 `apply-route-batch --route-id ...`，不再用范围过宽的 `bulk-apply-routes --source-id --thesis-id`
  - `weekly-decision-note` 会新增 `Verification Batch Opportunities`
  - `today-cockpit` 会新增 `verification_batch_summary / verification_batch_items`
  - KOL 样例里，福总 backlog 可以一次性把 9 条高信号 corroboration 收敛，并把 `thesis_kol_china_compute_seed` 从 `evidence_backed` 推到 `active`
- phase27 起，`route-normalization-queue` 也上线了：
  - 它不做新判断，只负责把“已被当前系统吸收但还挂在 pending 的 route backlog”收掉
  - 一手 `thesis_input` 会被分组为 `accept_first_hand_input`
  - 基础语音 memo 的 `thesis_seed` 会被分组为 `supersede_foundational_seed`
  - 低信号二手/KOL 残余 corroboration 会被分组为 `supersede_low_signal_corroboration`
  - `today-cockpit` 会新增 `route_normalization_summary / route_normalization_batches`
- phase28 起，source semantics 进一步收紧：
  - 一手来源和 operator 追踪分数彻底拆开
  - `SEC / CNINFO / Aave Governance` 这类来源在 `source-board / decision-dashboard / weekly-decision-note` 中显示为 `trust=anchor`
  - `OpenAlex` 这类研究参考来源显示为 `trust=reference`
  - `福总 / DefiLlama` 这类二手来源继续显示 `priority=high_priority/watch`
- phase29 起，`weekly-decision-note` 会自动压缩低信号段落：
  - `Validation Changes` 只保留高信号 delta，并把剩余条目聚合成一句统计
  - 当 operator 队列为零时，不再刷一长串“当前没有...”空 section
  - 用统一 `System Health` 段汇总 `verification / review / source` 队列健康状态
- phase30 起，`scripts/live_cases_smoke.py` 已升级为 finish-line smoke：
  - 会从干净 root 重建 live cases
  - 自动补齐缺失 `SourceViewpoint`
  - 自动吃掉剩余 verification batch、review remediation、route normalization backlog
  - 终态要求 `decision / route / source` 三层队列全部清零

## Claim Routing

当前已经有第一版 `ClaimRoute`：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  route-claims \
  --artifact-id art_xxx

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  routing-board

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  route-workbench \
  --status pending
```

当前默认路由规则：

- `personal -> thesis_seed`
- `kol / second_hand -> corroboration_needed`
- `first_hand -> thesis_input`

进一步的生命周期命令：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  set-route-status \
  --route-id route_xxx \
  --status accepted

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  set-route-status-batch \
  --route-id route_xxx \
  --route-id route_yyy \
  --status superseded \
  --note "superseded low-signal corroboration backlog"

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  apply-route \
  --route-id route_xxx \
  --link-object-type thesis \
  --link-object-id thesis_xxx \
  --evidence-artifact-id art_first_hand_xxx

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  bulk-apply-routes \
  --source-id src_xxx \
  --route-type thesis_input \
  --link-object-type thesis \
  --link-object-id thesis_xxx

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  corroboration-queue

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  thesis-gate-report
```

`apply-route` 的作用：

- 把某条 route 正式接受并挂到 `thesis / thesis_version / target_case / artifact`
- 为 `corroboration_needed` 保存一手证据 artifact 链接
- 如果挂到 thesis，会把相关 artifact 追加进 `thesis_version.created_from_artifacts_json`
- 让 `KOL/video -> corroborated evidence -> thesis gate` 真正形成闭环
- `bulk-apply-routes` 适合把同一 artifact 下一批 `thesis_input` 或 `corroboration_needed` 一次性处理掉，避免 daily use 被细粒度 route 拖慢

如果 gate 缺的是“补一手证据 / 增加 invalidator / 建 target case + timing + monitor”，现在也有可执行补救入口：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  remediate-thesis \
  --thesis-id thesis_xxx \
  --action attach_first_hand_artifact \
  --artifact-id art_first_hand_xxx

PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  remediate-thesis \
  --thesis-id thesis_xxx \
  --action create_target_case \
  --target-id target_xxx \
  --target-case-id tc_xxx \
  --exposure-type proxy \
  --window-type 6_12_months \
  --desired-posture prepare \
  --catalyst 'official filings' \
  --confirmation-signal 'recent_filing_count>=1' \
  --monitor-artifact-id art_first_hand_xxx \
  --monitor-metric-name recent_filing_count \
  --monitor-comparator gte \
  --monitor-threshold 1
```

如果 gate 已经满足，可以继续用：

```bash
PYTHONPATH=/vol1/1000/projects/finagent python3 -m finagent.cli \
  --root /vol1/1000/projects/finagent \
  promote-thesis \
  --thesis-id thesis_xxx \
  --new-status evidence_backed
```

`promotion-wizard` 和 `promote-thesis` 的目标是把：

- `gate diagnostics`
- `下一步应该做什么`
- `人工确认后的状态推进`

接成一条可执行链，而不是只停在“诊断报告”。

当前第二阶段的语义约束也已经收紧：

- `second_hand / kol` thesis 可以在完成一手 corroboration 后升到 `evidence_backed`
- 但如果 `pending_corroboration_count` 仍然很高，`promotion-wizard` 不会继续推荐 `active`
- 也就是说，`evidence_backed` 表示“已被一手证据落地”，`active` 表示“证据落地且核验债务已进入可控范围”

## 最新业务 Smoke

`scripts/live_cases_smoke.py` 现在已经覆盖：

- `Voice memo -> thesis_seed`
- `KOL digest -> corroboration_needed -> attach first-hand -> create target case -> promote thesis`
- `SEC / CNINFO / OpenAlex / DefiLlama -> thesis_input / monitoring`
- `Today Cockpit / Route Workbench / Thesis Board / Watch Board / Review Board / Playbook Board`

最新结果见：

- `docs/runs/2026-03-08_live_cases_smoke_phase6.json`
- `docs/runs/2026-03-08_phase6_route_workbench_and_remediation.md`
- `docs/runs/2026-03-08_live_cases_smoke_phase7.json`
- `docs/runs/2026-03-08_phase7_evidence_semantics_tightening.md`

下一阶段会继续在这条真实 smoke 上验证：

- `decision-dashboard`
- `weekly-decision-note`
