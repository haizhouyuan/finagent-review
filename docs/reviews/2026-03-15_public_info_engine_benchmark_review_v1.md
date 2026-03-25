# Public-Info Engine Benchmark Review v1

> 日期: 2026-03-15
> 主题: `finagent` 对标 `Qlib / OpenBB / QuantConnect LEAN / FinRL`
> 结论来源:
> - ChatGPT Pro: `/vol1/1000/projects/ChatgptREST/artifacts/jobs/07b115871e7745348c35736238b709d7/answer.md`
> - Gemini DeepThink: `/vol1/1000/projects/ChatgptREST/artifacts/jobs/19ac1e7d80554429a82f68ba6f872543/answer.md`
> - 官方参考:
>   - Qlib workflow / task management: `https://qlib.readthedocs.io/en/stable/component/workflow.html`, `https://qlib.readthedocs.io/en/stable/advanced/task_management.html`
>   - QuantConnect LEAN Algorithm Framework: `https://www.quantconnect.com/docs/v2/writing-algorithms/algorithm-framework/overview`
>   - OpenBB provider / data model extension: `https://docs.openbb.co/platform/developer_guide/extension_platform/build_extension`
>   - FinRL architecture / three-layer DataOps: `https://finrl.readthedocs.io/en/latest/start/three_layer/environments.html`

## 1. 总结判断

`finagent` 不该模仿这些项目的“产品形状”，而应该有选择地借它们的“能力模式”。

最可靠的 donor 排序是：

1. `Qlib`
   最适合借的是 experiment discipline、workflow / task / recorder、可回放研究运行面。
2. `OpenBB`
   最适合借的是 typed provider contract、标准化数据网关、统一查询接口。
3. `LEAN`
   最适合借的是 stage boundary、event-driven separation、模块职责清晰。
4. `FinRL`
   最适合借的是 DataOps / leakage hygiene / train-validate-deploy 分层。

不应该复制的是：

- `LEAN` 的 tick-level / live trading engine
- `FinRL` 的 end-to-end black-box RL 代理
- `OpenBB Terminal` 的同步终端交互心智
- `Qlib` 的量价 alpha/factor 主体

一句话:

**要借的是 discipline、typed interface、event separation、evaluation hygiene；不要借的是 high-frequency trading baggage 和 black-box trading policy。**

## 2. 双模型共识

两边对 `finagent` 的共识都很强：

- 系统应继续坚持 `append-only event ledger -> state projection -> candidate` 主线
- 应把“扫描/抽取”和“资本判断”严格分开
- 应把“已知 watch 执行”和“未知机会发现”拆成两个平面
- 事件应该优先于文档，状态应该优先于摘要
- 需要更强的 source typing、state grammar、run recording、evaluation discipline

两边的分工重点不同：

- `ChatGPT Pro` 更强调项目对标、该借什么/不该借什么、实施阶段划分
- `Gemini DeepThink` 更强调 stateful public-information engine、residualization、state transition 和记账式架构

因此，这轮吸收不是“二选一”，而是：

- 用 `ChatGPT Pro` 纠偏产品方向
- 用 `Gemini` 收紧事件/状态运行面

## 3. 当前项目最该吸收的能力

### 3.1 Qlib: experiment / workflow / recorder discipline

Qlib 最值得借的是：

- 任务/工作流组织
- recorder/experiment 管理
- 可回放研究运行与比较
- 统一的 research lifecycle

对 `finagent` 的直接启发是：

- 主题运行、KOL 运行、LLM 提取都应该落成统一 `analysis_runs`
- 需要 `run board` 和 `run compare`
- 需要把 `theme suite / kol suite / extraction run` 放到一个统一 registry，而不是零散目录

### 3.2 OpenBB: typed provider contract

OpenBB 最值得借的是：

- 统一 provider 模型
- 数据接入层和应用层解耦
- 标准化对象契约

对 `finagent` 的直接启发是：

- source role 不能再只是字符串枚举，必须带 authority / interrupt / corroboration 语义
- adapter family 要进入 contract
- future source adapter 必须对齐统一 source policy，而不是每个脚本自己决定“算不算证据”

### 3.3 LEAN: event separation and stage boundaries

LEAN 最值得借的是：

- 明确的事件驱动心智
- 模块职责分离
- 不把所有逻辑揉进一个大策略函数

对 `finagent` 的直接启发是：

- `sector grammar` 应成为正式 contract
- 不同行业的 commercialization progression 不能共用模糊情绪词
- stage / trigger / route / projection 之间要保持窄接口

### 3.4 FinRL: leakage discipline

FinRL 不适合借的是 RL 交易代理本身。

真正适合借的是：

- dataops discipline
- leakage prevention
- train / validate / deploy 分离

对 `finagent` 的直接启发是：

- event extraction 不能只靠 prompt 存在，必须可校验、可记录、可比较
- run metadata、schema version、golden fixtures、route validation 都应该保留
- 未来要把 extractor / validator / router 拆开看，而不是“跑出结果就算对”

## 4. 这轮实际已经做出的 benchmark 改造

本轮不是只写 benchmark 评论，已经把一部分设计吸收到代码里。

### 4.1 Typed source policy

新增:

- `finagent/source_policy.py`

作用:

- 把 `company_filing / regulator / customer_signal / competitor_pr / conference / patent / media / hiring / kol_digest`
  收成 typed contract
- 每个 role 都带：
  - `source_tier`
  - `adapter_family`
  - `state_authority`
  - `interrupt_eligible`
  - `needs_corroboration`
  - `discovery_only`

这一步主要来自 `OpenBB` 的 provider contract 思路。

### 4.2 Sector grammar registry

新增:

- `finagent/sector_grammars.py`

作用:

- 把电力设备、AI 能源、商业航天、硅光、内存 bifurcation 的 grammar key 正式化
- 每个 grammar 都带：
  - `stage_focus`
  - `proving_cues`
  - `constraint_cues`

这一步主要来自 `LEAN` 的 stage boundary 思路 + 双模型都强调的 sector-specific grammar。

### 4.3 Run recorder / compare

新增:

- `finagent/event_runs.py`
- CLI:
  - `event-run-board`
  - `event-run-compare`

作用:

- 把 theme suite / KOL suite / ChatgptREST extraction 统一写入 `analysis_runs`
- 为 benchmark 和回归建立 Qlib-style recorder 面

这一步主要来自 `Qlib`。

### 4.4 Prompt context hardening

改动:

- `sentinel.build_spec_prompt_context`
- `event_extraction.sentinel_context_from_spec`
- `build_extraction_prompt`

作用:

- prompt 里不再只是 sentinel 上下文
- 还显式注入：
  - Source Policy Snapshot
  - Sector Grammar Hints

这使 extractor 更接近 typed extraction，而不是裸 prompt 摘要器。

## 5. 仍然不能做的事

即使 benchmark 做完，当前也不该误判成：

- 已经拥有 OpenBB 那种数据源广度
- 已经拥有 LEAN 那种全量事件引擎
- 已经拥有 Qlib 那种完备实验工厂
- 已经拥有 FinRL 那种训练/评估流水线

现在更准确的说法是：

**finagent 已经开始具备这些项目的“纪律性骨架”，但还没有它们各自的“规模化基础设施”。**

## 6. 下一阶段最优先做什么

不是继续扩 ontology，而是把这轮 benchmark 学到的 discipline 变成更稳定的运行面：

1. live extraction success path
   - 用真实 ChatgptREST extraction job 跑通
2. source adapter family
   - 先补 filing / newsroom / conference / patent / hiring
3. run registry surfaces
   - 把 event run board 真正用于 benchmark / rerun / regression
4. source/grammar introspection
   - 让 operator 直接看到哪些 source 可以改 state，哪些 grammar 在驱动当前主题

## 7. 最终裁定

这一轮 benchmark 的正确结论不是“去模仿一个成熟量化平台”，而是：

**把 `finagent` 继续做成一个以公开信息事件为中心、以 typed contract 为边界、以 run recorder 为纪律、以 sector grammar 为解释器的研究系统。**

这和传统量化平台有交集，但不是它们的缩小版。
