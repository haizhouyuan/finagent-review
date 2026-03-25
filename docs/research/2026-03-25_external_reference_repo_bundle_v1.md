# External Reference Repo Bundle v1

> 日期: 2026-03-25
> 目的: 把跨市场投研 OS 相关的开源参考仓库统一落到 `finagent/external_research_repos/`，后续做架构对标、源码阅读和模块拆借时直接按本地路径进入。

## 1. 本地参考仓库总表

统一目录:

- `/vol1/1000/projects/finagent/external_research_repos/`

当前已落地:

| 项目 | 本地路径 | 主要定位 |
|------|----------|----------|
| OpenBB | `/vol1/1000/projects/finagent/external_research_repos/OpenBB` | 跨市场金融数据平台 / 数据总线 |
| Qlib | `/vol1/1000/projects/finagent/external_research_repos/qlib` | AI-first 量化研究平台 |
| FinRobot | `/vol1/1000/projects/finagent/external_research_repos/FinRobot` | 金融多智能体协作平台 |
| FinGPT | `/vol1/1000/projects/finagent/external_research_repos/FinGPT` | 金融文本 / 金融 RAG / 金融 LLM 生态 |
| FinRL | `/vol1/1000/projects/finagent/external_research_repos/FinRL` | 端到端量化交易 / RL 研究框架 |
| TradingAgents | `/vol1/1000/projects/finagent/external_research_repos/TradingAgents` | 单标的多智能体交易评审器 |
| TradingAgents-CN | `/vol1/1000/projects/finagent/external_research_repos/TradingAgents-CN` | TradingAgents 中文化与本土化分支 |
| LEAN | `/vol1/1000/projects/finagent/external_research_repos/Lean` | 事件驱动回测 / 交易执行引擎 |

## 2. 你本地已经有的 OpenBB 痕迹

这次不是第一次引入 OpenBB。本机已经有两套与 OpenBB 相关的现成资产:

- `finagent` venv:
  - `/vol1/1000/projects/finagent/.venv/bin/openbb`
  - `/vol1/1000/projects/finagent/.venv/bin/openbb-api`
  - `/vol1/1000/projects/finagent/.venv/bin/openbb-mcp`
- `planning` 独立 venv:
  - `/vol1/1000/projects/planning/.venv-openbb/`

另外，`finagent` 自己已经写过一层 OpenBB 统一适配:

- `/vol1/1000/projects/finagent/finagent/openbb_adapter.py`
- `/vol1/1000/projects/finagent/finagent/openbb_mcp_server.py`

也就是说，OpenBB 在你的体系里并不是“从零开始的新候选”，而是已经有:

1. 本地运行环境
2. `finagent` 统一 adapter
3. MCP 暴露层

## 3. 当前最值得重点对读的映射

如果目标是做“跨市场投研 OS”，这几套仓库建议按下面方式看，而不是平铺地看。

### 3.1 数据总线 / 数据标准化

- OpenBB

重点看:

- provider contract
- 统一 data model
- MCP / Python API 入口

对应 `finagent` 最值得借的能力:

- typed adapter contract
- 标准化 `RawDocument / MarketSnapshot / Filing / News` 中间层

### 3.2 研究纪律 / replay / leakage guard

- Qlib
- LEAN
- FinRL

重点看:

- workflow / recorder / replay
- event-driven runtime
- strict time discipline / leakage guard

对应 `finagent` 最值得借的能力:

- run metrics
- replay
- evidence date guard

### 3.3 多智能体协作 / 后置评审

- FinRobot
- TradingAgents
- TradingAgents-CN

重点看:

- role design
- debate / review loop
- 单主题 / 单标的的多 agent 协作输出

对应 `finagent` / `OpenClaw finbot` 最值得借的能力:

- committee review
- candidate deepen 后的 bull/bear/risk second opinion

### 3.4 金融文本 / 金融 RAG / 非结构化信息处理

- FinGPT

重点看:

- 金融文本数据管线
- 金融 RAG / 金融 search agent
- 金融 claim 提取与问答

对应 `finagent` 最值得借的能力:

- 金融文本标准化 ingestion
- 金融 RAG 的引用纪律

## 4. 当前判断

这些仓库更适合被拆成“参考件”，而不是直接替代 `finagent`。

更准确的角色分工应该是:

- OpenBB: 数据总线参考件
- Qlib / LEAN / FinRL: 研究纪律与验证参考件
- FinRobot / TradingAgents: 多智能体评审参考件
- FinGPT: 金融文本 / RAG 参考件

而 `finagent` / `OpenClaw finbot` 仍然应该承担:

- discovery loop
- event / claim / chronology
- thesis evolution
- candidate surfacing
- opportunity deepen

## 5. 建议的后续研究顺序

### 第一轮

先看:

1. OpenBB
2. Qlib
3. TradingAgents

目标:

- 数据总线怎么抽象
- run / replay / metrics 怎么立
- 后置 committee review 怎么接

### 第二轮

再看:

1. FinRobot
2. FinGPT
3. FinRL
4. LEAN

目标:

- 多 agent workflow
- 金融文本 / RAG
- DataOps / leakage guard
- 事件驱动 runtime discipline

## 6. 备注

- 这次优先保证“本地可研究副本”齐全，因此部分仓库使用 tarball 快照方式落地，而不是完整 git history clone。
- 如果后面某个项目要做长期跟踪，再单独把它切成完整 git clone 或加上 upstream remote 即可。
