# Event Mining Benchmark Refactor Walkthrough v1

> 日期: 2026-03-15
> 分支: `feat/event-mining-quality-pass`
> 目的: 记录这轮 “Qlib / OpenBB / LEAN / FinRL + 双模型 benchmark 吸收” 的实际工程落地

## 1. 为什么做这轮

用户要的不是再写一篇“我们应该学谁”的评论，而是：

- 去官方项目取经
- 用双模型再想一轮
- 把结论真正压进 `finagent`
- 最后以 PR 形式提交

这轮的关键判断是：

**不要模仿这些项目的产品形状，而是借它们最适合 `public-information event engine` 的能力模式。**

## 2. 这轮吸收了什么

### 2.1 来自 Qlib

- 研究运行不应该散落在目录里
- 需要统一 recorder / compare / replay discipline

对应代码:

- `finagent/event_runs.py`
- `event-run-board`
- `event-run-compare`

### 2.2 来自 OpenBB

- source/provider contract 要 typed
- adapter family 不能再隐藏在脚本里

对应代码:

- `finagent/source_policy.py`
- `event-source-policy`

### 2.3 来自 LEAN

- stage boundary 和模块职责必须清晰
- 不同行业需要 sector-specific grammar

对应代码:

- `finagent/sector_grammars.py`
- `event-sector-grammars`
- `validate_sentinel_spec` grammar 校验

### 2.4 来自 FinRL

- 不借 RL black-box
- 只借 DataOps / validation hygiene

对应代码:

- schema / prompt / test / run registry 的一致性强化

## 3. 实际改了什么

### 3.1 新文件

- `finagent/source_policy.py`
- `finagent/sector_grammars.py`
- `finagent/event_runs.py`
- `tests/test_event_runs.py`
- `tests/test_benchmark_contracts.py`

### 3.2 关键改动

- `finagent/sentinel.py`
  - source policy prompt lines
  - grammar prompt lines
  - grammar key 校验
  - `build_spec_prompt_context`
- `finagent/event_extraction.py`
  - extraction context 直接复用 sentinel prompt context
- `finagent/cli.py`
  - 新增:
    - `event-run-board`
    - `event-run-compare`
    - `event-source-policy`
    - `event-sector-grammars`
- `scripts/run_event_mining_theme_suite.py`
  - 写入 run registry
- `scripts/run_event_mining_kol_suite.py`
  - 写入 run registry
- `scripts/run_event_extraction_chatgptrest.py`
  - 写入 run registry
- `finagent/theme_report.py`
  - expression card 带 grammar/source policy 摘要

## 4. 验证

跑过：

```bash
cd /vol1/1000/projects/finagent
PYTHONPATH=. pytest -q \
  tests/test_event_runs.py \
  tests/test_event_extraction.py \
  tests/test_sentinel.py \
  tests/test_theme_report.py \
  tests/test_benchmark_contracts.py
```

结果：

- `27 passed`

命令面验证：

```bash
python3 -m finagent.cli event-source-policy
python3 -m finagent.cli event-sector-grammars
python3 -m finagent.cli event-run-board --limit 10
python3 -m finagent.cli event-run-compare --run-id <id1> --run-id <id2>
```

额外 runtime 验证：

```bash
python3 scripts/run_event_mining_theme_suite.py \
  --theme-slug transformer_benchmark_v2 \
  --run-root artifacts/theme_runs/2026-03-15_transformer_benchmark_v2 \
  --spec specs/theme_runs/2026-03-14_transformer_sentinel_v2.yaml \
  --events imports/theme_runs/2026-03-14_transformer_events_v2.json \
  --as-of 2026-03-15T16:00:00+08:00

python3 scripts/run_event_mining_kol_suite.py \
  --run-root artifacts/kol_runs/2026-03-15_kol_signal_benchmark_v2 \
  --suite-slug kol_signal_benchmark_v2
```

结果：

- `transformer_benchmark_v2`
  - `analysis_run_id = run_event_mining_theme_suite_transfo_19c0670fdd`
  - `recommended_posture = watch_with_prepare_candidate`
  - `best_expression = sntl_xidian_alt`
  - `imported = 6`
- `kol_signal_benchmark_v2`
  - `analysis_run_id = run_event_mining_kol_suite_kol_signa_d780808073`
  - `ingested_sources = 5`
  - `total_claims = 77`
  - `pending_routes = 0`

之后再跑：

```bash
python3 -m finagent.cli --root /vol1/1000/projects/finagent event-run-board --limit 6
```

可以在 repo-root registry 里同时看到：

- `transformer_benchmark_v2`
- `kol_signal_benchmark_v2`
- 之前的 `silicon_photonics_benchmark_v1`

说明 recorder / compare plane 不只是单测通过，而是真正进入了主题运行面。

## 5. 这轮没有做的事

我明确没有做：

- Kafka / Redis Streams
- 全量 provider adapter SDK
- RL / backtest framework
- tick/time-slice runtime
- 大规模 graph database 重构

因为这些属于“模仿平台”，不是“吸收适合当前产品的 discipline”。

## 6. 最终结论

这轮 benchmark refactor 最重要的结果，不是多了几个文件，而是把三条长期缺失的 contract 补上了：

1. `source contract`
2. `sector grammar contract`
3. `run recorder contract`

这三条 contract 让 `finagent` 更像一个真正的研究系统，而不是一组脚本和主题文档。
