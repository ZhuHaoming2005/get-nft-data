# Dedup Benchmark HTML Analysis Report Design

## Goal

新增一个仅面向当前 `dedup_bench_rs/results/result.json` 结果格式的 Python 分析工具，生成单文件 `HTML` 可视化报告，重点回答 3 个问题：

1. `name` / `metadata` 查重的时间成本差异
2. `name` / `metadata` 查重的效果差异
3. 在“按合约级命中”口径下，`name` 查重是否覆盖 `metadata` 查重，是否还有必要保留 `metadata` 查重

## Current State

- `dedup_bench_rs` 已能输出：
  - `reference`
  - `name_algorithms`
  - `metadata_algorithms`
- `name_algorithms[*].duplicates` 已是合约级结果，元素包含：
  - `contract_address`
  - `max_score`
  - `duplicate_token_count`
- `metadata_algorithms[*].duplicates` 仍是 token 级结果，元素包含：
  - `contract_address`
  - `token_id`
  - `metadata_doc`
  - `score`
- 当前仓库中没有针对该 benchmark JSON 的专用可视化分析工具。
- 本次分析使用的是上一版脚本结果，因此工具只围绕当前这份 `result.json` 的实际字段实现，不为后续 JSON 结构变化做兼容。

## Required Analysis Semantics

### Contract-Level Success Rule

- 本次查重效果统计统一使用“合约级命中”口径。
- `name` 算法：
  - 直接使用 `duplicates[].contract_address` 集合作为该算法命中的重复合约集合。
- `metadata` 算法：
  - 先将 token 级 `duplicates` 按 `contract_address` 去重折叠。
  - 只要某个合约下任意一个 token 被命中，即视为该合约被该 `metadata` 算法成功查重。

### Coverage Baseline

- “`name` 是否覆盖 `metadata`”只以 `metadata_algorithms` 为对照基准。
- 不以 `reference` 作为覆盖率或必要性判断的基准。

### Coverage Granularity

- 只做逐算法配对分析。
- 对每一对 `(name_algorithm, metadata_algorithm)` 单独计算覆盖率和缺口。
- 不额外计算“所有 `name` 算法并集覆盖 `metadata`”的体系级指标。

### Recommendation Rule

- 报告需要自动生成“是否还有必要保留 `metadata` 查重”的结论。
- 结论必须基于具体数值证据，而不是固定模板判断。

## Desired State

新增一个 Python CLI 工具，默认面向当前 `dedup_bench_rs/results/result.json`，输出一个单文件 `HTML` 报告。该报告应：

- 直接展示样本和输入文件概况
- 对所有 `name` / `metadata` 算法展示耗时对比
- 对所有 `name` / `metadata` 算法展示合约级查重效果对比
- 对所有 `(name, metadata)` 算法配对展示覆盖率和未覆盖合约数
- 自动生成基于当前数据的结论，说明：
  - 哪些 `metadata` 算法明显补充了 `name`
  - 哪些 `metadata` 算法增益有限但成本较高

## Architecture

工具建议放在 `dedup_bench_rs/py_report` 子目录下，不修改 `dedup_bench_rs` Rust 逻辑。

### Module Layout

- `dedup_bench_rs/py_report/dedup_bench_report/__init__.py`
  - 包初始化与版本占位
- `dedup_bench_rs/py_report/dedup_bench_report/models.py`
  - 定义输入结果结构、标准化算法指标、覆盖分析结果、结论数据结构
- `dedup_bench_rs/py_report/dedup_bench_report/analyzer.py`
  - 负责按当前 `result.json` 的字段直接读取 JSON、折叠 `metadata` 到合约级、构建指标和覆盖矩阵
- `dedup_bench_rs/py_report/dedup_bench_report/render_html.py`
  - 负责生成单文件 `HTML`，内嵌 Plotly 图表和摘要文本
- `dedup_bench_rs/py_report/dedup_bench_report/cli.py`
  - 命令行入口，串联读取、分析、渲染和写文件

## Input Model

### Expected Top-Level Fields

工具直接依赖当前 `result.json` 中这些顶层字段：

- `chain`
- `source`
- `sample`
- `recall_elapsed_ms`
- `recall_candidate_count`
- `reference`
- `name_algorithms`
- `metadata_algorithms`

### Algorithm Result Normalization

对 `name` 和 `metadata` 算法统一抽象为标准化结果对象，至少包含：

- `algorithm_id`
- `group`
  - `name` 或 `metadata`
- `decision_rule`
- `repeat`
- `runs_ms`
- `avg_ms`
- `min_ms`
- `contract_hits`
  - 合约级命中集合
- `contract_hit_count`
  - 合约级命中数

其中：

- `name` 的 `contract_hits` 直接来自原始 `duplicates[].contract_address`
- `metadata` 的 `contract_hits` 来自 token 级 `duplicates` 去重后的 `contract_address` 集合

不为字段缺失、字段改名、结果版本升级引入兼容层；若当前 JSON 结构变化，直接修改工具实现。

## Metrics

### 1. Time Cost Metrics

对每个算法保留：

- `avg_ms`
- `min_ms`
- `repeat`
- `runs_ms`

主比较指标使用 `avg_ms`，原因是：

- 与“时间成本”语义最直接对应
- 避免 `min_ms` 过度乐观

### 2. Dedup Effectiveness Metrics

对每个算法计算：

- `contract_hit_count`
- `contract_hits`

报告中“查重效果”默认以 `contract_hit_count` 展示，不展示 NFT/token 级命中数作为主指标，避免被大合约放大。

### 3. Pairwise Coverage Metrics

对每一对 `(name_algorithm, metadata_algorithm)` 计算：

- `name_contract_count`
- `metadata_contract_count`
- `intersection_contract_count`
- `coverage_rate`
  - `intersection_contract_count / metadata_contract_count`
- `metadata_only_contract_count`
  - `metadata_contract_count - intersection_contract_count`
- `metadata_only_contracts`
  - 作为明细表或可折叠文本展示的合约集合

若某个 `metadata` 算法 `metadata_contract_count == 0`：

- `coverage_rate` 设为 `null`
- 报告展示为 `N/A`
- 不把它算作 `name` 覆盖能力强的证据

### 4. Recommendation Evidence

自动结论至少基于这些证据：

- 每个 `metadata` 算法被各 `name` 算法覆盖的最高覆盖率
- 每个 `metadata` 算法仍保留的最小缺口
  - 即在所有 `name` 算法中，`metadata_only_contract_count` 的最小值
- 各 `metadata` 算法的时间成本
- 各 `name` 算法的时间成本

结论原则：

- 若某个 `metadata` 算法在所有 `name` 算法配对下都存在明显 `metadata_only_contract_count`，则说明它补充了 `name` 无法覆盖的合约，应倾向“仍有必要保留 `metadata` 查重”。
- 若某个 `metadata` 算法被某个 `name` 算法高比例覆盖，且其耗时显著更高，则应指出其边际收益有限。
- 结论应允许同时给出：
  - “总体上 metadata 仍有价值”
  - “但个别 metadata 算法性价比较低”

## Report Structure

报告生成单文件 `HTML`，所有图表资源内嵌，不依赖外部 CDN。

### Section 1: Summary

展示：

- 输入文件路径
- `chain`
- 样本 `contract_address`
- 样本 `name`
- `recall_candidate_count`
- `recall_elapsed_ms`
- 最快算法
- 命中合约数最多算法
- 自动结论摘要

### Section 2: Time Cost

展示：

- `name` 算法 `avg_ms` 横向条形图
- `metadata` 算法 `avg_ms` 横向条形图
- 一张明细表，列出：
  - `algorithm_id`
  - `group`
  - `avg_ms`
  - `min_ms`
  - `repeat`

### Section 3: Dedup Effectiveness

展示：

- 所有算法的 `contract_hit_count` 对比图
- 文本说明：
  - `metadata` 的命中数已按合约级去重折叠
  - “发现合约涉及重复即算成功”

### Section 4: Name-vs-Metadata Coverage

展示：

- pairwise coverage heatmap
  - 行：`name` 算法
  - 列：`metadata` 算法
  - 值：`coverage_rate`
- pairwise gap heatmap 或表格
  - 值：`metadata_only_contract_count`
- 一张配对明细表，列出：
  - `name_algorithm`
  - `metadata_algorithm`
  - `coverage_rate`
  - `intersection_contract_count`
  - `metadata_only_contract_count`

### Section 5: Recommendation

自动生成 2-4 段高信号结论，包括：

- 对整体是否保留 `metadata` 查重的判断
- 支持该判断的关键数字
- 若存在性价比较低的 `metadata` 算法，明确点名
- 若某些 `name` 算法比其他 `name` 算法明显更接近覆盖 `metadata`，明确点名

## Rendering Strategy

### Library Choice

优先使用：

- `pandas`
- `plotly`

原因：

- `pandas` 适合做算法指标、矩阵和表格整理
- `plotly` 适合输出单文件、可交互、易读的 HTML 图表

### Single-File HTML

- 使用 Plotly 的内嵌脚本模式生成自包含图表。
- 最终输出为一个独立 `report.html`，用户双击即可查看。
- 不生成额外 `assets/` 目录。

### Styling

- 样式保持简洁，以分析可读性优先。
- 页面不追求营销型视觉设计。
- 使用少量自定义 CSS 做：
  - 摘要卡片
  - 指标说明块
  - 表格可读性优化

## CLI

初始 CLI 只需要支持最小参数集：

- `--input`
  - benchmark JSON 路径
- `--output`
  - 输出 HTML 路径
- `--title`
  - 可选，自定义报告标题

示例：

```bash
cd dedup_bench_rs\py_report
C:\Users\z1766\.conda\envs\codex\python.exe -m dedup_bench_report.cli ^
  --input ..\results\result.json ^
  --output ..\results\result_analysis.html
```

## Testing Strategy

### Analyzer Tests

新增 `tests/dedup_bench_report/test_analyzer.py`，至少覆盖：

- `metadata` token 级结果能正确折叠为合约级集合
- `contract_hit_count` 计算正确
- pairwise `coverage_rate` 正确
- pairwise `metadata_only_contract_count` 正确
- 当 `metadata_contract_count == 0` 时返回 `N/A` 语义

### Rendering Tests

新增 `tests/dedup_bench_report/test_render_html.py`，至少覆盖：

- HTML 包含标题和 5 个主要 section 标题
- HTML 中包含 Plotly 图表容器或关键脚本片段
- HTML 中包含自动结论文本块
- HTML 明确说明“按合约级命中统计”

### Smoke Validation

实现完成后，应使用当前样例：

- 输入：`dedup_bench_rs/results/result.json`
- 输出：`dedup_bench_rs/results/result_analysis.html`

验证：

- 脚本能成功运行
- 生成单文件 HTML
- HTML 可打开
- 关键图表和结论存在

## Non-Goals

- 不修改 `dedup_bench_rs` Rust benchmark 输出逻辑
- 不重新计算任何 name 或 metadata 分数
- 不分析 `reference` 的覆盖率
- 不深入到 NFT/token 级效果评估作为主结论
- 不支持未来未知格式的 benchmark JSON 变体
- 不把当前工具抽象成通用 benchmark viewer
