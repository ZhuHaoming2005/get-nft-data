# Dedup Benchmark Duplicates-Only Design

> Note
>
> 这份设计记录了最初的 duplicates-only 方向。后续实现已进一步收缩正式报告形态：
> - 不再保留单独的 `reference` 报告块
> - 正式报告只保留 `name_algorithms` 和 `metadata_algorithms`
> - `name` 结果按合约聚类输出

## Goal

将 `dedup_bench_rs` 从“输出各算法 top_k 候选”改为“只输出各算法判定为重复的结果”。每个算法必须有独立、可配置、可解释的判重标准，所有标准集中在单独文件中维护。

## Current State

- 普通算法当前路径是：
  1. 对 `recall_rows` 逐行打分
  2. 过滤 `score > 0`
  3. 按分数排序
  4. 截取 `top_k`
- `reference` 当前路径是：
  1. 计算 `name_score` 与 `metadata_score`
  2. 按 `DEFAULT_NAME_THRESHOLD` / `DEFAULT_METADATA_THRESHOLD` 判断是否命中
  3. 按 `combined_score` 排序
  4. 截取 `top_k`
- 正式报告同时输出 JSON 和 Markdown，核心展示字段是 `candidate_count` 与 `top_candidates`。

## Desired State

### Formal Output Contract

- 正式报告中不再出现 `top_k` 概念。
- 正式报告中不再出现“排序后的候选列表”。
- 每个算法只输出被其判定为重复的 `duplicates`。
- `duplicates` 的输出顺序可以保留稳定排序以便可读，但排序不再是业务语义的一部分，也不再称为 top-k。

### Algorithm Decision Rules

- 为每个算法定义一个明确的判重规则。
- 规则必须集中定义在单独文件中，后续调整阈值时不需要改业务聚合逻辑。
- 普通算法默认采用“分数阈值”规则。
- `reference` 采用显式规则定义，而不是散落在算法实现中的硬编码判断。

### Report Semantics

- `candidate_count` 不再表示“分数大于 0 的候选数”。
- 报告中应显式区分：
  - `recall_candidate_count`: 召回阶段进入算法的总候选数
  - `duplicate_count`: 某算法最终判定为重复的数量
- 对每个算法，正式报告只展示：
  - 算法标识
  - 判重规则说明
  - `duplicate_count`
  - 耗时统计
  - `duplicates`

## Architecture

### 1. New Rules Module

新增独立规则模块，例如：

- `dedup_bench_rs/src/decision_rules.rs`

职责：

- 定义每个算法的判重规则常量
- 提供按 `algorithm_id` 查询规则的统一接口
- 提供格式化后的规则说明字符串，供报告输出

建议结构：

- `DuplicateDecisionRule`
- `ReferenceDecisionRule`
- `algorithm_duplicate_rule(algorithm_id: &str) -> ...`

普通算法规则至少包含：

- `algorithm_id`
- `duplicate_threshold`
- `score_scale_hint`

`reference` 规则至少包含：

- `name_threshold`
- `metadata_threshold`
- `combined_score_field_for_display`

### 2. Replace Top-K Aggregation with Duplicate Filtering

普通算法路径改为：

1. 计算所有 `scores`
2. 根据对应算法规则过滤出 duplicates
3. 输出全部 duplicates

这里仍可以保留稳定排序，仅用于：

- 让 JSON/Markdown 输出稳定
- 让测试结果稳定

但不再接受 `top_k`，也不做截断。

`reference` 路径改为：

1. 计算 `name_score` / `metadata_score`
2. 读取规则模块中的 `reference` 判重阈值
3. 输出全部满足规则的 duplicates

### 3. Report Model Changes

数据结构从“候选”切换为“重复项”：

- `AlgorithmReport.top_candidates` -> `duplicates`
- `ReferenceReport.top_candidates` -> `duplicates`
- `candidate_count` -> `duplicate_count`

如果需要兼容已有测试或消费者，也可以短期保留旧字段名，但正式输出和 Markdown 文案必须全面切换到 duplicates 语义。推荐直接完成字段重命名，避免双语义并存。

### 4. CLI / Config Changes

`top_k` 将不再参与正式报告逻辑，因此应从 CLI 和配置中移除，避免误导使用者。

影响范围：

- `BenchmarkConfig.top_k`
- CLI 参数 `--top-k`
- README 中所有 top-k 文案
- 测试构造 benchmark config 的位置

如果短期需要兼容旧调用，可保留参数但标记为废弃且忽略其值。推荐直接删除，避免“传了参数但无效”。

## Data Flow

### Ordinary Algorithm

1. 加载样本与 `recall_rows`
2. 对每个算法计算 `scores`
3. 根据规则模块中的该算法阈值过滤 duplicates
4. 将 duplicates 和耗时统计写入 `AlgorithmReport`

### Reference Algorithm

1. 计算 `name_score` 与 `metadata_score`
2. 按规则模块中的 reference 规则过滤 duplicates
3. 将 duplicates 和耗时统计写入 `ReferenceReport`

## Error Handling

- 若某个 `algorithm_id` 在规则模块中没有定义，立即报错，而不是静默跳过。
- 若阈值越界，例如给 `0..1` 量纲的算法配置了 `95.0`，应在规则定义或测试中尽早暴露。
- 报告生成阶段不应依赖“至少存在一个 duplicate”；空列表是合法结果。

## Testing Strategy

### Unit Tests

- 普通算法：
  - 当分数低于阈值时，不出现在 `duplicates`
  - 当分数高于等于阈值时，出现在 `duplicates`
  - 输出不再被 `top_k` 截断
- `reference`：
  - `name` 命中时进入 `duplicates`
  - `metadata` 命中时进入 `duplicates`
  - 两者都不命中时不输出

### Integration Tests

- `run_benchmark()` 输出的 `duplicate_count` 与 `duplicates.len()` 一致
- `repeat=1` 与 `repeat>1` 时，duplicate 集合保持一致
- CLI 输出的 JSON / Markdown 不再包含 top-k 语义文案

### Regression Tests

- 并行化后的 `reference` duplicates 结果与按相同规则从预计算分数构造的结果一致
- 普通算法的 duplicate 过滤使用独立规则文件中的阈值，而不是散落常量

## Migration Notes

- 这是一次报告语义变更，不是简单字段重命名。
- 下游若依赖 `top_candidates`，需要同步切换到 `duplicates`。
- README 和命令行帮助必须同步更新，否则行为与文档会不一致。

## Recommendation

按以下顺序实施：

1. 新增规则模块与规则查询接口
2. 为普通算法与 `reference` 增加 duplicates-only 聚合函数
3. 重构报告结构体与 Markdown 渲染
4. 移除 `top_k` 配置与 CLI 参数
5. 更新测试与 README
