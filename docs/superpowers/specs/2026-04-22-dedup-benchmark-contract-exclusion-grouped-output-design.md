# Dedup Benchmark Contract Exclusion And Grouped Output Design

## Goal

在现有 duplicates-only benchmark 基础上增加三项行为：

1. 默认排除样本自己的 `contract_address`
2. 正式报告将 `name` 与 `metadata` 结果分开输出
3. `name` 结果按合约地址聚合，且不再展示单个 NFT 明细；`metadata` 结果保持 token 级输出

## Current State

- `FeatureStore::load_recall_rows()` 会召回同链候选，并在 `collect_recall_rows_from_query()` 中排除“同合约且同 token”的样本自身记录。
- 正式报告当前结构为：
  - `name_algorithms`
  - `metadata_algorithms`
- `name` 算法的 `duplicates` 结果已经按合约聚类展示。

## Desired State

### Recall Filtering

- 若样本 `contract_address` 非空，则召回阶段默认排除所有 `contract_address == sample.contract_address` 的记录。
- 若样本 `contract_address` 为空，则保持现有行为，不做合约级排除。
- 该排除应发生在召回/读取阶段，而不是报告渲染阶段，以保证：
  - 计时口径一致
  - `duplicate_count` 一致
  - 所有算法看到的候选集一致

### Report Shape

`BenchmarkReport` 从：

- `name_algorithms`
- `metadata_algorithms`

改为：

- `name_algorithms`
- `metadata_algorithms`

### Name Output Aggregation

`name` 类算法正式输出按合约聚合：

- 同一 `contract_address` 下多个 token 命中时，输出一个合约级结果
- 合约级结果包含：
  - `contract_address`
  - `max_score`
  - `duplicate_token_count`

排序只用于输出稳定性，不作为业务语义。

### Metadata Output

`metadata` 类算法继续保持 token 级输出：

- 每个 duplicate 仍是单个 token 结果
- 不做合约级聚合
- 正式报告中展示 `metadata_doc`，不再展示 `name`

理由：

- metadata 相似通常依赖具体 token 的文本内容
- 直接聚合到合约层容易掩盖差异

## Architecture

### 1. Recall Exclusion In Store Layer

修改 `dedup_bench_rs/src/store.rs`：

- 在 `collect_recall_rows_from_query()` 中，把当前排除条件从：
  - `sample.contract_address && sample.token_id`
  - 且候选 `contract_address + token_id` 完全相同
- 调整为：
  - 只要样本 `contract_address` 非空
  - 且候选 `contract_address` 与样本相同
  - 就直接跳过

### 2. Split Algorithm Reports By Field

修改 `dedup_bench_rs/src/report.rs` 和 `dedup_bench_rs/src/benchmark.rs`：

- benchmark 正式报告不再包含单独 `reference`
- 所有算法结果统一按 `AlgorithmField` 分为：
  - `name_algorithms`
  - `metadata_algorithms`
- `name_current_hybrid` 和 `metadata_current_hybrid` 作为普通算法分别保留在两类结果中

### 3. Add Name Contract Aggregation Model

修改 `dedup_bench_rs/src/algorithms.rs`：

新增结构：

- `NameContractDuplicate`
- `NameAlgorithmReport`
- `MetadataAlgorithmReport`
- `MetadataDuplicate`

其中：

- `NameAlgorithmReport.duplicates` 是 `Vec<NameContractDuplicate>`
- `MetadataAlgorithmReport.duplicates` 是 `Vec<MetadataDuplicate>`

`MetadataDuplicate` 至少包含：

- `contract_address`
- `token_id`
- `metadata_doc`
- `score`

### 4. Name Aggregation Logic

对 `name` 类算法：

- 先保留当前 token 级 duplicate 过滤逻辑
- 再按 `contract_address` 聚合
- 每个合约下：
  - `max_score` 取该合约命中 token 中最高值
  - `duplicate_token_count` 表示该合约下命中的 token 数

### 5. Markdown Rendering

`report.rs` 中的 Markdown 渲染改为两块：

- `Name Algorithms`
- `Metadata Algorithms`

其中：

- `Name Algorithms` 下每个算法按合约输出，只展示合约级聚类信息
- `Metadata Algorithms` 下每个算法按 token 输出，展示 `metadata_doc`

## Testing Strategy

### Store Tests

- 样本 `contract_address` 非空时，同合约不同 token 的记录也会被排除
- 样本 `contract_address` 为空时，不触发合约级排除

### Algorithm Tests

- `name` 类 duplicate 结果能正确聚合为合约级
- 同一合约多个 token 命中时：
  - `duplicate_token_count` 正确
  - `max_score` 正确
  - `tokens` 明细完整
- `metadata` 类 duplicate 结果仍保持 token 级
- `metadata` 类 duplicate 报告项包含 `metadata_doc` 而不是 `name`

### Benchmark / Report Tests

- `BenchmarkReport` 正确拆分为 `name_algorithms` / `metadata_algorithms`
- Markdown 里有分块标题
- 原合约记录不会出现在 `name`、`metadata` 输出中

## Non-Goals

- 不修改各算法打分逻辑
- 不修改阈值规则文件
- 不新增 CLI 参数
- 不改变 benchmark 外层计时调度方式
