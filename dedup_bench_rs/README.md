# dedup_bench_rs

单 NFT `name / metadata` 查重 benchmark 工具。

目标：

- 输入 1 个样例 NFT
- 只基于 `name` 和 `metadata` 做召回与打分
- 对比多种字符串算法的重复判定结果和耗时
- 复用现有 DuckDB / Parquet 特征数据，不访问 PostgreSQL

当前实现明确**不使用**这些字段：

- `token_uri`
- `image_uri`
- `symbol`

## 功能范围

- 样例输入来自命令行参数和一个 metadata 文件：
  - `name`：通过命令行直接传入
  - `contract-address`：可选
  - `token-id`：可选
  - `metadata-file`：样例 metadata JSON
- 数据源优先级：
  - 先读 `feature-db` 里的 `nft_features`
  - 若该链无数据，则从 `feature-parquet` 导入到 `feature-db`，再读 `nft_features`
- 共享召回只使用：
  - `name_norm` 前 8 字符前缀
  - `metadata_keywords_arr` 关键词
- 输出：
  - 一份 JSON 详细报告
  - 一份 Markdown 摘要报告
  - 正式报告只输出被各算法判定为重复的 `duplicates`
  - 每个算法的判重规则集中定义在 `src/decision_rules.rs`
- 计时口径：
  - 算法层并行线程数默认是 `32`，可通过参数自定义
  - 每个算法都会先对同一批 `recall_rows` 预热一次，再进入正式计时
  - 正式计时时会轮转各算法的执行顺序，降低固定顺序带来的缓存偏差

## 运行方式

在 [dedup_bench_rs](..\dedup_bench_rs) 目录下执行：

```bash
cargo run -- run \
  --chain ethereum \
  --contract-address 0xseed \
  --token-id 1 \
  --name "Azuki #1" \
  --metadata-file ./examples/metadata.json \
  --feature-db ../output/top_contract_analysis/features.duckdb \
  --feature-parquet ../output/top_contract_analysis/ethereum.parquet \
  --output ./result/result.json \
  --repeat 5 \
  --algorithm-threads 30
```

参数说明：

- `--chain`
  目标链名，例如 `ethereum`
- `--name`
  样例 NFT 的名称，直接从命令行输入
- `--contract-address`
  可选。样例合约地址；提供后会在召回阶段排除该合约下的全部记录
- `--token-id`
  可选。样例 token id，仅作为样例标识保留
- `--metadata-file`
  样例 metadata JSON 文件
- `--feature-db`
  DuckDB 文件路径。文件不存在时会自动创建；Parquet 回退导入结果也会持久化到这里
- `--feature-parquet`
  可选。DuckDB 中没有该链数据时的 Parquet 导入源路径
- `--output`
  JSON 输出路径；同名 `.md` 会自动生成
- `--repeat`
  每个算法重复执行次数，用于统计平均耗时和最小耗时
- `--algorithm-threads`
  算法层并行线程数，默认 `30`

## 输入文件格式

### metadata-file

直接传原始 metadata JSON。

对象示例：

```json
{
  "name": "Azuki #1",
  "description": "rare dragon gold",
  "attributes": [
    { "trait_type": "Mood", "value": "Calm" }
  ]
}
```

数组示例也支持：

```json
[
  { "trait_type": "Mood", "value": "Calm" },
  { "trait_type": "Type", "value": "Dragon" }
]
```

程序会：

1. 读取 JSON
2. 序列化成 `metadata_json`
3. 使用现有逻辑生成 `metadata_doc`
4. 提取 `metadata_keywords`

## 算法列表

### Name

- `name_exact_normalized`
- `name_jaro_winkler`
- `name_damerau_levenshtein`
- `name_monge_elkan`

### Metadata

- `metadata_bm25`
- `metadata_token_cosine`
- `metadata_soft_tfidf`
- `metadata_weighted_jaccard`

## 输出说明

### JSON 报告

主字段：

- `chain`
- `source`
- `sample`
- `recall_elapsed_ms`
- `recall_candidate_count`
- `name_algorithms`
- `metadata_algorithms`

其中：

- `name_algorithms` 包含所有 `name` 类算法结果
- `metadata_algorithms` 包含所有 `metadata` 类算法结果

每个算法包含：

- `algorithm_id`
- `field`
- `decision_rule`
- `repeat`
- `runs_ms`
- `avg_ms`
- `min_ms`
- `duplicate_count`
- `duplicates`

其中：

- `name_algorithms[*].duplicates` 为合约级结果，包含：
  - `contract_address`
  - `name`
  - `max_score`
  - `duplicate_token_count`
- `metadata_algorithms[*].duplicates` 为合约级结果，包含：
  - `contract_address`
  - `metadata_doc`
  - `score`

### Markdown 报告

用于快速人工查看：

- 样例信息
- 召回数量和耗时
- `Name Algorithms` 合约级重复结果
- `Metadata Algorithms` 合约级重复结果

## 数据要求

DuckDB / Parquet 至少需要包含这些业务字段：

- `contract_address`
- `token_id`
- `name`

建议包含这些预计算列：

- `name_norm`
- `metadata_json`
- `metadata_doc`
- `metadata_keywords_arr`

如果缺少这些预计算列：

- `name_norm` 会回退为运行时计算
- `metadata_doc` 会回退为运行时从 `metadata_json` 生成
- `metadata_keywords_arr` 会回退为运行时提取

## 测试

运行测试：

```bash
cargo test
```

静态检查：

```bash
cargo clippy --all-targets -- -D warnings
```

## 已验证点

- DuckDB 直接读取
- DuckDB 无链数据时会从 Parquet 导入并持久化到 `feature-db`
- 召回阶段不会因为 `token_uri / image_uri / symbol` 命中
- `repeat=1` 与 `repeat>1` 候选集合一致
- CLI 会同时输出 JSON 和 Markdown
