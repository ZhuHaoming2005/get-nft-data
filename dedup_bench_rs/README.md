# dedup_bench_rs

单 NFT `name / metadata` 查重 benchmark 工具。

目标：

- 输入 1 个样例 NFT
- 只基于 `name` 和 `metadata` 做召回与打分
- 对比多种字符串算法的候选结果和耗时
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
  --top-k 50 \
  --repeat 5
```

参数说明：

- `--chain`
  目标链名，例如 `ethereum`
- `--name`
  样例 NFT 的名称，直接从命令行输入
- `--contract-address`
  可选。样例合约地址，用于排除自己命中自己
- `--token-id`
  可选。样例 token id，用于排除自己命中自己
- `--metadata-file`
  样例 metadata JSON 文件
- `--feature-db`
  DuckDB 文件路径。文件不存在时会自动创建；Parquet 回退导入结果也会持久化到这里
- `--feature-parquet`
  可选。DuckDB 中没有该链数据时的 Parquet 导入源路径
- `--output`
  JSON 输出路径；同名 `.md` 会自动生成
- `--top-k`
  每个算法保留的候选数量
- `--repeat`
  每个算法重复执行次数，用于统计平均耗时和最小耗时

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
- `name_normalized_levenshtein`
- `name_trigram_jaccard`
- `name_current_hybrid`

当前基线公式：

```text
0.65 * Jaro-Winkler + 0.35 * normalized Levenshtein
```

### Metadata

- `metadata_token_jaccard`
- `metadata_jaro_winkler_doc`
- `metadata_trigram_jaccard_doc`
- `metadata_token_cosine`
- `metadata_current_hybrid`

当前基线公式：

```text
0.45 * token Jaccard + 0.55 * Jaro-Winkler
```

### Reference

- `current_name_metadata_reference`

含义：

- 仍使用当前 name / metadata 基线公式
- 但完全去掉 `uri / symbol` 相关规则
- 只有 `name_match` 和 `metadata_match`

## 输出说明

### JSON 报告

主字段：

- `chain`
- `source`
- `sample`
- `recall_elapsed_ms`
- `recall_candidate_count`
- `reference`
- `algorithms`

其中：

- `reference` 是当前 `name + metadata` 基线结果
- `algorithms` 是所有对比算法的独立结果

每个算法包含：

- `algorithm_id`
- `field`
- `repeat`
- `avg_ms`
- `min_ms`
- `candidate_count`
- `top_candidates`

### Markdown 报告

用于快速人工查看：

- 样例信息
- 召回数量和耗时
- 当前参考结果
- 各算法的 Top-K 候选

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
