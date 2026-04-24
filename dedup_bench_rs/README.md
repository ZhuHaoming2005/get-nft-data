# dedup_bench_rs

单 NFT `name / metadata` 查重 benchmark 工具。输入 1 个样例 NFT，从 DuckDB / Parquet 特征数据中召回候选，比较多种字符串算法的重复判定结果和耗时。

## 运行

在本目录执行：

```bash
cargo run -- run \
  --chain ethereum \
  --contract-address 0xseed \
  --token-id 1 \
  --token-uri "Seed/" \
  --image-uri "Seed/" \
  --name "Azuki #1" \
  --metadata-file ./examples/metadata.json \
  --feature-db ../output/top_contract_analysis/features.duckdb \
  --feature-parquet ../output/top_contract_analysis/ethereum.parquet \
  --output ./result/result.json \
  --repeat 5 \
  --algorithm-threads 32
```

## 参数

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--chain` | 是 | - | 目标链名，例如 `ethereum` |
| `--name` | 是 | - | 样例 NFT 名称 |
| `--metadata-file` | 是 | - | 样例 metadata JSON 文件 |
| `--feature-db` | 是 | - | DuckDB 特征库；无文件时自动创建 |
| `--output` | 是 | - | JSON 输出路径；同名 `.md` 会自动生成 |
| `--contract-address` | 否 | 空字符串 | 样例合约地址；非空时召回阶段排除同合约记录 |
| `--token-id` | 否 | 空字符串 | 样例 token id，仅写入报告样例信息 |
| `--token-uri` | 否 | 空字符串 | URI baseline 过滤片段；非空时排除 `token_uri` 包含该片段的合约 |
| `--image-uri` | 否 | 空字符串 | URI baseline 过滤片段；非空时排除 `image_uri` 包含该片段的合约 |
| `--feature-parquet` | 否 | - | `feature-db` 中无该链数据时的 Parquet 导入源 |
| `--repeat` | 否 | `3` | 每个算法正式计时重复次数；代码会按至少 `1` 次执行 |
| `--algorithm-threads` | 否 | `32` | 算法层并行线程数；必须大于 `0` |

`--token-uri` / `--image-uri` 不参与 name 或 metadata 算法评分，只作为隐藏 URI baseline：已经能被 URI 片段命中的合约不会出现在最终 `duplicates` 中。

## 输入

`--metadata-file` 直接传原始 metadata JSON，对象或数组都可以：

```json
{
  "name": "Azuki #1",
  "description": "rare dragon gold",
  "attributes": [
    { "trait_type": "Mood", "value": "Calm" }
  ]
}
```

程序会从 metadata 生成 `metadata_doc` 和 `metadata_keywords`，用于 metadata 召回与评分。

## 数据源

读取优先级：

1. 读取 `feature-db` 中的 `nft_features`。
2. 如果该链无数据，并且提供了 `feature-parquet`，则导入 Parquet 到 `feature-db` 后再读取。

推荐字段：

| 字段 | 用途 |
| --- | --- |
| `contract_address` / `token_id` / `name` | 基础身份和 name scoring |
| `token_uri` / `image_uri` | URI baseline 过滤 |
| `name_norm` | name 前缀召回；缺失时运行时计算 |
| `metadata_json` / `metadata_doc` | metadata 展示与评分；缺失 `metadata_doc` 时从 JSON 生成 |
| `metadata_keywords_arr` | metadata 关键词召回；缺失时运行时提取 |

召回只使用 `name_norm` 前 8 字符前缀和 `metadata_keywords_arr` 关键词；正式报告只保留阈值命中的 `duplicates`。

## 判重规则

规则集中在 `src/decision_rules.rs`。当前阈值是本 benchmark 的默认经验规则，不是跨数据集通用推荐阈值；不同算法分数量纲不同，不应套用统一阈值。

| 算法 | 分数范围 | 当前阈值 |
| --- | --- | ---: |
| `name_exact_normalized` | `0..100` | `100.0` |
| `name_jaro_winkler` | `0..100` | `95.0` |
| `name_damerau_levenshtein` | `0..100` | `80.0` |
| `name_monge_elkan` | `0..100` | `85.0` |
| `metadata_bm25` | `0..1` | `0.60` |
| `metadata_token_cosine` | `0..1` | `0.80` |
| `metadata_soft_tfidf` | `0..1` | `0.75` |
| `metadata_weighted_jaccard` | `0..1` | `0.70` |

补充参数：

- `metadata_bm25` 使用 `k1 = 1.2`、`b = 0.75`，并用样例 metadata 对自身的 BM25 分数归一化到 `0..1`。
- `metadata_soft_tfidf` 内部 token 匹配阈值为 Jaro-Winkler `0.9`。

严格比较算法优劣时，应使用人工标注样本或阈值扫描，把各算法校准到相同目标，例如相同 precision / PPV，再比较 recall 和耗时。

## 输出

输出包含：

- `result.json`：完整结构化报告
- `result.md`：同名 Markdown 摘要

核心字段：

- `sample`：样例输入
- `recall_elapsed_ms` / `recall_candidate_count`：召回耗时和候选数
- `name_algorithms` / `metadata_algorithms`：各算法的 `decision_rule`、`runs_ms`、`avg_ms`、`min_ms`、`duplicate_count`、`duplicates`

`name` duplicates 为合约级结果，包含 `contract_address`、`name`、`metadata_doc`、`max_score`、`duplicate_token_count`。`metadata` duplicates 包含 `contract_address`、`metadata_doc`、`score`。

## 验证

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```
