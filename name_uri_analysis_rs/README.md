# Name URI Analysis RS

Rust + DuckDB 一体分析脚本，读取 `top_contract_analysis_rs export-snapshot` 导出的 Parquet。

功能：

- `name` 使用 Jaro-Winkler，单阈值运行，CLI 默认阈值为 `95`。
- 分析链集合来自传入的 Parquet 文件；未传入的链不会参与统计，只有一条链时不输出跨链结果。
- `token_uri` / `image_uri` 只输出单链内规范化 URI 的跨合约重复，即 `norm_cross`：
  - `v1`: `token_uri` 命中
  - `v2`: `token_uri` 未命中但 `image_uri` 命中
  - `v3`: 任一 URI 命中
  - 不输出 URI 任意重复、严格串重复、跨链 URI 汇总。
- `metadata` 使用 BM25 文档查重，阈值为 `0.6`；每个合约保留可查重的唯一 metadata 内容文档，最终判重文档提取 `description`、`attributes.trait_type/value`、`image`、`external_url` 等内容值，不把通用 JSON schema 字段名作为重复依据。脚本先并行解析 metadata 并按全局唯一内容文档去重，再并行构建倒排索引、BM25 query 和候选文档对评分，命中后在合约层合并重复组。metadata 评分在每个 worker 内按 left-doc 临时生成候选、立即评分并释放；进度条按已处理 left-doc 推进，消息显示已评分候选对数、基于已处理 left-doc 的剩余候选估算、吞吐、ETA 和已命中文档对数。
- DuckDB 使用 `:memory:` 内存数据库，不再设置 DuckDB `memory_limit`；兼容旧命令保留的 `--database` 参数不再用于打开磁盘库。准备阶段只生成本次运行的临时工作投影，不做持久化 prepared-table 缓存。

运行示例：

```bash
cargo run --release -- \
  --parquet ./data/ethereum.parquet \
  --output-dir ./output \
  --threads 96 \
  --temp-directory ./data/duckdb-temp
```

输出：

- `summary.json`
- `summary.csv`

大数据建议：

- 默认面向 96 核、192GB 内存机器：`--threads` 默认 `96`，`--memory-limit` 默认 `auto`。
- 默认不需要传 `--database`；DuckDB 固定使用 `:memory:`，不会创建或复用 `.duckdb` 文件。
- `--temp-directory` 指向空间充足的本地磁盘，供 DuckDB 临时文件使用。
- 不再对 DuckDB 执行 `PRAGMA memory_limit`；DuckDB 可按进程和系统可用内存自行使用资源。
- `--memory-limit` 只作为 Rust name 分析的自适应分批预算，默认 `auto`，不限制 DuckDB。
- 通常不需要传 `--analysis-memory-limit`；如需手动指定 Rust name 分析预算，可传 `--analysis-memory-limit 16GB`，该值仍包含在 `--memory-limit` 指定的 Rust 预算内。传 `auto` 等同默认。
- CLI 默认显示进度条；批处理、日志重定向或作为库嵌入时可用 `--no-progress` 关闭。
- name 只维护一个阈值 state，默认 `95`；仍会对传入 Parquet 中的唯一规范名做完整 Jaro-Winkler 比较，并用长度上界跳过不可能达标的 pair。
- `chain_matrix` 会优先在内存预算允许时复用全局跨链 name 打分结果；预算不足时回退到按链对逐个计算，并只为命中的 name pair 建稀疏 union-find。
- metadata 解析批次为 16K 行，metadata BM25 评分按 256 个 left-doc 一批调度；候选 right-doc 不做 chunk 级缓存，而是在 worker 内随生成随评分，并通过 scratch pool 复用候选去重数组；postings、候选列表和 metadata contract membership 使用紧凑 `u32` 编号，BM25 预处理后常驻候选 doc 只保留唯一 token，适配 96 核机器并限制候选缓存峰值。
