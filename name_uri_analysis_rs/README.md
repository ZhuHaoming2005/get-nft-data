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
- `metadata` 使用 BM25 文档查重，阈值为 `0.6`；每个合约保留可查重的唯一 metadata 文档，先并行解析 metadata 并按全局唯一文档去重，再并行构建倒排索引、BM25 query 和候选文档对评分，命中后在合约层合并重复组。
- DuckDB 使用磁盘数据库，不支持 `:memory:`；准备阶段只生成本次运行的临时工作投影，不做持久化 prepared-table 缓存。

运行示例：

```bash
cargo run --release -- \
  --parquet ../output/top_contract_analysis/ethereum.parquet \
  --database ./data/analysis.duckdb \
  --output-dir ./output \
  --threads 32 \
  --memory-limit 60GB \
  --temp-directory ./data/duckdb-temp
```

输出：

- `summary.json`
- `summary.csv`

大数据建议：

- `--database` 放到空间充足的 SSD。
- `--temp-directory` 指向空间充足的本地磁盘。
- `--memory-limit` 按总预算处理；DuckDB 准备阶段可使用该总预算加速临时表构建，进入 Rust name 分析前会按已加载数据、threshold state、`chain_matrix` 复用状态估算和运行时 RSS 重新平衡 DuckDB/Rust 分配。
- 每个 DuckDB 重 SQL 执行前都会按当前进程 RSS 重新收紧 DuckDB `memory_limit`，避免 DuckDB 自身缓存、临时表和 Rust 常驻数据叠加后超过总预算。
- 通常不需要传 `--analysis-memory-limit`；如需手动指定 Rust name 分析预算，可传 `--analysis-memory-limit 16GB`，该值仍包含在 `--memory-limit` 总预算内。传 `auto` 等同默认自动平衡。
- CLI 默认显示进度条；批处理、日志重定向或作为库嵌入时可用 `--no-progress` 关闭。
- name 只维护一个阈值 state，默认 `95`；仍会对传入 Parquet 中的唯一规范名做完整 Jaro-Winkler 比较，并用长度上界跳过不可能达标的 pair。
- `chain_matrix` 会优先在内存预算允许时复用全局跨链 name 打分结果；预算不足时回退到按链对逐个计算，并只为命中的 name pair 建稀疏 union-find。
