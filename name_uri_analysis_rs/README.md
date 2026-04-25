# Name URI Analysis RS

Rust + DuckDB 一体分析脚本，读取 `top_contract_analysis_rs export-snapshot` 导出的 Parquet。

功能：

- `name` 使用 Jaro-Winkler，相似度阈值默认 `90,95,98`。
- 分析链集合来自传入的 Parquet 文件；未传入的链不会参与统计，只有一条链时不输出跨链结果。
- `token_uri` / `image_uri` 保留旧脚本口径：
  - `v1`: `token_uri` 命中
  - `v2`: `token_uri` 未命中但 `image_uri` 命中
  - `v3`: 任一 URI 命中
  - 单链内支持严格串、规范化串，以及 EVM 旧口径中的任意重复 / 跨合约重复。
  - 跨链统计只判断 URI 是否出现在其它链。
- DuckDB 使用磁盘数据库，不支持 `:memory:`；准备阶段会先生成临时工作投影，尽量用 `--memory-limit` 内的缓存和临时空间减少 Parquet 重复扫描，URI key 会按字段分批聚合以压低单次 hash 聚合峰值。

运行示例：

```bash
cargo run --release -- \
  --parquet ../output/top_contract_analysis/ethereum.parquet \
  --database ./data/analysis.duckdb \
  --output-dir ./output \
  --threads 32 \
  --memory-limit 60GB \
  --temp-directory ./data/duckdb-temp \
  --persist-prepared
```

输出：

- `summary.json`
- `summary.csv`

大数据建议：

- `--database` 放到空间充足的 SSD。
- `--temp-directory` 指向空间充足的本地磁盘。
- `--persist-prepared` 将 DuckDB 准备阶段表持久化到 `--database`，适合后续重复调阈值或只重跑 Rust 分析。
- `--reuse-prepared` 在 Parquet 路径、文件大小、mtime 和 schema version 匹配时复用已持久化表；不匹配会自动重建，并隐含 `--persist-prepared`。
- `--memory-limit` 按总预算处理；DuckDB 准备阶段可使用该总预算加速临时表构建，进入 Rust name 分析前会按已加载数据、threshold state、`chain_matrix` 复用状态估算和运行时 RSS 重新平衡 DuckDB/Rust 分配。
- 每个 DuckDB 重 SQL 执行前都会按当前进程 RSS 重新收紧 DuckDB `memory_limit`，避免 DuckDB 自身缓存、临时表和 Rust 常驻数据叠加后超过总预算。
- 通常不需要传 `--analysis-memory-limit`；如需手动指定 Rust name 分析预算，可传 `--analysis-memory-limit 16GB`，该值仍包含在 `--memory-limit` 总预算内。传 `auto` 等同默认自动平衡。
- CLI 默认显示进度条；批处理、日志重定向或作为库嵌入时可用 `--no-progress` 关闭。
- 程序会按总预算、当前 RSS 和每个 threshold state 的实际估算自动把 name 阈值分批：headroom 足够时一次 Jaro-Winkler 打分服务多个阈值；headroom 不足时自动退回小批/单阈值。
- name 第一版不做 blocking，会对传入 Parquet 中的唯一规范名做全量两两 Jaro-Winkler；结果优先准确性，运行时间按唯一 name 数量平方增长。
- `chain_matrix` 会优先在内存预算允许时复用全局跨链 name 打分结果；预算不足时回退到按链对逐个计算，并只为命中的 name pair 建稀疏 union-find。
