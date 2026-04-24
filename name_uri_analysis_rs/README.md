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
- DuckDB 使用磁盘数据库，不支持 `:memory:`，适合大数据用外存中间表控制峰值内存。

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
- `--memory-limit` 按总预算处理，DuckDB 和 Rust name 分析共享该预算；程序会预留系统/allocator/字符串/HashMap 开销，并在 DuckDB 建表阶段就避免使用完整预算。
- 如需指定 Rust name 分析预算，可传 `--analysis-memory-limit 16GB`；该值会从 `--memory-limit` 总预算中扣除，不会额外叠加到 DuckDB 限额。传 `auto` 时按设备当前可用内存估算。
- 程序会按内存预算和运行时 RSS 自动把 name 阈值分批：内存足够时一次 Jaro-Winkler 打分服务多个阈值，提高速度；接近预算高水位时根据剩余 headroom 自动退回小批/单阈值，降低峰值。
- name 第一版不做 blocking，会对传入 Parquet 中的唯一规范名做全量两两 Jaro-Winkler；结果优先准确性，运行时间按唯一 name 数量平方增长。
- `chain_matrix` 按链对逐个计算，并只为命中的 name pair 建稀疏 union-find；不会为所有链对常驻完整节点数组。
