# Name URI Analysis RS

Rust + DuckDB 一体分析脚本，读取 `top_contract_analysis_rs export-snapshot` 导出的 Parquet。

功能：

- `name` 使用 RapidFuzz 的 Jaro-Winkler 批量比较器，单阈值运行，CLI 默认阈值为 `95`；多链输入时同时输出跨链汇总和定向链对矩阵。
- 分析链集合来自传入的 Parquet 文件；未传入的链不会参与统计，只有一条链时不输出跨链结果。
- `token_uri` / `image_uri` 使用规范化 URI 的 `norm_cross` 口径，输出范围包括：
  - `intra_chain`：同一链内的跨合约重复；
  - `cross_chain_summary`：主链 NFT 与任意其它输入链发生 URI 重复；
  - `chain_matrix`：主链 NFT 与指定目标链发生 URI 重复，按方向分别输出。
- 每个 URI 范围均包含：
  - `v1`: `token_uri` 命中
  - `v2`: `token_uri` 未命中但 `image_uri` 命中
  - `v3`: 任一 URI 命中
- `metadata` 使用 BM25 文档查重，阈值为 `0.6`，多链输入时输出跨链汇总和定向链对矩阵；每个合约选取 `token_id` 最小的可用 metadata 作为代表文档。脚本先用模板文档召回候选，再按 `top_contract_analysis_rs` 的 final metadata 语义提取 `description`、`attributes.trait_type/value`、`image`、`external_url` 等内容值进行验证，不把完整 raw JSON 作为重复依据。
- `duplicate_contract_ratio` / `duplicate_nft_ratio` 的分母统一使用每条链在 `analysis_rows` 中的全量非空合约地址数和 NFT 行数；name、URI、metadata 不再分别使用各自可分析子集作为分母。
- DuckDB 使用 `:memory:` 内存数据库，不再设置 DuckDB `memory_limit`；兼容旧命令保留的 `--database` 参数不再用于打开磁盘库。准备阶段只生成本次运行的临时工作投影，不做持久化 prepared-table 缓存。

运行示例：

```bash
cargo run --release -- \
  --parquet ./data/ethereum.parquet \
  --output-dir ./output \
  --physical-cores 32 \
  --temp-directory ./data/duckdb-temp
```

同时分析 Ethereum、Base、Polygon 和 Solana：

```bash
cargo run --release -- \
  --parquet ./data/ethereum.parquet \
  --parquet ./data/base.parquet \
  --parquet ./data/polygon.parquet \
  --parquet ./data/solana.parquet \
  --output-dir ./output \
  --physical-cores 32 \
  --temp-directory ./data/duckdb-temp
```

EVM 合约地址按小写规范化后聚合；Solana collection 地址按 Base58 原值聚合，
不会把仅大小写不同的 Solana 标识符合并。`token_id` 在分析输入中统一按字符串处理，
因此既兼容 EVM 数字 token ID，也兼容 Solana mint 地址。

输出：

- `summary.json`
- `summary.csv`

大数据建议：

- 线程数面向 SMT 主机：`--threads` 默认 `0`（= auto = SMT，使用全部逻辑核）。在 32 物理核 + SMT（64 逻辑核）的机器上默认 64 线程；计算密集的 DuckDB hash join 和 Jaro-Winkler 打分在物理核上更高效，传 `--physical-cores 32` 即把 DuckDB 和 Rayon 都钉到 32 线程。优先级：`--physical-cores` > `--threads` > SMT 默认。
- 默认不需要传 `--database`；DuckDB 固定使用 `:memory:`，不会创建或复用 `.duckdb` 文件。
- DuckDB 显式执行 `PRAGMA memory_limit`：`--duckdb-memory-limit` 默认 `auto`（取系统可用内存的约 75%，给 Rust 分析结构留出空间），也可传 `200GB` 等显式值。DuckDB 与 Rust 共享进程地址空间。
- `--temp-directory` 默认指向系统临时目录下的 `name_uri_analysis_rs_duckdb` 子目录，供 DuckDB hash join 溢写临时文件；大数据下建议显式指向高速本地 NVMe。
- `--memory-limit` 只作为 Rust name 分析的自适应分批预算，默认 `auto`，不限制 DuckDB。
- 若 64 vCPU 切自双路宿主、跨两个 NUMA 节点，大块只读结构（name atoms / 候选索引 / MetadataData）的 first-touch 落点会影响性能；代码层不做 NUMA 绑定，可用 `numactl --interleave=all` 或 `--cpunodebind` 运行。
- 通常不需要传 `--analysis-memory-limit`；如需手动指定 Rust name 分析预算，可传 `--analysis-memory-limit 16GB`，该值仍包含在 `--memory-limit` 指定的 Rust 预算内。传 `auto` 等同默认。
- CLI 默认显示进度条；批处理、日志重定向或作为库嵌入时可用 `--no-progress` 关闭。
- name 只维护一个阈值 state，默认 `95`；仍会对传入 Parquet 中的唯一规范名做完整 Jaro-Winkler 比较，并用长度上界跳过不可能达标的 pair。候选索引容器和所有并行 dense scratch 均计入 Rust 分析预算，预算不足时 scratch 自动切换为稀疏去重。
- `chain_matrix` 会优先在内存预算允许时复用全局跨链 name 打分结果；预算不足时回退到按链对逐个计算，并只为命中的 name pair 建稀疏 union-find。
- DuckDB 在一次合约级分组中计算链总量、name 代表和 `arg_min(token_id,rowid)` metadata 代表；代表行和共享 token 行通过 Arrow 批量读取。
- metadata JSON 单次解析后同时生成模板与内容文档；候选 scratch 通过轻量池跨批次复用，锁仅用于 Rayon job 获取和归还 scratch；代表内容用 `Arc` 共享，避免深拷贝。
- metadata 模板索引、内容候选 postings、BM25 文档和合约 membership 使用紧凑整数编号；内容 BM25 使用排序 `(u32 token_id, frequency)` 评分，结果与原字符串实现等价。
