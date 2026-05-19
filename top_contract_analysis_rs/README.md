# top_contract_analysis_rs

这是 NFT 重复合约分析流程的 Rust 实现。当前二进制支持 3 个子命令：

- `analyze`：分析单个 seed 合约，输出 JSON 报告和 Markdown 报告
- `batch`：从文本文件读取多个 seed 合约，批量分析并输出单合约报告和汇总报告
- `export-snapshot`：从 PostgreSQL 导出特征快照到 Parquet，供本地 DuckDB 分析使用

## 运行要求

- Rust stable 工具链
- 运行 `analyze` / `batch` 时，需要能访问 Alchemy、Etherscan、OpenSea
- 运行 `export-snapshot` 时，需要能访问 PostgreSQL

项目使用了 bundled DuckDB，不需要单独安装 DuckDB。`analyze` / `batch` 默认会让 DuckDB 使用当前可用线程数、`80GB` 内存预算，并显式关闭 insertion-order 保留以提升导入和 SQL recall 吞吐；如需调整，可通过 CLI 参数覆盖。

## 目录结构

- `src/main.rs`：CLI 入口
- `src/constants.rs`：`export-snapshot` 的 PostgreSQL 连接常量
- `src/analysis`：重复合约分析逻辑
- `src/reporting`：JSON / Markdown 报告渲染
- `src/store`：DuckDB 特征库、Postgres 快照导出、signal cache
- `tests`：集成测试

## 构建与测试

在 [top_contract_analysis_rs](../top_contract_analysis_rs) 目录下执行：

```bash
cargo build
cargo test
```

## 配置变量

先复制 [.env.example](./.env.example)，再按实际环境填写。

注意：程序本身**不会自动加载** `.env` 文件。`analyze` / `batch` 不直接读 API key 环境变量，而是通过 CLI 参数传入。Alchemy REST/RPC URL 会在内部按 `/nft/v2/<key>`、`/nft/v3/<key>`、`/v2/<key>` 拼接 API key，不需要把 key 放到 header。`.env.example` 里的 `ALCHEMY_API_KEY` 等变量主要用于命令行插值。Bash 示例：

```bash
set -a
source .env
set +a
```

`export-snapshot` 不再读取 `DB_*` 环境变量。PostgreSQL 连接参数集中在 [src/constants.rs](./src/constants.rs)，运行前直接修改该文件中的 `DB_HOST`、`DB_PORT`、`DB_NAME`、`DB_USER`、`DB_PASS`、`DB_CONNECT_TIMEOUT` 常量。

## 命令说明

### 1. 导出特征快照

把 PostgreSQL 中的 NFT 特征快照导出为 Parquet 文件。

```bash
cargo run --release --features export-snapshot -- export-snapshot \
  --chain ethereum \
  --output ../output/top_contract_analysis/ethereum.parquet
```

可选参数：

- `--fetch-size 100000`

### 2. 分析单个 Seed 合约

分析一个 seed 合约，并默认输出：

- `result/top_contract_analysis__<seed>.json`
- `result/top_contract_analysis__<seed>.md`

如果传了 `--output`，JSON 会写到该路径，Markdown 会写到同目录、同 basename 的 `.md` 文件。

示例：

```bash
cargo run --release -- analyze \
  --chain ethereum \
  --seed-contract-address 0xBd3531dA5CF5857e7CfAA92426877b022e612cf8 \
  --alchemy-api-key "O6O-K8fkagLHjOa-LLM3_" \
  --etherscan-api-key "5S6SMJYGF2H28RZWVV97YXQMQHTWFG7N3M" \
  --opensea-api-key "2d17a25e68714720883ac996f5459b17" \
  --feature-parquet ../output/top_contract_analysis/ethereum.parquet \
  --feature-db ../output/top_contract_analysis/features.duckdb \
  --signal-cache-db ../output/top_contract_analysis/signals.duckdb \
  --max-recall-rows 30000000 \
  --api-max-concurrency 8 \
  --duckdb-memory-limit 50GB \
  --duckdb-threads 32
```

常用参数：

- `--alchemy-network eth-mainnet`
- `--name-threshold 95`
- `--metadata-threshold 0.6`
- `--timeout 60`
- `--max-tokens-per-contract 500`
- `--max-recall-rows 100000`：单批 SQL recall 读取行数；`0` 表示单次读取全部。非 `0` 时会分批读取完整 recall 结果。
- `--api-max-concurrency 12`
- `--contract-max-concurrency 4`
- `--sale-metric-max-concurrency 4`
- `--duckdb-threads 0`：`0` 表示使用当前可用线程数
- `--duckdb-memory-limit 80GB`
- `--output ./result/azuki.json`

### 3. 批量分析 Seed 合约

`seed_file` 里每行写一个合约地址，例如：

```text
0xed5af388653567af2f388e6224dc7c4b3241c544
0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d
```

运行示例：

```bash
cargo run --release -- batch \
  --chain ethereum \
  --seed-file ./seeds.txt \
  --alchemy-api-key "O6O-K8fkagLHjOa-LLM3_" \
  --etherscan-api-key "5S6SMJYGF2H28RZWVV97YXQMQHTWFG7N3M" \
  --opensea-api-key "2d17a25e68714720883ac996f5459b17" \
  --feature-parquet ../output/top_contract_analysis/ethereum.parquet \
  --feature-db ../output/top_contract_analysis/features.duckdb \
  --signal-cache-db ../output/top_contract_analysis/signals.duckdb \
  --output-dir ./result \
  --seed-network-max-concurrency 3 \
  --max-recall-rows 30000000 \
  --api-max-concurrency 8 \
  --seed-metadata-max-concurrency 1 \
  --contract-max-concurrency 16 \
  --sale-metric-max-concurrency 16 \
  --seed-cpu-max-concurrency 1 \
  --duckdb-memory-limit 50GB
```

批量输出包括：

- 每个 seed 合约各自的 JSON + Markdown 报告
- `top_contract_analysis__summary.json`
- `top_contract_analysis__summary.md`

常用参数：

- `--timeout 30`
- `--seed-network-max-concurrency 4`：同时处于 seed 级网络阶段的 seed 合约数，覆盖 seed context 抓取和后续候选合约网络分析阶段。
- `--api-max-concurrency 8`
- `--seed-metadata-max-concurrency 1`：批量模式下同时下载 seed 合约 metadata 的 seed 数。
- `--contract-max-concurrency 4`
- `--sale-metric-max-concurrency 4`
- `--seed-cpu-max-concurrency 1`：同时处于 seed 级 CPU 密集阶段的 seed 合约数，覆盖 DuckDB recall / duplicate scoring。
- `--duckdb-threads 0`
- `--duckdb-memory-limit 80GB`
- `--max-recall-rows 100000`：单批 SQL recall 读取行数；`0` 表示单次读取全部。非 `0` 时会分批读取完整 recall 结果，不作为总量截断。
- `--max-tokens-per-contract 500`

## 典型使用流程

1. 先用 `cargo run --release --features export-snapshot -- export-snapshot ...` 从 PostgreSQL 导出特征快照到 Parquet。
2. 用 `analyze` 跑一个 seed 合约，确认 API 凭证、阈值和输出格式都正常。
3. 再用 `batch` 跑正式批量分析。

## 补充说明

- `--feature-db` 默认是 `:memory:`。如果你希望 DuckDB 状态跨进程保留，请传文件路径。
- `--signal-cache-db` 默认是 `:memory:`。如果你希望 transfers / owners 的 signal cache 跨运行保留，请传文件路径。
- 如果不传 `--feature-parquet`，程序会假设 DuckDB 特征库里已经有可用数据集。
- 如果同时传了 `--feature-db` 和 `--feature-parquet`，且 `feature-db` 中该链已经有当前版本数据，则会复用 `feature-db`；如果没有该链数据，才从 Parquet 导入。旧版本 `feature-db` / 旧快照缺少预计算列会直接报错，需要重新运行 `export-snapshot`。
- 当前快照 schema 强制包含 `metadata_json`、`token_uri_norm`、`image_uri_norm`、`name_norm`。metadata 文档不再持久化，召回和最终复核都从 `metadata_json` 派生；SQL recall 会先用规范化 URI/name 列做精确召回，metadata recall 则在 Rust 侧从 `metadata_json` 构建 sketch/source candidate 和 BM25 prefilter。
- duplicate scoring 使用合约级聚合：查重阶段每个候选合约只用代表 token 评分，BM25 metadata scoring 会复用缓存的 token、term frequency 和文档长度；合约命中后，分析阶段会通过 Alchemy `getNFTsForContract` 拉取该合约下全量 NFT，用于 NFT 级报告、地址和交易统计。
- `batch` 按 seed 流式调度：每个 seed 依次经过 seed context 网络阶段、DuckDB recall / duplicate scoring CPU 阶段、候选合约网络分析阶段；不同 seed 可以在这些阶段之间错峰执行，不再等待同一批 seed 收齐后才进入 load 阶段。
- `batch` 的资源在整个进程内全局复用：API client、HTTP semaphore、DuckDB feature store、signal cache 不按并发槽位复制，避免重复占用内存。
- `batch` 的并发参数都是全局限制：`--seed-network-max-concurrency` 控制同时参与 seed 级网络阶段的 seed 数，`--seed-cpu-max-concurrency` 控制同时参与 DuckDB recall / duplicate scoring 的 seed 数，`--api-max-concurrency` 控制全局 HTTP 请求并发，`--seed-metadata-max-concurrency` 控制同时下载 seed 合约 metadata 的 seed 数，`--contract-max-concurrency` 控制全局候选合约分析并发，`--sale-metric-max-concurrency` 控制全局 sale metric 并发。默认 `--seed-metadata-max-concurrency 1` 和 `--seed-cpu-max-concurrency 1`，避免多个 seed 同时前置下载 metadata 或同时打满 DuckDB / Rayon CPU。
