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
- `src/store`：DuckDB 特征库、Postgres 快照导出
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
  --max-recall-rows 30000000 \
  --alchemy-api-max-concurrency 16 \
  --other-api-max-concurrency 3 \
  --matched-contract-max-concurrency 16 \
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
- `--alchemy-api-max-concurrency 12`：Alchemy 请求全局并发上限。
- `--other-api-max-concurrency 4`：OpenSea、Etherscan、ETH/USD 等非 Alchemy 请求的速率桶 burst 上限，默认 4；参数值优先。旧参数 `--api-max-concurrency` 仍作为兼容别名。
- `--other-api-rate-limit-refill-ms 300`：非 Alchemy 请求速率桶补充间隔，默认每 300ms 补充 1 个请求 token。
- `--matched-contract-max-concurrency 4`：matched contract 分析阶段的合约级全局并发上限。
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
  --output-dir ./result \
  --max-recall-rows 30000000 \
  --alchemy-api-max-concurrency 16 \
  --other-api-max-concurrency 3 \
  --matched-contract-max-concurrency 16 \
  --seed-network-max-concurrency 1 \
  --seed-cpu-max-concurrency 1 \
  --duckdb-memory-limit 50GB
```

批量输出包括：

- 每个 seed 合约各自的 JSON + Markdown 报告
- `top_contract_analysis__summary.json`
- `top_contract_analysis__summary.md`

常用参数：

- `--timeout 30`
- `--seed-network-max-concurrency 4`：同时处于 seed context 网络 IO 阶段的 seed 合约数。
- `--alchemy-api-max-concurrency 8`：Alchemy 请求全局并发上限。
- `--other-api-max-concurrency 4`：OpenSea、Etherscan、ETH/USD 等非 Alchemy 请求的速率桶 burst 上限，默认 4；参数值优先。旧参数 `--api-max-concurrency` 仍作为兼容别名。
- `--other-api-rate-limit-refill-ms 300`：非 Alchemy 请求速率桶补充间隔，默认每 300ms 补充 1 个请求 token。
- `--matched-contract-max-concurrency 4`：matched contract 分析阶段的合约级全局并发上限，跨 seed 共享。
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
- HTTP API 默认最多请求 5 次；429、408、5xx 和网络错误会重试，每次重试之间等待 500ms。400 等非临时客户端错误不会等待重试。
- 如果不传 `--feature-parquet`，程序会假设 DuckDB 特征库里已经有可用数据集。
- 如果同时传了 `--feature-db` 和 `--feature-parquet`，且 `feature-db` 中该链已经有当前版本数据，则会复用 `feature-db`；如果没有该链数据，才从 Parquet 导入。旧版本 `feature-db` / 旧快照缺少预计算列会直接报错，需要重新运行 `export-snapshot`。
- 当前快照 schema 强制包含 `metadata_json`、`token_uri_norm`、`image_uri_norm`、`name_norm`。metadata 文档不再持久化，召回和最终复核都从 `metadata_json` 派生；SQL recall 会先用规范化 URI/name 列做精确召回，metadata recall 则在 Rust 侧从 `metadata_json` 构建 sketch/source candidate 和 BM25 prefilter。
- duplicate scoring 使用合约级聚合：查重阶段每个候选合约只用代表 token 评分，BM25 metadata scoring 会复用缓存的 token、term frequency 和文档长度；合约命中后，分析阶段会通过 Alchemy `getNFTsForContract` 拉取该合约下全量 NFT，用于 NFT 级报告、地址和交易统计。
- `batch` 按 seed 流式调度：每个 seed 依次经过三阶段：seed context 网络 IO 阶段、DuckDB recall / duplicate scoring CPU 阶段、matched contract 分析阶段。第三阶段不占用 seed 级网络或 CPU 槽位，因此某个 seed 正在分析 matched contracts 时，其他 seed 仍可进入第一、第二阶段；matched contract 合约级分析跨 seed 共享 `--matched-contract-max-concurrency`，不同 seed 下的 matched 合约可以同时执行。
- `batch` 的资源在整个进程内全局复用：API client、HTTP semaphore、DuckDB feature store 不按并发槽位复制，避免重复占用内存。
- `batch` 的并发和速率参数都是全局限制：`--seed-network-max-concurrency` 控制同时参与第一阶段网络 IO 的 seed 数，`--seed-cpu-max-concurrency` 控制同时参与第二阶段 DuckDB recall / duplicate scoring 的 seed 数，`--matched-contract-max-concurrency` 控制第三阶段合约级分析并发，`--alchemy-api-max-concurrency` 控制 Alchemy HTTP 并发，`--other-api-max-concurrency` 控制其它 HTTP 请求的速率桶 burst 上限（默认 4，参数值优先），`--other-api-rate-limit-refill-ms` 控制补充间隔（默认 300ms，每次补 1 个 token）。sale metric 和 mint value-flow 不再有单独并发参数。默认 `--seed-cpu-max-concurrency 1`，避免多个 seed 同时打满 DuckDB / Rayon CPU。
