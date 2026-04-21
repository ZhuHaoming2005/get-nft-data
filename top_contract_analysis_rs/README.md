# top_contract_analysis_rs

这是 NFT 重复合约分析流程的 Rust 实现。当前二进制支持 3 个子命令：

- `analyze`：分析单个 seed 合约，输出 JSON 报告和 Markdown 报告
- `batch`：从文本文件读取多个 seed 合约，批量分析并输出单合约报告和汇总报告
- `export-snapshot`：从 PostgreSQL 导出特征快照到 Parquet，供本地 DuckDB 分析使用

## 运行要求

- Rust stable 工具链
- 运行 `analyze` / `batch` 时，需要能访问 Alchemy、Etherscan、OpenSea
- 运行 `export-snapshot` 时，需要能访问 PostgreSQL

项目使用了 bundled DuckDB，不需要单独安装 DuckDB。

## 目录结构

- `src/main.rs`：CLI 入口
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

## 环境变量

先复制 [.env.example](./.env.example)，再按实际环境填写。

注意：程序本身**不会自动加载** `.env` 文件。你需要先把变量导入当前 shell。Bash 示例：

```bash
set -a
source .env
set +a
```

其中：

- `export-snapshot` 会直接读取以下环境变量：
  - `DB_HOST`
  - `DB_PORT`
  - `DB_NAME`
  - `DB_USER`
  - `DB_PASS`
  - `DB_CONNECT_TIMEOUT`
- `analyze` / `batch` 不直接读 API key 环境变量，而是通过 CLI 参数传入。`.env.example` 里的 `ALCHEMY_API_KEY` 等变量主要用于命令行插值。

## 命令说明

### 1. 导出特征快照

把 PostgreSQL 中的 NFT 特征快照导出为 Parquet 文件。

```bash
cargo run --release -- export-snapshot \
  --chain ethereum \
  --output ../output/top_contract_analysis/ethereum.parquet
```

可选参数：

- `--fetch-size 100000`
- `--keep-metadata-json`

### 2. 分析单个 Seed 合约

分析一个 seed 合约，并默认输出：

- `result/top_contract_analysis__<seed>.json`
- `result/top_contract_analysis__<seed>.md`

如果传了 `--output`，JSON 会写到该路径，Markdown 会写到同目录、同 basename 的 `.md` 文件。

示例：

```bash
cargo run --release -- analyze \
  --chain ethereum \
  --seed-contract-address 0xed5af388653567af2f388e6224dc7c4b3241c544 \
  --alchemy-api-key "$ALCHEMY_API_KEY" \
  --etherscan-api-key "$ETHERSCAN_API_KEY" \
  --opensea-api-key "$OPENSEA_API_KEY" \
  --feature-parquet ../output/top_contract_analysis/ethereum.parquet \
  --feature-db ../output/top_contract_analysis/features.duckdb \
  --signal-cache-db ../output/top_contract_analysis/signals.duckdb
```

常用参数：

- `--alchemy-network eth-mainnet`
- `--name-threshold 95`
- `--metadata-threshold 0.55`
- `--timeout 60`
- `--max-tokens-per-contract 500`
- `--max-recall-rows 100000`
- `--api-max-concurrency 8`
- `--contract-max-concurrency 4`
- `--sale-metric-max-concurrency 4`
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
  --alchemy-api-key "$ALCHEMY_API_KEY" \
  --etherscan-api-key "$ETHERSCAN_API_KEY" \
  --opensea-api-key "$OPENSEA_API_KEY" \
  --feature-parquet ../output/top_contract_analysis/ethereum.parquet \
  --feature-db ../output/top_contract_analysis/features.duckdb \
  --signal-cache-db ../output/top_contract_analysis/signals.duckdb \
  --output-dir ../result \
  --workers 4
```

批量输出包括：

- 每个 seed 合约各自的 JSON + Markdown 报告
- `top_contract_analysis__summary.json`
- `top_contract_analysis__summary.md`

常用参数：

- `--timeout 30`
- `--workers 4`
- `--strict-parquet`
- `--max-recall-rows 100000`
- `--max-tokens-per-contract 500`

## 典型使用流程

1. 先用 `export-snapshot` 从 PostgreSQL 导出特征快照到 Parquet。
2. 用 `analyze` 跑一个 seed 合约，确认 API 凭证、阈值和输出格式都正常。
3. 再用 `batch` 跑正式批量分析。

## 补充说明

- `--feature-db` 默认是 `:memory:`。如果你希望 DuckDB 状态跨进程保留，请传文件路径。
- `--signal-cache-db` 默认是 `:memory:`。如果你希望 transfers / owners 的 signal cache 跨运行保留，请传文件路径。
- 如果不传 `--feature-parquet`，程序会假设 DuckDB 特征库里已经有可用数据集。
- `batch` 当前内部构造 API client 时使用固定的请求并发默认值；批量运行时 CLI 暴露出来的主要吞吐控制参数是 `--workers`。
