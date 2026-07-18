# NFT 跨链去重独立工作区

本目录为独立 Rust 生产工作区。正式运行目标为 64 位 Linux；所有命令均在 `dedup/` 下执行。

配置文件中的路径相对于**配置文件所在目录**解析。`input_files` 必须显式按业务顺序列出，不要依赖 glob 或目录枚举顺序。

## 配置

复制 `config/default.toml` 为运行配置（例如 `/etc/nft-dedup/run.toml`），至少填写：

```toml
input_files = ["base.parquet", "ethereum.parquet", "polygon.parquet", "solana.parquet"]
output_dir = "/data/nft-dedup/result"
temporary_volumes = ["/nvme0/nft-dedup", "/nvme1/nft-dedup"] # 必须是本地盘
chains = ["base", "ethereum", "polygon", "solana"]
evm_chains = ["base", "ethereum", "polygon"]
memory_limit = 0 # 0 = 取 cgroup 与物理内存的较小值
```

常用调节项（默认值见 `config/default.toml`）：

| 配置项 | 说明 |
|---|---|
| `entity_execution_mode` / `uri_execution_mode` | `auto`、`in_memory`、`hybrid` 或 `external`；生产推荐 `auto` |
| `metadata_execution_mode` | 保留用于配置兼容和运行记录；Metadata 实际固定为 `in_memory` |
| `stage_concurrency` | 各阶段并发；`0` 表示自动确定 |
| `name_threshold` | Name Jaro-Winkler 阈值，默认 `95.0` |
| `metadata_content_threshold` | Metadata BM25 阈值，默认 `0.6` |
| `metadata_anchor_tokens` | 每合约锚点数，默认 `8` |
| `metadata_prefilter_parameters` | LSH `(bands, rows)` 由模板阈值与目标召回确定性推导 |
| `work_budgets` | 精确工作上限，超限返回结构化错误 |
| `quality_gate` | Metadata recall audit 门槛 |

Name 所需合约名和紧凑 posting CSR 全量保留在内存；候选按 left Name 并行生成、去重、过滤并立即评分，不物化全局候选表。Metadata 所需 anchors、templates、prefilter evidence、候选和比较索引也全量保留在内存，并在同一进程的验证与 recall audit 间复用。预测峰值超出阶段预算时只记录警告并继续，不切换外存模式。

## 生产编译

```bash
cargo build --release --locked --bin dedup
```

逻辑实体 ID 可能超过 `u32` 时：

```bash
cargo build --release --locked --features wide_ids --bin dedup
```

## 推荐运行

交互式终端：

```bash
./target/release/dedup \
  --config /etc/nft-dedup/run.toml \
  --progress tty \
  --progress-interval-ms 1000 \
  all
```

systemd、容器或日志采集环境：

```bash
./target/release/dedup \
  --config /etc/nft-dedup/run.toml \
  --progress json \
  --progress-interval-ms 1000 \
  all
```

`--progress auto` 为默认值；终端使用 `tty`，否则使用 JSON Lines。进度包含阶段、完成量、吞吐、RSS、NUMA 调度量和 EWMA ETA；未建立数据页归属证明时远端访问显示为 `unknown`。至少 3 个正吞吐样本后 `eta_confident=true`，总量未知时 ETA 为 `null`。

最新快照和历史分别写入 `output_dir/run/progress.json` 与 `progress.jsonl`。

## 分阶段执行与恢复

`all` 顺序为：

```text
build-entities → run-name → run-uri → run-metadata → audit-metadata → report
```

单独执行时，将 `all` 替换为相应子命令。带合法 `_SUCCESS` 且摘要一致的阶段会复用；中断后重跑同一条命令即可继续。`SIGTERM` 或第一次 `SIGINT` 受控退出，第二次 `SIGINT` 立即以 `130` 退出。

同一 `output_dir` 同时只允许一个进程运行。Entity/URI 的外存文件按配置摘要隔离在本地临时卷的 `dedup-spill/<digest>/<stage>` 下；Name/Metadata 不写外存中间数据。

## 结果

主要产物位于 `output_dir`：

- `summary.csv`
- `chain_matrix.csv`
- `run_manifest.json`
- `hardware_profile.json`
- `data_quality.json`
- `recall_audit.json`
- `stage_metrics.json`

各阶段指标、在线进度和资源计划位于 `output_dir/run`。

发布前推荐运行：

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test --workspace --all-targets --features wide_ids
```
