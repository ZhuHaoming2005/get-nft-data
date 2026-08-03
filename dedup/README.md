# dedup — 全内存 NFT 去重

独立 Rust 工作区（`dedup_core` + `dedup_cli`，二进制 `dedup`），按 `docs/REWRITE_DESIGN.md` / `REWRITE_ARCHITECTURE.md` 实现 Name / URI / Metadata 查重。执行模型：Arrow 直扫 Parquet → 边扫边建内存实体 → 内存计算。无配置文件、无外存 spill、无断点恢复。

## 构建

```bash
cargo build --release --manifest-path dedup/Cargo.toml
```

## 运行

```bash
./dedup all \
  --input ./data/base.parquet \
  --input ./data/polygon.parquet \
  --input ./data/ethereum.parquet \
  --input ./data/solana.parquet \
  --output-dir ./out \
  --chains base,ethereum,polygon,solana \
  --evm-chains base,ethereum,polygon
```

默认不执行 Name 查重；只有显式传入 `--name-threshold <百分比>` 时才执行。默认不限制 Metadata anchor 数量，所有具有有效 Metadata 的 NFT 都参与查重；如需主动限制，显式传入 `--metadata-anchors <数量>`。

抽样默认关闭，不加载抽样所需的图片 URI，也不发起图片网络请求。显式传入 `--sample-pairs <正整数>` 后，Name 与 Metadata 的合约对抽样才作为一个整体启用。Name 导出有界的已确认重复合约对；Metadata 通过一次查重扫描保留有界候选池，并且只有实际比较 token 的两张媒体均成功下载的合约对，才会同时写入 `metadata_duplicate_pairs*.csv`、`metadata_image_samples.csv` 和 `metadata_sample_images/`。任意一侧下载失败会永久舍弃整对，已成功下载的对会在同一临时集合中保留并继续用后续候选补足；成功运行时最终恰好得到 `--sample-pairs N` 对完整媒体。候选池默认是 `max(N*16, N+256)`，可用 `--sample-candidate-limit` 显式调整；候选耗尽仍不足 N 时，命令会在正常写出查重统计后明确报错，且不会发布本轮不完整的媒体样本，不会反复执行 Metadata 查重或扩张到全部合约对。

Parquet 中由 `top_contract_analysis_rs` 导出的规范化 URI 会在请求前恢复：`ipfs:<cid/path>` 映射到 IPFS 网关，`ar:<tx/path>` 映射到 Arweave 网关，普通 HTTP(S) URI 直接使用。下载器同时兼容未规范化的 `ipfs://`、`ar://` 和 base64 `data:image/...`，图片不转码。HTTP(S) 请求禁用代理和自动重定向，只允许解析到公网地址的目标；每次重定向都会重新执行相同检查，拒绝 loopback、私网、link-local 和其他非公网地址。

```bash
./dedup run-metadata \
  --input ./data/ethereum.parquet \
  --output-dir ./out \
  --chains ethereum \
  --evm-chains ethereum \
  --sample-pairs 100
```

程序不设置内存预算或内存上限，不会因内存估算切换算法、降低并行度、跳过数据或输出内存警告。索引仍使用紧凑整数、字符串驻留、分块临时缓冲和流式候选评分来减少实际内存占用；若物理内存不足，由系统分配失败直接终止。

使用 `--threads <正整数>` 指定全链路 Rayon 工作线程数；省略时使用系统默认并行度。进度默认 `auto`（TTY 用人类可读格式，否则 JSON Lines），含 EWMA ETA。

## Linux SMT 对比测试

`scripts/compare_smt.py` 可在 SMT 开启的 Linux 目标机上对比“每个物理核一个 worker”和“使用全部 SMT siblings”。脚本读取当前 cpuset 与 sysfs CPU 拓扑，通过 `taskset` 固定 CPU，交替运行两种模式，并从 `run_manifest.json` 比较 `direct_bm25` 中位耗时。两种模式的输出互相隔离；至少相差 2% 才给出明确推荐。

```bash
python3 scripts/compare_smt.py \
  --binary ./target/release/dedup \
  --output-root ./smt-benchmark \
  --repetitions 1 \
  -- run-metadata \
  --input ./data/base.parquet \
  --input ./data/ethereum.parquet \
  --chains base,ethereum \
  --evm-chains base,ethereum \
  --sample-pairs 0
```

脚本自动注入 `--threads` 和 `--output-dir`，不要在 `--` 后重复传入。建议先使用保持真实 Metadata/anchor 分布的代表性子集；需要降低运行波动时使用 `--repetitions 2` 或 `3`。结果写入 `smt-benchmark/smt_comparison.json`。如果阿里云已经关闭或隐藏 SMT，脚本会明确退出，因为此时没有两组可比较的逻辑 CPU。

## 输出

- `summary.csv`
- `chain_matrix.csv`
- `run_manifest.json`
- `name_summary.csv` / `name_chain_matrix.csv`（显式指定 `--name-threshold` 且 Name 阶段完成后提交）
- `name_duplicate_pairs.csv`（Name 全链汇总随机样本）
- `name_duplicate_pairs_intra_chain.csv` / `name_duplicate_pairs_chain_matrix.csv` / `name_duplicate_pairs_cross_chain_summary.csv`
- `uri_summary.csv` / `uri_chain_matrix.csv`（URI 阶段完成后立即提交）
- `metadata_duplicate_pairs.csv`（双方实际比较 token 均有图片 URI 的 Metadata 全链样本）
- `metadata_duplicate_pairs_intra_chain.csv` / `metadata_duplicate_pairs_chain_matrix.csv` / `metadata_duplicate_pairs_cross_chain_summary.csv`
- `metadata_image_samples.csv`（最终成功样本的实际 Metadata 比较 token、图片 URI 与下载路径；仅显式抽样时生成）
- `metadata_sample_images/<序号>/<序号>a.<原扩展名>` / `<序号>b.<原扩展名>`（仅成功下载时生成）

设计说明：`docs/superpowers/specs/2026-07-18-dedup2-experimental-design.md`
