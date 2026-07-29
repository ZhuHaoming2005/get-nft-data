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

程序不设置内存预算或内存上限，不会因内存估算切换算法、降低并行度、跳过数据或输出内存警告。索引仍使用紧凑整数、字符串驻留、分块临时缓冲和流式候选评分来减少实际内存占用；若物理内存不足，由系统分配失败直接终止。

进度默认 `auto`（TTY 用人类可读格式，否则 JSON Lines），含 EWMA ETA。

## 输出

- `summary.csv`
- `chain_matrix.csv`
- `run_manifest.json`
- `name_summary.csv` / `name_chain_matrix.csv`（显式指定 `--name-threshold` 且 Name 阶段完成后提交）
- `uri_summary.csv` / `uri_chain_matrix.csv`（URI 阶段完成后立即提交）

设计说明：`docs/superpowers/specs/2026-07-18-dedup2-experimental-design.md`
