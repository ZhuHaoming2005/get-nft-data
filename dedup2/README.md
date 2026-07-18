# dedup2 — 实验级全内存 NFT 去重

独立 Rust 工作区（`dedup_core` + `dedup_cli`，二进制 `dedup2`），按 `docs/REWRITE_DESIGN.md` / `REWRITE_ARCHITECTURE.md` 实现 Name / URI / Metadata 查重。执行模型：Arrow 直扫 Parquet → 边扫边建内存实体 → 内存计算。无配置文件、无外存 spill、无断点恢复。

## 构建

```bash
cargo build --release --manifest-path dedup2/Cargo.toml
```

## 运行

```bash
./dedup2/target/release/dedup2 all \
  --input base.parquet \
  --input ethereum.parquet \
  --output-dir ./out \
  --chains base,ethereum \
  --evm-chains base,ethereum
```

进度默认 `auto`（TTY 用人类可读格式，否则 JSON Lines），含 EWMA ETA。

## 输出

- `summary.csv`
- `chain_matrix.csv`
- `run_manifest.json`

设计说明：`docs/superpowers/specs/2026-07-18-dedup2-experimental-design.md`
