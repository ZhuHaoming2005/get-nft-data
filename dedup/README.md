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

查重和抽样是两个独立入口。`all`、`run-name`、`run-uri`、`run-metadata` 不接受 `--sample-pairs`，不会加载仅供抽样使用的图片 URI，也不会发起图片网络请求。快速抽样必须使用独立的 `sample-metadata` 命令；它不生成完整查重统计，而是先构建与完整 Metadata 查重相同的无损倒排候选索引，再随机访问 left profile 及其候选任务，单链池和跨链池各取得 `--sample-pairs N` 对完整媒体后立即停止。该语义是“随机候选搜索后取前 N”，不是遍历所有有效对后的严格均匀抽样。任意一侧下载失败会永久舍弃该合约对并继续搜索，两个池都达到 N 才发布结果；候选空间耗尽仍不足时返回错误，并保留此前已经发布的完整样本集。

Parquet 中由 `top_contract_analysis_rs` 导出的规范化 URI 会在请求前恢复：`ipfs:<cid/path>` 映射到 IPFS 网关，`ar:<tx/path>` 映射到 Arweave 网关，普通 HTTP(S) URI 直接使用。下载器同时兼容未规范化的 `ipfs://`、`ar://` 和 base64 `data:image/...`，图片不转码。HTTP(S) 请求禁用代理和自动重定向，只允许解析到公网地址的目标；每次重定向都会重新执行相同检查，拒绝 loopback、私网、link-local 和其他非公网地址。每轮快速抽样从操作系统取得独立的 256 位随机密钥，域分离 SHA-256 驱动 profile、候选任务和匹配 NFT 对的无放回随机访问；候选索引内部仍会排序以支持快速无损查询，但排序结果不决定抽样顺序，也不按 token、内部合约 ID 或合约使用次数决定入选。只有至少存在一个图片 witness 的 profile 才进入随机候选搜索；Solana 等单个逻辑合约包含多个 NFT 时，会先为该 `(chain, contract_address)` 真随机选择一个实际 Metadata 匹配 witness，再按不同逻辑合约枚举，避免同合约 NFT 形成二次方无效候选。图片 witness 按 profile/anchor 连续存储，评分批次只传递轻量标识；最多 32 个媒体对由独立下载线程池并行验证并分别补入单链/跨链池。同一 `(chain, contract_address)` 可以出现在不同合约对中。

最终 `2N` 对会再次使用操作系统随机源进行无偏洗牌。单链样本写入 `metadata_sample_images/intra_chain/<池内序号>/`，跨链样本写入 `metadata_sample_images/cross_chain/<池内序号>/`；每个目录除两张原格式媒体外，还包含 `<池内序号>a.json` 和 `<池内序号>b.json`，记录池类型、链、合约地址、token ID、图片 URI、媒体文件名及实际参与比较的 Metadata。`metadata_image_samples.csv` 的 `row` 是全局洗牌顺序，`pool` 和 `pool_row` 标识对应目录。媒体目录、manifest 和四份合约对 CSV 作为一个事务提交；如果进程在替换多个旧输出期间被强制终止，下次启动 `sample-metadata` 会从 `.metadata-fast-sample-*` 事务记录回滚未完成提交，或清理已经完成但未清理的事务目录。Ctrl+C 会停止后续评分、请求和重试，并在当前在途请求退出后结束。

```bash
./dedup sample-metadata \
  --input ./data/base.parquet \
  --input ./data/polygon.parquet \
  --input ./data/ethereum.parquet \
  --input ./data/solana.parquet \
  --output-dir ./out \
  --chains base,ethereum,polygon,solana \
  --evm-chains base,ethereum,polygon \
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
  --evm-chains base,ethereum
```

脚本自动注入 `--threads` 和 `--output-dir`，不要在 `--` 后重复传入。建议先使用保持真实 Metadata/anchor 分布的代表性子集；需要降低运行波动时使用 `--repetitions 2` 或 `3`。结果写入 `smt-benchmark/smt_comparison.json`。如果阿里云已经关闭或隐藏 SMT，脚本会明确退出，因为此时没有两组可比较的逻辑 CPU。

## 输出

- `summary.csv`
- `chain_matrix.csv`
- `run_manifest.json`
- `name_summary.csv` / `name_chain_matrix.csv`（显式指定 `--name-threshold` 且 Name 阶段完成后提交）
- `uri_summary.csv` / `uri_chain_matrix.csv`（URI 阶段完成后立即提交）
- `metadata_duplicate_pairs.csv`（仅 `sample-metadata`；共 `2N` 对）
- `metadata_duplicate_pairs_intra_chain.csv` / `metadata_duplicate_pairs_chain_matrix.csv` / `metadata_duplicate_pairs_cross_chain_summary.csv`
- `metadata_image_samples.csv`（仅 `sample-metadata`；最终成功样本的实际 Metadata 比较 token、图片 URI 与下载路径）
- `metadata_sample_images/intra_chain/<池内序号>/<池内序号>a.<原扩展名>` / `<池内序号>b.<原扩展名>`（单链成功样本）
- `metadata_sample_images/cross_chain/<池内序号>/<池内序号>a.<原扩展名>` / `<池内序号>b.<原扩展名>`（跨链成功样本）

设计说明：`docs/superpowers/specs/2026-07-18-dedup2-experimental-design.md`
