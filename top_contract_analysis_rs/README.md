# top_contract_analysis_rs

这是 NFT 重复合约分析流程的 Rust 实现。生产运行把大体量特征导入和只读分析拆开：

- `prepare-features`：把一个或多个 Parquet 原子导入文件 DuckDB，并构建 URI/name/metadata 召回数据
- `analyze`：分析单个 seed 合约，输出 JSON 报告和 Markdown 报告
- `batch`：从 `chain,address` CSV 读取多个 seed 合约，执行固定四链矩阵分析
- `export-snapshot`（需 `export-snapshot` feature）：从 PostgreSQL 导出特征快照到 Parquet

## 运行要求

- Rust stable 工具链
- 运行 `analyze` / `batch` 时，需要能访问 Alchemy、Etherscan、OpenSea；四链 `batch` 还需要 Helius
- 运行 `export-snapshot` 时，需要能访问 PostgreSQL

项目使用 bundled DuckDB，不需要单独安装 DuckDB。默认资源参数按 `128 vCPU / 512 GiB RAM` 设备设置：`prepare-features` 使用 96 个 DuckDB 线程、96 个 Rayon 线程和 `300GB` DuckDB 内存；`analyze` / `batch` 使用 64 个 DuckDB 线程、96 个 Rayon 线程、`96GB` DuckDB 内存、2 个只读连接和 `260GB` Rust 召回索引缓存。两个连接会平分 DuckDB 线程与内存，不会把总预算翻倍。默认 snapshot bulk 每批最多合并 2 个 seed，并把“装载、候选构建”期间的完整快照硬限制为 2 个；候选生成后立即释放 metadata 与 prepared 投影，再用独立的 `48GB` 全局 compact-plan 字节许可覆盖 provider 分析。按 2×24GB 完整快照、48GB compact plan、16GB name scratch、96GB DuckDB 与 260GB 索引计算，保守最坏包络约为 468GB 十进制（约 436GiB），剩余约 76GiB 给 API、序列化、分配器、运行时和操作系统。CLI 会在打开数据库前拒绝超过目标机安全预算的组合。所有参数均可覆盖；较小机器仍需主动下调。

DuckDB 与 Rayon 线程预算相互独立。`--physical-cores N` 非 0 时把 DuckDB 上限收紧为 `min(--duckdb-threads, N)`，并把 Rayon 固定为 `N`；默认 64/96 是按目标设备给出的理论高吞吐配置，不是针对具体数据分布实测得到的极值。

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
- `--start-block 19000000`：仅导出 `first_seen_block >= 19000000` 的记录（包含边界）
- `--end-block 20000000`：仅导出 `first_seen_block <= 20000000` 的记录（包含边界）

例如，导出一个闭区间内的 Ethereum 快照：

```bash
cargo run --release --features export-snapshot -- export-snapshot \
  --chain ethereum \
  --start-block 19000000 \
  --end-block 20000000 \
  --output ../output/top_contract_analysis/ethereum-19000000-20000000.parquet
```

`--start-block` / `--end-block` 目前只适用于 EVM 链。当前 Solana 采集记录的
`first_seen_block` 是占位值 `0`，因此 Solana 导出在传入任一范围参数时会明确报错；
不传范围参数时可正常导出完整 `nft_assets_solana` 快照。Solana collection 和 mint
地址在 Parquet 中保留 Base58 大小写，EVM 合约地址仍规范化为小写。

### 2. 准备生产特征库

大 Parquet 必须先显式导入文件数据库。四链文件可以一次传入；跨文件重复的 `(chain, contract_address, token_id)` 会统一去重，优先保留非空字段更多、metadata 更完整的行，再按字段字典序稳定决胜，结果不依赖参数顺序。

```bash
cargo run --release -- prepare-features \
  --feature-parquet ../output/top_contract_analysis/ethereum.parquet \
  --feature-parquet ../output/top_contract_analysis/base.parquet \
  --feature-parquet ../output/top_contract_analysis/polygon.parquet \
  --feature-parquet ../output/top_contract_analysis/solana.parquet \
  --feature-db ../output/top_contract_analysis/features.duckdb
```

导入使用非物化输入视图和单份去重 staging 表。权威快照按本次输入所含链原地全量替换，并在导入事务中同时提交 generation 与 prepare journal；之后每条链的派生召回表分别事务提交和 checkpoint，最后统一构建索引。输入文件按内容 SHA-256 指纹，文件在哈希期间变化会被拒绝。进程中断后可运行下列命令从最后一个已提交阶段继续，不会重复已提交的权威导入：

```bash
cargo run --release -- prepare-features \
  --prepare-only \
  --feature-db ../output/top_contract_analysis/features.duckdb
```

若要放弃未完成 journal 并用新的完整输入重新开始，显式传 `--restart-prepare`。journal 会保存最近一次主错误。`analyze` / `batch` 只读打开数据库；若 generation、格式/算法/构建身份或 prepared 数据缺失、过期，会明确要求重新准备，不会在首个 seed 上偷偷重建全链数据。

生产路径必须使用文件数据库。仅小型 fixture 可显式传 `--feature-db :memory: --allow-in-memory-feature-db`。导入还需要足够的本地临时磁盘空间；DuckDB 临时目录默认位于数据库旁的 `<db>.tmp`。

常用参数：

- `--duckdb-threads 96`
- `--rayon-threads 96`
- `--duckdb-memory-limit 300GB`
- `--physical-cores N`：非 0 时限制 DuckDB 并覆盖 Rayon 线程数

### 3. 分析单个 Seed 合约

分析一个 seed 合约，并默认输出：

- `result/top_contract_analysis__<seed>.json`
- `result/top_contract_analysis__<seed>.md`

如果传了 `--output`，JSON 会写到该路径，Markdown 会写到同目录、同 basename 的 `.md` 文件。

示例：

```bash
cargo run --release -- analyze \
  --chain ethereum \
  --seed-contract-address 0xBd3531dA5CF5857e7CfAA92426877b022e612cf8 \
  --alchemy-api-key "$ALCHEMY_API_KEY" \
  --etherscan-api-key "$ETHERSCAN_API_KEY" \
  --opensea-api-key "$OPENSEA_API_KEY" \
  --feature-db ../output/top_contract_analysis/features.duckdb
```

常用参数：

- `--alchemy-network eth-mainnet`
- `--name-threshold 95`
- `--metadata-threshold 0.6`
- `--timeout 60`
- `--max-tokens-per-contract 200`：`0` 表示不限制；生产默认限制每个合约进入后续 metadata/payload 阶段的 token 数。
- `--max-recall-rows 100000`：payload 读取批大小；`0` 使用内部生产批大小 500000。该参数不截断总召回结果。
- `--max-candidate-contracts-per-seed 100000`：完整 URI/name/metadata 召回合并后的候选合约硬上限；在大字段读取前校验。
- `--max-selected-rows-per-seed 2000000`：确定性合约内 token 排序与上限应用后的总行数硬上限。
- `--max-snapshot-bytes-per-seed 24GB`：最终入选 payload 的保守常驻内存上限；DuckDB 在 Arrow/Rust 装载大字段前预估并拒绝超限任务。
- `--alchemy-api-max-concurrency 16`：Alchemy 请求全局并发上限。账号/环境需能承受 16 路并发；否则请调低该参数。
- `--other-api-max-concurrency 4`：OpenSea、Etherscan、ETH/USD 等非 Alchemy 请求的速率桶 burst 上限，默认 4；参数值优先。
- `--other-api-rate-limit-refill-ms 300`：非 Alchemy 请求速率桶补充间隔，默认每 300ms 补充 1 个请求 token。
- `--matched-contract-max-concurrency 8`：matched contract 分析阶段的合约级全局并发上限。
- `--paper-min-cycle-size 2`：Wash Trading SCC / cycle 的最小节点数。
- `--paper-min-path-length 3`：Layered Transfer 的最小路径钱包数。
- `--paper-center-fanout-threshold 3`：Sybil / Fraud / Poisoning 与 Inventory Concentration 的中心 fan-out 阈值。
- `--paper-concentration-top-pct 0.1`：攻击投入和诚实损失集中度的前百分比合约口径。
- `--paper-analysis-timestamp 0`：时间相关统计的 Unix 时间；`0` 表示本次运行开始时间。指定固定值可获得可复现结果。
- `--duckdb-threads 64`
- `--rayon-threads 96`
- `--duckdb-read-connections 2`：平分总 DuckDB 线程和内存预算。
- `--duckdb-memory-limit 96GB`：所有只读连接合计预算。
- `--recall-index-memory-limit 260GB`：name/metadata Rust 常驻索引合计预算；按字节 LRU 淘汰。
- `--output ./result/azuki.json`

### 4. 批量分析 Seed 合约

`batch` 固定读取带表头的 `chain,address` CSV。支持链为 `ethereum`、`base`、`polygon`、`solana`；EVM 地址规范化为小写，Solana Base58 地址保留大小写。非法地址、不支持的链和规范化后的重复 `(chain,address)` 会被拒绝。

```csv
chain,address
ethereum,0xed5af388653567af2f388e6224dc7c4b3241c544
base,0x1111111111111111111111111111111111111111
polygon,0x2222222222222222222222222222222222222222
solana,So11111111111111111111111111111111111111112
```

可先按链独立抓取 Top 合约种子：Ethereum、Base、Polygon 使用 OpenSea 30 天成交量榜，Solana 使用 Magic Eden 30 天热门集合榜。默认四链各取 100 个去重地址，只有四条链都完整获得 100 个时才原子替换正式 CSV 和审计 JSON：

```bash
python scripts/fetch_opensea_top_seeds.py \
  --api-key "2d17a25e68714720883ac996f5459b17" \
  --helius-api-key "$HELIUS_API_KEY" \
  --limit 100 \
  --contracts-output ./seeds/top_contracts.csv \
  --audit-output ./seeds/top_contracts.audit.json
```

Magic Eden 热门榜只返回 collection symbol；脚本会从 listing（无 listing 时回退到 activity）抽取一个 NFT mint，再通过 Helius `getAsset` 解析认证 collection address。因此只要包含 `solana`，就必须传入 `--helius-api-key` 或设置 `HELIUS_API_KEY`；仅抓取 EVM 时不需要 Helius key。

运行示例：

```bash
cargo run --release -- batch \
  --seed-file ./seeds/top_contracts.csv \
  --alchemy-api-key "$ALCHEMY_API_KEY" \
  --etherscan-api-key "$ETHERSCAN_API_KEY" \
  --opensea-api-key "$OPENSEA_API_KEY" \
  --helius-api-key "$HELIUS_API_KEY" \
  --alchemy-network ethereum=eth-mainnet \
  --alchemy-network base=base-mainnet \
  --alchemy-network polygon=polygon-mainnet \
  --feature-db ../output/top_contract_analysis/features.duckdb \
  --output-dir ./result \
  --alchemy-api-max-concurrency 16 \
  --helius-api-max-concurrency 16 \
  --helius-rate-limit-refill-ms 100 \
  --max-history-transactions-per-collection 10000
```

批量输出包括：

- 每个 seed 与候选链的 v3 缓存 JSON：`<primary>__<address-utf8-hex>__vs__<secondary>.json`
- `summary.json`：包含 `intra_chain`、`chain_matrix`、`cross_chain_summary` 三类 scope
- `failures.json`：记录 seed、候选链、stage、provider、retryable 和错误信息
- `run-manifest.json`：记录本轮 run ID、分析时间、参数指纹、四链快照身份和 `incomplete|complete` 状态
- `run-metrics.jsonl`：每个 seed 的耗时、缓存命中、取消状态和失败数量；每行写入后执行 data sync

每个 seed 都会在四条快照中召回候选。`scoped_duplicate_scale` 和 `scoped_paper_stats` 均按 primary/secondary chain 分桶；NFT/合约比例始终以 primary chain 的完整快照总数为分母。链矩阵行保留对应候选链的 `native_symbol`，跨链汇总不合并 ETH、POL、SOL 原生金额，只输出可加总的 USD 金额。任何 work unit 失败不会取消其他 seed；失败批次保留 `incomplete` manifest 和已成功的 scoped report。仅当下一次运行的分析参数、规范化 seed 集合与四链快照身份均匹配时，才恢复该未完成批次；一个 seed 的四个 scope 必须全部有效才复用，否则四链整体重算，避免混合不同 provider 时刻的证据。成功批次会标记为 `complete`，后续调用始终开始新一轮并刷新 provider 数据。损坏或不匹配的 manifest 也会安全地开始新一轮。同一输出目录通过 `run.lock` 保证同一时刻只有一个 batch 写入。地址使用全小写十六进制文件名，因此大小写敏感的 Solana 地址在 Windows 文件系统上也不会互相覆盖。

批处理支持两级中断：第一次 `Ctrl+C` 停止调度后续 seed/链，在恢复边界同步 `incomplete` manifest 后以退出码 130 结束；第二次 `Ctrl+C` 可在等待该边界时立即终止。scoped report、manifest、summary 和 failures 均通过同步临时文件后原子替换写入，强制终止后用相同命令即可恢复。

常用参数：

- `--timeout 30`
- `--seed-network-max-concurrency 2`：同时获取 primary-chain seed context 的 seed 数；permit 在 context 完成后立即释放，使后续 seed 可与前一 seed 的 recall/分析阶段重叠。
- `--alchemy-api-max-concurrency 16`：Alchemy 请求全局并发上限。账号/环境需能承受 16 路并发；否则请调低该参数。
- `--other-api-max-concurrency 4`：OpenSea、Etherscan、ETH/USD 等非 Alchemy 请求的速率桶 burst 上限，默认 4；参数值优先。
- `--other-api-rate-limit-refill-ms 300`：非 Alchemy 请求速率桶补充间隔，默认每 300ms 补充 1 个请求 token。
- `--matched-contract-max-concurrency 8`：matched contract 分析阶段的合约级全局并发上限，跨 seed 共享。
- `--helius-api-max-concurrency 4`：Helius DAS/RPC 的进程级 in-flight 请求上限；请求完成即归还 permit，与 Alchemy 使用相同的并发和重试策略。
- `--helius-rate-limit-refill-ms 100`：Helius 独立请求速率桶的 token 补充间隔；与 in-flight semaphore 同时生效。
- `--max-history-transactions-per-asset 0`：每个 Solana asset 的历史交易上限；`0` 表示不截断。
- `--max-history-transactions-per-collection 10000`：单个 Solana collection 最多保留的 asset-signature 引用数。同一签名关联多个资产时按资产分别计费，但交易详情 HTTP 请求仍按签名去重。发现阶段以固定批次按 asset 公平轮询，空页和失败页不预扣预算；达到预算后保留截断与覆盖质量标记。为避免无界引用内存，`0` 或超过 `100000` 的配置均使用 `100000` 的安全硬上限。
- `--alchemy-network chain=network`：可重复指定 EVM 网络覆盖；未指定时自动使用对应主网。
- `--feature-db features.duckdb`：正式四链分析要求已 prepared 的文件库同时包含四条链；Parquet 只能交给 `prepare-features`。
- `--seed-cpu-max-concurrency 2`：同时执行 DuckDB recall / duplicate scoring 的 seed 数。同一 seed 持有一个 permit，并按固定顺序完成四链 plan，不并行扫描本 seed 的四条链。
- `--paper-min-cycle-size 2`：Wash Trading SCC / cycle 的最小节点数。
- `--paper-min-path-length 3`：Layered Transfer 的最小路径钱包数。
- `--paper-center-fanout-threshold 3`：Sybil / Fraud / Poisoning 与 Inventory Concentration 的中心 fan-out 阈值。
- `--paper-concentration-top-pct 0.1`：攻击投入和诚实损失集中度的前百分比合约口径。
- `--paper-analysis-timestamp 0`：时间相关统计使用的 Unix 时间；`0` 在新批次启动时固定为当前时间，恢复匹配的未完成批次时沿用 manifest 中的时间。
- `--refresh-scoped-cache`：忽略匹配的未完成 manifest，强制开始新批次并刷新 provider 数据。
- `--duckdb-threads 64`
- `--rayon-threads 96`
- `--duckdb-read-connections 2`：平分总 DuckDB 线程和内存预算。
- `--duckdb-memory-limit 96GB`：所有只读连接合计预算。
- `--recall-index-memory-limit 260GB`：name/metadata Rust 常驻索引合计预算；四链总量不超过预算时可全部常驻。
- `--max-recall-rows 100000`：payload 读取批大小；`0` 使用内部生产批大小 500000，不作为总量截断。
- `--max-tokens-per-contract 200`
- `--max-candidate-contracts-per-seed 100000`
- `--max-selected-rows-per-seed 2000000`
- `--max-snapshot-bytes-per-seed 24GB`

## 论文统计输出

JSON 报告使用 `schema_version: 2`。对外金额字段由 `*_eth` 破坏性改名为 `*_native` 并携带 `native_symbol`；四链合计只使用 `*_usd`。`analyze` 单合约 JSON 以完整 `paper_stats` 为新版统计出口；`batch` 通过 scoped paper stats 输出单链、链矩阵和跨链统计。不再输出旧版 `report_summary`、`batch_summary`、`seed_reports` 兼容结构。完整 `paper_stats` 覆盖：

- `duplicate_scale`：按 `token_uri`、`image_uri`、`metadata`、`name`、`total` 统计重复 NFT / 合约数量、比例、分子、分母。
- `address_classification`：恶意地址、跨合约重复侵权恶意地址、诚实地址、地址总数。
- `contract_behavior_stats`：单合约 JSON 逐合约输出 Wash Trading、Pump-and-Exit、Sybil/Fraud/Poisoning、Layered Transfer、Inventory Concentration、诚实买家明细，并在每个 match 合约内统计 2、3、4、5+ 节点 wash cycle 数量和比例；汇总 JSON 不包含逐合约明细。
- `malicious_behavior_summary`：按行为类型汇总合约覆盖率、实例占比、涉及地址/NFT、关联买家和可归因买家损失；Pump-and-Exit 以及直接 sale 给诚实买家的星型 Sybil/Fraud/Poisoning 行为会写入 `linked_buyer_count` / `linked_loss`，Layered、Inventory 等未直接关联诚实买家的行为价值保留在合约行为明细的 `total_value` / `value_collected` 中。
- `wash_cycle_size_distribution`：汇总统计 2、3、4、5+ 节点 wash cycle 数量、比例、分子和分母；单合约 JSON 和批量汇总 JSON 都导出。
- `wash_cycle_size_by_contract`：单合约 JSON 按每个 match 合约统计同口径 wash cycle 节点规模；无循环合约也保留 0 值行，汇总 JSON 不导出该逐合约明细。
- `attacker_cost`：Setup / Lure / Exit / Total gas 成本和前百分比合约成本集中度；Setup 统计复制合约部署和资金准备 gas，Lure 统计恶意地址付费 mint、刷量/诱导成交 gas，Exit 统计攻击者卖出、withdrawal、cashout 等退出 gas；同一合约同一交易只计一次，跨阶段重复时按 Exit / Lure / Setup 优先级归入更具体阶段，不把诚实买家支付的 gas 计入攻击者成本；集中度的前百分比合约数按全部疑似重复合约计算，不按有正成本的合约计算。
- `attacker_cost_details`：仅在 JSON 中逐交易输出攻击者成本明细，包括 `contract_address`、`stage`、`channel`、`tx_hash`、`gas_payer_address`、`gas_native/usd`、`from_role`、`to_role`、`evidence_type`；gas payer 只有在显式恶意地址或交易发送方具备攻击者/运营者角色时才计入，不再因为只出现在 gas payer 字段就自动归为恶意；无法取得汇率时 USD 字段不把不同链的原生币直接相加。
- `output_input_ratio_by_contract` / `output_input_summary`：单合约 JSON 为每个有正产出的 match 合约统计 `output_usd / input_usd`，产出为 0 的合约不进入该表；`input_usd` 使用同合约攻击者 gas 成本，`output_usd` 只统计进入仿冒合约或运营者角色的 mint/sale/royalty 收入，以及 `exit_payment` 标记的攻击者出货收入，不把 protocol fee、普通 royalty、withdrawal/cashout 二次转移当作新增产出；汇总 JSON 输出总产出投入比，以及单合约产出投入比 `>= 1` 和 `< 1` 的数量比例。
- `honest_loss`：单个总计对象，汇总二级市场损失、付费 mint 损失、总损失、套牢 NFT 和集中度，不再按类别拆成多行；套牢比例分子为诚实地址当前持有的 fake NFT，分母为全部 fake NFT，套牢时间在 Markdown 中按倍数展示；诚实买家明细中的 `fake_nft_bought` 采用套牢 fake NFT 口径，与汇总套牢数保持一致。
- `data_quality`：销售价格可解析比例、官方参与型重复合约数，以及 Solana asset/history 完整性。字段区分已分析、未请求、签名发现失败、交易详情失败、截断、未归因 SOL 交易、未解析 compressed mint、缺失 mint 前余额和缺失 collection authority；只有完整证据才计算完整交易覆盖率。

统计阶段不做代表合约或买家 top-k 截断，单合约 JSON 会导出所有可识别的合约行为、诚实买家行、攻击者成本明细、地址集合、分母 keys、按合约贡献和按行为去重集合。论文撰写时再按行为覆盖数、关联损失、虚假交易额、地址规模等指标选择代表案例。旧参数 `--paper-top-k` 已删除。

Markdown 报告只保留论文阅读用摘要，不展开攻击者成本逐交易明细。`summary.md` 只输出汇总章节，不列举逐合约明细；单合约 Markdown 在最后按综合影响金额 USD、行为实例数、合约地址排序列出 match 合约，并为每个 match 合约分别输出设计文档中单合约例子的 Wash Trading、Pump-and-Exit、星型行为、Layered Transfer、Inventory Concentration、诚实买家和 Wash Cycle 节点规模表。地址、cycle id、path id 只作识别标签，过长时截断显示；`unattributed_sale` 诚实买家不进入 Markdown 明细表。完整细节以单合约 JSON 为准。

所有论文比例字段都保留可复核口径。重复规模、行为汇总、wash cycle 节点规模、攻击投入、产出投入比、诚实损失、sale 价格可解析比例已经输出对应的 numerator / denominator；单合约行为中的 `exit_price_premium`、`exit_ratio`、`avg_fan_out`、`token_share`、`value_share` 也分别导出对应分子和分母字段。

## 典型使用流程

1. 先用 `cargo run --release --features export-snapshot -- export-snapshot ...` 从 PostgreSQL 导出特征快照到 Parquet。
2. 用 `prepare-features` 把全部 Parquet 导入文件 DuckDB 并构建 prepared recall 数据。
3. 用 `analyze` 跑一个 seed 合约，确认 API 凭证、阈值和输出格式都正常。
4. 再用 `batch` 跑正式批量分析；只读复用同一个 feature DB。

## 补充说明

- `analyze` / `batch` 的 `--feature-db` 默认是 `features.duckdb`，且只读打开。`prepare-features` 要求显式提供文件路径。
- HTTP API 默认最多请求 5 次；429、408、5xx 和网络错误会重试，每次重试之间等待 500ms。400 等非临时客户端错误不会等待重试。
- `analyze` / `batch` 仍保留 `--feature-parquet` 仅用于给旧调用方返回明确迁移错误；它们不会导入或修改数据库。
- 当前快照 schema 强制包含 `metadata_json`、`token_uri_norm`、`image_uri_norm`、`name_norm`。精确 URI 召回使用仅保存非空 URI 的固定宽度 hash posting，并在回表后校验原始规范化字符串；hash 碰撞不会产生最终误命中。
- name recall 为每个合约的每个不同规范化名称保留一条精简行，并用 `u32` 字符 posting 和长度界限先做无损候选剪枝、再执行 Jaro-Winkler；metadata recall 以合约代表行为单位构建共享 token 词典、精确 term posting 和紧凑 BM25 文档。完整 URI、name、token 和 metadata payload 都在命中后分块回表，不在四链常驻索引中重复保存。
- generation catalog 在导入/prepare 时维护行数和合约数，batch 启动读取 O(1) 快照身份，不再对 2 亿行执行全表 hash。旧库或手工修改过 `nft_features` 的数据库需要重新运行 `prepare-features`。
- 合约命中后，分析阶段会通过 Alchemy `getNFTsForContract` 拉取该合约下全量 NFT，用于 NFT 级报告、地址和交易统计。
- Solana 历史余额、手续费和同交易资金流只读取目标 `getTransaction`，多个消费者共享有界缓存；分析路径不扫描整 slot 的 `getBlock`。
- 四链 `batch` 使用有界三阶段 seed 流水线：context 网络阶段、四链 recall/评分 CPU 阶段、四链 matched-contract 分析阶段。不同 seed 可以处于不同阶段并行推进；同一 seed 的四条候选链始终顺序执行。每个 seed 只获取一次 primary-chain context。
- `batch` 的资源在整个进程内全局复用：API client、HTTP semaphore、DuckDB feature store 不按并发槽位复制，避免重复占用内存。
- 四链 `batch` 的并发限制均为批次级共享：network 和 CPU semaphore 分别限制前两阶段；所有活跃 seed 共享同一个 `--matched-contract-max-concurrency` semaphore。Alchemy 使用 `--alchemy-api-max-concurrency` 控制 in-flight 请求；Helius 同时使用 `--helius-api-max-concurrency` 和 `--helius-rate-limit-refill-ms` 控制并发与请求速率，并受 collection 级历史预算约束。`--other-api-max-concurrency` / `--other-api-rate-limit-refill-ms` 继续控制 OpenSea、Etherscan 和汇率请求的速率桶。流水线 backlog 固定封顶，seed 数量增加不会让全部 context/plan 同时驻留内存。
