# `name_uri_analysis_rs` 2 亿行单机优化设计

日期：2026-07-12

## 1. 背景与目标

`name_uri_analysis_rs` 当前使用单进程、内存 DuckDB 和 Rust 分析阶段串行完成 URI、name 与 metadata 查重。准备阶段首先把输入 Parquet 的全部分析字段物化到宽表 `analysis_rows`，随后再构建合约统计、URI key、name atoms 与 metadata 中间状态。这种设计在小规模数据上简单有效，但在约 2 亿行输入上存在以下结构性问题：

- 未压缩内存宽表长期保留 URI、name、token ID 与 metadata JSON；
- 合约聚合在 hash state 中直接维护大字符串 metadata JSON；
- URI 中间查询会把 token/image key 展开为更多行；
- metadata 在过滤共享 token 之前为全部 token ID 建立全局编号；
- name 按 `(chain, name)` 建点，使相同规范名跨链重复评分；
- DuckDB、name 和 metadata 共处一个进程，阶段结束后 RSS 不一定完全返还给操作系统；
- 长时间运行缺少阶段级检查点，失败后需要从头重跑。

本设计在不改变现有查重语义和最终输出格式的前提下，将程序重构为单机、分阶段、可恢复的 DuckDB + Rust 混合外存流水线。

### 1.1 固定部署条件

- 阿里云 AMD EPYC 实例；
- 32 个物理核，关闭 SMT，操作系统可见 32 CPU；
- 256 GiB 内存；
- 本机 ESSD，工作空间充足；
- 输入为本机 Parquet；
- 输入规模约 2 亿行，源数据总体量不足约 200 GiB；
- 同一快照只执行一次完整分析；
- 单机执行，不引入分布式框架；
- 不直接查询 PostgreSQL。

### 1.2 必须保持的行为

- EVM 合约地址按小写规范化；
- Solana collection 地址保持 Base58 原值大小写；
- name 使用默认 95 的 Jaro-Winkler 阈值；
- URI 保持 V1、V2、V3 定义及 intra-chain、cross-chain summary、directional chain matrix 范围；
- metadata 保持代表文档、模板召回、内容验证、共享 token 优先、无共同 token fallback 与 0.6 阈值；
- metadata 仍只接受非空、首字符为 `{` 或 `[`、大小不超过 64 KiB 的输入；
- 最终仍输出 `summary.json` 和 `summary.csv`，字段、排序和比例定义不变。

## 2. 总体架构

用户继续运行一个公开命令。主进程作为 controller，通过三个隐藏子命令启动独立子进程：

```text
name_uri_analysis_rs
  ├─ __internal-prepare
  ├─ __internal-name
  └─ __internal-metadata
```

数据流如下：

```text
本机 Parquet
    |
    v
Prepare 子进程（DuckDB，32 threads，160 GiB）
    ├─ contract_dim / canonical name artifact
    ├─ URI partial summary
    └─ compact metadata artifacts
    |
    +-- 子进程退出，释放全部 DuckDB 内存
    |
    v
Name 子进程（Rayon，32 threads，192 GiB）
    ├─ name partial summary
    +-- 子进程退出，释放 name index、DSU 与 scratch
    |
    v
Metadata 子进程（Rayon，32 threads，192 GiB）
    ├─ metadata partial summary
    +-- 子进程退出，释放 postings、BM25、DSU 与 mmap
    |
    v
Controller 合并、排序并写出最终 summary
```

三个重负载阶段不得并发。进程隔离用于保证每个阶段结束后由操作系统回收全部地址空间，而不是依赖 allocator、DuckDB buffer manager 或容器析构是否立即降低 RSS。

## 3. CLI 与资源模型

### 3.1 删除的参数

删除以下参数及 README、测试和帮助文本中的所有引用：

```text
--physical-cores
--database
--persist-prepared
--reuse-prepared
```

`--physical-cores` 只是线程数覆盖，并未设置 CPU affinity；最终只保留一个明确线程参数。

### 3.2 公开参数

```text
--parquet <PATH>...                 必填
--output-dir <PATH>
--work-directory <PATH>
--threads <N>                      默认 32
--duckdb-memory-limit <SIZE>       默认 160GiB
--analysis-memory-limit <SIZE>     默认 192GiB
--name-threshold <FLOAT>           默认 95，单值
--resume                           默认关闭；显式启用检查点恢复
--keep-work-directory              默认关闭
--no-progress
```

`--threads=0` 不再表示 auto。有效线程数为 `min(cli_threads, available_parallelism)`，并要求结果至少为 1。目标机器关闭 SMT 后为 32。

`--work-directory` 默认位于系统临时目录的 `name_uri_analysis_rs_work` 子目录；生产命令必须显式指向 ESSD。DuckDB spill 目录固定为 `<work-directory>/duckdb-temp`，不再单独暴露重复配置。普通运行发现已有工作目录时拒绝覆盖；只有显式传入 `--resume` 且输入指纹完全一致时才复用完整检查点。

`--name-threshold` 为单值，因为程序已经只支持一个 name threshold。删除阈值数组、阈值 batch 和与多阈值状态相关的兼容代码。

### 3.3 部署命令

```bash
numactl --interleave=all \
  ./name_uri_analysis_rs \
  --threads 32 \
  --duckdb-memory-limit 160GiB \
  --analysis-memory-limit 192GiB \
  --work-directory /mnt/essd/name-uri-work \
  --parquet ...
```

阿里云实例配置保持 32 个物理核心，并将 `ThreadsPerCore` 设为 1。不得在关闭 SMT 的同时把物理核心数降为 16。

## 4. 工作目录、manifest 与恢复

工作目录结构：

```text
work/
  manifest.json
  stage.duckdb
  duckdb-temp/
  artifacts/
    name/
    metadata/
  partial/
    uri-summary.json
    name-summary.json
    metadata-summary.json
  metrics/
```

Manifest 保存：

- pipeline schema version；
- 二进制版本；
- 规范化输入路径、文件大小、mtime、Parquet 行数与 schema hash；
- 文件顺序和 file ID；
- chain ID 映射；
- 完整 CLI 参数；
- 每个阶段状态；
- artifact 行数、文件大小与校验值。

阶段状态按顺序为：

```text
input_validated
contracts_ready
uri_complete
metadata_compact_ready
prepare_complete
name_complete
metadata_complete
finalized
```

Artifact 和 manifest 先写入 `.partial` 文件，flush 并关闭后再原子 rename。输入指纹或 pipeline schema version 不一致时必须拒绝 resume，不得静默复用旧数据。失败时保留工作目录；全部成功后默认删除，`--keep-work-directory` 可保留。

## 5. Prepare 子进程

### 5.1 DuckDB 配置

Prepare 使用 ESSD 上的临时持久化 DuckDB 数据库，以获得压缩和可恢复性：

```sql
SET threads = 32;
SET memory_limit = '160GiB';
SET preserve_insertion_order = false;
SET temp_directory = '<work-directory>/duckdb-temp';
SET parquet_metadata_cache = true;
```

`memory_limit` 只作为 DuckDB buffer manager 的限制。其余 96 GiB 留给非 buffer-manager 内存、操作系统、页缓存和峰值安全余量。每个大型 stage 完成后执行 checkpoint。

### 5.2 输入预检

Prepare 先读取 Parquet footer，而不是扫描全部数据。预检内容包括：

- 必要字段和可选 metadata 字段；
- 总文件数、行数、row-group 数量和大小；
- chain 集合；
- 每个文件的稳定顺序；
- 每个文件的行数是否能由 `u64` 表示。

若 row group 数少于 32 或 row-group 大小明显不在约 10 万至 100 万行范围，输出性能警告，但不在一次性分析前强制重写源 Parquet。

输入行身份定义为：

```text
SourceId {
  file_id: u32,
  file_row_number: u64,
}
```

`file_id` 按规范化后的 CLI 文件顺序分配，`file_row_number` 来自 `read_parquet(..., file_row_number=true)`。所有代表行 tie-break 都使用 `(file_id, file_row_number)`，替代临时 DuckDB `rowid`。

## 6. Contract 与 chain 维表

首次扫描只读取 `chain`、`contract_address`、`name_norm`，不读取 URI、token ID 或 metadata。

建立：

```text
chain_dim
  chain_id          u8
  chain_name        string

contract_dim
  contract_id       u32
  chain_id          u8
  contract_address  string
  nft_count         u64
  name_norm         optional string
```

地址规范化在这一阶段完成。`contract_id` 是单次运行内部编号，无需按地址排序；使用无 `ORDER BY` 的编号，避免对全部合约执行额外全局排序。建立后验证合约数量不超过 `u32::MAX`。

chain 总量直接从 `contract_dim` 汇总。所有后续大型表使用 `chain_id` 和 `contract_id`，不重复保存或 hash 完整合约字符串。

## 7. Name 领域设计

### 7.1 全局规范名模型

从 `contract_dim` 建立：

```text
name_values
  name_id
  name_norm
  char_length

name_chain_weights
  name_id
  chain_id
  contract_count
  nft_count
```

同一个 `name_norm` 无论出现在哪条链，只建立一个 `name_id`。相同字符串的 Jaro-Winkler 分数必然为 100，因此提前合并与原模型等价，只消除跨链重复评分。链内与跨链统计通过 `name_chain_weights` 保持。

### 7.2 Rust scoring

保留现有正确的：

- 长度上界；
- 字符出现次数 token；
- rare-prefix postings；
- sorted overlap 验证；
- RapidFuzz Jaro-Winkler cutoff；
- dense/sparse scratch 预算选择。

进一步调整：

- `PreparedNameQuery` 作为 worker-local 对象复用；
- hit buffer 作为 worker-local `Vec` 复用；
- Rayon `fold/reduce` 生成平坦 edge batch；
- edge batch 通过有界 channel 交给单线程 DSU consumer；
- 队列容量和全部 worker scratch 计入 192 GiB 分析预算；
- 只维护一个 threshold state；
- intra-chain、cross-chain summary 和 directional matrix 都从同一个全局 component 与 chain weights 生成；
- 删除预算不足时按链对重新评分的 fallback。

Name 阶段完成后写出 partial summary 并退出子进程。

## 8. URI 领域设计

### 8.1 URI 投影

建立压缩表：

```text
uri_rows
  chain_id
  contract_id
  token_uri_norm
  image_uri_norm
```

只保留至少一个 URI 非空的行，不保存地址、name、token ID 或 metadata。

### 8.2 key 聚合

不再对每行执行 `CROSS JOIN LATERAL VALUES`。分别建立 token URI 和 image URI 的 contract-level key 统计，再对已经聚合的小结果执行 `UNION ALL`：

```text
uri_key_contracts
  key_kind
  key_value
  chain_id
  contract_id
  nft_count
```

每个 key 进一步保存：

```text
chain_presence_mask: u64
per_chain_contract_count
```

本设计允许最多 64 条链，超过时明确返回输入错误。对主链 `c`，`other_mask = mask & ~(1 << c)`；非零表示 cross-chain summary 命中，mask 中每个 bit 对应 directional matrix 的目标链。

### 8.3 V1/V2/V3

在一次 `uri_rows` 扫描中同时 join token/image lookup，并严格按以下优先关系计算：

```text
V1 = token URI 命中
V2 = token URI 未命中且 image URI 命中
V3 = token URI 或 image URI 命中
```

结果直接聚合为 contract-level NFT counts 和 contract flags，再汇总为最终 URI rows。URI partial summary 写出后删除 URI 大表并 checkpoint。

## 9. Metadata 准备设计

### 9.1 eligible raw stage

metadata 扫描只读取：

```text
chain
contract_address
token_id
metadata_json / metadata_doc
filename
file_row_number
```

单次完成 contract ID 映射、token ID 规范化、trim、非空判断、64 KiB 上限、JSON 首字符判断和 source ID 建立。只将合法数据落入压缩表：

```text
eligible_metadata_raw
  source_file_id
  source_row_number
  contract_id
  token_id
  metadata_json
```

metadata 合法性只计算一次。

### 9.2 代表 source

禁止对大字符串执行 `arg_min(metadata_json, ...)`。代表行选择分为：

1. 聚合固定长度 `SourceId`；
2. 按 source ID 回表读取 JSON。

每个合约的代表 source 按 `(token_id, source_file_id, source_row_number)` 取最小值。每个 `(contract_id, token_id)` 的 source 按 `(source_file_id, source_row_number)` 取最小值。

### 9.3 先过滤全局单例 token

建立 `contract_token_source` 后，先执行：

```sql
GROUP BY token_id
HAVING count(DISTINCT contract_id) >= 2
```

只有跨合约共享的 token ID 才分配紧凑 `token_index`。删除全局单例 token 不改变任意合约对是否存在 token 交集，因此同时保持 shared-token 验证与 no-common-token fallback 语义。

### 9.4 紧凑 artifacts

真正需要读取和解析 JSON 的 source 集合为：

```text
每个合约的代表 source
UNION
每个共享 token 的 contract-token source
```

Source ID 去重后回表读取 JSON，写出：

```text
metadata_representatives
  contract_id
  chain_id
  nft_count
  source_id
  metadata_json

metadata_shared_token_docs
  token_index
  contract_id
  source_id
  metadata_json

contract_shared_tokens
  contract_id
  token_index
```

Artifacts 完成并验证后删除 `eligible_metadata_raw`，checkpoint，写出 `metadata_compact_ready`，然后 Prepare 子进程退出。

## 10. Rust metadata 设计

### 10.1 单次 JSON 解析

按稳定 128-bit 内容 hash 分组；hash 相同时执行完整字节比较，避免碰撞改变结果。每个不同 raw JSON 只执行一次：

- JSON parse；
- whitelist 字段提取；
- NFKC/lowercase/whitespace normalization；
- template document；
- content document；
- lexical tokenization。

代表记录和 shared-token 记录只引用紧凑 `doc_id`。

### 10.2 mmap 索引

大型不可变结构写成连续文件并只读 mmap：

```text
docs.bin
doc_offsets.bin
postings.bin
posting_offsets.bin
queries.bin
sketches.bin
```

堆内存只保留 DSU、有限批次、worker scratch 和小型映射。OS 页缓存负责热点数据。

### 10.3 候选与 union

保留现有 template exact match、BM25 recall、metadata sketch、低频 anchor gate、0.6 内容验证、shared-token 优先、no-common-token fallback 与连通分量聚合。

并行策略：

- left-document range 并行；
- 每 worker 独立 scratch；
- candidate edge 使用有界 batch；
- BM25/content score 并行；
- DSU union 单线程顺序应用；
- 已连通 singleton pair 提前跳过；
- candidate、scored、matched counters 使用 `u64`。

Metadata partial summary 写出后子进程退出。

## 11. 输出与确定性

Controller 读取 URI、name、metadata partial summaries，按现有字段排序规则合并，最后以临时文件加原子 rename 的方式写出：

```text
summary.json
summary.csv
```

内部 contract/name/doc ID 顺序允许变化；最终 summary 必须与相同输入、参数下的原实现语义一致。所有 tie-break 由稳定 SourceId 定义，不依赖 DuckDB 临时表 rowid 或线程调度顺序。

## 12. 可观测性

每阶段记录：

- wall time 与 CPU time；
- 输入/输出行数；
- 峰值 RSS；
- DuckDB peak buffer memory 与 peak temp size；
- ESSD 读写字节；
- name candidates、scored pairs、matched pairs；
- metadata raw、eligible、selected source 数；
- 被删除的 singleton token 数量；
- raw JSON 去重率；
- template/content doc 数量；
- postings、mmap 与 DSU 大小。

进度按已处理工作量推进，不按命中数推进。DuckDB prepare 开启 JSON profiling，并把 operator timing、cardinality、memory 与 temp 指标写入 `metrics/`。

## 13. 错误处理

- 输入 schema、chain 数、u32/u64 边界在大型分配前验证；
- DuckDB stage 使用事务和原子检查点；
- 任何 artifact 校验失败时不得标记阶段完成；
- child exit code 非零时 controller 停止，不启动后续阶段；
- OOM、磁盘错误、DuckDB 错误和数据错误分别输出明确阶段与上下文；
- resume 只从最后一个完整检查点开始；
- 成功前不得删除仍被后续阶段依赖的 artifact。

## 14. 验证策略

### 14.1 语义差分测试

使用小型生成 Parquet 同时运行旧路径和新路径，逐字段比较最终 summary。覆盖：

- EVM/Solana 地址规范化；
- name 代表与跨链相同 name；
- metadata 最小 token/source tie-break；
- URI V1/V2/V3；
- intra、cross summary、directional matrix；
- shared-token 与 fallback；
- 64 KiB 边界、空值和无效 JSON；
- summary 排序与比例。

### 14.2 属性测试

- canonical name collapse 与原 `(chain, name)` atom 模型等价；
- 删除 singleton token 后，任意合约对的 token-intersection 布尔值不变；
- URI chain mask 与原 chain-presence 行表示等价；
- bounded queue 不改变 edge 集合；
- mmap 与内存索引产生相同候选和分数；
- SourceId tie-break 与规范化输入顺序一致。

### 14.3 结构性性能门禁

CI 不使用易波动的绝对耗时作为门禁，而验证：

- 不存在全量宽 `analysis_rows`；
- 不存在 `arg_min(metadata_json, ...)`；
- token dictionary 只在 shared-token 集合上建立；
- 相同 `name_norm` 只形成一个 scoring node；
- 大型队列均有容量上限；
- 每 worker scratch 计入预算；
- prepare、name、metadata 不在一个进程同时常驻。

## 15. 实施边界与顺序

实施必须按以下顺序推进，每一步保持可测试：

1. CLI 简化、删除 `--physical-cores`、固定资源模型；
2. controller、隐藏子命令、manifest 和原子 artifact；
3. contract/chain 维表，删除宽 `analysis_rows`；
4. 整数 SourceId 代表选择，删除大 JSON 聚合；
5. shared-token 前置过滤；
6. URI 领域投影与 chain mask；
7. canonical name 节点与全局单次评分；
8. name worker-local query/hit buffers 和有界 edge pipeline；
9. metadata raw JSON 全局去重和单次解析；
10. metadata 连续 mmap 索引；
11. profiling、RSS、spill 和 artifact metrics；
12. 差分、属性、恢复和结构性性能测试；
13. README 与运行示例更新。

本设计不要求把 DuckDB 替换为自研 Rust 外排序或分布式系统，也不改变查重阈值和业务口径。

## 16. 验收标准

设计实现完成需同时满足：

- `--physical-cores` 及全部引用被删除；
- 目标命令使用 32 threads，关闭 SMT；
- Prepare DuckDB buffer limit 为 160 GiB，Rust worker hard budget 为 192 GiB；
- 全量宽 `analysis_rows` 不再存在；
- DuckDB 聚合状态不保存 metadata JSON；
- 全局单例 token 不进入 token dictionary 和 Rust metadata scorer；
- 跨链相同 name 只评分一次；
- URI 使用紧凑 chain mask；
- 三个重负载阶段由独立进程执行；
- 失败可从完整阶段检查点恢复；
- 旧、新路径在覆盖语义边界的差分测试上输出一致；
- `cargo fmt --check`、严格 Clippy、crate tests 和 integration tests 全部通过；
- 工作目录在成功后按配置清理，失败后保留。

不承诺未经目标数据实测的固定加速倍数。验收关注语义一致、峰值内存有界、无重复大规模工作和 2 亿行执行路径的结构可扩展性。
