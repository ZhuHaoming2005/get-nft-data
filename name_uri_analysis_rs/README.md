# Name URI Analysis RS

面向大规模本机 Parquet 快照的 Rust + DuckDB NFT 重复分析器。输出 `name`、URI 和
metadata 的链内、跨链汇总及定向链对矩阵，最终文件为 `summary.json` 和
`summary.csv`；`summary.manifest.json` 保存两者的大小与 SHA-256，并作为同一输出代次的
原子提交标记。

## 生产运行

目标配置为阿里云 AMD EPYC、关闭 SMT 后 32 个可见物理核、256 GiB 内存和本机
ESSD：

```bash
numactl --interleave=all \
  ./name_uri_analysis_rs \
  --parquet /mnt/essd/data/ethereum.parquet \
  --parquet /mnt/essd/data/base.parquet \
  --parquet /mnt/essd/data/polygon.parquet \
  --parquet /mnt/essd/data/solana.parquet \
  --output-dir /mnt/essd/output \
  --work-directory /mnt/essd/name-uri-work \
  --threads 32 \
  --duckdb-memory-limit 160GiB \
  --analysis-memory-limit 192GiB \
  --name-threshold 95
```

`--threads` 默认 32 且不接受 0。程序不再区分“物理核参数”和“虚拟核参数”；部署时
由云实例将 `ThreadsPerCore` 设为 1，程序只使用操作系统可见的 worker 数。

## 执行架构

公开命令是 controller，依次启动三个互不重叠的重负载子进程：

1. prepare + URI：DuckDB 扫描 Parquet、建立压缩维表并完成 URI 汇总；
2. name：加载 canonical name 节点并执行并行 Jaro-Winkler；
3. metadata：加载紧凑 shared-token 数据并执行模板召回和内容验证。

每个阶段退出后由操作系统回收整个地址空间，避免 DuckDB、Rayon scratch 和 allocator
高水位同时驻留。DuckDB 使用 `<work-directory>/stage.duckdb`，spill 固定写入
`<work-directory>/duckdb-temp`。

manifest 固定记录 CLI 文件顺序、规范化路径、文件大小、纳秒 mtime、Parquet footer
行数、row-group 统计、schema SHA-256、二进制版本和全部分析参数。链集合只在 prepare
扫描中生成并校验，controller 不会为此预先再扫描一次 Parquet chain 列。row group
少于实际生效 worker 数，或大小明显偏离约 10 万至 100 万行时会给出性能警告，但不会自动重写一次性
输入。每个阶段的 partial summary
也保存大小与 SHA-256；恢复时会重新校验，stage 数据库还会验证后续阶段所需的表。
child 先持久化 partial 和带哈希的 ready checkpoint，再由 controller 更新 manifest；阶段
内部不再对 resumable stage 做破坏性清理。controller 全生命周期持有 OS 独占 owner lock 和
phase lease；启动 child 后才交出 phase lease，child 取得 lease 后还必须校验 controller 写入并
随进程环境传递的代际令牌，旧 controller 遗留的等待者即使先抢到锁也无法读取 manifest 或触碰
stage。child 退出后 controller 先重新取得 lease 并复核代际令牌，再更新状态。父端 stdin 生命
管道断开时 child 会立即退出。这样控制器
被 OOM/强制终止后，恢复进程既不会误删新锁，也不会和遗留阶段并发操作同一 stage。普通运行
拒绝覆盖非空工作目录，并拒绝重复输入、output 位于 work 内部、输入/参数/产物或阶段表不一致的恢复。

成功后默认删除工作目录；删除失败只警告，不会把已经完成的分析改判为失败。诊断或基准运行
应同时传 `--keep-work-directory --diagnostics`。默认关闭详细诊断，避免 DuckDB profiling、
RSS/I/O 采样和递归 spill 目录扫描进入生产关键路径。开启后会保留：

- `metrics/*-phase.json`：wall/CPU time、输入与 summary 行数、峰值 RSS、DuckDB spill
  高水位、进程 I/O 字节、数据库和 artifact 大小；
- `metrics/duckdb-prepare/*.json`：每个 prepare 主查询的 DuckDB detailed operator
  profile，包括 cardinality、timing、peak buffer memory 和 peak temp size；
- `metrics/name-algorithm.json`：canonical 节点、candidate/scored/matched pair、index、
  worker scratch 和 DSU 大小；
- `metrics/metadata-algorithm.json`：eligible/selected source、复用 JSON 缓存、singleton
  token 删除量、template/content 文档和候选、mmap 与 DSU 大小。

## 大数据路径

- 首轮扫描直接聚合为 `contract_dim`；不会物化包含 2 亿行字符串的宽表。
- URI 和 metadata 大表使用 `u32 contract_id`，不重复保存链名和合约地址。
- URI token/image key 分别先聚合再合并；大表只携带 `u32 chain_index` 和 `u8 key_kind`，
  跨链 presence 使用最多 64 条链的 `u64` bit mask。URI summary 一生成就释放
  prepare-only 的 URI/contract 临时表，再开始
  shared-token metadata compaction，两个大 hash 工作集不会同时驻留。
- metadata 合法性只计算一次并在聚合前过滤；代表行聚合只保存固定宽度行引用，不在 hash
  state 或 `analysis_contracts` 中复制 JSON。metadata 紧凑编号窗口也只处理有合法代表的
  合约；代表选择使用 CLI 顺序的 `u32 file_id + u64 file_row_number` 作为稳定 tie-break。
- prepare 在可恢复的 ESSD stage 中持久化合法性预筛后的 raw 表；代表、shared-token source 和
  singleton-filtered token 表均已压缩。raw 表保留到 metadata 完成，是为了在最低 token
  source 虽通过字节长度/首字符预筛但不能产生可用 prefilter 时，仍能按稳定 SourceId 寻找
  下一条，保持 fallback 语义。`--keep-work-directory` 会保留这些 resumable stage；默认在
  整条流水线成功后随工作目录一起删除。
- 代表与 shared-token source 中实际复用的 raw JSON 使用有界复用缓存；预算按解析后的
  String、HashMap 和 BM25 document 实际容量计算，缓存满后只降级为正常解析而不终止。
  payload 与 HashMap bucket 采用 O(1) 增量记账；预算为零或首次耗尽时立即停止 DuckDB
  结果流，不继续扫描无用候选。shared-token 验证结束后缓存立即释放。
- metadata 初始加载和稳定 fallback 共用按实际 raw/cached payload 估算的动态批次；并行解析前按
  `96 × raw payload`、缓存克隆和 25% allocator slack 的较大值预检；该上界覆盖 prefilter/content
  两份 BM25 文档对高基数短 token 的 Vec、HashMap key 和 unique-token 所有权。瞬时区最多使用分析预算的
  1/8 且封顶 4 GiB，普通小 metadata 仍可达到 16K 行批次；这部分先从 builder 上限扣除，避免
  接近 192 GiB 时先发生 OOM、随后才报告预算超限。
- metadata postings 及 query term/frequency/denominator、candidate token、prepared
  token/weight 使用连续 offsets/value 数组。正常情况下紧凑索引留在 Rust heap；只有最终
  resident 预算确实放不下时才原子写入 `artifacts/metadata/*.bin` 并只读 mmap。已移除重复
  doc-token 数组、冗余 token 交集扫描、simhash/anchor 硬门和对应 artifact，避免为了“省内存”
  反而主动制造 ESSD I/O。
- 只有至少被两个合约共享的 token ID 才进入紧凑 token dictionary 和 Rust scorer。
  singleton 统计和紧凑 contract-token source 表在 prepare 事务中持久化，metadata 子进程
  不会重新扫描全量表来重建 token dictionary。
- contract-token membership 使用 `u64 offsets + u32 values` 连续 CSR，而不是
  `Vec<Vec<u32>>`。加载时第一遍只计数，第二遍复用计数数组作为 cursor，再在配置的 Rayon
  线程池中按互不重叠切片排序；builder 完成后会先为 counts/offsets/values 构造峰值预留空间，
  必要时先把 metadata index remap 到 ESSD，再开始 token 分配。
- metadata shared-token group 最多每 1024 行分块解析，并立即压缩为 group-local term ID、在线
  合并相同 atom；raw-group 与初始加载共用 `raw + 96 × parse + 25%` 的高基数瞬时估算，下一行
  会超预算时先 flush 已有非空 chunk，单行仍放不下则在解析前失败。不会先为整个高频 token
  group 保留一份 owned raw JSON。representative
  fallback 复用同一在线 atom builder，不再创建全合约 records/compact-doc 副本；两条路径的
  builder、flat candidate index、scratch 和 pair batch 均纳入增量 working-set 预算；fallback
  每写入一个 document 就用 O(1) 的容量/计数器记账校验，不留固定数千条的越界窗口。
- 相同 `name_norm` 跨链只建立一个评分节点，单一阈值只执行一次全局评分；worker 复用
  候选、命中和 edge buffer，命中边通过容量为 `2 × Rayon workers` 的同步队列分批交给
  单一 DSU consumer，不形成无界全量 edge 集合。
- `--duckdb-memory-limit` 的 160 GiB 默认值用于 prepare；controller 在 name 子进程将
  DuckDB 自动收紧到最多 8 GiB，在 metadata 子进程收紧到最多 32 GiB（显式更小值保持
  不变）。Rust 分析预算默认 192 GiB。name 的所有 worker candidate/hit scratch、sparse dedup 和有界
  edge queue 都按最坏容量计入预算，并在候选索引分配前完成峰值预检；metadata builder
  每批使用增量 O(1) 记账检查 document、contract membership、复用 content 和 lookup
  内存；完成 builder 前会按 token occurrence/unique term 估算 lexical dictionary、source
  docs、postings、prepared query/doc 和 flat scoring 数组的重叠峰值，并先释放只在 load
  阶段使用的 key HashMap。最终预算还包含 contract-token、resident index、并行 scratch、
  DSU、summary scratch、template-match 的 compact pair/offset 构造峰值和 content working
  set，以及 metadata raw+parsed 动态加载瞬时区，超过时会在继续扩大内存前给出明确错误。两类
  阶段属于不同子进程；即使 metadata 仍流式读取 DuckDB stage，也不会同时暴露
  160 GiB buffer cap 与 192 GiB Rust cap。

template matching 每批按剩余 pair 预算重算 left-doc chunk；并行 scorer 在写入每个 hit 前
原子取得许可，不能先生成超限 batch 再报错。预算按每个 logical pair 40 bytes 加 offsets/cursors
预留 Rayon reduce、主 Vec 扩容和 symmetric postings 的构造重叠峰值。

当前输入仅约 16 GiB Parquet、解压后不足 200 GiB 时，不建议把 stage 改成 DuckDB
`:memory:`：160 GiB buffer manager 已会优先把热页和 hash state 留在内存，ESSD 只承接持久
stage、checkpoint 和必要 spill；同时仍保留断点恢复和阶段隔离。prepare 的上限是 160 GiB；
name/metadata 子进程分别把 DuckDB 收紧到 8/32 GiB，并使用最多 192 GiB Rust heap，因此不会
出现 160+192 GiB 同进程叠加，约 32 GiB 留给内核、allocator 和页缓存。

## 语义

- EVM 合约地址转小写；Solana collection 地址保留 Base58 原值大小写。
- name 默认使用 95 的 Jaro-Winkler 阈值。
- URI `v1` 为 token URI 命中，`v2` 为 token 未命中但 image 命中，`v3` 为任一命中。
- metadata 阈值为 0.6；先按模板/BM25 召回，再验证 description、attributes、image、
  external URL 等内容值。每个合约优先选 token ID 最小的合法 metadata。
- 所有比例的分母都是主链全部非空合约数和 NFT 行数，不是可分析子集。

可用 `--no-progress` 关闭终端进度条。完整参数以 `--help` 为准。
