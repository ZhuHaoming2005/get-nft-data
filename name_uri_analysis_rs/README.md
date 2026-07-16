# Name URI Analysis RS

面向大规模本机 Parquet 快照的 Rust + DuckDB NFT 重复分析器。输出 `name`、URI 和
metadata 的链内、跨链汇总及定向链对矩阵；最终原子发布 `summary.json`、
`summary.csv` 和校验两者大小与 SHA-256 的 `summary.manifest.json`。

## 生产运行

目标配置为 128 vCPU、512 GiB 内存。默认模式可使用本机 ESSD；速度优先且允许释放
Prepare/Name DuckDB 中间状态时，可使用 Linux tmpfs 纯内存模式：

```bash
./main \
  --parquet ./data/ethereum.parquet \
  --parquet ./data/base.parquet \
  --parquet ./data/polygon.parquet \
  --parquet ./data/solana.parquet \
  --output-dir ./output \
  --ephemeral-in-memory \
  --threads 128 \
  --prepare-threads 64 \
  --metadata-encode-threads 128 \
  --name-threads 128 \
  --metadata-match-threads 128 \
  --duckdb-threads 64 \
  --duckdb-memory-limit 320GiB \
  --analysis-memory-limit 448GiB \
  --name-threshold 95
```

`--ephemeral-in-memory` 在 Linux 上默认使用稳定路径
`/dev/shm/name_uri_analysis_rs_work`，会校验该路径确实属于 tmpfs，并在新任务启动前按
`3 × 输入文件大小 + 8 GiB` 做最低容量检查。Docker 默认 `/dev/shm` 往往只有 64 MiB，
必须通过 `--work-directory` 指向单独挂载且容量足够的 tmpfs。Name 完成后，Match 启动前会删除
`stage.duckdb` 和 `duckdb-temp`，避免它们与 Match hard top 同时占用 RAM。

Linux 多 NUMA 节点默认尝试 `MPOL_INTERLEAVE`，容器拒绝该系统调用时自动退化；可通过
`--disable-numa-interleave`（或环境变量
`NAME_URI_ANALYSIS_DISABLE_NUMA_INTERLEAVE=1`）禁用。各阶段线程数未单独指定时继承
`--threads`，DuckDB 默认仍限制为 64。

metadata 由 `metadata_engine` 负责，不提供另一套 matcher、运行时回退或
`--metadata-recall-mode`。缺失、损坏或超预算的 artifact 会使 Match 失败，且不会发布
metadata summary/ready checkpoint。

Match 固化快照指纹、引擎 revision、语义门禁、摘要哈希及实际主机规格，Finalize 将其持久发布到
`<output-dir>/advisory/metadata-readiness-input.json`。运行证据由外部任务写入
`<output-dir>/production-evidence/metadata-v2.json`；Finalize 和已完成任务的 resume 会按当前证据
重算 `<output-dir>/advisory/metadata-production-readiness.json`。也可在补齐证据后单独刷新，无需
重跑 Match 或读取 Parquet：

```bash
./main \
  --refresh-production-readiness \
  --output-dir ./output
```

readiness 始终只供观测，不参与 Finalize、resume、summary 发布或 summary 代次校验。证据缺失、
损坏、过期或 `production_ready=false` 只会写入 blocker；即使 advisory 刷新自身失败，主分析结果
仍保持可用。`metadata-readiness-input.json` 是 Match 事实，外部任务只应更新
`production-evidence/metadata-v2.json`。

## 执行架构

Controller 固定执行五个阶段：

1. `Prepare + URI`：DuckDB 建立压缩维表；URI 与 eligible metadata 共用一次 Parquet Arrow
   投影流并在内存中拆分，不物化宽中间表；
2. `MetadataEncode`：流式选择首个可用的 token-specific/fallback source，在 Rust 内存中建立
   source dictionary、membership 和 payload arena，写不可变 feature、atom、token membership
   与 blocking artifact；不创建 Encode 临时关系表或持久 payload CAS；
3. `Name`：加载 canonical name 节点并执行并行 Jaro-Winkler；
4. `MetadataMatch`：只读 metadata snapshot，在内存中完成 catalog、证据型 ExactIsland、并行候选
   评分、scope forest 收集与流式归约；CLI 使用 `MatchPersistence::Ephemeral`，不写
   component snapshot、raw Evidence ready JSON 或 Match 内部恢复文件，只把 compact summary
   交给 Finalize；
5. `Finalize`：合并三个 summary partial，排序并原子发布输出代次。

重负载阶段在独立子进程中运行，退出后由操作系统回收 DuckDB、Rayon scratch 与 allocator
高水位。`MetadataEncode` artifact 采用 revisioned ready marker；`StorageBroker` 记录 pin、
依赖、可重建与可回收状态。CLI 的 Ephemeral Match 若失败会从 Encode snapshot 重新运行 Match，
而不是恢复 Match 内部子阶段。

`metadata_engine::pipeline::run_metadata_pipeline` 和
`run_metadata_pipeline_with_progress` 默认使用 `MatchPersistence::MemoryFirst`：catalog、Exact、
rescue/recall 和 connectivity run 只驻留本次进程内，且启动时会以 ledger-safe 的方式清理同一输出
目录中的旧恢复 artifact。确实需要细粒度 Match 内部恢复时，库调用方可显式使用
`run_metadata_pipeline_durable`，或通过
`run_metadata_pipeline_with_progress_and_persistence(..., MatchPersistence::Durable, ...)` 启用持久恢复
产物；需要与 CLI 相同的无 Match 私有落盘行为时，使用
`run_metadata_pipeline_ephemeral_with_progress`。三种模式的最终分组、summary 和业务语义相同。

公开库入口 `run_analysis` 使用相同五阶段实现；它只创建唯一的临时兼容工作目录，并在成功或
失败后清理，不再维护另一套进程内 metadata 算法。

## 进度与 ETA

进度位置按实际尝试的工作量推进，不按命中数推进。metadata 引擎通过无终端依赖的事件报告稳定
subphase、`completed/total`、工作单位和诊断计数，CLI 只负责渲染。

- 可预先精确计数的 rows、contracts、token groups、memberships、pair visits、edges、nodes 和 files
  使用确定进度；
- Catalog 使用“精确 routing work + 所有可能 contract expansion”的稳定安全上界，shared-token
  使用剩余 pair-visit budget 上界；这两类 task 标签显示 `(upper bound)`、metrics 显示
  `ETA ≤ ...`，终点保留原上界而不重建 exact total；
- DuckDB Rust API 无法观测单条 CTAS/聚合查询的执行总量；这些任务明确保持 indeterminate 和
  `ETA n/a (total unknown)`，不会用输出行数或固定阶段数伪造百分比；
- 精确总量为零的阶段显示 `skipped (0 <unit>)`，不再显示无意义的 `n/a` 速率或历史 ETA；
- ETA 在至少 1 秒预热和有效增量后，用当前 homogeneous subphase 的 EWMA 速率计算；
- 切换 subphase 或工作单位时重置估计器，零增量刷新不会改变速率；
- candidate、scored、matched、selected 等计数仅用于诊断，不影响位置或 ETA；
- retained-token source 分类按精确 `(contract, token)` group 总量推进，即使没有选中 source
  也计为已完成；跨 Arrow batch 的同一 group 只计一次；
- fallback source 分类只使用一个 contract 级 phase，候选行数与选中来源作为诊断计数，避免两个
  phase 交错导致任务条和 ETA 反复重置；Encode publish 与 membership sort 使用稳定已知总量；
- Name atom 加载按 DuckDB 表的精确行数推进，candidate index 按 token/posting 构建和文档排序两次
  遍历的 `2N` 工作量分批推进；
- task 行使用千位分隔的位置和总量，metrics 行只显示紧凑吞吐、当前 ETA 和语义化诊断计数，
  不重复任务名与位置；
- Catalog 在长 job 内增量上报，Exact 按 frontier/group、Reduce 按 edge/root
  chunk 上报；MemoryFirst 使用 `finalize component groups`，不会显示实际未执行的 connectivity
  commit/recovery 阶段；
- UI 以 20 Hz 刷新，完成值被钳制到 total，失败不会显示 100%。

因此 `ETA` 表示“当前子阶段剩余同类工作”的估计。只有 Metadata Match 的引擎事件会独立显示
`match ETA n/a (uncalibrated)`；在没有同 revision、同规模目标机历史分布前不会把子阶段速率外推成
整段 Match 的伪精确 ETA。当前 Match controller revision 为 18，旧持久化路径的历史样本不会污染
新的 MemoryFirst ETA。
可用 `--no-progress` 关闭终端进度。

## 资源、恢复与诊断

- DuckDB 默认最多使用 64 线程，也可用 `--duckdb-threads` 显式调整；Prepare、Encode、Name、
  Match 分别支持独立线程上限。Name 的 Arrow 转换、排序、candidate index 和 scorer 共用同一个
  受控 Rayon pool，并在读取前按行数、字符串字节和 worker stack 做内存准入。Name DuckDB 收紧到
  最多 8 GiB；MetadataMatch 不打开 DuckDB。
- MetadataEncode 在全量物化前保留保守基线，并在每个解析批次分配前按该批 JSON 上界扩容；token
  relation 的准入同时覆盖 selected rows、排序副本、source records、source-id hash table 和
  memberships 的并存峰值。批次完成后依据实际 `Vec`/`HashMap` capacity、唯一 payload、
  template/content interner、source/token membership 重新调整 lease。冻结后还会按实际 CSR 维度
  准入 persistence scratch，避免大量小合约和唯一短 payload 绕过全局内存核算，同时不增加一次
  全量 JSON 预扫描。
- pair work、catalog jobs、Exact evidence、Rescue lane cache、edge buffer、snapshot/reduce 和
  artifact overlap 都在分配或执行前 checked admission。Rescue payload cache 每 lane 有固定上限，
  不会随 payload universe 无界增长；整数溢出、预算耗尽和不完整恢复均 fail closed。
- Match 使用实际物理内存探测保留 host headroom；edge 上限由 admitted Match 内存给出，随后按
  contract/scope 的最大 forest 上界自动缩小。Catalog 与 Exact 并行度均由 MemoryBroker 限制。
  hot block 在 catalog 中只保留一个惰性 fanout 描述符；执行时根据抽样 posting expansion 选择一维
  建立共享只读倒排索引，并在另一维并行验证 left-row term overlap 后才进入 scorer；共享 posting
  scratch 按并发 hot job 准入，线程局部 row scratch 按 lane 准入。shared-token ExactIsland 不再
  物化完整 routing pair 集，而是复用并行构建的只读 routing plan；小 population 全量扫描，大
  population 在 calibration/holdout 分区内按完整 pair population 等比例分配固定总样本预算。
  Reduce 直接在内存中对各 forest run 执行并行原子 union 和并行 root 解析，不再串行归并排序边；
  CLI Ephemeral 模式生成 summary 后立即释放组件根，不提交组件文件。
- 默认成功后删除工作目录；需要检查阶段恢复产物时使用
  `--keep-work-directory --diagnostics`。
- manifest 校验输入指纹、阶段 revision、partial hash 和后续阶段必需的 DuckDB 表；算法
  revision 变化只使受影响阶段及下游失效。

## 语义

- EVM 合约地址转小写；Solana collection 地址保留 Base58 大小写。
- name 默认使用 95 的 Jaro-Winkler 阈值。
- URI `v1` 为 token URI 命中，`v2` 为 token 未命中但 image 命中，`v3` 为任一命中。
- metadata 阈值为 0.6；BaseEquivalent 冻结候选关系后执行精确 template/content 校验，并按稳定
  SourceId 保留 token-specific metadata source。Calibration ExactIsland 冻结确定性 RescuePlan，
  Rescue 扫描完成后可补充生产连通边；独立 holdout 只负责门禁，不会改变 RescuePlan。门禁以
  sampled-left/shared-token group 为独立统计 cluster，并对 skipped pair-work 比例单独 fail closed。
  Shared-token calibration 和 holdout 各自按 pair population 等比例采样；只有实际评估 pair work
  等于所选 group 的完整 population 时才允许标记为 exhaustive。Evidence gate revision 为 4。
- 所有比例的分母都是主链全部非空合约数和 NFT 行数，不是可分析子集。

完整参数以 `--help` 为准。
