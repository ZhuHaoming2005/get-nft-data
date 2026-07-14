# `name_uri_analysis_rs` 代表 metadata fallback 提速设计

日期：2026-07-14
状态：设计已确认，书面规格待复核

## 1. 背景与版本结论

生产慢日志来自提交
`18348f7fc4efd517bae1fda583a675fbfeed6ea2`。该版本尚未提供
`MetadataRecallMode::Conservative`，代表 metadata fallback 使用隐式 Exact
候选召回，并在单线程中依次处理全部 left atoms。日志显示：

- 13,533,773 个 fallback left atoms；
- 已处理 8,448 个 atoms；
- 约 1.6 atoms/s；
- 已枚举 10,947,874,110 个候选；
- 仅 146,986,292 个候选进入内容评分。

当前 HEAD 已加入 Conservative 索引、抽样校准、内存预算和 shared-token
并行波次，但这些能力没有覆盖代表 fallback：调用方仍把该阶段的
`recall_mode` 显式设置为 `Exact`，核心循环仍串行遍历 left atoms。

因此，旧日志不能作为当前 HEAD 的精确性能基线，但它揭示的两个结构性问题
在当前代码中仍然存在：代表 fallback 的候选集合过大，以及候选生成没有并行。

本设计是
`docs/superpowers/specs/2026-07-12-name-uri-analysis-200m-optimization-design.md`
的增量设计。该文档已经实现并仍然有效的子进程隔离、DuckDB 准备、紧凑
metadata artifacts、内存硬预算、checkpoint 和最终输出约束不在本次重写。

## 2. 目标与约束

### 2.1 目标

1. 让默认 Conservative 模式同时作用于 shared-token 和代表 fallback。
2. 通过有界并行波次使用空闲 CPU，同时保持结果确定性和内存硬上限。
3. 通过工作量受限的 Exact 校准，把允许的漏召回控制在明确阈值内。
4. 对高密度候选使用空间换时间，避免大量随机 generation-array 访问和巨大
   `Vec<u32>`。
5. 让进度和 metrics 能区分候选生成、维度过滤、token-overlap 过滤、
   connected skip、评分和匹配成本。
6. 保留显式 Exact 模式；该模式不得因本次优化改变候选集合、最终 component
   或 summary 语义。

### 2.2 固定约束

- 用户允许极少量 Conservative 漏召回。
- `--analysis-memory-limit` 仍是 Rust metadata 阶段的硬限制；高空闲内存只能在
  该限制内使用。
- 不改变代表 metadata 的选择规则、BM25/template 验证、0.6 内容阈值、
  shared-token 优先、无共同 token fallback 和连通分量统计口径。
- 相同输入和参数必须得到确定的输出，不得依赖线程数或 wave size。
- Exact 模式的候选集合、最终 component 和 summary 必须与优化前相同。由于
  Dense 候选采用不同的确定性遍历顺序，already-connected、scored 等内部工作量
  计数允许变化，但其字段定义必须保持真实、可解释。
- 修改 metadata 语义后只失效 metadata 和 finalizer checkpoint；prepare、URI
  和 name checkpoint 必须可复用。

### 2.3 本次不做

- 不增加 Git SHA、构建时间或新的二进制版本指纹；现有 manifest 行为保持不变。
- 不引入 GPU、分布式执行或外部向量数据库。
- 不改变 name、URI 或 metadata 的业务阈值。
- 不因为一次旧版本日志承诺固定加速倍数。
- 不无条件重写 Prepare、URI、Name 和 finalizer；这些阶段只接受由指标证明的
  后续优化。

## 3. 总体架构

代表 fallback 拆成五个职责单一的单元：

```text
fallback atoms / compact docs
            |
            v
  FallbackWorkPlanner
  - 估算每个 left 的 postings 工作量
  - 建立确定性分层样本
            |
            v
  FallbackRecallPlanner
  - 校准 Conservative profile
  - 检查漏召回与 component 漂移
            |
            v
  ParallelCandidateCollector
  - 有界 wave
  - worker-local scratch
  - sparse/dense CandidateSet
            |
            v
  OrderedFallbackConsumer
  - 按原 left 顺序消费
  - connected skip
  - 并行 batch scoring
  - 串行确定性 DSU union
            |
            v
  FallbackMetrics
```

现有 `MetadataLocalCandidateIndex`、`MetadataCandidateScratchPool`、
`score_and_apply_metadata_fallback_atom_pair_batch` 和 shared-token wave 逻辑继续
复用。新增抽象不能复制另一套 BM25 或 DSU 语义。

## 4. Recall 策略

### 4.1 CLI 语义

`--metadata-recall-mode` 必须覆盖 metadata 的两个候选路径：

- `exact`：shared-token 与代表 fallback 都使用 Exact；
- `conservative`：shared-token 与代表 fallback 都先使用 Conservative；
  shared-token 保留现有按 group 校准，代表 fallback 使用本设计的工作量受限
  校准。

代表 fallback 不再把调用方模式覆盖为 `MetadataRecallMode::Exact`。不得在
Conservative 校准失败后静默运行全局 Exact，因为这会重新产生以周或月计的
运行时间。

### 4.2 Conservative profiles

Conservative 使用两个递增召回 profile：

- **Base**：沿用现有最多 16 个低频 anchor 和 8 个 8-bit SimHash band 的精确
  band 命中；
- **Widened**：最多 32 个 anchor，并对每个 8-bit band 增加 Hamming distance 1
  的 multi-probe。

最终 BM25/template 和内容相似度验证完全不变。Widened 只扩大候选召回，不降低
最终匹配阈值。

索引按 Widened 所需的上限一次构建。Base 只是读取其中的子集，校准升级时不得
重建全部 atoms 或 docs。

### 4.3 工作量受限的 Exact 校准

固定抽取 1% left atoms 在 1353 万 atoms 上仍可能枚举数百亿候选，因此代表
fallback 改用工作量受限、确定性的分层校准：

1. 为每个 left 估算 template postings、content postings 和二者交集成本；
2. 按 chain 与 `log2(estimated_posting_visits)` 形成 strata；
3. 每个非空 stratum 至少选择一个确定性样本，并对高成本尾部过采样；
4. 最多选择 4,096 个 left atoms；
5. Exact 样本的预估 postings visits 总和最多 1,000,000,000；
6. 在 atoms 足够时至少覆盖 256 个 left atoms，否则覆盖全部 left atoms；
7. 汇总时按 stratum 总人口加权，不能直接使用过采样后的原始比例。

若 mandatory strata 样本中的任意单个 left 已超过总 postings-work 预算，或者
mandatory strata 总成本使最低覆盖无法满足，校准必须在执行 Exact 前失败并报告
超预算 strata；不得突破预算或静默减少 strata 覆盖。

校准在同一批 sampled lefts 上分别运行 Exact 和 Conservative，最终评分和
token-group eligibility 使用生产逻辑。校准记录：

- Exact/Conservative raw candidates；
- Exact/Conservative scored pairs；
- Exact matched pairs 与 Conservative missed matched pairs；
- duplicate contract/member 漂移；
- component member 漂移；
- matched-pair miss ratio 的 95% Wilson 上界。

若 Exact 样本没有匹配边，matched-pair 指标标记为无信息，不把它误报为统计上
确认的零漏召回；此时仍检查 contract/member 和 component 指标。

### 4.4 接受与升级规则

一个 profile 只有同时满足以下条件才可用于全量代表 fallback：

- duplicate contract/member 漂移不超过 0.5%；
- component member 漂移不超过 0.2%；
- 当 Exact 样本存在匹配边时，matched-pair miss ratio 的 95% Wilson 上界不超过
  0.5%。

先校准 Base；失败后校准 Widened。Widened 仍失败时，只允许对被校准识别为
高风险的 strata 使用 Exact，并且这些 strata 的预计 Exact 工作量不得超过全局
Exact 工作量的 5%，也不得超过 1,000,000,000 次预计 postings visits。混合
profile 必须在相同 sampled lefts 上重新计算三类漂移。若仍不能通过阈值，阶段
明确失败并输出各 stratum 漂移，不得自动执行全局 Exact。

显式 `--metadata-recall-mode exact` 不执行 Conservative 校准，也不受上述漂移
门禁限制。

## 5. 候选表示与空间换时间

### 5.1 CandidateSet

候选集合使用统一枚举：

```rust
enum CandidateSet {
    Sparse(Vec<MetadataDocIndex>),
    Dense(DenseCandidateBitmap),
}
```

`DenseCandidateBitmap` 使用连续 `u64` words，并通过池复用。atom universe 固定时，
一个位图大小为 `ceil(atom_count / 64) * 8`。在 13,533,773 atoms 上约为
1.61 MiB。

collector 从 Sparse 开始；当唯一候选数超过
`max(atom_count / 32, 65_536)` 时提升为 Dense。`atom_count / 32` 是
`Vec<u32>` 与一位/atom 位图的近似内存平衡点。本次实现固定使用该公式；后续
调整需要新的基准证据和规格变更。

### 5.2 交集与遍历

- Sparse/Sparse：对较小集合生成并用 generation scratch 验证另一集合；
- Sparse/Dense：保留 Sparse 中位图命中的元素；
- Dense/Dense：按 word 执行 AND；
- Dense 结果按 `trailing_zeros` 递增遍历，产生与 atom index 相同的稳定顺序。

位图只改变候选集合的表示和交集方式，不改变召回或评分语义。

### 5.3 token-group 过滤

`metadata_fallback_atoms_have_disjoint_token_groups` 只依赖不可变 atoms 和
contract tokens，应在并行 collector 中执行。已经连通判断依赖不断变化的 DSU，
必须留在 ordered consumer 中。

对于每个 atom 只有一个 fallback token group 的常见路径，复用稀疏
token-to-atom postings，在 worker-local 排除位图中 OR 当前 left group 的 token
postings，再从候选位图中执行 AND-NOT。不得为每个 token 常驻一张全量 atom
位图。多 token-group atom 继续使用现有精确标量逻辑，直到指标证明值得实现
更复杂的位图表达。

## 6. 有界并行波次

### 6.1 collector

fallback 复用 shared-token 路径的并行波次模型：

1. 一次 wave 包含连续的 left atom 范围；
2. Rayon 动态调度每个 left，重成本 left 不得固定绑定到单个 worker；
3. 每个 worker 从 `MetadataCandidateScratchPool` 获取 scratch；
4. collector 完成 postings 扫描、双维度候选交集和 token-group 过滤；
5. wave 结果按 left index 收集；
6. 生成下一 wave 与消费当前 wave 可通过 `rayon::join` 重叠。

### 6.2 ordered consumer

consumer 必须按原 left index 处理候选。同一 left 内，Sparse 保持 collector 的
稳定生成顺序，Dense 按递增 bit index 遍历；两者都不得依赖线程调度：

1. 查询当前 DSU，跳过已连接 pair；
2. 填充有界 scoring batch；
3. scoring batch 继续使用 Rayon；
4. 评分结果按输入 pair 顺序应用 union；
5. 更新确定性统计。

这样候选生成能并行，而 DSU 状态和最终 component 不依赖调度顺序。

### 6.3 内存治理

新增 `FallbackMemoryPlan`，输入为：

- atom count；
- Exact/Conservative index 常驻大小；
- `maximum_working_bytes`；
- Rayon thread count；
- Sparse/Dense scratch 上界；
- scoring batch 和 template cache 上界。

输出为 active collector 数、wave size、dense bitmap pool 容量和 scoring batch
容量。分配顺序为：

1. 预留现有常驻 index、DSU、template cache 和 scoring batch；
2. 为至少一个 collector 预留 scratch；
3. 在剩余预算内扩大 collector 和 wave；
4. 内存不足时先缩小 wave，再减少 active collectors；
5. 无法容纳一个 collector 时在大型分配前返回明确错误。

不得只按系统空闲内存扩张，也不得超出 `--analysis-memory-limit`。内存预算计算
使用 checked/saturating arithmetic，并由测试覆盖极值。

## 7. 进度与 metrics

atom 数不能代表实际工作量。fallback task 改用 WorkPlanner 估算的 postings visits
作为进度总量，并同时显示 processed left atoms。至少记录以下累计计数：

- estimated/visited posting entries；
- processed left atoms；
- raw candidate insertions；
- unique candidates；
- dimension-rejected pairs；
- token-overlap-rejected pairs；
- already-connected pairs；
- template-rejected pairs；
- scored pairs；
- matched pairs；
- Sparse-to-Dense promotions；
- Base/Widened/risk-strata Exact 的 left atom 数；
- active collectors、wave size 和 candidate peak bytes。

ETA 按已完成 postings work 计算。metrics 保留原有字段并新增明细；不能复用旧字段
表达不同含义。

## 8. 全流程审查与优化边界

### 8.1 Controller、Prepare 与恢复

独立 Prepare、Name、Metadata 子进程和 checkpoint 依赖图正确，继续保留。三次
窄列 Parquet 扫描利用列式读取，不能在没有 profile 证据时合并成一个包含 URI、
token ID 和 JSON 的宽物化表。

本次 metadata 行为变更把 `METADATA_STAGE_REVISION` 从 2 提升到 3，finalizer
依赖随之失效；prepare 和 name revision 不变。

### 8.2 URI

URI 后续仍存在将长 `key_value` 重复用于 hash/group/join 的提速空间。只有当
DuckDB operator profile 显示 URI 字符串 hash/join 占 Prepare wall time 至少 20%
时，才另行把 `(key_kind, key_value)` 映射为 dense `key_id`，后续 flags 和
chain-pair 聚合只使用整数。该优化保持精确语义，但不与当前 metadata 修复捆绑。

### 8.3 Name

Name 已使用 Rayon producer、有界 channel 和串行 DSU consumer。只有当新增或
现有指标显示 producer 因队列阻塞超过 Name wall time 的 20%，或者 consumer
独占超过 Name wall time 的 30% 时，才设计 component/star-union 优化。本次不改
Name 候选或阈值。

### 8.4 Metadata summary 与 finalizer

当前主要部署链数较少，summary 和 finalizer 数据量远小于候选阶段。只有 summary
超过 metadata wall time 的 10% 时，才把多次 sparse component 扫描改成一次聚合。
finalizer 不增加并行。

## 9. 错误处理与恢复

- 校准预算耗尽但未覆盖最低样本时，输出 strata、已用工作量和最低需求后失败；
- Conservative profiles 均超过漂移阈值时，输出漏召回分布后失败；
- 任何 candidate/memory 计算溢出时在分配前失败；
- worker panic 或 wave 结果缺失时不得继续消费不完整结果；
- `.partial` metrics 和 metadata summary 仍遵循现有原子写入规则；
- stage revision 变化后，resume 必须复用 prepare/name，重新运行 metadata 和
  finalizer；
- Exact 模式仍可由用户显式选择，但错误消息必须提示其预计工作量可能极大。

## 10. 测试与基准

### 10.1 失败优先的回归测试

实施前先加入以下会在当前代码上失败的测试：

1. 代表 fallback 必须接收 CLI 的 Conservative mode，不能硬编码 Exact；
2. fallback candidate collection 在大型 atom 集合上使用并行 wave；
3. ordered wave 与旧串行 Exact 路径产生相同候选集合、component 和 summary；
4. Base 校准失败后升级 Widened，而不是全局 Exact；
5. Widened 失败且高风险 Exact 超预算时明确失败；
6. stratified calibration 不超过 left-count 和 postings-work 双预算；
7. Sparse、Dense 及三种交集组合产生相同递增候选；
8. 单 token-group 位图过滤与标量逻辑等价；
9. 多 token-group atom 始终走精确标量逻辑；
10. 不同线程数和 wave size 的 Exact 输出逐字段一致；
11. memory plan 缩 wave、缩 collectors，并拒绝无法容纳的配置；
12. progress 按工作推进且所有过滤计数守恒；
13. metadata stage revision 变化只失效 metadata 和 finalizer。

### 10.2 Conservative 语义测试

生成包含以下情况的小型图：

- anchor 相同但 SimHash 不同；
- 无共同 anchor、但 band 近邻；
- Base 漏召回而 Widened 召回；
- Conservative 漏一条边但 component 不漂移；
- 漏一条桥边导致 component 漂移；
- Exact 样本无匹配边；
- 高成本 stratum 被过采样且加权结果正确。

测试分别断言 pair、contract/member 和 component 三层指标，不能只验证候选数。

### 10.3 基准

增加可重复、非 CI 绝对耗时门禁的 fallback benchmark：

- 稀疏 postings；
- 10% 左右密度的高频 postings；
- template/content 成本高度不平衡；
- 双维度都高密度；
- 大量 token-overlap reject；
- 大量 already-connected skip。

基准比较 serial Exact、parallel Exact、Base Conservative 和 Widened
Conservative，输出 candidates/s、lefts/s、posting visits/s、peak candidate bytes
和 recall drift。CI 只锁定结构与等价性，不以共享机器上的 wall time 作为失败条件。

## 11. 实施顺序

1. 增加当前 HEAD 的基准与失败测试；
2. 把 fallback recall mode 从硬编码 Exact 改为调用方模式；
3. 实现 WorkPlanner、分层校准和 profile 升级；
4. 抽取并复用 shared-token 的有界 parallel wave；
5. 实现 CandidateSet、dense bitmap pool 和单 token-group 位图过滤；
6. 接入 FallbackMemoryPlan、工作量进度和详细 metrics；
7. 提升 metadata stage revision；
8. 运行 targeted tests、crate tests、fmt、严格 Clippy 和 benchmark；
9. 更新 README 中 recall mode、校准失败和 Exact 风险说明。

每一步保持可编译并可独立验证。不得在一个未验证的大改中同时替换召回、候选
表示、并行和 DSU 语义。

## 12. 验收标准

实现完成必须满足：

1. 默认 Conservative 模式不再执行全局代表 fallback Exact；
2. 代表 fallback 的 candidate collection 使用有界并行 wave；
3. Exact 模式与优化前候选集合、summary 和 component 一致；新增工作量指标按
   新定义守恒，不要求 already-connected/scored 等顺序相关计数等于旧值；
4. Conservative 校准满足 0.5% contract/member、0.2% component 和可用时
   0.5% matched-pair Wilson 上界；
5. 任何自动 Exact 工作都限制在被识别的高风险 strata 和 5% 全局工作量内；
6. 高密度候选自动使用 Dense，稀疏候选不承担全量位图清零成本；
7. 实际峰值分配不超过 `--analysis-memory-limit` 的预算模型；
8. 进度能解释 raw candidates 到 scored pairs 之间的全部主要过滤去向；
9. metadata revision 为 3，resume 不重复 Prepare 和 Name；
10. targeted tests、完整 crate tests、`cargo fmt --check`、严格 Clippy 和
    `git diff --check` 全部通过；
11. benchmark 报告当前 HEAD 上 Exact/Conservative、串行/并行和 Sparse/Dense
    的相对结果，不沿用 `18348f7` 的旧 ETA 作为完成证明。
