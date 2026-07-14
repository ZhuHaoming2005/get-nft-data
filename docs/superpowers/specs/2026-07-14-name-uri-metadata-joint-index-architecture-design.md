# `name_uri_analysis_rs` metadata 联合索引架构设计

日期：2026-07-14  
状态：设计已确认，实施中

## 1. 目标与边界

- 默认 Conservative 模式在 13,533,773 个代表 fallback atoms 上以单机 24 小时内完成为容量目标。
- 显式 Exact 模式的候选集合、最终 component 和 summary 语义不得改变。
- `JointBaseEquivalent` 必须与当前 Conservative Base 的候选集合等价；只有受校准保护的 bounded profile 可以引入现有门限内的漏召回。
- 最终 template BM25、content BM25、token-group eligibility 和 DSU union 语义保持不变。
- 新增常驻和瞬时内存必须计入 `--analysis-memory-limit`，不得依据系统空闲内存绕过硬预算。

## 2. 架构

metadata fallback 拆成以下独立单元：

1. `FeatureStore`：并行构建两维 SimHash、anchors、document frequency 和不可变 token-group 特征。
2. `JointCandidateIndex`：为 8 个 template band 与 8 个 content band 的 64 个组合建立确定性 CSR。
3. `RecallController`：校准 `JointBaseEquivalent`，并对出现 exact 漏边的 `(chain, exact-work bucket)` 启用受预算限制的 per-left Exact rescue。
4. `ProductionWorkPlanner`：按最终 profile 的 joint、anchor 和 token-filter 实际成本进行 difficult-first 排序。
5. `ParallelCandidateCollector`：只读并行收集候选；不访问可变 DSU。
6. `AdaptiveTokenDisjointFilter`：候选较少时使用精确 scalar merge，候选较多时使用仅扫描 `right > left` 的 suffix bitmap。
7. `OrderedScoringConsumer`：并行评分，按确定顺序串行执行 connected check 和 union。
8. `MetadataPerformanceMetrics`：分别记录 build、joint band、anchor、token exclusion、connected、scoring 和 union 成本。

现有 `metadata/index/mod.rs` 只保留稳定类型和 façade；候选索引、召回、工作计划、过滤和消费逻辑不再互相读取内部可变状态。

## 3. 联合 band CSR

每个 atom 对 64 个 `(template_band_index, content_band_index)` family 各写入一次：

```text
bucket = (template_band_value << 8) | content_band_value
```

每个 family 具有 65,536 个 bucket。构建时对 atom index 做两遍递增扫描：第一遍计数并形成 offsets，第二遍按 atom index 写入 postings。因此 posting 顺序确定，无需比较排序，也不依赖线程调度。

在 13,533,773 atoms 上，核心联合 posting 约占 `N * 64 * 4 = 3.23 GiB`，offsets 约 32 MiB。64 个 family 可以独立并行，但 active builders 由内存计划限制。

Base band 候选满足：两维分别至少共享一个对应 band，当且仅当至少共享一个联合 family bucket。因此该路径不得产生相对当前 Base 的新增召回偏移。

## 4. Anchor 混合召回

- 稀有 template anchor posting 扫描后立即验证 content candidate predicate。
- 稀有 content anchor posting 扫描后立即验证 template candidate predicate。
- band×band、anchor×band、band×anchor 和 anchor×anchor 的并集必须与当前 Conservative 候选条件等价。
- 重型 anchor 不得静默截断。`JointBaseEquivalent` 继续精确处理；只有 `JointBounded` 可以在校准与 holdout 均通过时应用 posting-work 上限或重型 anchor 联合子索引。

全量 24×24 特征笛卡尔索引不在首轮实现范围内；只有指标证明重型 anchor 主导时才构建数据依赖的稀疏联合子索引。

## 5. Recall 风险门禁

- tuning 样本只用于选择 profile，独立 holdout 用于最终验收。
- 分层至少包含 chain、实际工作量 bucket、anchor-heavy 状态和 token-group 形态。
- 同时验证 matched pair、duplicate contract members 和最终 component members。
- Exact matched-edge 样本不足以计算既定 Wilson 上界时必须 fail closed 或扩大样本，不能视为零风险。
- 8-bit band 的 1-bit widened probe 不能覆盖允许 32-bit Hamming 距离的最终谓词，因此不得把二次 Widened 校准当成召回修复。
- 校准发现漏边时，以 `(chain, exact-work bucket)` 为风险层，整层切换到 per-left Exact；rescue postings 总预算硬限制为 10 亿。
- 超出 rescue 预算的风险层继续使用 Base，而不是退回无界全组 Exact 或中止整个 pipeline；必须通过 `recall_risk_exceeded_groups` 与 `unrescued_recall_risk_strata` 明确暴露。这一策略以用户已接受极少量漏召回为前提。
- 只有改变 Conservative 候选语义的 profile 才提升 metadata stage revision；Base 等价重构不得改变输出版本语义。

## 6. 工作计划、进度和内存

生产顺序只能使用最终 profile 的实际成本：

```text
joint suffix visits
+ rare/heavy anchor visits
+ estimated token-filter work
```

Exact 成本仅用于校准和被选中的 per-left rescue；其余 left 的正式 ETA 必须使用 Conservative 实际成本。进度分别报告 planned total、visited candidate、visited exclusion、rescue left/work 和未救援风险层。

内存计划同时覆盖 resident、build transient、每 worker scratch、wave buffers、scoring batch、DSU 和 summary scratch。预算不足时依次缩减 family builder 并发、wave、collector 数和可选重型 anchor 缓存；仍不足则在大型分配前失败。

## 7. 确定性与错误处理

- CSR postings、difficult-first tie-break 和同一 left 候选均按 atom index 确定排序。
- collector 可以并行，DSU mutation 必须保持有序串行。
- 任何 bucket/count/index 溢出或内存预检失败均返回明确错误；rescue 超预算不得扩大为全组 Exact，而是保留 Base 并输出未救援风险指标。
- 不完整 family、worker panic 或 metrics 守恒失败不得继续消费候选。

## 8. TDD 与验收

失败优先测试至少覆盖：

1. 联合 band 与旧双维度 band 交集候选等价。
2. anchor×band、band×anchor、anchor×anchor 和空文档。
3. Base/Widened profile、`right > left` 和去重顺序。
4. scalar 与 suffix-bitmap token 过滤等价，且 exclusion visits 单独计账。
5. 实际 Conservative work 排序，不再使用 Exact 估算代替。
6. 不同线程数和 wave size 的候选、component 与 summary 一致。
7. tuning/holdout 分离及低样本 fail-closed。
8. 联合索引、transient build 和每 worker scratch 均纳入内存硬预算。

最终验收需要完整生产规模运行证明 24 小时目标；小型 benchmark 只作为回归和相对比较，不作为完成证明。
