# top_contract_analysis_rs 重写任务与分析策略

本文档只定义完全重写 `top_contract_analysis_rs` 时需要完成的业务任务、分析目标、
查重策略和分析方法，不规定阶段拆分、代码结构、存储形式、线程模型、缓存、恢复机制或
具体 API 实现。

查重方法参考 `dedup/crates/core` 当前代码中的 Name、URI 和 Metadata 核心判定方法，
但按本阶段目标进行适应化改造：四条链地位相同，Solana 不从任何查重维度或作用域中排除。
现有 `top_contract_analysis_rs` 代码只用于确定深入分析任务和报告统计口径，不沿用其中
旧的查重、候选召回、置信分层或综合评分策略。`docs/dedup/REWRITE_DESIGN.md` 仅作为本文档
的组织方式与表达风格参考。

## 总体目标

`top_contract_analysis_rs` 的固定研究样本为四条链各 25 个头部 NFT 合约或集合：

- Base：25 个；
- Ethereum：25 个；
- Polygon：25 个；
- Solana：25 个；
- 合计：100 个 seed。

本文所称“每链 25 个 NFT”是 25 个 NFT 项目、合约或 collection；具体 NFT token 仍是
查重证据和深入分析的基本对象。

完整任务为：

1. 独立给出每条链的 25 个 seed，并保存其排名与选择依据；
2. 将每个 seed 与四链完整 NFT 快照从 Name、`token_uri`、`image_uri`、Metadata
   四个维度进行查重；
3. 得到直接命中的 NFT、合约或 collection，并保留逐维证据；
4. 对查到的 NFT 及其所属候选合约或 collection 进行指定的链上、市场、地址、传播和
   经济影响分析；
5. 分别形成单 seed、单链、链矩阵、跨链汇总和四链总报告。

## 目标环境、数据规模与并行目标

目标设备：

- 128 vCPU；
- 512 GiB RAM；
- Linux。

生产规模以完整四链快照为基线：

| Chain | Contracts / Collections | NFTs |
|---|---:|---:|
| Base | 419,071 | 71,956,906 |
| Ethereum | 85,143 | 8,386,527 |
| Polygon | 298,963 | 39,679,029 |
| Solana | 28,499,776 | 37,590,683 |
| Total | 29,302,953 | 157,613,145 |

重写需要同时服务两类工作：

- CPU 密集型工作：快照读取、查重、候选归并、图与统计分析；
- 网络 I/O 型工作：链上、交易、市场、持有者和价格证据采集。

设计目标是让两类工作在资源边界内并行推进。网络等待不能使 CPU 分析停顿，CPU 满载也
不能造成网络请求无界增长；不同 seed 或候选对象的独立工作可以重叠执行。

## 输入与证据边界

### NFT 快照

查重读取与 `dedup` 相同的完整快照语义。每个 NFT 的逻辑主键为：

```text
(chain, contract_address, token_id)
```

至少使用以下字段：

- `chain`；
- `contract_address`；
- `token_id`；
- `name_norm`；
- `token_uri_norm`；
- `image_uri_norm`；
- `metadata_json`。

Name 和 URI 在导出快照前完成规范化。分析阶段直接使用 `name_norm`、
`token_uri_norm` 和 `image_uri_norm`，不读取原始字段重新规范化。

同一逻辑主键出现多行时，按稳定的文件顺序和文件内行号处理。冲突的非空规范化 URI
不能静默选择其一；Metadata 与 Name 使用稳定、可复现的记录选择规则。

### Seed

四条链分别独立排名，不先混合四链再截取前 100。默认参考当前 seed 获取代码，以近
30 日成交量为排名指标，每条链取得 25 个不重复且地址有效的合约或 collection。

每个 seed 至少保存：

- chain 与合约或 collection 地址；
- 链内排名；
- collection 名称与稳定标识；
- 排名指标、指标值和统计窗口；
- 数据来源与采集时间。

某条链不足 25 个有效 seed 时，该链样本不完整，不能用其他链或重复地址补足；报告必须
明确缺失数量。

### 外部证据

后续分析按需获取：

- 合约部署、创建者、管理者和 collection authority；
- NFT mint、transfer、sale、holder 和余额变化；
- 交易时间、价格、手续费和原生资产兑美元价格；
- 市场活动、成交记录和必要的资产元数据。

所有外部证据必须记录来源与观测时间。未请求、请求失败、被截断和确实不存在必须区分，
不能统一解释为零。

## 核心任务与方法

| 任务 | 目标 | 方法 |
|---|---|---|
| Seed 定义 | 固定四链各 25 个研究对象 | 各链独立排名并保存完整选择依据 |
| 多链查重 | 找到与 100 个 seed 重复或高度相似的对象 | 每个 seed 按下文四维策略比较四链完整快照 |
| 候选归并 | 形成待分析合约或集合 | 对四维命中取并集，同时保留每个维度的证据 |
| 深入分析集 | 固定真正进入后续分析的 NFT | 使用直接命中的 NFT，并按所属合约或 collection 归组 |
| 证据补全 | 获取离线快照不能提供的活动信息 | 按命中 NFT 和候选对象采集链上、市场、持有和价格证据 |
| 地址与事件分析 | 识别参与角色和资产流向 | 依据部署、资金、交易、持有和时序关系构建证据链 |
| 行为分析 | 识别可疑操纵或传播模式 | 在交易与转移关系中识别循环、拉升退出、星型和分层行为 |
| 经济影响 | 区分攻击投入、攻击收入和诚实损失 | 分阶段归集成本与收入，并对诚实参与者单独计量 |
| 报告 | 形成可复核研究结果 | 输出单 seed、候选合约、链矩阵、跨链汇总和批量汇总 |

## 查重公共语义

### 分析粒度

| 维度 | EVM | Solana |
|---|---|---|
| Name | 合约级代表 Name | collection 级代表 Name |
| `token_uri` | NFT 级 | NFT 级 |
| `image_uri` | NFT 级 | NFT 级 |
| Metadata | 合约级 | collection 级 |

### 作用域

- `intra_chain`：seed 与同链其他合约或集合之间的重复；
- `cross_chain_summary`：seed 与任意其他链对象之间的重复；
- `chain_matrix`：seed 所在主链到指定候选链的方向性重复。

每个 seed 都必须执行本链及另外三条链的比较，形成完整的 `4 × 4` 方向性链矩阵。
Solana 同时参与 Solana 链内、Solana 到其他链、其他链到 Solana，以及
Solana–Solana 的全部四维查重。

seed 自身的逻辑对象必须从候选集合中排除。

主链决定 seed、分子与分母；候选链只限定匹配对象。一个对象在同一作用域命中多个对象或
多个维度时只计一次。四维综合结果取集合并集，不能把各维计数直接相加。

空 Name、空 URI 和不可用 Metadata 不参与对应维度。相似匹配只判断直接对象对，不通过
相似图、Union-Find 或传递闭包推导额外重复关系。

## Name 查重策略

四条链统一以合约或 collection 为 Name 分析单位。一个对象内可能存在多个 `name_norm`，
选择规则为：

1. 忽略空值、常见空值占位符和无意义的单个数字；
2. 选择该合约 NFT 中出现次数最多的 `name_norm`；
3. 出现次数并列时，选择字节序最小的值。

选出的值是该合约或 collection 唯一的代表 Name。该规则同时用于 EVM 和 Solana，不排除
任何同链或跨链组合。Name 命中后，结论作用于整个合约或 collection 及其全部 NFT。

### 判定

- 字节完全相同的规范化 Name 相似度为 `1.0`；
- 其余使用 Jaro-Winkler；
- 默认阈值为 `0.98`；
- 每个 Name 对独立判定，不做传递推断。

候选缩减只能排除被证明不可能达到阈值的 Name 对，不能改变完整比较应得到的命中集合。

## URI 查重策略

URI 直接使用导出后的：

- `token_uri_norm`；
- `image_uri_norm`。

两个维度都使用规范化字符串的精确相等，不使用模糊相似度。

判定顺序为：

1. 先判断 `token_uri_norm`；
2. 对同一个 NFT 和同一个作用域，只有未命中 `token_uri` 时才判断
   `image_uri_norm`。

因此 `image_uri` 表示“token URI 未匹配、但 image URI 匹配”的补充证据，不与同一作用域
内已经命中的 `token_uri` 重复计数。

链内重复要求同一 URI 至少出现在两个不同合约或集合中；跨链重复要求该 URI 同时出现在
主链与候选链。一个合约内多个 NFT 共享同一 URI 时，合约只计一次，NFT 按实际命中的行
计数。

## Metadata 查重策略

Metadata 使用完整内容比较，不抽取合约模板，也不使用结构指纹作为最终或近似判定依据。

### 有效 Metadata

Metadata 只有同时满足以下条件才参与：

- 去除首尾空白后非空；
- 以 `{` 或 `[` 开始；
- 是不存在重复对象键的有效 JSON；
- 大小不超过 64 KiB；
- 规范化后不是空对象 `{}`。

规范化统一对象键顺序、Unicode、大小写、空白、数值表示和 attributes 顺序，但不删除
NFT 的名称、属性值、URI 或其他实际内容。

### 合约或 collection 表示

四条链统一以合约或 collection 为 Metadata 分析单位。每个对象从有效 Metadata 中选择
按 token id 升序排列的前 `k` 条作为 anchors，默认 `k = 8`：

- EVM token id 按任意精度非负整数排序；
- Solana token id 按字符串字典序排序；
- 同一 token id 有多条记录时，选择稳定来源顺序中的第一条有效记录。

### 内容对齐与判定

任意两条链之间使用同一判定方法：

1. 若两个对象的 anchors 存在相同 token id，选择最大的共同 anchor token；
2. 若不存在共同 token id，各自选择最大的 anchor；
3. 比较所选 token 的完整规范化 Metadata；
4. 内容字节完全相同直接命中；
5. 否则使用完整内容的 BM25 加权余弦相似度，默认阈值为 `0.6`。

Solana token id 通常无法与 EVM token id 对齐，此时使用双方最大的 anchor，不因此跳过比较。

选定内容达到阈值即判定两个合约或 collection 的 Metadata 重复，结论作用于该对象及其
全部 NFT。

### 候选完整性

Metadata 的候选缩减必须是无损的：只跳过能够证明无法达到阈值的内容对；无法证明时必须
保留比较，资源边界不允许静默丢弃候选。业务策略不使用模板摘要、MinHash/LSH、候选配额
或低信息 veto 缩小最终查重集合。

## 深入分析对象

四维任一维度直接命中即可进入疑似重复集合。每条匹配关系必须保存：

- seed 与候选对象的 `(chain, contract_address, token_id)`；
- 命中的具体维度；
- 精确相等值或相似度与阈值；
- 合约级或 NFT 级匹配粒度；
- `intra_chain`、`chain_matrix` 或 `cross_chain_summary` 作用域。

URI 命中只纳入实际匹配的 NFT。Name 或 Metadata 命中按合约或 collection 结论纳入该
对象的全部 NFT。四维结果取 NFT 集合并集，再按候选合约或 collection 分组进行网络证据
采集和深入分析。

同一候选 NFT 匹配多个 seed 时，每条 seed–candidate 关系分别保留；单 seed、单链或四链
总体数量按完整对象键去重。候选不能因与另一个候选相似而自动继承 seed 的重复关系。

官方迁移、授权重发和可验证的合法跨链版本单独标记为 `legit_duplicate`，保留在查重审计
中，但不进入疑似侵权、恶意行为和诚实损失的分子。

## 指定深入分析

### 地址角色

对命中 NFT 的部署、mint、transfer、sale、holder、funding、withdrawal 和 cashout 证据
进行地址归因，区分：

- `suspected_operator`：部署、管理、资金准备、收入接收或退出证据指向的运营地址；
- `suspected_colluder`：参与循环交易、协同分发或资金回流的关联地址；
- `likely_victim`：付费 mint 或购买后仍持有疑似重复 NFT，且没有运营证据的地址；
- `corrupted_victim`：先以受害者身份获取 NFT，随后参与转售或扩散的地址；
- `neutral`：只有弱共现或证据不足的地址。

单独的 mint、sale、共同持有或地址共现不能直接判为恶意。每个标签必须保留证据类型、
相关合约、token、交易、权重和置信度。

### 生命周期与传播

对每个候选合约或 collection 构建 NFT 传播路径与价值流，统计：

- 部署、首次 mint、首次 transfer、首次 sale 和首次受害者出现时间；
- 从部署到首次 transfer、sale 和受害者出现的时间；
- 传播涉及的 NFT、地址节点、事件边、mint、transfer、sale 数量；
- 恶意、受害、诚实和仍持有受害地址数量；
- gross revenue、operator revenue、marketplace fee；
- funding、withdrawal、revenue backflow 的金额与边数；
- 最大价值接收地址、接收金额及其占比；
- 首次 sale 或受害事件前已经出现的独立风险信号数。

早期信号以“已观察到 sale 或受害结果、部署时间有效、结果发生前至少存在两个独立风险
信号”为阳性条件。

### Wash Trading

仅使用疑似恶意地址之间的 sale 边构图，以强连通分量表示循环交易。节点数达到最小阈值
才形成一个 wash cycle，默认最小节点数为 `2`。

每个 cycle 统计：

- 参与地址数；
- 涉及 NFT 数和 token 交易次数的 Gini 系数；
- 首末交易区块跨度；
- 虚假成交原生币金额和 USD 金额。

同时按 `2`、`3`、`4`、`5+` 个节点统计 cycle 数量和比例。

### Pump-and-Exit

在 wash cycle 结束后，若 cycle 参与者向诚实买家出售 NFT，且退出成交均价高于 cycle
内部均价，则记为 Pump-and-Exit。

统计：

- 从 cycle 结束到首次退出成交的时间；
- 退出均价与 cycle 均价之比；
- 退出 NFT 数与 cycle NFT 数之比；
- 关联诚实买家数；
- 关联买家支付的原生币与 USD 损失。

### Sybil Distribution、Fraud Revenue 与 Poisoning

将传播图的强连通分量压缩为有向无环图。包含疑似恶意地址且下游分支数达到阈值的分量
视为星型中心，默认 fan-out 阈值为 `3`：

- 存在继续向下游传播的分支：`Sybil Distribution`；
- 不继续传播但存在 sale 或正金额：`Fraud Revenue`；
- 只有无价或弱价值扩散：`Poisoning`。

每类统计中心数、边数、地址数、NFT 数、平均 fan-out、总价值、直接关联诚实买家数及
关联损失。

### Layered Transfer

在 NFT 转移图中识别不重复地址的多跳路径，默认至少包含 `3` 个地址。每条路径统计：

- 涉及 NFT 数、地址数和路径长度；
- 零或低价值跳数；
- 首末事件时间差；
- 路径累计原生币和 USD 价值。

低价值跳的参考边界为同时不高于 `1 USD` 和 `0.001` 单位原生资产。

### Inventory Concentration

当疑似恶意地址从多个来源接收 NFT，或高 fan-out 地址重新集中回收库存时，记为库存集中
模式。每个中心统计：

- 来源地址数和入站交易数；
- 汇集 NFT 数占候选合约全部相关 NFT 的比例；
- 汇集价值占全部相关价值的比例；
- 汇集时间窗口。

### 诚实买家

对付费 mint 或二级市场购买后仍持有疑似重复 NFT 的地址，统计：

- 仍持有的 NFT 数；
- 购买与付费 mint 总成本；
- 与其直接关联的行为模式；
- 从候选合约首次活动到首次购买的时间；
- 从首次购买到分析时点的持有时间。

## 报告指标与统计方法

### Seed 样本与执行完整性

| 指标 | 统计方法 |
|---|---|
| `selected_seed_count` | 每链固定为有效 seed 数，目标为 `25` |
| `analyzed_seed_count` | 完成四个候选链作用域的 seed 去重计数 |
| `failed_seed_count` | 未完成全部四个作用域的 seed 去重计数 |
| `seed_completion_ratio` | `analyzed_seed_count / selected_seed_count` |
| `seed_with_duplicate_count` | 至少存在一条直接重复关系的 seed 去重计数 |
| `seed_duplicate_ratio` | `seed_with_duplicate_count / analyzed_seed_count` |

以上指标分别按四条链和全部 100 个 seed 输出。一个 seed 的四个链作用域必须全部完成，
才能进入正式汇总。

### 重复规模

重复规模分别对 `token_uri`、`image_uri`、`metadata`、`name` 和 `total` 输出：

- `duplicate_nft_count`：主链中至少存在一个直接匹配的 NFT，以
  `(chain, contract_address, token_id)` 去重；
- `duplicate_contract_count`：主链中至少存在一个直接匹配 NFT 的合约或 collection，
  以 `(chain, contract_address)` 去重；
- `duplicate_nft_ratio`：`duplicate_nft_count / primary_chain_total_nfts`；
- `duplicate_contract_ratio`：
  `duplicate_contract_count / primary_chain_total_contracts`。

`total` 是四维对象集合的并集，不是四行计数之和。所有比例同时输出 numerator 和
denominator。

报告作用域包括：

- `intra_chain`：同链查重；
- `chain_matrix`：方向性的主链到候选链；
- `cross_chain_summary`：主链对象在任意其他链存在匹配；
- `all_chains`：四条链和 100 个 seed 的总体结果。

### 候选与合法重复

| 指标 | 统计方法 |
|---|---|
| `representative_candidate_count` | 去重后的候选 `(chain, contract_address, token_id)` 数 |
| `candidate_contract_count` | 去重后的候选 `(chain, contract_address)` 数 |
| `suspected_duplicate_contract_count` | 排除 seed 本身和合法重复后的候选合约或 collection 数 |
| `infringing_nft_count` | 排除合法重复后的候选 NFT 数 |
| `legit_duplicate_contract_count` | 经迁移、授权或官方关系验证的候选合约数 |

### 地址分类

| 指标 | 统计方法 |
|---|---|
| `malicious_address_count` | 具有 operator 或 colluder 强证据、且未被归为诚实参与者的唯一地址数 |
| `repeat_infringing_malicious_address_count` | 与两个及以上疑似重复合约有关的恶意地址数 |
| `honest_address_count` | 具有付费获取或仍持有损失证据、且无强运营证据的唯一地址数 |
| `total_address_count` | 恶意与诚实地址集合并集大小 |

详细报告同时保留 operator、colluder、victim、corrupted victim 和 neutral 标签及其证据。

### 合约行为与行为汇总

每个候选合约输出 Wash Trading、Pump-and-Exit、三类星型行为、Layered Transfer、
Inventory Concentration 和诚实买家明细。

每种行为的汇总指标为：

- `contract_count`：出现该行为的唯一候选合约数；
- `contract_coverage_ratio`：
  `contract_count / 完成行为分析的疑似重复合约数`；
- `instance_count`：该行为实例数；
- `instance_ratio`：`instance_count / 全部行为实例数`；
- `address_count`：该行为涉及的唯一地址数；
- `nft_count`：该行为涉及的唯一 NFT 数；
- `linked_buyer_count`：与该行为直接关联的唯一诚实买家数；
- `linked_loss`：这些买家的直接可归因损失。

`total` 行对合约、地址、NFT 和买家使用集合并集，避免同一对象跨行为重复计数。

### 攻击者成本

攻击者成本只统计可证明由疑似运营或恶意地址支付的 gas：

- `Setup`：合约部署和资金准备；
- `Lure`：付费 mint、刷量、诱导成交和相关 sale；
- `Exit`：withdrawal、cashout 和退出支付。

同一合约同一交易只计一次；同一交易跨阶段时按 `Exit > Lure > Setup` 归入更具体阶段。
分别输出三个阶段及总 gas 的原生币和 USD 金额。

逐交易明细保留候选合约、阶段、channel、交易哈希、gas payer、gas 金额、双方角色和
证据类型。

默认还统计前 `10%` 疑似重复合约的成本集中度：

```text
top_contract_contribution_ratio
= 前 ceil(疑似重复合约数 × 10%) 个合约的 gas USD
  / 全部疑似重复合约的 gas USD
```

### 攻击者产出与产出投入比

攻击者产出只包括：

- 进入候选合约或运营地址的 mint payment；
- 接收者明确为运营地址的 sale payment 和 royalty；
- 由运营地址发起的 exit payment。

protocol fee、普通第三方 royalty、withdrawal 和 cashout 的重复转移不作为新增产出。
每条价值边只计一次。

对每个有正产出的候选合约统计：

```text
output_input_ratio = operator_output_usd / attacker_gas_input_usd
```

同时输出总产出、总投入、总体产出投入比，以及在投入大于零的合约中
`output_input_ratio >= 1` 和 `< 1` 的数量与比例。

### 诚实参与者损失

诚实损失分为：

- `secondary_sale_loss`：诚实买家购买后仍持有的疑似重复 NFT 成本；
- `paid_mint_loss`：诚实地址付费 mint 后仍持有的成本；
- `total_loss`：两者之和。

套牢 NFT 数为二级市场仍持有数与付费 mint 仍持有数之和：

```text
stuck_nft_ratio = stuck_nft_count / 全部疑似重复 NFT 数
```

若无法取得完整疑似重复 NFT 分母，才退回使用已观察到的二级市场购买和付费 mint NFT
数量，并在数据质量中标记该退化口径。

套牢时间比例按诚实买家的 sale 边汇总：

```text
stuck_time_ratio
= Σ(分析时点 - 购买时点)
  / Σ(购买时点 - 候选合约首次活动时点)
```

另按候选合约归集 USD 损失，并用与攻击成本相同的前 `10%` 口径计算损失集中度。

### 数据质量

数据质量至少输出：

- sale 价格可解析数、总数及比例；
- 候选 asset 列表已分析数、总数、覆盖率及截断合约数；
- 历史记录已请求、成功、完整、失败、未请求和截断的 asset 数；
- 已获取、provider 报告和失败的交易数及交易覆盖率；
- 签名发现失败、交易详情失败和未归因 Solana 交易数；
- 未解析 compressed mint、缺失 mint 前余额和缺失 collection authority 数；
- 补充 provider 与质量查询失败数；
- 每个 seed、链作用域和整个批次的失败记录。

只有分子、分母和数据完整性状态同时有效时才输出覆盖率。未请求、请求失败、截断和真实
零值必须分别表示。

## 输出目标

重写后的报告分为：

1. **Seed 清单与审计报告**：四条链各 25 个 seed、排名依据和采集时间；
2. **单 seed 报告**：四维直接查重证据、候选 NFT、候选合约及全部深入分析指标；
3. **候选合约明细**：传播路径、地址证据、行为实例、成本、产出和诚实损失；
4. **链内报告**：每条主链的 `intra_chain` 统计；
5. **链矩阵报告**：完整方向性 `4 × 4` 统计；
6. **跨链报告**：每条主链的 `cross_chain_summary`；
7. **四链总报告**：100 个 seed 的去重汇总、行为汇总、经济影响和数据质量。

JSON 保存完整对象键、证据和统计，Markdown 提供研究阅读所需的表格与摘要。跨链汇总不
直接相加不同链的原生资产，只汇总具有有效汇率的 USD 金额。任何比例或金额都必须同时
输出分子、分母、去重键口径和数据质量边界。
