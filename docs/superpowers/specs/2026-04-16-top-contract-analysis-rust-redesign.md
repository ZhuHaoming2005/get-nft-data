# Top Contract Analysis Rust 重构设计

## 背景

现有 `top_contract_analysis` 已经形成完整能力，但实现分散在 Python 包与 `rust_ext/top_contract_analysis_rust` 扩展之间：

- Python 负责 CLI、批量流程、DuckDB/Parquet、HTTP API 访问、报告输出。
- Rust 扩展只覆盖部分匹配与信号分析逻辑。

用户要求在新目录中将 `top_contract_analysis` 一次性重构为独立 Rust 实现，并完整覆盖现有能力，而不是继续沿用 Python 作为运行时入口。

## 目标

在新目录 `top_contract_analysis_rs/` 中构建一个独立 Rust CLI，完整替代当前 `top_contract_analysis` 的运行时能力，包括：

- 单个 seed 分析
- 批量分析
- PostgreSQL 到 Parquet 的快照导出
- DuckDB 特征库加载与候选召回
- 链上与第三方 API 数据拉取
- JSON 与 Markdown 报告输出
- 进度事件与缓存复用

## 非目标

本次重构不包含以下事项：

- 继续保留 `python -m top_contract_analysis` 作为主入口
- 通过 PyO3 或 Python 绑定保留混合运行时
- 对现有分析指标做语义级改造
- 为无关模块做统一仓库级 Rust 化改造

## 总体方案

采用单一 Cargo 包加内部模块化方案，在新目录中提供一个独立二进制：

- `top_contract_analysis_rs analyze ...`
- `top_contract_analysis_rs batch ...`
- `top_contract_analysis_rs export-snapshot ...`

该方案的原则是：

- 交付上一次性覆盖现有能力
- 运行时不依赖 Python
- 输出契约尽量兼容现有 Python 版本
- 充分复用现有 `rust_ext/top_contract_analysis_rust` 中已经验证过的算法实现

## 目录结构

建议的新目录结构如下：

```text
top_contract_analysis_rs/
  Cargo.toml
  src/
    main.rs
    cli/
    models/
    normalize/
    api/
    store/
    analysis/
    reporting/
    progress/
    error.rs
  tests/
```

模块职责如下：

- `src/main.rs`
  负责 CLI 入口和子命令分发。
- `src/cli/`
  使用 `clap` 定义 `analyze`、`batch`、`export-snapshot` 参数，并尽量保持与现有参数名一致。
- `src/models/`
  承载领域模型、报告 payload、序列化结构和内部传输对象。
- `src/normalize/`
  实现名称、symbol、URL、metadata 文档与关键词归一化逻辑。
- `src/api/`
  封装 Alchemy、Etherscan、OpenSea、RPC 请求，统一超时、重试、解析与限流。
- `src/store/`
  封装 PostgreSQL 读取、DuckDB 特征库、Parquet 导入导出、signal cache。
- `src/analysis/`
  封装 seed 分析主流程、候选召回、合同级聚合、高低置信分析和地址信号分析。
- `src/reporting/`
  负责 JSON payload 生成、Markdown 报告渲染、默认输出路径与批量汇总。
- `src/progress/`
  负责单 seed 与 batch 的进度事件抽象。
- `src/error.rs`
  统一错误类型与上下文包装。

## 命令设计

### analyze

对应现有 `python -m top_contract_analysis` 主流程。

核心输入：

- `--chain`
- `--seed-contract-address`
- `--alchemy-api-key`
- `--alchemy-network`
- `--etherscan-api-key`
- `--opensea-api-key`
- `--name-threshold`
- `--metadata-threshold`
- `--timeout`
- `--max-tokens-per-contract`
- `--max-recall-rows`
- `--output`
- `--feature-parquet`
- `--feature-db`
- `--signal-cache-db`

核心行为：

1. 拉取种子合约 metadata。
2. 拉取种子 NFT 列表和 license sample。
3. 加载 DuckDB 特征库或从 Parquet 预热特征库。
4. 召回候选重复 NFT。
5. 以候选合约为粒度分组。
6. 对高置信候选补全 transfer、owner、sale 等信号。
7. 汇总 report payload。
8. 输出 JSON 与 Markdown。

### batch

对应现有 `python -m top_contract_analysis.batch`。

核心行为：

1. 读取 seed 文件。
2. 跳过目标输出目录中已缓存完成的 seed 报告。
3. 并发执行多个单 seed 分析任务。
4. 聚合批量 summary。
5. 输出汇总 JSON 与 Markdown。

### export-snapshot

对应现有 `python -m top_contract_analysis.export_snapshot`。

核心行为：

1. 流式读取 PostgreSQL NFT 表。
2. 在 Rust 中计算预处理字段：
   - `token_uri_norm`
   - `image_uri_norm`
   - `name_norm`
   - `symbol_norm`
   - `metadata_doc`
   - `metadata_keywords_arr`
3. 写出 Parquet 快照。

## 数据模型

Rust 领域模型需要完整承接当前 Python 模型语义，至少覆盖：

- `SeedNft`
- `DatabaseNftRecord`
- `ContractNameRecord`
- `ContractSignal`
- `ContractMetadata`
- `DuplicateCandidate`
- `TransferRecord`
- `NftSaleRecord`
- `TransactionReceiptRecord`
- `EthTransferRecord`
- `OwnerBalance`
- `DatabaseSnapshot`

报告输出结构需显式建模，避免使用无结构的 `serde_json::Value` 在核心流程中传递。建议将以下 payload 独立建模：

- 单 seed 输出 payload
- `report_summary`
- `infringing_tokens`
- `malicious_addresses`
- `honest_addresses`
- `victim_addresses`
- 批量 summary payload

## 主数据流

### export-snapshot 数据流

```text
PostgreSQL
  -> stream rows
  -> normalize + metadata preprocessing
  -> Parquet writer
```

要求：

- 流式处理，避免全量加载内存
- 支持较大的 `fetch-size`
- 输出字段顺序与现有快照保持一致

### analyze 数据流

```text
CLI args
  -> fetch seed metadata/NFT/license
  -> feature store init
  -> candidate recall from DuckDB
  -> group by contract
  -> enrich high-confidence contracts with chain signals
  -> build report payload
  -> write JSON/Markdown
```

要求：

- 单 seed 分析内部可使用 async HTTP，并结合并发上限控制
- 高置信候选合约的信号分析应支持受限并发
- signal cache 命中时应跳过重复链上拉取与重复分析

### batch 数据流

```text
seed file
  -> resolve pending seeds
  -> concurrent analyze tasks
  -> collect per-seed output metadata
  -> build batch summary
  -> write summary outputs
```

要求：

- 批量模式需要保留跳过已完成 seed 的行为
- 批量汇总逻辑需与现有 summary 指标一致

## 存储与缓存设计

### DuckDB feature store

Rust 实现需支持：

- 加载 Parquet 数据到 DuckDB
- 当 Parquet 包含预计算字段时直接使用
- 严格模式下缺列直接失败
- 非严格模式下允许慢速回退
- `max_tokens_per_contract`
- `max_recall_rows`
- 候选 contract 的两阶段查询与 per-contract cap

### signal cache

Rust 实现需保留：

- 基于 DuckDB 的缓存存储
- key 为 `chain + contract_address + token_type`
- 缓存内容包含：
  - mint recipients
  - active sellers
  - address signals
  - victim signals
  - transfers
  - owners

### 兼容现有产物

新实现必须尽量兼容以下现有产物：

- `output/top_contract_analysis/*.parquet`
- 已存在的 DuckDB feature DB
- 已存在的 signal cache DB
- 批量输出目录中的历史 JSON/Markdown 结果

## API 与并发设计

Rust 版本统一采用 async HTTP 客户端，建议基于 `reqwest` + `tokio` 实现。

需要支持：

- 全局并发上限
- 合约级并发上限
- sale metric 级并发上限
- 可配置 timeout
- 瞬时失败重试
- Alchemy 失败时按现有行为回退到 Etherscan 或 OpenSea

需要保留的解析行为包括：

- `fetch_seed_contract_nfts` 的分页处理
- owner 零地址过滤
- transfer 区块时间优先取 metadata，缺失时回查 block
- ERC721 transfer 拉取失败时回退 Etherscan
- sale 数据的 Alchemy/OpenSea 回退逻辑

## 报告与输出契约

### 文件命名

需要继续使用：

- `top_contract_analysis__<slug>.json`
- `top_contract_analysis__<slug>.md`
- `top_contract_analysis__summary.json`
- `top_contract_analysis__summary.md`

### JSON 契约

需要保持以下顶层结构与关键字段稳定：

- `seed_contract`
- `report_summary`
- `suspected_infringing_duplicates_high_confidence`
- `suspected_infringing_duplicates_low_confidence`
- `address_signals`
- `victim_signals`
- `infringing_tokens`
- `malicious_addresses`
- `honest_addresses`
- `victim_addresses`

批量模式需保持：

- `batch_summary`
- `seed_reports`

### Markdown 契约

Markdown 的排版可以做有限整理，但必须保持：

- 指标含义不变
- 汇总项不丢失
- 单 seed 与 batch 的主要指标可直接人工比对

## 兼容性要求

新 Rust CLI 需要主动兼容以下外部约定：

- 尽量复用现有参数名
- 保持输出文件命名规则不变
- 保持 JSON payload 字段结构和指标语义不变
- 保持 Markdown 报告的指标语义不变
- 复用现有 Parquet、DuckDB 和批量缓存产物

不兼容点仅限入口形式：

- 不再使用 `python -m top_contract_analysis`
- 新主入口为 Rust 二进制及其子命令

## 测试策略

实现前先在 Rust 中建立测试覆盖，优先把现有 Python 回归测试迁移为 Rust 测试。

### 单元测试

覆盖：

- 名称归一化
- metadata 文档抽取
- 名称相似度
- metadata 相似度
- transfer signal 分析
- victim signal 分析
- infringing token、malicious address、victim address、honest address 构建

### 存储层测试

覆盖：

- Parquet 导出字段
- DuckDB 特征库导入
- strict parquet 行为
- per-contract token cap
- max recall rows
- signal cache 读写

### API 层测试

覆盖：

- 分页
- 重试
- fallback
- block timestamp 回填
- owner/sale/receipt 解析

### CLI 集成测试

覆盖：

- `analyze`
- `batch`
- `export-snapshot`
- 输出文件命名
- JSON/Markdown 结构

优先迁移的现有测试语义来自：

- `tests/test_top_contract_analysis.py`
- `tests/test_top_contract_analysis_accelerated.py`
- `tests/test_top_contract_analysis_low_confidence_pytest.py`

## 实施顺序

建议按以下顺序执行：

1. 创建 `top_contract_analysis_rs` Cargo 项目骨架。
2. 迁移并整理现有 `rust_ext/top_contract_analysis_rust` 算法模块。
3. 建立 Rust 领域模型与序列化结构。
4. 迁移 `normalize`、`reporting`、`signal cache`、DuckDB feature store。
5. 实现 `export-snapshot`。
6. 实现单 seed `analyze` 主流程。
7. 实现 `batch` 与批量缓存复用。
8. 补足 CLI 集成测试与回归测试。
9. 与现有 Python 输出做结构和关键指标对比，确保可替代。

## 风险与对应策略

### 风险 1：现有行为分散在 Python 与 Rust 扩展之间

策略：

- 先迁移已有 Rust 算法
- 用测试固定行为后再迁移外围流程

### 风险 2：DuckDB 与 Parquet 行为细节回归

策略：

- 保留现有字段与查询语义
- 用存储层测试锁住 strict/non-strict、两阶段召回和 token cap 行为

### 风险 3：API 回退逻辑在重构中丢失

策略：

- 显式为 fallback 建测试
- 不在第一阶段抽象过度

### 风险 4：输出结构变动影响下游

策略：

- 报告 payload 使用强类型模型
- 增加 JSON 顶层结构与关键字段回归测试

## 结论

本次重构将以新目录中的独立 Rust CLI 取代现有 Python 运行时，完整覆盖 `top_contract_analysis` 的全部能力，并以输出兼容、缓存复用、测试先行作为主要约束。实现上采用单一 Cargo 包加内部模块化，以控制复杂度并保证一次性交付。
