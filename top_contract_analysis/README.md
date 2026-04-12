# top_contract_analysis

用于分析一个 Top NFT 种子合约是否存在重复发行、疑似侵权扩散和受害者信号。

这套脚本默认按大数据场景使用。

推荐流程很简单：

1. 先把整条链的 NFT 快照导出成 Parquet。
2. 再让 `top_contract_analysis` 从 Parquet 载入到 DuckDB 做召回分析。

不要默认直接走数据库全量扫描。数据量大时，先导出 Parquet 会更稳。

## 推荐流程

### 第一步：导出链快照 Parquet

先导出一次链级快照：

```powershell
python -m top_contract_analysis.export_snapshot `
  --chain ethereum `
  --output output/top_contract_analysis/ethereum.parquet
```

产物示例：

- `output/top_contract_analysis/ethereum.parquet`

这个文件可以重复使用，不需要每跑一个 seed 都重新导出。

### 第二步：执行单个种子合约分析

推荐命令：

```powershell
python -m top_contract_analysis `
  --chain ethereum `
  --seed-contract-address <seed_contract_address> `
  --alchemy-api-key <alchemy_key> `
  --feature-parquet output/top_contract_analysis/ethereum.parquet `
  --feature-db output/top_contract_analysis/features.duckdb `
  --signal-cache-db output/top_contract_analysis/signals.duckdb
```

执行时会做这些事：

1. 拉取种子合约元数据和种子 NFT。
2. 从 Parquet 载入 DuckDB 特征库。
3. 在 DuckDB 中召回候选重复 NFT。
4. 对高置信候选合约补充 transfer、owner、sale 等链上信号。
5. 输出 JSON 和 Markdown 报告。

默认输出到当前目录下的 `result/`：

- `top_contract_analysis__<seed_name>.json`
- `top_contract_analysis__<seed_name>.md`

## 批量执行

如果要批量跑很多 seed，先准备一个文本文件，例如 `seeds.txt`：

```text
0xseed1
0xseed2
0xseed3
```

然后执行：

```powershell
python -m top_contract_analysis.batch `
  --chain ethereum `
  --seed-file seeds.txt `
  --alchemy-api-key <alchemy_key> `
  --feature-parquet output/top_contract_analysis/ethereum.parquet `
  --feature-db output/top_contract_analysis/features.duckdb `
  --signal-cache-db output/top_contract_analysis/signals.duckdb `
  --output-dir result/top_contract_analysis_batch `
  --workers 4
```

批量模式会：

1. 读取 `seeds.txt`。
2. 复用 Parquet 和 DuckDB。
3. 为每个 seed 生成单独报告。
4. 额外生成一个汇总报告。

汇总文件：

- `top_contract_analysis__summary.json`
- `top_contract_analysis__summary.md`

## 常用参数

大数据场景下，通常只需要关心这几个参数：

- `--chain`
  链名，默认 `ethereum`。
- `--alchemy-api-key`
  必填。
- `--feature-parquet`
  Parquet 快照路径，推荐始终传。
- `--feature-db`
  DuckDB 特征库路径，推荐给一个固定文件路径，便于复用。
- `--signal-cache-db`
  DuckDB 信号缓存路径，推荐给一个固定文件路径，便于批量复用。
- `--workers`
  批量并发数。常用 `2` 到 `4`。

## 参数说明

### `python -m top_contract_analysis.export_snapshot`

- `--chain`
  要导出的链，默认 `ethereum`。
- `--output`
  输出 Parquet 文件路径，必填。
- `--fetch-size`
  每次从 PostgreSQL 拉取多少行再写入 Parquet，默认 `100000`。
  数据量很大时通常保持默认即可。
- `--keep-metadata-json`
  额外把原始 `metadata_json` 一起写进 Parquet。
  默认不写，目的是减少文件体积。

### `python -m top_contract_analysis`

- `--chain`
  链名，默认 `ethereum`。
- `--seed-contract-address`
  种子合约地址，必填。
- `--alchemy-api-key`
  Alchemy API key。也可以通过环境变量 `ALCHEMY_API_KEY` 提供。
- `--alchemy-network`
  指定 Alchemy 网络名。
  默认留空，脚本会按 `chain` 自动选择网络。
- `--etherscan-api-key`
  Etherscan key。
  当 Alchemy 的 ERC721 transfer 拉取失败时，可作为回退数据源。
- `--opensea-api-key`
  OpenSea key。
  当成交记录无法从 Alchemy 获取时，可补充 OpenSea sale/event 数据。
- `--name-threshold`
  名称相似度阈值，默认 `95.0`。
  数值越低，名称模糊召回越宽。
- `--metadata-threshold`
  metadata 文本相似度阈值，默认 `0.55`。
  数值越低，metadata 相似匹配越宽。
- `--timeout`
  单次链上/API 请求超时时间，单位秒，默认 `30`。
- `--max-tokens-per-contract`
  DuckDB 召回阶段，每个候选合约最多保留多少 token，默认 `500`。
  主要用于控制超大合约拖慢分析。
- `--max-recall-rows`
  单个 seed 最多召回多少条 token 级候选记录，默认 `50000`。
  设为 `0` 表示不限制。
- `--output`
  指定 JSON 输出文件路径。
  如果不传，默认写到当前目录的 `result/` 下。
- `--feature-parquet`
  Parquet 快照路径。
  大数据场景下推荐始终传这个参数。
- `--feature-db`
  DuckDB 特征库路径，默认 `:memory:`。
  推荐改成固定文件路径，便于重复使用。
- `--signal-cache-db`
  DuckDB 信号缓存路径，默认 `:memory:`。
  推荐改成固定文件路径，便于复用 transfer/owner 分析结果。

### `python -m top_contract_analysis.batch`

- `--chain`
  链名，默认 `ethereum`。
- `--seed-file`
  seed 文件路径，必填。
  文件中一行一个合约地址，空行和 `#` 注释行会被忽略。
- `--alchemy-api-key`
  Alchemy API key。
- `--alchemy-network`
  指定 Alchemy 网络名；默认按 `chain` 自动选择。
- `--etherscan-api-key`
  Etherscan key。
- `--opensea-api-key`
  OpenSea key。
- `--name-threshold`
  名称相似度阈值，默认 `95.0`。
- `--metadata-threshold`
  metadata 相似度阈值，默认 `0.55`。
- `--timeout`
  单次请求超时秒数，默认 `30`。
- `--output-dir`
  批量结果输出目录，默认 `result`。
- `--workers`
  批量并发数，默认 `1`。
  常用 `2` 到 `4`。
- `--feature-parquet`
  Parquet 快照路径。
- `--feature-db`
  DuckDB 特征库路径。
- `--signal-cache-db`
  DuckDB 信号缓存路径。
- `--strict-parquet`
  要求 Parquet 必须包含预计算特征列。
  如果缺列则直接失败，而不是退回慢速路径。
- `--max-recall-rows`
  每个 seed 的 token 级候选召回上限，默认 `50000`，`0` 表示不限制。
- `--max-tokens-per-contract`
  每个候选合约最多保留多少 token 参与分析，默认 `500`。

## 推荐参数

如果没有特殊需求，直接用下面这组：

- `--workers 4`
- 其余阈值保持默认

默认阈值已经适合先跑首版结果，不建议一开始就调很多参数。

## 建议

建议长期固定下面三个文件路径：

- `output/top_contract_analysis/ethereum.parquet`
- `output/top_contract_analysis/features.duckdb`
- `output/top_contract_analysis/signals.duckdb`

这样后续无论是单跑还是批量跑，流程都比较简单：

1. 必要时更新一次 Parquet。
2. 直接执行分析脚本。
