# Name/Metadata Change Samples

从本地 `nft_features` DuckDB 中读取 seed 合约，独立比较 `name` 和 `metadata` 文本相似性，并输出 Markdown 样例，方便人工观察复制合约改动了哪些 name/metadata 字段。

## 输入

输入文件每行一个合约地址，空行和 `#` 开头的注释会被忽略。

```text
0xseedcontract...
# 0xignored...
```

## 运行

```powershell
cargo run --release -- \
  --feature-db ../output/top_contract_analysis/features.duckdb \
  --input ./seeds.txt \
  --output ./result.md
```

如需从 OpenSea 当前 Top NFT collections 按 30 日交易量生成 seed 输入文件：

```powershell
$env:OPENSEA_API_KEY="..."
python .\scripts\fetch_opensea_top_seeds.py --output .\seeds.txt --limit 100 --chain ethereum
```

脚本只调用官方 API `GET https://api.opensea.io/api/v2/collections/top`，默认使用 `sort_by=thirty_days_volume`、`chains=ethereum`，从响应的 `collections[].contracts[].address` 提取合约地址，最终按一行一个小写合约地址写入 `seeds.txt`。默认读取 `OPENSEA_API_KEY`，也可用 `--api-key` 显式传入。

常用参数：

- `--chain`：默认 `ethereum`
- `--name-threshold`：默认 `95.0`
- `--metadata-threshold`：默认 `0.6`
- `--max-recall-rows`：name 或 metadata 命中文本输出上限，默认 `0` 表示不限制
- `--max-seed-tokens`：读取 seed 合约 token 行上限，默认 `0` 表示不限制
- `--duckdb-threads` / `--duckdb-memory-limit`：DuckDB 资源参数；`--duckdb-threads 0` 表示使用本机可用并行度

进度显示分两层：总进度按 seed 合约推进，当前 seed 内的小进度按读取 seed、加载 name 候选、评分 name、加载 metadata 候选、评分 metadata 和完成阶段推进。seed 合约之间串行处理；启动时会一次性从 DuckDB 加载本地 name/metadata 文本索引，name 候选按合约聚合，metadata token 会 intern 成整数 id，合约内 name/metadata 候选评分使用 Rayon 并行。

## 输出口径

工具输出三个部分：

- `Modification Summary`：按 name/metadata 分别统计修改方式标签，输出 `count (percent)`。`total matches` 是该侧参与统计的重复记录数；标签采用叠加统计，同一条重复记录可同时计入多个标签，因此各标签比例相加可能超过 100%；未命中任何规则时计入 `other`。seed name 为空、`None`、`null`、全问号或无字母数字等无效值时，该 seed 的 name matches 不参与 name 统计。
- `Name Matches`：每个有效 seed 的 name，以及查到重复的合约级代表 name 文本；每条 match 前显示对应标签。
- `Metadata Matches`：每个 seed 的 metadata，以及查到重复的 metadata 文本；每条 match 前显示对应标签。
