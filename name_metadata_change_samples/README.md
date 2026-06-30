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

如需从 OpenSea trending collections 生成 Ethereum、Base、Polygon 和 Solana
四条链各自的 seed 输入文件：

```powershell
$env:OPENSEA_API_KEY="..."
python .\scripts\fetch_opensea_top_seeds.py --output-dir .\seeds --limit 100
```

默认输出 `ethereum.seeds.txt`、`base.seeds.txt`、`polygon.seeds.txt` 和
`solana.seeds.txt`。脚本逐链调用
`GET https://api.opensea.io/api/v2/collections/trending`，从响应中的合约地址字段提取
collection 地址。EVM 地址输出为小写；Solana 地址必须是可解码为 32 字节的 Base58
值并保留原始大小写。默认读取 `OPENSEA_API_KEY`，也可用 `--api-key` 显式传入。

兼容原来的单链文件用法：

```powershell
python .\scripts\fetch_opensea_top_seeds.py `
  --chain ethereum `
  --output .\seeds.txt `
  --limit 100
```

也可以用 `--chains ethereum base polygon solana` 显式选择链集合。该 Python 脚本
只负责下载和验证 seed 文件，不会启动 EVM 行为分析或 Solana transfer/gas 分析。

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

- `Modification Summary`：name 按修改标签统计 `count (percent)`；metadata 按“语义区域 × 操作类型”矩阵统计。`total matches` 是该侧参与统计的重复记录数；标签采用叠加统计，同一条重复记录可同时计入多个标签，因此各标签比例相加可能超过 100%。seed name 为空、`None`、`null`、全问号或无字母数字等无效值时，该 seed 的 name matches 不参与 name 统计。
- `Name Matches`：每个有效 seed 的 name，以及查到重复的合约级代表 name 文本；每条 match 前显示对应标签。
- `Metadata Matches`：每个 seed 的 metadata，以及查到重复的 metadata 文本；每条 match 前显示对应标签。

Name 标签只保留可解释的高层改动方式：`exact_clone`、`format_perturbation`、`suffix_augmentation`、`lexical_mutation`。`format_perturbation` 覆盖大小写、空白、不可见 Unicode 和 Unicode 兼容字符变化；`suffix_augmentation` 覆盖 `404`、`v2`、`gen2`、`2.0`、`1st`、`2nd`、罗马数字、`2D`、`3D`、`VX`、`AI`、`XR`、`GIF`、`FC`、`ID`、`ART`、`(TEST)` 等编号/版本/形态后缀，也覆盖 `.fun`、`x`、`official`、`nft`、`club`、`dao`、`pass`、`mint`、`claim`、`free`、`vip`、`collection`、`edition`、`clone`、`copy`、`reloaded`、`remastered` 等衍生语义后缀；`lexical_mutation` 覆盖拼写变体、单复数变化、近似词替换和可识别品牌名变形。不再输出 `case_change`、`spacing_change`、`unicode_compatibility`、`invisible_unicode`、`token_number_suffix`、`derivative_suffix`、`ai_marker` 和 `homoglyph_or_typo` 这类细粒度探索标签。

Metadata 标签只基于可解析 JSON 的 path diff，不再使用文本相似度启发式。横向语义区域为 `title`、`description`、`attributes`、`references`、`auxiliary_fields`、`platform_fields`、`structure`；纵向操作类型为 `added`、`removed`、`replaced`、`reordered`。`title`、`description`、`attributes`、`references`、`auxiliary_fields` 计入 content-bearing changes；`platform_fields` 和窄口径 `structure` 计入 non-content-bearing changes。完全一致的 metadata 计入 `exact_match`；无法解析为 JSON 且发生变化的 metadata 计入 `unparseable_changed`，不混入 `other`；JSON 中未知 path 的变化按 path-based 的 `auxiliary_fields` 统计。`null`、空字符串、空数组和空对象这类无内容字段的 added/removed 不计入修改，避免 `external_url: null` 等占位字段抬高结果。
