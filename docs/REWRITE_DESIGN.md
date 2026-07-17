# name_uri_analysis_rs 查重策略

本文仅定义 `name_uri_analysis_rs` 重写后的查重任务和业务策略，不规定阶段划分、代码结构或执行实现。

## 运行目标与数据规模

目标运行设备：

- 128 vCPU；
- 512 GiB RAM。

当前生产数据规模以现有 `summary.csv` 中的主链总量为基准：

| 链 | 合约数 | NFT 数 |
|---|---:|---:|
| Base | 419,071 | 71,956,906 |
| Ethereum | 85,143 | 8,386,527 |
| Polygon | 298,963 | 39,679,029 |
| Solana | 28,499,776 | 37,590,683 |
| 合计 | 29,302,953 | 157,613,145 |

这些数字用于确定重写版本需要支持的实际数据量级。开发时尽量使用最优时间、空间复杂度。

## Parquet 数据来源规范

### 数据来源

Parquet 文件应由各链 NFT 数据库的完整快照导出，而不是由不同时间点的临时查询结果拼接。每一行
表示一个 NFT，逻辑主键为：

```text
(chain, contract_address, token_id)
```

### 字段规范

| 字段 | 要求 | 用途 |
|---|---|---|
| `chain` | 非空 UTF-8 字符串，使用稳定的小写链名 | 确定所属链 |
| `contract_address` | 非空 UTF-8 字符串 | 确定合约或 collection |
| `token_id` | 非空 UTF-8 字符串，同一链内使用统一表示 | 对齐同一 NFT |
| `name_norm` | Parquet 导出端已经规范化的名称，可为空 | Name 查重 |
| `token_uri_norm` | Parquet 导出端已经规范化的 token URI，可为空 | `token_uri` 查重 |
| `image_uri_norm` | Parquet 导出端已经规范化的 image URI，可为空 | `image_uri` 查重 |
| `metadata_json` | 内容为完整 Metadata JSON 字符串，可为空 | Metadata 模板与内容对比 |

Name 和 URI 规范化均在 Parquet 导出前完成。直接使用 `name_norm`、
`token_uri_norm` 和 `image_uri_norm`，不读取原始值重新计算，也不对规范化结果执行第二次转换。

EVM 地址统一为小写；Solana 地址保持 Base58 大小写。

### 从 Parquet 读取数据

多个 Parquet 作为一张逻辑表读取。读取前检查所有文件是否包含必需字段，以及同名字段是否能够
统一转换为 UTF-8 字符串。只读取查重所需列，并保留输入文件顺序和文件内行号作为稳定来源顺序。

逻辑读取形式如下：

```sql
SELECT
    lower(trim(CAST(chain AS VARCHAR))) AS chain,
    trim(CAST(contract_address AS VARCHAR)) AS contract_address,
    trim(CAST(token_id AS VARCHAR)) AS token_id,
    coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm,
    coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
    coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
    coalesce(nullif(trim(CAST(metadata_json AS VARCHAR)), ''), '') AS metadata_json,
    filename,
    file_row_number
FROM read_parquet(
    ['base.parquet', 'ethereum.parquet', 'polygon.parquet', 'solana.parquet'],
    filename = true,
    file_row_number = true
);
```

## 通用统计口径

- EVM 合约地址统一转为小写；Solana 地址保留 Base58 大小写。
- 空名称、空 URI 和不可用 Metadata 不参与对应字段的查重。
- `total_contracts` 和 `total_nfts` 使用主链全部有效非空合约与 NFT 数作为分母，不使用查重可分析子集作为分母。
- 输出以下三种范围：
  - `intra_chain`：主链内部重复；
  - `cross_chain_summary`：主链对象与任意其他链对象重复；
  - `chain_matrix`：主链对象与指定次链对象重复。
- `chain_matrix` 有方向。`primary_chain` 决定分母以及被统计的合约和 NFT，`secondary_chain` 只限定匹配对象来源。
- 相同对象在同一统计范围内命中多个重复对象时，`duplicate_contract_count` 和
  `duplicate_nft_count` 只统计一次。

## Name 查重

每个合约的name是一样的，因此name按合约地址聚合，不做重复查重。

### 已规范化输入

Name 查重直接读取 Parquet 中的 `name_norm`。分析程序不处理原始 `name`，也不重复执行名称
规范化。规范化后的完全相同名称视为相似度 `1.0`。

### 判定

- 使用 Jaro-Winkler 相似度；
- 默认阈值为 `95.0`，即内部相似度 `0.95`；
- 每个名称对独立比较并独立给出是否重复的判断；
- 不使用 Union-Find；
- 不做传递闭包；

## URI 查重

### 已规范化输入

URI 查重直接读取 Parquet 中的：

- `token_uri_norm`；
- `image_uri_norm`。

分析程序不处理原始 `token_uri` 或 `image_uri`。

### 判定

URI 使用规范化结果的精确相等判定，不使用模糊相似度，也不使用传递推断。

指标定义为：

- `token_uri`：token URI 与目标范围内其他对象相同；
- `image_uri`：token URI 未命中，但 image URI 相同；

### 范围语义

- 链内重复要求同一个规范化 URI 在主链至少关联两个不同合约；
- 跨链汇总要求主链 URI 在至少一个其他链出现；
- 链对矩阵要求主链 URI 在指定次链出现；
- 同一合约内多个 NFT 使用相同 URI 时，NFT 按实际命中行统计，合约只统计一次。

## Metadata 查重

Metadata 查重采用“合约模板对比—同 token ID 完整内容对比”的两层判断。模板用于判断两个合约
是否可能属于相同或高度相似的 NFT 集合；完整内容用于确认同一 token 在两个合约中是否实际重复。

### 有效 Metadata

只有同时满足以下条件的 Metadata 参与比较：

- 内容非空；
- 去除首尾空白后以 `{` 或 `[` 开头；
- 能够作为 JSON 解析；
- 大小不超过 64 KiB。

同一个合约的 Metadata 按 `token_id` 组织。一个 token 有多条来源记录时，按输入文件顺序和文件内
行号选择第一条有效记录，保证每次运行使用同一内容。

### 模板生成方式

模板从同一合约的多条有效 Metadata 中归纳，不直接把某一个 token 的完整 Metadata 当作合约模板。

模板保留集合中重复出现的结构和稳定信息：

- JSON 字段路径和对象层级；
- attributes 中稳定出现的 `trait_type`；
- description、collection、creator、royalty、license 等集合级字段；
- 字段值的数据类型；
- 在多个 token 中保持一致的文本或平台信息。

明显随 token 变化的内容不作为固定模板值，例如：

- token 名称中的编号；
- token ID；
- image、animation 和 external URL 中的具体资源地址；
- attributes 中每个 NFT 独有的 `value`；
- 仅在单个 token 中出现的偶然字段。

这些可变位置在模板中保留字段路径，但不保留其值。

模板文本统一执行 NFKC、Unicode 小写和空白折叠。最终模板表达的是“这个合约的 Metadata 通常
具有哪些字段、结构和稳定集合信息”，而不是某个 NFT 的具体图片、属性值或编号。

### 模板对比

先对不同合约的模板进行相似度比较。模板相似表示两个合约具有相近的 Metadata 组织方式和集合级
语义，因此可以继续检查具体 NFT 内容；模板不相似则不判为 Metadata 重复。

模板完全一致可以直接通过模板对比。非完全一致模板使用 BM25 衡量规范化模板文本，相似度阈值为
`0.6`。模板对比只负责筛选合约对，不能单独作为最终重复结论。

### 按 token ID 对比完整内容

通过模板对比后，取两个合约共同拥有的一个 token ID，并只比较相同 token ID 的 Metadata：

```text
contract A / token 1  ↔ contract B / token 1
contract A / token 2  ↔ contract B / token 2
```

不得使用 A 的 token 1 与 B 的 token 2 比较，也不得各选一个代表 token 后比较，因为这会把正常的
token 间内容差异误判为合约差异。

完整内容包括该 token Metadata 中的全部 JSON 内容，包括名称、描述、attributes、图片、动画、
外部链接、collection 信息和其他辅助字段。比较前统一 JSON 对象字段顺序、attributes 对齐方式、
Unicode NFKC、Unicode 小写和空白形式。规范化只消除表示形式差异，不删除 token 的实际名称、
属性值、URI 或其他内容。

相同 token ID 的完整内容完全一致时直接命中；不完全一致时使用完整内容 BM25，相似度达到 `0.6`
视为该 token 重复。一个合约对至少有一个共同 token ID 通过完整内容比较，才判定为 Metadata
重复合约对。

双方没有共同 token ID 时，取各token id最小的 metadata 进行查重。

由于solana链上的token id为账户地址，不可能相同，涉及solana的部分直接选字典序最小者参与查重。

### 速度优化

metadata查重允许使用并查集或预筛选等方式加速，允许可控的结果偏差。
