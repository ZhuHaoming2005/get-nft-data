# name_symbol_stats

按合约地址聚合 NFT 的 `name` / `symbol`，只做重复情况统计，不改原始业务表。

## CLI

```bash
python -m name_symbol_stats.main build-contract-identity
python -m name_symbol_stats.main symbol-stats
python -m name_symbol_stats.main name-stats --thresholds 85 90 95
python -m name_symbol_stats.main export-report
```

## 设计约束

- 统计单位是合约，不是单条 NFT
- 比例按命中重复组的 NFT 总数计算
- `symbol` 走精确匹配
- `name` 先做 blocking，再做相似度精排和聚类
- 默认输出单链、跨链汇总、链对链矩阵三类结果

## 依赖

- 必需：`psycopg2`
- 推荐：`rapidfuzz`、`polars`

未安装 `rapidfuzz` 时会退回 `difflib`，未安装 `polars` 时会继续导出文本和 CSV，并跳过 Parquet。
