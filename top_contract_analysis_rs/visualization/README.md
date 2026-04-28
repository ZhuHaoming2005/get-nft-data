# NFT 传播路径可视化工具

本目录只负责可视化 `top_contract_analysis_rs` 导出的 JSON，不参与 Rust 分析流程。

可视化实现使用的前端库：

- `Cytoscape.js`: 合约级 NFT 传播网络图、节点/边选择和布局。
- `ECharts`: 地址角色与边类型分布统计。

## 使用方式

1. 重新运行 Rust 分析，生成带 `nft_propagation_paths` 字段的 JSON。
2. 在浏览器中打开 `propagation_viewer.html`。
3. 选择导出的 `top_contract_analysis__*.json` 文件。
4. 在合约下拉框中选择某个疑似侵权合约，查看 NFT mint、转移、成交、受害者、诚实持有者和恶意地址传播图。

旧 JSON 如果没有 `nft_propagation_paths` 字段，只能显示提示；完整路径必须由 Rust 重新导出。
