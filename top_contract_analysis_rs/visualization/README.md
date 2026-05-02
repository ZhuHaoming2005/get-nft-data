# NFT 复制/侵权模型可视化工具

本目录只负责可视化 `top_contract_analysis_rs` 导出的 JSON，不参与 Rust 分析流程。

可视化实现使用的前端库：

- `Cytoscape.js`: 合约级 NFT 传播网络图、节点/边选择和布局。大量同源 mint/transfer 会按合约、方向和边类型聚合，sale 与同交易 transfer 会合并展示为 sale 边。
- `ECharts`: 地址角色与边类型分布统计。

页面直接消费 Rust 导出的研究模型字段：

- `nft_propagation_paths`: NFT mint、transfer、sale 传播图和 token 级路径。
- `contract_lifecycle_events` / `lifecycle_metrics`: 复制准备、仿冒部署、复制铸造、传播、变现、受害购买、退场等生命周期阶段。
- `value_flow_edges`: mint payment、sale payment、protocol fee、royalty fee、funding、withdrawal 等资金流证据。
- `weak_supervision_labels` / `address_evidence_features` / `campaign_clusters`: 地址归因、弱监督标签和活动簇证据。
- `early_detection_features`: 早期检测窗口样本和正负/未标注弱标签。

## 使用方式

1. 重新运行 Rust 分析，生成带 `nft_propagation_paths` 字段的 JSON。
2. 在浏览器中打开 `propagation_viewer.html`。
3. 选择导出的 `top_contract_analysis__*.json` 文件。
4. 在合约下拉框中选择某个疑似侵权合约，查看 NFT mint、转移、成交、受害者、诚实持有者和恶意地址传播图。
5. 使用“仅受害者相关”过滤器收缩到 victim token 的完整传播链，减少无关节点干扰。
6. 右侧侧边栏分为“全局模型”和“选中对象”两个标签页：前者展示报告级研究模型概览，后者展示当前合约、token、节点或边的生命周期、弱监督和资金证据。

旧 JSON 如果没有 `nft_propagation_paths` 字段，只能显示提示；完整路径和生命周期模型必须由 Rust 重新导出。
