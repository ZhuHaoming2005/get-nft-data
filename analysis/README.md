# 四链头部 NFT 合约分析

本 crate 实现 Base、Ethereum、Polygon、Solana 四链头部 NFT 查重与深入分析。
生产运行要求 Linux cgroup v2、至少 128 个有效 CPU 和 464 GiB 任务可用内存。

## 配置

本 crate 以 package `analysis` 纳入仓库根 Cargo workspace，库名为
`analysis`，二进制为 `analysis`；它与 workspace 内保留的旧
package `top_contract_analysis_rs` 名称不同。以下命令可在 `analysis/` 目录执行。先编辑
[`config/default.toml`](./config/default.toml)，填写 `snapshot_files` 和 API Key：

```toml
[api_keys]
alchemy = ""
etherscan = ""
opensea = ""
helius = ""
```

空 Key 不会触发对应 API；`select-seeds` 必须配置 `opensea` 和 `helius`。完整分析建议配置
`alchemy`、`helius` 和 `opensea`，`etherscan` 仅用于 EVM 转账回退。缺少必要数据源时程序
仍可运行，但会将对应证据标记为 `not_requested` 或 `truncated`。Key 内容不写入运行清单
或运行 ID；运行 ID 只记录各 Key 是否已配置。请勿提交包含真实 Key 的配置文件。

每条链选取的头部合约数量由配置独立控制；未配置时各链默认均为 `25`：

```toml
[seed_top]
base = 10
ethereum = 25
polygon = 15
solana = 20
```

`select-seeds` 按这些数量生成清单；`run` 和 `run --seeds <路径>` 也会按同一配置校验每条
链的数量以及从 `1` 开始连续且不重复的排名。

默认供应商地址和 Alchemy 网络位于同一配置文件：

```toml
[provider_endpoints]
opensea = "https://api.opensea.io"
magic_eden = "https://api-mainnet.magiceden.dev/v2"
etherscan = "https://api.etherscan.io/v2/api"
helius = "https://mainnet.helius-rpc.com/"
alchemy_prices = "https://api.g.alchemy.com/prices/v1"

[provider_endpoints.alchemy_networks]
ethereum = "eth-mainnet"
base = "base-mainnet"
polygon = "polygon-mainnet"
```

程序直接请求供应商，不依赖外部归一化网关：

- seed 选择：Base、Ethereum、Polygon 使用 OpenSea 近 30 日成交量排名；Solana 使用
  Magic Eden 的 `30d` 热门集合排名，并通过 Helius DAS 将样本 mint 解析为可验证的
  Metaplex collection address。Solana 清单将指标明确记录为 `popularity_rank`，不把
  Magic Eden 当前实际返回的 7 日统计伪装成 30 日成交量。
- OpenSea 统一提供四链 **sale** 市场证据（不再请求 listing/上下架）；不使用 Alchemy 或 Helius
  市场结果。
- EVM：Alchemy 提供合约、持有者、转账、资金流、receipt、gas 和**事件当日 UTC 日桶** USD
  价格；Alchemy 转账失败时使用 Etherscan ERC-721/1155 转账回退。
- Solana：Helius DAS 提供资产、持有者、authority、mint/transfer 历史、手续费及同交易
  资金流；Helius 的 sale/listing 标签不进入市场证据。

计价口径写入 `run_manifest.json` 的 `pricing_policy` 与总报告 Markdown：原生币 USD 换算使用
Alchemy Prices 的同日（UTC 日历日）历史日桶价格，跨链汇总只累加 USD micros，不跨链相加
wei/lamports。

请求共享连接池和供应商级限流器；同一候选的独立证据分支并行执行，同日价格按原生资产和
UTC 日桶 single-flight 缓存。全合约预取升级时复用既有证据，仅补新增关系验证。分页结果
受 `provider_page_limits` 约束，达到上限会标记为 `truncated`。

## 推荐运行命令

```bash
cargo run --release -- select-seeds --config config/default.toml
cargo run --release -- run --config config/default.toml
```

配置中的相对路径以配置文件所在目录为基准。需要使用其他 seed 文件时再传
`--seeds <路径>`。

开发验证：

```bash
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

## 输出

每次运行创建独占结果目录。每个候选只执行一次 JSON 序列化与 zstd 压缩，先写入 run 内
staging 文件，再原子移动到最终 artifact 路径；writer queue 按实际压缩字节限流。全部
候选产物和最终报告完成后统一执行一次 run 级 durability 屏障，随后才写入 `_SUCCESS`。
没有 `_SUCCESS` 的目录属于不完整结果，不会被复用。
