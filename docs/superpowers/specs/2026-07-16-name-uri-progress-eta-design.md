# name_uri_analysis_rs 当前进度与 ETA 修复设计

## 目标与范围

本次只修复当前运行的终端进度展示和当前子阶段 ETA，不新增跨运行历史预测，也不估算五阶段整体 ETA。首要修复 `MetadataEncode` 中 `classify retained-token sources` 长时间运行却没有百分比和 ETA、错误显示 Match ETA，以及指标名称含糊的问题。

## 已确认方案

使用 Prepare 阶段已经构建并在 Encode admission 中读取的 `metadata_contract_token_rows` 行数作为精确 token-group 总量。每一行对应一个 `(contract_index, token_index)` 工作组。主查询继续单次流式执行，不增加等价 `COUNT JOIN`，不使用会漂移的动态总量外推。

## 进度语义

- `EncodeTokenSources` 改为确定进度事件，total 为已知 `token_rows`。
- completed 按完成分类的 `(contract_index, token_index)` 组推进，而不是按候选 JSON 行数或成功选中的 source 数推进。
- 查询按组排序，但同一组可能跨 Arrow batch。只有遇到下一组时才确认上一组完成；流结束后再确认最后一组，保证组不重计、不漏计。
- 即使某组没有可用 source，也必须计入 completed。最终 completed 必须等于 total；不满足时应返回数据一致性错误，而不是伪造 100%。
- 工作单位显示为 `token groups`；成功选择的 source 使用独立的 `selected` 诊断计数，不再借用 `matched`。

## 展示与 ETA

目标展示结构：

```text
⠁ pipeline [00:01:11] [####>-------------------] 1/5 metadata encode
  ⠉ stage [00:01:11] [#######>--------------------] 1/4  25% opened Prepare DuckDB
    ⠒ task [00:01:11] [################>---------------] 22,097,544/44,752,896 49% classify retained-token sources
      ⠚ metrics 308.4K token groups/s · ETA 1m 13s · selected 21,830,112 sources
```

- 任务条使用确定 total，因此展示进度条、位置和百分比。
- ETA 继续使用现有预热与 EWMA 逻辑，但输入改为已完成 token group 数；零增量刷新不改变速率。
- metrics 行不重复任务名称；大整数使用千位分隔，吞吐量使用紧凑单位。
- `match ETA` 只在 `PipelineStage::MetadataMatch` 中启用。Metadata Encode 以及其他阶段不得显示 `match ETA n/a (uncalibrated)`。
- total 为零时沿用 `skipped` 语义，不显示无意义的吞吐或 ETA。

## 代码边界

- `metadata_engine::progress` 增加可复用的 token-group 工作单位和 `selected` 诊断计数。
- `analysis/metadata/encode.rs` 负责按组上报确定进度并校验 terminal completed。
- `analysis/progress.rs` 负责阶段限定的 Match ETA、数字格式化和终端文本渲染。
- 不改变 metadata 选择算法、排序、内存准入、artifact 内容、恢复语义或分析结果。

## 错误处理

- 如果上报完成的 token group 数超过或小于已知 total，Encode 失败并给出计划值与实际值。
- Arrow batch 边界不得造成重复计数；空表必须产生精确的 `0/0` skipped 事件。
- 进度展示失败或历史 Match ETA 缺失不得影响分析业务结果；本次不改变该既有原则。

## 测试与验证

实现采用 TDD：

1. 先增加失败测试，证明 `EncodeTokenSources` 当前为 unknown total，且非 Match 阶段错误显示 Match ETA。
2. 覆盖一个工作组跨 Arrow batch、无可用 source 的组仍推进、terminal completed 等于 total。
3. 覆盖渲染结果：确定任务条、`token groups`、`selected sources`、紧凑吞吐、千位分隔，以及 Encode 不含 Match ETA。
4. 运行 progress/encode 定向测试，再运行 `name_uri_analysis_rs` 全量测试、严格 Clippy、格式检查和 `git diff --check`。

## 非目标

- 不估算完整 Pipeline 或完整 Metadata Match 的剩余时间。
- 不新增或修改跨运行历史样本。
- 不通过额外全量扫描换取候选 JSON 行级 total。
- 不调整四个 Encode stage 的业务划分。
