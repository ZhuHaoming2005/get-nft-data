# Name/Symbol Stats V2

独立于现有脚本的第二版 NFT 名称与 `symbol` 重复统计流水线。

目标：

- 保持“按合约聚合、按 NFT 数统计比例”的统计口径
- 把 `symbol` 统计完全下推到 PostgreSQL
- 把 `name` 模糊统计改成“唯一规范名原子 + 分片任务 + C++ worker”
- 控制内存占用，避免在 Python 中生成或持久化海量候选对

主要表：

- `nsv2_contract_identity`
- `nsv2_name_atoms`
- `nsv2_name_work_items`
- `nsv2_name_match_edges`
- `nsv2_name_duplicate_groups`
- `nsv2_symbol_duplicate_groups`
- `nsv2_duplicate_summary`

执行流程：

```powershell
python -m name_symbol_stats.main build-contract-identity --run-label apr01 --chains ethereum base polygon solana
python -m name_symbol_stats.main symbol-stats --run-label apr01 --chains ethereum base polygon solana
python -m name_symbol_stats.main prepare-name-tasks --run-label apr01 --chains ethereum base polygon solana --blocking-strategy adaptive_v1 --max-atoms-per-task 30000
cmake -S name_symbol_stats/cpp -B name_symbol_stats/cpp/build
cmake --build name_symbol_stats/cpp/build --config Release
python -m name_symbol_stats.main run-name-worker --run-label apr01 --worker-exe name_symbol_stats/cpp/build/Release/name_worker.exe --thresholds 85 90 95 --parallel-workers 12 
python -m name_symbol_stats.main finalize-name-stats --run-label apr01 --chains ethereum base polygon solana --thresholds 85 90 95
python -m name_symbol_stats.main export-report --run-label apr01 --output-dir name_symbol_stats_output
```
