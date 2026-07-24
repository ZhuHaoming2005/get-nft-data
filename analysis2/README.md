# analysis2

Experimental in-memory NFT analysis pipeline (standalone Cargo workspace).

Design: [`docs/superpowers/specs/2026-07-23-analysis2-experimental-design.md`](../docs/superpowers/specs/2026-07-23-analysis2-experimental-design.md)

Business semantics: [`docs/analysis/REWRITE_DESIGN.md`](../docs/analysis/REWRITE_DESIGN.md)

## Hardware

Target research host: **128 vCPU / 512 GiB RAM** (Linux preferred). The pipeline keeps
snapshot indexes + evidence in process memory; there is no disk spill. Memory exhaustion
fails the process (no approximate fallback). Prefer `--rayon-threads` near core count and
`--http-concurrency` around 32 unless provider rate limits force lower.

## Build

```powershell
cargo build --manifest-path analysis2/Cargo.toml --release
```

Binary: `analysis2/target/release/analysis2.exe` (Windows) / `analysis2` (Unix).

## Seeds JSON

`select-seeds` writes a JSON array under `--output-dir/seeds.json` (plus `seeds.audit.json`).
Hand-written seeds for `run-dedup` / `run` still work:

```json
[
  { "chain": "ethereum", "address": "0xseed", "rank": 1 }
]
```

`chain` + `address` are required; `rank` is optional. Extra fields from `select-seeds`
(`name`, `metric`, `window`, `source`, `collected_at`) are ignored by the run readers.

### `select-seeds`

```powershell
cargo run --manifest-path analysis2/Cargo.toml --release -- select-seeds `
  --output-dir ./out/seeds `
  --chains ethereum,base,polygon,solana `
  --seeds-per-chain 25 `
  --opensea-api-key $env:OPENSEA_API_KEY `
  --helius-api-key $env:HELIUS_API_KEY `
  --progress auto
```

EVM ranking uses OpenSea `thirty_days_volume` (API key required). Solana uses Magic Eden
`popular_collections?timeRange=30d` plus Helius DAS resolve when on-chain address is missing.
Incomplete chains are recorded in `seeds.audit.json` and are not backfilled from other chains.

## Phase A — offline `run-dedup`

Materialize the golden Parquet once (writes `testdata/report_golden.parquet`):

```powershell
cargo test --manifest-path analysis2/Cargo.toml -p analysis2_core --test report_golden
```

Then:

```powershell
cargo run --manifest-path analysis2/Cargo.toml --release -- run-dedup `
  --input analysis2/crates/core/testdata/report_golden.parquet `
  --seeds analysis2/crates/core/testdata/report_golden_seeds.json `
  --output-dir analysis2/crates/core/testdata/report_golden_cli_out `
  --chains ethereum,base,solana `
  --evm-chains ethereum,base `
  --progress off
```

Writes under `--output-dir` in three roots:

```text
intermediate/          # run_manifest.json, failures.jsonl, caches
detail/seeds/…         # per-seed report.json|.md
summary/
  intra_chain.*        # 单链
  chain_matrix.*       # 跨链矩阵
  cross_chain.*        # 跨链总结 (scope: cross_chain_summary)
  all_chains.*         # 全链汇总 (scope: all_chains) + batch metrics
```

## Phase C — full `run`

End-to-end: load → dedup all seeds → enrich unique candidates → deep analysis → reports.

```powershell
cargo run --manifest-path analysis2/Cargo.toml --release -- run `
  --input ./data/base.parquet `
  --input ./data/ethereum.parquet `
  --input ./data/polygon.parquet `
  --input ./data/solana.parquet `
  --seeds ./out/seeds/seeds.json `
  --output-dir ./out/run `
  --chains base,ethereum,polygon,solana `
  --evm-chains base,ethereum,polygon `
  --name-threshold 0.98 `
  --metadata-threshold 0.6 `
  --metadata-anchors 8 `
  --alchemy-api-key $env:ALCHEMY_API_KEY `
  --etherscan-api-key $env:ETHERSCAN_API_KEY `
  --helius-api-key $env:HELIUS_API_KEY `
  --opensea-api-key $env:OPENSEA_API_KEY `
  --rayon-threads 128 `
  --http-concurrency 32 `
  --progress auto
```

API keys are optional per provider: missing keys mark dependent evidence `not_requested`
and the run continues. OpenSea is used only for sales fallback when preferred providers
cannot supply amounts. Cancel / OOM paths do **not** write `status: complete` into
`intermediate/run_manifest.json`. Incomplete four-scope seeds are excluded from formal
summary denominators. Cross-chain economics in `summary/all_chains.json` sum **USD only**.

### Dedup cache (skip re-query)

After URI/Name/Metadata queries finish, `run` always writes a portable checkpoint:

```text
<output-dir>/intermediate/dedup_cache.json
```

(override with `--dedup-cache PATH`). Edges are stored with stable chain/address/token
identities (not process-local ids).

### Evidence cache (skip re-enrich / resume after interrupt)

While enrich runs, network results are checkpointed **in batches** (default every 16
candidates):

```text
<output-dir>/intermediate/evidence_cache.json       # full snapshot (rewritten each flush)
<output-dir>/intermediate/evidence_cache.jsonl      # append-only per-candidate lines
<output-dir>/intermediate/evidence_cache.meta.json  # version + params
```

(override base path with `--evidence-cache PATH`). Bundles use stable chain/address;
`contract_id` is remapped on load.

On the next `run` with the same output dir / params, the cache is **auto-resumed**
(even without `--reuse-evidence`): already-cached candidates skip HTTP; only missing
ones are fetched. `--reuse-evidence` makes a missing/invalid cache a hard error.
Seeds, pagination limits, and API-key *presence* must match the cache.

### Fast re-run (dedup + evidence)

```powershell
cargo run --manifest-path analysis2/Cargo.toml --release -- run `
  --input ... `
  --seeds ./out/seeds/seeds.json `
  --output-dir ./out/run `
  --chains base,ethereum,polygon,solana `
  --evm-chains base,ethereum,polygon `
  --reuse-dedup `
  --reuse-evidence `
  # same thresholds / inputs / seeds / keys as the cache-producing run
  --alchemy-api-key $env:ALCHEMY_API_KEY `
  ...
```

`--reuse-dedup` still loads Parquet identity (for candidate expansion + enrich), but
skips Name/URI/Metadata index build and all seed queries. Inputs, chains, thresholds,
anchors, and the seeds list must match the cache or the run fails fast.

### Evidence depth (enrich → economics)

- **EVM gas:** Alchemy/ETH `eth_getTransactionReceipt` → `TransferEvent.gas_native` /
  `fee_payer`; `quality.gas` Complete/Truncated/Empty/Failed/NotRequested.
- **EVM value flows:** native EXTERNAL transfers around operator seeds →
  `ValueFlowEdge` (Funding / Withdrawal / RevenueBackflow); Cashout vs CEX tracing is MVP-limited.
- **Solana decode:** Helius `getSignaturesForAsset` stubs → deduped `getTransaction` jsonParsed
  fills from/to/timestamp/fee and SOL value-flow edges; signature-only stubs stay Truncated.
- **MVP gaps:** Bubblegum/compressed NFT mint completeness (no token balances → Truncated);
  Cashout classification is coarse (often Withdrawal); enrich-time controllers may be empty so
  operator seeds lean on mint `fee_payer`.
- **Economics:** when `quality.gas` is Complete, Setup/Lure use operator-paid transfer gas;
  Withdrawal/Cashout edges with known `gas_native` contribute Exit.

Additional outputs vs `run-dedup`:

- `detail/candidates/<chain>__<address>.json` (streamed as each candidate finishes analysis)
- Seed reports under `detail/seeds/` include `scopes_complete`, `analysis_complete`, and `analysis` rollups
- `summary/all_chains.*` adds candidate / address / behavior / economics / data_quality / `duplicate_scale`

## CLI

```text
analysis2 select-seeds ...   # seed ranking
analysis2 run-dedup ...      # offline dedup + hit reports
analysis2 run ...            # full enrich + analysis + reports
```

## Tests

```powershell
cargo test --manifest-path analysis2/Cargo.toml
cargo build --release --manifest-path analysis2/Cargo.toml
```
