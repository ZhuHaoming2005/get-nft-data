# analysis2 Experimental In-Memory Pipeline Design

## Goal

Build a separate experimental Rust workspace `analysis2/` that implements the
business tasks in [`docs/analysis/REWRITE_DESIGN.md`](../../analysis/REWRITE_DESIGN.md)
with a simplified **execution** model optimized for a **128 vCPU / 512 GiB**
machine:

1. Load selected Parquet snapshots fully into memory.
2. Run seed-scoped Name / URI / Metadata deduplication in process.
3. Collect network evidence and run deep analysis.
4. Emit the report hierarchy defined by the rewrite design.

Dedup **algorithms** are rewritten inside `analysis2` by reference to
`dedup/crates/core` (query-to-index for ~100 seeds). Overall task list, metrics,
and deep-analysis methods follow `REWRITE_DESIGN.md`, with method hints taken
from `top_contract_analysis_rs` where useful. Runtime packaging follows the
experimental spirit of `dedup/` (all in-memory, minimal engineering gates).

## Decisions locked in brainstorming

| Topic | Choice |
|---|---|
| Relation to `analysis/` | Parallel experiment; do not depend on or modify `analysis/` |
| Business / report semantics | `docs/analysis/REWRITE_DESIGN.md` |
| Scope of v1 | Full end-to-end (load → dedup → enrich → deep analysis → reports) |
| Dedup code reuse | Rewrite query-to-index engines; no crate dependency on `dedup` |
| Architecture | Single-process pipeline (Option 1) |
| Solana Name | NFT-level matching; any NFT hit marks the whole collection |
| Anchor / token-id order | All former ascending selections use **descending** order |
| OpenSea | Use only when no alternative provider can supply a required field |

## Non-goals

- Reusing crates from `analysis/`, `dedup/`, or `top_contract_analysis_rs`
- Adding `analysis2` as a root workspace member (standalone workspace, like `dedup/`)
- DuckDB / PostgreSQL / mmap spill / external postings / hybrid execution
- Stage checkpoints, cross-run resume, or durable intermediate caches
- Config-file hard gates, cgroup/NUMA profiling, memory-lease refusal paths
- MinHash / LSH / template fingerprints / candidate quotas for Metadata
- Calling OpenSea when Alchemy or Helius can provide the same evidence

Memory exhaustion fails the process explicitly. There is no approximate fallback.

## Target environment

- Linux preferred for production runs
- 128 vCPU, 512 GiB RAM
- Input: full four-chain Parquet snapshots (same column contract as dedup /
  `REWRITE_DESIGN.md`)
- Fixed research sample: 25 seeds per chain (Base, Ethereum, Polygon, Solana),
  100 total; per-chain counts configurable via CLI

## Workspace layout

```text
analysis2/
  Cargo.toml                 # independent workspace
  README.md
  crates/
    core/                    # package analysis2_core
      src/
        lib.rs
        error.rs
        progress.rs
        entity/              # resident NFT/contract entities, pools, CSR
        parquet/             # Arrow direct scan + ordered merge
        dedup/               # name / uri / metadata (query-to-index)
        seed/                # seed selection + manifest
        enrich/              # Alchemy / Helius / (rare) OpenSea / prices
        analysis/            # attribution, lifecycle, behavior, economics
        reporting/           # JSON / Markdown / aggregates
    cli/                     # package analysis2_cli, binary analysis2
      src/
        main.rs
        pipeline.rs
```

Dependency direction: `cli` → `core` only. Engines do not call each other;
they read `ResidentStore` / `HitGraph` / `EvidenceBundle` through explicit types.

## CLI

All runtime options are CLI flags. No run config file.

### Subcommands (v1)

| Command | Role |
|---|---|
| `select-seeds` | Build per-chain top-N seed list + audit JSON |
| `run` | End-to-end: load → dedup → enrich → analyze → reports |
| `run-dedup` | Debug path: load + dedup + hit/candidate reports only (no enrich) |

### Example

```text
analysis2 run \
  --input ./data/base.parquet \
  --input ./data/ethereum.parquet \
  --input ./data/polygon.parquet \
  --input ./data/solana.parquet \
  --seeds ./seeds.json \
  --output-dir ./out \
  --chains base,ethereum,polygon,solana \
  --evm-chains base,ethereum,polygon \
  --name-threshold 0.98 \
  --metadata-threshold 0.6 \
  --metadata-anchors 8 \
  --alchemy-api-key "$ALCHEMY_API_KEY" \
  --etherscan-api-key "$ETHERSCAN_API_KEY" \
  --helius-api-key "$HELIUS_API_KEY" \
  --opensea-api-key "$OPENSEA_API_KEY" \
  --rayon-threads 128 \
  --http-concurrency 32 \
  --progress auto
```

`--opensea-api-key` is optional. If absent, OpenSea-only fields are marked
`not_requested`. Missing Alchemy/Helius keys similarly mark dependent evidence
without aborting the whole run when other work can continue.

## Data model and load

### Parquet contract

Required columns:

- `chain`, `contract_address`, `token_id`
- `name_norm`, `token_uri_norm`, `image_uri_norm`, `metadata_json`

Rules:

- Cast to UTF-8; missing or incompatible columns fail fast with file path
- Preserve input file order and in-file row number as `SourceOrder`
- Use normalized fields as-is; do not re-normalize
- Logical key: `(chain, contract_address, token_id)`
- Conflicting non-empty normalized URIs for the same key fail fast
- Name / Metadata conflicts use stable source-order first-valid-record rules

### Load path

Arrow/`parquet` column projection only. **Do not** stage full tables in DuckDB.

Pipeline:

```text
validate schemas (parallel)
  → per-file parallel row-group scan (Rayon)
  → local shards: intern strings, merge contracts, URI postings, metadata candidates
  → merge shards in explicit input order (no global hot locks)
  → build Name representatives / Solana NFT name postings, URI CSR,
    Metadata anchors + BM25 prefix index
  → drop scan scratch
```

Default load uses **two-pass** projection: pass 1 builds identity + name + URI
structures; pass 2 streams only `metadata_json` to fill anchors and BM25 docs.
A single-pass path may replace it later if benchmarks show equal peak RSS and
lower wall time.

### ResidentStore

| Layer | Contents |
|---|---|
| Identity | chains, contracts, NFTs, `SourceOrder` |
| StringPool | interned name / URI / canonical metadata bytes |
| Contract stats | `nft_count`, EVM representative Name, Metadata anchors |
| URI index | CSR postings for `token_uri` and `image_uri` |
| Name index | EVM contract-level postings; Solana NFT-level postings |
| Metadata index | prepared documents + lossless prefix postings for BM25 |
| Denominators | per-chain `total_contracts` / `total_nfts` over full snapshot |

Dimension indexes may be dropped immediately after that dimension’s hits are
materialized into a compact `HitGraph`, freeing RAM for enrich/analysis.

## Dedup engines (query-to-index)

For each seed, query all four chains (full directional `4×4`). Exclude the seed
object itself. Empty Name / empty URI / unusable Metadata do not participate.
No similarity-graph transitive closure.

Parallelism: seed × dimension × candidate-chain work units on Rayon.

### Name

| | EVM | Solana |
|---|---|---|
| Query unit | Contract representative Name | Each NFT `name_norm` in the collection |
| Hit conclusion | Whole contract + all NFTs | **Any NFT hit → whole collection + all NFTs** |

EVM representative Name selection:

1. Drop empties, null-like placeholders, and single-digit numeric names
2. Take the mode by NFT count
3. Ties: lexicographically smallest `name_norm` (tie-break only; not a scan order)

Scoring: byte-identical → `1.0`; else Jaro-Winkler; default threshold `0.98`.

Candidate reduction must be lossless (length windows, multiset-overlap bounds,
rare-prefix probes) as in `dedup` `CandidateBounds`, then exact JW verify.
Thread-local scratch and batch comparators are required.

### URI

Exact equality via CSR postings.

Order: try `token_uri_norm` first; only if that NFT/scope misses, try
`image_uri_norm`. `image_uri` is supplemental and must not double-count with an
already matched `token_uri` in the same scope.

Intra-chain: URI must appear in ≥2 distinct contracts/collections.
Cross-chain: URI must appear on both primary and secondary chains.
Contracts counted once per scope; NFTs counted by matched rows.

### Metadata

Contract/collection unit on all chains. Per object, select the first `k` valid
Metadata records ordered by token id **descending** (default `k=8`):

- EVM: arbitrary-precision non-negative integer, descending
- Solana: lexicographic string order, descending
- Same token id: first valid record in stable source order

Validity: non-empty trim, starts with `{`/`[`, valid JSON without duplicate
object keys, ≤64 KiB, canonical form not `{}`.

Alignment:

1. If anchors share token ids, compare the **largest** shared anchor token
2. Else each side takes its largest remaining anchor
3. Byte-identical canonical JSON hits immediately
4. Else BM25-weighted cosine on full canonical content; default threshold `0.6`

Only lossless BM25 pruning (zero overlap / safe upper bound). No template /
MinHash / LSH / quotas. A hit applies to the whole object and all its NFTs.

### Candidate merge

1. Union NFT hits across four dimensions; group by `(chain, contract_address)`
2. Keep per-edge dimension, score/equality, granularity, and scope
3. Multi-seed hits on one candidate: keep per-seed relations; enrich and
   candidate-level analysis run **once** per candidate
4. `legit_duplicate` stays in audit; excluded from infringing / malicious /
   honest-loss numerators after verification in the analysis stage

Outputs: compact `HitGraph` + `CandidateRegistry`.

## Enrichment and deep analysis

### Overlapped pipeline

```text
CPU: merge candidates / legit pre-tags / graph prep
IO:  dedupe candidates → bounded HTTP fetch (Tokio)
        ↓
EvidenceBundle → deep analysis (Rayon) → reports
```

- Per-provider semaphores and simple rate limits
- Shared in-flight de-dupe for identical provider requests / prices
- Finite retries only; failures recorded, not complex state machines
- No durable evidence cache across runs

### Providers (OpenSea minimized)

| Need | Preferred source | OpenSea |
|---|---|---|
| EVM transfers / holders / gas / value flow | Alchemy; Etherscan transfer fallback | No |
| EVM / Solana sales & market activity | Alchemy (EVM) / Helius (Solana) | Only if required and no alternative |
| USD pricing (UTC day bucket) | Alchemy Prices | No |
| Solana assets / history / authority | Helius | No |
| `select-seeds` EVM ranking | OpenSea 30d volume (no alternate list in v1) | Required for this command |
| `select-seeds` Solana ranking | Magic Eden 30d popular + Helius collection resolve | No |

Unrequested / failed / truncated / true-empty must remain distinct quality states.

### Deep analysis modules

Method references may follow `top_contract_analysis_rs` implementations; metrics
and definitions follow `REWRITE_DESIGN.md`:

1. `legit_duplicate` verification
2. Address attribution (operator / colluder / victim / corrupted_victim / neutral)
3. Lifecycle + value-flow timelines and aggregates
4. Behaviors: Wash Trading (SCC), Pump-and-Exit, Sybil / Fraud Revenue /
   Poisoning, Layered Transfer, Inventory Concentration
5. Economics: attacker cost (Setup/Lure/Exit), output and output/input ratio,
   honest loss (secondary + paid mint)
6. Provider data quality coverage

Graphs and SCCs are built once per candidate and reused. Default paper thresholds
match current production knobs (min cycle size 2, layered path length 3,
fan-out 3, top 10% concentration) and are CLI-overridable.

## Reports

Under `--output-dir`:

1. `seeds.json` + `seeds.audit.json`
2. `seeds/<chain>__<address>/report.json|.md` — per seed
3. `candidates/<chain>__<address>.json` — per candidate detail
4. `intra_chain.json|.md`, `chain_matrix.json|.md`, `cross_chain.json|.md`
5. `summary.json|.md` — four-chain aggregate
6. `run_manifest.json` — params, snapshot identity, completeness,
   `pricing_policy`, stage timings
7. `failures.jsonl` — seed/scope/stage/provider/retryable

JSON holds full keys and evidence; Markdown holds readable tables. Cross-chain
totals sum USD only. Every ratio includes numerator, denominator, and quality
bounds. Candidate artifacts may stream out as soon as analysis completes; the
global summary waits until all completed seed scopes are known.

No checkpoint: interrupt requires a full re-run. Do not write `complete` into
`run_manifest.json` on OOM or cancel.

## Error model

| Class | Behavior |
|---|---|
| Schema / load / conflicting URI | Fail fast, non-zero exit |
| Single seed / candidate / provider | Record failure; continue siblings |
| Incomplete four-scope seed | Excluded from formal summary denominators |
| OOM / cancel | Exit without false `complete` manifest |

## Testing

| Layer | Coverage |
|---|---|
| Unit | JW bounds, URI exactness, BM25 prune, descending anchors, Solana Name→collection |
| Tiny oracle | Hand-built Parquet; hit sets match exhaustive enumeration |
| Report golden | Fixed fixture summary field shapes |
| Optional HTTP mock | Enrich paths; no default live network in CI |

No production cgroup/NUMA/pressure-gate suites in v1.

## Performance posture

- Prefer wall-clock reduction within the 512 GiB envelope
- Keep one global copy of Name/URI/Metadata bytes
- Overlap CPU dedup/analysis with network waits via bounded queues
- Drop dimension indexes and raw provider payloads after last use
- Do not introduce disk spill or re-scan Parquet mid-run to reduce RSS

## Implementation notes

- Package names: `analysis2_core`, `analysis2_cli`; binary name `analysis2`
- Progress: `auto` | `tty` | `json` | `off` with EWMA ETA (same UX idea as `dedup`)
- Standalone `cargo build --release --manifest-path analysis2/Cargo.toml`
- Business semantics that diverge from `REWRITE_DESIGN.md` text are intentional
  and listed in “Decisions locked in brainstorming” (Solana Name, descending
  anchors, OpenSea minimization)
