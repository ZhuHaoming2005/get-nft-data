# dedup2 Experimental In-Memory Deduplicator Design

## Goal

Build a separate experimental Rust workspace `dedup2/` that implements the
deduplication strategies in `docs/REWRITE_DESIGN.md` and
`docs/REWRITE_ARCHITECTURE.md` with a simplified **execution** model: load the
selected Parquet inputs fully into memory, then compute Name / URI / Metadata
duplicates in process.

Business matching strategies for Name and Metadata follow the architecture
document (lossless JW candidate bounds; template digest + MinHash/LSH + quotas +
shared-token BM25). Only the runtime packaging is simplified relative to
production `dedup/`.

## Non-goals

- Reusing crates from `dedup/`
- Config files (`*.toml` run configs)
- Disk spill / hybrid / external execution modes (`external_postings`, external
  candidate sets, mmap spill volumes)
- Stage `_SUCCESS` checkpoint resume
- Metadata recall-audit quality gates (truncation / probe stats still go into
  `run_manifest.json`; a full stratified oracle audit is out of scope for v1)
- Hardware / NUMA / cgroup profiling artifacts

## Workspace layout

```text
dedup2/
  Cargo.toml
  README.md
  crates/
    core/                 # entities, engines, scopes, stats
      src/
        lib.rs
        entity.rs
        parquet.rs
        scope.rs
        stats.rs
        name/
          mod.rs          # atoms, canonical names, JW verify
          candidate_bounds.rs
          postings.rs     # occurrence-token resident postings
        uri.rs
        metadata/
          mod.rs
          anchors.rs
          template.rs
          prefilter.rs    # exact digest + MinHash/LSH + quotas
          canonical_json.rs
          bm25.rs
          verify.rs       # shared-token judgment
    cli/                  # clap args, progress+ETA, report writers
```

Directories are `crates/core` and `crates/cli`. Package names are
`dedup_core` and `dedup_cli` (a Cargo package cannot be named `core` because it
shadows `::core` used by proc-macros such as clap). The binary name is `dedup2`.

## CLI

All runtime options are CLI flags. No config file.

```text
dedup2 all \
  --input base.parquet \
  --input ethereum.parquet \
  --input polygon.parquet \
  --input solana.parquet \
  --output-dir ./out \
  --chains base,ethereum,polygon,solana \
  --evm-chains base,ethereum,polygon \
  --name-threshold 95.0 \
  --metadata-threshold 0.6 \
  --metadata-anchors 8 \
  --template-jaccard-threshold 0.9 \
  --lsh-bands 0 \
  --lsh-rows-per-band 0 \
  --max-outgoing-candidates-per-contract 64 \
  --max-candidates-per-target-chain 32 \
  --neighbors-per-target-chain 16 \
  --progress auto \
  --progress-interval-ms 1000
```

When `--lsh-bands` / `--lsh-rows-per-band` are `0`, derive `(b, r)` from
`--template-jaccard-threshold` and a fixed target candidate recall, as in
`REWRITE_ARCHITECTURE.md` §13.2 / `REWRITE_ACCEPTANCE.md`. Derived values and
the predicted template-Jaccard candidate recall are written to
`run_manifest.json`.

Subcommands:

- `all` — load → build entities → name → uri → metadata → report
- `run-name` / `run-uri` / `run-metadata` — each invocation reloads Parquet and
  rebuilds entities in memory, then runs only that dimension and writes
  reports (no cross-process checkpoint reuse)

Progress modes: `auto` | `tty` | `json` | `off`.

## Data model and load

### Parquet contract

Follow `REWRITE_DESIGN.md`:

- Required columns: `chain`, `contract_address`, `token_id`, `name_norm`,
  `token_uri_norm`, `image_uri_norm`, `metadata_json`
- Cast uniformly to UTF-8 strings; missing columns or incompatible casts fail
  fast with the offending file path
- Preserve input file order and in-file row number as stable source order
- Use normalized fields as-is; do not re-normalize

### Load path (optimal)

Use the Arrow/`parquet` crates to scan only the required columns and build
entities **during the scan**. Do **not** use DuckDB (including `:memory:`) as a
full-table staging store: that would hold a second copy of `metadata_json` and
compete with the final Rust working set.

Projection semantics still match the logical SQL in `REWRITE_DESIGN.md`
(`lower`/`trim`/`cast`/`coalesce`, plus stable `file_ordinal` /
`file_row_number`). Schema validation happens before the scan; invalid files
fail fast.

### In-memory entities

One sequential (or per-file parallel then merge) scan builds:

1. **Contracts** keyed by `(chain, contract_address)`
   - `name_norm`: first non-empty in stable source order
   - `nft_count`
   - valid metadata map `token_id → metadata_json` (first valid record wins)
2. **URI postings** for non-empty `token_uri_norm` / `image_uri_norm`
3. **Metadata anchors**: first `k` valid metadata records per contract ordered
   by ascending token id (EVM: arbitrary-precision non-negative integer;
   Solana: lexicographic Base58)
4. Denominators: `total_contracts` / `total_nfts` per chain over all valid
   non-empty contracts and NFTs (not the analyzable subset)

Empty names, empty URIs, and unusable metadata do not participate in their
dimension.

## Scope and counting

Three scopes for every dimension:

| Scope | Meaning |
|---|---|
| `intra_chain` | duplicates inside the primary chain |
| `cross_chain_summary` | primary-chain object matches any other chain |
| `chain_matrix` | primary-chain object matches a specified secondary chain |

Rules:

- `chain_matrix` is directional; denominator and counted objects belong to
  `primary_chain`
- Within one scope, an object matched against several peers contributes once to
  `duplicate_contract_count` / `duplicate_nft_count`
- Name and Metadata are contract-level hits: a matched contract counts the
  contract once and **all** of its NFTs
- URI counts matched NFT rows; the contract is counted once per scope when any
  of its NFTs match

## Engines

### Name (architecture §8, in-memory only)

Follow `REWRITE_ARCHITECTURE.md` §8.1–8.4 and `REWRITE_ACCEPTANCE.md` Name
section. Execution uses **resident postings only** (no `external_postings`).

1. Aggregate contracts into `NameAtom` per `(chain_id, name_norm)` and
   `CanonicalName` per distinct name value
2. Byte-identical canonical names update scope statistics from atom counts
   without materializing contract pairs (similarity `1.0`)
3. Fuzzy candidates use lossless `CandidateBounds` derived from the JW
   definition and `--name-threshold` (default `95.0` → `0.95`):
   - pairable length interval
   - minimum character-multiset overlap given lengths and common prefix
4. Candidate generation:
   - order canonical Names by character count; apply safe length interval
   - encode character multiplicity as `(character, occurrence_rank)`
   - build occurrence-token postings in memory
   - probe the safe rare-token prefix, de-duplicate, check multiset overlap
   - only survivors run exact Jaro-Winkler
5. Verifier: version-pinned RapidFuzz Jaro-Winkler over Unicode scalar values,
   with score cutoff; each unordered pair scored once (`left_id < right_id`)
6. No Union-Find, no similarity graph, no transitive closure
7. Accepted pairs expand to `NameAtom` members and accumulate the three scopes

The candidate filter must cover every real hit of exhaustive JW at the same
threshold; tokens and candidates are never silently dropped for memory reasons
(if work cannot complete, fail explicitly).

### URI

1. Exact equality on `token_uri_norm` and `image_uri_norm`
2. Metric `token_uri` when token URIs match; otherwise `image_uri` when image
   URIs match
3. Intra-chain: normalized URI appears on ≥ 2 distinct contracts on the primary
   chain
4. Cross summary / matrix: primary-chain URI appears on the target other chain(s)
5. Multiple NFTs in one contract sharing a URI: count matched NFT rows; count
   the contract once

### Metadata (architecture §11–14, in-memory only)

Follow `REWRITE_DESIGN.md` Metadata section and `REWRITE_ARCHITECTURE.md`
§11–14. Pre-filter and verification stay fully resident in memory.

1. **Valid metadata**: non-empty; trimmed content starts with `{` or `[`;
   parses as JSON; size ≤ 64 KiB
2. **Anchors**: first `k` valid records by ascending token id (`k` default 8)
3. **Compact template fingerprint** from anchors:
   - keep structural `(path, node_type)` and discriminative collection-level
     stable values (name/symbol/description, creator, royalty, license, URL
     bases)
   - drop per-token variable content
   - defaults: `min_anchor_documents=2`, `stable_value_min_anchors=2`,
     `stable_value_support_ratio=0.80`
   - emit sorted feature tokens, fingerprint bytes, `template_digest`, and a
     sparse MinHash feature vector
4. **Low-information guard**: structure-only or placeholder-only templates do
   not participate in any candidate path (neither exact bucket nor LSH probe /
   neighbor); flag counts in `run_manifest.json`
5. **Pre-filter** (template space only; never final match):
   - exact `template_digest` buckets as strong candidates, subject to
     per-contract outgoing quota and bucket-size cap (huge buckets do not
     enumerate all pairs; sample by reducer order)
   - lightweight MinHash/LSH over template features for near-identical
     templates; `(b, r)` from `template_jaccard_threshold` + target recall
   - candidate reducer orders by: exact digest match → shared discriminative
     feature count → LSH band matches → target `ContractId`; apply
     `max_candidates_per_target_chain` and
     `max_outgoing_candidates_per_contract`; union both ends and globally
     pair-dedup
6. **Shared-token BM25 verify** (content space):
   - largest shared anchor token id; else max-token fallback; Solana pairs
     always use each side's lexicographically largest anchor
   - canonicalize JSON then accept on byte-identity or BM25 cosine ≥
     `--metadata-threshold` (default `0.6`; fixed `k1=1.2`, `b=0.75`)
   - one matched token is enough; count both contracts and all their NFTs
7. Record probe counts, quota truncations, bucket-cap truncations, and
   low-information counts in `run_manifest.json`

Full stratified recall audit against an exhaustive oracle remains a non-goal
for v1; the pre-filter is still lossy and must not be described as exact.

## Progress and ETA

`cli` owns a progress reporter used by all stages.

Display fields:

- stage / phase
- `completed` / `total` (when known)
- percent
- throughput (items/s)
- elapsed
- ETA

ETA uses EWMA over positive throughput samples for the current phase. When
`total` is unknown, ETA is shown as unknown/null. After enough positive samples,
mark the estimate confident.

Suggested phase totals:

- `load` / `build_entities`: rows or contracts processed
- `name`: left Names / posting touches / scored candidates (advance by work,
  not by hit count)
- `uri`: URI groups scanned
- `metadata`: LSH probes + candidate pairs verified
- `report`: output files written

`--progress json` emits JSON Lines; tty is the interactive default under
`auto`.

## Outputs

Written under `--output-dir`:

| File | Content |
|---|---|
| `summary.csv` | per primary chain × dimension × (`intra_chain` / `cross_chain_summary`) counts and denominators |
| `chain_matrix.csv` | primary × secondary × dimension directional counts |
| `run_manifest.json` | inputs, CLI args, timings, entity scale, Name candidate stats, metadata `t_tmpl`/`(b,r)`, probe/quota/bucket truncations, low-information counts |

No `recall_audit.json`, hardware profile, or stage metrics tree beyond what is
useful inside the manifest.

## Parallelism and errors

- Entity build may parallelize per input file then merge deterministically by
  file ordinal
- Name candidate scoring and metadata pair verification use `rayon`
- Progress counters are atomic and drive ETA
- Missing/invalid Parquet schema → hard fail
- Memory exhaustion surfaces as allocation failure; no budget downgrade path
  that silently drops candidates
- First `SIGINT` requests cooperative stop, prints the latest progress, exits
  non-zero

## Testing posture

Keep tests small and local to `core`:

- Name: `CandidateBounds` covers exhaustive JW hits at threshold 0.95; byte-
  identical path; no transitive closure
- URI exact grouping and token-vs-image precedence
- Metadata: low-information exclusion from digest and LSH; quota / bucket-cap
  truncation behavior; shared-token / Solana fallback judgment; BM25 threshold
- Scope counting: one object counted once per scope

CLI smoke test: tiny multi-file Parquet fixture → `all` → CSV row smoke checks.

## Relationship to architecture / `dedup/`

| Concern | Architecture / `dedup/` | `dedup2/` |
|---|---|---|
| Purpose | production path | experimental in-memory runner |
| Memory | staged / hybrid / external | full in-memory |
| Name strategy | CandidateBounds + postings + JW | **same strategy**, resident postings only |
| Metadata prefilter | digest + MinHash/LSH + quotas | **same strategy**, resident candidates only |
| Metadata verify | shared-token BM25 | same |
| Recall audit | required quality gate | stats in manifest; oracle audit out of v1 |
| Config | TOML file | CLI flags only |
| Progress | tty/json + EWMA ETA | same idea, simpler reporter |
