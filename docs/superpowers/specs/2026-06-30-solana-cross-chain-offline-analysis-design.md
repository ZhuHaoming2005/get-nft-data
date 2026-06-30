# Solana and Cross-Chain Offline Analysis Design

## Goal

Make the existing PostgreSQL-to-Parquet analysis path handle Ethereum, Base,
Polygon, and Solana without corrupting Solana identifiers. The delivered
vertical slice must:

1. export a Solana snapshot from the current `nft_assets_solana` table;
2. analyze that snapshot together with EVM snapshots in
   `name_uri_analysis_rs`;
3. fetch OpenSea top/trending seed contracts for all four chains and optionally
   run the local name/metadata sample analysis for each chain; and
4. allow EVM snapshot exports to select an inclusive stored block range.

The change does not make the live behavioral analysis in
`top_contract_analysis_rs analyze` or `batch` support Solana.

## Current Data Contract

The EVM and Solana main tables expose the same logical columns:

- `contract_address`
- `token_id`
- `token_uri`
- `image_uri`
- `name`
- `symbol`
- `metadata`
- `token_standard`
- `first_seen_block`

Their identifier semantics differ:

- Ethereum, Base, and Polygon contract addresses are case-insensitive and use
  lowercase canonical form.
- Solana collection and mint addresses are case-sensitive Base58 strings.
- EVM `token_id` values are numeric in PostgreSQL but are exported as strings.
- Solana `token_id` stores the mint address and is already a string.
- Current Solana discovery writes `first_seen_block = 0`; it does not have a
  reliable per-row discovery slot suitable for range filtering.

## Chosen Approach

Use the existing offline tools and make their address identity rules
chain-aware. Keep the full EVM behavioral analyzer unchanged, because its
Alchemy/Etherscan transfer, receipt, sale, gas, and attacker-cost stages do not
have Solana-equivalent semantics.

The Python top-seed script will compose the existing local sample analyzer
rather than introduce a new multi-chain behavioral engine.

## Components

### 1. Snapshot export in `top_contract_analysis_rs`

`export-snapshot` remains a one-chain command and retains its current Parquet
schema.

It gains optional arguments:

- `--start-block <N>`
- `--end-block <N>`

For Ethereum, Base, and Polygon, supplied bounds filter
`first_seen_block` inclusively. Either bound may be supplied independently.
Bounds must be non-negative, and `start <= end` when both are present.

For Solana, any supplied block bound is rejected with a message explaining
that current Solana rows use the placeholder value `0`. An unbounded Solana
export is allowed.

Exported contract addresses are canonicalized by chain:

- EVM: trim and lowercase;
- Solana: trim and preserve case.

No numeric conversion is applied to `token_id`; PostgreSQL casts either the
EVM numeric token ID or Solana mint string to text.

### 2. Dataset-wide analysis in `name_uri_analysis_rs`

The analyzer continues to accept repeated `--parquet` arguments and derives the
selected chain set from their `chain` columns.

The temporary `analysis_rows` projection canonicalizes contract identity by
chain:

- `solana`: preserve the trimmed address;
- all other supported chains: lowercase the trimmed address.

All downstream name, URI, and metadata grouping uses that projected identity.
This prevents two distinct Solana addresses that differ by letter case from
being merged while preserving current EVM behavior.

The intended four-chain invocation is one `name_uri_analysis_rs` run over four
Parquet files. Existing single-chain invocations remain valid.

### 3. Seed-based local analysis in `name_metadata_change_samples`

The CLI accepts exactly one feature source:

- existing `--feature-db <PATH>`; or
- new `--feature-parquet <PATH>`.

For Parquet input, the process opens an in-memory DuckDB connection and exposes
the Parquet rows as `nft_features`. This avoids requiring a persistent feature
database solely to analyze newly exported data.

Seed parsing, SQL grouping, candidate exclusion, and candidate loading use the
same chain-aware contract identity rule as the snapshot exporter. Solana seed
addresses retain case; EVM seed addresses are lowercased.

The analyzer still processes one chain and one seed file per invocation. The
Python orchestration layer runs one invocation per chain, which keeps reports
and failure boundaries explicit.

### 4. Four-chain OpenSea seed orchestration

`name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py` supports:

- `--chains ethereum base polygon solana`, with these four chains as the
  multi-chain default;
- legacy `--chain <CHAIN>` for a single-chain run;
- `--output-dir <DIR>` for multi-chain seed files named
  `<chain>.seeds.txt`;
- legacy `--output <FILE>` for a single-chain run;
- `--analyze` to run the local sample analyzer after fetching;
- `--parquet-dir <DIR>`, required with `--analyze`, containing
  `<chain>.parquet`;
- `--analysis-output-dir <DIR>` for reports named `<chain>.md`.

The script performs one OpenSea request sequence per chain, so pagination,
deduplication, and per-chain limits do not leak across chains.

Address validation is chain-aware:

- EVM chains require `0x` followed by 40 hexadecimal digits and are
  lowercased.
- Solana requires a valid Base58 value that decodes to 32 bytes and retains
  its original case.

With `--analyze`, the script invokes `name_metadata_change_samples` once per
successfully fetched chain using that chain's seed and Parquet files. A
subprocess failure stops the script with a non-zero exit status and identifies
the failed chain. Fetch-only operation remains available.

## Data Flow

```text
nft_assets_{chain} in PostgreSQL
        |
        | top_contract_analysis_rs export-snapshot
        v
<chain>.parquet
        |
        +--> four files --> name_uri_analysis_rs --> cross-chain summary
        |
OpenSea trending collections
        |
        | fetch_opensea_top_seeds.py
        v
<chain>.seeds.txt
        |
        | optional per-chain orchestration
        v
name_metadata_change_samples + <chain>.parquet --> <chain>.md
```

## Error Handling

- Reject empty or unsafe chain names before forming PostgreSQL table names.
- Reject negative block bounds and reversed ranges before connecting to
  PostgreSQL.
- Reject Solana block bounds before exporting any rows.
- Report malformed OpenSea addresses by omitting them; fail a chain if no
  valid addresses remain.
- Keep output files separated by chain so partial fetch results cannot be
  misidentified as another chain.
- Do not silently continue after a requested local analyzer subprocess fails.

## Testing

### Rust exporter

- CLI parsing tests for optional block bounds.
- Validation tests for negative, reversed, and Solana block ranges.
- Query-construction tests for unbounded, lower-only, upper-only, and bounded
  inclusive filters.
- Parquet tests showing EVM addresses are lowercased and Solana addresses keep
  their Base58 case.

### `name_uri_analysis_rs`

- A mixed-chain Parquet fixture with Ethereum, Base, Polygon, and Solana rows.
- Two case-distinct Solana contract addresses remain two contracts.
- Equivalent case-distinct EVM addresses collapse to one canonical contract.
- The four-chain run produces chain and chain-matrix summary rows.

### `name_metadata_change_samples`

- Direct Parquet input is accepted without a persistent DuckDB file.
- Solana seed lookup preserves case.
- EVM seed lookup remains case-insensitive.
- Case-distinct Solana candidates are not merged or excluded as the same
  contract.

### Python script

- Four-chain default and legacy single-chain argument behavior.
- EVM and Solana address validation and canonicalization.
- Separate pagination/deduplication state for every chain.
- Deterministic seed and report paths.
- Analyzer command construction and failed-subprocess propagation.

### End-to-end fixture

Generate small Parquet fixtures for all four chains, run the dataset-wide
analyzer over all files, and run the seed-based analyzer against a Solana
Parquet fixture. No live OpenSea or PostgreSQL service is required for this
test.

## Non-Goals

- Solana transfer, sale, ownership, receipt, gas, attacker-cost, or lifecycle
  analysis.
- Converting timestamps to EVM blocks or Solana slots.
- Changing the Solana scanner to populate a real `first_seen_block`.
- Combining all chain seed reports into one behavioral report.
- Refactoring unrelated EVM analysis or report-generation code.
