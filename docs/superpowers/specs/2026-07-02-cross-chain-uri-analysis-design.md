# Cross-Chain URI Analysis Design

## Goal

Extend `name_uri_analysis_rs` so all four content dimensions participate in
cross-chain analysis:

- `token_uri`;
- `image_uri`;
- `name`;
- `metadata`.

The existing name and metadata implementations remain unchanged. URI analysis
gains the same two cross-chain output scopes they already provide:

- `cross_chain_summary`: for each primary chain, count rows that match at least
  one different selected chain;
- `chain_matrix`: for each directed primary/secondary chain pair, count rows
  from the primary chain that match the secondary chain.

## Compatibility Contract

URI output keeps the existing schema and metric meanings:

- `field_name = uri`;
- `match_mode = norm_cross`;
- `v1`: normalized `token_uri` matches;
- `v2`: normalized `token_uri` does not match in the selected scope, but
  normalized `image_uri` matches;
- `v3`: either normalized URI matches.

`token_uri` and `image_uri` therefore remain distinguishable through `v1` and
`v2`, while `v3` remains their union. No new field names or report columns are
introduced, so existing CSV/JSON consumers continue to work.

The scope is evaluated independently:

- for `cross_chain_summary`, a URI matches when it exists on any other selected
  chain;
- for `chain_matrix`, a URI matches only when it exists on that row's
  `secondary_chain`.

Consequently, one primary-chain NFT may be `v1` against one secondary chain and
`v2` against another. The global cross-chain summary is computed directly and
is not the sum of matrix rows, avoiding double counting when a URI appears on
multiple chains.

## Data Preparation

The existing `analysis_rows` and `uri_key_contracts` tables remain the source
of truth. URI matching continues to use the precomputed normalized values
`token_uri_norm` and `image_uri_norm`.

Preparation adds three compact structures:

1. `uri_cross_chain_keys`
   - one row per `(key_kind, key_value)` present on at least two distinct
     chains;
   - used to add cross-chain-any flags to `uri_contract_flags`;
   - supports `cross_chain_summary` without multiplying rows by chain count.

2. `uri_key_chain_presence`
   - contains only keys already proven to occur on at least two chains;
   - avoids retaining the much larger set of single-chain-only URI keys.

3. `uri_chain_pair_contract_flags`
   - one row per directed
     `(primary_chain, secondary_chain, contract_address)` that has at least one
     URI match;
   - created by sparse inner joins from primary token/image keys to URI-key
     presence on matching secondary chains;
   - stores the same `v1/v2/v3` NFT and contract flag columns as the existing
     intra-chain table.

The matrix path uses key-presence joins rather than a full NFT-to-NFT
self-join or a dense source-row-to-chain cross join. Token and image hits are
merged by source row before computing V1/V2/V3, preserving V2 exclusion while
materializing only actual cross-chain hits. All directed pair totals are then
loaded with one grouped query; missing pairs are emitted as zero-valued rows in
Rust.

## Aggregation and Output

`run_uri_analysis` emits:

1. existing `intra_chain` rows from the `norm_contract` columns;
2. one `cross_chain_summary` row set per primary chain from cross-chain-any
   columns;
3. one `chain_matrix` row set per ordered pair of distinct selected chains.

Each row set contains `v1`, `v2`, and `v3`.
The per-chain intra-chain and cross-chain-any contract totals are loaded
together with one `GROUP BY chain` query rather than rescanning
`uri_contract_flags` once per chain and scope.

For four selected chains:

- four cross-chain summary row sets are emitted;
- twelve directed matrix row sets are emitted;
- each row set contains three URI metric rows.

All ratios keep the existing denominator contract: the primary chain's full
contract and NFT totals from `analysis_rows`. Zero-match rows are retained so
the matrix remains complete and directly comparable with name and metadata.

When only one chain is selected, no URI cross-chain summary or matrix rows are
emitted.

## Correctness Boundaries

- A URI must be non-empty after existing normalization.
- Intra-chain matching still requires reuse across contracts on the same
  chain.
- Cross-chain matching requires URI presence on a different chain; the address
  text is irrelevant because contract identity is `(chain, address)`.
- `v2` remains exclusive of `v1` within the same scope.
- The pair matrix must not be contaminated by a third chain.
- Solana address case preservation remains unchanged and does not affect URI
  key comparison.

## Testing

Tests will cover:

- a cross-chain `token_uri` match producing `v1` and `v3`;
- an image-only cross-chain match producing `v2` and `v3`;
- `cross_chain_summary` for each participating primary chain;
- directed pairwise `chain_matrix` rows;
- absence of third-chain contamination in a pair;
- zero rows for non-matching chain pairs;
- single-chain runs continuing to omit every cross-chain scope;
- SQL preparation exposing both cross-chain-any and pairwise flag columns;
- unchanged name and metadata cross-chain behavior.

The existing test that requires all URI rows to be `intra_chain` will be
replaced by positive cross-chain URI assertions.

## Documentation

`name_uri_analysis_rs/README.md` will describe all four dimensions as
cross-chain capable and define the URI `v1/v2/v3` scope semantics. The
four-Parquet command remains unchanged.

## Non-Goals

- No Top-contract filtering or `top_contracts.csv` integration.
- No change to URI normalization.
- No independent overlapping image metric in addition to `v2`.
- No changes to name Jaro-Winkler scoring or metadata BM25 scoring.
- No symbol analysis.
