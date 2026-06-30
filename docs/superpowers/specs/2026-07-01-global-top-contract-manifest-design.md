# Global Four-Chain Top Contract Manifest Design

## Goal

Replace the current union of four independent per-chain rankings with one
global OpenSea Top collection ranking over Ethereum, Base, Polygon, and Solana.
Export the selected contracts as `(chain, address)` pairs and make
`name_uri_analysis_rs` calculate all denominators, intra-chain results, and
cross-chain matrices only from that shared ranked population.

## Terminology

- **Global Top N collections**: the first `N` analyzable collection records in
  one OpenSea response sequence ordered by 30-day volume across all four
  selected chains.
- **Contract pair**: `(chain, address)`. This is the only contract identity
  used for deduplication, filtering, or joins.
- **Analyzable collection**: a ranked collection containing at least one valid
  contract pair on the selected chains.
- **Expanded contract manifest**: all valid contract pairs belonging to the
  selected Global Top N collections. Its row count may be greater than `N`
  because one collection may contain multiple contracts.

“Global” does not mean the same logical collection must be deployed on every
chain, and it does not impose an equal quota per chain.

## Current Problem

`fetch_opensea_top_seeds.py` currently performs one request sequence per chain
and applies `--limit` independently. With the default four chains and
`--limit 100`, it therefore creates up to four unrelated Top-100 populations.
Combining those files does not produce a global Top-100 ranking.

The current text files also contain addresses without an attached chain.
Although EVM and Solana address formats differ in most cases, address-only
identity is insufficient for a cross-chain experiment and cannot safely drive
a relational filter.

`name_uri_analysis_rs` currently analyzes every row in the supplied Parquet
files. It has no contract-manifest input, so downloading a Top list does not
change its analysis population.

## Chosen Architecture

```text
OpenSea /collections/top
  one request sequence, four repeated chains parameters
  30-day-volume ordering, one cursor, global collection limit
                 |
                 v
       ranked collection records
          |                 |
          v                 v
 top_contracts.csv    top_collections.json
 exact pair input     ranking audit record
          |
          v
 name_uri_analysis_rs --contracts top_contracts.csv
          |
          v
 selected_contracts temp table
          |
          v
 four Parquet inputs joined by (chain, canonical address)
          |
          v
 summary.json + summary.csv for the shared Top population
```

The PostgreSQL snapshot exporter remains unchanged. It can continue exporting
complete per-chain Parquet snapshots, optionally with the already implemented
EVM block bounds. Filtering belongs in the analysis projection so one snapshot
can be reused with different ranked populations.

## OpenSea Ranking Contract

The fetcher changes its default endpoint from
`/api/v2/collections/trending` to `/api/v2/collections/top`.

The request has these semantics:

- one request sequence for all selected chains;
- repeated `chains` query parameters encoded with `urlencode(..., doseq=True)`;
- 30-day volume ordering;
- one pagination cursor shared by the global ranking;
- `--limit` counts analyzable ranked collections globally.

The exact accepted query parameter name for the 30-day sort and the
multi-chain array encoding must be verified against a live OpenSea response
during implementation. The official public documentation distinguishes Top
collections from Trending collections but does not currently expose a complete
parameter schema for the Top endpoint.

The script must fail explicitly if OpenSea rejects the multi-chain Top request
or if the response does not contain enough information to establish one server
ordering. It must not silently fall back to four per-chain rankings because
that would change the experimental population.

Pagination stops when:

1. `N` analyzable ranked collections have been accepted;
2. the server returns no next cursor; or
3. a repeated cursor is detected, which is an error.

If pagination ends before `N` analyzable collections are found, the script
fails without replacing an existing complete manifest.

## Collection and Contract Extraction

Each collection keeps its server response order as `global_rank`.

For every accepted collection:

1. inspect every supported contract container in the response;
2. read both chain and address from each contract record;
3. discard contracts outside the selected chain set;
4. canonicalize the chain name to lowercase;
5. canonicalize Ethereum, Base, and Polygon addresses to lowercase;
6. validate and preserve Solana Base58 address case;
7. deduplicate by `(chain, canonical_address)`.

Deduplication does not remove a pair from an earlier-ranked collection if the
same pair appears again later. The pair remains associated with the earliest
global rank in the audit output.

A collection with no valid selected-chain pair does not consume one of the
`N` ranked collection slots.

## Output Contracts

### Analysis manifest

Default path: `top_contracts.csv`.

The file contains exactly two columns:

```csv
chain,address
ethereum,0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d
base,0x1234567890abcdef1234567890abcdef12345678
solana,So11111111111111111111111111111111111111112
```

Properties:

- UTF-8;
- one header row;
- deterministic order by earliest `global_rank`, then collection contract
  order;
- no duplicate pair;
- written atomically through a temporary sibling file and rename;
- no address-only compatibility file in multi-chain mode.

### Ranking audit

Default path: `top_collections.json`.

Each ranked collection object contains:

- `global_rank`;
- OpenSea collection slug;
- collection display name when available;
- ranking/sort value when returned by OpenSea;
- requested ranking criterion (`thirty_days_volume`);
- extracted contract pairs;
- raw chain labels only when they differ from canonical labels.

The audit file is not consumed by the analyzer. It exists to reproduce and
review how the pair manifest was derived.

Both output files are produced from one in-memory result and replaced only
after both serializations succeed.

## Analyzer Interface

`name_uri_analysis_rs` gains a required-for-ranked-analysis option:

```text
--contracts ./top_contracts.csv
```

The existing no-manifest behavior remains available for full-dataset analysis.
When the option is present:

1. parse the CSV before creating analysis tables;
2. reject missing headers, extra columns, empty values, unsupported chains, or
   invalid addresses;
3. canonicalize pairs with the same chain-specific rules as the exporter;
4. reject duplicate pairs in the input rather than silently counting them;
5. load pairs into a DuckDB temporary table with a unique
   `(chain, contract_address)` key;
6. build `analysis_rows` by joining each Parquet row to that table on canonical
   chain and canonical contract address.

The manifest filter is applied before:

- selected-chain discovery;
- chain totals;
- URI grouping;
- name atoms;
- metadata representatives;
- chain-matrix analysis.

Consequently, all reported denominators and ratios refer only to NFT rows and
contracts selected by the common Top manifest.

## Missing-Pair Accounting

After preparing `analysis_rows`, the analyzer compares the selected manifest
with the distinct pairs found in the Parquet inputs.

It writes `manifest_coverage.json` containing:

- requested pair count;
- matched pair count;
- missing pair count;
- missing pairs grouped by chain;
- matched NFT row count by chain.

Default behavior is to complete the analysis when some pairs are missing but
print a warning and persist the coverage report. A new
`--require-all-contracts` flag turns any missing pair into an error.

If no manifest pair matches any Parquet row, analysis fails regardless of the
flag. If fewer than two chains remain after filtering, intra-chain analysis
continues and cross-chain rows are omitted using the existing behavior.

## Error Handling

- Reject a response that lacks a collection list.
- Reject a repeated OpenSea pagination cursor.
- Reject a multi-chain request that the endpoint does not support.
- Reject malformed EVM and Solana addresses.
- Reject manifest duplicate pairs and schema drift.
- Never deduplicate by address alone.
- Never infer a missing chain from address format.
- Never reuse partially written outputs after a failed fetch.
- Make incomplete Parquet coverage visible through
  `manifest_coverage.json`.

## Testing

### Fetcher unit tests

- one URL contains all four repeated `chains` values;
- the endpoint is `/collections/top`, not `/collections/trending`;
- 30-day-volume ordering is requested;
- one cursor drives pagination globally;
- `--limit` counts collections rather than expanded pairs;
- a multi-contract collection expands into multiple rows;
- duplicate pairs retain the earliest rank;
- the same address string on two chains remains two pairs;
- EVM case folds and Solana case does not;
- short result sets and repeated cursors fail;
- CSV contains exactly `chain,address`;
- CSV and JSON writes are atomic.

### Analyzer unit and integration tests

- valid pair manifests parse and canonicalize correctly;
- malformed headers, duplicate pairs, and invalid addresses fail;
- an unselected contract present in Parquet does not enter
  `analysis_rows`;
- a selected Solana address matches case-sensitively;
- EVM case variants match the canonical manifest pair;
- totals and duplicate ratios use only selected rows;
- four-chain fixture produces chain-matrix rows from the selected population;
- missing pairs appear in `manifest_coverage.json`;
- `--require-all-contracts` rejects incomplete coverage;
- zero matched pairs always fails.

## Non-Goals

- Equal per-chain quotas.
- Client-side USD conversion of four independent per-chain rankings.
- Matching logical collections across chains by slug, name, owner, or branding.
- Solana transfer, sale, ownership, gas, or lifecycle analysis.
- Changing PostgreSQL schemas or Solana ingestion.
- Reintroducing `name_metadata_change_samples` into this analysis flow.

## Success Criteria

Given four complete snapshot Parquet files and a requested Global Top N:

1. the fetcher performs one globally ordered multi-chain request sequence;
2. `top_contracts.csv` contains unique `(chain, address)` pairs derived from
   exactly the first `N` analyzable ranked collections;
3. the analyzer includes only those pairs;
4. all totals and cross-chain results are computed from that selected
   population;
5. missing snapshot coverage is explicitly measurable.
