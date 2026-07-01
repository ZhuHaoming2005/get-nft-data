# Global Four-Chain Top Contract Manifest Design

## Goal

Replace the current union of four independent per-chain rankings with one
global OpenSea Top collection ranking over Ethereum, Base, Polygon, and Solana.
Export the selected contracts as `(chain, address)` pairs. Do not connect this
manifest to an analyzer in this change.

## Terminology

- **Global Top N collections**: the first `N` analyzable collection records in
  one OpenSea response sequence ordered by 30-day volume across all four
  selected chains.
- **Contract pair**: `(chain, address)`. This is the only contract identity
  used for manifest deduplication and export.
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

`name_uri_analysis_rs` intentionally analyzes every row in the supplied
block/slot-bounded Parquet snapshots. The common Top manifest must not change
that full-snapshot experiment population.

## Chosen Architecture

```text
OpenSea /collections/top
  one request sequence, one comma-separated four-chain parameter
  30-day-volume ordering, one cursor, global collection limit
                 |
                 v
       ranked collection records
          |                 |
          v                 v
 top_contracts.csv    top_collections.json
 exact pair output    ranking audit record
```

The PostgreSQL snapshot exporter and all Rust analyzers remain unchanged.
`name_uri_analysis_rs` continues to run large-scale full-data deduplication over
all rows in the supplied snapshots. A future Top-contract-specific integration
must use a separate entry point and define comparable Solana/EVM metrics before
consuming this manifest.

## OpenSea Ranking Contract

The fetcher changes its default endpoint from
`/api/v2/collections/trending` to `/api/v2/collections/top`.

The request has these semantics:

- one request sequence for all selected chains;
- one comma-separated `chains=ethereum,base,polygon,solana` query parameter;
- `sort_by=thirty_days_volume`;
- one pagination cursor shared by the global ranking;
- `--limit` counts analyzable ranked collections globally.

OpenSea's current official OpenAPI description defines `sort_by` and describes
`chains` as a comma-separated list. The implementation tests lock that exact
encoding. A live run is still required to verify that the authenticated
response contains the documented collection list, contracts, and pagination
cursor before replacing local outputs. The ranking value is recorded when the
response includes it.

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

### Contract pair manifest

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

## Analysis Boundary

This change stops after writing `top_contracts.csv` and
`top_collections.json`. It does not add a `--contracts` option, filtered
DuckDB projection, or coverage report to `name_uri_analysis_rs`.

The two populations remain explicit:

- `name_uri_analysis_rs`: every NFT row in the selected exported snapshot
  range;
- common Top manifest: a ranked `(chain,address)` artifact reserved for a
  future Top-contract-specific analysis flow.

## Error Handling

- Reject a response that lacks a collection list.
- Reject a repeated OpenSea pagination cursor.
- Reject a multi-chain request that the endpoint does not support.
- Reject malformed EVM and Solana addresses.
- Never deduplicate by address alone.
- Never infer a missing chain from address format.
- Never reuse partially written outputs after a failed fetch.

## Testing

### Fetcher unit tests

- one URL contains one comma-separated `chains` value with all four chains;
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

## Non-Goals

- Equal per-chain quotas.
- Client-side USD conversion of four independent per-chain rankings.
- Matching logical collections across chains by slug, name, owner, or branding.
- Solana transfer, sale, ownership, gas, or lifecycle analysis.
- Changing PostgreSQL schemas or Solana ingestion.
- Reintroducing `name_metadata_change_samples` into this analysis flow.
- Connecting `top_contracts.csv` to `name_uri_analysis_rs` or changing its
  full-snapshot denominators.
- Defining or implementing the future Top-contract-specific cross-chain
  analyzer.

## Success Criteria

Given a requested Global Top N:

1. the fetcher performs one globally ordered multi-chain request sequence;
2. `top_contracts.csv` contains unique `(chain, address)` pairs derived from
   exactly the first `N` analyzable ranked collections;
3. `top_collections.json` preserves enough ranking and collection-to-pair
   information to audit the CSV;
4. `name_uri_analysis_rs` remains a full-snapshot analyzer and does not read
   either output.
