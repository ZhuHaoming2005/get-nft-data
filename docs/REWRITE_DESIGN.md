# name_uri_analysis_rs deduplication strategy

This document defines only the deduplication tasks and business strategy of the rewritten
`name_uri_analysis_rs`. It does not prescribe stage decomposition, code structure, or execution
implementation.

## Target environment and data scale

Target device:

- 128 vCPU;
- 512 GiB RAM;
- Linux.

The production data scale uses the primary-chain totals from the current `summary.csv` as the
baseline:

| Chain | Contracts | NFTs |
|---|---:|---:|
| Base | 419,071 | 71,956,906 |
| Ethereum | 85,143 | 8,386,527 |
| Polygon | 298,963 | 39,679,029 |
| Solana | 28,499,776 | 37,590,683 |
| Total | 29,302,953 | 157,613,145 |

These figures fix the real data volume the rewrite must support. Development must use the best
achievable time and space complexity.

## Parquet source specification

### Source

Parquet files must be exported from full snapshots of each chain's NFT database, not stitched
together from ad-hoc queries taken at different points in time. Each row represents one NFT, whose
logical primary key is:

```text
(chain, contract_address, token_id)
```

### Fields

| Field | Requirement | Purpose |
|---|---|---|
| `chain` | Non-empty UTF-8 string, stable lowercase chain name | Determine owning chain |
| `contract_address` | Non-empty UTF-8 string | Determine contract or collection |
| `token_id` | Non-empty UTF-8 string, one representation per chain; EVM token ids are ordered as arbitrary-precision non-negative integers, Solana account addresses lexicographically | Align the same NFT |
| `name_norm` | Name already normalized by the exporter, may be empty | Name dedup |
| `token_uri_norm` | Token URI already normalized by the exporter, may be empty | `token_uri` dedup |
| `image_uri_norm` | Image URI already normalized by the exporter, may be empty | `image_uri` dedup |
| `metadata_json` | Full metadata JSON string, may be empty | Metadata content comparison |

Name and URI normalization are completed before Parquet export. Use `name_norm`, `token_uri_norm`
and `image_uri_norm` directly; do not re-read raw values to recompute them, and do not run a second
transformation over the normalized results.

EVM addresses are lowercase; Solana addresses keep their Base58 case.

### Reading from Parquet

Multiple Parquet files are read as a single logical table. Before reading, verify that every file
contains the required fields and that same-named fields can be uniformly cast to UTF-8 strings.
Read only the columns needed for deduplication, and keep the input file order and in-file row number
as the stable source order.

The logical read is:

```sql
SELECT
    lower(trim(CAST(chain AS VARCHAR))) AS chain,
    trim(CAST(contract_address AS VARCHAR)) AS contract_address,
    trim(CAST(token_id AS VARCHAR)) AS token_id,
    coalesce(CAST(name_norm AS VARCHAR), '') AS name_norm,
    coalesce(CAST(token_uri_norm AS VARCHAR), '') AS token_uri_norm,
    coalesce(CAST(image_uri_norm AS VARCHAR), '') AS image_uri_norm,
    coalesce(nullif(trim(CAST(metadata_json AS VARCHAR)), ''), '') AS metadata_json,
    filename,
    file_row_number
FROM read_parquet(
    ['base.parquet', 'ethereum.parquet', 'polygon.parquet', 'solana.parquet'],
    filename = true,
    file_row_number = true
);
```

## Common statistics conventions

- EVM contract addresses are lowercased; Solana addresses keep their Base58 case.
- Empty names, empty URIs and unusable metadata do not participate in the corresponding field's
  deduplication.
- `total_contracts` and `total_nfts` use all valid non-empty contracts and NFTs of the primary chain
  as the denominator, not the analyzable subset.
- Three scopes are produced:
  - `intra_chain`: duplicates inside the primary chain;
  - `cross_chain_summary`: a primary-chain object duplicated against an object on any other chain;
  - `chain_matrix`: a primary-chain object duplicated against an object on a specified secondary
    chain.
- `chain_matrix` is directional. `primary_chain` determines the denominator and the counted
  contracts and NFTs; `secondary_chain` only restricts the source of matching objects.
- When one object matches several duplicate objects inside the same scope,
  `duplicate_contract_count` and `duplicate_nft_count` count it only once.

## Name deduplication

Every NFT of a contract carries the same name, so names are aggregated by contract address and are
not deduplicated within a contract.

### Already-normalized input

Name deduplication reads `name_norm` from Parquet directly. The analyzer does not touch the raw
`name` and does not re-run name normalization. Byte-identical normalized names are treated as
similarity `1.0`.

### Decision

- Uses Jaro-Winkler similarity;
- The default threshold is `95.0`, i.e. an internal similarity of `0.95`;
- Every name pair is compared independently and independently judged duplicate or not;
- No Union-Find;
- No transitive closure.

## URI deduplication

### Already-normalized input

URI deduplication reads directly from Parquet:

- `token_uri_norm`;
- `image_uri_norm`.

The analyzer does not touch raw `token_uri` or `image_uri`.

### Decision

URI uses exact equality of the normalized result. No fuzzy similarity and no transitive inference.

The metrics are:

- `token_uri`: the token URI equals that of another object inside the target scope;
- `image_uri`: the token URI did not match, but the image URI is equal.

### Scope semantics

- An intra-chain duplicate requires the same normalized URI to be associated with at least two
  distinct contracts on the primary chain;
- A cross-chain summary requires the primary-chain URI to appear on at least one other chain;
- A chain matrix requires the primary-chain URI to appear on the specified secondary chain;
- When several NFTs inside one contract share the same URI, NFTs are counted by the actually matched
  rows and the contract is counted once.

## Metadata deduplication

Metadata deduplication is performed at **contract granularity**. It does not use templates as a
matching criterion and it does not compare metadata over a full token-id intersection. Instead,
candidate contract pairs are produced by a lightweight pre-filter, and each candidate pair is judged
by comparing the metadata of a shared token id with BM25.

A metadata match is a contract-level conclusion: once a contract is judged duplicate, the contract
and **all of its NFTs** are counted (the same counting model as Name).

### Valid metadata

Only metadata satisfying all of the following participates:

- content is non-empty;
- after trimming, it starts with `{` or `[`;
- it parses as JSON;
- its size does not exceed 64 KiB.

A contract's metadata is organized by `token_id`. When a token has several source records, the first
valid record by input file order and in-file row number is chosen, so every run uses the same
content.

### Anchor selection

For each contract, select the first `k` valid metadata records ordered by ascending token id (EVM:
token id compared as an arbitrary-precision non-negative integer; Solana: account address
lexicographic order). If a contract has fewer than `k` valid records, use all of them. `k` is a
small constant (default `8`). These `k` records are the contract's **anchors** and are the only
metadata that is canonicalized; the whole metadata stage is therefore bounded by contract count, not
by NFT count. Anchors are the lowest token ids because collections overwhelmingly start near the
minimum id, which maximizes the chance that two contracts share anchor token ids.

### Pre-filter by a compact contract template

Candidate contract pairs are produced by a lightweight pre-filter built on a **compact contract
template fingerprint** aggregated from the anchors. The template is used only for candidate
generation; it is never the final matching criterion.

The template keeps:

- structural features: the set of `(path, node_type)`;
- discriminative collection-level stable values that are identical across the anchors, e.g.
  collection name / symbol / description, creator, royalty, license, and the **base** of image and
  external URLs (shared CID directory or host prefix).

The template drops per-token-variable content, e.g. token number, token id, the concrete image /
animation / external resource address of each token, and each NFT's own `attributes[].value`.

The template must carry discriminative collection-level values, not structure alone: generic ERC-721
structure (`name` / `description` / `image` / `attributes`) is nearly universal, so a structure-only
fingerprint would collapse unrelated contracts into one huge bucket.

The pre-filter then:

- groups contracts whose template digest is byte-identical as strong candidates;
- uses a lightweight MinHash/LSH over the template feature set to add near-identical templates as
  candidates;
- applies per-contract outgoing candidate quotas and bucket-size caps.

### Content guard for low-information contracts

If a contract's template carries no discriminative collection-level stable value (structure only, or
the `k` anchors are all identical placeholder content, typical of pre-reveal / default metadata), the
contract is treated as **low-information**: it does not drive cross-contract grouping and it is
flagged in the output. This prevents unrelated placeholder collections from collapsing into a single
spurious duplicate group that would inflate `duplicate_nft_count`.

### Judgment by shared-token BM25

For each candidate contract pair, alignment is by token id and only the metadata of a shared token id
is compared:

```text
contract A / token t  ↔  contract B / token t
```

- Take the largest token id shared by both contracts' anchors. The largest shared anchor token is
  preferred over the smallest so that the compared token is less likely to be the `#0` pre-reveal or
  placeholder token.
- If the two contracts have no shared anchor token id, compare each contract's largest anchor token
  (max-token fallback).
- Because Solana token ids are account addresses and can never be equal, any pair involving Solana
  directly uses each side's lexicographically largest anchor token.

Full content includes all JSON content of that token's metadata (name, description, attributes,
image, animation, external links, collection info and other auxiliary fields). Before comparison,
JSON object field order, attributes alignment, Unicode NFKC, Unicode lowercasing and whitespace are
normalized. Normalization only removes representational differences; it does not delete the token's
actual name, attribute values, URIs or other content.

The compared token matches when its canonical content is byte-identical, or otherwise when the full
content BM25 similarity reaches `0.6`. Because counting is contract-level, **one matched shared token
is sufficient**: as soon as the compared token matches, the contract pair is a metadata duplicate and
both contracts (with all their NFTs) are counted. There is no need to compare additional shared
tokens.

### Approximation and audit

The pre-filter is lossy (LSH recall and quotas), so metadata is an approximate dimension. The
recall of the pre-filter must be measured by a decoupled audit against an exhaustive oracle on
sampled contracts, and all skipped candidates, quota truncations and low-information flags must be
reported.
