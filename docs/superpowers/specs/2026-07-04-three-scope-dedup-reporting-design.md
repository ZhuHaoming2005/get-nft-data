# Three-Scope NFT Deduplication Reporting Design

## Goal

`name_uri_analysis_rs` must report duplicate NFTs independently under three
scopes for every enabled deduplication dimension:

1. pairwise chain pools;
2. one pool containing every selected chain;
3. one pool per individual chain.

The matching algorithms and thresholds for name, URI, and metadata remain
unchanged. This change only defines and verifies pool isolation, directional
counting, and result aggregation.

## Scope Semantics

### Pairwise chain pool

For each unordered pair of distinct chains `(A, B)`, deduplication uses only
NFTs from A and B. The pair is computed once, then emitted as two directional
rows:

- `A -> B`: count only duplicate NFTs and contracts belonging to A;
- `B -> A`: count only duplicate NFTs and contracts belonging to B.

Both ratios use the selected primary chain's total NFT or contract count as
their denominator. A third chain must not affect either directional result.
The existing output scope remains `chain_matrix`.

### All-chain pool

All NFTs from all selected chains participate in one deduplication pool. One
`cross_chain_summary` row is emitted per primary chain.

Only primary-chain NFTs and contracts in components containing at least one
other chain are counted. A primary-chain NFT that matches multiple other chains
is counted once, so this result is computed from the all-chain components and
is not the sum of pairwise rows. Ratios use the primary chain's total NFT or
contract count.

### Intra-chain pool

Each chain is deduplicated independently using only its own NFTs. One
`intra_chain` row is emitted per chain. Duplicate NFT and contract counts are
based only on same-chain components and cannot be affected by cross-chain
edges. Existing ratio fields remain populated with the primary chain totals as
denominators.

## Execution Model

Candidate generation and similarity scoring may be shared, but the three
result views remain independent:

- an intra-chain union-find receives only same-chain matches;
- a global sparse union-find receives cross-chain matches from every selected
  chain;
- one sparse union-find per unordered chain pair receives only matches between
  those two chains.

This avoids repeated similarity scoring while preserving exact pool semantics.
Pairwise results must never be derived from the global union-find because a
path such as `A -> C -> B` would contaminate the isolated `(A, B)` pool.

URI analysis may use equivalent grouped SQL flag tables instead of in-memory
union-find structures, provided the same isolation and directional counting
rules hold.

## Output Compatibility

The existing `SummaryRow` schema and scope names remain unchanged:

- `chain_matrix`;
- `cross_chain_summary`;
- `intra_chain`.

Existing duplicate contract fields, group statistics, thresholds, match modes,
and URI `v1`/`v2`/`v3` metrics are retained. The primary required NFT outputs
are `duplicate_nft_count` and `duplicate_nft_ratio`.

## Tests

Behavioral tests must prove:

- an unordered chain pair emits both directional rows;
- directional counts and ratios use the corresponding primary-chain totals;
- a third chain cannot connect an otherwise unrelated pair;
- the all-chain pool counts a primary NFT once when it matches multiple chains;
- intra-chain results are unaffected by cross-chain matches;
- zero-match pair rows remain present;
- single-chain input emits only `intra_chain`;
- name, URI, and metadata retain their existing matching algorithms.

