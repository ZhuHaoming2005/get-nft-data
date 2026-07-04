# Three-Scope NFT Deduplication Reporting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Verify and lock down isolated pairwise-chain, all-chain, and intra-chain NFT duplicate reporting without changing the matching algorithms.

**Architecture:** Keep the existing three independent result views: same-chain matches feed the intra-chain state, cross-chain matches feed the all-chain state, and each unordered chain pair has an isolated sparse state that emits two directional rows. Add one end-to-end regression fixture that exercises all three views and verifies primary-chain NFT denominators.

**Tech Stack:** Rust, DuckDB, Cargo integration tests.

---

### Task 1: Add a three-scope behavioral regression

**Files:**
- Modify: `name_uri_analysis_rs/tests/analysis.rs`
- Verify: `name_uri_analysis_rs/src/analysis/name_scoring.rs`
- Verify: `name_uri_analysis_rs/src/analysis/components.rs`
- Verify: `name_uri_analysis_rs/src/analysis/metadata.rs`
- Verify: `name_uri_analysis_rs/src/analysis/uri.rs`

- [x] **Step 1: Add the end-to-end regression test**

Add `three_scope_reporting_keeps_pools_isolated_and_uses_primary_chain_totals`.
Its Parquet fixture must contain:

```rust
VALUES
('ethereum', '0xeth-token-cross', '1', 'shared-token-cross', 'img-eth-token-cross',
 'TokenCross', 'tokencross', '{"description":"token cross alpha"}'),
('base', '0xbase-token-cross', '1', 'shared-token-cross', 'img-base-token-cross',
 'TokenCross', 'tokencross', '{"description":"token cross alpha"}'),
('polygon', '0xpoly-token-cross', '1', 'shared-token-cross', 'img-poly-token-cross',
 'TokenCross', 'tokencross', '{"description":"token cross alpha"}'),
('ethereum', '0xeth-image-cross', '1', 'token-eth-image-cross', 'shared-image-cross',
 'ImageCross', 'imagecross', '{"description":"image cross beta"}'),
('base', '0xbase-image-cross', '1', 'token-base-image-cross', 'shared-image-cross',
 'ImageCross', 'imagecross', '{"description":"image cross beta"}'),
('polygon', '0xpoly-image-cross', '1', 'token-poly-image-cross', 'shared-image-cross',
 'ImageCross', 'imagecross', '{"description":"image cross beta"}'),
('ethereum', '0xeth-token-intra-a', '1', 'eth-token-intra', 'img-eth-token-intra-a',
 'TokenIntra', 'tokenintra', '{"description":"ethereum token intra gamma"}'),
('ethereum', '0xeth-token-intra-b', '1', 'eth-token-intra', 'img-eth-token-intra-b',
 'TokenIntra', 'tokenintra', '{"description":"ethereum token intra gamma"}'),
('ethereum', '0xeth-image-intra-a', '1', 'token-eth-image-intra-a', 'eth-image-intra',
 'ImageIntra', 'imageintra', '{"description":"ethereum image intra delta"}'),
('ethereum', '0xeth-image-intra-b', '1', 'token-eth-image-intra-b', 'eth-image-intra',
 'ImageIntra', 'imageintra', '{"description":"ethereum image intra delta"}'),
('solana', 'SolOnly111', 'MintOnly111', 'sol-only', 'img-sol-only',
 'SolOnly', 'solonly', '{"description":"solana only beta"}')
```

Add this assertion helper near `assert_uri_row`:

```rust
fn assert_nft_scope(
    report: &AnalysisReport,
    field_name: &str,
    metric: &str,
    scope: &str,
    primary_chain: &str,
    secondary_chain: &str,
    total_nfts: i64,
    duplicate_nfts: i64,
    duplicate_ratio: f64,
) {
    let row = report
        .summary_rows
        .iter()
        .find(|row| {
            row.field_name == field_name
                && row.metric == metric
                && row.scope == scope
                && row.primary_chain == primary_chain
                && row.secondary_chain == secondary_chain
        })
        .expect("expected three-scope summary row");
    assert_eq!(row.total_nfts, total_nfts);
    assert_eq!(row.duplicate_nft_count, duplicate_nfts);
    assert!((row.duplicate_nft_ratio - duplicate_ratio).abs() < 1e-9);
}
```

For name, URI `v1`/`v2`/`v3`, and metadata, assert:

```rust
for (field, metric, cross_duplicate_nfts, intra_duplicate_nfts) in [
    ("name", "duplicate_group", 2, 4),
    ("uri", "v1", 1, 2),
    ("uri", "v2", 1, 2),
    ("uri", "v3", 2, 4),
    ("metadata", "duplicate_group", 2, 4),
] {
    // Pair pool: computed once, reported in both primary-chain directions.
    assert_nft_scope(&report, field, metric, "chain_matrix", "ethereum", "base", 6, cross_duplicate_nfts, cross_duplicate_nfts as f64 * 100.0 / 6.0);
    assert_nft_scope(&report, field, metric, "chain_matrix", "base", "ethereum", 2, cross_duplicate_nfts, cross_duplicate_nfts as f64 * 100.0 / 2.0);

    // All-chain pool: each Ethereum cross-chain NFT matches two chains but counts once.
    assert_nft_scope(&report, field, metric, "cross_chain_summary", "ethereum", "", 6, cross_duplicate_nfts, cross_duplicate_nfts as f64 * 100.0 / 6.0);

    // Intra-chain pool: only Ethereum-local duplicate NFTs count.
    assert_nft_scope(&report, field, metric, "intra_chain", "ethereum", "", 6, intra_duplicate_nfts, intra_duplicate_nfts as f64 * 100.0 / 6.0);
}
```

Also assert the isolated `(base, polygon)` result and its two directional rows,
and confirm that pairwise rows use the primary chain's `total_nfts`.

- [x] **Step 2: Run the new test**

Run:

```powershell
cargo test --test analysis three_scope_reporting_keeps_pools_isolated_and_uses_primary_chain_totals
```

Expected: PASS if the current implementation already satisfies the approved
design. A pass means no production-code change is warranted; a failure must be
traced to the relevant scope state before any implementation edit.

### Task 2: Verify the complete analysis path

**Files:**
- Verify: `name_uri_analysis_rs/src/analysis/**/*.rs`
- Verify: `name_uri_analysis_rs/tests/analysis.rs`

- [x] **Step 1: Format and run all tests**

Run:

```powershell
cargo fmt -- --check
cargo test
```

Expected: 0 failures.

- [x] **Step 2: Check patch hygiene**

Run from the repository root:

```powershell
git diff --check
git status --short
```

Expected: no whitespace errors and no independent Cargo target directory.
