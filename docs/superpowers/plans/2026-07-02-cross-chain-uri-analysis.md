# Cross-Chain URI Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `cross_chain_summary` and directed `chain_matrix` output for normalized token/image URI reuse while preserving the existing `uri` `v1/v2/v3` report contract.

**Architecture:** Extend DuckDB preparation with cross-chain-only key presence and sparse directed chain-pair contract flags. Reuse the existing `UriCounts` and `push_uri_rows` aggregation path so URI output uses the same primary-chain denominators and scope structure as name and metadata without a full NFT-pair self-join or dense row-to-chain expansion. Load every directed pair total with one grouped query.

**Tech Stack:** Rust 2021, DuckDB SQL, Cargo tests, existing Parquet integration fixtures.

---

## File Structure

- Modify `name_uri_analysis_rs/src/analysis/duckdb_prep.rs`
  - Materialize cross-chain URI keys and directed pair contract flags.
  - Query compact URI counts for one directed chain pair.
- Modify `name_uri_analysis_rs/src/analysis/uri.rs`
  - Emit URI `intra_chain`, `cross_chain_summary`, and `chain_matrix` rows.
- Modify `name_uri_analysis_rs/src/analysis/tests.rs`
  - Unit-test generated SQL for single-chain and multi-chain preparation.
- Modify `name_uri_analysis_rs/tests/analysis.rs`
  - Replace the old no-cross-chain assertion with positive four-dimension
    cross-chain coverage and pair-isolation assertions.
- Modify `name_uri_analysis_rs/README.md`
  - Document URI cross-chain scope and unchanged `v1/v2/v3` semantics.

### Task 1: Build compact cross-chain URI preparation tables

**Files:**
- Modify: `name_uri_analysis_rs/src/analysis/duckdb_prep.rs:160-335`
- Test: `name_uri_analysis_rs/src/analysis/tests.rs:360-405`

- [ ] **Step 1: Add failing SQL-generation tests**

Add tests that require multi-chain preparation to expose global cross-chain
keys, secondary-chain key presence, cross-summary columns, and a directed
pair table:

```rust
#[test]
fn multi_chain_uri_flags_include_cross_chain_tables_and_metrics() {
    let key_sql = build_uri_cross_chain_keys_sql(false);
    let flags_sql = build_uri_contract_flags_sql(true, false);
    let pair_sql = build_uri_chain_pair_contract_flags_sql(false);

    assert!(key_sql.contains("count(DISTINCT chain) >= 2"));
    assert!(flags_sql.contains("uri_cross_chain_keys"));
    assert!(flags_sql.contains("norm_cross_chain_v1_nfts"));
    assert!(pair_sql.contains("uri_key_chain_presence"));
    assert!(pair_sql.contains("primary_chain"));
    assert!(pair_sql.contains("secondary_chain"));
    assert!(pair_sql.contains("norm_chain_v3_contracts"));
}
```

Keep the existing single-chain test and strengthen it so generated SQL does
not reference either cross-chain table:

```rust
assert!(!sql.contains("uri_cross_chain_keys"));
assert!(!sql.contains("norm_cross_chain"));
```

- [ ] **Step 2: Run the SQL unit test and verify RED**

Run:

```powershell
cargo test multi_chain_uri_flags_include_cross_chain_tables_and_metrics --lib -- --nocapture
```

Workdir: `name_uri_analysis_rs`

Expected: compilation fails because
`build_uri_cross_chain_keys_sql` and
`build_uri_chain_pair_contract_flags_sql` do not exist.

- [ ] **Step 3: Materialize global and per-chain URI-key presence**

Add these SQL builders:

```rust
fn build_uri_cross_chain_keys_sql(persist_prepared: bool) -> String {
    format!(
        "
        CREATE {table_scope}TABLE uri_cross_chain_keys AS
        SELECT key_kind, key_value
        FROM uri_key_contracts
        GROUP BY key_kind, key_value
        HAVING count(DISTINCT chain) >= 2;
        ",
        table_scope = prepared_table_scope(persist_prepared),
    )
}

fn build_uri_key_chain_presence_sql(persist_prepared: bool) -> String {
    format!(
        "
        CREATE {table_scope}TABLE uri_key_chain_presence AS
        SELECT DISTINCT keys.chain, keys.key_kind, keys.key_value
        FROM uri_key_contracts keys
        INNER JOIN uri_cross_chain_keys cross_keys
          ON cross_keys.key_kind = keys.key_kind
         AND cross_keys.key_value = keys.key_value;
        ",
        table_scope = prepared_table_scope(persist_prepared),
    )
}
```

Update `build_uri_key_stats` to execute both builders only when
`include_cross_chain` is true. Retain the current
`uri_duplicate_key_stats` table for intra-chain matching.

- [ ] **Step 4: Add cross-chain-any flags to `uri_contract_flags`**

Make `build_uri_contract_flags_sql(true, false)` add two keyed flags:

```sql
coalesce(ct.key_value IS NOT NULL, false) AS norm_token_cross_chain,
coalesce(ci.key_value IS NOT NULL, false) AS norm_image_cross_chain
```

The joins must use `uri_cross_chain_keys` by `(key_kind, key_value)` without a
chain predicate. Append this metric block:

```rust
uri_contract_metric_sql(
    "norm_cross_chain",
    "norm_token_cross_chain",
    "norm_image_cross_chain",
)
```

Keep the false branch byte-for-byte free of cross-table references so
single-chain runs do not build or query unused structures.

Generate the optional fragments explicitly:

```rust
let (cross_key_columns, cross_key_joins) = if include_cross_chain {
    (
        ",
                       coalesce(ct.key_value IS NOT NULL, false) AS norm_token_cross_chain,
                       coalesce(ci.key_value IS NOT NULL, false) AS norm_image_cross_chain",
        "
                LEFT JOIN uri_cross_chain_keys ct
                  ON ct.key_kind = 'norm_token'
                 AND ct.key_value = r.token_uri_norm
                LEFT JOIN uri_cross_chain_keys ci
                  ON ci.key_kind = 'norm_image'
                 AND ci.key_value = r.image_uri_norm",
    )
} else {
    ("", "")
};
```

Pass both fragments into the existing `format!` call. Build
`contract_columns` from the existing intra block and conditionally append:

```rust
let mut columns = uri_contract_metric_sql(
    "norm_contract",
    "norm_token_contract",
    "norm_image_contract",
);
if include_cross_chain {
    columns.push_str(",\n                   ");
    columns.push_str(&uri_contract_metric_sql(
        "norm_cross_chain",
        "norm_token_cross_chain",
        "norm_image_cross_chain",
    ));
}
```

- [ ] **Step 5: Build directed pair contract flags**

Add `build_uri_chain_pair_contract_flags_sql`. It must:

1. select non-empty URI rows from `analysis_rows`;
2. assign each source row a stable `uri_row_id`;
3. inner join `uri_key_chain_presence` separately for token and image keys,
   excluding the source chain;
4. union and merge token/image hits by source row and secondary chain;
5. group the sparse hits by
   `(primary_chain, secondary_chain, contract_address)`;
6. emit `norm_chain_v1/v2/v3_{nfts,contracts}` through
   `uri_contract_metric_sql`.

The builder uses `rows`, `token_hits`, `image_hits`, and `keyed` CTEs.
`token_hits` and `image_hits` use inner joins, while `keyed` combines them with
`UNION ALL` and `bool_or` grouped by source row and directed chain pair. The
final table contains only matching contract-pair rows and does not retain an
unused `total_nfts` column.

Execute it from `build_uri_contract_flags` only when
`include_cross_chain` is true.

- [ ] **Step 6: Add the directed-pair count query**

Add:

```rust
fn load_uri_chain_pair_counts(
    conn: &Connection,
) -> Result<HashMap<String, HashMap<String, UriCounts>>, AnalysisError>
```

Query and sum `norm_chain_v1/v2/v3_{nfts,contracts}` from
`uri_chain_pair_contract_flags`, grouping once by both chain columns. Load the
result into a nested map and emit zero-valued rows for requested pairs absent
from the sparse table.

- [ ] **Step 7: Run SQL unit tests and verify GREEN**

Run:

```powershell
cargo test uri_ --lib -- --nocapture
```

Workdir: `name_uri_analysis_rs`

Expected: all URI SQL-generation unit tests pass.

- [ ] **Step 8: Commit the preparation layer**

```powershell
git add name_uri_analysis_rs/src/analysis/duckdb_prep.rs name_uri_analysis_rs/src/analysis/tests.rs
git commit -m "feat: prepare cross-chain URI match flags"
```

### Task 2: Emit cross-chain URI summaries and matrices

**Files:**
- Modify: `name_uri_analysis_rs/src/analysis/uri.rs:1-70`
- Modify: `name_uri_analysis_rs/tests/analysis.rs:380-420`

- [ ] **Step 1: Replace the negative URI test with a positive fixture**

Replace `cross_chain_uri_rows_are_not_emitted` with
`cross_chain_uri_rows_emit_summary_and_isolated_pair_matrix`. Use four chains:

```sql
VALUES
('ethereum', '0xeth-token', '1', 'shared-token', 'eth-image', 'A', 'a'),
('base', '0xbase-token', '1', 'shared-token', 'base-image', 'B', 'b'),
('ethereum', '0xeth-image', '2', 'eth-only', 'shared-image', 'C', 'c'),
('polygon', '0xpoly-image', '1', 'poly-only', 'shared-image', 'D', 'd'),
('solana', 'So11111111111111111111111111111111111111112',
 'Mint111111111111111111111111111111111111111', 'sol-only', 'sol-image', 'E', 'e')
```

Assert:

```rust
// Ethereum matches another chain by token URI and by image-only URI.
assert_uri_row(&report, "cross_chain_summary", "ethereum", "", "v1", 1, 1);
assert_uri_row(&report, "cross_chain_summary", "ethereum", "", "v2", 1, 1);
assert_uri_row(&report, "cross_chain_summary", "ethereum", "", "v3", 2, 2);

// Pair-specific categorization and third-chain isolation.
assert_uri_row(&report, "chain_matrix", "ethereum", "base", "v1", 1, 1);
assert_uri_row(&report, "chain_matrix", "ethereum", "base", "v2", 0, 0);
assert_uri_row(&report, "chain_matrix", "ethereum", "polygon", "v2", 1, 1);
assert_uri_row(&report, "chain_matrix", "ethereum", "solana", "v3", 0, 0);
assert_uri_row(&report, "chain_matrix", "base", "polygon", "v3", 0, 0);
```

The helper must locate a row by
`field_name`, `scope`, `primary_chain`, `secondary_chain`, and `metric`, then
assert `duplicate_nft_count` and `duplicate_contract_count`:

```rust
fn assert_uri_row(
    report: &AnalysisReport,
    scope: &str,
    primary_chain: &str,
    secondary_chain: &str,
    metric: &str,
    duplicate_nfts: i64,
    duplicate_contracts: i64,
) {
    let row = report
        .summary_rows
        .iter()
        .find(|row| {
            row.field_name == "uri"
                && row.scope == scope
                && row.primary_chain == primary_chain
                && row.secondary_chain == secondary_chain
                && row.metric == metric
        })
        .expect("expected URI summary row");
    assert_eq!(row.duplicate_nft_count, duplicate_nfts);
    assert_eq!(row.duplicate_contract_count, duplicate_contracts);
}
```

- [ ] **Step 2: Run the integration test and verify RED**

Run:

```powershell
cargo test --test analysis cross_chain_uri_rows_emit_summary_and_isolated_pair_matrix -- --nocapture
```

Workdir: `name_uri_analysis_rs`

Expected: failure because `run_uri_analysis` still emits only
`intra_chain` rows.

- [ ] **Step 3: Extend `run_uri_analysis`**

Keep the existing intra-chain loop. When more than one chain is selected:

```rust
let total_for = |chain: &str| {
    totals.get(chain).copied().unwrap_or(NameTotals {
        contracts: 0,
        nfts: 0,
    })
};
let contract_counts = load_uri_contract_counts(conn, true)?;

for chain in chains {
    let counts = contract_counts
        .get(chain)
        .copied()
        .unwrap_or_default()
        .cross_chain;
    push_uri_rows(
        &mut rows,
        "cross_chain_summary",
        chain,
        "",
        "norm_cross",
        total_for(chain),
        counts,
    );
}

let pair_counts = load_uri_chain_pair_counts(conn)?;
for primary in chains {
    for secondary in chains {
        if primary == secondary {
            continue;
        }
        let counts = pair_counts
            .get(primary)
            .and_then(|secondary_counts| secondary_counts.get(secondary))
            .copied()
            .unwrap_or_default();
        push_uri_rows(
            &mut rows,
            "chain_matrix",
            primary,
            secondary,
            "norm_cross",
            total_for(primary),
            counts,
        );
    }
}
```

Use a small local helper or closure for the existing primary-chain total
fallback; do not change denominator semantics.

Update progress work units and messages so all three scopes are represented.

- [ ] **Step 4: Run the integration test and verify GREEN**

Run:

```powershell
cargo test --test analysis cross_chain_uri_rows_emit_summary_and_isolated_pair_matrix -- --nocapture
```

Workdir: `name_uri_analysis_rs`

Expected: the test passes with all summary, pair, and zero-row assertions.

- [ ] **Step 5: Verify single-chain behavior**

Run:

```powershell
cargo test --test analysis only_parquet_chains_are_analyzed_and_single_chain_skips_cross_chain -- --nocapture
```

Expected: the existing single-chain test passes without URI, name, or metadata
cross-chain rows.

- [ ] **Step 6: Commit output behavior**

```powershell
git add name_uri_analysis_rs/src/analysis/uri.rs name_uri_analysis_rs/tests/analysis.rs
git commit -m "feat: report cross-chain URI duplication"
```

### Task 3: Document and verify all four dimensions

**Files:**
- Modify: `name_uri_analysis_rs/README.md:5-16`

- [ ] **Step 1: Update the URI documentation**

Replace the statement that URI only emits intra-chain rows. Document:

- `intra_chain`: cross-contract reuse within one chain;
- `cross_chain_summary`: primary-chain rows matching any other selected chain;
- `chain_matrix`: primary-chain rows matching one named secondary chain;
- `v1/v2/v3` retain their existing exclusive/union meanings;
- name and metadata continue to emit the same two cross-chain scopes.

- [ ] **Step 2: Run formatting**

Run:

```powershell
cargo fmt --check
```

Workdir: `name_uri_analysis_rs`

Expected: exit code 0.

- [ ] **Step 3: Run the full test suite**

Run:

```powershell
cargo test
```

Workdir: `name_uri_analysis_rs`

Expected: every unit, integration, and doc test passes.

- [ ] **Step 4: Run Clippy**

Run:

```powershell
cargo clippy --all-targets -- -D warnings
```

Workdir: `name_uri_analysis_rs`

Expected: exit code 0 with no diagnostics.

- [ ] **Step 5: Verify diff hygiene and scope**

Run:

```powershell
git diff --check
git status --short
git diff --name-only
```

Expected: no whitespace errors; implementation changes are limited to
`name_uri_analysis_rs` plus this design/plan documentation. Existing
Top-manifest parameter fixes remain untouched.

- [ ] **Step 6: Commit documentation**

```powershell
git add name_uri_analysis_rs/README.md docs/superpowers/plans/2026-07-02-cross-chain-uri-analysis.md
git commit -m "docs: describe cross-chain URI metrics"
```
