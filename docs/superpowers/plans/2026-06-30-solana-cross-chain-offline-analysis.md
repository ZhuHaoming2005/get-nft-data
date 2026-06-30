# Solana Cross-Chain Offline Analysis Implementation Plan

> Execution scope amendment: the user subsequently excluded the
> `name_metadata_change_samples` Rust crate. Task 4 and the analyzer-invocation
> portion of Task 5 are superseded. Task 5 is limited to four-chain seed
> fetching, and final verification excludes that Rust crate.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Export and analyze Ethereum, Base, Polygon, and Solana NFT snapshots without corrupting Solana identifiers, with optional EVM block-range selection and four-chain top-seed orchestration.

**Architecture:** Keep `top_contract_analysis_rs analyze` and `batch` EVM-only. Add chain-aware identity at the offline boundaries: PostgreSQL snapshot export, DuckDB projections, seed lookup, and OpenSea seed parsing. Use `name_uri_analysis_rs` for one four-Parquet dataset-wide run and invoke `name_metadata_change_samples` once per chain for top-seed reports.

**Tech Stack:** Rust 2021, Clap, PostgreSQL, Arrow/Parquet, DuckDB, Python 3 standard library, Cargo tests, Python `unittest`.

---

## File Structure

- Modify `top_contract_analysis_rs/src/store/postgres_export.rs`
  - Own chain-aware export identity, block-range validation, SQL construction,
    and streaming Parquet export.
- Modify `top_contract_analysis_rs/src/store/mod.rs`
  - Re-export the block-range type.
- Modify `top_contract_analysis_rs/src/cli/mod.rs`
  - Expose optional snapshot block bounds.
- Modify `top_contract_analysis_rs/src/main.rs`
  - Pass validated bounds to the exporter.
- Modify `top_contract_analysis_rs/tests/cli_smoke.rs`
  - Cover CLI parsing for bounds.
- Modify `top_contract_analysis_rs/tests/store.rs`
  - Verify Parquet address identity.
- Modify `top_contract_analysis_rs/README.md`
  - Document range behavior and the Solana restriction.
- Modify `name_uri_analysis_rs/src/analysis/duckdb_prep.rs`
  - Build chain-aware contract identity in `analysis_rows`.
- Modify `name_uri_analysis_rs/src/analysis/tests.rs`
  - Unit-test the SQL projection.
- Modify `name_uri_analysis_rs/tests/analysis.rs`
  - Add a four-chain mixed-address integration fixture.
- Modify `name_uri_analysis_rs/README.md`
  - Document the four-Parquet invocation.
- Modify `name_metadata_change_samples/src/lib.rs`
  - Add a feature-source enum, direct Parquet connection, and chain-aware
    contract identity throughout seed and candidate queries.
- Modify `name_metadata_change_samples/src/main.rs`
  - Make `--feature-db` and `--feature-parquet` mutually exclusive inputs.
- Modify `name_metadata_change_samples/tests/sample_collection.rs`
  - Cover direct Parquet analysis and Solana case-sensitive identity.
- Modify `name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py`
  - Fetch four chains, validate chain-specific identifiers, write separate seed
    files, and optionally invoke the Rust sample analyzer.
- Modify `name_metadata_change_samples/tests/test_fetch_opensea_top_seeds.py`
  - Cover four-chain paths, validators, and subprocess command construction.
- Modify `name_metadata_change_samples/README.md`
  - Document direct Parquet and orchestration commands.

### Task 1: Make snapshot rows chain-aware

**Files:**
- Modify: `top_contract_analysis_rs/src/store/postgres_export.rs`
- Modify: `top_contract_analysis_rs/src/store/mod.rs`
- Test: `top_contract_analysis_rs/tests/store.rs`

- [ ] **Step 1: Write failing Parquet identity tests**

Extend the export-only test imports and add two tests:

```rust
#[cfg(feature = "export-snapshot")]
#[test]
fn snapshot_export_lowercases_evm_contract_addresses() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("ethereum.parquet");
    write_snapshot_rows_to_parquet(
        "ethereum",
        &[SnapshotExportRow {
            contract_address: "0xAbCd".into(),
            token_id: "1".into(),
            ..Default::default()
        }],
        &parquet_path,
    )
    .unwrap();

    let conn = Connection::open_in_memory().unwrap();
    let path = parquet_path_literal(&parquet_path);
    let address: String = conn
        .query_row(
            &format!("SELECT contract_address FROM read_parquet('{path}')"),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(address, "0xabcd");
}

#[cfg(feature = "export-snapshot")]
#[test]
fn snapshot_export_preserves_solana_contract_address_case() {
    let dir = tempdir().unwrap();
    let parquet_path = dir.path().join("solana.parquet");
    let address = "SoLanaCaseSensitive111111111111111111111111";
    write_snapshot_rows_to_parquet(
        "solana",
        &[SnapshotExportRow {
            contract_address: address.into(),
            token_id: "MintCaseSensitive11111111111111111111111111".into(),
            ..Default::default()
        }],
        &parquet_path,
    )
    .unwrap();

    let conn = Connection::open_in_memory().unwrap();
    let path = parquet_path_literal(&parquet_path);
    let actual: String = conn
        .query_row(
            &format!("SELECT contract_address FROM read_parquet('{path}')"),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(actual, address);
}
```

- [ ] **Step 2: Run the focused tests and verify the Solana test fails**

Run:

```powershell
cargo test --features export-snapshot snapshot_export_ -- --nocapture
```

Workdir: `top_contract_analysis_rs`

Expected: the EVM test passes under current behavior and the Solana test fails
because `snapshot_batch` lowercases the Base58 address.

- [ ] **Step 3: Add minimal chain-aware export canonicalization**

Add this helper in `postgres_export.rs`:

```rust
fn canonical_contract_address(chain: &str, address: &str) -> String {
    let trimmed = address.trim();
    if chain.eq_ignore_ascii_case("solana") {
        trimmed.to_string()
    } else {
        trimmed.to_lowercase()
    }
}
```

Replace the current unconditional `.to_lowercase()` mapping in
`snapshot_batch` with:

```rust
let contract_address_values: Vec<String> = rows
    .iter()
    .map(|row| canonical_contract_address(chain, &row.contract_address))
    .collect();
```

Do not change the Parquet schema.

- [ ] **Step 4: Run focused tests**

Run:

```powershell
cargo test --features export-snapshot snapshot_export_ -- --nocapture
```

Expected: all snapshot export tests pass.

- [ ] **Step 5: Commit**

```powershell
git add top_contract_analysis_rs/src/store/postgres_export.rs top_contract_analysis_rs/tests/store.rs
git commit -m "fix: preserve Solana snapshot identifiers"
```

### Task 2: Add inclusive EVM snapshot block bounds

**Files:**
- Modify: `top_contract_analysis_rs/src/store/postgres_export.rs`
- Modify: `top_contract_analysis_rs/src/store/mod.rs`
- Modify: `top_contract_analysis_rs/src/cli/mod.rs`
- Modify: `top_contract_analysis_rs/src/main.rs`
- Test: `top_contract_analysis_rs/src/store/postgres_export.rs`
- Test: `top_contract_analysis_rs/tests/cli_smoke.rs`

- [ ] **Step 1: Write failing range validation and SQL tests**

Add a private test module to `postgres_export.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_range_rejects_negative_reversed_and_solana_bounds() {
        assert!(SnapshotBlockRange::new(Some(-1), None).is_err());
        assert!(SnapshotBlockRange::new(Some(20), Some(10)).is_err());

        let range = SnapshotBlockRange::new(Some(10), Some(20)).unwrap();
        let error = range.validate_for_chain("solana").unwrap_err();
        assert!(error.to_string().contains("first_seen_block = 0"));
    }

    #[test]
    fn chain_table_rejects_unsafe_names() {
        assert!(chain_to_table("").is_err());
        assert!(chain_to_table("eth;drop").is_err());
    }

    #[test]
    fn snapshot_query_uses_inclusive_optional_bounds() {
        let lower = build_snapshot_query(
            "nft_assets_ethereum",
            "metadata",
            SnapshotBlockRange::new(Some(10), None).unwrap(),
        );
        assert!(lower.sql.contains("first_seen_block >= $1"));
        assert_eq!(lower.params, vec![10]);

        let bounded = build_snapshot_query(
            "nft_assets_ethereum",
            "metadata",
            SnapshotBlockRange::new(Some(10), Some(20)).unwrap(),
        );
        assert!(bounded.sql.contains("first_seen_block >= $1"));
        assert!(bounded.sql.contains("first_seen_block <= $2"));
        assert_eq!(bounded.params, vec![10, 20]);
    }
}
```

Add a direct Clap parsing test in `tests/cli_smoke.rs`:

```rust
#[test]
fn export_snapshot_accepts_optional_block_bounds() {
    let cli = TopContractAnalysisCli::parse_from([
        "top_contract_analysis_rs",
        "export-snapshot",
        "--output",
        "snapshot.parquet",
        "--start-block",
        "10",
        "--end-block",
        "20",
    ]);
    let CliCommand::ExportSnapshot(args) = cli.command else {
        panic!("expected export-snapshot command");
    };
    assert_eq!(args.start_block, Some(10));
    assert_eq!(args.end_block, Some(20));
}
```

- [ ] **Step 2: Run tests and verify compilation fails on missing APIs**

Run:

```powershell
cargo test --features export-snapshot block_range_ -- --nocapture
cargo test --features export-snapshot snapshot_query_ -- --nocapture
cargo test --features export-snapshot export_snapshot_accepts_optional_block_bounds -- --nocapture
```

Workdir: `top_contract_analysis_rs`

Expected: compilation fails because `SnapshotBlockRange`, `build_snapshot_query`,
and CLI fields do not exist.

- [ ] **Step 3: Implement range validation and query construction**

Add these types in `postgres_export.rs`:

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotBlockRange {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

impl SnapshotBlockRange {
    pub fn new(start: Option<i64>, end: Option<i64>) -> Result<Self, AppError> {
        if start.is_some_and(|value| value < 0) || end.is_some_and(|value| value < 0) {
            return Err(AppError::InvalidData(
                "snapshot block bounds must be non-negative".to_string(),
            ));
        }
        if matches!((start, end), (Some(start), Some(end)) if start > end) {
            return Err(AppError::InvalidData(
                "snapshot start block must not exceed end block".to_string(),
            ));
        }
        Ok(Self { start, end })
    }

    pub fn validate_for_chain(self, chain: &str) -> Result<Self, AppError> {
        if chain.eq_ignore_ascii_case("solana")
            && (self.start.is_some() || self.end.is_some())
        {
            return Err(AppError::InvalidData(
                "Solana block filtering is unavailable because current rows use first_seen_block = 0"
                    .to_string(),
            ));
        }
        Ok(self)
    }
}

struct SnapshotQuery {
    sql: String,
    params: Vec<i64>,
}
```

Tighten `chain_to_table` so it rejects, rather than removes, unsafe
characters:

```rust
fn chain_to_table(chain: &str) -> Result<String, AppError> {
    let normalized = chain.trim().to_lowercase();
    if normalized.is_empty()
        || !normalized
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        return Err(AppError::InvalidData(format!(
            "illegal chain name: {chain:?}"
        )));
    }
    Ok(format!("nft_assets_{normalized}"))
}
```

Implement `build_snapshot_query` so it selects the same seven columns as the
current exporter, appends inclusive predicates in parameter order, and retains
`ORDER BY id`. Select `contract_address` without SQL `lower(...)`; Rust owns
canonicalization.

Update `export_chain_snapshot_to_parquet` to accept
`block_range: SnapshotBlockRange`, validate it before querying, build boxed
PostgreSQL parameters, and call `query_raw`:

```rust
let query = build_snapshot_query(&table, &metadata_column, block_range);
let owned_params: Vec<Box<dyn ToSql + Sync>> = query
    .params
    .into_iter()
    .map(|value| Box::new(value) as Box<dyn ToSql + Sync>)
    .collect();
let params = owned_params
    .iter()
    .map(|value| value.as_ref() as &(dyn ToSql + Sync));
let mut rows = conn.query_raw(&query.sql, params)?;
```

- [ ] **Step 4: Wire CLI arguments**

Add to `ExportSnapshotArgs`:

```rust
#[arg(long)]
pub start_block: Option<i64>,
#[arg(long)]
pub end_block: Option<i64>,
```

Re-export `SnapshotBlockRange` from `store/mod.rs`, import it in `main.rs`, and
construct it before calling the exporter:

```rust
let block_range =
    SnapshotBlockRange::new(args.start_block, args.end_block)?
        .validate_for_chain(&args.chain)?;
```

- [ ] **Step 5: Run focused and full exporter tests**

Run:

```powershell
cargo test --features export-snapshot block_range_ -- --nocapture
cargo test --features export-snapshot snapshot_query_ -- --nocapture
cargo test --features export-snapshot export_snapshot_accepts_optional_block_bounds -- --nocapture
cargo test --features export-snapshot --test store -- --nocapture
```

Expected: all pass.

- [ ] **Step 6: Commit**

```powershell
git add top_contract_analysis_rs/src/store/postgres_export.rs top_contract_analysis_rs/src/store/mod.rs top_contract_analysis_rs/src/cli/mod.rs top_contract_analysis_rs/src/main.rs top_contract_analysis_rs/tests/cli_smoke.rs
git commit -m "feat: filter EVM snapshot exports by block range"
```

### Task 3: Preserve Solana identity in dataset-wide analysis

**Files:**
- Modify: `name_uri_analysis_rs/src/analysis/duckdb_prep.rs`
- Modify: `name_uri_analysis_rs/src/analysis/tests.rs`
- Modify: `name_uri_analysis_rs/tests/analysis.rs`

- [ ] **Step 1: Write failing projection and four-chain tests**

Extend the SQL projection test:

```rust
#[test]
fn analysis_rows_projection_preserves_solana_case_only() {
    let sql = build_analysis_rows_sql("'sample.parquet'", "metadata_json");
    assert!(sql.contains("WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'"));
    assert!(sql.contains("THEN trim(CAST(contract_address AS VARCHAR))"));
    assert!(sql.contains("ELSE lower(trim(CAST(contract_address AS VARCHAR)))"));
}
```

Add an integration test in `tests/analysis.rs` with all four chains:

```rust
#[test]
fn four_chain_analysis_uses_chain_aware_contract_identity() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = temp.path().join("four-chains.parquet");
    write_parquet(
        &parquet,
        r#"
            VALUES
            ('ethereum', '0xAbC', '1', 'eth-1', 'eth-img-1', 'Eth One', 'eth one'),
            ('ethereum', '0xabc', '2', 'eth-2', 'eth-img-2', 'Eth Two', 'eth two'),
            ('base', '0xBase', '1', 'base-1', 'base-img-1', 'Base', 'base'),
            ('polygon', '0xPoly', '1', 'poly-1', 'poly-img-1', 'Polygon', 'polygon'),
            ('solana', 'Abc111111111111111111111111111111111111111', 'Mint1', 'sol-1', 'sol-img-1', 'Sol One', 'sol one'),
            ('solana', 'abc111111111111111111111111111111111111111', 'Mint2', 'sol-2', 'sol-img-2', 'Sol Two', 'sol two')
        "#,
    );

    let report = run_analysis(AnalysisOptions {
        database_path: PathBuf::from(":memory:"),
        parquet_inputs: vec![parquet],
        output_dir: temp.path().join("out"),
        thresholds: vec![95.0],
        threads: 2,
        memory_limit: "256MB".into(),
        analysis_memory_limit: Some("64MB".into()),
        temp_directory: None,
        progress: false,
        persist_prepared: false,
        reuse_prepared: false,
    })
    .unwrap();

    let ethereum = report.summary_rows.iter().find(|row| {
        row.field_name == "name"
            && row.scope == "intra_chain"
            && row.primary_chain == "ethereum"
    }).unwrap();
    let solana = report.summary_rows.iter().find(|row| {
        row.field_name == "name"
            && row.scope == "intra_chain"
            && row.primary_chain == "solana"
    }).unwrap();
    assert_eq!(ethereum.total_contracts, 1);
    assert_eq!(solana.total_contracts, 2);
    assert!(report.summary_rows.iter().any(|row| row.scope == "chain_matrix"));
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
cargo test analysis_rows_projection_preserves_solana_case_only -- --nocapture
cargo test four_chain_analysis_uses_chain_aware_contract_identity -- --nocapture
```

Workdir: `name_uri_analysis_rs`

Expected: projection test fails and the integration test reports one Solana
contract because the current SQL lowercases both.

- [ ] **Step 3: Implement the chain-aware projection**

In `build_analysis_rows_sql`, replace unconditional contract lowercasing with:

```sql
CASE
    WHEN lower(trim(CAST(chain AS VARCHAR))) = 'solana'
        THEN trim(CAST(contract_address AS VARCHAR))
    ELSE lower(trim(CAST(contract_address AS VARCHAR)))
END AS contract_address
```

No downstream query needs a separate special case because all grouping starts
from `analysis_rows`.

- [ ] **Step 4: Run the name/URI suite**

Run:

```powershell
cargo test analysis_rows_projection -- --nocapture
cargo test four_chain_analysis_uses_chain_aware_contract_identity -- --nocapture
cargo test --test analysis -- --nocapture
```

Expected: all pass.

- [ ] **Step 5: Commit**

```powershell
git add name_uri_analysis_rs/src/analysis/duckdb_prep.rs name_uri_analysis_rs/src/analysis/tests.rs name_uri_analysis_rs/tests/analysis.rs
git commit -m "fix: preserve Solana identity in cross-chain analysis"
```

### Task 4: Add direct Parquet seed analysis and Solana identity

**Files:**
- Modify: `name_metadata_change_samples/src/lib.rs`
- Modify: `name_metadata_change_samples/src/main.rs`
- Modify: `name_metadata_change_samples/tests/sample_collection.rs`

- [ ] **Step 1: Write failing direct-Parquet and Solana tests**

Add a `write_feature_parquet` helper using DuckDB `COPY` and add:

```rust
fn write_feature_parquet(
    path: &std::path::Path,
    rows: &[(&str, &str, &str, &str, &str)],
) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "
        CREATE TABLE nft_features (
            chain VARCHAR NOT NULL,
            contract_address VARCHAR NOT NULL,
            token_id VARCHAR NOT NULL,
            token_uri VARCHAR,
            image_uri VARCHAR,
            name VARCHAR,
            symbol VARCHAR,
            metadata_json VARCHAR,
            token_uri_norm VARCHAR,
            image_uri_norm VARCHAR,
            name_norm VARCHAR
        );
        ",
    )
    .unwrap();
    let mut stmt = conn
        .prepare(
            "
            INSERT INTO nft_features (
                chain, contract_address, token_id, token_uri, image_uri, name,
                symbol, metadata_json, token_uri_norm, image_uri_norm, name_norm
            ) VALUES (?, ?, ?, '', '', ?, '', ?, '', '', lower(?))
            ",
        )
        .unwrap();
    for (chain, address, token_id, name, metadata_json) in rows {
        stmt.execute(params![chain, address, token_id, name, metadata_json, name])
            .unwrap();
    }
    drop(stmt);
    let path = path.to_string_lossy().replace('\\', "/").replace('\'', "''");
    conn.execute_batch(&format!(
        "COPY nft_features TO '{path}' (FORMAT PARQUET)"
    ))
    .unwrap();
}

#[test]
fn collect_samples_reads_solana_seed_from_parquet_without_lowercasing() {
    let temp = tempdir().unwrap();
    let parquet = temp.path().join("solana.parquet");
    let input = temp.path().join("seeds.txt");
    let output = temp.path().join("solana.md");
    let seed = "Abc111111111111111111111111111111111111111";
    let case_distinct = "abc111111111111111111111111111111111111111";

    write_feature_parquet(
        &parquet,
        &[
            ("solana", seed, "Mint1", "Azuki", r#"{"description":"gold dragon"}"#),
            ("solana", case_distinct, "Mint2", "Azuki Copy", r#"{"description":"gold dragon"}"#),
        ],
    );
    fs::write(&input, format!("{seed}\n")).unwrap();

    let report = collect_samples(SampleCollectionConfig {
        chain: "solana".into(),
        feature_source: FeatureSource::Parquet(parquet),
        input,
        output,
        name_threshold: 80.0,
        metadata_threshold: 0.1,
        max_recall_rows: 0,
        max_seed_tokens: 0,
        duckdb_threads: 1,
        duckdb_memory_limit: "1GB".into(),
    })
    .unwrap();

    assert_eq!(report.seed_reports.len(), 1);
    assert!(report.seed_reports[0]
        .name
        .matches
        .iter()
        .any(|name| name == "Azuki Copy"));
}
```

Add the corresponding EVM test:

```rust
#[test]
fn collect_samples_reads_evm_seed_case_insensitively() {
    let temp = tempdir().unwrap();
    let parquet = temp.path().join("ethereum.parquet");
    let input = temp.path().join("seeds.txt");
    let output = temp.path().join("ethereum.md");
    write_feature_parquet(
        &parquet,
        &[
            ("ethereum", "0xabc", "1", "Azuki", r#"{"description":"gold dragon"}"#),
            ("ethereum", "0xdef", "2", "Azuki Copy", r#"{"description":"gold dragon"}"#),
        ],
    );
    fs::write(&input, "0xAbC\n").unwrap();

    let report = collect_samples(SampleCollectionConfig {
        chain: "ethereum".into(),
        feature_source: FeatureSource::Parquet(parquet),
        input,
        output,
        name_threshold: 80.0,
        metadata_threshold: 0.1,
        max_recall_rows: 0,
        max_seed_tokens: 0,
        duckdb_threads: 1,
        duckdb_memory_limit: "1GB".into(),
    })
    .unwrap();

    assert_eq!(report.seed_reports.len(), 1);
    assert_eq!(report.seed_reports[0].name.seed, "Azuki");
}
```

- [ ] **Step 2: Run tests and verify compilation fails**

Run:

```powershell
cargo test collect_samples_reads_solana_seed_from_parquet_without_lowercasing -- --nocapture
```

Workdir: `name_metadata_change_samples`

Expected: compilation fails because `FeatureSource` and the Parquet path do not
exist.

- [ ] **Step 3: Introduce the explicit feature source**

Replace `feature_db: PathBuf` in `SampleCollectionConfig` with:

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum FeatureSource {
    DuckDb(PathBuf),
    Parquet(PathBuf),
}

pub struct SampleCollectionConfig {
    pub chain: String,
    pub feature_source: FeatureSource,
    pub input: PathBuf,
    pub output: PathBuf,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub max_recall_rows: usize,
    pub max_seed_tokens: usize,
    pub duckdb_threads: usize,
    pub duckdb_memory_limit: String,
}
```

Update existing tests to construct
`FeatureSource::DuckDb(feature_db)`.

Implement:

```rust
fn open_feature_connection(
    config: &SampleCollectionConfig,
) -> Result<Connection, SampleCollectionError>
```

For `DuckDb`, retain the current read-only behavior. For `Parquet`, verify the
file exists, open an in-memory connection, apply the same DuckDB resource
settings, and create:

```sql
CREATE VIEW nft_features AS
SELECT * FROM read_parquet('<escaped path>');
```

Add a `MissingFeatureParquet(String)` error variant. Keep all resource settings
in one helper so DB and Parquet sources do not diverge.

- [ ] **Step 4: Make every contract key chain-aware**

Add Rust and SQL helpers:

```rust
fn canonical_contract_address(chain: &str, address: &str) -> String {
    let trimmed = address.trim();
    if chain.eq_ignore_ascii_case("solana") {
        trimmed.to_string()
    } else {
        trimmed.to_lowercase()
    }
}

fn contract_identity_sql(chain_column: &str, address_column: &str) -> String {
    format!(
        "CASE WHEN lower(trim(CAST({chain_column} AS VARCHAR))) = 'solana' \
         THEN trim(CAST({address_column} AS VARCHAR)) \
         ELSE lower(trim(CAST({address_column} AS VARCHAR))) END"
    )
}
```

Use them in:

- `read_seed_contracts`, which now receives `chain`;
- `read_seed_rows`;
- `load_name_index`;
- `load_metadata_index`;
- `read_candidate_metadata_rows`;
- seed/candidate exclusion keys.

Do not use `lower(contract_address)` in any seed-analysis SQL path after this
step.

- [ ] **Step 5: Expose mutually exclusive CLI inputs**

Use a Clap argument group in `src/main.rs`:

```rust
#[command(group(
    clap::ArgGroup::new("feature_source")
        .required(true)
        .args(["feature_db", "feature_parquet"])
))]
struct Args {
    #[arg(long)]
    feature_db: Option<PathBuf>,
    #[arg(long)]
    feature_parquet: Option<PathBuf>,
    // existing arguments
}
```

Map the selected path to `FeatureSource` before constructing the config.

- [ ] **Step 6: Run focused and full tests**

Run:

```powershell
cargo test collect_samples_reads_solana_seed_from_parquet_without_lowercasing -- --nocapture
cargo test collect_samples_reads_evm_seed_case_insensitively -- --nocapture
cargo test --test sample_collection -- --nocapture
cargo test -- --nocapture
```

Expected: all pass.

- [ ] **Step 7: Commit**

```powershell
git add name_metadata_change_samples/src/lib.rs name_metadata_change_samples/src/main.rs name_metadata_change_samples/tests/sample_collection.rs
git commit -m "feat: analyze chain-aware seed samples from Parquet"
```

### Task 5: Fetch and analyze top seeds for four chains

**Files:**
- Modify: `name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py`
- Modify: `name_metadata_change_samples/tests/test_fetch_opensea_top_seeds.py`

- [ ] **Step 1: Write failing address and orchestration tests**

Add tests for these public functions:

```python
def test_normalize_contract_address_preserves_valid_solana_case(self):
    address = "11111111111111111111111111111111"
    self.assertEqual(
        fetch_opensea_top_seeds.normalize_contract_address(address, "solana"),
        address,
    )

def test_normalize_contract_address_lowercases_evm(self):
    address = "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD"
    self.assertEqual(
        fetch_opensea_top_seeds.normalize_contract_address(address, "base"),
        address.lower(),
    )

def test_default_chains_cover_all_four_datasets(self):
    args = fetch_opensea_top_seeds.parse_args([])
    self.assertEqual(
        args.chains,
        ["ethereum", "base", "polygon", "solana"],
    )

def test_build_analysis_command_uses_chain_parquet_seed_and_report(self):
    command = fetch_opensea_top_seeds.build_analysis_command(
        cargo="cargo",
        manifest_path=Path("name_metadata_change_samples/Cargo.toml"),
        chain="solana",
        parquet_path=Path("snapshots/solana.parquet"),
        seed_path=Path("seeds/solana.seeds.txt"),
        output_path=Path("reports/solana.md"),
    )
    self.assertIn("--feature-parquet", command)
    self.assertIn("snapshots/solana.parquet", command)
    self.assertIn("--chain", command)
    self.assertIn("solana", command)
```

Add an independent-collection test:

```python
def test_collect_seed_addresses_by_chain_calls_each_chain_independently(self):
    calls = []

    def collector(**kwargs):
        calls.append(kwargs["chain"])
        suffix = len(calls)
        return [f"0x{suffix:040x}"]

    result = fetch_opensea_top_seeds.collect_seed_addresses_by_chain(
        chains=["ethereum", "base", "polygon", "solana"],
        collector=collector,
        api_key="key",
        limit=1,
        page_size=1,
        trending_collections_url="https://example.test/trending",
        timeframe="thirty_days",
        timeout=1.0,
    )

    self.assertEqual(calls, ["ethereum", "base", "polygon", "solana"])
    self.assertEqual(set(result), {"ethereum", "base", "polygon", "solana"})
```

Add a failed-runner test:

```python
def test_run_analysis_for_chain_reports_failed_chain(self):
    def failing_runner(command, check):
        raise subprocess.CalledProcessError(7, command)

    with self.assertRaisesRegex(RuntimeError, "solana"):
        fetch_opensea_top_seeds.run_analysis_for_chain(
            cargo="cargo",
            manifest_path=Path("Cargo.toml"),
            chain="solana",
            parquet_path=Path("solana.parquet"),
            seed_path=Path("solana.seeds.txt"),
            output_path=Path("solana.md"),
            runner=failing_runner,
        )
```

- [ ] **Step 2: Run the Python tests and verify failure**

Run:

```powershell
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_top_seeds -v
```

Workdir: repository root.

Expected: failures for missing multi-chain, normalization, and command APIs.

- [ ] **Step 3: Implement chain-aware address validation**

Add:

```python
DEFAULT_CHAINS = ("ethereum", "base", "polygon", "solana")
EVM_CHAINS = frozenset({"ethereum", "base", "polygon"})
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_INDEX = {char: index for index, char in enumerate(BASE58_ALPHABET)}

def decode_base58(value: str) -> bytes:
    number = 0
    for char in value:
        if char not in BASE58_INDEX:
            raise ValueError("invalid Base58 character")
        number = number * 58 + BASE58_INDEX[char]
    body = number.to_bytes((number.bit_length() + 7) // 8, "big")
    leading_zeroes = len(value) - len(value.lstrip("1"))
    return b"\x00" * leading_zeroes + body

def normalize_contract_address(value: str, chain: str) -> str | None:
    address = value.strip()
    normalized_chain = chain.lower()
    if normalized_chain in EVM_CHAINS:
        lowered = address.lower()
        return lowered if ADDRESS_RE.fullmatch(lowered) else None
    if normalized_chain == "solana":
        try:
            return address if len(decode_base58(address)) == 32 else None
        except ValueError:
            return None
    return None
```

Route string and dictionary values in `contract_address_from_value` through
this helper. Do not lowercase Solana values in `format_seeds`.

- [ ] **Step 4: Implement multi-chain CLI and output paths**

Use a mutually exclusive parser group for legacy `--chain` and multi-value
`--chains`. Normalize selected chains after parsing; if neither flag is passed,
use `DEFAULT_CHAINS`.

Rules:

- `--output` is valid only for a single selected chain.
- Multi-chain output uses `--output-dir` and `<chain>.seeds.txt`.
- `--analyze` requires `--parquet-dir`.
- Reports use `--analysis-output-dir` and `<chain>.md`.

Factor the loop into a function that accepts a fetch callable so tests do not
make live requests.

Use this exact collection boundary:

```python
def collect_seed_addresses_by_chain(
    *,
    chains: list[str],
    collector=collect_seed_addresses,
    **collector_kwargs: Any,
) -> dict[str, list[str]]:
    return {
        chain: collector(chain=chain, **collector_kwargs)
        for chain in chains
    }
```

- [ ] **Step 5: Implement optional analyzer invocation**

Add `build_analysis_command` and use `subprocess.run(command, check=True)`.
The command must be:

```text
cargo run --release
  --manifest-path <name_metadata_change_samples/Cargo.toml>
  --
  --chain <chain>
  --feature-parquet <parquet-dir>/<chain>.parquet
  --input <output-dir>/<chain>.seeds.txt
  --output <analysis-output-dir>/<chain>.md
```

Catch `CalledProcessError`, print which chain failed, and return non-zero.
Do not run any analyzer unless `--analyze` is present.

Use an injectable runner at the subprocess boundary:

```python
def run_analysis_for_chain(
    *,
    cargo: str,
    manifest_path: Path,
    chain: str,
    parquet_path: Path,
    seed_path: Path,
    output_path: Path,
    runner=subprocess.run,
) -> None:
    command = build_analysis_command(
        cargo=cargo,
        manifest_path=manifest_path,
        chain=chain,
        parquet_path=parquet_path,
        seed_path=seed_path,
        output_path=output_path,
    )
    try:
        runner(command, check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"analysis failed for chain {chain}") from exc
```

- [ ] **Step 6: Run all script tests**

Run:

```powershell
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_top_seeds -v
```

Expected: all pass without network access.

- [ ] **Step 7: Commit**

```powershell
git add name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py name_metadata_change_samples/tests/test_fetch_opensea_top_seeds.py
git commit -m "feat: orchestrate four-chain top seed analysis"
```

### Task 6: Document the end-to-end workflows

**Files:**
- Modify: `top_contract_analysis_rs/README.md`
- Modify: `name_uri_analysis_rs/README.md`
- Modify: `name_metadata_change_samples/README.md`

- [ ] **Step 1: Update snapshot documentation**

Add EVM examples:

```powershell
cargo run --release --features export-snapshot -- export-snapshot `
  --chain ethereum `
  --start-block 19000000 `
  --end-block 20000000 `
  --output ../output/top_contract_analysis/ethereum.parquet
```

Document inclusive bounds and explicitly state that Solana rejects these flags
until its ingestion pipeline stores real slots instead of `0`.

- [ ] **Step 2: Update the four-chain dataset analysis example**

Show:

```powershell
cargo run --release -- `
  --parquet ./data/ethereum.parquet `
  --parquet ./data/base.parquet `
  --parquet ./data/polygon.parquet `
  --parquet ./data/solana.parquet `
  --output-dir ./output `
  --threads 96
```

State that Solana collection addresses and mint IDs remain case-sensitive.

- [ ] **Step 3: Update top-seed documentation**

Show fetch plus analysis:

```powershell
python .\scripts\fetch_opensea_top_seeds.py `
  --output-dir .\seeds `
  --analyze `
  --parquet-dir ..\output\top_contract_analysis `
  --analysis-output-dir .\reports
```

Also show legacy single-chain fetch and direct Rust Parquet analysis.

- [ ] **Step 4: Check documentation and commit**

Run:

```powershell
git diff --check
rg -n "first_seen_block|start-block|end-block|solana.parquet|--analyze" top_contract_analysis_rs/README.md name_uri_analysis_rs/README.md name_metadata_change_samples/README.md
```

Expected: no whitespace errors and every new workflow is documented.

Commit:

```powershell
git add top_contract_analysis_rs/README.md name_uri_analysis_rs/README.md name_metadata_change_samples/README.md
git commit -m "docs: describe four-chain offline analysis"
```

### Task 7: Format and verify the complete vertical slice

**Files:**
- Verify all modified files.

- [ ] **Step 1: Format Rust and check Python syntax**

Run:

```powershell
cargo fmt --manifest-path top_contract_analysis_rs/Cargo.toml
cargo fmt --manifest-path name_uri_analysis_rs/Cargo.toml
cargo fmt --manifest-path name_metadata_change_samples/Cargo.toml
python -c "import ast, pathlib; ast.parse(pathlib.Path(r'name_metadata_change_samples/scripts/fetch_opensea_top_seeds.py').read_text(encoding='utf-8'))"
```

Expected: all commands exit zero.

- [ ] **Step 2: Run complete targeted test suites**

Run:

```powershell
cargo test --manifest-path top_contract_analysis_rs/Cargo.toml --features export-snapshot
cargo test --manifest-path name_uri_analysis_rs/Cargo.toml
cargo test --manifest-path name_metadata_change_samples/Cargo.toml
python -m unittest name_metadata_change_samples.tests.test_fetch_opensea_top_seeds -v
```

Expected: all tests pass.

- [ ] **Step 3: Run static checks**

Run:

```powershell
cargo clippy --manifest-path top_contract_analysis_rs/Cargo.toml --features export-snapshot --all-targets -- -D warnings
cargo clippy --manifest-path name_uri_analysis_rs/Cargo.toml --all-targets -- -D warnings
cargo clippy --manifest-path name_metadata_change_samples/Cargo.toml --all-targets -- -D warnings
git diff --check
git status --short
```

Expected: clippy and whitespace checks pass; status contains only the intended
implementation changes, or is clean if all task commits were created.

- [ ] **Step 4: Verify the local end-to-end evidence**

Use the integration-test fixtures as the deterministic end-to-end proof:

```powershell
cargo test --manifest-path name_uri_analysis_rs/Cargo.toml four_chain_analysis_uses_chain_aware_contract_identity -- --nocapture
cargo test --manifest-path name_metadata_change_samples/Cargo.toml collect_samples_reads_solana_seed_from_parquet_without_lowercasing -- --nocapture
```

Expected: the four-chain dataset analysis and the Solana top-seed Parquet flow
both pass without PostgreSQL, OpenSea, Alchemy, or Etherscan.

- [ ] **Step 5: Commit formatting-only changes if needed**

If `cargo fmt` changed files after earlier commits:

```powershell
git add top_contract_analysis_rs name_uri_analysis_rs name_metadata_change_samples
git commit -m "style: format cross-chain analysis changes"
```

If formatting produced no diff, do not create an empty commit.
