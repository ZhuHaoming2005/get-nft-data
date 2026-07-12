# Rust testing

The repository shares one Cargo build directory at `target/`. Test profiles omit debug information to avoid large Windows PDB files.

The two active Rust crates are members of the root Cargo workspace and use one root `Cargo.lock`. The standard entry point is `scripts/rust-tests.ps1`:

```powershell
.\scripts\rust-tests.ps1 fast
.\scripts\rust-tests.ps1 db
.\scripts\rust-tests.ps1 api
.\scripts\rust-tests.ps1 cli
.\scripts\rust-tests.ps1 full
.\scripts\rust-tests.ps1 snapshot
```

Use direct Cargo commands when filtering to a specific test during development.

## Fast development tests

Run the default suite for the complete workspace:

```powershell
cargo test --workspace
```

The default tier contains unit tests plus the lightweight `algorithms` and `config` integration targets in `top_contract_analysis_rs`.

For the shortest feedback loop, filter by target or test name:

```powershell
cargo test -p top_contract_analysis_rs --test algorithms
cargo test -p name_uri_analysis_rs metadata_bm25
```

## Complete regression tests

High-cost tests are grouped by responsibility:

```powershell
# DuckDB and Parquet integration
cargo test --workspace --features db-tests

# HTTP mocks, API workflows, and multichain orchestration
cargo test -p top_contract_analysis_rs --features api-tests

# CLI subprocess coverage
cargo test -p top_contract_analysis_rs --features cli-tests

# All three groups
cargo test --workspace --features expensive-tests
```

Snapshot export coverage additionally requires `export-snapshot`:

```powershell
cargo test -p top_contract_analysis_rs --features "db-tests export-snapshot"
```

Do not routinely run `cargo clean`; the shared target is intended to be reused. Limit compile and test concurrency when lower peak CPU or memory usage is more important than elapsed time:

```powershell
cargo test --workspace -j 4 -- --test-threads=4
```
