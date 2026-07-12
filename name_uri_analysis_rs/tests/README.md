# Integration test layout

`analysis.rs` is the single Cargo integration-test target and requires `db-tests`. Its implementation lives in `analysis_cases/` so future query, metadata, URI, and multichain scenarios can be separated without creating additional linked test binaries.

Keep pure algorithm and private-helper tests beside their source modules under `src/`. Keep Parquet and DuckDB boundary coverage in this integration target.
