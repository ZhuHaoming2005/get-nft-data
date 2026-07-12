# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-chain NFT data collection and deduplication analytics system. Supports Ethereum, Base, Polygon, and Solana. The core problem: identifying duplicate/fake NFT contracts across chains using name/symbol similarity, URI normalization, and token transfer analysis.

## Tech Stack

- **Languages**: Python 3, C++17, Rust, SQL (PostgreSQL)
- **Key libs**: psycopg2, aiohttp, web3, duckdb, PyO3, RapidFuzz, CMake
- **PostgreSQL** is the primary datastore; **DuckDB** is used for in-memory analytics in `top_contract_analysis`

## Running the Code

Configuration is loaded from `.env` (copy from `.env.example`). All modules read DB credentials, RPC URLs, and API keys from environment.

### Tests

Python tests remain under `tests/` and can be run individually:

```bash
python -m pytest tests/test_ethereum_log_scanner.py
python -m pytest tests/test_top_contract_analysis_accelerated.py
```

The active Rust crates, `name_uri_analysis_rs` and `top_contract_analysis_rs`, are members of the root Cargo workspace. They share the root `Cargo.lock` and `target/`; do not create per-crate or temporary target directories, and do not routinely run `cargo clean`.

Use the standard PowerShell test entry point:

```powershell
.\scripts\rust-tests.ps1 fast      # unit tests and lightweight integration tests
.\scripts\rust-tests.ps1 db        # DuckDB and Parquet integration tests
.\scripts\rust-tests.ps1 api       # HTTP mocks, providers, and multichain workflows
.\scripts\rust-tests.ps1 cli       # CLI subprocess tests
.\scripts\rust-tests.ps1 full      # all non-snapshot test tiers
.\scripts\rust-tests.ps1 snapshot  # DB tests with export-snapshot coverage
```

During development, run the narrowest relevant target or test name first:

```powershell
cargo test -p top_contract_analysis_rs --test algorithms
cargo test -p name_uri_analysis_rs metadata_bm25
```

Before completing a Rust change, run the affected tier. Run `full` for cross-cutting changes and `snapshot` when snapshot export, Parquet schema, Arrow, or PostgreSQL export behavior changes. Test tiers are defined by the `db-tests`, `api-tests`, `cli-tests`, `expensive-tests`, and `export-snapshot` Cargo features.

Keep pure algorithms and private-helper tests beside their source modules. Cargo integration-test roots under `tests/*.rs` must remain small harnesses; put implementations in the matching `*_cases/` directory so test domains can be split without creating extra linked test binaries. Keep fixtures inside the narrowest applicable domain instead of introducing a global test prelude.

When lower peak resource usage is required, limit both compile and test concurrency:

```powershell
cargo test --workspace -j 4 -- --test-threads=4
```

The detailed command reference and target map are in `docs/rust-testing.md` and each Rust crate's `tests/README.md`.

### Name/Symbol Stats V2 Pipeline (full run)

```bash
python -m name_symbol_stats.main build-contract-identity --run-label apr01 --chains ethereum base polygon solana
python -m name_symbol_stats.main symbol-stats --run-label apr01 --chains ethereum base polygon solana
python -m name_symbol_stats.main prepare-name-tasks --run-label apr01 --chains ethereum base polygon solana --blocking-strategy adaptive_v1 --max-atoms-per-task 30000

# Build C++ worker first
cmake -S name_symbol_stats/cpp -B name_symbol_stats/cpp/build
cmake --build name_symbol_stats/cpp/build --config Release

python -m name_symbol_stats.main run-name-worker --run-label apr01 --worker-exe name_symbol_stats/cpp/build/Release/name_worker.exe --thresholds 85 90 95 --parallel-workers 12
python -m name_symbol_stats.main finalize-name-stats --run-label apr01 --chains ethereum base polygon solana --thresholds 85 90 95
python -m name_symbol_stats.main export-report --run-label apr01 --output-dir name_symbol_stats_output
```

### Build C++ Extension

```bash
cmake -S name_symbol_stats/cpp -B name_symbol_stats/cpp/build
cmake --build name_symbol_stats/cpp/build --config Release
# Output: name_symbol_stats/cpp/build/Release/name_worker.exe (Windows)
```

### Build Rust Extension

```bash
cd rust_ext/top_contract_analysis_rust
cargo build --release
```

## Architecture

### Major Modules

**`name_symbol_stats/`** — V2 pipeline for NFT name/symbol deduplication statistics. Designed for minimal memory use: symbol stats are pushed to PostgreSQL SQL; name fuzzy matching uses unique normalized "atoms" + sharded tasks sent to a C++ worker. CLI entry: `name_symbol_stats/main.py`.

**`top_contract_analysis/`** — Cross-chain contract analysis using Alchemy and Etherscan APIs. Entry: `top_contract_analysis/__main__.py`. The large `__init__.py` (~63KB) contains all dataclasses and API integration logic. Uses DuckDB (`duckdb_store.py`) for in-memory analytics and a Rust FFI bridge (`rust_bridge.py`) for performance-critical string ops.

**`dedup_analysis/`** — Deduplication analysis across chains. `intra_chain.py` handles single-chain dedup in three dimensions (strict URI, normalized URI, cross-contract). `cross_chain.py` extends this across chains. `common.py` provides shared utilities including SQLite-backed dedup index for large datasets.

**Root scripts** — Standalone tools:
- `dedup.py`: Multi-process JSON-vs-DB deduplication using multiprocessing
- `dedup_stats.py`: Three-pass streaming SQL for cross-chain duplicate stats (O(1) memory)
- `trend_stats.py`: Trending fake NFT analysis with OpenSea API and RapidFuzz similarity

### Database Schema

Tables are in `schema.sql` (base), `nft_assets_ethereum.sql` (EVM), `dedup_analysis.sql` (dedup functions), and `name_symbol_stats/sql/01_schema.sql` (V2 pipeline tables: `nsv2_contract_identity`, `nsv2_name_atoms`, `nsv2_name_work_items`, `nsv2_name_match_edges`, `nsv2_name_duplicate_groups`, `nsv2_symbol_duplicate_groups`, `nsv2_duplicate_summary`).

### Key Algorithms

- **URI normalization**: Extracts IPFS CID or Arweave TX ID from varied URL forms for canonical comparison
- **Name blocking**: `adaptive_v1` strategy shards name atoms into tasks by similarity keys to control C++ worker memory
- **Streaming DB processing**: All large-scale operations stream from PostgreSQL cursor-by-cursor; no full in-memory materialization
