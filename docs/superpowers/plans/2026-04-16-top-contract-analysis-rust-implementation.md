# Top Contract Analysis Rust Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Rust CLI in `top_contract_analysis_rs/` that fully replaces the current Python `top_contract_analysis` runtime for `analyze`, `batch`, and `export-snapshot`.

**Architecture:** Create a single Cargo package with focused modules for CLI parsing, domain models, normalization, async API clients, storage, analysis orchestration, progress reporting, and report rendering. Reuse the proven algorithms from `rust_ext/top_contract_analysis_rust` by moving them into the new crate, then layer storage and orchestration around them until the Rust binary can emit the same JSON/Markdown outputs and reuse the same cache artifacts.

**Tech Stack:** Rust 2021, `clap`, `tokio`, `reqwest`, `serde`, `serde_json`, `duckdb`, `polars` or `parquet`, `postgres`, `thiserror`, Cargo integration tests

---

## File Map

- Create: `top_contract_analysis_rs/Cargo.toml`
- Create: `top_contract_analysis_rs/src/main.rs`
- Create: `top_contract_analysis_rs/src/error.rs`
- Create: `top_contract_analysis_rs/src/cli/mod.rs`
- Create: `top_contract_analysis_rs/src/models/mod.rs`
- Create: `top_contract_analysis_rs/src/normalize/mod.rs`
- Create: `top_contract_analysis_rs/src/analysis/mod.rs`
- Create: `top_contract_analysis_rs/src/analysis/scoring.rs`
- Create: `top_contract_analysis_rs/src/analysis/signals.rs`
- Create: `top_contract_analysis_rs/src/analysis/duplicate.rs`
- Create: `top_contract_analysis_rs/src/analysis/address_records.rs`
- Create: `top_contract_analysis_rs/src/api/mod.rs`
- Create: `top_contract_analysis_rs/src/api/alchemy.rs`
- Create: `top_contract_analysis_rs/src/api/etherscan.rs`
- Create: `top_contract_analysis_rs/src/api/opensea.rs`
- Create: `top_contract_analysis_rs/src/store/mod.rs`
- Create: `top_contract_analysis_rs/src/store/duckdb_store.rs`
- Create: `top_contract_analysis_rs/src/store/signal_cache.rs`
- Create: `top_contract_analysis_rs/src/store/postgres_export.rs`
- Create: `top_contract_analysis_rs/src/reporting/mod.rs`
- Create: `top_contract_analysis_rs/src/progress/mod.rs`
- Create: `top_contract_analysis_rs/tests/cli_smoke.rs`
- Create: `top_contract_analysis_rs/tests/algorithms.rs`
- Create: `top_contract_analysis_rs/tests/store.rs`
- Create: `top_contract_analysis_rs/tests/api.rs`
- Create: `top_contract_analysis_rs/tests/analyze.rs`
- Create: `top_contract_analysis_rs/tests/batch.rs`

### Task 1: Bootstrap the Rust crate and CLI surface

**Files:**
- Create: `top_contract_analysis_rs/Cargo.toml`
- Create: `top_contract_analysis_rs/src/main.rs`
- Create: `top_contract_analysis_rs/src/error.rs`
- Create: `top_contract_analysis_rs/src/cli/mod.rs`
- Test: `top_contract_analysis_rs/tests/cli_smoke.rs`

- [ ] **Step 1: Write the failing CLI smoke tests**

```rust
use assert_cmd::Command;
use predicates::str::contains;

#[test]
fn analyze_subcommand_accepts_existing_flag_names() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .args([
            "analyze",
            "--chain", "ethereum",
            "--seed-contract-address", "0xseed",
            "--alchemy-api-key", "key",
        ])
        .assert()
        .failure()
        .stderr(contains("--feature-db"));
}

#[test]
fn export_snapshot_subcommand_is_exposed() {
    Command::cargo_bin("top_contract_analysis_rs")
        .unwrap()
        .arg("export-snapshot")
        .assert()
        .failure()
        .stderr(contains("--output"));
}
```

- [ ] **Step 2: Run the CLI smoke tests to verify they fail**

Run:

```powershell
cargo test --test cli_smoke
```

Expected:

```text
FAIL because the top_contract_analysis_rs crate and binary do not exist yet
```

- [ ] **Step 3: Create the Cargo package and dependency baseline**

Create `top_contract_analysis_rs/Cargo.toml`:

```toml
[package]
name = "top_contract_analysis_rs"
version = "0.1.0"
edition = "2021"

[dependencies]
clap = { version = "4.5", features = ["derive"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "2.0"
tokio = { version = "1.45", features = ["macros", "rt-multi-thread"] }

[dev-dependencies]
assert_cmd = "2.0"
predicates = "3.1"
```

- [ ] **Step 4: Add the minimal CLI with the final subcommand names**

Create `top_contract_analysis_rs/src/main.rs` and `src/cli/mod.rs`:

```rust
mod cli;
mod error;

use clap::Parser;

fn main() -> Result<(), error::AppError> {
    let command = cli::TopContractAnalysisCli::parse();
    match command.command {
        cli::Command::Analyze(args) => Err(error::AppError::NotImplemented(format!("analyze {:?}", args.seed_contract_address))),
        cli::Command::Batch(args) => Err(error::AppError::NotImplemented(format!("batch {:?}", args.seed_file))),
        cli::Command::ExportSnapshot(args) => Err(error::AppError::NotImplemented(format!("export {:?}", args.output))),
    }
}
```

```rust
use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
pub struct TopContractAnalysisCli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Analyze(AnalyzeArgs),
    Batch(BatchArgs),
    ExportSnapshot(ExportSnapshotArgs),
}

#[derive(Args, Debug)]
pub struct AnalyzeArgs {
    #[arg(long, default_value = "ethereum")]
    pub chain: String,
    #[arg(long)]
    pub seed_contract_address: String,
    #[arg(long, default_value = "")]
    pub alchemy_api_key: String,
    #[arg(long, default_value = "")]
    pub feature_db: String,
}

#[derive(Args, Debug)]
pub struct BatchArgs {
    #[arg(long)]
    pub seed_file: String,
}

#[derive(Args, Debug)]
pub struct ExportSnapshotArgs {
    #[arg(long)]
    pub output: String,
}
```

- [ ] **Step 5: Run the CLI smoke tests to verify they pass**

Run:

```powershell
cargo test --test cli_smoke
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis_rs/Cargo.toml top_contract_analysis_rs/src/main.rs top_contract_analysis_rs/src/error.rs top_contract_analysis_rs/src/cli/mod.rs top_contract_analysis_rs/tests/cli_smoke.rs
git commit -m "feat: bootstrap top contract analysis rust cli"
```

### Task 2: Port normalization and scoring algorithms from the existing Rust extension

**Files:**
- Create: `top_contract_analysis_rs/src/models/mod.rs`
- Create: `top_contract_analysis_rs/src/normalize/mod.rs`
- Create: `top_contract_analysis_rs/src/analysis/mod.rs`
- Create: `top_contract_analysis_rs/src/analysis/scoring.rs`
- Test: `top_contract_analysis_rs/tests/algorithms.rs`
- Source reference: `rust_ext/top_contract_analysis_rust/src/common.rs`
- Source reference: `rust_ext/top_contract_analysis_rust/src/scoring.rs`

- [ ] **Step 1: Write failing normalization and scoring tests**

```rust
use top_contract_analysis_rs::analysis::scoring::{metadata_document_from_json, score_metadata_documents, score_name_pairs};
use top_contract_analysis_rs::normalize::{normalize_name, normalize_symbol, normalize_url};

#[test]
fn normalize_name_strips_trailing_token_numbers() {
    assert_eq!(normalize_name("Azuki #123"), "azuki");
}

#[test]
fn metadata_document_from_json_flattens_relevant_fields() {
    let raw = r#"{"description":"cool cat","attributes":[{"trait_type":"Mood","value":"Happy"}]}"#;
    assert_eq!(metadata_document_from_json(raw), "cool cat happy");
}

#[test]
fn score_name_pairs_matches_existing_threshold_behavior() {
    let scores = score_name_pairs(&["Azuki".into()], &["Azuki #1".into()]);
    assert_eq!(scores.len(), 1);
    assert!(scores[0] >= 95.0);
}

#[test]
fn score_metadata_documents_rewards_shared_keywords() {
    let scores = score_metadata_documents(&["gold dragon rare".into()], &["rare dragon gold".into()]);
    assert!(scores[0] >= 0.8);
}
```

- [ ] **Step 2: Run the algorithm tests to verify they fail**

Run:

```powershell
cargo test --test algorithms normalize_name_strips_trailing_token_numbers
```

Expected:

```text
FAIL because the normalize and scoring modules do not exist yet
```

- [ ] **Step 3: Create the domain models used by scoring and matching**

Create `top_contract_analysis_rs/src/models/mod.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedNft {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub symbol: String,
    pub token_uri: String,
    pub image_uri: String,
    pub metadata_json: String,
    pub metadata_doc: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseNftRecord {
    pub contract_address: String,
    pub token_id: String,
    pub token_uri: String,
    pub image_uri: String,
    pub name: String,
    pub symbol: String,
    pub metadata_json: String,
    pub metadata_doc: String,
}
```

- [ ] **Step 4: Port the existing Rust extension logic instead of re-inventing it**

Create `src/normalize/mod.rs` and `src/analysis/scoring.rs` using the existing `rust_ext/top_contract_analysis_rust` logic as the source of truth:

```rust
pub fn normalize_name(raw: &str) -> String {
    strip_trailing_number_suffix(raw)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

pub fn normalize_symbol(raw: &str) -> String {
    raw.trim().to_lowercase()
}

pub fn normalize_url(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_lowercase()
}
```

```rust
pub fn score_name_pairs(left: &[String], right: &[String]) -> Vec<f64> {
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| {
            let left_norm = normalize_name(l);
            let right_norm = normalize_name(r);
            if left_norm.is_empty() || right_norm.is_empty() {
                0.0
            } else if left_norm == right_norm {
                100.0
            } else {
                strsim::normalized_levenshtein(&left_norm, &right_norm) * 100.0
            }
        })
        .collect()
}
```

- [ ] **Step 5: Run the normalization and scoring tests to verify they pass**

Run:

```powershell
cargo test --test algorithms
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis_rs/src/models/mod.rs top_contract_analysis_rs/src/normalize/mod.rs top_contract_analysis_rs/src/analysis/mod.rs top_contract_analysis_rs/src/analysis/scoring.rs top_contract_analysis_rs/tests/algorithms.rs
git commit -m "feat: port normalization and scoring into rust crate"
```

### Task 3: Port duplicate matching, transfer signals, and address record builders

**Files:**
- Create: `top_contract_analysis_rs/src/analysis/signals.rs`
- Create: `top_contract_analysis_rs/src/analysis/duplicate.rs`
- Create: `top_contract_analysis_rs/src/analysis/address_records.rs`
- Modify: `top_contract_analysis_rs/src/models/mod.rs`
- Test: `top_contract_analysis_rs/tests/algorithms.rs`
- Source reference: `rust_ext/top_contract_analysis_rust/src/duplicate.rs`
- Source reference: `rust_ext/top_contract_analysis_rust/src/signals.rs`
- Source reference: `rust_ext/top_contract_analysis_rust/src/address_analysis.rs`

- [ ] **Step 1: Write failing tests for the ported analysis helpers**

```rust
use top_contract_analysis_rs::analysis::duplicate::build_duplicate_candidates;
use top_contract_analysis_rs::analysis::signals::analyze_transfer_signals;
use top_contract_analysis_rs::models::{DatabaseNftRecord, SeedNft, TransferRecord};

#[test]
fn duplicate_candidates_use_token_uri_and_name_reason_flags() {
    let seed_nfts = vec![SeedNft { token_uri: "ipfs://seed/1".into(), name: "Azuki #1".into(), ..Default::default() }];
    let snapshot_rows = vec![DatabaseNftRecord {
        contract_address: "0xdup".into(),
        token_id: "1".into(),
        token_uri: "ipfs://seed/1".into(),
        name: "Azuki Mirror #1".into(),
        symbol: "AZUKI".into(),
        ..Default::default()
    }];
    let rows = build_duplicate_candidates(&seed_nfts, &snapshot_rows, 95.0, 0.55);
    assert_eq!(rows[0].contract_address, "0xdup");
    assert!(rows[0].match_reasons.contains(&"token_uri_match".into()));
}

#[test]
fn transfer_signals_calculate_fast_spread() {
    let signals = analyze_transfer_signals(&vec![
        TransferRecord::mint("0xdup", "1", 100, "0xholder1"),
        TransferRecord::transfer("0xdup", "1", 120, "0xholder1", "0xholder2"),
    ]);
    assert_eq!(signals.mint_to_first_transfer_seconds, Some(20));
    assert!(signals.fast_spread);
}
```

- [ ] **Step 2: Run the focused algorithm tests to verify they fail**

Run:

```powershell
cargo test --test algorithms duplicate_candidates_use_token_uri_and_name_reason_flags
```

Expected:

```text
FAIL because duplicate, signals, and address-record modules are not implemented yet
```

- [ ] **Step 3: Extend the model layer with the ported record types**

Add to `src/models/mod.rs`:

```rust
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DuplicateCandidate {
    pub contract_address: String,
    pub token_id: String,
    pub match_reasons: Vec<String>,
    pub confidence: String,
    pub token_uri: String,
    pub image_uri: String,
    pub name: String,
    pub symbol: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferRecord {
    pub contract_address: String,
    pub token_id: String,
    pub tx_hash: String,
    pub log_index: i64,
    pub block_number: i64,
    pub block_time: i64,
    pub from_address: String,
    pub to_address: String,
    pub event_type: String,
    pub source: String,
}
```

- [ ] **Step 4: Move the existing Rust extension logic into the new crate with equivalent function names**

Create the modules with these public entrypoints:

```rust
pub fn build_duplicate_candidates(
    seed_nfts: &[SeedNft],
    snapshot_rows: &[DatabaseNftRecord],
    name_threshold: f64,
    metadata_threshold: f64,
) -> Vec<DuplicateCandidate> { /* port logic from rust_ext/top_contract_analysis_rust/src/duplicate.rs */ }

pub fn analyze_transfer_signals(transfers: &[TransferRecord]) -> AddressSignals { /* port from signals.rs */ }

pub fn build_infringing_token_records(
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    transfers: &[TransferRecord],
) -> Vec<InfringingTokenRecord> { /* port from address_analysis.rs */ }
```

- [ ] **Step 5: Run the algorithm suite to verify the port passes**

Run:

```powershell
cargo test --test algorithms
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis_rs/src/models/mod.rs top_contract_analysis_rs/src/analysis/signals.rs top_contract_analysis_rs/src/analysis/duplicate.rs top_contract_analysis_rs/src/analysis/address_records.rs top_contract_analysis_rs/tests/algorithms.rs
git commit -m "feat: port duplicate and signal analysis into rust crate"
```

### Task 4: Implement report payloads and Markdown rendering before orchestration

**Files:**
- Create: `top_contract_analysis_rs/src/reporting/mod.rs`
- Modify: `top_contract_analysis_rs/src/models/mod.rs`
- Test: `top_contract_analysis_rs/tests/analyze.rs`
- Test: `top_contract_analysis_rs/tests/batch.rs`

- [ ] **Step 1: Write failing tests for output naming and summary payload shape**

```rust
use top_contract_analysis_rs::reporting::{default_output_basename, render_batch_human_readable_report};
use top_contract_analysis_rs::models::{BatchSummaryPayload, SeedContractPayload, SingleReportPayload};

#[test]
fn default_output_basename_matches_existing_prefix() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload { name: "Azuki".into(), contract_address: "0xseed".into(), ..Default::default() },
        ..Default::default()
    };
    assert_eq!(default_output_basename(&payload), "top_contract_analysis__azuki");
}

#[test]
fn batch_markdown_contains_summary_header() {
    let markdown = render_batch_human_readable_report(&BatchSummaryPayload::default());
    assert!(markdown.contains("# Top NFT 合约批量分析总报告"));
}
```

- [ ] **Step 2: Run the report tests to verify they fail**

Run:

```powershell
cargo test --test batch batch_markdown_contains_summary_header
```

Expected:

```text
FAIL because reporting and payload models are not implemented yet
```

- [ ] **Step 3: Add strong output models instead of passing raw JSON values**

Add to `src/models/mod.rs`:

```rust
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SeedContractPayload {
    pub chain: String,
    pub contract_address: String,
    pub name: String,
    pub symbol: String,
    pub token_type: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReportSummary {
    pub candidate_contract_count: i64,
    pub high_confidence_contract_count: i64,
    pub low_confidence_contract_count: i64,
    pub infringing_nft_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SingleReportPayload {
    pub seed_contract: SeedContractPayload,
    pub report_summary: ReportSummary,
}
```

- [ ] **Step 4: Implement the filename and report renderers with the current output contract**

Create `src/reporting/mod.rs`:

```rust
pub fn default_output_basename(payload: &SingleReportPayload) -> String {
    let seed_name = if payload.seed_contract.name.trim().is_empty() {
        &payload.seed_contract.contract_address
    } else {
        &payload.seed_contract.name
    };
    format!("top_contract_analysis__{}", slugify(seed_name))
}

pub fn render_human_readable_report(payload: &SingleReportPayload) -> String {
    format!(
        "# Top NFT 合约分析报告\n\n- Seed: {}\n- 高置信: {}\n- 低置信: {}\n",
        payload.seed_contract.contract_address,
        payload.report_summary.high_confidence_contract_count,
        payload.report_summary.low_confidence_contract_count,
    )
}
```

- [ ] **Step 5: Run the report tests to verify they pass**

Run:

```powershell
cargo test --test analyze --test batch
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis_rs/src/models/mod.rs top_contract_analysis_rs/src/reporting/mod.rs top_contract_analysis_rs/tests/analyze.rs top_contract_analysis_rs/tests/batch.rs
git commit -m "feat: add rust report payloads and renderers"
```

### Task 5: Implement DuckDB feature store, signal cache, and snapshot export

**Files:**
- Create: `top_contract_analysis_rs/src/store/mod.rs`
- Create: `top_contract_analysis_rs/src/store/duckdb_store.rs`
- Create: `top_contract_analysis_rs/src/store/signal_cache.rs`
- Create: `top_contract_analysis_rs/src/store/postgres_export.rs`
- Test: `top_contract_analysis_rs/tests/store.rs`

- [ ] **Step 1: Write failing storage tests for strict parquet, per-contract cap, and signal cache**

```rust
#[test]
fn strict_parquet_rejects_missing_precomputed_columns() {
    let err = load_parquet_dataset("ethereum", "tests/fixtures/missing_columns.parquet", true).unwrap_err();
    assert!(err.to_string().contains("missing pre-computed columns"));
}

#[test]
fn feature_store_applies_per_contract_token_cap() {
    let snapshot = feature_store.load_snapshot("ethereum", &seed_nfts, 1, 0).unwrap();
    assert_eq!(snapshot.nft_rows.iter().filter(|row| row.contract_address == "0xdup").count(), 1);
}

#[test]
fn signal_cache_round_trips_transfers_and_owners() {
    cache.put("ethereum", "0xdup", "ERC721", &transfers, &owners).unwrap();
    let cached = cache.get("ethereum", "0xdup", "ERC721").unwrap().unwrap();
    assert_eq!(cached.transfers.len(), 1);
    assert_eq!(cached.owners.len(), 1);
}
```

- [ ] **Step 2: Run the storage tests to verify they fail**

Run:

```powershell
cargo test --test store
```

Expected:

```text
FAIL because the store modules do not exist yet
```

- [ ] **Step 3: Implement the DuckDB feature store with the current recall semantics**

Create `src/store/duckdb_store.rs` with these public methods:

```rust
pub struct DuckDbFeatureStore { /* connection wrapper */ }

impl DuckDbFeatureStore {
    pub fn new(database_path: &str) -> Result<Self, AppError> { /* open and migrate */ }

    pub fn load_parquet_dataset(&self, chain: &str, parquet_path: &str, strict: bool) -> Result<(), AppError> { /* validate schema and insert */ }

    pub fn load_snapshot(
        &self,
        chain: &str,
        seed_nfts: &[SeedNft],
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> { /* two-step query with row cap */ }
}
```

- [ ] **Step 4: Implement the signal cache and PostgreSQL-to-Parquet export path**

Create `src/store/signal_cache.rs` and `src/store/postgres_export.rs`:

```rust
pub struct ContractSignalCache { /* duckdb wrapper */ }

impl ContractSignalCache {
    pub fn get(&self, chain: &str, contract_address: &str, token_type: &str) -> Result<Option<CachedSignals>, AppError> { /* deserialize */ }
    pub fn put(&self, chain: &str, contract_address: &str, token_type: &str, transfers: &[TransferRecord], owners: &[OwnerBalance]) -> Result<(), AppError> { /* serialize */ }
}
```

```rust
pub fn export_chain_snapshot_to_parquet(
    conn: &mut postgres::Client,
    chain: &str,
    output_path: &Path,
    fetch_size: usize,
    keep_metadata_json: bool,
) -> Result<(), AppError> { /* stream rows and write parquet */ }
```

- [ ] **Step 5: Run the storage tests to verify they pass**

Run:

```powershell
cargo test --test store
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis_rs/src/store/mod.rs top_contract_analysis_rs/src/store/duckdb_store.rs top_contract_analysis_rs/src/store/signal_cache.rs top_contract_analysis_rs/src/store/postgres_export.rs top_contract_analysis_rs/tests/store.rs
git commit -m "feat: add rust feature store and snapshot export"
```

### Task 6: Implement async API clients with retries and fallback behavior

**Files:**
- Create: `top_contract_analysis_rs/src/api/mod.rs`
- Create: `top_contract_analysis_rs/src/api/alchemy.rs`
- Create: `top_contract_analysis_rs/src/api/etherscan.rs`
- Create: `top_contract_analysis_rs/src/api/opensea.rs`
- Test: `top_contract_analysis_rs/tests/api.rs`

- [ ] **Step 1: Write failing API tests for pagination, retries, and fallback**

```rust
#[tokio::test]
async fn fetch_seed_contract_nfts_paginates_until_page_key_is_empty() {
    let server = start_mock_server(/* first page + next page */).await;
    let rows = fetch_seed_contract_nfts(server.client(), "key", "eth-mainnet", "ethereum", "0xseed").await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn contract_transfers_fall_back_to_etherscan_for_erc721() {
    let rows = fetch_contract_transfers(
        failing_alchemy_client(),
        working_etherscan_client(),
        "ethereum",
        "0xdup",
        "ERC721",
    ).await.unwrap();
    assert_eq!(rows[0].source, "etherscan");
}
```

- [ ] **Step 2: Run the API tests to verify they fail**

Run:

```powershell
cargo test --test api
```

Expected:

```text
FAIL because the API client modules are not implemented yet
```

- [ ] **Step 3: Implement a shared async HTTP client with retry and concurrency control**

Create `src/api/mod.rs`:

```rust
#[derive(Clone)]
pub struct AsyncApiClient {
    pub http: reqwest::Client,
    pub request_limit: Arc<Semaphore>,
    pub contract_limit: Arc<Semaphore>,
    pub sale_metric_limit: Arc<Semaphore>,
}

impl AsyncApiClient {
    pub async fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T, AppError> { /* retry loop */ }
    pub async fn post_json<T: DeserializeOwned, B: Serialize>(&self, url: &str, body: &B) -> Result<T, AppError> { /* retry loop */ }
}
```

- [ ] **Step 4: Implement Alchemy, Etherscan, and OpenSea helpers with the current parse rules**

Create helper entrypoints:

```rust
pub async fn fetch_seed_contract_nfts(/* ... */) -> Result<Vec<SeedNft>, AppError> { /* pagination */ }
pub async fn fetch_contract_metadata(/* ... */) -> Result<ContractMetadata, AppError> { /* alchemy contract metadata */ }
pub async fn fetch_contract_transfers(/* ... */) -> Result<Vec<TransferRecord>, AppError> { /* alchemy then etherscan fallback */ }
pub async fn fetch_contract_sales(/* ... */) -> Result<Vec<NftSaleRecord>, AppError> { /* alchemy then opensea fallback */ }
```

- [ ] **Step 5: Run the API tests to verify they pass**

Run:

```powershell
cargo test --test api
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis_rs/src/api/mod.rs top_contract_analysis_rs/src/api/alchemy.rs top_contract_analysis_rs/src/api/etherscan.rs top_contract_analysis_rs/src/api/opensea.rs top_contract_analysis_rs/tests/api.rs
git commit -m "feat: add rust api clients for nft analysis"
```

### Task 7: Implement the single-seed analyze workflow with TDD

**Files:**
- Create: `top_contract_analysis_rs/src/progress/mod.rs`
- Modify: `top_contract_analysis_rs/src/analysis/mod.rs`
- Modify: `top_contract_analysis_rs/src/reporting/mod.rs`
- Modify: `top_contract_analysis_rs/src/cli/mod.rs`
- Modify: `top_contract_analysis_rs/src/main.rs`
- Test: `top_contract_analysis_rs/tests/analyze.rs`

- [ ] **Step 1: Write failing end-to-end analyze tests**

```rust
#[tokio::test]
async fn analyze_builds_expected_summary_counts() {
    let payload = analyze_seed_contract(AnalyzeRequest {
        chain: "ethereum".into(),
        seed_contract_address: "0xseed".into(),
        alchemy_api_key: "key".into(),
        feature_store: fake_feature_store_with_one_duplicate(),
        api: fake_api_clients(),
        signal_cache: in_memory_signal_cache(),
    }).await.unwrap();

    assert_eq!(payload.report_summary.high_confidence_contract_count, 1);
    assert_eq!(payload.report_summary.candidate_contract_count, 1);
    assert_eq!(payload.seed_contract.contract_address, "0xseed");
}

#[tokio::test]
async fn analyze_writes_default_json_and_markdown_files() {
    let (json_path, md_path) = write_default_outputs(&payload, tempdir.path()).unwrap();
    assert!(json_path.file_name().unwrap().to_string_lossy().starts_with("top_contract_analysis__"));
    assert!(md_path.exists());
}
```

- [ ] **Step 2: Run the analyze tests to verify they fail**

Run:

```powershell
cargo test --test analyze
```

Expected:

```text
FAIL because the analyze orchestration and output writers are not implemented yet
```

- [ ] **Step 3: Define the orchestration request and progress reporter interfaces**

Create the interface in `src/analysis/mod.rs` and `src/progress/mod.rs`:

```rust
pub struct AnalyzeRequest {
    pub chain: String,
    pub seed_contract_address: String,
    pub alchemy_api_key: String,
    pub alchemy_network: Option<String>,
    pub etherscan_api_key: String,
    pub opensea_api_key: String,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub max_tokens_per_contract: usize,
    pub max_recall_rows: usize,
}

#[async_trait::async_trait]
pub trait SeedProgressReporter: Send + Sync {
    async fn on_seed_stage(&self, stage: &str);
    async fn on_high_confidence_contract_completed(&self, contract_address: &str, completed: usize, total: usize);
}
```

- [ ] **Step 4: Implement the analyze pipeline with cache reuse and current summary semantics**

Add the core orchestration:

```rust
pub async fn analyze_seed_contract(request: AnalyzeRequest, deps: AnalysisDeps) -> Result<SingleReportPayload, AppError> {
    let seed_contract = deps.api.fetch_contract_metadata(/* ... */).await?;
    let seed_nfts = deps.api.fetch_seed_contract_nfts(/* ... */).await?;
    let snapshot = deps.feature_store.load_snapshot(
        &request.chain,
        &seed_nfts,
        request.max_tokens_per_contract,
        request.max_recall_rows,
    )?;
    let candidates = build_duplicate_candidates(&seed_nfts, &snapshot.nft_rows, request.name_threshold, request.metadata_threshold);
    let grouped = group_candidates_by_contract(&candidates);
    enrich_high_confidence_contracts(&grouped, &deps, &request).await?;
    build_report_payload(seed_contract, candidates, grouped)
}
```

- [ ] **Step 5: Run the analyze tests to verify they pass**

Run:

```powershell
cargo test --test analyze
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis_rs/src/progress/mod.rs top_contract_analysis_rs/src/analysis/mod.rs top_contract_analysis_rs/src/reporting/mod.rs top_contract_analysis_rs/src/cli/mod.rs top_contract_analysis_rs/src/main.rs top_contract_analysis_rs/tests/analyze.rs
git commit -m "feat: add single-seed rust analysis workflow"
```

### Task 8: Implement batch orchestration, cached-seed skipping, and final verification

**Files:**
- Modify: `top_contract_analysis_rs/src/cli/mod.rs`
- Modify: `top_contract_analysis_rs/src/main.rs`
- Modify: `top_contract_analysis_rs/src/reporting/mod.rs`
- Modify: `top_contract_analysis_rs/src/progress/mod.rs`
- Test: `top_contract_analysis_rs/tests/batch.rs`
- Test: `top_contract_analysis_rs/tests/cli_smoke.rs`

- [ ] **Step 1: Write failing tests for batch cached-seed skipping and summary output**

```rust
#[tokio::test]
async fn batch_skips_cached_seed_reports_in_output_directory() {
    write_cached_seed_json(tempdir.path(), "0xseed1");
    let summary = run_batch(BatchRequest {
        chain: "ethereum".into(),
        seed_file: write_seed_file(&["0xseed1", "0xseed2"]),
        output_dir: tempdir.path().into(),
        workers: 2,
    }).await.unwrap();
    assert_eq!(summary.batch_summary.seed_report_count, 2);
    assert_eq!(summary.seed_reports.len(), 2);
}

#[tokio::test]
async fn batch_writes_summary_files_with_existing_names() {
    let (json_path, md_path) = write_batch_summary_outputs(&summary, tempdir.path()).unwrap();
    assert_eq!(json_path.file_name().unwrap().to_string_lossy(), "top_contract_analysis__summary.json");
    assert_eq!(md_path.file_name().unwrap().to_string_lossy(), "top_contract_analysis__summary.md");
}
```

- [ ] **Step 2: Run the batch tests to verify they fail**

Run:

```powershell
cargo test --test batch
```

Expected:

```text
FAIL because batch orchestration and summary writers are not implemented yet
```

- [ ] **Step 3: Add the batch request type and cached-seed loader**

Add to `src/analysis/mod.rs` or a batch-specific module:

```rust
pub struct BatchRequest {
    pub chain: String,
    pub seed_file: String,
    pub output_dir: PathBuf,
    pub workers: usize,
}

pub fn load_cached_seed_entries(output_dir: &Path, chain: &str) -> Result<HashMap<String, CachedSeedEntry>, AppError> {
    /* scan top_contract_analysis__*.json and ignore summary.json */
}
```

- [ ] **Step 4: Implement the batch runner and wire the CLI binary to all three subcommands**

Add the batch runner and final CLI dispatch:

```rust
pub async fn run_batch(request: BatchRequest, deps: AnalysisDeps) -> Result<BatchSummaryPayload, AppError> {
    let seeds = read_seed_addresses(&request.seed_file)?;
    let cached = load_cached_seed_entries(&request.output_dir, &request.chain)?;
    let pending = seeds.iter().filter(|seed| !cached.contains_key(*seed)).cloned().collect::<Vec<_>>();
    let fresh = futures::stream::iter(pending)
        .map(|seed| analyze_seed_contract(build_analyze_request(seed), deps.clone()))
        .buffer_unordered(request.workers)
        .collect::<Vec<_>>()
        .await;
    build_batch_summary_payload(/* cached + fresh */)
}
```

- [ ] **Step 5: Run the full Rust test suite to verify the rewrite baseline is green**

Run:

```powershell
cargo test
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit**

```bash
git add top_contract_analysis_rs/src/main.rs top_contract_analysis_rs/src/cli/mod.rs top_contract_analysis_rs/src/reporting/mod.rs top_contract_analysis_rs/src/progress/mod.rs top_contract_analysis_rs/tests/batch.rs top_contract_analysis_rs/tests/cli_smoke.rs
git commit -m "feat: add batch orchestration to rust nft analysis cli"
```

## Self-Review

### Spec coverage

- Rust 独立 CLI：Task 1, Task 7, Task 8
- 迁移现有 Rust 算法：Task 2, Task 3
- 强类型 payload 与 Markdown/JSON 输出：Task 4, Task 7, Task 8
- DuckDB/Parquet/PostgreSQL/signal cache：Task 5
- Alchemy/Etherscan/OpenSea/API fallback：Task 6
- 单 seed 分析：Task 7
- batch 与缓存跳过：Task 8

未发现 spec 中没有对应任务的要求。

### Placeholder scan

- 未使用 `TODO`、`TBD`、`implement later`
- 每个任务都给出目标文件、测试入口和命令
- 每个代码步骤都包含具体代码骨架或明确 public API

### Type consistency

- CLI 二进制名称统一为 `top_contract_analysis_rs`
- 单 seed 输出类型统一为 `SingleReportPayload`
- 批量输出类型统一为 `BatchSummaryPayload`
- 核心入口统一为 `analyze_seed_contract`、`run_batch`、`export_chain_snapshot_to_parquet`
