//! Integration test for two-pass Parquet load into ResidentStore.

use analysis2_core::parquet::{load_resident_store, write_tiny_multichain_fixture, LoadOptions};
use analysis2_core::{NoopProgress, ProgressObserver};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

struct RecordingProgress {
    stages: Mutex<Vec<String>>,
    phases: Mutex<Vec<String>>,
    completed: AtomicU64,
    finishes: AtomicU64,
}

impl RecordingProgress {
    fn new() -> Self {
        Self {
            stages: Mutex::new(Vec::new()),
            phases: Mutex::new(Vec::new()),
            completed: AtomicU64::new(0),
            finishes: AtomicU64::new(0),
        }
    }
}

impl ProgressObserver for RecordingProgress {
    fn set_stage(&self, stage: &str) {
        self.stages.lock().unwrap().push(stage.to_owned());
    }

    fn begin_phase(&self, phase: &str, _total: Option<u64>) {
        self.phases.lock().unwrap().push(phase.to_owned());
    }

    fn add_completed(&self, n: u64) {
        self.completed.fetch_add(n, Ordering::Relaxed);
    }

    fn check_cancelled(&self) -> Result<(), analysis2_core::Analysis2Error> {
        Ok(())
    }

    fn finish(&self) {
        self.finishes.fetch_add(1, Ordering::Relaxed);
    }
}

fn fixture_path() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata");
    std::fs::create_dir_all(&dir).expect("testdata dir");
    let path = dir.join("tiny_multichain.parquet");
    write_tiny_multichain_fixture(&path).expect("write fixture");
    path
}

fn default_options() -> LoadOptions {
    LoadOptions::new(
        ["ethereum", "base", "solana"].map(str::to_owned),
        ["ethereum", "base"].map(str::to_owned),
        2,
    )
}

#[test]
fn load_entities_totals_uri_csr_and_descending_anchors() {
    let path = fixture_path();
    let progress = RecordingProgress::new();
    let store = load_resident_store(&[path], &default_options(), &progress).expect("load");

    assert!(
        progress.stages.lock().unwrap().iter().any(|s| s == "load"),
        "ProgressObserver must see load stage"
    );
    let phases = progress.phases.lock().unwrap().clone();
    assert!(
        phases
            .iter()
            .any(|p| p.contains("pass1") || p.contains("scan")),
        "expected pass1/scan phase, got {phases:?}"
    );
    assert!(
        phases
            .iter()
            .any(|p| p.contains("pass2") || p.contains("metadata")),
        "expected pass2/metadata phase, got {phases:?}"
    );
    assert!(progress.completed.load(Ordering::Relaxed) > 0);
    assert_eq!(
        progress.finishes.load(Ordering::Relaxed),
        0,
        "a load subroutine must not stop the caller-owned progress reporter"
    );

    // 2 EVM contracts + 1 Solana collection, 6 NFTs
    assert_eq!(store.contracts.len(), 3);
    assert_eq!(store.nfts.len(), 6);
    assert_eq!(store.rows_loaded, 6);

    let eth = store.chain_ids["ethereum"];
    let base = store.chain_ids["base"];
    let sol = store.chain_ids["solana"];
    assert_eq!(store.totals[&eth].contracts, 1);
    assert_eq!(store.totals[&eth].nfts, 3);
    assert_eq!(store.totals[&base].contracts, 1);
    assert_eq!(store.totals[&base].nfts, 1);
    assert_eq!(store.totals[&sol].contracts, 1);
    assert_eq!(store.totals[&sol].nfts, 2);

    // Shared token URI appears on ethereum 0xaaa#1 and base 0xbbb#5
    let shared = store
        .string_id("ipfs://shared")
        .expect("shared uri interned");
    let shared_nfts = store
        .token_uri_csr
        .values_for(shared)
        .expect("shared uri CSR posting");
    assert_eq!(shared_nfts.len(), 2, "duplicate URI across two contracts");
    assert!(store.token_uri_csr.key_count() >= 4);
    assert!(!store.image_uri_csr.is_empty());

    // Per-NFT names stored for later name finalize
    assert!(store.nfts.iter().all(|n| n.name_id.is_some()));

    // EVM contract 0xaaa anchors: tokens 1,2,10 with k=2 → descending [10, 2]
    let aaa = store
        .contracts
        .iter()
        .find(|c| c.address == "0xaaa")
        .expect("0xaaa");
    let aaa_tokens: Vec<&str> = aaa
        .metadata_by_token
        .iter()
        .map(|r| r.token_id.as_str())
        .collect();
    assert_eq!(aaa_tokens, ["10", "2"]);

    // Solana collection: mint_a, mint_z with k=2 → lex descending [mint_z, mint_a]
    let coll = store
        .contracts
        .iter()
        .find(|c| c.address == "collxyz")
        .expect("solana collection");
    let sol_tokens: Vec<&str> = coll
        .metadata_by_token
        .iter()
        .map(|r| r.token_id.as_str())
        .collect();
    assert_eq!(sol_tokens, ["mint_z", "mint_a"]);
}

#[test]
fn load_options_mixed_case_evm_chains_use_bigint_descending_anchors() {
    let path = fixture_path();
    // Mixed-case option strings must normalize like row `chain` (trim + ascii_lowercase).
    let options = LoadOptions::new(
        ["Ethereum", " Base ", "SOLANA"].map(str::to_owned),
        ["Ethereum", "BASE"].map(str::to_owned),
        2,
    );
    assert!(options.allowed_chains.contains("ethereum"));
    assert!(options.evm_chains.contains("ethereum"));
    assert!(!options.evm_chains.contains("Ethereum"));

    let store = load_resident_store(&[path], &options, &NoopProgress).expect("load");
    let aaa = store
        .contracts
        .iter()
        .find(|c| c.address == "0xaaa")
        .expect("0xaaa");
    let aaa_tokens: Vec<&str> = aaa
        .metadata_by_token
        .iter()
        .map(|r| r.token_id.as_str())
        .collect();
    // Bigint descending (not lex): 10 > 2 > 1 → k=2 keeps [10, 2].
    assert_eq!(aaa_tokens, ["10", "2"]);
}

#[test]
fn conflicting_uri_same_logical_key_errors() {
    let dir = tempfile_dir();
    let path = dir.join("conflict.parquet");
    analysis2_core::parquet::write_uri_conflict_fixture(&path).expect("write");
    let options = LoadOptions::new(
        HashSet::from(["ethereum".to_owned()]),
        HashSet::from(["ethereum".to_owned()]),
        8,
    );
    let err = load_resident_store(&[path], &options, &NoopProgress).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("conflict") || msg.contains("distinct"),
        "unexpected error: {msg}"
    );
}

fn tempfile_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("analysis2_load_entities_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
