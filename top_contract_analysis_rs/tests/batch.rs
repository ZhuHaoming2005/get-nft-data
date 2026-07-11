use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tempfile::tempdir;
use top_contract_analysis_rs::analysis::{
    run_batch, AnalysisDeps, AnalyzeApi, BatchRequest, CandidateSeedHolderRequest,
    FeatureStoreReader,
};
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::models::{
    AddressAttributionPayload, AddressSignalPayload, BatchSummaryPayload, ContractDuplicateRecord,
    ContractMetadata, DatabaseNftRecord, DatabaseSnapshot, EthTransferRecord, HonestAddressPayload,
    InfringingTokenRecord, MaliciousAddressPayload, NftSaleRecord, OwnerBalance,
    PaperDuplicateScaleRowPayload, PaperStatsPayload, SecondarySaleVictimAddressPayload,
    SeedContractPayload, SeedNft, SingleReportPayload, TransactionReceiptRecord, TransferRecord,
    ValueFlowEdgePayload, VictimAcquisitionAddressPayload,
};
use top_contract_analysis_rs::progress::{
    BatchProgressReporter, NoopBatchProgressReporter, NoopProgressReporter, SeedProgressReporter,
};
use top_contract_analysis_rs::reporting::{
    render_batch_human_readable_report, write_batch_paper_stats_outputs,
};

struct EmptyFeatureStore;

impl FeatureStoreReader for EmptyFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        Ok(DatabaseSnapshot::default())
    }
}

#[derive(Default)]
struct CapturingBatchFeatureStore {
    captured_seed_names: Arc<Mutex<BTreeMap<String, Vec<String>>>>,
}

impl FeatureStoreReader for CapturingBatchFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        let mut captured_seed_names = self.captured_seed_names.lock().unwrap();
        if let Some(seed) = seed_nfts.first() {
            captured_seed_names.insert(
                seed.contract_address.clone(),
                seed_nfts.iter().map(|item| item.name.clone()).collect(),
            );
        }
        Ok(DatabaseSnapshot::default())
    }

    fn load_snapshots(
        &self,
        chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        let mut snapshots = BTreeMap::new();
        {
            let mut captured_seed_names = self.captured_seed_names.lock().unwrap();
            for (seed_address, seed_nfts) in seeds {
                captured_seed_names.insert(
                    seed_address.clone(),
                    seed_nfts.iter().map(|item| item.name.clone()).collect(),
                );
            }
        }
        for (seed_address, seed_nfts) in seeds {
            snapshots.insert(
                seed_address.clone(),
                self.load_snapshot(
                    chain,
                    seed_nfts,
                    name_threshold,
                    metadata_threshold,
                    max_tokens_per_contract,
                    max_recall_rows,
                )?,
            );
        }
        Ok(snapshots)
    }
}

#[derive(Default)]
struct BatchPipelineProbe {
    snapshot_active: AtomicUsize,
    cpu_current: AtomicUsize,
    cpu_max_seen: AtomicUsize,
    transfer_current: AtomicUsize,
    transfer_max_seen: AtomicUsize,
    candidate_transfer_current: AtomicUsize,
    candidate_transfer_max_seen: AtomicUsize,
    holder_current: AtomicUsize,
    holder_max_seen: AtomicUsize,
    expansion_current: AtomicUsize,
    expansion_max_seen: AtomicUsize,
    obsolete_metric_active: AtomicBool,
    obsolete_metric_finished: AtomicBool,
    obsolete_metric_receipt_calls: AtomicUsize,
    holder_started_before_obsolete_metric_finished: AtomicBool,
    expansion_observed_obsolete_metric_active: AtomicBool,
    metadata_calls: AtomicUsize,
    metadata_current: AtomicUsize,
    metadata_max_seen: AtomicUsize,
    candidate_metadata_calls: AtomicUsize,
    candidate_metadata_current: AtomicUsize,
    candidate_metadata_max_seen: AtomicUsize,
    snapshot_calls: AtomicUsize,
    snapshot_batch_calls: AtomicUsize,
    snapshot_started_during_contract_analysis: AtomicBool,
}

impl BatchPipelineProbe {
    fn record_max(max_seen: &AtomicUsize, current: usize) {
        loop {
            let seen = max_seen.load(Ordering::SeqCst);
            if current <= seen {
                break;
            }
            if max_seen
                .compare_exchange(seen, current, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }
}

struct InstrumentedFeatureStore {
    probe: Arc<BatchPipelineProbe>,
    sleep_ms: u64,
    wait_for_transfer_before_seed_two_snapshot: bool,
    panic_on_seed_snapshot: Option<&'static str>,
}

impl FeatureStoreReader for InstrumentedFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        self.probe.snapshot_calls.fetch_add(1, Ordering::SeqCst);
        let current = self.probe.cpu_current.fetch_add(1, Ordering::SeqCst) + 1;
        BatchPipelineProbe::record_max(&self.probe.cpu_max_seen, current);

        let seed_address = seed_nfts
            .first()
            .map(|seed| seed.contract_address.as_str())
            .unwrap_or_default();
        if self.panic_on_seed_snapshot == Some(seed_address) {
            panic!("forced snapshot panic for {seed_address}");
        }
        self.probe.snapshot_active.fetch_add(1, Ordering::SeqCst);
        if seed_address == "0xseed2"
            && self.probe.candidate_transfer_current.load(Ordering::SeqCst) > 0
        {
            self.probe
                .snapshot_started_during_contract_analysis
                .store(true, Ordering::SeqCst);
        }
        if self.wait_for_transfer_before_seed_two_snapshot && seed_address == "0xseed2" {
            let mut waited = 0;
            while self.probe.transfer_current.load(Ordering::SeqCst) == 0 && waited < 1000 {
                std::thread::sleep(Duration::from_millis(1));
                waited += 1;
            }
        }
        if self.sleep_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.sleep_ms));
        }
        self.probe.snapshot_active.fetch_sub(1, Ordering::SeqCst);
        self.probe.cpu_current.fetch_sub(1, Ordering::SeqCst);

        let seed = seed_nfts.first().cloned().unwrap_or_default();
        let suffix = seed.contract_address.trim_start_matches("0x");
        Ok(DatabaseSnapshot {
            nft_rows: vec![DatabaseNftRecord {
                contract_address: format!("0xcandidate{suffix}"),
                token_id: "1".into(),
                token_uri: seed.token_uri,
                image_uri: seed.image_uri,
                name: seed.name,
                symbol: seed.symbol,
                metadata_json: seed.metadata_json,
                metadata_recall_checked: false,
                metadata_recall_match: false,
            }],
            ..DatabaseSnapshot::default()
        })
    }

    fn load_snapshots(
        &self,
        chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        self.probe
            .snapshot_batch_calls
            .fetch_add(1, Ordering::SeqCst);
        let mut rows = BTreeMap::new();
        for (seed_address, seed_nfts) in seeds {
            rows.insert(
                seed_address.clone(),
                self.load_snapshot(
                    chain,
                    seed_nfts,
                    name_threshold,
                    metadata_threshold,
                    max_tokens_per_contract,
                    max_recall_rows,
                )?,
            );
        }
        Ok(rows)
    }
}

struct BatchSnapshotFailsFeatureStore {
    probe: Arc<BatchPipelineProbe>,
}

impl FeatureStoreReader for BatchSnapshotFailsFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        self.probe.snapshot_calls.fetch_add(1, Ordering::SeqCst);
        Ok(DatabaseSnapshot::default())
    }

    fn load_snapshots(
        &self,
        _chain: &str,
        _seeds: &[(String, Vec<SeedNft>)],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        self.probe
            .snapshot_batch_calls
            .fetch_add(1, Ordering::SeqCst);
        Err(AppError::InvalidData("batched snapshot failed".into()))
    }
}

struct SlowFirstSeedContextApi {
    slow_context_finished: Arc<AtomicBool>,
}

#[async_trait]
impl AnalyzeApi for SlowFirstSeedContextApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        if contract_address == "0xseed1" {
            tokio::time::sleep(Duration::from_millis(250)).await;
            self.slow_context_finished.store(true, Ordering::SeqCst);
        }
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 1,
            deployed_block_time: 0,
            name: format!("Seed {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
            ..ContractMetadata::default()
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: format!("Token {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
            ..SeedNft::default()
        }])
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        _contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord::default())
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(0.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(vec![])
    }
}

struct SnapshotBeforeSlowContextFeatureStore {
    slow_context_finished: Arc<AtomicBool>,
    snapshot_started_before_slow_context_finished: Arc<AtomicBool>,
}

impl SnapshotBeforeSlowContextFeatureStore {
    fn record_snapshot_start(&self) {
        if !self.slow_context_finished.load(Ordering::SeqCst) {
            self.snapshot_started_before_slow_context_finished
                .store(true, Ordering::SeqCst);
        }
    }
}

impl FeatureStoreReader for SnapshotBeforeSlowContextFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        self.record_snapshot_start();
        Ok(DatabaseSnapshot::default())
    }

    fn load_snapshots(
        &self,
        _chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        self.record_snapshot_start();
        Ok(seeds
            .iter()
            .map(|(seed_address, _)| (seed_address.clone(), DatabaseSnapshot::default()))
            .collect())
    }
}

struct SeedContextBackpressureApi {
    completed_contexts: Arc<AtomicUsize>,
}

#[async_trait]
impl AnalyzeApi for SeedContextBackpressureApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 1,
            deployed_block_time: 0,
            name: format!("Seed {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
            ..ContractMetadata::default()
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: format!("Token {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
            ..SeedNft::default()
        }])
    }

    async fn fetch_license_sample(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _seed_nfts: &[SeedNft],
    ) -> Result<bool, AppError> {
        self.completed_contexts.fetch_add(1, Ordering::SeqCst);
        Ok(false)
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        _contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord::default())
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(0.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(vec![])
    }
}

struct BlockingSnapshotFeatureStore {
    snapshot_started: Arc<AtomicBool>,
    sleep_ms: u64,
}

impl FeatureStoreReader for BlockingSnapshotFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        self.snapshot_started.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(self.sleep_ms));
        Ok(DatabaseSnapshot::default())
    }

    fn load_snapshots(
        &self,
        _chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        self.snapshot_started.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(self.sleep_ms));
        Ok(seeds
            .iter()
            .map(|(seed_address, _)| (seed_address.clone(), DatabaseSnapshot::default()))
            .collect())
    }
}

#[derive(Clone)]
struct StageOrderRecorder {
    events: Arc<Mutex<Vec<String>>>,
}

impl StageOrderRecorder {
    fn push(&self, event: impl Into<String>) {
        self.events.lock().unwrap().push(event.into());
    }
}

struct StageOrderFeatureStore {
    recorder: StageOrderRecorder,
}

impl FeatureStoreReader for StageOrderFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        self.recorder.push("snapshot:start");
        self.recorder.push("snapshot:finish");
        Ok(DatabaseSnapshot::default())
    }
}

struct HeavyCandidateFeatureStore {
    heartbeat: Arc<AtomicUsize>,
    heartbeat_window_open: Arc<AtomicBool>,
    name_count: usize,
}

impl FeatureStoreReader for HeavyCandidateFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        let name_norms = (0..self.name_count)
            .map(|index| {
                format!(
                    "seed seed1 generated candidate name {index:06} {}",
                    "x".repeat(80)
                )
            })
            .collect();
        self.heartbeat.store(0, Ordering::SeqCst);
        self.heartbeat_window_open.store(true, Ordering::SeqCst);
        Ok(DatabaseSnapshot {
            duplicate_contract_rows: vec![ContractDuplicateRecord {
                contract_address: "0xcandidateheavy".into(),
                representative: DatabaseNftRecord {
                    contract_address: "0xcandidateheavy".into(),
                    token_id: "1".into(),
                    name: "Seed seed1 generated candidate".into(),
                    ..DatabaseNftRecord::default()
                },
                name_norms,
                ..ContractDuplicateRecord::default()
            }],
            ..DatabaseSnapshot::default()
        })
    }
}

struct HeartbeatRecordingBatchApi {
    heartbeat: Arc<AtomicUsize>,
    heartbeat_window_open: Arc<AtomicBool>,
    observed_heartbeat_at_post_candidate_io: Arc<AtomicUsize>,
}

#[async_trait]
impl AnalyzeApi for HeartbeatRecordingBatchApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 1,
            name: format!("Seed {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
            ..ContractMetadata::default()
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: format!("Seed {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
            ..SeedNft::default()
        }])
    }

    async fn fetch_seed_collection_slug(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        _seed_contract_address: &str,
    ) -> Result<Option<String>, AppError> {
        self.observed_heartbeat_at_post_candidate_io
            .store(self.heartbeat.load(Ordering::SeqCst), Ordering::SeqCst);
        self.heartbeat_window_open.store(false, Ordering::SeqCst);
        Ok(None)
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        _contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord::default())
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(0.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(vec![])
    }
}

struct InstrumentedBatchApi {
    probe: Arc<BatchPipelineProbe>,
    transfer_sleep_ms: u64,
    emit_native_sale: bool,
}

struct PipelineOverlapBatchApi {
    inner: InstrumentedBatchApi,
}

#[async_trait]
impl AnalyzeApi for PipelineOverlapBatchApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        if contract_address == "0xseed2" {
            let mut waited = 0;
            while self
                .inner
                .probe
                .candidate_transfer_current
                .load(Ordering::SeqCst)
                == 0
                && waited < 1000
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
                waited += 1;
            }
        }
        AnalyzeApi::fetch_contract_metadata(
            &self.inner,
            chain,
            alchemy_api_key,
            alchemy_network,
            opensea_api_key,
            contract_address,
        )
        .await
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        AnalyzeApi::fetch_seed_contract_nfts(
            &self.inner,
            chain,
            alchemy_api_key,
            alchemy_network,
            contract_address,
        )
        .await
    }

    async fn fetch_contract_transfers(
        &self,
        chain: &str,
        etherscan_api_key: &str,
        alchemy_network: Option<&str>,
        alchemy_api_key: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        AnalyzeApi::fetch_contract_transfers(
            &self.inner,
            chain,
            etherscan_api_key,
            alchemy_network,
            alchemy_api_key,
            contract_address,
            token_type,
        )
        .await
    }

    async fn candidate_currently_holds_seed_nft(
        &self,
        request: CandidateSeedHolderRequest<'_>,
    ) -> Result<Option<bool>, AppError> {
        AnalyzeApi::candidate_currently_holds_seed_nft(&self.inner, request).await
    }

    async fn fetch_contract_nfts(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        etherscan_api_key: &str,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        AnalyzeApi::fetch_contract_nfts(
            &self.inner,
            chain,
            alchemy_api_key,
            alchemy_network,
            etherscan_api_key,
            opensea_api_key,
            contract_address,
        )
        .await
    }

    async fn fetch_contract_owners(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        AnalyzeApi::fetch_contract_owners(
            &self.inner,
            chain,
            alchemy_api_key,
            alchemy_network,
            contract_address,
        )
        .await
    }

    async fn fetch_contract_sales(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
        opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        AnalyzeApi::fetch_contract_sales(
            &self.inner,
            chain,
            alchemy_api_key,
            alchemy_network,
            contract_address,
            opensea_api_key,
        )
        .await
    }

    async fn fetch_transaction_receipt(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        AnalyzeApi::fetch_transaction_receipt(
            &self.inner,
            alchemy_api_key,
            alchemy_network,
            tx_hash,
        )
        .await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        AnalyzeApi::fetch_transaction_receipts_for_block(
            &self.inner,
            alchemy_api_key,
            alchemy_network,
            block_number,
        )
        .await
    }

    async fn fetch_eth_balance(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        AnalyzeApi::fetch_eth_balance(
            &self.inner,
            alchemy_api_key,
            alchemy_network,
            address,
            block_number,
        )
        .await
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        AnalyzeApi::fetch_same_block_eth_transfers_for_address(
            &self.inner,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }
}

#[async_trait]
impl AnalyzeApi for InstrumentedBatchApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        let is_seed_contract = contract_address.starts_with("0xseed");
        if is_seed_contract {
            self.probe.metadata_calls.fetch_add(1, Ordering::SeqCst);
            let current = self.probe.metadata_current.fetch_add(1, Ordering::SeqCst) + 1;
            BatchPipelineProbe::record_max(&self.probe.metadata_max_seen, current);
        } else {
            self.probe
                .candidate_metadata_calls
                .fetch_add(1, Ordering::SeqCst);
            let current = self
                .probe
                .candidate_metadata_current
                .fetch_add(1, Ordering::SeqCst)
                + 1;
            BatchPipelineProbe::record_max(&self.probe.candidate_metadata_max_seen, current);
            if self.transfer_sleep_ms > 0 && !self.emit_native_sale {
                let mut waited = 0;
                while self.probe.candidate_metadata_calls.load(Ordering::SeqCst) < 2 && waited < 100
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    waited += 1;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        if is_seed_contract {
            self.probe.metadata_current.fetch_sub(1, Ordering::SeqCst);
        } else {
            self.probe
                .candidate_metadata_current
                .fetch_sub(1, Ordering::SeqCst);
        }
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 1,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: format!("Seed {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: format!("Seed {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
            token_uri: format!("ipfs://{contract_address}/1"),
            image_uri: String::new(),
            metadata_json: String::new(),
        }])
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        let current = self.probe.transfer_current.fetch_add(1, Ordering::SeqCst) + 1;
        BatchPipelineProbe::record_max(&self.probe.transfer_max_seen, current);
        let is_seed_contract = contract_address.starts_with("0xseed");
        if !is_seed_contract {
            let current = self
                .probe
                .candidate_transfer_current
                .fetch_add(1, Ordering::SeqCst)
                + 1;
            BatchPipelineProbe::record_max(&self.probe.candidate_transfer_max_seen, current);
        }
        if self.transfer_sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.transfer_sleep_ms)).await;
        }
        if !is_seed_contract {
            self.probe
                .candidate_transfer_current
                .fetch_sub(1, Ordering::SeqCst);
        }
        self.probe.transfer_current.fetch_sub(1, Ordering::SeqCst);
        Ok(vec![])
    }

    async fn candidate_currently_holds_seed_nft(
        &self,
        request: CandidateSeedHolderRequest<'_>,
    ) -> Result<Option<bool>, AppError> {
        if !self.emit_native_sale && request.candidate_contract_address.contains("seed2") {
            let mut waited = 0;
            while self.probe.holder_current.load(Ordering::SeqCst) == 0 && waited < 100 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                waited += 1;
            }
        }
        let current = self.probe.holder_current.fetch_add(1, Ordering::SeqCst) + 1;
        if self.emit_native_sale
            && request.candidate_contract_address.contains("seed2")
            && !self.probe.obsolete_metric_finished.load(Ordering::SeqCst)
        {
            self.probe
                .holder_started_before_obsolete_metric_finished
                .store(true, Ordering::SeqCst);
        }
        BatchPipelineProbe::record_max(&self.probe.holder_max_seen, current);
        if self.transfer_sleep_ms > 0 && !self.emit_native_sale {
            tokio::time::sleep(Duration::from_millis(self.transfer_sleep_ms)).await;
        }
        self.probe.holder_current.fetch_sub(1, Ordering::SeqCst);
        Ok(Some(false))
    }

    async fn fetch_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _etherscan_api_key: &str,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        if self.emit_native_sale
            && contract_address.contains("seed2")
            && self.probe.obsolete_metric_active.load(Ordering::SeqCst)
        {
            self.probe
                .expansion_observed_obsolete_metric_active
                .store(true, Ordering::SeqCst);
        }
        if !self.emit_native_sale && contract_address.contains("seed2") {
            let mut waited = 0;
            while self.probe.expansion_current.load(Ordering::SeqCst) == 0 && waited < 100 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                waited += 1;
            }
        }
        let current = self.probe.expansion_current.fetch_add(1, Ordering::SeqCst) + 1;
        BatchPipelineProbe::record_max(&self.probe.expansion_max_seen, current);
        if self.transfer_sleep_ms > 0 && !self.emit_native_sale {
            tokio::time::sleep(Duration::from_millis(self.transfer_sleep_ms)).await;
        }
        self.probe.expansion_current.fetch_sub(1, Ordering::SeqCst);
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: "Expanded #1".into(),
            symbol: "EXP".into(),
            token_uri: String::new(),
            image_uri: String::new(),
            metadata_json: String::new(),
        }])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        if self.emit_native_sale && _contract_address.contains("seed1") {
            return Ok(vec![NftSaleRecord {
                contract_address: _contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xsale-seed1".into(),
                block_number: 2,
                log_index: 0,
                bundle_index: 0,
                buyer_address: "0xvictim".into(),
                seller_address: "0xminter".into(),
                marketplace: "opensea".into(),
                taker: "buyer".into(),
                payment_token_symbol: "ETH".into(),
                payment_token_address: "0x0000000000000000000000000000000000000000".into(),
                price_eth: Some(1.0),
                price_usd: Some(1.0),
                seller_fee_eth: 0.0,
                seller_fee_usd: 0.0,
                protocol_fee_eth: 0.0,
                protocol_fee_usd: 0.0,
                royalty_fee_eth: 0.0,
                royalty_fee_usd: 0.0,
                royalty_recipient_address: String::new(),
                source: "opensea".into(),
                is_native_eth: true,
            }]);
        }
        Ok(vec![])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        if self.emit_native_sale {
            self.probe
                .obsolete_metric_receipt_calls
                .fetch_add(1, Ordering::SeqCst);
            self.probe
                .obsolete_metric_active
                .store(true, Ordering::SeqCst);
            let mut waited = 0;
            while !self
                .probe
                .expansion_observed_obsolete_metric_active
                .load(Ordering::SeqCst)
                && waited < 1000
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
                waited += 1;
            }
            self.probe
                .obsolete_metric_active
                .store(false, Ordering::SeqCst);
            self.probe
                .obsolete_metric_finished
                .store(true, Ordering::SeqCst);
        }
        Ok(TransactionReceiptRecord::default())
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
    ) -> Result<std::collections::BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(std::collections::BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(0.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(vec![])
    }
}

fn cached_single_report(
    seed_contract: SeedContractPayload,
    infringing_tokens: Vec<InfringingTokenRecord>,
    malicious_addresses: Vec<MaliciousAddressPayload>,
    honest_addresses: Vec<HonestAddressPayload>,
    secondary_sale_victim_addresses: Vec<SecondarySaleVictimAddressPayload>,
    address_signals: BTreeMap<String, AddressSignalPayload>,
) -> SingleReportPayload {
    let address_attributions = honest_addresses
        .iter()
        .map(|item| AddressAttributionPayload {
            contract_address: item.contract_address.clone(),
            address: item.address.clone(),
            observed_roles: vec!["neutral_holder".into()],
            attribution_label: "neutral_participant".into(),
            neutral_score: 1.0,
            confidence: "test".into(),
            ..AddressAttributionPayload::default()
        })
        .collect();
    let victim_acquisition_addresses = secondary_sale_victim_addresses
        .iter()
        .map(|item| VictimAcquisitionAddressPayload {
            address: item.address.clone(),
            is_stuck: item.is_stuck,
            ..VictimAcquisitionAddressPayload::default()
        })
        .collect();

    SingleReportPayload {
        seed_contract,
        infringing_tokens,
        malicious_addresses,
        honest_addresses,
        address_attributions,
        secondary_sale_victim_addresses,
        victim_acquisition_addresses,
        address_signals,
        ..SingleReportPayload::default()
    }
}

struct FakeBatchApi;

#[async_trait]
impl AnalyzeApi for FakeBatchApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 1,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: format!("Seed {}", &contract_address[2..]),
            symbol: "SEED".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: format!("Token {} #1", &contract_address[2..]),
            symbol: "SEED".into(),
            ..SeedNft::default()
        }])
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        _contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord::default())
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
    ) -> Result<std::collections::BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(std::collections::BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(0.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(vec![])
    }
}

#[test]
fn batch_serializes_paper_stats_index_shape() {
    let payload = BatchSummaryPayload::default();

    let serialized = serde_json::to_value(&payload).unwrap();
    let object = serialized.as_object().unwrap();
    let keys: BTreeSet<_> = object.keys().map(String::as_str).collect();

    assert_eq!(
        keys,
        BTreeSet::from(["schema_version", "report_type", "paper_stats"])
    );
    assert_eq!(serialized["schema_version"], 2);
    assert_eq!(serialized["report_type"], "batch_summary");
    assert!(!object.contains_key("seed_reports"));
    assert!(!object.contains_key("batch_summary"));
    let paper_stats = serialized["paper_stats"].as_object().unwrap();
    let paper_stats_keys: BTreeSet<_> = paper_stats.keys().map(String::as_str).collect();
    assert_eq!(
        paper_stats_keys,
        BTreeSet::from([
            "duplicate_scale",
            "address_classification",
            "malicious_behavior_summary",
            "wash_cycle_size_distribution",
            "attacker_cost",
            "honest_loss",
            "data_quality",
        ])
    );
    assert!(!paper_stats.contains_key("contract_behavior_stats"));
    assert!(!paper_stats.contains_key("attacker_cost_details"));
    assert!(!paper_stats.contains_key("malicious_addresses"));
}

#[test]
fn batch_markdown_preserves_reference_summary_and_output_index_lines() {
    let payload = BatchSummaryPayload {
        paper_stats: PaperStatsPayload {
            duplicate_scale: vec![PaperDuplicateScaleRowPayload {
                category: "token_uri".into(),
                duplicate_nft_count: 4,
                duplicate_nft_ratio: Some(0.4),
                duplicate_nft_ratio_numerator: 4,
                duplicate_nft_ratio_denominator: 10,
                duplicate_contract_count: 2,
                duplicate_contract_ratio: Some(0.5),
                duplicate_contract_ratio_numerator: 2,
                duplicate_contract_ratio_denominator: 4,
            }],
            ..PaperStatsPayload::default()
        },
        ..BatchSummaryPayload::default()
    };

    let markdown = render_batch_human_readable_report(&payload);

    assert!(markdown.contains("# NFT 论文统计汇总报告"));
    assert!(markdown.contains("## 重复规模"));
    assert!(markdown.contains("| token_uri | 4 | 40.00% (4/10) | 2 | 50.00% (2/4) |"));
    assert!(markdown.contains("## 地址分类"));
    assert!(markdown.contains("## 攻击者成本"));
    assert!(markdown.contains("## 诚实买家损失"));
    assert!(markdown.contains("## 数据质量"));
    assert!(!markdown.contains("# Top NFT 合约批量分析总报告"));
    assert!(!markdown.contains("## Seed 报告索引"));
    assert!(!markdown.contains("- 检测到开放许可的 seed 数"));
}

#[tokio::test]
async fn batch_ignores_cached_legacy_outputs_in_output_directory() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let cached_report = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Cached Seed".into(),
            ..SeedContractPayload::default()
        },
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    std::fs::write(
        dir.path().join("top_contract_analysis__cached.json"),
        serde_json::to_string(&cached_report).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached.md"),
        "# cached\n",
    )
    .unwrap();
    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        summary
            .paper_stats
            .address_classification
            .malicious_address_count,
        0
    );
    assert!(dir
        .path()
        .join("top_contract_analysis__seed_seed1.json")
        .exists());
    assert!(dir
        .path()
        .join("top_contract_analysis__seed_seed2.json")
        .exists());
}

#[tokio::test]
async fn batch_ignores_cached_legacy_outputs_without_paper_stats() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n").unwrap();
    let cached_report = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Old Cached Seed".into(),
            ..SeedContractPayload::default()
        },
        Vec::new(),
        vec![MaliciousAddressPayload {
            address: "0xoperator".into(),
            wash_cycle_count: 1,
            ..MaliciousAddressPayload::default()
        }],
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    std::fs::write(
        dir.path().join("top_contract_analysis__cached.json"),
        serde_json::to_string(&cached_report).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached.md"),
        "# cached\n",
    )
    .unwrap();
    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        summary
            .paper_stats
            .address_classification
            .malicious_address_count,
        0
    );
    assert!(dir
        .path()
        .join("top_contract_analysis__seed_seed1.json")
        .exists());
}

#[tokio::test]
async fn batch_uses_contract_level_seed_name_for_snapshot_recall() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let feature_store = Arc::new(CapturingBatchFeatureStore::default());
    let captured_seed_names = feature_store.captured_seed_names.clone();
    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        captured_seed_names.lock().unwrap().clone(),
        BTreeMap::from([
            ("0xseed1".to_string(), vec!["Seed seed1".to_string()]),
            ("0xseed2".to_string(), vec!["Seed seed2".to_string()]),
        ])
    );
}

#[tokio::test]
async fn batch_repeat_infringing_count_does_not_recompute_cross_seed_minter_contracts() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();

    let cached_one = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Seed One".into(),
            ..SeedContractPayload::default()
        },
        vec![InfringingTokenRecord {
            contract_address: "0xc1".into(),
            token_id: "1".into(),
            minter_address: "0xcrossseed".into(),
            ..InfringingTokenRecord::default()
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_one.json"),
        serde_json::to_string(&cached_one).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_one.md"),
        "# cached one\n",
    )
    .unwrap();

    let cached_two = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed2".into(),
            name: "Seed Two".into(),
            ..SeedContractPayload::default()
        },
        vec![InfringingTokenRecord {
            contract_address: "0xc2".into(),
            token_id: "2".into(),
            minter_address: "0xcrossseed".into(),
            ..InfringingTokenRecord::default()
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_two.json"),
        serde_json::to_string(&cached_two).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_two.md"),
        "# cached two\n",
    )
    .unwrap();

    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        summary
            .paper_stats
            .address_classification
            .repeat_infringing_malicious_address_count,
        0
    );
}

#[tokio::test]
async fn batch_ignores_cached_repeat_infringing_addresses() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();

    let cached_one = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Seed One".into(),
            ..SeedContractPayload::default()
        },
        vec![
            InfringingTokenRecord {
                contract_address: "0xc1".into(),
                token_id: "1".into(),
                minter_address: "0xrepeat".into(),
                ..InfringingTokenRecord::default()
            },
            InfringingTokenRecord {
                contract_address: "0xc2".into(),
                token_id: "2".into(),
                minter_address: "0xrepeat".into(),
                ..InfringingTokenRecord::default()
            },
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_one.json"),
        serde_json::to_string(&cached_one).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_one.md"),
        "# cached one\n",
    )
    .unwrap();

    let cached_two = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed2".into(),
            name: "Seed Two".into(),
            ..SeedContractPayload::default()
        },
        vec![
            InfringingTokenRecord {
                contract_address: "0xc3".into(),
                token_id: "3".into(),
                minter_address: "0xrepeat".into(),
                ..InfringingTokenRecord::default()
            },
            InfringingTokenRecord {
                contract_address: "0xc4".into(),
                token_id: "4".into(),
                minter_address: "0xrepeat".into(),
                ..InfringingTokenRecord::default()
            },
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_two.json"),
        serde_json::to_string(&cached_two).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_two.md"),
        "# cached two\n",
    )
    .unwrap();

    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        summary
            .paper_stats
            .address_classification
            .repeat_infringing_malicious_address_count,
        0
    );
}

#[tokio::test]
async fn batch_ignores_cached_seed_summary_and_global_metrics_from_full_payloads() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();

    let mut cached_one = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Seed One".into(),
            ..SeedContractPayload::default()
        },
        vec![
            InfringingTokenRecord {
                contract_address: "0xc1".into(),
                token_id: "1".into(),
                minter_address: "0xrepeat".into(),
                ..InfringingTokenRecord::default()
            },
            InfringingTokenRecord {
                contract_address: "0xc2".into(),
                token_id: "2".into(),
                minter_address: "0xrepeat".into(),
                ..InfringingTokenRecord::default()
            },
        ],
        vec![
            MaliciousAddressPayload {
                address: "0xm1".into(),
                operator_level: 1,
                operator_level_label: "weak_behavioral_operator".into(),
                ..MaliciousAddressPayload::default()
            },
            MaliciousAddressPayload {
                address: "0xm2".into(),
                operator_level: 1,
                operator_level_label: "weak_behavioral_operator".into(),
                ..MaliciousAddressPayload::default()
            },
        ],
        vec![HonestAddressPayload {
            address: "0xh1".into(),
            is_corrupted_address: true,
            deployment_to_neutral_holder_seconds_samples: vec![5, 15],
            ..HonestAddressPayload::default()
        }],
        vec![
            SecondarySaleVictimAddressPayload {
                address: "0xv1".into(),
                buy_amount_eth: 7.0,
                buy_amount_usd: 7.0,
                last_buy_amount_eth: Some(2.0),
                last_buy_amount_usd: Some(2.0),
                is_stuck: true,
                ..SecondarySaleVictimAddressPayload::default()
            },
            SecondarySaleVictimAddressPayload {
                address: "0xv2".into(),
                buy_amount_eth: 3.0,
                buy_amount_usd: 3.0,
                last_buy_amount_eth: Some(4.0),
                last_buy_amount_usd: Some(4.0),
                is_stuck: false,
                ..SecondarySaleVictimAddressPayload::default()
            },
            SecondarySaleVictimAddressPayload {
                address: "0xm1".into(),
                buy_amount_eth: 1_000.0,
                buy_amount_usd: 1_000.0,
                last_buy_amount_eth: Some(1_000.0),
                last_buy_amount_usd: Some(1_000.0),
                is_stuck: true,
                ..SecondarySaleVictimAddressPayload::default()
            },
        ],
        BTreeMap::from([
            (
                "0xa1".into(),
                AddressSignalPayload {
                    first_transfer_delay_seconds: 4,
                    ..AddressSignalPayload::default()
                },
            ),
            (
                "0xa2".into(),
                AddressSignalPayload {
                    first_transfer_delay_seconds: 10,
                    ..AddressSignalPayload::default()
                },
            ),
        ]),
    );
    cached_one.value_flow_edges = vec![ValueFlowEdgePayload {
        from_address: "0xm1".into(),
        value_eth: Some(1_000.0),
        value_usd: Some(1_000.0),
        channel: "sale_payment".into(),
        ..ValueFlowEdgePayload::default()
    }];
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_one.json"),
        serde_json::to_string(&cached_one).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_one.md"),
        "# cached one\n",
    )
    .unwrap();

    let mut cached_two = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed2".into(),
            name: "Seed Two".into(),
            ..SeedContractPayload::default()
        },
        vec![InfringingTokenRecord {
            contract_address: "0xc3".into(),
            token_id: "3".into(),
            minter_address: "0xrepeat".into(),
            ..InfringingTokenRecord::default()
        }],
        vec![
            MaliciousAddressPayload {
                address: "0xm2".into(),
                operator_level: 2,
                operator_level_label: "likely_behavioral_operator".into(),
                ..MaliciousAddressPayload::default()
            },
            MaliciousAddressPayload {
                address: "0xm3".into(),
                operator_level: 3,
                operator_level_label: "strong_value_control_operator".into(),
                ..MaliciousAddressPayload::default()
            },
        ],
        vec![
            HonestAddressPayload {
                address: "0xh1".into(),
                deployment_to_neutral_holder_seconds_samples: vec![20],
                ..HonestAddressPayload::default()
            },
            HonestAddressPayload {
                address: "0xh2".into(),
                ..HonestAddressPayload::default()
            },
        ],
        vec![
            SecondarySaleVictimAddressPayload {
                address: "0xv2".into(),
                buy_amount_eth: 1.0,
                buy_amount_usd: 1.0,
                last_buy_amount_eth: Some(1.0),
                last_buy_amount_usd: Some(1.0),
                is_stuck: true,
                ..SecondarySaleVictimAddressPayload::default()
            },
            SecondarySaleVictimAddressPayload {
                address: "0xv3".into(),
                buy_amount_eth: 3.0,
                buy_amount_usd: 3.0,
                ..SecondarySaleVictimAddressPayload::default()
            },
            SecondarySaleVictimAddressPayload {
                address: "0xv4".into(),
                buy_amount_eth: 1.0,
                buy_amount_usd: 1.0,
                ..SecondarySaleVictimAddressPayload::default()
            },
        ],
        BTreeMap::from([(
            "0xa3".into(),
            AddressSignalPayload {
                first_transfer_delay_seconds: 20,
                ..AddressSignalPayload::default()
            },
        )]),
    );
    cached_two.value_flow_edges = vec![ValueFlowEdgePayload {
        from_address: "0xm2".into(),
        value_eth: Some(50.0),
        value_usd: Some(50.0),
        channel: "sale_payment".into(),
        ..ValueFlowEdgePayload::default()
    }];
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_two.json"),
        serde_json::to_string(&cached_two).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_two.md"),
        "# cached two\n",
    )
    .unwrap();

    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        summary
            .paper_stats
            .address_classification
            .malicious_address_count,
        0
    );
    assert_eq!(
        summary
            .paper_stats
            .address_classification
            .malicious_address_count,
        0
    );
    assert!(dir
        .path()
        .join("top_contract_analysis__seed_seed1.json")
        .exists());
    assert!(dir
        .path()
        .join("top_contract_analysis__seed_seed2.json")
        .exists());
}

#[tokio::test]
async fn batch_writes_summary_files_with_existing_names() {
    let payload = BatchSummaryPayload {
        paper_stats: PaperStatsPayload::default(),
        ..BatchSummaryPayload::default()
    };
    let dir = tempdir().unwrap();

    let (json_path, md_path) = write_batch_paper_stats_outputs(&payload, dir.path()).unwrap();

    assert_eq!(
        json_path.file_name().unwrap().to_string_lossy(),
        "top_contract_analysis__summary.json"
    );
    assert_eq!(
        md_path.file_name().unwrap().to_string_lossy(),
        "top_contract_analysis__summary.md"
    );
}

struct SlowBatchApi {
    current: AtomicUsize,
    max_seen: AtomicUsize,
}

impl SlowBatchApi {
    fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            max_seen: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl AnalyzeApi for SlowBatchApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let seen = self.max_seen.load(Ordering::SeqCst);
            if current <= seen {
                break;
            }
            if self
                .max_seen
                .compare_exchange(seen, current, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.current.fetch_sub(1, Ordering::SeqCst);
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 1,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: format!("Seed {}", &contract_address[2..]),
            symbol: "SEED".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: format!("Seed {}", &contract_address[2..]),
            symbol: "SEED".into(),
            ..SeedNft::default()
        }])
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        _contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord::default())
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
    ) -> Result<std::collections::BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(std::collections::BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(0.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(vec![])
    }
}

struct OneSeedFailsContextApi;

#[async_trait]
impl AnalyzeApi for OneSeedFailsContextApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        if contract_address == "0xseed1" {
            return Err(AppError::InvalidData("seed context failed".into()));
        }
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            contract_deployer: "0xcreator".into(),
            deployed_block_number: 1,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: format!("Seed {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: format!("Seed {}", contract_address.trim_start_matches("0x")),
            symbol: "SEED".into(),
            ..SeedNft::default()
        }])
    }

    async fn fetch_contract_transfers(
        &self,
        _chain: &str,
        _etherscan_api_key: &str,
        _alchemy_network: Option<&str>,
        _alchemy_api_key: &str,
        _contract_address: &str,
        _token_type: &str,
    ) -> Result<Vec<TransferRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord::default())
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
    ) -> Result<std::collections::BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(std::collections::BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(0.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(vec![])
    }
}

#[derive(Default)]
struct RecordingSeedProgressReporter {
    events: Mutex<Vec<String>>,
}

#[async_trait]
impl SeedProgressReporter for RecordingSeedProgressReporter {
    async fn on_seed_stage(&self, stage: &str) {
        self.events.lock().unwrap().push(format!("stage:{stage}"));
    }

    async fn on_seed_completed(&self) {
        self.events.lock().unwrap().push("completed".into());
    }
}

#[derive(Default)]
struct RecordingBatchProgressReporter {
    cached: Mutex<Vec<String>>,
    started: Mutex<Vec<String>>,
    finished: Mutex<Vec<String>>,
    failed: Mutex<Vec<String>>,
    seed_events: Mutex<Vec<String>>,
}

impl BatchProgressReporter for RecordingBatchProgressReporter {
    fn on_seed_cached(&self, seed_address: &str) {
        self.cached.lock().unwrap().push(seed_address.to_string());
    }

    fn on_seed_started(&self, seed_address: &str) {
        self.started.lock().unwrap().push(seed_address.to_string());
    }

    fn on_seed_finished(&self, seed_address: &str) {
        self.finished.lock().unwrap().push(seed_address.to_string());
    }

    fn on_seed_failed(&self, seed_address: &str, _error: &str) {
        self.failed.lock().unwrap().push(seed_address.to_string());
    }

    fn create_seed_reporter(&self, seed_address: &str) -> Arc<dyn SeedProgressReporter> {
        self.seed_events
            .lock()
            .unwrap()
            .push(format!("create:{seed_address}"));
        Arc::new(RecordingSeedProgressReporter::default())
    }
}

struct StageOrderSeedProgressReporter {
    recorder: StageOrderRecorder,
}

#[async_trait]
impl SeedProgressReporter for StageOrderSeedProgressReporter {
    async fn on_seed_stage(&self, stage: &str) {
        self.recorder.push(format!("stage:{stage}"));
    }
}

struct StageOrderBatchProgressReporter {
    recorder: StageOrderRecorder,
}

impl BatchProgressReporter for StageOrderBatchProgressReporter {
    fn on_seed_cached(&self, _seed_address: &str) {}

    fn on_seed_started(&self, _seed_address: &str) {}

    fn on_seed_finished(&self, _seed_address: &str) {}

    fn on_seed_failed(&self, _seed_address: &str, _error: &str) {}

    fn create_seed_reporter(&self, _seed_address: &str) -> Arc<dyn SeedProgressReporter> {
        Arc::new(StageOrderSeedProgressReporter {
            recorder: self.recorder.clone(),
        })
    }
}

#[tokio::test]
async fn batch_uses_seed_network_concurrency_for_uncached_seeds() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n0xseed3\n").unwrap();
    let api = Arc::new(SlowBatchApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(api.max_seen.load(Ordering::SeqCst) >= 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_bounds_in_flight_seed_futures_for_large_inputs() {
    let dir = tempdir().unwrap();
    let seeds = (1..=20)
        .map(|index| format!("0xseed{index}"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(dir.path().join("seeds.txt"), seeds).unwrap();
    let batch_progress = Arc::new(RecordingBatchProgressReporter::default());
    let deps = AnalysisDeps {
        api: Arc::new(SlowBatchApi::new()),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: batch_progress.clone(),
    };
    let request = BatchRequest {
        chain: "ethereum".into(),
        seed_file: dir.path().join("seeds.txt"),
        output_dir: dir.path().to_path_buf(),
        alchemy_api_key: "key".into(),
        seed_network_max_concurrency: 2,
        seed_cpu_max_concurrency: 1,
        matched_contract_max_concurrency: 1,
        ..BatchRequest::default()
    };

    let handle = tokio::spawn(async move { run_batch(request, &deps).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert!(
        batch_progress.seed_events.lock().unwrap().len() <= 4,
        "seed futures should be admitted in bounded batches"
    );
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_continues_successful_seed_after_context_failure() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let batch_progress = Arc::new(RecordingBatchProgressReporter::default());
    let deps = AnalysisDeps {
        api: Arc::new(OneSeedFailsContextApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: batch_progress.clone(),
    };

    let result = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await;

    assert!(result.is_err());
    assert_eq!(
        batch_progress.finished.lock().unwrap().as_slice(),
        ["0xseed2"]
    );
    let wrote_seed2_report = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .any(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.starts_with("top_contract_analysis__seed_seed2") && name.ends_with(".json")
        });
    assert!(wrote_seed2_report);
    assert_eq!(
        batch_progress.failed.lock().unwrap().as_slice(),
        ["0xseed1"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_reuses_existing_single_seed_report_and_analyzes_remaining_seeds() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let mut cached_report = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Cached Seed".into(),
            ..SeedContractPayload::default()
        },
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    cached_report.paper_stats = PaperStatsPayload {
        malicious_addresses: vec!["0xcached".into()],
        ..PaperStatsPayload::default()
    };
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_seed.json"),
        serde_json::to_string(&cached_report).unwrap(),
    )
    .unwrap();
    let batch_progress = Arc::new(RecordingBatchProgressReporter::default());
    let deps = AnalysisDeps {
        api: Arc::new(OneSeedFailsContextApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: batch_progress.clone(),
    };

    let summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        batch_progress.cached.lock().unwrap().as_slice(),
        ["0xseed1"]
    );
    assert_eq!(
        batch_progress.started.lock().unwrap().as_slice(),
        ["0xseed2"]
    );
    assert_eq!(
        batch_progress.finished.lock().unwrap().as_slice(),
        ["0xseed2"]
    );
    assert!(batch_progress.failed.lock().unwrap().is_empty());
    assert_eq!(
        summary
            .paper_stats
            .address_classification
            .malicious_address_count,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_reusing_written_single_seed_reports_matches_direct_batch_summary() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let direct_probe = Arc::new(BatchPipelineProbe::default());
    let direct_deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: direct_probe.clone(),
            transfer_sleep_ms: 0,
            emit_native_sale: false,
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: direct_probe,
            sleep_ms: 0,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let request = BatchRequest {
        chain: "ethereum".into(),
        seed_file: dir.path().join("seeds.txt"),
        output_dir: dir.path().to_path_buf(),
        alchemy_api_key: "key".into(),
        seed_network_max_concurrency: 2,
        seed_cpu_max_concurrency: 1,
        matched_contract_max_concurrency: 1,
        ..BatchRequest::default()
    };
    let direct_summary = run_batch(request.clone(), &direct_deps).await.unwrap();
    assert!(
        direct_summary
            .paper_stats
            .data_quality
            .candidate_contract_count
            > 0,
        "direct batch fixture should exercise non-empty paper_stats"
    );

    let cached_progress = Arc::new(RecordingBatchProgressReporter::default());
    let cached_deps = AnalysisDeps {
        api: Arc::new(OneSeedFailsContextApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: cached_progress.clone(),
    };

    let cached_summary = run_batch(request, &cached_deps).await.unwrap();

    assert_eq!(cached_summary, direct_summary);
    assert_eq!(
        cached_progress.cached.lock().unwrap().as_slice(),
        ["0xseed1", "0xseed2"]
    );
    assert!(cached_progress.started.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_reuses_mixed_case_cached_single_seed_report_without_dropping_summary_entry() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n").unwrap();
    let mut cached_report = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xSeed1".into(),
            name: "Cached Seed".into(),
            ..SeedContractPayload::default()
        },
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    cached_report.paper_stats = PaperStatsPayload {
        malicious_addresses: vec!["0xcached".into()],
        ..PaperStatsPayload::default()
    };
    std::fs::write(
        dir.path().join("top_contract_analysis__cached_seed.json"),
        serde_json::to_string(&cached_report).unwrap(),
    )
    .unwrap();
    let batch_progress = Arc::new(RecordingBatchProgressReporter::default());
    let deps = AnalysisDeps {
        api: Arc::new(OneSeedFailsContextApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: batch_progress.clone(),
    };

    let summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        batch_progress.cached.lock().unwrap().as_slice(),
        ["0xseed1"]
    );
    assert!(batch_progress.started.lock().unwrap().is_empty());
    assert_eq!(
        summary
            .paper_stats
            .address_classification
            .malicious_address_count,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_loads_seed_snapshots_in_batches() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: probe.clone(),
            transfer_sleep_ms: 0,
            emit_native_sale: false,
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 80,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 1,
            seed_cpu_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(probe.metadata_calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.snapshot_batch_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.snapshot_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_fetches_ready_seed_contexts_before_batched_snapshot() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: probe.clone(),
            transfer_sleep_ms: 0,
            emit_native_sale: false,
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 80,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 1,
            seed_cpu_max_concurrency: 1,
            matched_contract_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(probe.metadata_calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.snapshot_batch_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_flushes_ready_seed_context_before_slow_context_finishes() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let slow_context_finished = Arc::new(AtomicBool::new(false));
    let snapshot_started_before_slow_context_finished = Arc::new(AtomicBool::new(false));
    let deps = AnalysisDeps {
        api: Arc::new(SlowFirstSeedContextApi {
            slow_context_finished: slow_context_finished.clone(),
        }),
        feature_store: Arc::new(SnapshotBeforeSlowContextFeatureStore {
            slow_context_finished,
            snapshot_started_before_slow_context_finished:
                snapshot_started_before_slow_context_finished.clone(),
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            seed_cpu_max_concurrency: 1,
            matched_contract_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(
        snapshot_started_before_slow_context_finished.load(Ordering::SeqCst),
        "ready seed context should enter snapshot loading without waiting for a slower seed context"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_backpressures_seed_context_fetches_while_snapshot_is_blocked() {
    let dir = tempdir().unwrap();
    let seed_count = 12;
    let seed_network_max_concurrency = 1;
    let seed_cpu_max_concurrency = 1;
    let matched_contract_max_concurrency = 1;
    let seed_pipeline_max_concurrency =
        seed_network_max_concurrency + seed_cpu_max_concurrency + matched_contract_max_concurrency;
    let max_completed_without_unbounded_buffer =
        seed_pipeline_max_concurrency * 2 + seed_network_max_concurrency;
    let seeds = (1..=seed_count)
        .map(|index| format!("0xseed{index}"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(dir.path().join("seeds.txt"), format!("{seeds}\n")).unwrap();
    let completed_contexts = Arc::new(AtomicUsize::new(0));
    let snapshot_started = Arc::new(AtomicBool::new(false));
    let deps = AnalysisDeps {
        api: Arc::new(SeedContextBackpressureApi {
            completed_contexts: completed_contexts.clone(),
        }),
        feature_store: Arc::new(BlockingSnapshotFeatureStore {
            snapshot_started: snapshot_started.clone(),
            sleep_ms: 300,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let request = BatchRequest {
        chain: "ethereum".into(),
        seed_file: dir.path().join("seeds.txt"),
        output_dir: dir.path().to_path_buf(),
        alchemy_api_key: "key".into(),
        seed_network_max_concurrency,
        seed_cpu_max_concurrency,
        matched_contract_max_concurrency,
        ..BatchRequest::default()
    };
    let handle = tokio::spawn(async move { run_batch(request, &deps).await });

    for _ in 0..100 {
        if snapshot_started.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        snapshot_started.load(Ordering::SeqCst),
        "snapshot processing should start before checking context backpressure"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    let completed_while_snapshot_blocked = completed_contexts.load(Ordering::SeqCst);
    assert!(
        completed_while_snapshot_blocked <= max_completed_without_unbounded_buffer,
        "seed context fetches should be bounded by the active batch plus bounded queue while snapshot is blocked; completed={completed_while_snapshot_blocked}"
    );

    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn batch_reports_find_duplicate_candidates_after_snapshot_load_finishes() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n").unwrap();
    let recorder = StageOrderRecorder {
        events: Arc::new(Mutex::new(Vec::new())),
    };
    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Arc::new(StageOrderFeatureStore {
            recorder: recorder.clone(),
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(StageOrderBatchProgressReporter {
            recorder: recorder.clone(),
        }),
    };

    run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 1,
            seed_cpu_max_concurrency: 1,
            matched_contract_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    let events = recorder.events.lock().unwrap().clone();
    let snapshot_finish = events
        .iter()
        .position(|event| event == "snapshot:finish")
        .expect("snapshot should finish");
    let find_candidates = events
        .iter()
        .position(|event| event == "stage:find_duplicate_candidates")
        .expect("find_duplicate_candidates stage should be reported");

    assert!(
        snapshot_finish < find_candidates,
        "find_duplicate_candidates should be reported after snapshot load; events={events:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn batch_candidate_scoring_runs_on_blocking_worker() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n").unwrap();
    let heartbeat = Arc::new(AtomicUsize::new(0));
    let heartbeat_window_open = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let observed_heartbeat_at_post_candidate_io = Arc::new(AtomicUsize::new(0));
    let heartbeat_task = tokio::spawn({
        let heartbeat = heartbeat.clone();
        let heartbeat_window_open = heartbeat_window_open.clone();
        let stop = stop.clone();
        async move {
            while !stop.load(Ordering::SeqCst) {
                if heartbeat_window_open.load(Ordering::SeqCst) {
                    heartbeat.fetch_add(1, Ordering::SeqCst);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    });
    let deps = AnalysisDeps {
        api: Arc::new(HeartbeatRecordingBatchApi {
            heartbeat: heartbeat.clone(),
            heartbeat_window_open: heartbeat_window_open.clone(),
            observed_heartbeat_at_post_candidate_io: observed_heartbeat_at_post_candidate_io
                .clone(),
        }),
        feature_store: Arc::new(HeavyCandidateFeatureStore {
            heartbeat,
            heartbeat_window_open,
            name_count: 100_000,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let result = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            name_threshold: 0.0,
            seed_network_max_concurrency: 1,
            seed_cpu_max_concurrency: 1,
            matched_contract_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await;
    stop.store(true, Ordering::SeqCst);
    heartbeat_task.await.unwrap();
    result.unwrap();

    let observed = observed_heartbeat_at_post_candidate_io.load(Ordering::SeqCst);
    assert!(
        observed >= 5,
        "candidate scoring should not block the async runtime worker; observed heartbeat={observed}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_limits_cpu_stage_globally() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n0xseed3\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: probe.clone(),
            transfer_sleep_ms: 0,
            emit_native_sale: false,
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 60,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 3,
            seed_cpu_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(probe.cpu_max_seen.load(Ordering::SeqCst), 1);
    assert_eq!(probe.snapshot_batch_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.snapshot_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_loads_pending_seed_snapshots_in_one_cpu_limited_batch() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: probe.clone(),
            transfer_sleep_ms: 0,
            emit_native_sale: false,
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 0,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            seed_cpu_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(probe.snapshot_batch_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.snapshot_calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.cpu_max_seen.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_runs_next_seed_snapshot_while_previous_seed_contract_analysis_is_active() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(PipelineOverlapBatchApi {
            inner: InstrumentedBatchApi {
                probe: probe.clone(),
                transfer_sleep_ms: 250,
                emit_native_sale: false,
            },
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 0,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            seed_cpu_max_concurrency: 1,
            matched_contract_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(
        probe
            .snapshot_started_during_contract_analysis
            .load(Ordering::SeqCst),
        "seed2 snapshot should be allowed to start while seed1 contract analysis is still active"
    );
    assert_eq!(probe.cpu_max_seen.load(Ordering::SeqCst), 1);
    assert_eq!(probe.candidate_transfer_max_seen.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_caps_stage1_and_stage2_backlog_ahead_of_stage3_to_eight() {
    let dir = tempdir().unwrap();
    let seeds = (1..=9)
        .map(|index| format!("0xseed{index}"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(dir.path().join("seeds.txt"), format!("{seeds}\n")).unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: probe.clone(),
            transfer_sleep_ms: 200,
            emit_native_sale: false,
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 0,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };
    let request = BatchRequest {
        chain: "ethereum".into(),
        seed_file: dir.path().join("seeds.txt"),
        output_dir: dir.path().to_path_buf(),
        alchemy_api_key: "key".into(),
        seed_network_max_concurrency: 9,
        seed_cpu_max_concurrency: 9,
        matched_contract_max_concurrency: 1,
        ..BatchRequest::default()
    };

    let handle = tokio::spawn(async move { run_batch(request, &deps).await });
    for _ in 0..1000 {
        if probe.candidate_transfer_current.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        probe.candidate_transfer_current.load(Ordering::SeqCst) > 0,
        "first seed should enter stage3 before checking upstream backlog"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let snapshot_calls = probe.snapshot_calls.load(Ordering::SeqCst);
    assert!(
        snapshot_calls <= 8,
        "stage1/stage2 should not prepare more than 8 seeds ahead of blocked stage3; snapshot_calls={snapshot_calls}"
    );

    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_aborts_started_stage3_tasks_when_later_candidate_plan_batch_fails() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(PipelineOverlapBatchApi {
            inner: InstrumentedBatchApi {
                probe: probe.clone(),
                transfer_sleep_ms: 700,
                emit_native_sale: false,
            },
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 0,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: Some("0xseed2"),
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let result = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            seed_cpu_max_concurrency: 1,
            matched_contract_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await;

    let err = result.expect_err("seed2 snapshot panic should fail the batch");
    assert!(
        err.to_string().contains("candidate CPU task failed"),
        "{err}"
    );
    assert!(
        probe.candidate_transfer_max_seen.load(Ordering::SeqCst) > 0,
        "seed1 stage3 should have started before seed2 candidate-plan failure"
    );
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(
        !dir.path()
            .join("top_contract_analysis__seed_seed1.json")
            .exists(),
        "aborted stage3 task should not keep running and write seed1 output after run_batch returns"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_falls_back_to_per_seed_snapshots_when_batched_snapshot_fails() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let batch_progress = Arc::new(RecordingBatchProgressReporter::default());
    let deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: probe.clone(),
            transfer_sleep_ms: 0,
            emit_native_sale: false,
        }),
        feature_store: Arc::new(BatchSnapshotFailsFeatureStore {
            probe: probe.clone(),
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: batch_progress.clone(),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            seed_cpu_max_concurrency: 2,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(probe.snapshot_batch_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.snapshot_calls.load(Ordering::SeqCst), 2);
    assert!(batch_progress.failed.lock().unwrap().is_empty());
    assert_eq!(batch_progress.finished.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_seed_network_limit_controls_seed_context_metadata_fetches() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n0xseed3\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: probe.clone(),
            transfer_sleep_ms: 0,
            emit_native_sale: false,
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 0,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(probe.metadata_calls.load(Ordering::SeqCst), 3);
    assert_eq!(probe.metadata_max_seen.load(Ordering::SeqCst), 1);
    assert_eq!(probe.candidate_metadata_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_limits_contract_analysis_globally_across_seeds() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: probe.clone(),
            transfer_sleep_ms: 80,
            emit_native_sale: false,
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 80,
            wait_for_transfer_before_seed_two_snapshot: false,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            seed_cpu_max_concurrency: 2,
            matched_contract_max_concurrency: 2,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(probe.candidate_transfer_max_seen.load(Ordering::SeqCst), 2);
    assert!(
        probe.holder_max_seen.load(Ordering::SeqCst) >= 2,
        "expected candidate holder API checks to be scheduled outside the match-contract worker limit"
    );
    assert_eq!(probe.expansion_max_seen.load(Ordering::SeqCst), 2);
    assert_eq!(probe.metadata_calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.candidate_metadata_calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.candidate_metadata_max_seen.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_does_not_run_obsolete_receipt_metrics_across_seeds() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let probe = Arc::new(BatchPipelineProbe::default());
    let deps = AnalysisDeps {
        api: Arc::new(InstrumentedBatchApi {
            probe: probe.clone(),
            transfer_sleep_ms: 300,
            emit_native_sale: true,
        }),
        feature_store: Arc::new(InstrumentedFeatureStore {
            probe: probe.clone(),
            sleep_ms: 0,
            wait_for_transfer_before_seed_two_snapshot: true,
            panic_on_seed_snapshot: None,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 2,
            seed_cpu_max_concurrency: 2,
            matched_contract_max_concurrency: 2,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        probe.obsolete_metric_receipt_calls.load(Ordering::SeqCst),
        0,
        "removed receipt metric fetches are no longer part of the batch path"
    );
}

#[tokio::test]
async fn batch_progress_reporter_receives_seed_lifecycle_events() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n").unwrap();
    let batch_progress = Arc::new(RecordingBatchProgressReporter::default());
    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: batch_progress.clone(),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        batch_progress.started.lock().unwrap().as_slice(),
        ["0xseed1"]
    );
    assert_eq!(
        batch_progress.finished.lock().unwrap().as_slice(),
        ["0xseed1"]
    );
    assert!(batch_progress.cached.lock().unwrap().is_empty());
    assert!(batch_progress.failed.lock().unwrap().is_empty());
    assert_eq!(
        batch_progress.seed_events.lock().unwrap().as_slice(),
        ["create:0xseed1"]
    );
}

#[tokio::test]
async fn batch_progress_reporter_does_not_count_ignored_cached_legacy_outputs() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let cached_report = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Cached Seed".into(),
            ..SeedContractPayload::default()
        },
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    );
    std::fs::write(
        dir.path().join("top_contract_analysis__cached.json"),
        serde_json::to_string(&cached_report).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached.md"),
        "# cached\n",
    )
    .unwrap();
    let batch_progress = Arc::new(RecordingBatchProgressReporter::default());
    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Arc::new(EmptyFeatureStore),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: batch_progress.clone(),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            seed_network_max_concurrency: 1,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(batch_progress.cached.lock().unwrap().is_empty());
    assert_eq!(
        batch_progress.started.lock().unwrap().as_slice(),
        ["0xseed1", "0xseed2"]
    );
    assert_eq!(
        batch_progress.finished.lock().unwrap().as_slice(),
        ["0xseed1", "0xseed2"]
    );
    assert!(batch_progress.failed.lock().unwrap().is_empty());
}
