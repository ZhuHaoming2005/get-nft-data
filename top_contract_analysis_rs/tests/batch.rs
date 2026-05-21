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
    AddressAttributionPayload, AddressSignalPayload, BatchReportSummary, BatchSeedReportPayload,
    BatchSummaryPayload, ContractMetadata, DatabaseNftRecord, DatabaseSnapshot, EthTransferRecord,
    HonestAddressPayload, InfringingTokenRecord, MaliciousAddressPayload, NftSaleRecord,
    OutputFilesPayload, OwnerBalance, ReportSummary, SecondarySaleVictimAddressPayload,
    SeedContractPayload, SeedNft, SingleReportPayload, TransactionReceiptRecord, TransferRecord,
    VictimAcquisitionAddressPayload,
};
use top_contract_analysis_rs::progress::{
    BatchProgressReporter, NoopBatchProgressReporter, NoopProgressReporter, SeedProgressReporter,
};
use top_contract_analysis_rs::reporting::{
    render_batch_human_readable_report, write_batch_summary_outputs,
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
        let mut captured_seed_names = self.captured_seed_names.lock().unwrap();
        for (seed_address, seed_nfts) in seeds {
            captured_seed_names.insert(
                seed_address.clone(),
                seed_nfts.iter().map(|item| item.name.clone()).collect(),
            );
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
    later_context_overlapped_snapshot: AtomicBool,
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
    sale_metric_active: AtomicBool,
    sale_metric_finished: AtomicBool,
    sale_metric_receipt_calls: AtomicUsize,
    holder_started_before_sale_metric_finished: AtomicBool,
    expansion_observed_sale_metric_active: AtomicBool,
    metadata_calls: AtomicUsize,
    metadata_current: AtomicUsize,
    metadata_max_seen: AtomicUsize,
    candidate_metadata_calls: AtomicUsize,
    candidate_metadata_current: AtomicUsize,
    candidate_metadata_max_seen: AtomicUsize,
    snapshot_calls: AtomicUsize,
    snapshot_batch_calls: AtomicUsize,
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
        self.probe.snapshot_active.fetch_add(1, Ordering::SeqCst);
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

struct InstrumentedBatchApi {
    probe: Arc<BatchPipelineProbe>,
    transfer_sleep_ms: u64,
    emit_native_sale: bool,
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
        let seed_metadata_call_index = if is_seed_contract {
            Some(self.probe.metadata_calls.fetch_add(1, Ordering::SeqCst))
        } else {
            None
        };
        if is_seed_contract {
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
        if seed_metadata_call_index.is_some_and(|index| index > 0) {
            let mut waited = 0;
            while self.probe.snapshot_active.load(Ordering::SeqCst) == 0 && waited < 100 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                waited += 1;
            }
            if self.probe.snapshot_active.load(Ordering::SeqCst) > 0 {
                self.probe
                    .later_context_overlapped_snapshot
                    .store(true, Ordering::SeqCst);
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
            && !self.probe.sale_metric_finished.load(Ordering::SeqCst)
        {
            self.probe
                .holder_started_before_sale_metric_finished
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
            && self.probe.sale_metric_active.load(Ordering::SeqCst)
        {
            self.probe
                .expansion_observed_sale_metric_active
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
                .sale_metric_receipt_calls
                .fetch_add(1, Ordering::SeqCst);
            self.probe.sale_metric_active.store(true, Ordering::SeqCst);
            let mut waited = 0;
            while !self
                .probe
                .expansion_observed_sale_metric_active
                .load(Ordering::SeqCst)
                && waited < 1000
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
                waited += 1;
            }
            self.probe.sale_metric_active.store(false, Ordering::SeqCst);
            self.probe
                .sale_metric_finished
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
    report_summary: ReportSummary,
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
        report_summary,
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
fn batch_seed_reports_serialize_narrow_index_shape() {
    let payload = BatchSummaryPayload {
        seed_reports: vec![BatchSeedReportPayload {
            seed_contract: SeedContractPayload {
                contract_address: "0xseed".into(),
                ..Default::default()
            },
            report_summary: ReportSummary {
                candidate_contract_count: 2,
                ..Default::default()
            },
            output_files: Some(OutputFilesPayload {
                json: "result/top_contract_analysis__seed.json".into(),
                markdown: "result/top_contract_analysis__seed.md".into(),
            }),
        }],
        ..Default::default()
    };

    let serialized = serde_json::to_value(&payload).unwrap();
    let seed_report = serialized["seed_reports"][0].as_object().unwrap();
    let keys: BTreeSet<_> = seed_report.keys().map(String::as_str).collect();

    assert_eq!(
        keys,
        BTreeSet::from(["seed_contract", "report_summary", "output_files"])
    );
    assert!(!seed_report.contains_key("duplicate_candidates"));
    assert!(!seed_report.contains_key("malicious_addresses"));
    assert_eq!(
        serialized["seed_reports"][0]["output_files"]["json"],
        "result/top_contract_analysis__seed.json"
    );
    assert_eq!(
        serialized["seed_reports"][0]["report_summary"]["candidate_contract_count"],
        2
    );
}

#[test]
fn batch_markdown_preserves_reference_summary_and_output_index_lines() {
    let payload = BatchSummaryPayload {
        batch_summary: BatchReportSummary {
            seed_report_count: 2,
            chain: "ethereum".into(),
            chains: vec!["ethereum".into()],
            open_license_detected_count: 1,
            candidate_contract_count_total: 10,
            infringing_nft_count_total: 11,
            malicious_address_count_total: 7,
            neutral_address_count_total: 8,
            repeat_infringing_address_count_total: 3,
            repeat_infringing_address_count_global: 2,
            legit_duplicate_contract_count_total: 1,
            secondary_sale_victim_cost_eth_total: 12.5,
            secondary_sale_victim_cost_usd_total: 12.5,
            secondary_sale_stuck_cost_eth_total: 5.0,
            secondary_sale_stuck_cost_usd_total: 5.0,
            secondary_sale_stuck_cost_ratio_overall: Some(0.4),
            victim_acquisition_total_eth_total: 12.5,
            victim_acquisition_total_usd_total: 12.5,
            victim_acquisition_stuck_cost_eth_total: 5.0,
            victim_acquisition_stuck_cost_usd_total: 5.0,
            victim_acquisition_stuck_cost_ratio_overall: Some(0.4),
            victim_acquisition_address_count_total: 6,
            victim_acquisition_address_count_distinct: 4,
            buy_asset_ratio_known_address_count_total: 8,
            ratio_over_60_address_count_total: 3,
            ratio_over_60_address_ratio_overall: Some(0.375),
            ratio_over_80_address_count_total: 1,
            ratio_over_80_address_ratio_overall: Some(0.125),
            stuck_victim_address_count_total: 2,
            stuck_victim_address_ratio_overall: Some(0.25),
            stuck_victim_address_count_distinct: 2,
            stuck_victim_address_ratio_distinct: Some(0.5),
            corrupted_victim_address_count_total: 1,
            corrupted_victim_address_count_distinct: 1,
            avg_deployment_to_neutral_holder_seconds_mean: Some(12.5),
            median_deployment_to_neutral_holder_seconds_median: Some(10.0),
            avg_deployment_to_first_transfer_seconds_mean: Some(8.0),
            median_deployment_to_first_transfer_seconds_median: Some(7.0),
            avg_unique_receiver_count_mean: Some(4.0),
            generated_at: "2026-04-17T00:00:00+00:00".into(),
            ..BatchReportSummary::default()
        },
        seed_reports: vec![BatchSeedReportPayload {
            seed_contract: SeedContractPayload {
                name: "Azuki".into(),
                contract_address: "0xseed".into(),
                ..Default::default()
            },
            report_summary: ReportSummary {
                candidate_contract_count: 5,
                infringing_nft_count: 4,
                malicious_address_count: 5,
                neutral_address_count: 6,
                repeat_infringing_address_count: 1,
                legit_duplicate_contract_count: 1,
                secondary_sale_victim_cost_eth: 7.5,
                secondary_sale_victim_cost_usd: 7.5,
                secondary_sale_stuck_cost_eth: 2.5,
                secondary_sale_stuck_cost_usd: 2.5,
                secondary_sale_stuck_cost_ratio: Some(1.0 / 3.0),
                victim_acquisition_total_eth: 7.5,
                victim_acquisition_total_usd: 7.5,
                victim_acquisition_stuck_cost_eth: 2.5,
                victim_acquisition_stuck_cost_usd: 2.5,
                victim_acquisition_stuck_cost_ratio: Some(1.0 / 3.0),
                ratio_over_60_address_count: 2,
                ratio_over_60_address_ratio: Some(0.5),
                stuck_victim_address_count: 1,
                stuck_victim_address_ratio: Some(0.25),
                corrupted_victim_address_count: 1,
                avg_deployment_to_neutral_holder_seconds: Some(10.0),
                median_deployment_to_neutral_holder_seconds: Some(9.0),
                median_deployment_to_first_transfer_seconds: Some(8.0),
                ..Default::default()
            },
            output_files: Some(OutputFilesPayload {
                json: "result/top_contract_analysis__azuki.json".into(),
                markdown: "result/top_contract_analysis__azuki.md".into(),
            }),
        }],
    };

    let markdown = render_batch_human_readable_report(&payload);

    assert!(markdown.contains("# Top NFT 合约批量分析总报告"));
    assert!(markdown.contains("- 检测到开放许可的 seed 数: 1"));
    assert!(markdown.contains("- 疑似操作者地址总数: 7"));
    assert!(markdown.contains("- 受害者地址数(全局去重): 4"));
    assert!(markdown.contains("- 受害者地址观测数(按 seed 求和): 6"));
    assert!(markdown.contains("- 受害者获取成本(USD)汇总: 12.5"));
    assert!(markdown.contains("- 总套牢成本(USD)汇总: 5 / 40.00%"));
    assert!(markdown.contains("- 二级市场受害者成本(USD)汇总: 12.5"));
    assert!(markdown.contains("- 付费 mint 受害者成本(USD)汇总: 0 / edges=0"));
    assert!(
        markdown.contains("- 获取成本占购买前 ETH 余额估算 >60% 的受害者数/总体占比: 3 / 37.50%")
    );
    assert!(markdown.contains("- 套牢受害者地址数(全局去重)/占比: 2 / 50.00%"));
    assert!(markdown.contains("- 套牢受害者地址观测数(按 seed 求和)/占比: 2 / 25.00%"));
    assert!(markdown.contains("- 被腐化受害者地址数(全局去重): 1"));
    assert!(markdown.contains("- 被腐化受害者地址观测数(按 seed 求和): 1"));
    assert!(markdown.contains("- 生成时间(UTC): 2026-04-17T00:00:00+00:00"));
    assert!(markdown.contains("## Seed 报告索引"));
    assert!(markdown.contains(
        "- Azuki (0xseed) | 重复合约=5 | 侵权NFT=4 | 疑似操作者=5 | 中性地址=6 | 受害者=0 | 多次侵权地址=1 | 官方参与=1 | 受害者获取成本(USD)=7.5 | 二级成本(USD)=7.5 | 付费mint(USD)=0 | 总套牢(USD)=2.5/33.33% | >60%=2/50.00% | 套牢受害者=1/25.00% | 被腐化受害者=1 | 部署到中性接收平均=10秒 | 部署到中性接收中位=9秒 | 部署到首次转手中位=8秒 | JSON=result/top_contract_analysis__azuki.json | MD=result/top_contract_analysis__azuki.md"
    ));
}

#[tokio::test]
async fn batch_skips_cached_seed_reports_in_output_directory() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let cached_report = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Cached Seed".into(),
            ..SeedContractPayload::default()
        },
        ReportSummary {
            candidate_contract_count: 1,
            ..ReportSummary::default()
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

    assert_eq!(summary.batch_summary.seed_report_count, 2);
    assert_eq!(summary.seed_reports.len(), 2);
    assert_eq!(
        summary.seed_reports[0].seed_contract.contract_address,
        "0xseed1"
    );
    assert_eq!(
        summary.seed_reports[1].seed_contract.contract_address,
        "0xseed2"
    );
    assert_eq!(
        summary.seed_reports[0].output_files.as_ref().unwrap().json,
        "top_contract_analysis__cached.json"
    );
    assert!(summary.seed_reports[1]
        .output_files
        .as_ref()
        .unwrap()
        .json
        .starts_with("top_contract_analysis__"));
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
async fn batch_recomputes_cached_seed_summary_and_global_metrics_from_full_payloads() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();

    let cached_one = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Seed One".into(),
            ..SeedContractPayload::default()
        },
        ReportSummary {
            candidate_contract_count: 2,
            infringing_nft_count: 2,
            malicious_address_count: 2,
            neutral_address_count: 1,
            repeat_infringing_address_count: 1,
            secondary_sale_victim_cost_eth: 10.0,
            secondary_sale_victim_cost_usd: 10.0,
            secondary_sale_stuck_cost_eth: 2.0,
            secondary_sale_stuck_cost_usd: 2.0,
            secondary_sale_stuck_cost_ratio: Some(0.2),
            victim_acquisition_total_eth: 10.0,
            victim_acquisition_total_usd: 10.0,
            victim_acquisition_stuck_cost_eth: 2.0,
            victim_acquisition_stuck_cost_usd: 2.0,
            victim_acquisition_stuck_cost_ratio: Some(0.2),
            victim_acquisition_address_count: 2,
            buy_asset_ratio_known_address_count: 2,
            ratio_over_60_address_count: 1,
            ratio_over_80_address_count: 0,
            stuck_victim_address_count: 1,
            corrupted_victim_address_count: 1,
            avg_deployment_to_neutral_holder_seconds: Some(12.0),
            median_deployment_to_neutral_holder_seconds: Some(10.0),
            avg_deployment_to_first_transfer_seconds: Some(8.0),
            median_deployment_to_first_transfer_seconds: Some(7.0),
            avg_unique_receiver_count: Some(2.0),
            ..ReportSummary::default()
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
                ..MaliciousAddressPayload::default()
            },
            MaliciousAddressPayload {
                address: "0xm2".into(),
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
                last_buy_amount_eth: Some(2.0),
                last_buy_amount_usd: Some(2.0),
                is_stuck: true,
                ..SecondarySaleVictimAddressPayload::default()
            },
            SecondarySaleVictimAddressPayload {
                address: "0xv2".into(),
                last_buy_amount_eth: Some(4.0),
                last_buy_amount_usd: Some(4.0),
                is_stuck: false,
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
        ReportSummary {
            candidate_contract_count: 3,
            infringing_nft_count: 1,
            malicious_address_count: 2,
            neutral_address_count: 2,
            secondary_sale_victim_cost_eth: 5.0,
            secondary_sale_victim_cost_usd: 5.0,
            secondary_sale_stuck_cost_eth: 1.0,
            secondary_sale_stuck_cost_usd: 1.0,
            secondary_sale_stuck_cost_ratio: Some(0.2),
            victim_acquisition_total_eth: 5.0,
            victim_acquisition_total_usd: 5.0,
            victim_acquisition_stuck_cost_eth: 1.0,
            victim_acquisition_stuck_cost_usd: 1.0,
            victim_acquisition_stuck_cost_ratio: Some(0.2),
            victim_acquisition_address_count: 3,
            buy_asset_ratio_known_address_count: 3,
            ratio_over_60_address_count: 2,
            ratio_over_80_address_count: 1,
            stuck_victim_address_count: 1,
            corrupted_victim_address_count: 1,
            avg_deployment_to_neutral_holder_seconds: Some(18.0),
            median_deployment_to_neutral_holder_seconds: Some(20.0),
            avg_deployment_to_first_transfer_seconds: Some(14.0),
            median_deployment_to_first_transfer_seconds: Some(20.0),
            avg_unique_receiver_count: Some(4.0),
            ..ReportSummary::default()
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
                ..MaliciousAddressPayload::default()
            },
            MaliciousAddressPayload {
                address: "0xm3".into(),
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
                last_buy_amount_eth: Some(1.0),
                last_buy_amount_usd: Some(1.0),
                is_stuck: true,
                ..SecondarySaleVictimAddressPayload::default()
            },
            SecondarySaleVictimAddressPayload {
                address: "0xv3".into(),
                ..SecondarySaleVictimAddressPayload::default()
            },
            SecondarySaleVictimAddressPayload {
                address: "0xv4".into(),
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

    assert_eq!(summary.batch_summary.seed_report_count, 2);
    assert_eq!(summary.batch_summary.candidate_contract_count_total, 5);
    assert_eq!(summary.batch_summary.infringing_nft_count_total, 3);
    assert_eq!(summary.batch_summary.malicious_address_count_total, 3);
    assert_eq!(summary.batch_summary.neutral_address_count_total, 2);
    assert_eq!(
        summary.batch_summary.repeat_infringing_address_count_total,
        1
    );
    assert_eq!(
        summary.batch_summary.repeat_infringing_address_count_global,
        1
    );
    assert_eq!(
        summary.batch_summary.secondary_sale_victim_cost_eth_total,
        15.0
    );
    assert_eq!(
        summary.batch_summary.secondary_sale_stuck_cost_eth_total,
        3.0
    );
    assert_eq!(
        summary
            .batch_summary
            .secondary_sale_stuck_cost_ratio_overall,
        Some(0.2)
    );
    assert_eq!(
        summary
            .batch_summary
            .buy_asset_ratio_known_address_count_total,
        5
    );
    assert_eq!(summary.batch_summary.ratio_over_60_address_count_total, 3);
    assert_eq!(
        summary.batch_summary.ratio_over_60_address_ratio_overall,
        Some(0.6)
    );
    assert_eq!(summary.batch_summary.ratio_over_80_address_count_total, 1);
    assert_eq!(
        summary.batch_summary.ratio_over_80_address_ratio_overall,
        Some(0.2)
    );
    assert_eq!(summary.batch_summary.stuck_victim_address_count_total, 2);
    let batch_summary_json = serde_json::to_value(&summary.batch_summary).unwrap();
    assert_eq!(
        batch_summary_json["victim_acquisition_address_count_distinct"],
        4
    );
    assert_eq!(batch_summary_json["stuck_victim_address_count_distinct"], 2);
    assert_eq!(
        batch_summary_json["stuck_victim_address_ratio_distinct"],
        0.5
    );
    assert_eq!(
        summary.batch_summary.stuck_victim_address_ratio_overall,
        Some(0.4)
    );
    assert_eq!(
        summary.batch_summary.corrupted_victim_address_count_total,
        2
    );
    assert_eq!(
        batch_summary_json["corrupted_victim_address_count_distinct"],
        1
    );
    assert_eq!(
        summary
            .batch_summary
            .avg_deployment_to_neutral_holder_seconds_mean,
        Some(15.0)
    );
    assert_eq!(
        summary
            .batch_summary
            .median_deployment_to_neutral_holder_seconds_median,
        Some(15.0)
    );
    assert_eq!(
        summary
            .batch_summary
            .avg_deployment_to_first_transfer_seconds_mean,
        Some(11.0)
    );
    assert_eq!(
        summary
            .batch_summary
            .median_deployment_to_first_transfer_seconds_median,
        Some(13.5)
    );
    assert_eq!(
        summary.batch_summary.avg_unique_receiver_count_mean,
        Some(3.0)
    );

    assert_eq!(
        summary.seed_reports[0].report_summary.infringing_nft_count,
        2
    );
    assert_eq!(
        summary.seed_reports[0]
            .report_summary
            .malicious_address_count,
        2
    );
    assert_eq!(
        summary.seed_reports[0].report_summary.neutral_address_count,
        1
    );
    assert_eq!(
        summary.seed_reports[0]
            .report_summary
            .repeat_infringing_address_count,
        1
    );
    assert_eq!(
        summary.seed_reports[0]
            .report_summary
            .secondary_sale_stuck_cost_eth,
        2.0
    );
    assert_eq!(
        summary.seed_reports[0]
            .report_summary
            .secondary_sale_stuck_cost_ratio,
        Some(0.2)
    );
    assert_eq!(
        summary.seed_reports[0]
            .report_summary
            .median_deployment_to_neutral_holder_seconds,
        Some(10.0)
    );
    assert_eq!(
        summary.seed_reports[0]
            .report_summary
            .median_deployment_to_first_transfer_seconds,
        Some(7.0)
    );
}

#[tokio::test]
async fn batch_writes_summary_files_with_existing_names() {
    let payload = BatchSummaryPayload {
        batch_summary: BatchReportSummary {
            seed_report_count: 1,
            ..BatchReportSummary::default()
        },
        seed_reports: vec![BatchSeedReportPayload {
            seed_contract: SeedContractPayload {
                contract_address: "0xseed".into(),
                ..SeedContractPayload::default()
            },
            report_summary: ReportSummary::default(),
            output_files: Some(OutputFilesPayload {
                json: "top_contract_analysis__seed.json".into(),
                markdown: "top_contract_analysis__seed.md".into(),
            }),
        }],
    };
    let dir = tempdir().unwrap();

    let (json_path, md_path) = write_batch_summary_outputs(&payload, dir.path()).unwrap();

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
async fn batch_prefetches_later_seed_context_while_earlier_seed_loads_snapshot() {
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
    assert_eq!(probe.snapshot_batch_calls.load(Ordering::SeqCst), 0);
    assert!(probe
        .later_context_overlapped_snapshot
        .load(Ordering::SeqCst));
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
    assert_eq!(probe.snapshot_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_loads_snapshots_per_seed_without_seed_chunks() {
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

    assert_eq!(probe.snapshot_batch_calls.load(Ordering::SeqCst), 0);
    assert_eq!(probe.snapshot_calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.cpu_max_seen.load(Ordering::SeqCst), 1);
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
async fn batch_allows_match_contracts_from_different_seeds_to_overlap_sale_metrics() {
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

    assert!(
        probe.sale_metric_receipt_calls.load(Ordering::SeqCst) > 0,
        "expected seed1 native sale metrics to run"
    );
    assert!(
        probe
            .holder_started_before_sale_metric_finished
            .load(Ordering::SeqCst),
        "expected seed2 pre-analysis holder API check to start before seed1 sale metrics finished"
    );
    assert!(
        probe
            .expansion_observed_sale_metric_active
            .load(Ordering::SeqCst),
        "expected seed2 matched-contract expansion to overlap seed1 sale metrics under the global matched-contract limit"
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
async fn batch_progress_reporter_counts_cached_seed_reports() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    let cached_report = cached_single_report(
        SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed1".into(),
            name: "Cached Seed".into(),
            ..SeedContractPayload::default()
        },
        ReportSummary {
            candidate_contract_count: 1,
            ..ReportSummary::default()
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
}
