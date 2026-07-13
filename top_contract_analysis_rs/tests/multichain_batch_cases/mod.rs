use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tempfile::tempdir;
use tokio::sync::Semaphore;
use top_contract_analysis_rs::analysis::{
    run_multichain_batch, AnalysisDeps, AnalyzeApi, FeatureStoreReader, MultiChainBatchRequest,
};
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::models::{
    Chain, ChainTotalsPayload, ContractMetadata, DatabaseSnapshot, EthTransferRecord,
    NftSaleRecord, OwnerBalance, SeedNft, TransactionReceiptRecord, TransferRecord,
};
use top_contract_analysis_rs::progress::{NoopBatchProgressReporter, NoopProgressReporter};

struct EmptyCrossChainStore {
    chains: Arc<Mutex<Vec<String>>>,
}

struct BulkOnlyStore {
    bulk_calls: Arc<AtomicUsize>,
    max_batch_size: Arc<AtomicUsize>,
    loaded_seed_scopes: Arc<AtomicUsize>,
}

impl FeatureStoreReader for BulkOnlyStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        panic!("batch path must use load_snapshots")
    }

    fn load_snapshots(
        &self,
        _chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<std::collections::BTreeMap<String, DatabaseSnapshot>, AppError> {
        self.bulk_calls.fetch_add(1, Ordering::SeqCst);
        record_max(&self.max_batch_size, seeds.len());
        self.loaded_seed_scopes
            .fetch_add(seeds.len(), Ordering::SeqCst);
        Ok(seeds
            .iter()
            .map(|(address, _)| (address.clone(), DatabaseSnapshot::default()))
            .collect())
    }

    fn chain_totals(&self, _chain: &str) -> Result<ChainTotalsPayload, AppError> {
        Ok(ChainTotalsPayload {
            total_nfts: 1_000,
            total_contracts: 100,
        })
    }
}

#[tokio::test]
async fn batch_uses_bulk_snapshot_loading_for_multiple_seeds() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        concat!(
            "chain,address\n",
            "ethereum,0x1111111111111111111111111111111111111111\n",
            "ethereum,0x2222222222222222222222222222222222222222\n",
            "ethereum,0x3333333333333333333333333333333333333333\n",
            "ethereum,0x4444444444444444444444444444444444444444\n",
        ),
    )
    .unwrap();
    let bulk_calls = Arc::new(AtomicUsize::new(0));
    let max_batch_size = Arc::new(AtomicUsize::new(0));
    let loaded_seed_scopes = Arc::new(AtomicUsize::new(0));

    let result = run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir: dir.path().join("output"),
            ..MultiChainBatchRequest::default()
        },
        &AnalysisDeps {
            api: Arc::new(SeedOnlyApi {
                metadata_calls: Arc::new(AtomicUsize::new(0)),
            }),
            feature_store: Arc::new(BulkOnlyStore {
                bulk_calls: bulk_calls.clone(),
                max_batch_size: max_batch_size.clone(),
                loaded_seed_scopes,
            }),
            progress: Arc::new(NoopProgressReporter),
            batch_progress: Arc::new(NoopBatchProgressReporter),
        },
    )
    .await
    .unwrap();

    assert!(result.failures.is_empty());
    assert!(bulk_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(max_batch_size.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn batch_releases_full_snapshot_slots_before_provider_analysis() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        concat!(
            "chain,address\n",
            "ethereum,0x1111111111111111111111111111111111111111\n",
            "ethereum,0x2222222222222222222222222222222222222222\n",
            "ethereum,0x3333333333333333333333333333333333333333\n",
            "ethereum,0x4444444444444444444444444444444444444444\n",
        ),
    )
    .unwrap();
    let loaded_seed_scopes = Arc::new(AtomicUsize::new(0));
    let quality_started = Arc::new(AtomicUsize::new(0));
    let quality_release = Arc::new(Semaphore::new(0));
    let request = MultiChainBatchRequest {
        seed_file,
        output_dir: dir.path().join("output"),
        seed_network_max_concurrency: 8,
        seed_cpu_max_concurrency: 100,
        matched_contract_max_concurrency: 100,
        ..MultiChainBatchRequest::default()
    };
    let deps = AnalysisDeps {
        api: Arc::new(ConcurrentSeedApi {
            metadata_current: Arc::new(AtomicUsize::new(0)),
            metadata_max_seen: Arc::new(AtomicUsize::new(0)),
            wait_for_snapshot_address: None,
            snapshot_started: Arc::new(AtomicUsize::new(0)),
            matched_current: Arc::new(AtomicUsize::new(0)),
            matched_max_seen: Arc::new(AtomicUsize::new(0)),
            quality_started: quality_started.clone(),
            quality_release: Some(quality_release.clone()),
        }),
        feature_store: Arc::new(BulkOnlyStore {
            bulk_calls: Arc::new(AtomicUsize::new(0)),
            max_batch_size: Arc::new(AtomicUsize::new(0)),
            loaded_seed_scopes: loaded_seed_scopes.clone(),
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };
    let batch = tokio::spawn(async move { run_multichain_batch(request, &deps).await });

    tokio::time::timeout(Duration::from_secs(2), async {
        while quality_started.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("two snapshots should reach analysis");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let loaded_before_release = loaded_seed_scopes.load(Ordering::SeqCst);
    quality_release.add_permits(100);

    let result = batch.await.unwrap().unwrap();
    assert!(result.failures.is_empty());
    assert_eq!(
        loaded_before_release, 4,
        "provider stalls must retain only compact plans and allow bounded full-snapshot loading to continue"
    );
}

#[tokio::test]
async fn cancelled_batch_stops_before_new_seed_work_and_keeps_manifest_incomplete() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        "chain,address\nethereum,0x1111111111111111111111111111111111111111\n",
    )
    .unwrap();
    let bulk_calls = Arc::new(AtomicUsize::new(0));
    let cancellation_requested = Arc::new(AtomicBool::new(true));
    let output_dir = dir.path().join("output");

    let error = run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir: output_dir.clone(),
            cancellation_requested,
            ..MultiChainBatchRequest::default()
        },
        &AnalysisDeps {
            api: Arc::new(SeedOnlyApi {
                metadata_calls: Arc::new(AtomicUsize::new(0)),
            }),
            feature_store: Arc::new(BulkOnlyStore {
                bulk_calls: bulk_calls.clone(),
                max_batch_size: Arc::new(AtomicUsize::new(0)),
                loaded_seed_scopes: Arc::new(AtomicUsize::new(0)),
            }),
            progress: Arc::new(NoopProgressReporter),
            batch_progress: Arc::new(NoopBatchProgressReporter),
        },
    )
    .await
    .unwrap_err();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_dir.join("run-manifest.json")).unwrap())
            .unwrap();

    assert!(matches!(error, AppError::Interrupted(_)), "{error}");
    assert_eq!(bulk_calls.load(Ordering::SeqCst), 0);
    assert_eq!(manifest["status"], "incomplete");
    let metrics = std::fs::read_to_string(output_dir.join("run-metrics.jsonl")).unwrap();
    let metric: serde_json::Value = serde_json::from_str(metrics.trim()).unwrap();
    assert_eq!(metric["event"], "seed_finished");
    assert_eq!(metric["cancelled"], true);
}

impl FeatureStoreReader for EmptyCrossChainStore {
    fn load_snapshot(
        &self,
        chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        self.chains.lock().unwrap().push(chain.to_string());
        Ok(DatabaseSnapshot::default())
    }

    fn chain_totals(&self, _chain: &str) -> Result<ChainTotalsPayload, AppError> {
        Ok(ChainTotalsPayload {
            total_nfts: 1_000,
            total_contracts: 100,
        })
    }
}

struct SeedOnlyApi {
    metadata_calls: Arc<AtomicUsize>,
}

fn record_max(max_seen: &AtomicUsize, current: usize) {
    let mut observed = max_seen.load(Ordering::SeqCst);
    while current > observed {
        match max_seen.compare_exchange(observed, current, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(actual) => observed = actual,
        }
    }
}

struct ConcurrentSeedApi {
    metadata_current: Arc<AtomicUsize>,
    metadata_max_seen: Arc<AtomicUsize>,
    wait_for_snapshot_address: Option<String>,
    snapshot_started: Arc<AtomicUsize>,
    matched_current: Arc<AtomicUsize>,
    matched_max_seen: Arc<AtomicUsize>,
    quality_started: Arc<AtomicUsize>,
    quality_release: Option<Arc<Semaphore>>,
}

#[async_trait]
impl AnalyzeApi for ConcurrentSeedApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        let current = self.metadata_current.fetch_add(1, Ordering::SeqCst) + 1;
        record_max(&self.metadata_max_seen, current);
        if self.wait_for_snapshot_address.as_deref() == Some(contract_address) {
            while self.snapshot_started.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        } else {
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        self.metadata_current.fetch_sub(1, Ordering::SeqCst);
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            name: "Seed".into(),
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
            name: "Seed #1".into(),
            token_uri: "ipfs://shared".into(),
            ..SeedNft::default()
        }])
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
        let current = self.matched_current.fetch_add(1, Ordering::SeqCst) + 1;
        record_max(&self.matched_max_seen, current);
        tokio::time::sleep(Duration::from_millis(30)).await;
        self.matched_current.fetch_sub(1, Ordering::SeqCst);
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "dup".into(),
            name: "Seed #1".into(),
            token_uri: "ipfs://shared".into(),
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
        Ok(Vec::new())
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(Vec::new())
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(Vec::new())
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
        Ok(Vec::new())
    }

    async fn fetch_provider_data_quality(
        &self,
        _chain: &str,
        _contract_address: &str,
    ) -> Result<top_contract_analysis_rs::models::ProviderDataQualityPayload, AppError> {
        if let Some(release) = &self.quality_release {
            self.quality_started.fetch_add(1, Ordering::SeqCst);
            release
                .acquire()
                .await
                .expect("test quality gate remains open")
                .forget();
        }
        Ok(top_contract_analysis_rs::models::ProviderDataQualityPayload::default())
    }
}

struct ConcurrentSnapshotStore {
    current: Arc<AtomicUsize>,
    max_seen: Arc<AtomicUsize>,
    same_seed_current: Arc<Mutex<std::collections::BTreeMap<String, usize>>>,
    same_seed_max_seen: Arc<AtomicUsize>,
    network_current: Arc<AtomicUsize>,
    snapshot_started: Arc<AtomicUsize>,
    observed_stage_overlap: Arc<AtomicUsize>,
    emit_candidate: bool,
}

impl FeatureStoreReader for ConcurrentSnapshotStore {
    fn load_snapshot(
        &self,
        chain: &str,
        seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        let seed = seed_nfts
            .first()
            .map(|nft| nft.contract_address.clone())
            .unwrap_or_default();
        let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        record_max(&self.max_seen, current);
        {
            let mut active = self.same_seed_current.lock().unwrap();
            let seed_current = active.entry(seed.clone()).or_default();
            *seed_current += 1;
            record_max(&self.same_seed_max_seen, *seed_current);
        }
        self.snapshot_started.store(1, Ordering::SeqCst);
        if self.network_current.load(Ordering::SeqCst) > 0 {
            self.observed_stage_overlap.store(1, Ordering::SeqCst);
        }
        std::thread::sleep(Duration::from_millis(30));
        self.current.fetch_sub(1, Ordering::SeqCst);
        *self
            .same_seed_current
            .lock()
            .unwrap()
            .get_mut(&seed)
            .unwrap() -= 1;
        Ok(if self.emit_candidate {
            DatabaseSnapshot {
                nft_rows: vec![top_contract_analysis_rs::models::DatabaseNftRecord {
                    contract_address: if chain == "solana" {
                        "So11111111111111111111111111111111111111112".into()
                    } else {
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()
                    },
                    token_id: "dup".into(),
                    token_uri: "ipfs://shared".into(),
                    ..Default::default()
                }],
                ..DatabaseSnapshot::default()
            }
        } else {
            DatabaseSnapshot::default()
        })
    }

    fn chain_totals(&self, _chain: &str) -> Result<ChainTotalsPayload, AppError> {
        Ok(ChainTotalsPayload {
            total_nfts: 1_000,
            total_contracts: 100,
        })
    }
}

#[async_trait]
impl AnalyzeApi for SeedOnlyApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ContractMetadata {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_type: "ERC721".into(),
            name: "Seed".into(),
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
            name: "Seed #1".into(),
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
        Ok(Vec::new())
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(Vec::new())
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(Vec::new())
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
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn mixed_batch_analyzes_each_seed_against_all_four_chains() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        "chain,address\nethereum,0x1111111111111111111111111111111111111111\n",
    )
    .unwrap();
    let seen_chains = Arc::new(Mutex::new(Vec::new()));
    let deps = AnalysisDeps {
        api: Arc::new(SeedOnlyApi {
            metadata_calls: Arc::new(AtomicUsize::new(0)),
        }),
        feature_store: Arc::new(EmptyCrossChainStore {
            chains: seen_chains.clone(),
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let result = run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir: dir.path().join("output"),
            ..MultiChainBatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    let mut chains = seen_chains.lock().unwrap().clone();
    chains.sort();
    assert_eq!(chains, ["base", "ethereum", "polygon", "solana"]);
    assert!(result.failures.is_empty());
    assert!(result
        .scoped_duplicate_scale
        .iter()
        .any(|row| row.scope == "intra_chain"));
    assert!(result
        .scoped_duplicate_scale
        .iter()
        .any(|row| row.scope == "cross_chain_summary"));
    assert_eq!(
        result
            .scoped_duplicate_scale
            .iter()
            .filter(|row| row.scope == "chain_matrix")
            .count(),
        15
    );
    assert_eq!(
        result
            .scoped_paper_stats
            .iter()
            .filter(|row| row.scope == "chain_matrix")
            .count(),
        3
    );
    let solana = result
        .scoped_paper_stats
        .iter()
        .find(|row| row.secondary_chain == "solana")
        .unwrap();
    assert_eq!(solana.native_symbol, "SOL");
    assert_eq!(
        solana.paper_stats["duplicate_scale"][0]["duplicate_nft_ratio_denominator"],
        1_000
    );
    let cross = result
        .scoped_paper_stats
        .iter()
        .find(|row| row.scope == "cross_chain_summary")
        .unwrap();
    assert!(cross.native_symbol.is_empty());
    assert!(!serde_json::to_string(&cross.paper_stats)
        .unwrap()
        .contains("_native"));
}

#[tokio::test]
async fn completed_manifest_starts_a_fresh_run_instead_of_reusing_cache() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        "chain,address\nethereum,0x1111111111111111111111111111111111111111\n",
    )
    .unwrap();
    let metadata_calls = Arc::new(AtomicUsize::new(0));
    let deps = AnalysisDeps {
        api: Arc::new(SeedOnlyApi {
            metadata_calls: metadata_calls.clone(),
        }),
        feature_store: Arc::new(EmptyCrossChainStore {
            chains: Arc::new(Mutex::new(Vec::new())),
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };
    let request = MultiChainBatchRequest {
        seed_file,
        output_dir: dir.path().join("output"),
        paper_stats_config: top_contract_analysis_rs::analysis::paper_stats::PaperStatsConfig {
            analysis_timestamp: 1_700_000_000,
            ..Default::default()
        },
        ..MultiChainBatchRequest::default()
    };

    run_multichain_batch(request.clone(), &deps).await.unwrap();
    run_multichain_batch(request, &deps).await.unwrap();

    assert_eq!(metadata_calls.load(Ordering::SeqCst), 2);
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.path().join("output").join("run-manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["status"], "complete");
}

struct SelectivelyFailingStore {
    fail_chain: Option<String>,
    chains: Arc<Mutex<Vec<String>>>,
}

impl FeatureStoreReader for SelectivelyFailingStore {
    fn load_snapshot(
        &self,
        chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        self.chains.lock().unwrap().push(chain.to_string());
        if self.fail_chain.as_deref() == Some(chain) {
            return Err(AppError::InvalidData(format!("{chain} fixture failure")));
        }
        Ok(DatabaseSnapshot::default())
    }

    fn chain_totals(&self, _chain: &str) -> Result<ChainTotalsPayload, AppError> {
        Ok(ChainTotalsPayload {
            total_nfts: 1_000,
            total_contracts: 100,
        })
    }
}

#[tokio::test]
async fn partial_failure_recomputes_all_four_scopes_at_the_whole_seed_boundary() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        "chain,address\nethereum,0x1111111111111111111111111111111111111111\n",
    )
    .unwrap();
    let output_dir = dir.path().join("output");
    let first_seen = Arc::new(Mutex::new(Vec::new()));
    let api = Arc::new(SeedOnlyApi {
        metadata_calls: Arc::new(AtomicUsize::new(0)),
    });
    let first = run_multichain_batch(
        MultiChainBatchRequest {
            seed_file: seed_file.clone(),
            output_dir: output_dir.clone(),
            ..MultiChainBatchRequest::default()
        },
        &AnalysisDeps {
            api: api.clone(),
            feature_store: Arc::new(SelectivelyFailingStore {
                fail_chain: Some("solana".into()),
                chains: first_seen,
            }),
            progress: Arc::new(NoopProgressReporter),
            batch_progress: Arc::new(NoopBatchProgressReporter),
        },
    )
    .await
    .unwrap();
    assert_eq!(first.failures.len(), 1);
    assert_eq!(first.failures[0].secondary_chain, "solana");
    assert!(output_dir.join("failures.json").exists());
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_dir.join("run-manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["status"], "incomplete");
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let retry_seen = Arc::new(Mutex::new(Vec::new()));
    let second = run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir,
            ..MultiChainBatchRequest::default()
        },
        &AnalysisDeps {
            api,
            feature_store: Arc::new(SelectivelyFailingStore {
                fail_chain: None,
                chains: retry_seen.clone(),
            }),
            progress: Arc::new(NoopProgressReporter),
            batch_progress: Arc::new(NoopBatchProgressReporter),
        },
    )
    .await
    .unwrap();

    assert!(second.failures.is_empty());
    assert_eq!(
        *retry_seen.lock().unwrap(),
        ["ethereum", "base", "polygon", "solana"]
    );
}

struct CountingIdentityStore {
    identity_calls: Arc<AtomicUsize>,
}

impl FeatureStoreReader for CountingIdentityStore {
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

    fn chain_totals(&self, _chain: &str) -> Result<ChainTotalsPayload, AppError> {
        Ok(ChainTotalsPayload {
            total_nfts: 1_000,
            total_contracts: 100,
        })
    }

    fn snapshot_identity(&self, chain: &str) -> Result<String, AppError> {
        self.identity_calls.fetch_add(1, Ordering::SeqCst);
        Ok(format!("{chain}:snapshot"))
    }
}

#[tokio::test]
async fn snapshot_identity_is_computed_once_per_chain_before_seed_tasks() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        concat!(
            "chain,address\n",
            "ethereum,0x1111111111111111111111111111111111111111\n",
            "ethereum,0x2222222222222222222222222222222222222222\n",
            "ethereum,0x3333333333333333333333333333333333333333\n",
        ),
    )
    .unwrap();
    let identity_calls = Arc::new(AtomicUsize::new(0));

    run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir: dir.path().join("output"),
            ..MultiChainBatchRequest::default()
        },
        &AnalysisDeps {
            api: Arc::new(SeedOnlyApi {
                metadata_calls: Arc::new(AtomicUsize::new(0)),
            }),
            feature_store: Arc::new(CountingIdentityStore {
                identity_calls: identity_calls.clone(),
            }),
            progress: Arc::new(NoopProgressReporter),
            batch_progress: Arc::new(NoopBatchProgressReporter),
        },
    )
    .await
    .unwrap();

    assert_eq!(identity_calls.load(Ordering::SeqCst), Chain::ALL.len());
}

struct VersionedSnapshotStore {
    identity: String,
}

impl FeatureStoreReader for VersionedSnapshotStore {
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

    fn chain_totals(&self, _chain: &str) -> Result<ChainTotalsPayload, AppError> {
        Ok(ChainTotalsPayload {
            total_nfts: 1_000,
            total_contracts: 100,
        })
    }

    fn snapshot_identity(&self, chain: &str) -> Result<String, AppError> {
        Ok(format!("{chain}:{}", self.identity))
    }
}

#[tokio::test]
async fn cache_is_invalidated_when_snapshot_identity_or_analysis_config_changes() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        "chain,address\nethereum,0x1111111111111111111111111111111111111111\n",
    )
    .unwrap();
    let metadata_calls = Arc::new(AtomicUsize::new(0));
    let api = Arc::new(SeedOnlyApi {
        metadata_calls: metadata_calls.clone(),
    });
    let request = MultiChainBatchRequest {
        seed_file,
        output_dir: dir.path().join("output"),
        paper_stats_config: top_contract_analysis_rs::analysis::paper_stats::PaperStatsConfig {
            analysis_timestamp: 1_700_000_000,
            ..Default::default()
        },
        ..MultiChainBatchRequest::default()
    };

    for identity in ["snapshot-a", "snapshot-b"] {
        run_multichain_batch(
            request.clone(),
            &AnalysisDeps {
                api: api.clone(),
                feature_store: Arc::new(VersionedSnapshotStore {
                    identity: identity.into(),
                }),
                progress: Arc::new(NoopProgressReporter),
                batch_progress: Arc::new(NoopBatchProgressReporter),
            },
        )
        .await
        .unwrap();
    }

    run_multichain_batch(
        MultiChainBatchRequest {
            max_history_transactions_per_asset: 101,
            ..request
        },
        &AnalysisDeps {
            api,
            feature_store: Arc::new(VersionedSnapshotStore {
                identity: "snapshot-b".into(),
            }),
            progress: Arc::new(NoopProgressReporter),
            batch_progress: Arc::new(NoopBatchProgressReporter),
        },
    )
    .await
    .unwrap();

    assert_eq!(metadata_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn solana_cache_filenames_are_case_safe_and_collision_free() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        concat!(
            "chain,address\n",
            "solana,So11111111111111111111111111111111111111112\n",
            "solana,so11111111111111111111111111111111111111112\n",
        ),
    )
    .unwrap();
    let output_dir = dir.path().join("output");
    run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir: output_dir.clone(),
            ..MultiChainBatchRequest::default()
        },
        &AnalysisDeps {
            api: Arc::new(SeedOnlyApi {
                metadata_calls: Arc::new(AtomicUsize::new(0)),
            }),
            feature_store: Arc::new(EmptyCrossChainStore {
                chains: Arc::new(Mutex::new(Vec::new())),
            }),
            progress: Arc::new(NoopProgressReporter),
            batch_progress: Arc::new(NoopBatchProgressReporter),
        },
    )
    .await
    .unwrap();

    let cache_names = std::fs::read_dir(output_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.contains("__vs__"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(cache_names.len(), 8);
    assert!(cache_names
        .iter()
        .all(|name| name == &name.to_ascii_lowercase()));
    assert!(cache_names.iter().all(|name| !name.contains("So111")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multichain_batch_overlaps_seed_contexts_up_to_default_network_limit() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        concat!(
            "chain,address\n",
            "ethereum,0x1111111111111111111111111111111111111111\n",
            "ethereum,0x2222222222222222222222222222222222222222\n",
            "ethereum,0x3333333333333333333333333333333333333333\n",
        ),
    )
    .unwrap();
    let current = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let deps = AnalysisDeps {
        api: Arc::new(ConcurrentSeedApi {
            metadata_current: current,
            metadata_max_seen: max_seen.clone(),
            wait_for_snapshot_address: None,
            snapshot_started: Arc::new(AtomicUsize::new(0)),
            matched_current: Arc::new(AtomicUsize::new(0)),
            matched_max_seen: Arc::new(AtomicUsize::new(0)),
            quality_started: Arc::new(AtomicUsize::new(0)),
            quality_release: None,
        }),
        feature_store: Arc::new(EmptyCrossChainStore {
            chains: Arc::new(Mutex::new(Vec::new())),
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir: dir.path().join("output"),
            ..MultiChainBatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(max_seen.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multichain_batch_limits_cpu_stage_and_overlaps_pipeline_without_parallelizing_seed_chains()
{
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    let third_seed = "0x3333333333333333333333333333333333333333";
    std::fs::write(
        &seed_file,
        format!(
            "chain,address\nethereum,0x1111111111111111111111111111111111111111\nethereum,0x2222222222222222222222222222222222222222\nethereum,{third_seed}\n"
        ),
    )
    .unwrap();
    let network_current = Arc::new(AtomicUsize::new(0));
    let snapshot_started = Arc::new(AtomicUsize::new(0));
    let cpu_max_seen = Arc::new(AtomicUsize::new(0));
    let same_seed_max_seen = Arc::new(AtomicUsize::new(0));
    let observed_stage_overlap = Arc::new(AtomicUsize::new(0));
    let deps = AnalysisDeps {
        api: Arc::new(ConcurrentSeedApi {
            metadata_current: network_current.clone(),
            metadata_max_seen: Arc::new(AtomicUsize::new(0)),
            wait_for_snapshot_address: Some(third_seed.to_string()),
            snapshot_started: snapshot_started.clone(),
            matched_current: Arc::new(AtomicUsize::new(0)),
            matched_max_seen: Arc::new(AtomicUsize::new(0)),
            quality_started: Arc::new(AtomicUsize::new(0)),
            quality_release: None,
        }),
        feature_store: Arc::new(ConcurrentSnapshotStore {
            current: Arc::new(AtomicUsize::new(0)),
            max_seen: cpu_max_seen.clone(),
            same_seed_current: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            same_seed_max_seen: same_seed_max_seen.clone(),
            network_current,
            snapshot_started,
            observed_stage_overlap: observed_stage_overlap.clone(),
            emit_candidate: false,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir: dir.path().join("output"),
            seed_network_max_concurrency: 2,
            seed_cpu_max_concurrency: 2,
            ..MultiChainBatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(cpu_max_seen.load(Ordering::SeqCst), 2);
    assert_eq!(same_seed_max_seen.load(Ordering::SeqCst), 1);
    assert_eq!(observed_stage_overlap.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multichain_batch_shares_matched_contract_limit_across_seeds() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    std::fs::write(
        &seed_file,
        concat!(
            "chain,address\n",
            "ethereum,0x1111111111111111111111111111111111111111\n",
            "ethereum,0x2222222222222222222222222222222222222222\n",
            "ethereum,0x3333333333333333333333333333333333333333\n",
        ),
    )
    .unwrap();
    let matched_current = Arc::new(AtomicUsize::new(0));
    let matched_max_seen = Arc::new(AtomicUsize::new(0));
    let snapshot_started = Arc::new(AtomicUsize::new(0));
    let deps = AnalysisDeps {
        api: Arc::new(ConcurrentSeedApi {
            metadata_current: Arc::new(AtomicUsize::new(0)),
            metadata_max_seen: Arc::new(AtomicUsize::new(0)),
            wait_for_snapshot_address: None,
            snapshot_started: snapshot_started.clone(),
            matched_current,
            matched_max_seen: matched_max_seen.clone(),
            quality_started: Arc::new(AtomicUsize::new(0)),
            quality_release: None,
        }),
        feature_store: Arc::new(ConcurrentSnapshotStore {
            current: Arc::new(AtomicUsize::new(0)),
            max_seen: Arc::new(AtomicUsize::new(0)),
            same_seed_current: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            same_seed_max_seen: Arc::new(AtomicUsize::new(0)),
            network_current: Arc::new(AtomicUsize::new(0)),
            snapshot_started,
            observed_stage_overlap: Arc::new(AtomicUsize::new(0)),
            emit_candidate: true,
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir: dir.path().join("output"),
            seed_network_max_concurrency: 3,
            seed_cpu_max_concurrency: 3,
            matched_contract_max_concurrency: 2,
            ..MultiChainBatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(matched_max_seen.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn multichain_batch_caps_large_seed_pipeline_backlog_at_eight() {
    let dir = tempdir().unwrap();
    let seed_file = dir.path().join("seeds.csv");
    let mut csv = String::from("chain,address\n");
    for index in 1..=12 {
        csv.push_str(&format!("ethereum,0x{index:040x}\n"));
    }
    std::fs::write(&seed_file, csv).unwrap();
    let max_seen = Arc::new(AtomicUsize::new(0));
    let deps = AnalysisDeps {
        api: Arc::new(ConcurrentSeedApi {
            metadata_current: Arc::new(AtomicUsize::new(0)),
            metadata_max_seen: max_seen.clone(),
            wait_for_snapshot_address: None,
            snapshot_started: Arc::new(AtomicUsize::new(0)),
            matched_current: Arc::new(AtomicUsize::new(0)),
            matched_max_seen: Arc::new(AtomicUsize::new(0)),
            quality_started: Arc::new(AtomicUsize::new(0)),
            quality_release: None,
        }),
        feature_store: Arc::new(EmptyCrossChainStore {
            chains: Arc::new(Mutex::new(Vec::new())),
        }),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    run_multichain_batch(
        MultiChainBatchRequest {
            seed_file,
            output_dir: dir.path().join("output"),
            seed_network_max_concurrency: 100,
            seed_cpu_max_concurrency: 100,
            matched_contract_max_concurrency: 100,
            ..MultiChainBatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(max_seen.load(Ordering::SeqCst), 8);
}
