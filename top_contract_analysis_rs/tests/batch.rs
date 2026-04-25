use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tempfile::tempdir;
use top_contract_analysis_rs::analysis::{
    run_batch, AnalysisDeps, AnalyzeApi, BatchRequest, FeatureStoreReader,
};
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::models::{
    AddressSignalPayload, BatchReportSummary, BatchSeedReportPayload, BatchSummaryPayload,
    ContractMetadata, DatabaseSnapshot, EthTransferRecord, HonestAddressPayload,
    InfringingTokenRecord, MaliciousAddressPayload, NftSaleRecord, OutputFilesPayload,
    OwnerBalance, ReportSummary, SeedContractPayload, SeedNft, SingleReportPayload,
    TransactionReceiptRecord, TransferRecord, VictimAddressPayload,
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
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        Ok(DatabaseSnapshot::default())
    }
}

fn cached_single_report(
    seed_contract: SeedContractPayload,
    report_summary: ReportSummary,
    infringing_tokens: Vec<InfringingTokenRecord>,
    malicious_addresses: Vec<MaliciousAddressPayload>,
    honest_addresses: Vec<HonestAddressPayload>,
    victim_addresses: Vec<VictimAddressPayload>,
    address_signals: BTreeMap<String, AddressSignalPayload>,
) -> SingleReportPayload {
    SingleReportPayload {
        seed_contract,
        report_summary,
        infringing_tokens,
        malicious_addresses,
        honest_addresses,
        victim_addresses,
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
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
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
            honest_address_count_total: 8,
            repeat_infringing_address_count_total: 3,
            repeat_infringing_address_count_global: 2,
            legit_duplicate_contract_count_total: 1,
            honest_purchase_total_eth_total: 12.5,
            stuck_cost_eth_total: 5.0,
            stuck_cost_ratio_overall: Some(0.4),
            buy_asset_ratio_known_address_count_total: 8,
            ratio_over_60_address_count_total: 3,
            ratio_over_60_address_ratio_overall: Some(0.375),
            ratio_over_80_address_count_total: 1,
            ratio_over_80_address_ratio_overall: Some(0.125),
            stuck_honest_address_count_total: 2,
            stuck_honest_address_ratio_overall: Some(0.25),
            corrupted_honest_address_count_total: 1,
            avg_seconds_to_honest_holder_mean: Some(12.5),
            median_seconds_to_honest_holder_median: Some(10.0),
            avg_mint_to_first_transfer_seconds_mean: Some(8.0),
            median_mint_to_first_transfer_seconds_median: Some(7.0),
            avg_unique_receiver_count_mean: Some(4.0),
            generated_at: "2026-04-17T00:00:00+00:00".into(),
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
                honest_address_count: 6,
                repeat_infringing_address_count: 1,
                legit_duplicate_contract_count: 1,
                honest_purchase_total_eth: 7.5,
                stuck_cost_eth: 2.5,
                stuck_cost_ratio: Some(1.0 / 3.0),
                ratio_over_60_address_count: 2,
                ratio_over_60_address_ratio: Some(0.5),
                stuck_honest_address_count: 1,
                stuck_honest_address_ratio: Some(0.25),
                corrupted_honest_address_count: 1,
                avg_seconds_to_honest_holder: Some(10.0),
                median_seconds_to_honest_holder: Some(9.0),
                median_mint_to_first_transfer_seconds: Some(8.0),
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
    assert!(markdown.contains("- 恶意地址总数: 7"));
    assert!(markdown.contains("- 诚实地址购买总金额(ETH/WETH)汇总: 12.5"));
    assert!(markdown.contains("- 套牢资金(ETH/WETH)汇总: 5 / 40.00%"));
    assert!(markdown.contains("- 买入金额占钱包总额 >60% 的地址数/总体占比: 3 / 37.50%"));
    assert!(markdown.contains("- 生成时间(UTC): 2026-04-17T00:00:00+00:00"));
    assert!(markdown.contains("## Seed 报告索引"));
    assert!(markdown.contains(
        "- Azuki (0xseed) | 重复合约=5 | 侵权NFT=4 | 恶意地址=5 | 诚实地址=6 | 多次侵权地址=1 | 官方参与=1 | 诚实购买额=7.5 | 套牢资金=2.5/33.33% | >60%=2/50.00% | 套牢=1/25.00% | 被腐化=1 | 诚实购买时长=10秒 | 传播中位数=9秒 | 首次转手中位数=8秒 | JSON=result/top_contract_analysis__azuki.json | MD=result/top_contract_analysis__azuki.md"
    ));
}

#[tokio::test]
async fn batch_skips_cached_seed_reports_in_output_directory() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n").unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached.json"),
        serde_json::json!({
            "seed_contract": {
                "chain": "ethereum",
                "contract_address": "0xseed1",
                "name": "Cached Seed"
            },
            "report_summary": {
                "candidate_contract_count": 1
            }
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("top_contract_analysis__cached.md"),
        "# cached\n",
    )
    .unwrap();
    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Box::new(EmptyFeatureStore),
        signal_cache: None,
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
            honest_purchase_total_eth: 10.0,
            buy_asset_ratio_known_address_count: 2,
            ratio_over_60_address_count: 1,
            ratio_over_80_address_count: 0,
            stuck_honest_address_count: 1,
            corrupted_honest_address_count: 1,
            avg_seconds_to_honest_holder: Some(12.0),
            avg_mint_to_first_transfer_seconds: Some(8.0),
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
            mint_to_honest_seconds_samples: vec![5, 15],
            ..HonestAddressPayload::default()
        }],
        vec![
            VictimAddressPayload {
                address: "0xv1".into(),
                last_buy_amount_eth: Some(2.0),
                is_stuck: true,
                ..VictimAddressPayload::default()
            },
            VictimAddressPayload {
                address: "0xv2".into(),
                last_buy_amount_eth: Some(4.0),
                is_stuck: false,
                ..VictimAddressPayload::default()
            },
        ],
        BTreeMap::from([
            (
                "0xa1".into(),
                AddressSignalPayload {
                    mint_to_first_transfer_seconds: 4,
                    ..AddressSignalPayload::default()
                },
            ),
            (
                "0xa2".into(),
                AddressSignalPayload {
                    mint_to_first_transfer_seconds: 10,
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
            honest_purchase_total_eth: 5.0,
            buy_asset_ratio_known_address_count: 3,
            ratio_over_60_address_count: 2,
            ratio_over_80_address_count: 1,
            stuck_honest_address_count: 1,
            avg_seconds_to_honest_holder: Some(18.0),
            avg_mint_to_first_transfer_seconds: Some(14.0),
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
                mint_to_honest_seconds_samples: vec![20],
                ..HonestAddressPayload::default()
            },
            HonestAddressPayload {
                address: "0xh2".into(),
                ..HonestAddressPayload::default()
            },
        ],
        vec![VictimAddressPayload {
            address: "0xv3".into(),
            last_buy_amount_eth: Some(1.0),
            is_stuck: true,
            ..VictimAddressPayload::default()
        }],
        BTreeMap::from([(
            "0xa3".into(),
            AddressSignalPayload {
                mint_to_first_transfer_seconds: 20,
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
        feature_store: Box::new(EmptyFeatureStore),
        signal_cache: None,
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
    assert_eq!(summary.batch_summary.honest_address_count_total, 2);
    assert_eq!(
        summary.batch_summary.repeat_infringing_address_count_total,
        1
    );
    assert_eq!(
        summary.batch_summary.repeat_infringing_address_count_global,
        1
    );
    assert_eq!(summary.batch_summary.honest_purchase_total_eth_total, 15.0);
    assert_eq!(summary.batch_summary.stuck_cost_eth_total, 3.0);
    assert_eq!(summary.batch_summary.stuck_cost_ratio_overall, Some(0.2));
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
    assert_eq!(summary.batch_summary.stuck_honest_address_count_total, 2);
    assert_eq!(
        summary.batch_summary.stuck_honest_address_ratio_overall,
        Some(0.4)
    );
    assert_eq!(
        summary.batch_summary.corrupted_honest_address_count_total,
        1
    );
    assert_eq!(
        summary.batch_summary.avg_seconds_to_honest_holder_mean,
        Some(15.0)
    );
    assert_eq!(
        summary.batch_summary.median_seconds_to_honest_holder_median,
        Some(15.0)
    );
    assert_eq!(
        summary
            .batch_summary
            .avg_mint_to_first_transfer_seconds_mean,
        Some(11.0)
    );
    assert_eq!(
        summary
            .batch_summary
            .median_mint_to_first_transfer_seconds_median,
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
        summary.seed_reports[0].report_summary.honest_address_count,
        1
    );
    assert_eq!(
        summary.seed_reports[0]
            .report_summary
            .repeat_infringing_address_count,
        1
    );
    assert_eq!(summary.seed_reports[0].report_summary.stuck_cost_eth, 2.0);
    assert_eq!(
        summary.seed_reports[0].report_summary.stuck_cost_ratio,
        Some(0.2)
    );
    assert_eq!(
        summary.seed_reports[0]
            .report_summary
            .median_seconds_to_honest_holder,
        Some(10.0)
    );
    assert_eq!(
        summary.seed_reports[0]
            .report_summary
            .median_mint_to_first_transfer_seconds,
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
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
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
async fn batch_uses_worker_concurrency_for_uncached_seeds() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n0xseed2\n0xseed3\n").unwrap();
    let api = Arc::new(SlowBatchApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Box::new(EmptyFeatureStore),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            workers: 2,
            ..BatchRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(api.max_seen.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn batch_progress_reporter_receives_seed_lifecycle_events() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("seeds.txt"), "0xseed1\n").unwrap();
    let batch_progress = Arc::new(RecordingBatchProgressReporter::default());
    let deps = AnalysisDeps {
        api: Arc::new(FakeBatchApi),
        feature_store: Box::new(EmptyFeatureStore),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: batch_progress.clone(),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            workers: 1,
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
    std::fs::write(
        dir.path().join("top_contract_analysis__cached.json"),
        serde_json::json!({
            "seed_contract": {
                "chain": "ethereum",
                "contract_address": "0xseed1",
                "name": "Cached Seed"
            },
            "report_summary": {
                "candidate_contract_count": 1
            }
        })
        .to_string(),
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
        feature_store: Box::new(EmptyFeatureStore),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: batch_progress.clone(),
    };

    let _summary = run_batch(
        BatchRequest {
            chain: "ethereum".into(),
            seed_file: dir.path().join("seeds.txt"),
            output_dir: dir.path().to_path_buf(),
            alchemy_api_key: "key".into(),
            workers: 1,
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
