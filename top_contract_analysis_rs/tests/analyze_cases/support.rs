use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tempfile::tempdir;
use tokio::time::{sleep, Duration};
use top_contract_analysis_rs::analysis::{
    analyze_seed_contract, AnalysisDeps, AnalyzeApi, AnalyzeRequest, CandidateSeedHolderRequest,
    FeatureStoreReader,
};
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::models::{
    AddressSignalPayload, DuplicateContractPayload, HonestAddressPayload, MaliciousAddressPayload,
    SecondarySaleVictimAddressPayload, SeedCollectionStatsPayload, SeedContractPayload,
    SingleReportPayload, VictimSignalPayload,
};
use top_contract_analysis_rs::models::{
    ContractMetadata, ContractNameRecord, DatabaseNftRecord, DatabaseSnapshot, EthTransferRecord,
    NftMarketEventRecord, NftSaleRecord, OwnerBalance, SeedNft, TransactionReceiptRecord,
    TransferRecord, ZERO_ADDRESS,
};
use top_contract_analysis_rs::progress::{NoopBatchProgressReporter, NoopProgressReporter};
use top_contract_analysis_rs::reporting::{
    default_output_basename, render_human_readable_report, write_outputs_to_directory,
};

struct FakeFeatureStore {
    snapshot: DatabaseSnapshot,
}

impl FeatureStoreReader for FakeFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        Ok(self.snapshot.clone())
    }
}

#[derive(Default)]
struct CapturingFeatureStore {
    captured_seed_names: Arc<Mutex<Vec<Vec<String>>>>,
}

impl FeatureStoreReader for CapturingFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        self.captured_seed_names
            .lock()
            .unwrap()
            .push(seed_nfts.iter().map(|item| item.name.clone()).collect());
        Ok(DatabaseSnapshot::default())
    }
}

struct FakeApi;

#[async_trait]
impl AnalyzeApi for FakeApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: "0xowner".into(),
            admin_address: "0xadmin".into(),
            proxy_admin_address: "0xproxyadmin".into(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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

struct WarmCountingApi<T> {
    inner: T,
    warm_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl<T> AnalyzeApi for WarmCountingApi<T>
where
    T: AnalyzeApi,
{
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        self.inner
            .fetch_contract_metadata(
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
        self.inner
            .fetch_seed_contract_nfts(chain, alchemy_api_key, alchemy_network, contract_address)
            .await
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
        self.inner
            .fetch_contract_nfts(
                chain,
                alchemy_api_key,
                alchemy_network,
                etherscan_api_key,
                opensea_api_key,
                contract_address,
            )
            .await
    }

    async fn fetch_license_sample(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        seed_nfts: &[SeedNft],
    ) -> Result<bool, AppError> {
        self.inner
            .fetch_license_sample(chain, alchemy_api_key, alchemy_network, seed_nfts)
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
        self.inner
            .fetch_contract_transfers(
                chain,
                etherscan_api_key,
                alchemy_network,
                alchemy_api_key,
                contract_address,
                token_type,
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
        self.inner
            .fetch_contract_owners(chain, alchemy_api_key, alchemy_network, contract_address)
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
        self.inner
            .fetch_contract_sales(
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
        self.inner
            .fetch_transaction_receipt(alchemy_api_key, alchemy_network, tx_hash)
            .await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        self.inner
            .fetch_transaction_receipts_for_block(alchemy_api_key, alchemy_network, block_number)
            .await
    }

    async fn fetch_eth_balance(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        self.inner
            .fetch_eth_balance(alchemy_api_key, alchemy_network, address, block_number)
            .await
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.inner
            .fetch_same_block_eth_transfers_for_address(
                alchemy_api_key,
                alchemy_network,
                block_number,
                address,
            )
            .await
    }

    async fn warm_eth_usd_rate(&self) -> Result<(), AppError> {
        self.warm_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.warm_eth_usd_rate().await
    }
}

struct FakeSeedOwnerApi;

#[async_trait]
impl AnalyzeApi for FakeSeedOwnerApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        AnalyzeApi::fetch_contract_metadata(
            &FakeApi,
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
            &FakeApi,
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
            &FakeApi,
            chain,
            etherscan_api_key,
            alchemy_network,
            alchemy_api_key,
            contract_address,
            token_type,
        )
        .await
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        if contract_address.eq_ignore_ascii_case("0xseed") {
            return Ok(vec![OwnerBalance {
                owner_address: "0xwrapped".into(),
                token_balances: BTreeMap::from([("1".into(), 1)]),
            }]);
        }
        Ok(vec![])
    }

    async fn candidate_currently_holds_seed_nft(
        &self,
        request: CandidateSeedHolderRequest<'_>,
    ) -> Result<Option<bool>, AppError> {
        Ok(Some(
            request
                .candidate_contract_address
                .eq_ignore_ascii_case("0xwrapped"),
        ))
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
            &FakeApi,
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
        AnalyzeApi::fetch_transaction_receipt(&FakeApi, alchemy_api_key, alchemy_network, tx_hash)
            .await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        AnalyzeApi::fetch_transaction_receipts_for_block(
            &FakeApi,
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
            &FakeApi,
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
            &FakeApi,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }
}

struct FakeSeedTransferHistoryApi;

#[async_trait]
impl AnalyzeApi for FakeSeedTransferHistoryApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        AnalyzeApi::fetch_contract_metadata(
            &FakeApi,
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
            &FakeApi,
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
        if contract_address.eq_ignore_ascii_case("0xseed") {
            return Ok(vec![TransferRecord::transfer(
                "0xseed",
                "1",
                100,
                "0xowner",
                "0xwrapped",
            )]);
        }
        AnalyzeApi::fetch_contract_transfers(
            &FakeApi,
            chain,
            etherscan_api_key,
            alchemy_network,
            alchemy_api_key,
            contract_address,
            token_type,
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
            &FakeApi,
            chain,
            alchemy_api_key,
            alchemy_network,
            contract_address,
        )
        .await
    }

    async fn candidate_currently_holds_seed_nft(
        &self,
        _request: CandidateSeedHolderRequest<'_>,
    ) -> Result<Option<bool>, AppError> {
        Ok(Some(false))
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
            &FakeApi,
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
        AnalyzeApi::fetch_transaction_receipt(&FakeApi, alchemy_api_key, alchemy_network, tx_hash)
            .await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        AnalyzeApi::fetch_transaction_receipts_for_block(
            &FakeApi,
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
            &FakeApi,
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
            &FakeApi,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }
}

struct FakeTwoTokenOwnersApi;

#[async_trait]
impl AnalyzeApi for FakeTwoTokenOwnersApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        AnalyzeApi::fetch_contract_metadata(
            &FakeApi,
            chain,
            alchemy_api_key,
            alchemy_network,
            _opensea_api_key,
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
            &FakeApi,
            chain,
            alchemy_api_key,
            alchemy_network,
            contract_address,
        )
        .await
    }

    async fn fetch_contract_nfts(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        _etherscan_api_key: &str,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        if contract_address != "0xdup" {
            return AnalyzeApi::fetch_seed_contract_nfts(
                &FakeApi,
                chain,
                alchemy_api_key,
                alchemy_network,
                contract_address,
            )
            .await;
        }

        Ok(vec![
            SeedNft {
                chain: chain.to_string(),
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                name: "Azuki Mirror #1".into(),
                symbol: "AZUKI".into(),
                token_uri: "ipfs://seed/1".into(),
                image_uri: "ipfs://image/1.png".into(),
                metadata_json: r#"{"description":"gold dragon"}"#.into(),
            },
            SeedNft {
                chain: chain.to_string(),
                contract_address: contract_address.to_string(),
                token_id: "2".into(),
                name: "Azuki Mirror #2".into(),
                symbol: "AZUKI".into(),
                token_uri: "ipfs://candidate/2".into(),
                image_uri: "ipfs://candidate/2.png".into(),
                metadata_json: r#"{"description":"different trait"}"#.into(),
            },
        ])
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
        Ok(vec![
            TransferRecord::mint(contract_address, "1", 100, "0xminter1"),
            TransferRecord::mint(contract_address, "2", 110, "0xminter2"),
        ])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![
            OwnerBalance {
                owner_address: "0xholder1".into(),
                token_balances: BTreeMap::from([("1".into(), 1)]),
            },
            OwnerBalance {
                owner_address: "0xholder2".into(),
                token_balances: BTreeMap::from([("2".into(), 1)]),
            },
        ])
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
            &FakeApi,
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
        AnalyzeApi::fetch_transaction_receipt(&FakeApi, alchemy_api_key, alchemy_network, tx_hash)
            .await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        AnalyzeApi::fetch_transaction_receipts_for_block(
            &FakeApi,
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
            &FakeApi,
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
            &FakeApi,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }
}

struct FakeOpenLicenseApi;

struct FakeEmptyContractNftsApi;

#[async_trait]
impl AnalyzeApi for FakeEmptyContractNftsApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        AnalyzeApi::fetch_contract_metadata(
            &FakeTwoTokenOwnersApi,
            chain,
            alchemy_api_key,
            alchemy_network,
            _opensea_api_key,
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
            &FakeTwoTokenOwnersApi,
            chain,
            alchemy_api_key,
            alchemy_network,
            contract_address,
        )
        .await
    }

    async fn fetch_contract_nfts(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _etherscan_api_key: &str,
        _opensea_api_key: &str,
        _contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![])
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
            &FakeTwoTokenOwnersApi,
            chain,
            etherscan_api_key,
            alchemy_network,
            alchemy_api_key,
            contract_address,
            token_type,
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
            &FakeTwoTokenOwnersApi,
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
            &FakeTwoTokenOwnersApi,
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
            &FakeTwoTokenOwnersApi,
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
            &FakeTwoTokenOwnersApi,
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
            &FakeTwoTokenOwnersApi,
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
            &FakeTwoTokenOwnersApi,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }
}

#[async_trait]
impl AnalyzeApi for FakeOpenLicenseApi {
    async fn fetch_contract_metadata(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<ContractMetadata, AppError> {
        AnalyzeApi::fetch_contract_metadata(
            &FakeApi,
            chain,
            alchemy_api_key,
            alchemy_network,
            _opensea_api_key,
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
            &FakeApi,
            chain,
            alchemy_api_key,
            alchemy_network,
            contract_address,
        )
        .await
    }

    async fn fetch_license_sample(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _seed_nfts: &[SeedNft],
    ) -> Result<bool, AppError> {
        Ok(true)
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
            &FakeApi,
            chain,
            etherscan_api_key,
            alchemy_network,
            alchemy_api_key,
            contract_address,
            token_type,
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
            &FakeApi,
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
            &FakeApi,
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
        AnalyzeApi::fetch_transaction_receipt(&FakeApi, alchemy_api_key, alchemy_network, tx_hash)
            .await
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        AnalyzeApi::fetch_transaction_receipts_for_block(
            &FakeApi,
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
            &FakeApi,
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
            &FakeApi,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }
}

struct FakeEnrichedApi;

#[async_trait]
impl AnalyzeApi for FakeEnrichedApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: "0xowner".into(),
            admin_address: "0xadmin".into(),
            proxy_admin_address: "0xproxyadmin".into(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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
        Ok(vec![
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmint".into(),
                log_index: 0,
                block_number: 1,
                block_time: 100,
                from_address: "0x0000000000000000000000000000000000000000".into(),
                to_address: "0xminter".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmove".into(),
                log_index: 1,
                block_number: 2,
                block_time: 120,
                from_address: "0xminter".into(),
                to_address: "0xholder".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xsale".into(),
                log_index: 2,
                block_number: 2,
                block_time: 150,
                from_address: "0xminter".into(),
                to_address: "0xvictim".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
        ])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![OwnerBalance {
            owner_address: "0xholder".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![NftSaleRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xsale".into(),
            block_number: 2,
            log_index: 0,
            bundle_index: 0,
            buyer_address: "0xvictim".into(),
            seller_address: "0xminter".into(),
            marketplace: "opensea".into(),
            taker: "buyer".into(),
            payment_token_symbol: "ETH".into(),
            payment_token_address: "0x0000000000000000000000000000000000000000".into(),
            price_eth: Some(1.5),
            price_usd: Some(1.5),
            seller_fee_eth: 0.0,
            seller_fee_usd: 0.0,
            protocol_fee_eth: 0.0,
            protocol_fee_usd: 0.0,
            royalty_fee_eth: 0.0,
            royalty_fee_usd: 0.0,
            royalty_recipient_address: String::new(),
            source: "opensea".into(),
            is_native_eth: true,
        }])
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

    async fn fetch_mint_payment_eth_transfers_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        if block_number == 1 && address == "0xminter" {
            return Ok(vec![
                EthTransferRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    from_address: "0xfunder".into(),
                    to_address: "0xminter".into(),
                    value_eth: 0.08,
                    value_usd: Some(184.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
                EthTransferRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    from_address: "0xminter".into(),
                    to_address: "0xcreator".into(),
                    value_eth: 0.08,
                    value_usd: Some(184.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
            ]);
        }
        if block_number == 1 && address == "0xdup" {
            return Ok(vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xdup".into(),
                to_address: "0xfunder".into(),
                value_eth: 0.04,
                value_usd: Some(92.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "internal".into(),
            }]);
        }
        Ok(vec![])
    }

    async fn fetch_mint_payment_eth_transfers_to_address_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        if block_number == 1 && address == "0xcreator" {
            return Ok(vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xminter".into(),
                to_address: "0xcreator".into(),
                value_eth: 0.08,
                value_usd: Some(184.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }]);
        }
        Ok(vec![])
    }
}

struct PreSeedDeploymentApi {
    contract_nft_calls: AtomicUsize,
    total_supply_calls: AtomicUsize,
    transfer_calls: AtomicUsize,
    owner_calls: AtomicUsize,
    sale_calls: AtomicUsize,
}

#[async_trait]
impl AnalyzeApi for PreSeedDeploymentApi {
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
            deployed_block_number: if contract_address == "0xseed" {
                200
            } else {
                100
            },
            deployed_block_time: 0,
            owner_address: "0xowner".into(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1"}"#.into(),
        }])
    }

    async fn fetch_contract_nfts(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _etherscan_api_key: &str,
        _opensea_api_key: &str,
        _contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        self.contract_nft_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }

    async fn fetch_contract_total_supply(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Option<u64>, AppError> {
        self.total_supply_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(2))
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
        if contract_address != "0xseed" {
            self.transfer_calls.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Vec::new())
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        self.owner_calls.fetch_add(1, Ordering::SeqCst);
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
        self.sale_calls.fetch_add(1, Ordering::SeqCst);
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
        Ok(Vec::new())
    }
}

struct SupplyMismatchApi {
    current_total_supply: u64,
    contract_nft_calls: AtomicUsize,
    total_supply_calls: AtomicUsize,
    transfer_calls: AtomicUsize,
    owner_calls: AtomicUsize,
    sale_calls: AtomicUsize,
}

#[async_trait]
impl AnalyzeApi for SupplyMismatchApi {
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
            deployed_block_number: if contract_address == "0xseed" {
                100
            } else {
                200
            },
            deployed_block_time: 0,
            owner_address: "0xowner".into(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1"}"#.into(),
        }])
    }

    async fn fetch_contract_nfts(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _etherscan_api_key: &str,
        _opensea_api_key: &str,
        _contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        self.contract_nft_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }

    async fn fetch_contract_total_supply(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Option<u64>, AppError> {
        self.total_supply_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(self.current_total_supply))
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
        if contract_address != "0xseed" {
            self.transfer_calls.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Vec::new())
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        self.owner_calls.fetch_add(1, Ordering::SeqCst);
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
        self.sale_calls.fetch_add(1, Ordering::SeqCst);
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
        Ok(Vec::new())
    }
}

fn current_supply_snapshot_rows(token_count: u64) -> Vec<DatabaseNftRecord> {
    (1..=token_count)
        .map(|token_id| DatabaseNftRecord {
            contract_address: "0xdup".into(),
            token_id: token_id.to_string(),
            token_uri: if token_id == 1 {
                "ipfs://seed/1".into()
            } else {
                format!("ipfs://candidate/{token_id}")
            },
            image_uri: if token_id == 1 {
                "ipfs://image/1.png".into()
            } else {
                format!("ipfs://candidate/{token_id}.png")
            },
            name: format!("Azuki Mirror #{token_id}"),
            symbol: "AZUKI".into(),
            metadata_json: format!(r#"{{"name":"Azuki Mirror #{token_id}"}}"#),
            metadata_recall_checked: false,
            metadata_recall_match: false,
        })
        .collect()
}

#[derive(Default)]
struct FakeLegitApi {
    sales_calls: AtomicUsize,
}

#[async_trait]
impl AnalyzeApi for FakeLegitApi {
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
            contract_deployer: "0xminter".into(),
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        alchemy_api_key: &str,
        alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        AnalyzeApi::fetch_seed_contract_nfts(
            &FakeEnrichedApi,
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
            &FakeEnrichedApi,
            chain,
            etherscan_api_key,
            alchemy_network,
            alchemy_api_key,
            contract_address,
            token_type,
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
            &FakeEnrichedApi,
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
        self.sales_calls.fetch_add(1, Ordering::SeqCst);
        AnalyzeApi::fetch_contract_sales(
            &FakeEnrichedApi,
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
            &FakeEnrichedApi,
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
            &FakeEnrichedApi,
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
            &FakeEnrichedApi,
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
            &FakeEnrichedApi,
            alchemy_api_key,
            alchemy_network,
            block_number,
            address,
        )
        .await
    }
}

struct CountingApi {
    transfer_fetch_count: AtomicUsize,
    owner_fetch_count: AtomicUsize,
    seed_collection_slug: Option<String>,
    candidate_collection_slug: Option<String>,
}

impl CountingApi {
    fn new() -> Self {
        Self {
            transfer_fetch_count: AtomicUsize::new(0),
            owner_fetch_count: AtomicUsize::new(0),
            seed_collection_slug: None,
            candidate_collection_slug: None,
        }
    }

    fn with_seed_collection_slug(seed_collection_slug: &str) -> Self {
        Self {
            seed_collection_slug: Some(seed_collection_slug.to_string()),
            candidate_collection_slug: Some(seed_collection_slug.to_string()),
            ..Self::new()
        }
    }
}

#[async_trait]
impl AnalyzeApi for CountingApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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
        if contract_address.eq_ignore_ascii_case("0xseed") {
            return Ok(vec![]);
        }
        self.transfer_fetch_count.fetch_add(1, Ordering::SeqCst);
        Ok(vec![
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmint".into(),
                log_index: 0,
                block_number: 1,
                block_time: 100,
                from_address: "0x0000000000000000000000000000000000000000".into(),
                to_address: "0xminter".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xsale".into(),
                log_index: 1,
                block_number: 2,
                block_time: 110,
                from_address: "0xminter".into(),
                to_address: "0xvictim".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xgift".into(),
                log_index: 2,
                block_number: 3,
                block_time: 150,
                from_address: "0xvictim".into(),
                to_address: "0xhonest".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
        ])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        self.owner_fetch_count.fetch_add(1, Ordering::SeqCst);
        Ok(vec![OwnerBalance {
            owner_address: "0xhonest".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }])
    }

    async fn candidate_currently_holds_seed_nft(
        &self,
        _request: CandidateSeedHolderRequest<'_>,
    ) -> Result<Option<bool>, AppError> {
        Ok(Some(false))
    }

    async fn fetch_seed_collection_slug(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        _seed_contract_address: &str,
    ) -> Result<Option<String>, AppError> {
        Ok(self.seed_collection_slug.clone())
    }

    async fn fetch_contract_collection_slug(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        contract_address: &str,
    ) -> Result<Option<String>, AppError> {
        if contract_address.eq_ignore_ascii_case("0xseed") {
            Ok(self.seed_collection_slug.clone())
        } else {
            Ok(self.candidate_collection_slug.clone())
        }
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![NftSaleRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xsale".into(),
            block_number: 2,
            log_index: 0,
            bundle_index: 0,
            buyer_address: "0xvictim".into(),
            seller_address: "0xminter".into(),
            marketplace: "opensea".into(),
            taker: "buyer".into(),
            payment_token_symbol: "ETH".into(),
            payment_token_address: "0x0000000000000000000000000000000000000000".into(),
            price_eth: Some(1.5),
            price_usd: Some(1.5),
            seller_fee_eth: 0.0,
            seller_fee_usd: 0.0,
            protocol_fee_eth: 0.0,
            protocol_fee_usd: 0.0,
            royalty_fee_eth: 0.0,
            royalty_fee_usd: 0.0,
            royalty_recipient_address: String::new(),
            source: "opensea".into(),
            is_native_eth: true,
        }])
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

struct SecondaryVictimApi;

#[async_trait]
impl AnalyzeApi for SecondaryVictimApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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
        Ok(vec![
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmint".into(),
                log_index: 0,
                block_number: 1,
                block_time: 100,
                from_address: "0x0000000000000000000000000000000000000000".into(),
                to_address: "0xminter".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xsale".into(),
                log_index: 1,
                block_number: 2,
                block_time: 110,
                from_address: "0xminter".into(),
                to_address: "0xvictim".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
        ])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![OwnerBalance {
            owner_address: "0xvictim".into(),
            token_balances: BTreeMap::from([("1".into(), 1)]),
        }])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![NftSaleRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xsale".into(),
            block_number: 2,
            log_index: 0,
            bundle_index: 0,
            buyer_address: "0xvictim".into(),
            seller_address: "0xminter".into(),
            marketplace: "opensea".into(),
            taker: "buyer".into(),
            payment_token_symbol: "ETH".into(),
            payment_token_address: "0x0000000000000000000000000000000000000000".into(),
            price_eth: Some(1.5),
            price_usd: Some(1.5),
            seller_fee_eth: 0.0,
            seller_fee_usd: 0.0,
            protocol_fee_eth: 0.0,
            protocol_fee_usd: 0.0,
            royalty_fee_eth: 0.0,
            royalty_fee_usd: 0.0,
            royalty_recipient_address: String::new(),
            source: "opensea".into(),
            is_native_eth: true,
        }])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord {
            tx_hash: tx_hash.into(),
            block_number: 2,
            transaction_index: 3,
            from_address: "0xvictim".into(),
            gas_used: 21000,
            effective_gas_price_wei: 1_000_000_000,
        })
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(BTreeMap::from([(
            "0xprefund".into(),
            TransactionReceiptRecord {
                tx_hash: "0xprefund".into(),
                block_number: 2,
                transaction_index: 1,
                from_address: "0xother".into(),
                gas_used: 0,
                effective_gas_price_wei: 0,
            },
        )]))
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(1.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        Ok(vec![EthTransferRecord {
            tx_hash: "0xprefund".into(),
            block_number: 2,
            from_address: "0xother".into(),
            to_address: "0xvictim".into(),
            value_eth: 2.0,
            category: "external".into(),
            ..EthTransferRecord::default()
        }])
    }
}

struct MultiBuyerSameTxApi;

#[async_trait]
impl AnalyzeApi for MultiBuyerSameTxApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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
        Ok(vec![
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmint1".into(),
                log_index: 0,
                block_number: 1,
                block_time: 100,
                from_address: "0x0000000000000000000000000000000000000000".into(),
                to_address: "0xminter".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "2".into(),
                tx_hash: "0xmint2".into(),
                log_index: 1,
                block_number: 1,
                block_time: 101,
                from_address: "0x0000000000000000000000000000000000000000".into(),
                to_address: "0xminter".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xbundle".into(),
                log_index: 0,
                block_number: 2,
                block_time: 120,
                from_address: "0xminter".into(),
                to_address: "0xbuyer1".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "2".into(),
                tx_hash: "0xbundle".into(),
                log_index: 1,
                block_number: 2,
                block_time: 120,
                from_address: "0xminter".into(),
                to_address: "0xbuyer2".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
        ])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![
            OwnerBalance {
                owner_address: "0xbuyer1".into(),
                token_balances: BTreeMap::from([("1".into(), 1)]),
            },
            OwnerBalance {
                owner_address: "0xbuyer2".into(),
                token_balances: BTreeMap::from([("2".into(), 1)]),
            },
        ])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        Ok(vec![
            NftSaleRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xbundle".into(),
                block_number: 2,
                log_index: 0,
                bundle_index: 0,
                buyer_address: "0xbuyer1".into(),
                seller_address: "0xminter".into(),
                marketplace: "opensea".into(),
                payment_token_symbol: "ETH".into(),
                price_eth: Some(1.0),
                price_usd: Some(1.0),
                source: "opensea".into(),
                is_native_eth: true,
                ..NftSaleRecord::default()
            },
            NftSaleRecord {
                contract_address: contract_address.to_string(),
                token_id: "2".into(),
                tx_hash: "0xbundle".into(),
                block_number: 2,
                log_index: 1,
                bundle_index: 1,
                buyer_address: "0xbuyer2".into(),
                seller_address: "0xminter".into(),
                marketplace: "opensea".into(),
                payment_token_symbol: "ETH".into(),
                price_eth: Some(2.0),
                price_usd: Some(2.0),
                source: "opensea".into(),
                is_native_eth: true,
                ..NftSaleRecord::default()
            },
        ])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord {
            tx_hash: tx_hash.into(),
            block_number: 2,
            transaction_index: 3,
            from_address: "0xmarketplace".into(),
            gas_used: 0,
            effective_gas_price_wei: 0,
        })
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
        address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(match address {
            "0xbuyer1" => 1.0,
            "0xbuyer2" => 10.0,
            _ => 0.0,
        })
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

struct ConcurrentContractApi {
    active_transfer_fetches: AtomicUsize,
    max_transfer_fetches: AtomicUsize,
}

impl ConcurrentContractApi {
    fn new() -> Self {
        Self {
            active_transfer_fetches: AtomicUsize::new(0),
            max_transfer_fetches: AtomicUsize::new(0),
        }
    }
}

struct StaggeredExpansionApi {
    slow_expansion_done: AtomicUsize,
    transfer_before_slow_expansion_done: AtomicUsize,
}

impl StaggeredExpansionApi {
    fn new() -> Self {
        Self {
            slow_expansion_done: AtomicUsize::new(0),
            transfer_before_slow_expansion_done: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl AnalyzeApi for StaggeredExpansionApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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
        if contract_address == "0xdup1" {
            sleep(Duration::from_millis(120)).await;
            self.slow_expansion_done.store(1, Ordering::SeqCst);
        }
        Ok(vec![SeedNft {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            name: format!("Azuki Mirror {}", &contract_address[5..]),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"description":"gold dragon"}"#.into(),
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
        if contract_address == "0xdup2" && self.slow_expansion_done.load(Ordering::SeqCst) == 0 {
            self.transfer_before_slow_expansion_done
                .store(1, Ordering::SeqCst);
        }
        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: format!("0xmint-{contract_address}"),
            log_index: 0,
            block_number: 1,
            block_time: 100,
            from_address: "0x0000000000000000000000000000000000000000".into(),
            to_address: "0xminter".into(),
            event_type: "erc721".into(),
            source: "alchemy".into(),
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

#[async_trait]
impl AnalyzeApi for ConcurrentContractApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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
        let active = self.active_transfer_fetches.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_transfer_fetches
            .fetch_max(active, Ordering::SeqCst);
        sleep(Duration::from_millis(40)).await;
        self.active_transfer_fetches.fetch_sub(1, Ordering::SeqCst);

        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: format!("0xmint-{contract_address}"),
            log_index: 0,
            block_number: 1,
            block_time: 100,
            from_address: "0x0000000000000000000000000000000000000000".into(),
            to_address: "0xminter".into(),
            event_type: "erc721".into(),
            source: "alchemy".into(),
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

struct ObsoleteReceiptMetricProbeApi {
    active_receipts: AtomicUsize,
    max_receipts: AtomicUsize,
    duplicate_sale_tx: bool,
    same_buyer_history: bool,
    receipt_calls: AtomicUsize,
    balance_calls: AtomicUsize,
    same_block_transfer_calls: AtomicUsize,
}

impl ObsoleteReceiptMetricProbeApi {
    fn new() -> Self {
        Self {
            active_receipts: AtomicUsize::new(0),
            max_receipts: AtomicUsize::new(0),
            duplicate_sale_tx: false,
            same_buyer_history: false,
            receipt_calls: AtomicUsize::new(0),
            balance_calls: AtomicUsize::new(0),
            same_block_transfer_calls: AtomicUsize::new(0),
        }
    }

    fn with_duplicate_sale_tx() -> Self {
        Self {
            duplicate_sale_tx: true,
            ..Self::new()
        }
    }

    fn with_same_buyer_history() -> Self {
        Self {
            same_buyer_history: true,
            ..Self::new()
        }
    }
}

struct ConcurrentSingleContractFetchApi {
    active_fetches: AtomicUsize,
    max_fetches: AtomicUsize,
    active_post_signal_fetches: AtomicUsize,
    max_post_signal_fetches: AtomicUsize,
    market_event_fetch_count: AtomicUsize,
}

impl ConcurrentSingleContractFetchApi {
    fn new() -> Self {
        Self {
            active_fetches: AtomicUsize::new(0),
            max_fetches: AtomicUsize::new(0),
            active_post_signal_fetches: AtomicUsize::new(0),
            max_post_signal_fetches: AtomicUsize::new(0),
            market_event_fetch_count: AtomicUsize::new(0),
        }
    }

    async fn overlap_delay(&self) {
        let active = self.active_fetches.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_fetches.fetch_max(active, Ordering::SeqCst);
        sleep(Duration::from_millis(40)).await;
        self.active_fetches.fetch_sub(1, Ordering::SeqCst);
    }

    async fn post_signal_overlap_delay(&self) {
        let active = self
            .active_post_signal_fetches
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        self.max_post_signal_fetches
            .fetch_max(active, Ordering::SeqCst);
        sleep(Duration::from_millis(40)).await;
        self.active_post_signal_fetches
            .fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl AnalyzeApi for ConcurrentSingleContractFetchApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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
        self.overlap_delay().await;
        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            log_index: 0,
            block_number: 1,
            block_time: 100,
            from_address: "0x0000000000000000000000000000000000000000".into(),
            to_address: "0xminter".into(),
            event_type: "erc721".into(),
            source: "alchemy".into(),
        }])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        self.overlap_delay().await;
        Ok(vec![])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        self.post_signal_overlap_delay().await;
        Ok(vec![NftSaleRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xsale".into(),
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
            is_native_eth: false,
        }])
    }

    async fn fetch_contract_market_events(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftMarketEventRecord>, AppError> {
        self.market_event_fetch_count.fetch_add(1, Ordering::SeqCst);
        self.post_signal_overlap_delay().await;
        Ok(vec![NftMarketEventRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            event_type: "listing".into(),
            source: "opensea".into(),
            ..NftMarketEventRecord::default()
        }])
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

    async fn fetch_mint_payment_eth_transfers_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.post_signal_overlap_delay().await;
        if block_number == 1 && address == "0xminter" {
            return Ok(vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xminter".into(),
                to_address: "0xcreator".into(),
                value_eth: 0.08,
                value_usd: Some(184.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }]);
        }
        Ok(vec![])
    }

    async fn fetch_mint_payment_eth_transfers_to_address_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.post_signal_overlap_delay().await;
        if block_number == 1 && address == "0xcreator" {
            return Ok(vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xminter".into(),
                to_address: "0xcreator".into(),
                value_eth: 0.08,
                value_usd: Some(184.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }]);
        }
        Ok(vec![])
    }
}

struct DuplicateMintPaymentLookupApi {
    mint_transfer_calls: Mutex<Vec<(i64, String)>>,
    balance_calls: Mutex<Vec<(String, i64)>>,
    block_receipt_calls: Mutex<Vec<i64>>,
}

impl DuplicateMintPaymentLookupApi {
    fn new() -> Self {
        Self {
            mint_transfer_calls: Mutex::new(Vec::new()),
            balance_calls: Mutex::new(Vec::new()),
            block_receipt_calls: Mutex::new(Vec::new()),
        }
    }
}

const BINANCE_HOT_WALLET: &str = "0x28c6c06298d514db089934071355e5743bf21d60";
const TORNADO_CASH_1_ETH: &str = "0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936";
const ARBITRUM_ONE_BRIDGE: &str = "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a";

struct CashoutTraceApi {
    mint_transfer_calls: Mutex<Vec<(i64, String)>>,
}

impl CashoutTraceApi {
    fn new() -> Self {
        Self {
            mint_transfer_calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl AnalyzeApi for CashoutTraceApi {
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
            deployed_block_number: 90,
            deployed_block_time: 90,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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
        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            log_index: 0,
            block_number: 10,
            block_time: 100,
            from_address: ZERO_ADDRESS.into(),
            to_address: "0xminter".into(),
            event_type: "erc721".into(),
            source: "alchemy".into(),
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
        Ok(vec![])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord {
            tx_hash: tx_hash.into(),
            block_number: 10,
            transaction_index: 1,
            from_address: "0xminter".into(),
            gas_used: 21_000,
            effective_gas_price_wei: 1_000_000_000,
        })
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        Ok(BTreeMap::from([
            (
                "0xmint".into(),
                TransactionReceiptRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    transaction_index: 1,
                    from_address: "0xminter".into(),
                    gas_used: 21_000,
                    effective_gas_price_wei: 1_000_000_000,
                },
            ),
            (
                "0xbridge".into(),
                TransactionReceiptRecord {
                    tx_hash: "0xbridge".into(),
                    block_number,
                    transaction_index: 2,
                    from_address: "0xhop1".into(),
                    gas_used: 21_000,
                    effective_gas_price_wei: 1_000_000_000,
                },
            ),
        ]))
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _address: &str,
        _block_number: i64,
    ) -> Result<f64, AppError> {
        Ok(1.0)
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

    async fn fetch_mint_payment_eth_transfers_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.mint_transfer_calls
            .lock()
            .unwrap()
            .push((block_number, address.to_string()));
        let transfers = match address {
            "0xminter" => vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xminter".into(),
                to_address: "0xdup".into(),
                value_eth: 0.08,
                value_usd: Some(184.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }],
            "0xdup" => vec![
                EthTransferRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    from_address: "0xdup".into(),
                    to_address: "0xhop1".into(),
                    value_eth: 0.5,
                    value_usd: Some(1_150.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "internal".into(),
                },
                EthTransferRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    from_address: "0xdup".into(),
                    to_address: BINANCE_HOT_WALLET.into(),
                    value_eth: 0.2,
                    value_usd: Some(460.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
                EthTransferRecord {
                    tx_hash: "0xmint".into(),
                    block_number,
                    from_address: "0xdup".into(),
                    to_address: TORNADO_CASH_1_ETH.into(),
                    value_eth: 0.1,
                    value_usd: Some(230.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
            ],
            "0xhop1" => vec![
                EthTransferRecord {
                    tx_hash: "0xbridge".into(),
                    block_number,
                    from_address: "0xhop1".into(),
                    to_address: ARBITRUM_ONE_BRIDGE.into(),
                    value_eth: 0.49,
                    value_usd: Some(1_127.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
                EthTransferRecord {
                    tx_hash: "0xunrelated".into(),
                    block_number,
                    from_address: "0xhop1".into(),
                    to_address: "0xunrelatedrecipient".into(),
                    value_eth: 1.5,
                    value_usd: Some(3_450.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
            ],
            _ => vec![],
        };
        Ok(transfers)
    }

    async fn fetch_mint_payment_eth_transfers_to_address_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.mint_transfer_calls
            .lock()
            .unwrap()
            .push((block_number, address.to_string()));
        let transfers = match address {
            "0xdup" => vec![EthTransferRecord {
                tx_hash: "0xmint".into(),
                block_number,
                from_address: "0xminter".into(),
                to_address: "0xdup".into(),
                value_eth: 0.08,
                value_usd: Some(184.0),
                payment_token_symbol: "ETH".into(),
                payment_token_address: ZERO_ADDRESS.into(),
                category: "external".into(),
            }],
            _ => vec![],
        };
        Ok(transfers)
    }
}

#[async_trait]
impl AnalyzeApi for DuplicateMintPaymentLookupApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
        })
    }

    async fn fetch_seed_contract_nfts(
        &self,
        chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
    ) -> Result<Vec<SeedNft>, AppError> {
        Ok(vec![
            SeedNft {
                chain: chain.to_string(),
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                name: "Azuki #1".into(),
                symbol: "AZUKI".into(),
                token_uri: "ipfs://seed/1".into(),
                image_uri: "ipfs://image/1.png".into(),
                metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
            },
            SeedNft {
                chain: chain.to_string(),
                contract_address: contract_address.to_string(),
                token_id: "2".into(),
                name: "Azuki #2".into(),
                symbol: "AZUKI".into(),
                token_uri: "ipfs://seed/2".into(),
                image_uri: "ipfs://image/2.png".into(),
                metadata_json: r#"{"name":"Azuki #2","description":"gold dragon"}"#.into(),
            },
        ])
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
        Ok(vec![
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xmint1".into(),
                log_index: 0,
                block_number: 1,
                block_time: 100,
                from_address: ZERO_ADDRESS.into(),
                to_address: "0xminter".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
            TransferRecord {
                contract_address: contract_address.to_string(),
                token_id: "2".into(),
                tx_hash: "0xmint2".into(),
                log_index: 1,
                block_number: 1,
                block_time: 101,
                from_address: ZERO_ADDRESS.into(),
                to_address: "0xminter".into(),
                event_type: "erc721".into(),
                source: "alchemy".into(),
            },
        ])
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
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        Ok(TransactionReceiptRecord {
            tx_hash: tx_hash.into(),
            block_number: 1,
            transaction_index: 1,
            from_address: "0xminter".into(),
            gas_used: 21000,
            effective_gas_price_wei: 1_000_000_000,
        })
    }

    async fn fetch_transaction_receipts_for_block(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
    ) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
        self.block_receipt_calls.lock().unwrap().push(block_number);
        Ok(BTreeMap::new())
    }

    async fn fetch_eth_balance(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        address: &str,
        block_number: i64,
    ) -> Result<f64, AppError> {
        self.balance_calls
            .lock()
            .unwrap()
            .push((address.to_string(), block_number));
        Ok(1.0)
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

    async fn fetch_mint_payment_eth_transfers_to_address_on_chain(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        block_number: i64,
        address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.mint_transfer_calls
            .lock()
            .unwrap()
            .push((block_number, address.to_string()));
        if block_number == 1 && address == "0xcreator" {
            return Ok(vec![
                EthTransferRecord {
                    tx_hash: "0xmint1".into(),
                    block_number,
                    from_address: "0xminter".into(),
                    to_address: "0xcreator".into(),
                    value_eth: 0.08,
                    value_usd: Some(184.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
                EthTransferRecord {
                    tx_hash: "0xmint2".into(),
                    block_number,
                    from_address: "0xminter".into(),
                    to_address: "0xcreator".into(),
                    value_eth: 0.09,
                    value_usd: Some(207.0),
                    payment_token_symbol: "ETH".into(),
                    payment_token_address: ZERO_ADDRESS.into(),
                    category: "external".into(),
                },
            ]);
        }
        Ok(vec![])
    }
}

#[async_trait]
impl AnalyzeApi for ObsoleteReceiptMetricProbeApi {
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
            deployed_block_number: 123,
            deployed_block_time: 0,
            owner_address: String::new(),
            admin_address: String::new(),
            proxy_admin_address: String::new(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
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
            name: "Azuki #1".into(),
            symbol: "AZUKI".into(),
            token_uri: "ipfs://seed/1".into(),
            image_uri: "ipfs://image/1.png".into(),
            metadata_json: r#"{"name":"Azuki #1","description":"gold dragon"}"#.into(),
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
        Ok(vec![TransferRecord {
            contract_address: contract_address.to_string(),
            token_id: "1".into(),
            tx_hash: "0xmint".into(),
            log_index: 0,
            block_number: 1,
            block_time: 100,
            from_address: "0x0000000000000000000000000000000000000000".into(),
            to_address: "0xminter".into(),
            event_type: "erc721".into(),
            source: "alchemy".into(),
        }])
    }

    async fn fetch_contract_owners(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _contract_address: &str,
    ) -> Result<Vec<OwnerBalance>, AppError> {
        Ok(vec![
            OwnerBalance {
                owner_address: "0xvictim1".into(),
                token_balances: BTreeMap::from([("1".into(), 1)]),
            },
            OwnerBalance {
                owner_address: "0xvictim2".into(),
                token_balances: BTreeMap::from([("1".into(), 1)]),
            },
        ])
    }

    async fn fetch_contract_sales(
        &self,
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        contract_address: &str,
        _opensea_api_key: &str,
    ) -> Result<Vec<NftSaleRecord>, AppError> {
        let second_tx_hash = if self.duplicate_sale_tx {
            "0xsale1"
        } else {
            "0xsale2"
        };
        let second_buyer = if self.same_buyer_history {
            "0xvictim1"
        } else {
            "0xvictim2"
        };
        Ok(vec![
            NftSaleRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: "0xsale1".into(),
                block_number: 2,
                log_index: 0,
                bundle_index: 0,
                buyer_address: "0xvictim1".into(),
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
            },
            NftSaleRecord {
                contract_address: contract_address.to_string(),
                token_id: "1".into(),
                tx_hash: second_tx_hash.into(),
                block_number: 3,
                log_index: 0,
                bundle_index: 0,
                buyer_address: second_buyer.into(),
                seller_address: "0xminter".into(),
                marketplace: "opensea".into(),
                taker: "buyer".into(),
                payment_token_symbol: "ETH".into(),
                payment_token_address: "0x0000000000000000000000000000000000000000".into(),
                price_eth: Some(2.0),
                price_usd: Some(2.0),
                seller_fee_eth: 0.0,
                seller_fee_usd: 0.0,
                protocol_fee_eth: 0.0,
                protocol_fee_usd: 0.0,
                royalty_fee_eth: 0.0,
                royalty_fee_usd: 0.0,
                royalty_recipient_address: String::new(),
                source: "opensea".into(),
                is_native_eth: true,
            },
        ])
    }

    async fn fetch_transaction_receipt(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        tx_hash: &str,
    ) -> Result<TransactionReceiptRecord, AppError> {
        self.receipt_calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active_receipts.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_receipts.fetch_max(active, Ordering::SeqCst);
        sleep(Duration::from_millis(40)).await;
        self.active_receipts.fetch_sub(1, Ordering::SeqCst);

        Ok(TransactionReceiptRecord {
            tx_hash: tx_hash.into(),
            block_number: 2,
            transaction_index: 1,
            from_address: "0xvictim".into(),
            gas_used: 21000,
            effective_gas_price_wei: 1_000_000_000,
        })
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
        self.balance_calls.fetch_add(1, Ordering::SeqCst);
        Ok(5.0)
    }

    async fn fetch_same_block_eth_transfers_for_address(
        &self,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _block_number: i64,
        _address: &str,
    ) -> Result<Vec<EthTransferRecord>, AppError> {
        self.same_block_transfer_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(vec![])
    }
}

