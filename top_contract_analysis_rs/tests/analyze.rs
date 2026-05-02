use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tempfile::tempdir;
use tokio::time::{sleep, Duration};
use top_contract_analysis_rs::analysis::{
    analyze_seed_contract, AnalysisDeps, AnalyzeApi, AnalyzeRequest, FeatureStoreReader,
    SignalCacheStore,
};
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::models::{
    AddressSignalPayload, DuplicateContractPayload, FraudTradeStatsPayload, HonestAddressPayload,
    HonestAddressStatsPayload, MaliciousAddressPayload, ReportSummary, SeedCollectionStatsPayload,
    SeedContractPayload, SingleReportPayload, VictimAddressPayload, VictimSignalPayload,
};
use top_contract_analysis_rs::models::{
    ContractMetadata, ContractNameRecord, DatabaseNftRecord, DatabaseSnapshot, EthTransferRecord,
    NftSaleRecord, OwnerBalance, SeedNft, TransactionReceiptRecord, TransferRecord, ZERO_ADDRESS,
};
use top_contract_analysis_rs::progress::{NoopBatchProgressReporter, NoopProgressReporter};
use top_contract_analysis_rs::reporting::{
    default_output_basename, render_human_readable_report, write_outputs_to_directory,
};
use top_contract_analysis_rs::store::CachedSignals;

struct FakeFeatureStore {
    snapshot: DatabaseSnapshot,
}

impl FeatureStoreReader for FakeFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        Ok(self.snapshot.clone())
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
            metadata_doc: "gold dragon".into(),
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
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        _seed_contract_address: &str,
        candidate_contract_address: &str,
        _seed_collection_slug: Option<&str>,
    ) -> Result<Option<bool>, AppError> {
        Ok(Some(
            candidate_contract_address.eq_ignore_ascii_case("0xwrapped"),
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
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        _seed_contract_address: &str,
        _candidate_contract_address: &str,
        _seed_collection_slug: Option<&str>,
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
                metadata_doc: "gold dragon".into(),
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
                metadata_doc: "different trait".into(),
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
            metadata_doc: "gold dragon".into(),
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
                    ..EthTransferRecord::default()
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
                    ..EthTransferRecord::default()
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
                ..EthTransferRecord::default()
            }]);
        }
        Ok(vec![])
    }
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
}

impl CountingApi {
    fn new() -> Self {
        Self {
            transfer_fetch_count: AtomicUsize::new(0),
            owner_fetch_count: AtomicUsize::new(0),
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
            metadata_doc: "gold dragon".into(),
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
        _chain: &str,
        _alchemy_api_key: &str,
        _alchemy_network: Option<&str>,
        _opensea_api_key: &str,
        _seed_contract_address: &str,
        _candidate_contract_address: &str,
        _seed_collection_slug: Option<&str>,
    ) -> Result<Option<bool>, AppError> {
        Ok(Some(false))
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

struct FakeSaleMetricApi;

#[async_trait]
impl AnalyzeApi for FakeSaleMetricApi {
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
            metadata_doc: "gold dragon".into(),
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

struct MultiBuyerSameTxSaleMetricApi;

#[async_trait]
impl AnalyzeApi for MultiBuyerSameTxSaleMetricApi {
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
            metadata_doc: "gold dragon".into(),
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
            metadata_doc: "gold dragon".into(),
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
        if contract_address == "0xdup2" {
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
            metadata_doc: "gold dragon".into(),
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
        if contract_address == "0xdup1" && self.slow_expansion_done.load(Ordering::SeqCst) == 0 {
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
            metadata_doc: "gold dragon".into(),
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

struct ConcurrentSaleMetricApi {
    active_receipts: AtomicUsize,
    max_receipts: AtomicUsize,
    duplicate_sale_tx: bool,
    same_buyer_history: bool,
    receipt_calls: AtomicUsize,
    balance_calls: AtomicUsize,
    same_block_transfer_calls: AtomicUsize,
}

impl ConcurrentSaleMetricApi {
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
}

impl ConcurrentSingleContractFetchApi {
    fn new() -> Self {
        Self {
            active_fetches: AtomicUsize::new(0),
            max_fetches: AtomicUsize::new(0),
        }
    }

    async fn overlap_delay(&self) {
        let active = self.active_fetches.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_fetches.fetch_max(active, Ordering::SeqCst);
        sleep(Duration::from_millis(40)).await;
        self.active_fetches.fetch_sub(1, Ordering::SeqCst);
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
            metadata_doc: "gold dragon".into(),
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
        self.overlap_delay().await;
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
impl AnalyzeApi for ConcurrentSaleMetricApi {
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
            metadata_doc: "gold dragon".into(),
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

#[derive(Default)]
struct FakeSignalCache {
    rows: Mutex<BTreeMap<(String, String, String), CachedSignals>>,
}

impl SignalCacheStore for FakeSignalCache {
    fn get(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Option<CachedSignals>, AppError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&(
                chain.to_string(),
                contract_address.to_lowercase(),
                token_type.to_string(),
            ))
            .cloned())
    }

    fn put(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
        transfers: &[TransferRecord],
        owners: &[OwnerBalance],
    ) -> Result<(), AppError> {
        self.rows.lock().unwrap().insert(
            (
                chain.to_string(),
                contract_address.to_lowercase(),
                token_type.to_string(),
            ),
            CachedSignals {
                mint_recipients: vec!["0xminter".into()],
                active_sellers: vec!["0xminter".into(), "0xvictim".into()],
                address_signals: top_contract_analysis_rs::analysis::signals::analyze_transfer_signals(
                    transfers,
                ),
                victim_signals: Some(VictimSignalPayload {
                    owner_count: 1,
                    stuck_holder_count: 1,
                    stuck_holder_ratio: Some(1.0),
                    victim_wallet_count: 1,
                }),
                transfers: transfers.to_vec(),
                owners: owners.to_vec(),
            },
        );
        Ok(())
    }
}

#[test]
fn default_output_basename_matches_existing_prefix() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload {
            name: "Azuki".into(),
            contract_address: "0xseed".into(),
            ..Default::default()
        },
        ..Default::default()
    };

    assert_eq!(
        default_output_basename(&payload),
        "top_contract_analysis__azuki"
    );
}

#[test]
fn default_output_basename_casefolds_non_ascii_more_like_python() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload {
            name: "Straße".into(),
            contract_address: "0xseed".into(),
            ..Default::default()
        },
        ..Default::default()
    };

    assert_eq!(
        default_output_basename(&payload),
        "top_contract_analysis__strasse"
    );
}

#[test]
fn single_report_payload_serializes_current_python_top_level_shape() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload {
            contract_address: "0xseed".into(),
            ..Default::default()
        },
        seed_collection_stats: SeedCollectionStatsPayload {
            seed_nft_count: 1,
            unique_token_uri_count: 1,
            unique_image_uri_count: 1,
            unique_name_count: 1,
            unique_symbol_count: 1,
        },
        duplicate_candidates: vec![top_contract_analysis_rs::models::DuplicateCandidate {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            confidence: "high".into(),
            ..Default::default()
        }],
        contract_level_summary: BTreeMap::from([(
            "0xdup".into(),
            top_contract_analysis_rs::models::ContractLevelSummaryPayload { candidate_count: 1 },
        )]),
        infringing_tokens: vec![top_contract_analysis_rs::models::InfringingTokenRecord {
            contract_address: "0xdup".into(),
            token_id: "1".into(),
            minter_address: "0xminter".into(),
            ..Default::default()
        }],
        malicious_addresses: vec![MaliciousAddressPayload {
            address: "0xsybil".into(),
            ..Default::default()
        }],
        honest_addresses: vec![HonestAddressPayload {
            contract_address: "0xdup".into(),
            address: "0xholder".into(),
            hold_duration_count: 2,
            mint_to_honest_seconds_samples: vec![15, 30],
            ..Default::default()
        }],
        fraud_trade_stats: BTreeMap::from([(
            "0xdup".into(),
            FraudTradeStatsPayload {
                native_eth_sale_count: Some(4),
                native_eth_volume: Some(6.25),
                ..Default::default()
            },
        )]),
        ..Default::default()
    };

    let serialized = serde_json::to_value(&payload).unwrap();
    let object = serialized.as_object().unwrap();
    let keys: BTreeSet<_> = object.keys().map(String::as_str).collect();

    assert_eq!(
        keys,
        BTreeSet::from([
            "seed_contract",
            "seed_collection_stats",
            "legit_duplicates",
            "duplicate_contracts",
            "contract_level_summary",
            "address_signals",
            "victim_signals",
            "infringing_tokens",
            "malicious_addresses",
            "honest_addresses",
            "honest_address_stats",
            "victim_addresses",
            "address_evidence_features",
            "contract_lifecycle_events",
            "value_flow_edges",
            "content_similarity_edges",
            "campaign_clusters",
            "lifecycle_metrics",
            "weak_supervision_labels",
            "early_detection_features",
            "market_events",
            "fraud_trade_stats",
            "nft_propagation_paths",
            "report_summary",
        ])
    );
    assert!(!object.contains_key("output_files"));
    assert!(!object.contains_key("duplicate_candidates"));
    assert_eq!(
        serialized["contract_level_summary"]["0xdup"]["candidate_count"],
        1
    );
    assert_eq!(
        serialized["infringing_tokens"][0]["minter_address"],
        "0xminter"
    );
    assert_eq!(serialized["malicious_addresses"][0]["address"], "0xsybil");
    assert_eq!(serialized["honest_addresses"][0]["hold_duration_count"], 2);
    assert_eq!(
        serialized["honest_addresses"][0]["mint_to_honest_seconds_samples"],
        serde_json::json!([15, 30])
    );
    assert_eq!(
        serialized["fraud_trade_stats"]["0xdup"]["native_eth_sale_count"],
        4
    );
    assert_eq!(
        serialized["fraud_trade_stats"]["0xdup"]["native_eth_volume"],
        6.25
    );
}

#[test]
fn single_report_markdown_preserves_summary_sections_only() {
    let payload = SingleReportPayload {
        seed_contract: SeedContractPayload {
            chain: "ethereum".into(),
            contract_address: "0xseed".into(),
            name: "Azuki".into(),
            symbol: "AZUKI".into(),
            token_type: "erc721".into(),
            contract_deployer: "0xdeployer".into(),
            deployed_block_number: 12345,
        },
        seed_collection_stats: SeedCollectionStatsPayload {
            seed_nft_count: 10,
            unique_token_uri_count: 8,
            unique_image_uri_count: 7,
            unique_name_count: 6,
            unique_symbol_count: 1,
        },
        report_summary: ReportSummary {
            open_license_detected: true,
            candidate_contract_count: 9,
            infringing_nft_count: 11,
            malicious_address_count: 4,
            honest_address_count: 5,
            repeat_infringing_address_count: 2,
            legit_duplicate_contract_count: 1,
            candidate_open_license_token_count: 6,
            candidate_open_license_contract_count: 2,
            honest_purchase_total_eth: 10.0,
            honest_purchase_total_usd: 10.0,
            stuck_cost_eth: 6.5,
            stuck_cost_usd: 6.5,
            stuck_cost_ratio: Some(0.65),
            buy_asset_ratio_known_address_count: 5,
            ratio_over_60_address_count: 3,
            ratio_over_60_address_ratio: Some(0.6),
            ratio_over_80_address_count: 1,
            ratio_over_80_address_ratio: Some(0.2),
            stuck_honest_address_count: 2,
            stuck_honest_address_ratio: Some(0.4),
            corrupted_honest_address_count: 1,
            avg_seconds_to_honest_holder: Some(12.5),
            median_seconds_to_honest_holder: Some(10.0),
            avg_mint_to_first_transfer_seconds: Some(8.0),
            median_mint_to_first_transfer_seconds: Some(7.0),
            avg_unique_receiver_count: Some(4.0),
        },
        duplicate_contracts: vec![
            DuplicateContractPayload {
                contract_address: "0xhigh".into(),
                candidate_count: 2,
                match_reasons: vec!["token_uri_match".into(), "name_match".into()],
                ..Default::default()
            },
            DuplicateContractPayload {
                contract_address: "0xlow".into(),
                candidate_count: 1,
                match_reasons: vec!["image_uri_match".into()],
                ..Default::default()
            },
        ],
        legit_duplicates: vec![DuplicateContractPayload {
            contract_address: "0xlegit".into(),
            candidate_count: 1,
            mint_recipients: vec!["0xofficial".into()],
            ..Default::default()
        }],
        address_signals: BTreeMap::from([(
            "0xhigh".into(),
            AddressSignalPayload {
                mint_address_count: 2,
                mint_count: 3,
                unique_receiver_count: 4,
                cycle_edge_count: 1,
                star_distributor_count: 1,
                mint_to_first_transfer_seconds: 8,
                fast_spread: true,
            },
        )]),
        victim_signals: BTreeMap::from([(
            "0xhigh".into(),
            VictimSignalPayload {
                owner_count: 3,
                stuck_holder_count: 1,
                stuck_holder_ratio: Some(1.0 / 3.0),
                victim_wallet_count: 2,
            },
        )]),
        honest_address_stats: BTreeMap::from([(
            "0xhigh".into(),
            HonestAddressStatsPayload {
                honest_address_count: 2,
                corrupted_address_count: 1,
                honest_to_honest_transfer_count: 3,
                median_holding_seconds: Some(44.0),
                avg_seconds_to_honest_holder: Some(12.5),
                corrupted_addresses: vec!["0xhonest".into()],
            },
        )]),
        honest_addresses: vec![HonestAddressPayload {
            contract_address: "0xhigh".into(),
            address: "0xhonest".into(),
            interacted_token_count: 2,
            currently_holding_token_count: 1,
            hold_duration_median_seconds: Some(44.0),
            hold_duration_count: 2,
            is_corrupted_address: true,
            honest_sale_to_honest_count: 1,
            mint_to_honest_seconds_samples: vec![12, 13],
        }],
        victim_addresses: vec![VictimAddressPayload {
            address: "0xvictim".into(),
            buy_tx_hashes: vec!["0xbuy1".into(), "0xbuy2".into()],
            buy_amount_eth: 3.5,
            buy_amount_usd: 3.5,
            last_buy_amount_eth: Some(2.0),
            last_buy_amount_usd: Some(2.0),
            buy_before_eth_balance: Some(4.0),
            buy_before_usd_balance: Some(4.0),
            buy_asset_ratio: Some(0.5),
            buy_asset_ratio_with_gas: Some(0.55),
            is_stuck: true,
            last_buy_tx_hash: "0xbuy2".into(),
            ratio_status: "ok".into(),
        }],
        fraud_trade_stats: BTreeMap::from([(
            "0xhigh".into(),
            FraudTradeStatsPayload {
                unique_buyers: 2,
                eth_priced_sale_count: Some(2),
                usd_priced_sale_count: Some(2),
                eth_priced_volume: Some(5.5),
                usd_priced_volume: Some(5.5),
                native_eth_sale_count: Some(2),
                native_eth_volume: Some(5.5),
                stuck_wallet_count: 1,
                stuck_cost_eth: 2.0,
                stuck_cost_usd: 2.0,
            },
        )]),
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("# Top NFT 合约重复样本分析报告"));
    assert!(markdown.contains("## 种子合约"));
    assert!(markdown.contains("- 合约地址: 0xseed"));
    assert!(markdown.contains("- 合约部署者: 0xdeployer"));
    assert!(markdown.contains("## 摘要"));
    assert!(markdown.contains("- 检测到开放许可: 是"));
    assert!(markdown.contains("- 恶意地址数: 4"));
    assert!(markdown.contains("- 候选侧开放许可 token 数: 6"));
    assert!(markdown.contains("- 套牢资金(USD): 6.5 / 65.00%"));
    assert!(markdown.contains("- 买入金额占钱包总额 >60% 的地址数/占比: 3 / 60.00%"));
    assert!(markdown.contains("## 种子集合统计"));
    assert!(markdown.contains("- 拉取到的种子 NFT 数: 10"));
    assert!(markdown.contains("## 合约分类摘要"));
    assert!(markdown.contains("- 疑似重复合约数: 2"));
    assert!(markdown.contains("- 疑似重复 NFT 数: 3"));
    assert!(markdown.contains("- 命中原因分布: image_uri_match=1, name_match=1, token_uri_match=1"));
    assert!(markdown.contains("- 官方参与型重复合约数: 1"));
    assert!(markdown.contains("- 官方参与型重复 NFT 数: 1"));
    assert!(markdown.contains("- 官方参与型判定原因分布: mint 接收地址命中官方地址规则=1"));
    assert!(markdown.contains("## 资金与交易摘要"));
    assert!(markdown.contains("- 有定价销售记录数: 2"));
    assert!(markdown.contains("- 有定价销售额(USD): 5.5"));
    assert!(markdown.contains("- 唯一买家计数合计: 2"));
    assert!(!markdown.contains("## 地址行为信号"));
    assert!(!markdown.contains("## 受害者信号"));
    assert!(!markdown.contains("## 诚实地址画像"));
    assert!(!markdown.contains("## 被骗地址画像"));
    assert!(!markdown.contains("## 被骗交易与套牢资金"));
    assert!(!markdown.contains("0xhonest"));
    assert!(!markdown.contains("0xvictim"));
    assert!(!markdown.contains("0xhigh:"));
}

#[test]
fn single_report_markdown_omits_detailed_address_sections() {
    let payload = SingleReportPayload {
        honest_address_stats: BTreeMap::from([(
            "0xdup".into(),
            HonestAddressStatsPayload {
                honest_address_count: 1,
                corrupted_address_count: 0,
                honest_to_honest_transfer_count: 0,
                median_holding_seconds: None,
                avg_seconds_to_honest_holder: None,
                corrupted_addresses: vec![],
            },
        )]),
        honest_addresses: vec![HonestAddressPayload {
            contract_address: "0xdup".into(),
            address: "0xhonest".into(),
            interacted_token_count: 1,
            currently_holding_token_count: 0,
            hold_duration_median_seconds: None,
            hold_duration_count: 0,
            is_corrupted_address: false,
            honest_sale_to_honest_count: 0,
            mint_to_honest_seconds_samples: vec![],
        }],
        victim_addresses: vec![VictimAddressPayload {
            address: "0xvictim".into(),
            buy_tx_hashes: vec!["0xbuy".into()],
            buy_amount_eth: 1.0,
            buy_amount_usd: 1.0,
            last_buy_amount_eth: None,
            last_buy_amount_usd: None,
            buy_before_eth_balance: None,
            buy_before_usd_balance: None,
            buy_asset_ratio: None,
            buy_asset_ratio_with_gas: None,
            is_stuck: false,
            last_buy_tx_hash: String::new(),
            ratio_status: "unavailable".into(),
        }],
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("## 摘要"));
    assert!(markdown.contains("## 合约分类摘要"));
    assert!(!markdown.contains("## 诚实地址画像"));
    assert!(!markdown.contains("## 被骗地址画像"));
    assert!(!markdown.contains("0xhonest"));
    assert!(!markdown.contains("0xvictim"));
    assert!(!markdown.contains("buy_tx_count"));
    assert!(!markdown.contains("hold_duration_median_seconds"));
}

#[test]
fn single_report_markdown_omits_victim_address_rows() {
    let payload = SingleReportPayload {
        victim_addresses: vec![
            VictimAddressPayload {
                address: "0xvictim".into(),
                buy_tx_hashes: vec!["0xbuy1".into()],
                buy_amount_eth: 0.0,
                buy_amount_usd: 5.0,
                last_buy_amount_eth: Some(0.0),
                last_buy_amount_usd: Some(5.0),
                buy_before_usd_balance: Some(20.0),
                buy_asset_ratio: Some(0.25),
                is_stuck: false,
                last_buy_tx_hash: "0xbuy1".into(),
                ratio_status: "ok".into(),
                ..Default::default()
            },
            VictimAddressPayload {
                address: "0xvictim".into(),
                buy_tx_hashes: vec!["0xbuy2".into()],
                buy_amount_eth: 0.0,
                buy_amount_usd: 7.0,
                last_buy_amount_eth: Some(0.0),
                last_buy_amount_usd: Some(7.0),
                buy_before_usd_balance: Some(30.0),
                buy_asset_ratio: Some(7.0 / 30.0),
                is_stuck: true,
                last_buy_tx_hash: "0xbuy2".into(),
                ratio_status: "ok".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("## 资金与交易摘要"));
    assert_eq!(markdown.matches("0xvictim").count(), 0);
    assert!(!markdown.contains("buy_tx_count"));
    assert!(!markdown.contains("last_buy_tx"));
}

#[test]
fn single_report_fraud_trade_stats_do_not_fall_back_to_native_eth_fields_for_usd_output() {
    let payload = SingleReportPayload {
        fraud_trade_stats: BTreeMap::from([(
            "0xdup".into(),
            FraudTradeStatsPayload {
                unique_buyers: 3,
                native_eth_sale_count: Some(4),
                native_eth_volume: Some(6.25),
                stuck_wallet_count: 2,
                stuck_cost_eth: 1.5,
                stuck_cost_usd: 0.0,
                ..Default::default()
            },
        )]),
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("- 有定价销售记录数: 0"));
    assert!(markdown.contains("- 有定价销售额(USD): 0"));
    assert!(markdown.contains("- 唯一买家计数合计: 3"));
    assert!(!markdown.contains("native_eth_volume"));
    assert!(!markdown.contains("0xdup:"));
}

#[test]
fn single_report_fraud_trade_stats_preserve_explicit_zero_eth_priced_values() {
    let payload = SingleReportPayload {
        fraud_trade_stats: BTreeMap::from([(
            "0xdup".into(),
            FraudTradeStatsPayload {
                unique_buyers: 3,
                eth_priced_sale_count: Some(0),
                usd_priced_sale_count: Some(0),
                eth_priced_volume: Some(0.0),
                usd_priced_volume: Some(0.0),
                native_eth_sale_count: Some(4),
                native_eth_volume: Some(6.25),
                stuck_wallet_count: 2,
                stuck_cost_eth: 1.5,
                stuck_cost_usd: 1.5,
            },
        )]),
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("- 有定价销售记录数: 0"));
    assert!(markdown.contains("- 有定价销售额(USD): 0"));
    assert!(markdown.contains("- 唯一买家计数合计: 3"));
    assert!(!markdown.contains("0xdup:"));
}

#[test]
fn single_report_does_not_display_eth_values_in_usd_fields() {
    let payload = SingleReportPayload {
        victim_addresses: vec![VictimAddressPayload {
            address: "0xvictim".into(),
            buy_tx_hashes: vec!["0xbuy".into()],
            buy_amount_eth: 1.25,
            buy_amount_usd: 0.0,
            last_buy_amount_eth: Some(1.25),
            last_buy_amount_usd: None,
            is_stuck: true,
            last_buy_tx_hash: "0xbuy".into(),
            ratio_status: "unavailable".into(),
            ..Default::default()
        }],
        fraud_trade_stats: BTreeMap::from([(
            "0xdup".into(),
            FraudTradeStatsPayload {
                unique_buyers: 1,
                eth_priced_sale_count: Some(1),
                eth_priced_volume: Some(1.25),
                usd_priced_sale_count: Some(0),
                usd_priced_volume: Some(0.0),
                stuck_wallet_count: 1,
                stuck_cost_eth: 1.25,
                stuck_cost_usd: 0.0,
                ..Default::default()
            },
        )]),
        ..Default::default()
    };

    let markdown = render_human_readable_report(&payload);

    assert!(markdown.contains("- 有定价销售记录数: 0"));
    assert!(markdown.contains("- 有定价销售额(USD): 0"));
    assert!(markdown.contains("- 唯一买家计数合计: 1"));
    assert!(!markdown.contains("0xvictim"));
    assert!(!markdown.contains("0xdup:"));
    assert!(!markdown.contains("1.25"));
}

#[tokio::test]
async fn analyze_moves_official_reissues_into_legit_duplicates() {
    let api = Arc::new(FakeLegitApi::default());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"Creative Commons Zero license: CC0"}"#.into(),
                    metadata_doc: "creative commons zero public domain".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(payload.duplicate_contracts.is_empty());
    assert!(payload.infringing_tokens.is_empty());
    assert_eq!(payload.legit_duplicates.len(), 1);
    assert_eq!(payload.legit_duplicates[0].contract_address, "0xdup");
    assert_eq!(
        payload.legit_duplicates[0].mint_recipients,
        vec!["0xminter"]
    );
    assert_eq!(payload.report_summary.legit_duplicate_contract_count, 1);
    assert!(payload.content_similarity_edges.is_empty());
    assert!(payload
        .contract_lifecycle_events
        .iter()
        .all(|event| event.contract_address != "0xdup"));
    assert!(payload.campaign_clusters.is_empty());
    assert!(payload.lifecycle_metrics.is_empty());
    assert_eq!(
        api.sales_calls.load(Ordering::SeqCst),
        0,
        "official reissues should not fetch sale history"
    );
}

#[tokio::test]
async fn analyze_builds_expected_summary_counts() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.seed_contract.contract_address, "0xseed");
    assert_eq!(payload.report_summary.candidate_contract_count, 1);
    assert_eq!(payload.duplicate_candidates.len(), 1);
    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert_eq!(payload.contract_level_summary["0xdup"].candidate_count, 1);
}

#[tokio::test]
async fn analyze_excludes_candidate_contract_that_currently_holds_seed_nft() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeSeedOwnerApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xwrapped".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.report_summary.candidate_contract_count, 0);
    assert!(payload.duplicate_candidates.is_empty());
    assert!(payload.duplicate_contracts.is_empty());
    assert!(payload.contract_level_summary.is_empty());
    assert_eq!(payload.report_summary.legit_duplicate_contract_count, 1);
    assert_eq!(payload.legit_duplicates.len(), 1);
    assert_eq!(payload.legit_duplicates[0].contract_address, "0xwrapped");
    assert_eq!(
        payload.legit_duplicates[0].exclusion_reasons,
        vec!["当前持有 seed 合约 NFT"]
    );
}

#[tokio::test]
async fn analyze_excludes_candidate_contract_with_seed_transfer_history() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeSeedTransferHistoryApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xwrapped".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.report_summary.candidate_contract_count, 0);
    assert!(payload.duplicate_candidates.is_empty());
    assert!(payload.duplicate_contracts.is_empty());
    assert_eq!(payload.report_summary.legit_duplicate_contract_count, 1);
    assert_eq!(payload.legit_duplicates[0].contract_address, "0xwrapped");
    assert_eq!(
        payload.legit_duplicates[0].exclusion_reasons,
        vec!["链上历史 Transfer 显示接收过 seed 合约 NFT"]
    );
}

#[tokio::test]
async fn analyze_keeps_wrapper_named_candidate_without_chain_seed_relation() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xwrapped".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Wrapped Azuki #1".into(),
                    symbol: "WAZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.report_summary.candidate_contract_count, 1);
    assert_eq!(payload.duplicate_candidates.len(), 1);
    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert_eq!(
        payload.contract_level_summary["0xwrapped"].candidate_count,
        1
    );
    assert!(payload.legit_duplicates.is_empty());
}

#[tokio::test]
async fn analyze_keeps_locally_wrapper_named_candidate_without_chain_seed_relation() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xwrapped".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                contract_names: vec![ContractNameRecord {
                    contract_address: "0xwrapped".into(),
                    name_norm: "wrapped azuki".into(),
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.report_summary.candidate_contract_count, 1);
    assert_eq!(payload.duplicate_candidates.len(), 1);
    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert_eq!(
        payload.contract_level_summary["0xwrapped"].candidate_count,
        1
    );
    assert!(payload.legit_duplicates.is_empty());
}

#[tokio::test]
async fn analyze_marks_seed_open_license_and_skips_suspected_contracts() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeOpenLicenseApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(payload.report_summary.open_license_detected);
    assert!(payload.duplicate_contracts.is_empty());
    assert!(payload.infringing_tokens.is_empty());
}

#[tokio::test]
async fn analyze_writes_default_json_and_markdown_files() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot::default(),
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };
    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();
    let dir = tempdir().unwrap();

    let (json_path, md_path) = write_outputs_to_directory(&payload, dir.path()).unwrap();

    assert!(json_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("top_contract_analysis__"));
    assert!(md_path.exists());
}

#[tokio::test]
async fn analyze_enriches_duplicate_contracts_with_signals_and_infringing_tokens() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeEnrichedApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.infringing_tokens.len(), 1);
    assert_eq!(payload.infringing_tokens[0].contract_address, "0xdup");
    assert_eq!(payload.infringing_tokens[0].minter_address, "0xminter");
    assert_eq!(payload.address_signals["0xdup"].mint_address_count, 1);
    assert!(payload.address_signals["0xdup"].fast_spread);
    assert_eq!(payload.victim_signals["0xdup"].owner_count, 1);
    assert_eq!(payload.victim_signals["0xdup"].stuck_holder_count, 1);
    assert_eq!(payload.report_summary.infringing_nft_count, 1);
    assert_eq!(
        payload.duplicate_contracts[0].contract_deployer,
        "0xcreator"
    );
    assert_eq!(payload.duplicate_contracts[0].deployed_block_number, 123);
    assert_eq!(payload.duplicate_contracts[0].owner_address, "0xowner");
    assert_eq!(payload.duplicate_contracts[0].admin_address, "0xadmin");
    assert_eq!(
        payload.duplicate_contracts[0].proxy_admin_address,
        "0xproxyadmin"
    );
    assert!(payload.contract_lifecycle_events.iter().any(|event| {
        event.contract_address == "0xdup"
            && event.lifecycle_stage == "replica_deployment"
            && event.actor_address == "0xcreator"
            && event.block_number == 123
    }));
    let mint_payment_edge = payload
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "mint_payment")
        .expect("mint payment value flow edge");
    assert_eq!(mint_payment_edge.tx_hash, "0xmint");
    assert_eq!(mint_payment_edge.from_address, "0xminter");
    assert_eq!(mint_payment_edge.to_address, "0xcreator");
    assert_eq!(mint_payment_edge.value_eth, Some(0.08));
    assert_eq!(mint_payment_edge.value_usd, Some(184.0));
    let funding_edge = payload
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "funding")
        .expect("funding value flow edge");
    assert_eq!(funding_edge.from_address, "0xfunder");
    assert_eq!(funding_edge.to_address, "0xminter");
    let withdrawal_edge = payload
        .value_flow_edges
        .iter()
        .find(|edge| edge.channel == "withdrawal")
        .expect("withdrawal value flow edge");
    assert_eq!(withdrawal_edge.from_address, "0xdup");
    assert_eq!(withdrawal_edge.to_address, "0xfunder");
    let lifecycle_metric = payload
        .lifecycle_metrics
        .iter()
        .find(|metric| metric.contract_address == "0xdup")
        .expect("contract lifecycle metric");
    assert_eq!(lifecycle_metric.funding_edge_count, 1);
    assert_eq!(lifecycle_metric.withdrawal_edge_count, 1);
    assert_eq!(lifecycle_metric.revenue_backflow_edge_count, 1);
    assert!(lifecycle_metric.early_detection_positive);
    assert!(payload.weak_supervision_labels.iter().any(|label| {
        label.entity_type == "contract"
            && label.contract_address == "0xdup"
            && label.label == "probable_infringement_campaign"
    }));
    assert!(payload.early_detection_features.iter().any(|row| {
        row.contract_address == "0xdup"
            && row.observation_window_seconds == 86_400
            && row.weak_label == "positive_observed_sale_or_victimization"
    }));
    assert!(payload.contract_lifecycle_events.iter().any(|event| {
        event.lifecycle_stage == "primary_monetization"
            && event.event_type == "mint_payment"
            && event.tx_hash == "0xmint"
    }));
}

#[tokio::test]
async fn analyze_expands_matched_contracts_to_all_tokens_for_report_analysis() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeTwoTokenOwnersApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    ..DatabaseNftRecord::default()
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.duplicate_candidates.len(), 1);
    assert_eq!(payload.duplicate_contracts[0].candidate_count, 2);
    assert_eq!(payload.contract_level_summary["0xdup"].candidate_count, 2);
    assert_eq!(payload.report_summary.infringing_nft_count, 2);
    assert_eq!(
        payload
            .infringing_tokens
            .iter()
            .map(|item| item.token_id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["1", "2"])
    );
    assert_eq!(payload.report_summary.honest_address_count, 2);
    assert_eq!(
        payload
            .honest_addresses
            .iter()
            .map(|item| item.address.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["0xholder1", "0xholder2"])
    );
}

#[tokio::test]
async fn analyze_falls_back_to_local_snapshot_when_provider_expansion_is_empty() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeEmptyContractNftsApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![
                    DatabaseNftRecord {
                        contract_address: "0xdup".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
                        name: "Azuki Mirror #1".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_doc: "gold dragon".into(),
                        ..DatabaseNftRecord::default()
                    },
                    DatabaseNftRecord {
                        contract_address: "0xdup".into(),
                        token_id: "2".into(),
                        token_uri: "ipfs://candidate/2".into(),
                        image_uri: "ipfs://candidate/2.png".into(),
                        name: "Azuki Mirror #2".into(),
                        metadata_json: r#"{"description":"different trait"}"#.into(),
                        metadata_doc: "different trait".into(),
                        ..DatabaseNftRecord::default()
                    },
                ],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.duplicate_contracts[0].candidate_count, 2);
    assert_eq!(payload.contract_level_summary["0xdup"].candidate_count, 2);
    assert_eq!(payload.report_summary.infringing_nft_count, 2);
}

#[tokio::test]
async fn analyze_ignores_symbol_only_candidate_contracts() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeEnrichedApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://other/1".into(),
                    image_uri: "ipfs://other-image/1.png".into(),
                    name: "Completely Different".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"unrelated"}"#.into(),
                    metadata_doc: "unrelated".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert!(payload.duplicate_contracts.is_empty());
    assert!(payload.infringing_tokens.is_empty());
    assert!(payload.address_signals.is_empty());
    assert!(payload.victim_signals.is_empty());
    assert_eq!(payload.report_summary.candidate_contract_count, 0);
    assert_eq!(payload.report_summary.infringing_nft_count, 0);
}

#[tokio::test]
async fn analyze_builds_address_profiles_and_trade_stats_for_duplicate_contracts() {
    let deps = AnalysisDeps {
        api: Arc::new(CountingApi::new()),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.malicious_addresses.len(), 1);
    assert_eq!(payload.malicious_addresses[0].address, "0xminter");
    assert_eq!(payload.honest_addresses.len(), 2);
    assert_eq!(payload.victim_addresses.len(), 1);
    assert_eq!(payload.victim_addresses[0].address, "0xvictim");
    assert_eq!(payload.victim_addresses[0].buy_amount_eth, 1.5);
    assert!(!payload.victim_addresses[0].is_stuck);
    assert_eq!(
        payload.honest_address_stats["0xdup"].honest_address_count,
        2
    );
    assert_eq!(
        payload.honest_address_stats["0xdup"].corrupted_address_count,
        1
    );
    assert_eq!(
        payload.honest_address_stats["0xdup"].honest_to_honest_transfer_count,
        1
    );
    assert_eq!(
        payload.honest_address_stats["0xdup"].avg_seconds_to_honest_holder,
        Some(10.0)
    );
    assert_eq!(payload.fraud_trade_stats["0xdup"].unique_buyers, 1);
    assert_eq!(
        payload.fraud_trade_stats["0xdup"].eth_priced_sale_count,
        Some(1)
    );
    assert_eq!(
        payload.fraud_trade_stats["0xdup"].eth_priced_volume,
        Some(1.5)
    );
    assert_eq!(payload.fraud_trade_stats["0xdup"].stuck_wallet_count, 0);
    assert_eq!(payload.report_summary.malicious_address_count, 1);
    assert_eq!(payload.report_summary.honest_address_count, 2);
    assert_eq!(payload.report_summary.honest_purchase_total_eth, 1.5);
    assert_eq!(payload.report_summary.corrupted_honest_address_count, 1);

    let propagation = &payload.nft_propagation_paths["0xdup"];
    assert_eq!(propagation.summary.token_count, 1);
    assert_eq!(propagation.summary.mint_edge_count, 1);
    assert_eq!(propagation.summary.sale_edge_count, 1);
    assert_eq!(propagation.summary.malicious_node_count, 1);
    assert_eq!(propagation.summary.victim_node_count, 1);
    assert!(propagation.edges.iter().any(|edge| {
        edge.channel == "mint" && edge.to_address == "0xminter" && edge.token_id == "1"
    }));
    assert!(propagation.edges.iter().any(|edge| {
        edge.channel == "sale"
            && edge.from_address == "0xminter"
            && edge.to_address == "0xvictim"
            && edge.price_eth == Some(1.5)
    }));
    assert!(propagation.nodes["0xminter"]
        .roles
        .contains(&"malicious".to_string()));
    assert!(propagation.nodes["0xvictim"]
        .roles
        .contains(&"victim_buyer".to_string()));
}

#[tokio::test]
async fn analyze_reuses_signal_cache_for_transfers_and_owners() {
    let api = Arc::new(CountingApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: Some(Arc::new(FakeSignalCache::default())),
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };
    let request = AnalyzeRequest {
        chain: "ethereum".into(),
        seed_contract_address: "0xseed".into(),
        alchemy_api_key: "key".into(),
        ..AnalyzeRequest::default()
    };

    let first = analyze_seed_contract(request.clone(), &deps).await.unwrap();
    let second = analyze_seed_contract(request, &deps).await.unwrap();

    assert_eq!(api.transfer_fetch_count.load(Ordering::SeqCst), 1);
    assert_eq!(api.owner_fetch_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        first.address_signals["0xdup"],
        second.address_signals["0xdup"]
    );
    assert_eq!(
        first.victim_signals["0xdup"],
        second.victim_signals["0xdup"]
    );
}

#[tokio::test]
async fn analyze_computes_native_eth_sale_metrics_for_victim_addresses() {
    let deps = AnalysisDeps {
        api: Arc::new(FakeSaleMetricApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.victim_addresses.len(), 1);
    assert_eq!(payload.victim_addresses[0].address, "0xvictim");
    assert_eq!(
        payload.victim_addresses[0].buy_before_eth_balance,
        Some(3.0)
    );
    assert_eq!(payload.victim_addresses[0].buy_asset_ratio, Some(0.5));
    assert!(
        payload.victim_addresses[0]
            .buy_asset_ratio_with_gas
            .unwrap()
            > 0.5
    );
    assert_eq!(payload.victim_addresses[0].ratio_status, "ok");
    assert_eq!(
        payload.report_summary.buy_asset_ratio_known_address_count,
        1
    );
    assert_eq!(payload.report_summary.ratio_over_60_address_count, 0);
}

#[tokio::test]
async fn analyze_sale_metrics_are_keyed_by_transaction_and_buyer() {
    let deps = AnalysisDeps {
        api: Arc::new(MultiBuyerSameTxSaleMetricApi),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![
                    DatabaseNftRecord {
                        contract_address: "0xdup".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
                        name: "Azuki Mirror #1".into(),
                        symbol: "AZUKI".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_doc: "gold dragon".into(),
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                    DatabaseNftRecord {
                        contract_address: "0xdup".into(),
                        token_id: "2".into(),
                        token_uri: "ipfs://seed/2".into(),
                        image_uri: "ipfs://image/2.png".into(),
                        name: "Azuki Mirror #2".into(),
                        symbol: "AZUKI".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_doc: "gold dragon".into(),
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                ],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    let buyer1 = payload
        .victim_addresses
        .iter()
        .find(|row| row.address == "0xbuyer1")
        .expect("buyer1 victim row");
    let buyer2 = payload
        .victim_addresses
        .iter()
        .find(|row| row.address == "0xbuyer2")
        .expect("buyer2 victim row");

    assert_eq!(buyer1.buy_before_eth_balance, Some(1.0));
    assert_eq!(buyer1.buy_asset_ratio, Some(1.0));
    assert_eq!(buyer2.buy_before_eth_balance, Some(10.0));
    assert_eq!(buyer2.buy_asset_ratio, Some(0.2));
}

#[tokio::test]
async fn analyze_processes_duplicate_contracts_within_a_seed_concurrently() {
    let api = Arc::new(ConcurrentContractApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![
                    DatabaseNftRecord {
                        contract_address: "0xdup1".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
                        name: "Azuki Mirror #1".into(),
                        symbol: "AZUKI".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_doc: "gold dragon".into(),
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                    DatabaseNftRecord {
                        contract_address: "0xdup2".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
                        name: "Azuki Mirror #2".into(),
                        symbol: "AZUKI".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_doc: "gold dragon".into(),
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                ],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            contract_max_concurrency: 2,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.duplicate_contracts.len(), 2);
    assert!(
        api.max_transfer_fetches.load(Ordering::SeqCst) >= 2,
        "expected duplicate contract analysis to overlap within one seed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analyze_starts_contract_analysis_before_all_provider_expansions_finish() {
    let api = Arc::new(StaggeredExpansionApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![
                    DatabaseNftRecord {
                        contract_address: "0xdup1".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
                        name: "Azuki Mirror #1".into(),
                        symbol: "AZUKI".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_doc: "gold dragon".into(),
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                    DatabaseNftRecord {
                        contract_address: "0xdup2".into(),
                        token_id: "1".into(),
                        token_uri: "ipfs://seed/1".into(),
                        image_uri: "ipfs://image/1.png".into(),
                        name: "Azuki Mirror #2".into(),
                        symbol: "AZUKI".into(),
                        metadata_json: r#"{"description":"gold dragon"}"#.into(),
                        metadata_doc: "gold dragon".into(),
                        metadata_recall_checked: false,
                        metadata_recall_match: false,
                    },
                ],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            contract_max_concurrency: 2,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.duplicate_contracts.len(), 2);
    assert_eq!(
        api.transfer_before_slow_expansion_done
            .load(Ordering::SeqCst),
        1,
        "expected fast contract analysis to start before slow provider expansion finished"
    );
}

#[tokio::test]
async fn analyze_computes_sale_metrics_concurrently_within_a_contract() {
    let api = Arc::new(ConcurrentSaleMetricApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            sale_metric_max_concurrency: 2,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.victim_addresses.len(), 2);
    assert!(
        api.max_receipts.load(Ordering::SeqCst) >= 2,
        "expected sale metric receipt fetches to overlap within one contract"
    );
}

#[tokio::test]
async fn analyze_prefetches_sale_metrics_per_buyer_with_shared_transaction_hash() {
    let api = Arc::new(ConcurrentSaleMetricApi::with_duplicate_sale_tx());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            sale_metric_max_concurrency: 2,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.victim_addresses.len(), 2);
    assert_eq!(api.receipt_calls.load(Ordering::SeqCst), 2);
    assert_eq!(api.balance_calls.load(Ordering::SeqCst), 2);
    assert_eq!(api.same_block_transfer_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn analyze_prefetches_sale_metrics_only_for_latest_buyer_purchase() {
    let api = Arc::new(ConcurrentSaleMetricApi::with_same_buyer_history());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            sale_metric_max_concurrency: 2,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.victim_addresses.len(), 1);
    assert_eq!(api.receipt_calls.load(Ordering::SeqCst), 1);
    assert_eq!(api.balance_calls.load(Ordering::SeqCst), 1);
    assert_eq!(api.same_block_transfer_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn analyze_fetches_contract_inputs_concurrently_within_one_contract() {
    let api = Arc::new(ConcurrentSingleContractFetchApi::new());
    let deps = AnalysisDeps {
        api: api.clone(),
        feature_store: Arc::new(FakeFeatureStore {
            snapshot: DatabaseSnapshot {
                nft_rows: vec![DatabaseNftRecord {
                    contract_address: "0xdup".into(),
                    token_id: "1".into(),
                    token_uri: "ipfs://seed/1".into(),
                    image_uri: "ipfs://image/1.png".into(),
                    name: "Azuki Mirror #1".into(),
                    symbol: "AZUKI".into(),
                    metadata_json: r#"{"description":"gold dragon"}"#.into(),
                    metadata_doc: "gold dragon".into(),
                    metadata_recall_checked: false,
                    metadata_recall_match: false,
                }],
                ..DatabaseSnapshot::default()
            },
        }),
        signal_cache: None,
        progress: Arc::new(NoopProgressReporter),
        batch_progress: Arc::new(NoopBatchProgressReporter),
    };

    let payload = analyze_seed_contract(
        AnalyzeRequest {
            chain: "ethereum".into(),
            seed_contract_address: "0xseed".into(),
            alchemy_api_key: "key".into(),
            contract_max_concurrency: 1,
            ..AnalyzeRequest::default()
        },
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(payload.duplicate_contracts.len(), 1);
    assert!(
        api.max_fetches.load(Ordering::SeqCst) >= 2,
        "expected transfers/owners/sales fetches to overlap within one contract"
    );
}
