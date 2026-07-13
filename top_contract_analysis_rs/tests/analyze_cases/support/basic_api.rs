use super::*;

pub(in crate::analyze_cases) struct FakeApi;

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

pub(in crate::analyze_cases) struct FakeSeedOwnerApi;

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

pub(in crate::analyze_cases) struct FakeSeedTransferHistoryApi;

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

pub(in crate::analyze_cases) struct FakeTwoTokenOwnersApi;

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

pub(in crate::analyze_cases) struct FakeOpenLicenseApi;

pub(in crate::analyze_cases) struct FakeEmptyContractNftsApi;

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

pub(in crate::analyze_cases) struct CountingApi {
    pub(in crate::analyze_cases) transfer_fetch_count: AtomicUsize,
    pub(in crate::analyze_cases) owner_fetch_count: AtomicUsize,
    pub(in crate::analyze_cases) seed_collection_slug: Option<String>,
    pub(in crate::analyze_cases) candidate_collection_slug: Option<String>,
}

impl CountingApi {
    pub(in crate::analyze_cases) fn new() -> Self {
        Self {
            transfer_fetch_count: AtomicUsize::new(0),
            owner_fetch_count: AtomicUsize::new(0),
            seed_collection_slug: None,
            candidate_collection_slug: None,
        }
    }

    pub(in crate::analyze_cases) fn with_seed_collection_slug(seed_collection_slug: &str) -> Self {
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
