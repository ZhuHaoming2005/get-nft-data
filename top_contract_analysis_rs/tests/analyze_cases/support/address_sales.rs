use super::*;

pub(in crate::analyze_cases) struct SecondaryVictimApi;

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
            contract_address: String::new(),
            gas_used: 21000,
            effective_gas_price_wei: 1_000_000_000,
            fee_native: None,
            fee_usd: None,
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
                contract_address: String::new(),
                gas_used: 0,
                effective_gas_price_wei: 0,
                fee_native: None,
                fee_usd: None,
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

pub(in crate::analyze_cases) struct MultiBuyerSameTxApi;

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
            contract_address: String::new(),
            gas_used: 0,
            effective_gas_price_wei: 0,
            fee_native: None,
            fee_usd: None,
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
