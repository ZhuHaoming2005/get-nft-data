use httpmock::prelude::*;
use top_contract_analysis_rs::analysis::{AnalyzeApi, RealApi};
use top_contract_analysis_rs::api::{
    fetch_helius_asset_transfers, fetch_helius_assets_history,
    fetch_helius_assets_history_with_budget, fetch_helius_collection_assets,
    fetch_helius_collection_snapshot, fetch_helius_transaction_details, AsyncApiClient,
    HeliusCollectionAsset,
};
use top_contract_analysis_rs::models::SeedNft;

#[tokio::test]
async fn provider_quality_lookup_does_not_start_collection_or_history_fetches() {
    let server = MockServer::start_async().await;
    let assets = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 0, "items": []}
            }));
        })
        .await;
    let signatures = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 0, "items": []}
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let quality = api
        .fetch_provider_data_quality("solana", "Collection")
        .await
        .unwrap();

    assert_eq!(assets.hits_async().await, 0);
    assert_eq!(signatures.hits_async().await, 0);
    assert_eq!(quality.provider_quality_lookup_failure_count, 1);
    assert!(!quality.history_complete);
}

#[tokio::test]
async fn provider_quality_counts_snapshot_assets_when_history_was_not_requested() {
    let server = MockServer::start_async().await;
    let assets = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 2, "items": [
                    {"id": "AssetA", "content": {"metadata": {}}},
                    {"id": "AssetB", "content": {"metadata": {}}}
                ]}
            }));
        })
        .await;
    let signatures = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(500);
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let listed = api
        .fetch_seed_contract_nfts("solana", "", None, "Collection")
        .await
        .unwrap();
    let quality = api
        .fetch_provider_data_quality("solana", "Collection")
        .await
        .unwrap();

    assert_eq!(listed.len(), 2);
    assert_eq!(quality.history_requested_asset_count, 0);
    assert_eq!(quality.history_unrequested_asset_count, 2);
    assert_eq!(quality.provider_quality_lookup_failure_count, 1);
    assert!(!quality.history_complete);
    assets.assert_hits_async(1).await;
    signatures.assert_hits_async(0).await;
}

#[tokio::test]
async fn helius_collection_assets_paginate_and_preserve_solana_addresses() {
    let server = MockServer::start_async().await;
    let first = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup")
                .body_contains("\"page\":1")
                .body_contains("\"showGrandTotal\":true");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "total": 2,
                    "page": 1,
                    "limit": 1,
                    "items": [{
                        "id": "So11111111111111111111111111111111111111112",
                        "content": {
                            "metadata": {"name": "One", "symbol": "ONE"},
                            "json_uri": "ipfs://one",
                            "links": {"image": "ipfs://one.png"}
                        },
                        "ownership": {"owner": "Vote111111111111111111111111111111111111111"}
                    }]
                }
            }));
        })
        .await;
    let second = server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("\"page\":2");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "total": 2,
                    "page": 2,
                    "limit": 1,
                    "items": [{
                        "id": "Vote111111111111111111111111111111111111111",
                        "content": {"metadata": {"name": "Two"}}
                    }]
                }
            }));
        })
        .await;
    let client = AsyncApiClient::new(5, 4).unwrap();

    let assets = fetch_helius_collection_assets(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection111111111111111111111111111111111",
        1,
        0,
    )
    .await
    .unwrap();

    assert_eq!(assets.len(), 2);
    assert_eq!(
        assets[0].nft.contract_address,
        "Collection111111111111111111111111111111111"
    );
    assert_eq!(
        assets[0].nft.token_id,
        "So11111111111111111111111111111111111111112"
    );
    assert_eq!(
        assets[0].owner_address,
        "Vote111111111111111111111111111111111111111"
    );
    first.assert_async().await;
    second.assert_async().await;
}

#[tokio::test]
async fn real_api_routes_solana_seed_assets_to_helius() {
    let server = MockServer::start_async().await;
    let request = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "total": 1,
                    "items": [{
                        "id": "So11111111111111111111111111111111111111112",
                        "content": {"metadata": {"name": "One"}}
                    }]
                }
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let assets = api
        .fetch_seed_contract_nfts(
            "solana",
            "",
            None,
            "Collection111111111111111111111111111111111",
        )
        .await
        .unwrap();

    assert_eq!(assets.len(), 1);
    assert_eq!(
        assets[0].token_id,
        "So11111111111111111111111111111111111111112"
    );
    request.assert_async().await;
}

#[tokio::test]
async fn solana_pre_transaction_balance_uses_target_transaction_not_full_block() {
    let server = MockServer::start_async().await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getTransaction")
                .body_contains("mint-signature");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "slot": 42,
                    "transaction": {
                        "signatures": ["mint-signature"],
                        "message": {"accountKeys": ["payer", "minter"]}
                    },
                    "meta": {
                        "fee": 5000,
                        "preBalances": [1000000000_u64, 2500000000_u64],
                        "postBalances": [999995000_u64, 2500000000_u64]
                    }
                }
            }));
        })
        .await;
    let block = server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getBlock");
            then.status(500);
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let balance = api
        .fetch_pre_transaction_native_balance_on_chain(
            "solana",
            "",
            None,
            "mint-signature",
            "minter",
            42,
        )
        .await
        .unwrap();

    assert_eq!(balance, 2.5);
    assert_eq!(transaction.hits_async().await, 1);
    assert_eq!(block.hits_async().await, 0);
}

#[tokio::test]
async fn solana_target_transaction_evidence_is_fetched_once_across_consumers() {
    let server = MockServer::start_async().await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "slot": 42,
                    "transaction": {
                        "signatures": ["shared-signature"],
                        "message": {
                            "accountKeys": ["payer", "receiver"],
                            "instructions": [{"parsed": {"type": "transfer", "info": {
                                "source": "payer",
                                "destination": "receiver",
                                "lamports": 1_000_000_000_u64
                            }}}]
                        }
                    },
                    "meta": {
                        "err": null,
                        "fee": 5000,
                        "preBalances": [2_000_000_000_u64, 0],
                        "postBalances": [999_995_000_u64, 1_000_000_000_u64]
                    }
                }
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    api.fetch_transaction_receipt_on_chain("solana", "", None, "shared-signature")
        .await
        .unwrap();
    api.fetch_pre_transaction_native_balance_on_chain(
        "solana",
        "",
        None,
        "shared-signature",
        "payer",
        42,
    )
    .await
    .unwrap();
    let transfers = api
        .fetch_transaction_value_transfers_to_address_on_chain(
            "solana",
            "",
            None,
            "shared-signature",
            42,
            "receiver",
        )
        .await
        .unwrap();

    assert_eq!(transfers.len(), 1);
    assert_eq!(transaction.hits_async().await, 1);
}

#[tokio::test]
async fn real_api_builds_solana_metadata_and_owner_balances_from_das() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "total": 2,
                    "items": [
                        {
                            "id": "So11111111111111111111111111111111111111112",
                            "content": {"metadata": {"name": "Collection", "symbol": "COL"}},
                            "authorities": [{"address": "WrongAssetAuthority", "scopes": ["full"]}],
                            "grouping": [{
                                "group_key": "collection",
                                "group_value": "Collection111111111111111111111111111111111",
                                "collection_metadata": {
                                    "name": "Collection",
                                    "symbol": "COL",
                                    "update_authority": "CollectionAuthorityCaseSensitive"
                                }
                            }],
                            "ownership": {"owner": "Vote111111111111111111111111111111111111111"}
                        },
                        {
                            "id": "Vote111111111111111111111111111111111111111",
                            "content": {"metadata": {"name": "Collection #2", "symbol": "COL"}},
                            "ownership": {"owner": "Vote111111111111111111111111111111111111111"}
                        }
                    ]
                }
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();
    let collection = "Collection111111111111111111111111111111111";

    let metadata = api
        .fetch_contract_metadata("solana", "", None, "", collection)
        .await
        .unwrap();
    let owners = api
        .fetch_contract_owners("solana", "", None, collection)
        .await
        .unwrap();

    assert_eq!(metadata.chain, "solana");
    assert_eq!(metadata.contract_address, collection);
    assert_eq!(metadata.name, "Collection");
    assert_eq!(metadata.symbol, "COL");
    assert_eq!(metadata.owner_address, "CollectionAuthorityCaseSensitive");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].token_balances.len(), 2);
}

#[tokio::test]
async fn helius_collection_snapshot_reports_truncation_and_real_collection_metadata_shape() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "total": 3,
                    "items": [{
                        "id": "AssetOne111111111111111111111111111111111",
                        "content": {"metadata": {"name": "Asset #1"}},
                        "grouping": [{
                            "group_key": "collection",
                            "group_value": "Collection111111111111111111111111111111111",
                            "collection_metadata": {"name": "Real Collection", "symbol": "REAL"}
                        }]
                    }]
                }
            }));
        })
        .await;
    let client = AsyncApiClient::new(5, 2).unwrap();

    let snapshot = fetch_helius_collection_snapshot(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection111111111111111111111111111111111",
        100,
        1,
    )
    .await
    .unwrap();

    assert_eq!(snapshot.collection_name, "Real Collection");
    assert_eq!(snapshot.collection_symbol, "REAL");
    assert_eq!(snapshot.assets.len(), 1);
    assert_eq!(snapshot.total, 3);
    assert!(snapshot.truncated);
    assert_eq!(snapshot.coverage_ratio, Some(1.0 / 3.0));
}

#[tokio::test]
async fn helius_collection_snapshot_marks_unknown_total_as_truncated_at_asset_cap() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"items": [{"id": "AssetOne", "content": {"metadata": {}}}]}
            }));
        })
        .await;
    let client = AsyncApiClient::new(5, 2).unwrap();

    let snapshot = fetch_helius_collection_snapshot(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection",
        100,
        1,
    )
    .await
    .unwrap();

    assert_eq!(snapshot.assets.len(), 1);
    assert_eq!(snapshot.total, 1);
    assert!(snapshot.truncated);
    assert_eq!(snapshot.coverage_ratio, None);
}

#[tokio::test]
async fn helius_collection_snapshot_stops_on_repeated_full_page_and_deduplicates_mints() {
    let server = MockServer::start_async().await;
    let pages = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "items": [{"id": "RepeatedMint", "content": {"metadata": {}}}]
                }
            }));
        })
        .await;
    let client = AsyncApiClient::new(5, 2).unwrap();

    let snapshot = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        fetch_helius_collection_snapshot(
            &client,
            &format!("{}/?api-key=test", server.base_url()),
            "Collection",
            1,
            3,
        ),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(snapshot.assets.len(), 1);
    assert_eq!(snapshot.assets[0].nft.token_id, "RepeatedMint");
    assert!(snapshot.truncated);
    assert_eq!(pages.hits_async().await, 2);
}

#[tokio::test]
async fn helius_asset_history_normalizes_owner_change_as_transfer() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "total": 1,
                    "items": [["sig-one", "Transfer"]]
                }
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getTransaction")
                .body_contains("sig-one");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "slot": 42,
                    "blockTime": 1700000000,
                    "transaction": {
                        "signatures": ["sig-one"],
                        "message": {"accountKeys": ["FeePayer111111111111111111111111111111111"]}
                    },
                    "meta": {
                        "fee": 5000,
                        "preTokenBalances": [
                            {
                                "accountIndex": 1,
                                "mint": "So11111111111111111111111111111111111111112",
                                "owner": "OwnerBefore11111111111111111111111111111111",
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            },
                            {
                                "accountIndex": 2,
                                "mint": "So11111111111111111111111111111111111111112",
                                "owner": "OwnerAfter111111111111111111111111111111111",
                                "uiTokenAmount": {"amount": "0", "decimals": 0}
                            }
                        ],
                        "postTokenBalances": [
                            {
                                "accountIndex": 1,
                                "mint": "So11111111111111111111111111111111111111112",
                                "owner": "OwnerBefore11111111111111111111111111111111",
                                "uiTokenAmount": {"amount": "0", "decimals": 0}
                            },
                            {
                                "accountIndex": 2,
                                "mint": "So11111111111111111111111111111111111111112",
                                "owner": "OwnerAfter111111111111111111111111111111111",
                                "uiTokenAmount": {"amount": "1", "decimals": 0}
                            }
                        ]
                    }
                }
            }));
        })
        .await;
    let client = AsyncApiClient::new(5, 4).unwrap();

    let transfers = fetch_helius_asset_transfers(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection111111111111111111111111111111111",
        "So11111111111111111111111111111111111111112",
        0,
    )
    .await
    .unwrap();

    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].tx_hash, "sig-one");
    assert_eq!(transfers[0].block_number, 42);
    assert_eq!(transfers[0].event_type, "transfer");
    assert_eq!(
        transfers[0].from_address,
        "OwnerBefore11111111111111111111111111111111"
    );
    assert_eq!(
        transfers[0].to_address,
        "OwnerAfter111111111111111111111111111111111"
    );
}

#[tokio::test]
async fn helius_asset_history_parses_bubblegum_v1_compressed_transfer_owners() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"total": 1, "items": [["compressed-signature", "Transfer"]]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getTransaction")
                .body_contains("compressed-signature");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "slot": 99,
                    "transaction": {
                        "signatures": ["compressed-signature"],
                        "message": {
                            "accountKeys": ["payer"],
                            "instructions": [{
                                "programId": "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY",
                                "accounts": ["tree-config", "OldLeafOwner", "delegate", "NewLeafOwner"],
                                "data": "UJJfJRLDFLd"
                            }]
                        }
                    },
                    "meta": {"err": null, "preTokenBalances": [], "postTokenBalances": []}
                }
            }));
        })
        .await;
    let client = AsyncApiClient::new(5, 4).unwrap();

    let transfers = fetch_helius_asset_transfers(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection111111111111111111111111111111111",
        "CompressedAsset11111111111111111111111111111",
        100,
    )
    .await
    .unwrap();

    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].from_address, "OldLeafOwner");
    assert_eq!(transfers[0].to_address, "NewLeafOwner");
    assert_eq!(transfers[0].event_type, "transfer");
}

#[tokio::test]
async fn real_api_routes_solana_contract_transfers_to_helius_history() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"total": 1, "items": [{
                    "id": "So11111111111111111111111111111111111111112",
                    "content": {"metadata": {"name": "One"}}
                }]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"total": 1, "items": [["sig-one", "MintToCollectionV1"]]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "slot": 42,
                    "blockTime": 1700000000,
                    "transaction": {"signatures": ["sig-one"]},
                    "meta": {
                        "preTokenBalances": [{
                            "accountIndex": 1,
                            "mint": "So11111111111111111111111111111111111111112",
                            "owner": "OwnerAfter111111111111111111111111111111111",
                            "uiTokenAmount": {"amount": "0", "decimals": 0}
                        }],
                        "postTokenBalances": [{
                            "accountIndex": 1,
                            "mint": "So11111111111111111111111111111111111111112",
                            "owner": "OwnerAfter111111111111111111111111111111111",
                            "uiTokenAmount": {"amount": "1", "decimals": 0}
                        }]
                    }
                }
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let rows = api
        .fetch_contract_transfers(
            "solana",
            "",
            None,
            "",
            "Collection111111111111111111111111111111111",
            "NonFungible",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].event_type, "mint");
}

#[tokio::test]
async fn real_api_reads_solana_license_from_das_metadata_without_alchemy() {
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        "http://127.0.0.1:1/?api-key=test".to_string(),
    )
    .unwrap();
    let nfts = vec![top_contract_analysis_rs::models::SeedNft {
        chain: "solana".into(),
        metadata_json: r#"{"license":"license: CC0"}"#.into(),
        ..Default::default()
    }];

    let open = api
        .fetch_license_sample("solana", "", None, &nfts)
        .await
        .unwrap();

    assert!(open);
}

#[tokio::test]
async fn solana_transaction_adapter_parses_fee_balance_and_sol_flow() {
    let server = MockServer::start_async().await;
    let transaction = serde_json::json!({
        "slot": 42,
        "transaction": {
            "signatures": ["signature"],
            "message": {
                "accountKeys": [
                    {"pubkey": "payer", "signer": true},
                    {"pubkey": "receiver", "signer": false},
                    {"pubkey": "payer-usdc", "signer": false},
                    {"pubkey": "receiver-usdc", "signer": false},
                    {"pubkey": "payer-other", "signer": false},
                    {"pubkey": "receiver-other", "signer": false}
                ],
                "instructions": [{
                    "program": "system",
                    "parsed": {
                        "type": "transfer",
                        "info": {"source": "payer", "destination": "receiver", "lamports": 2_000_000_000_u64}
                    }
                }, {
                    "program": "spl-token",
                    "parsed": {
                        "type": "transferChecked",
                        "info": {
                            "source": "payer-usdc", "destination": "receiver-usdc",
                            "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                            "tokenAmount": {"amount": "2500000", "decimals": 6}
                        }
                    }
                }, {
                    "program": "spl-token-2022",
                    "parsed": {
                        "type": "transferChecked",
                        "info": {
                            "source": "payer-other", "destination": "receiver-other",
                            "mint": "OtherTokenMint111111111111111111111111111111",
                            "tokenAmount": {"amount": "750000000", "decimals": 8}
                        }
                    }
                }]
            }
        },
        "meta": {
            "fee": 5000,
            "preBalances": [5_000_000_000_u64, 0],
            "preTokenBalances": [
                {"accountIndex": 2, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "payer", "uiTokenAmount": {"amount": "2500000", "decimals": 6}},
                {"accountIndex": 3, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "receiver", "uiTokenAmount": {"amount": "0", "decimals": 6}}
            ],
            "postTokenBalances": [
                {"accountIndex": 2, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "payer", "uiTokenAmount": {"amount": "0", "decimals": 6}},
                {"accountIndex": 3, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "receiver", "uiTokenAmount": {"amount": "2500000", "decimals": 6}},
                {"accountIndex": 4, "mint": "OtherTokenMint111111111111111111111111111111", "owner": "payer", "uiTokenAmount": {"amount": "0", "decimals": 8}},
                {"accountIndex": 5, "mint": "OtherTokenMint111111111111111111111111111111", "owner": "receiver", "uiTokenAmount": {"amount": "750000000", "decimals": 8}}
            ],
            "err": null
        }
    });
    let transaction_response = transaction.clone();
    let transaction_mock = server
        .mock_async(move |when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": transaction_response
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let receipt = api
        .fetch_transaction_receipt_on_chain("solana", "", None, "signature")
        .await
        .unwrap();

    assert_eq!(receipt.tx_hash, "signature");
    assert_eq!(receipt.from_address, "payer");
    assert_eq!(receipt.gas_used, 0);
    assert_eq!(receipt.effective_gas_price_wei, 0);
    assert_eq!(receipt.fee_native, Some(0.000005));

    let client = AsyncApiClient::new(5, 2).unwrap();
    let details = fetch_helius_transaction_details(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "signature",
        Some(100.0),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(details.pre_balances_native["payer"], 5.0);
    assert_eq!(
        details.account_keys,
        [
            "payer",
            "receiver",
            "payer-usdc",
            "receiver-usdc",
            "payer-other",
            "receiver-other"
        ]
    );
    assert_eq!(details.native_transfers[0].value_eth, 2.0);
    assert_eq!(details.native_transfers[0].value_usd, Some(200.0));
    assert_eq!(details.native_transfers[0].payment_token_symbol, "SOL");
    let usdc = details
        .native_transfers
        .iter()
        .find(|transfer| transfer.payment_token_symbol == "USDC")
        .unwrap();
    assert_eq!(usdc.from_address, "payer");
    assert_eq!(usdc.to_address, "receiver");
    assert_eq!(usdc.value_usd, Some(2.5));
    assert_eq!(usdc.value_eth, 0.025);
    let other = details
        .native_transfers
        .iter()
        .find(|transfer| {
            transfer.payment_token_address == "OtherTokenMint111111111111111111111111111111"
        })
        .unwrap();
    assert_eq!(other.from_address, "payer");
    assert_eq!(other.to_address, "receiver");
    assert_eq!(other.payment_token_symbol, "SPL");
    assert_eq!(transaction_mock.hits_async().await, 2);
}

#[tokio::test]
async fn real_api_builds_solana_sale_from_helius_owner_and_usdc_flow() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 1, "items": [{
                    "id": "Asset1111111111111111111111111111111111111",
                    "content": {"metadata": {"name": "Asset"}}
                }]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 1, "items": [["sale-signature", "Transfer"]]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {
                    "slot": 77,
                    "blockTime": 1700000000,
                    "transaction": {"signatures": ["sale-signature"], "message": {
                        "accountKeys": ["buyer", "seller", "asset-seller", "asset-buyer", "buyer-usdc", "seller-usdc", "buyer-ata", "other-usdc"],
                        "instructions": [
                            {"program": "system", "parsed": {"type": "createAccount", "info": {
                                "source": "buyer", "newAccount": "buyer-ata", "lamports": 2_039_280_u64
                            }}},
                            {"program": "spl-token", "parsed": {"type": "transferChecked", "info": {
                                "source": "buyer-usdc", "destination": "seller-usdc",
                                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                                "tokenAmount": {"amount": "2500000", "decimals": 6}
                            }}},
                            {"program": "spl-token", "parsed": {"type": "transferChecked", "info": {
                                "source": "buyer-usdc", "destination": "other-usdc",
                                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                                "tokenAmount": {"amount": "9000000", "decimals": 6}
                            }}}
                        ]
                    }},
                    "meta": {
                        "err": null,
                        "preTokenBalances": [
                            {"accountIndex": 2, "mint": "Asset1111111111111111111111111111111111111", "owner": "seller", "uiTokenAmount": {"amount": "1", "decimals": 0}},
                            {"accountIndex": 3, "mint": "Asset1111111111111111111111111111111111111", "owner": "buyer", "uiTokenAmount": {"amount": "0", "decimals": 0}},
                            {"accountIndex": 4, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "buyer", "uiTokenAmount": {"amount": "11500000", "decimals": 6}},
                            {"accountIndex": 5, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "seller", "uiTokenAmount": {"amount": "0", "decimals": 6}},
                            {"accountIndex": 7, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "other", "uiTokenAmount": {"amount": "0", "decimals": 6}}
                        ],
                        "postTokenBalances": [
                            {"accountIndex": 2, "mint": "Asset1111111111111111111111111111111111111", "owner": "seller", "uiTokenAmount": {"amount": "0", "decimals": 0}},
                            {"accountIndex": 3, "mint": "Asset1111111111111111111111111111111111111", "owner": "buyer", "uiTokenAmount": {"amount": "1", "decimals": 0}},
                            {"accountIndex": 4, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "buyer", "uiTokenAmount": {"amount": "0", "decimals": 6}},
                            {"accountIndex": 5, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "seller", "uiTokenAmount": {"amount": "2500000", "decimals": 6}},
                            {"accountIndex": 7, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "other", "uiTokenAmount": {"amount": "9000000", "decimals": 6}}
                        ]
                    }
                }
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let sales = api
        .fetch_contract_sales(
            "solana",
            "",
            None,
            "Collection111111111111111111111111111111111",
            "",
        )
        .await
        .unwrap();

    assert_eq!(sales.len(), 1);
    assert_eq!(sales[0].seller_address, "seller");
    assert_eq!(sales[0].buyer_address, "buyer");
    assert_eq!(sales[0].payment_token_symbol, "USDC");
    assert_eq!(sales[0].price_usd, Some(2.5));
    assert_eq!(sales[0].source, "helius");
}

#[tokio::test]
async fn solana_owner_change_with_only_account_creation_rent_is_not_a_sale() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 1, "items": [{
                    "id": "GiftAsset11111111111111111111111111111111111", "content": {"metadata": {}}
                }]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 1, "items": [["gift-signature", "Transfer"]]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {
                    "slot": 80,
                    "transaction": {"signatures": ["gift-signature"], "message": {
                        "accountKeys": ["buyer", "seller", "buyer-ata"],
                        "instructions": [{"program": "system", "parsed": {"type": "createAccount", "info": {
                            "source": "buyer", "newAccount": "buyer-ata", "lamports": 2_039_280_u64
                        }}}]
                    }},
                    "meta": {
                        "err": null,
                        "preTokenBalances": [
                            {"accountIndex": 1, "mint": "GiftAsset11111111111111111111111111111111111", "owner": "seller", "uiTokenAmount": {"amount": "1", "decimals": 0}},
                            {"accountIndex": 2, "mint": "GiftAsset11111111111111111111111111111111111", "owner": "buyer", "uiTokenAmount": {"amount": "0", "decimals": 0}}
                        ],
                        "postTokenBalances": [
                            {"accountIndex": 1, "mint": "GiftAsset11111111111111111111111111111111111", "owner": "seller", "uiTokenAmount": {"amount": "0", "decimals": 0}},
                            {"accountIndex": 2, "mint": "GiftAsset11111111111111111111111111111111111", "owner": "buyer", "uiTokenAmount": {"amount": "1", "decimals": 0}}
                        ]
                    }
                }
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let sales = api
        .fetch_contract_sales(
            "solana",
            "",
            None,
            "Collection111111111111111111111111111111111",
            "",
        )
        .await
        .unwrap();

    assert!(sales.is_empty());
}

#[tokio::test]
async fn solana_collection_history_keeps_successful_assets_when_one_asset_fails() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 2, "items": [
                    {"id": "GoodAsset111111111111111111111111111111111", "content": {"metadata": {}}},
                    {"id": "BadAsset1111111111111111111111111111111111", "content": {"metadata": {}}}
                ]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset")
                .body_contains("GoodAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 2, "items": [
                    ["good-signature", "MintToCollectionV1"],
                    ["missing-detail-signature", "Transfer"]
                ]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getTransaction")
                .body_contains("missing-detail-signature");
            then.status(400)
                .json_body_obj(&serde_json::json!({"error": "missing transaction detail"}));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset")
                .body_contains("BadAsset");
            then.status(400)
                .json_body_obj(&serde_json::json!({"error": "bad asset"}));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction").body_contains("good-signature");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {
                    "slot": 1,
                    "transaction": {"signatures": ["good-signature"]},
                    "meta": {
                        "err": null,
                        "preTokenBalances": [{"accountIndex": 1, "mint": "GoodAsset111111111111111111111111111111111", "owner": "owner", "uiTokenAmount": {"amount": "0", "decimals": 0}}],
                        "postTokenBalances": [{"accountIndex": 1, "mint": "GoodAsset111111111111111111111111111111111", "owner": "owner", "uiTokenAmount": {"amount": "1", "decimals": 0}}]
                    }
                }
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let rows = api
        .fetch_contract_transfers(
            "solana",
            "",
            None,
            "",
            "Collection111111111111111111111111111111111",
            "NonFungible",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].token_id,
        "GoodAsset111111111111111111111111111111111"
    );
    let quality = api
        .fetch_provider_data_quality("solana", "Collection111111111111111111111111111111111")
        .await
        .unwrap();
    assert_eq!(quality.asset_listing_analyzed_count, 2);
    assert_eq!(quality.asset_listing_total_count, 2);
    assert_eq!(quality.history_failed_asset_count, 1);
    assert_eq!(quality.history_fetched_transaction_count, 1);
    assert_eq!(quality.history_reported_transaction_count, 2);
    assert_eq!(quality.history_failed_transaction_count, 1);
    let quality_json = serde_json::to_value(&quality).unwrap();
    assert_eq!(quality_json["history_requested_asset_count"], 2);
    assert_eq!(quality_json["history_successful_asset_count"], 1);
}

#[tokio::test]
async fn solana_collection_history_fetches_shared_transaction_once_across_assets() {
    let server = MockServer::start_async().await;
    let signatures = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 1, "items": [["shared-signature", "Transfer"]]}
            }));
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getTransaction")
                .body_contains("shared-signature");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {
                    "slot": 9,
                    "transaction": {"signatures": ["shared-signature"], "message": {
                        "accountKeys": ["buyer", "seller", "asset-one-seller", "asset-one-buyer", "asset-two-seller", "asset-two-buyer", "buyer-usdc", "seller-usdc"],
                        "instructions": [{"program": "spl-token", "parsed": {"type": "transferChecked", "info": {
                            "source": "buyer-usdc", "destination": "seller-usdc",
                            "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                            "tokenAmount": {"amount": "2500000", "decimals": 6}
                        }}}]
                    }},
                    "meta": {
                        "err": null,
                        "preTokenBalances": [
                            {"accountIndex": 2, "mint": "AssetOne", "owner": "seller", "uiTokenAmount": {"amount": "1", "decimals": 0}},
                            {"accountIndex": 3, "mint": "AssetOne", "owner": "buyer", "uiTokenAmount": {"amount": "0", "decimals": 0}},
                            {"accountIndex": 4, "mint": "AssetTwo", "owner": "seller", "uiTokenAmount": {"amount": "1", "decimals": 0}},
                            {"accountIndex": 5, "mint": "AssetTwo", "owner": "buyer", "uiTokenAmount": {"amount": "0", "decimals": 0}},
                            {"accountIndex": 6, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "buyer", "uiTokenAmount": {"amount": "2500000", "decimals": 6}},
                            {"accountIndex": 7, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "seller", "uiTokenAmount": {"amount": "0", "decimals": 6}}
                        ],
                        "postTokenBalances": [
                            {"accountIndex": 2, "mint": "AssetOne", "owner": "seller", "uiTokenAmount": {"amount": "0", "decimals": 0}},
                            {"accountIndex": 3, "mint": "AssetOne", "owner": "buyer", "uiTokenAmount": {"amount": "1", "decimals": 0}},
                            {"accountIndex": 4, "mint": "AssetTwo", "owner": "seller", "uiTokenAmount": {"amount": "0", "decimals": 0}},
                            {"accountIndex": 5, "mint": "AssetTwo", "owner": "buyer", "uiTokenAmount": {"amount": "1", "decimals": 0}},
                            {"accountIndex": 6, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "buyer", "uiTokenAmount": {"amount": "0", "decimals": 6}},
                            {"accountIndex": 7, "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "owner": "seller", "uiTokenAmount": {"amount": "2500000", "decimals": 6}}
                        ]
                    }
                }
            }));
        })
        .await;
    let assets = ["AssetOne", "AssetTwo"]
        .into_iter()
        .map(|token_id| HeliusCollectionAsset {
            nft: SeedNft {
                chain: "solana".into(),
                contract_address: "Collection".into(),
                token_id: token_id.into(),
                ..SeedNft::default()
            },
            owner_address: String::new(),
            compressed: false,
        })
        .collect::<Vec<_>>();
    let client = AsyncApiClient::new(5, 4).unwrap();

    let history = fetch_helius_assets_history(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection",
        &assets,
        100,
    )
    .await
    .unwrap();

    assert_eq!(transaction.hits_async().await, 1);
    assert_eq!(signatures.hits_async().await, 2);
    assert_eq!(history.parsed_transaction_count, 1);
    assert_eq!(history.sales.len(), 2);
    assert!(history.sales.iter().all(|sale| sale.price_usd.is_none()));
}

#[tokio::test]
async fn solana_collection_history_enforces_total_transaction_budget() {
    let server = MockServer::start_async().await;
    let signatures = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"total": 1, "items": [["one-signature", "Transfer"]]}
            }));
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": null
            }));
        })
        .await;
    let assets = ["AssetOne", "AssetTwo"]
        .into_iter()
        .map(|token_id| HeliusCollectionAsset {
            nft: SeedNft {
                token_id: token_id.into(),
                ..SeedNft::default()
            },
            owner_address: String::new(),
            compressed: false,
        })
        .collect::<Vec<_>>();
    let client = AsyncApiClient::new(5, 4).unwrap();

    let history = fetch_helius_assets_history_with_budget(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection",
        &assets,
        100,
        1,
    )
    .await
    .unwrap();

    assert_eq!(transaction.hits_async().await, 1);
    assert_eq!(signatures.hits_async().await, 1);
    assert_eq!(history.requested_asset_count, 1);
    assert_eq!(history.reported_transaction_count, 1);
    assert!(history.truncated_asset_history_count > 0);
}

#[tokio::test]
async fn solana_collection_history_empty_plans_do_not_consume_signature_budget() {
    let server = MockServer::start_async().await;
    let signatures = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"total": 0, "items": []}
            }));
        })
        .await;
    let assets = ["AssetOne", "AssetTwo"]
        .into_iter()
        .map(|token_id| HeliusCollectionAsset {
            nft: SeedNft {
                token_id: token_id.into(),
                ..SeedNft::default()
            },
            owner_address: String::new(),
            compressed: false,
        })
        .collect::<Vec<_>>();
    let client = AsyncApiClient::new(5, 4).unwrap();

    let history = fetch_helius_assets_history_with_budget(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection",
        &assets,
        100,
        1,
    )
    .await
    .unwrap();

    assert_eq!(signatures.hits_async().await, 2);
    assert_eq!(history.requested_asset_count, 2);
    assert_eq!(history.successful_asset_count, 2);
    assert_eq!(history.truncated_asset_history_count, 0);
}

#[tokio::test]
async fn solana_collection_history_failed_plans_do_not_consume_signature_budget() {
    let server = MockServer::start_async().await;
    let failed_signatures = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset")
                .body_contains("AssetOne");
            then.status(400);
        })
        .await;
    let successful_signatures = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset")
                .body_contains("AssetTwo");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"total": 1, "items": [["second-signature", "Transfer"]]}
            }));
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": null
            }));
        })
        .await;
    let assets = ["AssetOne", "AssetTwo"]
        .into_iter()
        .map(|token_id| HeliusCollectionAsset {
            nft: SeedNft {
                token_id: token_id.into(),
                ..SeedNft::default()
            },
            owner_address: String::new(),
            compressed: false,
        })
        .collect::<Vec<_>>();
    let client = AsyncApiClient::new(5, 1).unwrap();

    let history = fetch_helius_assets_history_with_budget(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection",
        &assets,
        100,
        1,
    )
    .await
    .unwrap();

    assert_eq!(failed_signatures.hits_async().await, 1);
    assert_eq!(successful_signatures.hits_async().await, 1);
    assert_eq!(transaction.hits_async().await, 1);
    assert_eq!(history.requested_asset_count, 2);
    assert_eq!(history.failed_asset_count, 1);
    assert_eq!(history.successful_asset_count, 1);
    assert_eq!(history.truncated_asset_history_count, 0);
}

#[tokio::test]
async fn solana_collection_history_allocates_budget_across_assets_and_downstreams_limit() {
    let server = MockServer::start_async().await;
    let signatures = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset")
                .body_contains("\"limit\":1");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"total": 1, "items": [["shared-signature", "Transfer"]]}
            }));
        })
        .await;
    let transaction = server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": null
            }));
        })
        .await;
    let assets = ["AssetOne", "AssetTwo", "AssetThree"]
        .into_iter()
        .map(|token_id| HeliusCollectionAsset {
            nft: SeedNft {
                token_id: token_id.into(),
                ..SeedNft::default()
            },
            owner_address: String::new(),
            compressed: false,
        })
        .collect::<Vec<_>>();
    let client = AsyncApiClient::new(5, 4).unwrap();

    let history = fetch_helius_assets_history_with_budget(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection",
        &assets,
        100,
        3,
    )
    .await
    .unwrap();

    assert_eq!(signatures.hits_async().await, 3);
    assert_eq!(transaction.hits_async().await, 1);
    assert_eq!(history.requested_asset_count, 3);
    assert_eq!(history.successful_asset_count, 3);
    assert_eq!(history.truncated_asset_history_count, 0);
}

#[tokio::test]
async fn compressed_mint_history_does_not_fabricate_historical_owner_from_current_snapshot() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getAssetsByGroup");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 1, "items": [{
                    "id": "CompressedAsset11111111111111111111111111111",
                    "content": {"metadata": {}},
                    "compression": {"compressed": true},
                    "ownership": {"owner": "CurrentOwnerCaseSensitive"}
                }]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {"total": 1, "items": [["mint-signature", "MintToCollectionV1"]]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0", "result": {
                    "slot": 2,
                    "transaction": {"signatures": ["mint-signature"], "message": {
                        "accountKeys": ["tree", "WrongOwnerMustNotBeUsed"],
                        "instructions": [{
                            "programId": "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY",
                            "accounts": [0, 1],
                            "data": "11111111"
                        }]
                    }},
                    "meta": {"err": null, "preTokenBalances": [], "postTokenBalances": []}
                }
            }));
        })
        .await;
    let api = RealApi::new_with_helius_endpoint(
        5,
        2,
        2,
        10,
        2,
        format!("{}/?api-key=test", server.base_url()),
    )
    .unwrap();

    let rows = api
        .fetch_contract_transfers(
            "solana",
            "",
            None,
            "",
            "Collection111111111111111111111111111111111",
            "NonFungible",
        )
        .await
        .unwrap();

    assert!(rows.is_empty());
}

#[tokio::test]
async fn compressed_mint_history_uses_bubblegum_leaf_owner_evidence() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"total": 1, "items": [["mint-signature", "MintToCollectionV1"]]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "slot": 2,
                    "transaction": {"signatures": ["mint-signature"], "message": {
                        "accountKeys": ["tree", "HistoricalOwnerCaseSensitive"],
                        "instructions": [{
                            "programId": "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY",
                            "accounts": [0, 1],
                            "data": "Sc11WcT5qXc"
                        }]
                    }},
                    "meta": {"err": null, "preTokenBalances": [], "postTokenBalances": []}
                }
            }));
        })
        .await;
    let client = AsyncApiClient::new(5, 2).unwrap();
    let assets = vec![HeliusCollectionAsset {
        nft: SeedNft {
            token_id: "CompressedAsset".into(),
            ..SeedNft::default()
        },
        owner_address: "CurrentOwnerMustNotBeUsed".into(),
        compressed: true,
    }];

    let history = fetch_helius_assets_history(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection",
        &assets,
        100,
    )
    .await
    .unwrap();

    assert_eq!(history.unresolved_compressed_mint_count, 0);
    assert_eq!(history.transfers.len(), 1);
    assert_eq!(history.transfers[0].event_type, "mint");
    assert_eq!(
        history.transfers[0].to_address,
        "HistoricalOwnerCaseSensitive"
    );
}

#[tokio::test]
async fn compressed_mint_history_continues_after_incomplete_bubblegum_instruction() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/")
                .body_contains("getSignaturesForAsset");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {"total": 1, "items": [["mint-signature", "MintToCollectionV1"]]}
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/").body_contains("getTransaction");
            then.status(200).json_body_obj(&serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "slot": 2,
                    "transaction": {"signatures": ["mint-signature"], "message": {
                        "accountKeys": ["tree", "HistoricalOwnerCaseSensitive"],
                        "instructions": [
                            {
                                "programId": "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY",
                                "accounts": [0],
                                "data": "Sc11WcT5qXc"
                            },
                            {
                                "programId": "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY",
                                "accounts": [0, 1],
                                "data": "Sc11WcT5qXc"
                            }
                        ]
                    }},
                    "meta": {"err": null, "preTokenBalances": [], "postTokenBalances": []}
                }
            }));
        })
        .await;
    let client = AsyncApiClient::new(5, 2).unwrap();
    let assets = vec![HeliusCollectionAsset {
        nft: SeedNft {
            token_id: "CompressedAsset".into(),
            ..SeedNft::default()
        },
        owner_address: String::new(),
        compressed: true,
    }];

    let history = fetch_helius_assets_history(
        &client,
        &format!("{}/?api-key=test", server.base_url()),
        "Collection",
        &assets,
        100,
    )
    .await
    .unwrap();

    assert_eq!(history.transfers.len(), 1);
    assert_eq!(
        history.transfers[0].to_address,
        "HistoricalOwnerCaseSensitive"
    );
}
