use httpmock::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use top_contract_analysis_rs::api::{
    fetch_contract_metadata, fetch_contract_metadata_with_opensea_fallback, fetch_contract_owners,
    fetch_contract_sales, fetch_contract_transfers, fetch_eth_balance, fetch_is_holder_of_contract,
    fetch_license_sample, fetch_opensea_account_holds_contract_nft,
    fetch_opensea_contract_collection_slug, fetch_opensea_contract_market_events,
    fetch_opensea_contract_metadata, fetch_opensea_contract_nfts,
    fetch_same_block_eth_transfers_for_address, fetch_seed_contract_nfts,
    fetch_transaction_receipt, fetch_transaction_receipts_for_block, is_open_license_payload,
    ApiEndpoints, AsyncApiClient,
};
use top_contract_analysis_rs::models::SeedNft;

fn test_client() -> AsyncApiClient {
    AsyncApiClient::new(5, 4).unwrap()
}

fn test_endpoints(base_url: &str) -> ApiEndpoints {
    ApiEndpoints {
        alchemy_nft_v2_base: format!("{base_url}/nft/v2/key"),
        alchemy_nft_v3_base: format!("{base_url}/nft/v3/key"),
        alchemy_rpc_base: format!("{base_url}/v2/key"),
        etherscan_base: base_url.to_string(),
        opensea_base: base_url.to_string(),
    }
}

#[tokio::test]
async fn api_client_error_includes_status_and_response_body() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET).path("/bad-request");
            then.status(400)
                .header("content-type", "application/json")
                .body(r#"{"errors":["asset_contract_address is not supported"]}"#);
        })
        .await;

    let client = test_client();
    let err = client
        .get_json::<serde_json::Value>(&format!("{}/bad-request", server.base_url()))
        .await
        .expect_err("bad request should include response body");
    let message = err.to_string();

    assert!(message.contains("400 Bad Request"), "{message}");
    assert!(
        message.contains("asset_contract_address is not supported"),
        "{message}"
    );
}

#[test]
fn alchemy_endpoints_embed_api_key_in_rest_and_rpc_urls() {
    let endpoints = ApiEndpoints::for_alchemy("eth-mainnet", "live-key");

    assert_eq!(
        endpoints.alchemy_nft_v2_base,
        "https://eth-mainnet.g.alchemy.com/nft/v2/live-key"
    );
    assert_eq!(
        endpoints.alchemy_nft_v3_base,
        "https://eth-mainnet.g.alchemy.com/nft/v3/live-key"
    );
    assert_eq!(
        endpoints.alchemy_rpc_base,
        "https://eth-mainnet.g.alchemy.com/v2/live-key"
    );
}

async fn spawn_sequential_json_server(
    expected_requests: Vec<(String, serde_json::Value)>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        for (expected_target, body) in expected_requests {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 8192];
            let read = stream.read(&mut buffer).await.unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]);
            let request_line = request.lines().next().unwrap_or("");
            assert!(
                request_line.starts_with(&format!("GET {expected_target} HTTP/1.1")),
                "unexpected request line: {request_line}",
            );

            let payload = body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                payload.len(),
                payload
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    (format!("http://{address}"), handle)
}

#[tokio::test]
async fn fetch_opensea_contract_metadata_parses_contract_fields() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xseed")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "address": "0xSeed",
                "contract_standard": "erc721",
                "contract_deployer": "0xCreator",
                "deployed_block_number": "123",
                "collection": {"name": "OpenSea Azuki"},
                "symbol": "AZ"
            }));
        })
        .await;

    let client = test_client();
    let meta = fetch_opensea_contract_metadata(
        &client,
        &server.base_url(),
        "ethereum",
        "0xseed",
        "opensea",
    )
    .await
    .unwrap();

    assert_eq!(meta.contract_address, "0xseed");
    assert_eq!(meta.token_type, "ERC721");
    assert_eq!(meta.contract_deployer, "0xcreator");
    assert_eq!(meta.deployed_block_number, 123);
    assert_eq!(meta.name, "OpenSea Azuki");
    assert_eq!(meta.symbol, "AZ");
}

#[tokio::test]
async fn fetch_opensea_contract_collection_slug_reads_seed_collection() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xseed")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "address": "0xSeed",
                "collection": {"slug": "pudgy-penguins", "name": "Pudgy Penguins"}
            }));
        })
        .await;

    let client = test_client();
    let slug = fetch_opensea_contract_collection_slug(
        &client,
        &server.base_url(),
        "ethereum",
        "0xseed",
        "opensea",
    )
    .await
    .unwrap();

    assert_eq!(slug.as_deref(), Some("pudgy-penguins"));
}

#[tokio::test]
async fn fetch_opensea_contract_nfts_paginates_contract_tokens() {
    let (base_url, server) = spawn_sequential_json_server(vec![
        (
            "/api/v2/chain/ethereum/contract/0xdup/nfts?limit=200".to_string(),
            serde_json::json!({
                "nfts": [{
                    "identifier": "1",
                    "contract": "0xdup",
                    "name": "Mirror #1",
                    "metadata_url": "ipfs://candidate/1",
                    "image_url": "ipfs://candidate/1.png",
                    "metadata": {"description": "gold dragon"}
                }],
                "next": "cursor"
            }),
        ),
        (
            "/api/v2/chain/ethereum/contract/0xdup/nfts?limit=200&next=cursor".to_string(),
            serde_json::json!({
                "nfts": [{
                    "identifier": "0x2",
                    "contract": "0xdup",
                    "name": "Mirror #2",
                    "display_image_url": "ipfs://candidate/2.png"
                }]
            }),
        ),
    ])
    .await;

    let client = test_client();
    let rows = fetch_opensea_contract_nfts(&client, &base_url, "ethereum", "0xdup", "opensea")
        .await
        .unwrap();
    server.await.unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].contract_address, "0xdup");
    assert_eq!(rows[0].token_id, "1");
    assert_eq!(rows[1].token_id, "2");
    assert_eq!(rows[1].image_uri, "ipfs://candidate/2.png");
}

#[tokio::test]
async fn fetch_opensea_account_holds_contract_nft_checks_contract_without_token_overlap() {
    let (base_url, server) = spawn_sequential_json_server(vec![(
        "/api/v2/chain/ethereum/account/0xwrapped/nfts?limit=200&collection=pudgy-penguins"
            .to_string(),
        serde_json::json!({
            "nfts": [{
                "identifier": "8888",
                "contract": "0xSeed",
                "name": "Seed #8888"
            }]
        }),
    )])
    .await;

    let client = test_client();
    let holds_seed_nft = fetch_opensea_account_holds_contract_nft(
        &client,
        &base_url,
        "ethereum",
        "0xwrapped",
        "0xseed",
        "opensea",
        Some("pudgy-penguins"),
    )
    .await
    .unwrap();
    server.await.unwrap();

    assert!(holds_seed_nft);
}

#[tokio::test]
async fn fetch_is_holder_of_contract_calls_alchemy_lightweight_holder_endpoint() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/nft/v3/key/isHolderOfContract")
                .query_param("wallet", "0xwrapped")
                .query_param("contractAddress", "0xseed");
            then.status(200).json_body_obj(&serde_json::json!({
                "isHolderOfContract": true
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let is_holder = fetch_is_holder_of_contract(&client, &endpoints, "0xwrapped", "0xseed")
        .await
        .unwrap();

    assert!(is_holder);
}

#[tokio::test]
async fn fetch_seed_contract_nfts_paginates_until_page_key_is_empty() {
    let (base_url, server) = spawn_sequential_json_server(vec![
        (
            "/nft/v3/key/getNFTsForContract?contractAddress=0xseed&withMetadata=true".to_string(),
            serde_json::json!({
                "nfts": [{
                    "contract": {"address": "0xseed"},
                    "id": {"tokenId": "0x1"},
                    "title": "Azuki #1",
                    "contractMetadata": {"symbol": "AZUKI"},
                    "tokenUri": {"raw": "ipfs://seed/1"},
                    "image": {"originalUrl": "ipfs://image/1.png"}
                }],
                "pageKey": "next-page"
            }),
        ),
        (
            "/nft/v3/key/getNFTsForContract?contractAddress=0xseed&withMetadata=true&startToken=next-page".to_string(),
            serde_json::json!({
                "nfts": [{
                    "contract": {"address": "0xseed"},
                    "id": {"tokenId": "0x2"},
                    "title": "Azuki #2",
                    "contractMetadata": {"symbol": "AZUKI"},
                    "tokenUri": "ipfs://seed/2",
                    "image": "ipfs://image/2.png"
                }]
            }),
        ),
    ])
    .await;

    let client = test_client();
    let endpoints = test_endpoints(&base_url);
    let rows = fetch_seed_contract_nfts(&client, &endpoints, "ethereum", "0xseed")
        .await
        .unwrap();
    server.await.unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].token_id, "1");
    assert_eq!(rows[1].token_id, "2");
    assert_eq!(rows[1].token_uri, "ipfs://seed/2");
}

#[tokio::test]
async fn fetch_seed_contract_nfts_reads_current_v3_top_level_token_id() {
    let (base_url, server) = spawn_sequential_json_server(vec![(
        "/nft/v3/key/getNFTsForContract?contractAddress=0xseed&withMetadata=true".to_string(),
        serde_json::json!({
            "nfts": [
                {
                    "contract": {"address": "0xseed"},
                    "tokenId": "44",
                    "name": "Azuki #44",
                    "contractMetadata": {"symbol": "AZUKI"},
                    "raw": {"tokenUri": "ipfs://seed/44"},
                    "image": {"originalUrl": "ipfs://image/44.png"}
                },
                {
                    "contract": {"address": "0xseed"},
                    "tokenId": "0x2d",
                    "name": "Azuki #45",
                    "contractMetadata": {"symbol": "AZUKI"},
                    "tokenUri": "ipfs://seed/45",
                    "image": {"originalUrl": "ipfs://image/45.png"}
                }
            ]
        }),
    )])
    .await;

    let client = test_client();
    let endpoints = test_endpoints(&base_url);
    let rows = fetch_seed_contract_nfts(&client, &endpoints, "ethereum", "0xseed")
        .await
        .unwrap();
    server.await.unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].token_id, "44");
    assert_eq!(rows[0].token_uri, "ipfs://seed/44");
    assert_eq!(rows[1].token_id, "45");
}

#[tokio::test]
async fn fetch_seed_contract_nfts_errors_when_page_key_repeats() {
    let (base_url, server) = spawn_sequential_json_server(vec![
        (
            "/nft/v3/key/getNFTsForContract?contractAddress=0xseed&withMetadata=true".to_string(),
            serde_json::json!({
                "nfts": [],
                "pageKey": "looping-page"
            }),
        ),
        (
            "/nft/v3/key/getNFTsForContract?contractAddress=0xseed&withMetadata=true&startToken=looping-page".to_string(),
            serde_json::json!({
                "nfts": [],
                "pageKey": "looping-page"
            }),
        ),
    ])
    .await;

    let client = test_client();
    let endpoints = test_endpoints(&base_url);
    let err = fetch_seed_contract_nfts(&client, &endpoints, "ethereum", "0xseed")
        .await
        .unwrap_err();
    server.await.unwrap();

    assert!(err.to_string().contains("repeated pageKey"));
}

#[tokio::test]
async fn fetch_contract_metadata_parses_contract_metadata_fields() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/nft/v3/key/getContractMetadata")
                .query_param("contractAddress", "0xseed");
            then.status(200).json_body_obj(&serde_json::json!({
                "address": "0xseed",
                "contractMetadata": {
                    "tokenType": "ERC721",
                    "contractDeployer": "0xcreator",
                    "deployedBlockNumber": 123,
                    "ownerAddress": "0xOwner",
                    "adminAddress": "0xAdmin",
                    "proxyAdminAddress": "0xProxyAdmin",
                    "name": "Azuki",
                    "symbol": "AZUKI"
                }
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let meta = fetch_contract_metadata(&client, &endpoints, "ethereum", "0xseed")
        .await
        .unwrap();

    assert_eq!(meta.contract_address, "0xseed");
    assert_eq!(meta.token_type, "ERC721");
    assert_eq!(meta.contract_deployer, "0xcreator");
    assert_eq!(meta.deployed_block_number, 123);
    assert_eq!(meta.owner_address, "0xowner");
    assert_eq!(meta.admin_address, "0xadmin");
    assert_eq!(meta.proxy_admin_address, "0xproxyadmin");
}

#[tokio::test]
async fn fetch_contract_metadata_enriches_control_addresses_from_rpc() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/nft/v3/key/getContractMetadata")
                .query_param("contractAddress", "0xseed");
            then.status(200).json_body_obj(&serde_json::json!({
                "address": "0xseed",
                "contractMetadata": {
                    "tokenType": "ERC721",
                    "contractDeployer": "0xcreator",
                    "deployedBlockNumber": 123,
                    "name": "Azuki",
                    "symbol": "AZUKI"
                }
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/v2/key")
                .body_contains("eth_call")
                .body_contains("8da5cb5b");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": "0x0000000000000000000000001111111111111111111111111111111111111111"
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/v2/key")
                .body_contains("eth_call")
                .body_contains("f851a440");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": "0x0000000000000000000000002222222222222222222222222222222222222222"
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/v2/key")
                .body_contains("eth_getStorageAt")
                .body_contains("b53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": "0x0000000000000000000000003333333333333333333333333333333333333333"
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let meta = fetch_contract_metadata(&client, &endpoints, "ethereum", "0xseed")
        .await
        .unwrap();

    assert_eq!(
        meta.owner_address,
        "0x1111111111111111111111111111111111111111"
    );
    assert_eq!(
        meta.admin_address,
        "0x2222222222222222222222222222222222222222"
    );
    assert_eq!(
        meta.proxy_admin_address,
        "0x3333333333333333333333333333333333333333"
    );
}

#[tokio::test]
async fn contract_info_uses_alchemy_before_opensea_when_available() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/nft/v3/key/getContractMetadata")
                .query_param("contractAddress", "0xseed");
            then.status(200).json_body_obj(&serde_json::json!({
                "address": "0xseed",
                "contractMetadata": {
                    "tokenType": "ERC721",
                    "contractDeployer": "0xAlchemyCreator",
                    "deployedBlockNumber": 456,
                    "name": "Alchemy Contract",
                    "symbol": "ALC"
                }
            }));
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xseed")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "address": "0xseed",
                "contract_standard": "erc721",
                "collection": {"name": "OpenSea Contract"}
            }));
        })
        .await;

    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: format!("{}/nft/v3/key", alchemy_server.base_url()),
        alchemy_rpc_base: alchemy_server.base_url(),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };
    let meta = fetch_contract_metadata_with_opensea_fallback(
        &client, &endpoints, "ethereum", "0xseed", "opensea",
    )
    .await
    .unwrap();

    assert_eq!(meta.contract_deployer, "0xalchemycreator");
    assert_eq!(meta.deployed_block_number, 456);
    assert_eq!(meta.name, "Alchemy Contract");
}

#[tokio::test]
async fn contract_info_falls_back_to_opensea_when_alchemy_fails() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/nft/v3/key/getContractMetadata")
                .query_param("contractAddress", "0xseed");
            then.status(500).body("alchemy unavailable");
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xseed")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "address": "0xSeed",
                "contract_standard": "erc721",
                "collection": {"name": "OpenSea Contract"},
                "symbol": "OS"
            }));
        })
        .await;

    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: alchemy_server.base_url(),
        alchemy_rpc_base: alchemy_server.base_url(),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };
    let meta = fetch_contract_metadata_with_opensea_fallback(
        &client, &endpoints, "ethereum", "0xseed", "opensea",
    )
    .await
    .unwrap();

    assert_eq!(meta.contract_address, "0xseed");
    assert_eq!(meta.token_type, "ERC721");
    assert_eq!(meta.contract_deployer, "");
    assert_eq!(meta.deployed_block_number, 0);
    assert_eq!(meta.name, "OpenSea Contract");
    assert_eq!(meta.symbol, "OS");
}

#[tokio::test]
async fn fetch_license_sample_reads_first_seed_token_metadata() {
    let (base_url, server) = spawn_sequential_json_server(vec![(
        "/nft/v3/key/getNFTMetadata?contractAddress=0xseed&tokenId=1&refreshCache=false"
            .to_string(),
        serde_json::json!({
            "tokenUri": "ipfs://seed/1",
            "raw": {
                "metadata": {
                    "license": "Creative Commons Zero"
                }
            }
        }),
    )])
    .await;

    let client = test_client();
    let endpoints = test_endpoints(&base_url);
    let payload = fetch_license_sample(
        &client,
        &endpoints,
        &[SeedNft {
            contract_address: "0xseed".into(),
            token_id: "1".into(),
            ..SeedNft::default()
        }],
    )
    .await
    .unwrap();
    server.await.unwrap();

    assert!(is_open_license_payload(&payload));
}

#[tokio::test]
async fn alchemy_contract_transfers_expand_erc1155_metadata_token_ids() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST).path("/v2/key");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": {
                    "transfers": [{
                        "blockNum": "0x10",
                        "hash": "0xtransfer",
                        "from": "0xfrom",
                        "to": "0xto",
                        "category": "erc1155",
                        "rawContract": {"address": "0xdup"},
                        "logIndex": "0x2",
                        "metadata": {"blockTimestamp": "2024-01-01T00:00:00Z"},
                        "erc721TokenId": null,
                        "tokenId": null,
                        "erc1155Metadata": [
                            {"tokenId": "0x2a", "value": "1"},
                            {"tokenId": "43", "value": "2"}
                        ]
                    }],
                    "pageKey": ""
                }
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let rows = fetch_contract_transfers(&client, &endpoints, "", "ethereum", "0xdup", "ERC1155")
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].token_id, "42");
    assert_eq!(rows[1].token_id, "43");
    assert_eq!(rows[0].log_index, 2);
    assert_eq!(rows[0].block_number, 16);
}

#[tokio::test]
async fn alchemy_contract_owners_parse_numeric_and_hex_balances() {
    let (base_url, server) = spawn_sequential_json_server(vec![(
        "/nft/v3/key/getOwnersForContract?contractAddress=0xdup&withTokenBalances=true".to_string(),
        serde_json::json!({
            "owners": [{
                "ownerAddress": "0xOwner",
                "tokenBalances": [
                    {"tokenId": "0x2a", "balance": 3},
                    {"tokenId": "43", "balance": "0x4"}
                ]
            }],
            "pageKey": ""
        }),
    )])
    .await;

    let client = test_client();
    let endpoints = test_endpoints(&base_url);
    let rows = fetch_contract_owners(&client, &endpoints, "0xdup")
        .await
        .unwrap();
    server.await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].owner_address, "0xowner");
    assert_eq!(rows[0].token_balances.get("42"), Some(&3));
    assert_eq!(rows[0].token_balances.get("43"), Some(&4));
}

#[tokio::test]
async fn contract_transfers_fall_back_to_etherscan_for_erc721() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(POST).path("/v2/key");
            then.status(500).body("rate limited");
        })
        .await;
    let etherscan_server = MockServer::start_async().await;
    etherscan_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/")
                .query_param("chainid", "1")
                .query_param("module", "account")
                .query_param("action", "tokennfttx")
                .query_param("contractaddress", "0xdup");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": [{
                    "contractAddress": "0xdup",
                    "tokenID": "1",
                    "hash": "0xabc",
                    "transactionIndex": "0",
                    "blockNumber": "1",
                    "timeStamp": "10",
                    "from": "0x0000000000000000000000000000000000000000",
                    "to": "0xbuyer"
                }]
            }));
        })
        .await;

    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: format!("{}/nft/v3/key", alchemy_server.base_url()),
        alchemy_rpc_base: format!("{}/v2/key", alchemy_server.base_url()),
        etherscan_base: etherscan_server.base_url(),
        opensea_base: alchemy_server.base_url(),
    };
    let rows = fetch_contract_transfers(
        &client,
        &endpoints,
        "etherscan",
        "ethereum",
        "0xdup",
        "ERC721",
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source, "etherscan");
    assert_eq!(rows[0].token_id, "1");
}

#[tokio::test]
async fn contract_sales_use_alchemy_before_opensea_when_available() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(200).json_body_obj(&serde_json::json!({
                "nftSales": [{
                    "marketplace": "seaport",
                    "contractAddress": "0xdup",
                    "tokenId": "1",
                    "buyerAddress": "0xbuyer",
                    "sellerAddress": "0xseller",
                    "sellerFee": {"amount": "1250000000000000000", "symbol": "ETH", "decimals": 18},
                    "protocolFee": {"amount": "0", "symbol": "ETH", "decimals": 18},
                    "royaltyFee": {"amount": "0", "symbol": "ETH", "decimals": 18},
                    "transactionHash": "0xalchemy",
                    "blockNumber": 11,
                    "logIndex": 1,
                    "bundleIndex": 0
                }]
            }));
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xdup")
                .header("x-api-key", "opensea");
            then.status(500).body("opensea should not be called first");
        })
        .await;

    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: format!("{}/nft/v3/key", alchemy_server.base_url()),
        alchemy_rpc_base: format!("{}/v2/key", alchemy_server.base_url()),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };
    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "opensea", None)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source, "alchemy");
    assert!(rows[0].is_native_eth);
}

#[tokio::test]
async fn contract_sales_enrich_alchemy_sales_with_opensea_creator_fee_recipient() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(200).json_body_obj(&serde_json::json!({
                "nftSales": [{
                    "marketplace": "seaport",
                    "contractAddress": "0xdup",
                    "tokenId": "1",
                    "buyerAddress": "0xbuyer",
                    "sellerAddress": "0xseller",
                    "sellerFee": {"amount": "1000000000000000000", "symbol": "ETH", "decimals": 18},
                    "protocolFee": {"amount": "0", "symbol": "ETH", "decimals": 18},
                    "royaltyFee": {"amount": "50000000000000000", "symbol": "ETH", "decimals": 18},
                    "transactionHash": "0xalchemy",
                    "blockNumber": 11,
                    "logIndex": 1,
                    "bundleIndex": 0
                }]
            }));
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xdup")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "address": "0xdup",
                "collection": {"slug": "dup-collection"}
            }));
        })
        .await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/collections/dup-collection")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "fees": [
                    {"fee": 1.0, "recipient": "0x0000a26b00c1f0df003000390027140000faa719", "required": true},
                    {"fee": 5.0, "recipient": "0xCreatorRoyalty", "required": false}
                ]
            }));
        })
        .await;

    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: format!("{}/nft/v3/key", alchemy_server.base_url()),
        alchemy_rpc_base: format!("{}/v2/key", alchemy_server.base_url()),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };
    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "opensea", None)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].royalty_recipient_address, "0xcreatorroyalty");
}

#[tokio::test]
async fn contract_sales_paginates_opensea_events_with_next_cursor() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(500).body("alchemy unavailable");
        })
        .await;
    let (opensea_base_url, server) = spawn_sequential_json_server(vec![
        (
            "/api/v2/chain/ethereum/contract/0xdup".to_string(),
            serde_json::json!({
                "collection": {"slug": "dup-collection"}
            }),
        ),
        (
            "/api/v2/events/collection/dup-collection?event_type=sale&limit=200".to_string(),
            serde_json::json!({
                "events": [{
                    "event_type": "sale",
                    "asset_contract_address": "0xdup",
                    "nft": {"identifier": "1"},
                    "payment": {"symbol": "USDC", "decimals": 6},
                    "payment_quantity": "5000000",
                    "transaction_hash": "0xpage1",
                    "block_number": 11,
                    "event_index": 1,
                    "to_account": {"address": "0xbuyer1"},
                    "from_account": {"address": "0xseller1"}
                }],
                "next": "cursor-2"
            }),
        ),
        (
            "/api/v2/events/collection/dup-collection?event_type=sale&limit=200&next=cursor-2"
                .to_string(),
            serde_json::json!({
                "events": [{
                    "event_type": "sale",
                    "asset_contract_address": "0xdup",
                    "nft": {"identifier": "2"},
                    "payment": {"symbol": "USDC", "decimals": 6},
                    "payment_quantity": "7000000",
                    "transaction_hash": "0xpage2",
                    "block_number": 12,
                    "event_index": 2,
                    "to_account": {"address": "0xbuyer2"},
                    "from_account": {"address": "0xseller2"}
                }],
                "next": ""
            }),
        ),
    ])
    .await;
    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: format!("{}/nft/v3/key", alchemy_server.base_url()),
        alchemy_rpc_base: format!("{}/v2/key", alchemy_server.base_url()),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_base_url,
    };

    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "opensea", None)
        .await
        .unwrap();
    if rows.len() == 2 {
        tokio::time::timeout(std::time::Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    } else {
        server.abort();
    }

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].tx_hash, "0xpage1");
    assert_eq!(rows[1].tx_hash, "0xpage2");
    assert_eq!(rows[0].price_usd, Some(5.0));
    assert_eq!(rows[1].price_usd, Some(7.0));
}

#[tokio::test]
async fn opensea_market_events_parse_order_cancel_and_transfer_events() {
    let (opensea_base_url, server) = spawn_sequential_json_server(vec![(
        "/api/v2/events/collection/dup-collection?event_type=all&limit=200".to_string(),
        serde_json::json!({
            "events": [
                {
                    "event_type": "order",
                    "order_type": "listing",
                    "order_hash": "0xorder",
                    "event_timestamp": 1710000000,
                    "nft": {"identifier": "1", "contract": {"address": "0xDup"}},
                    "maker": {"address": "0xseller"},
                    "payment": {
                        "symbol": "ETH",
                        "decimals": 18,
                        "quantity": "1000000000000000000",
                        "token": {"address": "0x0000000000000000000000000000000000000000"}
                    }
                },
                {
                    "event_type": "cancel",
                    "order_type": "listing",
                    "order_hash": "0xorder",
                    "event_timestamp": 1710000100,
                    "nft": {"identifier": "1", "contract": "0xDup"},
                    "maker": "0xseller"
                },
                {
                    "event_type": "transfer",
                    "event_timestamp": 1710000200,
                    "transaction": {"hash": "0xmove"},
                    "block_number": "0x20",
                    "nft": {"identifier": "2", "contract": "0xDup"},
                    "from_account": {"address": "0xfrom"},
                    "to_account": {"address": "0xto"}
                },
                {
                    "event_type": "order",
                    "order_type": "listing",
                    "event_timestamp": 1710000300,
                    "nft": {"identifier": "99", "contract": "0xOther"},
                    "maker": {"address": "0xother"}
                }
            ],
            "next": ""
        }),
    )])
    .await;

    let client = test_client();
    let rows = fetch_opensea_contract_market_events(
        &client,
        &opensea_base_url,
        "ethereum",
        "0xdup",
        Some("dup-collection"),
        "opensea",
        Some(3000.0),
    )
    .await
    .unwrap();
    server.await.unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].event_type, "order");
    assert_eq!(rows[0].order_type, "listing");
    assert_eq!(rows[0].actor_address, "0xseller");
    assert_eq!(rows[0].price_eth, Some(1.0));
    assert_eq!(rows[0].price_usd, Some(3000.0));
    assert_eq!(rows[1].event_type, "cancel");
    assert_eq!(rows[1].order_hash, "0xorder");
    assert_eq!(rows[2].event_type, "transfer");
    assert_eq!(rows[2].tx_hash, "0xmove");
    assert_eq!(rows[2].block_number, 32);
    assert_eq!(rows[2].from_address, "0xfrom");
    assert_eq!(rows[2].to_address, "0xto");
}

#[tokio::test]
async fn contract_sales_enrich_royalty_recipient_from_eip2981() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(200).json_body_obj(&serde_json::json!({
                "nftSales": [{
                    "marketplace": "seaport",
                    "contractAddress": "0xdup",
                    "tokenId": "1",
                    "buyerAddress": "0xbuyer",
                    "sellerAddress": "0xseller",
                    "sellerFee": {"amount": "1000000000000000000", "symbol": "ETH", "decimals": 18},
                    "royaltyFee": {"amount": "50000000000000000", "symbol": "ETH", "decimals": 18},
                    "blockNumber": 10,
                    "logIndex": 1,
                    "bundleIndex": 0,
                    "transactionHash": "0xsale"
                }]
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/v2/key")
                .body_contains("eth_call")
                .body_contains("2a55205a");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": "0x000000000000000000000000444444444444444444444444444444444444444400000000000000000000000000000000000000000000000000b1a2bc2ec50000"
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "", None)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].royalty_recipient_address,
        "0x4444444444444444444444444444444444444444"
    );
}

#[tokio::test]
async fn contract_sales_parse_opensea_fallback_sale_event_shape() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(500).body("alchemy unavailable");
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xdup")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "collection": {"slug": "dup-collection"}
            }));
        })
        .await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/events/collection/dup-collection")
                .query_param("event_type", "sale")
                .query_param("limit", "200")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "asset_events": [{
                    "event_type": "sale",
                    "nft": {
                        "identifier": "42",
                        "contract": {"address": "0xDup"}
                    },
                    "payment": {
                        "symbol": "USDC",
                        "decimals": "6",
                        "quantity": "12340000",
                        "token": {"address": "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}
                    },
                    "transaction": {"hash": "0xopensea"},
                    "block_number": "0x10",
                    "event_index": "0x2",
                    "bundle_index": "0x0",
                    "to_address": "0xbuyer",
                    "from_address": "0xseller",
                    "taker": {"account_address": "0xtaker"},
                    "maker": {"wallet_address": "0xmaker"}
                }],
                "next": ""
            }));
        })
        .await;
    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: format!("{}/nft/v3/key", alchemy_server.base_url()),
        alchemy_rpc_base: format!("{}/v2/key", alchemy_server.base_url()),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };

    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "opensea", None)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].contract_address, "0xdup");
    assert_eq!(rows[0].token_id, "42");
    assert_eq!(rows[0].tx_hash, "0xopensea");
    assert_eq!(rows[0].block_number, 16);
    assert_eq!(rows[0].log_index, 2);
    assert_eq!(rows[0].buyer_address, "0xbuyer");
    assert_eq!(rows[0].seller_address, "0xseller");
    assert_eq!(rows[0].taker, "0xtaker");
    assert_eq!(rows[0].payment_token_symbol, "USDC");
    assert_eq!(
        rows[0].payment_token_address,
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    );
    assert_eq!(rows[0].price_usd, Some(12.34));
}

#[tokio::test]
async fn contract_sales_do_not_treat_opensea_maker_taker_as_buyer_seller() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(500).body("alchemy unavailable");
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xdup")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "collection": {"slug": "dup-collection"}
            }));
        })
        .await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/events/collection/dup-collection")
                .query_param("event_type", "sale")
                .query_param("limit", "200")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "asset_events": [{
                    "event_type": "sale",
                    "nft": {"identifier": "7", "contract": "0xdup"},
                    "payment": {
                        "symbol": "ETH",
                        "decimals": 18,
                        "quantity": "1000000000000000000"
                    },
                    "transaction": "0xopensea",
                    "maker": "0xmaker",
                    "taker": "0xtaker"
                }],
                "next": ""
            }));
        })
        .await;
    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: format!("{}/nft/v3/key", alchemy_server.base_url()),
        alchemy_rpc_base: format!("{}/v2/key", alchemy_server.base_url()),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };

    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "opensea", None)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].buyer_address, "");
    assert_eq!(rows[0].seller_address, "");
    assert_eq!(rows[0].taker, "0xtaker");
}

#[tokio::test]
async fn contract_sales_opensea_fallback_uses_requested_chain() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(500).body("alchemy unavailable");
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/matic/contract/0xdup")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "collection": {"slug": "dup-collection"}
            }));
        })
        .await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/events/collection/dup-collection")
                .query_param("event_type", "sale")
                .query_param("limit", "200")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "events": []
            }));
        })
        .await;
    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: alchemy_server.base_url(),
        alchemy_rpc_base: alchemy_server.base_url(),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };

    let rows = fetch_contract_sales(&client, &endpoints, "polygon", "0xdup", "opensea", None)
        .await
        .unwrap();

    assert!(rows.is_empty());
}

#[tokio::test]
async fn contract_sales_fall_back_to_alchemy_when_opensea_fails() {
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xdup")
                .header("x-api-key", "opensea");
            then.status(500).body("opensea unavailable");
        })
        .await;
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(200).json_body_obj(&serde_json::json!({
                "nftSales": [{
                    "marketplace": "seaport",
                    "contractAddress": "0xdup",
                    "tokenId": "1",
                    "buyerAddress": "0xbuyer",
                    "sellerAddress": "0xseller",
                    "sellerFee": {"amount": "1000000000000000000", "symbol": "ETH", "decimals": 18},
                    "blockNumber": 10,
                    "logIndex": 1,
                    "bundleIndex": 0,
                    "transactionHash": "0xalchemy"
                }]
            }));
        })
        .await;
    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: format!("{}/nft/v3/key", alchemy_server.base_url()),
        alchemy_rpc_base: alchemy_server.base_url(),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };

    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "opensea", None)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source, "alchemy");
    assert_eq!(rows[0].price_eth, Some(1.0));
}

#[tokio::test]
async fn contract_sales_return_empty_when_alchemy_and_opensea_fail() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(500).body("alchemy unavailable");
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xdup")
                .header("x-api-key", "opensea");
            then.status(500).body("opensea unavailable");
        })
        .await;
    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: alchemy_server.base_url(),
        alchemy_rpc_base: alchemy_server.base_url(),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };

    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "opensea", None)
        .await
        .unwrap();

    assert!(rows.is_empty());
}

#[tokio::test]
async fn contract_sales_parse_numeric_fee_amounts_and_string_indexes() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(200).json_body_obj(&serde_json::json!({
                "nftSales": [{
                    "marketplace": "seaport",
                    "contractAddress": "0xdup",
                    "tokenId": "0x2a",
                    "buyerAddress": "0xbuyer",
                    "sellerAddress": "0xseller",
                    "taker": "BUYER",
                    "sellerFee": {
                        "amount": 1250000000000000000u64,
                        "symbol": "ETH",
                        "decimals": 18
                    },
                    "protocolFee": {
                        "amount": "250000000000000000",
                        "symbol": "ETH",
                        "decimals": "18"
                    },
                    "royaltyFee": {
                        "amount": "0",
                        "symbol": "ETH",
                        "decimals": 18,
                        "recipient": "0xRoyalty"
                    },
                    "blockNumber": "0x10",
                    "logIndex": "0x2",
                    "bundleIndex": "0",
                    "transactionHash": "0xsale"
                }]
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "", Some(3000.0))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].token_id, "42");
    assert_eq!(rows[0].buyer_address, "0xbuyer");
    assert_eq!(rows[0].seller_address, "0xseller");
    assert_eq!(rows[0].block_number, 16);
    assert_eq!(rows[0].log_index, 2);
    assert_eq!(rows[0].price_eth, Some(1.5));
    assert_eq!(rows[0].price_usd, Some(4500.0));
    assert_eq!(rows[0].royalty_recipient_address, "0xroyalty");
    assert!(rows[0].is_native_eth);
}

#[tokio::test]
async fn contract_sales_convert_alchemy_stablecoin_amounts_to_usd_primary_amount() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(200).json_body_obj(&serde_json::json!({
                "nftSales": [{
                    "marketplace": "seaport",
                    "contractAddress": "0xdup",
                    "tokenId": "1",
                    "buyerAddress": "0xbuyer",
                    "sellerAddress": "0xseller",
                    "sellerFee": {
                        "amount": "150000000",
                        "symbol": "USDC",
                        "decimals": 6
                    },
                    "blockNumber": 10,
                    "logIndex": 1,
                    "bundleIndex": 0,
                    "transactionHash": "0xusdc"
                }]
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let rows = fetch_contract_sales(&client, &endpoints, "ethereum", "0xdup", "", Some(3000.0))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payment_token_symbol, "USDC");
    assert_eq!(rows[0].price_eth, Some(0.05));
    assert_eq!(rows[0].price_usd, Some(150.0));
    assert!(!rows[0].is_native_eth);
}

#[tokio::test]
async fn contract_sales_convert_opensea_stablecoin_amounts_to_usd_primary_amount() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v3/key/getNFTSales");
            then.status(500).body("alchemy unavailable");
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/chain/ethereum/contract/0xdup")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "collection": {"slug": "dup-collection"}
            }));
        })
        .await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/events/collection/dup-collection")
                .query_param("event_type", "sale")
                .query_param("limit", "200")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "events": [{
                    "event_type": "sale",
                    "asset_contract_address": "0xdup",
                    "nft": {"identifier": "1", "contract": "0xdup"},
                    "payment": {"symbol": "USDT", "decimals": 6, "quantity": "300000000"},
                    "transaction_hash": "0xusdt",
                    "block_number": 10,
                    "event_index": 1,
                    "bundle_index": 0,
                    "to_account": {"address": "0xbuyer"},
                    "from_account": {"address": "0xseller"}
                }]
            }));
        })
        .await;
    let client = test_client();
    let endpoints = ApiEndpoints {
        alchemy_nft_v2_base: format!("{}/nft/v2/key", alchemy_server.base_url()),
        alchemy_nft_v3_base: alchemy_server.base_url(),
        alchemy_rpc_base: alchemy_server.base_url(),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };

    let rows = fetch_contract_sales(
        &client,
        &endpoints,
        "ethereum",
        "0xdup",
        "opensea",
        Some(3000.0),
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payment_token_symbol, "USDT");
    assert_eq!(rows[0].price_eth, Some(0.1));
    assert_eq!(rows[0].price_usd, Some(300.0));
    assert!(!rows[0].is_native_eth);
}

#[tokio::test]
async fn fetch_transaction_receipt_parses_hex_receipt_fields() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/v2/key")
                .body_contains("eth_getTransactionReceipt")
                .body_contains("0xsale");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": {
                    "transactionHash": "0xsale",
                    "blockNumber": "0x10",
                    "transactionIndex": "0x2",
                    "from": "0xbuyer",
                    "gasUsed": "0x5208",
                    "effectiveGasPrice": "0x3b9aca00"
                }
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let receipt = fetch_transaction_receipt(&client, &endpoints, "0xsale")
        .await
        .unwrap();

    assert_eq!(receipt.tx_hash, "0xsale");
    assert_eq!(receipt.block_number, 16);
    assert_eq!(receipt.transaction_index, 2);
    assert_eq!(receipt.from_address, "0xbuyer");
    assert_eq!(receipt.gas_used, 21000);
}

#[tokio::test]
async fn fetch_transaction_receipts_for_block_parses_receipt_map() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/v2/key")
                .body_contains("alchemy_getTransactionReceipts")
                .body_contains("0x2");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": {
                    "receipts": [{
                        "transactionHash": "0xprefund",
                        "transactionIndex": "0x1",
                        "from": "0xother",
                        "gasUsed": "0x0",
                        "effectiveGasPrice": "0x0"
                    }]
                }
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let rows = fetch_transaction_receipts_for_block(&client, &endpoints, 2)
        .await
        .unwrap();

    assert_eq!(rows["0xprefund"].transaction_index, 1);
    assert_eq!(rows["0xprefund"].from_address, "0xother");
}

#[tokio::test]
async fn fetch_eth_balance_and_same_block_transfers_parse_rpc_payloads() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/v2/key")
                .body_contains("eth_getBalance")
                .body_contains("0x1");
            then.status(200)
                .json_body_obj(&serde_json::json!({ "result": "0x29a2241af62c0000" }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/v2/key")
                .body_contains("alchemy_getAssetTransfers")
                .body_contains("\"fromAddress\":\"0xbuyer\"");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": {
                    "transfers": [{
                        "hash": "0xabc",
                        "from": "0xbuyer",
                        "to": "0xother",
                        "value": "0xde0b6b3a7640000",
                        "category": "external"
                    }]
                }
            }));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/v2/key")
                .body_contains("alchemy_getAssetTransfers")
                .body_contains("\"toAddress\":\"0xbuyer\"");
            then.status(200).json_body_obj(&serde_json::json!({
                "result": {
                    "transfers": [{
                        "hash": "0xdef",
                        "from": "0xother",
                        "to": "0xbuyer",
                        "value": 2.5,
                        "category": "internal"
                    }]
                }
            }));
        })
        .await;

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let balance = fetch_eth_balance(&client, &endpoints, "0xbuyer", 1)
        .await
        .unwrap();
    let transfers = fetch_same_block_eth_transfers_for_address(&client, &endpoints, 2, "0xbuyer")
        .await
        .unwrap();

    assert_eq!(balance, 3.0);
    assert_eq!(transfers.len(), 2);
    assert_eq!(transfers[0].block_number, 2);
    assert!(transfers.iter().any(|row| row.value_eth == 1.0));
    assert!(transfers.iter().any(|row| row.value_eth == 2.5));
}
