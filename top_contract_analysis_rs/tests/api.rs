use httpmock::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use top_contract_analysis_rs::api::{
    fetch_contract_metadata, fetch_contract_sales, fetch_contract_transfers, fetch_eth_balance,
    fetch_license_sample, fetch_same_block_eth_transfers_for_address, fetch_seed_contract_nfts,
    fetch_transaction_receipt, fetch_transaction_receipts_for_block, is_open_license_payload,
    ApiEndpoints, AsyncApiClient,
};
use top_contract_analysis_rs::models::SeedNft;

fn test_client() -> AsyncApiClient {
    AsyncApiClient::new(5, 4, 2, 2).unwrap()
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
            "/nft/v3/key/getNFTsForContract?contractAddress=0xseed&withMetadata=true&pageKey=next-page".to_string(),
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
            "/nft/v3/key/getNFTsForContract?contractAddress=0xseed&withMetadata=true&pageKey=looping-page".to_string(),
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
                .path("/nft/v2/key/getContractMetadata")
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

    let client = test_client();
    let endpoints = test_endpoints(&server.base_url());
    let meta = fetch_contract_metadata(&client, &endpoints, "ethereum", "0xseed")
        .await
        .unwrap();

    assert_eq!(meta.contract_address, "0xseed");
    assert_eq!(meta.token_type, "ERC721");
    assert_eq!(meta.contract_deployer, "0xcreator");
    assert_eq!(meta.deployed_block_number, 123);
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
async fn contract_sales_fall_back_to_opensea() {
    let alchemy_server = MockServer::start_async().await;
    alchemy_server
        .mock_async(|when, then| {
            when.method(GET).path("/nft/v2/key/getNFTSales");
            then.status(200)
                .json_body_obj(&serde_json::json!({"nftSales": []}));
        })
        .await;
    let opensea_server = MockServer::start_async().await;
    opensea_server
        .mock_async(|when, then| {
            when.method(GET)
                .path("/api/v2/events")
                .query_param("event_type", "sale")
                .query_param("asset_contract_address", "0xdup")
                .query_param("chain", "ethereum")
                .header("x-api-key", "opensea");
            then.status(200).json_body_obj(&serde_json::json!({
                "events": [{
                    "event_type": "sale",
                    "asset_contract_address": "0xdup",
                    "nft": {"identifier": "1"},
                    "payment": {"symbol": "ETH"},
                    "payment_quantity": "1250000000000000000",
                    "transaction_hash": "0xopensea",
                    "block_number": 11,
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
        alchemy_nft_v3_base: format!("{}/nft/v3/key", alchemy_server.base_url()),
        alchemy_rpc_base: format!("{}/v2/key", alchemy_server.base_url()),
        etherscan_base: alchemy_server.base_url(),
        opensea_base: opensea_server.base_url(),
    };
    let rows = fetch_contract_sales(&client, &endpoints, "0xdup", "opensea")
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source, "opensea");
    assert!(rows[0].is_native_eth);
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
