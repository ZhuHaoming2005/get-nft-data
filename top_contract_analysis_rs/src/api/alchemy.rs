use reqwest::Url;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

use crate::analysis::scoring::metadata_document_from_json;
use crate::api::{ApiEndpoints, AsyncApiClient};
use crate::currency::{is_supported_priced_symbol, to_normalized_amount};
use crate::error::AppError;
use crate::models::{
    ContractMetadata, EthTransferRecord, OwnerBalance, SeedNft, TransactionReceiptRecord,
    TransferRecord, ZERO_ADDRESS,
};
use crate::normalize::build_nft_metadata_json;

use super::etherscan::fetch_etherscan_contract_transfers;

fn normalize_token_id(raw: Option<&Value>) -> String {
    let Some(raw) = raw else {
        return String::new();
    };
    let text = raw
        .as_str()
        .map(ToString::to_string)
        .unwrap_or_else(|| raw.to_string());
    let trimmed = text.trim();
    if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
        i128::from_str_radix(
            trimmed.trim_start_matches("0x").trim_start_matches("0X"),
            16,
        )
        .map(|value| value.to_string())
        .unwrap_or_else(|_| trimmed.to_string())
    } else {
        trimmed.to_string()
    }
}

fn transfer_token_ids(item: &Value) -> Vec<String> {
    let mut token_ids = Vec::new();
    if let Some(metadata) = item.get("erc1155Metadata").and_then(Value::as_array) {
        for token in metadata {
            let token_id = normalize_token_id(token.get("tokenId"));
            if !token_id.is_empty() {
                token_ids.push(token_id);
            }
        }
    }
    if token_ids.is_empty() {
        token_ids.push(normalize_token_id(
            item.get("erc721TokenId").or_else(|| item.get("tokenId")),
        ));
    }
    token_ids
}

fn parse_block_timestamp(value: Option<&Value>) -> i64 {
    let Some(value) = value else {
        return 0;
    };
    if let Some(number) = value.as_i64() {
        return number;
    }
    let text = value.as_str().unwrap_or("").trim();
    if text.is_empty() {
        return 0;
    }
    if text.starts_with("0x") || text.starts_with("0X") {
        return i64::from_str_radix(text.trim_start_matches("0x").trim_start_matches("0X"), 16)
            .unwrap_or(0);
    }
    if let Ok(number) = text.parse::<i64>() {
        return number;
    }
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|dt| dt.timestamp())
        .unwrap_or(0)
}

async fn fetch_block_timestamp(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    block_num: &str,
) -> Result<i64, AppError> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [block_num, false]
    });
    let body: Value = client
        .post_json(&endpoints.alchemy_rpc_base, &payload)
        .await?;
    Ok(parse_block_timestamp(
        body.get("result").and_then(|value| value.get("timestamp")),
    ))
}

fn advance_page_key(
    seen_page_keys: &mut BTreeSet<String>,
    next_page_key: &str,
) -> Result<Option<String>, AppError> {
    if next_page_key.is_empty() {
        return Ok(None);
    }
    if !seen_page_keys.insert(next_page_key.to_string()) {
        return Err(AppError::Http(format!(
            "pagination stalled on repeated pageKey: {next_page_key}"
        )));
    }
    Ok(Some(next_page_key.to_string()))
}

pub async fn fetch_seed_contract_nfts(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
) -> Result<Vec<SeedNft>, AppError> {
    let mut rows = Vec::new();
    let mut page_key: Option<String> = None;
    let mut seen_page_keys = BTreeSet::new();
    loop {
        let mut url = Url::parse(&format!(
            "{}/getNFTsForContract",
            endpoints.alchemy_nft_v3_base
        ))
        .map_err(|err| AppError::Http(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("contractAddress", contract_address)
            .append_pair("withMetadata", "true");
        if let Some(page_key) = page_key.as_deref() {
            url.query_pairs_mut().append_pair("startToken", page_key);
        }
        let payload: Value = client.get_json(url.as_str()).await?;
        for raw in payload
            .get("nfts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let token_uri = raw
                .get("tokenUri")
                .and_then(|value| {
                    value
                        .get("raw")
                        .or_else(|| value.get("gateway"))
                        .or(Some(value))
                })
                .and_then(Value::as_str)
                .or_else(|| {
                    raw.get("raw")
                        .and_then(|value| value.get("tokenUri"))
                        .and_then(Value::as_str)
                })
                .unwrap_or("")
                .to_string();
            let image_uri = raw
                .get("image")
                .and_then(|value| {
                    value
                        .get("originalUrl")
                        .or_else(|| value.get("cachedUrl"))
                        .or_else(|| value.get("pngUrl"))
                        .or(Some(value))
                })
                .and_then(Value::as_str)
                .or_else(|| raw.get("image_url").and_then(Value::as_str))
                .unwrap_or("")
                .to_string();
            let metadata_json = build_nft_metadata_json(raw, &token_uri, &image_uri)?;
            rows.push(SeedNft {
                chain: chain.to_string(),
                contract_address: raw
                    .get("contract")
                    .and_then(|value| value.get("address"))
                    .and_then(Value::as_str)
                    .unwrap_or(contract_address)
                    .to_lowercase(),
                token_id: normalize_token_id(
                    raw.get("tokenId")
                        .or_else(|| raw.get("id").and_then(|value| value.get("tokenId"))),
                ),
                name: raw
                    .get("title")
                    .or_else(|| raw.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                symbol: raw
                    .get("contractMetadata")
                    .and_then(|value| value.get("symbol"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                token_uri,
                image_uri,
                metadata_doc: metadata_document_from_json(&metadata_json),
                metadata_json,
            });
        }
        page_key = advance_page_key(
            &mut seen_page_keys,
            payload.get("pageKey").and_then(Value::as_str).unwrap_or(""),
        )?;
        if page_key.is_none() {
            return Ok(rows);
        }
    }
}

pub async fn fetch_license_sample(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    seed_nfts: &[SeedNft],
) -> Result<Value, AppError> {
    let Some(sample) = seed_nfts.iter().find(|nft| !nft.token_id.trim().is_empty()) else {
        return Ok(Value::Object(serde_json::Map::new()));
    };

    let mut url = Url::parse(&format!("{}/getNFTMetadata", endpoints.alchemy_nft_v3_base))
        .map_err(|err| AppError::Http(err.to_string()))?;
    url.query_pairs_mut()
        .append_pair("contractAddress", &sample.contract_address)
        .append_pair("tokenId", &sample.token_id)
        .append_pair("refreshCache", "false");
    client.get_json(url.as_str()).await
}

pub fn is_open_license_payload(payload: &Value) -> bool {
    fn collect_strings(value: &Value, texts: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                for item in map.values() {
                    collect_strings(item, texts);
                }
            }
            Value::Array(items) => {
                for item in items {
                    collect_strings(item, texts);
                }
            }
            Value::String(text) => texts.push(text.clone()),
            _ => {}
        }
    }

    let mut texts = Vec::new();
    collect_strings(payload, &mut texts);
    let haystack = texts.join(" ").to_lowercase();
    [
        "cc0-1.0",
        "license: cc0",
        "creative commons zero",
        "public domain",
        "cc zero",
    ]
    .iter()
    .any(|needle| haystack.contains(needle))
}

pub async fn fetch_contract_metadata(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
) -> Result<ContractMetadata, AppError> {
    let mut url = Url::parse(&format!(
        "{}/getContractMetadata",
        endpoints.alchemy_nft_v3_base
    ))
    .map_err(|err| AppError::Http(err.to_string()))?;
    url.query_pairs_mut()
        .append_pair("contractAddress", contract_address);
    let payload: Value = client.get_json(url.as_str()).await?;
    let meta = payload.get("contractMetadata").unwrap_or(&payload);
    Ok(ContractMetadata {
        chain: chain.to_string(),
        contract_address: payload
            .get("address")
            .and_then(Value::as_str)
            .unwrap_or(contract_address)
            .to_lowercase(),
        token_type: meta
            .get("tokenType")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        contract_deployer: meta
            .get("contractDeployer")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_lowercase(),
        deployed_block_number: meta
            .get("deployedBlockNumber")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        owner_address: contract_metadata_lower_field(
            &payload,
            meta,
            &["ownerAddress", "owner", "contractOwner"],
        ),
        admin_address: contract_metadata_lower_field(
            &payload,
            meta,
            &["adminAddress", "admin", "contractAdmin"],
        ),
        proxy_admin_address: contract_metadata_lower_field(
            &payload,
            meta,
            &["proxyAdminAddress", "proxyAdmin", "proxy_admin"],
        ),
        name: meta
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        symbol: meta
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    })
}

fn contract_metadata_lower_field(payload: &Value, meta: &Value, fields: &[&str]) -> String {
    for source in [meta, payload] {
        for field in fields {
            let value = source
                .get(*field)
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            if !value.is_empty() {
                return value.to_lowercase();
            }
        }
    }
    String::new()
}

pub async fn fetch_alchemy_contract_transfers(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    contract_address: &str,
) -> Result<Vec<TransferRecord>, AppError> {
    let mut transfers = Vec::new();
    let mut page_key: Option<String> = None;
    let mut seen_page_keys = BTreeSet::new();
    let mut block_time_cache = std::collections::BTreeMap::<String, i64>::new();
    loop {
        let mut params = json!({
            "fromBlock": "0x0",
            "toBlock": "latest",
            "category": ["erc721", "erc1155"],
            "contractAddresses": [contract_address],
            "withMetadata": true,
            "excludeZeroValue": false,
            "maxCount": "0x3e8",
            "order": "asc"
        });
        if let Some(page_key) = &page_key {
            params["pageKey"] = Value::String(page_key.clone());
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "alchemy_getAssetTransfers",
            "params": [params]
        });
        let body: Value = client
            .post_json(&endpoints.alchemy_rpc_base, &payload)
            .await?;
        if body.get("error").is_some() {
            return Err(AppError::Http(body.get("error").unwrap().to_string()));
        }
        let result = body.get("result").unwrap_or(&Value::Null);
        for item in result
            .get("transfers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let block_num = item.get("blockNum").and_then(Value::as_str).unwrap_or("0");
            let mut block_time = parse_block_timestamp(
                item.get("metadata")
                    .and_then(|value| value.get("blockTimestamp")),
            );
            if block_time == 0 && block_num != "0" && !block_num.is_empty() {
                let cached = if let Some(cached) = block_time_cache.get(block_num) {
                    *cached
                } else {
                    let fetched = fetch_block_timestamp(client, endpoints, block_num).await?;
                    block_time_cache.insert(block_num.to_string(), fetched);
                    fetched
                };
                block_time = cached;
            }
            let contract_address = item
                .get("rawContract")
                .and_then(|value| value.get("address"))
                .and_then(Value::as_str)
                .unwrap_or(contract_address)
                .to_lowercase();
            let tx_hash = item
                .get("hash")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let log_index = parse_hex_or_decimal_i64(item.get("logIndex"));
            let block_number = parse_hex_or_decimal_i64(item.get("blockNum"));
            let from_address = item
                .get("from")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase();
            let to_address = item
                .get("to")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase();
            let event_type = item
                .get("category")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            for token_id in transfer_token_ids(item) {
                transfers.push(TransferRecord {
                    contract_address: contract_address.clone(),
                    token_id,
                    tx_hash: tx_hash.clone(),
                    log_index,
                    block_number,
                    block_time,
                    from_address: from_address.clone(),
                    to_address: to_address.clone(),
                    event_type: event_type.clone(),
                    source: "alchemy".to_string(),
                });
            }
        }
        page_key = advance_page_key(
            &mut seen_page_keys,
            result.get("pageKey").and_then(Value::as_str).unwrap_or(""),
        )?;
        if page_key.is_none() {
            return Ok(transfers);
        }
    }
}

pub async fn fetch_contract_transfers(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    etherscan_api_key: &str,
    chain: &str,
    contract_address: &str,
    token_type: &str,
) -> Result<Vec<TransferRecord>, AppError> {
    match fetch_alchemy_contract_transfers(client, endpoints, contract_address).await {
        Ok(rows) => Ok(rows),
        Err(_) => {
            fetch_etherscan_contract_transfers(
                client,
                &endpoints.etherscan_base,
                etherscan_api_key,
                chain,
                contract_address,
                token_type,
            )
            .await
        }
    }
}

pub async fn fetch_contract_owners(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    contract_address: &str,
) -> Result<Vec<OwnerBalance>, AppError> {
    let mut owners = Vec::new();
    let mut page_key: Option<String> = None;
    let mut seen_page_keys = BTreeSet::new();
    loop {
        let mut url = Url::parse(&format!(
            "{}/getOwnersForContract",
            endpoints.alchemy_nft_v3_base
        ))
        .map_err(|err| AppError::Http(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("contractAddress", contract_address)
            .append_pair("withTokenBalances", "true");
        if let Some(page_key) = page_key.as_deref() {
            url.query_pairs_mut().append_pair("pageKey", page_key);
        }
        let payload: Value = client.get_json(url.as_str()).await?;
        for row in payload
            .get("owners")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let owner_address = row
                .get("ownerAddress")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase();
            if owner_address.is_empty() || owner_address == ZERO_ADDRESS {
                continue;
            }
            let mut token_balances = std::collections::BTreeMap::new();
            for balance in row
                .get("tokenBalances")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                token_balances.insert(
                    normalize_token_id(balance.get("tokenId")),
                    parse_hex_or_decimal_i64(balance.get("balance")),
                );
            }
            owners.push(OwnerBalance {
                owner_address,
                token_balances,
            });
        }
        page_key = advance_page_key(
            &mut seen_page_keys,
            payload.get("pageKey").and_then(Value::as_str).unwrap_or(""),
        )?;
        if page_key.is_none() {
            return Ok(owners);
        }
    }
}

pub async fn fetch_is_holder_of_contract(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    wallet_address: &str,
    contract_address: &str,
) -> Result<bool, AppError> {
    let mut url = Url::parse(&format!(
        "{}/isHolderOfContract",
        endpoints.alchemy_nft_v3_base
    ))
    .map_err(|err| AppError::Http(err.to_string()))?;
    url.query_pairs_mut()
        .append_pair("wallet", wallet_address)
        .append_pair("contractAddress", contract_address);
    let payload: Value = client.get_json(url.as_str()).await?;
    Ok(payload
        .get("isHolderOfContract")
        .and_then(Value::as_bool)
        .unwrap_or(false))
}

pub async fn fetch_transaction_receipt(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    tx_hash: &str,
) -> Result<TransactionReceiptRecord, AppError> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getTransactionReceipt",
        "params": [tx_hash]
    });
    let body: Value = client
        .post_json(&endpoints.alchemy_rpc_base, &payload)
        .await?;
    let result = body.get("result").unwrap_or(&Value::Null);
    Ok(TransactionReceiptRecord {
        tx_hash: result
            .get("transactionHash")
            .and_then(Value::as_str)
            .unwrap_or(tx_hash)
            .to_lowercase(),
        block_number: parse_hex_or_decimal_i64(result.get("blockNumber")),
        transaction_index: parse_hex_or_decimal_i64(result.get("transactionIndex")),
        from_address: result
            .get("from")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_lowercase(),
        gas_used: parse_hex_or_decimal_i64(result.get("gasUsed")),
        effective_gas_price_wei: parse_hex_or_decimal_i64(result.get("effectiveGasPrice")),
    })
}

pub async fn fetch_transaction_receipts_for_block(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    block_number: i64,
) -> Result<BTreeMap<String, TransactionReceiptRecord>, AppError> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getTransactionReceipts",
        "params": [{ "blockNumber": format!("0x{:x}", block_number.max(0)) }]
    });
    let body: Value = client
        .post_json(&endpoints.alchemy_rpc_base, &payload)
        .await?;
    let mut rows = BTreeMap::new();
    for item in body
        .get("result")
        .and_then(|value| value.get("receipts"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let tx_hash = item
            .get("transactionHash")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_lowercase();
        if tx_hash.is_empty() {
            continue;
        }
        rows.insert(
            tx_hash.clone(),
            TransactionReceiptRecord {
                tx_hash,
                block_number,
                transaction_index: parse_hex_or_decimal_i64(item.get("transactionIndex")),
                from_address: item
                    .get("from")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_lowercase(),
                gas_used: parse_hex_or_decimal_i64(item.get("gasUsed")),
                effective_gas_price_wei: parse_hex_or_decimal_i64(item.get("effectiveGasPrice")),
            },
        );
    }
    Ok(rows)
}

pub async fn fetch_eth_balance(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    address: &str,
    block_number: i64,
) -> Result<f64, AppError> {
    if block_number < 0 {
        return Ok(0.0);
    }
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBalance",
        "params": [address, format!("0x{:x}", block_number)]
    });
    let body: Value = client
        .post_json(&endpoints.alchemy_rpc_base, &payload)
        .await?;
    let wei = body
        .get("result")
        .and_then(Value::as_str)
        .and_then(|text| u128::from_str_radix(text.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0);
    Ok(wei as f64 / 1_000_000_000_000_000_000_f64)
}

async fn fetch_address_eth_transfers(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    block_number: i64,
    address: &str,
    direction: &str,
    eth_usd_rate: Option<f64>,
) -> Result<Vec<EthTransferRecord>, AppError> {
    let mut params = json!({
        "fromBlock": format!("0x{:x}", block_number.max(0)),
        "toBlock": format!("0x{:x}", block_number.max(0)),
        "category": ["external", "internal", "erc20"],
        "withMetadata": false,
        "excludeZeroValue": true,
        "maxCount": "0x3e8",
        "order": "asc",
    });
    if direction == "from" {
        params["fromAddress"] = Value::String(address.to_string());
    } else {
        params["toAddress"] = Value::String(address.to_string());
    }
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [params]
    });
    let body: Value = client
        .post_json(&endpoints.alchemy_rpc_base, &payload)
        .await?;
    let mut rows = Vec::new();
    for item in body
        .get("result")
        .and_then(|value| value.get("transfers"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let symbol = transfer_asset_symbol(item);
        let token_address = transfer_token_address(item);
        let amount = parse_transfer_amount(item);
        let normalized = if is_supported_priced_symbol(&symbol) {
            to_normalized_amount(amount, &symbol, eth_usd_rate)
        } else {
            Default::default()
        };
        rows.push(EthTransferRecord {
            tx_hash: item
                .get("hash")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase(),
            block_number,
            from_address: item
                .get("from")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase(),
            to_address: item
                .get("to")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase(),
            value_eth: normalized.eth.unwrap_or(0.0),
            value_usd: normalized.usd,
            payment_token_symbol: symbol,
            payment_token_address: token_address,
            category: item
                .get("category")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        });
    }
    Ok(rows)
}

pub async fn fetch_same_block_eth_transfers_for_address(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    block_number: i64,
    address: &str,
) -> Result<Vec<EthTransferRecord>, AppError> {
    fetch_same_block_value_transfers_for_address(client, endpoints, block_number, address, None)
        .await
}

pub async fn fetch_same_block_value_transfers_for_address(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    block_number: i64,
    address: &str,
    eth_usd_rate: Option<f64>,
) -> Result<Vec<EthTransferRecord>, AppError> {
    let (from_rows, to_rows) = tokio::join!(
        fetch_address_eth_transfers(
            client,
            endpoints,
            block_number,
            address,
            "from",
            eth_usd_rate
        ),
        fetch_address_eth_transfers(client, endpoints, block_number, address, "to", eth_usd_rate)
    );
    let mut rows = from_rows?;
    rows.extend(to_rows?);
    let mut deduped = BTreeMap::new();
    for row in rows {
        deduped.insert(
            (
                row.tx_hash.clone(),
                row.from_address.clone(),
                row.to_address.clone(),
                format!("{:.18}", row.value_eth),
                row.value_usd
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_default(),
                row.payment_token_symbol.clone(),
                row.payment_token_address.clone(),
            ),
            row,
        );
    }
    Ok(deduped.into_values().collect())
}

fn parse_hex_or_decimal_i64(value: Option<&Value>) -> i64 {
    let Some(value) = value else {
        return 0;
    };
    if let Some(number) = value.as_i64() {
        return number;
    }
    let text = value.as_str().unwrap_or("").trim();
    if text.is_empty() {
        return 0;
    }
    if text.starts_with("0x") || text.starts_with("0X") {
        i64::from_str_radix(text.trim_start_matches("0x").trim_start_matches("0X"), 16).unwrap_or(0)
    } else {
        text.parse::<i64>().unwrap_or(0)
    }
}

fn transfer_asset_symbol(item: &Value) -> String {
    item.get("asset")
        .and_then(Value::as_str)
        .or_else(|| {
            item.get("metadata")
                .and_then(|value| value.get("symbol"))
                .and_then(Value::as_str)
        })
        .unwrap_or("ETH")
        .trim()
        .to_uppercase()
}

fn transfer_token_address(item: &Value) -> String {
    item.get("rawContract")
        .and_then(|value| value.get("address"))
        .and_then(Value::as_str)
        .unwrap_or(ZERO_ADDRESS)
        .to_lowercase()
}

fn parse_transfer_amount(item: &Value) -> f64 {
    if let Some(value) = item.get("value") {
        match value {
            Value::String(text) if text.starts_with("0x") || text.starts_with("0X") => {
                return parse_raw_amount_with_decimals(item, text);
            }
            Value::String(text) => return text.parse::<f64>().unwrap_or(0.0),
            Value::Number(number) => return number.as_f64().unwrap_or(0.0),
            _ => {}
        }
    }
    item.get("rawContract")
        .and_then(|value| value.get("value"))
        .and_then(Value::as_str)
        .map(|text| parse_raw_amount_with_decimals(item, text))
        .unwrap_or(0.0)
}

fn parse_raw_amount_with_decimals(item: &Value, text: &str) -> f64 {
    let raw = if text.starts_with("0x") || text.starts_with("0X") {
        u128::from_str_radix(text.trim_start_matches("0x").trim_start_matches("0X"), 16).ok()
    } else {
        text.parse::<u128>().ok()
    }
    .unwrap_or(0);
    let decimals = item
        .get("rawContract")
        .and_then(|value| value.get("decimal"))
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str()?.parse::<i64>().ok())
        })
        .unwrap_or(18)
        .clamp(0, 36) as i32;
    raw as f64 / 10_f64.powi(decimals)
}
