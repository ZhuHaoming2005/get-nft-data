use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use reqwest::Url;
use serde_json::Value;
use std::collections::BTreeSet;

use crate::analysis::scoring::metadata_document_from_json;
use crate::api::{ApiEndpoints, AsyncApiClient};
use crate::currency::{is_native_eth_symbol, is_supported_priced_symbol, to_normalized_amount};
use crate::error::AppError;
use crate::models::{ContractMetadata, NftSaleRecord, SeedNft};

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

fn opensea_chain(chain: &str) -> &str {
    match chain.trim().to_lowercase().as_str() {
        "polygon" => "matic",
        "ethereum" => "ethereum",
        "base" => "base",
        "arbitrum" => "arbitrum",
        "optimism" => "optimism",
        "avalanche" => "avalanche",
        "zora" => "zora",
        "blast" => "blast",
        _ => "ethereum",
    }
}

fn string_field<'a>(value: &'a Value, names: &[&str]) -> &'a str {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
        .unwrap_or("")
}

fn string_or_nested_field<'a>(value: &'a Value, names: &[&str]) -> &'a str {
    for name in names {
        if let Some(raw) = value.get(*name) {
            if let Some(text) = raw.as_str() {
                return text;
            }
            if let Some(text) = raw.get("address").and_then(Value::as_str) {
                return text;
            }
            if let Some(text) = raw.get("hash").and_then(Value::as_str) {
                return text;
            }
        }
    }
    ""
}

fn parse_numeric_text(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        trimmed.parse::<f64>().ok()
    }
}

fn numeric_value(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|number| number as f64))
        .or_else(|| value.as_u64().map(|number| number as f64))
        .or_else(|| value.as_str().and_then(parse_numeric_text))
}

fn integer_field(value: &Value, names: &[&str]) -> i64 {
    names
        .iter()
        .find_map(|name| {
            value.get(*name).and_then(|raw| {
                raw.as_i64().or_else(|| {
                    raw.as_str().and_then(|text| {
                        let trimmed = text.trim();
                        if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
                            i64::from_str_radix(
                                trimmed.trim_start_matches("0x").trim_start_matches("0X"),
                                16,
                            )
                            .ok()
                        } else {
                            trimmed.parse::<i64>().ok()
                        }
                    })
                })
            })
        })
        .unwrap_or(0)
}

fn int_field(value: &Value, names: &[&str]) -> i64 {
    names
        .iter()
        .find_map(|name| {
            value.get(*name).and_then(|raw| {
                raw.as_i64()
                    .or_else(|| raw.as_str().and_then(|text| text.parse::<i64>().ok()))
            })
        })
        .unwrap_or(0)
}

fn decode_fee_eth(
    payload: Option<&Value>,
    eth_usd_rate: Option<f64>,
) -> (Option<f64>, Option<f64>, String, String) {
    let payload = payload.unwrap_or(&Value::Null);
    let amount_raw = payload
        .get("amount")
        .and_then(Value::as_str)
        .unwrap_or("0")
        .trim();
    let symbol = payload
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_uppercase();
    let token_address = payload
        .get("contractAddress")
        .or_else(|| payload.get("tokenAddress"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_lowercase();
    let decimals = payload
        .get("decimals")
        .and_then(Value::as_i64)
        .or_else(|| {
            payload
                .get("decimals")
                .and_then(Value::as_str)
                .and_then(|text| text.parse::<i64>().ok())
        })
        .unwrap_or(18)
        .max(0) as u32;
    let amount = numeric_value(payload.get("amount")).unwrap_or_else(|| {
        if amount_raw.is_empty() {
            0.0
        } else {
            amount_raw.parse::<f64>().unwrap_or(0.0)
        }
    });
    let token_amount = amount / 10f64.powi(decimals as i32);
    let normalized = if token_amount == 0.0 && symbol.is_empty() {
        crate::currency::NormalizedCurrencyAmount {
            eth: Some(0.0),
            usd: Some(0.0),
        }
    } else {
        to_normalized_amount(token_amount, &symbol, eth_usd_rate)
    };
    (normalized.eth, normalized.usd, symbol, token_address)
}

pub async fn fetch_alchemy_nft_sales(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    contract_address: &str,
    token_id: Option<&str>,
    eth_usd_rate: Option<f64>,
) -> Result<Vec<NftSaleRecord>, AppError> {
    let mut page_key: Option<String> = None;
    let mut seen_page_keys = BTreeSet::new();
    let mut rows = Vec::new();
    loop {
        let mut url = Url::parse(&format!("{}/getNFTSales", endpoints.alchemy_nft_v2_base))
            .map_err(|err| AppError::Http(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("fromBlock", "0")
            .append_pair("toBlock", "latest")
            .append_pair("order", "asc")
            .append_pair("contractAddress", contract_address);
        if let Some(token_id) = token_id.filter(|value| !value.is_empty()) {
            url.query_pairs_mut().append_pair("tokenId", token_id);
        }
        if let Some(page_key) = page_key.as_deref() {
            url.query_pairs_mut().append_pair("pageKey", page_key);
        }
        let payload: Value = client.get_json(url.as_str()).await?;
        for item in payload
            .get("nftSales")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let (seller_fee_eth, seller_fee_usd, fee_symbol, fee_token_address) =
                decode_fee_eth(item.get("sellerFee"), eth_usd_rate);
            let (protocol_fee_eth, protocol_fee_usd, protocol_symbol, protocol_token_address) =
                decode_fee_eth(item.get("protocolFee"), eth_usd_rate);
            let (royalty_fee_eth, royalty_fee_usd, royalty_symbol, royalty_token_address) =
                decode_fee_eth(item.get("royaltyFee"), eth_usd_rate);
            let symbols: std::collections::BTreeSet<String> = [
                fee_symbol.clone(),
                protocol_symbol.clone(),
                royalty_symbol.clone(),
            ]
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect();
            let native_eth = !symbols.is_empty()
                && symbols
                    .iter()
                    .all(|value| is_native_eth_symbol(value.as_str()));
            let eth_priced = !symbols.is_empty()
                && symbols
                    .iter()
                    .all(|value| is_supported_priced_symbol(value.as_str()));
            let price_eth = if eth_priced {
                seller_fee_eth
                    .zip(protocol_fee_eth)
                    .zip(royalty_fee_eth)
                    .map(|((seller, protocol), royalty)| seller + protocol + royalty)
            } else {
                None
            };
            let price_usd = if eth_priced {
                seller_fee_usd
                    .zip(protocol_fee_usd)
                    .zip(royalty_fee_usd)
                    .map(|((seller, protocol), royalty)| seller + protocol + royalty)
            } else {
                None
            };
            rows.push(NftSaleRecord {
                contract_address: item
                    .get("contractAddress")
                    .and_then(Value::as_str)
                    .unwrap_or(contract_address)
                    .to_lowercase(),
                token_id: normalize_token_id(item.get("tokenId")),
                tx_hash: item
                    .get("transactionHash")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_lowercase(),
                block_number: integer_field(item, &["blockNumber", "block_number"]),
                log_index: integer_field(item, &["logIndex", "log_index"]),
                bundle_index: integer_field(item, &["bundleIndex", "bundle_index"]),
                buyer_address: string_or_nested_field(item, &["buyerAddress", "buyer_address"])
                    .to_lowercase(),
                seller_address: string_or_nested_field(item, &["sellerAddress", "seller_address"])
                    .to_lowercase(),
                marketplace: item
                    .get("marketplace")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                taker: item
                    .get("taker")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                payment_token_symbol: if !fee_symbol.is_empty() {
                    fee_symbol.clone()
                } else if !protocol_symbol.is_empty() {
                    protocol_symbol.clone()
                } else {
                    royalty_symbol.clone()
                },
                payment_token_address: if !fee_token_address.is_empty() {
                    fee_token_address
                } else if !protocol_token_address.is_empty() {
                    protocol_token_address
                } else {
                    royalty_token_address
                },
                price_eth,
                price_usd,
                seller_fee_eth: seller_fee_eth.unwrap_or(0.0),
                seller_fee_usd: seller_fee_usd.unwrap_or(0.0),
                protocol_fee_eth: protocol_fee_eth.unwrap_or(0.0),
                protocol_fee_usd: protocol_fee_usd.unwrap_or(0.0),
                royalty_fee_eth: royalty_fee_eth.unwrap_or(0.0),
                royalty_fee_usd: royalty_fee_usd.unwrap_or(0.0),
                source: "alchemy".to_string(),
                is_native_eth: native_eth,
            });
        }
        let next_page_key = payload.get("pageKey").and_then(Value::as_str).unwrap_or("");
        if next_page_key.is_empty() {
            return Ok(rows);
        }
        if !seen_page_keys.insert(next_page_key.to_string()) {
            return Err(AppError::Http(format!(
                "pagination stalled on repeated pageKey: {next_page_key}"
            )));
        }
        page_key = Some(next_page_key.to_string());
    }
}

pub async fn fetch_opensea_nft_events(
    client: &AsyncApiClient,
    base_url: &str,
    chain: &str,
    contract_address: &str,
    token_id: Option<&str>,
    opensea_api_key: &str,
    eth_usd_rate: Option<f64>,
) -> Result<Vec<NftSaleRecord>, AppError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(opensea_api_key).map_err(|err| AppError::Http(err.to_string()))?,
    );
    let chain = opensea_chain(chain);
    let url = if let Some(token_id) = token_id.filter(|value| !value.is_empty()) {
        format!("{base_url}/api/v2/events/chain/{chain}/contract/{contract_address}/nfts/{token_id}?event_type=sale")
    } else {
        format!("{base_url}/api/v2/events?event_type=sale&asset_contract_address={contract_address}&chain={chain}")
    };
    let payload: Value = client.get_json_with_headers(&url, headers).await?;
    let events = payload
        .get("asset_events")
        .or_else(|| payload.get("events"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut rows = Vec::new();
    for item in events {
        let event_type = item
            .get("event_type")
            .or_else(|| item.get("eventType"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_lowercase();
        if !event_type.is_empty() && event_type != "sale" {
            continue;
        }
        let nft = item
            .get("nft")
            .or_else(|| item.get("asset"))
            .unwrap_or(&Value::Null);
        let payment = item
            .get("payment")
            .or_else(|| item.get("payment_token"))
            .unwrap_or(&Value::Null);
        let payment_symbol = payment
            .get("symbol")
            .or_else(|| item.get("payment_token_symbol"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_uppercase();
        let raw_value = numeric_value(
            item.get("payment_quantity")
                .or_else(|| payment.get("quantity"))
                .or_else(|| item.get("price"))
                .or_else(|| item.get("total_price")),
        )
        .unwrap_or(0.0);
        let payment_decimals = payment
            .get("decimals")
            .and_then(Value::as_i64)
            .or_else(|| {
                payment
                    .get("decimals")
                    .and_then(Value::as_str)
                    .and_then(|text| text.parse::<i64>().ok())
            })
            .unwrap_or(18)
            .max(0) as i32;
        let token_id_value = nft
            .get("identifier")
            .or_else(|| nft.get("token_id"))
            .and_then(Value::as_str)
            .unwrap_or_else(|| token_id.unwrap_or(""));
        let normalized = to_normalized_amount(
            raw_value / 10f64.powi(payment_decimals),
            &payment_symbol,
            eth_usd_rate,
        );
        rows.push(NftSaleRecord {
            contract_address: nft
                .get("contract")
                .or_else(|| nft.get("contract_address"))
                .or_else(|| item.get("asset_contract_address"))
                .and_then(Value::as_str)
                .unwrap_or(contract_address)
                .to_lowercase(),
            token_id: normalize_token_id(Some(&Value::String(token_id_value.to_string()))),
            tx_hash: item
                .get("transaction")
                .or_else(|| item.get("transaction_hash"))
                .or_else(|| item.get("order_hash"))
                .and_then(|value| {
                    value
                        .as_str()
                        .or_else(|| value.get("hash").and_then(Value::as_str))
                })
                .unwrap_or("")
                .to_lowercase(),
            block_number: integer_field(&item, &["block_number", "blockNumber"]),
            log_index: integer_field(&item, &["event_index", "log_index", "logIndex"]),
            bundle_index: integer_field(&item, &["bundle_index", "bundleIndex"]),
            buyer_address: string_or_nested_field(
                &item,
                &[
                    "to_account",
                    "winner_account",
                    "buyer",
                    "buyer_address",
                    "buyerAddress",
                    "taker",
                ],
            )
            .to_lowercase(),
            seller_address: string_or_nested_field(
                &item,
                &[
                    "from_account",
                    "seller",
                    "seller_address",
                    "sellerAddress",
                    "maker",
                ],
            )
            .to_lowercase(),
            marketplace: "opensea".to_string(),
            taker: item
                .get("taker")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            payment_token_symbol: payment_symbol.clone(),
            payment_token_address: payment
                .get("address")
                .or_else(|| payment.get("token_address"))
                .or_else(|| payment.get("tokenAddress"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase(),
            price_eth: normalized.eth,
            price_usd: normalized.usd,
            seller_fee_eth: normalized.eth.unwrap_or(0.0),
            seller_fee_usd: normalized.usd.unwrap_or(0.0),
            protocol_fee_eth: 0.0,
            protocol_fee_usd: 0.0,
            royalty_fee_eth: 0.0,
            royalty_fee_usd: 0.0,
            source: "opensea".to_string(),
            is_native_eth: is_native_eth_symbol(&payment_symbol),
        });
    }
    Ok(rows)
}

pub async fn fetch_opensea_contract_metadata(
    client: &AsyncApiClient,
    base_url: &str,
    chain: &str,
    contract_address: &str,
    opensea_api_key: &str,
) -> Result<ContractMetadata, AppError> {
    if opensea_api_key.trim().is_empty() {
        return Err(AppError::Http(
            "OpenSea API key is required for contract metadata".to_string(),
        ));
    }

    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(opensea_api_key).map_err(|err| AppError::Http(err.to_string()))?,
    );

    let url = format!(
        "{base_url}/api/v2/chain/{}/contract/{contract_address}",
        opensea_chain(chain)
    );
    let payload: Value = client.get_json_with_headers(&url, headers).await?;
    let collection = payload.get("collection").unwrap_or(&Value::Null);
    let token_type =
        string_field(&payload, &["contract_standard", "token_type", "tokenType"]).to_uppercase();
    Ok(ContractMetadata {
        chain: chain.to_string(),
        contract_address: string_field(&payload, &["address", "contract_address"])
            .trim()
            .to_lowercase()
            .if_empty_then(contract_address.to_lowercase()),
        token_type,
        contract_deployer: string_field(&payload, &["contract_deployer", "deployer"])
            .trim()
            .to_lowercase(),
        deployed_block_number: int_field(
            &payload,
            &["deployed_block_number", "deployedBlockNumber"],
        ),
        name: string_field(&payload, &["name", "contract_name"])
            .to_string()
            .if_empty_then(string_field(collection, &["name", "collection_name"]).to_string()),
        symbol: string_field(&payload, &["symbol"]).to_string(),
    })
}

trait EmptyStringExt {
    fn if_empty_then(self, fallback: String) -> String;
}

impl EmptyStringExt for String {
    fn if_empty_then(self, fallback: String) -> String {
        if self.is_empty() {
            fallback
        } else {
            self
        }
    }
}

pub async fn fetch_opensea_contract_nfts(
    client: &AsyncApiClient,
    base_url: &str,
    chain: &str,
    contract_address: &str,
    opensea_api_key: &str,
) -> Result<Vec<SeedNft>, AppError> {
    if opensea_api_key.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(opensea_api_key).map_err(|err| AppError::Http(err.to_string()))?,
    );

    let mut rows = Vec::new();
    let mut next: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();
    loop {
        let mut url = Url::parse(&format!(
            "{base_url}/api/v2/chain/{}/contract/{contract_address}/nfts",
            opensea_chain(chain)
        ))
        .map_err(|err| AppError::Http(err.to_string()))?;
        url.query_pairs_mut().append_pair("limit", "200");
        if let Some(next) = next.as_deref() {
            url.query_pairs_mut().append_pair("next", next);
        }

        let payload: Value = client
            .get_json_with_headers(url.as_str(), headers.clone())
            .await?;
        for raw in payload
            .get("nfts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let token_id = normalize_token_id(
                raw.get("identifier")
                    .or_else(|| raw.get("token_id"))
                    .or_else(|| raw.get("tokenId")),
            );
            if token_id.is_empty() {
                continue;
            }
            let image_uri = string_field(
                raw,
                &[
                    "image_url",
                    "display_image_url",
                    "display_animation_url",
                    "animation_url",
                ],
            )
            .to_string();
            let token_uri =
                string_field(raw, &["metadata_url", "token_uri", "tokenUri"]).to_string();
            let metadata_json = raw
                .get("metadata")
                .or_else(|| raw.get("metadata_json"))
                .map(Value::to_string)
                .unwrap_or_default();
            rows.push(SeedNft {
                chain: chain.to_string(),
                contract_address: raw
                    .get("contract")
                    .or_else(|| raw.get("contract_address"))
                    .and_then(Value::as_str)
                    .unwrap_or(contract_address)
                    .to_lowercase(),
                token_id,
                name: string_field(raw, &["name", "title"]).to_string(),
                symbol: String::new(),
                token_uri,
                image_uri,
                metadata_doc: metadata_document_from_json(&metadata_json),
                metadata_json,
            });
        }

        let next_cursor = payload.get("next").and_then(Value::as_str).unwrap_or("");
        if next_cursor.is_empty() {
            return Ok(rows);
        }
        if !seen_cursors.insert(next_cursor.to_string()) {
            return Err(AppError::Http(format!(
                "opensea pagination stalled on repeated next cursor: {next_cursor}"
            )));
        }
        next = Some(next_cursor.to_string());
    }
}

pub async fn fetch_contract_sales(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
    opensea_api_key: &str,
    eth_usd_rate: Option<f64>,
) -> Result<Vec<NftSaleRecord>, AppError> {
    if !opensea_api_key.trim().is_empty() {
        match fetch_opensea_nft_events(
            client,
            &endpoints.opensea_base,
            chain,
            contract_address,
            None,
            opensea_api_key,
            eth_usd_rate,
        )
        .await
        {
            Ok(rows) if !rows.is_empty() => return Ok(rows),
            Ok(_) => {}
            Err(err) => {
                eprintln!(
                    "warning: OpenSea contract sales failed for {contract_address}: {err}; falling back to Alchemy"
                );
            }
        }
    }
    fetch_alchemy_nft_sales(client, endpoints, contract_address, None, eth_usd_rate).await
}
