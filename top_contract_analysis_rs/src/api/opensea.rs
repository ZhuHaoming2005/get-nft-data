use futures::{stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use reqwest::Url;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use crate::api::alchemy::fetch_eip2981_royalty_recipient;
use crate::api::{fetch_contract_collection_slug_alchemy_first, ApiEndpoints, AsyncApiClient};
use crate::currency::{is_chain_native_symbol, is_stablecoin_symbol, to_chain_normalized_amount};
use crate::error::AppError;
use crate::models::{ContractMetadata, NftSaleRecord, SeedNft};

const ROYALTY_RECIPIENT_LOOKUP_BUFFER: usize = 64;

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
        "solana" => "solana",
        "arbitrum" => "arbitrum",
        "optimism" => "optimism",
        "avalanche" => "avalanche",
        "zora" => "zora",
        "blast" => "blast",
        _ => "ethereum",
    }
}

fn normalize_opensea_identity(chain: &str, value: &str) -> String {
    if chain.trim().eq_ignore_ascii_case("solana") {
        value.trim().to_string()
    } else {
        value.trim().to_ascii_lowercase()
    }
}

fn opensea_identities_equal(chain: &str, left: &str, right: &str) -> bool {
    if chain.trim().eq_ignore_ascii_case("solana") {
        left.trim() == right.trim()
    } else {
        left.trim().eq_ignore_ascii_case(right.trim())
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
            if let Some(text) = address_like_field(raw) {
                return text;
            }
        }
    }
    ""
}

fn address_like_field(value: &Value) -> Option<&str> {
    value.as_str().or_else(|| {
        [
            "address",
            "hash",
            "wallet_address",
            "account_address",
            "user",
            "token",
        ]
        .iter()
        .find_map(|name| value.get(*name).and_then(address_like_field))
    })
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
    chain: &str,
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
        to_chain_normalized_amount(chain, token_amount, &symbol, eth_usd_rate)
    };
    (normalized.eth, normalized.usd, symbol, token_address)
}

fn nested_address_field(value: &Value, names: &[&str]) -> String {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(address_like_field))
        .unwrap_or("")
        .trim()
        .to_lowercase()
}

fn fee_recipient_address(payload: Option<&Value>) -> String {
    let payload = payload.unwrap_or(&Value::Null);
    nested_address_field(
        payload,
        &[
            "recipient",
            "recipientAddress",
            "feeRecipient",
            "receiver",
            "to",
            "toAddress",
        ],
    )
}

fn royalty_recipient_address(item: &Value) -> String {
    let fee_recipient = fee_recipient_address(item.get("royaltyFee"));
    if !fee_recipient.is_empty() {
        return fee_recipient;
    }
    nested_address_field(
        item,
        &[
            "royaltyRecipient",
            "royalty_recipient",
            "royaltyRecipientAddress",
            "royalty_recipient_address",
            "creatorFeeRecipient",
            "creator_fee_recipient",
        ],
    )
}

pub async fn fetch_alchemy_nft_sales(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
    token_id: Option<&str>,
    eth_usd_rate: Option<f64>,
) -> Result<Vec<NftSaleRecord>, AppError> {
    let mut page_key: Option<String> = None;
    let mut seen_page_keys = BTreeSet::new();
    let mut rows = Vec::new();
    loop {
        let mut url = Url::parse(&format!("{}/getNFTSales", endpoints.alchemy_nft_v3_base))
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
                decode_fee_eth(item.get("sellerFee"), chain, eth_usd_rate);
            let (protocol_fee_eth, protocol_fee_usd, protocol_symbol, protocol_token_address) =
                decode_fee_eth(item.get("protocolFee"), chain, eth_usd_rate);
            let (royalty_fee_eth, royalty_fee_usd, royalty_symbol, royalty_token_address) =
                decode_fee_eth(item.get("royaltyFee"), chain, eth_usd_rate);
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
                    .all(|value| is_chain_native_symbol(chain, value.as_str()));
            let eth_priced = !symbols.is_empty()
                && symbols.iter().all(|value| {
                    is_chain_native_symbol(chain, value.as_str())
                        || is_stablecoin_symbol(value.as_str())
                });
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
                royalty_recipient_address: royalty_recipient_address(item),
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
    let collection_slug = if token_id.filter(|value| !value.is_empty()).is_some() {
        None
    } else {
        fetch_opensea_contract_collection_slug(
            client,
            base_url,
            chain,
            contract_address,
            opensea_api_key,
        )
        .await?
    };
    fetch_opensea_nft_events_with_collection_slug(
        client,
        base_url,
        OpenSeaNftEventLookup {
            chain,
            contract_address,
            token_id,
            collection_slug: collection_slug.as_deref(),
        },
        opensea_api_key,
        eth_usd_rate,
    )
    .await
}

struct OpenSeaNftEventLookup<'a> {
    chain: &'a str,
    contract_address: &'a str,
    token_id: Option<&'a str>,
    collection_slug: Option<&'a str>,
}

async fn fetch_opensea_nft_events_with_collection_slug(
    client: &AsyncApiClient,
    base_url: &str,
    lookup: OpenSeaNftEventLookup<'_>,
    opensea_api_key: &str,
    eth_usd_rate: Option<f64>,
) -> Result<Vec<NftSaleRecord>, AppError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(opensea_api_key).map_err(|err| AppError::Http(err.to_string()))?,
    );
    let chain = opensea_chain(lookup.chain);
    let mut base_events_url =
        if let Some(token_id) = lookup.token_id.filter(|value| !value.is_empty()) {
            Url::parse(&format!(
                "{base_url}/api/v2/events/chain/{chain}/contract/{}/nfts/{token_id}",
                lookup.contract_address
            ))
            .map_err(|err| AppError::Http(err.to_string()))?
        } else {
            let Some(collection_slug) = lookup
                .collection_slug
                .filter(|value| !value.trim().is_empty())
            else {
                return Ok(Vec::new());
            };
            Url::parse(&format!(
                "{base_url}/api/v2/events/collection/{collection_slug}"
            ))
            .map_err(|err| AppError::Http(err.to_string()))?
        };
    base_events_url
        .query_pairs_mut()
        .append_pair("event_type", "sale")
        .append_pair("limit", "200");
    let mut rows = Vec::new();
    let mut next: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();
    loop {
        let mut url = base_events_url.clone();
        if let Some(cursor) = next.as_deref() {
            url.query_pairs_mut().append_pair("next", cursor);
        }
        let payload: Value = client
            .get_json_with_headers(url.as_str(), headers.clone())
            .await?;
        let events = payload
            .get("asset_events")
            .or_else(|| payload.get("events"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
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
                .unwrap_or_else(|| lookup.token_id.unwrap_or(""));
            let token_amount = raw_value / 10f64.powi(payment_decimals);
            let normalized = to_chain_normalized_amount(
                lookup.chain,
                token_amount,
                &payment_symbol,
                eth_usd_rate,
            );
            let parsed_contract_address = nft
                .get("contract")
                .or_else(|| nft.get("contract_address"))
                .or_else(|| nft.get("asset_contract"))
                .or_else(|| item.get("asset_contract_address"))
                .and_then(address_like_field)
                .unwrap_or("");
            let parsed_contract_address =
                normalize_opensea_identity(lookup.chain, parsed_contract_address);
            if !parsed_contract_address.is_empty()
                && !opensea_identities_equal(
                    lookup.chain,
                    &parsed_contract_address,
                    lookup.contract_address,
                )
            {
                continue;
            }
            rows.push(NftSaleRecord {
                contract_address: if parsed_contract_address.is_empty() {
                    normalize_opensea_identity(lookup.chain, lookup.contract_address)
                } else {
                    parsed_contract_address
                },
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
                    .trim()
                    .to_string(),
                block_number: integer_field(&item, &["block_number", "blockNumber"]),
                log_index: integer_field(&item, &["event_index", "log_index", "logIndex"]),
                bundle_index: integer_field(&item, &["bundle_index", "bundleIndex"]),
                buyer_address: normalize_opensea_identity(
                    lookup.chain,
                    string_or_nested_field(
                        &item,
                        &[
                            "to_account",
                            "winner_account",
                            "buyer",
                            "buyer_address",
                            "buyerAddress",
                            "to_address",
                        ],
                    ),
                ),
                seller_address: normalize_opensea_identity(
                    lookup.chain,
                    string_or_nested_field(
                        &item,
                        &[
                            "from_account",
                            "seller",
                            "seller_address",
                            "sellerAddress",
                            "from_address",
                        ],
                    ),
                ),
                marketplace: "opensea".to_string(),
                taker: item
                    .get("taker")
                    .and_then(address_like_field)
                    .unwrap_or("")
                    .to_string(),
                payment_token_symbol: payment_symbol.clone(),
                payment_token_address: normalize_opensea_identity(
                    lookup.chain,
                    payment
                        .get("address")
                        .or_else(|| payment.get("token_address"))
                        .or_else(|| payment.get("tokenAddress"))
                        .or_else(|| payment.get("token"))
                        .or_else(|| payment.get("contract"))
                        .and_then(address_like_field)
                        .unwrap_or(""),
                ),
                price_eth: normalized.eth,
                price_usd: normalized.usd,
                seller_fee_eth: normalized.eth.unwrap_or(0.0),
                seller_fee_usd: normalized.usd.unwrap_or(0.0),
                protocol_fee_eth: 0.0,
                protocol_fee_usd: 0.0,
                royalty_fee_eth: 0.0,
                royalty_fee_usd: 0.0,
                royalty_recipient_address: royalty_recipient_address(&item),
                source: "opensea".to_string(),
                is_native_eth: is_chain_native_symbol(lookup.chain, &payment_symbol),
            });
        }

        let next_cursor = payload.get("next").and_then(Value::as_str).unwrap_or("");
        if next_cursor.is_empty() {
            return Ok(rows);
        }
        if !seen_cursors.insert(next_cursor.to_string()) {
            return Err(AppError::Http(format!(
                "opensea events pagination stalled on repeated next cursor: {next_cursor}"
            )));
        }
        next = Some(next_cursor.to_string());
    }
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
        contract_address: normalize_opensea_identity(
            chain,
            string_field(&payload, &["address", "contract_address"]),
        )
        .if_empty_then(normalize_opensea_identity(chain, contract_address)),
        token_type,
        contract_deployer: normalize_opensea_identity(
            chain,
            string_field(&payload, &["contract_deployer", "deployer"]),
        ),
        deployed_block_number: int_field(
            &payload,
            &["deployed_block_number", "deployedBlockNumber"],
        ),
        deployed_block_time: int_field(&payload, &["deployed_block_time", "deployedBlockTime"]),
        owner_address: normalize_opensea_identity(
            chain,
            string_field(
                &payload,
                &["owner_address", "ownerAddress", "owner", "contract_owner"],
            ),
        ),
        admin_address: normalize_opensea_identity(
            chain,
            string_field(
                &payload,
                &["admin_address", "adminAddress", "admin", "contract_admin"],
            ),
        ),
        proxy_admin_address: normalize_opensea_identity(
            chain,
            string_field(
                &payload,
                &[
                    "proxy_admin_address",
                    "proxyAdminAddress",
                    "proxy_admin",
                    "proxyAdmin",
                ],
            ),
        ),
        name: string_field(&payload, &["name", "contract_name"])
            .to_string()
            .if_empty_then(string_field(collection, &["name", "collection_name"]).to_string()),
        symbol: string_field(&payload, &["symbol"]).to_string(),
    })
}

pub async fn fetch_opensea_contract_collection_slug(
    client: &AsyncApiClient,
    base_url: &str,
    chain: &str,
    contract_address: &str,
    opensea_api_key: &str,
) -> Result<Option<String>, AppError> {
    if opensea_api_key.trim().is_empty() {
        return Ok(None);
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
    let slug = string_field(collection, &["slug", "collection_slug"])
        .trim()
        .to_string();
    if slug.is_empty() {
        Ok(None)
    } else {
        Ok(Some(slug))
    }
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
                contract_address: normalize_opensea_identity(
                    chain,
                    raw.get("contract")
                        .or_else(|| raw.get("contract_address"))
                        .and_then(Value::as_str)
                        .unwrap_or(contract_address),
                ),
                token_id,
                name: string_field(raw, &["name", "title"]).to_string(),
                symbol: String::new(),
                token_uri,
                image_uri,
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

pub async fn fetch_opensea_account_holds_contract_nft(
    client: &AsyncApiClient,
    base_url: &str,
    chain: &str,
    account_address: &str,
    contract_address: &str,
    opensea_api_key: &str,
    collection_slug: Option<&str>,
) -> Result<bool, AppError> {
    if opensea_api_key.trim().is_empty() {
        return Ok(false);
    }

    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(opensea_api_key).map_err(|err| AppError::Http(err.to_string()))?,
    );

    let contract_key = normalize_opensea_identity(chain, contract_address);
    let mut next: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();
    loop {
        let mut url = Url::parse(&format!(
            "{base_url}/api/v2/chain/{}/account/{account_address}/nfts",
            opensea_chain(chain)
        ))
        .map_err(|err| AppError::Http(err.to_string()))?;
        url.query_pairs_mut().append_pair("limit", "200");
        if let Some(collection_slug) = collection_slug
            .map(str::trim)
            .filter(|slug| !slug.is_empty())
        {
            url.query_pairs_mut()
                .append_pair("collection", collection_slug);
        }
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
            let nft_contract = raw
                .get("contract")
                .or_else(|| raw.get("contract_address"))
                .or_else(|| raw.get("asset_contract"))
                .and_then(address_like_field)
                .unwrap_or("");
            let nft_contract = normalize_opensea_identity(chain, nft_contract);
            if !nft_contract.is_empty() && nft_contract == contract_key {
                return Ok(true);
            }
        }

        let next_cursor = payload.get("next").and_then(Value::as_str).unwrap_or("");
        if next_cursor.is_empty() {
            return Ok(false);
        }
        if !seen_cursors.insert(next_cursor.to_string()) {
            return Err(AppError::Http(format!(
                "opensea pagination stalled on repeated next cursor: {next_cursor}"
            )));
        }
        next = Some(next_cursor.to_string());
    }
}

async fn enrich_sales_with_royalty_recipient(
    alchemy_client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    contract_address: &str,
    mut rows: Vec<NftSaleRecord>,
) -> Vec<NftSaleRecord> {
    if rows.iter().all(|sale| {
        !sale.royalty_recipient_address.trim().is_empty()
            || (sale.royalty_fee_eth <= 0.0 && sale.royalty_fee_usd <= 0.0)
    }) {
        return rows;
    }

    let royalty_tokens = rows
        .iter()
        .filter(|sale| {
            sale.royalty_recipient_address.trim().is_empty()
                && (sale.royalty_fee_eth > 0.0 || sale.royalty_fee_usd > 0.0)
                && !sale.token_id.trim().is_empty()
        })
        .map(|sale| sale.token_id.clone())
        .collect::<BTreeSet<_>>();
    let mut royalty_by_token = BTreeMap::<String, String>::new();
    let mut fetched = stream::iter(royalty_tokens.into_iter().map(|token_id| async move {
        let recipient =
            fetch_eip2981_royalty_recipient(alchemy_client, endpoints, contract_address, &token_id)
                .await
                .ok()
                .flatten();
        (token_id, recipient)
    }))
    .buffer_unordered(ROYALTY_RECIPIENT_LOOKUP_BUFFER);
    while let Some((token_id, Some(recipient))) = fetched.next().await {
        royalty_by_token.insert(token_id, recipient);
    }
    for sale in &mut rows {
        if sale.royalty_recipient_address.trim().is_empty()
            && (sale.royalty_fee_eth > 0.0 || sale.royalty_fee_usd > 0.0)
        {
            if let Some(recipient) = royalty_by_token.get(&sale.token_id) {
                sale.royalty_recipient_address = recipient.clone();
            }
        }
    }

    rows
}

pub async fn fetch_contract_sales(
    client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
    opensea_api_key: &str,
    eth_usd_rate: Option<f64>,
) -> Result<Vec<NftSaleRecord>, AppError> {
    fetch_contract_sales_with_clients(
        client,
        client,
        endpoints,
        chain,
        contract_address,
        opensea_api_key,
        eth_usd_rate,
    )
    .await
}

pub async fn fetch_contract_sales_with_clients(
    alchemy_client: &AsyncApiClient,
    other_client: &AsyncApiClient,
    endpoints: &ApiEndpoints,
    chain: &str,
    contract_address: &str,
    opensea_api_key: &str,
    eth_usd_rate: Option<f64>,
) -> Result<Vec<NftSaleRecord>, AppError> {
    if chain.trim().eq_ignore_ascii_case("solana") {
        if opensea_api_key.trim().is_empty() {
            return Ok(Vec::new());
        }
        return fetch_opensea_nft_events(
            other_client,
            &endpoints.opensea_base,
            chain,
            contract_address,
            None,
            opensea_api_key,
            eth_usd_rate,
        )
        .await
        .map(|rows| filter_sales_for_contract(rows, chain, contract_address));
    }
    let alchemy_result = fetch_alchemy_nft_sales(
        alchemy_client,
        endpoints,
        chain,
        contract_address,
        None,
        eth_usd_rate,
    )
    .await
    .map(|rows| filter_sales_for_contract(rows, chain, contract_address));
    match alchemy_result {
        Ok(rows) => {
            return Ok(enrich_sales_with_royalty_recipient(
                alchemy_client,
                endpoints,
                contract_address,
                rows,
            )
            .await);
        }
        Err(err) if opensea_api_key.trim().is_empty() => {
            eprintln!(
                "warning: Alchemy contract sales failed for {contract_address}: {err}; continuing without contract sales"
            );
            return Ok(Vec::new());
        }
        Err(err) => {
            eprintln!(
                "warning: Alchemy contract sales failed for {contract_address}: {err}; falling back to OpenSea"
            );
        }
    }

    let collection_slug = match fetch_contract_collection_slug_alchemy_first(
        alchemy_client,
        other_client,
        endpoints,
        chain,
        contract_address,
        opensea_api_key,
    )
    .await
    {
        Ok(slug) => slug,
        Err(err) => {
            eprintln!(
                "warning: OpenSea contract sales collection lookup failed for {contract_address}: {err}; continuing without contract sales"
            );
            return Ok(Vec::new());
        }
    };
    match fetch_opensea_nft_events_with_collection_slug(
        other_client,
        &endpoints.opensea_base,
        OpenSeaNftEventLookup {
            chain,
            contract_address,
            token_id: None,
            collection_slug: collection_slug.as_deref(),
        },
        opensea_api_key,
        eth_usd_rate,
    )
    .await
    {
        Ok(rows) => {
            let rows = filter_sales_for_contract(rows, chain, contract_address);
            Ok(enrich_sales_with_royalty_recipient(
                alchemy_client,
                endpoints,
                contract_address,
                rows,
            )
            .await)
        }
        Err(err) => {
            eprintln!(
                "warning: OpenSea contract sales failed for {contract_address}: {err}; continuing without contract sales"
            );
            Ok(Vec::new())
        }
    }
}

fn filter_sales_for_contract(
    rows: Vec<NftSaleRecord>,
    chain: &str,
    contract_address: &str,
) -> Vec<NftSaleRecord> {
    rows.into_iter()
        .filter(|sale| opensea_identities_equal(chain, &sale.contract_address, contract_address))
        .collect()
}
