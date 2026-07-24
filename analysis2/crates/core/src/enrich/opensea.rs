//! OpenSea helpers.
//!
//! Rate limit: all requests go through [`HttpClient::get_json_opensea`] which
//! applies the `top_contract_analysis_rs` token-bucket strategy (burst 4,
//! refill every 300 ms). Prefer Alchemy / Helius for enrichment; OpenSea is
//! only for EVM `select-seeds` ranking and last-resort sales / EVM slug fallback.

use serde_json::Value;

use crate::error::Analysis2Error;

use super::http::HttpClient;

const DEFAULT_OPENSEA_BASE: &str = "https://api.opensea.io";
const PAGE_SIZE: usize = 50;

/// One contract extracted from an OpenSea top-collections page.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenSeaRankedItem {
    pub chain: String,
    pub address: String,
    pub name: String,
    pub slug: String,
    pub volume: Option<f64>,
}

/// Fetch and accumulate top contracts for one EVM chain until `limit` or exhaustion.
pub async fn fetch_top_contracts(
    client: &HttpClient,
    base_url: &str,
    api_key: &str,
    chain: &str,
    limit: usize,
) -> Result<Vec<OpenSeaRankedItem>, Analysis2Error> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let base = base_url.trim_end_matches('/');
    let opensea_chain = opensea_chain_query(chain);
    let mut ranked = Vec::with_capacity(limit);
    let mut seen_addresses = std::collections::BTreeSet::new();
    let mut seen_cursors = std::collections::BTreeSet::new();
    let mut cursor: Option<String> = None;

    while ranked.len() < limit {
        let mut url = format!(
            "{base}/api/v2/collections/top?chains={opensea_chain}&limit={PAGE_SIZE}&sort_by=thirty_days_volume"
        );
        if let Some(cursor) = &cursor {
            url.push_str("&cursor=");
            url.push_str(&urlencoding_minimal(cursor));
        }
        let payload = client
            .get_json_opensea(&url, &[("x-api-key", api_key)])
            .await?;
        let page = parse_top_collections(chain, &payload);
        for item in page {
            if !seen_addresses.insert(item.address.clone()) {
                continue;
            }
            ranked.push(item);
            if ranked.len() == limit {
                break;
            }
        }
        if ranked.len() == limit {
            break;
        }
        let next = next_cursor(&payload);
        let Some(next) = next else {
            break;
        };
        if !seen_cursors.insert(next.clone()) {
            return Err(Analysis2Error::http(format!(
                "OpenSea top pagination repeated cursor for {chain}"
            )));
        }
        cursor = Some(next);
    }
    Ok(ranked)
}

/// Parse OpenSea top-collections JSON into ranked contract items for `chain`.
pub fn parse_top_collections(chain: &str, payload: &Value) -> Vec<OpenSeaRankedItem> {
    let Some(collections) = collection_list(payload) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for collection in collections {
        out.extend(collection_items(chain, collection));
    }
    out
}

fn collection_list(payload: &Value) -> Option<&Vec<Value>> {
    for key in ["collections", "top_collections", "data", "results"] {
        if let Some(list) = payload.get(key).and_then(Value::as_array) {
            return Some(list);
        }
    }
    None
}

fn collection_items(chain: &str, collection: &Value) -> Vec<OpenSeaRankedItem> {
    let name = collection
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("")
        .to_owned();
    let slug = collection
        .get("collection")
        .or_else(|| collection.get("slug"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("")
        .to_owned();
    if slug.is_empty() {
        return Vec::new();
    }
    let volume = collection_volume(collection);
    let display_name = if name.is_empty() {
        slug.clone()
    } else {
        name
    };

    let mut items = Vec::new();
    for contract in collection_contracts(collection) {
        let raw_chain = contract
            .get("chain")
            .or_else(|| contract.get("chain_identifier"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if !chain_matches(chain, raw_chain) {
            continue;
        }
        let Some(address) = contract_address(contract) else {
            continue;
        };
        if !valid_evm_address(&address) {
            continue;
        }
        items.push(OpenSeaRankedItem {
            chain: chain.to_ascii_lowercase(),
            address: address.to_ascii_lowercase(),
            name: display_name.clone(),
            slug: slug.clone(),
            volume,
        });
    }
    items
}

fn collection_contracts(collection: &Value) -> Vec<&Value> {
    let mut contracts = Vec::new();
    for field in ["contracts", "primary_asset_contracts", "asset_contracts"] {
        if let Some(list) = collection.get(field).and_then(Value::as_array) {
            contracts.extend(list.iter());
        }
    }
    if contracts.is_empty() {
        contracts.push(collection);
    }
    contracts
}

fn contract_address(contract: &Value) -> Option<String> {
    for key in ["address", "contract_address", "contractAddress"] {
        if let Some(addr) = contract
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(addr.to_owned());
        }
    }
    None
}

fn collection_volume(collection: &Value) -> Option<f64> {
    for container in [Some(collection), collection.get("stats")] {
        let Some(container) = container else {
            continue;
        };
        for key in ["thirty_days_volume", "thirty_day_volume"] {
            if let Some(value) = container.get(key).and_then(json_number) {
                return Some(value);
            }
        }
    }
    None
}

fn next_cursor(payload: &Value) -> Option<String> {
    for key in ["cursor", "next", "next_cursor"] {
        if let Some(value) = payload
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(value.to_owned());
        }
    }
    None
}

fn chain_matches(chain: &str, raw: &str) -> bool {
    let chain = chain.trim().to_ascii_lowercase();
    let raw = raw.trim().to_ascii_lowercase();
    if chain == raw {
        return true;
    }
    matches!((chain.as_str(), raw.as_str()), ("polygon", "matic") | ("matic", "polygon"))
}

fn opensea_chain_query(chain: &str) -> &'static str {
    match chain.trim().to_ascii_lowercase().as_str() {
        "polygon" | "matic" => "polygon",
        "base" => "base",
        _ => "ethereum",
    }
}

fn valid_evm_address(value: &str) -> bool {
    let value = value.trim();
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .is_some_and(|hex| hex.len() == 40 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn json_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|n| n as f64))
        .or_else(|| value.as_u64().map(|n| n as f64))
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .filter(|v| v.is_finite() && *v >= 0.0)
}

fn urlencoding_minimal(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push(char::from(b"0123456789ABCDEF"[(byte >> 4) as usize]));
                out.push(char::from(b"0123456789ABCDEF"[(byte & 0xf) as usize]));
            }
        }
    }
    out
}

/// Default OpenSea API base URL.
pub fn default_base_url() -> &'static str {
    DEFAULT_OPENSEA_BASE
}

/// Rare fallback: OpenSea events for a contract when preferred sales lack required fields.
pub async fn fetch_contract_sales(
    client: &HttpClient,
    base_url: &str,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
) -> crate::enrich::alchemy::FetchOutcome<Vec<crate::enrich::types::SaleEvent>> {
    use crate::enrich::alchemy::FetchOutcome;
    use crate::enrich::types::{now_unix, EvidenceObservation, EvidenceStatus};

    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("opensea_sales");
    };
    let chain_q = opensea_chain_query(chain);
    let url = format!(
        "{}/api/v2/events/nft?chain={chain_q}&contract_address={}&event_type=sale&limit=50",
        base_url.trim_end_matches('/'),
        urlencoding_minimal(contract)
    );
    let payload = match client
        .get_json_opensea(&url, &[("x-api-key", api_key)])
        .await
    {
        Ok(v) => v,
        Err(e) => {
            // Unknown / unlisted contracts return 404 — treat as Empty, not Failed.
            // Avoid print_provider_error spam and quality.failures for expected misses.
            if crate::enrich::http::is_http_not_found(&e) {
                return FetchOutcome {
                    value: Vec::new(),
                    status: EvidenceStatus::Empty,
                    observation: Some(EvidenceObservation {
                        source: "opensea".into(),
                        request_key: "opensea_sales".into(),
                        observed_at: now_unix(),
                        status: EvidenceStatus::Empty,
                    }),
                    failure: None,
                    truncated: false,
                };
            }
            return FetchOutcome::failed("opensea", "opensea_sales", e);
        }
    };
    let sales = parse_sale_events(&payload);
    let count = sales.len();
    let status = if count == 0 {
        EvidenceStatus::Empty
    } else {
        EvidenceStatus::Complete
    };
    FetchOutcome {
        value: sales,
        status,
        observation: Some(EvidenceObservation {
            source: "opensea".into(),
            request_key: "opensea_sales".into(),
            observed_at: now_unix(),
            status,
        }),
        failure: None,
        truncated: false,
    }
}

/// Parse OpenSea NFT sale events payload.
pub fn parse_sale_events(payload: &Value) -> Vec<crate::enrich::types::SaleEvent> {
    use crate::enrich::types::SaleEvent;
    let mut out = Vec::new();
    let events = payload
        .get("asset_events")
        .or_else(|| payload.get("events"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for event in events {
        let event_type = event
            .get("event_type")
            .or_else(|| event.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("sale");
        if !event_type.eq_ignore_ascii_case("sale") && !event_type.eq_ignore_ascii_case("order") {
            // Still accept order-fulfilled style sale payloads that include payment.
            if event.get("payment").is_none() && event.get("total_price").is_none() {
                continue;
            }
        }
        let nft = event.get("nft").or_else(|| event.get("asset"));
        let token_id = nft
            .and_then(|n| n.get("identifier").or_else(|| n.get("token_id")))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let payment = event.get("payment");
        let native = payment
            .and_then(|p| p.get("quantity").or_else(|| p.get("amount")))
            .and_then(json_number)
            .or_else(|| event.get("total_price").and_then(json_number));
        let decimals = payment
            .and_then(|p| p.get("decimals"))
            .and_then(Value::as_u64)
            .unwrap_or(18) as i32;
        let native_amount = native.map(|n| n / 10f64.powi(decimals));
        out.push(SaleEvent {
            tx_hash: event
                .get("transaction")
                .and_then(|t| t.get("hash").or(Some(t)))
                .and_then(Value::as_str)
                .or_else(|| event.get("transaction_hash").and_then(Value::as_str))
                .unwrap_or("")
                .to_owned(),
            token_id,
            seller: event
                .get("seller")
                .and_then(|s| s.get("address").or(Some(s)))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase(),
            buyer: event
                .get("buyer")
                .and_then(|b| b.get("address").or(Some(b)))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase(),
            timestamp: event
                .get("event_timestamp")
                .and_then(Value::as_i64)
                .or_else(|| {
                    event
                        .get("event_timestamp")
                        .and_then(Value::as_str)
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.timestamp())
                }),
            block_number: None,
            marketplace: Some("opensea".into()),
            native_amount,
            usd_amount: payment
                .and_then(|p| p.get("price_usd").or_else(|| p.get("usd")))
                .and_then(json_number),
            currency_symbol: payment
                .and_then(|p| p.get("symbol"))
                .and_then(Value::as_str)
                .map(str::to_owned),
        });
    }
    out
}

/// OpenSea contract → collection slug (fallback when Alchemy has no slug).
pub async fn fetch_contract_collection_slug(
    client: &HttpClient,
    base_url: &str,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
) -> Option<String> {
    let api_key = api_key?;
    let chain_q = opensea_chain_query(chain);
    let url = format!(
        "{}/api/v2/chain/{chain_q}/contract/{}",
        base_url.trim_end_matches('/'),
        urlencoding_minimal(contract)
    );
    let payload = client
        .get_json_opensea(&url, &[("x-api-key", api_key)])
        .await
        .ok()?;
    if let Some(s) = payload.get("collection").and_then(Value::as_str) {
        let s = s.trim();
        return (!s.is_empty()).then(|| s.to_owned());
    }
    let collection = payload.get("collection")?;
    let slug = collection
        .get("slug")
        .or_else(|| collection.get("collection_slug"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    Some(slug.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_top_collections_for_matching_chain() {
        let payload = json!({
            "collections": [{
                "name": "Alpha",
                "collection": "alpha",
                "thirty_days_volume": "12.5",
                "contracts": [{
                    "chain": "ethereum",
                    "address": "0x1111111111111111111111111111111111111111"
                }]
            }]
        });
        let items = parse_top_collections("ethereum", &payload);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].address, "0x1111111111111111111111111111111111111111");
        assert_eq!(items[0].name, "Alpha");
        assert_eq!(items[0].volume, Some(12.5));
    }

    #[test]
    fn polygon_accepts_matic_chain_identifier() {
        let payload = json!({
            "collections": [{
                "collection": "poly",
                "contracts": [{
                    "chain": "matic",
                    "address": "0x2222222222222222222222222222222222222222"
                }]
            }]
        });
        let items = parse_top_collections("polygon", &payload);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].chain, "polygon");
    }

    #[test]
    fn polygon_query_param_is_polygon_not_matic() {
        assert_eq!(opensea_chain_query("polygon"), "polygon");
        assert_eq!(opensea_chain_query("matic"), "polygon");
        let url = format!(
            "https://example.com/api/v2/collections/top?chains={}&limit={PAGE_SIZE}&sort_by=thirty_days_volume",
            opensea_chain_query("polygon")
        );
        assert!(url
            .split('?')
            .nth(1)
            .unwrap_or("")
            .split('&')
            .any(|part| part == "chains=polygon"));
        assert!(!url.contains("chains=matic"));
    }

    #[test]
    fn skips_other_chain_contracts() {
        let payload = json!({
            "collections": [{
                "collection": "mixed",
                "contracts": [
                    {"chain": "base", "address": "0x3333333333333333333333333333333333333333"},
                    {"chain": "ethereum", "address": "0x4444444444444444444444444444444444444444"}
                ]
            }]
        });
        let items = parse_top_collections("base", &payload);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].address, "0x3333333333333333333333333333333333333333");
    }
}
