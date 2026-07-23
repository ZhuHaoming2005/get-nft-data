//! Magic Eden popular collections + sample-mint extraction for Solana seeds.

use serde_json::Value;

use crate::enrich::helius;
use crate::enrich::http::HttpClient;
use crate::error::Analysis2Error;

use super::address::{normalize_address, valid_solana_address};

const DEFAULT_MAGIC_EDEN_BASE: &str = "https://api-mainnet.magiceden.dev/v2";

#[derive(Clone, Debug, PartialEq)]
pub struct MagicEdenCollection {
    pub symbol: String,
    pub name: String,
    pub collection_address: Option<String>,
    pub rank: u32,
    pub volume: Option<f64>,
}

pub fn default_base_url() -> &'static str {
    DEFAULT_MAGIC_EDEN_BASE
}

pub async fn fetch_popular_collections(
    client: &HttpClient,
    base_url: &str,
) -> Result<Vec<MagicEdenCollection>, Analysis2Error> {
    let base = base_url.trim_end_matches('/');
    let url = format!("{base}/marketplace/popular_collections?timeRange=30d");
    let payload = client.get_json(&url, &[]).await?;
    Ok(parse_popular_collections(&payload))
}

pub fn parse_popular_collections(payload: &Value) -> Vec<MagicEdenCollection> {
    let Some(collections) = payload.as_array().map(Some).unwrap_or_else(|| {
        ["collections", "data", "results"]
            .into_iter()
            .find_map(|field| payload.get(field).and_then(Value::as_array))
    }) else {
        return Vec::new();
    };
    let mut seen_symbols = std::collections::BTreeSet::new();
    collections
        .iter()
        .enumerate()
        .filter_map(|(index, collection)| {
            let symbol = collection
                .get("symbol")
                .and_then(Value::as_str)?
                .trim();
            if symbol.is_empty() || !seen_symbols.insert(symbol.to_owned()) {
                return None;
            }
            let name = collection
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(symbol)
                .to_owned();
            let collection_address = [
                "onChainCollectionAddress",
                "on_chain_collection_address",
                "collectionAddress",
                "collection_address",
            ]
            .into_iter()
            .find_map(|field| {
                collection
                    .get(field)
                    .and_then(Value::as_str)
                    .and_then(|addr| normalize_address("solana", addr))
            });
            let volume = ["volume", "volumeAll", "volume_all", "totalVolume"]
                .into_iter()
                .find_map(|key| collection.get(key).and_then(json_number));
            Some(MagicEdenCollection {
                symbol: symbol.to_owned(),
                name,
                collection_address,
                rank: u32::try_from(index + 1).ok()?,
                volume,
            })
        })
        .collect()
}

pub fn parse_listing_mint(payload: &Value) -> Option<String> {
    let listing = payload
        .as_array()
        .and_then(|items| items.first())
        .or_else(|| payload.get("results")?.as_array()?.first())
        .or_else(|| payload.get("data")?.as_array()?.first())
        .or_else(|| payload.get("items")?.as_array()?.first())
        .or_else(|| payload.get("listings")?.as_array()?.first())?;
    listing
        .get("tokenMint")
        .or_else(|| listing.get("token_mint"))
        .or_else(|| listing.get("mintAddress"))
        .or_else(|| listing.get("mint"))
        .or_else(|| {
            listing
                .get("token")
                .and_then(|token| token.get("mintAddress").or_else(|| token.get("mint")))
        })
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| valid_solana_address(value))
        .map(str::to_owned)
}

/// Resolve collection address: prefer ME on-chain field, else listing mint → Helius DAS.
///
/// Returns `Err` with message `missing_helius_api_key` when a Helius DAS lookup is
/// required but no API key was provided. Listing / Helius HTTP failures are also
/// returned as `Err` (not swallowed).
pub async fn resolve_collection_address(
    client: &HttpClient,
    magic_eden_base: &str,
    helius_rpc: &str,
    helius_api_key: Option<&str>,
    collection: &MagicEdenCollection,
) -> Result<Option<String>, Analysis2Error> {
    if let Some(address) = &collection.collection_address {
        return Ok(Some(address.clone()));
    }
    let Some(api_key) = helius_api_key.filter(|k| !k.trim().is_empty()) else {
        return Err(Analysis2Error::invalid("missing_helius_api_key"));
    };
    let symbol = collection.symbol.trim();
    if symbol.is_empty() {
        return Ok(None);
    }
    let base = magic_eden_base.trim_end_matches('/');
    let encoded = urlencoding_minimal(symbol);
    let mut mint = None;
    let mut last_http_error: Option<Analysis2Error> = None;
    for resource in ["listings", "activities"] {
        let url = format!("{base}/collections/{encoded}/{resource}?offset=0&limit=1");
        match client.get_json(&url, &[]).await {
            Ok(payload) => {
                mint = parse_listing_mint(&payload);
                if mint.is_some() {
                    break;
                }
            }
            Err(error) => {
                last_http_error = Some(error);
                continue;
            }
        }
    }
    let Some(mint) = mint else {
        if let Some(error) = last_http_error {
            return Err(Analysis2Error::http(format!(
                "magic_eden_listing_error for {}: {error}",
                collection.symbol
            )));
        }
        return Ok(None);
    };
    helius::resolve_collection_address(client, helius_rpc, api_key, &mint).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_popular_and_listing_shapes() {
        let collections = parse_popular_collections(&json!([
            {"symbol":"popular", "name":"Popular"},
            {"symbol":"popular", "name":"Duplicate"}
        ]));
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].symbol, "popular");
        assert_eq!(collections[0].rank, 1);

        let addressed = parse_popular_collections(&json!([{
            "symbol":"addressed",
            "name":"Addressed",
            "onChainCollectionAddress":"So11111111111111111111111111111111111111112",
            "volume": 9.5
        }]));
        assert_eq!(
            addressed[0].collection_address.as_deref(),
            Some("So11111111111111111111111111111111111111112")
        );
        assert_eq!(addressed[0].volume, Some(9.5));

        assert_eq!(
            parse_listing_mint(&json!({"results":[{
                "mintAddress":"So11111111111111111111111111111111111111112"
            }]})),
            Some("So11111111111111111111111111111111111111112".into())
        );
    }
}
