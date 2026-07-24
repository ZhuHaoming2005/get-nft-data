//! Etherscan NFT transfer fallback for EVM candidates.

use serde_json::Value;

use super::alchemy::{normalize_token_id, FetchOutcome};
use super::http::HttpClient;
use super::types::{now_unix, EvidenceObservation, EvidenceStatus, TransferEvent};

fn etherscan_chain_id(chain: &str) -> Option<&'static str> {
    match chain {
        "ethereum" => Some("1"),
        "base" => Some("8453"),
        "polygon" | "matic" => Some("137"),
        _ => None,
    }
}

/// Fetch ERC-721 transfers via Etherscan v2 `tokennfttx` (fallback only).
pub async fn fetch_transfers(
    client: &HttpClient,
    base_url: &str,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    max_pages: usize,
) -> FetchOutcome<Vec<TransferEvent>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("etherscan_transfers");
    };
    let Some(chain_id) = etherscan_chain_id(chain) else {
        return FetchOutcome::failed(
            "etherscan",
            "etherscan_transfers",
            format!("unsupported chain {chain}"),
        );
    };

    let mut transfers = Vec::new();
    let mut truncated = false;
    let pages = max_pages.max(1);
    for page in 1..=pages {
        let url = format!(
            "{}{}chainid={}&module=account&action=tokennfttx&contractaddress={}&page={page}&offset=1000&startblock=0&endblock=9999999999&sort=asc&apikey={}",
            base_url.trim_end_matches('/'),
            if base_url.contains('?') { "&" } else { "?" },
            chain_id,
            contract,
            api_key
        );
        let payload = match client.get_json_etherscan(&url, &[]).await {
            Ok(v) => v,
            Err(e) => {
                if transfers.is_empty() {
                    return FetchOutcome::failed("etherscan", "etherscan_transfers", e);
                }
                truncated = true;
                break;
            }
        };
        let Some(items) = payload.get("result").and_then(Value::as_array) else {
            // Etherscan returns status/message when empty or errored.
            if transfers.is_empty() {
                let message = payload
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("empty etherscan result");
                if message.eq_ignore_ascii_case("OK") || message.eq_ignore_ascii_case("No transactions found") {
                    break;
                }
                return FetchOutcome::failed("etherscan", "etherscan_transfers", message);
            }
            break;
        };
        if items.is_empty() {
            break;
        }
        for item in items {
            transfers.push(parse_etherscan_transfer(item, contract));
        }
        if items.len() < 1000 {
            break;
        }
        if page == pages {
            truncated = true;
        }
    }

    let count = transfers.len();
    let status = if truncated {
        EvidenceStatus::Truncated
    } else if count == 0 {
        EvidenceStatus::Empty
    } else {
        EvidenceStatus::Complete
    };
    FetchOutcome {
        value: transfers,
        status,
        observation: Some(EvidenceObservation {
            source: "etherscan".into(),
            request_key: "etherscan_transfers".into(),
            observed_at: now_unix(),
            status,
        }),
        failure: None,
        truncated,
    }
}

pub fn parse_etherscan_transfer(item: &Value, fallback_contract: &str) -> TransferEvent {
    let from = item
        .get("from")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let to = item
        .get("to")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let is_mint = from.is_empty() || from == "0x0000000000000000000000000000000000000000";
    let _ = item
        .get("contractAddress")
        .and_then(Value::as_str)
        .unwrap_or(fallback_contract);
    TransferEvent {
        tx_hash: item
            .get("hash")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        token_id: normalize_token_id(item.get("tokenID").or_else(|| item.get("tokenId"))),
        from,
        to,
        timestamp: item
            .get("timeStamp")
            .and_then(Value::as_str)
            .and_then(|s| s.parse().ok())
            .or_else(|| item.get("timeStamp").and_then(Value::as_i64)),
        block_number: item
            .get("blockNumber")
            .and_then(Value::as_str)
            .and_then(|s| s.parse().ok())
            .or_else(|| item.get("blockNumber").and_then(Value::as_u64)),
        is_mint,
        gas_native: None,
        fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
    }
}
