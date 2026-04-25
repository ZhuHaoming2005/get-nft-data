use reqwest::Url;
use serde_json::Value;

use crate::api::AsyncApiClient;
use crate::error::AppError;
use crate::models::TransferRecord;

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

fn etherscan_chain_id(chain: &str) -> Option<&'static str> {
    match chain.to_lowercase().as_str() {
        "ethereum" => Some("1"),
        "base" => Some("8453"),
        "polygon" => Some("137"),
        _ => None,
    }
}

pub async fn fetch_etherscan_contract_transfers(
    client: &AsyncApiClient,
    base_url: &str,
    api_key: &str,
    chain: &str,
    contract_address: &str,
    token_type: &str,
) -> Result<Vec<TransferRecord>, AppError> {
    let chain_id = etherscan_chain_id(chain).ok_or_else(|| {
        AppError::InvalidData(format!("unsupported chain for etherscan fallback: {chain}"))
    })?;
    let action = if token_type.eq_ignore_ascii_case("ERC1155") {
        "token1155tx"
    } else {
        "tokennfttx"
    };
    let mut page = 1;
    let mut transfers = Vec::new();
    loop {
        let mut url = Url::parse(base_url).map_err(|err| AppError::Http(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("chainid", chain_id)
            .append_pair("module", "account")
            .append_pair("action", action)
            .append_pair("contractaddress", contract_address)
            .append_pair("page", &page.to_string())
            .append_pair("offset", "1000")
            .append_pair("startblock", "0")
            .append_pair("endblock", "9999999999")
            .append_pair("sort", "asc")
            .append_pair("apikey", api_key);
        let body: Value = client.get_json(url.as_str()).await?;
        let Some(items) = body.get("result").and_then(Value::as_array) else {
            return Ok(transfers);
        };
        for item in items {
            transfers.push(TransferRecord {
                contract_address: item
                    .get("contractAddress")
                    .and_then(Value::as_str)
                    .unwrap_or(contract_address)
                    .to_lowercase(),
                token_id: normalize_token_id(item.get("tokenID")),
                tx_hash: item
                    .get("hash")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                log_index: item
                    .get("transactionIndex")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0),
                block_number: item
                    .get("blockNumber")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0),
                block_time: item
                    .get("timeStamp")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0),
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
                event_type: if action == "token1155tx" {
                    "erc1155".to_string()
                } else {
                    "erc721".to_string()
                },
                source: "etherscan".to_string(),
            });
        }
        if items.len() < 1000 {
            return Ok(transfers);
        }
        page += 1;
    }
}
