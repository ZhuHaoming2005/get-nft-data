//! Alchemy NFT / transfers / sales / prices / receipt-gas / native EXTERNAL clients.

use ahash::AHashMap;
use serde_json::{json, Value};

use super::http::HttpClient;
use super::types::{
    day_bucket, now_unix, status_from_count, EvidenceObservation, EvidenceStatus, HolderRecord,
    PriceBucket, ProviderEndpoints, SaleEvent, TransferEvent,
};

/// Parsed native EXTERNAL transfer from `alchemy_getAssetTransfers`.
#[derive(Clone, Debug, Default)]
pub struct NativeTransfer {
    pub tx_hash: String,
    pub from: String,
    pub to: String,
    pub value_native: Option<f64>,
    pub timestamp: Option<i64>,
    pub block_number: Option<u64>,
}

const ZERO: &str = "0x0000000000000000000000000000000000000000";
const MAX_COUNT_HEX: &str = "0x3e8";

#[derive(Default)]
pub struct FetchOutcome<T> {
    pub value: T,
    pub status: EvidenceStatus,
    pub observation: Option<EvidenceObservation>,
    pub failure: Option<String>,
    pub truncated: bool,
}

impl<T: Default> FetchOutcome<T> {
    pub fn skipped(request_key: &str) -> Self {
        Self {
            value: T::default(),
            status: EvidenceStatus::NotRequested,
            observation: Some(EvidenceObservation {
                source: "none".into(),
                request_key: request_key.into(),
                observed_at: now_unix(),
                status: EvidenceStatus::NotRequested,
            }),
            failure: None,
            truncated: false,
        }
    }

    pub fn failed(source: &str, request_key: &str, error: impl ToString) -> Self {
        let message = format!("{request_key}: {}", error.to_string());
        Self {
            value: T::default(),
            status: EvidenceStatus::Failed,
            observation: Some(EvidenceObservation {
                source: source.into(),
                request_key: request_key.into(),
                observed_at: now_unix(),
                status: EvidenceStatus::Failed,
            }),
            failure: Some(message),
            truncated: false,
        }
    }

    pub fn ok(
        value: T,
        count: usize,
        truncated: bool,
        source: &str,
        request_key: &str,
    ) -> Self {
        let status = status_from_count(count, truncated);
        Self {
            value,
            status,
            observation: Some(EvidenceObservation {
                source: source.into(),
                request_key: request_key.into(),
                observed_at: now_unix(),
                status,
            }),
            failure: None,
            truncated,
        }
    }
}

/// Fetch ERC-721/1155 transfers via `alchemy_getAssetTransfers`.
pub async fn fetch_transfers(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    max_pages: usize,
) -> FetchOutcome<Vec<TransferEvent>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_transfers");
    };
    let Some(rpc) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_transfers",
            format!("unsupported alchemy network for {chain}"),
        );
    };

    let mut transfers = Vec::new();
    let mut page_key: Option<String> = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut truncated = false;
    let pages = max_pages.max(1);

    for page in 0..pages {
        let mut params = json!({
            "fromBlock": "0x0",
            "toBlock": "latest",
            "category": ["erc721", "erc1155"],
            "contractAddresses": [contract],
            "withMetadata": true,
            "excludeZeroValue": false,
            "maxCount": MAX_COUNT_HEX,
            "order": "asc"
        });
        if let Some(key) = &page_key {
            params["pageKey"] = Value::String(key.clone());
        }
        let body = json!({
            "jsonrpc": "2.0",
            "id": format!("transfers-{page}"),
            "method": "alchemy_getAssetTransfers",
            "params": [params]
        });
        let payload = match client.post_json(&rpc, &[], &body).await {
            Ok(v) => v,
            Err(e) => {
                if transfers.is_empty() {
                    return FetchOutcome::failed("alchemy", "alchemy_transfers", e);
                }
                truncated = true;
                break;
            }
        };
        if let Some(error) = payload.get("error") {
            if transfers.is_empty() {
                return FetchOutcome::failed(
                    "alchemy",
                    "alchemy_transfers",
                    error.to_string(),
                );
            }
            truncated = true;
            break;
        }
        let result = payload.get("result").cloned().unwrap_or(Value::Null);
        let page_items = result
            .get("transfers")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for item in &page_items {
            transfers.extend(parse_alchemy_transfer(item, contract));
        }
        let next = result
            .get("pageKey")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|s| !s.is_empty());
        match next {
            Some(next) => {
                if !seen.insert(next.clone()) {
                    truncated = true;
                    break;
                }
                page_key = Some(next);
                if page + 1 == pages {
                    truncated = true;
                }
            }
            None => break,
        }
    }

    let count = transfers.len();
    FetchOutcome::ok(transfers, count, truncated, "alchemy", "alchemy_transfers")
}

/// Fetch owners via Alchemy NFT API `getOwnersForContract`.
pub async fn fetch_holders(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    max_pages: usize,
) -> FetchOutcome<Vec<HolderRecord>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_holders");
    };
    let Some(mut url) = endpoints.alchemy_nft(chain, api_key, "getOwnersForContract") else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_holders",
            format!("unsupported alchemy network for {chain}"),
        );
    };
    url.push_str(&format!(
        "{}contractAddress={}&withTokenBalances=true",
        if url.contains('?') { "&" } else { "?" },
        urlencoding_minimal(contract)
    ));

    let mut holders = Vec::new();
    let mut page_key: Option<String> = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut truncated = false;
    let pages = max_pages.max(1);

    for page in 0..pages {
        let mut page_url = url.clone();
        if let Some(key) = &page_key {
            page_url.push_str("&pageKey=");
            page_url.push_str(&urlencoding_minimal(key));
        }
        let payload = match client.get_json(&page_url, &[]).await {
            Ok(v) => v,
            Err(e) => {
                if holders.is_empty() {
                    return FetchOutcome::failed("alchemy", "alchemy_holders", e);
                }
                truncated = true;
                break;
            }
        };
        holders.extend(parse_holders(&payload));
        let next = payload
            .get("pageKey")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|s| !s.is_empty());
        match next {
            Some(next) => {
                if !seen.insert(next.clone()) {
                    truncated = true;
                    break;
                }
                page_key = Some(next);
                if page + 1 == pages {
                    truncated = true;
                }
            }
            None => break,
        }
    }

    let count = holders.len();
    FetchOutcome::ok(holders, count, truncated, "alchemy", "alchemy_holders")
}

/// Fetch NFT sales via Alchemy `getNFTSales`.
pub async fn fetch_sales(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    max_pages: usize,
) -> FetchOutcome<Vec<SaleEvent>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_sales");
    };
    let Some(base) = endpoints.alchemy_nft(chain, api_key, "getNFTSales") else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_sales",
            format!("unsupported alchemy network for {chain}"),
        );
    };

    let mut sales = Vec::new();
    let mut page_key: Option<String> = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut truncated = false;
    let pages = max_pages.max(1);

    for page in 0..pages {
        let mut url = format!(
            "{base}?fromBlock=0&toBlock=latest&order=asc&contractAddress={}",
            urlencoding_minimal(contract)
        );
        if let Some(key) = &page_key {
            url.push_str("&pageKey=");
            url.push_str(&urlencoding_minimal(key));
        }
        let payload = match client.get_json(&url, &[]).await {
            Ok(v) => v,
            Err(e) => {
                if sales.is_empty() {
                    return FetchOutcome::failed("alchemy", "alchemy_sales", e);
                }
                truncated = true;
                break;
            }
        };
        sales.extend(parse_nft_sales(&payload, chain));
        let next = payload
            .get("pageKey")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|s| !s.is_empty());
        match next {
            Some(next) => {
                if !seen.insert(next.clone()) {
                    truncated = true;
                    break;
                }
                page_key = Some(next);
                if page + 1 == pages {
                    truncated = true;
                }
            }
            None => break,
        }
    }

    let count = sales.len();
    FetchOutcome::ok(sales, count, truncated, "alchemy", "alchemy_sales")
}

/// Fetch Alchemy historical prices for native symbols over UTC day buckets.
pub async fn fetch_prices(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    day_buckets: &[i64],
) -> FetchOutcome<Vec<PriceBucket>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_prices");
    };
    if day_buckets.is_empty() {
        return FetchOutcome::skipped("alchemy_prices");
    }
    let symbol = native_symbol(chain);
    let start = *day_buckets.iter().min().unwrap_or(&0);
    let end = *day_buckets.iter().max().unwrap_or(&0);
    let url = format!(
        "{}/{}/tokens/historical",
        endpoints.alchemy_prices.trim_end_matches('/'),
        api_key
    );
    let body = json!({
        "symbol": symbol,
        "startTime": rfc3339(start),
        "endTime": rfc3339(end.saturating_add(86_399)),
        "interval": "1d",
        "withMarketData": false
    });
    let payload = match client.post_json(&url, &[], &body).await {
        Ok(v) => v,
        Err(e) => return FetchOutcome::failed("alchemy", "alchemy_prices", e),
    };
    if let Some(error) = payload.get("error") {
        return FetchOutcome::failed("alchemy", "alchemy_prices", error.to_string());
    }
    let wanted: std::collections::BTreeSet<i64> = day_buckets.iter().copied().collect();
    let mut prices = Vec::new();
    for row in payload
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(ts) = row
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339)
        else {
            continue;
        };
        let day = day_bucket(ts);
        if !wanted.contains(&day) {
            continue;
        }
        let Some(usd) = json_f64(row.get("value")) else {
            continue;
        };
        prices.push(PriceBucket {
            chain: chain.to_owned(),
            day_utc: day,
            symbol: symbol.to_owned(),
            usd_per_native: usd,
        });
    }
    let missing = wanted.len().saturating_sub(prices.len());
    let truncated = missing > 0 && !prices.is_empty();
    let failed_partial = missing > 0 && prices.is_empty();
    if failed_partial {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_prices",
            "no overlapping historical price buckets",
        );
    }
    let count = prices.len();
    FetchOutcome::ok(prices, count, truncated, "alchemy", "alchemy_prices")
}

pub fn parse_alchemy_transfer(item: &Value, fallback_contract: &str) -> Vec<TransferEvent> {
    let tx = item
        .get("hash")
        .or_else(|| item.get("transactionHash"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
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
    let timestamp = item
        .get("metadata")
        .and_then(|m| m.get("blockTimestamp"))
        .and_then(parse_timestamp);
    let block_number = item.get("blockNum").and_then(parse_block_number);
    let is_mint = from.is_empty() || from == ZERO;
    let _ = item
        .get("rawContract")
        .and_then(|c| c.get("address"))
        .and_then(Value::as_str)
        .unwrap_or(fallback_contract);
    transfer_token_ids(item)
        .into_iter()
        .filter(|id| !id.is_empty())
        .map(|token_id| TransferEvent {
            tx_hash: tx.clone(),
            token_id,
            from: from.clone(),
            to: to.clone(),
            timestamp,
            block_number,
            is_mint,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
        })
        .collect()
}

pub fn parse_holders(payload: &Value) -> Vec<HolderRecord> {
    let mut out = Vec::new();
    for row in payload
        .get("owners")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let owner = row
            .get("ownerAddress")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        if owner.is_empty() || owner == ZERO {
            continue;
        }
        let balances = row
            .get("tokenBalances")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if balances.is_empty() {
            out.push(HolderRecord {
                token_id: String::new(),
                owner: owner.clone(),
                balance: None,
            });
            continue;
        }
        for balance in balances {
            let token_id = normalize_token_id(balance.get("tokenId"));
            let bal = parse_i64(balance.get("balance"));
            out.push(HolderRecord {
                token_id,
                owner: owner.clone(),
                balance: bal,
            });
        }
    }
    out
}

pub fn parse_nft_sales(payload: &Value, chain: &str) -> Vec<SaleEvent> {
    let mut out = Vec::new();
    for item in payload
        .get("nftSales")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let seller_fee = fee_amount(item.get("sellerFee"));
        let protocol_fee = fee_amount(item.get("protocolFee"));
        let royalty_fee = fee_amount(item.get("royaltyFee"));
        let native = match (seller_fee, protocol_fee, royalty_fee) {
            (Some(a), Some(b), Some(c)) => Some(a + b + c),
            (Some(a), Some(b), None) => Some(a + b),
            (Some(a), None, Some(c)) => Some(a + c),
            (None, Some(b), Some(c)) => Some(b + c),
            (Some(a), None, None) => Some(a),
            _ => None,
        };
        let symbol = item
            .get("sellerFee")
            .and_then(|f| f.get("symbol"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        out.push(SaleEvent {
            tx_hash: item
                .get("transactionHash")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            token_id: normalize_token_id(item.get("tokenId")),
            seller: item
                .get("sellerAddress")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase(),
            buyer: item
                .get("buyerAddress")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase(),
            timestamp: item.get("blockTimestamp").and_then(parse_timestamp),
            block_number: item
                .get("blockNumber")
                .and_then(parse_block_number)
                .or_else(|| item.get("blockNum").and_then(parse_block_number)),
            marketplace: item
                .get("marketplace")
                .and_then(Value::as_str)
                .map(str::to_owned),
            native_amount: native,
            usd_amount: None,
            currency_symbol: symbol.or_else(|| Some(native_symbol(chain).to_owned())),
        });
    }
    out
}

fn transfer_token_ids(item: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    if let Some(meta) = item.get("erc1155Metadata").and_then(Value::as_array) {
        for token in meta {
            let id = normalize_token_id(token.get("tokenId"));
            if !id.is_empty() {
                ids.push(id);
            }
        }
    }
    if ids.is_empty() {
        ids.push(normalize_token_id(
            item.get("erc721TokenId").or_else(|| item.get("tokenId")),
        ));
    }
    ids
}

pub fn normalize_token_id(raw: Option<&Value>) -> String {
    let Some(raw) = raw else {
        return String::new();
    };
    let text = raw
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| raw.to_string());
    let trimmed = text.trim();
    if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
        i128::from_str_radix(
            trimmed.trim_start_matches(['0', 'x', 'X']),
            16,
        )
        .map(|v| v.to_string())
        .unwrap_or_else(|_| trimmed.to_owned())
    } else {
        trimmed.to_owned()
    }
}

fn fee_amount(fee: Option<&Value>) -> Option<f64> {
    let fee = fee?;
    if let Some(amount) = json_f64(fee.get("amount")) {
        let decimals = fee
            .get("decimals")
            .and_then(Value::as_u64)
            .unwrap_or(18) as i32;
        return Some(amount / 10f64.powi(decimals));
    }
    json_f64(fee.get("value")).or_else(|| json_f64(fee.get("rawAmount").or(fee.get("amount"))))
}

fn parse_timestamp(value: &Value) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    let text = value.as_str()?.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(n) = text.parse::<i64>() {
        return Some(n);
    }
    parse_rfc3339(text)
}

fn parse_rfc3339(text: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|dt| dt.timestamp())
}

fn parse_block_number(value: &Value) -> Option<u64> {
    if let Some(n) = value.as_u64() {
        return Some(n);
    }
    let text = value.as_str()?.trim();
    if text.starts_with("0x") || text.starts_with("0X") {
        u64::from_str_radix(text.trim_start_matches(['0', 'x', 'X']), 16).ok()
    } else {
        text.parse().ok()
    }
}

fn parse_i64(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    let text = value.as_str()?.trim();
    if text.starts_with("0x") || text.starts_with("0X") {
        i64::from_str_radix(text.trim_start_matches(['0', 'x', 'X']), 16).ok()
    } else {
        text.parse().ok()
    }
}

fn json_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|n| n as f64))
        .or_else(|| value.as_u64().map(|n| n as f64))
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .filter(|v| v.is_finite())
}

fn native_symbol(chain: &str) -> &'static str {
    match chain {
        "polygon" | "matic" => "POL",
        "solana" => "SOL",
        _ => "ETH",
    }
}

fn rfc3339(ts: i64) -> String {
    use chrono::{SecondsFormat, TimeZone, Utc};
    Utc.timestamp_opt(ts, 0)
        .single()
        .map(|v| v.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".into())
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

/// Parsed receipt fields used to fill transfer gas / fee payer.
#[derive(Clone, Debug, Default)]
pub struct ReceiptGas {
    pub gas_native: Option<f64>,
    pub fee_payer: Option<String>,
}

/// Collect unique non-empty tx hashes from transfers and sales (lowercase).
pub fn collect_unique_tx_hashes(transfers: &[TransferEvent], sales: &[SaleEvent]) -> Vec<String> {
    let mut set = std::collections::BTreeSet::new();
    for event in transfers {
        let hash = event.tx_hash.trim();
        if !hash.is_empty() {
            set.insert(hash.to_ascii_lowercase());
        }
    }
    for event in sales {
        let hash = event.tx_hash.trim();
        if !hash.is_empty() {
            set.insert(hash.to_ascii_lowercase());
        }
    }
    set.into_iter().collect()
}

/// JSON-RPC batch size for `eth_getTransactionReceipt` (Alchemy supports arrays).
const RECEIPT_RPC_BATCH_SIZE: usize = 80;

/// Fetch `eth_getTransactionReceipt` for unique tx hashes; parse gas fee in native units.
///
/// Uses JSON-RPC batches gated only by [`HttpClient`] concurrency (no nested
/// per-phase semaphore). Batch HTTP failures fall back to per-hash requests.
///
/// Status: NotRequested (no key) / Empty (no txs) / Complete (all ok) /
/// Truncated (partial) / Failed (all fail).
pub async fn fetch_receipt_gas(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    tx_hashes: &[String],
) -> FetchOutcome<AHashMap<String, ReceiptGas>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_receipts");
    };
    if tx_hashes.is_empty() {
        return FetchOutcome::ok(
            AHashMap::new(),
            0,
            false,
            "alchemy",
            "alchemy_receipts",
        );
    }
    let Some(rpc) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_receipts",
            format!("unsupported alchemy network for {chain}"),
        );
    };

    let mut handles = Vec::new();
    for (batch_idx, chunk) in tx_hashes.chunks(RECEIPT_RPC_BATCH_SIZE).enumerate() {
        let client = client.clone();
        let rpc = rpc.clone();
        let hashes: Vec<String> = chunk.to_vec();
        handles.push(tokio::spawn(async move {
            fetch_receipt_gas_batch(&client, &rpc, batch_idx, &hashes).await
        }));
    }

    let mut ok = AHashMap::new();
    let mut failures = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(batch_rows) => {
                for (hash, result) in batch_rows {
                    match result {
                        Ok(info) => {
                            ok.insert(hash, info);
                        }
                        Err(err) => failures.push(format!("{hash}: {err}")),
                    }
                }
            }
            Err(e) => failures.push(format!("receipt batch join failed: {e}")),
        }
    }

    let requested = tx_hashes.len();
    let succeeded = ok.len();
    if succeeded == 0 {
        let detail = if failures.is_empty() {
            "all receipt fetches failed".into()
        } else {
            failures.join("; ")
        };
        return FetchOutcome::failed("alchemy", "alchemy_receipts", detail);
    }
    let truncated = succeeded < requested;
    let mut outcome = FetchOutcome::ok(ok, succeeded, truncated, "alchemy", "alchemy_receipts");
    if truncated && !failures.is_empty() {
        outcome.failure = Some(format!(
            "alchemy_receipts: partial failures ({}/{}): {}",
            failures.len(),
            requested,
            failures.into_iter().take(3).collect::<Vec<_>>().join("; ")
        ));
    }
    outcome
}

async fn fetch_receipt_gas_batch(
    client: &HttpClient,
    rpc: &str,
    batch_idx: usize,
    hashes: &[String],
) -> Vec<(String, Result<ReceiptGas, String>)> {
    if hashes.is_empty() {
        return Vec::new();
    }
    let body = Value::Array(
        hashes
            .iter()
            .enumerate()
            .map(|(i, hash)| {
                json!({
                    "jsonrpc": "2.0",
                    "id": format!("receipt-{batch_idx}-{i}"),
                    "method": "eth_getTransactionReceipt",
                    "params": [hash]
                })
            })
            .collect(),
    );

    match client.post_json(rpc, &[], &body).await {
        Ok(payload) => match parse_receipt_batch_payload(&payload, batch_idx, hashes) {
            Ok(rows) => rows,
            Err(_) => fetch_receipt_gas_singles(client, rpc, batch_idx, hashes).await,
        },
        Err(_) => fetch_receipt_gas_singles(client, rpc, batch_idx, hashes).await,
    }
}

fn parse_receipt_batch_payload(
    payload: &Value,
    batch_idx: usize,
    hashes: &[String],
) -> Result<Vec<(String, Result<ReceiptGas, String>)>, ()> {
    let responses = payload.as_array().ok_or(())?;
    let mut by_id: AHashMap<String, &Value> = AHashMap::with_capacity(responses.len());
    for response in responses {
        let id = match response.get("id") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            _ => continue,
        };
        by_id.insert(id, response);
    }
    if by_id.is_empty() && responses.len() == hashes.len() {
        return Ok(hashes
            .iter()
            .zip(responses.iter())
            .map(|(hash, response)| (hash.clone(), receipt_from_rpc_response(response)))
            .collect());
    }
    let mut out = Vec::with_capacity(hashes.len());
    for (i, hash) in hashes.iter().enumerate() {
        let id = format!("receipt-{batch_idx}-{i}");
        match by_id.get(&id) {
            Some(response) => out.push((hash.clone(), receipt_from_rpc_response(response))),
            None => {
                // Positional fallback when the provider rewrites ids.
                if let Some(response) = responses.get(i) {
                    out.push((hash.clone(), receipt_from_rpc_response(response)));
                } else {
                    out.push((hash.clone(), Err("missing batch response".into())));
                }
            }
        }
    }
    Ok(out)
}

fn receipt_from_rpc_response(response: &Value) -> Result<ReceiptGas, String> {
    if let Some(error) = response.get("error") {
        return Err(error.to_string());
    }
    let Some(result) = response.get("result").filter(|v| !v.is_null()) else {
        return Err("null receipt result".into());
    };
    match parse_receipt_gas(result) {
        Some(info) if info.gas_native.is_some() => Ok(info),
        Some(_) | None => Err("missing gasUsed/effectiveGasPrice".into()),
    }
}

async fn fetch_receipt_gas_singles(
    client: &HttpClient,
    rpc: &str,
    batch_idx: usize,
    hashes: &[String],
) -> Vec<(String, Result<ReceiptGas, String>)> {
    let mut handles = Vec::with_capacity(hashes.len());
    for (i, hash) in hashes.iter().cloned().enumerate() {
        let client = client.clone();
        let rpc = rpc.to_owned();
        handles.push(tokio::spawn(async move {
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("receipt-{batch_idx}-{i}"),
                "method": "eth_getTransactionReceipt",
                "params": [hash]
            });
            let payload = match client.post_json(&rpc, &[], &body).await {
                Ok(v) => v,
                Err(e) => return (hash, Err(e.to_string())),
            };
            (hash, receipt_from_rpc_response(&payload))
        }));
    }
    let mut out = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(row) => out.push(row),
            Err(e) => out.push((String::new(), Err(format!("receipt task join failed: {e}")))),
        }
    }
    out
}

/// Attach receipt gas / fee_payer onto matching transfers (by lowercase tx hash).
pub fn attach_receipt_gas(
    transfers: &mut [TransferEvent],
    receipts: &AHashMap<String, ReceiptGas>,
) {
    if receipts.is_empty() {
        return;
    }
    for transfer in transfers {
        let key = transfer.tx_hash.trim().to_ascii_lowercase();
        if key.is_empty() {
            continue;
        }
        let Some(info) = receipts.get(&key) else {
            continue;
        };
        if transfer.gas_native.is_none() {
            transfer.gas_native = info.gas_native;
        }
        if transfer.fee_payer.is_none() {
            if let Some(payer) = info.fee_payer.clone() {
                transfer.fee_payer = Some(payer);
            }
        }
    }
}

pub fn parse_receipt_gas(result: &Value) -> Option<ReceiptGas> {
    let gas_used = parse_u128(result.get("gasUsed"))?;
    let gas_price = parse_u128(
        result
            .get("effectiveGasPrice")
            .or_else(|| result.get("gasPrice")),
    )?;
    let wei = gas_used.checked_mul(gas_price)?;
    let gas_native = (wei as f64) / 1e18;
    if !gas_native.is_finite() {
        return None;
    }
    let fee_payer = result
        .get("from")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase());
    Some(ReceiptGas {
        gas_native: Some(gas_native),
        fee_payer,
    })
}

fn parse_u128(value: Option<&Value>) -> Option<u128> {
    let value = value?;
    if let Some(n) = value.as_u64() {
        return Some(u128::from(n));
    }
    let text = value.as_str()?.trim();
    if text.is_empty() {
        return None;
    }
    if let Some(hex) = text
        .strip_prefix("0x")
        .or_else(|| text.strip_prefix("0X"))
    {
        if hex.is_empty() {
            return Some(0);
        }
        u128::from_str_radix(hex, 16).ok()
    } else {
        text.parse().ok()
    }
}

/// Fetch native EXTERNAL transfers for one address (`from` or `to`) in a block window.
///
/// `to_block == u64::MAX` means `"latest"`. One page only (`maxCount`); pageKey ⇒ Truncated.
pub async fn fetch_external_transfers(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    address: &str,
    direction: &str,
    from_block: u64,
    to_block: u64,
    request_id: usize,
) -> FetchOutcome<Vec<NativeTransfer>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_external");
    };
    let Some(rpc) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_external",
            format!("unsupported alchemy network for {chain}"),
        );
    };

    let to_block_value = if to_block == u64::MAX {
        Value::String("latest".into())
    } else {
        Value::String(format!("0x{to_block:x}"))
    };
    let mut params = json!({
        "fromBlock": format!("0x{from_block:x}"),
        "toBlock": to_block_value,
        "category": ["external"],
        "withMetadata": true,
        "excludeZeroValue": true,
        "maxCount": MAX_COUNT_HEX,
        "order": "asc"
    });
    match direction {
        "from" => params["fromAddress"] = Value::String(address.to_owned()),
        "to" => params["toAddress"] = Value::String(address.to_owned()),
        other => {
            return FetchOutcome::failed(
                "alchemy",
                "alchemy_external",
                format!("invalid direction {other}"),
            );
        }
    }
    let body = json!({
        "jsonrpc": "2.0",
        "id": format!("external-{direction}-{request_id}"),
        "method": "alchemy_getAssetTransfers",
        "params": [params]
    });
    let payload = match client.post_json(&rpc, &[], &body).await {
        Ok(v) => v,
        Err(e) => return FetchOutcome::failed("alchemy", "alchemy_external", e),
    };
    if let Some(error) = payload.get("error") {
        return FetchOutcome::failed("alchemy", "alchemy_external", error.to_string());
    }
    let result = payload.get("result").cloned().unwrap_or(Value::Null);
    let mut transfers = Vec::new();
    for item in result
        .get("transfers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(row) = parse_native_transfer(item) {
            transfers.push(row);
        }
    }
    let truncated = result
        .get("pageKey")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .is_some();
    let count = transfers.len();
    FetchOutcome::ok(transfers, count, truncated, "alchemy", "alchemy_external")
}

/// Parse one Alchemy EXTERNAL transfer row; non-external categories are skipped.
pub fn parse_native_transfer(item: &Value) -> Option<NativeTransfer> {
    let category = item
        .get("category")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    if !category.is_empty() && category != "external" {
        return None;
    }
    let tx_hash = item
        .get("hash")
        .or_else(|| item.get("transactionHash"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())?
        .to_ascii_lowercase();
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
    if from.is_empty() && to.is_empty() {
        return None;
    }
    let value_native = json_f64(item.get("value")).or_else(|| {
        let raw = item.get("rawContract")?;
        let wei = parse_u128(raw.get("value"))?;
        let decimals = parse_u128(raw.get("decimal"))
            .or_else(|| raw.get("decimals").and_then(Value::as_u64).map(u128::from))
            .unwrap_or(18) as i32;
        let amount = (wei as f64) / 10f64.powi(decimals);
        amount.is_finite().then_some(amount)
    });
    Some(NativeTransfer {
        tx_hash,
        from,
        to,
        value_native,
        timestamp: item
            .get("metadata")
            .and_then(|m| m.get("blockTimestamp"))
            .and_then(parse_timestamp),
        block_number: item.get("blockNum").and_then(parse_block_number),
    })
}

/// Whether sales justify a rare OpenSea fallback.
///
/// True `Empty` from Alchemy means no sales — do not call OpenSea.
/// Fallback only for Failed/NotRequested, or Complete/Truncated rows missing amounts.
pub fn sales_need_opensea_fallback(sales: &[SaleEvent], sales_status: EvidenceStatus) -> bool {
    match sales_status {
        EvidenceStatus::Failed | EvidenceStatus::NotRequested => true,
        EvidenceStatus::Complete | EvidenceStatus::Truncated => sales
            .iter()
            .any(|s| s.native_amount.is_none() && s.usd_amount.is_none()),
        EvidenceStatus::Empty => false,
    }
}

#[cfg(test)]
mod receipt_gas_tests {
    use super::*;

    #[test]
    fn parse_u128_zero_hex() {
        assert_eq!(parse_u128(Some(&Value::String("0x0".into()))), Some(0));
        assert_eq!(parse_u128(Some(&Value::String("0x00".into()))), Some(0));
        assert_eq!(parse_u128(Some(&Value::String("0X0".into()))), Some(0));
    }

    #[test]
    fn parse_receipt_uses_effective_gas_price() {
        let result = json!({
            "from": "0xAbC",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x3b9aca00"
        });
        let info = parse_receipt_gas(&result).unwrap();
        assert!((info.gas_native.unwrap() - 0.000021).abs() < 1e-12);
        assert_eq!(info.fee_payer.as_deref(), Some("0xabc"));
    }

    #[test]
    fn parse_receipt_falls_back_to_gas_price() {
        let result = json!({
            "gasUsed": "21000",
            "gasPrice": "1000000000"
        });
        let info = parse_receipt_gas(&result).unwrap();
        assert!((info.gas_native.unwrap() - 0.000021).abs() < 1e-12);
    }

    #[test]
    fn collect_unique_hashes_from_transfers_and_sales() {
        let transfers = vec![TransferEvent {
            tx_hash: "0xAAA".into(),
            token_id: "1".into(),
            from: String::new(),
            to: String::new(),
            timestamp: None,
            block_number: None,
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
        }];
        let sales = vec![SaleEvent {
            tx_hash: "0xaaa".into(),
            token_id: "1".into(),
            seller: String::new(),
            buyer: String::new(),
            timestamp: None,
            block_number: None,
            marketplace: None,
            native_amount: None,
            usd_amount: None,
            currency_symbol: None,
        }];
        let hashes = collect_unique_tx_hashes(&transfers, &sales);
        assert_eq!(hashes, vec!["0xaaa".to_owned()]);
    }
}

#[cfg(test)]
mod sales_fallback_tests {
    use super::*;

    fn sale_with_amount(native: Option<f64>) -> SaleEvent {
        SaleEvent {
            tx_hash: "0x1".into(),
            token_id: "1".into(),
            seller: "0xa".into(),
            buyer: "0xb".into(),
            timestamp: None,
            block_number: None,
            marketplace: None,
            native_amount: native,
            usd_amount: None,
            currency_symbol: Some("ETH".into()),
        }
    }

    #[test]
    fn empty_alchemy_sales_do_not_need_opensea() {
        assert!(!sales_need_opensea_fallback(&[], EvidenceStatus::Empty));
    }

    #[test]
    fn failed_or_not_requested_need_opensea() {
        assert!(sales_need_opensea_fallback(&[], EvidenceStatus::Failed));
        assert!(sales_need_opensea_fallback(&[], EvidenceStatus::NotRequested));
    }

    #[test]
    fn complete_or_truncated_need_opensea_only_when_amounts_missing() {
        assert!(sales_need_opensea_fallback(
            &[sale_with_amount(None)],
            EvidenceStatus::Complete
        ));
        assert!(sales_need_opensea_fallback(
            &[sale_with_amount(None)],
            EvidenceStatus::Truncated
        ));
        assert!(!sales_need_opensea_fallback(
            &[sale_with_amount(Some(1.0))],
            EvidenceStatus::Complete
        ));
    }
}

/// Alchemy NFT API: collection slug from first NFT metadata (or None).
pub async fn fetch_collection_slug(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
) -> Option<String> {
    let api_key = api_key?;
    let base = endpoints.alchemy_nft(chain, api_key, "getNFTsForContract")?;
    let url = format!("{base}?contractAddress={contract}&withMetadata=true&limit=1");
    let payload = client.get_json(&url, &[]).await.ok()?;
    payload
        .get("nfts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find_map(|nft| {
            nft.get("collection")
                .and_then(|c| c.get("slug"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
        })
}

/// Alchemy NFT API: whether `wallet` currently holds any NFT of `contract`.
pub async fn is_holder_of_contract(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    wallet: &str,
    contract: &str,
) -> Result<Option<bool>, String> {
    let Some(api_key) = api_key else {
        return Ok(None);
    };
    let Some(base) = endpoints.alchemy_nft(chain, api_key, "isHolderOfContract") else {
        return Err(format!("unsupported alchemy network for {chain}"));
    };
    let url = format!("{base}?wallet={wallet}&contractAddress={contract}");
    let payload = client
        .get_json(&url, &[])
        .await
        .map_err(|e| e.to_string())?;
    Ok(Some(
        payload
            .get("isHolderOfContract")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    ))
}
