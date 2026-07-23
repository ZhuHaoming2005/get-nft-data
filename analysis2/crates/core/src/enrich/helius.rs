//! Helius DAS helpers for Solana collection resolve + enrichment.
//!
//! History path: `getSignaturesForAsset` stubs → deduped `getTransaction` jsonParsed
//! decode (standard SPL ownership + native SOL balance / transfer instructions).
//! Compressed NFT / Bubblegum full parity is intentionally out of MVP scope.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use ahash::AHashMap;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::error::Analysis2Error;

use super::alchemy::FetchOutcome;
use super::controllers::solana_authorities_from_asset;
use super::http::HttpClient;
use super::types::{
    EvidenceStatus, HolderRecord, SaleEvent, TransferEvent, ValueFlowEdge, ValueFlowKind,
};

const DEFAULT_HELIUS_RPC: &str = "https://mainnet.helius-rpc.com/";
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

/// One Solana asset from `getAssetsByGroup`.
#[derive(Clone, Debug, Default)]
pub struct SolanaAsset {
    pub mint: String,
    pub owner: Option<String>,
    pub compressed: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SolanaAssetSnapshot {
    pub assets: Vec<SolanaAsset>,
    pub total: Option<usize>,
    pub truncated: bool,
    /// Collection updateAuthority (+ verified creators) extracted while paging assets.
    pub authority: Vec<String>,
}

/// Resolve on-chain collection address for a mint via `getAsset`.
pub async fn resolve_collection_address(
    client: &HttpClient,
    rpc_url: &str,
    api_key: &str,
    mint: &str,
) -> Result<Option<String>, Analysis2Error> {
    let mut url = rpc_url.trim_end_matches('/').to_owned();
    if !url.contains('?') {
        url.push_str("?api-key=");
        url.push_str(api_key);
    } else if !url.contains("api-key=") {
        url.push_str("&api-key=");
        url.push_str(api_key);
    }
    let body = json!({
        "jsonrpc": "2.0",
        "id": format!("seed-collection-{mint}"),
        "method": "getAsset",
        "params": {"id": mint}
    });
    let payload = client.post_json(&url, &[], &body).await?;
    if let Some(error) = payload.get("error") {
        return Err(Analysis2Error::http(format!(
            "Helius getAsset failed for {mint}: {error}"
        )));
    }
    let Some(result) = payload.get("result") else {
        return Err(Analysis2Error::http(format!(
            "Helius getAsset omitted result for {mint}"
        )));
    };
    Ok(parse_collection_address(result))
}

/// Extract `grouping.group_value` where `group_key == "collection"`.
pub fn parse_collection_address(asset: &Value) -> Option<String> {
    let grouping = asset.get("grouping")?.as_array()?;
    for group in grouping {
        let key = group
            .get("group_key")
            .or_else(|| group.get("groupKey"))
            .and_then(Value::as_str)?;
        if key != "collection" {
            continue;
        }
        let value = group
            .get("group_value")
            .or_else(|| group.get("groupValue"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())?;
        if valid_solana_address(value) {
            return Some(value.to_owned());
        }
    }
    None
}

fn valid_solana_address(value: &str) -> bool {
    base58_decoded_len(value) == Some(32)
}

fn base58_decoded_len(value: &str) -> Option<usize> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if value.is_empty() {
        return None;
    }
    let leading_zeroes = value.bytes().take_while(|b| *b == b'1').count();
    let mut decoded = vec![0_u8];
    for byte in value.bytes() {
        let digit = ALPHABET.iter().position(|c| *c == byte)? as u16;
        let mut carry = digit;
        for part in &mut decoded {
            let value = u16::from(*part) * 58 + carry;
            *part = value as u8;
            carry = value >> 8;
        }
        while carry > 0 {
            decoded.push(carry as u8);
            carry >>= 8;
        }
    }
    while decoded.last() == Some(&0) && decoded.len() > 1 {
        decoded.pop();
    }
    let body_len = if decoded == [0] { 0 } else { decoded.len() };
    Some(leading_zeroes + body_len)
}

pub fn default_rpc_url() -> &'static str {
    DEFAULT_HELIUS_RPC
}

fn with_api_key(rpc_url: &str, api_key: &str) -> String {
    let mut url = rpc_url.trim_end_matches('/').to_owned();
    if !url.contains('?') {
        url.push_str("?api-key=");
        url.push_str(api_key);
    } else if !url.contains("api-key=") {
        url.push_str("&api-key=");
        url.push_str(api_key);
    }
    url
}

/// Paginate `getAssetsByGroup` for a collection address.
pub async fn fetch_collection_assets(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    collection: &str,
    max_assets: usize,
) -> FetchOutcome<SolanaAssetSnapshot> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("helius_assets");
    };
    let url = with_api_key(rpc_url, api_key);
    let page_size = 100usize.min(max_assets.max(1));
    let mut snapshot = SolanaAssetSnapshot::default();
    let mut page = 1usize;
    let mut seen_mints = std::collections::BTreeSet::new();

    loop {
        if snapshot.assets.len() >= max_assets {
            snapshot.truncated = true;
            break;
        }
        let limit = page_size.min(max_assets.saturating_sub(snapshot.assets.len()).max(1));
        let body = json!({
            "jsonrpc": "2.0",
            "id": format!("assets-{page}"),
            "method": "getAssetsByGroup",
            "params": {
                "groupKey": "collection",
                "groupValue": collection,
                "page": page,
                "limit": limit,
                "options": {
                    "showUnverifiedCollections": false,
                    "showCollectionMetadata": true,
                    "showGrandTotal": true
                }
            }
        });
        let payload = match client.post_json(&url, &[], &body).await {
            Ok(v) => v,
            Err(e) => {
                if snapshot.assets.is_empty() {
                    return FetchOutcome::failed("helius", "helius_assets", e);
                }
                snapshot.truncated = true;
                break;
            }
        };
        if let Some(error) = payload.get("error") {
            if snapshot.assets.is_empty() {
                return FetchOutcome::failed("helius", "helius_assets", error.to_string());
            }
            snapshot.truncated = true;
            break;
        }
        let Some(result) = payload.get("result") else {
            if snapshot.assets.is_empty() {
                return FetchOutcome::failed(
                    "helius",
                    "helius_assets",
                    "getAssetsByGroup omitted result",
                );
            }
            snapshot.truncated = true;
            break;
        };
        if let Some(total) = result.get("total").and_then(Value::as_u64) {
            snapshot.total = Some(total as usize);
        }
        let items = result
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if items.is_empty() {
            break;
        }
        let before = snapshot.assets.len();
        for item in &items {
            if snapshot.authority.is_empty() {
                let authorities = solana_authorities_from_asset(item, result, collection);
                if !authorities.is_empty() {
                    snapshot.authority = authorities;
                }
            }
            if let Some(asset) = parse_asset(item) {
                if seen_mints.insert(asset.mint.clone()) {
                    snapshot.assets.push(asset);
                    if snapshot.assets.len() >= max_assets {
                        snapshot.truncated = true;
                        break;
                    }
                }
            }
        }
        if snapshot.assets.len() == before {
            break;
        }
        page += 1;
        if let Some(total) = snapshot.total {
            if snapshot.assets.len() >= total {
                break;
            }
        }
    }

    let count = snapshot.assets.len();
    let truncated = snapshot.truncated
        || snapshot
            .total
            .is_some_and(|total| total > snapshot.assets.len());
    snapshot.truncated = truncated;
    FetchOutcome::ok(snapshot, count, truncated, "helius", "helius_assets")
}

/// Bounded history via `getSignaturesForAsset` → transfer/sale stubs.
///
/// Stubs alone are never Complete: callers must run [`decode_and_attach_transactions`]
/// and recompute field quality from decode stats.
pub async fn fetch_asset_histories(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    assets: &[SolanaAsset],
    max_assets: usize,
    max_sigs_per_asset: usize,
) -> FetchOutcome<(Vec<TransferEvent>, Vec<SaleEvent>)> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("helius_histories");
    };
    if assets.is_empty() {
        return FetchOutcome::ok(
            (Vec::new(), Vec::new()),
            0,
            false,
            "helius",
            "helius_histories",
        );
    }
    let url = with_api_key(rpc_url, api_key);
    let mut transfers = Vec::new();
    let mut sales = Vec::new();
    let mut truncated = assets.len() > max_assets;
    let mut any_ok = false;
    let mut failures = 0usize;

    for asset in assets.iter().take(max_assets.max(1)) {
        let body = json!({
            "jsonrpc": "2.0",
            "id": format!("sigs-{}", asset.mint),
            "method": "getSignaturesForAsset",
            "params": {
                "id": asset.mint,
                "page": 1,
                "limit": max_sigs_per_asset.max(1)
            }
        });
        let payload = match client.post_json(&url, &[], &body).await {
            Ok(v) => v,
            Err(_) => {
                failures += 1;
                continue;
            }
        };
        if payload.get("error").is_some() {
            failures += 1;
            continue;
        }
        any_ok = true;
        let items = payload
            .get("result")
            .and_then(|r| r.get("items"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if items.len() >= max_sigs_per_asset {
            truncated = true;
        }
        for item in items {
            let (sig, event_type) = parse_signature_item(&item);
            if sig.is_empty() {
                continue;
            }
            let kind = event_type.to_ascii_lowercase();
            if kind.contains("sale") || kind.contains("buy") || kind.contains("list") {
                sales.push(SaleEvent {
                    tx_hash: sig,
                    token_id: asset.mint.clone(),
                    seller: String::new(),
                    buyer: String::new(),
                    timestamp: None,
                    block_number: None,
                    marketplace: Some("helius".into()),
                    native_amount: None,
                    usd_amount: None,
                    currency_symbol: Some("SOL".into()),
                });
            } else {
                transfers.push(TransferEvent {
                    tx_hash: sig,
                    token_id: asset.mint.clone(),
                    from: String::new(),
                    to: asset.owner.clone().unwrap_or_default(),
                    timestamp: None,
                    block_number: None,
                    is_mint: kind.contains("mint") || kind.contains("create"),
                    gas_native: None,
                    fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
                });
            }
        }
    }

    if !any_ok && failures > 0 {
        return FetchOutcome::failed(
            "helius",
            "helius_histories",
            format!("{failures} signature discovery failures"),
        );
    }
    let count = transfers.len() + sales.len();
    // Only mark truncated for discovery caps; decode quality is decided later.
    FetchOutcome::ok(
        (transfers, sales),
        count,
        truncated,
        "helius",
        "helius_histories",
    )
}

/// Per-signature decode bookkeeping used for quality upgrades.
#[derive(Clone, Debug, Default)]
pub struct DecodeStats {
    pub requested: usize,
    pub fetched_ok: usize,
    pub fetch_failed: usize,
    pub null_result: usize,
    pub transfers_complete: usize,
    pub transfers_total: usize,
    pub sales_complete: usize,
    pub sales_total: usize,
}

impl DecodeStats {
    pub fn all_fetch_failed(&self) -> bool {
        self.requested > 0 && self.fetch_failed == self.requested
    }

    pub fn any_fetch_ok(&self) -> bool {
        self.fetched_ok > 0
    }

    pub fn transfers_all_complete(&self) -> bool {
        self.transfers_total > 0 && self.transfers_complete == self.transfers_total
    }

    pub fn sales_all_complete(&self) -> bool {
        self.sales_total > 0 && self.sales_complete == self.sales_total
    }
}

/// Dedupe signatures → `getTransaction` jsonParsed; attach from/to/timestamp/fee;
/// extract SOL [`ValueFlowEdge`]s involving fee payers / controllers.
///
/// Returns `(gas_outcome, value_flows_outcome, stats)`.
pub async fn decode_and_attach_transactions(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    transfers: &mut [TransferEvent],
    sales: &mut [SaleEvent],
    controllers: &[String],
    concurrency: usize,
) -> (
    FetchOutcome<()>,
    FetchOutcome<Vec<ValueFlowEdge>>,
    DecodeStats,
) {
    let mut stats = DecodeStats {
        transfers_total: transfers.len(),
        sales_total: sales.len(),
        ..DecodeStats::default()
    };

    let Some(api_key) = api_key else {
        return (
            FetchOutcome::skipped("helius_get_transaction"),
            FetchOutcome::skipped("helius_value_flows"),
            stats,
        );
    };

    let mut sig_mints: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for t in transfers.iter() {
        let sig = t.tx_hash.trim();
        if !sig.is_empty() {
            sig_mints
                .entry(sig.to_owned())
                .or_default()
                .insert(t.token_id.clone());
        }
    }
    for s in sales.iter() {
        let sig = s.tx_hash.trim();
        if !sig.is_empty() {
            sig_mints
                .entry(sig.to_owned())
                .or_default()
                .insert(s.token_id.clone());
        }
    }

    if sig_mints.is_empty() {
        return (
            FetchOutcome::ok((), 0, false, "helius", "helius_get_transaction"),
            FetchOutcome::ok(
                Vec::new(),
                0,
                false,
                "helius",
                "helius_value_flows",
            ),
            stats,
        );
    }

    let url = with_api_key(rpc_url, api_key);
    let signatures: Vec<String> = sig_mints.keys().cloned().collect();
    stats.requested = signatures.len();

    let sem = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut handles = Vec::with_capacity(signatures.len());
    for (idx, signature) in signatures.iter().cloned().enumerate() {
        let client = client.clone();
        let url = url.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.ok();
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("tx-{idx}"),
                "method": "getTransaction",
                "params": [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "commitment": "finalized",
                        "maxSupportedTransactionVersion": 0
                    }
                ]
            });
            match client.post_json(&url, &[], &body).await {
                Ok(payload) => {
                    if let Some(error) = payload.get("error") {
                        return Err((signature, error.to_string()));
                    }
                    let result = payload.get("result").cloned().unwrap_or(Value::Null);
                    if result.is_null() {
                        return Ok((signature, None));
                    }
                    Ok((signature, Some(result)))
                }
                Err(e) => Err((signature, e.to_string())),
            }
        }));
    }

    let mut decoded: AHashMap<String, DecodedTx> = AHashMap::new();
    let mut failures = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok((sig, Some(result)))) => {
                stats.fetched_ok += 1;
                let mints = sig_mints.get(&sig).cloned().unwrap_or_default();
                decoded.insert(sig.clone(), parse_decoded_tx(&sig, &result, &mints));
            }
            Ok(Ok((_sig, None))) => {
                stats.null_result += 1;
            }
            Ok(Err((sig, err))) => {
                stats.fetch_failed += 1;
                failures.push(format!("{sig}: {err}"));
            }
            Err(e) => {
                stats.fetch_failed += 1;
                failures.push(format!("getTransaction join failed: {e}"));
            }
        }
    }

    let mut operators: BTreeSet<String> = BTreeSet::new();
    for c in controllers {
        insert_sol_addr(&mut operators, c);
    }

    let mut value_flows = Vec::new();
    let mut seen_edges = HashSet::new();

    for tx in decoded.values() {
        if let Some(payer) = &tx.fee_payer {
            insert_sol_addr(&mut operators, payer);
        }
    }

    for transfer in transfers.iter_mut() {
        let Some(tx) = decoded.get(transfer.tx_hash.trim()) else {
            continue;
        };
        apply_transfer_decode(transfer, tx);
    }
    for sale in sales.iter_mut() {
        let Some(tx) = decoded.get(sale.tx_hash.trim()) else {
            continue;
        };
        apply_sale_decode(sale, tx);
    }

    // Recompute operators after fee_payer attach (mint fee payers).
    for transfer in transfers.iter() {
        if transfer.is_mint {
            if let Some(payer) = &transfer.fee_payer {
                insert_sol_addr(&mut operators, payer);
            }
            insert_sol_addr(&mut operators, &transfer.from);
        }
    }

    for tx in decoded.values() {
        for mv in &tx.native_moves {
            if let Some(edge) = classify_sol_edge(tx, mv, &operators) {
                let key = (
                    edge.tx_hash.clone(),
                    edge.from.clone(),
                    edge.to.clone(),
                    edge.kind,
                );
                if seen_edges.insert(key) {
                    value_flows.push(edge);
                }
            }
        }
    }

    stats.transfers_complete = transfers
        .iter()
        .filter(|t| {
            let Some(tx) = decoded.get(t.tx_hash.trim()) else {
                return false;
            };
            let had_owner_change = tx.owner_changes.contains_key(&t.token_id);
            transfer_fields_complete(t, had_owner_change)
        })
        .count();
    stats.sales_complete = sales.iter().filter(|s| sale_fields_complete(s)).count();

    let gas_outcome = gas_outcome_from_stats(&stats, transfers, &failures);
    let vf_outcome = value_flow_outcome(value_flows, &stats, &failures);

    (gas_outcome, vf_outcome, stats)
}

/// Field quality after decode. Signature-only stubs never become Complete.
pub fn field_status_after_decode(
    empty: bool,
    page_truncated: bool,
    all_complete: bool,
    stats: &DecodeStats,
) -> EvidenceStatus {
    if empty && !page_truncated {
        return EvidenceStatus::Empty;
    }
    if stats.all_fetch_failed() {
        return EvidenceStatus::Failed;
    }
    if page_truncated || !all_complete || !stats.any_fetch_ok() {
        return EvidenceStatus::Truncated;
    }
    if empty {
        EvidenceStatus::Empty
    } else {
        EvidenceStatus::Complete
    }
}

/// Combined histories status across transfers + sales.
pub fn histories_status_after_decode(
    transfers_empty: bool,
    sales_empty: bool,
    page_truncated: bool,
    stats: &DecodeStats,
) -> EvidenceStatus {
    let empty = transfers_empty && sales_empty;
    let all_complete = (transfers_empty || stats.transfers_all_complete())
        && (sales_empty || stats.sales_all_complete());
    field_status_after_decode(empty, page_truncated, all_complete, stats)
}

fn gas_outcome_from_stats(
    stats: &DecodeStats,
    transfers: &[TransferEvent],
    failures: &[String],
) -> FetchOutcome<()> {
    if stats.requested == 0 {
        return FetchOutcome::ok((), 0, false, "helius", "helius_get_transaction");
    }
    if stats.all_fetch_failed() {
        let detail = if failures.is_empty() {
            "all getTransaction fetches failed".into()
        } else {
            failures.join("; ")
        };
        return FetchOutcome::failed("helius", "helius_get_transaction", detail);
    }
    let with_fee = transfers.iter().filter(|t| t.gas_native.is_some()).count();
    let fee_complete = !transfers.is_empty() && with_fee == transfers.len();
    let truncated = !fee_complete
        || stats.fetch_failed > 0
        || stats.null_result > 0
        || stats.fetched_ok < stats.requested;
    let count = if with_fee > 0 || stats.fetched_ok > 0 {
        with_fee.max(1)
    } else {
        0
    };
    let mut outcome = FetchOutcome::ok((), count, truncated, "helius", "helius_get_transaction");
    // Prefer gas Complete when every transfer has fee and every sig fetched.
    if fee_complete && stats.fetched_ok == stats.requested && stats.fetch_failed == 0 {
        outcome.status = EvidenceStatus::Complete;
        if let Some(obs) = outcome.observation.as_mut() {
            obs.status = EvidenceStatus::Complete;
        }
        outcome.truncated = false;
    }
    if truncated && !failures.is_empty() {
        outcome.failure = Some(format!(
            "helius_get_transaction: partial failures ({}/{}): {}",
            failures.len(),
            stats.requested,
            failures.iter().take(3).cloned().collect::<Vec<_>>().join("; ")
        ));
    }
    outcome
}

fn value_flow_outcome(
    edges: Vec<ValueFlowEdge>,
    stats: &DecodeStats,
    failures: &[String],
) -> FetchOutcome<Vec<ValueFlowEdge>> {
    if stats.requested == 0 {
        return FetchOutcome::ok(edges, 0, false, "helius", "helius_value_flows");
    }
    if stats.all_fetch_failed() {
        let detail = if failures.is_empty() {
            "all getTransaction fetches failed".into()
        } else {
            failures.join("; ")
        };
        return FetchOutcome::failed("helius", "helius_value_flows", detail);
    }
    let truncated = stats.fetch_failed > 0
        || stats.null_result > 0
        || stats.fetched_ok < stats.requested;
    let count = edges.len();
    FetchOutcome::ok(edges, count, truncated, "helius", "helius_value_flows")
}

#[derive(Clone, Debug, Default)]
struct NativeSolMove {
    from: String,
    to: String,
    amount_sol: f64,
}

#[derive(Clone, Debug, Default)]
struct DecodedTx {
    signature: String,
    fee_payer: Option<String>,
    fee_sol: Option<f64>,
    timestamp: Option<i64>,
    slot: Option<u64>,
    /// mint → (from, to, is_mint)
    owner_changes: HashMap<String, (Option<String>, String, bool)>,
    native_moves: Vec<NativeSolMove>,
    failed: bool,
}

fn parse_decoded_tx(signature: &str, result: &Value, mints: &BTreeSet<String>) -> DecodedTx {
    let mut tx = DecodedTx {
        signature: signature.to_owned(),
        timestamp: result.get("blockTime").and_then(Value::as_i64),
        slot: result.get("slot").and_then(Value::as_u64),
        ..DecodedTx::default()
    };
    let meta = result.get("meta").unwrap_or(&Value::Null);
    if meta.get("err").is_some_and(|e| !e.is_null()) {
        tx.failed = true;
        return tx;
    }
    let transaction = result.get("transaction").unwrap_or(&Value::Null);
    let message = transaction.get("message").unwrap_or(&Value::Null);
    let accounts: Vec<String> = message
        .get("accountKeys")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(account_key)
        .collect();
    tx.fee_payer = accounts.first().cloned().filter(|s| !s.is_empty());
    if let Some(fee) = meta.get("fee").and_then(Value::as_i64) {
        if fee > 0 {
            tx.fee_sol = Some(fee as f64 / LAMPORTS_PER_SOL);
        } else if fee == 0 {
            tx.fee_sol = Some(0.0);
        }
    }
    for mint in mints {
        if let Some(change) = token_owner_change(meta, mint) {
            tx.owner_changes.insert(mint.clone(), change);
        }
    }

    tx.native_moves = parse_native_sol_moves(meta, message, &accounts, tx.fee_sol.unwrap_or(0.0));
    tx
}

fn apply_transfer_decode(transfer: &mut TransferEvent, tx: &DecodedTx) {
    if tx.failed {
        return;
    }
    if transfer.timestamp.is_none() {
        transfer.timestamp = tx.timestamp;
    }
    if transfer.block_number.is_none() {
        transfer.block_number = tx.slot;
    }
    if transfer.gas_native.is_none() {
        transfer.gas_native = tx.fee_sol;
    }
    if transfer.fee_payer.is_none() {
        transfer.fee_payer = tx.fee_payer.clone();
    }
    if let Some((from, to, is_mint)) = tx.owner_changes.get(&transfer.token_id) {
        transfer.is_mint = *is_mint || transfer.is_mint;
        if let Some(f) = from {
            if !f.is_empty() {
                transfer.from = f.clone();
            }
        } else if *is_mint {
            transfer.from.clear();
        }
        if !to.is_empty() {
            transfer.to = to.clone();
        }
    }
}

fn apply_sale_decode(sale: &mut SaleEvent, tx: &DecodedTx) {
    if tx.failed {
        return;
    }
    if sale.timestamp.is_none() {
        sale.timestamp = tx.timestamp;
    }
    if sale.block_number.is_none() {
        sale.block_number = tx.slot;
    }
    let Some((from, to, is_mint)) = tx.owner_changes.get(&sale.token_id) else {
        return;
    };
    if *is_mint {
        return;
    }
    let seller = from.clone().unwrap_or_default();
    let buyer = to.clone();
    if seller.is_empty() || buyer.is_empty() || seller == buyer {
        return;
    }
    sale.seller = seller.clone();
    sale.buyer = buyer.clone();

    // Optional price: native SOL from buyer → seller.
    if sale.native_amount.is_none() {
        let paid: f64 = tx
            .native_moves
            .iter()
            .filter(|m| m.from == buyer && m.to == seller && m.amount_sol > 0.0)
            .map(|m| m.amount_sol)
            .sum();
        if paid > 0.0 {
            sale.native_amount = Some(paid);
            if sale.currency_symbol.is_none() {
                sale.currency_symbol = Some("SOL".into());
            }
        }
    }
}

fn transfer_fields_complete(t: &TransferEvent, had_owner_change: bool) -> bool {
    // Mint/create must not be Complete without a successful ownership/token-balance
    // decode. Missing pre/postTokenBalances (Bubblegum/compressed) → Truncated.
    if !had_owner_change {
        return false;
    }
    let to_ok = !t.to.trim().is_empty();
    let from_ok = t.is_mint || !t.from.trim().is_empty();
    to_ok && from_ok && t.timestamp.is_some() && t.gas_native.is_some()
}

fn sale_fields_complete(s: &SaleEvent) -> bool {
    !s.seller.trim().is_empty() && !s.buyer.trim().is_empty() && s.timestamp.is_some()
}

fn classify_sol_edge(
    tx: &DecodedTx,
    mv: &NativeSolMove,
    operators: &BTreeSet<String>,
) -> Option<ValueFlowEdge> {
    if mv.from.is_empty() || mv.to.is_empty() || mv.from == mv.to || mv.amount_sol <= 0.0 {
        return None;
    }
    let from_op = operators.contains(&mv.from);
    let to_op = operators.contains(&mv.to);
    if !from_op && !to_op {
        return None;
    }
    let kind = match (from_op, to_op) {
        (false, true) => ValueFlowKind::Funding,
        (true, false) => ValueFlowKind::Withdrawal,
        (true, true) => ValueFlowKind::RevenueBackflow,
        (false, false) => return None,
    };
    Some(ValueFlowEdge {
        tx_hash: tx.signature.clone(),
        from: mv.from.clone(),
        to: mv.to.clone(),
        kind,
        native_amount: Some(mv.amount_sol),
        usd_amount: None,
        timestamp: tx.timestamp,
    })
}

fn insert_sol_addr(set: &mut BTreeSet<String>, raw: &str) {
    let addr = raw.trim();
    if addr.is_empty() {
        return;
    }
    set.insert(addr.to_owned());
}

fn account_key(value: &Value) -> Option<String> {
    value
        .as_str()
        .or_else(|| value.get("pubkey").and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

#[derive(Clone, Debug, Default)]
struct TokenBalanceEntry {
    owner: String,
    amount: i128,
}

fn token_balances(rows: Option<&Value>, mint_address: &str) -> HashMap<u64, TokenBalanceEntry> {
    rows.and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| row.get("mint").and_then(Value::as_str) == Some(mint_address))
        .filter_map(|row| {
            let account_index = row.get("accountIndex").and_then(Value::as_u64)?;
            let amount = row
                .get("uiTokenAmount")
                .and_then(|amount| amount.get("amount"))
                .and_then(Value::as_str)
                .and_then(|amount| amount.parse::<i128>().ok())?;
            Some((
                account_index,
                TokenBalanceEntry {
                    owner: row
                        .get("owner")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string(),
                    amount,
                },
            ))
        })
        .collect()
}

fn token_owner_change(meta: &Value, mint_address: &str) -> Option<(Option<String>, String, bool)> {
    let before = token_balances(meta.get("preTokenBalances"), mint_address);
    let after = token_balances(meta.get("postTokenBalances"), mint_address);
    let total_before = before.values().map(|entry| entry.amount).sum::<i128>();
    let mut indexes = before
        .keys()
        .chain(after.keys())
        .copied()
        .collect::<HashSet<_>>();
    let mut source: Option<(i128, String)> = None;
    let mut destination: Option<(i128, String)> = None;
    for index in indexes.drain() {
        let pre = before.get(&index).cloned().unwrap_or_default();
        let post = after.get(&index).cloned().unwrap_or_default();
        let delta = post.amount - pre.amount;
        if delta < 0 && !pre.owner.is_empty() {
            if source.as_ref().is_none_or(|(amount, _)| -delta > *amount) {
                source = Some((-delta, pre.owner));
            }
        } else if delta > 0
            && !post.owner.is_empty()
            && destination
                .as_ref()
                .is_none_or(|(amount, _)| delta > *amount)
        {
            destination = Some((delta, post.owner));
        } else if delta == 0
            && pre.amount > 0
            && !pre.owner.is_empty()
            && !post.owner.is_empty()
            && pre.owner != post.owner
        {
            // Same token account index, ownership reassigned without amount delta.
            source = Some((pre.amount, pre.owner));
            destination = Some((post.amount, post.owner));
        }
    }
    let (_, to) = destination?;
    let is_mint = total_before == 0;
    let from = (!is_mint).then(|| source.map(|(_, owner)| owner)).flatten();
    if !is_mint && from.is_none() {
        return None;
    }
    Some((from, to, is_mint))
}

/// Native SOL movements: prefer parsed system transfers; fall back to pre/post balance deltas.
fn parse_native_sol_moves(
    meta: &Value,
    message: &Value,
    accounts: &[String],
    fee_sol: f64,
) -> Vec<NativeSolMove> {
    let mut moves = Vec::new();
    let mut instructions = message
        .get("instructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for group in meta
        .get("innerInstructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        instructions.extend(
            group
                .get("instructions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        );
    }
    for instruction in &instructions {
        let Some(parsed) = instruction.get("parsed") else {
            continue;
        };
        let instruction_type = parsed.get("type").and_then(Value::as_str).unwrap_or("");
        if !matches!(instruction_type, "transfer" | "transferWithSeed") {
            continue;
        }
        let info = parsed.get("info").unwrap_or(&Value::Null);
        let Some(lamports) = info.get("lamports").and_then(Value::as_u64) else {
            continue;
        };
        if lamports == 0 {
            continue;
        }
        let from = info
            .get("source")
            .or_else(|| info.get("from"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();
        let to = info
            .get("destination")
            .or_else(|| info.get("to"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();
        if from.is_empty() || to.is_empty() {
            continue;
        }
        moves.push(NativeSolMove {
            from,
            to,
            amount_sol: lamports as f64 / LAMPORTS_PER_SOL,
        });
    }
    if !moves.is_empty() {
        return moves;
    }

    // Fallback: balance deltas excluding fee payer's fee-only debit.
    let Some(pre) = meta.get("preBalances").and_then(Value::as_array) else {
        return moves;
    };
    let Some(post) = meta.get("postBalances").and_then(Value::as_array) else {
        return moves;
    };
    if pre.len() != post.len() || pre.len() != accounts.len() {
        return moves;
    }
    let mut deltas: Vec<(usize, i128)> = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        if account.is_empty() {
            continue;
        }
        let before = pre[i].as_u64().unwrap_or_default() as i128;
        let after = post[i].as_u64().unwrap_or_default() as i128;
        let mut delta = after - before;
        if i == 0 {
            // Remove fee debit so remaining delta is transferable SOL.
            delta += (fee_sol * LAMPORTS_PER_SOL).round() as i128;
        }
        if delta != 0 {
            deltas.push((i, delta));
        }
    }
    let sinks: Vec<_> = deltas.iter().filter(|(_, d)| *d > 0).copied().collect();
    let sources: Vec<_> = deltas.iter().filter(|(_, d)| *d < 0).copied().collect();
    if sources.len() == 1 && sinks.len() == 1 {
        let (si, sd) = sources[0];
        let (di, dd) = sinks[0];
        let amount = (-sd).min(dd) as f64 / LAMPORTS_PER_SOL;
        if amount > 0.0 {
            moves.push(NativeSolMove {
                from: accounts[si].clone(),
                to: accounts[di].clone(),
                amount_sol: amount,
            });
        }
    }
    moves
}

pub fn holders_from_assets(assets: &[SolanaAsset]) -> Vec<HolderRecord> {
    assets
        .iter()
        .filter_map(|asset| {
            asset.owner.as_ref().map(|owner| HolderRecord {
                token_id: asset.mint.clone(),
                owner: owner.clone(),
                balance: Some(1),
            })
        })
        .collect()
}

fn parse_asset(item: &Value) -> Option<SolanaAsset> {
    let mint = item.get("id").and_then(Value::as_str)?.trim().to_owned();
    if mint.is_empty() {
        return None;
    }
    let owner = item
        .get("ownership")
        .and_then(|o| o.get("owner"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let compressed = item
        .get("compression")
        .and_then(|c| c.get("compressed"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Some(SolanaAsset {
        mint,
        owner,
        compressed,
    })
}

fn parse_signature_item(item: &Value) -> (String, String) {
    if let Some(arr) = item.as_array() {
        let sig = arr
            .first()
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let event = arr
            .get(1)
            .and_then(Value::as_str)
            .unwrap_or("transfer")
            .to_owned();
        return (sig, event);
    }
    let sig = item
        .get("signature")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let event = item
        .get("type")
        .or_else(|| item.get("event"))
        .and_then(Value::as_str)
        .unwrap_or("transfer")
        .to_owned();
    (sig, event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::enrich::types::{status_from_count, EvidenceStatus};
    use std::collections::BTreeSet;

    #[test]
    fn parses_collection_grouping() {
        let asset = json!({
            "grouping": [{
                "group_key": "collection",
                "group_value": "So11111111111111111111111111111111111111112"
            }]
        });
        assert_eq!(
            parse_collection_address(&asset).as_deref(),
            Some("So11111111111111111111111111111111111111112")
        );
    }

    #[test]
    fn returns_none_without_collection_group() {
        let asset = json!({
            "grouping": [{"group_key": "other", "group_value": "x"}]
        });
        assert_eq!(parse_collection_address(&asset), None);
    }

    #[test]
    fn parses_asset_owner_and_compression() {
        let item = json!({
            "id": "Mint111111111111111111111111111111111111111",
            "ownership": {"owner": "Owner1111111111111111111111111111111111111"},
            "compression": {"compressed": true}
        });
        let asset = parse_asset(&item).unwrap();
        assert!(asset.compressed);
        assert_eq!(
            asset.owner.as_deref(),
            Some("Owner1111111111111111111111111111111111111")
        );
    }

    #[test]
    fn status_from_count_distinguishes_empty_complete_truncated() {
        assert_eq!(status_from_count(0, false), EvidenceStatus::Empty);
        assert_eq!(status_from_count(1, false), EvidenceStatus::Complete);
        assert_eq!(status_from_count(1, true), EvidenceStatus::Truncated);
    }

    #[test]
    fn parses_standard_transfer_owner_change_and_fee() {
        let mint = "MintDecode1111111111111111111111111111111";
        let result = json!({
            "slot": 42,
            "blockTime": 1_700_000_000,
            "transaction": {
                "message": {
                    "accountKeys": [
                        {"pubkey": "FeePayer111111111111111111111111111111111", "signer": true},
                        {"pubkey": "Other111111111111111111111111111111111111", "signer": false}
                    ],
                    "instructions": [{
                        "parsed": {
                            "type": "transfer",
                            "info": {
                                "source": "BuyerFund1111111111111111111111111111111",
                                "destination": "FeePayer111111111111111111111111111111111",
                                "lamports": 2_000_000_000u64
                            }
                        }
                    }]
                }
            },
            "meta": {
                "err": null,
                "fee": 5000,
                "preTokenBalances": [{
                    "accountIndex": 1,
                    "mint": mint,
                    "owner": "Seller11111111111111111111111111111111111",
                    "uiTokenAmount": {"amount": "1", "decimals": 0}
                }],
                "postTokenBalances": [{
                    "accountIndex": 1,
                    "mint": mint,
                    "owner": "Buyer111111111111111111111111111111111111",
                    "uiTokenAmount": {"amount": "1", "decimals": 0}
                }]
            }
        });
        let mints = BTreeSet::from([mint.to_owned()]);
        let tx = parse_decoded_tx("SigDecode11111111111111111111111111111111", &result, &mints);
        assert_eq!(tx.fee_sol, Some(5000.0 / LAMPORTS_PER_SOL));
        assert_eq!(
            tx.fee_payer.as_deref(),
            Some("FeePayer111111111111111111111111111111111")
        );
        assert_eq!(tx.timestamp, Some(1_700_000_000));
        let (from, to, is_mint) = tx.owner_changes.get(mint).unwrap();
        assert_eq!(from.as_deref(), Some("Seller11111111111111111111111111111111111"));
        assert_eq!(to, "Buyer111111111111111111111111111111111111");
        assert!(!is_mint);
        assert_eq!(tx.native_moves.len(), 1);
        assert!((tx.native_moves[0].amount_sol - 2.0).abs() < 1e-12);
    }

    #[test]
    fn mint_without_token_balances_is_not_complete() {
        // Bubblegum/compressed-style: fee + timestamp present, no pre/postTokenBalances.
        let mint = "MintNoBal11111111111111111111111111111111";
        let result = json!({
            "slot": 7,
            "blockTime": 1_700_000_050i64,
            "transaction": {
                "message": {
                    "accountKeys": [
                        {"pubkey": "FeePayerMint11111111111111111111111111111", "signer": true}
                    ],
                    "instructions": []
                }
            },
            "meta": {
                "err": null,
                "fee": 5000
            }
        });
        let mints = BTreeSet::from([mint.to_owned()]);
        let tx = parse_decoded_tx("SigMintNoBal1111111111111111111111111111", &result, &mints);
        assert!(tx.owner_changes.is_empty());
        assert_eq!(tx.fee_sol, Some(5000.0 / LAMPORTS_PER_SOL));
        assert_eq!(tx.timestamp, Some(1_700_000_050));

        let mut transfer = TransferEvent {
            tx_hash: "SigMintNoBal1111111111111111111111111111".into(),
            token_id: mint.to_owned(),
            from: String::new(),
            // Stub may carry current asset owner as `to`.
            to: "OwnerStubMint111111111111111111111111111".into(),
            timestamp: None,
            block_number: None,
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
        };
        apply_transfer_decode(&mut transfer, &tx);
        assert!(transfer.timestamp.is_some());
        assert!(transfer.gas_native.is_some());
        let had_owner_change = tx.owner_changes.contains_key(mint);
        assert!(!had_owner_change);
        assert!(
            !transfer_fields_complete(&transfer, had_owner_change),
            "mint with fee/timestamp but no owner_change must not be Complete"
        );
    }

    #[test]
    fn field_status_stubs_without_fetch_stay_truncated() {
        let stats = DecodeStats {
            requested: 2,
            fetched_ok: 0,
            null_result: 2,
            transfers_total: 1,
            transfers_complete: 0,
            ..DecodeStats::default()
        };
        assert_eq!(
            field_status_after_decode(false, false, false, &stats),
            EvidenceStatus::Truncated
        );
    }

    #[test]
    fn field_status_all_fetch_failed() {
        let stats = DecodeStats {
            requested: 1,
            fetch_failed: 1,
            transfers_total: 1,
            ..DecodeStats::default()
        };
        assert_eq!(
            field_status_after_decode(false, false, false, &stats),
            EvidenceStatus::Failed
        );
    }

    #[test]
    fn field_status_complete_when_decoded() {
        let stats = DecodeStats {
            requested: 1,
            fetched_ok: 1,
            transfers_total: 1,
            transfers_complete: 1,
            ..DecodeStats::default()
        };
        assert_eq!(
            field_status_after_decode(false, false, true, &stats),
            EvidenceStatus::Complete
        );
    }
}
