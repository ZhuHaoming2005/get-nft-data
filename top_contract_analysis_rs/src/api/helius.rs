use futures::{stream, StreamExt};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::error::AppError;
use crate::models::{
    EthTransferRecord, NftSaleRecord, SeedNft, TransactionReceiptRecord, TransferRecord,
    ZERO_ADDRESS,
};

use super::AsyncApiClient;

const BUBBLEGUM_PROGRAM_ID: &str = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY";
const BUBBLEGUM_TRANSFER_V1_DISCRIMINATOR: [u8; 8] = [163, 52, 200, 231, 140, 3, 69, 186];
const BUBBLEGUM_TRANSFER_V2_DISCRIMINATOR: [u8; 8] = [119, 40, 6, 235, 234, 221, 248, 49];
const BUBBLEGUM_MINT_V1_DISCRIMINATOR: [u8; 8] = [145, 98, 192, 118, 184, 147, 118, 104];
const BUBBLEGUM_MINT_TO_COLLECTION_V1_DISCRIMINATOR: [u8; 8] = [153, 18, 178, 47, 197, 158, 86, 15];
const WRAPPED_SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const SOLANA_USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const MAX_COLLECTION_REFERENCE_HARD_LIMIT: usize = 100_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeliusCollectionAsset {
    pub nft: SeedNft,
    pub owner_address: String,
    pub compressed: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeliusCollectionSnapshot {
    pub assets: Vec<HeliusCollectionAsset>,
    pub total: usize,
    pub collection_name: String,
    pub collection_symbol: String,
    pub collection_authority: String,
    pub truncated: bool,
    pub coverage_ratio: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeliusCollectionHistory {
    pub transfers: Vec<TransferRecord>,
    pub sales: Vec<NftSaleRecord>,
    pub failed_asset_count: usize,
    pub requested_asset_count: usize,
    pub successful_asset_count: usize,
    pub complete_asset_count: usize,
    pub unrequested_asset_count: usize,
    pub truncated_asset_history_count: usize,
    pub fetched_transaction_count: usize,
    /// Number of unique transaction payloads decoded into common receipt/payment details.
    pub parsed_transaction_count: usize,
    pub reported_transaction_count: usize,
    pub failed_transaction_count: usize,
    pub signature_discovery_failure_count: usize,
    pub transaction_detail_failure_count: usize,
    pub unattributed_native_transaction_count: usize,
    pub unattributed_native_transaction_signatures: HashSet<String>,
    pub unresolved_compressed_mint_count: usize,
    pub complete: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeliusTransactionDetails {
    pub receipt: TransactionReceiptRecord,
    pub native_transfers: Vec<EthTransferRecord>,
    pub pre_balances_native: BTreeMap<String, f64>,
    pub account_keys: Vec<String>,
    pub native_transfer_attribution_complete: bool,
}

#[derive(Clone, Debug)]
struct HeliusAssetSignaturePlan {
    mint_address: String,
    compressed: bool,
    signatures: Vec<(usize, String, String)>,
}

#[derive(Clone, Debug)]
struct HeliusSignaturePage {
    signatures: Vec<(String, String)>,
    reported_total: Option<usize>,
}

#[derive(Clone, Debug)]
struct HeliusAssetDiscoveryState {
    asset: HeliusCollectionAsset,
    signatures: Vec<(String, String)>,
    seen_signatures: HashSet<String>,
    seen_cursors: HashSet<String>,
    cursor: Option<String>,
    reported_total: Option<usize>,
    requested: bool,
    failed: bool,
    complete: bool,
    truncated: bool,
}

impl HeliusAssetDiscoveryState {
    fn new(asset: HeliusCollectionAsset) -> Self {
        Self {
            asset,
            signatures: Vec::new(),
            seen_signatures: HashSet::new(),
            seen_cursors: HashSet::new(),
            cursor: None,
            reported_total: None,
            requested: false,
            failed: false,
            complete: false,
            truncated: false,
        }
    }

    fn remaining_allowance(&self, max_transactions_per_asset: usize) -> usize {
        if max_transactions_per_asset == 0 {
            usize::MAX
        } else {
            max_transactions_per_asset.saturating_sub(self.signatures.len())
        }
    }

    fn apply_signature_page(
        &mut self,
        page: HeliusSignaturePage,
        requested_limit: usize,
        max_transactions_per_asset: usize,
    ) -> usize {
        if let Some(total) = page.reported_total {
            self.reported_total = Some(self.reported_total.unwrap_or_default().max(total));
        }
        let page_len = page.signatures.len();
        let next_cursor = page
            .signatures
            .last()
            .map(|(signature, _)| signature.clone());
        let cursor_advanced = next_cursor.is_some() && next_cursor != self.cursor;
        let repeated_cursor = next_cursor
            .as_ref()
            .is_some_and(|cursor| self.seen_cursors.contains(cursor));
        if let Some(cursor) = next_cursor.as_ref() {
            self.seen_cursors.insert(cursor.clone());
        }
        self.cursor = next_cursor;

        let mut newly_charged = 0;
        for (signature, event_type) in page.signatures {
            if self.seen_signatures.insert(signature.clone()) {
                newly_charged += 1;
                self.signatures.push((signature, event_type));
            }
        }
        let reached_asset_limit =
            max_transactions_per_asset > 0 && self.signatures.len() >= max_transactions_per_asset;
        let pagination_stalled =
            repeated_cursor || (page_len >= requested_limit.max(1) && newly_charged == 0);
        self.truncated = pagination_stalled
            || (reached_asset_limit
                && self
                    .reported_total
                    .is_none_or(|total| self.signatures.len() < total));
        self.complete = pagination_stalled
            || signature_page_is_complete(
                page_len,
                requested_limit,
                self.reported_total,
                self.signatures.len(),
                cursor_advanced,
                reached_asset_limit,
            );
        newly_charged
    }
}

fn signature_page_is_complete(
    page_len: usize,
    requested_limit: usize,
    reported_total: Option<usize>,
    discovered_count: usize,
    cursor_advanced: bool,
    reached_asset_limit: bool,
) -> bool {
    reached_asset_limit
        || !cursor_advanced
        || reported_total.is_some_and(|total| discovered_count >= total)
        || (reported_total.is_none() && page_len < requested_limit)
}

fn allocate_signature_reference_limits(
    allowances: &[usize],
    remaining_budget: Option<usize>,
    active_asset_count: usize,
) -> Vec<usize> {
    let mut allocatable = remaining_budget.unwrap_or(usize::MAX);
    let mut limits = Vec::with_capacity(allowances.len());
    for allowance in allowances.iter().copied() {
        if remaining_budget.is_some() && allocatable == 0 {
            limits.resize(allowances.len(), 0);
            break;
        }
        let fair_share = if remaining_budget.is_some() {
            allocatable.div_ceil(active_asset_count.max(1)).max(1)
        } else {
            1_000
        };
        let limit = fair_share.min(allowance).min(1_000);
        limits.push(limit);
        if remaining_budget.is_some() {
            allocatable = allocatable.saturating_sub(limit);
        }
    }
    limits
}

fn effective_collection_reference_budget(configured: usize) -> usize {
    if configured == 0 {
        MAX_COLLECTION_REFERENCE_HARD_LIMIT
    } else {
        configured.min(MAX_COLLECTION_REFERENCE_HARD_LIMIT)
    }
}

#[derive(Clone, Debug)]
struct HeliusTransactionAssetRef {
    mint_address: String,
    compressed: bool,
    event_index: usize,
    asset_event_type: String,
}

pub async fn fetch_helius_transaction_details(
    client: &AsyncApiClient,
    rpc_url: &str,
    signature: &str,
    native_usd_rate: Option<f64>,
) -> Result<Option<HeliusTransactionDetails>, AppError> {
    Ok(fetch_helius_transaction_value(client, rpc_url, signature)
        .await?
        .as_ref()
        .map(|result| parse_transaction_details(result, None, native_usd_rate)))
}

async fn fetch_helius_transaction_value(
    client: &AsyncApiClient,
    rpc_url: &str,
    signature: &str,
) -> Result<Option<Value>, AppError> {
    let payload: Value = client
        .post_json(
            rpc_url,
            &serde_json::json!({
                "jsonrpc": "2.0",
                "id": format!("transaction-{signature}"),
                "method": "getTransaction",
                "params": [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "commitment": "finalized",
                        "maxSupportedTransactionVersion": 0
                    }
                ]
            }),
        )
        .await?;
    if let Some(error) = payload.get("error") {
        return Err(AppError::Http(format!(
            "Helius getTransaction failed for {signature}: {error}"
        )));
    }
    Ok(payload
        .get("result")
        .filter(|value| !value.is_null())
        .cloned())
}

pub async fn fetch_helius_collection_assets(
    client: &AsyncApiClient,
    rpc_url: &str,
    collection_address: &str,
    page_size: usize,
    max_assets: usize,
) -> Result<Vec<HeliusCollectionAsset>, AppError> {
    Ok(
        fetch_helius_collection_snapshot(
            client,
            rpc_url,
            collection_address,
            page_size,
            max_assets,
        )
        .await?
        .assets,
    )
}

pub async fn fetch_helius_collection_snapshot(
    client: &AsyncApiClient,
    rpc_url: &str,
    collection_address: &str,
    page_size: usize,
    max_assets: usize,
) -> Result<HeliusCollectionSnapshot, AppError> {
    let page_size = page_size.clamp(1, 1_000);
    let mut page = 1_usize;
    let mut snapshot = HeliusCollectionSnapshot::default();
    let mut seen_mints = HashSet::new();
    loop {
        let request_limit = if max_assets == 0 {
            page_size
        } else {
            page_size.min(max_assets.saturating_sub(snapshot.assets.len()).max(1))
        };
        let payload: Value = client
            .post_json(
                rpc_url,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": format!("collection-assets-{page}"),
                    "method": "getAssetsByGroup",
                    "params": {
                        "groupKey": "collection",
                        "groupValue": collection_address,
                        "page": page,
                        "limit": request_limit,
                        "options": {
                            "showUnverifiedCollections": false,
                            "showCollectionMetadata": true,
                            "showGrandTotal": true
                        }
                    }
                }),
            )
            .await?;
        if let Some(error) = payload.get("error") {
            return Err(AppError::Http(format!(
                "Helius getAssetsByGroup failed for {collection_address}: {error}"
            )));
        }
        let result = payload.get("result").ok_or_else(|| {
            AppError::InvalidData("Helius getAssetsByGroup response is missing result".to_string())
        })?;
        let reported_total = result
            .get("total")
            .and_then(Value::as_u64)
            .map(|value| value as usize);
        if let Some(total) = reported_total {
            snapshot.total = total;
        }
        let items = result
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                AppError::InvalidData("Helius getAssetsByGroup result is missing items".to_string())
            })?;
        let collection_metadata = items
            .iter()
            .flat_map(|item| {
                item.get("grouping")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .find(|group| {
                group.get("group_key").and_then(Value::as_str) == Some("collection")
                    && group.get("group_value").and_then(Value::as_str) == Some(collection_address)
            })
            .and_then(|group| {
                group
                    .get("collection_metadata")
                    .or_else(|| group.get("collectionMetadata"))
            })
            .or_else(|| result.get("collection_metadata"))
            .or_else(|| result.get("collectionMetadata"))
            .unwrap_or(&Value::Null);
        if snapshot.collection_name.is_empty() {
            snapshot.collection_name = collection_metadata
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
        }
        if snapshot.collection_symbol.is_empty() {
            snapshot.collection_symbol = collection_metadata
                .get("symbol")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
        }
        if snapshot.collection_authority.is_empty() {
            snapshot.collection_authority = collection_metadata
                .get("update_authority")
                .or_else(|| collection_metadata.get("updateAuthority"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
        }
        let asset_count_before_page = snapshot.assets.len();
        for item in items {
            let mint = item.get("id").and_then(Value::as_str).unwrap_or("").trim();
            if mint.is_empty() || !seen_mints.insert(mint.to_string()) {
                continue;
            }
            let content = item.get("content").unwrap_or(&Value::Null);
            let metadata = content.get("metadata").unwrap_or(&Value::Null);
            snapshot.assets.push(HeliusCollectionAsset {
                nft: SeedNft {
                    chain: "solana".to_string(),
                    contract_address: collection_address.trim().to_string(),
                    token_id: mint.to_string(),
                    name: metadata
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    symbol: metadata
                        .get("symbol")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    token_uri: content
                        .get("json_uri")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    image_uri: content
                        .get("links")
                        .and_then(|links| links.get("image"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    metadata_json: helius_metadata_json(item, metadata),
                },
                owner_address: item
                    .get("ownership")
                    .and_then(|ownership| ownership.get("owner"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                compressed: item
                    .get("compression")
                    .and_then(|compression| compression.get("compressed"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
            if max_assets > 0 && snapshot.assets.len() >= max_assets {
                if reported_total.is_some() {
                    snapshot.truncated = snapshot.assets.len() < snapshot.total;
                    snapshot.coverage_ratio = (snapshot.total > 0)
                        .then_some(snapshot.assets.len() as f64 / snapshot.total as f64);
                } else {
                    // Reaching a caller cap without a provider total proves that
                    // the snapshot may be incomplete, but not its coverage ratio.
                    snapshot.total = snapshot.assets.len();
                    snapshot.truncated = true;
                    snapshot.coverage_ratio = None;
                }
                return Ok(snapshot);
            }
        }
        if reported_total.is_none() {
            snapshot.total = snapshot.assets.len();
        }
        let pagination_stalled =
            !items.is_empty() && snapshot.assets.len() == asset_count_before_page;
        if pagination_stalled {
            snapshot.truncated = true;
            snapshot.coverage_ratio = reported_total
                .filter(|total| *total > 0)
                .map(|total| snapshot.assets.len() as f64 / total as f64);
            return Ok(snapshot);
        }
        if items.is_empty()
            || items.len() < request_limit
            || reported_total.is_some_and(|total| snapshot.assets.len() >= total)
        {
            snapshot.truncated = snapshot.assets.len() < snapshot.total;
            snapshot.coverage_ratio = (snapshot.total > 0)
                .then_some(snapshot.assets.len() as f64 / snapshot.total as f64);
            return Ok(snapshot);
        }
        page += 1;
    }
}

pub async fn fetch_helius_asset_transfers(
    client: &AsyncApiClient,
    rpc_url: &str,
    collection_address: &str,
    mint_address: &str,
    max_transactions: usize,
) -> Result<Vec<TransferRecord>, AppError> {
    let plan =
        fetch_helius_asset_signature_plan(client, rpc_url, mint_address, false, max_transactions)
            .await?;
    Ok(
        fetch_helius_signature_plans_history(client, rpc_url, collection_address, vec![plan])
            .await
            .transfers,
    )
}

async fn fetch_helius_asset_signature_plan(
    client: &AsyncApiClient,
    rpc_url: &str,
    mint_address: &str,
    compressed: bool,
    max_transactions: usize,
) -> Result<HeliusAssetSignaturePlan, AppError> {
    let mut signatures = Vec::new();
    let mut seen_signatures = HashSet::new();
    let mut seen_cursors = HashSet::new();
    let mut cursor = None;
    let mut reported_total = None::<usize>;
    loop {
        let remaining = if max_transactions == 0 {
            1_000
        } else {
            max_transactions.saturating_sub(signatures.len()).min(1_000)
        };
        if remaining == 0 {
            break;
        }
        let page = fetch_helius_asset_signature_page(
            client,
            rpc_url,
            mint_address,
            cursor.as_deref(),
            remaining,
        )
        .await?;
        if let Some(total) = page.reported_total {
            reported_total = Some(reported_total.unwrap_or_default().max(total));
        }
        let page_len = page.signatures.len();
        let next_cursor = page
            .signatures
            .last()
            .map(|(signature, _)| signature.clone());
        let repeated_cursor = next_cursor
            .as_ref()
            .is_some_and(|cursor| seen_cursors.contains(cursor));
        if let Some(cursor) = next_cursor.as_ref() {
            seen_cursors.insert(cursor.clone());
        }
        let previous_count = signatures.len();
        for (signature, event_type) in page.signatures {
            if seen_signatures.insert(signature.clone()) {
                signatures.push((signature, event_type));
            }
        }
        let pagination_stalled =
            repeated_cursor || (page_len >= remaining.max(1) && signatures.len() == previous_count);
        let cursor_advanced = next_cursor.is_some() && next_cursor != cursor;
        cursor = next_cursor;
        if pagination_stalled
            || signature_page_is_complete(
                page_len,
                remaining,
                reported_total,
                signatures.len(),
                cursor_advanced,
                max_transactions > 0 && signatures.len() >= max_transactions,
            )
        {
            break;
        }
    }

    Ok(HeliusAssetSignaturePlan {
        mint_address: mint_address.to_string(),
        compressed,
        signatures: signatures
            .into_iter()
            .enumerate()
            .map(|(index, (signature, event_type))| (index, signature, event_type))
            .collect(),
    })
}

async fn fetch_helius_asset_signature_page(
    client: &AsyncApiClient,
    rpc_url: &str,
    mint_address: &str,
    before: Option<&str>,
    limit: usize,
) -> Result<HeliusSignaturePage, AppError> {
    let mut params = serde_json::json!({
        "id": mint_address,
        "page": 1,
        "limit": limit.clamp(1, 1_000)
    });
    if let Some(before) = before.filter(|value| !value.is_empty()) {
        params["before"] = Value::String(before.to_string());
    }
    let payload: Value = client
        .post_json(
            rpc_url,
            &serde_json::json!({
                "jsonrpc": "2.0",
                "id": "asset-signatures",
                "method": "getSignaturesForAsset",
                "params": params
            }),
        )
        .await?;
    if let Some(error) = payload.get("error") {
        return Err(AppError::Http(format!(
            "Helius getSignaturesForAsset failed for {mint_address}: {error}"
        )));
    }
    let result = payload.get("result").ok_or_else(|| {
        AppError::InvalidData("Helius getSignaturesForAsset response is missing result".to_string())
    })?;
    let items = result
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AppError::InvalidData(
                "Helius getSignaturesForAsset result is missing items".to_string(),
            )
        })?;
    let signatures = items
        .iter()
        .filter_map(|item| {
            let parts = item.as_array();
            let signature = parts
                .and_then(|parts| parts.first())
                .and_then(Value::as_str)
                .or_else(|| item.get("signature").and_then(Value::as_str))
                .or_else(|| item.get("id").and_then(Value::as_str))
                .unwrap_or("")
                .trim();
            if signature.is_empty() {
                return None;
            }
            let event_type = parts
                .and_then(|parts| parts.get(1))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            Some((signature.to_string(), event_type))
        })
        .collect::<Vec<_>>();
    let reported_total = result
        .get("total")
        .and_then(Value::as_u64)
        .map(|total| total as usize);
    Ok(HeliusSignaturePage {
        signatures,
        reported_total,
    })
}

fn append_helius_asset_transaction(
    history: &mut HeliusCollectionHistory,
    collection_address: &str,
    asset_ref: &HeliusTransactionAssetRef,
    signature: &str,
    result: &Value,
    details: &HeliusTransactionDetails,
) {
    history.fetched_transaction_count += 1;
    let tx_hash = if details.receipt.tx_hash.is_empty() {
        signature.to_string()
    } else {
        details.receipt.tx_hash.clone()
    };
    if !details.native_transfer_attribution_complete {
        history
            .unattributed_native_transaction_signatures
            .insert(tx_hash.clone());
    }
    let meta = result.get("meta").unwrap_or(&Value::Null);
    if meta.get("err").is_some_and(|error| !error.is_null()) {
        return;
    }
    let owner_change = token_owner_change(meta, &asset_ref.mint_address).or_else(|| {
        compressed_nft_owner_change(result, &asset_ref.asset_event_type, &details.account_keys)
    });
    let Some((before, after, is_mint)) = owner_change else {
        if asset_ref.compressed
            && asset_ref
                .asset_event_type
                .trim()
                .to_ascii_lowercase()
                .starts_with("mint")
        {
            history.unresolved_compressed_mint_count += 1;
        }
        return;
    };
    let seller = before.clone().unwrap_or_default();
    let buyer = after.clone();
    if !is_mint {
        if let Some(mut sale) = sale_from_owner_and_payment_changes(
            collection_address,
            &asset_ref.mint_address,
            &tx_hash,
            (
                result
                    .get("slot")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
                asset_ref.event_index as i64,
            ),
            &seller,
            &buyer,
            &details.native_transfers,
        ) {
            if transaction_nft_owner_change_count(meta) > 1 {
                clear_sale_amounts(&mut sale);
            }
            history.sales.push(sale);
        }
    }
    history.transfers.push(TransferRecord {
        contract_address: collection_address.trim().to_string(),
        token_id: asset_ref.mint_address.trim().to_string(),
        tx_hash,
        log_index: asset_ref.event_index as i64,
        block_number: result
            .get("slot")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        block_time: result
            .get("blockTime")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        from_address: before.unwrap_or_else(|| ZERO_ADDRESS.to_string()),
        to_address: after,
        event_type: if is_mint {
            "mint".to_string()
        } else {
            "transfer".to_string()
        },
        source: "helius".to_string(),
    });
}

fn parse_transaction_details(
    result: &Value,
    slot_override: Option<i64>,
    native_usd_rate: Option<f64>,
) -> HeliusTransactionDetails {
    let transaction = result.get("transaction").unwrap_or(&Value::Null);
    let message = transaction.get("message").unwrap_or(&Value::Null);
    let meta = result.get("meta").unwrap_or(&Value::Null);
    let accounts = message
        .get("accountKeys")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(account_key)
        .collect::<Vec<_>>();
    let tx_hash = transaction
        .get("signatures")
        .and_then(Value::as_array)
        .and_then(|values| values.first())
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let slot = slot_override
        .or_else(|| result.get("slot").and_then(Value::as_i64))
        .unwrap_or_default();
    let fee_lamports = meta.get("fee").and_then(Value::as_i64).unwrap_or_default();
    let mut details = HeliusTransactionDetails {
        receipt: TransactionReceiptRecord {
            tx_hash: tx_hash.clone(),
            block_number: slot,
            from_address: accounts.first().cloned().unwrap_or_default(),
            fee_native: (fee_lamports > 0).then_some(fee_lamports as f64 / 1_000_000_000.0),
            fee_usd: (fee_lamports > 0)
                .then_some(fee_lamports as f64 / 1_000_000_000.0)
                .zip(native_usd_rate)
                .map(|(fee, rate)| fee * rate),
            ..TransactionReceiptRecord::default()
        },
        account_keys: accounts.clone(),
        ..HeliusTransactionDetails::default()
    };
    if let Some(pre_balances) = meta.get("preBalances").and_then(Value::as_array) {
        for (account, balance) in accounts.iter().zip(pre_balances) {
            if let Some(lamports) = balance.as_u64() {
                details
                    .pre_balances_native
                    .insert(account.clone(), lamports as f64 / 1_000_000_000.0);
            }
        }
    }
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
    let mut attributed_lamports = HashMap::<String, i128>::new();
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
        let value_native = lamports as f64 / 1_000_000_000.0;
        let from_address = info
            .get("source")
            .or_else(|| info.get("from"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let to_address = info
            .get("destination")
            .or_else(|| info.get("to"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if from_address.is_empty() || to_address.is_empty() {
            continue;
        }
        *attributed_lamports.entry(from_address.clone()).or_default() -= lamports as i128;
        *attributed_lamports.entry(to_address.clone()).or_default() += lamports as i128;
        details.native_transfers.push(EthTransferRecord {
            tx_hash: tx_hash.clone(),
            block_number: slot,
            from_address,
            to_address,
            value_eth: value_native,
            value_usd: native_usd_rate.map(|rate| value_native * rate),
            payment_token_symbol: "SOL".to_string(),
            category: "external".to_string(),
            ..EthTransferRecord::default()
        });
    }
    details.native_transfers.extend(spl_payment_transfers(
        meta,
        &accounts,
        &instructions,
        &tx_hash,
        slot,
        native_usd_rate,
    ));
    details.native_transfer_attribution_complete =
        native_balance_changes_are_attributed(meta, &accounts, fee_lamports, &attributed_lamports);
    details
}

fn account_key(value: &Value) -> Option<String> {
    value
        .as_str()
        .or_else(|| value.get("pubkey").and_then(Value::as_str))
        .map(str::to_string)
}

fn helius_metadata_json(item: &Value, metadata: &Value) -> String {
    let mut payload = metadata.as_object().cloned().unwrap_or_default();
    for (source, target) in [
        ("royalty", "_helius_royalty"),
        ("grouping", "_helius_grouping"),
        ("compression", "_helius_compression"),
    ] {
        if let Some(value) = item.get(source).filter(|value| !value.is_null()) {
            payload.insert(target.to_string(), value.clone());
        }
    }
    if payload.is_empty() {
        String::new()
    } else {
        Value::Object(payload).to_string()
    }
}

pub async fn fetch_helius_collection_transfers(
    client: &AsyncApiClient,
    rpc_url: &str,
    collection_address: &str,
    max_assets: usize,
    max_transactions_per_asset: usize,
) -> Result<Vec<TransferRecord>, AppError> {
    let assets =
        fetch_helius_collection_assets(client, rpc_url, collection_address, 1_000, max_assets)
            .await?;
    fetch_helius_assets_transfers(
        client,
        rpc_url,
        collection_address,
        &assets,
        max_transactions_per_asset,
    )
    .await
}

pub async fn fetch_helius_assets_transfers(
    client: &AsyncApiClient,
    rpc_url: &str,
    collection_address: &str,
    assets: &[HeliusCollectionAsset],
    max_transactions_per_asset: usize,
) -> Result<Vec<TransferRecord>, AppError> {
    Ok(fetch_helius_assets_history(
        client,
        rpc_url,
        collection_address,
        assets,
        max_transactions_per_asset,
    )
    .await?
    .transfers)
}

pub async fn fetch_helius_assets_history(
    client: &AsyncApiClient,
    rpc_url: &str,
    collection_address: &str,
    assets: &[HeliusCollectionAsset],
    max_transactions_per_asset: usize,
) -> Result<HeliusCollectionHistory, AppError> {
    fetch_helius_assets_history_with_budget(
        client,
        rpc_url,
        collection_address,
        assets,
        max_transactions_per_asset,
        0,
    )
    .await
}

pub async fn fetch_helius_assets_history_with_budget(
    client: &AsyncApiClient,
    rpc_url: &str,
    collection_address: &str,
    assets: &[HeliusCollectionAsset],
    max_transactions_per_asset: usize,
    max_transactions_per_collection: usize,
) -> Result<HeliusCollectionHistory, AppError> {
    let mut sorted_assets = assets.to_vec();
    sorted_assets.sort_by(|left, right| left.nft.token_id.cmp(&right.nft.token_id));
    let mut states = sorted_assets
        .into_iter()
        .map(HeliusAssetDiscoveryState::new)
        .collect::<Vec<_>>();
    let mut remaining_budget = Some(effective_collection_reference_budget(
        max_transactions_per_collection,
    ));
    let mut next_asset_index = 0_usize;
    while remaining_budget != Some(0) {
        let all_active = states
            .iter()
            .enumerate()
            .filter(|(_, state)| {
                !state.complete
                    && !state.failed
                    && state.remaining_allowance(max_transactions_per_asset) > 0
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if all_active.is_empty() {
            break;
        }

        let start = all_active
            .iter()
            .position(|index| *index >= next_asset_index)
            .unwrap_or_default();
        let selected = (0..all_active.len().min(64))
            .map(|offset| all_active[(start + offset) % all_active.len()])
            .collect::<Vec<_>>();
        let allowances = selected
            .iter()
            .map(|index| states[*index].remaining_allowance(max_transactions_per_asset))
            .collect::<Vec<_>>();
        let limits =
            allocate_signature_reference_limits(&allowances, remaining_budget, all_active.len());
        let mut scheduled = Vec::with_capacity(all_active.len().min(64));
        for (index, limit) in selected.into_iter().zip(limits) {
            if limit == 0 {
                continue;
            }
            states[index].requested = true;
            scheduled.push((
                index,
                states[index].asset.nft.token_id.clone(),
                states[index].cursor.clone(),
                limit,
            ));
            next_asset_index = (index + 1) % states.len().max(1);
        }
        if scheduled.is_empty() {
            break;
        }

        let mut fetched = stream::iter(scheduled.into_iter().map(
            |(index, mint_address, cursor, limit)| async move {
                (
                    index,
                    limit,
                    fetch_helius_asset_signature_page(
                        client,
                        rpc_url,
                        &mint_address,
                        cursor.as_deref(),
                        limit,
                    )
                    .await,
                )
            },
        ))
        .buffered(8);

        while let Some((index, requested_limit, result)) = fetched.next().await {
            match result {
                Ok(page) => {
                    let state = &mut states[index];
                    let newly_charged = state.apply_signature_page(
                        page,
                        requested_limit,
                        max_transactions_per_asset,
                    );
                    if let Some(remaining) = remaining_budget.as_mut() {
                        *remaining = remaining.saturating_sub(newly_charged);
                    }
                }
                Err(error) => {
                    states[index].failed = true;
                    eprintln!(
                    "warning: failed to fetch Helius history for one asset in collection {collection_address}: {error}"
                );
                }
            }
        }
    }

    let mut history = HeliusCollectionHistory::default();
    history.requested_asset_count = states.iter().filter(|state| state.requested).count();
    history.failed_asset_count = states.iter().filter(|state| state.failed).count();
    history.signature_discovery_failure_count = history.failed_asset_count;
    history.unrequested_asset_count = states.iter().filter(|state| !state.requested).count();
    history.successful_asset_count = states
        .iter()
        .filter(|state| state.requested && !state.failed)
        .count();
    history.reported_transaction_count = states
        .iter()
        .filter(|state| state.requested && !state.failed)
        .map(|state| state.reported_total.unwrap_or(state.signatures.len()))
        .sum();
    history.truncated_asset_history_count = states
        .iter()
        .filter(|state| !state.requested || (!state.failed && (!state.complete || state.truncated)))
        .count();
    history.complete_asset_count = states
        .iter()
        .filter(|state| state.requested && !state.failed && state.complete && !state.truncated)
        .count();
    let plans = states
        .into_iter()
        .filter(|state| !state.signatures.is_empty())
        .map(|state| HeliusAssetSignaturePlan {
            mint_address: state.asset.nft.token_id,
            compressed: state.asset.compressed,
            signatures: state
                .signatures
                .into_iter()
                .enumerate()
                .map(|(index, (signature, event_type))| (index, signature, event_type))
                .collect(),
        })
        .collect::<Vec<_>>();
    merge_helius_history(
        &mut history,
        fetch_helius_signature_plans_history(client, rpc_url, collection_address, plans).await,
    );
    history.transaction_detail_failure_count = history.failed_transaction_count;
    history.complete = history.failed_asset_count == 0
        && history.unrequested_asset_count == 0
        && history.truncated_asset_history_count == 0
        && history.failed_transaction_count == 0;
    let sale_count_by_transaction =
        history
            .sales
            .iter()
            .fold(HashMap::<String, usize>::new(), |mut counts, sale| {
                *counts.entry(sale.tx_hash.clone()).or_default() += 1;
                counts
            });
    for sale in &mut history.sales {
        if sale_count_by_transaction
            .get(&sale.tx_hash)
            .copied()
            .unwrap_or_default()
            > 1
        {
            // Raw RPC does not expose marketplace bundle allocation. Preserve the
            // sale event but do not copy a transaction-level payment to every NFT.
            clear_sale_amounts(sale);
        }
    }
    history.unattributed_native_transaction_count =
        history.unattributed_native_transaction_signatures.len();
    history.transfers.sort_by(|left, right| {
        (
            left.block_number,
            left.log_index,
            left.tx_hash.as_str(),
            left.token_id.as_str(),
        )
            .cmp(&(
                right.block_number,
                right.log_index,
                right.tx_hash.as_str(),
                right.token_id.as_str(),
            ))
    });
    history.sales.sort_by(|left, right| {
        (
            left.block_number,
            left.log_index,
            left.tx_hash.as_str(),
            left.token_id.as_str(),
        )
            .cmp(&(
                right.block_number,
                right.log_index,
                right.tx_hash.as_str(),
                right.token_id.as_str(),
            ))
    });
    Ok(history)
}

fn merge_helius_history(target: &mut HeliusCollectionHistory, mut source: HeliusCollectionHistory) {
    target.transfers.append(&mut source.transfers);
    target.sales.append(&mut source.sales);
    target.fetched_transaction_count += source.fetched_transaction_count;
    target.parsed_transaction_count += source.parsed_transaction_count;
    target.failed_transaction_count += source.failed_transaction_count;
    target
        .unattributed_native_transaction_signatures
        .extend(source.unattributed_native_transaction_signatures);
    target.unresolved_compressed_mint_count += source.unresolved_compressed_mint_count;
}

async fn fetch_helius_signature_plans_history(
    client: &AsyncApiClient,
    rpc_url: &str,
    collection_address: &str,
    plans: Vec<HeliusAssetSignaturePlan>,
) -> HeliusCollectionHistory {
    let mut refs_by_signature = HashMap::<String, Vec<HeliusTransactionAssetRef>>::new();
    for plan in plans {
        for (event_index, signature, asset_event_type) in plan.signatures {
            refs_by_signature
                .entry(signature)
                .or_default()
                .push(HeliusTransactionAssetRef {
                    mint_address: plan.mint_address.clone(),
                    compressed: plan.compressed,
                    event_index,
                    asset_event_type,
                });
        }
    }
    let mut fetched = stream::iter(refs_by_signature.into_iter().map(
        |(signature, asset_refs)| async move {
            let result = fetch_helius_transaction_value(client, rpc_url, &signature).await;
            (signature, asset_refs, result)
        },
    ))
    .buffer_unordered(32);
    let mut history = HeliusCollectionHistory::default();
    while let Some((signature, asset_refs, result)) = fetched.next().await {
        match result {
            Ok(Some(result)) => {
                let details = parse_transaction_details(&result, None, None);
                history.parsed_transaction_count += 1;
                for asset_ref in &asset_refs {
                    append_helius_asset_transaction(
                        &mut history,
                        collection_address,
                        asset_ref,
                        &signature,
                        &result,
                        &details,
                    );
                }
            }
            Ok(None) => history.failed_transaction_count += asset_refs.len(),
            Err(error) => {
                history.failed_transaction_count += asset_refs.len();
                eprintln!(
                    "warning: failed to fetch one deduplicated Helius transaction {signature}: {error}"
                );
            }
        }
        // Release each transaction JSON immediately after all referenced assets
        // are processed; only the bounded in-flight window remains resident.
    }
    history.unattributed_native_transaction_count =
        history.unattributed_native_transaction_signatures.len();
    history
}

#[derive(Clone, Debug, Default)]
struct TokenBalanceEntry {
    owner: String,
    amount: i128,
}

fn compressed_nft_owner_change(
    result: &Value,
    asset_event_type: &str,
    account_keys: &[String],
) -> Option<(Option<String>, String, bool)> {
    let is_transfer = asset_event_type.eq_ignore_ascii_case("transfer");
    let is_mint = asset_event_type
        .trim()
        .to_ascii_lowercase()
        .starts_with("mint");
    if !is_transfer && !is_mint {
        return None;
    }
    let message = result.get("transaction")?.get("message")?;
    let mut instructions = message
        .get("instructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for group in result
        .get("meta")
        .and_then(|meta| meta.get("innerInstructions"))
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
    for instruction in instructions {
        let program_id = instruction
            .get("programId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                instruction
                    .get("programIdIndex")
                    .and_then(Value::as_u64)
                    .and_then(|index| account_keys.get(index as usize).cloned())
            });
        if program_id.as_deref() != Some(BUBBLEGUM_PROGRAM_ID) {
            continue;
        }
        if is_mint {
            let discriminator = instruction
                .get("data")
                .and_then(Value::as_str)
                .and_then(decode_base58)
                .and_then(|bytes| bytes.get(..8).map(|slice| slice.to_vec()));
            let parsed_is_mint = instruction
                .get("parsed")
                .and_then(|parsed| parsed.get("type"))
                .and_then(Value::as_str)
                .is_some_and(|kind| kind.to_ascii_lowercase().starts_with("mint"));
            let supported_discriminator = matches!(
                discriminator.as_deref(),
                Some(bytes)
                    if bytes == BUBBLEGUM_MINT_V1_DISCRIMINATOR
                        || bytes == BUBBLEGUM_MINT_TO_COLLECTION_V1_DISCRIMINATOR
            );
            if !parsed_is_mint && !supported_discriminator {
                continue;
            }
            let parsed_owner = instruction
                .get("parsed")
                .and_then(|parsed| parsed.get("info"))
                .and_then(|info| {
                    info.get("leafOwner")
                        .or_else(|| info.get("leaf_owner"))
                        .or_else(|| info.get("owner"))
                })
                .and_then(Value::as_str)
                .map(str::to_string);
            let account_owner = instruction
                .get("accounts")
                .and_then(Value::as_array)
                .and_then(|accounts| accounts.get(1))
                .and_then(|value| instruction_account(value, account_keys));
            let Some(owner) = parsed_owner.or(account_owner) else {
                continue;
            };
            if !owner.is_empty() {
                return Some((None, owner, true));
            }
            continue;
        }
        let discriminator = instruction
            .get("data")
            .and_then(Value::as_str)
            .and_then(decode_base58)
            .and_then(|bytes| bytes.get(..8).map(|slice| slice.to_vec()));
        let (from_index, to_index) = match discriminator.as_deref() {
            Some(bytes) if bytes == BUBBLEGUM_TRANSFER_V1_DISCRIMINATOR => (1, 3),
            Some(bytes) if bytes == BUBBLEGUM_TRANSFER_V2_DISCRIMINATOR => (3, 5),
            _ => continue,
        };
        let accounts = instruction.get("accounts")?.as_array()?;
        let from = instruction_account(accounts.get(from_index)?, account_keys)?;
        let to = instruction_account(accounts.get(to_index)?, account_keys)?;
        if from.is_empty() || to.is_empty() || from == to {
            continue;
        }
        return Some((Some(from), to, false));
    }
    None
}

fn sale_from_owner_and_payment_changes(
    collection_address: &str,
    mint_address: &str,
    tx_hash: &str,
    event_position: (i64, i64),
    seller: &str,
    buyer: &str,
    payments: &[EthTransferRecord],
) -> Option<NftSaleRecord> {
    if seller.is_empty() || buyer.is_empty() || seller == buyer {
        return None;
    }
    let buyer_payments = payments
        .iter()
        .filter(|payment| payment.from_address == buyer)
        .filter(|payment| {
            payment.value_eth > 0.0
                || payment.value_usd.unwrap_or_default() > 0.0
                || (payment.category == "spl" && !payment.payment_token_address.is_empty())
        })
        .collect::<Vec<_>>();
    if buyer_payments.is_empty() {
        return None;
    }
    let mut payment_groups = BTreeMap::<String, Vec<&EthTransferRecord>>::new();
    for payment in buyer_payments {
        let key = if matches!(payment.payment_token_symbol.as_str(), "SOL" | "WSOL") {
            "SOL".to_string()
        } else if !payment.payment_token_address.is_empty() {
            payment.payment_token_address.clone()
        } else {
            payment.payment_token_symbol.clone()
        };
        if !key.is_empty() {
            payment_groups.entry(key).or_default().push(payment);
        }
    }
    let mut seller_payment_groups = payment_groups
        .into_iter()
        .filter(|(_, group)| {
            group.iter().any(|payment| {
                payment.to_address == seller
                    && (payment.value_eth > 0.0
                        || payment.value_usd.unwrap_or_default() > 0.0
                        || (payment.category == "spl" && !payment.payment_token_address.is_empty()))
            })
        })
        .collect::<Vec<_>>();
    if seller_payment_groups.len() != 1 {
        return Some(unpriced_helius_sale(
            collection_address,
            mint_address,
            tx_hash,
            event_position,
            seller,
            buyer,
            "MIXED",
        ));
    }
    let (_, group) = seller_payment_groups.pop()?;
    let buyer_payments = group
        .into_iter()
        .filter(|payment| payment.to_address == seller)
        .collect::<Vec<_>>();
    let symbol = if buyer_payments
        .iter()
        .all(|payment| matches!(payment.payment_token_symbol.as_str(), "SOL" | "WSOL"))
    {
        "SOL".to_string()
    } else {
        buyer_payments[0].payment_token_symbol.clone()
    };
    let price_native = buyer_payments
        .iter()
        .map(|payment| payment.value_eth)
        .sum::<f64>();
    let price_usd = buyer_payments
        .iter()
        .map(|payment| payment.value_usd)
        .collect::<Option<Vec<_>>>()
        .map(|values| values.into_iter().sum::<f64>());
    let seller_usd = buyer_payments
        .iter()
        .filter_map(|payment| payment.value_usd)
        .sum::<f64>();
    let payment_token_address = buyer_payments[0].payment_token_address.clone();
    Some(NftSaleRecord {
        contract_address: collection_address.trim().to_string(),
        token_id: mint_address.trim().to_string(),
        tx_hash: tx_hash.to_string(),
        block_number: event_position.0,
        log_index: event_position.1,
        buyer_address: buyer.to_string(),
        seller_address: seller.to_string(),
        marketplace: "helius".to_string(),
        payment_token_symbol: symbol.clone(),
        payment_token_address,
        price_eth: (price_native > 0.0).then_some(price_native),
        price_usd,
        seller_fee_eth: price_native,
        seller_fee_usd: seller_usd,
        protocol_fee_eth: 0.0,
        protocol_fee_usd: 0.0,
        source: "helius".to_string(),
        is_native_eth: matches!(symbol.as_str(), "SOL" | "WSOL"),
        ..NftSaleRecord::default()
    })
}

fn unpriced_helius_sale(
    collection_address: &str,
    mint_address: &str,
    tx_hash: &str,
    event_position: (i64, i64),
    seller: &str,
    buyer: &str,
    payment_symbol: &str,
) -> NftSaleRecord {
    NftSaleRecord {
        contract_address: collection_address.trim().to_string(),
        token_id: mint_address.trim().to_string(),
        tx_hash: tx_hash.to_string(),
        block_number: event_position.0,
        log_index: event_position.1,
        buyer_address: buyer.to_string(),
        seller_address: seller.to_string(),
        marketplace: "helius".to_string(),
        payment_token_symbol: payment_symbol.to_string(),
        source: "helius".to_string(),
        ..NftSaleRecord::default()
    }
}

fn clear_sale_amounts(sale: &mut NftSaleRecord) {
    sale.price_eth = None;
    sale.price_usd = None;
    sale.seller_fee_eth = 0.0;
    sale.seller_fee_usd = 0.0;
    sale.protocol_fee_eth = 0.0;
    sale.protocol_fee_usd = 0.0;
    sale.royalty_fee_eth = 0.0;
    sale.royalty_fee_usd = 0.0;
}

fn instruction_account(value: &Value, account_keys: &[String]) -> Option<String> {
    value.as_str().map(str::to_string).or_else(|| {
        value
            .as_u64()
            .and_then(|index| account_keys.get(index as usize).cloned())
    })
}

fn decode_base58(value: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if value.is_empty() {
        return None;
    }
    let mut decoded = vec![0_u8];
    for byte in value.bytes() {
        let digit = ALPHABET.iter().position(|candidate| *candidate == byte)? as u32;
        let mut carry = digit;
        for item in decoded.iter_mut().rev() {
            let next = u32::from(*item) * 58 + carry;
            *item = (next & 0xff) as u8;
            carry = next >> 8;
        }
        while carry > 0 {
            decoded.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let leading_zeroes = value.bytes().take_while(|byte| *byte == b'1').count();
    let first_nonzero = decoded
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(decoded.len());
    let mut result = vec![0_u8; leading_zeroes];
    result.extend_from_slice(&decoded[first_nonzero..]);
    Some(result)
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

fn spl_payment_transfers(
    meta: &Value,
    account_keys: &[String],
    instructions: &[&Value],
    tx_hash: &str,
    slot: i64,
    native_usd_rate: Option<f64>,
) -> Vec<EthTransferRecord> {
    let token_accounts = token_account_descriptors(meta, account_keys);
    let mut transfers = Vec::new();
    for instruction in instructions {
        let Some(parsed) = instruction.get("parsed") else {
            continue;
        };
        if !matches!(
            parsed.get("type").and_then(Value::as_str).unwrap_or(""),
            "transfer" | "transferChecked"
        ) {
            continue;
        }
        let info = parsed.get("info").unwrap_or(&Value::Null);
        let source_account = info.get("source").and_then(Value::as_str).unwrap_or("");
        let destination_account = info
            .get("destination")
            .and_then(Value::as_str)
            .unwrap_or("");
        let source = token_accounts.get(source_account);
        let destination = token_accounts.get(destination_account);
        let mint = info
            .get("mint")
            .and_then(Value::as_str)
            .or_else(|| source.map(|value| value.mint.as_str()))
            .or_else(|| destination.map(|value| value.mint.as_str()))
            .unwrap_or("");
        let decimals = info
            .get("tokenAmount")
            .and_then(|value| value.get("decimals"))
            .and_then(Value::as_u64)
            .map(|value| value as i32)
            .or_else(|| source.map(|value| value.decimals))
            .or_else(|| destination.map(|value| value.decimals))
            .unwrap_or_default();
        // Decimal-zero token transfers are predominantly NFT ownership changes and
        // must not be interpreted as fungible payment flows.
        if mint.is_empty() || decimals <= 0 {
            continue;
        }
        let raw_amount = info
            .get("tokenAmount")
            .and_then(|value| value.get("amount"))
            .or_else(|| info.get("amount"))
            .and_then(parse_i128_value)
            .unwrap_or_default();
        if raw_amount <= 0 {
            continue;
        }
        let from_address = source
            .map(|value| value.owner.as_str())
            .filter(|value| !value.is_empty())
            .or_else(|| info.get("authority").and_then(Value::as_str))
            .unwrap_or("");
        let to_address = destination
            .map(|value| value.owner.as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("");
        if from_address.is_empty() || to_address.is_empty() || from_address == to_address {
            continue;
        }
        let amount = raw_amount as f64 / 10_f64.powi(decimals);
        let (symbol, value_native, value_usd) = match mint {
            WRAPPED_SOL_MINT => (
                "WSOL".to_string(),
                amount,
                native_usd_rate.map(|rate| amount * rate),
            ),
            SOLANA_USDC_MINT => (
                "USDC".to_string(),
                native_usd_rate
                    .filter(|rate| *rate > 0.0)
                    .map(|rate| amount / rate)
                    .unwrap_or_default(),
                Some(amount),
            ),
            _ => ("SPL".to_string(), 0.0, None),
        };
        transfers.push(EthTransferRecord {
            tx_hash: tx_hash.to_string(),
            block_number: slot,
            from_address: from_address.to_string(),
            to_address: to_address.to_string(),
            value_eth: value_native,
            value_usd,
            payment_token_symbol: symbol,
            payment_token_address: mint.to_string(),
            category: "spl".to_string(),
        });
    }
    transfers
}

#[derive(Clone, Debug)]
struct TokenAccountDescriptor {
    owner: String,
    mint: String,
    decimals: i32,
}

fn token_account_descriptors(
    meta: &Value,
    account_keys: &[String],
) -> HashMap<String, TokenAccountDescriptor> {
    let mut descriptors = HashMap::<String, TokenAccountDescriptor>::new();
    for row in meta
        .get("preTokenBalances")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            meta.get("postTokenBalances")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
    {
        let Some(index) = row.get("accountIndex").and_then(Value::as_u64) else {
            continue;
        };
        let Some(account) = account_keys.get(index as usize) else {
            continue;
        };
        let incoming = TokenAccountDescriptor {
            owner: row
                .get("owner")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            mint: row
                .get("mint")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            decimals: row
                .get("uiTokenAmount")
                .and_then(|value| value.get("decimals"))
                .and_then(Value::as_u64)
                .unwrap_or_default() as i32,
        };
        descriptors
            .entry(account.clone())
            .and_modify(|existing| {
                if existing.owner.is_empty() {
                    existing.owner = incoming.owner.clone();
                }
                if existing.mint.is_empty() {
                    existing.mint = incoming.mint.clone();
                }
                if existing.decimals == 0 {
                    existing.decimals = incoming.decimals;
                }
            })
            .or_insert(incoming);
    }
    descriptors
}

fn parse_i128_value(value: &Value) -> Option<i128> {
    value
        .as_str()
        .and_then(|value| value.parse().ok())
        .or_else(|| value.as_i64().map(i128::from))
        .or_else(|| value.as_u64().map(i128::from))
}

fn native_balance_changes_are_attributed(
    meta: &Value,
    account_keys: &[String],
    fee_lamports: i64,
    attributed_lamports: &HashMap<String, i128>,
) -> bool {
    let Some(pre) = meta.get("preBalances").and_then(Value::as_array) else {
        return false;
    };
    let Some(post) = meta.get("postBalances").and_then(Value::as_array) else {
        return false;
    };
    if pre.len() != post.len() || pre.len() != account_keys.len() {
        return false;
    }
    account_keys.iter().enumerate().all(|(index, account)| {
        let actual = post[index].as_u64().unwrap_or_default() as i128
            - pre[index].as_u64().unwrap_or_default() as i128;
        let mut expected = attributed_lamports
            .get(account)
            .copied()
            .unwrap_or_default();
        if index == 0 {
            expected -= fee_lamports.max(0) as i128;
        }
        actual == expected
    })
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

fn transaction_nft_owner_change_count(meta: &Value) -> usize {
    let mut mints = HashSet::new();
    for field in ["preTokenBalances", "postTokenBalances"] {
        for balance in meta
            .get(field)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let decimals = balance
                .get("uiTokenAmount")
                .and_then(|amount| amount.get("decimals"))
                .and_then(Value::as_u64);
            if decimals == Some(0) {
                if let Some(mint) = balance.get("mint").and_then(Value::as_str) {
                    if !mint.is_empty() {
                        mints.insert(mint);
                    }
                }
            }
        }
    }
    mints
        .into_iter()
        .filter(|mint| token_owner_change(meta, mint).is_some())
        .count()
}

#[cfg(test)]
mod review_regression_tests {
    use super::{
        allocate_signature_reference_limits, effective_collection_reference_budget,
        signature_page_is_complete, HeliusAssetDiscoveryState, HeliusSignaturePage,
        MAX_COLLECTION_REFERENCE_HARD_LIMIT,
    };
    use crate::api::HeliusCollectionAsset;

    fn test_asset() -> HeliusCollectionAsset {
        HeliusCollectionAsset {
            nft: crate::models::SeedNft::default(),
            owner_address: String::new(),
            compressed: false,
        }
    }

    #[test]
    fn unknown_total_full_page_is_not_complete() {
        assert!(!signature_page_is_complete(
            100, 100, None, 100, true, false
        ));
    }

    #[test]
    fn known_total_short_page_is_not_complete_before_reported_total() {
        assert!(!signature_page_is_complete(
            50,
            100,
            Some(200),
            50,
            true,
            false,
        ));
    }

    #[test]
    fn collection_budget_limits_asset_signature_references() {
        assert_eq!(
            allocate_signature_reference_limits(&[100, 100, 100], Some(2), 3),
            vec![1, 1, 0]
        );
    }

    #[test]
    fn zero_or_oversized_collection_budget_uses_the_hard_memory_boundary() {
        assert_eq!(
            effective_collection_reference_budget(0),
            MAX_COLLECTION_REFERENCE_HARD_LIMIT
        );
        assert_eq!(
            effective_collection_reference_budget(usize::MAX),
            MAX_COLLECTION_REFERENCE_HARD_LIMIT
        );
    }

    #[test]
    fn repeated_signature_cursor_marks_asset_history_truncated() {
        let mut state = HeliusAssetDiscoveryState::new(test_asset());
        state.apply_signature_page(
            HeliusSignaturePage {
                signatures: vec![
                    ("signature-a".into(), "".into()),
                    ("signature-b".into(), "".into()),
                ],
                reported_total: None,
            },
            2,
            0,
        );

        let newly_charged = state.apply_signature_page(
            HeliusSignaturePage {
                signatures: vec![
                    ("signature-c".into(), "".into()),
                    ("signature-b".into(), "".into()),
                ],
                reported_total: None,
            },
            2,
            0,
        );

        assert_eq!(newly_charged, 1);
        assert!(state.complete);
        assert!(state.truncated);
    }

    #[test]
    fn full_signature_page_without_new_rows_marks_asset_history_truncated() {
        let mut state = HeliusAssetDiscoveryState::new(test_asset());
        state.apply_signature_page(
            HeliusSignaturePage {
                signatures: vec![
                    ("signature-a".into(), "".into()),
                    ("signature-b".into(), "".into()),
                ],
                reported_total: None,
            },
            2,
            0,
        );
        state.apply_signature_page(
            HeliusSignaturePage {
                signatures: vec![
                    ("signature-b".into(), "".into()),
                    ("signature-a".into(), "".into()),
                ],
                reported_total: None,
            },
            2,
            0,
        );

        assert!(state.complete);
        assert!(state.truncated);
    }
}
