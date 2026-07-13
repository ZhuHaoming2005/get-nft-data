use futures::{stream, StreamExt};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

use crate::api::AsyncApiClient;
use crate::error::AppError;
use crate::models::TransferRecord;

use super::collection::fetch_helius_collection_assets;
use super::transaction::{
    append_helius_asset_transaction, clear_sale_amounts, fetch_helius_transaction_value,
    parse_transaction_details,
};
use super::{HeliusCollectionAsset, HeliusCollectionHistory};

const MAX_COLLECTION_REFERENCE_HARD_LIMIT: usize = 100_000;
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
pub(super) struct HeliusTransactionAssetRef {
    pub(super) mint_address: String,
    pub(super) compressed: bool,
    pub(super) event_index: usize,
    pub(super) asset_event_type: String,
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
