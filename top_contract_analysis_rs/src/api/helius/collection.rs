use serde_json::Value;
use std::collections::HashSet;

use crate::api::AsyncApiClient;
use crate::error::AppError;
use crate::models::SeedNft;

use super::{HeliusCollectionAsset, HeliusCollectionSnapshot};
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
