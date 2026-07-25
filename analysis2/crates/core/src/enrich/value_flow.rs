//! EVM native value-flow edges (Alchemy EXTERNAL transfers).
//!
//! Derive operators from candidate NFT activity, then query
//! `alchemy_getAssetTransfers` category `external` for each operator
//! across the operator's full history. A returned page key is marked
//! Truncated, so formal reports never pretend a one-page partial history is complete.

use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use tokio::sync::{Mutex as AsyncMutex, OnceCell};

use super::alchemy::{self, FetchOutcome, NativeTransfer};
use super::http::HttpClient;
use super::roles::{HolderSnapshot, victim_addresses};
use super::types::{
    DeploymentEvent, EvidenceObservation, EvidenceStatus, ProviderEndpoints, SaleEvent,
    TransferEvent, ValueFlowEdge, ValueFlowKind, normalize_chain_address, now_unix,
};

const ZERO: &str = "0x0000000000000000000000000000000000000000";
/// Cap operator seeds so enrich stays bounded.
const MAX_OPERATORS: usize = 16;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ExternalTransferKey {
    chain: String,
    address: String,
    direction: String,
    from_block: u64,
    to_block: u64,
}

type SharedExternalCell = Arc<OnceCell<FetchOutcome<Vec<NativeTransfer>>>>;

/// Run-scoped singleflight for Alchemy EXTERNAL transfer requests. It is shared
/// by preliminary/final value-flow passes and mint-payment attribution.
#[derive(Clone, Default)]
pub struct ExternalTransferCache {
    cells: Arc<AsyncMutex<AHashMap<ExternalTransferKey, SharedExternalCell>>>,
}

impl ExternalTransferCache {
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch(
        &self,
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
        let key = ExternalTransferKey {
            chain: chain.trim().to_ascii_lowercase(),
            address: normalize_chain_address(chain, address),
            direction: direction.to_owned(),
            from_block,
            to_block,
        };
        let cell = {
            let mut cells = self.cells.lock().await;
            cells
                .entry(key.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let outcome = cell
            .get_or_init(|| async {
                alchemy::fetch_external_transfers(
                    client, endpoints, api_key, chain, address, direction, from_block, to_block,
                    request_id,
                )
                .await
            })
            .await
            .clone();
        if !matches!(
            outcome.status,
            EvidenceStatus::Complete | EvidenceStatus::Empty
        ) {
            let mut cells = self.cells.lock().await;
            if cells
                .get(&key)
                .is_some_and(|known| Arc::ptr_eq(known, &cell))
            {
                cells.remove(&key);
            }
        }
        outcome
    }
}

/// Normalize, sort, and cap a previously classified operator seed set.
pub fn collect_operator_seeds(addresses: &[String]) -> (Vec<String>, bool) {
    let mut set = BTreeSet::new();
    for address in addresses {
        insert_addr(&mut set, address);
    }
    let truncated = set.len() > MAX_OPERATORS;
    (set.into_iter().take(MAX_OPERATORS).collect(), truncated)
}

/// Derive the value-flow query set from the same role rule used by attribution:
/// paid buyers that still hold the purchased NFT are victims; every other
/// participant remains eligible as an operator seed.
pub(crate) fn derive_operator_seeds(
    chain: &str,
    candidate: &str,
    controllers: &[String],
    deployment: Option<&DeploymentEvent>,
    transfers: &[TransferEvent],
    sales: &[SaleEvent],
    holders: HolderSnapshot<'_>,
) -> Vec<String> {
    let mut all = BTreeSet::new();
    let victims = victim_addresses(chain, transfers, sales, holders);
    let mut insert = |raw: &str| {
        let address = normalize_chain_address(chain, raw);
        if !address.is_empty() && address != ZERO {
            all.insert(address);
        }
    };
    insert(candidate);
    for controller in controllers {
        insert(controller);
    }
    if let Some(payer) = deployment.and_then(|event| event.fee_payer.as_deref()) {
        insert(payer);
    }
    for transfer in transfers {
        insert(&transfer.from);
        insert(&transfer.to);
    }
    for sale in sales {
        insert(&sale.seller);
        insert(&sale.buyer);
    }
    all.into_iter()
        .filter(|address| !victims.contains(address))
        .collect()
}

fn insert_addr(set: &mut BTreeSet<String>, raw: &str) {
    let addr = raw.trim().to_ascii_lowercase();
    if addr.is_empty() || addr == ZERO {
        return;
    }
    set.insert(addr);
}

fn value_flow_request_key(operators_truncated: bool, association_incomplete: bool) -> String {
    let mut notes = Vec::new();
    if operators_truncated {
        notes.push(format!(
            "operator seeds truncated at MAX_OPERATORS={MAX_OPERATORS}"
        ));
    }
    if association_incomplete {
        notes.push("candidate activity window or value-flow block number unavailable".into());
    }
    if notes.is_empty() {
        "alchemy_value_flows".into()
    } else {
        format!("alchemy_value_flows ({})", notes.join("; "))
    }
}

/// Retain flows that can be tied to the candidate's observable NFT activity.
///
/// In-window transfers are retained. Outside that window, only the nearest
/// pre-window funding block and nearest post-window withdrawal block for each
/// operator are retained as setup/cashout boundary evidence. This prevents a
/// controller's unrelated lifetime wallet traffic from entering candidate
/// economics.
fn activity_related_transfers(
    raw: &[NativeTransfer],
    operators: &AHashSet<String>,
    window: Option<(u64, u64)>,
) -> (Vec<NativeTransfer>, bool) {
    let Some((lo, hi)) = window else {
        return (Vec::new(), true);
    };

    let mut nearest_funding = AHashMap::<String, u64>::new();
    let mut nearest_withdrawal = AHashMap::<String, u64>::new();
    let mut association_incomplete = false;

    for transfer in raw {
        let from_op = operators.contains(&transfer.from);
        let to_op = operators.contains(&transfer.to);
        if !from_op && !to_op {
            continue;
        }
        let Some(block) = transfer.block_number else {
            association_incomplete = true;
            continue;
        };
        if block < lo && !from_op && to_op {
            nearest_funding
                .entry(transfer.to.clone())
                .and_modify(|current| *current = (*current).max(block))
                .or_insert(block);
        } else if block > hi && from_op && !to_op {
            nearest_withdrawal
                .entry(transfer.from.clone())
                .and_modify(|current| *current = (*current).min(block))
                .or_insert(block);
        }
    }

    let selected = raw
        .iter()
        .filter(|transfer| {
            let Some(block) = transfer.block_number else {
                return false;
            };
            if (lo..=hi).contains(&block) {
                return true;
            }
            let from_op = operators.contains(&transfer.from);
            let to_op = operators.contains(&transfer.to);
            (block < lo && !from_op && to_op && nearest_funding.get(&transfer.to) == Some(&block))
                || (block > hi
                    && from_op
                    && !to_op
                    && nearest_withdrawal.get(&transfer.from) == Some(&block))
        })
        .cloned()
        .collect();
    (selected, association_incomplete)
}

/// Activity block window from NFT transfers / sales when block numbers are known.
pub fn activity_block_window(
    transfers: &[TransferEvent],
    sales: &[SaleEvent],
) -> Option<(u64, u64)> {
    let mut min_b = None;
    let mut max_b = None;
    for event in transfers {
        if let Some(b) = event.block_number {
            min_b = Some(min_b.map_or(b, |m: u64| m.min(b)));
            max_b = Some(max_b.map_or(b, |m: u64| m.max(b)));
        }
    }
    for event in sales {
        if let Some(b) = event.block_number {
            min_b = Some(min_b.map_or(b, |m: u64| m.min(b)));
            max_b = Some(max_b.map_or(b, |m: u64| m.max(b)));
        }
    }
    match (min_b, max_b) {
        (Some(lo), Some(hi)) => Some((lo, hi)),
        _ => None,
    }
}

/// Classify a native EXTERNAL transfer relative to the operator seed set.
pub fn classify_native_edge(
    transfer: &NativeTransfer,
    operators: &AHashSet<String>,
) -> Option<ValueFlowEdge> {
    if transfer.tx_hash.is_empty() {
        return None;
    }
    let from_op = operators.contains(&transfer.from);
    let to_op = operators.contains(&transfer.to);
    if !from_op && !to_op {
        return None;
    }
    if transfer.from == transfer.to {
        return None;
    }
    let kind = match (from_op, to_op) {
        (false, true) => ValueFlowKind::Funding,
        (true, false) => ValueFlowKind::Withdrawal,
        (true, true) => ValueFlowKind::RevenueBackflow,
        (false, false) => return None,
    };
    Some(ValueFlowEdge {
        tx_hash: transfer.tx_hash.clone(),
        event_id: transfer.event_id.clone(),
        from: transfer.from.clone(),
        to: transfer.to.clone(),
        kind,
        native_amount: transfer.value_native,
        usd_amount: None,
        timestamp: transfer.timestamp,
        gas_native: None,
        fee_payer: None,
    })
}

/// Fetch and classify EVM value-flow edges for operator seeds.
///
/// Status: NotRequested (no key) / Empty (no operators or no edges) /
/// Complete (all queries ok, window known, no page/operator truncation) /
/// Truncated (partial success, pageKey left, operator cap, or unbounded window) /
/// Failed (all requests fail).
pub async fn fetch_evm_value_flows(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    operator_seeds: &[String],
    transfers: &[TransferEvent],
    sales: &[SaleEvent],
) -> FetchOutcome<Vec<ValueFlowEdge>> {
    fetch_evm_value_flows_impl(
        client,
        endpoints,
        api_key,
        chain,
        operator_seeds,
        transfers,
        sales,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn fetch_evm_value_flows_cached(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    operator_seeds: &[String],
    transfers: &[TransferEvent],
    sales: &[SaleEvent],
    cache: &ExternalTransferCache,
) -> FetchOutcome<Vec<ValueFlowEdge>> {
    fetch_evm_value_flows_impl(
        client,
        endpoints,
        api_key,
        chain,
        operator_seeds,
        transfers,
        sales,
        Some(cache.clone()),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn fetch_evm_value_flows_impl(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    operator_seeds: &[String],
    transfers: &[TransferEvent],
    sales: &[SaleEvent],
    cache: Option<ExternalTransferCache>,
) -> FetchOutcome<Vec<ValueFlowEdge>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_value_flows");
    };

    let (operators, operators_truncated) = collect_operator_seeds(operator_seeds);
    if operators.is_empty() {
        return FetchOutcome::ok(Vec::new(), 0, false, "alchemy", "alchemy_value_flows");
    }
    if transfers.is_empty() && sales.is_empty() {
        return FetchOutcome::ok(Vec::new(), 0, false, "alchemy", "alchemy_value_flows");
    }

    // Fetch full history so the nearest setup/cashout boundary can be found,
    // then retain only flows associated with the candidate activity window.
    let (from_block, to_block) = (0, u64::MAX);

    let operator_set: AHashSet<String> = operators.iter().cloned().collect();
    let mut handles = Vec::new();

    for (idx, address) in operators.iter().cloned().enumerate() {
        for direction in ["from", "to"] {
            let client = client.clone();
            let endpoints = endpoints.clone();
            let api_key = api_key.to_owned();
            let chain = chain.to_owned();
            let address = address.clone();
            let dir = direction.to_owned();
            let cache = cache.clone();
            handles.push(tokio::spawn(async move {
                match cache {
                    Some(cache) => {
                        cache
                            .fetch(
                                &client,
                                &endpoints,
                                Some(&api_key),
                                &chain,
                                &address,
                                &dir,
                                from_block,
                                to_block,
                                idx,
                            )
                            .await
                    }
                    None => {
                        alchemy::fetch_external_transfers(
                            &client,
                            &endpoints,
                            Some(&api_key),
                            &chain,
                            &address,
                            &dir,
                            from_block,
                            to_block,
                            idx,
                        )
                        .await
                    }
                }
            }));
        }
    }

    let mut raw = Vec::new();
    let mut any_ok = false;
    let mut any_fail = false;
    let mut page_truncated = false;
    let mut failures = Vec::new();

    for handle in handles {
        match handle.await {
            Ok(outcome) => match outcome.status {
                EvidenceStatus::NotRequested => {}
                EvidenceStatus::Failed => {
                    any_fail = true;
                    if let Some(f) = outcome.failure {
                        failures.push(f);
                    }
                }
                EvidenceStatus::Empty | EvidenceStatus::Complete | EvidenceStatus::Truncated => {
                    any_ok = true;
                    if outcome.truncated || outcome.status == EvidenceStatus::Truncated {
                        page_truncated = true;
                    }
                    raw.extend(outcome.value);
                }
            },
            Err(e) => {
                any_fail = true;
                failures.push(format!("value_flow task join failed: {e}"));
            }
        }
    }

    if !any_ok {
        let detail = if failures.is_empty() {
            "all value-flow fetches failed".into()
        } else {
            failures.join("; ")
        };
        return FetchOutcome::failed("alchemy", "alchemy_value_flows", detail);
    }

    let (raw, association_incomplete) =
        activity_related_transfers(&raw, &operator_set, activity_block_window(transfers, sales));
    let mut edges = Vec::new();
    let mut seen = BTreeSet::new();
    for transfer in raw {
        let Some(edge) = classify_native_edge(&transfer, &operator_set) else {
            continue;
        };
        let key = (
            edge.tx_hash.clone(),
            edge.event_id.clone(),
            edge.from.clone(),
            edge.to.clone(),
            format!("{:?}", edge.kind),
            edge.native_amount
                .map(|v| format!("{v:.18}"))
                .unwrap_or_default(),
        );
        if seen.insert(key) {
            edges.push(edge);
        }
    }

    let truncated = page_truncated || any_fail || operators_truncated || association_incomplete;
    let count = edges.len();
    let request_key = value_flow_request_key(operators_truncated, association_incomplete);
    let mut outcome = FetchOutcome::ok(edges, count, truncated, "alchemy", &request_key);
    // A full-history query with no edges is conclusively Empty when every page
    // completed. Operator-seed truncation must never report Complete.
    if count == 0 && !page_truncated && !any_fail && !operators_truncated && !association_incomplete
    {
        outcome.status = EvidenceStatus::Empty;
        if let Some(obs) = outcome.observation.as_mut() {
            obs.status = EvidenceStatus::Empty;
            obs.request_key = request_key.clone();
        }
        outcome.truncated = false;
    } else if truncated {
        outcome.status = EvidenceStatus::Truncated;
        if let Some(obs) = outcome.observation.as_mut() {
            obs.status = EvidenceStatus::Truncated;
            obs.request_key = request_key.clone();
        }
        outcome.truncated = true;
    }
    // Real fetch failures only — informational truncation notes stay in provenance
    // (request_key), never in outcome.failure / quality.failures.
    if any_fail && !failures.is_empty() {
        outcome.failure = Some(format!(
            "alchemy_value_flows: partial failures: {}",
            failures.into_iter().take(3).collect::<Vec<_>>().join("; ")
        ));
    }
    // Ensure observation timestamp freshness for provenance.
    if let Some(obs) = &mut outcome.observation {
        obs.observed_at = now_unix();
        if obs.source.is_empty() {
            *obs = EvidenceObservation {
                source: "alchemy".into(),
                request_key,
                observed_at: now_unix(),
                status: outcome.status,
            };
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::{Method::POST, MockServer};
    use serde_json::json;

    use crate::enrich::HolderRecord;

    #[tokio::test]
    async fn external_transfer_cache_singleflights_identical_requests() {
        let server = MockServer::start_async().await;
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/rpc")
                    .body_contains("alchemy_getAssetTransfers");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "external-from-0",
                    "result": {"transfers": []}
                }));
            })
            .await;
        let endpoints = ProviderEndpoints {
            alchemy_rpc_template: format!("{}/rpc", server.base_url()),
            ..ProviderEndpoints::default()
        };
        let client = HttpClient::with_retries(2, 0).unwrap();
        let cache = ExternalTransferCache::default();
        for request_id in 0..2 {
            let outcome = cache
                .fetch(
                    &client,
                    &endpoints,
                    Some("key"),
                    "ethereum",
                    "0x1111111111111111111111111111111111111111",
                    "from",
                    0,
                    u64::MAX,
                    request_id,
                )
                .await;
            assert_eq!(outcome.status, EvidenceStatus::Empty);
        }
        assert_eq!(rpc.hits(), 1);
    }

    fn transfer(
        tx: &str,
        from: &str,
        to: &str,
        is_mint: bool,
        fee_payer: Option<&str>,
        block: Option<u64>,
    ) -> TransferEvent {
        TransferEvent {
            tx_hash: tx.into(),
            token_id: "1".into(),
            from: from.into(),
            to: to.into(),
            timestamp: None,
            block_number: block,
            is_mint,
            gas_native: None,
            fee_payer: fee_payer.map(str::to_owned),
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }
    }

    #[test]
    fn operator_seed_normalization_uses_only_the_classified_input_set() {
        let (seeds, truncated) = collect_operator_seeds(&["0xAAA".into()]);
        assert!(!truncated);
        assert!(seeds.contains(&"0xaaa".to_owned()));
        assert!(!seeds.contains(&"0xfeepayer".to_owned()));
    }

    #[test]
    fn paid_buyer_without_sale_is_excluded_from_derived_operator_seeds() {
        let mut paid = transfer(
            "0xmint",
            ZERO,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            true,
            Some("0xMintFeePayer"),
            Some(10),
        );
        paid.mint_payment_native = Some(1.0);
        let seeds = derive_operator_seeds(
            "ethereum",
            "0xcccccccccccccccccccccccccccccccccccccccc",
            &[],
            None,
            &[paid],
            &[],
            HolderSnapshot {
                records: &[HolderRecord {
                    token_id: "1".into(),
                    owner: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                    balance: Some(1),
                }],
                status: EvidenceStatus::Complete,
            },
        );
        assert!(!seeds.contains(&"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()));
        assert!(seeds.contains(&"0xcccccccccccccccccccccccccccccccccccccccc".to_owned()));
    }

    #[test]
    fn operator_seeds_truncated_past_max_operators() {
        let controllers: Vec<String> = (1..=(MAX_OPERATORS + 3))
            .map(|i| format!("0x{i:040x}"))
            .collect();
        let (seeds, truncated) = collect_operator_seeds(&controllers);
        assert!(truncated);
        assert_eq!(seeds.len(), MAX_OPERATORS);
    }

    #[test]
    fn request_key_carries_operator_truncation_note() {
        let key = value_flow_request_key(true, false);
        assert!(key.contains("MAX_OPERATORS"));
        assert_eq!(value_flow_request_key(false, false), "alchemy_value_flows");
    }

    #[test]
    fn classify_funding_and_withdrawal() {
        let mut ops = AHashSet::new();
        ops.insert("0xop".into());
        let funding = NativeTransfer {
            tx_hash: "0xf".into(),
            event_id: None,
            from: "0xfunder".into(),
            to: "0xop".into(),
            value_native: Some(1.5),
            timestamp: Some(1),
            block_number: Some(10),
        };
        let edge = classify_native_edge(&funding, &ops).unwrap();
        assert_eq!(edge.kind, ValueFlowKind::Funding);
        assert!((edge.native_amount.unwrap() - 1.5).abs() < 1e-12);

        let withdrawal = NativeTransfer {
            tx_hash: "0xw".into(),
            event_id: None,
            from: "0xop".into(),
            to: "0xout".into(),
            value_native: Some(0.25),
            timestamp: None,
            block_number: Some(11),
        };
        let edge = classify_native_edge(&withdrawal, &ops).unwrap();
        assert_eq!(edge.kind, ValueFlowKind::Withdrawal);
    }

    #[test]
    fn classify_revenue_backflow_between_operators() {
        let mut ops = AHashSet::new();
        ops.insert("0xa".into());
        ops.insert("0xb".into());
        let t = NativeTransfer {
            tx_hash: "0xr".into(),
            event_id: None,
            from: "0xa".into(),
            to: "0xb".into(),
            value_native: Some(0.1),
            timestamp: None,
            block_number: None,
        };
        let edge = classify_native_edge(&t, &ops).unwrap();
        assert_eq!(edge.kind, ValueFlowKind::RevenueBackflow);
    }

    #[test]
    fn parse_external_transfer_amount_from_value_and_raw() {
        let item = json!({
            "hash": "0xabc",
            "from": "0xFrom",
            "to": "0xTo",
            "category": "external",
            "value": 1.25,
            "blockNum": "0x10",
            "metadata": { "blockTimestamp": "2024-01-01T00:00:00Z" }
        });
        let parsed = alchemy::parse_native_transfer(&item).unwrap();
        assert_eq!(parsed.from, "0xfrom");
        assert_eq!(parsed.to, "0xto");
        assert!((parsed.value_native.unwrap() - 1.25).abs() < 1e-12);
        assert_eq!(parsed.block_number, Some(16));

        let item_raw = json!({
            "hash": "0xdef",
            "from": "0xa",
            "to": "0xb",
            "category": "external",
            "rawContract": {
                "value": "0xde0b6b3a7640000",
                "decimal": "0x12"
            }
        });
        let parsed = alchemy::parse_native_transfer(&item_raw).unwrap();
        assert!((parsed.value_native.unwrap() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn activity_window_from_transfers_and_sales() {
        let transfers = vec![transfer("0x1", ZERO, "0xbb", true, None, Some(5))];
        let sales = vec![SaleEvent {
            tx_hash: "0x2".into(),
            token_id: "1".into(),
            seller: "0xa".into(),
            buyer: "0xb".into(),
            timestamp: None,
            block_number: Some(20),
            marketplace: None,
            native_amount: None,
            usd_amount: None,
            currency_symbol: None,
            currency_address: None,
            seller_proceeds_native: None,
            seller_proceeds_usd: None,
            ..SaleEvent::default()
        }];
        assert_eq!(activity_block_window(&transfers, &sales), Some((5, 20)));
        assert_eq!(activity_block_window(&[], &[]), None);
    }

    #[test]
    fn activity_filter_keeps_window_and_nearest_setup_and_cashout_only() {
        let operators = AHashSet::from_iter(["0xop".to_owned()]);
        let raw = vec![
            NativeTransfer {
                tx_hash: "old-funding".into(),
                from: "0xa".into(),
                to: "0xop".into(),
                block_number: Some(1),
                ..NativeTransfer::default()
            },
            NativeTransfer {
                tx_hash: "setup".into(),
                from: "0xb".into(),
                to: "0xop".into(),
                block_number: Some(9),
                ..NativeTransfer::default()
            },
            NativeTransfer {
                tx_hash: "during".into(),
                from: "0xop".into(),
                to: "0xc".into(),
                block_number: Some(15),
                ..NativeTransfer::default()
            },
            NativeTransfer {
                tx_hash: "cashout".into(),
                from: "0xop".into(),
                to: "0xd".into(),
                block_number: Some(21),
                ..NativeTransfer::default()
            },
            NativeTransfer {
                tx_hash: "late".into(),
                from: "0xop".into(),
                to: "0xe".into(),
                block_number: Some(30),
                ..NativeTransfer::default()
            },
        ];
        let (selected, incomplete) = activity_related_transfers(&raw, &operators, Some((10, 20)));
        assert!(!incomplete);
        let hashes = selected
            .iter()
            .map(|transfer| transfer.tx_hash.as_str())
            .collect::<AHashSet<_>>();
        assert_eq!(hashes, AHashSet::from_iter(["setup", "during", "cashout"]));
    }

    #[test]
    fn activity_filter_marks_missing_blocks_incomplete() {
        let operators = AHashSet::from_iter(["0xop".to_owned()]);
        let raw = vec![NativeTransfer {
            tx_hash: "unknown".into(),
            from: "0xop".into(),
            to: "0xout".into(),
            block_number: None,
            ..NativeTransfer::default()
        }];
        let (selected, incomplete) = activity_related_transfers(&raw, &operators, Some((10, 20)));
        assert!(selected.is_empty());
        assert!(incomplete);
    }
}
