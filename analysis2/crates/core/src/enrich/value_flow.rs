//! EVM native value-flow edges (Alchemy EXTERNAL transfers).
//!
//! MVP: seed operators from controllers + mint fee_payers / non-zero mint `from`,
//! then query `alchemy_getAssetTransfers` category `external` for each operator
//! with `fromBlock`/`toBlock` clamped to the NFT activity block window when known.
//! Unbounded windows (no block numbers) are allowed but marked Truncated.

use std::collections::BTreeSet;
use std::sync::Arc;

use ahash::AHashSet;
use tokio::sync::Semaphore;

use super::alchemy::{self, FetchOutcome, NativeTransfer};
use super::http::HttpClient;
use super::types::{
    now_unix, EvidenceObservation, EvidenceStatus, ProviderEndpoints, SaleEvent, TransferEvent,
    ValueFlowEdge, ValueFlowKind,
};

const ZERO: &str = "0x0000000000000000000000000000000000000000";
/// Cap operator seeds so enrich stays bounded.
const MAX_OPERATORS: usize = 16;

/// Collect operator / controller seed addresses for value-flow classification.
///
/// Seeds = controllers + mint `fee_payer` + non-zero mint `from`.
/// Non-mint transfer fee_payers are ignored. Second return is true when the
/// unique seed set exceeded [`MAX_OPERATORS`] and was truncated.
pub fn collect_operator_seeds(
    controllers: &[String],
    transfers: &[TransferEvent],
) -> (Vec<String>, bool) {
    let mut set = BTreeSet::new();
    for controller in controllers {
        insert_addr(&mut set, controller);
    }
    for transfer in transfers {
        if !transfer.is_mint {
            continue;
        }
        if let Some(fee_payer) = &transfer.fee_payer {
            insert_addr(&mut set, fee_payer);
        }
        // Mint `from` is usually the zero address; keep non-zero senders as seeds.
        insert_addr(&mut set, &transfer.from);
    }
    let truncated = set.len() > MAX_OPERATORS;
    (set.into_iter().take(MAX_OPERATORS).collect(), truncated)
}

fn insert_addr(set: &mut BTreeSet<String>, raw: &str) {
    let addr = raw.trim().to_ascii_lowercase();
    if addr.is_empty() || addr == ZERO {
        return;
    }
    set.insert(addr);
}

fn value_flow_request_key(unbounded: bool, operators_truncated: bool) -> String {
    let mut notes = Vec::new();
    if operators_truncated {
        notes.push(format!(
            "operator seeds truncated at MAX_OPERATORS={MAX_OPERATORS}"
        ));
    }
    if unbounded {
        notes.push(
            "activity block window unknown; used unbounded fromBlock/toBlock".to_owned(),
        );
    }
    if notes.is_empty() {
        "alchemy_value_flows".into()
    } else {
        format!("alchemy_value_flows ({})", notes.join("; "))
    }
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
        from: transfer.from.clone(),
        to: transfer.to.clone(),
        kind,
        native_amount: transfer.value_native,
        usd_amount: None,
        timestamp: transfer.timestamp,
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
    controllers: &[String],
    transfers: &[TransferEvent],
    sales: &[SaleEvent],
    concurrency: usize,
) -> FetchOutcome<Vec<ValueFlowEdge>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_value_flows");
    };

    let (operators, operators_truncated) = collect_operator_seeds(controllers, transfers);
    if operators.is_empty() {
        return FetchOutcome::ok(
            Vec::new(),
            0,
            false,
            "alchemy",
            "alchemy_value_flows",
        );
    }

    let window = activity_block_window(transfers, sales);
    let unbounded = window.is_none();
    let (from_block, to_block) = window.unwrap_or((0, u64::MAX));

    let operator_set: AHashSet<String> = operators.iter().cloned().collect();
    let sem = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut handles = Vec::new();

    for (idx, address) in operators.iter().cloned().enumerate() {
        for direction in ["from", "to"] {
            let client = client.clone();
            let endpoints = endpoints.clone();
            let api_key = api_key.to_owned();
            let chain = chain.to_owned();
            let sem = sem.clone();
            let address = address.clone();
            let dir = direction.to_owned();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.ok();
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
            Ok(outcome) => {
                match outcome.status {
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
                }
            }
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

    let mut edges = Vec::new();
    let mut seen = BTreeSet::new();
    for transfer in raw {
        let Some(edge) = classify_native_edge(&transfer, &operator_set) else {
            continue;
        };
        let key = (
            edge.tx_hash.clone(),
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

    let truncated = page_truncated || unbounded || any_fail || operators_truncated;
    let count = edges.len();
    let request_key = value_flow_request_key(unbounded, operators_truncated);
    let mut outcome = FetchOutcome::ok(edges, count, truncated, "alchemy", &request_key);
    // Unbounded window with no edges stays Empty (no page/operator truncation).
    // Operator-seed truncation must never report Complete — including empty results.
    if unbounded && count == 0 && !page_truncated && !any_fail && !operators_truncated {
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
    use serde_json::json;

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
        }
    }

    #[test]
    fn operator_seeds_from_controllers_and_mint_fee_payers() {
        let (seeds, truncated) = collect_operator_seeds(
            &["0xAAA".into()],
            &[transfer(
                "0x1",
                ZERO,
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                true,
                Some("0xFeePayer"),
                Some(10),
            )],
        );
        assert!(!truncated);
        assert!(seeds.contains(&"0xaaa".to_owned()));
        assert!(seeds.contains(&"0xfeepayer".to_owned()));
    }

    #[test]
    fn non_mint_fee_payer_is_not_operator_seed() {
        let (seeds, truncated) = collect_operator_seeds(
            &[],
            &[
                transfer(
                    "0xmint",
                    ZERO,
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    true,
                    Some("0xMintFeePayer"),
                    Some(10),
                ),
                transfer(
                    "0xsec",
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "0xcccccccccccccccccccccccccccccccccccccccc",
                    false,
                    Some("0xSecondaryFeePayer"),
                    Some(11),
                ),
            ],
        );
        assert!(!truncated);
        assert!(seeds.contains(&"0xmintfeepayer".to_owned()));
        assert!(!seeds.contains(&"0xsecondaryfeepayer".to_owned()));
    }

    #[test]
    fn operator_seeds_truncated_past_max_operators() {
        let controllers: Vec<String> = (1..=(MAX_OPERATORS + 3))
            .map(|i| format!("0x{i:040x}"))
            .collect();
        let (seeds, truncated) = collect_operator_seeds(&controllers, &[]);
        assert!(truncated);
        assert_eq!(seeds.len(), MAX_OPERATORS);
    }

    #[test]
    fn request_key_carries_truncation_notes_not_failure() {
        let key = value_flow_request_key(true, true);
        assert!(key.contains("MAX_OPERATORS"));
        assert!(key.contains("activity block window unknown"));
        assert_eq!(value_flow_request_key(false, false), "alchemy_value_flows");
    }

    #[test]
    fn classify_funding_and_withdrawal() {
        let mut ops = AHashSet::new();
        ops.insert("0xop".into());
        let funding = NativeTransfer {
            tx_hash: "0xf".into(),
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
        }];
        assert_eq!(activity_block_window(&transfers, &sales), Some((5, 20)));
        assert_eq!(activity_block_window(&[], &[]), None);
    }
}
