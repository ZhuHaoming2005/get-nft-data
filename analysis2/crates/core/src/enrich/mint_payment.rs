//! Attach same-tx native payments onto mint `TransferEvent`s.

use ahash::AHashMap;

use super::types::{day_bucket, PriceBucket, TransferEvent, ValueFlowEdge};

fn norm_tx(tx: &str) -> String {
    tx.trim().to_ascii_lowercase()
}

fn norm_addr(addr: &str) -> String {
    addr.trim().to_ascii_lowercase()
}

fn usd_for_ts(prices: &[PriceBucket], chain: &str, ts: Option<i64>, native: f64) -> Option<f64> {
    let ts = ts?;
    let day = day_bucket(ts);
    let rate = prices.iter().find(|p| {
        p.day_utc == day
            && (p.symbol.eq_ignore_ascii_case("ETH")
                || p.symbol.eq_ignore_ascii_case("MATIC")
                || p.symbol.eq_ignore_ascii_case("POL")
                || p.symbol.eq_ignore_ascii_case("SOL")
                || chain_matches_symbol(chain, &p.symbol))
    })?;
    if rate.usd_per_native > 0.0 {
        Some(native * rate.usd_per_native)
    } else {
        None
    }
}

fn chain_matches_symbol(chain: &str, symbol: &str) -> bool {
    match chain.trim().to_ascii_lowercase().as_str() {
        "ethereum" | "base" => symbol.eq_ignore_ascii_case("ETH"),
        "polygon" | "matic" => {
            symbol.eq_ignore_ascii_case("MATIC") || symbol.eq_ignore_ascii_case("POL")
        }
        "solana" => symbol.eq_ignore_ascii_case("SOL"),
        _ => false,
    }
}

/// Sum same-tx value-flow amounts involving the mint recipient (`from` or `to`).
pub fn payment_native_from_value_flows(
    mint: &TransferEvent,
    value_flows: &[ValueFlowEdge],
) -> Option<f64> {
    let tx = norm_tx(&mint.tx_hash);
    let buyer = norm_addr(&mint.to);
    if tx.is_empty() || buyer.is_empty() {
        return None;
    }
    let mut total = 0.0;
    for edge in value_flows {
        if norm_tx(&edge.tx_hash) != tx {
            continue;
        }
        let amt = edge.native_amount.unwrap_or(0.0);
        if amt <= 0.0 {
            continue;
        }
        let from = norm_addr(&edge.from);
        let to = norm_addr(&edge.to);
        // Prefer buyer payout; also accept inflow to buyer in the mint tx.
        if from == buyer || to == buyer {
            total += amt;
        }
    }
    (total > 0.0).then_some(total)
}

/// Attach `mint_payment_*` from value-flow edges (and optional precomputed native map).
///
/// `extra_native_by_tx`: tx_hash (lower) → native amount from EXTERNAL `from=mint.to` probes.
pub fn attach_mint_payments(
    transfers: &mut [TransferEvent],
    value_flows: &[ValueFlowEdge],
    prices: &[PriceBucket],
    chain: &str,
    extra_native_by_tx: &AHashMap<String, f64>,
) {
    for transfer in transfers.iter_mut() {
        if !transfer.is_mint {
            continue;
        }
        let mut native = payment_native_from_value_flows(transfer, value_flows).unwrap_or(0.0);
        if let Some(&extra) = extra_native_by_tx.get(&norm_tx(&transfer.tx_hash)) {
            native = native.max(extra);
        }
        if native <= 0.0 {
            transfer.mint_payment_native = None;
            transfer.mint_payment_usd = None;
            continue;
        }
        transfer.mint_payment_native = Some(native);
        transfer.mint_payment_usd = usd_for_ts(prices, chain, transfer.timestamp, native)
            .or_else(|| {
                // Fall back to first matching chain price bucket.
                prices
                    .iter()
                    .find(|p| chain_matches_symbol(chain, &p.symbol) && p.usd_per_native > 0.0)
                    .map(|p| native * p.usd_per_native)
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrich::types::ValueFlowKind;

    fn mint(tx: &str, to: &str) -> TransferEvent {
        TransferEvent {
            tx_hash: tx.into(),
            token_id: "1".into(),
            from: "0x0000000000000000000000000000000000000000".into(),
            to: to.into(),
            timestamp: Some(1_700_000_000),
            block_number: Some(1),
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
        }
    }

    #[test]
    fn attaches_buyer_payout_from_value_flow() {
        let mut transfers = vec![mint("0xabc", "0xbuyer")];
        let flows = vec![ValueFlowEdge {
            tx_hash: "0xABC".into(),
            from: "0xBuyer".into(),
            to: "0xcontract".into(),
            kind: ValueFlowKind::Withdrawal,
            native_amount: Some(0.05),
            usd_amount: Some(100.0),
            timestamp: Some(1_700_000_000),
        }];
        let prices = vec![PriceBucket {
            chain: "ethereum".into(),
            day_utc: day_bucket(1_700_000_000),
            symbol: "ETH".into(),
            usd_per_native: 2000.0,
        }];
        attach_mint_payments(&mut transfers, &flows, &prices, "ethereum", &AHashMap::new());
        assert_eq!(transfers[0].mint_payment_native, Some(0.05));
        assert_eq!(transfers[0].mint_payment_usd, Some(100.0));
    }

    #[test]
    fn free_mint_stays_none() {
        let mut transfers = vec![mint("0xabc", "0xbuyer")];
        attach_mint_payments(
            &mut transfers,
            &[],
            &[],
            "ethereum",
            &AHashMap::new(),
        );
        assert!(transfers[0].mint_payment_native.is_none());
    }
}
