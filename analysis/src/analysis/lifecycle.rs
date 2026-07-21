use crate::model::{EventKind, LifecycleFacts, NormalizedEvent};
use std::collections::BTreeSet;

pub fn build_lifecycle(
    deployment_timestamp: Option<i64>,
    duplicate_content_timestamp: Option<i64>,
    events: &[NormalizedEvent],
    history_complete: bool,
    analysis_timestamp: i64,
) -> LifecycleFacts {
    let mut first_activity_timestamp = deployment_timestamp;
    let mut first_mint_timestamp = None;
    let mut first_transfer_timestamp = None;
    let mut first_sale_timestamp = None;
    let mut first_victim_timestamp = None;
    let mut relevant_timing_complete = true;
    for event in events {
        first_activity_timestamp = minimum_time(first_activity_timestamp, event.timestamp);
        match event.kind {
            EventKind::Mint => {
                first_mint_timestamp = minimum_time(first_mint_timestamp, event.timestamp);
            }
            EventKind::Transfer => {
                first_transfer_timestamp = minimum_time(first_transfer_timestamp, event.timestamp);
            }
            EventKind::Sale if event.is_nft_sale() => {
                first_sale_timestamp = minimum_time(first_sale_timestamp, event.timestamp);
            }
            _ => {}
        }
        if (event.is_nft_sale() || event.kind == EventKind::Mint)
            && (event.native_amount.unwrap_or(0) > 0 || event.usd_micros.unwrap_or(0) > 0)
        {
            first_victim_timestamp = minimum_time(first_victim_timestamp, event.timestamp);
        }
        if (matches!(
            event.kind,
            EventKind::Funding | EventKind::Mint | EventKind::Transfer | EventKind::Listing
        ) || event.is_nft_sale())
            && event.timestamp.is_none()
        {
            relevant_timing_complete = false;
        }
    }
    let first_result = [first_sale_timestamp, first_victim_timestamp]
        .into_iter()
        .flatten()
        .min();
    let mut categories = BTreeSet::new();
    if let Some(result) = first_result {
        for event in events
            .iter()
            .filter(|event| event.timestamp.is_some_and(|time| time < result))
        {
            match event.kind {
                EventKind::Funding => {
                    categories.insert("control_or_funding_link".to_owned());
                }
                EventKind::Mint
                    if event.native_amount.unwrap_or(0) <= 0
                        && event.usd_micros.unwrap_or(0) <= 0 =>
                {
                    categories.insert("coordinated_mint_or_distribution".to_owned());
                }
                EventKind::Transfer => {
                    categories.insert("coordinated_mint_or_distribution".to_owned());
                }
                EventKind::Listing => {
                    categories.insert("abnormal_listing_or_market_preparation".to_owned());
                }
                _ => {}
            }
        }
        if duplicate_content_timestamp.is_some_and(|timestamp| timestamp < result) {
            categories.insert("content_copy".to_owned());
        }
    }
    let early_signal_positive = match (deployment_timestamp, first_result) {
        (Some(deployment), Some(result))
            if result >= deployment
                && history_complete
                && relevant_timing_complete
                && (categories.len() >= 2 || duplicate_content_timestamp.is_some()) =>
        {
            Some(categories.len() >= 2)
        }
        _ => None,
    };
    LifecycleFacts {
        deployment_timestamp,
        first_activity_timestamp,
        first_mint_timestamp,
        first_transfer_timestamp,
        first_sale_timestamp,
        first_victim_timestamp,
        deployment_to_first_transfer_seconds: elapsed(
            deployment_timestamp,
            first_transfer_timestamp,
        ),
        deployment_to_first_sale_seconds: elapsed(deployment_timestamp, first_sale_timestamp),
        deployment_to_first_victim_seconds: elapsed(deployment_timestamp, first_victim_timestamp),
        first_activity_to_first_victim_seconds: elapsed(
            first_activity_timestamp,
            first_victim_timestamp,
        ),
        first_victim_holding_seconds: elapsed(first_victim_timestamp, Some(analysis_timestamp)),
        early_signal_categories: categories.into_iter().collect(),
        early_signal_positive,
    }
}

fn minimum_time(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn elapsed(start: Option<i64>, end: Option<i64>) -> Option<i64> {
    start
        .zip(end)
        .and_then(|(start, end)| end.checked_sub(start))
        .filter(|duration| *duration >= 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ChainId;
    use std::sync::Arc;

    fn event(index: u32, timestamp: i64, kind: EventKind) -> NormalizedEvent {
        NormalizedEvent {
            chain: ChainId::Ethereum,
            tx_id: Arc::from(format!("tx-{index}")),
            event_index: index,
            timestamp: Some(timestamp),
            block_number: None,
            kind,
            channel: None,
            from: Some(Arc::from("from")),
            to: Some(Arc::from("to")),
            fee_payer: None,
            payment_payer: None,
            payment_recipient: None,
            nft: None,
            native_amount: None,
            usd_micros: None,
            gas_native: None,
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        }
    }

    #[test]
    fn content_copy_without_a_pre_result_timestamp_is_not_assumed_early() {
        let facts = build_lifecycle(
            Some(0),
            None,
            &[
                event(0, 10, EventKind::Funding),
                event(1, 20, EventKind::Sale),
            ],
            true,
            30,
        );
        assert_eq!(facts.early_signal_positive, None);
    }

    #[test]
    fn two_independent_pre_result_signals_are_positive() {
        let mut sale = event(2, 20, EventKind::Sale);
        sale.native_amount = Some(1);
        let facts = build_lifecycle(
            Some(0),
            None,
            &[
                event(0, 5, EventKind::Funding),
                event(1, 10, EventKind::Listing),
                sale,
            ],
            true,
            30,
        );
        assert_eq!(facts.early_signal_positive, Some(true));
        assert_eq!(facts.first_victim_holding_seconds, Some(10));
    }
}
