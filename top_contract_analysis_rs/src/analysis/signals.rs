use std::collections::{HashMap, HashSet};

use crate::models::{AddressSignals, TransferRecord, ZERO_ADDRESS};

fn canonical_pair(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.to_string(), right.to_string())
    } else {
        (right.to_string(), left.to_string())
    }
}

pub fn analyze_transfer_signals(transfers: &[TransferRecord]) -> AddressSignals {
    let mut mint_recipients: HashSet<String> = HashSet::new();
    let mut receiver_addresses: HashSet<String> = HashSet::new();
    let mut seen_pairs: HashSet<(String, String)> = HashSet::new();
    let mut cycle_pairs: HashSet<(String, String)> = HashSet::new();
    let mut outgoing: HashMap<String, HashSet<String>> = HashMap::new();
    let mut incoming: HashMap<String, usize> = HashMap::new();
    let mut mint_count = 0_usize;
    let mut first_mint_time = 0_i64;
    let mut first_non_mint_time = 0_i64;

    for transfer in transfers {
        if !transfer.to_address.is_empty() && transfer.to_address != ZERO_ADDRESS {
            receiver_addresses.insert(transfer.to_address.clone());
        }
        if transfer.from_address == ZERO_ADDRESS {
            mint_count += 1;
            if !transfer.to_address.is_empty() {
                mint_recipients.insert(transfer.to_address.clone());
            }
            if transfer.block_time > 0
                && (first_mint_time == 0 || transfer.block_time < first_mint_time)
            {
                first_mint_time = transfer.block_time;
            }
            continue;
        }

        if transfer.block_time > 0
            && (first_non_mint_time == 0 || transfer.block_time < first_non_mint_time)
        {
            first_non_mint_time = transfer.block_time;
        }
        if transfer.to_address != ZERO_ADDRESS {
            outgoing
                .entry(transfer.from_address.clone())
                .or_default()
                .insert(transfer.to_address.clone());
            *incoming.entry(transfer.to_address.clone()).or_insert(0) += 1;
            let pair = (transfer.from_address.clone(), transfer.to_address.clone());
            let reverse = (transfer.to_address.clone(), transfer.from_address.clone());
            if seen_pairs.contains(&reverse) {
                cycle_pairs.insert(canonical_pair(&transfer.from_address, &transfer.to_address));
            }
            seen_pairs.insert(pair);
        }
    }

    let star_distributor_count = outgoing
        .iter()
        .filter(|(sender, recipients)| {
            recipients.len() >= 3 && *incoming.get(*sender).unwrap_or(&0) <= 1
        })
        .count();

    let mint_to_first_transfer_seconds =
        if first_mint_time > 0 && first_non_mint_time >= first_mint_time {
            Some(first_non_mint_time - first_mint_time)
        } else {
            None
        };
    let fast_spread = mint_to_first_transfer_seconds
        .map(|delay| delay > 0 && delay <= 24 * 3600)
        .unwrap_or(false);

    AddressSignals {
        mint_address_count: mint_recipients.len(),
        mint_count,
        unique_receiver_count: receiver_addresses.len(),
        cycle_edge_count: cycle_pairs.len(),
        star_distributor_count,
        mint_to_first_transfer_seconds,
        fast_spread,
    }
}
