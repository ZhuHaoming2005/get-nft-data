use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::models::{
    HonestAddressPayload, InfringingTokenRecord, MaliciousAddressPayload,
    NftPropagationEdgePayload, NftPropagationNodePayload, NftPropagationPathPayload,
    NftPropagationSummaryPayload, NftSaleRecord, NftTokenPropagationPayload, OwnerBalance,
    TransferRecord, VictimAddressPayload, ZERO_ADDRESS,
};

#[derive(Default)]
struct NodeAccumulator {
    roles: BTreeSet<String>,
    minted_token_count: i64,
    bought_token_count: i64,
    sold_token_count: i64,
    received_transfer_count: i64,
    sent_transfer_count: i64,
    current_holding_token_count: i64,
    total_buy_eth: f64,
    total_buy_usd: f64,
    is_stuck_victim: bool,
}

fn transfer_sort_key(transfer: &TransferRecord) -> (i64, i64, &str) {
    (
        transfer.block_number,
        transfer.log_index,
        transfer.tx_hash.as_str(),
    )
}

fn sale_sort_key(sale: &NftSaleRecord) -> (i64, i64, i64, &str) {
    (
        sale.block_number,
        sale.log_index,
        sale.bundle_index,
        sale.tx_hash.as_str(),
    )
}

fn canonical_address(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
        trimmed.to_lowercase()
    } else {
        trimmed.to_string()
    }
}

fn node_mut<'a>(
    nodes: &'a mut BTreeMap<String, NodeAccumulator>,
    address: &str,
) -> Option<&'a mut NodeAccumulator> {
    let address = canonical_address(address);
    if address.is_empty() {
        return None;
    }
    Some(nodes.entry(address).or_default())
}

fn add_role(nodes: &mut BTreeMap<String, NodeAccumulator>, address: &str, role: &str) {
    if let Some(node) = node_mut(nodes, address) {
        node.roles.insert(role.to_string());
    }
}

fn seconds_since_mint(
    mint_time_by_token: &HashMap<String, i64>,
    token_id: &str,
    block_time: i64,
) -> Option<i64> {
    let mint_time = *mint_time_by_token.get(token_id).unwrap_or(&0);
    (mint_time > 0 && block_time >= mint_time).then_some(block_time - mint_time)
}

fn edge_block_time_from_transfer(
    transfer_time_by_tx_token: &HashMap<(String, String), i64>,
    sale: &NftSaleRecord,
) -> i64 {
    transfer_time_by_tx_token
        .get(&(sale.tx_hash.clone(), sale.token_id.clone()))
        .copied()
        .unwrap_or_default()
}

fn current_holders_by_token(
    owners: &[OwnerBalance],
    relevant_token_ids: &HashSet<String>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut holders = BTreeMap::<String, BTreeSet<String>>::new();
    for owner in owners {
        let owner_address = canonical_address(&owner.owner_address);
        if owner_address.is_empty() || owner_address == ZERO_ADDRESS {
            continue;
        }
        for (token_id, balance) in &owner.token_balances {
            if *balance <= 0 || !relevant_token_ids.contains(token_id) {
                continue;
            }
            holders
                .entry(token_id.clone())
                .or_default()
                .insert(owner_address.clone());
        }
    }
    holders
}

fn build_summary(
    nodes: &BTreeMap<String, NftPropagationNodePayload>,
    edges: &[NftPropagationEdgePayload],
    token_count: usize,
) -> NftPropagationSummaryPayload {
    let blocks: Vec<i64> = edges
        .iter()
        .filter_map(|edge| (edge.block_number > 0).then_some(edge.block_number))
        .collect();
    let times: Vec<i64> = edges
        .iter()
        .filter_map(|edge| (edge.block_time > 0).then_some(edge.block_time))
        .collect();

    NftPropagationSummaryPayload {
        token_count: token_count as i64,
        node_count: nodes.len() as i64,
        edge_count: edges.len() as i64,
        mint_edge_count: edges.iter().filter(|edge| edge.channel == "mint").count() as i64,
        transfer_edge_count: edges
            .iter()
            .filter(|edge| edge.channel == "transfer")
            .count() as i64,
        sale_edge_count: edges.iter().filter(|edge| edge.channel == "sale").count() as i64,
        malicious_node_count: nodes
            .values()
            .filter(|node| node.roles.iter().any(|role| role == "malicious"))
            .count() as i64,
        victim_node_count: nodes
            .values()
            .filter(|node| node.roles.iter().any(|role| role == "victim_buyer"))
            .count() as i64,
        honest_node_count: nodes
            .values()
            .filter(|node| node.roles.iter().any(|role| role == "honest_holder"))
            .count() as i64,
        stuck_victim_node_count: nodes.values().filter(|node| node.is_stuck_victim).count() as i64,
        first_block_number: blocks.iter().min().copied().unwrap_or_default(),
        last_block_number: blocks.iter().max().copied().unwrap_or_default(),
        first_block_time: times.iter().min().copied().unwrap_or_default(),
        last_block_time: times.iter().max().copied().unwrap_or_default(),
    }
}

pub fn build_nft_propagation_path(
    contract_address: &str,
    transfers: &[TransferRecord],
    sales: &[NftSaleRecord],
    owners: &[OwnerBalance],
    infringing_tokens: &[InfringingTokenRecord],
    malicious_addresses: &[MaliciousAddressPayload],
    honest_addresses: &[HonestAddressPayload],
    victim_addresses: &[VictimAddressPayload],
) -> NftPropagationPathPayload {
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter_map(|item| (!item.token_id.is_empty()).then(|| item.token_id.clone()))
        .collect();
    let mut nodes = BTreeMap::<String, NodeAccumulator>::new();
    let mut edges = Vec::new();
    let holders_by_token = current_holders_by_token(owners, &relevant_token_ids);

    for token in infringing_tokens {
        add_role(&mut nodes, &token.minter_address, "minter");
        if token.official_or_legit_reissue {
            add_role(&mut nodes, &token.minter_address, "official_reissue");
        }
    }
    for address in holders_by_token.values().flatten() {
        if let Some(node) = node_mut(&mut nodes, address) {
            node.roles.insert("current_holder".into());
            node.current_holding_token_count += 1;
        }
    }
    for item in malicious_addresses {
        add_role(&mut nodes, &item.address, "malicious");
        if item.mint_role {
            add_role(&mut nodes, &item.address, "mint_role");
        }
        if item.wash_cycle_count > 0 {
            add_role(&mut nodes, &item.address, "wash_cycle");
        }
        if item.star_out_degree >= 3 {
            add_role(&mut nodes, &item.address, "star_distributor");
        }
        if !item.rapid_spread_contracts.is_empty() {
            add_role(&mut nodes, &item.address, "rapid_spreader");
        }
    }
    for item in honest_addresses {
        if let Some(node) = node_mut(&mut nodes, &item.address) {
            node.roles.insert("honest_holder".into());
            if item.is_corrupted_address {
                node.roles.insert("corrupted_honest".into());
            }
            node.current_holding_token_count = node
                .current_holding_token_count
                .max(item.currently_holding_token_count);
        }
    }
    for item in victim_addresses {
        if let Some(node) = node_mut(&mut nodes, &item.address) {
            node.roles.insert("victim_buyer".into());
            node.is_stuck_victim |= item.is_stuck;
            if item.is_stuck {
                node.roles.insert("stuck_victim".into());
            }
        }
    }

    let mut sorted_transfers: Vec<&TransferRecord> = transfers
        .iter()
        .filter(|transfer| {
            transfer
                .contract_address
                .eq_ignore_ascii_case(contract_address)
                && relevant_token_ids.contains(&transfer.token_id)
        })
        .collect();
    sorted_transfers.sort_by(|left, right| transfer_sort_key(left).cmp(&transfer_sort_key(right)));

    let mut mint_time_by_token = HashMap::<String, i64>::new();
    let mut transfer_time_by_tx_token = HashMap::<(String, String), i64>::new();
    for transfer in &sorted_transfers {
        transfer_time_by_tx_token.insert(
            (transfer.tx_hash.clone(), transfer.token_id.clone()),
            transfer.block_time,
        );
        if transfer.from_address == ZERO_ADDRESS && transfer.block_time > 0 {
            mint_time_by_token
                .entry(transfer.token_id.clone())
                .or_insert(transfer.block_time);
        }
    }

    for (index, transfer) in sorted_transfers.iter().enumerate() {
        let from_address = canonical_address(&transfer.from_address);
        let to_address = canonical_address(&transfer.to_address);
        let channel = if from_address == ZERO_ADDRESS {
            "mint"
        } else {
            "transfer"
        };
        if let Some(node) = node_mut(&mut nodes, &from_address) {
            node.roles.insert(if channel == "mint" {
                "mint_source".into()
            } else {
                "sender".into()
            });
            node.sent_transfer_count += 1;
        }
        if let Some(node) = node_mut(&mut nodes, &to_address) {
            node.roles.insert(if channel == "mint" {
                "mint_receiver".into()
            } else {
                "receiver".into()
            });
            node.received_transfer_count += 1;
            if channel == "mint" {
                node.minted_token_count += 1;
            }
        }
        edges.push(NftPropagationEdgePayload {
            edge_id: format!(
                "{}:{}:{}:{}:{}",
                channel, transfer.tx_hash, transfer.log_index, transfer.token_id, index
            ),
            contract_address: contract_address.to_string(),
            token_id: transfer.token_id.clone(),
            from_address,
            to_address,
            tx_hash: transfer.tx_hash.clone(),
            block_number: transfer.block_number,
            block_time: transfer.block_time,
            log_index: transfer.log_index,
            event_type: transfer.event_type.clone(),
            channel: channel.to_string(),
            marketplace: String::new(),
            price_eth: None,
            price_usd: None,
            seconds_since_mint: seconds_since_mint(
                &mint_time_by_token,
                &transfer.token_id,
                transfer.block_time,
            ),
        });
    }

    let mut sorted_sales: Vec<&NftSaleRecord> = sales
        .iter()
        .filter(|sale| {
            sale.contract_address.eq_ignore_ascii_case(contract_address)
                && relevant_token_ids.contains(&sale.token_id)
        })
        .collect();
    sorted_sales.sort_by(|left, right| sale_sort_key(left).cmp(&sale_sort_key(right)));

    for (index, sale) in sorted_sales.iter().enumerate() {
        let from_address = canonical_address(&sale.seller_address);
        let to_address = canonical_address(&sale.buyer_address);
        if let Some(node) = node_mut(&mut nodes, &from_address) {
            node.roles.insert("seller".into());
            node.sold_token_count += 1;
        }
        if let Some(node) = node_mut(&mut nodes, &to_address) {
            node.roles.insert("buyer".into());
            node.bought_token_count += 1;
            node.total_buy_eth += sale.price_eth.unwrap_or(0.0);
            node.total_buy_usd += sale.price_usd.unwrap_or(0.0);
        }
        let block_time = edge_block_time_from_transfer(&transfer_time_by_tx_token, sale);
        edges.push(NftPropagationEdgePayload {
            edge_id: format!(
                "sale:{}:{}:{}:{}",
                sale.tx_hash, sale.log_index, sale.bundle_index, index
            ),
            contract_address: contract_address.to_string(),
            token_id: sale.token_id.clone(),
            from_address,
            to_address,
            tx_hash: sale.tx_hash.clone(),
            block_number: sale.block_number,
            block_time,
            log_index: sale.log_index,
            event_type: "sale".into(),
            channel: "sale".into(),
            marketplace: sale.marketplace.clone(),
            price_eth: sale.price_eth,
            price_usd: sale.price_usd,
            seconds_since_mint: seconds_since_mint(&mint_time_by_token, &sale.token_id, block_time),
        });
    }

    edges.sort_by(|left, right| {
        (
            left.block_number,
            left.block_time,
            left.log_index,
            left.channel.as_str(),
            left.tx_hash.as_str(),
        )
            .cmp(&(
                right.block_number,
                right.block_time,
                right.log_index,
                right.channel.as_str(),
                right.tx_hash.as_str(),
            ))
    });

    let nodes: BTreeMap<String, NftPropagationNodePayload> = nodes
        .into_iter()
        .map(|(address, node)| {
            (
                address.clone(),
                NftPropagationNodePayload {
                    address,
                    roles: node.roles.into_iter().collect(),
                    minted_token_count: node.minted_token_count,
                    bought_token_count: node.bought_token_count,
                    sold_token_count: node.sold_token_count,
                    received_transfer_count: node.received_transfer_count,
                    sent_transfer_count: node.sent_transfer_count,
                    current_holding_token_count: node.current_holding_token_count,
                    total_buy_eth: node.total_buy_eth,
                    total_buy_usd: node.total_buy_usd,
                    is_stuck_victim: node.is_stuck_victim,
                },
            )
        })
        .collect();

    let mut token_paths = Vec::new();
    for token in infringing_tokens {
        let current_holder_addresses = holders_by_token
            .get(&token.token_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let buyer_addresses = sorted_sales
            .iter()
            .filter(|sale| sale.token_id == token.token_id)
            .filter_map(|sale| {
                let address = canonical_address(&sale.buyer_address);
                (!address.is_empty()).then_some(address)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let seller_addresses = sorted_sales
            .iter()
            .filter(|sale| sale.token_id == token.token_id)
            .filter_map(|sale| {
                let address = canonical_address(&sale.seller_address);
                (!address.is_empty()).then_some(address)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        token_paths.push(NftTokenPropagationPayload {
            token_id: token.token_id.clone(),
            match_reasons: token.match_reasons.clone(),
            minter_address: canonical_address(&token.minter_address),
            mint_tx_hash: token.mint_tx_hash.clone(),
            mint_block: token.mint_block,
            mint_time: mint_time_by_token
                .get(&token.token_id)
                .copied()
                .unwrap_or_default(),
            first_transfer_time: token.first_transfer_time,
            current_holder_addresses,
            buyer_addresses,
            seller_addresses,
            edge_count: edges
                .iter()
                .filter(|edge| edge.token_id == token.token_id)
                .count() as i64,
            sale_count: sorted_sales
                .iter()
                .filter(|sale| sale.token_id == token.token_id)
                .count() as i64,
        });
    }
    token_paths.sort_by(|left, right| left.token_id.cmp(&right.token_id));

    NftPropagationPathPayload {
        contract_address: contract_address.to_string(),
        summary: build_summary(&nodes, &edges, relevant_token_ids.len()),
        nodes,
        edges,
        token_paths,
    }
}
