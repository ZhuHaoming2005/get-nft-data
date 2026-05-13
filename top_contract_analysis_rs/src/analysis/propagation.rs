use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::models::{
    HonestAddressPayload, InfringingTokenRecord, MaliciousAddressPayload,
    NftPropagationEdgePayload, NftPropagationNodePayload, NftPropagationPathPayload,
    NftPropagationSummaryPayload, NftSaleRecord, NftTokenPropagationPayload, OwnerBalance,
    SecondarySaleVictimAddressPayload, TransferRecord, ZERO_ADDRESS,
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeAggregationKey {
    contract_address: String,
    channel: String,
    from_address: String,
    to_address: String,
    event_type: String,
    marketplace: String,
}

struct EdgeAccumulator {
    edge: NftPropagationEdgePayload,
    token_ids: BTreeSet<String>,
    tx_hashes: BTreeSet<String>,
}

impl EdgeAccumulator {
    fn new(mut edge: NftPropagationEdgePayload) -> Self {
        edge.aggregate_count = 1;
        let mut token_ids = BTreeSet::new();
        if !edge.token_id.is_empty() {
            token_ids.insert(edge.token_id.clone());
        }
        let mut tx_hashes = BTreeSet::new();
        if !edge.tx_hash.is_empty() {
            tx_hashes.insert(edge.tx_hash.clone());
        }
        Self {
            edge,
            token_ids,
            tx_hashes,
        }
    }

    fn absorb(&mut self, edge: &NftPropagationEdgePayload) {
        self.edge.aggregate_count += 1;
        if !edge.token_id.is_empty() {
            self.token_ids.insert(edge.token_id.clone());
        }
        if !edge.tx_hash.is_empty() {
            self.tx_hashes.insert(edge.tx_hash.clone());
        }
        if edge.block_number > self.edge.block_number {
            self.edge.last_block_number = Some(edge.block_number);
        }
        if edge.block_time > self.edge.block_time {
            self.edge.last_block_time = Some(edge.block_time);
        }
        if self.edge.seconds_since_mint.is_none() {
            self.edge.seconds_since_mint = edge.seconds_since_mint;
        }
    }

    fn finish(mut self, key: &EdgeAggregationKey) -> NftPropagationEdgePayload {
        if self.edge.aggregate_count > 1 {
            self.edge.edge_id = format!(
                "aggregate:{}:{}:{}:{}:{}",
                key.channel, key.contract_address, key.from_address, key.to_address, key.event_type
            );
            self.edge.token_ids = self.token_ids.into_iter().collect();
            self.edge.tx_hashes = self.tx_hashes.into_iter().take(50).collect();
            self.edge.last_block_number =
                self.edge.last_block_number.or(Some(self.edge.block_number));
            self.edge.last_block_time = self.edge.last_block_time.or(Some(self.edge.block_time));
        }
        self.edge
    }
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

fn positive_amount(value: f64) -> Option<f64> {
    (value > 0.0).then_some(value)
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

fn sale_transfer_key(
    tx_hash: &str,
    token_id: &str,
    from_address: &str,
    to_address: &str,
) -> Option<(String, String, String, String)> {
    let tx_hash = tx_hash.trim().to_lowercase();
    let token_id = token_id.trim().to_string();
    let from_address = canonical_address(from_address);
    let to_address = canonical_address(to_address);
    if tx_hash.is_empty() || token_id.is_empty() || from_address.is_empty() || to_address.is_empty()
    {
        None
    } else {
        Some((tx_hash, token_id, from_address, to_address))
    }
}

fn edge_aggregation_key(edge: &NftPropagationEdgePayload) -> Option<EdgeAggregationKey> {
    if !matches!(edge.channel.as_str(), "mint" | "transfer") {
        return None;
    }
    Some(EdgeAggregationKey {
        contract_address: edge.contract_address.clone(),
        channel: edge.channel.clone(),
        from_address: edge.from_address.clone(),
        to_address: edge.to_address.clone(),
        event_type: edge.event_type.clone(),
        marketplace: edge.marketplace.clone(),
    })
}

fn aggregate_transfer_like_edges(
    edges: Vec<NftPropagationEdgePayload>,
) -> Vec<NftPropagationEdgePayload> {
    let mut grouped = BTreeMap::<EdgeAggregationKey, EdgeAccumulator>::new();
    let mut passthrough = Vec::new();

    for edge in edges {
        if let Some(key) = edge_aggregation_key(&edge) {
            grouped
                .entry(key)
                .and_modify(|accumulator| accumulator.absorb(&edge))
                .or_insert_with(|| EdgeAccumulator::new(edge));
        } else {
            passthrough.push(edge);
        }
    }

    passthrough.extend(
        grouped
            .into_iter()
            .map(|(key, accumulator)| accumulator.finish(&key)),
    );
    passthrough
}

fn edge_contains_token(edge: &NftPropagationEdgePayload, token_id: &str) -> bool {
    edge.token_id == token_id || edge.token_ids.iter().any(|item| item == token_id)
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

pub struct NftPropagationInput<'a> {
    pub contract_address: &'a str,
    pub transfers: &'a [TransferRecord],
    pub sales: &'a [NftSaleRecord],
    pub owners: &'a [OwnerBalance],
    pub infringing_tokens: &'a [InfringingTokenRecord],
    pub malicious_addresses: &'a [MaliciousAddressPayload],
    pub honest_addresses: &'a [HonestAddressPayload],
    pub secondary_sale_victim_addresses: &'a [SecondarySaleVictimAddressPayload],
}

pub fn build_nft_propagation_path(input: NftPropagationInput<'_>) -> NftPropagationPathPayload {
    let NftPropagationInput {
        contract_address,
        transfers,
        sales,
        owners,
        infringing_tokens,
        malicious_addresses,
        honest_addresses,
        secondary_sale_victim_addresses,
    } = input;
    let relevant_token_ids: HashSet<String> = infringing_tokens
        .iter()
        .filter(|item| !item.token_id.is_empty())
        .map(|item| item.token_id.clone())
        .collect();
    let mut nodes = BTreeMap::<String, NodeAccumulator>::new();
    let mut transfer_edges = Vec::new();
    let mut sale_edges = Vec::new();
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
        if item.mint_activity_observed {
            add_role(&mut nodes, &item.address, "mint_activity_observed");
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
    for item in secondary_sale_victim_addresses {
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
        transfer_edges.push(NftPropagationEdgePayload {
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
            payment_token_symbol: String::new(),
            payment_token_address: String::new(),
            price_eth: None,
            price_usd: None,
            seller_fee_eth: None,
            seller_fee_usd: None,
            protocol_fee_eth: None,
            protocol_fee_usd: None,
            royalty_fee_eth: None,
            royalty_fee_usd: None,
            royalty_recipient_address: String::new(),
            seconds_since_mint: seconds_since_mint(
                &mint_time_by_token,
                &transfer.token_id,
                transfer.block_time,
            ),
            aggregate_count: 1,
            token_ids: vec![],
            tx_hashes: vec![],
            last_block_number: None,
            last_block_time: None,
            merged_transfer: false,
            underlying_channels: vec![channel.to_string()],
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

    let transfer_keys: HashSet<(String, String, String, String)> = transfer_edges
        .iter()
        .filter_map(|edge| {
            sale_transfer_key(
                &edge.tx_hash,
                &edge.token_id,
                &edge.from_address,
                &edge.to_address,
            )
        })
        .collect();
    let mut merged_transfer_keys = HashSet::<(String, String, String, String)>::new();

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
        let transfer_key =
            sale_transfer_key(&sale.tx_hash, &sale.token_id, &from_address, &to_address);
        let merged_transfer = transfer_key
            .as_ref()
            .map(|key| transfer_keys.contains(key))
            .unwrap_or(false);
        if let Some(key) = transfer_key.filter(|_| merged_transfer) {
            merged_transfer_keys.insert(key);
        }
        sale_edges.push(NftPropagationEdgePayload {
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
            payment_token_symbol: sale.payment_token_symbol.clone(),
            payment_token_address: sale.payment_token_address.clone(),
            price_eth: sale.price_eth,
            price_usd: sale.price_usd,
            seller_fee_eth: positive_amount(sale.seller_fee_eth),
            seller_fee_usd: positive_amount(sale.seller_fee_usd),
            protocol_fee_eth: positive_amount(sale.protocol_fee_eth),
            protocol_fee_usd: positive_amount(sale.protocol_fee_usd),
            royalty_fee_eth: positive_amount(sale.royalty_fee_eth),
            royalty_fee_usd: positive_amount(sale.royalty_fee_usd),
            royalty_recipient_address: canonical_address(&sale.royalty_recipient_address),
            seconds_since_mint: seconds_since_mint(&mint_time_by_token, &sale.token_id, block_time),
            aggregate_count: 1,
            token_ids: vec![],
            tx_hashes: vec![],
            last_block_number: None,
            last_block_time: None,
            merged_transfer,
            underlying_channels: if merged_transfer {
                vec!["sale".into(), "transfer".into()]
            } else {
                vec!["sale".into()]
            },
        });
    }

    let mut edges: Vec<NftPropagationEdgePayload> = transfer_edges
        .into_iter()
        .filter(|edge| {
            sale_transfer_key(
                &edge.tx_hash,
                &edge.token_id,
                &edge.from_address,
                &edge.to_address,
            )
            .map(|key| !merged_transfer_keys.contains(&key))
            .unwrap_or(true)
        })
        .collect();
    edges.extend(sale_edges);
    edges = aggregate_transfer_like_edges(edges);

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
                .filter(|edge| edge_contains_token(edge, &token.token_id))
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
