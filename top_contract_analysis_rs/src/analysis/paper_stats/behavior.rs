use super::*;

#[derive(Default)]
struct BehaviorMeasure {
    instance_count: i64,
    address_count: i64,
    nft_count: i64,
    buyer_count: i64,
    linked_loss_eth: f64,
    linked_loss_usd: f64,
}

#[derive(Default)]
pub(super) struct ContractBehaviorBuild {
    pub(super) stats: PaperContractBehaviorStatsPayload,
    pub(super) behavior_contracts: BTreeMap<String, BTreeSet<String>>,
    pub(super) behavior_addresses: BTreeMap<String, BTreeSet<String>>,
    pub(super) behavior_nfts: BTreeMap<String, BTreeSet<String>>,
    pub(super) behavior_buyers: BTreeMap<String, BTreeSet<String>>,
}

struct PumpExitPattern {
    row: PaperPumpExitRowPayload,
    addresses: BTreeSet<String>,
    token_ids: BTreeSet<String>,
    buyers: BTreeSet<String>,
}

struct StarBehaviorPattern {
    row: PaperStarBehaviorRowPayload,
    addresses: BTreeSet<String>,
    token_ids: BTreeSet<String>,
    buyers: BTreeSet<String>,
}

#[derive(Default)]
struct StarBehaviorBuild {
    centers: BTreeSet<usize>,
    edge_count: i64,
    wallets: BTreeSet<String>,
    token_ids: BTreeSet<String>,
    fanout_total: usize,
    total_value_eth: f64,
    total_value_usd: f64,
    buyers: BTreeSet<String>,
    linked_loss_eth: f64,
    linked_loss_usd: f64,
}

struct LayeredTransferPattern {
    row: PaperLayeredTransferRowPayload,
    addresses: BTreeSet<String>,
    token_ids: BTreeSet<String>,
}

struct InventoryConcentrationPattern {
    row: PaperInventoryConcentrationRowPayload,
    addresses: BTreeSet<String>,
    token_ids: BTreeSet<String>,
}

struct CyclePattern {
    row: PaperWashTradingRowPayload,
    participants: BTreeSet<String>,
    token_ids: BTreeSet<String>,
    max_block_time: i64,
    avg_price_eth: Option<f64>,
    avg_price_usd: Option<f64>,
}

pub(super) fn build_contract_behavior_stats(
    input: &PaperStatsInput<'_>,
    address_sets: &AddressSets,
) -> Vec<ContractBehaviorBuild> {
    input
        .nft_propagation_paths
        .par_iter()
        .filter_map(|(contract_key, path)| {
            let contract_address = if path.contract_address.trim().is_empty() {
                normalized_contract(contract_key)
            } else {
                normalized_contract(&path.contract_address)
            };
            let cycles = detect_wash_trading(&contract_address, path, address_sets, input.config);
            let mut behavior_contracts = BTreeMap::<String, BTreeSet<String>>::new();
            let mut behavior_addresses = BTreeMap::<String, BTreeSet<String>>::new();
            let mut behavior_nfts = BTreeMap::<String, BTreeSet<String>>::new();
            let mut behavior_buyers = BTreeMap::<String, BTreeSet<String>>::new();
            for cycle in &cycles {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    "Wash Trading",
                    cycle.participants.iter().cloned(),
                    cycle.token_ids.iter().cloned(),
                );
            }
            let wash_trading: Vec<PaperWashTradingRowPayload> =
                cycles.iter().map(|cycle| cycle.row.clone()).collect();
            let wash_cycle_size_distribution = wash_cycle_size_distribution_for_rows(&wash_trading);
            let pump_and_exit_patterns = detect_pump_and_exit(path, address_sets, &cycles);
            let mut source_patterns_by_buyer = BTreeMap::<String, BTreeSet<String>>::new();
            for pattern in &pump_and_exit_patterns {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    "Pump-and-Exit",
                    pattern.addresses.iter().cloned(),
                    pattern.token_ids.iter().cloned(),
                );
                insert_behavior_buyers(
                    &mut behavior_buyers,
                    "Pump-and-Exit",
                    pattern.buyers.iter().cloned(),
                );
                for buyer in &pattern.buyers {
                    source_patterns_by_buyer
                        .entry(buyer.clone())
                        .or_default()
                        .insert("Pump-and-Exit".into());
                }
            }
            let pump_and_exit = pump_and_exit_patterns
                .iter()
                .map(|pattern| pattern.row.clone())
                .collect();
            let star_behavior_patterns = detect_star_behaviors(path, address_sets, input.config);
            for pattern in &star_behavior_patterns {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    &pattern.row.behavior,
                    pattern.addresses.iter().cloned(),
                    pattern.token_ids.iter().cloned(),
                );
                insert_behavior_buyers(
                    &mut behavior_buyers,
                    &pattern.row.behavior,
                    pattern.buyers.iter().cloned(),
                );
                for buyer in &pattern.buyers {
                    source_patterns_by_buyer
                        .entry(buyer.clone())
                        .or_default()
                        .insert(pattern.row.behavior.clone());
                }
            }
            let star_behaviors = star_behavior_patterns
                .iter()
                .map(|pattern| pattern.row.clone())
                .collect();
            let layered_transfer_patterns =
                detect_layered_transfers(&contract_address, path, input.config);
            for pattern in &layered_transfer_patterns {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    "Layered Transfer",
                    pattern.addresses.iter().cloned(),
                    pattern.token_ids.iter().cloned(),
                );
            }
            let layered_transfers = layered_transfer_patterns
                .iter()
                .map(|pattern| pattern.row.clone())
                .collect();
            let inventory_concentration =
                detect_inventory_concentration(path, address_sets, input.config);
            for pattern in &inventory_concentration {
                insert_behavior_keys(
                    &contract_address,
                    &mut behavior_contracts,
                    &mut behavior_addresses,
                    &mut behavior_nfts,
                    "Inventory Concentration",
                    pattern.addresses.iter().cloned(),
                    pattern.token_ids.iter().cloned(),
                );
            }
            let inventory_concentration = inventory_concentration
                .iter()
                .map(|pattern| pattern.row.clone())
                .collect();
            let honest_buyers =
                honest_buyers(input, &contract_address, path, &source_patterns_by_buyer);

            let stats = PaperContractBehaviorStatsPayload {
                contract_address,
                wash_trading,
                wash_cycle_size_distribution,
                pump_and_exit,
                star_behaviors,
                layered_transfers,
                inventory_concentration,
                honest_buyers,
            };
            (!stats.wash_trading.is_empty()
                || !stats.pump_and_exit.is_empty()
                || !stats.star_behaviors.is_empty()
                || !stats.layered_transfers.is_empty()
                || !stats.inventory_concentration.is_empty()
                || !stats.honest_buyers.is_empty())
            .then_some(ContractBehaviorBuild {
                stats,
                behavior_contracts,
                behavior_addresses,
                behavior_nfts,
                behavior_buyers,
            })
        })
        .collect()
}

fn detect_wash_trading(
    contract_address: &str,
    path: &NftPropagationPathPayload,
    address_sets: &AddressSets,
    config: PaperStatsConfig,
) -> Vec<CyclePattern> {
    let mut adjacency = BTreeMap::<String, BTreeSet<String>>::new();
    let mut sale_edges = Vec::<&NftPropagationEdgePayload>::new();
    for edge in path.edges.iter().filter(|edge| edge.channel == "sale") {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        if !address_sets.malicious.contains(&from) || !address_sets.malicious.contains(&to) {
            continue;
        }
        if !is_participant_address(&from) || !is_participant_address(&to) {
            continue;
        }
        adjacency
            .entry(from.clone())
            .or_default()
            .insert(to.clone());
        adjacency.entry(to).or_default();
        sale_edges.push(edge);
    }

    let components = strongly_connected_components(&adjacency);
    let mut component_by_address = BTreeMap::<String, usize>::new();
    for (index, component) in components.iter().enumerate() {
        for address in component {
            component_by_address.insert(address.clone(), index);
        }
    }

    let mut edges_by_component = BTreeMap::<usize, Vec<&NftPropagationEdgePayload>>::new();
    for edge in sale_edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        let (Some(from_component), Some(to_component)) = (
            component_by_address.get(&from).copied(),
            component_by_address.get(&to).copied(),
        ) else {
            continue;
        };
        if from_component == to_component {
            edges_by_component
                .entry(from_component)
                .or_default()
                .push(edge);
        }
    }

    let mut cycles = Vec::new();
    for (component_index, all_edges) in edges_by_component {
        let component = &components[component_index];
        if component.len() < config.min_cycle_size() {
            continue;
        }
        if all_edges.is_empty() {
            continue;
        }
        let token_ids = all_edges
            .iter()
            .flat_map(|edge| edge_token_ids(edge))
            .collect::<BTreeSet<_>>();
        let block_times = all_edges
            .iter()
            .map(|edge| edge.block_time)
            .filter(|block_time| *block_time > 0)
            .collect::<Vec<_>>();
        let block_numbers = all_edges
            .iter()
            .map(|edge| edge.block_number)
            .filter(|block_number| *block_number > 0)
            .collect::<Vec<_>>();
        let fake_volume_eth: f64 = all_edges
            .iter()
            .map(|edge| edge.price_eth.unwrap_or_default())
            .sum();
        let fake_volume_usd: f64 = all_edges
            .iter()
            .map(|edge| edge.price_usd.unwrap_or_default())
            .sum();
        let avg_price_eth = average_optional(all_edges.iter().filter_map(|edge| edge.price_eth));
        let avg_price_usd = average_optional(all_edges.iter().filter_map(|edge| edge.price_usd));
        let token_counts = all_edges.iter().flat_map(|edge| edge_token_ids(edge)).fold(
            BTreeMap::<String, i64>::new(),
            |mut counts, token| {
                *counts.entry(token).or_default() += 1;
                counts
            },
        );
        let max_block_time = block_times.iter().max().copied().unwrap_or_default();
        let avg_cycle_blocks = match (block_numbers.iter().min(), block_numbers.iter().max()) {
            (Some(first), Some(last)) => Some((*last - *first).max(0) as f64),
            _ => None,
        };
        cycles.push(CyclePattern {
            row: PaperWashTradingRowPayload {
                cycle_id: format!("{contract_address}:wash:{}", cycles.len() + 1),
                participant_node_count: component.len() as i64,
                token_gini: gini(token_counts.values().map(|value| *value as f64).collect()),
                avg_cycle_blocks,
                fake_volume_eth,
                fake_volume_usd,
            },
            participants: component.clone(),
            token_ids,
            max_block_time,
            avg_price_eth,
            avg_price_usd,
        });
    }
    cycles
}

fn insert_behavior_keys(
    contract_address: &str,
    behavior_contracts: &mut BTreeMap<String, BTreeSet<String>>,
    behavior_addresses: &mut BTreeMap<String, BTreeSet<String>>,
    behavior_nfts: &mut BTreeMap<String, BTreeSet<String>>,
    behavior: &str,
    addresses: impl IntoIterator<Item = String>,
    token_ids: impl IntoIterator<Item = String>,
) {
    behavior_contracts
        .entry(behavior.to_string())
        .or_default()
        .insert(normalized_contract(contract_address));
    behavior_addresses
        .entry(behavior.to_string())
        .or_default()
        .extend(
            addresses
                .into_iter()
                .map(|address| normalized_address(&address))
                .filter(|address| is_participant_address(address)),
        );
    behavior_nfts
        .entry(behavior.to_string())
        .or_default()
        .extend(
            token_ids
                .into_iter()
                .filter_map(|token_id| behavior_nft_key(contract_address, &token_id)),
        );
}

fn insert_behavior_buyers(
    behavior_buyers: &mut BTreeMap<String, BTreeSet<String>>,
    behavior: &str,
    buyers: impl IntoIterator<Item = String>,
) {
    behavior_buyers
        .entry(behavior.to_string())
        .or_default()
        .extend(
            buyers
                .into_iter()
                .map(|address| normalized_address(&address))
                .filter(|address| is_participant_address(address)),
        );
}

fn behavior_nft_key(contract_address: &str, token_id: &str) -> Option<String> {
    let token_id = token_id.trim();
    (!token_id.is_empty()).then(|| format!("{}:{token_id}", normalized_contract(contract_address)))
}

fn strongly_connected_components(
    adjacency: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<BTreeSet<String>> {
    let nodes = adjacency
        .iter()
        .flat_map(|(node, neighbors)| {
            std::iter::once(node.clone()).chain(neighbors.iter().cloned())
        })
        .collect::<BTreeSet<_>>();
    let mut visited = BTreeSet::<String>::new();
    let mut order = Vec::<String>::new();

    for node in &nodes {
        if visited.contains(node) {
            continue;
        }
        let mut stack = vec![(node.clone(), false)];
        while let Some((current, expanded)) = stack.pop() {
            if expanded {
                order.push(current);
                continue;
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            stack.push((current.clone(), true));
            if let Some(neighbors) = adjacency.get(&current) {
                for neighbor in neighbors.iter().rev() {
                    if !visited.contains(neighbor) {
                        stack.push((neighbor.clone(), false));
                    }
                }
            }
        }
    }

    let mut reverse = BTreeMap::<String, BTreeSet<String>>::new();
    for node in &nodes {
        reverse.entry(node.clone()).or_default();
    }
    for (from, neighbors) in adjacency {
        for to in neighbors {
            reverse.entry(to.clone()).or_default().insert(from.clone());
        }
    }

    let mut assigned = BTreeSet::<String>::new();
    let mut components = Vec::new();
    while let Some(node) = order.pop() {
        if assigned.contains(&node) {
            continue;
        }
        let mut component = BTreeSet::<String>::new();
        let mut stack = vec![node];
        while let Some(current) = stack.pop() {
            if !assigned.insert(current.clone()) {
                continue;
            }
            component.insert(current.clone());
            if let Some(neighbors) = reverse.get(&current) {
                for neighbor in neighbors.iter().rev() {
                    if !assigned.contains(neighbor) {
                        stack.push(neighbor.clone());
                    }
                }
            }
        }
        components.push(component);
    }

    components
}

fn detect_pump_and_exit(
    path: &NftPropagationPathPayload,
    address_sets: &AddressSets,
    cycles: &[CyclePattern],
) -> Vec<PumpExitPattern> {
    let mut rows = Vec::new();
    for cycle in cycles {
        let exit_edges = path
            .edges
            .iter()
            .filter(|edge| {
                edge.channel == "sale"
                    && edge.block_time >= cycle.max_block_time
                    && cycle
                        .participants
                        .contains(&normalized_address(&edge.from_address))
                    && address_sets
                        .honest
                        .contains(&normalized_address(&edge.to_address))
            })
            .collect::<Vec<_>>();
        if exit_edges.is_empty() {
            continue;
        }
        let linked_buyers = exit_edges
            .iter()
            .map(|edge| normalized_address(&edge.to_address))
            .collect::<BTreeSet<_>>();
        let exit_tokens = exit_edges
            .iter()
            .flat_map(|edge| edge_token_ids(edge))
            .collect::<BTreeSet<_>>();
        let first_exit_time = exit_edges
            .iter()
            .map(|edge| edge.block_time)
            .filter(|block_time| *block_time > 0)
            .min();
        let (exit_price_premium, exit_price_premium_numerator, exit_price_premium_denominator) =
            price_premium_components(
                average_optional(exit_edges.iter().filter_map(|edge| edge.price_usd)),
                cycle.avg_price_usd,
            )
            .or_else(|| {
                price_premium_components(
                    average_optional(exit_edges.iter().filter_map(|edge| edge.price_eth)),
                    cycle.avg_price_eth,
                )
            })
            .unwrap_or((None, 0.0, 0.0));
        if exit_price_premium
            .filter(|premium| *premium > 1.0)
            .is_none()
        {
            continue;
        }
        let mut addresses = cycle.participants.clone();
        addresses.extend(linked_buyers.iter().cloned());
        let mut token_ids = cycle.token_ids.clone();
        token_ids.extend(exit_tokens.iter().cloned());
        rows.push(PumpExitPattern {
            row: PaperPumpExitRowPayload {
                cycle_id: cycle.row.cycle_id.clone(),
                exit_delay_seconds: first_exit_time
                    .map(|time| (time - cycle.max_block_time).max(0)),
                exit_price_premium,
                exit_price_premium_numerator,
                exit_price_premium_denominator,
                exit_ratio: ratio_i64(exit_tokens.len() as i64, cycle.token_ids.len() as i64),
                exit_ratio_numerator: exit_tokens.len() as i64,
                exit_ratio_denominator: cycle.token_ids.len() as i64,
                linked_honest_buyer_count: linked_buyers.len() as i64,
                linked_loss_eth: exit_edges
                    .iter()
                    .map(|edge| edge.price_eth.unwrap_or_default())
                    .sum(),
                linked_loss_usd: exit_edges
                    .iter()
                    .map(|edge| edge.price_usd.unwrap_or_default())
                    .sum(),
            },
            addresses,
            token_ids,
            buyers: linked_buyers,
        });
    }
    rows
}

fn detect_star_behaviors(
    path: &NftPropagationPathPayload,
    address_sets: &AddressSets,
    config: PaperStatsConfig,
) -> Vec<StarBehaviorPattern> {
    let mut adjacency = BTreeMap::<String, BTreeSet<String>>::new();
    let mut graph_edges = Vec::<&NftPropagationEdgePayload>::new();
    for edge in &path.edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        if !is_participant_address(&from) || !is_participant_address(&to) {
            continue;
        }
        adjacency
            .entry(from.clone())
            .or_default()
            .insert(to.clone());
        adjacency.entry(to).or_default();
        graph_edges.push(edge);
    }

    let components = strongly_connected_components(&adjacency);
    let mut component_by_address = BTreeMap::<String, usize>::new();
    for (index, component) in components.iter().enumerate() {
        for address in component {
            component_by_address.insert(address.clone(), index);
        }
    }

    let mut dag_targets = BTreeMap::<usize, BTreeSet<usize>>::new();
    let mut center_edges = BTreeMap::<usize, Vec<&NftPropagationEdgePayload>>::new();
    for edge in graph_edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        let (Some(from_component), Some(to_component)) = (
            component_by_address.get(&from).copied(),
            component_by_address.get(&to).copied(),
        ) else {
            continue;
        };
        if from_component == to_component {
            continue;
        }
        dag_targets
            .entry(from_component)
            .or_default()
            .insert(to_component);
        if components[from_component]
            .iter()
            .any(|address| address_sets.malicious.contains(address))
        {
            center_edges.entry(from_component).or_default().push(edge);
        }
    }

    let mut by_behavior = BTreeMap::<String, StarBehaviorBuild>::new();
    for (center_component, edges) in center_edges {
        let targets = dag_targets
            .get(&center_component)
            .cloned()
            .unwrap_or_default();
        if targets.len() < config.fanout_threshold() {
            continue;
        }
        let has_downstream = targets.iter().any(|target| {
            dag_targets
                .get(target)
                .map(|downstream| !downstream.is_empty())
                .unwrap_or(false)
        });
        let has_value = edges.iter().any(|edge| {
            edge.channel == "sale"
                || edge.price_eth.unwrap_or_default() > 0.0
                || edge.price_usd.unwrap_or_default() > 0.0
        });
        let behavior = if has_downstream {
            "Sybil Distribution"
        } else if has_value {
            "Fraud Revenue"
        } else {
            "Poisoning"
        };
        let entry = by_behavior.entry(behavior.to_string()).or_default();
        entry.centers.insert(center_component);
        entry.fanout_total += targets.len();
        entry.edge_count += edges.len() as i64;
        entry
            .wallets
            .extend(components[center_component].iter().cloned());
        for target in targets {
            entry.wallets.extend(components[target].iter().cloned());
        }
        for edge in edges {
            let buyer = normalized_address(&edge.to_address);
            if edge.channel == "sale" && address_sets.honest.contains(&buyer) {
                entry.buyers.insert(buyer);
                entry.linked_loss_eth += edge.price_eth.unwrap_or_default();
                entry.linked_loss_usd += edge.price_usd.unwrap_or_default();
            }
            entry.token_ids.extend(edge_token_ids(edge));
            entry.total_value_eth += edge.price_eth.unwrap_or_default();
            entry.total_value_usd += edge.price_usd.unwrap_or_default();
        }
    }

    by_behavior
        .into_iter()
        .map(|(behavior, build)| StarBehaviorPattern {
            row: PaperStarBehaviorRowPayload {
                behavior,
                centers: build.centers.len() as i64,
                edges: build.edge_count,
                wallets: build.wallets.len() as i64,
                tokens: build.token_ids.len() as i64,
                avg_fan_out: ratio_i64(build.fanout_total as i64, build.centers.len() as i64),
                avg_fan_out_numerator: build.fanout_total as i64,
                avg_fan_out_denominator: build.centers.len() as i64,
                median_holding_seconds: None,
                total_value_eth: build.total_value_eth,
                total_value_usd: build.total_value_usd,
                linked_honest_buyer_count: build.buyers.len() as i64,
                linked_loss_eth: build.linked_loss_eth,
                linked_loss_usd: build.linked_loss_usd,
            },
            addresses: build.wallets,
            token_ids: build.token_ids,
            buyers: build.buyers,
        })
        .collect()
}

fn detect_layered_transfers(
    contract_address: &str,
    path: &NftPropagationPathPayload,
    config: PaperStatsConfig,
) -> Vec<LayeredTransferPattern> {
    let mut adjacency = BTreeMap::<String, Vec<&NftPropagationEdgePayload>>::new();
    for edge in &path.edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        if is_participant_address(&from) && is_participant_address(&to) {
            adjacency.entry(from).or_default().push(edge);
        }
    }

    let mut rows = Vec::new();
    for start in adjacency.keys() {
        let mut visited = BTreeSet::from([start.clone()]);
        let mut current_edges = Vec::new();
        if find_layered_path(
            &adjacency,
            start,
            &mut visited,
            &mut current_edges,
            config.min_path_length(),
        ) {
            let wallets = current_edges
                .iter()
                .flat_map(|edge: &&NftPropagationEdgePayload| {
                    [
                        normalized_address(&edge.from_address),
                        normalized_address(&edge.to_address),
                    ]
                })
                .collect::<BTreeSet<_>>();
            let tokens = current_edges
                .iter()
                .flat_map(|edge| edge_token_ids(edge))
                .collect::<BTreeSet<_>>();
            let block_times = current_edges
                .iter()
                .map(|edge| edge.block_time)
                .filter(|block_time| *block_time > 0)
                .collect::<Vec<_>>();
            rows.push(LayeredTransferPattern {
                row: PaperLayeredTransferRowPayload {
                    path_id: format!("{contract_address}:path:{}", rows.len() + 1),
                    tokens: tokens.len() as i64,
                    length: wallets.len() as i64,
                    wallets: wallets.len() as i64,
                    zero_or_low_value_hops: current_edges
                        .iter()
                        .filter(|edge| {
                            edge.price_usd.unwrap_or_default() <= 1.0
                                && edge.price_eth.unwrap_or_default() <= 0.001
                        })
                        .count() as i64,
                    total_path_duration_seconds: match (
                        block_times.iter().min(),
                        block_times.iter().max(),
                    ) {
                        (Some(first), Some(last)) => Some((*last - *first).max(0)),
                        _ => None,
                    },
                    total_value_eth: current_edges
                        .iter()
                        .map(|edge| edge.price_eth.unwrap_or_default())
                        .sum(),
                    total_value_usd: current_edges
                        .iter()
                        .map(|edge| edge.price_usd.unwrap_or_default())
                        .sum(),
                },
                addresses: wallets,
                token_ids: tokens,
            });
            break;
        }
    }
    rows
}

fn find_layered_path<'a>(
    adjacency: &BTreeMap<String, Vec<&'a NftPropagationEdgePayload>>,
    current: &str,
    visited: &mut BTreeSet<String>,
    current_edges: &mut Vec<&'a NftPropagationEdgePayload>,
    min_wallet_count: usize,
) -> bool {
    if visited.len() >= min_wallet_count {
        return true;
    }
    let Some(edges) = adjacency.get(current) else {
        return false;
    };
    for edge in edges {
        let next = normalized_address(&edge.to_address);
        if visited.contains(&next) {
            continue;
        }
        visited.insert(next.clone());
        current_edges.push(*edge);
        if find_layered_path(adjacency, &next, visited, current_edges, min_wallet_count) {
            return true;
        }
        current_edges.pop();
        visited.remove(&next);
    }
    false
}

fn detect_inventory_concentration(
    path: &NftPropagationPathPayload,
    address_sets: &AddressSets,
    config: PaperStatsConfig,
) -> Vec<InventoryConcentrationPattern> {
    let total_tokens = if path.summary.token_count > 0 {
        path.summary.token_count
    } else {
        path.edges
            .iter()
            .flat_map(edge_token_ids)
            .collect::<BTreeSet<_>>()
            .len() as i64
    };
    let total_value_eth: f64 = path
        .edges
        .iter()
        .map(|edge| edge.price_eth.unwrap_or_default())
        .sum();
    let total_value_usd: f64 = path
        .edges
        .iter()
        .map(|edge| edge.price_usd.unwrap_or_default())
        .sum();
    let mut inbound = BTreeMap::<String, Vec<&NftPropagationEdgePayload>>::new();
    let mut outgoing = BTreeMap::<String, BTreeSet<String>>::new();
    for edge in &path.edges {
        let from = normalized_address(&edge.from_address);
        let to = normalized_address(&edge.to_address);
        if !is_participant_address(&from) || !is_participant_address(&to) {
            continue;
        }
        inbound.entry(to.clone()).or_default().push(edge);
        outgoing.entry(from).or_default().insert(to);
    }

    let mut rows = inbound
        .into_iter()
        .filter_map(|(hub, edges)| {
            if !address_sets.malicious.contains(&hub) {
                return None;
            }
            let sources = edges
                .iter()
                .map(|edge| normalized_address(&edge.from_address))
                .collect::<BTreeSet<_>>();
            let outgoing_fanout = outgoing.get(&hub).map(BTreeSet::len).unwrap_or_default();
            let enough_sources = sources.len() >= config.fanout_threshold();
            let fanout_center_returned_inventory =
                outgoing_fanout >= config.fanout_threshold() && !edges.is_empty();
            if !enough_sources && !fanout_center_returned_inventory {
                return None;
            }
            let tokens = edges
                .iter()
                .flat_map(|edge| edge_token_ids(edge))
                .collect::<BTreeSet<_>>();
            let block_times = edges
                .iter()
                .map(|edge| edge.block_time)
                .filter(|block_time| *block_time > 0)
                .collect::<Vec<_>>();
            let value_collected_eth: f64 = edges
                .iter()
                .map(|edge| edge.price_eth.unwrap_or_default())
                .sum();
            let value_collected_usd: f64 = edges
                .iter()
                .map(|edge| edge.price_usd.unwrap_or_default())
                .sum();
            let (value_share, value_share_numerator, value_share_denominator) =
                if total_value_usd > 0.0 {
                    (
                        ratio_f64(value_collected_usd, total_value_usd),
                        value_collected_usd,
                        total_value_usd,
                    )
                } else {
                    (
                        ratio_f64(value_collected_eth, total_value_eth),
                        value_collected_eth,
                        total_value_eth,
                    )
                };
            let mut addresses = sources.clone();
            addresses.insert(hub.clone());
            Some(InventoryConcentrationPattern {
                row: PaperInventoryConcentrationRowPayload {
                    hub_address: hub,
                    source_wallets: sources.len() as i64,
                    inbound_txns: edges.len() as i64,
                    token_share: ratio_i64(tokens.len() as i64, total_tokens),
                    token_share_numerator: tokens.len() as i64,
                    token_share_denominator: total_tokens,
                    value_collected_eth,
                    value_collected_usd,
                    value_share,
                    value_share_numerator,
                    value_share_denominator,
                    collection_window_seconds: match (
                        block_times.iter().min(),
                        block_times.iter().max(),
                    ) {
                        (Some(first), Some(last)) => Some((*last - *first).max(0)),
                        _ => None,
                    },
                },
                addresses,
                token_ids: tokens,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|pattern| Reverse(pattern.row.inbound_txns));
    rows
}

fn honest_buyers(
    input: &PaperStatsInput<'_>,
    contract_address: &str,
    path: &NftPropagationPathPayload,
    source_patterns_by_buyer: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<PaperHonestBuyerRowPayload> {
    let mint_payment_times = input
        .value_flow_edges
        .iter()
        .filter(|edge| {
            edge.channel == "mint_payment"
                && normalized_contract(&edge.contract_address) == contract_address
                && edge.block_time > 0
        })
        .collect::<Vec<_>>();
    let first_time = path
        .summary
        .first_block_time
        .gt(&0)
        .then_some(path.summary.first_block_time)
        .or_else(|| {
            path.edges
                .iter()
                .filter_map(|edge| (edge.block_time > 0).then_some(edge.block_time))
                .chain(mint_payment_times.iter().map(|edge| edge.block_time))
                .min()
        })
        .unwrap_or_default();
    let mut first_acquisition_by_buyer = path
        .edges
        .iter()
        .filter(|edge| edge.channel == "sale")
        .fold(BTreeMap::<String, i64>::new(), |mut map, edge| {
            let buyer = normalized_address(&edge.to_address);
            if is_participant_address(&buyer) && edge.block_time > 0 {
                insert_min_time(&mut map, buyer, edge.block_time);
            }
            map
        });
    for edge in mint_payment_times {
        let buyer = normalized_address(&edge.from_address);
        if is_participant_address(&buyer) {
            insert_min_time(&mut first_acquisition_by_buyer, buyer, edge.block_time);
        }
    }

    let mut rows = input
        .victim_acquisition_addresses
        .iter()
        .filter(|item| {
            item.contract_addresses
                .iter()
                .map(|contract| normalized_contract(contract))
                .any(|contract| contract == contract_address)
        })
        .filter(|item| {
            item.is_stuck || item.total_stuck_cost_eth > 0.0 || item.total_stuck_cost_usd > 0.0
        })
        .map(|item| {
            let buyer = normalized_address(&item.address);
            let first_buy_time = first_acquisition_by_buyer.get(&buyer).copied();
            let source_pattern = source_patterns_by_buyer
                .get(&buyer)
                .map(|patterns| patterns.iter().cloned().collect::<Vec<_>>().join("+"))
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "unattributed_sale".into());
            PaperHonestBuyerRowPayload {
                honest_buyer: buyer,
                fake_nft_bought: honest_buyer_stuck_nft_count(item),
                total_paid_eth: item
                    .total_acquisition_cost_eth
                    .max(item.total_stuck_cost_eth),
                total_paid_usd: item
                    .total_acquisition_cost_usd
                    .max(item.total_stuck_cost_usd),
                source_pattern,
                time_to_purchase_seconds: first_buy_time
                    .and_then(|time| (first_time > 0).then_some((time - first_time).max(0))),
                still_holding: item.is_stuck,
                holding_seconds: first_buy_time.and_then(|time| {
                    (input.config.analysis_timestamp > time)
                        .then_some(input.config.analysis_timestamp - time)
                }),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .total_paid_usd
            .partial_cmp(&left.total_paid_usd)
            .unwrap_or(Ordering::Equal)
    });
    rows
}

fn insert_min_time(target: &mut BTreeMap<String, i64>, address: String, time: i64) {
    target
        .entry(address)
        .and_modify(|existing| *existing = (*existing).min(time))
        .or_insert(time);
}

pub(super) fn paid_mint_observed_token_count(item: &VictimAcquisitionAddressPayload) -> i64 {
    item.paid_mint_token_count
        .max(item.paid_mint_stuck_token_count)
        .max(item.paid_mint_edge_count)
}

fn honest_buyer_stuck_nft_count(item: &VictimAcquisitionAddressPayload) -> i64 {
    let secondary_stuck_count = if item.is_stuck {
        item.secondary_sale_count
    } else {
        0
    };
    secondary_stuck_count + item.paid_mint_stuck_token_count
}

pub(super) fn wash_cycle_size_distribution_for_contracts(
    contract_stats: &[PaperContractBehaviorStatsPayload],
) -> Vec<PaperWashCycleSizeRowPayload> {
    let rows = contract_stats
        .iter()
        .flat_map(|stats| stats.wash_trading.iter().cloned())
        .collect::<Vec<_>>();
    wash_cycle_size_distribution_for_rows(&rows)
}

pub(super) fn wash_cycle_size_by_contract(
    input: &PaperStatsInput<'_>,
    contract_stats: &[PaperContractBehaviorStatsPayload],
) -> Vec<PaperContractWashCycleSizePayload> {
    let evidence_items = duplicate_evidence_items(input);
    let mut contracts = duplicate_contract_key_set(input, &evidence_items);
    for (contract_key, path) in input.nft_propagation_paths {
        let contract = if path.contract_address.trim().is_empty() {
            normalized_contract(contract_key)
        } else {
            normalized_contract(&path.contract_address)
        };
        if contract != "unknown" {
            contracts.insert(contract);
        }
    }
    if contracts.is_empty() {
        contracts.extend(
            contract_stats
                .iter()
                .map(|stats| normalized_contract(&stats.contract_address))
                .filter(|contract| contract != "unknown"),
        );
    }
    let distributions_by_contract = contract_stats
        .iter()
        .map(|stats| {
            (
                normalized_contract(&stats.contract_address),
                if stats.wash_cycle_size_distribution.is_empty() {
                    wash_cycle_size_distribution_for_rows(&stats.wash_trading)
                } else {
                    stats.wash_cycle_size_distribution.clone()
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    contracts
        .into_iter()
        .map(|contract| {
            let distribution = distributions_by_contract
                .get(&contract)
                .cloned()
                .unwrap_or_else(|| wash_cycle_size_distribution_for_rows(&[]));
            PaperContractWashCycleSizePayload {
                contract_address: contract,
                distribution,
            }
        })
        .collect()
}

pub(super) fn wash_cycle_size_by_contract_from_stats(
    contract_stats: &[PaperContractBehaviorStatsPayload],
) -> Vec<PaperContractWashCycleSizePayload> {
    contract_stats
        .iter()
        .map(|stats| PaperContractWashCycleSizePayload {
            contract_address: normalized_contract(&stats.contract_address),
            distribution: if stats.wash_cycle_size_distribution.is_empty() {
                wash_cycle_size_distribution_for_rows(&stats.wash_trading)
            } else {
                stats.wash_cycle_size_distribution.clone()
            },
        })
        .collect()
}

fn wash_cycle_size_distribution_for_rows(
    rows: &[PaperWashTradingRowPayload],
) -> Vec<PaperWashCycleSizeRowPayload> {
    let mut two_node_count = 0;
    let mut three_node_count = 0;
    let mut four_node_count = 0;
    let mut five_plus_node_count = 0;
    for row in rows {
        match row.participant_node_count {
            2 => two_node_count += 1,
            3 => three_node_count += 1,
            4 => four_node_count += 1,
            count if count >= 5 => five_plus_node_count += 1,
            _ => {}
        }
    }
    let total = two_node_count + three_node_count + four_node_count + five_plus_node_count;
    [
        ("2", two_node_count),
        ("3", three_node_count),
        ("4", four_node_count),
        ("5+", five_plus_node_count),
    ]
    .into_iter()
    .map(|(bucket, count)| PaperWashCycleSizeRowPayload {
        node_count_bucket: bucket.to_string(),
        cycle_count: count,
        cycle_ratio: ratio_i64(count, total),
        cycle_ratio_numerator: count,
        cycle_ratio_denominator: total,
    })
    .collect()
}

pub(super) fn build_behavior_summary(
    contract_stats: &[PaperContractBehaviorStatsPayload],
    contract_denominator: usize,
    behavior_contracts: &BTreeMap<String, BTreeSet<String>>,
    behavior_addresses: &BTreeMap<String, BTreeSet<String>>,
    behavior_nfts: &BTreeMap<String, BTreeSet<String>>,
    behavior_buyers: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<PaperBehaviorSummaryRowPayload> {
    let mut rows = Vec::new();
    let mut total = PaperBehaviorSummaryRowPayload {
        behavior_type: "total".into(),
        contract_coverage_denominator: contract_denominator as i64,
        ..PaperBehaviorSummaryRowPayload::default()
    };

    if let Some(mut row) = build_behavior_row(
        "Wash Trading",
        contract_stats,
        contract_denominator,
        |stats| BehaviorMeasure {
            instance_count: stats.wash_trading.len() as i64,
            address_count: stats
                .wash_trading
                .iter()
                .map(|row| row.participant_node_count)
                .sum(),
            nft_count: stats.wash_trading.len() as i64,
            ..BehaviorMeasure::default()
        },
    ) {
        apply_behavior_dedup(
            &mut row,
            behavior_contracts,
            behavior_addresses,
            behavior_nfts,
            behavior_buyers,
        );
        rows.push(row);
    }
    if let Some(mut row) = build_behavior_row(
        "Pump-and-Exit",
        contract_stats,
        contract_denominator,
        |stats| BehaviorMeasure {
            instance_count: stats.pump_and_exit.len() as i64,
            address_count: stats
                .pump_and_exit
                .iter()
                .map(|row| row.linked_honest_buyer_count)
                .sum(),
            nft_count: stats.pump_and_exit.len() as i64,
            buyer_count: stats
                .pump_and_exit
                .iter()
                .map(|row| row.linked_honest_buyer_count)
                .sum(),
            linked_loss_eth: stats
                .pump_and_exit
                .iter()
                .map(|row| row.linked_loss_eth)
                .sum(),
            linked_loss_usd: stats
                .pump_and_exit
                .iter()
                .map(|row| row.linked_loss_usd)
                .sum(),
        },
    ) {
        apply_behavior_dedup(
            &mut row,
            behavior_contracts,
            behavior_addresses,
            behavior_nfts,
            behavior_buyers,
        );
        rows.push(row);
    }
    for behavior in ["Sybil Distribution", "Fraud Revenue", "Poisoning"] {
        if let Some(mut row) =
            build_behavior_row(behavior, contract_stats, contract_denominator, |stats| {
                BehaviorMeasure {
                    instance_count: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.centers)
                        .sum(),
                    address_count: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.wallets)
                        .sum(),
                    nft_count: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.tokens)
                        .sum(),
                    buyer_count: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.linked_honest_buyer_count)
                        .sum(),
                    linked_loss_eth: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.linked_loss_eth)
                        .sum(),
                    linked_loss_usd: stats
                        .star_behaviors
                        .iter()
                        .filter(|row| row.behavior == behavior)
                        .map(|row| row.linked_loss_usd)
                        .sum(),
                }
            })
        {
            apply_behavior_dedup(
                &mut row,
                behavior_contracts,
                behavior_addresses,
                behavior_nfts,
                behavior_buyers,
            );
            rows.push(row);
        }
    }
    if let Some(mut row) = build_behavior_row(
        "Layered Transfer",
        contract_stats,
        contract_denominator,
        |stats| BehaviorMeasure {
            instance_count: stats.layered_transfers.len() as i64,
            address_count: stats.layered_transfers.iter().map(|row| row.wallets).sum(),
            nft_count: stats.layered_transfers.iter().map(|row| row.tokens).sum(),
            ..BehaviorMeasure::default()
        },
    ) {
        apply_behavior_dedup(
            &mut row,
            behavior_contracts,
            behavior_addresses,
            behavior_nfts,
            behavior_buyers,
        );
        rows.push(row);
    }
    if let Some(mut row) = build_behavior_row(
        "Inventory Concentration",
        contract_stats,
        contract_denominator,
        |stats| BehaviorMeasure {
            instance_count: stats.inventory_concentration.len() as i64,
            address_count: stats
                .inventory_concentration
                .iter()
                .map(|row| row.source_wallets + 1)
                .sum(),
            nft_count: stats
                .inventory_concentration
                .iter()
                .map(|row| row.inbound_txns)
                .sum(),
            ..BehaviorMeasure::default()
        },
    ) {
        apply_behavior_dedup(
            &mut row,
            behavior_contracts,
            behavior_addresses,
            behavior_nfts,
            behavior_buyers,
        );
        rows.push(row);
    }

    let instance_denominator: i64 = rows.iter().map(|row| row.instance_count).sum();
    for row in &mut rows {
        row.instance_ratio_denominator = instance_denominator;
        row.instance_ratio = ratio_i64(row.instance_ratio_numerator, instance_denominator);
        total.contract_count += row.contract_count;
        total.instance_count += row.instance_count;
        total.instance_ratio_numerator += row.instance_ratio_numerator;
        total.address_count += row.address_count;
        total.nft_count += row.nft_count;
        total.linked_buyer_count += row.linked_buyer_count;
        total.linked_loss_eth += row.linked_loss_eth;
        total.linked_loss_usd += row.linked_loss_usd;
    }
    total.contract_count = contract_stats
        .iter()
        .filter(|stats| {
            !stats.wash_trading.is_empty()
                || !stats.pump_and_exit.is_empty()
                || !stats.star_behaviors.is_empty()
                || !stats.layered_transfers.is_empty()
                || !stats.inventory_concentration.is_empty()
        })
        .count() as i64;
    if !behavior_contracts.is_empty() {
        total.contract_count = union_count(behavior_contracts) as i64;
    }
    if !behavior_addresses.is_empty() {
        total.address_count = union_count(behavior_addresses) as i64;
    }
    if !behavior_nfts.is_empty() {
        total.nft_count = union_count(behavior_nfts) as i64;
    }
    if !behavior_buyers.is_empty() {
        total.linked_buyer_count = union_count(behavior_buyers) as i64;
    }
    total.contract_coverage_numerator = total.contract_count;
    total.contract_coverage_ratio = ratio_i64(
        total.contract_coverage_numerator,
        total.contract_coverage_denominator,
    );
    total.instance_ratio_denominator = instance_denominator;
    total.instance_ratio = ratio_i64(total.instance_ratio_numerator, instance_denominator);
    if instance_denominator > 0 {
        rows.push(total);
    }
    rows
}

fn apply_behavior_dedup(
    row: &mut PaperBehaviorSummaryRowPayload,
    behavior_contracts: &BTreeMap<String, BTreeSet<String>>,
    behavior_addresses: &BTreeMap<String, BTreeSet<String>>,
    behavior_nfts: &BTreeMap<String, BTreeSet<String>>,
    behavior_buyers: &BTreeMap<String, BTreeSet<String>>,
) {
    if let Some(contracts) = behavior_contracts.get(&row.behavior_type) {
        row.contract_count = contracts.len() as i64;
        row.contract_coverage_numerator = row.contract_count;
        row.contract_coverage_ratio = ratio_i64(
            row.contract_coverage_numerator,
            row.contract_coverage_denominator,
        );
    }
    if let Some(addresses) = behavior_addresses.get(&row.behavior_type) {
        row.address_count = addresses.len() as i64;
    }
    if let Some(nfts) = behavior_nfts.get(&row.behavior_type) {
        row.nft_count = nfts.len() as i64;
    }
    if let Some(buyers) = behavior_buyers.get(&row.behavior_type) {
        row.linked_buyer_count = buyers.len() as i64;
    }
}

fn union_count(source: &BTreeMap<String, BTreeSet<String>>) -> usize {
    source
        .values()
        .flat_map(|values| values.iter().cloned())
        .collect::<BTreeSet<_>>()
        .len()
}

fn build_behavior_row(
    behavior_type: &str,
    contract_stats: &[PaperContractBehaviorStatsPayload],
    contract_denominator: usize,
    measure: impl Fn(&PaperContractBehaviorStatsPayload) -> BehaviorMeasure,
) -> Option<PaperBehaviorSummaryRowPayload> {
    let mut row = PaperBehaviorSummaryRowPayload {
        behavior_type: behavior_type.into(),
        contract_coverage_denominator: contract_denominator as i64,
        ..PaperBehaviorSummaryRowPayload::default()
    };
    for stats in contract_stats {
        let measure = measure(stats);
        if measure.instance_count <= 0 {
            continue;
        }
        row.contract_count += 1;
        row.instance_count += measure.instance_count;
        row.instance_ratio_numerator += measure.instance_count;
        row.address_count += measure.address_count;
        row.nft_count += measure.nft_count;
        row.linked_buyer_count += measure.buyer_count;
        row.linked_loss_eth += measure.linked_loss_eth;
        row.linked_loss_usd += measure.linked_loss_usd;
    }
    if row.instance_count <= 0 {
        return None;
    }
    row.contract_coverage_numerator = row.contract_count;
    row.contract_coverage_ratio = ratio_i64(
        row.contract_coverage_numerator,
        row.contract_coverage_denominator,
    );
    Some(row)
}

fn average_optional(values: impl Iterator<Item = f64>) -> Option<f64> {
    let values = values.filter(|value| *value > 0.0).collect::<Vec<_>>();
    (!values.is_empty()).then_some(values.iter().sum::<f64>() / values.len() as f64)
}

fn price_premium_components(
    numerator: Option<f64>,
    denominator: Option<f64>,
) -> Option<(Option<f64>, f64, f64)> {
    let numerator = numerator.filter(|value| *value > 0.0)?;
    let denominator = denominator.filter(|value| *value > 0.0)?;
    Some((Some(numerator / denominator), numerator, denominator))
}

fn gini(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let sum: f64 = values.iter().sum();
    if sum <= 0.0 {
        return Some(0.0);
    }
    let weighted_sum: f64 = values
        .iter()
        .enumerate()
        .map(|(index, value)| (index as f64 + 1.0) * value)
        .sum();
    Some(
        (2.0 * weighted_sum) / (values.len() as f64 * sum)
            - (values.len() as f64 + 1.0) / values.len() as f64,
    )
}

fn edge_token_ids(edge: &NftPropagationEdgePayload) -> Vec<String> {
    if edge.token_ids.is_empty() {
        if edge.token_id.trim().is_empty() {
            Vec::new()
        } else {
            vec![edge.token_id.clone()]
        }
    } else {
        edge.token_ids.clone()
    }
}
