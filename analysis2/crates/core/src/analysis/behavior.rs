//! Behavior detectors: wash, pump-exit, star, layered, inventory.

use std::collections::{BTreeMap, BTreeSet};

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};

use super::attribution::AddressRole;
use super::graph::AddressGraph;
use super::PaperConfig;
use crate::enrich::{EvidenceBundle, SaleEvent, TransferEvent};

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BehaviorKind {
    #[default]
    WashTrading,
    PumpAndExit,
    SybilDistribution,
    FraudRevenue,
    Poisoning,
    LayeredTransfer,
    InventoryConcentration,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BehaviorFacts {
    pub wash_cycles: u64,
    pub pump_and_exit: u64,
    pub sybil_distribution: u64,
    pub fraud_revenue: u64,
    pub poisoning: u64,
    pub layered_transfer: u64,
    pub inventory_concentration: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BehaviorInstance {
    pub kind: BehaviorKind,
    pub addresses: Vec<String>,
    pub nfts: Vec<String>,
    pub transactions: Vec<String>,
    pub linked_buyers: Vec<String>,
    pub edge_count: u64,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub native_value: f64,
    pub usd_value: f64,
    pub linked_loss_native: f64,
    pub linked_loss_usd: f64,
    pub gini_nft_count: Option<f64>,
    pub gini_token_transaction_count: Option<f64>,
    pub fan_out: Option<u64>,
    pub path_length: Option<u64>,
    pub low_value_hops: Option<u64>,
    pub source_address_count: Option<u64>,
    pub nft_share: Option<f64>,
    pub value_share: Option<f64>,
    pub exit_delay_seconds: Option<i64>,
    pub exit_to_internal_price_ratio: Option<f64>,
    pub exit_to_cycle_nft_ratio: Option<f64>,
}

pub struct BehaviorAnalysis {
    pub facts: BehaviorFacts,
    pub instances: Vec<BehaviorInstance>,
}

#[derive(Default)]
struct CycleValues {
    end: Option<i64>,
    internal_native_sum: f64,
    internal_native_count: u64,
    internal_usd_sum: f64,
    internal_usd_count: u64,
    exit_native_sum: f64,
    exit_native_count: u64,
    exit_usd_sum: f64,
    exit_usd_count: u64,
    internal_events: Vec<usize>,
    exit_events: Vec<usize>,
}

pub fn detect_behaviors(
    evidence: &EvidenceBundle,
    transfer_graph: &AddressGraph,
    transfer_sccs: &[Vec<usize>],
    roles: &BTreeMap<String, AddressRole>,
    cfg: &PaperConfig,
) -> BehaviorAnalysis {
    // Case-fold addresses: controller / sale checksum mismatches zeroed wash before.
    let malicious = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::SuspectedOperator | AddressRole::SuspectedColluder
            )
        })
        .map(|(address, _)| address.to_ascii_lowercase())
        .collect::<AHashSet<_>>();
    let honest = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::LikelyVictim | AddressRole::CorruptedVictim
            )
        })
        .map(|(address, _)| address.to_ascii_lowercase())
        .collect::<AHashSet<_>>();

    // Wash cycles: build SCC on *all* sales (normalized), then keep components that
    // are large enough and contain at least one malicious address. Using
    // both-malicious-only edges under-detects when colluder marking or address
    // casing dropped one side of a wash pair.
    let sale_graph = AddressGraph::from_sales(&evidence.sales);
    let sale_components = sale_graph.strongly_connected_components();
    let wash_components = sale_components
        .iter()
        .filter(|component| {
            component.len() >= cfg.min_cycle_size
                && component.iter().any(|&vertex| {
                    malicious.contains(&sale_graph.addresses[vertex].to_ascii_lowercase())
                })
        })
        .map(Vec::as_slice)
        .collect::<Vec<_>>();

    let mut wash_by_address = AHashMap::new();
    for (wash_id, component) in wash_components.iter().enumerate() {
        for &vertex in *component {
            wash_by_address.insert(
                sale_graph.addresses[vertex].to_ascii_lowercase(),
                wash_id,
            );
        }
    }

    let mut cycle_values = (0..wash_components.len())
        .map(|_| CycleValues::default())
        .collect::<Vec<_>>();
    for (event_index, sale) in evidence.sales.iter().enumerate() {
        let seller = sale.seller.to_ascii_lowercase();
        let buyer = sale.buyer.to_ascii_lowercase();
        if let (Some(&left), Some(&right)) =
            (wash_by_address.get(&seller), wash_by_address.get(&buyer))
        {
            if left == right {
                let values = &mut cycle_values[left];
                values.end = [values.end, sale.timestamp].into_iter().flatten().max();
                add_value(values, sale, ValueSide::Internal);
                values.internal_events.push(event_index);
            }
        }
    }
    for (event_index, sale) in evidence.sales.iter().enumerate() {
        let seller = sale.seller.to_ascii_lowercase();
        let buyer = sale.buyer.to_ascii_lowercase();
        let Some(&wash_id) = wash_by_address.get(&seller) else {
            continue;
        };
        let values = &mut cycle_values[wash_id];
        if honest.contains(&buyer)
            && wash_by_address.get(&buyer).copied() != Some(wash_id)
            && values
                .end
                .zip(sale.timestamp)
                .is_some_and(|(end, exit)| exit > end)
        {
            add_value(values, sale, ValueSide::Exit);
            values.exit_events.push(event_index);
        }
    }
    let pump_and_exit = cycle_values
        .iter()
        .filter(|values| exit_price_exceeds_internal(values))
        .count() as u64;

    let malicious_refs: AHashSet<&str> = malicious.iter().map(String::as_str).collect();
    let star = star_behaviors(
        evidence,
        transfer_graph,
        transfer_sccs,
        roles,
        &malicious_refs,
        cfg.fan_out,
    );
    let layered_instances = layered_paths(evidence, transfer_graph, cfg.layered_path_addresses);
    let layered_transfer = layered_instances.len() as u64;

    let mut indegree = vec![0_u64; transfer_graph.addresses.len()];
    for &target in &transfer_graph.edges {
        indegree[target] += 1;
    }
    let inventory_vertices = indegree
        .iter()
        .enumerate()
        .filter_map(|(vertex, degree)| {
            (malicious.contains(transfer_graph.addresses[vertex].as_str())
                && (*degree >= 3
                    || (*degree > 0
                        && transfer_graph.offsets[vertex + 1] - transfer_graph.offsets[vertex]
                            >= 3)))
            .then_some(vertex)
        })
        .collect::<Vec<_>>();
    let inventory_concentration = inventory_vertices.len() as u64;

    let mut instances = cycle_instances(&evidence.sales, &sale_graph, &wash_components, &cycle_values);
    instances.extend(
        cycle_values
            .iter()
            .enumerate()
            .filter(|(_, values)| exit_price_exceeds_internal(values))
            .map(|(cycle, values)| {
                pump_instance(&evidence.sales, &sale_graph, wash_components[cycle], values)
            }),
    );
    instances.extend(star.instances);
    instances.extend(layered_instances);
    instances.extend(inventory_instances(evidence, transfer_graph, &inventory_vertices));
    instances.sort_by(|left, right| {
        (
            left.kind,
            left.start_timestamp,
            left.start_block,
            &left.addresses,
            &left.transactions,
        )
            .cmp(&(
                right.kind,
                right.start_timestamp,
                right.start_block,
                &right.addresses,
                &right.transactions,
            ))
    });

    BehaviorAnalysis {
        facts: BehaviorFacts {
            wash_cycles: wash_components.len() as u64,
            pump_and_exit,
            sybil_distribution: star.sybil,
            fraud_revenue: star.fraud,
            poisoning: star.poisoning,
            layered_transfer,
            inventory_concentration,
        },
        instances,
    }
}

struct StarAnalysis {
    sybil: u64,
    fraud: u64,
    poisoning: u64,
    instances: Vec<BehaviorInstance>,
}

fn star_behaviors(
    evidence: &EvidenceBundle,
    graph: &AddressGraph,
    components: &[Vec<usize>],
    roles: &BTreeMap<String, AddressRole>,
    malicious: &AHashSet<&str>,
    fan_out: usize,
) -> StarAnalysis {
    if components.is_empty() || graph.addresses.is_empty() {
        return StarAnalysis {
            sybil: 0,
            fraud: 0,
            poisoning: 0,
            instances: Vec::new(),
        };
    }
    let mut component_by_vertex = vec![0_usize; graph.addresses.len()];
    let mut malicious_component = vec![false; components.len()];
    for (component_id, component) in components.iter().enumerate() {
        for &vertex in component {
            component_by_vertex[vertex] = component_id;
            malicious_component[component_id] |=
                malicious.contains(graph.addresses[vertex].as_str());
        }
    }
    let address_ids = graph.address_index();
    let honest: AHashSet<String> = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::LikelyVictim | AddressRole::CorruptedVictim
            )
        })
        .map(|(address, _)| address.to_ascii_lowercase())
        .collect();
    let malicious_lower: AHashSet<String> = malicious
        .iter()
        .map(|a| a.to_ascii_lowercase())
        .collect();
    let mut dag_targets = (0..components.len())
        .map(|_| BTreeSet::new())
        .collect::<Vec<_>>();
    let mut valuable_outgoing = vec![false; components.len()];
    let mut outgoing_events = (0..components.len())
        .map(|_| Vec::new())
        .collect::<Vec<_>>();

    for (event_index, transfer) in evidence.transfers.iter().enumerate() {
        let (Some(&from), Some(&to)) = (
            address_ids.get(transfer.from.as_str()),
            address_ids.get(transfer.to.as_str()),
        ) else {
            continue;
        };
        let left = component_by_vertex[from];
        let right = component_by_vertex[to];
        if left == right {
            continue;
        }
        dag_targets[left].insert(right);
        outgoing_events[left].push(PropEvent::Transfer(event_index));
    }
    for (event_index, sale) in evidence.sales.iter().enumerate() {
        let (Some(&from), Some(&to)) = (
            address_ids.get(sale.seller.as_str()),
            address_ids.get(sale.buyer.as_str()),
        ) else {
            continue;
        };
        let left = component_by_vertex[from];
        let right = component_by_vertex[to];
        if left == right {
            continue;
        }
        dag_targets[left].insert(right);
        outgoing_events[left].push(PropEvent::Sale(event_index));
        valuable_outgoing[left] |= sale.native_amount.unwrap_or(0.0) > 0.0
            || sale.usd_amount.unwrap_or(0.0) > 0.0;
    }

    let mut instances = Vec::new();
    let mut sybil = 0_u64;
    let mut fraud = 0_u64;
    let mut poisoning = 0_u64;
    for component in 0..components.len() {
        if !malicious_component[component] || dag_targets[component].len() < fan_out {
            continue;
        }
        let kind = if dag_targets[component]
            .iter()
            .any(|target| !dag_targets[*target].is_empty())
        {
            BehaviorKind::SybilDistribution
        } else if valuable_outgoing[component] {
            BehaviorKind::FraudRevenue
        } else {
            BehaviorKind::Poisoning
        };
        match kind {
            BehaviorKind::SybilDistribution => sybil += 1,
            BehaviorKind::FraudRevenue => fraud += 1,
            BehaviorKind::Poisoning => poisoning += 1,
            _ => {}
        }
        let mut instance = instance_from_prop_events(kind, evidence, &outgoing_events[component]);
        instance.fan_out = Some(dag_targets[component].len() as u64);
        // Linked buyers + loss: direct paid sales from this star component to
        // non-malicious counterparties (paper Sybil/Fraud/Poisoning 关联损失).
        // Prefer role-tagged victims; also count paid non-malicious buyers when
        // holder snapshots are incomplete (otherwise linked_loss stays 0 in practice).
        let mut linked_buyers = AHashSet::new();
        let mut linked_loss_native = 0.0_f64;
        let mut linked_loss_usd = 0.0_f64;
        for event in &outgoing_events[component] {
            let PropEvent::Sale(index) = event else {
                continue;
            };
            let sale = &evidence.sales[*index];
            let buyer = sale.buyer.to_ascii_lowercase();
            if buyer.is_empty() || malicious_lower.contains(&buyer) {
                continue;
            }
            let paid_native = sale.native_amount.filter(|v| *v > 0.0);
            let paid_usd = sale.usd_amount.filter(|v| *v > 0.0);
            let paid = paid_native.is_some() || paid_usd.is_some();
            let role_victim = honest.contains(&buyer);
            if !role_victim && !paid {
                continue;
            }
            linked_buyers.insert(buyer);
            if let Some(native) = paid_native {
                linked_loss_native += native;
            }
            if let Some(usd) = paid_usd {
                linked_loss_usd += usd;
            }
        }
        let mut linked_buyer_list = linked_buyers.into_iter().collect::<Vec<_>>();
        linked_buyer_list.sort();
        instance.linked_buyers = linked_buyer_list;
        instance.linked_loss_native = linked_loss_native;
        instance.linked_loss_usd = linked_loss_usd;
        instances.push(instance);
    }
    StarAnalysis {
        sybil,
        fraud,
        poisoning,
        instances,
    }
}

#[derive(Clone, Copy)]
enum PropEvent {
    Transfer(usize),
    Sale(usize),
}

#[derive(Clone, Copy)]
enum ValueSide {
    Internal,
    Exit,
}

fn add_value(values: &mut CycleValues, sale: &SaleEvent, side: ValueSide) {
    if let Some(native) = sale.native_amount.filter(|value| *value >= 0.0) {
        match side {
            ValueSide::Internal => {
                values.internal_native_sum += native;
                values.internal_native_count += 1;
            }
            ValueSide::Exit => {
                values.exit_native_sum += native;
                values.exit_native_count += 1;
            }
        }
    }
    if let Some(usd) = sale.usd_amount.filter(|value| *value >= 0.0) {
        match side {
            ValueSide::Internal => {
                values.internal_usd_sum += usd;
                values.internal_usd_count += 1;
            }
            ValueSide::Exit => {
                values.exit_usd_sum += usd;
                values.exit_usd_count += 1;
            }
        }
    }
}

fn exit_price_exceeds_internal(values: &CycleValues) -> bool {
    if values.internal_usd_count > 0 && values.exit_usd_count > 0 {
        return (values.exit_usd_sum / values.exit_usd_count as f64)
            > (values.internal_usd_sum / values.internal_usd_count as f64);
    }
    values.internal_native_count > 0
        && values.exit_native_count > 0
        && (values.exit_native_sum / values.exit_native_count as f64)
            > (values.internal_native_sum / values.internal_native_count as f64)
}

fn cycle_instances(
    sales: &[SaleEvent],
    graph: &AddressGraph,
    components: &[&[usize]],
    values: &[CycleValues],
) -> Vec<BehaviorInstance> {
    components
        .iter()
        .zip(values)
        .map(|(component, values)| {
            let selected = values
                .internal_events
                .iter()
                .map(|&index| &sales[index])
                .collect::<Vec<_>>();
            let mut instance = instance_from_sales(BehaviorKind::WashTrading, &selected);
            instance.addresses = component
                .iter()
                .map(|&vertex| graph.addresses[vertex].clone())
                .collect();
            instance.addresses.sort();
            let (nft_counts, transaction_counts) =
                participant_distributions(&instance.addresses, &selected);
            instance.gini_nft_count = gini(&nft_counts);
            instance.gini_token_transaction_count = gini(&transaction_counts);
            instance
        })
        .collect()
}

fn pump_instance(
    sales: &[SaleEvent],
    graph: &AddressGraph,
    component: &[usize],
    values: &CycleValues,
) -> BehaviorInstance {
    let selected = values
        .exit_events
        .iter()
        .map(|&index| &sales[index])
        .collect::<Vec<_>>();
    let mut instance = instance_from_sales(BehaviorKind::PumpAndExit, &selected);
    instance.addresses.extend(
        component
            .iter()
            .map(|&vertex| graph.addresses[vertex].clone()),
    );
    instance.addresses.sort();
    instance.addresses.dedup();
    instance.linked_buyers = selected.iter().map(|sale| sale.buyer.clone()).collect();
    instance.linked_buyers.sort();
    instance.linked_buyers.dedup();
    instance.linked_loss_native = values.exit_native_sum;
    instance.linked_loss_usd = values.exit_usd_sum;
    instance.exit_to_internal_price_ratio = comparable_average_ratio(values);
    let cycle_nfts = values
        .internal_events
        .iter()
        .map(|&index| sales[index].token_id.as_str())
        .collect::<BTreeSet<_>>();
    instance.exit_to_cycle_nft_ratio =
        (!cycle_nfts.is_empty()).then(|| instance.nfts.len() as f64 / cycle_nfts.len() as f64);
    instance.exit_delay_seconds = values
        .end
        .zip(instance.start_timestamp)
        .map(|(cycle_end, exit_start)| exit_start.saturating_sub(cycle_end));
    instance
}

fn comparable_average_ratio(values: &CycleValues) -> Option<f64> {
    let (internal_sum, internal_count, exit_sum, exit_count) =
        if values.internal_usd_count > 0 && values.exit_usd_count > 0 {
            (
                values.internal_usd_sum,
                values.internal_usd_count,
                values.exit_usd_sum,
                values.exit_usd_count,
            )
        } else if values.internal_native_count > 0 && values.exit_native_count > 0 {
            (
                values.internal_native_sum,
                values.internal_native_count,
                values.exit_native_sum,
                values.exit_native_count,
            )
        } else {
            return None;
        };
    let internal = internal_sum / internal_count as f64;
    (internal > 0.0).then(|| (exit_sum / exit_count as f64) / internal)
}

fn participant_distributions(addresses: &[String], sales: &[&SaleEvent]) -> (Vec<u64>, Vec<u64>) {
    let mut nfts = addresses
        .iter()
        .cloned()
        .map(|address| (address, BTreeSet::<&str>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut transactions = addresses
        .iter()
        .cloned()
        .map(|address| (address, 0_u64))
        .collect::<BTreeMap<_, _>>();
    for sale in sales {
        for address in [&sale.seller, &sale.buyer] {
            nfts.entry(address.clone())
                .or_default()
                .insert(sale.token_id.as_str());
            *transactions.entry(address.clone()).or_default() += 1;
        }
    }
    (
        addresses
            .iter()
            .map(|address| nfts.get(address).map_or(0, |values| values.len() as u64))
            .collect(),
        addresses
            .iter()
            .map(|address| transactions.get(address).copied().unwrap_or(0))
            .collect(),
    )
}

fn gini(values: &[u64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let sum = values.iter().map(|&value| value as u128).sum::<u128>();
    if sum == 0 {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let weighted = sorted
        .iter()
        .enumerate()
        .map(|(index, &value)| (index as u128 + 1) * value as u128)
        .sum::<u128>();
    let count = sorted.len() as f64;
    Some((2.0 * weighted as f64) / (count * sum as f64) - (count + 1.0) / count)
}

fn layered_paths(
    evidence: &EvidenceBundle,
    graph: &AddressGraph,
    minimum_addresses: usize,
) -> Vec<BehaviorInstance> {
    let address_ids = graph.address_index();
    let mut edge_events = AHashMap::<(usize, usize), Vec<PropEvent>>::new();
    for (event_index, transfer) in evidence.transfers.iter().enumerate() {
        let (Some(&from), Some(&to)) = (
            address_ids.get(transfer.from.as_str()),
            address_ids.get(transfer.to.as_str()),
        ) else {
            continue;
        };
        edge_events
            .entry((from, to))
            .or_default()
            .push(PropEvent::Transfer(event_index));
    }
    for (event_index, sale) in evidence.sales.iter().enumerate() {
        let (Some(&from), Some(&to)) = (
            address_ids.get(sale.seller.as_str()),
            address_ids.get(sale.buyer.as_str()),
        ) else {
            continue;
        };
        edge_events
            .entry((from, to))
            .or_default()
            .push(PropEvent::Sale(event_index));
    }
    let Some(path) = first_layered_path(graph, minimum_addresses) else {
        return Vec::new();
    };
    let addresses = path
        .iter()
        .map(|&member| graph.addresses[member].clone())
        .collect::<Vec<_>>();
    let selected = path
        .windows(2)
        .filter_map(|pair| edge_events.get(&(pair[0], pair[1]))?.first().copied())
        .collect::<Vec<_>>();
    if selected.len() + 1 != path.len() {
        return Vec::new();
    }
    let mut instance = instance_from_prop_events(BehaviorKind::LayeredTransfer, evidence, &selected);
    instance.addresses = addresses;
    instance.path_length = Some(minimum_addresses as u64);
    instance.low_value_hops = Some(
        selected
            .iter()
            .filter(|event| match event {
                PropEvent::Transfer(_) => true,
                PropEvent::Sale(index) => {
                    let sale = &evidence.sales[*index];
                    sale.usd_amount.unwrap_or(f64::MAX) <= 1.0
                        && sale.native_amount.unwrap_or(f64::MAX) <= 0.001
                }
            })
            .count() as u64,
    );
    vec![instance]
}

fn first_layered_path(graph: &AddressGraph, minimum_addresses: usize) -> Option<Vec<usize>> {
    if minimum_addresses == 0 {
        return Some(Vec::new());
    }
    for start in 0..graph.addresses.len() {
        let mut path = Vec::with_capacity(minimum_addresses);
        path.push(start);
        let mut next_edges = Vec::with_capacity(minimum_addresses);
        next_edges.push(graph.offsets[start]);
        while let Some(next_edge) = next_edges.last_mut() {
            if path.len() == minimum_addresses {
                return Some(path);
            }
            let vertex = *path.last().expect("nonempty layered path");
            if *next_edge == graph.offsets[vertex + 1] {
                next_edges.pop();
                path.pop();
                continue;
            }
            let next = graph.edges[*next_edge];
            *next_edge += 1;
            if path.contains(&next) {
                continue;
            }
            path.push(next);
            next_edges.push(graph.offsets[next]);
        }
    }
    None
}

fn inventory_instances(
    evidence: &EvidenceBundle,
    graph: &AddressGraph,
    vertices: &[usize],
) -> Vec<BehaviorInstance> {
    let use_usd = evidence
        .sales
        .iter()
        .any(|sale| sale.usd_amount.is_some());
    let mut total_nfts = BTreeSet::new();
    let mut total_value = 0.0_f64;
    let selected_vertices = vertices
        .iter()
        .enumerate()
        .map(|(slot, &vertex)| (graph.addresses[vertex].as_str(), slot))
        .collect::<AHashMap<_, _>>();
    let mut inbound = (0..vertices.len())
        .map(|_| Vec::<PropEvent>::new())
        .collect::<Vec<_>>();
    for (event_index, transfer) in evidence.transfers.iter().enumerate() {
        total_nfts.insert(transfer.token_id.as_str());
        if let Some(slot) = selected_vertices.get(transfer.to.as_str()) {
            inbound[*slot].push(PropEvent::Transfer(event_index));
        }
    }
    for (event_index, sale) in evidence.sales.iter().enumerate() {
        total_nfts.insert(sale.token_id.as_str());
        let value = if use_usd {
            sale.usd_amount.unwrap_or(0.0)
        } else {
            sale.native_amount.unwrap_or(0.0)
        }
        .max(0.0);
        total_value += value;
        if let Some(slot) = selected_vertices.get(sale.buyer.as_str()) {
            inbound[*slot].push(PropEvent::Sale(event_index));
        }
    }
    vertices
        .iter()
        .enumerate()
        .map(|(slot, _)| {
            let selected = &inbound[slot];
            let mut instance =
                instance_from_prop_events(BehaviorKind::InventoryConcentration, evidence, selected);
            let sources = selected
                .iter()
                .filter_map(|event| match event {
                    PropEvent::Transfer(index) => Some(evidence.transfers[*index].from.as_str()),
                    PropEvent::Sale(index) => Some(evidence.sales[*index].seller.as_str()),
                })
                .collect::<BTreeSet<_>>();
            instance.source_address_count = Some(sources.len() as u64);
            instance.nft_share = (!total_nfts.is_empty())
                .then(|| instance.nfts.len() as f64 / total_nfts.len() as f64);
            let selected_value = if use_usd {
                instance.usd_value
            } else {
                instance.native_value
            };
            instance.value_share =
                (total_value > 0.0).then(|| selected_value / total_value);
            instance
        })
        .collect()
}

fn instance_from_sales(kind: BehaviorKind, sales: &[&SaleEvent]) -> BehaviorInstance {
    let mut instance = BehaviorInstance {
        kind,
        ..Default::default()
    };
    for sale in sales {
        absorb_sale(&mut instance, sale);
    }
    finalize_instance(&mut instance);
    instance
}

fn instance_from_prop_events(
    kind: BehaviorKind,
    evidence: &EvidenceBundle,
    events: &[PropEvent],
) -> BehaviorInstance {
    let mut instance = BehaviorInstance {
        kind,
        ..Default::default()
    };
    for event in events {
        match event {
            PropEvent::Transfer(index) => absorb_transfer(&mut instance, &evidence.transfers[*index]),
            PropEvent::Sale(index) => absorb_sale(&mut instance, &evidence.sales[*index]),
        }
    }
    finalize_instance(&mut instance);
    instance
}

fn absorb_transfer(instance: &mut BehaviorInstance, transfer: &TransferEvent) {
    instance.edge_count += 1;
    instance.addresses.push(transfer.from.clone());
    instance.addresses.push(transfer.to.clone());
    instance.transactions.push(transfer.tx_hash.clone());
    instance.nfts.push(transfer.token_id.clone());
    instance.start_timestamp = min_option(instance.start_timestamp, transfer.timestamp);
    instance.end_timestamp = max_option(instance.end_timestamp, transfer.timestamp);
    instance.start_block = min_option(instance.start_block, transfer.block_number);
    instance.end_block = max_option(instance.end_block, transfer.block_number);
}

fn absorb_sale(instance: &mut BehaviorInstance, sale: &SaleEvent) {
    instance.edge_count += 1;
    instance.addresses.push(sale.seller.clone());
    instance.addresses.push(sale.buyer.clone());
    instance.transactions.push(sale.tx_hash.clone());
    instance.nfts.push(sale.token_id.clone());
    instance.start_timestamp = min_option(instance.start_timestamp, sale.timestamp);
    instance.end_timestamp = max_option(instance.end_timestamp, sale.timestamp);
    instance.start_block = min_option(instance.start_block, sale.block_number);
    instance.end_block = max_option(instance.end_block, sale.block_number);
    instance.native_value += sale.native_amount.unwrap_or(0.0).max(0.0);
    instance.usd_value += sale.usd_amount.unwrap_or(0.0).max(0.0);
}

fn finalize_instance(instance: &mut BehaviorInstance) {
    instance.addresses.sort();
    instance.addresses.dedup();
    instance.transactions.sort();
    instance.transactions.dedup();
    instance.nfts.sort();
    instance.nfts.dedup();
}

fn min_option<T: Ord>(left: Option<T>, right: Option<T>) -> Option<T> {
    left.into_iter().chain(right).min()
}

fn max_option<T: Ord>(left: Option<T>, right: Option<T>) -> Option<T> {
    left.into_iter().chain(right).max()
}
