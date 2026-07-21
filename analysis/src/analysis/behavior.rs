use crate::analysis::attribution::AddressRole;
use crate::analysis::propagation::PropagationGraph;
use crate::model::{BehaviorFacts, BehaviorInstance, BehaviorKind, NftKey, NormalizedEvent};
use ahash::{AHashMap, AHashSet};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Default)]
struct CycleValues {
    end: Option<i64>,
    internal_native_sum: i128,
    internal_native_count: u64,
    internal_usd_sum: i128,
    internal_usd_count: u64,
    exit_native_sum: i128,
    exit_native_count: u64,
    exit_usd_sum: i128,
    exit_usd_count: u64,
    internal_events: Vec<usize>,
    exit_events: Vec<usize>,
}

pub struct BehaviorAnalysis {
    pub facts: BehaviorFacts,
    pub instances: Vec<BehaviorInstance>,
}

pub fn detect_behaviors(
    events: &[NormalizedEvent],
    graph: &PropagationGraph,
    roles: &BTreeMap<Arc<str>, AddressRole>,
    components: &[Vec<usize>],
) -> BehaviorAnalysis {
    analyze_behaviors(events, graph, roles, components, true)
}

pub fn detect_behavior_facts(
    events: &[NormalizedEvent],
    graph: &PropagationGraph,
    roles: &BTreeMap<Arc<str>, AddressRole>,
    components: &[Vec<usize>],
) -> BehaviorFacts {
    analyze_behaviors(events, graph, roles, components, false).facts
}

fn analyze_behaviors(
    events: &[NormalizedEvent],
    graph: &PropagationGraph,
    roles: &BTreeMap<Arc<str>, AddressRole>,
    components: &[Vec<usize>],
    collect_instances: bool,
) -> BehaviorAnalysis {
    let malicious = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::SuspectedOperator | AddressRole::SuspectedColluder
            )
        })
        .map(|(address, _)| address.as_ref())
        .collect::<AHashSet<_>>();
    let honest = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::LikelyVictim | AddressRole::CorruptedVictim
            )
        })
        .map(|(address, _)| address.as_ref())
        .collect::<AHashSet<_>>();
    let sale_graph = PropagationGraph::build_filtered(events, |event| {
        event.is_nft_sale()
            && event
                .from
                .as_deref()
                .is_some_and(|address| malicious.contains(address))
            && event
                .to
                .as_deref()
                .is_some_and(|address| malicious.contains(address))
    });
    let sale_components = sale_graph.strongly_connected_components();
    let wash_components = sale_components
        .iter()
        .filter(|component| component.len() >= 2)
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    let mut wash_by_address = AHashMap::new();
    for (wash_id, component) in wash_components.iter().enumerate() {
        for &vertex in component.iter() {
            wash_by_address.insert(sale_graph.addresses[vertex].as_ref(), wash_id);
        }
    }

    let mut cycle_values = (0..wash_components.len())
        .map(|_| CycleValues::default())
        .collect::<Vec<_>>();
    for (event_index, event) in events.iter().enumerate() {
        let Some(from) = event.from.as_deref() else {
            continue;
        };
        if !event.is_nft_sale() {
            continue;
        }
        let Some(to) = event.to.as_deref() else {
            continue;
        };
        if let (Some(&left), Some(&right)) = (wash_by_address.get(from), wash_by_address.get(to)) {
            if left == right {
                let values = &mut cycle_values[left];
                values.end = [values.end, event.timestamp].into_iter().flatten().max();
                add_value(
                    values,
                    event.native_amount,
                    event.usd_micros,
                    ValueSide::Internal,
                );
                if collect_instances {
                    values.internal_events.push(event_index);
                }
            }
        }
    }
    for (event_index, event) in events.iter().enumerate() {
        if !event.is_nft_sale() {
            continue;
        }
        let Some(from) = event.from.as_deref() else {
            continue;
        };
        let Some(&wash_id) = wash_by_address.get(from) else {
            continue;
        };
        let Some(to) = event.to.as_deref() else {
            continue;
        };
        let values = &mut cycle_values[wash_id];
        if honest.contains(to)
            && wash_by_address.get(to).copied() != Some(wash_id)
            && values
                .end
                .zip(event.timestamp)
                .is_some_and(|(end, exit)| exit > end)
        {
            add_value(
                values,
                event.native_amount,
                event.usd_micros,
                ValueSide::Exit,
            );
            if collect_instances {
                values.exit_events.push(event_index);
            }
        }
    }
    let pump_and_exit = cycle_values
        .iter()
        .filter(|values| exit_price_exceeds_internal(values))
        .count() as u64;

    let star = star_behaviors(
        events,
        graph,
        components,
        roles,
        &malicious,
        collect_instances,
    );
    let (layered_transfer, layered_instances) = if collect_instances {
        let instances = layered_paths(events, graph, 3);
        (instances.len() as u64, instances)
    } else {
        (count_layered_paths(graph, 3), Vec::new())
    };
    let mut indegree = vec![0_u64; graph.addresses.len()];
    for &target in &graph.edges {
        indegree[target] += 1;
    }
    let inventory_vertices = indegree
        .iter()
        .enumerate()
        .filter_map(|(vertex, degree)| {
            (malicious.contains(graph.addresses[vertex].as_ref())
                && (*degree >= 3
                    || (*degree > 0 && graph.offsets[vertex + 1] - graph.offsets[vertex] >= 3)))
                .then_some(vertex)
        })
        .collect::<Vec<_>>();
    let inventory_concentration = inventory_vertices.len() as u64;
    let mut instances = if collect_instances {
        let mut instances = cycle_instances(events, &sale_graph, &wash_components, &cycle_values);
        instances.extend(
            cycle_values
                .iter()
                .enumerate()
                .filter(|(_, values)| exit_price_exceeds_internal(values))
                .map(|(cycle, values)| {
                    pump_instance(events, &sale_graph, wash_components[cycle], values)
                }),
        );
        instances.extend(star.instances);
        instances.extend(layered_instances);
        instances.extend(inventory_instances(events, graph, &inventory_vertices));
        instances
    } else {
        Vec::new()
    };
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
    events: &[NormalizedEvent],
    graph: &PropagationGraph,
    components: &[Vec<usize>],
    roles: &BTreeMap<Arc<str>, AddressRole>,
    malicious: &AHashSet<&str>,
    collect_instances: bool,
) -> StarAnalysis {
    let mut component_by_vertex = vec![0_usize; graph.addresses.len()];
    let mut malicious_component = vec![false; components.len()];
    for (component_id, component) in components.iter().enumerate() {
        for &vertex in component {
            component_by_vertex[vertex] = component_id;
            malicious_component[component_id] |=
                malicious.contains(graph.addresses[vertex].as_ref());
        }
    }
    let address_ids = graph
        .addresses
        .iter()
        .enumerate()
        .map(|(id, address)| (address.as_ref(), id))
        .collect::<AHashMap<_, _>>();
    let mut dag_targets = (0..components.len())
        .map(|_| BTreeSet::new())
        .collect::<Vec<_>>();
    let mut valuable_outgoing = vec![false; components.len()];
    let mut outgoing_events = if collect_instances {
        (0..components.len())
            .map(|_| Vec::new())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    for (event_index, event) in events.iter().enumerate() {
        if !event.is_nft_propagation() {
            continue;
        }
        let (Some(from), Some(to)) = (
            event
                .from
                .as_deref()
                .and_then(|address| address_ids.get(address)),
            event
                .to
                .as_deref()
                .and_then(|address| address_ids.get(address)),
        ) else {
            continue;
        };
        let left = component_by_vertex[*from];
        let right = component_by_vertex[*to];
        if left == right {
            continue;
        }
        dag_targets[left].insert(right);
        if collect_instances {
            outgoing_events[left].push(event_index);
        }
        valuable_outgoing[left] |= event.is_nft_sale()
            || event.native_amount.unwrap_or(0) > 0
            || event.usd_micros.unwrap_or(0) > 0;
    }
    let mut instances = Vec::new();
    let mut sybil = 0_u64;
    let mut fraud = 0_u64;
    let mut poisoning = 0_u64;
    for component in 0..components.len() {
        if !malicious_component[component] || dag_targets[component].len() < 3 {
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
            _ => unreachable!("star detector emits only star behavior kinds"),
        }
        if !collect_instances {
            continue;
        }
        let selected = outgoing_events[component]
            .iter()
            .map(|&index| &events[index])
            .collect::<Vec<_>>();
        let mut instance = instance_from_events(kind, selected.iter().copied());
        instance.fan_out = Some(dag_targets[component].len() as u64);
        instance.linked_buyers = instance
            .addresses
            .iter()
            .filter(|address| {
                roles.get(*address).is_some_and(|role| {
                    matches!(
                        role,
                        AddressRole::LikelyVictim | AddressRole::CorruptedVictim
                    )
                })
            })
            .cloned()
            .collect();
        instance.linked_loss_native = selected
            .iter()
            .filter(|event| {
                event
                    .to
                    .as_ref()
                    .is_some_and(|address| instance.linked_buyers.binary_search(address).is_ok())
            })
            .filter_map(|event| event.native_amount)
            .filter(|value| *value > 0)
            .fold(0_i128, i128::saturating_add);
        instance.linked_loss_usd_micros = selected
            .iter()
            .filter(|event| {
                event
                    .to
                    .as_ref()
                    .is_some_and(|address| instance.linked_buyers.binary_search(address).is_ok())
            })
            .filter_map(|event| event.usd_micros)
            .filter(|value| *value > 0)
            .fold(0_i128, i128::saturating_add);
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
enum ValueSide {
    Internal,
    Exit,
}

fn add_value(values: &mut CycleValues, native: Option<i128>, usd: Option<i128>, side: ValueSide) {
    if let Some(native) = native.filter(|value| *value >= 0) {
        match side {
            ValueSide::Internal => {
                values.internal_native_sum = values.internal_native_sum.saturating_add(native);
                values.internal_native_count += 1;
            }
            ValueSide::Exit => {
                values.exit_native_sum = values.exit_native_sum.saturating_add(native);
                values.exit_native_count += 1;
            }
        }
    }
    if let Some(usd) = usd.filter(|value| *value >= 0) {
        match side {
            ValueSide::Internal => {
                values.internal_usd_sum = values.internal_usd_sum.saturating_add(usd);
                values.internal_usd_count += 1;
            }
            ValueSide::Exit => {
                values.exit_usd_sum = values.exit_usd_sum.saturating_add(usd);
                values.exit_usd_count += 1;
            }
        }
    }
}

fn exit_price_exceeds_internal(values: &CycleValues) -> bool {
    if values.internal_usd_count > 0 && values.exit_usd_count > 0 {
        return average_exceeds(
            values.exit_usd_sum,
            values.exit_usd_count,
            values.internal_usd_sum,
            values.internal_usd_count,
        );
    }
    values.internal_native_count > 0
        && values.exit_native_count > 0
        && average_exceeds(
            values.exit_native_sum,
            values.exit_native_count,
            values.internal_native_sum,
            values.internal_native_count,
        )
}

fn average_exceeds(left_sum: i128, left_count: u64, right_sum: i128, right_count: u64) -> bool {
    match (
        left_sum.checked_mul(i128::from(right_count)),
        right_sum.checked_mul(i128::from(left_count)),
    ) {
        (Some(left), Some(right)) => left > right,
        _ => left_sum as f64 / left_count as f64 > right_sum as f64 / right_count as f64,
    }
}

fn cycle_instances(
    events: &[NormalizedEvent],
    graph: &PropagationGraph,
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
                .map(|&index| &events[index])
                .collect::<Vec<_>>();
            let mut instance =
                instance_from_events(BehaviorKind::WashTrading, selected.iter().copied());
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
    events: &[NormalizedEvent],
    graph: &PropagationGraph,
    component: &[usize],
    values: &CycleValues,
) -> BehaviorInstance {
    let selected = values
        .exit_events
        .iter()
        .map(|&index| &events[index])
        .collect::<Vec<_>>();
    let mut instance = instance_from_events(BehaviorKind::PumpAndExit, selected.iter().copied());
    instance.addresses.extend(
        component
            .iter()
            .map(|&vertex| graph.addresses[vertex].clone()),
    );
    instance.addresses.sort();
    instance.addresses.dedup();
    instance.linked_buyers = selected
        .iter()
        .filter_map(|event| event.to.clone())
        .collect();
    instance.linked_buyers.sort();
    instance.linked_buyers.dedup();
    instance.linked_loss_native = values.exit_native_sum;
    instance.linked_loss_usd_micros = values.exit_usd_sum;
    instance.exit_to_internal_price_ratio = comparable_average_ratio(values);
    let cycle_nfts = values
        .internal_events
        .iter()
        .filter_map(|&index| events[index].nft.as_ref())
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
    let internal = internal_sum as f64 / internal_count as f64;
    if internal > 0.0 {
        Some((exit_sum as f64 / exit_count as f64) / internal)
    } else {
        None
    }
}

fn participant_distributions(
    addresses: &[Arc<str>],
    events: &[&NormalizedEvent],
) -> (Vec<u64>, Vec<u64>) {
    let mut nfts = addresses
        .iter()
        .cloned()
        .map(|address| (address, BTreeSet::<&NftKey>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut transactions = addresses
        .iter()
        .cloned()
        .map(|address| (address, 0_u64))
        .collect::<BTreeMap<_, _>>();
    for event in events {
        for address in event.from.iter().chain(event.to.iter()) {
            if let Some(nft) = &event.nft {
                nfts.entry(address.clone()).or_default().insert(nft);
            }
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
    events: &[NormalizedEvent],
    graph: &PropagationGraph,
    minimum_addresses: usize,
) -> Vec<BehaviorInstance> {
    let address_ids = graph
        .addresses
        .iter()
        .enumerate()
        .map(|(index, address)| (address.as_ref(), index))
        .collect::<AHashMap<_, _>>();
    let mut edge_events = AHashMap::<(usize, usize), Vec<usize>>::new();
    for (event_index, event) in events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.is_nft_propagation())
    {
        let (Some(from), Some(to)) = (
            event
                .from
                .as_deref()
                .and_then(|address| address_ids.get(address)),
            event
                .to
                .as_deref()
                .and_then(|address| address_ids.get(address)),
        ) else {
            continue;
        };
        edge_events
            .entry((*from, *to))
            .or_default()
            .push(event_index);
    }
    let Some(path) = first_layered_path(graph, minimum_addresses) else {
        return Vec::new();
    };
    let addresses = path
        .iter()
        .map(|&member| graph.addresses[member].clone())
        .collect::<Vec<_>>();
    // Preserve the established path semantics: one concrete event per hop.
    // Including every duplicate event for an address pair would multiply the
    // same path's values and could make a dense candidate artifact explode.
    let selected_indices = path
        .windows(2)
        .filter_map(|pair| edge_events.get(&(pair[0], pair[1]))?.first().copied())
        .collect::<Vec<_>>();
    if selected_indices.len() + 1 != path.len() {
        return Vec::new();
    }
    let mut instance = instance_from_events(
        BehaviorKind::LayeredTransfer,
        selected_indices.iter().map(|&index| &events[index]),
    );
    instance.addresses = addresses;
    instance.path_length = Some(minimum_addresses as u64);
    instance.low_value_hops = Some(
        selected_indices
            .iter()
            .filter(|&&event_index| {
                let event = &events[event_index];
                event.usd_micros.is_some_and(|value| value <= 1_000_000)
                    && event
                        .native_amount
                        .is_some_and(|value| value <= 1_000_000_000_000_000)
            })
            .count() as u64,
    );
    vec![instance]
}

fn count_layered_paths(graph: &PropagationGraph, minimum_addresses: usize) -> u64 {
    u64::from(first_layered_path(graph, minimum_addresses).is_some())
}

fn first_layered_path(graph: &PropagationGraph, minimum_addresses: usize) -> Option<Vec<usize>> {
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
    events: &[NormalizedEvent],
    graph: &PropagationGraph,
    vertices: &[usize],
) -> Vec<BehaviorInstance> {
    let use_usd = events.iter().any(|event| event.usd_micros.is_some());
    let mut total_nfts = BTreeSet::new();
    let mut total_value = 0_i128;
    let selected_vertices = vertices
        .iter()
        .enumerate()
        .map(|(slot, &vertex)| (graph.addresses[vertex].as_ref(), slot))
        .collect::<AHashMap<_, _>>();
    let mut inbound = (0..vertices.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    for event in events {
        total_nfts.extend(event.nft.iter());
        total_value = total_value.saturating_add(
            if use_usd {
                event.usd_micros
            } else {
                event.native_amount
            }
            .unwrap_or(0)
            .max(0),
        );
        if let Some(slot) = event
            .to
            .as_deref()
            .and_then(|address| selected_vertices.get(address))
        {
            inbound[*slot].push(event);
        }
    }
    vertices
        .iter()
        .enumerate()
        .map(|(slot, _)| {
            let selected = &inbound[slot];
            let mut instance = instance_from_events(
                BehaviorKind::InventoryConcentration,
                selected.iter().copied(),
            );
            let sources = selected
                .iter()
                .filter_map(|event| event.from.clone())
                .collect::<BTreeSet<_>>();
            instance.source_address_count = Some(sources.len() as u64);
            instance.nft_share = (!total_nfts.is_empty())
                .then(|| instance.nfts.len() as f64 / total_nfts.len() as f64);
            let selected_value = if use_usd {
                instance.usd_micros
            } else {
                instance.native_value
            };
            instance.value_share =
                (total_value > 0).then(|| selected_value as f64 / total_value as f64);
            instance
        })
        .collect()
}

fn instance_from_events<'a>(
    kind: BehaviorKind,
    events: impl Iterator<Item = &'a NormalizedEvent>,
) -> BehaviorInstance {
    let mut instance = BehaviorInstance {
        kind,
        ..Default::default()
    };
    for event in events {
        instance.edge_count += 1;
        instance.addresses.extend(event.from.iter().cloned());
        instance.addresses.extend(event.to.iter().cloned());
        instance.transactions.push(event.tx_id.clone());
        instance.nfts.extend(event.nft.iter().cloned());
        instance.start_timestamp = min_option(instance.start_timestamp, event.timestamp);
        instance.end_timestamp = max_option(instance.end_timestamp, event.timestamp);
        instance.start_block = min_option(instance.start_block, event.block_number);
        instance.end_block = max_option(instance.end_block, event.block_number);
        instance.native_value = instance
            .native_value
            .saturating_add(event.native_amount.unwrap_or(0).max(0));
        instance.usd_micros = instance
            .usd_micros
            .saturating_add(event.usd_micros.unwrap_or(0).max(0));
    }
    instance.addresses.sort();
    instance.addresses.dedup();
    instance.transactions.sort();
    instance.transactions.dedup();
    instance.nfts.sort();
    instance.nfts.dedup();
    instance
}

fn min_option<T: Ord>(left: Option<T>, right: Option<T>) -> Option<T> {
    left.into_iter().chain(right).min()
}

fn max_option<T: Ord>(left: Option<T>, right: Option<T>) -> Option<T> {
    left.into_iter().chain(right).max()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ChainId, EventKind};

    fn event(index: u32, kind: EventKind, from: &str, to: &str) -> NormalizedEvent {
        NormalizedEvent {
            chain: ChainId::Ethereum,
            tx_id: Arc::from(format!("tx-{index}")),
            event_index: index,
            timestamp: Some(i64::from(index)),
            block_number: Some(u64::from(index)),
            kind,
            channel: None,
            from: Some(Arc::from(from)),
            to: Some(Arc::from(to)),
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
    fn transfer_cycle_does_not_create_a_wash_cycle() {
        let events = vec![
            event(0, EventKind::Transfer, "a", "b"),
            event(1, EventKind::Transfer, "b", "a"),
            event(2, EventKind::Sale, "a", "b"),
        ];
        let graph = PropagationGraph::build(&events);
        let components = graph.strongly_connected_components();
        let roles = BTreeMap::from([
            (Arc::from("a"), AddressRole::SuspectedOperator),
            (Arc::from("b"), AddressRole::SuspectedColluder),
        ]);
        let analysis = detect_behaviors(&events, &graph, &roles, &components);
        assert_eq!(analysis.facts.wash_cycles, 0);
    }

    #[test]
    fn reciprocal_malicious_sales_create_one_wash_cycle() {
        let events = vec![
            event(0, EventKind::Sale, "a", "b"),
            event(1, EventKind::Sale, "b", "a"),
        ];
        let graph = PropagationGraph::build(&events);
        let components = graph.strongly_connected_components();
        let roles = BTreeMap::from([
            (Arc::from("a"), AddressRole::SuspectedOperator),
            (Arc::from("b"), AddressRole::SuspectedColluder),
        ]);
        let analysis = detect_behaviors(&events, &graph, &roles, &components);
        assert_eq!(analysis.facts.wash_cycles, 1);
        let wash = analysis
            .instances
            .iter()
            .find(|instance| instance.kind == BehaviorKind::WashTrading)
            .unwrap();
        assert_eq!(
            wash.addresses.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(
            wash.transactions
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            ["tx-0", "tx-1"]
        );
        assert_eq!(wash.edge_count, 2);
    }

    #[test]
    fn layered_transfer_retains_only_the_first_deterministic_path() {
        let mut events = vec![
            event(0, EventKind::Transfer, "a", "b"),
            event(1, EventKind::Transfer, "a", "b"),
            event(2, EventKind::Transfer, "b", "c"),
            event(3, EventKind::Transfer, "b", "d"),
            event(4, EventKind::Transfer, "a", "e"),
            event(5, EventKind::Transfer, "e", "f"),
        ];
        for item in &mut events {
            item.native_amount = Some(1);
            item.usd_micros = Some(1);
        }
        let graph = PropagationGraph::build(&events);
        let instances = layered_paths(&events, &graph, 3);
        assert_eq!(instances.len(), 1);
        assert_eq!(
            instances[0]
                .addresses
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        assert_eq!(instances[0].edge_count, 2);
        assert_eq!(instances[0].native_value, 2);
        assert_eq!(count_layered_paths(&graph, 3), 1);
    }

    #[test]
    fn pump_and_exit_counts_only_attributed_honest_buyers() {
        let mut events = vec![
            event(0, EventKind::Sale, "a", "b"),
            event(1, EventKind::Sale, "b", "a"),
            event(2, EventKind::Sale, "a", "c"),
            event(3, EventKind::Sale, "a", "d"),
        ];
        events[0].usd_micros = Some(1);
        events[1].usd_micros = Some(1);
        events[2].usd_micros = Some(100);
        events[3].usd_micros = Some(10);
        let graph = PropagationGraph::build(&events);
        let components = graph.strongly_connected_components();
        let roles = BTreeMap::from([
            (Arc::from("a"), AddressRole::SuspectedOperator),
            (Arc::from("b"), AddressRole::SuspectedColluder),
            (Arc::from("c"), AddressRole::Neutral),
            (Arc::from("d"), AddressRole::LikelyVictim),
        ]);
        let analysis = detect_behaviors(&events, &graph, &roles, &components);
        assert_eq!(analysis.facts.pump_and_exit, 1);
        let pump = analysis
            .instances
            .iter()
            .find(|instance| instance.kind == BehaviorKind::PumpAndExit)
            .unwrap();
        assert_eq!(
            pump.linked_buyers
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            ["d"]
        );
        assert_eq!(pump.linked_loss_usd_micros, 10);
    }
}
