use crate::analysis::attribution::AddressRole;
use crate::model::{EventKind, NftKey, NormalizedEvent, PropagationFacts};
use ahash::{AHashMap, AHashSet};
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct PropagationAnalysis {
    pub facts: PropagationFacts,
    pub nft_count: u64,
    pub transaction_count: u64,
    pub event_kind_counts: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default)]
pub struct PropagationGraph {
    pub addresses: Vec<Arc<str>>,
    pub offsets: Vec<usize>,
    pub edges: Vec<usize>,
}

impl PropagationGraph {
    pub fn build(events: &[NormalizedEvent]) -> Self {
        Self::build_filtered(events, |_| true)
    }

    pub fn build_filtered(
        events: &[NormalizedEvent],
        include: impl Fn(&NormalizedEvent) -> bool,
    ) -> Self {
        let mut unique_addresses = ahash::AHashSet::<&Arc<str>>::new();
        for event in events.iter().filter(|event| include(event)) {
            unique_addresses.extend(event.from.iter());
            unique_addresses.extend(event.to.iter());
        }
        let mut addresses = unique_addresses.into_iter().cloned().collect::<Vec<_>>();
        addresses.sort();
        addresses.dedup();
        let by_address = addresses
            .iter()
            .enumerate()
            .map(|(index, address)| (address.as_ref(), index))
            .collect::<AHashMap<_, _>>();
        let mut pairs = Vec::with_capacity(events.len());
        for event in events.iter().filter(|event| include(event)) {
            let (Some(from), Some(to)) = (&event.from, &event.to) else {
                continue;
            };
            let from_id = by_address[from.as_ref()];
            let to_id = by_address[to.as_ref()];
            pairs.push((from_id, to_id));
        }
        pairs.sort_unstable();
        pairs.dedup();
        let mut offsets = vec![0; addresses.len() + 1];
        for &(from, _) in &pairs {
            offsets[from + 1] += 1;
        }
        for vertex in 0..addresses.len() {
            offsets[vertex + 1] += offsets[vertex];
        }
        let edges = pairs.into_iter().map(|(_, to)| to).collect();
        Self {
            addresses,
            offsets,
            edges,
        }
    }

    pub fn strongly_connected_components(&self) -> Vec<Vec<usize>> {
        let mut state = Tarjan::new(self);
        for vertex in 0..self.addresses.len() {
            if state.indices[vertex].is_none() {
                state.visit_iterative(vertex);
            }
        }
        state.components
    }
}

pub fn summarize(
    events: &[NormalizedEvent],
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: &[(NftKey, Arc<str>)],
) -> PropagationFacts {
    analyze(events, roles, holders).facts
}

pub fn analyze(
    events: &[NormalizedEvent],
    roles: &BTreeMap<Arc<str>, AddressRole>,
    holders: &[(NftKey, Arc<str>)],
) -> PropagationAnalysis {
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
    let victims = roles
        .iter()
        .filter(|(_, role)| {
            matches!(
                role,
                AddressRole::LikelyVictim | AddressRole::CorruptedVictim
            )
        })
        .map(|(address, _)| address.as_ref())
        .collect::<AHashSet<_>>();
    let likely_honest = roles
        .values()
        .filter(|role| **role == AddressRole::LikelyVictim)
        .count() as u64;
    let holding_victims = holders
        .iter()
        .filter_map(|(_, address)| {
            victims
                .contains(address.as_ref())
                .then_some(address.as_ref())
        })
        .collect::<AHashSet<_>>()
        .len() as u64;
    let mut facts = PropagationFacts {
        malicious_address_count: malicious.len() as u64,
        victim_address_count: victims.len() as u64,
        likely_honest_address_count: likely_honest,
        currently_holding_victim_address_count: holding_victims,
        ..Default::default()
    };
    let mut receiver_values = BTreeMap::<&Arc<str>, (i128, i128)>::new();
    let mut nfts = holders.iter().map(|(nft, _)| nft).collect::<AHashSet<_>>();
    let mut transactions = AHashSet::new();
    let mut event_kind_counts = BTreeMap::<String, u64>::new();
    let mut total_native = 0_i128;
    let mut total_usd = 0_i128;
    for event in events {
        let channel = event.value_channel();
        nfts.extend(event.nft.iter());
        transactions.insert((event.chain, event.tx_id.as_ref()));
        *event_kind_counts
            .entry(event.kind.as_str().to_owned())
            .or_default() += 1;
        match event.kind {
            EventKind::Mint => facts.mint_edge_count += 1,
            EventKind::Transfer => facts.transfer_edge_count += 1,
            EventKind::Sale if event.is_nft_sale() => facts.sale_edge_count += 1,
            EventKind::Funding => facts.funding_edge_count += 1,
            EventKind::Withdrawal => facts.withdrawal_edge_count += 1,
            EventKind::Cashout => facts.cashout_edge_count += 1,
            EventKind::Deploy | EventKind::Listing | EventKind::Sale => {}
        }
        if event.is_nft_propagation() {
            facts.propagation_edge_count += 1;
        }
        if matches!(
            channel,
            crate::model::ValueChannel::MintPayment | crate::model::ValueChannel::SalePayment
        ) {
            facts.gross_revenue_edge_count += 1;
        }
        if channel == crate::model::ValueChannel::SalePayment
            && (event.marketplace_fee_native.unwrap_or(0) > 0
                || event.marketplace_fee_usd_micros.unwrap_or(0) > 0)
        {
            facts.marketplace_fee_edge_count += 1;
        }
        let recipient = event.payment_recipient.as_ref().or(match channel {
            crate::model::ValueChannel::SalePayment | crate::model::ValueChannel::MintPayment => {
                event.from.as_ref()
            }
            crate::model::ValueChannel::RoyaltyFee => None,
            _ => event.to.as_ref(),
        });
        if (recipient.is_some_and(|address| malicious.contains(address.as_ref()))
            && matches!(
                channel,
                crate::model::ValueChannel::MintPayment
                    | crate::model::ValueChannel::SalePayment
                    | crate::model::ValueChannel::RoyaltyFee
            ))
            || (channel == crate::model::ValueChannel::ExitPayment
                && event
                    .from
                    .as_ref()
                    .is_some_and(|address| malicious.contains(address.as_ref())))
        {
            facts.operator_revenue_edge_count += 1;
        }
        if matches!(event.kind, EventKind::Withdrawal | EventKind::Cashout)
            && event
                .from
                .as_ref()
                .is_some_and(|address| malicious.contains(address.as_ref()))
            && event
                .to
                .as_ref()
                .is_some_and(|address| malicious.contains(address.as_ref()))
        {
            facts.revenue_backflow_edge_count += 1;
        }
        let Some(recipient) = recipient else {
            continue;
        };
        let native = event.native_amount.unwrap_or(0).max(0);
        let usd = event.usd_micros.unwrap_or(0).max(0);
        if native == 0 && usd == 0 {
            continue;
        }
        let value = receiver_values.entry(recipient).or_default();
        value.0 = value.0.saturating_add(native);
        value.1 = value.1.saturating_add(usd);
        total_native = total_native.saturating_add(native);
        total_usd = total_usd.saturating_add(usd);
    }
    let use_usd = total_usd > 0;
    if let Some((receiver, (native, usd))) =
        receiver_values
            .into_iter()
            .max_by(|(left_address, left), (right_address, right)| {
                let left_value = if use_usd { left.1 } else { left.0 };
                let right_value = if use_usd { right.1 } else { right.0 };
                left_value
                    .cmp(&right_value)
                    .then_with(|| right_address.cmp(left_address))
            })
    {
        facts.max_value_receiver = Some(receiver.clone());
        facts.max_value_receiver_native = native;
        facts.max_value_receiver_usd_micros = usd;
        let denominator = if use_usd { total_usd } else { total_native };
        let numerator = if use_usd { usd } else { native };
        facts.max_value_receiver_share =
            (denominator > 0).then(|| numerator as f64 / denominator as f64);
    }
    PropagationAnalysis {
        facts,
        nft_count: nfts.len() as u64,
        transaction_count: transactions.len() as u64,
        event_kind_counts,
    }
}

struct Tarjan<'a> {
    graph: &'a PropagationGraph,
    next_index: usize,
    indices: Vec<Option<usize>>,
    lowlink: Vec<usize>,
    stack: Vec<usize>,
    on_stack: Vec<bool>,
    components: Vec<Vec<usize>>,
}

impl<'a> Tarjan<'a> {
    fn new(graph: &'a PropagationGraph) -> Self {
        let count = graph.addresses.len();
        Self {
            graph,
            next_index: 0,
            indices: vec![None; count],
            lowlink: vec![0; count],
            stack: Vec::new(),
            on_stack: vec![false; count],
            components: Vec::new(),
        }
    }

    fn discover(&mut self, vertex: usize) {
        let index = self.next_index;
        self.next_index += 1;
        self.indices[vertex] = Some(index);
        self.lowlink[vertex] = index;
        self.stack.push(vertex);
        self.on_stack[vertex] = true;
    }

    fn visit_iterative(&mut self, start: usize) {
        self.discover(start);
        let mut frames = vec![DfsFrame {
            vertex: start,
            next_edge: self.graph.offsets[start],
            parent: None,
        }];
        while let Some(frame) = frames.last_mut() {
            let vertex = frame.vertex;
            if frame.next_edge < self.graph.offsets[vertex + 1] {
                let next = self.graph.edges[frame.next_edge];
                frame.next_edge += 1;
                if self.indices[next].is_none() {
                    self.discover(next);
                    frames.push(DfsFrame {
                        vertex: next,
                        next_edge: self.graph.offsets[next],
                        parent: Some(vertex),
                    });
                } else if self.on_stack[next] {
                    self.lowlink[vertex] = self.lowlink[vertex]
                        .min(self.indices[next].expect("visited vertex has an index"));
                }
                continue;
            }

            let completed = frames.pop().expect("active DFS frame is present");
            if let Some(parent) = completed.parent {
                self.lowlink[parent] = self.lowlink[parent].min(self.lowlink[completed.vertex]);
            }
            let index = self.indices[completed.vertex].expect("discovered vertex has an index");
            if self.lowlink[completed.vertex] == index {
                let mut component = Vec::new();
                loop {
                    let member = self.stack.pop().expect("Tarjan stack is nonempty");
                    self.on_stack[member] = false;
                    component.push(member);
                    if member == completed.vertex {
                        break;
                    }
                }
                component.sort_unstable();
                self.components.push(component);
            }
        }
    }
}

struct DfsFrame {
    vertex: usize,
    next_edge: usize,
    parent: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ChainId;

    #[test]
    fn iterative_tarjan_handles_a_deep_cycle() {
        let count = 50_000;
        let addresses = (0..count)
            .map(|id| Arc::<str>::from(id.to_string()))
            .collect::<Vec<_>>();
        let offsets = (0..=count).collect::<Vec<_>>();
        let mut edges = (1..count).collect::<Vec<_>>();
        edges.push(0);
        let graph = PropagationGraph {
            addresses,
            offsets,
            edges,
        };
        let components = graph.strongly_connected_components();
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].len(), count);
    }

    #[test]
    fn propagation_summary_counts_edges_roles_and_max_receiver() {
        let event = |index, kind, from: &str, to: &str, usd| NormalizedEvent {
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
            native_amount: Some(usd),
            usd_micros: Some(usd),
            gas_native: None,
            gas_usd_micros: None,
            marketplace_fee_native: None,
            marketplace_fee_usd_micros: None,
        };
        let events = [
            event(0, EventKind::Sale, "operator", "buyer", 10),
            event(1, EventKind::Funding, "funder", "operator", 20),
            event(2, EventKind::Cashout, "operator", "colluder", 5),
        ];
        let roles = BTreeMap::from([
            (Arc::from("operator"), AddressRole::SuspectedOperator),
            (Arc::from("colluder"), AddressRole::SuspectedColluder),
            (Arc::from("buyer"), AddressRole::LikelyVictim),
        ]);
        let nft = NftKey {
            chain: ChainId::Ethereum,
            contract_address: Arc::from("contract"),
            token_id: Arc::from("1"),
        };
        let holders = [(nft, Arc::from("buyer"))];
        let facts = summarize(&events, &roles, &holders);
        assert_eq!(facts.sale_edge_count, 1);
        assert_eq!(facts.funding_edge_count, 1);
        assert_eq!(facts.cashout_edge_count, 1);
        assert_eq!(facts.operator_revenue_edge_count, 1);
        assert_eq!(facts.revenue_backflow_edge_count, 1);
        assert_eq!(facts.malicious_address_count, 2);
        assert_eq!(facts.victim_address_count, 1);
        assert_eq!(facts.currently_holding_victim_address_count, 1);
        assert_eq!(facts.max_value_receiver.as_deref(), Some("operator"));
        assert_eq!(facts.max_value_receiver_usd_micros, 30);
    }
}
