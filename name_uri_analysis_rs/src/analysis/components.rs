use super::*;

#[derive(Default)]
pub(crate) struct ComponentAccumulator {
    primary_contract_count: i64,
    primary_nft_count: i64,
    total_contract_count: i64,
    first_chain: Option<usize>,
    multiple_chains: bool,
    has_secondary: bool,
}

pub(crate) struct DenseComponentScratch {
    components: Vec<ComponentAccumulator>,
    touched_roots: Vec<usize>,
}

impl DenseComponentScratch {
    pub(crate) fn new(size: usize) -> Self {
        let mut components = Vec::with_capacity(size);
        components.resize_with(size, ComponentAccumulator::default);
        Self {
            components,
            touched_roots: Vec::new(),
        }
    }

    pub(crate) fn clear_touched(&mut self) {
        for root in self.touched_roots.drain(..) {
            self.components[root] = ComponentAccumulator::default();
        }
    }
}

pub(crate) fn summarize_components_for_primary_with_scratch(
    atoms: &[NameAtom],
    primary_atoms: &[usize],
    union_find: &mut UnionFind,
    scratch: &mut DenseComponentScratch,
) -> GroupSummary {
    for &index in primary_atoms {
        let atom = &atoms[index];
        let root = union_find.find(index);
        let component = &mut scratch.components[root];
        if component.total_contract_count == 0 && component.primary_contract_count == 0 {
            scratch.touched_roots.push(root);
        }
        component.total_contract_count += atom.contract_count;
        component.primary_contract_count += atom.contract_count;
        component.primary_nft_count += atom.nft_count;
    }

    let mut summary = GroupSummary::default();
    for &root in &scratch.touched_roots {
        let component = &scratch.components[root];
        if component.primary_contract_count == 0 || component.total_contract_count < 2 {
            continue;
        }
        summary.group_count += 1;
        summary.duplicate_contract_count += component.primary_contract_count;
        summary.duplicate_nft_count += component.primary_nft_count;
        summary.group_size_ge_2_count += i64::from(component.total_contract_count >= 2);
        summary.group_size_gt_2_count += i64::from(component.total_contract_count > 2);
    }
    scratch.clear_touched();
    summary
}

pub(crate) fn summarize_sparse_components_for_chain_pair(
    atoms: &[NameAtom],
    union_find: &mut SparseUnionFind,
    left_chain: usize,
    right_chain: usize,
) -> (GroupSummary, GroupSummary) {
    let mut components: HashMap<usize, PairComponentAccumulator> = HashMap::new();
    for local_index in 0..union_find.atom_count() {
        let index = union_find.atom_at(local_index);
        let atom = &atoms[index];
        let root = union_find.find_local(local_index);
        let component = components.entry(root).or_default();
        component.total_contract_count += atom.contract_count;
        if atom.chain_index == left_chain {
            component.left_contract_count += atom.contract_count;
            component.left_nft_count += atom.nft_count;
        } else if atom.chain_index == right_chain {
            component.right_contract_count += atom.contract_count;
            component.right_nft_count += atom.nft_count;
        }
    }

    let mut left_summary = GroupSummary::default();
    let mut right_summary = GroupSummary::default();
    for component in components.values() {
        accumulate_pair_component_summary(
            &mut left_summary,
            component.left_contract_count,
            component.left_nft_count,
            component.right_contract_count,
            component.total_contract_count,
        );
        accumulate_pair_component_summary(
            &mut right_summary,
            component.right_contract_count,
            component.right_nft_count,
            component.left_contract_count,
            component.total_contract_count,
        );
    }
    (left_summary, right_summary)
}

pub(crate) fn accumulate_pair_component_summary(
    summary: &mut GroupSummary,
    primary_contract_count: i64,
    primary_nft_count: i64,
    secondary_contract_count: i64,
    total_contract_count: i64,
) {
    if primary_contract_count == 0 || secondary_contract_count == 0 || total_contract_count < 2 {
        return;
    }
    summary.group_count += 1;
    summary.duplicate_contract_count += primary_contract_count;
    summary.duplicate_nft_count += primary_nft_count;
    summary.group_size_ge_2_count += i64::from(total_contract_count >= 2);
    summary.group_size_gt_2_count += i64::from(total_contract_count > 2);
}

pub(crate) fn summarize_sparse_components_for_primary(
    atoms: &[NameAtom],
    union_find: &mut SparseUnionFind,
    primary: usize,
) -> GroupSummary {
    let mut components: HashMap<usize, ComponentAccumulator> = HashMap::new();
    for local_index in 0..union_find.atom_count() {
        let index = union_find.atom_at(local_index);
        let atom = &atoms[index];
        let root = union_find.find_local(local_index);
        let component = components.entry(root).or_default();
        component.total_contract_count += atom.contract_count;
        match component.first_chain {
            Some(first) if first != atom.chain_index => component.multiple_chains = true,
            None => component.first_chain = Some(atom.chain_index),
            _ => {}
        }
        if atom.chain_index != primary {
            component.has_secondary = true;
        } else {
            component.primary_contract_count += atom.contract_count;
            component.primary_nft_count += atom.nft_count;
        }
    }

    let mut summary = GroupSummary::default();
    for component in components.values() {
        if component.primary_contract_count == 0
            || !component.has_secondary
            || component.total_contract_count < 2
        {
            continue;
        }
        summary.group_count += 1;
        summary.duplicate_contract_count += component.primary_contract_count;
        summary.duplicate_nft_count += component.primary_nft_count;
        summary.group_size_ge_2_count += i64::from(component.total_contract_count >= 2);
        summary.group_size_gt_2_count += i64::from(component.total_contract_count > 2);
    }
    summary
}
