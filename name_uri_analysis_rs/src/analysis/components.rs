#![cfg_attr(not(test), allow(dead_code))]

use super::*;

#[derive(Default)]
pub(crate) struct ComponentAccumulator {
    primary_contract_count: i64,
    primary_nft_count: i64,
    total_contract_count: i64,
    has_secondary: bool,
}

pub(crate) struct DenseComponentScratch {
    components: Vec<ComponentAccumulator>,
    touched_roots: Vec<usize>,
}

impl DenseComponentScratch {
    #[cfg(test)]
    pub(crate) fn new(size: usize) -> Self {
        Self::with_touched_capacity(size, 0)
    }

    pub(crate) fn with_touched_capacity(size: usize, touched_capacity: usize) -> Self {
        let mut components = Vec::with_capacity(size);
        components.resize_with(size, ComponentAccumulator::default);
        Self {
            components,
            touched_roots: Vec::with_capacity(touched_capacity),
        }
    }

    pub(crate) fn clear_touched(&mut self) {
        for root in self.touched_roots.drain(..) {
            self.components[root] = ComponentAccumulator::default();
        }
    }

    fn clear_primary_touched(&mut self) {
        for root in self.touched_roots.drain(..) {
            let component = &mut self.components[root];
            component.primary_contract_count = 0;
            component.primary_nft_count = 0;
        }
    }
}

pub(crate) fn dense_component_scratch_bytes(
    component_count: usize,
    touched_capacity: usize,
) -> usize {
    component_count
        .saturating_mul(std::mem::size_of::<ComponentAccumulator>())
        .saturating_add(touched_capacity.saturating_mul(std::mem::size_of::<usize>()))
}

pub(crate) fn sparse_all_chain_summary_workspace_bytes(
    atom_count: usize,
    chain_count: usize,
) -> usize {
    chain_count
        .saturating_mul(std::mem::size_of::<GroupSummary>())
        .saturating_add(chain_count.saturating_mul(std::mem::size_of::<usize>()))
        .saturating_add(atom_count.saturating_mul(std::mem::size_of::<usize>()))
        .saturating_add(
            chain_count
                .saturating_add(1)
                .saturating_mul(std::mem::size_of::<usize>()),
        )
        .saturating_add(chain_count.saturating_mul(std::mem::size_of::<usize>()))
        .saturating_add(atom_count.saturating_mul(std::mem::size_of::<u32>()))
}

pub(crate) fn summarize_components_for_primary_with_scratch<A: NameAtomStore + ?Sized>(
    atoms: &A,
    primary_atoms: &[u32],
    union_find: &mut UnionFind,
    scratch: &mut DenseComponentScratch,
) -> GroupSummary {
    for &index in primary_atoms {
        let index = index as usize;
        let root = union_find.find(index);
        let component = &mut scratch.components[root];
        if component.total_contract_count == 0 && component.primary_contract_count == 0 {
            scratch.touched_roots.push(root);
        }
        let contract_count = atoms.contract_count(index);
        component.total_contract_count += contract_count;
        component.primary_contract_count += contract_count;
        component.primary_nft_count += atoms.nft_count(index);
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

#[cfg(target_pointer_width = "64")]
pub(crate) fn summarize_dense_components_for_chain_pair<A: NameAtomStore + ?Sized>(
    atoms: &A,
    mut union_find: UnionFind,
    left_atoms: &[u32],
    right_atoms: &[u32],
) -> (GroupSummary, GroupSummary) {
    for local_index in 0..union_find.parent.len() {
        union_find.find(local_index);
    }
    let UnionFind { parent, rank } = union_find;
    drop(rank);
    let mut locals = (0..parent.len() as u32).collect::<Vec<_>>();
    locals.sort_unstable_by_key(|&local| (parent[local as usize], local));

    let mut left_summary = GroupSummary::default();
    let mut right_summary = GroupSummary::default();
    let mut component_start = 0usize;
    while component_start < locals.len() {
        let root = parent[locals[component_start] as usize];
        let mut component_end = component_start;
        let mut component = PairComponentAccumulator::default();
        while component_end < locals.len() && parent[locals[component_end] as usize] == root {
            let local_index = locals[component_end] as usize;
            let (atom_index, is_left) = if local_index < left_atoms.len() {
                (left_atoms[local_index] as usize, true)
            } else {
                (right_atoms[local_index - left_atoms.len()] as usize, false)
            };
            let contract_count = atoms.contract_count(atom_index);
            component.total_contract_count += contract_count;
            if is_left {
                component.left_contract_count += contract_count;
                component.left_nft_count += atoms.nft_count(atom_index);
            } else {
                component.right_contract_count += contract_count;
                component.right_nft_count += atoms.nft_count(atom_index);
            }
            component_end += 1;
        }
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
        component_start = component_end;
    }
    (left_summary, right_summary)
}

#[cfg(not(target_pointer_width = "64"))]
pub(crate) fn summarize_dense_components_for_chain_pair<A: NameAtomStore + ?Sized>(
    atoms: &A,
    mut union_find: UnionFind,
    left_atoms: &[u32],
    right_atoms: &[u32],
) -> (GroupSummary, GroupSummary) {
    let mut components: HashMap<usize, PairComponentAccumulator> = HashMap::new();
    for local_index in 0..union_find.parent.len() {
        let root = union_find.find(local_index);
        let (atom_index, is_left) = if local_index < left_atoms.len() {
            (left_atoms[local_index] as usize, true)
        } else {
            (right_atoms[local_index - left_atoms.len()] as usize, false)
        };
        let component = components.entry(root).or_default();
        let contract_count = atoms.contract_count(atom_index);
        component.total_contract_count += contract_count;
        if is_left {
            component.left_contract_count += contract_count;
            component.left_nft_count += atoms.nft_count(atom_index);
        } else {
            component.right_contract_count += contract_count;
            component.right_nft_count += atoms.nft_count(atom_index);
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

#[cfg(target_pointer_width = "64")]
pub(crate) fn summarize_sparse_components_for_chain_pair<A: NameAtomStore + ?Sized>(
    atoms: &A,
    mut union_find: SparseUnionFind,
    left_chain: usize,
    right_chain: usize,
) -> (GroupSummary, GroupSummary) {
    let atom_count = union_find.atom_count();
    for local_index in 0..atom_count {
        union_find.find_local(local_index);
    }
    debug_assert!(atom_count <= u32::MAX as usize);
    let SparseUnionFind {
        index_by_atom,
        atoms: local_atoms,
        parent,
        rank,
    } = union_find;
    drop(index_by_atom);
    drop(rank);

    let mut locals = (0..parent.len() as u32).collect::<Vec<_>>();
    locals.sort_unstable_by_key(|&local| (parent[local as usize], local));

    let mut left_summary = GroupSummary::default();
    let mut right_summary = GroupSummary::default();
    let mut component_start = 0usize;
    while component_start < locals.len() {
        let root = parent[locals[component_start] as usize];
        let mut component_end = component_start;
        let mut component = PairComponentAccumulator::default();
        while component_end < locals.len() && parent[locals[component_end] as usize] == root {
            let local_index = locals[component_end] as usize;
            let atom_index = local_atoms[local_index] as usize;
            let contract_count = atoms.contract_count(atom_index);
            component.total_contract_count += contract_count;
            if atoms.chain_index(atom_index) == left_chain {
                component.left_contract_count += contract_count;
                component.left_nft_count += atoms.nft_count(atom_index);
            } else if atoms.chain_index(atom_index) == right_chain {
                component.right_contract_count += contract_count;
                component.right_nft_count += atoms.nft_count(atom_index);
            }
            component_end += 1;
        }
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
        component_start = component_end;
    }
    (left_summary, right_summary)
}

#[cfg(not(target_pointer_width = "64"))]
pub(crate) fn summarize_sparse_components_for_chain_pair<A: NameAtomStore + ?Sized>(
    atoms: &A,
    mut union_find: SparseUnionFind,
    left_chain: usize,
    right_chain: usize,
) -> (GroupSummary, GroupSummary) {
    summarize_sparse_components_for_chain_pair_reference(
        atoms,
        &mut union_find,
        left_chain,
        right_chain,
    )
}

#[cfg(any(test, not(target_pointer_width = "64")))]
pub(crate) fn summarize_sparse_components_for_chain_pair_reference<A: NameAtomStore + ?Sized>(
    atoms: &A,
    union_find: &mut SparseUnionFind,
    left_chain: usize,
    right_chain: usize,
) -> (GroupSummary, GroupSummary) {
    let mut components: HashMap<usize, PairComponentAccumulator> = HashMap::new();
    for local_index in 0..union_find.atom_count() {
        let index = union_find.atom_at(local_index);
        let root = union_find.find_local(local_index);
        let component = components.entry(root).or_default();
        let contract_count = atoms.contract_count(index);
        component.total_contract_count += contract_count;
        if atoms.chain_index(index) == left_chain {
            component.left_contract_count += contract_count;
            component.left_nft_count += atoms.nft_count(index);
        } else if atoms.chain_index(index) == right_chain {
            component.right_contract_count += contract_count;
            component.right_nft_count += atoms.nft_count(index);
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

pub(crate) fn summarize_sparse_components_by_chain<A: NameAtomStore + ?Sized>(
    atoms: &A,
    union_find: &mut SparseUnionFind,
    chain_count: usize,
    scratch: &mut DenseComponentScratch,
) -> Vec<GroupSummary> {
    let atom_count = union_find.atom_count();
    let mut summaries = vec![GroupSummary::default(); chain_count];
    if atom_count == 0 || chain_count == 0 {
        return summaries;
    }

    debug_assert!(scratch.components.len() >= atom_count);
    debug_assert!(atom_count <= u32::MAX as usize);
    let mut chain_counts = vec![0usize; chain_count];
    let mut root_chain_marker = vec![usize::MAX; atom_count];

    for local_index in 0..union_find.atom_count() {
        let index = union_find.atom_at(local_index);
        let root = union_find.find_local(local_index);
        let component = &mut scratch.components[root];
        component.total_contract_count += atoms.contract_count(index);
        let chain_index = atoms.chain_index(index);
        let first_chain = &mut root_chain_marker[root];
        if *first_chain == usize::MAX {
            *first_chain = chain_index;
        } else if *first_chain != chain_index {
            component.has_secondary = true;
        }
        chain_counts[chain_index] += 1;
    }

    let mut chain_offsets = Vec::with_capacity(chain_count + 1);
    chain_offsets.push(0usize);
    for &count in &chain_counts {
        let next = chain_offsets
            .last()
            .copied()
            .unwrap_or(0)
            .saturating_add(count);
        chain_offsets.push(next);
    }
    let mut write_offsets = chain_offsets[..chain_count].to_vec();
    let mut locals_by_chain = vec![0u32; atom_count];
    for local_index in 0..atom_count {
        let chain_index = atoms.chain_index(union_find.atom_at(local_index));
        let write_index = write_offsets[chain_index];
        locals_by_chain[write_index] = local_index as u32;
        write_offsets[chain_index] += 1;
    }

    root_chain_marker.fill(usize::MAX);
    for chain_index in 0..chain_count {
        for &local_index in
            &locals_by_chain[chain_offsets[chain_index]..chain_offsets[chain_index + 1]]
        {
            let local_index = local_index as usize;
            let atom_index = union_find.atom_at(local_index);
            // The first pass compressed every local parent to its final root.
            let root = union_find.parent[local_index] as usize;
            let component = &mut scratch.components[root];
            if root_chain_marker[root] != chain_index {
                root_chain_marker[root] = chain_index;
                scratch.touched_roots.push(root);
            }
            component.primary_contract_count += atoms.contract_count(atom_index);
            component.primary_nft_count += atoms.nft_count(atom_index);
        }

        let summary = &mut summaries[chain_index];
        for &root in &scratch.touched_roots {
            let component = &scratch.components[root];
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
        scratch.clear_primary_touched();
    }

    for component in scratch.components.iter_mut().take(atom_count) {
        *component = ComponentAccumulator::default();
    }
    summaries
}

pub(crate) fn summarize_dense_cross_components_by_chain<A: NameAtomStore + ?Sized>(
    atoms: &A,
    union_find: &mut UnionFind,
    chain_count: usize,
    scratch: &mut DenseComponentScratch,
) -> Vec<GroupSummary> {
    let atom_count = atoms.len();
    let mut summaries = vec![GroupSummary::default(); chain_count];
    if atom_count == 0 || chain_count == 0 {
        return summaries;
    }

    debug_assert!(scratch.components.len() >= atom_count);
    debug_assert!(atom_count <= u32::MAX as usize);
    let mut chain_counts = vec![0usize; chain_count];
    let mut root_chain_marker = vec![usize::MAX; atom_count];
    for index in 0..atoms.len() {
        let root = union_find.find(index);
        let component = &mut scratch.components[root];
        component.total_contract_count += atoms.contract_count(index);
        let chain_index = atoms.chain_index(index);
        let first_chain = &mut root_chain_marker[root];
        if *first_chain == usize::MAX {
            *first_chain = chain_index;
        } else if *first_chain != chain_index {
            component.has_secondary = true;
        }
        chain_counts[chain_index] += 1;
    }

    let mut chain_offsets = Vec::with_capacity(chain_count + 1);
    chain_offsets.push(0usize);
    for &count in &chain_counts {
        chain_offsets.push(
            chain_offsets
                .last()
                .copied()
                .unwrap_or(0)
                .saturating_add(count),
        );
    }
    let mut write_offsets = chain_offsets[..chain_count].to_vec();
    let mut atoms_grouped_by_chain = vec![0u32; atom_count];
    for index in 0..atoms.len() {
        let chain_index = atoms.chain_index(index);
        let write_index = write_offsets[chain_index];
        atoms_grouped_by_chain[write_index] = index as u32;
        write_offsets[chain_index] += 1;
    }

    root_chain_marker.fill(usize::MAX);
    for chain_index in 0..chain_count {
        for &index in
            &atoms_grouped_by_chain[chain_offsets[chain_index]..chain_offsets[chain_index + 1]]
        {
            let index = index as usize;
            let root = union_find.parent[index] as usize;
            let component = &mut scratch.components[root];
            if root_chain_marker[root] != chain_index {
                root_chain_marker[root] = chain_index;
                scratch.touched_roots.push(root);
            }
            component.primary_contract_count += atoms.contract_count(index);
            component.primary_nft_count += atoms.nft_count(index);
        }

        let summary = &mut summaries[chain_index];
        for &root in &scratch.touched_roots {
            let component = &scratch.components[root];
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
        scratch.clear_primary_touched();
    }

    for component in scratch.components.iter_mut().take(atom_count) {
        *component = ComponentAccumulator::default();
    }
    summaries
}

pub(crate) fn summarize_components_by_chain_low_memory<A: NameAtomStore + ?Sized>(
    atoms: &A,
    atoms_by_chain: &mut [Vec<u32>],
    mut union_find: UnionFind,
) -> Vec<GroupSummary> {
    for index in 0..union_find.parent.len() {
        union_find.find(index);
    }
    let UnionFind { parent, rank } = union_find;
    drop(rank);

    atoms_by_chain
        .iter_mut()
        .map(|primary_atoms| {
            // In-place unstable sorting keeps the fallback's auxiliary heap at O(chain_count).
            primary_atoms.sort_unstable_by_key(|&index| parent[index as usize]);
            let mut summary = GroupSummary::default();
            let mut cursor = 0usize;
            while cursor < primary_atoms.len() {
                let root = parent[primary_atoms[cursor] as usize];
                let mut contract_count = 0i64;
                let mut nft_count = 0i64;
                while cursor < primary_atoms.len() && parent[primary_atoms[cursor] as usize] == root
                {
                    let atom_index = primary_atoms[cursor] as usize;
                    contract_count += atoms.contract_count(atom_index);
                    nft_count += atoms.nft_count(atom_index);
                    cursor += 1;
                }
                if contract_count < 2 {
                    continue;
                }
                summary.group_count += 1;
                summary.duplicate_contract_count += contract_count;
                summary.duplicate_nft_count += nft_count;
                summary.group_size_ge_2_count += 1;
                summary.group_size_gt_2_count += i64::from(contract_count > 2);
            }
            summary
        })
        .collect()
}

#[cfg(target_pointer_width = "64")]
pub(crate) fn summarize_dense_cross_components_by_chain_low_memory<A: NameAtomStore + ?Sized>(
    atoms: &A,
    mut union_find: UnionFind,
    chain_count: usize,
) -> Vec<GroupSummary> {
    for index in 0..union_find.parent.len() {
        union_find.find(index);
    }
    debug_assert!(atoms.len() <= u32::MAX as usize);
    let UnionFind { parent, rank } = union_find;
    drop(rank);

    let mut indexes = (0..parent.len() as u32).collect::<Vec<_>>();
    indexes.sort_unstable_by(|left, right| {
        let left_root = parent[*left as usize];
        let right_root = parent[*right as usize];
        left_root.cmp(&right_root).then_with(|| {
            let left_index = *left as usize;
            let right_index = *right as usize;
            atoms
                .chain_index(left_index)
                .cmp(&atoms.chain_index(right_index))
        })
    });

    let mut summaries = vec![GroupSummary::default(); chain_count];
    let mut component_start = 0usize;
    while component_start < indexes.len() {
        let root = parent[indexes[component_start] as usize];
        let mut component_end = component_start;
        let mut total_contract_count = 0i64;
        let mut distinct_chains = 0usize;
        let mut previous_chain = usize::MAX;
        while component_end < indexes.len() && parent[indexes[component_end] as usize] == root {
            let index = indexes[component_end] as usize;
            total_contract_count += atoms.contract_count(index);
            let chain_index = atoms.chain_index(index);
            if chain_index != previous_chain {
                distinct_chains += 1;
                previous_chain = chain_index;
            }
            component_end += 1;
        }

        if distinct_chains > 1 && total_contract_count >= 2 {
            let mut chain_start = component_start;
            while chain_start < component_end {
                let index = indexes[chain_start] as usize;
                let chain_index = atoms.chain_index(index);
                let mut primary_contract_count = 0i64;
                let mut primary_nft_count = 0i64;
                while chain_start < component_end {
                    let index = indexes[chain_start] as usize;
                    if atoms.chain_index(index) != chain_index {
                        break;
                    }
                    primary_contract_count += atoms.contract_count(index);
                    primary_nft_count += atoms.nft_count(index);
                    chain_start += 1;
                }
                let summary = &mut summaries[chain_index];
                summary.group_count += 1;
                summary.duplicate_contract_count += primary_contract_count;
                summary.duplicate_nft_count += primary_nft_count;
                summary.group_size_ge_2_count += 1;
                summary.group_size_gt_2_count += i64::from(total_contract_count > 2);
            }
        }
        component_start = component_end;
    }
    summaries
}

#[cfg(not(target_pointer_width = "64"))]
pub(crate) fn summarize_dense_cross_components_by_chain_low_memory<A: NameAtomStore + ?Sized>(
    atoms: &A,
    mut union_find: UnionFind,
    chain_count: usize,
) -> Vec<GroupSummary> {
    (0..chain_count)
        .map(|primary| {
            let mut components: HashMap<usize, ComponentAccumulator> = HashMap::new();
            for index in 0..atoms.len() {
                let root = union_find.find(index);
                let component = components.entry(root).or_default();
                let contract_count = atoms.contract_count(index);
                component.total_contract_count += contract_count;
                if atoms.chain_index(index) != primary {
                    component.has_secondary = true;
                } else {
                    component.primary_contract_count += contract_count;
                    component.primary_nft_count += atoms.nft_count(index);
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
                summary.group_size_ge_2_count += 1;
                summary.group_size_gt_2_count += i64::from(component.total_contract_count > 2);
            }
            summary
        })
        .collect()
}

#[cfg(target_pointer_width = "64")]
pub(crate) fn summarize_sparse_components_by_chain_low_memory<A: NameAtomStore + ?Sized>(
    atoms: &A,
    mut union_find: SparseUnionFind,
    chain_count: usize,
) -> Vec<GroupSummary> {
    let atom_count = union_find.atom_count();
    for local_index in 0..atom_count {
        union_find.find_local(local_index);
    }
    debug_assert!(atom_count <= u32::MAX as usize);
    let SparseUnionFind {
        index_by_atom,
        atoms: local_atoms,
        parent,
        rank,
    } = union_find;
    drop(index_by_atom);
    drop(rank);

    let mut locals = (0..parent.len() as u32).collect::<Vec<_>>();
    locals.sort_unstable_by(|left, right| {
        let left_root = parent[*left as usize];
        let right_root = parent[*right as usize];
        left_root.cmp(&right_root).then_with(|| {
            let left_local = *left as usize;
            let right_local = *right as usize;
            atoms
                .chain_index(local_atoms[left_local] as usize)
                .cmp(&atoms.chain_index(local_atoms[right_local] as usize))
        })
    });

    let mut summaries = vec![GroupSummary::default(); chain_count];
    let mut component_start = 0usize;
    while component_start < locals.len() {
        let root = parent[locals[component_start] as usize];
        let mut component_end = component_start;
        let mut total_contract_count = 0i64;
        let mut distinct_chains = 0usize;
        let mut previous_chain = usize::MAX;
        while component_end < locals.len() && parent[locals[component_end] as usize] == root {
            let local_index = locals[component_end] as usize;
            let atom_index = local_atoms[local_index] as usize;
            total_contract_count += atoms.contract_count(atom_index);
            let chain_index = atoms.chain_index(atom_index);
            if chain_index != previous_chain {
                distinct_chains += 1;
                previous_chain = chain_index;
            }
            component_end += 1;
        }

        if distinct_chains > 1 && total_contract_count >= 2 {
            let mut chain_start = component_start;
            while chain_start < component_end {
                let local_index = locals[chain_start] as usize;
                let chain_index = atoms.chain_index(local_atoms[local_index] as usize);
                let mut primary_contract_count = 0i64;
                let mut primary_nft_count = 0i64;
                while chain_start < component_end {
                    let local_index = locals[chain_start] as usize;
                    let atom_index = local_atoms[local_index] as usize;
                    if atoms.chain_index(atom_index) != chain_index {
                        break;
                    }
                    primary_contract_count += atoms.contract_count(atom_index);
                    primary_nft_count += atoms.nft_count(atom_index);
                    chain_start += 1;
                }
                let summary = &mut summaries[chain_index];
                summary.group_count += 1;
                summary.duplicate_contract_count += primary_contract_count;
                summary.duplicate_nft_count += primary_nft_count;
                summary.group_size_ge_2_count += 1;
                summary.group_size_gt_2_count += i64::from(total_contract_count > 2);
            }
        }
        component_start = component_end;
    }
    summaries
}

#[cfg(not(target_pointer_width = "64"))]
pub(crate) fn summarize_sparse_components_by_chain_low_memory<A: NameAtomStore + ?Sized>(
    atoms: &A,
    mut union_find: SparseUnionFind,
    chain_count: usize,
) -> Vec<GroupSummary> {
    (0..chain_count)
        .map(|chain| summarize_sparse_components_for_primary_impl(atoms, &mut union_find, chain))
        .collect()
}

#[cfg(test)]
pub(crate) fn summarize_sparse_components_for_primary<A: NameAtomStore + ?Sized>(
    atoms: &A,
    union_find: &mut SparseUnionFind,
    primary: usize,
) -> GroupSummary {
    summarize_sparse_components_for_primary_impl(atoms, union_find, primary)
}

#[cfg(any(test, not(target_pointer_width = "64")))]
fn summarize_sparse_components_for_primary_impl<A: NameAtomStore + ?Sized>(
    atoms: &A,
    union_find: &mut SparseUnionFind,
    primary: usize,
) -> GroupSummary {
    let mut components: HashMap<usize, ComponentAccumulator> = HashMap::new();
    for local_index in 0..union_find.atom_count() {
        let index = union_find.atom_at(local_index);
        let root = union_find.find_local(local_index);
        let component = components.entry(root).or_default();
        let contract_count = atoms.contract_count(index);
        component.total_contract_count += contract_count;
        if atoms.chain_index(index) != primary {
            component.has_secondary = true;
        } else {
            component.primary_contract_count += contract_count;
            component.primary_nft_count += atoms.nft_count(index);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn atom(chain_index: usize, contract_count: i64, nft_count: i64) -> NameAtom {
        NameAtom {
            chain_index,
            name_norm: std::sync::Arc::from(format!("chain-{chain_index}")),
            char_len: 7,
            contract_count,
            nft_count,
        }
    }

    fn cross_components() -> SparseUnionFind {
        let mut union_find = SparseUnionFind::default();
        union_find.union(0, 1);
        union_find.union(1, 2);
        union_find.union(3, 4);
        union_find.union(4, 5);
        union_find
    }

    #[test]
    fn all_chain_cross_summary_matches_repeated_reference_scans() {
        let atoms = vec![
            atom(0, 2, 20),
            atom(1, 1, 10),
            atom(0, 3, 30),
            atom(1, 4, 40),
            atom(2, 5, 50),
            atom(2, 6, 60),
        ];
        let mut reference_union = cross_components();
        let reference = (0..3)
            .map(|chain| {
                summarize_sparse_components_for_primary(&atoms, &mut reference_union, chain)
            })
            .collect::<Vec<_>>();

        let mut union_find = cross_components();
        let mut scratch = DenseComponentScratch::new(atoms.len());
        let summaries =
            summarize_sparse_components_by_chain(&atoms, &mut union_find, 3, &mut scratch);

        assert_eq!(summaries, reference);
        assert!(scratch.touched_roots.is_empty());
        assert!(scratch.components.iter().all(|component| {
            component.primary_contract_count == 0
                && component.primary_nft_count == 0
                && component.total_contract_count == 0
                && !component.has_secondary
        }));
    }

    #[test]
    fn in_place_chain_pair_summary_matches_hash_map_reference() {
        let atoms = vec![
            atom(0, 2, 20),
            atom(1, 1, 10),
            atom(0, 3, 30),
            atom(1, 4, 40),
            atom(2, 5, 50),
            atom(2, 6, 60),
        ];
        let mut reference_union = cross_components();
        let reference = summarize_sparse_components_for_chain_pair_reference(
            &atoms,
            &mut reference_union,
            0,
            1,
        );
        let in_place = summarize_sparse_components_for_chain_pair(&atoms, cross_components(), 0, 1);

        assert_eq!(in_place, reference);
    }

    #[test]
    fn dense_global_cross_summary_matches_sparse_fast_and_low_memory_paths() {
        let atoms = vec![
            atom(0, 2, 20),
            atom(1, 1, 10),
            atom(0, 3, 30),
            atom(1, 4, 40),
            // Untouched multi-contract singleton must not become a cross group.
            atom(2, 9, 90),
            atom(2, 6, 60),
        ];
        let mut sparse = SparseUnionFind::default();
        sparse.union(0, 1);
        sparse.union(1, 2);
        sparse.union(2, 3);
        let mut sparse_scratch = DenseComponentScratch::new(atoms.len());
        let sparse_summary =
            summarize_sparse_components_by_chain(&atoms, &mut sparse, 3, &mut sparse_scratch);

        let mut dense = UnionFind::new(atoms.len());
        dense.union(0, 1);
        dense.union(1, 2);
        dense.union(2, 3);
        let mut dense_scratch = DenseComponentScratch::new(atoms.len());
        let dense_summary =
            summarize_dense_cross_components_by_chain(&atoms, &mut dense, 3, &mut dense_scratch);

        let mut dense_low = UnionFind::new(atoms.len());
        dense_low.union(0, 1);
        dense_low.union(1, 2);
        dense_low.union(2, 3);
        let dense_low_summary =
            summarize_dense_cross_components_by_chain_low_memory(&atoms, dense_low, 3);

        assert_eq!(dense_summary, sparse_summary);
        assert_eq!(dense_low_summary, sparse_summary);
        assert_eq!(dense_summary[2], GroupSummary::default());
    }
}
