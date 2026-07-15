use super::*;

#[test]
fn dense_summary_scratch_can_be_reused() {
    let atoms = vec![
        NameAtom {
            chain_index: 0,
            name_norm: "azuki".into(),
            char_len: 5,
            contract_count: 1,
            nft_count: 2,
        },
        NameAtom {
            chain_index: 0,
            name_norm: "azukis".into(),
            char_len: 6,
            contract_count: 1,
            nft_count: 3,
        },
    ];
    let primary_atoms = vec![0, 1];
    let mut union_find = UnionFind::new(atoms.len());
    union_find.union(0, 1);
    let mut scratch = DenseComponentScratch::new(atoms.len());

    let first = summarize_components_for_primary_with_scratch(
        &atoms,
        &primary_atoms,
        &mut union_find,
        &mut scratch,
    );
    let second = summarize_components_for_primary_with_scratch(
        &atoms,
        &primary_atoms,
        &mut union_find,
        &mut scratch,
    );

    assert_eq!(first.duplicate_contract_count, 2);
    assert_eq!(first.duplicate_nft_count, 5);
    assert_eq!(first, second);
}

#[test]
fn sparse_union_find_reports_only_existing_connections() {
    let mut union_find = SparseUnionFind::default();

    assert!(!union_find.connected(1, 2));
    assert_eq!(union_find.atom_count(), 0);
    union_find.union(1, 2);
    assert!(union_find.connected(1, 2));
    assert!(!union_find.connected(1, 3));
    assert_eq!(union_find.atom_count(), 2);
}

#[test]
fn chain_pair_indexes_round_trip() {
    let chain_count = 5;
    let mut seen = Vec::new();

    for left in 0..chain_count {
        for right in left + 1..chain_count {
            let index = chain_pair_index(left, right, chain_count);
            seen.push(index);
            assert_eq!(chain_pair_from_index(index, chain_count), (left, right));
        }
    }

    seen.sort_unstable();
    assert_eq!(seen, (0..chain_pair_count(chain_count)).collect::<Vec<_>>());
}
