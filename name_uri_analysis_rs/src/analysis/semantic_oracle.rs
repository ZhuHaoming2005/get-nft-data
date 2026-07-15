//! Semantic consistency oracle for Prepare/Encode/Match differentials.
//!
//! Internal IDs, feature bytes, forest edge order, and processing order are
//! allowed to differ. Equality is defined only over normalized summary rows
//! and duplicate groups keyed by business identity `(chain, address)`.

#![allow(dead_code)] // exercised by cfg(test) differentials; kept available for Match oracles

use std::cmp::Ordering;

use metadata_engine::pipeline::MetadataSummaryRow;

use super::types::SummaryRow;

/// Business identity for a contract in semantic comparisons.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContractKey {
    pub chain: String,
    pub address: String,
}

impl ContractKey {
    pub fn new(chain: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            chain: chain.into(),
            address: address.into(),
        }
    }
}

/// Sort summary rows into a stable comparison order and return a clone.
pub fn normalize_summary_rows(rows: &[SummaryRow]) -> Vec<SummaryRow> {
    let mut rows = rows.to_vec();
    rows.sort_by(cmp_summary_row);
    rows
}

fn cmp_summary_row(left: &SummaryRow, right: &SummaryRow) -> Ordering {
    left.field_name
        .cmp(&right.field_name)
        .then_with(|| left.scope.cmp(&right.scope))
        .then_with(|| left.primary_chain.cmp(&right.primary_chain))
        .then_with(|| left.secondary_chain.cmp(&right.secondary_chain))
        .then_with(|| left.match_mode.cmp(&right.match_mode))
        .then_with(|| left.metric.cmp(&right.metric))
}

/// Sort metadata Match summary rows into a stable comparison order.
pub fn normalize_metadata_summary_rows(rows: &[MetadataSummaryRow]) -> Vec<MetadataSummaryRow> {
    let mut rows = rows.to_vec();
    rows.sort_by(|left, right| {
        left.scope
            .cmp(&right.scope)
            .then_with(|| left.primary_chain.cmp(&right.primary_chain))
            .then_with(|| left.secondary_chain.cmp(&right.secondary_chain))
    });
    rows
}

/// Normalize duplicate groups: sort members within each group, then sort groups.
pub fn normalize_duplicate_groups(groups: Vec<Vec<ContractKey>>) -> Vec<Vec<ContractKey>> {
    let mut groups: Vec<Vec<ContractKey>> = groups
        .into_iter()
        .map(|mut members| {
            members.sort();
            members.dedup();
            members
        })
        .filter(|members| members.len() >= 2)
        .collect();
    groups.sort();
    groups
}

/// Build connected components from a dense union-find parent/root array.
///
/// `roots[i]` is the component root for contract index `i`. Only components
/// with two or more members are returned (duplicate groups).
pub fn duplicate_groups_from_roots(roots: &[u32], keys: &[ContractKey]) -> Vec<Vec<ContractKey>> {
    assert_eq!(
        roots.len(),
        keys.len(),
        "roots and contract keys must align"
    );
    let mut buckets = std::collections::BTreeMap::<u32, Vec<ContractKey>>::new();
    for (index, &root) in roots.iter().enumerate() {
        buckets.entry(root).or_default().push(keys[index].clone());
    }
    normalize_duplicate_groups(buckets.into_values().collect())
}

/// Compare two summary slices after normalization.
pub fn summaries_semantically_equal(left: &[SummaryRow], right: &[SummaryRow]) -> bool {
    normalize_summary_rows(left) == normalize_summary_rows(right)
}

/// Compare two metadata summary slices after normalization.
pub fn metadata_summaries_semantically_equal(
    left: &[MetadataSummaryRow],
    right: &[MetadataSummaryRow],
) -> bool {
    normalize_metadata_summary_rows(left) == normalize_metadata_summary_rows(right)
}

/// Compare two duplicate-group collections after normalization.
pub fn groups_semantically_equal(
    left: Vec<Vec<ContractKey>>,
    right: Vec<Vec<ContractKey>>,
) -> bool {
    normalize_duplicate_groups(left) == normalize_duplicate_groups(right)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_summary(scope: &str, primary: &str, groups: i64) -> SummaryRow {
        SummaryRow {
            field_name: "metadata".into(),
            scope: scope.into(),
            primary_chain: primary.into(),
            secondary_chain: String::new(),
            threshold: Some(0.9),
            match_mode: "template_recall_hybrid_verify".into(),
            metric: "duplicate_group".into(),
            total_contracts: 10,
            total_nfts: 100,
            group_count: groups,
            duplicate_contract_count: groups * 2,
            duplicate_nft_count: groups * 20,
            duplicate_contract_ratio: 0.4,
            duplicate_nft_ratio: 0.4,
            group_size_ge_2_count: groups,
            group_size_gt_2_count: 0,
        }
    }

    #[test]
    fn summary_normalization_ignores_input_order() {
        let left = vec![
            sample_summary("intra_chain", "base", 1),
            sample_summary("intra_chain", "ethereum", 2),
        ];
        let right = vec![
            sample_summary("intra_chain", "ethereum", 2),
            sample_summary("intra_chain", "base", 1),
        ];
        assert!(summaries_semantically_equal(&left, &right));
    }

    #[test]
    fn summary_normalization_detects_stat_drift() {
        let left = vec![sample_summary("intra_chain", "ethereum", 2)];
        let right = vec![sample_summary("intra_chain", "ethereum", 3)];
        assert!(!summaries_semantically_equal(&left, &right));
    }

    #[test]
    fn group_normalization_ignores_member_and_group_order() {
        let left = vec![
            vec![
                ContractKey::new("ethereum", "0xbbb"),
                ContractKey::new("ethereum", "0xaaa"),
            ],
            vec![
                ContractKey::new("base", "0xccc"),
                ContractKey::new("base", "0xddd"),
            ],
        ];
        let right = vec![
            vec![
                ContractKey::new("base", "0xddd"),
                ContractKey::new("base", "0xccc"),
            ],
            vec![
                ContractKey::new("ethereum", "0xaaa"),
                ContractKey::new("ethereum", "0xbbb"),
            ],
        ];
        assert!(groups_semantically_equal(left, right));
    }

    #[test]
    fn group_normalization_drops_singletons_and_detects_membership_change() {
        let left = vec![
            vec![ContractKey::new("ethereum", "0xaaa")],
            vec![
                ContractKey::new("ethereum", "0xbbb"),
                ContractKey::new("ethereum", "0xccc"),
            ],
        ];
        let right = vec![vec![
            ContractKey::new("ethereum", "0xbbb"),
            ContractKey::new("ethereum", "0xddd"),
        ]];
        assert!(!groups_semantically_equal(left, right));
    }

    #[test]
    fn roots_expand_to_normalized_duplicate_groups() {
        let keys = vec![
            ContractKey::new("ethereum", "0xaaa"),
            ContractKey::new("ethereum", "0xbbb"),
            ContractKey::new("ethereum", "0xccc"),
        ];
        // contracts 0 and 2 share root 0; contract 1 is alone under root 1
        let roots = vec![0, 1, 0];
        let groups = duplicate_groups_from_roots(&roots, &keys);
        assert_eq!(
            groups,
            vec![vec![
                ContractKey::new("ethereum", "0xaaa"),
                ContractKey::new("ethereum", "0xccc"),
            ]]
        );
    }

    #[test]
    fn metadata_summary_helpers_round_trip() {
        let rows = vec![MetadataSummaryRow {
            scope: "intra_chain".into(),
            primary_chain: "ethereum".into(),
            secondary_chain: String::new(),
            total_contracts: 3,
            total_nfts: 3,
            group_count: 1,
            duplicate_contract_count: 2,
            duplicate_nft_count: 2,
            group_size_ge_2_count: 1,
            group_size_gt_2_count: 0,
        }];
        assert!(metadata_summaries_semantically_equal(&rows, &rows));
        assert_eq!(normalize_metadata_summary_rows(&rows), rows);
    }
}
