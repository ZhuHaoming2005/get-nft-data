use super::*;

fn scored_rights_for_left(
    atoms: &[NameAtom],
    candidate_index: &NameCandidateIndex,
    left: usize,
    right_range: std::ops::Range<usize>,
    threshold: f64,
    scratch: &mut NameCandidateScratch,
) -> Vec<usize> {
    let mut hits = Vec::new();
    visit_indexed_name_pairs_for_left(
        atoms,
        candidate_index,
        left,
        right_range,
        threshold,
        scratch,
        |hit| hits.push(hit.right),
    );
    hits
}

fn output_generation_report(metric: &str, duplicate_contract_count: i64) -> AnalysisReport {
    AnalysisReport {
        summary_rows: vec![SummaryRow {
            field_name: "name".to_string(),
            scope: "intra_chain".to_string(),
            primary_chain: "ethereum".to_string(),
            secondary_chain: String::new(),
            threshold: Some(95.0),
            match_mode: "jaro_winkler".to_string(),
            metric: metric.to_string(),
            total_contracts: 10,
            total_nfts: 100,
            group_count: 1,
            duplicate_contract_count,
            duplicate_nft_count: 20,
            duplicate_contract_ratio: 20.0,
            duplicate_nft_ratio: 20.0,
            group_size_ge_2_count: 1,
            group_size_gt_2_count: 0,
        }],
    }
}

// Deterministic xorshift PRNG so the randomized test is reproducible
// without pulling in the `rand` crate.
fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

mod components;
mod memory;
mod name_scoring;
mod progress;
mod sql_output;
