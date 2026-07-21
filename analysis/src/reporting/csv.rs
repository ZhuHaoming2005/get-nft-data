use crate::error::Result;
use crate::reporting::aggregate::{AggregateSnapshot, ScopedAggregateMetric};
use crate::reporting::contract_writer::atomic_write_deferred;
use crate::reporting::scope::{DedupScopeReport, ScopeMetric};
use serde::Serialize;
use std::path::Path;

pub fn write_scope_csvs(run_dir: &Path, report: &DedupScopeReport) -> Result<()> {
    write_csv(
        &run_dir.join("chain_matrix.csv"),
        &scope_rows(report, "chain_matrix"),
    )?;
    write_csv(
        &run_dir.join("intra_chain.csv"),
        &scope_rows(report, "intra_chain"),
    )?;
    write_csv(
        &run_dir.join("cross_chain_summary.csv"),
        &scope_rows(report, "cross_chain_summary"),
    )?;
    write_csv(
        &run_dir.join("all_chains_dedup.csv"),
        &scope_rows(report, "all_chains"),
    )
}

/// Flat analysis metrics at the same three scope levels as dedup CSVs.
/// Full nested behaviors/quality live in `analysis_scopes.json`.
pub fn write_analysis_scope_csvs(run_dir: &Path, snapshot: &AggregateSnapshot) -> Result<()> {
    write_csv(
        &run_dir.join("chain_matrix_analysis.csv"),
        &analysis_csv_rows(snapshot, "chain_matrix"),
    )?;
    write_csv(
        &run_dir.join("intra_chain_analysis.csv"),
        &analysis_csv_rows(snapshot, "intra_chain"),
    )?;
    write_csv(
        &run_dir.join("cross_chain_summary_analysis.csv"),
        &analysis_csv_rows(snapshot, "cross_chain_summary"),
    )?;
    write_csv(
        &run_dir.join("all_chains_analysis.csv"),
        &analysis_csv_rows(snapshot, "all_chains"),
    )
}

#[derive(Serialize)]
struct AnalysisScopeCsvRow {
    scope: &'static str,
    primary_chain: Option<String>,
    candidate_chain: Option<String>,
    representative_candidate_count: u64,
    candidate_contract_count: u64,
    suspected_duplicate_contract_count: u64,
    legit_duplicate_contract_count: u64,
    infringing_nft_count: u64,
    behavior_analyzed_suspected_contract_count: u64,
    malicious_address_count: u64,
    honest_address_count: u64,
    total_classified_address_count: u64,
    repeat_infringing_malicious_address_count: u64,
    analyzed_seed_count: Option<u64>,
    persisted_candidate_count: Option<u64>,
    observed_unique_nft_count: Option<u64>,
    observed_unique_transaction_count: Option<u64>,
    behavior_total_contract_count: u64,
    behavior_total_instance_count: u64,
    behavior_linked_loss_native: i128,
    behavior_linked_loss_usd_micros: i128,
    honest_loss_native: i128,
    honest_loss_usd_micros: i128,
    operator_output_usd_micros: i128,
    attacker_gas_usd_micros: i128,
    stuck_nft_count: u64,
    failure_records: u64,
}

fn scope_rows<'a>(report: &'a DedupScopeReport, scope: &str) -> Vec<&'a ScopeMetric> {
    report
        .metrics
        .iter()
        .filter(|row| row.scope == scope)
        .collect()
}

fn analysis_csv_rows(snapshot: &AggregateSnapshot, scope: &str) -> Vec<AnalysisScopeCsvRow> {
    snapshot
        .scopes
        .iter()
        .filter(|row| row.scope == scope)
        .map(flatten_analysis_scope)
        .collect()
}

fn flatten_analysis_scope(row: &ScopedAggregateMetric) -> AnalysisScopeCsvRow {
    let total = row.behaviors.get("total");
    AnalysisScopeCsvRow {
        scope: row.scope,
        primary_chain: row.primary_chain.map(|chain| chain.as_str().to_owned()),
        candidate_chain: row.candidate_chain.map(|chain| chain.as_str().to_owned()),
        representative_candidate_count: row.representative_candidate_count,
        candidate_contract_count: row.candidate_contract_count,
        suspected_duplicate_contract_count: row.suspected_duplicate_contract_count,
        legit_duplicate_contract_count: row.legit_duplicate_contract_count,
        infringing_nft_count: row.infringing_nft_count,
        behavior_analyzed_suspected_contract_count: row.behavior_analyzed_suspected_contract_count,
        malicious_address_count: row.malicious_address_count,
        honest_address_count: row.honest_address_count,
        total_classified_address_count: row.total_classified_address_count,
        repeat_infringing_malicious_address_count: row.repeat_infringing_malicious_address_count,
        analyzed_seed_count: row.analyzed_seed_count,
        persisted_candidate_count: row.persisted_candidate_count,
        observed_unique_nft_count: row.observed_unique_nft_count,
        observed_unique_transaction_count: row.observed_unique_transaction_count,
        behavior_total_contract_count: total.map_or(0, |metric| metric.contract_count),
        behavior_total_instance_count: total.map_or(0, |metric| metric.instance_count),
        behavior_linked_loss_native: total.map_or(0, |metric| metric.linked_loss_native),
        behavior_linked_loss_usd_micros: total.map_or(0, |metric| metric.linked_loss_usd_micros),
        honest_loss_native: row.economics.honest_loss_native,
        honest_loss_usd_micros: row.economics.honest_loss_usd_micros,
        operator_output_usd_micros: row.economics.operator_output_usd_micros,
        attacker_gas_usd_micros: row.economics_derived.attacker_gas_usd_micros,
        stuck_nft_count: row.economics.stuck_nft_count,
        failure_records: row.data_quality.failure_records,
    }
}

fn write_csv<T: Serialize>(path: &Path, values: &[T]) -> Result<()> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    for value in values {
        writer.serialize(value)?;
    }
    let bytes = writer.into_inner().map_err(|error| error.into_error())?;
    atomic_write_deferred(path, &bytes)
}
