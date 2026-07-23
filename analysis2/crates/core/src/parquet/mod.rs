//! Two-pass Arrow/Parquet load into ResidentStore.

mod fixture;
mod merge;
mod metadata;
mod pass1;
mod pass2;
mod validate;

use ahash::AHashSet;
use std::path::PathBuf;

use crate::Analysis2Error;
use crate::dedup::metadata::finalize_metadata_index_with_progress;
use crate::dedup::name::finalize_name_index_with_progress;
use crate::entity::ResidentStore;
use crate::progress::ProgressObserver;

pub use fixture::{
    write_report_golden_fixture, write_tiny_multichain_fixture, write_uri_conflict_fixture,
};
pub use metadata::validated_metadata;

/// Load-time chain filters and metadata anchor bound.
#[derive(Clone, Debug, Default)]
pub struct LoadOptions {
    pub allowed_chains: AHashSet<String>,
    pub evm_chains: AHashSet<String>,
    pub metadata_anchors: usize,
}

impl LoadOptions {
    pub fn new(
        allowed_chains: impl IntoIterator<Item = String>,
        evm_chains: impl IntoIterator<Item = String>,
        metadata_anchors: usize,
    ) -> Self {
        Self {
            allowed_chains: normalize_chain_set(allowed_chains),
            evm_chains: normalize_chain_set(evm_chains),
            metadata_anchors,
        }
    }
}

fn normalize_chain_set(chains: impl IntoIterator<Item = String>) -> AHashSet<String> {
    chains
        .into_iter()
        .map(|chain| pass1::normalize_chain(&chain))
        .filter(|chain| !chain.is_empty())
        .collect()
}

/// Validate schemas, two-pass project, ordered merge, URI CSR, name stub finalize.
pub fn load_resident_store(
    inputs: &[PathBuf],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<ResidentStore, Analysis2Error> {
    if inputs.is_empty() {
        return Err(Analysis2Error::invalid("at least one --input is required"));
    }
    progress.set_stage("load");
    progress.begin_phase("validate", Some(inputs.len() as u64));
    let validated = validate::validate_inputs(inputs, progress)?;

    let total_rows: u64 = validated.iter().map(|input| input.row_count).sum();
    progress.begin_phase("pass1_scan", Some(total_rows));
    let mut store = pass1::scan_pass1(&validated, options, progress)?;

    if !options.allowed_chains.is_empty() && store.contracts.is_empty() {
        return Err(Analysis2Error::invalid(
            "none of the requested --chains were present in the inputs",
        ));
    }

    progress.begin_phase("build_uri_csr", Some(store.nfts.len() as u64));
    store.rebuild_uri_csr();
    progress.add_completed(store.nfts.len() as u64);

    progress.begin_phase("pass2_metadata", Some(total_rows));
    pass2::scan_pass2(&validated, &mut store, options, progress)?;

    finalize_name_index_with_progress(&mut store, progress)?;

    finalize_metadata_index_with_progress(&mut store, progress)?;
    Ok(store)
}
