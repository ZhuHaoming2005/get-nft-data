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
pub use pass2::CollectedPass2Anchors;

/// Load-time chain filters and metadata anchor bound.
#[derive(Clone, Debug)]
pub struct LoadOptions {
    pub allowed_chains: AHashSet<String>,
    pub evm_chains: AHashSet<String>,
    pub metadata_anchors: usize,
    /// When false, skip URI/Name/Metadata index build (identity + contract→NFT CSR only).
    /// Used when replaying a dedup cache so load is much cheaper.
    pub build_dedup_indexes: bool,
    /// When false, skip the Name index while retaining URI and Metadata indexes.
    pub build_name_index: bool,
}

impl Default for LoadOptions {
    fn default() -> Self {
        Self {
            allowed_chains: AHashSet::default(),
            evm_chains: AHashSet::default(),
            metadata_anchors: 8,
            build_dedup_indexes: true,
            build_name_index: true,
        }
    }
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
            build_dedup_indexes: true,
            build_name_index: true,
        }
    }

    /// Identity-only load for `--reuse-dedup` (no Name/URI/Metadata indexes).
    pub fn identity_only(
        allowed_chains: impl IntoIterator<Item = String>,
        evm_chains: impl IntoIterator<Item = String>,
        metadata_anchors: usize,
    ) -> Self {
        Self {
            allowed_chains: normalize_chain_set(allowed_chains),
            evm_chains: normalize_chain_set(evm_chains),
            metadata_anchors,
            build_dedup_indexes: false,
            build_name_index: false,
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

/// Remaining work after pass-1 identity + URI CSR are ready for seed URI queries.
///
/// Pass-2 Parquet I/O does not need the store, so the CLI can overlap it with
/// URI seed queries before name/metadata finalize.
pub struct PendingDedupLoad {
    validated: Vec<validate::ValidatedInput>,
    options: LoadOptions,
    total_rows: u64,
}

impl PendingDedupLoad {
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Heavy pass-2 scan (no store mutation). Safe to run beside URI queries.
    pub fn collect_pass2(
        &self,
        progress: &dyn ProgressObserver,
    ) -> Result<pass2::CollectedPass2Anchors, Analysis2Error> {
        progress.begin_phase("pass2_metadata", Some(self.total_rows));
        pass2::collect_pass2_anchors(&self.validated, &self.options, progress)
    }

    /// Apply pass-2 anchors and build Name + Metadata indexes.
    pub fn finish(
        self,
        store: &mut ResidentStore,
        anchors: pass2::CollectedPass2Anchors,
        progress: &dyn ProgressObserver,
    ) -> Result<(), Analysis2Error> {
        progress.begin_phase("apply_pass2_anchors", Some(1));
        pass2::apply_pass2_anchors(store, anchors)?;
        progress.add_completed(1);
        if self.options.build_name_index {
            finalize_name_index_with_progress(store, progress)?;
        }
        finalize_metadata_index_with_progress(store, progress)?;
        Ok(())
    }
}

/// Pass-1 + URI CSR only. Pair with [`PendingDedupLoad`] for overlapped URI/pass2.
pub fn load_resident_store_uri_ready(
    inputs: &[PathBuf],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<(ResidentStore, Option<PendingDedupLoad>), Analysis2Error> {
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

    if !options.build_dedup_indexes {
        progress.begin_phase("build_contract_nft_csr", Some(store.nfts.len() as u64));
        store.rebuild_contract_nft_csr();
        progress.add_completed(store.nfts.len() as u64);
        return Ok((store, None));
    }

    progress.begin_phase("build_uri_csr", Some(store.nfts.len() as u64));
    store.rebuild_uri_csr();
    progress.add_completed(store.nfts.len() as u64);

    Ok((
        store,
        Some(PendingDedupLoad {
            validated,
            options: options.clone(),
            total_rows,
        }),
    ))
}

/// Validate schemas, two-pass project, ordered merge, URI CSR, name/metadata finalize.
pub fn load_resident_store(
    inputs: &[PathBuf],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<ResidentStore, Analysis2Error> {
    let (mut store, pending) = load_resident_store_uri_ready(inputs, options, progress)?;
    let Some(pending) = pending else {
        return Ok(store);
    };
    let anchors = pending.collect_pass2(progress)?;
    pending.finish(&mut store, anchors, progress)?;
    Ok(store)
}
