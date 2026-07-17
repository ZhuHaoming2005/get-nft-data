//! BaseEquivalent blocking constants, band keys, and compile entrypoint.

mod base_equivalent;
mod local;
mod sketch;
mod stats;

pub use base_equivalent::{
    blocking_artifact_upper_bound, blocking_artifact_upper_bound_view, compile_base_equivalent,
    compile_base_equivalent_parallel_with_progress,
    compile_base_equivalent_view_parallel_with_progress, compile_base_equivalent_with_progress,
    scoring_owner, AtomSketch, AtomSketchSoA, AtomSketchView, BlockKind, BlockingBundle,
    BlockingCompileConfig, BlockingError, RoutingStatus, ANCHOR_COUNT, BANDS, BAND_BITS,
    JOINT_BAND_FAMILIES,
};
pub(crate) use local::LocalRoutingPlan;
pub use local::{for_each_local_base_equivalent_pair, for_each_local_base_equivalent_pair_while};
pub use sketch::{
    build_base_equivalent_atom_sketch_soa_from_view_parallel, build_base_equivalent_atom_sketches,
    build_base_equivalent_atom_sketches_from_feature_view_parallel,
    build_base_equivalent_atom_sketches_from_soa_parallel,
    build_base_equivalent_atom_sketches_from_view_parallel,
    build_base_equivalent_atom_sketches_parallel, AtomDimensionAccumulator, AtomDimensionSketch,
    BaseEquivalentAtomInput,
};
pub use stats::{BlockStats, HotBlockPlan, HotBlockPlanSink, HotBlockTile};

/// Blocking artifact schema revision for BaseEquivalent.
pub const BLOCKING_REVISION: u32 = 3;

/// Membership threshold shared by Blocking compilation and Match scheduling.
pub const DEFAULT_MAX_ROUTING_BLOCK_MEMBERS: usize = 1_000_000;

/// SimHash band key matching legacy `metadata_recall_simhash_band_key`.
pub fn simhash_band_key(simhash: u64, band_index: usize) -> u32 {
    let shift = band_index.saturating_mul(BAND_BITS);
    let value = ((simhash >> shift) as u32) & ((1u32 << BAND_BITS) - 1);
    (band_index as u32) << BAND_BITS | value
}

/// Extract the 8-bit band value at `band_index`.
pub fn simhash_band_value(simhash: u64, band_index: usize) -> u8 {
    let shift = band_index.saturating_mul(BAND_BITS);
    ((simhash >> shift) & 0xff) as u8
}
