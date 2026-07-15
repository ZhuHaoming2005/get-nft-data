//! Thin CLI adapter around the production `metadata_engine` crate.
//!
//! Metadata matching lives exclusively in `metadata_engine`. This module owns
//! only the DuckDB preparation and encode adapters needed to feed it.

mod encode;
mod prepare;

pub(crate) use encode::run_metadata_encode;
pub(crate) use prepare::prepare_metadata_compact_tables;
pub(super) use prepare::MAX_METADATA_BYTES_FOR_DEDUP;

#[cfg(test)]
mod tests;
