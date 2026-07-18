//! Unified logical entity construction and read-only indexes.

mod entity_builder;
mod external_strings;
mod string_dictionary;

pub use dedup_storage::{
    CandidateBuffer, ExternalRadix, ExternalRadixStats, LshProbeAccumulator, MappedContracts,
    MappedEntityObjects, MappedNfts, MemoryBudget, PairReducerBuffer, RadixRecord, SpillVolume,
};
pub use entity_builder::*;
pub use string_dictionary::*;
