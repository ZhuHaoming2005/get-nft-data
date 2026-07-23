//! Dedup hit graph and candidate registry.

pub mod candidates;
pub mod hits;
pub mod metadata;
pub mod name;
pub mod uri;

pub use candidates::{CandidateRegistry, SeedCandidateRelation};
pub use hits::{Dimension, HitEdge, HitGraph, ScopeKind};
pub use metadata::{
    finalize_metadata_index, query_metadata_for_seed, MetadataIndex, DEFAULT_METADATA_THRESHOLD,
};
pub use name::{finalize_name_index, query_name_for_seed, DEFAULT_NAME_THRESHOLD};
pub use uri::query_uri_for_seed;
