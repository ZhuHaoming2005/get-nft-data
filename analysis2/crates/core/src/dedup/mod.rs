//! Dedup hit graph and candidate registry.

pub mod candidates;
pub mod hits;
pub mod metadata;
pub mod name;
pub mod uri;

pub use candidates::{CandidateRegistry, SeedCandidateRelation};
pub use hits::{Dimension, HitEdge, HitGraph, ScopeKind};
pub use metadata::{
    DEFAULT_METADATA_THRESHOLD, MetadataIndex, MetadataQueryScratch, finalize_metadata_index,
    query_metadata_for_seed, query_metadata_for_seed_with_scratch,
};
pub use name::{
    DEFAULT_NAME_THRESHOLD, NameQueryScratch, finalize_name_index, query_name_for_seed,
    query_name_for_seed_with_scratch,
};
pub use uri::{UriQueryScratch, query_uri_for_seed, query_uri_for_seed_with_scratch};
