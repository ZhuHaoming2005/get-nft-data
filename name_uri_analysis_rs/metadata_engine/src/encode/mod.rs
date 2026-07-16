pub mod csr;
pub mod feature_soa;
pub mod parse;
pub mod payload_arena;
pub mod payload_cas;

pub use feature_soa::{
    encode_artifact_upper_bound_soa, write_encode_artifacts,
    write_encode_artifacts_soa_with_progress,
    write_encode_artifacts_with_contracts, write_encode_artifacts_with_contracts_and_atoms,
    write_encode_artifacts_with_contracts_and_atoms_with_progress, EncodeBundle, EncodeContractRow,
    EncodeContractSoA, EncodePayloadRow, EncodePersistStats, EncodeSourceRow, EncodeSourceSoA,
    FallbackAtomCsr, FeatureSoaError, FeatureView, PayloadTermListBatch, PayloadTermLists,
    PayloadTermSoA,
};
pub use parse::{
    metadata_has_prefilter_tokens, parse_metadata_documents, ParsedMetadataDocuments,
    MAX_METADATA_BYTES_FOR_DEDUP,
};
pub use payload_arena::{
    FrozenShardedPayloadArena, PayloadArena, PayloadArenaError, PayloadInsert, PayloadInsertRef,
    PayloadRef, ShardedPayloadArena, DEFAULT_ARENA_CHUNK_BYTES, DEFAULT_PAYLOAD_SHARD_COUNT,
};
pub use payload_cas::{
    payload_digest, PayloadCasError, PayloadCasIndex, PayloadCasWriter, PayloadDigest,
    DEFAULT_MAX_PACK_BYTES,
};

pub const ENCODE_SCHEMA_REVISION: u32 = 3;
