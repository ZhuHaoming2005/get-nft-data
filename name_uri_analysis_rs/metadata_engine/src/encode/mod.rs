pub mod csr;
pub mod feature_soa;
pub mod parse;
pub mod payload_cas;

pub use feature_soa::{
    write_encode_artifacts, write_encode_artifacts_with_contracts,
    write_encode_artifacts_with_contracts_and_atoms,
    write_encode_artifacts_with_contracts_and_atoms_with_progress, EncodeBundle, EncodeContractRow,
    EncodePayloadRow, EncodePersistStats, EncodeSourceRow, FeatureSoaError, FeatureView,
};
pub use parse::{parse_metadata_documents, ParsedMetadataDocuments, MAX_METADATA_BYTES_FOR_DEDUP};
pub use payload_cas::{
    payload_digest, PayloadCasError, PayloadCasIndex, PayloadCasWriter, PayloadDigest,
    DEFAULT_MAX_PACK_BYTES,
};

pub const ENCODE_SCHEMA_REVISION: u32 = 1;
