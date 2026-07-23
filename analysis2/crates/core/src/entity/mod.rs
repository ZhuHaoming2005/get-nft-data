//! Resident entity identities, string pool, and CSR indexes.

pub mod csr;
pub mod ids;
pub mod store;
pub mod string_pool;

pub use csr::{CsrIndex, NamePostingStub, UriPostingKey};
pub use ids::{
    compare_token_ids, compare_token_ids_desc, normalized_evm_token, ChainId, ChainTotals,
    Contract, ContractId, MetadataRecord, Nft, NftId, SourceOrder, StringId,
};
pub use store::{finalize_name_representatives_stub, IdentityRow, ResidentStore};
pub use string_pool::StringPool;
