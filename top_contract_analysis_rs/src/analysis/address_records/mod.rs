use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::models::{
    normalize_chain_identity, AddressAttributionPayload, AddressEvidencePayload,
    DuplicateCandidate, HonestAddressPayload, InfringingTokenRecord, MaliciousAddressPayload,
    NftSaleRecord, OwnerBalance, SecondarySaleVictimAddressPayload, TransferRecord,
    ValueFlowEdgePayload, VictimAcquisitionAddressPayload, ZERO_ADDRESS,
};

mod activity;
mod attribution;
mod honest;
mod infringing;
mod malicious;
mod victims;

use activity::*;

pub(crate) use activity::{prepare_contract_activity, PreparedContractActivity};
pub use attribution::{
    add_acquisition_exposure_attribution_evidence, build_address_attribution_records,
};
pub(crate) use honest::build_honest_address_records_from_activity;
pub use honest::{build_honest_address_records, HonestAddressRecordInput};
pub use infringing::{
    build_infringing_token_records, build_infringing_token_records_with_context,
    build_infringing_token_records_with_context_refs,
};
pub use malicious::build_malicious_address_records;
pub(crate) use malicious::build_malicious_address_records_from_activity;
pub use victims::build_secondary_sale_victim_address_records;
pub(crate) use victims::build_secondary_sale_victim_address_records_excluding_malicious_from_activity;
#[cfg(test)]
pub(crate) use victims::build_secondary_sale_victim_address_records_from_activity;

#[cfg(test)]
mod tests;
