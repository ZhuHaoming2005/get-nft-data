//! Checked identity-space boundaries shared by Encode, Blocking and Match.

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{resource} cardinality {count} exceeds u32 identity space")]
pub struct IdentityOverflow {
    pub resource: &'static str,
    pub count: u64,
}

pub fn checked_u32_identity(resource: &'static str, count: u64) -> Result<u32, IdentityOverflow> {
    u32::try_from(count).map_err(|_| IdentityOverflow { resource, count })
}
