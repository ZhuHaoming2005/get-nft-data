//! Fixed-size little-endian array header.

use crate::format::ArrayKind;
use crate::format::FormatError;

/// Magic bytes identifying a metadata_engine typed-array file (`MEAR`).
pub const MAGIC: [u8; 4] = *b"MEAR";

/// On-disk header size in bytes.
pub const HEADER_SIZE: usize = 28;

/// Little-endian typed-array header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArrayHeader {
    pub magic: [u8; 4],
    pub schema_revision: u32,
    pub kind: u32,
    pub element_count: u64,
    pub payload_bytes: u64,
}

impl ArrayHeader {
    pub fn new(kind: ArrayKind, element_count: u64, payload_bytes: u64) -> Self {
        Self {
            magic: MAGIC,
            schema_revision: crate::format::FORMAT_SCHEMA_REVISION,
            kind: kind.as_u32(),
            element_count,
            payload_bytes,
        }
    }

    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut out = [0u8; HEADER_SIZE];
        out[0..4].copy_from_slice(&self.magic);
        out[4..8].copy_from_slice(&self.schema_revision.to_le_bytes());
        out[8..12].copy_from_slice(&self.kind.to_le_bytes());
        out[12..20].copy_from_slice(&self.element_count.to_le_bytes());
        out[20..28].copy_from_slice(&self.payload_bytes.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < HEADER_SIZE {
            return Err(FormatError::Truncated {
                expected: HEADER_SIZE,
                actual: bytes.len(),
            });
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        let schema_revision = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let kind = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let element_count = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        let payload_bytes = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
        Ok(Self {
            magic,
            schema_revision,
            kind,
            element_count,
            payload_bytes,
        })
    }

    pub fn validate_for_kind(&self, expected: ArrayKind) -> Result<(), FormatError> {
        if self.magic != MAGIC {
            return Err(FormatError::BadMagic { got: self.magic });
        }
        if self.schema_revision != crate::format::FORMAT_SCHEMA_REVISION {
            return Err(FormatError::UnsupportedRevision {
                got: self.schema_revision,
                expected: crate::format::FORMAT_SCHEMA_REVISION,
            });
        }
        if self.kind != expected.as_u32() {
            return Err(FormatError::KindMismatch {
                got: self.kind,
                expected: expected.as_u32(),
            });
        }
        let expected_payload = self
            .element_count
            .checked_mul(expected.element_size() as u64)
            .ok_or(FormatError::InvalidHeader)?;
        if self.payload_bytes != expected_payload {
            return Err(FormatError::PayloadLengthMismatch {
                header: self.payload_bytes,
                expected: expected_payload,
            });
        }
        Ok(())
    }
}
