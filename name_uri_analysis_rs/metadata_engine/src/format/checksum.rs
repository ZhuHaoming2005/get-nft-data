//! SHA-256 checksum helpers for typed-array files.

use sha2::{Digest, Sha256};

use crate::format::FormatError;

/// SHA-256 digest length in bytes.
pub const CHECKSUM_SIZE: usize = 32;

/// Compute SHA-256 over `header || payload`.
pub fn checksum_header_payload(header: &[u8], payload: &[u8]) -> [u8; CHECKSUM_SIZE] {
    let mut hasher = Sha256::new();
    hasher.update(header);
    hasher.update(payload);
    let digest = hasher.finalize();
    let mut out = [0u8; CHECKSUM_SIZE];
    out.copy_from_slice(&digest);
    out
}

/// Verify footer checksum matches `header || payload`.
pub fn verify_checksum(header: &[u8], payload: &[u8], footer: &[u8]) -> Result<(), FormatError> {
    if footer.len() != CHECKSUM_SIZE {
        return Err(FormatError::Truncated {
            expected: CHECKSUM_SIZE,
            actual: footer.len(),
        });
    }
    let expected = checksum_header_payload(header, payload);
    if footer != expected.as_slice() {
        return Err(FormatError::ChecksumMismatch);
    }
    Ok(())
}
