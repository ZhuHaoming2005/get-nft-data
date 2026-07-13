use std::io::{self, Read};
use std::path::Path;

use sha2::{Digest, Sha256};

pub fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

/// Hash `path` with a reusable buffer of `buffer_bytes`, returning `(size, hex digest)`.
pub fn sha256_file(path: &Path, buffer_bytes: usize) -> io::Result<(u64, String)> {
    if buffer_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "SHA-256 file buffer must be non-zero",
        ));
    }
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; buffer_bytes];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let size = file.metadata()?.len();
    Ok((size, sha256_hex(hasher.finalize().as_ref())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_file_rejects_a_zero_sized_buffer() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("payload.bin");
        std::fs::write(&path, b"non-empty payload").unwrap();

        let error = sha256_file(&path, 0).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
