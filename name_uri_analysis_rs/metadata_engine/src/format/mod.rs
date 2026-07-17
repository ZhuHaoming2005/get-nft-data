//! Versioned little-endian typed-array format with SHA-256 footers.

pub(crate) mod atomic;
mod checksum;
mod header;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub use atomic::{commit_ready, commit_ready_serialized};
pub use header::ArrayHeader;

/// Schema revision for on-disk typed-array files.
pub const FORMAT_SCHEMA_REVISION: u32 = 1;
pub const TYPED_ARRAY_CHECKSUM_PREFIX: &str = "typed-array-v1:";

/// Element kind stored in a typed-array file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ArrayKind {
    U32 = 1,
    U64 = 2,
    F64 = 3,
    U8 = 4,
}

impl ArrayKind {
    pub(crate) fn as_u32(self) -> u32 {
        self as u32
    }

    pub(crate) fn element_size(self) -> usize {
        match self {
            ArrayKind::U8 => 1,
            ArrayKind::U32 => 4,
            ArrayKind::U64 | ArrayKind::F64 => 8,
        }
    }

    fn from_u32(value: u32) -> Result<Self, FormatError> {
        match value {
            1 => Ok(Self::U32),
            2 => Ok(Self::U64),
            3 => Ok(Self::F64),
            4 => Ok(Self::U8),
            _ => Err(FormatError::InvalidHeader),
        }
    }
}

/// Errors from typed-array IO, checksum verification, and ready commits.
#[derive(Debug, Error)]
pub enum FormatError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("truncated file: expected at least {expected} bytes, got {actual}")]
    Truncated { expected: usize, actual: usize },
    #[error("bad magic: got {got:?}")]
    BadMagic { got: [u8; 4] },
    #[error("unsupported schema revision {got} (expected {expected})")]
    UnsupportedRevision { got: u32, expected: u32 },
    #[error("array kind mismatch: got {got}, expected {expected}")]
    KindMismatch { got: u32, expected: u32 },
    #[error("payload length mismatch: header {header}, expected {expected}")]
    PayloadLengthMismatch { header: u64, expected: u64 },
    #[error("invalid array header")]
    InvalidHeader,
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("array kind not supported for this API: {0:?}")]
    UnsupportedKind(ArrayKind),
    #[error("array iterator length mismatch: expected {expected} elements, got {actual}")]
    ElementCountMismatch { expected: u64, actual: u64 },
    #[error("array payload length overflow")]
    PayloadLengthOverflow,
    #[error("invalid ready manifest JSON: {0}")]
    InvalidManifest(String),
    #[error("mmap alignment error")]
    Misaligned,
}

/// Memory-mapped little-endian `u32` array (checksum verified before cast).
pub struct MappedU32Array {
    mmap: Mmap,
    payload_offset: usize,
    len: usize,
}

/// Memory-mapped checksummed `u8` array.
pub struct MappedU8Array {
    mmap: Mmap,
    payload_offset: usize,
    len: usize,
}

impl Deref for MappedU8Array {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.mmap[self.payload_offset..self.payload_offset + self.len]
    }
}

impl Deref for MappedU32Array {
    type Target = [u32];

    fn deref(&self) -> &[u32] {
        let byte_len = self
            .len
            .checked_mul(4)
            .expect("payload length already validated");
        let bytes = &self.mmap[self.payload_offset..self.payload_offset + byte_len];
        // SAFETY: map_u32_array verified magic, revision, kind, length, alignment,
        // and SHA-256 over header+payload before constructing this mapping.
        unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<u32>(), self.len) }
    }
}

/// Memory-mapped little-endian `u64` array (checksum verified before cast).
pub struct MappedU64Array {
    mmap: Mmap,
    payload_offset: usize,
    len: usize,
}

impl Deref for MappedU64Array {
    type Target = [u64];

    fn deref(&self) -> &[u64] {
        let byte_len = self
            .len
            .checked_mul(8)
            .expect("payload length already validated");
        let bytes = &self.mmap[self.payload_offset..self.payload_offset + byte_len];
        // SAFETY: map_u64_array verified magic, revision, kind, length, alignment,
        // and SHA-256 over header+payload before constructing this mapping.
        unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<u64>(), self.len) }
    }
}

/// Memory-mapped little-endian `f64` array (checksum verified before cast).
pub struct MappedF64Array {
    mmap: Mmap,
    payload_offset: usize,
    len: usize,
}

macro_rules! impl_verified_checksum {
    ($type:ty) => {
        impl $type {
            /// SHA-256 footer already verified while the typed array was mapped.
            /// Consumers can compose artifact identities from this digest
            /// without scanning the payload a second time.
            pub fn verified_checksum(&self) -> &[u8] {
                &self.mmap[self.mmap.len() - checksum::CHECKSUM_SIZE..]
            }
        }
    };
}

impl_verified_checksum!(MappedU8Array);
impl_verified_checksum!(MappedU32Array);
impl_verified_checksum!(MappedU64Array);
impl_verified_checksum!(MappedF64Array);

impl Deref for MappedF64Array {
    type Target = [f64];

    fn deref(&self) -> &[f64] {
        let byte_len = self
            .len
            .checked_mul(8)
            .expect("payload length already validated");
        let bytes = &self.mmap[self.payload_offset..self.payload_offset + byte_len];
        // SAFETY: map_f64_array verified magic, revision, kind, length, alignment,
        // and SHA-256 over header+payload before constructing this mapping.
        unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<f64>(), self.len) }
    }
}

/// File offset where typed-array payload begins (header + alignment padding).
///
/// Padding ensures an 8-byte-aligned payload start for safe `u64` mmap casts.
fn payload_prefix_len() -> usize {
    let pad = (8 - (header::HEADER_SIZE % 8)) % 8;
    header::HEADER_SIZE + pad
}

fn checksum_hex(checksum: &[u8]) -> String {
    let mut output = String::with_capacity(checksum.len() * 2);
    for byte in checksum {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn validate_dynamic_header(header: ArrayHeader, file_size: u64) -> Result<(), FormatError> {
    let kind = ArrayKind::from_u32(header.kind)?;
    header.validate_for_kind(kind)?;
    let expected = (payload_prefix_len() as u64)
        .checked_add(header.payload_bytes)
        .and_then(|size| size.checked_add(checksum::CHECKSUM_SIZE as u64))
        .ok_or(FormatError::InvalidHeader)?;
    if file_size != expected {
        return Err(FormatError::Truncated {
            expected: usize::try_from(expected).unwrap_or(usize::MAX),
            actual: usize::try_from(file_size).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}

/// Read and structurally validate a typed-array footer without scanning its payload.
/// Returns `None` when the file is not a typed array.
pub fn typed_array_footer_fingerprint(path: &Path) -> Result<Option<(u64, String)>, FormatError> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    if size < header::HEADER_SIZE as u64 {
        return Ok(None);
    }
    let mut header_bytes = [0u8; header::HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    if header_bytes[..4] != header::MAGIC {
        return Ok(None);
    }
    let header = ArrayHeader::decode(&header_bytes)?;
    validate_dynamic_header(header, size)?;
    file.seek(SeekFrom::End(-(checksum::CHECKSUM_SIZE as i64)))?;
    let mut footer = [0u8; checksum::CHECKSUM_SIZE];
    file.read_exact(&mut footer)?;
    Ok(Some((size, checksum_hex(&footer))))
}

/// Read and structurally validate a typed-array header without scanning or
/// mapping its payload. This is used by memory planning before checksum
/// verification touches the full file.
pub fn typed_array_element_count(
    path: &Path,
    expected_kind: ArrayKind,
) -> Result<u64, FormatError> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let mut header_bytes = [0u8; header::HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let header = ArrayHeader::decode(&header_bytes)?;
    header.validate_for_kind(expected_kind)?;
    validate_dynamic_header(header, size)?;
    Ok(header.element_count)
}

/// Fully verify a typed-array payload and return its footer identity.
pub fn verify_typed_array_fingerprint(path: &Path) -> Result<(u64, String), FormatError> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let size = mmap.len() as u64;
    if mmap.len() < header::HEADER_SIZE {
        return Err(FormatError::Truncated {
            expected: header::HEADER_SIZE,
            actual: mmap.len(),
        });
    }
    let header_bytes = &mmap[..header::HEADER_SIZE];
    let header = ArrayHeader::decode(header_bytes)?;
    validate_dynamic_header(header, size)?;
    let payload_offset = payload_prefix_len();
    let payload_end = payload_offset
        .checked_add(header.payload_bytes as usize)
        .ok_or(FormatError::InvalidHeader)?;
    let payload = &mmap[payload_offset..payload_end];
    let footer = &mmap[payload_end..];
    checksum::verify_checksum(header_bytes, payload, footer)?;
    Ok((size, checksum_hex(footer)))
}

fn write_typed_array_file(
    path: &Path,
    header_bytes: &[u8],
    write_payload: impl FnOnce(&mut File, &mut Sha256) -> Result<(), FormatError>,
) -> Result<(), FormatError> {
    let prefix = payload_prefix_len();
    atomic::write_atomic_file(path, |file| {
        file.write_all(header_bytes)?;
        const ZERO_PADDING: [u8; 8] = [0; 8];
        file.write_all(&ZERO_PADDING[..prefix - header_bytes.len()])?;

        let mut hasher = Sha256::new();
        hasher.update(header_bytes);
        write_payload(file, &mut hasher)?;
        file.write_all(&hasher.finalize())?;
        Ok(())
    })
}

const ENCODE_BUFFER_BYTES: usize = 64 * 1024;

/// Incremental, exact-length typed-array writer for fallible external streams.
/// The durable destination is replaced only by [`Self::finish`] after the
/// declared element count, checksum, and file sync all succeed.
pub struct TypedArraySink {
    final_path: PathBuf,
    partial_path: PathBuf,
    file: Option<File>,
    hasher: Sha256,
    kind: ArrayKind,
    expected: u64,
    written: u64,
    buffer: Vec<u8>,
}

impl TypedArraySink {
    pub fn create(path: &Path, kind: ArrayKind, element_count: u64) -> Result<Self, FormatError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let header = iterator_header(kind, element_count)?;
        let header_bytes = header.encode();
        let partial_path = atomic::partial_path(path);
        let mut file = File::create(&partial_path)?;
        file.write_all(&header_bytes)?;
        const ZERO_PADDING: [u8; 8] = [0; 8];
        file.write_all(&ZERO_PADDING[..payload_prefix_len() - header_bytes.len()])?;
        let mut hasher = Sha256::new();
        hasher.update(header_bytes);
        Ok(Self {
            final_path: path.to_path_buf(),
            partial_path,
            file: Some(file),
            hasher,
            kind,
            expected: element_count,
            written: 0,
            buffer: Vec::with_capacity(ENCODE_BUFFER_BYTES),
        })
    }

    pub fn push_u32(&mut self, value: u32) -> Result<(), FormatError> {
        if self.kind != ArrayKind::U32 {
            return Err(FormatError::UnsupportedKind(self.kind));
        }
        self.push_bytes(&value.to_le_bytes())
    }

    pub fn push_u8(&mut self, value: u8) -> Result<(), FormatError> {
        if self.kind != ArrayKind::U8 {
            return Err(FormatError::UnsupportedKind(self.kind));
        }
        self.push_bytes(&[value])
    }

    pub fn push_u64(&mut self, value: u64) -> Result<(), FormatError> {
        if self.kind != ArrayKind::U64 {
            return Err(FormatError::UnsupportedKind(self.kind));
        }
        self.push_bytes(&value.to_le_bytes())
    }

    pub const fn elements_written(&self) -> u64 {
        self.written
    }

    fn push_bytes(&mut self, bytes: &[u8]) -> Result<(), FormatError> {
        if self.written == self.expected {
            return Err(FormatError::ElementCountMismatch {
                expected: self.expected,
                actual: self.written.saturating_add(1),
            });
        }
        if self.buffer.len() + bytes.len() > ENCODE_BUFFER_BYTES {
            self.flush_buffer()?;
        }
        self.buffer.extend_from_slice(bytes);
        self.written += 1;
        Ok(())
    }

    fn flush_buffer(&mut self) -> Result<(), FormatError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.file
            .as_mut()
            .expect("typed-array sink file is open")
            .write_all(&self.buffer)?;
        self.hasher.update(&self.buffer);
        self.buffer.clear();
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), FormatError> {
        if self.written != self.expected {
            return Err(FormatError::ElementCountMismatch {
                expected: self.expected,
                actual: self.written,
            });
        }
        self.flush_buffer()?;
        let mut file = self.file.take().expect("typed-array sink file is open");
        file.write_all(&self.hasher.finalize())?;
        file.sync_all()?;
        drop(file);
        atomic::replace_file_atomically(&self.partial_path, &self.final_path)
    }
}

fn write_little_endian_iter<T, I, const WIDTH: usize>(
    file: &mut File,
    hasher: &mut Sha256,
    values: I,
    expected: u64,
    encode: impl Fn(T) -> [u8; WIDTH],
    mut on_payload_bytes: impl FnMut(u64),
) -> Result<(), FormatError>
where
    I: IntoIterator<Item = T>,
{
    debug_assert!(WIDTH > 0 && ENCODE_BUFFER_BYTES.is_multiple_of(WIDTH));
    let mut buffer = [0u8; ENCODE_BUFFER_BYTES];
    let mut buffered = 0usize;
    let mut actual = 0u64;
    let mut last_reported = None;
    for value in values {
        if actual == expected {
            return Err(FormatError::ElementCountMismatch {
                expected,
                actual: actual.saturating_add(1),
            });
        }
        let bytes = encode(value);
        buffer[buffered..buffered + WIDTH].copy_from_slice(&bytes);
        buffered += WIDTH;
        actual += 1;
        if buffered == buffer.len() {
            file.write_all(&buffer)?;
            hasher.update(buffer.as_slice());
            buffered = 0;
            let bytes = actual.saturating_mul(WIDTH as u64);
            on_payload_bytes(bytes);
            last_reported = Some(bytes);
        }
    }
    if actual != expected {
        return Err(FormatError::ElementCountMismatch { expected, actual });
    }
    if buffered != 0 {
        file.write_all(&buffer[..buffered])?;
        hasher.update(&buffer[..buffered]);
    }
    let payload_bytes = actual.saturating_mul(WIDTH as u64);
    if last_reported != Some(payload_bytes) {
        on_payload_bytes(payload_bytes);
    }
    Ok(())
}

fn iterator_header(kind: ArrayKind, element_count: u64) -> Result<ArrayHeader, FormatError> {
    let payload_bytes = element_count
        .checked_mul(kind.element_size() as u64)
        .ok_or(FormatError::PayloadLengthOverflow)?;
    Ok(ArrayHeader::new(kind, element_count, payload_bytes))
}

/// Write a little-endian `u32` array with header and SHA-256 footer.
pub fn write_u32_array(path: &Path, kind: ArrayKind, values: &[u32]) -> Result<(), FormatError> {
    write_u32_iter(path, kind, values.len() as u64, values.iter().copied())
}

/// Stream a known-length little-endian `u32` iterator without materializing a column-sized buffer.
pub fn write_u32_iter(
    path: &Path,
    kind: ArrayKind,
    element_count: u64,
    values: impl IntoIterator<Item = u32>,
) -> Result<(), FormatError> {
    write_u32_iter_with_progress(path, kind, element_count, values, |_| {})
}

pub fn write_u32_iter_with_progress(
    path: &Path,
    kind: ArrayKind,
    element_count: u64,
    values: impl IntoIterator<Item = u32>,
    on_payload_bytes: impl FnMut(u64),
) -> Result<(), FormatError> {
    if kind != ArrayKind::U32 {
        return Err(FormatError::UnsupportedKind(kind));
    }

    let header = iterator_header(kind, element_count)?;
    let header_bytes = header.encode();

    write_typed_array_file(path, &header_bytes, |file, hasher| {
        write_little_endian_iter(
            file,
            hasher,
            values,
            element_count,
            |value| value.to_le_bytes(),
            on_payload_bytes,
        )
    })
}

pub fn write_u8_array(path: &Path, values: &[u8]) -> Result<(), FormatError> {
    write_u8_iter(path, values.len() as u64, values.iter().copied())
}

/// Stream a known-length `u8` iterator without materializing a column-sized buffer.
pub fn write_u8_iter(
    path: &Path,
    element_count: u64,
    values: impl IntoIterator<Item = u8>,
) -> Result<(), FormatError> {
    write_u8_iter_with_progress(path, element_count, values, |_| {})
}

pub fn write_u8_iter_with_progress(
    path: &Path,
    element_count: u64,
    values: impl IntoIterator<Item = u8>,
    on_payload_bytes: impl FnMut(u64),
) -> Result<(), FormatError> {
    let header = iterator_header(ArrayKind::U8, element_count)?;
    let header_bytes = header.encode();
    write_typed_array_file(path, &header_bytes, |file, hasher| {
        write_little_endian_iter(
            file,
            hasher,
            values,
            element_count,
            |value| [value],
            on_payload_bytes,
        )
    })
}

/// Write a little-endian `u64` array with header and SHA-256 footer.
pub fn write_u64_array(path: &Path, kind: ArrayKind, values: &[u64]) -> Result<(), FormatError> {
    write_u64_iter(path, kind, values.len() as u64, values.iter().copied())
}

/// Stream a known-length little-endian `u64` iterator without materializing a column-sized buffer.
pub fn write_u64_iter(
    path: &Path,
    kind: ArrayKind,
    element_count: u64,
    values: impl IntoIterator<Item = u64>,
) -> Result<(), FormatError> {
    write_u64_iter_with_progress(path, kind, element_count, values, |_| {})
}

pub fn write_u64_iter_with_progress(
    path: &Path,
    kind: ArrayKind,
    element_count: u64,
    values: impl IntoIterator<Item = u64>,
    on_payload_bytes: impl FnMut(u64),
) -> Result<(), FormatError> {
    if kind != ArrayKind::U64 {
        return Err(FormatError::UnsupportedKind(kind));
    }

    let header = iterator_header(kind, element_count)?;
    let header_bytes = header.encode();

    write_typed_array_file(path, &header_bytes, |file, hasher| {
        write_little_endian_iter(
            file,
            hasher,
            values,
            element_count,
            |value| value.to_le_bytes(),
            on_payload_bytes,
        )
    })
}

/// Write a little-endian `f64` array with header and SHA-256 footer.
pub fn write_f64_array(path: &Path, kind: ArrayKind, values: &[f64]) -> Result<(), FormatError> {
    write_f64_iter(path, kind, values.len() as u64, values.iter().copied())
}

/// Stream a known-length little-endian `f64` iterator without materializing a column-sized buffer.
pub fn write_f64_iter(
    path: &Path,
    kind: ArrayKind,
    element_count: u64,
    values: impl IntoIterator<Item = f64>,
) -> Result<(), FormatError> {
    write_f64_iter_with_progress(path, kind, element_count, values, |_| {})
}

pub fn write_f64_iter_with_progress(
    path: &Path,
    kind: ArrayKind,
    element_count: u64,
    values: impl IntoIterator<Item = f64>,
    on_payload_bytes: impl FnMut(u64),
) -> Result<(), FormatError> {
    if kind != ArrayKind::F64 {
        return Err(FormatError::UnsupportedKind(kind));
    }

    let header = iterator_header(kind, element_count)?;
    let header_bytes = header.encode();

    write_typed_array_file(path, &header_bytes, |file, hasher| {
        write_little_endian_iter(
            file,
            hasher,
            values,
            element_count,
            |value| value.to_le_bytes(),
            on_payload_bytes,
        )
    })
}

/// Map a checksummed little-endian `u32` array file.
pub fn map_u32_array(path: &Path) -> Result<MappedU32Array, FormatError> {
    let (mmap, payload_offset, len) = map_typed_array(path, ArrayKind::U32)?;
    Ok(MappedU32Array {
        mmap,
        payload_offset,
        len,
    })
}

pub fn map_u8_array(path: &Path) -> Result<MappedU8Array, FormatError> {
    let (mmap, payload_offset, len) = map_typed_array(path, ArrayKind::U8)?;
    Ok(MappedU8Array {
        mmap,
        payload_offset,
        len,
    })
}

/// Map a checksummed little-endian `u64` array file.
pub fn map_u64_array(path: &Path) -> Result<MappedU64Array, FormatError> {
    let (mmap, payload_offset, len) = map_typed_array(path, ArrayKind::U64)?;
    Ok(MappedU64Array {
        mmap,
        payload_offset,
        len,
    })
}

/// Map a checksummed little-endian `f64` array file.
pub fn map_f64_array(path: &Path) -> Result<MappedF64Array, FormatError> {
    let (mmap, payload_offset, len) = map_typed_array(path, ArrayKind::F64)?;
    Ok(MappedF64Array {
        mmap,
        payload_offset,
        len,
    })
}

fn map_typed_array(path: &Path, kind: ArrayKind) -> Result<(Mmap, usize, usize), FormatError> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let total = mmap.len();
    let prefix = payload_prefix_len();
    let min_size = prefix + checksum::CHECKSUM_SIZE;
    if total < min_size {
        return Err(FormatError::Truncated {
            expected: min_size,
            actual: total,
        });
    }

    let header_bytes = &mmap[..header::HEADER_SIZE];
    let header = ArrayHeader::decode(header_bytes)?;
    header.validate_for_kind(kind)?;

    let payload_len = header.payload_bytes as usize;
    let expected_total = prefix
        .checked_add(payload_len)
        .and_then(|n| n.checked_add(checksum::CHECKSUM_SIZE))
        .ok_or(FormatError::InvalidHeader)?;
    if total != expected_total {
        return Err(FormatError::Truncated {
            expected: expected_total,
            actual: total,
        });
    }

    let payload_offset = prefix;
    let payload = &mmap[payload_offset..payload_offset + payload_len];
    let footer = &mmap[payload_offset + payload_len..];
    checksum::verify_checksum(header_bytes, payload, footer)?;

    let align = kind.element_size();
    let ptr = payload.as_ptr() as usize;
    if !ptr.is_multiple_of(align) {
        return Err(FormatError::Misaligned);
    }

    Ok((mmap, payload_offset, header.element_count as usize))
}
