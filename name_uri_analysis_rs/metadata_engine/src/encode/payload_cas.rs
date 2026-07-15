//! Append-only content-addressed payload packs.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::format::{self, atomic, ArrayKind, FormatError};

/// SHA-256 digest used as the CAS primary key.
pub type PayloadDigest = [u8; 32];

/// Default max pack segment size (64 MiB). Tests may pass a smaller cap.
pub const DEFAULT_MAX_PACK_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum PayloadCasError {
    #[error(transparent)]
    Identity(#[from] crate::identity::IdentityOverflow),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("payload_id {0} out of range")]
    UnknownPayload(u32),
    #[error("payload larger than max pack size ({len} > {max})")]
    PayloadTooLarge { len: u64, max: u64 },
}

#[derive(Debug, Clone)]
struct PayloadMeta {
    pack_id: u32,
    /// Byte offset within the pack file.
    offset: u64,
    length: u32,
    digest: PayloadDigest,
}

#[derive(Debug, Clone)]
enum PayloadDigestEntry {
    Single(u32),
    Collision(Vec<u32>),
}

impl PayloadDigestEntry {
    fn ids(&self) -> &[u32] {
        match self {
            Self::Single(id) => std::slice::from_ref(id),
            Self::Collision(ids) => ids,
        }
    }

    fn push(&mut self, id: u32) {
        match self {
            Self::Single(first) => *self = Self::Collision(vec![*first, id]),
            Self::Collision(ids) => ids.push(id),
        }
    }
}

/// Streaming CAS writer under `payload_blobs/`.
pub struct PayloadCasWriter {
    dir: PathBuf,
    max_pack_bytes: u64,
    current_pack_id: u32,
    current_len: u64,
    current_file: Option<File>,
    payloads: Vec<PayloadMeta>,
    by_digest: HashMap<PayloadDigest, PayloadDigestEntry>,
}

impl PayloadCasWriter {
    pub fn create(payload_blobs_dir: &Path, max_pack_bytes: u64) -> Result<Self, PayloadCasError> {
        fs::create_dir_all(payload_blobs_dir)?;
        Ok(Self {
            dir: payload_blobs_dir.to_path_buf(),
            max_pack_bytes: max_pack_bytes.max(1),
            current_pack_id: 0,
            current_len: 0,
            current_file: None,
            payloads: Vec::new(),
            by_digest: HashMap::new(),
        })
    }

    /// Insert payload bytes; reuse `payload_id` only when digest hits and full bytes equal.
    pub fn insert(&mut self, bytes: &[u8]) -> Result<u32, PayloadCasError> {
        let digest = digest_bytes(bytes);
        self.insert_with_digest(bytes, digest)
    }

    /// Capacity-based resident bytes retained by the streaming CAS index.
    /// Payload bodies are excluded because they are written directly to pack files.
    pub fn resident_bytes(&self) -> u64 {
        const HASH_BUCKET_OVERHEAD: usize = 16;
        let fixed = std::mem::size_of::<Self>()
            .saturating_add(self.dir.as_os_str().len())
            .saturating_add(
                self.payloads
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PayloadMeta>()),
            )
            .saturating_add(
                self.by_digest.capacity().saturating_mul(
                    std::mem::size_of::<PayloadDigest>()
                        .saturating_add(std::mem::size_of::<PayloadDigestEntry>())
                        .saturating_add(HASH_BUCKET_OVERHEAD),
                ),
            )
            // At most one collision-list ID exists per payload. Counting this
            // for every payload is conservative and keeps the query O(1).
            .saturating_add(
                self.payloads
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            );
        u64::try_from(fixed).unwrap_or(u64::MAX)
    }

    /// Test hook: force a digest (hash-collision paths).
    ///
    /// Not `#[cfg(test)]` so integration tests under `tests/` can exercise collisions;
    /// production Encode must call [`Self::insert`] only.
    #[doc(hidden)]
    pub fn insert_with_digest_for_test(
        &mut self,
        bytes: &[u8],
        digest: PayloadDigest,
    ) -> Result<u32, PayloadCasError> {
        self.insert_with_digest(bytes, digest)
    }

    fn insert_with_digest(
        &mut self,
        bytes: &[u8],
        digest: PayloadDigest,
    ) -> Result<u32, PayloadCasError> {
        if let Some(entry) = self.by_digest.get(&digest) {
            for &id in entry.ids() {
                let meta = &self.payloads[id as usize];
                if meta.length as usize == bytes.len() && self.bytes_equal(meta, bytes)? {
                    return Ok(id);
                }
            }
        }

        self.append_new(bytes, digest)
    }

    fn bytes_equal(&self, meta: &PayloadMeta, bytes: &[u8]) -> Result<bool, PayloadCasError> {
        let path = pack_path(&self.dir, meta.pack_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(meta.offset))?;
        const COMPARE_BUFFER_BYTES: usize = 64 * 1024;
        let mut buffer = [0u8; COMPARE_BUFFER_BYTES];
        let mut compared = 0usize;
        while compared < bytes.len() {
            let chunk_len = (bytes.len() - compared).min(buffer.len());
            file.read_exact(&mut buffer[..chunk_len])?;
            if buffer[..chunk_len] != bytes[compared..compared + chunk_len] {
                return Ok(false);
            }
            compared += chunk_len;
        }
        Ok(true)
    }

    fn append_new(&mut self, bytes: &[u8], digest: PayloadDigest) -> Result<u32, PayloadCasError> {
        let len = bytes.len() as u64;
        let payload_length = crate::identity::checked_u32_identity("payload bytes", len)?;
        if len > self.max_pack_bytes {
            return Err(PayloadCasError::PayloadTooLarge {
                len,
                max: self.max_pack_bytes,
            });
        }

        if self.current_file.is_none() || self.current_len.saturating_add(len) > self.max_pack_bytes
        {
            self.rotate_pack()?;
        }

        let offset = self.current_len;
        let pack_id = self.current_pack_id;
        {
            let file = self.current_file.as_mut().expect("pack file open");
            file.write_all(bytes)?;
        }
        self.current_len += len;

        let id = crate::identity::checked_u32_identity("payloads", self.payloads.len() as u64)?;
        self.payloads.push(PayloadMeta {
            pack_id,
            offset,
            length: payload_length,
            digest,
        });
        self.by_digest
            .entry(digest)
            .and_modify(|entry| entry.push(id))
            .or_insert(PayloadDigestEntry::Single(id));
        Ok(id)
    }

    fn rotate_pack(&mut self) -> Result<(), PayloadCasError> {
        if let Some(mut file) = self.current_file.take() {
            file.flush()?;
            file.sync_all()?;
            self.current_pack_id = self.current_pack_id.saturating_add(1);
        }
        let path = pack_path(&self.dir, self.current_pack_id);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        self.current_file = Some(file);
        self.current_len = 0;
        Ok(())
    }

    /// Flush open pack and write index arrays. Returns a read handle for Encode-side CAS use.
    pub fn finish(mut self) -> Result<PayloadCasIndex, PayloadCasError> {
        if let Some(mut file) = self.current_file.take() {
            file.flush()?;
            file.sync_all()?;
        } else if self.payloads.is_empty() {
            // Ensure at least an empty pack file exists for a consistent layout.
            let path = pack_path(&self.dir, 0);
            File::create(path)?;
        }

        format::write_u64_iter(
            &self.dir.join("payload_offsets.u64"),
            ArrayKind::U64,
            self.payloads.len() as u64,
            self.payloads.iter().map(|meta| {
                // High 32 bits = pack_id, low 32 bits = offset within pack
                // (packs are capped below 4 GiB).
                ((meta.pack_id as u64) << 32) | (meta.offset & 0xffff_ffff)
            }),
        )?;
        format::write_u32_iter(
            &self.dir.join("payload_lengths.u32"),
            ArrayKind::U32,
            self.payloads.len() as u64,
            self.payloads.iter().map(|meta| meta.length),
        )?;
        atomic::write_atomic_file(&self.dir.join("payload_hashes.bin"), |file| {
            for meta in &self.payloads {
                file.write_all(&meta.digest)?;
            }
            Ok(())
        })?;

        Ok(PayloadCasIndex {
            dir: self.dir,
            payloads: self.payloads,
        })
    }
}

/// Encode-side index over finished payload packs (not part of Match FeatureView).
pub struct PayloadCasIndex {
    dir: PathBuf,
    payloads: Vec<PayloadMeta>,
}

impl PayloadCasIndex {
    pub fn payload_count(&self) -> usize {
        self.payloads.len()
    }

    pub fn read_payload_bytes(&self, payload_id: u32) -> Result<Vec<u8>, PayloadCasError> {
        let meta = self
            .payloads
            .get(payload_id as usize)
            .ok_or(PayloadCasError::UnknownPayload(payload_id))?;
        let path = pack_path(&self.dir, meta.pack_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(meta.offset))?;
        let mut buf = vec![0u8; meta.length as usize];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }
}

fn pack_path(dir: &Path, pack_id: u32) -> PathBuf {
    dir.join(format!("pack-{pack_id:06}.bin"))
}

fn digest_bytes(bytes: &[u8]) -> PayloadDigest {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// Compute the CAS digest used by [`PayloadCasWriter::insert`].
pub fn payload_digest(bytes: &[u8]) -> PayloadDigest {
    digest_bytes(bytes)
}
