use ahash::{AHashMap, RandomState};
use dedup_model::{DedupError, ErrorContext};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

pub trait DigestFunction {
    fn digest(&self, bytes: &[u8]) -> [u8; 32];
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Sha256Digest;

impl DigestFunction for Sha256Digest {
    fn digest(&self, bytes: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        Sha256::digest(bytes).into()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketLocation {
    Resident,
    Spilled,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DigestMapStats {
    pub spill_bytes: u64,
    pub spill_handle_touches: u64,
}

#[derive(Debug)]
struct Entry<V> {
    bytes: Vec<u8>,
    value: V,
}

#[derive(Clone, Copy, Debug)]
struct SpillHandle<V> {
    offset: u64,
    length: u64,
    value: V,
}

#[derive(Debug)]
enum Bucket<V> {
    Resident(Vec<Entry<V>>),
    Spilled(Vec<SpillHandle<V>>),
}

#[derive(Debug)]
pub struct DigestMap<V, D> {
    digest: D,
    bucket_limit: usize,
    buckets: AHashMap<[u8; 32], Bucket<V>>,
    spill: Mutex<File>,
    stats: DigestMapStats,
}

impl<V: Copy, D: DigestFunction> DigestMap<V, D> {
    pub fn new(digest: D, bucket_limit: usize) -> Result<Self, DedupError> {
        if bucket_limit == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("digest_map"),
                message: "bucket limit must be positive".to_owned(),
            });
        }
        Ok(Self {
            digest,
            bucket_limit,
            buckets: AHashMap::with_hasher(RandomState::with_seeds(1, 2, 3, 4)),
            spill: Mutex::new(tempfile::tempfile()?),
            stats: DigestMapStats::default(),
        })
    }

    pub fn get(&self, bytes: &[u8]) -> Result<Option<V>, DedupError> {
        let digest = self.digest.digest(bytes);
        let Some(bucket) = self.buckets.get(&digest) else {
            return Ok(None);
        };
        match bucket {
            Bucket::Resident(entries) => Ok(entries
                .iter()
                .find(|entry| entry.bytes == bytes)
                .map(|entry| entry.value)),
            Bucket::Spilled(handles) => {
                let mut file = self.spill.lock().unwrap_or_else(|error| error.into_inner());
                for handle in handles {
                    if spilled_bytes_equal(&mut file, *handle, bytes)? {
                        return Ok(Some(handle.value));
                    }
                }
                Ok(None)
            }
        }
    }

    pub fn insert_with<F>(&mut self, bytes: &[u8], create: F) -> Result<(V, bool), DedupError>
    where
        F: FnOnce() -> V,
    {
        if let Some(value) = self.get(bytes)? {
            return Ok((value, false));
        }
        let digest = self.digest.digest(bytes);
        let value = create();
        let spill = &self.spill;
        let stats = &mut self.stats;
        let bucket = self
            .buckets
            .entry(digest)
            .or_insert_with(|| Bucket::Resident(Vec::new()));
        match bucket {
            Bucket::Resident(entries) => {
                entries.push(Entry {
                    bytes: bytes.to_vec(),
                    value,
                });
                if entries.len() > self.bucket_limit {
                    let pending = std::mem::take(entries);
                    let mut handles = Vec::with_capacity(pending.len());
                    let mut file = spill.lock().unwrap_or_else(|error| error.into_inner());
                    for entry in pending {
                        handles.push(append_spilled_bytes(
                            &mut file,
                            &entry.bytes,
                            entry.value,
                            stats,
                        )?);
                    }
                    *bucket = Bucket::Spilled(handles);
                }
            }
            Bucket::Spilled(handles) => {
                let mut file = spill.lock().unwrap_or_else(|error| error.into_inner());
                handles.push(append_spilled_bytes(&mut file, bytes, value, stats)?);
            }
        }
        Ok((value, true))
    }

    pub fn bucket_location(&self, bytes: &[u8]) -> Option<BucketLocation> {
        self.buckets
            .get(&self.digest.digest(bytes))
            .map(|bucket| match bucket {
                Bucket::Resident(_) => BucketLocation::Resident,
                Bucket::Spilled(_) => BucketLocation::Spilled,
            })
    }

    pub fn max_bucket_len(&self) -> usize {
        self.buckets
            .values()
            .map(|bucket| match bucket {
                Bucket::Resident(entries) => entries.len(),
                Bucket::Spilled(handles) => handles.len(),
            })
            .max()
            .unwrap_or(0)
    }

    pub fn stats(&self) -> DigestMapStats {
        self.stats
    }
}

fn append_spilled_bytes<V: Copy>(
    file: &mut File,
    bytes: &[u8],
    value: V,
    stats: &mut DigestMapStats,
) -> Result<SpillHandle<V>, DedupError> {
    let offset = file.seek(SeekFrom::End(0))?;
    file.write_all(bytes)?;
    let length = u64::try_from(bytes.len()).map_err(|_| DedupError::ResourceBudgetExceeded {
        context: ErrorContext::stage("digest_map"),
        requested: u64::MAX,
    })?;
    stats.spill_bytes =
        stats
            .spill_bytes
            .checked_add(length)
            .ok_or(DedupError::CounterOverflow {
                counter: "digest_map_spill_bytes",
            })?;
    stats.spill_handle_touches =
        stats
            .spill_handle_touches
            .checked_add(1)
            .ok_or(DedupError::CounterOverflow {
                counter: "digest_map_spill_handle_touches",
            })?;
    Ok(SpillHandle {
        offset,
        length,
        value,
    })
}

fn spilled_bytes_equal<V: Copy>(
    file: &mut File,
    handle: SpillHandle<V>,
    expected: &[u8],
) -> Result<bool, DedupError> {
    if handle.length
        != u64::try_from(expected.len()).map_err(|_| DedupError::ResourceBudgetExceeded {
            context: ErrorContext::stage("digest_map"),
            requested: u64::MAX,
        })?
    {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(handle.offset))?;
    let mut remaining = expected;
    let mut buffer = [0_u8; 8 * 1024];
    while !remaining.is_empty() {
        let length = remaining.len().min(buffer.len());
        file.read_exact(&mut buffer[..length])?;
        if buffer[..length] != remaining[..length] {
            return Ok(false);
        }
        remaining = &remaining[length..];
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug)]
    struct FixedDigest;

    impl DigestFunction for FixedDigest {
        fn digest(&self, _bytes: &[u8]) -> [u8; 32] {
            [7; 32]
        }
    }

    #[test]
    fn collision_never_implies_equality_and_only_hot_bucket_spills() {
        let mut map = DigestMap::new(FixedDigest, 1).unwrap();
        assert_eq!(map.insert_with(b"alpha", || 1).unwrap(), (1, true));
        assert_eq!(map.insert_with(b"beta", || 2).unwrap(), (2, true));
        assert_eq!(map.insert_with(b"alpha", || 3).unwrap(), (1, false));
        assert_eq!(map.get(b"beta").unwrap(), Some(2));
        assert_eq!(map.bucket_location(b"alpha"), Some(BucketLocation::Spilled));
        assert_eq!(map.stats().spill_bytes, 9);
        assert_eq!(map.stats().spill_handle_touches, 2);
    }
}
