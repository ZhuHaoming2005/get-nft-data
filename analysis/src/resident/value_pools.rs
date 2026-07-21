use ahash::AHashMap;
use sha2::{Digest, Sha256};
use std::hash::{BuildHasher, Hasher};
use std::sync::OnceLock;

#[derive(Clone, Debug, Default)]
pub struct FrozenBytePool {
    offsets: Vec<u64>,
    bytes: Vec<u8>,
}

impl FrozenBytePool {
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, id: u32) -> &str {
        let start = self.offsets[id as usize] as usize;
        let end = self.offsets[id as usize + 1] as usize;
        // Values are accepted from Rust UTF-8 strings only.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[start..end]) }
    }

    pub fn bytes(&self) -> u64 {
        self.offsets.len() as u64 * 8 + self.bytes.len() as u64
    }
}

#[derive(Debug)]
pub struct ByteInterner {
    offsets: Vec<u64>,
    bytes: Vec<u8>,
    buckets: AHashMap<u64, InternBucket>,
}

#[derive(Debug)]
enum InternBucket {
    One(u32),
    Many(Vec<u32>),
}

impl InternBucket {
    fn for_each(&self, mut visit: impl FnMut(u32) -> bool) -> bool {
        match self {
            Self::One(id) => visit(*id),
            Self::Many(ids) => ids.iter().copied().any(visit),
        }
    }

    fn push(&mut self, id: u32) {
        match self {
            Self::One(first) => {
                *self = Self::Many(vec![*first, id]);
            }
            Self::Many(ids) => ids.push(id),
        }
    }
}

impl Default for ByteInterner {
    fn default() -> Self {
        Self {
            offsets: vec![0],
            bytes: Vec::new(),
            buckets: AHashMap::new(),
        }
    }
}

impl ByteInterner {
    pub fn with_capacity(values: usize, bytes: usize) -> Self {
        Self {
            offsets: Vec::with_capacity(values.saturating_add(1)),
            bytes: Vec::with_capacity(bytes),
            buckets: AHashMap::with_capacity(values),
        }
        .with_zero_offset()
    }

    fn with_zero_offset(mut self) -> Self {
        self.offsets.push(0);
        self
    }

    pub fn intern(&mut self, value: &str) -> u32 {
        let hash = stable_hash(value.as_bytes());
        if let Some(id) = self.lookup_hashed(value, hash) {
            return id;
        }
        let id = u32::try_from(self.len()).expect("byte pool ID capacity checked by builder");
        self.bytes.extend_from_slice(value.as_bytes());
        self.offsets.push(self.bytes.len() as u64);
        self.buckets
            .entry(hash)
            .and_modify(|bucket| bucket.push(id))
            .or_insert(InternBucket::One(id));
        id
    }

    pub fn lookup(&self, value: &str) -> Option<u32> {
        self.lookup_hashed(value, stable_hash(value.as_bytes()))
    }

    fn lookup_hashed(&self, value: &str, hash: u64) -> Option<u32> {
        if let Some(bucket) = self.buckets.get(&hash) {
            let mut found = None;
            bucket.for_each(|id| {
                if self.get(id).as_bytes() == value.as_bytes() {
                    found = Some(id);
                    true
                } else {
                    false
                }
            });
            if let Some(id) = found {
                return Some(id);
            }
        }
        None
    }

    pub fn get(&self, id: u32) -> &str {
        let start = self.offsets[id as usize] as usize;
        let end = self.offsets[id as usize + 1] as usize;
        unsafe { std::str::from_utf8_unchecked(&self.bytes[start..end]) }
    }

    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn reserve_from(&mut self, other: &Self) {
        self.offsets.reserve(other.len());
        self.bytes.reserve(other.bytes.len());
        self.buckets.reserve(other.buckets.len());
    }

    pub fn freeze(self) -> FrozenBytePool {
        FrozenBytePool {
            offsets: self.offsets,
            bytes: self.bytes,
        }
    }

    pub fn estimated_bytes(&self) -> u64 {
        let bucket_values = self
            .buckets
            .values()
            .map(|bucket| match bucket {
                InternBucket::One(_) => 0,
                InternBucket::Many(ids) => {
                    ids.capacity() as u64 * std::mem::size_of::<u32>() as u64
                }
            })
            .sum::<u64>();
        self.offsets.capacity() as u64 * std::mem::size_of::<u64>() as u64
            + self.bytes.capacity() as u64
            + self.buckets.capacity() as u64
                * (std::mem::size_of::<u64>() + std::mem::size_of::<InternBucket>()) as u64
            + bucket_values
    }
}

fn stable_hash(value: &[u8]) -> u64 {
    static STATE: OnceLock<ahash::RandomState> = OnceLock::new();
    let state = STATE.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0x243f_6a88_85a3_08d3,
            0x1319_8a2e_0370_7344,
            0xa409_3822_299f_31d0,
            0x082e_fa98_ec4e_6c89,
        )
    });
    let mut hasher = state.build_hasher();
    hasher.write(value);
    hasher.finish()
}

pub fn digest_hex(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    hex_encode(&digest)
}

pub fn hex_encode(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interner_stores_equal_bytes_once() {
        let mut pool = ByteInterner::default();
        let first = pool.intern("alpha");
        let second = pool.intern("alpha");
        assert_eq!(first, second);
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.freeze().get(first), "alpha");
    }
}
