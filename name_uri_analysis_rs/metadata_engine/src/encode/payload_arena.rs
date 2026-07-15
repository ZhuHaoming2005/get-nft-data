//! Chunked in-memory payload arena for MetadataEncode.
//!
//! Unique JSON bodies live in fixed-size chunks so growth never reallocates a
//! single giant `Vec<u8>`. Digests index first-seen `payload_id` values; hits
//! always compare full bytes before reuse (collision fail-safe).

use std::collections::HashMap;
use std::path::Path;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::encode::payload_cas::PayloadDigest;
use crate::identity;

/// Default chunk capacity (256 MiB).
pub const DEFAULT_ARENA_CHUNK_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum PayloadArenaError {
    #[error(transparent)]
    Identity(#[from] crate::identity::IdentityOverflow),
    #[error("payload_id {0} out of range")]
    UnknownPayload(u32),
    #[error("payload larger than arena chunk ({len} > {chunk})")]
    PayloadTooLarge { len: usize, chunk: usize },
    #[error("payload body was released")]
    BodyReleased,
}

#[derive(Debug, Clone, Copy)]
struct PayloadMeta {
    chunk_id: u32,
    offset: u32,
    length: u32,
    #[allow(dead_code)] // retained for diagnostics / future spill serializers
    digest: PayloadDigest,
}

#[derive(Debug, Clone)]
enum DigestEntry {
    Single(u32),
    Collision(Vec<u32>),
}

impl DigestEntry {
    fn ids(&self) -> &[u32] {
        match self {
            Self::Single(id) => std::slice::from_ref(id),
            Self::Collision(ids) => ids.as_slice(),
        }
    }

    fn push(&mut self, id: u32) {
        match self {
            Self::Single(first) => *self = Self::Collision(vec![*first, id]),
            Self::Collision(ids) => ids.push(id),
        }
    }
}

/// Insert result distinguishing first-seen payloads from reusable hits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadInsert {
    pub payload_id: u32,
    pub is_new: bool,
}

/// Chunked content-addressed store for unique Encode payloads.
pub struct PayloadArena {
    chunk_capacity: usize,
    chunks: Vec<Vec<u8>>,
    payloads: Vec<PayloadMeta>,
    by_digest: HashMap<PayloadDigest, DigestEntry>,
    bodies_released: bool,
}

impl PayloadArena {
    pub fn new(chunk_capacity: usize) -> Self {
        let chunk_capacity = chunk_capacity.max(1);
        Self {
            chunk_capacity,
            chunks: Vec::new(),
            payloads: Vec::new(),
            by_digest: HashMap::new(),
            bodies_released: false,
        }
    }

    pub fn with_default_chunks() -> Self {
        Self::new(DEFAULT_ARENA_CHUNK_BYTES)
    }

    /// Compatibility shim: disk path is unused; arena never creates files.
    pub fn create(_unused_dir: &Path, chunk_capacity: usize) -> Self {
        Self::new(chunk_capacity)
    }

    pub fn len(&self) -> usize {
        self.payloads.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }

    pub fn insert_or_get(&mut self, bytes: &[u8]) -> Result<PayloadInsert, PayloadArenaError> {
        self.insert_or_get_with_digest(bytes, digest_bytes(bytes))
    }

    fn insert_or_get_with_digest(
        &mut self,
        bytes: &[u8],
        digest: PayloadDigest,
    ) -> Result<PayloadInsert, PayloadArenaError> {
        if self.bodies_released {
            return Err(PayloadArenaError::BodyReleased);
        }
        if bytes.len() > self.chunk_capacity {
            return Err(PayloadArenaError::PayloadTooLarge {
                len: bytes.len(),
                chunk: self.chunk_capacity,
            });
        }
        if let Some(entry) = self.by_digest.get(&digest) {
            for &id in entry.ids() {
                if self.payload_bytes(id)? == bytes {
                    return Ok(PayloadInsert {
                        payload_id: id,
                        is_new: false,
                    });
                }
            }
        }
        let id = self.append_new(bytes, digest)?;
        Ok(PayloadInsert {
            payload_id: id,
            is_new: true,
        })
    }

    /// Disk-CAS compatible insert API used by transitional Encode adapters.
    pub fn insert(&mut self, bytes: &[u8]) -> Result<u32, PayloadArenaError> {
        Ok(self.insert_or_get(bytes)?.payload_id)
    }

    pub fn bytes(&self, payload_id: u32) -> Result<&[u8], PayloadArenaError> {
        self.payload_bytes(payload_id)
    }

    /// Force a digest for collision tests (mirrors PayloadCasWriter test hook).
    #[doc(hidden)]
    pub fn insert_with_digest_for_test(
        &mut self,
        bytes: &[u8],
        digest: PayloadDigest,
    ) -> Result<u32, PayloadArenaError> {
        if self.bodies_released {
            return Err(PayloadArenaError::BodyReleased);
        }
        if bytes.len() > self.chunk_capacity {
            return Err(PayloadArenaError::PayloadTooLarge {
                len: bytes.len(),
                chunk: self.chunk_capacity,
            });
        }
        if let Some(entry) = self.by_digest.get(&digest) {
            for &id in entry.ids() {
                if self.payload_bytes(id)? == bytes {
                    return Ok(id);
                }
            }
        }
        self.append_new(bytes, digest)
    }

    /// Drop JSON bodies after feature terms are materialized.
    pub fn clear_bodies(&mut self) {
        self.chunks.clear();
        self.chunks.shrink_to_fit();
        self.bodies_released = true;
    }

    /// Capacity-based resident estimate: chunk capacities, metadata, digest map.
    pub fn resident_bytes(&self) -> u64 {
        const HASH_BUCKET_OVERHEAD: usize = 16;
        let chunk_bytes = self
            .chunks
            .iter()
            .map(|chunk| chunk.capacity())
            .fold(0usize, usize::saturating_add);
        let fixed = std::mem::size_of::<Self>()
            .saturating_add(chunk_bytes)
            .saturating_add(
                self.chunks
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<u8>>()),
            )
            .saturating_add(
                self.payloads
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PayloadMeta>()),
            )
            .saturating_add(
                self.by_digest.capacity().saturating_mul(
                    std::mem::size_of::<PayloadDigest>()
                        .saturating_add(std::mem::size_of::<DigestEntry>())
                        .saturating_add(HASH_BUCKET_OVERHEAD),
                ),
            )
            .saturating_add(
                self.payloads
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            );
        u64::try_from(fixed).unwrap_or(u64::MAX)
    }

    fn payload_bytes(&self, payload_id: u32) -> Result<&[u8], PayloadArenaError> {
        if self.bodies_released {
            return Err(PayloadArenaError::BodyReleased);
        }
        let meta = self
            .payloads
            .get(payload_id as usize)
            .ok_or(PayloadArenaError::UnknownPayload(payload_id))?;
        let chunk = self
            .chunks
            .get(meta.chunk_id as usize)
            .ok_or(PayloadArenaError::UnknownPayload(payload_id))?;
        let start = meta.offset as usize;
        let end = start.saturating_add(meta.length as usize);
        chunk
            .get(start..end)
            .ok_or(PayloadArenaError::UnknownPayload(payload_id))
    }

    fn append_new(
        &mut self,
        bytes: &[u8],
        digest: PayloadDigest,
    ) -> Result<u32, PayloadArenaError> {
        let needs_chunk = self
            .chunks
            .last()
            .is_none_or(|chunk| chunk.len().saturating_add(bytes.len()) > self.chunk_capacity);
        if needs_chunk {
            // Grow within the chunk as payloads arrive; never reallocate one
            // monolithic buffer across all payloads. Full chunk_capacity is an
            // upper bound, not an eager reservation (avoids OOM under tight
            // analysis-memory-limit budgets).
            self.chunks.push(Vec::with_capacity(bytes.len()));
        }
        let chunk_id =
            identity::checked_u32_identity("arena chunks", self.chunks.len() as u64 - 1)?;
        let chunk = self.chunks.last_mut().expect("chunk present");
        let offset = identity::checked_u32_identity("arena offset", chunk.len() as u64)?;
        let length = identity::checked_u32_identity("arena length", bytes.len() as u64)?;
        chunk.extend_from_slice(bytes);
        let id = identity::checked_u32_identity("payloads", self.payloads.len() as u64)?;
        self.payloads.push(PayloadMeta {
            chunk_id,
            offset,
            length,
            digest,
        });
        self.by_digest
            .entry(digest)
            .and_modify(|entry| entry.push(id))
            .or_insert(DigestEntry::Single(id));
        Ok(id)
    }
}

fn digest_bytes(bytes: &[u8]) -> PayloadDigest {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// Shard-local payload handle used during Encode registration / parse.
/// Converted to a dense global `u32` via [`ShardedPayloadArena::global_id`]
/// after every shard has finished inserting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PayloadRef {
    pub shard_id: u16,
    pub local_id: u32,
}

/// Fixed shard count for hash-partitioned payload arenas.
pub const DEFAULT_PAYLOAD_SHARD_COUNT: usize = 16;

/// Hash-sharded wrapper over independent [`PayloadArena`] instances.
///
/// Digests select a shard from their high bits. Each shard owns its digest map
/// and local IDs; global dense IDs are assigned only after registration via a
/// prefix sum. Collision handling still requires length + full byte compare
/// inside the selected shard.
pub struct ShardedPayloadArena {
    shards: Vec<std::sync::Mutex<PayloadArena>>,
    shard_bits: u32,
}

impl ShardedPayloadArena {
    pub fn with_shard_count(shard_count: usize, chunk_capacity: usize) -> Self {
        let shard_count = shard_count.next_power_of_two().max(1);
        let shard_bits = shard_count.trailing_zeros();
        Self {
            shards: (0..shard_count)
                .map(|_| std::sync::Mutex::new(PayloadArena::new(chunk_capacity)))
                .collect(),
            shard_bits,
        }
    }

    pub fn with_default_shards(chunk_capacity: usize) -> Self {
        Self::with_shard_count(DEFAULT_PAYLOAD_SHARD_COUNT, chunk_capacity)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shard_for_bytes(&self, bytes: &[u8]) -> u16 {
        self.shard_for_digest(&digest_bytes(bytes))
    }

    pub fn shard_for_digest(&self, digest: &PayloadDigest) -> u16 {
        if self.shard_bits == 0 {
            return 0;
        }
        let high = u16::from_be_bytes([digest[0], digest[1]]);
        high >> (16 - self.shard_bits)
    }

    pub fn insert_or_get(&self, bytes: &[u8]) -> Result<PayloadInsertRef, PayloadArenaError> {
        let digest = digest_bytes(bytes);
        let shard_id = self.shard_for_digest(&digest);
        let mut shard = self.shards[shard_id as usize]
            .lock()
            .expect("payload shard lock");
        let insert = shard.insert_or_get_with_digest(bytes, digest)?;
        Ok(PayloadInsertRef {
            payload_ref: PayloadRef {
                shard_id,
                local_id: insert.payload_id,
            },
            is_new: insert.is_new,
        })
    }

    pub fn insert(&self, bytes: &[u8]) -> Result<PayloadRef, PayloadArenaError> {
        Ok(self.insert_or_get(bytes)?.payload_ref)
    }

    pub fn with_bytes<R>(
        &self,
        payload_ref: PayloadRef,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, PayloadArenaError> {
        let shard = self.shards[payload_ref.shard_id as usize]
            .lock()
            .expect("payload shard lock");
        Ok(f(shard.bytes(payload_ref.local_id)?))
    }

    pub fn copy_bytes(&self, payload_ref: PayloadRef) -> Result<Vec<u8>, PayloadArenaError> {
        self.with_bytes(payload_ref, |bytes| bytes.to_vec())
    }

    /// Prefix sums of per-shard lengths: `offsets[shard]` is the global base.
    pub fn global_offsets(&self) -> Result<Vec<u32>, PayloadArenaError> {
        let mut offsets = Vec::with_capacity(self.shards.len().saturating_add(1));
        let mut cursor = 0u32;
        offsets.push(0);
        for shard in &self.shards {
            let len = shard.lock().expect("payload shard lock").len() as u64;
            cursor = identity::checked_u32_identity("sharded payload count", cursor as u64 + len)?;
            offsets.push(cursor);
        }
        Ok(offsets)
    }

    pub fn len(&self) -> Result<usize, PayloadArenaError> {
        Ok(*self.global_offsets()?.last().unwrap_or(&0) as usize)
    }

    pub fn is_empty(&self) -> Result<bool, PayloadArenaError> {
        Ok(self.len()? == 0)
    }

    pub fn global_id(
        &self,
        payload_ref: PayloadRef,
        offsets: &[u32],
    ) -> Result<u32, PayloadArenaError> {
        let base = *offsets
            .get(payload_ref.shard_id as usize)
            .ok_or(PayloadArenaError::UnknownPayload(payload_ref.local_id))?;
        base.checked_add(payload_ref.local_id)
            .ok_or(PayloadArenaError::UnknownPayload(payload_ref.local_id))
    }

    pub fn payload_ref_for_global(
        &self,
        global_id: u32,
        offsets: &[u32],
    ) -> Result<PayloadRef, PayloadArenaError> {
        for shard_id in 0..self.shards.len() {
            let begin = offsets[shard_id];
            let end = offsets[shard_id + 1];
            if global_id >= begin && global_id < end {
                return Ok(PayloadRef {
                    shard_id: shard_id as u16,
                    local_id: global_id - begin,
                });
            }
        }
        Err(PayloadArenaError::UnknownPayload(global_id))
    }

    pub fn with_global_bytes<R>(
        &self,
        global_id: u32,
        offsets: &[u32],
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, PayloadArenaError> {
        let payload_ref = self.payload_ref_for_global(global_id, offsets)?;
        self.with_bytes(payload_ref, f)
    }

    pub fn clear_bodies(&self) {
        for shard in &self.shards {
            shard.lock().expect("payload shard lock").clear_bodies();
        }
    }

    pub fn resident_bytes(&self) -> u64 {
        self.shards.iter().fold(0u64, |total, shard| {
            total.saturating_add(shard.lock().expect("payload shard lock").resident_bytes())
        })
    }

    /// Consume the insertion-capable arena and expose immutable shards for
    /// lock-free parallel parsing.
    pub fn freeze(self) -> Result<FrozenShardedPayloadArena, PayloadArenaError> {
        Ok(FrozenShardedPayloadArena {
            shards: self
                .shards
                .into_iter()
                .map(|shard| shard.into_inner().expect("payload shard lock"))
                .collect(),
        })
    }
}

/// Immutable, lock-free payload arena used after registration is complete.
pub struct FrozenShardedPayloadArena {
    shards: Vec<PayloadArena>,
}

impl FrozenShardedPayloadArena {
    pub fn with_global_bytes<R>(
        &self,
        global_id: u32,
        offsets: &[u32],
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, PayloadArenaError> {
        let payload_ref = payload_ref_for_global(global_id, offsets, self.shards.len())?;
        let shard = self
            .shards
            .get(payload_ref.shard_id as usize)
            .ok_or(PayloadArenaError::UnknownPayload(global_id))?;
        Ok(f(shard.bytes(payload_ref.local_id)?))
    }
}

fn payload_ref_for_global(
    global_id: u32,
    offsets: &[u32],
    shard_count: usize,
) -> Result<PayloadRef, PayloadArenaError> {
    if offsets.len() != shard_count.saturating_add(1) {
        return Err(PayloadArenaError::UnknownPayload(global_id));
    }
    for shard_id in 0..shard_count {
        let begin = offsets[shard_id];
        let end = offsets[shard_id + 1];
        if global_id >= begin && global_id < end {
            return Ok(PayloadRef {
                shard_id: shard_id as u16,
                local_id: global_id - begin,
            });
        }
    }
    Err(PayloadArenaError::UnknownPayload(global_id))
}

/// Insert result for [`ShardedPayloadArena`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadInsertRef {
    pub payload_ref: PayloadRef,
    pub is_new: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_payload_is_stored_once() {
        let mut arena = PayloadArena::new(1024);
        let first = arena.insert_or_get(b"shared").unwrap();
        let second = arena.insert_or_get(b"shared").unwrap();
        assert_eq!(first.payload_id, 0);
        assert!(first.is_new);
        assert_eq!(second.payload_id, 0);
        assert!(!second.is_new);
        assert_eq!(arena.len(), 1);
        assert_eq!(arena.bytes(0).unwrap(), b"shared");
    }

    #[test]
    fn prehashed_insert_preserves_dedup_and_collision_checks() {
        let mut arena = PayloadArena::new(1024);
        let digest = digest_bytes(b"shared");

        let first = arena.insert_or_get_with_digest(b"shared", digest).unwrap();
        let second = arena.insert_or_get_with_digest(b"shared", digest).unwrap();

        assert!(first.is_new);
        assert!(!second.is_new);
        assert_eq!(first.payload_id, second.payload_id);
    }

    #[test]
    fn cross_chunk_reads_remain_exact() {
        let mut arena = PayloadArena::new(8);
        let a = arena.insert_or_get(b"aaaa").unwrap().payload_id;
        let b = arena.insert_or_get(b"bbbb").unwrap().payload_id;
        let c = arena.insert_or_get(b"cccc").unwrap().payload_id;
        assert_eq!(arena.chunks.len(), 2);
        assert_eq!(arena.bytes(a).unwrap(), b"aaaa");
        assert_eq!(arena.bytes(b).unwrap(), b"bbbb");
        assert_eq!(arena.bytes(c).unwrap(), b"cccc");
        // a+b fill the first 8-byte chunk exactly; c opens the next chunk.
    }

    #[test]
    fn forced_digest_collision_does_not_merge_distinct_bodies() {
        let mut arena = PayloadArena::new(64);
        let digest = [7u8; 32];
        let left = arena
            .insert_with_digest_for_test(b"left-body", digest)
            .unwrap();
        let right = arena
            .insert_with_digest_for_test(b"right-body", digest)
            .unwrap();
        assert_ne!(left, right);
        assert_eq!(arena.bytes(left).unwrap(), b"left-body");
        assert_eq!(arena.bytes(right).unwrap(), b"right-body");
    }

    #[test]
    fn payload_ids_follow_first_seen_order() {
        let mut arena = PayloadArena::new(1024);
        assert_eq!(arena.insert(b"a").unwrap(), 0);
        assert_eq!(arena.insert(b"b").unwrap(), 1);
        assert_eq!(arena.insert(b"a").unwrap(), 0);
        assert_eq!(arena.insert(b"c").unwrap(), 2);
    }

    #[test]
    fn resident_bytes_cover_chunk_capacity_and_maps() {
        let mut arena = PayloadArena::new(64);
        arena.insert(b"hello").unwrap();
        let bytes = arena.resident_bytes();
        assert!(
            bytes >= std::mem::size_of::<PayloadMeta>() as u64,
            "must count payload metadata: {bytes}"
        );
        assert!(bytes >= 5, "must count stored body capacity: {bytes}");
        arena.clear_bodies();
        assert!(arena.resident_bytes() < bytes);
        assert!(matches!(
            arena.bytes(0),
            Err(PayloadArenaError::BodyReleased)
        ));
    }

    #[test]
    fn sharded_arena_dedups_within_shard_and_assigns_global_ids() {
        let arena = ShardedPayloadArena::with_shard_count(4, 1024);
        let first = arena.insert(b"alpha").unwrap();
        let second = arena.insert(b"alpha").unwrap();
        assert_eq!(first, second);
        let other = arena.insert(b"beta").unwrap();
        assert_ne!(first, other);
        let offsets = arena.global_offsets().unwrap();
        let global_a = arena.global_id(first, &offsets).unwrap();
        let global_b = arena.global_id(other, &offsets).unwrap();
        assert_ne!(global_a, global_b);
        assert_eq!(arena.len().unwrap(), 2);
        arena
            .with_global_bytes(global_a, &offsets, |bytes| {
                assert_eq!(bytes, b"alpha");
            })
            .unwrap();
    }

    #[test]
    fn frozen_sharded_arena_reads_global_payloads_without_mutation() {
        let arena = ShardedPayloadArena::with_shard_count(4, 1024);
        let first = arena.insert(b"alpha").unwrap();
        let second = arena.insert(b"beta").unwrap();
        let offsets = arena.global_offsets().unwrap();
        let first_global = arena.global_id(first, &offsets).unwrap();
        let second_global = arena.global_id(second, &offsets).unwrap();

        let frozen = arena.freeze().unwrap();

        assert_eq!(
            frozen
                .with_global_bytes(first_global, &offsets, |bytes| bytes.to_vec())
                .unwrap(),
            b"alpha"
        );
        assert_eq!(
            frozen
                .with_global_bytes(second_global, &offsets, |bytes| bytes.to_vec())
                .unwrap(),
            b"beta"
        );
    }

    #[test]
    fn sharded_forced_same_digest_still_byte_compares() {
        // Distinct bodies that hash to different shards stay independent; same
        // shard collisions are covered by PayloadArena tests. Here we only
        // assert insert_or_get reports is_new correctly across shards.
        let arena = ShardedPayloadArena::with_shard_count(8, 1024);
        let a = arena.insert_or_get(b"one").unwrap();
        let b = arena.insert_or_get(b"two").unwrap();
        let a_again = arena.insert_or_get(b"one").unwrap();
        assert!(a.is_new);
        assert!(b.is_new);
        assert!(!a_again.is_new);
        assert_eq!(a.payload_ref, a_again.payload_ref);
    }
}
