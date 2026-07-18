use ahash::RandomState;
use dedup_model::{DedupError, EntityId, ErrorContext, StringId};
use dedup_storage::{DigestFunction, DigestMap};
use std::sync::Arc;

#[derive(Debug)]
pub struct StringDictionary<D = FastDigest> {
    values: Vec<Arc<[u8]>>,
    map: DigestMap<StringId, D>,
}

#[derive(Clone, Debug)]
pub struct FastDigest {
    state: RandomState,
}

impl Default for FastDigest {
    fn default() -> Self {
        Self {
            state: RandomState::with_seeds(101, 102, 103, 104),
        }
    }
}

impl DigestFunction for FastDigest {
    fn digest(&self, bytes: &[u8]) -> [u8; 32] {
        let hash = self.state.hash_one(bytes).to_le_bytes();
        let mut digest = [0_u8; 32];
        for part in digest.chunks_exact_mut(hash.len()) {
            part.copy_from_slice(&hash);
        }
        digest
    }
}

impl StringDictionary<FastDigest> {
    pub fn new(bucket_limit: usize) -> Result<Self, DedupError> {
        Self::with_digest(FastDigest::default(), bucket_limit)
    }

    pub fn with_capacity(bucket_limit: usize, capacity: usize) -> Result<Self, DedupError> {
        Self::with_digest_and_capacity(FastDigest::default(), bucket_limit, capacity)
    }
}

impl<D: DigestFunction> StringDictionary<D> {
    pub fn with_digest(digest: D, bucket_limit: usize) -> Result<Self, DedupError> {
        Self::with_digest_and_capacity(digest, bucket_limit, 0)
    }

    pub fn with_digest_and_capacity(
        digest: D,
        bucket_limit: usize,
        capacity: usize,
    ) -> Result<Self, DedupError> {
        Ok(Self {
            values: Vec::with_capacity(capacity),
            map: DigestMap::with_capacity(digest, bucket_limit, capacity)?,
        })
    }

    pub fn intern(&mut self, bytes: &[u8]) -> Result<StringId, DedupError> {
        let raw = EntityId::try_from(self.values.len()).map_err(|_| DedupError::InvalidInput {
            context: ErrorContext::stage("string_dictionary"),
            message: "StringId capacity exceeded; rebuild with wide_ids".to_owned(),
        })?;
        let id = StringId::new(raw);
        let (stored, inserted) = self.map.insert_shared_with(bytes, || id)?;
        if let Some(bytes) = inserted {
            self.values.push(bytes);
        }
        Ok(stored)
    }

    pub fn resolve(&self, id: StringId) -> Option<&[u8]> {
        usize::try_from(id.get())
            .ok()
            .and_then(|index| self.values.get(index))
            .map(AsRef::as_ref)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn max_digest_bucket_len(&self) -> usize {
        self.map.max_bucket_len()
    }

    pub fn spill_stats(&self) -> dedup_storage::DigestMapStats {
        self.map.stats()
    }

    pub fn values(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.values.iter().map(AsRef::as_ref)
    }

    pub fn into_shared_values(self) -> Vec<Arc<[u8]>> {
        self.values
    }

    pub fn from_ordered_values(
        values: impl IntoIterator<Item = Vec<u8>>,
        bucket_limit: usize,
    ) -> Result<Self, DedupError>
    where
        D: Default,
    {
        let mut dictionary = Self::with_digest(D::default(), bucket_limit)?;
        for (expected, value) in values.into_iter().enumerate() {
            let id = dictionary.intern(&value)?;
            if usize::try_from(id.get()).ok() != Some(expected) {
                return Err(DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("string_dictionary"),
                    message: "persisted StringId order contains a duplicate".to_owned(),
                });
            }
        }
        Ok(dictionary)
    }

    pub fn from_ordered_shared_values(
        values: Vec<Arc<[u8]>>,
        bucket_limit: usize,
    ) -> Result<Self, DedupError>
    where
        D: Default,
    {
        let mut dictionary =
            Self::with_digest_and_capacity(D::default(), bucket_limit, values.len())?;
        for (expected, value) in values.into_iter().enumerate() {
            let raw = EntityId::try_from(expected).map_err(|_| DedupError::InvalidInput {
                context: ErrorContext::stage("string_dictionary"),
                message: "StringId capacity exceeded; rebuild with wide_ids".to_owned(),
            })?;
            let id = StringId::new(raw);
            let (stored, inserted) = dictionary.map.insert_shared_with(value, || id)?;
            let Some(bytes) = inserted else {
                return Err(DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("string_dictionary"),
                    message: "persisted StringId order contains a duplicate".to_owned(),
                });
            };
            if stored != id {
                return Err(DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("string_dictionary"),
                    message: "persisted StringId order is inconsistent".to_owned(),
                });
            }
            dictionary.values.push(bytes);
        }
        Ok(dictionary)
    }
}
