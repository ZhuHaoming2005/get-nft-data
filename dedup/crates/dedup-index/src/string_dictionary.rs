use dedup_model::{DedupError, EntityId, ErrorContext, StringId};
use dedup_storage::{DigestFunction, DigestMap, Sha256Digest};

#[derive(Debug)]
pub struct StringDictionary<D = Sha256Digest> {
    values: Vec<Box<[u8]>>,
    map: DigestMap<StringId, D>,
}

impl StringDictionary<Sha256Digest> {
    pub fn new(bucket_limit: usize) -> Result<Self, DedupError> {
        Self::with_digest(Sha256Digest, bucket_limit)
    }
}

impl<D: DigestFunction> StringDictionary<D> {
    pub fn with_digest(digest: D, bucket_limit: usize) -> Result<Self, DedupError> {
        Ok(Self {
            values: Vec::new(),
            map: DigestMap::new(digest, bucket_limit)?,
        })
    }

    pub fn intern(&mut self, bytes: &[u8]) -> Result<StringId, DedupError> {
        if let Some(id) = self.map.get(bytes)? {
            return Ok(id);
        }
        let raw = EntityId::try_from(self.values.len()).map_err(|_| DedupError::InvalidInput {
            context: ErrorContext::stage("string_dictionary"),
            message: "StringId capacity exceeded; rebuild with wide_ids".to_owned(),
        })?;
        let id = StringId::new(raw);
        let (stored, inserted) = self.map.insert_with(bytes, || id)?;
        if inserted {
            self.values.push(bytes.into());
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
}
