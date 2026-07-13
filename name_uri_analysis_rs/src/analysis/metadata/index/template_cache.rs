use super::super::bm25::CompactMetadataScoring;
use super::super::{metadata_doc_index_to_usize, MetadataDocIndex, METADATA_THRESHOLD};

use super::*;

impl<'a> MetadataTemplateCompatibility<'a> {
    pub(in super::super) fn evaluate(
        self,
        left: MetadataDocIndex,
        right: MetadataDocIndex,
    ) -> (bool, u64) {
        if left == right {
            return (true, 0);
        }
        match self {
            Self::Scored(scoring) => {
                let left = metadata_doc_index_to_usize(left);
                let right = metadata_doc_index_to_usize(right);
                let (left_score, right_score) = scoring.score_bidirectional(left, right);
                if left_score >= METADATA_THRESHOLD {
                    (true, 1)
                } else {
                    (right_score >= METADATA_THRESHOLD, 2)
                }
            }
            #[cfg(test)]
            Self::Precomputed(matches) => (
                matches.matches(
                    metadata_doc_index_to_usize(left),
                    metadata_doc_index_to_usize(right),
                ),
                0,
            ),
        }
    }

    pub(in super::super) fn scoring(self) -> Option<&'a CompactMetadataScoring> {
        match self {
            Self::Scored(scoring) => Some(scoring),
            #[cfg(test)]
            Self::Precomputed(_) => None,
        }
    }

    #[cfg(test)]
    pub(in super::super) fn matches(
        self,
        left: MetadataDocIndex,
        right: MetadataDocIndex,
        stats: &mut MetadataContentUnionStats,
    ) -> bool {
        let (matched, scored) = self.evaluate(left, right);
        stats.template_candidate_pairs = stats.template_candidate_pairs.saturating_add(1);
        stats.template_scored_pairs = stats.template_scored_pairs.saturating_add(scored);
        if matched {
            stats.template_matched_pairs = stats.template_matched_pairs.saturating_add(1);
        }
        matched
    }
}

impl MetadataTemplateScoreCacheEntry {
    const EMPTY: Self = Self {
        key: 0,
        score_count: 0,
        matched: false,
        valid: false,
    };
}

impl Default for MetadataTemplateScoreCache {
    fn default() -> Self {
        Self {
            entries: vec![
                MetadataTemplateScoreCacheEntry::EMPTY;
                METADATA_TEMPLATE_SCORE_CACHE_SLOTS
            ]
            .into_boxed_slice(),
        }
    }
}

impl MetadataTemplateScoreCache {
    pub(in super::super) const fn memory_bytes() -> usize {
        std::mem::size_of::<Self>().saturating_add(
            METADATA_TEMPLATE_SCORE_CACHE_SLOTS
                .saturating_mul(std::mem::size_of::<MetadataTemplateScoreCacheEntry>()),
        )
    }

    pub(in super::super) fn mixed_key(key: u64) -> u64 {
        key.wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(key.rotate_right(29))
    }

    pub(in super::super) fn set_start(key: u64) -> usize {
        debug_assert!(METADATA_TEMPLATE_SCORE_CACHE_SLOTS.is_power_of_two());
        debug_assert!(METADATA_TEMPLATE_SCORE_CACHE_WAYS.is_power_of_two());
        debug_assert_eq!(
            METADATA_TEMPLATE_SCORE_CACHE_SLOTS % METADATA_TEMPLATE_SCORE_CACHE_WAYS,
            0
        );
        let set_count = METADATA_TEMPLATE_SCORE_CACHE_SLOTS / METADATA_TEMPLATE_SCORE_CACHE_WAYS;
        (Self::mixed_key(key) as usize & (set_count - 1)) * METADATA_TEMPLATE_SCORE_CACHE_WAYS
    }

    pub(in super::super) fn evaluate(
        &mut self,
        left: MetadataDocIndex,
        right: MetadataDocIndex,
        compatibility: MetadataTemplateCompatibility<'_>,
    ) -> (bool, u64, bool) {
        if left == right {
            return (true, 0, false);
        }
        let (left, right) = if left < right {
            (left, right)
        } else {
            (right, left)
        };
        let key = (u64::from(left) << 32) | u64::from(right);
        let set_start = Self::set_start(key);
        let set_end = set_start + METADATA_TEMPLATE_SCORE_CACHE_WAYS;
        for cached in &self.entries[set_start..set_end] {
            if cached.valid && cached.key == key {
                return (cached.matched, u64::from(cached.score_count), true);
            }
        }
        let (matched, scores) = compatibility.evaluate(left, right);
        let slot = self.entries[set_start..set_end]
            .iter()
            .position(|entry| !entry.valid)
            .map(|offset| set_start + offset)
            .unwrap_or_else(|| {
                let mixed = Self::mixed_key(key);
                set_start + ((mixed >> 32) as usize & (METADATA_TEMPLATE_SCORE_CACHE_WAYS - 1))
            });
        self.entries[slot] = MetadataTemplateScoreCacheEntry {
            key,
            score_count: scores as u8,
            matched,
            valid: true,
        };
        (matched, scores, false)
    }
}

impl MetadataTemplateScoreCachePool {
    pub(in super::super) fn take(&self) -> MetadataTemplateScoreCacheLease<'_> {
        let cache = self
            .caches
            .lock()
            .expect("metadata template score cache pool lock poisoned")
            .pop()
            .unwrap_or_default();
        MetadataTemplateScoreCacheLease {
            pool: self,
            cache: Some(cache),
        }
    }
}

impl std::ops::Deref for MetadataTemplateScoreCacheLease<'_> {
    type Target = MetadataTemplateScoreCache;

    fn deref(&self) -> &Self::Target {
        self.cache
            .as_ref()
            .expect("metadata template score cache lease is empty")
    }
}

impl std::ops::DerefMut for MetadataTemplateScoreCacheLease<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.cache
            .as_mut()
            .expect("metadata template score cache lease is empty")
    }
}

impl Drop for MetadataTemplateScoreCacheLease<'_> {
    fn drop(&mut self) {
        let Some(cache) = self.cache.take() else {
            return;
        };
        self.pool
            .caches
            .lock()
            .expect("metadata template score cache pool lock poisoned")
            .push(cache);
    }
}
