use dedup_model::{
    DedupError, Dimension, EntityKind, ErrorContext, HitEvent, HitEventSink, ScopeId,
};
use roaring::RoaringTreemap;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct BitmapHitSink {
    capacity: usize,
    pending: usize,
    entity_upper_bound: u64,
    shards: Vec<BTreeMap<(Dimension, ScopeId, EntityKind), RoaringTreemap>>,
}

impl BitmapHitSink {
    pub fn new(capacity: usize) -> Result<Self, DedupError> {
        Self::new_sharded(capacity, 1, u64::MAX)
    }

    pub fn new_sharded(
        capacity: usize,
        shard_count: usize,
        entity_upper_bound: u64,
    ) -> Result<Self, DedupError> {
        if capacity == 0 || shard_count == 0 || entity_upper_bound == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("hit_sink"),
                message: "capacity, shard count and entity upper bound must be positive".to_owned(),
            });
        }
        Ok(Self {
            capacity,
            pending: 0,
            entity_upper_bound,
            shards: (0..shard_count).map(|_| BTreeMap::new()).collect(),
        })
    }

    pub fn bitmap(
        &self,
        dimension: Dimension,
        scope: ScopeId,
        kind: EntityKind,
    ) -> Option<&RoaringTreemap> {
        debug_assert!(
            self.shards.iter().skip(1).all(BTreeMap::is_empty),
            "finish_batch must merge HitSink shards before bitmap access"
        );
        self.shards[0].get(&(dimension, scope, kind))
    }

    pub fn finish_batch(&mut self) {
        self.pending = 0;
        for shard in 1..self.shards.len() {
            let entries = std::mem::take(&mut self.shards[shard]);
            for (key, bitmap) in entries {
                *self.shards[0].entry(key).or_default() |= bitmap;
            }
        }
    }

    pub fn entries(
        &self,
    ) -> impl Iterator<Item = (&(Dimension, ScopeId, EntityKind), &RoaringTreemap)> {
        debug_assert!(
            self.shards.iter().skip(1).all(BTreeMap::is_empty),
            "finish_batch must merge HitSink shards before entry access"
        );
        self.shards[0].iter()
    }

    pub fn insert_bitmap(
        &mut self,
        dimension: Dimension,
        scope: ScopeId,
        kind: EntityKind,
        bitmap: RoaringTreemap,
    ) {
        *self.shards[0].entry((dimension, scope, kind)).or_default() |= bitmap;
    }

    pub fn apply_image_priority(&mut self, scope: ScopeId) {
        let token_key = (Dimension::TokenUri, scope, EntityKind::Nft);
        let image_key = (Dimension::ImageUri, scope, EntityKind::Nft);
        for shard in &mut self.shards {
            if let Some(token) = shard.get(&token_key).cloned()
                && let Some(image) = shard.get_mut(&image_key)
            {
                *image -= token;
            }
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn shard_for(&self, entity_id: u64) -> usize {
        let scaled = u128::from(entity_id)
            .saturating_mul(self.shards.len() as u128)
            .checked_div(u128::from(self.entity_upper_bound))
            .unwrap_or_default();
        usize::try_from(scaled)
            .unwrap_or(usize::MAX)
            .min(self.shards.len() - 1)
    }
}

impl HitEventSink for BitmapHitSink {
    fn submit(&mut self, event: HitEvent) -> Result<(), DedupError> {
        let shard = self.shard_for(event.entity_id);
        let key = (event.dimension, event.scope, event.entity_kind);
        if self.shards[shard]
            .get(&key)
            .is_some_and(|bitmap| bitmap.contains(event.entity_id))
        {
            return Ok(());
        }
        if self.pending == self.capacity {
            return Err(DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("hit_sink"),
                requested: 1,
            });
        }
        self.pending += 1;
        self.shards[shard]
            .entry(key)
            .or_default()
            .insert(event.entity_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dedup_model::ChainId;

    #[test]
    fn de_duplicates_hits_and_token_uri_has_priority() {
        let scope = ScopeId::Intra(ChainId::new(0));
        let mut sink = BitmapHitSink::new(8).unwrap();
        for dimension in [
            Dimension::TokenUri,
            Dimension::TokenUri,
            Dimension::ImageUri,
        ] {
            sink.submit(HitEvent {
                dimension,
                scope,
                entity_kind: EntityKind::Nft,
                entity_id: 7,
            })
            .unwrap();
        }
        sink.finish_batch();
        sink.apply_image_priority(scope);
        assert_eq!(
            sink.bitmap(Dimension::TokenUri, scope, EntityKind::Nft)
                .unwrap()
                .len(),
            1
        );
        assert!(
            sink.bitmap(Dimension::ImageUri, scope, EntityKind::Nft)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn range_shards_merge_deterministically_and_enforce_capacity() {
        let scope = ScopeId::Intra(ChainId::new(0));
        let mut sink = BitmapHitSink::new_sharded(4, 4, 16).unwrap();
        for entity_id in [0, 4, 8, 12] {
            sink.submit(HitEvent {
                dimension: Dimension::Name,
                scope,
                entity_kind: EntityKind::Contract,
                entity_id,
            })
            .unwrap();
        }
        assert_eq!(sink.shard_count(), 4);
        let error = sink
            .submit(HitEvent {
                dimension: Dimension::Name,
                scope,
                entity_kind: EntityKind::Contract,
                entity_id: 15,
            })
            .unwrap_err();
        assert!(matches!(error, DedupError::ResourceBudgetExceeded { .. }));
        sink.finish_batch();
        assert_eq!(
            sink.bitmap(Dimension::Name, scope, EntityKind::Contract)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 4, 8, 12]
        );
    }

    #[test]
    fn duplicate_submissions_do_not_consume_pending_capacity() {
        let scope = ScopeId::Intra(ChainId::new(0));
        let event = HitEvent {
            dimension: Dimension::Name,
            scope,
            entity_kind: EntityKind::Contract,
            entity_id: 7,
        };
        let mut sink = BitmapHitSink::new(1).unwrap();

        sink.submit(event).unwrap();
        sink.submit(event).unwrap();
        sink.submit(event).unwrap();
        assert!(matches!(
            sink.submit(HitEvent {
                entity_id: 8,
                ..event
            }),
            Err(DedupError::ResourceBudgetExceeded { .. })
        ));
        sink.finish_batch();
        assert_eq!(
            sink.bitmap(Dimension::Name, scope, EntityKind::Contract)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![7]
        );
    }
}
