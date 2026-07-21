use crate::model::{NftId, UriValueId};
use crate::resident::{UriFeatureStore, UriNftIdentityStore};
use ahash::{AHashMap, AHashSet};
use rayon::prelude::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UriPostingRef {
    pub uri: UriValueId,
    pub shard: usize,
    start: u64,
    end: u64,
}

#[derive(Clone, Debug, Default)]
pub struct UriIndex {
    token_uri_postings: Vec<NftId>,
    image_uri_postings: Vec<NftId>,
    token_uri_directory: AHashMap<UriValueId, Vec<UriPostingRef>>,
    image_uri_directory: AHashMap<UriValueId, Vec<UriPostingRef>>,
    pending_token_uri_postings: Vec<AHashMap<UriValueId, Vec<NftId>>>,
    pending_image_uri_postings: Vec<AHashMap<UriValueId, Vec<NftId>>>,
}

impl UriIndex {
    pub fn build(
        identities: &UriNftIdentityStore,
        features: &UriFeatureStore,
        seed_token_uris: &[UriValueId],
        seed_image_uris: &[UriValueId],
        shard_count: usize,
    ) -> Self {
        let mut index = Self::build_partition(
            identities,
            features,
            seed_token_uris,
            seed_image_uris,
            shard_count,
            0,
            1,
        );
        index.finalize();
        index
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_partition(
        identities: &UriNftIdentityStore,
        features: &UriFeatureStore,
        seed_token_uris: &[UriValueId],
        seed_image_uris: &[UriValueId],
        shard_count: usize,
        lane: usize,
        lane_count: usize,
    ) -> Self {
        debug_assert_eq!(identities.nfts.len(), features.features.len());
        let seed_token_uris = seed_token_uris.iter().copied().collect::<AHashSet<_>>();
        let seed_image_uris = seed_image_uris.iter().copied().collect::<AHashSet<_>>();
        // Each NUMA lane scans a disjoint contiguous range. The old owner-lane
        // filter made every lane read the entire feature column before keeping
        // only 1/lane_count of it.
        let start = features.features.len().saturating_mul(lane) / lane_count;
        let end = features
            .features
            .len()
            .saturating_mul(lane.saturating_add(1))
            / lane_count;
        features.features[start..end]
            .par_iter()
            .enumerate()
            .fold(
                || Self::with_shards(shard_count),
                |mut output, (offset, feature)| {
                    let nft = start + offset;
                    let shard = crate::model::owner_shard(nft as u32, shard_count);
                    if let Some(uri) = feature
                        .token_uri
                        .filter(|uri| seed_token_uris.contains(uri))
                    {
                        output.pending_token_uri_postings[shard]
                            .entry(uri)
                            .or_default()
                            .push(NftId(nft as u32));
                    }
                    if let Some(uri) = feature
                        .image_uri
                        .filter(|uri| seed_image_uris.contains(uri))
                    {
                        output.pending_image_uri_postings[shard]
                            .entry(uri)
                            .or_default()
                            .push(NftId(nft as u32));
                    }
                    output
                },
            )
            .reduce(
                || Self::with_shards(shard_count),
                |mut left, right| {
                    left.merge(right);
                    left
                },
            )
    }

    fn with_shards(shard_count: usize) -> Self {
        Self {
            pending_token_uri_postings: (0..shard_count).map(|_| AHashMap::new()).collect(),
            pending_image_uri_postings: (0..shard_count).map(|_| AHashMap::new()).collect(),
            ..Default::default()
        }
    }

    pub(crate) fn merge(&mut self, mut right: Self) {
        debug_assert!(self.token_uri_postings.is_empty());
        debug_assert!(right.token_uri_postings.is_empty());
        for shard in 0..self.pending_token_uri_postings.len() {
            for (uri, mut postings) in right.pending_token_uri_postings[shard].drain() {
                self.pending_token_uri_postings[shard]
                    .entry(uri)
                    .or_default()
                    .append(&mut postings);
            }
            for (uri, mut postings) in right.pending_image_uri_postings[shard].drain() {
                self.pending_image_uri_postings[shard]
                    .entry(uri)
                    .or_default()
                    .append(&mut postings);
            }
        }
    }

    /// Converts build maps into compact contiguous postings and a small
    /// URI-to-present-shards directory. The build maps are dropped here, so
    /// query execution does not retain two representations of the index.
    pub(crate) fn finalize(&mut self) {
        (self.token_uri_postings, self.token_uri_directory) =
            flatten_postings(std::mem::take(&mut self.pending_token_uri_postings));
        (self.image_uri_postings, self.image_uri_directory) =
            flatten_postings(std::mem::take(&mut self.pending_image_uri_postings));
    }

    pub(crate) fn token_posting_refs(&self, uri: UriValueId) -> &[UriPostingRef] {
        self.token_uri_directory
            .get(&uri)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub(crate) fn image_posting_refs(&self, uri: UriValueId) -> &[UriPostingRef] {
        self.image_uri_directory
            .get(&uri)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub(crate) fn token_postings(&self, posting: UriPostingRef) -> &[NftId] {
        &self.token_uri_postings[posting.start as usize..posting.end as usize]
    }

    pub(crate) fn image_postings(&self, posting: UriPostingRef) -> &[NftId] {
        &self.image_uri_postings[posting.start as usize..posting.end as usize]
    }

    pub fn posting_count(&self) -> u64 {
        (self.token_uri_postings.len() + self.image_uri_postings.len()) as u64
    }
}

fn flatten_postings(
    shards: Vec<AHashMap<UriValueId, Vec<NftId>>>,
) -> (Vec<NftId>, AHashMap<UriValueId, Vec<UriPostingRef>>) {
    let posting_count = shards
        .iter()
        .flat_map(|shard| shard.values())
        .map(Vec::len)
        .sum();
    let entry_count = shards.iter().map(|shard| shard.len()).sum();
    let mut postings_flat = Vec::with_capacity(posting_count);
    let mut directory = AHashMap::<UriValueId, Vec<UriPostingRef>>::with_capacity(entry_count);
    for (shard, shard_postings) in shards.into_iter().enumerate() {
        let mut entries = shard_postings.into_iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(uri, _)| *uri);
        for (uri, mut postings) in entries {
            postings.sort_unstable();
            let start = postings_flat.len() as u64;
            postings_flat.append(&mut postings);
            let end = postings_flat.len() as u64;
            directory.entry(uri).or_default().push(UriPostingRef {
                uri,
                shard,
                start,
                end,
            });
        }
    }
    (postings_flat, directory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContractId, NftIdentityRecord, TokenIdId, UriFeatureRecord};
    use crate::resident::ByteInterner;

    #[test]
    fn disjoint_lane_partitions_equal_single_pass_index() {
        let mut token_ids = ByteInterner::default();
        let token = TokenIdId(token_ids.intern("1"));
        let identities = UriNftIdentityStore {
            token_ids: token_ids.freeze(),
            nfts: (0..64)
                .map(|nft| NftIdentityRecord {
                    contract_id: ContractId(nft / 4),
                    token_id_id: token,
                })
                .collect(),
            contract_offsets: Vec::new(),
        };
        let features = UriFeatureStore {
            values: ByteInterner::default().freeze(),
            features: (0..64)
                .map(|nft| UriFeatureRecord {
                    token_uri: (nft % 3 != 0).then_some(UriValueId(nft % 2)),
                    image_uri: (nft % 5 != 0).then_some(UriValueId((nft + 1) % 2)),
                })
                .collect(),
        };
        let expected = UriIndex::build(
            &identities,
            &features,
            &[UriValueId(0), UriValueId(1)],
            &[UriValueId(0), UriValueId(1)],
            8,
        );
        let mut actual = UriIndex::build_partition(
            &identities,
            &features,
            &[UriValueId(0), UriValueId(1)],
            &[UriValueId(0), UriValueId(1)],
            8,
            0,
            2,
        );
        actual.merge(UriIndex::build_partition(
            &identities,
            &features,
            &[UriValueId(0), UriValueId(1)],
            &[UriValueId(0), UriValueId(1)],
            8,
            1,
            2,
        ));
        actual.finalize();

        assert_eq!(actual.token_uri_postings, expected.token_uri_postings);
        assert_eq!(actual.image_uri_postings, expected.image_uri_postings);
        assert_eq!(actual.token_uri_directory, expected.token_uri_directory);
        assert_eq!(actual.image_uri_directory, expected.image_uri_directory);
    }
}
