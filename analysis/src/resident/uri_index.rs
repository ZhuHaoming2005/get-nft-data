use crate::model::{NftId, UriValueId};
use crate::resident::{UriFeatureStore, UriNftIdentityStore};
use ahash::AHashMap;
use rayon::prelude::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UriPostingRef {
    pub uri: UriValueId,
    pub shard: usize,
    start: u64,
    end: u64,
}

/// Dense, immutable seed-URI membership prepared once and shared by all NUMA
/// lanes. `UriValueId` is already a compact pool index, so two flag bits avoid
/// rebuilding and probing a pair of hash tables on every lane.
#[derive(Clone, Debug)]
pub struct UriSeedFilter {
    flags: Vec<u8>,
}

impl UriSeedFilter {
    pub fn new(
        uri_value_count: usize,
        seed_token_uris: &[UriValueId],
        seed_image_uris: &[UriValueId],
    ) -> Self {
        let seed_extent = seed_token_uris
            .iter()
            .chain(seed_image_uris)
            .map(|uri| uri.index().saturating_add(1))
            .max()
            .unwrap_or(0);
        let mut flags = vec![0_u8; uri_value_count.max(seed_extent).div_ceil(4)];
        for uri in seed_token_uris {
            let shift = (uri.index() % 4) * 2;
            flags[uri.index() / 4] |= 1 << shift;
        }
        for uri in seed_image_uris {
            let shift = (uri.index() % 4) * 2;
            flags[uri.index() / 4] |= 2 << shift;
        }
        Self { flags }
    }

    fn contains_token(&self, uri: UriValueId) -> bool {
        let shift = (uri.index() % 4) * 2;
        self.flags
            .get(uri.index() / 4)
            .is_some_and(|flags| flags & (1 << shift) != 0)
    }

    fn contains_image(&self, uri: UriValueId) -> bool {
        let shift = (uri.index() % 4) * 2;
        self.flags
            .get(uri.index() / 4)
            .is_some_and(|flags| flags & (2 << shift) != 0)
    }
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
        let seed_filter =
            UriSeedFilter::new(features.values.len(), seed_token_uris, seed_image_uris);
        Self::build_partition_with_filter(
            identities,
            features,
            &seed_filter,
            shard_count,
            lane,
            lane_count,
        )
    }

    pub fn build_partition_with_filter(
        identities: &UriNftIdentityStore,
        features: &UriFeatureStore,
        seed_filter: &UriSeedFilter,
        shard_count: usize,
        lane: usize,
        lane_count: usize,
    ) -> Self {
        Self::build_partition_with_filter_progress(
            identities,
            features,
            seed_filter,
            shard_count,
            lane,
            lane_count,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_partition_with_filter_progress(
        identities: &UriNftIdentityStore,
        features: &UriFeatureStore,
        seed_filter: &UriSeedFilter,
        shard_count: usize,
        lane: usize,
        lane_count: usize,
        progress: Option<&crate::progress::Progress>,
    ) -> Self {
        debug_assert_eq!(identities.nfts.len(), features.features.len());
        // Each NUMA lane scans a disjoint contiguous range. The old owner-lane
        // filter made every lane read the entire feature column before keeping
        // only 1/lane_count of it.
        let start = features.features.len().saturating_mul(lane) / lane_count;
        let end = features
            .features
            .len()
            .saturating_mul(lane.saturating_add(1))
            / lane_count;
        const URI_CHUNK: usize = 4_096;
        features.features[start..end]
            .par_chunks(URI_CHUNK)
            .enumerate()
            .fold(
                || Self::with_shards(shard_count),
                |mut output, (chunk, features)| {
                    let chunk_start = start + chunk * URI_CHUNK;
                    for (offset, feature) in features.iter().enumerate() {
                        let nft = chunk_start + offset;
                        let shard = crate::model::owner_shard(nft as u32, shard_count);
                        if let Some(uri) = feature
                            .token_uri
                            .filter(|uri| seed_filter.contains_token(*uri))
                        {
                            output.pending_token_uri_postings[shard]
                                .entry(uri)
                                .or_default()
                                .push(NftId(nft as u32));
                        }
                        if let Some(uri) = feature
                            .image_uri
                            .filter(|uri| seed_filter.contains_image(*uri))
                        {
                            output.pending_image_uri_postings[shard]
                                .entry(uri)
                                .or_default()
                                .push(NftId(nft as u32));
                        }
                    }
                    if let Some(progress) = progress {
                        progress.add_phase_completed(features.len() as u64);
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
    fn packed_seed_filter_keeps_token_and_image_bits_independent() {
        let filter = UriSeedFilter::new(
            8,
            &[UriValueId(0), UriValueId(3), UriValueId(4)],
            &[UriValueId(1), UriValueId(3), UriValueId(7)],
        );
        for id in 0..8 {
            assert_eq!(
                filter.contains_token(UriValueId(id)),
                [0, 3, 4].contains(&id)
            );
            assert_eq!(
                filter.contains_image(UriValueId(id)),
                [1, 3, 7].contains(&id)
            );
        }
    }

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
