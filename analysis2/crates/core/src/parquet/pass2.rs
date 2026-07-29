//! Pass 2: metadata_json projection into descending anchors.

use ahash::{AHashMap, AHashSet};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::fs::File;

use crate::Analysis2Error;
use crate::entity::{
    ContractId, MetadataRecord, ResidentStore, SourceOrder, compare_token_ids_desc,
    normalized_evm_token_slice,
};
use crate::parquet::LoadOptions;
use crate::parquet::metadata::validated_metadata;
use crate::parquet::pass1::{ProjectedUtf8Columns, normalize_chain};
use crate::parquet::validate::{PASS2_COLUMNS, ValidatedInput};
use crate::progress::ProgressObserver;

/// Per-file metadata records. `metadata_anchors = None` keeps all valid rows.
#[derive(Default)]
struct ShardAnchors {
    by_contract: AHashMap<(String, String), Vec<MetadataRecord>>,
}

/// Exact subset needed by the seed-scoped metadata alignment algorithm.
pub struct MetadataQuerySelection<'a> {
    seed_contracts: AHashMap<String, AHashSet<String>>,
    evm_seed_token_ids: AHashSet<&'a str>,
    evm_seed_token_sets: Vec<AHashSet<&'a str>>,
}

impl<'a> MetadataQuerySelection<'a> {
    pub fn new(store: &'a ResidentStore, seeds: &[ContractId]) -> Self {
        let mut seed_contracts = AHashMap::<String, AHashSet<String>>::new();
        let mut evm_seed_token_ids = AHashSet::new();
        let mut evm_seed_token_sets = Vec::new();
        for &seed in seeds {
            let contract = &store.contracts[seed as usize];
            let chain = store.chain_name(contract.chain_id);
            seed_contracts
                .entry(chain.to_owned())
                .or_default()
                .insert(contract.address.clone());
            if store.is_evm_chain(chain) {
                let mut seed_token_ids = AHashSet::new();
                for &nft in store.nfts_for_contract(seed) {
                    let token = normalized_evm_token_slice(
                        store.string(store.nfts[nft as usize].token_id_id),
                    );
                    evm_seed_token_ids.insert(token);
                    seed_token_ids.insert(token);
                }
                evm_seed_token_sets.push(seed_token_ids);
            }
        }
        Self {
            seed_contracts,
            evm_seed_token_ids,
            evm_seed_token_sets,
        }
    }

    fn is_seed_contract(&self, chain: &str, contract: &str) -> bool {
        self.seed_contracts
            .get(chain)
            .is_some_and(|contracts| contracts.contains(contract))
    }

    fn is_shared_evm_token(&self, token_id: &str) -> bool {
        self.evm_seed_token_ids
            .contains(normalized_evm_token_slice(token_id))
    }
}

impl ShardAnchors {
    #[allow(clippy::too_many_arguments)] // Fields are moved directly into a bounded metadata record.
    fn insert(
        &mut self,
        chain: String,
        contract_address: String,
        token_id: String,
        canonical_json: String,
        source_order: SourceOrder,
        options: &LoadOptions,
        selection: Option<&MetadataQuerySelection<'_>>,
    ) {
        let is_seed_contract = selection
            .is_some_and(|selection| selection.is_seed_contract(&chain, &contract_address));
        let anchors = self
            .by_contract
            .entry((chain.clone(), contract_address))
            .or_default();
        if options.metadata_anchors.is_none() && selection.is_none() {
            anchors.push(MetadataRecord {
                token_id,
                canonical_json,
                source_order,
            });
            return;
        }
        let is_evm = options.evm_chains.contains(&chain);
        if options.metadata_anchors.is_none()
            && let Some(selection) = selection
            && !is_seed_contract
        {
            let shared = is_evm && selection.is_shared_evm_token(&token_id);
            if !shared {
                let prior_non_shared = anchors
                    .iter()
                    .position(|record| !is_evm || !selection.is_shared_evm_token(&record.token_id));
                if let Some(position) = prior_non_shared {
                    if compare_token_ids_desc(&token_id, &anchors[position].token_id, is_evm)
                        == std::cmp::Ordering::Less
                    {
                        anchors[position] = MetadataRecord {
                            token_id,
                            canonical_json,
                            source_order,
                        };
                        anchors.sort_unstable_by(|left, right| {
                            compare_token_ids_desc(&left.token_id, &right.token_id, is_evm)
                                .then_with(|| left.source_order.cmp(&right.source_order))
                        });
                    }
                    return;
                }
            }
        }
        // Same token id: keep first valid in source order.
        if anchors.iter().any(|record| record.token_id == token_id) {
            return;
        }
        let insert_at = anchors
            .binary_search_by(|record| compare_token_ids_desc(&record.token_id, &token_id, is_evm))
            .unwrap_or_else(|position| position);
        if let Some(limit) = options.metadata_anchors
            && insert_at >= limit
            && anchors.len() >= limit
        {
            return;
        }
        anchors.insert(
            insert_at,
            MetadataRecord {
                token_id,
                canonical_json,
                source_order,
            },
        );
        if let Some(limit) = options.metadata_anchors
            && anchors.len() > limit
        {
            anchors.pop();
        }
    }

    fn merge_ordered(
        &mut self,
        other: Self,
        options: &LoadOptions,
        selection: Option<&MetadataQuerySelection<'_>>,
    ) {
        for ((chain, contract_address), records) in other.by_contract {
            for record in records {
                self.insert(
                    chain.clone(),
                    contract_address.clone(),
                    record.token_id,
                    record.canonical_json,
                    record.source_order,
                    options,
                    selection,
                );
            }
        }
    }

    fn prune_to_alignment_anchors(
        &mut self,
        options: &LoadOptions,
        selection: &MetadataQuerySelection<'_>,
    ) {
        for ((chain, contract), anchors) in &mut self.by_contract {
            if options.metadata_anchors.is_some()
                || selection.is_seed_contract(chain, contract)
                || !options.evm_chains.contains(chain)
                || anchors.len() <= 1
            {
                continue;
            }
            let mut unresolved = vec![true; selection.evm_seed_token_sets.len()];
            let mut retained = Vec::with_capacity(unresolved.len().saturating_add(1));
            for (position, record) in anchors.drain(..).enumerate() {
                let token = normalized_evm_token_slice(&record.token_id);
                let mut required = position == 0;
                for (seed_index, seed_tokens) in selection.evm_seed_token_sets.iter().enumerate() {
                    if unresolved[seed_index] && seed_tokens.contains(token) {
                        unresolved[seed_index] = false;
                        required = true;
                    }
                }
                if required {
                    retained.push(record);
                }
                if unresolved.iter().all(|pending| !pending) {
                    break;
                }
            }
            *anchors = retained;
        }
    }
}

/// Collected pass-2 anchors prior to store ingestion.
#[derive(Default)]
pub struct CollectedPass2Anchors {
    by_contract: AHashMap<(String, String), Vec<MetadataRecord>>,
}

/// Scan + merge pass-2 metadata without touching the resident store.
pub fn collect_pass2_anchors(
    inputs: &[ValidatedInput],
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
    selection: Option<&MetadataQuerySelection<'_>>,
) -> Result<CollectedPass2Anchors, Analysis2Error> {
    let shard_results: Vec<Result<ShardAnchors, Analysis2Error>> = inputs
        .par_iter()
        .map(|input| scan_file_pass2(input, options, progress, selection))
        .collect();

    // Merge in an ordered parallel tree. This preserves first-source-row
    // semantics without serially replaying every intermediate file shard.
    let mut shard = merge_anchor_shards_ordered(shard_results, options, selection)?;
    if let Some(selection) = selection {
        shard.prune_to_alignment_anchors(options, selection);
    }
    Ok(CollectedPass2Anchors {
        by_contract: shard.by_contract,
    })
}

/// Ingest previously collected pass-2 anchors into the resident store.
pub fn apply_pass2_anchors(
    store: &mut ResidentStore,
    anchors: CollectedPass2Anchors,
) -> Result<(), Analysis2Error> {
    for ((chain, contract_address), records) in anchors.by_contract {
        store.ingest_metadata_records(&chain, &contract_address, records)?;
    }
    Ok(())
}

fn scan_file_pass2(
    input: &ValidatedInput,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
    selection: Option<&MetadataQuerySelection<'_>>,
) -> Result<ShardAnchors, Analysis2Error> {
    let mut row_start = 0_u64;
    let mut row_groups = Vec::with_capacity(input.row_group_count);
    for row_group in 0..input.row_group_count {
        row_groups.push((row_group, row_start));
        let rows = input
            .metadata
            .metadata()
            .row_group(row_group)
            .num_rows()
            .max(0) as u64;
        row_start = row_start.saturating_add(rows);
    }

    // Parse independent row groups in parallel, then merge in row-group order so
    // duplicate token ids still keep the first valid source row.
    let row_group_results: Vec<Result<ShardAnchors, Analysis2Error>> = row_groups
        .par_iter()
        .map(|&(row_group, row_start)| {
            scan_row_group_pass2(input, row_group, row_start, options, progress, selection)
        })
        .collect();
    merge_anchor_shards_ordered(row_group_results, options, selection)
}

fn merge_anchor_shards_ordered(
    mut shards: Vec<Result<ShardAnchors, Analysis2Error>>,
    options: &LoadOptions,
    selection: Option<&MetadataQuerySelection<'_>>,
) -> Result<ShardAnchors, Analysis2Error> {
    match shards.len() {
        0 => Ok(ShardAnchors::default()),
        1 => shards.pop().expect("one anchor shard is present"),
        _ => {
            let right = shards.split_off(shards.len() / 2);
            let (left, right) = rayon::join(
                || merge_anchor_shards_ordered(shards, options, selection),
                || merge_anchor_shards_ordered(right, options, selection),
            );
            let mut left = left?;
            left.merge_ordered(right?, options, selection);
            Ok(left)
        }
    }
}

fn scan_row_group_pass2(
    input: &ValidatedInput,
    row_group: usize,
    row_start: u64,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
    selection: Option<&MetadataQuerySelection<'_>>,
) -> Result<ShardAnchors, Analysis2Error> {
    progress.check_cancelled()?;
    let file = File::open(&input.path)
        .map_err(|error| Analysis2Error::parquet(format!("{}: {error}", input.path.display())))?;
    let mask = ProjectionMask::roots(
        input.metadata.metadata().file_metadata().schema_descr(),
        input.pass2_projection.iter().copied(),
    );
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, input.metadata.clone())
        .with_projection(mask)
        .with_row_groups(vec![row_group])
        .with_batch_size(8 * 1024)
        .build()
        .map_err(|error| Analysis2Error::parquet(format!("{}: {error}", input.path.display())))?;
    let mut row_offset = 0_u64;
    let mut shard = ShardAnchors::default();
    for batch in reader {
        let batch = batch.map_err(|error| {
            Analysis2Error::parquet(format!("{}: {error}", input.path.display()))
        })?;
        let columns = ProjectedUtf8Columns::new(&batch, &input.path, &PASS2_COLUMNS)?;
        for row_index in 0..batch.num_rows() {
            let chain = normalize_chain(columns.value_at(0, row_index));
            let contract_address = columns.value_at(1, row_index).trim().to_owned();
            let token_id = columns.value_at(2, row_index).trim().to_owned();
            let metadata_raw = columns.value_at(3, row_index).trim();
            let source_order = SourceOrder {
                file_ordinal: input.file_ordinal,
                file_row_number: row_start + row_offset,
            };
            row_offset += 1;
            if !options.allowed_chains.is_empty() && !options.allowed_chains.contains(&chain) {
                continue;
            }
            if chain.is_empty() || contract_address.is_empty() || token_id.is_empty() {
                continue;
            }
            // Cheap reject before full JSON parse+canonicalize.
            if metadata_raw.is_empty()
                || metadata_raw == "{}"
                || !matches!(metadata_raw.as_bytes().first(), Some(b'{') | Some(b'['))
            {
                continue;
            }
            let Some(canonical_json) = validated_metadata(metadata_raw) else {
                continue;
            };
            shard.insert(
                chain,
                contract_address,
                token_id,
                canonical_json,
                source_order,
                options,
                selection,
            );
        }
        progress.add_completed(batch.num_rows() as u64);
    }
    Ok(shard)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(shard: &mut ShardAnchors, token: &str, selection: &MetadataQuerySelection<'_>) {
        let options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], None);
        shard.insert(
            "ethereum".to_owned(),
            "0xcandidate".to_owned(),
            token.to_owned(),
            format!(r#"{{"token":"{token}"}}"#),
            SourceOrder {
                file_ordinal: 0,
                file_row_number: token.parse().unwrap(),
            },
            &options,
            Some(selection),
        );
    }

    #[test]
    fn seed_selection_keeps_shared_tokens_and_only_the_largest_fallback() {
        let shared = "2";
        let selection = MetadataQuerySelection {
            seed_contracts: AHashMap::new(),
            evm_seed_token_ids: AHashSet::from([shared]),
            evm_seed_token_sets: vec![AHashSet::from([shared])],
        };
        let mut shard = ShardAnchors::default();
        for token in ["1", "2", "3", "4"] {
            record(&mut shard, token, &selection);
        }
        let anchors = &shard.by_contract[&("ethereum".to_owned(), "0xcandidate".to_owned())];
        assert_eq!(
            anchors
                .iter()
                .map(|record| record.token_id.as_str())
                .collect::<Vec<_>>(),
            ["4", "2"]
        );
    }

    #[test]
    fn ordered_merge_discards_row_group_local_fallbacks() {
        let shared = "2";
        let selection = MetadataQuerySelection {
            seed_contracts: AHashMap::new(),
            evm_seed_token_ids: AHashSet::from([shared]),
            evm_seed_token_sets: vec![AHashSet::from([shared])],
        };
        let options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], None);
        let mut left = ShardAnchors::default();
        record(&mut left, "5", &selection);
        let mut right = ShardAnchors::default();
        record(&mut right, "2", &selection);
        record(&mut right, "7", &selection);
        left.merge_ordered(right, &options, Some(&selection));
        let anchors = &left.by_contract[&("ethereum".to_owned(), "0xcandidate".to_owned())];
        assert_eq!(
            anchors
                .iter()
                .map(|record| record.token_id.as_str())
                .collect::<Vec<_>>(),
            ["7", "2"]
        );
    }

    #[test]
    fn final_prune_keeps_only_largest_shared_token_per_seed() {
        let selection = MetadataQuerySelection {
            seed_contracts: AHashMap::new(),
            evm_seed_token_ids: AHashSet::from(["2", "3", "4", "5"]),
            evm_seed_token_sets: vec![AHashSet::from(["2", "4"]), AHashSet::from(["3", "5"])],
        };
        let options = LoadOptions::new(["ethereum".to_owned()], ["ethereum".to_owned()], None);
        let mut shard = ShardAnchors::default();
        for token in ["1", "2", "3", "4", "5", "6"] {
            record(&mut shard, token, &selection);
        }
        shard.prune_to_alignment_anchors(&options, &selection);
        let anchors = &shard.by_contract[&("ethereum".to_owned(), "0xcandidate".to_owned())];
        assert_eq!(
            anchors
                .iter()
                .map(|record| record.token_id.as_str())
                .collect::<Vec<_>>(),
            ["6", "5", "4"]
        );
    }
}
