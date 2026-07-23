//! Pass 2: metadata_json projection into descending anchors.

use ahash::AHashMap;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::fs::File;

use crate::Analysis2Error;
use crate::entity::{MetadataRecord, ResidentStore, SourceOrder, compare_token_ids_desc};
use crate::parquet::LoadOptions;
use crate::parquet::metadata::validated_metadata;
use crate::parquet::pass1::{ProjectedUtf8Columns, normalize_chain};
use crate::parquet::validate::{PASS2_COLUMNS, ValidatedInput};
use crate::progress::ProgressObserver;

/// Per-file bounded anchors: at most `k` records per contract (O(k × contracts)).
#[derive(Default)]
struct ShardAnchors {
    by_contract: AHashMap<(String, String), Vec<MetadataRecord>>,
}

impl ShardAnchors {
    fn insert(
        &mut self,
        chain: String,
        contract_address: String,
        token_id: String,
        json: String,
        canonical_json: String,
        source_order: SourceOrder,
        options: &LoadOptions,
    ) {
        if options.metadata_anchors == 0 {
            return;
        }
        let is_evm = options.evm_chains.contains(&chain);
        let anchors = self
            .by_contract
            .entry((chain, contract_address))
            .or_default();
        // Same token id: keep first valid in source order.
        if anchors.iter().any(|record| record.token_id == token_id) {
            return;
        }
        let insert_at = anchors
            .binary_search_by(|record| compare_token_ids_desc(&record.token_id, &token_id, is_evm))
            .unwrap_or_else(|position| position);
        if insert_at >= options.metadata_anchors && anchors.len() >= options.metadata_anchors {
            return;
        }
        anchors.insert(
            insert_at,
            MetadataRecord {
                token_id,
                json,
                canonical_json,
                source_order,
            },
        );
        if anchors.len() > options.metadata_anchors {
            anchors.pop();
        }
    }

    fn merge_ordered(&mut self, other: Self, options: &LoadOptions) {
        for ((chain, contract_address), records) in other.by_contract {
            if options.metadata_anchors == 0 {
                continue;
            }
            let is_evm = options.evm_chains.contains(&chain);
            let anchors = self
                .by_contract
                .entry((chain, contract_address))
                .or_default();
            for record in records {
                // Left shards are earlier in source order, so an existing token
                // id wins exactly as in repeated ordered insertion.
                if anchors
                    .iter()
                    .any(|existing| existing.token_id == record.token_id)
                {
                    continue;
                }
                let insert_at = anchors
                    .binary_search_by(|existing| {
                        compare_token_ids_desc(&existing.token_id, &record.token_id, is_evm)
                    })
                    .unwrap_or_else(|position| position);
                if insert_at >= options.metadata_anchors
                    && anchors.len() >= options.metadata_anchors
                {
                    continue;
                }
                anchors.insert(insert_at, record);
                if anchors.len() > options.metadata_anchors {
                    anchors.pop();
                }
            }
        }
    }
}

pub fn scan_pass2(
    inputs: &[ValidatedInput],
    store: &mut ResidentStore,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
) -> Result<(), Analysis2Error> {
    let shard_results: Vec<Result<ShardAnchors, Analysis2Error>> = inputs
        .par_iter()
        .map(|input| scan_file_pass2(input, options, progress))
        .collect();

    // Merge in an ordered parallel tree, then apply the bounded final anchor
    // set once. This preserves first-source-row semantics without serially
    // replaying every intermediate file shard into the resident store.
    let shard = merge_anchor_shards_ordered(shard_results, options)?;
    for ((chain, contract_address), records) in shard.by_contract {
        for record in records {
            store.ingest_metadata_anchor(
                &chain,
                &contract_address,
                &record.token_id,
                record.json,
                record.canonical_json,
                record.source_order,
            )?;
        }
    }
    Ok(())
}

fn scan_file_pass2(
    input: &ValidatedInput,
    options: &LoadOptions,
    progress: &dyn ProgressObserver,
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
            scan_row_group_pass2(input, row_group, row_start, options, progress)
        })
        .collect();
    merge_anchor_shards_ordered(row_group_results, options)
}

fn merge_anchor_shards_ordered(
    mut shards: Vec<Result<ShardAnchors, Analysis2Error>>,
    options: &LoadOptions,
) -> Result<ShardAnchors, Analysis2Error> {
    match shards.len() {
        0 => Ok(ShardAnchors::default()),
        1 => shards.pop().expect("one anchor shard is present"),
        _ => {
            let right = shards.split_off(shards.len() / 2);
            let (left, right) = rayon::join(
                || merge_anchor_shards_ordered(shards, options),
                || merge_anchor_shards_ordered(right, options),
            );
            let mut left = left?;
            left.merge_ordered(right?, options);
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
            let metadata_json = columns.value_at(3, row_index).trim().to_owned();
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
            let Some(canonical_json) = validated_metadata(&metadata_json) else {
                continue;
            };
            shard.insert(
                chain,
                contract_address,
                token_id,
                metadata_json,
                canonical_json,
                source_order,
                options,
            );
        }
        progress.add_completed(batch.num_rows() as u64);
    }
    Ok(shard)
}
