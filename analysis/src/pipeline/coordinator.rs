use crate::config::RunConfig;
use crate::dedup::{
    merge_relations, query_metadata_bm25_shard_with_plan_into,
    query_metadata_exact_shard_with_plan_into, query_name_shard_with_plan_into,
    query_uri_shard_with_plan_into,
};
use crate::error::{AnalysisError, Result};
use crate::progress::Progress;
use crate::resident::{
    MetadataIndex, NameIndex, PreparedMetadataPlan, PreparedNamePlan, PreparedUriPlan,
    ResidentBaseStore, SeedRawPlan, UriIndex,
};
use crate::seed::SeedManifest;
use rayon::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

const CANDIDATE_BATCH_CAPACITY: usize = 32;

pub struct DedupOutput {
    pub store: ResidentBaseStore,
    pub failed_seeds: std::collections::BTreeSet<crate::model::SeedId>,
}

pub fn execute_dedup(
    mut store: ResidentBaseStore,
    manifest: &SeedManifest,
    config: &RunConfig,
    executor: &crate::pipeline::CpuExecutor,
    progress: &Progress,
    finalized_candidates: Sender<crate::pipeline::CandidateRelationsEvent>,
) -> Result<DedupOutput> {
    let mut failed_seeds = std::collections::BTreeSet::new();
    let raw_plan = SeedRawPlan::build(&store, manifest)?;
    let uri_identities = store
        .uri_identity
        .as_ref()
        .ok_or_else(|| AnalysisError::State("URI identity stage unavailable".into()))?;
    let uri_features = store
        .uri_features
        .as_ref()
        .ok_or_else(|| AnalysisError::State("URI feature stage unavailable".into()))?;
    let token_uris = raw_plan
        .seeds
        .iter()
        .flat_map(|seed| seed.token_uri_values.iter().copied())
        .collect::<Vec<_>>();
    let image_uris = raw_plan
        .seeds
        .iter()
        .flat_map(|seed| seed.image_uri_values.iter().copied())
        .collect::<Vec<_>>();
    let seed_names = raw_plan
        .seeds
        .iter()
        .filter_map(|seed| seed.name_value)
        .collect::<Vec<_>>();
    let mut uri_index_parts = executor.install_on_all(|lane, lane_count| {
        UriIndex::build_partition(
            uri_identities,
            uri_features,
            &token_uris,
            &image_uris,
            config.index_shards,
            lane,
            lane_count,
        )
    });
    let mut uri_index = uri_index_parts
        .pop()
        .expect("CpuExecutor always has at least one NUMA lane");
    for partial in uri_index_parts {
        uri_index.merge(partial);
    }
    uri_index.finalize();
    progress.add_postings(crate::model::Dimension::TokenUri, uri_index.posting_count());
    let prepared_uri_plan = PreparedUriPlan::build(&raw_plan, &uri_index, config.index_shards);
    let uri_work = prepared_uri_plan
        .queries
        .iter()
        .enumerate()
        .flat_map(|(seed, query)| {
            (0..query.shards.len()).map(move |prepared_shard| (seed, prepared_shard))
        })
        .collect::<Vec<_>>();
    let tail_start = if config.next_dimension_overlap {
        uri_work.len().saturating_mul(7) / 8
    } else {
        uri_work.len()
    };
    // Dimension-level shard seal: every URI shard query below (main pass and
    // overlapped tail) registers a seed-batch guard so a panicking or
    // otherwise failed query soft-fails into `failed_seed_bitmap` instead of
    // corrupting shared state; the seal is checked once both passes finish.
    let uri_tracker = Arc::new(crate::pipeline::ShardWorkTracker::default());
    let mut uri_relations = executor
        .install_on_all(|lane, lane_count| {
            (0..tail_start)
                .into_par_iter()
                .filter(|&work| {
                    let (seed, prepared_shard) = uri_work[work];
                    prepared_uri_plan.queries[seed].shards[prepared_shard].shard % lane_count
                        == lane
                })
                .fold(
                    || crate::dedup::RelationAccumulator::new(&store.contracts),
                    |mut accumulator, work| {
                        let (seed_index, prepared_shard) = uri_work[work];
                        let seed = &raw_plan.seeds[seed_index];
                        query_uri_shard_tracked(
                            &store,
                            uri_identities,
                            uri_features,
                            &uri_index,
                            seed,
                            &prepared_uri_plan.queries[seed_index].shards[prepared_shard],
                            config.index_shards,
                            &mut accumulator,
                            &uri_tracker,
                        );
                        progress.add_shard_batch();
                        accumulator
                    },
                )
                .reduce(
                    || crate::dedup::RelationAccumulator::new(&store.contracts),
                    crate::dedup::RelationAccumulator::merge,
                )
        })
        .into_iter()
        .reduce(crate::dedup::RelationAccumulator::merge)
        .unwrap_or_else(|| crate::dedup::RelationAccumulator::new(&store.contracts));
    let mut overlapped_name_index = None;
    if tail_start < uri_work.len() {
        let name_features = store
            .name_features
            .as_ref()
            .ok_or_else(|| AnalysisError::State("Name feature stage unavailable".into()))?;
        let (tail_relations, name_index) = executor.install_on_lane(0, || {
            rayon::join(
                || {
                    executor
                        .install_on_all(|lane, lane_count| {
                            (tail_start..uri_work.len())
                                .into_par_iter()
                                .filter(|&work| {
                                    let (seed, prepared_shard) = uri_work[work];
                                    prepared_uri_plan.queries[seed].shards[prepared_shard].shard
                                        % lane_count
                                        == lane
                                })
                                .fold(
                                    || crate::dedup::RelationAccumulator::new(&store.contracts),
                                    |mut accumulator, work| {
                                        let (seed_index, prepared_shard) = uri_work[work];
                                        let seed = &raw_plan.seeds[seed_index];
                                        query_uri_shard_tracked(
                                            &store,
                                            uri_identities,
                                            uri_features,
                                            &uri_index,
                                            seed,
                                            &prepared_uri_plan.queries[seed_index].shards
                                                [prepared_shard],
                                            config.index_shards,
                                            &mut accumulator,
                                            &uri_tracker,
                                        );
                                        progress.add_shard_batch();
                                        accumulator
                                    },
                                )
                                .reduce(
                                    || crate::dedup::RelationAccumulator::new(&store.contracts),
                                    crate::dedup::RelationAccumulator::merge,
                                )
                        })
                        .into_iter()
                        .reduce(crate::dedup::RelationAccumulator::merge)
                        .unwrap_or_else(|| crate::dedup::RelationAccumulator::new(&store.contracts))
                },
                || NameIndex::build_numa(name_features, &seed_names, config.index_shards, executor),
            )
        });
        uri_relations = uri_relations.merge(tail_relations);
        overlapped_name_index = Some(name_index);
    }
    uri_tracker.close_producer();
    let uri_seal = uri_tracker.try_seal().ok_or_else(|| {
        AnalysisError::State("URI dimension tracker failed to reach quiescence".into())
    })?;
    failed_seeds.extend(uri_seal.failed_seed_ids());
    let mut uri_relations = uri_relations.finish();
    progress.add_incomplete_seeds(mark_incomplete_seeds(&mut uri_relations, uri_seal));
    drop(uri_work);
    drop(prepared_uri_plan);
    executor.broadcast(crate::dedup::release_uri_scratch);
    drop(uri_index);
    drop(store.take_uri_stage());

    let name_features = store
        .name_features
        .as_ref()
        .ok_or_else(|| AnalysisError::State("Name feature stage unavailable".into()))?;
    let name_index = overlapped_name_index.unwrap_or_else(|| {
        NameIndex::build_numa(name_features, &seed_names, config.index_shards, executor)
    });
    progress.add_postings(crate::model::Dimension::Name, name_index.posting_count());
    // Safe rarity-sorted candidate prefixes are computed once per seed here,
    // then reused unchanged across every one of the 128 owner-shard queries
    // below instead of unioning all of a seed's occurrence tokens per query.
    let prepared_name_plan = PreparedNamePlan::build(&raw_plan, &name_index, config.name_threshold);
    // Dimension-level shard seal for Name, mirroring the URI tracker above:
    // one guard per (seed, shard) query, sealed once the whole fold/reduce
    // returns. failed_seed_bitmap soft-fails affected seeds instead of
    // aborting the run.
    let name_tracker = Arc::new(crate::pipeline::ShardWorkTracker::default());
    let mut name_relations = executor
        .install_on_all(|lane, lane_count| {
            (0..raw_plan.seeds.len() * config.index_shards)
                .into_par_iter()
                .filter(|work| work % config.index_shards % lane_count == lane)
                .fold(
                    || crate::dedup::RelationAccumulator::new(&store.contracts),
                    |mut accumulator, work| {
                        let seed_index = work / config.index_shards;
                        let seed = &raw_plan.seeds[seed_index];
                        let shard = work % config.index_shards;
                        let guard = name_tracker.register_seed_batch(seed.seed_id);
                        let outcome =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                query_name_shard_with_plan_into(
                                    &store,
                                    name_features,
                                    &name_index,
                                    seed,
                                    shard,
                                    config.name_threshold,
                                    &prepared_name_plan.queries[seed_index],
                                    &mut accumulator,
                                );
                            }));
                        match outcome {
                            Ok(()) => guard.succeed(),
                            Err(_) => {
                                // Leave the guard unsucceeded: dropping it marks
                                // this seed failed in the tracker's bitmap.
                            }
                        }
                        progress.add_shard_batch();
                        accumulator
                    },
                )
                .reduce(
                    || crate::dedup::RelationAccumulator::new(&store.contracts),
                    crate::dedup::RelationAccumulator::merge,
                )
                .finish()
        })
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    name_relations = merge_relations(name_relations);
    name_tracker.close_producer();
    let name_seal = name_tracker.try_seal().ok_or_else(|| {
        AnalysisError::State("Name dimension tracker failed to reach quiescence".into())
    })?;
    failed_seeds.extend(name_seal.failed_seed_ids());
    progress.add_incomplete_seeds(mark_incomplete_seeds(&mut name_relations, name_seal));
    drop(prepared_name_plan);
    executor.broadcast(crate::dedup::release_name_scratch);
    drop(name_index);
    drop(store.take_name_stage());
    let mut base_relations = merge_relations(uri_relations.into_iter().chain(name_relations));
    if !base_relations.is_empty() {
        sort_by_candidate(&mut base_relations);
        stream_relation_refs(
            &finalized_candidates,
            &base_relations,
            crate::pipeline::CandidateRelationsEvent::Prefetch,
            CANDIDATE_BATCH_CAPACITY,
        )?;
    }

    let seed_documents = raw_plan
        .seeds
        .iter()
        .flat_map(|seed| seed.metadata_documents.iter().copied())
        .collect::<Vec<_>>();
    let metadata_index = MetadataIndex::build_numa(
        store
            .metadata_features
            .as_ref()
            .expect("Metadata feature stage was checked before index build"),
        &seed_documents,
        config.index_shards,
        executor,
    );
    let prepared_metadata_plan = Arc::new(PreparedMetadataPlan::build(
        &raw_plan,
        store
            .metadata_features
            .as_ref()
            .expect("Metadata feature stage is resident during query preparation"),
        &metadata_index,
    ));
    let metadata_index = Arc::new(metadata_index);
    progress.add_postings(
        crate::model::Dimension::Metadata,
        metadata_index.posting_count(),
    );
    let mut base_by_metadata_shard = (0..config.index_shards)
        .map(|_| Vec::new())
        .collect::<Vec<_>>();
    let mut no_metadata_owner = Vec::new();
    for relation in base_relations {
        let contract_id = crate::model::ContractId(relation.candidate_id.0);
        match store.contracts.contracts[contract_id.index()].metadata_owner_shard {
            Some(owner) => base_by_metadata_shard[usize::from(owner)].push(relation),
            None => no_metadata_owner.push(relation),
        }
    }
    let mut exact_tasks = Vec::new();
    let mut bm25_tasks = Vec::new();
    let mut pending_batches = vec![0_usize; config.index_shards];
    for (shard, pending) in pending_batches.iter_mut().enumerate() {
        for start in (0..raw_plan.seeds.len()).step_by(config.seed_batch_size) {
            let end = (start + config.seed_batch_size).min(raw_plan.seeds.len());
            exact_tasks.push((shard, start, end));
            bm25_tasks.push((shard, start, end));
            *pending += 1;
        }
    }
    let total_batches = exact_tasks.len();
    let store = Arc::new(store);
    let raw_plan = Arc::new(raw_plan);
    let mut exact_by_metadata_shard = (0..config.index_shards)
        .map(|_| crate::dedup::RelationAccumulator::new(&store.contracts))
        .collect::<Vec<_>>();
    let (completion_tx, completion_rx) = std::sync::mpsc::channel();
    let spawn_exact_batch = |(shard, start, end): (usize, usize, usize)| {
        let completion_tx = completion_tx.clone();
        let raw_plan = raw_plan.clone();
        let store = store.clone();
        let metadata_index = metadata_index.clone();
        let prepared_metadata_plan = prepared_metadata_plan.clone();
        let index_shards = config.index_shards;
        executor.submit_kind_on_lane(
            crate::pipeline::CpuTaskKind::Dedup,
            shard % executor.numa_pool_count(),
            move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let metadata_features = store
                        .metadata_features
                        .as_ref()
                        .expect("Metadata feature stage is resident during exact query");
                    let mut accumulator = crate::dedup::RelationAccumulator::new(&store.contracts);
                    for seed_index in start..end {
                        let seed = &raw_plan.seeds[seed_index];
                        query_metadata_exact_shard_with_plan_into(
                            &store,
                            metadata_features,
                            &metadata_index,
                            seed,
                            shard,
                            index_shards,
                            &prepared_metadata_plan.queries[seed_index],
                            &mut accumulator,
                        );
                    }
                    Ok(accumulator.into_relations_unfinished())
                }))
                .unwrap_or_else(|_| {
                    Err(AnalysisError::State(format!(
                        "metadata exact owner shard {shard} panicked"
                    )))
                });
                let _ = completion_tx.send((shard, start, end, result));
            },
        )
    };
    let mut tasks = exact_tasks.into_iter();
    let admission = config
        .cpu_queue_capacity
        .min(executor.workers())
        .min(total_batches);
    for task in tasks.by_ref().take(admission) {
        let _ = spawn_exact_batch(task);
    }
    let mut exact_error = None;
    for _ in 0..total_batches {
        let (shard, _start, _end, result) = completion_rx
            .recv()
            .map_err(|_| AnalysisError::State("metadata exact completion channel closed".into()))?;
        progress.add_shard_batch();
        if let Some(task) = tasks.next() {
            let _ = spawn_exact_batch(task);
        }
        match result {
            Ok(relations) => exact_by_metadata_shard[shard].extend_relations(relations),
            Err(error) => {
                exact_error.get_or_insert(error);
            }
        }
    }
    if let Some(error) = exact_error {
        executor.broadcast(crate::dedup::release_metadata_scratch);
        return Err(error);
    }
    let mut exact_relations_by_metadata_shard = exact_by_metadata_shard
        .into_iter()
        .map(crate::dedup::RelationAccumulator::finish)
        .collect::<Vec<_>>();
    for exact_relations in &mut exact_relations_by_metadata_shard {
        if exact_relations.is_empty() {
            continue;
        }
        sort_by_candidate(exact_relations);
        stream_relation_refs(
            &finalized_candidates,
            exact_relations,
            crate::pipeline::CandidateRelationsEvent::Prefetch,
            CANDIDATE_BATCH_CAPACITY,
        )?;
    }
    let mut candidate_count = 0_u64;
    if !no_metadata_owner.is_empty() {
        let mut relations = no_metadata_owner;
        progress.add_incomplete_seeds(mark_relations_incomplete_for_failed(
            &mut relations,
            &failed_seeds,
        ));
        candidate_count = candidate_count.saturating_add(stream_relations(
            &finalized_candidates,
            relations,
            crate::pipeline::CandidateRelationsEvent::Frozen,
            CANDIDATE_BATCH_CAPACITY,
        )?);
    }
    let (completion_tx, completion_rx) = std::sync::mpsc::channel();
    let spawn_bm25_batch = |(shard, start, end): (usize, usize, usize)| {
        let completion_tx = completion_tx.clone();
        let raw_plan = raw_plan.clone();
        let store = store.clone();
        let metadata_index = metadata_index.clone();
        let prepared_metadata_plan = prepared_metadata_plan.clone();
        let index_shards = config.index_shards;
        let metadata_threshold = config.metadata_threshold;
        executor.submit_kind_on_lane(
            crate::pipeline::CpuTaskKind::Dedup,
            shard % executor.numa_pool_count(),
            move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let metadata_features = store
                        .metadata_features
                        .as_ref()
                        .expect("Metadata feature stage is resident during BM25 query");
                    let mut accumulator = crate::dedup::RelationAccumulator::new(&store.contracts);
                    for seed_index in start..end {
                        let seed = &raw_plan.seeds[seed_index];
                        query_metadata_bm25_shard_with_plan_into(
                            &store,
                            metadata_features,
                            &metadata_index,
                            seed,
                            shard,
                            index_shards,
                            metadata_threshold,
                            &prepared_metadata_plan.queries[seed_index],
                            &mut accumulator,
                        );
                    }
                    Ok(accumulator.into_relations_unfinished())
                }))
                .unwrap_or_else(|_| {
                    Err(AnalysisError::State(format!(
                        "metadata owner shard {shard} panicked"
                    )))
                });
                let _ = completion_tx.send((shard, start, end, result));
            },
        )
    };
    let mut metadata_tasks = bm25_tasks.into_iter();
    let admission = config
        .cpu_queue_capacity
        .min(executor.workers())
        .min(total_batches);
    for task in metadata_tasks.by_ref().take(admission) {
        let _ = spawn_bm25_batch(task);
    }
    let mut accumulated = (0..config.index_shards)
        .map(|_| crate::dedup::RelationAccumulator::new(&store.contracts))
        .collect::<Vec<_>>();
    let mut bm25_error = None;
    for _ in 0..total_batches {
        let (shard, _start, _end, result) = completion_rx
            .recv()
            .map_err(|_| AnalysisError::State("metadata shard completion channel closed".into()))?;
        progress.add_shard_batch();
        if let Some(task) = metadata_tasks.next() {
            let _ = spawn_bm25_batch(task);
        }
        match result {
            Ok(relations) => accumulated[shard].extend_relations(relations),
            Err(error) => {
                bm25_error.get_or_insert(error);
            }
        }
        pending_batches[shard] = pending_batches[shard]
            .checked_sub(1)
            .ok_or_else(|| AnalysisError::State("metadata shard completion underflow".into()))?;
        if pending_batches[shard] == 0 {
            progress.add_shard_seal();
            if bm25_error.is_some() {
                continue;
            }
            let bm25_relations = std::mem::replace(
                &mut accumulated[shard],
                crate::dedup::RelationAccumulator::new(&store.contracts),
            )
            .finish();
            let metadata_relations = merge_relations(
                std::mem::take(&mut exact_relations_by_metadata_shard[shard])
                    .into_iter()
                    .chain(bm25_relations),
            );
            let mut relations = merge_relations(
                std::mem::take(&mut base_by_metadata_shard[shard])
                    .into_iter()
                    .chain(metadata_relations),
            );
            progress.add_incomplete_seeds(mark_relations_incomplete_for_failed(
                &mut relations,
                &failed_seeds,
            ));
            candidate_count = candidate_count.saturating_add(stream_relations(
                &finalized_candidates,
                relations,
                crate::pipeline::CandidateRelationsEvent::Frozen,
                CANDIDATE_BATCH_CAPACITY,
            )?);
        }
    }
    if let Some(error) = bm25_error {
        executor.broadcast(crate::dedup::release_metadata_scratch);
        return Err(error);
    }
    drop(accumulated);
    drop(prepared_metadata_plan);
    executor.broadcast(crate::dedup::release_metadata_scratch);
    drop(metadata_index);
    drop(raw_plan);
    let mut store = Arc::try_unwrap(store).map_err(|_| {
        AnalysisError::State("metadata tasks still reference resident store".into())
    })?;
    drop(store.take_metadata_stage());
    progress.set_candidates(candidate_count);
    drop(finalized_candidates);
    Ok(DedupOutput {
        store,
        failed_seeds,
    })
}

/// Runs one URI shard query under a dimension-level [`ShardWorkTracker`]
/// guard. A panic inside the query is caught and soft-fails the owning seed
/// into the tracker's `failed_seed_bitmap` instead of unwinding the whole
/// dedup run; the accumulator otherwise keeps whatever partial state the
/// query already wrote before panicking.
#[allow(clippy::too_many_arguments)]
fn query_uri_shard_tracked(
    store: &ResidentBaseStore,
    identities: &crate::resident::UriNftIdentityStore,
    features: &crate::resident::UriFeatureStore,
    index: &crate::resident::UriIndex,
    seed: &crate::resident::SeedRawQuery,
    prepared: &crate::resident::PreparedUriShardQuery,
    shard_count: usize,
    output: &mut crate::dedup::RelationAccumulator<'_>,
    tracker: &Arc<crate::pipeline::ShardWorkTracker>,
) {
    let guard = tracker.register_seed_batch(seed.seed_id);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        query_uri_shard_with_plan_into(
            store,
            identities,
            features,
            index,
            seed,
            prepared,
            shard_count,
            output,
        );
    }));
    if outcome.is_ok() {
        guard.succeed();
    }
    // Leaving the guard unsucceeded on panic marks this seed failed; the
    // caller checks `try_seal().failed_seed_bitmap` once every shard query
    // for the dimension has completed.
}

/// Marks every relation whose seed fell in `seal`'s `failed_seed_bitmap` as
/// `incomplete` (soft-fail; see REWRITE_ARCHITECTURE §7.6/§8.4) instead of
/// aborting the run, and returns how many relations were touched so callers
/// can surface it through [`Progress::add_incomplete_seeds`].
fn mark_incomplete_seeds(
    relations: &mut [crate::model::SeedCandidateRelation],
    seal: crate::pipeline::DimensionShardSeal,
) -> u64 {
    let mut marked = 0_u64;
    for relation in relations.iter_mut() {
        if seal.seed_failed(relation.seed_id) && !relation.incomplete {
            relation.incomplete = true;
            marked += 1;
        }
    }
    marked
}

fn mark_relations_incomplete_for_failed(
    relations: &mut [crate::model::SeedCandidateRelation],
    failed_seeds: &std::collections::BTreeSet<crate::model::SeedId>,
) -> u64 {
    let mut marked = 0_u64;
    for relation in relations {
        if failed_seeds.contains(&relation.seed_id) && !relation.incomplete {
            relation.incomplete = true;
            marked = marked.saturating_add(1);
        }
    }
    marked
}

fn stream_relations(
    sender: &Sender<crate::pipeline::CandidateRelationsEvent>,
    mut relations: Vec<crate::model::SeedCandidateRelation>,
    wrap: fn(Vec<crate::model::SeedCandidateRelation>) -> crate::pipeline::CandidateRelationsEvent,
    candidate_capacity: usize,
) -> Result<u64> {
    relations.sort_by(|left, right| {
        (left.candidate_id, left.seed_id).cmp(&(right.candidate_id, right.seed_id))
    });
    let mut batch = Vec::new();
    let mut candidates = 0_usize;
    let mut total_candidates = 0_u64;
    let mut last_candidate = None;
    for relation in relations {
        let is_new = last_candidate != Some(relation.candidate_id);
        if is_new && candidates == candidate_capacity {
            sender
                .blocking_send(wrap(std::mem::take(&mut batch)))
                .map_err(|_| AnalysisError::State("candidate stream closed during dedup".into()))?;
            candidates = 0;
        }
        if is_new {
            candidates += 1;
            total_candidates += 1;
            last_candidate = Some(relation.candidate_id);
        }
        batch.push(relation);
    }
    if !batch.is_empty() {
        sender
            .blocking_send(wrap(batch))
            .map_err(|_| AnalysisError::State("candidate stream closed during dedup".into()))?;
    }
    Ok(total_candidates)
}

fn stream_relation_refs(
    sender: &Sender<crate::pipeline::CandidateRelationsEvent>,
    relations: &[crate::model::SeedCandidateRelation],
    wrap: fn(Vec<crate::model::SeedCandidateRelation>) -> crate::pipeline::CandidateRelationsEvent,
    candidate_capacity: usize,
) -> Result<()> {
    let mut batch = Vec::new();
    let mut candidates = 0_usize;
    let mut last_candidate = None;
    for relation in relations {
        let is_new = last_candidate != Some(relation.candidate_id);
        if is_new && candidates == candidate_capacity {
            sender
                .blocking_send(wrap(std::mem::take(&mut batch)))
                .map_err(|_| AnalysisError::State("candidate stream closed during dedup".into()))?;
            candidates = 0;
        }
        if is_new {
            candidates += 1;
            last_candidate = Some(relation.candidate_id);
        }
        batch.push(relation.clone());
    }
    if !batch.is_empty() {
        sender
            .blocking_send(wrap(batch))
            .map_err(|_| AnalysisError::State("candidate stream closed during dedup".into()))?;
    }
    Ok(())
}

fn sort_by_candidate(relations: &mut [crate::model::SeedCandidateRelation]) {
    relations.sort_by(|left, right| {
        (left.candidate_id, left.seed_id).cmp(&(right.candidate_id, right.seed_id))
    });
}
