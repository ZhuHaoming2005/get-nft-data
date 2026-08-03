use crate::progress::ProgressReporter;
use crate::report::{
    PhaseTiming, ReportPartition, ReportRequest, StageTiming, write_duplicate_pair_samples,
    write_partition_reports, write_reports,
};
use crate::sample_images::{
    DownloadOutcome, clear_published_metadata_image_samples, download_metadata_image_samples,
};
use dedup_core::{
    DedupError, Dimension, DuplicatePairSample, DuplicatePairSamples, LoadOptions,
    MetadataImagePairSample, ProgressObserver, SummaryAccumulator, load_entities_with_options,
    run_metadata, run_metadata_with_samples, run_name, run_name_with_samples, run_uri,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub inputs: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: Option<f64>,
    pub metadata_threshold: f64,
    pub metadata_anchors: Option<usize>,
    pub sample_pairs: usize,
    pub sample_candidate_limit: usize,
    pub threads: usize,
    pub run_name: bool,
    pub run_uri: bool,
    pub run_metadata: bool,
}

pub fn run(config: RunConfig, progress: &ProgressReporter) -> Result<(), DedupError> {
    let started = Instant::now();
    let allowed = config
        .chains
        .iter()
        .map(|c| c.trim().to_ascii_lowercase())
        .filter(|c| !c.is_empty())
        .collect::<Vec<_>>();
    let evm_names = config
        .evm_chains
        .iter()
        .map(|c| c.trim().to_ascii_lowercase())
        .filter(|c| !c.is_empty())
        .collect::<Vec<_>>();
    let mut load_options = LoadOptions::new(
        allowed,
        evm_names.iter().cloned(),
        if config.run_metadata {
            config.metadata_anchors
        } else {
            Some(0)
        },
    );
    load_options.load_names = config.run_name && config.name_threshold.is_some();
    load_options.load_token_uris = config.run_uri;
    let sample_metadata_images = config.run_metadata && config.sample_pairs != 0;
    load_options.load_image_uris = config.run_uri || sample_metadata_images;
    load_options.load_metadata = config.run_metadata;
    let stage_started = Instant::now();
    let mut store = load_entities_with_options(&config.inputs, &load_options, progress)?;
    let interned_strings = store.strings.len();
    let token_uri_postings = store.token_uri_postings.len();
    let image_uri_postings = store.image_uri_postings.len();
    let mut stage_timings = vec![StageTiming {
        stage: "load",
        elapsed_secs: stage_started.elapsed().as_secs_f64(),
    }];

    let mut acc = SummaryAccumulator::default();
    if let (true, Some(name_threshold)) = (config.run_name, config.name_threshold) {
        let stage_started = Instant::now();
        let samples = if config.sample_pairs == 0 {
            run_name(&store, name_threshold / 100.0, &mut acc, progress)?;
            DuplicatePairSamples::default()
        } else {
            run_name_with_samples(
                &store,
                name_threshold / 100.0,
                &mut acc,
                progress,
                config.sample_pairs,
            )?
        };
        stage_timings.push(StageTiming {
            stage: "name",
            elapsed_secs: stage_started.elapsed().as_secs_f64(),
        });
        write_completed_partition(
            &config.output_dir,
            &store,
            &acc,
            ReportPartition::Name,
            "name",
            progress,
        )?;
        if config.sample_pairs != 0 {
            write_duplicate_pair_samples(&config.output_dir, "name_duplicate_pairs.csv", &samples)
                .map_err(|error| DedupError::Message(error.to_string()))?;
        }
        acc.seal_dimension(Dimension::Name);
    }
    if config.run_uri {
        let stage_started = Instant::now();
        run_uri(&store, &mut acc, progress)?;
        stage_timings.push(StageTiming {
            stage: "uri",
            elapsed_secs: stage_started.elapsed().as_secs_f64(),
        });
        write_completed_partition(
            &config.output_dir,
            &store,
            &acc,
            ReportPartition::Uri,
            "uri",
            progress,
        )?;
        acc.seal_dimension(Dimension::TokenUri);
        acc.seal_dimension(Dimension::ImageUri);
    }
    if sample_metadata_images {
        store.release_completed_dimension_data_preserving_image_uris();
    } else {
        store.release_completed_dimension_data();
    }
    let mut metadata_stats = None;
    let mut sampling_error = None;
    if config.run_metadata {
        let stage_started = Instant::now();
        let evm: std::collections::HashSet<String> = config
            .evm_chains
            .iter()
            .map(|c| c.trim().to_ascii_lowercase())
            .collect();
        let result = if config.sample_pairs == 0 {
            run_metadata(
                &mut store,
                &evm,
                config.metadata_anchors,
                config.metadata_threshold,
                &mut acc,
                progress,
            )?
        } else {
            run_metadata_with_samples(
                &mut store,
                &evm,
                config.metadata_anchors,
                config.metadata_threshold,
                &mut acc,
                progress,
                config.sample_candidate_limit,
            )?
        };
        metadata_stats = Some(result.stats);
        stage_timings.push(StageTiming {
            stage: "metadata",
            elapsed_secs: stage_started.elapsed().as_secs_f64(),
        });
        if sample_metadata_images {
            let stage_started = Instant::now();
            clear_previous_metadata_samples(&config.output_dir)?;
            progress.set_stage("sample_images");
            progress.begin_phase("download", Some(result.image_samples.len() as u64));
            let download = download_metadata_image_samples(
                &config.output_dir,
                &result.image_samples,
                config.sample_pairs,
                progress,
            );
            match download {
                Ok(DownloadOutcome::Complete(downloaded)) => {
                    let final_pairs = retain_downloaded_pairs(result.samples, &downloaded);
                    write_duplicate_pair_samples(
                        &config.output_dir,
                        "metadata_duplicate_pairs.csv",
                        &final_pairs,
                    )
                    .map_err(|error| DedupError::Message(error.to_string()))?;
                }
                Ok(DownloadOutcome::Insufficient {
                    successful,
                    candidates,
                }) => {
                    sampling_error = Some(DedupError::Message(format!(
                        "requested {} complete Metadata media pairs, but only {successful} of {candidates} bounded image-qualified candidates downloaded successfully; raise --sample-candidate-limit (currently {}) to inspect more candidates",
                        config.sample_pairs, config.sample_candidate_limit
                    )));
                }
                Err(error) => {
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::Interrupted)
                    {
                        return Err(DedupError::Interrupted);
                    }
                    sampling_error = Some(DedupError::Message(format!(
                        "failed to build a complete Metadata media sample: {error}"
                    )));
                }
            }
            stage_timings.push(StageTiming {
                stage: "sample_images",
                elapsed_secs: stage_started.elapsed().as_secs_f64(),
            });
        }
    }

    let phase_timings = progress
        .phase_timings()
        .into_iter()
        .map(|timing| PhaseTiming {
            stage: timing.stage,
            phase: timing.phase,
            elapsed_secs: timing.elapsed.as_secs_f64(),
        })
        .collect();
    progress.set_stage("report");
    progress.begin_phase("write", Some(3));
    write_reports(
        &config.output_dir,
        ReportRequest {
            store: &store,
            accumulator: &acc,
            inputs: &config.inputs,
            chains: &config.chains,
            evm_chains: &config.evm_chains,
            name_threshold: config.name_threshold,
            metadata_threshold: config.metadata_threshold,
            metadata_anchors: config.metadata_anchors,
            sample_pairs: config.sample_pairs,
            sample_candidate_limit: (config.sample_pairs != 0)
                .then_some(config.sample_candidate_limit),
            threads: config.threads,
            interned_strings,
            token_uri_postings,
            image_uri_postings,
            metadata_direct: metadata_stats,
            stage_timings,
            phase_timings,
            elapsed: started.elapsed(),
        },
    )
    .map_err(|error| DedupError::Message(error.to_string()))?;
    progress.add_completed(3);
    if let Some(error) = sampling_error {
        Err(error)
    } else {
        Ok(())
    }
}

fn retain_downloaded_pairs(
    mut pairs: DuplicatePairSamples,
    downloaded: &[MetadataImagePairSample],
) -> DuplicatePairSamples {
    let retained = downloaded
        .iter()
        .map(|sample| {
            (
                sample.contract_a_chain.clone(),
                sample.contract_a_address.clone(),
                sample.contract_b_chain.clone(),
                sample.contract_b_address.clone(),
            )
        })
        .collect::<HashSet<_>>();
    let keep = |pair: &DuplicatePairSample| {
        retained.contains(&(
            pair.contract_a_chain.clone(),
            pair.contract_a_address.clone(),
            pair.contract_b_chain.clone(),
            pair.contract_b_address.clone(),
        ))
    };
    pairs.all_chains.retain(&keep);
    for scope in &mut pairs.intra_chain {
        scope.pairs.retain(&keep);
    }
    pairs.intra_chain.retain(|scope| !scope.pairs.is_empty());
    for scope in &mut pairs.chain_pairs {
        scope.pairs.retain(&keep);
    }
    pairs.chain_pairs.retain(|scope| !scope.pairs.is_empty());
    for scope in &mut pairs.cross_chain_summary {
        scope.pairs.retain(&keep);
    }
    pairs
        .cross_chain_summary
        .retain(|scope| !scope.pairs.is_empty());
    pairs
}

fn clear_previous_metadata_samples(output_dir: &std::path::Path) -> Result<(), DedupError> {
    clear_published_metadata_image_samples(output_dir)
        .map_err(|error| DedupError::Message(error.to_string()))?;
    for name in [
        "metadata_duplicate_pairs.csv",
        "metadata_duplicate_pairs_intra_chain.csv",
        "metadata_duplicate_pairs_chain_matrix.csv",
        "metadata_duplicate_pairs_cross_chain_summary.csv",
    ] {
        let path = output_dir.join(name);
        if path.exists() {
            std::fs::remove_file(path).map_err(|error| DedupError::Message(error.to_string()))?;
        }
    }
    Ok(())
}

fn write_completed_partition(
    output_dir: &std::path::Path,
    store: &dedup_core::EntityStore,
    accumulator: &SummaryAccumulator,
    partition: ReportPartition,
    stage: &str,
    progress: &ProgressReporter,
) -> Result<(), DedupError> {
    progress.set_stage("report");
    progress.begin_phase(stage, Some(2));
    write_partition_reports(output_dir, store, accumulator, partition)
        .map_err(|error| DedupError::Message(error.to_string()))?;
    progress.add_completed(2);
    Ok(())
}
