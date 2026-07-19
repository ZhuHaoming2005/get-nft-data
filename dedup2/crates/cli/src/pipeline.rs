use crate::progress::ProgressReporter;
use crate::report::{PhaseTiming, ReportRequest, StageTiming, write_reports};
use dedup_core::{
    DedupError, LoadOptions, ProgressObserver, SummaryAccumulator, load_entities_with_options,
    run_metadata, run_name, run_uri,
};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub inputs: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub metadata_anchors: usize,
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
    let load_options =
        LoadOptions::new(allowed, evm_names.iter().cloned(), config.metadata_anchors);
    let stage_started = Instant::now();
    let store = load_entities_with_options(&config.inputs, &load_options, progress)?;
    let mut stage_timings = vec![StageTiming {
        stage: "load",
        elapsed_secs: stage_started.elapsed().as_secs_f64(),
    }];

    let mut acc = SummaryAccumulator::default();
    let name_threshold = config.name_threshold / 100.0;
    if config.run_name {
        let stage_started = Instant::now();
        run_name(&store, name_threshold, &mut acc, progress)?;
        stage_timings.push(StageTiming {
            stage: "name",
            elapsed_secs: stage_started.elapsed().as_secs_f64(),
        });
    }
    if config.run_uri {
        let stage_started = Instant::now();
        run_uri(&store, &mut acc, progress)?;
        stage_timings.push(StageTiming {
            stage: "uri",
            elapsed_secs: stage_started.elapsed().as_secs_f64(),
        });
    }
    let mut metadata_stats = None;
    if config.run_metadata {
        let stage_started = Instant::now();
        let evm: std::collections::HashSet<String> = config
            .evm_chains
            .iter()
            .map(|c| c.trim().to_ascii_lowercase())
            .collect();
        let result = run_metadata(
            &store,
            &evm,
            config.metadata_anchors,
            config.metadata_threshold,
            &mut acc,
            progress,
        )?;
        metadata_stats = Some(result.stats);
        stage_timings.push(StageTiming {
            stage: "metadata",
            elapsed_secs: stage_started.elapsed().as_secs_f64(),
        });
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
            metadata_direct: metadata_stats,
            stage_timings,
            phase_timings,
            elapsed: started.elapsed(),
        },
    )
    .map_err(|error| DedupError::Message(error.to_string()))?;
    progress.add_completed(3);
    Ok(())
}
