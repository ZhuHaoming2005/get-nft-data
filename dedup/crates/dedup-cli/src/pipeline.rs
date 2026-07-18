use dedup_engine::metadata::{
    BorrowedMetadataRecord, CanonicalMetadataValidator, ContractAnchors,
    ExhaustiveSharedTokenOracle, MetadataExecutionConfig, MetadataPrefilterExecutionConfig,
    MetadataPrefilterRequest, StratifiedSampler, TemplateGuard, audit_metadata_recall,
    build_template_fingerprints_with_progress,
    generate_metadata_candidates_for_request_with_progress,
    generate_metadata_candidates_with_execution_and_progress,
    run_metadata_verification_with_config_progress_and_executor,
    select_anchors_from_sorted_records,
};
use dedup_engine::name::{NameEngineConfig, run_name_with_progress_and_executor};
use dedup_engine::uri::{UriExecutionConfig, run_uri_mapped_with_config_progress_and_executor};
use dedup_index::{EntityBuildResult, EntityBuilder, EntityExecutionConfig, StringDictionary};
use dedup_model::{
    ChainId, ChunkExecutor, DedupError, Dimension, EntityArtifacts, EntityKind, ErrorContext,
    MetadataDocId, ProgressObserver, RunConfig, ScopeId, StageCounters,
};
use dedup_report::{BitmapHitSink, StatisticsRow};
use dedup_storage::{
    ArtifactManifest, ArtifactWriter, MappedContracts, MappedEntityObjects, MappedMetadata,
    MappedStrings, MemoryBudget, ParallelScanConfig, ResourceEstimate, ResourcePlan, SpillVolume,
    WorkerThreadSetup, recover_incomplete_artifact, scan_parquet_inputs_parallel,
    validate_artifact, validate_parquet_inputs,
};
use fs2::FileExt;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub struct PipelineContext {
    config: RunConfig,
    config_digest: String,
    run_dir: PathBuf,
    diagnostic: bool,
    _run_lock: File,
    _lifecycle_monitor: dedup_linux::LifecycleMonitor,
    progress: ProgressReporter,
}

struct PipelineNumaExecutor<'a>(&'a dedup_linux::NumaWorkerPool);

impl ChunkExecutor for PipelineNumaExecutor<'_> {
    fn worker_count(&self) -> usize {
        self.0.worker_count()
    }

    fn map_chunks<T, R, F>(
        &self,
        items: &[T],
        chunk_size: usize,
        map: F,
    ) -> Result<Vec<R>, DedupError>
    where
        T: Sync,
        R: Send,
        F: Fn(&[T]) -> Result<R, DedupError> + Send + Sync,
    {
        self.0
            .map_chunks(items, chunk_size, map)
            .map_err(|error| match error {
                dedup_linux::NumaExecutionError::Platform(error) => platform_error(error),
                dedup_linux::NumaExecutionError::Task(error) => error,
            })
    }
}

impl PipelineContext {
    pub fn load(
        config_path: PathBuf,
        diagnostic: bool,
        progress_mode: ProgressMode,
        progress_interval: Duration,
    ) -> Result<Self, DedupError> {
        let config_path = config_path.canonicalize().map_err(DedupError::Io)?;
        let content = fs::read_to_string(&config_path)?;
        let mut config: RunConfig =
            toml::from_str(&content).map_err(|error| DedupError::InvalidInput {
                context: ErrorContext::stage("config"),
                message: error.to_string(),
            })?;
        validate_config(&config)?;
        let configured_lsh_shape = format!(
            "{},{}",
            config.metadata_prefilter_parameters.lsh_bands,
            config.metadata_prefilter_parameters.lsh_rows_per_band
        );
        if !config
            .metadata_prefilter_parameters
            .apply_derived_lsh_shape()
        {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("config"),
                message:
                    "template threshold and target recall have no bounded deterministic LSH shape"
                        .to_owned(),
            });
        }
        config.recorded_overrides.insert(
            "metadata_prefilter_parameters.configured_lsh_shape".to_owned(),
            configured_lsh_shape,
        );
        let base = config_path
            .parent()
            .ok_or_else(|| DedupError::InvalidInput {
                context: ErrorContext::stage("config"),
                message: "configuration path has no parent".to_owned(),
            })?;
        config.input_files = config
            .input_files
            .iter()
            .map(|path| resolve(base, path).to_string_lossy().into_owned())
            .collect();
        config.temporary_volumes = config
            .temporary_volumes
            .iter()
            .map(|path| resolve(base, path).to_string_lossy().into_owned())
            .collect();
        let output_dir = resolve(base, &config.output_dir);
        config.output_dir = output_dir.to_string_lossy().into_owned();
        let digest: [u8; 32] = Sha256::digest(content.as_bytes()).into();
        let config_digest = hex(&digest);
        let run_dir = output_dir.join("run");
        fs::create_dir_all(&run_dir)?;
        let run_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(run_dir.join("pipeline.lock"))?;
        run_lock
            .try_lock_exclusive()
            .map_err(|error| DedupError::InvalidInput {
                context: ErrorContext::stage("startup"),
                message: format!(
                    "another dedup process is already using output_dir {}: {error}",
                    output_dir.display()
                ),
            })?;
        let lifecycle_monitor =
            dedup_linux::LifecycleMonitor::install_native().map_err(platform_error)?;
        let progress = ProgressReporter::new(
            &run_dir,
            progress_mode,
            progress_interval,
            lifecycle_monitor.handle(),
        )?;
        Ok(Self {
            config,
            config_digest,
            run_dir,
            diagnostic,
            _run_lock: run_lock,
            _lifecycle_monitor: lifecycle_monitor,
            progress,
        })
    }

    pub fn track_stage(
        &self,
        stage: &'static str,
        operation: impl FnOnce() -> Result<(), DedupError>,
    ) -> Result<(), DedupError> {
        if self.progress.should_stop_intake() {
            return Err(DedupError::Interrupted { stage });
        }
        self.progress.begin_stage(stage);
        let result = self
            .progress
            .check_cancelled(stage)
            .and_then(|()| operation());
        self.progress.finish_stage(&result);
        result?;
        // SIGTERM enters Draining: let the current atomic stage publish its
        // artifact checkpoint, then refuse admission to the next stage.
        if self.progress.should_stop_intake() {
            return Err(DedupError::Interrupted { stage });
        }
        Ok(())
    }

    pub fn preflight(&self) -> Result<(), DedupError> {
        let production_platform = dedup_linux::is_linux_platform();
        if !production_platform && !self.diagnostic {
            return Err(DedupError::PlatformCapabilityMissing {
                capability:
                    "official production runs require Linux; pass --diagnostic for development"
                        .to_owned(),
            });
        }
        let profile_path = self.run_dir.join("hardware_profile.json");
        if profile_path.is_file() {
            self.progress.begin_phase("reuse_hardware_profile", Some(1));
            let profile: serde_json::Value =
                serde_json::from_slice(&fs::read(&profile_path)?).map_err(json_error)?;
            let compatible = profile
                .get("configuration_digest")
                .and_then(serde_json::Value::as_str)
                == Some(self.config_digest.as_str())
                && profile
                    .get("diagnostic")
                    .and_then(serde_json::Value::as_bool)
                    == Some(self.diagnostic);
            if compatible {
                let hardware_matches = if production_platform {
                    let current =
                        dedup_linux::read_hardware_topology(&dedup_linux::SystemPlatformReader)
                            .map_err(platform_error)?;
                    profile.get("hardware")
                        == Some(&serde_json::to_value(current).map_err(json_error)?)
                } else {
                    true
                };
                if hardware_matches {
                    if let Some(limit) = profile
                        .get("stage_memory_limit")
                        .and_then(serde_json::Value::as_u64)
                    {
                        self.progress.set_memory_limit(limit);
                    }
                    self.progress.advance(1);
                    return Ok(());
                }
            } else {
                return Err(DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("preflight"),
                    message: "existing hardware profile belongs to a different run configuration"
                        .to_owned(),
                });
            }
        }
        self.progress.begin_phase(
            "validate_parquet_inputs",
            u64::try_from(self.config.input_files.len()).ok(),
        );
        let inputs = validate_parquet_inputs(&self.config.input_files)?;
        self.progress
            .advance(u64::try_from(inputs.len()).unwrap_or(u64::MAX));
        self.progress.begin_phase(
            "prepare_temporary_volumes",
            u64::try_from(self.config.temporary_volumes.len()).ok(),
        );
        for volume in &self.config.temporary_volumes {
            fs::create_dir_all(volume)?;
            self.progress.advance(1);
        }
        fs::create_dir_all(&self.run_dir)?;
        let source_bytes = self
            .config
            .input_files
            .iter()
            .try_fold(0_u64, |total, path| {
                total
                    .checked_add(fs::metadata(path)?.len())
                    .ok_or(DedupError::CounterOverflow {
                        counter: "preflight_source_bytes",
                    })
            })?;
        // Entity spill can retain the fixed-width input rows while one complete
        // sort generation is read and the next is written. Metadata/string
        // payloads are additionally bounded by the source size.
        let input_rows = inputs
            .iter()
            .fold(0_u64, |total, input| total.saturating_add(input.row_count));
        let entity_sort_spill_ceiling = input_rows.saturating_mul(80 + 96 * 2);
        let spill_ceiling = source_bytes
            .saturating_mul(2)
            .saturating_add(entity_sort_spill_ceiling);
        let temporary_free_bytes =
            self.config
                .temporary_volumes
                .iter()
                .try_fold(0_u64, |total, volume| {
                    total.checked_add(fs2::available_space(volume)?).ok_or(
                        DedupError::CounterOverflow {
                            counter: "temporary_free_bytes",
                        },
                    )
                })?;
        if temporary_free_bytes < spill_ceiling {
            return Err(DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("preflight"),
                requested: spill_ceiling,
            });
        }
        let storage_calibration = self
            .config
            .temporary_volumes
            .iter()
            .map(|volume| calibrate_volume(Path::new(volume)))
            .collect::<Result<Vec<_>, _>>()?;
        let (physical_memory, cgroup_memory_limit, hardware) = if production_platform {
            let topology = dedup_linux::read_hardware_topology(&dedup_linux::SystemPlatformReader)
                .map_err(platform_error)?;
            dedup_linux::enforce_hardware_quality_gate(
                &dedup_linux::NativePlatformController,
                &topology,
            )
            .map_err(platform_error)?;
            let mounts = dedup_linux::parse_mountinfo(
                &dedup_linux::read_native_mountinfo().map_err(platform_error)?,
            )
            .map_err(platform_error)?;
            for path in self
                .config
                .input_files
                .iter()
                .map(String::as_str)
                .chain(self.config.temporary_volumes.iter().map(String::as_str))
                .chain([self.config.output_dir.as_str()])
            {
                let canonical = Path::new(path).canonicalize().or_else(|_| {
                    Path::new(path)
                        .parent()
                        .unwrap_or(Path::new(path))
                        .canonicalize()
                })?;
                dedup_linux::inspect_local_filesystem(&canonical, &mounts)
                    .map_err(platform_error)?;
            }
            let configured_limit =
                (self.config.memory_limit > 0).then_some(self.config.memory_limit);
            let effective_cgroup_limit = configured_limit
                .map_or(topology.cgroup_memory_limit, |limit| {
                    topology.cgroup_memory_limit.min(limit)
                });
            (
                topology.physical_memory,
                effective_cgroup_limit,
                Some(serde_json::to_value(&topology).map_err(json_error)?),
            )
        } else {
            let limit = if self.config.memory_limit == 0 {
                1024 * 1024 * 1024
            } else {
                self.config.memory_limit
            };
            (limit, limit, None)
        };
        let memory = MemoryBudget::new(physical_memory, cgroup_memory_limit);
        self.progress.set_memory_limit(memory.stage_limit());
        let estimate = ResourceEstimate {
            fixed_bytes: source_bytes.saturating_mul(2),
            variable_bytes: source_bytes.saturating_mul(3),
            hottest_group_bytes: source_bytes / 100,
        };
        let entity_plan = ResourcePlan::choose(
            self.config.entity_execution_mode,
            estimate,
            memory.in_memory_admission_limit(),
            memory.stage_limit(),
        );
        let worker_limit = hardware
            .as_ref()
            .and_then(|value| value.get("physical_cores"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map_or(1, |value| u64::try_from(value.get()).unwrap_or(u64::MAX))
            })
            .max(1);
        let effective_concurrency = serde_json::json!({
            "preflight": 1,
            "entity": effective_workers(self.config.stage_concurrency.entity, worker_limit),
            "name": effective_workers(self.config.stage_concurrency.name, worker_limit),
            "uri": effective_workers(self.config.stage_concurrency.uri, worker_limit),
            "metadata": effective_workers(self.config.stage_concurrency.metadata, worker_limit),
            "report": 1,
        });
        let requested_concurrency = serde_json::json!({
            "preflight": self.config.stage_concurrency.preflight,
            "entity": self.config.stage_concurrency.entity,
            "name": self.config.stage_concurrency.name,
            "uri": self.config.stage_concurrency.uri,
            "metadata": self.config.stage_concurrency.metadata,
            "report": self.config.stage_concurrency.report,
        });
        let profile = serde_json::json!({
            "official": production_platform && !self.diagnostic,
            "diagnostic": self.diagnostic,
            "input_files": self.config.input_files,
            "row_groups": inputs.iter().map(|input| input.row_group_count).sum::<usize>(),
            "configuration_digest": self.config_digest,
            "hardware": hardware,
            "available_memory": memory.available(),
            "stage_memory_limit": memory.stage_limit(),
            "in_memory_admission_limit": memory.in_memory_admission_limit(),
            "source_bytes": source_bytes,
            "input_rows": input_rows,
            "entity_sort_spill_ceiling": entity_sort_spill_ceiling,
            "spill_ceiling": spill_ceiling,
            "temporary_free_bytes": temporary_free_bytes,
            "storage_calibration": storage_calibration,
            "entity_resource_plan": format!("{entity_plan:?}"),
            "requested_concurrency": requested_concurrency,
            "effective_concurrency": effective_concurrency,
            "serial_stage_reason": {
                "preflight": "ordered hardware and filesystem validation",
                "report": "deterministic bitmap aggregation and artifact publication",
            },
        });
        let profile_bytes = serde_json::to_vec_pretty(&profile).map_err(json_error)?;
        fs::write(self.run_dir.join("hardware_profile.json"), &profile_bytes)?;
        fs::create_dir_all(&self.config.output_dir)?;
        fs::write(
            Path::new(&self.config.output_dir).join("hardware_profile.json"),
            profile_bytes,
        )?;
        Ok(())
    }

    pub fn build_entities(&self) -> Result<(), DedupError> {
        recover_incomplete_artifact(self.entities_path())?;
        self.preflight()?;
        let inputs = validate_parquet_inputs(&self.config.input_files)?;
        let total_rows = inputs.iter().try_fold(0_u64, |total, input| {
            total
                .checked_add(input.row_count)
                .ok_or(DedupError::CounterOverflow {
                    counter: "entity_input_rows",
                })
        })?;
        self.progress
            .begin_phase("scan_and_build_entities", Some(total_rows));
        let requested_workers = self.stage_workers("entity")?;
        let effective_memory = self.effective_memory_limit()?;
        let memory = MemoryBudget::new(effective_memory, effective_memory);
        let source_bytes = self
            .config
            .input_files
            .iter()
            .try_fold(0_u64, |total, path| {
                total
                    .checked_add(fs::metadata(path)?.len())
                    .ok_or(DedupError::CounterOverflow {
                        counter: "entity_source_bytes",
                    })
            })?;
        let average_encoded_row = source_bytes
            .saturating_div(total_rows.max(1))
            .clamp(256, 64 * 1024);
        let queue_budget = memory.stage_limit() / 4;
        let minimum_bytes_per_worker = average_encoded_row.saturating_mul(256).saturating_mul(2);
        let affordable_workers = (memory.stage_limit() / 4)
            .saturating_div(minimum_bytes_per_worker.max(1))
            .max(1);
        let workers = requested_workers.min(
            usize::try_from(affordable_workers)
                .unwrap_or(usize::MAX)
                .max(1),
        );
        let batch_size = usize::try_from(
            queue_budget
                .saturating_div(u64::try_from(workers).unwrap_or(u64::MAX).max(1))
                .saturating_div(average_encoded_row.saturating_mul(2).max(1)),
        )
        .unwrap_or(8 * 1024)
        .clamp(256, 8 * 1024);
        let bytes_per_worker = average_encoded_row
            .saturating_mul(u64::try_from(batch_size).unwrap_or(u64::MAX))
            .saturating_mul(2);
        let queue_bytes = bytes_per_worker
            .saturating_mul(u64::try_from(workers).unwrap_or(u64::MAX))
            .min(queue_budget);
        let _queue_lease = memory.require_lease(queue_bytes)?;
        let entity_topology = self.stage_topology("entity", workers)?;
        let entity_placements = dedup_linux::plan_worker_placements(&entity_topology, workers)
            .map_err(platform_error)?;
        let enforce_entity_binding = dedup_linux::is_linux_platform() && !self.diagnostic;
        self.write_worker_placement(
            "entity",
            workers,
            workers.saturating_mul(2).max(1),
            enforce_entity_binding,
            &entity_topology,
            &entity_placements,
        )?;
        let worker_setup: Arc<dyn WorkerThreadSetup> = Arc::new(PlatformWorkerSetup {
            placements: entity_placements,
            enforce_binding: enforce_entity_binding,
        });
        let entity_plan = ResourcePlan::choose(
            self.config.entity_execution_mode,
            ResourceEstimate {
                fixed_bytes: total_rows.saturating_mul(64),
                variable_bytes: source_bytes.saturating_mul(2),
                hottest_group_bytes: source_bytes / 100,
            },
            memory.in_memory_admission_limit(),
            memory.stage_limit(),
        );
        let external_sort_rows = usize::try_from(
            memory
                .stage_limit()
                .saturating_div(8)
                .saturating_div(12 * 8),
        )
        .unwrap_or(1_048_576)
        .clamp(4_096, 1_048_576);
        let external_merge_fan_in = 15;
        let external_sort_memory_bytes = u64::try_from(external_sort_rows)
            .unwrap_or(u64::MAX)
            .saturating_mul(12 * 8)
            .saturating_add(
                u64::try_from(external_merge_fan_in)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(64 * 1024),
            );
        let _external_sort_lease = matches!(entity_plan.mode, dedup_model::ExecutionMode::External)
            .then(|| memory.require_lease(external_sort_memory_bytes))
            .transpose()?;
        let spill_volumes = self.spill_volumes("entity")?;
        let spill_root = spill_volumes
            .first()
            .map(|volume| volume.root.clone())
            .ok_or_else(|| invariant("Entity has no temporary volume"))?;
        let mut builder = EntityBuilder::new_with_execution(
            self.config.chains.clone(),
            self.config.evm_chains.clone(),
            64,
            CanonicalMetadataValidator,
            EntityExecutionConfig::new(
                entity_plan.mode,
                Some(spill_root.clone()),
                external_sort_rows,
                external_merge_fan_in,
            )?
            .with_spill_volumes(spill_volumes.clone())?
            .with_string_sort_bytes(
                usize::try_from(
                    memory
                        .stage_limit()
                        .saturating_div(16)
                        .clamp(8 * 1024 * 1024, 512 * 1024 * 1024),
                )
                .unwrap_or(512 * 1024 * 1024),
            )?,
        )?;
        fs::write(
            self.run_dir.join("entity_execution.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "requested_mode": format!("{:?}", self.config.entity_execution_mode),
                "effective_mode": format!("{:?}", entity_plan.mode),
                "spill_root": spill_root,
                "spill_volumes": radix_volume_plan(&spill_volumes),
                "predicted_peak_bytes": entity_plan.predicted_peak_bytes,
                "metadata_blob_spill": matches!(
                    entity_plan.mode,
                    dedup_model::ExecutionMode::Hybrid | dedup_model::ExecutionMode::External
                ),
                "external_sort_rows": external_sort_rows,
                "external_merge_fan_in": external_merge_fan_in,
                "external_sort_memory_bytes": external_sort_memory_bytes,
                "requested_workers": requested_workers,
                "effective_workers": workers,
                "batch_rows": batch_size,
                "queue_memory_bytes": queue_bytes,
                "worker_placement": read_json_value(
                    &self.run_dir.join("entity_worker_placement.json")
                )?,
            }))
            .map_err(json_error)?,
        )?;
        let mut counters = StageCounters::default();
        let scan = scan_parquet_inputs_parallel(
            &inputs,
            &self.config.chains.iter().cloned().collect(),
            &self.config.evm_chains.iter().cloned().collect(),
            &mut |row| builder.push(row),
            &mut counters,
            &self.progress,
            ParallelScanConfig::new(workers, 1, batch_size)?.with_worker_setup(worker_setup),
        )?;
        let path = self.entities_path();
        if path.exists() {
            let existing = validate_artifact(&path)?;
            if existing.logical_input_digest == scan.logical_input_digest
                && existing.configuration_digest == self.config_digest
            {
                return self.write_stage_metrics("entities", &counters);
            }
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("entities"),
                message: "existing entity artifact belongs to different input or configuration"
                    .to_owned(),
            });
        }
        self.progress.begin_phase("finalize_entities", None);
        let summary = builder.finish_to_artifact(
            &path,
            scan.logical_input_digest,
            self.config_digest.clone(),
            &self.progress,
        )?;
        counters.spill_bytes(summary.metadata_spill_bytes)?;
        counters.spill_bytes(summary.external_handle_spill_bytes)?;
        counters.entity_radix_handle_touches(summary.external_handle_touches)?;
        counters.entity_digest_bucket_max(summary.digest_bucket_max)?;
        let execution_path = self.run_dir.join("entity_execution.json");
        let mut execution: serde_json::Value =
            serde_json::from_slice(&fs::read(&execution_path)?).map_err(json_error)?;
        let execution_fields = execution
            .as_object_mut()
            .ok_or_else(|| invariant("entity execution plan is not a JSON object"))?;
        execution_fields.insert(
            "metadata_spill_bytes".to_owned(),
            serde_json::json!(summary.metadata_spill_bytes),
        );
        execution_fields.insert(
            "external_handle_spill_bytes".to_owned(),
            serde_json::json!(summary.external_handle_spill_bytes),
        );
        execution_fields.insert(
            "external_handle_touches".to_owned(),
            serde_json::json!(summary.external_handle_touches),
        );
        execution_fields.insert(
            "external_volumes_used".to_owned(),
            serde_json::json!(summary.external_volumes_used),
        );
        execution_fields.insert(
            "string_count".to_owned(),
            serde_json::json!(summary.string_count),
        );
        execution_fields.insert(
            "contract_count".to_owned(),
            serde_json::json!(summary.contract_count),
        );
        execution_fields.insert("nft_count".to_owned(), serde_json::json!(summary.nft_count));
        execution_fields.insert(
            "resident_memory_bytes_at_completion".to_owned(),
            serde_json::json!(self.progress.resident_memory_bytes()),
        );
        fs::write(
            execution_path,
            serde_json::to_vec_pretty(&execution).map_err(json_error)?,
        )?;
        self.write_stage_metrics("entities", &counters)
    }

    pub fn run_name(&self) -> Result<(), DedupError> {
        let (entities, manifest) = self.load_entities()?;
        if self.restore_completed_hits("name", &manifest)? {
            return Ok(());
        }
        let mut name_config =
            NameEngineConfig::production_default(self.config.work_budgets.name_scored_candidates);
        name_config.threshold = self.config.name_threshold / 100.0;
        let requested_workers = self.stage_workers("name")?;
        let workers =
            self.write_name_memory_forecast(&entities, &name_config, requested_workers)?;
        let worker_pool = self.stage_worker_pool("name", workers)?;
        self.progress.set_numa_metrics(worker_pool.metrics_handle());
        let mut sink = BitmapHitSink::new_sharded(
            hit_capacity(entities.artifacts.contracts.len(), self.config.chains.len())?,
            worker_pool.node_count(),
            entity_upper_bound(entities.artifacts.contracts.len())?,
        )?;
        let result = run_name_with_progress_and_executor(
            &entities.artifacts.contracts,
            &entities.strings,
            name_config,
            &mut sink,
            &self.progress,
            &PipelineNumaExecutor(&worker_pool),
        )?;
        sink.finish_batch();
        let name_plan_path = self.run_dir.join("name_resource_plan.json");
        let mut name_plan: serde_json::Value =
            serde_json::from_slice(&fs::read(&name_plan_path)?).map_err(json_error)?;
        let plan_fields = name_plan
            .as_object_mut()
            .ok_or_else(|| invariant("Name resource plan is not a JSON object"))?;
        plan_fields.insert(
            "actual_atoms".to_owned(),
            serde_json::json!(result.atoms.len()),
        );
        plan_fields.insert(
            "actual_canonical_names".to_owned(),
            serde_json::json!(result.canonical_names.len()),
        );
        plan_fields.insert(
            "actual_contract_ids".to_owned(),
            serde_json::json!(result.contract_ids.len()),
        );
        plan_fields.insert(
            "resident_memory_bytes_at_completion".to_owned(),
            serde_json::json!(self.progress.resident_memory_bytes()),
        );
        plan_fields.insert(
            "worker_placement".to_owned(),
            read_json_value(&self.run_dir.join("name_worker_placement.json"))?,
        );
        plan_fields.insert(
            "hit_sink_shards".to_owned(),
            serde_json::json!(sink.shard_count()),
        );
        plan_fields.insert(
            "numa_execution".to_owned(),
            serde_json::to_value(worker_pool.execution_metrics()).map_err(json_error)?,
        );
        fs::write(
            name_plan_path,
            serde_json::to_vec_pretty(&name_plan).map_err(json_error)?,
        )?;
        self.save_hits("name", &sink, &manifest, &result.counters)?;
        self.write_stage_metrics("name", &result.counters)
    }

    pub fn run_uri(&self) -> Result<(), DedupError> {
        let (entities, manifest) = self.load_mapped_entity_objects()?;
        if self.restore_completed_hits("uri", &manifest)? {
            return Ok(());
        }
        let mark_shards = self.stage_workers("uri")?;
        let nft_count = usize::try_from(entities.nfts.len()).map_err(|_| {
            DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("uri"),
                requested: entities.nfts.len(),
            }
        })?;
        let mut sink = BitmapHitSink::new_sharded(
            hit_capacity(nft_count, self.config.chains.len())?,
            mark_shards,
            entity_upper_bound(nft_count)?,
        )?;
        let estimated_member_bytes = entities.nfts.len().saturating_mul(64);
        let effective_memory = self.effective_memory_limit()?;
        let memory = MemoryBudget::new(effective_memory, effective_memory);
        let plan = ResourcePlan::choose(
            self.config.uri_execution_mode,
            ResourceEstimate {
                fixed_bytes: estimated_member_bytes / 2,
                variable_bytes: estimated_member_bytes,
                hottest_group_bytes: estimated_member_bytes,
            },
            memory.in_memory_admission_limit(),
            memory.stage_limit(),
        );
        let radix_volumes = self.spill_volumes("uri")?;
        let spill_root = radix_volumes
            .first()
            .map(|volume| volume.root.clone())
            .ok_or_else(|| invariant("URI has no temporary volume"))?;
        let member_capacity = nft_count.saturating_mul(2).max(1);
        let hot_group_member_limit =
            usize::try_from(memory.in_memory_admission_limit().saturating_div(64))
                .unwrap_or(usize::MAX)
                .clamp(1, member_capacity);
        let radix_memory = radix_memory_bytes(&memory, 16);
        let mark_buffer_capacity =
            mark_shards
                .checked_mul(4_096)
                .ok_or(DedupError::ResourceBudgetExceeded {
                    context: ErrorContext::stage("uri"),
                    requested: u64::MAX,
                })?;
        let uri_config = UriExecutionConfig::new(
            plan.mode,
            spill_root,
            hot_group_member_limit,
            4,
            16,
            member_capacity,
        )?
        .with_radix_memory_budget(memory.clone(), radix_memory)?
        .with_radix_volumes(radix_volumes)?
        .with_mark_shards(mark_shards, mark_buffer_capacity)?
        .with_workers(mark_shards)?;
        let worker_pool = self.stage_worker_pool("uri", uri_config.workers)?;
        self.progress.set_numa_metrics(worker_pool.metrics_handle());
        let result = run_uri_mapped_with_config_progress_and_executor(
            &entities.contracts,
            &entities.nfts,
            &mut sink,
            &uri_config,
            &self.progress,
            &PipelineNumaExecutor(&worker_pool),
        )?;
        sink.finish_batch();
        fs::write(
            self.run_dir.join("uri_resource_plan.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "requested_mode": format!("{:?}", self.config.uri_execution_mode),
                "effective_mode": format!("{:?}", plan.mode),
                "spill_root": uri_config.spill_root,
                "radix_volumes": radix_volume_plan(&uri_config.radix_volumes),
                "predicted_peak_bytes": plan.predicted_peak_bytes,
                "stage_memory_limit": memory.stage_limit(),
                "hot_group_member_limit": hot_group_member_limit,
                "radix_memory_bytes": radix_memory,
                "mark_shards": mark_shards,
                "workers": uri_config.workers,
                "numa_execution": worker_pool.execution_metrics(),
                "worker_placement": read_json_value(
                    &self.run_dir.join("uri_worker_placement.json")
                )?,
                "mark_buffer_capacity": mark_buffer_capacity,
                "token_groups": result.token_groups,
                "image_groups": result.image_groups,
                "spilled_members": result.counters.uri_spilled_members,
                "spill_bytes": result.counters.spill_bytes,
                "radix_handle_touches": result.counters.uri_radix_handle_touches,
                "max_spill_reducer_buffered_members": result.max_spill_reducer_buffered_members,
                "max_spill_hit_buffered_events": result.max_spill_hit_buffered_events,
                "actual_spill_hit_shards": result.spill_hit_shards,
                "hit_sink_shards": sink.shard_count(),
                "resident_memory_bytes_at_completion": self.progress.resident_memory_bytes(),
            }))
            .map_err(json_error)?,
        )?;
        for primary in 0..self.config.chains.len() {
            let primary = chain_id(primary)?;
            sink.apply_image_priority(ScopeId::Intra(primary));
            sink.apply_image_priority(ScopeId::CrossSummary(primary));
            for secondary in 0..self.config.chains.len() {
                let secondary = chain_id(secondary)?;
                if primary != secondary {
                    sink.apply_image_priority(ScopeId::Matrix { primary, secondary });
                }
            }
        }
        self.save_hits("uri", &sink, &manifest, &result.counters)?;
        self.write_stage_metrics("uri", &result.counters)
    }

    fn effective_memory_limit(&self) -> Result<u64, DedupError> {
        if self.config.memory_limit > 0 {
            return Ok(self.config.memory_limit);
        }
        let profile: serde_json::Value =
            serde_json::from_slice(&fs::read(self.run_dir.join("hardware_profile.json"))?)
                .map_err(json_error)?;
        profile
            .get("available_memory")
            .and_then(serde_json::Value::as_u64)
            .filter(|value| *value > 0)
            .ok_or_else(|| DedupError::ArtifactMismatch {
                context: ErrorContext::stage("uri"),
                message: "hardware profile has no positive available_memory".to_owned(),
            })
    }

    fn stage_workers(&self, stage: &'static str) -> Result<usize, DedupError> {
        let profile: serde_json::Value =
            serde_json::from_slice(&fs::read(self.run_dir.join("hardware_profile.json"))?)
                .map_err(json_error)?;
        let workers = profile
            .get("effective_concurrency")
            .and_then(|value| value.get(stage))
            .and_then(serde_json::Value::as_u64)
            .filter(|value| *value > 0)
            .ok_or_else(|| DedupError::ArtifactMismatch {
                context: ErrorContext::stage(stage),
                message: format!("hardware profile has no positive {stage} worker count"),
            })?;
        usize::try_from(workers).map_err(|_| DedupError::InvalidInput {
            context: ErrorContext::stage(stage),
            message: format!("{stage} worker count does not fit usize"),
        })
    }

    fn stage_worker_pool(
        &self,
        stage: &'static str,
        workers: usize,
    ) -> Result<dedup_linux::NumaWorkerPool, DedupError> {
        let topology = self.stage_topology(stage, workers)?;
        let require_binding = dedup_linux::is_linux_platform() && !self.diagnostic;
        let queue_capacity = workers.saturating_mul(4).max(1);
        let pool = dedup_linux::build_numa_worker_pool(
            &topology,
            workers,
            queue_capacity,
            stage,
            require_binding,
        )
        .map_err(platform_error)?;
        self.write_worker_placement(
            stage,
            workers,
            queue_capacity,
            require_binding,
            &topology,
            pool.placements(),
        )?;
        Ok(pool)
    }

    fn stage_topology(
        &self,
        stage: &'static str,
        workers: usize,
    ) -> Result<dedup_linux::HardwareTopology, DedupError> {
        let profile: serde_json::Value =
            serde_json::from_slice(&fs::read(self.run_dir.join("hardware_profile.json"))?)
                .map_err(json_error)?;
        let topology = match profile.get("hardware") {
            Some(value) if !value.is_null() => {
                serde_json::from_value::<dedup_linux::HardwareTopology>(value.clone())
                    .map_err(json_error)?
            }
            _ => {
                let logical_cpus: Vec<u32> = (0..workers)
                    .map(|index| {
                        u32::try_from(index).map_err(|_| DedupError::ResourceBudgetExceeded {
                            context: ErrorContext::stage(stage),
                            requested: u64::try_from(workers).unwrap_or(u64::MAX),
                        })
                    })
                    .collect::<Result<_, _>>()?;
                dedup_linux::HardwareTopology {
                    allowed_logical_cpus: logical_cpus.clone(),
                    physical_cores: u32::try_from(workers).map_err(|_| {
                        DedupError::ResourceBudgetExceeded {
                            context: ErrorContext::stage(stage),
                            requested: u64::try_from(workers).unwrap_or(u64::MAX),
                        }
                    })?,
                    numa_nodes: vec![dedup_linux::NumaNode {
                        id: 0,
                        logical_cpus: logical_cpus.clone(),
                        memory_bytes: self.effective_memory_limit()?,
                    }],
                    cpu_to_numa_node: logical_cpus.into_iter().map(|cpu| (cpu, 0)).collect(),
                    physical_memory: self.effective_memory_limit()?,
                    cgroup_memory_limit: self.effective_memory_limit()?,
                    cpu_quota_parallelism: None,
                }
            }
        };
        Ok(topology)
    }

    fn write_worker_placement(
        &self,
        stage: &'static str,
        workers: usize,
        queue_capacity: usize,
        binding_enforced: bool,
        topology: &dedup_linux::HardwareTopology,
        placements: &[dedup_linux::WorkerPlacement],
    ) -> Result<(), DedupError> {
        let memory = MemoryBudget::new(topology.physical_memory, topology.cgroup_memory_limit);
        let node_budgets = memory.split_node_budgets(
            &topology
                .numa_nodes
                .iter()
                .map(|node| node.memory_bytes.max(1))
                .collect::<Vec<_>>(),
        )?;
        let node_memory_limits: BTreeMap<_, _> = topology
            .numa_nodes
            .iter()
            .zip(&node_budgets)
            .map(|(node, budget)| (node.id, budget.limit()))
            .collect();
        let mut node_workers = BTreeMap::<u32, u64>::new();
        for placement in placements {
            let count = node_workers.entry(placement.numa_node).or_default();
            *count = count.saturating_add(1);
        }
        fs::write(
            self.run_dir.join(format!("{stage}_worker_placement.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "workers": workers,
                "worker_queue_capacity_per_node": queue_capacity,
                "binding_enforced": binding_enforced,
                "node_workers": node_workers,
                "node_memory_limits": node_memory_limits,
                "placements": placements,
            }))
            .map_err(json_error)?,
        )?;
        Ok(())
    }

    fn write_name_memory_forecast(
        &self,
        entities: &EntityBuildResult,
        config: &NameEngineConfig,
        workers: usize,
    ) -> Result<usize, DedupError> {
        self.progress.begin_phase(
            "forecast_name_memory",
            u64::try_from(entities.artifacts.contracts.len()).ok(),
        );
        let mut unique_names = BTreeSet::new();
        let mut work = 0_u64;
        for contract in &entities.artifacts.contracts {
            if let Some(name_ref) = contract.name_ref {
                unique_names.insert(name_ref);
            }
            work = work.saturating_add(1);
            if work == 4_096 {
                self.progress.advance(work);
                work = 0;
            }
        }
        self.progress.advance(work);
        let name_bytes = unique_names.iter().try_fold(0_u64, |total, name_ref| {
            let length = entities
                .strings
                .resolve(*name_ref)
                .ok_or_else(|| invariant("missing Name StringId during memory forecast"))?
                .len();
            total
                .checked_add(u64::try_from(length).unwrap_or(u64::MAX))
                .ok_or(DedupError::CounterOverflow {
                    counter: "name_forecast_bytes",
                })
        })?;
        let canonical_count = u64::try_from(unique_names.len()).unwrap_or(u64::MAX);
        let candidate_upper = canonical_count
            .saturating_mul(canonical_count.saturating_sub(1))
            .saturating_div(2)
            .min(config.candidate_pair_budget);
        let atom_bytes = u64::try_from(entities.artifacts.contracts.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(48);
        let canonical_bytes = canonical_count
            .saturating_mul(72)
            .saturating_add(name_bytes.saturating_mul(12));
        let posting_bytes = name_bytes.saturating_mul(24);
        let candidate_bytes = candidate_upper.saturating_mul(40);
        let base_peak_bytes = atom_bytes
            .saturating_add(canonical_bytes)
            .saturating_add(posting_bytes)
            .saturating_add(candidate_bytes);
        let effective_memory = self.effective_memory_limit()?;
        let stage_limit = effective_memory.saturating_mul(75) / 100;
        let scratch_per_worker = name_bytes.clamp(1, 16 * 1024 * 1024);
        let affordable_workers = stage_limit
            .saturating_sub(base_peak_bytes)
            .saturating_div(scratch_per_worker)
            .max(1);
        let effective_workers = workers.min(
            usize::try_from(affordable_workers)
                .unwrap_or(usize::MAX)
                .max(1),
        );
        let worker_scratch_bytes =
            scratch_per_worker.saturating_mul(u64::try_from(effective_workers).unwrap_or(u64::MAX));
        let predicted_peak_bytes = base_peak_bytes.saturating_add(worker_scratch_bytes);
        let warning = predicted_peak_bytes > stage_limit;
        let forecast = serde_json::json!({
            "mode": "in_memory",
            "admission_policy": "warn_and_continue",
            "unique_canonical_names": canonical_count,
            "name_utf8_bytes": name_bytes,
            "candidate_upper_bound": candidate_upper,
            "predicted_peak_bytes": predicted_peak_bytes,
            "stage_memory_limit": stage_limit,
            "requested_workers": workers,
            "effective_workers": effective_workers,
            "worker_reduction_reason": (effective_workers < workers).then_some(
                "worker scratch was reduced to limit additional memory pressure"
            ),
            "warning": warning,
            "warning_message": warning.then_some(
                "predicted Name peak exceeds the stage limit; execution continues in memory as configured"
            ),
        });
        fs::write(
            self.run_dir.join("name_resource_plan.json"),
            serde_json::to_vec_pretty(&forecast).map_err(json_error)?,
        )?;
        Ok(effective_workers)
    }

    pub fn run_metadata(&self) -> Result<(), DedupError> {
        let (entities, manifest) = self.load_mapped_entity_objects()?;
        if self.restore_completed_hits("metadata", &manifest)? {
            return Ok(());
        }
        let (contracts, templates, mut preparation_counters) =
            self.prepare_metadata_inputs(&entities)?;
        let (prefilter_execution, prefilter_plan, prefilter_radix_memory) =
            self.metadata_prefilter_execution(&templates)?;
        let prefilter = generate_metadata_candidates_with_execution_and_progress(
            &contracts,
            &templates,
            &self.config.metadata_prefilter_parameters,
            self.config.work_budgets.metadata_prefilter_pairs,
            &mut preparation_counters,
            &prefilter_execution,
            &self.progress,
        )?;
        let metadata_workers = self.stage_workers("metadata")?;
        let candidate_count = prefilter.candidates.count();
        let candidate_bytes = candidate_count.saturating_mul(32);
        let effective_memory = self.effective_memory_limit()?;
        let memory = MemoryBudget::new(effective_memory, effective_memory);
        let plan = ResourcePlan::choose(
            self.config.metadata_execution_mode,
            ResourceEstimate {
                fixed_bytes: candidate_bytes / 2,
                variable_bytes: candidate_bytes,
                hottest_group_bytes: candidate_bytes,
            },
            memory.in_memory_admission_limit(),
            memory.stage_limit(),
        );
        let radix_volumes = self.spill_volumes("metadata")?;
        let spill_root = radix_volumes
            .first()
            .map(|volume| volume.root.clone())
            .ok_or_else(|| invariant("Metadata has no temporary volume"))?;
        let candidate_capacity = usize::try_from(candidate_count.max(1)).map_err(|_| {
            DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("metadata"),
                requested: candidate_bytes,
            }
        })?;
        let resident_candidate_limit =
            usize::try_from(memory.in_memory_admission_limit().saturating_div(32))
                .unwrap_or(usize::MAX)
                .clamp(1, candidate_capacity);
        let radix_memory = radix_memory_bytes(&memory, 16);
        let execution = MetadataExecutionConfig::new(
            plan.mode,
            spill_root,
            resident_candidate_limit,
            4,
            16,
            candidate_capacity,
        )?
        .with_radix_memory_budget(memory.clone(), radix_memory)?
        .with_radix_volumes(radix_volumes)?
        .with_workers(metadata_workers)?;
        let worker_pool = self.stage_worker_pool("metadata", execution.workers)?;
        self.progress.set_numa_metrics(worker_pool.metrics_handle());
        let contract_count = usize::try_from(entities.contracts.len()).map_err(|_| {
            DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("metadata"),
                requested: entities.contracts.len(),
            }
        })?;
        let mut sink = BitmapHitSink::new_sharded(
            hit_capacity(contract_count, self.config.chains.len())?,
            worker_pool.node_count(),
            entities.contracts.len().max(1),
        )?;
        let mut result = run_metadata_verification_with_config_progress_and_executor(
            &contracts,
            &prefilter,
            self.config.metadata_content_threshold,
            self.config.work_budgets.metadata_verify_pairs,
            &mut sink,
            &execution,
            &self.progress,
            &PipelineNumaExecutor(&worker_pool),
        )?;
        sink.finish_batch();
        result.counters.merge(&preparation_counters)?;
        fs::write(
            self.run_dir.join("metadata_resource_plan.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "requested_mode": format!("{:?}", self.config.metadata_execution_mode),
                "effective_mode": format!("{:?}", plan.mode),
                "prefilter_effective_mode": format!("{:?}", prefilter_plan.mode),
                "spill_root": execution.spill_root,
                "radix_volumes": radix_volume_plan(&execution.radix_volumes),
                "predicted_peak_bytes": plan.predicted_peak_bytes,
                "stage_memory_limit": memory.stage_limit(),
                "candidate_count": candidate_count,
                "resident_candidate_limit": resident_candidate_limit,
                "radix_memory_bytes": radix_memory,
                "prefilter_radix_memory_bytes": prefilter_radix_memory,
                "workers": execution.workers,
                "numa_execution": worker_pool.execution_metrics(),
                "hit_sink_shards": sink.shard_count(),
                "worker_placement": read_json_value(
                    &self.run_dir.join("metadata_worker_placement.json")
                )?,
                "spill_bytes": result.counters.spill_bytes,
                "radix_handle_touches": result.counters.metadata_radix_handle_touches,
                "resident_memory_bytes_at_completion": self.progress.resident_memory_bytes(),
            }))
            .map_err(json_error)?,
        )?;
        self.save_hits("metadata", &sink, &manifest, &result.counters)?;
        self.write_stage_metrics("metadata", &result.counters)?;
        let summary = serde_json::json!({
            "templates": templates.len(),
            "candidates": candidate_count,
            "matches": result.matches.len(),
            "prefilter_audit": {
                "planned_probes": prefilter.audit.planned_probes,
                "emitted_probes": prefilter.audit.emitted_probes,
                "probe_budget_truncations": prefilter.audit.probe_budget_truncations,
                "exact_bucket_cap_truncations": prefilter.audit.exact_bucket_cap_truncations,
                "quota_truncations": prefilter.audit.quota_truncations,
            }
        });
        let bytes = serde_json::to_vec_pretty(&summary).map_err(json_error)?;
        fs::write(self.run_dir.join("data_quality.json"), &bytes)?;
        fs::write(
            Path::new(&self.config.output_dir).join("data_quality.json"),
            bytes,
        )?;
        Ok(())
    }

    pub fn audit_metadata(&self) -> Result<(), DedupError> {
        let (entities, _) = self.load_mapped_entity_objects()?;
        let (contracts, templates, mut counters) = self.prepare_metadata_inputs(&entities)?;
        let sampler = StratifiedSampler {
            seed: self.config.quality_gate.sample_seed,
            contracts_per_stratum: 128,
        };
        let sample = sampler.sample(&contracts, &templates);
        let oracle = ExhaustiveSharedTokenOracle.matches(
            &contracts,
            &sample,
            self.config.metadata_content_threshold,
        )?;
        let (prefilter_execution, _, _) = self.metadata_prefilter_execution(&templates)?;
        let prefilter = generate_metadata_candidates_for_request_with_progress(
            MetadataPrefilterRequest {
                contracts: &contracts,
                templates: &templates,
                parameters: &self.config.metadata_prefilter_parameters,
                probe_budget: self.config.work_budgets.metadata_prefilter_pairs,
                audited_pairs: Some(&oracle),
                execution: Some(&prefilter_execution),
            },
            &mut counters,
            &self.progress,
        )?;
        let (breakdown, decision) = audit_metadata_recall(
            &oracle,
            &prefilter,
            &templates,
            self.config.quality_gate.minimum_positive_pairs,
            self.config.quality_gate.metadata_recall,
        )?;
        let value = serde_json::json!({
            "sample_contracts": sample.len(),
            "true_positive_pairs": breakdown.true_positive_pairs,
            "retained_positive_pairs": breakdown.retained_positive_pairs,
            "recall_ppm": breakdown.recall_ppm(),
            "digest_bucket_cap_misses": breakdown.digest_bucket_cap_misses,
            "lsh_band_misses": breakdown.lsh_band_misses,
            "candidate_quota_misses": breakdown.candidate_quota_misses,
            "low_information_guard_misses": breakdown.low_information_guard_misses,
            "decision": format!("{decision:?}"),
        });
        let bytes = serde_json::to_vec_pretty(&value).map_err(json_error)?;
        fs::write(self.run_dir.join("recall_audit.json"), &bytes)?;
        fs::write(
            Path::new(&self.config.output_dir).join("recall_audit.json"),
            bytes,
        )?;
        Ok(())
    }

    pub fn report(&self) -> Result<(), DedupError> {
        let (entities, manifest) = self.load_mapped_entity_objects()?;
        let mut combined = BitmapHitSink::new(1)?;
        for stage in ["name", "uri", "metadata"] {
            let sink = self.load_hits(stage, &manifest)?;
            for ((dimension, scope, kind), bitmap) in sink.entries() {
                combined.insert_bitmap(*dimension, *scope, *kind, bitmap.clone());
            }
        }
        let rows = build_statistics(&combined, &entities, self.config.chains.len())?;
        fs::create_dir_all(&self.config.output_dir)?;
        write_csv(
            Path::new(&self.config.output_dir).join("summary.csv"),
            rows.iter()
                .filter(|row| row.scope != "chain_matrix")
                .cloned(),
        )?;
        write_csv(
            Path::new(&self.config.output_dir).join("chain_matrix.csv"),
            rows.iter()
                .filter(|row| row.scope == "chain_matrix")
                .cloned(),
        )?;
        let resource_plans = serde_json::json!({
            "entities": read_json_value(&self.run_dir.join("entity_execution.json"))?,
            "name": read_json_value(&self.run_dir.join("name_resource_plan.json"))?,
            "uri": read_json_value(&self.run_dir.join("uri_resource_plan.json"))?,
            "metadata": read_json_value(&self.run_dir.join("metadata_resource_plan.json"))?,
        });
        let run_manifest = serde_json::json!({
            "configuration_digest": self.config_digest,
            "logical_input_digest": manifest.logical_input_digest,
            "name_threshold": self.config.name_threshold,
            "metadata_content_threshold": self.config.metadata_content_threshold,
            "metadata_anchor_tokens": self.config.metadata_anchor_tokens,
            "template_jaccard_threshold": self.config.metadata_prefilter_parameters.template_jaccard_threshold,
            "lsh_bands": self.config.metadata_prefilter_parameters.lsh_bands,
            "lsh_rows_per_band": self.config.metadata_prefilter_parameters.lsh_rows_per_band,
            "neighbors_per_target_chain": self.config.metadata_prefilter_parameters.neighbors_per_target_chain,
            "predicted_candidate_recall": self.config.metadata_prefilter_parameters.predicted_candidate_recall(),
            "recorded_overrides": self.config.recorded_overrides,
            "runtime_decisions": {
                "name_storage": "resident_only",
                "name_over_budget_policy": "warn_and_continue",
                "name_worker_reduction_allowed": true,
            },
            "online_progress": {
                "schema_version": 1,
                "snapshot": "run/progress.json",
                "history": "run/progress.jsonl",
                "eta": "phase EWMA throughput; confident after three positive samples",
                "work_unit_policy": "processed inputs, features, probes or candidates; never hit count",
            },
            "lifecycle": {
                "sigterm": "stop intake after current atomic unit and leave committed artifacts reusable",
                "sigint": "controlled shutdown; second SIGINT exits immediately",
                "sighup": "log reload epoch only; run semantics unchanged",
            },
            "resource_plans": resource_plans,
            "run_status": "complete",
        });
        fs::write(
            Path::new(&self.config.output_dir).join("run_manifest.json"),
            serde_json::to_vec_pretty(&run_manifest).map_err(json_error)?,
        )?;
        let mut stage_metrics = serde_json::Map::new();
        for stage in ["entities", "name", "uri", "metadata"] {
            let bytes = fs::read(self.run_dir.join(format!("{stage}-metrics.json")))?;
            let value = serde_json::from_slice(&bytes).map_err(json_error)?;
            stage_metrics.insert(stage.to_owned(), value);
        }
        fs::write(
            Path::new(&self.config.output_dir).join("stage_metrics.json"),
            serde_json::to_vec_pretty(&stage_metrics).map_err(json_error)?,
        )?;
        Ok(())
    }

    pub fn all(&self) -> Result<(), DedupError> {
        self.track_stage("entities", || self.build_entities())?;
        self.track_stage("name", || self.run_name())?;
        self.track_stage("uri", || self.run_uri())?;
        self.track_stage("metadata", || self.run_metadata())?;
        self.track_stage("metadata_audit", || self.audit_metadata())?;
        self.track_stage("report", || self.report())
    }

    fn load_entities(&self) -> Result<(EntityBuildResult, ArtifactManifest), DedupError> {
        let path = self.entities_path();
        let manifest = validate_artifact(&path)?;
        if manifest.configuration_digest != self.config_digest {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("pipeline"),
                message: "entity artifact configuration digest mismatch".to_owned(),
            });
        }
        let available = self.effective_memory_limit()?;
        let budget = MemoryBudget::new(available, available);
        let contracts = MappedContracts::open(&path, &budget, (budget.stage_limit() / 16).max(1))?;
        let mapped_strings =
            MappedStrings::open(&path, &budget, (budget.stage_limit() / 8).max(1))?;
        let contract_capacity =
            usize::try_from(contracts.len()).map_err(|_| DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("name"),
                requested: contracts.len(),
            })?;
        let mut resident_contracts = Vec::with_capacity(contract_capacity);
        let mut name_strings = StringDictionary::new(64)?;
        for contract in contracts.iter() {
            let mut contract = contract?;
            contract.name_ref = contract
                .name_ref
                .map(|name| name_strings.intern(mapped_strings.resolve(name)?))
                .transpose()?;
            resident_contracts.push(contract);
        }
        Ok((
            EntityBuildResult {
                artifacts: EntityArtifacts {
                    contracts: resident_contracts,
                    nfts: Vec::new(),
                },
                strings: name_strings,
                metadata_by_nft: BTreeMap::new(),
                metadata_spill_bytes: 0,
                external_handle_spill_bytes: 0,
                external_handle_touches: 0,
                external_volumes_used: 0,
            },
            manifest,
        ))
    }

    fn load_mapped_entity_objects(
        &self,
    ) -> Result<(MappedEntityObjects, ArtifactManifest), DedupError> {
        let path = self.entities_path();
        let manifest = validate_artifact(&path)?;
        if manifest.configuration_digest != self.config_digest {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("pipeline"),
                message: "entity artifact configuration digest mismatch".to_owned(),
            });
        }
        let available = self.effective_memory_limit()?;
        let budget = MemoryBudget::new(available, available);
        let residency = (budget.stage_limit() / 4).max(2);
        Ok((
            MappedEntityObjects::open(path, &budget, residency)?,
            manifest,
        ))
    }

    fn metadata_prefilter_execution(
        &self,
        templates: &[dedup_engine::metadata::TemplateFingerprint],
    ) -> Result<(MetadataPrefilterExecutionConfig, ResourcePlan, u64), DedupError> {
        let eligible_upper = u64::try_from(
            templates
                .iter()
                .filter(|template| !template.low_information)
                .count(),
        )
        .map_err(|_| DedupError::CounterOverflow {
            counter: "metadata_prefilter_eligible_upper",
        })?;
        let probe_upper = eligible_upper
            .checked_mul(u64::from(
                self.config.metadata_prefilter_parameters.lsh_bands,
            ))
            .ok_or(DedupError::CounterOverflow {
                counter: "metadata_prefilter_probe_upper",
            })?
            .min(self.config.work_budgets.metadata_prefilter_pairs);
        let probe_bytes = probe_upper.saturating_mul(32);
        let available_memory = self.effective_memory_limit()?;
        let memory = MemoryBudget::new(available_memory, available_memory);
        let plan = ResourcePlan::choose(
            self.config.metadata_execution_mode,
            ResourceEstimate {
                fixed_bytes: probe_bytes / 8,
                variable_bytes: probe_bytes,
                hottest_group_bytes: probe_bytes / 64,
            },
            memory.in_memory_admission_limit(),
            memory.stage_limit(),
        );
        let radix_memory = radix_memory_bytes(&memory, 16);
        let radix_record_capacity = probe_upper
            .max(
                self.config
                    .work_budgets
                    .metadata_prefilter_pairs
                    .saturating_mul(2),
            )
            .max(1);
        let record_capacity = usize::try_from(radix_record_capacity).map_err(|_| {
            DedupError::ResourceBudgetExceeded {
                context: ErrorContext::stage("metadata_prefilter"),
                requested: radix_record_capacity.saturating_mul(32),
            }
        })?;
        let execution = MetadataPrefilterExecutionConfig::new(
            plan.mode,
            self.spill_root("metadata")?,
            4,
            16,
            record_capacity,
        )?
        .with_radix_memory_budget(memory, radix_memory)?
        .with_radix_volumes(self.spill_volumes("metadata")?)?;
        Ok((execution, plan, radix_memory))
    }

    fn prepare_metadata_inputs(
        &self,
        entities: &MappedEntityObjects,
    ) -> Result<
        (
            Vec<ContractAnchors>,
            Vec<dedup_engine::metadata::TemplateFingerprint>,
            StageCounters,
        ),
        DedupError,
    > {
        let evm_chains: BTreeSet<ChainId> = self
            .config
            .chains
            .iter()
            .enumerate()
            .filter(|(_, chain)| self.config.evm_chains.contains(chain))
            .map(|(index, _)| chain_id(index))
            .collect::<Result<_, _>>()?;
        let available_memory = self.effective_memory_limit()?;
        let memory = MemoryBudget::new(available_memory, available_memory);
        let metadata = MappedMetadata::open(
            self.entities_path(),
            &memory,
            (memory.stage_limit() / 8).max(1),
        )?;
        let strings = MappedStrings::open(
            self.entities_path(),
            &memory,
            (memory.stage_limit() / 8).max(1),
        )?;
        let records = metadata.iter().enumerate().map(|(index, entry)| {
            let (nft_id, content) = entry?;
            let doc_id =
                MetadataDocId::new(dedup_model::EntityId::try_from(index).map_err(|_| {
                    DedupError::InvalidInput {
                        context: ErrorContext::stage("metadata"),
                        message: "metadata document count exceeds configured EntityId".to_owned(),
                    }
                })?);
            let nft = entities
                .nfts
                .get(nft_id.as_u64())
                .map_err(|_| invariant("metadata references missing NFT"))?;
            let contract = entities
                .contracts
                .get(nft.contract_id.as_u64())
                .map_err(|_| invariant("NFT references missing contract"))?;
            let token_id =
                std::str::from_utf8(strings.resolve(nft.token_id_ref)?).map_err(|error| {
                    DedupError::ArtifactMismatch {
                        context: ErrorContext::stage("metadata"),
                        message: error.to_string(),
                    }
                })?;
            Ok(BorrowedMetadataRecord {
                doc_id,
                contract_id: contract.id,
                chain_id: contract.chain_id,
                token_id,
                content,
            })
        });
        let mut counters = StageCounters::default();
        let mut contracts = select_anchors_from_sorted_records(
            records,
            metadata.len(),
            &evm_chains,
            self.config.metadata_anchor_tokens,
            &mut counters,
            &self.progress,
        )?;
        let guard = &self.config.metadata_guard_parameters;
        let templates = build_template_fingerprints_with_progress(
            &contracts,
            TemplateGuard {
                min_anchor_documents: guard.min_anchor_documents,
                stable_value_min_anchors: guard.stable_value_min_anchors,
                stable_value_support_ratio: guard.stable_value_support_ratio,
            },
            &mut counters,
            &self.progress,
        )?;
        self.progress.begin_phase(
            "release_metadata_template_scratch",
            u64::try_from(contracts.len()).ok(),
        );
        dedup_engine::metadata::release_template_scratch(&mut contracts);
        self.progress
            .advance(u64::try_from(contracts.len()).unwrap_or(u64::MAX));
        self.progress.check_cancelled("metadata")?;
        Ok((contracts, templates, counters))
    }

    fn save_hits(
        &self,
        stage: &str,
        sink: &BitmapHitSink,
        upstream: &ArtifactManifest,
        counters: &StageCounters,
    ) -> Result<(), DedupError> {
        let path = self.hits_path(stage);
        recover_incomplete_artifact(&path)?;
        if path.exists() {
            let manifest = validate_artifact(&path)?;
            if manifest.logical_input_digest == upstream.logical_input_digest
                && manifest.configuration_digest == self.config_digest
            {
                return Ok(());
            }
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("hit_artifact"),
                message: format!("existing {stage} hit artifact is incompatible"),
            });
        }
        let mut artifact = ArtifactWriter::new(
            &path,
            ArtifactManifest {
                schema_version: 1,
                stage: format!("{stage}_hits"),
                logical_input_digest: upstream.logical_input_digest.clone(),
                configuration_digest: self.config_digest.clone(),
                upstream_checksums: upstream.data_checksums.clone(),
                data_checksums: BTreeMap::new(),
            },
        )?;
        let mut descriptors = Vec::new();
        for (index, ((dimension, scope, kind), bitmap)) in sink.entries().enumerate() {
            let file = format!("bitmap-{index:05}.bin");
            bitmap.serialize_into(artifact.create_data_file(&file)?)?;
            descriptors.push(HitDescriptor {
                dimension: *dimension,
                scope: *scope,
                kind: *kind,
                file,
            });
        }
        artifact
            .create_data_file("index.json")?
            .write_all(&serde_json::to_vec(&descriptors).map_err(json_error)?)?;
        artifact
            .create_data_file("counters.json")?
            .write_all(&serde_json::to_vec(counters).map_err(json_error)?)?;
        artifact.commit()?;
        Ok(())
    }

    fn restore_completed_hits(
        &self,
        stage: &str,
        upstream: &ArtifactManifest,
    ) -> Result<bool, DedupError> {
        let path = self.hits_path(stage);
        if !path.exists() {
            return Ok(false);
        }
        recover_incomplete_artifact(&path)?;
        if !path.exists() {
            return Ok(false);
        }
        let manifest = validate_artifact(&path)?;
        if manifest.logical_input_digest != upstream.logical_input_digest
            || manifest.configuration_digest != self.config_digest
        {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("hit_artifact"),
                message: format!("existing {stage} hit artifact is incompatible"),
            });
        }
        let counters: StageCounters =
            serde_json::from_slice(&fs::read(path.join("counters.json"))?).map_err(json_error)?;
        self.progress
            .begin_phase("reuse_committed_hit_artifact", Some(1));
        let metrics_path = self.run_dir.join(format!("{stage}-metrics.json"));
        if !metrics_path.is_file() {
            self.write_stage_metrics(stage, &counters)?;
        }
        self.progress.advance(1);
        Ok(true)
    }

    fn load_hits(
        &self,
        stage: &str,
        upstream: &ArtifactManifest,
    ) -> Result<BitmapHitSink, DedupError> {
        let path = self.hits_path(stage);
        let manifest = validate_artifact(&path)?;
        if manifest.logical_input_digest != upstream.logical_input_digest
            || manifest.configuration_digest != self.config_digest
        {
            return Err(DedupError::ArtifactMismatch {
                context: ErrorContext::stage("hit_artifact"),
                message: format!("{stage} hit artifact is incompatible"),
            });
        }
        let descriptors: Vec<HitDescriptor> =
            serde_json::from_slice(&fs::read(path.join("index.json"))?).map_err(|error| {
                DedupError::ArtifactMismatch {
                    context: ErrorContext::stage("hit_artifact"),
                    message: error.to_string(),
                }
            })?;
        let mut sink = BitmapHitSink::new(1)?;
        for descriptor in descriptors {
            let bitmap = RoaringTreemap::deserialize_from(File::open(path.join(descriptor.file))?)?;
            sink.insert_bitmap(
                descriptor.dimension,
                descriptor.scope,
                descriptor.kind,
                bitmap,
            );
        }
        Ok(sink)
    }

    fn write_stage_metrics(&self, stage: &str, counters: &StageCounters) -> Result<(), DedupError> {
        fs::create_dir_all(&self.run_dir)?;
        let path = self.run_dir.join(format!("{stage}-metrics.json"));
        let mut metrics = serde_json::to_value(counters).map_err(json_error)?;
        let fields = metrics
            .as_object_mut()
            .ok_or_else(|| invariant("stage counters did not serialize as an object"))?;
        fields.insert(
            "elapsed_seconds".to_owned(),
            serde_json::json!(self.progress.stage_elapsed_seconds()),
        );
        fields.insert(
            "resident_memory_bytes".to_owned(),
            serde_json::json!(self.progress.resident_memory_bytes()),
        );
        fs::write(
            path,
            serde_json::to_vec_pretty(&metrics).map_err(json_error)?,
        )?;
        Ok(())
    }

    fn entities_path(&self) -> PathBuf {
        self.run_dir.join("entities")
    }

    fn spill_root(&self, stage: &'static str) -> Result<PathBuf, DedupError> {
        self.spill_volumes(stage)?
            .into_iter()
            .next()
            .map(|volume| volume.root)
            .ok_or_else(|| DedupError::InvalidInput {
                context: ErrorContext::stage(stage),
                message: "at least one temporary volume is required".to_owned(),
            })
    }

    fn spill_volumes(&self, stage: &'static str) -> Result<Vec<SpillVolume>, DedupError> {
        let namespace = self
            .config_digest
            .get(..16)
            .ok_or_else(|| invariant("configuration digest is shorter than 16 bytes"))?;
        let profile: serde_json::Value =
            serde_json::from_slice(&fs::read(self.run_dir.join("hardware_profile.json"))?)
                .map_err(json_error)?;
        let calibrations: Vec<StorageCalibration> = serde_json::from_value(
            profile
                .get("storage_calibration")
                .cloned()
                .ok_or_else(|| invariant("hardware profile has no storage calibration"))?,
        )
        .map_err(json_error)?;
        self.config
            .temporary_volumes
            .iter()
            .map(|configured| {
                let calibration = calibrations
                    .iter()
                    .find(|value| Path::new(&value.volume) == Path::new(configured))
                    .ok_or_else(|| DedupError::ArtifactMismatch {
                        context: ErrorContext::stage(stage),
                        message: format!(
                            "temporary volume {configured} has no persisted calibration"
                        ),
                    })?;
                let capacity_units = calibration.free_bytes.div_ceil(1024 * 1024 * 1024).max(1);
                let throughput_units = calibration
                    .read_bytes_per_second
                    .min(calibration.write_bytes_per_second)
                    .div_ceil(1024 * 1024)
                    .max(1);
                SpillVolume::new(
                    Path::new(configured)
                        .join("dedup-spill")
                        .join(namespace)
                        .join(stage),
                    capacity_units.saturating_mul(throughput_units),
                )
            })
            .collect()
    }

    fn hits_path(&self, stage: &str) -> PathBuf {
        self.run_dir.join(format!("{stage}-hits"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HitDescriptor {
    dimension: Dimension,
    scope: ScopeId,
    kind: EntityKind,
    file: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StorageCalibration {
    volume: String,
    bytes: u64,
    #[serde(default)]
    free_bytes: u64,
    write_bytes_per_second: u64,
    read_bytes_per_second: u64,
}

fn build_statistics(
    sink: &BitmapHitSink,
    entities: &MappedEntityObjects,
    chain_count: usize,
) -> Result<Vec<StatisticsRow>, DedupError> {
    let mut totals = BTreeMap::new();
    for index in 0..chain_count {
        totals.insert(chain_id(index)?, (0_u64, 0_u64));
    }
    for contract in entities.contracts.iter() {
        let contract = contract?;
        let total = totals
            .get_mut(&contract.chain_id)
            .ok_or_else(|| invariant("contract has unknown chain"))?;
        total.0 += 1;
        total.1 = total
            .1
            .checked_add(contract.nft_count)
            .ok_or(DedupError::CounterOverflow {
                counter: "report_total_nfts",
            })?;
    }
    let bitmaps: BTreeMap<_, _> = sink.entries().map(|(key, bitmap)| (*key, bitmap)).collect();
    let all_chains = (0..chain_count)
        .map(chain_id)
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows = Vec::new();
    for dimension in [
        Dimension::Name,
        Dimension::TokenUri,
        Dimension::ImageUri,
        Dimension::Metadata,
    ] {
        let kind = if matches!(dimension, Dimension::TokenUri | Dimension::ImageUri) {
            EntityKind::Nft
        } else {
            EntityKind::Contract
        };
        for primary in &all_chains {
            let mut scopes = vec![ScopeId::Intra(*primary), ScopeId::CrossSummary(*primary)];
            scopes.extend(
                all_chains
                    .iter()
                    .copied()
                    .filter(|secondary| secondary != primary)
                    .map(|secondary| ScopeId::Matrix {
                        primary: *primary,
                        secondary,
                    }),
            );
            for scope in scopes {
                let empty = RoaringTreemap::new();
                let bitmap = bitmaps
                    .get(&(dimension, scope, kind))
                    .copied()
                    .unwrap_or(&empty);
                let (duplicate_contract_count, duplicate_nft_count) =
                    bitmap_counts(bitmap, kind, entities)?;
                let (total_contracts, total_nfts) = totals[primary];
                rows.push(StatisticsRow {
                    dimension,
                    subtype: dimension_name(dimension).to_owned(),
                    scope: match scope {
                        ScopeId::Intra(_) => "intra_chain",
                        ScopeId::CrossSummary(_) => "cross_chain_summary",
                        ScopeId::Matrix { .. } => "chain_matrix",
                    }
                    .to_owned(),
                    primary_chain: *primary,
                    secondary_chain: match scope {
                        ScopeId::Matrix { secondary, .. } => Some(secondary),
                        _ => None,
                    },
                    total_contracts,
                    total_nfts,
                    duplicate_contract_count,
                    duplicate_nft_count,
                    is_approximate: dimension == Dimension::Metadata,
                    run_status: "complete".to_owned(),
                });
            }
        }
    }
    rows.sort_by_key(|row| {
        (
            row.primary_chain,
            row.secondary_chain,
            row.scope.clone(),
            row.dimension,
        )
    });
    Ok(rows)
}

fn bitmap_counts(
    bitmap: &RoaringTreemap,
    kind: EntityKind,
    entities: &MappedEntityObjects,
) -> Result<(u64, u64), DedupError> {
    match kind {
        EntityKind::Contract => {
            let nft_count = bitmap.iter().try_fold(0_u64, |total, id| {
                let contract = entities
                    .contracts
                    .get(id)
                    .map_err(|_| invariant("hit references missing contract"))?;
                total
                    .checked_add(contract.nft_count)
                    .ok_or(DedupError::CounterOverflow {
                        counter: "report_duplicate_nfts",
                    })
            })?;
            Ok((bitmap.len(), nft_count))
        }
        EntityKind::Nft => {
            let mut duplicate_contracts = 0_u64;
            let mut previous_contract = None;
            for id in bitmap.iter() {
                let nft = entities
                    .nfts
                    .get(id)
                    .map_err(|_| invariant("hit references missing NFT"))?;
                if previous_contract != Some(nft.contract_id) {
                    duplicate_contracts =
                        duplicate_contracts
                            .checked_add(1)
                            .ok_or(DedupError::CounterOverflow {
                                counter: "report_duplicate_contracts",
                            })?;
                    previous_contract = Some(nft.contract_id);
                }
            }
            Ok((duplicate_contracts, bitmap.len()))
        }
    }
}

fn write_csv(
    path: PathBuf,
    rows: impl IntoIterator<Item = StatisticsRow>,
) -> Result<(), DedupError> {
    let mut writer = csv::Writer::from_path(path).map_err(csv_error)?;
    for row in rows {
        writer.serialize(row).map_err(csv_error)?;
    }
    writer.flush()?;
    Ok(())
}

fn validate_config(config: &RunConfig) -> Result<(), DedupError> {
    if config.chains.is_empty()
        || config.input_files.is_empty()
        || config.temporary_volumes.is_empty()
        || config.output_dir.trim().is_empty()
        || config
            .input_files
            .iter()
            .chain(&config.temporary_volumes)
            .any(|path| path.trim().is_empty())
        || !(0.0..=100.0).contains(&config.name_threshold)
        || !(0.0..=1.0).contains(&config.metadata_content_threshold)
        || config.metadata_anchor_tokens == 0
        || config.work_budgets.name_scored_candidates == 0
        || config.work_budgets.metadata_prefilter_pairs == 0
        || config.work_budgets.metadata_verify_pairs == 0
        || !(0.0..=1.0).contains(&config.quality_gate.metadata_recall)
        || config.quality_gate.minimum_positive_pairs == 0
    {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("config"),
            message: "paths, thresholds, anchors, quality gates or budgets are invalid".to_owned(),
        });
    }
    let unique: BTreeSet<_> = config
        .chains
        .iter()
        .map(|chain| chain.trim().to_lowercase())
        .collect();
    if unique.len() != config.chains.len()
        || config
            .evm_chains
            .iter()
            .any(|chain| !unique.contains(&chain.trim().to_lowercase()))
    {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("config"),
            message: "chain configuration contains duplicates or unknown EVM chains".to_owned(),
        });
    }
    let prefilter = &config.metadata_prefilter_parameters;
    let guard = &config.metadata_guard_parameters;
    if !(0.0..=1.0).contains(&prefilter.template_jaccard_threshold)
        || !(0.0..=1.0).contains(&prefilter.target_candidate_recall)
        || prefilter.derived_lsh_shape().is_none()
        || prefilter.neighbors_per_target_chain == 0
        || prefilter.max_candidates_per_target_chain == 0
        || prefilter.max_outgoing_candidates_per_contract == 0
        || prefilter.exact_bucket_size_cap == 0
        || guard.min_anchor_documents == 0
        || guard.stable_value_min_anchors == 0
        || !(0.0..=1.0).contains(&guard.stable_value_support_ratio)
    {
        return Err(DedupError::InvalidInput {
            context: ErrorContext::stage("config"),
            message: "metadata prefilter or guard parameters are invalid".to_owned(),
        });
    }
    Ok(())
}

fn effective_workers(requested: usize, available: u64) -> u64 {
    if requested == 0 {
        available
    } else {
        u64::try_from(requested).unwrap_or(u64::MAX).min(available)
    }
}

fn radix_memory_bytes(memory: &MemoryBudget, open_files: usize) -> u64 {
    let minimum = u64::try_from(open_files)
        .unwrap_or(u64::MAX)
        .saturating_mul(4 * 1024)
        .saturating_add(32);
    (memory.stage_limit() / 8)
        .clamp(minimum, 256 * 1024 * 1024)
        .min(memory.stage_limit())
}

fn read_json_value(path: &Path) -> Result<serde_json::Value, DedupError> {
    serde_json::from_slice(&fs::read(path)?).map_err(json_error)
}

struct PlatformWorkerSetup {
    placements: Vec<dedup_linux::WorkerPlacement>,
    enforce_binding: bool,
}

impl WorkerThreadSetup for PlatformWorkerSetup {
    fn setup(&self, worker_index: usize) -> Result<(), DedupError> {
        if !self.enforce_binding {
            return Ok(());
        }
        let placement =
            self.placements
                .get(worker_index)
                .ok_or_else(|| DedupError::InvariantViolation {
                    context: ErrorContext::stage("entity"),
                    message: format!("missing placement for Parquet worker {worker_index}"),
                })?;
        let controller = dedup_linux::NativePlatformController;
        dedup_linux::PlatformController::set_current_thread_affinity(
            &controller,
            &[placement.logical_cpu],
        )
        .and_then(|()| {
            dedup_linux::PlatformController::set_preferred_numa_node(
                &controller,
                placement.numa_node,
            )
        })
        .map_err(platform_error)
    }
}

fn resolve(base: &Path, configured: &str) -> PathBuf {
    let path = Path::new(configured);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn hit_capacity(objects: usize, chains: usize) -> Result<usize, DedupError> {
    objects
        .checked_mul(chains.saturating_add(2))
        .and_then(|value| value.checked_mul(2))
        .map(|value| value.max(1))
        .ok_or(DedupError::ResourceBudgetExceeded {
            context: ErrorContext::stage("hit_sink"),
            requested: u64::MAX,
        })
}

fn entity_upper_bound(objects: usize) -> Result<u64, DedupError> {
    u64::try_from(objects)
        .map(|value| value.max(1))
        .map_err(|_| DedupError::ResourceBudgetExceeded {
            context: ErrorContext::stage("hit_sink"),
            requested: u64::MAX,
        })
}

fn radix_volume_plan(volumes: &[SpillVolume]) -> Vec<serde_json::Value> {
    volumes
        .iter()
        .map(|volume| {
            serde_json::json!({
                "root": volume.root,
                "weight": volume.weight,
            })
        })
        .collect()
}

fn chain_id(index: usize) -> Result<ChainId, DedupError> {
    u16::try_from(index)
        .map(ChainId::new)
        .map_err(|_| DedupError::InvalidInput {
            context: ErrorContext::stage("config"),
            message: "too many chains".to_owned(),
        })
}

fn invariant(message: &str) -> DedupError {
    DedupError::InvariantViolation {
        context: ErrorContext::stage("pipeline"),
        message: message.to_owned(),
    }
}

fn json_error(error: serde_json::Error) -> DedupError {
    DedupError::InvariantViolation {
        context: ErrorContext::stage("json"),
        message: error.to_string(),
    }
}

fn csv_error(error: csv::Error) -> DedupError {
    DedupError::InvariantViolation {
        context: ErrorContext::stage("csv"),
        message: error.to_string(),
    }
}

fn dimension_name(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::Name => "name",
        Dimension::TokenUri => "token_uri",
        Dimension::ImageUri => "image_uri",
        Dimension::Metadata => "metadata",
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn calibrate_volume(volume: &Path) -> Result<StorageCalibration, DedupError> {
    let path = volume.join(format!(".dedup-calibration-{}.bin", std::process::id()));
    let bytes = vec![0x5a; 1024 * 1024];
    let result = (|| {
        let write_start = std::time::Instant::now();
        let mut file = File::create(&path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        let write_elapsed = write_start.elapsed();

        let read_start = std::time::Instant::now();
        let mut file = File::open(&path)?;
        let mut read = vec![0; bytes.len()];
        file.read_exact(&mut read)?;
        let read_elapsed = read_start.elapsed();
        if read != bytes {
            return Err(invariant("temporary-volume calibration read mismatch"));
        }
        Ok(StorageCalibration {
            volume: volume.to_string_lossy().into_owned(),
            bytes: bytes.len() as u64,
            free_bytes: fs2::available_space(volume)?,
            write_bytes_per_second: throughput(bytes.len(), write_elapsed),
            read_bytes_per_second: throughput(bytes.len(), read_elapsed),
        })
    })();
    if path.exists() {
        fs::remove_file(path)?;
    }
    result
}

fn throughput(bytes: usize, elapsed: std::time::Duration) -> u64 {
    let nanos = elapsed.as_nanos().max(1);
    u64::try_from((bytes as u128).saturating_mul(1_000_000_000) / nanos).unwrap_or(u64::MAX)
}

fn platform_error(error: dedup_linux::PlatformError) -> DedupError {
    DedupError::PlatformCapabilityMissing {
        capability: error.to_string(),
    }
}
use crate::progress::{ProgressMode, ProgressReporter};

#[cfg(test)]
mod tests {
    use super::*;
    use dedup_model::{Contract, ContractId, Nft, NftId, PersistedEntityArtifacts, StringId};

    #[test]
    fn mapped_report_counts_nft_hits_by_contiguous_contract_without_a_set() {
        let persisted = PersistedEntityArtifacts {
            strings: vec![b"a".to_vec(), b"b".to_vec(), b"0".to_vec()],
            entities: EntityArtifacts {
                contracts: vec![
                    Contract {
                        id: ContractId::new(0),
                        chain_id: ChainId::new(0),
                        address_ref: StringId::new(0),
                        name_ref: None,
                        first_nft_id: NftId::new(0),
                        nft_count: 2,
                    },
                    Contract {
                        id: ContractId::new(1),
                        chain_id: ChainId::new(0),
                        address_ref: StringId::new(1),
                        name_ref: None,
                        first_nft_id: NftId::new(2),
                        nft_count: 2,
                    },
                ],
                nfts: (0..4)
                    .map(|id| Nft {
                        id: NftId::new(id),
                        contract_id: ContractId::new(id / 2),
                        token_id_ref: StringId::new(2),
                        token_uri_ref: None,
                        image_uri_ref: None,
                        has_metadata: false,
                    })
                    .collect(),
            },
            metadata_by_nft: Vec::new(),
        };
        let directory = tempfile::tempdir().unwrap();
        let artifact = directory.path().join("entities");
        dedup_storage::write_entity_artifact(
            &artifact,
            &persisted,
            "input".to_owned(),
            "config".to_owned(),
        )
        .unwrap();
        let budget = MemoryBudget::new(1024 * 1024, 1024 * 1024);
        let entities = MappedEntityObjects::open(&artifact, &budget, 8192).unwrap();

        let nft_hits = RoaringTreemap::from_iter([0, 1, 3]);
        assert_eq!(
            bitmap_counts(&nft_hits, EntityKind::Nft, &entities).unwrap(),
            (2, 3)
        );
        let contract_hits = RoaringTreemap::from_iter([0, 1]);
        assert_eq!(
            bitmap_counts(&contract_hits, EntityKind::Contract, &entities).unwrap(),
            (2, 4)
        );
    }
}
