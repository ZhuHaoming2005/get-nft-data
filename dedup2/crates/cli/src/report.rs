use dedup_core::{Dimension, EntityStore, MetadataStats, ScopeKind, SummaryAccumulator};
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::thread;
use std::time::Duration;
use tempfile::Builder;

type ReportError = Box<dyn std::error::Error + Send + Sync>;
const REPORT_FILES: [&str; 3] = ["summary.csv", "chain_matrix.csv", "run_manifest.json"];
const NAME_REPORT_FILES: [&str; 2] = ["name_summary.csv", "name_chain_matrix.csv"];
const URI_REPORT_FILES: [&str; 2] = ["uri_summary.csv", "uri_chain_matrix.csv"];

#[derive(Clone, Copy)]
pub enum ReportPartition {
    Name,
    Uri,
}

impl ReportPartition {
    fn files(self) -> &'static [&'static str; 2] {
        match self {
            Self::Name => &NAME_REPORT_FILES,
            Self::Uri => &URI_REPORT_FILES,
        }
    }

    fn dimensions(self) -> &'static [Dimension] {
        match self {
            Self::Name => &[Dimension::Name],
            Self::Uri => &[Dimension::TokenUri, Dimension::ImageUri],
        }
    }
}

#[derive(Serialize)]
struct RunManifest {
    inputs: Vec<String>,
    chains: Vec<String>,
    evm_chains: Vec<String>,
    rows_loaded: u64,
    contracts: usize,
    nfts: usize,
    interned_strings: usize,
    token_uri_postings: usize,
    image_uri_postings: usize,
    chain_totals: Vec<ChainTotalRow>,
    elapsed_secs: f64,
    stage_timings: Vec<StageTiming>,
    phase_timings: Vec<PhaseTiming>,
    name_threshold: f64,
    metadata_threshold: f64,
    metadata_anchors: usize,
    metadata_direct: Option<MetadataStats>,
}

#[derive(Clone, Serialize)]
pub struct StageTiming {
    pub stage: &'static str,
    pub elapsed_secs: f64,
}

#[derive(Clone, Serialize)]
pub struct PhaseTiming {
    pub stage: String,
    pub phase: String,
    pub elapsed_secs: f64,
}

#[derive(Serialize)]
struct ChainTotalRow {
    chain: String,
    contracts: u64,
    nfts: u64,
}

pub struct ReportRequest<'a> {
    pub store: &'a EntityStore,
    pub accumulator: &'a SummaryAccumulator,
    pub inputs: &'a [std::path::PathBuf],
    pub chains: &'a [String],
    pub evm_chains: &'a [String],
    pub name_threshold: f64,
    pub metadata_threshold: f64,
    pub metadata_anchors: usize,
    pub metadata_direct: Option<MetadataStats>,
    pub stage_timings: Vec<StageTiming>,
    pub phase_timings: Vec<PhaseTiming>,
    pub elapsed: Duration,
}

pub fn write_reports(output_dir: &Path, request: ReportRequest<'_>) -> Result<(), ReportError> {
    fs::create_dir_all(output_dir)?;
    let staging = Builder::new()
        .prefix(".dedup2-report-staging-")
        .tempdir_in(output_dir)?;
    let mut chain_totals: Vec<ChainTotalRow> = request
        .store
        .totals
        .iter()
        .map(|(id, totals)| ChainTotalRow {
            chain: request.store.chain_name(*id).to_owned(),
            contracts: totals.contracts,
            nfts: totals.nfts,
        })
        .collect();
    chain_totals.sort_by(|a, b| a.chain.cmp(&b.chain));
    let manifest = RunManifest {
        inputs: request
            .inputs
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        chains: request.chains.to_vec(),
        evm_chains: request.evm_chains.to_vec(),
        rows_loaded: request.store.rows_loaded,
        contracts: request.store.contracts.len(),
        nfts: request.store.nfts.len(),
        interned_strings: request.store.strings.len(),
        token_uri_postings: request.store.token_uri_postings.len(),
        image_uri_postings: request.store.image_uri_postings.len(),
        chain_totals,
        elapsed_secs: request.elapsed.as_secs_f64(),
        stage_timings: request.stage_timings,
        phase_timings: request.phase_timings,
        name_threshold: request.name_threshold,
        metadata_threshold: request.metadata_threshold,
        metadata_anchors: request.metadata_anchors,
        metadata_direct: request.metadata_direct,
    };
    thread::scope(|scope| -> Result<(), ReportError> {
        let summary = scope.spawn(|| {
            write_summary(
                staging.path(),
                REPORT_FILES[0],
                request.store,
                request.accumulator,
                None,
            )
        });
        let matrix = scope.spawn(|| {
            write_matrix(
                staging.path(),
                REPORT_FILES[1],
                request.store,
                request.accumulator,
                None,
            )
        });
        let manifest = scope.spawn(|| write_manifest(staging.path(), &manifest));
        summary
            .join()
            .map_err(|_| std::io::Error::other("summary writer panicked"))??;
        matrix
            .join()
            .map_err(|_| std::io::Error::other("matrix writer panicked"))??;
        manifest
            .join()
            .map_err(|_| std::io::Error::other("manifest writer panicked"))??;
        Ok(())
    })?;
    commit_reports(output_dir, staging.path(), &REPORT_FILES)
}

pub fn write_partition_reports(
    output_dir: &Path,
    store: &EntityStore,
    accumulator: &SummaryAccumulator,
    partition: ReportPartition,
) -> Result<(), ReportError> {
    fs::create_dir_all(output_dir)?;
    let staging = Builder::new()
        .prefix(".dedup2-partition-staging-")
        .tempdir_in(output_dir)?;
    let files = partition.files();
    let dimensions = partition.dimensions();
    thread::scope(|scope| -> Result<(), ReportError> {
        let summary = scope.spawn(|| {
            write_summary(
                staging.path(),
                files[0],
                store,
                accumulator,
                Some(dimensions),
            )
        });
        let matrix = scope.spawn(|| {
            write_matrix(
                staging.path(),
                files[1],
                store,
                accumulator,
                Some(dimensions),
            )
        });
        summary
            .join()
            .map_err(|_| std::io::Error::other("partition summary writer panicked"))??;
        matrix
            .join()
            .map_err(|_| std::io::Error::other("partition matrix writer panicked"))??;
        Ok(())
    })?;
    commit_reports(output_dir, staging.path(), files)
}

fn commit_reports(
    output_dir: &Path,
    staging_dir: &Path,
    report_files: &[&str],
) -> Result<(), ReportError> {
    for &name in report_files {
        if !staging_dir.join(name).is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("staged report is missing: {name}"),
            )
            .into());
        }
    }

    let backup = Builder::new()
        .prefix(".dedup2-report-backup-")
        .tempdir_in(output_dir)?;
    let mut backed_up = Vec::new();
    for &name in report_files {
        let final_path = output_dir.join(name);
        if final_path.exists() {
            if let Err(error) = fs::rename(&final_path, backup.path().join(name)) {
                let rollback = restore_reports(output_dir, backup.path(), &[], &backed_up);
                return commit_error("backing up previous reports", error, rollback, backup);
            }
            backed_up.push(name);
        }
    }

    let mut installed = Vec::new();
    for &name in report_files {
        if let Err(error) = fs::rename(staging_dir.join(name), output_dir.join(name)) {
            let rollback = restore_reports(output_dir, backup.path(), &installed, &backed_up);
            return commit_error("installing staged reports", error, rollback, backup);
        }
        installed.push(name);
    }
    Ok(())
}

fn restore_reports(
    output_dir: &Path,
    backup_dir: &Path,
    installed: &[&str],
    backed_up: &[&str],
) -> std::io::Result<()> {
    for name in installed.iter().rev() {
        let path = output_dir.join(name);
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    for name in backed_up.iter().rev() {
        fs::rename(backup_dir.join(name), output_dir.join(name))?;
    }
    Ok(())
}

fn commit_error(
    operation: &str,
    error: std::io::Error,
    rollback: std::io::Result<()>,
    backup: tempfile::TempDir,
) -> Result<(), ReportError> {
    match rollback {
        Ok(()) => Err(std::io::Error::other(format!(
            "failed while {operation}; previous report set was restored: {error}"
        ))
        .into()),
        Err(rollback_error) => {
            let recovery_dir = backup.keep();
            Err(std::io::Error::other(format!(
                "failed while {operation}: {error}; rollback also failed: {rollback_error}; \
                 previous files remain in {}",
                recovery_dir.display()
            ))
            .into())
        }
    }
}

fn write_manifest(output_dir: &Path, manifest: &RunManifest) -> Result<(), ReportError> {
    let mut file = File::create(output_dir.join("run_manifest.json"))?;
    serde_json::to_writer_pretty(&mut file, manifest)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn write_summary(
    output_dir: &Path,
    file_name: &str,
    store: &EntityStore,
    acc: &SummaryAccumulator,
    dimensions: Option<&[Dimension]>,
) -> Result<(), ReportError> {
    let mut writer = csv::Writer::from_path(output_dir.join(file_name))?;
    writer.write_record([
        "primary_chain",
        "scope",
        "dimension",
        "duplicate_contract_count",
        "duplicate_contract_ratio",
        "duplicate_nft_count",
        "duplicate_nft_ratio",
        "total_contracts",
        "total_nfts",
    ])?;
    let mut rows: Vec<_> = acc
        .counts()
        .iter()
        .filter(|(key, _)| {
            key.kind != ScopeKind::ChainMatrix
                && dimensions.is_none_or(|allowed| allowed.contains(&key.dimension))
        })
        .collect();
    rows.sort_by(|(a, _), (b, _)| {
        store
            .chain_name(a.primary_chain)
            .cmp(store.chain_name(b.primary_chain))
            .then(scope_name(&a.kind).cmp(scope_name(&b.kind)))
            .then(dimension_name(a.dimension).cmp(dimension_name(b.dimension)))
    });
    for (key, counts) in rows {
        let chain = store.chain_name(key.primary_chain);
        let totals = store
            .totals
            .get(&key.primary_chain)
            .cloned()
            .unwrap_or_default();
        let contract_ratio = ratio(counts.duplicate_contract_count, totals.contracts);
        let nft_ratio = ratio(counts.duplicate_nft_count, totals.nfts);
        writer.write_record([
            chain,
            scope_name(&key.kind),
            dimension_name(key.dimension),
            &counts.duplicate_contract_count.to_string(),
            &contract_ratio.to_string(),
            &counts.duplicate_nft_count.to_string(),
            &nft_ratio.to_string(),
            &totals.contracts.to_string(),
            &totals.nfts.to_string(),
        ])?;
    }
    writer.flush()?;
    Ok(())
}

fn write_matrix(
    output_dir: &Path,
    file_name: &str,
    store: &EntityStore,
    acc: &SummaryAccumulator,
    dimensions: Option<&[Dimension]>,
) -> Result<(), ReportError> {
    let mut writer = csv::Writer::from_path(output_dir.join(file_name))?;
    writer.write_record([
        "primary_chain",
        "secondary_chain",
        "dimension",
        "duplicate_contract_count",
        "duplicate_contract_ratio",
        "duplicate_nft_count",
        "duplicate_nft_ratio",
        "total_contracts",
        "total_nfts",
    ])?;
    let mut rows: Vec<_> = acc
        .counts()
        .iter()
        .filter(|(key, _)| {
            key.kind == ScopeKind::ChainMatrix
                && dimensions.is_none_or(|allowed| allowed.contains(&key.dimension))
        })
        .collect();
    rows.sort_by(|(a, _), (b, _)| {
        let a_sec = a
            .secondary_chain
            .map(|id| store.chain_name(id))
            .unwrap_or("");
        let b_sec = b
            .secondary_chain
            .map(|id| store.chain_name(id))
            .unwrap_or("");
        store
            .chain_name(a.primary_chain)
            .cmp(store.chain_name(b.primary_chain))
            .then(a_sec.cmp(b_sec))
            .then(dimension_name(a.dimension).cmp(dimension_name(b.dimension)))
    });
    for (key, counts) in rows {
        let primary = store.chain_name(key.primary_chain);
        let secondary = key
            .secondary_chain
            .map(|id| store.chain_name(id))
            .unwrap_or("");
        let totals = store
            .totals
            .get(&key.primary_chain)
            .cloned()
            .unwrap_or_default();
        let contract_ratio = ratio(counts.duplicate_contract_count, totals.contracts);
        let nft_ratio = ratio(counts.duplicate_nft_count, totals.nfts);
        writer.write_record([
            primary,
            secondary,
            dimension_name(key.dimension),
            &counts.duplicate_contract_count.to_string(),
            &contract_ratio.to_string(),
            &counts.duplicate_nft_count.to_string(),
            &nft_ratio.to_string(),
            &totals.contracts.to_string(),
            &totals.nfts.to_string(),
        ])?;
    }
    writer.flush()?;
    Ok(())
}

fn ratio(count: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        count as f64 / total as f64
    }
}

fn scope_name(kind: &ScopeKind) -> &'static str {
    match kind {
        ScopeKind::IntraChain => "intra_chain",
        ScopeKind::CrossChainSummary => "cross_chain_summary",
        ScopeKind::ChainMatrix => "chain_matrix",
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

#[cfg(test)]
mod tests {
    use super::{REPORT_FILES, commit_reports, ratio};
    use std::fs;

    #[test]
    fn ratio_uses_fraction_and_handles_zero_total() {
        assert_eq!(ratio(0, 0), 0.0);
        assert_eq!(ratio(1, 4), 0.25);
        assert_eq!(ratio(3, 2), 1.5);
    }

    #[test]
    fn incomplete_staging_keeps_previous_report_set() {
        let output = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir_in(output.path()).unwrap();
        for name in REPORT_FILES {
            fs::write(output.path().join(name), format!("old-{name}")).unwrap();
        }
        fs::write(staging.path().join(REPORT_FILES[0]), "new-summary").unwrap();
        fs::write(staging.path().join(REPORT_FILES[1]), "new-matrix").unwrap();

        assert!(commit_reports(output.path(), staging.path(), &REPORT_FILES).is_err());
        for name in REPORT_FILES {
            assert_eq!(
                fs::read_to_string(output.path().join(name)).unwrap(),
                format!("old-{name}")
            );
        }
    }

    #[test]
    fn complete_staging_replaces_the_whole_report_set() {
        let output = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir_in(output.path()).unwrap();
        for name in REPORT_FILES {
            fs::write(output.path().join(name), format!("old-{name}")).unwrap();
            fs::write(staging.path().join(name), format!("new-{name}")).unwrap();
        }

        commit_reports(output.path(), staging.path(), &REPORT_FILES).unwrap();
        for name in REPORT_FILES {
            assert_eq!(
                fs::read_to_string(output.path().join(name)).unwrap(),
                format!("new-{name}")
            );
        }
    }
}
