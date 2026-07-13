use super::*;

use std::io::Write;

use crate::replace_file_atomically;

pub const SUMMARY_MANIFEST_FILE_NAME: &str = "summary.manifest.json";
const OUTPUT_GENERATION_SCHEMA_VERSION: u32 = 1;
const OUTPUT_HASH_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct OutputGenerationManifest {
    schema_version: u32,
    artifacts: OutputGenerationArtifacts,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct OutputGenerationArtifacts {
    #[serde(rename = "summary.json")]
    summary_json: OutputArtifactFingerprint,
    #[serde(rename = "summary.csv")]
    summary_csv: OutputArtifactFingerprint,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct OutputArtifactFingerprint {
    size: u64,
    sha256: String,
}

pub(crate) fn summary_row(spec: SummarySpec<'_>, groups: GroupSummary) -> SummaryRow {
    SummaryRow {
        field_name: spec.field_name.to_string(),
        scope: spec.scope.to_string(),
        primary_chain: spec.primary_chain.to_string(),
        secondary_chain: spec.secondary_chain.to_string(),
        threshold: spec.threshold,
        match_mode: spec.match_mode.to_string(),
        metric: spec.metric.to_string(),
        total_contracts: spec.total_contracts,
        total_nfts: spec.total_nfts,
        group_count: groups.group_count,
        duplicate_contract_count: groups.duplicate_contract_count,
        duplicate_nft_count: groups.duplicate_nft_count,
        duplicate_contract_ratio: pct(groups.duplicate_contract_count, spec.total_contracts),
        duplicate_nft_ratio: pct(groups.duplicate_nft_count, spec.total_nfts),
        group_size_ge_2_count: groups.group_size_ge_2_count,
        group_size_gt_2_count: groups.group_size_gt_2_count,
    }
}

pub(crate) fn write_outputs(
    report: &AnalysisReport,
    output_dir: &Path,
) -> Result<(), AnalysisError> {
    let json_path = output_dir.join("summary.json");
    let json_partial = output_dir.join("summary.json.partial");
    let mut json_file = fs::File::create(&json_partial)?;
    serde_json::to_writer_pretty(&mut json_file, report)?;
    json_file.flush()?;
    json_file.sync_all()?;
    drop(json_file);

    let csv_path = output_dir.join("summary.csv");
    let csv_partial = output_dir.join("summary.csv.partial");
    let mut file = fs::File::create(&csv_partial)?;
    writeln!(
        file,
        "field_name,scope,primary_chain,secondary_chain,threshold,match_mode,metric,total_contracts,total_nfts,group_count,duplicate_contract_count,duplicate_nft_count,duplicate_contract_ratio,duplicate_nft_ratio,group_size_ge_2_count,group_size_gt_2_count"
    )?;
    for row in &report.summary_rows {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{}",
            csv_cell(&row.field_name),
            csv_cell(&row.scope),
            csv_cell(&row.primary_chain),
            csv_cell(&row.secondary_chain),
            row.threshold
                .map(|value| format!("{value:.6}"))
                .unwrap_or_default(),
            csv_cell(&row.match_mode),
            csv_cell(&row.metric),
            row.total_contracts,
            row.total_nfts,
            row.group_count,
            row.duplicate_contract_count,
            row.duplicate_nft_count,
            row.duplicate_contract_ratio,
            row.duplicate_nft_ratio,
            row.group_size_ge_2_count,
            row.group_size_gt_2_count,
        )?;
    }
    file.flush()?;
    file.sync_all()?;
    drop(file);

    let manifest = OutputGenerationManifest {
        schema_version: OUTPUT_GENERATION_SCHEMA_VERSION,
        artifacts: OutputGenerationArtifacts {
            summary_json: fingerprint_output_artifact(&json_partial)?,
            summary_csv: fingerprint_output_artifact(&csv_partial)?,
        },
    };
    let manifest_path = output_dir.join(SUMMARY_MANIFEST_FILE_NAME);
    let manifest_partial = output_dir.join("summary.manifest.json.partial");
    let mut manifest_file = fs::File::create(&manifest_partial)?;
    serde_json::to_writer_pretty(&mut manifest_file, &manifest)?;
    manifest_file.flush()?;
    manifest_file.sync_all()?;
    drop(manifest_file);

    replace_file_atomically(&json_partial, &json_path)?;
    replace_file_atomically(&csv_partial, &csv_path)?;
    // The two public summaries cannot be renamed as one cross-platform
    // operation. Publish their fingerprint manifest last as the logical commit
    // marker; until this succeeds, the previous marker cannot validate a mixed
    // or otherwise incomplete generation.
    replace_file_atomically(&manifest_partial, &manifest_path)?;
    Ok(())
}

pub fn validate_output_generation(output_dir: &Path) -> Result<(), AnalysisError> {
    let manifest_path = output_dir.join(SUMMARY_MANIFEST_FILE_NAME);
    let manifest_bytes = fs::read(&manifest_path).map_err(|error| {
        AnalysisError::InvalidData(format!(
            "cannot read output generation marker {}: {error}",
            manifest_path.display()
        ))
    })?;
    let manifest: OutputGenerationManifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.schema_version != OUTPUT_GENERATION_SCHEMA_VERSION {
        return Err(AnalysisError::InvalidData(format!(
            "unsupported output generation schema {}, expected {}",
            manifest.schema_version, OUTPUT_GENERATION_SCHEMA_VERSION
        )));
    }

    for (file_name, expected) in [
        ("summary.json", &manifest.artifacts.summary_json),
        ("summary.csv", &manifest.artifacts.summary_csv),
    ] {
        let path = output_dir.join(file_name);
        let actual = fingerprint_output_artifact(&path).map_err(|error| {
            AnalysisError::InvalidData(format!(
                "cannot validate committed output artifact {}: {error}",
                path.display()
            ))
        })?;
        if actual != *expected {
            return Err(AnalysisError::InvalidData(format!(
                "output artifact {file_name} does not match the committed generation"
            )));
        }
    }
    Ok(())
}

fn fingerprint_output_artifact(path: &Path) -> Result<OutputArtifactFingerprint, AnalysisError> {
    let (size, sha256) = crate::sha256_file(path, OUTPUT_HASH_BUFFER_BYTES)?;
    Ok(OutputArtifactFingerprint { size, sha256 })
}

pub(crate) fn pct(part: i64, total: i64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

pub(crate) fn parquet_input_sql(paths: &[PathBuf]) -> String {
    if paths.len() == 1 {
        parquet_sql_literal(&paths[0])
    } else {
        let values = paths
            .iter()
            .map(|path| parquet_sql_literal(path))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{values}]")
    }
}

/// Quote a filesystem path as a single-file DuckDB/Parquet SQL string literal.
/// Backslashes become `/`; single quotes are doubled for SQL escaping.
pub fn parquet_sql_literal(path: &Path) -> String {
    format!(
        "'{}'",
        sql_string(&path.display().to_string().replace('\\', "/"))
    )
}

pub(crate) fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn csv_cell(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
