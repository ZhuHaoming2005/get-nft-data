use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

fn write_parquet(path: &Path, rows: &[[&str; 7]]) {
    let schema = Arc::new(Schema::new(
        [
            "chain",
            "contract_address",
            "token_id",
            "name_norm",
            "token_uri_norm",
            "image_uri_norm",
            "metadata_json",
        ]
        .into_iter()
        .map(|name| Field::new(name, DataType::Utf8, false))
        .collect::<Vec<_>>(),
    ));
    let mut columns = vec![Vec::new(); 7];
    for row in rows {
        for (i, value) in row.iter().enumerate() {
            columns[i].push((*value).to_owned());
        }
    }
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|values| Arc::new(StringArray::from(values)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn all_writes_summary_files() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    write_parquet(
        &input,
        &[
            [
                "ethereum",
                "0xa",
                "1",
                "collection",
                "ipfs://shared/1",
                "",
                r#"{"collection":{"name":"shared"},"name":"t1"}"#,
            ],
            [
                "ethereum",
                "0xb",
                "1",
                "collection",
                "ipfs://shared/1",
                "",
                r#"{"collection":{"name":"shared"},"name":"t1"}"#,
            ],
            [
                "base",
                "0xc",
                "1",
                "collection",
                "ipfs://other/1",
                "",
                r#"{"collection":{"name":"shared"},"name":"t1"}"#,
            ],
        ],
    );
    let out = temp.path().join("out");
    let exe = env!("CARGO_BIN_EXE_dedup2");
    let status = Command::new(exe)
        .args([
            "all",
            "--input",
            input.to_str().unwrap(),
            "--output-dir",
            out.to_str().unwrap(),
            "--chains",
            "ethereum,base",
            "--evm-chains",
            "ethereum,base",
            "--progress",
            "off",
            "--metadata-anchors",
            "2",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(out.join("summary.csv").is_file());
    assert!(out.join("chain_matrix.csv").is_file());
    assert!(out.join("run_manifest.json").is_file());
}
