use analysis::input::load_resident_store;
use analysis::pipeline::CpuExecutor;
use analysis::progress::Progress;
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;

#[test]
fn metadata_pass_keeps_first_eight_valid_tokens_in_logical_order() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("fixture.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("chain", DataType::Utf8, false),
        Field::new("contract_address", DataType::Utf8, false),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("name_norm", DataType::Utf8, true),
        Field::new("token_uri_norm", DataType::Utf8, true),
        Field::new("image_uri_norm", DataType::Utf8, true),
        Field::new("metadata_json", DataType::Utf8, true),
    ]));
    let tokens = (0..=10)
        .rev()
        .map(|token| token.to_string())
        .chain(std::iter::once("0001".to_owned()))
        .collect::<Vec<_>>();
    let contracts = std::iter::repeat_n(Some("0xabc".to_owned()), tokens.len() - 1)
        .chain(std::iter::once(Some("0xAbC".to_owned())));
    let metadata = tokens
        .iter()
        .enumerate()
        .map(|(row, token)| {
            if token == "0" {
                Some("{invalid".to_owned())
            } else if row + 1 == tokens.len() {
                Some(r#"{"winner":"late"}"#.to_owned())
            } else {
                Some(format!(r#"{{"token":{token}}}"#))
            }
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            strings(std::iter::repeat_n(
                Some("ethereum".to_owned()),
                tokens.len(),
            )),
            strings(contracts),
            strings(tokens.iter().cloned().map(Some)),
            strings(std::iter::repeat_n(
                Some("fixture".to_owned()),
                tokens.len(),
            )),
            strings(std::iter::repeat_n(None, tokens.len())),
            strings(std::iter::repeat_n(None, tokens.len())),
            strings(metadata),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let executor = CpuExecutor::new(2).unwrap();
    let progress = Progress::default();
    let store =
        load_resident_store(&[path], 8, 128, 1024 * 1024 * 1024, &executor, &progress).unwrap();
    assert_eq!(store.quality.logical_nfts, 11);
    assert_eq!(store.quality.duplicate_rows, 1);
    assert_eq!(store.quality.invalid_metadata, 1);
    let features = store.metadata_features.unwrap();
    let profile = features.contract_profiles[0].unwrap();
    let anchor_tokens = features
        .profile_anchors(profile)
        .iter()
        .map(|anchor| features.anchor_tokens.get(anchor.token_id_id.0))
        .collect::<Vec<_>>();
    assert_eq!(anchor_tokens, ["1", "2", "3", "4", "5", "6", "7", "8"]);
    let first = features.profile_anchors(profile)[0];
    assert_eq!(
        features.documents.get(first.metadata_id.0),
        r#"{"token":1}"#
    );
    assert_eq!(progress.snapshot().completed_row_groups, 6);
}

fn strings(values: impl IntoIterator<Item = Option<String>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values)) as ArrayRef
}
