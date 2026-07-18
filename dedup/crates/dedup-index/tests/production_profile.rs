use dedup_index::{EntityBuilder, EntityExecutionConfig, SpillVolume};
use dedup_model::{ExecutionMode, InputRow, MetadataSourceValidator, SourceOrder};

#[derive(Clone, Copy)]
struct RejectMetadata;

impl MetadataSourceValidator for RejectMetadata {
    fn is_valid_metadata(&self, _content: &str) -> bool {
        false
    }
}

#[test]
fn one_hundred_thousand_nft_contract_uses_bounded_external_runs() {
    const NFTS: usize = 100_000;
    let directory = tempfile::tempdir().unwrap();
    let second_directory = tempfile::tempdir().unwrap();
    let mut builder = EntityBuilder::new_with_execution(
        ["ethereum".to_owned()],
        ["ethereum".to_owned()],
        64,
        RejectMetadata,
        EntityExecutionConfig::new(
            ExecutionMode::External,
            Some(directory.path().to_owned()),
            4_096,
            4,
        )
        .unwrap()
        .with_spill_volumes(vec![
            SpillVolume::new(directory.path(), 1).unwrap(),
            SpillVolume::new(second_directory.path(), 1).unwrap(),
        ])
        .unwrap(),
    )
    .unwrap();
    for token in (0..NFTS).rev() {
        builder
            .push(InputRow {
                chain: "ethereum".to_owned(),
                contract_address: "0xabc".to_owned(),
                token_id: token.to_string(),
                name_norm: "collection".to_owned(),
                token_uri_norm: String::new(),
                image_uri_norm: String::new(),
                metadata_json: String::new(),
                source_order: SourceOrder::new(0, token as u64),
            })
            .unwrap();
    }
    let result = builder.finish().unwrap();
    assert_eq!(result.artifacts.contracts.len(), 1);
    assert_eq!(result.artifacts.contracts[0].nft_count, NFTS as u64);
    assert_eq!(result.artifacts.nfts.len(), NFTS);
    assert_eq!(
        result
            .strings
            .resolve(result.artifacts.nfts[0].token_id_ref)
            .unwrap(),
        b"0"
    );
    assert_eq!(
        result
            .strings
            .resolve(result.artifacts.nfts[NFTS - 1].token_id_ref)
            .unwrap(),
        b"99999"
    );
    assert!(result.external_handle_spill_bytes > (NFTS as u64) * 80);
    assert!(result.external_handle_touches > NFTS as u64);
    assert_eq!(result.external_volumes_used, 2);
    assert_eq!(result.metadata_spill_bytes, 0);
}
