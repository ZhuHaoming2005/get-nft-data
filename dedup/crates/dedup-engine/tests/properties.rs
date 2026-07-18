use dedup_engine::uri::{UriExecutionConfig, run_uri, run_uri_with_config};
use dedup_model::{
    ChainId, Contract, ContractId, DedupError, EntityId, HitEvent, HitEventSink, Nft, NftId,
    StringId,
};
use std::collections::BTreeSet;

#[derive(Default)]
struct RecordingSink(BTreeSet<HitEvent>);

impl HitEventSink for RecordingSink {
    fn submit(&mut self, event: HitEvent) -> Result<(), DedupError> {
        self.0.insert(event);
        Ok(())
    }
}

#[test]
fn uri_external_mode_matches_resident_for_all_small_assignments() {
    let contracts: Vec<_> = (0..4_u32)
        .map(|id| Contract {
            id: ContractId::new(EntityId::from(id)),
            chain_id: ChainId::new(u16::try_from(id / 2).unwrap()),
            address_ref: StringId::new(EntityId::from(id)),
            name_ref: None,
            first_nft_id: NftId::new(EntityId::from(id)),
            nft_count: 1,
        })
        .collect();
    for assignment in 0..81_u32 {
        let mut value = assignment;
        let nfts: Vec<_> = (0..4_u32)
            .map(|id| {
                let uri = match value % 3 {
                    0 => None,
                    candidate => Some(StringId::new(EntityId::from(candidate))),
                };
                value /= 3;
                Nft {
                    id: NftId::new(EntityId::from(id)),
                    contract_id: ContractId::new(EntityId::from(id)),
                    token_id_ref: StringId::new(EntityId::from(id + 10)),
                    token_uri_ref: uri,
                    image_uri_ref: None,
                    has_metadata: false,
                }
            })
            .collect();
        let mut resident = RecordingSink::default();
        run_uri(&contracts, &nfts, &mut resident).unwrap();

        let directory = tempfile::tempdir().unwrap();
        let config = UriExecutionConfig::new(
            dedup_model::ExecutionMode::External,
            directory.path(),
            1,
            1,
            2,
            8,
        )
        .unwrap();
        let mut external = RecordingSink::default();
        let result = run_uri_with_config(&contracts, &nfts, &mut external, &config).unwrap();
        assert_eq!(external.0, resident.0, "assignment {assignment}");
        if nfts.iter().any(|nft| nft.token_uri_ref.is_some()) {
            assert_eq!(result.max_spill_reducer_buffered_members, 1);
        }
    }
}
