use dedup_engine::name::{NameEngineConfig, run_name};
use dedup_engine::uri::{UriExecutionConfig, run_uri_with_config};
use dedup_index::{MemoryBudget, StringDictionary};
use dedup_model::{
    ChainId, Contract, ContractId, DedupError, EntityId, ExecutionMode, HitEvent, HitEventSink,
    Nft, NftId, StringId,
};

#[derive(Default)]
struct CountingSink(u64);

impl HitEventSink for CountingSink {
    fn submit(&mut self, _event: HitEvent) -> Result<(), DedupError> {
        self.0 = self.0.checked_add(1).ok_or(DedupError::CounterOverflow {
            counter: "production_profile_hits",
        })?;
        Ok(())
    }
}

fn entity_id(value: usize) -> EntityId {
    EntityId::try_from(value).unwrap()
}

#[test]
fn one_hundred_thousand_identical_names_do_not_add_jaro_winkler_work() {
    const CONTRACTS: usize = 100_000;
    let mut strings = StringDictionary::new(8).unwrap();
    let address = strings.intern(b"address").unwrap();
    let name = strings.intern(b"one canonical collection").unwrap();
    let contracts: Vec<_> = (0..CONTRACTS)
        .map(|index| Contract {
            id: ContractId::new(entity_id(index)),
            chain_id: ChainId::new(0),
            address_ref: address,
            name_ref: Some(name),
            first_nft_id: NftId::new(entity_id(index)),
            nft_count: 1,
        })
        .collect();
    let result = run_name(
        &contracts,
        &strings,
        NameEngineConfig::production_default(1),
        &mut CountingSink::default(),
    )
    .unwrap();
    assert_eq!(result.canonical_names.len(), 1);
    assert_eq!(result.atoms.len(), 1);
    assert_eq!(result.contract_ids.len(), CONTRACTS);
    assert_eq!(result.counters.name_scored_candidates, 0);
}

#[test]
fn one_hundred_thousand_member_uri_group_keeps_constant_reducer_buffer() {
    const MEMBERS: usize = 100_000;
    let contracts: Vec<_> = (0..MEMBERS)
        .map(|index| Contract {
            id: ContractId::new(entity_id(index)),
            chain_id: ChainId::new(u16::try_from(index % 2).unwrap()),
            address_ref: StringId::new(entity_id(index)),
            name_ref: None,
            first_nft_id: NftId::new(entity_id(index)),
            nft_count: 1,
        })
        .collect();
    let shared_uri = StringId::new(entity_id(MEMBERS + 1));
    let nfts: Vec<_> = (0..MEMBERS)
        .map(|index| Nft {
            id: NftId::new(entity_id(index)),
            contract_id: ContractId::new(entity_id(index)),
            token_id_ref: StringId::new(entity_id(index)),
            token_uri_ref: Some(shared_uri),
            image_uri_ref: None,
            has_metadata: false,
        })
        .collect();
    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryBudget::new(64 * 1024 * 1024, 64 * 1024 * 1024);
    let config =
        UriExecutionConfig::new(ExecutionMode::External, directory.path(), 1, 4, 16, MEMBERS)
            .unwrap()
            .with_radix_memory_budget(memory, 8 * 1024 * 1024)
            .unwrap()
            .with_mark_shards(8, 256)
            .unwrap();
    let result =
        run_uri_with_config(&contracts, &nfts, &mut CountingSink::default(), &config).unwrap();
    assert_eq!(result.counters.uri_spilled_members, MEMBERS as u64);
    assert_eq!(result.member_accesses, MEMBERS as u64);
    assert_eq!(result.max_spill_reducer_buffered_members, 1);
    assert!(result.max_spill_hit_buffered_events <= 256);
    assert_eq!(result.spill_hit_shards, 8);
    assert_eq!(result.spill_handle_touches, MEMBERS as u64 * 6);
}
