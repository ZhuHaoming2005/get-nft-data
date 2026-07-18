use dedup_engine::metadata::{
    MetadataCandidate, MetadataRecord, PrefilterAudit, PrefilterResult, run_metadata_verification,
    select_anchors,
};
use dedup_engine::name::{NameEngineConfig, run_name};
use dedup_engine::uri::run_uri;
use dedup_index::StringDictionary;
use dedup_model::{
    ChainId, Contract, ContractId, Dimension, EntityId, EntityKind, HitEvent, HitEventSink, Nft,
    NftId, ScopeId, StringId,
};
use dedup_report::BitmapHitSink;
use std::collections::{BTreeMap, BTreeSet};

fn bitmap_sink() -> BitmapHitSink {
    BitmapHitSink::new(1_024).unwrap()
}

fn assert_bitmap_bits(mut actual: BitmapHitSink, expected: &BTreeSet<HitEvent>) {
    actual.finish_batch();
    let mut expected_sink = bitmap_sink();
    for event in expected {
        expected_sink.submit(*event).unwrap();
    }
    expected_sink.finish_batch();
    let snapshot = |sink: &BitmapHitSink| {
        sink.entries()
            .map(|(key, bitmap)| (*key, bitmap.iter().collect::<Vec<_>>()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(snapshot(&actual), snapshot(&expected_sink));
}

fn contract(id: u32, chain: u16, name_ref: Option<StringId>) -> Contract {
    Contract {
        id: ContractId::new(EntityId::from(id)),
        chain_id: ChainId::new(chain),
        address_ref: StringId::new(0),
        name_ref,
        first_nft_id: NftId::new(EntityId::from(id)),
        nft_count: 1,
    }
}

#[test]
fn name_bitmaps_match_independent_exhaustive_jaro_winkler() {
    let mut strings = StringDictionary::new(8).unwrap();
    let names = [
        strings.intern(b"alpha collection").unwrap(),
        strings.intern(b"alpha collectiom").unwrap(),
        strings.intern(b"unrelated").unwrap(),
        strings.intern(b"alpha collection").unwrap(),
    ];
    let contracts = [
        contract(0, 0, Some(names[0])),
        contract(1, 0, Some(names[1])),
        contract(2, 1, Some(names[2])),
        contract(3, 1, Some(names[3])),
    ];
    let mut actual = bitmap_sink();
    run_name(
        &contracts,
        &strings,
        NameEngineConfig::production_default(100),
        &mut actual,
    )
    .unwrap();

    let mut expected = BTreeSet::new();
    for left in 0..contracts.len() {
        for right in left + 1..contracts.len() {
            let left_name = std::str::from_utf8(strings.resolve(names[left]).unwrap()).unwrap();
            let right_name = std::str::from_utf8(strings.resolve(names[right]).unwrap()).unwrap();
            if independent_jaro_winkler(left_name, right_name) < 0.95 {
                continue;
            }
            emit_expected_name_pair(&contracts[left], &contracts[right], &mut expected);
        }
    }
    assert_bitmap_bits(actual, &expected);
}

fn emit_expected_name_pair(left: &Contract, right: &Contract, output: &mut BTreeSet<HitEvent>) {
    emit_expected_contract_pair(Dimension::Name, left, right, output);
}

fn emit_expected_contract_pair(
    dimension: Dimension,
    left: &Contract,
    right: &Contract,
    output: &mut BTreeSet<HitEvent>,
) {
    if left.chain_id == right.chain_id {
        for contract in [left, right] {
            output.insert(HitEvent {
                dimension,
                scope: ScopeId::Intra(contract.chain_id),
                entity_kind: EntityKind::Contract,
                entity_id: contract.id.as_u64(),
            });
        }
        return;
    }
    for (primary, secondary) in [(left, right), (right, left)] {
        for scope in [
            ScopeId::CrossSummary(primary.chain_id),
            ScopeId::Matrix {
                primary: primary.chain_id,
                secondary: secondary.chain_id,
            },
        ] {
            output.insert(HitEvent {
                dimension,
                scope,
                entity_kind: EntityKind::Contract,
                entity_id: primary.id.as_u64(),
            });
        }
    }
}

#[test]
fn metadata_bitmaps_match_independent_exact_content_oracle() {
    let contracts = [
        contract(0, 0, None),
        contract(1, 0, None),
        contract(2, 1, None),
        contract(3, 1, None),
    ];
    let contents = [
        r#"{"collection":"shared","value":1}"#,
        r#"{"collection":"shared","value":1}"#,
        r#"{"collection":"shared","value":1}"#,
        r#"{"collection":"other","value":9}"#,
    ];
    let records = contracts
        .iter()
        .zip(contents)
        .map(|(contract, content)| MetadataRecord {
            doc_id: dedup_model::MetadataDocId::new(contract.id.get()),
            contract_id: contract.id,
            chain_id: contract.chain_id,
            token_id: "7".to_owned(),
            content: content.to_owned(),
        })
        .collect();
    let mut counters = dedup_model::StageCounters::default();
    let anchors = select_anchors(
        records,
        &BTreeSet::from([ChainId::new(0), ChainId::new(1)]),
        1,
        &mut counters,
    )
    .unwrap();
    let candidates = (0_u32..4)
        .flat_map(|left| {
            (left + 1..4).map(move |right| {
                MetadataCandidate::new(
                    ContractId::new(EntityId::from(left)),
                    ContractId::new(EntityId::from(right)),
                )
                .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let prefilter = PrefilterResult {
        candidates: candidates.into(),
        audit: PrefilterAudit::default(),
    };
    let mut actual = bitmap_sink();
    run_metadata_verification(&anchors, &prefilter, 0.6, 6, &mut actual).unwrap();

    let mut expected = BTreeSet::new();
    for left in 0..contracts.len() {
        for right in left + 1..contracts.len() {
            if contents[left] == contents[right] {
                emit_expected_contract_pair(
                    Dimension::Metadata,
                    &contracts[left],
                    &contracts[right],
                    &mut expected,
                );
            }
        }
    }
    assert_bitmap_bits(actual, &expected);
}

#[test]
fn uri_bitmaps_match_independent_group_oracle() {
    let contracts = vec![
        contract(0, 0, None),
        contract(1, 0, None),
        contract(2, 1, None),
    ];
    let nfts = vec![
        nft(0, 0, Some(7), Some(70)),
        nft(1, 1, Some(7), Some(71)),
        nft(2, 2, Some(7), Some(70)),
        nft(3, 2, Some(8), Some(71)),
    ];
    let mut actual = bitmap_sink();
    run_uri(&contracts, &nfts, &mut actual).unwrap();
    let mut expected = BTreeSet::new();
    oracle_uri_dimension(
        &contracts,
        &nfts,
        Dimension::TokenUri,
        |nft| nft.token_uri_ref,
        &mut expected,
    );
    oracle_uri_dimension(
        &contracts,
        &nfts,
        Dimension::ImageUri,
        |nft| nft.image_uri_ref,
        &mut expected,
    );
    assert_bitmap_bits(actual, &expected);
}

fn nft(id: u32, contract_id: u32, token: Option<u64>, image: Option<u64>) -> Nft {
    Nft {
        id: NftId::new(EntityId::from(id)),
        contract_id: ContractId::new(EntityId::from(contract_id)),
        token_id_ref: StringId::new(EntityId::from(id)),
        token_uri_ref: token
            .map(|value| StringId::new(EntityId::try_from(value).expect("fixture ID fits"))),
        image_uri_ref: image
            .map(|value| StringId::new(EntityId::try_from(value).expect("fixture ID fits"))),
        has_metadata: false,
    }
}

fn oracle_uri_dimension(
    contracts: &[Contract],
    nfts: &[Nft],
    dimension: Dimension,
    uri: impl Fn(&Nft) -> Option<StringId>,
    output: &mut BTreeSet<HitEvent>,
) {
    let mut groups: BTreeMap<StringId, Vec<&Nft>> = BTreeMap::new();
    for nft in nfts {
        if let Some(value) = uri(nft) {
            groups.entry(value).or_default().push(nft);
        }
    }
    for members in groups.values() {
        let mut contracts_by_chain: BTreeMap<ChainId, BTreeSet<ContractId>> = BTreeMap::new();
        for nft in members {
            let contract = &contracts[usize::try_from(nft.contract_id.get()).unwrap()];
            contracts_by_chain
                .entry(contract.chain_id)
                .or_default()
                .insert(contract.id);
        }
        for nft in members {
            let chain = contracts[usize::try_from(nft.contract_id.get()).unwrap()].chain_id;
            if contracts_by_chain[&chain].len() >= 2 {
                output.insert(uri_event(dimension, ScopeId::Intra(chain), nft.id));
            }
            let other_chains: Vec<_> = contracts_by_chain
                .keys()
                .copied()
                .filter(|candidate| *candidate != chain)
                .collect();
            for secondary in &other_chains {
                output.insert(uri_event(
                    dimension,
                    ScopeId::Matrix {
                        primary: chain,
                        secondary: *secondary,
                    },
                    nft.id,
                ));
            }
            if !other_chains.is_empty() {
                output.insert(uri_event(dimension, ScopeId::CrossSummary(chain), nft.id));
            }
        }
    }
}

fn uri_event(dimension: Dimension, scope: ScopeId, nft: NftId) -> HitEvent {
    HitEvent {
        dimension,
        scope,
        entity_kind: EntityKind::Nft,
        entity_id: nft.as_u64(),
    }
}

fn independent_jaro_winkler(left: &str, right: &str) -> f64 {
    if left == right {
        return 1.0;
    }
    let left: Vec<char> = left.chars().collect();
    let right: Vec<char> = right.chars().collect();
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let radius = left
        .len()
        .max(right.len())
        .saturating_div(2)
        .saturating_sub(1);
    let mut left_match = vec![false; left.len()];
    let mut right_match = vec![false; right.len()];
    let mut matches = 0_u32;
    for (left_index, character) in left.iter().enumerate() {
        let start = left_index.saturating_sub(radius);
        let end = (left_index + radius + 1).min(right.len());
        for right_index in start..end {
            if !right_match[right_index] && *character == right[right_index] {
                left_match[left_index] = true;
                right_match[right_index] = true;
                matches += 1;
                break;
            }
        }
    }
    if matches == 0 {
        return 0.0;
    }
    let matched_left: Vec<_> = left
        .iter()
        .zip(left_match)
        .filter_map(|(value, matched)| matched.then_some(*value))
        .collect();
    let matched_right: Vec<_> = right
        .iter()
        .zip(right_match)
        .filter_map(|(value, matched)| matched.then_some(*value))
        .collect();
    let transpositions = matched_left
        .iter()
        .zip(matched_right)
        .filter(|(left, right)| **left != *right)
        .count()
        / 2;
    let matches = f64::from(matches);
    let jaro = (matches / left.len() as f64
        + matches / right.len() as f64
        + (matches - transpositions as f64) / matches)
        / 3.0;
    let prefix = left
        .iter()
        .zip(&right)
        .take(4)
        .take_while(|(left, right)| left == right)
        .count();
    jaro + prefix as f64 * 0.1 * (1.0 - jaro)
}
