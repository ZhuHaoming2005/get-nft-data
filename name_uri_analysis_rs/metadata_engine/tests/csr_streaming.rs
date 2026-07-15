use metadata_engine::encode::csr::{build_bidirectional_csr, CsrSourceMembership};

#[test]
fn in_memory_builder_canonicalizes_unsorted_duplicate_memberships() {
    let csr = build_bidirectional_csr(&[
        CsrSourceMembership {
            source_doc_id: 9,
            contract_id: 1,
            retained_token_ids: vec![3, 1, 3],
        },
        CsrSourceMembership {
            source_doc_id: 4,
            contract_id: 0,
            retained_token_ids: vec![2, 1],
        },
        CsrSourceMembership {
            source_doc_id: 7,
            contract_id: 1,
            retained_token_ids: vec![2, 1],
        },
        CsrSourceMembership {
            source_doc_id: 7,
            contract_id: 1,
            retained_token_ids: vec![1],
        },
    ])
    .unwrap();

    assert_eq!(csr.contract_token_offsets, [0, 2, 5]);
    assert_eq!(csr.contract_tokens, [1, 2, 1, 2, 3]);
    assert_eq!(csr.token_member_offsets, [0, 0, 3, 5, 6]);
    assert_eq!(csr.token_member_contracts, [0, 1, 1, 0, 1, 1]);
    assert_eq!(csr.token_member_sources, [4, 7, 9, 4, 7, 9]);
}

#[test]
fn bucketed_builder_matches_a_set_reference_across_skewed_inputs() {
    use std::collections::BTreeSet;

    let mut state = 0x9e37_79b9_u32;
    let mut next = || {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        state
    };
    let mut memberships = Vec::new();
    for source in 0..300u32 {
        let contract = next() % 31;
        let token_len = (next() % 40) as usize;
        let tokens = (0..token_len).map(|_| next() % 17).collect::<Vec<_>>();
        memberships.push(CsrSourceMembership {
            source_doc_id: source,
            contract_id: contract,
            retained_token_ids: tokens,
        });
    }
    let csr = build_bidirectional_csr(&memberships).unwrap();

    let mut contract_sets = vec![BTreeSet::new(); 31];
    let mut token_sets = vec![BTreeSet::new(); 17];
    for membership in &memberships {
        for &token in &membership.retained_token_ids {
            contract_sets[membership.contract_id as usize].insert(token);
            token_sets[token as usize].insert((membership.contract_id, membership.source_doc_id));
        }
    }
    let mut expected_contract_offsets = vec![0u64];
    let mut expected_contract_tokens = Vec::new();
    for tokens in contract_sets {
        expected_contract_tokens.extend(tokens);
        expected_contract_offsets.push(expected_contract_tokens.len() as u64);
    }
    let mut expected_token_offsets = vec![0u64];
    let mut expected_contracts = Vec::new();
    let mut expected_sources = Vec::new();
    for members in token_sets {
        for (contract, source) in members {
            expected_contracts.push(contract);
            expected_sources.push(source);
        }
        expected_token_offsets.push(expected_contracts.len() as u64);
    }

    assert_eq!(csr.contract_token_offsets, expected_contract_offsets);
    assert_eq!(csr.contract_tokens, expected_contract_tokens);
    assert_eq!(csr.token_member_offsets, expected_token_offsets);
    assert_eq!(csr.token_member_contracts, expected_contracts);
    assert_eq!(csr.token_member_sources, expected_sources);
}
