use super::*;

#[test]
fn duplicate_contract_row_uses_precomputed_name_pair_uniqueness() {
    let record = DatabaseNftRecord {
        contract_address: "0xcandidate".into(),
        token_id: "1".into(),
        ..Default::default()
    };
    let mut rows_by_contract = HashMap::new();

    update_duplicate_contract_row(
        &mut rows_by_contract,
        &record,
        false,
        false,
        "azuki",
        true,
        false,
    );
    update_duplicate_contract_row(
        &mut rows_by_contract,
        &record,
        false,
        false,
        "azuki",
        false,
        false,
    );

    assert_eq!(
        rows_by_contract["0xcandidate"].name_norms,
        vec!["azuki".to_string()]
    );
}

#[test]
fn indexed_name_recall_matches_brute_force_for_all_threshold_hits() {
    let candidate_names = [
        "yabcdefghijklmnopqrst",
        "moon birds official",
        "moon birdz official",
        "completely unrelated",
        "短い名前",
        "短い名前",
    ];
    let rows = candidate_names
        .iter()
        .enumerate()
        .map(|(index, name)| RecallRow {
            feature_rowid: index as i64,
            contract_address: format!("0x{index}"),
            token_id: "1".to_string(),
            token_uri_norm: String::new(),
            image_uri_norm: String::new(),
            name_norm: (*name).to_string(),
        })
        .collect::<Vec<_>>();
    let profiles = vec![
        SeedRecallProfile::new(
            "seed-a".to_string(),
            &[SeedNft {
                contract_address: "0xseed-a".into(),
                name: "xabcdefghijklmnopqrst".into(),
                ..Default::default()
            }],
        ),
        SeedRecallProfile::new(
            "seed-b".to_string(),
            &[SeedNft {
                contract_address: "0xseed-b".into(),
                name: "moon birds official".into(),
                ..Default::default()
            }],
        ),
        SeedRecallProfile::new(
            "seed-c".to_string(),
            &[SeedNft {
                contract_address: "0xseed-c".into(),
                name: "短い名前".into(),
                ..Default::default()
            }],
        ),
    ];
    let index = NameRecallIndex::new(
        rows.iter()
            .map(|row| NameRecallRow {
                feature_rowid: row.feature_rowid,
                contract_address: row.contract_address.clone(),
                name_norm: row.name_norm.clone(),
            })
            .collect(),
    )
    .unwrap();

    for threshold in [95.0, 90.0, 0.0] {
        let brute = DuckDbFeatureStore::score_name_recall_batch(&profiles, &rows, threshold)
            .into_iter()
            .map(|matched| (matched.seed_index, matched.row_index))
            .collect::<BTreeSet<_>>();
        let indexed = DuckDbFeatureStore::score_name_recall_indexed(&profiles, &index, threshold)
            .into_iter()
            .map(|matched| (matched.seed_index, matched.row_index))
            .collect::<BTreeSet<_>>();

        assert_eq!(indexed, brute, "threshold {threshold}");
    }
}

#[test]
fn name_candidate_sparse_scratch_matches_dense_scratch() {
    let rows = ["moon birds", "moon birdz", "other collection"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| NameRecallRow {
            feature_rowid: index as i64,
            contract_address: format!("0x{index}"),
            name_norm: name.into(),
        })
        .collect::<Vec<_>>();
    let index = NameRecallIndex::new(rows).unwrap();
    let mut dense = NameCandidateScratch::new_dense(index.rows.len());
    let mut sparse = NameCandidateScratch::new_sparse();

    assert_eq!(
        index.candidates_for_query("moon birds", 90.0, &mut dense),
        index.candidates_for_query("moon birds", 90.0, &mut sparse)
    );
}

#[test]
fn name_candidate_scratch_reuse_is_isolated_between_queries() {
    let rows = ["moon birds", "azuki", "短い名前", "other collection"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| NameRecallRow {
            feature_rowid: index as i64,
            contract_address: format!("0x{index}"),
            name_norm: name.into(),
        })
        .collect::<Vec<_>>();
    let index = NameRecallIndex::new(rows).unwrap();
    let mut reused = NameCandidateScratch::new_dense(index.rows.len());

    let _ = index
        .candidates_for_query("moon birds", 90.0, &mut reused)
        .to_vec();
    let reused_result = index
        .candidates_for_query("短い名前", 95.0, &mut reused)
        .to_vec();
    let fresh_result = index
        .candidates_for_query(
            "短い名前",
            95.0,
            &mut NameCandidateScratch::new_dense(index.rows.len()),
        )
        .to_vec();

    assert_eq!(reused_result, fresh_result);
}

#[test]
fn load_snapshot_reuses_and_invalidates_name_recall_index_cache() {
    let store = DuckDbFeatureStore::new(":memory:").unwrap();
    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xcandidate".into(),
                token_id: "1".into(),
                name: "Moon Birds".into(),
                ..Default::default()
            }],
        )
        .unwrap();
    let seed = [SeedNft {
        contract_address: "0xseed".into(),
        token_id: "1".into(),
        name: "Moon Birds".into(),
        ..Default::default()
    }];

    assert_eq!(store.name_recall_index_cache_len(), 0);
    store
        .load_snapshot("ethereum", &seed, 95.0, 1.1, 0, 0)
        .unwrap();
    assert_eq!(store.name_recall_index_cache_len(), 1);
    store
        .load_snapshot("ethereum", &seed, 95.0, 1.1, 0, 0)
        .unwrap();
    assert_eq!(store.name_recall_index_cache_len(), 1);

    store
        .replace_chain_rows(
            "ethereum",
            &[DatabaseNftRecord {
                contract_address: "0xother".into(),
                token_id: "1".into(),
                name: "Other".into(),
                ..Default::default()
            }],
        )
        .unwrap();
    assert_eq!(store.name_recall_index_cache_len(), 0);
}
