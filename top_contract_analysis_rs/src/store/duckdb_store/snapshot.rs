use super::*;

impl DuckDbFeatureStore {
    pub fn load_snapshot(
        &self,
        chain: &str,
        seed_nfts: &[SeedNft],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        let mut timer = SnapshotLoadTimer::new("load_snapshot");
        let profiles = vec![SeedRecallProfile::new(String::new(), seed_nfts)];
        let mut accumulators = BTreeMap::from([(String::new(), SnapshotAccumulator::default())]);
        let profile_index = SeedProfileIndex::new(&profiles);

        let conn = self.conn()?;
        let prepared_recall_state = self.ensure_prepared_recall_state(&conn, chain)?;
        timer.finish_phase("ensure_prepared_recall");
        if !prepared_recall_state.ready {
            timer.finish();
            return Ok(accumulators
                .remove("")
                .expect("single-seed snapshot accumulator exists")
                .finish());
        }
        Self::create_selected_recall_rowid_table(&conn)?;
        timer.finish_phase("create_temp_tables");
        let result = (|| {
            let phase_result = Self::append_exact_uri_recall_rows(
                &conn,
                ExactUriRecallInput {
                    chain,
                    profiles: &profiles,
                    profile_index: &profile_index,
                    all_token_keys: &profiles[0].exact_token_keys,
                    all_image_keys: &profiles[0].exact_image_keys,
                    name_threshold,
                    max_tokens_per_contract,
                    max_recall_rows,
                    prepared_recall_state,
                },
                &mut accumulators,
            );
            timer.finish_phase("exact_uri_recall");
            phase_result?;

            let phase_result = Self::append_name_recall_rows(
                &conn,
                chain,
                &profiles,
                name_threshold,
                &mut accumulators,
                max_tokens_per_contract,
                prepared_recall_state,
            );
            timer.finish_phase("name_recall");
            phase_result?;

            if metadata_threshold <= 1.0
                && profiles
                    .iter()
                    .any(|profile| profile.seed_metadata_doc.is_some())
            {
                let phase_result =
                    self.cached_metadata_recall_index(&conn, chain, prepared_recall_state);
                timer.finish_phase("metadata_index");
                let metadata_index = phase_result?;

                let phase_result = Self::append_metadata_recall_rows(
                    &conn,
                    &profiles,
                    metadata_threshold,
                    metadata_index.as_ref(),
                    &mut accumulators,
                    max_tokens_per_contract,
                );
                timer.finish_phase("metadata_recall");
                phase_result?;
            }
            let phase_result = Self::append_overlapping_metadata_token_rows(
                &conn,
                chain,
                &profiles[0].seed_token_ids,
                &mut accumulators
                    .get_mut("")
                    .expect("single-seed snapshot accumulator exists")
                    .duplicate_rows_by_contract,
            );
            timer.finish_phase("overlapping_metadata_rows");
            phase_result?;
            Ok(accumulators
                .remove("")
                .expect("single-seed snapshot accumulator exists")
                .finish())
        })();
        let cleanup_result = Self::drop_seed_temp_tables(&conn);
        timer.finish_phase("cleanup_temp_tables");
        timer.finish();
        match (result, cleanup_result) {
            (Ok(snapshot), Ok(())) => Ok(snapshot),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
        }
    }

    pub fn load_snapshots(
        &self,
        chain: &str,
        seeds: &[(String, Vec<SeedNft>)],
        name_threshold: f64,
        metadata_threshold: f64,
        max_tokens_per_contract: usize,
        max_recall_rows: usize,
    ) -> Result<BTreeMap<String, DatabaseSnapshot>, AppError> {
        let mut timer = SnapshotLoadTimer::new("load_snapshots");
        if seeds.len() <= 1 {
            let mut snapshots = BTreeMap::new();
            for (seed_address, seed_nfts) in seeds {
                snapshots.insert(
                    seed_address.clone(),
                    self.load_snapshot(
                        chain,
                        seed_nfts,
                        name_threshold,
                        metadata_threshold,
                        max_tokens_per_contract,
                        max_recall_rows,
                    )?,
                );
            }
            return Ok(snapshots);
        }

        let profiles = seeds
            .iter()
            .map(|(seed_address, seed_nfts)| {
                SeedRecallProfile::new(seed_address.clone(), seed_nfts)
            })
            .collect::<Vec<_>>();
        let mut all_token_keys = HashSet::new();
        let mut all_image_keys = HashSet::new();
        for profile in &profiles {
            all_token_keys.extend(profile.exact_token_keys.iter().cloned());
            all_image_keys.extend(profile.exact_image_keys.iter().cloned());
        }

        let mut accumulators = profiles
            .iter()
            .map(|profile| (profile.seed_address.clone(), SnapshotAccumulator::default()))
            .collect::<BTreeMap<_, _>>();
        let profile_index = SeedProfileIndex::new(&profiles);
        if !profiles
            .iter()
            .any(SeedRecallProfile::has_strong_recall_keys)
            && !profiles
                .iter()
                .any(|profile| profile.seed_metadata_doc.is_some())
        {
            return Ok(accumulators
                .into_iter()
                .map(|(seed_address, accumulator)| (seed_address, accumulator.finish()))
                .collect());
        }

        let conn = self.conn()?;
        let prepared_recall_state = self.ensure_prepared_recall_state(&conn, chain)?;
        timer.finish_phase("ensure_prepared_recall");
        if !prepared_recall_state.ready {
            timer.finish();
            return Ok(accumulators
                .into_iter()
                .map(|(seed_address, accumulator)| (seed_address, accumulator.finish()))
                .collect());
        }
        Self::create_selected_recall_rowid_table(&conn)?;
        timer.finish_phase("create_temp_tables");
        let result = (|| {
            let phase_result = Self::append_exact_uri_recall_rows(
                &conn,
                ExactUriRecallInput {
                    chain,
                    profiles: &profiles,
                    profile_index: &profile_index,
                    all_token_keys: &all_token_keys,
                    all_image_keys: &all_image_keys,
                    name_threshold,
                    max_tokens_per_contract,
                    max_recall_rows,
                    prepared_recall_state,
                },
                &mut accumulators,
            );
            timer.finish_phase("exact_uri_recall");
            phase_result?;

            let phase_result = Self::append_name_recall_rows(
                &conn,
                chain,
                &profiles,
                name_threshold,
                &mut accumulators,
                max_tokens_per_contract,
                prepared_recall_state,
            );
            timer.finish_phase("name_recall");
            phase_result?;

            if metadata_threshold <= 1.0
                && profiles
                    .iter()
                    .any(|profile| profile.seed_metadata_doc.is_some())
            {
                let phase_result =
                    self.cached_metadata_recall_index(&conn, chain, prepared_recall_state);
                timer.finish_phase("metadata_index");
                let metadata_index = phase_result?;

                let phase_result = Self::append_metadata_recall_rows(
                    &conn,
                    &profiles,
                    metadata_threshold,
                    metadata_index.as_ref(),
                    &mut accumulators,
                    max_tokens_per_contract,
                );
                timer.finish_phase("metadata_recall");
                phase_result?;
            }
            for profile in &profiles {
                let Some(accumulator) = accumulators.get_mut(&profile.seed_address) else {
                    continue;
                };
                let phase_result = Self::append_overlapping_metadata_token_rows(
                    &conn,
                    chain,
                    &profile.seed_token_ids,
                    &mut accumulator.duplicate_rows_by_contract,
                );
                timer.finish_phase("overlapping_metadata_rows");
                phase_result?;
            }

            let snapshots = accumulators
                .into_iter()
                .map(|(seed_address, accumulator)| (seed_address, accumulator.finish()))
                .collect();
            Ok(snapshots)
        })();
        let cleanup_result = Self::drop_seed_temp_tables(&conn);
        timer.finish_phase("cleanup_temp_tables");
        timer.finish();
        match (result, cleanup_result) {
            (Ok(snapshots), Ok(())) => Ok(snapshots),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
        }
    }
}
