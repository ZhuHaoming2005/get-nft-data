use super::*;

pub(in crate::analyze_cases) struct FakeFeatureStore {
    pub(in crate::analyze_cases) snapshot: DatabaseSnapshot,
}

impl FeatureStoreReader for FakeFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        _seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        Ok(self.snapshot.clone())
    }
}

#[derive(Default)]
pub(in crate::analyze_cases) struct CapturingFeatureStore {
    pub(in crate::analyze_cases) captured_seed_names: Arc<Mutex<Vec<Vec<String>>>>,
}

impl FeatureStoreReader for CapturingFeatureStore {
    fn load_snapshot(
        &self,
        _chain: &str,
        seed_nfts: &[SeedNft],
        _name_threshold: f64,
        _metadata_threshold: f64,
        _max_tokens_per_contract: usize,
        _max_recall_rows: usize,
    ) -> Result<DatabaseSnapshot, AppError> {
        self.captured_seed_names
            .lock()
            .unwrap()
            .push(seed_nfts.iter().map(|item| item.name.clone()).collect());
        Ok(DatabaseSnapshot::default())
    }
}
