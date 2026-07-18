use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    #[default]
    Auto,
    InMemory,
    Hybrid,
    External,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StageConcurrency {
    pub preflight: usize,
    pub entity: usize,
    pub name: usize,
    pub uri: usize,
    pub metadata: usize,
    pub report: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataPrefilterParameters {
    pub template_jaccard_threshold: f64,
    pub lsh_bands: u32,
    pub lsh_rows_per_band: u32,
    pub target_candidate_recall: f64,
    pub neighbors_per_target_chain: usize,
    pub max_candidates_per_target_chain: usize,
    pub max_outgoing_candidates_per_contract: usize,
    pub exact_bucket_size_cap: usize,
}

impl MetadataPrefilterParameters {
    pub const MAX_DERIVED_SIGNATURE_COMPONENTS: u32 = 128;

    pub fn predicted_candidate_recall(&self) -> f64 {
        let s = self.template_jaccard_threshold;
        1.0 - (1.0 - s.powf(f64::from(self.lsh_rows_per_band))).powf(f64::from(self.lsh_bands))
    }

    /// Derives a deterministic `(bands, rows)` shape from the template
    /// threshold and recall target. Among shapes bounded to 128 MinHash
    /// components, prefer the lowest collision probability at half the
    /// threshold, then the smaller signature.
    pub fn derived_lsh_shape(&self) -> Option<(u32, u32)> {
        let threshold = self.template_jaccard_threshold;
        let target = self.target_candidate_recall;
        if !(0.0..=1.0).contains(&threshold) || !(0.0..=1.0).contains(&target) {
            return None;
        }
        let mut best: Option<(f64, u32, u32)> = None;
        for rows in 1..=Self::MAX_DERIVED_SIGNATURE_COMPONENTS {
            for bands in 1..=Self::MAX_DERIVED_SIGNATURE_COMPONENTS / rows {
                let recall = 1.0 - (1.0 - threshold.powf(f64::from(rows))).powf(f64::from(bands));
                if recall + f64::EPSILON < target {
                    continue;
                }
                let half = threshold / 2.0;
                let false_collision =
                    1.0 - (1.0 - half.powf(f64::from(rows))).powf(f64::from(bands));
                let signature = bands.saturating_mul(rows);
                let candidate = (false_collision, signature, rows);
                if best.is_none_or(|current| candidate < current) {
                    best = Some(candidate);
                }
            }
        }
        best.map(|(_, signature, rows)| (signature / rows, rows))
    }

    pub fn apply_derived_lsh_shape(&mut self) -> bool {
        let Some((bands, rows)) = self.derived_lsh_shape() else {
            return false;
        };
        self.lsh_bands = bands;
        self.lsh_rows_per_band = rows;
        true
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataGuardParameters {
    pub min_anchor_documents: usize,
    pub stable_value_min_anchors: usize,
    pub stable_value_support_ratio: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkBudgets {
    pub name_scored_candidates: u64,
    pub metadata_prefilter_pairs: u64,
    pub metadata_verify_pairs: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QualityGate {
    pub metadata_recall: f64,
    pub minimum_positive_pairs: u64,
    pub sample_seed: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunConfig {
    pub input_files: Vec<String>,
    pub output_dir: String,
    pub temporary_volumes: Vec<String>,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub memory_limit: u64,
    pub stage_concurrency: StageConcurrency,
    pub entity_execution_mode: ExecutionMode,
    pub uri_execution_mode: ExecutionMode,
    pub metadata_execution_mode: ExecutionMode,
    pub name_threshold: f64,
    pub metadata_content_threshold: f64,
    pub metadata_anchor_tokens: usize,
    pub metadata_prefilter_parameters: MetadataPrefilterParameters,
    pub metadata_guard_parameters: MetadataGuardParameters,
    pub work_budgets: WorkBudgets,
    pub quality_gate: QualityGate,
    #[serde(default)]
    pub recorded_overrides: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsh_recall_uses_template_threshold() {
        let p = MetadataPrefilterParameters {
            template_jaccard_threshold: 0.75,
            lsh_bands: 32,
            lsh_rows_per_band: 2,
            target_candidate_recall: 0.99,
            neighbors_per_target_chain: 16,
            max_candidates_per_target_chain: 64,
            max_outgoing_candidates_per_contract: 256,
            exact_bucket_size_cap: 4096,
        };
        assert!(p.predicted_candidate_recall() > 0.99);
        assert_eq!(p.derived_lsh_shape(), Some((17, 5)));
    }

    #[test]
    fn derived_lsh_shape_is_bounded_and_meets_target() {
        let mut p = MetadataPrefilterParameters {
            template_jaccard_threshold: 0.75,
            lsh_bands: 1,
            lsh_rows_per_band: 1,
            target_candidate_recall: 0.99,
            neighbors_per_target_chain: 16,
            max_candidates_per_target_chain: 64,
            max_outgoing_candidates_per_contract: 256,
            exact_bucket_size_cap: 4096,
        };
        assert!(p.apply_derived_lsh_shape());
        assert!(
            p.lsh_bands.saturating_mul(p.lsh_rows_per_band)
                <= MetadataPrefilterParameters::MAX_DERIVED_SIGNATURE_COMPONENTS
        );
        assert!(p.predicted_candidate_recall() + f64::EPSILON >= p.target_candidate_recall);
    }
}
