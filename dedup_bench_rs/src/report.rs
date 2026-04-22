use serde::Serialize;

use crate::algorithms::{MetadataAlgorithmReport, NameAlgorithmReport};
use crate::sample::BenchmarkSample;
use crate::store::SourceInfo;

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct BenchmarkReport {
    pub chain: String,
    pub source: SourceInfo,
    pub sample: BenchmarkSample,
    pub recall_elapsed_ms: f64,
    pub recall_candidate_count: usize,
    pub name_algorithms: Vec<NameAlgorithmReport>,
    pub metadata_algorithms: Vec<MetadataAlgorithmReport>,
}

impl BenchmarkReport {
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("# NFT Name/Metadata Dedup Benchmark\n\n");
        out.push_str(&format!(
            "- chain: `{}`\n- source: `{:?}`\n- source location: `{}`\n- recall candidates: `{}`\n- recall elapsed: `{:.3} ms`\n\n",
            self.chain,
            self.source.kind,
            self.source.location,
            self.recall_candidate_count,
            self.recall_elapsed_ms
        ));
        out.push_str("## Sample\n\n");
        out.push_str(&format!(
            "- contract_address: `{}`\n- token_id: `{}`\n- name: `{}`\n- metadata_doc: `{}`\n\n",
            self.sample.contract_address,
            self.sample.token_id,
            self.sample.name,
            self.sample.metadata_doc
        ));

        out.push_str("## Name Algorithms\n\n");
        for algorithm in &self.name_algorithms {
            out.push_str(&format!(
                "### {}\n\n- field: `{:?}`\n- decision_rule: `{}`\n- duplicate_count: `{}`\n- avg_ms: `{:.3}`\n- min_ms: `{:.3}`\n- runs_ms: `{:?}`\n\n",
                algorithm.algorithm_id,
                algorithm.field,
                algorithm.decision_rule,
                algorithm.duplicate_count,
                algorithm.avg_ms,
                algorithm.min_ms,
                algorithm.runs_ms
            ));
            for candidate in &algorithm.duplicates {
                out.push_str(&format!(
                    "1. contract=`{}` max_score=`{:.4}` duplicate_token_count=`{}`\n",
                    candidate.contract_address, candidate.max_score, candidate.duplicate_token_count
                ));
            }
            out.push('\n');
        }

        out.push_str("## Metadata Algorithms\n\n");
        for algorithm in &self.metadata_algorithms {
            out.push_str(&format!(
                "### {}\n\n- field: `{:?}`\n- decision_rule: `{}`\n- duplicate_count: `{}`\n- avg_ms: `{:.3}`\n- min_ms: `{:.3}`\n- runs_ms: `{:?}`\n\n",
                algorithm.algorithm_id,
                algorithm.field,
                algorithm.decision_rule,
                algorithm.duplicate_count,
                algorithm.avg_ms,
                algorithm.min_ms,
                algorithm.runs_ms
            ));
            for candidate in &algorithm.duplicates {
                out.push_str(&format!(
                    "1. `{}` / `{}` score=`{:.4}` metadata_doc=`{}`\n",
                    candidate.contract_address,
                    candidate.token_id,
                    candidate.score,
                    candidate.metadata_doc
                ));
            }
            out.push('\n');
        }
        out
    }
}
