use std::fs;
use std::path::Path;

use serde::Serialize;
use top_contract_analysis_rs::analysis::scoring::metadata_document_from_json;
use top_contract_analysis_rs::normalize::normalize_name;

use crate::algorithms::metadata_keywords;
use crate::error::BenchError;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct BenchmarkSample {
    pub chain: String,
    pub contract_address: String,
    pub token_id: String,
    pub name: String,
    pub name_norm: String,
    pub metadata_json: String,
    pub metadata_doc: String,
    pub metadata_keywords: Vec<String>,
}

impl BenchmarkSample {
    pub fn load(
        chain: &str,
        contract_address: &str,
        token_id: &str,
        name: &str,
        metadata_file: &Path,
    ) -> Result<Self, BenchError> {
        let metadata_value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(metadata_file)?)?;
        let metadata_json = serde_json::to_string(&metadata_value)?;
        let metadata_doc = metadata_document_from_json(&metadata_json);
        Ok(Self {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: token_id.to_string(),
            name_norm: normalize_name(name),
            name: name.to_string(),
            metadata_keywords: metadata_keywords(&metadata_doc, 8),
            metadata_doc,
            metadata_json,
        })
    }

    pub fn name_prefix(&self) -> Option<String> {
        if self.name_norm.is_empty() {
            None
        } else {
            Some(self.name_norm.chars().take(8).collect())
        }
    }
}
