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
    pub token_uri: String,
    pub image_uri: String,
    pub name: String,
    pub name_norm: String,
    pub metadata_json: String,
    #[serde(skip_serializing)]
    pub metadata_doc: String,
    #[serde(rename = "metadata_doc")]
    pub metadata_display_doc: String,
    pub metadata_keywords: Vec<String>,
}

impl BenchmarkSample {
    pub fn load(
        chain: &str,
        contract_address: &str,
        token_id: &str,
        token_uri: &str,
        image_uri: &str,
        name: &str,
        metadata_file: &Path,
    ) -> Result<Self, BenchError> {
        let metadata_value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(metadata_file)?)?;
        let metadata_json = serde_json::to_string(&metadata_value)?;
        let metadata_doc = metadata_document_from_json(&metadata_json);
        let metadata_display_doc = metadata_display_document(&metadata_value);
        Ok(Self {
            chain: chain.to_string(),
            contract_address: contract_address.to_string(),
            token_id: token_id.to_string(),
            token_uri: token_uri.to_string(),
            image_uri: image_uri.to_string(),
            name_norm: normalize_name(name),
            name: name.to_string(),
            metadata_keywords: metadata_keywords(&metadata_doc, 8),
            metadata_doc,
            metadata_display_doc,
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

pub fn metadata_display_document_from_json_str(metadata_json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(metadata_json)
        .map(|value| metadata_display_document(&value))
        .unwrap_or_default()
}

fn metadata_display_document(value: &serde_json::Value) -> String {
    let mut parts = Vec::new();
    collect_display_parts(value, &mut parts);
    parts.join(" ")
}

fn collect_display_parts(value: &serde_json::Value, parts: &mut Vec<String>) {
    match value {
        serde_json::Value::Null => {}
        serde_json::Value::Bool(boolean) => parts.push(boolean.to_string()),
        serde_json::Value::Number(number) => parts.push(number.to_string()),
        serde_json::Value::String(string) => {
            if !string.trim().is_empty() {
                parts.push(string.trim().to_string());
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_display_parts(item, parts);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_display_parts(value, parts);
            }
        }
    }
}
