//! Independent historical parser oracle used only by differential tests.

use std::collections::BTreeSet;

use metadata_engine::encode::ParsedMetadataDocuments;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());
const MAX_METADATA_BYTES_FOR_DEDUP: usize = 64 * 1024;

pub fn legacy_parse_metadata_documents(raw: &str) -> ParsedMetadataDocuments {
    let docs = metadata_documents_from_json(raw);
    ParsedMetadataDocuments {
        prefilter_tokens: metadata_bm25_tokens_from_normalized(&docs.prefilter),
        content_tokens: metadata_bm25_tokens_from_normalized(&docs.content),
    }
}

#[derive(Debug, Default)]
struct MetadataDocuments {
    prefilter: String,
    content: String,
}

fn metadata_documents_from_json(raw: &str) -> MetadataDocuments {
    if raw.trim().is_empty() {
        return MetadataDocuments::default();
    }
    match serde_json::from_str::<Value>(raw) {
        Ok(value) => {
            let mut prefilter_parts = BTreeSet::new();
            collect_metadata_prefilter_parts(&value, &mut prefilter_parts);
            let prefilter = prefilter_parts.into_iter().collect::<Vec<_>>().join(" ");
            let content = if metadata_is_dedup_eligible(raw) {
                let mut content_parts = Vec::new();
                flatten_metadata_content_values(&value, &mut content_parts);
                normalize_metadata_text(&content_parts.join(" "))
            } else {
                String::new()
            };
            MetadataDocuments { prefilter, content }
        }
        Err(_) => {
            let normalized = normalize_metadata_text(raw);
            MetadataDocuments {
                prefilter: normalized.clone(),
                content: if metadata_is_dedup_eligible(raw) {
                    normalized
                } else {
                    String::new()
                },
            }
        }
    }
}

fn collect_metadata_prefilter_parts(value: &Value, parts: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                let key_norm = normalize_metadata_text(key);
                if key_norm.is_empty() {
                    continue;
                }
                if matches!(key_norm.as_str(), "metadata" | "rawmetadata" | "raw") {
                    collect_metadata_prefilter_parts(item, parts);
                } else if key_norm == "trait_type" {
                    push_metadata_prefilter_part(parts, &key_norm);
                    if let Some(text) = item.as_str() {
                        push_metadata_prefilter_part(parts, text);
                    }
                } else if metadata_prefilter_includes_value(&key_norm) {
                    push_metadata_prefilter_part(parts, &key_norm);
                    collect_metadata_prefilter_values(item, parts);
                } else {
                    push_metadata_prefilter_part(parts, &key_norm);
                    collect_metadata_prefilter_parts(item, parts);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_metadata_prefilter_parts(item, parts);
            }
        }
        _ => {}
    }
}

fn collect_metadata_prefilter_values(value: &Value, parts: &mut BTreeSet<String>) {
    match value {
        Value::String(text) => push_metadata_prefilter_part(parts, text),
        Value::Number(number) => push_metadata_prefilter_part(parts, &number.to_string()),
        Value::Bool(value) => push_metadata_prefilter_part(parts, &value.to_string()),
        Value::Array(items) => {
            for item in items {
                collect_metadata_prefilter_values(item, parts);
            }
        }
        Value::Object(map) => {
            for (key, item) in map {
                push_metadata_prefilter_part(parts, key);
                collect_metadata_prefilter_values(item, parts);
            }
        }
        Value::Null => {}
    }
}

fn metadata_prefilter_includes_value(key: &str) -> bool {
    matches!(
        key,
        "description"
            | "bio"
            | "story"
            | "lore"
            | "summary"
            | "about"
            | "seller_fee_basis_points"
            | "fee_recipient"
            | "royalty"
            | "royalties"
            | "creator"
            | "creators"
            | "compiler"
            | "license"
            | "collection"
            | "marketplace"
            | "contract"
            | "chain"
    )
}

fn push_metadata_prefilter_part(parts: &mut BTreeSet<String>, raw: &str) {
    let text = normalize_metadata_text(raw);
    if !text.is_empty() {
        parts.insert(text);
    }
}

fn metadata_is_dedup_eligible(raw: &str) -> bool {
    let raw = raw.trim();
    !raw.is_empty()
        && raw.len() <= MAX_METADATA_BYTES_FOR_DEDUP
        && matches!(raw.chars().next(), Some('{') | Some('['))
}

fn flatten_metadata_content_values(value: &Value, parts: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                if metadata_content_key_includes_value(&normalize_metadata_text(key)) {
                    flatten_metadata_content_values(item, parts);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                flatten_metadata_content_values(item, parts);
            }
        }
        Value::String(text) if !text.trim().is_empty() => parts.push(text.trim().to_string()),
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {}
    }
}

fn metadata_content_key_includes_value(key: &str) -> bool {
    matches!(
        key,
        "description"
            | "trait_type"
            | "value"
            | "display_type"
            | "image"
            | "image_url"
            | "animation_url"
            | "external_url"
            | "attributes"
            | "metadata"
            | "rawmetadata"
            | "raw"
    )
}

fn normalize_metadata_text(raw: &str) -> String {
    raw.nfkc()
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn metadata_bm25_tokens_from_normalized(document: &str) -> Vec<String> {
    TOKEN_RE
        .find_iter(document)
        .map(|m| m.as_str().to_string())
        .filter(|token| token.len() >= 2)
        .collect()
}
