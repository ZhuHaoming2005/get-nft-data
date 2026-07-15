//! Production metadata document SAX/visitor parser.
//!
//! The visitor reduces each JSON subtree to the token-relevant summaries needed
//! by the legacy semantics. It never builds a generic JSON DOM or a
//! joined Match-facing document string.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use once_cell::sync::Lazy;
use regex::Regex;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use unicode_normalization::UnicodeNormalization;

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\p{L}\p{N}_]+").unwrap());

pub const MAX_METADATA_BYTES_FOR_DEDUP: usize = 64 * 1024;
const MAX_JSON_NESTING: usize = 128;

/// Tokenized prefilter / content documents for Encode (ordered BM25 token lists).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedMetadataDocuments {
    pub prefilter_tokens: Vec<String>,
    pub content_tokens: Vec<String>,
}

#[derive(Debug, Default)]
struct NodeSummary {
    recursive_prefilter: BTreeSet<String>,
    value_prefilter: BTreeSet<String>,
    direct_string: Option<String>,
    content_values: Vec<String>,
}

impl<'de> Deserialize<'de> for NodeSummary {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(NodeSummaryVisitor)
    }
}

struct NodeSummaryVisitor;

impl<'de> Visitor<'de> for NodeSummaryVisitor {
    type Value = NodeSummary;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(scalar_value_summary(value.to_string()))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(scalar_value_summary(
            serde_json::Number::from(value).to_string(),
        ))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(scalar_value_summary(
            serde_json::Number::from(value).to_string(),
        ))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let number = serde_json::Number::from_f64(value)
            .ok_or_else(|| E::custom("non-finite JSON number"))?;
        Ok(scalar_value_summary(number.to_string()))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(value.to_owned())
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(value.to_owned())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        let mut summary = scalar_value_summary(value.clone());
        summary.direct_string = Some(value.clone());
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            summary.content_values.push(trimmed.to_owned());
        }
        Ok(summary)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(NodeSummary::default())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(NodeSummary::default())
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut summary = NodeSummary::default();
        while let Some(child) = sequence.next_element::<NodeSummary>()? {
            summary
                .recursive_prefilter
                .extend(child.recursive_prefilter);
            summary.value_prefilter.extend(child.value_prefilter);
            summary.content_values.extend(child.content_values);
        }
        Ok(summary)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // serde_json::Map is key-sorted without preserve_order and duplicate
        // keys are last-wins. Retain one reduced child per raw key to reproduce
        // both properties without retaining a JSON DOM.
        let mut fields = BTreeMap::<String, NodeSummary>::new();
        while let Some((key, value)) = map.next_entry::<String, NodeSummary>()? {
            fields.insert(key, value);
        }

        let mut summary = NodeSummary::default();
        for (key, child) in fields {
            let key_norm = normalize_text(&key);

            push_prefilter_part(&mut summary.value_prefilter, &key);
            summary
                .value_prefilter
                .extend(child.value_prefilter.iter().cloned());

            if content_key_includes_value(&key_norm) {
                summary
                    .content_values
                    .extend(child.content_values.iter().cloned());
            }

            if key_norm.is_empty() {
                continue;
            }
            if matches!(key_norm.as_str(), "metadata" | "rawmetadata" | "raw") {
                summary
                    .recursive_prefilter
                    .extend(child.recursive_prefilter);
            } else if key_norm == "trait_type" {
                push_prefilter_part(&mut summary.recursive_prefilter, &key_norm);
                if let Some(text) = &child.direct_string {
                    push_prefilter_part(&mut summary.recursive_prefilter, text);
                }
            } else if prefilter_includes_value(&key_norm) {
                push_prefilter_part(&mut summary.recursive_prefilter, &key_norm);
                summary.recursive_prefilter.extend(child.value_prefilter);
            } else {
                push_prefilter_part(&mut summary.recursive_prefilter, &key_norm);
                summary
                    .recursive_prefilter
                    .extend(child.recursive_prefilter);
            }
        }
        Ok(summary)
    }
}

/// Parse raw metadata into prefilter and content BM25 token lists.
///
/// Semantics match the legacy `metadata_documents_from_json` + tokenize path.
pub fn parse_metadata_documents(raw: &str) -> ParsedMetadataDocuments {
    if raw.trim().is_empty() {
        return ParsedMetadataDocuments::default();
    }

    let parsed = json_nesting_within_limit(raw, MAX_JSON_NESTING)
        .then(|| serde_json::from_str::<NodeSummary>(raw))
        .transpose()
        .ok()
        .flatten();
    match parsed {
        Some(summary) => {
            let mut prefilter_tokens = Vec::new();
            for part in summary.recursive_prefilter {
                append_tokens_from_normalized(&part, &mut prefilter_tokens);
            }

            let mut content_tokens = Vec::new();
            if is_dedup_eligible(raw) {
                for value in summary.content_values {
                    append_tokens_from_normalized(&normalize_text(&value), &mut content_tokens);
                }
            }
            ParsedMetadataDocuments {
                prefilter_tokens,
                content_tokens,
            }
        }
        None => {
            let normalized = normalize_text(raw);
            let tokens = tokens_from_normalized(&normalized);
            ParsedMetadataDocuments {
                prefilter_tokens: tokens.clone(),
                content_tokens: if is_dedup_eligible(raw) {
                    tokens
                } else {
                    Vec::new()
                },
            }
        }
    }
}

/// Non-recursive preflight for untrusted JSON. Serde's normal recursion guard
/// remains enabled, but rejecting pathological nesting before deserialization
/// also bounds visitor/native-stack use independently of parser internals.
fn json_nesting_within_limit(raw: &str, limit: usize) -> bool {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for byte in raw.bytes() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth = depth.saturating_add(1);
                if depth > limit {
                    return false;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}

fn scalar_value_summary(raw: String) -> NodeSummary {
    let mut value_prefilter = BTreeSet::new();
    push_prefilter_part(&mut value_prefilter, &raw);
    NodeSummary {
        value_prefilter,
        ..NodeSummary::default()
    }
}

fn prefilter_includes_value(key: &str) -> bool {
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

fn push_prefilter_part(parts: &mut BTreeSet<String>, raw: &str) {
    let text = normalize_text(raw);
    if !text.is_empty() {
        parts.insert(text);
    }
}

fn is_dedup_eligible(raw: &str) -> bool {
    let raw = raw.trim();
    !raw.is_empty()
        && raw.len() <= MAX_METADATA_BYTES_FOR_DEDUP
        && matches!(raw.chars().next(), Some('{') | Some('['))
}

fn content_key_includes_value(key: &str) -> bool {
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

fn normalize_text(raw: &str) -> String {
    raw.nfkc()
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn tokens_from_normalized(document: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    append_tokens_from_normalized(document, &mut tokens);
    tokens
}

fn append_tokens_from_normalized(document: &str, out: &mut Vec<String>) {
    for capture in TOKEN_RE.find_iter(document) {
        let token = capture.as_str();
        if token.len() >= 2 {
            out.push(token.to_owned());
        }
    }
}
