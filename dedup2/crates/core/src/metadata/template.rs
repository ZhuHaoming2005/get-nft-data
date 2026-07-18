use crate::metadata::anchors::ContractAnchors;
use ahash::AHashMap;
use serde_json::Value;
use sha2::{Digest, Sha256};

const MIN_ANCHOR_DOCUMENTS: usize = 2;
const STABLE_VALUE_MIN_ANCHORS: usize = 2;
const STABLE_VALUE_SUPPORT_RATIO: f64 = 0.80;

#[derive(Clone, Debug)]
pub struct TemplateFingerprint {
    pub digest: [u8; 32],
    pub features: Vec<String>,
    pub low_information: bool,
}

pub fn fingerprint_anchors(anchors: &ContractAnchors) -> TemplateFingerprint {
    let mut structure: ahash::AHashSet<String> = ahash::AHashSet::new();
    let mut value_votes: AHashMap<String, AHashMap<String, usize>> = AHashMap::new();
    let docs = anchors.anchors.len();

    for anchor in &anchors.anchors {
        if let Ok(value) = serde_json::from_str::<Value>(&anchor.json) {
            walk_json(&value, "", &mut structure, &mut value_votes);
        }
    }

    // Need at least min_anchor_documents before any collection-level stable value.
    let mut stable_values = Vec::new();
    if docs >= MIN_ANCHOR_DOCUMENTS {
        let min_support = (((docs as f64) * STABLE_VALUE_SUPPORT_RATIO).ceil() as usize)
            .max(STABLE_VALUE_MIN_ANCHORS);
        for (path, counts) in &value_votes {
            if !is_discriminative_path(path) {
                continue;
            }
            if let Some((value, &count)) = counts.iter().max_by_key(|(_, c)| *c)
                && count >= min_support
            {
                stable_values.push(format!("v:{path}={value}"));
            }
        }
        stable_values.sort();
    }

    let mut features: Vec<String> = structure.into_iter().map(|s| format!("s:{s}")).collect();
    features.sort();
    features.extend(stable_values.iter().cloned());

    let placeholder = all_identical_placeholder(anchors);
    let low_information = stable_values.is_empty() || placeholder;
    let mut hasher = Sha256::new();
    for feature in &features {
        hasher.update(feature.as_bytes());
        hasher.update([0]);
    }
    let digest: [u8; 32] = hasher.finalize().into();
    TemplateFingerprint {
        digest,
        features,
        low_information,
    }
}

fn all_identical_placeholder(anchors: &ContractAnchors) -> bool {
    let Some(first) = anchors.anchors.first() else {
        return false;
    };
    let first_trim = first.json.trim();
    if anchors
        .anchors
        .iter()
        .any(|a| a.json.trim() != first_trim)
    {
        return false;
    }
    is_placeholder_content(first_trim)
}

fn is_placeholder_content(json: &str) -> bool {
    let lower = json.to_ascii_lowercase();
    const MARKERS: [&str; 8] = [
        "unrevealed",
        "not revealed",
        "coming soon",
        "placeholder",
        "reveal soon",
        "to be revealed",
        "prereveal",
        "pre-reveal",
    ];
    MARKERS.iter().any(|m| lower.contains(m))
}

fn walk_json(
    value: &Value,
    path: &str,
    structure: &mut ahash::AHashSet<String>,
    value_votes: &mut AHashMap<String, AHashMap<String, usize>>,
) {
    let node_type = match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    };
    structure.insert(format!("{path}:{node_type}"));
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                if is_stable_value_path(&child_path)
                    && let Some(s) = child.as_str()
                {
                    *value_votes
                        .entry(child_path.clone())
                        .or_default()
                        .entry(s.to_owned())
                        .or_default() += 1;
                }
                if is_url_base_path(&child_path)
                    && let Some(s) = child.as_str()
                {
                    let base = url_base(s);
                    *value_votes
                        .entry(format!("{child_path}#base"))
                        .or_default()
                        .entry(base)
                        .or_default() += 1;
                }
                walk_json(child, &child_path, structure, value_votes);
            }
        }
        Value::Array(items) => {
            for child in items {
                let child_path = format!("{path}[]");
                walk_json(child, &child_path, structure, value_votes);
            }
        }
        _ => {}
    }
}

fn is_discriminative_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    p.contains("collection")
        || p.ends_with("name")
        || p.ends_with("symbol")
        || p.contains("description")
        || p.contains("creator")
        || p.contains("royalty")
        || p.contains("license")
        || p.contains("#base")
}

fn is_stable_value_path(path: &str) -> bool {
    is_discriminative_path(path) && !path.contains("attributes")
}

fn is_url_base_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    p.ends_with("image") || p.ends_with("external_url") || p.ends_with("animation_url")
}

fn url_base(url: &str) -> String {
    let trimmed = url.trim();
    if let Some(rest) = trimmed.strip_prefix("ipfs://") {
        let cid = rest.split('/').next().unwrap_or(rest);
        return format!("ipfs://{cid}");
    }
    if let Some(idx) = trimmed.find("://") {
        let after = &trimmed[idx + 3..];
        let host = after.split('/').next().unwrap_or(after);
        return host.to_owned();
    }
    trimmed
        .rsplit_once('/')
        .map(|(prefix, _)| prefix.to_owned())
        .unwrap_or_else(|| trimmed.to_owned())
}
