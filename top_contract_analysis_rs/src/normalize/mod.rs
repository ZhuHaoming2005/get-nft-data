use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{Map, Value};
use unicode_normalization::UnicodeNormalization;

static TRAILING_PATTERNS: Lazy<Vec<Regex>> = Lazy::new(|| {
    vec![
        Regex::new(r"\s*#\s*[0-9a-fA-FxX]+\s*$").unwrap(),
        Regex::new(r"\s*#\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*-\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*:\s*\d+\s*$").unwrap(),
        Regex::new(r"\s*\(\s*\d+\s*\)\s*$").unwrap(),
        Regex::new(r"\s*\[\s*\d+\s*\]\s*$").unwrap(),
        Regex::new(r"\s*/\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+No\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+nr\.?\s*\d+\s*$").unwrap(),
        Regex::new(r"\s+\d{1,12}\s*$").unwrap(),
    ]
});

static WHITESPACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static IPFS_HTTP_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^https?://[^/]+/ipfs/([A-Za-z0-9][^?#\s]*)").unwrap());
static ARWEAVE_HTTP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^https?://(?:[^/]+\.)?arweave\.net/([A-Za-z0-9_-]{43}(?:/[^?#\s]*)?)").unwrap()
});

fn normalize_nfkc(raw: &str) -> String {
    raw.nfkc().collect::<String>()
}

fn strip_trailing_number_suffix(raw: &str) -> String {
    let mut text = normalize_nfkc(raw).trim().to_string();
    let mut changed = true;
    let mut guard = 0;
    while changed && guard < 20 {
        changed = false;
        guard += 1;
        for pattern in TRAILING_PATTERNS.iter() {
            let updated = pattern.replace(&text, "").trim().to_string();
            if updated != text {
                text = updated;
                changed = true;
                break;
            }
        }
    }
    WHITESPACE_RE.replace_all(&text, " ").trim().to_string()
}

pub fn normalize_name(raw: &str) -> String {
    strip_trailing_number_suffix(raw).to_lowercase()
}

pub fn normalize_symbol(raw: &str) -> String {
    normalize_nfkc(raw).trim().to_lowercase()
}

pub fn normalize_text(raw: &str) -> String {
    let text = normalize_nfkc(raw).to_lowercase();
    WHITESPACE_RE.replace_all(text.trim(), " ").to_string()
}

pub fn normalize_url(raw: &str) -> Option<String> {
    let text = raw.trim();
    if text.is_empty() {
        return None;
    }
    let lowered = text.to_lowercase();
    if matches!(
        lowered.as_str(),
        "nano" | "null" | "none" | "undefined" | "n/a" | "na" | "-" | "." | "false" | "true" | "0"
    ) {
        return None;
    }
    if lowered.starts_with("data:") {
        return None;
    }
    if lowered.starts_with("ipfs://") {
        let mut tail = text[7..].to_string();
        if tail.to_lowercase().starts_with("ipfs/") {
            tail = tail[5..].to_string();
        }
        let cid_path = tail
            .split('?')
            .next()
            .unwrap_or("")
            .split('#')
            .next()
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        return if cid_path.is_empty() {
            None
        } else {
            Some(format!("ipfs:{}", cid_path))
        };
    }
    if lowered.starts_with("ar://") {
        let tx_path = text[5..]
            .split('?')
            .next()
            .unwrap_or("")
            .split('#')
            .next()
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        return if tx_path.is_empty() {
            None
        } else {
            Some(format!("ar:{}", tx_path))
        };
    }
    if let Some(captures) = IPFS_HTTP_RE.captures(text) {
        let cid_path = captures
            .get(1)
            .map(|value| value.as_str())
            .unwrap_or("")
            .split('?')
            .next()
            .unwrap_or("")
            .split('#')
            .next()
            .unwrap_or("")
            .trim_end_matches('/')
            .to_string();
        return if cid_path.is_empty() {
            None
        } else {
            Some(format!("ipfs:{}", cid_path))
        };
    }
    if let Some(captures) = ARWEAVE_HTTP_RE.captures(text) {
        let tx_path = captures
            .get(1)
            .map(|value| value.as_str())
            .unwrap_or("")
            .split('?')
            .next()
            .unwrap_or("")
            .split('#')
            .next()
            .unwrap_or("")
            .trim_end_matches('/')
            .to_string();
        return if tx_path.is_empty() {
            None
        } else {
            Some(format!("ar:{}", tx_path))
        };
    }
    Some(lowered.trim_end_matches('/').to_string())
}

pub fn build_nft_metadata_json(
    raw_nft: &Value,
    token_uri: &str,
    image_uri: &str,
) -> Result<String, serde_json::Error> {
    let mut payload = Map::<String, Value>::new();
    let raw_meta = raw_nft
        .get("rawMetadata")
        .or_else(|| raw_nft.get("metadata"))
        .and_then(Value::as_object);
    if let Some(meta) = raw_meta {
        for (key, value) in meta {
            payload.insert(key.clone(), value.clone());
        }
    }
    for (source_key, target_key) in [
        ("title", "name"),
        ("name", "name"),
        ("description", "description"),
    ] {
        if !payload.contains_key(target_key) {
            if let Some(value) = raw_nft.get(source_key).filter(|value| !value.is_null()) {
                payload.insert(target_key.to_string(), value.clone());
            }
        }
    }
    if !token_uri.is_empty() && !payload.contains_key("token_uri") {
        payload.insert(
            "token_uri".to_string(),
            Value::String(token_uri.to_string()),
        );
    }
    if !image_uri.is_empty() && !payload.contains_key("image") {
        payload.insert("image".to_string(), Value::String(image_uri.to_string()));
    }
    if payload.is_empty() {
        return Ok(String::new());
    }
    serde_json::to_string(&Value::Object(payload))
}
