use serde_json::{Map, Value};
use unicode_normalization::UnicodeNormalization;

pub fn canonicalize_json(raw: &str) -> Option<String> {
    let value: Value = serde_json::from_str(raw.trim()).ok()?;
    let normalized = normalize_value(value);
    serde_json::to_string(&normalized).ok()
}

fn normalize_value(value: Value) -> Value {
    match value {
        Value::String(s) => Value::String(normalize_string(&s)),
        Value::Array(items) => Value::Array(items.into_iter().map(normalize_value).collect()),
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map
                .into_iter()
                .map(|(k, v)| (normalize_string(&k), normalize_value(v)))
                .collect();
            // attributes: sort by trait_type then value for stable alignment
            if let Some(attrs) = entries.iter_mut().find(|(k, _)| k == "attributes")
                && let Value::Array(items) = &mut attrs.1
            {
                items.sort_by(|a, b| {
                    attr_key(a).cmp(&attr_key(b))
                });
            }
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = Map::new();
            for (k, v) in entries {
                out.insert(k, v);
            }
            Value::Object(out)
        }
        other => other,
    }
}

fn attr_key(value: &Value) -> (String, String) {
    match value {
        Value::Object(map) => (
            map.get("trait_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned(),
            map.get("value")
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ),
        _ => (String::new(), value.to_string()),
    }
}

fn normalize_string(input: &str) -> String {
    input
        .nfkc()
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
