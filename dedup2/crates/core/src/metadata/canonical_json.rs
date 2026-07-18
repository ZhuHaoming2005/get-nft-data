use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::collections::BTreeSet;
use std::fmt;
use unicode_normalization::UnicodeNormalization;

pub fn canonicalize_json(raw: &str) -> Option<String> {
    let mut deserializer = serde_json::Deserializer::from_str(raw.trim());
    let value = StrictValue::deserialize(&mut deserializer).ok()?.0;
    deserializer.end().ok()?;
    let normalized = normalize_value(value)?;
    serde_json::to_string(&normalized).ok()
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictVisitor)
    }
}

struct StrictVisitor;

impl<'de> Visitor<'de> for StrictVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .map(StrictValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<StrictValue>()? {
            values.push(value.0);
        }
        Ok(StrictValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = BTreeSet::new();
        let mut output = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!(
                    "duplicate JSON object key `{key}`"
                )));
            }
            let value = map.next_value::<StrictValue>()?;
            output.insert(key, value.0);
        }
        Ok(StrictValue(Value::Object(output)))
    }
}

fn normalize_value(value: Value) -> Option<Value> {
    match value {
        Value::String(value) => Some(Value::String(normalize_string(&value))),
        Value::Number(number) => Some(Value::Number(normalize_number(number))),
        Value::Array(items) => Some(Value::Array(
            items
                .into_iter()
                .map(normalize_value)
                .collect::<Option<Vec<_>>>()?,
        )),
        Value::Object(map) => {
            let mut entries = Vec::with_capacity(map.len());
            let mut normalized_keys = BTreeSet::new();
            for (key, value) in map {
                let key = normalize_string(&key);
                if !normalized_keys.insert(key.clone()) {
                    return None;
                }
                entries.push((key, normalize_value(value)?));
            }
            if let Some((_, Value::Array(attributes))) =
                entries.iter_mut().find(|(key, _)| key == "attributes")
            {
                attributes.sort_by_key(attr_key);
            }
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Some(Value::Object(entries.into_iter().collect()))
        }
        other => Some(other),
    }
}

fn normalize_number(number: Number) -> Number {
    if let Some(value) = number.as_i64() {
        return Number::from(value);
    }
    if let Some(value) = number.as_u64() {
        return Number::from(value);
    }
    if let Some(value) = number.as_f64()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value <= i64::MAX as f64
    {
        return Number::from(value as i64);
    }
    number
}

fn attr_key(value: &Value) -> (String, String) {
    match value {
        Value::Object(map) => (
            map.get("trait_type")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            map.get("value").map(Value::to_string).unwrap_or_default(),
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

#[cfg(test)]
mod tests {
    use super::canonicalize_json;

    #[test]
    fn rejects_duplicate_raw_keys() {
        assert!(canonicalize_json(r#"{"name":"a","name":"b"}"#).is_none());
    }

    #[test]
    fn rejects_post_normalization_key_conflicts() {
        assert!(canonicalize_json(r#"{"Name":"a","name":"b"}"#).is_none());
    }

    #[test]
    fn normalizes_integer_float_representation() {
        assert_eq!(
            canonicalize_json(r#"{"value":1.0}"#).unwrap(),
            r#"{"value":1}"#
        );
    }
}
