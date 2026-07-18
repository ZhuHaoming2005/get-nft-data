use dedup_model::{DedupError, ErrorContext, MetadataSourceValidator};
use std::collections::{BTreeMap, BTreeSet};
use unicode_normalization::UnicodeNormalization;

pub const MAX_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_JSON_DEPTH: usize = 128;

#[derive(Clone, Copy, Debug, Default)]
pub struct CanonicalMetadataValidator;

impl MetadataSourceValidator for CanonicalMetadataValidator {
    fn is_valid_metadata(&self, content: &str) -> bool {
        canonicalize_json(content).is_ok()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CanonicalValue {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<CanonicalValue>),
    Object(BTreeMap<String, CanonicalValue>),
}

impl CanonicalValue {
    pub fn node_type(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Number(_) => "number",
            Self::String(_) => "string",
            Self::Array(_) => "array",
            Self::Object(_) => "object",
        }
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.write_canonical(&mut output);
        output
    }

    fn write_canonical(&self, output: &mut Vec<u8>) {
        match self {
            Self::Null => output.extend_from_slice(b"null"),
            Self::Bool(true) => output.extend_from_slice(b"true"),
            Self::Bool(false) => output.extend_from_slice(b"false"),
            Self::Number(number) => output.extend_from_slice(number.as_bytes()),
            Self::String(value) => write_json_string(value, output),
            Self::Array(values) => {
                output.push(b'[');
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(b',');
                    }
                    value.write_canonical(output);
                }
                output.push(b']');
            }
            Self::Object(values) => {
                output.push(b'{');
                for (index, (key, value)) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(b',');
                    }
                    write_json_string(key, output);
                    output.push(b':');
                    value.write_canonical(output);
                }
                output.push(b'}');
            }
        }
    }
}

pub fn canonicalize_json(input: &str) -> Result<CanonicalValue, DedupError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return invalid_metadata("metadata is empty");
    }
    if trimmed.len() > MAX_METADATA_BYTES {
        return invalid_metadata("metadata exceeds 64 KiB");
    }
    if !matches!(trimmed.as_bytes().first(), Some(b'{') | Some(b'[')) {
        return invalid_metadata("metadata must start with an object or array");
    }
    let mut parser = Parser::new(trimmed);
    let mut value = parser.parse_value(0)?;
    parser.skip_whitespace();
    if parser.position != parser.input.len() {
        return invalid_metadata("trailing content after JSON value");
    }
    align_attributes(&mut value);
    Ok(value)
}

pub fn normalize_text(value: &str) -> String {
    value
        .nfkc()
        .flat_map(char::to_lowercase)
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

struct Parser<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Parser<'a> {
    const fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            position: 0,
        }
    }

    fn parse_value(&mut self, depth: usize) -> Result<CanonicalValue, DedupError> {
        if depth > MAX_JSON_DEPTH {
            return invalid_metadata("JSON nesting exceeds 128 levels");
        }
        self.skip_whitespace();
        match self.peek() {
            Some(b'n') => {
                self.consume_literal(b"null")?;
                Ok(CanonicalValue::Null)
            }
            Some(b't') => {
                self.consume_literal(b"true")?;
                Ok(CanonicalValue::Bool(true))
            }
            Some(b'f') => {
                self.consume_literal(b"false")?;
                Ok(CanonicalValue::Bool(false))
            }
            Some(b'"') => self.parse_string().map(CanonicalValue::String),
            Some(b'[') => self.parse_array(depth),
            Some(b'{') => self.parse_object(depth),
            Some(b'-' | b'0'..=b'9') => self.parse_number().map(CanonicalValue::Number),
            _ => invalid_metadata("invalid JSON value"),
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<CanonicalValue, DedupError> {
        self.expect(b'[')?;
        let mut values = Vec::new();
        self.skip_whitespace();
        if self.consume_if(b']') {
            return Ok(CanonicalValue::Array(values));
        }
        loop {
            values.push(self.parse_value(depth.saturating_add(1))?);
            self.skip_whitespace();
            if self.consume_if(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(CanonicalValue::Array(values))
    }

    fn parse_object(&mut self, depth: usize) -> Result<CanonicalValue, DedupError> {
        self.expect(b'{')?;
        let mut values = BTreeMap::new();
        let mut raw_keys = BTreeSet::new();
        self.skip_whitespace();
        if self.consume_if(b'}') {
            return Ok(CanonicalValue::Object(values));
        }
        loop {
            self.skip_whitespace();
            let raw_key = self.parse_string_raw()?;
            if !raw_keys.insert(raw_key.clone()) {
                return invalid_metadata("duplicate object key");
            }
            let key = normalize_text(&raw_key);
            if values.contains_key(&key) {
                return invalid_metadata("object keys conflict after normalization");
            }
            self.skip_whitespace();
            self.expect(b':')?;
            values.insert(key, self.parse_value(depth.saturating_add(1))?);
            self.skip_whitespace();
            if self.consume_if(b'}') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(CanonicalValue::Object(values))
    }

    fn parse_string(&mut self) -> Result<String, DedupError> {
        self.parse_string_raw().map(|value| normalize_text(&value))
    }

    fn parse_string_raw(&mut self) -> Result<String, DedupError> {
        let start = self.position;
        self.expect(b'"')?;
        let mut escaped = false;
        while let Some(byte) = self.peek() {
            self.position += 1;
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                let slice = &self.input[start..self.position];
                return serde_json::from_slice(slice).map_err(|error| {
                    DedupError::InvalidMetadata {
                        context: ErrorContext::stage("metadata_parse"),
                        message: error.to_string(),
                    }
                });
            } else if byte < 0x20 {
                return invalid_metadata("unescaped control character in string");
            }
        }
        invalid_metadata("unterminated JSON string")
    }

    fn parse_number(&mut self) -> Result<String, DedupError> {
        let start = self.position;
        self.consume_if(b'-');
        match self.peek() {
            Some(b'0') => {
                self.position += 1;
                if matches!(self.peek(), Some(b'0'..=b'9')) {
                    return invalid_metadata("leading zero in JSON number");
                }
            }
            Some(b'1'..=b'9') => self.consume_digits(),
            _ => return invalid_metadata("invalid JSON number"),
        }
        if self.consume_if(b'.') {
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return invalid_metadata("fraction requires a digit");
            }
            self.consume_digits();
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.position += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.position += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return invalid_metadata("exponent requires a digit");
            }
            self.consume_digits();
        }
        let raw = std::str::from_utf8(&self.input[start..self.position]).map_err(|error| {
            DedupError::InvalidMetadata {
                context: ErrorContext::stage("metadata_parse"),
                message: error.to_string(),
            }
        })?;
        canonical_number(raw)
    }

    fn consume_digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.position += 1;
        }
    }

    fn consume_literal(&mut self, expected: &[u8]) -> Result<(), DedupError> {
        if self
            .input
            .get(self.position..self.position + expected.len())
            == Some(expected)
        {
            self.position += expected.len();
            Ok(())
        } else {
            invalid_metadata("invalid JSON literal")
        }
    }

    fn expect(&mut self, expected: u8) -> Result<(), DedupError> {
        self.skip_whitespace();
        if self.consume_if(expected) {
            Ok(())
        } else {
            invalid_metadata("unexpected JSON token")
        }
    }

    fn consume_if(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.position).copied()
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.position += 1;
        }
    }
}

fn canonical_number(raw: &str) -> Result<String, DedupError> {
    let (negative, unsigned) = raw
        .strip_prefix('-')
        .map_or((false, raw), |value| (true, value));
    let (mantissa, exponent) = if let Some(position) = unsigned.find(['e', 'E']) {
        let exponent =
            unsigned[position + 1..]
                .parse::<i64>()
                .map_err(|_| DedupError::InvalidMetadata {
                    context: ErrorContext::stage("metadata_parse"),
                    message: "number exponent is out of range".to_owned(),
                })?;
        (&unsigned[..position], exponent)
    } else {
        (unsigned, 0)
    };
    let (integer, fraction) = mantissa
        .split_once('.')
        .map_or((mantissa, ""), |parts| parts);
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(integer);
    digits.push_str(fraction);
    let first_nonzero = digits.find(|character| character != '0');
    let Some(first_nonzero) = first_nonzero else {
        return Ok("0e0".to_owned());
    };
    digits.drain(..first_nonzero);
    let mut decimal_exponent = exponent
        .checked_sub(
            i64::try_from(fraction.len()).map_err(|_| DedupError::InvalidMetadata {
                context: ErrorContext::stage("metadata_parse"),
                message: "number fraction is too long".to_owned(),
            })?,
        )
        .ok_or_else(|| DedupError::InvalidMetadata {
            context: ErrorContext::stage("metadata_parse"),
            message: "number exponent is out of range".to_owned(),
        })?;
    while digits.ends_with('0') {
        digits.pop();
        decimal_exponent =
            decimal_exponent
                .checked_add(1)
                .ok_or_else(|| DedupError::InvalidMetadata {
                    context: ErrorContext::stage("metadata_parse"),
                    message: "number exponent is out of range".to_owned(),
                })?;
    }
    let scientific_exponent = decimal_exponent
        .checked_add(
            i64::try_from(digits.len())
                .map_err(|_| DedupError::InvalidMetadata {
                    context: ErrorContext::stage("metadata_parse"),
                    message: "number is too long".to_owned(),
                })?
                .saturating_sub(1),
        )
        .ok_or_else(|| DedupError::InvalidMetadata {
            context: ErrorContext::stage("metadata_parse"),
            message: "number exponent is out of range".to_owned(),
        })?;
    Ok(format!(
        "{}{}e{}",
        if negative { "-" } else { "" },
        digits,
        scientific_exponent
    ))
}

fn align_attributes(value: &mut CanonicalValue) {
    match value {
        CanonicalValue::Array(values) => {
            for value in values {
                align_attributes(value);
            }
        }
        CanonicalValue::Object(values) => {
            for value in values.values_mut() {
                align_attributes(value);
            }
            if let Some(CanonicalValue::Array(attributes)) = values.get_mut("attributes") {
                attributes.sort_by_cached_key(|attribute| {
                    let trait_type = match attribute {
                        CanonicalValue::Object(fields) => fields
                            .get("trait_type")
                            .and_then(|value| match value {
                                CanonicalValue::String(value) => Some(value.clone()),
                                _ => None,
                            })
                            .unwrap_or_default(),
                        _ => String::new(),
                    };
                    (trait_type, attribute.canonical_bytes())
                });
            }
        }
        _ => {}
    }
}

fn write_json_string(value: &str, output: &mut Vec<u8>) {
    let encoded = serde_json::to_vec(value).expect("serializing a string cannot fail");
    output.extend_from_slice(&encoded);
}

fn invalid_metadata<T>(message: &str) -> Result<T, DedupError> {
    Err(DedupError::InvalidMetadata {
        context: ErrorContext::stage("metadata_parse"),
        message: message.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(value: &str) -> Vec<u8> {
        canonicalize_json(value).unwrap().canonical_bytes()
    }

    #[test]
    fn representational_differences_are_canonicalized() {
        let left = r#"{"Name":"  CAFÉ  ","n":1.00,"attributes":[{"value":"B","trait_type":"z"},{"trait_type":"a","value":"A"}]}"#;
        let right = r#"{"attributes":[{"value":"a","trait_type":"A"},{"trait_type":"Z","value":"b"}],"n":1e0,"name":"café"}"#;
        assert_eq!(bytes(left), bytes(right));
    }

    #[test]
    fn real_value_changes_remain_different() {
        assert_ne!(bytes(r#"{"value":"one"}"#), bytes(r#"{"value":"two"}"#));
    }

    #[test]
    fn duplicate_and_normalized_conflicting_keys_are_rejected() {
        assert!(canonicalize_json(r#"{"name":1,"name":2}"#).is_err());
        assert!(canonicalize_json(r#"{"Name":1,"name":2}"#).is_err());
    }

    #[test]
    fn number_normalization_is_exact_and_bounded() {
        assert_eq!(canonical_number("1.00").unwrap(), "1e0");
        assert_eq!(canonical_number("100e-2").unwrap(), "1e0");
        assert_eq!(canonical_number("0.0010").unwrap(), "1e-3");
        assert_eq!(canonical_number("-0.0").unwrap(), "0e0");
        assert!(canonical_number("1e999999999999999999999").is_err());
    }

    #[test]
    fn invalid_metadata_boundaries_are_enforced() {
        assert!(canonicalize_json("").is_err());
        assert!(canonicalize_json("\"scalar\"").is_err());
        assert!(canonicalize_json("{\"x\":").is_err());
        let too_large = format!("{{\"x\":\"{}\"}}", "a".repeat(MAX_METADATA_BYTES));
        assert!(canonicalize_json(&too_large).is_err());
    }

    #[test]
    fn excessive_nesting_is_rejected_before_recursive_stack_growth() {
        let nested = format!(
            "{}0{}",
            "[".repeat(MAX_JSON_DEPTH + 2),
            "]".repeat(MAX_JSON_DEPTH + 2)
        );
        assert!(matches!(
            canonicalize_json(&nested),
            Err(DedupError::InvalidMetadata { .. })
        ));
    }
}
