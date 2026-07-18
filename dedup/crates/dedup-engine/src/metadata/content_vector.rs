use super::CanonicalValue;
use dedup_model::{DedupError, ErrorContext};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RawContentVector {
    pub term_frequencies: BTreeMap<Vec<u8>, u32>,
    pub document_length: u32,
}

pub fn vectorize_content(value: &CanonicalValue) -> Result<RawContentVector, DedupError> {
    let mut terms = BTreeMap::new();
    let mut path = Vec::new();
    emit_node_terms(value, &mut path, &mut terms)?;
    let document_length = terms.values().try_fold(0_u32, |total, count| {
        total
            .checked_add(*count)
            .ok_or(DedupError::CounterOverflow {
                counter: "metadata_document_terms",
            })
    })?;
    Ok(RawContentVector {
        term_frequencies: terms,
        document_length,
    })
}

pub fn structural_features(value: &CanonicalValue) -> BTreeSet<Vec<u8>> {
    let mut features = BTreeSet::new();
    let mut path = Vec::new();
    collect_structure(value, &mut path, &mut features);
    features
}

fn emit_node_terms(
    value: &CanonicalValue,
    path: &mut Vec<String>,
    terms: &mut BTreeMap<Vec<u8>, u32>,
) -> Result<(), DedupError> {
    add_term(
        terms,
        encode_term(path, b"type", value.node_type().as_bytes()),
    )?;
    match value {
        CanonicalValue::Null => {
            add_term(terms, encode_term(path, b"scalar", b"null"))?;
        }
        CanonicalValue::Bool(value) => {
            add_term(
                terms,
                encode_term(path, b"scalar", if *value { b"true" } else { b"false" }),
            )?;
        }
        CanonicalValue::Number(value) | CanonicalValue::String(value) => {
            add_term(terms, encode_term(path, b"scalar", value.as_bytes()))?;
            for word in value.split_whitespace() {
                add_term(terms, encode_term(path, b"word", word.as_bytes()))?;
            }
        }
        CanonicalValue::Array(values) => {
            path.push("[]".to_owned());
            for value in values {
                emit_node_terms(value, path, terms)?;
            }
            path.pop();
        }
        CanonicalValue::Object(values) => {
            for (key, value) in values {
                path.push(key.clone());
                emit_node_terms(value, path, terms)?;
                path.pop();
            }
        }
    }
    Ok(())
}

fn collect_structure(
    value: &CanonicalValue,
    path: &mut Vec<String>,
    features: &mut BTreeSet<Vec<u8>>,
) {
    features.insert(encode_term(path, b"type", value.node_type().as_bytes()));
    match value {
        CanonicalValue::Array(values) => {
            path.push("[]".to_owned());
            for value in values {
                collect_structure(value, path, features);
            }
            path.pop();
        }
        CanonicalValue::Object(values) => {
            for (key, value) in values {
                path.push(key.clone());
                collect_structure(value, path, features);
                path.pop();
            }
        }
        _ => {}
    }
}

fn add_term(terms: &mut BTreeMap<Vec<u8>, u32>, term: Vec<u8>) -> Result<(), DedupError> {
    let count = terms.entry(term).or_default();
    *count = count.checked_add(1).ok_or(DedupError::CounterOverflow {
        counter: "metadata_term_frequency",
    })?;
    Ok(())
}

pub fn encode_term(path: &[String], kind: &[u8], value: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    write_part(&mut output, kind);
    for component in path {
        write_part(&mut output, component.as_bytes());
    }
    write_part(&mut output, value);
    output
}

fn write_part(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.extend_from_slice(value);
}

pub fn scalar_paths(value: &CanonicalValue) -> Vec<(Vec<String>, String)> {
    let mut scalars = Vec::new();
    let mut path = Vec::new();
    collect_scalars(value, &mut path, &mut scalars);
    scalars
}

fn collect_scalars(
    value: &CanonicalValue,
    path: &mut Vec<String>,
    scalars: &mut Vec<(Vec<String>, String)>,
) {
    match value {
        CanonicalValue::Bool(value) => {
            scalars.push((path.clone(), value.to_string()));
        }
        CanonicalValue::Number(value) | CanonicalValue::String(value) => {
            scalars.push((path.clone(), value.clone()));
        }
        CanonicalValue::Array(values) => {
            path.push("[]".to_owned());
            for value in values {
                collect_scalars(value, path, scalars);
            }
            path.pop();
        }
        CanonicalValue::Object(values) => {
            for (key, value) in values {
                path.push(key.clone());
                collect_scalars(value, path, scalars);
                path.pop();
            }
        }
        CanonicalValue::Null => {}
    }
}

pub fn require_nonempty_vector(vector: &RawContentVector) -> Result<(), DedupError> {
    if vector.document_length == 0 {
        return Err(DedupError::InvariantViolation {
            context: ErrorContext::stage("metadata_vector"),
            message: "canonical JSON emitted no terms".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::canonicalize_json;

    #[test]
    fn length_encoding_prevents_separator_collisions() {
        assert_ne!(
            encode_term(&["a".to_owned()], b"bc", b"d"),
            encode_term(&["ab".to_owned()], b"c", b"d")
        );
    }

    #[test]
    fn vector_contains_structure_exact_scalar_and_words() {
        let value = canonicalize_json(r#"{"name":"hello world"}"#).unwrap();
        let vector = vectorize_content(&value).unwrap();
        assert!(vector.document_length >= 4);
    }
}
