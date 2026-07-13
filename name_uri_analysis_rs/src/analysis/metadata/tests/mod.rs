use super::super::analysis_contracts_sql;
use super::parse::*;
use super::*;
use crate::analysis::PipelineStage;

/// Concatenated `index/` sources so structure pins keep working after the split.
const INDEX_SOURCE: &str = concat!(
    include_str!("../index/mod.rs"),
    include_str!("../index/atoms.rs"),
    include_str!("../index/postings.rs"),
    include_str!("../index/conservative.rs"),
    include_str!("../index/template_cache.rs"),
    include_str!("../index/scratch.rs"),
    include_str!("../index/waves.rs"),
    include_str!("../index/union.rs"),
);

fn metadata_doc_entry(text: &str) -> SourceMetadataDocEntry {
    SourceMetadataDocEntry {
        doc: MetadataBm25Document::from_text(text).unwrap().into(),
        contracts: vec![0],
    }
}

fn high_cardinality_metadata_json() -> String {
    const BASE36: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut description = String::new();
    for index in 0usize.. {
        let token = String::from_utf8(vec![
            BASE36[(index / (36 * 36)) % 36],
            BASE36[(index / 36) % 36],
            BASE36[index % 36],
        ])
        .unwrap();
        if description
            .len()
            .saturating_add(token.len())
            .saturating_add(32)
            >= MAX_METADATA_BYTES_FOR_DEDUP
        {
            break;
        }
        if !description.is_empty() {
            description.push(' ');
        }
        description.push_str(&token);
    }
    format!(r#"{{"description":"{description}"}}"#)
}

mod bm25_parse;
mod budget;
mod conservative;
mod fallback;
mod index_scoring;
mod load;
mod progress;
mod structure_pins;
