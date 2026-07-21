use crate::error::{AnalysisError, Result};
use arrow_schema::Schema;

pub const REQUIRED_COLUMNS: [&str; 7] = [
    "chain",
    "contract_address",
    "token_id",
    "name_norm",
    "token_uri_norm",
    "image_uri_norm",
    "metadata_json",
];

pub fn projection_indices(schema: &Schema) -> Result<Vec<usize>> {
    projection_indices_for(schema, &REQUIRED_COLUMNS)
}

pub fn projection_indices_for(schema: &Schema, columns: &[&str]) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|name| {
            schema.index_of(name).map_err(|_| {
                AnalysisError::Input(format!("missing required Parquet column `{name}`"))
            })
        })
        .collect()
}
