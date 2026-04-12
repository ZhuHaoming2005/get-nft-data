mod address_analysis;
mod common;
mod duplicate;
mod scoring;
mod signals;
mod snapshot_build;

use pyo3::prelude::*;

#[pymodule]
fn top_contract_analysis_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scoring::score_name_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(scoring::score_metadata_pairs, m)?)?;
    m.add_function(wrap_pyfunction!(scoring::score_metadata_documents, m)?)?;
    m.add_function(wrap_pyfunction!(scoring::metadata_document_from_json, m)?)?;
    m.add_function(wrap_pyfunction!(scoring::metadata_keywords, m)?)?;
    m.add_function(wrap_pyfunction!(signals::analyze_transfer_signals, m)?)?;
    m.add_function(wrap_pyfunction!(signals::analyze_victim_signals, m)?)?;
    m.add_function(wrap_pyfunction!(duplicate::build_duplicate_candidates, m)?)?;
    m.add_function(wrap_pyfunction!(address_analysis::build_infringing_token_records, m)?)?;
    m.add_function(wrap_pyfunction!(address_analysis::build_malicious_address_records, m)?)?;
    m.add_function(wrap_pyfunction!(address_analysis::build_victim_address_records, m)?)?;
    m.add_function(wrap_pyfunction!(address_analysis::build_honest_address_records, m)?)?;
    m.add_function(wrap_pyfunction!(snapshot_build::build_database_snapshot, m)?)?;
    Ok(())
}
