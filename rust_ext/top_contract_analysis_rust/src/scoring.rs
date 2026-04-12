use crate::common::{
    metadata_document, metadata_keywords_internal, metadata_score, metadata_score_from_documents,
    name_score,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rayon::prelude::*;

#[pyfunction]
pub fn score_name_pairs(py: Python<'_>, left: Vec<String>, right: Vec<String>) -> PyResult<Vec<f64>> {
    if left.len() != right.len() {
        return Err(PyValueError::new_err(
            "left and right sequences must have identical lengths",
        ));
    }
    Ok(py.allow_threads(|| {
        left.par_iter()
            .zip(right.par_iter())
            .map(|(l, r)| name_score(l, r))
            .collect()
    }))
}

#[pyfunction]
pub fn score_metadata_pairs(
    py: Python<'_>,
    left: Vec<String>,
    right: Vec<String>,
) -> PyResult<Vec<f64>> {
    if left.len() != right.len() {
        return Err(PyValueError::new_err(
            "left and right sequences must have identical lengths",
        ));
    }
    Ok(py.allow_threads(|| {
        left.par_iter()
            .zip(right.par_iter())
            .map(|(l, r)| metadata_score(l, r))
            .collect()
    }))
}

#[pyfunction]
pub fn score_metadata_documents(
    py: Python<'_>,
    left: Vec<String>,
    right: Vec<String>,
) -> PyResult<Vec<f64>> {
    if left.len() != right.len() {
        return Err(PyValueError::new_err(
            "left and right sequences must have identical lengths",
        ));
    }
    Ok(py.allow_threads(|| {
        left.par_iter()
            .zip(right.par_iter())
            .map(|(l, r)| metadata_score_from_documents(l, r))
            .collect()
    }))
}

#[pyfunction]
pub fn metadata_document_from_json(py: Python<'_>, raw: String) -> PyResult<String> {
    Ok(py.allow_threads(|| metadata_document(&raw)))
}

#[pyfunction(signature = (document, limit=8))]
pub fn metadata_keywords(py: Python<'_>, document: String, limit: usize) -> PyResult<Vec<String>> {
    Ok(py.allow_threads(|| metadata_keywords_internal(&document, limit)))
}
