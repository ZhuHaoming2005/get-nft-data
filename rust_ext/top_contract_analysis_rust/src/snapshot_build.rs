use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySet, PyTuple};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
struct SnapshotRowInput {
    contract_address: String,
    token_id: String,
    token_uri: String,
    image_uri: String,
    name: String,
    symbol: String,
    metadata_json: String,
    metadata_doc: String,
    token_uri_norm: String,
    image_uri_norm: String,
    symbol_norm: String,
    name_norm: String,
    metadata_keywords: Vec<String>,
}

#[derive(Clone)]
struct SnapshotSignalInput {
    contract_address: String,
    name_norm: String,
    symbol_norm: String,
    keyword_match: bool,
    name_prefix_match: bool,
    symbol_match: bool,
    uri_match: bool,
    image_match: bool,
}

struct SnapshotAggregate {
    nft_rows: Vec<SnapshotRowInput>,
    contract_names: Vec<(String, String)>,
    symbol_contracts: HashMap<String, Vec<String>>,
    contract_signals: Vec<(String, usize, usize, usize, bool, bool, bool)>,
}

fn aggregate_snapshot_rows(
    rows: Vec<SnapshotRowInput>,
    exact_token_keys: HashSet<String>,
    exact_image_keys: HashSet<String>,
    exact_symbols: HashSet<String>,
    name_prefixes: HashSet<String>,
    metadata_recall_terms: HashSet<String>,
) -> SnapshotAggregate {
    let signal_inputs: Vec<SnapshotSignalInput> = rows
        .par_iter()
        .map(|row| {
            let keyword_match = !metadata_recall_terms.is_empty()
                && row
                    .metadata_keywords
                    .iter()
                    .any(|keyword| metadata_recall_terms.contains(keyword));
            SnapshotSignalInput {
                contract_address: row.contract_address.clone(),
                name_norm: row.name_norm.clone(),
                symbol_norm: row.symbol_norm.clone(),
                keyword_match,
                name_prefix_match: {
                    let prefix: String = row.name_norm.chars().take(8).collect();
                    !prefix.is_empty() && name_prefixes.contains(&prefix)
                },
                symbol_match: exact_symbols.contains(&row.symbol_norm),
                uri_match: exact_token_keys.contains(&row.token_uri_norm),
                image_match: exact_image_keys.contains(&row.image_uri_norm),
            }
        })
        .collect();

    let mut seen_contract_name_pairs: HashSet<(String, String)> = HashSet::new();
    let mut contract_names: Vec<(String, String)> = Vec::new();
    let mut symbol_contracts_raw: HashMap<String, HashSet<String>> = HashMap::new();
    let mut contract_signal_counts: HashMap<String, (usize, usize, usize, bool, bool, bool)> = HashMap::new();

    for signal in signal_inputs.into_iter() {
        if !signal.name_norm.is_empty() {
            let key = (signal.contract_address.clone(), signal.name_norm.clone());
            if seen_contract_name_pairs.insert(key.clone()) {
                contract_names.push(key);
            }
        }
        if !signal.symbol_norm.is_empty() {
            symbol_contracts_raw
                .entry(signal.symbol_norm.clone())
                .or_default()
                .insert(signal.contract_address.clone());
        }
        let entry = contract_signal_counts
            .entry(signal.contract_address.clone())
            .or_insert((0, 0, 0, false, false, false));
        entry.0 += 1;
        if signal.uri_match {
            entry.1 += 1;
        }
        if signal.image_match {
            entry.2 += 1;
        }
        entry.3 = entry.3 || signal.symbol_match;
        entry.4 = entry.4 || signal.name_prefix_match;
        entry.5 = entry.5 || signal.keyword_match;
    }

    let mut symbol_contracts: HashMap<String, Vec<String>> = symbol_contracts_raw
        .into_iter()
        .map(|(symbol, contracts)| {
            let mut values: Vec<String> = contracts.into_iter().collect();
            values.sort();
            (symbol, values)
        })
        .collect();
    let mut contract_signals: Vec<(String, usize, usize, usize, bool, bool, bool)> = contract_signal_counts
        .into_iter()
        .map(|(contract_address, values)| {
            (
                contract_address,
                values.0,
                values.1,
                values.2,
                values.3,
                values.4,
                values.5,
            )
        })
        .collect();
    contract_signals.sort_by(|left, right| left.0.cmp(&right.0));
    let _ = &mut symbol_contracts;

    SnapshotAggregate {
        nft_rows: rows,
        contract_names,
        symbol_contracts,
        contract_signals,
    }
}

#[pyfunction]
pub fn build_database_snapshot(
    py: Python<'_>,
    contract_addresses: Vec<String>,
    token_ids: Vec<String>,
    token_uris: Vec<String>,
    image_uris: Vec<String>,
    names: Vec<String>,
    symbols: Vec<String>,
    metadata_jsons: Vec<String>,
    metadata_docs: Vec<String>,
    token_uri_norms: Vec<String>,
    image_uri_norms: Vec<String>,
    symbol_norms: Vec<String>,
    name_norms: Vec<String>,
    metadata_keywords_arr: Vec<Vec<String>>,
    exact_token_keys: Vec<String>,
    exact_image_keys: Vec<String>,
    exact_symbols: Vec<String>,
    name_prefixes: Vec<String>,
    metadata_recall_terms: Vec<String>,
) -> PyResult<PyObject> {
    let row_count = contract_addresses.len();
    let lengths = [
        token_ids.len(),
        token_uris.len(),
        image_uris.len(),
        names.len(),
        symbols.len(),
        metadata_jsons.len(),
        metadata_docs.len(),
        token_uri_norms.len(),
        image_uri_norms.len(),
        symbol_norms.len(),
        name_norms.len(),
        metadata_keywords_arr.len(),
    ];
    if lengths.iter().any(|length| *length != row_count) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "snapshot columns must have identical lengths",
        ));
    }

    let rows: Vec<SnapshotRowInput> = (0..row_count)
        .map(|idx| SnapshotRowInput {
            contract_address: contract_addresses[idx].clone(),
            token_id: token_ids[idx].clone(),
            token_uri: token_uris[idx].clone(),
            image_uri: image_uris[idx].clone(),
            name: names[idx].clone(),
            symbol: symbols[idx].clone(),
            metadata_json: metadata_jsons[idx].clone(),
            metadata_doc: metadata_docs[idx].clone(),
            token_uri_norm: token_uri_norms[idx].clone(),
            image_uri_norm: image_uri_norms[idx].clone(),
            symbol_norm: symbol_norms[idx].clone(),
            name_norm: name_norms[idx].clone(),
            metadata_keywords: metadata_keywords_arr[idx].clone(),
        })
        .collect();

    let aggregate = py.allow_threads(|| {
        aggregate_snapshot_rows(
            rows,
            exact_token_keys.into_iter().collect(),
            exact_image_keys.into_iter().collect(),
            exact_symbols.into_iter().collect(),
            name_prefixes.into_iter().collect(),
            metadata_recall_terms.into_iter().collect(),
        )
    });

    let models = py.import_bound("top_contract_analysis.models")?;
    let nft_record_cls = models.getattr("DatabaseNFTRecord")?;
    let contract_name_cls = models.getattr("ContractNameRecord")?;
    let contract_signal_cls = models.getattr("ContractSignal")?;
    let snapshot_cls = models.getattr("DatabaseSnapshot")?;

    let nft_rows_py = PyList::empty_bound(py);
    for row in aggregate.nft_rows.into_iter() {
        let item = nft_record_cls.call1((
            row.contract_address,
            row.token_id,
            row.token_uri,
            row.image_uri,
            row.name,
            row.symbol,
            row.metadata_json,
            row.metadata_doc,
        ))?;
        nft_rows_py.append(item)?;
    }

    let contract_names_py = PyList::empty_bound(py);
    for (contract_address, name_norm) in aggregate.contract_names.into_iter() {
        let item = contract_name_cls.call1((contract_address, name_norm))?;
        contract_names_py.append(item)?;
    }

    let symbol_contracts_py = PyDict::new_bound(py);
    for (symbol_norm, contracts) in aggregate.symbol_contracts.into_iter() {
        let contract_set = PySet::new_bound(py, &contracts)?;
        symbol_contracts_py.set_item(symbol_norm, contract_set)?;
    }

    let contract_signals_py = PyDict::new_bound(py);
    for (contract_address, token_count, uri_match_count, image_match_count, symbol_match, name_prefix_match, keyword_match) in aggregate.contract_signals.into_iter() {
        let signal = contract_signal_cls.call1((
            contract_address.clone(),
            token_count,
            uri_match_count,
            image_match_count,
            symbol_match,
            name_prefix_match,
            keyword_match,
        ))?;
        contract_signals_py.set_item(contract_address, signal)?;
    }

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("nft_rows", nft_rows_py)?;
    kwargs.set_item("contract_names", contract_names_py)?;
    kwargs.set_item("symbol_contracts", symbol_contracts_py)?;
    kwargs.set_item("contract_signals", contract_signals_py)?;
    let snapshot = snapshot_cls.call(PyTuple::empty_bound(py), Some(&kwargs))?;
    Ok(snapshot.into_any().unbind())
}
