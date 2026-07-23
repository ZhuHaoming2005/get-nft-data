//! EVM mode representative Name selection.

use ahash::AHashMap;

use crate::entity::{ContractId, ResidentStore, StringId};

/// Drop empties, null-like placeholders, and single-digit numeric names.
pub fn is_usable_name(name: &str) -> bool {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return false;
    }
    const NULL_LIKE: [&str; 11] = [
        "none",
        "null",
        "nil",
        "undefined",
        "n/a",
        "na",
        "n.a.",
        "nan",
        "-",
        "--",
        ".",
    ];
    if NULL_LIKE
        .iter()
        .any(|null_like| trimmed.eq_ignore_ascii_case(null_like))
    {
        return false;
    }
    !(trimmed.len() == 1 && trimmed.as_bytes()[0].is_ascii_digit())
}

/// Mode by NFT count; ties → lexicographically smallest `name_norm`.
pub fn select_evm_representatives(store: &ResidentStore) -> Vec<(ContractId, StringId)> {
    let mut counts: AHashMap<(ContractId, StringId), u64> = AHashMap::new();
    for nft in &store.nfts {
        let Some(name_id) = nft.name_id else {
            continue;
        };
        let contract = &store.contracts[nft.contract_id as usize];
        let chain = store.chain_name(contract.chain_id);
        if !store.is_evm_chain(chain) {
            continue;
        }
        if !is_usable_name(store.string(name_id)) {
            continue;
        }
        *counts.entry((nft.contract_id, name_id)).or_default() += 1;
    }

    let mut by_contract: AHashMap<ContractId, (StringId, u64)> = AHashMap::new();
    for ((contract_id, name_id), count) in counts {
        match by_contract.get_mut(&contract_id) {
            Some((selected, selected_count)) => {
                let replace = count > *selected_count
                    || (count == *selected_count
                        && store.string(name_id) < store.string(*selected));
                if replace {
                    *selected = name_id;
                    *selected_count = count;
                }
            }
            None => {
                by_contract.insert(contract_id, (name_id, count));
            }
        }
    }

    let mut out: Vec<(ContractId, StringId)> = by_contract
        .into_iter()
        .map(|(contract_id, (name_id, _))| (contract_id, name_id))
        .collect();
    out.sort_unstable_by_key(|(contract_id, _)| *contract_id);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{IdentityRow, SourceOrder};
    use ahash::AHashSet;

    fn row(chain: &str, contract: &str, token: &str, name: &str, n: u64) -> IdentityRow {
        IdentityRow {
            chain: chain.to_owned(),
            contract_address: contract.to_owned(),
            token_id: token.to_owned(),
            name_norm: name.to_owned(),
            token_uri_norm: String::new(),
            image_uri_norm: String::new(),
            source_order: SourceOrder {
                file_ordinal: 0,
                file_row_number: n,
            },
        }
    }

    #[test]
    fn mode_tie_breaks_lex_smallest() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(8, &evm);
        // "Beta" and "Alpha" each once → Alpha wins lex; add another Alpha → still Alpha.
        store
            .ingest_identity_row(row("ethereum", "0xa", "1", "Beta", 1))
            .unwrap();
        store
            .ingest_identity_row(row("ethereum", "0xa", "2", "Alpha", 2))
            .unwrap();
        let reps = select_evm_representatives(&store);
        assert_eq!(reps.len(), 1);
        assert_eq!(store.string(reps[0].1), "Alpha");
    }

    #[test]
    fn mode_prefers_higher_count() {
        let evm = ["ethereum".to_owned()].into_iter().collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(8, &evm);
        store
            .ingest_identity_row(row("ethereum", "0xa", "1", "Zed", 1))
            .unwrap();
        store
            .ingest_identity_row(row("ethereum", "0xa", "2", "Zed", 2))
            .unwrap();
        store
            .ingest_identity_row(row("ethereum", "0xa", "3", "Alpha", 3))
            .unwrap();
        let reps = select_evm_representatives(&store);
        assert_eq!(store.string(reps[0].1), "Zed");
    }

    #[test]
    fn drops_null_like_and_single_digit() {
        assert!(!is_usable_name("null"));
        assert!(!is_usable_name("5"));
        assert!(!is_usable_name(""));
        assert!(is_usable_name("CoolCats"));
    }
}
