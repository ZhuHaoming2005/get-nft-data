use super::*;

pub fn build_infringing_token_records(
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    transfers: &[TransferRecord],
) -> Vec<InfringingTokenRecord> {
    let candidate_refs: Vec<&DuplicateCandidate> = contract_candidates.iter().collect();
    build_infringing_token_records_with_context_refs(
        contract_address,
        &candidate_refs,
        transfers,
        &HashSet::new(),
        &HashMap::new(),
    )
}

pub fn build_infringing_token_records_with_context(
    contract_address: &str,
    contract_candidates: &[DuplicateCandidate],
    transfers: &[TransferRecord],
    official_addresses: &HashSet<String>,
    candidate_open_license_by_token: &HashMap<(String, String), bool>,
) -> Vec<InfringingTokenRecord> {
    let candidate_refs: Vec<&DuplicateCandidate> = contract_candidates.iter().collect();
    build_infringing_token_records_with_context_refs(
        contract_address,
        &candidate_refs,
        transfers,
        official_addresses,
        candidate_open_license_by_token,
    )
}

pub fn build_infringing_token_records_with_context_refs(
    contract_address: &str,
    contract_candidates: &[&DuplicateCandidate],
    transfers: &[TransferRecord],
    official_addresses: &HashSet<String>,
    candidate_open_license_by_token: &HashMap<(String, String), bool>,
) -> Vec<InfringingTokenRecord> {
    let mut transfers_by_token: HashMap<String, Vec<&TransferRecord>> = HashMap::new();
    for transfer in transfers {
        if transfer.contract_address != contract_address || transfer.token_id.is_empty() {
            continue;
        }
        transfers_by_token
            .entry(transfer.token_id.clone())
            .or_default()
            .push(transfer);
    }
    for token_transfers in transfers_by_token.values_mut() {
        token_transfers
            .sort_by(|left, right| transfer_sort_key(left).cmp(&transfer_sort_key(right)));
    }

    let mut rows: Vec<InfringingTokenRecord> = contract_candidates
        .iter()
        .map(|candidate| {
            let token_transfers = transfers_by_token.get(&candidate.token_id);
            let mint_transfer = token_transfers.and_then(|rows| {
                rows.iter()
                    .find(|row| row.from_address == ZERO_ADDRESS)
                    .copied()
            });
            let first_non_mint_transfer = token_transfers.and_then(|rows| {
                rows.iter()
                    .find(|row| row.from_address != ZERO_ADDRESS)
                    .copied()
            });
            let first_transfer = token_transfers.and_then(|rows| rows.first().copied());
            let (minter_address, mint_tx_hash, mint_block, first_transfer_time) =
                if let Some(mint_transfer) = mint_transfer {
                    (
                        mint_transfer.to_address.clone(),
                        mint_transfer.tx_hash.clone(),
                        mint_transfer.block_number,
                        first_non_mint_transfer
                            .map(|transfer| transfer.block_time)
                            .unwrap_or(0),
                    )
                } else if let Some(first_transfer) = first_transfer {
                    (
                        first_transfer.to_address.clone(),
                        first_transfer.tx_hash.clone(),
                        first_transfer.block_number,
                        first_transfer.block_time,
                    )
                } else {
                    (String::new(), String::new(), 0, 0)
                };

            let official_or_legit_reissue =
                !minter_address.is_empty() && official_addresses.contains(&minter_address);

            InfringingTokenRecord {
                contract_address: contract_address.to_string(),
                token_id: candidate.token_id.clone(),
                mint_tx_hash,
                mint_block,
                minter_address,
                first_transfer_time,
                history_window: "full".to_string(),
                match_reasons: candidate.match_reasons.clone(),
                candidate_open_license: candidate_open_license_by_token
                    .get(&(contract_address.to_string(), candidate.token_id.clone()))
                    .copied()
                    .unwrap_or(false),
                official_or_legit_reissue,
            }
        })
        .collect();

    rows.sort_by(|left, right| {
        (&left.token_id, &left.contract_address).cmp(&(&right.token_id, &right.contract_address))
    });
    rows
}
