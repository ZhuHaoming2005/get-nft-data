use super::*;

pub fn build_secondary_sale_victim_address_records(
    contract_address: &str,
    sales: &[NftSaleRecord],
    transfers: &[TransferRecord],
    owners: &[OwnerBalance],
) -> Vec<SecondarySaleVictimAddressPayload> {
    let activity = prepare_contract_activity(transfers, sales, owners);
    build_secondary_sale_victim_address_records_from_activity(contract_address, &activity)
}

pub(crate) fn build_secondary_sale_victim_address_records_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
) -> Vec<SecondarySaleVictimAddressPayload> {
    build_secondary_sale_victim_address_records_excluding_malicious_from_activity(
        contract_address,
        activity,
        &[],
    )
}

pub(crate) fn build_secondary_sale_victim_address_records_excluding_malicious_from_activity(
    contract_address: &str,
    activity: &PreparedContractActivity<'_>,
    malicious_addresses: &[MaliciousAddressPayload],
) -> Vec<SecondarySaleVictimAddressPayload> {
    let malicious_buyers: HashSet<String> = malicious_addresses
        .iter()
        .map(|item| normalize_chain_identity(&item.address))
        .filter(|value| !value.is_empty())
        .collect();
    let mut grouped: BTreeMap<String, SecondarySaleVictimAddressPayload> = BTreeMap::new();
    let mut last_buy_key: HashMap<String, (i64, i64, i64, String)> = HashMap::new();

    for sale in &activity.sorted_sales {
        let buyer_address = normalize_chain_identity(&sale.buyer_address);
        if buyer_address.is_empty() || malicious_buyers.contains(&buyer_address) {
            continue;
        }
        let later_transfer_out = activity
            .latest_outgoing
            .get(&(sale.buyer_address.clone(), sale.token_id.clone()))
            .map(|transfer_key| {
                transfer_key > &(sale.block_number, sale.log_index, sale.tx_hash.clone())
            })
            .unwrap_or(false);
        let is_stuck = activity
            .owner_token_map
            .get(&sale.buyer_address)
            .map(|held_tokens| held_tokens.contains(&sale.token_id))
            .unwrap_or(false)
            && !later_transfer_out;

        let entry = grouped
            .entry(sale.buyer_address.clone())
            .or_insert_with(|| SecondarySaleVictimAddressPayload {
                contract_address: contract_address.to_string(),
                address: sale.buyer_address.clone(),
                ..SecondarySaleVictimAddressPayload::default()
            });
        entry.buy_tx_hashes.push(sale.tx_hash.clone());
        if sale.price_eth.is_some() {
            entry.buy_amount_eth += sale.price_eth.unwrap_or(0.0);
        }
        if let Some(amount_usd) = sale_usd_value(sale) {
            entry.buy_amount_usd += amount_usd;
        }
        let current_key = (
            sale.block_number,
            sale.log_index,
            sale.bundle_index,
            sale.tx_hash.clone(),
        );
        let should_update_last = last_buy_key
            .get(&sale.buyer_address)
            .map(|existing| &current_key >= existing)
            .unwrap_or(true);
        if should_update_last {
            last_buy_key.insert(sale.buyer_address.clone(), current_key);
            entry.last_buy_tx_hash = sale.tx_hash.clone();
            entry.last_buy_amount_eth = sale.price_eth;
            entry.last_buy_amount_usd = sale_usd_value(sale);
        }
        entry.is_stuck = entry.is_stuck || is_stuck;
    }

    grouped.into_values().collect()
}
