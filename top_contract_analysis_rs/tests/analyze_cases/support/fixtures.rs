use super::*;

pub(in crate::analyze_cases) fn current_supply_snapshot_rows(
    token_count: u64,
) -> Vec<DatabaseNftRecord> {
    (1..=token_count)
        .map(|token_id| DatabaseNftRecord {
            contract_address: "0xdup".into(),
            token_id: token_id.to_string(),
            token_uri: if token_id == 1 {
                "ipfs://seed/1".into()
            } else {
                format!("ipfs://candidate/{token_id}")
            },
            image_uri: if token_id == 1 {
                "ipfs://image/1.png".into()
            } else {
                format!("ipfs://candidate/{token_id}.png")
            },
            name: format!("Azuki Mirror #{token_id}"),
            symbol: "AZUKI".into(),
            metadata_json: format!(r#"{{"name":"Azuki Mirror #{token_id}"}}"#),
            metadata_recall_checked: false,
            metadata_recall_match: false,
        })
        .collect()
}
