//! Fill `EvidenceBundle.controllers` from Alchemy / on-chain (EVM) and Helius (Solana).

use serde_json::{json, Value};

use super::alchemy::FetchOutcome;
use super::http::HttpClient;
use super::types::ProviderEndpoints;

const EIP1967_ADMIN_SLOT: &str =
    "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103";

/// EVM: Alchemy `getContractMetadata` fields + on-chain owner/admin/EIP-1967.
pub async fn fetch_evm_controllers(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
) -> FetchOutcome<Vec<String>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("contract_controllers");
    };
    let Some(nft_url) = endpoints.alchemy_nft(chain, api_key, "getContractMetadata") else {
        return FetchOutcome::failed(
            "alchemy",
            "contract_controllers",
            format!("unsupported alchemy network for {chain}"),
        );
    };
    let Some(rpc_url) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "contract_controllers",
            format!("unsupported alchemy rpc for {chain}"),
        );
    };

    let meta_url = format!("{nft_url}?contractAddress={contract}");
    let mut controllers = Vec::new();
    let mut supplemental_failed = false;
    let mut deployer: Option<String> = None;

    match client.get_json(&meta_url, &[]).await {
        Ok(payload) => {
            if payload.get("error").is_some() {
                supplemental_failed = true;
            } else {
                let metadata = payload.get("contractMetadata").unwrap_or(&payload);
                for field in [
                    "contractDeployer",
                    "ownerAddress",
                    "owner",
                    "adminAddress",
                    "proxyAdminAddress",
                ] {
                    push_evm_address(
                        &mut controllers,
                        metadata
                            .get(field)
                            .or_else(|| payload.get(field))
                            .and_then(Value::as_str),
                    );
                }
                deployer = [
                    "contractDeployer",
                    "deployerAddress",
                    "deployer",
                    "creatorAddress",
                ]
                .into_iter()
                .find_map(|field| {
                    metadata
                        .get(field)
                        .or_else(|| payload.get(field))
                        .and_then(Value::as_str)
                        .and_then(normalize_evm_address)
                });
            }
        }
        Err(_) => {
            supplemental_failed = true;
        }
    }

    match onchain_controllers(client, &rpc_url, contract).await {
        Ok(onchain) => {
            for addr in onchain {
                push_evm_address(&mut controllers, Some(&addr));
            }
        }
        Err(_) => {
            supplemental_failed = true;
        }
    }

    if let Some(deployer) = deployer {
        push_evm_address(&mut controllers, Some(&deployer));
    }

    controllers.sort();
    controllers.dedup();
    let count = controllers.len();
    let mut outcome = FetchOutcome::ok(
        controllers,
        count,
        supplemental_failed,
        "alchemy",
        "contract_controllers",
    );
    // Truncated when supplemental probes failed but we still have some addresses.
    if supplemental_failed && count > 0 {
        outcome.status = super::types::EvidenceStatus::Truncated;
        if let Some(obs) = outcome.observation.as_mut() {
            obs.status = super::types::EvidenceStatus::Truncated;
        }
    }
    outcome
}

async fn onchain_controllers(
    client: &HttpClient,
    rpc_url: &str,
    contract: &str,
) -> Result<Vec<String>, String> {
    let batch = json!([
        {
            "jsonrpc": "2.0",
            "id": "owner",
            "method": "eth_call",
            "params": [{"to": contract, "data": "0x8da5cb5b"}, "latest"]
        },
        {
            "jsonrpc": "2.0",
            "id": "owner-fallback",
            "method": "eth_call",
            "params": [{"to": contract, "data": "0x893d20e8"}, "latest"]
        },
        {
            "jsonrpc": "2.0",
            "id": "admin",
            "method": "eth_call",
            "params": [{"to": contract, "data": "0xf851a440"}, "latest"]
        },
        {
            "jsonrpc": "2.0",
            "id": "eip1967-admin",
            "method": "eth_getStorageAt",
            "params": [contract, EIP1967_ADMIN_SLOT, "latest"]
        }
    ]);
    let payload = client
        .post_json(rpc_url, &[], &batch)
        .await
        .map_err(|e| e.to_string())?;
    let rows = payload
        .as_array()
        .ok_or_else(|| "Alchemy controller batch response was not an array".to_owned())?;
    let storage_complete = rows.iter().any(|row| {
        row.get("id").and_then(Value::as_str) == Some("eip1967-admin")
            && row.get("result").and_then(Value::as_str).is_some()
    });
    if !storage_complete {
        return Err("Alchemy controller batch omitted the EIP-1967 storage result".into());
    }

    let mut controllers = Vec::new();
    let mut owner = None;
    let mut owner_fallback = None;
    for row in rows {
        let id = row.get("id").and_then(Value::as_str).unwrap_or_default();
        let Some(address) = abi_address(row.get("result").and_then(Value::as_str)) else {
            continue;
        };
        match id {
            "owner" => owner = Some(address),
            "owner-fallback" => owner_fallback = Some(address),
            _ => controllers.push(address),
        }
    }
    if let Some(address) = owner.or(owner_fallback) {
        controllers.push(address);
    }
    controllers.sort();
    controllers.dedup();
    Ok(controllers)
}

fn abi_address(value: Option<&str>) -> Option<String> {
    let raw = value?.trim();
    let hex = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X"))?;
    if hex.len() < 40 {
        return None;
    }
    let address = &hex[hex.len() - 40..];
    if address.bytes().all(|byte| byte == b'0')
        || !address.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(format!("0x{}", address.to_ascii_lowercase()))
}

fn push_evm_address(values: &mut Vec<String>, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return;
    };
    let Some(value) = normalize_evm_address(value) else {
        return;
    };
    values.push(value);
}

pub fn normalize_evm_address(value: &str) -> Option<String> {
    let hex = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))?;
    if hex.len() != 40
        || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())
        || hex.bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    Some(format!("0x{}", hex.to_ascii_lowercase()))
}

/// Extract collection `updateAuthority` (+ verified creators) from a DAS asset item / result.
pub fn solana_authorities_from_asset(item: &Value, result: &Value, collection: &str) -> Vec<String> {
    let mut out = Vec::new();
    let metadata = item
        .get("grouping")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|group| {
            let key = group
                .get("group_key")
                .or_else(|| group.get("groupKey"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            key == "collection"
                && group
                    .get("group_value")
                    .or_else(|| group.get("groupValue"))
                    .and_then(Value::as_str)
                    .is_none_or(|value| value == collection)
        })
        .and_then(|group| {
            group
                .get("collection_metadata")
                .or_else(|| group.get("collectionMetadata"))
        })
        .or_else(|| result.get("collection_metadata"))
        .or_else(|| result.get("collectionMetadata"));

    if let Some(metadata) = metadata {
        for field in ["update_authority", "updateAuthority"] {
            if let Some(addr) = metadata.get(field).and_then(Value::as_str) {
                let trimmed = addr.trim();
                if !trimmed.is_empty() {
                    out.push(trimmed.to_owned());
                }
            }
        }
    }

    // Verified creators on the asset itself.
    let creators = item
        .get("creators")
        .or_else(|| item.pointer("/content/metadata/creators"))
        .and_then(Value::as_array);
    if let Some(creators) = creators {
        for creator in creators {
            let verified = creator
                .get("verified")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !verified {
                continue;
            }
            if let Some(addr) = creator
                .get("address")
                .or_else(|| creator.get("creator"))
                .and_then(Value::as_str)
            {
                let trimmed = addr.trim();
                if !trimmed.is_empty() {
                    out.push(trimmed.to_owned());
                }
            }
        }
    }

    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalize_rejects_zero_and_short() {
        assert!(normalize_evm_address("0x0000000000000000000000000000000000000000").is_none());
        assert!(normalize_evm_address("0xabc").is_none());
        assert_eq!(
            normalize_evm_address("0xAbcDef0123456789AbcDef0123456789AbcDef01").as_deref(),
            Some("0xabcdef0123456789abcdef0123456789abcdef01")
        );
    }

    #[test]
    fn abi_address_takes_trailing_40() {
        let padded =
            "0x000000000000000000000000abcdef0123456789abcdef0123456789abcdef01";
        assert_eq!(
            abi_address(Some(padded)).as_deref(),
            Some("0xabcdef0123456789abcdef0123456789abcdef01")
        );
    }

    #[test]
    fn solana_authorities_prefer_update_authority_and_verified_creators() {
        let item = json!({
            "grouping": [{
                "group_key": "collection",
                "group_value": "Coll1111111111111111111111111111111111111",
                "collection_metadata": {
                    "updateAuthority": "Auth1111111111111111111111111111111111111"
                }
            }],
            "creators": [
                {"address": "Cre11111111111111111111111111111111111111", "verified": true},
                {"address": "Fake1111111111111111111111111111111111111", "verified": false}
            ]
        });
        let authorities = solana_authorities_from_asset(
            &item,
            &Value::Null,
            "Coll1111111111111111111111111111111111111",
        );
        assert!(authorities
            .iter()
            .any(|a| a == "Auth1111111111111111111111111111111111111"));
        assert!(authorities
            .iter()
            .any(|a| a == "Cre11111111111111111111111111111111111111"));
        assert!(!authorities
            .iter()
            .any(|a| a == "Fake1111111111111111111111111111111111111"));
    }
}
