use std::collections::{BTreeSet, HashSet};

use duckdb::{params, Connection, OptionalExt};

use crate::analysis::signals::analyze_transfer_signals;
use crate::error::AppError;
use crate::models::{
    AddressSignals, OwnerBalance, TransferRecord, VictimSignalPayload, ZERO_ADDRESS,
};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CachedSignals {
    pub mint_recipients: Vec<String>,
    pub active_sellers: Vec<String>,
    pub address_signals: AddressSignals,
    pub victim_signals: Option<VictimSignalPayload>,
    pub transfers: Vec<TransferRecord>,
    pub owners: Vec<OwnerBalance>,
}

pub struct ContractSignalCache {
    conn: Connection,
}

fn analyze_victim_signals_from_active_sellers(
    active_sellers: &[String],
    owners: &[OwnerBalance],
) -> VictimSignalPayload {
    let active_seller_set: HashSet<&str> = active_sellers
        .iter()
        .map(String::as_str)
        .filter(|seller| !seller.is_empty() && *seller != ZERO_ADDRESS)
        .collect();

    let mut owner_count = 0_i64;
    let mut stuck_holder_count = 0_i64;
    for owner in owners {
        if owner.owner_address.is_empty() || owner.owner_address == ZERO_ADDRESS {
            continue;
        }
        let has_positive_balance = owner.token_balances.values().any(|balance| *balance > 0);
        if !has_positive_balance {
            continue;
        }
        owner_count += 1;
        if !active_seller_set.contains(owner.owner_address.as_str()) {
            stuck_holder_count += 1;
        }
    }

    VictimSignalPayload {
        owner_count,
        stuck_holder_count,
        stuck_holder_ratio: Some(if owner_count > 0 {
            stuck_holder_count as f64 / owner_count as f64
        } else {
            0.0
        }),
        victim_wallet_count: stuck_holder_count,
    }
}

impl ContractSignalCache {
    pub fn new(database_path: &str) -> Result<Self, AppError> {
        let conn = if database_path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(database_path)?
        };
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS contract_signal_cache (
                chain VARCHAR NOT NULL,
                contract_address VARCHAR NOT NULL,
                token_type VARCHAR NOT NULL,
                mint_recipients_json VARCHAR NOT NULL,
                active_sellers_json VARCHAR NOT NULL,
                address_signals_json VARCHAR NOT NULL,
                victim_signals_json VARCHAR,
                transfers_json VARCHAR NOT NULL,
                owners_json VARCHAR NOT NULL,
                PRIMARY KEY (chain, contract_address, token_type)
            );
            ",
        )?;
        Ok(Self { conn })
    }

    pub fn get(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Option<CachedSignals>, AppError> {
        let row = self
            .conn
            .query_row(
                "
                SELECT mint_recipients_json, active_sellers_json, address_signals_json,
                       victim_signals_json, transfers_json, owners_json
                FROM contract_signal_cache
                WHERE chain = ? AND contract_address = ? AND token_type = ?
                ",
                params![chain, contract_address.to_lowercase(), token_type],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?;

        let Some((
            mint_recipients_json,
            active_sellers_json,
            address_signals_json,
            victim_signals_json,
            transfers_json,
            owners_json,
        )) = row
        else {
            return Ok(None);
        };

        Ok(Some(CachedSignals {
            mint_recipients: serde_json::from_str(&mint_recipients_json)?,
            active_sellers: serde_json::from_str(&active_sellers_json)?,
            address_signals: serde_json::from_str(&address_signals_json)?,
            victim_signals: victim_signals_json
                .as_deref()
                .map(serde_json::from_str::<VictimSignalPayload>)
                .transpose()?,
            transfers: serde_json::from_str(&transfers_json)?,
            owners: serde_json::from_str(&owners_json)?,
        }))
    }

    pub fn put(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
        transfers: &[TransferRecord],
        owners: &[OwnerBalance],
    ) -> Result<(), AppError> {
        let mint_recipients: Vec<String> = transfers
            .iter()
            .filter(|item| item.from_address == ZERO_ADDRESS && !item.to_address.is_empty())
            .map(|item| item.to_address.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let active_sellers: Vec<String> = transfers
            .iter()
            .filter(|item| !item.from_address.is_empty() && item.from_address != ZERO_ADDRESS)
            .map(|item| item.from_address.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let address_signals = analyze_transfer_signals(transfers);
        let victim_signals = if owners.is_empty() {
            None
        } else {
            Some(analyze_victim_signals_from_active_sellers(
                &active_sellers,
                owners,
            ))
        };

        self.conn.execute(
            "
            INSERT OR REPLACE INTO contract_signal_cache (
                chain, contract_address, token_type, mint_recipients_json, active_sellers_json,
                address_signals_json, victim_signals_json, transfers_json, owners_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
            params![
                chain,
                contract_address.to_lowercase(),
                token_type,
                serde_json::to_string(&mint_recipients)?,
                serde_json::to_string(&active_sellers)?,
                serde_json::to_string(&address_signals)?,
                victim_signals
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                serde_json::to_string(transfers)?,
                serde_json::to_string(owners)?,
            ],
        )?;
        Ok(())
    }
}
