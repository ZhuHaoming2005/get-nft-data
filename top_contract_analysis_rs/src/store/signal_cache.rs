use std::collections::{BTreeSet, HashSet};
use std::sync::{Mutex, MutexGuard, TryLockError};

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
    conn: Mutex<Connection>,
}

type SignalCacheRow = (String, String, String, Option<String>, String, String);

struct SignalCacheWritePayload {
    mint_recipients_json: String,
    active_sellers_json: String,
    address_signals_json: String,
    victim_signals_json: String,
    transfers_json: String,
    owners_json: String,
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
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn conn(&self) -> Result<MutexGuard<'_, Connection>, AppError> {
        self.conn
            .lock()
            .map_err(|err| AppError::DuckDb(format!("signal cache lock poisoned: {err}")))
    }

    fn try_conn(&self) -> Result<Option<MutexGuard<'_, Connection>>, AppError> {
        match self.conn.try_lock() {
            Ok(conn) => Ok(Some(conn)),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Poisoned(err)) => Err(AppError::DuckDb(format!(
                "signal cache lock poisoned: {err}"
            ))),
        }
    }

    fn read_row(
        conn: &Connection,
        chain: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Option<SignalCacheRow>, AppError> {
        Ok(conn
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
            .optional()?)
    }

    fn decode_row(row: Option<SignalCacheRow>) -> Result<Option<CachedSignals>, AppError> {
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

    fn write_payload(
        transfers: &[TransferRecord],
        owners: &[OwnerBalance],
    ) -> Result<SignalCacheWritePayload, AppError> {
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
        let victim_signals = analyze_victim_signals_from_active_sellers(&active_sellers, owners);
        Ok(SignalCacheWritePayload {
            mint_recipients_json: serde_json::to_string(&mint_recipients)?,
            active_sellers_json: serde_json::to_string(&active_sellers)?,
            address_signals_json: serde_json::to_string(&address_signals)?,
            victim_signals_json: serde_json::to_string(&victim_signals)?,
            transfers_json: serde_json::to_string(transfers)?,
            owners_json: serde_json::to_string(owners)?,
        })
    }

    fn write_row(
        conn: &Connection,
        chain: &str,
        contract_address: &str,
        token_type: &str,
        payload: SignalCacheWritePayload,
    ) -> Result<(), AppError> {
        conn.execute(
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
                payload.mint_recipients_json,
                payload.active_sellers_json,
                payload.address_signals_json,
                payload.victim_signals_json,
                payload.transfers_json,
                payload.owners_json,
            ],
        )?;
        Ok(())
    }

    pub fn get(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Option<CachedSignals>, AppError> {
        let row = {
            let conn = self.conn()?;
            Self::read_row(&conn, chain, contract_address, token_type)?
        };
        Self::decode_row(row)
    }

    pub fn try_get(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
    ) -> Result<Option<CachedSignals>, AppError> {
        let Some(conn) = self.try_conn()? else {
            return Ok(None);
        };
        let row = Self::read_row(&conn, chain, contract_address, token_type)?;
        drop(conn);
        Self::decode_row(row)
    }

    pub fn put(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
        transfers: &[TransferRecord],
        owners: &[OwnerBalance],
    ) -> Result<(), AppError> {
        let payload = Self::write_payload(transfers, owners)?;
        let conn = self.conn()?;
        Self::write_row(&conn, chain, contract_address, token_type, payload)
    }

    pub fn try_put(
        &self,
        chain: &str,
        contract_address: &str,
        token_type: &str,
        transfers: &[TransferRecord],
        owners: &[OwnerBalance],
    ) -> Result<(), AppError> {
        let payload = Self::write_payload(transfers, owners)?;
        let Some(conn) = self.try_conn()? else {
            return Ok(());
        };
        Self::write_row(&conn, chain, contract_address, token_type, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_get_returns_miss_when_cache_lock_is_busy() {
        let cache = ContractSignalCache::new(":memory:").unwrap();
        let _guard = cache.conn.lock().unwrap();

        let cached = cache.try_get("ethereum", "0xdup", "ERC721").unwrap();

        assert!(cached.is_none());
    }

    #[test]
    fn try_put_skips_write_when_cache_lock_is_busy() {
        let cache = ContractSignalCache::new(":memory:").unwrap();
        let transfers = vec![TransferRecord::mint("0xdup", "1", 100, "0xminter")];
        let owners = Vec::new();
        let guard = cache.conn.lock().unwrap();

        cache
            .try_put("ethereum", "0xdup", "ERC721", &transfers, &owners)
            .unwrap();
        drop(guard);

        assert!(cache.get("ethereum", "0xdup", "ERC721").unwrap().is_none());
    }
}
