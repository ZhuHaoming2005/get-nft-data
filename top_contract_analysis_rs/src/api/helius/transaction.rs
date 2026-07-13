use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::api::AsyncApiClient;
use crate::error::AppError;
use crate::models::{
    EthTransferRecord, NftSaleRecord, TransactionReceiptRecord, TransferRecord, ZERO_ADDRESS,
};

use super::history::HeliusTransactionAssetRef;
use super::{HeliusCollectionHistory, HeliusTransactionDetails};

const BUBBLEGUM_PROGRAM_ID: &str = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY";
const BUBBLEGUM_TRANSFER_V1_DISCRIMINATOR: [u8; 8] = [163, 52, 200, 231, 140, 3, 69, 186];
const BUBBLEGUM_TRANSFER_V2_DISCRIMINATOR: [u8; 8] = [119, 40, 6, 235, 234, 221, 248, 49];
const BUBBLEGUM_MINT_V1_DISCRIMINATOR: [u8; 8] = [145, 98, 192, 118, 184, 147, 118, 104];
const BUBBLEGUM_MINT_TO_COLLECTION_V1_DISCRIMINATOR: [u8; 8] = [153, 18, 178, 47, 197, 158, 86, 15];
const WRAPPED_SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const SOLANA_USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub async fn fetch_helius_transaction_details(
    client: &AsyncApiClient,
    rpc_url: &str,
    signature: &str,
    native_usd_rate: Option<f64>,
) -> Result<Option<HeliusTransactionDetails>, AppError> {
    Ok(fetch_helius_transaction_value(client, rpc_url, signature)
        .await?
        .as_ref()
        .map(|result| parse_transaction_details(result, None, native_usd_rate)))
}

pub(super) async fn fetch_helius_transaction_value(
    client: &AsyncApiClient,
    rpc_url: &str,
    signature: &str,
) -> Result<Option<Value>, AppError> {
    let payload: Value = client
        .post_json(
            rpc_url,
            &serde_json::json!({
                "jsonrpc": "2.0",
                "id": format!("transaction-{signature}"),
                "method": "getTransaction",
                "params": [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "commitment": "finalized",
                        "maxSupportedTransactionVersion": 0
                    }
                ]
            }),
        )
        .await?;
    if let Some(error) = payload.get("error") {
        return Err(AppError::Http(format!(
            "Helius getTransaction failed for {signature}: {error}"
        )));
    }
    Ok(payload
        .get("result")
        .filter(|value| !value.is_null())
        .cloned())
}

pub(super) fn append_helius_asset_transaction(
    history: &mut HeliusCollectionHistory,
    collection_address: &str,
    asset_ref: &HeliusTransactionAssetRef,
    signature: &str,
    result: &Value,
    details: &HeliusTransactionDetails,
) {
    history.fetched_transaction_count += 1;
    let tx_hash = if details.receipt.tx_hash.is_empty() {
        signature.to_string()
    } else {
        details.receipt.tx_hash.clone()
    };
    if !details.native_transfer_attribution_complete {
        history
            .unattributed_native_transaction_signatures
            .insert(tx_hash.clone());
    }
    let meta = result.get("meta").unwrap_or(&Value::Null);
    if meta.get("err").is_some_and(|error| !error.is_null()) {
        return;
    }
    let owner_change = token_owner_change(meta, &asset_ref.mint_address).or_else(|| {
        compressed_nft_owner_change(result, &asset_ref.asset_event_type, &details.account_keys)
    });
    let Some((before, after, is_mint)) = owner_change else {
        if asset_ref.compressed
            && asset_ref
                .asset_event_type
                .trim()
                .to_ascii_lowercase()
                .starts_with("mint")
        {
            history.unresolved_compressed_mint_count += 1;
        }
        return;
    };
    let seller = before.clone().unwrap_or_default();
    let buyer = after.clone();
    if !is_mint {
        if let Some(mut sale) = sale_from_owner_and_payment_changes(
            collection_address,
            &asset_ref.mint_address,
            &tx_hash,
            (
                result
                    .get("slot")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
                asset_ref.event_index as i64,
            ),
            &seller,
            &buyer,
            &details.native_transfers,
        ) {
            if transaction_nft_owner_change_count(meta) > 1 {
                clear_sale_amounts(&mut sale);
            }
            history.sales.push(sale);
        }
    }
    history.transfers.push(TransferRecord {
        contract_address: collection_address.trim().to_string(),
        token_id: asset_ref.mint_address.trim().to_string(),
        tx_hash,
        log_index: asset_ref.event_index as i64,
        block_number: result
            .get("slot")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        block_time: result
            .get("blockTime")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        from_address: before.unwrap_or_else(|| ZERO_ADDRESS.to_string()),
        to_address: after,
        event_type: if is_mint {
            "mint".to_string()
        } else {
            "transfer".to_string()
        },
        source: "helius".to_string(),
    });
}

pub(super) fn parse_transaction_details(
    result: &Value,
    slot_override: Option<i64>,
    native_usd_rate: Option<f64>,
) -> HeliusTransactionDetails {
    let transaction = result.get("transaction").unwrap_or(&Value::Null);
    let message = transaction.get("message").unwrap_or(&Value::Null);
    let meta = result.get("meta").unwrap_or(&Value::Null);
    let accounts = message
        .get("accountKeys")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(account_key)
        .collect::<Vec<_>>();
    let tx_hash = transaction
        .get("signatures")
        .and_then(Value::as_array)
        .and_then(|values| values.first())
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let slot = slot_override
        .or_else(|| result.get("slot").and_then(Value::as_i64))
        .unwrap_or_default();
    let fee_lamports = meta.get("fee").and_then(Value::as_i64).unwrap_or_default();
    let mut details = HeliusTransactionDetails {
        receipt: TransactionReceiptRecord {
            tx_hash: tx_hash.clone(),
            block_number: slot,
            from_address: accounts.first().cloned().unwrap_or_default(),
            fee_native: (fee_lamports > 0).then_some(fee_lamports as f64 / 1_000_000_000.0),
            fee_usd: (fee_lamports > 0)
                .then_some(fee_lamports as f64 / 1_000_000_000.0)
                .zip(native_usd_rate)
                .map(|(fee, rate)| fee * rate),
            ..TransactionReceiptRecord::default()
        },
        account_keys: accounts.clone(),
        ..HeliusTransactionDetails::default()
    };
    if let Some(pre_balances) = meta.get("preBalances").and_then(Value::as_array) {
        for (account, balance) in accounts.iter().zip(pre_balances) {
            if let Some(lamports) = balance.as_u64() {
                details
                    .pre_balances_native
                    .insert(account.clone(), lamports as f64 / 1_000_000_000.0);
            }
        }
    }
    let mut instructions = message
        .get("instructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for group in meta
        .get("innerInstructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        instructions.extend(
            group
                .get("instructions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        );
    }
    let mut attributed_lamports = HashMap::<String, i128>::new();
    for instruction in &instructions {
        let Some(parsed) = instruction.get("parsed") else {
            continue;
        };
        let instruction_type = parsed.get("type").and_then(Value::as_str).unwrap_or("");
        if !matches!(instruction_type, "transfer" | "transferWithSeed") {
            continue;
        }
        let info = parsed.get("info").unwrap_or(&Value::Null);
        let Some(lamports) = info.get("lamports").and_then(Value::as_u64) else {
            continue;
        };
        let value_native = lamports as f64 / 1_000_000_000.0;
        let from_address = info
            .get("source")
            .or_else(|| info.get("from"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let to_address = info
            .get("destination")
            .or_else(|| info.get("to"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if from_address.is_empty() || to_address.is_empty() {
            continue;
        }
        *attributed_lamports.entry(from_address.clone()).or_default() -= lamports as i128;
        *attributed_lamports.entry(to_address.clone()).or_default() += lamports as i128;
        details.native_transfers.push(EthTransferRecord {
            tx_hash: tx_hash.clone(),
            block_number: slot,
            from_address,
            to_address,
            value_eth: value_native,
            value_usd: native_usd_rate.map(|rate| value_native * rate),
            payment_token_symbol: "SOL".to_string(),
            category: "external".to_string(),
            ..EthTransferRecord::default()
        });
    }
    details.native_transfers.extend(spl_payment_transfers(
        meta,
        &accounts,
        &instructions,
        &tx_hash,
        slot,
        native_usd_rate,
    ));
    details.native_transfer_attribution_complete =
        native_balance_changes_are_attributed(meta, &accounts, fee_lamports, &attributed_lamports);
    details
}

fn account_key(value: &Value) -> Option<String> {
    value
        .as_str()
        .or_else(|| value.get("pubkey").and_then(Value::as_str))
        .map(str::to_string)
}

#[derive(Clone, Debug, Default)]
struct TokenBalanceEntry {
    owner: String,
    amount: i128,
}

fn compressed_nft_owner_change(
    result: &Value,
    asset_event_type: &str,
    account_keys: &[String],
) -> Option<(Option<String>, String, bool)> {
    let is_transfer = asset_event_type.eq_ignore_ascii_case("transfer");
    let is_mint = asset_event_type
        .trim()
        .to_ascii_lowercase()
        .starts_with("mint");
    if !is_transfer && !is_mint {
        return None;
    }
    let message = result.get("transaction")?.get("message")?;
    let mut instructions = message
        .get("instructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for group in result
        .get("meta")
        .and_then(|meta| meta.get("innerInstructions"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        instructions.extend(
            group
                .get("instructions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        );
    }
    for instruction in instructions {
        let program_id = instruction
            .get("programId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                instruction
                    .get("programIdIndex")
                    .and_then(Value::as_u64)
                    .and_then(|index| account_keys.get(index as usize).cloned())
            });
        if program_id.as_deref() != Some(BUBBLEGUM_PROGRAM_ID) {
            continue;
        }
        if is_mint {
            let discriminator = instruction
                .get("data")
                .and_then(Value::as_str)
                .and_then(decode_base58)
                .and_then(|bytes| bytes.get(..8).map(|slice| slice.to_vec()));
            let parsed_is_mint = instruction
                .get("parsed")
                .and_then(|parsed| parsed.get("type"))
                .and_then(Value::as_str)
                .is_some_and(|kind| kind.to_ascii_lowercase().starts_with("mint"));
            let supported_discriminator = matches!(
                discriminator.as_deref(),
                Some(bytes)
                    if bytes == BUBBLEGUM_MINT_V1_DISCRIMINATOR
                        || bytes == BUBBLEGUM_MINT_TO_COLLECTION_V1_DISCRIMINATOR
            );
            if !parsed_is_mint && !supported_discriminator {
                continue;
            }
            let parsed_owner = instruction
                .get("parsed")
                .and_then(|parsed| parsed.get("info"))
                .and_then(|info| {
                    info.get("leafOwner")
                        .or_else(|| info.get("leaf_owner"))
                        .or_else(|| info.get("owner"))
                })
                .and_then(Value::as_str)
                .map(str::to_string);
            let account_owner = instruction
                .get("accounts")
                .and_then(Value::as_array)
                .and_then(|accounts| accounts.get(1))
                .and_then(|value| instruction_account(value, account_keys));
            let Some(owner) = parsed_owner.or(account_owner) else {
                continue;
            };
            if !owner.is_empty() {
                return Some((None, owner, true));
            }
            continue;
        }
        let discriminator = instruction
            .get("data")
            .and_then(Value::as_str)
            .and_then(decode_base58)
            .and_then(|bytes| bytes.get(..8).map(|slice| slice.to_vec()));
        let (from_index, to_index) = match discriminator.as_deref() {
            Some(bytes) if bytes == BUBBLEGUM_TRANSFER_V1_DISCRIMINATOR => (1, 3),
            Some(bytes) if bytes == BUBBLEGUM_TRANSFER_V2_DISCRIMINATOR => (3, 5),
            _ => continue,
        };
        let accounts = instruction.get("accounts")?.as_array()?;
        let from = instruction_account(accounts.get(from_index)?, account_keys)?;
        let to = instruction_account(accounts.get(to_index)?, account_keys)?;
        if from.is_empty() || to.is_empty() || from == to {
            continue;
        }
        return Some((Some(from), to, false));
    }
    None
}

fn sale_from_owner_and_payment_changes(
    collection_address: &str,
    mint_address: &str,
    tx_hash: &str,
    event_position: (i64, i64),
    seller: &str,
    buyer: &str,
    payments: &[EthTransferRecord],
) -> Option<NftSaleRecord> {
    if seller.is_empty() || buyer.is_empty() || seller == buyer {
        return None;
    }
    let buyer_payments = payments
        .iter()
        .filter(|payment| payment.from_address == buyer)
        .filter(|payment| {
            payment.value_eth > 0.0
                || payment.value_usd.unwrap_or_default() > 0.0
                || (payment.category == "spl" && !payment.payment_token_address.is_empty())
        })
        .collect::<Vec<_>>();
    if buyer_payments.is_empty() {
        return None;
    }
    let mut payment_groups = BTreeMap::<String, Vec<&EthTransferRecord>>::new();
    for payment in buyer_payments {
        let key = if matches!(payment.payment_token_symbol.as_str(), "SOL" | "WSOL") {
            "SOL".to_string()
        } else if !payment.payment_token_address.is_empty() {
            payment.payment_token_address.clone()
        } else {
            payment.payment_token_symbol.clone()
        };
        if !key.is_empty() {
            payment_groups.entry(key).or_default().push(payment);
        }
    }
    let mut seller_payment_groups = payment_groups
        .into_iter()
        .filter(|(_, group)| {
            group.iter().any(|payment| {
                payment.to_address == seller
                    && (payment.value_eth > 0.0
                        || payment.value_usd.unwrap_or_default() > 0.0
                        || (payment.category == "spl" && !payment.payment_token_address.is_empty()))
            })
        })
        .collect::<Vec<_>>();
    if seller_payment_groups.len() != 1 {
        return Some(unpriced_helius_sale(
            collection_address,
            mint_address,
            tx_hash,
            event_position,
            seller,
            buyer,
            "MIXED",
        ));
    }
    let (_, group) = seller_payment_groups.pop()?;
    let buyer_payments = group
        .into_iter()
        .filter(|payment| payment.to_address == seller)
        .collect::<Vec<_>>();
    let symbol = if buyer_payments
        .iter()
        .all(|payment| matches!(payment.payment_token_symbol.as_str(), "SOL" | "WSOL"))
    {
        "SOL".to_string()
    } else {
        buyer_payments[0].payment_token_symbol.clone()
    };
    let price_native = buyer_payments
        .iter()
        .map(|payment| payment.value_eth)
        .sum::<f64>();
    let price_usd = buyer_payments
        .iter()
        .map(|payment| payment.value_usd)
        .collect::<Option<Vec<_>>>()
        .map(|values| values.into_iter().sum::<f64>());
    let seller_usd = buyer_payments
        .iter()
        .filter_map(|payment| payment.value_usd)
        .sum::<f64>();
    let payment_token_address = buyer_payments[0].payment_token_address.clone();
    Some(NftSaleRecord {
        contract_address: collection_address.trim().to_string(),
        token_id: mint_address.trim().to_string(),
        tx_hash: tx_hash.to_string(),
        block_number: event_position.0,
        log_index: event_position.1,
        buyer_address: buyer.to_string(),
        seller_address: seller.to_string(),
        marketplace: "helius".to_string(),
        payment_token_symbol: symbol.clone(),
        payment_token_address,
        price_eth: (price_native > 0.0).then_some(price_native),
        price_usd,
        seller_fee_eth: price_native,
        seller_fee_usd: seller_usd,
        protocol_fee_eth: 0.0,
        protocol_fee_usd: 0.0,
        source: "helius".to_string(),
        is_native_eth: matches!(symbol.as_str(), "SOL" | "WSOL"),
        ..NftSaleRecord::default()
    })
}

fn unpriced_helius_sale(
    collection_address: &str,
    mint_address: &str,
    tx_hash: &str,
    event_position: (i64, i64),
    seller: &str,
    buyer: &str,
    payment_symbol: &str,
) -> NftSaleRecord {
    NftSaleRecord {
        contract_address: collection_address.trim().to_string(),
        token_id: mint_address.trim().to_string(),
        tx_hash: tx_hash.to_string(),
        block_number: event_position.0,
        log_index: event_position.1,
        buyer_address: buyer.to_string(),
        seller_address: seller.to_string(),
        marketplace: "helius".to_string(),
        payment_token_symbol: payment_symbol.to_string(),
        source: "helius".to_string(),
        ..NftSaleRecord::default()
    }
}

pub(super) fn clear_sale_amounts(sale: &mut NftSaleRecord) {
    sale.price_eth = None;
    sale.price_usd = None;
    sale.seller_fee_eth = 0.0;
    sale.seller_fee_usd = 0.0;
    sale.protocol_fee_eth = 0.0;
    sale.protocol_fee_usd = 0.0;
    sale.royalty_fee_eth = 0.0;
    sale.royalty_fee_usd = 0.0;
}

fn instruction_account(value: &Value, account_keys: &[String]) -> Option<String> {
    value.as_str().map(str::to_string).or_else(|| {
        value
            .as_u64()
            .and_then(|index| account_keys.get(index as usize).cloned())
    })
}

fn decode_base58(value: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if value.is_empty() {
        return None;
    }
    let mut decoded = vec![0_u8];
    for byte in value.bytes() {
        let digit = ALPHABET.iter().position(|candidate| *candidate == byte)? as u32;
        let mut carry = digit;
        for item in decoded.iter_mut().rev() {
            let next = u32::from(*item) * 58 + carry;
            *item = (next & 0xff) as u8;
            carry = next >> 8;
        }
        while carry > 0 {
            decoded.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let leading_zeroes = value.bytes().take_while(|byte| *byte == b'1').count();
    let first_nonzero = decoded
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(decoded.len());
    let mut result = vec![0_u8; leading_zeroes];
    result.extend_from_slice(&decoded[first_nonzero..]);
    Some(result)
}

fn token_balances(rows: Option<&Value>, mint_address: &str) -> HashMap<u64, TokenBalanceEntry> {
    rows.and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| row.get("mint").and_then(Value::as_str) == Some(mint_address))
        .filter_map(|row| {
            let account_index = row.get("accountIndex").and_then(Value::as_u64)?;
            let amount = row
                .get("uiTokenAmount")
                .and_then(|amount| amount.get("amount"))
                .and_then(Value::as_str)
                .and_then(|amount| amount.parse::<i128>().ok())?;
            Some((
                account_index,
                TokenBalanceEntry {
                    owner: row
                        .get("owner")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string(),
                    amount,
                },
            ))
        })
        .collect()
}

fn spl_payment_transfers(
    meta: &Value,
    account_keys: &[String],
    instructions: &[&Value],
    tx_hash: &str,
    slot: i64,
    native_usd_rate: Option<f64>,
) -> Vec<EthTransferRecord> {
    let token_accounts = token_account_descriptors(meta, account_keys);
    let mut transfers = Vec::new();
    for instruction in instructions {
        let Some(parsed) = instruction.get("parsed") else {
            continue;
        };
        if !matches!(
            parsed.get("type").and_then(Value::as_str).unwrap_or(""),
            "transfer" | "transferChecked"
        ) {
            continue;
        }
        let info = parsed.get("info").unwrap_or(&Value::Null);
        let source_account = info.get("source").and_then(Value::as_str).unwrap_or("");
        let destination_account = info
            .get("destination")
            .and_then(Value::as_str)
            .unwrap_or("");
        let source = token_accounts.get(source_account);
        let destination = token_accounts.get(destination_account);
        let mint = info
            .get("mint")
            .and_then(Value::as_str)
            .or_else(|| source.map(|value| value.mint.as_str()))
            .or_else(|| destination.map(|value| value.mint.as_str()))
            .unwrap_or("");
        let decimals = info
            .get("tokenAmount")
            .and_then(|value| value.get("decimals"))
            .and_then(Value::as_u64)
            .map(|value| value as i32)
            .or_else(|| source.map(|value| value.decimals))
            .or_else(|| destination.map(|value| value.decimals))
            .unwrap_or_default();
        // Decimal-zero token transfers are predominantly NFT ownership changes and
        // must not be interpreted as fungible payment flows.
        if mint.is_empty() || decimals <= 0 {
            continue;
        }
        let raw_amount = info
            .get("tokenAmount")
            .and_then(|value| value.get("amount"))
            .or_else(|| info.get("amount"))
            .and_then(parse_i128_value)
            .unwrap_or_default();
        if raw_amount <= 0 {
            continue;
        }
        let from_address = source
            .map(|value| value.owner.as_str())
            .filter(|value| !value.is_empty())
            .or_else(|| info.get("authority").and_then(Value::as_str))
            .unwrap_or("");
        let to_address = destination
            .map(|value| value.owner.as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("");
        if from_address.is_empty() || to_address.is_empty() || from_address == to_address {
            continue;
        }
        let amount = raw_amount as f64 / 10_f64.powi(decimals);
        let (symbol, value_native, value_usd) = match mint {
            WRAPPED_SOL_MINT => (
                "WSOL".to_string(),
                amount,
                native_usd_rate.map(|rate| amount * rate),
            ),
            SOLANA_USDC_MINT => (
                "USDC".to_string(),
                native_usd_rate
                    .filter(|rate| *rate > 0.0)
                    .map(|rate| amount / rate)
                    .unwrap_or_default(),
                Some(amount),
            ),
            _ => ("SPL".to_string(), 0.0, None),
        };
        transfers.push(EthTransferRecord {
            tx_hash: tx_hash.to_string(),
            block_number: slot,
            from_address: from_address.to_string(),
            to_address: to_address.to_string(),
            value_eth: value_native,
            value_usd,
            payment_token_symbol: symbol,
            payment_token_address: mint.to_string(),
            category: "spl".to_string(),
        });
    }
    transfers
}

#[derive(Clone, Debug)]
struct TokenAccountDescriptor {
    owner: String,
    mint: String,
    decimals: i32,
}

fn token_account_descriptors(
    meta: &Value,
    account_keys: &[String],
) -> HashMap<String, TokenAccountDescriptor> {
    let mut descriptors = HashMap::<String, TokenAccountDescriptor>::new();
    for row in meta
        .get("preTokenBalances")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            meta.get("postTokenBalances")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
    {
        let Some(index) = row.get("accountIndex").and_then(Value::as_u64) else {
            continue;
        };
        let Some(account) = account_keys.get(index as usize) else {
            continue;
        };
        let incoming = TokenAccountDescriptor {
            owner: row
                .get("owner")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            mint: row
                .get("mint")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            decimals: row
                .get("uiTokenAmount")
                .and_then(|value| value.get("decimals"))
                .and_then(Value::as_u64)
                .unwrap_or_default() as i32,
        };
        descriptors
            .entry(account.clone())
            .and_modify(|existing| {
                if existing.owner.is_empty() {
                    existing.owner = incoming.owner.clone();
                }
                if existing.mint.is_empty() {
                    existing.mint = incoming.mint.clone();
                }
                if existing.decimals == 0 {
                    existing.decimals = incoming.decimals;
                }
            })
            .or_insert(incoming);
    }
    descriptors
}

fn parse_i128_value(value: &Value) -> Option<i128> {
    value
        .as_str()
        .and_then(|value| value.parse().ok())
        .or_else(|| value.as_i64().map(i128::from))
        .or_else(|| value.as_u64().map(i128::from))
}

fn native_balance_changes_are_attributed(
    meta: &Value,
    account_keys: &[String],
    fee_lamports: i64,
    attributed_lamports: &HashMap<String, i128>,
) -> bool {
    let Some(pre) = meta.get("preBalances").and_then(Value::as_array) else {
        return false;
    };
    let Some(post) = meta.get("postBalances").and_then(Value::as_array) else {
        return false;
    };
    if pre.len() != post.len() || pre.len() != account_keys.len() {
        return false;
    }
    account_keys.iter().enumerate().all(|(index, account)| {
        let actual = post[index].as_u64().unwrap_or_default() as i128
            - pre[index].as_u64().unwrap_or_default() as i128;
        let mut expected = attributed_lamports
            .get(account)
            .copied()
            .unwrap_or_default();
        if index == 0 {
            expected -= fee_lamports.max(0) as i128;
        }
        actual == expected
    })
}

fn token_owner_change(meta: &Value, mint_address: &str) -> Option<(Option<String>, String, bool)> {
    let before = token_balances(meta.get("preTokenBalances"), mint_address);
    let after = token_balances(meta.get("postTokenBalances"), mint_address);
    let total_before = before.values().map(|entry| entry.amount).sum::<i128>();
    let mut indexes = before
        .keys()
        .chain(after.keys())
        .copied()
        .collect::<HashSet<_>>();
    let mut source: Option<(i128, String)> = None;
    let mut destination: Option<(i128, String)> = None;
    for index in indexes.drain() {
        let pre = before.get(&index).cloned().unwrap_or_default();
        let post = after.get(&index).cloned().unwrap_or_default();
        let delta = post.amount - pre.amount;
        if delta < 0 && !pre.owner.is_empty() {
            if source.as_ref().is_none_or(|(amount, _)| -delta > *amount) {
                source = Some((-delta, pre.owner));
            }
        } else if delta > 0
            && !post.owner.is_empty()
            && destination
                .as_ref()
                .is_none_or(|(amount, _)| delta > *amount)
        {
            destination = Some((delta, post.owner));
        }
    }
    let (_, to) = destination?;
    let is_mint = total_before == 0;
    let from = (!is_mint).then(|| source.map(|(_, owner)| owner)).flatten();
    if !is_mint && from.is_none() {
        return None;
    }
    Some((from, to, is_mint))
}

fn transaction_nft_owner_change_count(meta: &Value) -> usize {
    let mut mints = HashSet::new();
    for field in ["preTokenBalances", "postTokenBalances"] {
        for balance in meta
            .get(field)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let decimals = balance
                .get("uiTokenAmount")
                .and_then(|amount| amount.get("decimals"))
                .and_then(Value::as_u64);
            if decimals == Some(0) {
                if let Some(mint) = balance.get("mint").and_then(Value::as_str) {
                    if !mint.is_empty() {
                        mints.insert(mint);
                    }
                }
            }
        }
    }
    mints
        .into_iter()
        .filter(|mint| token_owner_change(meta, mint).is_some())
        .count()
}
