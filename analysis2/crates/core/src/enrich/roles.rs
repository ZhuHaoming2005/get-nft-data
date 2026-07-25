//! Shared binary victim/operator rule used before and after deep enrichment.

use ahash::AHashSet;

use super::types::{EvidenceStatus, SaleEvent, TransferEvent, normalize_chain_address};

fn usable_address(chain: &str, raw: &str) -> Option<String> {
    let address = normalize_chain_address(chain, raw);
    if address.is_empty()
        || (!chain.eq_ignore_ascii_case("solana")
            && address == "0x0000000000000000000000000000000000000000")
    {
        None
    } else {
        Some(address)
    }
}

fn paid(native: Option<f64>, usd: Option<f64>) -> bool {
    native.is_some_and(|amount| amount > 0.0) || usd.is_some_and(|amount| amount > 0.0)
}

/// Paid mint recipients / secondary-market buyers that have never sent any NFT
/// from the hit contract. Incomplete transfer history cannot prove "never".
pub(crate) fn victim_addresses(
    chain: &str,
    transfers: &[TransferEvent],
    sales: &[SaleEvent],
    transfer_status: EvidenceStatus,
) -> AHashSet<String> {
    if !matches!(
        transfer_status,
        EvidenceStatus::Complete | EvidenceStatus::Empty
    ) {
        return AHashSet::new();
    }

    let mut paid_acquirers = AHashSet::new();
    for transfer in transfers {
        if transfer.is_mint
            && paid(transfer.mint_payment_native, transfer.mint_payment_usd)
            && let Some(recipient) = usable_address(chain, &transfer.to)
        {
            paid_acquirers.insert(recipient);
        }
    }
    for sale in sales {
        if paid(sale.native_amount, sale.usd_amount)
            && let Some(buyer) = usable_address(chain, &sale.buyer)
        {
            paid_acquirers.insert(buyer);
        }
    }

    let mut outbound = AHashSet::new();
    for transfer in transfers {
        if let Some(sender) = usable_address(chain, &transfer.from) {
            outbound.insert(sender);
        }
    }
    // A market sale is necessarily an outbound NFT transfer even if a provider
    // omitted its matching transfer row.
    for sale in sales {
        if let Some(seller) = usable_address(chain, &sale.seller) {
            outbound.insert(seller);
        }
    }

    paid_acquirers.retain(|address| !outbound.contains(address));
    paid_acquirers
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer(from: &str, to: &str, is_mint: bool) -> TransferEvent {
        TransferEvent {
            tx_hash: "tx".into(),
            token_id: "1".into(),
            from: from.into(),
            to: to.into(),
            timestamp: Some(1),
            block_number: Some(1),
            is_mint,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: is_mint.then_some(1.0),
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }
    }

    fn paid_sale(seller: &str, buyer: &str, token_id: &str) -> SaleEvent {
        SaleEvent {
            tx_hash: format!("sale-{token_id}"),
            token_id: token_id.into(),
            seller: seller.into(),
            buyer: buyer.into(),
            native_amount: Some(1.0),
            ..SaleEvent::default()
        }
    }

    #[test]
    fn any_outbound_transfer_in_contract_makes_paid_buyer_an_operator() {
        let transfers = vec![transfer("0xbuyer", "0xother", false)];
        let sales = vec![paid_sale("0xseller", "0xbuyer", "2")];
        assert!(
            victim_addresses("ethereum", &transfers, &sales, EvidenceStatus::Complete).is_empty()
        );
    }

    #[test]
    fn paid_buyer_without_outbound_transfer_is_a_victim() {
        let victims = victim_addresses(
            "ethereum",
            &[],
            &[paid_sale("0xseller", "0xbuyer", "1")],
            EvidenceStatus::Empty,
        );
        assert!(victims.contains("0xbuyer"));
    }

    #[test]
    fn incomplete_transfer_history_never_assigns_victim() {
        let sales = vec![paid_sale("0xseller", "0xbuyer", "1")];
        for status in [
            EvidenceStatus::NotRequested,
            EvidenceStatus::Failed,
            EvidenceStatus::Truncated,
        ] {
            assert!(victim_addresses("ethereum", &[], &sales, status).is_empty());
        }
    }
}
