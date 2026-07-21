use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

macro_rules! id_type {
    ($name:ident, $inner:ty) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            Eq,
            Hash,
            Ord,
            PartialEq,
            PartialOrd,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub $inner);

        impl $name {
            pub const fn index(self) -> usize {
                self.0 as usize
            }
        }
    };
}

id_type!(ContractId, u32);
id_type!(NftId, u32);
id_type!(TokenIdId, u32);
id_type!(MatchedNftKeyId, u32);
id_type!(NameValueId, u32);
id_type!(UriValueId, u32);
id_type!(MetadataId, u32);
id_type!(ProfileId, u32);
id_type!(TermId, u32);
id_type!(GlobalAddressId, u32);
id_type!(GlobalTxId, u32);
id_type!(GlobalNftId, u32);
id_type!(CandidateId, u32);
id_type!(SeedId, u16);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainId {
    Base,
    Ethereum,
    Polygon,
    Solana,
}

impl ChainId {
    pub const ALL: [Self; 4] = [Self::Base, Self::Ethereum, Self::Polygon, Self::Solana];

    pub const fn ordinal(self) -> u8 {
        match self {
            Self::Base => 0,
            Self::Ethereum => 1,
            Self::Polygon => 2,
            Self::Solana => 3,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Ethereum => "ethereum",
            Self::Polygon => "polygon",
            Self::Solana => "solana",
        }
    }

    pub const fn is_evm(self) -> bool {
        !matches!(self, Self::Solana)
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ChainId {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("base") {
            Ok(Self::Base)
        } else if value.eq_ignore_ascii_case("ethereum") || value.eq_ignore_ascii_case("eth") {
            Ok(Self::Ethereum)
        } else if value.eq_ignore_ascii_case("polygon") || value.eq_ignore_ascii_case("matic") {
            Ok(Self::Polygon)
        } else if value.eq_ignore_ascii_case("solana") || value.eq_ignore_ascii_case("sol") {
            Ok(Self::Solana)
        } else {
            Err(format!("unknown chain `{}`", value.to_ascii_lowercase()))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SourceOrder {
    pub file_ordinal: u16,
    pub file_row_number: u64,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ContractKey {
    pub chain: ChainId,
    pub contract_address: Arc<str>,
}

impl ContractKey {
    pub fn new(chain: ChainId, contract_address: impl Into<Arc<str>>) -> Self {
        Self {
            chain,
            contract_address: contract_address.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct NftKey {
    pub chain: ChainId,
    pub contract_address: Arc<str>,
    pub token_id: Arc<str>,
}

impl NftKey {
    pub fn contract_key(&self) -> ContractKey {
        ContractKey::new(self.chain, self.contract_address.clone())
    }
}

pub fn stable_mix(value: u32) -> u32 {
    let mut value = value;
    value ^= value >> 16;
    value = value.wrapping_mul(0x7feb_352d);
    value ^= value >> 15;
    value = value.wrapping_mul(0x846c_a68b);
    value ^ (value >> 16)
}

pub fn owner_shard(value: u32, shard_count: usize) -> usize {
    debug_assert!(shard_count.is_power_of_two());
    stable_mix(value) as usize & (shard_count - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_order_is_business_order() {
        assert_eq!(
            ChainId::ALL.map(ChainId::as_str),
            ["base", "ethereum", "polygon", "solana"]
        );
    }

    #[test]
    fn chain_parser_preserves_case_insensitive_aliases() {
        assert_eq!(ChainId::from_str(" ETH ").unwrap(), ChainId::Ethereum);
        assert_eq!(ChainId::from_str("MaTiC").unwrap(), ChainId::Polygon);
        assert_eq!(ChainId::from_str("SOL").unwrap(), ChainId::Solana);
    }

    #[test]
    fn owner_is_stable_and_bounded() {
        for value in 0..100_000 {
            assert!(owner_shard(value, 128) < 128);
        }
    }
}
