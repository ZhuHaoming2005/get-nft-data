use serde::{Deserialize, Serialize};

macro_rules! strong_id {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub EntityId);

        impl $name {
            pub const fn new(value: EntityId) -> Self {
                Self(value)
            }

            pub const fn get(self) -> EntityId {
                self.0
            }

            #[cfg(not(feature = "wide_ids"))]
            pub const fn as_u64(self) -> u64 {
                self.0 as u64
            }

            #[cfg(feature = "wide_ids")]
            pub const fn as_u64(self) -> u64 {
                self.0
            }
        }
    };
}

#[cfg(not(feature = "wide_ids"))]
pub type EntityId = u32;
#[cfg(feature = "wide_ids")]
pub type EntityId = u64;

strong_id!(ContractId);
strong_id!(NftId);
strong_id!(StringId);
strong_id!(NameAtomId);
strong_id!(CanonicalNameId);
strong_id!(MetadataDocId);

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ChainId(pub u16);

impl ChainId {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SourceOrder {
    pub file_ordinal: u32,
    pub file_row_number: u64,
}

impl SourceOrder {
    pub const fn new(file_ordinal: u32, file_row_number: u64) -> Self {
        Self {
            file_ordinal,
            file_row_number,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_order_is_lexicographic() {
        assert!(
            SourceOrder::new(0, u64::MAX) < SourceOrder::new(1, 0),
            "configured file order must dominate row number"
        );
    }

    #[test]
    fn configured_id_width_preserves_the_same_semantic_json_golden() {
        let ids = [
            ContractId::new(EntityId::from(7_u32)),
            ContractId::new(EntityId::from(42_u32)),
        ];
        assert_eq!(serde_json::to_string(&ids).unwrap(), "[7,42]");
        assert_eq!(ids[0].as_u64(), 7);
        assert_eq!(ids[1].as_u64(), 42);
        assert!(ids[0] < ids[1]);
    }
}
