use crate::model::{
    ChainId, ContractId, ContractKey, ContractRecord, InputQuality, MetadataAnchor, NameValueId,
    NftId, NftIdentityRecord, NftKey, ProfileId, UriFeatureRecord,
};
use crate::resident::FrozenBytePool;

#[derive(Clone, Debug)]
pub struct ContractCatalog {
    pub contracts: Vec<ContractRecord>,
}

impl ContractCatalog {
    pub fn key(&self, id: ContractId) -> ContractKey {
        let record = &self.contracts[id.index()];
        ContractKey::new(record.chain, record.address.clone())
    }

    pub fn find(&self, key: &ContractKey) -> Option<ContractId> {
        self.contracts
            .binary_search_by(|record| {
                (record.chain, record.address.as_ref())
                    .cmp(&(key.chain, key.contract_address.as_ref()))
            })
            .ok()
            .map(|index| ContractId(index as u32))
    }
}

#[derive(Clone, Debug)]
pub struct UriNftIdentityStore {
    pub token_ids: FrozenBytePool,
    pub nfts: Vec<NftIdentityRecord>,
    pub contract_offsets: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct UriFeatureStore {
    pub values: FrozenBytePool,
    pub features: Vec<UriFeatureRecord>,
}

#[derive(Clone, Debug)]
pub struct NameFeatureStore {
    pub values: FrozenBytePool,
    pub contract_names: Vec<Option<NameValueId>>,
}

#[derive(Clone, Debug)]
pub struct MetadataProfile {
    pub anchor_start: u64,
    pub anchor_len: u8,
    pub member_start: u64,
    pub member_len: u32,
}

#[derive(Clone, Debug)]
pub struct MetadataFeatureStore {
    pub anchor_tokens: FrozenBytePool,
    pub documents: FrozenBytePool,
    pub anchors: Vec<MetadataAnchor>,
    pub profile_members: Vec<ContractId>,
    pub profiles: Vec<MetadataProfile>,
    pub contract_profiles: Vec<Option<ProfileId>>,
}

impl MetadataFeatureStore {
    pub fn profile_anchors(&self, profile: ProfileId) -> &[MetadataAnchor] {
        let profile = &self.profiles[profile.index()];
        let start = profile.anchor_start as usize;
        &self.anchors[start..start + usize::from(profile.anchor_len)]
    }

    pub fn profile_members(&self, profile: ProfileId) -> &[ContractId] {
        let profile = &self.profiles[profile.index()];
        let start = profile.member_start as usize;
        &self.profile_members[start..start + profile.member_len as usize]
    }
}

#[derive(Clone, Debug)]
pub struct ResidentBaseStore {
    pub contracts: ContractCatalog,
    pub uri_identity: Option<UriNftIdentityStore>,
    pub uri_features: Option<UriFeatureStore>,
    pub name_features: Option<NameFeatureStore>,
    pub metadata_features: Option<MetadataFeatureStore>,
    pub quality: InputQuality,
}

impl ResidentBaseStore {
    pub fn nft_key(&self, nft_id: NftId) -> Option<NftKey> {
        let identities = self.uri_identity.as_ref()?;
        let identity = identities.nfts.get(nft_id.index())?;
        let contract = &self.contracts.contracts[identity.contract_id.index()];
        Some(NftKey {
            chain: contract.chain,
            contract_address: contract.address.clone(),
            token_id: std::sync::Arc::from(identities.token_ids.get(identity.token_id_id.0)),
        })
    }

    pub fn chain(&self, contract_id: ContractId) -> ChainId {
        self.contracts.contracts[contract_id.index()].chain
    }

    pub fn take_uri_stage(&mut self) -> Option<(UriNftIdentityStore, UriFeatureStore)> {
        Some((self.uri_identity.take()?, self.uri_features.take()?))
    }

    pub fn take_name_stage(&mut self) -> Option<NameFeatureStore> {
        self.name_features.take()
    }

    pub fn take_metadata_stage(&mut self) -> Option<MetadataFeatureStore> {
        self.metadata_features.take()
    }

    pub fn logical_bytes(&self) -> u64 {
        let mut bytes =
            self.contracts.contracts.len() as u64 * std::mem::size_of::<ContractRecord>() as u64;
        bytes += self
            .contracts
            .contracts
            .iter()
            .map(|contract| contract.address.len() as u64)
            .sum::<u64>();
        if let Some(identity) = &self.uri_identity {
            bytes += identity.token_ids.bytes();
            bytes += identity.nfts.len() as u64 * std::mem::size_of::<NftIdentityRecord>() as u64;
            bytes += identity.contract_offsets.len() as u64 * 8;
        }
        if let Some(features) = &self.uri_features {
            bytes += features.values.bytes();
            bytes +=
                features.features.len() as u64 * std::mem::size_of::<UriFeatureRecord>() as u64;
        }
        if let Some(features) = &self.name_features {
            bytes += features.values.bytes();
            bytes += features.contract_names.len() as u64
                * std::mem::size_of::<Option<NameValueId>>() as u64;
        }
        if let Some(features) = &self.metadata_features {
            bytes += features.anchor_tokens.bytes();
            bytes += features.documents.bytes();
            bytes += features.profiles.len() as u64 * std::mem::size_of::<MetadataProfile>() as u64;
            bytes += features.anchors.len() as u64 * std::mem::size_of::<MetadataAnchor>() as u64;
            bytes +=
                features.profile_members.len() as u64 * std::mem::size_of::<ContractId>() as u64;
            bytes += features.contract_profiles.len() as u64
                * std::mem::size_of::<Option<ProfileId>>() as u64;
        }
        bytes
    }
}
