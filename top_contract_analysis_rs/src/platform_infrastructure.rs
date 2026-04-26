pub struct PlatformInfrastructureContract {
    pub chain: &'static str,
    pub address: &'static str,
    pub label: &'static str,
}

pub const PLATFORM_INFRASTRUCTURE_CONTRACT_BLACKLIST: &[PlatformInfrastructureContract] = &[
    PlatformInfrastructureContract {
        chain: "ethereum",
        address: "0x7c770595a2be9a87cf49b35ea9bc534f1a59552d",
        label: "zkSync NFT Factory Contract",
    },
    PlatformInfrastructureContract {
        chain: "ethereum",
        address: "0xf3cd1e9326d1965935b287b1ee75c7183359a88a",
        label: "Manifold Contract Deployment Factory",
    },
    PlatformInfrastructureContract {
        chain: "base",
        address: "0xf3cd1e9326d1965935b287b1ee75c7183359a88a",
        label: "Manifold Contract Deployment Factory",
    },
    PlatformInfrastructureContract {
        chain: "optimism",
        address: "0xf3cd1e9326d1965935b287b1ee75c7183359a88a",
        label: "Manifold Contract Deployment Factory",
    },
    PlatformInfrastructureContract {
        chain: "shape",
        address: "0xf3cd1e9326d1965935b287b1ee75c7183359a88a",
        label: "Manifold Contract Deployment Factory",
    },
    PlatformInfrastructureContract {
        chain: "apechain",
        address: "0xf3cd1e9326d1965935b287b1ee75c7183359a88a",
        label: "Manifold Contract Deployment Factory",
    },
    PlatformInfrastructureContract {
        chain: "sepolia",
        address: "0xf3cd1e9326d1965935b287b1ee75c7183359a88a",
        label: "Manifold Contract Deployment Factory",
    },
    PlatformInfrastructureContract {
        chain: "ethereum",
        address: "0x6d9dd3547baf4c190ab89e0103c363feaf325eca",
        label: "Rarible ERC721 Factory",
    },
    PlatformInfrastructureContract {
        chain: "ethereum",
        address: "0x60f80121c31a0d46b5279700f9df786054aa5ee5",
        label: "Rarible pre-deployed ERC721 contract",
    },
    PlatformInfrastructureContract {
        chain: "ethereum",
        address: "0xd07dc4262bcdbf85190c01c996b4c06a461d2430",
        label: "Rarible pre-deployed ERC1155 contract",
    },
    PlatformInfrastructureContract {
        chain: "ethereum",
        address: "0x09200b963c52d3297a93af71f919e7829c53cf9a",
        label: "OpenSea Shared Storefront",
    },
];

pub fn is_platform_infrastructure_contract_blacklisted(
    chain: &str,
    contract_address: &str,
) -> bool {
    PLATFORM_INFRASTRUCTURE_CONTRACT_BLACKLIST
        .iter()
        .any(|candidate| {
            chain.eq_ignore_ascii_case(candidate.chain)
                && contract_address.eq_ignore_ascii_case(candidate.address)
        })
}
