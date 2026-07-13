use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tempfile::tempdir;
use tokio::time::{sleep, Duration};
use top_contract_analysis_rs::analysis::{
    analyze_seed_contract, AnalysisDeps, AnalyzeApi, AnalyzeRequest, CandidateSeedHolderRequest,
    FeatureStoreReader,
};
use top_contract_analysis_rs::error::AppError;
use top_contract_analysis_rs::models::{
    AddressSignalPayload, ContractMetadata, ContractNameRecord, DatabaseNftRecord,
    DatabaseSnapshot, DuplicateContractPayload, EthTransferRecord, HonestAddressPayload,
    MaliciousAddressPayload, NftSaleRecord, OwnerBalance, ProviderDataQualityPayload,
    SecondarySaleVictimAddressPayload, SeedCollectionStatsPayload, SeedContractPayload, SeedNft,
    SingleReportPayload, TransactionReceiptRecord, TransferRecord, VictimSignalPayload,
    ZERO_ADDRESS,
};
use top_contract_analysis_rs::progress::{NoopBatchProgressReporter, NoopProgressReporter};
use top_contract_analysis_rs::reporting::{
    default_output_basename, render_human_readable_report, write_outputs_to_directory,
};

mod support;

use support::*;

mod address_and_sales;
mod candidates;
mod concurrency;
mod reporting;
mod value_flow;
