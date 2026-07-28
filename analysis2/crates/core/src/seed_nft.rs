//! Complete seed-contract NFT snapshots, resident for the hot path and persisted
//! to reusable Zstandard caches.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use ahash::AHashSet;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::Analysis2Error;
use crate::enrich::http::HttpClient;
use crate::enrich::types::{ApiKeys, ProviderEndpoints};
use crate::normalize::{normalize_name, normalize_url};
use crate::parquet::validated_metadata;
use crate::reporting::SeedRecord;

pub const MAX_SEED_NFTS_PER_CONTRACT: usize = 50_000;
const CACHE_VERSION: u32 = 1;
const ALCHEMY_PAGE_SIZE: usize = 100;
const HELIUS_PAGE_SIZE: usize = 1_000;
const ZSTD_LEVEL: i32 = 3;

#[derive(Clone, Debug)]
pub struct SeedNftDownloadOptions {
    pub cache_dir: PathBuf,
    pub api_keys: ApiKeys,
    pub endpoints: ProviderEndpoints,
    pub concurrency: usize,
    pub retries: usize,
    pub refresh: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedNftRecord {
    pub token_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name_norm: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token_uri_norm: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image_uri_norm: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub metadata_json: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CacheHeader {
    kind: String,
    version: u32,
    chain: String,
    address: String,
    max_nfts: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CacheFooter {
    kind: String,
    item_count: usize,
    provider_total: Option<usize>,
    truncated: bool,
}

#[derive(Clone, Debug)]
pub struct SeedNftCacheRef {
    pub seed: SeedRecord,
    pub path: PathBuf,
    pub item_count: usize,
    pub provider_total: Option<usize>,
    pub truncated: bool,
    pub reused: bool,
    resident_records: Option<Vec<SeedNftRecord>>,
}

impl SeedNftCacheRef {
    /// Visit decoded rows without cloning when the cache is resident. Disk-only
    /// references fall back to one streaming decode.
    pub fn for_each_nft(
        &self,
        mut callback: impl FnMut(&SeedNftRecord) -> Result<(), Analysis2Error>,
    ) -> Result<(), Analysis2Error> {
        if let Some(records) = &self.resident_records {
            self.verify_count(records.len())?;
            for record in records {
                callback(record)?;
            }
            return Ok(());
        }
        let mut count = 0usize;
        read_cache(&self.path, |line_no, value| {
            if line_no == 0 || value.get("kind").and_then(Value::as_str) == Some("complete") {
                return Ok(());
            }
            let record = serde_json::from_value(value)
                .map_err(|e| Analysis2Error::invalid(format!("parse seed NFT cache row: {e}")))?;
            callback(&record)?;
            count += 1;
            Ok(())
        })?;
        self.verify_count(count)
    }

    /// Consume decoded rows for their final pipeline use. Resident memory is
    /// detached before callbacks start, so it is freed promptly even on error.
    pub fn consume_nfts(
        &mut self,
        mut callback: impl FnMut(SeedNftRecord) -> Result<(), Analysis2Error>,
    ) -> Result<(), Analysis2Error> {
        if let Some(records) = self.resident_records.take() {
            let count = records.len();
            for record in records {
                callback(record)?;
            }
            return self.verify_count(count);
        }
        let mut count = 0usize;
        read_cache(&self.path, |line_no, value| {
            if line_no == 0 || value.get("kind").and_then(Value::as_str) == Some("complete") {
                return Ok(());
            }
            let record = serde_json::from_value(value)
                .map_err(|e| Analysis2Error::invalid(format!("parse seed NFT cache row: {e}")))?;
            callback(record)?;
            count += 1;
            Ok(())
        })?;
        self.verify_count(count)
    }

    fn verify_count(&self, count: usize) -> Result<(), Analysis2Error> {
        if count != self.item_count {
            return Err(Analysis2Error::invalid(format!(
                "seed NFT cache count changed for {} / {}",
                self.seed.chain, self.seed.address
            )));
        }
        Ok(())
    }
}

struct CacheStreamWriter {
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    tmp: PathBuf,
    final_path: PathBuf,
    count: usize,
}

impl CacheStreamWriter {
    fn create(path: &Path, seed: &SeedRecord) -> Result<Self, Analysis2Error> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("jsonl.zst.tmp");
        let file = File::create(&tmp)?;
        let mut encoder = zstd::stream::write::Encoder::new(BufWriter::new(file), ZSTD_LEVEL)
            .map_err(|e| Analysis2Error::invalid(format!("create seed NFT zstd cache: {e}")))?;
        write_json_line(
            &mut encoder,
            &CacheHeader {
                kind: "header".into(),
                version: CACHE_VERSION,
                chain: seed.chain.clone(),
                address: seed.address.clone(),
                max_nfts: MAX_SEED_NFTS_PER_CONTRACT,
            },
        )?;
        Ok(Self {
            encoder,
            tmp,
            final_path: path.to_owned(),
            count: 0,
        })
    }

    fn push(&mut self, record: &SeedNftRecord) -> Result<(), Analysis2Error> {
        write_json_line(&mut self.encoder, record)?;
        self.count += 1;
        Ok(())
    }

    fn finish(
        mut self,
        provider_total: Option<usize>,
        truncated: bool,
    ) -> Result<CacheFooter, Analysis2Error> {
        let footer = CacheFooter {
            kind: "complete".into(),
            item_count: self.count,
            provider_total,
            truncated,
        };
        write_json_line(&mut self.encoder, &footer)?;
        let mut output = self
            .encoder
            .finish()
            .map_err(|e| Analysis2Error::invalid(format!("finish seed NFT zstd cache: {e}")))?;
        output.flush()?;
        drop(output);
        if self.final_path.is_file() {
            // Windows cannot atomically rename over an existing file. Preserve
            // the last valid cache until the new stream is in place.
            let backup = self.final_path.with_extension("jsonl.zst.bak");
            if backup.is_file() {
                fs::remove_file(&backup)?;
            }
            fs::rename(&self.final_path, &backup)?;
            if let Err(error) = fs::rename(&self.tmp, &self.final_path) {
                let _ = fs::rename(&backup, &self.final_path);
                return Err(Analysis2Error::invalid(format!(
                    "publish seed NFT cache {}: {error}",
                    self.final_path.display()
                )));
            }
            fs::remove_file(backup)?;
        } else {
            fs::rename(&self.tmp, &self.final_path)?;
        }
        Ok(footer)
    }
}

fn write_json_line<T: Serialize>(writer: &mut impl Write, value: &T) -> Result<(), Analysis2Error> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|e| Analysis2Error::invalid(format!("serialize seed NFT cache row: {e}")))?;
    writer.write_all(b"\n")?;
    Ok(())
}

/// Download missing seed snapshots concurrently. Each contract is persisted to
/// its own compressed file and retained in memory for the immediate pipeline
/// stages, avoiding a second decompression/JSON parse on the hot path.
pub async fn prepare_seed_nft_caches(
    seeds: &[SeedRecord],
    options: &SeedNftDownloadOptions,
) -> Result<Vec<SeedNftCacheRef>, Analysis2Error> {
    fs::create_dir_all(&options.cache_dir)?;
    let client = HttpClient::with_retries(options.concurrency.max(1), options.retries)?;
    let width = options.concurrency.max(1);
    let (solana, evm): (Vec<_>, Vec<_>) = seeds
        .iter()
        .cloned()
        .enumerate()
        .partition(|(_, seed)| seed.chain == "solana");
    let solana_downloads = prepare_provider_seed_caches(solana, &client, options, width);
    let evm_downloads = prepare_provider_seed_caches(evm, &client, options, width);
    let (solana_results, evm_results) = tokio::join!(solana_downloads, evm_downloads);
    let mut results = solana_results;
    results.extend(evm_results);
    let mut ordered = Vec::with_capacity(results.len());
    for result in results {
        ordered.push(result?);
    }
    ordered.sort_by_key(|(index, _)| *index);
    Ok(ordered.into_iter().map(|(_, cache)| cache).collect())
}

async fn prepare_provider_seed_caches(
    seeds: Vec<(usize, SeedRecord)>,
    client: &HttpClient,
    options: &SeedNftDownloadOptions,
    width: usize,
) -> Vec<Result<(usize, SeedNftCacheRef), Analysis2Error>> {
    stream::iter(seeds)
        .map(|(index, seed)| {
            let client = client.clone();
            async move {
                let path = cache_path(&options.cache_dir, &seed);
                if !options.refresh
                    && let Ok(cached) = validate_cache(&path, &seed)
                {
                    return Ok((index, cached));
                }
                let cached = match seed.chain.as_str() {
                    "solana" => download_helius(&client, options, &seed, &path).await,
                    _ => download_alchemy(&client, options, &seed, &path).await,
                }?;
                Ok::<_, Analysis2Error>((index, cached))
            }
        })
        .buffer_unordered(width)
        .collect::<Vec<_>>()
        .await
}

async fn download_alchemy(
    client: &HttpClient,
    options: &SeedNftDownloadOptions,
    seed: &SeedRecord,
    path: &Path,
) -> Result<SeedNftCacheRef, Analysis2Error> {
    let key = options.api_keys.alchemy().ok_or_else(|| {
        Analysis2Error::invalid(format!(
            "missing Alchemy API key and reusable seed NFT cache for {} / {}",
            seed.chain, seed.address
        ))
    })?;
    let base = options
        .endpoints
        .alchemy_nft(&seed.chain, key, "getNFTsForContract")
        .ok_or_else(|| {
            Analysis2Error::invalid(format!("unsupported Alchemy chain {}", seed.chain))
        })?;
    let mut writer = CacheStreamWriter::create(path, seed)?;
    let mut page_key: Option<String> = None;
    let mut seen_keys = AHashSet::new();
    let mut seen_tokens = AHashSet::new();
    let mut provider_total = None;
    let mut resident_records = Vec::new();
    loop {
        let remaining = MAX_SEED_NFTS_PER_CONTRACT.saturating_sub(writer.count);
        if remaining == 0 {
            break;
        }
        let limit = ALCHEMY_PAGE_SIZE.min(remaining);
        let mut url = format!(
            "{base}?contractAddress={}&withMetadata=true&limit={limit}",
            encode_query(&seed.address)
        );
        if let Some(key) = &page_key {
            url.push_str("&pageKey=");
            url.push_str(&encode_query(key));
        }
        let payload = client.get_json_alchemy(&url, &[]).await?;
        if let Some(total) = json_usize(payload.get("totalCount").or_else(|| payload.get("total")))
        {
            provider_total = Some(total);
        }
        let rows = payload
            .get("nfts")
            .and_then(Value::as_array)
            .ok_or_else(|| Analysis2Error::http("Alchemy getNFTsForContract omitted nfts"))?;
        for row in rows {
            let record = parse_alchemy_nft(row).ok_or_else(|| {
                Analysis2Error::http("Alchemy getNFTsForContract NFT omitted tokenId")
            })?;
            if seen_tokens.insert(record.token_id.clone()) {
                writer.push(&record)?;
                resident_records.push(record);
                if writer.count >= MAX_SEED_NFTS_PER_CONTRACT {
                    break;
                }
            }
        }
        let next = payload
            .get("pageKey")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        if rows.is_empty() || next.is_none() {
            page_key = None;
            break;
        }
        let next = next.expect("checked above");
        if !seen_keys.insert(next.clone()) {
            return Err(Analysis2Error::http(
                "Alchemy repeated getNFTsForContract pageKey",
            ));
        }
        page_key = Some(next);
    }
    let capped = writer.count >= MAX_SEED_NFTS_PER_CONTRACT;
    if let Some(total) = provider_total
        && (total < writer.count || (total > writer.count && !capped))
    {
        return Err(Analysis2Error::http(format!(
            "Alchemy getNFTsForContract totalCount={total} disagrees with {} downloaded NFTs",
            writer.count
        )));
    }
    let truncated =
        capped && (page_key.is_some() || provider_total.is_some_and(|total| total > writer.count));
    let footer = writer.finish(provider_total, truncated)?;
    Ok(cache_ref(seed, path, footer, false, Some(resident_records)))
}

async fn download_helius(
    client: &HttpClient,
    options: &SeedNftDownloadOptions,
    seed: &SeedRecord,
    path: &Path,
) -> Result<SeedNftCacheRef, Analysis2Error> {
    let key = options.api_keys.helius().ok_or_else(|| {
        Analysis2Error::invalid(format!(
            "missing Helius API key and reusable seed NFT cache for {} / {}",
            seed.chain, seed.address
        ))
    })?;
    let url = with_api_key(&options.endpoints.helius, key);
    let mut writer = CacheStreamWriter::create(path, seed)?;
    let mut page = 1usize;
    let mut has_more;
    let mut seen_tokens = AHashSet::new();
    let mut resident_records = Vec::new();
    loop {
        let remaining = MAX_SEED_NFTS_PER_CONTRACT.saturating_sub(writer.count);
        if remaining == 0 {
            has_more = true;
            break;
        }
        let limit = HELIUS_PAGE_SIZE;
        let body = json!({
            "jsonrpc": "2.0",
            "id": format!("seed-nfts-{page}"),
            "method": "getAssetsByGroup",
            "params": {
                "groupKey": "collection",
                "groupValue": seed.address,
                "page": page,
                "limit": limit,
                "sortBy": {"sortBy": "id", "sortDirection": "asc"},
                "options": {
                    "showUnverifiedCollections": false,
                    "showCollectionMetadata": false,
                    "showGrandTotal": false
                }
            }
        });
        let payload = client.post_json_helius(&url, &[], &body).await?;
        if let Some(error) = payload.get("error") {
            return Err(Analysis2Error::http(format!(
                "Helius getAssetsByGroup: {error}"
            )));
        }
        let result = payload
            .get("result")
            .ok_or_else(|| Analysis2Error::http("Helius getAssetsByGroup omitted result"))?;
        let rows = result
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| Analysis2Error::http("Helius getAssetsByGroup omitted items"))?;
        for row in rows {
            let record = parse_helius_nft(row)
                .ok_or_else(|| Analysis2Error::http("Helius getAssetsByGroup item omitted id"))?;
            if seen_tokens.insert(record.token_id.clone()) {
                writer.push(&record)?;
                resident_records.push(record);
                if writer.count >= MAX_SEED_NFTS_PER_CONTRACT {
                    break;
                }
            }
        }
        has_more = rows.len() >= limit;
        if rows.is_empty() || rows.len() < limit {
            break;
        }
        page += 1;
    }
    let capped = writer.count >= MAX_SEED_NFTS_PER_CONTRACT;
    let truncated = capped && has_more;
    let footer = writer.finish(None, truncated)?;
    Ok(cache_ref(seed, path, footer, false, Some(resident_records)))
}

fn parse_alchemy_nft(row: &Value) -> Option<SeedNftRecord> {
    let token_id = text(row.get("tokenId"))?;
    let metadata = row.get("raw").and_then(|raw| raw.get("metadata"));
    Some(SeedNftRecord {
        token_id,
        name_norm: text(row.get("name"))
            .or_else(|| metadata.and_then(|value| text(value.get("name"))))
            .map(|value| normalize_name(&value))
            .unwrap_or_default(),
        token_uri_norm: text(row.get("raw").and_then(|raw| raw.get("tokenUri")))
            .or_else(|| text(row.get("tokenUri")))
            .and_then(|value| normalize_url(&value))
            .unwrap_or_default(),
        image_uri_norm: metadata
            .and_then(|value| text(value.get("image")))
            .or_else(|| text(row.get("image").and_then(|image| image.get("originalUrl"))))
            .or_else(|| text(row.get("image").and_then(|image| image.get("cachedUrl"))))
            .and_then(|value| normalize_url(&value))
            .unwrap_or_default(),
        metadata_json: canonical_metadata(metadata),
    })
}

fn parse_helius_nft(row: &Value) -> Option<SeedNftRecord> {
    let token_id = text(row.get("id"))?;
    let content = row.get("content");
    let metadata = content.and_then(|value| value.get("metadata"));
    let image = content
        .and_then(|value| value.get("links"))
        .and_then(|value| text(value.get("image")))
        .or_else(|| metadata.and_then(|value| text(value.get("image"))))
        .or_else(|| {
            content
                .and_then(|value| value.get("files"))
                .and_then(Value::as_array)
                .and_then(|files| files.first())
                .and_then(|file| text(file.get("uri")))
        })
        .unwrap_or_default();
    Some(SeedNftRecord {
        token_id,
        name_norm: metadata
            .and_then(|value| text(value.get("name")))
            .map(|value| normalize_name(&value))
            .unwrap_or_default(),
        token_uri_norm: content
            .and_then(|value| text(value.get("json_uri")))
            .and_then(|value| normalize_url(&value))
            .unwrap_or_default(),
        image_uri_norm: normalize_url(&image).unwrap_or_default(),
        metadata_json: canonical_metadata(metadata),
    })
}

fn canonical_metadata(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    if let Some(raw) = value.as_str() {
        return validated_metadata(raw).unwrap_or_default();
    }
    let Ok(raw) = serde_json::to_string(value) else {
        return String::new();
    };
    validated_metadata(&raw).unwrap_or_default()
}

fn text(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn json_usize(value: Option<&Value>) -> Option<usize> {
    value.and_then(|value| {
        value
            .as_u64()
            .and_then(|number| usize::try_from(number).ok())
            .or_else(|| value.as_str()?.parse().ok())
    })
}

fn encode_query(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn with_api_key(base: &str, key: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    if base.contains("api-key=") {
        base.to_owned()
    } else {
        format!(
            "{}{separator}api-key={}",
            base.trim_end_matches('/'),
            encode_query(key)
        )
    }
}

fn cache_path(dir: &Path, seed: &SeedRecord) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(seed.chain.as_bytes());
    hasher.update([0]);
    hasher.update(seed.address.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    dir.join(format!("{}__{}.jsonl.zst", seed.chain, &digest[..20]))
}

fn cache_ref(
    seed: &SeedRecord,
    path: &Path,
    footer: CacheFooter,
    reused: bool,
    resident_records: Option<Vec<SeedNftRecord>>,
) -> SeedNftCacheRef {
    SeedNftCacheRef {
        seed: seed.clone(),
        path: path.to_owned(),
        item_count: footer.item_count,
        provider_total: footer.provider_total,
        truncated: footer.truncated,
        reused,
        resident_records,
    }
}

fn validate_cache(path: &Path, seed: &SeedRecord) -> Result<SeedNftCacheRef, Analysis2Error> {
    let mut header = None;
    let mut footer = None;
    let mut resident_records = Vec::new();
    read_cache(path, |line_no, value| {
        if line_no == 0 {
            header = Some(serde_json::from_value::<CacheHeader>(value).map_err(|e| {
                Analysis2Error::invalid(format!("parse seed NFT cache header: {e}"))
            })?);
        } else if value.get("kind").and_then(Value::as_str) == Some("complete") {
            footer = Some(serde_json::from_value::<CacheFooter>(value).map_err(|e| {
                Analysis2Error::invalid(format!("parse seed NFT cache footer: {e}"))
            })?);
        } else {
            resident_records.push(
                serde_json::from_value::<SeedNftRecord>(value).map_err(|e| {
                    Analysis2Error::invalid(format!("parse seed NFT cache row: {e}"))
                })?,
            );
        }
        Ok(())
    })?;
    let header = header.ok_or_else(|| Analysis2Error::invalid("seed NFT cache missing header"))?;
    let footer = footer.ok_or_else(|| Analysis2Error::invalid("seed NFT cache incomplete"))?;
    let count = resident_records.len();
    let invalid_completion = if footer.truncated {
        count != MAX_SEED_NFTS_PER_CONTRACT
            || footer.provider_total.is_some_and(|total| total <= count)
    } else {
        footer.provider_total.is_some_and(|total| total != count)
    };
    if header.kind != "header"
        || footer.kind != "complete"
        || header.version != CACHE_VERSION
        || header.chain != seed.chain
        || header.address != seed.address
        || header.max_nfts != MAX_SEED_NFTS_PER_CONTRACT
        || footer.item_count != count
        || count > MAX_SEED_NFTS_PER_CONTRACT
        || invalid_completion
    {
        return Err(Analysis2Error::invalid(
            "seed NFT cache identity/count mismatch",
        ));
    }
    Ok(cache_ref(seed, path, footer, true, Some(resident_records)))
}

pub fn for_each_cached_nft(
    cache: &SeedNftCacheRef,
    mut callback: impl FnMut(SeedNftRecord) -> Result<(), Analysis2Error>,
) -> Result<(), Analysis2Error> {
    cache.for_each_nft(|record| callback(record.clone()))
}

/// Release decoded seed rows after their final consumer. The durable cache
/// reference and fingerprint remain usable.
pub fn release_resident_seed_nfts(caches: &mut [SeedNftCacheRef]) {
    for cache in caches {
        cache.resident_records = None;
    }
}

fn read_cache(
    path: &Path,
    mut callback: impl FnMut(usize, Value) -> Result<(), Analysis2Error>,
) -> Result<(), Analysis2Error> {
    let file = File::open(path)?;
    let decoder = zstd::stream::read::Decoder::new(BufReader::new(file))
        .map_err(|e| Analysis2Error::invalid(format!("open seed NFT zstd cache: {e}")))?;
    let reader = BufReader::new(decoder);
    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str(&line).map_err(|e| {
            Analysis2Error::invalid(format!(
                "parse seed NFT cache {} line {}: {e}",
                path.display(),
                line_no + 1
            ))
        })?;
        callback(line_no, value)?;
    }
    Ok(())
}

pub fn cache_fingerprint(caches: &[SeedNftCacheRef]) -> Result<String, Analysis2Error> {
    let mut hasher = Sha256::new();
    for cache in caches {
        hasher.update(cache.seed.chain.as_bytes());
        hasher.update([0]);
        hasher.update(cache.seed.address.as_bytes());
        hasher.update([0]);
        let mut file = File::open(&cache.path)?;
        let mut buffer = [0_u8; 128 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}
