use base64::Engine;
use dedup_core::{MetadataImagePairSample, ProgressObserver};
use rayon::prelude::*;
use reqwest::blocking::Client;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::{CONTENT_TYPE, LOCATION, RETRY_AFTER};
use reqwest::redirect::Policy;
use reqwest::{StatusCode, Url};
use serde::Serialize;
use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const DOWNLOAD_WORKERS: usize = 32;
const MAX_IMAGE_BYTES: u64 = 50 * 1024 * 1024;
const MAX_ATTEMPTS: usize = 2;
const MAX_REDIRECTS: usize = 10;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct PublicDnsResolver;

impl Resolve for PublicDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addresses = resolve_public_host(&host)
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?;
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

struct DownloadedImage {
    bytes: Vec<u8>,
    suffix: &'static str,
}

struct DownloadRow {
    index: usize,
    file_a: String,
    error_a: String,
    file_b: String,
    error_b: String,
    record_a: String,
    record_b: String,
}

struct StagedPair {
    sample: MetadataImagePairSample,
    file_a: PathBuf,
    suffix_a: &'static str,
    file_b: PathBuf,
    suffix_b: &'static str,
}

#[derive(Serialize)]
struct NftSampleRecord<'a> {
    chain: &'a str,
    contract_address: &'a str,
    token_id: &'a str,
    image_uri: &'a str,
    image_file: &'a str,
    metadata: serde_json::Value,
}

pub enum DownloadOutcome {
    Complete(Vec<MetadataImagePairSample>),
    Insufficient {
        successful: usize,
        candidates: usize,
    },
}

pub fn clear_published_metadata_image_samples(output_dir: &Path) -> Result<(), std::io::Error> {
    let manifest = output_dir.join("metadata_image_samples.csv");
    if manifest.exists() {
        fs::remove_file(manifest)?;
    }
    let image_root = output_dir.join("metadata_sample_images");
    if image_root.exists() {
        fs::remove_dir_all(image_root)?;
    }
    Ok(())
}

pub fn download_metadata_image_samples(
    output_dir: &Path,
    samples: &[MetadataImagePairSample],
    target: usize,
    progress: &dyn ProgressObserver,
) -> Result<DownloadOutcome, Box<dyn std::error::Error + Send + Sync>> {
    debug_assert!(target > 0);
    fs::create_dir_all(output_dir)?;
    let staging = tempfile::Builder::new()
        .prefix(".metadata-sample-images-")
        .tempdir_in(output_dir)?;
    let client = build_http_client()?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(DOWNLOAD_WORKERS.min(samples.len()).max(1))
        .build()?;
    let mut successful = Vec::with_capacity(target);
    let ordered_samples = random_candidate_order(samples).map_err(|error| {
        std::io::Error::other(format!("operating-system randomness unavailable: {error}"))
    })?;
    for (chunk_index, chunk) in ordered_samples.chunks(DOWNLOAD_WORKERS).enumerate() {
        check_cancelled(progress)?;
        let attempted = pool.install(|| {
            chunk
                .par_iter()
                .enumerate()
                .map(|(offset, &sample)| {
                    let candidate_index = chunk_index * DOWNLOAD_WORKERS + offset + 1;
                    let staged = stage_pair(
                        &client,
                        sample,
                        candidate_index,
                        staging.path(),
                        progress,
                    );
                    if !matches!(staged.as_ref(), Err(error) if error.kind() == std::io::ErrorKind::Interrupted)
                    {
                        progress.add_completed(1);
                    }
                    staged
                })
                .collect::<Vec<_>>()
        });
        for staged in attempted {
            match staged {
                Ok(staged) => successful.push(staged),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                    return Err(error.into());
                }
                Err(_) => {}
            }
        }
        if successful.len() >= target {
            break;
        }
    }
    if successful.len() < target {
        return Ok(DownloadOutcome::Insufficient {
            successful: successful.len(),
            candidates: samples.len(),
        });
    }
    successful.truncate(target);

    let publish_root = staging.path().join("publish");
    fs::create_dir(&publish_root)?;
    let mut rows = Vec::with_capacity(target);
    let mut selected = Vec::with_capacity(target);
    for (offset, staged) in successful.into_iter().enumerate() {
        let index = offset + 1;
        let row_dir = publish_root.join(index.to_string());
        fs::create_dir(&row_dir)?;
        let destination_a = row_dir.join(format!("{index}a{}", staged.suffix_a));
        let destination_b = row_dir.join(format!("{index}b{}", staged.suffix_b));
        fs::rename(staged.file_a, &destination_a)?;
        fs::rename(staged.file_b, &destination_b)?;
        let file_name_a = destination_a
            .file_name()
            .expect("published image always has a file name")
            .to_string_lossy();
        let file_name_b = destination_b
            .file_name()
            .expect("published image always has a file name")
            .to_string_lossy();
        write_nft_record(
            &row_dir.join(format!("{index}a.json")),
            NftSampleRecord {
                chain: &staged.sample.contract_a_chain,
                contract_address: &staged.sample.contract_a_address,
                token_id: &staged.sample.token_id_a,
                image_uri: &staged.sample.image_uri_a,
                image_file: &file_name_a,
                metadata: serde_json::from_str(&staged.sample.metadata_json_a)?,
            },
        )?;
        write_nft_record(
            &row_dir.join(format!("{index}b.json")),
            NftSampleRecord {
                chain: &staged.sample.contract_b_chain,
                contract_address: &staged.sample.contract_b_address,
                token_id: &staged.sample.token_id_b,
                image_uri: &staged.sample.image_uri_b,
                image_file: &file_name_b,
                metadata: serde_json::from_str(&staged.sample.metadata_json_b)?,
            },
        )?;
        rows.push(DownloadRow {
            index,
            file_a: relative_image_path(index, &destination_a),
            error_a: String::new(),
            file_b: relative_image_path(index, &destination_b),
            error_b: String::new(),
            record_a: format!("metadata_sample_images/{index}/{index}a.json"),
            record_b: format!("metadata_sample_images/{index}/{index}b.json"),
        });
        selected.push(staged.sample);
    }

    let image_root = output_dir.join("metadata_sample_images");
    if image_root.exists() {
        fs::remove_dir_all(&image_root)?;
    }
    fs::rename(&publish_root, &image_root)?;
    write_manifest(output_dir, &selected, &rows)?;
    Ok(DownloadOutcome::Complete(selected))
}

fn random_candidate_order(
    samples: &[MetadataImagePairSample],
) -> Result<Vec<&MetadataImagePairSample>, getrandom::Error> {
    let mut shuffled = samples.iter().collect::<Vec<_>>();
    for upper in (2..=shuffled.len()).rev() {
        let limit = u64::try_from(upper).expect("candidate count fits in u64");
        let minimum = limit.wrapping_neg() % limit;
        let index = loop {
            let value = getrandom::u64()?;
            if value >= minimum {
                break usize::try_from(value % limit).expect("random index fits in usize");
            }
        };
        shuffled.swap(upper - 1, index);
    }
    Ok(shuffled)
}

fn build_http_client() -> Result<Client, reqwest::Error> {
    Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .user_agent("dedup-metadata-image-sampler/1.0")
        .redirect(Policy::none())
        .no_proxy()
        .dns_resolver(Arc::new(PublicDnsResolver))
        .build()
}

fn stage_pair(
    client: &Client,
    sample: &MetadataImagePairSample,
    candidate_index: usize,
    staging_root: &Path,
    progress: &dyn ProgressObserver,
) -> Result<StagedPair, std::io::Error> {
    check_cancelled(progress)?;
    let row_dir = staging_root.join(format!("candidate-{candidate_index}"));
    fs::create_dir(&row_dir)?;
    let image_a = download_image(client, &sample.image_uri_a, progress)?;
    let file_a = row_dir.join(format!("a{}", image_a.suffix));
    atomic_write(&file_a, &image_a.bytes)?;
    check_cancelled(progress)?;
    let image_b = download_image(client, &sample.image_uri_b, progress)?;
    let file_b = row_dir.join(format!("b{}", image_b.suffix));
    atomic_write(&file_b, &image_b.bytes)?;
    Ok(StagedPair {
        sample: sample.clone(),
        file_a,
        suffix_a: image_a.suffix,
        file_b,
        suffix_b: image_b.suffix,
    })
}

fn relative_image_path(index: usize, path: &Path) -> String {
    let file_name = path
        .file_name()
        .expect("published image always has a file name")
        .to_string_lossy();
    format!("metadata_sample_images/{index}/{file_name}")
}

fn download_image(
    client: &Client,
    image_uri: &str,
    progress: &dyn ProgressObserver,
) -> Result<DownloadedImage, std::io::Error> {
    check_cancelled(progress)?;
    if let Some(data) = image_uri.trim().strip_prefix("data:") {
        return decode_data_image(data);
    }
    let url = normalize_image_uri(image_uri)?;
    let mut last_error = None;
    for attempt in 0..MAX_ATTEMPTS {
        check_cancelled(progress)?;
        match send_public_get(client, &url, progress) {
            Ok((response, final_url)) if response.status().is_success() => {
                if response
                    .content_length()
                    .is_some_and(|size| size > MAX_IMAGE_BYTES)
                {
                    return Err(std::io::Error::other("image exceeds 50 MiB limit"));
                }
                let content_type = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                let bytes = read_response_body(response, progress)?;
                if bytes.len() as u64 > MAX_IMAGE_BYTES {
                    return Err(std::io::Error::other("image exceeds 50 MiB limit"));
                }
                let suffix = infer_image_suffix(&content_type, &final_url, &bytes)?;
                return Ok(DownloadedImage { bytes, suffix });
            }
            Ok((response, _)) => {
                let status = response.status();
                let retryable = status.as_u16() == 429 || status.is_server_error();
                let retry_after = response
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok());
                last_error = Some(format!("HTTP {status}"));
                if !retryable || attempt + 1 == MAX_ATTEMPTS {
                    break;
                }
                cancellable_sleep(
                    Duration::from_secs(
                        retry_after
                            .unwrap_or(1_u64 << attempt.min(2))
                            .min(MAX_RETRY_DELAY.as_secs()),
                    ),
                    progress,
                )?;
            }
            Err(error) => {
                if error.kind() == std::io::ErrorKind::PermissionDenied
                    || error.kind() == std::io::ErrorKind::InvalidInput
                {
                    return Err(error);
                }
                last_error = Some(error.to_string());
                if attempt + 1 == MAX_ATTEMPTS {
                    break;
                }
                cancellable_sleep(
                    Duration::from_secs(1_u64 << attempt.min(2)).min(MAX_RETRY_DELAY),
                    progress,
                )?;
            }
        }
    }
    Err(std::io::Error::other(
        last_error.unwrap_or_else(|| "download failed".to_owned()),
    ))
}

fn send_public_get(
    client: &Client,
    initial_url: &str,
    progress: &dyn ProgressObserver,
) -> Result<(reqwest::blocking::Response, String), std::io::Error> {
    let mut current = Url::parse(initial_url)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    for redirect in 0..=MAX_REDIRECTS {
        check_cancelled(progress)?;
        validate_request_url(&current)?;
        let response = client
            .get(current.clone())
            .send()
            .map_err(std::io::Error::other)?;
        if !is_followed_redirect(response.status()) {
            return Ok((response, current.to_string()));
        }
        if redirect == MAX_REDIRECTS {
            return Err(std::io::Error::other("too many image redirects"));
        }
        let location = response
            .headers()
            .get(LOCATION)
            .ok_or_else(|| std::io::Error::other("image redirect has no Location header"))?
            .to_str()
            .map_err(|error| {
                std::io::Error::other(format!("invalid redirect Location: {error}"))
            })?;
        current = current.join(location).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid image redirect: {error}"),
            )
        })?;
    }
    unreachable!("redirect loop returns or errors at its bound")
}

fn read_response_body(
    response: reqwest::blocking::Response,
    progress: &dyn ProgressObserver,
) -> Result<Vec<u8>, std::io::Error> {
    let mut reader = response.take(MAX_IMAGE_BYTES + 1);
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        check_cancelled(progress)?;
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
}

fn cancellable_sleep(
    duration: Duration,
    progress: &dyn ProgressObserver,
) -> Result<(), std::io::Error> {
    let deadline = Instant::now() + duration;
    loop {
        check_cancelled(progress)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }
        std::thread::sleep(remaining.min(Duration::from_millis(100)));
    }
}

fn check_cancelled(progress: &dyn ProgressObserver) -> Result<(), std::io::Error> {
    progress
        .check_cancelled()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::Interrupted, error.to_string()))
}

fn is_followed_redirect(status: StatusCode) -> bool {
    matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308)
}

fn validate_request_url(url: &Url) -> Result<(), std::io::Error> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "image URL must use HTTP or HTTPS",
        ));
    }
    let host = url.host().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "image URL has no host")
    })?;
    let allowed = match host {
        url::Host::Domain(_) => true,
        url::Host::Ipv4(address) => is_public_ipv4(address),
        url::Host::Ipv6(address) => is_public_ipv6(address),
    };
    if allowed {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "image URL resolves to a non-public network address",
        ))
    }
}

fn resolve_public_host(host: &str) -> Result<Vec<SocketAddr>, std::io::Error> {
    let addresses = (host, 0).to_socket_addrs()?.collect::<Vec<_>>();
    let public = addresses
        .into_iter()
        .filter(|address| is_public_ip(address.ip()))
        .collect::<Vec<_>>();
    if public.is_empty() {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("image host {host:?} has no public network address"),
        ))
    } else {
        Ok(public)
    }
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    if let Some(mapped) = address.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }
    let segments = address.segments();
    !(address.is_unspecified()
        || address.is_loopback()
        || address.is_multicast()
        || segments[0] & 0xe000 != 0x2000
        || segments[0] & 0xfe00 == 0xfc00
        || segments[0] & 0xffc0 == 0xfe80
        || segments[0] & 0xffc0 == 0xfec0
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || (segments[0] == 0x2001 && segments[1] == 0)
        || segments[0] == 0x2002
        || (segments[0] == 0x0064 && segments[1] == 0xff9b))
}

fn decode_data_image(data: &str) -> Result<DownloadedImage, std::io::Error> {
    let (metadata, payload) = data
        .split_once(',')
        .ok_or_else(|| std::io::Error::other("invalid data image URI"))?;
    let mime = metadata.split(';').next().unwrap_or("");
    if !metadata.split(';').any(|part| part == "base64") {
        return Err(std::io::Error::other(
            "only base64 data image URIs are supported",
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|error| std::io::Error::other(format!("invalid base64 image: {error}")))?;
    if bytes.len() as u64 > MAX_IMAGE_BYTES {
        return Err(std::io::Error::other("image exceeds 50 MiB limit"));
    }
    let suffix = infer_image_suffix(mime, "", &bytes)?;
    Ok(DownloadedImage { bytes, suffix })
}

fn normalize_image_uri(uri: &str) -> Result<String, std::io::Error> {
    let uri = uri.trim();
    let lowered = uri.to_ascii_lowercase();
    if lowered.starts_with("ipfs:") {
        let path = uri["ipfs:".len()..].trim_start_matches('/');
        let path = if path
            .get(.."ipfs/".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ipfs/"))
        {
            &path["ipfs/".len()..]
        } else {
            path
        };
        if path.is_empty() {
            return Err(std::io::Error::other("empty IPFS image URI"));
        }
        return Ok(format!(
            "https://ipfs.io/ipfs/{}",
            path.trim_start_matches('/')
        ));
    }
    if lowered.starts_with("ar:") {
        let path = uri["ar:".len()..].trim_start_matches('/');
        if path.is_empty() {
            return Err(std::io::Error::other("empty Arweave image URI"));
        }
        return Ok(format!(
            "https://arweave.net/{}",
            path.trim_start_matches('/')
        ));
    }
    if lowered.starts_with("http://") || lowered.starts_with("https://") {
        return Ok(uri.to_owned());
    }
    Err(std::io::Error::other("unsupported image URI scheme"))
}

fn infer_image_suffix(
    content_type: &str,
    url: &str,
    bytes: &[u8],
) -> Result<&'static str, std::io::Error> {
    let mime = content_type.split(';').next().unwrap_or("").trim();
    if let Some(suffix) = suffix_for_mime(mime) {
        return Ok(suffix);
    }
    let path = url
        .split(['?', '#'])
        .next()
        .unwrap_or(url)
        .to_ascii_lowercase();
    for suffix in [
        ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".avif", ".bmp", ".tif", ".tiff", ".ico",
    ] {
        if path.ends_with(suffix) {
            return Ok(match suffix {
                ".jpeg" => ".jpg",
                ".tif" => ".tiff",
                _ => suffix,
            });
        }
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Ok(".png");
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Ok(".jpg");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Ok(".gif");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return Ok(".webp");
    }
    if bytes.get(..1024.min(bytes.len())).is_some_and(|prefix| {
        String::from_utf8_lossy(prefix)
            .to_ascii_lowercase()
            .contains("<svg")
    }) {
        return Ok(".svg");
    }
    Err(std::io::Error::other("response is not a recognized image"))
}

fn suffix_for_mime(mime: &str) -> Option<&'static str> {
    match mime {
        "image/png" => Some(".png"),
        "image/jpeg" => Some(".jpg"),
        "image/gif" => Some(".gif"),
        "image/webp" => Some(".webp"),
        "image/svg+xml" => Some(".svg"),
        "image/avif" => Some(".avif"),
        "image/bmp" => Some(".bmp"),
        "image/tiff" => Some(".tiff"),
        "image/x-icon" => Some(".ico"),
        _ => None,
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("image path has no parent"))?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn write_nft_record(
    path: &Path,
    record: NftSampleRecord<'_>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bytes = serde_json::to_vec_pretty(&record)?;
    atomic_write(path, &bytes)?;
    Ok(())
}

fn write_manifest(
    output_dir: &Path,
    samples: &[MetadataImagePairSample],
    rows: &[DownloadRow],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = output_dir.join("metadata_image_samples.csv");
    let mut temporary = NamedTempFile::new_in(output_dir)?;
    {
        let mut writer = csv::Writer::from_writer(&mut temporary);
        writer.write_record([
            "row",
            "contract_a_chain",
            "contract_a_address",
            "token_id_a",
            "image_uri_a",
            "file_a",
            "error_a",
            "contract_b_chain",
            "contract_b_address",
            "token_id_b",
            "image_uri_b",
            "file_b",
            "error_b",
            "record_a",
            "record_b",
        ])?;
        for (sample, row) in samples.iter().zip(rows) {
            writer.write_record([
                row.index.to_string(),
                sample.contract_a_chain.clone(),
                sample.contract_a_address.clone(),
                sample.token_id_a.clone(),
                sample.image_uri_a.clone(),
                row.file_a.clone(),
                row.error_a.clone(),
                sample.contract_b_chain.clone(),
                sample.contract_b_address.clone(),
                sample.token_id_b.clone(),
                sample.image_uri_b.clone(),
                row.file_b.clone(),
                row.error_b.clone(),
                row.record_a.clone(),
                row.record_b.clone(),
            ])?;
        }
        writer.flush()?;
    }
    temporary.persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dedup_core::{DedupError, NoopProgress};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct CountingProgress(AtomicU64);

    impl ProgressObserver for CountingProgress {
        fn set_stage(&self, _stage: &str) {}
        fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
        fn set_total(&self, _total: Option<u64>) {}
        fn add_completed(&self, delta: u64) {
            self.0.fetch_add(delta, Ordering::Relaxed);
        }
    }

    struct CancelledProgress;

    impl ProgressObserver for CancelledProgress {
        fn set_stage(&self, _stage: &str) {}
        fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
        fn set_total(&self, _total: Option<u64>) {}
        fn add_completed(&self, _delta: u64) {}
        fn check_cancelled(&self) -> Result<(), DedupError> {
            Err(DedupError::Interrupted)
        }
    }

    fn sample(address: &str, image_uri: &str) -> MetadataImagePairSample {
        MetadataImagePairSample {
            contract_a_chain: "ethereum".to_owned(),
            contract_a_address: address.to_owned(),
            token_id_a: "1".to_owned(),
            image_uri_a: image_uri.to_owned(),
            metadata_json_a: r#"{"name":"left"}"#.to_owned(),
            contract_b_chain: "ethereum".to_owned(),
            contract_b_address: format!("{address}-peer"),
            token_id_b: "2".to_owned(),
            image_uri_b: image_uri.to_owned(),
            metadata_json_b: r#"{"name":"right"}"#.to_owned(),
        }
    }

    #[test]
    fn normalizes_decentralized_uris() {
        assert_eq!(
            normalize_image_uri("ipfs://ipfs/QmHash/image.png").unwrap(),
            "https://ipfs.io/ipfs/QmHash/image.png"
        );
        assert_eq!(
            normalize_image_uri("ar://transaction").unwrap(),
            "https://arweave.net/transaction"
        );
        assert_eq!(
            normalize_image_uri("ipfs:QmNormalized/image.png").unwrap(),
            "https://ipfs.io/ipfs/QmNormalized/image.png"
        );
        assert_eq!(
            normalize_image_uri("ar:normalized-transaction/image.png").unwrap(),
            "https://arweave.net/normalized-transaction/image.png"
        );
    }

    #[test]
    fn infers_image_types_without_transcoding() {
        assert_eq!(
            infer_image_suffix("image/jpeg", "https://x/a", b"raw").unwrap(),
            ".jpg"
        );
        assert_eq!(
            infer_image_suffix("", "https://x/a", b"\x89PNG\r\n\x1a\nrest").unwrap(),
            ".png"
        );
    }

    #[test]
    fn decodes_original_data_image_bytes() {
        let expected = b"\x89PNG\r\n\x1a\n".to_vec();
        let client = build_http_client().unwrap();
        let image =
            download_image(&client, "data:image/png;base64,iVBORw0KGgo=", &NoopProgress).unwrap();
        assert_eq!(image.suffix, ".png");
        assert_eq!(image.bytes, expected);
    }

    #[test]
    fn rejects_non_public_literal_and_resolved_hosts() {
        for url in [
            "http://127.0.0.1/image.png",
            "http://10.0.0.1/image.png",
            "http://169.254.169.254/latest/meta-data",
            "http://[::1]/image.png",
            "http://[::127.0.0.1]/image.png",
            "http://[::ffff:127.0.0.1]/image.png",
            "http://[fe80::1]/image.png",
            "http://[fc00::1]/image.png",
        ] {
            let parsed = Url::parse(url).unwrap();
            assert_eq!(
                validate_request_url(&parsed).unwrap_err().kind(),
                std::io::ErrorKind::PermissionDenied,
                "unsafe URL was accepted: {url}"
            );
        }
        assert_eq!(
            resolve_public_host("localhost").unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        validate_request_url(&Url::parse("https://8.8.8.8/image.png").unwrap()).unwrap();

        let client = build_http_client().unwrap();
        assert_eq!(
            send_public_get(&client, "http://127.0.0.1/image.png", &NoopProgress,)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        let redirect = Url::parse("https://example.com/image.png")
            .unwrap()
            .join("http://169.254.169.254/latest/meta-data")
            .unwrap();
        assert_eq!(
            validate_request_url(&redirect).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn random_candidate_order_preserves_every_pair_and_allows_reuse() {
        let good = "data:image/png;base64,iVBORw0KGgo=";
        let mut first = sample("0xhub", good);
        first.contract_b_address = "0xone".to_owned();
        let mut repeated = sample("0xhub", good);
        repeated.contract_b_address = "0xtwo".to_owned();
        let mut disjoint = sample("0xthree", good);
        disjoint.contract_b_address = "0xfour".to_owned();
        let candidates = [first, repeated, disjoint];

        let ordered = random_candidate_order(&candidates).unwrap();
        let mut addresses = ordered
            .iter()
            .map(|sample| sample.contract_a_address.as_str())
            .collect::<Vec<_>>();
        addresses.sort_unstable();
        assert_eq!(addresses, vec!["0xhub", "0xhub", "0xthree"]);
    }

    #[test]
    fn failed_pair_is_discarded_and_replaced_by_a_complete_pair() {
        let temp = tempfile::tempdir().unwrap();
        let good = "data:image/png;base64,iVBORw0KGgo=";
        let candidates = [
            sample("0xfail", "unsupported://image"),
            sample("0xok", good),
        ];
        let progress = CountingProgress::default();
        let outcome =
            download_metadata_image_samples(temp.path(), &candidates, 1, &progress).unwrap();
        let DownloadOutcome::Complete(selected) = outcome else {
            panic!("a complete fallback pair should have been selected");
        };
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].contract_a_address, "0xok");
        assert_eq!(progress.0.load(Ordering::Relaxed), 2);
        assert!(
            temp.path()
                .join("metadata_sample_images/1/1a.png")
                .is_file()
        );
        assert!(
            temp.path()
                .join("metadata_sample_images/1/1b.png")
                .is_file()
        );
        let nft_a: serde_json::Value = serde_json::from_slice(
            &fs::read(temp.path().join("metadata_sample_images/1/1a.json")).unwrap(),
        )
        .unwrap();
        let nft_b: serde_json::Value = serde_json::from_slice(
            &fs::read(temp.path().join("metadata_sample_images/1/1b.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(nft_a["contract_address"], "0xok");
        assert_eq!(nft_a["token_id"], "1");
        assert_eq!(nft_a["image_file"], "1a.png");
        assert_eq!(nft_a["metadata"]["name"], "left");
        assert_eq!(nft_b["contract_address"], "0xok-peer");
        assert_eq!(nft_b["token_id"], "2");
        assert_eq!(nft_b["image_file"], "1b.png");
        assert_eq!(nft_b["metadata"]["name"], "right");
        let mut manifest =
            csv::Reader::from_path(temp.path().join("metadata_image_samples.csv")).unwrap();
        assert!(
            manifest
                .headers()
                .unwrap()
                .iter()
                .any(|field| field == "record_a")
        );
        assert!(
            manifest
                .headers()
                .unwrap()
                .iter()
                .any(|field| field == "record_b")
        );
        assert_eq!(manifest.records().count(), 1);
    }

    #[test]
    fn insufficient_candidates_do_not_publish_partial_output() {
        let temp = tempfile::tempdir().unwrap();
        let candidates = [sample("0xok", "data:image/png;base64,iVBORw0KGgo=")];
        let outcome =
            download_metadata_image_samples(temp.path(), &candidates, 2, &NoopProgress).unwrap();
        let DownloadOutcome::Insufficient {
            successful,
            candidates,
        } = outcome
        else {
            panic!("one candidate cannot satisfy a target of two");
        };
        assert_eq!(successful, 1);
        assert_eq!(candidates, 1);
        assert!(!temp.path().join("metadata_image_samples.csv").exists());
        assert!(!temp.path().join("metadata_sample_images").exists());
    }

    #[test]
    fn cancellation_stops_before_starting_candidate_downloads() {
        let temp = tempfile::tempdir().unwrap();
        let candidates = [sample("0xcancelled", "data:image/png;base64,iVBORw0KGgo=")];
        let error = match download_metadata_image_samples(
            temp.path(),
            &candidates,
            1,
            &CancelledProgress,
        ) {
            Ok(_) => panic!("cancelled download unexpectedly completed"),
            Err(error) => error,
        };
        assert_eq!(
            error
                .downcast_ref::<std::io::Error>()
                .expect("cancellation remains an I/O interruption")
                .kind(),
            std::io::ErrorKind::Interrupted
        );
        assert!(!temp.path().join("metadata_image_samples.csv").exists());
        assert!(!temp.path().join("metadata_sample_images").exists());
    }
}
