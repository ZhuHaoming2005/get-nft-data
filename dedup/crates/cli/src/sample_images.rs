use base64::Engine;
use dedup_core::MetadataImagePairSample;
use rayon::prelude::*;
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::NamedTempFile;

const DOWNLOAD_WORKERS: usize = 8;
const MAX_IMAGE_BYTES: u64 = 50 * 1024 * 1024;
const MAX_ATTEMPTS: usize = 5;

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
}

struct StagedPair {
    sample: MetadataImagePairSample,
    file_a: PathBuf,
    suffix_a: &'static str,
    file_b: PathBuf,
    suffix_b: &'static str,
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
) -> Result<DownloadOutcome, Box<dyn std::error::Error + Send + Sync>> {
    debug_assert!(target > 0);
    fs::create_dir_all(output_dir)?;
    let staging = tempfile::Builder::new()
        .prefix(".metadata-sample-images-")
        .tempdir_in(output_dir)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent("dedup-metadata-image-sampler/1.0")
        .build()?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(DOWNLOAD_WORKERS.min(samples.len()).max(1))
        .build()?;
    let mut successful = Vec::with_capacity(target);
    for (chunk_index, chunk) in samples.chunks(DOWNLOAD_WORKERS).enumerate() {
        let staged = pool.install(|| {
            chunk
                .par_iter()
                .enumerate()
                .filter_map(|(offset, sample)| {
                    let candidate_index = chunk_index * DOWNLOAD_WORKERS + offset + 1;
                    stage_pair(&client, sample, candidate_index, staging.path()).ok()
                })
                .collect::<Vec<_>>()
        });
        successful.extend(staged);
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
        rows.push(DownloadRow {
            index,
            file_a: relative_image_path(index, &destination_a),
            error_a: String::new(),
            file_b: relative_image_path(index, &destination_b),
            error_b: String::new(),
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

fn stage_pair(
    client: &Client,
    sample: &MetadataImagePairSample,
    candidate_index: usize,
    staging_root: &Path,
) -> Result<StagedPair, std::io::Error> {
    let row_dir = staging_root.join(format!("candidate-{candidate_index}"));
    fs::create_dir(&row_dir)?;
    let image_a = download_image(client, &sample.image_uri_a)?;
    let file_a = row_dir.join(format!("a{}", image_a.suffix));
    atomic_write(&file_a, &image_a.bytes)?;
    let image_b = download_image(client, &sample.image_uri_b)?;
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

fn download_image(client: &Client, image_uri: &str) -> Result<DownloadedImage, std::io::Error> {
    if let Some(data) = image_uri.trim().strip_prefix("data:") {
        return decode_data_image(data);
    }
    let url = normalize_image_uri(image_uri)?;
    let mut last_error = None;
    for attempt in 0..MAX_ATTEMPTS {
        match client.get(&url).send() {
            Ok(response) if response.status().is_success() => {
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
                let mut bytes = Vec::new();
                response.take(MAX_IMAGE_BYTES + 1).read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MAX_IMAGE_BYTES {
                    return Err(std::io::Error::other("image exceeds 50 MiB limit"));
                }
                let suffix = infer_image_suffix(&content_type, &url, &bytes)?;
                return Ok(DownloadedImage { bytes, suffix });
            }
            Ok(response) => {
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
                std::thread::sleep(Duration::from_secs(
                    retry_after.unwrap_or(1_u64 << attempt.min(5)).min(30),
                ));
            }
            Err(error) => {
                last_error = Some(error.to_string());
                if attempt + 1 == MAX_ATTEMPTS {
                    break;
                }
                std::thread::sleep(Duration::from_secs(1_u64 << attempt.min(5)));
            }
        }
    }
    Err(std::io::Error::other(
        last_error.unwrap_or_else(|| "download failed".to_owned()),
    ))
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
    if let Some(path) = uri.strip_prefix("ipfs://") {
        let path = path.strip_prefix("ipfs/").unwrap_or(path);
        return Ok(format!(
            "https://ipfs.io/ipfs/{}",
            path.trim_start_matches('/')
        ));
    }
    if let Some(path) = uri.strip_prefix("ar://") {
        return Ok(format!(
            "https://arweave.net/{}",
            path.trim_start_matches('/')
        ));
    }
    if uri.starts_with("http://") || uri.starts_with("https://") {
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
    use std::net::TcpListener;

    fn sample(address: &str, image_uri: &str) -> MetadataImagePairSample {
        MetadataImagePairSample {
            contract_a_chain: "ethereum".to_owned(),
            contract_a_address: address.to_owned(),
            token_id_a: "1".to_owned(),
            image_uri_a: image_uri.to_owned(),
            contract_b_chain: "ethereum".to_owned(),
            contract_b_address: format!("{address}-peer"),
            token_id_b: "2".to_owned(),
            image_uri_b: image_uri.to_owned(),
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
    fn downloads_original_image_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let expected = b"\x89PNG\r\n\x1a\noriginal-bytes".to_vec();
        let response_bytes = expected.clone();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).unwrap();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_bytes.len()
            )
            .unwrap();
            stream.write_all(&response_bytes).unwrap();
        });
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let image = download_image(&client, &format!("http://{address}/image")).unwrap();
        server.join().unwrap();
        assert_eq!(image.suffix, ".png");
        assert_eq!(image.bytes, expected);
    }

    #[test]
    fn failed_pair_is_discarded_and_replaced_by_a_complete_pair() {
        let temp = tempfile::tempdir().unwrap();
        let good = "data:image/png;base64,iVBORw0KGgo=";
        let candidates = [
            sample("0xfail", "unsupported://image"),
            sample("0xok", good),
        ];
        let outcome = download_metadata_image_samples(temp.path(), &candidates, 1).unwrap();
        let DownloadOutcome::Complete(selected) = outcome else {
            panic!("a complete fallback pair should have been selected");
        };
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].contract_a_address, "0xok");
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
        let mut manifest =
            csv::Reader::from_path(temp.path().join("metadata_image_samples.csv")).unwrap();
        assert_eq!(manifest.records().count(), 1);
    }

    #[test]
    fn insufficient_candidates_do_not_publish_partial_output() {
        let temp = tempfile::tempdir().unwrap();
        let candidates = [sample("0xok", "data:image/png;base64,iVBORw0KGgo=")];
        let outcome = download_metadata_image_samples(temp.path(), &candidates, 2).unwrap();
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
}
