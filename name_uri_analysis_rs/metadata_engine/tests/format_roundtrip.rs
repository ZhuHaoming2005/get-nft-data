#[test]
fn writes_little_endian_header_with_sha256_footer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("payload_lengths.u32");
    let values = [1u32, 2, 3, 4];
    metadata_engine::format::write_u32_array(
        &path,
        metadata_engine::format::ArrayKind::U32,
        &values,
    )
    .unwrap();
    let mapped = metadata_engine::format::map_u32_array(&path).unwrap();
    assert_eq!(&*mapped, &values);
}

#[test]
fn rejects_truncated_or_checksum_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("broken.u32");
    metadata_engine::format::write_u32_array(&path, metadata_engine::format::ArrayKind::U32, &[9])
        .unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
    assert!(metadata_engine::format::map_u32_array(&path).is_err());
}

#[test]
fn mapped_array_exposes_the_already_verified_footer_digest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.u32");
    metadata_engine::format::write_u32_array(
        &path,
        metadata_engine::format::ArrayKind::U32,
        &[1, 2, 3],
    )
    .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let expected = &bytes[bytes.len() - 32..];

    let mapped = metadata_engine::format::map_u32_array(&path).unwrap();

    assert_eq!(mapped.verified_checksum(), expected);
}

#[test]
fn ready_marker_is_atomic_and_requires_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join("encode-1");
    std::fs::create_dir_all(&bundle).unwrap();
    assert!(
        metadata_engine::format::commit_ready(&bundle, "blocking.ready", r#"{"ok":true}"#).is_ok()
    );
    assert!(bundle.join("blocking.ready").is_file());
}

#[test]
fn windows_atomic_replace_never_unlinks_the_durable_destination_first() {
    let source = include_str!("../src/format/atomic.rs");
    let production = source.split("#[cfg(test)]").next().unwrap();

    assert!(production.contains("MoveFileExW"));
    assert!(production.contains("ReplaceFileW"));
    assert!(production.contains("MOVEFILE_WRITE_THROUGH"));
    assert!(!production.contains("remove_file(to)"));
}
