use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use metadata_engine::format::{
    write_f64_array, write_f64_iter, write_u32_array, write_u32_iter, write_u64_array,
    write_u64_iter, write_u64_iter_with_progress, write_u8_array, ArrayHeader, ArrayKind,
    TypedArraySink,
};
use sha2::{Digest, Sha256};

struct LargestAllocationTracker;

static LARGEST_ALLOCATION: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for LargestAllocationTracker {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: LargestAllocationTracker = LargestAllocationTracker;

fn record_allocation(bytes: usize) {
    LARGEST_ALLOCATION.fetch_max(bytes, Ordering::Relaxed);
}

fn legacy_typed_array_bytes(kind: ArrayKind, payload: Vec<u8>, elements: usize) -> Vec<u8> {
    let header = ArrayHeader::new(kind, elements as u64, payload.len() as u64).encode();
    let mut hasher = Sha256::new();
    hasher.update(header);
    hasher.update(&payload);
    let checksum = hasher.finalize();

    let mut bytes = Vec::with_capacity(32 + payload.len() + checksum.len());
    bytes.extend_from_slice(&header);
    bytes.resize(32, 0);
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&checksum);
    bytes
}

#[test]
fn typed_array_writers_remain_byte_compatible_for_boundary_and_random_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = 0x4d59_5df4_d0f3_3173u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut u8_values = vec![0, 1, u8::MAX, 17, 83];
    let mut u32_values = vec![0, 1, u32::MAX, 0x0123_4567, 0x89ab_cdef];
    let mut u64_values = vec![0, 1, u64::MAX, 0x0123_4567_89ab_cdef];
    let mut f64_values = vec![0.0, -0.0, 1.0, f64::MIN, f64::MAX, f64::NAN];
    for _ in 0..257 {
        let value = next();
        u8_values.push(value as u8);
        u32_values.push(value as u32);
        u64_values.push(value);
        f64_values.push(f64::from_bits(value));
    }

    let path = dir.path().join("values.u8");
    write_u8_array(&path, &u8_values).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        legacy_typed_array_bytes(ArrayKind::U8, u8_values.clone(), u8_values.len())
    );

    let path = dir.path().join("values.u32");
    write_u32_array(&path, ArrayKind::U32, &u32_values).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        legacy_typed_array_bytes(
            ArrayKind::U32,
            u32_values
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect(),
            u32_values.len(),
        )
    );

    let path = dir.path().join("values.u64");
    write_u64_array(&path, ArrayKind::U64, &u64_values).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        legacy_typed_array_bytes(
            ArrayKind::U64,
            u64_values
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect(),
            u64_values.len(),
        )
    );

    let path = dir.path().join("values.f64");
    write_f64_array(&path, ArrayKind::F64, &f64_values).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        legacy_typed_array_bytes(
            ArrayKind::F64,
            f64_values
                .iter()
                .flat_map(|value| value.to_bits().to_le_bytes())
                .collect(),
            f64_values.len(),
        )
    );

    let path = dir.path().join("empty.u64");
    write_u64_array(&path, ArrayKind::U64, &[]).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        legacy_typed_array_bytes(ArrayKind::U64, Vec::new(), 0)
    );
}

#[test]
fn typed_array_writer_does_not_allocate_an_array_sized_byte_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.u64");
    let values = vec![0x0123_4567_89ab_cdefu64; 2 * 1024 * 1024];
    LARGEST_ALLOCATION.store(0, Ordering::Relaxed);

    write_u64_array(&path, ArrayKind::U64, &values).unwrap();

    let largest = LARGEST_ALLOCATION.load(Ordering::Relaxed);
    assert!(
        largest <= 1024 * 1024,
        "writer allocated a full-array buffer of {largest} bytes"
    );
}

#[test]
fn iterator_writers_are_byte_compatible_without_materializing_the_column() {
    let dir = tempfile::tempdir().unwrap();

    let u32_path = dir.path().join("iter.u32");
    write_u32_iter(&u32_path, ArrayKind::U32, 4, [1, 2, 3, u32::MAX]).unwrap();
    let slice_u32_path = dir.path().join("slice.u32");
    write_u32_array(&slice_u32_path, ArrayKind::U32, &[1, 2, 3, u32::MAX]).unwrap();
    assert_eq!(
        std::fs::read(u32_path).unwrap(),
        std::fs::read(slice_u32_path).unwrap()
    );

    let u64_path = dir.path().join("iter.u64");
    write_u64_iter(&u64_path, ArrayKind::U64, 3, [1, 2, u64::MAX]).unwrap();
    let slice_u64_path = dir.path().join("slice.u64");
    write_u64_array(&slice_u64_path, ArrayKind::U64, &[1, 2, u64::MAX]).unwrap();
    assert_eq!(
        std::fs::read(u64_path).unwrap(),
        std::fs::read(slice_u64_path).unwrap()
    );

    let f64_path = dir.path().join("iter.f64");
    write_f64_iter(&f64_path, ArrayKind::F64, 3, [1.0, -0.0, f64::NAN]).unwrap();
    let slice_f64_path = dir.path().join("slice.f64");
    write_f64_array(&slice_f64_path, ArrayKind::F64, &[1.0, -0.0, f64::NAN]).unwrap();
    assert_eq!(
        std::fs::read(f64_path).unwrap(),
        std::fs::read(slice_f64_path).unwrap()
    );
}

#[test]
fn iterator_writers_fail_closed_on_a_length_mismatch_without_publishing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mismatch.u32");

    assert!(write_u32_iter(&path, ArrayKind::U32, 3, [1, 2]).is_err());
    assert!(!path.exists());
    assert!(write_u32_iter(&path, ArrayKind::U32, 1, [1, 2]).is_err());
    assert!(!path.exists());
}

#[test]
fn incremental_sink_matches_the_slice_writer_and_publishes_only_on_finish() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sink.u32");
    let mut sink = TypedArraySink::create(&path, ArrayKind::U32, 4).unwrap();
    sink.push_u32(1).unwrap();
    sink.push_u32(2).unwrap();
    assert!(!path.exists());
    sink.push_u32(3).unwrap();
    sink.push_u32(u32::MAX).unwrap();
    sink.finish().unwrap();

    let expected = dir.path().join("expected.u32");
    write_u32_array(&expected, ArrayKind::U32, &[1, 2, 3, u32::MAX]).unwrap();
    assert_eq!(
        std::fs::read(path).unwrap(),
        std::fs::read(expected).unwrap()
    );
}

#[test]
fn incremental_sink_interruption_or_wrong_count_keeps_the_durable_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sink.u64");
    std::fs::write(&path, b"durable").unwrap();
    {
        let mut sink = TypedArraySink::create(&path, ArrayKind::U64, 2).unwrap();
        sink.push_u64(1).unwrap();
    }
    assert_eq!(std::fs::read(&path).unwrap(), b"durable");

    let mut sink = TypedArraySink::create(&path, ArrayKind::U64, 2).unwrap();
    sink.push_u64(1).unwrap();
    assert!(sink.finish().is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"durable");
}

#[test]
fn iterator_progress_reports_actual_payload_bytes_at_bounded_intervals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("progress.u64");
    let count = 100_000u64;
    let mut observed = Vec::new();

    write_u64_iter_with_progress(&path, ArrayKind::U64, count, 0..count, |bytes| {
        observed.push(bytes)
    })
    .unwrap();

    assert!(observed.len() > 1);
    assert!(observed.windows(2).all(|values| values[0] < values[1]));
    assert_eq!(observed.last().copied(), Some(count * 8));
}
