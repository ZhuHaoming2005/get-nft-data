use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use metadata_engine::encode::PayloadCasWriter;

struct LargestAllocationTracker;
static LARGEST_ALLOCATION: AtomicUsize = AtomicUsize::new(0);
static ALLOCATION_TEST_LOCK: Mutex<()> = Mutex::new(());

unsafe impl GlobalAlloc for LargestAllocationTracker {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LARGEST_ALLOCATION.fetch_max(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LARGEST_ALLOCATION.fetch_max(new_size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: LargestAllocationTracker = LargestAllocationTracker;

#[test]
fn duplicate_payload_verification_uses_bounded_scratch() {
    let _guard = ALLOCATION_TEST_LOCK.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut writer = PayloadCasWriter::create(directory.path(), 8 * 1024 * 1024).unwrap();
    let payload = vec![b'x'; 4 * 1024 * 1024];
    assert_eq!(writer.insert(&payload).unwrap(), 0);
    LARGEST_ALLOCATION.store(0, Ordering::Relaxed);

    assert_eq!(writer.insert(&payload).unwrap(), 0);

    let largest = LARGEST_ALLOCATION.load(Ordering::Relaxed);
    assert!(
        largest <= 1024 * 1024,
        "duplicate verification allocated {largest} bytes"
    );
}

#[test]
fn finishing_cas_streams_index_columns_without_a_payload_count_sized_buffer() {
    let _guard = ALLOCATION_TEST_LOCK.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut writer = PayloadCasWriter::create(directory.path(), 8 * 1024 * 1024).unwrap();
    for value in 0..50_000u64 {
        writer.insert(&value.to_le_bytes()).unwrap();
    }
    LARGEST_ALLOCATION.store(0, Ordering::Relaxed);

    let index = writer.finish().unwrap();

    assert_eq!(index.payload_count(), 50_000);
    let largest = LARGEST_ALLOCATION.load(Ordering::Relaxed);
    assert!(
        largest <= 1024 * 1024,
        "CAS finish allocated a payload-count-sized buffer of {largest} bytes"
    );
}
