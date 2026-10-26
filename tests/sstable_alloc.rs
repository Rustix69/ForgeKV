//! Evidence that malformed SSTables cannot trigger large allocations.
//!
//! This test binary installs a counting global allocator that records the
//! largest single allocation. The `unsafe` is confined to this test file and
//! only forwards to the system allocator; the library itself forbids unsafe.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use forgekv::sstable::SstReader;
use tempfile::TempDir;

struct Counting;
static MAX_ALLOC: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every method forwards unchanged to `System`, which upholds the
// `GlobalAlloc` contract; we only read `layout.size()` to record a maximum.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        MAX_ALLOC.fetch_max(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        MAX_ALLOC.fetch_max(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        MAX_ALLOC.fetch_max(new_size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Single test function so no other test thread allocates concurrently.
#[test]
fn malformed_tables_never_allocate_more_than_their_own_size() {
    const LIMIT: usize = 64 * 1024; // every crafted file is < 1 KiB
    let dir = TempDir::new().unwrap();
    let cases = common::malicious_cases();
    assert!(cases.len() > 40);
    for case in cases {
        assert!(case.bytes.len() < 1024, "{}", case.name);
        let path = dir.path().join("t.sst");
        std::fs::write(&path, &case.bytes).unwrap();

        MAX_ALLOC.store(0, Ordering::Relaxed);
        let result = SstReader::open(&path).and_then(|r| r.verify());
        let biggest = MAX_ALLOC.load(Ordering::Relaxed);

        assert!(result.is_err(), "{}: accepted a malformed table", case.name);
        assert!(
            biggest <= LIMIT,
            "{}: allocated {biggest} bytes for a {}-byte file",
            case.name,
            case.bytes.len()
        );
    }
}
