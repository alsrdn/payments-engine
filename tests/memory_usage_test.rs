use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use payments_engine::transactions_cache::{SqliteKvStore, TransactionCache};

struct CountingAllocator;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = unsafe { System.alloc(layout) };
        if !ret.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::SeqCst);
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        ALLOCATED.fetch_sub(layout.size(), Ordering::SeqCst);
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[ignore = "used for debugging"]
#[test]
fn test_cache_memory_usage() {
    let mut cache = TransactionCache::<SqliteKvStore, u32, u32, 65536>::new().unwrap();

    let usage_before = ALLOCATED.load(Ordering::SeqCst);
    for i in 0..524288 {
        cache.put(i, i as u32).unwrap();
    }
    let usage_after_cache_full = ALLOCATED.load(Ordering::SeqCst);
    println!("Allocated: {} bytes", usage_after_cache_full - usage_before);

    for i in 524288..2097152 {
        cache.put(i, i as u32).unwrap();
    }

    let usage_after = ALLOCATED.load(Ordering::SeqCst);
    println!("Allocated: {} bytes", usage_after - usage_after_cache_full);
}
