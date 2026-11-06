//! The global allocator of every build that links latte-core: mimalloc.
//!
//! Debug builds also count the allocations of the current thread, so that tests can
//! prove that the per-op path allocates nothing. Release builds only forward to mimalloc.

use std::alloc::{GlobalAlloc, Layout};

use mimalloc::MiMalloc;

/// mimalloc, plus a per-thread allocation counter in debug builds.
pub struct Allocator;

#[cfg(debug_assertions)]
mod counter {
    use std::cell::Cell;

    /// Allocation sizes are counted in buckets of 16 bytes up to `SIZE_LIMIT`; larger
    /// sizes share the last bucket.
    pub(super) const SIZE_LIMIT: usize = 4096;
    const BUCKETS: usize = SIZE_LIMIT / 16 + 1;

    thread_local! {
        static COUNTING: Cell<bool> = const { Cell::new(false) };
        static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
        static SIZES: [Cell<u64>; BUCKETS] = const { [const { Cell::new(0) }; BUCKETS] };
    }

    #[inline]
    pub(super) fn note(size: usize) {
        let _ = COUNTING.try_with(|c| {
            if c.get() {
                let _ = ALLOCATIONS.try_with(|a| a.set(a.get() + 1));
                let _ = SIZES.try_with(|s| {
                    let b = &s[size.div_ceil(16).min(BUCKETS - 1)];
                    b.set(b.get() + 1);
                });
            }
        });
    }

    /// Counts allocations of `f` by size: (upper bound of the size bucket in bytes, count).
    pub(super) fn count_by_size(f: impl FnOnce()) -> Vec<(usize, u64)> {
        SIZES.with(|s| s.iter().for_each(|b| b.set(0)));
        COUNTING.with(|c| c.set(true));
        f();
        COUNTING.with(|c| c.set(false));
        SIZES.with(|s| {
            s.iter()
                .enumerate()
                .filter(|(_, b)| b.get() > 0)
                .map(|(i, b)| (i * 16, b.get()))
                .collect()
        })
    }

    pub(super) fn count(f: impl FnOnce()) -> u64 {
        ALLOCATIONS.with(|a| a.set(0));
        COUNTING.with(|c| c.set(true));
        f();
        COUNTING.with(|c| c.set(false));
        ALLOCATIONS.with(Cell::get)
    }
}

#[inline(always)]
fn note(_size: usize) {
    #[cfg(debug_assertions)]
    counter::note(_size);
}

/// Runs `f` and returns its allocations on this thread by size, as pairs of the upper bound
/// of a 16-byte size bucket and a count. Sizes above 4096 bytes share the 4096 bucket.
/// Only debug builds count. Release builds return an empty list.
pub fn count_allocations_by_size(f: impl FnOnce()) -> Vec<(usize, u64)> {
    #[cfg(debug_assertions)]
    {
        counter::count_by_size(f)
    }
    #[cfg(not(debug_assertions))]
    {
        f();
        Vec::new()
    }
}

/// Runs `f` and returns how many allocations it made on this thread.
/// Only debug builds count. Release builds return 0.
pub fn count_allocations(f: impl FnOnce()) -> u64 {
    #[cfg(debug_assertions)]
    {
        counter::count(f)
    }
    #[cfg(not(debug_assertions))]
    {
        f();
        0
    }
}

unsafe impl GlobalAlloc for Allocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { MiMalloc.alloc(layout) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { MiMalloc.alloc_zeroed(layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        unsafe { MiMalloc.realloc(ptr, layout, new_size) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { MiMalloc.dealloc(ptr, layout) }
    }
}
