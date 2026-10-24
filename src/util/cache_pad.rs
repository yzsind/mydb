//! Cache-line padding helpers.
//!
//! Apple Silicon (M-series) prefetches in 128 B chunks; x86_64 in 64 B.
//! We pick 128 B as the universal alignment — wasting one extra cache
//! line per padded atomic on x86 is cheap (BufferPool has a handful of
//! hot atomics, the absolute cost is single-digit KB per pool), while
//! undersizing on ARM would leave false sharing on the table.
//!
//! Use sparingly: only on atomics or small structures that are written
//! cross-core at very high frequency (BufferPool::hits/misses,
//! HeapFile::page_count, SlotStatusShard, etc.).

pub const CACHE_LINE: usize = 128;

/// 128-byte-aligned wrapper. Same Deref/DerefMut as [`crossbeam_utils::CachePadded`]
/// so existing call sites (`self.hits.fetch_add(...)`) keep working.
#[repr(align(128))]
#[derive(Debug, Default)]
pub struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    #[cfg(test)]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: Clone> Clone for CachePadded<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn alignment_is_128() {
        let p: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
        let addr = &p as *const _ as usize;
        assert_eq!(addr % CACHE_LINE, 0, "CachePadded not 128-byte aligned");
        assert_eq!(std::mem::align_of::<CachePadded<AtomicU64>>(), CACHE_LINE);
        assert_eq!(std::mem::size_of::<CachePadded<AtomicU64>>(), CACHE_LINE);
    }

    #[test]
    fn deref_passes_through() {
        let p: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
        p.fetch_add(7, Ordering::Relaxed);
        assert_eq!(p.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn array_elements_are_isolated() {
        let arr: [CachePadded<AtomicU64>; 4] =
            std::array::from_fn(|_| CachePadded::new(AtomicU64::new(0)));
        let a0 = &arr[0] as *const _ as usize;
        let a1 = &arr[1] as *const _ as usize;
        assert!(a1 - a0 >= CACHE_LINE);
    }
}
