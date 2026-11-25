//! Runtime-only query accounting shared by native vector indexes.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub(super) struct QueryCounters {
    candidates_returned: AtomicU64,
    heap_reranks: AtomicU64,
    exact_fallbacks: AtomicU64,
}

impl QueryCounters {
    #[inline]
    pub(super) fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.candidates_returned.load(Ordering::Relaxed),
            self.heap_reranks.load(Ordering::Relaxed),
            self.exact_fallbacks.load(Ordering::Relaxed),
        )
    }

    #[inline]
    pub(super) fn record(&self, candidates: u64, heap: u64, fallback: bool) {
        self.candidates_returned
            .fetch_add(candidates, Ordering::Relaxed);
        self.heap_reranks.fetch_add(heap, Ordering::Relaxed);
        self.exact_fallbacks
            .fetch_add(u64::from(fallback), Ordering::Relaxed);
    }
}
