//! Monotonic process-wide counters written on every statement.
//!
//! A single `AtomicU64` bumped by every connection thread keeps one cache
//! line bouncing between all cores. These counters are only ever summed by
//! status/introspection readers, so each thread adds to its own padded
//! stripe and readers fold the stripes.

use super::cache_pad::CachePadded;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const STRIPES: usize = 32;

#[derive(Debug)]
pub(crate) struct StripedCounter {
    stripes: [CachePadded<AtomicU64>; STRIPES],
}

impl StripedCounter {
    pub(crate) const fn new() -> Self {
        Self {
            stripes: [const { CachePadded::new(AtomicU64::new(0)) }; STRIPES],
        }
    }

    #[inline]
    pub(crate) fn add(&self, n: u64) {
        self.stripes[stripe()].fetch_add(n, Ordering::Relaxed);
    }

    /// Sum of all stripes. Concurrent additions may or may not be included,
    /// exactly as with a relaxed load of a single counter.
    pub(crate) fn sum(&self) -> u64 {
        self.stripes
            .iter()
            .map(|stripe| stripe.load(Ordering::Relaxed))
            .fold(0, u64::wrapping_add)
    }
}

#[inline]
fn stripe() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static STRIPE: usize = NEXT.fetch_add(1, Ordering::Relaxed) % STRIPES;
    }
    STRIPE.with(|stripe| *stripe)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn striped_counter_sums_additions_from_many_threads() {
        static COUNTER: StripedCounter = StripedCounter::new();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        COUNTER.add(2);
                    }
                });
            }
        });
        assert_eq!(COUNTER.sum(), 16_000);
    }
}
