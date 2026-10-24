//! Process-wide coarse clock for hot paths that need a timestamp but
//! not clock precision.
//!
//! Profiling sysbench oltp_point_select showed `clock_gettime` /
//! `gettimeofday` (via `Instant::now` and `SystemTime::now`) at ~3.5%
//! of server CPU, a large slice of it from per-command bookkeeping
//! whose consumers have second-level resolution anyway:
//! `SHOW PROCESSLIST`'s TIME column (`mark_busy` / `mark_idle`) and the
//! MDL ticket grant timestamp. Those read this clock instead — one
//! relaxed atomic load — and a background ticker refreshes it every
//! 100 ms, so the reported values are at most one tick stale.
//!
//! Not for measuring durations: statement latency, slow-log
//! thresholds and timeouts keep real `Instant::now()` reads.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TICK: Duration = Duration::from_millis(100);

struct CoarseClock {
    anchor: Instant,
    /// Nanoseconds since `anchor`, refreshed by the ticker thread.
    elapsed_ns: AtomicU64,
    /// Milliseconds since the UNIX epoch, refreshed by the ticker thread.
    unix_ms: AtomicU64,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn clock() -> &'static CoarseClock {
    static CLOCK: OnceLock<CoarseClock> = OnceLock::new();
    CLOCK.get_or_init(|| {
        let anchor = Instant::now();
        // Ticker failure (thread limit) degrades to a frozen clock —
        // still monotone, still valid, just stale; acceptable for the
        // second-resolution consumers.
        let _ = std::thread::Builder::new()
            .name("mydb-coarse-clock".into())
            .spawn(|| loop {
                std::thread::sleep(TICK);
                let c = clock();
                c.elapsed_ns
                    .store(c.anchor.elapsed().as_nanos() as u64, Ordering::Relaxed);
                c.unix_ms.store(now_unix_ms(), Ordering::Relaxed);
            });
        CoarseClock {
            anchor,
            elapsed_ns: AtomicU64::new(0),
            unix_ms: AtomicU64::new(now_unix_ms()),
        }
    })
}

/// Monotone `Instant` at most one tick (~100 ms) behind the real clock.
pub fn coarse_instant() -> Instant {
    let c = clock();
    c.anchor + Duration::from_nanos(c.elapsed_ns.load(Ordering::Relaxed))
}

/// Monotone milliseconds since the clock was first used, at most one tick
/// (~100 ms) behind the real clock. Suitable for cheap rate limiting.
pub fn coarse_elapsed_ms() -> u64 {
    clock().elapsed_ns.load(Ordering::Relaxed) / 1_000_000
}

/// UNIX-epoch milliseconds at most one tick (~100 ms) behind the real
/// clock.
pub fn coarse_unix_ms() -> u64 {
    clock().unix_ms.load(Ordering::Relaxed)
}

/// UNIX-epoch microseconds backed by the coarse millisecond clock. Intended
/// for diagnostic timestamps whose SQL type is `TIMESTAMP(6)` but whose
/// producer does not require microsecond measurement precision.
pub fn coarse_unix_us() -> i64 {
    i64::try_from(coarse_unix_ms().saturating_mul(1_000)).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coarse_instant_is_monotone_and_close() {
        let a = coarse_instant();
        let real = Instant::now();
        assert!(a <= real + Duration::from_millis(1));
        // Within a tick + slack of the real clock.
        assert!(real.duration_since(a) < Duration::from_secs(2));
        let b = coarse_instant();
        assert!(b >= a);
    }

    #[test]
    fn coarse_unix_ms_is_close_to_real() {
        let coarse = coarse_unix_ms();
        let real = now_unix_ms();
        assert!(real >= coarse);
        assert!(real - coarse < 2_000);
    }

    #[test]
    fn coarse_unix_us_uses_the_same_bounded_epoch_clock() {
        let coarse = coarse_unix_us();
        let real = now_unix_ms().saturating_mul(1_000) as i64;
        assert!(real >= coarse);
        assert!(real - coarse < 2_000_000);
        assert_eq!(coarse % 1_000, 0);
    }

    #[test]
    fn ticker_advances_the_clock() {
        let before = coarse_instant();
        std::thread::sleep(Duration::from_millis(250));
        let after = coarse_instant();
        assert!(after > before, "ticker never advanced the coarse clock");
    }
}
