//! Background Auto-ANALYZE.
//!
//! Historically a query that planned against stale statistics paid for a
//! synchronous re-sample on the hot path: the first `EXPLAIN` / plan build
//! after a bulk load blocked on a full-table scan. This module moves that
//! work off the query path. A dedicated worker thread wakes once per
//! interval and asks the [`Database`](crate::storage::core::Database) to
//! re-sample every base table whose accumulated modification delta has
//! crossed a configurable ratio of its row count.
//!
//! Configuration is read from the environment once (lower-bound clamped)
//! and is afterwards runtime-tunable via the `mydb_auto_analyze_*` system
//! variables, which read/write the atomics here.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::background::BackgroundWorker;
use crate::storage::Database;

/// Lower bound on the poll interval; a 0s interval would spin.
const MIN_INTERVAL_S: u64 = 1;

struct Config {
    /// Poll interval in seconds (>= [`MIN_INTERVAL_S`]).
    interval_s: AtomicU64,
    /// Stale ratio threshold, stored as `f64::to_bits` (must be finite > 0).
    ratio_bits: AtomicU64,
    /// Master disable switch.
    disabled: AtomicBool,
    /// Observability: last successful run wall-clock (unix ms), 0 = never.
    last_run_unix_ms: AtomicU64,
    /// Observability: total worker iterations that completed.
    total_runs: AtomicU64,
    /// Observability: cumulative count of tables re-sampled.
    total_tables_analyzed: AtomicU64,
}

fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let interval_s = std::env::var("MYDB_AUTO_ANALYZE_INTERVAL_S")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(|v| v.max(MIN_INTERVAL_S))
            .unwrap_or(60);
        let ratio = std::env::var("MYDB_AUTO_ANALYZE_RATIO")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(0.1);
        let disabled = std::env::var("MYDB_AUTO_ANALYZE_DISABLE")
            .map(|v| {
                matches!(
                    v.trim(),
                    "1" | "true" | "TRUE" | "True" | "on" | "ON" | "On" | "yes" | "YES"
                )
            })
            .unwrap_or(false);
        Config {
            interval_s: AtomicU64::new(interval_s),
            ratio_bits: AtomicU64::new(ratio.to_bits()),
            disabled: AtomicBool::new(disabled),
            last_run_unix_ms: AtomicU64::new(0),
            total_runs: AtomicU64::new(0),
            total_tables_analyzed: AtomicU64::new(0),
        }
    })
}

/// Current poll interval in seconds (always `>= MIN_INTERVAL_S`).
pub fn interval_s() -> u64 {
    config()
        .interval_s
        .load(Ordering::Relaxed)
        .max(MIN_INTERVAL_S)
}

/// Stale ratio threshold (finite, > 0).
pub fn ratio() -> f64 {
    f64::from_bits(config().ratio_bits.load(Ordering::Relaxed))
}

/// Whether the background Auto-ANALYZE worker is disabled.
pub fn is_disabled() -> bool {
    config().disabled.load(Ordering::Relaxed)
}

/// Set the poll interval (clamped to `>= MIN_INTERVAL_S`).
pub fn set_interval_s(value: u64) {
    config()
        .interval_s
        .store(value.max(MIN_INTERVAL_S), Ordering::Relaxed);
}

/// Set the stale ratio. Non-finite / non-positive values are ignored
/// (keeping the previous value) so a bad `SET GLOBAL` can't disable the
/// trigger by stealth.
pub fn set_ratio(value: f64) -> bool {
    if value.is_finite() && value > 0.0 {
        config()
            .ratio_bits
            .store(value.to_bits(), Ordering::Relaxed);
        true
    } else {
        false
    }
}

/// Enable / disable the worker at runtime.
pub fn set_disabled(value: bool) {
    config().disabled.store(value, Ordering::Relaxed);
}

/// Snapshot of observable Auto-ANALYZE state, for `SHOW ENGINE MYDB
/// STATUS`.
#[derive(Debug, Clone, Copy)]
pub struct AutoAnalyzeStats {
    pub enabled: bool,
    pub interval_s: u64,
    pub ratio: f64,
    pub last_run_unix_ms: u64,
    pub total_runs: u64,
    pub total_tables_analyzed: u64,
}

/// Snapshot the current Auto-ANALYZE observability counters + config.
pub fn stats_snapshot() -> AutoAnalyzeStats {
    let cfg = config();
    AutoAnalyzeStats {
        enabled: !cfg.disabled.load(Ordering::Relaxed),
        interval_s: cfg.interval_s.load(Ordering::Relaxed).max(MIN_INTERVAL_S),
        ratio: f64::from_bits(cfg.ratio_bits.load(Ordering::Relaxed)),
        last_run_unix_ms: cfg.last_run_unix_ms.load(Ordering::Relaxed),
        total_runs: cfg.total_runs.load(Ordering::Relaxed),
        total_tables_analyzed: cfg.total_tables_analyzed.load(Ordering::Relaxed),
    }
}

fn record_run(tables_analyzed: u64) {
    let cfg = config();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    cfg.last_run_unix_ms.store(now_ms, Ordering::Relaxed);
    cfg.total_runs.fetch_add(1, Ordering::Relaxed);
    cfg.total_tables_analyzed
        .fetch_add(tables_analyzed, Ordering::Relaxed);
}

/// Background Auto-ANALYZE worker. Mirrors the shutdown / wake / join
/// lifecycle of the other engine background handles (checkpointer,
/// undo-purge): a shutdown flag plus a condvar so `Drop` wakes the
/// thread immediately rather than waiting out a full interval.
pub struct AutoAnalyzeHandle {
    worker: BackgroundWorker,
}

impl AutoAnalyzeHandle {
    pub(crate) fn disabled() -> Self {
        Self {
            worker: BackgroundWorker::disabled(),
        }
    }

    pub fn spawn(db: Arc<Database>) -> Self {
        if db.is_snapshot_read_only() {
            return Self::disabled();
        }
        let worker = BackgroundWorker::spawn("mydb-auto-analyze", move |worker_shutdown| {
            while !worker_shutdown.is_requested() {
                let wait_ms = interval_s().saturating_mul(1000).max(1000);
                if worker_shutdown.wait_timeout(Duration::from_millis(wait_ms)) {
                    break;
                }
                if is_disabled() {
                    continue;
                }
                if db.physical_replica_mode() {
                    // Statistics are authoritative DD state and arrive from the
                    // source WAL. Re-analyzing locally would make the replica's
                    // catalog diverge even though user SQL is read-only.
                    continue;
                }
                match db.auto_analyze_stale_tables(ratio()) {
                    Ok(n) => record_run(n),
                    Err(err) => {
                        crate::util::logging::log_message(format_args!(
                            "background auto-analyze failed: {} ({})",
                            err.message(),
                            err.sql_state()
                        ));
                    }
                }
            }
        });
        Self { worker }
    }

    pub fn shutdown(&self) {
        self.worker.shutdown();
    }
}
