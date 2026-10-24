//! Statement-digest collector backing
//! `performance_schema.events_statements_summary_by_digest`.
//!
//! MySQL's digest table aggregates one row per `(SCHEMA_NAME, DIGEST)` pair,
//! where the DIGEST is a hash over the *normalized* statement text (literals
//! folded to `?`). It is the single most-used performance_schema table for
//! "which query shape is hot / slow", so mydb collects the columns it can
//! source honestly from the statement-execution path:
//!
//! * `COUNT_STAR`, `SUM/MIN/AVG/MAX_TIMER_WAIT` — from each statement's wall
//!   clock (nanoseconds → picoseconds, MySQL's timer unit).
//! * `SUM_LOCK_TIME` — from the per-statement row-lock wait accounting that
//!   already feeds the slow-SQL log (milliseconds → picoseconds).
//! * `SUM_ERRORS`, `SUM_WARNINGS`, `SUM_ROWS_AFFECTED`, `SUM_ROWS_SENT` — from
//!   the statement result / diagnostics, along with `SUM_ROWS_EXAMINED`.
//! * Latency histograms and quantiles from the recorded timers.
//! * `FIRST_SEEN` / `LAST_SEEN` and a longest-seen `QUERY_SAMPLE_*`.
//!
//! Counters mydb does not instrument (the `SUM_CREATED_TMP_*` / `SUM_SELECT_*`
//! / `SUM_SORT_*` / `NO_INDEX_USED` family, CPU time and memory high-water
//! marks) are reported as `0` rather
//! than fabricated — honest "not measured", consistent with CLAUDE.md.
//!
//! Collection follows the `statements_digest` consumer (on by default).
//! Identity admission is bounded globally, while writers accumulate in
//! independent stripes even for the same digest. Readers merge every stripe;
//! no sampling or delayed flush is involved. State is process-global with
//! the same lifetime as MySQL's
//! in-memory performance_schema data — it resets on restart and on
//! `TRUNCATE TABLE performance_schema.events_statements_summary_by_digest`.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::TimeZone;

use parking_lot::{Mutex, RwLock};

use super::perf_statement_stats::{StatementMetrics, StatementSample};
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::Value;
use crate::util::text::char_prefix;

/// Maximum number of distinct `(schema, digest)` rows retained, mirroring
/// MySQL 8.0.46 `performance_schema_digests_size` default. Once the
/// table is full, further distinct digests are folded into a single catch-all
/// row whose `SCHEMA_NAME` / `DIGEST` / `DIGEST_TEXT` are `NULL` (MySQL's
/// behaviour for the "digests lost" overflow row).
const DEFAULT_MAX_DIGESTS: usize = 10_000;

/// Cap on the stored normalized digest text and query sample, matching MySQL's
/// `performance_schema_max_digest_length` / `…max_sql_text_length` defaults so
/// a pathological statement cannot grow the table without bound.
const MAX_TEXT_LEN: usize = 1024;
const MAX_SAMPLE_BYTES: usize = MAX_TEXT_LEN * 4;
const HISTOGRAM_BUCKETS: usize = 450;
const WRITER_STRIPES: usize = 64;
const DIRECTORY_SHARDS: usize = 64;
const WRITER_CACHE_SIZE: usize = 8;

/// One aggregated digest bucket. All timer fields are picoseconds (MySQL's
/// `*_TIMER_WAIT` unit); `min_timer_wait` starts at `u64::MAX` so the first
/// sample sets it.
#[derive(Clone)]
struct DigestStat {
    digest_text: Option<String>,
    metrics: StatementMetrics,
    /// Local-epoch microseconds; formatted to `timestamp(6)` text only
    /// when the table is read (`rows()`), so the per-statement hot path
    /// never formats or allocates timestamps.
    first_seen: i64,
    last_seen: i64,
    query_sample_text: Option<String>,
    query_sample_seen: i64,
    query_sample_timer_wait: u64,
    histogram: [u64; HISTOGRAM_BUCKETS],
}

impl DigestStat {
    fn new(digest_text: Option<String>, now: i64) -> Self {
        Self {
            digest_text,
            metrics: StatementMetrics::default(),
            first_seen: now,
            last_seen: now,
            query_sample_text: None,
            query_sample_seen: now,
            query_sample_timer_wait: 0,
            histogram: [0; HISTOGRAM_BUCKETS],
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn accumulate(
        &mut self,
        timer_ps: u64,
        lock_ps: u64,
        is_error: bool,
        warnings: u64,
        rows_affected: u64,
        rows_sent: u64,
        rows_examined: u64,
        sample: &str,
        now: i64,
    ) {
        if self.metrics.count_star == 0 {
            self.first_seen = now;
            self.last_seen = now;
        }
        self.metrics.record(StatementSample {
            timer_wait: timer_ps,
            lock_time: lock_ps,
            errors: u64::from(is_error),
            warnings,
            rows_affected,
            rows_sent,
            rows_examined,
        });
        self.first_seen = self.first_seen.min(now);
        self.last_seen = self.last_seen.max(now);
        let bucket = histogram_bucket(timer_ps);
        self.histogram[bucket] = self.histogram[bucket].saturating_add(1);
        // MySQL keeps the slowest occurrence as the representative sample.
        // Equal timers keep the earliest sample timestamp. Repeated samples
        // at the same timestamp do not copy their text again.
        if self.query_sample_text.is_none()
            || timer_ps > self.query_sample_timer_wait
            || (timer_ps == self.query_sample_timer_wait && now < self.query_sample_seen)
        {
            self.query_sample_text = Some(char_prefix(sample, MAX_TEXT_LEN).to_owned());
            self.query_sample_seen = now;
            self.query_sample_timer_wait = timer_ps;
        }
    }

    fn merge(&mut self, other: &Self) {
        if other.metrics.count_star == 0 {
            return;
        }
        if self.metrics.count_star == 0 {
            *self = other.clone();
            return;
        }
        self.metrics.merge(&other.metrics);
        self.first_seen = self.first_seen.min(other.first_seen);
        self.last_seen = self.last_seen.max(other.last_seen);
        if other.query_sample_timer_wait > self.query_sample_timer_wait
            || (other.query_sample_timer_wait == self.query_sample_timer_wait
                && other.query_sample_seen < self.query_sample_seen)
        {
            self.query_sample_text = other.query_sample_text.clone();
            self.query_sample_seen = other.query_sample_seen;
            self.query_sample_timer_wait = other.query_sample_timer_wait;
        }
        for (target, source) in self.histogram.iter_mut().zip(other.histogram) {
            *target = target.saturating_add(source);
        }
    }
}

/// A registered identity owns one prepaid stripe. Additional writer stripes
/// are optional allocations: on admission failure writers use stripe zero,
/// preserving every observation without exceeding the unified memory budget.
struct DigestEntry {
    schema: Box<str>,
    hash: Option<Arc<str>>,
    digest_text: Option<String>,
    generation: u64,
    stripes: [WriterStripe; WRITER_STRIPES],
    active: AtomicU64,
    _memory_charge: ControlledMemoryCharge,
}

struct DigestStripe {
    stat: DigestStat,
    histogram_epoch: u64,
    _memory_charge: Option<ControlledMemoryCharge>,
}

// Independent writers must not bounce the cache line containing a neighbour's
// mutex. The counters themselves live in separately allocated stripe boxes.
#[repr(align(64))]
struct WriterStripe(Mutex<Option<Box<DigestStripe>>>);

impl DigestEntry {
    fn try_new(
        schema: &str,
        hash: Option<&str>,
        text: Option<&str>,
        generation: u64,
        histogram_epoch: u64,
    ) -> Option<Arc<Self>> {
        // Include the mandatory stripe, maximum sample, identity strings,
        // registry hash slots and Arc allocation in the identity reservation.
        let bytes = std::mem::size_of::<Self>()
            .saturating_add(std::mem::size_of::<DigestStripe>())
            .saturating_add(MAX_SAMPLE_BYTES)
            .saturating_add(schema.len().saturating_mul(2))
            .saturating_add(hash.map_or(0, str::len))
            .saturating_add(text.map_or(0, |text| char_prefix(text, MAX_TEXT_LEN).len()))
            .saturating_add(256);
        let charge = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::PerformanceSchema,
            bytes,
        )?;
        let stripes = std::array::from_fn(|_| WriterStripe(Mutex::new(None)));
        let entry = Arc::new(Self {
            schema: schema.into(),
            hash: hash.map(Arc::from),
            digest_text: text.map(|text| char_prefix(text, MAX_TEXT_LEN).to_owned()),
            generation,
            stripes,
            active: AtomicU64::new(1),
            _memory_charge: charge,
        });
        *entry.stripes[0].0.lock() = Some(Box::new(DigestStripe {
            stat: DigestStat::new(None, 0),
            histogram_epoch,
            _memory_charge: None,
        }));
        Some(entry)
    }

    fn writer_stripe(&self, writer: usize, histogram_epoch: u64) -> usize {
        self.writer_stripe_with(writer, histogram_epoch, |bytes| {
            ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::PerformanceSchema,
                bytes,
            )
        })
    }

    fn writer_stripe_with(
        &self,
        writer: usize,
        histogram_epoch: u64,
        reserve: impl FnOnce(usize) -> Option<ControlledMemoryCharge>,
    ) -> usize {
        let index = writer % WRITER_STRIPES;
        let mut slot = self.stripes[index].0.lock();
        if slot.is_none() {
            let bytes = std::mem::size_of::<DigestStripe>().saturating_add(MAX_SAMPLE_BYTES);
            let Some(charge) = reserve(bytes) else {
                return 0;
            };
            *slot = Some(Box::new(DigestStripe {
                stat: DigestStat::new(None, 0),
                histogram_epoch,
                _memory_charge: Some(charge),
            }));
            self.active.fetch_or(1 << index, Ordering::Release);
        }
        index
    }

    fn record(
        &self,
        collector: &DigestCollector,
        stripe: usize,
        sample: &DigestObservation<'_>,
    ) -> bool {
        let mut slot = self.stripes[stripe].0.lock();
        // A cached identity can outlive TRUNCATE. Check under its writer
        // lock so a request starting after reset cannot revive old counters.
        if self.generation != collector.generation.load(Ordering::Acquire) {
            return false;
        }
        let stripe = slot.as_mut().expect("writer stripe was admitted");
        let epoch = collector.histogram_epoch.load(Ordering::Acquire);
        if stripe.histogram_epoch != epoch {
            stripe.stat.histogram.fill(0);
            stripe.histogram_epoch = epoch;
        }
        stripe.stat.accumulate(
            sample.timer_ps,
            sample.lock_ps,
            sample.is_error,
            sample.warnings,
            sample.rows_affected,
            sample.rows_sent,
            sample.rows_examined,
            sample.sql,
            sample.now,
        );
        true
    }

    fn snapshot(&self, histogram_epoch: u64) -> DigestStat {
        let mut stat = DigestStat::new(self.digest_text.clone(), 0);
        let mut active = self.active.load(Ordering::Acquire);
        while active != 0 {
            let index = active.trailing_zeros() as usize;
            active &= active - 1;
            let slot = self.stripes[index].0.lock();
            if let Some(stripe) = slot.as_ref() {
                let mut part = stripe.stat.clone();
                if stripe.histogram_epoch != histogram_epoch {
                    part.histogram.fill(0);
                }
                stat.merge(&part);
            }
        }
        stat.digest_text = self.digest_text.clone();
        stat
    }

    fn count(&self) -> u64 {
        let mut count = 0u64;
        let mut active = self.active.load(Ordering::Acquire);
        while active != 0 {
            let index = active.trailing_zeros() as usize;
            active &= active - 1;
            let slot = self.stripes[index].0.lock();
            if let Some(stripe) = slot.as_ref() {
                count = count.saturating_add(stripe.stat.metrics.count_star);
            }
        }
        count
    }
}

struct DigestObservation<'a> {
    sql: &'a str,
    timer_ps: u64,
    lock_ps: u64,
    is_error: bool,
    warnings: u64,
    rows_affected: u64,
    rows_sent: u64,
    rows_examined: u64,
    now: i64,
}

type DigestMap = std::collections::HashMap<
    Box<str>,
    std::collections::HashMap<Arc<str>, Arc<DigestEntry>, ahash::RandomState>,
    ahash::RandomState,
>;

#[derive(Default)]
struct DigestRegistry {
    total: usize,
    overflow: Option<Arc<DigestEntry>>,
    rejected: u64,
}

/// Only identity misses touch the directory. A bounded thread cache borrows
/// its Arc on hits, avoiding both a shared directory lock and a shared Arc
/// reference-count update on every completed statement.
struct CachedDigest {
    entry: Arc<DigestEntry>,
    stripe: usize,
    overflow_key: Option<CachedOverflowKey>,
}

struct CachedOverflowKey {
    schema: Box<str>,
    hash: Box<str>,
    _memory_charge: ControlledMemoryCharge,
}

struct DigestWriterCache {
    writer: usize,
    generation: u64,
    entries: Vec<CachedDigest>,
    next: usize,
    _memory_charge: Option<ControlledMemoryCharge>,
}

impl DigestWriterCache {
    fn new(writer: usize) -> Self {
        Self {
            writer,
            generation: 0,
            entries: Vec::new(),
            next: 0,
            _memory_charge: None,
        }
    }

    fn insert(&mut self, entry: Arc<DigestEntry>, stripe: usize, schema: &str, hash: &str) {
        if self._memory_charge.is_none() {
            let Some(charge) = ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::PerformanceSchema,
                WRITER_CACHE_SIZE * std::mem::size_of::<CachedDigest>(),
            ) else {
                return;
            };
            self.entries = Vec::with_capacity(WRITER_CACHE_SIZE);
            self._memory_charge = Some(charge);
        }
        // Registered identities already own their lookup strings. Only the
        // catch-all row needs the original key to cache a specific miss.
        let overflow_key = if entry.hash.is_none() {
            let Some(charge) = ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::PerformanceSchema,
                schema.len().saturating_add(hash.len()),
            ) else {
                return;
            };
            Some(CachedOverflowKey {
                schema: schema.into(),
                hash: hash.into(),
                _memory_charge: charge,
            })
        } else {
            None
        };
        let cached = CachedDigest {
            entry,
            stripe,
            overflow_key,
        };
        if self.entries.len() < WRITER_CACHE_SIZE {
            self.entries.push(cached);
        } else {
            self.entries[self.next] = cached;
            self.next = (self.next + 1) % WRITER_CACHE_SIZE;
        }
    }
}

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_WRITER: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static WRITER_CACHE: RefCell<DigestWriterCache> = RefCell::new(
        DigestWriterCache::new(NEXT_WRITER.fetch_add(1, Ordering::Relaxed))
    );
}

struct DigestCollector {
    generation: AtomicU64,
    histogram_epoch: AtomicU64,
    directory: [RwLock<DigestMap>; DIRECTORY_SHARDS],
    registry: Mutex<DigestRegistry>,
    max_digests: usize,
}

impl DigestCollector {
    fn new(max_digests: usize) -> Self {
        Self {
            generation: AtomicU64::new(NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)),
            histogram_epoch: AtomicU64::new(1),
            directory: std::array::from_fn(|_| RwLock::new(DigestMap::default())),
            registry: Mutex::new(DigestRegistry::default()),
            max_digests,
        }
    }

    fn entry(&self, schema: &str, hash: &str, text: &str) -> Option<Arc<DigestEntry>> {
        // The caller supplies a SHA-256 hex digest. Admission remains global;
        // only directory lookup is sharded, so skew cannot exhaust a fraction
        // of performance_schema_digests_size or serialize unrelated digests.
        let shard = hash
            .get(..8)
            .and_then(|prefix| u32::from_str_radix(prefix, 16).ok())
            .map_or(0, |value| value as usize & (DIRECTORY_SHARDS - 1));
        {
            let directory = self.directory[shard].read();
            if let Some(entry) = directory.get(schema).and_then(|map| map.get(hash)) {
                return Some(Arc::clone(entry));
            }
        }
        let mut directory = self.directory[shard].write();
        if let Some(entry) = directory.get(schema).and_then(|map| map.get(hash)) {
            return Some(Arc::clone(entry));
        }
        let mut registry = self.registry.lock();
        if self.max_digests == 0 {
            registry.rejected = registry.rejected.saturating_add(1);
            return None;
        }
        let overflow = registry.total >= self.max_digests;
        if overflow {
            if let Some(entry) = &registry.overflow {
                return Some(Arc::clone(entry));
            }
        }
        let Some(entry) = DigestEntry::try_new(
            if overflow { "" } else { schema },
            (!overflow).then_some(hash),
            (!overflow).then_some(text),
            self.generation.load(Ordering::Acquire),
            self.histogram_epoch.load(Ordering::Acquire),
        ) else {
            registry.rejected = registry.rejected.saturating_add(1);
            return None;
        };
        if overflow {
            registry.overflow = Some(Arc::clone(&entry));
        } else {
            directory.entry(schema.into()).or_default().insert(
                Arc::clone(entry.hash.as_ref().expect("registered digest")),
                Arc::clone(&entry),
            );
            registry.total += 1;
        }
        Some(entry)
    }

    fn record(
        &self,
        cache: &mut DigestWriterCache,
        schema: &str,
        hash: &str,
        text: &str,
        sample: &DigestObservation<'_>,
    ) {
        loop {
            let generation = self.generation.load(Ordering::Acquire);
            if cache.generation != generation {
                cache.entries.clear();
                cache.next = 0;
                cache.generation = generation;
            }
            if let Some(cached) = cache.entries.iter().rev().find(|cached| {
                if let Some(key) = &cached.overflow_key {
                    key.schema.as_ref() == schema && key.hash.as_ref() == hash
                } else {
                    cached.entry.schema.as_ref() == schema
                        && cached.entry.hash.as_deref() == Some(hash)
                }
            }) {
                if cached.entry.record(self, cached.stripe, sample) {
                    return;
                }
                continue;
            }
            let Some(entry) = self.entry(schema, hash, text) else {
                return;
            };
            let stripe =
                entry.writer_stripe(cache.writer, self.histogram_epoch.load(Ordering::Acquire));
            if !entry.record(self, stripe, sample) {
                continue;
            }
            // If reset raced with this observation, the next call drops
            // this bounded stale handle before looking at its counters.
            cache.insert(entry, stripe, schema, hash);
            return;
        }
    }

    fn reset(&self) {
        // Cold operations lock directories in array order, then admission.
        // Writers hold at most one directory before taking admission; the
        // hit path takes neither, so there is no reverse lock dependency.
        let mut directories: Vec<_> = self.directory.iter().map(|shard| shard.write()).collect();
        let mut registry = self.registry.lock();
        for directory in &mut directories {
            **directory = DigestMap::default();
        }
        *registry = DigestRegistry::default();
        self.generation.store(
            NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            Ordering::Release,
        );
    }

    fn reset_histograms(&self) {
        self.histogram_epoch.fetch_add(1, Ordering::AcqRel);
    }

    fn snapshot(&self) -> Vec<(Option<String>, Option<String>, DigestStat)> {
        let directories: Vec<_> = self.directory.iter().map(|shard| shard.read()).collect();
        let registry = self.registry.lock();
        let histogram_epoch = self.histogram_epoch.load(Ordering::Acquire);
        let mut entries =
            Vec::with_capacity(registry.total + usize::from(registry.overflow.is_some()));
        for entry in directories
            .iter()
            .flat_map(|directory| directory.values())
            .flat_map(|map| map.values())
        {
            entries.push((
                (!entry.schema.is_empty()).then(|| entry.schema.to_string()),
                entry.hash.as_ref().map(ToString::to_string),
                entry.snapshot(histogram_epoch),
            ));
        }
        if let Some(entry) = &registry.overflow {
            entries.push((None, None, entry.snapshot(histogram_epoch)));
        }
        entries
    }

    fn lost(&self) -> u64 {
        let registry = self.registry.lock();
        registry
            .rejected
            .saturating_add(registry.overflow.as_ref().map_or(0, |entry| entry.count()))
    }

    fn row_count(&self) -> usize {
        let registry = self.registry.lock();
        registry
            .total
            .saturating_add(usize::from(registry.overflow.is_some()))
    }
}

fn collector() -> &'static DigestCollector {
    static COLLECTOR: OnceLock<DigestCollector> = OnceLock::new();
    COLLECTOR.get_or_init(|| {
        DigestCollector::new(crate::engine::perf_setup::configured_usize(
            "performance_schema_digests_size",
            DEFAULT_MAX_DIGESTS,
        ))
    })
}

/// Record a completed statement when the digest consumer is enabled. The
/// caller caches normalization and hashing; the collector caches identity
/// admission independently of SQL syntax, parameter types or execution route.
#[allow(clippy::too_many_arguments)]
pub(crate) fn record(
    schema: &str,
    sql: &str,
    digest_text: &str,
    digest_hash: &str,
    elapsed: Duration,
    is_error: bool,
    warnings: u64,
    lock_wait_ms: f64,
    rows_affected: u64,
    rows_sent: u64,
    rows_examined: u64,
) {
    if digest_text.is_empty() {
        return;
    }
    let sample = DigestObservation {
        sql,
        timer_ps: duration_to_ps(elapsed),
        lock_ps: ms_to_ps(lock_wait_ms),
        is_error,
        warnings,
        rows_affected,
        rows_sent,
        rows_examined,
        now: now_us(),
    };
    WRITER_CACHE.with(|cache| {
        collector().record(
            &mut cache.borrow_mut(),
            schema,
            digest_hash,
            digest_text,
            &sample,
        )
    });
}

/// Reset the directory and its lost counter as one generation. Cached writer
/// handles remain charged while retained, but can never revive a reset row.
pub(crate) fn reset() {
    collector().reset();
}

pub(crate) fn lost() -> u64 {
    collector().lost()
}

pub(crate) fn reset_histograms() {
    collector().reset_histograms();
}

/// Produce the 39-column summary after merging all live writer stripes.
pub(crate) fn rows() -> Vec<Vec<Value>> {
    let mut entries = collector().snapshot();
    entries.sort_by(|a, b| {
        b.2.metrics
            .count_star
            .cmp(&a.2.metrics.count_star)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.0.cmp(&b.0))
    });
    entries
        .into_iter()
        .map(|(schema, digest, stat)| digest_row(schema.as_deref(), digest.as_deref(), &stat))
        .collect()
}

pub(crate) fn row_count() -> usize {
    collector().row_count()
}

pub(crate) fn histogram_row_count() -> usize {
    row_count().saturating_mul(HISTOGRAM_BUCKETS)
}

fn digest_row(schema: Option<&str>, digest: Option<&str>, stat: &DigestStat) -> Vec<Value> {
    let avg = if stat.metrics.count_star > 0 {
        stat.metrics.sum_timer_wait / stat.metrics.count_star
    } else {
        0
    };
    let min = if stat.metrics.count_star > 0 && stat.metrics.min_timer_wait != u64::MAX {
        stat.metrics.min_timer_wait
    } else {
        0
    };
    vec![
        opt_str(schema),                                   // SCHEMA_NAME
        opt_str(digest),                                   // DIGEST
        opt_string(stat.digest_text.clone()),              // DIGEST_TEXT
        u(stat.metrics.count_star),                        // COUNT_STAR
        u(stat.metrics.sum_timer_wait),                    // SUM_TIMER_WAIT
        u(min),                                            // MIN_TIMER_WAIT
        u(avg),                                            // AVG_TIMER_WAIT
        u(stat.metrics.max_timer_wait),                    // MAX_TIMER_WAIT
        u(stat.metrics.sum_lock_time),                     // SUM_LOCK_TIME
        u(stat.metrics.sum_errors),                        // SUM_ERRORS
        u(stat.metrics.sum_warnings),                      // SUM_WARNINGS
        u(stat.metrics.sum_rows_affected),                 // SUM_ROWS_AFFECTED
        u(stat.metrics.sum_rows_sent),                     // SUM_ROWS_SENT
        u(stat.metrics.sum_rows_examined),                 // SUM_ROWS_EXAMINED
        u(0),                                              // SUM_CREATED_TMP_DISK_TABLES
        u(0),                                              // SUM_CREATED_TMP_TABLES
        u(0),                                              // SUM_SELECT_FULL_JOIN
        u(0),                                              // SUM_SELECT_FULL_RANGE_JOIN
        u(0),                                              // SUM_SELECT_RANGE
        u(0),                                              // SUM_SELECT_RANGE_CHECK
        u(0),                                              // SUM_SELECT_SCAN
        u(0),                                              // SUM_SORT_MERGE_PASSES
        u(0),                                              // SUM_SORT_RANGE
        u(0),                                              // SUM_SORT_ROWS
        u(0),                                              // SUM_SORT_SCAN
        u(0),                                              // SUM_NO_INDEX_USED
        u(0),                                              // SUM_NO_GOOD_INDEX_USED
        u(0),                                              // SUM_CPU_TIME (not measured)
        u(0),                                              // MAX_CONTROLLED_MEMORY
        u(0),                                              // MAX_TOTAL_MEMORY
        u(0),                                              // COUNT_SECONDARY
        Value::String(format_ts6(stat.first_seen)),        // FIRST_SEEN
        Value::String(format_ts6(stat.last_seen)),         // LAST_SEEN
        u(histogram_quantile(stat, 0.95)),                 // QUANTILE_95
        u(histogram_quantile(stat, 0.99)),                 // QUANTILE_99
        u(histogram_quantile(stat, 0.999)),                // QUANTILE_999
        opt_string(stat.query_sample_text.clone()),        // QUERY_SAMPLE_TEXT
        Value::String(format_ts6(stat.query_sample_seen)), // QUERY_SAMPLE_SEEN
        u(stat.query_sample_timer_wait),                   // QUERY_SAMPLE_TIMER_WAIT
    ]
}

pub(crate) fn histogram_global_rows() -> Vec<Vec<Value>> {
    collector().histogram_global_rows()
}

pub(crate) fn histogram_by_digest_rows() -> Vec<Vec<Value>> {
    collector().histogram_by_digest_rows()
}

impl DigestCollector {
    fn histogram_global_rows(&self) -> Vec<Vec<Value>> {
        let mut buckets = [0u64; HISTOGRAM_BUCKETS];
        for (_, _, stat) in self.snapshot() {
            for (total, count) in buckets.iter_mut().zip(stat.histogram) {
                *total = total.saturating_add(count);
            }
        }
        histogram_tail_rows(None, None, &buckets)
    }

    fn histogram_by_digest_rows(&self) -> Vec<Vec<Value>> {
        self.snapshot()
            .into_iter()
            .flat_map(|(schema, digest, stat)| {
                let mut rows = histogram_tail_rows(None, None, &stat.histogram);
                for row in &mut rows {
                    row.splice(
                        0..0,
                        [opt_str(schema.as_deref()), opt_str(digest.as_deref())],
                    );
                }
                rows
            })
            .collect()
    }
}

fn histogram_tail_rows(
    schema: Option<&str>,
    digest: Option<&str>,
    buckets: &[u64; HISTOGRAM_BUCKETS],
) -> Vec<Vec<Value>> {
    let total = buckets.iter().copied().fold(0u64, u64::saturating_add);
    let mut cumulative = 0u64;
    (0..HISTOGRAM_BUCKETS)
        .map(|bucket| {
            cumulative = cumulative.saturating_add(buckets[bucket]);
            let mut row = Vec::with_capacity(if schema.is_some() { 8 } else { 6 });
            if schema.is_some() {
                row.push(opt_str(schema));
                row.push(opt_str(digest));
            }
            row.extend([
                u(bucket as u64),
                u(histogram_low(bucket)),
                u(histogram_high(bucket)),
                u(buckets[bucket]),
                u(cumulative),
                Value::Double(if total == 0 {
                    0.0
                } else {
                    cumulative as f64 / total as f64
                }),
            ]);
            row
        })
        .collect()
}

fn histogram_bucket(timer_ps: u64) -> usize {
    if timer_ps < 10_000_000 {
        return 0;
    }
    let exponent = ((timer_ps as f64 / 10_000_000.0).log10() * 50.0).floor();
    (exponent as usize + 1).min(HISTOGRAM_BUCKETS - 1)
}

fn histogram_low(bucket: usize) -> u64 {
    if bucket == 0 {
        0
    } else {
        histogram_high(bucket - 1)
    }
}

fn histogram_high(bucket: usize) -> u64 {
    if bucket >= HISTOGRAM_BUCKETS - 1 {
        u64::MAX
    } else {
        (10_000_000.0 * 10f64.powf(bucket as f64 / 50.0)) as u64
    }
}

fn histogram_quantile(stat: &DigestStat, quantile: f64) -> u64 {
    let total = stat
        .histogram
        .iter()
        .copied()
        .fold(0u64, u64::saturating_add);
    if total == 0 {
        return 0;
    }
    let target = (total as f64 * quantile).ceil() as u64;
    let mut cumulative = 0u64;
    for (bucket, count) in stat.histogram.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        if cumulative >= target {
            return histogram_high(bucket);
        }
    }
    u64::MAX
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn u(value: u64) -> Value {
    Value::UInt(value)
}

fn opt_str(value: Option<&str>) -> Value {
    match value {
        Some(s) => Value::String(s.to_string()),
        None => Value::Null,
    }
}

fn opt_string(value: Option<String>) -> Value {
    match value {
        Some(s) => Value::String(s),
        None => Value::Null,
    }
}

/// 64-char lowercase hex SHA-256 of the normalized digest text — the value
/// reported in the `DIGEST` column, matching MySQL's hex digest representation.
/// Nanoseconds → picoseconds (MySQL's `*_TIMER_WAIT` unit), saturating.
fn duration_to_ps(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos().saturating_mul(1_000)).unwrap_or(u64::MAX)
}

/// Milliseconds (as accounted by the lock-wait instrumentation) → picoseconds.
fn ms_to_ps(ms: f64) -> u64 {
    if ms > 0.0 {
        (ms * 1_000_000_000.0) as u64
    } else {
        0
    }
}

/// Epoch microseconds for diagnostic FIRST/LAST_SEEN fields. The process-wide
/// coarse clock is refreshed every 100 ms, which is more precise than these
/// operational timestamps require and avoids a SystemTime syscall on every
/// statement. Duration measurement and slow-query thresholds use `Instant`
/// elsewhere and are deliberately unaffected.
fn now_us() -> i64 {
    crate::util::coarse_clock::coarse_unix_us()
}

/// `timestamp(6)` string in the server's local time, matching mydb's
/// `NOW()` / `CURRENT_TIMESTAMP` convention (see `expr.rs`).
fn format_ts6(us: i64) -> String {
    chrono::Local
        .timestamp_micros(us)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(timer_ps: u64, now: i64) -> DigestObservation<'static> {
        DigestObservation {
            sql: "SELECT 'sample'",
            timer_ps,
            lock_ps: 3,
            is_error: true,
            warnings: 2,
            rows_affected: 4,
            rows_sent: 5,
            rows_examined: 6,
            now,
        }
    }

    fn record_sample(
        collector: &DigestCollector,
        cache: &mut DigestWriterCache,
        sample: &DigestObservation<'_>,
    ) {
        collector.record(cache, "db", "hash", "SELECT ?", sample);
    }

    #[test]
    fn concurrent_writers_match_all_serial_summary_columns_and_histograms() {
        use std::collections::BTreeMap;
        let collector = Arc::new(DigestCollector::new(100));
        let mut serial = BTreeMap::<(String, String), DigestStat>::new();
        for writer in 0..24 {
            for event in 0..120 {
                let schema = ["", "db", "other"][event % 3];
                let hash = format!("{:08x}", event % 5);
                let timer = ((writer * 120 + event) as u64 + 1) * 1_000_000;
                let now = (writer * 120 + event) as i64 + 100;
                let stat = serial
                    .entry((schema.into(), hash))
                    .or_insert_with(|| DigestStat::new(Some("SELECT ?".into()), 0));
                stat.accumulate(timer, 3, true, 2, 4, 5, 6, "SELECT 'sample'", now);
            }
        }
        std::thread::scope(|scope| {
            for writer in 0..24 {
                let collector = &collector;
                scope.spawn(move || {
                    let mut cache = DigestWriterCache::new(writer);
                    for event in 0..120 {
                        let schema = ["", "db", "other"][event % 3];
                        let hash = format!("{:08x}", event % 5);
                        let timer = ((writer * 120 + event) as u64 + 1) * 1_000_000;
                        let now = (writer * 120 + event) as i64 + 100;
                        collector.record(
                            &mut cache,
                            schema,
                            &hash,
                            "SELECT ?",
                            &observation(timer, now),
                        );
                    }
                });
            }
        });
        let rows = collector.snapshot();
        assert_eq!(rows.len(), serial.len());
        for (schema, hash, stat) in rows {
            let key = (schema.clone().unwrap_or_default(), hash.clone().unwrap());
            let expected = &serial[&key];
            assert_eq!(
                digest_row(schema.as_deref(), hash.as_deref(), &stat),
                digest_row(schema.as_deref(), hash.as_deref(), expected)
            );
            assert_eq!(stat.histogram, expected.histogram);
        }
        assert_eq!(collector.lost(), 0);
        assert_eq!(collector.row_count(), 15);
        let by_digest = collector.histogram_by_digest_rows();
        assert_eq!(by_digest.len(), 15 * HISTOGRAM_BUCKETS);
        assert!(by_digest.iter().all(|row| row.len() == 8));
        assert!(by_digest
            .iter()
            .any(|row| row[0] == Value::Null && row[1] != Value::Null));
        let global = collector.histogram_global_rows();
        for bucket in 0..HISTOGRAM_BUCKETS {
            let expected = serial.values().map(|stat| stat.histogram[bucket]).sum();
            assert_eq!(global[bucket][3], u(expected));
        }
    }

    #[test]
    fn capacity_is_global_and_overflow_retains_all_observations() {
        let collector = DigestCollector::new(2);
        let mut first = DigestWriterCache::new(1);
        let mut second = DigestWriterCache::new(2);
        record_sample(&collector, &mut first, &observation(0, 100));
        collector.record(
            &mut first,
            "other",
            "hash",
            "SELECT ?",
            &observation(20, 200),
        );
        for cache in [&mut first, &mut second] {
            record_sample(&collector, cache, &observation(30, 300));
            collector.record(cache, "overflow", "a", "SELECT ?", &observation(40, 400));
            collector.record(cache, "overflow", "b", "SELECT ?", &observation(50, 500));
        }
        assert_eq!(collector.row_count(), 3);
        assert_eq!(collector.lost(), 4);
        let rows = collector.snapshot();
        let overflow = rows.iter().find(|row| row.1.is_none()).unwrap();
        assert_eq!(overflow.0, None);
        assert_eq!(overflow.2.digest_text, None);
        assert_eq!(overflow.2.metrics.count_star, 4);
        assert_eq!(overflow.2.metrics.sum_rows_examined, 24);
        assert_eq!(overflow.2.query_sample_timer_wait, 50);
        let buckets = collector.histogram_by_digest_rows();
        let overflow_buckets: Vec<_> = buckets.iter().filter(|row| row[1] == Value::Null).collect();
        assert_eq!(overflow_buckets.len(), HISTOGRAM_BUCKETS);
        assert!(overflow_buckets
            .iter()
            .all(|row| row.len() == 8 && row[0] == Value::Null));
        assert_eq!(overflow_buckets[0][5], u(4));

        let disabled = DigestCollector::new(0);
        record_sample(&disabled, &mut first, &observation(1, 1));
        record_sample(&disabled, &mut first, &observation(2, 2));
        assert_eq!(disabled.row_count(), 0);
        assert_eq!(disabled.lost(), 2);
        assert!(disabled.snapshot().is_empty());
    }

    #[test]
    fn concurrent_admission_across_directories_keeps_one_global_capacity() {
        use std::sync::Barrier;
        let collector = Arc::new(DigestCollector::new(17));
        let start = Barrier::new(25);
        std::thread::scope(|scope| {
            for writer in 0..24 {
                let collector = &collector;
                let start = &start;
                scope.spawn(move || {
                    let mut cache = DigestWriterCache::new(writer);
                    let hash = format!("{writer:08x}");
                    start.wait();
                    for _ in 0..100 {
                        collector.record(&mut cache, "db", &hash, "SELECT ?", &observation(1, 1));
                    }
                });
            }
            start.wait();
        });
        let rows = collector.snapshot();
        assert_eq!(rows.len(), 18);
        assert_eq!(rows.iter().filter(|row| row.1.is_some()).count(), 17);
        assert_eq!(
            rows.iter().map(|row| row.2.metrics.count_star).sum::<u64>(),
            2400
        );
        assert_eq!(collector.lost(), 700);
    }

    #[test]
    fn histogram_reset_keeps_summary_and_uses_new_histogram_population() {
        let collector = DigestCollector::new(10);
        let mut a = DigestWriterCache::new(1);
        let mut b = DigestWriterCache::new(2);
        record_sample(&collector, &mut a, &observation(1_000_000_000, 100));
        record_sample(&collector, &mut b, &observation(2_000_000_000, 200));
        let before = collector.snapshot().pop().unwrap().2;
        collector.reset_histograms();
        let cleared = collector.snapshot().pop().unwrap().2;
        assert_eq!(cleared.metrics, before.metrics);
        assert_eq!(cleared.query_sample_text, before.query_sample_text);
        assert_eq!(
            cleared.query_sample_timer_wait,
            before.query_sample_timer_wait
        );
        assert_eq!(cleared.histogram, [0; HISTOGRAM_BUCKETS]);
        assert_eq!(histogram_quantile(&cleared, 0.95), 0);
        record_sample(&collector, &mut a, &observation(1, 300));
        let after = collector.snapshot().pop().unwrap().2;
        assert_eq!(after.metrics.count_star, 3);
        assert_eq!(after.histogram.iter().sum::<u64>(), 1);
        assert_eq!(histogram_quantile(&after, 0.95), histogram_high(0));
        assert_eq!(after.query_sample_timer_wait, 2_000_000_000);
    }

    #[test]
    fn truncate_invalidates_cached_writers_and_overflow_after_barrier() {
        use std::sync::Barrier;
        let collector = Arc::new(DigestCollector::new(1));
        let ready = Barrier::new(17);
        let resume = Barrier::new(17);
        std::thread::scope(|scope| {
            for writer in 0..16 {
                let collector = &collector;
                let ready = &ready;
                let resume = &resume;
                scope.spawn(move || {
                    let mut cache = DigestWriterCache::new(writer);
                    record_sample(collector, &mut cache, &observation(1_000, 100));
                    collector.record(
                        &mut cache,
                        "other",
                        "overflow",
                        "SELECT ?",
                        &observation(1_000, 100),
                    );
                    ready.wait();
                    resume.wait();
                    for _ in 0..3 {
                        record_sample(collector, &mut cache, &observation(1, 200));
                    }
                });
            }
            ready.wait();
            assert_eq!(collector.lost(), 16);
            collector.reset();
            assert_eq!(collector.row_count(), 0);
            assert_eq!(collector.lost(), 0);
            assert!(collector.snapshot().is_empty());
            resume.wait();
        });
        let rows = collector.snapshot();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].2.metrics.count_star, 48);
        assert_eq!(rows[0].2.metrics.max_timer_wait, 1);
        assert_eq!((rows[0].2.first_seen, rows[0].2.last_seen), (200, 200));
        assert_eq!(rows[0].2.histogram.iter().sum::<u64>(), 48);
    }

    #[test]
    fn memory_pressure_falls_back_without_losing_accepted_digest_statistics() {
        let collector = DigestCollector::new(1);
        let entry = collector.entry("db", "hash", "SELECT ?").unwrap();
        let stripe = entry.writer_stripe_with(7, 1, |_| None);
        assert_eq!(stripe, 0);
        for _ in 0..100 {
            assert!(entry.record(&collector, stripe, &observation(0, 10)));
        }
        let stat = collector.snapshot().pop().unwrap().2;
        assert_eq!(stat.metrics.count_star, 100);
        assert_eq!(stat.query_sample_text.as_deref(), Some("SELECT 'sample'"));
        assert_eq!(stat.query_sample_timer_wait, 0);
        assert_eq!(stat.histogram[0], 100);
        assert_eq!(collector.lost(), 0);
        assert!(entry.stripes[7].0.lock().is_none());
    }

    #[test]
    fn zero_timer_merge_multibyte_samples_and_saturation_are_preserved() {
        let collector = DigestCollector::new(1);
        let mut a = DigestWriterCache::new(1);
        let mut b = DigestWriterCache::new(2);
        let text = "😀".repeat(MAX_TEXT_LEN + 1);
        let mut sample = observation(0, 200);
        sample.sql = &text;
        record_sample(&collector, &mut a, &sample);
        sample.now = 100;
        record_sample(&collector, &mut b, &sample);
        let stat = collector.snapshot().pop().unwrap().2;
        assert_eq!(stat.query_sample_seen, 100);
        assert_eq!(stat.query_sample_timer_wait, 0);
        assert_eq!(
            stat.query_sample_text.as_ref().unwrap().len(),
            MAX_SAMPLE_BYTES
        );
        assert_eq!(stat.histogram[0], 2);

        let mut left = stat.clone();
        left.metrics.count_star = u64::MAX;
        left.metrics.sum_timer_wait = u64::MAX;
        left.histogram[0] = u64::MAX;
        left.merge(&stat);
        assert_eq!(left.metrics.count_star, u64::MAX);
        assert_eq!(left.metrics.sum_timer_wait, u64::MAX);
        assert_eq!(left.histogram[0], u64::MAX);
    }

    #[test]
    fn bounded_cache_eviction_thread_exit_and_reset_release_identity_ownership() {
        let collector = DigestCollector::new(20);
        let mut cache = DigestWriterCache::new(1);
        for n in 0..20 {
            collector.record(
                &mut cache,
                "db",
                &format!("hash-{n}"),
                "SELECT ?",
                &observation(0, n),
            );
            assert!(cache.entries.len() <= WRITER_CACHE_SIZE);
        }
        assert_eq!(
            collector
                .snapshot()
                .iter()
                .map(|row| row.2.metrics.count_star)
                .sum::<u64>(),
            20
        );
        let weak = Arc::downgrade(&cache.entries.last().unwrap().entry);
        collector.reset();
        assert!(weak.upgrade().is_some());
        drop(cache);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn live_snapshots_keep_metrics_and_histograms_consistent_during_writes() {
        use std::sync::Barrier;
        let collector = Arc::new(DigestCollector::new(1));
        let start = Barrier::new(9);
        std::thread::scope(|scope| {
            for writer in 0..8 {
                let collector = &collector;
                let start = &start;
                scope.spawn(move || {
                    let mut cache = DigestWriterCache::new(writer);
                    start.wait();
                    for n in 0..2_000 {
                        record_sample(collector, &mut cache, &observation(10, n));
                    }
                });
            }
            start.wait();
            for _ in 0..100 {
                for (_, _, stat) in collector.snapshot() {
                    assert_eq!(stat.metrics.count_star, stat.histogram.iter().sum::<u64>());
                    assert_eq!(stat.metrics.sum_rows_sent, stat.metrics.count_star * 5);
                }
                std::thread::yield_now();
            }
        });
        assert_eq!(collector.snapshot()[0].2.metrics.count_star, 16_000);
    }

    #[test]
    fn merged_digest_preserves_statement_columns_and_sample_metadata() {
        let mut left = DigestStat::new(None, 100);
        left.accumulate(10, 3, false, 2, 4, 5, 6, "first", 100);
        let mut right = DigestStat::new(None, 200);
        right.accumulate(30, 7, true, 8, 9, 10, 11, "slowest", 200);
        left.merge(&right);
        assert_eq!(
            &digest_row(None, None, &left)[3..14],
            &[
                u(2),
                u(40),
                u(10),
                u(20),
                u(30),
                u(10),
                u(1),
                u(10),
                u(13),
                u(15),
                u(17),
            ]
        );
        assert_eq!((left.first_seen, left.last_seen), (100, 200));
        assert_eq!(left.query_sample_text.as_deref(), Some("slowest"));
        assert_eq!(
            (left.query_sample_seen, left.query_sample_timer_wait),
            (200, 30)
        );
        assert_eq!(left.histogram.iter().sum::<u64>(), 2);
    }

    #[test]
    fn mysql8046_histogram_boundaries_match_reference() {
        assert_eq!(histogram_low(0), 0);
        assert_eq!(histogram_high(0), 10_000_000);
        assert_eq!(histogram_high(1), 10_471_285);
        assert_eq!(histogram_high(2), 10_964_781);
        assert_eq!(histogram_high(100), 1_000_000_000);
        assert_eq!(histogram_high(449), u64::MAX);
    }
}
