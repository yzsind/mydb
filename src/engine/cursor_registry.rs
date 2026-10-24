//! Process-wide registry of server-side cursors backing `COM_STMT_EXECUTE`
//! with `CURSOR_TYPE_READ_ONLY` (the JDBC `useCursorFetch=true` mode).
//!
//! The registry is the source of truth for cursor-related admin views and
//! protections:
//!
//!   * `information_schema.mydb_cursors` reads `snapshot()` to list every
//!     live cursor with its statement id, owning connection, rows
//!     fetched, and idle time.
//!   * `SHOW ENGINE MYDB STATUS` reads the lifetime counters in
//!     [`CursorStats`] to surface aggregate open/close/eviction rates.
//!   * `try_register` enforces the `mydb_cursor_per_session_max` and
//!     `mydb_cursor_global_max` caps so a misbehaving client can no
//!     longer leak unbounded server-side cursors.
//!   * `evict_idle` (called opportunistically before each new
//!     registration) drops cursors whose last activity is older than
//!     `mydb_cursor_max_idle_ms`. Idle eviction unpins any MVCC
//!     snapshots / overflow chains the cursor would otherwise hold.
//!
//! Lifetime is owned via [`CursorHandle`] embedded inside the per-session
//! `PreparedCursor`. Dropping the handle (cursor exhausted, FETCH errored,
//! `COM_STMT_RESET`, `COM_STMT_CLOSE`, session disconnect, idle eviction)
//! removes the entry; we never rely on an explicit `close` path. See
//! `src/mysql/session.rs::PreparedCursor` for the integration site.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::storage::error;
use crate::types::QueryError;

/// Default per-session cap; matches the design note's recommendation in
/// CLAUDE.md's "Working Notes For Future Agents". A real workload almost
/// never opens this many cursors on one connection, so the cap acts as a
/// guardrail against bugs rather than a tuning knob.
const DEFAULT_PER_SESSION_MAX: usize = 64;
/// Default process-wide cap. With each cursor optionally pinning an MVCC
/// snapshot + scan state, ~4 K cursors is the point where the registry
/// itself starts to weigh more than the cursors do.
const DEFAULT_GLOBAL_MAX: usize = 4096;
/// Default idle timeout (5 min). Cursors silently abandoned by the
/// client (TCP drop without `COM_STMT_CLOSE`, GC of a JDBC statement
/// without a network FIN) must not pin undo retention forever.
const DEFAULT_MAX_IDLE_MS: u64 = 5 * 60 * 1000;

static PER_SESSION_MAX: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_MAX: AtomicUsize = AtomicUsize::new(0);
static MAX_IDLE_MS: AtomicU64 = AtomicU64::new(0);

fn read_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn read_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn ensure_initialized() {
    if PER_SESSION_MAX.load(Ordering::Relaxed) == 0 {
        let v = read_env_usize("MYDB_CURSOR_PER_SESSION_MAX", DEFAULT_PER_SESSION_MAX);
        PER_SESSION_MAX.store(v, Ordering::Relaxed);
    }
    if GLOBAL_MAX.load(Ordering::Relaxed) == 0 {
        let v = read_env_usize("MYDB_CURSOR_GLOBAL_MAX", DEFAULT_GLOBAL_MAX);
        GLOBAL_MAX.store(v, Ordering::Relaxed);
    }
    if MAX_IDLE_MS.load(Ordering::Relaxed) == 0 {
        let v = read_env_u64("MYDB_CURSOR_MAX_IDLE_MS", DEFAULT_MAX_IDLE_MS);
        MAX_IDLE_MS.store(v, Ordering::Relaxed);
    }
}

pub fn per_session_max() -> usize {
    ensure_initialized();
    PER_SESSION_MAX.load(Ordering::Relaxed)
}

pub fn global_max() -> usize {
    ensure_initialized();
    GLOBAL_MAX.load(Ordering::Relaxed)
}

pub fn max_idle_ms() -> u64 {
    ensure_initialized();
    MAX_IDLE_MS.load(Ordering::Relaxed)
}

pub fn set_per_session_max(value: usize) -> Result<(), QueryError> {
    if value == 0 {
        return Err(invalid_value("mydb_cursor_per_session_max", value as u64));
    }
    ensure_initialized();
    PER_SESSION_MAX.store(value, Ordering::Relaxed);
    Ok(())
}

pub fn set_global_max(value: usize) -> Result<(), QueryError> {
    if value == 0 {
        return Err(invalid_value("mydb_cursor_global_max", value as u64));
    }
    ensure_initialized();
    GLOBAL_MAX.store(value, Ordering::Relaxed);
    Ok(())
}

pub fn set_max_idle_ms(value: u64) -> Result<(), QueryError> {
    if value == 0 {
        return Err(invalid_value("mydb_cursor_max_idle_ms", value));
    }
    ensure_initialized();
    MAX_IDLE_MS.store(value, Ordering::Relaxed);
    Ok(())
}

fn invalid_value(name: &str, value: u64) -> QueryError {
    error(
        crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
        format!("Incorrect value '{value}' for variable '{name}'"),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CursorKey {
    pub connection_id: u32,
    pub statement_id: u32,
}

#[derive(Debug, Clone)]
pub struct CursorSnapshot {
    pub key: CursorKey,
    pub sql: Arc<str>,
    pub columns: u32,
    pub rows_fetched: u64,
    pub created_unix_ms: i64,
    pub last_active_unix_ms: i64,
    pub idle_ms: u64,
}

#[derive(Debug)]
struct CursorEntry {
    sql: Arc<str>,
    columns: u32,
    rows_fetched: AtomicU64,
    created_unix_ms: i64,
    /// Wall-clock ms of the last touch (install / fetch). Stored as
    /// ms-since-epoch so the value survives `SystemTime` jitter without
    /// extra translation in the snapshot path.
    last_active_unix_ms: AtomicU64,
}

#[derive(Debug, Default)]
pub struct CursorStats {
    pub opened_total: AtomicU64,
    pub closed_total: AtomicU64,
    pub rows_fetched_total: AtomicU64,
    pub evicted_idle_total: AtomicU64,
    pub rejected_session_cap: AtomicU64,
    pub rejected_global_cap: AtomicU64,
}

#[derive(Debug)]
pub struct CursorRegistry {
    map: Mutex<hashbrown::HashMap<CursorKey, Arc<CursorEntry>>>,
    stats: CursorStats,
}

impl CursorRegistry {
    fn new() -> Self {
        Self {
            map: Mutex::new(hashbrown::HashMap::new()),
            stats: CursorStats::default(),
        }
    }
}

static REGISTRY: OnceLock<CursorRegistry> = OnceLock::new();

pub fn global() -> &'static CursorRegistry {
    REGISTRY.get_or_init(CursorRegistry::new)
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug)]
pub enum RegisterError {
    SessionCapExceeded { current: usize, limit: usize },
    GlobalCapExceeded { current: usize, limit: usize },
}

impl RegisterError {
    pub fn into_query_error(self) -> QueryError {
        match self {
            RegisterError::SessionCapExceeded { current, limit } => error(
                crate::error::catalog::ER_MAX_PREPARED_STMT_COUNT_REACHED,
                format!(
                    "Cannot open a server-side cursor: connection already holds {current} \
                     (mydb_cursor_per_session_max={limit})"
                ),
            ),
            RegisterError::GlobalCapExceeded { current, limit } => error(
                crate::error::catalog::ER_MAX_PREPARED_STMT_COUNT_REACHED,
                format!(
                    "Cannot open a server-side cursor: server already holds {current} \
                     (mydb_cursor_global_max={limit})"
                ),
            ),
        }
    }
}

impl CursorRegistry {
    pub fn stats(&self) -> &CursorStats {
        &self.stats
    }

    pub fn active_count(&self) -> usize {
        self.map.lock().len()
    }

    /// Evict any cursor whose last activity is older than
    /// `mydb_cursor_max_idle_ms`. Called opportunistically right before
    /// each new registration so caps reflect "live" cursors instead of
    /// stragglers that the client has abandoned.
    pub fn evict_idle(&self) -> Vec<CursorKey> {
        let max_idle = max_idle_ms();
        if max_idle == 0 {
            return Vec::new();
        }
        let now = now_unix_ms();
        let mut map = self.map.lock();
        let mut evicted = Vec::new();
        map.retain(|key, entry| {
            let last = entry.last_active_unix_ms.load(Ordering::Relaxed) as i64;
            let idle = now.saturating_sub(last).max(0) as u64;
            if idle >= max_idle {
                evicted.push(*key);
                false
            } else {
                true
            }
        });
        if !evicted.is_empty() {
            self.stats
                .evicted_idle_total
                .fetch_add(evicted.len() as u64, Ordering::Relaxed);
            self.stats
                .closed_total
                .fetch_add(evicted.len() as u64, Ordering::Relaxed);
        }
        evicted
    }

    pub fn try_register(
        &self,
        key: CursorKey,
        sql: Arc<str>,
        columns: u32,
    ) -> Result<CursorHandle, RegisterError> {
        // Opportunistic idle sweep: drop dead cursors before a new
        // registration enforces caps. Done under a separate lock-acquire
        // to keep the registration's critical section short.
        self.evict_idle();
        let session_max = per_session_max();
        let global_max = global_max();
        let now = now_unix_ms();
        let mut map = self.map.lock();
        let session_count = map
            .keys()
            .filter(|k| k.connection_id == key.connection_id)
            .count();
        if session_count >= session_max {
            self.stats
                .rejected_session_cap
                .fetch_add(1, Ordering::Relaxed);
            return Err(RegisterError::SessionCapExceeded {
                current: session_count,
                limit: session_max,
            });
        }
        if map.len() >= global_max {
            self.stats
                .rejected_global_cap
                .fetch_add(1, Ordering::Relaxed);
            return Err(RegisterError::GlobalCapExceeded {
                current: map.len(),
                limit: global_max,
            });
        }
        let entry = Arc::new(CursorEntry {
            sql,
            columns,
            rows_fetched: AtomicU64::new(0),
            created_unix_ms: now,
            last_active_unix_ms: AtomicU64::new(now as u64),
        });
        // Replace any stale entry for the same key (e.g. previous EXECUTE
        // on this prepared statement died without reaching close). Drop
        // the replacement outside the lock to be safe.
        let stale = map.insert(key, Arc::clone(&entry));
        self.stats.opened_total.fetch_add(1, Ordering::Relaxed);
        drop(stale);
        Ok(CursorHandle {
            key,
            entry: Some(entry),
        })
    }

    fn unregister(&self, key: CursorKey) {
        if self.map.lock().remove(&key).is_some() {
            self.stats.closed_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> Vec<CursorSnapshot> {
        let now = now_unix_ms();
        let map = self.map.lock();
        let mut out = Vec::with_capacity(map.len());
        for (key, entry) in map.iter() {
            let last = entry.last_active_unix_ms.load(Ordering::Relaxed) as i64;
            let idle = now.saturating_sub(last).max(0) as u64;
            out.push(CursorSnapshot {
                key: *key,
                sql: Arc::clone(&entry.sql),
                columns: entry.columns,
                rows_fetched: entry.rows_fetched.load(Ordering::Relaxed),
                created_unix_ms: entry.created_unix_ms,
                last_active_unix_ms: last,
                idle_ms: idle,
            });
        }
        out.sort_by_key(|s| (s.key.connection_id, s.key.statement_id));
        out
    }

    #[cfg(test)]
    fn clear(&self) {
        self.map.lock().clear();
        for f in [
            &self.stats.opened_total,
            &self.stats.closed_total,
            &self.stats.rows_fetched_total,
            &self.stats.evicted_idle_total,
            &self.stats.rejected_session_cap,
            &self.stats.rejected_global_cap,
        ] {
            f.store(0, Ordering::Relaxed);
        }
    }
}

/// Lifetime token attached to a `PreparedCursor`; dropping it removes
/// the cursor from the global registry exactly once.
#[derive(Debug)]
pub struct CursorHandle {
    key: CursorKey,
    entry: Option<Arc<CursorEntry>>,
}

impl CursorHandle {
    pub fn key(&self) -> CursorKey {
        self.key
    }

    pub fn record_fetch(&self, rows: u64) {
        if let Some(entry) = self.entry.as_ref() {
            entry.rows_fetched.fetch_add(rows, Ordering::Relaxed);
            entry
                .last_active_unix_ms
                .store(now_unix_ms() as u64, Ordering::Relaxed);
            global()
                .stats
                .rows_fetched_total
                .fetch_add(rows, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    pub fn touch(&self) {
        if let Some(entry) = self.entry.as_ref() {
            entry
                .last_active_unix_ms
                .store(now_unix_ms() as u64, Ordering::Relaxed);
        }
    }
}

impl Drop for CursorHandle {
    fn drop(&mut self) {
        if self.entry.is_some() {
            global().unregister(self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    /// nextest runs tests in parallel — every test in this module
    /// pokes the same process-wide registry / atomic GUCs, so we
    /// serialize them. Other modules don't take this lock; they
    /// observe the registry through their own (session-id-keyed)
    /// entries which don't collide.
    static TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    fn fresh_registry() -> (
        &'static CursorRegistry,
        parking_lot::MutexGuard<'static, ()>,
    ) {
        let guard = TEST_LOCK.lock();
        let r = global();
        r.clear();
        (r, guard)
    }

    #[test]
    fn drop_handle_unregisters_cursor() {
        let (r, _g) = fresh_registry();
        let key = CursorKey {
            connection_id: 1,
            statement_id: 1,
        };
        let handle = r
            .try_register(key, Arc::from("SELECT 1"), 1)
            .expect("register");
        assert_eq!(r.active_count(), 1);
        drop(handle);
        assert_eq!(r.active_count(), 0);
        assert_eq!(r.stats.opened_total.load(Ordering::Relaxed), 1);
        assert_eq!(r.stats.closed_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn record_fetch_increments_counters_and_touches_timestamp() {
        let (r, _g) = fresh_registry();
        let key = CursorKey {
            connection_id: 7,
            statement_id: 3,
        };
        let handle = r.try_register(key, Arc::from("SELECT v"), 1).unwrap();
        let before = r.snapshot()[0].last_active_unix_ms;
        thread::sleep(Duration::from_millis(5));
        handle.record_fetch(42);
        let snap = r.snapshot();
        assert_eq!(snap[0].rows_fetched, 42);
        assert!(snap[0].last_active_unix_ms >= before);
        assert_eq!(r.stats.rows_fetched_total.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn per_session_cap_rejects_extras() {
        let (r, _g) = fresh_registry();
        set_per_session_max(2).unwrap();
        let key = |s| CursorKey {
            connection_id: 99,
            statement_id: s,
        };
        let h1 = r.try_register(key(1), Arc::from("a"), 1).unwrap();
        let h2 = r.try_register(key(2), Arc::from("b"), 1).unwrap();
        let err = r.try_register(key(3), Arc::from("c"), 1).err().unwrap();
        match err {
            RegisterError::SessionCapExceeded { current, limit } => {
                assert_eq!(current, 2);
                assert_eq!(limit, 2);
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(r.stats.rejected_session_cap.load(Ordering::Relaxed), 1);
        drop(h1);
        drop(h2);
        set_per_session_max(DEFAULT_PER_SESSION_MAX).unwrap();
    }

    #[test]
    fn evict_idle_removes_stale_cursors() {
        let (r, _g) = fresh_registry();
        set_max_idle_ms(50).unwrap();
        let key = CursorKey {
            connection_id: 11,
            statement_id: 11,
        };
        let _h = r.try_register(key, Arc::from("idle"), 1).unwrap();
        thread::sleep(Duration::from_millis(80));
        let evicted = r.evict_idle();
        assert_eq!(evicted, vec![key]);
        // The drop of `_h` will then run `unregister`, but it's a no-op
        // because the entry is already gone.
        assert_eq!(r.active_count(), 0);
        assert_eq!(r.stats.evicted_idle_total.load(Ordering::Relaxed), 1);
        set_max_idle_ms(DEFAULT_MAX_IDLE_MS).unwrap();
    }

    /// Minimal `ResultCursor` for named-cursor tests — yields a finite
    /// integer sequence so we can assert FETCH advances by exactly the
    /// expected number of rows.
    #[derive(Debug)]
    struct CountingCursor {
        next: i64,
        limit: i64,
    }

    impl crate::engine::api::ResultCursor for CountingCursor {
        fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
            if self.next > self.limit {
                return Ok(None);
            }
            let row = vec![Value::Int(self.next)];
            self.next += 1;
            Ok(Some(row))
        }
    }

    fn make_column() -> Column {
        Column {
            name: "v".to_string(),
            column_type: crate::types::ColumnType::LongLong,
            source: Default::default(),
        }
    }

    #[test]
    fn named_open_fetch_close_cycle() {
        let (_r, _g) = fresh_registry();
        named_clear();
        named_open(
            7,
            "c1".to_string(),
            Arc::from("SELECT v FROM t"),
            vec![make_column()],
            Box::new(CountingCursor { next: 1, limit: 5 }),
        )
        .expect("open");
        let f1 = named_fetch(7, "c1", 3).expect("fetch3");
        assert_eq!(f1.rows.len(), 3);
        assert_eq!(f1.rows[0], vec![Value::Int(1)]);
        assert_eq!(f1.rows[2], vec![Value::Int(3)]);
        assert!(!f1.exhausted);
        let f2 = named_fetch(7, "c1", 10).expect("fetch10");
        assert_eq!(f2.rows.len(), 2);
        assert_eq!(f2.rows[0], vec![Value::Int(4)]);
        assert_eq!(f2.rows[1], vec![Value::Int(5)]);
        assert!(f2.exhausted);
        assert_eq!(named_active_count(), 1);
        named_close(7, "c1").expect("close");
        assert_eq!(named_active_count(), 0);
        let err = named_fetch(7, "c1", 1).expect_err("missing cursor");
        assert_eq!(err.code(), 1326);
        let stats = named_stats_snapshot();
        assert_eq!(stats.opened_total.load(Ordering::Relaxed), 1);
        assert_eq!(stats.closed_total.load(Ordering::Relaxed), 1);
        assert_eq!(stats.rows_fetched_total.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn named_open_rejects_duplicate_name_on_same_connection() {
        let (_r, _g) = fresh_registry();
        named_clear();
        named_open(
            8,
            "dup".to_string(),
            Arc::from("SELECT 1"),
            vec![make_column()],
            Box::new(CountingCursor { next: 1, limit: 1 }),
        )
        .unwrap();
        let err = named_open(
            8,
            "dup".to_string(),
            Arc::from("SELECT 2"),
            vec![make_column()],
            Box::new(CountingCursor { next: 1, limit: 1 }),
        )
        .expect_err("duplicate name should be rejected");
        assert_eq!(err.code(), 1325);
        named_close(8, "dup").unwrap();
    }

    #[test]
    fn named_close_for_connection_releases_all() {
        let (_r, _g) = fresh_registry();
        named_clear();
        for name in ["a", "b", "c"] {
            named_open(
                12,
                name.to_string(),
                Arc::from("SELECT 1"),
                vec![make_column()],
                Box::new(CountingCursor { next: 1, limit: 1 }),
            )
            .unwrap();
        }
        // A cursor on a different connection must NOT be touched.
        named_open(
            999,
            "other".to_string(),
            Arc::from("SELECT 1"),
            vec![make_column()],
            Box::new(CountingCursor { next: 1, limit: 1 }),
        )
        .unwrap();
        let removed = named_close_for_connection(12);
        assert_eq!(removed, 3);
        assert_eq!(named_active_count(), 1);
        // Snapshot should now show only the surviving connection.
        let snap = named_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].connection_id, 999);
        named_close(999, "other").unwrap();
    }
}

// ----------------------------------------------------------------------
// Named-cursor sub-registry: backs `DECLARE <name> CURSOR FOR <query>` /
// `FETCH N FROM <name>` / `CLOSE <name>` SQL statements at the session
// level (a PostgreSQL-flavoured extension; standard MySQL only allows
// DECLARE CURSOR inside stored routines).
//
// We keep this as a sibling structure to the prepared CursorRegistry
// rather than overloading `CursorKey`, because:
//   * The lifecycle is different — these cursors are addressed by SQL
//     identifier, not by `(connection_id, statement_id)`.
//   * The protocol surface is different — DECLARE/FETCH/CLOSE flow as
//     plain `COM_QUERY` results, not `COM_STMT_FETCH` binary rows.
//
// Both registries share the same `mydb_cursor_*` GUCs (per-session cap,
// global cap, idle timeout) so a misbehaving client cannot leak named
// cursors any more than it can leak prepared cursors. The
// `information_schema.mydb_cursors` view unions the two sources so
// admins see every server-side cursor in one place.
// ----------------------------------------------------------------------

use crate::types::{Column, Value};

#[derive(Debug, Clone)]
pub struct NamedCursorSnapshot {
    pub connection_id: u32,
    pub name: String,
    pub sql: Arc<str>,
    pub columns: u32,
    pub rows_fetched: u64,
    pub created_unix_ms: i64,
    pub last_active_unix_ms: i64,
    pub idle_ms: u64,
}

struct NamedCursorState {
    inner: Box<dyn crate::engine::api::ResultCursor>,
    lookahead: Option<Vec<Value>>,
    exhausted: bool,
}

pub struct NamedCursorEntry {
    pub columns: Vec<Column>,
    pub sql: Arc<str>,
    /// Per-cursor lock isolates this cursor's pull/advance from the
    /// registry's outer lock: a slow `next_row()` does NOT block
    /// other DECLARE/FETCH/CLOSE on unrelated cursors.
    state: parking_lot::Mutex<NamedCursorState>,
    pub created_unix_ms: i64,
    pub rows_fetched: AtomicU64,
    pub last_active_unix_ms: AtomicU64,
}

impl std::fmt::Debug for NamedCursorEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamedCursorEntry")
            .field("columns", &self.columns)
            .field("sql", &self.sql)
            .field("rows_fetched", &self.rows_fetched.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct NamedFetchResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    pub exhausted: bool,
}

#[derive(Debug, Default)]
struct NamedRegistryStorage {
    cursors: hashbrown::HashMap<(u32, String), Arc<NamedCursorEntry>>,
}

static NAMED_REGISTRY: OnceLock<parking_lot::Mutex<NamedRegistryStorage>> = OnceLock::new();
static NAMED_STATS: OnceLock<CursorStats> = OnceLock::new();

fn named_registry() -> &'static parking_lot::Mutex<NamedRegistryStorage> {
    NAMED_REGISTRY.get_or_init(|| parking_lot::Mutex::new(NamedRegistryStorage::default()))
}

fn named_stats() -> &'static CursorStats {
    NAMED_STATS.get_or_init(CursorStats::default)
}

pub fn named_stats_snapshot() -> &'static CursorStats {
    named_stats()
}

pub fn named_active_count() -> usize {
    named_registry().lock().cursors.len()
}

fn named_evict_idle() -> usize {
    let max_idle = max_idle_ms();
    if max_idle == 0 {
        return 0;
    }
    let now = now_unix_ms();
    let mut storage = named_registry().lock();
    let before = storage.cursors.len();
    storage.cursors.retain(|_key, entry| {
        let last = entry.last_active_unix_ms.load(Ordering::Relaxed) as i64;
        let idle = now.saturating_sub(last).max(0) as u64;
        idle < max_idle
    });
    let evicted = before - storage.cursors.len();
    if evicted > 0 {
        named_stats()
            .evicted_idle_total
            .fetch_add(evicted as u64, Ordering::Relaxed);
        named_stats()
            .closed_total
            .fetch_add(evicted as u64, Ordering::Relaxed);
    }
    evicted
}

/// Install a freshly opened SQL cursor under `name` for `connection_id`.
/// Returns an error when the per-session or global cap would be
/// exceeded — the cursor (and its underlying stream / snapshot) is
/// then dropped, never installed.
pub fn named_open(
    connection_id: u32,
    name: String,
    sql: Arc<str>,
    columns: Vec<Column>,
    inner: Box<dyn crate::engine::api::ResultCursor>,
) -> Result<(), QueryError> {
    named_evict_idle();
    let session_max = per_session_max();
    let global_max = global_max();
    let mut storage = named_registry().lock();
    let session_count = storage
        .cursors
        .keys()
        .filter(|(conn, _)| *conn == connection_id)
        .count();
    if session_count >= session_max {
        named_stats()
            .rejected_session_cap
            .fetch_add(1, Ordering::Relaxed);
        return Err(error(
            crate::error::catalog::ER_MAX_PREPARED_STMT_COUNT_REACHED,
            format!(
                "Cannot DECLARE CURSOR '{name}': connection already holds {session_count} \
                 (mydb_cursor_per_session_max={session_max})"
            ),
        ));
    }
    if storage.cursors.len() >= global_max {
        named_stats()
            .rejected_global_cap
            .fetch_add(1, Ordering::Relaxed);
        return Err(error(
            crate::error::catalog::ER_MAX_PREPARED_STMT_COUNT_REACHED,
            format!(
                "Cannot DECLARE CURSOR '{name}': server already holds {} \
                 (mydb_cursor_global_max={global_max})",
                storage.cursors.len()
            ),
        ));
    }
    let key = (connection_id, name.clone());
    if storage.cursors.contains_key(&key) {
        return Err(error(
            crate::error::catalog::ER_SP_CURSOR_ALREADY_OPEN,
            format!("Cursor '{name}' is already open"),
        ));
    }
    let now = now_unix_ms();
    let entry = Arc::new(NamedCursorEntry {
        columns: columns.clone(),
        sql,
        state: parking_lot::Mutex::new(NamedCursorState {
            inner,
            lookahead: None,
            exhausted: false,
        }),
        created_unix_ms: now,
        rows_fetched: AtomicU64::new(0),
        last_active_unix_ms: AtomicU64::new(now as u64),
    });
    storage.cursors.insert(key, entry);
    named_stats().opened_total.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Pull up to `count` rows from the named cursor. Returns
/// `Err(ER_SP_CURSOR_NOT_OPEN)` if no such cursor exists, mirroring
/// MySQL's error for in-routine FETCH of a closed cursor.
pub fn named_fetch(
    connection_id: u32,
    name: &str,
    count: u64,
) -> Result<NamedFetchResult, QueryError> {
    // Hold the outer registry lock only long enough to `Arc::clone` the
    // entry — the actual row pull happens on the entry's own mutex.
    let entry = {
        let storage = named_registry().lock();
        storage
            .cursors
            .get(&(connection_id, name.to_string()))
            .cloned()
    };
    let Some(entry) = entry else {
        return Err(error(
            crate::error::catalog::ER_SP_CURSOR_NOT_OPEN,
            format!("Cursor '{name}' is not open"),
        ));
    };
    let columns = entry.columns.clone();
    let mut state = entry.state.lock();
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(count as usize);
    while (rows.len() as u64) < count {
        if let Some(row) = state.lookahead.take() {
            rows.push(row);
            continue;
        }
        if state.exhausted {
            break;
        }
        match state.inner.next_row()? {
            Some(row) => rows.push(row),
            None => {
                state.exhausted = true;
                break;
            }
        }
    }
    let exhausted = state.exhausted && state.lookahead.is_none();
    drop(state);
    let now = now_unix_ms();
    entry
        .rows_fetched
        .fetch_add(rows.len() as u64, Ordering::Relaxed);
    entry
        .last_active_unix_ms
        .store(now as u64, Ordering::Relaxed);
    named_stats()
        .rows_fetched_total
        .fetch_add(rows.len() as u64, Ordering::Relaxed);
    Ok(NamedFetchResult {
        columns,
        rows,
        exhausted,
    })
}

/// Close + drop the named cursor (and any MVCC snapshot / scan state it
/// pinned). Returns `Err` if the cursor isn't open — matches MySQL's
/// `ER_SP_CURSOR_NOT_OPEN`.
pub fn named_close(connection_id: u32, name: &str) -> Result<(), QueryError> {
    let removed = named_registry()
        .lock()
        .cursors
        .remove(&(connection_id, name.to_string()));
    if removed.is_some() {
        named_stats().closed_total.fetch_add(1, Ordering::Relaxed);
        Ok(())
    } else {
        Err(error(
            crate::error::catalog::ER_SP_CURSOR_NOT_OPEN,
            format!("Cursor '{name}' is not open"),
        ))
    }
}

/// Session-drop sweep: discard every named cursor owned by
/// `connection_id`. Called from `Session::Drop` so a disconnected client
/// can't leak named cursors past its lifetime.
pub fn named_close_for_connection(connection_id: u32) -> usize {
    let mut storage = named_registry().lock();
    let before = storage.cursors.len();
    storage
        .cursors
        .retain(|(conn, _name), _entry| *conn != connection_id);
    let removed = before - storage.cursors.len();
    if removed > 0 {
        named_stats()
            .closed_total
            .fetch_add(removed as u64, Ordering::Relaxed);
    }
    removed
}

pub fn named_snapshot() -> Vec<NamedCursorSnapshot> {
    let storage = named_registry().lock();
    let now = now_unix_ms();
    let mut out = Vec::with_capacity(storage.cursors.len());
    for ((conn, name), entry) in storage.cursors.iter() {
        let last = entry.last_active_unix_ms.load(Ordering::Relaxed) as i64;
        let idle = now.saturating_sub(last).max(0) as u64;
        out.push(NamedCursorSnapshot {
            connection_id: *conn,
            name: name.clone(),
            sql: Arc::clone(&entry.sql),
            columns: entry.columns.len() as u32,
            rows_fetched: entry.rows_fetched.load(Ordering::Relaxed),
            created_unix_ms: entry.created_unix_ms,
            last_active_unix_ms: last,
            idle_ms: idle,
        });
    }
    out.sort_by(|a, b| (a.connection_id, a.name.clone()).cmp(&(b.connection_id, b.name.clone())));
    out
}

#[cfg(test)]
pub(crate) fn named_clear() {
    named_registry().lock().cursors.clear();
    let stats = named_stats();
    for f in [
        &stats.opened_total,
        &stats.closed_total,
        &stats.rows_fetched_total,
        &stats.evicted_idle_total,
        &stats.rejected_session_cap,
        &stats.rejected_global_cap,
    ] {
        f.store(0, Ordering::Relaxed);
    }
}
