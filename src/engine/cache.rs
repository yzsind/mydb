//! Parsed-statement and compiled SELECT plan caches.
//!
//! Presentation-bearing parsed ASTs use exact SQL text keys. Semantic SELECT
//! artifacts use a typed structural fingerprint plus database/session
//! semantics and validate catalog dependencies on every shared-cache hit.

use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use ahash::RandomState as AhashState;
use lru::LruCache;
use parking_lot::Mutex;
use smallvec::SmallVec;

use crate::engine::api::PreparedDmlPlan;
use crate::engine::auth::RequiredPrivilege;
use crate::parser::{Expr, Select, SelectItem, Statement};
use crate::planner::PhysicalPlan;
use crate::storage::{
    IndexKey, IndexSchema, MdlRequest, ProjectedReadPlan, RowDecodePlan, TableSchema,
};

/// Process-global registry so observability code (e.g. `SHOW ENGINE MYDB
/// STATUS`) can read cache stats without threading cache references through
/// every call site. Holds `Weak` pointers so dropping the engine releases
/// the caches. mydb runs a single `StorageQueryEngine` per process; multiple
/// engines (e.g. tests) register the most recent.
struct GlobalSlots {
    statement: Option<Weak<ParsedStatementCache>>,
    plan: Option<Weak<PlanCache>>,
    prepared_dml: Option<Weak<PreparedDmlPlanCache>>,
    prepared_dml_batch: Option<Weak<PreparedDmlBatchCache>>,
}

static GLOBAL: OnceLock<Mutex<GlobalSlots>> = OnceLock::new();

fn global_slots() -> &'static Mutex<GlobalSlots> {
    GLOBAL.get_or_init(|| {
        Mutex::new(GlobalSlots {
            statement: None,
            plan: None,
            prepared_dml: None,
            prepared_dml_batch: None,
        })
    })
}

pub fn register_global(cache: &Arc<ParsedStatementCache>) {
    global_slots().lock().statement = Some(Arc::downgrade(cache));
}

pub fn register_global_plan(cache: &Arc<PlanCache>) {
    global_slots().lock().plan = Some(Arc::downgrade(cache));
}

pub fn register_global_prepared_dml(cache: &Arc<PreparedDmlPlanCache>) {
    global_slots().lock().prepared_dml = Some(Arc::downgrade(cache));
}

pub fn register_global_prepared_dml_batch(cache: &Arc<PreparedDmlBatchCache>) {
    global_slots().lock().prepared_dml_batch = Some(Arc::downgrade(cache));
}

pub fn global_stats() -> Option<CacheStats> {
    global_slots()
        .lock()
        .statement
        .as_ref()
        .and_then(Weak::upgrade)
        .map(|c| c.stats())
}

pub fn global_plan_stats() -> Option<PlanCacheStats> {
    global_slots()
        .lock()
        .plan
        .as_ref()
        .and_then(Weak::upgrade)
        .map(|c| c.stats())
}

pub fn global_prepared_dml_stats() -> Option<PlanCacheStats> {
    global_slots()
        .lock()
        .prepared_dml
        .as_ref()
        .and_then(Weak::upgrade)
        .map(|c| c.stats())
}

pub fn global_prepared_dml_batch_stats() -> Option<PlanCacheStats> {
    global_slots()
        .lock()
        .prepared_dml_batch
        .as_ref()
        .and_then(Weak::upgrade)
        .map(|c| c.stats())
}

pub fn global_plan_cache() -> Option<Arc<PlanCache>> {
    global_slots().lock().plan.as_ref().and_then(Weak::upgrade)
}

/// Clear only the prepared-DML shape caches (no epoch bump, no statement /
/// plan clear). Used by VIEW DDL: SELECT plans invalidate precisely via
/// view object-deps, but a cached DML memo built through an updatable view
/// could embed the old view definition, so the (small, rarely churned) DML
/// caches are cleared conservatively.
pub fn clear_prepared_dml_caches() {
    let (prepared_dml, prepared_dml_batch) = {
        let slots = global_slots().lock();
        (
            slots.prepared_dml.as_ref().and_then(Weak::upgrade),
            slots.prepared_dml_batch.as_ref().and_then(Weak::upgrade),
        )
    };
    if let Some(cache) = prepared_dml {
        cache.clear();
    }
    if let Some(cache) = prepared_dml_batch {
        cache.clear();
    }
}

/// Complete administrative SQL-cache flush. Engine entry points supply
/// their own caches; the legacy dispatch route resolves the registered ones.
pub(crate) fn flush_sql_caches(
    statement: Option<&ParsedStatementCache>,
    plan: Option<&PlanCache>,
    prepared_dml: Option<&PreparedDmlPlanCache>,
    prepared_dml_batch: Option<&PreparedDmlBatchCache>,
) {
    if let Some(cache) = statement {
        cache.bump_epoch();
        cache.clear();
    }
    if let Some(cache) = plan {
        cache.bump_epoch();
        cache.clear();
    }
    if let Some(cache) = prepared_dml {
        cache.clear();
    }
    if let Some(cache) = prepared_dml_batch {
        cache.clear();
    }
    crate::engine::expr_program::flush_program_caches();
}

pub(crate) fn flush_global_sql_caches() {
    let (statement, plan, dml, batch) = {
        let slots = global_slots().lock();
        (
            slots.statement.as_ref().and_then(Weak::upgrade),
            slots.plan.as_ref().and_then(Weak::upgrade),
            slots.prepared_dml.as_ref().and_then(Weak::upgrade),
            slots.prepared_dml_batch.as_ref().and_then(Weak::upgrade),
        )
    };
    flush_sql_caches(
        statement.as_deref(),
        plan.as_deref(),
        dml.as_deref(),
        batch.as_deref(),
    );
}

pub fn clear_global_caches() {
    let (plan, prepared_dml, prepared_dml_batch) = {
        let slots = global_slots().lock();
        (
            slots.plan.as_ref().and_then(Weak::upgrade),
            slots.prepared_dml.as_ref().and_then(Weak::upgrade),
            slots.prepared_dml_batch.as_ref().and_then(Weak::upgrade),
        )
    };
    if let Some(cache) = plan {
        cache.bump_epoch();
        cache.clear();
    }
    if let Some(cache) = prepared_dml {
        cache.clear();
    }
    if let Some(cache) = prepared_dml_batch {
        cache.clear();
    }
    // Compiled expression programs are cached thread-locally + in a
    // shared L2; FLUSH SQL_CACHE resets those too so differential
    // testing can clear the full compile state in one statement.
    crate::engine::expr_program::flush_program_caches();
}

/// Pressure-only cache reclamation. Unlike DDL invalidation this also drops
/// parsed statement ASTs. Active users retain their Arc-owned payload and its
/// governor charge until the last owner exits; clearing only removes optional
/// cache ownership and never changes statement semantics.
pub(crate) fn reclaim_global_caches() {
    let (statement, plan, prepared_dml, prepared_dml_batch) = {
        let slots = global_slots().lock();
        (
            slots.statement.as_ref().and_then(Weak::upgrade),
            slots.plan.as_ref().and_then(Weak::upgrade),
            slots.prepared_dml.as_ref().and_then(Weak::upgrade),
            slots.prepared_dml_batch.as_ref().and_then(Weak::upgrade),
        )
    };
    if let Some(cache) = statement {
        cache.clear();
    }
    if let Some(cache) = plan {
        cache.clear();
    }
    if let Some(cache) = prepared_dml {
        cache.clear();
    }
    if let Some(cache) = prepared_dml_batch {
        cache.clear();
    }
}

/// Per-statement context propagated through the SELECT execution path so
/// `execute_select` can look up plans without rethreading the SQL string and
/// cache reference through every signature in `dispatch::*`. Set by the
/// query entrypoint via `with_query_hint`; cleared automatically when the
/// `QueryHintGuard` drops. Only valid on the thread that called
/// `with_query_hint` — this matches the spawn_blocking execution model.
///
/// The `plan_cache` is carried as a `Weak` so that each engine drives its
/// own cache (vs. the process-global registry which is for observability
/// only and may be stale when multiple engines coexist, e.g. concurrent
/// tests).
#[derive(Clone)]
pub struct QueryHint {
    pub normalized_sql: Arc<str>,
    pub database: Arc<str>,
    pub semantics: PlanSemantics,
    pub parameterized: bool,
    pub plan_cache: Weak<PlanCache>,
}

thread_local! {
    static CURRENT_HINT: std::cell::RefCell<Option<QueryHint>> = const { std::cell::RefCell::new(None) };
}

/// Take the current hint, leaving the slot empty. Use this from
/// `execute_select` so a single statement consumes its hint exactly once;
/// nested SELECTs (IN subqueries, derived tables, scalar subqueries) then
/// observe `None` and skip the plan cache. Without this, a recursive
/// `execute_select` would reuse the outer SQL's cache key for an inner
/// subquery and risk pulling out a plan that doesn't match its statement.
pub fn take_query_hint() -> Option<QueryHint> {
    CURRENT_HINT.with(|cell| cell.borrow_mut().take())
}

pub struct QueryHintGuard {
    previous: Option<QueryHint>,
}

impl Drop for QueryHintGuard {
    fn drop(&mut self) {
        CURRENT_HINT.with(|cell| *cell.borrow_mut() = self.previous.take());
    }
}

/// Hide the caller's key while a stored program executes its own SQL.
/// Restore it even when the routine errors or unwinds.
pub(crate) fn suspend_query_hint() -> QueryHintGuard {
    QueryHintGuard {
        previous: take_query_hint(),
    }
}

pub fn with_query_hint<R>(hint: QueryHint, f: impl FnOnce() -> R) -> R {
    let previous = CURRENT_HINT.with(|cell| cell.replace(Some(hint)));
    // Start each statement with an empty view-dependency accumulator so a
    // plan inserted for this statement records only the views it expands.
    reset_plan_object_deps();
    let _guard = QueryHintGuard { previous };
    f()
}

/// Pre-normalised lookup keys carried from a session-level prepared
/// statement into the engine. Lets the COM_STMT_EXECUTE hot path skip the
/// per-execute `normalize_sql` + `Arc::<str>::from` allocations: a sysbench
/// `oltp_read_only` workload was spending ~4% of CPU there because the
/// prepared SQL is identical on every execute.
#[derive(Clone)]
pub struct PreparedHint {
    pub normalized_sql: Arc<str>,
    pub database: Arc<str>,
}

thread_local! {
    static PREPARED_HINT: std::cell::RefCell<Option<PreparedHint>> = const { std::cell::RefCell::new(None) };
}

pub fn current_prepared_hint() -> Option<PreparedHint> {
    PREPARED_HINT.with(|cell| cell.borrow().clone())
}

pub struct PreparedHintGuard {
    previous: Option<PreparedHint>,
}

impl Drop for PreparedHintGuard {
    fn drop(&mut self) {
        PREPARED_HINT.with(|cell| *cell.borrow_mut() = self.previous.take());
    }
}

pub fn with_prepared_hint<R>(hint: PreparedHint, f: impl FnOnce() -> R) -> R {
    let previous = PREPARED_HINT.with(|cell| cell.replace(Some(hint)));
    let _guard = PreparedHintGuard { previous };
    f()
}

/// Per-cache monotonically increasing epoch. Bumped on any catalog-mutating
/// DDL routed through this cache; cached entries captured under an older
/// epoch are treated as stale on the next lookup.
#[derive(Debug, Default)]
pub struct CacheEpoch(AtomicU64);

impl CacheEpoch {
    pub const fn new() -> Self {
        Self(AtomicU64::new(1))
    }

    pub fn current(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    /// Returns the new epoch value.
    pub fn bump(&self) -> u64 {
        self.0.fetch_add(1, Ordering::AcqRel) + 1
    }
}

/// Shared cache key. Parsed-statement users put exact SQL bytes in `sql`;
/// compiled-plan users put a typed structural fingerprint there. Database and
/// session semantics prevent cross-context reuse.
///
/// Session semantics that can affect SELECT normalization, physical-plan
/// choice, or the result produced by an expression embedded in a cached plan.
/// Keep this separate from [`StatementKey`] so every cache producer uses the
/// same exhaustive set instead of growing ad-hoc fingerprint fields.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PlanSemantics {
    pub sql_mode_fingerprint: u64,
    pub optimizer_switch_fingerprint: u64,
    pub sql_select_limit: u64,
    pub use_invisible_indexes: bool,
    pub time_zone: Arc<str>,
    pub character_set_client: Arc<str>,
    pub character_set_connection: Arc<str>,
    pub collation_connection: Arc<str>,
    /// Affects DECIMAL division and AVG result precision/scale.
    pub div_precision_increment: u8,
    /// Selects VARCHAR versus BLOB for GROUP_CONCAT and fixes its maximum
    /// advertised length.  Reusing a plan across different values would make
    /// prepared metadata stale even when the physical plan is unchanged.
    pub group_concat_max_len: u64,
}

impl Default for PlanSemantics {
    fn default() -> Self {
        Self {
            sql_mode_fingerprint: 0,
            optimizer_switch_fingerprint: 0,
            sql_select_limit: u64::MAX,
            use_invisible_indexes: false,
            time_zone: Arc::from("SYSTEM"),
            character_set_client: Arc::from(crate::charset::DEFAULT_CHARSET),
            character_set_connection: Arc::from(crate::charset::DEFAULT_CHARSET),
            collation_connection: Arc::from(crate::charset::DEFAULT_COLLATION),
            div_precision_increment: 4,
            group_concat_max_len: 1024,
        }
    }
}

/// The SQL text/fingerprint and current database identify the statement
/// shape; `semantics` prevents that shape from crossing a session boundary
/// that can change its plan or result.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StatementKey {
    pub sql: Arc<str>,
    pub database: Arc<str>,
    pub semantics: PlanSemantics,
}

/// Exact parser-cache identity. Parsed ASTs are deliberately kept
/// unqualified: catalog/database/session state belongs to binding and plan
/// caches, while parsing depends only on the original source bytes and the
/// tokenizer-affecting sql_mode bits.
///
/// `sql_len` is redundant with `sql.len()` but kept explicit so the key shape
/// documents and tests the full collision guard required by the protocol
/// cache contract. Equality still compares the complete SQL bytes.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ParsedStatementKey {
    pub sql: Arc<str>,
    pub sql_len: usize,
    pub parser_sql_mode_bits: u16,
}

impl ParsedStatementKey {
    pub fn new(sql: &str, parser_sql_mode_bits: u16) -> Self {
        Self {
            sql: Arc::from(sql),
            sql_len: sql.len(),
            parser_sql_mode_bits,
        }
    }
}

pub fn optimizer_switch_fingerprint(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Cached, unqualified statement AST. The `epoch` snapshots the global
/// epoch at insertion time; consumers must compare with `current_epoch()` and
/// drop the entry on mismatch.
#[derive(Debug)]
pub struct CachedStatement {
    pub ast: Arc<Statement>,
    pub epoch: u64,
}

#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub stale_evictions: u64,
    pub lru_evictions: u64,
    pub size: u64,
    pub capacity: u64,
    /// Approximate resident bytes: per-entry key text plus a fixed
    /// per-entry overhead standing in for the cached value (the parsed
    /// AST / plan is `Arc`-shared, so exact accounting is fuzzy). A
    /// growth gauge, not a precise number.
    pub approx_bytes: u64,
}

/// Fixed per-entry overhead added on top of the key text when estimating
/// resident bytes. The values (parsed `Statement`, `PhysicalPlan`) are
/// `Arc`-shared and vary widely; these constants give operators a stable,
/// monotone growth signal without walking the AST/plan on every stats read.
const STMT_ENTRY_OVERHEAD: u64 = 256;
const PLAN_ENTRY_OVERHEAD: u64 = 512;
const DML_ENTRY_OVERHEAD: u64 = 192;

#[derive(Debug, Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    stale_evictions: AtomicU64,
    lru_evictions: AtomicU64,
}

/// Shard count for the statement / plan caches. Production-sized caches
/// (capacity ≥ 256) split into 8 independent LRUs routed by key hash, so
/// concurrent sessions stop serialising every lookup on one mutex. Small
/// caches (tests, tuned-down configs) stay single-shard so exact LRU
/// capacity semantics hold.
const CACHE_SHARDS: usize = 8;

fn cache_shard_count(capacity: usize) -> usize {
    if capacity >= 256 {
        CACHE_SHARDS
    } else {
        1
    }
}

/// Storage mechanics shared by caches; validity and accounting stay with
/// each cache because parsed source and compiled plans have different epochs.
struct ShardedLru<K, V> {
    shards: Box<[Mutex<LruCache<K, V, AhashState>>]>,
    router: AhashState,
    capacity: usize,
}

impl<K: std::hash::Hash + Eq, V> ShardedLru<K, V> {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let shard_count = cache_shard_count(capacity);
        let shards = (0..shard_count)
            .map(|index| {
                let capacity = capacity / shard_count + usize::from(index < capacity % shard_count);
                Mutex::new(LruCache::with_hasher(
                    NonZeroUsize::new(capacity).unwrap(),
                    AhashState::new(),
                ))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            router: AhashState::new(),
            capacity,
        }
    }

    #[inline]
    fn shard_for(&self, key: &K) -> &Mutex<LruCache<K, V, AhashState>> {
        let h = self.router.hash_one(key) as usize;
        &self.shards[h % self.shards.len()]
    }

    fn size(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.lock().len() as u64)
            .sum()
    }

    fn size_and_bytes(&self, entry_bytes: impl Fn(&K, &V) -> u64) -> (u64, u64) {
        let mut size = 0;
        let mut bytes = 0;
        for shard in &self.shards {
            let guard = shard.lock();
            size += guard.len() as u64;
            bytes += guard
                .iter()
                .map(|(key, value)| entry_bytes(key, value))
                .sum::<u64>();
        }
        (size, bytes)
    }

    fn put(&self, key: K, value: V, evictions: &AtomicU64) {
        let mut guard = self.shard_for(&key).lock();
        if guard.len() == guard.cap().get() && guard.peek(&key).is_none() {
            evictions.fetch_add(1, Ordering::Relaxed);
        }
        guard.put(key, value);
    }

    fn clear(&self) {
        for shard in &self.shards {
            shard.lock().clear();
        }
    }
}

/// Cross-session statement AST cache, sharded by key hash (see
/// [`cache_shard_count`]). Each shard is an independent `Mutex<LruCache>`;
/// a lookup takes exactly one shard lock, so N concurrent sessions touch
/// N/8 of the previous single-mutex traffic.
///
/// The LRU is keyed by [`ParsedStatementKey`] (exact SQL bytes, byte length,
/// and parser sql-mode bits)
/// and hashed with `ahash::RandomState` — short-key SipHash was the previous
/// implicit hasher, ahash is ~2-3× faster on the typical OLTP SQL sizes
/// (50-200 bytes) and the random per-process seed costs nothing on a pure
/// in-memory LRU.
pub struct ParsedStatementCache {
    entries: ShardedLru<ParsedStatementKey, Arc<CachedStatement>>,
    epoch: Arc<CacheEpoch>,
    counters: Counters,
}

impl ParsedStatementCache {
    pub fn new(capacity: usize) -> Self {
        Self::with_epoch(capacity, Arc::new(CacheEpoch::new()))
    }

    pub fn with_epoch(capacity: usize, epoch: Arc<CacheEpoch>) -> Self {
        Self {
            entries: ShardedLru::new(capacity),
            epoch,
            counters: Counters::default(),
        }
    }

    #[cfg(test)]
    pub fn epoch_handle(&self) -> Arc<CacheEpoch> {
        Arc::clone(&self.epoch)
    }

    #[cfg(test)]
    pub fn current_epoch(&self) -> u64 {
        self.epoch.current()
    }

    /// Bump the cache epoch. All entries inserted before this call are
    /// observed as stale on subsequent lookups (lazy invalidation). Because
    /// the epoch is shared with any sibling caches (e.g. PlanCache built
    /// with the same handle), they invalidate together.
    pub fn bump_epoch(&self) -> u64 {
        self.epoch.bump()
    }

    /// Look up a cached statement. Returns `Some(ast)` only if the entry's
    /// epoch matches the cache's current epoch; stale entries are evicted on
    /// the spot.
    pub fn get(&self, key: &ParsedStatementKey) -> Option<Arc<Statement>> {
        let now = self.epoch.current();
        let mut guard = self.entries.shard_for(key).lock();
        match guard.get(key) {
            Some(entry) if entry.epoch == now => {
                let ast = Arc::clone(&entry.ast);
                drop(guard);
                self.counters.hits.fetch_add(1, Ordering::Relaxed);
                Some(ast)
            }
            Some(_) => {
                guard.pop(key);
                drop(guard);
                self.counters
                    .stale_evictions
                    .fetch_add(1, Ordering::Relaxed);
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            None => {
                drop(guard);
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert (or replace) a cached AST for `key`. Returns the inserted Arc so
    /// callers can use it directly without a follow-up `get`.
    pub fn insert(&self, key: ParsedStatementKey, ast: Arc<Statement>) -> Arc<Statement> {
        let entry = Arc::new(CachedStatement {
            ast: Arc::clone(&ast),
            epoch: self.epoch.current(),
        });
        self.entries.put(key, entry, &self.counters.lru_evictions);
        ast
    }

    /// Drop every entry. Called on `bump_epoch` paths that want eager
    /// reclamation; correctness only requires the lazy check inside `get`.
    pub fn clear(&self) {
        self.entries.clear();
    }

    fn size(&self) -> u64 {
        self.entries.size()
    }

    /// One pass over every shard computing `size` and `approx_bytes`
    /// together (both need the shard locks), so `stats()` locks each shard
    /// once instead of twice.
    fn size_and_bytes(&self) -> (u64, u64) {
        self.entries
            .size_and_bytes(|key, _| parsed_key_bytes(key) + STMT_ENTRY_OVERHEAD)
    }

    pub fn stats(&self) -> CacheStats {
        let (size, approx_bytes) = self.size_and_bytes();
        CacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            stale_evictions: self.counters.stale_evictions.load(Ordering::Relaxed),
            lru_evictions: self.counters.lru_evictions.load(Ordering::Relaxed),
            size,
            capacity: self.entries.capacity as u64,
            approx_bytes,
        }
    }
}

/// Bytes contributed by a cache key's variable-length text.
fn parsed_key_bytes(key: &ParsedStatementKey) -> u64 {
    key.sql_len as u64
}

fn key_bytes(key: &StatementKey) -> u64 {
    (key.sql.len() + key.database.len()) as u64
}

impl std::fmt::Debug for ParsedStatementCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParsedStatementCache")
            .field("capacity", &self.entries.capacity)
            .field("size", &self.size())
            .finish()
    }
}

/// Compatibility alias for existing status/API surfaces. New executor code
/// should use [`ParsedStatementCache`] to make the cache's responsibility
/// explicit.
pub type StatementCache = ParsedStatementCache;

// ---------------------------------------------------------------------------
// Plan cache
// ---------------------------------------------------------------------------

/// One base-table dependency captured at plan build time:
/// `(table_key, table_id, schema_version)`. The `table_key`
/// (`db.name`) lets validation resolve the table with a single
/// `BTreeMap` lookup — O(deps) — instead of scanning every table by id
/// (O(deps × tables)); `table_id` is still compared so a DROP + CREATE
/// that reuses the same name (and resets `schema_version` to 1) can't be
/// mistaken for the original table.
pub type PlanDep = (Arc<str>, u64, u64);

/// One non-table object a cached plan was built through:
/// `(object_key, schema_version, object_identity)` where `object_key` is the `db.name` of a
/// view expanded into the plan. Views vanish from the `PhysicalPlan` (they
/// are rewritten to derived tables before planning), so they can't be
/// captured by [`collect_plan_deps`]; instead the view-expansion choke point
/// records them via [`record_plan_object_dep`] and the plan cache validates
/// them against the live catalog on lookup. This lets ALTER / DROP VIEW
/// invalidate exactly the plans that expanded that view — not every hot
/// plan in the process.
pub type ObjectDep = (Arc<str>, u64, u64);

thread_local! {
    /// View object dependencies accumulated while expanding the current
    /// statement. Reset when a `QueryHint` is installed (once per statement),
    /// appended to by `record_plan_object_dep` during view expansion, and
    /// read by the plan-cache insert path. Thread-local because the whole
    /// parse → expand → plan → execute pipeline for one statement runs on a
    /// single `spawn_blocking` worker.
    static PLAN_OBJECT_DEPS: std::cell::RefCell<SmallVec<[ObjectDep; 2]>> =
        std::cell::RefCell::new(SmallVec::new());
}

/// Record that the currently-executing statement expanded a view. Deduplicated
/// by key; a no-op cost when the statement references no view.
pub fn record_plan_object_dep(object_key: Arc<str>, schema_version: u64, object_identity: u64) {
    PLAN_OBJECT_DEPS.with(|cell| {
        let mut deps = cell.borrow_mut();
        if !deps
            .iter()
            .any(|(k, _, _)| k.as_ref() == object_key.as_ref())
        {
            deps.push((object_key, schema_version, object_identity));
        }
    });
}

/// Clear the per-statement view-dependency accumulator (called when a new
/// `QueryHint` is installed).
fn reset_plan_object_deps() {
    PLAN_OBJECT_DEPS.with(|cell| cell.borrow_mut().clear());
}

/// Snapshot the view dependencies accumulated for the current statement.
pub fn current_plan_object_deps() -> SmallVec<[ObjectDep; 2]> {
    PLAN_OBJECT_DEPS.with(|cell| cell.borrow().clone())
}

/// Presentation-only SELECT data. It is built from the current parsed text,
/// not shared by the semantic plan cache, so expression-column casing and
/// aliases can never leak between lexically different SQL strings.
#[derive(Debug, Clone)]
pub struct SelectPresentation {
    pub projection: Arc<[SelectItem]>,
}

impl SelectPresentation {
    #[cfg(test)]
    pub fn from_select(select: &Select) -> Self {
        Self {
            projection: select.projection.clone().into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectExecutionClass {
    ReadOnlyNonBlocking,
    LockingRead,
    MayBlock,
}

impl SelectExecutionClass {
    pub fn from_select(select: &Select) -> Self {
        if select.for_update || select.lock_clause.is_some() {
            Self::LockingRead
        } else if select.derived.is_some()
            || !select.joins.is_empty()
            || select_contains_subquery(select)
            || select_has_computed_ordering(select)
        {
            Self::MayBlock
        } else {
            Self::ReadOnlyNonBlocking
        }
    }
}

/// An expression sort has to evaluate candidate rows even when its result is
/// tiny. In particular, an MVCC fallback can turn a LIMIT 1 into a complete
/// scan. It is lock-safe, but must not monopolize an async I/O worker.
pub(crate) fn select_has_computed_ordering(select: &Select) -> bool {
    select.table.is_some()
        && select.order_by.iter().any(|order| {
            !matches!(
                order.expr,
                Expr::Identifier(_) | Expr::CompoundIdentifier(_)
            )
        })
}

/// A storage read can be lock-safe and still run for an unbounded amount of
/// CPU time. Subquery evaluation is synchronous and may execute complete child
/// plans from the scalar evaluator, so keep such SELECTs off the Tokio inline
/// connection task. This preserves the listener's ability to accept the
/// control connection used by mysql's Ctrl+C / KILL QUERY path.
fn select_contains_subquery(select: &Select) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_contains_subquery(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }) || select
        .selection
        .as_ref()
        .is_some_and(expr_contains_subquery)
        || select.having.as_ref().is_some_and(expr_contains_subquery)
        || select.group_by.iter().any(expr_contains_subquery)
        || select
            .order_by
            .iter()
            .any(|order| expr_contains_subquery(&order.expr))
        || select
            .values
            .as_ref()
            .is_some_and(|values| values.rows.iter().flatten().any(expr_contains_subquery))
        || select
            .json_table
            .as_ref()
            .is_some_and(|json_table| expr_contains_subquery(&json_table.expr))
        || select.named_windows.iter().any(|window| {
            window.partition_by.iter().any(expr_contains_subquery)
                || window
                    .order_by
                    .iter()
                    .any(|order| expr_contains_subquery(&order.expr))
        })
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    crate::parser::visit::value_expr_contains_subquery(expr)
}

/// Physical operator selected once from the planned tree. The enum is kept
/// deliberately small: specialized implementations remain normal physical
/// operators, while every unrecognized tree is represented by `Generic`.
/// Execution never re-runs the old planner-before AST eligibility matrix.
/// Immutable read/decode recipe for a projected heap lookup.  This belongs to
/// the compiled physical operator (rather than a prepared handle), so text and
/// prepared executions share the same column-pruning work.
#[derive(Debug)]
pub struct CompiledProjectedRead {
    pub(crate) columns: Arc<[usize]>,
    /// Output-slot to decoded-slot remap when the SELECT projects the same
    /// source column more than once. `RowDecodePlan` intentionally decodes
    /// each physical column once; this tiny post-read recipe restores SQL
    /// projection multiplicity without putting duplicate handling in the
    /// storage hot loop.
    pub(crate) output_map: Option<Arc<[usize]>>,
    pub(crate) decode_plan: RowDecodePlan,
    pub(crate) projected_plan: ProjectedReadPlan,
}

/// Fully compiled unique-index point lookup.  The key may contain deferred
/// `KeyPart::Param` slots; storage resolves those against the borrowed
/// execution parameter context without rebuilding this recipe.
#[derive(Debug)]
pub struct ProjectedPointLookupOperator {
    pub(crate) table: Arc<TableSchema>,
    pub(crate) index: IndexSchema,
    pub(crate) key: IndexKey,
    /// Slots of ordinary index columns in the compact decoded row. Functional
    /// indexes retain the full-row key evaluator when this is absent.
    pub(crate) key_slots: Option<Arc<[usize]>>,
    /// Decode index columns only when repair/history requires a key recheck.
    /// Ordinary reads keep their original narrow projection even for wide keys.
    pub(crate) key_projection: Option<CompiledProjectedRead>,
    /// Immutable result metadata derived from the SELECT projection and
    /// catalog at compile time. Reusing it avoids repeating column-name
    /// resolution and wire flag/length/collation derivation on every execute.
    pub(crate) result_columns: Arc<[crate::types::Column]>,
    pub(crate) projection: CompiledProjectedRead,
}

#[derive(Debug)]
pub enum CompiledOperator {
    ProjectedUniquePointLookup(Arc<ProjectedPointLookupOperator>),
    ProjectedIndexRangeOrMultiLookup,
    FusedScanFilterProjectLimit,
    OrderedIndexScan,
    IndexAggregate,
    CoveringIndexAggregate,
    Generic,
}

impl CompiledOperator {
    pub fn compile(plan: &PhysicalPlan, select: &Select) -> Self {
        if let Some(operator) =
            crate::engine::executor::select::simple::compile_projected_point_lookup_operator(
                select, plan,
            )
        {
            return Self::ProjectedUniquePointLookup(Arc::new(operator));
        }

        fn classify(plan: &PhysicalPlan, projected: bool, limited: bool) -> CompiledOperator {
            match plan {
                PhysicalPlan::Projection { input, .. } => classify(input, true, limited),
                // A scalar aggregate is a presentation wrapper for the
                // existing fused single-table kernel.  The final semantic
                // shape gate below rejects grouped, HAVING and DISTINCT-
                // aggregate forms before this classification can escape.
                PhysicalPlan::HashAggregate {
                    input, group_by, ..
                } if group_by.is_empty() => classify(input, projected, limited),
                // Prepared range estimates can admit a parallel reducer even
                // when this execution binds a tiny interval. Keep the range
                // kernel available; its runtime proof and source guard only
                // bypass the parallel wrapper for a bounded single batch.
                PhysicalPlan::ParallelHashAggregate {
                    input,
                    group_by,
                    having: None,
                    ..
                } if group_by.is_empty() => match classify(input, projected, limited) {
                    CompiledOperator::ProjectedIndexRangeOrMultiLookup => {
                        CompiledOperator::ProjectedIndexRangeOrMultiLookup
                    }
                    _ => CompiledOperator::Generic,
                },
                PhysicalPlan::Limit { input, .. }
                | PhysicalPlan::TopK { input, .. }
                | PhysicalPlan::TopWindow { input, .. } => classify(input, projected, true),
                // ORDER BY / DISTINCT are presentation operators for the
                // fused single-table kernel: it implements both after the
                // projected range read.  Treating either wrapper as Generic
                // made prepared range queries fall back to PlanRow
                // materialisation plus the external-sort work-area even
                // though the leaf access itself was already eligible.
                PhysicalPlan::Sort { input, .. }
                | PhysicalPlan::StreamingDistinct { input, .. } => {
                    match classify(input, projected, limited) {
                        // Keep this promotion deliberately tied to the bounded
                        // index-id-list kernel.  Fused full scans have additional
                        // dynamic comparison scopes (JSON/string is one example)
                        // that the generic executor must continue to install.
                        CompiledOperator::ProjectedIndexRangeOrMultiLookup => {
                            CompiledOperator::ProjectedIndexRangeOrMultiLookup
                        }
                        _ => CompiledOperator::Generic,
                    }
                }
                PhysicalPlan::Filter { input, .. } => classify(input, projected, limited),
                PhysicalPlan::IndexMultiLookup { table, .. }
                | PhysicalPlan::IndexInLookup { table, .. }
                | PhysicalPlan::IndexRangeScan { table, .. }
                    if !table.schema.is_partitioned() =>
                {
                    CompiledOperator::ProjectedIndexRangeOrMultiLookup
                }
                PhysicalPlan::IndexFastScan { table, reverse, .. }
                    if !table.schema.is_partitioned() && *reverse =>
                {
                    CompiledOperator::OrderedIndexScan
                }
                PhysicalPlan::IndexAggregate { .. } => CompiledOperator::IndexAggregate,
                PhysicalPlan::CoveringIndexAggregate { .. }
                | PhysicalPlan::ParallelIndexAggregate { .. } => {
                    CompiledOperator::CoveringIndexAggregate
                }
                PhysicalPlan::SeqScan { .. } | PhysicalPlan::ZoneMapScan { .. }
                    if projected || limited =>
                {
                    CompiledOperator::FusedScanFilterProjectLimit
                }
                _ => CompiledOperator::Generic,
            }
        }
        let operator = classify(plan, false, false);
        match operator {
            CompiledOperator::ProjectedIndexRangeOrMultiLookup
            | CompiledOperator::FusedScanFilterProjectLimit
            | CompiledOperator::OrderedIndexScan
                if !crate::engine::executor::select::simple::fused_single_table_shape_supported(
                    select,
                ) =>
            {
                CompiledOperator::Generic
            }
            operator => operator,
        }
    }
}

/// Immutable semantic SELECT artifact shared by text and prepared execution.
/// Cache lookups validate every dependency against the live catalog; a single
/// mismatch evicts the entry lazily.
#[derive(Debug)]
pub struct CompiledSelectPlan {
    pub semantic_select: Arc<Select>,
    pub physical: Arc<PhysicalPlan>,
    pub executable: Arc<CompiledOperator>,
    pub(crate) compact_range:
        Option<Arc<crate::engine::executor::select::simple::CompactRangeRecipe>>,
    pub deps: SmallVec<[PlanDep; 4]>,
    /// Views expanded into this plan; validated against the live catalog on
    /// lookup so ALTER / DROP VIEW invalidates exactly these plans.
    pub object_deps: SmallVec<[ObjectDep; 2]>,
    pub epoch: u64,
    /// Cost-model version captured when this physical plan was built.
    pub cbo_epoch: u64,
    pub execution_class: SelectExecutionClass,
    /// Metadata-only decision that this immutable physical/projection shape
    /// can use the common BatchOperator graph. Runtime streams and transaction
    /// state are still constructed per execution.
    pub(crate) batch_route: bool,
    /// Reuse the no-I/O output-capability proof as well as the route decision.
    /// Errors are deliberately not cached: execution retains canonical error timing.
    pub(crate) native_output_supported: Option<bool>,
    /// Immutable execution-envelope recipes. They are initialized by the
    /// first runner that has session/catalog binding context (normally the
    /// compiling prepared handle) and then shared by every handle reusing the
    /// semantic plan. `OnceLock` keeps cache insertion independent of the
    /// protocol/session layer while preserving compile-once behavior.
    required_privileges: OnceLock<Arc<[RequiredPrivilege]>>,
    mdl_requests: OnceLock<Arc<[MdlRequest]>>,
    /// Per-entry hit counter, bumped on each cache hit. Powers the
    /// per-plan `HITS` column of `information_schema.mydb_plan_cache`.
    pub hits: AtomicU64,
}

impl CompiledSelectPlan {
    pub fn new(
        semantic_select: Arc<Select>,
        physical: Arc<PhysicalPlan>,
        deps: SmallVec<[PlanDep; 4]>,
        object_deps: SmallVec<[ObjectDep; 2]>,
        epoch: u64,
    ) -> Self {
        let batch_route = crate::engine::executor::select::vec_bridge::should_route_result_to_batch(
            &physical,
            &semantic_select.projection,
        );
        Self {
            execution_class: SelectExecutionClass::from_select(&semantic_select),
            batch_route,
            native_output_supported:
                crate::engine::executor::select::vec_bridge::preflight_query_output_supported(
                    &physical,
                )
                .ok(),
            executable: Arc::new(CompiledOperator::compile(&physical, &semantic_select)),
            compact_range: crate::engine::executor::select::simple::compile_compact_range_recipe(
                &semantic_select,
                &physical,
            ),
            semantic_select,
            physical,
            deps,
            object_deps,
            epoch,
            cbo_epoch: crate::engine::cbo::cbo_epoch(),
            required_privileges: OnceLock::new(),
            mdl_requests: OnceLock::new(),
            hits: AtomicU64::new(0),
        }
    }

    pub fn initialize_execution_envelope(
        &self,
        required_privileges: Arc<[RequiredPrivilege]>,
        mdl_requests: Arc<[MdlRequest]>,
    ) {
        let _ = self.required_privileges.set(required_privileges);
        let _ = self.mdl_requests.set(mdl_requests);
    }

    pub fn required_privileges(&self) -> &[RequiredPrivilege] {
        self.required_privileges.get().map_or(&[], Arc::as_ref)
    }

    pub fn mdl_requests(&self) -> &[MdlRequest] {
        self.mdl_requests.get().map_or(&[], Arc::as_ref)
    }
}

/// One snapshot row for `information_schema.mydb_plan_cache`.
#[derive(Debug, Clone)]
pub struct PlanCacheEntryInfo {
    /// Normalized SQL text, or the `ap:<hex>` auto-parameterization
    /// fingerprint the entry is keyed by.
    pub sql: Arc<str>,
    pub database: Arc<str>,
    pub sql_mode_fingerprint: u64,
    pub hits: u64,
    pub deps: usize,
    pub approx_bytes: u64,
    pub epoch: u64,
}

#[derive(Debug, Default)]
pub struct PlanCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub stale_evictions: u64,
    pub deps_invalidations: u64,
    pub lru_evictions: u64,
    pub size: u64,
    pub capacity: u64,
    /// Approximate resident bytes — see [`CacheStats::approx_bytes`].
    pub approx_bytes: u64,
}

#[derive(Debug, Default)]
struct PlanCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    stale_evictions: AtomicU64,
    deps_invalidations: AtomicU64,
    lru_evictions: AtomicU64,
}

/// Cross-session compiled SELECT cache. Keyed by structural SQL + database
/// name, value is `Arc<CompiledSelectPlan>`. Deferred parameter bindings in
/// physical keys make the same artifact reusable by COM_QUERY auto-params and
/// prepared placeholders.
///
/// Sharded by key hash exactly like [`ParsedStatementCache`] (8 shards once the
/// capacity is production-sized); uses `ahash::RandomState` for both the
/// router and the per-shard maps.
pub struct PlanCache {
    entries: ShardedLru<StatementKey, Arc<CompiledSelectPlan>>,
    epoch: Arc<CacheEpoch>,
    counters: PlanCounters,
}

impl PlanCache {
    pub fn new(capacity: usize) -> Self {
        Self::with_epoch(capacity, Arc::new(CacheEpoch::new()))
    }

    pub fn with_epoch(capacity: usize, epoch: Arc<CacheEpoch>) -> Self {
        Self {
            entries: ShardedLru::new(capacity),
            epoch,
            counters: PlanCounters::default(),
        }
    }

    pub fn epoch_handle(&self) -> Arc<CacheEpoch> {
        Arc::clone(&self.epoch)
    }

    pub fn bump_epoch(&self) -> u64 {
        self.epoch.bump()
    }

    pub fn current_epoch(&self) -> u64 {
        self.epoch.current()
    }

    /// Look up a cached plan. The caller passes a closure that validates
    /// every (table_id, expected_version) pair against the live catalog;
    /// returning false for any pair drops the entry.
    pub fn get<F>(&self, key: &StatementKey, validate_deps: F) -> Option<Arc<CompiledSelectPlan>>
    where
        F: FnOnce(&[PlanDep], &[ObjectDep]) -> bool,
    {
        self.get_validated(key, validate_deps, |_| true)
    }

    fn get_validated<F, S>(
        &self,
        key: &StatementKey,
        validate_deps: F,
        validate_select: S,
    ) -> Option<Arc<CompiledSelectPlan>>
    where
        F: FnOnce(&[PlanDep], &[ObjectDep]) -> bool,
        S: FnOnce(&Select) -> bool,
    {
        let now = self.epoch.current();
        let shard = self.entries.shard_for(key);
        let entry = {
            let mut guard = shard.lock();
            guard.get(key).cloned()
        };
        let cbo_now = crate::engine::cbo::cbo_epoch();
        match entry {
            Some(cached) if cached.epoch != now || cached.cbo_epoch != cbo_now => {
                shard.lock().pop(key);
                self.counters
                    .stale_evictions
                    .fetch_add(1, Ordering::Relaxed);
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Some(cached) => {
                if validate_deps(&cached.deps, &cached.object_deps) {
                    if !validate_select(&cached.semantic_select) {
                        shard.lock().pop(key);
                        self.counters.misses.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                    self.counters.hits.fetch_add(1, Ordering::Relaxed);
                    cached.hits.fetch_add(1, Ordering::Relaxed);
                    Some(cached)
                } else {
                    shard.lock().pop(key);
                    self.counters
                        .deps_invalidations
                        .fetch_add(1, Ordering::Relaxed);
                    self.counters.misses.fetch_add(1, Ordering::Relaxed);
                    None
                }
            }
            None => {
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// SELECT lookup with a full semantic-AST collision guard. Hash routing
    /// narrows the candidate; equality decides the hit. A mismatching AST is
    /// evicted so a synthetic fingerprint collision can never execute the
    /// wrong physical plan.
    pub fn get_select<F>(
        &self,
        key: &StatementKey,
        semantic_select: &Select,
        validate_deps: F,
    ) -> Option<Arc<CompiledSelectPlan>>
    where
        F: FnOnce(&[PlanDep], &[ObjectDep]) -> bool,
    {
        self.get_validated(key, validate_deps, |cached| {
            if cached == semantic_select {
                return true;
            }
            let mut cached_canonical = cached.clone();
            let mut requested_canonical = semantic_select.clone();
            crate::parser::canonicalize_select_for_plan_cache(&mut cached_canonical);
            crate::parser::canonicalize_select_for_plan_cache(&mut requested_canonical);
            cached_canonical == requested_canonical
        })
    }

    pub fn insert(
        &self,
        key: StatementKey,
        semantic_select: Arc<Select>,
        physical: Arc<PhysicalPlan>,
        deps: SmallVec<[PlanDep; 4]>,
        object_deps: SmallVec<[ObjectDep; 2]>,
    ) -> Arc<CompiledSelectPlan> {
        let entry = Arc::new(CompiledSelectPlan::new(
            semantic_select,
            physical,
            deps,
            object_deps,
            self.epoch.current(),
        ));
        self.entries
            .put(key, Arc::clone(&entry), &self.counters.lru_evictions);
        entry
    }

    pub fn clear(&self) {
        self.entries.clear();
    }

    /// Eagerly evict only plans that depend on `table_id`. Zone Map rebuilds
    /// change access-path selectivity without changing the table schema, so
    /// the normal schema-version validator cannot detect them; clearing the
    /// whole process cache would unnecessarily chill unrelated hot plans.
    pub fn invalidate_table(&self, table_id: u64) -> u64 {
        let mut evicted = 0u64;
        for shard in self.entries.shards.iter() {
            let mut guard = shard.lock();
            let keys = guard
                .iter()
                .filter(|(_, entry)| entry.deps.iter().any(|(_, id, _)| *id == table_id))
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in keys {
                if guard.pop(&key).is_some() {
                    evicted += 1;
                }
            }
        }
        if evicted > 0 {
            self.counters
                .deps_invalidations
                .fetch_add(evicted, Ordering::Relaxed);
        }
        evicted
    }

    fn size(&self) -> u64 {
        self.entries.size()
    }

    fn size_and_bytes(&self) -> (u64, u64) {
        self.entries.size_and_bytes(|key, entry| {
            key_bytes(key)
                + PLAN_ENTRY_OVERHEAD
                + (entry.deps.len() as u64) * std::mem::size_of::<PlanDep>() as u64
        })
    }

    pub fn stats(&self) -> PlanCacheStats {
        let (size, approx_bytes) = self.size_and_bytes();
        PlanCacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            stale_evictions: self.counters.stale_evictions.load(Ordering::Relaxed),
            deps_invalidations: self.counters.deps_invalidations.load(Ordering::Relaxed),
            lru_evictions: self.counters.lru_evictions.load(Ordering::Relaxed),
            size,
            capacity: self.entries.capacity as u64,
            approx_bytes,
        }
    }

    /// One [`PlanCacheEntryInfo`] per resident entry, for
    /// `information_schema.mydb_plan_cache`. Snapshots under each shard lock;
    /// not a hot path (driven by an explicit introspection query).
    pub fn snapshot(&self) -> Vec<PlanCacheEntryInfo> {
        let mut out = Vec::new();
        for shard in self.entries.shards.iter() {
            let guard = shard.lock();
            for (key, entry) in guard.iter() {
                let approx_bytes = key_bytes(key)
                    + PLAN_ENTRY_OVERHEAD
                    + (entry.deps.len() as u64) * std::mem::size_of::<PlanDep>() as u64;
                out.push(PlanCacheEntryInfo {
                    sql: Arc::clone(&key.sql),
                    database: Arc::clone(&key.database),
                    sql_mode_fingerprint: key.semantics.sql_mode_fingerprint,
                    hits: entry.hits.load(Ordering::Relaxed),
                    deps: entry.deps.len(),
                    approx_bytes,
                    epoch: entry.epoch,
                });
            }
        }
        out
    }
}

impl std::fmt::Debug for PlanCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanCache")
            .field("capacity", &self.entries.capacity)
            .field("size", &self.size())
            .finish()
    }
}

#[derive(Debug)]
pub struct PreparedDmlBatch {
    pub plans: Arc<[Arc<PreparedDmlPlan>]>,
    pub param_counts: Arc<[usize]>,
    pub epoch: u64,
}

// ---------------------------------------------------------------------------
// Prepared-DML plan cache
// ---------------------------------------------------------------------------

/// Common lifecycle for epoch-validated prepared DML memos. Validation runs
/// outside the shard lock, preserving the existing dependency-check contract.
trait PreparedDmlCacheEntry {
    fn epoch(&self) -> u64;
    fn approx_bytes(&self) -> u64;
}

impl PreparedDmlCacheEntry for PreparedDmlPlan {
    fn epoch(&self) -> u64 {
        self.epoch
    }
    fn approx_bytes(&self) -> u64 {
        DML_ENTRY_OVERHEAD
    }
}

impl PreparedDmlCacheEntry for PreparedDmlBatch {
    fn epoch(&self) -> u64 {
        self.epoch
    }
    fn approx_bytes(&self) -> u64 {
        self.plans.len().max(1) as u64 * DML_ENTRY_OVERHEAD
    }
}

struct EpochDmlCache<T> {
    shards: Box<[Mutex<LruCache<StatementKey, Arc<T>, AhashState>>]>,
    router: AhashState,
    capacity: usize,
    epoch: Arc<CacheEpoch>,
    counters: PlanCounters,
}

impl<T: PreparedDmlCacheEntry> EpochDmlCache<T> {
    pub fn with_epoch(capacity: usize, epoch: Arc<CacheEpoch>) -> Self {
        let capacity = capacity.max(1);
        let shard_count = cache_shard_count(capacity);
        let shards = (0..shard_count)
            .map(|index| {
                let capacity = capacity / shard_count + usize::from(index < capacity % shard_count);
                let cap = NonZeroUsize::new(capacity).unwrap();
                Mutex::new(LruCache::with_hasher(cap, AhashState::new()))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            router: AhashState::new(),
            capacity,
            epoch,
            counters: PlanCounters::default(),
        }
    }

    fn shard_for(&self, key: &StatementKey) -> &Mutex<LruCache<StatementKey, Arc<T>, AhashState>> {
        let h = self.router.hash_one(key) as usize;
        &self.shards[h % self.shards.len()]
    }

    pub fn current_epoch(&self) -> u64 {
        self.epoch.current()
    }

    pub fn get<F>(&self, key: &StatementKey, validate: F) -> Option<Arc<T>>
    where
        F: FnOnce(&T) -> bool,
    {
        let now = self.epoch.current();
        let shard = self.shard_for(key);
        let entry = {
            let mut guard = shard.lock();
            guard.get(key).cloned()
        };
        match entry {
            Some(plan) if plan.epoch() != now => {
                shard.lock().pop(key);
                self.counters
                    .stale_evictions
                    .fetch_add(1, Ordering::Relaxed);
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Some(plan) => {
                if validate(&plan) {
                    self.counters.hits.fetch_add(1, Ordering::Relaxed);
                    Some(plan)
                } else {
                    shard.lock().pop(key);
                    self.counters
                        .deps_invalidations
                        .fetch_add(1, Ordering::Relaxed);
                    self.counters.misses.fetch_add(1, Ordering::Relaxed);
                    None
                }
            }
            None => {
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    fn insert_entry(&self, key: StatementKey, plan: Arc<T>) -> Arc<T> {
        let mut guard = self.shard_for(&key).lock();
        if guard.len() == guard.cap().get() && guard.peek(&key).is_none() {
            self.counters.lru_evictions.fetch_add(1, Ordering::Relaxed);
        }
        guard.put(key, Arc::clone(&plan));
        plan
    }

    pub fn clear(&self) {
        for shard in self.shards.iter() {
            shard.lock().clear();
        }
    }

    fn size_and_bytes(&self) -> (u64, u64) {
        let mut size = 0u64;
        let mut bytes = 0u64;
        for shard in self.shards.iter() {
            let guard = shard.lock();
            size += guard.len() as u64;
            for (key, entry) in guard.iter() {
                bytes += key_bytes(key) + entry.approx_bytes();
            }
        }
        (size, bytes)
    }

    pub fn stats(&self) -> PlanCacheStats {
        let (size, approx_bytes) = self.size_and_bytes();
        PlanCacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            stale_evictions: self.counters.stale_evictions.load(Ordering::Relaxed),
            deps_invalidations: self.counters.deps_invalidations.load(Ordering::Relaxed),
            lru_evictions: self.counters.lru_evictions.load(Ordering::Relaxed),
            size,
            capacity: self.capacity as u64,
            approx_bytes,
        }
    }
}

// Keep each cache's public entry type and insertion policy explicit while
// sharing the same shard, epoch, eviction, validation and statistics lifecycle.
macro_rules! define_prepared_dml_cache {
    ($cache:ident, $entry:ty) => {
        pub struct $cache {
            inner: EpochDmlCache<$entry>,
        }
        impl $cache {
            pub fn with_epoch(capacity: usize, epoch: Arc<CacheEpoch>) -> Self {
                Self {
                    inner: EpochDmlCache::with_epoch(capacity, epoch),
                }
            }
            pub fn current_epoch(&self) -> u64 {
                self.inner.current_epoch()
            }
            pub fn get<F>(&self, key: &StatementKey, validate: F) -> Option<Arc<$entry>>
            where
                F: FnOnce(&$entry) -> bool,
            {
                self.inner.get(key, validate)
            }
            pub fn clear(&self) {
                self.inner.clear();
            }
            pub fn stats(&self) -> PlanCacheStats {
                self.inner.stats()
            }
        }
        impl std::fmt::Debug for $cache {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($cache))
                    .field("capacity", &self.inner.capacity)
                    .field("size", &self.inner.size_and_bytes().0)
                    .finish()
            }
        }
    };
}

define_prepared_dml_cache!(PreparedDmlPlanCache, PreparedDmlPlan);
define_prepared_dml_cache!(PreparedDmlBatchCache, PreparedDmlBatch);

impl PreparedDmlPlanCache {
    pub fn insert(&self, key: StatementKey, plan: Arc<PreparedDmlPlan>) -> Arc<PreparedDmlPlan> {
        self.inner.insert_entry(key, plan)
    }
}

impl PreparedDmlBatchCache {
    pub fn insert(
        &self,
        key: StatementKey,
        plans: Vec<Arc<PreparedDmlPlan>>,
        param_counts: Vec<usize>,
    ) -> Arc<PreparedDmlBatch> {
        self.inner.insert_entry(
            key,
            Arc::new(PreparedDmlBatch {
                plans: plans.into(),
                param_counts: param_counts.into(),
                epoch: self.current_epoch(),
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_audit_shards_respect_exact_capacity() {
        for capacity in [1, 255, 256, 257, 511, 512, 1025] {
            let epoch = Arc::new(CacheEpoch::new());
            let statement = ParsedStatementCache::new(capacity);
            let plan = PlanCache::new(capacity);
            let dml = PreparedDmlPlanCache::with_epoch(capacity, epoch.clone());
            let batch = PreparedDmlBatchCache::with_epoch(capacity, epoch);
            assert_eq!(
                statement
                    .entries
                    .shards
                    .iter()
                    .map(|s| s.lock().cap().get())
                    .sum::<usize>(),
                capacity
            );
            assert_eq!(
                plan.entries
                    .shards
                    .iter()
                    .map(|s| s.lock().cap().get())
                    .sum::<usize>(),
                capacity
            );
            assert_eq!(
                dml.inner
                    .shards
                    .iter()
                    .map(|s| s.lock().cap().get())
                    .sum::<usize>(),
                capacity
            );
            assert_eq!(
                batch
                    .inner
                    .shards
                    .iter()
                    .map(|s| s.lock().cap().get())
                    .sum::<usize>(),
                capacity
            );
        }
    }

    #[test]
    fn pressure_reclaim_clears_registered_statement_cache() {
        let cache = Arc::new(ParsedStatementCache::new(8));
        register_global(&cache);
        let sql = "SELECT 1";
        let statement = crate::parser::parse_statement(sql).unwrap();
        cache.insert(ParsedStatementKey::new(sql, 0), Arc::new(statement));
        assert_eq!(cache.stats().size, 1);
        reclaim_global_caches();
        assert_eq!(cache.stats().size, 0);
    }
    use crate::parser::parse_statement;

    fn parsed_key(sql: &str) -> ParsedStatementKey {
        ParsedStatementKey::new(sql, 0)
    }

    fn key(sql: &str, db: &str) -> StatementKey {
        StatementKey {
            sql: Arc::<str>::from(sql),
            database: Arc::<str>::from(db),
            semantics: PlanSemantics::default(),
        }
    }

    #[test]
    fn subquery_selects_do_not_run_on_tokio_inline_path() {
        for sql in [
            "SELECT (SELECT MAX(id) FROM t) FROM t",
            "SELECT id FROM t WHERE id IN (SELECT id FROM u)",
            "SELECT id FROM t WHERE id >= (SELECT RAND() * (SELECT MAX(id) FROM u))",
            "SELECT id FROM t ORDER BY RAND() LIMIT 1",
            "SELECT id FROM t ORDER BY RAND(7) LIMIT 2",
            "SELECT id FROM t ORDER BY id + 1 LIMIT 1",
            "SELECT name FROM t ORDER BY LOWER(name) LIMIT 1",
        ] {
            let Statement::Select(select) = parse_statement(sql).unwrap() else {
                panic!("expected SELECT: {sql}");
            };
            assert_eq!(
                SelectExecutionClass::from_select(&select),
                SelectExecutionClass::MayBlock,
                "synchronous scan work must stay off the I/O worker: {sql}"
            );
        }

        let Statement::Select(point_read) =
            parse_statement("SELECT id FROM t WHERE id = 1").unwrap()
        else {
            unreachable!()
        };
        assert_eq!(
            SelectExecutionClass::from_select(&point_read),
            SelectExecutionClass::ReadOnlyNonBlocking
        );
    }

    #[test]
    fn miss_then_hit() {
        let cache = ParsedStatementCache::new(8);
        let k = parsed_key("SELECT 1");
        assert!(cache.get(&k).is_none());

        let ast = Arc::new(parse_statement("SELECT 1").unwrap());
        cache.insert(k.clone(), Arc::clone(&ast));

        let got = cache.get(&k).expect("hit after insert");
        assert!(Arc::ptr_eq(&got, &ast), "must reuse Arc, not clone");
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn epoch_bump_invalidates() {
        let cache = ParsedStatementCache::new(8);
        let k = parsed_key("SELECT 2");
        let ast = Arc::new(parse_statement("SELECT 2").unwrap());
        cache.insert(k.clone(), ast);

        cache.bump_epoch();

        assert!(cache.get(&k).is_none(), "post-DDL lookup must miss");
        let stats = cache.stats();
        assert!(stats.stale_evictions >= 1);
    }

    #[test]
    fn parser_sql_mode_scopes_keys() {
        let cache = ParsedStatementCache::new(8);
        let a = ParsedStatementKey::new("SELECT * FROM t", 0);
        let b = ParsedStatementKey::new("SELECT * FROM t", 1);
        let ast_a = Arc::new(parse_statement("SELECT * FROM t").unwrap());
        let ast_b = Arc::new(parse_statement("SELECT * FROM t").unwrap());
        cache.insert(a.clone(), Arc::clone(&ast_a));
        cache.insert(b.clone(), Arc::clone(&ast_b));

        let got_a = cache.get(&a).unwrap();
        let got_b = cache.get(&b).unwrap();
        assert!(Arc::ptr_eq(&got_a, &ast_a));
        assert!(Arc::ptr_eq(&got_b, &ast_b));
    }

    #[test]
    fn lru_eviction_counted() {
        let cache = ParsedStatementCache::new(2);
        for i in 0..3 {
            let sql = format!("SELECT {i}");
            let ast = Arc::new(parse_statement(&sql).unwrap());
            cache.insert(parsed_key(&sql), ast);
        }
        let stats = cache.stats();
        assert_eq!(stats.size, 2);
        assert!(stats.lru_evictions >= 1);
    }

    #[test]
    fn plan_cache_basic_hit_with_dep_match() {
        use crate::planner::{PhysicalPlan, PlanTable};
        use crate::storage::TableSchema;
        use std::collections::{BTreeMap, HashMap};

        let table = Arc::new(TableSchema {
            id: 7,
            object_id: 7,
            active_generation: 1,
            database: "test".into(),
            name: "t".into(),
            engine: "mydb".into(),
            charset: "utf8mb4".into(),
            collation: crate::charset::DEFAULT_COLLATION.into(),
            comment: String::new(),
            created_ms: 0,
            updated_ms: 0,
            table_type: "BASE TABLE".into(),
            columns: Vec::new(),
            primary_key: Vec::new(),
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            fk_referenced_by: Vec::new(),
            stats: crate::storage::TableStats {
                analyze_revision: 0,
                explicit_histograms: std::collections::BTreeMap::new(),
                row_count: 0,
                heap_page_count: 0,
                sample_fraction: None,
                confidence: None,
                columns: BTreeMap::new(),
                indexes: BTreeMap::new(),
                partition_stats: BTreeMap::new(),
                conditional_key_domains: Vec::new(),
                multi_column_stats: std::collections::BTreeMap::new(),
                modified_rows_since_analyze: 0,
                analyzed_at_unix_ms: Some(0),
                last_analyze_live_baseline: 0,
                last_analyze_modification_baseline: 0,
            },
            system: false,
            schema_version: 3,
            system_time_epoch_us: 0,
            row_generation_txn_id: 0,
            next_column_id: 1,
            active_layout_id: 1,
            row_layouts: Vec::new(),
            pending_primary_key: None,
            primary_definition_scn: 0,
            next_index_id: 0,
            primary_publication_job_id: 0,
            column_name_map: Default::default(),
            column_id_map: HashMap::new(),
            row_codec: Default::default(),
            partitioning: None,
            pctfree: 0,
            pctused: 0,
            mysql_table_options: Vec::new(),
        });
        let plan = Arc::new(PhysicalPlan::SeqScan {
            table: PlanTable::new(7, Arc::clone(&table), None),
            filter: None,
            estimated_rows: 0.0,
            access_rows: 0.0,
            cost: crate::engine::cbo::Cost::ZERO,
            possible_keys: Vec::new(),
            runtime_filters: Vec::new(),
            hint_note: None,
            projected_columns: None,
        });

        let cache = PlanCache::new(8);
        let k = key("SELECT * FROM t", "test");
        let mut deps = SmallVec::<[PlanDep; 4]>::new();
        deps.push((Arc::<str>::from("test.t"), 7, 3));
        let Statement::Select(select) = parse_statement("SELECT * FROM t").unwrap() else {
            unreachable!()
        };
        let select = Arc::new(select);
        cache.insert(
            k.clone(),
            Arc::clone(&select),
            Arc::clone(&plan),
            deps.clone(),
            SmallVec::new(),
        );

        let hit = cache
            .get(&k, |entry_deps, _obj| {
                entry_deps.iter().all(|(_, _, v)| *v == 3)
            })
            .expect("should hit when deps match");
        assert!(Arc::ptr_eq(&hit.physical, &plan));
        let Statement::Select(case_variant) = parse_statement("select * from T").unwrap() else {
            unreachable!()
        };
        assert!(
            cache.get_select(&k, &case_variant, |_, _| true).is_some(),
            "identifier casing is presentation, not a semantic collision"
        );

        let prior = crate::engine::cbo::cbo_consts().rnd_page_cost();
        crate::engine::cbo::set_variable("cbo_rnd_page_cost", prior + 0.25).unwrap();
        assert!(
            cache.get(&k, |_, _| true).is_none(),
            "a cost-model change must invalidate an older physical plan"
        );
        crate::engine::cbo::set_variable("cbo_rnd_page_cost", prior).unwrap();
        cache.insert(
            k.clone(),
            Arc::clone(&select),
            Arc::clone(&plan),
            deps.clone(),
            SmallVec::new(),
        );

        let miss = cache.get(&k, |entry_deps, _obj| {
            entry_deps.iter().all(|(_, _, v)| *v == 4)
        });
        assert!(miss.is_none(), "stale schema_version evicts entry");
        let stats = cache.stats();
        assert!(stats.deps_invalidations >= 1);

        let Statement::Select(literal_upper) =
            parse_statement("SELECT LENGTH('A') FROM t").unwrap()
        else {
            unreachable!()
        };
        let Statement::Select(literal_lower) =
            parse_statement("SELECT LENGTH('a') FROM t").unwrap()
        else {
            unreachable!()
        };
        cache.insert(
            k.clone(),
            Arc::new(literal_upper),
            Arc::clone(&plan),
            deps.clone(),
            SmallVec::new(),
        );
        let before_collision = cache.stats();
        assert!(
            cache.get_select(&k, &literal_lower, |_, _| true).is_none(),
            "case-sensitive literal differences must reject a synthetic key collision"
        );
        assert_eq!(cache.stats().hits, before_collision.hits);
        assert_eq!(cache.stats().misses, before_collision.misses + 1);

        cache.insert(k.clone(), select, plan, deps, SmallVec::new());
        assert_eq!(cache.invalidate_table(7), 1);
        assert!(cache.get(&k, |_, _| true).is_none());
    }

    #[test]
    fn parsed_and_compiled_cache_epochs_are_independent() {
        let parsed = ParsedStatementCache::new(4);
        let plan = PlanCache::new(4);
        let before_parsed = parsed.current_epoch();
        plan.bump_epoch();
        assert_eq!(parsed.current_epoch(), before_parsed);

        let dml = PreparedDmlPlanCache::with_epoch(4, plan.epoch_handle());
        let before_dml = dml.current_epoch();
        plan.bump_epoch();
        assert_eq!(dml.current_epoch(), before_dml + 1);
    }

    #[test]
    fn concurrent_get_insert_stress() {
        // Finish fixed lookup and epoch counts after every worker is ready;
        // parser startup and scheduler delays must not erase the exercise.
        const WORKERS: usize = 4;
        const ROUNDS: usize = 50;
        let cache = Arc::new(ParsedStatementCache::new(64));
        let phases = Arc::new(std::sync::Barrier::new(WORKERS + 1));
        let initial_epoch = cache.current_epoch();

        let batches = (0..WORKERS)
            .map(|worker| {
                (0..16)
                    .map(|i| {
                        let sql = format!("SELECT {i} FROM t WHERE c = {worker}");
                        let ast = Arc::new(parse_statement(&sql).unwrap());
                        (sql, ast)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut handles = Vec::new();
        for batch in batches {
            let cache = Arc::clone(&cache);
            let phases = Arc::clone(&phases);
            handles.push(std::thread::spawn(move || {
                phases.wait();
                for _ in 0..ROUNDS {
                    for (sql, ast) in &batch {
                        let k = parsed_key(sql);
                        match cache.get(&k) {
                            Some(got) => {
                                assert!(Arc::ptr_eq(&got, ast));
                            }
                            None => {
                                cache.insert(k, Arc::clone(ast));
                            }
                        }
                    }
                }
            }));
        }

        let cache_ddl = Arc::clone(&cache);
        let phases_ddl = Arc::clone(&phases);
        handles.push(std::thread::spawn(move || {
            phases_ddl.wait();
            for _ in 0..ROUNDS {
                cache_ddl.bump_epoch();
                std::thread::yield_now();
            }
        }));

        for h in handles {
            h.join().expect("worker panicked");
        }

        let stats = cache.stats();
        assert_eq!(stats.hits + stats.misses, (WORKERS * ROUNDS * 16) as u64);
        assert_eq!(cache.current_epoch(), initial_epoch + ROUNDS as u64);
        assert!(stats.size <= 64, "size respects capacity");
    }
}
