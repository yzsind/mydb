//! Catalog-aware conservative dependency hints for scheduling and redaction.
use crate::storage::Database;
use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock, RwLock,
    },
};
struct Entry {
    epoch: u64,
    names: Arc<BTreeSet<String>>,
    trigger_tables: Arc<BTreeSet<String>>,
    owner: std::sync::Weak<Database>,
}
static CACHE: OnceLock<RwLock<HashMap<usize, Entry>>> = OnceLock::new();
/// Bumped under the cache write lock after every change to [`CACHE`], so a
/// [`SensitivityMemo`] can tell whether its dependency-name verdict is stale.
static GENERATION: AtomicU64 = AtomicU64::new(0);
/// Changes whenever [`sensitive`]'s dependency-name verdicts may change.
pub(crate) fn dependency_generation() -> u64 {
    GENERATION.load(Ordering::Acquire)
}
fn cache() -> &'static RwLock<HashMap<usize, Entry>> {
    CACHE.get_or_init(Default::default)
}

/// Redaction verdict for one immutable SQL text, equal to
/// `sql_may_have_ai(sql) || sensitive(sql)`. The text-only scans run once at
/// construction; the dependency-name part is recomputed only after the
/// catalog-derived cache changes. Prepared statements publish their text to
/// the process list and performance_schema on every EXECUTE, so rescanning an
/// unchanged statement there was pure per-execution overhead.
#[derive(Debug)]
pub(crate) struct SensitivityMemo {
    text_sensitive: bool,
    /// `(generation << 1) | verdict`; `u64::MAX` until first computed.
    dependency_verdict: AtomicU64,
}

impl SensitivityMemo {
    pub(crate) fn new(sql: &str) -> Self {
        Self {
            text_sensitive: crate::engine::ai::sql::sql_may_have_ai(sql) || super::mentions(sql),
            dependency_verdict: AtomicU64::new(u64::MAX),
        }
    }

    /// `sql` must be the text this memo was built from.
    pub(crate) fn is_sensitive(&self, sql: &str) -> bool {
        if self.text_sensitive {
            return true;
        }
        // Load the generation before reading the cache: a concurrent change
        // then leaves a stale generation behind and forces a recomputation.
        let generation = GENERATION.load(Ordering::Acquire);
        let cached = self.dependency_verdict.load(Ordering::Acquire);
        if cached != u64::MAX && cached >> 1 == generation {
            return cached & 1 == 1;
        }
        let verdict = dependency_sensitive(sql);
        self.dependency_verdict
            .store((generation << 1) | u64::from(verdict), Ordering::Release);
        verdict
    }
}
fn contains(sql: &str, names: &BTreeSet<String>) -> bool {
    if names.is_empty() {
        return false;
    }
    // False positives inside literals are deliberate: they redact more and
    // route one query away from the ordinary worker pool without executing it.
    if sql
        .split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
        .any(|word| names.contains(&word.to_ascii_lowercase()))
    {
        return true;
    }
    // Quoted identifiers can contain spaces, punctuation, and escaped quotes.
    // Search their complete spellings conservatively, including SQL escapes.
    names
        .iter()
        .filter(|name| {
            name.chars()
                .any(|c| !(c.is_alphanumeric() || c == '_' || c == '$'))
        })
        .any(|name| {
            [
                name.clone(),
                name.replace('`', "``"),
                name.replace('"', "\"\""),
            ]
            .iter()
            .any(|spelling| {
                !spelling.is_empty()
                    && sql
                        .as_bytes()
                        .windows(spelling.len())
                        .any(|w| w.eq_ignore_ascii_case(spelling.as_bytes()))
            })
        })
}
fn has_keyword(sql: &str, keywords: &[&str]) -> bool {
    use crate::sql_lex::{segments, SqlLexMode, SqlSegmentKind};
    // Scheduling runs before a parser-mode guard is installed. Inspect both
    // escape interpretations conservatively, without treating quoted text or
    // ordinary comments as executable DML.
    for no_backslash_escapes in [false, true] {
        let mode = SqlLexMode {
            no_backslash_escapes,
            ..SqlLexMode::default()
        };
        for segment in segments(sql, mode, u32::MAX) {
            let segment = match segment {
                Ok(s) => s,
                Err(_) => return true,
            };
            let range = match segment.kind {
                SqlSegmentKind::Code => segment.range,
                SqlSegmentKind::ExecutableComment { body, .. } => body,
                _ => continue,
            };
            if sql[range]
                .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                .any(|w| keywords.iter().any(|k| w.eq_ignore_ascii_case(k)))
            {
                return true;
            }
        }
    }
    false
}
fn may_write(sql: &str) -> bool {
    has_keyword(sql, &["INSERT", "UPDATE", "DELETE", "REPLACE", "LOAD"])
}
fn matches(sql: &str, names: &BTreeSet<String>, tables: &BTreeSet<String>) -> bool {
    contains(sql, names) || (!tables.is_empty() && contains(sql, tables) && may_write(sql))
}
pub(crate) fn sensitive(sql: &str) -> bool {
    super::mentions(sql) || dependency_sensitive(sql)
}
fn dependency_sensitive(sql: &str) -> bool {
    cache()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .values()
        .any(|entry| matches(sql, &entry.names, &entry.trigger_tables))
}
/// Per-thread copy of one database's dependency names, valid while both the
/// catalog epoch and [`GENERATION`] are unchanged.
struct LocalEntry {
    key: usize,
    epoch: u64,
    generation: u64,
    names: Arc<BTreeSet<String>>,
    trigger_tables: Arc<BTreeSet<String>>,
}
thread_local! {
    static LOCAL: std::cell::RefCell<Option<LocalEntry>> = const { std::cell::RefCell::new(None) };
}
pub(crate) fn may_call(db: &Arc<Database>, sql: &str) -> bool {
    let mentions = crate::engine::perf_events::prepared_sql_traits(sql)
        .map_or_else(|| super::mentions(sql), |traits| traits.http_mentions);
    if mentions {
        return true;
    }
    let key = Arc::as_ptr(db) as usize;
    let epoch = db.catalog_publication_epoch();
    // Every statement asks this, often more than once. Serve the steady state
    // from a thread-local copy instead of the process-wide lock, whose reader
    // count is one cache line shared by every executing session.
    let generation = GENERATION.load(Ordering::Acquire);
    let local = LOCAL.with(|local| {
        local.borrow().as_ref().and_then(|e| {
            (e.key == key && e.epoch == epoch && e.generation == generation)
                .then(|| matches(sql, &e.names, &e.trigger_tables))
        })
    });
    if let Some(verdict) = local {
        return verdict;
    }
    let shared = cache()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .filter(|e| e.epoch == epoch)
        .map(|e| (Arc::clone(&e.names), Arc::clone(&e.trigger_tables)));
    if let Some((names, trigger_tables)) = shared {
        LOCAL.with(|local| {
            *local.borrow_mut() = Some(LocalEntry {
                key,
                epoch,
                generation,
                names: Arc::clone(&names),
                trigger_tables: Arc::clone(&trigger_tables),
            });
        });
        return matches(sql, &names, &trigger_tables);
    }
    let (epoch, sources) = db.http_dependency_sources();
    let mut names = BTreeSet::new();
    let mut trigger_tables = BTreeSet::new();
    loop {
        let before = names.len() + trigger_tables.len();
        for (name, body, is_trigger) in &sources {
            let dynamic = has_keyword(body, &["EXECUTE"]);
            if super::mentions(body) || dynamic || matches(body, &names, &trigger_tables) {
                if *is_trigger {
                    trigger_tables.insert(name.to_ascii_lowercase());
                } else {
                    names.insert(name.to_ascii_lowercase());
                }
            }
        }
        if names.len() + trigger_tables.len() == before {
            break;
        }
    }
    let result = super::mentions(sql) || matches(sql, &names, &trigger_tables);
    let mut cache = cache().write().unwrap_or_else(|e| e.into_inner());
    // A database's shutdown removes its entry. Avoid unbounded metadata from
    // embedding applications that open many short-lived engines.
    if cache.len() >= 1024 && !cache.contains_key(&key) {
        cache.retain(|_, e| e.owner.strong_count() > 0);
        GENERATION.fetch_add(1, Ordering::AcqRel);
    }
    if cache.len() >= 1024 && !cache.contains_key(&key) {
        return true;
    }
    cache.insert(
        key,
        Entry {
            epoch,
            names: Arc::new(names),
            trigger_tables: Arc::new(trigger_tables),
            owner: Arc::downgrade(db),
        },
    );
    GENERATION.fetch_add(1, Ordering::AcqRel);
    result
}
pub(crate) fn retire(db: &Database) {
    let mut cache = cache().write().unwrap_or_else(|e| e.into_inner());
    cache.remove(&(db as *const Database as usize));
    GENERATION.fetch_add(1, Ordering::AcqRel);
}

/// SQL-level EXECUTE names a connection-owned template, unlike binary EXECUTE
/// which already supplies the original SQL to the protocol dispatcher.
pub(crate) fn bound_sql<'a>(
    session: &'a crate::engine::api::SessionContext,
    sql: &'a str,
) -> &'a str {
    use crate::sql_lex::{PreparedSql, SqlLexMode, SqlTokenCursor, SqlTokenKind};
    if session.sql_prepared_statements.is_empty() {
        return sql;
    }
    let bytes = sql.as_bytes();
    if !memchr::memchr2_iter(b'e', b'E', bytes).any(|i| {
        bytes
            .get(i..i + 7)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"EXECUTE"))
    }) {
        return sql;
    }
    let mode = SqlLexMode::from_parser_bits(crate::engine::api::parse_temporal_sql_mode_bits(
        &session.sql_mode,
    ));
    let Ok(effective) = PreparedSql::new(sql, mode, crate::parser::MYSQL_VERSION_ID) else {
        return sql;
    };
    let mut tokens =
        SqlTokenCursor::new(effective.effective(), mode, crate::parser::MYSQL_VERSION_ID);
    if tokens.eat_keyword(b"EXECUTE") != Ok(true) {
        return sql;
    }
    let Ok(Some(token)) = tokens.next() else {
        return sql;
    };
    let name = match token.kind {
        SqlTokenKind::Word => std::borrow::Cow::Borrowed(tokens.token_text(&token)),
        SqlTokenKind::QuotedIdentifier => match tokens.decoded_quoted_text(&token) {
            Some(s) => std::borrow::Cow::Owned(s),
            None => return sql,
        },
        _ => return sql,
    };
    session
        .sql_prepared_statements
        .get(name.as_ref())
        .or_else(|| {
            session
                .sql_prepared_statements
                .get(&name.to_ascii_lowercase())
        })
        .map(|p| p.original_sql.as_ref())
        .unwrap_or(sql)
}
