//! Thread-local compile caches for per-call-expensive function inputs.
//!
//! Two SQL-function families re-parse their "program" argument on every
//! row today: regex functions (`REGEXP_LIKE` / `REGEXP_INSTR` /
//! `REGEXP_REPLACE` / `REGEXP_SUBSTR` and the `REGEXP` operator) rebuild
//! the automaton per call, and the binary-JSON read path
//! (`JSON_EXTRACT` / `->` / `->>` / `JSON_VALUE`) re-parses the path
//! string per call. Both inputs are almost always literals, so a scan
//! over N rows compiles the same pattern N times.
//!
//! The caches are thread-local LRUs (no locks on the per-row hot path;
//! parallel workers each warm their own copy, mirroring the
//! expression-program L1 in `expr_program.rs`). Values are `Arc`ed so a
//! hit is one refcount bump. Only successful compiles are cached: a
//! malformed pattern aborts the statement anyway, so the error path is
//! never row-hot.
//!
//! Capacities are deliberately small — the working set is "distinct
//! literal patterns in currently-running queries on this thread", which
//! is rarely more than a handful.

use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lru::LruCache;

use crate::engine::json::JsonPath;
use crate::types::QueryError;

pub(crate) const REGEX_CACHE_CAPACITY: usize = 128;
pub(crate) const JSON_PATH_CACHE_CAPACITY: usize = 512;
pub(crate) const JSON_SCHEMA_CACHE_CAPACITY: usize = 64;
/// B9: `col LIKE ?` under prepared / auto-param patterns re-classifies
/// the pattern once per row without a cache. Typical workloads only
/// bind a handful of distinct patterns, so a per-thread LRU here nets
/// out to one classify per unique pattern.
pub(crate) const LIKE_PLAN_CACHE_CAPACITY: usize = 128;

static REGEX_HITS: AtomicU64 = AtomicU64::new(0);
static TZ_HITS: AtomicU64 = AtomicU64::new(0);
static TZ_MISSES: AtomicU64 = AtomicU64::new(0);
static REGEX_MISSES: AtomicU64 = AtomicU64::new(0);
static JSON_PATH_HITS: AtomicU64 = AtomicU64::new(0);
static JSON_PATH_MISSES: AtomicU64 = AtomicU64::new(0);
static JSON_SCHEMA_HITS: AtomicU64 = AtomicU64::new(0);
static JSON_SCHEMA_MISSES: AtomicU64 = AtomicU64::new(0);
static LIKE_PLAN_HITS: AtomicU64 = AtomicU64::new(0);
static LIKE_PLAN_MISSES: AtomicU64 = AtomicU64::new(0);

/// Cache payload whose governor reservation follows the last cache/query
/// owner. A cache admission failure returns an uncharged transient value and
/// deliberately skips insertion; correctness never depends on cache space.
#[derive(Debug)]
pub(crate) struct Governed<T> {
    value: T,
    charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

impl<T> Deref for Governed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> Governed<T> {
    fn new(value: T, bytes: usize) -> Self {
        Self {
            value,
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                bytes,
            ),
        }
    }

    fn admitted(&self) -> bool {
        self.charge.is_some()
    }

    fn charged_bytes(&self) -> usize {
        self.charge.as_ref().map_or(0, |charge| charge.bytes())
    }
}

#[derive(Debug)]
struct RegexCacheEntry {
    pattern: String,
    match_type: String,
    regex: Arc<Governed<regex::Regex>>,
}

#[derive(Debug)]
struct LikeCacheEntry {
    pattern: String,
    escape: Option<char>,
    collation_id: Option<u16>,
    plan: Arc<Governed<crate::engine::expr_program::LikePlan>>,
}

fn borrowed_hash(value: impl Hash) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

thread_local! {
    static REGEX_CACHE: RefCell<LruCache<u64, Vec<RegexCacheEntry>, ahash::RandomState>> =
        RefCell::new(LruCache::with_hasher(
            NonZeroUsize::new(REGEX_CACHE_CAPACITY).unwrap(),
            ahash::RandomState::new(),
        ));
    static LIKE_PLAN_CACHE: RefCell<
        LruCache<
            u64,
            Vec<LikeCacheEntry>,
            ahash::RandomState,
        >,
    > = RefCell::new(LruCache::with_hasher(
        NonZeroUsize::new(LIKE_PLAN_CACHE_CAPACITY).unwrap(),
        ahash::RandomState::new(),
    ));
    static JSON_PATH_CACHE: RefCell<LruCache<String, Arc<Governed<JsonPath>>, ahash::RandomState>> =
        RefCell::new(LruCache::with_hasher(
            NonZeroUsize::new(JSON_PATH_CACHE_CAPACITY).unwrap(),
            ahash::RandomState::new(),
        ));
    static JSON_SCHEMA_CACHE: RefCell<
        LruCache<String, Arc<Governed<jsonschema::Validator>>, ahash::RandomState>,
    > = RefCell::new(LruCache::with_hasher(
        NonZeroUsize::new(JSON_SCHEMA_CACHE_CAPACITY).unwrap(),
        ahash::RandomState::new(),
    ));
}

/// Compile (or fetch) a MySQL-flavoured regex. `match_type` is the raw
/// MySQL match-type string (`i` / `c` / `m` / `n` / `u`); unknown flags
/// are ignored, matching MySQL's lenient behaviour. This is the single
/// canonical compiler — both the legacy evaluator (`expr.rs`) and the
/// compiled-program engine (`expr_program.rs`) route through it, so flag
/// semantics can't drift between the two paths.
pub(crate) fn cached_regex(
    pattern: &str,
    match_type: &str,
) -> Result<Arc<Governed<regex::Regex>>, QueryError> {
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let hash = borrowed_hash((pattern, match_type));
        // Hash the borrowed inputs, then verify the rare collision in-bucket:
        // the hit path performs no String allocation.
        if let Some(bucket) = cache.get(&hash) {
            if let Some(entry) = bucket
                .iter()
                .find(|entry| entry.pattern == pattern && entry.match_type == match_type)
            {
                REGEX_HITS.fetch_add(1, Ordering::Relaxed);
                return Ok(Arc::clone(&entry.regex));
            }
        }
        REGEX_MISSES.fetch_add(1, Ordering::Relaxed);
        let bytes = std::mem::size_of::<RegexCacheEntry>()
            .saturating_add(pattern.len())
            .saturating_add(match_type.len())
            // `regex` does not expose automaton capacity. Deliberately
            // over-reserve a 64 KiB floor plus a large pattern-proportional
            // allowance; optional admission skips caching under pressure.
            .saturating_add(pattern.len().saturating_mul(64))
            .saturating_add(64 * 1_024);
        let re = Arc::new(Governed::new(
            compile_mysql_regex(pattern, match_type)?,
            bytes,
        ));
        if re.admitted() {
            let entry = RegexCacheEntry {
                pattern: pattern.to_owned(),
                match_type: match_type.to_owned(),
                regex: Arc::clone(&re),
            };
            // A hash collision replaces the old candidate. Equality above
            // still guards reuse, and each bucket owns at most one entry.
            cache.put(hash, vec![entry]);
        }
        Ok(re)
    })
}

fn compile_mysql_regex(pattern: &str, match_type: &str) -> Result<regex::Regex, QueryError> {
    if pattern.is_empty() {
        return Err(crate::error::catalog::ER_REGEXP_ILLEGAL_ARGUMENT
            .error("Illegal argument to a regular expression."));
    }
    if match_type
        .chars()
        .any(|c| !matches!(c, 'c' | 'i' | 'm' | 'n' | 'u'))
    {
        return Err(crate::error::catalog::ER_WRONG_ARGUMENTS
            .error("Incorrect arguments to regular expression"));
    }
    // Bound pathological patterns before asking the Rust compiler to build a
    // large automaton. MySQL's ICU layer reports this resource/syntax failure
    // as ER_REGEXP_INTERNAL_ERROR rather than the legacy 1139.
    if pattern.len() > 8_192 {
        return Err(crate::error::catalog::ER_REGEXP_INTERNAL_ERROR
            .error("regular expression exceeded the implementation resource limit"));
    }
    // Rust's `regex` crate intentionally keeps POSIX bracket classes ASCII
    // only. MySQL's ICU regexp engine applies them to the decoded character
    // repertoire instead (for example latin1 0xff is lowercase and koi8r
    // 0xff is uppercase). Translate the character classes whose Unicode
    // semantics differ; ASCII inputs remain byte-for-byte equivalent.
    let pattern = pattern
        .replace("[[:lower:]]", r"\p{Lowercase}")
        .replace("[[:upper:]]", r"\p{Uppercase}")
        .replace("[[:alpha:]]", r"\p{Alphabetic}");
    // Keep multiline mode visible to consumers proving whole-string matches.
    // RegexBuilder flags are otherwise absent from Regex::as_str().
    let pattern = if match_type.contains('m') {
        format!("(?m){pattern}")
    } else {
        pattern
    };
    let mut builder = regex::RegexBuilder::new(&pattern);
    for ch in match_type.chars() {
        match ch {
            'i' => {
                builder.case_insensitive(true);
            }
            'c' => {
                builder.case_insensitive(false);
            }
            'm' => {
                builder.multi_line(true);
            }
            'n' => {
                builder.dot_matches_new_line(true);
            }
            // 'u': Unix-only line endings — the regex crate's default.
            _ => {}
        }
    }
    builder.build().map_err(|e| {
        let message = e.to_string();
        let spec = if message.contains("unclosed character class") {
            crate::error::catalog::ER_REGEXP_MISSING_CLOSE_BRACKET
        } else if message.contains("unclosed group") || message.contains("unopened group") {
            crate::error::catalog::ER_REGEXP_MISMATCHED_PAREN
        } else if message.contains("invalid repetition count range") {
            crate::error::catalog::ER_REGEXP_MAX_LT_MIN
        } else {
            crate::error::catalog::ER_REGEXP_RULE_SYNTAX
        };
        // The regex crate renders syntax diagnostics on multiple lines with
        // a source excerpt and caret. Protocol errors must remain one line:
        // otherwise MTR/CLI clients mistake the continuation for result rows.
        let summary = message.lines().last().unwrap_or("invalid pattern");
        spec.error(format!("invalid regular expression: {summary}"))
    })
}

/// Compile (or fetch) a LIKE pattern plan. Feeds the B9 fast path so
/// `col LIKE ?` with a bound parameter reuses the classified plan
/// across rows / statements. `collation_id` participates in the key so
/// the same pattern reused after `SET NAMES` or across differently
/// collated columns yields the correct CI/CS plan.
pub(crate) fn cached_like_plan(
    pattern: &str,
    escape: Option<char>,
    collation_id: Option<u16>,
) -> Arc<Governed<crate::engine::expr_program::LikePlan>> {
    LIKE_PLAN_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let hash = borrowed_hash((pattern, escape, collation_id));
        if let Some(bucket) = cache.get(&hash) {
            if let Some(entry) = bucket.iter().find(|entry| {
                entry.pattern == pattern
                    && entry.escape == escape
                    && entry.collation_id == collation_id
            }) {
                LIKE_PLAN_HITS.fetch_add(1, Ordering::Relaxed);
                return Arc::clone(&entry.plan);
            }
        }
        LIKE_PLAN_MISSES.fetch_add(1, Ordering::Relaxed);
        let info = collation_id.and_then(crate::charset::resolve_collation_id);
        let bytes = std::mem::size_of::<LikeCacheEntry>()
            .saturating_add(pattern.len().saturating_mul(8))
            .saturating_add(256);
        let plan = Arc::new(Governed::new(
            crate::engine::expr_program::classify_like_plan_public(pattern, escape, info),
            bytes,
        ));
        if plan.admitted() {
            let entry = LikeCacheEntry {
                pattern: pattern.to_owned(),
                escape,
                collation_id,
                plan: Arc::clone(&plan),
            };
            // A hash collision replaces the old candidate. Equality above
            // still guards reuse, and each bucket owns at most one entry.
            cache.put(hash, vec![entry]);
        }
        plan
    })
}

/// Parse (or fetch) a MySQL JSON path for the binary jsonb engine.
pub(crate) fn cached_json_path(path: &str) -> Result<Arc<Governed<JsonPath>>, QueryError> {
    JSON_PATH_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(parsed) = cache.get(path) {
            JSON_PATH_HITS.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::clone(parsed));
        }
        JSON_PATH_MISSES.fetch_add(1, Ordering::Relaxed);
        let bytes = std::mem::size_of::<JsonPath>()
            .saturating_add(path.len().saturating_mul(4))
            .saturating_add(256);
        let parsed = Arc::new(Governed::new(JsonPath::parse(path)?, bytes));
        if parsed.admitted() {
            cache.put(path.to_string(), Arc::clone(&parsed));
        }
        Ok(parsed)
    })
}

/// Compile (or fetch) a Draft 4 JSON Schema validator. Schema compilation is
/// substantially more expensive than validation and schemas are normally SQL
/// literals, so this removes a full meta-schema walk and compiler build per row.
pub(crate) fn cached_json_schema(
    schema: &serde_json::Value,
) -> Result<Arc<Governed<jsonschema::Validator>>, QueryError> {
    let key = serde_json::to_string(schema).map_err(|err| {
        crate::error::catalog::ER_INVALID_JSON_TYPE
            .error(format!("Invalid JSON Schema document: {err}"))
    })?;
    JSON_SCHEMA_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(validator) = cache.get(&key) {
            JSON_SCHEMA_HITS.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::clone(validator));
        }
        JSON_SCHEMA_MISSES.fetch_add(1, Ordering::Relaxed);
        if let Err(err) = jsonschema::draft4::meta::validate(schema) {
            return Err(crate::error::catalog::ER_INVALID_JSON_TYPE
                .error(format!("Invalid JSON Schema document: {err}")));
        }
        let validator = jsonschema::draft4::new(schema).map_err(|err| {
            crate::error::catalog::ER_INVALID_JSON_TYPE
                .error(format!("Invalid JSON Schema document: {err}"))
        })?;
        let bytes = std::mem::size_of::<jsonschema::Validator>()
            .saturating_add(key.capacity())
            .saturating_add(key.len().saturating_mul(16))
            .saturating_add(64 * 1_024);
        let validator = Arc::new(Governed::new(validator, bytes));
        if validator.admitted() {
            cache.put(key, Arc::clone(&validator));
        }
        Ok(validator)
    })
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FuncCacheStats {
    pub(crate) regex_hits: u64,
    pub(crate) regex_misses: u64,
    pub(crate) json_path_hits: u64,
    pub(crate) json_path_misses: u64,
    pub(crate) json_schema_hits: u64,
    pub(crate) json_schema_misses: u64,
    pub(crate) tz_hits: u64,
    pub(crate) tz_misses: u64,
    pub(crate) like_plan_hits: u64,
    pub(crate) like_plan_misses: u64,
    pub(crate) regex_entries: u64,
    pub(crate) regex_bytes: u64,
    pub(crate) json_path_entries: u64,
    pub(crate) json_path_bytes: u64,
    pub(crate) json_schema_entries: u64,
    pub(crate) json_schema_bytes: u64,
    pub(crate) like_plan_entries: u64,
    pub(crate) like_plan_bytes: u64,
}

/// Counters for the timezone-spec resolve cache (the cache itself lives
/// next to `TzSpec` in `expr.rs`; the counters live here so
/// `mydb_caches` / `SHOW ENGINE MYDB STATUS` surface it alongside the
/// other function caches).
pub(crate) fn note_tz_hit() {
    TZ_HITS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn note_tz_miss() {
    TZ_MISSES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn stats() -> FuncCacheStats {
    let (regex_entries, regex_bytes) = REGEX_CACHE.with(|cache| {
        cache
            .borrow()
            .iter()
            .fold((0_u64, 0_u64), |acc, (_, bucket)| {
                bucket.iter().fold(acc, |(entries, bytes), entry| {
                    (
                        entries + 1,
                        bytes.saturating_add(entry.regex.charged_bytes() as u64),
                    )
                })
            })
    });
    let (like_plan_entries, like_plan_bytes) = LIKE_PLAN_CACHE.with(|cache| {
        cache
            .borrow()
            .iter()
            .fold((0_u64, 0_u64), |acc, (_, bucket)| {
                bucket.iter().fold(acc, |(entries, bytes), entry| {
                    (
                        entries + 1,
                        bytes.saturating_add(entry.plan.charged_bytes() as u64),
                    )
                })
            })
    });
    let (json_path_entries, json_path_bytes) = JSON_PATH_CACHE.with(|cache| {
        cache
            .borrow()
            .iter()
            .fold((0_u64, 0_u64), |(entries, bytes), (_, path)| {
                (
                    entries + 1,
                    bytes.saturating_add(path.charged_bytes() as u64),
                )
            })
    });
    let (json_schema_entries, json_schema_bytes) = JSON_SCHEMA_CACHE.with(|cache| {
        cache
            .borrow()
            .iter()
            .fold((0_u64, 0_u64), |(entries, bytes), (_, validator)| {
                (
                    entries + 1,
                    bytes.saturating_add(validator.charged_bytes() as u64),
                )
            })
    });
    FuncCacheStats {
        regex_hits: REGEX_HITS.load(Ordering::Relaxed),
        regex_misses: REGEX_MISSES.load(Ordering::Relaxed),
        json_path_hits: JSON_PATH_HITS.load(Ordering::Relaxed),
        json_path_misses: JSON_PATH_MISSES.load(Ordering::Relaxed),
        json_schema_hits: JSON_SCHEMA_HITS.load(Ordering::Relaxed),
        json_schema_misses: JSON_SCHEMA_MISSES.load(Ordering::Relaxed),
        tz_hits: TZ_HITS.load(Ordering::Relaxed),
        tz_misses: TZ_MISSES.load(Ordering::Relaxed),
        like_plan_hits: LIKE_PLAN_HITS.load(Ordering::Relaxed),
        like_plan_misses: LIKE_PLAN_MISSES.load(Ordering::Relaxed),
        regex_entries,
        regex_bytes,
        json_path_entries,
        json_path_bytes,
        json_schema_entries,
        json_schema_bytes,
        like_plan_entries,
        like_plan_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_audit_hash_collisions_replace_instead_of_growing() {
        let old = cached_regex("^old$", "").unwrap();
        let hash = borrowed_hash(("^new$", ""));
        REGEX_CACHE.with(|cache| {
            cache.borrow_mut().put(
                hash,
                vec![RegexCacheEntry {
                    pattern: "^old$".into(),
                    match_type: String::new(),
                    regex: old,
                }],
            );
        });
        let new = cached_regex("^new$", "").unwrap();
        assert!(new.is_match("new"));
        assert!(!new.is_match("old"));
        REGEX_CACHE.with(|cache| assert_eq!(cache.borrow().peek(&hash).unwrap().len(), 1));

        let old = cached_like_plan("old%", None, None);
        let hash = borrowed_hash(("new%", None::<char>, None::<u16>));
        LIKE_PLAN_CACHE.with(|cache| {
            cache.borrow_mut().put(
                hash,
                vec![LikeCacheEntry {
                    pattern: "old%".into(),
                    escape: None,
                    collation_id: None,
                    plan: old.clone(),
                }],
            );
        });
        let new = cached_like_plan("new%", None, None);
        assert!(!Arc::ptr_eq(&old, &new));
        assert!(Arc::ptr_eq(&new, &cached_like_plan("new%", None, None)));
        LIKE_PLAN_CACHE.with(|cache| assert_eq!(cache.borrow().peek(&hash).unwrap().len(), 1));
    }

    #[test]
    fn regex_hit_returns_same_arc() {
        let a = cached_regex("^ab+c$", "").unwrap();
        let b = cached_regex("^ab+c$", "").unwrap();
        assert!(Arc::ptr_eq(&a, &b), "second lookup must be a cache hit");
        assert!(a.is_match("abbc"));
    }

    #[test]
    fn regex_match_type_is_part_of_key() {
        let cs = cached_regex("^abc$", "").unwrap();
        let ci = cached_regex("^abc$", "i").unwrap();
        assert!(!Arc::ptr_eq(&cs, &ci));
        assert!(!cs.is_match("ABC"));
        assert!(ci.is_match("ABC"));
    }

    #[test]
    fn regex_error_not_cached() {
        let err = cached_regex("(unclosed", "").unwrap_err();
        assert!(
            !err.message().contains('\n'),
            "protocol error text must stay on one line: {}",
            err.message()
        );
        // A subsequent good pattern still works.
        assert!(cached_regex("a+", "").is_ok());
    }

    #[test]
    fn regex_posix_character_classes_are_unicode_aware() {
        assert!(cached_regex("[[:lower:]]", "")
            .unwrap()
            .is_match("\u{00ff}"));
        assert!(cached_regex("[[:upper:]]", "")
            .unwrap()
            .is_match("\u{042a}"));
        assert!(cached_regex("[[:alpha:]]", "")
            .unwrap()
            .is_match("\u{0427}"));
    }

    #[test]
    fn json_path_hit_returns_same_arc() {
        let a = cached_json_path("$.a.b[0]").unwrap();
        let b = cached_json_path("$.a.b[0]").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(!a.has_wildcard);
        let wild = cached_json_path("$[*]").unwrap();
        assert!(wild.has_wildcard);
    }

    #[test]
    fn json_path_error_propagates() {
        assert!(cached_json_path("no-dollar").is_err());
    }

    #[test]
    fn json_schema_hit_returns_same_arc() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "id": { "type": "integer" } }
        });
        let first = cached_json_schema(&schema).unwrap();
        let second = cached_json_schema(&schema).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(first.is_valid(&serde_json::json!({"id": 1})));
    }
}
