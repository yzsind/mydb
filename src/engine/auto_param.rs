//! Auto-parameterization for COM_QUERY SELECT statements.
//!
//! Walks a SELECT AST and replaces "safe" `Expr::Literal` positions with
//! `Expr::Placeholder(idx)` (and `LimitValue::Rows(n)` with
//! `LimitValue::Placeholder(idx)`), recording the literal's runtime `Value`
//! into a parallel `Vec<Value>`. Combined with the thread-local parameter
//! context (`crate::engine::params`), this lets two queries that differ
//! only in their literals — e.g.
//!
//! ```text
//! SELECT * FROM t WHERE id = 42
//! SELECT * FROM t WHERE id = 43
//! ```
//!
//! collapse to a single shared cached plan.
//!
//! Scope (Phase 2 v1):
//! - WHERE / HAVING / JOIN ON predicates: parameterize equality operands.
//! - `IN (a,b,c)` lists whose members are all literals (≤ 64): each member
//!   becomes a placeholder and the list is padded to the next power of two
//!   by repeating the last value (duplicate IN members are a semantic
//!   no-op, for `NOT IN` too), so `IN (1,2,3)` and `IN (4,5,6,7)` collapse
//!   onto one plan-cache entry per arity bucket instead of one per arity.
//!   The planner's IN → IndexMultiLookup extraction already accepts
//!   `Expr::Placeholder` members (`PlanConstraintValue::Param`), resolved
//!   at every index access.
//! - LIMIT / OFFSET: parameterize numeric `Rows(n)` into
//!   `LimitValue::Placeholder`. Plan-shape decisions peek the bound value
//!   at plan time (TopK-vs-Sort, vector KNN k, row goals); the cached plan
//!   re-resolves the count at execution via `planner::LimitBinding`. The
//!   fingerprint appends a power-of-two magnitude bucket of
//!   `offset + limit`, so a reused plan only ever serves values within 2×
//!   of the shape it was planned for.
//!
//! Explicitly skipped:
//! - Constant predicates and literal range bounds (`<`, `<=`, `>`, `>=`,
//!   `BETWEEN`). Ranges retain literal-keyed caching and their planning-time
//!   selectivity / parallel-scan eligibility; extracting parameters cannot
//!   increase plan sharing for these statements.
//! - `LIKE pattern` operands. Pattern shape (`'foo%'` vs `'%foo'`) drives
//!   index strategy, so erasing the literal would hide a planning input.
//! - Function call arguments. Many builtins have const-folding semantics
//!   that change result type/precision when the argument is a literal.
//! - Projection / GROUP BY / ORDER BY expressions. Those affect column
//!   names and ordering keys; replacing them would break clients that
//!   echo back deterministic result columns.
//! - DDL.
//!
//! Mode `All` (env values `all` / `dml` / `on` / `true` / `1` / `yes`)
//! additionally rewrites text-protocol DML:
//! - UPDATE: SET values, WHERE, JOIN ON.
//! - DELETE: WHERE, JOIN ON.
//! - INSERT: small literal `VALUES` lists (≤ 4 rows — bulk inserts keep
//!   their literals: thousands of extracted params would buy nothing) and
//!   ON DUPLICATE KEY UPDATE values; `INSERT ... SELECT` reuses the
//!   SELECT rewriter for its source.

use std::sync::OnceLock;

use crate::parser::{BinaryOp, Expr, InsertSource, LimitValue, Literal, Select, Statement};
use crate::types::Value;
use crate::util::decimal::{parse_exact_decimal_str, ExactDecimal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoParameterizeMode {
    Off,
    Select,
    /// SELECT plus text-protocol DML (UPDATE / DELETE / INSERT).
    All,
}

impl AutoParameterizeMode {
    /// Read the configured mode.
    ///
    /// This is consulted once per statement on the COM_QUERY path, and
    /// `crate::util::env::var` bottoms out in `std::env::var`, which takes a
    /// process-wide lock and allocates a `String` (plus another for the
    /// lowercase fold). TPC-C profiling attributed ~0.4% of on-CPU time to
    /// that pair of allocations alone. The value is startup configuration —
    /// it cannot change while the server runs — so resolve it once.
    ///
    /// Tests drive the mode through `crate::util::env` thread-local
    /// overrides, so they keep the uncached read.
    pub fn from_env() -> Self {
        #[cfg(not(test))]
        {
            static CACHED: OnceLock<AutoParameterizeMode> = OnceLock::new();
            *CACHED.get_or_init(Self::read_env)
        }
        #[cfg(test)]
        {
            Self::read_env()
        }
    }

    fn read_env() -> Self {
        match crate::util::env::var("MYDB_AUTO_PARAMETERIZE")
            .ok()
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("off" | "0" | "false" | "no") => AutoParameterizeMode::Off,
            // Generic truthy values enable the full rewrite. Text-protocol
            // DML now consumes these fingerprints through the prepared-DML
            // shape cache, so the pass is no longer pure overhead for OLTP
            // clients that send repeated literal UPDATE/DELETE/INSERT bodies.
            Some("all" | "dml" | "on" | "1" | "true" | "yes") => AutoParameterizeMode::All,
            Some("select") => AutoParameterizeMode::Select,
            // Keep broad DML rewriting explicit: literal provenance affects
            // strict-mode diagnostics for several expression classes. The
            // executor separately promotes a proven-safe integer counter
            // UPDATE subset to `All` by default.
            //
            // Measured, on the current tree, with the default flipped to
            // `All`: `INSERT … VALUES (1/0)` stops raising the strict
            // division-by-zero error (the literal is const-folded, the
            // parameter is not), `INSERT DELAYED` loses its 1287 warning, and
            // several temporal / numeric conversion diagnostics change. Those
            // are exactly the provenance effects this comment warns about, so
            // the wider rewrite stays operator-selected.
            None | Some(_) => AutoParameterizeMode::Select,
        }
    }
}

/// Result of an auto-parameterization pass over a `Statement`.
///
/// - `params` is empty when nothing was rewritten, in which case the caller
///   can skip the late-binding setup.
/// - `fingerprint` is `Some(...)` whenever rewrites happened and the
///   rewritten shape is safe to share across literal values. It contains
///   a stable string suitable for use as a plan-cache key — an
///   ahash-derived 128-bit fingerprint over the *rewritten* AST's Debug
///   serialization, rendered as `"ap:<32 hex chars>"`. Two queries that
///   differ only in the auto-extracted literals collapse to the same
///   fingerprint because the literals have already been replaced by
///   `Expr::Placeholder(idx)`. `None` is returned when no rewrites took
///   place (e.g. mode=Off, or the statement was a non-SELECT, or the
///   SELECT had nothing safe to extract), or when a range-sensitive
///   literal was rewritten; in that case the caller should fall back to
///   its previous key (typically the normalized SQL text).
pub struct AutoParamResult {
    pub params: Vec<Value>,
    pub fingerprint: Option<String>,
}

/// One-byte type tag for a parameter's runtime value. Used in the
/// auto-param plan-cache fingerprint so numerically-typed and string-
/// typed literals produce distinct cache keys — otherwise the rewritten
/// AST reduces both to `Placeholder(i)` and a plan planned for `x = 1`
/// (integer comparison) would be reused for `x = '1'` (string
/// comparison). We collapse related types into equivalence classes so
/// prepared re-executes with, say, `Int` vs `UInt` still share a plan;
/// the classes are:
///   `i` : integer (signed or unsigned)
///   `f` : floating point
///   `d` : exact-decimal
///   `s` : text / bytes
///   `n` : NULL
///   `t` : temporal (date / datetime / timestamp / time)
///   `v` : vector
///   `?` : anything else
fn param_type_tag(value: &Value) -> u8 {
    match value {
        Value::Int(_) | Value::UInt(_) => b'i',
        Value::Float(_) | Value::Double(_) => b'f',
        Value::Decimal(_) | Value::DecimalCompact(_, _) => b'd',
        Value::String(_) | Value::SmallString(_) | Value::Bytes(_) => b's',
        Value::Null => b'n',
        Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_) | Value::Time(_) => b't',
        Value::Vector(_) => b'v',
        _ => b'?',
    }
}

/// Planning inputs carried by runtime parameters, excluding the structural
/// statement fingerprint. Prepared-handle L0 memos keep this compact suffix
/// so an EXECUTE that changes a parameter's comparison domain (or a dynamic
/// LIMIT/OFFSET magnitude bucket) recompiles instead of reusing a physical
/// plan chosen for the previous domain.
pub(crate) fn parameter_plan_signature(select: &Select, params: &[Value]) -> String {
    use std::fmt::Write as _;

    let mut signature = String::with_capacity(params.len().saturating_add(24));
    signature.push('t');
    for value in params {
        // Tuple bounds require lossless per-component conversions. Keep the
        // domains distinct without keying plans by concrete parameter values.
        signature.push(match value {
            Value::Int(v) if *v < 0 => '-',
            Value::Int(_) => '+',
            Value::UInt(v) if *v > i64::MAX as u64 => 'U',
            Value::UInt(_) => 'u',
            Value::Date(_) => 'D',
            Value::Time(_) => 'T',
            Value::DateTime(_) => 'Z',
            Value::Timestamp(_) => 'P',
            _ => param_type_tag(value) as char,
        });
    }
    let runtime_bucket = |value: &Option<LimitValue>| -> Option<u64> {
        let (LimitValue::Placeholder(index) | LimitValue::OffsetCommaPlaceholder(index)) =
            value.as_ref()?
        else {
            return None;
        };
        let n = match params.get(*index)? {
            Value::Int(n) if *n >= 0 => *n as usize,
            Value::UInt(n) => usize::try_from(*n).unwrap_or(usize::MAX),
            _ => return None,
        };
        Some(limit_magnitude_bucket(n))
    };
    if let Some(bucket) = runtime_bucket(&select.limit) {
        let _ = write!(signature, ":l{bucket:x}");
    }
    if let Some(bucket) = runtime_bucket(&select.offset) {
        let _ = write!(signature, ":o{bucket:x}");
    }
    signature
}

/// Power-of-two magnitude bucket for LIMIT / OFFSET fingerprinting. Two
/// values share a bucket iff they share `next_power_of_two()`, so a plan
/// peeked at one value is only ever reused for values within 2× — which
/// bounds how far the TopK-heap / row-goal decisions can drift from the
/// runtime count.
fn limit_magnitude_bucket(n: usize) -> u64 {
    (n as u64).checked_next_power_of_two().unwrap_or(u64::MAX)
}

/// If `statement` is a SELECT, walk it and rewrite literals into
/// placeholders per the rules in the module doc. Returns the extracted
/// runtime values; the AST is mutated in place. Non-SELECT statements
/// are left untouched.
pub fn auto_parameterize(mode: AutoParameterizeMode, statement: &mut Statement) -> AutoParamResult {
    let mut params = Vec::new();
    let mut cache_sensitive = false;
    if matches!(mode, AutoParameterizeMode::Off) {
        return AutoParamResult {
            params,
            fingerprint: None,
        };
    }
    let dml = matches!(mode, AutoParameterizeMode::All);
    let eligible = matches!(statement, Statement::Select(_))
        || (dml
            && matches!(
                statement,
                Statement::Update(_) | Statement::Delete(_) | Statement::Insert(_)
            ));
    // Large VALUES bodies without ODKU cannot reuse a parameterized plan.
    // Do not scan their payload merely to discover there is nothing to rewrite.
    let bulk_values = matches!(statement, Statement::Insert(insert)
        if matches!(&insert.source, InsertSource::Values(rows) if rows.len() > INSERT_VALUES_MAX_PARAM_ROWS)
            && insert.on_duplicate_key_update.is_none());
    if !eligible || bulk_values {
        return AutoParamResult {
            params,
            fingerprint: None,
        };
    }
    let mut placeholders = PlaceholderCounter::default();
    crate::parser::visit::visit_statement(&mut placeholders, statement);
    let mut next_idx = placeholders.next_index();
    match statement {
        Statement::Select(select) => {
            rewrite_select(select, &mut next_idx, &mut params, &mut cache_sensitive);
            rewrite_limit_offset(select, &mut next_idx, &mut params);
        }
        Statement::Update(update) if dml => {
            for assignment in &mut update.assignments {
                rewrite_predicate_expr::<false>(
                    &[],
                    &mut assignment.value,
                    &mut next_idx,
                    &mut params,
                    &mut cache_sensitive,
                );
                try_replace_literal(&mut assignment.value, &mut next_idx, &mut params);
            }
            if let Some(selection) = &mut update.selection {
                rewrite_predicate_expr::<false>(
                    &[],
                    selection,
                    &mut next_idx,
                    &mut params,
                    &mut cache_sensitive,
                );
            }
            for join in &mut update.joins {
                if let Some(condition) = &mut join.condition {
                    rewrite_predicate_expr::<false>(
                        &[],
                        condition,
                        &mut next_idx,
                        &mut params,
                        &mut cache_sensitive,
                    );
                }
            }
        }
        Statement::Delete(delete) if dml => {
            if let Some(selection) = &mut delete.selection {
                rewrite_predicate_expr::<false>(
                    &[],
                    selection,
                    &mut next_idx,
                    &mut params,
                    &mut cache_sensitive,
                );
            }
            for join in &mut delete.joins {
                if let Some(condition) = &mut join.condition {
                    rewrite_predicate_expr::<false>(
                        &[],
                        condition,
                        &mut next_idx,
                        &mut params,
                        &mut cache_sensitive,
                    );
                }
            }
        }
        Statement::Insert(insert) if dml => {
            rewrite_insert(insert, &mut next_idx, &mut params, &mut cache_sensitive);
        }
        _ => {}
    }
    let fingerprint = if params.is_empty() || cache_sensitive {
        None
    } else {
        // The rewritten AST captures *only* the structural shape of the
        // query — extracted literals have been replaced by indexed
        // `Placeholder` nodes, so the Debug serialization is a stable
        // canonical key (no `f64` left in `Literal::Float`, no HashMap
        // iteration). We funnel that Debug stream through two seeded
        // ahash hashers and combine the u64 outputs into a 128-bit
        // fingerprint, rendered as a short fixed-length string. This
        // matches what `format!("{:?}", statement)` would key on but
        // skips the intermediate String allocation (several hundred
        // bytes to several KB on a per-SELECT hot path).
        let fp = typed_plan_fingerprint(statement, &params);
        // Encode extracted parameter type shape into the fingerprint so
        // `WHERE x = 1` and `WHERE x = '1'` mint separate plan-cache
        // entries. The rewritten AST replaces both with `Placeholder(i)`,
        // which erases the original type — the planner can then cache a
        // plan chosen under integer coercion and hand it back to a call
        // that expects string coercion, silently changing which rows
        // match. One byte per parameter (see `param_type_tag`) makes
        // this a fixed constant overhead per entry.
        Some(fp)
    };
    AutoParamResult {
        params,
        fingerprint,
    }
}

/// Canonical compiled-plan key shared by auto-parameterized text SQL and
/// prepared placeholders. Runtime parameter type classes and LIMIT/OFFSET
/// magnitude remain part of the key because both can affect physical shape.
pub fn typed_plan_fingerprint<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
    params: &[Value],
) -> String {
    let statement = statement.into();
    let mut fp = structural_fingerprint(statement);
    fp.push(':');
    if let crate::parser::QueryRef::Select(select) = statement {
        fp.push_str(&parameter_plan_signature(select, params));
    } else {
        fp.push('t');
        for value in params {
            fp.push(param_type_tag(value) as char);
        }
    }
    fp
}

/// Fixed-size plan-cache key for a text SELECT shape that has already been
/// proven equivalent to the auto-parameterized AST by the full parser. Hashing
/// the compact lexical skeleton in two slices avoids replaying `Debug` over
/// the complete AST on every literal COM_QUERY execution.
pub(crate) fn typed_text_shape_fingerprint(
    shape_sql: &str,
    select: &Select,
    params: &[Value],
) -> String {
    use std::fmt::Write as _;
    use std::hash::{BuildHasher, Hasher};

    static STATE_LO: OnceLock<ahash::RandomState> = OnceLock::new();
    static STATE_HI: OnceLock<ahash::RandomState> = OnceLock::new();
    let lo = STATE_LO.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0x5445_5854_5348_4150,
            0x4553_454C_4543_5431,
            0xD6E8_FEB8_6659_FD93,
            0xA5A3_56F1_1C2D_9E47,
        )
    });
    let hi = STATE_HI.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0xC0DE_CAFE_51EC_7001,
            0x9E37_79B9_7F4A_7C15,
            0x243F_6A88_85A3_08D3,
            0x1319_8A2E_0370_7344,
        )
    });
    let signature = parameter_plan_signature(select, params);
    let mut h_lo = lo.build_hasher();
    let mut h_hi = hi.build_hasher();
    h_lo.write(shape_sql.as_bytes());
    h_lo.write(signature.as_bytes());
    h_hi.write(shape_sql.as_bytes());
    h_hi.write(signature.as_bytes());
    let mut out = String::with_capacity(35);
    let _ = write!(out, "ts:{:016x}{:016x}", h_hi.finish(), h_lo.finish());
    out
}

/// Rewrite literal top-level LIMIT / OFFSET counts into
/// `LimitValue::Placeholder`, recording the extracted values and their
/// magnitude buckets. Returns `None` when neither clause was rewritten.
fn rewrite_limit_offset(
    select: &mut Select,
    next_idx: &mut usize,
    params: &mut Vec<Value>,
) -> Option<(Option<u64>, Option<u64>)> {
    let mut limit_bucket = None;
    let mut offset_bucket = None;
    if let Some(LimitValue::Rows(n)) = select.limit {
        params.push(Value::UInt(n as u64));
        select.limit = Some(LimitValue::Placeholder(*next_idx));
        *next_idx += 1;
        limit_bucket = Some(limit_magnitude_bucket(n));
    }
    if let Some(LimitValue::Rows(n)) = select.offset {
        params.push(Value::UInt(n as u64));
        select.offset = Some(LimitValue::Placeholder(*next_idx));
        *next_idx += 1;
        offset_bucket = Some(limit_magnitude_bucket(n));
    }
    if limit_bucket.is_none() && offset_bucket.is_none() {
        None
    } else {
        Some((limit_bucket, offset_bucket))
    }
}

/// Render an ahash-derived 128-bit fingerprint of the (already
/// rewritten) `Statement`'s Debug output as `"ap:<32 hex chars>"`.
///
/// This is the plan-cache key for auto-parameterized SELECTs. Two
/// fixed seeded `ahash::RandomState`s feed two independent hashers
/// fed by the same Debug stream; the resulting `(u64, u64)` is
/// rendered as a 32-character lowercase-hex tail with an `"ap:"`
/// prefix so the key origin is obvious in logs.
///
/// Why not `format!("{:?}", statement)`: that allocates a String the
/// size of the entire Debug serialization (hundreds of bytes to
/// several KB) on every SELECT entry. The hasher consumes the same
/// byte stream without materializing it.
///
/// Why two hashers (u128 not u64): plan-cache capacity is a few
/// thousand entries; a 64-bit fingerprint gives ~10⁻¹⁰ birthday
/// collision probability, a 128-bit one ~10⁻³⁰. Cost of the extra
/// hasher is a handful of nanoseconds.
///
/// Why fixed seeds (not `RandomState::new()`): the fingerprint is
/// used as a *cross-session* plan-cache key. Different connections
/// must produce the same fingerprint for the same rewritten SQL
/// shape, so the seeds must be the same for every construction in
/// this process. `std::hash_map::DefaultHasher::new()` (the previous
/// implicit hasher behind `format!`-keyed lookups) has the same
/// fixed-seed property.
pub(crate) fn structural_fingerprint<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> String {
    let statement = statement.into();
    use std::fmt::Write as _;
    use std::hash::{BuildHasher, Hasher};

    static STATE_LO: OnceLock<ahash::RandomState> = OnceLock::new();
    static STATE_HI: OnceLock<ahash::RandomState> = OnceLock::new();
    let rs_lo = STATE_LO.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0x4D59_4442_4150_4644u64, // "MYDBAPFD"
            0x6175_746F_7061_726Du64, // "autoparm"
            0x1A4D_C8B7_3E2F_5901u64,
            0x9E37_79B9_7F4A_7C15u64,
        )
    });
    let rs_hi = STATE_HI.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0xCAFE_BABE_DEAD_BEEFu64,
            0x0123_4567_89AB_CDEFu64,
            0xFEDC_BA98_7654_3210u64,
            0x6A09_E667_F3BC_C908u64,
        )
    });
    struct HashWriter<'a, H: Hasher + ?Sized>(&'a mut H);
    impl<H: Hasher + ?Sized> std::fmt::Write for HashWriter<'_, H> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            // P2-C1: fold ASCII case while hashing. The parser now preserves
            // the query's original casing (so unaliased expression columns can
            // be named the MySQL way, `COUNT(*)` not `count(*)`), which would
            // otherwise fragment the plan cache — `A.name` vs `a.name`, `Select`
            // vs `SELECT`. Identifiers/keywords are case-insensitive and string
            // literals have already been extracted into params, so folding case
            // here restores the case-insensitive plan sharing `normalize_sql`
            // used to provide. Most Debug fragments (field names,
            // punctuation and normalized identifiers) are already lowercase;
            // feed those to ahash as one slice instead of one virtual hasher
            // call per byte. Mixed-case enum/type names use a bounded stack
            // buffer and remain byte-for-byte identical to the old stream.
            let bytes = s.as_bytes();
            if !bytes.iter().any(u8::is_ascii_uppercase) {
                self.0.write(bytes);
            } else {
                let mut folded = [0u8; 128];
                for chunk in bytes.chunks(folded.len()) {
                    for (dst, &src) in folded.iter_mut().zip(chunk) {
                        *dst = src.to_ascii_lowercase();
                    }
                    self.0.write(&folded[..chunk.len()]);
                }
            }
            Ok(())
        }
    }
    let mut h_lo = rs_lo.build_hasher();
    let mut h_hi = rs_hi.build_hasher();
    // Two passes is unavoidable: `std::fmt::Debug` doesn't expose a
    // way to fan one write stream out to multiple sinks, and we
    // need two independent hash states for the 128-bit output.
    // Each pass is allocation-free.
    let _ = write!(HashWriter(&mut h_lo), "{statement:?}");
    let _ = write!(HashWriter(&mut h_hi), "{statement:?}");
    let lo = h_lo.finish();
    let hi = h_hi.finish();
    // Pre-size to "ap:" + 32 hex chars. `write!` against a sized
    // `String` does no further reallocations.
    let mut out = String::with_capacity(35);
    let _ = write!(out, "ap:{hi:016x}{lo:016x}");
    debug_assert_eq!(out.len(), 35);
    out
}

/// One complete scan reserves every existing parameter index before any
/// literals are extracted. The shared walker includes query, window, GROUP BY,
/// ORDER BY, LIMIT/OFFSET, and DML expression slots.
#[derive(Default)]
struct PlaceholderCounter(Option<usize>);
impl PlaceholderCounter {
    fn record(&mut self, index: usize) {
        self.0 = Some(self.0.map_or(index, |previous| previous.max(index)));
    }
    fn next_index(&self) -> usize {
        self.0.map_or(0, |index| index + 1)
    }
}
impl crate::parser::visit::AstVisitor for PlaceholderCounter {
    fn enter_expr(&mut self, expr: &Expr) {
        if let Expr::Placeholder(index) = expr {
            self.record(*index);
        }
    }
    fn enter_limit(&mut self, limit: &LimitValue) {
        if let LimitValue::Placeholder(index) | LimitValue::OffsetCommaPlaceholder(index) = limit {
            self.record(*index);
        }
    }
}

fn rewrite_query(
    query: &mut crate::parser::QueryExpr,
    next_idx: &mut usize,
    params: &mut Vec<Value>,
    cache_sensitive: &mut bool,
) {
    use crate::parser::QueryExpr;
    match query {
        QueryExpr::Select(select) => rewrite_select(select, next_idx, params, cache_sensitive),
        QueryExpr::SetOp(set) => {
            rewrite_query(&mut set.left, next_idx, params, cache_sensitive);
            rewrite_query(&mut set.right, next_idx, params, cache_sensitive);
        }
        QueryExpr::WithCte(with) => {
            for cte in &mut with.ctes {
                rewrite_query(&mut cte.query, next_idx, params, cache_sensitive);
            }
            rewrite_query(&mut with.body, next_idx, params, cache_sensitive);
        }
    }
}

#[cfg(test)]
fn walk_max_placeholder(expr: &Expr, max_seen: &mut Option<usize>) {
    let mut counter = PlaceholderCounter(*max_seen);
    crate::parser::visit::visit_expr(&mut counter, expr);
    *max_seen = counter.0;
}

fn rewrite_select(
    select: &mut Select,
    next_idx: &mut usize,
    params: &mut Vec<Value>,
    cache_sensitive: &mut bool,
) {
    if let Some(selection) = &mut select.selection {
        rewrite_predicate_expr::<true>(&[], selection, next_idx, params, cache_sensitive);
    }
    if let Some(having) = &mut select.having {
        let protected: Vec<&Expr> = select.group_by.iter().map(|group| {
            let projected = match group {
                Expr::Identifier(name) => select.projection.iter().find(|item| matches!(item,
                    crate::parser::SelectItem::Expr { alias: Some(alias), .. } if alias.eq_ignore_ascii_case(name))),
                Expr::Literal(Literal::Int(pos)) if *pos > 0 => select.projection.get(*pos as usize - 1),
                _ => None,
            };
            match projected {
                Some(crate::parser::SelectItem::Expr { expr, .. }) => expr,
                _ => group,
            }
        }).collect();
        rewrite_predicate_expr::<true>(&protected, having, next_idx, params, cache_sensitive);
    }
    for join in &mut select.joins {
        if let Some(condition) = &mut join.condition {
            rewrite_predicate_expr::<true>(&[], condition, next_idx, params, cache_sensitive);
        }
    }
    // LIMIT / OFFSET are rewritten by `rewrite_limit_offset` — only at
    // the top-level statement entry (not for derived selects routed
    // through here by INSERT ... SELECT), because the magnitude bucket
    // has to reach the statement fingerprint and the plan-cache key.
}

/// Mode `All`: rewrite an INSERT. Small literal `VALUES` lists and
/// ON DUPLICATE KEY UPDATE values are extracted; `INSERT ... SELECT`
/// delegates to the SELECT rewriter. Bulk literal inserts keep their
/// literals — extracting thousands of params would scale the rewrite
/// cost with row count for zero plan-shape benefit (the bulk body is
/// one-shot for the plan cache anyway).
const INSERT_VALUES_MAX_PARAM_ROWS: usize = 4;

fn rewrite_insert(
    insert: &mut crate::parser::Insert,
    next_idx: &mut usize,
    params: &mut Vec<Value>,
    cache_sensitive: &mut bool,
) {
    match &mut insert.source {
        InsertSource::Values(rows) if rows.len() <= INSERT_VALUES_MAX_PARAM_ROWS => {
            for row in rows {
                for expr in row {
                    try_replace_literal(expr, next_idx, params);
                }
            }
        }
        InsertSource::Values(_) => {}
        InsertSource::Query(select) => {
            rewrite_query(select, next_idx, params, cache_sensitive);
        }
    }
    if let Some(assignments) = &mut insert.on_duplicate_key_update {
        for assignment in assignments {
            rewrite_predicate_expr::<false>(
                &[],
                &mut assignment.value,
                next_idx,
                params,
                cache_sensitive,
            );
            try_replace_literal(&mut assignment.value, next_idx, params);
        }
    }
}

/// Rewrite a predicate expression. Recurses into the structural junctions
/// (`AND`/`OR`/`NOT`/`Between`) but stops at sub-expressions that aren't
/// safe to parameterize (function calls, LIKE patterns and subqueries).
/// SELECT keeps planning literals; DML retains the complete literal ordering
/// required by its lexical template scanner and compiled counter kernels.
fn rewrite_predicate_expr<const PRESERVE_PLANNING_LITERALS: bool>(
    protected: &[&Expr],
    expr: &mut Expr,
    next_idx: &mut usize,
    params: &mut Vec<Value>,
    cache_sensitive: &mut bool,
) {
    // GROUP BY/projection expressions retain literals as part of their
    // identity. Parameterizing only the matching HAVING subtree would make
    // a legal grouped expression look like an ungrouped input column.
    if protected
        .iter()
        .any(|group| crate::engine::expr_program::expr_semantically_eq(group, expr))
    {
        return;
    }
    // A range OR already needs a literal-sensitive cache entry. Its equality
    // prefixes participate in the same endpoints: retain them too, otherwise
    // a>10 OR (a=10 AND b>2) mixes a literal with an unrelated placeholder.
    if PRESERVE_PLANNING_LITERALS
        && matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::Or,
                ..
            }
        )
        && crate::planner::boolean_range::has_range_or(expr)
    {
        *cache_sensitive = true;
        return;
    }
    // Keep constant subtrees visible to the ordinary constant folder and
    // cardinality estimator. Turning `1=1` into `?=?` hides its selectivity
    // (and can turn a parallel scan/aggregate into a serial plan). Do not
    // evaluate anything here: the binder still owns diagnostics and types.
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And | BinaryOp::Or,
            right,
        } if PRESERVE_PLANNING_LITERALS => {
            // Boolean operands are predicates themselves. A literal TRUE,
            // FALSE or NULL here must not become a generic parameter either.
            rewrite_predicate_expr::<PRESERVE_PLANNING_LITERALS>(
                protected,
                left,
                next_idx,
                params,
                cache_sensitive,
            );
            rewrite_predicate_expr::<PRESERVE_PLANNING_LITERALS>(
                protected,
                right,
                next_idx,
                params,
                cache_sensitive,
            );
        }
        Expr::Binary { left, op, right } => {
            if PRESERVE_PLANNING_LITERALS
                && crate::engine::expr::expr_is_constant(left)
                && crate::engine::expr::expr_is_constant(right)
            {
                return;
            }
            if PRESERVE_PLANNING_LITERALS && binary_op_is_range_sensitive(*op) {
                // These statements already use a literal-keyed cache entry.
                // Keep the bounds literal too: KeyPart::Param deliberately
                // disables parallel index scans for generic prepared plans.
                *cache_sensitive |= parameterizable_literal(left).is_some()
                    || parameterizable_literal(right).is_some();
            } else {
                let before = params.len();
                try_replace_literal(left, next_idx, params);
                try_replace_literal(right, next_idx, params);
                if params.len() != before && binary_op_is_range_sensitive(*op) {
                    *cache_sensitive = true;
                }
            }
            // Continue descending in case either side is itself a Binary
            // (e.g. `(a = 1) AND (b = 2)`).
            rewrite_predicate_expr::<PRESERVE_PLANNING_LITERALS>(
                protected,
                left,
                next_idx,
                params,
                cache_sensitive,
            );
            rewrite_predicate_expr::<PRESERVE_PLANNING_LITERALS>(
                protected,
                right,
                next_idx,
                params,
                cache_sensitive,
            );
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            rewrite_predicate_expr::<PRESERVE_PLANNING_LITERALS>(
                protected,
                expr,
                next_idx,
                params,
                cache_sensitive,
            );
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            if PRESERVE_PLANNING_LITERALS
                && crate::engine::expr::expr_is_constant(expr)
                && crate::engine::expr::expr_is_constant(low)
                && crate::engine::expr::expr_is_constant(high)
            {
                return;
            }
            rewrite_predicate_expr::<PRESERVE_PLANNING_LITERALS>(
                protected,
                expr,
                next_idx,
                params,
                cache_sensitive,
            );
            if PRESERVE_PLANNING_LITERALS {
                *cache_sensitive |= parameterizable_literal(low).is_some()
                    || parameterizable_literal(high).is_some();
            } else {
                let before = params.len();
                try_replace_literal(low, next_idx, params);
                try_replace_literal(high, next_idx, params);
                if params.len() != before {
                    *cache_sensitive = true;
                }
            }
        }
        Expr::InList { expr, list, .. } => {
            if PRESERVE_PLANNING_LITERALS
                && crate::engine::expr::expr_is_constant(expr)
                && list.iter().all(crate::engine::expr::expr_is_constant)
            {
                return;
            }
            rewrite_predicate_expr::<PRESERVE_PLANNING_LITERALS>(
                protected,
                expr,
                next_idx,
                params,
                cache_sensitive,
            );
            try_param_in_list(list, next_idx, params);
        }
        // Stop at constructs whose literals should not be erased.
        Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. }
        | Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::Like { .. }
        | Expr::Function { .. }
        | Expr::Case { .. }
        | Expr::WindowFunction { .. } => {}
        Expr::GroupKeyRef { expr, .. } => {
            rewrite_predicate_expr::<PRESERVE_PLANNING_LITERALS>(
                protected,
                expr,
                next_idx,
                params,
                cache_sensitive,
            );
        }
        Expr::Grouping(_) => {}
        Expr::Tuple(items) => {
            for item in items {
                rewrite_predicate_expr::<PRESERVE_PLANNING_LITERALS>(
                    protected,
                    item,
                    next_idx,
                    params,
                    cache_sensitive,
                );
            }
        }
        // Leaves: nothing to descend into.
        Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::BoundValue(_)
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::UserVariable(_)
        | Expr::SystemVariable { .. }
        | Expr::Wildcard => {}
    }
}

fn binary_op_is_range_sensitive(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
    )
}

/// The runtime `Value` of an expression that is safe to extract as a
/// parameter: a bare literal, or a negated numeric literal (`-42` parses
/// as `Unary(Neg, Literal(42))`).
fn parameterizable_literal(expr: &Expr) -> Option<Value> {
    match expr {
        // Bit / hex literals (`b'…'` / `0x…`) must keep their AST provenance:
        // downstream numeric coercion in comparisons (`bitcol = b'101'`) keys
        // off `Literal::BitBytes` / `Literal::HexBytes`, and folding them into
        // a `Value::Bytes` placeholder would erase that and drop rows. They
        // are rare in hot SELECTs, so the lost plan-cache sharing is immaterial.
        Expr::Literal(Literal::HexBytes(_) | Literal::BitBytes(_)) => None,
        Expr::Literal(lit) => Some(literal_to_value(lit)),
        // MySQL text-protocol drivers render a bytes parameter as
        // `_binary'…'`.  The parser deliberately preserves introducer
        // provenance as CONVERT_USING for general expressions; when the whole
        // direct value is late-bound, a binary Value is the exact runtime
        // representation and avoids retaining/cloning the large wrapper AST.
        Expr::Function {
            name,
            args,
            distinct: false,
        } if name.eq_ignore_ascii_case("CONVERT_USING")
            && matches!(args.as_slice(), [
                Expr::Literal(Literal::String(_)),
                Expr::Literal(Literal::String(charset)),
                Expr::Literal(Literal::Bool(true)),
            ] if charset.eq_ignore_ascii_case("binary")) =>
        {
            let Expr::Literal(Literal::String(value)) = &args[0] else {
                unreachable!("binary introducer shape checked above")
            };
            Some(Value::Bytes(value.as_bytes().to_vec()))
        }
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr: inner,
        } => match inner.as_ref() {
            Expr::Literal(Literal::Int(n)) => Some(Value::Int(-*n)),
            Expr::Literal(Literal::Float(f)) => Some(Value::Double(-*f)),
            _ => None,
        },
        _ => None,
    }
}

fn try_replace_literal(expr: &mut Expr, next_idx: &mut usize, params: &mut Vec<Value>) {
    if !parameterizable_literal_shape(expr) {
        return;
    }
    let owned = std::mem::replace(expr, Expr::Placeholder(*next_idx));
    let value = parameterizable_literal_owned(owned)
        .expect("shape predicate and owned literal conversion must agree");
    params.push(value);
    *next_idx += 1;
}

fn parameterizable_literal_shape(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Literal::HexBytes(_) | Literal::BitBytes(_)) => false,
        Expr::Literal(_) => true,
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr: inner,
        } => matches!(
            inner.as_ref(),
            Expr::Literal(Literal::Int(_) | Literal::Float(_))
        ),
        Expr::Function {
            name,
            args,
            distinct: false,
        } => {
            name.eq_ignore_ascii_case("CONVERT_USING")
                && matches!(args.as_slice(), [
                Expr::Literal(Literal::String(_)),
                Expr::Literal(Literal::String(charset)),
                Expr::Literal(Literal::Bool(true)),
            ] if charset.eq_ignore_ascii_case("binary"))
        }
        _ => false,
    }
}

fn parameterizable_literal_owned(expr: Expr) -> Option<Value> {
    match expr {
        Expr::Literal(Literal::HexBytes(_) | Literal::BitBytes(_)) => None,
        Expr::Literal(lit) => Some(literal_to_value_owned(lit)),
        Expr::Unary {
            op: crate::parser::UnaryOp::Neg,
            expr: inner,
        } => match *inner {
            Expr::Literal(Literal::Int(n)) => Some(Value::Int(-n)),
            Expr::Literal(Literal::Float(f)) => Some(Value::Double(-f)),
            _ => None,
        },
        Expr::Function {
            name,
            mut args,
            distinct: false,
        } if name.eq_ignore_ascii_case("CONVERT_USING")
            && matches!(args.as_slice(), [
                Expr::Literal(Literal::String(_)),
                Expr::Literal(Literal::String(charset)),
                Expr::Literal(Literal::Bool(true)),
            ] if charset.eq_ignore_ascii_case("binary")) =>
        {
            match args.remove(0) {
                Expr::Literal(Literal::String(value)) => Some(Value::Bytes(value.into_bytes())),
                _ => unreachable!("binary introducer shape checked above"),
            }
        }
        _ => None,
    }
}

/// IN-lists longer than this keep their literals: very long lists are
/// generated bulk shapes whose rewrite cost scales with arity for little
/// plan-cache benefit, and padding them to a power of two would bloat
/// the parameter vector.
const IN_LIST_MAX_PARAM_ITEMS: usize = 64;

/// Rewrite an all-literal IN-list into placeholders, padding the list to
/// the next power of two by repeating the final value. Duplicate IN
/// members are a semantic no-op (for `IN` and `NOT IN`, NULLs included),
/// so the padding collapses arities 5..=8 etc. onto one plan shape —
/// without it, ORM-generated lists would mint one plan-cache entry per
/// distinct arity. Mixed literal / non-literal lists are left untouched.
fn try_param_in_list(list: &mut Vec<Expr>, next_idx: &mut usize, params: &mut Vec<Value>) {
    if list.is_empty() || list.len() > IN_LIST_MAX_PARAM_ITEMS {
        return;
    }
    let Some(values) = list
        .iter()
        .map(parameterizable_literal)
        .collect::<Option<Vec<_>>>()
    else {
        return;
    };
    let padded_len = list.len().next_power_of_two();
    let last = values.last().cloned();
    for (slot, value) in list.iter_mut().zip(values) {
        params.push(value);
        *slot = Expr::Placeholder(*next_idx);
        *next_idx += 1;
    }
    if let Some(pad_value) = last {
        while list.len() < padded_len {
            params.push(pad_value.clone());
            list.push(Expr::Placeholder(*next_idx));
            *next_idx += 1;
        }
    }
}

fn literal_to_value(lit: &Literal) -> Value {
    match lit {
        Literal::Null => Value::Null,
        Literal::Bool(b) => Value::Int(if *b { 1 } else { 0 }),
        Literal::Int(v) => Value::Int(*v),
        Literal::UInt(v) => Value::UInt(*v),
        Literal::Float(v) => Value::Double(*v),
        Literal::Decimal(s) => {
            compact_auto_param_decimal(s).unwrap_or_else(|| Value::Decimal(s.clone()))
        }
        Literal::String(s) => Value::String(s.clone()),
        Literal::HexBytes(b) | Literal::BitBytes(b) => Value::Bytes(b.clone()),
    }
}

fn literal_to_value_owned(lit: Literal) -> Value {
    match lit {
        Literal::Null => Value::Null,
        Literal::Bool(b) => Value::Int(if b { 1 } else { 0 }),
        Literal::Int(v) => Value::Int(v),
        Literal::UInt(v) => Value::UInt(v),
        Literal::Float(v) => Value::Double(v),
        Literal::Decimal(s) => compact_auto_param_decimal(&s).unwrap_or(Value::Decimal(s)),
        Literal::String(s) => Value::String(s),
        Literal::HexBytes(b) | Literal::BitBytes(b) => Value::Bytes(b),
    }
}

/// Keep ordinary exact-numeric parameters in the same allocation-free lane
/// as non-parameterized DECIMAL literals.  Leaving them as `Value::Decimal`
/// makes every comparison parse the same string again for every input row.
///
/// Tokens wider than i128 deliberately stay textual: unlike a materialized
/// DECIMAL expression, comparison operands may exceed MySQL's 65-digit
/// storage width, so the original spelling remains their exact provenance.
#[inline]
fn compact_auto_param_decimal(text: &str) -> Option<Value> {
    parse_exact_decimal_str(text).map(|(magnitude, scale)| {
        Value::from_exact_decimal(ExactDecimal::from_i128(magnitude, scale))
    })
}

/// Decode the source spelling of one unsigned numeric SQL literal with the
/// same value representation used by [`literal_to_value`].  Lightweight text
/// shape scanners use this only as a candidate generator; their output is
/// still compared with the full parser/auto-parameterizer before a reusable
/// artifact is published.
pub(crate) fn scanned_numeric_literal_value(text: &str) -> Option<Value> {
    if text.contains(['e', 'E']) {
        return text.parse::<f64>().ok().map(Value::Double);
    }
    if text.contains('.') {
        return parse_exact_decimal_str(text)
            .map(|(magnitude, scale)| {
                Value::from_exact_decimal(ExactDecimal::from_i128(magnitude, scale))
            })
            .or_else(|| Some(Value::Decimal(text.to_string())));
    }
    if let Ok(value) = text.parse::<i64>() {
        Some(Value::Int(value))
    } else {
        text.parse::<u64>().ok().map(Value::UInt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_statement;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn auto(sql: &str) -> (Statement, Vec<Value>) {
        let mut stmt = parse_statement(sql).unwrap();
        let result = auto_parameterize(AutoParameterizeMode::Select, &mut stmt);
        (stmt, result.params)
    }

    fn placeholder_count<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>) -> usize {
        let stmt = stmt.into();
        let mut counter = PlaceholderCounter::default();
        if let crate::parser::QueryRef::Select(s) = stmt {
            crate::parser::visit::visit_select(&mut counter, s);
        }
        counter.next_index()
    }

    #[test]
    fn nested_placeholders_do_not_collide_with_extracted_literals() {
        for sql in [
            "SELECT (SELECT ?) FROM t WHERE id=7",
            "SELECT ROW_NUMBER() OVER (ORDER BY ?) FROM t WHERE id=7",
            "SELECT id FROM (SELECT ? AS id) d WHERE id=7",
            "SELECT id FROM t WHERE id=7 GROUP BY ?",
        ] {
            let mut stmt = parse_statement(sql).unwrap();
            crate::parser::visit::number_statement_placeholders(&mut stmt, &mut 5);
            let result = auto_parameterize(AutoParameterizeMode::Select, &mut stmt);
            assert_eq!(result.params, vec![Value::Int(7)], "{sql}");
            let Statement::Select(select) = stmt else {
                panic!("SELECT expected")
            };
            let Some(Expr::Binary { right, .. }) = select.selection else {
                panic!("predicate expected")
            };
            assert_eq!(*right, Expr::Placeholder(6), "{sql}");
        }
    }

    #[test]
    fn dml_parameter_scan_reserves_query_and_tail_slots() {
        use crate::parser::visit::{visit_statement, AstVisitor};
        #[derive(Default)]
        struct Indices(Vec<usize>);
        impl AstVisitor for Indices {
            fn enter_expr(&mut self, expr: &Expr) {
                if let Expr::Placeholder(index) = expr {
                    self.0.push(*index);
                }
            }
            fn enter_limit(&mut self, limit: &LimitValue) {
                if let LimitValue::Placeholder(index) | LimitValue::OffsetCommaPlaceholder(index) =
                    limit
                {
                    self.0.push(*index);
                }
            }
        }
        for sql in [
            "UPDATE t SET v=7 ORDER BY ? LIMIT ?",
            "DELETE FROM t WHERE id=7 ORDER BY ? LIMIT ?",
            "INSERT INTO t SELECT ? UNION ALL SELECT ? ON DUPLICATE KEY UPDATE id=7",
        ] {
            let mut stmt = parse_statement(sql).unwrap();
            let mut next = 5;
            crate::parser::visit::number_statement_placeholders(&mut stmt, &mut next);
            let result = auto_parameterize(AutoParameterizeMode::All, &mut stmt);
            assert_eq!(result.params, vec![Value::Int(7)], "{sql}");
            let mut indices = Indices::default();
            visit_statement(&mut indices, &stmt);
            indices.0.sort_unstable();
            assert_eq!(indices.0, (5..=next).collect::<Vec<_>>(), "{sql}");
        }
    }

    #[test]
    fn rewrites_where_equality_literal() {
        let mut stmt = parse_statement("SELECT * FROM t WHERE id = 42").unwrap();
        let result = auto_parameterize(AutoParameterizeMode::Select, &mut stmt);
        assert_eq!(result.params, vec![Value::Int(42)]);
        assert_eq!(placeholder_count(&stmt), 1);
        assert!(
            result.fingerprint.is_some(),
            "equality literals can share a generic plan-cache key"
        );
    }

    #[test]
    fn rewrites_multiple_predicates() {
        let (_stmt, params) = auto("SELECT * FROM t WHERE id = 42 AND name = 'foo'");
        assert_eq!(
            params,
            vec![Value::Int(42), Value::String("foo".to_string())]
        );
    }

    #[test]
    fn keeps_between_bounds_literal() {
        let mut stmt = parse_statement("SELECT * FROM t WHERE id BETWEEN 10 AND 20").unwrap();
        let original = stmt.clone();
        let result = auto_parameterize(AutoParameterizeMode::Select, &mut stmt);
        assert!(result.params.is_empty());
        assert_eq!(stmt, original);
        assert!(
            result.fingerprint.is_none(),
            "BETWEEN selectivity is value-sensitive and must use the normalized SQL cache key"
        );
    }

    #[test]
    fn range_comparison_preserves_literal_bounds_and_literal_keyed_cache() {
        let mut stmt = parse_statement("SELECT * FROM t WHERE created_at < '2020-01-01'").unwrap();
        let original = stmt.clone();
        let result = auto_parameterize(AutoParameterizeMode::Select, &mut stmt);
        assert!(result.params.is_empty());
        assert_eq!(stmt, original);
        assert!(
            result.fingerprint.is_none(),
            "range selectivity is value-sensitive and must not share one generic plan"
        );
    }

    #[test]
    fn rewrites_negative_literal_as_single_value() {
        let (_stmt, params) = auto("SELECT * FROM t WHERE id = -7");
        assert_eq!(params, vec![Value::Int(-7)]);
    }

    #[test]
    fn rewrites_limit_offset_with_magnitude_bucket() {
        let mut stmt = parse_statement("SELECT * FROM t WHERE id = 1 LIMIT 10 OFFSET 20").unwrap();
        let result = auto_parameterize(AutoParameterizeMode::Select, &mut stmt);
        assert_eq!(
            result.params,
            vec![Value::Int(1), Value::UInt(10), Value::UInt(20)]
        );
        let Statement::Select(select) = &stmt else {
            panic!("expected SELECT");
        };
        assert_eq!(select.limit, Some(LimitValue::Placeholder(1)));
        assert_eq!(select.offset, Some(LimitValue::Placeholder(2)));
        let fp = result.fingerprint.expect("limit rewrite keeps fingerprint");
        assert!(
            fp.ends_with(":l10:o20"),
            "fingerprint must append hex magnitude buckets (16/32): {fp}"
        );
    }

    #[test]
    fn limit_values_share_fingerprint_within_bucket_only() {
        // 10 and 12 share next_power_of_two = 16; 10 and 1000 do not.
        // Magnitude-bucketed keys bound how far the planning-time peek
        // (TopK heap size, row goals) can drift from the runtime value.
        let fp_10 = fingerprint_of("SELECT * FROM t ORDER BY a LIMIT 10");
        let fp_12 = fingerprint_of("SELECT * FROM t ORDER BY a LIMIT 12");
        let fp_1000 = fingerprint_of("SELECT * FROM t ORDER BY a LIMIT 1000");
        assert!(fp_10.is_some());
        assert_eq!(fp_10, fp_12);
        assert_ne!(fp_10, fp_1000);
    }

    #[test]
    fn limit_only_select_is_parameterized() {
        // No WHERE literal at all — the LIMIT alone justifies a shared
        // plan-cache entry.
        let (stmt, params) = auto("SELECT * FROM t LIMIT 50");
        assert_eq!(params, vec![Value::UInt(50)]);
        let Statement::Select(select) = &stmt else {
            panic!("expected SELECT");
        };
        assert_eq!(select.limit, Some(LimitValue::Placeholder(0)));
    }

    #[test]
    fn rewrites_in_list_with_pow2_padding() {
        // IN (1,2,3) pads to 4 members by repeating the last value —
        // semantically a no-op for IN/NOT IN — so arities 3 and 4 share
        // one plan shape.
        let (stmt, params) = auto("SELECT * FROM t WHERE id IN (1, 2, 3)");
        assert_eq!(
            params,
            vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(3),]
        );
        assert_eq!(placeholder_count(&stmt), 4);
        let fp_3 = fingerprint_of("SELECT * FROM t WHERE id IN (1, 2, 3)");
        let fp_4 = fingerprint_of("SELECT * FROM t WHERE id IN (7, 8, 9, 10)");
        assert_eq!(fp_3, fp_4, "arities 3 and 4 share the padded shape");
        let fp_2 = fingerprint_of("SELECT * FROM t WHERE id IN (1, 2)");
        assert_ne!(fp_3, fp_2, "arity buckets 2 and 4 stay distinct");
    }

    #[test]
    fn in_list_with_non_literal_member_untouched() {
        let (_stmt, params) = auto("SELECT * FROM t WHERE id IN (1, other_col, 3)");
        assert!(
            params.is_empty(),
            "mixed literal / column lists keep their literals"
        );
    }

    #[test]
    fn long_in_list_untouched() {
        let members: Vec<String> = (0..65).map(|i| i.to_string()).collect();
        let sql = format!("SELECT * FROM t WHERE id IN ({})", members.join(", "));
        let (_stmt, params) = auto(&sql);
        assert!(params.is_empty(), "lists past 64 members keep literals");
    }

    #[test]
    fn negated_in_list_also_parameterized() {
        // Padding duplicates are harmless for NOT IN too: `x NOT IN
        // (1,2,2)` ≡ `x NOT IN (1,2)` under three-valued logic.
        let (_stmt, params) = auto("SELECT * FROM t WHERE id NOT IN (1, 2, 3)");
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn skips_like_pattern() {
        let (_stmt, params) = auto("SELECT * FROM t WHERE name LIKE 'foo%'");
        assert!(params.is_empty(), "LIKE patterns drive index strategy");
    }

    #[test]
    fn skips_inside_function_args_but_rewrites_outer_comparison() {
        // The literal `5` is the RHS of a Binary comparison and should be
        // parameterized like any other top-level comparison constant.
        // Function arguments themselves (e.g. literals INSIDE LENGTH(...))
        // are left alone — the rewrite stops at Function nodes.
        let (_stmt, params) = auto("SELECT * FROM t WHERE LENGTH(name) = 5");
        assert_eq!(params, vec![Value::Int(5)]);

        let (_stmt, params) = auto("SELECT * FROM t WHERE LENGTH(name) > LENGTH('xx')");
        assert!(
            params.is_empty(),
            "literals nested inside function args must not be rewritten"
        );
    }

    #[test]
    fn skips_projection_literals() {
        let (_stmt, params) = auto("SELECT 1, name FROM t WHERE id = 5");
        assert_eq!(params, vec![Value::Int(5)], "only WHERE/JOIN is rewritten");
    }

    #[test]
    fn off_mode_is_a_noop() {
        let mut stmt = parse_statement("SELECT * FROM t WHERE id = 42").unwrap();
        let result = auto_parameterize(AutoParameterizeMode::Off, &mut stmt);
        assert!(result.params.is_empty());
    }

    #[test]
    fn non_select_statements_unchanged() {
        let mut stmt = parse_statement("INSERT INTO t VALUES (1, 'a')").unwrap();
        let result = auto_parameterize(AutoParameterizeMode::Select, &mut stmt);
        assert!(result.params.is_empty());
    }

    fn fingerprint_of(sql: &str) -> Option<String> {
        let mut stmt = parse_statement(sql).unwrap();
        auto_parameterize(AutoParameterizeMode::Select, &mut stmt).fingerprint
    }

    #[test]
    fn constant_predicates_remain_part_of_the_shared_plan_key() {
        for join in ["AND", "OR"] {
            for constant in ["1=1", "1=0", "2+2=4", "NULL", "0", "1"] {
                let sql = format!("SELECT * FROM t WHERE ({constant}) {join} id = 42");
                let (statement, params) = auto(&sql);
                assert_eq!(params, vec![Value::Int(42)], "{sql}: {statement:?}");
            }
            let true_key = fingerprint_of(&format!("SELECT * FROM t WHERE 1=1 {join} id=42"));
            assert!(true_key.is_some());
            assert_eq!(
                true_key,
                fingerprint_of(&format!("SELECT * FROM t WHERE 1=1 {join} id=99"))
            );
            assert_ne!(
                true_key,
                fingerprint_of(&format!("SELECT * FROM t WHERE 1=0 {join} id=42"))
            );
        }
    }

    #[test]
    fn fingerprint_is_stable_across_invocations() {
        // Same SQL parsed twice must produce the same fingerprint.
        // This is the core contract that lets the cross-session plan
        // cache key on it.
        let fp_a = fingerprint_of("SELECT * FROM t WHERE id = 42");
        let fp_b = fingerprint_of("SELECT * FROM t WHERE id = 42");
        assert_eq!(fp_a, fp_b);
        assert!(fp_a.is_some());
    }

    #[test]
    fn fingerprint_collapses_literal_variations() {
        // Two queries that differ only in the *magnitude* of their
        // extracted literals share a plan-cache entry — the whole
        // point of auto-parameterization. Both literals get replaced
        // by the same `Placeholder(0)` and both are extracted as
        // `Value::Int`, so the type-tag suffix matches too.
        let fp_1 = fingerprint_of("SELECT * FROM t WHERE id = 42");
        let fp_2 = fingerprint_of("SELECT * FROM t WHERE id = 9999");
        assert_eq!(fp_1, fp_2);
        // Different literal *type* must NOT share a fingerprint (B9):
        // `x = 1` and `x = '1'` route through different comparison
        // coercion in the planner, so the plan chosen for one is not
        // safe to reuse for the other.
        let fp_str = fingerprint_of("SELECT * FROM t WHERE id = 'x'");
        assert_ne!(fp_1, fp_str);
    }

    #[test]
    fn fingerprint_distinguishes_shape() {
        // Different column name → different shape → different fp.
        let fp_id = fingerprint_of("SELECT * FROM t WHERE id = 1");
        let fp_name = fingerprint_of("SELECT * FROM t WHERE name = 'x'");
        assert_ne!(fp_id, fp_name);
        // Additional predicate → different shape.
        let fp_and = fingerprint_of("SELECT * FROM t WHERE id = 1 AND name = 'x'");
        assert_ne!(fp_id, fp_and);
        assert_ne!(fp_name, fp_and);
        // Different table → different shape.
        let fp_u = fingerprint_of("SELECT * FROM u WHERE id = 1");
        assert_ne!(fp_id, fp_u);
    }

    #[test]
    fn all_mode_rewrites_update_set_and_where() {
        let mut stmt =
            parse_statement("UPDATE warehouse SET w_ytd = w_ytd + 100.5 WHERE w_id = 7").unwrap();
        let result = auto_parameterize(AutoParameterizeMode::All, &mut stmt);
        assert_eq!(
            result.params,
            vec![Value::DecimalCompact(1005, 1), Value::Int(7)],
            "Literals in SET and WHERE equality must both be extracted"
        );
        let Statement::Update(update) = &stmt else {
            panic!("expected UPDATE");
        };
        let mut max_seen = None;
        for a in &update.assignments {
            walk_max_placeholder(&a.value, &mut max_seen);
        }
        if let Some(sel) = &update.selection {
            walk_max_placeholder(sel, &mut max_seen);
        }
        assert_eq!(
            max_seen,
            Some(1),
            "Placeholders 0 and 1 must be present in the AST"
        );
        assert!(result.fingerprint.is_some());
    }

    #[test]
    fn truthy_env_values_enable_all_mode() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
        for value in ["true", "on", "1", "yes", "all", "dml"] {
            std::env::set_var("MYDB_AUTO_PARAMETERIZE", value);
            assert_eq!(AutoParameterizeMode::from_env(), AutoParameterizeMode::All);
        }
        std::env::set_var("MYDB_AUTO_PARAMETERIZE", "select");
        assert_eq!(
            AutoParameterizeMode::from_env(),
            AutoParameterizeMode::Select
        );
        match prior {
            Some(v) => std::env::set_var("MYDB_AUTO_PARAMETERIZE", v),
            None => std::env::remove_var("MYDB_AUTO_PARAMETERIZE"),
        }
    }

    #[test]
    fn unset_env_defaults_to_select_mode() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prior = std::env::var("MYDB_AUTO_PARAMETERIZE").ok();
        std::env::remove_var("MYDB_AUTO_PARAMETERIZE");
        assert_eq!(
            AutoParameterizeMode::from_env(),
            AutoParameterizeMode::Select
        );
        match prior {
            Some(v) => std::env::set_var("MYDB_AUTO_PARAMETERIZE", v),
            None => std::env::remove_var("MYDB_AUTO_PARAMETERIZE"),
        }
    }

    #[test]
    fn all_mode_rewrites_delete_where_and_small_insert_values() {
        let mut stmt =
            parse_statement("DELETE FROM new_order WHERE no_o_id = 3001 AND no_d_id = 5").unwrap();
        let result = auto_parameterize(AutoParameterizeMode::All, &mut stmt);
        assert_eq!(result.params, vec![Value::Int(3001), Value::Int(5)]);

        let mut stmt =
            parse_statement("INSERT INTO history (h_w_id, h_amount, h_data) VALUES (4, 10.0, 'x')")
                .unwrap();
        let result = auto_parameterize(AutoParameterizeMode::All, &mut stmt);
        assert_eq!(
            result.params,
            vec![
                Value::Int(4),
                Value::DecimalCompact(100, 1),
                Value::String("x".to_string())
            ]
        );
    }

    #[test]
    fn exact_decimal_params_use_fixed_width_runtime_lanes() {
        let (_stmt, params) =
            auto("SELECT * FROM t WHERE price IN (100.00,12345678901234567890.1234)");
        assert_eq!(
            params,
            vec![
                Value::DecimalCompact(10_000, 2),
                Value::Decimal128(123_456_789_012_345_678_901_234, 4),
            ]
        );
    }

    #[test]
    fn overwide_decimal_param_preserves_source_text() {
        let token = "1234567890123456789012345678901234567890.5";
        let (_stmt, params) = auto(&format!("SELECT * FROM t WHERE price = {token}"));
        assert_eq!(params, vec![Value::Decimal(token.to_string())]);
    }

    #[test]
    fn all_mode_skips_bulk_insert_values() {
        let rows: Vec<String> = (0..8).map(|i| format!("({i}, 'r{i}')")).collect();
        let sql = format!("INSERT INTO t (a, b) VALUES {}", rows.join(", "));
        let mut stmt = parse_statement(&sql).unwrap();
        let result = auto_parameterize(AutoParameterizeMode::All, &mut stmt);
        assert!(
            result.params.is_empty(),
            "Bulk VALUES (>4 rows) retains literals to avoid unproductive parameter extraction"
        );
    }

    #[test]
    fn select_mode_leaves_dml_untouched() {
        let mut stmt = parse_statement("UPDATE t SET a = 1 WHERE id = 2").unwrap();
        let result = auto_parameterize(AutoParameterizeMode::Select, &mut stmt);
        assert!(result.params.is_empty());
        assert!(result.fingerprint.is_none());
    }

    #[test]
    fn all_mode_rewrites_odku_values() {
        let mut stmt = parse_statement(
            "INSERT INTO t (id, cnt) VALUES (1, 2) ON DUPLICATE KEY UPDATE cnt = cnt + 3",
        )
        .unwrap();
        let result = auto_parameterize(AutoParameterizeMode::All, &mut stmt);
        assert_eq!(
            result.params,
            vec![Value::Int(1), Value::Int(2), Value::Int(3)]
        );
    }

    #[test]
    fn fingerprint_has_short_fixed_length() {
        // Guards against regression to `format!("{:?}", stmt)` — that
        // would produce a length proportional to AST size. The
        // fingerprint now has the shape `ap:<32 hex>:t<one tag per
        // param>`, so length = 35 (fixed head) + 2 (":t" separator) +
        // N (params).
        let fp = fingerprint_of("SELECT * FROM t WHERE id = 42").unwrap();
        // 35 head + ":t" + 1 param tag = 38.
        assert_eq!(fp.len(), 38, "expected `ap:<32 hex>:t<tag>`");
        assert!(fp.starts_with("ap:"));
        assert!(
            fp[3..35].chars().all(|c| c.is_ascii_hexdigit()),
            "hex tail must be lowercase hex: {fp}"
        );
        assert!(fp[35..].starts_with(":t"));
        // The hex tail is the literal hex rendering, not somehow
        // truncated debug output.
        let big = fingerprint_of(
            "SELECT * FROM very_long_table_name_with_many_columns \
             WHERE col_a = 1 AND col_b = 2 AND col_c = 'hello world'",
        )
        .unwrap();
        // 35 head + ":t" + 3 param tags = 40.
        assert_eq!(
            big.len(),
            40,
            "fingerprint length must be O(number of params), not AST size"
        );
    }
}
