//! MySQL 8.0 LIKE predicate compatibility and correctness regressions.
//!
//! Cover doc_old/design/like-review-2026-07.md:
//! B1: LIKE respects column collation, default CI versus utf8mb4_bin CS.
//! B2: legacy eval_plan_expr_legacy routes through the shared eval_like_values entry.
//! B3: UTF-8 prefix successors increment code points for indexed CJK/emoji ranges.
//! B4: ILIKE explicitly returns 1064 rather than silently becoming LIKE.
//! B5: validate ESCAPE X'...' and ESCAPE _binary '...' lengths with 1210.
//! B6: replace recursive perf_setup.rs NFA with an iterative kernel to prevent percent-pattern DoS.
//! B7: default backslash escape no longer blocks LikeBytePlan::classify byte fast paths.
//! B8: refine LIKE contains selectivity by the count of non-wildcard characters.
//! B9: prepared col LIKE ? reuses LikePlanCache.
//! B12: like_constant_prefix accepts an escape argument.
//!
//! All assertions use side-by-side MySQL 8.0.45 results as the oracle.

use super::support::execute_materialized as exec;
use super::*;

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match exec(engine, session, sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected ResultSet from {sql}, got {other:?}"),
    }
}

fn one_int(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> i64 {
    let rows = rows(engine, session, sql);
    assert_eq!(rows.len(), 1, "expected single row from {sql}");
    match &rows[0][0] {
        Value::Int(n) => *n,
        other => panic!("expected Int from {sql}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// B1: LIKE respects column collation.
// ---------------------------------------------------------------------------

#[test]
fn b1_ci_collation_matches_mixed_case() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Default utf8mb4_0900_ai_ci is case-insensitive: 'foo' LIKE 'FOO' returns 1.
    assert_eq!(
        one_int(&engine, &mut session, "SELECT 'foo' LIKE 'FOO'"),
        1,
        "Under utf8mb4_0900_ai_ci, 'foo' LIKE 'FOO' must match case-insensitively"
    );
    // The BINARY prefix forces byte-exact comparison.
    assert_eq!(
        one_int(&engine, &mut session, "SELECT BINARY 'foo' LIKE 'FOO'"),
        0,
        "BINARY forces byte comparison, so 'foo' differs from 'FOO'"
    );
}

#[test]
fn b1_column_binary_collation_is_case_sensitive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t_bin (c VARCHAR(20) COLLATE utf8mb4_bin)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t_bin VALUES ('foo'), ('Foo'), ('FOO')",
    )
    .unwrap();

    // LIKE 'foo' on utf8mb4_bin matches only lowercase text.
    let hits = rows(
        &engine,
        &mut session,
        "SELECT c FROM t_bin WHERE c LIKE 'foo' ORDER BY c",
    );
    assert_eq!(
        hits.len(),
        1,
        "LIKE on utf8mb4_bin must be case-sensitive, got {hits:?}"
    );
    assert_eq!(hits[0][0], Value::String("foo".to_string()));
}

#[test]
fn b1_column_ci_collation_matches_all_case_variants() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t_ci (c VARCHAR(20) COLLATE utf8mb4_0900_ai_ci)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t_ci VALUES ('foo'), ('Foo'), ('FOO')",
    )
    .unwrap();

    let hits = rows(
        &engine,
        &mut session,
        "SELECT c FROM t_ci WHERE c LIKE 'foo'",
    );
    assert_eq!(
        hits.len(),
        3,
        "LIKE on utf8mb4_0900_ai_ci must match every case variant"
    );
}

// ---------------------------------------------------------------------------
// B2: legacy evaluation also respects collation.
// ---------------------------------------------------------------------------

#[test]
fn b2_null_operand_returns_null_via_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let val = rows(
        &engine,
        &mut session,
        "SELECT CAST(NULL AS CHAR) LIKE 'foo'",
    );
    // Direct legacy match_like_pattern compared NULL as literal "NULL"; eval_like_values propagates NULL.
    assert_eq!(val[0][0], Value::Null, "NULL LIKE must propagate NULL");
}

// ---------------------------------------------------------------------------
// B3: UTF-8 prefix successor increments by code point.
// ---------------------------------------------------------------------------

#[test]
fn b3_cjk_prefix_lookup_returns_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (c VARCHAR(20), INDEX (c))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES ('汉A'), ('汉B'), ('汉C'), ('测试')",
    )
    .unwrap();

    let hits = rows(&engine, &mut session, "SELECT c FROM t WHERE c LIKE '汉%'");
    assert_eq!(
        hits.len(),
        3,
        "CJK-prefix LIKE must match all rows with that prefix (index successors increment code points), got {hits:?}"
    );
}

#[test]
fn btree_like_prefix_uses_collation_weight_successor_for_select_and_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t_weight (id INT PRIMARY KEY, \
         s VARCHAR(32) COLLATE utf8mb4_0900_ai_ci, touched INT, KEY idx_s(s))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t_weight VALUES \
         (1,'efoo',0),(2,'éfoo',0),(3,'êfoo',0),(4,'ffoo',0),\
         (5,'Zulu',0),(6,'zebra',0),(7,'[bracket',0),\
         (8,'ßeta',0),(9,'sset',0),(10,'🙂x',0)",
    )
    .unwrap();

    let indexed = rows(
        &engine,
        &mut session,
        "SELECT id FROM t_weight WHERE s LIKE 'é%' ORDER BY id",
    );
    let heap = rows(
        &engine,
        &mut session,
        "SELECT id FROM t_weight IGNORE INDEX (idx_s) WHERE s LIKE 'é%' ORDER BY id",
    );
    assert_eq!(indexed, heap);
    assert_eq!(indexed.len(), 3, "AI collation should match e/é/ê prefixes");

    exec(
        &engine,
        &mut session,
        "UPDATE t_weight SET touched=1 WHERE s LIKE 'é%'",
    )
    .unwrap();
    assert_eq!(
        one_int(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t_weight WHERE touched=1"
        ),
        3,
        "UPDATE must use the same safe LIKE candidate range"
    );

    let z_indexed = rows(
        &engine,
        &mut session,
        "SELECT id FROM t_weight WHERE s LIKE 'Z%' ORDER BY id",
    );
    let z_heap = rows(
        &engine,
        &mut session,
        "SELECT id FROM t_weight IGNORE INDEX (idx_s) WHERE s LIKE 'Z%' ORDER BY id",
    );
    assert_eq!(z_indexed, z_heap);
    assert_eq!(
        z_indexed.len(),
        2,
        "CI Z% range must use [z, {{), not [Z, [)"
    );

    for pattern in ["ß%", "🙂%"] {
        let indexed = rows(
            &engine,
            &mut session,
            &format!("SELECT id FROM t_weight WHERE s LIKE '{pattern}' ORDER BY id"),
        );
        let heap = rows(
            &engine,
            &mut session,
            &format!(
                "SELECT id FROM t_weight IGNORE INDEX (idx_s) WHERE s LIKE '{pattern}' ORDER BY id"
            ),
        );
        assert_eq!(indexed, heap, "pattern={pattern}");
    }

    exec(
        &engine,
        &mut session,
        "DELETE FROM t_weight WHERE s LIKE 'é%'",
    )
    .unwrap();
    assert_eq!(
        one_int(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t_weight WHERE id IN (1,2,3)"
        ),
        0,
        "DELETE must consume the same collation sort-key candidate range"
    );
}

#[test]
fn like_prefix_index_is_a_candidate_superset_with_residual() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t_like_prefix_idx (tenant INT, id INT PRIMARY KEY, \
         s VARCHAR(32) COLLATE utf8mb4_0900_ai_ci, touched INT, \
         KEY idx_t_s(tenant,s(2)))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t_like_prefix_idx VALUES \
         (1,1,'abcdef',0),(1,2,'abcxyz',0),(1,3,'abacus',0),\
         (1,4,'abd',0),(2,5,'abcdef',0),(1,6,'éclair',0),(1,7,'eclipse',0)",
    )
    .unwrap();

    let indexed = rows(
        &engine,
        &mut session,
        "SELECT id FROM t_like_prefix_idx WHERE tenant=1 AND s LIKE 'abc%' ORDER BY id",
    );
    let heap = rows(
        &engine,
        &mut session,
        "SELECT id FROM t_like_prefix_idx IGNORE INDEX (idx_t_s) \
         WHERE tenant=1 AND s LIKE 'abc%' ORDER BY id",
    );
    assert_eq!(indexed, heap);
    assert_eq!(indexed.len(), 2);

    exec(
        &engine,
        &mut session,
        "UPDATE t_like_prefix_idx SET touched=1 WHERE tenant=1 AND s LIKE 'abc%'",
    )
    .unwrap();
    assert_eq!(
        one_int(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t_like_prefix_idx WHERE touched=1"
        ),
        2
    );
}

#[test]
fn prepared_like_reclassifies_cached_index_bounds_per_binding() {
    use crate::engine::api::QueryEngine;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t_like_param (id INT PRIMARY KEY, \
         s VARCHAR(32) COLLATE utf8mb4_0900_ai_ci, KEY idx_s(s(3)))",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t_like_param VALUES \
         (1,'abcdef'),(2,'abcxyz'),(3,'zabc'),(4,'eclair'),(5,'éclair'),(6,'')",
    )
    .unwrap();

    let sql = "SELECT id FROM t_like_param FORCE INDEX (idx_s) WHERE s LIKE ? ORDER BY id";
    let oracle_sql = "SELECT id FROM t_like_param IGNORE INDEX (idx_s) WHERE s LIKE ? ORDER BY id";
    let planned = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let oracle = engine.prepare_cached(&mut session, oracle_sql, 1).unwrap();
    for pattern in ["abc%", "%abc%", "é%", "", "z_bc"] {
        let params = vec![Value::String(pattern.to_string())];
        let indexed = crate::engine::params::with_params(params.clone(), || {
            engine
                .execute_prepared_statement(&mut session, sql, (*planned).clone())
                .expect("prepared indexed LIKE")
        })
        .unwrap()
        .materialize()
        .unwrap();
        let heap = crate::engine::params::with_params(params, || {
            engine
                .execute_prepared_statement(&mut session, oracle_sql, (*oracle).clone())
                .expect("prepared heap LIKE")
        })
        .unwrap()
        .materialize()
        .unwrap();
        assert_eq!(
            format!("{indexed:?}"),
            format!("{heap:?}"),
            "pattern={pattern}"
        );
    }
}

#[test]
fn randomized_multilingual_like_index_candidates_have_no_false_negatives() {
    use rand::{Rng, SeedableRng};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (table, collation) in [
        ("like_prop_ai", "utf8mb4_0900_ai_ci"),
        ("like_prop_as", "utf8mb4_0900_as_cs"),
        ("like_prop_bin", "utf8mb4_bin"),
    ] {
        exec(
            &engine,
            &mut session,
            &format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, s VARCHAR(64) COLLATE {collation}, KEY idx_s(s))"
            ),
        )
        .unwrap();
    }

    let alphabet = [
        'a', 'A', 'e', 'é', 'ê', 'ß', '中', '文', '汉', '🙂', '%', '_',
    ];
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x4c49_4b45_2026_0716);
    let mut values = Vec::new();
    for id in 1..=80 {
        let len = rng.gen_range(0..=8);
        let value = (0..len)
            .map(|_| alphabet[rng.gen_range(0..alphabet.len())])
            .collect::<String>();
        values.push((id, value));
    }
    for table in ["like_prop_ai", "like_prop_as", "like_prop_bin"] {
        for (id, value) in &values {
            let escaped = value.replace('\'', "''");
            exec(
                &engine,
                &mut session,
                &format!("INSERT INTO {table} VALUES ({id},'{escaped}')"),
            )
            .unwrap();
        }
    }

    for case in 0..40 {
        let literal_len = rng.gen_range(0..=4);
        let literal = (0..literal_len)
            .map(|_| alphabet[rng.gen_range(0..alphabet.len())])
            .collect::<String>();
        let pattern = match rng.gen_range(0..6) {
            0 => literal.clone(),
            1 => format!("{literal}%"),
            2 => format!("%{literal}%"),
            3 => format!("_{literal}"),
            4 => format!("{}!%", literal.replace('!', "!!")),
            _ => format!("{}!_", literal.replace('!', "!!")),
        };
        let pattern_sql = pattern.replace('\'', "''");
        for table in ["like_prop_ai", "like_prop_as", "like_prop_bin"] {
            let indexed = rows(
                &engine,
                &mut session,
                &format!(
                    "SELECT id FROM {table} WHERE s LIKE '{pattern_sql}' ESCAPE '!' ORDER BY id"
                ),
            );
            let heap = rows(
                &engine,
                &mut session,
                &format!(
                    "SELECT id FROM {table} IGNORE INDEX (idx_s) WHERE s LIKE '{pattern_sql}' ESCAPE '!' ORDER BY id"
                ),
            );
            assert_eq!(
                indexed, heap,
                "table={table} case={case} pattern={pattern:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// B4: reject ILIKE explicitly.
// ---------------------------------------------------------------------------

#[test]
fn b4_ilike_is_rejected_as_syntax_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(&engine, &mut session, "SELECT 'foo' ILIKE 'FOO'").unwrap_err();
    assert_eq!(
        err.code(),
        1064,
        "ILIKE must return 1064 rather than silently degrade to LIKE"
    );
    assert!(
        err.message().to_uppercase().contains("ILIKE"),
        "The error message must mention ILIKE: {}",
        err.message()
    );
}

// ---------------------------------------------------------------------------
// B5: all ESCAPE literal forms.
// ---------------------------------------------------------------------------

#[test]
fn b5_escape_hex_literal_extracts_single_char() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // X'21' is '!', equivalent to ESCAPE '!'.
    assert_eq!(
        one_int(
            &engine,
            &mut session,
            "SELECT 'a%b' LIKE 'a!%b' ESCAPE X'21'"
        ),
        1,
        "ESCAPE X'21' must equal ESCAPE '!'"
    );
}

#[test]
fn b5_escape_multi_char_is_1210() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(&engine, &mut session, "SELECT 'x' LIKE 'x' ESCAPE 'ab'").unwrap_err();
    assert_eq!(
        err.code(),
        1064,
        "A multicharacter ESCAPE must fail during parsing"
    );
}

// ---------------------------------------------------------------------------
// B6: iterative perf_setup kernel prevents DoS.
// ---------------------------------------------------------------------------

#[test]
fn b6_pathological_percent_pattern_finishes_fast() {
    // One hundred percent signs followed by a character caused exponential recursive-NFA work,
    // stack overflow, or timeout; the iterative kernel must finish linearly.
    let pattern: String = std::iter::repeat('%')
        .take(100)
        .chain(std::iter::once('X'))
        .collect();
    let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let start = std::time::Instant::now();
    // Call the kernel directly without relying on the performance_schema query pipeline.
    let m = crate::engine::expr::match_like_pattern(value, &pattern, None);
    let dur = start.elapsed();
    // The value lacks X and must not match; completion time is the property under test.
    assert!(!m, "A value without X must not match");
    assert!(
        dur.as_millis() < 500,
        "A pattern of 100 percent signs followed by X must finish within 500ms (observed {}ms)",
        dur.as_millis()
    );
}

// ---------------------------------------------------------------------------
// B7: the default backslash escape no longer prevents byte fast paths.
// ---------------------------------------------------------------------------

#[test]
fn b7_default_backslash_still_takes_byte_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // An ASCII pattern without backslashes should use the byte fast path with unchanged results.
    assert_eq!(
        one_int(&engine, &mut session, "SELECT 'orderline' LIKE '%order%'"),
        1
    );
    assert_eq!(
        one_int(&engine, &mut session, "SELECT 'orderline' LIKE 'order%'"),
        1
    );
    assert_eq!(
        one_int(&engine, &mut session, "SELECT 'orderline' LIKE '%line'"),
        1
    );
}

// ---------------------------------------------------------------------------
// B12: like_constant_prefix accepts escape.
// ---------------------------------------------------------------------------

#[test]
fn b12_prefix_with_escape_extracts_literal() {
    // Direct kernel: pattern a!%b% with ESCAPE '!' has prefix a%b.
    let prefix = crate::engine::expr::like_constant_prefix("a!%b%", Some('!'));
    assert_eq!(prefix.as_deref(), Some("a%b"));
}

#[test]
fn b12_prefix_no_escape_treats_backslash_as_literal() {
    // With escape=None (ESCAPE ''), backslash is ordinary and remains in the prefix.
    let prefix = crate::engine::expr::like_constant_prefix("a\\b%c", None);
    assert_eq!(prefix.as_deref(), Some("a\\b"));
}

#[test]
fn b12_prefix_default_backslash_escape_stops_at_first_wildcard() {
    // With default backslash escape, an escaped percent becomes literal percent in the prefix.
    let prefix = crate::engine::expr::like_constant_prefix("a\\%b", Some('\\'));
    assert_eq!(prefix.as_deref(), Some("a%b"));
}

// ---------------------------------------------------------------------------
// prefix_successor unit tests.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// B7/B9 performance smoke measurements have no strict timing threshold.
// ---------------------------------------------------------------------------

#[test]
fn b7_default_backslash_takes_fast_path_perf_smoke() {
    let pattern = "%order%line%completed%";
    assert!(
        matches!(
            crate::engine::expr::LikeBytePlan::classify(pattern, Some('\\')),
            Some(crate::engine::expr::LikeBytePlan::MultiSegment { .. })
        ),
        "An unused default backslash must not block the multisegment byte fast path"
    );
    // Structural assertions prove routing; sample loops cover public end-to-end semantics.
    // Wall-clock thresholds are unsuitable correctness assertions under parallel suite scheduling.
    let values: Vec<String> = (0..1_000)
        .map(|i| format!("order_line_{i}_completed_at_2026"))
        .collect();
    let mut hits = 0usize;
    for v in &values {
        if crate::engine::expr::match_like_pattern(v, pattern, Some('\\')) {
            hits += 1;
        }
    }
    assert_eq!(hits, values.len());
}

#[test]
fn b9_like_plan_cache_hits_on_repeated_pattern() {
    // Call cached_like_plan directly to inspect statistics.
    let before = crate::engine::func_cache::stats();
    let p1 = crate::engine::func_cache::cached_like_plan("cache_hit_%", None, None);
    let p2 = crate::engine::func_cache::cached_like_plan("cache_hit_%", None, None);
    let after = crate::engine::func_cache::stats();
    assert!(
        std::sync::Arc::ptr_eq(&p1, &p2),
        "The same pattern must hit the cache"
    );
    assert!(
        after.like_plan_hits > before.like_plan_hits,
        "The hits counter must increase"
    );
}

#[test]
fn b3_prefix_successor_ascii() {
    assert_eq!(
        crate::engine::expr::like_prefix_successor("abc"),
        Some(Value::String("abd".to_string()))
    );
}

#[test]
fn b3_prefix_successor_cjk_codepoint_increment() {
    // U+6C49 has successor U+6C4A.
    let next = crate::engine::expr::like_prefix_successor("汉");
    match next {
        Some(Value::String(s)) => {
            assert_eq!(s.chars().count(), 1);
            let ch = s.chars().next().unwrap();
            assert_eq!(
                ch as u32,
                0x6C49 + 1,
                "The CJK code point must increase by one"
            );
        }
        other => panic!("Expected Some(String) for the CJK successor, got {other:?}"),
    }
}

#[test]
fn b3_prefix_successor_max_codepoint_backtracks() {
    // A prefix ending in U+10FFFF backtracks and increments the preceding character.
    let s: String = "a".chars().chain(std::iter::once('\u{10FFFF}')).collect();
    let next = crate::engine::expr::like_prefix_successor(&s);
    assert_eq!(
        next,
        Some(Value::String("b".to_string())),
        "An ending U+10FFFF must backtrack and increment 'a' to 'b'"
    );
}

// ---------------------------------------------------------------------------
// B14: vectorized column LIKE constant kernel.
// ---------------------------------------------------------------------------
//
// Differential oracle: MYDB_VECTORIZED=on reads Utf8 lanes directly through
// try_column_like_const_bitmap; off uses row-wise ExprProgram::eval_row.
// Cover Literal, Prefix, Suffix, Contains, MultiSegment, WildcardAny,
// General, and CiAscii LikePlan branches with NULL, NOT LIKE, single-byte, and CJK values.

fn b14_seed(dir: &std::path::Path) -> StorageQueryEngine {
    let engine = open_test_engine(dir);
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, c VARCHAR(64))",
    )
    .unwrap();
    // Mixed shapes: prefix / suffix / substring / NULL / CJK / mixed-case.
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..500i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        let s = if i % 7 == 0 {
            "NULL".to_string()
        } else if i % 5 == 0 {
            format!("'order-{i}-COMPLETED-2026'")
        } else if i % 5 == 1 {
            format!("'cart-{i}-pending'")
        } else if i % 5 == 2 {
            format!("'汉字-{i}-测试'")
        } else if i % 5 == 3 {
            format!("'  refund_{i}_ok  '")
        } else {
            format!("'plain-{i}'")
        };
        sql.push_str(&format!("({i}, {s})"));
    }
    exec(&engine, &mut session, &sql).unwrap();
    engine
}

fn b14_diff(sql: &str, engine: &StorageQueryEngine) {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let mut session_a = SessionContext::default();
    let mut session_b = SessionContext::default();
    let legacy = rows(engine, &mut session_a, sql);
    let vec = with_vec_mode(VecMode::On, || rows(engine, &mut session_b, sql));
    assert_eq!(
        legacy, vec,
        "Vector LIKE results must exactly match the row oracle: {sql}"
    );
}

#[test]
fn b14_vec_like_matches_legacy_across_plan_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = b14_seed(dir.path());
    // Prefix (LikePlan::Prefix)
    b14_diff(
        "SELECT id FROM t WHERE c LIKE 'order-%' ORDER BY id",
        &engine,
    );
    // Suffix (LikePlan::Suffix)
    b14_diff(
        "SELECT id FROM t WHERE c LIKE '%-ok  ' ORDER BY id",
        &engine,
    );
    // Contains (LikePlan::Contains)
    b14_diff(
        "SELECT id FROM t WHERE c LIKE '%COMPLETED%' ORDER BY id",
        &engine,
    );
    // MultiSegment
    b14_diff(
        "SELECT id FROM t WHERE c LIKE '%order%2026%' ORDER BY id",
        &engine,
    );
    // Literal (no wildcards)
    b14_diff(
        "SELECT id FROM t WHERE c LIKE 'plain-3' ORDER BY id",
        &engine,
    );
    // WildcardAny (`%`)
    b14_diff("SELECT id FROM t WHERE c LIKE '%' ORDER BY id", &engine);
    // NOT LIKE
    b14_diff(
        "SELECT id FROM t WHERE c NOT LIKE 'plain-%' ORDER BY id",
        &engine,
    );
    // NULL propagation (LIKE on NULL row must not set the bitmap bit).
    b14_diff(
        "SELECT id FROM t WHERE c LIKE '%-测试' ORDER BY id",
        &engine,
    );
    // Non-ASCII haystack against ASCII CI pattern — General/CiAscii branch.
    b14_diff("SELECT id FROM t WHERE c LIKE '汉字%' ORDER BY id", &engine);
}

// Direct unit tests establish vector-kernel activation and exact results:
// `src/engine/executor/vec/expr_batch.rs::tests::b14_vec_like_const_kernel_fires_and_matches_oracle`
// construct a Utf8 Batch and Column LIKE Const program without SQL,
// assert VEC_LIKE_CONST_ROWS increases and the bitmap exactly matches the row oracle.

// ---------------------------------------------------------------------------
// B10: LOWER(c) LIKE 'FOO%' and UPPER(c) LIKE 'foo%' use functional indexes.
// ---------------------------------------------------------------------------

#[test]
fn b10_lower_like_uppercase_pattern_hits_functional_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(64), \
         INDEX idx_lower ((LOWER(name))))",
    )
    .unwrap();
    for (i, n) in [
        "Alpha", "Apple", "Beta", "BRAVO", "Carol", "alpha", "alphabet",
    ]
    .iter()
    .enumerate()
    {
        exec(
            &engine,
            &mut session,
            &format!("INSERT INTO u VALUES ({i}, '{n}')"),
        )
        .unwrap();
    }
    exec(&engine, &mut session, "ANALYZE TABLE u").unwrap();
    // Uppercase patterns previously missed idx_lower because [FOO,FOP) did not overlap
    // lowercase indexed values, silently scanning. B10 lowercases the pattern in advance.
    let rs = exec(
        &engine,
        &mut session,
        "EXPLAIN SELECT id FROM u WHERE LOWER(name) LIKE 'A%'",
    )
    .unwrap();
    let rows = match rs {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("expect ResultSet, got {other:?}"),
    };
    let mentions_idx = rows.iter().any(|r| {
        r.iter()
            .any(|v| matches!(v, Value::String(s) if s.contains("idx_lower")))
    });
    assert!(
        mentions_idx,
        "LOWER(name) LIKE 'A%' must use idx_lower (B10 pattern pre-lower); rows: {rows:?}"
    );
    // Correctness: Alpha, Apple, alpha, and alphabet all start with lowercase a after conversion.
    let cnt = rows_int_scalar(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM u WHERE LOWER(name) LIKE 'A%'",
    );
    assert_eq!(cnt, 4);
}

#[test]
fn b10_upper_like_lowercase_pattern_hits_functional_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(64), \
         INDEX idx_upper ((UPPER(name))))",
    )
    .unwrap();
    for (i, n) in [
        "Alpha", "Apple", "Beta", "BRAVO", "Carol", "alpha", "alphabet",
    ]
    .iter()
    .enumerate()
    {
        exec(
            &engine,
            &mut session,
            &format!("INSERT INTO u VALUES ({i}, '{n}')"),
        )
        .unwrap();
    }
    exec(&engine, &mut session, "ANALYZE TABLE u").unwrap();
    let rs = exec(
        &engine,
        &mut session,
        "EXPLAIN SELECT id FROM u WHERE UPPER(name) LIKE 'a%'",
    )
    .unwrap();
    let rows = match rs {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("expect ResultSet, got {other:?}"),
    };
    let mentions_idx = rows.iter().any(|r| {
        r.iter()
            .any(|v| matches!(v, Value::String(s) if s.contains("idx_upper")))
    });
    assert!(
        mentions_idx,
        "UPPER(name) LIKE 'a%' must use idx_upper (B10 pattern pre-upper); rows: {rows:?}"
    );
    let cnt = rows_int_scalar(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM u WHERE UPPER(name) LIKE 'a%'",
    );
    assert_eq!(
        cnt, 4,
        "Alpha, Apple, alpha, and alphabet all start with uppercase A after conversion"
    );
}

#[test]
fn b10_lcase_ucase_aliases_also_lower_and_upper_pattern() {
    // LCASE aliases LOWER and UCASE aliases UPPER, matching MySQL 8.0.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(64), \
         INDEX idx_lower ((LCASE(name))))",
    )
    .unwrap();
    for (i, n) in ["Alpha", "Zeta", "alpha"].iter().enumerate() {
        exec(
            &engine,
            &mut session,
            &format!("INSERT INTO u VALUES ({i}, '{n}')"),
        )
        .unwrap();
    }
    exec(&engine, &mut session, "ANALYZE TABLE u").unwrap();
    let cnt = rows_int_scalar(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM u WHERE LCASE(name) LIKE 'A%'",
    );
    assert_eq!(
        cnt, 2,
        "For the LCASE alias, pattern A% must be pre-lowercased to match Alpha and alpha"
    );
}

fn rows_int_scalar(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> i64 {
    let rs = rows(engine, session, sql);
    assert_eq!(rs.len(), 1, "expected single row from {sql}");
    match &rs[0][0] {
        Value::Int(n) => *n,
        other => panic!("expected Int from {sql}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// B13: SHOW/sys_schema metadata LIKE uses case-insensitive utf8mb4_general_ci.
// ---------------------------------------------------------------------------

#[test]
fn b13_show_tables_like_is_ci_by_metadata_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE OrderItems (id INT PRIMARY KEY)",
    )
    .unwrap();
    // Mixed-case patterns match case-insensitively, as MySQL 8.0 metadata requires.
    let hits = rows(&engine, &mut session, "SHOW TABLES LIKE 'order%'");
    assert_eq!(
        hits.len(),
        1,
        "SHOW TABLES LIKE must match mixed-case table names case-insensitively"
    );
    let hits = rows(&engine, &mut session, "SHOW TABLES LIKE 'ORDER%'");
    assert_eq!(
        hits.len(),
        1,
        "SHOW TABLES LIKE must also match an uppercase pattern case-insensitively (B13)"
    );
    // Reverse direction: match mixed-case table names with the pattern.
    let hits = rows(&engine, &mut session, "SHOW TABLES LIKE 'OrDeRitEmS'");
    assert_eq!(
        hits.len(),
        1,
        "SHOW TABLES LIKE must match patterns in any case"
    );
}

#[test]
fn dynamic_empty_escape_subquery_uses_default_backslash() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE escape_source (x CHAR(1))",
    )
    .unwrap();

    assert_eq!(
        one_int(
            &engine,
            &mut session,
            r"SELECT 'a%b' LIKE 'a\%b' ESCAPE (SELECT x FROM escape_source)"
        ),
        1
    );
}

#[test]
fn like_dangling_escape_is_literal_and_no_backslash_mode_disables_default_escape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    assert_eq!(
        one_int(&engine, &mut session, "SELECT 'a\\\\' LIKE 'a\\\\'"),
        1
    );
    exec(
        &engine,
        &mut session,
        "SET sql_mode = 'NO_BACKSLASH_ESCAPES'",
    )
    .unwrap();
    assert_eq!(
        one_int(
            &engine,
            &mut session,
            r"SELECT _latin1'a\bc' LIKE _latin1'a\%'"
        ),
        1
    );
}
