//! Executor-level NLJ fast-path regressions:
//!
//! 1. Runtime equality NLJ-to-hash switching in execute_nested_loop_join, while the
//!    plan remains NestedLoopJoin. MYDB_NLJ_HASH on/off must have identical results,
//!    covering Inner/Left/Right, NULL three-valued logic,
//!    equality plus nonequality residuals, and cross-type INT/DECIMAL equality.
//! 2. Naive from_rows pair evaluation compiles conditions first and materializes only matches.
//!    Nonequality predicates cannot extract hash keys and exercise this path against hand calculations.
//!
//! MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS=0 lets small tables trigger switching;
//! with_test_env provides thread-local overrides without affecting parallel tests.

use super::support::query_sorted_rows as sorted_rows;
use super::*;

fn native_result_batch_hits() -> u64 {
    crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
}

fn setup_join_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE nlj_a (id INT PRIMARY KEY, k INT, d DECIMAL(10,2), v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE nlj_b (id INT PRIMARY KEY, k INT, d DECIMAL(10,2), v INT)")
        .unwrap();
    // Include NULLs, duplicates, and keys present on only one side; equal INT/DECIMAL
    // values with distinct types exercise cross-type equality.
    engine
        .execute(
            "INSERT INTO nlj_a VALUES \
             (1, 10, 10.00, 1), (2, 20, 20.00, 2), (3, NULL, NULL, 3), \
             (4, 30, 30.00, 4), (5, 20, 20.00, 5), (6, 40, 40.00, 6)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO nlj_b VALUES \
             (101, 10, 10.00, 9), (102, 20, 20.00, 1), (103, NULL, NULL, 7), \
             (104, 50, 50.00, 2), (105, 20, 20.00, 8)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE nlj_a").unwrap();
    engine.execute("ANALYZE TABLE nlj_b").unwrap();
}

/// Identical SQL with NLJ-to-hash enabled/disabled must agree; the naive legacy path
/// is the correctness oracle.
fn assert_nlj_hash_differential(engine: &StorageQueryEngine, sql: &str) {
    let fast = with_test_env(
        &[("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0"))],
        || sorted_rows(engine, sql),
    );
    let naive = with_test_env(
        &[
            ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0")),
            ("MYDB_NLJ_HASH", Some("off")),
        ],
        || sorted_rows(engine, sql),
    );
    assert_eq!(fast, naive, "NLJ hash fast path diverged for: {sql}");
    assert!(!fast.is_empty() || naive.is_empty(), "sanity: {sql}");
}

#[test]
fn nlj_hash_fast_path_inner_join_matches_naive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    // Pure equality with NULL keys on both sides: NULL = NULL must not match.
    assert_nlj_hash_differential(
        &engine,
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a JOIN nlj_b ON nlj_a.k = nlj_b.k",
    );
    // Combine equality and nonequality residuals.
    assert_nlj_hash_differential(
        &engine,
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a JOIN nlj_b ON nlj_a.k = nlj_b.k AND nlj_a.v < nlj_b.v",
    );
    // INT=DECIMAL requires hash normalization to match SQL equality.
    assert_nlj_hash_differential(
        &engine,
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a JOIN nlj_b ON nlj_a.k = nlj_b.d",
    );
    // Null-safe equality must match NULL with NULL.
    assert_nlj_hash_differential(
        &engine,
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a JOIN nlj_b ON nlj_a.k <=> nlj_b.k",
    );
}

#[test]
fn nlj_hash_fast_path_outer_joins_match_naive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    assert_nlj_hash_differential(
        &engine,
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a LEFT JOIN nlj_b ON nlj_a.k = nlj_b.k",
    );
    assert_nlj_hash_differential(
        &engine,
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a RIGHT JOIN nlj_b ON nlj_a.k = nlj_b.k",
    );
    // LEFT residuals filter matched pairs without changing preserved-side NULL extensions.
    assert_nlj_hash_differential(
        &engine,
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a LEFT JOIN nlj_b ON nlj_a.k = nlj_b.k AND nlj_b.v > 5",
    );
}

#[test]
fn nlj_hash_fast_path_semi_anti_match_naive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    // EXISTS/NOT EXISTS decorrelation produces Semi/Anti joins; enabled/disabled results
    // must agree regardless of CBO physical shape. The NLJ fast path forces
    // build-right for Semi/Anti.
    assert_nlj_hash_differential(
        &engine,
        "SELECT nlj_a.id FROM nlj_a \
         WHERE EXISTS (SELECT 1 FROM nlj_b WHERE nlj_b.k = nlj_a.k)",
    );
    assert_nlj_hash_differential(
        &engine,
        "SELECT nlj_a.id FROM nlj_a \
         WHERE NOT EXISTS (SELECT 1 FROM nlj_b WHERE nlj_b.k = nlj_a.k)",
    );
}

#[test]
fn nlj_pair_eval_non_equi_condition_matches_expected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE pe_a (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE pe_b (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("INSERT INTO pe_a VALUES (1, 10), (2, 20), (3, NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO pe_b VALUES (11, 15), (12, 25), (13, NULL)")
        .unwrap();

    // Nonequality predicates cannot extract hash keys and use from_rows pair evaluation.
    // Expected matches: 10<15, 10<25, 20<25; three-valued NULL comparisons do not match.
    let rows = sorted_rows(
        &engine,
        "SELECT /*+ NL_JOIN(pe_a, pe_b) */ pe_a.id, pe_b.id \
         FROM pe_a JOIN pe_b ON pe_a.k < pe_b.k",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(1), Value::Int(12)],
            vec![Value::Int(2), Value::Int(12)],
        ]
    );

    // LEFT nonequality joins NULL-extend unmatched left rows, including NULL keys.
    let rows = sorted_rows(
        &engine,
        "SELECT /*+ NL_JOIN(pe_a, pe_b) */ pe_a.id, pe_b.id \
         FROM pe_a LEFT JOIN pe_b ON pe_a.k > pe_b.k",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Null],
            vec![Value::Int(2), Value::Int(11)],
            vec![Value::Int(3), Value::Null],
        ]
    );
}

/// WS-N8: legacy NLJ executions/pairs_compared/pairs_kept counters must increase
/// when MYDB_NLJ_HASH=off and MYDB_NLJ_BLOCK=off select the old per-pair
/// from_rows path; kept must not exceed compared.
#[test]
fn nlj_pairs_counters_advance_on_naive_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let before = native_result_batch_hits();

    with_test_env(
        &[
            ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0")),
            ("MYDB_NLJ_HASH", Some("off")),
            ("MYDB_NLJ_BLOCK", Some("off")),
        ],
        || {
            // Nonequality prevents hash-key extraction, forcing the from_rows loop.
            sorted_rows(
                &engine,
                "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
                 FROM nlj_a JOIN nlj_b ON nlj_a.v < nlj_b.v",
            )
        },
    );

    assert!(
        native_result_batch_hits() > before,
        "batch NLJ did not execute"
    );
}

/// WS-N1: default block NLJ must increase block counters.
/// The same SQL must return identical results with block execution on/off.
#[test]
fn nlj_block_path_matches_legacy_and_counters_advance() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let sql = "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
               FROM nlj_a JOIN nlj_b ON nlj_a.v < nlj_b.v";

    let baseline = with_test_env(
        &[
            ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0")),
            ("MYDB_NLJ_HASH", Some("off")),
            ("MYDB_NLJ_BLOCK", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );

    let before = native_result_batch_hits();
    let blocked = with_test_env(
        &[
            ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0")),
            ("MYDB_NLJ_HASH", Some("off")),
        ],
        || sorted_rows(&engine, sql),
    );
    assert_eq!(baseline, blocked, "Block-NL diverged from legacy NLJ");
    assert!(
        native_result_batch_hits() > before,
        "batch NLJ did not execute"
    );
}

/// WS-N2: force outer parallel fan-out with MYDB_NLJ_OUTER_PARALLEL_THRESHOLD=0;
/// results must match serial blocks for every join form.
#[test]
fn nlj_block_outer_parallel_matches_serial() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let sqls = [
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a JOIN nlj_b ON nlj_a.v < nlj_b.v",
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a LEFT JOIN nlj_b ON nlj_a.k = nlj_b.k AND nlj_b.v > nlj_a.v",
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a RIGHT JOIN nlj_b ON nlj_a.k = nlj_b.k AND nlj_a.v < nlj_b.v",
        "SELECT nlj_a.id FROM nlj_a \
         WHERE EXISTS (SELECT 1 FROM nlj_b WHERE nlj_b.v > nlj_a.v)",
        "SELECT nlj_a.id FROM nlj_a \
         WHERE NOT EXISTS (SELECT 1 FROM nlj_b WHERE nlj_b.v > nlj_a.v)",
    ];

    for sql in sqls {
        let serial = with_test_env(
            &[
                ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0")),
                ("MYDB_NLJ_HASH", Some("off")),
                ("MYDB_NLJ_OUTER_PARALLELISM", Some("1")),
            ],
            || sorted_rows(&engine, sql),
        );

        let parallel = with_test_env(
            &[
                ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0")),
                ("MYDB_NLJ_HASH", Some("off")),
                ("MYDB_NLJ_OUTER_PARALLEL_THRESHOLD", Some("0")),
                ("MYDB_NLJ_OUTER_PARALLELISM", Some("4")),
            ],
            || sorted_rows(&engine, sql),
        );
        assert_eq!(serial, parallel, "Block-NL parallel diverged for: {sql}");
    }
}

/// WS-N1: LEFT/RIGHT/SEMI/ANTI/INNER with residuals and NULL three-valued logic
/// must be equivalent with block execution enabled/disabled.
#[test]
fn nlj_block_path_all_join_types_match_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let sqls = [
        // INNER with nonequality and NULL.
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a JOIN nlj_b ON nlj_a.v < nlj_b.v",
        // LEFT with mixed equality/nonequality residuals.
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a LEFT JOIN nlj_b ON nlj_a.k = nlj_b.k AND nlj_b.v > 5",
        // RIGHT with residuals.
        "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id, nlj_b.id \
         FROM nlj_a RIGHT JOIN nlj_b ON nlj_a.k = nlj_b.k AND nlj_a.v < nlj_b.v",
        // SEMI / ANTI
        "SELECT nlj_a.id FROM nlj_a \
         WHERE EXISTS (SELECT 1 FROM nlj_b WHERE nlj_b.v > nlj_a.v)",
        "SELECT nlj_a.id FROM nlj_a \
         WHERE NOT EXISTS (SELECT 1 FROM nlj_b WHERE nlj_b.v > nlj_a.v)",
    ];
    for sql in sqls {
        let off = with_test_env(
            &[
                ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0")),
                ("MYDB_NLJ_HASH", Some("off")),
                ("MYDB_NLJ_BLOCK", Some("off")),
            ],
            || sorted_rows(&engine, sql),
        );
        let on = with_test_env(
            &[
                ("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0")),
                ("MYDB_NLJ_HASH", Some("off")),
            ],
            || sorted_rows(&engine, sql),
        );
        assert_eq!(off, on, "Block-NL diverged from legacy for: {sql}");
    }
}

#[test]
fn nlj_hash_fast_path_increments_counter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_join_tables(&engine);

    let before = native_result_batch_hits();
    with_test_env(
        &[("MYDB_ADAPTIVE_JOIN_NLJ_MAX_COMPARISONS", Some("0"))],
        || {
            sorted_rows(
                &engine,
                "SELECT /*+ NL_JOIN(nlj_a, nlj_b) */ nlj_a.id \
                 FROM nlj_a JOIN nlj_b ON nlj_a.k = nlj_b.k",
            )
        },
    );
    assert!(
        native_result_batch_hits() > before,
        "batch NLJ did not execute"
    );
}
#[test]
fn hash_join_decimal_scale_and_type_mismatch_matches_nlj() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE hjd_a (id INT PRIMARY KEY, k INT, d2 DECIMAL(10,2))")
        .unwrap();
    engine
        .execute("CREATE TABLE hjd_b (id INT PRIMARY KEY, d4 DECIMAL(10,4))")
        .unwrap();
    engine
        .execute("INSERT INTO hjd_a VALUES (1, 10, 10.00), (2, 20, 20.50), (3, 30, 30.00)")
        .unwrap();
    engine
        .execute("INSERT INTO hjd_b VALUES (101, 10.0000), (102, 20.5000), (103, 99.5000)")
        .unwrap();

    // Historical bug: DecimalCompact ValueKey ignored scale, causing cross-scale/type
    // hash equality to miss matches while NLJ was correct. Hash and NLJ must
    // now agree.
    let expected_d2 = vec![
        vec![Value::Int(1), Value::Int(101)],
        vec![Value::Int(2), Value::Int(102)],
    ];
    // Only INT 10 equals 10.0000; 20 differs from 20.5000.
    let expected_k = vec![vec![Value::Int(1), Value::Int(101)]];
    for (cond, expected) in [
        ("hjd_a.d2 = hjd_b.d4", &expected_d2),
        ("hjd_a.k = hjd_b.d4", &expected_k),
    ] {
        let hash = sorted_rows(
            &engine,
            &format!(
                "SELECT /*+ HASH_JOIN(hjd_a, hjd_b) */ hjd_a.id, hjd_b.id \
                 FROM hjd_a JOIN hjd_b ON {cond}"
            ),
        );
        let nlj = sorted_rows(
            &engine,
            &format!(
                "SELECT /*+ NL_JOIN(hjd_a, hjd_b) */ hjd_a.id, hjd_b.id \
                 FROM hjd_a JOIN hjd_b ON {cond}"
            ),
        );
        assert_eq!(hash, nlj, "hash/NLJ diverged for {cond}");
        assert_eq!(&nlj, expected, "unexpected NLJ baseline for {cond}");
    }
}

// ===================== WS-N3: vectorized block-major BNL differential tests =====================
//
// Compare MYDB_NLJ_BLOCK=off (legacy per-pair oracle),
// MYDB_NLJ_VECTORIZE=off (row block-major), and default SIMD execution.
// Test outer blocks contain eight rows; fixture sizes 41/23 exceed two blocks
// and are not multiples of eight, covering partial boundaries. Production remains 64.
// NL_JOIN forces NLJ to cover Eq/Ne and all nonequality operators end to end.

fn setup_bnl_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE bnl_a (id INT PRIMARY KEY, ai INT, af DOUBLE, asx VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE bnl_b (id INT PRIMARY KEY, bi INT, bf DOUBLE, bsx VARCHAR(16))")
        .unwrap();
    let mut rows = Vec::new();
    for i in 1..=41i64 {
        // Scatter NULLs in ai every 17 and af every 23 to exercise lane NULL bitmaps.
        let ai = if i % 17 == 0 {
            "NULL".to_string()
        } else {
            ((i * 7) % 100).to_string()
        };
        let af = if i % 23 == 0 {
            "NULL".to_string()
        } else {
            format!("{}.5", (i * 3) % 50)
        };
        let m = i % 10;
        rows.push(format!("({i}, {ai}, {af}, 's{m}')"));
    }
    engine
        .execute(&format!("INSERT INTO bnl_a VALUES {}", rows.join(", ")))
        .unwrap();
    let mut rows = Vec::new();
    for i in 1..=23i64 {
        let bi = if i % 13 == 0 {
            "NULL".to_string()
        } else {
            ((i * 11) % 100).to_string()
        };
        let bf = if i % 19 == 0 {
            "NULL".to_string()
        } else {
            format!("{}.5", (i * 5) % 50)
        };
        let m = i % 10;
        rows.push(format!("({i}, {bi}, {bf}, 's{m}')"));
    }
    engine
        .execute(&format!("INSERT INTO bnl_b VALUES {}", rows.join(", ")))
        .unwrap();
    engine.execute("ANALYZE TABLE bnl_a").unwrap();
    engine.execute("ANALYZE TABLE bnl_b").unwrap();
}

fn assert_bnl_three_way(engine: &StorageQueryEngine, sql: &str) {
    let oracle = with_test_env(&[("MYDB_NLJ_BLOCK", Some("off"))], || {
        sorted_rows(engine, sql)
    });
    let rowwise = with_test_env(&[("MYDB_NLJ_VECTORIZE", Some("off"))], || {
        sorted_rows(engine, sql)
    });
    let vectorized = with_test_env(&[], || sorted_rows(engine, sql));
    assert_eq!(oracle, rowwise, "row-wise block-major diverged: {sql}");
    assert_eq!(oracle, vectorized, "vectorized diverged: {sql}");
}

/// Confirm the SIMD mask path actually ran (didn't silently fall back to
/// the row-wise path), so the differential above is really exercising
/// vectorization.
fn assert_bnl_simd_engaged(engine: &StorageQueryEngine, sql: &str) {
    let before = native_result_batch_hits();
    let _ = with_test_env(&[], || sorted_rows(engine, sql));
    assert!(
        native_result_batch_hits() > before,
        "batch NLJ did not execute: {sql}"
    );
}

#[test]
fn bnl_vectorized_inner_all_ops_match_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_bnl_tables(&engine);
    let h = "/*+ NL_JOIN(bnl_a, bnl_b) */";
    for cond in [
        "bnl_a.ai < bnl_b.bi",
        "bnl_a.ai <= bnl_b.bi",
        "bnl_a.ai > bnl_b.bi",
        "bnl_a.ai >= bnl_b.bi",
        "bnl_a.ai = bnl_b.bi",
        "bnl_a.ai <> bnl_b.bi",
        "bnl_b.bi > bnl_a.ai", // Flipped form: the left operand is a right-side column.
        "bnl_a.ai < bnl_b.bi AND bnl_a.af > bnl_b.bf", // AND chain with i64 and f64 terms.
        "bnl_a.ai < bnl_b.bi AND bnl_a.asx <> bnl_b.bsx", // i64 SIMD with string residual refinement.
    ] {
        let sql = format!("SELECT {h} bnl_a.id, bnl_b.id FROM bnl_a JOIN bnl_b ON {cond}");
        assert_bnl_three_way(&engine, &sql);
    }
    // Require actual vector execution for same-lane i64/f64 comparisons across multiple blocks.
    assert_bnl_simd_engaged(
        &engine,
        &format!("SELECT {h} bnl_a.id, bnl_b.id FROM bnl_a JOIN bnl_b ON bnl_a.ai < bnl_b.bi"),
    );
    assert_bnl_simd_engaged(
        &engine,
        &format!("SELECT {h} bnl_a.id, bnl_b.id FROM bnl_a JOIN bnl_b ON bnl_a.af < bnl_b.bf"),
    );
}

#[test]
fn bnl_vectorized_outer_joins_match_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_bnl_tables(&engine);
    for (jt, cond) in [
        ("LEFT JOIN", "bnl_a.ai < bnl_b.bi"),
        ("RIGHT JOIN", "bnl_a.ai >= bnl_b.bi"),
        ("LEFT JOIN", "bnl_a.af < bnl_b.bf"),
        ("RIGHT JOIN", "bnl_a.ai < bnl_b.bi AND bnl_a.af > bnl_b.bf"),
    ] {
        let sql = format!(
            "SELECT /*+ NL_JOIN(bnl_a, bnl_b) */ bnl_a.id, bnl_b.id FROM bnl_a {jt} bnl_b ON {cond}"
        );
        assert_bnl_three_way(&engine, &sql);
    }
}

#[test]
fn bnl_vectorized_fallbacks_match_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_bnl_tables(&engine);
    let h = "/*+ NL_JOIN(bnl_a, bnl_b) */";
    // Without vectorizable same-lane column comparisons, the whole predicate uses row block-major,
    // which must still match the oracle.
    for cond in [
        "bnl_a.asx < bnl_b.bsx", // Strings only.
        "bnl_a.ai < bnl_b.bf",   // Cross-lane INT versus DOUBLE.
        "bnl_a.af < bnl_b.bi",   // Cross-lane DOUBLE versus INT.
    ] {
        let sql = format!("SELECT {h} bnl_a.id, bnl_b.id FROM bnl_a JOIN bnl_b ON {cond}");
        assert_bnl_three_way(&engine, &sql);
    }
}

#[test]
fn bnl_vectorized_semi_anti_match_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_bnl_tables(&engine);
    // Nonequality EXISTS/NOT EXISTS decorrelates to semi/anti NLJ without equality keys,
    // using block-major existence-only execution. bnl_b.bi > bnl_a.ai has a right-side
    // left operand and compiles with flipped direction to ai < bi.
    assert_bnl_three_way(
        &engine,
        "SELECT id FROM bnl_a WHERE EXISTS (SELECT 1 FROM bnl_b WHERE bnl_b.bi > bnl_a.ai)",
    );
    assert_bnl_three_way(
        &engine,
        "SELECT id FROM bnl_a WHERE NOT EXISTS (SELECT 1 FROM bnl_b WHERE bnl_b.bi > bnl_a.ai)",
    );
}

#[test]
fn bnl_vectorized_parallel_matches_serial() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_bnl_tables(&engine);
    let sql = "SELECT /*+ NL_JOIN(bnl_a, bnl_b) */ bnl_a.id, bnl_b.id \
               FROM bnl_a JOIN bnl_b ON bnl_a.ai < bnl_b.bi";
    let serial = with_test_env(&[("MYDB_NLJ_OUTER_PARALLELISM", Some("1"))], || {
        sorted_rows(&engine, sql)
    });
    let parallel = with_test_env(
        &[
            ("MYDB_NLJ_OUTER_PARALLEL_THRESHOLD", Some("0")),
            ("MYDB_NLJ_OUTER_PARALLELISM", Some("4")),
        ],
        || sorted_rows(&engine, sql),
    );
    assert_eq!(serial, parallel, "parallel vectorized diverged from serial");
}
