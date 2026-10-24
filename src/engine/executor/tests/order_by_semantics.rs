//! ORDER BY semantic regressions, P0-3 and P1-3:
//! Outer set-operation ORDER BY expressions bind against the first SELECT's output columns.
//! Table qualifiers return 1250, aggregates 3028, and unknown columns 1054; previously
//! keys other than names/ordinals were silently skipped, producing incorrect order.
//! Explicit COLLATE participates in sorting.
//! Mixed-direction multikey sorting with interleaved NULL values forms a total order.
//! Forced spill matches in-memory order; compare key encoding enabled/disabled.
//! Parallel and serial sorting have identical order.

use super::*;
use crate::engine::executor::sort::key_encode::with_sort_key_encode;

fn rows_of(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        QueryResponse::StreamingResultSet(mut r) => {
            let mut out = Vec::new();
            while let Ok(Some(row)) = r.cursor.next_row() {
                out.push(row);
            }
            out
        }
        other => panic!("expected result set, got {other:?}"),
    }
}

fn columns_and_rows(response: QueryResponse) -> (Vec<String>, Vec<Vec<Value>>) {
    match response {
        QueryResponse::ResultSet(result) => (
            result
                .columns
                .into_iter()
                .map(|column| column.name)
                .collect(),
            result.rows,
        ),
        QueryResponse::SharedResultSet(result) => (
            result
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            result.rows,
        ),
        QueryResponse::StreamingResultSet(mut result) => {
            let columns = result
                .columns
                .into_iter()
                .map(|column| column.name)
                .collect();
            let mut rows = Vec::new();
            while let Some(row) = result.cursor.next_row().unwrap() {
                rows.push(row);
            }
            (columns, rows)
        }
        other => panic!("expected result set, got {other:?}"),
    }
}

// ---- Set-operation ORDER BY expressions ----

fn setup_union_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT, s VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, v INT, s VARCHAR(16))")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 5, 'x'), (2, 3, 'y')")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (10, 4, 'z'), (11, NULL, 'w')")
        .unwrap();
}

#[test]
fn union_order_by_expression_over_output_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_union_tables(&engine);
    // v * -1 is like v DESC, except default ASC still puts NULL first.
    let rows = rows_of(
        engine
            .execute("SELECT id, v FROM a UNION ALL SELECT id, v FROM b ORDER BY v * -1, id")
            .unwrap(),
    );
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int(v) => *v,
            other => panic!("expected int, got {other:?}"),
        })
        .collect();
    // v*-1 ASC: NULL/id11, then -5/id1, -4/id10, -3/id2.
    assert_eq!(ids, vec![11, 1, 10, 2]);
    // Function expression key.
    let rows = rows_of(
        engine
            .execute("SELECT s FROM a UNION ALL SELECT s FROM b ORDER BY UPPER(s) DESC")
            .unwrap(),
    );
    let vals: Vec<Value> = rows.into_iter().map(|mut r| r.remove(0)).collect();
    assert_eq!(
        vals,
        vec![
            Value::String("z".into()),
            Value::String("y".into()),
            Value::String("x".into()),
            Value::String("w".into()),
        ]
    );
}

#[test]
fn nested_union_all_aggregate_order_uses_projected_schema() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE union_sort_data (\
             id INT PRIMARY KEY, sourceid INT, targetid INT, amount INT)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO union_sort_data VALUES \
             (1, 10, 20, 5), (2, 10, 20, 7), (3, 30, 40, 3)",
        )
        .unwrap();

    let three_branches = "\
        SELECT sourceid AS source_key, targetid AS target_key, \
               SUM(amount) AS total_amount \
        FROM union_sort_data WHERE id <= 2 GROUP BY sourceid, targetid \
        UNION ALL \
        SELECT sourceid, targetid, SUM(amount) \
        FROM union_sort_data WHERE id = 3 GROUP BY sourceid, targetid \
        UNION ALL \
        SELECT sourceid, targetid, SUM(amount) \
        FROM union_sort_data WHERE id = 1 GROUP BY sourceid, targetid \
        ORDER BY total_amount DESC, source_key";
    let four_branches = "\
        SELECT sourceid AS source_key, targetid AS target_key, \
               SUM(amount) AS total_amount \
        FROM union_sort_data WHERE id <= 2 GROUP BY sourceid, targetid \
        UNION ALL \
        SELECT sourceid, targetid, SUM(amount) \
        FROM union_sort_data WHERE id = 3 GROUP BY sourceid, targetid \
        UNION ALL \
        SELECT sourceid, targetid, SUM(amount) \
        FROM union_sort_data WHERE id = 1 GROUP BY sourceid, targetid \
        UNION ALL \
        SELECT sourceid, targetid, SUM(amount) \
        FROM union_sort_data WHERE id = 2 GROUP BY sourceid, targetid \
        ORDER BY total_amount DESC, source_key";

    let expected_columns = vec![
        "source_key".to_string(),
        "target_key".to_string(),
        "total_amount".to_string(),
    ];
    let expected_three = vec![
        vec![Value::Int(10), Value::Int(20), Value::DecimalCompact(12, 0)],
        vec![Value::Int(10), Value::Int(20), Value::DecimalCompact(5, 0)],
        vec![Value::Int(30), Value::Int(40), Value::DecimalCompact(3, 0)],
    ];
    let expected_four = vec![
        vec![Value::Int(10), Value::Int(20), Value::DecimalCompact(12, 0)],
        vec![Value::Int(10), Value::Int(20), Value::DecimalCompact(7, 0)],
        vec![Value::Int(10), Value::Int(20), Value::DecimalCompact(5, 0)],
        vec![Value::Int(30), Value::Int(40), Value::DecimalCompact(3, 0)],
    ];

    for mode in [VecMode::Off, VecMode::On, VecMode::Auto] {
        let three = with_vec_mode(mode, || {
            columns_and_rows(engine.execute(three_branches).unwrap())
        });
        assert_eq!(
            three.0, expected_columns,
            "column names diverged in {mode:?}"
        );
        assert_eq!(
            three.1, expected_three,
            "three branches diverged in {mode:?}"
        );

        let four = with_vec_mode(mode, || {
            columns_and_rows(engine.execute(four_branches).unwrap())
        });
        assert_eq!(
            four.0, expected_columns,
            "column names diverged in {mode:?}"
        );
        assert_eq!(four.1, expected_four, "four branches diverged in {mode:?}");
    }
}

#[test]
fn union_order_by_qualified_name_rejected_1250() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_union_tables(&engine);
    let err = engine
        .execute("SELECT id FROM a UNION SELECT id FROM b ORDER BY a.id")
        .unwrap_err();
    assert_eq!(err.code(), 1250, "expected 1250, got {err:?}");
}

#[test]
fn union_order_by_aggregate_rejected_3028() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_union_tables(&engine);
    let err = engine
        .execute("SELECT id FROM a UNION SELECT id FROM b ORDER BY SUM(id)")
        .unwrap_err();
    assert_eq!(err.code(), 3028, "expected 3028, got {err:?}");
}

#[test]
fn union_order_by_unknown_column_rejected_1054() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_union_tables(&engine);
    let err = engine
        .execute("SELECT id FROM a UNION SELECT id FROM b ORDER BY missing")
        .unwrap_err();
    assert_eq!(err.code(), 1054, "expected 1054, got {err:?}");
    // Unknown columns inside expressions may yield NULL keys or errors; MySQL reports
    // 1054 on failed evaluation-time binding, while mydb's permissive key evaluation uses NULL.
    // The requirement here is that legal expressions actually affect sorting, as tested above.
}

// ---- Explicit COLLATE ----

#[test]
fn order_by_explicit_collate_changes_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(16))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'B'), (3, 'b'), (4, 'A')")
        .unwrap();
    // Default CI folds a/A and b/B into ties, preserving insertion order stably.
    let rows = rows_of(engine.execute("SELECT id FROM t ORDER BY s, id").unwrap());
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int(v) => *v,
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(ids, vec![1, 4, 2, 3]);
    // Binary collation places uppercase code points first: A(4), B(2), a(1), b(3).
    let rows = rows_of(
        engine
            .execute("SELECT id FROM t ORDER BY s COLLATE utf8mb4_bin, id")
            .unwrap(),
    );
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int(v) => *v,
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(ids, vec![4, 2, 1, 3]);
}

// ---- Differential key encoding, forced external sort, and parallelism ----

fn setup_diff_table(engine: &StorageQueryEngine, rows: usize) {
    engine
        .execute(
            "CREATE TABLE d (id INT PRIMARY KEY, v INT, s VARCHAR(32), dt DATETIME, \
             dc DECIMAL(10,2))",
        )
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.d",
        (0..rows).map(|i| {
            vec![
                Value::Int(i as i64),
                if i % 9 == 0 {
                    Value::Null
                } else {
                    Value::Int(((i * 37) % 100) as i64)
                },
                if i % 11 == 0 {
                    Value::Null
                } else {
                    Value::String(format!("name-{:03}", (i * 17) % 50))
                },
                Value::String(format!("2024-01-{:02} 10:{:02}:00", i % 28 + 1, i % 60)),
                Value::DecimalCompact((((i * 13) % 500) * 100 + i % 100) as i64, 2),
            ]
        }),
    );
}

#[test]
fn key_encode_on_off_full_order_identical() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_diff_table(&engine, 500);
    for sql in [
        "SELECT id FROM d ORDER BY s, v DESC, id",
        "SELECT id FROM d ORDER BY dt DESC, dc, id",
        "SELECT id FROM d ORDER BY v NULLS LAST, s DESC NULLS FIRST, id",
        "SELECT id FROM d ORDER BY dc DESC LIMIT 37",
    ] {
        let on = with_sort_key_encode(true, || rows_of(engine.execute(sql).unwrap()));
        let off = with_sort_key_encode(false, || rows_of(engine.execute(sql).unwrap()));
        assert_eq!(on, off, "encode on/off diverged for {sql}");
    }
}

#[test]
fn forced_spill_matches_in_memory_order() {
    // MYDB_SORT_RUN_BYTES has a 1MB minimum, so 500 rows do not actually spill;
    // this checks configuration stability. Large spill differential tests are ignored benchmarks.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_diff_table(&engine, 500);
    let sql = "SELECT id, s FROM d ORDER BY s DESC, id";
    let baseline = rows_of(engine.execute(sql).unwrap());
    let spill = with_test_env(&[("MYDB_SORT_RUN_BYTES", Some("1048576"))], || {
        rows_of(engine.execute(sql).unwrap())
    });
    assert_eq!(baseline, spill);
}

#[test]
fn parallel_and_serial_sort_agree() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_diff_table(&engine, 600);
    let sql = "SELECT id FROM d ORDER BY s, dt DESC, id";
    let serial = rows_of(engine.execute(sql).unwrap());
    let parallel = with_test_env(
        &[
            ("MYDB_PARALLEL_DEGREE", Some("4")),
            ("MYDB_PARALLEL_THRESHOLD_ROWS", Some("1")),
        ],
        || rows_of(engine.execute(sql).unwrap()),
    );
    assert_eq!(serial, parallel);
}

// ---- Mixed directions and interleaved NULL total order ----

#[test]
fn mixed_direction_multi_key_with_nulls_total_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE m (id INT PRIMARY KEY, a INT, b VARCHAR(8))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO m VALUES \
             (1, 1, 'x'), (2, NULL, 'x'), (3, 1, NULL), (4, NULL, NULL), \
             (5, 2, 'y'), (6, 2, 'x'), (7, 1, 'y'), (8, NULL, 'y')",
        )
        .unwrap();
    // a ASC puts NULL first; b DESC puts NULL last.
    let rows = rows_of(
        engine
            .execute("SELECT id FROM m ORDER BY a, b DESC, id")
            .unwrap(),
    );
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Int(v) => *v,
            _ => unreachable!(),
        })
        .collect();
    // Within a=NULL, b DESC gives y(8),x(2),NULL(4); within a=1, y(7),x(1),NULL(3).
    // a=2: y(5), x(6)。
    assert_eq!(ids, vec![8, 2, 4, 7, 1, 3, 5, 6]);
}

// ---- Vector TopN expression keys, expanded under P1-2 ----

#[test]
fn vec_topn_expression_keys_match_row_path() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_diff_table(&engine, 300);
    for sql in [
        // Arithmetic expression keys.
        "SELECT id FROM d ORDER BY v * -1, id LIMIT 20",
        // Function expression keys supported by the batch scalar compiler.
        "SELECT id FROM d ORDER BY YEAR(dt) DESC, id LIMIT 15",
        // Expression keys combined with NULLS clauses.
        "SELECT id FROM d ORDER BY v + 1 NULLS LAST, id LIMIT 25",
        // Mix expressions and column references.
        "SELECT id FROM d ORDER BY s, v * 2 DESC, id LIMIT 30",
    ] {
        let row_path = with_vec_mode(VecMode::Off, || rows_of(engine.execute(sql).unwrap()));
        let vec_path = with_vec_mode(VecMode::On, || rows_of(engine.execute(sql).unwrap()));
        assert_eq!(row_path, vec_path, "vec/row diverged for {sql}");
    }
}

#[test]
fn distinct_order_by_limit_uses_bounded_topk_and_matches_full_sort() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE distinct_topk (seq INT PRIMARY KEY, account INT, amount INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO distinct_topk VALUES \
             (1, 1, 10), (2, 2, 90), (3, 3, 70), (4, 2, 90), \
             (5, 4, 80), (6, 1, 10), (7, 5, 60), (8, 6, 100), \
             (9, 7, 50), (10, 3, 70), (11, 8, 40), (12, 9, 30)",
        )
        .unwrap();

    let full = rows_of(
        engine
            .execute(
                "SELECT DISTINCT account, amount FROM distinct_topk \
                 ORDER BY amount DESC, account",
            )
            .unwrap(),
    );
    let expected = full.into_iter().skip(2).take(4).collect::<Vec<_>>();
    for mode in [VecMode::Off, VecMode::On, VecMode::Auto] {
        let bounded = with_vec_mode(mode, || {
            rows_of(
                engine
                    .execute(
                        "SELECT DISTINCT account, amount FROM distinct_topk \
                         ORDER BY amount DESC, account LIMIT 4 OFFSET 2",
                    )
                    .unwrap(),
            )
        });
        assert_eq!(
            bounded, expected,
            "row/vector fallback mismatch in {mode:?}"
        );
    }

    let plan = explain_analyze_tree(
        &engine,
        "SELECT DISTINCT account, amount FROM distinct_topk \
         ORDER BY amount DESC, account LIMIT 4 OFFSET 2",
    );
    assert!(plan.contains("Distinct TopK"), "unexpected plan: {plan}");
    assert!(
        !plan.contains("Sort ("),
        "full sort must be eliminated: {plan}"
    );
}

#[test]
fn distinct_topk_preserves_collation_dedup_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE distinct_topk_text (id INT PRIMARY KEY, s VARCHAR(32))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO distinct_topk_text VALUES \
             (1, 'Zulu'), (2, 'alpha'), (3, 'ALPHA'), (4, 'echo'), \
             (5, 'Echo'), (6, 'bravo'), (7, 'charlie')",
        )
        .unwrap();
    let full = rows_of(
        engine
            .execute("SELECT DISTINCT s FROM distinct_topk_text ORDER BY s")
            .unwrap(),
    );
    let bounded = rows_of(
        engine
            .execute("SELECT DISTINCT s FROM distinct_topk_text ORDER BY s LIMIT 3")
            .unwrap(),
    );
    assert_eq!(bounded, full.into_iter().take(3).collect::<Vec<_>>());
}

#[test]
fn ordered_index_distinct_topk_stops_on_distinct_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ordered_distinct (id INT PRIMARY KEY, account INT, amount INT)")
        .unwrap();
    engine
        .execute("CREATE INDEX ordered_distinct_amount ON ordered_distinct(amount)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO ordered_distinct VALUES \
             (1, 1, 100), (2, 1, 100), (3, 2, 90), (4, 3, 80), \
             (5, 4, 70), (6, 5, 60), (7, 6, 50)",
        )
        .unwrap();

    let rows = rows_of(
        engine
            .execute(
                "SELECT DISTINCT account, amount FROM ordered_distinct \
                 ORDER BY amount DESC LIMIT 3",
            )
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(100)],
            vec![Value::Int(2), Value::Int(90)],
            vec![Value::Int(3), Value::Int(80)],
        ]
    );

    let plan = explain_analyze_tree(
        &engine,
        "SELECT DISTINCT account, amount FROM ordered_distinct \
         ORDER BY amount DESC LIMIT 3",
    );
    assert!(plan.contains("Distinct TopK"), "unexpected plan: {plan}");
    assert!(
        plan.contains("Index range scan on ordered_distinct using ordered_distinct_amount"),
        "ordered index was not selected: {plan}"
    );
    assert!(
        !plan.contains("Parallel table scan"),
        "unexpected plan: {plan}"
    );
}
