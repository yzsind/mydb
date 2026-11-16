//! Regressions from the 2026-10-25 release audit.

use super::support::query_rows;
use super::*;

fn exec_all(engine: &StorageQueryEngine, statements: &[&str]) {
    for sql in statements {
        engine
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}

fn ints(rows: &[&[i64]]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| row.iter().map(|v| Value::Int(*v)).collect())
        .collect()
}

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}

/// A nested-loop join whose condition holds a correlated subquery feeds
/// vectorized TopK/aggregate operators through a row-to-batch adapter. Both
/// sides of a self-join carry the same base table name, so the adapter must
/// bind each batch column by relation alias, never by table name.
#[test]
fn self_join_row_batch_adapter_binds_columns_by_alias() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE sj (id INT PRIMARY KEY, b INT, s VARCHAR(10))",
            "INSERT INTO sj VALUES (0,0,'s0'),(1,1,'s1'),(2,2,'s2'),(3,3,'s3'),(4,4,'s4'),(5,5,'s5')",
            "CREATE TABLE sk (a INT)",
            "INSERT INTO sk VALUES (0),(1),(2)",
        ],
    );
    assert_eq!(
        query_rows(
            &engine,
            "SELECT q0.id, q1.id FROM sj q0 CROSS JOIN sj q1 \
             WHERE q0.id = (SELECT q1.id - 1) AND q0.id < 3 ORDER BY q1.b DESC LIMIT 3",
        ),
        ints(&[&[2, 3], &[1, 2], &[0, 1]])
    );
    assert_eq!(
        sorted(query_rows(
            &engine,
            "SELECT q1.b, MIN(q0.b), COUNT(*) FROM sj q0 CROSS JOIN sj q1 \
             WHERE q0.b = (SELECT MIN(z.a) FROM sk z WHERE z.a < q1.b) GROUP BY q1.b",
        )),
        ints(&[&[1, 0, 1], &[2, 0, 1], &[3, 0, 1], &[4, 0, 1], &[5, 0, 1]])
    );
    // LEFT JOIN with a subquery residual: null-extended rows must keep the
    // right side's NULLs instead of reading the left alias' columns.
    assert_eq!(
        query_rows(
            &engine,
            "SELECT COUNT(*), GROUP_CONCAT(a.s ORDER BY a.s), GROUP_CONCAT(b.s) \
             FROM sj a LEFT JOIN sj b ON b.id = a.id AND a.id IN (SELECT 1) \
             WHERE b.s NOT LIKE 'x%'",
        ),
        vec![vec![
            Value::Int(1),
            Value::String("s1".into()),
            Value::String("s1".into())
        ]]
    );
}

/// An outer filter must not move into a CTE body whose LIMIT/OFFSET, window
/// function or ORDER BY ... LIMIT over groups selects rows before the filter.
#[test]
fn cte_predicate_pushdown_stops_at_limit_and_window_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE lp (id INT PRIMARY KEY, v INT)",
            "INSERT INTO lp VALUES (0,0),(1,1),(2,0),(3,1),(4,0),(5,1),(6,0),(7,1),(8,0),(9,1)",
        ],
    );
    let cases: &[(&str, Vec<Vec<Value>>)] = &[
        (
            "WITH w AS (SELECT id, v FROM lp ORDER BY id LIMIT 4) SELECT id FROM w WHERE v = 1 ORDER BY id",
            ints(&[&[1], &[3]]),
        ),
        (
            "WITH w AS (SELECT id, v FROM lp ORDER BY id LIMIT 2 OFFSET 1) SELECT id FROM w WHERE v = 0",
            ints(&[&[2]]),
        ),
        (
            "WITH w AS (SELECT id, v FROM lp WHERE v = 1 ORDER BY id LIMIT 3) SELECT id FROM w WHERE id = 9",
            vec![],
        ),
        (
            "WITH w AS (SELECT id, v FROM lp ORDER BY id LIMIT 4) \
             SELECT COUNT(*) FROM w a, w b WHERE a.v = 1 AND b.v = 1",
            ints(&[&[4]]),
        ),
        (
            "WITH w AS (SELECT v, COUNT(*) c FROM lp GROUP BY v ORDER BY v LIMIT 1) SELECT v FROM w WHERE v = 1",
            vec![],
        ),
        (
            "WITH w AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) rn FROM lp) SELECT id, rn FROM w WHERE id = 5",
            ints(&[&[5, 6]]),
        ),
        (
            "WITH w AS (SELECT id, SUM(v) OVER (ORDER BY id) rs FROM lp) SELECT id, CAST(rs AS SIGNED) FROM w WHERE id = 5",
            ints(&[&[5, 3]]),
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(&query_rows(&engine, sql), expected, "{sql}");
    }
}

/// A materialized derived table keeps its column collation when the outer
/// filter is evaluated directly against the spooled rows.
#[test]
fn materialized_derived_filter_keeps_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE c1 (s VARCHAR(10) COLLATE utf8mb4_bin)",
            "INSERT INTO c1 VALUES ('a'),('A'),('b')",
        ],
    );
    for sql in [
        "SELECT s FROM (SELECT * FROM c1) d WHERE s = 'a'",
        "SELECT s FROM (SELECT s FROM c1 LIMIT 10) d WHERE s = 'a'",
        "SELECT s FROM (SELECT DISTINCT s FROM c1) d WHERE s = 'a'",
        "SELECT s FROM (SELECT s FROM c1 UNION ALL SELECT 'x') d WHERE s = 'a'",
        "SELECT s FROM (SELECT * FROM c1) d WHERE s IN ('a', 'c')",
        "SELECT s FROM (SELECT * FROM c1) d WHERE s BETWEEN 'a' AND 'a'",
        "WITH w AS (SELECT s FROM c1 LIMIT 10) SELECT s FROM w WHERE s = 'a'",
    ] {
        assert_eq!(
            query_rows(&engine, sql),
            vec![vec![Value::String("a".into())]],
            "{sql}"
        );
    }
}

/// CASE inherits the collation of its result branches, so a CASE over a
/// `utf8mb4_bin` column compares binary against a coercible literal.
#[test]
fn case_result_inherits_branch_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE cc (s VARCHAR(10) COLLATE utf8mb4_bin)",
            "INSERT INTO cc VALUES ('a'),('A'),('b')",
        ],
    );
    assert_eq!(
        query_rows(
            &engine,
            "SELECT s FROM cc WHERE CASE WHEN s IS NOT NULL THEN s ELSE 'w' END = 'A'"
        ),
        vec![vec![Value::String("A".into())]]
    );
    assert_eq!(
        query_rows(
            &engine,
            "SELECT COLLATION(CASE WHEN s = 'b' THEN 'w' ELSE s END) FROM cc LIMIT 1"
        ),
        vec![vec![Value::String("utf8mb4_bin".into())]]
    );
}

/// A hash LEFT JOIN that builds on the preserved side must NULL-extend every
/// build row even when no probe can match: all build keys NULL, or an empty
/// probe input. DISTINCT over a right-side expression selects that plan.
#[test]
fn build_preserved_hash_join_emits_rows_without_matches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE e1 (id INT PRIMARY KEY, a INT, KEY(a))",
            "CREATE TABLE e2 (id INT PRIMARY KEY, k INT, s VARCHAR(10))",
            "INSERT INTO e1 VALUES (1, NULL), (2, NULL)",
            "INSERT INTO e2 WITH RECURSIVE r(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM r WHERE n < 199) \
             SELECT n, n, 'v' FROM r",
            "ANALYZE TABLE e1, e2",
        ],
    );
    let q = vec![vec![Value::String("q".into())]];
    let sql = "SELECT DISTINCT COALESCE(e2.s, 'q') FROM e1 LEFT JOIN e2 ON e2.k = e1.a";
    let mirrored = "SELECT DISTINCT COALESCE(e2.s, 'q') FROM e2 RIGHT JOIN e1 ON e2.k = e1.a";
    for env in [None, Some(("MYDB_HASH_JOIN_STREAMING_INNER_DISABLE", "1"))] {
        let run = || {
            assert_eq!(query_rows(&engine, sql), q, "{sql} {env:?}");
            assert_eq!(query_rows(&engine, mirrored), q, "{mirrored} {env:?}");
        };
        match env {
            Some((key, value)) => crate::util::env::with_vars(&[(key, Some(value))], run),
            None => run(),
        }
    }
    // An empty probe input (statistics still describe 200 rows).
    engine.execute("DELETE FROM e2").unwrap();
    engine.execute("UPDATE e1 SET a = id").unwrap();
    assert_eq!(query_rows(&engine, sql), q, "{sql} (empty probe)");
}

/// An index nested-loop join only binds index parts to outer-row values. An
/// inner-table comparison on the next index part (`m0.a = m0.k` over
/// `KEY(b, a)`) stays a residual instead of an unresolvable lookup key.
#[test]
fn inlj_inner_column_comparison_is_residual_not_lookup_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE m0 (id INT PRIMARY KEY, a INT, b INT, k INT, KEY(b, a))",
            "CREATE TABLE m1 (id INT PRIMARY KEY, a INT, s VARCHAR(10))",
            "INSERT INTO m0 WITH RECURSIVE r(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM r WHERE n < 199) \
             SELECT n, n % 10, n % 10 + 100, n % 3 FROM r",
            "INSERT INTO m1 VALUES (59, 101, 'abc')",
            "ANALYZE TABLE m0, m1",
        ],
    );
    // m0.b = 101 <=> n % 10 = 1, so m0.a = 1; m0.k = n % 3 must also be 1.
    let expected: Vec<Vec<Value>> = (0..200)
        .filter(|n| n % 10 == 1 && n % 3 == 1)
        .map(|n| vec![Value::Int(n)])
        .collect();
    for sql in [
        "SELECT m0.id FROM m0 JOIN m1 ON m1.a = m0.b WHERE m0.a = m0.k ORDER BY m0.id",
        "SELECT m0.id FROM m0 JOIN m1 ON m1.a = m0.b AND m1.s LIKE '_b%' WHERE m0.k = m0.a ORDER BY m0.id",
        "SELECT m0.id FROM m0 JOIN m1 ON m1.a = m0.b WHERE m0.a <= m0.k AND m0.a >= m0.k ORDER BY m0.id",
    ] {
        assert_eq!(query_rows(&engine, sql), expected, "{sql}");
    }
}

/// Window functions over subqueries: a subquery inside a window argument or
/// key keeps its own scope, a decorrelated WHERE subquery's derived join is
/// materialized for the window input, and `GROUP BY 1` over a derived table
/// survives re-binding of its constant select item.
#[test]
fn window_and_group_by_paths_keep_subquery_and_ordinal_scopes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE sj (id INT PRIMARY KEY, b INT)",
            "INSERT INTO sj VALUES (0,0),(1,1),(2,2)",
            "CREATE TABLE sk (a INT)",
            "INSERT INTO sk VALUES (0),(1)",
        ],
    );
    let cases: &[(&str, Vec<Vec<Value>>)] = &[
        (
            "SELECT id, RANK() OVER (ORDER BY (SELECT MAX(a) FROM sk)) FROM sj ORDER BY id",
            ints(&[&[0, 1], &[1, 1], &[2, 1]]),
        ),
        (
            "SELECT id, RANK() OVER (ORDER BY (SELECT MAX(a) FROM sk WHERE sk.a <= sj.b)) FROM sj ORDER BY id",
            ints(&[&[0, 1], &[1, 2], &[2, 2]]),
        ),
        (
            "SELECT id, CAST(SUM(CASE WHEN EXISTS (SELECT 1 FROM sk WHERE sk.a = sj.b) THEN 1 ELSE 0 END) \
             OVER (ORDER BY id) AS SIGNED) FROM sj ORDER BY id",
            ints(&[&[0, 1], &[1, 2], &[2, 2]]),
        ),
        (
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM sj \
             WHERE sj.b > (SELECT COUNT(*) FROM sk WHERE sk.a = sj.b)",
            ints(&[&[2, 1]]),
        ),
        ("SELECT CAST(1 AS SIGNED), COUNT(*) FROM (SELECT * FROM sj) d GROUP BY 1", ints(&[&[1, 3]])),
        (
            "SELECT 'x' AS c, COUNT(*) FROM (SELECT * FROM sj LIMIT 5) d GROUP BY c",
            vec![vec![Value::String("x".into()), Value::Int(3)]],
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(&query_rows(&engine, sql), expected, "{sql}");
    }
}

/// Without ANSI_QUOTES a double-quoted string is a literal in every DDL
/// position MySQL accepts a string: SET/ENUM members and COMMENT values.
#[test]
fn ddl_accepts_double_quoted_set_members_and_comments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            r#"CREATE TABLE dq (e ENUM("a","b"), s SET("x","y's","z""q") COMMENT "col ""c"" it's", KEY k (e) COMMENT "idx") COMMENT = "tbl""#,
            r#"ALTER TABLE dq ADD COLUMN t INT COMMENT "added""#,
            "INSERT INTO dq (e, s, t) VALUES ('b', 'x,y''s', 1)",
        ],
    );
    assert_eq!(
        query_rows(&engine, "SELECT e, s FROM dq"),
        vec![vec![
            Value::String("b".into()),
            Value::String("x,y's".into())
        ]]
    );
    let ddl = query_rows(&engine, "SHOW CREATE TABLE dq");
    let Value::String(ddl) = &ddl[0][1] else {
        panic!("{ddl:?}")
    };
    for part in [
        "set('x','y''s','z\"q')",
        "COMMENT 'col \"c\" it''s'",
        "COMMENT 'idx'",
        "COMMENT 'added'",
        "COMMENT='tbl'",
    ] {
        assert!(ddl.contains(part), "{part} missing in {ddl}");
    }
}

/// Join output pruning keeps every column read by a row-constructor IN
/// residual on an index nested-loop join.
#[test]
fn join_output_pruning_keeps_row_constructor_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE m0 (id INT PRIMARY KEY, a INT, b INT, s VARCHAR(10), KEY(a), KEY(b, a))",
            "CREATE TABLE m1 (id INT PRIMARY KEY, a INT, b INT, s VARCHAR(10), KEY(a), KEY(b, a))",
            "INSERT INTO m0 WITH RECURSIVE r(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM r WHERE n < 199) \
             SELECT n, n % 10, n % 7, 'x' FROM r",
            "INSERT INTO m1 VALUES (1, 3, 3, 'a'), (2, 3, 5, 'b')",
            "ANALYZE TABLE m0, m1",
        ],
    );
    // q1.b = 3 or 5 joins m0.a; the row IN keeps (a, b) in {(3,3), (3,5)}.
    let expected: Vec<Vec<Value>> = (0..200)
        .filter(|n| n % 10 == 3 && (n % 7 == 3 || n % 7 == 5))
        .map(|n| vec![Value::Int(n)])
        .collect();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT m0.id FROM m0 JOIN m1 q1 ON q1.b = m0.a \
             WHERE (m0.a, m0.b) IN (SELECT z.a, z.b FROM m1 z) ORDER BY 1",
        ),
        expected
    );
    let not_in: Vec<Vec<Value>> = (0..200)
        .filter(|n| n % 10 == 3 || n % 10 == 5)
        .filter(|n| !(n % 10 == 3 && (n % 7 == 3 || n % 7 == 5)))
        .map(|n| vec![Value::Int(n)])
        .collect();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT m0.id FROM m0 JOIN m1 q1 ON q1.b = m0.a \
             WHERE (m0.a, m0.b) NOT IN (SELECT z.a, z.b FROM m1 z) ORDER BY 1",
        ),
        not_in
    );
}

/// Join residuals compiled over both sides keep each column's collation: two
/// `utf8mb4_bin` columns compare bytewise in index, hash and block NLJ paths.
#[test]
fn join_residual_string_comparison_keeps_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE y0 (id INT PRIMARY KEY, b INT, s VARCHAR(10) COLLATE utf8mb4_bin, KEY(b))",
            "CREATE TABLE y1 (id INT PRIMARY KEY, k INT, s VARCHAR(10) COLLATE utf8mb4_bin)",
            "INSERT INTO y0 WITH RECURSIVE r(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM r WHERE n < 199) \
             SELECT n, n % 50, CASE WHEN n % 2 = 0 THEN 'a' ELSE 'A' END FROM r",
            "INSERT INTO y1 VALUES (1, 3, 'a'), (2, 4, 'a')",
            "ANALYZE TABLE y0, y1",
        ],
    );
    // b = 3 rows hold 'A', b = 4 rows hold 'a': only k = 4 matches bytewise.
    for sql in [
        "SELECT COUNT(*) FROM y0 JOIN y1 ON y1.k = y0.b WHERE y1.s = y0.s",
        "SELECT COUNT(*) FROM y0 IGNORE INDEX (b) JOIN y1 ON y1.k = y0.b WHERE y1.s = y0.s",
        "SELECT COUNT(*) FROM y1 STRAIGHT_JOIN y0 ON y1.k = y0.b AND NOT (y1.s <> y0.s)",
        "SELECT COUNT(*) FROM y0 JOIN y1 ON y1.k = y0.b WHERE y1.s > y0.s",
    ] {
        assert_eq!(query_rows(&engine, sql), ints(&[&[4]]), "{sql}");
    }
}

/// A fused aggregate over a build-preserved hash LEFT JOIN evaluates computed
/// aggregate inputs for unmatched build rows too: `SUM(0)` is 0, not NULL.
#[test]
fn fused_outer_join_aggregate_evaluates_computed_inputs_for_unmatched_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let seq = |n: u32| {
        format!("WITH RECURSIVE r(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM r WHERE n < {n}) ")
    };
    exec_all(
        &engine,
        &[
            "CREATE TABLE f0 (id INT PRIMARY KEY, a INT, KEY(a))",
            "CREATE TABLE f1 (id INT PRIMARY KEY, k INT)",
            "CREATE TABLE f2 (id INT PRIMARY KEY, k INT)",
            &format!(
                "INSERT INTO f0 {}SELECT n, CASE WHEN n < 10 THEN n ELSE n + 1000 END FROM r",
                seq(14)
            ),
            &format!(
                "INSERT INTO f1 {}SELECT n, CASE WHEN n % 3 = 0 THEN NULL ELSE n % 4 END FROM r",
                seq(59)
            ),
            &format!("INSERT INTO f2 {}SELECT n, n % 4 FROM r", seq(199)),
            "ANALYZE TABLE f0, f1, f2",
        ],
    );
    for aggregate in ["SUM(0)", "SUM(COALESCE(f2.k, 0) * 0)"] {
        let sql = format!(
            "SELECT f1.k, COUNT(*), CAST({aggregate} AS SIGNED) FROM f0 \
             LEFT JOIN f1 ON f1.id = f0.a LEFT JOIN f2 ON f2.k = f1.k GROUP BY f1.k"
        );
        let rows = query_rows(&engine, &sql);
        assert!(!rows.is_empty(), "{sql}");
        for row in rows {
            assert_eq!(row[2], Value::Int(0), "{sql}: {row:?}");
        }
    }
}

/// A correlated subquery executed through the reusable placeholder plan keeps
/// the outer column's collation: `o1.s > 'abc'` is binary for a bin column.
#[test]
fn cached_correlated_subquery_keeps_outer_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE o1 (id INT PRIMARY KEY, a INT, s VARCHAR(10) COLLATE utf8mb4_bin)",
            "CREATE TABLE o2 (id INT PRIMARY KEY, a INT)",
            "INSERT INTO o1 VALUES (1, 1, 'a_b'), (2, 1, 'b'), (3, 1, 'A'), (4, 1, 'abd')",
            "INSERT INTO o2 VALUES (1, 1), (2, 1)",
        ],
    );
    // Binary order: 'A' < 'a_b' < 'abc' < 'abd' < 'b'.
    let cases: &[(&str, Vec<Vec<Value>>)] = &[
        (
            "SELECT id FROM o1 WHERE a IN (SELECT o2.a FROM o2 WHERE o1.s > 'abc') ORDER BY id",
            ints(&[&[2], &[4]]),
        ),
        (
            "SELECT id FROM o1 WHERE a NOT IN (SELECT o2.a FROM o2 WHERE o1.s > 'abc') ORDER BY id",
            ints(&[&[1], &[3]]),
        ),
        (
            "SELECT id, (SELECT COUNT(*) FROM o2 WHERE o1.s > 'abc') FROM o1 ORDER BY id",
            ints(&[&[1, 0], &[2, 2], &[3, 0], &[4, 2]]),
        ),
        (
            "SELECT id, (SELECT COUNT(*) FROM o2 WHERE o1.s = 'a') FROM o1 ORDER BY id",
            ints(&[&[1, 0], &[2, 0], &[3, 0], &[4, 0]]),
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(&query_rows(&engine, sql), expected, "{sql}");
    }
}

/// An index nested-loop bound whose value is a correlated subquery over the
/// inner row (`q1.a < (SELECT ... q1.a ...)`) is a residual, not a key that
/// can be evaluated from the outer row alone.
#[test]
fn inlj_bound_with_inner_correlated_subquery_is_residual() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT, k INT, KEY(a), KEY(b, a))",
            "INSERT INTO t1 VALUES (52,NULL,7,NULL),(53,-1,0,NULL),(54,6,3,0),(55,NULL,-7,3),\
             (56,-7,NULL,6),(57,3,NULL,4),(41,-7,6,-7),(42,1,100,10),(43,5,NULL,-7),(44,3,0,1),\
             (45,10,NULL,2),(46,5,7,1),(47,1,-7,10),(48,2,0,2),(49,10,3,0),(50,1,6,3),(51,100,7,10)",
            "ANALYZE TABLE t1",
        ],
    );
    let filter = "FROM t1 q0 JOIN t1 q1 ON q1.b = q0.b \
                  WHERE q1.a < (SELECT COUNT(z0.b) FROM t1 z0 WHERE z0.a = (q1.a * q0.k))";
    assert_eq!(
        query_rows(
            &engine,
            &format!("SELECT q0.id, q1.id {filter} ORDER BY 1, 2")
        ),
        ints(&[&[41, 41], &[44, 53], &[48, 53], &[50, 41], &[53, 53]])
    );
    assert_eq!(
        query_rows(
            &engine,
            &format!(
                "SELECT q0.a, CAST(SUM(10) OVER (PARTITION BY q0.a) AS SIGNED) {filter} ORDER BY 1"
            )
        ),
        ints(&[&[-7, 10], &[-1, 10], &[1, 10], &[2, 10], &[3, 10]])
    );
}

/// Control-flow expressions keep their value operands' collation on the
/// row path too: a CASE over a subquery condition compares `utf8mb4_bin`.
#[test]
fn control_flow_with_subquery_condition_keeps_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE p1 (id INT PRIMARY KEY, b INT, s VARCHAR(10) COLLATE utf8mb4_bin)",
            "CREATE TABLE p2 (id INT PRIMARY KEY, a INT)",
            "INSERT INTO p1 VALUES (1, 1, 'a'), (2, 1, 'A'), (3, 2, 'b')",
            "INSERT INTO p2 VALUES (1, 1), (2, 2)",
        ],
    );
    for sql in [
        "SELECT id FROM p1 WHERE CASE WHEN EXISTS (SELECT 1 FROM p2 WHERE p2.a = p1.b) THEN s ELSE 'w' END = 'a'",
        "SELECT id FROM p1 WHERE IF((SELECT COUNT(*) FROM p2 WHERE p2.a = p1.b) > 0, s, 'w') = 'a'",
        "SELECT id FROM p1 WHERE COALESCE((SELECT NULL FROM p2 WHERE p2.a = p1.b), s) = 'a'",
    ] {
        assert_eq!(query_rows(&engine, sql), ints(&[&[1]]), "{sql}");
    }
}

/// Correlated UPDATE/DELETE bind the target row's qualified columns as
/// collated values, so `t.s < 'ab'` on a `utf8mb4_bin` column stays binary.
#[test]
fn correlated_dml_keeps_target_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_all(
        &engine,
        &[
            "CREATE TABLE u1 (id INT PRIMARY KEY, a INT, s VARCHAR(10) COLLATE utf8mb4_bin)",
            "CREATE TABLE u2 (id INT PRIMARY KEY, a INT)",
            "INSERT INTO u1 VALUES (1, 1, 'a_b'), (2, 1, 'b'), (3, 1, 'A')",
            "INSERT INTO u2 VALUES (1, 100)",
            "UPDATE u1 SET a = (SELECT MAX(u2.a) FROM u2 WHERE u2.a > u1.id) \
             WHERE (u1.id IN (-7, 6) OR NOT (u1.s < 'ab')) \
             AND 10 < (SELECT SUM(u2.a) FROM u2 WHERE u2.a > u1.id)",
        ],
    );
    // Binary order: 'A' < 'a_b' < 'ab' < 'b'; only 'b' satisfies s >= 'ab'.
    assert_eq!(
        query_rows(&engine, "SELECT id, a FROM u1 ORDER BY id"),
        ints(&[&[1, 1], &[2, 100], &[3, 1]])
    );
    engine
        .execute("DELETE FROM u1 WHERE u1.s = 'a' AND EXISTS (SELECT 1 FROM u2 WHERE u2.a > u1.id)")
        .unwrap();
    assert_eq!(
        query_rows(&engine, "SELECT COUNT(*) FROM u1"),
        ints(&[&[3]])
    );
}
