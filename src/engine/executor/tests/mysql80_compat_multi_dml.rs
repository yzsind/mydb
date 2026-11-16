//! P1-A2: NATURAL/USING extensions for multitable UPDATE/DELETE.
//!
//! Previously multitable UPDATE/DELETE rejected NATURAL JOIN and
//! USING columns with 1235. MySQL 8.0 supports both, and ORMs such as Hibernate/Diesel
//! use USING to simplify joins. These tests establish:
//!
//! USING columns actually affect multitable UPDATE/DELETE.
//! NATURAL JOIN actually affects multitable UPDATE/DELETE.
//! NATURAL without shared columns still reports a clear error.
//! Combine USING columns with WHERE filters.

use super::support::execute_materialized as exec;
use super::*;

fn count_int(rs: QueryResponse) -> i64 {
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    match &rs.rows[0][0] {
        Value::Int(v) => *v,
        Value::UInt(v) => *v as i64,
        Value::DecimalCompact(mantissa, _scale) => *mantissa as i64,
        Value::Decimal(s) => s
            .parse::<i64>()
            .unwrap_or_else(|_| panic!("decimal {s} is not integer-like")),
        other => panic!("expected int-like, got {other:?}"),
    }
}

#[test]
fn multi_table_update_using_clause() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE a (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE b (id INT PRIMARY KEY, w INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO a VALUES (1,10),(2,20)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO b VALUES (1,100),(2,200)",
    )
    .unwrap();

    // USING(id) is equivalent to ON a.id = b.id.
    exec(
        &engine,
        &mut session,
        "UPDATE a JOIN b USING (id) SET a.v = b.w",
    )
    .unwrap();

    let rs = exec(&engine, &mut session, "SELECT SUM(v) FROM a").unwrap();
    assert_eq!(count_int(rs), 300);
}

#[test]
fn multi_table_delete_using_clause() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE a (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE TABLE b (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO a VALUES (1,10),(2,20),(3,30)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO b VALUES (2)").unwrap();

    // Delete rows in `a` whose id matches `b`.
    exec(&engine, &mut session, "DELETE a FROM a JOIN b USING (id)").unwrap();

    let rs = exec(&engine, &mut session, "SELECT COUNT(*) FROM a").unwrap();
    assert_eq!(count_int(rs), 2);
    let rs = exec(&engine, &mut session, "SELECT SUM(v) FROM a").unwrap();
    assert_eq!(count_int(rs), 40);
}

#[test]
fn multi_table_update_natural_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE a (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE b (id INT PRIMARY KEY, w INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO a VALUES (1,10),(2,20)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO b VALUES (1,100),(2,200)",
    )
    .unwrap();

    // NATURAL JOIN on the shared `id` column.
    exec(
        &engine,
        &mut session,
        "UPDATE a NATURAL JOIN b SET a.v = b.w",
    )
    .unwrap();

    let rs = exec(&engine, &mut session, "SELECT SUM(v) FROM a").unwrap();
    assert_eq!(count_int(rs), 300);
}

#[test]
fn multi_table_natural_join_without_common_columns_degenerates_to_cross() {
    // MySQL 8.0 lets NATURAL JOIN silently degenerate to CROSS JOIN
    // when there is no shared column. mydb matches: the UPDATE runs as
    // a Cartesian-product update over the implicit cross.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE a (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE b (other INT PRIMARY KEY, w INT)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO a VALUES (1,10)").unwrap();
    exec(&engine, &mut session, "INSERT INTO b VALUES (5,100)").unwrap();

    // Should succeed: every (a, b) pair is "matched" since there is
    // no NATURAL-derived predicate. a.v is overwritten with b.w.
    exec(
        &engine,
        &mut session,
        "UPDATE a NATURAL JOIN b SET a.v = b.w",
    )
    .unwrap();
    let rs = exec(&engine, &mut session, "SELECT v FROM a WHERE id = 1").unwrap();
    assert_eq!(count_int(rs), 100);
}

#[test]
fn multi_table_update_using_combined_with_where() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE a (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE b (id INT PRIMARY KEY, w INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO a VALUES (1,10),(2,20),(3,30)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO b VALUES (1,100),(2,200),(3,300)",
    )
    .unwrap();

    // Only rows whose b.w > 150 are updated → ids 2 and 3.
    exec(
        &engine,
        &mut session,
        "UPDATE a JOIN b USING (id) SET a.v = b.w WHERE b.w > 150",
    )
    .unwrap();

    let rs = exec(&engine, &mut session, "SELECT v FROM a WHERE id = 1").unwrap();
    assert_eq!(count_int(rs), 10);
    let rs = exec(&engine, &mut session, "SELECT v FROM a WHERE id = 2").unwrap();
    assert_eq!(count_int(rs), 200);
    let rs = exec(&engine, &mut session, "SELECT v FROM a WHERE id = 3").unwrap();
    assert_eq!(count_int(rs), 300);
}

// ---------------------------------------------------------------------------
// G1 — multi-table DELETE `USING` form (MySQL 8.0). `DELETE FROM t1, t2 USING
// refs` is the clause-reordered twin of `DELETE t1, t2 FROM refs`; both must
// produce identical results.
// ---------------------------------------------------------------------------

fn setup_ab(engine: &StorageQueryEngine, session: &mut SessionContext) {
    exec(
        engine,
        session,
        "CREATE TABLE a (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(engine, session, "CREATE TABLE b (id INT PRIMARY KEY)").unwrap();
    exec(engine, session, "INSERT INTO a VALUES (1,10),(2,20),(3,30)").unwrap();
    exec(engine, session, "INSERT INTO b VALUES (2)").unwrap();
}

#[test]
fn delete_using_form_matches_from_form() {
    // Form B (USING) and form A must delete the same row (a.id = 2).
    let dir_a = tempfile::tempdir().unwrap();
    let engine_a = open_test_engine(dir_a.path());
    let mut s_a = SessionContext::default();
    setup_ab(&engine_a, &mut s_a);
    exec(&engine_a, &mut s_a, "DELETE a FROM a JOIN b ON a.id = b.id").unwrap();
    let form_a = exec(&engine_a, &mut s_a, "SELECT id FROM a ORDER BY id").unwrap();

    let dir_b = tempfile::tempdir().unwrap();
    let engine_b = open_test_engine(dir_b.path());
    let mut s_b = SessionContext::default();
    setup_ab(&engine_b, &mut s_b);
    exec(
        &engine_b,
        &mut s_b,
        "DELETE FROM a USING a JOIN b ON a.id = b.id",
    )
    .unwrap();
    let form_b = exec(&engine_b, &mut s_b, "SELECT id FROM a ORDER BY id").unwrap();

    let (QueryResponse::ResultSet(ra), QueryResponse::ResultSet(rb)) = (form_a, form_b) else {
        panic!("expected result sets");
    };
    assert_eq!(ra.rows, rb.rows);
    assert_eq!(ra.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn delete_using_form_with_where_and_using_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_ab(&engine, &mut session);
    exec(&engine, &mut session, "INSERT INTO b VALUES (3)").unwrap();
    // USING(id) join + WHERE filter: only a.id in b AND a.v > 25 → id 3.
    exec(
        &engine,
        &mut session,
        "DELETE FROM a USING a JOIN b USING (id) WHERE a.v > 25",
    )
    .unwrap();
    let rs = exec(&engine, &mut session, "SELECT id FROM a ORDER BY id").unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn delete_using_form_left_join_anti() {
    // Anti-join delete via USING form: remove `a` rows with no match in `b`.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_ab(&engine, &mut session);
    exec(
        &engine,
        &mut session,
        "DELETE FROM a USING a LEFT JOIN b ON a.id = b.id WHERE b.id IS NULL",
    )
    .unwrap();
    let rs = exec(&engine, &mut session, "SELECT id FROM a ORDER BY id").unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    // Only id = 2 matched b, so ids 1 and 3 (unmatched) are removed.
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn delete_using_form_multiple_targets() {
    // `DELETE FROM a, b USING a JOIN b` deletes matched rows from BOTH tables.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_ab(&engine, &mut session);
    exec(
        &engine,
        &mut session,
        "DELETE FROM a, b USING a JOIN b ON a.id = b.id",
    )
    .unwrap();
    let ra = exec(&engine, &mut session, "SELECT id FROM a ORDER BY id").unwrap();
    let rb = exec(&engine, &mut session, "SELECT COUNT(*) FROM b").unwrap();
    let QueryResponse::ResultSet(ra) = ra else {
        panic!("expected result set");
    };
    assert_eq!(ra.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    assert_eq!(count_int(rb), 0); // b's only row (id 2) matched and is gone.
}

// ---------------------------------------------------------------------------
// G2 — multi-table DELETE RIGHT JOIN, normalised to LEFT JOIN.
// ---------------------------------------------------------------------------

#[test]
fn delete_right_join_matches_left_equivalent() {
    // `DELETE a FROM a RIGHT JOIN b` ≡ `DELETE a FROM b LEFT JOIN a`. Both keep
    // every b row and delete matched a rows.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_ab(&engine, &mut session);
    exec(
        &engine,
        &mut session,
        "DELETE a FROM a RIGHT JOIN b ON a.id = b.id",
    )
    .unwrap();
    let rs = exec(&engine, &mut session, "SELECT id FROM a ORDER BY id").unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    // b has only id 2, so a.id = 2 is deleted; 1 and 3 remain.
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn delete_right_join_chain_rejected() {
    // RIGHT inside a multi-join chain is not the lone-flip case → clear error.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_ab(&engine, &mut session);
    exec(&engine, &mut session, "CREATE TABLE c (id INT PRIMARY KEY)").unwrap();
    let err = exec(
        &engine,
        &mut session,
        "DELETE a FROM a JOIN b ON a.id = b.id RIGHT JOIN c ON b.id = c.id",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1235, "{}", err.message());
}

// ---------------------------------------------------------------------------
// G3 — `tbl_name.*` target suffix (cosmetic; stripped before the parser).
// ---------------------------------------------------------------------------

#[test]
fn delete_target_wildcard_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_ab(&engine, &mut session);
    // Form A with `.*` on the target, and USING form with `.*` too.
    exec(
        &engine,
        &mut session,
        "DELETE a.* FROM a JOIN b ON a.id = b.id",
    )
    .unwrap();
    let rs = exec(&engine, &mut session, "SELECT id FROM a ORDER BY id").unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn delete_target_wildcard_suffix_using_form() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    setup_ab(&engine, &mut session);
    exec(
        &engine,
        &mut session,
        "DELETE FROM a.* USING a JOIN b ON a.id = b.id",
    )
    .unwrap();
    let rs = exec(&engine, &mut session, "SELECT COUNT(*) FROM a").unwrap();
    assert_eq!(count_int(rs), 2);
}

#[test]
fn delete_wildcard_strip_does_not_corrupt_strings_or_subqueries() {
    // Guard against the `.*` strip touching a string literal or a subquery
    // wildcard (both must be left intact).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(16))",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1,'.*'),(2,'keep'),(3,'x')",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO u VALUES (3)").unwrap();

    // String literal '.*' at depth 0 must NOT be stripped.
    exec(&engine, &mut session, "DELETE FROM t WHERE s = '.*'").unwrap();
    let rs = exec(&engine, &mut session, "SELECT id FROM t ORDER BY id").unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);

    // `u.*` inside a subquery (paren depth ≥ 1) must NOT be stripped. If it
    // were, `SELECT u.* FROM u` would collapse to `SELECT u FROM u` and fail;
    // instead it expands to u's single `id` column and the IN filter matches.
    exec(
        &engine,
        &mut session,
        "DELETE FROM t WHERE id IN (SELECT u.* FROM u)",
    )
    .unwrap();
    let rs = exec(&engine, &mut session, "SELECT id FROM t ORDER BY id").unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    // u = {3}; row id 3 is removed, leaving only id 2.
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

// ---------------------------------------------------------------------------
// G4 — per-table `PARTITION (...)` clause in multi-table DELETE.
// ---------------------------------------------------------------------------

#[test]
fn delete_multi_table_honors_partition_clause() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
            PARTITION p0 VALUES LESS THAN (10), \
            PARTITION p1 VALUES LESS THAN MAXVALUE)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE TABLE d (id INT PRIMARY KEY)").unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1,5),(2,15),(3,7),(4,20)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO d VALUES (1),(2),(3),(4)",
    )
    .unwrap();

    // Restrict the join scan of `t` to partition p0 (y < 10 → ids 1,3). All
    // four ids join `d`, but only p0 rows are eligible → delete ids 1 and 3.
    exec(
        &engine,
        &mut session,
        "DELETE t FROM t PARTITION (p0) JOIN d ON t.id = d.id",
    )
    .unwrap();
    let rs = exec(&engine, &mut session, "SELECT id FROM t ORDER BY id").unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(4)]]);
}

#[test]
fn delete_multi_table_unknown_partition_errors_1735() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
            PARTITION p0 VALUES LESS THAN (10), \
            PARTITION p1 VALUES LESS THAN MAXVALUE)",
    )
    .unwrap();
    exec(&engine, &mut session, "CREATE TABLE d (id INT PRIMARY KEY)").unwrap();
    let err = exec(
        &engine,
        &mut session,
        "DELETE t FROM t PARTITION (nope) JOIN d ON t.id = d.id",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1735, "{}", err.message());
}

// ---------------------------------------------------------------------------
// G5 — mydb intentionally extends MySQL with PostgreSQL 18 RETURNING.
// ---------------------------------------------------------------------------

#[test]
fn delete_returning_pg18_extension_returns_old_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "INSERT INTO a VALUES (1)").unwrap();
    let response = exec(
        &engine,
        &mut session,
        "DELETE FROM a WHERE id = 1 RETURNING id",
    )
    .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected RETURNING result set, got {response:?}");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

// ── B2: multi-table DML on a partitioned target must not confuse rows that
// share a row_id across partitions ─────────────────────────────────────────
fn all_rows_sorted(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let mut rows = rs.rows;
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}

#[test]
fn b2_multi_delete_partitioned_target_deletes_correct_partition_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE p (id INT, g INT, v INT) PARTITION BY LIST (g) (\
                 PARTITION p0 VALUES IN (0), PARTITION p1 VALUES IN (1))",
        )
        .unwrap();
    engine.execute("INSERT INTO p VALUES (100, 0, 10)").unwrap();
    engine.execute("INSERT INTO p VALUES (200, 1, 20)").unwrap();
    engine.execute("CREATE TABLE d (id INT)").unwrap();
    engine.execute("INSERT INTO d VALUES (200)").unwrap();
    engine
        .execute("DELETE p FROM p JOIN d ON p.id = d.id")
        .unwrap();
    assert_eq!(
        all_rows_sorted(&engine, "SELECT id, g, v FROM p"),
        vec![vec![Value::Int(100), Value::Int(0), Value::Int(10)]],
        "only the id=200 (partition p1) row should be gone"
    );
}

#[test]
fn b2_multi_delete_partitioned_target_no_cross_partition_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE p (id INT, g INT, v INT) PARTITION BY LIST (g) (\
                 PARTITION p0 VALUES IN (0), PARTITION p1 VALUES IN (1))",
        )
        .unwrap();
    engine.execute("INSERT INTO p VALUES (100, 0, 10)").unwrap();
    engine.execute("INSERT INTO p VALUES (200, 1, 20)").unwrap();
    engine.execute("CREATE TABLE d (id INT)").unwrap();
    engine.execute("INSERT INTO d VALUES (100), (200)").unwrap();
    let resp = engine
        .execute("DELETE p FROM p JOIN d ON p.id = d.id")
        .unwrap();
    match resp {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 2),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    assert!(
        all_rows_sorted(&engine, "SELECT id FROM p").is_empty(),
        "both partition rows should be deleted"
    );
}

#[test]
fn b2_multi_update_partitioned_target_updates_correct_partition_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE p (id INT, g INT, v INT) PARTITION BY LIST (g) (\
                 PARTITION p0 VALUES IN (0), PARTITION p1 VALUES IN (1))",
        )
        .unwrap();
    engine.execute("INSERT INTO p VALUES (100, 0, 10)").unwrap();
    engine.execute("INSERT INTO p VALUES (200, 1, 20)").unwrap();
    engine.execute("CREATE TABLE d (id INT)").unwrap();
    engine.execute("INSERT INTO d VALUES (200)").unwrap();
    engine
        .execute("UPDATE p JOIN d ON p.id = d.id SET p.v = 999")
        .unwrap();
    assert_eq!(
        all_rows_sorted(&engine, "SELECT id, v FROM p"),
        vec![
            vec![Value::Int(100), Value::Int(10)],
            vec![Value::Int(200), Value::Int(999)],
        ],
        "only the id=200 (partition p1) row should change"
    );
}

// B5 — a partitioned table's UNIQUE/PRIMARY key must be enforced against
// existing rows. The pre-check probed the non-existent `table.id` index/heap
// for partitioned tables, silently missing 1062. The unique key includes the
// partition column (MySQL requirement), so the conflict is partition-local.
#[test]
fn b5_partitioned_insert_enforces_unique_against_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE p (id INT, g INT, PRIMARY KEY (id, g)) PARTITION BY LIST (g) (\
                 PARTITION p0 VALUES IN (0), PARTITION p1 VALUES IN (1))",
        )
        .unwrap();
    engine.execute("INSERT INTO p VALUES (1, 0)").unwrap();

    // Single-row conflict against an already-committed row.
    let e1 = engine.execute("INSERT INTO p VALUES (1, 0)");
    assert_eq!(
        e1.as_ref().err().map(|e| e.code()),
        Some(1062),
        "single-row duplicate PK on partitioned table must be 1062, got {e1:?}"
    );

    // Batch conflict: a fresh row + a conflicting row in one statement.
    let e2 = engine.execute("INSERT INTO p VALUES (5, 0), (1, 0)");
    assert_eq!(
        e2.as_ref().err().map(|e| e.code()),
        Some(1062),
        "batch duplicate PK on partitioned table must be 1062, got {e2:?}"
    );

    // The conflicting statements were rejected atomically: exactly one row
    // with (1,0), and (5,0) from the failed batch must not have leaked.
    assert_eq!(
        all_rows_sorted(&engine, "SELECT id, g FROM p"),
        vec![vec![Value::Int(1), Value::Int(0)]],
    );
}

// B9 — multi-table UPDATE must refresh ON UPDATE CURRENT_TIMESTAMP columns on
// its target rows, just like single-table UPDATE. Previously the on-update
// column was never touched by the multi-table path.
#[test]
fn b9_multi_table_update_refreshes_on_update_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t1 (id INT PRIMARY KEY, v INT, \
                 ts TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00' \
                 ON UPDATE CURRENT_TIMESTAMP)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO t1 (id, v) VALUES (1, 10)")
        .unwrap();
    engine.execute("INSERT INTO t2 VALUES (1)").unwrap();

    engine
        .execute("UPDATE t1, t2 SET t1.v = 20 WHERE t1.id = t2.id")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT ts FROM t1 WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    let ts = rs.rows[0][0].to_display_string();
    assert_ne!(
        ts, "2000-01-01 00:00:00",
        "ON UPDATE CURRENT_TIMESTAMP must be refreshed by multi-table UPDATE, got {ts}"
    );
}

#[test]
fn outer_join_requires_condition_and_preserves_unmatched_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t4 (id VARCHAR(64))",
        "CREATE TABLE t2 (id VARCHAR(64) PRIMARY KEY)",
        "INSERT INTO t4 VALUES ('matched'), ('missing'), (NULL)",
        "INSERT INTO t2 VALUES ('matched')",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    for sql in [
        "SELECT COUNT(*) FROM t4 LEFT JOIN t2 WHERE t4.id=t2.id",
        "UPDATE t4 LEFT JOIN t2 SET t4.id='changed' WHERE t4.id=t2.id",
        "DELETE t4 FROM t4 LEFT JOIN t2 WHERE t4.id=t2.id",
    ] {
        let err = exec(&engine, &mut session, sql).unwrap_err();
        assert_eq!(err.code(), 1064, "{sql}: {}", err.message());
        assert!(err.message().contains("requires ON or USING"));
    }
    for (sql, expected) in [
        ("SELECT COUNT(*) FROM t4", 3),
        ("SELECT COUNT(*) FROM t4 LEFT JOIN t2 ON t4.id=t2.id", 3),
        ("SELECT COUNT(*) FROM t4 LEFT JOIN t2 USING (id)", 3),
        ("SELECT COUNT(*) FROM t4 NATURAL LEFT JOIN t2", 3),
        ("SELECT COUNT(*) FROM t2 RIGHT JOIN t4 ON t4.id=t2.id", 3),
        (
            "SELECT COUNT(*) FROM t4 LEFT JOIN t2 ON t4.id=t2.id WHERE t2.id IS NULL",
            2,
        ),
        (
            "SELECT COUNT(*) FROM t4 LEFT JOIN t2 ON t4.id=t2.id WHERE t4.id=t2.id",
            1,
        ),
        ("SELECT COUNT(*) FROM t4 JOIN t2 WHERE t4.id=t2.id", 1),
    ] {
        assert_eq!(
            count_int(exec(&engine, &mut session, sql).unwrap()),
            expected,
            "{sql}"
        );
    }
    exec(&engine, &mut session, "DELETE FROM t2").unwrap();
    assert_eq!(
        count_int(
            exec(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM t4 LEFT JOIN t2 ON t4.id=t2.id"
            )
            .unwrap()
        ),
        3
    );
}
