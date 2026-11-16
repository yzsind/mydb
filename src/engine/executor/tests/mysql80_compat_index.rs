//! MySQL 8.0 index compatibility regressions in docs/index-mysql80-compat-design.md.
//! Cover constraint errors 1170 for unprefixed BLOB/TEXT, 1071 for keys over 3072
//! bytes, 1070 for over 16 key columns, and 1069 for over 64 indexes; also preserve valid
//! LOB prefixes, exact 3072-byte keys, 16-column keys, and 64 indexes.
//! USING HASH and KEY_BLOCK_SIZE acceptance is covered in mysql80_compat_table_ddl.rs.
use super::*;

fn exec_ok(engine: &StorageQueryEngine, sql: &str) {
    engine
        .execute(sql)
        .unwrap_or_else(|err| panic!("expected success for `{sql}`: {err:?}"));
}

fn exec_err(engine: &StorageQueryEngine, sql: &str) -> QueryError {
    match engine.execute(sql) {
        Err(err) => err,
        Ok(_) => panic!("expected error for `{sql}`"),
    }
}

// G4, ER 1170: BLOB/TEXT keys require a prefix length.

#[test]
fn blob_text_key_without_prefix_is_1170() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Inline KEY in CREATE TABLE.
    let err = exec_err(&engine, "CREATE TABLE t1 (id INT, body TEXT, KEY k (body))");
    assert_eq!(err.code(), 1170, "{}", err.message());
    assert!(err.message().contains("body"), "{}", err.message());
    // The same applies to BLOB and LOB columns inside composite keys.
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (id INT, payload BLOB, KEY k (id, payload))",
    );
    assert_eq!(err.code(), 1170, "{}", err.message());
    // ALTER ADD INDEX and CREATE INDEX paths.
    exec_ok(&engine, "CREATE TABLE t1 (id INT, body TEXT)");
    let err = exec_err(&engine, "ALTER TABLE t1 ADD INDEX k (body)");
    assert_eq!(err.code(), 1170, "{}", err.message());
    let err = exec_err(&engine, "CREATE INDEX k ON t1 (body)");
    assert_eq!(err.code(), 1170, "{}", err.message());
    // TEXT in PRIMARY KEY follows the same rule.
    let err = exec_err(&engine, "CREATE TABLE t2 (body TEXT, PRIMARY KEY (body))");
    assert_eq!(err.code(), 1170, "{}", err.message());
}

#[test]
fn lob_key_with_prefix_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT, body TEXT, payload BLOB, KEY k1 (body(64)), KEY k2 (payload(64)))",
    );
    exec_ok(&engine, "INSERT INTO t1 VALUES (1, 'hello', 'world')");
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM t1 WHERE body = 'hello'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn fulltext_on_text_without_prefix_stays_legal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // FULLTEXT intentionally indexes entire LOB columns and must not fail with 1170.
    exec_ok(
        &engine,
        "CREATE TABLE t1 (id INT, body TEXT, FULLTEXT KEY ft (body))",
    );
}

// G5, ER 1071: maximum key length is 3072 bytes.

#[test]
fn key_longer_than_3072_bytes_is_1071() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // utf8mb4 has mbmaxlen=4; VARCHAR(769) needs 3076 bytes and exceeds the limit.
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (v VARCHAR(769) CHARACTER SET utf8mb4, KEY k (v))",
    );
    assert_eq!(err.code(), 1071, "{}", err.message());
    // Composite key length also exceeds the limit: 768*4+4=3076.
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (id INT, v VARCHAR(768) CHARACTER SET utf8mb4, KEY k (v, id))",
    );
    assert_eq!(err.code(), 1071, "{}", err.message());
    // Prefix lengths count too: TEXT(769) needs 3076 bytes.
    let err = exec_err(
        &engine,
        "CREATE TABLE t1 (body TEXT CHARACTER SET utf8mb4, KEY k (body(769)))",
    );
    assert_eq!(err.code(), 1071, "{}", err.message());
    // ALTER path.
    exec_ok(
        &engine,
        "CREATE TABLE t1 (v VARCHAR(769) CHARACTER SET utf8mb4)",
    );
    let err = exec_err(&engine, "ALTER TABLE t1 ADD INDEX k (v)");
    assert_eq!(err.code(), 1071, "{}", err.message());
}

#[test]
fn key_at_exactly_3072_bytes_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // 768*4=3072 exactly meets the accepted MySQL boundary.
    exec_ok(
        &engine,
        "CREATE TABLE t1 (v VARCHAR(768) CHARACTER SET utf8mb4, KEY k (v))",
    );
    // VARCHAR(3072) is valid in a single-byte character set.
    exec_ok(
        &engine,
        "CREATE TABLE t2 (v VARCHAR(3072) CHARACTER SET latin1, KEY k (v))",
    );
    // One extra byte must fail.
    let err = exec_err(
        &engine,
        "CREATE TABLE t3 (v VARCHAR(3073) CHARACTER SET latin1, KEY k (v))",
    );
    assert_eq!(err.code(), 1071, "{}", err.message());
}

// G6, ER 1070: at most 16 key columns.

#[test]
fn more_than_16_key_parts_is_1070() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let cols: Vec<String> = (0..17).map(|i| format!("c{i} INT")).collect();
    let parts: Vec<String> = (0..17).map(|i| format!("c{i}")).collect();
    let err = exec_err(
        &engine,
        &format!(
            "CREATE TABLE t1 ({}, KEY k ({}))",
            cols.join(", "),
            parts.join(", ")
        ),
    );
    assert_eq!(err.code(), 1070, "{}", err.message());
    // PRIMARY KEY also rejects more than 16 columns.
    let err = exec_err(
        &engine,
        &format!(
            "CREATE TABLE t1 ({}, PRIMARY KEY ({}))",
            cols.join(", "),
            parts.join(", ")
        ),
    );
    assert_eq!(err.code(), 1070, "{}", err.message());
    // Exactly 16 columns are valid.
    let parts16 = parts[..16].join(", ");
    exec_ok(
        &engine,
        &format!("CREATE TABLE t1 ({}, KEY k ({parts16}))", cols.join(", ")),
    );
    // ALTER path.
    let err = exec_err(
        &engine,
        &format!("ALTER TABLE t1 ADD INDEX k2 ({})", parts.join(", ")),
    );
    assert_eq!(err.code(), 1070, "{}", err.message());
}

// G8: optimizer_switch use_invisible_indexes takes effect.

/// The first EXPLAIN row's key column; SQL NULL becomes None when no index is used.
fn explain_key(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Option<String> {
    let QueryResponse::ResultSet(result) = engine.execute_with_session(session, sql).unwrap()
    else {
        panic!("expected result set for `{sql}`");
    };
    let key_pos = result
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("key"))
        .expect("EXPLAIN must expose a `key` column");
    match &result.rows[0][key_pos] {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => panic!("unexpected key value: {other:?}"),
    }
}

#[test]
fn use_invisible_indexes_switch_controls_planner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let exec = |s: &mut SessionContext, sql: &str| {
        engine
            .execute_with_session(s, sql)
            .unwrap_or_else(|err| panic!("expected success for `{sql}`: {err:?}"));
    };
    exec(
        &mut s,
        "CREATE TABLE t1 (id INT, v INT, KEY k (v) INVISIBLE)",
    );
    for i in 0..200 {
        exec(&mut s, &format!("INSERT INTO t1 VALUES ({i}, {i})"));
    }
    // Default: INVISIBLE indexes are unavailable.
    let key = explain_key(&engine, &mut s, "EXPLAIN SELECT id FROM t1 WHERE v = 5");
    assert_eq!(key, None, "invisible index must not be chosen by default");
    // After enabling the switch, the index is usable and results remain correct.
    exec(&mut s, "SET optimizer_switch = 'use_invisible_indexes=on'");
    let key = explain_key(&engine, &mut s, "EXPLAIN SELECT id FROM t1 WHERE v = 5");
    assert_eq!(
        key.as_deref(),
        Some("k"),
        "use_invisible_indexes=on must expose the invisible index"
    );
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut s, "SELECT id FROM t1 WHERE v = 5")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5)]]);
    // Disabling restores default behavior.
    exec(&mut s, "SET optimizer_switch = 'use_invisible_indexes=off'");
    let key = explain_key(&engine, &mut s, "EXPLAIN SELECT id FROM t1 WHERE v = 5");
    assert_eq!(key, None);
    // default restores the documented off value.
    exec(&mut s, "SET optimizer_switch = 'use_invisible_indexes=on'");
    exec(
        &mut s,
        "SET optimizer_switch = 'use_invisible_indexes=default'",
    );
    let key = explain_key(&engine, &mut s, "EXPLAIN SELECT id FROM t1 WHERE v = 5");
    assert_eq!(key, None, "'default' must restore the documented off value");
}

#[test]
fn optimizer_switch_rejects_unknown_flag() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let err = engine
        .execute_with_session(&mut s, "SET optimizer_switch = 'no_such_flag=on'")
        .unwrap_err();
    assert_eq!(err.code(), 1231, "{}", err.message());
    // Existing flags remain unaffected and valid SET statements still work.
    engine
        .execute_with_session(&mut s, "SET optimizer_switch = 'hash_join=off'")
        .unwrap();
}

#[test]
fn optimizer_switch_defaults_are_honest() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut s, "SELECT @@optimizer_switch")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(switch) = &result.rows[0][0] else {
        panic!("expected string");
    };
    // Unimplemented optimizer paths must report off; implemented Index Merge
    // defaults to on, matching MySQL 8.0.
    for off_flag in ["skip_scan=off", "use_invisible_indexes=off"] {
        assert!(switch.contains(off_flag), "{off_flag} missing in {switch}");
    }
    for on_flag in [
        "index_merge=on",
        "index_merge_union=on",
        "index_merge_sort_union=on",
        "index_merge_intersection=on",
    ] {
        assert!(switch.contains(on_flag), "{on_flag} missing in {switch}");
    }
    assert!(switch.contains("hash_join=on"), "{switch}");
    assert!(switch.contains("semijoin=on"), "{switch}");
    // ICP is implemented through read_plan_table_ids_range_icp before heap fetches and defaults on,
    // so optimizer_switch truthfully reports on, matching MySQL 8.0.
    assert!(
        switch.contains("index_condition_pushdown=on"),
        "ICP should be reported on: {switch}"
    );
}

// G6, ER 1069: at most 64 secondary indexes.

#[test]
fn more_than_64_indexes_is_1069() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let keys: Vec<String> = (0..64).map(|i| format!("KEY k{i} (c)")).collect();
    // 64 secondary indexes plus PRIMARY are valid; PRIMARY is not counted.
    exec_ok(
        &engine,
        &format!(
            "CREATE TABLE t1 (id INT PRIMARY KEY, c INT, {})",
            keys.join(", ")
        ),
    );
    // The 65th secondary index fails with 1069 in ALTER and CREATE INDEX.
    let err = exec_err(&engine, "ALTER TABLE t1 ADD INDEX k64 (c)");
    assert_eq!(err.code(), 1069, "{}", err.message());
    let err = exec_err(&engine, "CREATE INDEX k64 ON t1 (c)");
    assert_eq!(err.code(), 1069, "{}", err.message());
    // Declaring 65 in CREATE TABLE also fails with 1069.
    let keys65: Vec<String> = (0..65).map(|i| format!("KEY k{i} (c)")).collect();
    let err = exec_err(
        &engine,
        &format!("CREATE TABLE t2 (c INT, {})", keys65.join(", ")),
    );
    assert_eq!(err.code(), 1069, "{}", err.message());
}

// ---------- NULL-key index equality semantics ----------

/// `WHERE indexed_col = NULL` must match no rows — SQL `=` against NULL is
/// UNKNOWN, regardless of whether the column is indexed. NULLs are still
/// stored in the index (seekable), so `IS NULL` and the null-safe `<=>`
/// keep matching them. Regression: the index equality path used to treat
/// `= NULL` (and a deferred `= ?` parameter that bound to NULL) like
/// `IS NULL` and return the NULL-valued rows, because both surface as a
/// `KeyPart::Null` probe and the B-tree orders `Null == Null` equal.
#[test]
fn indexed_equality_against_null_matches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(
        &engine,
        "CREATE TABLE t (id INT PRIMARY KEY, cat VARCHAR(8))",
    );
    // Index on the probed column so the equality drives an index lookup
    // (the only path that exhibited the bug; an unindexed column always
    // applied the residual `cat = NULL` filter and correctly returned 0).
    exec_ok(&engine, "CREATE INDEX ix_cat ON t(cat)");
    exec_ok(
        &engine,
        "INSERT INTO t VALUES (1,'A'),(2,'A'),(3,NULL),(4,NULL),(5,NULL)",
    );

    let count = |sql: &str| -> i64 {
        match engine.execute(sql).unwrap() {
            QueryResponse::ResultSet(rs) => match rs.rows[0][0] {
                Value::Int(n) => n,
                ref other => panic!("expected integer count, got {other:?} for `{sql}`"),
            },
            other => panic!("expected result set, got {other:?} for `{sql}`"),
        }
    };

    // The fix: `= NULL` is UNKNOWN -> no rows (was 3 before the fix, when
    // the planner pushed it into the index as a concrete NULL-key seek).
    assert_eq!(count("SELECT count(*) FROM t WHERE cat = NULL"), 0);
    // Null-safe equality and IS NULL still match the three stored NULLs.
    assert_eq!(count("SELECT count(*) FROM t WHERE cat <=> NULL"), 3);
    assert_eq!(count("SELECT count(*) FROM t WHERE cat IS NULL"), 3);
    // Ordinary (non-NULL) equality is unaffected.
    assert_eq!(count("SELECT count(*) FROM t WHERE cat = 'A'"), 2);
    // The deferred-parameter form (`= ?` binding NULL via a cached
    // parametric correlated-subquery plan, the original repro) is covered
    // by the decorrelate-on-vs-off differential test
    // `scalar_decorrelate::correlated_avg_null_key_group_matches_legacy_oracle`.
}

/// End-to-end: a non-aggregate `SELECT *` with an IN / OR predicate on an
/// indexed column returns exactly the rows the equivalent single-point
/// lookups do (regression guard on the fast-path → planner routing).
#[test]
fn in_list_select_star_matches_single_point_lookups() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    exec_ok(&engine, "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))");
    for a in 1..=50 {
        for b in 1..=3 {
            exec_ok(&engine, &format!("INSERT INTO t VALUES ({a}, {b})"));
        }
    }
    let rows = |sql: &str| -> Vec<Vec<Value>> {
        match engine.execute(sql).unwrap() {
            QueryResponse::ResultSet(rs) => {
                let mut r = rs.rows;
                r.sort_by_cached_key(|row| format!("{row:?}"));
                r
            }
            other => panic!("expected result set, got {other:?}"),
        }
    };
    // IN, its OR-chain equivalent, and the UNION of single lookups must all
    // agree — the buggy full-scan path returned the same rows but slowly,
    // so this pins correctness while the timing win is covered manually.
    let via_in = rows("SELECT * FROM t WHERE a IN (7, 11, 40)");
    let via_or = rows("SELECT * FROM t WHERE a = 7 OR a = 11 OR a = 40");
    let via_union = rows(
        "SELECT * FROM t WHERE a = 7 \
         UNION ALL SELECT * FROM t WHERE a = 11 \
         UNION ALL SELECT * FROM t WHERE a = 40",
    );
    assert_eq!(via_in.len(), 9, "3 keys x 3 rows each");
    assert_eq!(via_in, via_or);
    assert_eq!(via_in, via_union);
    // IN with an extra ANDed predicate still filters correctly.
    let mut filtered = rows("SELECT * FROM t WHERE a IN (7, 11, 40) AND b = 2");
    filtered.sort_by_key(|r| match r[0] {
        Value::Int(n) => n,
        _ => unreachable!(),
    });
    assert_eq!(
        filtered,
        vec![
            vec![Value::Int(7), Value::Int(2)],
            vec![Value::Int(11), Value::Int(2)],
            vec![Value::Int(40), Value::Int(2)],
        ]
    );
    // No-match IN returns empty, not the whole table.
    assert!(rows("SELECT * FROM t WHERE a IN (-1, -2)").is_empty());
}
