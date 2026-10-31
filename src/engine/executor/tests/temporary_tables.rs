use super::*;

fn result_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set, got {response:?}");
    };
    result.rows
}

fn first_int(response: QueryResponse) -> i64 {
    match &result_rows(response)[0][0] {
        Value::Int(v) => *v,
        other => panic!("expected int, got {other:?}"),
    }
}

#[test]
fn temporary_create_matches_persistent_schema_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    for (suffix, definition) in [
        ("invisible_pk", "(id INT, PRIMARY KEY (id) INVISIBLE)"),
        ("wide_bit", "(b BIT(65))"),
        (
            "oversized_fixed_vector_row",
            "(id INT PRIMARY KEY, a VECTOR(2048), b VECTOR(2048), c VECTOR(2048))",
        ),
    ] {
        let persistent_name = format!("base_{suffix}");
        let temporary_name = format!("temp_{suffix}");
        let persistent_error = engine
            .execute(&format!("CREATE TABLE {persistent_name} {definition}"))
            .expect_err("invalid persistent schema must be rejected");
        let temporary_error = engine
            .execute_with_session(
                &mut session,
                &format!("CREATE TEMPORARY TABLE {temporary_name} {definition}"),
            )
            .expect_err("invalid temporary schema must be rejected");
        assert_eq!(
            temporary_error.code(),
            persistent_error.code(),
            "persistent and temporary CREATE diverged for {definition}: persistent={persistent_error:?}, temporary={temporary_error:?}"
        );
        assert!(
            !session.has_temp_table(&temporary_name),
            "failed CREATE published temporary table {temporary_name}"
        );
    }
    let payload = "00".repeat(65532);
    for (name, temporary) in [
        ("base_large_vector", ""),
        ("temp_large_vector", "TEMPORARY "),
    ] {
        engine
            .execute_with_session(
                &mut session,
                &format!("CREATE {temporary}TABLE {name}(v VECTOR(16383))"),
            )
            .unwrap();
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "INSERT INTO {name} VALUES(VECTOR_FROM_BINARY(X'{payload}','FLOAT32',16383))"
                ),
            )
            .unwrap();
        assert_eq!(
            first_int(
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("SELECT VECTOR_DIM(v) FROM {name}")
                    )
                    .unwrap()
            ),
            16383
        );
    }
}

#[test]
fn temporary_create_supports_shared_advanced_schema_features() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE advanced (\
                id INT PRIMARY KEY, \
                a INT NOT NULL, \
                doubled INT GENERATED ALWAYS AS (a * 2) STORED, \
                CHECK (a > 0), \
                KEY idx_expr ((a + 1)), \
                KEY idx_doubled (doubled) INVISIBLE\
            ) COMMENT='temporary advanced schema'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO advanced (id, a) VALUES (1, 7)")
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT doubled FROM advanced WHERE a + 1 = 8")
                .unwrap()
        ),
        vec![vec![Value::Int(14)]]
    );

    let check_error = engine
        .execute_with_session(&mut session, "INSERT INTO advanced (id, a) VALUES (2, 0)")
        .expect_err("temporary CHECK constraint must be enforced");
    assert_eq!(
        check_error.code(),
        crate::error::catalog::ER_CHECK_CONSTRAINT_VIOLATED
            .code()
            .get()
    );

    let indexes = result_rows(
        engine
            .execute_with_session(&mut session, "SHOW INDEX FROM advanced")
            .unwrap(),
    );
    assert!(indexes.iter().any(|row| {
        row[2] == Value::String("idx_doubled".to_string())
            && row[13] == Value::String("NO".to_string())
    }));
    assert!(indexes
        .iter()
        .any(|row| row[2] == Value::String("idx_expr".to_string())));
}

#[test]
fn temporary_table_shadows_persistent_table_per_session() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s1 = SessionContext::default();
    let mut s2 = SessionContext::default();

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1)").unwrap();

    engine
        .execute_with_session(&mut s1, "CREATE TEMPORARY TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    assert!(s1.has_temp_table("t"), "session temp registry lost t");
    assert_eq!(
        s1.temp_tables
            .get(&s1.temp_visible_key("t"))
            .map(|t| t.on_commit),
        Some(crate::parser::TempTableOnCommit::PreserveRows)
    );
    assert_eq!(
        s1.resolve_temp_table_name("test.t"),
        s1.resolve_temp_table_name("t")
    );
    engine
        .execute_with_session(&mut s1, "INSERT INTO t VALUES (2)")
        .unwrap();

    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut s1, "SELECT id FROM t ORDER BY id")
                .unwrap()
        ),
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut s2, "SELECT id FROM t ORDER BY id")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );

    engine
        .execute_with_session(&mut s1, "CREATE TEMPORARY TABLE tmp_only (id INT)")
        .unwrap();
    let show = result_rows(engine.execute_with_session(&mut s1, "SHOW TABLES").unwrap());
    assert!(!show
        .iter()
        .any(|row| row[0] == Value::String("tmp_only".to_string())));

    engine
        .execute_with_session(&mut s1, "DROP TEMPORARY TABLE t")
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut s1, "SELECT id FROM t")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );
    engine
        .execute_with_session(&mut s1, "DROP TEMPORARY TABLE IF EXISTS t")
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut s1, "SELECT id FROM t")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn on_commit_lifecycles_follow_transaction_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TEMP TABLE rolled_back (id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO rolled_back VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM rolled_back")
                .unwrap()
        ),
        0,
        "CREATE survives ROLLBACK while inserted rows roll back"
    );

    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE preserve_rows (id INT) ON COMMIT PRESERVE ROWS",
        )
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO preserve_rows VALUES (7)")
        .unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM preserve_rows")
                .unwrap()
        ),
        1
    );

    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE delete_rows (id INT) ON COMMIT DELETE ROWS",
        )
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO delete_rows VALUES (1), (2)")
        .unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM delete_rows")
                .unwrap()
        ),
        0
    );

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE drop_rows (id INT) ON COMMIT DROP",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO drop_rows VALUES (1)")
        .unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    assert!(engine
        .execute_with_session(&mut session, "SELECT * FROM drop_rows")
        .is_err());

    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE auto_drop (id INT) ON COMMIT DROP",
        )
        .unwrap();
    assert!(engine
        .execute_with_session(&mut session, "SELECT * FROM auto_drop")
        .is_err());
}

#[test]
fn temporary_create_drop_are_not_rollbackable_and_report_mysql_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE sp (id INT, v INT)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "SAVEPOINT before_create")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE made (id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT before_create")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut session, "SELECT * FROM made")
        .is_ok());

    engine
        .execute_with_session(&mut session, "SAVEPOINT before_drop")
        .unwrap();
    engine
        .execute_with_session(&mut session, "DROP TEMPORARY TABLE sp")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut session, "SELECT * FROM sp")
        .is_err());
    engine
        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT before_drop")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut session, "SELECT * FROM sp")
        .is_err());

    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let warnings = result_rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    let codes = warnings
        .iter()
        .map(|row| row[1].to_display_string())
        .collect::<Vec<_>>();
    assert!(codes.iter().any(|code| code == "1751"), "{warnings:?}");
    assert!(codes.iter().any(|code| code == "1752"), "{warnings:?}");
}

#[test]
fn temporary_alter_index_and_truncate_implicitly_commit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE committed (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE tt (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO tt VALUES (1, 10)")
        .unwrap();

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO committed VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE tt ADD COLUMN c INT DEFAULT 7, ADD INDEX tt_v_idx(v)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        first_int(engine.execute("SELECT COUNT(*) FROM committed").unwrap()),
        1
    );
    let indexes = result_rows(
        engine
            .execute_with_session(&mut session, "SHOW INDEX FROM tt")
            .unwrap(),
    );
    assert!(indexes
        .iter()
        .any(|row| row[2] == Value::String("tt_v_idx".to_string())));
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT c FROM tt WHERE id=1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(7)]]);

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO committed VALUES (2)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "TRUNCATE TABLE tt")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        first_int(engine.execute("SELECT COUNT(*) FROM committed").unwrap()),
        2
    );
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM tt")
                .unwrap()
        ),
        0
    );
}

#[test]
fn temporary_modify_change_and_index_rename_preserve_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE tt(id INT PRIMARY KEY, v VARCHAR(10), KEY iv(v))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO tt VALUES(1,'10'),(2,'20')")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE tt MODIFY v BIGINT")
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT id,v FROM tt ORDER BY id")
                .unwrap()
        ),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
    engine
        .execute_with_session(&mut session, "ALTER TABLE tt CHANGE v amount BIGINT")
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT id,amount FROM tt ORDER BY id")
                .unwrap()
        ),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
    engine
        .execute_with_session(&mut session, "ALTER TABLE tt RENAME INDEX iv TO amount_idx")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(
            &mut session,
            "SELECT id,amount FROM tt FORCE INDEX(amount_idx) WHERE amount=20",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(2), Value::Int(20)]]);
    let create = result_rows(
        engine
            .execute_with_session(&mut session, "SHOW CREATE TABLE tt")
            .unwrap(),
    )[0][1]
        .to_display_string();
    assert!(create.contains("`amount_idx`"), "{create}");
    assert!(!create.contains("`iv`"), "{create}");
}

#[test]
fn temporary_like_ctas_show_and_rename_work() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT, KEY v_idx (v))")
        .unwrap();
    engine.execute("INSERT INTO src VALUES (1, 10)").unwrap();

    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE tl LIKE src")
        .unwrap();
    let create = result_rows(
        engine
            .execute_with_session(&mut session, "SHOW CREATE TABLE tl")
            .unwrap(),
    )[0][1]
        .to_display_string();
    assert!(create.starts_with("CREATE TEMPORARY TABLE"), "{create}");
    assert!(create.contains("PRIMARY KEY"), "{create}");
    assert!(create.contains("`v_idx`"), "{create}");

    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE tc AS SELECT id, v FROM src",
        )
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT * FROM tc")
                .unwrap()
        ),
        vec![vec![Value::Int(1), Value::Int(10)]]
    );

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE tc RENAME TO tc2")
        .unwrap();
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT * FROM tc2")
                .unwrap()
        ),
        vec![vec![Value::Int(1), Value::Int(10)]]
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut session, "SELECT * FROM tc")
        .is_err());
    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT * FROM tc2")
                .unwrap()
        ),
        vec![vec![Value::Int(1), Value::Int(10)]]
    );
}

#[test]
fn temporary_dml_does_not_write_wal_or_undo_journal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    let wal_before = engine.database().storage_status().wal_bytes;
    let undo_before = engine.database().storage_status().undo_disk_bytes;

    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE tw (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO tw VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE tw SET v = v + 1 WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "DELETE FROM tw WHERE id = 2")
        .unwrap();

    assert_eq!(
        result_rows(
            engine
                .execute_with_session(&mut session, "SELECT v FROM tw")
                .unwrap()
        ),
        vec![vec![Value::Int(11)]]
    );
    assert_eq!(engine.database().storage_status().wal_bytes, wal_before);
    let undo_after = engine.database().storage_status().undo_disk_bytes;
    assert_eq!(undo_after, undo_before);
}

/// G2: temporary-table TRUNCATE resets AUTO_INCREMENT; the next INSERT starts at 1, matching MySQL.
#[test]
fn temp_table_truncate_resets_auto_increment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t (v) VALUES (10), (20), (30)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "TRUNCATE TABLE t")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t (v) VALUES (99)")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT id FROM t")
                .unwrap()
        ),
        1,
        "temp table AUTO_INCREMENT must restart at 1 after TRUNCATE"
    );
}

/// MySQL TRUNCATE on a temporary table implicitly commits and is not
/// rollbackable; rows stay gone and AUTO_INCREMENT restarts at 1.
#[test]
fn temp_table_truncate_is_not_rollbackable_and_resets_auto_increment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t (v) VALUES (10), (20), (30)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "TRUNCATE TABLE t")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
                .unwrap()
        ),
        0
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
                .unwrap()
        ),
        0,
        "rollback must not restore truncated rows"
    );
    engine
        .execute_with_session(&mut session, "INSERT INTO t (v) VALUES (40)")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT MAX(id) FROM t")
                .unwrap()
        ),
        1,
        "TRUNCATE must reset the AUTO_INCREMENT counter"
    );
}

#[test]
fn temporary_column_metadata_and_check_alter_match_persistent_capabilities() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE t (a INT, b INT DEFAULT 1)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, -1)")
        .unwrap();

    engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE t MODIFY b INT COMMENT 'middle' FIRST",
        )
        .unwrap();
    let columns = result_rows(
        engine
            .execute_with_session(&mut session, "SHOW FULL COLUMNS FROM t")
            .unwrap(),
    );
    assert_eq!(columns[0][0], Value::String("b".to_string()));
    assert_eq!(columns[0][8], Value::String("middle".to_string()));

    engine
        .execute_with_session(&mut session, "ALTER TABLE t ALTER COLUMN b SET DEFAULT 9")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t (a) VALUES (2)")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT b FROM t WHERE a=2")
                .unwrap()
        ),
        9
    );

    engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE t ADD CONSTRAINT b_positive CHECK (b > 0) NOT ENFORCED",
        )
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE t ALTER CHECK b_positive ENFORCED",
        )
        .unwrap_err();
    assert_eq!(err.code(), 3819);
    engine
        .execute_with_session(&mut session, "UPDATE t SET b=1 WHERE a=1")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "ALTER TABLE t ALTER CHECK b_positive ENFORCED",
        )
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "INSERT INTO t VALUES (-1, 3)")
            .unwrap_err()
            .code(),
        3819
    );
    engine
        .execute_with_session(&mut session, "ALTER TABLE t DROP CHECK b_positive")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (-1, 3)")
        .unwrap();
}

#[test]
fn temporary_audit_internal_names_are_session_private() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut owner = SessionContext::default();
    let mut other = SessionContext::default();
    engine
        .execute_with_session(&mut owner, "CREATE TEMPORARY TABLE secret (id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO secret VALUES (42)")
        .unwrap();
    let key = owner.resolve_temp_table_name("secret").unwrap();
    for sql in [
        format!("SELECT * FROM {key}"),
        format!("INSERT INTO {key} VALUES (99)"),
        format!("UPDATE {key} SET id=99"),
        format!("DELETE FROM {key}"),
        format!("CHECK TABLE {key}"),
        format!("SHOW CREATE TABLE {key}"),
        format!("DROP TABLE {key}"),
    ] {
        assert!(
            engine.execute_with_session(&mut other, &sql).is_err(),
            "cross-session access succeeded: {sql}"
        );
    }
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut owner, "SELECT id FROM secret")
                .unwrap()
        ),
        42
    );
}

#[test]
fn temporary_audit_view_keeps_persistent_binding() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE shadow (id INT)",
        "INSERT INTO shadow VALUES (1)",
        "CREATE VIEW persistent_view AS SELECT id FROM shadow",
        "CREATE VIEW nested_view AS SELECT id FROM persistent_view",
        "CREATE TEMPORARY TABLE shadow (id INT)",
        "INSERT INTO shadow VALUES (777)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    for sql in [
        "SELECT id FROM persistent_view",
        "SELECT id FROM nested_view",
        "SELECT v.id FROM persistent_view v JOIN shadow t ON t.id=777",
    ] {
        assert_eq!(
            first_int(engine.execute_with_session(&mut session, sql).unwrap()),
            1,
            "{sql}"
        );
    }
}

#[test]
fn temporary_audit_prepared_rebinds_shadow_and_recreated_table() {
    use crate::engine::api::QueryEngine;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE shadow (id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO shadow VALUES (1)")
        .unwrap();
    let sql = "SELECT * FROM shadow";
    let base_template = engine.prepare_cached(&mut session, sql, 0).unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE shadow (id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO shadow VALUES (777)")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_prepared_statement(&mut session, sql, (*base_template).clone())
                .unwrap()
                .unwrap()
                .materialize()
                .unwrap()
        ),
        777
    );
    let temp_template = engine.prepare_cached(&mut session, sql, 0).unwrap();
    engine
        .execute_with_session(&mut session, "DROP TEMPORARY TABLE shadow")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE shadow (other INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO shadow VALUES (888)")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_prepared_statement(&mut session, sql, (*temp_template).clone())
                .unwrap()
                .unwrap()
                .materialize()
                .unwrap()
        ),
        888
    );
}

#[test]
fn temporary_audit_ctas_privileges_and_metadata_errors() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE TABLE src(id INT)",
        "INSERT INTO src VALUES(7)",
        "CREATE VIEW source_view AS SELECT id FROM src",
        "CREATE USER 'temp_creator'@'%' IDENTIFIED BY 'pw'",
        "GRANT CREATE TEMPORARY TABLES, SELECT ON *.* TO 'temp_creator'@'%'",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut user = SessionContext::default();
    user.user = "temp_creator".into();
    user.login_user = user.user.clone();
    user.authenticated_as = crate::engine::auth::AccountKey::new("temp_creator", "%");
    user.auth_epoch = engine.db.auth_catalog().epoch;
    engine
        .execute_with_session(&mut user, "CREATE TEMPORARY TABLE cp AS SELECT * FROM src")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut user, "SELECT id FROM cp")
                .unwrap()
        ),
        7
    );
    assert_eq!(
        engine
            .execute_with_session(&mut user, "CREATE TEMPORARY TABLE bad LIKE source_view")
            .unwrap_err()
            .code(),
        1347
    );
    for sql in [
        "CREATE TEMPORARY TABLE information_schema.tables(id INT)",
        "CREATE TEMPORARY TABLE information_schema.x LIKE src",
        "CREATE TEMPORARY TABLE information_schema.x AS SELECT * FROM src",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut root, sql)
                .unwrap_err()
                .code(),
            1044,
            "{sql}"
        );
    }
    let err = engine
        .execute_with_session(&mut root, "DROP TEMPORARY TABLE missing")
        .unwrap_err();
    assert_eq!(err.code(), 1051);
    assert!(err.message().contains("test.missing"));
}

#[test]
fn temporary_audit_maintenance_resolves_shadow() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE maintained(id INT)",
        "INSERT INTO maintained VALUES (1)",
        "CREATE TEMPORARY TABLE maintained(id INT)",
        "INSERT INTO maintained VALUES (2)",
        "CREATE TEMPORARY TABLE equivalent(id INT)",
        "INSERT INTO equivalent VALUES (2)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    for sql in [
        "CHECK TABLE maintained",
        "ANALYZE TABLE maintained",
        "REPAIR TABLE maintained",
    ] {
        let rows = result_rows(engine.execute_with_session(&mut session, sql).unwrap());
        assert_eq!(rows[0][0], Value::String("test.maintained".into()));
        assert!(
            !rows
                .iter()
                .any(|row| row[2].to_display_string().eq_ignore_ascii_case("error")),
            "{sql}: {rows:?}"
        );
    }
    let rows = result_rows(
        engine
            .execute_with_session(&mut session, "CHECKSUM TABLE maintained, equivalent")
            .unwrap(),
    );
    assert_ne!(rows[0][1], Value::Null);
    assert_eq!(rows[0][1], rows[1][1]);
}

#[test]
fn temporary_audit_prepared_join_writes_use_current_shadow() {
    use crate::engine::api::QueryEngine;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE tgt(id INT PRIMARY KEY, v INT)",
        "CREATE TABLE src(id INT PRIMARY KEY, v INT)",
        "INSERT INTO tgt VALUES (1,0)",
        "INSERT INTO src VALUES (1,5)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let update = "UPDATE tgt JOIN src ON tgt.id=src.id SET tgt.v=src.v";
    let delete = "DELETE tgt FROM tgt JOIN src ON tgt.id=src.id WHERE src.v=99";
    let u = engine.prepare_cached(&mut session, update, 0).unwrap();
    let d = engine.prepare_cached(&mut session, delete, 0).unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE src(id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src VALUES (1,99)")
        .unwrap();
    engine
        .execute_prepared_statement(&mut session, update, (*u).clone())
        .unwrap()
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT v FROM tgt")
                .unwrap()
        ),
        99
    );
    engine
        .execute_prepared_statement(&mut session, delete, (*d).clone())
        .unwrap()
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM tgt")
                .unwrap()
        ),
        0
    );
}

#[test]
fn temporary_audit_commit_cleanup_failure_keeps_committed_success() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE durable(id INT)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE cleanup(id INT) ON COMMIT DELETE ROWS",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE also_drop(id INT) ON COMMIT DROP",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO durable VALUES (42)")
        .unwrap();
    let key = session.resolve_temp_table_name("cleanup").unwrap();
    let id = engine.db.table(&key).unwrap().id;
    let heap = crate::storage::naming::temp_tables_dir(dir.path()).join(format!("{id}.heap"));
    std::fs::remove_file(&heap).unwrap();
    std::fs::create_dir(&heap).unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    assert!(session.current_txn.is_none());
    assert!(!session.explicit_txn);
    assert!(!session.has_temp_table("cleanup"));
    assert!(!session.has_temp_table("also_drop"));
    let warnings = result_rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert!(
        warnings
            .iter()
            .any(|row| row[2].to_display_string().contains("Transaction committed")),
        "{warnings:?}"
    );
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT id FROM durable")
                .unwrap()
        ),
        42
    );
    std::fs::remove_dir(&heap).unwrap();
}

#[test]
fn temporary_audit_self_reference_and_savepoint_are_supported() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "BEGIN",
        "CREATE TEMPORARY TABLE self_ref(id INT)",
        "INSERT INTO self_ref VALUES(1),(2),(3)",
        "SAVEPOINT s",
        "INSERT INTO self_ref SELECT * FROM self_ref",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM self_ref")
                .unwrap()
        ),
        6
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK TO s")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(
                    &mut session,
                    "SELECT COUNT(*) FROM self_ref a JOIN self_ref b ON a.id=b.id"
                )
                .unwrap()
        ),
        3
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert!(session.has_temp_table("self_ref"));
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM self_ref")
                .unwrap()
        ),
        0
    );
}

#[test]
fn temporary_audit_field_list_checks_owner() {
    use crate::engine::api::QueryEngine;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut owner = SessionContext::default();
    engine
        .execute_with_session(&mut owner, "CREATE TEMPORARY TABLE fields(id INT)")
        .unwrap();
    let key = owner.resolve_temp_table_name("fields").unwrap();
    assert_eq!(
        engine
            .field_list_columns(&owner, "fields", "")
            .unwrap()
            .len(),
        1
    );
    assert!(engine
        .field_list_columns(&SessionContext::default(), &key, "")
        .is_err());
}

#[test]
fn temporary_audit_prepared_memos_do_not_outlive_shadowing() {
    use crate::engine::api::{PreparedDmlState, PreparedSelectMemoState, QueryEngine};
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE memo(id INT PRIMARY KEY, v INT)",
        "INSERT INTO memo VALUES(1,1)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let select_sql = "SELECT v FROM memo WHERE id=?";
    let update_sql = "UPDATE memo SET v=? WHERE id=?";
    let select = engine.prepare_cached(&mut session, select_sql, 1).unwrap();
    let update = engine.prepare_cached(&mut session, update_sql, 2).unwrap();
    let mut select_state = PreparedSelectMemoState::default();
    let mut update_state = PreparedDmlState::default();
    engine
        .execute_prepared_select(
            &mut session,
            &mut select_state,
            &select,
            select_sql,
            &[Value::Int(1)],
        )
        .unwrap()
        .unwrap()
        .materialize()
        .unwrap();
    engine
        .execute_prepared_dml(
            &mut session,
            &mut update_state,
            &update,
            update_sql,
            &[Value::Int(2), Value::Int(1)],
        )
        .unwrap()
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE memo(id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO memo VALUES(1,99)")
        .unwrap();
    assert!(engine
        .execute_prepared_select(
            &mut session,
            &mut select_state,
            &select,
            select_sql,
            &[Value::Int(1)]
        )
        .is_none());
    assert!(engine
        .execute_prepared_dml(
            &mut session,
            &mut update_state,
            &update,
            update_sql,
            &[Value::Int(3), Value::Int(1)]
        )
        .is_none());
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT v FROM memo")
                .unwrap()
        ),
        99
    );
    engine
        .execute_with_session(&mut session, "DROP TEMPORARY TABLE memo")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT v FROM memo")
                .unwrap()
        ),
        2
    );
}

#[test]
fn temporary_audit_alter_snapshot_quota_failure_is_atomic() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE quota(id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO quota VALUES(1),(1)")
        .unwrap();
    let previous = super::super::spill_quota::global_limit_bytes();
    super::super::spill_quota::set_global_limit_bytes(1024);
    let result = engine.execute_with_session(&mut session, "ALTER TABLE quota ADD UNIQUE(id)");
    super::super::spill_quota::set_global_limit_bytes(previous);
    assert_eq!(result.unwrap_err().code(), 1041);
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM quota")
                .unwrap()
        ),
        2
    );
    let tmp = crate::storage::naming::system_tmp_dir(dir.path());
    assert!(!std::fs::read_dir(tmp).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with("temp-snapshot-")));
    // A subsequent ordinary duplicate-key failure must also restore the table.
    assert_eq!(
        engine
            .execute_with_session(&mut session, "ALTER TABLE quota ADD UNIQUE(id)")
            .unwrap_err()
            .code(),
        1062
    );
    engine
        .execute_with_session(&mut session, "INSERT INTO quota VALUES(1)")
        .unwrap();
}

#[test]
fn temporary_audit_counters_measure_internal_materializations() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    fn counters(engine: &StorageQueryEngine, session: &mut SessionContext) -> Vec<Vec<Value>> {
        result_rows(
            engine
                .execute_with_session(session, "SHOW SESSION STATUS LIKE 'Created_tmp%'")
                .unwrap(),
        )
    }
    let before = counters(&engine, &mut session);
    assert_eq!(before.len(), 3);
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE user_temp(id INT)")
        .unwrap();
    assert_eq!(counters(&engine, &mut session), before);
    crate::engine::expr::with_session_expr_info(&session, || {
        let mut builder = super::super::cte::CteSpillBuilder::new(
            vec![Column {
                name: "id".into(),
                column_type: ColumnType::LongLong,
                ..Column::default()
            }],
            dir.path().to_path_buf(),
        );
        builder.push(vec![Value::Int(1)]).unwrap();
        builder.finish().unwrap();
    });
    let after = counters(&engine, &mut session);
    let total = |rows: &Vec<Vec<Value>>| {
        rows.iter()
            .find(|row| row[0] == Value::String("Created_tmp_tables".into()))
            .unwrap()[1]
            .to_display_string()
            .parse::<u64>()
            .unwrap()
    };
    assert_eq!(total(&after), total(&before) + 1);
}

#[test]
fn temporary_audit_view_locking_and_failed_drop_keep_bindings() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE source(id INT PRIMARY KEY)",
        "INSERT INTO source VALUES(1)",
        "CREATE VIEW fixed_view AS SELECT id FROM source",
        "CREATE TEMPORARY TABLE source(other INT)",
        "INSERT INTO source VALUES(99)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT id FROM fixed_view FOR UPDATE")
                .unwrap()
        ),
        1
    );
    assert_eq!(
        engine
            .execute_with_session(&mut session, "DROP TEMPORARY TABLE source, missing")
            .unwrap_err()
            .code(),
        1051
    );
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT other FROM source")
                .unwrap()
        ),
        99
    );
}

#[test]
fn temporary_audit_session_quota_counts_storage_and_releases_on_truncate_drop() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut owner = SessionContext::default();
    owner.temp_quota = Arc::new(crate::storage::temp_quota::SessionTempQuota::new(
        1,
        32 * 1024,
        1024 * 1024,
    ));
    let mut other = SessionContext::default();
    engine
        .execute_with_session(
            &mut owner,
            "CREATE TEMPORARY TABLE capped(id INT, payload LONGTEXT)",
        )
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut owner, "CREATE TEMPORARY TABLE extra(id INT)")
            .unwrap_err()
            .code(),
        1114
    );
    assert_eq!(owner.temp_quota.usage().0, 1);
    engine
        .execute_with_session(&mut owner, "INSERT INTO capped VALUES(1,'small')")
        .unwrap();
    let before = owner.temp_quota.usage().1;
    assert_eq!(before, 32 * 1024);
    assert_eq!(
        engine
            .execute_with_session(
                &mut owner,
                "INSERT INTO capped VALUES(2,REPEAT('x',100000))"
            )
            .unwrap_err()
            .code(),
        1114
    );
    assert_eq!(
        owner.temp_quota.usage().1,
        before,
        "rejected extent must not consume quota"
    );
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut owner, "SELECT COUNT(*) FROM capped")
                .unwrap()
        ),
        1
    );
    engine
        .execute_with_session(
            &mut other,
            "CREATE TEMPORARY TABLE capped(id INT, payload LONGTEXT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut other,
            "INSERT INTO capped VALUES(1,REPEAT('x',100000))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut owner, "TRUNCATE TABLE capped")
        .unwrap();
    assert_eq!(owner.temp_quota.usage().1, 16 * 1024);
    engine
        .execute_with_session(&mut owner, "INSERT INTO capped VALUES(3,'again')")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "DROP TEMPORARY TABLE capped")
        .unwrap();
    assert_eq!(
        (owner.temp_quota.usage().0, owner.temp_quota.usage().1),
        (0, 0)
    );
    engine
        .execute_with_session(&mut owner, "CREATE TEMPORARY TABLE replacement(id INT)")
        .unwrap();
    engine.close_session(&mut owner).unwrap();
    assert_eq!(
        (owner.temp_quota.usage().0, owner.temp_quota.usage().1),
        (0, 0)
    );
}

#[test]
fn temporary_audit_session_undo_quota_rejection_rolls_back_only_statement() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.temp_quota = Arc::new(crate::storage::temp_quota::SessionTempQuota::new(
        10,
        1024 * 1024,
        1,
    ));
    engine
        .execute_with_session(&mut session, "CREATE TABLE uncapped(id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE undo_capped(id INT)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO uncapped VALUES(42)")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "INSERT INTO undo_capped VALUES(1)")
            .unwrap_err()
            .code(),
        1114
    );
    assert_eq!(session.temp_quota.usage().2, 0);
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM undo_capped")
                .unwrap()
        ),
        0
    );
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT id FROM uncapped")
                .unwrap()
        ),
        42
    );
}

#[test]
fn temporary_audit_session_index_quota_and_snapshot_rollback() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.temp_quota = Arc::new(crate::storage::temp_quota::SessionTempQuota::new(
        10,
        64 * 1024,
        1024 * 1024,
    ));
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE index_capped(id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO index_capped VALUES(1)")
        .unwrap();
    // Snapshot (32KB) + backing heap (32KB) exhausts the budget before a new
    // B-tree can be built. The failed ALTER must restore the original schema.
    assert_eq!(
        engine
            .execute_with_session(&mut session, "ALTER TABLE index_capped ADD UNIQUE(id)")
            .unwrap_err()
            .code(),
        1114
    );
    assert_eq!(session.temp_quota.usage().1, 32 * 1024);
    engine
        .execute_with_session(&mut session, "INSERT INTO index_capped VALUES(1)")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM index_capped")
                .unwrap()
        ),
        2
    );
}

#[test]
fn temporary_audit_failed_alter_restores_buffered_rows_lobs_and_indexes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TEMPORARY TABLE restore_me(id INT PRIMARY KEY, v INT, payload LONGTEXT)",
        "INSERT INTO restore_me VALUES(1,7,REPEAT('a',100000)),(2,7,REPEAT('b',100000))",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        engine
            .execute_with_session(&mut session, "ALTER TABLE restore_me ADD UNIQUE(v)")
            .unwrap_err()
            .code(),
        1062
    );
    let rows = result_rows(
        engine
            .execute_with_session(
                &mut session,
                "SELECT id, v, LENGTH(payload), LEFT(payload,1) FROM restore_me ORDER BY id",
            )
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(7),
                Value::Int(100000),
                Value::String("a".into())
            ],
            vec![
                Value::Int(2),
                Value::Int(7),
                Value::Int(100000),
                Value::String("b".into())
            ]
        ]
    );
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO restore_me VALUES(1,9,'duplicate')"
            )
            .unwrap_err()
            .code(),
        1062
    );
    engine
        .execute_with_session(&mut session, "UPDATE restore_me SET v=8 WHERE id=2")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE restore_me ADD UNIQUE(v)")
        .unwrap();
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT id FROM restore_me WHERE v=8")
                .unwrap()
        ),
        2
    );
}

#[test]
fn temporary_audit_quota_during_index_growth_keeps_previous_commits() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.temp_quota = Arc::new(crate::storage::temp_quota::SessionTempQuota::new(
        10,
        96 * 1024,
        4 * 1024 * 1024,
    ));
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE growing(id INT PRIMARY KEY, v VARCHAR(100), KEY k(v))",
        )
        .unwrap();
    let mut count = 0;
    let mut rejected = false;
    for id in 1..=4000 {
        let sql = format!("INSERT INTO growing VALUES({id},'value-{id:08}')");
        match engine.execute_with_session(&mut session, &sql) {
            Ok(_) => count += 1,
            Err(err) => {
                assert_eq!(err.code(), 1114);
                rejected = true;
                break;
            }
        }
    }
    assert!(rejected);
    assert!(count > 0);
    assert_eq!(
        first_int(
            engine
                .execute_with_session(&mut session, "SELECT COUNT(*) FROM growing")
                .unwrap()
        ),
        count
    );
    assert_eq!(
        first_int(
            engine
                .execute_with_session(
                    &mut session,
                    &format!("SELECT id FROM growing WHERE id={count}")
                )
                .unwrap()
        ),
        count
    );
    assert!(session.temp_quota.usage().1 <= 96 * 1024);
    engine
        .execute_with_session(&mut session, "TRUNCATE TABLE growing")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO growing VALUES(1,'reused')")
        .unwrap();
}
