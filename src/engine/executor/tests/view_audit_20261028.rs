use super::support::execute_materialized as exec;
use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> ResultSet {
    let QueryResponse::ResultSet(result) = exec(engine, session, sql).expect(sql) else {
        panic!("expected rows: {sql}")
    };
    result
}

#[test]
fn view_oct28_logical_columns_and_definition_replay() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,a INT)",
        "ALTER TABLE t ADD COLUMN extra INT",
        "ALTER TABLE t DROP COLUMN extra",
        "ALTER TABLE t ADD COLUMN a_new VARCHAR(17) FIRST",
        "ALTER TABLE t ADD COLUMN hidden INT INVISIBLE",
        "INSERT INTO t(id,a,a_new) VALUES(1,10,'old')",
        "CREATE VIEW v AS SELECT * FROM t",
        "CREATE VIEW q AS SELECT t.* FROM t",
    ] {
        exec(&engine, &mut s, sql).expect(sql);
    }
    for view in ["v", "q"] {
        let fields = rows(&engine, &mut s, &format!("SHOW COLUMNS FROM {view}"));
        assert_eq!(
            fields
                .rows
                .iter()
                .map(|r| r[0].to_display_string())
                .collect::<Vec<_>>(),
            ["a_new", "id", "a"]
        );
        exec(
            &engine,
            &mut s,
            &format!("UPDATE {view} SET a=11 WHERE id=1"),
        )
        .unwrap();
        let ddl = rows(&engine, &mut s, &format!("SHOW CREATE VIEW {view}")).rows[0][1]
            .to_display_string();
        exec(&engine, &mut s, &format!("DROP VIEW {view}")).unwrap();
        exec(&engine, &mut s, &ddl).expect(&ddl);
        assert_eq!(
            rows(&engine, &mut s, &format!("SELECT * FROM {view}")).rows,
            vec![vec![
                Value::String("old".into()),
                Value::Int(1),
                Value::Int(11)
            ]]
        );
    }
    for sql in [
        "ALTER TABLE t ADD COLUMN extra VARCHAR(23) CHARACTER SET latin1",
        "CREATE VIEW reused AS SELECT extra, hidden FROM t",
        "INSERT INTO v VALUES('new',2,20)",
    ] {
        exec(&engine, &mut s, sql).expect(sql);
    }
    let columns = rows(&engine, &mut s, "SHOW FULL COLUMNS FROM reused");
    assert_eq!(columns.rows[0][1].to_display_string(), "varchar(23)");
    assert!(columns.rows[0][2].to_display_string().starts_with("latin1"));
}

#[test]
fn view_oct28_explain_caller_privileges() {
    let (_dir, engine, mut root) = setup();
    for sql in [
        "CREATE TABLE private_t(id INT PRIMARY KEY, secret INT)",
        "CREATE VIEW secured AS SELECT id FROM private_t",
        "CREATE VIEW nested AS SELECT * FROM secured",
        "CREATE USER 'view_oct28'@'%' IDENTIFIED BY 'pw'",
        "GRANT SELECT ON test.secured TO 'view_oct28'@'%'",
        "GRANT SELECT ON test.nested TO 'view_oct28'@'%'",
    ] {
        exec(&engine, &mut root, sql).expect(sql);
    }
    let mut user = SessionContext::default();
    user.authenticated_as = crate::engine::auth::AccountKey::new("view_oct28", "%");
    exec(&engine, &mut user, "SELECT * FROM secured").unwrap();
    for sql in [
        "EXPLAIN SELECT * FROM secured",
        "EXPLAIN FORMAT=JSON SELECT * FROM nested",
        "EXPLAIN SELECT * FROM (SELECT * FROM secured) d",
        "EXPLAIN SELECT 1 WHERE EXISTS(SELECT 1 FROM secured)",
        "EXPLAIN WITH c AS (SELECT * FROM nested) SELECT * FROM c",
    ] {
        let err = exec(&engine, &mut user, sql).expect_err(sql);
        assert_eq!(err.code(), 1345, "{sql}: {err:?}");
        assert!(!err.message().contains("private_t"));
    }
    for sql in [
        "GRANT SHOW VIEW ON test.secured TO 'view_oct28'@'%'",
        "GRANT SHOW VIEW ON test.nested TO 'view_oct28'@'%'",
    ] {
        exec(&engine, &mut root, sql).unwrap();
    }
    assert_eq!(
        exec(&engine, &mut user, "EXPLAIN SELECT * FROM nested")
            .unwrap_err()
            .code(),
        1345
    );
    exec(
        &engine,
        &mut root,
        "GRANT SELECT ON test.private_t TO 'view_oct28'@'%'",
    )
    .unwrap();
    exec(&engine, &mut user, "EXPLAIN SELECT * FROM nested").unwrap();
    let sql = "EXPLAIN SELECT * FROM nested";
    let prepared = engine.prepare_cached(&mut user, sql, 0).unwrap();
    engine
        .execute_prepared_statement(&mut user, sql, (*prepared).clone())
        .unwrap()
        .unwrap()
        .materialize()
        .unwrap();
    exec(
        &engine,
        &mut root,
        "REVOKE SELECT ON test.private_t FROM 'view_oct28'@'%'",
    )
    .unwrap();
    assert_eq!(
        engine
            .execute_prepared_statement(&mut user, sql, (*prepared).clone())
            .unwrap()
            .unwrap_err()
            .code(),
        1345
    );
}

#[test]
fn view_oct28_check_option_subquery_forms_and_postimages() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
        "CREATE TABLE allowed(id INT PRIMARY KEY, max_v INT)",
        "INSERT INTO allowed VALUES(1,50),(2,60)",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    for predicate in [
        "id IN (SELECT id FROM allowed)",
        "EXISTS (SELECT 1 FROM allowed WHERE allowed.id=t.id AND allowed.max_v >= t.v)",
        "v <= (SELECT max_v FROM allowed WHERE allowed.id=t.id)",
        "id = ANY (SELECT id FROM allowed)",
        "v < ALL (SELECT max_v FROM allowed)",
    ] {
        exec(&engine, &mut s, &format!("CREATE OR REPLACE VIEW checked AS SELECT * FROM t WHERE {predicate} WITH CASCADED CHECK OPTION")).expect(predicate);
        exec(&engine, &mut s, "INSERT INTO checked VALUES(1,10)").expect(predicate);
        exec(&engine, &mut s, "UPDATE checked SET v=20 WHERE id=1").expect(predicate);
        exec(
            &engine,
            &mut s,
            "INSERT INTO checked VALUES(1,25) ON DUPLICATE KEY UPDATE v=30",
        )
        .expect(predicate);
        assert_eq!(
            exec(&engine, &mut s, "INSERT INTO checked VALUES(9,999)")
                .expect_err(predicate)
                .code(),
            1369
        );
        exec(&engine, &mut s, "REPLACE INTO checked VALUES(1,15)").expect(predicate);
        exec(&engine, &mut s, "INSERT INTO checked SELECT 2,20").expect(predicate);
        assert_eq!(
            exec(&engine, &mut s, "UPDATE checked SET id=9,v=999 WHERE id=1")
                .expect_err(predicate)
                .code(),
            1369
        );
        exec(&engine, &mut s, "DELETE FROM t").unwrap();
        assert_eq!(
            exec(&engine, &mut s, "INSERT INTO checked VALUES(1,10),(9,999)")
                .expect_err(predicate)
                .code(),
            1369
        );
        assert!(rows(&engine, &mut s, "SELECT * FROM t").rows.is_empty());
        exec(
            &engine,
            &mut s,
            "INSERT IGNORE INTO checked VALUES(1,10),(9,999)",
        )
        .expect(predicate);
        assert_eq!(
            rows(&engine, &mut s, "SELECT id FROM t").rows,
            vec![vec![Value::Int(1)]]
        );
        exec(&engine, &mut s, "DELETE FROM t").unwrap();
    }
}

#[test]
fn view_oct28_join_locking_preserves_base_row_identity() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE TABLE a(id INT PRIMARY KEY,v INT)",
        "CREATE TABLE b(id INT PRIMARY KEY,a_id INT)",
        "INSERT INTO a VALUES(1,10),(2,20)",
        "INSERT INTO b VALUES(1,1)",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    for join in ["JOIN", "LEFT JOIN", "RIGHT JOIN"] {
        exec(&engine,&mut s,&format!("CREATE OR REPLACE VIEW joined AS SELECT a.id aid,a.v,b.id bid FROM a {join} b ON b.a_id=a.id")).unwrap();
        let columns = engine
            .describe_prepared_statement(
                &mut s,
                "SELECT * FROM joined WHERE aid=? FOR UPDATE NOWAIT",
            )
            .unwrap();
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["aid", "v", "bid"]
        );
        let sql = "SELECT * FROM joined WHERE aid=1 FOR UPDATE NOWAIT";
        let prepared = engine.prepare_cached(&mut s, sql, 0).unwrap();
        let result = engine
            .execute_prepared_statement(&mut s, sql, (*prepared).clone())
            .unwrap()
            .unwrap()
            .materialize()
            .unwrap();
        assert!(matches!(result, QueryResponse::ResultSet(_)));
        for mode in ["FOR UPDATE", "FOR SHARE", "LOCK IN SHARE MODE"] {
            exec(&engine, &mut s, "BEGIN").unwrap();
            let result = rows(
                &engine,
                &mut s,
                &format!("SELECT * FROM joined WHERE aid=1 ORDER BY aid LIMIT 1 {mode}"),
            );
            assert_eq!(
                result.rows,
                vec![vec![Value::Int(1), Value::Int(10), Value::Int(1)]]
            );
            let mut other = SessionContext::default();
            exec(&engine, &mut other, "BEGIN").unwrap();
            for table in ["a", "b"] {
                assert_eq!(
                    exec(
                        &engine,
                        &mut other,
                        &format!("SELECT * FROM {table} WHERE id=1 FOR UPDATE NOWAIT")
                    )
                    .unwrap_err()
                    .code(),
                    3572
                );
            }
            exec(
                &engine,
                &mut other,
                "SELECT * FROM a WHERE id=2 FOR UPDATE NOWAIT",
            )
            .unwrap();
            assert_eq!(
                exec(
                    &engine,
                    &mut other,
                    "SELECT * FROM joined WHERE aid=1 FOR UPDATE NOWAIT"
                )
                .unwrap_err()
                .code(),
                3572
            );
            assert!(rows(
                &engine,
                &mut other,
                "SELECT * FROM joined WHERE aid=1 FOR UPDATE SKIP LOCKED"
            )
            .rows
            .is_empty());
            exec(&engine, &mut other, "ROLLBACK").unwrap();
            exec(&engine, &mut s, "ROLLBACK").unwrap();
        }
        if join == "LEFT JOIN" {
            exec(&engine, &mut s, "BEGIN").unwrap();
            assert_eq!(
                rows(
                    &engine,
                    &mut s,
                    "SELECT * FROM joined WHERE aid=2 FOR UPDATE"
                )
                .rows,
                vec![vec![Value::Int(2), Value::Int(20), Value::Null]]
            );
            let mut other = SessionContext::default();
            exec(&engine, &mut other, "BEGIN").unwrap();
            assert_eq!(
                exec(
                    &engine,
                    &mut other,
                    "SELECT * FROM a WHERE id=2 FOR UPDATE NOWAIT"
                )
                .unwrap_err()
                .code(),
                3572
            );
            exec(
                &engine,
                &mut other,
                "SELECT * FROM b WHERE id=1 FOR UPDATE NOWAIT",
            )
            .unwrap();
            exec(&engine, &mut other, "ROLLBACK").unwrap();
            exec(&engine, &mut s, "ROLLBACK").unwrap();
        }
    }
}

#[test]
fn view_oct28_check_option_aliases_nested_names_and_errors() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
        "CREATE TABLE allowed(id INT,max_v INT)",
        "INSERT INTO allowed VALUES(1,50),(2,60)",
        "CREATE VIEW checked(k,n) AS SELECT x.id,x.v FROM t x WHERE EXISTS(SELECT 1 FROM allowed t WHERE t.id=x.id AND t.max_v>=x.v) WITH CASCADED CHECK OPTION",
        "CREATE VIEW nested AS SELECT k,n FROM checked WITH CASCADED CHECK OPTION",
        "INSERT INTO nested VALUES(1,10),(2,20)",
        "UPDATE nested AS y SET n=30 WHERE k=1",
    ] { exec(&engine,&mut s,sql).expect(sql); }
    assert_eq!(
        rows(&engine, &mut s, "SELECT * FROM nested FOR UPDATE")
            .rows
            .len(),
        2
    );
    for sql in [
        "INSERT INTO nested VALUES(9,10)",
        "INSERT INTO nested VALUES(1,20) ON DUPLICATE KEY UPDATE n=999",
        "UPDATE nested SET n=999 WHERE k=1",
    ] {
        assert_eq!(exec(&engine, &mut s, sql).expect_err(sql).code(), 1369);
    }
    assert_eq!(
        rows(&engine, &mut s, "SELECT * FROM t ORDER BY id").rows,
        vec![
            vec![Value::Int(1), Value::Int(30)],
            vec![Value::Int(2), Value::Int(20)]
        ]
    );
    // Preserve scalar-subquery errors instead of disguising them as CHECK failures.
    exec(&engine,&mut s,"CREATE VIEW scalar_v AS SELECT * FROM t WHERE v<(SELECT max_v FROM allowed) WITH CHECK OPTION").unwrap();
    assert_eq!(
        exec(&engine, &mut s, "INSERT INTO scalar_v VALUES(3,5)")
            .unwrap_err()
            .code(),
        1242
    );
    exec(&engine, &mut s, "DELETE FROM allowed").unwrap();
    exec(&engine,&mut s,"CREATE VIEW null_v AS SELECT * FROM t WHERE v<(SELECT max_v FROM allowed) WITH CHECK OPTION").unwrap();
    assert_eq!(
        exec(&engine, &mut s, "INSERT INTO null_v VALUES(3,5)")
            .unwrap_err()
            .code(),
        1369
    );
}
