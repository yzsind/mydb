//! Regression coverage for the 2026-09-28 CTAS/INSERT SELECT audit.
use super::*;

fn check(setup: &[&str], sql: &str, expected_error: Option<u16>) {
    let dir = tempfile::Builder::new()
        .prefix("audit-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for statement in setup {
        engine
            .execute_with_session(&mut session, statement)
            .unwrap();
    }
    let result = engine.execute_with_session(&mut session, sql);
    match expected_error {
        Some(code) => assert_eq!(result.unwrap_err().code(), code, "{sql}"),
        None => {
            result.unwrap_or_else(|err| panic!("{sql}: {err:?}"));
        }
    }
}

#[test]
fn audit_ctas_marker_literals_are_inert() {
    for suffix in [
        "WHERE '__MYDB_CTAS_REPLACE__' IS NOT NULL",
        "/* __MYDB_CTAS_IGNORE__ */",
    ] {
        check(
            &["CREATE TABLE s(k INT)", "INSERT INTO s VALUES (1),(1)"],
            &format!("CREATE TABLE t(k INT PRIMARY KEY) AS SELECT k FROM s {suffix}"),
            Some(1062),
        );
    }
}
#[test]
fn audit_ctas_keyword_names_and_parentheses() {
    for sql in [
        "CREATE TABLE ignore AS SELECT 1 v",
        "CREATE TABLE replace AS SELECT 1 v",
        "CREATE TEMPORARY TABLE tmp (SELECT 1 v)",
        "CREATE TABLE `test`.`t` (SELECT 1 v)",
    ] {
        check(&[], sql, None);
    }
}
#[test]
fn audit_ctas_ignore_without_as() {
    check(
        &["CREATE TABLE s(k INT)", "INSERT INTO s VALUES (1),(1)"],
        "CREATE TABLE t(k INT PRIMARY KEY) IGNORE SELECT k FROM s",
        None,
    );
}
#[test]
fn audit_insert_hidden_and_dropped_columns() {
    for alter in [
        "ALTER TABLE t ADD COLUMN c INT INVISIBLE DEFAULT 1",
        "ALTER TABLE t ADD COLUMN c INT",
        "ALTER TABLE t DROP COLUMN b",
    ] {
        let insert = if alter.contains("DROP") {
            "INSERT INTO t SELECT 1"
        } else if alter.contains("INVISIBLE") {
            "INSERT INTO t SELECT 1,2"
        } else {
            "INSERT INTO t SELECT 1,2,3"
        };
        check(&["CREATE TABLE t(a INT,b INT)", alter], insert, None);
    }
}
#[test]
fn audit_trigger_missing_default_strict() {
    check(
        &[
            "CREATE TABLE t(a INT,b INT NOT NULL)",
            "CREATE TABLE log(a INT)",
            "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(NEW.a)",
        ],
        "INSERT INTO t(a) SELECT 1",
        Some(1364),
    );
}
#[test]
fn audit_trigger_missing_default_nonstrict() {
    check(
        &[
            "CREATE TABLE t(a INT,b INT NOT NULL)",
            "CREATE TABLE log(a INT)",
            "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(NEW.a)",
            "SET sql_mode=''",
        ],
        "INSERT INTO t(a) SELECT 1",
        None,
    );
}
#[test]
fn audit_ctas_missing_default() {
    check(
        &[],
        "CREATE TABLE t(a INT,b INT NOT NULL) AS SELECT 1 a",
        Some(1364),
    );
}
#[test]
fn audit_partition_column_order_and_ignore() {
    let setup = ["CREATE TABLE t(a INT,b INT) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN(100))"];
    check(&setup, "INSERT INTO t PARTITION(p0) (a,b) SELECT 1,2", None);
    check(
        &setup,
        "INSERT IGNORE INTO t PARTITION(p0) SELECT 10,2",
        None,
    );
    check(
        &setup,
        "INSERT IGNORE INTO t PARTITION(p0) VALUES(10,2)",
        None,
    );
}
#[test]
fn audit_insert_select_numeric_error() {
    check(
        &[
            "CREATE TABLE t(a INT)",
            "CREATE TABLE s(v VARCHAR(10))",
            "INSERT INTO s VALUES('abc')",
        ],
        "INSERT INTO t SELECT v+0 FROM s",
        Some(1292),
    );
}
#[test]
fn audit_ctas_cte_union_numeric_error() {
    for query in [
        "WITH c AS(SELECT i FROM s) SELECT i FROM c WHERE i='abc'",
        "SELECT i FROM s WHERE i='abc' UNION SELECT i FROM s WHERE i='abc'",
    ] {
        check(
            &["CREATE TABLE s(i INT)", "INSERT INTO s VALUES(0)"],
            &format!("CREATE TABLE t AS {query}"),
            Some(1292),
        );
    }
}
#[test]
fn audit_odku_source_column() {
    check(
        &[
            "CREATE TABLE t(k INT PRIMARY KEY,v INT)",
            "INSERT INTO t VALUES(1,1)",
            "CREATE TABLE s(k INT,nv INT)",
            "INSERT INTO s VALUES(1,100)",
        ],
        "INSERT INTO t(k,v) SELECT k,nv FROM s ON DUPLICATE KEY UPDATE v=nv",
        None,
    );
}

fn result_rows(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Vec<Vec<Value>> {
    let result = engine
        .execute_with_session(session, sql)
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("{sql}: {result:?}")
    };
    result.rows
}

#[test]
fn audit_ctas_affected_and_real_table_names() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for name in ["ignore", "replace"] {
        let result = engine
            .execute_with_session(
                &mut session,
                &format!("CREATE TABLE {name} AS SELECT 1 v UNION ALL SELECT 2"),
            )
            .unwrap();
        assert!(matches!(
            result,
            QueryResponse::OkAffected { affected_rows: 2 }
        ));
        assert_eq!(
            result_rows(
                &engine,
                &mut session,
                &format!("SELECT v FROM `{name}` ORDER BY v")
            ),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
    }
}

#[test]
fn audit_parallel_unique_cross_chunk_rollback() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in ["CREATE TABLE s(k INT)", "CREATE TABLE t(k INT PRIMARY KEY)"] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let values = (0..257)
        .chain(std::iter::once(12))
        .map(|n| format!("({n})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute_with_session(&mut session, &format!("INSERT INTO s VALUES{values}"))
        .unwrap();
    for _ in 0..8 {
        assert_eq!(
            engine
                .execute_with_session(&mut session, "INSERT INTO t SELECT k FROM s")
                .unwrap_err()
                .code(),
            1062
        );
        assert_eq!(
            result_rows(&engine, &mut session, "SELECT COUNT(*) FROM t")[0][0].to_display_string(),
            "0"
        );
    }
}

#[test]
fn audit_odku_source_values_and_types() {
    for update in ["nv", "s.nv", "nv+1", "x", "s.extra"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let mut session = SessionContext::default();
        for sql in [
            "CREATE TABLE t(k INT PRIMARY KEY,v INT)",
            "INSERT INTO t VALUES(1,1)",
            "CREATE TABLE s(k INT,nv INT,extra INT)",
            "INSERT INTO s VALUES(1,100,200)",
        ] {
            engine.execute_with_session(&mut session, sql).unwrap();
        }
        let projection = if update == "x" { "k,nv AS x" } else { "k,nv" };
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "INSERT INTO t SELECT {projection} FROM s ON DUPLICATE KEY UPDATE v={update}"
                ),
            )
            .unwrap();
        let expected = if update == "s.extra" {
            "200"
        } else if update == "nv+1" {
            "101"
        } else {
            "100"
        };
        assert_eq!(
            result_rows(&engine, &mut session, "SELECT v FROM t")[0][0].to_display_string(),
            expected
        );
    }
}

#[test]
fn audit_ctas_self_join_nullability() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in ["CREATE TABLE t(id INT NOT NULL PRIMARY KEY)", "INSERT INTO t VALUES(1),(2)",
        "CREATE TABLE dst AS SELECT a.id AS a,c.id AS c FROM t a JOIN t b ON b.id=a.id LEFT JOIN t c ON c.id=a.id+100"] { engine.execute_with_session(&mut session,sql).unwrap(); }
    let rows = result_rows(&engine, &mut session, "SHOW COLUMNS FROM dst");
    assert_eq!(rows[0][2].to_display_string(), "NO");
    assert_eq!(rows[1][2].to_display_string(), "YES");
}

#[test]
fn audit_ctas_injected_head_comment_is_inert() {
    check(&["CREATE TABLE s(k INT)","INSERT INTO s VALUES(1),(1)"],
        "/*__MYDB_CTAS_IGNORE__*/ CREATE TABLE t(k INT PRIMARY KEY) -- REPLACE\n AS SELECT k FROM s",Some(1062));
}

#[test]
fn audit_union_lock_is_preserved_in_ast() {
    use crate::parser::{parse_statement, Statement};
    let Statement::SetOperation(set) =
        parse_statement("SELECT * FROM a UNION SELECT * FROM b FOR UPDATE").unwrap()
    else {
        panic!("set operation expected")
    };
    for branch in [&set.left, &set.right] {
        let crate::parser::QueryExpr::Select(select) = branch else {
            panic!("select expected")
        };
        assert!(select.for_update);
        assert!(select.lock_clause.is_some());
    }
}

#[test]
fn audit_before_trigger_supplies_omitted_required_column() {
    check(
        &[
            "CREATE TABLE t(a INT,b INT NOT NULL)",
            "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.b=7",
        ],
        "INSERT INTO t(a) SELECT 1",
        None,
    );
}

#[test]
fn audit_trigger_ignore_info_survives_nested_statements() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(k INT PRIMARY KEY)",
        "INSERT INTO t VALUES(1)",
        "CREATE TABLE log(k INT)",
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO log VALUES(NEW.k)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    engine
        .execute_with_session(
            &mut session,
            "INSERT IGNORE INTO t SELECT 1 UNION ALL SELECT 2",
        )
        .unwrap();
    assert!(crate::engine::api::peek_statement_info().contains("Records: 2 Duplicates: 1"));
}

#[test]
fn audit_ctas_vector_warning_survives_source_execution() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(v VECTOR(3) NOT NULL)",
        "CREATE INDEX vi ON t(v) USING HNSW",
        "CREATE TABLE dst AS SELECT v FROM t",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let warnings = result_rows(&engine, &mut session, "SHOW WARNINGS");
    assert!(
        warnings.iter().any(|r| r[1].to_display_string() == "1287"
            && r[2].to_display_string().contains("vector indexes")),
        "{warnings:?}"
    );
}

#[test]
fn audit_insert_null_repair_respects_logical_column_order() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT,b INT NOT NULL)",
        "ALTER TABLE t ADD COLUMN c INT NOT NULL FIRST",
        "SET sql_mode=''",
        "INSERT INTO t SELECT NULL,5,6",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        result_rows(&engine, &mut session, "SELECT c,a,b FROM t"),
        vec![vec![Value::Int(0), Value::Int(5), Value::Int(6)]]
    );
}

#[test]
fn audit_union_and_insert_source_lock_blocks_other_session() {
    for query in [
        "SELECT id FROM t WHERE id=1 UNION SELECT id FROM t WHERE id=2 FOR UPDATE",
        "INSERT INTO dst SELECT id FROM t WHERE id=1 UNION SELECT id FROM t WHERE id=2 FOR UPDATE",
        "INSERT INTO dst SELECT id FROM t FOR UPDATE",
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let mut a = SessionContext::default();
        a.connection_id = 901;
        let mut b = SessionContext::default();
        b.connection_id = 902;
        for sql in [
            "CREATE TABLE t(id INT PRIMARY KEY)",
            "INSERT INTO t VALUES(1),(2)",
            "CREATE TABLE dst(id INT)",
            "BEGIN",
        ] {
            engine.execute_with_session(&mut a, sql).unwrap();
        }
        engine
            .execute_with_session(&mut a, query)
            .unwrap()
            .materialize()
            .unwrap();
        engine.execute_with_session(&mut b, "BEGIN").unwrap();
        let err = engine
            .execute_with_session(&mut b, "SELECT id FROM t WHERE id=1 FOR UPDATE NOWAIT")
            .unwrap_err();
        assert_eq!(err.code(), 3572, "{query}: {err:?}");
        engine.execute_with_session(&mut a, "ROLLBACK").unwrap();
        engine.execute_with_session(&mut b, "ROLLBACK").unwrap();
    }
}

#[test]
fn audit_ctas_info_counts_copied_type_warnings() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE s(v FLOAT(5,2))",
        "INSERT INTO s VALUES(1)",
        "CREATE TABLE t AS SELECT v FROM s",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert!(crate::engine::api::peek_statement_info().contains("Warnings: 1"));
    assert_eq!(result_rows(&engine, &mut session, "SHOW WARNINGS").len(), 1);
}

#[test]
fn audit_odku_reuses_volatile_source_alias_value() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(k INT PRIMARY KEY,v DOUBLE)",
        "INSERT INTO t VALUES(1,1)",
        "INSERT INTO t SELECT 1,RAND() AS nv ON DUPLICATE KEY UPDATE v=nv-VALUES(v)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        result_rows(&engine, &mut session, "SELECT v FROM t")[0][0].to_display_string(),
        "0"
    );
}

#[test]
fn audit_cross_chunk_composite_unique_collation_and_nulls() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE s(k VARCHAR(20),n INT)",
        "CREATE TABLE t(k VARCHAR(20) COLLATE utf8mb4_general_ci,n INT,UNIQUE KEY uk(k,n))",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let values = (0..130)
        .map(|i| format!("('k{i}',{i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute_with_session(
            &mut session,
            &format!("INSERT INTO s VALUES {values},('K12',12)"),
        )
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "INSERT INTO t SELECT * FROM s")
            .unwrap_err()
            .code(),
        1062
    );
    assert_eq!(
        result_rows(&engine, &mut session, "SELECT COUNT(*) FROM t")[0][0].to_display_string(),
        "0"
    );
    engine
        .execute_with_session(&mut session, "INSERT INTO t SELECT NULL,n FROM s")
        .unwrap();
    assert_eq!(
        result_rows(&engine, &mut session, "SELECT COUNT(*) FROM t")[0][0].to_display_string(),
        "131"
    );
}

#[test]
fn audit_odku_source_keeps_precoercion_type() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(k INT PRIMARY KEY,v INT,raw_v VARCHAR(20))",
        "INSERT INTO t VALUES(1,7,NULL)",
        "CREATE TABLE s(k INT,nv VARCHAR(20))",
        "INSERT INTO s VALUES(1,'1.9')",
        "INSERT INTO t(k,v) SELECT k,nv FROM s ON DUPLICATE KEY UPDATE raw_v=nv",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        result_rows(&engine, &mut session, "SELECT raw_v FROM t")[0][0].to_display_string(),
        "1.9"
    );
}
