use super::*;

fn result(engine: &StorageQueryEngine, sql: &str) -> ResultSet {
    let QueryResponse::ResultSet(result) = engine
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    else {
        panic!("expected rows: {sql}");
    };
    result
}

#[test]
fn json_audit_paths_and_typed_members() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        r#"SELECT JSON_EXTRACT('{"a":1}', '$[*]')"#,
        "SELECT JSON_EXTRACT('5', '$[*]')",
        r#"SELECT JSON_EXTRACT('{"a":{"b":2}}', '$**[*]')"#,
        r#"SELECT JSON_SEARCH('{"a":"v"}', 'one', 'v', NULL, '$[*]')"#,
    ] {
        assert_eq!(result(&engine, sql).rows, vec![vec![Value::Null]], "{sql}");
    }
    for (doc, expected) in [
        (r#"{"a":[1,2]}"#, "[1, 2]"),
        (r#"[{"b":2}]"#, r#"[{"b": 2}]"#),
    ] {
        assert_eq!(
            result(&engine, &format!("SELECT JSON_EXTRACT('{doc}', '$**[*]')")).rows[0][0]
                .to_display_string(),
            expected
        );
    }
    assert_eq!(
        result(
            &engine,
            "SELECT JSON_EXTRACT('5', '$[0]'), JSON_EXTRACT('5', '$[0 to 1]')"
        )
        .rows[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        ["5", "[5]"]
    );
    for expr in [
        "TRUE",
        "FALSE",
        "CAST('2020-01-01' AS DATE)",
        "CAST('12:30:00' AS TIME)",
        "CAST('2020-01-01 12:30:00' AS DATETIME)",
        "CAST(1.25 AS DECIMAL(5,2))",
    ] {
        let sql = format!("SELECT ({expr}) MEMBER OF (JSON_ARRAY({expr}))");
        let rs = result(&engine, &sql);
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]], "{sql}");
        assert_eq!(rs.columns[0].column_type, ColumnType::LongLong);
    }
    assert_eq!(
        result(
            &engine,
            "SELECT (TRUE) MEMBER OF (CAST('[1]' AS JSON)), (1) MEMBER OF (CAST('[true]' AS JSON))"
        )
        .rows,
        vec![vec![Value::Int(0), Value::Int(0)]]
    );
}

#[test]
fn json_audit_depth_and_charset() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(j JSON)").unwrap();
    for depth in [100, 101, 127, 128, 150, 200, 1000] {
        let doc = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        for sql in [
            format!("SELECT JSON_VALID('{doc}')"),
            format!("SELECT JSON_VALUE('{doc}', '$')"),
            format!("SELECT JSON_SCHEMA_VALID('{{}}','{doc}')"),
            format!("INSERT INTO t VALUES ('{doc}')"),
            format!("SELECT CAST('{doc}' AS JSON)"),
        ] {
            if depth <= 100 {
                engine.execute(&sql).unwrap();
            } else {
                assert_eq!(engine.execute(&sql).unwrap_err().code(), 3157, "{sql}");
            }
        }
    }
    assert_eq!(
        result(
            &engine,
            "SELECT JSON_VALID('{bad'),JSON_VALID('null'),JSON_VALID('\"[[[[\"')"
        )
        .rows,
        vec![vec![Value::Int(0), Value::Int(1), Value::Int(1)]]
    );
    for sql in [
        "SELECT JSON_TYPE(CAST('5' AS BINARY))",
        "SELECT JSON_VALUE(CAST('5' AS BINARY), '$')",
        "SELECT JSON_EXTRACT(_binary X'FF', '$')",
        "SELECT JSON_UNQUOTE(CAST('\"a\"' AS BINARY))",
        "SELECT JSON_SCHEMA_VALID('{}', CAST('{}' AS BINARY))",
        "INSERT INTO t VALUES (CAST('5' AS BINARY))",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 3144, "{sql}");
    }
}

#[test]
fn json_audit_assignments_and_load() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT, j JSON)").unwrap();
    engine.execute("INSERT INTO t VALUES (1, '5')").unwrap();
    for expr in [
        "'{bad'",
        "''",
        "5",
        "1.5",
        "TRUE",
        "CAST('2020-01-01' AS DATE)",
    ] {
        for sql in [
            format!("INSERT INTO t VALUES (2,{expr})"),
            format!("UPDATE t SET j={expr} WHERE id=1"),
        ] {
            let err = engine.execute(&sql).unwrap_err();
            assert_eq!(err.code(), 3140, "{sql}");
            assert!(err.message().contains("column 'j'"), "{err:?}");
            assert!(!err.message().contains("function json"));
        }
    }
    engine
        .execute("INSERT INTO t VALUES (2, CAST(5 AS JSON)),(3,NULL),(4,JSON_ARRAY(TRUE))")
        .unwrap();
    let path = dir.path().join("invalid.tsv");
    std::fs::write(&path, "5\t{bad\n").unwrap();
    let sql = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t",
        crate::util::file::sql_path(&path)
    );
    assert_eq!(engine.execute(&sql).unwrap_err().code(), 3140);
    assert_eq!(
        result(&engine, "SELECT COUNT(*) FROM t").rows[0][0],
        Value::Int(4)
    );
}

#[test]
fn json_audit_value_clauses() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for target in ["TINYINT", "BLOB", "BOOLEAN", "VARCHAR(2)"] {
        for doc in ["NULL", "'{}'", "'{\"a\":1}'"] {
            let sql = format!("SELECT JSON_VALUE({doc}, '$.a' RETURNING {target})");
            assert_eq!(engine.execute(&sql).unwrap_err().code(), 1064, "{sql}");
        }
    }
    for doc in ["NULL", "'{\"a\":\"x\"}'", "'{}'"] {
        for clause in ["DEFAULT 'yy' ON ERROR", "DEFAULT 'yy' ON EMPTY"] {
            let sql = format!("SELECT JSON_VALUE({doc}, '$.a' RETURNING CHAR(1) {clause})");
            assert_eq!(engine.execute(&sql).unwrap_err().code(), 1690, "{sql}");
        }
    }
    assert_eq!(engine.execute("SELECT JSON_VALUE('{\"a\":\"xx\"}', '$.a' RETURNING CHAR(1) DEFAULT 'yy' ON ERROR)").unwrap_err().code(),1690);
}

#[test]
fn json_audit_cast_functional_indexes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for key in ["INDEX", "UNIQUE KEY"] {
        let sql = format!("CREATE TABLE t(j JSON, {key} idx ((CAST(j AS JSON))))");
        assert_eq!(engine.execute(&sql).unwrap_err().code(), 3753);
    }
    engine.execute("CREATE TABLE t(j JSON)").unwrap();
    for sql in [
        "CREATE INDEX idx ON t ((CAST(j AS JSON)))",
        "ALTER TABLE t ADD INDEX idx ((CAST(j AS JSON)))",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 3753, "{sql}");
    }
    engine
        .execute("CREATE INDEX idx ON t ((CAST(j AS CHAR(20))))")
        .unwrap();
}

#[test]
fn json_audit_row_and_vector_execution_agree() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, d DATE, j JSON)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,'2020-01-01',JSON_ARRAY(CAST('2020-01-01' AS DATE))),(2,'2020-01-01','[\"2020-01-01\"]')").unwrap();
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let _scope = crate::engine::executor::vec::config::enter_mode(mode);
        assert_eq!(
            result(
                &engine,
                "SELECT id FROM t WHERE (d) MEMBER OF(j) ORDER BY id"
            )
            .rows,
            vec![vec![Value::Int(1)]]
        );
        let rs = result(
            &engine,
            "SELECT JSON_VALUE(j, '$[0]' RETURNING CHAR(10)) FROM t WHERE id<0",
        );
        assert_eq!(rs.columns[0].source.length, Some(40));
    }
}
