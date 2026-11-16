use super::*;

fn exec(
    e: &StorageQueryEngine,
    s: &mut SessionContext,
    sql: &str,
) -> Result<QueryResponse, QueryError> {
    <StorageQueryEngine as QueryEngine>::execute(e, s, sql).and_then(QueryResponse::materialize)
}
fn setup() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_strict_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,a INT)",
        "INSERT INTO t VALUES(1,10),(2,20)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    (dir, e, s)
}
fn rows(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rs) = exec(e, s, sql).expect(sql) else {
        panic!()
    };
    rs.rows
}
#[test]
fn view_0929_trim_roundtrip() {
    let (_d, e, mut s) = setup();
    exec(&e,&mut s,"CREATE VIEW v AS SELECT TRIM(BOTH 'x' FROM 'xxaxx') AS b,TRIM(LEADING 'x' FROM 'xxaxx') AS l,TRIM(TRAILING 'x' FROM 'xxaxx') AS r,TRIM(BOTH 'x' FROM NULL) AS n").unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM v"),
        vec![vec![
            Value::String("a".into()),
            Value::String("axx".into()),
            Value::String("xxa".into()),
            Value::Null
        ]]
    );
    let ddl = rows(&e, &mut s, "SHOW CREATE VIEW v")[0][1]
        .to_display_string()
        .replace("VIEW `v`", "VIEW `copy`");
    exec(&e, &mut s, &ddl).unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM v"),
        rows(&e, &mut s, "SELECT * FROM copy")
    );
}
#[test]
fn view_0929_insert_lifecycle() {
    let (d, e, mut s) = setup();
    for sql in [
        "CREATE TABLE dest(id INT PRIMARY KEY,a INT)",
        "CREATE VIEW v AS SELECT id,a FROM t",
        "CREATE OR REPLACE VIEW v AS SELECT id,a+1000 AS a FROM t",
        "DROP VIEW v",
        "CREATE VIEW v AS SELECT id,a FROM t",
        "INSERT INTO dest SELECT id,a FROM v WHERE id=2",
    ] {
        exec(&e, &mut s, sql).expect(sql);
    }
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM dest"),
        vec![vec![Value::Int(2), Value::Int(20)]]
    );
    e.checkpoint().unwrap();
    drop(e);
    let e = open_strict_test_engine(d.path());
    let mut s = SessionContext::default();
    exec(&e, &mut s, "INSERT INTO dest SELECT id,a FROM v WHERE id=1").unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM dest ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)]
        ]
    );
}
#[test]
fn view_0929_invisible_metadata_alter() {
    let (_d, e, mut s) = setup();
    for sql in [
        "ALTER TABLE t ADD COLUMN h INT DEFAULT 7 INVISIBLE",
        "CREATE VIEW v AS SELECT * FROM t",
        "CREATE VIEW q AS SELECT t.* FROM t",
        "CREATE VIEW explicit_h AS SELECT h FROM t",
    ] {
        exec(&e, &mut s, sql).expect(sql);
    }
    for name in ["v", "q"] {
        let v = e.database().view_definition("test", name).unwrap();
        assert_eq!(
            v.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "a"]
        );
    }
    for sql in [
        "ALTER TABLE t ADD COLUMN z INT DEFAULT 99",
        "ALTER TABLE t MODIFY COLUMN a BIGINT",
        "ALTER TABLE t DROP COLUMN z",
    ] {
        exec(&e, &mut s, sql).unwrap();
        assert_eq!(
            rows(&e, &mut s, "SELECT * FROM v ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(20)]
            ]
        );
    }
    assert_eq!(
        rows(&e, &mut s, "SELECT h FROM explicit_h"),
        vec![vec![Value::Int(7)], vec![Value::Int(7)]]
    );
}
#[test]
fn view_0929_drop_atomic() {
    let (_d, e, mut s) = setup();
    for sql in [
        "CREATE VIEW v1 AS SELECT 1 AS n",
        "CREATE VIEW v2 AS SELECT 2 AS n",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    for sql in ["DROP VIEW v1,v2,missing", "DROP VIEW IF EXISTS v1,v2,t"] {
        assert!(exec(&e, &mut s, sql).is_err());
        assert_eq!(
            rows(&e, &mut s, "SELECT * FROM v1"),
            vec![vec![Value::Int(1)]]
        );
        assert_eq!(
            rows(&e, &mut s, "SELECT * FROM v2"),
            vec![vec![Value::Int(2)]]
        );
    }
    exec(&e, &mut s, "DROP VIEW IF EXISTS v1,v2,missing").unwrap();
    assert!(e.database().view_definition("test", "v1").is_none());
}
#[test]
fn view_0929_routine_recovery() {
    let (_d, e, mut s) = setup();
    for sql in [
        "CREATE FUNCTION fn(x INT) RETURNS INT DETERMINISTIC RETURN x*2",
        "CREATE VIEW v AS SELECT id,fn(id) AS n FROM t",
        "CREATE VIEW nested AS SELECT * FROM v",
        "DROP FUNCTION fn",
    ] {
        exec(&e, &mut s, sql).expect(sql);
    }
    assert_eq!(
        exec(&e, &mut s, "SELECT * FROM v").unwrap_err().code(),
        1356
    );
    exec(
        &e,
        &mut s,
        "CREATE FUNCTION fn(x INT) RETURNS INT DETERMINISTIC RETURN x*3",
    )
    .unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM nested ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(3)],
            vec![Value::Int(2), Value::Int(6)]
        ]
    );
}
#[test]
fn view_0929_join_update() {
    let (_d, e, mut s) = setup();
    for sql in [
        "CREATE TABLE u(id INT PRIMARY KEY,b INT)",
        "INSERT INTO u VALUES(1,7),(2,8)",
        "CREATE VIEW j AS SELECT t.id AS tid,t.a,u.id AS uid,u.b FROM t JOIN u ON t.id=u.id",
        "UPDATE j SET a=b+35 WHERE tid=1",
    ] {
        exec(&e, &mut s, sql).expect(sql);
    }
    assert_eq!(
        rows(&e, &mut s, "SELECT a FROM t ORDER BY id"),
        vec![vec![Value::Int(42)], vec![Value::Int(20)]]
    );
    assert!(
        e.database()
            .view_definition("test", "j")
            .unwrap()
            .is_updatable
    );
    exec(&e, &mut s, "CREATE VIEW nested_j AS SELECT * FROM j").unwrap();
    assert_eq!(
        exec(
            &e,
            &mut s,
            "CREATE VIEW nested_check AS SELECT * FROM nested_j WITH CHECK OPTION"
        )
        .unwrap_err()
        .code(),
        1235
    );
    assert_eq!(exec(&e, &mut s, "CREATE VIEW checked AS SELECT t.id AS k,t.a,u.b FROM t JOIN u ON t.id=u.id WITH CHECK OPTION").unwrap_err().code(), 1235);
    assert_eq!(
        exec(&e, &mut s, "UPDATE j SET a=1,b=2").unwrap_err().code(),
        1393
    );
    assert_eq!(
        exec(&e, &mut s, "DELETE FROM j WHERE tid=1")
            .unwrap_err()
            .code(),
        1395
    );
}
#[test]
fn view_0929_check_option_validation() {
    let (_d, e, mut s) = setup();
    for sql in [
        "CREATE VIEW v AS SELECT id,COUNT(*) AS n FROM t GROUP BY id WITH CHECK OPTION",
        "CREATE VIEW v AS SELECT DISTINCT id FROM t WITH LOCAL CHECK OPTION",
        "CREATE ALGORITHM=TEMPTABLE VIEW v AS SELECT id FROM t WITH CHECK OPTION",
    ] {
        assert_eq!(exec(&e, &mut s, sql).unwrap_err().code(), 1368, "{sql}");
        assert!(e.database().view_definition("test", "v").is_none());
    }
}

#[test]
fn view_0929_join_predicates_duplicates_and_rollback() {
    let (_d, e, mut s) = setup();
    for sql in [
        "CREATE TABLE u(uid INT PRIMARY KEY,tid INT,b INT)",
        "INSERT INTO u VALUES(10,1,5),(11,1,5),(12,2,7)",
        "CREATE VIEW j(k,n,b) AS SELECT t.id,t.a,u.b FROM t JOIN u ON t.id=u.tid WHERE u.b=5",
        "BEGIN",
        "UPDATE j AS x SET x.n=x.n+x.b WHERE x.k=1",
    ] {
        exec(&e, &mut s, sql).expect(sql);
    }
    assert_eq!(
        rows(&e, &mut s, "SELECT a FROM t ORDER BY id"),
        vec![vec![Value::Int(15)], vec![Value::Int(20)]]
    );
    exec(&e, &mut s, "ROLLBACK").unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT a FROM t ORDER BY id"),
        vec![vec![Value::Int(10)], vec![Value::Int(20)]]
    );
    exec(&e, &mut s, "UPDATE j SET b=9 WHERE k=1").unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT b FROM u ORDER BY uid"),
        vec![
            vec![Value::Int(9)],
            vec![Value::Int(9)],
            vec![Value::Int(7)]
        ]
    );
    for sql in [
        "UPDATE j SET a=4",
        "UPDATE j SET n=tid",
        "UPDATE j SET n=t.a",
    ] {
        assert_eq!(exec(&e, &mut s, sql).unwrap_err().code(), 1054, "{sql}");
    }
}

#[test]
fn view_0929_join_security() {
    let (_d, e, mut root) = setup();
    for sql in [
        "CREATE TABLE u(id INT PRIMARY KEY,b INT)",
        "INSERT INTO u VALUES(1,7)",
        "CREATE USER 'vd'@'%' IDENTIFIED BY 'pw'",
        "CREATE USER 'vu'@'%' IDENTIFIED BY 'pw'",
        "GRANT SELECT,UPDATE ON test.t TO 'vd'@'%'",
        "GRANT SELECT ON test.u TO 'vd'@'%'",
        "CREATE DEFINER='vd'@'%' VIEW j AS SELECT t.id AS k,t.a,u.b FROM t JOIN u ON t.id=u.id",
        "GRANT SELECT,UPDATE ON test.j TO 'vu'@'%'",
    ] {
        exec(&e, &mut root, sql).expect(sql);
    }
    let mut user = SessionContext::default();
    user.authenticated_as = crate::engine::auth::AccountKey::new("vu", "%");
    exec(&e, &mut user, "UPDATE j SET a=b+1 WHERE k=1").unwrap();
    assert_eq!(
        rows(&e, &mut root, "SELECT a FROM t WHERE id=1"),
        vec![vec![Value::Int(8)]]
    );
    assert_eq!(
        exec(&e, &mut user, "UPDATE j SET b=99 WHERE k=1")
            .unwrap_err()
            .code(),
        1356
    );
    exec(&e, &mut root, "REVOKE UPDATE ON test.t FROM 'vd'@'%'").unwrap();
    assert_eq!(
        exec(&e, &mut user, "UPDATE j SET a=99 WHERE k=1")
            .unwrap_err()
            .code(),
        1356
    );
    assert_eq!(
        rows(&e, &mut root, "SELECT a FROM t WHERE id=1"),
        vec![vec![Value::Int(8)]]
    );
}

#[test]
fn view_0929_routine_multiple_dependencies_reopen() {
    let (d, e, mut s) = setup();
    for sql in [
        "CREATE DATABASE other",
        "CREATE FUNCTION f1(x INT) RETURNS INT DETERMINISTIC RETURN x*2",
        "CREATE FUNCTION f2(x INT) RETURNS INT DETERMINISTIC RETURN x*3",
        "CREATE VIEW other.v AS SELECT test.f1(id)+test.f2(id) AS n FROM test.t",
        "CREATE VIEW other.nested AS SELECT * FROM other.v",
        "DROP FUNCTION f1",
        "DROP FUNCTION f2",
        "CREATE FUNCTION f1(x INT) RETURNS INT DETERMINISTIC RETURN x*4",
    ] {
        exec(&e, &mut s, sql).expect(sql);
    }
    assert_eq!(
        exec(&e, &mut s, "SELECT * FROM other.v")
            .unwrap_err()
            .code(),
        1356
    );
    e.checkpoint().unwrap();
    drop(e);
    let e = open_strict_test_engine(d.path());
    let mut s = SessionContext::default();
    exec(
        &e,
        &mut s,
        "CREATE FUNCTION f2(x INT) RETURNS INT DETERMINISTIC RETURN x*5",
    )
    .unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM other.nested ORDER BY n"),
        vec![vec![Value::Int(9)], vec![Value::Int(18)]]
    );
}

#[test]
fn view_0929_drop_cross_schema_reopen() {
    let (d, e, mut s) = setup();
    for sql in [
        "CREATE DATABASE other",
        "CREATE VIEW v AS SELECT id FROM t",
        "CREATE VIEW other.v AS SELECT * FROM test.v",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert_eq!(
        exec(&e, &mut s, "DROP VIEW v,other.v,other.absent")
            .unwrap_err()
            .code(),
        1051
    );
    e.checkpoint().unwrap();
    drop(e);
    let e = open_strict_test_engine(d.path());
    let mut s = SessionContext::default();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM other.v ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    assert_eq!(exec(&e, &mut s, "DROP VIEW v,v").unwrap_err().code(), 1066);
    assert!(e.database().view_definition("test", "v").is_some());
    exec(&e, &mut s, "DROP VIEW v").unwrap();
    assert_eq!(
        exec(&e, &mut s, "SELECT * FROM other.v")
            .unwrap_err()
            .code(),
        1356
    );
}

#[test]
fn view_0929_wildcard_join_metadata() {
    let (_d, e, mut s) = setup();
    for sql in [
        "ALTER TABLE t ADD COLUMN h INT DEFAULT 7 INVISIBLE",
        "CREATE TABLE u(uid INT PRIMARY KEY,b VARCHAR(12),hidden INT INVISIBLE)",
        "INSERT INTO u(uid,b) VALUES(1,'x')",
        "CREATE VIEW j AS SELECT t.*,u.* FROM t JOIN u ON t.id=u.uid",
        "CREATE VIEW right_v AS SELECT u.* FROM t JOIN u ON t.id=u.uid",
    ] {
        exec(&e, &mut s, sql).expect(sql);
    }
    for (name, expected) in [
        ("j", vec!["id", "a", "uid", "b"]),
        ("right_v", vec!["uid", "b"]),
    ] {
        let v = e.database().view_definition("test", name).unwrap();
        assert_eq!(
            v.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        let ddl = rows(&e, &mut s, &format!("SHOW CREATE VIEW {name}"))[0][1]
            .to_display_string()
            .replace(&format!("VIEW `{name}`"), &format!("VIEW `{name}_copy`"));
        exec(&e, &mut s, &ddl).expect(&ddl);
        assert_eq!(
            rows(&e, &mut s, &format!("SELECT * FROM {name}")),
            rows(&e, &mut s, &format!("SELECT * FROM {name}_copy"))
        );
    }
}

#[test]
fn view_0929_check_alter_preserves_definition() {
    let (_d, e, mut s) = setup();
    exec(&e, &mut s, "CREATE VIEW v AS SELECT id FROM t").unwrap();
    for verb in ["ALTER VIEW", "CREATE OR REPLACE VIEW"] {
        let sql = format!("{verb} v AS SELECT COUNT(*) AS n FROM t WITH CHECK OPTION");
        assert_eq!(exec(&e, &mut s, &sql).unwrap_err().code(), 1368);
        assert_eq!(
            rows(&e, &mut s, "SELECT * FROM v ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
    }
}

#[test]
fn view_0929_insert_prepare_source_security() {
    let (_d, e, mut root) = setup();
    for sql in [
        "CREATE TABLE dest(id INT,a INT)",
        "CREATE VIEW v AS SELECT id,a FROM t",
        "CREATE USER 'vu'@'%' IDENTIFIED BY 'pw'",
        "GRANT SELECT ON test.v TO 'vu'@'%'",
        "GRANT INSERT ON test.dest TO 'vu'@'%'",
    ] {
        exec(&e, &mut root, sql).expect(sql);
    }
    let mut user = SessionContext::default();
    user.authenticated_as = crate::engine::auth::AccountKey::new("vu", "%");
    for sql in [
        "PREPARE q FROM 'INSERT INTO dest SELECT id,a FROM v WHERE id=?'",
        "SET @n=1",
        "EXECUTE q USING @n",
        "SET @n=2",
        "EXECUTE q USING @n",
    ] {
        exec(&e, &mut user, sql).expect(sql);
    }
    exec(&e, &mut root, "ALTER VIEW v AS SELECT id,a+1 AS a FROM t").unwrap();
    exec(&e, &mut user, "EXECUTE q USING @n").unwrap();
    assert_eq!(
        rows(&e, &mut root, "SELECT * FROM dest ORDER BY id,a"),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(2), Value::Int(21)]
        ]
    );
    exec(&e, &mut root, "REVOKE SELECT ON test.v FROM 'vu'@'%'").unwrap();
    assert!(exec(&e, &mut user, "EXECUTE q USING @n").is_err());
    assert_eq!(
        rows(&e, &mut root, "SELECT COUNT(*) FROM dest"),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn view_0929_trim_reopen_and_invisible_dump() {
    let (d, e, mut s) = setup();
    for sql in [
        "ALTER TABLE t ADD COLUMN h INT DEFAULT 7 INVISIBLE",
        "CREATE VIEW v(k,n) AS SELECT id,TRIM(TRAILING '0' FROM CAST(a AS CHAR)) FROM t",
        "CREATE VIEW star AS SELECT * FROM t",
    ] {
        exec(&e, &mut s, sql).expect(sql);
    }
    let ddl = rows(&e, &mut s, "SHOW CREATE VIEW star")[0][1]
        .to_display_string()
        .replace("VIEW `star`", "VIEW `copy`");
    exec(&e, &mut s, &ddl).expect(&ddl);
    e.checkpoint().unwrap();
    drop(e);
    let e = open_strict_test_engine(d.path());
    let mut s = SessionContext::default();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM v ORDER BY k"),
        vec![
            vec![Value::Int(1), Value::String("1".into())],
            vec![Value::Int(2), Value::String("2".into())]
        ]
    );
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM star ORDER BY id"),
        rows(&e, &mut s, "SELECT * FROM copy ORDER BY id")
    );
}

#[test]
fn view_0929_aggregate_lock_is_explicit_error() {
    let (_d, e, mut s) = setup();
    exec(
        &e,
        &mut s,
        "CREATE VIEW v AS SELECT a,COUNT(*) AS n FROM t GROUP BY a",
    )
    .unwrap();
    assert_eq!(
        exec(&e, &mut s, "SELECT * FROM v FOR UPDATE")
            .unwrap_err()
            .code(),
        1235
    );
}

#[test]
fn view_0929_join_defaults_and_types() {
    let (_d, e, mut s) = setup();
    for sql in [
        "CREATE TABLE typed(id INT PRIMARY KEY,n DECIMAL(12,2) DEFAULT 9.25,label VARCHAR(30),dt DATE,j JSON)",
        "INSERT INTO typed VALUES(1,20,'old','2020-01-01','{\"a\":1}')",
        "CREATE VIEW jv AS SELECT typed.id AS k,typed.n AS renamed,typed.label,typed.dt,typed.j,t.a FROM typed JOIN t ON typed.id=t.id",
        "UPDATE jv SET renamed=DEFAULT(renamed),label=CONCAT('value:',a),dt=DATE_ADD(dt,INTERVAL 1 DAY),j=JSON_SET(j,'$.a',a) WHERE k=1",
    ] {exec(&e,&mut s,sql).expect(sql);}
    let base = e.database().table_required("test", "typed").unwrap();
    let view = e.database().view_definition("test", "jv").unwrap();
    for (output, source) in [
        ("renamed", "n"),
        ("label", "label"),
        ("dt", "dt"),
        ("j", "j"),
    ] {
        let column = base
            .columns
            .iter()
            .find(|column| column.name == source)
            .unwrap();
        let projected = view
            .columns
            .iter()
            .find(|column| column.name == output)
            .unwrap();
        assert_eq!(
            projected.data_type.as_ref(),
            Some(&column.data_type),
            "{output}"
        );
    }
    let actual = rows(
        &e,
        &mut s,
        "SELECT n,label,dt,JSON_UNQUOTE(JSON_EXTRACT(j,'$.a')) FROM typed",
    );
    let display = actual[0]
        .iter()
        .map(Value::to_display_string)
        .collect::<Vec<_>>();
    assert_eq!(display, vec!["9.25", "value:10", "2020-01-02", "10"]);
}

#[test]
fn view_0929_join_subquery_alias_cannot_capture_outer_column() {
    let (_d, e, mut s) = setup();
    for sql in [
        "CREATE TABLE u(id INT PRIMARY KEY)",
        "INSERT INTO u VALUES(1)",
        "CREATE VIEW j AS SELECT t.id AS k,t.a AS n FROM t JOIN u ON t.id=u.id",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    // Correlated joined-DML subqueries are currently unsupported. An internal
    // alias collision must not turn one into an uncorrelated query returning 9.
    assert!(exec(
        &e,
        &mut s,
        "UPDATE j AS x SET n=(SELECT x.k FROM (SELECT 9 AS id) __view_update_0) WHERE k=1"
    )
    .is_err());
    assert_eq!(
        rows(&e, &mut s, "SELECT a FROM t WHERE id=1"),
        vec![vec![Value::Int(10)]]
    );
    exec(
        &e,
        &mut s,
        "UPDATE j SET n=(SELECT id FROM (SELECT 9 AS id) __view_update_0) WHERE k=1",
    )
    .unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT a FROM t WHERE id=1"),
        vec![vec![Value::Int(9)]]
    );
    exec(&e, &mut s, "DROP TABLE u").unwrap();
    for sql in ["UPDATE j SET n=1", "DELETE FROM j"] {
        assert_eq!(exec(&e, &mut s, sql).unwrap_err().code(), 1356);
    }
}
