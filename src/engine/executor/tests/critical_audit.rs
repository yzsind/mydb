use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine) {
    let root = std::path::Path::new("/tmp/mydb");
    std::fs::create_dir_all(root).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("critical-audit-")
        .tempdir_in(root)
        .unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    let response = engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    let values = match response {
        QueryResponse::ResultSet(rs) => rs.rows,
        QueryResponse::StreamingResultSet(mut rs) => {
            let mut rows = Vec::new();
            while let Some(row) = rs.cursor.next_row().unwrap() {
                rows.push(row);
            }
            rows
        }
        other => panic!("expected rows, got {other:?}"),
    };
    values
        .into_iter()
        .map(|row| row.into_iter().map(|v| v.to_display_string()).collect())
        .collect()
}

#[test]
fn critical_audit_index_keys_follow_visible_versions() {
    let (_dir, engine) = setup();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY iv(v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,1),(2,2),(3,3)")
        .unwrap();
    let mut writer = SessionContext::default();
    let mut reader = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    for value in [9999, 8888, 7777] {
        engine
            .execute_with_session(&mut writer, &format!("UPDATE t SET v={value} WHERE id=1"))
            .unwrap();
        for aggregate in ["COUNT(*)", "COUNT(v)", "COUNT(DISTINCT v)"] {
            let query = format!("SELECT {aggregate} FROM t WHERE v={value}");
            assert_eq!(
                rows(&engine, &mut reader, &query),
                vec![vec!["0"]],
                "{query}"
            );
        }
        assert_eq!(
            rows(
                &engine,
                &mut reader,
                "SELECT MIN(v),MAX(v) FROM t WHERE v>=7777"
            ),
            vec![vec!["NULL", "NULL"]]
        );
        assert_eq!(
            rows(&engine, &mut reader, "SELECT COUNT(*) FROM t WHERE v=1"),
            vec![vec!["1"]]
        );
        assert_eq!(
            rows(
                &engine,
                &mut writer,
                &format!("SELECT COUNT(*) FROM t WHERE v={value}")
            ),
            vec![vec!["1"]]
        );
        assert_eq!(
            rows(&engine, &mut writer, "SELECT COUNT(*) FROM t WHERE v=1"),
            vec![vec!["0"]]
        );
    }
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    assert_eq!(
        rows(&engine, &mut reader, "SELECT COUNT(DISTINCT v) FROM t"),
        vec![vec!["3"]]
    );
}

#[test]
fn critical_audit_read_write_consistent_snapshot_is_eager() {
    let (_dir, engine) = setup();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,100)").unwrap();
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut reader,
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ WRITE",
        )
        .unwrap();
    engine.execute("UPDATE t SET v=200 WHERE id=1").unwrap();
    assert_eq!(
        rows(&engine, &mut reader, "SELECT v FROM t"),
        vec![vec!["100"]]
    );
    engine
        .execute_with_session(&mut reader, "UPDATE t SET v=300 WHERE id=1")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut reader, "SELECT v FROM t"),
        vec![vec!["300"]]
    );
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
}

#[test]
fn critical_audit_long_predicates_preserve_order_and_three_valued_logic() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    let where_sql = format!(
        "SELECT 1 WHERE {}",
        std::iter::repeat_n("1=1", 500)
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    assert_eq!(rows(&engine, &mut session, &where_sql), vec![vec!["1"]]);
    for operator in ["AND", "OR"] {
        let operand = if operator == "AND" { "1=1" } else { "1=0" };
        let sql = format!(
            "SELECT {}",
            std::iter::repeat_n(operand, 500)
                .collect::<Vec<_>>()
                .join(&format!(" {operator} "))
        );
        assert_eq!(
            rows(&engine, &mut session, &sql),
            vec![vec![if operator == "AND" { "1" } else { "0" }]]
        );
    }
    assert_eq!(rows(&engine,&mut session,"SELECT NULL AND 1 AND 0, NULL OR 0 OR 1, 1='a' LIKE 'a',1=2 IN(0),POSITION('b' IN 'abc')"),vec![vec!["0","1","1","0","2"]]);
}

#[test]
fn critical_audit_row_default_copy_preserves_supplied_values() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    engine.execute("CREATE TABLE t(a INT PRIMARY KEY)").unwrap();
    engine.execute("INSERT INTO t VALUES(2),(3)").unwrap();
    engine
        .execute("ALTER TABLE t ADD b INT DEFAULT(a*10)")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT * FROM t ORDER BY a"),
        vec![vec!["2", "20"], vec!["3", "30"]]
    );
    engine.execute("INSERT INTO t(a) VALUES(4)").unwrap();
    engine.execute("INSERT INTO t VALUES(5,99)").unwrap();
    engine.execute("INSERT INTO t VALUES(6,DEFAULT)").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT * FROM t WHERE a>=4 ORDER BY a"
        ),
        vec![vec!["4", "40"], vec!["5", "99"], vec!["6", "60"]]
    );
    engine.execute("UPDATE t SET b=DEFAULT WHERE a=5").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT b FROM t WHERE a=5"),
        vec![vec!["50"]]
    );
}

#[test]
fn critical_audit_geometry_input_codecs_reject_incomplete_shapes() {
    let (_dir, engine) = setup();
    for sql in [
        "SELECT ST_GeomFromText('LINESTRING(0 0)')",
        "SELECT ST_GeomFromText('POLYGON((0 0,1 0,1 1))')",
        "SELECT ST_GeomFromGeoJSON('{\"type\":\"LineString\",\"coordinates\":[[0,0]]}')",
        "SELECT ST_GeomFromWKB(X'01020000000100000000000000000000000000000000000000')",
    ] {
        let error = engine.execute(sql).unwrap_err();
        assert_eq!(error.code(), 3037, "{sql}: {error:?}");
    }
}

#[test]
fn critical_audit_decimal_text_hash_keys_recheck_precision_collisions() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    engine.execute("CREATE TABLE d(n DECIMAL(65,0))").unwrap();
    engine.execute("CREATE TABLE s(n VARCHAR(80))").unwrap();
    engine
        .execute("INSERT INTO d VALUES(9007199254740992),(9007199254740993)")
        .unwrap();
    engine
        .execute("INSERT INTO s VALUES('9007199254740992'),('9007199254740993')")
        .unwrap();
    for from in ["d JOIN s", "s JOIN d"] {
        assert_eq!(
            rows(
                &engine,
                &mut session,
                &format!("SELECT COUNT(*) FROM {from} ON d.n=s.n")
            ),
            vec![vec!["2"]]
        );
    }
}

#[test]
fn critical_audit_procedure_recursion_accepts_public_limit() {
    let (_dir, engine) = setup();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET SESSION max_sp_recursion_depth=255")
        .unwrap();
    engine.execute("CREATE PROCEDURE deep(IN d INT, OUT acc INT) BEGIN IF d=0 THEN SET acc=0; ELSE CALL deep(d-1,acc); SET acc=acc+1; END IF; END").unwrap();
    engine
        .execute_with_session(&mut session, "CALL deep(255,@a)")
        .unwrap();
    assert_eq!(rows(&engine, &mut session, "SELECT @a"), vec![vec!["255"]]);
    assert_eq!(
        engine
            .execute_with_session(&mut session, "CALL deep(256,@a)")
            .unwrap_err()
            .code(),
        1456
    );
    engine
        .execute_with_session(&mut session, "CALL deep(1,@a)")
        .unwrap();
    assert_eq!(rows(&engine, &mut session, "SELECT @a"), vec![vec!["1"]]);
}
