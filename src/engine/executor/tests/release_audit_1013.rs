use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

fn query(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> ResultSet {
    match engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .materialize()
        .unwrap()
    {
        QueryResponse::ResultSet(rs) => rs,
        _ => panic!("expected rows: {sql}"),
    }
}

#[test]
fn audit13_set_operations_read_own_writes() {
    for isolation in ["READ COMMITTED", "REPEATABLE READ"] {
        let (_dir, engine, mut s) = setup();
        for sql in [
            "CREATE TABLE a(k INT)",
            "CREATE TABLE b(k INT)",
            "INSERT INTO a VALUES(1)",
            "INSERT INTO b VALUES(2)",
        ] {
            engine.execute(sql).unwrap();
        }
        engine
            .execute_with_session(
                &mut s,
                &format!("SET SESSION TRANSACTION ISOLATION LEVEL {isolation}"),
            )
            .unwrap();
        engine.execute_with_session(&mut s, "BEGIN").unwrap();
        for (write, expected) in [
            ("INSERT INTO a VALUES(3)", vec![1, 3]),
            ("UPDATE a SET k=4 WHERE k=1", vec![3, 4]),
            ("DELETE FROM a WHERE k=3", vec![4]),
        ] {
            engine.execute_with_session(&mut s, write).unwrap();
            for op in ["UNION ALL", "UNION", "INTERSECT", "EXCEPT"] {
                let rhs = if op == "INTERSECT" {
                    "a"
                } else {
                    "b WHERE k=99"
                };
                let rs = query(
                    &engine,
                    &mut s,
                    &format!("SELECT k FROM a {op} SELECT k FROM {rhs} ORDER BY k"),
                );
                assert_eq!(
                    rs.rows,
                    expected
                        .iter()
                        .map(|v| vec![Value::Int(*v)])
                        .collect::<Vec<_>>(),
                    "{isolation}: {write}: {op}"
                );
            }
        }
        engine.execute_with_session(&mut s, "ROLLBACK").unwrap();
        assert_eq!(
            query(&engine, &mut s, "SELECT k FROM a").rows,
            vec![vec![Value::Int(1)]]
        );
    }
}

#[test]
fn audit13_extract_output_name_does_not_shadow_source() {
    let (_dir, engine, mut s) = setup();
    engine
        .execute("CREATE TABLE t(id INT,dt DATETIME)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'2024-01-02 00:00:01'),(2,'2024-01-01 00:00:59')")
        .unwrap();
    let rs = query(
        &engine,
        &mut s,
        "SELECT id, EXTRACT(SECOND FROM dt) FROM t ORDER BY dt",
    );
    assert_ne!(rs.columns[1].name, "dt");
    assert_eq!(rs.rows[0][0], Value::Int(2));
}

#[test]
fn audit13_enum_set_numeric_contexts() {
    let (_dir, engine, mut s) = setup();
    engine
        .execute("CREATE TABLE t(id INT,e ENUM('a','b','c'),st SET('x','y'))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'b','y'),(2,'a','x'),(3,NULL,NULL)")
        .unwrap();
    for expr in ["e+0", "e*1", "ABS(e)", "st+0", "ABS(st)"] {
        for sql in [
            format!("SELECT id FROM t WHERE {expr}=2"),
            format!("SELECT id FROM t GROUP BY id,e,st HAVING {expr}=2"),
            format!("SELECT t.id FROM t JOIN (SELECT 2 q) x ON {expr}=q"),
        ] {
            assert_eq!(
                query(&engine, &mut s, &sql).rows,
                vec![vec![Value::Int(1)]],
                "{sql}"
            );
        }
        assert_eq!(
            query(
                &engine,
                &mut s,
                &format!("SELECT {expr},COUNT(*) FROM t GROUP BY {expr}")
            )
            .rows
            .len(),
            3
        );
    }
    for sql in [
        "SELECT id FROM t AS base WHERE e+0=2",
        "SELECT id FROM t WHERE t.e+0=2",
        "SELECT id FROM test.t WHERE test.t.e+0=2",
        "SELECT id FROM t AS base WHERE base.e+0=2",
        "SELECT id FROM t AS base WHERE ABS(st)=2",
        "SELECT id FROM t AS base GROUP BY id,e,st HAVING e+0=2",
        "SELECT base.id FROM t AS base JOIN (SELECT 2 q) probe ON e+0=q",
    ] {
        assert_eq!(
            query(&engine, &mut s, sql).rows,
            vec![vec![Value::Int(1)]],
            "{sql}"
        );
    }
    assert_eq!(
        query(&engine, &mut s, "SELECT e+0 FROM t AS base WHERE id=1").rows,
        vec![vec![Value::Int(2)]]
    );
    let ambiguous = engine
        .execute("SELECT e+0 FROM t AS a JOIN t AS b ON a.id=b.id")
        .and_then(QueryResponse::materialize)
        .unwrap_err();
    assert_eq!(ambiguous.code(), 1052);
}

#[test]
fn audit13_sequence_values_are_evaluated_per_row_and_without_from() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE SEQUENCE seq",
        "CREATE TABLE src(id INT)",
        "INSERT INTO src VALUES(1),(2),(3)",
        "CREATE TABLE dst(id BIGINT,src INT)",
    ] {
        engine.execute_with_session(&mut s, sql).unwrap();
    }
    engine
        .execute_with_session(&mut s, "INSERT INTO dst SELECT NEXTVAL('seq'),id FROM src")
        .unwrap();
    assert_eq!(
        query(&engine, &mut s, "SELECT id FROM dst ORDER BY src").rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    assert_eq!(
        query(&engine, &mut s, "SELECT NEXTVAL('seq')").rows,
        vec![vec![Value::Int(4)]]
    );
    assert_eq!(
        query(&engine, &mut s, "SELECT CURRVAL('seq'),LASTVAL()").rows,
        vec![vec![Value::Int(4), Value::Int(4)]]
    );
    let sequence_rows = query(&engine, &mut s, "SELECT NEXTVAL('seq') FROM src");
    assert_eq!(sequence_rows.columns[0].column_type, ColumnType::LongLong);
    assert_eq!(
        sequence_rows.rows,
        vec![
            vec![Value::Int(5)],
            vec![Value::Int(6)],
            vec![Value::Int(7)]
        ]
    );
    assert_eq!(
        query(&engine, &mut s, "SELECT CURRVAL('seq')").rows,
        vec![vec![Value::Int(7)]]
    );
}

#[test]
fn audit13_views_accept_declared_builtins() {
    let (_dir, engine, mut s) = setup();
    for expr in [
        "COALESCE(NULL,1)",
        "IF(1,1,2)",
        "IFNULL(NULL,1)",
        "NULLIF(1,2)",
        "GREATEST(1,2)",
        "LEAST(1,2)",
        "ISNULL(NULL)",
    ] {
        engine
            .execute(&format!("CREATE OR REPLACE VIEW v AS SELECT {expr} AS x"))
            .unwrap();
        assert_eq!(
            query(&engine, &mut s, "SELECT x FROM v").rows,
            query(&engine, &mut s, &format!("SELECT {expr}")).rows
        );
        engine
            .execute(&format!("ALTER VIEW v AS SELECT {expr} AS x"))
            .unwrap();
    }
    assert!(engine
        .execute("CREATE VIEW bad AS SELECT made_up_function(1)")
        .is_err());
}

#[test]
fn audit13_load_null_defaults_and_strict_atomicity() {
    let (_dir, engine, mut s) = setup();
    let file = tempfile::NamedTempFile::new_in(crate::util::file::test_temp_dir()).unwrap();
    std::fs::write(file.path(), b"1\t\\N\n2\t5\n").unwrap();
    engine
        .execute("CREATE TABLE t(id INT,v INT NOT NULL)")
        .unwrap();
    for (mode, ignore) in [("STRICT_TRANS_TABLES", "IGNORE"), ("", "")] {
        engine
            .execute_with_session(&mut s, &format!("SET sql_mode='{mode}'"))
            .unwrap();
        engine
            .execute_with_session(
                &mut s,
                &format!(
                    "LOAD DATA INFILE '{}' {ignore} INTO TABLE t",
                    file.path().display()
                ),
            )
            .unwrap();
        assert_eq!(
            query(&engine, &mut s, "SELECT id,v FROM t ORDER BY id").rows,
            vec![
                vec![Value::Int(1), Value::Int(0)],
                vec![Value::Int(2), Value::Int(5)]
            ]
        );
        engine.execute("TRUNCATE TABLE t").unwrap();
    }
    engine
        .execute_with_session(&mut s, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut s,
            &format!("LOAD DATA INFILE '{}' INTO TABLE t", file.path().display()),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1048);
    assert!(query(&engine, &mut s, "SELECT * FROM t").rows.is_empty());
}

#[test]
fn audit13_partition_vector_updates_commit_rollback_and_migration() {
    for algo in ["HNSW", "IVF_SQ8"] {
        let (_dir, engine, mut s) = setup();
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,KEY vk USING {algo}(v)) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(1000),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
        let rows = (0..2000)
            .map(|id| format!("({id},'[{},1]')", id + 10))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES{rows}"))
            .unwrap();
        engine.execute("ANALYZE TABLE t").unwrap();
        let knn="SELECT /*+ HNSW_EF(512) IVF_NPROBE(256) */ id FROM t ORDER BY VEC_DISTANCE(v,'[0,0]') LIMIT 1";
        assert_eq!(query(&engine, &mut s, knn).rows, vec![vec![Value::Int(0)]]);
        engine.execute_with_session(&mut s, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut s, "UPDATE t SET v='[0,0]' WHERE id=1500")
            .unwrap();
        engine.execute_with_session(&mut s, "ROLLBACK").unwrap();
        assert_eq!(query(&engine, &mut s, knn).rows, vec![vec![Value::Int(0)]]);
        engine
            .execute_with_session(&mut s, "UPDATE t SET v='[0,0]' WHERE id=1500")
            .unwrap();
        assert_eq!(
            query(&engine, &mut s, knn).rows,
            vec![vec![Value::Int(1500)]],
            "{algo}"
        );
        engine
            .execute_with_session(&mut s, "UPDATE t SET id=-1 WHERE id=1500")
            .unwrap();
        assert_eq!(
            query(&engine, &mut s, knn).rows,
            vec![vec![Value::Int(-1)]],
            "{algo}"
        );
    }
}

#[test]
fn audit13_enum_numeric_string_assignment_and_minmax_metadata() {
    let (_dir, engine, mut s) = setup();
    engine
        .execute("CREATE TABLE t(id INT,e ENUM('a','b','1'))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'2'),(2,'1')")
        .unwrap();
    assert_eq!(
        query(&engine, &mut s, "SELECT e FROM t ORDER BY id")
            .rows
            .iter()
            .map(|r| r[0].to_display_string())
            .collect::<Vec<_>>(),
        ["b", "1"]
    );
    let rs = query(&engine, &mut s, "SELECT MIN(id),MAX(id) FROM t");
    assert!(
        rs.columns
            .iter()
            .all(|c| matches!(c.column_type, ColumnType::Long | ColumnType::LongLong)),
        "{:?}",
        rs.columns
    );
    assert!(engine.execute("CREATE INDEX test.bad ON t(id)").is_err());
}

#[test]
fn audit13_decimal_div_overflow_names_source_column() {
    let (_dir, engine, mut s) = setup();
    engine.execute("CREATE TABLE t(dd DECIMAL(38,10))").unwrap();
    engine
        .execute("INSERT INTO t VALUES(99999999999999999999.9999999999)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "SELECT dd DIV 1 FROM t")
        .and_then(|r| r.materialize())
        .unwrap_err();
    assert_eq!(err.code(), 1690);
    assert!(!err.message().contains('\0'), "{}", err.message());
    assert!(err.message().contains("dd"), "{}", err.message());
}
