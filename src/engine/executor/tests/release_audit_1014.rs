use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_strict_test_engine(dir.path());
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let response = engine
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected rows: {sql}");
    };
    result
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|v| v.to_display_string()).collect())
        .collect()
}

#[test]
fn audit14_binary_subquery_preserves_bytes_after_restart() {
    let (dir, engine) = setup();
    for sql in [
        "CREATE TABLE src(id INT PRIMARY KEY,b LONGBLOB)",
        "CREATE TABLE dst(id INT PRIMARY KEY,b LONGBLOB)",
        "INSERT INTO src VALUES(1,0x41FFFE42800043)",
        "INSERT INTO dst VALUES(1,(SELECT b FROM src WHERE id=1)),(2,NULL)",
        "UPDATE dst SET b=(SELECT b FROM src WHERE id=1) WHERE id=2",
    ] {
        engine.execute(sql).unwrap();
    }
    assert_eq!(
        rows(&engine, "SELECT HEX(b) FROM dst ORDER BY id"),
        vec![vec!["41FFFE42800043"], vec!["41FFFE42800043"]]
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT HEX(b) FROM dst ORDER BY id"),
        vec![vec!["41FFFE42800043"], vec!["41FFFE42800043"]]
    );
    engine
        .execute("DELETE FROM dst WHERE b=(SELECT b FROM src WHERE id=1)")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT COUNT(*) FROM dst"), vec![vec!["0"]]);
}

#[test]
fn audit14_not_in_self_reference_select_explain_and_dml() {
    let (_dir, engine) = setup();
    for sql in [
        "CREATE TABLE t(a INT PRIMARY KEY,b INT NOT NULL)",
        "INSERT INTO t VALUES(1,1),(2,2),(3,2)",
    ] {
        engine.execute(sql).unwrap();
    }
    for source in ["t", "t AS x"] {
        let q = format!("SELECT a FROM {source} WHERE a NOT IN(SELECT b FROM t WHERE a<3)");
        assert_eq!(rows(&engine, &q), vec![vec!["3"]]);
        engine.execute(&format!("EXPLAIN {q}")).unwrap();
    }
    engine
        .execute("UPDATE t SET b=9 WHERE a NOT IN(SELECT b FROM t WHERE a<3)")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT b FROM t WHERE a=3"), vec![vec!["9"]]);
    engine
        .execute("DELETE FROM t WHERE a NOT IN(SELECT b FROM t WHERE a<3)")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT COUNT(*) FROM t"), vec![vec!["2"]]);
}

#[test]
fn audit14_numeric_hexbit_predicates_and_assignments() {
    let (_dir, engine) = setup();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,n INT,d DECIMAL(12,2),b VARBINARY(8))",
        "INSERT INTO t VALUES(1,1,1,0x01),(2,2,2,0x02),(3,3,3,0x03)",
    ] {
        engine.execute(sql).unwrap();
    }
    for predicate in [
        "id=0x01",
        "n=0x01",
        "d=0x01",
        "id IN(0x01)",
        "n BETWEEN 0x01 AND 0x01",
        "0x01=n",
    ] {
        assert_eq!(
            rows(&engine, &format!("SELECT id FROM t WHERE {predicate}")),
            vec![vec!["1"]],
            "{predicate}"
        );
    }
    for sql in [
        "UPDATE t SET n=0x10,d=b'10' WHERE id=1",
        "INSERT INTO t VALUES(1,0,0,NULL) ON DUPLICATE KEY UPDATE n=0x11",
        "UPDATE t a JOIN t b ON a.id=b.id SET a.d=0x12 WHERE a.id=1",
    ] {
        engine.execute(sql).unwrap();
    }
    assert_eq!(
        rows(&engine, "SELECT n,d,HEX(b) FROM t WHERE id=1"),
        vec![vec!["17", "18.00", "01"]]
    );
    engine.execute("DELETE FROM t WHERE id=b'11'").unwrap();
    assert_eq!(rows(&engine, "SELECT COUNT(*) FROM t"), vec![vec!["2"]]);
}

#[test]
fn audit14_enum_numeric_context_and_ordered_aggregate() {
    let (_dir, engine) = setup();
    for sql in ["CREATE TABLE t(id INT PRIMARY KEY,e ENUM('small','medium','large'),s SET('a','b'),n DECIMAL(4,1))", "INSERT INTO t VALUES(1,'small','a',1),(2,'medium','b',2),(3,'large','a,b',3)"] {engine.execute(sql).unwrap();}
    assert_eq!(
        rows(&engine, "SELECT id FROM t WHERE e>1.5 ORDER BY id"),
        vec![vec!["2"], vec!["3"]]
    );
    assert_eq!(
        rows(&engine, "SELECT id FROM t WHERE e=n AND s=n ORDER BY id"),
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT CAST(e AS SIGNED),CAST(s AS DECIMAL(4,1)) FROM t ORDER BY id"
        ),
        vec![vec!["1", "1.0"], vec!["2", "2.0"], vec!["3", "3.0"]]
    );
    assert_eq!(
        rows(&engine, "SELECT GROUP_CONCAT(e ORDER BY e) FROM t"),
        vec![vec!["small,medium,large"]]
    );
}

#[test]
fn audit14_dropped_required_column_does_not_block_auto_increment() {
    let (dir, engine) = setup();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY AUTO_INCREMENT,a INT NOT NULL)")
        .unwrap();
    for _ in 0..4 {
        for sql in [
            "ALTER TABLE t ADD COLUMN extra INT NOT NULL DEFAULT 7",
            "INSERT INTO t(a) VALUES(1)",
            "ALTER TABLE t DROP COLUMN extra",
            "INSERT INTO t(a) VALUES(2)",
        ] {
            engine.execute(sql).unwrap();
        }
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    engine.execute("INSERT INTO t(a) VALUES(3)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT COUNT(*),MAX(id) FROM t"),
        vec![vec!["9", "9"]]
    );
    assert_eq!(
        engine
            .execute("INSERT INTO t(a) VALUES(NULL)")
            .unwrap_err()
            .code(),
        1048
    );
}

#[test]
fn audit14_exchange_rejects_incompatible_column_metadata() {
    let (_dir, engine) = setup();
    for (left, right) in [
        (
            "VARCHAR(20) CHARACTER SET utf8mb4",
            "VARCHAR(20) CHARACTER SET latin1",
        ),
        (
            "INT GENERATED ALWAYS AS (id+1) STORED",
            "INT GENERATED ALWAYS AS (id+2) STORED",
        ),
    ] {
        engine.execute("DROP TABLE IF EXISTS p,s").unwrap();
        engine.execute(&format!("CREATE TABLE p(id INT,g {left}) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
        engine
            .execute(&format!("CREATE TABLE s(id INT,g {right})"))
            .unwrap();
        assert_eq!(
            engine
                .execute("ALTER TABLE p EXCHANGE PARTITION p0 WITH TABLE s")
                .unwrap_err()
                .code(),
            1736
        );
    }
}

#[test]
fn audit14_index_join_and_analyzed_shared_subquery() {
    let (_dir, engine) = setup();
    for sql in [
        "CREATE TABLE o(id INT PRIMARY KEY,d DOUBLE,c DECIMAL(10,2),s VARCHAR(16))",
        "CREATE TABLE i(id INT PRIMARY KEY)",
        "INSERT INTO o VALUES(1,1.5,1.5,'1.5'),(2,-0.5,-0.5,'-0.5'),(3,0,0,'0'),(4,2,2,'2')",
    ] {
        engine.execute(sql).unwrap();
    }
    let vals = (0..5000)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO i VALUES {vals}"))
        .unwrap();
    for col in ["d", "c", "s"] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT o.id,i.id FROM o JOIN i ON o.{col}=i.id ORDER BY o.id")
            ),
            vec![vec!["3", "0"], vec!["4", "2"]]
        );
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT o.id FROM o LEFT JOIN i ON o.d=i.id WHERE i.id IS NULL ORDER BY o.id"
        ),
        vec![vec!["1"], vec!["2"]]
    );
    engine.execute("ANALYZE TABLE o,i").unwrap();
    for _ in 0..2 {
        assert_eq!(
            rows(
                &engine,
                "SELECT i.id FROM o JOIN i ON o.d=i.id WHERE o.id=3"
            ),
            vec![vec!["0"]]
        );
        assert!(rows(
            &engine,
            "SELECT i.id FROM o JOIN i ON o.d=i.id WHERE o.id=1"
        )
        .is_empty());
    }
}

#[test]
fn audit14_exchange_preserves_both_auto_increment_high_watermarks() {
    let (dir, engine) = setup();
    for sql in ["CREATE TABLE p(id INT AUTO_INCREMENT,v INT,PRIMARY KEY(id,v)) PARTITION BY RANGE(v)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)","CREATE TABLE s(id INT AUTO_INCREMENT,v INT,PRIMARY KEY(id,v))","INSERT INTO p VALUES(100,1)","INSERT INTO s VALUES(5,1)","ALTER TABLE p AUTO_INCREMENT=200","ALTER TABLE p EXCHANGE PARTITION p0 WITH TABLE s"] {engine.execute(sql).unwrap();}
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    engine.execute("INSERT INTO s(v) VALUES(11)").unwrap();
    engine.execute("INSERT INTO p(v) VALUES(11)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT id FROM s WHERE v=11"),
        vec![vec!["200"]]
    );
    assert_eq!(
        rows(&engine, "SELECT id FROM p WHERE v=11"),
        vec![vec!["200"]]
    );
}

#[test]
fn audit14_server_file_access_disabled_by_default() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    let file = tempfile::NamedTempFile::new_in(crate::util::file::test_temp_dir()).unwrap();
    std::fs::write(file.path(), b"1\n").unwrap();
    engine.execute("CREATE TABLE t(id INT)").unwrap();
    let sql = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t",
        crate::util::file::sql_path(file.path())
    );
    assert_eq!(engine.execute(&sql).unwrap_err().code(), 1290);
    engine
        .execute(&format!(
            "SET GLOBAL secure_file_priv='{}'",
            crate::util::file::sql_path(file.path().parent().unwrap())
        ))
        .unwrap();
    engine.execute(&sql).unwrap();
    assert_eq!(rows(&engine, "SELECT id FROM t"), vec![vec!["1"]]);
}

#[test]
fn audit14_index_join_rejects_rounded_decimal_and_float_keys() {
    let (_dir, engine) = setup();
    engine
        .execute("CREATE TABLE o(id INT PRIMARY KEY,d DOUBLE)")
        .unwrap();
    engine
        .execute("INSERT INTO o VALUES(1,1.234),(2,1.00000001),(3,2)")
        .unwrap();
    let values = (0..5000)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    for (name, kind) in [("de", "DECIMAL(12,2)"), ("fl", "FLOAT")] {
        engine
            .execute(&format!("CREATE TABLE {name}(k {kind} PRIMARY KEY)"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES {values}"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES(1.23)"))
            .unwrap();
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT o.id FROM o STRAIGHT_JOIN {name} i ON o.d=i.k ORDER BY o.id")
            ),
            vec![vec!["3"]]
        );
    }
}

#[test]
fn audit14_enum_numeric_context_in_dml_and_prepared_assignments() {
    let (_dir, engine) = setup();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,e ENUM('a','b'),n INT)",
        "INSERT INTO t VALUES(1,'a',0),(2,'b',0)",
        "UPDATE t SET n=CAST(e AS SIGNED)",
    ] {
        engine.execute(sql).unwrap();
    }
    assert_eq!(
        rows(&engine, "SELECT n FROM t ORDER BY id"),
        vec![vec!["1"], vec!["2"]]
    );
    let mut session = SessionContext::default();
    for sql in [
        "PREPARE u FROM 'UPDATE t SET n=CAST(e AS SIGNED)+? WHERE id=?'",
        "SET @v=10,@id=2",
        "EXECUTE u USING @v,@id",
        "DEALLOCATE PREPARE u",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        rows(&engine, "SELECT n FROM t WHERE id=2"),
        vec![vec!["12"]]
    );
    engine
        .execute("INSERT INTO t VALUES(2,'a',0) ON DUPLICATE KEY UPDATE n=CAST(e AS SIGNED)")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT n FROM t WHERE id=2"), vec![vec!["2"]]);
    engine
        .execute("UPDATE t a JOIN t b ON a.id=b.id SET a.n=CAST(b.e AS SIGNED)+1")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT n FROM t ORDER BY id"),
        vec![vec!["2"], vec!["3"]]
    );
    engine.execute("DELETE FROM t WHERE e<1.5").unwrap();
    assert_eq!(rows(&engine, "SELECT id FROM t"), vec![vec!["2"]]);
}
