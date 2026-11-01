use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected rows: {sql}");
    };
    result.rows
}

#[test]
fn trigger_column_bindings_follow_drop_and_readd() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for assignment in [false, true] {
        engine
            .execute("CREATE TABLE tr_t(id INT PRIMARY KEY,a INT,b INT)")
            .unwrap();
        engine.execute("CREATE TABLE tr_a(v INT)").unwrap();
        engine.execute(if assignment {
            "CREATE TRIGGER bi BEFORE INSERT ON tr_t FOR EACH ROW SET NEW.b=NEW.a+1"
        } else {
            "CREATE TRIGGER bi BEFORE INSERT ON tr_t FOR EACH ROW INSERT INTO tr_a VALUES(NEW.b)"
        }).unwrap();
        engine.execute("INSERT INTO tr_t VALUES(1,10,100)").unwrap();
        engine.execute("ALTER TABLE tr_t DROP COLUMN b").unwrap();
        assert_eq!(
            engine
                .execute("INSERT INTO tr_t(id,a) VALUES(2,20)")
                .unwrap_err()
                .code(),
            1054
        );
        assert_eq!(
            rows(&engine, "SELECT COUNT(*) FROM tr_t")[0][0],
            Value::Int(1)
        );
        engine.execute("ALTER TABLE tr_t ADD COLUMN b INT").unwrap();
        engine.execute("INSERT INTO tr_t VALUES(3,30,300)").unwrap();
        if assignment {
            assert_eq!(
                rows(&engine, "SELECT b FROM tr_t WHERE id=3")[0][0],
                Value::Int(31)
            );
        } else {
            assert_eq!(
                rows(&engine, "SELECT v FROM tr_a ORDER BY v"),
                vec![vec![Value::Int(100)], vec![Value::Int(300)]]
            );
        }
        engine.execute("DROP TABLE tr_t,tr_a").unwrap();
    }
}

#[test]
fn cursor_fetch_exhaustion_errors_without_a_handler() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(7)").unwrap();
    engine.execute("CREATE PROCEDURE p() BEGIN DECLARE v INT; DECLARE c CURSOR FOR SELECT a FROM t; OPEN c; FETCH c INTO v; FETCH c INTO v; CLOSE c; END").unwrap();
    assert_eq!(engine.execute("CALL p()").unwrap_err().code(), 1329);
    engine.execute("CREATE PROCEDURE handled() BEGIN DECLARE v INT; DECLARE done INT DEFAULT 0; DECLARE c CURSOR FOR SELECT a FROM t; DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=1; OPEN c; FETCH c INTO v; FETCH c INTO v; CLOSE c; SELECT done,v; END").unwrap();
    assert!(engine.execute("CALL handled()").is_ok());
    engine.execute("CREATE PROCEDURE empty_select() BEGIN DECLARE v INT DEFAULT 9; SELECT a INTO v FROM t WHERE a=0; SELECT v; END").unwrap();
    assert!(engine.execute("CALL empty_select()").is_ok());
}

#[test]
fn functions_reject_flush_and_table_locks_but_allow_global_set() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT)").unwrap();
    for statement in [
        "FLUSH TABLES",
        "FLUSH STATUS",
        "LOCK TABLES t READ",
        "UNLOCK TABLES",
    ] {
        let sql = format!(
            "CREATE FUNCTION f() RETURNS INT DETERMINISTIC BEGIN {statement}; RETURN 1; END"
        );
        let error = engine.execute(&sql).unwrap_err();
        assert!(
            matches!(error.code(), 1314 | 1336 | 1422),
            "{statement}: {error:?}"
        );
    }
    // MySQL 8.0 permits SET GLOBAL here; authorization still applies at CALL.
    engine.execute("CREATE FUNCTION g() RETURNS INT DETERMINISTIC BEGIN SET GLOBAL innodb_lock_wait_timeout=50; RETURN 1; END").unwrap();
}

#[test]
fn temporary_table_creation_is_allowed_under_explicit_locks() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(a INT)",
        "INSERT INTO t VALUES(7)",
        "LOCK TABLES t WRITE",
        "CREATE TEMPORARY TABLE tmp(a INT)",
        "CREATE TEMPORARY TABLE tmp_like LIKE t",
        "CREATE TEMPORARY TABLE tmp_as AS SELECT * FROM t",
        "UNLOCK TABLES",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    engine.close_session(&mut session).unwrap();
}

#[test]
fn missing_derived_fulltext_file_is_rebuilt_from_the_heap() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    engine
        .execute("CREATE TABLE ft(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY f(body))")
        .unwrap();
    engine
        .execute("INSERT INTO ft VALUES(1,'apple database'),(2,'orange database')")
        .unwrap();
    let table = engine.db_handle().table("test.ft").unwrap();
    let index = table.indexes.iter().find(|i| i.is_fulltext()).unwrap();
    let path = crate::storage::naming::index_object_path_by_id(
        dir.path(),
        table.id,
        table.active_generation.max(1),
        index.index_id,
        "idx",
    );
    engine.shutdown().unwrap();
    drop(table);
    drop(engine);
    std::fs::remove_file(path).unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM ft WHERE MATCH(body) AGAINST('apple')"
        ),
        vec![vec![Value::Int(1)]]
    );
    engine.shutdown().unwrap();
}

#[test]
fn fulltext_phrase_positions_preserve_filtered_words() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ft(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY f(body))")
        .unwrap();
    engine.execute("INSERT INTO ft VALUES(1,'abc the def'),(2,'abc def'),(3,'abc and def'),(4,'the abc def'),(5,'abc a def'),(6,'abc xx def')").unwrap();
    for (phrase, expected) in [
        ("abc def", vec![2, 4]),
        ("abc the def", vec![1]),
        ("the abc def", vec![2, 4]),
        ("abc a def", vec![5]),
        ("abc xx def", vec![6]),
    ] {
        let sql=format!("SELECT id FROM ft WHERE MATCH(body) AGAINST('\"{phrase}\"' IN BOOLEAN MODE) ORDER BY id");
        assert_eq!(
            rows(&engine, &sql),
            expected
                .into_iter()
                .map(|i| vec![Value::Int(i)])
                .collect::<Vec<_>>(),
            "{sql}"
        );
    }
    engine
        .execute("UPDATE ft SET body='abc def' WHERE id=1")
        .unwrap();
    assert_eq!(rows(&engine,"SELECT id FROM ft WHERE MATCH(body) AGAINST('\"abc def\"' IN BOOLEAN MODE) ORDER BY id"),vec![vec![Value::Int(1)],vec![Value::Int(2)],vec![Value::Int(4)]]);
    engine
        .execute("CREATE TABLE ops(id INT,body TEXT,FULLTEXT(body))")
        .unwrap();
    engine
        .execute("INSERT INTO ops VALUES(1,'foo'),(2,'bar'),(3,'foo bar')")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM ops WHERE MATCH(body) AGAINST('foo-bar' IN BOOLEAN MODE) ORDER BY id"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM ops WHERE MATCH(body) AGAINST('foo+bar' IN BOOLEAN MODE) ORDER BY id"
        ),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
}

#[test]
fn fulltext_alias_and_locking_read_bind_scores() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ft(id INT PRIMARY KEY,body TEXT,FULLTEXT(body))")
        .unwrap();
    engine
        .execute("INSERT INTO ft VALUES(1,'apple database'),(2,'orange database')")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT a.id FROM ft a JOIN ft b ON a.id=b.id WHERE MATCH(a.body) AGAINST('apple')"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM ft WHERE MATCH(body) AGAINST('apple') FOR UPDATE"
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn vector_key_width_and_comparison_validation() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        engine
            .execute("CREATE TABLE huge(v VECTOR(16383) PRIMARY KEY)")
            .unwrap_err()
            .code(),
        1071
    );
    engine
        .execute("CREATE TABLE v(id INT PRIMARY KEY,v VECTOR(2),KEY k(v))")
        .unwrap();
    engine
        .execute("INSERT INTO v VALUES(1,'[1,2]'),(2,'[3,4]')")
        .unwrap();
    for hint in ["FORCE INDEX(k)", "IGNORE INDEX(k)"] {
        assert_eq!(
            rows(&engine, &format!("SELECT id FROM v {hint} WHERE v='[1,2]'")),
            vec![vec![Value::Int(1)]]
        );
        assert!(engine
            .execute(&format!("SELECT id FROM v {hint} WHERE v='garbage'"))
            .and_then(|r| r.materialize())
            .is_err());
    }
}

#[test]
fn spatial_release_semantics_and_input_validation() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for function in ["ST_FrechetDistance", "ST_HausdorffDistance"] {
        for (srid, code) in [(0, 3704), (4326, 3618), (3857, 3704)] {
            let sql = format!("SELECT {function}(ST_GeomFromText('POINT(1 2)',{srid}),ST_GeomFromText('POINT(3 4)',{srid}))");
            assert_eq!(
                engine
                    .execute(&sql)
                    .and_then(|r| r.materialize())
                    .unwrap_err()
                    .code(),
                code,
                "{sql}"
            );
        }
        let sql = format!("SELECT {function}(POINT(1,2),POINT(3,4))");
        assert_eq!(
            engine
                .execute(&sql)
                .and_then(|r| r.materialize())
                .unwrap_err()
                .code(),
            3704
        );
    }
    for sql in [
        "SELECT ST_LATITUDE(ST_GeomFromText('POINT(1 2)',0))",
        "SELECT ST_PointFromGeoHash('',4326)",
        "SELECT ST_GeoHash(ST_GeomFromText('POINT(1 2)',3857),4)",
        "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,1e200 0,1e200 1e200,0 0))'))",
    ] {
        assert!(
            engine.execute(sql).and_then(|r| r.materialize()).is_err(),
            "{sql}"
        );
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT ST_AsText(ST_GeomFromText('GEOMETRYCOLLECTION()'))"
        )[0][0]
            .to_display_string(),
        "GEOMETRYCOLLECTION EMPTY"
    );
    assert_eq!(rows(&engine,"SELECT ST_Contains(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0,2 0,2 2,0 2,0 0)),POINT(5 5))'),ST_GeomFromText('MULTIPOINT((1 1),(5 5))'))")[0][0],Value::Int(1));
    assert_eq!(rows(&engine,"SELECT ST_Contains(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0,2 0,2 2,0 2,0 0)),POINT(5 5))'),ST_GeomFromText('POLYGON((1 1,4 1,4 4,1 4,1 1))'))")[0][0],Value::Int(0));
    engine
        .execute("CREATE TABLE geo(id INT PRIMARY KEY,g GEOMETRY NOT NULL)")
        .unwrap();
    engine.execute("CREATE INDEX k ON geo(g)").unwrap();
    let schema = engine.db_handle().table("test.geo").unwrap();
    assert_eq!(
        schema
            .indexes
            .iter()
            .find(|i| i.name == "k")
            .unwrap()
            .index_type,
        "BTREE"
    );
    engine
        .execute("INSERT INTO geo VALUES(1,ST_GeomFromText('POINT(1 2)'))")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM geo FORCE INDEX(k) WHERE g=ST_GeomFromText('POINT(1 2)')"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert!(engine
        .execute("CREATE SPATIAL INDEX bad ON geo(g,id)")
        .is_err());
}

#[test]
fn drop_view_if_exists_reports_each_missing_view() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE VIEW present AS SELECT 1 AS a")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "DROP VIEW IF EXISTS missing_a,present,missing_b",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected warnings")
    };
    assert_eq!(result.rows.len(), 2);
    for (row, name) in result.rows.iter().zip(["missing_a", "missing_b"]) {
        assert_eq!(row[0].to_display_string(), "Note");
        assert_eq!(row[1].to_display_string(), "1051");
        assert!(row[2].to_display_string().contains(name));
    }
    assert!(engine.execute("SELECT * FROM present").is_err());
    engine.close_session(&mut session).unwrap();
}

#[test]
fn view_string_expressions_keep_their_bound_widths() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE view_input(b VARCHAR(20) CHARACTER SET utf8mb4)")
        .unwrap();
    engine
        .execute("CREATE VIEW view_strings AS SELECT UPPER(b) u,REVERSE(b) r FROM view_input")
        .unwrap();
    let view = engine
        .db_handle()
        .view_definition("test", "view_strings")
        .unwrap();
    // Unicode case mappings can expand; retain the engine's proven bound
    // rather than copying MySQL's narrower twenty-character declaration.
    assert!(matches!(
        view.columns[0].data_type,
        Some(crate::storage::schema::DataTypeKind::VarChar { len: Some(60) })
    ));
    assert!(matches!(
        view.columns[1].data_type,
        Some(crate::storage::schema::DataTypeKind::VarChar { len: Some(20) })
    ));
    engine
        .execute("INSERT INTO view_input VALUES('ß')")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT u FROM view_strings"),
        vec![vec![Value::String("ß".into())]]
    );
}

#[test]
fn regex_preserves_binary_and_matches_sql_search_regions() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "SELECT REGEXP_LIKE(0xC0FF,'a')",
        "SELECT REGEXP_LIKE('a',0x61)",
        "SELECT REGEXP_INSTR(0xFF,'a')",
        "SELECT REGEXP_REPLACE('a','a',0x62)",
    ] {
        assert_eq!(
            engine
                .execute(sql)
                .and_then(|r| r.materialize())
                .unwrap_err()
                .code(),
            3995,
            "{sql}"
        );
    }
    for (sql, expected) in [
        ("SELECT REGEXP_INSTR(0xE28080,0x80)", "2"),
        ("SELECT HEX(REGEXP_SUBSTR(0xE28080,0x80))", "80"),
        ("SELECT HEX(REGEXP_REPLACE(0xC0FF,0xC0,0x58))", "58FF"),
        ("SELECT HEX(REGEXP_REPLACE(0xE28080,0x80,0x41))", "E24141"),
        ("SELECT REGEXP_REPLACE('abc','b*','-')", "-a--c-"),
        ("SELECT REGEXP_REPLACE('aba','a*','-')", "--b--"),
        ("SELECT REGEXP_REPLACE('','a*','-')", ""),
        ("SELECT REGEXP_REPLACE('abc','^b','X',2)", "abc"),
        ("SELECT REGEXP_REPLACE('aaa','a*','-',1,2)", "aaa-"),
        ("SELECT REGEXP_REPLACE('abc','$','X',4)", "abcX"),
        ("SELECT REGEXP_INSTR('abc','^b',2)", "2"),
        ("SELECT REGEXP_LIKE(0xC0,0xE0,'i')", "1"),
        (
            "SELECT REGEXP_LIKE(0xFF,CAST('[[:alpha:]]' AS BINARY))",
            "1",
        ),
        (
            "SELECT HEX(REGEXP_REPLACE(0x610d0a62,0x612e62,0x58,1,0,'n'))",
            "58",
        ),
        ("SELECT REGEXP_INSTR(0x610d,0x6124)", "1"),
        ("SELECT REGEXP_LIKE(0x618562,0x612e62)", "1"),
        (
            "SELECT HEX(REGEXP_REPLACE(0x80859F,CAST('(.*)' AS BINARY),0x2431))",
            "80859F",
        ),
        (
            "SELECT HEX(REGEXP_REPLACE(0x62,CAST('(?P<x>a)?b' AS BINARY),0x247b787d))",
            "",
        ),
        (
            "SELECT HEX(REGEXP_REPLACE(0xFF0D0AFF,CAST('^' AS BINARY),0x58,1,0,'m'))",
            "58FF0D0A58FF",
        ),
    ] {
        assert_eq!(
            rows(&engine, sql)[0][0].to_display_string(),
            expected,
            "{sql}"
        );
    }
    assert_eq!(
        rows(&engine, "SELECT REGEXP_SUBSTR('abc','^b',2)")[0][0],
        Value::Null
    );
    assert_eq!(
        rows(&engine, "SELECT REGEXP_SUBSTR('abc','$',4)")[0][0],
        Value::String(String::new())
    );
}

#[test]
fn regex_unicode_classes_and_line_terminators() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for class in ["digit", "alnum", "word", "xdigit", "graph", "print"] {
        assert_eq!(
            rows(&engine, &format!("SELECT REGEXP_LIKE('٣','[[:{class}:]]')"))[0][0],
            Value::Int(1),
            "{class}"
        );
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT REGEXP_LIKE('٣','[[:^{class}:]]')")
            )[0][0],
            Value::Int(0),
            "^{class}"
        );
    }
    for terminator in [
        "\n", "\r", "\r\n", "\u{b}", "\u{c}", "\u{85}", "\u{2028}", "\u{2029}",
    ] {
        let hex = terminator
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let text = format!("CONVERT(UNHEX('61{hex}62') USING utf8mb4)");
        for (pattern, flags, expected) in [
            ("a.b", "", 0),
            ("a.*b", "n", 1),
            ("^b", "m", 1),
            ("a$", "m", 1),
        ] {
            let sql = format!("SELECT REGEXP_LIKE({text},'{pattern}','{flags}')");
            assert_eq!(rows(&engine, &sql)[0][0], Value::Int(expected), "{sql}");
        }
        let text = format!("CONVERT(UNHEX('61{hex}') USING utf8mb4)");
        assert_eq!(
            rows(&engine, &format!("SELECT REGEXP_LIKE({text},'a$')"))[0][0],
            Value::Int(1),
            "{hex}"
        );
        assert_eq!(
            rows(&engine, &format!("SELECT REGEXP_INSTR({text},'$')"))[0][0],
            Value::Int(2),
            "{hex}"
        );
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT REGEXP_REPLACE({text},'^(a)$','$1X')")
            )[0][0]
                .to_display_string(),
            format!("aX{terminator}"),
            "{hex}"
        );
    }
}

#[test]
fn acknowledged_fulltext_delete_is_in_the_durable_wal() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        crate::storage::StorageConfig::from_env(),
    )
    .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE ft(id INT PRIMARY KEY,body TEXT,FULLTEXT(body))",
        )
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    for i in 1..=1500 {
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "INSERT INTO ft VALUES({i},'apple database entry {i} {}')",
                    "a".repeat(50)
                ),
            )
            .unwrap();
    }
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    engine
        .execute_with_session(&mut session, "DELETE FROM ft WHERE id>500")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM ft")
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(result.rows[0][0], Value::Int(500));
    let status = engine.db_handle().log_state_atomics();
    eprintln!(
        "WAL head={} durable={}",
        status.next_lsn(),
        status.durable_lsn()
    );
    let scan = crate::wal::LogManager::scan(dir.path()).unwrap();
    let begins = scan
        .records
        .iter()
        .filter(|r| matches!(r.payload, crate::wal::WalPayload::BeginTxn { .. }))
        .map(|r| r.txn_id)
        .collect::<std::collections::BTreeSet<_>>();
    let commits = scan
        .records
        .iter()
        .filter(|r| matches!(r.payload, crate::wal::WalPayload::CommitTxn { .. }))
        .map(|r| r.txn_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        begins, commits,
        "acknowledged writes must have durable decisions"
    );
    engine.shutdown().unwrap();
}

#[test]
fn before_triggers_observe_generated_columns_and_functions_report_missing_return() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE g(id INT PRIMARY KEY,v INT,g INT GENERATED ALWAYS AS(v*2) STORED)",
        "CREATE TABLE seen(n INT)",
        "CREATE TRIGGER bi BEFORE INSERT ON g FOR EACH ROW INSERT INTO seen VALUES(NEW.g)",
        "INSERT INTO g(id,v) VALUES(1,7)",
        "CREATE TRIGGER bu BEFORE UPDATE ON g FOR EACH ROW INSERT INTO seen VALUES(NEW.g)",
        "UPDATE g SET v=9 WHERE id=1",
    ] {
        engine.execute(sql).unwrap();
    }
    assert_eq!(
        rows(&engine, "SELECT n FROM seen ORDER BY n"),
        vec![vec![Value::Int(14)], vec![Value::Int(18)]]
    );
    engine.execute("CREATE FUNCTION missing_return() RETURNS INT DETERMINISTIC BEGIN IF 0 THEN RETURN 1; END IF; END").unwrap();
    assert_eq!(
        engine
            .execute("SELECT missing_return()")
            .unwrap_err()
            .code(),
        1321
    );
}

#[test]
fn partitioned_fulltext_locking_read_uses_physical_row_identity() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE ft(id INT PRIMARY KEY,body TEXT,FULLTEXT(body)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine.execute("INSERT INTO ft VALUES(1,'apple database'),(2,'orange database'),(3,'apple transaction')").unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM ft WHERE MATCH(body) AGAINST('apple') ORDER BY id FOR UPDATE"
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(3)]]
    );
}

#[test]
fn regex_scoped_flags_comments_and_unix_eof_keep_their_meaning() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for (pattern, expected) in [
        ("(?s)a(?-i).b", 1),
        ("(?s)a.(b)", 1),
        ("(?s:a.b)", 1),
        ("(?x)a # [\n . b", 0),
        ("(?s)a(?-s:.b)", 0),
    ] {
        let sql = format!(
            "SELECT REGEXP_LIKE(CONVERT(0x610D62 USING utf8mb4),'{}')",
            pattern.replace('\\', "\\\\").replace('\'', "''")
        );
        assert_eq!(rows(&engine, &sql)[0][0], Value::Int(expected), "{pattern}");
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT REGEXP_REPLACE(CONVERT(0x610A USING utf8mb4),'^','- ',1,0,'mu')"
        )[0][0]
            .to_display_string(),
        "- a\n- "
    );
}

#[test]
fn fulltext_backfill_preserves_the_same_word_positions_as_dml() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ft(id INT PRIMARY KEY,body TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO ft VALUES(1,'abc the def'),(2,'abc def')")
        .unwrap();
    engine
        .execute("CREATE FULLTEXT INDEX f ON ft(body)")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM ft WHERE MATCH(body) AGAINST('\"abc def\"' IN BOOLEAN MODE)"
        ),
        vec![vec![Value::Int(2)]]
    );
    engine
        .execute("UPDATE ft SET body='abc def' WHERE id=1")
        .unwrap();
    assert_eq!(rows(&engine,"SELECT id FROM ft WHERE MATCH(body) AGAINST('\"abc def\"' IN BOOLEAN MODE) ORDER BY id"),vec![vec![Value::Int(1)],vec![Value::Int(2)]]);
}

#[test]
fn partitioned_spatial_add_rejects_unsupported_backend_and_preserves_copy() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE pg(id INT PRIMARY KEY,g GEOMETRY NOT NULL) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO pg VALUES(1,POINT(1,1)),(2,POINT(2,2))")
        .unwrap();
    for sql in [
        "CREATE SPATIAL INDEX s ON pg(g)",
        "ALTER TABLE pg ADD SPATIAL INDEX s(g)",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1235);
        let schema = engine.db_handle().table("test.pg").unwrap();
        assert!(!schema.indexes.iter().any(|i| i.name == "s"));
        assert!(schema.column_index("extra").is_err());
    }
    // COPY constructs fresh partition-local files through the inline path;
    // it already supports this shape and must not be rejected globally.
    engine
        .execute("ALTER TABLE pg ADD COLUMN extra INT,ADD SPATIAL INDEX s(g)")
        .unwrap();
    let schema = engine.db_handle().table("test.pg").unwrap();
    assert!(schema
        .indexes
        .iter()
        .any(|i| i.name == "s" && i.is_spatial()));
    assert!(schema.column_index("extra").is_ok());
    assert_eq!(rows(&engine,"SELECT id FROM pg WHERE MBRContains(ST_GeomFromText('POLYGON((0 0,3 0,3 3,0 3,0 0))'),g) ORDER BY id"),vec![vec![Value::Int(1)],vec![Value::Int(2)]]);
}
