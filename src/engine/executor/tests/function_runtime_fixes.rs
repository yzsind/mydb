use super::*;

fn row(engine: &StorageQueryEngine, sql: &str) -> Vec<Value> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected a result set: {sql}");
    };
    result.rows[0].clone()
}

#[test]
fn timediff_preserves_date_and_datetime_kinds() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "SELECT TIMEDIFF(CAST('2020-01-02 12:00:00' AS DATETIME),CAST('2020-01-02' AS DATE))",
        "SELECT TIMEDIFF(CAST('2020-01-02' AS DATE),CAST('2020-01-02 12:00:00' AS DATETIME))",
    ] {
        assert_eq!(row(&engine, sql), vec![Value::Null], "{sql}");
    }
    assert_eq!(
        row(
            &engine,
            "SELECT TIMEDIFF(CAST('2020-01-02' AS DATE),CAST('2020-01-01' AS DATE))"
        ),
        vec![Value::Time(86_400_000_000)]
    );
    assert_eq!(row(&engine,"SELECT TIMEDIFF(CAST('2020-01-02' AS DATE),'2020-01-01 '),TIMEDIFF(' 2020-01-02',CAST('2020-01-01' AS DATE)),TIMEDIFF(CAST('2020-01-02 00:00:00' AS DATETIME),'2020-01-01 ')") ,vec![Value::Time(86_400_000_000),Value::Time(86_400_000_000),Value::Null]);
    engine
        .execute("CREATE TABLE td(d DATE,dt DATETIME(6),ts TIMESTAMP(6))")
        .unwrap();
    engine.execute("INSERT INTO td VALUES('2020-01-02','2020-01-02 12:00:00.123456','2040-01-02 12:00:00.123456')").unwrap();
    assert_eq!(
        row(
            &engine,
            "SELECT TIMEDIFF(dt,d),TIMEDIFF(d,dt),TIMEDIFF(ts,d) FROM td"
        ),
        vec![Value::Null; 3]
    );
    assert_eq!(
        row(&engine, "SELECT YEAR(ts) FROM td"),
        vec![Value::Int(2040)]
    );
}

#[test]
fn geometry_constructors_preserve_typed_null_and_reject_non_geometry() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE gn(g GEOMETRY)").unwrap();
    engine.execute("INSERT INTO gn VALUES(NULL)").unwrap();
    for name in [
        "POLYGON",
        "LINESTRING",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
        "GEOMCOLLECTION",
    ] {
        for argument in ["ST_GEOMFROMTEXT(NULL)", "g"] {
            let from = if argument == "g" { " FROM gn" } else { "" };
            let sql = format!("SELECT {name}({argument}){from}");
            assert_eq!(row(&engine, &sql), vec![Value::Null], "{sql}");
        }
        let error = engine.execute(&format!("SELECT {name}(NULL)")).unwrap_err();
        assert_eq!(error.code(), 1367, "{name}");
        let error = engine
            .execute(&format!("SELECT {name}(ST_GEOMFROMTEXT(NULL),'bad')"))
            .unwrap_err();
        assert_eq!(error.code(), 1367, "{name}");
    }
}

#[test]
fn deadline_interrupts_stateful_projection_and_session_recovers() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 4096);
    engine
        .execute("CREATE TABLE cr(id BIGINT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO cr SELECT n FROM generate_series(1,100000) AS s(n)")
        .unwrap();
    let mut session = SessionContext::default();
    for hint in ["NO_PARALLEL", "PARALLEL(2)"] {
        engine
            .execute_with_session(&mut session, "DROP SEQUENCE IF EXISTS cancel_seq")
            .unwrap();
        engine
            .execute_with_session(&mut session, "CREATE SEQUENCE cancel_seq")
            .unwrap();
        engine
            .execute_with_session(&mut session, "SET max_execution_time=10")
            .unwrap();
        let started = std::time::Instant::now();
        let error = engine
            .execute_with_session(
                &mut session,
                &format!("SELECT /*+ {hint} */ SETVAL('cancel_seq',id) FROM cr"),
            )
            .and_then(QueryResponse::materialize)
            .unwrap_err();
        assert_eq!(error.code(), 3024, "{hint}");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{hint}: cancellation took {:?}",
            started.elapsed()
        );
        engine
            .execute_with_session(&mut session, "SET max_execution_time=0")
            .unwrap();
        assert!(engine
            .execute_with_session(&mut session, "SELECT 1")
            .is_ok());
    }
}

#[test]
fn format_small_units_and_extended_timestamp_range() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(row(&engine,"SELECT FORMAT_BYTES(0),FORMAT_BYTES(1.9),FORMAT_BYTES(-1),FORMAT_BYTES(1023),FORMAT_BYTES(1024),FORMAT_PICO_TIME(0),FORMAT_PICO_TIME(1.9),FORMAT_PICO_TIME(-1),FORMAT_PICO_TIME(1000)"),["   0 bytes","   1 bytes","  -1 bytes","1023 bytes","1.00 KiB","  0 ps","  1 ps"," -1 ps","1.00 ns"].map(|s|Value::String(s.into())).to_vec());
    engine
        .execute("CREATE TABLE future(ts TIMESTAMP(6))")
        .unwrap();
    engine
        .execute("INSERT INTO future VALUES('9999-12-31 23:59:59.123456')")
        .unwrap();
    assert_eq!(
        row(&engine, "SELECT YEAR(ts),MICROSECOND(ts) FROM future"),
        vec![Value::Int(9999), Value::Int(123456)]
    );
    assert_eq!(
        row(&engine, "SELECT MD5('abc'),SHA1('abc'),SHA2('abc',256)"),
        [
            "900150983cd24fb0d6963f7d28e17f72",
            "a9993e364706816aba3e25717850c26c9cd0d89d",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        ]
        .map(|s| Value::String(s.into()))
        .to_vec()
    );
}

#[test]
fn parallel_projection_matches_serial_and_releases_workers_on_close() {
    let _tuning = super::parallel::auto_parallel_test_tuning();
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 4096);
    engine.execute("CREATE TABLE pp(id BIGINT PRIMARY KEY,s VARCHAR(128),b VARBINARY(128),d DATE) ZONE_MAP=OFF").unwrap();
    engine.execute("INSERT INTO pp SELECT n,IF(n%19=0,NULL,CONCAT('中文 a',n)),IF(n%23=0,NULL,X'FF00C3'),DATE '2024-02-29' FROM generate_series(1,65537) AS s(n)").unwrap();
    engine.execute("ANALYZE TABLE pp").unwrap();
    crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::On,
        || {
            let expressions="id,id AS duplicate_id,MD5(s),SHA1(b),SHA2(REVERSE(s),512),CHAR_LENGTH(UPPER(s)),YEAR(d)*100+MONTH(d)";
            let mut serial = rows(
                &engine,
                &format!("SELECT /*+ NO_PARALLEL FULL_SCAN(pp) */ {expressions} FROM pp"),
            );
            for predicate in ["", " WHERE id%7=0"] {
                let mut expected = if predicate.is_empty() {
                    serial.clone()
                } else {
                    rows(&engine,&format!("SELECT /*+ NO_PARALLEL FULL_SCAN(pp) */ {expressions} FROM pp{predicate}"))
                };
                let query = format!(
                    "SELECT /*+ PARALLEL(2) FULL_SCAN(pp) */ {expressions} FROM pp{predicate}"
                );
                let mut actual = rows(&engine, &query);
                let key = |row: &Vec<Value>| {
                    if let Value::Int(id) = row[0] {
                        id
                    } else {
                        panic!("missing id")
                    }
                };
                expected.sort_by_key(key);
                actual.sort_by_key(key);
                assert_eq!(actual, expected);
                let text = rows(&engine, &format!("EXPLAIN ANALYZE {query}"))
                    .into_iter()
                    .flatten()
                    .map(|v| v.to_display_string())
                    .collect::<Vec<_>>()
                    .join("\n");
                if predicate.is_empty() {
                    assert!(
                        text.contains("expression_mode=parallel_column")
                            && text.contains("expression_workers=2"),
                        "{text}"
                    );
                } else {
                    // The modulo predicate retains its diagnostic-capable coordinator lane.
                    assert!(!text.contains("expression_mode=parallel_column"), "{text}");
                }
            }
            serial.clear();
            for sql in [
                "SELECT /*+ PARALLEL(2) FULL_SCAN(pp) */ id,SHA2(s,123) FROM pp LIMIT 3",
                "SELECT /*+ PARALLEL(2) FULL_SCAN(pp) */ id,MD5(s) FROM pp ORDER BY id LIMIT 3",
            ] {
                let text = rows(&engine, &format!("EXPLAIN ANALYZE {sql}"))
                    .into_iter()
                    .flatten()
                    .map(|v| v.to_display_string())
                    .collect::<Vec<_>>()
                    .join("\n");
                assert!(!text.contains("expression_mode=parallel_column"), "{text}");
            }
            for _ in 0..3 {
                let response = engine
                    .execute_streaming_for_test(
                        "SELECT /*+ PARALLEL(2) FULL_SCAN(pp) */ SHA2(s,512) FROM pp",
                    )
                    .unwrap();
                let QueryResponse::StreamingResultSet(mut stream) = response else {
                    panic!("expected streaming projection")
                };
                assert!(stream.cursor.next_row().unwrap().is_some());
                let start = std::time::Instant::now();
                drop(stream);
                assert!(start.elapsed() < Duration::from_secs(1));
                assert_eq!(
                    crate::util::exec_permit::status_snapshot().parallel_read_pressure_active,
                    0
                );
            }
        },
    );
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected rows: {sql}")
    };
    result.rows
}

#[test]
fn digest_conversions_retain_declared_character_octets() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    // Independently verified with native MySQL 8.0.46 hashes of E9 / 006500E9.
    assert_eq!(row(&engine,"SELECT MD5(_latin1 X'E9'),SHA1(CONVERT('é' USING latin1)),SHA2(_utf16 X'006500E9',256)"),[
        "3406877694691ddd1dfb0aca54681407",
        "1599e9fa41ec68c80230491902786bee889f5bcb",
        "8220717f956ed6dffa4b320951fc95b9954906be20b435829d706d8950bded86",
    ].map(|s|Value::String(s.into())).to_vec());
    engine
        .execute("CREATE TABLE enc(s VARCHAR(8) CHARACTER SET latin1)")
        .unwrap();
    engine
        .execute("INSERT INTO enc VALUES('é'),(NULL)")
        .unwrap();
    assert_eq!(row(&engine,"SELECT MD5(CONVERT(s USING latin1)),SHA1(CONVERT(s USING latin1)) FROM enc WHERE s IS NOT NULL"),[
        "3406877694691ddd1dfb0aca54681407",
        "1599e9fa41ec68c80230491902786bee889f5bcb",
    ].map(|s|Value::String(s.into())).to_vec());
}

#[test]
fn optional_scan_worker_transfer_declines_without_consuming_or_poisoning_input() {
    let _tuning = super::parallel::auto_parallel_test_tuning();
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 4096);
    engine
        .execute("CREATE TABLE transfer_input(id BIGINT PRIMARY KEY,s VARCHAR(32)) ZONE_MAP=OFF")
        .unwrap();
    engine
        .execute(
            "INSERT INTO transfer_input SELECT n,'hello' FROM generate_series(1,8193) AS gs(n)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE transfer_input").unwrap();
    let crate::parser::Statement::Select(select) = crate::parser::parse_statement(
        "SELECT /*+ PARALLEL(2) FULL_SCAN(transfer_input) */ id,s FROM transfer_input",
    )
    .unwrap() else {
        unreachable!()
    };
    let db = engine.db_for_tests();
    let planned = crate::planner::plan_select(db, &select).unwrap();
    let txn = db.begin_read_only_txn().unwrap();
    let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
        64 * 1024 * 1024,
    ));
    crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
        crate::engine::executor::vec::config::with_vec_mode(
            crate::engine::executor::vec::config::VecMode::On,
            || {
                let mut input =
                    crate::engine::executor::select::vec_bridge::try_native_result_batch_input(
                        db,
                        txn,
                        &planned.physical,
                        &select.projection,
                        &[],
                    )
                    .unwrap()
                    .unwrap();
                assert!(input.stream.is_native_parallel_scan());
                crate::engine::executor::mem_budget::with_test_effective_memory_limit(
                    1024 * 1024,
                    || {
                        assert!(input.stream.take_parallel_workers(2).unwrap().is_none());
                        assert!(budget.terminal_oom().is_none());
                        crate::engine::cancel::check_cancelled().unwrap();
                    },
                );
                let mut ids = Vec::new();
                while let Some(batch) = input.stream.next_batch().unwrap() {
                    ids.extend(batch.visible_indices().map(|row| {
                        batch.columns()[0]
                            .value_at(row)
                            .to_display_string()
                            .parse::<i64>()
                            .unwrap()
                    }));
                }
                ids.sort_unstable();
                assert_eq!(ids, (1..=8193).collect::<Vec<_>>());
            },
        );
    });
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn bound_column_charset_metadata_keeps_nulls_declared_collations_and_scope_errors() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE cm(id INT, s VARCHAR(30) CHARACTER SET latin1 COLLATE latin1_bin,b VARBINARY(30),n BIGINT,d DATE)").unwrap();
    engine
        .execute("INSERT INTO cm VALUES(1,NULL,NULL,NULL,NULL),(2,'abc',X'FF',123,'2024-02-29')")
        .unwrap();
    let sql="SELECT CHARSET(s),COLLATION(s),CHARSET(b),COLLATION(b),CHARSET(n),COLLATION(n),CHARSET(d),COLLATION(d) FROM cm ORDER BY id";
    let expected = [
        "latin1",
        "latin1_bin",
        "binary",
        "binary",
        "binary",
        "binary",
        "binary",
        "binary",
    ]
    .map(|s| Value::String(s.into()))
    .to_vec();
    for mode in [
        crate::engine::executor::vec::config::VecMode::On,
        crate::engine::executor::vec::config::VecMode::Off,
    ] {
        crate::engine::executor::vec::config::with_vec_mode(mode, || {
            assert_eq!(rows(&engine, sql), vec![expected.clone(), expected.clone()]);
            for sql in [
                "SELECT CHARSET(missing) FROM cm",
                "SELECT COLLATION(missing) FROM cm",
            ] {
                assert_eq!(engine.execute(sql).unwrap_err().code(), 1054);
            }
            // Computed operands retain evaluation, even though their result's
            // metadata could otherwise be known before execution.
            assert!(engine
                .execute("SELECT CHARSET(UUID_TO_BIN('invalid')) FROM cm")
                .is_err());
        });
    }
}

#[test]
fn json_column_prepared_paths_keep_parallel_parameters_and_scalar_errors() {
    let _tuning = super::parallel::auto_parallel_test_tuning();
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 4096);
    engine
        .execute("CREATE TABLE json_paths(id BIGINT PRIMARY KEY,j JSON)")
        .unwrap();
    engine.execute("INSERT INTO json_paths SELECT n,IF(n%19=0,NULL,JSON_OBJECT('a',n,'b',JSON_ARRAY('文',n))) FROM generate_series(1,65537) AS s(n)").unwrap();
    engine.execute("ANALYZE TABLE json_paths").unwrap();
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session,"PREPARE jp FROM 'SELECT /*+ PARALLEL(2) FULL_SCAN(json_paths) */ id,JSON_UNQUOTE(JSON_EXTRACT(j,?)) FROM json_paths'").unwrap();
    for path in ["$.a", "$.b[0]", "$.missing", "$", "$.a"] {
        engine
            .execute_with_session(&mut session, &format!("SET @jp_path='{path}'"))
            .unwrap();
        let mut expected = crate::engine::executor::vec::config::with_vec_mode(
            crate::engine::executor::vec::config::VecMode::Off,
            || {
                rows(&engine,&format!("SELECT /*+ NO_PARALLEL FULL_SCAN(json_paths) */ id,JSON_UNQUOTE(JSON_EXTRACT(j,'{path}')) FROM json_paths"))
            },
        );
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, "EXECUTE jp USING @jp_path")
            .and_then(QueryResponse::materialize)
            .unwrap()
        else {
            panic!("expected rows")
        };
        let mut actual = result.rows;
        let key = |r: &Vec<Value>| {
            if let Value::Int(n) = r[0] {
                n
            } else {
                panic!("id")
            }
        };
        expected.sort_by_key(key);
        actual.sort_by_key(key);
        assert_eq!(actual, expected, "{path}");
    }
    engine
        .execute_with_session(&mut session, "SET @jp_path='bad'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "EXECUTE jp USING @jp_path")
            .and_then(QueryResponse::materialize)
            .unwrap_err()
            .code(),
        3143
    );
    engine
        .execute_with_session(&mut session, "SET @jp_path='$.a'")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut session, "EXECUTE jp USING @jp_path")
        .and_then(QueryResponse::materialize)
        .is_ok());
    for sql in [
        "SELECT JSON_EXTRACT(CAST(NULL AS JSON),'bad')",
        "SELECT JSON_EXTRACT(NULL,'bad')",
    ] {
        assert_eq!(row(&engine, sql), vec![Value::Null]);
    }
    assert_eq!(
        engine
            .execute("SELECT JSON_EXTRACT('invalid','bad')")
            .unwrap_err()
            .code(),
        3141
    );
}
