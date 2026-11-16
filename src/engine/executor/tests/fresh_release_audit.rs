use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

fn result(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> ResultSet {
    let response = engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("{sql}: expected result set")
    };
    result
}

#[test]
fn fresh_audit_timestamp_numeric_domain_and_precision() {
    let (_dir, engine, mut session) = setup();
    let rs = result(&engine, &mut session, "SELECT TIMESTAMP('2024-02-29 12:00:00')+0,TIMESTAMP('2024-02-29 12:00:00')=20240229120000,TIMESTAMP('2024-02-29','12:00:00')+0");
    assert_eq!(
        rs.rows[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        ["20240229120000", "1", "20240229120000"]
    );
    engine
        .execute("CREATE TABLE t(id INT,dt DATETIME(6))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'2024-02-29 12:00:00.123456'),(2,NULL)")
        .unwrap();
    let rs = result(
        &engine,
        &mut session,
        "SELECT id FROM t WHERE TIMESTAMP(dt)=20240229120000.123456",
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    let rs = result(
        &engine,
        &mut session,
        "SELECT TIMESTAMP(dt) FROM t ORDER BY id",
    );
    assert_eq!(rs.columns[0].column_type, ColumnType::DateTime);
    assert_eq!(rs.columns[0].source.decimals, Some(6));
    assert!(matches!(rs.rows[1][0], Value::Null));
}

#[test]
fn fresh_audit_binary_ranges_scan_and_index() {
    for data_type in ["BINARY(3)", "VARBINARY(3)", "BLOB"] {
        let (_dir, engine, mut session) = setup();
        engine
            .execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,b {data_type})"))
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,X'6161'),(2,X'6262'),(3,X'808080'),(4,NULL)")
            .unwrap();
        for indexed in [false, true] {
            if indexed {
                engine.execute("CREATE INDEX ib ON t(b(3))").unwrap();
            }
            for (predicate, expected) in [
                ("b<'abc'", vec![1]),
                ("'abc'>b", vec![1]),
                ("b>'abc'", vec![2, 3]),
                ("b BETWEEN 'aaa' AND 'ccc'", vec![2]),
            ] {
                let rs = result(
                    &engine,
                    &mut session,
                    &format!("SELECT id FROM t WHERE {predicate} ORDER BY id"),
                );
                assert_eq!(
                    rs.rows,
                    expected
                        .into_iter()
                        .map(|id| vec![Value::Int(id)])
                        .collect::<Vec<_>>(),
                    "{data_type} indexed={indexed} {predicate}"
                );
            }
        }
    }
}

#[test]
fn fresh_audit_binary_comparisons_preserve_character_operand_charset() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute(
            "CREATE TABLE t(id INT PRIMARY KEY,b VARBINARY(4),c VARCHAR(4) CHARACTER SET latin1)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,X'e9','é'),(2,X'd0','x')")
        .unwrap();
    for indexed in [false, true] {
        if indexed {
            engine.execute("CREATE INDEX ib ON t(b)").unwrap();
        }
        engine
            .execute_with_session(&mut session, "SET NAMES latin1")
            .unwrap();
        for (condition, expected) in [
            ("b > 'é'", vec![]),
            ("b < 'é'", vec![2]),
            ("'é' > b", vec![2]),
            ("b BETWEEN 'Ð' AND 'é'", vec![1, 2]),
            ("b IN ('é')", vec![1]),
            ("b > CHAR(233 USING latin1)", vec![]),
        ] {
            let rs = result(
                &engine,
                &mut session,
                &format!("SELECT id FROM t WHERE {condition} ORDER BY id"),
            );
            assert_eq!(
                rs.rows,
                expected
                    .into_iter()
                    .map(|id| vec![Value::Int(id)])
                    .collect::<Vec<_>>(),
                "indexed={indexed} {condition}"
            );
        }
        engine
            .execute_with_session(&mut session, "SET NAMES utf8mb4")
            .unwrap();
        let rs = result(
            &engine,
            &mut session,
            "SELECT id FROM t WHERE b=c ORDER BY id",
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    }
}

#[test]
fn fresh_audit_bit_aggregate_and_set_function_metadata() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute("CREATE TABLE t(v BIGINT UNSIGNED,s SET('a','b','c'),b VARBINARY(2))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(18446744073709551615,'a,c',X'8080'),(3,'b',X'0101')")
        .unwrap();
    for suffix in ["", " GROUP BY s"] {
        for function in ["BIT_OR", "BIT_XOR", "BIT_AND"] {
            let rs = result(
                &engine,
                &mut session,
                &format!("SELECT {function}(v) FROM t{suffix}"),
            );
            assert_eq!(rs.columns[0].column_type, ColumnType::LongLong);
            assert_ne!(
                rs.columns[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
                0,
                "{function}{suffix}: {:?}",
                rs.columns[0]
            );
            assert!(rs
                .rows
                .iter()
                .all(|r| matches!(r[0], Value::Int(_) | Value::UInt(_))));
            let binary = result(
                &engine,
                &mut session,
                &format!("SELECT {function}(b) FROM t{suffix}"),
            );
            assert!(matches!(binary.columns[0].column_type, ColumnType::Blob));
            assert!(binary.rows.iter().all(|r| matches!(r[0], Value::Bytes(_))));
        }
    }
    let rs = result(
        &engine,
        &mut session,
        "SELECT FIND_IN_SET('c',s) FROM t ORDER BY v DESC",
    );
    assert_eq!(rs.columns[0].column_type, ColumnType::LongLong);
    assert_eq!(rs.rows, vec![vec![Value::Int(3)], vec![Value::Int(0)]]);
}

#[test]
fn fresh_audit_isolation_aliases_and_prepared_xa_one_phase() {
    let (_dir, engine, mut session) = setup();
    for (input, output) in [
        ("READ UNCOMMITTED", "READ-UNCOMMITTED"),
        ("read committed", "READ-COMMITTED"),
        ("REPEATABLE READ", "REPEATABLE-READ"),
        ("SERIALIZABLE", "SERIALIZABLE"),
    ] {
        engine
            .execute_with_session(
                &mut session,
                &format!("SET transaction_isolation='{input}'"),
            )
            .unwrap();
        let rs = result(&engine, &mut session, "SELECT @@transaction_isolation");
        assert_eq!(rs.rows, vec![vec![Value::String(output.into())]]);
    }
    for sql in ["XA START 'fresh'", "XA END 'fresh'", "XA PREPARE 'fresh'"] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let err = engine
        .execute_with_session(&mut session, "XA COMMIT 'fresh' ONE PHASE")
        .unwrap_err();
    assert_eq!(err.code(), 1399);
    assert_eq!(
        result(&engine, &mut session, "XA RECOVER CONVERT XID")
            .rows
            .len(),
        1
    );
    engine
        .execute_with_session(&mut session, "XA ROLLBACK 'fresh'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "XA COMMIT 'absent' ONE PHASE")
            .unwrap_err()
            .code(),
        1397
    );
}

#[test]
fn fresh_audit_time_bounds_and_wkt_text_domain() {
    let (_dir, engine, mut session) = setup();
    let rs = result(&engine, &mut session, "SELECT CAST('850:00:00' AS TIME),CAST('-850:00:00' AS TIME(6)),CAST('838:59:59.999999' AS TIME(6)),CAST('26:00:00.123456' AS TIME(6))");
    let limit = (838 * 3600 + 59 * 60 + 59) * 1_000_000i64;
    assert_eq!(
        rs.rows[0],
        vec![
            Value::Time(limit),
            Value::Time(-limit),
            Value::Time(limit),
            Value::Time(26 * 3600 * 1_000_000 + 123456)
        ]
    );
    assert_eq!(result(&engine, &mut session, "SHOW WARNINGS").rows.len(), 3);
    let large = result(
        &engine,
        &mut session,
        "SELECT CAST('4294967295:00:00' AS TIME(6)),CAST('-4294967295:00:00' AS TIME)",
    );
    assert_eq!(large.rows[0], vec![Value::Time(limit), Value::Time(-limit)]);
    for function in ["ST_AsText", "ST_AsWKT"] {
        let rs = result(
            &engine,
            &mut session,
            &format!("SELECT {function}(POINT(1,2))"),
        );
        assert_eq!(rs.rows, vec![vec![Value::String("POINT(1 2)".into())]]);
        assert_ne!(
            rs.columns[0].source.collation_id,
            Some(crate::charset::BINARY_COLLATION_ID)
        );
    }
}
