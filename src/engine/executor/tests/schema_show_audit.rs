use super::*;

fn query(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> ResultSet {
    match engine.execute_with_session(session, sql).unwrap() {
        QueryResponse::ResultSet(result) => result,
        other => panic!("expected materialized result for {sql}: {other:?}"),
    }
}

#[test]
fn schema_show_audit_ddl_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session,
        "CREATE TABLE `rt``x` (`c``1` INT PRIMARY KEY, v VARBINARY(8) DEFAULT X'ff0041', e ENUM('a\u{1}b','c'), s VARCHAR(20), KEY `k``x` ((LOWER(s))))").unwrap();
    let before = engine.db.table("test.rt`x").unwrap();
    let ddl =
        query(&engine, &mut session, "SHOW CREATE TABLE `rt``x`").rows[0][1].to_display_string();
    for _ in 0..3 {
        engine
            .execute_with_session(&mut session, "DROP TABLE `rt``x`")
            .unwrap();
        engine.execute_with_session(&mut session, &ddl).unwrap();
        let after = engine.db.table("test.rt`x").unwrap();
        assert_eq!(before.columns[1].default, after.columns[1].default);
        assert_eq!(before.columns[2].data_type, after.columns[2].data_type);
        assert_eq!(
            ddl,
            query(&engine, &mut session, "SHOW CREATE TABLE `rt``x`").rows[0][1]
                .to_display_string()
        );
    }
}

#[test]
fn schema_show_audit_prepare_and_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    let bits = session.temporal_sql_mode_bits;
    assert!(engine
        .describe_prepared_statement(&mut session, "SET sql_mode=''")
        .unwrap()
        .is_empty());
    assert_eq!(session.sql_mode, "STRICT_TRANS_TABLES");
    assert_eq!(session.temporal_sql_mode_bits, bits);
    engine
        .execute_with_session(&mut session, "SET sql_mode='ERROR_FOR_DIVISION_BY_ZERO'")
        .unwrap();
    query(&engine, &mut session, "SELECT 1/0");
    assert_eq!(
        query(&engine, &mut session, "SHOW COUNT(*) WARNINGS").rows[0][0].to_display_string(),
        "1"
    );
    assert_eq!(
        query(&engine, &mut session, "SELECT @@warning_count").rows[0][0].to_display_string(),
        "1"
    );
    assert!(query(&engine, &mut session, "SHOW WARNINGS")
        .rows
        .is_empty());
}

#[test]
fn schema_show_audit_wire_domains() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE wire_t (a BINARY(16), b VARBINARY(32), t TINYTEXT, l LONGTEXT, bval BOOL, tm TIME(3), dt DATETIME(6), ts TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), i INT, j INT, PRIMARY KEY(i,j))").unwrap();
    let table = engine.db.table("test.wire_t").unwrap();
    let expected = [
        (254, 16, 63),
        (253, 32, 63),
        (252, 1020, 255),
        (252, u32::MAX, 255),
        (1, 1, 63),
        (11, 14, 63),
        (12, 26, 63),
    ];
    for (i, (kind, len, charset)) in expected.into_iter().enumerate() {
        let packet = crate::mysql::protocol::column_definition_packet_full(
            "x",
            table.result_type(i),
            &table.wire_column_source(i),
        );
        let tail = &packet[packet.len() - 12..];
        assert_eq!(tail[6], kind, "column {i}");
        assert_eq!(u32::from_le_bytes(tail[2..6].try_into().unwrap()), len);
        assert_eq!(u16::from_le_bytes(tail[0..2].try_into().unwrap()), charset);
    }
    use crate::mysql::protocol::*;
    assert_eq!(
        table.wire_column_source(7).flags & (TIMESTAMP_FLAG | ON_UPDATE_NOW_FLAG),
        TIMESTAMP_FLAG | ON_UPDATE_NOW_FLAG
    );
    assert_ne!(table.wire_column_source(9).flags & PRI_KEY_FLAG, 0);
}

#[test]
fn schema_show_audit_full_tables_enhancement_and_view_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine.execute("CREATE TABLE t (i INT)").unwrap();
    engine
        .execute("CREATE VIEW v AS SELECT i*2 AS product FROM t")
        .unwrap();
    let full = query(&engine, &mut session, "SHOW FULL TABLES");
    assert_eq!(full.columns.len(), 7);
    assert!(full.rows.iter().all(|row| row.len() == 7));
    assert_eq!(
        query(&engine, &mut session, "SHOW COLUMNS FROM v").rows[0][1],
        Value::String("bigint".into())
    );
    assert_eq!(
        query(&engine, &mut session, "SHOW TABLE STATUS LIKE 'v'").rows[0][16],
        Value::Null
    );
}

#[test]
fn schema_show_audit_binary_charset_and_precision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE bin_t (a CHAR(4) CHARACTER SET binary, b VARCHAR(8) CHARACTER SET binary, c TEXT CHARACTER SET binary)").unwrap();
    let table = engine.db.table("test.bin_t").unwrap();
    assert!(matches!(
        table.columns[0].data_type,
        DataTypeKind::Binary { len: Some(4) }
    ));
    assert!(matches!(
        table.columns[1].data_type,
        DataTypeKind::VarBinary { len: Some(8) }
    ));
    assert!(matches!(
        table.columns[2].data_type,
        DataTypeKind::Blob { .. }
    ));
    engine
        .execute(
            "CREATE TABLE bin_default (v TEXT CHARACTER /*charset*/ SET binary DEFAULT (X'ff'))",
        )
        .unwrap();
    engine.execute("INSERT INTO bin_default VALUES ()").unwrap();
    let mut session = SessionContext::default();
    assert_eq!(
        query(&engine, &mut session, "SELECT HEX(v) FROM bin_default").rows[0][0]
            .to_display_string(),
        "FF"
    );
    for kind in ["TIME", "DATETIME", "TIMESTAMP"] {
        let err = engine
            .execute(&format!("CREATE TABLE bad (v {kind}(7))"))
            .unwrap_err();
        assert_eq!(err.code(), 1426);
    }
}

#[test]
fn schema_show_audit_aggregate_domains_are_bound() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    engine
        .execute("CREATE TABLE ag (i INT,a DECIMAL(10,2),v VARCHAR(10))")
        .unwrap();
    for populated in [false, true] {
        if populated {
            engine
                .execute("INSERT INTO ag VALUES (1,1.25,'abc'),(2,2.50,'def')")
                .unwrap();
        }
        let plain = query(&engine, &mut session, "SELECT a,i,i+0 FROM ag");
        let aggregate = query(&engine, &mut session, "SELECT a,i,i+0,COUNT(*) FROM ag");
        for (left, right) in plain.columns.iter().zip(&aggregate.columns) {
            assert_eq!(left.column_type, right.column_type);
        }
        for suffix in ["", " LIMIT 1"] {
            let result = query(
                &engine,
                &mut session,
                &format!("SELECT i,COUNT(*),SUM(i),MAX(v) FROM ag GROUP BY i{suffix}"),
            );
            for i in [1, 2] {
                let column = &result.columns[i];
                let packet = crate::mysql::protocol::column_definition_packet_full(
                    &column.name,
                    column.column_type,
                    &column.source,
                );
                let tail = &packet[packet.len() - 12..];
                assert_eq!(u16::from_le_bytes(tail[..2].try_into().unwrap()), 63);
                assert_eq!(tail[9], 0);
            }
            assert_eq!(result.columns[3].source.decimals.unwrap_or(0), 0);
        }
    }
}
