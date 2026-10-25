use super::*;

#[test]
fn top_level_sql_prepared_statement_lifecycle_and_typed_binding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET @p = 7")
        .unwrap();
    engine
        .execute_with_session(&mut session, "PREPARE s FROM 'SELECT ? + 1 AS v'")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "EXECUTE s USING @p")
        .unwrap()
    else {
        panic!("expected prepared SELECT result");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(8)]]);

    let err = engine
        .execute_with_session(&mut session, "EXECUTE s")
        .unwrap_err();
    assert_eq!(err.code(), 1210, "{}", err.message());

    engine
        .execute_with_session(&mut session, "DEALLOCATE PREPARE s")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "EXECUTE s USING @p")
        .unwrap_err();
    assert_eq!(err.code(), 1243, "{}", err.message());
}

#[test]
fn sql_prepare_shares_global_quota_and_releases_on_failure_and_reset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let initial = crate::engine::status_counters::prepared_stmt_count();
    engine
        .execute_with_session(&mut session, "SET GLOBAL max_prepared_stmt_count = 2")
        .unwrap();

    engine
        .execute_with_session(&mut session, "PREPARE first FROM 'SELECT 1'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "PREPARE second FROM 'SELECT 2'")
        .unwrap();
    assert_eq!(
        crate::engine::status_counters::prepared_stmt_count(),
        initial + 2
    );
    let worker_snapshot = session.clone();
    assert_eq!(
        crate::engine::status_counters::prepared_stmt_count(),
        initial + 2
    );
    drop(worker_snapshot);
    assert_eq!(
        crate::engine::status_counters::prepared_stmt_count(),
        initial + 2
    );
    let err = engine
        .execute_with_session(&mut session, "PREPARE third FROM 'SELECT 3'")
        .unwrap_err();
    assert_eq!(err.code(), 1461);
    engine
        .execute_with_session(&mut session, "PREPARE second FROM 'SELECT 4'")
        .unwrap();
    assert_eq!(
        crate::engine::status_counters::prepared_stmt_count(),
        initial + 2
    );

    let err = engine
        .execute_with_session(&mut session, "PREPARE first FROM 'invalid sql'")
        .unwrap_err();
    assert_eq!(err.code(), 1064);
    assert_eq!(
        crate::engine::status_counters::prepared_stmt_count(),
        initial + 1
    );
    engine
        .execute_with_session(&mut session, "PREPARE third FROM 'SELECT 3'")
        .unwrap();
    engine.reset_session(&mut session).unwrap();
    assert_eq!(
        crate::engine::status_counters::prepared_stmt_count(),
        initial
    );
    engine
        .execute_with_session(&mut session, "SET GLOBAL max_prepared_stmt_count = 1000000")
        .unwrap();
}

#[test]
fn sql_execute_reuses_prepare_time_result_domain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET @p = 1")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE metadata_stmt FROM 'SELECT IFNULL(?, \\'test\\') AS x'",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "EXECUTE metadata_stmt USING @p")
        .unwrap()
    else {
        panic!("expected prepared SELECT result");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
    assert_eq!(result.columns[0].column_type, ColumnType::VarString);
    assert_eq!(result.columns[0].source.length, Some(84));
    assert_ne!(
        result.columns[0].source.flags & crate::mysql::protocol::NOT_NULL_FLAG,
        0
    );
}

#[test]
fn failed_reprepare_removes_old_named_statement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "PREPARE s FROM 'SELECT 1'")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "PREPARE s FROM 'definitely not sql'")
        .unwrap_err();
    assert_eq!(err.code(), 1064, "{}", err.message());
    let err = engine
        .execute_with_session(&mut session, "EXECUTE s")
        .unwrap_err();
    assert_eq!(err.code(), 1243, "{}", err.message());
}

#[test]
fn prepare_from_user_variable_captures_default_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE DATABASE prepared_db")
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE prepared_db")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (9)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET @sql = 'SELECT id FROM t'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "PREPARE captured FROM @sql")
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE test")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "EXECUTE captured")
        .unwrap()
    else {
        panic!("expected prepared SELECT result");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(9)]]);
    assert_eq!(session.current_database, "test");
}

#[test]
fn cast_as_json_preserves_enum_set_bit_and_binary_source_types() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE typed_json (
                e ENUM('red','blue'),
                s SET('a','b'),
                bitv BIT(4),
                payload BLOB
            )",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO typed_json VALUES ('red','a,b',b'1010',x'7B')",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT JSON_TYPE(CAST(e AS JSON)),
                    JSON_TYPE(CAST(s AS JSON)),
                    JSON_TYPE(CAST(bitv AS JSON)),
                    JSON_TYPE(CAST(payload AS JSON))
             FROM typed_json",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::String("STRING".into()),
            Value::String("STRING".into()),
            Value::String("UNSIGNED INTEGER".into()),
            Value::String("STRING".into()),
        ]]
    );
}

#[test]
fn compare_values_orders_same_type_text_without_changing_semantics() {
    assert_eq!(
        compare_values(
            &Value::String("alice".to_string()),
            &Value::String("bob".to_string())
        )
        .unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_values(
            &Value::Bytes(b"alice".to_vec()),
            &Value::Bytes(b"alice".to_vec())
        )
        .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        compare_values(
            &Value::Decimal("10.50".to_string()),
            &Value::Decimal("9.25".to_string())
        )
        .unwrap(),
        Ordering::Greater
    );
}

#[test]
fn compare_values_same_lane_numeric_fast_path() {
    // Int×Int: ordering matches direct i64 cmp, including values
    // above 2^53 where the legacy f64 round-trip would lose precision.
    assert_eq!(
        compare_values(&Value::Int(1), &Value::Int(2)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::Int(i64::MAX), &Value::Int(i64::MAX - 1)).unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        compare_values(&Value::Int(i64::MIN), &Value::Int(-1)).unwrap(),
        Ordering::Less
    );

    // UInt×UInt
    assert_eq!(
        compare_values(&Value::UInt(0), &Value::UInt(u64::MAX)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::UInt(u64::MAX), &Value::UInt(u64::MAX - 1)).unwrap(),
        Ordering::Greater
    );

    // Double×Double; NaN coalesced to Equal as in legacy path.
    assert_eq!(
        compare_values(&Value::Double(1.5), &Value::Double(2.5)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::Double(f64::NAN), &Value::Double(1.0)).unwrap(),
        Ordering::Equal
    );

    // Float×Float
    assert_eq!(
        compare_values(&Value::Float(3.0), &Value::Float(2.0)).unwrap(),
        Ordering::Greater
    );
}

#[test]
fn compare_values_same_lane_temporal_fast_path() {
    use crate::types::{ZERO_DATE, ZERO_DATETIME};

    // Date(i32): days since epoch; zero-date sentinel sorts first.
    assert_eq!(
        compare_values(&Value::Date(0), &Value::Date(1)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::Date(ZERO_DATE), &Value::Date(0)).unwrap(),
        Ordering::Less
    );

    // DateTime(i64): microseconds; zero-datetime sentinel sorts first.
    assert_eq!(
        compare_values(&Value::DateTime(1), &Value::DateTime(2)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::DateTime(ZERO_DATETIME), &Value::DateTime(0)).unwrap(),
        Ordering::Less
    );

    // Time(i64) signed micros — negative values are valid (-838h..+838h).
    assert_eq!(
        compare_values(&Value::Time(-1), &Value::Time(0)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::Time(100), &Value::Time(100)).unwrap(),
        Ordering::Equal
    );

    // Year(u8) — Year(0) is sentinel for 0000 and sorts before 1901+.
    assert_eq!(
        compare_values(&Value::Year(0), &Value::Year(1)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_values(&Value::Year(70), &Value::Year(100)).unwrap(),
        Ordering::Less
    );

    // DecimalCompact: same-scale fast path; different-scale falls through
    // to the full numeric() path which renormalises.
    assert_eq!(
        compare_values(
            &Value::DecimalCompact(150, 2), // 1.50
            &Value::DecimalCompact(250, 2), // 2.50
        )
        .unwrap(),
        Ordering::Less
    );
    // Same magnitude, different scale → fall through → numeric values
    // 1.50 vs 1.5 → equal.
    assert_eq!(
        compare_values(
            &Value::DecimalCompact(150, 2),
            &Value::DecimalCompact(15, 1),
        )
        .unwrap(),
        Ordering::Equal
    );
}

#[test]
fn compare_values_mixed_lane_uses_existing_path() {
    // Mixed numeric lanes still coerce through `numeric()` for MySQL-style
    // implicit casts; verify a handful of representative pairs.
    assert_eq!(
        compare_values(&Value::Int(5), &Value::Double(5.0)).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        compare_values(&Value::Int(10), &Value::UInt(5)).unwrap(),
        Ordering::Greater
    );
    // Number vs string-numeric: MySQL coerces strings to double.
    assert_eq!(
        compare_values(&Value::Int(7), &Value::String("7".to_string())).unwrap(),
        Ordering::Equal
    );
    // NULL on either side is the existing short-circuit branch.
    assert_eq!(
        compare_values(&Value::Null, &Value::Int(1)).unwrap(),
        Ordering::Less
    );
}

fn bind_statement_placeholders_for_test<'q>(
    statement: impl Into<crate::parser::QueryMut<'q>>,
    params: &[Value],
) -> Result<(), QueryError> {
    let statement = statement.into();
    match statement {
        crate::parser::QueryMut::Select(stmt) => {
            if let Some(selection) = &mut stmt.selection {
                bind_expr_placeholders_for_test(selection, params)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn bind_expr_placeholders_for_test(expr: &mut Expr, params: &[Value]) -> Result<(), QueryError> {
    match expr {
        Expr::Placeholder(idx) => {
            let value = params.get(*idx).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WRONG_ARGUMENTS,
                    "not enough test parameters",
                )
            })?;
            *expr = Expr::Literal(match value {
                Value::Int(value) => Literal::Int(*value),
                Value::String(value) => Literal::String(value.clone()),
                Value::Null => Literal::Null,
                _ => {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_ARGUMENTS,
                        "unsupported test parameter",
                    ))
                }
            });
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            bind_expr_placeholders_for_test(expr, params)?
        }
        Expr::Binary { left, right, .. } => {
            bind_expr_placeholders_for_test(left, params)?;
            bind_expr_placeholders_for_test(right, params)?;
        }
        Expr::InList { expr, list, .. } => {
            bind_expr_placeholders_for_test(expr, params)?;
            for item in list {
                bind_expr_placeholders_for_test(item, params)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            bind_expr_placeholders_for_test(expr, params)?;
            bind_expr_placeholders_for_test(low, params)?;
            bind_expr_placeholders_for_test(high, params)?;
        }
        _ => {}
    }
    Ok(())
}

#[test]
fn select_one_returns_single_integer_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let result = engine
        .execute_with_session(&mut session, " select   1 ; ")
        .unwrap();

    assert_eq!(
        result,
        QueryResponse::ResultSet(ResultSet {
            columns: vec![Column {
                source: crate::types::ColumnSource {
                    flags: crate::mysql::protocol::NOT_NULL_FLAG
                        | crate::mysql::protocol::BINARY_FLAG,
                    length: Some(1),
                    decimals: Some(0),
                    ..Default::default()
                },
                name: "1".to_string(),
                column_type: ColumnType::LongLong,
            }],
            rows: vec![vec![Value::Int(1)]],
        })
    );
}

#[test]
fn unsupported_query_returns_mysql_parse_error() {
    let engine = StubQueryEngine;
    let mut session = SessionContext::default();

    let err = engine.execute(&mut session, "select * from t").unwrap_err();

    assert_eq!(err.code(), 1064);
    assert_eq!(err.sql_state(), "42000");
}

#[test]
fn set_queries_are_acknowledged_for_jdbc_startup() {
    let engine = StubQueryEngine;
    let mut session = SessionContext::default();

    assert_eq!(
        engine.execute(&mut session, "SET sql_mode=''").unwrap(),
        QueryResponse::Ok
    );
    assert_eq!(session.sql_mode, "");
    assert_ne!(
        session.temporal_sql_mode_bits & crate::engine::api::TEMPORAL_MODE_EXPLICIT,
        0
    );
}

#[test]
fn set_default_value_is_acknowledged_for_jdbc_reset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    assert_eq!(
        engine
            .execute_with_session(&mut session, "SET sql_select_limit=DEFAULT")
            .unwrap(),
        QueryResponse::Ok
    );
    assert_eq!(
        engine
            .execute_with_session(&mut session, "SET @@SESSION.sql_select_limit=DEFAULT")
            .unwrap(),
        QueryResponse::Ok
    );
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "SET sql_mode=DEFAULT, sql_select_limit=DEFAULT"
            )
            .unwrap(),
        QueryResponse::Ok
    );
}

#[test]
fn jdbc_comment_before_select_one_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let result = engine
        .execute_with_session(&mut session, "/* mysql-connector-j */ SELECT 1")
        .unwrap();

    assert!(matches!(result, QueryResponse::ResultSet(_)));
}

#[test]
fn long_sql_log_text_is_truncated_in_the_middle_at_one_kb() {
    let sql = format!(
        "SELECT '{}' FROM t WHERE pad = '{}'",
        "a".repeat(900),
        "z".repeat(900)
    );

    let truncated = truncate_sql_for_log(&sql);

    assert!(truncated.len() <= 1024);
    assert!(truncated.starts_with("SELECT '"));
    assert!(truncated.ends_with("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'"));
    assert!(truncated.contains("[truncated, "));
    assert!(truncated.contains(" bytes total]"));
}

#[test]
fn long_sql_log_truncation_keeps_utf8_boundaries() {
    let sql = format!(
        "SELECT '{}{}{}'",
        "你".repeat(500),
        "🙂".repeat(200),
        "尾".repeat(500)
    );

    let truncated = truncate_sql_for_log(&sql);

    assert!(truncated.len() <= 1024);
    assert!(truncated.is_char_boundary(truncated.len()));
    assert!(truncated.starts_with("SELECT '"));
    assert!(truncated.ends_with("尾尾尾尾尾尾尾尾尾'"));
    assert!(truncated.contains("[truncated, "));
}

#[test]
fn jdbc_system_variable_query_returns_one_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let result = engine
        .execute_with_session(&mut session, "SELECT @@session.transaction_read_only AS transaction_read_only, @@version_comment LIMIT 1")
        .unwrap();

    let QueryResponse::ResultSet(result_set) = result else {
        panic!("expected result set");
    };
    assert_eq!(result_set.columns.len(), 2);
    assert_eq!(result_set.rows.len(), 1);
}

#[test]
fn session_variables_query_is_supported() {
    let engine = StubQueryEngine;
    let mut session = SessionContext::default();

    let result = engine
        .execute(&mut session, "SHOW SESSION VARIABLES")
        .unwrap();

    assert!(matches!(result, QueryResponse::ResultSet(_)));
}

#[test]
fn show_variables_and_status_like_are_filtered() {
    let engine = StubQueryEngine;
    let mut session = SessionContext::default();
    session.autocommit = false;
    session.ssl_cipher = Some("TLS13_AES_256_GCM_SHA384".to_string());
    session.ssl_version = Some("TLSv1.3".to_string());

    let QueryResponse::ResultSet(vars) = engine
        .execute(&mut session, "SHOW VARIABLES LIKE 'version%'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(vars.rows.iter().all(|row| match &row[0] {
        Value::String(name) => name.starts_with("version"),
        _ => false,
    }));

    let QueryResponse::ResultSet(isolation) = engine
        .execute(&mut session, "SHOW VARIABLES LIKE 'transaction_isolation'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        isolation.rows,
        vec![vec![
            Value::String("transaction_isolation".into()),
            Value::String("READ-COMMITTED".into())
        ]]
    );

    let QueryResponse::ResultSet(global_autocommit) = engine
        .execute(
            &mut session,
            "SHOW GLOBAL VARIABLES WHERE Variable_name = 'autocommit'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // MySQL 8.0 renders boolean SHOW VARIABLES entries as ON/OFF, not 1/0.
    // (The `1`/`0` form is reserved for `SELECT @@autocommit`.)
    assert_eq!(
        global_autocommit.rows,
        vec![vec![
            Value::String("autocommit".into()),
            Value::String("ON".into())
        ]]
    );

    let QueryResponse::ResultSet(status) = engine
        .execute(&mut session, "SHOW STATUS LIKE 'Ssl_cipher'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        status.rows,
        vec![vec![
            Value::String("Ssl_cipher".to_string()),
            Value::String("TLS13_AES_256_GCM_SHA384".to_string())
        ]]
    );

    let QueryResponse::ResultSet(status_where) = engine
        .execute(
            &mut session,
            "SHOW STATUS WHERE Variable_name = 'Ssl_version'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        status_where.rows,
        vec![vec![
            Value::String("Ssl_version".to_string()),
            Value::String("TLSv1.3".to_string())
        ]]
    );
}

#[test]
fn storage_runtime_variables_are_visible_and_settable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(vars) = engine
        .execute_with_session(&mut session, "SHOW VARIABLES LIKE 'mydb_buffer_pool_pages'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        vars.rows,
        vec![vec![
            Value::String("mydb_buffer_pool_pages".into()),
            Value::String("512".into())
        ]]
    );

    engine
        .execute_with_session(&mut session, "SET GLOBAL mydb_buffer_pool_pages = 8")
        .unwrap();
    let QueryResponse::ResultSet(selected) = engine
        .execute_with_session(&mut session, "SELECT @@global.mydb_buffer_pool_pages")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(selected.rows, vec![vec![Value::String("8".into())]]);

    let QueryResponse::ResultSet(status) = engine
        .execute_with_session(&mut session, "SHOW ENGINE MYDB STATUS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(status.rows.iter().any(|row| {
        row == &vec![
            Value::String("mydb".into()),
            Value::String("buffer_pool_pages".into()),
            Value::String("8".into()),
        ]
    }));
}

#[test]
fn binlog_variables_and_disabled_status_are_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(vars) = engine
        .execute_with_session(&mut session, "SHOW VARIABLES LIKE 'binlog_%'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(vars.rows.contains(&vec![
        Value::String("binlog_format".into()),
        Value::String("ROW".into())
    ]));
    assert!(vars.rows.contains(&vec![
        Value::String("binlog_row_image".into()),
        Value::String("FULL".into())
    ]));

    let QueryResponse::ResultSet(selected) = engine
        .execute_with_session(&mut session, "SELECT @@global.log_bin, @@global.gtid_mode")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        selected.rows,
        vec![vec![
            Value::String("OFF".into()),
            Value::String("OFF".into())
        ]]
    );

    let err = engine
        .execute_with_session(&mut session, "SHOW MASTER STATUS")
        .unwrap_err();
    assert_eq!(err.message(), "binary logging is disabled");
}

#[test]
fn flush_tables_with_read_lock_blocks_local_writes_until_unlock() {
    struct UnlockGuard;

    impl Drop for UnlockGuard {
        fn drop(&mut self) {
            let _ = crate::binlog::handle_admin_sql("UNLOCK TABLES", &SessionContext::default());
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let _guard = UnlockGuard;

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "FLUSH TABLES WITH READ LOCK")
        .unwrap();

    let err = engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap_err();
    assert_eq!(err.code(), 1223);
    assert!(err.message().contains("FLUSH TABLES WITH READ LOCK"));

    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(0)]]);

    engine
        .execute_with_session(&mut session, "UNLOCK TABLES")
        .unwrap();
    engine
        .execute_with_session(&mut session, "LOCK TABLES t AS a READ")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "SELECT * FROM t")
        .unwrap_err();
    assert_eq!(err.code(), 1100);
    engine
        .execute_with_session(&mut session, "SELECT * FROM t AS a")
        .unwrap();
    engine
        .execute_with_session(&mut session, "UNLOCK TABLES")
        .unwrap();

    engine
        .execute_with_session(&mut session, "LOCK TABLES t LOW_PRIORITY WRITE")
        .unwrap();
    assert_eq!(session.warning_count(), 1);
    engine
        .execute_with_session(&mut session, "UNLOCK TABLES")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();
}

#[test]
fn lock_tables_read_blocks_writes_until_unlock_and_reports_pending_mdl() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let mut s1 = SessionContext::default();
    s1.connection_id = 10;
    let mut s2 = SessionContext::default();
    s2.connection_id = 11;

    engine
        .execute_with_session(&mut s1, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut s1, "LOCK TABLES t READ")
        .unwrap();

    let QueryResponse::ResultSet(selected) = engine
        .execute_with_session(&mut s2, "SELECT COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected SELECT result");
    };
    assert_eq!(selected.rows, vec![vec![Value::Int(0)]]);

    let worker_engine = std::sync::Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        worker_engine.execute_with_session(&mut s2, "INSERT INTO t VALUES (1)")
    });

    let mut saw_pending = false;
    for _ in 0..100 {
        let QueryResponse::ResultSet(mdl) = engine
            .execute_with_session(
                &mut s1,
                "SELECT LOCK_STATUS FROM performance_schema.metadata_locks",
            )
            .unwrap()
        else {
            panic!("expected metadata_locks result");
        };
        if mdl
            .rows
            .iter()
            .any(|row| row[0] == Value::String("PENDING".to_string()))
        {
            saw_pending = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(saw_pending, "blocked writer should appear as PENDING MDL");

    engine
        .execute_with_session(&mut s1, "UNLOCK TABLES")
        .unwrap();
    handle.join().unwrap().unwrap();

    let QueryResponse::ResultSet(selected) = engine
        .execute_with_session(&mut s1, "SELECT COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected SELECT result");
    };
    assert_eq!(selected.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn lock_tables_write_blocks_other_reads_and_allows_owner_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let mut owner = SessionContext::default();
    owner.connection_id = 20;
    let mut other = SessionContext::default();
    other.connection_id = 21;

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "LOCK TABLES t WRITE")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1)")
        .unwrap();

    let worker_engine = std::sync::Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        worker_engine.execute_with_session(&mut other, "SELECT COUNT(*) FROM t")
    });

    let mut saw_pending = false;
    for _ in 0..100 {
        let QueryResponse::ResultSet(mdl) = engine
            .execute_with_session(
                &mut owner,
                "SELECT LOCK_STATUS FROM performance_schema.metadata_locks",
            )
            .unwrap()
        else {
            panic!("expected metadata_locks result");
        };
        if mdl
            .rows
            .iter()
            .any(|row| row[0] == Value::String("PENDING".to_string()))
        {
            saw_pending = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(saw_pending, "blocked reader should appear as PENDING MDL");

    engine
        .execute_with_session(&mut owner, "UNLOCK TABLES")
        .unwrap();
    let QueryResponse::ResultSet(selected) = handle.join().unwrap().unwrap() else {
        panic!("expected SELECT result");
    };
    assert_eq!(selected.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn explicit_lock_tables_enforce_allowlist_and_read_only_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 30;

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE u (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "LOCK TABLES t READ")
        .unwrap();

    let err = engine
        .execute_with_session(&mut session, "SELECT * FROM u")
        .unwrap_err();
    assert_eq!(err.code(), 1100);

    let err = engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap_err();
    assert_eq!(err.code(), 1099);

    let err = engine
        .execute_with_session(&mut session, "DROP TABLE t")
        .unwrap_err();
    assert_eq!(err.code(), 1099);

    engine
        .execute_with_session(&mut session, "UNLOCK TABLES")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();
}

#[test]
fn begin_releases_explicit_lock_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 31;

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "LOCK TABLES t READ")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn xa_prepared_branch_keeps_mdl_until_commit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let mut xa = SessionContext::default();
    xa.connection_id = 40;
    let mut ddl = SessionContext::default();
    ddl.connection_id = 41;

    engine
        .execute_with_session(&mut xa, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut xa, "XA START 'g1'")
        .unwrap();
    engine
        .execute_with_session(&mut xa, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine.execute_with_session(&mut xa, "XA END 'g1'").unwrap();
    engine
        .execute_with_session(&mut xa, "XA PREPARE 'g1'")
        .unwrap();

    let worker_engine = std::sync::Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        worker_engine.execute_with_session(&mut ddl, "ALTER TABLE t ADD COLUMN v INT")
    });

    let mut saw_pending = false;
    for _ in 0..100 {
        let QueryResponse::ResultSet(mdl) = engine
            .execute_with_session(
                &mut xa,
                "SELECT LOCK_STATUS FROM performance_schema.metadata_locks",
            )
            .unwrap()
        else {
            panic!("expected metadata_locks result");
        };
        if mdl
            .rows
            .iter()
            .any(|row| row[0] == Value::String("PENDING".to_string()))
        {
            saw_pending = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(saw_pending, "DDL should wait on prepared XA MDL");

    engine
        .execute_with_session(&mut xa, "XA COMMIT 'g1'")
        .unwrap();
    handle.join().unwrap().unwrap();
}

#[test]
fn cbo_coefficients_are_visible_and_settable_via_sql() {
    // Phase 1 CBO smoke: every cbo_* coefficient must show up in
    // SHOW VARIABLES, be readable via @@global, and be writable via
    // SET GLOBAL. This exercises the full plumbing from SQL parsing
    // through to the cbo registry's atomic store.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(vars) = engine
        .execute_with_session(&mut session, "SHOW VARIABLES LIKE 'cbo_seq_page_cost'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        vars.rows,
        vec![vec![
            Value::String("cbo_seq_page_cost".into()),
            Value::String("1".into()),
        ]]
    );

    engine
        .execute_with_session(&mut session, "SET GLOBAL cbo_rnd_page_cost = 7.5")
        .unwrap();
    let QueryResponse::ResultSet(selected) = engine
        .execute_with_session(&mut session, "SELECT @@global.cbo_rnd_page_cost")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(selected.rows, vec![vec![Value::String("7.5".into())]]);

    // Restore default so later tests aren't perturbed by the global
    // state mutation.
    engine
        .execute_with_session(&mut session, "SET GLOBAL cbo_rnd_page_cost = 4")
        .unwrap();
}

#[test]
fn cbo_model_is_read_only_v2() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let before = crate::engine::cbo::cbo_epoch();

    let QueryResponse::ResultSet(selected) = engine
        .execute_with_session(&mut session, "SELECT @@global.cbo_model")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(selected.rows, vec![vec![Value::String("v2".into())]]);

    let err = engine
        .execute_with_session(&mut session, "SET GLOBAL cbo_model = 'v2'")
        .unwrap_err();
    assert_eq!(err.code(), 1238);
    assert_eq!(crate::engine::cbo::cbo_epoch(), before);
}

#[test]
fn session_scope_mydb_runtime_variable_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = engine
        .execute_with_session(&mut session, "SET SESSION mydb_buffer_pool_pages = 8")
        .unwrap_err();

    assert_eq!(err.code(), 1229);
}

#[test]
fn query_log_config_defaults_and_ddl_detection_work() {
    assert_eq!(
        QueryLogConfig::default().slow_sql_threshold_ms(),
        QueryLogConfig::DEFAULT_SLOW_SQL_MS as u128
    );
    assert!(is_ddl_statement(
        &parse_statement("CREATE TABLE t (id INT)").unwrap()
    ));
    assert!(is_ddl_statement(
        &parse_statement("TRUNCATE TABLE t").unwrap()
    ));
    assert!(!is_ddl_statement(&parse_statement("SELECT 1").unwrap()));
}

#[test]
fn slow_sql_log_line_includes_lock_wait_attribution() {
    let mut session = SessionContext::default();
    session.connection_id = 42;
    session.user = "root".into();
    session.host = "127.0.0.1:3306".into();
    session.current_database = "tpcc".into();
    let line = StorageQueryEngine::slow_sql_log_line_for_test(
        &session,
        123.456,
        &QueryLogConfig::default(),
        QueryLogMetadata {
            lock_wait_count: 3,
            lock_wait_ms: 98.765,
            dml_path: "odku_mixed",
            dml_rows_input: 500,
            dml_rows_batched: 448,
            dml_conflict_rows: 1,
            dml_legacy_rows: 52,
            dml_race_rollbacks: 0,
            ..QueryLogMetadata::default()
        },
        "DELETE FROM t WHERE id = ?",
    );

    assert!(line.contains("lock_wait_count=3"), "{line}");
    assert!(line.contains("lock_wait_ms=98.765"), "{line}");
    assert!(line.contains("dml_path=odku_mixed"), "{line}");
    assert!(line.contains("rows_input=500"), "{line}");
    assert!(line.contains("rows_batched=448"), "{line}");
    assert!(line.contains("conflict_rows=1"), "{line}");
    assert!(line.contains("legacy_rows=52"), "{line}");
    assert!(
        line.contains("sql=\"DELETE FROM t WHERE id = ?\""),
        "{line}"
    );
}

#[test]
fn catalog_defaults_to_test_and_supports_database_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE DATABASE app")
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE app")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        )
        .unwrap();

    let QueryResponse::ResultSet(databases) = engine
        .execute_with_session(&mut session, "SHOW DATABASES")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(databases
        .rows
        .iter()
        .any(|row| row[0] == Value::String("app".to_string())));

    let QueryResponse::ResultSet(tables) = engine
        .execute_with_session(&mut session, "SHOW FULL TABLES")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(tables.rows[0][0], Value::String("users".to_string()));
    assert_eq!(tables.rows[0][1], Value::String("BASE TABLE".to_string()));
}

#[test]
fn show_full_tables_includes_table_statistics_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE orders (id INT PRIMARY KEY, note VARCHAR(32)) COMMENT='sales orders'",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO orders VALUES (1, 'first'), (2, 'second')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE VIEW orders_v AS SELECT id, note FROM orders",
        )
        .unwrap();

    let QueryResponse::ResultSet(tables) = engine
        .execute_with_session(&mut session, "SHOW FULL TABLES LIKE 'orders%'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        tables
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "Tables_in_test",
            "Table_type",
            "TABLE_ROWS",
            "DATA_LENGTH",
            "INDEX_LENGTH",
            "CREATE_TIME",
            "TABLE_COMMENT",
        ]
    );
    assert_eq!(tables.rows.len(), 2);

    let orders = tables
        .rows
        .iter()
        .find(|row| row[0] == Value::String("orders".to_string()))
        .unwrap();
    assert_eq!(orders[1], Value::String("BASE TABLE".to_string()));
    assert_eq!(orders[2], Value::Int(2));
    assert!(matches!(orders[3], Value::Int(bytes) if bytes > 0));
    assert!(matches!(orders[4], Value::Int(bytes) if bytes > 0));
    assert!(matches!(&orders[5], Value::String(ts) if !ts.is_empty()));
    assert_eq!(orders[6], Value::String("sales orders".to_string()));

    let view = tables
        .rows
        .iter()
        .find(|row| row[0] == Value::String("orders_v".to_string()))
        .unwrap();
    assert_eq!(view[1], Value::String("VIEW".to_string()));
    assert_eq!(view[2], Value::Null);
    assert_eq!(view[3], Value::Null);
    assert_eq!(view[4], Value::Null);
    assert_eq!(view[5], Value::Null);
    assert_eq!(view[6], Value::String("VIEW".to_string()));

    // The enhanced FULL shape also applies to system schemas; plain SHOW
    // TABLES retains its single-column result.
    for sql in [
        "SHOW FULL TABLES FROM information_schema",
        "SHOW FULL TABLES FROM mysql",
        "SHOW FULL TABLES FROM performance_schema",
        "SHOW FULL TABLES FROM sys",
    ] {
        let QueryResponse::ResultSet(result) =
            engine.execute_with_session(&mut session, sql).unwrap()
        else {
            panic!("expected result set: {sql}");
        };
        assert_eq!(result.columns.len(), 7, "{sql}");
        assert!(result.rows.iter().all(|row| row.len() == 7), "{sql}");
    }
    let QueryResponse::ResultSet(plain) = engine
        .execute_with_session(&mut session, "SHOW TABLES LIKE 'orders%'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(plain.columns.len(), 1);
    assert!(plain.rows.iter().all(|row| row.len() == 1));
}

#[test]
fn count_star_unqualified_matches_qualified_in_non_default_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE DATABASE shop")
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE shop")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE orders (id INT PRIMARY KEY, total INT)",
        )
        .unwrap();
    for i in 1..=5 {
        engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO orders (id, total) VALUES ({i}, {})", i * 10),
            )
            .unwrap();
    }

    let count = |engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str| -> i64 {
        let QueryResponse::ResultSet(rs) = engine.execute_with_session(session, sql).unwrap()
        else {
            panic!("expected result set for {sql}");
        };
        match &rs.rows[0][0] {
            Value::Int(n) => *n,
            other => panic!("expected int count for {sql}, got {other:?}"),
        }
    };

    let qualified = count(&engine, &mut session, "SELECT COUNT(*) FROM shop.orders");
    let unqualified = count(&engine, &mut session, "SELECT COUNT(*) FROM orders");
    let scanned = {
        let QueryResponse::ResultSet(rs) = engine
            .execute_with_session(&mut session, "SELECT id FROM orders")
            .unwrap()
        else {
            panic!("expected result set");
        };
        rs.rows.len() as i64
    };

    assert_eq!(qualified, 5, "qualified COUNT(*) should be 5");
    assert_eq!(scanned, 5, "actual scan should return 5 rows");
    assert_eq!(
        unqualified, qualified,
        "unqualified COUNT(*) must match qualified COUNT(*)"
    );
}

#[test]
fn create_database_dump_encryption_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let sql = "CREATE DATABASE IF NOT EXISTS `jzcloud_dev`/*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci *//*!80016 DEFAULT ENCRYPTION ='N' */;";
    {
        let engine = open_strict_test_engine(dir.path());
        engine.execute(sql).unwrap();
        engine.execute(sql).unwrap();
        let error = engine
            .execute("CREATE DATABASE encrypted_db DEFAULT ENCRYPTION='Y'")
            .unwrap_err();
        assert!(error.to_string().contains("ENCRYPTION"), "{error}");
        let QueryResponse::ResultSet(databases) = engine
            .execute("SHOW DATABASES LIKE 'encrypted_db'")
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert!(databases.rows.is_empty());
        engine
            .execute("CREATE TABLE jzcloud_dev.t (name VARCHAR(20))")
            .unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    let QueryResponse::ResultSet(result) =
        engine.execute("SHOW CREATE DATABASE jzcloud_dev").unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        matches!(&result.rows[0][1], Value::String(sql) if sql.contains("DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"))
    );
    engine
        .execute("INSERT INTO jzcloud_dev.t VALUES ('中文')")
        .unwrap();
}

#[test]
fn show_core_metadata_filters_and_mysql80_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE DATABASE app DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE app")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(32), note TEXT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE INDEX name_idx ON users (name) COMMENT 'lookup'",
        )
        .unwrap();

    let QueryResponse::ResultSet(databases) = engine
        .execute_with_session(&mut session, "SHOW DATABASES WHERE `Database` = 'app'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(databases.rows, vec![vec![Value::String("app".into())]]);

    let QueryResponse::ResultSet(create_db) = engine
        .execute_with_session(&mut session, "SHOW CREATE DATABASE IF NOT EXISTS app")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(matches!(
        &create_db.rows[0][1],
        Value::String(sql)
            if sql.contains("IF NOT EXISTS")
                && sql.contains("DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    ));

    let QueryResponse::ResultSet(tables) = engine
        .execute_with_session(&mut session, "SHOW FULL TABLES LIKE 'users'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(tables.rows.len(), 1);
    assert_eq!(tables.rows[0][0], Value::String("users".into()));
    assert_eq!(tables.rows[0][1], Value::String("BASE TABLE".into()));

    let QueryResponse::ResultSet(columns) = engine
        .execute_with_session(
            &mut session,
            "SHOW FULL COLUMNS FROM users WHERE Field = 'name'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let column_names: Vec<_> = columns.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        column_names,
        vec![
            "Field",
            "Type",
            "Collation",
            "Null",
            "Key",
            "Default",
            "Extra",
            "Privileges",
            "Comment"
        ]
    );
    assert_eq!(columns.rows.len(), 1);
    assert_eq!(columns.rows[0][0], Value::String("name".into()));
    assert_eq!(
        columns.rows[0][7],
        Value::String("select,insert,update,references".into())
    );

    let QueryResponse::ResultSet(indexes) = engine
        .execute_with_session(
            &mut session,
            "SHOW INDEX FROM users WHERE Key_name = 'name_idx'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let index_columns: Vec<_> = indexes.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        index_columns,
        vec![
            "Table",
            "Non_unique",
            "Key_name",
            "Seq_in_index",
            "Column_name",
            "Collation",
            "Cardinality",
            "Sub_part",
            "Packed",
            "Null",
            "Index_type",
            "Comment",
            "Index_comment",
            "Visible",
            "Expression"
        ]
    );
    assert_eq!(indexes.rows.len(), 1);
    assert_eq!(indexes.rows[0][12], Value::String("lookup".into()));

    let QueryResponse::ResultSet(charsets) = engine
        .execute_with_session(&mut session, "SHOW CHARACTER SET WHERE Charset = 'utf8mb4'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(charsets.rows.len(), 1);
    assert_eq!(charsets.rows[0][0], Value::String("utf8mb4".into()));

    let QueryResponse::ResultSet(collations) = engine
        .execute_with_session(&mut session, "SHOW COLLATION LIKE 'utf8mb4_0900_ai_ci'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(collations.rows.len(), 1);
    assert_eq!(
        collations.rows[0][0],
        Value::String("utf8mb4_0900_ai_ci".into())
    );
}

#[test]
fn show_columns_create_table_grants_and_processlist_work() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 42;
    session.user = "alice".to_string();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v TEXT NOT NULL)",
        )
        .unwrap();

    let QueryResponse::ResultSet(columns) = engine
        .execute_with_session(&mut session, "SHOW FULL COLUMNS FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(columns.rows[0][0], Value::String("id".to_string()));

    let QueryResponse::ResultSet(create) = engine
        .execute_with_session(&mut session, "SHOW CREATE TABLE t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(matches!(&create.rows[0][1], Value::String(sql) if sql.contains("PRIMARY KEY")));

    let QueryResponse::ResultSet(processlist) = engine
        .execute_with_session(&mut session, "SHOW PROCESSLIST")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(processlist.rows[0][0], Value::UInt(42));

    let QueryResponse::ResultSet(grants) = engine
        .execute_with_session(&mut session, "SHOW GRANTS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(matches!(&grants.rows[0][0], Value::String(grant) if grant.contains("alice")));
}

#[test]
fn show_table_status_supports_dbeaver_metadata_sql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE DATABASE tpch")
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE tpch")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE lineitem (\
                id INT PRIMARY KEY AUTO_INCREMENT, \
                name VARCHAR(32)\
             ) COMMENT='tpch lineitem'",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO lineitem (name) VALUES ('first'), ('second')",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "DELETE FROM lineitem WHERE id = 2")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE VIEW lineitem_v AS SELECT id, name FROM lineitem",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE test")
        .unwrap();

    let QueryResponse::ResultSet(status) = engine
        .execute_with_session(
            &mut session,
            "/* ApplicationName=DBeaver 26.0.4 - Metadata */ \
             SHOW TABLE STATUS FROM tpch LIKE 'lineitem'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        status
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "Name",
            "Engine",
            "Version",
            "Row_format",
            "Rows",
            "Avg_row_length",
            "Data_length",
            "Max_data_length",
            "Index_length",
            "Data_free",
            "Auto_increment",
            "Create_time",
            "Update_time",
            "Check_time",
            "Collation",
            "Checksum",
            "Create_options",
            "Comment",
        ]
    );
    assert_eq!(status.rows.len(), 1);
    let row = &status.rows[0];
    assert_eq!(row[0], Value::String("lineitem".to_string()));
    assert_eq!(row[1], Value::String("InnoDB".to_string()));
    assert_eq!(row[3], Value::String("Dynamic".to_string()));
    // INSERT +2 and DELETE -1 is one live row. The unsigned touched-row
    // counter is only for ANALYZE staleness and must not inflate TABLE_ROWS.
    assert_eq!(row[4], Value::Int(1));
    assert!(matches!(row[6], Value::Int(bytes) if bytes > 0));
    assert!(matches!(row[8], Value::Int(bytes) if bytes > 0));
    assert_eq!(row[10].to_display_string(), "3");
    assert_eq!(
        row[14],
        Value::String(crate::charset::DEFAULT_COLLATION.to_string())
    );
    assert!(matches!(&row[11], Value::String(ts) if !ts.is_empty()));
    assert!(matches!(&row[12], Value::String(ts) if !ts.is_empty()));
    assert_eq!(row[17], Value::String("tpch lineitem".to_string()));

    let QueryResponse::ResultSet(where_status) = engine
        .execute_with_session(
            &mut session,
            "SHOW TABLE STATUS IN `tpch` WHERE Name = 'lineitem'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(where_status.rows.len(), 1);
    assert_eq!(
        where_status.rows[0][0],
        Value::String("lineitem".to_string())
    );

    let QueryResponse::ResultSet(rows_status) = engine
        .execute_with_session(&mut session, "SHOW TABLE STATUS FROM tpch WHERE Rows >= 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(rows_status
        .rows
        .iter()
        .any(|row| row[0] == Value::String("lineitem".to_string())));

    let QueryResponse::ResultSet(view_status) = engine
        .execute_with_session(
            &mut session,
            "SHOW TABLE STATUS FROM tpch LIKE 'lineitem_v'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(view_status.rows.len(), 1);
    assert_eq!(
        view_status.rows[0][0],
        Value::String("lineitem_v".to_string())
    );
    assert_eq!(view_status.rows[0][1], Value::Null);
    assert_eq!(view_status.rows[0][17], Value::String("VIEW".to_string()));
}

#[test]
fn create_table_like_copies_visible_schema_indexes_and_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE src (\
                id INT PRIMARY KEY AUTO_INCREMENT, \
                name VARCHAR(20) NOT NULL DEFAULT 'anon', \
                email VARCHAR(40), \
                UNIQUE email_uniq (email), \
                INDEX name_idx (name)\
            )",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO src (name, email) VALUES ('a', 'a@x')",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "CREATE TABLE dst LIKE src")
        .unwrap();
    let QueryResponse::ResultSet(empty) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM dst")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(empty.rows, vec![vec![Value::Int(0)]]);

    let QueryResponse::ResultSet(create) = engine
        .execute_with_session(&mut session, "SHOW CREATE TABLE dst")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(create_sql) = &create.rows[0][1] else {
        panic!("expected CREATE TABLE text");
    };
    assert!(create_sql.contains("PRIMARY KEY"));
    assert!(create_sql.contains("UNIQUE KEY `email_uniq`"));
    assert!(create_sql.contains("KEY `name_idx`"));

    engine
        .execute_with_session(&mut session, "INSERT INTO dst (email) VALUES ('b@x')")
        .unwrap();
    let QueryResponse::ResultSet(row) = engine
        .execute_with_session(&mut session, "SELECT id, name, email FROM dst")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        row.rows,
        vec![vec![
            Value::Int(1),
            Value::String("anon".to_string()),
            Value::String("b@x".to_string())
        ]]
    );

    let duplicate = engine
        .execute_with_session(&mut session, "INSERT INTO dst (email) VALUES ('b@x')")
        .unwrap_err();
    assert_eq!(duplicate.code(), 1062);
}

#[test]
fn create_table_like_accepts_parenthesized_and_if_not_exists_forms() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE dst (LIKE src)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE IF NOT EXISTS dst (LIKE missing_src)",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM dst")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(0)]]);
}

fn show_create_table_text(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    table: &str,
) -> String {
    let QueryResponse::ResultSet(create) = engine
        .execute_with_session(session, &format!("SHOW CREATE TABLE {table}"))
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(sql) = &create.rows[0][1] else {
        panic!("expected CREATE TABLE text");
    };
    sql.clone()
}

#[test]
fn create_table_like_copies_table_options_and_pctfree() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // MySQL copies all table options (except DATA/INDEX DIRECTORY) plus
    // PCTFREE/PCTUSED through CREATE TABLE ... LIKE.
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE src (id INT PRIMARY KEY) \
             ROW_FORMAT=DYNAMIC KEY_BLOCK_SIZE=8 MAX_ROWS=1000 \
             PCTFREE=20 PCTUSED=40 COMMENT='hi there'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE dst LIKE src")
        .unwrap();

    let create_sql = show_create_table_text(&engine, &mut session, "dst");
    assert!(create_sql.contains("ROW_FORMAT=DYNAMIC"), "{create_sql}");
    assert!(create_sql.contains("KEY_BLOCK_SIZE=8"), "{create_sql}");
    assert!(create_sql.contains("MAX_ROWS=1000"), "{create_sql}");
    assert!(create_sql.contains("PCTFREE=20"), "{create_sql}");
    assert!(create_sql.contains("PCTUSED=40"), "{create_sql}");
    assert!(create_sql.contains("COMMENT='hi there'"), "{create_sql}");
}

#[test]
fn create_temp_table_like_partitioned_source_errors_1562() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE src (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2",
        )
        .unwrap();
    // MySQL forbids partitioned TEMPORARY tables entirely; LIKE onto a
    // partitioned source must fail with 1562, never silently drop the
    // partitioning.
    let err = engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE dst LIKE src")
        .unwrap_err();
    assert_eq!(err.code(), 1562);
}

#[test]
fn auth_grants_are_persisted_and_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    engine
        .execute_with_session(&mut root, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "INSERT INTO t VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();

    let mut alice = SessionContext::default();
    alice.user = "alice".to_string();
    alice.login_user = "alice".to_string();
    alice.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;

    let denied = engine
        .execute_with_session(&mut alice, "SELECT id FROM t")
        .unwrap_err();
    assert_eq!(denied.code(), 1142);

    engine
        .execute_with_session(&mut root, "GRANT SELECT ON test.t TO 'alice'@'%'")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut alice, "SELECT id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);

    let denied = engine
        .execute_with_session(&mut alice, "INSERT INTO t VALUES (2)")
        .unwrap_err();
    assert_eq!(denied.code(), 1142);
}

#[test]
fn show_full_columns_privileges_reflect_grants_and_active_roles() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    engine
        .execute_with_session(
            &mut root,
            "CREATE TABLE priv_cols (id INT PRIMARY KEY, secret INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'reader'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE ROLE 'writer'@'%'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT SELECT ON test.priv_cols TO 'reader'@'%'")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "GRANT UPDATE (secret) ON test.priv_cols TO 'reader'@'%'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT INSERT ON test.priv_cols TO 'writer'@'%'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT 'writer'@'%' TO 'reader'@'%'")
        .unwrap();

    let mut reader = SessionContext::default();
    reader.user = "reader".to_string();
    reader.login_user = "reader".to_string();
    reader.authenticated_as = crate::engine::auth::AccountKey::new("reader", "%");
    reader
        .active_roles
        .insert(crate::engine::auth::AccountKey::new("writer", "%"));
    reader.auth_epoch = engine.db.auth_catalog().epoch;

    let QueryResponse::ResultSet(columns) = engine
        .execute_with_session(&mut reader, "SHOW FULL COLUMNS FROM priv_cols")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let privileges_col = columns
        .columns
        .iter()
        .position(|c| c.name == "Privileges")
        .unwrap();
    let id_row = columns
        .rows
        .iter()
        .find(|row| row[0] == Value::String("id".to_string()))
        .unwrap();
    let secret_row = columns
        .rows
        .iter()
        .find(|row| row[0] == Value::String("secret".to_string()))
        .unwrap();
    assert_eq!(
        id_row[privileges_col],
        Value::String("select,insert".to_string())
    );
    assert_eq!(
        secret_row[privileges_col],
        Value::String("select,insert,update".to_string())
    );
}

fn caching_sha2_token(password: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};

    let seed = crate::mysql::protocol::auth_plugin_data();
    let stage1 = Sha256::digest(password);
    let stage2 = Sha256::digest(stage1);
    let mut sha = Sha256::new();
    sha.update(stage2);
    sha.update(seed);
    let digest = sha.finalize();
    stage1
        .iter()
        .zip(digest.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

#[test]
fn password_expire_blocks_login() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    engine
        .execute_with_session(&mut root, "CREATE USER 'eve'@'%' IDENTIFIED BY 'pw'")
        .unwrap();

    let token = caching_sha2_token(b"pw");

    // Correct credentials authenticate before expiry.
    engine
        .authenticate("eve", "127.0.0.1", &token, Some("caching_sha2_password"))
        .expect("login should succeed before password expiry");

    engine
        .execute_with_session(&mut root, "ALTER USER 'eve'@'%' PASSWORD EXPIRE")
        .unwrap();

    // After PASSWORD EXPIRE, the same correct credentials are refused with
    // ER_MUST_CHANGE_PASSWORD_LOGIN (1862) instead of silently succeeding.
    let err = engine
        .authenticate("eve", "127.0.0.1", &token, Some("caching_sha2_password"))
        .expect_err("expired password must block login");
    assert_eq!(err.code(), 1862);
}

#[test]
fn ssl_requirement_and_failed_login_policy_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    engine
        .execute_with_session(
            &mut root,
            "CREATE USER 'ssluser'@'%' IDENTIFIED BY 'pw' REQUIRE SSL",
        )
        .unwrap();
    let token = caching_sha2_token(b"pw");
    let err = engine
        .authenticate(
            "ssluser",
            "127.0.0.1",
            &token,
            Some("caching_sha2_password"),
        )
        .expect_err("REQUIRE SSL must reject plaintext auth");
    assert_eq!(err.code(), 3159);

    engine
        .execute_with_session(
            &mut root,
            "CREATE USER 'lockme'@'%' IDENTIFIED BY 'pw' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1",
        )
        .unwrap();
    // A fast-auth miss requests full authentication; only the final password
    // rejection counts towards FAILED_LOGIN_ATTEMPTS.
    let request = crate::engine::auth::AuthRequest::legacy(
        "lockme",
        "127.0.0.1",
        &[],
        Some("caching_sha2_password"),
    );
    for _ in 0..2 {
        assert!(engine
            .authenticate_with_plaintext_password("lockme", "127.0.0.1", "wrong", &request)
            .is_err());
    }
    let err = engine
        .authenticate(
            "lockme",
            "127.0.0.1",
            &caching_sha2_token(b"pw"),
            Some("caching_sha2_password"),
        )
        .expect_err("account should be temporarily locked after failed attempts");
    assert_eq!(err.code(), 3955);
}

#[test]
fn self_password_change_and_show_create_user_follow_auth_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    engine
        .execute_with_session(
            &mut root,
            "CREATE USER 'eve'@'%' IDENTIFIED BY 'old' WITH MAX_QUERIES_PER_HOUR 7",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "CREATE USER 'showme'@'%' IDENTIFIED BY 'pw' REQUIRE SSL WITH MAX_QUERIES_PER_HOUR 7",
        )
        .unwrap();
    let plugin = engine
        .auth_plugin_for_user("eve", "127.0.0.1")
        .unwrap()
        .expect("created account should resolve an auth plugin");
    assert_eq!(plugin, "caching_sha2_password");

    let mut eve = SessionContext::default();
    eve.user = "eve".to_string();
    eve.login_user = "eve".to_string();
    eve.authenticated_as = crate::engine::auth::AccountKey::new("eve", "%");
    eve.auth_epoch = engine.db.auth_catalog().epoch;

    engine
        .execute_with_session(&mut eve, "ALTER USER CURRENT_USER() IDENTIFIED BY 'new'")
        .unwrap();
    let err = engine
        .authenticate(
            "eve",
            "127.0.0.1",
            &caching_sha2_token(b"old"),
            Some("caching_sha2_password"),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1045);
    engine
        .authenticate(
            "eve",
            "127.0.0.1",
            &caching_sha2_token(b"new"),
            Some("caching_sha2_password"),
        )
        .expect("self ALTER USER should update the stored password");

    engine
        .execute_with_session(&mut eve, "SET PASSWORD = 'newer'")
        .unwrap();
    engine
        .authenticate(
            "eve",
            "127.0.0.1",
            &caching_sha2_token(b"newer"),
            Some("caching_sha2_password"),
        )
        .expect("SET PASSWORD should update the current account");

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut root, "SHOW CREATE USER 'showme'@'%'")
        .unwrap()
    else {
        panic!("expected SHOW CREATE USER result set");
    };
    assert!(matches!(
        &rs.rows[0][0],
        Value::String(sql)
            if sql.contains("CREATE USER 'showme'@'%'")
                && sql.contains("IDENTIFIED WITH 'caching_sha2_password'")
                && sql.contains("REQUIRE SSL")
                && sql.contains("MAX_QUERIES_PER_HOUR 7")
    ));
}

#[test]
fn column_grants_and_scoped_grant_option_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    engine
        .execute_with_session(&mut root, "CREATE TABLE t (id INT PRIMARY KEY, secret INT)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "INSERT INTO t VALUES (1, 99)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "GRANT SELECT (id) ON test.t TO 'alice'@'%' WITH GRANT OPTION",
        )
        .unwrap();

    let mut alice = SessionContext::default();
    alice.user = "alice".to_string();
    alice.login_user = "alice".to_string();
    alice.authenticated_as = crate::engine::auth::AccountKey::new("alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;

    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut alice, "SELECT id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);

    let denied = engine
        .execute_with_session(&mut alice, "SELECT secret FROM t")
        .unwrap_err();
    assert!(denied.is(crate::error::catalog::ER_COLUMNACCESS_DENIED_ERROR));

    engine
        .execute_with_session(&mut alice, "GRANT SELECT (id) ON test.t TO 'bob'@'%'")
        .unwrap();
    let mut bob = SessionContext::default();
    bob.user = "bob".to_string();
    bob.login_user = "bob".to_string();
    bob.authenticated_as = crate::engine::auth::AccountKey::new("bob", "%");
    bob.auth_epoch = engine.db.auth_catalog().epoch;
    engine
        .execute_with_session(&mut bob, "SELECT id FROM t")
        .unwrap();
}

#[test]
fn decimal_literal_preserves_full_precision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    s.connection_id = 9_999_991;
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE t (id INT PRIMARY KEY, d DECIMAL(30,2))",
        )
        .unwrap();
    // 20-digit magnitude: an f64 holds only ~15-17 significant digits, so the
    // old parse-as-f64 literal path silently truncated this on the way in.
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (1, 123456789012345678.99)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT d FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "123456789012345678.99");
}

#[test]
fn analyze_table_bumps_schema_version_to_invalidate_plans() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let before = engine.db.table("test.t").unwrap().schema_version;
    engine
        .execute_with_session(&mut s, "ANALYZE TABLE t")
        .unwrap();
    let after = engine.db.table("test.t").unwrap().schema_version;
    // The bump is what invalidates plan-cache entries keyed on
    // (table_id, schema_version); without it ANALYZE never forces re-planning
    // (e.g. a plan built on an empty table survives a later bulk load).
    assert!(
        after > before,
        "ANALYZE TABLE must bump schema_version ({before} -> {after})"
    );
}

#[test]
fn set_isolation_variable_validates_level() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // All MySQL 8.0 isolation levels are accepted as variable assignments
    // and actually applied (not silently swallowed like the old unknown-
    // variable no-op).
    for (sql, expected) in [
        (
            "SET SESSION transaction_isolation = 'READ-COMMITTED'",
            IsolationLevel::ReadCommitted,
        ),
        (
            "SET SESSION transaction_isolation = 'REPEATABLE-READ'",
            IsolationLevel::RepeatableRead,
        ),
        (
            "SET tx_isolation = 'SERIALIZABLE'",
            IsolationLevel::Serializable,
        ),
        (
            "SET @@session.transaction_isolation = 'READ-UNCOMMITTED'",
            IsolationLevel::ReadUncommitted,
        ),
    ] {
        engine.execute_with_session(&mut s, sql).unwrap();
        assert_eq!(s.isolation, expected);
    }
    // @@transaction_isolation reflects the session's actual level (so JDBC's
    // getTransactionIsolation and friends read back what was set).
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT @@transaction_isolation")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::String("READ-UNCOMMITTED".to_string()));
    let err = engine
        .execute_with_session(&mut s, "SET tx_isolation = 'BOGUS'")
        .unwrap_err();
    assert_eq!(err.code(), 1231);
}

#[test]
fn set_names_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    // Clients/ORMs/connection pools issue these during setup; they must not
    // error (previously returned 1235), or the connection handshake breaks.
    engine
        .execute_with_session(&mut s, "SET NAMES utf8mb4")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET NAMES latin1")
        .unwrap();
}

#[test]
fn multi_statement_executes_each_statement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    // A single call carrying several ;-separated statements
    // (CLIENT_MULTI_STATEMENTS) used to error 1235; now each runs in order.
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)",
        )
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(3));
}

#[test]
fn performance_schema_core_tables_are_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    // setup_consumers: MySQL 8.0.46's fixed 16-row set, including the
    // events_statements_cpu consumer, with the global switch ON.
    let QueryResponse::ResultSet(consumers) = engine
        .execute_with_session(&mut s, "SELECT * FROM performance_schema.setup_consumers")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(consumers.columns.len(), 2);
    assert_eq!(consumers.rows.len(), 16);
    assert!(consumers.rows.iter().any(|r| {
        r[0] == Value::String("global_instrumentation".to_string())
            && r[1] == Value::String("YES".to_string())
    }));

    // setup_instruments queryable and non-empty.
    let QueryResponse::ResultSet(instruments) = engine
        .execute_with_session(&mut s, "SELECT * FROM performance_schema.setup_instruments")
        .unwrap()
    else {
        panic!("expected result set");
    };
    // MySQL 8.0 setup_instruments has 7 columns
    // (NAME/ENABLED/TIMED/PROPERTIES/FLAGS/VOLATILITY/DOCUMENTATION).
    assert_eq!(instruments.columns.len(), 7);
    assert_eq!(instruments.rows.len(), 1250);

    // threads: full 24-column MySQL 8.0.46 PS shape (row count depends on live
    // connections).
    let QueryResponse::ResultSet(threads) = engine
        .execute_with_session(&mut s, "SELECT * FROM performance_schema.threads")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(threads.columns.len(), 24);

    // `@@performance_schema` reports 1 since PS tables carry real data.
    // (MySQL convention: SHOW VARIABLES renders bool as ON/OFF; the
    // `@@var` form returns the underlying 1/0 — matching mydb's
    // sysvar registry.)
    let QueryResponse::ResultSet(ps) = engine
        .execute_with_session(&mut s, "SELECT @@performance_schema")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(ps.rows[0][0], Value::String("1".to_string()));
}

#[test]
fn performance_schema_setup_tables_are_writable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    let count = |s: &mut SessionContext, table: &str| -> usize {
        let sql = format!("SELECT * FROM performance_schema.{table}");
        match engine.execute_with_session(s, &sql).unwrap() {
            QueryResponse::ResultSet(rs) => rs.rows.len(),
            other => panic!("expected result set, got {other:?}"),
        }
    };

    // UPDATE a writable column (setup_consumers.ENABLED) and read it back.
    let resp = engine
        .execute_with_session(
            &mut s,
            "UPDATE performance_schema.setup_consumers SET ENABLED='YES' \
             WHERE NAME='statements_digest'",
        )
        .unwrap();
    assert!(matches!(
        resp,
        QueryResponse::OkAffected { affected_rows: 0 }
    ));
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(
            &mut s,
            "SELECT ENABLED FROM performance_schema.setup_consumers \
             WHERE NAME='statements_digest'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::String("YES".to_string()));

    // UPDATE setup_instruments.TIMED via a LIKE predicate. MySQL 8.0.46
    // defaults all statement/sql instruments to YES, so changed rows is zero.
    let resp = engine
        .execute_with_session(
            &mut s,
            "UPDATE performance_schema.setup_instruments SET TIMED='YES' \
             WHERE NAME LIKE 'statement/sql/%'",
        )
        .unwrap();
    assert!(matches!(
        resp,
        QueryResponse::OkAffected { affected_rows: 0 }
    ));

    // INSERT then DELETE a setup_objects row.
    let before = count(&mut s, "setup_objects");
    engine
        .execute_with_session(
            &mut s,
            "INSERT INTO performance_schema.setup_objects \
             (OBJECT_TYPE,OBJECT_SCHEMA,OBJECT_NAME,ENABLED,TIMED) \
             VALUES ('TABLE','mydb','t1','YES','YES')",
        )
        .unwrap();
    assert_eq!(count(&mut s, "setup_objects"), before + 1);
    engine
        .execute_with_session(
            &mut s,
            "DELETE FROM performance_schema.setup_objects WHERE OBJECT_NAME='t1'",
        )
        .unwrap();
    assert_eq!(count(&mut s, "setup_objects"), before);
}

#[test]
fn performance_schema_readonly_tables_reject_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    // MySQL's PFS ACL rejects unsupported commands before the engine handler.
    let err = engine
        .execute_with_session(
            &mut s,
            "INSERT INTO performance_schema.events_waits_history_long (THREAD_ID) VALUES (1)",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1142, "events_waits_history_long INSERT");

    // UPDATE on a read-only PFS table follows the same ACL path.
    let err = engine
        .execute_with_session(
            &mut s,
            "UPDATE performance_schema.global_status SET VARIABLE_VALUE='1'",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1142, "global_status UPDATE");

    // mysql / information_schema DML still rejected with 1142 (regression).
    let err = engine
        .execute_with_session(&mut s, "UPDATE mysql.user SET User='x'")
        .unwrap_err();
    assert_eq!(err.code(), 1142, "mysql.user UPDATE");
}

#[test]
fn performance_schema_truncate_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    // Fixed-row config table is rejected by the PFS ACL → 1142.
    let err = engine
        .execute_with_session(
            &mut s,
            "TRUNCATE TABLE performance_schema.setup_instruments",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1142, "TRUNCATE setup_instruments");

    // setup_objects: TRUNCATE removes all rows.
    engine
        .execute_with_session(&mut s, "TRUNCATE TABLE performance_schema.setup_objects")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT * FROM performance_schema.setup_objects")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 0, "setup_objects emptied by TRUNCATE");

    // An event table is truncatable (clears collected data; no-op while empty).
    engine
        .execute_with_session(
            &mut s,
            "TRUNCATE TABLE performance_schema.events_waits_history",
        )
        .unwrap();
}

#[test]
fn statement_diagnostics_show_errors_and_counts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    let err = engine
        .execute_with_session(&mut s, "SELEC 1")
        .expect_err("parse error should populate diagnostics");
    assert_eq!(err.code(), 1064);
    assert_eq!(s.warning_count(), 1);
    assert_eq!(s.error_count(), 1);

    let QueryResponse::ResultSet(counts) = engine
        .execute_with_session(&mut s, "SELECT @@warning_count, @@error_count")
        .unwrap()
    else {
        panic!("expected count result set");
    };
    assert_eq!(counts.rows[0], vec![Value::UInt(1), Value::UInt(1)]);

    assert_eq!(s.warning_count(), 0);
    assert_eq!(s.error_count(), 0);
    // Restore a condition to independently verify SHOW diagnostics preserve it.
    engine.execute_with_session(&mut s, "SELEC 1").unwrap_err();

    let QueryResponse::ResultSet(show_count) = engine
        .execute_with_session(&mut s, "SHOW COUNT(*) ERRORS")
        .unwrap()
    else {
        panic!("expected SHOW COUNT result set");
    };
    assert_eq!(show_count.rows[0][0], Value::UInt(1));

    let QueryResponse::ResultSet(errors) =
        engine.execute_with_session(&mut s, "SHOW ERRORS").unwrap()
    else {
        panic!("expected SHOW ERRORS result set");
    };
    assert_eq!(errors.rows.len(), 1);
    assert_eq!(errors.rows[0][0], Value::String("Error".to_string()));
    assert_eq!(errors.rows[0][1], Value::Int(1064));

    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut s, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected SHOW WARNINGS result set");
    };
    assert_eq!(warnings.rows, errors.rows);

    engine.execute_with_session(&mut s, "SELECT 1").unwrap();
    assert_eq!(s.warning_count(), 0);
    assert_eq!(s.error_count(), 0);
}

#[test]
fn statement_diagnostics_show_limit_preserves_area() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    s.set_warnings(vec![
        crate::engine::api::SqlWarning::known(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_NO,
            "first",
        ),
        crate::engine::api::SqlWarning::known(
            crate::error::ConditionLevel::Error,
            crate::error::catalog::ER_YES,
            "second",
        ),
        crate::engine::api::SqlWarning::known(
            crate::error::ConditionLevel::Error,
            crate::error::catalog::ER_CANT_CREATE_FILE,
            "third",
        ),
    ]);

    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut s, "SHOW WARNINGS LIMIT 1, 1")
        .unwrap()
    else {
        panic!("expected SHOW WARNINGS result set");
    };
    assert_eq!(warnings.rows.len(), 1);
    assert_eq!(warnings.rows[0][1], Value::Int(1003));
    assert_eq!(s.warning_count(), 3);

    let QueryResponse::ResultSet(errors) = engine
        .execute_with_session(&mut s, "SHOW ERRORS LIMIT 1")
        .unwrap()
    else {
        panic!("expected SHOW ERRORS result set");
    };
    assert_eq!(errors.rows.len(), 1);
    assert_eq!(errors.rows[0][1], Value::Int(1003));
    assert_eq!(s.error_count(), 2);
}

#[test]
fn performance_schema_error_summary_records_and_truncates() {
    crate::engine::perf_errors::reset();

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    s.connection_id = 9_999_991;

    engine
        .execute_with_session(&mut s, "CREATE TABLE errsum (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO errsum VALUES (1)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut s, "INSERT INTO errsum VALUES (1)")
        .expect_err("duplicate key should be counted");
    assert_eq!(err.code(), 1062);

    let QueryResponse::ResultSet(global) = engine
        .execute_with_session(
            &mut s,
            "SELECT ERROR_NUMBER, ERROR_NAME, SQL_STATE, SUM_ERROR_RAISED, SUM_ERROR_HANDLED \
             FROM performance_schema.events_errors_summary_global_by_error \
             WHERE ERROR_NUMBER = 1062",
        )
        .unwrap()
    else {
        panic!("expected global error summary");
    };
    assert_eq!(global.rows.len(), 1);
    assert_eq!(global.rows[0][0], Value::Int(1062));
    assert_eq!(global.rows[0][1], Value::String("ER_DUP_ENTRY".to_string()));
    assert_eq!(global.rows[0][2], Value::String("23000".to_string()));
    assert!(
        matches!(global.rows[0][3], Value::UInt(n) if n >= 1),
        "global count should include at least the duplicate-key error from this test"
    );
    assert!(matches!(global.rows[0][4], Value::UInt(_)));

    let QueryResponse::ResultSet(by_thread) = engine
        .execute_with_session(
            &mut s,
            "SELECT THREAD_ID, ERROR_NUMBER, SUM_ERROR_RAISED \
             FROM performance_schema.events_errors_summary_by_thread_by_error \
             WHERE ERROR_NUMBER = 1062",
        )
        .unwrap()
    else {
        panic!("expected thread error summary");
    };
    assert_eq!(
        by_thread.rows[0],
        vec![Value::UInt(9_999_991), Value::Int(1062), Value::UInt(1)]
    );

    engine
        .execute_with_session(
            &mut s,
            "TRUNCATE TABLE performance_schema.events_errors_summary_global_by_error",
        )
        .unwrap();
    let QueryResponse::ResultSet(empty) = engine
        .execute_with_session(
            &mut s,
            "SELECT ERROR_NUMBER \
             FROM performance_schema.events_errors_summary_by_thread_by_error \
             WHERE THREAD_ID = 9999991",
        )
        .unwrap()
    else {
        panic!("expected empty thread error summary");
    };
    assert!(empty.rows.is_empty());
}

#[test]
fn performance_schema_digest_accumulates_by_statement_shape() {
    // Deterministic clean slate regardless of test runner (the digest table is
    // process-global static state shared across tests under `cargo test`).
    crate::engine::perf_digest::reset();

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    engine
        .execute_with_session(&mut s, "CREATE TABLE dg (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO dg VALUES (1,10),(2,20),(3,30)")
        .unwrap();

    // `statements_digest` is ON by MySQL 8.0 factory default (perf_setup §4).
    // Set it explicitly anyway so the test is self-contained even under
    // `cargo test`, where a prior test may have toggled the shared flag; then
    // reset the accumulator so only the statements below are counted.
    engine
        .execute_with_session(
            &mut s,
            "UPDATE performance_schema.setup_consumers SET ENABLED='YES' \
             WHERE NAME='statements_digest'",
        )
        .unwrap();
    crate::engine::perf_digest::reset();

    // Three same-shape SELECTs differing only in their literal collapse to one
    // DIGEST; an INSERT is a distinct shape.
    for id in [1, 2, 3] {
        engine
            .execute_with_session(&mut s, &format!("SELECT v FROM dg WHERE id = {id}"))
            .unwrap();
    }
    engine
        .execute_with_session(&mut s, "INSERT INTO dg VALUES (4,40)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(
            &mut s,
            "SELECT SCHEMA_NAME, DIGEST, DIGEST_TEXT, COUNT_STAR, SUM_ROWS_AFFECTED \
             FROM performance_schema.events_statements_summary_by_digest",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    // The SELECT shape accumulated COUNT_STAR = 3 under one digest.
    let select_row = rs
        .rows
        .iter()
        .find(|r| matches!(&r[2], Value::String(t) if t.contains("`dg`") && t.contains("ID = ?")))
        .expect("digest row for the SELECT shape");
    assert_eq!(select_row[3], Value::UInt(3), "COUNT_STAR for SELECT shape");
    assert_eq!(
        select_row[0],
        Value::String("test".to_string()),
        "SCHEMA_NAME is the executing default schema"
    );
    // DIGEST is the 64-char hex SHA-256 of the normalized text.
    let Value::String(digest) = &select_row[1] else {
        panic!("DIGEST must be non-null");
    };
    assert_eq!(digest.len(), 64);
    assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));

    // The INSERT shape is a separate digest and recorded its affected rows.
    let insert_row = rs
        .rows
        .iter()
        .find(|r| {
            matches!(&r[2], Value::String(t)
                if t.to_ascii_uppercase().starts_with("INSERT") && t.contains("dg"))
        })
        .expect("digest row for the INSERT shape");
    assert_eq!(
        insert_row[4],
        Value::UInt(1),
        "SUM_ROWS_AFFECTED for INSERT"
    );

    // Full 39-column shape is intact for SELECT *.
    let QueryResponse::ResultSet(star) = engine
        .execute_with_session(
            &mut s,
            "SELECT * FROM performance_schema.events_statements_summary_by_digest",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(star.columns.len(), 39);

    // TRUNCATE resets the accumulator.
    engine
        .execute_with_session(
            &mut s,
            "TRUNCATE TABLE performance_schema.events_statements_summary_by_digest",
        )
        .unwrap();
    // Restore the shared consumer flag to its factory default (ON) so we leave
    // it as other tests expect to find it.
    engine
        .execute_with_session(
            &mut s,
            "UPDATE performance_schema.setup_consumers SET ENABLED='YES' \
             WHERE NAME='statements_digest'",
        )
        .unwrap();
}

#[test]
fn performance_schema_statement_events_and_summaries_record_when_enabled() {
    crate::engine::perf_events::reset();
    crate::engine::perf_digest::reset();

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    s.connection_id = 42_424;
    s.user = "alice".to_string();
    s.host = "client-a".to_string();

    engine
        .execute_with_session(&mut s, "CREATE TABLE pfs_stmt (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "UPDATE performance_schema.setup_consumers SET ENABLED='YES' \
             WHERE NAME IN ('events_statements_current','events_statements_history',\
                            'events_statements_history_long','statements_digest')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut s,
            "UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' \
             WHERE NAME LIKE 'statement/sql/%'",
        )
        .unwrap();

    engine
        .execute_with_session(&mut s, "SELECT 123 AS observed")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO pfs_stmt VALUES (1,10)")
        .unwrap();

    let QueryResponse::ResultSet(history) = engine
        .execute_with_session(
            &mut s,
            "SELECT THREAD_ID, EVENT_NAME, SQL_TEXT, ROWS_SENT, ROWS_AFFECTED \
             FROM performance_schema.events_statements_history \
             WHERE THREAD_ID = 42424",
        )
        .unwrap()
    else {
        panic!("expected statement history");
    };
    assert!(history.rows.iter().any(|row| {
        row[1] == Value::String("statement/sql/select".to_string())
            && matches!(&row[2], Value::String(sql) if sql.contains("SELECT 123"))
            && row[3] == Value::UInt(1)
    }));
    assert!(history.rows.iter().any(|row| {
        row[1] == Value::String("statement/sql/insert".to_string())
            && matches!(&row[2], Value::String(sql) if sql.contains("pfs_stmt"))
            && row[4] == Value::UInt(1)
    }));

    let QueryResponse::ResultSet(current) = engine
        .execute_with_session(
            &mut s,
            "SELECT THREAD_ID, EVENT_NAME \
             FROM performance_schema.events_statements_current \
             WHERE THREAD_ID = 42424",
        )
        .unwrap()
    else {
        panic!("expected statement current");
    };
    assert!(
        !current.rows.is_empty(),
        "events_statements_current should retain the last event"
    );

    let QueryResponse::ResultSet(long) = engine
        .execute_with_session(
            &mut s,
            "SELECT EVENT_NAME FROM performance_schema.events_statements_history_long \
             WHERE THREAD_ID = 42424",
        )
        .unwrap()
    else {
        panic!("expected statement history_long");
    };
    assert!(
        long.rows.len() >= 2,
        "history_long should retain statement events"
    );

    let QueryResponse::ResultSet(summary) = engine
        .execute_with_session(
            &mut s,
            "SELECT COUNT_STAR, SUM_ROWS_SENT \
             FROM performance_schema.events_statements_summary_by_thread_by_event_name \
             WHERE THREAD_ID = 42424 AND EVENT_NAME = 'statement/sql/select'",
        )
        .unwrap()
    else {
        panic!("expected statement summary");
    };
    assert!(!summary.rows.is_empty(), "summary row for SELECT");
    assert!(matches!(summary.rows[0][0], Value::UInt(n) if n >= 1));
    assert!(matches!(summary.rows[0][1], Value::UInt(n) if n >= 1));

    let QueryResponse::ResultSet(digest) = engine
        .execute_with_session(
            &mut s,
            "SELECT COUNT_STAR \
             FROM performance_schema.events_statements_summary_by_digest \
             WHERE DIGEST_TEXT LIKE 'SELECT ? AS `observed`%'",
        )
        .unwrap()
    else {
        panic!("expected digest summary");
    };
    assert!(!digest.rows.is_empty(), "digest row for normalized SELECT");
    assert!(matches!(digest.rows[0][0], Value::UInt(n) if n >= 1));

    engine
        .execute_with_session(
            &mut s,
            "TRUNCATE TABLE performance_schema.events_statements_summary_by_thread_by_event_name",
        )
        .unwrap();
    let QueryResponse::ResultSet(empty_summary) = engine
        .execute_with_session(
            &mut s,
            "SELECT COUNT_STAR \
             FROM performance_schema.events_statements_summary_by_thread_by_event_name \
             WHERE THREAD_ID = 42424 AND EVENT_NAME = 'statement/sql/insert'",
        )
        .unwrap()
    else {
        panic!("expected empty statement summary");
    };
    assert!(empty_summary.rows.is_empty());

    crate::engine::perf_events::reset();
    crate::engine::perf_digest::reset();
}

#[test]
fn performance_schema_wait_events_record_lock_waits_when_enabled() {
    crate::engine::perf_events::reset();

    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(engine_with_lock_wait_ms(dir.path(), 2_000));
    let mut owner = SessionContext::default();
    owner.connection_id = 50_001;
    owner.user = "owner".to_string();
    owner.host = "client-owner".to_string();
    let mut waiter = SessionContext::default();
    waiter.connection_id = 50_002;
    waiter.user = "waiter".to_string();
    waiter.host = "client-waiter".to_string();

    engine
        .execute_with_session(
            &mut owner,
            "CREATE TABLE pfs_wait (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO pfs_wait VALUES (1,10)")
        .unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "UPDATE performance_schema.setup_consumers SET ENABLED='YES' \
             WHERE NAME IN ('events_waits_current','events_waits_history',\
                            'events_waits_history_long')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut owner,
            "UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' \
             WHERE NAME = 'wait/lock/table/sql/handler'",
        )
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE pfs_wait SET v = 11 WHERE id = 1")
        .unwrap();

    let waiter_engine = std::sync::Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        waiter_engine.execute_with_session(&mut waiter, "UPDATE pfs_wait SET v = 12 WHERE id = 1")
    });

    let mut saw_wait = false;
    for _ in 0..100 {
        let QueryResponse::ResultSet(waits) = engine
            .execute_with_session(
                &mut owner,
                "SELECT * FROM performance_schema.data_lock_waits",
            )
            .unwrap()
        else {
            panic!("expected data_lock_waits");
        };
        if !waits.rows.is_empty() {
            saw_wait = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(saw_wait, "blocked writer should appear in data_lock_waits");

    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    handle.join().unwrap().unwrap();

    let QueryResponse::ResultSet(history) = engine
        .execute_with_session(
            &mut owner,
            "SELECT THREAD_ID, EVENT_NAME, TIMER_WAIT \
             FROM performance_schema.events_waits_history_long \
             WHERE THREAD_ID = 50002",
        )
        .unwrap()
    else {
        panic!("expected wait history_long");
    };
    assert!(history.rows.iter().any(|row| {
        row[1] == Value::String("wait/lock/table/sql/handler".to_string())
            && matches!(row[2], Value::UInt(n) if n > 0)
    }));

    let QueryResponse::ResultSet(summary) = engine
        .execute_with_session(
            &mut owner,
            "SELECT USER, HOST, EVENT_NAME, COUNT_STAR, SUM_TIMER_WAIT \
             FROM performance_schema.events_waits_summary_by_account_by_event_name \
             WHERE USER = 'waiter' AND HOST = 'client-waiter'",
        )
        .unwrap()
    else {
        panic!("expected wait summary by account");
    };
    assert_eq!(summary.rows.len(), 1);
    assert_eq!(
        summary.rows[0][2],
        Value::String("wait/lock/table/sql/handler".to_string())
    );
    assert!(matches!(summary.rows[0][3], Value::UInt(n) if n >= 1));
    assert!(matches!(summary.rows[0][4], Value::UInt(n) if n > 0));

    engine
        .execute_with_session(
            &mut owner,
            "TRUNCATE TABLE performance_schema.events_waits_summary_by_account_by_event_name",
        )
        .unwrap();
    let QueryResponse::ResultSet(empty_summary) = engine
        .execute_with_session(
            &mut owner,
            "SELECT COUNT_STAR \
             FROM performance_schema.events_waits_summary_by_account_by_event_name \
             WHERE USER = 'waiter' AND HOST = 'client-waiter'",
        )
        .unwrap()
    else {
        panic!("expected empty wait summary");
    };
    assert!(empty_summary.rows.is_empty());

    crate::engine::perf_events::reset();
}

#[test]
fn performance_schema_data_locks_reflect_held_locks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    engine
        .execute_with_session(&mut s, "CREATE TABLE lk (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO lk VALUES (1,10),(2,20)")
        .unwrap();

    // No open transaction → no locks held.
    let QueryResponse::ResultSet(idle) = engine
        .execute_with_session(&mut s, "SELECT * FROM performance_schema.data_locks")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(idle.columns.len(), 15, "data_locks has 15 columns");
    assert!(idle.rows.is_empty(), "no locks before any transaction");

    // Open a transaction and take an exclusive row lock via UPDATE.
    engine.execute_with_session(&mut s, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut s, "UPDATE lk SET v = 11 WHERE id = 1")
        .unwrap();

    let QueryResponse::ResultSet(locks) = engine
        .execute_with_session(
            &mut s,
            "SELECT ENGINE, ENGINE_TRANSACTION_ID, OBJECT_SCHEMA, OBJECT_NAME, \
             LOCK_TYPE, LOCK_MODE, LOCK_STATUS, LOCK_DATA \
             FROM performance_schema.data_locks",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        !locks.rows.is_empty(),
        "held locks must appear in data_locks while the transaction is open"
    );
    // Every row is a granted MYDB lock on the table we just updated, with a
    // real transaction id.
    for r in &locks.rows {
        assert_eq!(r[0], Value::String("MYDB".to_string()), "ENGINE");
        assert!(
            matches!(&r[1], Value::UInt(t) if *t > 0),
            "ENGINE_TRANSACTION_ID is a real txn id, got {:?}",
            r[1]
        );
        assert_eq!(r[2], Value::String("test".to_string()), "OBJECT_SCHEMA");
        assert_eq!(r[3], Value::String("lk".to_string()), "OBJECT_NAME");
        assert_eq!(r[6], Value::String("GRANTED".to_string()), "LOCK_STATUS");
    }
    // The exclusive row lock from the UPDATE is present as a RECORD/X lock with
    // a physical row locator in LOCK_DATA.
    let record = locks
        .rows
        .iter()
        .find(|r| r[4] == Value::String("RECORD".to_string()))
        .expect("an exclusive RECORD lock from the UPDATE");
    assert_eq!(record[5], Value::String("X".to_string()), "row LOCK_MODE");
    assert!(
        matches!(&record[7], Value::String(d) if !d.is_empty()),
        "RECORD lock exposes its row locator in LOCK_DATA, got {:?}",
        record[7]
    );

    // No contention with a single session → data_lock_waits is empty.
    let QueryResponse::ResultSet(waits) = engine
        .execute_with_session(&mut s, "SELECT * FROM performance_schema.data_lock_waits")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(waits.columns.len(), 11, "data_lock_waits has 11 columns");
    assert!(waits.rows.is_empty(), "no waiters without contention");

    let QueryResponse::ResultSet(mdl) = engine
        .execute_with_session(&mut s, "SELECT * FROM performance_schema.metadata_locks")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        mdl.rows.iter().any(|r| {
            r[0] == Value::String("TABLE".to_string())
                && r[1] == Value::String("test".to_string())
                && r[2] == Value::String("lk".to_string())
                && r[7] == Value::String("GRANTED".to_string())
        }),
        "metadata_locks should expose the transaction MDL held on lk"
    );

    // Committing releases the locks.
    engine.execute_with_session(&mut s, "COMMIT").unwrap();
    let QueryResponse::ResultSet(after) = engine
        .execute_with_session(&mut s, "SELECT * FROM performance_schema.data_locks")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        after.rows.is_empty(),
        "locks released after COMMIT, got {} rows",
        after.rows.len()
    );
}

#[test]
fn performance_schema_all_tables_select_star_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    // Every registered performance_schema table must be queryable, and each
    // producer's row width must match its declared column count. This guards
    // the 39-column digest / 15-column data_locks alignments and any future
    // producer against silently drifting from its COLUMNS definition.
    for def in crate::engine::perf_schema::definitions() {
        let sql = format!("SELECT * FROM performance_schema.{}", def.name);
        match engine.execute_with_session(&mut s, &sql) {
            Ok(QueryResponse::ResultSet(rs)) => {
                assert_eq!(
                    rs.columns.len(),
                    def.columns.len(),
                    "column count mismatch for performance_schema.{}",
                    def.name
                );
                for row in &rs.rows {
                    assert_eq!(
                        row.len(),
                        def.columns.len(),
                        "row width mismatch for performance_schema.{}",
                        def.name
                    );
                }
            }
            Ok(other) => panic!(
                "SELECT * FROM performance_schema.{} returned {other:?}",
                def.name
            ),
            Err(e) => panic!(
                "SELECT * FROM performance_schema.{} failed: {e:?}",
                def.name
            ),
        }
    }
}

#[test]
fn performance_schema_global_status_exposes_real_counters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(
            &mut s,
            "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.global_status",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let names: Vec<&str> = rs
        .rows
        .iter()
        .filter_map(|r| match &r[0] {
            Value::String(name) => Some(name.as_str()),
            _ => None,
        })
        .collect();
    // Counters with a genuine data source (process uptime + connection metrics +
    // InnoDB-style buffer pool / log / row-lock counters + mydb-native
    // transaction / WAL / plan-cache observability — P1-D2).
    for expected in [
        "Uptime",
        "Threads_connected",
        "Threads_running",
        "Threads_created",
        "Connections",
        "Aborted_connects",
        "Aborted_clients",
        "Max_used_connections",
        "Innodb_buffer_pool_read_requests",
        "Innodb_buffer_pool_pages_data",
        "Innodb_buffer_pool_pages_free",
        "Innodb_buffer_pool_pages_dirty",
        "Innodb_buffer_pool_pages_total",
        "Innodb_buffer_pool_write_requests",
        "Innodb_buffer_pool_wait_free",
        "Innodb_pages_read",
        "Innodb_pages_written",
        "Innodb_data_reads",
        "Innodb_data_writes",
        "Innodb_log_writes",
        "Innodb_log_write_requests",
        "Innodb_os_log_written",
        "Innodb_row_lock_waits",
        "Innodb_row_lock_current_waits",
        "Innodb_row_lock_time",
        "Innodb_deadlocks",
        "Mydb_active_transactions",
        "Mydb_undo_bytes",
        "Mydb_undo_records",
        "Mydb_wal_bytes",
        "Mydb_wal_syncs",
        "Mydb_checkpoints_total",
        "Mydb_buffer_pool_evictions",
        "Mydb_plan_cache_hits",
        "Mydb_plan_cache_misses",
        "Mydb_txn_admission_waits",
        "Mydb_txn_admission_wait_us",
        "Mydb_mixed_admission_waits",
        "Mydb_mixed_admission_wait_us",
        "Mydb_parallel_read_pressure_entries",
        "Mydb_parallel_read_pressure_suppressed_rw_txn",
        "Mydb_parallel_read_pressure_active",
    ] {
        assert!(
            names.contains(&expected),
            "global_status must expose {expected}, got {names:?}"
        );
    }

    // session_status includes the current connection's session-only TLS
    // variables, while PFS deliberately excludes every Com_xxx counter.
    let QueryResponse::ResultSet(sess) = engine
        .execute_with_session(&mut s, "SELECT * FROM performance_schema.session_status")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let session_names: std::collections::BTreeSet<String> = sess
        .rows
        .iter()
        .map(|row| row[0].to_display_string())
        .collect();
    assert!(session_names.contains("Ssl_cipher"));
    assert!(session_names.contains("Ssl_version"));
    assert!(session_names.contains("Uptime"));
    assert!(session_names.iter().all(|name| !name.starts_with("Com_")));
}

#[test]
fn performance_schema_status_by_tables_use_session_lifecycle_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 77;
    session.user = "alice".to_string();
    let status = crate::engine::status_counters::SessionStatus::new_untracked();
    status.add(crate::engine::status_counters::SessionCounter::Questions, 5);
    session.status = Some(status.clone());
    let _handle = engine.registry.register_with_status(
        77,
        "alice".into(),
        "localhost:44000".into(),
        "test".into(),
        status,
    );

    let QueryResponse::ResultSet(thread_rows) = engine
        .execute_with_session(
            &mut session,
            "SELECT THREAD_ID, VARIABLE_NAME, VARIABLE_VALUE \
             FROM performance_schema.status_by_thread \
             WHERE THREAD_ID = 77 AND VARIABLE_NAME = 'Questions'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(thread_rows.rows.len(), 1);
    assert_eq!(thread_rows.rows[0][2].to_display_string(), "5");

    engine.registry.unregister(77);
    let QueryResponse::ResultSet(account_rows) = engine
        .execute_with_session(
            &mut session,
            "SELECT USER, HOST, VARIABLE_NAME, VARIABLE_VALUE \
             FROM performance_schema.status_by_account \
             WHERE USER = 'alice' AND HOST = 'localhost' \
               AND VARIABLE_NAME = 'Questions'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(account_rows.rows.len(), 1);
    assert_eq!(account_rows.rows[0][3].to_display_string(), "5");
}

#[test]
fn roles_and_show_grants_reflect_auth_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE ROLE 'reader'@'%'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT SELECT ON test.t TO 'reader'@'%'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT 'reader'@'%' TO 'bob'@'%'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "SET DEFAULT ROLE 'reader'@'%' TO 'bob'@'%'")
        .unwrap();

    let mut bob = SessionContext::default();
    bob.user = "bob".to_string();
    bob.login_user = "bob".to_string();
    bob.authenticated_as = crate::engine::auth::AccountKey::new("bob", "%");
    bob.active_roles = engine
        .db
        .auth_catalog()
        .active_default_roles(&bob.authenticated_as)
        .into();
    bob.auth_epoch = engine.db.auth_catalog().epoch;

    engine
        .execute_with_session(&mut bob, "SELECT id FROM t")
        .unwrap();
    let QueryResponse::ResultSet(grants) = engine
        .execute_with_session(&mut root, "SHOW GRANTS FOR 'bob'@'%'")
        .unwrap()
    else {
        panic!("expected grants");
    };
    assert!(matches!(&grants.rows[0][0], Value::String(s) if s.contains("reader")));
}

#[test]
fn mysql_schema_privilege_tables_reflect_auth_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();

    engine
        .execute_with_session(
            &mut root,
            "CREATE TABLE t (id INT PRIMARY KEY, email VARCHAR(64))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE PROCEDURE p() SELECT 1")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "CREATE ROLE 'reader'@'%'")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'alice'@'%'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT SELECT ON test.* TO 'alice'@'%'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT SELECT, INSERT ON test.t TO 'reader'@'%'")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "GRANT EXECUTE ON PROCEDURE test.p TO 'reader'@'%'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT 'reader'@'%' TO 'alice'@'%'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "SET DEFAULT ROLE 'reader'@'%' TO 'alice'@'%'")
        .unwrap();

    engine
        .db
        .mutate_auth_catalog(|auth| {
            auth.grants.push(crate::engine::auth::GrantEntry {
                updated_ms: crate::storage::naming::current_unix_ms(),
                grantor: crate::engine::auth::AccountKey::new("root", "localhost"),
                grantee: crate::engine::auth::AccountKey::new("reader", "%"),
                privilege: crate::engine::auth::Privilege::Update,
                scope: crate::engine::auth::PrivilegeScope::Column {
                    database: "test".to_string(),
                    table: "t".to_string(),
                    column: "email".to_string(),
                },
                grant_option: false,
                origin: crate::engine::auth::GrantOrigin::Explicit,
            });
            Ok(())
        })
        .unwrap();

    let QueryResponse::ResultSet(user) = engine
        .execute_with_session(
            &mut root,
            "SELECT User, Select_priv FROM mysql.user WHERE User = 'alice'",
        )
        .unwrap()
    else {
        panic!("expected mysql.user rows");
    };
    assert_eq!(
        user.rows,
        vec![vec![
            Value::String("alice".to_string()),
            Value::String("N".to_string())
        ]]
    );

    let QueryResponse::ResultSet(global) = engine
        .execute_with_session(
            &mut root,
            "SELECT PRIV, WITH_GRANT_OPTION FROM mysql.global_grants WHERE USER = 'alice'",
        )
        .unwrap()
    else {
        panic!("expected mysql.global_grants rows");
    };
    assert_eq!(
        global.rows,
        vec![vec![
            Value::String("SYSTEM_VARIABLES_ADMIN".to_string()),
            Value::String("N".to_string())
        ]]
    );

    let QueryResponse::ResultSet(db_rows) = engine
        .execute_with_session(
            &mut root,
            "SELECT Select_priv, Insert_priv FROM mysql.db WHERE User = 'alice' AND Db = 'test'",
        )
        .unwrap()
    else {
        panic!("expected mysql.db rows");
    };
    assert_eq!(
        db_rows.rows,
        vec![vec![
            Value::String("Y".to_string()),
            Value::String("N".to_string())
        ]]
    );

    let QueryResponse::ResultSet(table_rows) = engine
        .execute_with_session(
            &mut root,
            "SELECT Table_priv FROM mysql.tables_priv WHERE User = 'reader' AND Table_name = 't'",
        )
        .unwrap()
    else {
        panic!("expected mysql.tables_priv rows");
    };
    assert_eq!(
        table_rows.rows,
        vec![vec![Value::String("Select,Insert".to_string())]]
    );

    let QueryResponse::ResultSet(column_rows) = engine
        .execute_with_session(
            &mut root,
            "SELECT Column_priv FROM mysql.columns_priv WHERE User = 'reader' AND Column_name = 'email'",
        )
        .unwrap()
    else {
        panic!("expected mysql.columns_priv rows");
    };
    assert_eq!(
        column_rows.rows,
        vec![vec![Value::String("Update".to_string())]]
    );

    let QueryResponse::ResultSet(proc_rows) = engine
        .execute_with_session(
            &mut root,
            "SELECT Proc_priv FROM mysql.procs_priv WHERE User = 'reader' AND Routine_name = 'p'",
        )
        .unwrap()
    else {
        panic!("expected mysql.procs_priv rows");
    };
    assert_eq!(
        proc_rows.rows,
        vec![vec![Value::String("Execute".to_string())]]
    );

    let QueryResponse::ResultSet(edges) = engine
        .execute_with_session(
            &mut root,
            "SELECT FROM_USER, TO_USER FROM mysql.role_edges WHERE TO_USER = 'alice'",
        )
        .unwrap()
    else {
        panic!("expected mysql.role_edges rows");
    };
    assert_eq!(
        edges.rows,
        vec![vec![
            Value::String("reader".to_string()),
            Value::String("alice".to_string())
        ]]
    );

    let QueryResponse::ResultSet(defaults) = engine
        .execute_with_session(
            &mut root,
            "SELECT DEFAULT_ROLE_USER FROM mysql.default_roles WHERE USER = 'alice'",
        )
        .unwrap()
    else {
        panic!("expected mysql.default_roles rows");
    };
    assert_eq!(
        defaults.rows,
        vec![vec![Value::String("reader".to_string())]]
    );
}

#[test]
fn prepared_template_is_qualified_once_and_survives_database_switch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE DATABASE app")
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE app")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let mut template = parse_statement("SELECT v FROM t WHERE id = ?").unwrap();
    let mut next = 0;
    number_statement_placeholders_for_test(&mut template, &mut next);
    let qualified = engine
        .prepare_statement_template(&mut session, "SELECT v FROM t WHERE id = ?", template)
        .unwrap();

    engine
        .execute_with_session(&mut session, "USE test")
        .unwrap();
    let mut bound = qualified.clone();
    bind_statement_placeholders_for_test(&mut bound, &[Value::Int(1)]).unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_qualified_parsed_statement(&mut session, "SELECT v FROM t WHERE id = ?", bound)
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(10)]]);

    let err = engine
        .execute_with_session(&mut session, "SELECT v FROM t WHERE id = 1")
        .unwrap_err();
    assert_eq!(err.code(), 1146);
}

#[test]
fn prepared_limit_offset_and_dml_order_limit_execute() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
        )
        .unwrap();

    let select = Statement::Select(Select {
        projection: vec![SelectItem::Expr {
            expr: Expr::Identifier("id".into()),
            alias: None,
        }],
        table: Some("t".into()),
        from: vec![TableRef {
            name: "t".into(),
            alias: None,
            system_time: None,
            index_hints: vec![],
            partitions: vec![],
            column_aliases: Vec::new(),
            generate_series: None,
        }],
        values: None,
        json_table: None,
        derived: None,
        no_merge: false,
        straight_join: false,
        joins: Vec::new(),
        selection: None,
        group_by: Vec::new(),
        grouping_sets: Vec::new(),
        group_by_modifier: crate::parser::GroupByModifier::None,
        having: None,
        order_by: vec![OrderBy {
            expr: Expr::Identifier("id".into()),
            asc: true,
            nulls_first: None,
        }],
        limit: Some(LimitValue::Rows(2)),
        offset: Some(LimitValue::Rows(1)),
        distinct: false,
        for_update: false,
        lock_clause: None,
        sql_calc_found_rows: false,
        named_windows: Vec::new(),
        query_hints: vec![],
        hint_warnings: Vec::new(),
    });
    let QueryResponse::ResultSet(selected) = engine
        .execute_prepared_for_test(
            &mut session,
            "SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?",
            select,
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        selected.rows,
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );

    let update = Statement::Update(Update {
        table: "t".into(),
        assignments: vec![Assignment {
            column: "v".into(),
            value: Expr::Binary {
                left: Box::new(Expr::Identifier("v".into())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(Literal::Int(100))),
            },
            table_qualifier: None,
        }],
        selection: Some(Expr::Binary {
            left: Box::new(Expr::Identifier("id".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(0))),
        }),
        view_check_predicate: None,
        order_by: vec![OrderBy {
            expr: Expr::Identifier("v".into()),
            asc: false,
            nulls_first: None,
        }],
        limit: Some(LimitValue::Rows(1)),
        ignore: false,
        low_priority: false,
        primary_alias: None,
        joins: Vec::new(),
        partitions: Vec::new(),
        query_hints: Vec::new(),
        hint_warnings: Vec::new(),
        returning: None,
    });
    engine
        .execute_prepared_for_test(
            &mut session,
            "UPDATE t SET v = v + ? WHERE id > ? ORDER BY v DESC LIMIT ?",
            update,
        )
        .unwrap();

    let delete = Statement::Delete(Delete {
        table: "t".into(),
        selection: Some(Expr::Binary {
            left: Box::new(Expr::Identifier("id".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Literal::Int(0))),
        }),
        order_by: vec![OrderBy {
            expr: Expr::Identifier("v".into()),
            asc: true,
            nulls_first: None,
        }],
        limit: Some(LimitValue::Rows(1)),
        primary_alias: None,
        joins: Vec::new(),
        targets: Vec::new(),
        partitions: Vec::new(),
        low_priority: false,
        quick: false,
        ignore: false,
        query_hints: Vec::new(),
        hint_warnings: Vec::new(),
        returning: None,
    });
    engine
        .execute_prepared_for_test(
            &mut session,
            "DELETE FROM t WHERE id > ? ORDER BY v LIMIT ?",
            delete,
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT id, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)],
            vec![Value::Int(4), Value::Int(140)],
        ]
    );
}

#[test]
fn shutdown_runs_final_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1)").unwrap();
    engine.shutdown().unwrap();

    let scan = crate::wal::LogManager::scan(dir.path()).unwrap();
    assert!(
        matches!(
            scan.records.as_slice(),
            [crate::wal::WalRecord {
                payload: crate::wal::WalPayload::CheckpointEnd { .. },
                ..
            }]
        ),
        "graceful shutdown must leave one self-contained recovery point, got {:?}",
        scan.records
            .iter()
            .map(|record| record.payload.record_type())
            .collect::<Vec<_>>()
    );

    let QueryResponse::ResultSet(status) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected status result set");
    };
    assert!(status.rows.iter().any(|row| row[1]
        == Value::String("last_checkpoint_unix_ms".into())
        && row[2] != Value::String(String::new())));
}

#[test]
fn alter_system_checkpoint_runs_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1)").unwrap();
    engine.execute("ALTER SYSTEM CHECKPOINT").unwrap();

    let QueryResponse::ResultSet(status) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected status result set");
    };
    assert!(status.rows.iter().any(|row| row[1]
        == Value::String("last_checkpoint_unix_ms".into())
        && row[2] != Value::String(String::new())));
}

#[test]
fn alter_system_flush_data_cache_evicts_unpinned_buffer_pages() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let _ = engine.execute("SELECT * FROM t").unwrap();
    assert!(engine.database().storage_status().buffer_pool_used_pages > 0);

    engine.execute("ALTER SYSTEM FLUSH DATA_CACHE").unwrap();

    let status = engine.database().storage_status();
    assert_eq!(status.buffer_pool_dirty_pages, 0);
    assert_eq!(status.buffer_pool_used_pages, 0);
    assert!(status.buffer_pool_flush_data_cache_pages > 0);
    assert!(status.buffer_pool_flush_data_cache_us > 0);

    let QueryResponse::ResultSet(rows) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap() else {
        panic!("expected status result set");
    };
    for metric in [
        "buffer_pool_flush_data_cache_pages",
        "buffer_pool_flush_data_cache_us",
    ] {
        assert!(
            rows.rows
                .iter()
                .any(|row| row[1] == Value::String(metric.into())),
            "SHOW ENGINE MYDB STATUS should expose {metric}"
        );
    }
}

#[test]
fn alter_system_flush_sql_cache_clears_statement_and_plan_caches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT)")
        .unwrap();
    engine.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    engine.execute("INSERT INTO b VALUES (1, 1)").unwrap();
    engine
        .prepare_cached(&mut session, "SELECT * FROM a WHERE id = ?", 1)
        .unwrap();
    engine
        .execute("SELECT a.id FROM a JOIN b ON a.id = b.a_id")
        .unwrap();

    assert!(engine.statement_cache().stats().size > 0);
    assert!(engine.plan_cache().stats().size > 0);

    engine.execute("ALTER SYSTEM FLUSH SQL_CACHE").unwrap();

    assert_eq!(engine.statement_cache().stats().size, 0);
    assert_eq!(engine.plan_cache().stats().size, 0);
}

#[test]
fn init_session_database_fast_path_sets_existing_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .init_session_database(&mut session, "test")
        .expect("default database should exist");
    assert_eq!(session.current_database, "test");

    let err = engine
        .init_session_database(&mut session, "missing_db")
        .unwrap_err();
    assert_eq!(err.code(), 1049);
    assert_eq!(err.sql_state(), "42000");
}

#[test]
fn show_engine_status_exposes_connection_management_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(status) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected status result set");
    };
    for metric in [
        "active_connections",
        "max_connections",
        "effective_max_connections",
        "admin_connections",
        "admin_connection_reserve",
        "accepted_connections",
        "rejected_connections",
        "handshake_successes",
        "handshake_failures",
        "closed_connections",
        "listen_backlog",
        "handshake_timeout_secs",
        "open_files_soft_limit",
        "open_files_hard_limit",
        "open_files_requested_limit",
        "open_files_reserve",
    ] {
        assert!(
            status
                .rows
                .iter()
                .any(|row| row[1] == Value::String(metric.into())),
            "SHOW ENGINE MYDB STATUS should expose {metric}"
        );
    }
}

#[test]
fn spatial_index_requires_a_geometry_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // SPATIAL INDEX is now executed; it requires a geometry-typed column, so a
    // spatial index over an INT column is rejected with a clear error.
    let err = engine
        .execute("CREATE TABLE spatial_t (g INT NOT NULL, SPATIAL INDEX(g))")
        .unwrap_err();
    assert_eq!(err.code(), 1687);
    assert_eq!(err.sql_state(), "42000");
    assert_eq!(
        err.message(),
        "A SPATIAL index may only contain a geometrical type column"
    );

    // A spatial index over a real geometry column succeeds.
    engine
        .execute("CREATE TABLE geo_t (g GEOMETRY NOT NULL, SPATIAL INDEX(g))")
        .unwrap();
}

#[test]
fn storage_engine_executes_multi_statement_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine
        .execute("CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t")
        .unwrap()
    else {
        panic!("expected final SELECT result set");
    };

    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
#[ignore = "requires external BenchmarkSQL SQL files; set BSQL_RUN to its run directory"]
fn benchmarksql_mysql_ddl_files_execute() {
    let benchmarksql_run = std::env::var("BSQL_RUN")
        .expect("set BSQL_RUN to the BenchmarkSQL run directory before running this test");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE DATABASE tpcc").unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "USE tpcc")
        .unwrap();

    for file in [
        "sql.mysql/tableCreates.sql",
        "sql.mysql/indexCreates.sql",
        "sql.mysql/foreignKeys.sql",
        "sql.mysql/buildFinish.sql",
    ] {
        let path = std::path::Path::new(&benchmarksql_run).join(file);
        let sql = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read BenchmarkSQL SQL file {}: {err}", path.display()));
        let statements = split_sql_script(&sql);
        assert!(
            !statements.is_empty(),
            "no SQL statements found in BenchmarkSQL file {}",
            path.display()
        );
        for statement in statements {
            engine
                .execute_with_session(&mut session, &statement)
                .unwrap_or_else(|err| panic!("{}: {statement}\n{err:?}", path.display()));
        }
    }
}

#[test]
fn prepared_describe_does_not_execute_probe_sql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let columns = engine
        .describe_prepared_statement(&mut session, "SELECT v FROM t WHERE id = ?")
        .unwrap();
    // After P0-7 (`select carries column source`), the prepared describe
    // path now reports the catalog-derived org_table/org_name so JDBC's
    // `ResultSetMetaData.getTableName()` works against prepared queries.
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "v");
    assert_eq!(columns[0].column_type, ColumnType::Long);
    assert_eq!(columns[0].source.org_table, "t");
    assert_eq!(columns[0].source.org_name, "v");

    let QueryResponse::ResultSet(result) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn prepared_describe_exact_aggregates_report_decimal_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, i INT, d DECIMAL(6,2), f DOUBLE)")
        .unwrap();

    let columns = engine
        .describe_prepared_statement(
            &mut session,
            "SELECT SUM(d) AS sum_d, AVG(d) AS avg_d, AVG(i) AS avg_i, SUM(f) AS sum_f \
             FROM t WHERE id > ?",
        )
        .unwrap();

    assert_eq!(columns.len(), 4);
    assert_eq!(
        columns[0].column_type,
        ColumnType::Decimal {
            precision: 28,
            scale: 2,
        }
    );
    assert_eq!(
        columns[1].column_type,
        ColumnType::Decimal {
            precision: 10,
            scale: 6,
        }
    );
    assert_eq!(
        columns[2].column_type,
        ColumnType::Decimal {
            precision: 14,
            scale: 4,
        }
    );
    assert_eq!(columns[3].column_type, ColumnType::Double);
}

#[test]
fn prepared_describe_uses_placeholder_context_and_type_session_variables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (v VARCHAR(10))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET NAMES utf8mb4")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET group_concat_max_len=400")
        .unwrap();
    let short = engine
        .describe_prepared_statement(&mut session, "SELECT GROUP_CONCAT(v) FROM t WHERE v <> ?")
        .unwrap();
    assert_eq!(short[0].column_type, ColumnType::VarString);
    assert_eq!(short[0].source.length, Some(1600));

    engine
        .execute_with_session(&mut session, "SET group_concat_max_len=1024")
        .unwrap();
    let long = engine
        .describe_prepared_statement(&mut session, "SELECT GROUP_CONCAT(v) FROM t WHERE v <> ?")
        .unwrap();
    assert_eq!(long[0].column_type, ColumnType::Blob);
    assert_eq!(long[0].source.length, Some(65_536));

    let control = engine
        .describe_prepared_statement(
            &mut session,
            "SELECT CASE WHEN ? THEN CAST(? AS SIGNED) ELSE CAST(? AS UNSIGNED) END AS x",
        )
        .unwrap();
    assert_eq!(
        control[0].column_type,
        ColumnType::Decimal {
            precision: 21,
            scale: 0
        }
    );
    assert_eq!(control[0].source.length, Some(22));
    assert_ne!(
        control[0].source.flags & crate::mysql::protocol::BINARY_FLAG,
        0
    );

    let ifnull = engine
        .describe_prepared_statement(&mut session, "SELECT IFNULL(?, 'test') AS x")
        .unwrap();
    assert_eq!(ifnull[0].column_type, ColumnType::VarString);
    assert_eq!(ifnull[0].source.length, Some(84));
    assert_ne!(
        ifnull[0].source.flags & crate::mysql::protocol::NOT_NULL_FLAG,
        0
    );
}

#[test]
fn subquery_tables_are_qualified_with_current_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine.execute("CREATE DATABASE tpcc").unwrap();
    engine
        .execute_with_session(&mut session, "USE tpcc")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE orders (id INT PRIMARY KEY, c_id INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO orders VALUES (1, 7), (2, 7), (3, 8)",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT id FROM orders WHERE id = (SELECT MAX(id) FROM orders WHERE c_id = 7)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);
}

fn split_sql_script(sql: &str) -> Vec<String> {
    sql.lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(str::to_string)
        .collect()
}

#[test]
fn mysql_general_log_table_persists_and_truncates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .sql_log_for_tests()
        .set_global_variable("log_output", "TABLE")
        .unwrap();
    engine
        .sql_log_for_tests()
        .set_global_variable("general_log", "ON")
        .unwrap();

    let mut session = SessionContext::default();
    session.connection_id = 99;
    session.user = "root".into();
    QueryEngine::log_general_event(&engine, &session, "Query", "SELECT 1");

    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(
            &mut session,
            "SELECT command_type, argument FROM mysql.general_log",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::String("Query".into()));
    assert_eq!(rows.rows[0][1], Value::Bytes(b"SELECT 1".to_vec()));

    drop(engine);
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT command_type FROM mysql.general_log")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows.len(), 1);

    engine
        .execute_with_session(&mut session, "TRUNCATE TABLE mysql.general_log")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT * FROM mysql.general_log")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(rows.rows.is_empty());
}

#[test]
fn mysql_slow_log_table_records_finished_statements() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let config = engine.sql_log_for_tests().config_snapshot();
    assert!(config.slow_query_log);
    assert_eq!(config.log_output.as_mysql_value(), "TABLE");
    assert_eq!(config.long_query_time, std::time::Duration::from_secs(1));
    engine
        .sql_log_for_tests()
        .set_global_variable("long_query_time", "0")
        .unwrap();

    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SELECT 1")
        .unwrap();

    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(
            &mut session,
            "SELECT sql_text, rows_sent, thread_id FROM mysql.slow_log",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(!rows.rows.is_empty());
    assert_eq!(rows.rows[0][0], Value::Bytes(b"SELECT 1".to_vec()));
    assert_eq!(rows.rows[0][1], Value::UInt(1));
    assert_eq!(rows.rows[0][2], Value::UInt(session.connection_id as u64));
}

#[test]
fn logging_variables_and_flush_logs_are_sql_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET SESSION sql_log_off = ON")
        .unwrap();
    assert!(session.sql_log_off);
    let QueryResponse::ResultSet(selected) = engine
        .execute_with_session(&mut session, "SELECT @@session.sql_log_off")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(selected.rows[0][0], Value::String("1".into()));

    engine
        .execute_with_session(&mut session, "SET GLOBAL log_output = 'TABLE'")
        .unwrap();
    let QueryResponse::ResultSet(vars) = engine
        .execute_with_session(&mut session, "SHOW VARIABLES LIKE 'log_output'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(vars.rows.contains(&vec![
        Value::String("log_output".into()),
        Value::String("TABLE".into())
    ]));

    assert!(matches!(
        parse_statement("FLUSH LOGS").unwrap(),
        Statement::FlushLogs {
            kind: crate::engine::logging::FlushLogKind::All
        }
    ));
    engine
        .execute_with_session(&mut session, "FLUSH GENERAL LOGS")
        .unwrap();
}

/// End-to-end `DECLARE` / `FETCH` / `CLOSE` against a real engine. Verifies:
///   * DECLARE returns `Ok` and registers the cursor (visible in
///     `cursor_registry::named_snapshot()`).
///   * FETCH advances by exactly the requested count and resumes from
///     the right offset on the next FETCH.
///   * CLOSE removes the cursor; a follow-up FETCH errors with the
///     standard "cursor not open" SQLSTATE 24000.
///   * A second open with the same name on the same connection is
///     rejected (ER_SP_CURSOR_ALREADY_OPEN).
#[test]
fn declare_fetch_close_sql_cursor_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 7711;
    crate::engine::cursor_registry::named_close_for_connection(session.connection_id);

    engine
        .execute_with_session(&mut session, "CREATE TABLE t (v INT)")
        .unwrap();
    for v in 1..=5 {
        engine
            .execute_with_session(&mut session, &format!("INSERT INTO t(v) VALUES ({v})"))
            .unwrap();
    }

    let resp = engine
        .execute_with_session(
            &mut session,
            "DECLARE c1 CURSOR FOR SELECT v FROM t ORDER BY v",
        )
        .unwrap();
    assert!(matches!(resp, QueryResponse::Ok));
    let snap = crate::engine::cursor_registry::named_snapshot();
    assert!(snap
        .iter()
        .any(|s| s.connection_id == session.connection_id && s.name == "c1"));

    let fetched_1 = engine
        .execute_with_session(&mut session, "FETCH 3 FROM c1")
        .unwrap();
    let QueryResponse::ResultSet(rs1) = fetched_1 else {
        panic!("expected result set");
    };
    assert_eq!(rs1.rows.len(), 3);
    assert_eq!(rs1.rows[0], vec![Value::Int(1)]);
    assert_eq!(rs1.rows[2], vec![Value::Int(3)]);

    let fetched_2 = engine
        .execute_with_session(&mut session, "FETCH 10 FROM c1")
        .unwrap();
    let QueryResponse::ResultSet(rs2) = fetched_2 else {
        panic!("expected result set");
    };
    assert_eq!(rs2.rows.len(), 2);
    assert_eq!(rs2.rows[0], vec![Value::Int(4)]);
    assert_eq!(rs2.rows[1], vec![Value::Int(5)]);

    // Cursor not yet closed — registry still knows about it.
    assert!(crate::engine::cursor_registry::named_snapshot()
        .iter()
        .any(|s| s.connection_id == session.connection_id && s.name == "c1"));

    // Re-DECLARE on the same name is rejected.
    let err = engine
        .execute_with_session(&mut session, "DECLARE c1 CURSOR FOR SELECT v FROM t")
        .unwrap_err();
    assert_eq!(err.code(), 1325);

    engine
        .execute_with_session(&mut session, "CLOSE c1")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "FETCH 1 FROM c1")
        .unwrap_err();
    assert_eq!(err.code(), 1326);
    assert!(!crate::engine::cursor_registry::named_snapshot()
        .iter()
        .any(|s| s.connection_id == session.connection_id && s.name == "c1"));
}

#[test]
fn inline_cached_prepare_preserves_binding_and_placeholder_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE prepare_inline_t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let sql = "UPDATE prepare_inline_t SET v=? WHERE id=?";
    assert!(engine
        .try_prepare_cached_inline(&mut session, sql, 2)
        .is_none());
    let expected = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let (actual, columns) = engine
        .try_prepare_cached_inline(&mut session, sql, 2)
        .unwrap()
        .unwrap();
    assert_eq!(*actual, *expected);
    assert!(columns.is_empty());
    let inline_error = engine
        .try_prepare_cached_inline(&mut session, sql, 1)
        .unwrap()
        .unwrap_err();
    let ordinary_error = engine.prepare_cached(&mut session, sql, 1).unwrap_err();
    assert_eq!(inline_error, ordinary_error);
    let select = "SELECT v FROM prepare_inline_t WHERE id=?";
    engine.prepare_cached(&mut session, select, 1).unwrap();
    assert!(engine
        .try_prepare_cached_inline(&mut session, select, 1)
        .is_none());
    // The shared parser cache never stores a session-qualified AST.
    engine.execute("CREATE DATABASE inline_other").unwrap();
    engine
        .execute("CREATE TABLE inline_other.prepare_inline_t (id INT PRIMARY KEY, v VARCHAR(10))")
        .unwrap();
    session.current_database = "inline_other".into();
    let expected = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let (actual, _) = engine
        .try_prepare_cached_inline(&mut session, sql, 2)
        .unwrap()
        .unwrap();
    assert_eq!(*actual, *expected);
    let Statement::Update(update) = actual.as_ref() else {
        panic!("UPDATE")
    };
    assert_eq!(update.table, "inline_other.prepare_inline_t");
}

#[test]
fn automatic_statistics_refresh_invalidates_prepared_and_shared_plans() {
    use crate::engine::api::PreparedSelectMemoState;
    use std::sync::Arc;
    for background in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        let mut session = SessionContext::default();
        for name in ["stats_plan_changed", "stats_plan_untouched"] {
            engine
                .execute(&format!("CREATE TABLE {name} (id INT PRIMARY KEY, v INT)"))
                .unwrap();
            let rows = (1..=200)
                .map(|id| format!("({id},{})", id * 10))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO {name} VALUES {rows}"))
                .unwrap();
            engine.execute(&format!("ANALYZE TABLE {name}")).unwrap();
        }
        let untouched = engine
            .db_for_tests()
            .table("stats_plan_untouched")
            .unwrap()
            .schema_version;
        let sql = "SELECT v FROM stats_plan_changed WHERE id=?";
        let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let mut state = PreparedSelectMemoState::default();
        engine
            .execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(1)])
            .unwrap()
            .unwrap();
        let PreparedSelectMemoState::Ready(before) = &state else {
            panic!("expected cached prepared plan")
        };
        let before = Arc::clone(before);
        let rows = (201..=300)
            .map(|id| format!("({id},{})", id * 10))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO stats_plan_changed VALUES {rows}"))
            .unwrap();
        if background {
            assert_eq!(
                engine
                    .db_for_tests()
                    .auto_analyze_stale_tables(0.1)
                    .unwrap(),
                1
            );
        } else {
            engine
                .db_for_tests()
                .refresh_stale_stats_for_tables(["stats_plan_changed"])
                .unwrap();
        }
        assert!(
            !engine
                .db_for_tests()
                .table_versions_match(&before.compiled.deps),
            "new optimizer statistics must invalidate plans even without explicit ANALYZE"
        );
        assert_eq!(
            engine
                .db_for_tests()
                .table("stats_plan_untouched")
                .unwrap()
                .schema_version,
            untouched
        );
        engine
            .execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(3)])
            .unwrap()
            .unwrap();
        let PreparedSelectMemoState::Ready(after) = &state else {
            panic!("expected recompiled plan")
        };
        assert!(!Arc::ptr_eq(&before.compiled, &after.compiled));
        let after = Arc::clone(after);
        let mut other_session = SessionContext::default();
        let mut other_state = PreparedSelectMemoState::default();
        engine
            .execute_prepared_select(
                &mut other_session,
                &mut other_state,
                &parsed,
                sql,
                &[Value::Int(2)],
            )
            .unwrap()
            .unwrap();
        let PreparedSelectMemoState::Ready(shared) = &other_state else {
            panic!("expected shared plan")
        };
        assert!(Arc::ptr_eq(&after.compiled, &shared.compiled));
        let version = engine
            .db_for_tests()
            .table("stats_plan_changed")
            .unwrap()
            .schema_version;
        engine
            .db_for_tests()
            .refresh_stale_stats_for_tables(["stats_plan_changed"])
            .unwrap();
        assert_eq!(
            engine
                .db_for_tests()
                .table("stats_plan_changed")
                .unwrap()
                .schema_version,
            version,
            "checking already-fresh statistics must not invalidate plans"
        );
    }
}
