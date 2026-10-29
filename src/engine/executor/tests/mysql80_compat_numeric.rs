//! MySQL 8.0 numeric types and mathematical function regressions.
//!
//! Oracle: local MySQL 9.6, 2026-06-12, /tmp/mydb-numeric-ground-truth.md:
//! ROUND/TRUNCATE/CAST DECIMAL use exact fixed-point arithmetic for exact inputs,
//! round halves away from zero, and preserve input-dependent result types.
//! Implicit string-to-number conversion uses prefixes with warning 1292; empty string gives 0 silently.
//! LN/LOG on nonpositive values return NULL with warning 3020; COT(0), POW, and EXP overflow
//! return 1690. Invalid SQRT/ASIN/ACOS domains return NULL without warning.
//! Division by zero returns NULL with warning 1365 when default sql_mode includes
//! ERROR_FOR_DIVISION_BY_ZERO; explicitly removing that bit makes it silent.
//! - AVG(exact) → DECIMAL(scale + div_precision_increment);
//! DECIMAL writes round excess scale, e.g. 1.005 to 1.01, without warning.
//! CONV saturates u64 with warning 1292 and wraps negative values.
//! - ABS(-2^63) → 1690;CAST(1 AS UNSIGNED)-10 → 1690,
//! With NO_UNSIGNED_SUBTRACTION, the result is -9.
use super::*;

fn rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected result set, got {other:?}"),
    }
}

fn one_row(engine: &StorageQueryEngine, sql: &str) -> Vec<Value> {
    let mut all = rows(engine.execute(sql).unwrap());
    assert_eq!(all.len(), 1, "expected single row for: {sql}");
    all.pop().unwrap()
}

#[test]
fn unified_expression_descriptors_match_mysql80_domains_on_empty_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE type_domain_t(
                i INT,
                s BIGINT,
                u BIGINT UNSIGNED,
                d DECIMAL(6,2),
                day DATE,
                moment DATETIME(6)
            )",
        )
        .unwrap();

    let QueryResponse::ResultSet(control) = engine
        .execute(
            "SELECT
                CASE WHEN i = 0 THEN s ELSE u END AS mixed_integer,
                CASE WHEN i = 0 THEN day ELSE moment END AS mixed_temporal
             FROM type_domain_t WHERE 0",
        )
        .unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(
        control.columns[0].column_type,
        ColumnType::Decimal {
            precision: 21,
            scale: 0
        }
    );
    assert_eq!(control.columns[1].column_type, ColumnType::DateTime);
    assert_eq!(control.columns[1].source.decimals, Some(6));

    let QueryResponse::ResultSet(aggregates) = engine
        .execute(
            "SELECT SUM(i) AS sum_i, SUM(d) AS sum_d, AVG(i) AS avg_i
             FROM type_domain_t WHERE 0",
        )
        .unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(
        aggregates.columns[0].column_type,
        ColumnType::Decimal {
            precision: 32,
            scale: 0
        }
    );
    assert_eq!(
        aggregates.columns[1].column_type,
        ColumnType::Decimal {
            precision: 28,
            scale: 2
        }
    );
    assert_eq!(
        aggregates.columns[2].column_type,
        ColumnType::Decimal {
            precision: 14,
            scale: 4
        }
    );

    let set_response = engine
        .execute(
            "SELECT s AS v FROM type_domain_t WHERE 0
             UNION ALL
             SELECT u AS v FROM type_domain_t WHERE 0",
        )
        .unwrap();
    let set_columns = match &set_response {
        QueryResponse::ResultSet(result) => &result.columns,
        QueryResponse::StreamingResultSet(result) => &result.columns,
        other => panic!("expected UNION ALL result, got {other:?}"),
    };
    assert_eq!(
        set_columns[0].column_type,
        ColumnType::Decimal {
            precision: 21,
            scale: 0
        }
    );
}

#[test]
fn const_control_flow_uses_bound_type_for_metadata_and_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT
                CASE WHEN 1 THEN CAST(1 AS SIGNED) ELSE CAST(1 AS UNSIGNED) END AS mixed_integer,
                CASE WHEN 1 THEN CAST('2024-01-01' AS DATE)
                     ELSE CAST('2024-01-01 00:00:00.123456' AS DATETIME(6)) END AS mixed_temporal,
                IFNULL(1, 'test') AS nonnull_string",
        )
        .unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(
        result.columns[0].column_type,
        ColumnType::Decimal {
            precision: 21,
            scale: 0
        }
    );
    assert_eq!(result.columns[1].column_type, ColumnType::DateTime);
    assert_eq!(result.columns[1].source.decimals, Some(6));
    assert_ne!(
        result.columns[0].source.flags & crate::mysql::protocol::BINARY_FLAG,
        0
    );
    assert_ne!(
        result.columns[2].source.flags & crate::mysql::protocol::NOT_NULL_FLAG,
        0
    );
    assert!(matches!(result.rows[0][0], Value::DecimalCompact(1, 0)));
    let Value::DateTime(micros) = result.rows[0][1] else {
        panic!("mixed temporal branch must be coerced to DATETIME")
    };
    assert_eq!(
        crate::types::datetime_components(micros),
        (2024, 1, 1, 0, 0, 0, 0)
    );
}

#[test]
fn compiled_control_flow_coerces_every_temporal_branch_to_bound_fsp() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE temporal_case(
                id INT, day DATE, moment DATETIME(6)
             )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO temporal_case VALUES
                (0, '2024-01-01', '2024-01-02 03:04:05.123456'),
                (1, '2024-01-01', '2024-01-02 03:04:05.123456')",
        )
        .unwrap();
    let raw = one_row(&engine, "SELECT moment FROM temporal_case WHERE id = 1");
    let Value::DateTime(raw_micros) = raw[0] else {
        panic!("DATETIME(6) source must stay typed")
    };
    assert_eq!(
        crate::types::datetime_components(raw_micros),
        (2024, 1, 2, 3, 4, 5, 123_456)
    );
    let crate::parser::Statement::Select(parsed) = crate::parser::parse_statement(
        "SELECT CASE WHEN id = 0 THEN day ELSE moment END FROM temporal_case",
    )
    .unwrap() else {
        panic!("expected SELECT")
    };
    let crate::parser::SelectItem::Expr { expr, .. } = &parsed.projection[0] else {
        panic!("expected expression")
    };
    let table = engine.database().table("test.temporal_case").unwrap();
    let schema = crate::engine::expr_program::InputSchema::from_table(&table);
    let program = crate::engine::expr_program::ExprCompiler::compile(expr, &schema).unwrap();
    assert_eq!(
        program.result_type().kind,
        crate::engine::expr_type::ExprTypeKind::Temporal {
            kind: crate::engine::expr_type::TemporalKind::DateTime,
            fsp: 6
        }
    );
    let QueryResponse::ResultSet(first_result) = engine
        .execute(
            "SELECT CASE WHEN id = 0 THEN day ELSE moment END AS value
             FROM temporal_case WHERE id = 0",
        )
        .unwrap()
    else {
        panic!("expected result set")
    };
    let QueryResponse::ResultSet(second_result) = engine
        .execute(
            "SELECT CASE WHEN id = 0 THEN day ELSE moment END AS value
             FROM temporal_case WHERE id = 1",
        )
        .unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(first_result.columns[0].column_type, ColumnType::DateTime);
    assert_eq!(first_result.columns[0].source.decimals, Some(6));
    let Value::DateTime(first) = first_result.rows[0][0] else {
        panic!("DATE branch must be promoted to DATETIME")
    };
    let Value::DateTime(second) = second_result.rows[0][0] else {
        panic!("DATETIME branch must remain DATETIME")
    };
    assert_eq!(
        crate::types::datetime_components(first),
        (2024, 1, 1, 0, 0, 0, 0)
    );
    assert_eq!(
        crate::types::datetime_components(second),
        (2024, 1, 2, 3, 4, 5, 123_456)
    );
    let QueryResponse::ResultSet(ordered) = engine
        .execute(
            "SELECT CASE WHEN id = 0 THEN day ELSE moment END AS value
             FROM temporal_case ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set")
    };
    let Value::DateTime(ordered_second) = ordered.rows[1][0] else {
        panic!("ordered plan must retain DATETIME(6)")
    };
    assert_eq!(
        crate::types::datetime_components(ordered_second),
        (2024, 1, 2, 3, 4, 5, 123_456)
    );
}

#[test]
fn fixed_builtin_and_ctas_use_bound_descriptor_not_runtime_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(result) = engine.execute("SELECT CONNECTION_ID() AS id").unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(result.columns[0].column_type, ColumnType::LongLong);
    assert_ne!(
        result.columns[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
        0
    );
    assert_ne!(
        result.columns[0].source.flags & crate::mysql::protocol::NOT_NULL_FLAG,
        0
    );

    engine
        .execute(
            "CREATE TABLE expression_ctas_t AS
             SELECT CONNECTION_ID() AS id, IFNULL('a', 'test') AS label, NOW(6) AS created_at",
        )
        .unwrap();
    let description = rows(engine.execute("DESCRIBE expression_ctas_t").unwrap());
    assert_eq!(description[0][1].to_display_string(), "bigint unsigned");
    assert_eq!(description[1][1].to_display_string(), "varchar(4)");
    assert_eq!(description[2][1].to_display_string(), "datetime(6)");

    engine
        .execute(
            "CREATE TABLE integer_domain_ctas_t AS
             SELECT 1 = 1 AS predicate_value, 8 DIV 2 + 1 AS quotient",
        )
        .unwrap();
    let integer_description = rows(engine.execute("DESCRIBE integer_domain_ctas_t").unwrap());
    assert_eq!(integer_description[0][1].to_display_string(), "int");
    assert_eq!(integer_description[0][2].to_display_string(), "NO");
    assert_eq!(integer_description[1][1].to_display_string(), "int");
    assert_eq!(integer_description[1][2].to_display_string(), "YES");
}

#[test]
fn exact_numeric_comparison_with_prepared_string_keeps_boundary_precision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE exact_cmp(i BIGINT, u BIGINT UNSIGNED, d DECIMAL(65,0))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO exact_cmp VALUES(\
             -9223372036854775808,18446744073709551615,\
             99999999999999999999999999999999999999999999999999999999999999999)",
        )
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute_with_session(
                    &mut session,
                    "SELECT d=100000000000000000000000000000000000000000000000000000000000000000,\
                            d<100000000000000000000000000000000000000000000000000000000000000000,\
                            d>-100000000000000000000000000000000000000000000000000000000000000000 \
                     FROM exact_cmp",
                )
                .unwrap(),
        ),
        vec![vec![Value::Int(0), Value::Int(1), Value::Int(1)]]
    );
    assert!(
        session.warnings.is_empty(),
        "unexpected warnings: {:?}",
        session.warnings
    );
    assert_eq!(
        rows(
            engine
                .execute_with_session(
                    &mut session,
                    "SELECT i='-9223372036854775809',\
                            u='18446744073709551616',\
                            d='100000000000000000000000000000000000000000000000000000000000000000'\
                     FROM exact_cmp",
                )
                .unwrap(),
        ),
        // BIGINT/string comparison retains MySQL's approximate context, but
        // a DECIMAL column compares the clean string as exact fixed point.
        vec![vec![Value::Int(1), Value::Int(1), Value::Int(0)]]
    );

    engine
        .execute_with_session(
            &mut session,
            "PREPARE exact_cmp_stmt FROM \
             'SELECT i=?,u=?,d=? FROM exact_cmp'",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SET @i='-9223372036854775809',\
                 @u='18446744073709551616',\
                 @d='100000000000000000000000000000000000000000000000000000000000000000'",
        )
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut session, "EXECUTE exact_cmp_stmt USING @i,@u,@d",)
                .unwrap(),
        ),
        vec![vec![Value::Int(0), Value::Int(0), Value::Int(0)]]
    );
}

fn one_row_s(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Value> {
    let mut all = rows(engine.execute_with_session(session, sql).unwrap());
    assert_eq!(all.len(), 1, "expected single row for: {sql}");
    all.pop().unwrap()
}

fn warnings(engine: &StorageQueryEngine, session: &mut SessionContext) -> Vec<(i64, String)> {
    rows(
        engine
            .execute_with_session(session, "SHOW WARNINGS")
            .unwrap(),
    )
    .into_iter()
    .map(|row| {
        let code = match row[1] {
            Value::Int(c) => c,
            Value::UInt(c) => c as i64,
            ref other => panic!("unexpected warning code {other:?}"),
        };
        (code, row[2].to_display_string())
    })
    .collect()
}

// ---------- ROUND / TRUNCATE ----------

#[test]
fn wide_decimal_assignment_and_math_remain_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wide_math(v DECIMAL(60,30))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO wide_math VALUES \
             (123456.12345),\
             (123456789012345678901234567890.123456789012345678901234567890)",
        )
        .unwrap();

    let result = rows(
        engine
            .execute(
                "SELECT v, ROUND(v,2), TRUNCATE(v,-2), FORMAT(v,2) \
                 FROM wide_math ORDER BY v",
            )
            .unwrap(),
    );
    let display = result
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| value.to_display_string())
                .collect()
        })
        .collect::<Vec<Vec<_>>>();
    assert_eq!(
        display,
        vec![
            vec![
                "123456.123450000000000000000000000000",
                "123456.12",
                "123400",
                "123,456.12",
            ],
            vec![
                "123456789012345678901234567890.123456789012345678901234567890",
                "123456789012345678901234567890.12",
                "123456789012345678901234567800",
                "123,456,789,012,345,678,901,234,567,890.12",
            ],
        ]
    );
}

#[test]
fn round_exact_decimal_half_away_from_zero() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT ROUND(2.675, 2), ROUND(-2.675, 2), ROUND(2.5), ROUND(-2.5), ROUND(2.5, 5)",
    );
    // f64 computes 2.675*100 as 267.4999..., previously producing 2.67.
    assert_eq!(row[0], Value::DecimalCompact(268, 2));
    assert_eq!(row[1], Value::DecimalCompact(-268, 2));
    assert_eq!(row[2], Value::DecimalCompact(3, 0));
    assert_eq!(row[3], Value::DecimalCompact(-3, 0));
    // D >= scale preserves value and scale.
    assert_eq!(row[4], Value::DecimalCompact(25, 1));
}

#[test]
fn round_integer_keeps_integer_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT ROUND(123, -1), ROUND(125, -1), ROUND(123), ROUND(-125, -1)",
    );
    assert_eq!(row[0], Value::Int(120));
    assert_eq!(row[1], Value::Int(130));
    assert_eq!(row[2], Value::Int(123));
    assert_eq!(row[3], Value::Int(-130));
    // DOUBLE input retains DOUBLE and uses approximate-number ties-to-even rounding.
    let row = one_row(&engine, "SELECT ROUND(2.5e0)");
    assert_eq!(row[0], Value::Double(2.0));
}

#[test]
fn truncate_exact_and_negative_d() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT TRUNCATE(2.999, 2), TRUNCATE(-2.999, 2), TRUNCATE(123.456, -2), TRUNCATE(999, -1)",
    );
    assert_eq!(row[0], Value::DecimalCompact(299, 2));
    assert_eq!(row[1], Value::DecimalCompact(-299, 2));
    assert_eq!(row[2], Value::DecimalCompact(100, 0));
    assert_eq!(row[3], Value::Int(990));
}

#[test]
fn format_after_extreme_truncate_does_not_emit_infinity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT FORMAT(TRUNCATE('1.7976931348623157E+308', -12), 1, 'fr_BE')",
    );
    assert_eq!(row, vec![Value::String("0".to_string())]);
}

#[test]
fn round_truncate_extreme_second_arg() {
    // MySQL func_math.test lines 217-218: an extreme second argument used to
    // overflow `scale - d` (panic in debug, wrong magnitude in release) and
    // the `as i32` cast wrapped, flipping the sign of the request. Both ROUND
    // and TRUNCATE saturate plain numeric D values without changing sign.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Exact DECIMAL input (1.5). Below i32::MIN and above i32::MAX literals
    // plus the exact i32 bounds — all four must not panic or wrap.
    let row = one_row(
        &engine,
        "SELECT ROUND(1.5, -2147483649), TRUNCATE(1.5, -2147483649), \
                ROUND(1.5, 2147483648),  TRUNCATE(1.5, 2147483648), \
                ROUND(1.5, -2147483648), ROUND(1.5, 2147483647)",
    );
    assert_eq!(row[0], Value::DecimalCompact(0, 0)); // round far left → 0
    assert_eq!(row[1], Value::DecimalCompact(0, 0)); // truncate far left → 0
    assert_eq!(row[2], Value::DecimalCompact(15, 1)); // round far right → 1.5
    assert_eq!(row[3], Value::DecimalCompact(15, 1)); // truncate far right → 1.5
    assert_eq!(row[4], Value::DecimalCompact(0, 0)); // i32::MIN → 0
    assert_eq!(row[5], Value::DecimalCompact(15, 1)); // i32::MAX → 1.5

    // Integer input keeps the integer type.
    let row = one_row(
        &engine,
        "SELECT ROUND(12345, -2147483649), ROUND(12345, 2147483648), \
                TRUNCATE(12345, -2147483649), ROUND(12345, -2147483648)",
    );
    assert_eq!(row[0], Value::Int(0));
    assert_eq!(row[1], Value::Int(12345));
    assert_eq!(row[2], Value::Int(0));
    assert_eq!(row[3], Value::Int(0));

    // Float / DOUBLE input keeps the DOUBLE type.
    let row = one_row(
        &engine,
        "SELECT ROUND(1.5e0, -2147483649), ROUND(1.5e0, 2147483648), \
                TRUNCATE(9.87e0, -2147483648), TRUNCATE(9.87e0, 2147483647)",
    );
    assert_eq!(row[0], Value::Double(0.0));
    assert_eq!(row[1], Value::Double(1.5));
    assert_eq!(row[2], Value::Double(0.0));
    assert_eq!(row[3], Value::Double(9.87));

    // MySQL 8.0 Bug#32802251 regression: the invalid scale short-circuits
    // before a multi-row scalar subquery can raise error 1242.
    engine
        .execute(
            "CREATE TABLE truncate_domain_t (v DECIMAL(14,14));
             INSERT INTO truncate_domain_t VALUES (-0.99999999999999),
                                                   (-0.99999999999999),
                                                   (0.15610000000000)",
        )
        .unwrap();
    assert_eq!(
        one_row(
            &engine,
            "SELECT TRUNCATE((SELECT v FROM truncate_domain_t), -17410)",
        )[0],
        Value::Null
    );
}

#[test]
fn unary_minus_distinguishes_unsigned_runtime_values_from_literals() {
    for mode in ["off", "on", "auto"] {
        with_test_env(&[("MYDB_VECTORIZED", Some(mode))], || {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            engine
                .execute("CREATE TABLE unary_unsigned_t (v BIGINT UNSIGNED)")
                .unwrap();
            engine
                .execute("INSERT INTO unary_unsigned_t VALUES (9223372036854775809)")
                .unwrap();

            let err = engine
                .execute("SELECT -v FROM unary_unsigned_t")
                .unwrap_err();
            assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));

            // The positive token itself is an exact literal. MySQL permits
            // unary minus to promote this shape to DECIMAL.
            let row = one_row(&engine, "SELECT -9223372036854775809");
            assert_eq!(row[0].to_display_string(), "-9223372036854775809");
        });
    }
}

#[test]
fn oversized_decimal_arithmetic_reports_expression_overflow() {
    for mode in ["off", "on", "auto"] {
        with_test_env(&[("MYDB_VECTORIZED", Some(mode))], || {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            let mut session = SessionContext::default();
            engine
                .execute_with_session(
                    &mut session,
                    "SET @a:=999999999999999999999999999999999999999999999999999999999999999999999999999999999",
                )
                .unwrap();

            for sql in [
                "SELECT @a + @a",
                "SELECT @a * @a",
                "SELECT -@a - @a",
                "SELECT @a / 0.5",
            ] {
                let err = engine.execute_with_session(&mut session, sql).unwrap_err();
                assert_eq!(
                    (err.code(), err.sql_state().as_ref()),
                    (1690, "22003"),
                    "{mode}: expected DECIMAL overflow for {sql}, got {err:?}"
                );
            }

            let boundary = "99999999999999999999999999999999999999999999999999999999999999999";
            let row = one_row_s(&engine, &mut session, &format!("SELECT {boundary} + 0"));
            assert_eq!(row[0].to_display_string(), boundary);
            let err = engine
                .execute_with_session(&mut session, &format!("SELECT {boundary} + 1"))
                .unwrap_err();
            assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));
        });
    }
}

#[test]
fn cast_double_to_signed_checks_bigint_bounds_in_text_and_prepare_paths() {
    for mode in ["off", "on", "auto"] {
        with_test_env(&[("MYDB_VECTORIZED", Some(mode))], || {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            let mut session = SessionContext::default();

            for (sql, expected) in [
                ("SELECT CAST(POW(2,63) AS SIGNED)", i64::MAX),
                ("SELECT 1-CAST(POW(2,100) AS SIGNED)", 1 - i64::MAX),
                ("SELECT 1-CAST(POW(2,1000) AS SIGNED)", 1 - i64::MAX),
            ] {
                assert_eq!(
                    one_row_s(&engine, &mut session, sql),
                    vec![Value::Int(expected)],
                    "{mode}: {sql}"
                );
            }

            let row = one_row_s(
                &engine,
                &mut session,
                "SELECT CAST(POW(2,63)-1024 AS SIGNED), CAST(1-POW(2,63) AS SIGNED)",
            );
            assert_eq!(row[0], Value::Int(9_223_372_036_854_774_784));
            assert_eq!(row[1], Value::Int(i64::MIN));

            engine
                .execute_with_session(
                    &mut session,
                    "PREPARE cast_pow FROM 'SELECT CAST(POW(2,63) AS SIGNED)'",
                )
                .unwrap();
            assert_eq!(
                one_row_s(&engine, &mut session, "EXECUTE cast_pow"),
                vec![Value::Int(i64::MAX)]
            );
        });
    }
}

#[test]
fn div_rejects_geometry_operands_before_evaluating_invalid_wkb() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE div_geo_ceil SELECT CEIL(ST_LINESTRINGFROMWKB(1) DIV NULL)",
        "CREATE TABLE div_geo_floor SELECT FLOOR(ST_LINESTRINGFROMWKB(1) DIV NULL)",
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(
            (err.code(), err.sql_state().as_ref()),
            (1210, "HY000"),
            "{sql}: {err:?}"
        );
        assert_eq!(err.message(), "Incorrect arguments to DIV");
    }
}

// ---------- CAST ----------

#[test]
fn cast_decimal_rounds_and_clamps_with_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row_s(&engine, &mut session, "SELECT CAST(2.675 AS DECIMAL(4,2))");
    assert_eq!(row[0], Value::DecimalCompact(268, 2));
    assert!(warnings(&engine, &mut session).is_empty());

    // Precision overflow saturates to 99.9 with warning 1264.
    let row = one_row_s(&engine, &mut session, "SELECT CAST(999.99 AS DECIMAL(3,1))");
    assert_eq!(row[0], Value::DecimalCompact(999, 1));
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 1);
    assert_eq!(warns[0].0, 1264);

    // Nonnumeric strings produce 0.00 with warning 1292.
    let row = one_row_s(&engine, &mut session, "SELECT CAST('abc' AS DECIMAL(5,2))");
    assert_eq!(row[0], Value::DecimalCompact(0, 2));
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 1);
    assert_eq!(warns[0].0, 1292);

    // CAST(1 AS DECIMAL(4,2)) uses the target scale.
    let row = one_row_s(&engine, &mut session, "SELECT CAST(1 AS DECIMAL(4,2))");
    assert_eq!(row[0], Value::DecimalCompact(100, 2));
    assert_eq!(row[0].to_display_string(), "1.00");
}

#[test]
fn decimal_65_storage_cast_comparison_and_arithmetic_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let maximum = "99999999999999999999999999999999999.999999999999999999999999999999";
    engine
        .execute("CREATE TABLE decimal65_t (v DECIMAL(65,30))")
        .unwrap();
    engine
        .execute(&format!("INSERT INTO decimal65_t VALUES ({maximum})"))
        .unwrap();

    let row = one_row(&engine, "SELECT v FROM decimal65_t");
    assert_eq!(row[0].to_display_string(), maximum);

    let row = one_row(
        &engine,
        "SELECT
          CAST(99999999999999999999999999999999999999999999999999999999999999999
               AS DECIMAL(65,0)),
          CAST(0.12 AS DECIMAL(2,2)),
          CAST(9999999999999999999999999999999999999999 AS DECIMAL(65,0)) + 1,
          CAST(1000000000000000000000000000000000000000 AS DECIMAL(65,0)) >
            CAST(999999999999999999999999999999999999999 AS DECIMAL(65,0))",
    );
    assert_eq!(
        row[0].to_display_string(),
        "99999999999999999999999999999999999999999999999999999999999999999"
    );
    assert_eq!(row[1].to_display_string(), "0.12");
    assert_eq!(
        row[2].to_display_string(),
        "10000000000000000000000000000000000000000"
    );
    assert_eq!(row[3], Value::Int(1));
}

#[test]
fn decimal_65_btree_keys_are_binary_exact_and_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE decimal65_idx (
                 id INT PRIMARY KEY,
                 v DECIMAL(65,0),
                 UNIQUE KEY uk_v(v)
             )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO decimal65_idx VALUES
             (1,-99999999999999999999999999999999999999999999999999999999999999999),
             (2,-1),(3,0),(4,1),
             (5,99999999999999999999999999999999999999999999999999999999999999998),
             (6,99999999999999999999999999999999999999999999999999999999999999999)",
        )
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute("SELECT id FROM decimal65_idx ORDER BY v")
                .unwrap()
        ),
        (1..=6).map(|id| vec![Value::Int(id)]).collect::<Vec<_>>()
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT id FROM decimal65_idx
             WHERE v=99999999999999999999999999999999999999999999999999999999999999999",
        ),
        vec![Value::Int(6)]
    );
    let duplicate = engine
        .execute(
            "INSERT INTO decimal65_idx VALUES
             (7,99999999999999999999999999999999999999999999999999999999999999999)",
        )
        .unwrap_err();
    assert_eq!(duplicate.code(), 1062);
}

#[test]
fn oversized_decimal_literals_follow_mysql_expression_normalization() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Scale is reduced to 30 for a temporary/result DECIMAL while retaining
    // the exact leading digits.
    engine
        .execute(
            "CREATE TABLE decimal_scale_t AS
             SELECT 0.123456789012345678901234567890123456 AS v",
        )
        .unwrap();
    assert_eq!(
        one_row(&engine, "SELECT v FROM decimal_scale_t")[0].to_display_string(),
        "0.123456789012345678901234567890"
    );

    // Integer digits take priority over scale: 45 integer digits leave 20
    // fractional digits in DECIMAL(65,20), rounded half away from zero.
    engine
        .execute(
            "CREATE TABLE decimal_precision_t AS
             SELECT
             123451234512345123451234512345123451234512345.678906789067890678906789
             AS v",
        )
        .unwrap();
    assert_eq!(
        one_row(&engine, "SELECT v FROM decimal_precision_t")[0].to_display_string(),
        "123451234512345123451234512345123451234512345.67890678906789067891"
    );

    // An over-wide literal is first normalized to DECIMAL(65, 0). Its
    // expression may retain the single carry digit, matching MySQL's exact
    // arithmetic behavior instead of falling back to DOUBLE.
    assert_eq!(
        one_row(
            &engine,
            "SELECT
             99999999999999999999999999999999999999999999999999999999999999999999999999999999
             + 1",
        )[0]
        .to_display_string(),
        format!("1{}", "0".repeat(65))
    );

    // Assignment remains governed by the destination DECIMAL. In non-strict
    // mode both a huge exact literal and scientific notation saturate with
    // warning 1264 instead of aborting the INSERT.
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = '';
             CREATE TABLE decimal_saturate_t (v DECIMAL(64,0))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO decimal_saturate_t VALUES
             (89000000000000000000000000000000000000000000000000000000000000000000000000000000000000000),
             (1e100)",
        )
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut session, "SELECT v FROM decimal_saturate_t ORDER BY v",)
                .unwrap()
        )
        .into_iter()
        .map(|row| row[0].to_display_string())
        .collect::<Vec<_>>(),
        vec!["9".repeat(64), "9".repeat(64)]
    );
}

#[test]
fn cast_integer_string_prefix_vs_decimal_rounding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // String conversion truncates the prefix to 3; DECIMAL/DOUBLE round to 4.
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT CAST('3.7' AS SIGNED), CAST(3.7 AS SIGNED), CAST(3.7e0 AS SIGNED), CAST('123abc' AS UNSIGNED), CAST('1e3' AS SIGNED)",
    );
    assert_eq!(row[0], Value::Int(3));
    assert_eq!(row[1], Value::Int(4));
    assert_eq!(row[2], Value::Int(4));
    assert_eq!(row[3], Value::UInt(123));
    assert_eq!(row[4], Value::Int(1));
    let warns = warnings(&engine, &mut session);
    // Three truncation warnings for '3.7', '123abc', and '1e3'.
    assert_eq!(
        warns.iter().filter(|(code, _)| *code == 1292).count(),
        3,
        "got {warns:?}"
    );

    // Negative-to-UNSIGNED conversion wraps in two's complement without implementation-specific warnings.
    let row = one_row_s(&engine, &mut session, "SELECT CAST('-12' AS UNSIGNED)");
    assert_eq!(row[0], Value::UInt(18446744073709551604));
    let warns = warnings(&engine, &mut session);
    assert!(warns.is_empty(), "unexpected warnings: {warns:?}");
}

#[test]
fn oversized_integer_cast_clamps_once_with_source_diagnostic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let huge = "9999999999999999999999999999999999999999999";

    let row = one_row_s(
        &engine,
        &mut session,
        &format!("SELECT CONVERT({huge}, UNSIGNED)"),
    );
    assert_eq!(row[0], Value::UInt(u64::MAX));
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 1, "intermediate saturation leaked: {warns:?}");
    assert_eq!(warns[0].0, 1292);
    assert!(warns[0].1.contains("DECIMAL"));
    assert!(warns[0].1.contains(huge));

    let row = one_row_s(
        &engine,
        &mut session,
        &format!("SELECT CONVERT('{huge}', UNSIGNED)"),
    );
    assert_eq!(row[0], Value::UInt(u64::MAX));
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 1, "intermediate saturation leaked: {warns:?}");
    assert_eq!(warns[0].0, 1292);
    assert!(warns[0].1.contains("INTEGER"));
    assert!(warns[0].1.contains(huge));
}

#[test]
fn integer_assignment_uses_exact_numeric_prefix_rounding_and_mode_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = ''; \
             CREATE TABLE integer_assignment_text (
               i INT, iu INT UNSIGNED, b BIGINT, bu BIGINT UNSIGNED
             )",
        )
        .unwrap();

    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO integer_assignment_text VALUES \
             ('1.2.3.4', '1.2.3.4', '1.2.3.4', '1.2.3.4')",
        )
        .unwrap();
    assert_eq!(
        warnings(&engine, &mut session)
            .iter()
            .map(|(code, _)| *code)
            .collect::<Vec<_>>(),
        vec![1265, 1265, 1265, 1265]
    );

    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO integer_assignment_text (b,bu) VALUES \
             ('92233720368547758000e+', '92233720368547758000e+')",
        )
        .unwrap();
    assert_eq!(
        warnings(&engine, &mut session)
            .iter()
            .map(|(code, _)| *code)
            .collect::<Vec<_>>(),
        vec![1264, 1264]
    );

    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO integer_assignment_text VALUES \
             ('0.9223372036854775807', '-1e19', \
              '0.9223372036854775808', '-18446744073709551616')",
        )
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute_with_session(
                    &mut session,
                    "SELECT i,iu,b,bu FROM integer_assignment_text \
                     ORDER BY COALESCE(i,-1), COALESCE(iu,0)",
                )
                .unwrap()
        ),
        vec![
            vec![
                Value::Null,
                Value::Null,
                Value::Int(i64::MAX),
                Value::UInt(u64::MAX),
            ],
            vec![Value::Int(1), Value::UInt(0), Value::Int(1), Value::UInt(0),],
            vec![Value::Int(1), Value::UInt(1), Value::Int(1), Value::UInt(1),],
        ]
    );

    engine
        .execute_with_session(&mut session, "SET SESSION sql_mode = 'TRADITIONAL'")
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO integer_assignment_text (i) VALUES ('1.2.3')",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1366, "HY000"));
}

#[test]
fn cast_hex_bit_literal_as_integer_matches_mysql() {
    // MySQL CAST/CONVERT AS SIGNED/UNSIGNED interprets hex/bit literals as 64-bit big-endian
    // unsigned integers, matching arithmetic coercion, rather than parsing binary bytes as decimal
    // text. The former utf8_lossy-then-parse path returned 0, contradicting MySQL 8.0.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Single-byte, even-length, and multibyte hex literals; odd-length parser support
    // is a separate historical issue outside this regression's scope.
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT CONVERT(0xFF, UNSIGNED), CAST(0x0123 AS UNSIGNED), \
         CONVERT(0x0999999999, UNSIGNED), CAST(0x00 AS UNSIGNED)",
    );
    assert_eq!(row[0], Value::UInt(255));
    assert_eq!(row[1], Value::UInt(0x0123));
    assert_eq!(row[2], Value::UInt(0x0999999999));
    assert_eq!(row[3], Value::UInt(0));

    // Arithmetic coercion already works and serves as the differential oracle for CAST/CONVERT.
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT CAST(0xFF AS UNSIGNED) = 0xFF + 0, \
                CAST(0x0123 AS UNSIGNED) = 0x0123 + 0",
    );
    assert_eq!(row[0], Value::Int(1));
    assert_eq!(row[1], Value::Int(1));

    // SIGNED reinterprets two's-complement bits: all-one hex becomes -1, not saturated i64::MAX.
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT CAST(0xFFFFFFFFFFFFFFFF AS SIGNED), CAST(0x7F AS SIGNED)",
    );
    assert_eq!(row[0], Value::Int(-1));
    assert_eq!(row[1], Value::Int(127));

    // Bit literals use the same branch.
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT CAST(b'11111111' AS UNSIGNED), CAST(b'100000001' AS UNSIGNED)",
    );
    assert_eq!(row[0], Value::UInt(255));
    assert_eq!(row[1], Value::UInt(257));

    // Hex literals longer than eight bytes are binary strings rather than BIGINT literals in MySQL;
    // integer conversion yields 0 and warning 1292 for BINARY conversion.
    let _ = engine
        .execute_with_session(&mut session, "SELECT 1")
        .unwrap(); // Clear warnings.
    let _ = warnings(&engine, &mut session);
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT CAST(0x010203040506070809 AS UNSIGNED)",
    );
    assert_eq!(row[0], Value::UInt(0));
    let warns = warnings(&engine, &mut session);
    assert!(
        warns.iter().any(|(code, _)| *code == 1292),
        "expected 1292, got {warns:?}"
    );

    let err = engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE overflow_hex AS \
             SELECT CAST(0x010203040506070809 AS UNSIGNED)",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1292, "22007"));
}

#[test]
fn hex_bit_literal_assignment_uses_destination_type_without_retyping_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE hex_assignment (
               id INT PRIMARY KEY,
               n BIGINT UNSIGNED,
               raw BINARY(8)
             );
             INSERT INTO hex_assignment VALUES
               (1, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF),
               (2, b'1000000000000000000000000000000000000000000000000000000000000000',
                   0x0102030405060708);
             PREPARE hex_insert FROM
               'INSERT INTO hex_assignment VALUES
                  (3, 0x7FFFFFFFFFFFFFFF, 0x0807060504030201)';
             EXECUTE hex_insert;
             DEALLOCATE PREPARE hex_insert",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT n, HEX(raw) FROM hex_assignment ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::UInt(u64::MAX),
                Value::String("FFFFFFFFFFFFFFFF".into())
            ],
            vec![
                Value::UInt(1u64 << 63),
                Value::String("0102030405060708".into())
            ],
            vec![
                Value::UInt(i64::MAX as u64),
                Value::String("0807060504030201".into())
            ],
        ]
    );
}

// ---------- Implicit string-to-number conversion ----------

#[test]
fn string_to_number_takes_prefix_with_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT '123abc'+0, ''+0, '  12.5x'+1, 'abc'+0",
    );
    assert_eq!(row[0], Value::Double(123.0));
    assert_eq!(row[1], Value::Double(0.0));
    assert_eq!(row[2], Value::Double(13.5));
    assert_eq!(row[3], Value::Double(0.0));
    let warns = warnings(&engine, &mut session);
    // '123abc', '  12.5x', and 'abc' warn; empty string does not.
    assert_eq!(
        warns.iter().filter(|(code, _)| *code == 1292).count(),
        3,
        "got {warns:?}"
    );
}

// ---------- Mathematical domain errors ----------

#[test]
fn logarithm_domain_returns_null_with_warning_3020() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT LN(-1), LOG(0), LOG2(0), LOG10(-5), LOG(1, 5), LOG(-2, 5)",
    );
    assert!(row.iter().all(|v| matches!(v, Value::Null)), "got {row:?}");
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 6, "got {warns:?}");
    assert!(warns
        .iter()
        .all(|(code, msg)| *code == 3020 && msg.contains("Invalid argument for logarithm")));

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE logarithm_dml (v DOUBLE); \
             SET SESSION sql_mode = 'TRADITIONAL'",
        )
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO logarithm_dml VALUES (LN(0))")
        .expect_err("strict DML must reject an invalid logarithm");
    assert_eq!((err.code(), err.sql_state().as_ref()), (3020, "2201E"));
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO logarithm_dml VALUES (LOG(1,0))")
        .expect_err("invalid logarithm base must use the same strict DML policy");
    assert_eq!((err.code(), err.sql_state().as_ref()), (3020, "2201E"));
    engine
        .execute_with_session(&mut session, "INSERT INTO logarithm_dml VALUES (1)")
        .unwrap();
    for sql in [
        "UPDATE logarithm_dml SET v=LOG10(-1)",
        "DELETE FROM logarithm_dml WHERE LOG2(0) IS NULL",
    ] {
        let err = engine
            .execute_with_session(&mut session, sql)
            .expect_err("strict DML must reject an invalid logarithm");
        assert_eq!((err.code(), err.sql_state().as_ref()), (3020, "2201E"));
    }
}

#[test]
fn sqrt_asin_acos_domain_null_without_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let row = one_row_s(&engine, &mut session, "SELECT SQRT(-1), ASIN(2), ACOS(-2)");
    assert!(row.iter().all(|v| matches!(v, Value::Null)), "got {row:?}");
    assert!(warnings(&engine, &mut session).is_empty());
}

#[test]
fn cot_zero_and_double_overflow_error_1690() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "SELECT COT(0)",
        "SELECT POW(1e308, 2)",
        "SELECT EXP(1000)",
        "SELECT DEGREES(1e307)",
        "SELECT 1e308 * 10",
        "SELECT ABS(-9223372036854775808)",
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 1690, "expected 1690 for {sql}, got {err:?}");
    }
}

// ---------- CEIL / FLOOR / MOD / SIGN ----------

#[test]
fn ceil_floor_type_follows_argument() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT CEIL(1.5), FLOOR(-1.5), CEIL(-1.5), FLOOR(1.5), CEIL(7), CEIL(1.5e0)",
    );
    assert_eq!(row[0], Value::Int(2));
    assert_eq!(row[1], Value::Int(-2));
    assert_eq!(row[2], Value::Int(-1));
    assert_eq!(row[3], Value::Int(1));
    assert_eq!(row[4], Value::Int(7));
    assert_eq!(row[5], Value::Double(2.0));
}

#[test]
fn mod_keeps_exact_types() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT MOD(7, 3), MOD(5.7, 2), MOD(-7, 3), 5.7 % 2",
    );
    assert_eq!(row[0], Value::Int(1));
    assert_eq!(row[1], Value::DecimalCompact(17, 1)); // 1.7
    assert_eq!(row[2], Value::Int(-1)); // The sign follows the dividend.
    assert_eq!(row[3], Value::DecimalCompact(17, 1));
}

// ---------- Division by zero ----------

#[test]
fn division_by_zero_null_with_warning_1365_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT 1/0, MOD(5,0), 5 DIV 0, 5 % 0",
    );
    assert!(row.iter().all(|v| matches!(v, Value::Null)), "got {row:?}");
    let warns = warnings(&engine, &mut session);
    assert_eq!(
        warns.iter().filter(|(code, _)| *code == 1365).count(),
        4,
        "got {warns:?}"
    );

    // Explicit sql_mode without ERROR_FOR_DIVISION_BY_ZERO produces silent NULL.
    engine
        .execute_with_session(&mut session, "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'")
        .unwrap();
    let row = one_row_s(&engine, &mut session, "SELECT 1/0");
    assert_eq!(row[0], Value::Null);
    assert!(warnings(&engine, &mut session).is_empty());

    // Adding the bit restores warnings.
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
        )
        .unwrap();
    let row = one_row_s(&engine, &mut session, "SELECT 1/0");
    assert_eq!(row[0], Value::Null);
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 1);
    assert_eq!(warns[0].0, 1365);
}

#[test]
fn explicit_empty_sql_mode_survives_routine_calls_for_decimal_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET sql_mode = 'NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET @@sql_mode = 'TRADITIONAL'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    assert_eq!(session.sql_mode, "");
    assert_ne!(
        session.temporal_sql_mode_bits & crate::engine::api::TEMPORAL_MODE_EXPLICIT,
        0,
        "an explicit empty sql_mode must remain distinguishable from the compiled default"
    );
    engine
        .execute_with_session(
            &mut session,
            "CREATE PROCEDURE decimal_mode_probe()
             BEGIN
               DECLARE v DECIMAL(5) DEFAULT 0;
               SET v = v * 10;
             END",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "CALL decimal_mode_probe()")
        .unwrap();
    engine
        .execute_with_session(&mut session, "DROP PROCEDURE decimal_mode_probe")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE decimal_mode_t (v DECIMAL(64,0))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO decimal_mode_t VALUES
             (89000000000000000000000000000000000000000000000000000000000000000000000000000000000000000),
             (1e100)",
        )
        .unwrap();
    let stored = rows(
        engine
            .execute_with_session(&mut session, "SELECT v FROM decimal_mode_t")
            .unwrap(),
    );
    assert_eq!(stored.len(), 2);
    assert!(stored.iter().all(|row| {
        row[0].to_display_string()
            == "9999999999999999999999999999999999999999999999999999999999999999"
    }));
}

#[test]
fn temporal_expression_and_wide_decimal_bit_aggregate_keep_mysql_type_coercion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE temporal_decimal_t (v DECIMAL(20,6));
             INSERT INTO temporal_decimal_t
             VALUES (CAST('10:11:12' AS DATE) + INTERVAL 14 MICROSECOND)",
        )
        .unwrap();
    assert_eq!(
        one_row_s(&engine, &mut session, "SELECT v FROM temporal_decimal_t")[0].to_display_string(),
        "20101112000000.000014"
    );

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT BIT_XOR(
         999999999999999999999999999999999999999999999999999999999999999999999999999999999
         )",
    );
    assert_eq!(row[0], Value::UInt(i64::MAX as u64));
    assert_eq!(warnings(&engine, &mut session)[0].0, 1292);
}

#[test]
fn conditional_integer_branch_keeps_exact_division_scale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE conditional_decimal_t (v INT);
             INSERT INTO conditional_decimal_t VALUES (4500), (6000)",
        )
        .unwrap();

    let result = rows(
        engine
            .execute_with_session(
                &mut session,
                "SELECT IF(v > 5000, 1 / v, 5000),
                        CASE v > 5000 WHEN TRUE THEN 1 / v ELSE 5000 END
                 FROM conditional_decimal_t ORDER BY v",
            )
            .unwrap(),
    );
    assert_eq!(result[0][0].to_display_string(), "5000.0000");
    assert_eq!(result[1][0].to_display_string(), "0.0002");
    assert_eq!(result[0][1].to_display_string(), "5000.0000");
    assert_eq!(result[1][1].to_display_string(), "0.0002");

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE conditional_year_t (y YEAR);
             INSERT INTO conditional_year_t VALUES (2001)",
        )
        .unwrap();
    let result = rows(
        engine
            .execute_with_session(
                &mut session,
                "SELECT AVG(NULLIF(YEAR('2001-01-01'), 10)),
                        AVG(NULLIF(y, 10))
                 FROM conditional_year_t",
            )
            .unwrap(),
    );
    assert_eq!(result[0][0].to_display_string(), "2001");
    assert_eq!(result[0][1].to_display_string(), "2001");
}

#[test]
fn strict_dml_division_by_zero_errors_but_select_still_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE div_dml_t (v DECIMAL(4,2));
             INSERT INTO div_dml_t VALUES (1);
             SET SESSION sql_mode = 'TRADITIONAL'",
        )
        .unwrap();

    let err = engine
        .execute_with_session(&mut session, "UPDATE div_dml_t SET v = v / 0")
        .unwrap_err();
    assert_eq!(err.code(), 1365);
    assert_eq!(err.sql_state().as_ref(), "22012");
    assert_eq!(
        one_row_s(&engine, &mut session, "SELECT v FROM div_dml_t")[0].to_display_string(),
        "1.00"
    );

    let row = one_row_s(&engine, &mut session, "SELECT 1 / 0");
    assert_eq!(row[0], Value::Null);
    assert_eq!(warnings(&engine, &mut session)[0].0, 1365);
}

#[test]
fn unsigned_decimal_assignment_obeys_strict_and_nonstrict_modes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE unsigned_decimal_t (v DECIMAL(20,2) UNSIGNED);
             SET SESSION sql_mode = 'TRADITIONAL'",
        )
        .unwrap();

    let err = engine
        .execute_with_session(&mut session, "INSERT INTO unsigned_decimal_t VALUES (-1)")
        .unwrap_err();
    assert_eq!(err.code(), 1264);
    assert_eq!(err.sql_state().as_ref(), "22003");

    engine
        .execute_with_session(&mut session, "SET SESSION sql_mode = ''")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO unsigned_decimal_t VALUES (-1)")
        .unwrap();
    assert_eq!(warnings(&engine, &mut session)[0].0, 1264);
    assert_eq!(
        one_row_s(&engine, &mut session, "SELECT v FROM unsigned_decimal_t")[0].to_display_string(),
        "0.00"
    );
}

// ---------- UNSIGNED subtraction ----------

#[test]
fn unsigned_subtraction_error_and_mode_override() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = engine
        .execute_with_session(&mut session, "SELECT CAST(1 AS UNSIGNED) - 10")
        .unwrap_err();
    assert_eq!(err.code(), 1690);

    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = 'NO_UNSIGNED_SUBTRACTION'",
        )
        .unwrap();
    let row = one_row_s(&engine, &mut session, "SELECT CAST(1 AS UNSIGNED) - 10");
    assert_eq!(row[0], Value::Int(-9));
    let err = engine
        .execute_with_session(
            &mut session,
            "SELECT 18446744073709551615 - CAST(1 AS UNSIGNED)",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));

    engine
        .execute_with_session(&mut session, "SET SESSION sql_mode = ''")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE years (a YEAR, b YEAR)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO years VALUES (2020, 2022)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "SELECT a-b FROM years")
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE unsigned_mix (a INT UNSIGNED, b DECIMAL(10,2) UNSIGNED)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO unsigned_mix VALUES (0, 1)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "SELECT a-b FROM unsigned_mix")
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE unsigned_dec (a DECIMAL(10,2) UNSIGNED, b DECIMAL(10,2) UNSIGNED)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO unsigned_dec VALUES (0, 1)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "SELECT a-b FROM unsigned_dec")
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE year_dec (a YEAR, b DECIMAL(10,2) UNSIGNED)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO year_dec VALUES (1901, 1902), (1902, 1902)",
        )
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "SELECT a-b FROM year_dec ORDER BY (a-b)")
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE year_fn_sources (d DATE, dt DATETIME); \
             INSERT INTO year_fn_sources VALUES ('1999-12-31', '2000-01-01 00:00:00')",
        )
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "SELECT YEAR(d) - YEAR(dt) FROM year_fn_sources",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));

    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = 'NO_UNSIGNED_SUBTRACTION'",
        )
        .unwrap();
    let row = one_row_s(&engine, &mut session, "SELECT a-b FROM years");
    assert_eq!(row[0], Value::Int(-2));
    assert_eq!(
        one_row_s(
            &engine,
            &mut session,
            "SELECT YEAR(d) - YEAR(dt) FROM year_fn_sources",
        )[0],
        Value::Int(-1)
    );
    assert_eq!(
        one_row_s(&engine, &mut session, "SELECT a-b FROM unsigned_dec")[0].to_display_string(),
        "-1.00"
    );
}

#[test]
fn integer_division_rejects_signed_boundary_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "SELECT -9223372036854775808 DIV -1",
        "SELECT 18446744073709551615 DIV -1",
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));
    }
}

#[test]
fn variance_uses_stable_online_central_moment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (s SMALLINT, d DOUBLE, e DECIMAL)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,1,1),(2,2,2),(3,3,3),(6,6,6),(7,7,7)")
        .unwrap();

    let row = one_row(
        &engine,
        "SELECT VAR_SAMP(s), VAR_POP(s), VAR_SAMP(d), VAR_POP(e) FROM t",
    );
    assert_eq!(
        row,
        vec![
            Value::Double(6.7),
            Value::Double(5.36),
            Value::Double(6.7),
            Value::Double(5.36),
        ]
    );
}

// ---------- div_precision_increment ----------

#[test]
fn div_precision_increment_controls_scale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row_s(&engine, &mut session, "SELECT 10/3");
    assert_eq!(row[0], Value::DecimalCompact(33333, 4));
    assert_eq!(row[0].to_display_string(), "3.3333");

    engine
        .execute_with_session(&mut session, "SET SESSION div_precision_increment = 8")
        .unwrap();
    let row = one_row_s(&engine, &mut session, "SELECT 10/3");
    assert_eq!(row[0], Value::DecimalCompact(333333333, 8));
    assert_eq!(row[0].to_display_string(), "3.33333333");
}

// ---------- Aggregate return types ----------

#[test]
fn avg_exact_inputs_return_decimal_scale_plus_four() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, i INT, d DECIMAL(10,2), f DOUBLE)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, 1.25, 1.5), (2, 2, 2.50, 2.5)")
        .unwrap();

    let row = one_row(&engine, "SELECT AVG(i), AVG(d), AVG(f), SUM(d) FROM t");
    assert_eq!(row[0], Value::DecimalCompact(15000, 4)); // 1.5000
    assert_eq!(row[0].to_display_string(), "1.5000");
    assert_eq!(row[1], Value::DecimalCompact(1875000, 6)); // 1.875000
    assert_eq!(row[1].to_display_string(), "1.875000");
    assert_eq!(row[2], Value::Double(2.0)); // Floating input retains DOUBLE.
    assert_eq!(row[3], Value::DecimalCompact(375, 2)); // SUM preserves input scale.
}

// ---------- Write-time rounding ----------

#[test]
fn insert_decimal_rounds_excess_fraction_half_away() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dd (id INT PRIMARY KEY, c DECIMAL(4,2))")
        .unwrap();
    engine
        .execute("INSERT INTO dd VALUES (1, 1.005), (2, 2.994), (3, -1.005), (4, 1.004)")
        .unwrap();
    let rows = rows(engine.execute("SELECT c FROM dd ORDER BY id").unwrap());
    assert_eq!(
        rows.iter()
            .map(|r| r[0].to_display_string())
            .collect::<Vec<_>>(),
        vec!["1.01", "2.99", "-1.01", "1.00"]
    );
}

// ---------- FLOAT(M,D)/DOUBLE(M,D) rounding and range ----------

#[test]
fn insert_float_md_rounds_to_scale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE fm (id INT PRIMARY KEY, f FLOAT(6,2), d DOUBLE(8,3))")
        .unwrap();
    engine
        .execute("INSERT INTO fm VALUES (1, 12.345, 1.23456), (2, -9.994, 7.6545)")
        .unwrap();
    let rows = rows(engine.execute("SELECT f, d FROM fm ORDER BY id").unwrap());
    // FLOAT(6,2): 12.345 → 12.35 (half away), -9.994 → -9.99;
    // DOUBLE(8,3): 1.23456 → 1.235, 7.6545 → 7.655 (half away).
    assert_eq!(
        rows.iter()
            .map(|r| (r[0].to_display_string(), r[1].to_display_string()))
            .collect::<Vec<_>>(),
        vec![
            ("12.35".to_string(), "1.235".to_string()),
            ("-9.99".to_string(), "7.655".to_string()),
        ]
    );
}

#[test]
fn insert_float_md_out_of_range_errors_1264() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE fr (id INT PRIMARY KEY, f FLOAT(4,2))")
        .unwrap();
    // FLOAT(4,2) max magnitude is 99.99; 100.0 overflows the M-D digit budget.
    let err = engine
        .execute("INSERT INTO fr VALUES (1, 100.0)")
        .unwrap_err();
    assert_eq!(err.code(), 1264, "expected out-of-range 1264, got {err:?}");
    // Rounding that pushes into the next digit also overflows: 99.995 → 100.00.
    let err = engine
        .execute("INSERT INTO fr VALUES (2, 99.999)")
        .unwrap_err();
    assert_eq!(err.code(), 1264, "expected out-of-range 1264, got {err:?}");
    // 99.99 is exactly in range.
    engine.execute("INSERT INTO fr VALUES (3, 99.99)").unwrap();
}

#[test]
fn float_bare_precision_hint_keeps_ieee_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Single-argument FLOAT(p) is a bit-precision hint, not (M,D): no rounding.
    engine
        .execute("CREATE TABLE fp (id INT PRIMARY KEY, d DOUBLE)")
        .unwrap();
    engine
        .execute("INSERT INTO fp VALUES (1, 1.23456789)")
        .unwrap();
    let row = one_row(&engine, "SELECT d FROM fp");
    assert_eq!(row[0].to_display_string(), "1.23456789");
}

// ---------- CAST AS DATETIME(N)/TIME(N) fractional precision ----------

#[test]
fn cast_datetime_time_fsp_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT CAST('2020-01-02 03:04:05.123456' AS DATETIME(3)), \
                CAST('2020-01-02 03:04:05.123456' AS DATETIME), \
                CAST('2020-01-02 03:04:05.987654' AS TIME(2))",
    );
    // P1-8: CAST fsp rounds half-away-from-zero, matching MySQL 8.0.
    // .123456 → fsp 3 truncates .123456 to .123 (below half at position 4).
    // Bare DATETIME rounds fractional .5s into the whole seconds.
    // .987654 → fsp 2 rounds up to .99.
    assert_eq!(row[0].to_display_string(), "2020-01-02 03:04:05.123");
    assert_eq!(row[1].to_display_string(), "2020-01-02 03:04:05");
    assert_eq!(row[2].to_display_string(), "03:04:05.99");
}

// ---------- CONV ----------

#[test]
fn conv_uses_unsigned_64_bit_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT CONV('FFFFFFFFFFFFFFFF',16,10), CONV(-1,10,16), CONV('FFFFFFFFFFFFFFFFF',16,10)",
    );
    assert_eq!(row[0].to_display_string(), "18446744073709551615");
    assert_eq!(row[1].to_display_string(), "FFFFFFFFFFFFFFFF");
    // Seventeen F digits saturate u64 and warn 1292.
    assert_eq!(row[2].to_display_string(), "18446744073709551615");
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 1);
    assert_eq!(warns[0].0, 1292);
}

#[test]
fn hex_and_conv_saturate_numeric_literals_in_the_selected_integer_domain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT HEX(29223372036854775809), HEX(-29223372036854775809), \
                CONV(29223372036854775809,-10,16), \
                CONV(29223372036854775809,10,16), \
                CONV(-29223372036854775809,-10,16), \
                CONV(-29223372036854775809,10,16)",
    );
    assert_eq!(
        row.iter().map(Value::to_display_string).collect::<Vec<_>>(),
        [
            "7FFFFFFFFFFFFFFF",
            "8000000000000000",
            "7FFFFFFFFFFFFFFF",
            "FFFFFFFFFFFFFFFF",
            "8000000000000000",
            "0",
        ]
    );
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 6);
    assert!(warns.iter().all(|warning| warning.0 == 1292));
}

// ---------- Comparison precision ----------

#[test]
fn comparisons_are_exact_past_2_pow_53() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT 9223372036854775806 = 9223372036854775807, \
                18446744073709551615 > 9223372036854775807, \
                2.5 = 2.50, \
                9223372036854775807 = 9223372036854775806.5",
    );
    assert_eq!(row[0], Value::Int(0)); // These values collapse to equality in f64.
    assert_eq!(row[1], Value::Int(1));
    assert_eq!(row[2], Value::Int(1)); // Align differing fixed-point scales.
    assert_eq!(row[3], Value::Int(0)); // Exact Int versus DecimalCompact comparison.
}

// ---------- DECIMAL multiplication scale clamping ----------

#[test]
fn decimal_multiply_clamps_scale_to_30_with_rounding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT 0.1234567890123456789 * 0.1234567890123456789",
    );
    // An exact scale-38 product is clamped to 30 and rounded at digit 30, matching MySQL.
    assert_eq!(
        row[0].to_display_string(),
        "0.015241578753238836750190519988"
    );
}

// Issue #46: a product significand exceeding i64 must not fall back to f64.

#[test]
fn decimal_multiply_mantissa_overflow_stays_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // 3037000500 * 3037000500 = 9223372037000250000 > i64::MAX,
    // Result scale is 3+3=6 and the value must remain exact.
    let row = one_row(&engine, "SELECT 3037000.500 * 3037000.500");
    assert_eq!(row[0].to_display_string(), "9223372037000.250000");

    // Below the i64::MAX significand boundary, the result was already exact.
    let row = one_row(&engine, "SELECT 3037000.499 * 3037000.499");
    assert_eq!(row[0].to_display_string(), "9223372030926.249001");

    // DECIMAL(10,3) column multiplication always produces scale 6.
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE d (a DECIMAL(10,3), b DECIMAL(10,3))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO d VALUES (1234567.890, 9876543.210)",
        )
        .unwrap();
    let row = one_row(&engine, "SELECT a * b FROM d");
    assert_eq!(row[0].to_display_string(), "12193263111263.526900");
}

// Issue #59: BOOLEAN/BOOL aliases TINYINT(1), without clamping to 0/1.

#[test]
fn boolean_column_is_signed_tinyint_alias() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t(b BOOLEAN)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (5),(127),(-1),(0)")
        .unwrap();
    let vals: Vec<i64> = rows(
        engine
            .execute_with_session(&mut session, "SELECT b FROM t ORDER BY b")
            .unwrap(),
    )
    .into_iter()
    .map(|r| match r[0] {
        Value::Int(v) => v,
        ref other => panic!("expected Int, got {other:?}"),
    })
    .collect();
    // Retain the complete TINYINT range rather than clamping to 0/1.
    assert_eq!(vals, vec![-1, 0, 5, 127]);

    // Out-of-range values still fail under default strict sql_mode.
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (200)")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1264,
        "boolean overflow must reject like tinyint: {err:?}"
    );

    // The BOOL alias behaves identically.
    engine
        .execute_with_session(&mut session, "CREATE TABLE t2(b BOOL)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t2 VALUES (5)")
        .unwrap();
    let row = one_row(&engine, "SELECT b FROM t2");
    assert_eq!(row[0], Value::Int(5));
}

// ---------- DIV divides before truncation ----------

#[test]
fn integer_div_divides_before_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(&engine, "SELECT 0.9 DIV 0.3, 7 DIV 2, -7 DIV 2");
    assert_eq!(row[0], Value::Int(3));
    assert_eq!(row[1], Value::Int(3));
    assert_eq!(row[2], Value::Int(-3)); // Truncate toward zero.
}

// G1: non-strict INSERT overflow saturates with warnings.
//
// MySQL defaults include STRICT_TRANS_TABLES, making overflow an error;
// explicit empty sql_mode disables strictness and saturates numerics with warning 1264,
// or truncates strings with 1265. mydb treats empty sql_mode as strict, consistent with
// temporal handling, so defaults and explicit STRICT retain hard errors.

#[test]
fn nonstrict_int_overflow_saturates_with_warning_1264() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, a TINYINT, b INT UNSIGNED)",
        )
        .unwrap();
    // Default empty sql_mode is strict and overflow returns 1264.
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 999, 1)")
        .unwrap_err();
    assert_eq!(err.code(), 1264, "default sql_mode must reject: {err:?}");

    // Disable strictness to get saturation with warnings.
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2, 999, -5)")
        .unwrap();
    let warns = warnings(&engine, &mut session);
    assert!(
        warns.iter().filter(|(c, _)| *c == 1264).count() >= 2,
        "expected two 1264 warnings (TINYINT + UNSIGNED), got {warns:?}"
    );
    let row = one_row_s(&engine, &mut session, "SELECT a, b FROM t WHERE id = 2");
    assert_eq!(row[0], Value::Int(127), "TINYINT saturates to 127");
    assert_eq!(row[1], Value::UInt(0), "negative into UNSIGNED clamps to 0");
}

#[test]
fn nonstrict_decimal_and_float_saturate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE d (id INT PRIMARY KEY, dec DECIMAL(4,2), f FLOAT(4,2))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO d VALUES (1, 123.456, 100.0)")
        .unwrap();
    let warns = warnings(&engine, &mut session);
    assert!(
        warns.iter().filter(|(c, _)| *c == 1264).count() >= 2,
        "expected 1264 for decimal + float, got {warns:?}"
    );
    let row = one_row_s(&engine, &mut session, "SELECT dec, f FROM d WHERE id = 1");
    assert_eq!(
        row[0].to_display_string(),
        "99.99",
        "DECIMAL(4,2) saturates"
    );
    assert_eq!(row[1].to_display_string(), "99.99", "FLOAT(4,2) saturates");
}

// ---------- BIT(M) ----------

#[test]
fn bit_column_stores_integer_bit_and_hex_literals() {
    // Regression for issue #85: BIT(M) accepts integers, bit-literals
    // (b'…'), and hex (0x…), stores an M-bit value, and reads back numeric
    // (b + 0). Previously integers stored 0 (silent corruption) and
    // literals errored 1366.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, b BIT(8))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 170)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (2, b'10101010')")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (3, 0xAA)").unwrap();
    let all = rows(
        engine
            .execute("SELECT id, b + 0 FROM t ORDER BY id")
            .unwrap(),
    );
    for r in &all {
        assert_eq!(
            r[1],
            Value::UInt(170),
            "row {:?} should read back 170",
            r[0]
        );
    }

    // BIT(1) round-trips 0 and 1.
    engine.execute("CREATE TABLE t1 (b BIT(1))").unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (b'1'), (b'0')")
        .unwrap();
    let vals = rows(
        engine
            .execute("SELECT b + 0 FROM t1 ORDER BY b + 0")
            .unwrap(),
    );
    assert_eq!(vals[0][0], Value::UInt(0));
    assert_eq!(vals[1][0], Value::UInt(1));

    // Multi-byte BIT(12) is big-endian and exact.
    engine.execute("CREATE TABLE t2 (b BIT(12))").unwrap();
    engine
        .execute("INSERT INTO t2 VALUES (256), (4095)")
        .unwrap();
    let vals = rows(
        engine
            .execute("SELECT b + 0 FROM t2 ORDER BY b + 0")
            .unwrap(),
    );
    assert_eq!(vals[0][0], Value::UInt(256));
    assert_eq!(vals[1][0], Value::UInt(4095));

    // Out-of-range (256 into BIT(8)) errors 1264 under the default mode.
    let err = engine
        .execute("INSERT INTO t VALUES (4, 256)")
        .expect_err("256 exceeds BIT(8)");
    assert_eq!(err.code(), 1406);
}

#[test]
fn bit_column_accepts_bit_literal_default() {
    // Regression for issue #85: `BIT(M) DEFAULT b'…'` no longer errors 1067.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, b BIT(4) DEFAULT b'1010')")
        .unwrap();
    engine.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let row = one_row(&engine, "SELECT b + 0 FROM t");
    assert_eq!(row[0], Value::UInt(10));
}

#[test]
fn bit_ignore_distinguishes_binary_width_from_numeric_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (b BIT(2))")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT IGNORE INTO t VALUES (b'00'),(b'01'),(b'10'),(b'100')",
        )
        .unwrap();
    assert_eq!(
        warnings(&engine, &mut session)
            .into_iter()
            .map(|warning| warning.0)
            .collect::<Vec<_>>(),
        vec![1406],
        "an over-width bit string retains ER_DATA_TOO_LONG under IGNORE"
    );
    assert_eq!(
        one_row_s(&engine, &mut session, "SELECT HEX(MAX(b)) FROM t"),
        vec![Value::String("3".into())],
        "the over-width bit string saturates to the all-ones BIT value"
    );

    engine
        .execute_with_session(&mut session, "CREATE TABLE td (v DOUBLE NOT NULL)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO td VALUES (-20000000000000000000000000000.0001),\
             (20000000000000000000000000000.0001)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE tb (b BIT(3))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT IGNORE INTO tb SELECT v FROM td")
        .unwrap();
    assert_eq!(
        warnings(&engine, &mut session)
            .into_iter()
            .map(|warning| warning.0)
            .collect::<Vec<_>>(),
        vec![1406, 1406]
    );
    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut session, "SELECT HEX(b) FROM tb")
                .unwrap()
        ),
        vec![
            vec![Value::String("7".into())],
            vec![Value::String("7".into())]
        ]
    );

    engine
        .execute_with_session(&mut session, "CREATE TABLE tc (a BIT(3), b BIT(12))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO tc VALUES (7, 4094), (1, 511)")
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute_with_session(
                    &mut session,
                    "SELECT HEX(CONCAT(a)), HEX(CONCAT(b)) FROM tc ORDER BY a DESC",
                )
                .unwrap()
        ),
        vec![
            vec![Value::String("07".into()), Value::String("0FFE".into())],
            vec![Value::String("01".into()), Value::String("01FF".into())],
        ],
        "BIT contributes fixed-width binary octets to CONCAT"
    );
    assert_eq!(
        one_row_s(
            &engine,
            &mut session,
            "SELECT HEX(GROUP_CONCAT(DISTINCT a ORDER BY a)), \
             HEX(GROUP_CONCAT(DISTINCT b ORDER BY b)) FROM tc",
        ),
        vec![
            Value::String("012C07".into()),
            Value::String("01FF2C0FFE".into()),
        ],
        "explicit GROUP_CONCAT ordering is deterministic and retains BIT octets"
    );
}

// ---------- BIT(M) audit 2026-07 regressions ----------

#[test]
fn bit_width_over_64_is_rejected_1439() {
    // B1: BIT(M > 64) must error 1439 at CREATE (MySQL: "Display width out
    // of range … max = 64"). Previously it silently created the table and
    // then panicked on the first INSERT (row-codec slice out of bounds).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("CREATE TABLE t (b BIT(65))")
        .expect_err("BIT(65) must be rejected");
    assert_eq!(err.code(), 1439);
    // ALTER TABLE is guarded too.
    engine.execute("CREATE TABLE t2 (a INT)").unwrap();
    let err = engine
        .execute("ALTER TABLE t2 ADD COLUMN b BIT(100)")
        .expect_err("BIT(100) must be rejected");
    assert_eq!(err.code(), 1439);
    // The boundary value BIT(64) is allowed and round-trips its full range.
    engine.execute("CREATE TABLE t3 (b BIT(64))").unwrap();
    engine
        .execute("INSERT INTO t3 VALUES (18446744073709551615)")
        .unwrap();
    let row = one_row(&engine, "SELECT b + 0 FROM t3");
    assert_eq!(row[0], Value::UInt(u64::MAX));
}

#[test]
fn bit_zero_width_is_rejected() {
    // MySQL 8 rejects BIT(0) with ER_INVALID_FIELD_SIZE; a bare BIT is the
    // declaration that defaults to one bit.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("CREATE TABLE invalid_bit (b BIT(0))")
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (3013, "HY000"));
    engine.execute("CREATE TABLE t (b BIT)").unwrap();
    engine.execute("INSERT INTO t VALUES (1), (0)").unwrap();
    let vals = rows(
        engine
            .execute("SELECT b + 0 FROM t ORDER BY b + 0")
            .unwrap(),
    );
    assert_eq!(vals[0][0], Value::UInt(0));
    assert_eq!(vals[1][0], Value::UInt(1));
}

#[test]
fn bit_column_compared_with_bit_and_hex_literals() {
    // B3: `WHERE bitcol = b'…'` / `= 0x..` / `IN (…)` must match the stored
    // value. Previously comparison did not coerce the bit/hex literal, so it
    // compared UInt(5) against Bytes([0x05]) via a lenient string→f64 path
    // ("\x05" → 0.0) and silently returned no rows.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, b BIT(8))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, b'101'), (2, b'110'), (3, 200)")
        .unwrap();

    let hit = rows(engine.execute("SELECT id FROM t WHERE b = b'101'").unwrap());
    assert_eq!(hit, vec![vec![Value::Int(1)]]);

    let hit = rows(
        engine
            .execute("SELECT id FROM t WHERE b = 0xC8 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(hit, vec![vec![Value::Int(3)]]);

    let hit = rows(
        engine
            .execute("SELECT id FROM t WHERE b IN (b'101', b'110') ORDER BY id")
            .unwrap(),
    );
    assert_eq!(hit, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    // Boolean projection of the comparison.
    let val = one_row(&engine, "SELECT b = b'101' FROM t WHERE id = 1");
    assert_eq!(val[0], Value::Int(1));

    // `charcol = x'41'` must still compare as the byte string "A", not 65.
    engine.execute("CREATE TABLE s (c CHAR(1))").unwrap();
    engine.execute("INSERT INTO s VALUES ('A')").unwrap();
    let hit = rows(engine.execute("SELECT c FROM s WHERE c = x'41'").unwrap());
    assert_eq!(hit.len(), 1, "x'41' should match the char 'A'");
}

#[test]
fn bit64_high_bit_values_index_and_order_unsigned() {
    // B4: a BIT(64) index key must be treated as unsigned. A signed key
    // flipped values >= 2^63 negative and mis-ordered range scans (and did
    // not even match the stored unsigned key). Uses a secondary index so
    // the range scan goes through the b-tree.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, b BIT(64), KEY(b))")
        .unwrap();
    // 2^63 (high bit set) and a couple of small values.
    engine
        .execute("INSERT INTO t VALUES (1, 9223372036854775808), (2, 1), (3, 5)")
        .unwrap();

    // Equality via the index finds the high-bit row.
    let hit = rows(
        engine
            .execute("SELECT id FROM t WHERE b = 9223372036854775808")
            .unwrap(),
    );
    assert_eq!(hit, vec![vec![Value::Int(1)]]);

    // Range scan orders unsigned: 1 < 5 < 2^63.
    let ordered = rows(
        engine
            .execute("SELECT id FROM t WHERE b > 0 ORDER BY b")
            .unwrap(),
    );
    assert_eq!(
        ordered,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(1)]
        ]
    );

    // The high-bit value reads back as the correct unsigned magnitude.
    let val = one_row(&engine, "SELECT b + 0 FROM t WHERE id = 1");
    assert_eq!(val[0], Value::UInt(9223372036854775808));
}

#[test]
fn bit_default_shows_as_bit_literal() {
    // B6: SHOW CREATE TABLE and information_schema.COLUMNS.COLUMN_DEFAULT
    // must echo a BIT default as `b'…'`, not raw (control-character) bytes,
    // so dump/restore round-trips.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, b BIT(4) DEFAULT b'1010')")
        .unwrap();

    let show = rows(engine.execute("SHOW CREATE TABLE t").unwrap());
    let create_sql = match &show[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("expected create sql string, got {other:?}"),
    };
    assert!(
        create_sql.contains("DEFAULT b'1010'"),
        "SHOW CREATE should echo `DEFAULT b'1010'`, got:\n{create_sql}"
    );

    let def = one_row(
        &engine,
        "SELECT COLUMN_DEFAULT FROM information_schema.columns \
         WHERE table_name = 't' AND column_name = 'b'",
    );
    assert_eq!(def[0], Value::String("b'1010'".to_string()));
}

#[test]
fn bit_numeric_precision_reports_width() {
    // B9: information_schema.COLUMNS.NUMERIC_PRECISION = M for BIT(M).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (b BIT(9))").unwrap();
    let row = one_row(
        &engine,
        "SELECT NUMERIC_PRECISION, DATA_TYPE, COLUMN_TYPE \
         FROM information_schema.columns \
         WHERE table_name = 't' AND column_name = 'b'",
    );
    assert_eq!(row[0], Value::Int(9));
    assert_eq!(row[1], Value::String("bit".to_string()));
    assert_eq!(row[2], Value::String("bit(9)".to_string()));
}

// ---------- Unary negation implicitly converts strings to numbers ----------

#[test]
fn unary_minus_coerces_string_operand() {
    // MySQL unary negation converts a numeric string prefix to a number;
    // nonnumeric text yields zero with warning 1292, and the result is DOUBLE.
    // Previously mydb returned 1366 cannot negate 3, found in func_concat.test.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Negating '3' gives DOUBLE -3, displayed as -3, without warning.
    let row = one_row_s(&engine, &mut session, "SELECT -'3'");
    assert_eq!(row[0], Value::Double(-3.0));
    assert_eq!(row[0].to_display_string(), "-3");
    assert!(warnings(&engine, &mut session).is_empty());

    // Negating '3.5' gives DOUBLE -3.5 without warning.
    let row = one_row_s(&engine, &mut session, "SELECT -'3.5'");
    assert_eq!(row[0], Value::Double(-3.5));
    assert_eq!(row[0].to_display_string(), "-3.5");
    assert!(warnings(&engine, &mut session).is_empty());

    // Negating 'abc' gives 0, not negative zero, with truncation warning 1292.
    let row = one_row_s(&engine, &mut session, "SELECT -'abc'");
    assert_eq!(row[0], Value::Double(0.0));
    assert_eq!(row[0].to_display_string(), "0");
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 1);
    assert_eq!(warns[0].0, 1292);

    // Negating prefix '12abc' gives -12 with warning 1292.
    let row = one_row_s(&engine, &mut session, "SELECT -'12abc'");
    assert_eq!(row[0], Value::Double(-12.0));
    let warns = warnings(&engine, &mut session);
    assert_eq!(warns.len(), 1);
    assert_eq!(warns[0].0, 1292);
}

#[test]
fn unary_minus_string_in_concat() {
    // func_concat.test: CONCAT converts the negated numeric result back to text.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // CONCAT('a', -'3') → 'a-3'。
    let row = one_row(&engine, "SELECT CONCAT('a', -'3')");
    assert_eq!(row[0], Value::String("a-3".to_string()));

    // CONCAT('a', -CONCAT('3',4)) negates '34' and returns 'a-34'.
    let row = one_row(&engine, "SELECT CONCAT('a', -CONCAT('3', 4))");
    assert_eq!(row[0], Value::String("a-34".to_string()));
}

#[test]
fn floating_assignment_obeys_sql_mode_signedness_and_ieee_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (f FLOAT, fu FLOAT UNSIGNED, d DOUBLE)",
        )
        .unwrap();

    // Explicit non-strict mode consumes the longest numeric prefix and stores
    // zero when there is none, while retaining a diagnostic warning.
    engine
        .execute_with_session(&mut session, "SET sql_mode = ''")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t(f) VALUES ('1.'), ('2.0.'), ('.')",
        )
        .unwrap();
    let values = rows(
        engine
            .execute_with_session(&mut session, "SELECT f FROM t ORDER BY f")
            .unwrap(),
    );
    assert_eq!(
        values,
        vec![
            vec![Value::Float(0.0)],
            vec![Value::Float(1.0)],
            vec![Value::Float(2.0)]
        ]
    );

    engine
        .execute_with_session(&mut session, "SET sql_mode = 'STRICT_TRANS_TABLES'")
        .unwrap();
    for (sql, code, state) in [
        ("INSERT INTO t(fu) VALUES (-1)", 1264, "22003"),
        ("INSERT INTO t(f) VALUES ('1e+52')", 1264, "22003"),
        (
            "INSERT INTO t(d) VALUES (1.79769313486232e+308)",
            1367,
            "22007",
        ),
    ] {
        let err = engine.execute_with_session(&mut session, sql).unwrap_err();
        assert_eq!(
            (err.code(), err.sql_state().as_ref()),
            (code, state),
            "{sql}: {err:?}"
        );
    }
}

#[test]
fn oversized_exact_decimal_literal_multiplication_errors_before_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (d DECIMAL(64))").unwrap();
    let operand =
        "99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999";
    let sql = format!("INSERT INTO t VALUES ({operand} * {operand})");
    let err = engine.execute(&sql).unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1690, "22003"));
    assert!(err
        .message()
        .starts_with("DECIMAL value is out of range in "))
}

#[test]
fn bare_decimal_uses_mysql_default_precision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE bare_decimal_t (v DECIMAL)")
        .unwrap();
    engine
        .execute("INSERT INTO bare_decimal_t VALUES (1234567890)")
        .unwrap();
    assert_eq!(
        one_row(&engine, "SELECT v FROM bare_decimal_t")[0].to_display_string(),
        "1234567890"
    );
    let err = engine
        .execute("INSERT INTO bare_decimal_t VALUES (12345678901)")
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1264, "22003"));
}

#[test]
fn decimal_max_scale_ctas_precision_check_does_not_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE decimal_scale_source (a DECIMAL(30,30));
             INSERT INTO decimal_scale_source VALUES (0.1),(0.2),(0.3);
             CREATE TABLE decimal_scale_result
             SELECT CASE a WHEN 0.1 THEN
               0.0000000000000000000000000000000000000000000000000000000000000000001
             END AS c1
             FROM decimal_scale_source",
        )
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute("SELECT c1 FROM decimal_scale_result ORDER BY c1")
                .unwrap()
        )
        .len(),
        3
    );
}

#[test]
fn modulo_uses_full_bounded_decimal_literal_intermediate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let operand =
        ".123456789123456789123456789123456789123456789123456789123456789123456789123456789";
    let expected =
        "0.012345687012345687012345687012345687012345687012345687012345687012345687000000000";

    for expression in [format!("1 % {operand}"), format!("MOD(1, {operand})")] {
        assert_eq!(
            one_row(&engine, &format!("SELECT {expression}"))[0].to_display_string(),
            expected,
            "{expression}"
        );
    }

    engine
        .execute(&format!(
            "CREATE TABLE modulo_ctas_t AS SELECT 1 % {operand} AS v"
        ))
        .unwrap();
    let description = rows(engine.execute("DESCRIBE modulo_ctas_t").unwrap());
    assert_eq!(description[0][1].to_display_string(), "decimal(65,30)");
    assert_eq!(
        one_row(&engine, "SELECT v FROM modulo_ctas_t")[0].to_display_string(),
        "0.012345687012345687012345687012"
    );
}

#[test]
fn invalid_hex_literal_string_comparison_uses_connection_charset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE hex_compare_t(a DECIMAL(10,4));
             INSERT INTO hex_compare_t VALUES(-1),(1),(100);
             SET @d:=' '",
        )
        .unwrap();
    let err = engine
        .execute_with_session(
            &mut session,
            "WITH RECURSIVE cte1 AS (
               SELECT a AS a1 FROM hex_compare_t
             ), cte2 AS (
               SELECT (0xfa > @d) AS a2 FROM hex_compare_t
             )
             SELECT cte1.a1
             FROM cte1, cte2
             WHERE cte1.a1 >= cte2.a2",
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (3854, "HY000"));
    assert_eq!(
        err.message(),
        "Cannot convert string '\\xFA' from binary to utf8mb4"
    );
}

#[test]
fn non_strict_decimal_scientific_overflow_and_dirty_exponent_are_coerced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET sql_mode = 'NO_ENGINE_SUBSTITUTION'; \
             CREATE TABLE decimal_exponent_t (v DECIMAL(10,2))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO decimal_exponent_t VALUES \
             ('1e4294967295'), ('1e4294967297'), ('1e-4294967296'), ('-1e+1000'), \
             ('123.4e'), ('123.4e+2')",
        )
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut session, "SELECT v FROM decimal_exponent_t ORDER BY v",)
                .unwrap()
        )
        .into_iter()
        .map(|row| row[0].to_display_string())
        .collect::<Vec<_>>(),
        vec![
            "-99999999.99",
            "0.00",
            "123.40",
            "12340.00",
            "99999999.99",
            "99999999.99"
        ]
    );
}

#[test]
fn oversized_hex_integer_cast_warns_in_select_and_fails_ctas() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let expression = "CONVERT(0x9999999999999999999999999999999999999999999, UNSIGNED)";

    assert_eq!(
        one_row_s(&engine, &mut session, &format!("SELECT {expression}"))[0],
        Value::UInt(0)
    );
    assert!(warnings(&engine, &mut session)
        .iter()
        .any(|(code, _)| *code == 1292));

    let err = engine
        .execute_with_session(
            &mut session,
            &format!("CREATE TABLE hex_overflow_ctas SELECT {expression} AS value"),
        )
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1292, "22007"));
    let err = engine
        .execute("SELECT * FROM hex_overflow_ctas")
        .unwrap_err();
    assert_eq!(err.code(), 1146);
}

#[test]
fn oversized_exact_literal_clamps_before_addition_and_keeps_carry_digit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let literal = "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999";

    let row = one_row_s(
        &engine,
        &mut session,
        &format!("SELECT {literal}, {literal} + 1"),
    );
    assert_eq!(
        row[0].to_display_string(),
        "99999999999999999999999999999999999999999999999999999999999999999"
    );
    assert_eq!(
        row[1].to_display_string(),
        "100000000000000000000000000000000000000000000000000000000000000000"
    );
    assert!(
        !warnings(&engine, &mut session)
            .iter()
            .any(|(code, _)| *code == 1292),
        "normalizing an exact numeric literal is not a string conversion"
    );
}

#[test]
fn inet_functions_support_mysql_legacy_forms_and_reject_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT HEX(INET_ATON('127')), HEX(INET_ATON('127.1')), \
                HEX(INET_ATON('127.1.1')), INET_ATON('0.1.255'), \
                INET_NTOA(4294902271)",
    );
    assert_eq!(
        row.iter().map(Value::to_display_string).collect::<Vec<_>>(),
        ["7F", "7F000001", "7F010001", "65791", "255.255.1.255"]
    );
    assert_eq!(
        one_row_s(
            &engine,
            &mut session,
            "SELECT INET_ATON('122.256'), INET_NTOA(1099511627775)"
        ),
        vec![Value::Null, Value::Null]
    );
    assert_eq!(
        warnings(&engine, &mut session)
            .iter()
            .filter(|(code, _)| *code == 1411)
            .count(),
        2
    );
}

#[test]
fn plain_explain_does_not_publish_constant_evaluation_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "EXPLAIN SELECT INET_ATON('122.256'), INET_ATON('122.226.'), INET_ATON('')",
        )
        .unwrap();
    assert!(
        warnings(&engine, &mut session)
            .iter()
            .all(|(code, _)| *code != 1411),
        "plain EXPLAIN must not execute invalid conversion expressions"
    );
}

#[test]
fn ctas_decimal_predicate_uses_field_conversion_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE decimal_predicate_source (a DECIMAL(10,0), KEY(a))")
        .unwrap();
    for expression in ["CONCAT('m')", "COALESCE('m')"] {
        let err = engine
            .execute(&format!(
                "CREATE TEMPORARY TABLE decimal_predicate_result AS \
                 SELECT * FROM decimal_predicate_source WHERE a={expression}"
            ))
            .unwrap_err();
        assert_eq!(err.code(), 1366, "{expression}: {err}");
    }
}

#[test]
fn overwide_decimal_user_variable_preserves_overflow_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = crate::engine::SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET @a:=999999999999999999999999999999999999999999999999999999999999999999999999999999999",
        )
        .unwrap();
    for sql in ["SELECT @a + @a", "SELECT -@a - @a", "SELECT @a * @a"] {
        let err = engine.execute_with_session(&mut session, sql).unwrap_err();
        assert_eq!(err.code(), 1690, "{sql}: {err}");
    }
}

#[test]
fn between_reports_earlier_constant_operand_error_before_collation_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute(
            "DO 1 BETWEEN TO_BASE64(PERIOD_ADD(1207980960, 2383)) \
             AND 0x43c98093",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1210, "{err}");
}

#[test]
fn period_functions_reject_invalid_period_encoding_before_numeric_use() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        one_row(
            &engine,
            "SELECT PERIOD_ADD(202601, 3), PERIOD_DIFF(202605, 202501)"
        ),
        vec![Value::Int(202604), Value::Int(16)]
    );
    for sql in [
        "SELECT PERIOD_ADD(-15803,-22788)",
        "SELECT PERIOD_DIFF(1509498847,-26603)",
        "SELECT PERIOD_DIFF(1167685773,15899)",
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(
            (err.code(), err.sql_state().as_ref()),
            (1210, "HY000"),
            "{sql}: {err:?}"
        );
        assert!(err.message().starts_with("Incorrect arguments to period_"));
    }
}

#[test]
fn set_operation_aggregates_integer_family_before_ctas() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE union_types (flag BOOL, y YEAR)")
        .unwrap();
    engine
        .execute("INSERT INTO union_types VALUES (1, 1999)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE union_result \
             SELECT flag AS value FROM union_types \
             UNION SELECT y FROM union_types",
        )
        .unwrap();
    let values = rows(
        engine
            .execute("SELECT value FROM union_result ORDER BY value")
            .unwrap(),
    );
    assert_eq!(values, vec![vec![Value::Int(1)], vec![Value::Int(1999)]]);
    let describe = rows(engine.execute("DESCRIBE union_result").unwrap());
    assert_eq!(describe[0][1], Value::String("smallint".into()));
}
