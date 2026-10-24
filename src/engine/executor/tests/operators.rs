//! MySQL 8.0 operator compatibility: bitwise operators, logical XOR,
//! &&/||, DIV, NULL propagation, unsigned semantics, and shift boundaries.
//!
//! Cover constant folding through SELECT expr and column evaluation through
//! SELECT expr FROM t using compiled expr_program wherever possible.
//! String truthiness uses numeric prefixes: 'a' gives 0, '1abc' gives 1, '0.0' is false.
//! Dedicated string_*_numeric_truthiness* tests at this file's end cover it.
use super::support::query_one_row as one_row;
use super::*;

fn scalar(engine: &StorageQueryEngine, sql: &str) -> Value {
    let row = one_row(engine, sql);
    assert_eq!(row.len(), 1, "expected single column for: {sql}");
    row.into_iter().next().unwrap()
}

#[test]
fn unary_minus_promotes_unsigned_literal_past_signed_range_to_decimal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        scalar(&engine, "SELECT -18446744073709551615"),
        Value::Decimal128(-18_446_744_073_709_551_615, 0)
    );
}

// ---------------------------------------------------------------------------
// Bitwise operators: result type is BIGINT UNSIGNED (Value::UInt).
// ---------------------------------------------------------------------------

#[test]
fn bitwise_and_or_xor_constants() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT 5 & 3"), Value::UInt(1));
    assert_eq!(scalar(&engine, "SELECT 5 | 2"), Value::UInt(7));
    assert_eq!(scalar(&engine, "SELECT 5 ^ 1"), Value::UInt(4));
    assert_eq!(scalar(&engine, "SELECT 12 & 10"), Value::UInt(8));
    assert_eq!(scalar(&engine, "SELECT 12 | 10"), Value::UInt(14));
    assert_eq!(scalar(&engine, "SELECT 12 ^ 10"), Value::UInt(6));
}

#[test]
fn bitwise_shift_operators() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT 1 << 4"), Value::UInt(16));
    assert_eq!(scalar(&engine, "SELECT 256 >> 2"), Value::UInt(64));
    assert_eq!(scalar(&engine, "SELECT 1 << 0"), Value::UInt(1));
    assert_eq!(scalar(&engine, "SELECT 255 >> 8"), Value::UInt(0));
    assert_eq!(scalar(&engine, "SELECT 3 << 63"), Value::UInt(1u64 << 63));
}

#[test]
fn shift_count_of_64_or_more_is_zero() {
    // MySQL: shifting by >= 64 yields 0 (not the Rust shift-overflow panic).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT 1 << 64"), Value::UInt(0));
    assert_eq!(scalar(&engine, "SELECT 1 << 100"), Value::UInt(0));
    assert_eq!(scalar(&engine, "SELECT 255 >> 64"), Value::UInt(0));
    assert_eq!(scalar(&engine, "SELECT 255 >> 999"), Value::UInt(0));
}

#[test]
fn bitwise_not_returns_unsigned_bigint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // `~0` must surface as the unsigned 64-bit complement, NOT a signed -1.
    // (`~~` tokenizes as a single PG operator, so nest with parens.)
    let row = one_row(&engine, "SELECT ~0, ~5, ~(~5)");
    assert_eq!(row[0], Value::UInt(u64::MAX));
    assert_eq!(row[1], Value::UInt(u64::MAX - 5));
    assert_eq!(row[2], Value::UInt(5));
}

#[test]
fn bitwise_negative_operand_uses_twos_complement() {
    // MySQL treats a signed operand as its two's-complement bit pattern:
    // -1 is 0xFFFF…FFFF, so `-1 & 255` = 255 and `5 & -1` = 5.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT -1 & 255"), Value::UInt(255));
    assert_eq!(scalar(&engine, "SELECT 5 & -1"), Value::UInt(5));
    assert_eq!(scalar(&engine, "SELECT -1 | 0"), Value::UInt(u64::MAX),);
}

#[test]
fn bitwise_rounds_non_integer_operands() {
    // Decimal / real operands round half-away-from-zero to the nearest
    // integer before the bit operation, matching MySQL's BIGINT conversion.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT 5.7 | 0"), Value::UInt(6));
    assert_eq!(scalar(&engine, "SELECT 5.4 | 0"), Value::UInt(5));
    assert_eq!(scalar(&engine, "SELECT 2.5 | 0"), Value::UInt(3));
    assert_eq!(scalar(&engine, "SELECT 7.9 & 6"), Value::UInt(0)); // 8 & 6 = 0
}

#[test]
fn bitwise_binary_string_and_json_conversion_warnings_match_mysql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE bitwise_coercion(\
             c CHAR(8), b BLOB, t TEXT, j JSON)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO bitwise_coercion VALUES \
             ('abcdefg', x'cafebabe', 'abcdefg', JSON_OBJECT('a', 3))",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT c | 2147483647, b | 2147483647, \
                    t | 2147483647, j | 2147483647 \
             FROM bitwise_coercion",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0],
        vec![
            Value::UInt(2147483647),
            Value::UInt(2147483647),
            Value::UInt(2147483647),
            Value::UInt(2147483647),
        ]
    );
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected SHOW WARNINGS result set");
    };
    let codes = warnings
        .rows
        .iter()
        .map(|row| row[1].to_display_string())
        .collect::<Vec<_>>();
    assert_eq!(codes, vec!["1292", "3156"]);

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT c | x'cafebabe', t | x'cafebabe' FROM bitwise_coercion",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0],
        vec![Value::UInt(0xcafebabe), Value::UInt(0xcafebabe)]
    );
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected SHOW WARNINGS result set");
    };
    assert_eq!(warnings.rows.len(), 1);
    assert_eq!(warnings.rows[0][1].to_display_string(), "1292");
}

#[test]
fn text_blob_implicit_numeric_and_prepared_bit_aggregates_match_mysql_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE implicit_numeric(\
             c CHAR(2), v VARCHAR(2), b BINARY(2), vb VARBINARY(2), t TEXT, bl BLOB)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO implicit_numeric VALUES ('x','x','x','x','x','x')",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SELECT -c,-v,-b,-vb,-t,-bl,t < (1 & 1) FROM implicit_numeric",
        )
        .unwrap();
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected SHOW WARNINGS result set");
    };
    let codes = warnings
        .rows
        .iter()
        .map(|row| row[1].to_display_string())
        .collect::<Vec<_>>();
    assert_eq!(codes, vec!["1292", "1292", "1292", "1292"]);

    for (label, sql) in [
        (
            "json assignment",
            "do json_array(1, database(), bit_or(@c:=0xe9))",
        ),
        (
            "field/export_set",
            "do field('1',monthname(now()),bit_and(export_set(1,'',1,0x29,186)))",
        ),
        (
            "aes/concat",
            "do length(concat(1,bit_xor(aes_encrypt(1,1,1))))",
        ),
    ] {
        let prepared_source = sql.replace('\'', "''");
        engine
            .execute_with_session(&mut session, &format!("PREPARE s FROM '{prepared_source}'"))
            .unwrap();
        engine
            .execute_with_session(&mut session, "EXECUTE s")
            .unwrap();
        let QueryResponse::ResultSet(warnings) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!("expected SHOW WARNINGS result set");
        };
        let codes = warnings
            .rows
            .iter()
            .map(|row| row[1].to_display_string())
            .collect::<Vec<_>>();
        assert!(
            !codes.iter().any(|code| code == "1292"),
            "{label}: {:?}",
            warnings.rows
        );
        match label {
            "json assignment" => assert_eq!(codes, vec!["1287"]),
            "field/export_set" => {
                assert!(warnings.rows.is_empty(), "{label}: {:?}", warnings.rows)
            }
            "aes/concat" => {}
            _ => unreachable!(),
        }
        engine
            .execute_with_session(&mut session, "DEALLOCATE PREPARE s")
            .unwrap();
    }
}

#[test]
fn bitwise_null_propagation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT NULL & 1"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT 1 | NULL"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT NULL ^ NULL"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT NULL << 2"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT 2 >> NULL"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT ~NULL"), Value::Null);
}

#[test]
fn binary_string_bitwise_preserves_width_and_rejects_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE binary_bits (a VARBINARY(4), b VARBINARY(4))")
        .unwrap();
    engine
        .execute("INSERT INTO binary_bits VALUES (x'5939',x'6ac3')")
        .unwrap();
    assert_eq!(
        one_row(
            &engine,
            "SELECT HEX(a & b),HEX(a | b),HEX(a ^ b),HEX(~a),\
                    HEX(a << 4),HEX(a >> 4),BIT_COUNT(a) FROM binary_bits"
        ),
        vec![
            Value::String("4801".into()),
            Value::String("7BFB".into()),
            Value::String("33FA".into()),
            Value::String("A6C6".into()),
            Value::String("9390".into()),
            Value::String("0593".into()),
            Value::Int(8),
        ]
    );

    engine
        .execute("INSERT INTO binary_bits VALUES (x'5939',x'6a')")
        .unwrap();
    let err = engine
        .execute("SELECT a & b FROM binary_bits WHERE LENGTH(b)=1")
        .unwrap_err();
    assert_eq!(err.code(), 3513);
    assert_eq!(err.sql_state(), "HY000");

    // A hex literal remains a binary string when paired with a binary
    // column; it must not be unconditionally folded to BIGINT.
    let err = engine
        .execute("SELECT a | x'CAFEBABE' FROM binary_bits WHERE LENGTH(a)=2")
        .unwrap_err();
    assert_eq!(err.code(), 3513);
}

#[test]
fn bitwise_precedence_and_before_or() {
    // MySQL precedence: `&` binds tighter than `|`.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // 1 | (2 & 3) = 1 | 2 = 3
    assert_eq!(scalar(&engine, "SELECT 1 | 2 & 3"), Value::UInt(3));
    // (2 & 3) | 4 = 2 | 4 = 6
    assert_eq!(scalar(&engine, "SELECT 2 & 3 | 4"), Value::UInt(6));
}

// ---------------------------------------------------------------------------
// Logical XOR / && / || (numeric operands only).
// ---------------------------------------------------------------------------

#[test]
fn logical_xor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT 1 XOR 1"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 1 XOR 0"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 0 XOR 1"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 0 XOR 0"), Value::Int(0));
    // Any non-zero is truthy: 5 XOR 3 -> both true -> 0.
    assert_eq!(scalar(&engine, "SELECT 5 XOR 3"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 5 XOR 0"), Value::Int(1));
    // Left-associative chaining: (1 XOR 1) XOR 1 = 1.
    assert_eq!(scalar(&engine, "SELECT 1 XOR 1 XOR 1"), Value::Int(1));
}

#[test]
fn logical_xor_null_is_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT NULL XOR 1"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT 1 XOR NULL"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT NULL XOR NULL"), Value::Null);
}

#[test]
fn double_ampersand_is_logical_and() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Unambiguous truth table.
    assert_eq!(scalar(&engine, "SELECT 1 && 1"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 1 && 0"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 0 && 5"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 7 && 3"), Value::Int(1));
    // The contract of `&&` is exact equivalence to the AND keyword — verify
    // it across a matrix that includes NULL, whatever AND yields here.
    for (l, r) in [
        ("0", "0"),
        ("1", "0"),
        ("1", "1"),
        ("0", "NULL"),
        ("1", "NULL"),
        ("NULL", "NULL"),
    ] {
        let amp = scalar(&engine, &format!("SELECT {l} && {r}"));
        let kw = scalar(&engine, &format!("SELECT {l} AND {r}"));
        assert_eq!(amp, kw, "`{l} && {r}` must equal `{l} AND {r}`");
    }
}

#[test]
fn double_pipe_is_logical_or() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Default sql_mode: `||` is logical OR (NOT string concat).
    assert_eq!(scalar(&engine, "SELECT 0 || 0"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 0 || 5"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 1 || 0"), Value::Int(1));
    // The contract of `||` is exact equivalence to the OR keyword.
    for (l, r) in [
        ("0", "0"),
        ("1", "0"),
        ("0", "1"),
        ("0", "NULL"),
        ("1", "NULL"),
        ("NULL", "NULL"),
    ] {
        let pipe = scalar(&engine, &format!("SELECT {l} || {r}"));
        let kw = scalar(&engine, &format!("SELECT {l} OR {r}"));
        assert_eq!(pipe, kw, "`{l} || {r}` must equal `{l} OR {r}`");
    }
}

// ---------------------------------------------------------------------------
// Integer division operator `DIV`.
// ---------------------------------------------------------------------------

#[test]
fn integer_div_operator() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT 7 DIV 2"), Value::Int(3));
    assert_eq!(scalar(&engine, "SELECT 7 DIV 3"), Value::Int(2));
    assert_eq!(scalar(&engine, "SELECT -7 DIV 2"), Value::Int(-3));
    // Division by zero yields NULL (MySQL), not an error.
    assert_eq!(scalar(&engine, "SELECT 7 DIV 0"), Value::Null);
}

// ---------------------------------------------------------------------------
// Integer arithmetic type: `+ - * %` over integers stay integral (BIGINT),
// matching MySQL rather than silently widening to DOUBLE.
// ---------------------------------------------------------------------------

#[test]
fn integer_add_sub_mul_mod_are_integral() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT 5 + 3"), Value::Int(8));
    assert_eq!(scalar(&engine, "SELECT 5 - 8"), Value::Int(-3));
    assert_eq!(scalar(&engine, "SELECT 5 * 3"), Value::Int(15));
    assert_eq!(scalar(&engine, "SELECT 10 % 3"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT -10 % 3"), Value::Int(-1)); // sign of dividend
    assert_eq!(scalar(&engine, "SELECT 10 % 0"), Value::Null); // MySQL: % 0 = NULL
}

#[test]
fn integer_arithmetic_preserves_precision_past_f64() {
    // 2^53 + 1 is not representable in f64; the old Double path rounded it.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        scalar(&engine, "SELECT 9007199254740993 + 0"),
        Value::Int(9_007_199_254_740_993),
    );
    assert_eq!(
        scalar(&engine, "SELECT 9007199254740991 * 2 + 1"),
        Value::Int(18_014_398_509_481_983),
    );
}

#[test]
fn integer_overflow_raises_error_1690() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("SELECT 9223372036854775807 + 1")
        .unwrap_err();
    assert_eq!(err.code(), 1690, "expected BIGINT out-of-range error 1690");

    let err = engine
        .execute("SELECT 9223372036854775807 * 2")
        .unwrap_err();
    assert_eq!(err.code(), 1690);
}

#[test]
fn unsigned_propagates_through_arithmetic() {
    // A bitwise result is unsigned; adding to it stays unsigned (MySQL's
    // "either operand unsigned -> unsigned result" rule).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(scalar(&engine, "SELECT (5 & 7) + 1"), Value::UInt(6));
    assert_eq!(scalar(&engine, "SELECT (1 << 2) * 3"), Value::UInt(12));
}

#[test]
fn integer_arithmetic_over_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE n (a INT, b INT)").unwrap();
    engine.execute("INSERT INTO n VALUES (7, 3)").unwrap();
    let row = one_row(&engine, "SELECT a + b, a - b, a * b, a % b FROM n");
    assert_eq!(row[0], Value::Int(10));
    assert_eq!(row[1], Value::Int(4));
    assert_eq!(row[2], Value::Int(21));
    assert_eq!(row[3], Value::Int(1));
}

// ---------------------------------------------------------------------------
// Division `/` of exact operands yields a scaled DECIMAL (MySQL), and
// unary minus negates decimals / unsigned values.
// ---------------------------------------------------------------------------

#[test]
fn exact_division_produces_scaled_decimal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Scale = dividend scale + div_precision_increment (4).
    assert_eq!(
        scalar(&engine, "SELECT 10 / 3"),
        Value::DecimalCompact(33333, 4)
    );
    assert_eq!(
        scalar(&engine, "SELECT 1 / 3"),
        Value::DecimalCompact(3333, 4)
    );
    assert_eq!(
        scalar(&engine, "SELECT 2 / 3"),
        Value::DecimalCompact(6667, 4)
    ); // rounds up
    assert_eq!(
        scalar(&engine, "SELECT 22 / 7"),
        Value::DecimalCompact(31429, 4)
    );
    assert_eq!(
        scalar(&engine, "SELECT 10 / 2"),
        Value::DecimalCompact(50000, 4)
    );
    // Display form matches MySQL.
    assert_eq!(
        scalar(&engine, "SELECT 10 / 3").to_display_string(),
        "3.3333"
    );
    // Division by zero is NULL, not an error.
    assert_eq!(scalar(&engine, "SELECT 10 / 0"), Value::Null);
}

#[test]
fn division_with_decimal_dividend_keeps_extra_scale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // `10.5` has scale 1 -> result scale 5; `10.5 / 3` = 3.50000.
    assert_eq!(
        scalar(&engine, "SELECT 10.5 / 3").to_display_string(),
        "3.50000"
    );
    // `10 / 4.0`: dividend scale 0 -> result scale 4; = 2.5000.
    assert_eq!(
        scalar(&engine, "SELECT 10 / 4.0").to_display_string(),
        "2.5000"
    );
}

#[test]
fn unary_minus_negates_decimal_and_unsigned() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Decimal literal negation (previously errored "cannot negate").
    assert_eq!(
        scalar(&engine, "SELECT -(3.5)"),
        Value::DecimalCompact(-35, 1)
    );
    assert_eq!(scalar(&engine, "SELECT -(-3.5)").to_display_string(), "3.5");
    // Negating an unsigned (bitwise) result yields a signed value.
    assert_eq!(scalar(&engine, "SELECT -(5 & 3)"), Value::Int(-1));
    assert_eq!(scalar(&engine, "SELECT -(1 << 2)"), Value::Int(-4));

    // Negation of a DECIMAL column.
    engine
        .execute("CREATE TABLE money (d DECIMAL(10,2))")
        .unwrap();
    engine.execute("INSERT INTO money VALUES (3.50)").unwrap();
    assert_eq!(
        scalar(&engine, "SELECT -d FROM money").to_display_string(),
        "-3.50"
    );
}

// ---------------------------------------------------------------------------
// Column path — exercises the compiled expression engine (expr_program).
// ---------------------------------------------------------------------------

#[test]
fn bitwise_and_logical_over_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (12, 10)").unwrap();

    let row = one_row(
        &engine,
        "SELECT a & b, a | b, a ^ b, a << 1, b >> 1, ~a, a XOR 0, a DIV b FROM t",
    );
    assert_eq!(row[0], Value::UInt(8)); // 12 & 10
    assert_eq!(row[1], Value::UInt(14)); // 12 | 10
    assert_eq!(row[2], Value::UInt(6)); // 12 ^ 10
    assert_eq!(row[3], Value::UInt(24)); // 12 << 1
    assert_eq!(row[4], Value::UInt(5)); // 10 >> 1
    assert_eq!(row[5], Value::UInt(u64::MAX - 12)); // ~12
    assert_eq!(row[6], Value::Int(1)); // 12 XOR 0 -> true
    assert_eq!(row[7], Value::Int(1)); // 12 DIV 10
}

#[test]
fn bitwise_in_where_clause() {
    // Bitmask filtering — a common real-world use of `&`.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE flags (id INT, mask INT)")
        .unwrap();
    engine
        .execute("INSERT INTO flags VALUES (1, 5), (2, 2), (3, 7), (4, 8)")
        .unwrap();

    // Rows whose mask has bit 0 (value 1) set: 5 (101), 7 (111).
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM flags WHERE mask & 1 = 1 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<Value> = result.rows.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(ids, vec![Value::Int(1), Value::Int(3)]);
}

#[test]
fn operator_precedence_matches_mysql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Shifts bind looser than `+`/`-`: (1+2) << 3 = 24.
    assert_eq!(scalar(&engine, "SELECT 1 + 2 << 3"), Value::UInt(24));
    // `+` binds tighter than `>>`: 4 >> (1+1) = 1.
    assert_eq!(scalar(&engine, "SELECT 4 >> 1 + 1"), Value::UInt(1));
    // `+` binds tighter than `&`: 5 & (2+1) = 1.
    assert_eq!(scalar(&engine, "SELECT 5 & 2 + 1"), Value::UInt(1));
    // `&` binds tighter than `|`: 4 | (2&0) = 4.
    assert_eq!(scalar(&engine, "SELECT 4 | 2 & 0"), Value::UInt(4));
    // Logical precedence: AND binds tighter than XOR binds tighter than OR.
    // (1 AND 0) -> 0; 0 XOR 1 -> 1; 1 OR 0 -> 1.
    assert_eq!(scalar(&engine, "SELECT 1 OR 0 XOR 1 AND 0"), Value::Int(1));

    // Mixed-precedence cases are deliberately chosen so the two possible
    // parse trees do not happen to produce the same value.
    assert_eq!(scalar(&engine, "SELECT 4 << 1 ^ 1"), Value::UInt(4));
    assert_eq!(scalar(&engine, "SELECT 4 ^ 1 & 1"), Value::UInt(1));
    assert_eq!(scalar(&engine, "SELECT 4 & 1 << 2"), Value::UInt(4));
    assert_eq!(scalar(&engine, "SELECT 3 ^ 1 + 1"), Value::UInt(3));
    assert_eq!(scalar(&engine, "SELECT 0 XOR 2 = 2"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 1 XOR 1 AND 0"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 1 || 0 AND 0"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 8 DIV 2 + 1"), Value::Int(5));
    assert_eq!(scalar(&engine, "SELECT 8 DIV 2 * 2"), Value::Int(8));
    assert_eq!(scalar(&engine, "SELECT 16 DIV 4 DIV 2"), Value::Int(2));
}

#[test]
fn predicate_precedence_matches_mysql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT 5 BETWEEN 0 AND 10 BETWEEN 0 AND 1, \
         (5 BETWEEN 0 AND 10) BETWEEN 0 AND 1",
    );
    assert_eq!(row, vec![Value::Int(0), Value::Int(1)]);
    assert_eq!(scalar(&engine, "SELECT '10' LIKE '1%' = 1"), Value::Int(1));
    assert_eq!(
        scalar(&engine, "SELECT 'ab' REGEXP '^a' = 1"),
        Value::Int(1)
    );
    assert_eq!(
        scalar(&engine, "SELECT 2 = 1 BETWEEN 0 AND 1"),
        Value::Int(0)
    );
    assert_eq!(
        scalar(&engine, "SELECT 5 BETWEEN 0 AND 10 < 2"),
        Value::Int(1)
    );

    for sql in [
        "SELECT 1 LIKE 1 BETWEEN 0 AND 1",
        "SELECT 1 IN (1) BETWEEN 0 AND 1",
        "SELECT 1 IS TRUE = 1",
        "SELECT '10' LIKE '1%' IN (1)",
    ] {
        let err = engine.execute(sql).expect_err(sql);
        assert_eq!(err.code(), 1064, "{sql}: {err:?}");
    }
}

#[test]
fn mysql_prefix_operator_precedence_and_high_not_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    assert_eq!(scalar(&engine, "SELECT !0 = 2"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT ~1 * 0"), Value::UInt(0));
    assert_eq!(scalar(&engine, "SELECT -1 ^ 2"), Value::UInt(u64::MAX - 2));
    assert_eq!(scalar(&engine, "SELECT !!1"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT !!!1"), Value::Int(1));

    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='HIGH_NOT_PRECEDENCE'")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT NOT 0 = 2, NOT 1 BETWEEN 0 AND 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(0), Value::Int(1)]]);
}

#[test]
fn deprecated_operator_warnings_match_mysql_order_and_prepare_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SELECT !1, !!1, !!!1, 1 && 1, 1 || 0")
        .unwrap();
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected SHOW WARNINGS result set");
    };
    let messages = warnings
        .rows
        .iter()
        .map(|row| (row[1].to_display_string(), row[2].to_display_string()))
        .collect::<Vec<_>>();
    assert_eq!(messages.len(), 6);
    assert!(messages.iter().all(|(code, _)| code == "1287"));
    assert!(messages[..4]
        .iter()
        .all(|(_, message)| message.starts_with("'!' is deprecated")));
    assert!(messages[4].1.starts_with("'&&' is deprecated"));
    assert!(messages[5]
        .1
        .starts_with("'|| as a synonym for OR' is deprecated"));

    engine
        .execute_with_session(&mut session, "SET sql_mode='PIPES_AS_CONCAT'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SELECT !1, 1 && 1, 1 || 0")
        .unwrap();
    assert_eq!(session.warning_count(), 2, "concat-mode || must not warn");

    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE op_warn FROM 'SELECT !1, 1 && 1, 1 || 0'",
        )
        .unwrap();
    assert_eq!(session.warning_count(), 3, "warnings belong to PREPARE");
    engine
        .execute_with_session(&mut session, "EXECUTE op_warn")
        .unwrap();
    assert_eq!(
        session.warning_count(),
        3,
        "EXECUTE starts a fresh diagnostics area and republishes source warnings"
    );
}

#[test]
fn sql_prepare_freezes_pipes_as_concat_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let scalar_with = |session: &mut SessionContext, sql: &str| {
        let QueryResponse::ResultSet(result) = engine.execute_with_session(session, sql).unwrap()
        else {
            panic!("expected result set: {sql}");
        };
        result.rows[0][0].clone()
    };

    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    engine
        .execute_with_session(&mut session, "PREPARE p FROM 'SELECT 2 * 3 || 4'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode='PIPES_AS_CONCAT'")
        .unwrap();
    assert_eq!(scalar_with(&mut session, "EXECUTE p"), Value::Int(1));
    engine
        .execute_with_session(&mut session, "DEALLOCATE PREPARE p")
        .unwrap();

    engine
        .execute_with_session(&mut session, "PREPARE p FROM 'SELECT 2 * 3 || 4'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    assert_eq!(scalar_with(&mut session, "EXECUTE p"), Value::Double(68.0));
}

#[test]
fn user_variable_assignment_has_mysql_low_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE assign_src(id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO assign_src VALUES (1)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT @x := 0 OR 2, @x FROM assign_src")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(1)]]);
    assert_eq!(session.warning_count(), 1);
}

#[test]
fn persisted_expressions_keep_creation_time_concat_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='PIPES_AS_CONCAT'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE VIEW concat_view AS SELECT 2 * 3 || 4 AS value",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE concat_generated(\
             a INT, g VARCHAR(20) GENERATED ALWAYS AS (a || 4) STORED)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE concat_checked(\
             a INT CHECK ((a || 4) = '34'))",
        )
        .unwrap();
    let stored_view = engine
        .db_for_tests()
        .view_definition("test", "concat_view")
        .expect("stored view");
    assert!(
        stored_view
            .definition_normalized
            .to_ascii_lowercase()
            .contains("concat("),
        "stored definition must be mode independent: {:?}",
        stored_view.definition_normalized
    );

    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    // Reopen to force CHECK/generated/view expressions through their
    // persisted, mode-independent SQL rather than relying on live ASTs.
    engine.shutdown().unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    let QueryResponse::ResultSet(view) = engine
        .execute_with_session(&mut session, "SELECT value FROM concat_view")
        .unwrap()
    else {
        panic!("expected view result");
    };
    assert_eq!(view.rows[0][0].to_display_string(), "68");

    engine
        .execute_with_session(&mut session, "INSERT INTO concat_generated(a) VALUES (3)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO concat_checked(a) VALUES (3)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO concat_checked(a) VALUES (2)")
        .unwrap_err();
    assert_eq!(err.code(), 3819);
    let QueryResponse::ResultSet(generated) = engine
        .execute_with_session(&mut session, "SELECT g FROM concat_generated")
        .unwrap()
    else {
        panic!("expected generated-column result");
    };
    assert_eq!(generated.rows, vec![vec![Value::String("34".to_string())]]);
}

// ---------------------------------------------------------------------------
// Logical AND / OR three-valued logic (MySQL 8.0): NULL propagation.
//   TRUE  AND NULL = NULL    FALSE AND NULL = FALSE    NULL AND NULL = NULL
//   TRUE  OR  NULL = TRUE    FALSE OR  NULL = NULL     NULL OR  NULL = NULL
// ---------------------------------------------------------------------------

#[test]
fn logical_and_or_null_three_valued_constants() {
    // Constant path: eval_const / eval_const_runtime / eval_expr_legacy.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // AND
    assert_eq!(scalar(&engine, "SELECT 1 AND NULL"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT NULL AND 1"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT 0 AND NULL"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT NULL AND 0"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT NULL AND NULL"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT 1 AND 1"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 1 AND 0"), Value::Int(0));
    // OR
    assert_eq!(scalar(&engine, "SELECT 1 OR NULL"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT NULL OR 1"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 0 OR NULL"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT NULL OR 0"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT NULL OR NULL"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT 0 OR 0"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 0 OR 1"), Value::Int(1));
    // `&&` / `||` inherit the same three-valued semantics.
    assert_eq!(scalar(&engine, "SELECT 1 && NULL"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT 0 && NULL"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 1 || NULL"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT 0 || NULL"), Value::Null);
}

#[test]
fn logical_and_or_null_three_valued_columns() {
    // Column path: eval_plan_expr_with_db / compiled expr_program /
    // eval_plan_binary_legacy. t=TRUE(1), f=FALSE(0), n=NULL.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE tri (id INT, t INT, f INT, n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO tri VALUES (1, 1, 0, NULL)")
        .unwrap();
    // AND
    assert_eq!(scalar(&engine, "SELECT t AND n FROM tri"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT n AND t FROM tri"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT f AND n FROM tri"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT n AND f FROM tri"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT n AND n FROM tri"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT t AND t FROM tri"), Value::Int(1));
    // OR
    assert_eq!(scalar(&engine, "SELECT f OR n FROM tri"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT n OR f FROM tri"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT t OR n FROM tri"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT n OR t FROM tri"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT n OR n FROM tri"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT f OR f FROM tri"), Value::Int(0));
    // `&&` / `||` over columns.
    assert_eq!(scalar(&engine, "SELECT t && n FROM tri"), Value::Null);
    assert_eq!(scalar(&engine, "SELECT f || n FROM tri"), Value::Null);
    // WHERE still treats NULL as false (truthy: NULL -> exclude row).
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id FROM tri WHERE t AND n").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows.len(),
        0,
        "TRUE AND NULL is NULL -> row excluded"
    );
}

// ---------------------------------------------------------------------------
// String boolean coercion: MySQL takes a string's *numeric* value, not a
// non-empty/non-"0" test. `truthy('a') == truthy('0.0') == false`, while
// `truthy('1abc') == truthy('12xy') == true`. Feeds IF / AND / OR / XOR / NOT
// and WHERE filtering. (Bitwise / arithmetic on non-numeric strings instead
// route through `numeric()` which ERRORS — a separate, intentional deviation,
// so it is not exercised here.)
// ---------------------------------------------------------------------------

#[test]
fn string_if_numeric_truthiness_constants() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Non-numeric / zero-valued prefixes are FALSE.
    assert_eq!(scalar(&engine, "SELECT IF('a', 1, 0)"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT IF('', 1, 0)"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT IF(' ', 1, 0)"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT IF('0', 1, 0)"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT IF('0.0', 1, 0)"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT IF('-0', 1, 0)"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT IF('.', 1, 0)"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT IF('abc', 1, 0)"), Value::Int(0));
    // A leading numeric prefix is TRUE; trailing garbage is tolerated.
    assert_eq!(scalar(&engine, "SELECT IF('1x', 1, 0)"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT IF('1abc', 1, 0)"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT IF('12abc', 1, 0)"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT IF('5', 1, 0)"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT IF('-3', 1, 0)"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT IF('0.5', 1, 0)"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT IF(' 5 ', 1, 0)"), Value::Int(1));
    // Exponent forms: well-formed exponent consumed; dangling 'e' is garbage
    // after the mantissa (still TRUE).
    assert_eq!(scalar(&engine, "SELECT IF('1e3', 1, 0)"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT IF('1e', 1, 0)"), Value::Int(1));
}

#[test]
fn string_logical_operators_numeric_truthiness_constants() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // AND / OR fold each operand through the numeric prefix.
    assert_eq!(scalar(&engine, "SELECT 'a' AND 1"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT '1abc' AND 1"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT '0.0' AND 1"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 'a' OR 0"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT '5' OR 0"), Value::Int(1));
    // NOT / XOR.
    assert_eq!(scalar(&engine, "SELECT NOT 'a'"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT NOT '5'"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT '2' XOR '3'"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 'a' XOR '5'"), Value::Int(1));
}

#[test]
fn string_column_where_numeric_truthiness() {
    // Column path: `WHERE s` evaluates the bare identifier through `truthy`
    // via the compiled expr_program / legacy plan eval. Only rows whose
    // string has a non-zero numeric prefix survive.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE s (id INT, v VARCHAR(16))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO s VALUES (1,'a'), (2,'0'), (3,'5'), (4,'0.0'), \
             (5,'1abc'), (6,' '), (7,''), (8,'-3'), (9,'0.5')",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM s WHERE v ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<Value> = result.rows.into_iter().map(|r| r[0].clone()).collect();
    // Truthy: '5'(3), '1abc'(5), '-3'(8), '0.5'(9). The rest coerce to 0.
    assert_eq!(
        ids,
        vec![Value::Int(3), Value::Int(5), Value::Int(8), Value::Int(9)],
    );
}

#[test]
fn string_column_if_numeric_truthiness() {
    // Same coercion, but exercised through IF over a column so the compiled
    // expression engine evaluates `truthy` per row.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE s (id INT, v VARCHAR(16))")
        .unwrap();
    engine
        .execute("INSERT INTO s VALUES (1,'a'), (2,'1x'), (3,'0.0'), (4,'12abc')")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, IF(v, 1, 0) FROM s ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let flags: Vec<Value> = result.rows.into_iter().map(|r| r[1].clone()).collect();
    assert_eq!(
        flags,
        vec![Value::Int(0), Value::Int(1), Value::Int(0), Value::Int(1)],
    );
}

// ---------------------------------------------------------------------------
// SOUNDS LIKE — rewritten to SOUNDEX(a) = SOUNDEX(b). MySQL 8.0 semantics.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// `^` operator precedence — MySQL 8.0 binds `^` tighter than `* / DIV % MOD`,
// opposite to sqlparser's C-like default. Verify the reassociation.
// ---------------------------------------------------------------------------

#[test]
fn bitxor_precedence_above_mul_div_mod() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // 1^2*3 == (1^2)*3 == 3*3 == 9. sqlparser default would give 1^(2*3) = 7.
    assert_eq!(scalar(&engine, "SELECT 1 ^ 2 * 3"), Value::UInt(9));
    // Chained: 1^2*3*4 == ((1^2)*3)*4 == 36.
    assert_eq!(scalar(&engine, "SELECT 1 ^ 2 * 3 * 4"), Value::UInt(36));
    // Parenthesised right side is honoured: 1^(2*3) = 7.
    assert_eq!(scalar(&engine, "SELECT 1 ^ (2 * 3)"), Value::UInt(7));
    // `^` also binds on the right of multiplication: 2 * (3^1) = 4.
    assert_eq!(scalar(&engine, "SELECT 2 * 3 ^ 1"), Value::UInt(4));
    // Division: 6^4/2 = (6^4)/2 = 2/2 = 1.0000 (MySQL `/` returns DECIMAL).
    assert_eq!(
        scalar(&engine, "SELECT 6 ^ 4 / 2").to_display_string(),
        "1.0000",
    );
    // Mod: 7^3 % 4 = (7^3) % 4 = 4 % 4 = 0.
    assert_eq!(scalar(&engine, "SELECT 7 ^ 3 % 4"), Value::UInt(0));
    // DIV (integer div): 5^3 DIV 2 = (5^3) DIV 2 = 6 DIV 2 = 3.
    assert_eq!(scalar(&engine, "SELECT 5 ^ 3 DIV 2"), Value::UInt(3));
}

// ---------------------------------------------------------------------------
// Hex / bit literal numeric coercion. MySQL stores `0xCAFE` / `b'1010'` as
// binary strings by default but coerces them to BIGINT UNSIGNED in arithmetic
// and bitwise context. Provenance is held by `Literal::HexBytes/BitBytes`;
// generic BLOB column values (also `Value::Bytes`) must NOT auto-coerce.
// ---------------------------------------------------------------------------

#[test]
fn hex_literal_numeric_in_arithmetic_and_bitwise() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    assert_eq!(scalar(&engine, "SELECT 0xFF + 1"), Value::UInt(256));
    assert_eq!(scalar(&engine, "SELECT 0xFF * 2"), Value::UInt(510));
    assert_eq!(scalar(&engine, "SELECT 0xFF & 0xF0"), Value::UInt(0xF0));
    assert_eq!(scalar(&engine, "SELECT 0xFF | 0x01"), Value::UInt(0xFF));
    assert_eq!(
        scalar(&engine, "SELECT 0xFF ^ 0xAA"),
        Value::UInt(0xFF ^ 0xAA)
    );
    assert_eq!(scalar(&engine, "SELECT 0x01 << 4"), Value::UInt(0x10));
    // Mixed hex + decimal in bitwise.
    assert_eq!(scalar(&engine, "SELECT 255 & 0xF0"), Value::UInt(0xF0));
    // Multi-byte hex → big-endian u64.
    assert_eq!(
        scalar(&engine, "SELECT 0xCAFEBABE + 0"),
        Value::UInt(0xCAFEBABE)
    );
    engine.execute("CREATE TABLE hex_numeric(a INT)").unwrap();
    engine
        .execute("INSERT INTO hex_numeric VALUES (64)")
        .unwrap();
    assert_eq!(
        scalar(&engine, "SELECT a | x'CAFEBABE' FROM hex_numeric"),
        Value::UInt(0xCAFEBABE | 64)
    );
}

#[test]
fn bit_literal_numeric_in_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    assert_eq!(scalar(&engine, "SELECT b'1010' + 1"), Value::UInt(11));
    assert_eq!(scalar(&engine, "SELECT b'1111' * 2"), Value::UInt(30));
    assert_eq!(
        scalar(&engine, "SELECT b'1010' & b'1100'"),
        Value::UInt(0b1000)
    );
}

#[test]
fn hex_literal_string_context_preserves_bytes() {
    // In string-y context (HEX(), LENGTH()) the hex literal should still be a
    // binary string, not a u64. Verifies that the coercion only fires through
    // the arithmetic/bitwise eval branch — Literal provenance survives when
    // those paths aren't taken.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // HEX(0xCAFEBABE) renders the original bytes as hex digits.
    assert_eq!(
        scalar(&engine, "SELECT HEX(0xCAFEBABE)").to_display_string(),
        "CAFEBABE",
    );
    // LENGTH of a 4-byte hex literal is 4 (binary string length), NOT the
    // number of digits in the integer 3405691582.
    assert_eq!(scalar(&engine, "SELECT LENGTH(0xCAFEBABE)"), Value::Int(4));
}

#[test]
fn blob_column_value_is_not_auto_coerced_to_numeric() {
    // BLOB column stores `Value::Bytes`; arithmetic against another bytes
    // operand should NOT auto-coerce — only literal provenance triggers
    // coercion. Verify by reading from a BLOB column.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, b BLOB)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 0xCAFEBABE)")
        .unwrap();

    // Adding two BLOB column values together — both are Value::Bytes WITHOUT
    // Literal provenance — should NOT coerce. mydb falls through to its
    // existing arithmetic semantics for Bytes (numeric() converts).
    // This is a regression guard: if the coercion fires when it shouldn't,
    // the read of HEX(b) would still print the original 8-hex-digit form.
    let row = one_row(&engine, "SELECT HEX(b) FROM t");
    assert_eq!(row[0].to_display_string(), "CAFEBABE");
}

// ---------------------------------------------------------------------------
// PIPES_AS_CONCAT — sql_mode toggle between logical OR and string CONCAT for
// the `||` operator.
// ---------------------------------------------------------------------------

#[test]
fn pipes_or_default_is_logical_or() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Without PIPES_AS_CONCAT (default), `||` is OR (truthy of strings is 0).
    assert_eq!(scalar(&engine, "SELECT 'a' || 'b'"), Value::Int(0));
    assert_eq!(scalar(&engine, "SELECT 1 || 0"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT NULL || 1"), Value::Int(1));
    assert_eq!(scalar(&engine, "SELECT NULL || 0"), Value::Null);
}

#[test]
fn pipes_or_under_pipes_as_concat_returns_string() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Need a sticky session — `engine.execute(...)` constructs a fresh
    // `SessionContext` each call so `SET sql_mode=...` wouldn't persist.
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='PIPES_AS_CONCAT'")
        .unwrap();

    let mut scalar_with = |sql: &str| -> Value {
        let QueryResponse::ResultSet(result) =
            engine.execute_with_session(&mut session, sql).unwrap()
        else {
            panic!("expected result set for: {sql}");
        };
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].len(), 1);
        result
            .rows
            .into_iter()
            .next()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    };

    assert_eq!(
        scalar_with("SELECT 'a' || 'b'"),
        Value::String("ab".to_string())
    );
    assert_eq!(
        scalar_with("SELECT 1 || 0"),
        Value::String("10".to_string())
    );
    // NULL propagates through CONCAT (MySQL behaviour).
    assert_eq!(scalar_with("SELECT NULL || 'b'"), Value::Null);
    assert_eq!(scalar_with("SELECT 2 * 3 || 4"), Value::Double(68.0));
}

#[test]
fn sounds_like_constants_match_soundex_equality() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // 'Robert' and 'Rupert' both Soundex to R163; 'a' and 'A' both A000.
    assert_eq!(
        scalar(&engine, "SELECT 'Robert' SOUNDS LIKE 'Rupert'"),
        Value::Int(1),
    );
    assert_eq!(scalar(&engine, "SELECT 'a' SOUNDS LIKE 'A'"), Value::Int(1),);
    assert_eq!(
        scalar(&engine, "SELECT 'Smith' SOUNDS LIKE 'Apple'"),
        Value::Int(0),
    );
}

#[test]
fn sounds_like_in_where_clause_filters_phonetic_match() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE names (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO names VALUES (1,'Robert'),(2,'Rupert'),(3,'Sarah'),(4,'Sahra')")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM names WHERE name SOUNDS LIKE 'Robert' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<Value> = result.rows.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(ids, vec![Value::Int(1), Value::Int(2)]);
}
