//! M1 P0: string functions on Value::Bytes/BINARY use byte kernels,
//! preserving binary semantics like MySQL 8.0. Each comparison uses observed local MySQL
//! behavior, checking HEX output, type, and length.
//!
//! Fixes covered:
//! CHAR_LENGTH(binary) counts bytes rather than lossy-decoded UTF-8 characters.
//! REVERSE(binary) reverses bytes and preserves Value::Bytes.
//! REPLACE uses byte search/replacement and returns Value::Bytes if any argument is binary.
//! LPAD/RPAD operate on bytes when the source or pad is Value::Bytes.
//! SUBSTRING/LEFT/RIGHT/INSERT use byte offsets for Value::Bytes.
//! SUBSTRING_INDEX/LOCATE/INSTR/POSITION use byte semantics for Value::Bytes.
//!
//! Also cover independent M1 P0 issues:
//! ORD packs bytes big-endian; ORD('é')=50089.
//! CHAR(N1,N2,...) concatenates big-endian bytes into binary.
//! QUOTE escapes NUL and Ctrl-Z.
//! Remove implicit CHAR fallback for unknown CAST targets; return 1064.

use super::support::query_one_row as one_row;
use super::*;

fn b(bytes: &[u8]) -> Value {
    Value::Bytes(bytes.to_vec())
}

#[test]
fn char_length_of_binary_is_byte_count() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // X'C380' is valid one-character UTF-8 U+00C0, but BINARY length must be 2.
    let row = one_row(
        &engine,
        "SELECT CHAR_LENGTH(_binary X'C380'), LENGTH(_binary X'C380')",
    );
    assert_eq!(row[0], Value::Int(2), "CHAR_LENGTH(binary) = byte count");
    assert_eq!(row[1], Value::Int(2));

    // Empty binary has length zero.
    let row = one_row(&engine, "SELECT CHAR_LENGTH(_binary '')");
    assert_eq!(row[0], Value::Int(0));

    // Binary containing invalid UTF-8 sequences.
    let row = one_row(&engine, "SELECT CHAR_LENGTH(_binary X'FFFEC0')");
    assert_eq!(row[0], Value::Int(3));
}

#[test]
fn reverse_of_binary_flips_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Byte reversal: C3 A9 80 becomes 80 A9 C3.
    let row = one_row(&engine, "SELECT HEX(REVERSE(_binary X'C3A980'))");
    assert_eq!(row[0], Value::String("80A9C3".to_string()));

    // Empty binary remains empty.
    let row = one_row(&engine, "SELECT REVERSE(_binary '')");
    assert_eq!(row[0], b(b""));

    // Text REVERSE still reverses characters.
    let row = one_row(&engine, "SELECT REVERSE('abc')");
    assert_eq!(row[0], Value::String("cba".to_string()));
}

#[test]
fn replace_of_binary_uses_byte_search() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Replace each C3 in C3 80 C3 81 with FF to get FF 80 FF 81.
    let row = one_row(
        &engine,
        "SELECT HEX(REPLACE(_binary X'C380C381', _binary X'C3', _binary X'FF'))",
    );
    assert_eq!(row[0], Value::String("FF80FF81".to_string()));

    // Empty from preserves the original string.
    let row = one_row(
        &engine,
        "SELECT HEX(REPLACE(_binary X'AABB', _binary '', _binary X'CC'))",
    );
    assert_eq!(row[0], Value::String("AABB".to_string()));

    // Replacement increases length.
    let row = one_row(
        &engine,
        "SELECT HEX(REPLACE(_binary X'AA', _binary X'AA', _binary X'01020304'))",
    );
    assert_eq!(row[0], Value::String("01020304".to_string()));
}

#[test]
fn lpad_rpad_of_binary_pads_by_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // LPAD(BINARY 'ab', 5, BINARY X'00') → 00 00 00 61 62
    let row = one_row(&engine, "SELECT HEX(LPAD(_binary 'ab', 5, _binary X'00'))");
    assert_eq!(row[0], Value::String("0000006162".to_string()));

    let row = one_row(&engine, "SELECT HEX(RPAD(_binary 'ab', 5, _binary X'FF'))");
    assert_eq!(row[0], Value::String("6162FFFFFF".to_string()));

    // Pad cycling with multibyte padding.
    let row = one_row(&engine, "SELECT HEX(RPAD(_binary '', 5, _binary X'1234'))");
    assert_eq!(row[0], Value::String("1234123412".to_string()));
}

#[test]
fn substring_left_right_of_binary_by_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // SUBSTRING(_binary X'C3A9C280', 1, 2) → C3 A9
    let row = one_row(&engine, "SELECT HEX(SUBSTRING(_binary X'C3A9C280', 1, 2))");
    assert_eq!(row[0], Value::String("C3A9".to_string()));

    // LEFT/RIGHT use byte offsets.
    let row = one_row(&engine, "SELECT HEX(LEFT(_binary X'C3A9C280', 1))");
    assert_eq!(row[0], Value::String("C3".to_string()));

    let row = one_row(&engine, "SELECT HEX(RIGHT(_binary X'C3A9C280', 1))");
    assert_eq!(row[0], Value::String("80".to_string()));

    // Negative start.
    let row = one_row(&engine, "SELECT HEX(SUBSTRING(_binary X'C3A9C280', -2))");
    assert_eq!(row[0], Value::String("C280".to_string()));
}

#[test]
fn insert_of_binary_by_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT HEX(INSERT(_binary X'AABBCC', 2, 1, _binary X'FF'))",
    );
    assert_eq!(row[0], Value::String("AAFFCC".to_string()));

    // Out-of-range pos preserves the source.
    let row = one_row(
        &engine,
        "SELECT HEX(INSERT(_binary X'AABBCC', 10, 1, _binary X'FF'))",
    );
    assert_eq!(row[0], Value::String("AABBCC".to_string()));

    // Negative len replaces through the end.
    let row = one_row(
        &engine,
        "SELECT HEX(INSERT(_binary X'AABBCC', 2, -1, _binary X'FF'))",
    );
    assert_eq!(row[0], Value::String("AAFF".to_string()));
}

#[test]
fn substring_index_of_binary() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Use 0x00 as delimiter.
    let row = one_row(
        &engine,
        "SELECT HEX(SUBSTRING_INDEX(_binary X'0100020003', _binary X'00', 2))",
    );
    assert_eq!(row[0], Value::String("010002".to_string()));

    let row = one_row(
        &engine,
        "SELECT HEX(SUBSTRING_INDEX(_binary X'0100020003', _binary X'00', -2))",
    );
    assert_eq!(row[0], Value::String("020003".to_string()));
}

#[test]
fn locate_of_binary_by_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // LOCATE searches byte substrings with one-based positions.
    let row = one_row(&engine, "SELECT LOCATE(_binary X'C3', _binary X'C3A9C280')");
    assert_eq!(row[0], Value::Int(1));

    let row = one_row(&engine, "SELECT LOCATE(_binary X'80', _binary X'C3A9C280')");
    assert_eq!(row[0], Value::Int(4));

    // Missing matches return zero.
    let row = one_row(&engine, "SELECT LOCATE(_binary X'FF', _binary X'C3A9C280')");
    assert_eq!(row[0], Value::Int(0));

    // MySQL json_no_table.test regression: a binary needle longer than
    // the remaining haystack must return 0 instead of slicing out of bounds.
    let row = one_row(&engine, "SELECT INSTR(1, 0xf722a67b)");
    assert_eq!(row[0], Value::Int(0));

    // Empty needle.
    let row = one_row(&engine, "SELECT LOCATE(_binary '', _binary X'AABB', 1)");
    assert_eq!(row[0], Value::Int(1));
}

#[test]
fn ord_packs_bytes_big_endian() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // 'é' UTF-8 = C3 A9 → 0xC3*256 + 0xA9 = 50089
    let row = one_row(&engine, "SELECT ORD('é')");
    assert_eq!(row[0], Value::Int(50089));

    // U+4E2D encodes as E4 B8 AD; big-endian packing gives 14989485.
    let row = one_row(&engine, "SELECT ORD('中')");
    assert_eq!(row[0], Value::Int(14989485));

    // ASCII still equals its code point.
    let row = one_row(&engine, "SELECT ORD('A')");
    assert_eq!(row[0], Value::Int(65));

    // Empty string yields zero.
    let row = one_row(&engine, "SELECT ORD('')");
    assert_eq!(row[0], Value::Int(0));

    // First binary byte.
    let row = one_row(&engine, "SELECT ORD(_binary X'FF00')");
    assert_eq!(row[0], Value::Int(255));
}

#[test]
fn char_packs_bytes_big_endian() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // CHAR(65) → 'A' (1 byte)
    let row = one_row(&engine, "SELECT HEX(CHAR(65))");
    assert_eq!(row[0], Value::String("41".to_string()));

    // CHAR(256) → 0x01 0x00 (2 bytes)
    let row = one_row(&engine, "SELECT HEX(CHAR(256))");
    assert_eq!(row[0], Value::String("0100".to_string()));

    // CHAR(195, 169) → C3 A9 (2 bytes binary). Using decimal because
    // sqlparser 0x-hex literals go through a different Value path.
    let row = one_row(&engine, "SELECT HEX(CHAR(195, 169))");
    assert_eq!(row[0], Value::String("C3A9".to_string()));

    // CHAR(0) → 0x00 (1 zero byte, not empty)
    let row = one_row(&engine, "SELECT HEX(CHAR(0))");
    assert_eq!(row[0], Value::String("00".to_string()));

    // CHAR with NULL arg skipped
    let row = one_row(&engine, "SELECT HEX(CHAR(65, NULL, 66))");
    assert_eq!(row[0], Value::String("4142".to_string()));

    // The result type is BINARY; LENGTH counts bytes.
    let row = one_row(&engine, "SELECT LENGTH(CHAR(228, 184, 173))");
    assert_eq!(row[0], Value::Int(3));
}

#[test]
fn quote_escapes_nul_and_ctrl_z() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // QUOTE(CHAR(0)) → '\0' (4 chars: quote, backslash, zero, quote)
    let row = one_row(&engine, "SELECT QUOTE(CHAR(0))");
    assert_eq!(row[0], Value::String("'\\0'".to_string()));

    // QUOTE(CHAR(26)) → '\Z' — 0x1A is Ctrl-Z.
    let row = one_row(&engine, "SELECT QUOTE(CHAR(26))");
    assert_eq!(row[0], Value::String("'\\Z'".to_string()));

    // Preserve existing escape behavior.
    let row = one_row(&engine, "SELECT QUOTE('O\\'Reilly')");
    assert_eq!(row[0], Value::String("'O\\'Reilly'".to_string()));

    // QUOTE is intentionally not NULL-propagating: its output is ready to
    // splice into generated SQL.
    let row = one_row(&engine, "SELECT QUOTE(NULL), CONCAT('a', QUOTE(NULL))");
    assert_eq!(row[0], Value::String("NULL".to_string()));
    assert_eq!(row[1], Value::String("aNULL".to_string()));
}

#[test]
fn unknown_cast_target_reports_1064() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // sqlparser rejects CAST AS UUID/BYTEA;
    // these must now return syntax errors rather than silently falling back to CHAR.
    // Use UUID rather than an unknown identifier, which enters the Custom branch.
    let err = engine.execute("SELECT CAST('x' AS UUID)");
    assert!(
        err.is_err(),
        "CAST AS UUID should fail rather than fallback to CHAR"
    );
}

#[test]
fn cast_as_binary_with_length_pads_or_truncates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Padded with 0x00 to the requested length.
    let row = one_row(&engine, "SELECT HEX(CAST('abc' AS BINARY(5)))");
    assert_eq!(row[0], Value::String("6162630000".to_string()));

    // Truncated to the requested length.
    let row = one_row(&engine, "SELECT HEX(CAST('abcdef' AS BINARY(3)))");
    assert_eq!(row[0], Value::String("616263".to_string()));

    // Exact fit — no change.
    let row = one_row(&engine, "SELECT HEX(CAST('abc' AS BINARY(3)))");
    assert_eq!(row[0], Value::String("616263".to_string()));

    // No length keeps source bytes.
    let row = one_row(&engine, "SELECT HEX(CAST('abc' AS BINARY))");
    assert_eq!(row[0], Value::String("616263".to_string()));
}

#[test]
fn cast_as_binary_checks_packet_limit_before_allocation() {
    struct ResetPacketLimit;
    impl Drop for ResetPacketLimit {
        fn drop(&mut self) {
            crate::engine::sysvar::builtin::connection::reset_max_allowed_packet_for_mtr();
        }
    }

    let _reset = ResetPacketLimit;
    crate::engine::sysvar::builtin::connection::reset_max_allowed_packet_for_mtr();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET GLOBAL max_allowed_packet = 1024")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT CAST('x' AS BINARY(1025))")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let row = &result.rows[0];
    assert_eq!(row[0], Value::Null);
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warning result");
    };
    assert_eq!(warnings.rows.len(), 1);
    assert_eq!(warnings.rows[0][1], Value::Int(1301));

    // A NULL input still validates the declared width and publishes binary
    // LOB metadata. CTAS must therefore create LONGBLOB rather than narrowing
    // the all-NULL observed value to TEXT.
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT CAST(NULL AS BINARY(4294967295)) AS payload",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Null);
    assert_eq!(result.columns[0].column_type, ColumnType::Blob);
    assert_eq!(result.columns[0].source.length, Some(u32::MAX));
    assert_ne!(
        result.columns[0].source.flags & crate::mysql::protocol::BINARY_FLAG,
        0
    );
    assert!(session
        .warnings
        .iter()
        .any(|warning| warning.code() == 1301));

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE binary_null_copy AS \
             SELECT CAST(NULL AS BINARY(4294967295)) AS payload",
        )
        .unwrap();
    let table = engine.db.table("test.binary_null_copy").unwrap();
    assert!(matches!(
        table.columns[0].data_type,
        crate::storage::DataTypeKind::Blob { max_len }
            if max_len == crate::storage::schema::LONG_LOB_MAX
    ));
    assert!(session
        .warnings
        .iter()
        .any(|warning| warning.code() == 1301));

    // Planning the maximum legal display width must take the guarded path
    // without constructing a 4-GiB folded constant.
    engine
        .execute_with_session(
            &mut session,
            "EXPLAIN SELECT CAST('x' AS BINARY(4294967295))",
        )
        .unwrap();
}

#[test]
fn to_base64_line_wraps_from_base64_ignores_whitespace() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // TO_BASE64 wraps every 76 chars — 100 bytes → 136 base64 chars → one newline.
    let row = one_row(&engine, "SELECT TO_BASE64(REPEAT('a', 100))");
    let out = match &row[0] {
        Value::String(s) => s.clone(),
        v => panic!("expected string, got {v:?}"),
    };
    assert!(
        out.contains('\n'),
        "TO_BASE64 100-byte input should contain a newline: {out:?}"
    );
    assert!(out.len() > 76, "encoded length {}", out.len());

    // Round-trip: FROM_BASE64 accepts the wrapped output.
    let row = one_row(&engine, "SELECT FROM_BASE64(TO_BASE64(REPEAT('a', 100)))");
    assert_eq!(row[0], Value::Bytes("a".repeat(100).into_bytes()));

    // FROM_BASE64 also tolerates externally-injected whitespace.
    let row = one_row(&engine, "SELECT FROM_BASE64('YWJj\\n\\rZGVm')");
    assert_eq!(row[0], Value::Bytes(b"abcdef".to_vec()));
}

#[test]
fn random_bytes_error_codes_split_by_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // The lower boundary is an out-of-range length, just like > 1024.
    let err = engine.execute("SELECT RANDOM_BYTES(0)").unwrap_err();
    assert_eq!(err.code(), 1690);
    assert_eq!(err.sql_state(), "22003");

    // A negative signed argument is also an out-of-range length.
    let err = engine.execute("SELECT RANDOM_BYTES(-5)").unwrap_err();
    assert_eq!(err.code(), 1690);
    assert_eq!(err.sql_state(), "22003");

    // len > 1024 → 1690.
    let err = engine.execute("SELECT RANDOM_BYTES(2000)").unwrap_err();
    assert_eq!(err.code(), 1690);
    assert_eq!(err.sql_state(), "22003");

    // Happy path.
    let row = one_row(&engine, "SELECT LENGTH(RANDOM_BYTES(16))");
    assert_eq!(row[0], Value::Int(16));
}

#[test]
fn invalid_ip_and_unhex_values_follow_dml_strictness() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE invalid_binary_value (v VARBINARY(16))",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "SET sql_mode = ''")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO invalid_binary_value VALUES \
             (INET6_ATON('1.2.256')), (INET_ATON('122.256')), (UNHEX('Z0'))",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "SET sql_mode = DEFAULT")
        .unwrap();
    for expression in [
        "INET6_ATON('1.2.256')",
        "INET_ATON('122.256')",
        "UNHEX('Z0')",
    ] {
        let err = engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO invalid_binary_value VALUES ({expression})"),
            )
            .unwrap_err();
        assert_eq!(err.code(), 1411, "{expression}");
        assert_eq!(err.sql_state(), "HY000", "{expression}");
    }
}

#[test]
fn regexp_instr_and_replace_reject_bad_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // pos < 1 rejected on REGEXP_INSTR.
    assert!(engine
        .execute("SELECT REGEXP_INSTR('abc', 'b', 0)")
        .is_err());
    assert!(engine
        .execute("SELECT REGEXP_INSTR('abc', 'b', -1)")
        .is_err());

    // occurrence < 1 rejected on REGEXP_INSTR.
    assert!(engine
        .execute("SELECT REGEXP_INSTR('abc', 'b', 1, -1)")
        .is_err());

    // return_option ∉ {0, 1} rejected.
    assert!(engine
        .execute("SELECT REGEXP_INSTR('abc', 'b', 1, 1, 2)")
        .is_err());

    // REGEXP_REPLACE occurrence < 0 rejected.
    assert!(engine
        .execute("SELECT REGEXP_REPLACE('aaa', 'a', 'X', 1, -1)")
        .is_err());

    // Sanity: occurrence == 0 still means "replace all".
    let row = one_row(&engine, "SELECT REGEXP_REPLACE('aaa', 'a', 'X', 1, 0)");
    assert_eq!(row[0], Value::String("XXX".to_string()));
}

#[test]
fn regexp_binary_operand_forces_case_sensitivity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Text defaults to CI under utf8mb4_0900_ai_ci.
    let row = one_row(&engine, "SELECT 'a' REGEXP 'A'");
    assert_eq!(row[0], Value::Int(1));

    // BINARY forces case sensitivity.
    let row = one_row(&engine, "SELECT _binary 'a' REGEXP 'A'");
    assert_eq!(row[0], Value::Int(0));

    // An explicit i flag overrides the binary default.
    let row = one_row(&engine, "SELECT REGEXP_LIKE(_binary 'a', 'A', 'i')");
    assert_eq!(row[0], Value::Int(1));
}

#[test]
fn regexp_posix_classes_follow_introduced_charset_and_binary_warns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT \
         _latin1 0xFF REGEXP _latin1 '[[:lower:]]' COLLATE latin1_bin, \
         _koi8r 0xFF REGEXP _koi8r '[[:upper:]]' COLLATE koi8r_bin, \
         _koi8r 0xF7 REGEXP _koi8r '[[:alpha:]]'",
    );
    assert_eq!(row, vec![Value::Int(1), Value::Int(1), Value::Int(1)]);

    let mut session = SessionContext::default();
    let response = engine
        .execute_with_session(&mut session, "SELECT NULL REGEXP BINARY NULL")
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Null);
    assert!(
        session
            .warnings
            .iter()
            .any(|warning| warning.code() == 1287),
        "BINARY expr deprecation must be visible even when the value is NULL"
    );
}

#[test]
fn if_aggregates_binary_result_type_before_branch_selection() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT HEX(IF(0, BINARY 'a', 'BBB')), HEX(IF(1, 'BBB', BINARY 'a'))",
    );
    assert_eq!(
        row,
        vec![
            Value::String("424242".to_string()),
            Value::String("424242".to_string()),
        ]
    );
}
