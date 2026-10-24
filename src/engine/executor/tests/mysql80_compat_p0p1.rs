//! MySQL 8.0 P0/P1 built-in function compatibility coverage.
//! Cover unary BINARY, windowed STDDEV/VAR/BIT_*/JSON_*AGG,
//! DEFAULT(col) expression contexts, PS_CURRENT_THREAD_ID, ICU_VERSION,
//! TIMESTAMP fractional precision, CAST YEAR/REAL/NCHAR, complete GROUP_CONCAT clauses,
//! COERCIBILITY inference, named CONVERT_TZ zones, AES mode matrix, FORMAT locale,
//! REGEXP default case/match_type, WEEK default_week_format, seeded RAND
//! my_rnd sequences, GTID_SUBSET, and GTID_SUBTRACT.
use super::support::query_one_row as one_row;
use super::*;

fn all_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    result.rows
}

/// Read a single row for session-sensitive SET SESSION tests using the same SessionContext throughout.
fn one_row_s(engine: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Value> {
    let QueryResponse::ResultSet(result) = engine.execute_with_session(s, sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    assert_eq!(result.rows.len(), 1, "expected single row for: {sql}");
    result.rows.into_iter().next().unwrap()
}

fn as_text(value: &Value) -> String {
    match value {
        Value::Json(b) => crate::engine::json::to_text(b),
        other => crate::storage::value_to_string(other),
    }
}

// ---------- P0: unary BINARY ----------

#[test]
fn unary_binary_forces_case_sensitive_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT 'a' = 'A', BINARY 'a' = 'A', BINARY 'a' = 'a'",
    );
    assert_eq!(row[0], Value::Int(1), "default collation is ci");
    assert_eq!(row[1], Value::Int(0), "BINARY forces byte comparison");
    assert_eq!(row[2], Value::Int(1));
}

#[test]
fn unary_binary_in_where_and_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ub (id INT PRIMARY KEY, s VARCHAR(10))")
        .unwrap();
    engine
        .execute("INSERT INTO ub VALUES (1,'abc'), (2,'ABC'), (3,'xyz')")
        .unwrap();

    let rows = all_rows(&engine, "SELECT id FROM ub WHERE BINARY s = 'ABC'");
    assert_eq!(rows, vec![vec![Value::Int(2)]]);

    // Case-insensitive comparison matches both rows.
    let rows = all_rows(&engine, "SELECT id FROM ub WHERE s = 'ABC' ORDER BY id");
    assert_eq!(rows.len(), 2);

    let rows = all_rows(&engine, "SELECT s FROM ub ORDER BY BINARY s");
    // Byte order: 'ABC' < 'abc' < 'xyz'.
    assert_eq!(as_text(&rows[0][0]), "ABC");
    assert_eq!(as_text(&rows[1][0]), "abc");
    assert_eq!(as_text(&rows[2][0]), "xyz");
}

#[test]
fn binary_data_type_positions_are_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Do not rewrite BINARY in DDL column types, CAST targets, or SHOW statements.
    engine
        .execute("CREATE TABLE bt (id INT PRIMARY KEY, b BINARY(4), v VARBINARY(8))")
        .unwrap();
    engine
        .execute("INSERT INTO bt VALUES (1, 'ab', 'cd')")
        .unwrap();
    let row = one_row(&engine, "SELECT CAST('a' AS BINARY) = 'a' FROM bt");
    assert_eq!(row[0], Value::Int(1));
}

// ---------- P0: windowed extended aggregates ----------

#[test]
fn window_stddev_variance_over_partition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE w (id INT PRIMARY KEY, g VARCHAR(2), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO w VALUES (1,'a',1),(2,'a',3),(3,'a',5),(4,'b',10),(5,'b',10)")
        .unwrap();

    // Full-partition frame without ORDER BY.
    let rows = all_rows(
        &engine,
        "SELECT g, VAR_POP(v) OVER (PARTITION BY g), STDDEV_SAMP(v) OVER (PARTITION BY g), \
                VARIANCE(v) OVER (PARTITION BY g) \
         FROM w ORDER BY id",
    );
    // group a: values 1,3,5 → pop var = 8/3, samp std = 2
    let var_pop_a = match rows[0][1] {
        Value::Double(d) => d,
        ref other => panic!("expected double, got {other:?}"),
    };
    assert!((var_pop_a - 8.0 / 3.0).abs() < 1e-9);
    let stddev_samp_a = match rows[0][2] {
        Value::Double(d) => d,
        ref other => panic!("expected double, got {other:?}"),
    };
    assert!((stddev_samp_a - 2.0).abs() < 1e-9);
    // group b: 10,10 → var 0
    assert_eq!(rows[3][1], Value::Double(0.0));

    // Running frame: ORDER BY implies RANGE UNBOUNDED PRECEDING through CURRENT ROW.
    let rows = all_rows(
        &engine,
        "SELECT id, STDDEV_POP(v) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id",
    );
    assert_eq!(rows[0][1], Value::Double(0.0), "single-row frame");
    let two = match rows[1][1] {
        Value::Double(d) => d,
        ref other => panic!("expected double, got {other:?}"),
    };
    assert!((two - 1.0).abs() < 1e-9, "1,3 → pop stddev 1");

    // STDDEV_SAMP over one row returns NULL because n<2.
    let rows = all_rows(
        &engine,
        "SELECT STDDEV_SAMP(v) OVER (PARTITION BY g ORDER BY id) FROM w WHERE g='a' ORDER BY id",
    );
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn window_bit_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wb (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO wb VALUES (1,1),(2,2),(3,4)")
        .unwrap();

    let rows = all_rows(
        &engine,
        "SELECT BIT_OR(v) OVER (ORDER BY id), BIT_AND(v) OVER (), BIT_XOR(v) OVER () \
         FROM wb ORDER BY id",
    );
    // Running BIT_OR values: 1,3,7.
    assert_eq!(rows[0][0], Value::UInt(1));
    assert_eq!(rows[1][0], Value::UInt(3));
    assert_eq!(rows[2][0], Value::UInt(7));
    // Full-frame BIT_AND(1,2,4)=0 and BIT_XOR=7.
    assert_eq!(rows[0][1], Value::UInt(0));
    assert_eq!(rows[0][2], Value::UInt(7));
}

#[test]
fn window_json_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wj (id INT PRIMARY KEY, k VARCHAR(4), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO wj VALUES (1,'a',10),(2,'b',20)")
        .unwrap();

    let rows = all_rows(
        &engine,
        "SELECT JSON_ARRAYAGG(v) OVER (ORDER BY id), JSON_OBJECTAGG(k, v) OVER () \
         FROM wj ORDER BY id",
    );
    assert_eq!(as_text(&rows[0][0]), "[10]");
    assert_eq!(as_text(&rows[1][0]), "[10, 20]");
    assert_eq!(as_text(&rows[0][1]), r#"{"a": 10, "b": 20}"#);
}

// ---------- P0: DEFAULT(col) ----------

#[test]
fn default_function_in_expression_contexts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE dt (id INT PRIMARY KEY, a INT DEFAULT 7, b VARCHAR(8) DEFAULT 'x', c INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO dt VALUES (1, 1, 'y', 5)")
        .unwrap();

    let row = one_row(&engine, "SELECT DEFAULT(a), DEFAULT(b) FROM dt");
    assert_eq!(row[0], Value::Int(7));
    assert_eq!(as_text(&row[1]), "x");

    // NOT NULL without a default yields 1364.
    let err = engine.execute("SELECT DEFAULT(c) FROM dt").unwrap_err();
    assert_eq!(err.code(), 1364, "{}", err.message());

    // UPDATE SET a = DEFAULT(a) + 1。
    engine
        .execute("UPDATE dt SET a = DEFAULT(a) + 1 WHERE id = 1")
        .unwrap();
    let row = one_row(&engine, "SELECT a FROM dt WHERE id = 1");
    assert_eq!(row[0], Value::Int(8));
}

// ---------- P0: three small fixes ----------

#[test]
fn ps_current_thread_id_icu_version_timestamp_fsp() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT PS_CURRENT_THREAD_ID(), ICU_VERSION(), \
                TIMESTAMP('2003-12-31 12:00:00.123'), \
                TIMESTAMP('2003-12-31 12:00:00'), \
                TIMESTAMP('2003-12-31 12:00:00.500', '1:00:00.250')",
    );
    assert!(matches!(row[0], Value::UInt(_)));
    assert_eq!(as_text(&row[1]), "77.1");
    assert_eq!(as_text(&row[2]), "2003-12-31 12:00:00.123");
    assert_eq!(as_text(&row[3]), "2003-12-31 12:00:00");
    // Typed values use canonical display; protocol formatting retains FSP.
    assert_eq!(as_text(&row[4]), "2003-12-31 13:00:00.75");
    let response = engine
        .execute("SELECT TIMESTAMP('2003-12-31 12:00:00.500','1:00:00.250')")
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set")
    };
    assert_eq!(result.columns[0].source.decimals, Some(3));
}

// ---------- P0: CAST AS YEAR / REAL / NCHAR ----------

#[test]
fn cast_year_real_nchar() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT CAST('2020-06-01' AS YEAR), CAST(69 AS YEAR), CAST(70 AS YEAR), \
                CAST(0 AS YEAR), CAST(2155 AS YEAR), \
                CAST(1.5 AS REAL), CAST('abc' AS NCHAR)",
    );
    // Post-B22: CAST AS YEAR returns typed Value::Year(byte),
    // byte = year-1900 (0 for the literal 0).
    assert_eq!(row[0], Value::Year(120)); // 2020
    assert_eq!(row[1], Value::Year(169)); // 2069
    assert_eq!(row[2], Value::Year(70)); // 1970
    assert_eq!(row[3], Value::Year(0)); // literal 0
    assert_eq!(row[4], Value::Year(255)); // 2155
    assert_eq!(row[5], Value::Double(1.5));
    assert_eq!(as_text(&row[6]), "abc");

    assert_eq!(
        one_row(&engine, "SELECT CAST(3000 AS YEAR)"),
        vec![Value::Null]
    );
}

// ---------- P0: GROUP_CONCAT completeness ----------

#[test]
fn group_concat_distinct_order_separator_and_max_len() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE gc (id INT PRIMARY KEY, v VARCHAR(8))")
        .unwrap();
    engine
        .execute("INSERT INTO gc VALUES (1,'b'),(2,'a'),(3,'b'),(4,'c')")
        .unwrap();

    let row = one_row(
        &engine,
        "SELECT GROUP_CONCAT(DISTINCT v ORDER BY v DESC SEPARATOR '|') FROM gc",
    );
    assert_eq!(as_text(&row[0]), "c|b|a");

    // group_concat_max_len truncation is session-local and requires one SessionContext throughout.
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET SESSION group_concat_max_len = 4")
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut s,
        "SELECT GROUP_CONCAT(v ORDER BY id) FROM gc",
    );
    assert_eq!(as_text(&row[0]).len(), 4, "truncated to max_len bytes");
}

// ---------- P1: COERCIBILITY ----------

#[test]
fn coercibility_levels() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE co (id INT PRIMARY KEY, s VARCHAR(8))")
        .unwrap();
    engine
        .execute("INSERT INTO co VALUES (1, 'x'), (2, NULL)")
        .unwrap();

    let row = one_row(
        &engine,
        "SELECT COERCIBILITY('abc' COLLATE utf8mb4_bin), COERCIBILITY(USER()), \
                COERCIBILITY('abc'), COERCIBILITY(1000), COERCIBILITY(NULL)",
    );
    assert_eq!(row[0], Value::Int(0));
    assert_eq!(row[1], Value::Int(3));
    assert_eq!(row[2], Value::Int(4));
    assert_eq!(row[3], Value::Int(5));
    assert_eq!(row[4], Value::Int(6));

    let row = one_row(&engine, "SELECT COERCIBILITY(s) FROM co WHERE id=1");
    assert_eq!(row[0], Value::Int(2), "column reference = implicit");

    let row = one_row(
        &engine,
        "SELECT CHARSET(s), COLLATION(s), COERCIBILITY(s), \
                CHARSET(id), COLLATION(id), COERCIBILITY(id) FROM co WHERE id=2",
    );
    assert_eq!(
        row,
        vec![
            Value::String("utf8mb4".into()),
            Value::String("utf8mb4_0900_ai_ci".into()),
            Value::Int(2),
            Value::String("binary".into()),
            Value::String("binary".into()),
            Value::Int(5),
        ],
        "stored NULL text retains column derivation; numeric fields are binary/level 5"
    );

    let row = one_row(
        &engine,
        "SELECT CHARSET(NULL), COLLATION(NULL), \
                COERCIBILITY(BIN(130)), COERCIBILITY(OCT(130)), \
                COERCIBILITY(CONV(130,16,10)), COERCIBILITY(HEX(130)), \
                COERCIBILITY(CHAR(130)), COERCIBILITY(FORMAT(130,10))",
    );
    assert_eq!(
        row,
        vec![
            Value::String("binary".into()),
            Value::String("binary".into()),
            Value::Int(4),
            Value::Int(4),
            Value::Int(4),
            Value::Int(4),
            Value::Int(4),
            Value::Int(4),
        ]
    );
}

// ---------- P1: CONVERT_TZ ----------

#[test]
fn convert_tz_offsets_and_named_zones() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00'), \
                CONVERT_TZ('2004-01-01 12:00:00.250','-05:00','+00:00')",
    );
    assert_eq!(as_text(&row[0]), "2004-01-01 22:00:00");
    // Observed MySQL behavior preserves input text fractional precision, .250.
    assert_eq!(as_text(&row[1]), "2004-01-01 17:00:00.250");

    // Named zones depend on host zoneinfo; check exact conversion when available.
    if crate::engine::tz::find_named_zone("Asia/Shanghai").is_some() {
        let row = one_row(
            &engine,
            "SELECT CONVERT_TZ('2004-01-01 12:00:00','UTC','Asia/Shanghai')",
        );
        assert_eq!(as_text(&row[0]), "2004-01-01 20:00:00");
    }
    if crate::engine::tz::find_named_zone("Europe/Berlin").is_some() {
        // Both sides of daylight-saving transitions.
        let row = one_row(
            &engine,
            "SELECT CONVERT_TZ('2023-07-01 00:00:00','UTC','Europe/Berlin'), \
                    CONVERT_TZ('2023-01-15 00:00:00','UTC','Europe/Berlin')",
        );
        assert_eq!(as_text(&row[0]), "2023-07-01 02:00:00", "CEST +02:00");
        assert_eq!(as_text(&row[1]), "2023-01-15 01:00:00", "CET +01:00");
    }

    assert_eq!(
        one_row(
            &engine,
            "SELECT CONVERT_TZ('2004-01-01 12:00:00','UTC','No/Such_Zone')"
        ),
        vec![Value::Null]
    );
}

// ---------- P1: AES mode matrix ----------

#[test]
fn aes_block_encryption_modes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Default aes-128-ecb round trip and observed MySQL ciphertext vector.
    let row = one_row(
        &engine,
        "SELECT AES_DECRYPT(AES_ENCRYPT('secret text', 'k'), 'k'), \
                HEX(AES_ENCRYPT('hello', 'key'))",
    );
    assert_eq!(as_text(&row[0]), "secret text");
    assert_eq!(as_text(&row[1]), "5E27050B59C889FEE9F953137259BF6B");

    let iv = "'0123456789abcdef'";
    let mut s = SessionContext::default();
    for mode in [
        "aes-128-cbc",
        "aes-192-cbc",
        "aes-256-cbc",
        "aes-128-cfb1",
        "aes-128-cfb8",
        "aes-128-cfb128",
        "aes-256-ofb",
        "aes-256-ctr",
    ] {
        engine
            .execute_with_session(
                &mut s,
                &format!("SET SESSION block_encryption_mode = '{mode}'"),
            )
            .unwrap();
        let row = one_row_s(
            &engine,
            &mut s,
            &format!(
                "SELECT AES_DECRYPT(AES_ENCRYPT('roundtrip payload', 'key', {iv}), 'key', {iv})"
            ),
        );
        assert_eq!(as_text(&row[0]), "roundtrip payload", "mode {mode}");
        // Non-ECB ciphertext must differ from plaintext.
        let row = one_row_s(
            &engine,
            &mut s,
            &format!("SELECT AES_ENCRYPT('roundtrip payload', 'key', {iv}) = 'roundtrip payload'"),
        );
        assert_eq!(row[0], Value::Int(0), "ciphertext differs ({mode})");
    }

    // Observed MySQL HEX(AES_ENCRYPT('hello','key',IV)) vectors must match bit-for-bit per mode.
    for (mode, expected) in [
        ("aes-128-cbc", "69A6F153AF542950DAA1C3BB5801EE3C"),
        ("aes-256-cbc", "8354F04A13EF146C1375E9D0ADFFB07B"),
        ("aes-128-ofb", "28E93AB0AB"),
        ("aes-128-cfb128", "28E93AB0AB"),
        ("aes-128-cfb8", "28E7E5B73C"),
        ("aes-128-cfb1", "06EF9297F1"),
    ] {
        engine
            .execute_with_session(
                &mut s,
                &format!("SET SESSION block_encryption_mode = '{mode}'"),
            )
            .unwrap();
        let row = one_row_s(
            &engine,
            &mut s,
            &format!("SELECT HEX(AES_ENCRYPT('hello', 'key', {iv}))"),
        );
        assert_eq!(as_text(&row[0]), expected, "MySQL vector for {mode}");
    }

    // Streaming modes retain plaintext length without padding; CBC pads.
    engine
        .execute_with_session(&mut s, "SET SESSION block_encryption_mode = 'aes-128-ctr'")
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut s,
        &format!("SELECT LENGTH(AES_ENCRYPT('12345', 'key', {iv}))"),
    );
    assert_eq!(row[0], Value::Int(5));
    engine
        .execute_with_session(&mut s, "SET SESSION block_encryption_mode = 'aes-128-cbc'")
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut s,
        &format!("SELECT LENGTH(AES_ENCRYPT('12345', 'key', {iv}))"),
    );
    assert_eq!(row[0], Value::Int(16));

    // Non-ECB without IV returns 1582.
    let err = engine
        .execute_with_session(&mut s, "SELECT AES_ENCRYPT('x', 'key')")
        .unwrap_err();
    assert_eq!(err.code(), 1582, "{}", err.message());
    // A short IV returns 1882.
    let err = engine
        .execute_with_session(&mut s, "SELECT AES_ENCRYPT('x', 'key', 'short')")
        .unwrap_err();
    assert_eq!(err.code(), 1882, "{}", err.message());

    // SET rejects invalid mode values.
    let err = engine
        .execute_with_session(&mut s, "SET SESSION block_encryption_mode = 'aes-512-gcm'")
        .unwrap_err();
    assert_eq!(err.code(), 1231, "{}", err.message());
}

// ---------- P1: FORMAT locale ----------

#[test]
fn format_with_locale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT FORMAT(12332.2, 2), FORMAT(12332.2, 2, 'de_DE'), \
                FORMAT(12332.2, 2, 'fr_FR'), FORMAT(1234567.89, 2, 'en_IN'), \
                FORMAT(1234567.89, 2, 'de_CH'), FORMAT(12332.2, 0)",
    );
    assert_eq!(as_text(&row[0]), "12,332.20");
    assert_eq!(as_text(&row[1]), "12.332,20");
    // Observed MySQL fr_FR formatting has no thousands separator.
    assert_eq!(as_text(&row[2]), "12332,20");
    assert_eq!(as_text(&row[3]), "12,34,567.89");
    assert_eq!(as_text(&row[4]), "1'234'567.89");
    assert_eq!(as_text(&row[5]), "12,332");
}

// ---------- P1: REGEXP default case and match_type ----------

#[test]
fn regexp_default_ci_and_match_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT REGEXP_LIKE('ABC', 'abc'), REGEXP_LIKE('ABC', 'abc', 'c'), \
                REGEXP_LIKE('ABC', 'abc', 'i'), 'ABC' REGEXP 'abc', \
                REGEXP_LIKE('a\nb', 'a.b', 'n'), REGEXP_LIKE('a\nb', 'a.b')",
    );
    assert_eq!(row[0], Value::Int(1), "default follows ci collation");
    assert_eq!(row[1], Value::Int(0), "'c' forces case-sensitive");
    assert_eq!(row[2], Value::Int(1));
    assert_eq!(row[3], Value::Int(1), "REGEXP operator ci by default");
    assert_eq!(row[4], Value::Int(1), "'n' lets . match newline");
    assert_eq!(row[5], Value::Int(0));
}

// ---------- P1: WEEK default_week_format ----------

#[test]
fn week_honours_default_week_format() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(&engine, "SELECT WEEK('2008-02-20'), WEEK('2008-02-20', 1)");
    assert_eq!(row[0], Value::Int(7), "mode 0");
    assert_eq!(row[1], Value::Int(8), "mode 1");

    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "SET SESSION default_week_format = 1")
        .unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT WEEK('2008-02-20')");
    assert_eq!(row[0], Value::Int(8), "1-arg WEEK uses default_week_format");
}

// ---------- P1: RAND(seed) ----------

#[test]
fn seeded_rand_matches_mysql_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // MySQL 8.0 documented/observable values for RAND(1) / RAND(3)。
    let row = one_row(&engine, "SELECT RAND(1), RAND(3), RAND(1)");
    let r1 = match row[0] {
        Value::Double(d) => d,
        ref other => panic!("expected double, got {other:?}"),
    };
    let r3 = match row[1] {
        Value::Double(d) => d,
        ref other => panic!("expected double, got {other:?}"),
    };
    assert!((r1 - 0.40540353712197724).abs() < 1e-12, "RAND(1) = {r1}");
    assert!((r3 - 0.9057697559760601).abs() < 1e-12, "RAND(3) = {r3}");
    assert_eq!(row[0], row[2], "same seed → same first draw");
}

#[test]
fn rand_seed_sysvars_and_seeded_continuation_match_mysql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET @@rand_seed1=10000000, @@rand_seed2=1000000",
        )
        .unwrap();
    assert_eq!(*session.sysvars.rand_state.lock(), (10_000_000, 1_000_000));

    let row = one_row_s(&engine, &mut session, "SELECT RAND(999999), RAND()");
    let values = row
        .iter()
        .map(|value| match value {
            Value::Double(value) => *value,
            other => panic!("expected double, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert!(
        (values[0] - 0.014231365187309091).abs() < 1e-15,
        "first RAND = {}",
        values[0]
    );
    assert!(
        (values[1] - 0.028870999839968048).abs() < 1e-15,
        "continued RAND = {}",
        values[1]
    );

    engine
        .execute_with_session(
            &mut session,
            "SET @@rand_seed1=10000000, @@rand_seed2=1000000",
        )
        .unwrap();
    let first = one_row_s(&engine, &mut session, "SELECT RAND()");
    engine
        .execute_with_session(
            &mut session,
            "SET @@rand_seed1=10000000, @@rand_seed2=1000000",
        )
        .unwrap();
    let replay = one_row_s(&engine, &mut session, "SELECT RAND()");
    assert_eq!(
        first, replay,
        "resetting both session seeds must replay RAND()"
    );
}

#[test]
fn seeded_rand_state_is_scoped_per_expression_occurrence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE rand_rows(a INT)").unwrap();
    engine
        .execute("INSERT INTO rand_rows VALUES (1),(1),(1),(2)")
        .unwrap();

    let rows = all_rows(
        &engine,
        "SELECT CAST(RAND(2) * 1000 AS UNSIGNED), \
                CAST(RAND(a) * 1000 AS UNSIGNED) FROM rand_rows",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::UInt(656), Value::UInt(405)],
            vec![Value::UInt(122), Value::UInt(405)],
            vec![Value::UInt(645), Value::UInt(405)],
            vec![Value::UInt(858), Value::UInt(656)],
        ]
    );
}

#[test]
fn seeded_rand_advances_across_correlated_lateral_rebinding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE rand_outer(i INT)").unwrap();
    engine
        .execute("INSERT INTO rand_outer VALUES (1),(2),(3)")
        .unwrap();

    let rows = all_rows(
        &engine,
        "SELECT r FROM rand_outer AS o \
         LEFT JOIN LATERAL (SELECT o.i, RAND(0) AS r) AS dt ON TRUE \
         ORDER BY o.i",
    );
    let values: Vec<f64> = rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Double(value)) => *value,
            other => panic!("expected RAND double, got {other:?}"),
        })
        .collect();
    assert_eq!(values.len(), 3);
    assert!((values[0] - 0.15522042769493574).abs() < 1e-12);
    assert!((values[1] - 0.620881741513388).abs() < 1e-12);
    assert!((values[2] - 0.6387474552157777).abs() < 1e-12);
}

#[test]
fn constant_do_evaluates_lazy_make_set_once() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "DO NULLIF(RTRIM(MAKE_SET(CAST('%S' AS UNSIGNED), POINT(0xAF, ''))), '')",
        )
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .filter(|warning| warning.code() == 1292)
            .count(),
        1,
        "the selected mask expression warns once and the unselected member is not evaluated: {:?}",
        session.warnings
    );
}

#[test]
fn enum_numeric_context_uses_declaration_ordinal_and_survives_ctas() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE enum_num(e ENUM('a', '', 'b'))")
        .unwrap();
    engine.execute("INSERT INTO enum_num VALUES ('b')").unwrap();
    engine.execute("INSERT INTO enum_num VALUES ('')").unwrap();
    engine
        .execute("INSERT IGNORE INTO enum_num VALUES (0)")
        .unwrap();
    engine.execute("INSERT INTO enum_num VALUES ('')").unwrap();

    let enum_rows = all_rows(&engine, "SELECT e + 0, e FROM enum_num");
    assert_eq!(
        enum_rows,
        vec![
            vec![Value::Int(3), Value::String("b".into())],
            vec![Value::Int(2), Value::String(String::new())],
            vec![Value::Int(0), Value::Bytes(Vec::new())],
            vec![Value::Int(2), Value::String(String::new())],
        ]
    );
    assert!(
        matches!(&enum_rows[1][1], Value::SmallString(bytes) if bytes.is_empty())
            && matches!(&enum_rows[3][1], Value::SmallString(bytes) if bytes.is_empty()),
        "declared empty ENUM members must keep stable storage identity: {enum_rows:?}"
    );
    let mut grouped = all_rows(&engine, "SELECT e + 0, COUNT(*) FROM enum_num GROUP BY e");
    grouped.sort_by_key(|row| match row.first() {
        Some(Value::Int(value)) => *value,
        _ => i64::MAX,
    });
    assert_eq!(
        grouped,
        vec![
            vec![Value::Int(0), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
            vec![Value::Int(3), Value::Int(1)],
        ]
    );
    engine
        .execute("CREATE TABLE enum_num_copy SELECT * FROM enum_num")
        .unwrap();
    assert_eq!(
        all_rows(&engine, "SELECT e + 0 FROM enum_num_copy"),
        vec![
            vec![Value::Int(3)],
            vec![Value::Int(2)],
            vec![Value::Int(0)],
            vec![Value::Int(2)],
        ]
    );
}

// ---------- P1: GTID set operations ----------

#[test]
fn gtid_subset_and_subtract() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let u = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
    let ul = u.to_ascii_lowercase();

    let row = one_row(
        &engine,
        &format!(
            "SELECT GTID_SUBSET('{u}:23', '{u}:21-57'), \
                    GTID_SUBSET('{u}:23-25', '{u}:23'), \
                    GTID_SUBSET('', '{u}:1-5'), \
                    GTID_SUBTRACT('{u}:21-57', '{u}:21-25'), \
                    GTID_SUBTRACT('{u}:21-57', '{u}:30-35'), \
                    GTID_SUBTRACT('{u}:21-57', '{u}:21-57')"
        ),
    );
    assert_eq!(row[0], Value::Int(1));
    assert_eq!(row[1], Value::Int(0));
    assert_eq!(row[2], Value::Int(1), "empty set is a subset of anything");
    assert_eq!(as_text(&row[3]), format!("{ul}:26-57"));
    assert_eq!(as_text(&row[4]), format!("{ul}:21-29:36-57"));
    assert_eq!(as_text(&row[5]), "");

    // Multiple UUIDs, interval merging, and singleton rendering.
    let row = one_row(
        &engine,
        &format!(
            "SELECT GTID_SUBTRACT('{u}:1-5:7,11111111-2222-3333-4444-555555555555:1-3', '{u}:2-4')"
        ),
    );
    // Observed MySQL output joins UUIDs with comma-newline.
    assert_eq!(
        as_text(&row[0]),
        format!("11111111-2222-3333-4444-555555555555:1-3,\n{ul}:1:5:7")
    );

    let err = engine
        .execute("SELECT GTID_SUBSET('not-a-gtid', 'x')")
        .unwrap_err();
    assert_eq!(err.code(), 1772, "{}", err.message());
    let err = engine
        .execute(&format!("SELECT GTID_SUBTRACT('{u}:5-3', '{u}:1')"))
        .unwrap_err();
    assert_eq!(err.code(), 1772, "reversed interval is malformed");
}

// ---------- issue #86: `@var := expr` user-variable assignment ----------

#[test]
fn user_variable_assignment_in_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    // Assignment returns the assigned value (not NULL) and persists it.
    let row = one_row_s(&engine, &mut s, "SELECT @z := 5 AS a");
    assert_eq!(row[0].to_display_string(), "5");
    // Read-your-writes within one statement, left-to-right.
    let row = one_row_s(&engine, &mut s, "SELECT @z := 5 AS a, @z + 1 AS b");
    assert_eq!(row[0].to_display_string(), "5");
    assert_eq!(row[1].to_display_string(), "6");
    // Cross-statement persistence.
    engine.execute_with_session(&mut s, "SET @c = 0").unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT @c := @c + 1 AS r");
    assert_eq!(row[0].to_display_string(), "1");
    let row = one_row_s(&engine, &mut s, "SELECT @c AS r");
    assert_eq!(row[0].to_display_string(), "1");

    // Chained (right-associative) assignment: all variables get the value.
    let row = one_row_s(&engine, &mut s, "SELECT @a := @b := 7 AS chained");
    assert_eq!(row[0].to_display_string(), "7");
    let row = one_row_s(&engine, &mut s, "SELECT @a AS a, @b AS b");
    assert_eq!(row[0].to_display_string(), "7");
    assert_eq!(row[1].to_display_string(), "7");

    // Running total across rows (classic idiom) evaluates in row order.
    engine
        .execute_with_session(&mut s, "CREATE TABLE t (v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO t VALUES (10),(20),(30)")
        .unwrap();
    engine.execute_with_session(&mut s, "SET @sum = 0").unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(
            &mut s,
            "SELECT v, @sum := @sum + v AS running FROM t ORDER BY v",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let running: Vec<String> = rs.rows.iter().map(|r| r[1].to_display_string()).collect();
    assert_eq!(running, vec!["10", "30", "60"]);

    // Assignment as a side effect inside WHERE.
    engine.execute_with_session(&mut s, "SET @x = 0").unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SELECT v FROM t WHERE (@x := v) > 15 ORDER BY v")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let vs: Vec<String> = rs.rows.iter().map(|r| r[0].to_display_string()).collect();
    assert_eq!(vs, vec!["20", "30"]);

    // `SET @v := expr` (the SET-clause form) still works.
    engine.execute_with_session(&mut s, "SET @y := 42").unwrap();
    let row = one_row_s(&engine, &mut s, "SELECT @y AS y");
    assert_eq!(row[0].to_display_string(), "42");

    // A `:=` inside a string literal is untouched; `=` stays a comparison.
    let row = one_row_s(&engine, &mut s, "SELECT ':=' AS lit, 5 = 5 AS eq");
    assert_eq!(row[0].to_display_string(), ":=");
    assert_eq!(row[1], Value::Int(1));
}

#[test]
fn derived_count_preserves_user_variable_projection_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t(v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1),(2),(3)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET @n := 0")
        .unwrap();

    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM (SELECT @n := @n + 1 AS n FROM t AS a, t AS b) AS d",
    );
    assert_eq!(row[0], Value::Int(9));
    let row = one_row_s(&engine, &mut session, "SELECT @n");
    assert_eq!(row[0], Value::Int(9));

    engine
        .execute_with_session(&mut session, "SET @n := 1, @s := ''")
        .unwrap();
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM \
         (SELECT @s := CONCAT(@s, @n := @n + 1, ',') AS s \
          FROM t AS a, t AS b) AS d",
    );
    assert_eq!(row[0], Value::Int(9));
    let row = one_row_s(&engine, &mut session, "SELECT @s");
    assert!(
        row[0].to_display_string().starts_with("2,3,4,"),
        "nested assignments must evaluate once and left-to-right: {:?}",
        row[0]
    );
}

#[test]
fn benchmark_reexecutes_volatile_scalar_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET @n = 0")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SELECT BENCHMARK(3, (SELECT @n := @n + 1 + RAND() * 0))",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT @n")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Double(3.0)]]);
}

// ---------- 2026-07 numeric review: HEX / UNHEX dispatch ----------

#[test]
fn hex_dispatches_by_argument_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // String argument → byte-wise hex (each source byte becomes two hex
    // chars). `HEX('123')` used to be parsed as the integer 123 and
    // returned "7B"; MySQL 8.0 returns "313233".
    let row = one_row(&engine, "SELECT HEX('123'), HEX('42'), HEX(''), HEX('abc')");
    assert_eq!(as_text(&row[0]), "313233");
    assert_eq!(as_text(&row[1]), "3432");
    assert_eq!(as_text(&row[2]), "");
    assert_eq!(as_text(&row[3]), "616263");

    // Numeric argument → CONV(N, 10, 16). Fractional values round
    // half-away-from-zero first: HEX(1.5) = "2", not the byte pattern
    // of the string "1.5". Negative integers use two's-complement.
    let row = one_row(
        &engine,
        "SELECT HEX(255), HEX(1.5), HEX(-1), HEX(CAST(15.7 AS DOUBLE))",
    );
    assert_eq!(as_text(&row[0]), "FF");
    assert_eq!(as_text(&row[1]), "2");
    assert_eq!(as_text(&row[2]), "FFFFFFFFFFFFFFFF");
    assert_eq!(as_text(&row[3]), "10");
}

#[test]
fn unhex_odd_length_left_pads_and_rejects_bad_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Odd length: MySQL left-pads with '0'. `HEX(UNHEX('AAA'))` therefore
    // round-trips to '0AAA', not 'AA0A'.
    let row = one_row(
        &engine,
        "SELECT HEX(UNHEX('AAA')), HEX(UNHEX('A')), HEX(UNHEX('ABCD'))",
    );
    assert_eq!(as_text(&row[0]), "0AAA");
    assert_eq!(as_text(&row[1]), "0A");
    assert_eq!(as_text(&row[2]), "ABCD");

    // Any non-hex character (including multibyte UTF-8 characters that
    // previously panicked on the string-slice boundary) yields NULL.
    let row = one_row(&engine, "SELECT UNHEX('aé'), UNHEX('nothex'), UNHEX('a€b')");
    assert_eq!(row[0], Value::Null);
    assert_eq!(row[1], Value::Null);
    assert_eq!(row[2], Value::Null);
}

#[test]
fn crc32_hashes_raw_bytes_not_lossy_utf8() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Known vector: CRC32("abc") == 0x352441C2 == 891568578.
    let row = one_row(&engine, "SELECT CRC32('abc'), CRC32('')");
    assert_eq!(row[0], Value::UInt(891_568_578));
    assert_eq!(row[1], Value::UInt(0));

    // `HEX(UNHEX('FF')) = 'FF'` proves the byte is stored as 0xFF; hashing
    // that single-byte value must not go through UTF-8 lossy replacement
    // (which would produce a 3-byte 0xEF 0xBF 0xBD input to the hasher).
    // Expected: crc32([0xFF]) = 0xFF000000 = 4278190080.
    let row = one_row(&engine, "SELECT CRC32(UNHEX('FF'))");
    assert_eq!(row[0], Value::UInt(4_278_190_080));

    engine
        .execute("CREATE TABLE crc_source (a VARCHAR(8))")
        .unwrap();
    engine
        .execute("CREATE TABLE crc_result AS SELECT CRC32(a) AS checksum FROM crc_source")
        .unwrap();
    let QueryResponse::ResultSet(description) = engine.execute("DESCRIBE crc_result").unwrap()
    else {
        panic!("expected DESCRIBE result set");
    };
    assert_eq!(description.rows[0][1], Value::String("int unsigned".into()));
}

// ---------- 2026-07 numeric review: RAND state ----------

#[test]
fn rand_null_is_treated_as_no_seed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // MySQL 8.0: RAND(NULL) is documented as *not* a seed → equivalent
    // to unseeded RAND(). Two calls in the same query produce distinct
    // doubles (with astronomical probability).
    let row = one_row(&engine, "SELECT RAND(NULL), RAND(NULL)");
    let (a, b) = match (&row[0], &row[1]) {
        (Value::Double(a), Value::Double(b)) => (*a, *b),
        other => panic!("expected doubles, got {other:?}"),
    };
    assert!((0.0..1.0).contains(&a), "RAND(NULL) in [0,1): {a}");
    assert!((0.0..1.0).contains(&b), "RAND(NULL) in [0,1): {b}");
}

#[test]
fn seeded_rand_advances_lcg_across_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t3 (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t3 VALUES (1),(2),(3)").unwrap();

    // MySQL 8.0: `SELECT RAND(5) FROM t3` produces three distinct values,
    // one per row along the deterministic sequence seeded with 5.
    let rows = all_rows(&engine, "SELECT RAND(5) FROM t3 ORDER BY id");
    let vals: Vec<f64> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Double(d) => *d,
            other => panic!("expected double, got {other:?}"),
        })
        .collect();
    assert_eq!(vals.len(), 3);
    // First draw for seed=5 uses MySQL's `my_rnd_init(5, ...)`. The
    // exact value comes from the LCG; verifying pairwise inequality is
    // enough to prove the state now advances rather than replaying
    // draw #1 each row.
    assert!(vals[0] != vals[1] && vals[1] != vals[2] && vals[0] != vals[2]);
    assert!(vals.iter().all(|v| (0.0..1.0).contains(v)));

    // The state is per-statement: re-running the same query returns the
    // same triple. This is the whole point of seeded RAND being
    // repeatable.
    let rows2 = all_rows(&engine, "SELECT RAND(5) FROM t3 ORDER BY id");
    let vals2: Vec<f64> = rows2
        .iter()
        .map(|r| match &r[0] {
            Value::Double(d) => *d,
            other => panic!("expected double, got {other:?}"),
        })
        .collect();
    assert_eq!(vals, vals2, "seeded RAND must be repeatable per statement");
}

// ---------- 2026-07 numeric review: FORMAT clamp + locale warning ----------

#[test]
fn format_clamps_decimals_and_warns_on_unknown_locale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();

    // D > 30 is clamped to 30 rather than raising 1690 out-of-range.
    let row = one_row_s(&engine, &mut s, "SELECT FORMAT(1.5, 40)");
    assert_eq!(
        as_text(&row[0]),
        "1.500000000000000000000000000000",
        "D clamped to 30"
    );

    // Negative D is treated as 0.
    let row = one_row_s(&engine, &mut s, "SELECT FORMAT(123.456, -5)");
    assert_eq!(as_text(&row[0]), "123");

    // Unknown locale: warning 1649, en_US formatting.
    let row = one_row_s(&engine, &mut s, "SELECT FORMAT(12332.2, 2, 'xx_XX')");
    assert_eq!(as_text(&row[0]), "12,332.20");
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut s, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        rs.rows
            .iter()
            .any(|r| matches!(&r[1], Value::UInt(1649) | Value::Int(1649))),
        "expected warning 1649 for unknown locale, got {:?}",
        rs.rows
    );
}

// ---------- 2026-07 numeric review: CONV / BIN / OCT ----------

#[test]
fn conv_accepts_negative_from_base_as_signed_domain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // MySQL 8.0 accepts a negative `from_base`; for numeric operands it
    // selects the signed BIGINT input domain. A negative `to_base` treats
    // the converted value as signed on the output side.
    let row = one_row(
        &engine,
        "SELECT CONV('10', -10, 16), CONV('10', 10, -16), CONV('-1', 10, -16)",
    );
    assert_eq!(as_text(&row[0]), "A");
    // to_base = -16 → N interpreted as signed: '10' = 10 → "A", '-1'
    // stays negative → "-1".
    assert_eq!(as_text(&row[1]), "A");
    assert_eq!(as_text(&row[2]), "-1");
}

#[test]
fn bin_and_oct_truncate_fractional_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // BIN/OCT/CONV truncate; HEX still rounds.
    let row = one_row(
        &engine,
        "SELECT BIN(1.5), BIN(1.4), OCT(7.5), OCT(7.4), BIN(-1)",
    );
    assert_eq!(as_text(&row[0]), "1", "BIN(1.5) truncates to 1");
    assert_eq!(as_text(&row[1]), "1");
    assert_eq!(as_text(&row[2]), "7", "OCT(7.5) truncates to 7");
    assert_eq!(as_text(&row[3]), "7");
    // BIN(-1) treats N as u64: 2^64 - 1 = 64 ones.
    assert_eq!(
        as_text(&row[4]),
        "1111111111111111111111111111111111111111111111111111111111111111"
    );
}

// ---------- 2026-07 numeric review: bitwise DECIMAL precision ----------

#[test]
fn bitwise_ops_preserve_decimal_precision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // A DECIMAL(20,0) magnitude near i64::MAX must survive a bitwise
    // op without being folded through f64 (which would drop the low
    // bits and saturate to i64::MAX). Below: 9223372036854775800 | 0
    // must return the argument unchanged; the f64 path returned
    // 9223372036854775807.
    let row = one_row(
        &engine,
        "SELECT CAST(9223372036854775800 AS DECIMAL(20,0)) | 0",
    );
    assert_eq!(row[0], Value::UInt(9_223_372_036_854_775_800));
}

// ---------- 2026-07 numeric review: CAST scientific notation ----------

#[test]
fn cast_string_as_decimal_supports_scientific_notation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // "1e6" is exactly representable as DECIMAL(10,0). The old code
    // fell through to f64 for any exponent form and lost precision for
    // large exponents.
    let row = one_row(
        &engine,
        "SELECT CAST('1e6' AS DECIMAL(10,0)), CAST('1.5e2' AS DECIMAL(10,2)), \
                CAST('1e30' AS DECIMAL(38,0))",
    );
    // 1e6 = 1_000_000, scale 0.
    assert_eq!(as_text(&row[0]), "1000000");
    // 1.5e2 = 150.00, scale 2.
    assert_eq!(as_text(&row[1]), "150.00");
    // 1e30 fits in i128 (~1.7e38); no precision loss.
    assert_eq!(as_text(&row[2]), "1000000000000000000000000000000");
}

// ---------- 2026-07 numeric review: ROUND d parameter saturation ----------

#[test]
fn round_extreme_d_argument_saturates_not_wraps() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // 2^33 is beyond i32 range. Both legacy and expr_program paths must
    // saturate to i32::MAX (no-op on any finite value), not wrap into
    // negative and zero the result.
    let row = one_row(
        &engine,
        "SELECT ROUND(1.5, 8589934592), TRUNCATE(1.5, 8589934592)",
    );
    // ROUND / TRUNCATE with |d| >= scale leaves the value unchanged.
    assert_eq!(as_text(&row[0]), "1.5");
    assert_eq!(as_text(&row[1]), "1.5");

    // Large negative d saturates to i32::MIN → zeros out the value.
    let row = one_row(&engine, "SELECT ROUND(12345, -8589934592)");
    assert_eq!(row[0], Value::Int(0));
}
