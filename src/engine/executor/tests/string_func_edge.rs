//! String function boundary regressions; oracle: local MySQL 9.6 on 2026-06-12.
//! Negative LPAD/RPAD target lengths return NULL; previously casting -1 to usize
//! produced usize::MAX and attempted a huge allocation.
//! Negative REPEAT/SPACE counts return empty strings.
//! INSERT with pos<1 or pos>CHAR_LENGTH preserves the original;
//! negative len replaces through the end.
//! Results exceeding max_allowed_packet return NULL with warning 1301.
use super::support::query_one_row as one_row;
use super::*;

fn one_row_s(engine: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Value> {
    let QueryResponse::ResultSet(result) = engine.execute_with_session(s, sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    assert_eq!(result.rows.len(), 1, "expected single row for: {sql}");
    result.rows.into_iter().next().unwrap()
}

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

#[test]
fn pad_negative_length_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT LPAD('hi',-1,'?'), RPAD('hi',-1,'?'), LPAD('hi',0,'?'), RPAD('hi',0,'?')",
    );
    assert_eq!(row[0], Value::Null, "LPAD negative len is NULL");
    assert_eq!(row[1], Value::Null, "RPAD negative len is NULL");
    assert_eq!(row[2], s(""), "LPAD zero len truncates to empty");
    assert_eq!(row[3], s(""), "RPAD zero len truncates to empty");
}

#[test]
fn pad_counts_characters_not_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT LPAD('日本語',2,'+'), LPAD('ab',5,'éx'), RPAD('ab',5,'éx'), LPAD('hi',5,'')",
    );
    assert_eq!(row[0], s("日本"), "truncation is by chars");
    assert_eq!(row[1], s("éxéab"), "pad cycles by chars");
    assert_eq!(row[2], s("abéxé"));
    assert_eq!(row[3], s(""), "empty pad cannot extend the input");
}

#[test]
fn repeat_and_space_negative_yield_empty() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(&engine, "SELECT REPEAT('x',-5), SPACE(-3), REPEAT('x',0)");
    assert_eq!(row[0], s(""));
    assert_eq!(row[1], s(""));
    assert_eq!(row[2], s(""));
}

#[test]
fn insert_position_and_negative_length_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT INSERT('hello',5,2,'X'), INSERT('hello',6,2,'X'), \
         INSERT('hello',0,2,'X'), INSERT('hello',-1,2,'X'), INSERT('hello',2,-1,'X')",
    );
    assert_eq!(row[0], s("hellX"), "pos = CHAR_LENGTH is in range");
    assert_eq!(row[1], s("hello"), "pos past end returns original");
    assert_eq!(row[2], s("hello"), "pos 0 returns original");
    assert_eq!(row[3], s("hello"), "negative pos returns original");
    assert_eq!(row[4], s("hX"), "negative len replaces through end");
}

/// Column-driven compiled kernels match legacy literal/constant-folded semantics.
/// Cover new P2 string kernels LTRIM/RTRIM/REPLACE/LOCATE/INSTR and related functions.
/// LPAD/RPAD/REPEAT/SPACE/REVERSE/ASCII/ORD/STRCMP。
#[test]
fn compiled_string_kernels_match_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE k (id INT PRIMARY KEY, s VARCHAR(30), n INT)")
        .unwrap();
    engine
        .execute("INSERT INTO k VALUES (1, '  hi  ', 3), (2, 'AbC', -1)")
        .unwrap();

    let row = one_row(
        &engine,
        "SELECT LTRIM(s), RTRIM(s), REPLACE(s,'h','H'), LOCATE('I', s), INSTR(s, 'I'), \
         LPAD(s, n, '*'), REVERSE(s), ASCII(s), ORD(s), STRCMP(s, '  HI  ') \
         FROM k WHERE id = 1",
    );
    assert_eq!(row[0], s("hi  "));
    assert_eq!(row[1], s("  hi"));
    assert_eq!(row[2], s("  Hi  "));
    assert_eq!(row[3], Value::Int(4), "LOCATE follows default ci collation");
    assert_eq!(row[4], Value::Int(4), "INSTR is haystack-first");
    assert_eq!(row[5], s("  h"), "LPAD truncates to target chars");
    assert_eq!(row[6], s("  ih  "));
    assert_eq!(row[7], Value::Int(32), "first byte is the space");
    assert_eq!(row[8], Value::Int(32));
    assert_eq!(row[9], Value::Int(0), "STRCMP ci equal");

    let row = one_row(
        &engine,
        "SELECT LPAD(s, n, '*'), RPAD(s, n, '*'), REPEAT(s, n), SPACE(n), \
         RPAD(s, 5, '*'), REPEAT(s, 2), STRCMP(BINARY s, 'abc') \
         FROM k WHERE id = 2",
    );
    assert_eq!(
        row[0],
        Value::Null,
        "negative pad length is NULL (kernel path)"
    );
    assert_eq!(row[1], Value::Null);
    assert_eq!(row[2], s(""), "negative repeat count is empty");
    assert_eq!(row[3], s(""));
    assert_eq!(row[4], s("AbC**"));
    assert_eq!(row[5], s("AbCAbC"));
    assert_eq!(row[6], Value::Int(-1), "binary STRCMP: 'AbC' < 'abc'");
}

#[test]
fn oversize_string_result_is_null_with_warning_1301() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET GLOBAL max_allowed_packet = 1024")
        .unwrap();
    let QueryResponse::ResultSet(set_warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected SET warning result set");
    };
    assert!(set_warnings.rows.iter().any(|row| {
        row.get(1) == Some(&Value::Int(1708))
            && row
                .get(2)
                .is_some_and(|value| value.to_display_string().contains("net_buffer_length"))
    }));

    let row = one_row_s(&engine, &mut session, "SELECT REPEAT('x', 2048)");
    assert_eq!(row[0], Value::Null, "over-cap REPEAT is NULL");
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected SHOW WARNINGS result set");
    };
    assert!(
        warnings.rows.iter().any(|row| row[1] == Value::Int(1301)
            && crate::storage::value_to_string(&row[2]).contains("max_allowed_packet")),
        "expected warning 1301, got {:?}",
        warnings.rows
    );

    // Reject huge lengths before allocation, including multiplication overflow,
    // without depending on actual materialization.
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT LPAD('x', 99999999999, 'y'), RPAD('x', 99999999999, 'y'), \
         SPACE(99999999999), REPEAT('xy', 4611686018427387904)",
    );
    assert_eq!(row[0], Value::Null);
    assert_eq!(row[1], Value::Null);
    assert_eq!(row[2], Value::Null);
    assert_eq!(row[3], Value::Null);

    // CONCAT/CONCAT_WS obey the same limit.
    let row = one_row_s(
        &engine,
        &mut session,
        "SELECT CONCAT(REPEAT('x',600), REPEAT('y',600)), \
         CONCAT_WS(REPEAT('-',600), 'a', REPEAT('z',600))",
    );
    assert_eq!(row[0], Value::Null);
    assert_eq!(row[1], Value::Null);

    // Within-limit results remain unaffected.
    let row = one_row_s(&engine, &mut session, "SELECT LENGTH(REPEAT('x', 1024))");
    assert_eq!(row[0], Value::Int(1024));
}

// TRIM with a multi-character remstr strips it as a *substring* from the
// specified end(s), matching MySQL 8.0 — NOT as a character set.
#[test]
fn trim_multichar_remstr_is_substring_not_charset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Only the trailing 'xy' substring is removed; the leading 'yx' is not a
    // prefix match, so it stays. A char-set impl would wrongly yield 'Content'.
    let row = one_row(
        &engine,
        "SELECT TRIM('xy' FROM 'yxContentxy'), \
                TRIM(LEADING 'ab' FROM 'ababXabab'), \
                TRIM(TRAILING 'ab' FROM 'ababXabab'), \
                TRIM(BOTH 'ab' FROM 'ababXabab')",
    );
    assert_eq!(row[0], s("yxContent"));
    assert_eq!(row[1], s("Xabab"));
    assert_eq!(row[2], s("ababX"));
    assert_eq!(row[3], s("X"));
}

#[test]
fn trim_default_and_empty_remstr() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Default trims spaces; empty remstr trims nothing.
    let row = one_row(
        &engine,
        "SELECT TRIM('  hi  '), TRIM('' FROM 'zz'), TRIM('z' FROM 'zzhizz')",
    );
    assert_eq!(row[0], s("hi"));
    assert_eq!(row[1], s("zz"));
    assert_eq!(row[2], s("hi"));
}

#[test]
fn ltrim_rtrim_strip_only_ascii_space() {
    // MySQL LTRIM/RTRIM removes only ASCII spaces 0x20, not tabs/newlines or other whitespace.
    // Rust trim_start/trim_end used char::is_whitespace, removing too much
    // and conflicting with our TRIM behavior. Oracle: MySQL 8.0.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Preserve tabs/newlines and remove spaces only.
    let row = one_row(
        &engine,
        "SELECT LTRIM('\t x'), RTRIM('x \t'), LTRIM('  a'), RTRIM('a  '), \
                LTRIM('\nx'), RTRIM('x\n')",
    );
    assert_eq!(row[0], s("\t x"), "LTRIM preserves leading tabs");
    assert_eq!(row[1], s("x \t"), "RTRIM preserves trailing tabs");
    assert_eq!(row[2], s("a"), "LTRIM removes leading spaces");
    assert_eq!(row[3], s("a"), "RTRIM removes trailing spaces");
    assert_eq!(row[4], s("\nx"), "LTRIM preserves leading newlines");
    assert_eq!(row[5], s("x\n"), "RTRIM preserves trailing newlines");

    // Column forms force compiled row execution and must match literals.
    engine
        .execute("CREATE TABLE ts (id INT PRIMARY KEY, v VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO ts VALUES (1, '\t x'), (2, '  a  ')")
        .unwrap();
    assert_eq!(
        one_row(&engine, "SELECT LTRIM(v) FROM ts WHERE id=1")[0],
        s("\t x")
    );
    assert_eq!(
        one_row(&engine, "SELECT RTRIM(LTRIM(v)) FROM ts WHERE id=2")[0],
        s("a")
    );
}

#[test]
fn text_string_functions_reject_invalid_binary_inputs_in_both_evaluators() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let err = engine.execute("SELECT QUOTE(x'80')").unwrap_err();
    assert_eq!(err.code(), 3854, "{}", err.message());
    let err = engine.execute("SELECT QUOTE(_utf8mb4 x'80')").unwrap_err();
    assert_eq!(err.code(), 1300, "{}", err.message());

    engine
        .execute("CREATE TABLE compat_s(c VARCHAR(10))")
        .unwrap();
    engine
        .execute("INSERT INTO compat_s VALUES ('Alaska')")
        .unwrap();
    engine
        .execute("CREATE TABLE compat_s_empty(c VARCHAR(10))")
        .unwrap();
    let err = engine
        .execute("SELECT LPAD(c, 3, x'80') FROM compat_s_empty")
        .unwrap_err();
    assert_eq!(err.code(), 3854, "empty input: {}", err.message());
    for sql in [
        "SELECT LPAD('', 8, REPLACE(COMPRESS(42), c, '')) FROM compat_s",
        "SELECT LPAD('', 8, SUBSTRING_INDEX(COMPRESS(42), c, 1)) FROM compat_s",
        "SELECT LPAD('', 8, TRIM(BOTH c FROM x'ff')) FROM compat_s",
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 3854, "{sql}: {}", err.message());
    }
}

#[test]
fn explicit_charset_conversion_is_lossy_but_introducers_are_strict() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(&engine, "SELECT CONVERT('你好' USING latin1), SHA2('x', 1)");
    assert_eq!(row, vec![s("??"), Value::Null]);
}

#[test]
fn nested_numeric_and_json_errors_are_not_hidden_by_outer_functions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    for (sql, code) in [
        (
            "SELECT UNCOMPRESS(BIN(POW(-9223372036854775807, 0xa5)))",
            1690,
        ),
        ("SELECT JSON_TYPE(BIN(EXP(0x5f022b6c81)))", 1690),
        ("SELECT BIN(IFNULL(JSON_LENGTH(5), 0))", 3146),
        (
            "SELECT SUBSTRING_INDEX(JSON_QUOTE(9223372036854775806), ',', 1)",
            3064,
        ),
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), code, "{sql}: {}", err.message());
    }
}

#[test]
fn strict_insert_select_promotes_string_numeric_warnings_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE dst(v VARCHAR(10))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE src(v VARCHAR(64))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO src VALUES ('316-354')")
        .unwrap();

    for (expression, code) in [
        ("IF(v, 1, 0)", 1292),
        ("IF(v <> 0, 1, 0)", 1292),
        ("v | 2147483647", 1292),
        ("CAST(v AS DECIMAL)", 1292),
    ] {
        let sql = format!("INSERT INTO dst SELECT {expression} FROM src");
        let err = engine.execute_with_session(&mut session, &sql).unwrap_err();
        assert_eq!(err.code(), code, "{sql}: {}", err.message());
    }
    assert_eq!(
        one_row_s(&engine, &mut session, "SELECT COUNT(*) FROM dst")[0],
        Value::Int(0)
    );
    engine
        .execute_with_session(&mut session, "CREATE TABLE upd(v DECIMAL)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO upd VALUES (NULL)")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "UPDATE upd SET v = 1 WHERE (v IS NULL) >> ''")
        .unwrap_err();
    assert_eq!(err.code(), 1292, "{}", err.message());
    assert_eq!(
        one_row_s(&engine, &mut session, "SELECT v FROM upd")[0],
        Value::Null
    );

    engine
        .execute_with_session(&mut session, "SET sql_mode = ''")
        .unwrap();
    for expression in [
        "IF(v, 1, 0)",
        "IF(v <> 0, 1, 0)",
        "v | 2147483647",
        "CAST(v AS DECIMAL)",
    ] {
        let sql = format!("INSERT INTO dst SELECT {expression} FROM src");
        engine.execute_with_session(&mut session, &sql).unwrap();
    }
    assert_eq!(
        one_row_s(&engine, &mut session, "SELECT COUNT(*) FROM dst")[0],
        Value::Int(4)
    );
}
