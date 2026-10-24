//! String search function/collation matrix; oracle is local MySQL 9.6
//! with utf8mb4/utf8mb4_0900_ai_ci connections on 2026-06-12.
//! LIKE/LOCATE/INSTR/POSITION/FIND_IN_SET/FIELD/STRCMP and equality
//! share the same default case- and accent-insensitive collation.
//! If either operand is binary through BINARY/VARBINARY/BLOB,
//! use byte semantics instead.
use super::support::query_one_row as one_row;
use super::*;

fn all_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    result.rows
}

/// Baseline: case-insensitive equality and index consistency on indexed VARCHAR.
#[test]
fn indexed_varchar_equality_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(20), KEY idx_s (s))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'abc'),(2,'ABC'),(3,'xyz'),(4,'Ábc')")
        .unwrap();

    let rows = all_rows(&engine, "SELECT id FROM t WHERE s = 'abc' ORDER BY id");
    eprintln!("eq via index: {rows:?}");
    let rows2 = all_rows(
        &engine,
        "SELECT id FROM t IGNORE INDEX (idx_s) WHERE s = 'abc' ORDER BY id",
    );
    eprintln!("eq via scan: {rows2:?}");
    let explain = all_rows(&engine, "EXPLAIN SELECT id FROM t WHERE s = 'abc'");
    eprintln!("explain: {explain:?}");
    let eq_accent = one_row(&engine, "SELECT 'é' = 'e', 'ß' = 'ss'");
    assert_eq!(eq_accent, vec![Value::Int(1), Value::Int(1)]);
    assert_eq!(rows, rows2, "index lookup must agree with table scan");

    // LIKE shares equality's default case/accent-insensitive collation;
    // folded index prefix keys agree with residual filtering.
    let like_rows = all_rows(&engine, "SELECT id FROM t WHERE s LIKE 'ab%' ORDER BY id");
    assert_eq!(
        like_rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(4)]
        ],
        "LIKE prefix matches abc/ABC/Ábc under the default collation"
    );
    let like_scan = all_rows(
        &engine,
        "SELECT id FROM t IGNORE INDEX (idx_s) WHERE s LIKE 'ab%' ORDER BY id",
    );
    assert_eq!(like_rows, like_scan, "LIKE via index must agree with scan");
}

#[test]
fn legacy_general_and_explicit_binary_collations_keep_mysql_weights() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_ne!(
        crate::charset::compare_strings("latin1_general_ci", "ß", "ss"),
        std::cmp::Ordering::Equal
    );
    let folded = crate::parser::parse_expr_fragment(
        "_latin1'ß' COLLATE latin1_general_ci = \
         _latin1'ss' COLLATE latin1_general_ci",
    )
    .unwrap();
    assert_eq!(
        crate::engine::expr::eval_const(&folded).unwrap(),
        Value::Int(0)
    );

    let row = one_row(
        &engine,
        "SELECT _latin1'ß' COLLATE latin1_general_ci = \
                    _latin1'ss' COLLATE latin1_general_ci, \
                _latin1'ß' = _utf8mb4'ss', \
                _utf8mb4'ss' = _latin1'ß', \
                _latin1'B' BETWEEN _latin1'a' AND \
                    _latin1'c' COLLATE latin1_bin, \
                _latin1'B' IN (_latin1'a' COLLATE latin1_bin, _latin1'b')",
    );
    assert_eq!(
        row,
        vec![
            Value::Int(0),
            Value::Int(0),
            Value::Int(0),
            Value::Int(0),
            Value::Int(0),
        ]
    );
}

/// MySQL `main/type_blob` converts the same payloads through BLOB,
/// VARBINARY, CHAR and BINARY. The binary types retain bytewise order, while
/// CHAR uses the column's UCA 9.0 collation where NUL is primary-ignorable.
/// Stable sorting must therefore preserve insertion order inside the
/// `"a\0"`/`"a"`/CHAR-trimmed `"a "` tie group.
#[test]
fn char_sort_uses_uca_0900_nul_weight_after_binary_conversion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (a BLOB)").unwrap();
    engine
        .execute("INSERT INTO t VALUES ('b'),('a\\0'),('a'),('a '),('aa'),(NULL)")
        .unwrap();

    let blob = all_rows(&engine, "SELECT HEX(a) FROM t ORDER BY a");
    assert_eq!(
        blob,
        ["NULL", "61", "6100", "6120", "6161", "62"]
            .into_iter()
            .map(|value| {
                vec![if value == "NULL" {
                    Value::Null
                } else {
                    Value::String(value.to_string())
                }]
            })
            .collect::<Vec<_>>()
    );
    let blob_concat = all_rows(
        &engine,
        "SELECT HEX(CONCAT(a,'\\0')) FROM t ORDER BY CONCAT(a,'\\0')",
    );
    assert_eq!(
        blob_concat,
        ["NULL", "6100", "610000", "612000", "616100", "6200"]
            .into_iter()
            .map(|value| {
                vec![if value == "NULL" {
                    Value::Null
                } else {
                    Value::String(value.to_string())
                }]
            })
            .collect::<Vec<_>>()
    );

    engine.execute("ALTER TABLE t MODIFY a CHAR(5)").unwrap();
    let char_rows = all_rows(&engine, "SELECT HEX(a) FROM t ORDER BY a");
    assert_eq!(
        char_rows,
        ["NULL", "6100", "61", "61", "6161", "62"]
            .into_iter()
            .map(|value| {
                vec![if value == "NULL" {
                    Value::Null
                } else {
                    Value::String(value.to_string())
                }]
            })
            .collect::<Vec<_>>()
    );
    let concat_rows = all_rows(
        &engine,
        "SELECT HEX(CONCAT(a,'\\0')) FROM t ORDER BY CONCAT(a,'\\0')",
    );
    assert_eq!(
        concat_rows,
        ["NULL", "610000", "6100", "6100", "616100", "6200"]
            .into_iter()
            .map(|value| {
                vec![if value == "NULL" {
                    Value::Null
                } else {
                    Value::String(value.to_string())
                }]
            })
            .collect::<Vec<_>>()
    );
}

#[test]
fn indexed_pad_space_order_by_preserves_equivalent_tie_multiset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (v VARCHAR(254), INDEX(v)) \
             CHARACTER SET latin1 COLLATE latin1_swedish_ci",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES ('This is a test '),('This is a test'),('This is a test')")
        .unwrap();

    let indexed = all_rows(
        &engine,
        "SELECT v FROM t WHERE v='This is a test' ORDER BY v",
    );
    let scanned = all_rows(
        &engine,
        "SELECT v FROM t IGNORE INDEX (v) WHERE v='This is a test' ORDER BY v",
    );
    let display = |rows: &[Vec<Value>]| {
        rows.iter()
            .map(|row| crate::engine::expr::string_value(&row[0]))
            .collect::<Vec<_>>()
    };
    let mut indexed_values = display(&indexed);
    let mut scanned_values = display(&scanned);
    assert_eq!(
        indexed_values, scanned_values,
        "index access and table-scan sort must produce the same tie ordering"
    );
    indexed_values.sort();
    scanned_values.sort();
    assert_eq!(
        indexed_values,
        vec![
            "This is a test".to_string(),
            "This is a test".to_string(),
            "This is a test ".to_string(),
        ],
        "ORDER BY may reorder an equivalent tie group but must preserve its multiset"
    );
    assert!(scanned_values.iter().all(|value| {
        crate::charset::compare_strings("latin1_swedish_ci", value, "This is a test")
            == std::cmp::Ordering::Equal
    }));
    assert_ne!(
        crate::charset::compare_strings("utf8mb4_0900_ai_ci", "This is a test ", "This is a test"),
        std::cmp::Ordering::Equal,
        "the same reordering must not be generalized to NO PAD collations"
    );
}

#[test]
fn like_follows_default_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT 'ABC' LIKE 'abc', 'café' LIKE 'CAFE', 'é' LIKE 'e', \
         'ß' LIKE 'ss', 'ABC' LIKE 'a_c', 'é' LIKE '_', 'éx' LIKE '_x'",
    );
    assert_eq!(row[0], Value::Int(1), "ci fold");
    assert_eq!(row[1], Value::Int(1), "accent + case fold");
    assert_eq!(row[2], Value::Int(1), "accent fold");
    assert_eq!(
        row[3],
        Value::Int(0),
        "LIKE is char-by-char: no ß=ss expansion"
    );
    assert_eq!(row[4], Value::Int(1), "`_` with ci literals");
    assert_eq!(row[5], Value::Int(1), "`_` matches one non-ASCII char");
    assert_eq!(row[6], Value::Int(1));

    // ESCAPE bypasses byte fast paths but still compares literals case-insensitively.
    let row = one_row(
        &engine,
        "SELECT 'A_C' LIKE 'a\\\\_c', 'AXC' LIKE 'a\\\\_c', '100%' LIKE '100|%' ESCAPE '|'",
    );
    assert_eq!(
        row[0],
        Value::Int(1),
        "escaped `_` matches literal underscore ci"
    );
    assert_eq!(
        row[1],
        Value::Int(0),
        "escaped `_` is literal, not wildcard"
    );
    assert_eq!(row[2], Value::Int(1), "custom ESCAPE char");

    // NOT LIKE negates the same predicate.
    let row = one_row(&engine, "SELECT 'ABC' NOT LIKE 'abc', 'xyz' NOT LIKE 'abc'");
    assert_eq!(row[0], Value::Int(0));
    assert_eq!(row[1], Value::Int(1));
}

#[test]
fn like_binary_operands_stay_byte_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT BINARY 'ABC' LIKE 'abc', 'ABC' LIKE BINARY 'abc', \
         BINARY 'abc' LIKE 'abc', BINARY 'abc' LIKE 'ab%', \
         X'FF80' LIKE '%abc%', X'0061FF' LIKE '%a%'",
    );
    assert_eq!(row[0], Value::Int(0), "BINARY value forces byte match");
    assert_eq!(row[1], Value::Int(0), "BINARY pattern forces byte match");
    assert_eq!(row[2], Value::Int(1));
    assert_eq!(row[3], Value::Int(1));
    assert_eq!(row[4], Value::Int(0), "invalid UTF-8 remains valid binary");
    assert_eq!(row[5], Value::Int(1), "binary LIKE searches raw bytes");

    // VARBINARY columns behave similarly, carrying Value::Bytes.
    engine
        .execute("CREATE TABLE vb (id INT PRIMARY KEY, b VARBINARY(10))")
        .unwrap();
    engine
        .execute("INSERT INTO vb VALUES (1, 'abc'), (2, 'ABC')")
        .unwrap();
    let rows = all_rows(&engine, "SELECT id FROM vb WHERE b LIKE 'abc' ORDER BY id");
    assert_eq!(
        rows,
        vec![vec![Value::Int(1)]],
        "binary column LIKE is byte-exact"
    );
}

#[test]
fn like_null_operands_yield_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT NULL LIKE 'x', 'x' LIKE NULL, NULL LIKE NULL, NULL NOT LIKE 'x'",
    );
    assert_eq!(
        row,
        vec![Value::Null, Value::Null, Value::Null, Value::Null]
    );

    // WHERE still filters NULL as false, matching MySQL.
    engine.execute("CREATE TABLE n (s VARCHAR(10))").unwrap();
    engine
        .execute("INSERT INTO n VALUES (NULL), ('NULL'), ('abc')")
        .unwrap();
    let rows = all_rows(&engine, "SELECT s FROM n WHERE s LIKE 'NU%'");
    assert_eq!(
        rows,
        vec![vec![Value::String("NULL".into())]],
        "only the literal string 'NULL' matches; SQL NULL never does"
    );
}

/// Differential oracle: MySQL 9.6 with utf8mb4_0900_ai_ci.
/// LOCATE('É','resume')=2, LOCATE('e','résumé')=2, INSTR('abcd','BC')=2,
/// LOCATE('BC','abcd',3)=0, FIND_IN_SET('É','e,f')=1, FIELD('é','a','e')=2,
/// STRCMP('abc','ABC')=0, STRCMP('é','e')=0。
#[test]
fn search_functions_follow_default_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT LOCATE('BC','abcd'), INSTR('abcd','BC'), POSITION('BC' IN 'abcd'), \
         LOCATE('É','resume'), LOCATE('e','résumé'), LOCATE('BC','abcd',3), \
         LOCATE('bc','xbcabc',4)",
    );
    assert_eq!(row[0], Value::Int(2), "LOCATE ci");
    assert_eq!(row[1], Value::Int(2), "INSTR ci");
    assert_eq!(row[2], Value::Int(2), "POSITION ci");
    assert_eq!(row[3], Value::Int(2), "LOCATE accent fold (needle)");
    assert_eq!(row[4], Value::Int(2), "LOCATE accent fold (haystack)");
    assert_eq!(row[5], Value::Int(0), "start past match");
    assert_eq!(row[6], Value::Int(5), "start offset with ci");

    let row = one_row(
        &engine,
        "SELECT LOCATE('ss','ß'), LOCATE('ß','ss'), \
         LOCATE('viii','Ⅷ',1), LOCATE('','å',3)",
    );
    assert_eq!(
        row,
        vec![Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(0)],
        "substring search must use UCA expansion weights and bounded empty-needle positions"
    );

    engine
        .execute(
            "CREATE TABLE search_collation (\
               ci VARCHAR(8) COLLATE utf8mb4_0900_ai_ci,\
               bin VARCHAR(8) COLLATE utf8mb4_bin)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO search_collation VALUES ('a','a')")
        .unwrap();
    let row = one_row(
        &engine,
        "SELECT LOCATE('A', ci), LOCATE('A', bin), \
                INSTR(ci, 'A'), POSITION('A' IN bin) FROM search_collation",
    );
    assert_eq!(
        row,
        vec![Value::Int(1), Value::Int(0), Value::Int(1), Value::Int(0)],
        "search functions must consume the column collation in every argument order"
    );

    let row = one_row(
        &engine,
        "SELECT POSITION(_latin1'B' COLLATE latin1_general_ci \
                         IN _latin1'abcd' COLLATE latin1_bin), \
                POSITION(_latin1'B' IN _latin2'abcd')",
    );
    assert_eq!(
        row,
        vec![Value::Int(0), Value::Int(2)],
        "POSITION coerces its needle into the haystack collation"
    );

    let row = one_row(
        &engine,
        "SELECT FIND_IN_SET('B','a,b,c'), FIND_IN_SET('É','e,f'), \
         FIELD('B','a','b','c'), FIELD('é','a','e'), \
         STRCMP('abc','ABC'), STRCMP('é','e'), STRCMP('a','b')",
    );
    assert_eq!(row[0], Value::Int(2));
    assert_eq!(row[1], Value::Int(1));
    assert_eq!(row[2], Value::Int(2));
    assert_eq!(row[3], Value::Int(2));
    assert_eq!(row[4], Value::Int(0), "STRCMP ci");
    assert_eq!(row[5], Value::Int(0), "STRCMP accent fold");
    assert_eq!(row[6], Value::Int(-1));

    // Search coerces the needle into the haystack domain. Only a binary
    // haystack selects byte semantics; an explicitly binary needle still
    // follows the textual haystack's collation.
    let row = one_row(
        &engine,
        "SELECT LOCATE(BINARY 'BC','abcd'), INSTR('abcd',BINARY 'BC'), \
         LOCATE(x'44', _utf8mb4'abcdef'), POSITION(x'44' IN _utf8mb4'abcdef'), \
         FIND_IN_SET(BINARY 'B','a,b,c'), FIELD(BINARY 'B','a','b','c'), \
         STRCMP(BINARY 'abc','ABC')",
    );
    assert_eq!(row[0], Value::Int(2));
    assert_eq!(row[1], Value::Int(2));
    assert_eq!(row[2], Value::Int(4), "hex needle coerces to haystack text");
    assert_eq!(
        row[3],
        Value::Int(4),
        "POSITION uses the haystack text domain"
    );
    assert_eq!(row[4], Value::Int(0));
    assert_eq!(row[5], Value::Int(0));
    assert_eq!(row[6], Value::Int(1), "binary STRCMP: 'abc' > 'ABC'");

    // MySQL REPLACE/TRIM/SUBSTRING_INDEX are always case-sensitive;
    // their byte semantics remain unaffected by these changes.
    let row = one_row(
        &engine,
        "SELECT REPLACE('aAbA','a','X'), TRIM('a' FROM 'AaXa'), \
         SUBSTRING_INDEX('aXbXc','x',1)",
    );
    assert_eq!(row[0], Value::String("XAbA".into()));
    assert_eq!(row[1], Value::String("AaX".into()));
    assert_eq!(row[2], Value::String("aXbXc".into()));
}

#[test]
fn find_in_set_uses_declaration_ordinal_for_set_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE set_find(s SET('alpha','bravo','charlie','delta','echo','foxtrot'))")
        .unwrap();
    engine
        .execute("INSERT INTO set_find VALUES ('bravo,delta,foxtrot'),('foxtrot'),('alpha')")
        .unwrap();
    let rows = all_rows(
        &engine,
        "SELECT FIND_IN_SET('foxtrot', s) FROM set_find ORDER BY s",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(0)],
            vec![Value::Int(6)],
            vec![Value::Int(6)],
        ]
    );
}

/// MySQL 9.6: UPPER('ß')='ß' as one character; LOWER('İ')='i';
/// UPPER('ﬁ')='ﬁ'; UPPER/LOWER on binary strings are no-ops.
#[test]
fn upper_lower_use_one_to_one_case_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT UPPER('ß'), CHAR_LENGTH(UPPER('ß')), LOWER('İ'), HEX(LOWER('İ')), \
         UPPER('ﬁ'), UPPER('abc'), LOWER('ÀBÇ')",
    );
    assert_eq!(row[0], Value::String("ß".into()), "no ß→SS expansion");
    assert_eq!(row[1], Value::Int(1));
    assert_eq!(
        row[2],
        Value::String("i".into()),
        "İ simple-maps to plain i"
    );
    assert_eq!(row[3], Value::String("69".into()));
    assert_eq!(row[4], Value::String("ﬁ".into()), "no ligature expansion");
    assert_eq!(row[5], Value::String("ABC".into()));
    assert_eq!(row[6], Value::String("àbç".into()));

    let row = one_row(&engine, "SELECT UPPER(BINARY 'abc'), LOWER(BINARY 'ABC')");
    assert_eq!(
        row[0],
        Value::Bytes(b"abc".to_vec()),
        "binary string: no-op"
    );
    assert_eq!(
        row[1],
        Value::Bytes(b"ABC".to_vec()),
        "binary string: no-op"
    );

    // Column-driven compiled execution matches legacy evaluation.
    engine.execute("CREATE TABLE u (s VARCHAR(10))").unwrap();
    engine.execute("INSERT INTO u VALUES ('ß'), ('İ')").unwrap();
    let rows = all_rows(&engine, "SELECT UPPER(s), LOWER(s) FROM u ORDER BY s");
    assert_eq!(rows.len(), 2);
    for row in &rows {
        for v in row {
            assert!(matches!(v, Value::String(_) | Value::SmallString(_)));
        }
    }
    let texts: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            (
                crate::storage::value_to_string(&r[0]),
                crate::storage::value_to_string(&r[1]),
            )
        })
        .collect();
    assert!(texts.contains(&("ß".to_string(), "ß".to_string())));
    assert!(texts.contains(&("İ".to_string(), "i".to_string())));
}

/// Protocol column definitions report actual text collation IDs;
/// BINARY/VARBINARY/BLOB report binary ID 63.
#[test]
fn column_definition_collation_ids_follow_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE cs (v VARCHAR(10), vb VARBINARY(10), b BLOB, t TEXT, \
             vbin VARCHAR(10) COLLATE utf8mb4_bin, i INT)",
        )
        .unwrap();

    let QueryResponse::ResultSet(result) = engine.execute("SELECT * FROM cs").unwrap() else {
        panic!("expected result set");
    };
    let ids: Vec<Option<u16>> = result
        .columns
        .iter()
        .map(|c| c.source.collation_id)
        .collect();
    let default_id = crate::charset::collation_id(crate::charset::DEFAULT_COLLATION);
    let binary_id = crate::charset::collation_id("binary");
    assert_eq!(
        ids[0],
        Some(default_id),
        "VARCHAR reports default collation"
    );
    assert_eq!(ids[1], Some(binary_id), "VARBINARY reports binary (63)");
    assert_eq!(ids[2], Some(binary_id), "BLOB reports binary (63)");
    assert_eq!(ids[3], Some(default_id), "TEXT reports default collation");
    assert_eq!(
        ids[4],
        Some(crate::charset::collation_id("utf8mb4_bin")),
        "explicit COLLATE reaches the wire metadata"
    );
    assert_eq!(ids[5], None, "non-string columns keep type-derived default");
    assert_eq!(
        binary_id, 63,
        "binary pseudo-collation id is fixed by protocol"
    );
}

#[test]
fn derived_charset_introducer_metadata_drives_outer_conversion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT _latin1 'a' AS introduced").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.columns[0].source.collation_id,
        Some(crate::charset::collation_id(
            crate::charset::default_collation_for_charset("latin1").unwrap()
        ))
    );

    engine
        .execute("CREATE TABLE charset_target(a VARCHAR(10) CHARACTER SET ascii)")
        .unwrap();
    engine
        .execute("INSERT INTO charset_target VALUES ('a')")
        .unwrap();
    let QueryResponse::ResultSet(joined) = engine
        .execute(
            "SELECT dt.a, t.a FROM charset_target t, \
             (SELECT _utf8mb4 '±') AS dt(a)",
        )
        .unwrap()
    else {
        panic!("expected joined result set");
    };
    assert_eq!(joined.columns[0].source.org_table, "");
    assert_eq!(joined.columns[1].source.org_table, "charset_target");
    assert_eq!(
        joined.columns[0].source.collation_id,
        Some(crate::charset::collation_id(
            crate::charset::default_collation_for_charset("utf8mb4").unwrap()
        ))
    );
    assert_eq!(
        joined.columns[1].source.collation_id,
        Some(crate::charset::collation_id(
            crate::charset::default_collation_for_charset("ascii").unwrap()
        ))
    );
    let err = engine
        .execute(
            "SELECT GREATEST(dt.a, t.a) FROM charset_target t, \
             (SELECT _utf8mb4 '±') AS dt(a)",
        )
        .unwrap_err();
    assert_eq!(err.code(), 3854, "{err}");
}

/// SHOW CREATE column CHARACTER SET/COLLATE follows MySQL omission rules:
/// omit table defaults; if COLLATE is the charset default, show only CHARACTER SET.
#[test]
fn show_create_table_echoes_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE sc (a VARCHAR(10), b VARCHAR(10) COLLATE utf8mb4_bin, \
             c VARCHAR(10) CHARACTER SET latin1, \
             d VARCHAR(10) CHARACTER SET latin1 COLLATE latin1_bin)",
        )
        .unwrap();

    let row = one_row(&engine, "SHOW CREATE TABLE sc");
    let ddl = crate::storage::value_to_string(&row[1]);
    assert!(
        !ddl.contains("`a` varchar(10) C"),
        "column inheriting the table default renders bare: {ddl}"
    );
    assert!(
        ddl.contains("`b` varchar(10) COLLATE utf8mb4_bin"),
        "explicit non-default collation echoes COLLATE only: {ddl}"
    );
    assert!(
        ddl.contains("`c` varchar(10) CHARACTER SET latin1"),
        "explicit charset echoes CHARACTER SET: {ddl}"
    );
    assert!(
        !ddl.contains("`c` varchar(10) CHARACTER SET latin1 COLLATE"),
        "charset-default collation is elided: {ddl}"
    );
    assert!(
        ddl.contains("`d` varchar(10) CHARACTER SET latin1 COLLATE latin1_bin"),
        "non-default collation of a non-default charset echoes both: {ddl}"
    );
}

/// Charset introducers for strings or hex parse as CONVERT_USING.
/// _binary produces byte-semantic binary strings; other charsets
/// decode hex/binary input with their respective encoding.
/// MySQL 9.6: _utf8mb4'ABC'='abc' is 1; _binary'abc'='ABC' is 0;
/// _binary'abc' LIKE 'ABC' is 0; _latin1 X'E9' gives é; _utf8mb4 X'E4B8AD' gives U+4E2D.
#[test]
fn charset_introducers_parse_and_apply() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT _utf8mb4'ABC' = 'abc', _binary'abc' = 'ABC', _binary'abc' LIKE 'ABC'",
    );
    assert_eq!(row[0], Value::Int(1), "text introducer keeps ci compare");
    assert_eq!(row[1], Value::Int(0), "_binary forces byte equality");
    assert_eq!(row[2], Value::Int(0), "_binary forces byte LIKE");

    let row = one_row(
        &engine,
        "SELECT _latin1 X'E9', _utf8mb4 X'E4B8AD', CHARSET(_binary'x')",
    );
    assert_eq!(
        crate::storage::value_to_string(&row[0]),
        "é",
        "hex bytes decode under the introducer charset"
    );
    assert_eq!(crate::storage::value_to_string(&row[1]), "中");
    assert_eq!(row[2], Value::String("binary".into()));

    let row = one_row(
        &engine,
        "SELECT COERCIBILITY(_latin1 X'E9'), \
                COERCIBILITY(N'national'), \
                COERCIBILITY(CONVERT('text' USING latin1))",
    );
    assert_eq!(
        row,
        vec![Value::Int(4), Value::Int(4), Value::Int(2)],
        "introducers remain coercible literals; explicit conversion is implicit"
    );

    let row = one_row(
        &engine,
        "SELECT COLLATION(TRIM(LEADING _latin2' ' FROM _latin2'a')), \
                COLLATION(TRIM(TRAILING _latin2' ' FROM _latin2'a')), \
                COLLATION(TRIM(BOTH _latin2' ' FROM _latin2'a'))",
    );
    assert_eq!(
        row,
        vec![
            Value::String("latin2_general_ci".into()),
            Value::String("latin2_general_ci".into()),
            Value::String("latin2_general_ci".into()),
        ],
        "TRIM result collation follows its source, not its internal direction marker"
    );

    engine
        .execute("CREATE TABLE introduced_cmp (a CHAR(10) CHARACTER SET utf8mb3 NOT NULL)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO introduced_cmp VALUES \
             ('bbbb'), (_koi8r X'C3C3C3C3'), (_latin1 X'C4C4C4C4')",
        )
        .unwrap();
    let rows = all_rows(
        &engine,
        "SELECT a FROM introduced_cmp \
         WHERE a IN ('bbbb', _koi8r X'C3C3C3C3', _latin1 X'C4C4C4C4') \
         ORDER BY a",
    );
    assert_eq!(
        rows.len(),
        3,
        "the column's implicit collation wins over introducer literals"
    );

    // CONVERT(bytes USING charset) uses the same decoding path.
    let row = one_row(
        &engine,
        "SELECT CONVERT(X'E9' USING latin1), CONVERT(X'D6D0' USING gbk)",
    );
    assert_eq!(crate::storage::value_to_string(&row[0]), "é");
    assert_eq!(crate::storage::value_to_string(&row[1]), "中");

    // Bulk INSERT fast paths must preserve introducer semantics.
    engine
        .execute("CREATE TABLE bi (id INT PRIMARY KEY, b VARBINARY(10))")
        .unwrap();
    engine
        .execute("INSERT INTO bi VALUES (1, _binary'ab'), (2, _binary'cd')")
        .unwrap();
    let rows = all_rows(&engine, "SELECT id FROM bi WHERE b = _binary'ab'");
    assert_eq!(rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn field_aggregates_coercible_introducer_literals_via_connection_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        one_row(
            &engine,
            "SELECT FIELD(_latin2'b','A','B'), \
                    FIELD('b',_latin2'A','B'), \
                    FIELD('1',_latin2'3','2',1)",
        ),
        vec![Value::Int(2), Value::Int(2), Value::Int(3)]
    );
    let err = engine
        .execute("SELECT FIELD(_utf8mb4'abc', X'FF')")
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (3854, "HY000"));
}

/// ENUM sorting and numeric comparison use declaration ordinals; oracle: MySQL 9.6.
/// ORDER BY → NULL,small,large,medium;v=2→'large';v>1→large,medium;
/// v<>9 matches all non-NULL rows.
#[test]
fn enum_orders_and_compares_by_declaration_ordinal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE e (id INT PRIMARY KEY, v ENUM('small','large','medium'))")
        .unwrap();
    engine
        .execute("INSERT INTO e VALUES (1,'small'),(2,'large'),(3,'medium'),(4,NULL)")
        .unwrap();

    let rows = all_rows(&engine, "SELECT id FROM e ORDER BY v, id");
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(4)],
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ],
        "ENUM sorts by declaration ordinal, NULLs first"
    );

    let rows = all_rows(&engine, "SELECT id FROM e WHERE v = 2");
    assert_eq!(
        rows,
        vec![vec![Value::Int(2)]],
        "numeric eq hits the 2nd value"
    );
    let rows = all_rows(&engine, "SELECT id FROM e WHERE v > 1 ORDER BY id");
    assert_eq!(rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    let rows = all_rows(&engine, "SELECT id FROM e WHERE v <> 9 ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ],
        "out-of-range ordinal: all non-NULL rows differ"
    );
    // String comparison is unaffected.
    let rows = all_rows(&engine, "SELECT id FROM e WHERE v = 'LARGE'");
    assert_eq!(
        rows,
        vec![vec![Value::Int(2)]],
        "string compare stays ci string"
    );
}

/// SET sorting and numeric comparison use member bitmaps; oracle: MySQL 9.6.
/// ORDER BY → NULL,'','a','b','a,c';v=5→'a,c';v>1→'b','a,c'。
#[test]
fn set_orders_and_compares_by_bitmask() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY, v SET('a','b','c'))")
        .unwrap();
    engine
        .execute("INSERT INTO s VALUES (1,'a'),(2,'b'),(3,'a,c'),(4,''),(5,NULL)")
        .unwrap();

    let rows = all_rows(&engine, "SELECT id FROM s ORDER BY v, id");
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(5)],
            vec![Value::Int(4)],
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ],
        "SET sorts by bitmask, NULLs first"
    );

    let rows = all_rows(&engine, "SELECT id FROM s WHERE v = 5");
    assert_eq!(rows, vec![vec![Value::Int(3)]], "bitmask 5 = a|c");
    let rows = all_rows(&engine, "SELECT id FROM s WHERE v > 1 ORDER BY id");
    assert_eq!(rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
}

/// Byte-sensitive functions directly over CONVERT use bytes in the target encoding.
/// MySQL 9.6: LENGTH(CONVERT(U+4E2D USING gbk))=2.
/// HEX(...)='D6D0',BIT_LENGTH=16,ASCII=214,CHAR_LENGTH=1。
#[test]
fn byte_functions_over_convert_using_use_target_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT LENGTH(CONVERT('中' USING gbk)), OCTET_LENGTH(CONVERT('中' USING gbk)), \
         BIT_LENGTH(CONVERT('中' USING gbk)), HEX(CONVERT('中' USING gbk)), \
         ASCII(CONVERT('中' USING gbk)), CHAR_LENGTH(CONVERT('中' USING gbk)), \
         LENGTH(CONVERT(X'E9' USING latin1)), LENGTH('中')",
    );
    assert_eq!(row[0], Value::Int(2), "gbk bytes");
    assert_eq!(row[1], Value::Int(2));
    assert_eq!(row[2], Value::Int(16));
    assert_eq!(row[3], Value::String("D6D0".into()));
    assert_eq!(row[4], Value::Int(0xD6), "first target-encoding byte");
    assert_eq!(
        row[5],
        Value::Int(1),
        "char semantics stay on the decoded text"
    );
    assert_eq!(row[6], Value::Int(1), "latin1 'é' is one byte");
    assert_eq!(
        row[7],
        Value::Int(3),
        "plain literals keep UTF-8 byte length"
    );

    let row = one_row(&engine, "SELECT LENGTH(CONVERT(NULL USING gbk))");
    assert_eq!(row[0], Value::Null);
}

/// Binary strings are byte sequences, not lossy UTF-8 strings, when assigned
/// to a character column.  The destination character set decides how they
/// are decoded: every byte is representable in latin1, while malformed
/// utf8mb4 remains an error.
#[test]
fn binary_assignment_uses_destination_character_set() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE binary_text (\
             id INT PRIMARY KEY,\
             latin CHAR(16) CHARACTER SET latin1,\
             utf8 VARCHAR(4) CHARACTER SET utf8mb4)",
        )
        .unwrap();
    let stored = engine.db.table("test.binary_text").unwrap();
    assert_eq!(stored.columns[1].charset.as_deref(), Some("latin1"));
    assert_eq!(
        stored.columns[1].collation.as_deref(),
        Some("latin1_swedish_ci")
    );

    engine
        .execute("INSERT INTO binary_text (id, latin) VALUES (1, X'FF80')")
        .unwrap();
    engine
        .execute("INSERT INTO binary_text (id, latin) VALUES (2, AES_ENCRYPT('foo', 'bar'))")
        .unwrap();
    assert_eq!(
        one_row(&engine, "SELECT COUNT(*) FROM binary_text"),
        vec![Value::Int(2)]
    );
    let aes_roundtrip = one_row(
        &engine,
        "SELECT COUNT(*) FROM binary_text WHERE AES_DECRYPT(latin, 'bar') = 'foo'",
    );
    assert_eq!(
        aes_roundtrip,
        vec![Value::Int(1)],
        "octet functions must encode a latin1 column back to its source bytes"
    );

    let err = engine
        .execute("INSERT INTO binary_text (id, utf8) VALUES (3, X'FF')")
        .unwrap_err();
    assert_eq!(err.code(), 1366);
    assert_eq!(err.sql_state(), "HY000");
}

/// ALTER CONVERT CHARACTER SET changing collation must rebuild embedded index-key
/// collation IDs, or old rows disappear from indexed lookups.
#[test]
fn convert_to_charset_rebuilds_collated_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE cc (id INT PRIMARY KEY, s VARCHAR(20) COLLATE utf8mb4_bin, \
             KEY idx_s (s))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO cc VALUES (1,'abc'),(2,'ABC'),(3,'xyz')")
        .unwrap();

    engine
        .execute("ALTER TABLE cc CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        .unwrap();

    // Both rebuilt old rows and new rows must remain index-visible.
    engine.execute("INSERT INTO cc VALUES (4,'abd')").unwrap();
    let via_index = all_rows(&engine, "SELECT id FROM cc WHERE s = 'abc' ORDER BY id");
    let via_scan = all_rows(
        &engine,
        "SELECT id FROM cc IGNORE INDEX (idx_s) WHERE s = 'abc' ORDER BY id",
    );
    assert_eq!(
        via_index, via_scan,
        "index agrees with scan after conversion"
    );
    assert!(
        via_index.contains(&vec![Value::Int(1)]),
        "pre-conversion rows reachable via index: {via_index:?}"
    );
    let row = one_row(&engine, "SHOW CREATE TABLE cc");
    let ddl = crate::storage::value_to_string(&row[1]);
    assert!(
        ddl.contains("COLLATE=utf8mb4_0900_ai_ci") || ddl.contains("utf8mb4_0900_ai_ci"),
        "table collation updated: {ddl}"
    );
}

#[test]
fn like_multisegment_ci_matches_q13_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE c (id INT PRIMARY KEY, t VARCHAR(60))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO c VALUES \
             (1,'no special handling or requests here'), \
             (2,'no SPECIAL handling or REQUESTS here'), \
             (3,'nothing to see'), \
             (4,'spécial client requests')",
        )
        .unwrap();

    // Multisegment %literal1%literal2% uses folded byte fast paths for ASCII,
    // and collation NFA for non-ASCII such as é; both must match.
    let rows = all_rows(
        &engine,
        "SELECT id FROM c WHERE t LIKE '%special%requests%' ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(4)]
        ]
    );
    let rows = all_rows(
        &engine,
        "SELECT id FROM c WHERE t NOT LIKE '%special%requests%' ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Value::Int(3)]]);
}
