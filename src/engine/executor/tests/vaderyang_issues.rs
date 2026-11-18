//! Reproductions for issues reported by vaderyang (#14–#42).
use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine)
}

fn result(engine: &StorageQueryEngine, sql: &str) -> crate::engine::api::ResultSet {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected result set: {sql}")
    };
    result
}

#[test]
fn issue_22_delayed_derived_alias_pushdown() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE t(a INT,b INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(20,0)")
        .unwrap();
    for column in ["k", "d.k"] {
        let sql = format!("SELECT * FROM (SELECT *,b AS k FROM t) d(k,q,r) WHERE {column}>5");
        assert_eq!(
            result(&engine, &sql).rows,
            vec![vec![Value::Int(20), Value::Int(0), Value::Int(0)]],
            "{sql}"
        );
    }
}

#[test]
fn issue_23_binary_character_hash_join() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE a(id INT,s VARCHAR(10))")
        .unwrap();
    engine
        .execute("CREATE TABLE b(id INT,v VARBINARY(10))")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1,'abc'),(2,'ABC'),(3,'abc ')")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (1,X'616263'),(2,X'414243')")
        .unwrap();
    for hint in ["HASH_JOIN(a,b)", "NO_HASH_JOIN(a,b)"] {
        let sql = format!("SELECT /*+ {hint} */ a.id FROM a JOIN b ON a.s=b.v ORDER BY a.id");
        assert_eq!(
            result(&engine, &sql).rows,
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
            "{sql}"
        );
    }
}

#[test]
fn issue_24_enum_grouping_preserves_column_collation() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE t(e ENUM('a','A') CHARACTER SET latin1 COLLATE latin1_bin,x INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES ('a',1),('A',2)")
        .unwrap();
    let rows = result(&engine, "SELECT e,COUNT(*),SUM(x) FROM t GROUP BY e").rows;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().all(|row| row[1] == Value::Int(1)));
}

#[test]
fn issue_24_streaming_grouping_preserves_column_collation() {
    let (_dir, engine) = fixture();
    engine
        .execute(
            "CREATE TABLE t(v VARCHAR(10) CHARACTER SET latin1 COLLATE latin1_bin,x INT,KEY(v))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES ('A',1),('a',2)")
        .unwrap();
    let rows = result(
        &engine,
        "SELECT v,COUNT(*),SUM(x),(SELECT 1) q FROM t FORCE INDEX(v) GROUP BY v",
    )
    .rows;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().all(|row| row[1] == Value::Int(1)));
}

#[test]
fn issue_25_double_predicate_matches_expression() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,d DOUBLE)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10.0),(2,10.000000000000002)")
        .unwrap();
    for predicate in [
        "d=10.000000000000001",
        "d<=>10.000000000000001",
        "d+0=10.000000000000001",
    ] {
        assert_eq!(
            result(&engine, &format!("SELECT id FROM t WHERE {predicate}")).rows,
            vec![vec![Value::Int(2)]],
            "{predicate}"
        );
    }
}

#[test]
fn issue_26_shadowed_derived_equivalence_domain() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE s(k DATE)").unwrap();
    engine.execute("CREATE TABLE u(k DATE)").unwrap();
    engine
        .execute("INSERT INTO u VALUES ('2020-01-01')")
        .unwrap();
    for sql in [
        "SELECT COUNT(*) FROM (SELECT CAST('2020-01-01' AS CHAR(10)) k) s JOIN u uu ON s.k=uu.k WHERE uu.k=20200101",
        "WITH s AS (SELECT CAST('2020-01-01' AS CHAR(10)) k) SELECT COUNT(*) FROM s JOIN u uu ON s.k=uu.k WHERE uu.k=20200101",
    ] {
        assert_eq!(result(&engine, sql).rows,vec![vec![Value::Int(1)]], "{sql}");
    }
}

#[test]
fn issue_28_binary_string_rounding() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE t(b VARBINARY(10),p BINARY(4))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES ('12','12')").unwrap();
    let row = result(
        &engine,
        "SELECT ROUND(b),TRUNCATE(b,0),ROUND(p),ROUND(CAST('12' AS BINARY)) FROM t",
    )
    .rows
    .remove(0);
    for value in row {
        assert_eq!(value, Value::Double(12.0));
    }
    assert_eq!(
        result(&engine, "SELECT ROUND(X'3132'),TRUNCATE(X'3132',0)").rows,
        vec![vec![Value::UInt(12594), Value::UInt(12594)]]
    );
    for (sql, expected) in [
        (
            "SELECT MAKEDATE(CAST('2024' AS BINARY),CAST('12' AS BINARY))",
            "2024-01-12",
        ),
        ("SELECT MAKEDATE(X'07E8',X'0C')", "2024-01-12"),
        (
            "SELECT MAKETIME(CAST('12' AS BINARY),CAST('34' AS BINARY),CAST('56' AS BINARY))",
            "12:34:56",
        ),
        ("SELECT MAKETIME(X'0C',X'22',X'38')", "12:34:56"),
        ("SELECT BIN(CAST('12' AS BINARY))", "1100"),
        ("SELECT BIN(X'0C')", "1100"),
        ("SELECT OCT(CAST('12' AS BINARY))", "14"),
        ("SELECT OCT(X'0C')", "14"),
    ] {
        assert_eq!(
            result(&engine, sql).rows[0][0].to_display_string(),
            expected,
            "{sql}"
        );
    }
}

#[test]
fn issue_29_year_string_zero() {
    let (_dir, engine) = fixture();
    let row = result(&engine,"SELECT CAST('0' AS YEAR),CAST('00' AS YEAR),CAST('0000' AS YEAR),CAST(0 AS YEAR),CAST('0.0' AS YEAR)").rows.remove(0);
    assert_eq!(
        row,
        vec![
            Value::Year(100),
            Value::Year(100),
            Value::Year(100),
            Value::Year(0),
            Value::Year(100)
        ]
    );
}

#[test]
fn issue_30_decimal_text_uses_result_scale() {
    let (_dir, engine) = fixture();
    let row = result(
        &engine,
        "SELECT CAST(1/3 AS CHAR),CONCAT('',1/3),CAST(1.0/3 AS CHAR)",
    )
    .rows
    .remove(0);
    assert_eq!(
        row,
        vec![
            Value::String("0.3333".into()),
            Value::String("0.3333".into()),
            Value::String("0.33333".into())
        ]
    );
    engine
        .execute("CREATE TABLE text_result(v VARCHAR(100))")
        .unwrap();
    engine
        .execute("INSERT INTO text_result VALUES (1.0/3)")
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT v FROM text_result").rows,
        vec![vec![Value::String("0.33333".into())]]
    );
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET div_precision_increment=2")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute_with_session(&mut session, "SELECT CONCAT('',1/3)")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::String("0.33".into())]]);
}

#[test]
fn issue_30_export_shapes_decimal_result_and_never_panics() {
    let (dir, engine) = fixture();
    engine
        .execute(&format!(
            "SET GLOBAL secure_file_priv='{}'",
            dir.path().display()
        ))
        .unwrap();
    let outfile = dir.path().join("result.txt");
    engine
        .execute(&format!("SELECT 1/3 INTO OUTFILE '{}'", outfile.display()))
        .unwrap();
    assert_eq!(std::fs::read(outfile).unwrap(), b"0.3333\n");
    let dumpfile = dir.path().join("result.bin");
    engine
        .execute(&format!(
            "SELECT CAST(0 AS DECIMAL(65,30))/CAST(1 AS DECIMAL(65,30)) INTO DUMPFILE '{}'",
            dumpfile.display()
        ))
        .unwrap();
    assert_eq!(
        std::fs::read(dumpfile).unwrap(),
        b"0.000000000000000000000000000000"
    );
    for scale in [0, 30, 47, 54, 72, 255] {
        for coefficient in [0, i64::MIN, i64::MAX] {
            let mut buffer = [0; 258];
            let bytes =
                crate::types::write_decimal_compact_canonical(coefficient, scale, &mut buffer);
            assert_eq!(
                std::str::from_utf8(bytes).unwrap(),
                Value::DecimalCompact(coefficient, scale).to_display_string()
            );
        }
    }
}

#[test]
fn issue_23_computed_binary_character_hash_join() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE a(s VARCHAR(10))").unwrap();
    engine.execute("CREATE TABLE b(v VARBINARY(10))").unwrap();
    engine.execute("INSERT INTO a VALUES ('abc')").unwrap();
    engine.execute("INSERT INTO b VALUES (X'616263')").unwrap();
    for condition in ["CONCAT(a.s,'')=b.v", "a.s=CONCAT(b.v,X'')"] {
        let sql = format!("SELECT /*+ HASH_JOIN(a,b) */ COUNT(*) FROM a JOIN b ON {condition}");
        assert_eq!(
            result(&engine, &sql).rows,
            vec![vec![Value::Int(1)]],
            "{sql}"
        );
    }
}

#[test]
fn issue_23_binary_join_preserves_character_source_octets() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE b(v VARBINARY(10))").unwrap();
    for charset in ["latin1", "utf16", "utf16le", "utf32", "gbk"] {
        engine
            .execute(&format!(
                "CREATE TABLE a(s VARCHAR(10) CHARACTER SET {charset})"
            ))
            .unwrap();
        engine.execute("INSERT INTO a VALUES ('é')").unwrap();
        let bytes = crate::charset::encode_string(charset, "é").unwrap();
        let hex: String = bytes.iter().map(|b| format!("{b:02X}")).collect();
        engine
            .execute(&format!("INSERT INTO b VALUES (X'{hex}')"))
            .unwrap();
        for hint in ["HASH_JOIN(a,b)", "NO_HASH_JOIN(a,b)"] {
            for condition in ["a.s=b.v", "CONCAT(a.s,'')=b.v", "a.s=CONCAT(b.v,X'')"] {
                let sql = format!("SELECT /*+ {hint} */ COUNT(*) FROM a JOIN b ON {condition}");
                assert_eq!(
                    result(&engine, &sql).rows,
                    vec![vec![Value::Int(1)]],
                    "{charset}: {sql}"
                );
            }
        }
        engine.execute("DROP TABLE a").unwrap();
        engine.execute("DELETE FROM b").unwrap();
    }
}

#[test]
fn issue_31_unimplemented_uca_collations_are_explicitly_rejected() {
    let (_dir, engine) = fixture();
    for collation in [
        "utf8mb4_0900_as_ci",
        "utf8mb4_0900_as_cs",
        "utf8mb4_de_pb_0900_ai_ci",
        "utf8mb4_is_0900_ai_ci",
    ] {
        assert_eq!(
            engine
                .execute(&format!("SELECT 'é'<'f' COLLATE {collation}"))
                .unwrap_err()
                .code(),
            1235
        );
        assert_eq!(
            engine
                .execute(&format!(
                    "CREATE TABLE rejected(v VARCHAR(10) COLLATE {collation})"
                ))
                .unwrap_err()
                .code(),
            1235
        );
    }
}

#[test]
fn issue_30_union_all_shapes_decimal_scratch_values() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE t(x DECIMAL(31,30))").unwrap();
    engine.execute("INSERT INTO t VALUES (0)").unwrap();
    let output = result(&engine, "SELECT x FROM t UNION ALL SELECT x/0.1 FROM t");
    assert_eq!(output.rows.len(), 2);
    for row in output.rows {
        assert_eq!(
            row[0].to_display_string(),
            "0.000000000000000000000000000000"
        );
    }
}

#[test]
fn issue_33_derived_grouping_column_names() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE t(id INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    for column in ["g", "d.g"] {
        let sql =
            format!("SELECT {column},COUNT(*) FROM (SELECT id,id%2 g FROM t) d GROUP BY {column}");
        let output = result(&engine, &sql);
        assert_eq!(output.columns[0].name, "g", "{sql}");
    }
}

#[test]
fn issue_34_hex_lookbehind_respects_identifier_boundaries() {
    for prefix in ["éx", "$x", "@x", "abcx", "`éx`"] {
        let sql = format!("SELECT {prefix}'hello'");
        assert!(crate::parser::parse_statement(&sql).is_ok(), "{sql}");
    }
    assert!(crate::parser::parse_statement("SELECT X'hello'").is_err());
}

#[test]
fn issue_35_load_data_unicode_returns_error_without_panic() {
    for sql in ["LOAD DATA 表表", "LOAD DATA éééé", "LOAD DATA abc"] {
        assert!(crate::parser::parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn issue_30_stored_and_generated_decimal_string_scales() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE scaled(x DECIMAL(20,6),v DECIMAL(20,6) GENERATED ALWAYS AS(x/3) VIRTUAL,s DECIMAL(20,6) GENERATED ALWAYS AS(x/3) STORED)").unwrap();
    engine.execute("INSERT INTO scaled(x) VALUES(1)").unwrap();
    let output = result(&engine, "SELECT CAST(x AS CHAR),CAST(v AS CHAR),CAST(s AS CHAR),CONCAT('',v),CONCAT('',s) FROM scaled");
    assert_eq!(
        output.rows,
        vec![vec![
            Value::String("1.000000".into()),
            Value::String("0.333333".into()),
            Value::String("0.333333".into()),
            Value::String("0.333333".into()),
            Value::String("0.333333".into())
        ]]
    );
}
