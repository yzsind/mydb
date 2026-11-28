//! SQL contracts for the migration compatibility reports #108 through #123.
use super::*;

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
        .materialize()
        .unwrap()
    else {
        panic!("expected result set: {sql}")
    };
    result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| value.to_display_string())
                .collect()
        })
        .collect()
}

#[test]
fn issue108_introduced_literal_defaults_survive_ddl_and_reopen() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    {
        let engine = open_relaxed_test_engine(dir.path());
        let mut session = SessionContext::default();
        engine.execute("CREATE TABLE introduced(id INT PRIMARY KEY, s VARCHAR(32) DEFAULT _utf8mb4'默认值', l VARCHAR(8) CHARACTER SET latin1 DEFAULT _latin1 X'E9', b VARBINARY(8) DEFAULT _binary X'00FF')").unwrap();
        engine
            .execute("INSERT INTO introduced(id) VALUES(1)")
            .unwrap();
        engine
            .execute("ALTER TABLE introduced ADD COLUMN a VARCHAR(32) DEFAULT _utf8mb4'追加'")
            .unwrap();
        engine
            .execute("ALTER TABLE introduced ALTER COLUMN s SET DEFAULT _utf8mb4'更新'")
            .unwrap();
        engine.execute("ALTER TABLE introduced MODIFY COLUMN l VARCHAR(8) CHARACTER SET latin1 DEFAULT _latin1 X'F1'").unwrap();
        engine
            .execute(
                "ALTER TABLE introduced CHANGE COLUMN b b VARBINARY(8) DEFAULT _binary X'FF00'",
            )
            .unwrap();
        engine
            .execute("INSERT INTO introduced(id) VALUES(2)")
            .unwrap();
        let expected = vec![
            vec!["1", "默认值", "E9", "00FF", "追加"],
            vec!["2", "更新", "F1", "FF00", "追加"],
        ];
        assert_eq!(
            rows(
                &engine,
                &mut session,
                "SELECT id,s,HEX(l),HEX(b),a FROM introduced ORDER BY id"
            ),
            expected
        );
        let ddl = rows(&engine, &mut session, "SHOW CREATE TABLE introduced")[0][1].clone();
        engine
            .execute(&ddl.replacen("`introduced`", "`introduced_copy`", 1))
            .unwrap();
        engine
            .execute("INSERT INTO introduced_copy(id) VALUES(2)")
            .unwrap();
        assert_eq!(
            rows(
                &engine,
                &mut session,
                "SELECT id,s,HEX(l),HEX(b),a FROM introduced_copy"
            ),
            vec![expected[1].clone()]
        );
        engine.checkpoint().unwrap();
    }
    let engine = open_relaxed_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("INSERT INTO introduced(id) VALUES(3)")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT s,HEX(l),HEX(b),a FROM introduced WHERE id=3"
        ),
        vec![vec!["更新", "F1", "FF00", "追加"]]
    );
}

#[test]
fn issue108_introducers_keep_charset_validation_and_default_restrictions() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (sql, code) in [
        ("CREATE TABLE bad(c VARCHAR(8) DEFAULT _unknown'x')", 1115),
        (
            "CREATE TABLE bad(c VARCHAR(8) DEFAULT _utf8mb4 X'FF')",
            1300,
        ),
        ("CREATE TABLE bad(c VARCHAR(8) DEFAULT LOWER('ABC'))", 1067),
        (
            "CREATE TABLE bad(c VARCHAR(8) DEFAULT CONVERT('abc' USING utf8mb4))",
            1067,
        ),
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), code, "{sql}");
    }
    engine
        .execute("CREATE TABLE national(c VARCHAR(8) DEFAULT N'中文')")
        .unwrap();
    engine.execute("INSERT INTO national VALUES() ").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT c FROM national"),
        vec![vec!["中文"]]
    );
}

#[test]
fn issue109_decimal_synonyms_preserve_unsigned_zerofill_and_bounds() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_ALL_TABLES'")
        .unwrap();
    for (alias_index, alias) in ["DECIMAL", "NUMERIC", "DEC"].iter().enumerate() {
        for (suffix_index, suffix) in [
            "",
            " UNSIGNED",
            " ZEROFILL",
            " UNSIGNED ZEROFILL",
            " SIGNED",
            " ZEROFILL UNSIGNED",
        ]
        .iter()
        .enumerate()
        {
            let table = format!("dec_{alias_index}_{suffix_index}");
            let ddl = format!("CREATE TABLE {table}(c {alias}(5,2){suffix} DEFAULT 1.25)");
            engine
                .execute(&ddl)
                .unwrap_or_else(|err| panic!("{ddl}: {err:?}"));
            engine
                .execute(&format!("INSERT INTO {table} VALUES() "))
                .unwrap();
            let unsigned = suffix.contains("UNSIGNED") || suffix.contains("ZEROFILL");
            let negative = engine
                .execute_with_session(&mut session, &format!("INSERT INTO {table} VALUES(-1.25)"));
            if unsigned {
                assert_eq!(negative.unwrap_err().code(), 1264, "{ddl}");
            } else {
                negative.unwrap();
            }
            assert_eq!(
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("INSERT INTO {table} VALUES(1000)")
                    )
                    .unwrap_err()
                    .code(),
                1264,
                "{ddl}"
            );
            let create = rows(&engine, &mut session, &format!("SHOW CREATE TABLE {table}"))[0][1]
                .to_ascii_lowercase();
            assert_eq!(create.contains("unsigned"), unsigned, "{create}");
            assert_eq!(
                create.contains("zerofill"),
                suffix.contains("ZEROFILL"),
                "{create}"
            );
            engine
                .execute(&format!(
                    "ALTER TABLE {table} MODIFY COLUMN c {alias}(6,3){suffix}"
                ))
                .unwrap();
        }
    }
    for ddl in [
        "CREATE TABLE bad(c VARCHAR(8) UNSIGNED)",
        "CREATE TABLE bad(c NUMERIC(5,2) UNSIGNED UNSIGNED)",
        "CREATE TABLE bad(c NUMERIC(5,2) DEFAULT 1 UNSIGNED)",
    ] {
        assert!(engine.execute(ddl).is_err(), "{ddl}");
    }
}
