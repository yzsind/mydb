//! Regressions for the November 23 GitHub compatibility reports.
use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    let response = engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected rows: {sql}")
    };
    result
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|v| v.to_display_string()).collect())
        .collect()
}

#[test]
fn issue67_ngram_wildcards_keep_token_and_phrase_boundaries() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute(
            "CREATE TABLE ft(id INT PRIMARY KEY,doc TEXT,FULLTEXT KEY f(doc) WITH PARSER ngram)",
        )
        .unwrap();
    engine.execute("INSERT INTO ft VALUES(1,'hello world beautiful day'),(2,'the quick brown fox'),(3,'hello hello rusty world'),(4,'数据 数据库'),(5,'数 据 库')").unwrap();
    for (query, ids) in [
        ("rusty*", vec!["3"]),
        ("hello*", vec!["1", "3"]),
        ("wor*", vec!["1", "3"]),
        ("h*", vec!["1", "2", "3"]),
        ("he*", vec!["1", "2", "3"]),
        ("数据*", vec!["4"]),
        ("数据库*", vec!["4"]),
        ("+hello* -rusty*", vec!["1"]),
        ("helloworld*", vec![]),
    ] {
        let expected: Vec<Vec<String>> = ids.into_iter().map(|id| vec![id.into()]).collect();
        for predicate in [
            format!("MATCH(doc) AGAINST('{query}' IN BOOLEAN MODE)"),
            format!("MATCH(doc) AGAINST('{query}' IN BOOLEAN MODE) > 0"),
        ] {
            assert_eq!(
                rows(
                    &engine,
                    &mut session,
                    &format!("SELECT id FROM ft WHERE {predicate} ORDER BY id")
                ),
                expected,
                "{predicate}"
            );
        }
    }
    engine
        .execute("UPDATE ft SET doc='different text' WHERE id=3")
        .unwrap();
    assert!(rows(
        &engine,
        &mut session,
        "SELECT id FROM ft WHERE MATCH(doc) AGAINST('rusty*' IN BOOLEAN MODE)"
    )
    .is_empty());
}

#[test]
fn issue69_leap_seconds_never_normalize_into_valid_datetimes() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    engine
        .execute("CREATE TABLE dt(id INT PRIMARY KEY,d DATETIME(6),t TIMESTAMP(6))")
        .unwrap();
    for text in [
        "2024-06-15 23:59:60",
        "2016-12-31T23:59:60.123456",
        "2024-06-15 23:59:60+00:00",
    ] {
        for column in ["d", "t"] {
            let sql = format!("INSERT INTO dt(id,{column}) VALUES(1,'{text}')");
            assert_eq!(
                engine
                    .execute_with_session(&mut session, &sql)
                    .unwrap_err()
                    .code(),
                1292,
                "{sql}"
            );
        }
        assert_eq!(
            rows(
                &engine,
                &mut session,
                &format!("SELECT CAST('{text}' AS DATETIME)")
            ),
            vec![vec!["NULL"]],
            "{text}"
        );
        assert!(session.warning_count() > 0, "{text}");
    }
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM dt"),
        vec![vec!["0"]]
    );
    engine
        .execute_with_session(
            &mut session,
            "INSERT IGNORE INTO dt(id,d) VALUES(2,'2024-06-15 23:59:60')",
        )
        .unwrap();
    assert!(session.warning_count() > 0);
    assert_eq!(
        rows(&engine, &mut session, "SELECT d FROM dt WHERE id=2"),
        vec![vec!["0000-00-00 00:00:00"]]
    );
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO dt(id,d) VALUES(3,'2024-06-15 23:59:59.999999')",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT d FROM dt WHERE id=3"),
        vec![vec!["2024-06-15 23:59:59.999999"]]
    );
}

#[test]
fn issue81_collate_validates_binary_sources_before_execution() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute_with_session(&mut session, "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        .unwrap();
    engine
        .execute("CREATE TABLE binary_sources(b VARBINARY(8),c VARCHAR(8),bt BIT(8))")
        .unwrap();
    engine
        .execute("INSERT INTO binary_sources(b,c) VALUES(X'61','a')")
        .unwrap();
    for sql in [
        "SELECT x'61' COLLATE utf8mb4_bin",
        "SELECT UPPER(x'c3bf' COLLATE utf8mb4_bin)",
        "SELECT _binary 'a' COLLATE utf8mb4_bin",
        "SELECT CAST('a' AS BINARY) COLLATE utf8mb4_bin",
        "SELECT NULL COLLATE utf8mb4_bin",
        "SELECT b COLLATE utf8mb4_bin FROM binary_sources",
        "SELECT bt COLLATE utf8mb4_bin FROM binary_sources",
        "PREPARE bad FROM 'SELECT x''c3bf'' COLLATE utf8mb4_bin'",
        "PREPARE bad_column FROM 'SELECT b COLLATE utf8mb4_bin FROM binary_sources'",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut session, sql)
                .unwrap_err()
                .code(),
            1253,
            "{sql}"
        );
    }
    assert_eq!(rows(&engine, &mut session, "SELECT 1 COLLATE utf8mb4_bin,1.5 COLLATE utf8mb4_bin,CONVERT(x'c3bf' USING utf8mb4) COLLATE utf8mb4_bin"), vec![vec!["1", "1.5", "ÿ"]]);
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT c COLLATE utf8mb4_bin FROM binary_sources"
        ),
        vec![vec!["a"]]
    );
    engine
        .execute_with_session(&mut session, "SET @binary_value=X'61'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "SELECT @binary_value COLLATE utf8mb4_bin")
            .unwrap_err()
            .code(),
        1253
    );
    engine
        .execute_with_session(
            &mut session,
            "PREPARE bound_collate FROM 'SELECT ? COLLATE utf8mb4_bin'",
        )
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "EXECUTE bound_collate USING @binary_value"
        ),
        vec![vec!["a"]]
    );
    for (value, expected) in [("'a'", "a"), ("NULL", "NULL"), ("1", "1"), ("X'61'", "a")] {
        engine
            .execute_with_session(&mut session, &format!("SET @binary_value={value}"))
            .unwrap();
        assert_eq!(
            rows(
                &engine,
                &mut session,
                "EXECUTE bound_collate USING @binary_value"
            ),
            vec![vec![expected]],
            "{value}"
        );
    }
    for collation in ["utf8mb4_bin", "latin1_bin", "binary"] {
        let sql = format!("SELECT ? COLLATE {collation}");
        let planned = engine.prepare_cached(&mut session, &sql, 1).unwrap();
        for value in [
            Value::Bytes(b"a".to_vec()),
            Value::String("b".into()),
            Value::Null,
            Value::Int(7),
        ] {
            let expected = value.to_display_string();
            let response = crate::engine::params::with_params(vec![value], || {
                engine
                    .execute_prepared_statement(&mut session, &sql, (*planned).clone())
                    .expect("prepared COLLATE execution")
            })
            .unwrap()
            .materialize()
            .unwrap();
            let QueryResponse::ResultSet(result) = response else {
                panic!("{sql}")
            };
            assert_eq!(result.rows[0][0].to_display_string(), expected, "{sql}");
        }
    }
}

#[test]
fn issue69_superfluous_datetime_delimiter_is_accepted_with_warning() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    engine
        .execute("CREATE TABLE flexible_time(d DATETIME)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO flexible_time VALUES('2024-06-15 -12:34:56')",
        )
        .unwrap();
    assert!(session.warnings.iter().any(|w| w.code() == 4096));
    assert_eq!(
        rows(&engine, &mut session, "SELECT d FROM flexible_time"),
        vec![vec!["2024-06-15 12:34:56"]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT CAST('2024-06-15 -12:34:56' AS DATETIME)"
        ),
        vec![vec!["2024-06-15 12:34:56"]]
    );
    assert!(session.warnings.iter().any(|w| w.code() == 4096));
}

#[test]
fn issue73_string_extrema_preserve_mysql_tie_operands() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute_with_session(&mut session, "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        .unwrap();
    assert_eq!(rows(&engine, &mut session, "SELECT GREATEST('a','A','9'),LEAST('b','B'),GREATEST('A','a'),LEAST('B','b'),GREATEST(1,2,2)"), vec![vec!["A","b","a","B","2"]]);
    engine
        .execute("CREATE TABLE extrema(id INT PRIMARY KEY,a VARCHAR(8),b VARCHAR(8))")
        .unwrap();
    engine
        .execute("INSERT INTO extrema VALUES(1,'a','A'),(2,'é','e'),(3,NULL,'a')")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT GREATEST(a,b),LEAST(a,b) FROM extrema ORDER BY id"
        ),
        vec![vec!["A", "a"], vec!["e", "é"], vec!["NULL", "NULL"]]
    );
}

#[test]
fn issue74_exhausted_auto_increment_uses_the_type_domain() {
    let (_dir, engine, mut session) = setup();
    for (index, kind, max) in [
        (0, "TINYINT", "127"),
        (1, "SMALLINT UNSIGNED", "65535"),
        (2, "INT", "2147483647"),
        (3, "BIGINT", "9223372036854775807"),
    ] {
        let table = format!("mx{index}");
        engine
            .execute(&format!(
                "CREATE TABLE {table}(id {kind} PRIMARY KEY AUTO_INCREMENT,v INT)"
            ))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {table} VALUES({max},1)"))
            .unwrap();
        assert_eq!(
            engine
                .execute_with_session(&mut session, &format!("INSERT INTO {table}(v) VALUES(2)"))
                .unwrap_err()
                .code(),
            1062
        );
        engine.execute(&format!("DELETE FROM {table}")).unwrap();
        engine
            .execute_with_session(&mut session, &format!("INSERT INTO {table}(v) VALUES(3)"))
            .unwrap();
        assert_eq!(
            rows(&engine, &mut session, &format!("SELECT id FROM {table}")),
            vec![vec![max]]
        );
    }
    engine
        .execute("CREATE TABLE umax(id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT)")
        .unwrap();
    engine
        .execute("INSERT INTO umax VALUES(18446744073709551615)")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "INSERT INTO umax VALUES(NULL)")
            .unwrap_err()
            .code(),
        1467
    );
    engine
        .execute("CREATE TABLE crossing(id TINYINT PRIMARY KEY AUTO_INCREMENT,v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO crossing VALUES(126,1)")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "INSERT INTO crossing(v) VALUES(2),(3)")
            .unwrap_err()
            .code(),
        1467
    );
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM crossing"),
        vec![vec!["1"]]
    );
    engine
        .execute_with_session(
            &mut session,
            "SET auto_increment_increment=10,auto_increment_offset=3",
        )
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "INSERT INTO mx0(v) VALUES(4)")
            .unwrap_err()
            .code(),
        1264
    );
}

#[test]
fn issue75_integer_assignment_distinguishes_truncation_from_invalid_input() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    engine
        .execute("CREATE TABLE nums(i INT,u BIGINT UNSIGNED)")
        .unwrap();
    for column in ["i", "u"] {
        for (text, code) in [
            ("12x", 1265),
            ("abc", 1366),
            ("999999999999999999999999999999", 1264),
        ] {
            let sql = format!("INSERT INTO nums({column}) VALUES('{text}')");
            assert_eq!(
                engine
                    .execute_with_session(&mut session, &sql)
                    .unwrap_err()
                    .code(),
                code,
                "{sql}"
            );
        }
    }
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO nums(i) VALUES('12x')")
        .unwrap();
    assert!(session
        .warnings
        .iter()
        .any(|warning| warning.code() == 1265));
    assert_eq!(
        rows(&engine, &mut session, "SELECT i FROM nums"),
        vec![vec!["12"]]
    );
}

#[test]
fn issue76_show_tables_like_metadata_matches_execution_and_prepare() {
    let (_dir, engine, mut session) = setup();
    engine.execute("CREATE TABLE issue_table(id INT)").unwrap();
    for full in ["", "FULL "] {
        for (database, pattern, heading) in [
            ("", "issue_%", "Tables_in_test"),
            (
                " FROM INFORMATION_SCHEMA",
                "TABLES",
                "Tables_in_information_schema",
            ),
            (
                " FROM PERFORMANCE_SCHEMA",
                "threads",
                "Tables_in_performance_schema",
            ),
        ] {
            let sql = format!("SHOW {full}TABLES{database} LIKE '{pattern}'");
            let prepared = engine
                .describe_prepared_statement(&mut session, &sql)
                .unwrap();
            let result = engine
                .execute_with_session(&mut session, &sql)
                .unwrap()
                .materialize()
                .unwrap();
            let QueryResponse::ResultSet(result) = result else {
                panic!("{sql}")
            };
            assert_eq!(
                result.columns[0].name,
                format!("{heading} ({pattern})"),
                "{sql}"
            );
            assert_eq!(prepared, result.columns, "{sql}");
            assert_eq!(
                result.columns.len(),
                if full.is_empty() { 1 } else { 7 },
                "{sql}"
            );
            assert_eq!(result.rows.len(), 1, "{sql}");
        }
    }
    engine
        .execute_with_session(
            &mut session,
            "PREPARE st FROM 'SHOW TABLES LIKE ''issue_%'''",
        )
        .unwrap();
    let result = engine
        .execute_with_session(&mut session, "EXECUTE st")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(result.columns()[0].name, "Tables_in_test (issue_%)");
}

#[test]
fn issue77_show_create_keeps_collations_and_numeric_defaults() {
    let (_dir, engine, mut session) = setup();
    engine.execute("CREATE TABLE ddl(v VARCHAR(10),i INT DEFAULT 0,f DOUBLE DEFAULT 1.25,b BIT(3) DEFAULT b'010',n INT DEFAULT NULL) COLLATE utf8mb4_0900_ai_ci").unwrap();
    engine
        .execute("ALTER TABLE ddl CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    let ddl = rows(&engine, &mut session, "SHOW CREATE TABLE ddl")[0][1].clone();
    for fragment in [
        "`v` varchar(10) COLLATE utf8mb4_bin DEFAULT NULL",
        "`i` int DEFAULT '0'",
        "`f` double DEFAULT '1.25'",
        "DEFAULT b'10'",
        "`n` int DEFAULT NULL",
    ] {
        assert!(ddl.contains(fragment), "{fragment}: {ddl}");
    }
    let restored = ddl.replacen("`ddl`", "`ddl_copy`", 1);
    engine.execute(&restored).unwrap();
    engine
        .execute("INSERT INTO ddl_copy(v) VALUES('a')")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT i,f,n,COLLATION(v) FROM ddl_copy"
        ),
        vec![vec!["0", "1.25", "NULL", "utf8mb4_bin"]]
    );
}

#[test]
fn issue78_constraint_errors_identify_input_columns_and_rows() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    engine.execute("CREATE TABLE errs(id INT PRIMARY KEY,ti TINYINT,s VARCHAR(3),en ENUM('a','b'),g INT AS(id+1))").unwrap();
    for (sql, fragments) in [
        (
            "INSERT INTO errs(id,ti) VALUES(1,1),(2,128)",
            vec!["'ti'", "at row 2"],
        ),
        (
            "INSERT INTO errs(id,en) VALUES(1,'c')",
            vec!["'en'", "at row 1"],
        ),
        (
            "INSERT INTO errs(id,s) VALUES(1,'long')",
            vec!["'s'", "at row 1"],
        ),
        ("INSERT INTO errs(id,g) VALUES(1,3)", vec!["'g'", "'errs'"]),
        ("INSERT INTO errs(id) VALUES(1),(2,3)", vec!["at row 2"]),
    ] {
        let err = engine.execute_with_session(&mut session, sql).unwrap_err();
        for fragment in fragments {
            assert!(err.message().contains(fragment), "{sql}: {err:?}");
        }
    }
    engine
        .execute("CREATE TABLE alter_err(id INT PRIMARY KEY,s VARCHAR(10))")
        .unwrap();
    engine
        .execute("INSERT INTO alter_err VALUES(1,'12'),(2,'abc')")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "ALTER TABLE alter_err MODIFY s INT")
        .unwrap_err();
    for fragment in ["'abc'", "'s'", "at row 2"] {
        assert!(err.message().contains(fragment), "{err:?}");
    }
    assert_eq!(
        rows(&engine, &mut session, "SELECT s FROM alter_err ORDER BY id"),
        vec![vec!["12"], vec!["abc"]]
    );
}

#[test]
fn issue79_ok_info_uses_statement_shape_and_mysql_separators() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute("CREATE TABLE info(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO info VALUES(1)")
        .unwrap();
    assert!(crate::engine::api::peek_statement_info().is_empty());
    engine
        .execute_with_session(&mut session, "INSERT IGNORE INTO info VALUES(1)")
        .unwrap();
    assert!(crate::engine::api::peek_statement_info().is_empty());
    engine
        .execute_with_session(&mut session, "INSERT INTO info VALUES(2),(3)")
        .unwrap();
    assert_eq!(
        crate::engine::api::peek_statement_info(),
        "Records: 2  Duplicates: 0  Warnings: 0"
    );
    engine
        .execute_with_session(&mut session, "PREPARE p FROM 'SELECT 1'")
        .unwrap();
    assert_eq!(
        crate::engine::api::peek_statement_info(),
        "Statement prepared"
    );
    engine
        .execute_with_session(&mut session, "ALTER TABLE info ADD KEY k(id)")
        .unwrap();
    assert_eq!(
        crate::engine::api::peek_statement_info(),
        "Records: 0  Duplicates: 0  Warnings: 0"
    );
}

#[test]
fn issue80_conversion_warnings_keep_expression_occurrences() {
    let (_dir, engine, mut session) = setup();
    engine
        .execute("CREATE TABLE warn(id INT PRIMARY KEY,s VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO warn VALUES(1,'200abc')")
        .unwrap();
    rows(&engine, &mut session, "SELECT id FROM warn WHERE s=100");
    assert_eq!(session.warning_count(), 1);
    assert!(session
        .warnings
        .iter()
        .next()
        .unwrap()
        .message()
        .contains("DOUBLE"));
    rows(
        &engine,
        &mut session,
        "SELECT SEC_TO_TIME(8385959),SEC_TO_TIME(-8385959)",
    );
    assert_eq!(session.warning_count(), 2);
    assert!(session.warnings.iter().all(|w| w.code() == 1292));
    rows(
        &engine,
        &mut session,
        "SELECT SEC_TO_TIME(8385959),SEC_TO_TIME(8385959)",
    );
    assert_eq!(session.warning_count(), 2);
    rows(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM information_schema.processlist",
    );
    assert!(session.warnings.iter().any(|w| w.code() == 1287));
    rows(
        &engine,
        &mut session,
        "SELECT SEC_TO_TIME(3020399),SEC_TO_TIME(1.25)",
    );
    assert_eq!(session.warning_count(), 0);
}

#[test]
fn issue82_pi_metadata_retains_scale_without_rounding_the_value() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "SELECT PI() AS p,1e0/3 AS d",
        "SELECT p,d FROM (SELECT PI() AS p,1e0/3 AS d) sub",
    ] {
        let result = engine
            .execute_with_session(&mut session, sql)
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("{sql}")
        };
        assert_eq!(result.columns[0].source.decimals, Some(6), "{sql}");
        assert_eq!(result.columns[1].source.decimals, Some(31), "{sql}");
        assert_eq!(result.rows[0][0], Value::Double(std::f64::consts::PI));
    }
}
