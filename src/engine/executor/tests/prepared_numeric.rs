use super::*;

fn run(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    match e
        .execute_with_session(s, sql)
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResponse::ResultSet(rs) => rs
            .rows
            .iter()
            .map(|row| row.iter().map(Value::to_display_string).collect())
            .collect(),
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn prepared_division_precision_is_bound_but_sysvar_is_live() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    e.execute_with_session(&mut s, "SET div_precision_increment=4")
        .unwrap();
    e.execute_with_session(
        &mut s,
        "PREPARE p FROM 'SELECT 2/9,(2/9)*9,@@div_precision_increment'",
    )
    .unwrap();
    for precision in [0, 9, 0, 4] {
        e.execute_with_session(&mut s, &format!("SET div_precision_increment={precision}"))
            .unwrap();
        assert_eq!(
            run(&e, &mut s, "EXECUTE p"),
            vec![vec![
                "0.2222".to_string(),
                "2.0000".to_string(),
                precision.to_string()
            ]]
        );
        assert_eq!(s.prepared_div_precision_increment, None);
        let expected = if precision == 0 {
            "0"
        } else if precision == 9 {
            "0.222222222"
        } else {
            "0.2222"
        };
        assert_eq!(
            run(&e, &mut s, "SELECT 2/9"),
            vec![vec![expected.to_string()]]
        );
    }
}

#[test]
fn prepared_numeric_rebinds_only_after_dependent_ddl() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(n INT)",
        "INSERT INTO t VALUES(2)",
        "CREATE TABLE unrelated(n INT)",
        "SET div_precision_increment=4",
        "PREPARE p FROM 'WITH seed AS(SELECT n FROM t) SELECT n/9,(n/9)*9,@@div_precision_increment FROM seed'",
        "PREPARE a FROM 'SELECT AVG(n),@@div_precision_increment FROM t'",
        "SET div_precision_increment=0",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    for ddl in [None, Some("ALTER TABLE unrelated ADD extra INT")] {
        if let Some(ddl) = ddl {
            e.execute_with_session(&mut s, ddl).unwrap();
        }
        assert_eq!(
            run(&e, &mut s, "EXECUTE p"),
            vec![vec![
                "0.2222".to_string(),
                "2.0000".to_string(),
                "0".to_string()
            ]]
        );
        assert_eq!(
            run(&e, &mut s, "EXECUTE a"),
            vec![vec!["2.0000".to_string(), "0".to_string()]]
        );
    }
    e.execute_with_session(&mut s, "ALTER TABLE t ADD extra INT")
        .unwrap();
    for precision in [0, 9] {
        e.execute_with_session(&mut s, &format!("SET div_precision_increment={precision}"))
            .unwrap();
        assert_eq!(
            run(&e, &mut s, "EXECUTE p"),
            vec![vec![
                "0".to_string(),
                "0".to_string(),
                precision.to_string()
            ]]
        );
        assert_eq!(
            run(&e, &mut s, "EXECUTE a"),
            vec![vec!["2".to_string(), precision.to_string()]]
        );
    }
}

#[test]
fn prepared_numeric_view_reprepare_and_error_restore() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE vbase(n INT)",
        "INSERT INTO vbase VALUES(2)",
        "CREATE VIEW vp AS SELECT n/9 AS d FROM vbase",
        "SET div_precision_increment=4",
        "PREPARE p FROM 'SELECT d,@@div_precision_increment FROM vp'",
        "SET div_precision_increment=0",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    assert_eq!(
        run(&e, &mut s, "EXECUTE p"),
        vec![vec!["0.2222".to_string(), "0".to_string()]]
    );
    e.execute_with_session(&mut s, "ALTER VIEW vp AS SELECT n/3 AS d FROM vbase")
        .unwrap();
    assert_eq!(
        run(&e, &mut s, "EXECUTE p"),
        vec![vec!["0".to_string(), "0".to_string()]]
    );
    e.execute_with_session(&mut s, "SET div_precision_increment=9")
        .unwrap();
    assert_eq!(
        run(&e, &mut s, "EXECUTE p"),
        vec![vec!["0".to_string(), "9".to_string()]]
    );
    e.execute_with_session(&mut s, "DROP TABLE vbase").unwrap();
    assert!(e.execute_with_session(&mut s, "EXECUTE p").is_err());
    assert_eq!(s.prepared_div_precision_increment, None);
    assert_eq!(
        run(&e, &mut s, "SELECT 2/9"),
        vec![vec!["0.222222222".to_string()]]
    );
}

#[test]
fn numeric_parameter_reprepare_rebinds_precision_and_preserves_widened_domains() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "SET div_precision_increment=4",
        "PREPARE p FROM 'SELECT 2/9,?/9,@@div_precision_increment'",
        "SET div_precision_increment=0",
        "SET @v=2",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    assert_eq!(
        run(&e, &mut s, "EXECUTE p USING @v"),
        vec![vec![
            "0.2222".to_string(),
            "0.2222".to_string(),
            "0".to_string()
        ]]
    );
    e.execute_with_session(&mut s, "SET @v=2.5").unwrap();
    assert_eq!(
        run(&e, &mut s, "EXECUTE p USING @v"),
        vec![vec![
            "0".to_string(),
            "0.277777777000000000000000000000".to_string(),
            "0".to_string()
        ]]
    );
    e.execute_with_session(&mut s, "SET @v=2").unwrap();
    assert_eq!(
        run(&e, &mut s, "EXECUTE p USING @v"),
        vec![vec![
            "0".to_string(),
            "0.000000000000000000000000000000".to_string(),
            "0".to_string()
        ]]
    );
    e.execute_with_session(&mut s, "SET @v=CAST(2 AS DOUBLE)")
        .unwrap();
    let first = run(&e, &mut s, "EXECUTE p USING @v");
    e.execute_with_session(&mut s, "SET @v=2").unwrap();
    assert_eq!(run(&e, &mut s, "EXECUTE p USING @v"), first);
    assert_eq!(s.prepared_div_precision_increment, None);
    assert!(s.prepared_numeric_parameter_types.is_none());
}

const ORIGINAL_SUDOKU: &str = r#"WITH RECURSIVE
 input(sud) AS (VALUES('53..7....6..195....98....6.8...6...34..8.3..17...2...6.6....28....419..5....8..79')),
 digits(z, lp) AS (VALUES('1', 1) UNION ALL SELECT lp+1, lp+1 FROM digits WHERE lp<9),
 x(s, ind) AS (
 SELECT sud, instr(sud, '.') FROM input
 UNION ALL
 SELECT concat(substr(s, 1, ind-1), z, substr(s, ind+1)),
 instr(concat(substr(s, 1, ind-1), z, substr(s, ind+1)), '.')
 FROM x, digits AS z
 WHERE ind>0 AND NOT EXISTS (
 SELECT 1 FROM digits AS lp
 WHERE z.z = substr(s, ((ind-1)/9)*9 + lp, 1)
 OR z.z = substr(s, ((ind-1)%9) + (lp-1)*9 + 1, 1)
 OR z.z = substr(s, (((ind-1)/3) % 3)*3 + ((ind-1)/27)*27 + lp + ((lp-1)/3)*6, 1)))
SELECT s FROM x WHERE ind=0;
"#;

#[test]
fn original_slash_recursive_cte_keeps_prepared_zero_precision_after_session_change() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    e.execute_with_session(&mut s, "SET div_precision_increment=0")
        .unwrap();
    let prepare = format!(
        "PREPARE original FROM '{}'",
        ORIGINAL_SUDOKU.replace('\'', "''")
    );
    e.execute_with_session(&mut s, &prepare).unwrap();
    e.execute_with_session(&mut s, "SET div_precision_increment=4")
        .unwrap();
    assert_eq!(
        run(&e, &mut s, "EXECUTE original"),
        vec![vec![
            "534678912672195348198342567859761423426853791713924856961537284287419635345286179"
                .to_string()
        ]]
    );
    assert_eq!(
        run(&e, &mut s, "SELECT @@div_precision_increment,2/9"),
        vec![vec!["4".to_string(), "0.2222".to_string()]]
    );
}

#[test]
fn numeric_parameter_domains_do_not_cross_shared_plan_or_expression_caches() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE domain_cache(n INT)",
        "INSERT INTO domain_cache VALUES(2)",
        "SET div_precision_increment=0",
        "PREPARE a FROM 'SELECT n/9+?/9 FROM domain_cache'",
        "PREPARE b FROM 'SELECT n/9+?/9 FROM domain_cache'",
        "SET @v=2",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    assert_eq!(
        run(&e, &mut s, "EXECUTE a USING @v"),
        vec![vec!["0".to_string()]]
    );
    e.execute_with_session(&mut s, "SET @v=2.5").unwrap();
    assert_eq!(
        run(&e, &mut s, "EXECUTE b USING @v"),
        vec![vec!["0.277777777000000000000000000000".to_string()]]
    );
    e.execute_with_session(&mut s, "SET @v=2").unwrap();
    for _ in 0..3 {
        assert_eq!(
            run(&e, &mut s, "EXECUTE b USING @v"),
            vec![vec!["0.000000000000000000000000000000".to_string()]]
        );
        assert_eq!(
            run(&e, &mut s, "EXECUTE a USING @v"),
            vec![vec!["0".to_string()]]
        );
    }
}

#[test]
fn prepared_avg_parameter_has_float_domain_and_keeps_precision_binding() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "SET div_precision_increment=4",
        "PREPARE a FROM 'SELECT AVG(?),2/9,@@div_precision_increment'",
        "SET div_precision_increment=0",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    for input in ["2", "2.5", "CAST(2 AS DOUBLE)", "2"] {
        e.execute_with_session(&mut s, &format!("SET @v={input}"))
            .unwrap();
        let result = run(&e, &mut s, "EXECUTE a USING @v");
        assert_eq!(result[0][1..], ["0.2222", "0"]);
    }
    assert!(s.prepared_numeric_parameter_types.is_none());
}

#[test]
fn prepared_numeric_text_conversion_keeps_lazy_row_diagnostics() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE diagnostic_rows(n INT)",
        "INSERT INTO diagnostic_rows VALUES(1),(2),(3)",
        "SET div_precision_increment=4",
        "PREPARE p FROM 'SELECT ?/9 FROM diagnostic_rows'",
        "PREPARE unused FROM 'SELECT IF(1,7,?/9) FROM diagnostic_rows'",
        "PREPARE integer_read FROM 'SELECT ?+1,2/9'",
        "SET div_precision_increment=0",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    for (input, expected) in [
        ("'bad'", "0.0000"),
        ("'2x'", "0.2222"),
        ("'2.5x'", "0.2778"),
    ] {
        e.execute_with_session(&mut s, &format!("SET @v={input}"))
            .unwrap();
        assert_eq!(
            run(&e, &mut s, "EXECUTE p USING @v"),
            vec![vec![expected.to_string()]; 3]
        );
        let warnings = run(&e, &mut s, "SHOW WARNINGS");
        assert_eq!(warnings.len(), 3, "{input}: {warnings:?}");
        assert!(
            warnings
                .iter()
                .all(|w| w[1] == "1292" && w[2].contains("DECIMAL")),
            "{warnings:?}"
        );
        assert_eq!(
            run(&e, &mut s, "EXECUTE unused USING @v"),
            vec![vec!["7.0000".to_string()]; 3]
        );
        assert!(run(&e, &mut s, "SHOW WARNINGS").is_empty());
    }
    e.execute_with_session(&mut s, "SET @v='2.5x'").unwrap();
    assert_eq!(
        run(&e, &mut s, "EXECUTE integer_read USING @v"),
        vec![vec!["3".to_string(), "0.2222".to_string()]]
    );
    let warnings = run(&e, &mut s, "SHOW WARNINGS");
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0][2].contains("INTEGER"));
    e.execute_with_session(&mut s, "SET @v=NULL").unwrap();
    assert_eq!(
        run(&e, &mut s, "EXECUTE p USING @v"),
        vec![vec!["NULL".to_string()]; 3]
    );
    assert_eq!(s.prepared_div_precision_increment, None);
}

#[test]
fn prepared_precision_binding_does_not_require_numeric_parameter_contexts() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "SET div_precision_increment=4",
        "PREPARE p FROM 'SELECT LENGTH(?),2/9'",
        "SET div_precision_increment=0",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    for (input, length) in [("2", "1"), ("2.5", "3"), ("'abcde'", "5")] {
        e.execute_with_session(&mut s, &format!("SET @v={input}"))
            .unwrap();
        assert_eq!(
            run(&e, &mut s, "EXECUTE p USING @v"),
            vec![vec![length.to_string(), "0.2222".to_string()]]
        );
    }
}
