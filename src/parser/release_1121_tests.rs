use super::*;

#[test]
fn parser_release_1121_rejects_lost_or_malformed_semantics() {
    for sql in [
        "SELECT 2 BETWEEN 1 = 1 AND 3",
        "SELECT 2 BETWEEN 1 IS NULL AND 3",
        "SELECT 1 NOT NULL",
        "SELECT a DISTINCT FROM t",
        "SELECT a ALL FROM t",
        "SELECT DISTINCT `a` DISTINCT FROM t",
        "SELECT DATE_ADD('2024-01-02',1)",
        "SELECT DATE_SUB('2024-01-02',INTERVAL(1,2))",
        "SELECT 1 --\0\n,2",
        "SELECT 1 #\0\n,2",
        "SELECT CAST(1 AS TINYINT)",
        "SELECT CAST('a' AS TEXT)",
        "SELECT CAST('a' AS VARCHAR(3))",
        "SELECT CAST(1 AS BLOB)",
        "SELECT CAST(1 AS VARBINARY(3))",
        "SELECT CAST(1 AS TIMESTAMP)",
        "SELECT CAST(1 AS NUMERIC(8,2))",
        "CREATE TABLE t(a INT) PARTITION BY RANGE(a)(PARTITION p VALUES LESS THAN(10) STORAGE DEFAULT)",
    ] {
        assert!(
            parse_statement(sql).is_err(),
            "{sql}: {:?}",
            parse_statement(sql)
        );
    }
}

#[test]
fn parser_release_1121_window_bound_order() {
    for frame in [
        "CURRENT ROW AND 1 PRECEDING",
        "1 FOLLOWING AND CURRENT ROW",
        "1 FOLLOWING AND 1 PRECEDING",
    ] {
        let sql = format!("SELECT SUM(a) OVER(ORDER BY a ROWS BETWEEN {frame}) FROM t");
        assert_eq!(
            parse_statement(&sql).unwrap_err().spec.code().get(),
            3586,
            "{sql}"
        );
    }
    for frame in [
        "2 PRECEDING AND 1 PRECEDING",
        "1 PRECEDING AND CURRENT ROW",
        "1 PRECEDING AND 2 PRECEDING",
    ] {
        assert!(parse_statement(&format!(
            "SELECT SUM(a) OVER(ORDER BY a ROWS BETWEEN {frame}) FROM t"
        ))
        .is_ok());
    }
}

#[test]
fn parser_release_1121_decimal_alias_and_uppercase_bit_identifier() {
    let Statement::Select(s) = parse_statement("SELECT 1.5abc,.5z,5.x").unwrap() else {
        panic!()
    };
    for (item, name) in s.projection.iter().zip(["abc", "z", "x"]) {
        assert!(
            matches!(item,SelectItem::Expr{alias:Some(a),..} if a==name),
            "{item:?}"
        );
    }
    let Statement::Select(s) = parse_statement("SELECT 0B0101").unwrap() else {
        panic!()
    };
    assert!(
        matches!(&s.projection[0], SelectItem::Expr{expr:Expr::Identifier(n),..} if n=="0B0101")
    );
    let sql = format!("SELECT '{}',1efoo", "E".repeat(4096));
    let Statement::Select(s) = parse_statement(&sql).unwrap() else {
        panic!()
    };
    assert!(
        matches!(&s.projection[1], SelectItem::Expr{expr:Expr::Identifier(n),..} if n=="1efoo")
    );
    let sql = format!("SELECT '{}',1.5abc", "1.".repeat(4096));
    let Statement::Select(s) = parse_statement(&sql).unwrap() else {
        panic!()
    };
    assert!(matches!(&s.projection[1], SelectItem::Expr{alias:Some(n),..} if n=="abc"));
}

#[test]
fn parser_release_1121_wide_decimal_aliases_keep_all_slots() {
    let sql = format!(
        "SELECT {}",
        (0..2048)
            .map(|n| format!("{n}.5a{n}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let Statement::Select(select) = parse_statement(&sql).unwrap() else {
        panic!("SELECT")
    };
    assert_eq!(select.projection.len(), 2048);
    assert!(
        matches!(&select.projection[2047], SelectItem::Expr { alias: Some(name), .. } if name == "a2047")
    );
    let Statement::Select(select) = parse_statement("SELECT 1efoo,1.5abc").unwrap() else {
        panic!("SELECT")
    };
    assert_eq!(select.projection.len(), 2);
    assert!(
        matches!(&select.projection[1], SelectItem::Expr { alias: Some(name), .. } if name == "abc")
    );
}

#[test]
fn parser_release_1121_foreign_key_match_is_explicit() {
    for kind in ["FULL", "PARTIAL"] {
        let sql =
            format!("CREATE TABLE c(a INT,b INT,FOREIGN KEY(a,b) REFERENCES p(a,b) MATCH {kind})");
        assert_eq!(
            parse_statement(&sql).unwrap_err().spec,
            crate::error::catalog::ER_NOT_SUPPORTED_YET
        );
    }
    assert!(
        parse_statement("CREATE TABLE c(a INT,FOREIGN KEY(a) REFERENCES p(a) MATCH SIMPLE)")
            .is_ok()
    );
}

#[test]
fn parser_release_1121_column_and_partition_storage_grammars_are_separate() {
    assert!(parse_statement("CREATE TABLE t(a INT STORAGE DEFAULT) PARTITION BY RANGE(a)(PARTITION p VALUES LESS THAN(10) STORAGE ENGINE InnoDB)").is_ok());
    assert!(parse_statement("ALTER TABLE t ADD COLUMN b INT STORAGE MEMORY").is_ok());
    assert!(parse_statement(
        "ALTER TABLE t ADD PARTITION(PARTITION p VALUES LESS THAN(10) STORAGE DEFAULT)"
    )
    .is_err());
}

#[test]
fn parser_release_1121_native_integer_aliases_and_catalog_intervals() {
    for target in ["INT", "INTEGER", "BIGINT", "SIGNED"] {
        assert!(parse_statement(&format!("SELECT CAST(1 AS {target})")).is_ok());
    }
    let expr = parse_expr_fragment("d + INTERVAL 1 DAY").unwrap();
    let canonical = expr_to_catalog_sql(&expr).unwrap();
    assert_eq!(parse_catalog_expr(&canonical).unwrap(), expr);
}
