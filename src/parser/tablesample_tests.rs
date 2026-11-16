use super::*;

#[test]
fn tablesample_defaults_aliases_expressions_and_lexical_parameters() {
    for (method, expected) in [
        ("", SampleMethod::System),
        ("SYSTEM", SampleMethod::System),
        ("BLOCK", SampleMethod::System),
        ("BERNOULLI", SampleMethod::Bernoulli),
        ("ROW", SampleMethod::Bernoulli),
    ] {
        let Statement::Select(select) = parse_statement(&format!("SELECT ? FROM t AS x TABLESAMPLE {method} (? + 1 ROWS) REPEATABLE (? * 3) WHERE x.id = ?")).unwrap() else { panic!() };
        let sample = select.from[0].sample.as_ref().unwrap();
        assert_eq!(sample.method, expected);
        assert_eq!(sample.unit, SampleUnit::Rows);
        assert!(sample.binding_id > 0);
        assert_eq!(visit::next_select_parameter_index(&select), 4);
        assert!(
            matches!(sample.amount, Expr::Binary { ref left, .. } if **left == Expr::Placeholder(1))
        );
        assert!(
            matches!(sample.seed, Some(Expr::Binary { ref left, .. }) if **left == Expr::Placeholder(2))
        );
    }
    let Statement::Select(select) = parse_statement("SELECT * FROM t TABLESAMPLE (1.5)").unwrap()
    else {
        panic!()
    };
    let sample = select.from[0].sample.as_ref().unwrap();
    assert_eq!(
        (sample.method, sample.unit),
        (SampleMethod::System, SampleUnit::Percent)
    );
}

#[test]
fn tablesample_nested_bindings_round_trip_and_unsupported_forms() {
    let sql = "SELECT * FROM t a TABLESAMPLE (10) REPEATABLE (-2.5) JOIN t b TABLESAMPLE ROW (7 ROWS) REPEATABLE (CAST(? AS SIGNED)) ON a.id=b.id";
    let Statement::Select(select) = parse_statement(sql).unwrap() else {
        panic!()
    };
    let a = select.from[0].sample.as_ref().unwrap();
    let b = select.joins[0].table.sample.as_ref().unwrap();
    assert_ne!(a.binding_id, b.binding_id);
    assert!(parse_statement(&format!("SELECT * FROM t {}", b.to_sql())).is_ok());
    for sql in [
        "SELECT * FROM t SAMPLE (10)",
        "SELECT * FROM t TABLESAMPLE 10",
        "SELECT * FROM t TABLESAMPLE (10) SEED (42)",
        "SELECT * FROM t TABLESAMPLE (BUCKET 1 OUT OF 3)",
        "SELECT * FROM (SELECT 1) s TABLESAMPLE (10)",
        "UPDATE t TABLESAMPLE (10) SET id=1",
        "SELECT * FROM generate_series(1,10) TABLESAMPLE (10)",
    ] {
        assert!(
            !matches!(
                parse_statement(sql),
                Ok(Statement::Select(_)) | Ok(Statement::Update(_))
            ),
            "{sql}"
        );
    }
    assert!(matches!(
        parse_statement("SELECT 'TABLESAMPLE (10) REPEATABLE (?)'"),
        Ok(Statement::Select(_))
    ));
}
