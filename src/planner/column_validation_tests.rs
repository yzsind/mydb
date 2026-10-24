use super::*;

fn binding_scope() -> ColumnScope<'static> {
    ColumnScope {
        tables: vec![ScopeTable {
            name: "t".into(),
            alias: None,
            columns: vec!["a".into(), "b".into()],
            wildcard_columns: vec!["a".into(), "b".into()],
            visible: true,
        }],
        outer: None,
    }
}

#[test]
fn column_validation_preserves_scalar_coverage_and_query_alias_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let scope = binding_scope();
    let aliases = HashSet::from(["output_alias".to_string()]);
    for sql in [
        "a + -b",
        "t.a IS NOT NULL",
        "a IN (1, b)",
        "a BETWEEN b AND output_alias",
        "a LIKE b",
        "(a, b, output_alias)",
        "CASE a WHEN b THEN output_alias ELSE a END",
        "ABS(a)",
        "get_format(DATE, a)",
        "timestampadd(DAY, a, b)",
        "timestampdiff(DAY, a, b)",
        "a IN (SELECT t.b)",
        "EXISTS (SELECT t.a)",
        "(SELECT t.b)",
        "a = ANY (SELECT t.b)",
        "DEFAULT(a)",
        "SUM(a) OVER (PARTITION BY b ORDER BY output_alias ROWS 1 PRECEDING)",
    ] {
        let expr = crate::parser::parse_expr_fragment(sql).unwrap();
        validate_expr_columns(engine.db_for_tests(), &expr, &scope, Some(&aliases), true)
            .unwrap_or_else(|err| panic!("{sql}: {err}"));
    }
    for (sql, code, name) in [
        ("a + missing", 1054, "missing"),
        ("ABS(missing)", 1054, "missing"),
        ("get_format(DATE, missing)", 1054, "missing"),
        ("timestampadd(DAY, missing, b)", 1054, "missing"),
        ("(SELECT output_alias)", 1054, "output_alias"),
        ("DEFAULT(output_alias)", 1364, "output_alias"),
        ("other.a", 1054, "a"),
    ] {
        let expr = crate::parser::parse_expr_fragment(sql).unwrap();
        let err = validate_expr_columns(engine.db_for_tests(), &expr, &scope, Some(&aliases), true)
            .unwrap_err();
        assert_eq!(err.code(), code, "{sql}: {err}");
        assert!(err.to_string().contains(name), "{sql}: {err}");
    }
}

#[test]
fn column_validation_keeps_first_error_across_case_and_query_hooks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let scope = binding_scope();
    for (sql, first) in [
        (
            "CASE WHEN a THEN missing_result WHEN missing_condition THEN b END",
            "missing_condition",
        ),
        (
            "missing_left IN (SELECT missing_right FROM nonexistent)",
            "missing_left",
        ),
        (
            "missing_left = ANY (SELECT missing_right FROM nonexistent)",
            "missing_left",
        ),
        ("missing_left + (SELECT 1)", "missing_left"),
        ("ABS(missing_left) + missing_right", "missing_left"),
    ] {
        let expr = crate::parser::parse_expr_fragment(sql).unwrap();
        let err =
            validate_expr_columns(engine.db_for_tests(), &expr, &scope, None, true).unwrap_err();
        assert_eq!(err.code(), 1054, "{sql}: {err}");
        assert!(err.to_string().contains(first), "{sql}: {err}");
    }
}

#[test]
fn column_validation_window_frames_exclude_aliases_and_hidden_sources() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut scope = binding_scope();
    scope.tables.push(ScopeTable {
        name: "hidden".into(),
        alias: None,
        columns: vec!["hidden_col".into()],
        wildcard_columns: vec![],
        visible: false,
    });
    let aliases = HashSet::from(["output_alias".to_string()]);
    for (arg, bound, first) in [
        ("a", "output_alias", "output_alias"),
        ("a", "hidden_col", "hidden_col"),
        ("missing_arg", "missing_bound", "missing_arg"),
    ] {
        let mut expr =
            crate::parser::parse_expr_fragment("SUM(a) OVER (ROWS 1 PRECEDING)").unwrap();
        let Expr::WindowFunction { args, spec, .. } = &mut expr else {
            panic!("window expression");
        };
        let crate::parser::WindowSpec { frame, .. } = spec.as_mut();

        args[0] = Expr::Identifier(arg.into());
        frame.as_mut().unwrap().start =
            crate::parser::WindowFrameBound::Preceding(Box::new(Expr::Identifier(bound.into())));
        let err =
            validate_expr_columns(engine.db_for_tests(), &expr, &scope, Some(&aliases), false)
                .unwrap_err();
        assert_eq!(err.code(), 1054);
        assert!(err.to_string().contains(first), "{err}");
    }
}

#[test]
fn wildcard_binding_preserves_aliases_column_order_and_invisible_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wildcard_source (a INT, hidden INT INVISIBLE, b INT)")
        .unwrap();
    let Statement::Select(mut select) =
        parse_statement("SELECT *, x.*, 7 AS literal FROM wildcard_source x").unwrap()
    else {
        panic!("SELECT");
    };
    assert_eq!(
        query_output_column_names(engine.db_for_tests(), &select).unwrap(),
        ["a", "b", "a", "b", "literal"]
    );
    freeze_select_wildcards(engine.db_for_tests(), &mut select).unwrap();
    let refs: Vec<_> = select
        .projection
        .iter()
        .take(4)
        .map(|item| match item {
            SelectItem::Expr {
                expr: Expr::CompoundIdentifier(parts),
                alias,
            } => (parts.clone(), alias.clone()),
            _ => panic!("expanded wildcard"),
        })
        .collect();
    assert_eq!(
        refs,
        ["a", "b", "a", "b"].map(|name| (vec!["x".into(), name.into()], Some(name.into())))
    );
    validate_select_column_references(engine.db_for_tests(), &select).unwrap();
    for sql in [
        "SELECT X.* FROM wildcard_source x",
        "SELECT y.*, x.* FROM wildcard_source x JOIN wildcard_source y ON x.a = y.a",
    ] {
        let Statement::Select(mut select) = parse_statement(sql).unwrap() else {
            panic!("SELECT");
        };
        let before = query_output_column_names(engine.db_for_tests(), &select).unwrap();
        validate_select_column_references(engine.db_for_tests(), &select).unwrap();
        freeze_select_wildcards(engine.db_for_tests(), &mut select).unwrap();
        assert_eq!(
            query_output_column_names(engine.db_for_tests(), &select).unwrap(),
            before
        );
    }
    let Statement::Select(select) =
        parse_statement("SELECT wildcard_source.* FROM wildcard_source x").unwrap()
    else {
        panic!("SELECT");
    };
    assert_eq!(
        validate_select_column_references(engine.db_for_tests(), &select)
            .unwrap_err()
            .code(),
        1054
    );
}
