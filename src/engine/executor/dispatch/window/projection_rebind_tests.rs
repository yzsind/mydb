use super::*;

fn expr(sql: &str) -> Expr {
    crate::parser::parse_expr_fragment(sql).unwrap()
}

#[test]
fn output_projection_rebinding_visits_membership_and_quantified_operands() {
    let source = expr("ABS(v)");
    let projection = [(source, "projected".to_string())];
    for (input, expected) in [
        ("ABS(v) IN (1, ABS(v))", "projected IN (1, projected)"),
        (
            "ABS(v) NOT IN (1, ABS(v))",
            "projected NOT IN (1, projected)",
        ),
        (
            "ABS(v) IN (SELECT ABS(v) FROM t)",
            "projected IN (SELECT ABS(v) FROM t)",
        ),
    ] {
        let mut rebound = expr(input);
        rebind_expr_to_output_columns(&mut rebound, &projection);
        assert_eq!(rebound, expr(expected), "{input}");
    }
    let mut quantified = Expr::QuantifiedComparison {
        left: Box::new(expr("ABS(v)")),
        op: crate::parser::BinaryOp::Eq,
        quantifier: crate::parser::SubqueryQuantifier::Any,
        right: crate::parser::QuantifiedComparisonRight::List(vec![expr("1"), expr("ABS(v)")]),
    };
    rebind_expr_to_output_columns(&mut quantified, &projection);
    let Expr::QuantifiedComparison { left, right, .. } = quantified else {
        unreachable!()
    };
    assert_eq!(*left, expr("projected"));
    assert_eq!(
        right,
        crate::parser::QuantifiedComparisonRight::List(vec![expr("1"), expr("projected")])
    );
}

#[test]
fn inner_projection_rebinding_keeps_replacement_provenance_and_nested_scopes() {
    let mapped = Expr::GroupKeyRef {
        index: 2,
        expr: Box::new(expr("v")),
    };
    let aliases = std::collections::HashMap::from([("v".into(), mapped.clone())]);
    let original =
        expr("SUM(v) OVER (PARTITION BY v ORDER BY v ROWS BETWEEN v PRECEDING AND CURRENT ROW)");
    let Expr::WindowFunction { args, spec, .. } =
        rebind_window_expr_to_inner_projection(&original, &aliases)
    else {
        panic!("expected window");
    };
    let crate::parser::WindowSpec {
        partition_by,
        order_by,
        frame,
        ..
    } = *spec;

    assert_eq!(args, [mapped.clone()]);
    assert_eq!(partition_by, [mapped.clone()]);
    assert_eq!(order_by[0].expr, mapped.clone());
    let Expr::WindowFunction {
        spec: original_spec,
        ..
    } = original
    else {
        unreachable!()
    };
    let original_frame = original_spec.frame;
    assert_eq!(
        frame, original_frame,
        "frame bounds retain their own binding rules"
    );
    let original = expr("v IN (SELECT v FROM t)");
    let Expr::InSubquery {
        expr: left,
        subquery,
        ..
    } = rebind_window_expr_to_inner_projection(&original, &aliases)
    else {
        panic!("expected IN");
    };
    assert_eq!(*left, mapped);
    let Expr::InSubquery {
        subquery: original_subquery,
        ..
    } = original
    else {
        unreachable!()
    };
    assert_eq!(subquery, original_subquery);
}

#[test]
fn output_projection_rebinding_preserves_first_match_and_window_boundaries() {
    let source = expr("ABS(v)");
    let map = [(source.clone(), "first".into()), (source, "second".into())];
    let mut rebound = expr("ABS(v) + 1");
    rebind_expr_to_output_columns(&mut rebound, &map);
    assert_eq!(rebound, expr("first + 1"));
    let mut window = expr("SUM(ABS(v)) OVER (ORDER BY ABS(v))");
    let original = window.clone();
    rebind_expr_to_output_columns(&mut window, &map);
    assert_eq!(
        window, original,
        "unmatched window definitions are separate scopes"
    );
}

#[test]
fn inner_projection_rebinding_normalizes_only_ascii_identifier_case() {
    let replacement = expr("materialized");
    let aliases = std::collections::HashMap::from([
        ("v".into(), replacement.clone()),
        ("év".into(), replacement.clone()),
    ]);
    for original in [
        expr("v"),
        expr("V"),
        expr("t.V"),
        Expr::Identifier("éV".into()),
        Expr::Identifier("év".into()),
    ] {
        assert_eq!(
            rebind_window_expr_to_inner_projection(&original, &aliases),
            replacement
        );
    }
    let unchanged = Expr::Identifier("Év".into());
    assert_eq!(
        rebind_window_expr_to_inner_projection(&unchanged, &aliases),
        unchanged
    );
}
