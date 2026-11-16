use super::*;

fn expr(sql: &str) -> Expr {
    crate::parser::parse_expr_fragment(sql).unwrap()
}

#[test]
fn value_walk_preserves_case_order_and_query_boundaries() {
    let value = expr("CASE WHEN a THEN b WHEN c THEN d ELSE (SELECT e FROM (SELECT hidden FROM t) AS q WHERE f GROUP BY g HAVING h ORDER BY i) END");
    let mut names = Vec::new();
    walk_expr_for_idents(&value, &mut |node| {
        if let Expr::Identifier(name) = node {
            names.push(name.clone());
        }
    });
    assert_eq!(names, ["a", "c", "b", "d", "e", "f", "h", "g", "i"]);
}

#[test]
fn column_collectors_preserve_tuple_default_and_alias_rules() {
    let value = expr("(a.x, b.y, DEFAULT(z), (SELECT c.w FROM t WHERE c.v = a.x))");
    let mut all = HashSet::new();
    collect_expr_columns(&value, &mut all);
    assert_eq!(all, ["x", "y", "z", "w", "v"].map(String::from).into());
    let allowed = ["x", "y", "z", "w", "v"].map(String::from).into();
    let mut selected = HashSet::new();
    collect_expr_cte_column_refs(&value, "b", Some("a"), &allowed, &mut selected);
    assert_eq!(selected, ["x", "z"].map(String::from).into());
    let mut general = Vec::new();
    walk_expr_for_idents(&value, &mut |node| {
        if matches!(node, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            general.push(node.clone());
        }
    });
    assert!(
        general.is_empty(),
        "the general walk keeps tuple nodes opaque"
    );
}

#[test]
fn nonaggregate_walk_prunes_aggregate_arguments_in_nested_queries() {
    let value = expr("a + SUM(b) + (SELECT MAX(c) FROM t WHERE d = e)");
    let mut names = Vec::new();
    walk_expr_for_nonaggregate_idents(&value, &mut |node| {
        if let Expr::Identifier(name) = node {
            names.push(name.clone());
        }
    });
    assert_eq!(names, ["a", "d", "e"]);
}

#[test]
fn local_value_inspection_skips_subqueries_and_window_frames() {
    let value = expr("x IN (SELECT hidden FROM t)");
    let mut names = Vec::new();
    crate::parser::visit::inspect_value_expr(
        &value,
        |node| {
            if let Expr::Identifier(name) = node {
                names.push(name.clone());
            }
            true
        },
        |_| {},
    );
    assert_eq!(names, ["x"]);

    let value =
        expr("SUM(a) OVER (PARTITION BY b ORDER BY c ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");
    let mut names = Vec::new();
    walk_expr_for_nonaggregate_idents(&value, &mut |node| {
        if let Expr::Identifier(name) = node {
            names.push(name.clone());
        }
    });
    assert_eq!(names, ["a", "b", "c"]);
}

#[test]
fn window_and_outer_reference_probes_preserve_scope_policies() {
    let window = expr("SUM(v) OVER (ORDER BY k)");
    assert!(expr_has_window_function(&window));
    assert!(!expr_has_window_function(&Expr::Tuple(
        vec![window.clone()]
    )));
    assert!(!expr_has_window_function(&expr(
        "(SELECT SUM(v) OVER () FROM t)"
    )));
    let outer = ["o".to_string()].into();
    let inner = ["i".to_string()].into();
    assert!(subquery_expr_references_outer(
        &expr("(i.a, o.b)"),
        &outer,
        &inner
    ));
    assert!(!subquery_expr_references_outer(
        &expr("(SELECT o.b)"),
        &outer,
        &inner
    ));
    assert!(!subquery_expr_references_outer(
        &expr("SUM(o.b) OVER ()"),
        &outer,
        &inner
    ));
    assert!(!subquery_expr_references_outer(
        &expr("o.b"),
        &outer,
        &outer
    ));
}

#[test]
fn having_rewrite_is_single_pass_and_preserves_query_scope() {
    let aliases = HashMap::from([
        ("a".to_string(), expr("a + 1")),
        ("b".to_string(), expr("a")),
    ]);
    let rewritten = resolve_having_alias_expr(expr("a + b + (SELECT a FROM t)"), &aliases);
    assert_eq!(
        format!("{rewritten:?}"),
        format!("{:?}", expr("(a + 1) + a + (SELECT a FROM t)"))
    );
}
