//! Semantic regressions from the parser architecture audit.
use super::*;

#[test]
fn refactor_rewrite_quote_boundaries_follow_sql_mode() {
    let _mode = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES);
    let sql = "CREATE TABLE t (a TEXT(12) COMMENT '汉字\\', b TEXT(65536))";
    let rewritten = rewrite::rewrite_legacy_text_lengths(sql).unwrap();
    assert_eq!(
        rewritten,
        "CREATE TABLE t (a TINYTEXT COMMENT '汉字\\', b MEDIUMTEXT)"
    );
    assert!(
        rewrite::validate_cast_type_widths("SELECT 'slash\\', CAST(1 AS CHAR(4294967296))")
            .is_err()
    );

    let _ansi = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_ANSI_QUOTES);
    assert!(
        rewrite::validate_cast_type_widths("SELECT \"column\\\", CAST(1 AS CHAR(4294967296))")
            .is_err()
    );
}

#[test]
fn quality_like_disabled_escape_roundtrip() {
    for input in [r"s LIKE 'a\\b' ESCAPE ''", r"s NOT LIKE 'a\\b' ESCAPE ''"] {
        let expr = parse_expr_fragment(input).unwrap();
        assert_eq!(parse_expr_fragment(&expr_to_sql(&expr)).unwrap(), expr);
    }
}

#[test]
fn quality_float_literal_kind_roundtrip() {
    for input in ["1e0", "1.25e0", "1e100", "0e0", "5e-200"] {
        let expr = parse_expr_fragment(input).unwrap();
        assert_eq!(parse_expr_fragment(&expr_to_sql(&expr)).unwrap(), expr);
    }
}

#[test]
fn quality_derived_expression_source_names() {
    let Statement::Select(select) = parse_statement("SELECT * FROM (SELECT 1+2, 3*4) d").unwrap()
    else {
        panic!()
    };
    let derived = select.derived.unwrap();
    let inner = derived.as_select().unwrap();
    for (item, name) in inner.projection.iter().zip(["1+2", "3*4"]) {
        assert!(matches!(item, SelectItem::Expr { alias: Some(alias), .. } if alias == name));
    }
}

#[test]
fn quality_error_span_includes_leading_trivia() {
    let input = "/* metadata */\n  SELECT 1 +";
    let error = parse_statement(input).unwrap_err();
    assert_eq!(error.span.start, input.len());
    assert_eq!(error.line, 2);
    assert_eq!(error.column, 13);
}

#[test]
fn quality_view_check_option_trailing_trivia() {
    for suffix in [
        "",
        " -- tail",
        " /* tail */",
        "; /* tail */",
        " /* tail */;",
    ] {
        let sql = format!("CREATE VIEW v AS SELECT 1 AS c WITH CHECK OPTION{suffix}");
        let Statement::CreateView { check_option, .. } = parse_statement(&sql).unwrap() else {
            panic!()
        };
        assert_eq!(check_option.as_deref(), Some("CASCADED"));
    }
}

#[test]
fn quality_partition_routing_token_boundaries() {
    for prefix in [
        "CREATE TABLE",
        "CREATE  TABLE",
        "CREATE/*c*/TABLE",
        "CREATE\nTABLE",
    ] {
        let sql = format!("{prefix} p (id INT) PARTITION BY HASH(id) PARTITIONS 2");
        let Statement::CreateTable(table) = parse_statement(&sql).unwrap() else {
            panic!()
        };
        assert_eq!(table.partition.unwrap().partition_count, Some(2));
    }
}

#[test]
fn quality_delimiter_is_rejected() {
    for sql in ["DELIMITER", "DELIMITER $$", "DELIMITER $$ garbage"] {
        let parsed = parse_statement(sql);
        assert!(
            parsed.is_err() || matches!(parsed, Ok(Statement::Unsupported { .. })),
            "{sql}: {parsed:?}"
        );
    }
}

#[test]
fn quality_role_keywords_keep_quote_identity() {
    for name in ["ALL", "NONE", "DEFAULT"] {
        let Statement::SetRole(SetRole::Roles(roles)) =
            parse_statement(&format!("SET ROLE '{name}'")).unwrap()
        else {
            panic!()
        };
        assert_eq!(roles[0].user, name);
        let Statement::SetDefaultRole(SetDefaultRole::Roles { roles, .. }) =
            parse_statement(&format!("SET DEFAULT ROLE '{name}' TO 'u'")).unwrap()
        else {
            panic!()
        };
        assert_eq!(roles[0].user, name);
    }
    assert!(matches!(
        parse_statement("SET ROLE ALL").unwrap(),
        Statement::SetRole(SetRole::All)
    ));
}

#[test]
fn quality_manual_grammars_consume_complete_input() {
    for sql in [
        "SET ROLE ALL garbage",
        "SET ROLE NONE garbage",
        "SET ROLE DEFAULT garbage",
        "LOCK TABLES t READ,",
        "COMMIT CHAIN",
        "COMMIT AND CHAIN AND NO CHAIN",
        "ROLLBACK RELEASE RELEASE",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
    for sql in [
        "COMMIT WORK AND NO CHAIN NO RELEASE; /* tail */",
        "ROLLBACK AND CHAIN",
        "ROLLBACK WORK TO SAVEPOINT `a b`",
    ] {
        assert!(parse_statement(sql).is_ok(), "{sql}");
    }
}

#[test]
fn quality_routine_depth_budget_on_small_stack() {
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(|| {
            for depth in [10, 40, 160] {
                let sql = format!(
                    "{}SELECT 1;{}",
                    "BEGIN ".repeat(depth),
                    " END;".repeat(depth)
                );
                let result = parse_routine_body_sql(&sql);
                if depth <= 40 {
                    assert!(result.is_ok());
                } else {
                    assert!(result.unwrap_err().message.contains("depth"));
                }
                assert!(parse_routine_body_sql("BEGIN SELECT 1; END").is_ok());
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn quality_definer_dispatch_distinguishes_object_families() {
    for (sql, expected) in [
        ("CREATE DEFINER='u'@'%' PROCEDURE p() SELECT 1", 0),
        (
            "CREATE DEFINER='u'@'%' FUNCTION f() RETURNS INT RETURN 1",
            0,
        ),
        (
            "CREATE DEFINER='u'@'%' TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.id=1",
            1,
        ),
        (
            "CREATE DEFINER='u'@'%' EVENT ev ON SCHEDULE EVERY 1 HOUR DO SELECT 1",
            2,
        ),
        ("ALTER DEFINER='u'@'%' EVENT ev DO SELECT 2", 2),
        ("CREATE DEFINER='u'@'%' VIEW v AS SELECT 1", 3),
    ] {
        let parsed = parse_statement(sql).unwrap();
        assert!(
            match expected {
                0 => matches!(parsed, Statement::CreateRoutine(_)),
                1 => matches!(parsed, Statement::CreateTrigger(_)),
                2 => matches!(parsed, Statement::CreateEvent(_) | Statement::AlterEvent(_)),
                _ => matches!(parsed, Statement::CreateView { .. }),
            },
            "{sql}"
        );
    }
}

#[test]
fn quality_read_and_mutable_walkers_share_scope_and_source_order() {
    use super::visit::{AstVisitor, AstVisitorMut};
    #[derive(Default)]
    struct Trace(Vec<String>);
    macro_rules! record {
        ($trait_name:ident, [$($mutable:tt)*]) => {
            impl $trait_name for Trace {
                fn enter_statement(&mut self, _: & $($mutable)* Statement) { self.0.push("statement+".into()); }
                fn exit_statement(&mut self, _: & $($mutable)* Statement) { self.0.push("statement-".into()); }
                fn enter_cte(&mut self, cte: & $($mutable)* CteDef) { self.0.push(format!("cte+{}", cte.name)); }
                fn exit_cte(&mut self, cte: & $($mutable)* CteDef) { self.0.push(format!("cte-{}", cte.name)); }
                fn enter_select(&mut self, _: & $($mutable)* Select) { self.0.push("select+".into()); }
                fn exit_select(&mut self, _: & $($mutable)* Select) { self.0.push("select-".into()); }
                fn enter_select_alias_scope(&mut self, _: &[SelectItem]) { self.0.push("aliases+".into()); }
                fn exit_select_alias_scope(&mut self) { self.0.push("aliases-".into()); }
                fn enter_expr(&mut self, expr: & $($mutable)* Expr) { self.0.push(format!("expr:{:?}", std::mem::discriminant(expr))); }
                fn enter_limit(&mut self, _: & $($mutable)* LimitValue) { self.0.push("limit".into()); }
                fn enter_window_frame(&mut self, _: & $($mutable)* WindowFrame) { self.0.push("frame".into()); }
            }
        }
    }
    record!(AstVisitor, []);
    record!(AstVisitorMut, [mut]);
    for sql in [
        "WITH c AS (SELECT ? AS id) SELECT SUM(?) OVER (ORDER BY ? ROWS BETWEEN ? PRECEDING AND CURRENT ROW), CASE WHEN id>0 THEN ? ELSE ? END FROM c WHERE id IN(SELECT ? FROM t) ORDER BY ? LIMIT ?,?",
        "CREATE VIEW v AS SELECT CASE WHEN x=1 THEN y ELSE z END FROM t",
        "INSERT INTO t VALUES(?, ?) ON DUPLICATE KEY UPDATE x=? RETURNING x",
        "CREATE PROCEDURE p() BEGIN DECLARE x INT DEFAULT 1; IF x>0 THEN SET x=ABS(x); ELSE SET x=0; END IF; END",
    ] {
        let statement = parse_statement(sql).unwrap();
        let mut writable = statement.clone();
        let mut read = Trace::default();
        let mut write = Trace::default();
        visit::visit_statement(&mut read, &statement);
        visit::visit_statement_mut(&mut write, &mut writable);
        assert_eq!(read.0, write.0, "{sql}");
        assert_eq!(statement, writable, "a read-only visitor must not alter the tree");
    }
}

#[test]
fn quality_sql_mode_is_shared_by_operator_grammars() {
    let _mode = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES);
    for (source, expected) in [
        (
            r"SELECT 'a\' AS s, v <-> w AS n",
            r"SELECT 'a\' AS s, L2_DISTANCE(v,w) AS n",
        ),
        (
            r"SELECT 'a\' AS s, v <#> w AS n",
            r"SELECT 'a\' AS s, (0-DOT_PRODUCT(v,w)) AS n",
        ),
        (
            r"SELECT 'a\' AS s, 7 MOD 3 AS n",
            r"SELECT 'a\' AS s, 7 % 3 AS n",
        ),
        (
            r"SELECT v <-> 'a\' AS n",
            r"SELECT L2_DISTANCE(v,'a\') AS n",
        ),
    ] {
        assert_eq!(
            parse_statement(source).unwrap(),
            parse_statement(expected).unwrap(),
            "{source}"
        );
    }
}

#[test]
fn quality_vector_operator_grammar_has_explicit_operand_and_source_contracts() {
    for (source, expected) in [
        ("a <-> b <-> c", "L2_DISTANCE(L2_DISTANCE(a,b),c)"),
        ("(a+b) <-> (c+d)", "L2_DISTANCE((a+b),(c+d))"),
        ("1 + a <-> b * 2", "1 + L2_DISTANCE(a,b) * 2"),
        ("1-(a <#> b)", "1-(0-DOT_PRODUCT(a,b))"),
        ("café <#> café", "0-DOT_PRODUCT(café,café)"),
        ("'a' <-> 'b'", "L2_DISTANCE('a','b')"),
    ] {
        assert_eq!(
            parse_expr_fragment(source).unwrap(),
            parse_expr_fragment(expected).unwrap(),
            "{source}"
        );
    }
    for source in ["SELECT v <->", "SELECT v <#>", "SELECT v <-> w +"] {
        assert_eq!(
            parse_statement(source).unwrap_err().span.start,
            source.len()
        );
    }
    assert!(parse_statement("SELECT 1 # <#> is a comment\n +2").is_ok());
    assert!(parse_statement("DO 1 MOD 2, a <-> b").is_ok());
}

#[test]
fn quality_large_update_template_keeps_literal_provenance() {
    let payload = "a".repeat(65536);
    for hex in ["X'F'", "X'G0'"] {
        let sql = format!("UPDATE t SET s='{payload}', h={hex}");
        assert!(
            parse_statement(&sql).is_err(),
            "{hex} must not become a different literal syntax"
        );
        assert!(parse_statement_via_sqlparser_legacy(&sql).is_err());
    }
    for hex in ["0x", "0xZZ", "0xF", "X'0F'", "X''", "0x00FF"] {
        let sql = format!("UPDATE t SET s='{payload}', h={hex}");
        assert_eq!(
            parse_statement(&sql).unwrap(),
            parse_statement_via_sqlparser_legacy(&sql).unwrap(),
            "{hex}"
        );
    }
}

#[test]
fn quality_mod_keyword_function_requires_two_plain_arguments() {
    for source in ["MOD()", "MOD(1)", "MOD(1,2,3)", "MOD(DISTINCT 1,2)"] {
        assert_eq!(
            parse_expr_fragment(source).unwrap_err().code(),
            1064,
            "{source}"
        );
    }
    assert!(parse_expr_fragment("MOD(1,2)").is_ok());
}

#[test]
fn quality_query_walkers_cover_with_definitions_and_set_branches() {
    let statement = parse_statement(
        "WITH c AS(SELECT 10 AS n UNION ALL SELECT 20) SELECT n+30 FROM c UNION ALL SELECT 40",
    )
    .unwrap();
    let mut query = QueryExpr::try_from(statement.clone()).unwrap();
    #[derive(Default)]
    struct Literals(Vec<i64>);
    impl visit::AstVisitor for Literals {
        fn enter_expr(&mut self, expr: &Expr) {
            if let Expr::Literal(Literal::Int(value)) = expr {
                self.0.push(*value);
            }
        }
    }
    struct Change;
    impl visit::AstVisitorMut for Change {
        fn enter_expr(&mut self, expr: &mut Expr) {
            if let Expr::Literal(Literal::Int(value)) = expr {
                *value += 1;
            }
        }
    }
    let mut read = Literals::default();
    visit::visit_query(&mut read, &query);
    assert_eq!(read.0, [10, 20, 30, 40]);
    visit::visit_query_mut(&mut Change, &mut query);
    read.0.clear();
    visit::visit_query(&mut read, &query);
    assert_eq!(read.0, [11, 21, 31, 41]);
    assert_eq!(
        QueryExpr::try_from(query.clone().into_statement()).unwrap(),
        query
    );
    assert_eq!(
        crate::engine::auto_param::typed_plan_fingerprint(&statement, &[]),
        crate::engine::auto_param::typed_plan_fingerprint(
            &QueryExpr::try_from(statement.clone()).unwrap(),
            &[]
        ),
    );
}

#[test]
fn quality_query_slots_reject_nonquery_statements() {
    for sql in [
        "DELETE FROM t",
        "UPDATE t SET id=1",
        "CREATE TABLE t(id INT)",
    ] {
        assert!(
            QueryExpr::try_from(parse_statement(sql).unwrap()).is_err(),
            "{sql}"
        );
    }
    // Deserializing persisted query slots cannot admit a general statement.
    assert!(serde_json::from_str::<QueryExpr>(r#"{"Delete":{}}"#).is_err());
}

#[test]
fn quality_select_into_target_errors_point_to_the_original_token() {
    let sql = "/*lead*/ SELECT 1 INTO @x+1";
    let error = parse_statement(sql).unwrap_err();
    assert_eq!(error.span.start, sql.find('+').unwrap());
    let error = parse_statement("SELECT 1 INTO @x,@").unwrap_err();
    assert_eq!(error.span.start, "SELECT 1 INTO @x,@".len());
}

#[test]
fn insert_set_query_keeps_query_structure() {
    let Statement::Insert(insert) =
        parse_statement("INSERT INTO t SELECT 1 UNION ALL SELECT 2").unwrap()
    else {
        panic!("INSERT expected")
    };
    assert!(matches!(
        insert.source,
        InsertSource::Query(QueryExpr::SetOp(_))
    ));
}

#[test]
fn quality_catalog_encoder_refuses_lossy_query_nodes() {
    for sql in [
        "(SELECT 1)",
        "1 IN (SELECT 1)",
        "EXISTS (SELECT 1)",
        "ROW_NUMBER() OVER ()",
    ] {
        let expr = parse_expr_fragment(sql).unwrap();
        assert!(expr_to_catalog_sql(&expr).is_err(), "lossy encoding: {sql}");
    }
}

#[test]
fn quality_catalog_encoder_preserves_scalar_semantics() {
    for sql in [
        "a + 2 * b",
        "CASE WHEN a > 0 THEN 'x' ELSE 'y' END",
        "s LIKE 'x%' ESCAPE ''",
        "1e0",
        "X'00FF'",
        "CAST(a AS SIGNED)",
        "a COLLATE utf8mb4_bin",
        "JSON_EXTRACT(doc, '$.a')",
    ] {
        let expr = parse_expr_fragment(sql).unwrap();
        let encoded = expr_to_catalog_sql(&expr).unwrap();
        assert_eq!(
            parse_catalog_expr(&encoded).unwrap(),
            expr,
            "{sql}: {encoded}"
        );
    }
    let _mode = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES);
    let expr = parse_expr_fragment(r"'a\b'").unwrap();
    let encoded = expr_to_catalog_sql(&expr).unwrap();
    assert_eq!(parse_catalog_expr(&encoded).unwrap(), expr);
}

#[test]
fn quality_catalog_encoder_rejects_nested_and_bound_fallbacks() {
    let subquery = parse_expr_fragment("(SELECT 1)").unwrap();
    let expr = Expr::Function {
        name: "COALESCE".into(),
        args: vec![Expr::Literal(Literal::Int(1)), subquery],
        distinct: false,
    };
    assert!(expr_to_catalog_sql(&expr).is_err());
    for expr in [
        Expr::BoundValue(crate::types::Value::String("raw SQL".into())),
        Expr::Placeholder(7),
        Expr::Grouping(vec![0]),
        Expr::Literal(Literal::Float(f64::INFINITY)),
    ] {
        assert!(expr_to_catalog_sql(&expr).is_err());
    }
}

#[test]
fn quality_json_value_uses_sql_mode_and_comment_boundaries() {
    let _mode = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES);
    for sql in [
        r"SELECT 'a\' AS s, JSON_VALUE('{}', '$.x' DEFAULT 3 ON EMPTY)",
        "SELECT JSON_VALUE('{}', '$.x' /* ) RETURNING bogus */ DEFAULT 3 ON EMPTY)",
        "SELECT JSON_VALUE /* call comment */ ('{}', '$.x' DEFAULT 3 ON EMPTY)",
    ] {
        assert!(parse_statement(sql).is_ok(), "{sql}");
    }
}
#[test]
fn quality_json_value_nested_clauses_parse() {
    assert!(parse_statement(r#"SELECT JSON_VALUE(JSON_VALUE('{"x":{}}', '$.x' RETURNING JSON), '$.y' DEFAULT 3 ON EMPTY)"#).is_ok());
}
#[test]
fn quality_json_value_does_not_discard_response_tokens() {
    for sql in [
        "SELECT JSON_VALUE('{}', '$.x' NULL ON EMPTY 9)",
        "SELECT JSON_VALUE('{}', '$.x' NULL ON EMPTY ERROR ON EMPTY)",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn quality_sounds_like_keeps_predicate_and_identifier_roles() {
    for sql in [
        "SELECT 'a' SOUNDS LIKE 'b' SOUNDS LIKE 'c'",
        "SELECT 'a' LIKE 'b' SOUNDS LIKE 'c'",
        "SELECT 'a' SOUNDS LIKE 'b' BETWEEN 0 AND 1",
        "SELECT 'a' SOUNDS LIKE 'b' IN (1)",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
    for sql in [
        "SELECT 1 sounds",
        "SELECT sounds LIKE 'a%' FROM t",
        "SELECT `sounds` LIKE 'a%' FROM t",
        "SELECT ('a' SOUNDS LIKE 'b') SOUNDS LIKE 'c'",
    ] {
        assert!(parse_statement(sql).is_ok(), "{sql}");
    }
}

#[test]
fn quality_cast_does_not_discard_unsupported_options() {
    for sql in [
        "SELECT CAST('2026-10-03' AS DATE FORMAT 'YYYY-MM-DD')",
        "SELECT CAST(1 AS SIGNED ARRAY)",
        "SELECT TRY_CAST('bad' AS SIGNED)",
        "SELECT SAFE_CAST('bad' AS SIGNED)",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn quality_array_index_flag_belongs_to_its_ast_node() {
    let Statement::CreateTable(table) = parse_statement("CREATE TABLE t (j JSON, INDEX regular_i ((CAST(j AS UNSIGNED))), INDEX multi_i ((CAST(j AS UNSIGNED ARRAY))))").unwrap() else { panic!() };
    assert_eq!(table.indexes[0].key_expression_multi_valued, vec![false]);
    assert_eq!(table.indexes[1].key_expression_multi_valued, vec![true]);
}
#[test]
fn quality_array_cast_cannot_leak_into_generated_column() {
    assert!(parse_statement(
        "CREATE TABLE t (j JSON, g INT AS (CAST(j AS UNSIGNED ARRAY)), KEY regular_i (g))"
    )
    .is_err());
}
#[test]
fn quality_array_index_preserves_unicode_identifiers() {
    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE 数据 (j JSON, INDEX 多值 ((CAST(j AS UNSIGNED ARRAY))))")
            .unwrap()
    else {
        panic!()
    };
    assert_eq!(table.name, "数据");
    assert_eq!(table.indexes[0].name.as_deref(), Some("多值"));
}

#[test]
fn quality_native_cast_collate_keeps_column_option_scope() {
    let Statement::CreateTable(table) = parse_statement(
        "CREATE TABLE t (s VARCHAR(20) DEFAULT CAST('A' AS CHAR) COLLATE utf8mb4_bin)",
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(table.columns[0].collation.as_deref(), Some("utf8mb4_bin"));
    let expression = parse_expr_fragment("CAST('A' AS CHAR) COLLATE utf8mb4_bin").unwrap();
    assert!(matches!(expression, Expr::Function { name, .. } if name == "COLLATE"));
}

#[test]
fn quality_cast_error_span_keeps_integer_suffix() {
    let sql = "/* metadata */ SELECT CAST(1 AS SIGNED /* alias */ INTEGER) +";
    let error = parse_statement(sql).unwrap_err();
    assert_eq!(error.span.start, sql.len());
}

#[test]
fn quality_interval_native_requires_supported_unit_and_complete_syntax() {
    for sql in [
        "SELECT INTERVAL 1",
        "SELECT INTERVAL 1 FORTNIGHT",
        "SELECT INTERVAL 1 `DAY`",
        "SELECT INTERVAL 1 SECOND(3)",
        "SELECT INTERVAL 1 DAY TO SECOND",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
    assert_eq!(
        parse_expr_fragment("INTERVAL(3,1,2,4)+10").unwrap(),
        parse_expr_fragment("__INTERVAL_LIST(3,1,2,4)+10").unwrap()
    );
}

#[test]
fn quality_parameters_keep_identity_when_rebased_or_duplicated() {
    let mut statement = parse_statement("SELECT INTERVAL ? DAY + ?, ? LIKE ? ESCAPE ?").unwrap();
    let mut next = 5;
    visit::number_statement_placeholders(&mut statement, &mut next);
    assert_eq!(next, 10);
    struct Indices(Vec<usize>);
    impl visit::AstVisitor for Indices {
        fn enter_expr(&mut self, expr: &Expr) {
            if let Expr::Placeholder(index) = expr {
                self.0.push(*index);
            }
        }
    }
    let mut indices = Indices(Vec::new());
    visit::visit_statement(&mut indices, &statement);
    assert_eq!(indices.0, vec![6, 5, 7, 8, 9]);
    let Statement::Select(mut select) = parse_statement("SELECT ?").unwrap() else {
        panic!()
    };
    select.projection.push(select.projection[0].clone());
    let mut duplicated = Statement::Select(select);
    let mut count = 0;
    visit::number_statement_placeholders(&mut duplicated, &mut count);
    assert_eq!(count, 1);
}

#[test]
fn quality_parameter_display_preserves_literals_and_anonymous_names() {
    let Statement::Select(select) = parse_statement("SELECT TRIM('?123' FROM ?)").unwrap() else {
        panic!()
    };
    assert!(
        matches!(&select.projection[0], SelectItem::Expr { alias: Some(alias), .. } if alias.contains("'?123'") && !alias.contains("FROM ?0"))
    );
    let Statement::Call(call) = parse_statement("CALL p(INTERVAL ? DAY + ?, ?)").unwrap() else {
        panic!()
    };
    assert!(matches!(&call.args[1], Expr::Placeholder(2)));
}

#[test]
fn quality_parameter_identity_rejects_external_index_injection() {
    for sql in ["SELECT ?0", "SELECT ?18446744073709551615", "SELECT ?1 + ?"] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
    assert_eq!(
        parse_expr_fragment("'?18446744073709551615'").unwrap(),
        Expr::Literal(Literal::String("?18446744073709551615".into()))
    );
}

#[test]
fn quality_trim_direction_requires_from() {
    for sql in [
        "SELECT TRIM(LEADING ' a ')",
        "SELECT TRIM(BOTH ' a ')",
        "SELECT TRIM(TRAILING ' a ')",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn quality_catalog_native_scalar_forms_roundtrip() {
    for sql in [
        "CAST(a AS UNSIGNED)",
        "CAST(a AS DECIMAL(10,2))",
        "CAST(a AS BINARY(4))",
        "TRIM(LEADING 'x' FROM a)",
        "a COLLATE utf8mb4_bin",
        "CONVERT(a USING utf8mb4)",
    ] {
        let expr = parse_expr_fragment(sql).unwrap();
        let encoded = expr_to_catalog_sql(&expr).unwrap();
        assert_eq!(
            parse_catalog_expr(&encoded).unwrap(),
            expr,
            "{sql}: {encoded}"
        );
    }
    let _mode = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_PIPES_AS_CONCAT);
    let expr = parse_expr_fragment("a || 'x'").unwrap();
    assert_eq!(
        expr,
        parse_catalog_expr(&expr_to_catalog_sql(&expr).unwrap()).unwrap()
    );
}

#[test]
fn quality_shared_inspection_prunes_and_preserves_first_error() {
    let expr = parse_expr_fragment("outer_fn(skip_fn(a), b, c)").unwrap();
    let mut identifiers = Vec::new();
    let result = super::visit::try_for_each_expr(&expr, |expr| match expr {
        Expr::Function { name, .. } if name.eq_ignore_ascii_case("skip_fn") => Ok(false),
        Expr::Identifier(name) => {
            identifiers.push(name.clone());
            Err(name.clone())
        }
        _ => Ok(true),
    });
    assert_eq!(result, Err("b".to_string()));
    assert_eq!(identifiers, ["b"]);
}

#[test]
fn quality_column_definition_contract_is_shared_by_alter() {
    for definition in [
        "a SERIAL",
        "a INT ZEROFILL",
        "a INT ZEROFILL UNSIGNED",
        "a DECIMAL(12,3) UNSIGNED ZEROFILL",
        "a REAL(12,3) ZEROFILL",
        "a INT NOT NULL ZEROFILL",
        "a NCHAR(10)",
        "a NVARCHAR(10)",
        "a NATIONAL /* type */ CHAR(10)",
        "a NATIONAL CHARACTER /* varying */ VARYING(10)",
        "a NCHAR /* varying */ VARCHAR(10)",
        "a REAL(42,12) UNSIGNED",
        "a DOUBLE /* type */ PRECISION(42,12) SIGNED",
        "a FLOAT(58,0) SIGNED NOT NULL",
        "a LONG",
        "a LONG /* type suffix */ BYTE",
        "a YEAR(4) /* type suffix */ UNSIGNED",
        "a INT NOT NULL DEFAULT (1 + 2) COMMENT 'column'",
        "a INT GENERATED ALWAYS AS (b + 1) STORED",
        "a VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin INVISIBLE",
        "a INT CONSTRAINT positive CHECK (a > 0)",
        "a POINT SRID 4326",
    ] {
        let Statement::CreateTable(create) =
            parse_statement(&format!("CREATE TABLE t ({definition})")).unwrap()
        else {
            panic!("CREATE TABLE")
        };
        let expected = &create.columns[0];
        for action in [
            format!("ADD COLUMN {definition}"),
            format!("MODIFY COLUMN {definition}"),
            format!("CHANGE COLUMN old_a {definition}"),
        ] {
            let sql = format!("ALTER TABLE t {action}");
            let Statement::AlterTable(alter) = parse_statement(&sql).unwrap() else {
                panic!("{sql}")
            };
            let actual = alter
                .actions
                .iter()
                .find_map(|action| match action {
                    AlterAction::AddColumn(column) | AlterAction::ModifyColumn(column) => {
                        Some(column)
                    }
                    AlterAction::ChangeColumn { new_def, .. } => Some(new_def),
                    _ => None,
                })
                .unwrap();
            assert_eq!(actual, expected, "{sql}");
        }
    }
}

#[test]
fn quality_alter_rejects_invalid_column_attributes_consistently() {
    for definition in [
        "a INT PRIMARY KEY PRIMARY KEY",
        "a INT PRIMARY KEY INVISIBLE",
    ] {
        let expected = parse_statement(&format!("CREATE TABLE t ({definition})")).unwrap_err();
        for action in ["ADD COLUMN", "MODIFY COLUMN", "CHANGE COLUMN old_a"] {
            let sql = format!("ALTER TABLE t {action} {definition}");
            let actual = parse_statement(&sql).expect_err(&sql);
            assert_eq!(actual.code(), expected.code(), "{sql}");
        }
    }
}

#[test]
fn quality_column_type_suffixes_are_validated_in_type_scope() {
    for sql in [
        "CREATE TABLE t (a INT BYTE)",
        "CREATE TABLE t (a LONG BYTE BYTE)",
        "CREATE TABLE t (a LONG DEFAULT 'x' BYTE)",
        "CREATE TABLE t (a VARCHAR(10) UNSIGNED)",
        "CREATE TABLE t (a YEAR NULL UNSIGNED)",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
    let Statement::CreateTable(table) = parse_statement(
        "CREATE TABLE long (long LONG BYTE, y YEAR UNSIGNED, s VARCHAR(40) DEFAULT 'LONG BYTE YEAR UNSIGNED')"
    ).unwrap() else { panic!("CREATE TABLE") };
    assert_eq!(table.name, "long");
    assert_eq!(table.columns[0].name, "long");
    assert_eq!(
        table.columns[2].default,
        Some(Expr::Literal(Literal::String(
            "LONG BYTE YEAR UNSIGNED".into()
        )))
    );
}

#[test]
fn quality_native_numeric_suffixes_preserve_mode_and_bounds() {
    for mode in [0, crate::engine::api::SQL_MODE_REAL_AS_FLOAT] {
        let _mode = ParserSqlModeGuard::activate(mode);
        for spelling in ["REAL(42,12)", "REAL /* precision */ (42,12)"] {
            let Statement::CreateTable(table) =
                parse_statement(&format!("CREATE TABLE t (a {spelling} UNSIGNED)")).unwrap()
            else {
                panic!("CREATE TABLE")
            };
            let expected = if mode == 0 {
                DataType::Double(Some((42, 12)))
            } else {
                DataType::Float(Some((42, 12)))
            };
            assert_eq!(table.columns[0].data_type, expected);
            assert!(table.columns[0].unsigned);
        }
    }
    for ty in ["REAL(200,100)", "DOUBLE PRECISION(200,100)"] {
        assert_eq!(
            parse_statement(&format!("CREATE TABLE t (a {ty})"))
                .unwrap_err()
                .code(),
            1425
        );
    }
    for ty in [
        "REAL(4,2)(4,2)",
        "REAL UNSIGNED(4,2)",
        "DOUBLE PRECISION NULL (4,2)",
        "REAL(4,2) UNSIGNED SIGNED",
        "VARCHAR(4) SIGNED",
        "INT UNSIGNED SIGNED",
    ] {
        assert!(
            parse_statement(&format!("CREATE TABLE t (a {ty})")).is_err(),
            "{ty}"
        );
    }
}

#[test]
fn quality_national_types_keep_identifier_literal_and_cast_scopes() {
    for mode in [0, crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES] {
        let _mode = ParserSqlModeGuard::activate(mode);
        let Statement::CreateTable(table) = parse_statement(
            "CREATE TABLE nvarchar (nchar NCHAR(10), s VARCHAR(50) DEFAULT 'NATIONAL CHARACTER VARYING(10)', c NCHAR(5) COLLATE utf8mb3_bin)"
        ).unwrap() else { panic!("CREATE TABLE") };
        assert_eq!(table.name, "nvarchar");
        assert_eq!(table.columns[0].name, "nchar");
        assert_eq!(table.columns[0].charset.as_deref(), Some("utf8mb3"));
        assert_eq!(
            table.columns[1].default,
            Some(Expr::Literal(Literal::String(
                "NATIONAL CHARACTER VARYING(10)".into()
            )))
        );
        assert_eq!(table.columns[2].collation.as_deref(), Some("utf8mb3_bin"));
        parse_statement("CREATE TABLE t AS SELECT CAST('abcdef' AS /* target */ NCHAR(3)) AS c")
            .unwrap();
    }
    for definition in [
        "a NATIONAL VARYING(10)",
        "a NCHAR CHAR(10)",
        "a NATIONAL",
        "a INT VARYING(10)",
        "a NCHAR(5) VARYING(10)",
        "a NATIONAL CHAR(10) VARCHAR(20)",
    ] {
        assert!(
            parse_statement(&format!("CREATE TABLE t ({definition})")).is_err(),
            "{definition}"
        );
    }
}

#[test]
fn quality_bit_tokens_preserve_source_and_literal_slots() {
    for mode in [0, crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES] {
        let _mode = ParserSqlModeGuard::activate(mode);
        for sql in [
            "SELECT 0b1010 AS a, 0B000000001 AS b, '0b10' AS s, `0b10` AS c, ? AS p /* 0b1 */",
            "CREATE TABLE t (a VARBINARY(2) DEFAULT 0b1010, b INT CHECK (b > 0b1))",
            "SELECT 0b1010 + ? AS a, v <-> w AS d",
        ] {
            let quoted = sql
                .replace("0b1010", "B'1010'")
                .replace("0B000000001", "B'000000001'");
            assert_eq!(
                parse_statement(sql).unwrap(),
                parse_statement(&quoted).unwrap(),
                "{sql}"
            );
        }
    }
    assert_eq!(
        parse_expr_fragment("0b1010").unwrap(),
        parse_expr_fragment("B'1010'").unwrap()
    );
    assert!(
        matches!(parse_expr_fragment("`0b10`").unwrap(), Expr::Identifier(name) if name == "0b10")
    );
}

#[test]
fn quality_bit_literal_value_slots_reach_semantic_validation() {
    let err =
        parse_statement("SELECT MATCH(body) AGAINST(0b1010 IN BOOLEAN MODE) FROM t").unwrap_err();
    assert!(
        err.message
            .contains("expects a string literal or parameter"),
        "{err:?}"
    );
}

#[test]
fn quality_zerofill_is_bound_to_its_column_definition() {
    let Statement::AlterTable(table) =
        parse_statement("ALTER TABLE t MODIFY a INT, MODIFY a INT ZEROFILL").unwrap()
    else {
        panic!("ALTER TABLE")
    };
    let columns = table
        .actions
        .iter()
        .filter_map(|action| match action {
            AlterAction::ModifyColumn(column) => Some(column),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!columns[0].zerofill);
    assert!(columns[1].zerofill && columns[1].unsigned);
    for ty in ["VARCHAR(10)", "JSON", "YEAR", "DATE", "BLOB"] {
        assert!(
            parse_statement(&format!("CREATE TABLE t (a {ty} ZEROFILL)")).is_err(),
            "{ty}"
        );
    }
    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE t (`zerofill` INT COMMENT 'ZEROFILL')").unwrap()
    else {
        panic!("CREATE TABLE")
    };
    assert!(!table.columns[0].zerofill);
}

#[test]
fn quality_owned_query_bodies_keep_with_dml_and_query_tails() {
    for sql in [
        "WITH c AS (SELECT 1 AS id) UPDATE t SET v=(SELECT id FROM c) WHERE id=1",
        "WITH c AS (SELECT 1 AS id) DELETE FROM t WHERE id IN (SELECT id FROM c)",
    ] {
        let Statement::WithCte(with) = parse_statement(sql).unwrap() else {
            panic!("{sql}")
        };
        assert_eq!(with.ctes.len(), 1);
        assert!(
            matches!(*with.body, Statement::Update(_) | Statement::Delete(_)),
            "{sql}"
        );
    }
    for sql in [
        "SELECT (WITH c AS (SELECT 1 AS id) UPDATE t SET v=1)",
        "SELECT * FROM (WITH c AS (SELECT 1 AS id) DELETE FROM t) AS d",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
    let Statement::Select(select) =
        parse_statement("(SELECT 1 AS x) ORDER BY x DESC LIMIT 2").unwrap()
    else {
        panic!("SELECT")
    };
    assert_eq!(select.limit, Some(LimitValue::Rows(2)));
    assert_eq!(select.order_by.len(), 1);
    assert!(!select.order_by[0].asc);
}

#[test]
fn quality_table_query_lowering_matches_select_without_reparse() {
    for sql in [
        "WITH c AS (TABLE test.t ORDER BY id DESC LIMIT 2) SELECT * FROM c",
        "SELECT EXISTS(TABLE test.t) AS found",
        "WITH c AS (TABLE test.t UNION ALL TABLE test.u) SELECT * FROM c",
        "WITH c AS (TABLE `my db`.`a``b` LIMIT 3) SELECT * FROM c",
    ] {
        let expanded = sql.replace("TABLE ", "SELECT * FROM ");
        assert_eq!(
            parse_statement(sql).unwrap(),
            parse_statement(&expanded).unwrap(),
            "{sql}"
        );
    }
    // Upstream does not parse unqualified TABLE tails reliably. Test the
    // converter independently instead of depending on its token overread.
    for name in ["t", "dual"] {
        for tail in [
            "",
            "ORDER BY id DESC LIMIT 2",
            "ORDER BY 0",
            "ORDER BY 1",
            "ORDER BY SUM(id) OVER missing",
        ] {
            let sql = format!("SELECT * FROM {name} {tail}");
            let mut parsed = adapter::parse_sql(&MydbDialect::default(), &sql).unwrap();
            let SqlStatement::Query(mut query) = parsed.pop().unwrap() else {
                panic!("query")
            };
            query.body = Box::new(SetExpr::Table(Box::new(sqlparser::ast::Table {
                table_name: Some(name.into()),
                schema_name: None,
            })));
            let actual = super::convert::convert_statement(&sql, SqlStatement::Query(query));
            let expected = parse_statement(&sql);
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => assert_eq!(actual, expected, "{sql}"),
                (Err(actual), Err(expected)) => assert_eq!(actual.code(), expected.code(), "{sql}"),
                (actual, expected) => panic!("{sql}: {actual:?} != {expected:?}"),
            }
        }
    }
}

#[test]
fn quality_window_resolution_keeps_original_parent_graph() {
    for definitions in [
        "wbase AS (PARTITION BY k), wchild AS (wbase ORDER BY v)",
        "wchild AS (wbase ORDER BY v), wbase AS (PARTITION BY k)",
    ] {
        let Statement::Select(select) = parse_statement(&format!(
            "SELECT SUM(v) OVER wchild AS total FROM t WINDOW {definitions}"
        ))
        .unwrap() else {
            panic!("SELECT")
        };
        let wchild = select
            .named_windows
            .iter()
            .find(|window| window.name == "wchild")
            .unwrap();
        assert_eq!(wchild.partition_by, [Expr::Identifier("k".into())]);
        assert_eq!(wchild.order_by[0].expr, Expr::Identifier("v".into()));
        assert!(wchild.inherits.is_none());
    }
    for definitions in [
        "wbase AS (), wmid AS (wbase), wleaf AS (wmid)",
        "wleaf AS (wmid), wmid AS (wbase), wbase AS ()",
        "wbase AS (), WBASE AS ()",
        "wbase AS (ROWS UNBOUNDED PRECEDING), wchild AS (wbase)",
    ] {
        assert!(
            parse_statement(&format!("SELECT 1 WINDOW {definitions}")).is_err(),
            "{definitions}"
        );
    }
}

#[test]
fn quality_insert_set_preserves_quoted_assignment_name() {
    for target in ["`a.b`", "t.`a.b`"] {
        let sql = format!("INSERT INTO t SET {target}=7");
        let Statement::Insert(insert) = parse_statement(&sql).unwrap() else {
            panic!("{sql}")
        };
        assert_eq!(insert.columns, Some(vec!["a.b".to_owned()]), "{sql}");
    }
}

#[test]
fn quality_dml_rejects_unimplemented_table_modifiers() {
    for sql in [
        "UPDATE t WITH ORDINALITY SET a=1",
        "DELETE FROM t WITH ORDINALITY",
        "UPDATE t TABLESAMPLE BERNOULLI(10) SET a=1",
        "DELETE FROM t TABLESAMPLE BERNOULLI(10)",
    ] {
        assert!(
            matches!(parse_statement(sql).unwrap(), Statement::Unsupported { .. }),
            "{sql}"
        );
    }
}

#[test]
fn quality_table_expression_errors_keep_syntax_code_and_source() {
    for sql in [
        "SELECT * FROM generate_series(X'1',3)",
        "SELECT * FROM JSON_TABLE(X'1', '$' COLUMNS(v INT PATH '$.v')) jt",
        "SELECT * FROM t FOR SYSTEM_TIME AS OF X'1'",
        "SELECT * FROM t JOIN u ON t.id=X'1'",
    ] {
        let error = parse_statement(sql).unwrap_err();
        assert_eq!(error.code(), 1064, "{sql}: {error:?}");
        assert_eq!(error.span.start, sql.find("X'1'").unwrap(), "{sql}");
    }
}

#[test]
fn quality_generate_series_modifiers_are_not_silently_dropped() {
    for sql in [
        "SELECT * FROM generate_series(1,3) TABLESAMPLE BERNOULLI(10)",
        "SELECT * FROM generate_series(1,3) USE INDEX (i)",
    ] {
        assert!(
            matches!(parse_statement(sql).unwrap(), Statement::Unsupported { .. }),
            "{sql}"
        );
    }
    let Statement::Select(select) =
        parse_statement("SELECT * FROM generate_series(1,3) WITH ORDINALITY AS g(n,pos)").unwrap()
    else {
        panic!("SELECT")
    };
    let series = select.from[0].generate_series.as_ref().unwrap();
    assert!(series.with_ordinality);
    assert_eq!(series.column_aliases, ["n", "pos"]);
}

#[test]
fn quality_join_using_keeps_identifier_boundaries() {
    let Statement::Select(select) =
        parse_statement("SELECT * FROM l JOIN r USING (`a.b`)").unwrap()
    else {
        panic!("SELECT")
    };
    assert_eq!(select.joins[0].using_columns, ["a.b"]);
    for columns in ["l.a", "a,A", "`a.b`,`A.B`"] {
        let sql = format!("SELECT * FROM l JOIN r USING ({columns})");
        assert!(
            matches!(
                parse_statement(&sql).unwrap(),
                Statement::Unsupported { .. }
            ),
            "{sql}"
        );
    }
}

#[test]
fn quality_sequence_cursor_preserves_roles_and_owner_parts() {
    let Statement::CreateSequence { options, .. } = parse_statement(
        "CREATE /* header */ SEQUENCE s OWNED BY `t.x`.`c.y` CACHE 8 INCREMENT BY -2",
    )
    .unwrap() else {
        panic!("CREATE SEQUENCE")
    };
    assert_eq!(options.owned_table.as_deref(), Some("t.x"));
    assert_eq!(options.owned_column.as_deref(), Some("c.y"));
    assert_eq!(options.cache, Some(8));
    assert_eq!(options.increment, Some(-2));
    for sql in [
        "CREATE SEQUENCE s `CACHE` 10",
        "ALTER SEQUENCE s RESTART WITH '2'",
        "CREATE SEQUENCE s AS `bigint`",
        "CREATE SEQUENCE s OWNED BY `none`",
        "CREATE SEQUENCE s OWNED BY t",
        "SHOW SEQUENCES LIKE 's%' trailing",
        "SHOW SEQUENCES LIKE `s%`",
        "SHOW CREATE SEQUENCE s extra",
    ] {
        let error = parse_statement(sql).unwrap_err();
        assert_eq!(error.code(), 1064, "{sql}");
        assert!(error.span.start > 0, "{sql}: {error:?}");
    }
    let Statement::AlterSequence { restart, .. } =
        parse_statement("ALTER SEQUENCE s RESTART WITH -9223372036854775808").unwrap()
    else {
        panic!("ALTER SEQUENCE")
    };
    assert_eq!(restart, Some(SequenceRestart::To(i64::MIN)));
    parse_statement("SHOW SEQUENCES FROM `db.x` LIKE 's%';").unwrap();
}

#[test]
fn quality_rewrite_chain_preserves_borrowed_slices_and_owned_identity() {
    use std::borrow::Cow;
    let input = String::from("SELECT 1");
    let pointer = input.as_ptr();
    let (output, metadata) =
        super::rewrite::rewrite_sql_with(Cow::Owned(input), |sql| (Cow::Borrowed(sql), 7));
    assert!(matches!(output, Cow::Owned(_)));
    assert_eq!(output.as_ptr(), pointer);
    assert_eq!(metadata, 7);
    let output = super::rewrite::rewrite_sql(Cow::Owned(" prefix SELECT 1".into()), |sql| {
        Cow::Borrowed(&sql[8..])
    });
    assert_eq!(output, "SELECT 1");
    let output = super::rewrite::rewrite_sql(Cow::Borrowed("SELECT 1"), |sql| Cow::Borrowed(sql));
    assert!(matches!(output, Cow::Borrowed("SELECT 1")));
}

#[test]
fn quality_explain_extension_tokens_and_errors() {
    for sql in [
        "EXPLAIN FOR /* c */ CONNECTION 7",
        "EXPLAIN FOR CONNECTION 7 /* c */; -- tail",
        "EXPLAIN FORMAT /* c */ = JSON FOR CONNECTION 7",
    ] {
        assert!(
            matches!(
                parse_statement(sql).unwrap(),
                Statement::ExplainForConnection {
                    connection_id: 7,
                    ..
                }
            ),
            "{sql}"
        );
    }
    for sql in [
        "EXPLAIN FORMAT=甲乙丙丁 SELECT 1",
        "EXPLAIN FORMAT=`TRADITIONAL` SELECT 1",
        "EXPLAIN FOR CONNECTION -1",
        "EXPLAIN FOR CONNECTION 4294967296",
        "EXPLAIN FOR CONNECTION 1.5",
        "EXPLAIN FOR CONNECTION 7 trailing",
        "EXPLAIN FOR CONNECTION 7 /* unfinished",
        "EXPLAIN ANALYZE FOR CONNECTION 7",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn quality_explain_normalization_preserves_body_and_positions() {
    let sql = "EXPLAIN /* 中文\n注释 */ FORMAT=JSON -- c\nSELECT '-- unchanged', 1 /* body */";
    let normalized = strip_comments_in_explain_header(sql);
    assert_eq!(normalized.len(), sql.len());
    let body = sql.find("SELECT").unwrap();
    assert_eq!(&normalized[body..], &sql[body..]);
    for (i, byte) in sql
        .bytes()
        .enumerate()
        .filter(|(_, b)| matches!(b, b'\r' | b'\n'))
    {
        assert_eq!(normalized.as_bytes()[i], byte);
    }
    for sql in [
        "EXPLAIN SELECT 1",
        "EXPLAIN FORMAT=JSON SELECT 1",
        "SELECT 1",
    ] {
        assert!(matches!(
            strip_comments_in_explain_header(sql),
            std::borrow::Cow::Borrowed(_)
        ));
        assert!(matches!(
            normalize_explain(sql).unwrap().0,
            std::borrow::Cow::Borrowed(_)
        ));
    }
    let table = normalize_explain("EXPLAIN /* c */ FORMAT=TREE TABLE t LIMIT 2")
        .unwrap()
        .0;
    assert!(table.ends_with("SELECT * FROM t LIMIT 2"));
    assert!(parse_statement(&table).is_ok());
    let sql = "EXPLAIN FORMAT=TRADITIONAL\nSELECT 1 +";
    let fragment = parse_statement("SELECT 1 +").unwrap_err();
    let error = parse_statement(sql).unwrap_err();
    assert_eq!(
        error.span.start,
        sql.find("SELECT").unwrap() + fragment.span.start
    );
    assert_eq!(error.code(), fragment.code());
}

#[test]
fn quality_transaction_tokens_preserve_comments_names_and_boundaries() {
    for sql in [
        "START TRANSACTION READ /* c */ ONLY",
        "BEGIN /* c */ WORK READ\nONLY",
    ] {
        assert!(
            matches!(
                parse_statement(sql).unwrap(),
                Statement::Begin {
                    read_only: Some(true),
                    consistent_snapshot: false
                }
            ),
            "{sql}"
        );
    }
    assert!(matches!(
        parse_statement("SET /* c */ SESSION TRANSACTION READ ONLY").unwrap(),
        Statement::SetTransactionCharacteristics {
            scope: Some(SetScope::Session),
            ..
        }
    ));
    assert!(matches!(
        parse_statement("SET\nLOCAL TRANSACTION ISOLATION /* c */ LEVEL READ COMMITTED").unwrap(),
        Statement::SetTransactionIsolation {
            scope: Some(SetScope::Session),
            isolation: TransactionIsolation::ReadCommitted
        }
    ));
    assert_eq!(
        parse_statement("SAVEPOINT /* c */ `a.b`").unwrap(),
        Statement::Savepoint("a.b".into())
    );
    assert_eq!(
        parse_statement("RELEASE /* c */ SAVEPOINT `a.b`").unwrap(),
        Statement::ReleaseSavepoint("a.b".into())
    );
    assert_eq!(
        parse_statement("SET /* c */ @@SESSION.autocommit = /* c */ OFF").unwrap(),
        Statement::SetAutocommit(false)
    );
    for sql in [
        "SET auto commit=0",
        "SET CHARSETutf8mb4",
        "SET CHARACTER SET utf8mb4 trailing",
        "START TRANSACTION READ ONLY, READ WRITE",
        "START TRANSACTION READ ONLY,",
        "SET TRANSACTION READ ONLY,",
        "SAVEPOINT `s` trailing",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn quality_set_charset_tokens_and_generic_assignment_fallback() {
    for sql in [
        "SET CHARACTER /* c */ SET 'utf8mb4'",
        "SET\nCHARSET utf8mb4",
        "SET SESSION CHARACTER SET `utf8mb4`",
    ] {
        assert_eq!(
            parse_statement(sql).unwrap(),
            Statement::SetCharacterSet {
                charset: Some("utf8mb4".into())
            }
        );
    }
    assert_eq!(
        parse_statement("SET CHARSET DEFAULT").unwrap(),
        Statement::SetCharacterSet { charset: None }
    );
    for sql in [
        "SET autocommit=0, sql_mode=''",
        "SET @autocommit=0",
        "SET GLOBAL autocommit=0",
        "SET autocommit=1-1",
    ] {
        assert!(
            matches!(parse_statement(sql).unwrap(), Statement::Set(_)),
            "{sql}"
        );
    }
}

#[test]
fn quality_shared_routine_keyword_scans_respect_structure() {
    let sql = "'FROM' /* FROM */ f(FROM), `FROM`, FROM t WHERE x=1";
    assert_eq!(
        find_first_top_level_keyword(sql, 0, &["FROM", "WHERE"]),
        sql.find("FROM t")
    );
    assert_eq!(
        find_first_top_level_keyword(sql, sql.find("WHERE").unwrap(), &["FROM", "WHERE"]),
        sql.find("WHERE")
    );
    assert!(
        find_first_top_level_keyword("SELECT from_col, (SELECT x FROM t)", 0, &["FROM"]).is_none()
    );
    for body in [
        "BEGIN DECLARE c CURSOR /* gap */ FOR SELECT 1; OPEN c; CLOSE c; END",
        "BEGIN DECLARE c CONDITION /* gap */ FOR SQLSTATE '45000'; SIGNAL c SET MESSAGE_TEXT='a,b', MYSQL_ERRNO=1644; END",
        "BEGIN SET a=COALESCE(1,2), b='x,y'; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT=CONCAT('x,','y'), MYSQL_ERRNO=1644; END",
    ] {
        parse_routine_body_sql(body).unwrap_or_else(|error| panic!("{body}: {error:?}"));
    }
}

#[test]
fn quality_routine_declaration_phrase_spans_preserve_payload() {
    let body =
        parse_routine_body_sql("BEGIN DECLARE c CURSOR /* gap */ FOR SELECT 1; END").unwrap();
    assert!(
        matches!(&body.statements[0], RoutineStmt::DeclareCursor { select_sql, .. } if select_sql == "SELECT 1")
    );
    let body = parse_routine_body_sql(
        "BEGIN DECLARE CONTINUE HANDLER /* gap */ FOR SQLEXCEPTION SET x=1; END",
    )
    .unwrap();
    assert!(
        matches!(&body.statements[0], RoutineStmt::DeclareHandler { conditions, statement, .. }
        if matches!(&conditions[..], [RoutineCondition::SqlException]) && matches!(statement.as_ref(), RoutineStmt::Set { .. }))
    );
}

#[test]
fn quality_routine_boundary_tokens_preserve_unicode_and_bad_batch_tail() {
    let sql = "CREATE PROCEDURE p() BEGIN SET 前END=1; SET x=2; END; SELECT 1";
    let parsed = parse_statements(sql).unwrap();
    assert_eq!(parsed.len(), 2);
    let sql = "SELECT 1; CREATE PROCEDURE p() BEGIN SET x='unterminated";
    let (effective, ranges) = split_query_statements(sql, 0);
    assert_eq!(ranges.len(), 2);
    assert_eq!(&effective[ranges[0].clone()], "SELECT 1");
    assert!(parse_statement(&effective[ranges[1].clone()]).is_err());
    let parts = split_routine_statements("label_longer_than_sixteen_bytes: LOOP SET x=REPEAT('x',2); END /* c */ LOOP label_longer_than_sixteen_bytes; SELECT 2");
    assert_eq!(parts.len(), 2);
}

#[test]
fn quality_source_map_public_error_coordinates() {
    for sql in [
        "SELECT 1;\n/* 第二条 */ SELECT 2 +",
        "/* 前导 */ TABLE t LIMIT",
    ] {
        let error = parse_statements(sql).unwrap_err();
        assert_eq!(
            error.span,
            Span {
                start: sql.len(),
                end: sql.len()
            },
            "{error:?}"
        );
    }
    for sql in ["1 +", "'中文' +"] {
        let error = parse_expr_fragment(sql).unwrap_err();
        assert_eq!(
            error.span,
            Span {
                start: sql.len(),
                end: sql.len()
            }
        );
        assert_eq!(error.column, sql.chars().count() + 1);
    }
}

#[test]
fn quality_source_map_explicit_edits_and_composition() {
    let input = "aa中文bb";
    let (output, map) = edit_source(
        input,
        &[
            TextEdit {
                range: 0..0,
                replacement: "SELECT ",
            },
            TextEdit {
                range: 2..8,
                replacement: "?",
            },
        ],
    )
    .unwrap();
    assert_eq!(output, "SELECT aa?bb");
    assert_eq!(
        map.span(Span { start: 9, end: 10 }),
        Span { start: 2, end: 8 }
    );
    assert_eq!(
        map.span(Span { start: 10, end: 12 }),
        Span { start: 8, end: 10 }
    );
    let outer = "prefix aa中文bb suffix";
    let slice = SourceMap::slice(outer, 7..17);
    assert_eq!(
        slice.span(map.span(Span { start: 9, end: 10 })),
        Span { start: 9, end: 15 }
    );
    let (_, deleted) = edit_source(
        "abc",
        &[TextEdit {
            range: 1..2,
            replacement: "",
        }],
    )
    .unwrap();
    assert_eq!(
        deleted.span(Span { start: 1, end: 1 }),
        Span { start: 2, end: 2 }
    );
    assert!(edit_source(
        "中文",
        &[TextEdit {
            range: 1..3,
            replacement: ""
        }]
    )
    .is_err());
}

#[test]
fn quality_view_qualified_name_ast_and_binding() {
    let Statement::CreateView { name, .. } =
        parse_statement("CREATE VIEW `db`.`v` AS SELECT 1").unwrap()
    else {
        panic!()
    };
    assert_eq!(name.database(), Some("db"));
    assert_eq!(name.object(), "v");
    assert_eq!(name.bind("other").catalog_key(), "db.v");
    let Statement::AlterView { name, .. } = parse_statement("ALTER VIEW v AS SELECT 1").unwrap()
    else {
        panic!()
    };
    assert_eq!(name.bind("other").catalog_key(), "other.v");
    for sql in [
        "CREATE VIEW `db.v` AS SELECT 1",
        "ALTER VIEW `db.v` AS SELECT 1",
        "CREATE VIEW a.b.c AS SELECT 1",
        "DROP VIEW `db.v`",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
    let json = serde_json::to_string(&name).unwrap();
    assert_eq!(serde_json::from_str::<QualifiedName>(&json).unwrap(), name);
    assert!(serde_json::from_str::<QualifiedName>(r#"{"database":null,"object":"db.v"}"#).is_err());
}

#[test]
fn quality_view_source_map_tracks_removed_check_option() {
    let sql = "CREATE VIEW v AS SELECT 1 + WITH CHECK OPTION /* 尾注释 */";
    let error = parse_statement(sql).unwrap_err();
    assert_eq!(error.span.start, sql.len(), "{error:?}");
    assert_eq!(error.column, sql.chars().count() + 1);
}

#[test]
fn quality_explain_source_map_and_single_diagnostic_location() {
    let sql = "EXPLAIN /* 中文 */ TABLE t WHERE ) trailing";
    let error = parse_statement(sql).unwrap_err();
    assert_eq!(error.span.start, sql.find("TABLE").unwrap(), "{error:?}");
    assert_eq!(error.column, sql[..error.span.start].chars().count() + 1);
    assert!(!error.message.contains(" at Line: "), "{error}");
    assert!(error
        .to_string()
        .ends_with(&format!("at line {}, column {}", error.line, error.column)));
    let sql = "CREATE /*+\n NO_MERGE */ TABLE t (id INT)";
    let (masked, _, _) = strip_ddl_optimizer_hint(sql).unwrap();
    assert_eq!(masked.len(), sql.len());
    assert_eq!(masked.find("TABLE"), sql.find("TABLE"));
    assert_eq!(masked.find('\n'), sql.find('\n'));
}

#[test]
fn refactor_grouping_rewrites_preserve_quoted_text_and_sql_modes() {
    for bits in [
        0,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
    ] {
        let _mode = ParserSqlModeGuard::activate(bits);
        let sql = "SELECT 'GROUPING SETS (())', `GROUPING SETS`, '汉字' FROM t GROUP BY GROUPING SETS ((a), ()) /* GROUPING SETS (()) */";
        let first = rewrite::rewrite_grouping_sets_keyword(sql).unwrap();
        let second = rewrite::rewrite_empty_grouping_set_tuples(&first).unwrap();
        assert!(second.starts_with("SELECT 'GROUPING SETS (())', `GROUPING SETS`, '汉字' FROM t GROUP BY GROUPING_SETS ((a), ("));
        assert!(second.ends_with("())) /* GROUPING SETS (()) */"));
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!()
        };
        assert!(select.grouping_sets.iter().any(Vec::is_empty));
    }
    let _mode = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES);
    let sql = "SELECT 'slash\\' FROM t GROUP BY GROUPING SETS (())";
    assert!(rewrite::rewrite_grouping_sets_keyword(sql)
        .unwrap()
        .contains("GROUPING_SETS"));
    let sql = "SELECT 1 FROM t GROUP BY GROUPING_SETS ((a /* ) */), ())";
    assert!(rewrite::rewrite_empty_grouping_set_tuples(sql)
        .unwrap()
        .contains("__mydb_extension_"));
}

#[test]
fn refactor_subquery_inspection_covers_window_children_and_excludes_derived_scope() {
    for sql in [
        "SELECT (SELECT 1)",
        "SELECT SUM(a) OVER (PARTITION BY (SELECT 1)) FROM t",
        "SELECT CASE WHEN a=1 THEN (SELECT 1) ELSE 0 END FROM t",
    ] {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("{sql}");
        };
        assert!(visit::select_body_contains_subquery(&select), "{sql}");
    }
    let Statement::Select(select) =
        parse_statement("SELECT a FROM (SELECT (SELECT 1) AS a) d").unwrap()
    else {
        panic!();
    };
    assert!(!visit::select_body_contains_subquery(&select));
}
