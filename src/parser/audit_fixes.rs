//! Regression contracts from the October 2026 parser audit.
use super::*;

fn rejects(sql: &str) {
    assert!(
        matches!(
            parse_statement(sql),
            Err(_) | Ok(Statement::Unsupported { .. })
        ),
        "{sql}: {:?}",
        parse_statement(sql)
    );
}

#[test]
fn audit_query_fields_are_never_silently_discarded() {
    for sql in [
        "SELECT * FROM t START WITH a=1 CONNECT BY PRIOR a=b",
        "SELECT 1 FOR JSON AUTO",
        "SELECT 1 FOR XML AUTO",
        "SELECT 1 a UNION BY NAME SELECT 2 b",
        "SELECT 1 a UNION ALL BY NAME SELECT 2 b",
        "WITH c AS (SELECT 1 FOR JSON AUTO) SELECT * FROM c",
        "SELECT * FROM (SELECT 1 a UNION BY NAME SELECT 2 b) d",
    ] {
        rejects(sql);
    }
}

#[test]
fn audit_insert_values_rejects_query_tails() {
    for tail in [
        "LIMIT 1",
        "ORDER BY 1 DESC",
        "LIMIT 1 OFFSET 1",
        "FOR UPDATE",
    ] {
        for head in ["INSERT INTO", "REPLACE INTO", "INSERT IGNORE INTO"] {
            rejects(&format!("{head} t VALUES (1),(2) {tail}"));
        }
    }
    assert!(matches!(
        parse_statement("INSERT INTO t SELECT 1 UNION SELECT 2 LIMIT 1").unwrap(),
        Statement::Insert(_)
    ));
}

#[test]
fn audit_user_identifiers_cannot_inject_extension_semantics() {
    for name in [
        "__MYDB_EMPTY_ROW__",
        "`__MYDB_EMPTY_ROW__`",
        "__mydb_extension_0",
        "`__mydb_extension_1`",
    ] {
        let Statement::Insert(insert) =
            parse_statement(&format!("INSERT INTO t VALUES ({name})")).unwrap()
        else {
            panic!()
        };
        let InsertSource::Values(rows) = insert.source else {
            panic!()
        };
        assert_eq!(rows[0].len(), 1);
        assert!(matches!(&rows[0][0], Expr::Identifier(_)));
    }
    let Statement::Insert(insert) = parse_statement(
        "INSERT INTO t VALUES () RETURNING 5 AS __mydb_returning_old_78, 6 AS __mydb_extension_0",
    )
    .unwrap() else {
        panic!()
    };
    let returning = insert.returning.unwrap();
    assert_eq!(returning.projection.len(), 2);
    assert_eq!(returning.old_alias, None);
    assert!(matches!(insert.source, InsertSource::Values(ref rows) if rows == &[vec![]]));
    let Statement::Insert(insert) = parse_statement("INSERT INTO t VALUES (1) RETURNING WITH (OLD AS before_row, NEW AS after_row) 5 AS __mydb_returning_old_78, after_row.*").unwrap() else { panic!() };
    let returning = insert.returning.unwrap();
    assert_eq!(returning.projection.len(), 2);
    assert_eq!(returning.old_alias.as_deref(), Some("before_row"));
    assert_eq!(returning.new_alias.as_deref(), Some("after_row"));
}

#[test]
fn audit_defaults_validate_argument_shape() {
    for argument in ["", "1", "a+1", "a,b", "*", "(SELECT 1)"] {
        rejects(&format!("INSERT INTO t VALUES (DEFAULT({argument}))"));
    }
    for sql in [
        "INSERT INTO t VALUES(DEFAULT)",
        "INSERT INTO t VALUES(DEFAULT(a))",
        "UPDATE t SET a=DEFAULT",
    ] {
        assert!(parse_statement(sql).is_ok(), "{sql}");
    }
}

#[test]
fn audit_function_modifiers_have_explicit_capabilities() {
    for expr in [
        "SUM(x ORDER BY y)",
        "SUM(x LIMIT 1)",
        "GROUP_CONCAT(x LIMIT 1)",
        "COUNT(DISTINCT *)",
        "JSON_ARRAYAGG(DISTINCT x)",
        "COUNT(DISTINCT x) OVER ()",
    ] {
        rejects(&format!("SELECT {expr} FROM t"));
    }
    for expr in [
        "GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR ',')",
        "SUM(DISTINCT x)",
        "SUM(x) OVER ()",
        "COUNT(*)",
    ] {
        assert!(
            parse_statement(&format!("SELECT {expr} FROM t")).is_ok(),
            "{expr}"
        );
    }
}

#[test]
fn audit_admin_names_preserve_quoted_whitespace() {
    for name in ["a  b", "a\tb", "a\nb", "a``b"] {
        let decoded = name.replace("``", "`");
        let Statement::AnalyzeTable { tables, .. } =
            parse_statement(&format!("ANALYZE /* gap */ TABLE `{name}`")).unwrap()
        else {
            panic!()
        };
        assert_eq!(tables, [decoded.clone()]);
        let Statement::FlushTables { tables, .. } =
            parse_statement(&format!("FLUSH TABLES `{name}`")).unwrap()
        else {
            panic!()
        };
        assert_eq!(tables, [decoded]);
    }
}

#[test]
fn audit_unicode_identifiers_share_token_boundaries() {
    for name in ["a视图", "视图a", "a中2文", "_中", "$中"] {
        assert!(
            matches!(parse_statement(&format!("SAVEPOINT {name}")).unwrap(), Statement::Savepoint(n) if n == name)
        );
        assert!(
            matches!(parse_statement(&format!("CREATE VIEW {name} AS SELECT 1")).unwrap(), Statement::CreateView { name: n, .. } if n.object() == name)
        );
    }
    assert!(
        parse_statement("CREATE TABLE t(a INT) PARTITION BY HASH(a) (PARTITION p分区)").is_ok()
    );
}

#[test]
fn audit_version_comment_uses_five_digit_gate() {
    for (sql, expected) in [
        ("SELECT /*!1 + */ 2", "1+2"),
        ("SELECT /*!1234 + */ 2", "1234+2"),
        ("SELECT /*!800001+*/ 2", "1+2"),
    ] {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!()
        };
        let SelectItem::Expr { expr, .. } = &select.projection[0] else {
            panic!()
        };
        assert_eq!(expr, &parse_expr_fragment(expected).unwrap());
    }
}

#[test]
fn audit_primary_key_options_survive_create_and_alter() {
    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE t(id INT, PRIMARY KEY(id) COMMENT 'keep')").unwrap()
    else {
        panic!()
    };
    assert_eq!(table.indexes[0].comment, "keep");
    let Statement::AlterTable(table) =
        parse_statement("ALTER TABLE t ADD PRIMARY KEY(id) COMMENT 'keep'").unwrap()
    else {
        panic!()
    };
    assert!(matches!(&table.actions[0], AlterAction::AddIndex(index) if index.comment == "keep"));
}

#[test]
fn audit_native_function_spacing_is_expression_scoped() {
    assert!(
        parse_statement("CREATE TABLE t(count INT, FOREIGN KEY(count) REFERENCES count (id))")
            .is_ok()
    );
    rejects("SELECT COUNT (1)");
    assert!(parse_statement("SELECT app.count (1)").is_ok());
    let _mode = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_IGNORE_SPACE);
    assert!(parse_statement("SELECT COUNT (1)").is_ok());
}

#[test]
fn audit_rewritten_errors_use_original_coordinates() {
    for sql in [
        r"SELECT 'a\f', 1 +",
        "SELECT 1 INTO @target FROM",
        "/*!99999 中文 */ SELECT 1 +",
    ] {
        let error = parse_statement(sql).unwrap_err();
        assert_eq!(error.span.start, sql.len(), "{sql}: {error:?}");
        assert_eq!(error.column, sql.chars().count() + 1, "{sql}: {error:?}");
    }
}

#[test]
fn audit_concatenation_preserves_following_tokens_and_values() {
    let sql = format!(
        "SELECT {}, 2+3",
        std::iter::repeat_n("'x' /* gap */", 8000).collect::<String>()
    );
    let Statement::Select(select) = parse_statement(&sql).unwrap() else {
        panic!()
    };
    assert_eq!(select.projection.len(), 2);
    assert!(
        matches!(&select.projection[0], SelectItem::Expr { expr: Expr::Literal(Literal::String(s)), .. } if s == &"x".repeat(8000))
    );
}

#[test]
fn audit_legacy_rewrite_tail_errors_retain_original_offsets() {
    for sql in [
        "CREATE TABLE t(a TEXT(100), b)",
        "CREATE TABLE t(a TEXT(100), b TEXT(200), c)",
    ] {
        let error = parse_statement(sql).unwrap_err();
        assert_eq!(error.span.start, sql.len() - 1, "{sql}: {error:?}");
    }
}

#[test]
fn audit_wide_syntax_has_independent_memory_admission() {
    let mode = crate::sql_lex::SqlLexMode::default();
    let wide = format!(
        "SELECT {}",
        std::iter::repeat_n("1", 4000).collect::<Vec<_>>().join(",")
    );
    let narrow = format!("SELECT '{}'", "x".repeat(wide.len()));
    let before = crate::engine::controlled_memory_snapshot().2[12];
    let charge = |sql: &str| {
        let mut guard = ParserMemoryGuard::enter(sql).unwrap();
        guard.reserve_syntax(sql, mode).unwrap();
        let bytes = crate::engine::controlled_memory_snapshot().2[12] - before;
        drop(guard);
        assert_eq!(crate::engine::controlled_memory_snapshot().2[12], before);
        bytes
    };
    assert!(charge(&wide) > charge(&narrow) * 10);
}

#[test]
fn audit_parameters_keep_lexical_identity_through_extensions() {
    struct Collect(Vec<usize>);
    impl visit::AstVisitor for Collect {
        fn enter_limit(&mut self, limit: &LimitValue) {
            if let LimitValue::Placeholder(index) = limit {
                self.0.push(*index);
            }
        }
        fn enter_expr(&mut self, expr: &Expr) {
            if let Expr::Placeholder(index) = expr {
                self.0.push(*index);
            }
        }
    }
    for sql in [
        "INSERT INTO t(id) VALUES (?) RETURNING WITH (NEW AS n) ?,n.id",
        "WITH c AS(SELECT ? AS v) SELECT TRIM(? FROM ?),v FROM c ORDER BY ? LIMIT ?",
        "SELECT ? AS v FROM t FOR SHARE OF t NOWAIT",
    ] {
        let statement = parse_statement(sql).unwrap();
        let mut indices = Collect(Vec::new());
        visit::visit_statement(&mut indices, &statement);
        indices.0.sort_unstable();
        indices.0.dedup();
        assert_eq!(
            indices.0,
            (0..sql.bytes().filter(|byte| *byte == b'?').count()).collect::<Vec<_>>(),
            "{sql}"
        );
    }
}

#[test]
fn audit_not_encoder_preserves_operator_precedence() {
    for input in ["!a * 2", "(!a) = b", "(!a) + b", "NOT (a * 2)"] {
        let expr = parse_expr_fragment(input).unwrap();
        let encoded = try_expr_to_sql(&expr).unwrap();
        assert_eq!(
            parse_expr_fragment(&encoded).unwrap(),
            expr,
            "{input}: {encoded}"
        );
    }
}

#[test]
fn audit_rollup_bounds_expansion_before_allocation() {
    let sql = format!(
        "SELECT 1 GROUP BY ROLLUP({})",
        std::iter::repeat_n("1", 4096).collect::<Vec<_>>().join(",")
    );
    let error = parse_statement(&sql).unwrap_err();
    assert!(error.message.contains("4096"), "{error:?}");
    assert!(parse_statement("SELECT 1").is_ok());
}

#[test]
fn audit_alias_clone_expansion_is_admitted_and_released() {
    let sql = format!(
        "SELECT CONCAT('{}') AS k ORDER BY k,k,k",
        "x".repeat(10_000)
    );
    let before = crate::engine::controlled_memory_snapshot().2[12];
    let guard = ParserMemoryGuard::enter(&sql).unwrap();
    let base = crate::engine::controlled_memory_snapshot().2[12];
    let statement = parse_statement(&sql).unwrap();
    assert!(crate::engine::controlled_memory_snapshot().2[12] >= base + 30_000);
    drop(statement);
    drop(guard);
    assert_eq!(crate::engine::controlled_memory_snapshot().2[12], before);
}

#[test]
fn audit_digit_leading_hex_names_are_not_split_into_literals_and_aliases() {
    for name in ["0x", "0xZZ", "0xABZZ", "0x中", "0xAB_from"] {
        assert_eq!(
            parse_expr_fragment(name).unwrap(),
            Expr::Identifier(name.into())
        );
    }
    assert!(matches!(
        parse_expr_fragment("0xABC").unwrap(),
        Expr::Literal(Literal::HexBytes(_))
    ));
    assert!(
        matches!(parse_expr_fragment("X''").unwrap(),Expr::Literal(Literal::HexBytes(ref bytes)) if bytes.is_empty())
    );
}

#[test]
fn audit_plain_query_route_matches_canonical_semantics() {
    for mode in [
        0,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
        crate::engine::api::SQL_MODE_PIPES_AS_CONCAT,
    ] {
        let _mode = ParserSqlModeGuard::activate(mode);
        for sql in [
            "SELECT a,b FROM t WHERE id=42 ORDER BY b LIMIT 10",
            "SELECT *, id+1 AS result FROM t WHERE id BETWEEN 1 AND 20 ORDER BY result DESC",
            "SELECT a FROM t WHERE x IS NOT NULL AND y='quoted ; -- bait'",
            "SELECT 0xABZZ FROM t",
            "SELECT CURRENT_DATE FROM t",
            "DELETE FROM t WHERE id=? ORDER BY id LIMIT 1",
            "DELETE a,b FROM a,b WHERE a.id=b.id",
            "SELECT 1+2 AS x, 3*4 AS y ORDER BY x",
            "SELECT a,b FROM t GROUP BY a,b WITH ROLLUP",
        ] {
            if let Some(fast) = try_parse_plain_query_or_delete(sql).unwrap() {
                let slow = parse_statement_via_sqlparser_legacy(sql).unwrap();
                assert_eq!(fast, slow, "{sql}");
            }
        }
        for sql in [
            "SELECT 1 AS 123",
            "SELECT a 123 FROM t",
            "SELECT * FROM a LEFT JOIN b JOIN c ON b.id=c.id ON a.id=b.id",
            "SELECT * FROM t LOCK IN SHARE MODE",
            "DELETE LOW_PRIORITY QUICK FROM t",
        ] {
            assert!(
                try_parse_plain_query_or_delete(sql).unwrap().is_none(),
                "{sql}"
            );
        }
    }
}

#[test]
fn audit_nested_tail_scope_cannot_collide_with_user_names() {
    let sql = "(SELECT 1 AS a ORDER BY a LIMIT 1) ORDER BY __mydb_extension_0.a LIMIT 1";
    let Statement::Select(select) = parse_statement(sql).unwrap() else {
        panic!()
    };
    assert_ne!(select.from[0].alias.as_deref(), Some("__mydb_extension_0"));
}

#[test]
fn audit_hex_identifier_keeps_adjacent_quoted_alias() {
    for (sql, name, alias) in [
        ("SELECT 0xX'alias' FROM t", "0xX", "alias"),
        ("SELECT 0xN'名' FROM t", "0xN", "名"),
    ] {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!()
        };
        assert!(
            matches!(&select.projection[0],SelectItem::Expr {expr:Expr::Identifier(found),alias:Some(label)} if found==name && label==alias),
            "{sql}: {:?}",
            select.projection
        );
    }
}

#[test]
fn audit_large_hex_payload_is_not_counted_as_ast_nodes() {
    let sql = format!("SELECT 0x{}", "AB".repeat(1_048_576));
    let Statement::Select(select) = parse_statement(&sql).unwrap() else {
        panic!()
    };
    assert!(
        matches!(&select.projection[0],SelectItem::Expr {expr:Expr::Literal(Literal::HexBytes(bytes)),..} if bytes.len()==1_048_576)
    );
}

#[test]
fn audit_extension_reservation_does_not_depend_on_shallow_token_boundaries() {
    let sql="INSERT INTO t VALUES(1) RETURNING WITH (OLD AS o) 1e+2 AS __mydb_extension_0, 5 AS __mydb_returning_old_78";
    let Statement::Insert(insert) = parse_statement(sql).unwrap() else {
        panic!()
    };
    let returning = insert.returning.unwrap();
    assert_eq!(returning.projection.len(), 2);
    assert_eq!(returning.old_alias.as_deref(), Some("o"));
}

#[test]
fn audit_routine_labels_are_not_discarded_on_unlabelable_statements() {
    for body in [
        "x: SELECT 1",
        "x: SET @v=1",
        "x: IF 1 THEN SELECT 1; END IF",
        "``: BEGIN SELECT 1; END",
    ] {
        rejects(&format!("CREATE PROCEDURE p() BEGIN {body}; END"));
    }
}

#[test]
fn audit_error_between_multiple_type_rewrites_has_exact_source_position() {
    let sql = "CREATE TABLE t(a TEXT(100), b ?, c TEXT(70000))";
    let error = parse_statement(sql).unwrap_err();
    assert_eq!(error.span.start, sql.find('?').unwrap(), "{error:?}");
}

#[test]
fn audit_short_syntax_envelope_is_at_least_full_lexer_admission() {
    let before = crate::engine::controlled_memory_snapshot().2[12];
    for bits in [
        0,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
    ] {
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(bits);
        let charge = |sql: &str| {
            let mut guard = ParserMemoryGuard::enter(sql).unwrap();
            guard.reserve_syntax(sql, mode).unwrap();
            crate::engine::controlled_memory_snapshot().2[12] - before
        };
        for sql in [
            "SELECT foo$1, a中文, 1+2-3, 1e+2, 0xABZZ FROM t",
            "SELECT 'abc def', \"a b\", `a``b`, @a, @@sql_mode, ?",
            "UPDATE t SET v=42 WHERE a!=b AND id BETWEEN 1 AND 20",
            "SELECT 'a' 'b', _utf8mb4'中文', 0b101, x'01', n'text'",
        ] {
            let padded = format!("{}{sql}", " ".repeat(300));
            assert!(charge(sql) >= charge(&padded), "{bits}: {sql}");
            assert_eq!(crate::engine::controlled_memory_snapshot().2[12], before);
        }
    }
}

#[test]
fn audit_function_name_encoder_restores_required_quoting() {
    for bits in [0, 8, crate::engine::api::SQL_MODE_ANSI_QUOTES] {
        let _mode = ParserSqlModeGuard::activate(bits);
        for sql in [
            "SELECT()*2",
            "WITH()*2",
            "`select`()*2",
            "`a b`(1)+2",
            "db.`a b`(1)+2",
            "`a``b`(1)+2",
            "ABS(-1)+2",
            "COALESCE(NULL,1)",
        ] {
            let expression = parse_expr_fragment(sql).unwrap();
            let encoded = try_expr_to_sql(&expression).unwrap();
            assert_eq!(
                parse_expr_fragment(&encoded).unwrap(),
                expression,
                "{sql} -> {encoded}"
            );
        }
    }
    for sql in ["SELECT ROW()", "SELECT ROW(1)"] {
        rejects(sql);
    }
    assert!(try_expr_to_sql(&Expr::Tuple(Vec::new())).is_err());
    assert!(try_expr_to_sql(&Expr::Tuple(vec![Expr::Literal(Literal::Int(1))])).is_err());
}

#[test]
fn audit_plain_route_accepts_split_numeric_operands_without_alias_confusion() {
    for sql in [
        "SELECT 1+ 2,'literal' AS s FROM t WHERE id=?",
        "SELECT 1- 2",
        "SELECT 1+ -2",
    ] {
        let fast = try_parse_plain_query_or_delete(sql).unwrap().unwrap();
        let slow = parse_statement_via_sqlparser_legacy(sql).unwrap();
        assert_eq!(fast, slow, "{sql}");
    }
}

#[test]
fn audit_scalar_and_row_functions_do_not_drop_modifiers() {
    for sql in [
        "SELECT ABS(DISTINCT 1)",
        "SELECT ABS(ALL 1)",
        "SELECT ROW(1,2) OVER()",
        "SELECT DEFAULT(a) OVER()",
        "SELECT ROW(DISTINCT 1,2)",
        "SELECT DEFAULT(DISTINCT a)",
        "SELECT SUM(1) RESPECT NULLS OVER()",
        "SELECT STDDEV(DISTINCT 1)",
        "SELECT BIT_AND(DISTINCT 1)",
        "SELECT GROUP_CONCAT(ALL 1)",
        "SELECT MAX(*)",
        "SELECT COUNT(t.*)",
    ] {
        rejects(sql);
    }
    for sql in [
        "SELECT COUNT(ALL *)",
        "SELECT SUM(DISTINCT a)",
        "SELECT STDDEV(ALL a)",
        "SELECT JSON_ARRAYAGG(ALL a)",
        "SELECT FIRST_VALUE(a) RESPECT NULLS OVER()",
        "SELECT ROW(1,2) = ROW(1,2)",
    ] {
        assert!(parse_statement(sql).is_ok(), "{sql}");
    }
}

#[test]
fn audit_plain_updates_match_canonical_ast_and_parameter_identity() {
    for bits in [
        0,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
        crate::engine::api::SQL_MODE_PIPES_AS_CONCAT,
    ] {
        let _mode = ParserSqlModeGuard::activate(bits);
        for sql in [
            "UPDATE t SET v=v+1 WHERE id=42",
            "UPDATE t SET v=?, n=n+? WHERE id=?",
            "UPDATE /*+ NO_INDEX(t) */ t SET v=1 WHERE id=2",
            "UPDATE t SET v=CONCAT('a','b'),n=IFNULL(n,0) WHERE id BETWEEN 1 AND 3",
            "UPDATE t SET v='a\\nb' WHERE id=1",
            "UPDATE t SET v=0xAB WHERE id=2",
        ] {
            let fast = try_parse_plain_update_fast(sql).unwrap().unwrap();
            let slow = parse_statement_via_sqlparser_legacy(sql).unwrap();
            assert_eq!(fast, slow, "{bits}: {sql}");
        }
        for sql in [
            "UPDATE t SET v=1 ORDER BY id LIMIT 1",
            "UPDATE a JOIN b ON a.id=b.id SET a.v=b.v",
            "UPDATE t SET v=(SELECT 1)",
        ] {
            assert!(try_parse_plain_update_fast(sql).unwrap().is_none(), "{sql}");
        }
    }
}

#[test]
fn audit_literal_bulk_admission_falls_back_without_losing_semantics() {
    for bits in [0, crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES] {
        let _mode = ParserSqlModeGuard::activate(bits);
        for values in [
            "(1,'payload'),(2,'a,b')",
            "(-1,X'AB'),(+2,NULL)",
            "(1,1+2),(2,3+4)",
            "(1),(2,3)",
            "(1,TO_VECTOR('[1,2]'))",
            "(1),(2) RETURNING id",
        ] {
            let sql = format!(
                "/* {} */ INSERT INTO t VALUES {values}",
                "padding".repeat(50)
            );
            let fast = parse_statement(&sql).unwrap();
            let slow = parse_statement_via_sqlparser_legacy(&sql).unwrap();
            assert_eq!(fast, slow, "{bits}: {sql}");
        }
    }
    let sql = format!(
        "INSERT INTO t VALUES ({}){}",
        std::iter::repeat_n("1", 1000).collect::<Vec<_>>().join(","),
        ",(1)".repeat(1000)
    );
    assert!(literal_bulk_layout(&sql).is_none());
    assert!(try_parse_bulk_insert(&sql).is_none());
    assert!(parse_statement(&sql).is_ok());
}

#[test]
fn audit_modified_dml_fast_route_preserves_flags_and_literal_provenance() {
    for bits in [
        0,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
    ] {
        let _mode = ParserSqlModeGuard::activate(bits);
        for sql in [
            "UPDATE LOW_PRIORITY IGNORE t SET v=v+? WHERE id=?",
            "UPDATE IGNORE t SET v=X'' WHERE id=1",
            "DELETE LOW_PRIORITY QUICK IGNORE FROM t WHERE id=0xAB",
            "DELETE IGNORE FROM t WHERE v='a\\nb' ORDER BY id LIMIT 1",
        ] {
            let fast = try_parse_plain_modified_dml(sql).unwrap();
            let slow = parse_statement_via_sqlparser_legacy(sql).unwrap();
            assert_eq!(fast, slow, "{bits}: {sql}");
        }
    }
}

#[test]
fn audit_small_control_routes_preserve_modes_and_fallbacks() {
    for bits in [
        0,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
    ] {
        let _mode = ParserSqlModeGuard::activate(bits);
        for sql in [
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
            "SAVEPOINT `a  点`",
            "SET @@SESSION.autocommit=OFF",
            "SET SESSION TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE",
            "SET CHARACTER SET utf8mb4",
            "EXPLAIN FORMAT=JSON FOR /* target */ CONNECTION 7",
        ] {
            assert!(small_control_route(sql).is_some(), "{sql}");
            let fast = parse_statement(sql).unwrap();
            let slow =
                parse_effective_statement(sql, crate::sql_lex::SqlLexMode::from_parser_bits(bits))
                    .unwrap();
            assert_eq!(fast, slow, "{bits}: {sql}");
        }
        for sql in [
            "SET @x='autocommit'",
            "SET autocommit=1+0",
            "SET @@GLOBAL.autocommit=OFF",
            "SET sql_mode='ANSI_QUOTES'",
        ] {
            assert!(small_control_route(sql).is_none(), "{sql}");
        }
        let sql = "EXPLAIN FORMAT=TRADITIONAL SELECT CONNECTION_ID()";
        assert!(matches!(
            parse_statement(sql).unwrap(),
            Statement::Explain { .. }
        ));
    }
}

#[test]
fn audit_plain_explain_target_matches_upstream_lowering() {
    for sql in [
        "EXPLAIN SELECT id FROM t WHERE id=?",
        "EXPLAIN FORMAT=JSON SELECT a+1 AS n FROM t ORDER BY n LIMIT 2",
        "EXPLAIN ANALYZE FORMAT=TREE SELECT 0xAB FROM t",
    ] {
        let fast = parse_statement(sql).unwrap();
        let _raw = ParserRawSqlGuard::activate(sql);
        let mut parsed = adapter::parse_sql(&MydbDialect::default(), sql).unwrap();
        let slow = convert_statement(sql, parsed.pop().unwrap()).unwrap();
        assert_eq!(fast, slow, "{sql}");
    }
}

#[test]
fn audit_upstream_prefix_stack_is_bounded_during_scalar_roundtrip() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let before = crate::engine::controlled_memory_snapshot().2[12];
            for depth in [16, 30, 45, 60] {
                let sql = format!("{}1", "! ".repeat(depth));
                let expression = parse_expr_fragment(&sql).unwrap();
                let encoded = try_expr_to_sql(&expression).unwrap();
                assert_eq!(
                    parse_expr_fragment(&encoded).unwrap(),
                    expression,
                    "depth {depth}"
                );
                assert_eq!(crate::engine::controlled_memory_snapshot().2[12], before);
                assert!(parse_statement("SELECT 1").is_ok());
            }
            let _mode = ParserSqlModeGuard::activate(0x284d);
            let sql = format!(" {}@(", "!".repeat(93));
            if let Ok(expression) = parse_expr_fragment(&sql) {
                let encoded = try_expr_to_sql(&expression).unwrap();
                assert_eq!(parse_expr_fragment(&encoded).unwrap(), expression);
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn fuzz_round1_unsigned_literal_roundtrip() {
    for bits in [
        0,
        0x0a5f,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
    ] {
        let _mode = ParserSqlModeGuard::activate(bits);
        for value in [0, 10, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
            let mut expr = Expr::Literal(Literal::UInt(value));
            if value <= i64::MAX as u64 {
                assert!(try_expr_to_sql(&expr).is_err());
                expr = Expr::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(expr),
                };
            }
            for _ in 0..10 {
                let sql = try_expr_to_sql(&expr).unwrap();
                assert_eq!(parse_expr_fragment(&sql).unwrap(), expr, "{sql}");
                expr = Expr::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(expr),
                };
            }
        }
    }
}

#[test]
fn fuzz_round1_placeholder_cannot_be_persisted() {
    for expr in [
        Expr::Placeholder(0),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(Expr::Placeholder(7)),
        },
    ] {
        assert!(try_expr_to_sql(&expr)
            .unwrap_err()
            .to_string()
            .contains("parameter identity"));
        assert!(super::print::expr_to_catalog_sql(&expr).is_err());
    }
}

#[test]
fn fuzz_round1_unsigned_numeric_contexts_and_binary_literals() {
    for sql in [
        "ABS(0xa)",
        "ROUND(0xa, 0)",
        "NOT 0xa",
        "~0xa",
        "0xa AND 1",
        "0xa OR 0xb",
        "0xa XOR 0xb",
        "COALESCE(-0xa, 1)",
        "0xa",
        "X'000A'",
        "0xa + 1",
        "COALESCE(0xa, 1)",
    ] {
        let expr = parse_expr_fragment(sql).unwrap();
        let encoded = try_expr_to_sql(&expr).unwrap();
        assert_eq!(
            parse_expr_fragment(&encoded).unwrap(),
            expr,
            "{sql} -> {encoded}"
        );
        let catalog = super::print::expr_to_catalog_sql(&expr).unwrap();
        assert_eq!(parse_expr_fragment(&catalog).unwrap(), expr);
    }
}

#[test]
fn fuzz_round1_quoted_user_variable_identity() {
    for bits in [
        0,
        0x402f,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
    ] {
        let _mode = ParserSqlModeGuard::activate(bits);
        for name in ["@", "@@", "@@global.x", "?0", "a b", "a`b"] {
            let sql = format!("@`{}`", name.replace('`', "``"));
            let expr = parse_expr_fragment(&sql).unwrap();
            assert_eq!(expr, Expr::UserVariable(name.into()));
            let encoded = try_expr_to_sql(&expr).unwrap();
            assert_eq!(parse_expr_fragment(&encoded).unwrap(), expr);
        }
        for sql in ["@@sql_mode", "@@session.sql_mode", "@@global.sql_mode"] {
            let expr = parse_expr_fragment(sql).unwrap();
            assert!(matches!(expr, Expr::SystemVariable { .. }));
            assert_eq!(
                parse_expr_fragment(&try_expr_to_sql(&expr).unwrap()).unwrap(),
                expr
            );
        }
    }
}
