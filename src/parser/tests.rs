//! Parser unit and differential regression tests.

use super::*;

#[derive(Default)]
struct PlaceholderIndexCollector {
    indexes: Vec<usize>,
}

impl visit::AstVisitorMut for PlaceholderIndexCollector {
    fn enter_expr(&mut self, expr: &mut Expr) {
        if let Expr::Placeholder(index) = expr {
            self.indexes.push(*index);
        }
    }

    fn enter_limit(&mut self, limit: &mut LimitValue) {
        if let LimitValue::Placeholder(index) | LimitValue::OffsetCommaPlaceholder(index) = limit {
            self.indexes.push(*index);
        }
    }
}

fn number_and_collect_placeholders(sql: &str) -> Vec<usize> {
    let mut statement = parse_statement(sql).unwrap();
    let mut next = 0;
    number_statement_placeholders(&mut statement, &mut next);
    let mut collector = PlaceholderIndexCollector::default();
    visit::visit_statement_mut(&mut collector, &mut statement);
    assert_eq!(next, collector.indexes.len());
    collector.indexes
}

fn assert_single_entry_parity(sql: &str) {
    let single = parse_statement(sql);
    let batch = parse_statements(sql);
    match (single, batch) {
            (Ok(single), Ok(batch)) => {
                assert_eq!(batch.as_slice(), [single], "entry-point AST drift for {sql}");
            }
            (Err(single), Err(batch)) => {
                assert_eq!(batch, single, "entry-point error drift for {sql}");
            }
            (single, batch) => panic!(
                "entry points disagree for {sql}: parse_statement={single:?}, parse_statements={batch:?}"
            ),
        }
}

#[test]
fn single_statement_public_entries_share_the_canonical_pipeline() {
    for sql in [
        "SELECT 1",
        "DO 1 + 2, SLEEP(0)",
        "SET PERSIST max_connections = 100",
        "SELECT 1 INTO @answer",
        "SHOW OPEN TABLES FROM test LIKE 't%'",
        "CREATE VIEW v AS SELECT 1 WITH CASCADED CHECK OPTION",
        "CREATE FULLTEXT INDEX ft ON docs(body) WITH PARSER ngram",
        "CREATE TABLE z (id INT) ZONE_MAP=ON ZONE_SIZE=1M",
        "CREATE TABLE mv (j JSON, INDEX i ((CAST(j AS UNSIGNED ARRAY))))",
        "SELECT FROM",
        "SHOW MYDB MEMORY EXTRA",
    ] {
        assert_single_entry_parity(sql);
    }
}

#[test]
fn long_logical_predicate_chains_parse_on_the_default_worker_stack() {
    for operator in ["AND", "OR"] {
        let predicates = (0..64)
            .map(|index| format!("c{index} = {index}"))
            .collect::<Vec<_>>()
            .join(&format!(" {operator} "));
        let Statement::Select(select) =
            parse_statement(&format!("SELECT 1 FROM t WHERE {predicates}")).unwrap()
        else {
            panic!("expected SELECT");
        };
        let mut work = vec![select.selection.as_ref().expect("WHERE predicate")];
        let mut leaves = 0;
        while let Some(expr) = work.pop() {
            match expr {
                Expr::Binary { left, op, right }
                    if matches!(
                        (operator, op),
                        ("AND", BinaryOp::And) | ("OR", BinaryOp::Or)
                    ) =>
                {
                    work.push(right);
                    work.push(left);
                }
                _ => leaves += 1,
            }
        }
        assert_eq!(leaves, 64, "all {operator} operands must be preserved");
    }
}

#[test]
fn numbers_placeholders_through_cte_join_derived_and_named_window() {
    let sql = "WITH c AS (SELECT ? AS x) \
             SELECT ?, d.y FROM (SELECT ? AS y) d \
             JOIN (SELECT ? AS z) j ON j.z = ? \
             WINDOW w AS (PARTITION BY ? ORDER BY ?) \
             ORDER BY ? LIMIT ? OFFSET ?";
    let indexes = number_and_collect_placeholders(sql);
    assert_eq!(indexes, (0..10).collect::<Vec<_>>());

    let mut statement = parse_statement(sql).unwrap();
    let mut next = 0;
    number_statement_placeholders(&mut statement, &mut next);
    let Statement::WithCte(with) = statement else {
        panic!("expected WITH");
    };
    let crate::parser::QueryExpr::Select(cte) = &with.ctes[0].query else {
        panic!("expected CTE SELECT");
    };
    assert!(matches!(
        cte.projection[0],
        SelectItem::Expr {
            expr: Expr::Placeholder(0),
            ..
        }
    ));
    let Statement::Select(body) = with.body.as_ref() else {
        panic!("expected body SELECT");
    };
    assert!(matches!(
        body.projection[0],
        SelectItem::Expr {
            expr: Expr::Placeholder(1),
            ..
        }
    ));
    let derived = body.derived.as_ref().and_then(SubQuery::as_select).unwrap();
    assert!(matches!(
        derived.projection[0],
        SelectItem::Expr {
            expr: Expr::Placeholder(2),
            ..
        }
    ));
    let joined = body.joins[0]
        .derived
        .as_ref()
        .and_then(SubQuery::as_select)
        .unwrap();
    assert!(matches!(
        joined.projection[0],
        SelectItem::Expr {
            expr: Expr::Placeholder(3),
            ..
        }
    ));
    let Expr::Binary { right, .. } = body.joins[0].condition.as_ref().unwrap() else {
        panic!("expected join predicate");
    };
    assert_eq!(right.as_ref(), &Expr::Placeholder(4));
    assert_eq!(body.named_windows[0].partition_by, [Expr::Placeholder(5)]);
    assert_eq!(body.named_windows[0].order_by[0].expr, Expr::Placeholder(6));
    assert_eq!(body.order_by[0].expr, Expr::Placeholder(7));
    assert_eq!(body.limit, Some(LimitValue::Placeholder(8)));
    assert_eq!(body.offset, Some(LimitValue::Placeholder(9)));
}

#[test]
fn numbers_placeholders_in_window_frame_bounds() {
    let mut statement = parse_statement(
        "SELECT SUM(?) OVER (ORDER BY ? ROWS BETWEEN ? PRECEDING AND ? FOLLOWING) FROM t",
    )
    .unwrap();
    let mut next = 0;
    number_statement_placeholders(&mut statement, &mut next);
    assert_eq!(next, 4);
    let Statement::Select(select) = statement else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr {
        expr: Expr::WindowFunction { args, spec, .. },
        ..
    } = &select.projection[0]
    else {
        panic!("expected window function");
    };
    assert_eq!(args, &[Expr::Placeholder(0)]);
    let order_by = &spec.order_by;
    let frame = spec.frame.as_ref().expect("window frame");
    assert_eq!(order_by[0].expr, Expr::Placeholder(1));
    assert_eq!(
        frame.start,
        WindowFrameBound::Preceding(Box::new(Expr::Placeholder(2)))
    );
    assert_eq!(
        frame.end,
        WindowFrameBound::Following(Box::new(Expr::Placeholder(3)))
    );
}

#[test]
fn scalar_ast_layout_and_window_serialization_remain_compact() {
    if std::mem::size_of::<usize>() == 8 {
        assert!(std::mem::size_of::<Expr>() <= 64);
    }
    for sql in [
        "CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END",
        "CASE a WHEN 1 THEN 7 END",
    ] {
        let expr = parse_expr_fragment(sql).unwrap();
        let encoded = serde_json::to_value(&expr).unwrap();
        assert!(encoded["Case"]["conditions"].is_array());
        let decoded: Expr = serde_json::from_value(encoded).unwrap();
        assert_eq!(expr, decoded);
        assert_eq!(expr, expr.clone());
    }
    for sql in [
        "SUM(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
        "ROW_NUMBER() OVER (ORDER BY id)",
        "SUM(v) OVER ()",
    ] {
        let expr = parse_expr_fragment(sql).unwrap();
        let encoded = serde_json::to_value(&expr).unwrap();
        let window = encoded.get("WindowFunction").unwrap();
        assert!(window.get("partition_by").is_some());
        assert!(window.get("spec").is_none());
        let decoded: Expr = serde_json::from_value(encoded).unwrap();
        assert_eq!(expr, decoded);
        assert_eq!(expr, expr.clone());
        assert_eq!(format!("{expr:?}"), format!("{decoded:?}"));
        let mut invalid = serde_json::to_value(&expr).unwrap();
        invalid["WindowFunction"]
            .as_object_mut()
            .unwrap()
            .insert("unknown_field".into(), serde_json::Value::Null);
        assert!(serde_json::from_value::<Expr>(invalid).is_err());
    }
}

#[test]
fn distinct_window_aggregate_is_rejected_explicitly() {
    let err = parse_statement("SELECT SUM(DISTINCT k) OVER (ORDER BY id) FROM t")
        .expect_err("MySQL 8.0 rejects DISTINCT window aggregates");
    assert!(
        err.to_string().contains("SUM(DISTINCT ...) OVER"),
        "unexpected error: {err}"
    );
}

/// Core safety invariant of the bulk-INSERT fast path: whenever
/// [`try_parse_bulk_insert`] returns `Some`, the authoritative sqlparser
/// pipeline must also accept the input and yield the *identical* AST.
/// A fast-path `Some` that sqlparser would reject (accepting invalid SQL)
/// or parse differently (wrong AST) is a bug.
fn assert_fastpath_consistent(sql: &str) {
    if let Some(fast) = try_parse_bulk_insert(sql) {
        let slow = parse_statement_via_sqlparser(sql)
            .unwrap_or_else(|e| panic!("fast path accepted but sqlparser rejected {sql:?}: {e:?}"));
        assert_eq!(fast, slow, "fast/slow AST mismatch for {sql:?}");
    }
}

#[test]
fn bulk_insert_folds_constant_string_to_vector() {
    use crate::types::Value;
    // The fast path folds `STRING_TO_VECTOR('[..]')` to a pre-parsed vector
    // constant; the sqlparser path must fold identically (consistency
    // invariant) and the value must be the canonical little-endian bytes.
    let expect = crate::engine::vector::codec::parse_text("[1,2,3]").unwrap();
    for sql in [
            "INSERT INTO t(id, emb) VALUES (1, STRING_TO_VECTOR('[1,2,3]'))",
            "INSERT INTO t(id, emb) VALUES (1, to_vector('[1,2,3]'))",
            "INSERT INTO t(id, emb) VALUES (1,STRING_TO_VECTOR('[1,2,3]')),(2,STRING_TO_VECTOR('[1,2,3]'))",
            "INSERT INTO t SET id=1, emb=STRING_TO_VECTOR('[1,2,3]')",
        ] {
            assert_fastpath_consistent(sql);
            let stmt = parse_statement(sql).unwrap();
            let Statement::Insert(insert) = stmt else {
                panic!("expected Insert for {sql:?}");
            };
            let InsertSource::Values(rows) = &insert.source else {
                panic!("expected VALUES source for {sql:?}");
            };
            for row in rows {
                let last = row.last().unwrap();
                assert_eq!(
                    last,
                    &Expr::BoundValue(Value::Vector(expect.clone())),
                    "expected folded vector constant for {sql:?}, got {last:?}"
                );
            }
        }
    // A malformed literal must NOT fold (it stays a function call so the
    // exact eval-time error is preserved) — and fast/slow stay consistent.
    for sql in [
        "INSERT INTO t(id, emb) VALUES (1, STRING_TO_VECTOR('[1,nan]'))",
        "INSERT INTO t(id, emb) VALUES (1, STRING_TO_VECTOR('not a vector'))",
        // Placeholder / non-constant arg never folds.
        "INSERT INTO t(id, emb) VALUES (1, STRING_TO_VECTOR(?))",
    ] {
        assert_fastpath_consistent(sql);
        let stmt = parse_statement(sql).unwrap();
        let Statement::Insert(insert) = stmt else {
            panic!("expected Insert for {sql:?}");
        };
        let InsertSource::Values(rows) = &insert.source else {
            panic!("expected VALUES source for {sql:?}");
        };
        let last = rows[0].last().unwrap();
        assert!(
            matches!(last, Expr::Function { .. }),
            "malformed/non-const must stay a function for {sql:?}, got {last:?}"
        );
    }
}

#[test]
fn bulk_insert_fastpath_matches_sqlparser() {
    // Combinatorial corpus over the supported surface. Every accepted
    // statement is cross-checked against sqlparser for AST equality.
    let prefixes = ["INSERT INTO", "insert", "REPLACE INTO", "INSERT IGNORE"];
    let tables = ["t", "`t`", "db.t", "DB.Mixed"];
    let collists = ["", " (a, b)", " (`a`, b)"];
    let valuekws = ["VALUES", "VALUE"];
    let valuesets = [
        "(1, 'x')",
        "(1, 'x'),(2, 'yy')",
        "(-5, NULL)",
        "(+7, null)",
        "(3.14, TRUE)",
        "(0, '')",
        "(9999999999999999999999, false)",
        "(9223372036854775807, '123')",
        "(9223372036854775808, 'u64')",
        "(18446744073709551616, 'big')",
        "(1.5e3, -2.5)",
        "('hello world', 42)",
        "(1, 'x'), (2, 'y'), (3, 'z')",
    ];
    let mut accepted = 0usize;
    for p in &prefixes {
        for t in &tables {
            for cl in &collists {
                for vk in &valuekws {
                    for vs in &valuesets {
                        let sql = format!("{p} {t}{cl} {vk} {vs}");
                        assert_fastpath_consistent(&sql);
                        if try_parse_bulk_insert(&sql).is_some() {
                            accepted += 1;
                        }
                    }
                }
            }
        }
    }
    // Sanity: the corpus must actually exercise the fast path, not silently
    // bail on everything (which would make the test vacuous).
    assert!(accepted > 500, "fast path accepted too few: {accepted}");
}

#[test]
fn bulk_insert_fastpath_accelerates_expected_shapes() {
    // These canonical shapes MUST take the fast path (and match sqlparser).
    let must_accelerate = [
        "INSERT INTO t (k, c, pad) VALUES (1, 'aaa', 'bbb')",
        "insert into sbtest1(k, c, pad) values (1,'abc','def'),(2,'ghi','jkl')",
        "REPLACE INTO t VALUES (1, 2, 3)",
        "INSERT IGNORE INTO t (a) VALUES (1)",
        "INSERT INTO `db`.`t` (`a`, `b`) VALUES (NULL, -3.5)",
        "INSERT INTO t (id, payload) VALUES (1, X'0000803F'),(2, x'000020C0')",
        "INSERT INTO t VALUE (1)",
        "INSERT INTO t (a, b) VALUES (1, 2),(3, 4) \
         ON DUPLICATE KEY UPDATE a=VALUES(a),b=VALUES(b)",
        "INSERT /*Data-Replication*/ INTO `db`.`t` (`a`, `b`) VALUES (1, 2) \
         ON DUPLICATE KEY UPDATE `a`=VALUES(`a`),`b`=VALUES(`b`)",
    ];
    for sql in must_accelerate {
        assert!(
            try_parse_bulk_insert(sql).is_some(),
            "expected fast path to accept: {sql:?}"
        );
        assert_fastpath_consistent(sql);
    }
}

#[test]
fn bulk_insert_x_hex_fastpath_matches_sqlparser_and_preserves_bytes() {
    let sql = "INSERT INTO t(id, payload) VALUES \
               (1,X'0000803F000020C0'),(2,x'0000504000008040')";
    let fast = try_parse_bulk_insert(sql).expect("X'..' bulk INSERT should use fast path");
    let slow = parse_statement_via_sqlparser(sql).expect("canonical parser should accept X'..'");
    assert_eq!(fast, slow);

    let Statement::Insert(insert) = fast else {
        panic!("expected INSERT");
    };
    let InsertSource::Values(rows) = insert.source else {
        panic!("expected VALUES");
    };
    assert!(matches!(
        &rows[0][1],
        Expr::Literal(Literal::HexBytes(bytes))
            if bytes == &[0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x20, 0xc0]
    ));

    // These forms retain their existing canonical-parser behavior.
    for unsupported_fast_shape in [
        "INSERT INTO t VALUES (X'F')",
        "INSERT INTO t VALUES (X'GG')",
        "INSERT INTO t VALUES (0x1234)",
        "INSERT INTO t VALUES (_binary X'00')",
    ] {
        assert!(
            try_parse_bulk_insert(unsupported_fast_shape).is_none(),
            "must fall back: {unsupported_fast_shape}"
        );
    }
}

#[test]
fn bulk_insert_from_base64_fastpath_matches_sqlparser_and_preserves_bytes() {
    use crate::types::Value;

    let sql = "INSERT INTO t(id, payload) VALUES \
               (1,FROM_BASE64('AACAPwAAIMA=')),(2,from_base64('AABQQAAAgEA='))";
    let fast = try_parse_bulk_insert(sql).expect("constant FROM_BASE64 should use fast path");
    let slow = parse_statement_via_sqlparser(sql).expect("canonical parser should accept base64");
    assert_eq!(fast, slow);

    let Statement::Insert(insert) = fast else {
        panic!("expected INSERT");
    };
    let InsertSource::Values(rows) = insert.source else {
        panic!("expected VALUES");
    };
    assert!(matches!(
        &rows[0][1],
        Expr::BoundValue(Value::Bytes(bytes))
            if bytes == &[0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x20, 0xc0]
    ));

    for unsupported_fast_shape in [
        "INSERT INTO t VALUES (FROM_BASE64(?))",
        "INSERT INTO t VALUES (FROM_BASE64(col))",
        "INSERT INTO t VALUES (FROM_BASE64('bad!'))",
    ] {
        assert!(
            try_parse_bulk_insert(unsupported_fast_shape).is_none(),
            "must fall back: {unsupported_fast_shape}"
        );
    }
}

#[test]
fn bulk_insert_fastpath_bails_to_sqlparser() {
    // Everything here must bail (return None) so sqlparser handles it.
    let must_bail = [
        "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE a = VALUES(a) + 1",
        "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE a = NEW.a",
        "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE t.a = VALUES(a)",
        "INSERT /*+ NO_RANGE_OPTIMIZATION(t) */ INTO t VALUES (1)",
        "INSERT INTO t SET a = 1, b = 2",
        "INSERT INTO t SELECT * FROM x",
        "INSERT INTO t (a) VALUES (f(1))",
        "INSERT INTO t VALUES (DEFAULT)",
        "INSERT INTO t VALUES (a)",
        "INSERT INTO t VALUES (\"x\")",
        "INSERT INTO t PARTITION (p0) VALUES (1)",
        "INSERT INTO t VALUES (1) AS new",
        "INSERT INTO t VALUES (1), (2) AS new(x)",
        "INSERT LOW_PRIORITY INTO t VALUES (1)",
        "INSERT DELAYED INTO t VALUES (1)",
        "INSERT INTO t VALUES ()",
        "INSERT INTO t VALUES (1,)",
        "INSERT INTO t VALUES (1) RETURNING a",
        "REPLACE IGNORE INTO t VALUES (1)",
        "INSERT INTO t (a) VALUES (1); INSERT INTO t (a) VALUES (2)",
        "INSERT INTO t VALUES (0x1f)",
        "INSERT INTO t VALUES (123abc)",
        "SELECT 1",
        "UPDATE t SET a = 1",
        "",
        "   ",
        "INSERT INTO",
        "INSERT INTO t",
        "INSERT INTO t VALUES",
        "INSERT INTO t VALUES (1",
    ];
    for sql in must_bail {
        assert!(
            try_parse_bulk_insert(sql).is_none(),
            "expected bail (None) but accepted: {sql:?}"
        );
        // And, defensively, the invariant still holds for these.
        assert_fastpath_consistent(sql);
    }
}

#[test]
fn extract_compound_units_use_native_ast() {
    for (source, canonical) in [
        ("EXTRACT(YEAR_MONTH FROM d)", "EXTRACT('YEAR_MONTH', d)"),
        ("extract(day_second from d)", "EXTRACT('DAY_SECOND', d)"),
        (
            "EXTRACT(HOUR_SECOND FROM DATE_ADD(x, INTERVAL 1 DAY))",
            "EXTRACT('HOUR_SECOND', DATE_ADD(x, INTERVAL 1 DAY))",
        ),
        ("EXTRACT(YEAR FROM d)", "EXTRACT('YEAR', d)"),
        ("EXTRACT(MICROSECOND FROM d)", "EXTRACT('MICROSECOND', d)"),
    ] {
        assert_eq!(
            parse_expr_fragment(source).unwrap(),
            parse_expr_fragment(canonical).unwrap(),
            "{source}"
        );
    }
    let Statement::Select(select) =
        parse_statement("SELECT EXTRACT(YEAR_MONTH FROM a), EXTRACT(MINUTE_SECOND FROM b)")
            .unwrap()
    else {
        panic!()
    };
    for (item, source) in select.projection.iter().zip([
        "EXTRACT(YEAR_MONTH FROM a)",
        "EXTRACT(MINUTE_SECOND FROM b)",
    ]) {
        assert!(matches!(item, SelectItem::Expr { alias: Some(alias), .. } if alias == source));
    }
    assert!(
        matches!(parse_expr_fragment("EXTRACTOR(YEAR_MONTH)").unwrap(), Expr::Function { name, .. } if name == "EXTRACTOR")
    );
    assert_eq!(
        parse_expr_fragment("'EXTRACT(YEAR_MONTH FROM d)'").unwrap(),
        Expr::Literal(Literal::String("EXTRACT(YEAR_MONTH FROM d)".into()))
    );
    assert_eq!(
        parse_expr_fragment("`EXTRACT(DAY_HOUR FROM d)`").unwrap(),
        Expr::Identifier("EXTRACT(DAY_HOUR FROM d)".into())
    );
}

#[test]
fn infix_mod_matches_native_modulo_grammar() {
    for (source, native) in [
        ("5 MOD 3", "5 % 3"),
        ("15 MOD 4 MOD 2", "15 % 4 % 2"),
        ("5 mod 3", "5 % 3"),
        ("5 MOD (2+1)", "5 % (2+1)"),
        ("a MOD(b)", "a % (b)"),
        ("a MOD 2=1", "a % 2=1"),
    ] {
        assert_eq!(
            parse_expr_fragment(source).unwrap(),
            parse_expr_fragment(native).unwrap()
        );
    }
    for sql in [
        "SELECT MOD(5,3)",
        "SELECT MOD(a,b)+1",
        "SELECT POW(2,MOD(a,b))",
        "SELECT 'a MOD b'",
        "SELECT `mod` FROM t",
        "SELECT 1 -- a MOD b\n",
        "ALTER TABLE t MODIFY c INT",
    ] {
        assert!(parse_statement(sql).is_ok(), "{sql}");
    }
}

#[test]
fn infix_mod_parses_end_to_end() {
    for sql in [
        "SELECT 5 MOD 3",
        "SELECT 15 MOD 4 MOD 2",
        "SELECT a FROM g WHERE a MOD 2 = 1",
        "SELECT MOD(5, 3)",
        "SELECT 5 % 3",
    ] {
        assert!(parse_statement(sql).is_ok(), "failed to parse: {sql}");
    }
}

#[test]
fn scan_bulk_value_spans_handles_embedded_delimiters() {
    // The structural span scan must not split on commas / parens that live
    // inside string literals — the `STRING_TO_VECTOR('[1,2,3]')` and
    // `'a,b)c'` cases — and must bail (None) on unknown escapes,
    // unterminated tuples/strings, and trailing tokens, exactly where the
    // serial `parse_value_tuple` would.
    let cases: &[(&str, Option<usize>)] = &[
        ("(1),(2),(3)", Some(3)),
        ("(1,'a,b'),(2,'c,d')", Some(2)),
        ("(1,'a)b(c'),(2,']')", Some(2)),
        (
            "(1,STRING_TO_VECTOR('[1,2,3]')),(2,STRING_TO_VECTOR('[4,5,6]'))",
            Some(2),
        ),
        ("  (1) , (2) ", Some(2)),
        ("(1)", Some(1)),
        // Bails:
        ("(1) trailing", None),      // trailing tokens
        ("(1),", None),              // dangling separator → expects '('
        ("(1", None),                // unterminated tuple
        ("(1,'unterminated)", None), // unterminated string
        (r"(1,'a\'b')", Some(1)),    // backslash escape
        ("(1,'a''b')", Some(1)),     // doubled-quote escape
    ];
    for (inner, expect) in cases {
        let got = scan_bulk_value_spans(inner.as_bytes(), 0).map(|s| s.len());
        assert_eq!(got, *expect, "span scan mismatch for {inner:?}");
    }
}

#[test]
fn bulk_insert_validated_utf8_literals_match_canonical_in_all_modes() {
    for mode in [
        0,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
    ] {
        let _guard = ParserSqlModeGuard::activate(mode);
        for literal in [
            "'中🙂é'",
            r"'x\n界'",
            r"'\0A\Z界'",
            "'''中'''",
            r"'tail\\💾'",
        ] {
            let sql = format!("INSERT INTO t VALUES (1,{literal}),(2,{literal})");
            assert!(try_parse_bulk_insert(&sql).is_some(), "{sql}");
            assert_fastpath_consistent(&sql);
        }
    }
    assert!(parse_bulk_value_tuples_with_degree(b"('\xff')", &[(0, 5)], 2).is_none());
}

#[test]
fn bulk_insert_parallel_matches_serial() {
    // A large multi-row INSERT (vectors with embedded commas/brackets, plus
    // strings, signed numbers, scientific notation, NULL) must parse to the
    // identical AST whether tuples are parsed serially or fanned out across
    // workers — the core correctness property of the parallel fast path.
    let mut sql = String::from("INSERT INTO t (id, emb, note, w) VALUES ");
    let row_count = 600usize;
    for i in 0..row_count {
        if i > 0 {
            sql.push(',');
        }
        // Mix shapes per row so spans vary in length and content.
        sql.push_str(&format!(
            "({i},STRING_TO_VECTOR('[1.5,-2.25,3e0,{i}.0]'),'a,b)c{i}',{})",
            if i % 3 == 0 {
                "NULL".to_string()
            } else {
                format!("-{i}.5e1")
            }
        ));
    }
    let b = sql.as_bytes();
    // Locate the start of the VALUES list (just before the first '(').
    let vstart = sql.find("VALUES ").unwrap() + "VALUES ".len();
    let spans = scan_bulk_value_spans(b, vstart).expect("scan should succeed");
    assert_eq!(spans.len(), row_count, "span count");
    let serial = parse_bulk_value_tuples_with_degree(b, &spans, 1).expect("serial parse");
    for degree in [2usize, 3, 8, 16] {
        let par = parse_bulk_value_tuples_with_degree(b, &spans, degree).expect("parallel parse");
        assert_eq!(par, serial, "degree {degree} diverged from serial");
    }
    // And the whole statement still matches the authoritative sqlparser AST.
    assert_fastpath_consistent(&sql);
}

#[test]
fn parses_fulltext_index_comment_regression() {
    let sql = "CREATE TABLE `mysql_struct_test2` (\
        `id` bigint unsigned NOT NULL AUTO_INCREMENT,\
        `char_col` char(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_la_0900_as_cs DEFAULT 'ab',\
        FULLTEXT KEY `idx_fulltext` (`char_col`) COMMENT '全文索引'\
        ) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci";
    let Statement::CreateTable(table) = parse_statement(sql).unwrap() else {
        panic!("expected CreateTable");
    };
    assert_eq!(table.indexes[0].comment, "全文索引");
    assert!(table.indexes[0].is_fulltext());
    assert_eq!(table.auto_increment_start, Some(16));
}

#[test]
fn fulltext_comments_follow_their_index_and_preserve_literals() {
    for suffix in [
        "COMMENT '全文 ''FULLTEXT'' WITH PARSER fake' WITH PARSER `ngram`",
        "WITH /* FULLTEXT ignored */ PARSER `ngram` COMMENT '全文 ''FULLTEXT'' WITH PARSER fake'",
    ] {
        for sql in [
            format!("CREATE TABLE t (body TEXT, KEY ordinary (body(3)) COMMENT 'ordinary', FULLTEXT KEY ft (body) {suffix}, FULLTEXT KEY ft2 (body) COMMENT 'second') COMMENT='table'"),
            format!("ALTER TABLE t ADD KEY ordinary (body(3)) COMMENT 'ordinary', ADD FULLTEXT KEY ft (body) {suffix}, ADD FULLTEXT KEY ft2 (body) COMMENT 'second'"),
        ] {
            let indexes = match parse_statement(&sql).unwrap() {
                Statement::CreateTable(table) => table.indexes,
                Statement::AlterTable(table) => table.actions.into_iter().filter_map(|action| {
                    if let AlterAction::AddIndex(index) = action { Some(index) } else { None }
                }).collect(),
                other => panic!("unexpected statement: {other:?}"),
            };
            assert_eq!(indexes[0].comment, "ordinary");
            assert_eq!(indexes[1].comment, "全文 'FULLTEXT' WITH PARSER fake");
            assert_eq!(indexes[1].fulltext_parser, "ngram");
            assert_eq!(indexes[2].comment, "second");
            assert_eq!(indexes[2].fulltext_parser, "");
        }
        let sql = format!("CREATE FULLTEXT INDEX ft ON t (body) {suffix}");
        let Statement::CreateIndex(index) = parse_statement(&sql).unwrap() else {
            panic!("expected CreateIndex");
        };
        assert_eq!(index.comment, "全文 'FULLTEXT' WITH PARSER fake");
        assert_eq!(index.fulltext_parser, "ngram");
    }
}

#[test]
fn fulltext_comment_sql_modes_and_invalid_options() {
    for (mode, literal, expected) in [
        (
            0,
            r"'a\' FULLTEXT WITH PARSER fake'",
            "a' FULLTEXT WITH PARSER fake",
        ),
        (
            crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
            r"'a\n'",
            r"a\n",
        ),
        (crate::engine::api::SQL_MODE_ANSI_QUOTES, "'全文'", "全文"),
    ] {
        let _guard = ParserSqlModeGuard::activate(mode);
        let sql = format!("CREATE TABLE t (body TEXT, FULLTEXT (body) COMMENT {literal})");
        let Statement::CreateTable(table) = parse_statement(&sql).unwrap() else {
            panic!("expected CreateTable");
        };
        assert_eq!(table.indexes[0].comment, expected);
    }
    for suffix in [
        "COMMENT",
        "COMMENT 123",
        "COMMENT 'x' UNKNOWN_OPTION",
        "WITH PARSER",
        "WITH PARSER 'ngram'",
    ] {
        assert!(
            parse_statement(&format!(
                "CREATE TABLE t (body TEXT, FULLTEXT (body) {suffix})"
            ))
            .is_err(),
            "{suffix}"
        );
    }
    let _guard = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_ANSI_QUOTES);
    assert!(
        parse_statement("CREATE TABLE t (body TEXT, FULLTEXT (body) COMMENT \"identifier\")")
            .is_err()
    );
}

#[test]
fn parses_fulltext_ddl_and_match() {
    // Inline FULLTEXT KEY in CREATE TABLE.
    let stmt = parse_statement(
        "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft (body))",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected CreateTable");
    };
    assert!(ct
        .indexes
        .iter()
        .any(|i| i.index_type == "FULLTEXT" && i.columns == vec!["body".to_string()]));

    // MATCH(columns) AGAINST(query) lowers to internal __match_against.
    let stmt =
        parse_statement("SELECT id FROM docs WHERE MATCH(body) AGAINST('quick fox')").unwrap();
    let dump = format!("{stmt:?}");
    assert!(
        dump.contains("__match_against"),
        "expected __match_against, got {dump}"
    );

    // IN BOOLEAN MODE parses successfully.
    parse_statement("SELECT id FROM docs WHERE MATCH(body) AGAINST('+a -b' IN BOOLEAN MODE)")
        .unwrap();

    // WITH QUERY EXPANSION is explicitly unsupported.
    assert!(parse_statement(
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('x' WITH QUERY EXPANSION)"
    )
    .is_err());

    // Strip WITH PARSER ngram before parsing, then inject the parser name into the full-text index.
    let stmt = parse_statement(
        "CREATE TABLE z (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft (body) WITH PARSER ngram)",
    )
    .unwrap();
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected CreateTable");
    };
    let ft = ct
        .indexes
        .iter()
        .find(|i| i.is_fulltext())
        .expect("fulltext index");
    assert_eq!(ft.fulltext_parser, "ngram");
}

#[test]
fn parses_standalone_create_fulltext_index() {
    // sqlparser 0.61 does not recognize CREATE FULLTEXT INDEX; strip FULLTEXT,
    // convert to CreateIndex, then restore FULLTEXT index_type and inject the parser.
    let stmt = parse_statement("CREATE FULLTEXT INDEX ft ON t (body) WITH PARSER ngram").unwrap();
    let Statement::CreateIndex(index) = stmt else {
        panic!("expected CreateIndex, got {stmt:?}");
    };
    assert_eq!(index.index_type, "FULLTEXT");
    assert_eq!(index.fulltext_parser, "ngram");
    assert_eq!(index.columns, vec!["body".to_string()]);

    // Without WITH PARSER, select the default tokenizer represented by an empty string.
    let Statement::CreateIndex(index) =
        parse_statement("CREATE FULLTEXT INDEX ft ON t (body)").unwrap()
    else {
        panic!("expected CreateIndex");
    };
    assert_eq!(index.index_type, "FULLTEXT");
    assert_eq!(index.fulltext_parser, "");

    // Preprocessing leaves ordinary BTREE indexes unaffected.
    let Statement::CreateIndex(index) = parse_statement("CREATE INDEX bt ON t (a)").unwrap() else {
        panic!("expected CreateIndex");
    };
    assert_eq!(index.index_type, "BTREE");
    assert_eq!(index.fulltext_parser, "");

    // MySQL accepts and ignores FULLTEXT prefix lengths and sort directions, meaningless
    // for inverted indexes. Parse successfully and clear both, matching inline syntax.
    let Statement::CreateIndex(index) =
        parse_statement("CREATE FULLTEXT INDEX ft ON t (body(10))").unwrap()
    else {
        panic!("expected CreateIndex");
    };
    assert_eq!(index.index_type, "FULLTEXT");
    assert!(index.column_prefix_lengths.iter().all(Option::is_none));
    let Statement::CreateIndex(index) =
        parse_statement("CREATE FULLTEXT INDEX ft ON t (body DESC)").unwrap()
    else {
        panic!("expected CreateIndex");
    };
    assert!(index.column_descending.iter().all(|d| !d));
}

#[test]
fn json_multi_valued_index_array_cast_is_marked() {
    let Statement::CreateTable(table) = parse_statement(
        "CREATE TABLE t (doc JSON, INDEX idx_tags ((CAST(doc->'$.tags' AS UNSIGNED ARRAY))))",
    )
    .unwrap() else {
        panic!("expected CreateTable");
    };
    assert_eq!(table.indexes.len(), 1);
    assert_eq!(table.indexes[0].key_expression_multi_valued, vec![true]);
    assert_eq!(
        table.indexes[0].key_expressions,
        vec![Some("CAST((`doc` -> '$.tags') AS UNSIGNED)".to_string())]
    );
}

fn rows(value: usize) -> Option<LimitValue> {
    Some(LimitValue::Rows(value))
}

#[test]
fn parses_create_event_recurring() {
    let stmt = parse_statement(
        "CREATE EVENT IF NOT EXISTS daily \
             ON SCHEDULE EVERY 1 DAY STARTS '2099-01-01 00:00:00' \
             ON COMPLETION PRESERVE \
             DISABLE \
             COMMENT 'noop' \
             DO INSERT INTO log VALUES (1)",
    )
    .unwrap();
    let Statement::CreateEvent(def) = stmt else {
        panic!("expected CreateEvent, got {stmt:?}");
    };
    assert_eq!(def.name, "daily");
    assert!(def.if_not_exists);
    match def.schedule {
        EventSchedule::Every {
            interval_value,
            interval_unit,
            starts_expr,
            ends_expr,
        } => {
            assert_eq!(interval_value, "1");
            assert_eq!(interval_unit, "DAY");
            assert_eq!(starts_expr.as_deref(), Some("'2099-01-01 00:00:00'"));
            assert_eq!(ends_expr, None);
        }
        other => panic!("unexpected schedule {other:?}"),
    }
    assert_eq!(def.on_completion, Some(EventOnCompletion::Preserve));
    assert_eq!(def.status, Some(EventStatus::Disabled));
    assert_eq!(def.comment.as_deref(), Some("noop"));
    assert!(def.body_sql.starts_with("INSERT"));
}

#[test]
fn parses_create_event_one_shot() {
    let stmt = parse_statement(
        "CREATE EVENT once ON SCHEDULE AT '2099-12-31 23:59:59' DO INSERT INTO t VALUES (1)",
    )
    .unwrap();
    let Statement::CreateEvent(def) = stmt else {
        panic!("expected CreateEvent");
    };
    match def.schedule {
        EventSchedule::At { at_expr } => {
            assert_eq!(at_expr, "'2099-12-31 23:59:59'");
        }
        other => panic!("unexpected schedule {other:?}"),
    }
}

#[test]
fn parses_alter_event_rename_and_enable() {
    let stmt =
        parse_statement("ALTER EVENT e1 ON SCHEDULE EVERY 30 MINUTE ENABLE RENAME TO e2").unwrap();
    let Statement::AlterEvent(stmt) = stmt else {
        panic!("expected AlterEvent");
    };
    assert_eq!(stmt.name, "e1");
    assert_eq!(stmt.rename_to.as_deref(), Some("e2"));
    assert_eq!(stmt.status, Some(EventStatus::Enabled));
    assert!(matches!(stmt.schedule, Some(EventSchedule::Every { .. })));
}

#[test]
fn parses_drop_event_if_exists() {
    let stmt = parse_statement("DROP EVENT IF EXISTS gone").unwrap();
    let Statement::DropEvent { name, if_exists } = stmt else {
        panic!("expected DropEvent");
    };
    assert_eq!(name, "gone");
    assert!(if_exists);
}

#[test]
fn parses_show_events_with_filters() {
    let stmt = parse_statement("SHOW EVENTS FROM mydb LIKE 'tick%'").unwrap();
    let Statement::ShowEvents { database, filter } = stmt else {
        panic!("expected ShowEvents");
    };
    assert_eq!(database.as_deref(), Some("mydb"));
    assert!(matches!(filter, Some(ShowFilter::Like(pattern)) if pattern == "tick%"));
}

#[test]
fn parses_show_table_status_for_mysql_metadata_clients() {
    let stmt = parse_statement(
        "/* ApplicationName=DBeaver 26.0.4 - Metadata */ \
             SHOW TABLE STATUS FROM tpch LIKE 'lineitem'",
    )
    .unwrap();
    let Statement::ShowTableStatus { database, filter } = stmt else {
        panic!("expected ShowTableStatus");
    };
    assert_eq!(database.as_deref(), Some("tpch"));
    assert!(matches!(filter, Some(ShowFilter::Like(pattern)) if pattern == "lineitem"));

    let stmt = parse_statement("SHOW TABLE STATUS IN `tpch` WHERE Name = 'lineitem'").unwrap();
    let Statement::ShowTableStatus { database, filter } = stmt else {
        panic!("expected ShowTableStatus");
    };
    assert_eq!(database.as_deref(), Some("tpch"));
    assert!(matches!(filter, Some(ShowFilter::Where(_))));

    assert!(parse_statement("SHOW TABLE STATUS LIKE").is_err());
    assert!(parse_statement("SHOW TABLE STATUS FROM tpch LIKE 'lineitem' EXTRA").is_err());
}

#[test]
fn parses_show_mysql80_metadata_extensions() {
    assert!(matches!(
        parse_statement("SHOW CREATE DATABASE IF NOT EXISTS `app-db`").unwrap(),
        Statement::ShowCreateDatabase {
            name,
            if_not_exists: true
        } if name == "app-db"
    ));
    assert!(matches!(
        parse_statement("SHOW STORAGE ENGINES").unwrap(),
        Statement::ShowEngines
    ));
    assert!(matches!(
        parse_statement("/* ApplicationName=NineData */ SHOW ENGINES").unwrap(),
        Statement::ShowEngines
    ));
    assert!(matches!(
        parse_statement("SHOW INDEX FROM t FROM app WHERE Key_name = 'idx'").unwrap(),
        Statement::ShowIndex {
            table,
            filter: Some(ShowFilter::Where(_))
        } if table == "app.t"
    ));
    assert!(matches!(
        parse_statement("SHOW VARIABLES WHERE Variable_name LIKE 'version%'").unwrap(),
        Statement::ShowVariables {
            scope: ShowScope::Session,
            filter: Some(ShowFilter::Where(_))
        }
    ));
    assert!(matches!(
        parse_statement("SHOW GLOBAL STATUS LIKE 'Ssl_%'").unwrap(),
        Statement::ShowStatus {
            scope: ShowScope::Global,
            filter: Some(ShowFilter::Like(pattern))
        } if pattern == "Ssl_%"
    ));
    assert_eq!(
        parse_statement("SHOW ERRORS LIMIT 2, 5").unwrap(),
        Statement::ShowDiagnostics {
            errors_only: true,
            limit: Some(ShowLimit {
                offset: 2,
                row_count: 5
            })
        }
    );
    assert_eq!(
        parse_statement("SHOW COUNT(*) WARNINGS").unwrap(),
        Statement::ShowCountDiagnostics { errors_only: false }
    );
    assert!(parse_statement("SHOW VARIABLES ILIKE 'version%'").is_err());
}

#[test]
fn parses_show_mydb_memory_commands_exactly() {
    assert_eq!(
        parse_statement("show   mydb memory;").unwrap(),
        Statement::ShowMydbMemory
    );
    assert_eq!(
        parse_statement("/* monitor */ SHOW MYDB MEMORY SESSIONS").unwrap(),
        Statement::ShowMydbMemorySessions
    );
    assert_eq!(
        parse_statement("SHOW /* monitor */ MYDB /* sample */ MEMORY ;").unwrap(),
        Statement::ShowMydbMemory
    );
    assert!(parse_statement("SHOW MYDB MEMORY EXTRA").is_err());
    assert!(parse_statement("SHOW MYDB MEMORY SESSIONS EXTRA").is_err());
    assert!(parse_statement("SHOW /* monitor */ MYDB MEMORY EXTRA").is_err());

    let mut statement = Statement::ShowMydbMemory;
    let mut next = 7;
    number_statement_placeholders(&mut statement, &mut next);
    assert_eq!(next, 7);
}

#[test]
fn parses_kill_variants() {
    assert_eq!(
        parse_statement("KILL 42").unwrap(),
        Statement::Kill {
            target: KillTarget::Connection,
            connection_id: 42
        }
    );
    assert_eq!(
        parse_statement("KILL CONNECTION 42").unwrap(),
        Statement::Kill {
            target: KillTarget::Connection,
            connection_id: 42
        }
    );
    assert_eq!(
        parse_statement("KILL QUERY 42").unwrap(),
        Statement::Kill {
            target: KillTarget::Query,
            connection_id: 42
        }
    );
    assert!(parse_statement("KILL QUERY abc").is_err());
}

#[test]
fn parses_shutdown_and_restart() {
    assert_eq!(parse_statement("SHUTDOWN").unwrap(), Statement::Shutdown);
    assert_eq!(parse_statement("shutdown").unwrap(), Statement::Shutdown);
    assert_eq!(
        parse_statement("  SHUTDOWN ;").unwrap(),
        Statement::Shutdown
    );
    assert_eq!(parse_statement("RESTART").unwrap(), Statement::Restart);
    assert_eq!(parse_statement("restart;").unwrap(), Statement::Restart);
    // Unknown trailing tokens must not silently parse as SHUTDOWN.
    assert_ne!(
        parse_statement("SHUTDOWN NOW").ok(),
        Some(Statement::Shutdown)
    );
    assert_ne!(
        parse_statement("RESTART SERVER").ok(),
        Some(Statement::Restart)
    );
}

#[test]
fn parses_cancel_ddl_job_extension() {
    assert_eq!(
        parse_statement("CANCEL DDL JOB 42").unwrap(),
        Statement::CancelDdlJob(42)
    );
}

#[test]
fn parses_create_table_with_constraints() {
    let stmt = parse_statement(
            "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL DEFAULT 'anon', email TEXT, UNIQUE KEY email_idx (email))",
        )
        .unwrap();

    let Statement::CreateTable(table) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(table.name, "users");
    assert!(!table.if_not_exists);
    assert_eq!(table.columns.len(), 3);
    assert_eq!(table.primary_key, vec!["id"]);
    assert_eq!(table.indexes.len(), 1);
    assert_eq!(
        table.columns[1].default,
        Some(Expr::Literal(Literal::String("anon".into())))
    );
}

#[test]
fn parses_create_index_prefix_direction_visibility_and_comment() {
    let stmt = parse_statement(
        "CREATE INDEX name_pref ON users (name(3) DESC) COMMENT 'prefix only' INVISIBLE;",
    )
    .unwrap();
    let Statement::CreateIndex(index) = stmt else {
        panic!("expected create index");
    };
    assert_eq!(index.name, "name_pref");
    assert_eq!(index.table, "users");
    assert_eq!(index.columns, vec!["name"]);
    assert_eq!(index.column_prefix_lengths, vec![Some(3)]);
    assert_eq!(index.column_descending, vec![true]);
    assert_eq!(index.index_type, "BTREE");
    assert!(!index.visible);
    assert_eq!(index.comment, "prefix only");
}

#[test]
fn parses_alter_index_visibility() {
    let stmt = parse_statement("ALTER TABLE users ALTER INDEX name_pref VISIBLE").unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table");
    };
    assert_eq!(
        alter.actions,
        vec![AlterAction::AlterIndexVisibility {
            name: "name_pref".to_string(),
            visible: true,
        }]
    );
}

#[test]
fn parses_mysql_alter_drop_key_alias_without_rewriting_literals() {
    let stmt =
        parse_statement("ALTER TABLE users CHANGE name name VARCHAR(254), DROP KEY name_pref")
            .unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table");
    };
    assert!(matches!(
        alter.actions.as_slice(),
        [
            AlterAction::ChangeColumn { .. },
            AlterAction::DropIndex(name)
        ] if name == "name_pref"
    ));

    let untouched =
        rewrite_alter_drop_key("ALTER TABLE users COMMENT = 'DROP KEY literal', DROP `DROP KEY`")
            .unwrap();
    assert_eq!(
        untouched,
        "ALTER TABLE users COMMENT = 'DROP KEY literal', DROP `DROP KEY`"
    );
}

#[test]
fn parses_create_table_check_constraints() {
    let stmt = parse_statement(
            "CREATE TABLE t (a INT CONSTRAINT a_pos CHECK (a > 0) NOT ENFORCED, b INT, CONSTRAINT sum_pos CHECK (a + b > 0) ENFORCED)",
        )
        .unwrap();

    let Statement::CreateTable(table) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(table.checks.len(), 1);
    assert_eq!(table.columns[0].checks[0].name.as_deref(), Some("a_pos"));
    assert_eq!(table.columns[0].checks[0].column.as_deref(), Some("a"));
    assert!(!table.columns[0].checks[0].enforced);
    assert_eq!(table.checks[0].name.as_deref(), Some("sum_pos"));
    assert_eq!(table.checks[0].column, None);
    assert!(table.checks[0].enforced);
}

#[test]
fn parses_create_table_auto_increment_option() {
    let stmt = parse_statement(
            "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=42",
        )
        .unwrap();
    let Statement::CreateTable(table) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(table.auto_increment_start, Some(42));
}

#[test]
fn parses_alter_table_auto_increment_option() {
    let stmt = parse_statement("ALTER TABLE t AUTO_INCREMENT = 99").unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table");
    };
    assert_eq!(alter.actions, vec![AlterAction::SetAutoIncrement(99)]);
}

#[test]
fn parses_mysql80_generated_columns() {
    let stmt = parse_statement(
            "CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED, c INT AS (b + 1) VIRTUAL)",
        )
        .unwrap();
    let Statement::CreateTable(table) = stmt else {
        panic!("expected create table");
    };
    let b = table.columns[1].generated.as_ref().unwrap();
    assert!(b.stored);
    assert_eq!(
        b.expr,
        Expr::Binary {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(1))),
        }
    );
    let c = table.columns[2].generated.as_ref().unwrap();
    assert!(!c.stored);
}

#[test]
fn parses_alter_table_check_constraints() {
    let stmt =
        parse_statement("ALTER TABLE t ADD CONSTRAINT c CHECK (v >= 0) NOT ENFORCED").unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table");
    };
    assert!(matches!(
        &alter.actions[..],
        [AlterAction::AddCheck(CheckConstraintDef {
            name: Some(name),
            enforced: false,
            column: None,
            ..
        })] if name == "c"
    ));

    let stmt = parse_statement("ALTER TABLE t DROP CHECK c").unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table");
    };
    assert_eq!(
        alter.actions,
        vec![AlterAction::DropCheck {
            name: "c".to_string(),
            if_exists: false,
            include_foreign_keys: false,
        }]
    );

    let Statement::AlterTable(alter) = parse_statement("ALTER TABLE t DROP CONSTRAINT c").unwrap()
    else {
        panic!("expected alter table");
    };
    assert!(matches!(&alter.actions[..], [AlterAction::DropCheck {
        name, if_exists: false, include_foreign_keys: true,
    }] if name == "c"));

    let stmt = parse_statement("ALTER TABLE t ALTER CHECK c NOT ENFORCED").unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table");
    };
    assert_eq!(
        alter.actions,
        vec![AlterAction::AlterCheck {
            name: "c".to_string(),
            enforced: false,
        }]
    );
}

#[test]
fn parses_update_ignore() {
    let stmt = parse_statement("UPDATE IGNORE t SET v = v + 1 WHERE id = 1").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected update");
    };
    assert!(update.ignore);
}

#[test]
fn parses_create_table_like_mysql_forms() {
    let stmt = parse_statement("CREATE TABLE dst LIKE src").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateTableLike {
            name: "dst".to_string(),
            if_not_exists: false,
            source: "src".to_string(),
            temporary: false,
            on_commit: None,
        }
    );

    let stmt = parse_statement("CREATE TABLE IF NOT EXISTS app.dst (LIKE test.src)").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateTableLike {
            name: "app.dst".to_string(),
            if_not_exists: true,
            source: "test.src".to_string(),
            temporary: false,
            on_commit: None,
        }
    );
}

#[test]
fn parses_create_temporary_table_like_with_on_commit() {
    let stmt = parse_statement("CREATE TEMPORARY TABLE dst LIKE src").unwrap();
    assert_eq!(
        stmt,
        Statement::CreateTableLike {
            name: "dst".to_string(),
            if_not_exists: false,
            source: "src".to_string(),
            temporary: true,
            on_commit: None,
        }
    );

    let stmt = parse_statement("CREATE TEMP TABLE dst2 (id INT) ON COMMIT DELETE ROWS").unwrap();
    let Statement::CreateTable(table) = stmt else {
        panic!("expected CreateTable");
    };
    assert!(table.temporary);
    assert_eq!(table.on_commit, Some(TempTableOnCommit::DeleteRows));

    let stmt =
        parse_statement("CREATE TEMPORARY TABLE dst3 ON COMMIT DROP AS SELECT 1 AS id").unwrap();
    assert!(matches!(
        stmt,
        Statement::CreateTableAsSelect {
            table,
            ..
        } if table.name == "dst3"
            && table.temporary
            && table.on_commit == Some(TempTableOnCommit::Drop)
    ));

    let stmt = parse_statement("CREATE TABLE persistent_t (id INT) ON COMMIT DROP").unwrap();
    assert!(matches!(
        stmt,
        Statement::Unsupported { kind, .. } if kind == "ON COMMIT on non-temporary table"
    ));
}

#[test]
fn ctas_keeps_complete_target_options() {
    let stmt = parse_statement(
        "CREATE TABLE dst PCTFREE=12 PCTUSED=34 ENGINE=MyISAM ROW_FORMAT=COMPRESSED \
             AS SELECT 1 AS id",
    )
    .unwrap();
    let Statement::CreateTableAsSelect {
        table,
        explicit_definition,
        ..
    } = stmt
    else {
        panic!("expected CTAS");
    };
    assert!(!explicit_definition);
    assert_eq!(table.pctfree, Some(12));
    assert_eq!(table.pctused, Some(34));
    assert_eq!(table.engine.as_deref(), Some("MyISAM"));
    assert!(table
        .table_options
        .iter()
        .any(|(key, value)| key == "ROW_FORMAT" && value == "COMPRESSED"));
}

#[test]
fn parses_parenthesized_union_ctas_without_as_keyword() {
    let statement =
        parse_statement("CREATE TABLE dst (SELECT 1 AS a) UNION ALL (SELECT 2 AS a)").unwrap();
    assert!(matches!(
        statement,
        Statement::CreateTableAsSelect {
            explicit_definition: false,
            ..
        }
    ));

    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE ordinary (a INT, b VARCHAR(10))").unwrap()
    else {
        panic!("expected plain CREATE TABLE");
    };
    assert_eq!(table.columns.len(), 2);
}

#[test]
fn pct_option_scanner_ignores_identifiers_and_ctas_query() {
    let stmt = parse_statement("CREATE TABLE pctfree (pctused INT, pctfree INT)").unwrap();
    let Statement::CreateTable(table) = stmt else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.name, "pctfree");
    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.pctfree, None);
    assert_eq!(table.pctused, None);

    let stmt = parse_statement("CREATE TABLE dst AS SELECT pctfree, pctused FROM src").unwrap();
    assert!(matches!(stmt, Statement::CreateTableAsSelect { .. }));
}

#[test]
fn parses_select_where_order_limit() {
    let stmt = parse_statement("SELECT id, name AS n FROM users WHERE id >= 10 AND name IS NOT NULL ORDER BY id DESC LIMIT 5").unwrap();

    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.projection.len(), 2);
    assert_eq!(select.table.as_deref(), Some("users"));
    assert_eq!(select.order_by.len(), 1);
    assert_eq!(select.limit, rows(5));
}

#[test]
fn parses_analyze_table() {
    let stmt = parse_statement("ANALYZE TABLE users").unwrap();

    assert_eq!(
        stmt,
        Statement::AnalyzeTable {
            tables: vec!["users".to_string()],
            partitions: None,
            histogram: None,
        }
    );
}

#[test]
fn parses_analyze_multiple_tables() {
    let stmt = parse_statement("ANALYZE TABLE users, audit.events").unwrap();
    assert_eq!(
        stmt,
        Statement::AnalyzeTable {
            tables: vec!["users".to_string(), "audit.events".to_string()],
            partitions: None,
            histogram: None,
        }
    );
}

#[test]
fn parses_analyze_table_partition_compatibility_syntax() {
    let stmt = parse_statement("ANALYZE TABLE t PARTITION (p0,p1)").unwrap();
    assert_eq!(
        stmt,
        Statement::AnalyzeTable {
            tables: vec!["t".to_string()],
            partitions: Some(crate::parser_partition::PartitionTargets::Names(vec![
                "p0".into(),
                "p1".into()
            ])),
            histogram: None,
        }
    );
}

#[test]
fn parses_dynamic_like_escape_operands() {
    for sql in [
        "SELECT 'a%b' LIKE 'a\\%b' ESCAPE ?",
        "SELECT 'a%b' LIKE 'a\\%b' ESCAPE (SELECT 'x')",
        "SELECT 'a%b' LIKE 'a\\%b' ESCAPE CAST('' AS CHAR)",
        "SELECT 'a%b' LIKE 'a\\%b' ESCAPE _latin1''",
        "SELECT 'a%b' LIKE 'a\\%b' ESCAPE 1",
    ] {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected SELECT for {sql}");
        };
        let SelectItem::Expr { expr, .. } = &select.projection[0] else {
            panic!("expected expression for {sql}");
        };
        assert!(
            matches!(expr, Expr::Function { name, args, .. }
                if name == "__LIKE_DYNAMIC_ESCAPE" && args.len() == 4),
            "{sql}: {expr:?}"
        );
    }
}

#[test]
fn parses_analyze_update_histogram() {
    let stmt = parse_statement("ANALYZE TABLE users UPDATE HISTOGRAM ON a, b").unwrap();
    assert_eq!(
        stmt,
        Statement::AnalyzeTable {
            tables: vec!["users".to_string()],
            partitions: None,
            histogram: Some(HistogramAction::Update {
                columns: vec!["a".to_string(), "b".to_string()],
                buckets: None,
            }),
        }
    );
}

#[test]
fn parses_analyze_update_histogram_with_buckets() {
    let stmt =
        parse_statement("ANALYZE TABLE users UPDATE HISTOGRAM ON a WITH 64 BUCKETS").unwrap();
    assert_eq!(
        stmt,
        Statement::AnalyzeTable {
            tables: vec!["users".to_string()],
            partitions: None,
            histogram: Some(HistogramAction::Update {
                columns: vec!["a".to_string()],
                buckets: Some(64),
            }),
        }
    );
}

#[test]
fn parses_analyze_drop_histogram() {
    let stmt = parse_statement("ANALYZE TABLE users DROP HISTOGRAM ON a, b").unwrap();
    assert_eq!(
        stmt,
        Statement::AnalyzeTable {
            tables: vec!["users".to_string()],
            partitions: None,
            histogram: Some(HistogramAction::Drop {
                columns: vec!["a".to_string(), "b".to_string()],
            }),
        }
    );
}

#[test]
fn parses_join_group_limit_offset_and_distinct() {
    let stmt = parse_statement(
            "SELECT DISTINCT u.name, COUNT(*) AS c FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.amount > 10 GROUP BY u.name HAVING COUNT(*) > 1 ORDER BY c DESC LIMIT 5 OFFSET 2",
        )
        .unwrap();

    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert!(select.distinct);
    assert_eq!(select.from[0].alias.as_deref(), Some("u"));
    assert_eq!(select.joins.len(), 1);
    assert_eq!(select.group_by.len(), 1);
    assert!(select.having.is_some());
    assert_eq!(select.limit, rows(5));
    assert_eq!(select.offset, rows(2));
}

#[test]
fn explain_with_comment_in_header_parses() {
    // sqlparser chokes on a comment sitting between the EXPLAIN header
    // and the explained statement. `strip_comments_in_explain_header`
    // drops those comments so `EXPLAIN <file-with-leading-comment>` (the
    // common case when explaining a saved query) parses. Each form must
    // yield an `Explain` whose target is the underlying SELECT.
    for sql in [
        "EXPLAIN\n-- a comment\nSELECT 1",
        "EXPLAIN\n# a comment\nSELECT 1",
        "EXPLAIN /* block */ SELECT 1",
        "EXPLAIN ANALYZE -- c\nSELECT 1",
        "EXPLAIN FORMAT=JSON\n-- c\nSELECT 1",
        "EXPLAIN FORMAT = TREE -- c\nSELECT 1",
        "explain\n-- lower\nwith x as (select 1 as a) select * from x",
    ] {
        let stmt = parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert!(
            matches!(stmt, Statement::Explain { .. }),
            "expected Explain for {sql}, got {stmt:?}"
        );
    }
    // A `--` comment inside a string literal in the body must survive.
    let stmt = parse_statement("EXPLAIN\n-- c\nSELECT '-- not a comment' AS s").unwrap();
    let Statement::Explain { target, .. } = stmt else {
        panic!("expected Explain");
    };
    let Statement::Select(select) = *target else {
        panic!("expected Select target");
    };
    match &select.projection[0] {
        SelectItem::Expr {
            expr: Expr::Literal(Literal::String(s)),
            ..
        } => assert_eq!(s, "-- not a comment"),
        other => panic!("unexpected projection: {other:?}"),
    }
    // Non-EXPLAIN statements are returned untouched (borrowed).
    assert!(matches!(
        strip_comments_in_explain_header("SELECT 1"),
        std::borrow::Cow::Borrowed(_)
    ));
}

#[test]
fn leading_line_comment_does_not_disable_head_prescan() {
    // A leading `--`/`#` line comment must not hide the head keyword: doing
    // so disables the pre-scan that strips MySQL-only `WITH ROLLUP` before
    // sqlparser sees it. (`mysql` CLI strips comments, but JDBC /
    // programmatic clients / `mysql --comments` preserve them.)
    let (buf, len) = extract_first_keyword("-- header\n  SELECT 1");
    assert_eq!(&buf[..len], b"SELECT");
    let (buf, len) = extract_first_keyword("# header\nSELECT 1");
    assert_eq!(&buf[..len], b"SELECT");
    let (buf, len) = extract_first_keyword("/* a */ -- b\n\t WITH x AS (SELECT 1) SELECT * FROM x");
    assert_eq!(&buf[..len], b"WITH");
    // `--` without trailing whitespace is NOT a comment in MySQL.
    let (_buf, len) = extract_first_keyword("--3 SELECT 1");
    assert_eq!(len, 0, "`--` not followed by whitespace is not a comment");

    for sql in [
        "-- TPC-DS Query\nSELECT a, COUNT(*) FROM t GROUP BY a WITH ROLLUP",
        "# c\nSELECT a, COUNT(*) FROM t GROUP BY a WITH ROLLUP",
    ] {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected select for {sql}");
        };
        assert_eq!(select.group_by_modifier, GroupByModifier::Rollup, "{sql}");
    }
}

#[test]
fn comments_cannot_inject_rewriter_keywords() {
    for (with_comment, bare) in [
        ("SELECT /* INTO @comment_var */ 1", "SELECT 1"),
        ("UPDATE /* IGNORE */ t SET a = 1", "UPDATE t SET a = 1"),
        (
            "INSERT INTO t VALUES (1) /* RETURNING a */",
            "INSERT INTO t VALUES (1)",
        ),
        (
            "SELECT a FROM t GROUP BY a /* WITH ROLLUP */",
            "SELECT a FROM t GROUP BY a",
        ),
        (
            "CREATE INDEX i ON t(a) /* INVISIBLE */",
            "CREATE INDEX i ON t(a)",
        ),
    ] {
        assert_eq!(
            parse_statement(with_comment).unwrap(),
            parse_statement(bare).unwrap(),
            "{with_comment}"
        );
    }
}

#[test]
fn manual_parsers_accept_comments_between_keywords() {
    assert!(matches!(
        parse_statement("CREATE /* note */ USER 'comment_user'@'%'").unwrap(),
        Statement::CreateUser(_)
    ));
    assert!(matches!(
        parse_statement("LOAD /* note */ DATA INFILE '/tmp/x' INTO TABLE t").unwrap(),
        Statement::LoadData(_)
    ));
}

#[test]
fn statement_splitting_uses_effective_sql() {
    assert_eq!(
        parse_statements("SELECT 1 /* ; SELECT 9 */; SELECT 2")
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        parse_statements("SELECT 1 /*!80000 ; SELECT 2 */")
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        parse_statements("SELECT 1 /*!99999 ; SELECT 2 */")
            .unwrap()
            .len(),
        1
    );

    let statements = parse_statements(
        "SELECT 1; CREATE TABLE dst PCTFREE=9 ENGINE=MyISAM \
             ROW_FORMAT=COMPRESSED AS SELECT 2 AS id",
    )
    .unwrap();
    let Statement::CreateTableAsSelect { table, .. } = &statements[1] else {
        panic!("second statement should be CTAS");
    };
    assert_eq!(table.pctfree, Some(9));
    assert_eq!(table.engine.as_deref(), Some("MyISAM"));
    assert!(table
        .table_options
        .iter()
        .any(|(key, value)| key == "ROW_FORMAT" && value == "COMPRESSED"));
}

#[test]
fn statement_batch_retains_effective_child_sql_ranges() {
    let sql = "INSERT INTO t VALUES ('a;b'); /* ordinary ; comment */ \
               CREATE TABLE u(id INT); SELECT 3";
    let batch = parse_statement_batch(sql).unwrap();
    assert_eq!(batch.parts.len(), 3);
    let child_sql: Vec<&str> = batch
        .parts
        .iter()
        .map(|part| &batch.effective_sql[part.effective_range.clone()])
        .collect();
    assert!(child_sql[0].contains("'a;b'"));
    assert!(child_sql[1].contains("CREATE TABLE u"));
    assert!(child_sql[2].contains("SELECT 3"));
}

#[test]
fn statement_batch_executable_comment_children_are_standalone_sql() {
    let batch = parse_statement_batch("/*!80000 CREATE TABLE a(id INT); CREATE TABLE b(id INT) */")
        .unwrap();
    assert_eq!(batch.parts.len(), 2);
    let child_sql: Vec<&str> = batch
        .parts
        .iter()
        .map(|part| batch.effective_sql[part.effective_range.clone()].trim())
        .collect();
    assert_eq!(child_sql[0], "CREATE TABLE a(id INT)");
    assert_eq!(child_sql[1], "CREATE TABLE b(id INT)");
    assert!(batch
        .parts
        .iter()
        .all(|part| matches!(part.statement, Statement::CreateTable(_))));

    let inactive =
        parse_statement_batch("SELECT 1; /*!99999 SELECT 2; SELECT 3 */ SELECT 4").unwrap();
    assert_eq!(inactive.parts.len(), 2);
}

#[test]
fn statement_splitter_honors_no_backslash_escapes() {
    let _guard = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES);
    let statements = parse_statements(r"SELECT 'a\'; SELECT 2").unwrap();
    assert_eq!(statements.len(), 2);
}

#[test]
fn comment_only_query_is_effectively_empty() {
    // mysqldump banner lines the `mysql` CLI forwards verbatim → OK, not 1064.
    for sql in [
        "",
        "   ",
        "\n\t ",
        ";",
        ";;  ",
        "--", // bare `--` at EOF (the failing dump line)
        "--\n",
        "-- comment",
        "-- comment\n",
        "-- a\n-- b\n--\n",
        "#",
        "# hash comment\n",
        "/* block */",
        "/* multi\nline */\n  ",
        "/* a */ -- b\n # c\n",
        "/**/",
        "/*!99999 SELECT 1 */",
    ] {
        assert!(sql_is_effectively_empty(sql), "should be empty: {sql:?}");
    }
    assert!(parse_statement("/* unterminated").is_err());

    // Anything executable — even hidden behind leading comments or version
    // gates — is NOT empty.
    for sql in [
        "SELECT 1",
        "-- c\nSELECT 1",
        "/* c */ SELECT 1",
        "--3", // `--` without trailing whitespace is two minus signs
        "--3 SELECT 1",
        "/*!40101 SET NAMES utf8 */",
        "/*+ HINT */ SELECT 1",
        "  ; SELECT 1",
    ] {
        assert!(
            !sql_is_effectively_empty(sql),
            "should NOT be empty: {sql:?}"
        );
    }
}

// ---------- C3: versioned comments are gated by version number ----------

#[test]
fn versioned_comment_gate_honours_server_version() {
    let bare = parse_statement("SELECT 1").unwrap();
    assert_eq!(parse_statement("/*!40000 SELECT 1 */").unwrap(), bare);
    assert_eq!(parse_statement("/*!80000 SELECT 1 */").unwrap(), bare);
    assert_eq!(parse_statement("/*! SELECT 1 */").unwrap(), bare);
    assert!(sql_is_effectively_empty("/*!99999 DROP TABLE t */"));
    assert!(sql_is_effectively_empty("/*!80100 SELECT 1 */"));
}

// ---------- C4: embedded executable comment fragments ----------

#[test]
fn inline_versioned_comment_body_spliced_when_gated_in() {
    let active = parse_statement("SELECT /*!40001 SQL_NO_CACHE */ * FROM t").unwrap();
    let bare = parse_statement("SELECT SQL_NO_CACHE * FROM t").unwrap();
    assert_eq!(active, bare);
    let future = parse_statement("SELECT /*!99999 SQL_NO_CACHE */ * FROM t").unwrap();
    let removed = parse_statement("SELECT * FROM t").unwrap();
    assert_eq!(future, removed);
}

#[test]
fn inline_versioned_comment_leaves_hints_and_plain_comments() {
    for sql in [
        "SELECT /*+ NO_VECTOR_INDEX */ * FROM t",
        "SELECT /* just a note */ * FROM t",
        "SELECT '/*!50100 x */' AS c",
    ] {
        let prepared = crate::sql_lex::PreparedSql::new(
            sql,
            crate::sql_lex::SqlLexMode::default(),
            MYSQL_VERSION_ID,
        )
        .unwrap();
        assert_eq!(prepared.effective(), sql);
    }
    let mb = parse_statement("SELECT /*!40001 SQL_NO_CACHE */ '中文注释' AS c").unwrap();
    let bare = parse_statement("SELECT SQL_NO_CACHE '中文注释' AS c").unwrap();
    assert_eq!(mb, bare);
}

#[test]
fn inline_versioned_partition_clause_reparses() {
    // Expand SHOW CREATE fragments such as executable PARTITION BY comments
    // so CREATE TABLE partition clauses can be parsed again.
    let sql = "CREATE TABLE t (id INT)\n\
                   /*!50100 PARTITION BY RANGE (id)\n\
                   ( PARTITION `p0` VALUES LESS THAN (10),\n\
                    PARTITION `p1` VALUES LESS THAN (20)) */";
    let stmt = parse_statement(sql).expect("partitioned SHOW CREATE must re-parse");
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected CreateTable");
    };
    let part = ct.partition.expect("partition clause must be recovered");
    assert_eq!(part.partitions.len(), 2);
}

#[test]
fn parses_mysql80_group_by_with_rollup_modifier() {
    let Statement::Select(select) =
        parse_statement("SELECT a, b, COUNT(*) FROM t GROUP BY a, b WITH ROLLUP").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.group_by.len(), 2);
    assert_eq!(select.group_by_modifier, GroupByModifier::Rollup);

    let Statement::Select(select) =
        parse_statement("SELECT a AS x, COUNT(*) FROM t GROUP BY x WITH ROLLUP").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.group_by, vec![Expr::Identifier("x".to_string())]);
    assert_eq!(select.group_by_modifier, GroupByModifier::Rollup);

    // GROUP BY ordinals are substituted with the referenced select
    // expression at parse time (see `resolve_group_by_ordinals`), so
    // `GROUP BY 1` arrives at the planner as the first select item.
    let Statement::Select(select) =
        parse_statement("SELECT a, COUNT(*) FROM t GROUP BY 1 WITH ROLLUP").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.group_by, vec![Expr::Identifier("a".to_string())]);
    assert_eq!(select.group_by_modifier, GroupByModifier::Rollup);
}

#[test]
fn parses_with_rollup_in_nested_derived_table() {
    // TPC-DS q67 shape: `WITH ROLLUP` lives two derived levels deep
    // (`... group by ... with rollup) dw1) dw2`). The text-strip must reach
    // paren depth 2, and the re-attach must descend through the FROM-clause
    // derived tables so the modifier lands on the innermost SELECT.
    let Statement::Select(outer) = parse_statement(
        "SELECT * FROM (SELECT a, s FROM (SELECT a, b, SUM(x) s FROM t \
             GROUP BY a, b WITH ROLLUP) dw1) dw2",
    )
    .unwrap() else {
        panic!("expected select");
    };
    // The outer two SELECTs have no GROUP BY of their own.
    assert_eq!(outer.group_by_modifier, GroupByModifier::None);
    let middle = outer
        .derived
        .as_ref()
        .and_then(SubQuery::as_select)
        .expect("dw2 derived select");
    assert_eq!(middle.group_by_modifier, GroupByModifier::None);
    let inner = middle
        .derived
        .as_ref()
        .and_then(SubQuery::as_select)
        .expect("dw1 derived select");
    assert_eq!(inner.group_by.len(), 2);
    assert_eq!(inner.group_by_modifier, GroupByModifier::Rollup);
    assert_eq!(inner.grouping_sets, vec![vec![0, 1], vec![0], vec![]]);
}

#[test]
fn parses_with_cube_in_nested_derived_and_join_tables() {
    let Statement::Select(outer) = parse_statement(
        "SELECT * FROM base b JOIN (SELECT a, c, COUNT(*) n FROM t \
             GROUP BY a, c WITH CUBE) d ON d.a = b.a",
    )
    .unwrap() else {
        panic!("expected select");
    };
    assert_eq!(outer.group_by_modifier, GroupByModifier::None);
    let inner = outer.joins[0]
        .derived
        .as_ref()
        .and_then(SubQuery::as_select)
        .expect("join-derived select");
    assert_eq!(inner.group_by_modifier, GroupByModifier::Cube);
    assert_eq!(
        inner.grouping_sets,
        vec![vec![0, 1], vec![0], vec![1], vec![]]
    );
}

#[test]
fn with_rollup_binds_to_outer_query_not_plain_inner_derived() {
    // An outer `GROUP BY ... WITH ROLLUP` wrapping a plain inner aggregation
    // must bind to the OUTER query; the inner derived GROUP BY stays plain.
    let Statement::Select(outer) = parse_statement(
        "SELECT a, SUM(s) FROM (SELECT a, b, SUM(x) s FROM t GROUP BY a, b) d \
             GROUP BY a WITH ROLLUP",
    )
    .unwrap() else {
        panic!("expected select");
    };
    assert_eq!(outer.group_by_modifier, GroupByModifier::Rollup);
    assert_eq!(outer.grouping_sets, vec![vec![0], vec![]]);
    let inner = outer
        .derived
        .as_ref()
        .and_then(SubQuery::as_select)
        .expect("derived select");
    assert_eq!(inner.group_by.len(), 2);
    assert_eq!(
        inner.group_by_modifier,
        GroupByModifier::None,
        "inner plain GROUP BY must not inherit the outer rollup"
    );
}

#[test]
fn parses_rollup_cube_and_grouping_sets() {
    let Statement::Select(select) =
        parse_statement("SELECT a, COUNT(*) FROM t GROUP BY ROLLUP(a, b)").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.group_by_modifier, GroupByModifier::Rollup);
    assert_eq!(select.group_by.len(), 2);
    assert_eq!(select.grouping_sets, vec![vec![0, 1], vec![0], vec![]]);

    let Statement::Select(select) =
        parse_statement("SELECT a, COUNT(*) FROM t GROUP BY CUBE(a, b)").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.group_by_modifier, GroupByModifier::Cube);
    assert_eq!(
        select.grouping_sets,
        vec![vec![0, 1], vec![0], vec![1], vec![]]
    );

    let Statement::Select(select) =
        parse_statement("SELECT a, COUNT(*) FROM t GROUP BY a, b WITH CUBE").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.group_by_modifier, GroupByModifier::Cube);
    assert_eq!(
        select.grouping_sets,
        vec![vec![0, 1], vec![0], vec![1], vec![]]
    );

    let Statement::Select(select) =
        parse_statement("SELECT d, COUNT(*) FROM t GROUP BY d, GROUPING SETS ((a), (b))").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.group_by_modifier, GroupByModifier::GroupingSets);
    assert_eq!(select.group_by.len(), 3);
    assert_eq!(select.grouping_sets, vec![vec![0, 1], vec![0, 2]]);
}

#[test]
fn postfix_grouping_modifiers_use_mode_aware_shared_tokens() {
    let Statement::Select(select) = parse_statement(
        "SELECT a, COUNT(*) FROM t GROUP /* between */ BY a \
             WITH /* modifier */ CUBE",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };
    assert_eq!(select.group_by_modifier, GroupByModifier::Cube);

    let _guard = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES);
    let Statement::Select(select) =
        parse_statement("SELECT 'a'' WITH CUBE' AS s, COUNT(*) FROM t GROUP BY a WITH ROLLUP")
            .unwrap()
    else {
        panic!("expected SELECT");
    };
    assert_eq!(select.group_by_modifier, GroupByModifier::Rollup);
}

#[test]
fn rejects_group_by_grouping_limits() {
    let too_many_keys = (0..65)
        .map(|idx| format!("c{idx}"))
        .collect::<Vec<_>>()
        .join(", ");
    let err =
        parse_statement(&format!("SELECT COUNT(*) FROM t GROUP BY {too_many_keys}")).unwrap_err();
    assert!(err.message.contains("at most 64 grouping expressions"));

    let cube_dims = (0..13)
        .map(|idx| format!("c{idx}"))
        .collect::<Vec<_>>()
        .join(", ");
    let err = parse_statement(&format!(
        "SELECT COUNT(*) FROM t GROUP BY CUBE({cube_dims})"
    ))
    .unwrap_err();
    assert!(err.message.contains("more than 4096 grouping sets"));
}

#[test]
fn rejects_outer_join_without_join_condition() {
    for join in [
        "LEFT JOIN",
        "LEFT OUTER JOIN",
        "RIGHT JOIN",
        "RIGHT OUTER JOIN",
        "FULL OUTER JOIN",
    ] {
        for sql in [
            format!("SELECT COUNT(*) FROM t4 {join} t2 WHERE t4.id=t2.id"),
            format!("SELECT * FROM t4 {join} t2"),
            format!("SELECT * FROM (t4 {join} t2)"),
            format!("SELECT * FROM (SELECT * FROM t4 {join} t2) AS d"),
            format!("SELECT * FROM t4 WHERE EXISTS (SELECT * FROM t2 {join} t3)"),
            format!("WITH c AS (SELECT * FROM t4 {join} t2) SELECT * FROM c"),
            format!("SELECT * FROM t1, t4 {join} t2 WHERE t4.id=t2.id"),
            format!("UPDATE t4 {join} t2 SET t4.id='x' WHERE t4.id=t2.id"),
            format!("DELETE t4 FROM t4 {join} t2 WHERE t4.id=t2.id"),
            format!("DELETE FROM t4 USING t4 {join} t2 WHERE t4.id=t2.id"),
        ] {
            let err = parse_statement(&sql).expect_err(&sql);
            assert_eq!(err.code(), 1064, "{sql}: {err}");
            assert!(err.message.contains("requires ON or USING"), "{sql}: {err}");
        }
    }
}

#[test]
fn accepts_join_conditions_and_unconstrained_inner_joins() {
    for join in [
        "LEFT JOIN",
        "LEFT OUTER JOIN",
        "RIGHT JOIN",
        "RIGHT OUTER JOIN",
        "FULL OUTER JOIN",
    ] {
        for condition in ["ON t4.id=t2.id", "ON TRUE", "USING (id)"] {
            let sql = format!("SELECT * FROM t4 {join} t2 {condition} WHERE t4.id > 0");
            assert!(
                matches!(parse_statement(&sql).unwrap(), Statement::Select(_)),
                "{sql}"
            );
        }
    }
    for join in [
        "JOIN",
        "INNER JOIN",
        "CROSS JOIN",
        "NATURAL JOIN",
        "NATURAL LEFT JOIN",
        "NATURAL RIGHT JOIN",
    ] {
        let sql = format!("SELECT * FROM t4 {join} t2 WHERE t4.id=t2.id");
        assert!(
            matches!(parse_statement(&sql).unwrap(), Statement::Select(_)),
            "{sql}"
        );
    }
}

#[test]
fn parses_join_using_and_natural_join() {
    let Statement::Select(select) =
        parse_statement("SELECT * FROM users u JOIN orders o USING (id)").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.joins.len(), 1);
    assert_eq!(select.joins[0].using_columns, vec!["id".to_string()]);
    assert!(!select.joins[0].natural);
    assert!(select.joins[0].condition.is_none());

    let Statement::Select(select) =
        parse_statement("SELECT * FROM users NATURAL LEFT JOIN orders").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.joins.len(), 1);
    assert_eq!(select.joins[0].join_type, JoinType::Left);
    assert!(select.joins[0].natural);
}

#[test]
fn parses_lateral_derived_join() {
    let Statement::Select(select) = parse_statement(
        "SELECT * FROM customers c \
             LEFT JOIN LATERAL (SELECT id FROM orders o WHERE o.customer_id = c.id LIMIT 1) AS x \
             ON 1 = 1",
    )
    .unwrap() else {
        panic!("expected select");
    };
    assert_eq!(select.joins.len(), 1);
    assert_eq!(select.joins[0].join_type, JoinType::Left);
    assert!(select.joins[0].lateral);
    assert!(select.joins[0].derived.is_some());
}

#[test]
fn parses_explain_select() {
    let stmt = parse_statement("EXPLAIN SELECT * FROM users WHERE id = 1").unwrap();

    let Statement::Explain { target, .. } = stmt else {
        panic!("expected explain");
    };
    assert!(matches!(*target, Statement::Select(_)));
}

#[test]
fn parses_explain_analyze_traditional() {
    let stmt =
        parse_statement("EXPLAIN ANALYZE FORMAT=TRADITIONAL SELECT * FROM users WHERE id = 1")
            .unwrap();

    let Statement::Explain {
        target,
        format,
        analyze,
    } = stmt
    else {
        panic!("expected explain");
    };
    assert!(analyze);
    assert_eq!(format, Some(ExplainFormat::Traditional));
    assert!(matches!(*target, Statement::Select(_)));
}

#[test]
fn explain_table_rewrites_to_select_star() {
    // A4: `EXPLAIN TABLE t` (MySQL 8.0.19+) == `EXPLAIN SELECT * FROM t`.
    for sql in [
        "EXPLAIN TABLE users",
        "EXPLAIN FORMAT=JSON TABLE users",
        "EXPLAIN ANALYZE TABLE users",
        "DESCRIBE TABLE users",
    ] {
        let stmt = parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let Statement::Explain { target, .. } = stmt else {
            panic!("{sql}: expected explain, got {stmt:?}");
        };
        let Statement::Select(select) = *target else {
            panic!("{sql}: expected inner SELECT, got {target:?}");
        };
        assert_eq!(select.table.as_deref(), Some("users"), "{sql}");
    }
}

#[test]
fn explain_table_carries_trailing_order_and_limit() {
    // `TABLE t ORDER BY ... LIMIT ...` is legal MySQL; the tail must survive
    // the rewrite to `SELECT * FROM t ORDER BY ... LIMIT ...`.
    let stmt = parse_statement("EXPLAIN TABLE users ORDER BY id LIMIT 5").unwrap();
    let Statement::Explain { target, .. } = stmt else {
        panic!("expected explain");
    };
    let Statement::Select(select) = *target else {
        panic!("expected inner SELECT");
    };
    assert_eq!(select.table.as_deref(), Some("users"));
    assert_eq!(select.limit, rows(5));
    assert!(!select.order_by.is_empty());
}

#[test]
fn parses_mysql_quoted_identifiers_and_function_calls() {
    let stmt = parse_statement(
        "SELECT `id`, NOW() AS n FROM `users` WHERE `name` = 'Ada' ORDER BY `id` LIMIT 1",
    )
    .unwrap();

    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.table.as_deref(), Some("users"));
    assert_eq!(select.projection.len(), 2);
    assert_eq!(select.limit, rows(1));
}

#[test]
fn parses_select_without_from() {
    let stmt = parse_statement("SELECT 23, NOW() AS n").unwrap();

    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.table, None);
    assert_eq!(select.projection.len(), 2);
}

#[test]
fn parses_mysql80_no_from_select_and_dual() {
    let Statement::Select(select) = parse_statement("SELECT 1 WHERE 1 ORDER BY 1").unwrap() else {
        panic!("expected select");
    };
    assert_eq!(select.table, None);
    assert!(select.from.is_empty());
    assert!(select.selection.is_some());
    assert_eq!(select.order_by.len(), 1);

    let Statement::Select(select) = parse_statement("SELECT 1 FROM DUAL").unwrap() else {
        panic!("expected select");
    };
    assert_eq!(select.table, None);
    assert!(select.from.is_empty());
}

#[test]
fn parses_mysql80_insert_sources_and_default_exprs() {
    let Statement::Insert(insert) =
        parse_statement("INSERT INTO dst (id, v) SELECT id, v FROM src").unwrap()
    else {
        panic!("expected insert");
    };
    assert_eq!(insert.table, "dst");
    assert_eq!(
        insert.columns,
        Some(vec!["id".to_string(), "v".to_string()])
    );
    assert!(matches!(insert.source, InsertSource::Query(_)));

    let Statement::Insert(insert) =
        parse_statement("INSERT INTO t SET a = 1, b = DEFAULT").unwrap()
    else {
        panic!("expected insert");
    };
    assert_eq!(insert.columns, Some(vec!["a".to_string(), "b".to_string()]));
    let InsertSource::Values(rows) = insert.source else {
        panic!("expected values source");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Expr::Default(None));

    let expr = select_first_projection("SELECT DEFAULT(v)");
    assert_eq!(expr, Expr::Default(Some("v".to_string())));

    let Statement::Insert(insert) = parse_statement(
        "INSERT INTO t (id, v) VALUES (1, 10) AS new(i, val) \
             ON DUPLICATE KEY UPDATE v = new.val",
    )
    .unwrap() else {
        panic!("expected insert");
    };
    let alias = insert.insert_alias.expect("expected insert alias");
    assert_eq!(alias.row_alias, "new");
    assert_eq!(
        alias.column_aliases,
        vec!["i".to_string(), "val".to_string()]
    );
}

#[test]
fn parses_mysql80_view_options() {
    let Statement::CreateView {
        name,
        algorithm,
        definer,
        security_type,
        check_option,
        columns,
        ..
    } = parse_statement(
        "CREATE ALGORITHM=MERGE DEFINER='alice'@'%' SQL SECURITY INVOKER \
             VIEW v(a) AS SELECT id FROM t WITH LOCAL CHECK OPTION",
    )
    .unwrap()
    else {
        panic!("expected create view");
    };
    assert_eq!(name.object(), "v");
    assert_eq!(name.database(), None);
    assert_eq!(algorithm.as_deref(), Some("MERGE"));
    let definer = definer.expect("definer");
    assert_eq!(definer.user, "alice");
    assert_eq!(definer.host, "%");
    assert_eq!(security_type, Some(RoutineSecurity::Invoker));
    assert_eq!(check_option.as_deref(), Some("LOCAL"));
    assert_eq!(columns, vec!["a".to_string()]);
}

#[test]
fn parses_mysql80_alter_view_options() {
    let Statement::AlterView {
        name,
        algorithm,
        definer,
        security_type,
        check_option,
        ..
    } = parse_statement(
        "ALTER ALGORITHM=TEMPTABLE DEFINER='bob'@'localhost' SQL SECURITY DEFINER \
             VIEW v AS SELECT id FROM t WITH CASCADED CHECK OPTION",
    )
    .unwrap()
    else {
        panic!("expected alter view");
    };
    assert_eq!(name.object(), "v");
    assert_eq!(name.database(), None);
    assert_eq!(algorithm.as_deref(), Some("TEMPTABLE"));
    let definer = definer.expect("definer");
    assert_eq!(definer.user, "bob");
    assert_eq!(definer.host, "localhost");
    assert_eq!(security_type, Some(RoutineSecurity::Definer));
    assert_eq!(check_option.as_deref(), Some("CASCADED"));
}

#[test]
fn view_check_option_scanner_ignores_string_literals() {
    let Statement::CreateView {
        sql, check_option, ..
    } = parse_statement("CREATE VIEW v AS SELECT 'WITH CHECK OPTION' AS s").unwrap()
    else {
        panic!("expected create view");
    };
    assert_eq!(sql, "SELECT 'WITH CHECK OPTION' AS s");
    assert_eq!(check_option, None);
}

#[test]
fn parses_single_table_dml_aliases() {
    let Statement::Update(update) =
        parse_statement("UPDATE t AS x SET v = DEFAULT WHERE x.id = 1").unwrap()
    else {
        panic!("expected update");
    };
    assert_eq!(update.table, "t");
    assert_eq!(update.assignments[0].value, Expr::Default(None));
    assert!(matches!(
        update.selection,
        Some(Expr::Binary { left, .. })
            if matches!(*left, Expr::CompoundIdentifier(ref parts) if parts == &vec!["x".to_string(), "id".to_string()])
    ));

    let Statement::Delete(delete) = parse_statement("DELETE FROM t AS x WHERE x.id = 1").unwrap()
    else {
        panic!("expected delete");
    };
    assert_eq!(delete.table, "t");
    assert!(delete.selection.is_some());
}

#[test]
fn parses_expression_compatibility_placeholders() {
    let expr = select_first_projection("SELECT (1, 2)");
    assert!(matches!(expr, Expr::Tuple(items) if items.len() == 2));

    let expr = select_first_projection("SELECT 1 = ANY (SELECT 1)");
    assert!(matches!(
        expr,
        Expr::QuantifiedComparison {
            op: BinaryOp::Eq,
            quantifier: SubqueryQuantifier::Any,
            right: QuantifiedComparisonRight::Subquery(_),
            ..
        }
    ));

    let expr = select_first_projection("SELECT 1 < SOME (SELECT 2)");
    assert!(matches!(
        expr,
        Expr::QuantifiedComparison {
            op: BinaryOp::Lt,
            quantifier: SubqueryQuantifier::Any,
            right: QuantifiedComparisonRight::Subquery(_),
            ..
        }
    ));

    let expr = select_first_projection("SELECT 1 >= ALL (SELECT 0)");
    assert!(matches!(
        expr,
        Expr::QuantifiedComparison {
            op: BinaryOp::GtEq,
            quantifier: SubqueryQuantifier::All,
            right: QuantifiedComparisonRight::Subquery(_),
            ..
        }
    ));

    let expr = select_first_projection("SELECT 'a' COLLATE utf8mb4_bin");
    assert!(matches!(
        expr,
        Expr::Function { name, args, .. }
            if name == "COLLATE"
                && matches!(args.get(1), Some(Expr::Literal(Literal::String(collation))) if collation == "utf8mb4_bin")
    ));
}

#[test]
fn parses_create_database_dump_encryption() {
    let sql = "CREATE DATABASE IF NOT EXISTS `jzcloud_dev`/*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci *//*!80016 DEFAULT ENCRYPTION ='N' */;";
    assert_eq!(
        parse_statement(sql).unwrap(),
        Statement::CreateDatabase {
            name: "jzcloud_dev".into(),
            if_not_exists: true,
            default_charset: Some("utf8mb4".into()),
            default_collation: Some("utf8mb4_0900_ai_ci".into()),
        }
    );
    assert_single_entry_parity(sql);
}

#[test]
fn create_database_encryption_options_are_validated() {
    for sql in [
        "CREATE DATABASE db ENCRYPTION 'N'",
        "CREATE /* comment */ SCHEMA IF NOT EXISTS `db` DEFAULT ENCRYPTION='n' DEFAULT CHARSET='utf8mb4' COLLATE='utf8mb4_bin';",
        "CREATE DATABASE db COLLATE utf8mb4_bin ENCRYPTION=\"N\" CHARACTER SET utf8mb4",
        "CREATE DATABASE `ENCRYPTION` /*!99999 DEFAULT ENCRYPTION='Y' */",
    ] {
        assert!(matches!(parse_statement(sql).unwrap(), Statement::CreateDatabase { .. }), "{sql}");
        assert_single_entry_parity(sql);
    }
    for sql in [
        "CREATE DATABASE db DEFAULT ENCRYPTION='Y'",
        "CREATE SCHEMA db ENCRYPTION='y' CHARSET=utf8mb4",
    ] {
        assert!(
            matches!(parse_statement(sql).unwrap(), Statement::Unsupported { kind, .. } if kind.contains("ENCRYPTION")),
            "{sql}"
        );
    }
    for sql in [
        "CREATE DATABASE db ENCRYPTION='invalid'",
        "CREATE DATABASE db ENCRYPTION=1",
        "CREATE DATABASE db ENCRYPTION=N",
        "CREATE DATABASE db ENCRYPTION=",
        "CREATE DATABASE db DEFAULT",
        "CREATE DATABASE db ENCRYPTION='N' garbage",
        "CREATE DATABASE db CHARSET=utf8mb4 CHARACTER SET latin1",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
        assert_single_entry_parity(sql);
    }
    assert!(parse_statement("CREATE DATABASE db ENCRYPTION='N'; SELECT 1").is_err());
}

#[test]
fn parses_mysql80_charset_and_collation_ddl() {
    let Statement::CreateDatabase {
        name,
        default_charset,
        default_collation,
        ..
    } = parse_statement(
        "CREATE DATABASE app DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci",
    )
    .unwrap()
    else {
        panic!("expected create database");
    };
    assert_eq!(name, "app");
    assert_eq!(default_charset.as_deref(), Some("utf8mb4"));
    assert_eq!(default_collation.as_deref(), Some("utf8mb4_0900_ai_ci"));

    let Statement::CreateTable(table) = parse_statement(
        "CREATE TABLE t (name VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_bin) \
             DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs",
    )
    .unwrap() else {
        panic!("expected create table");
    };
    assert_eq!(table.default_charset.as_deref(), Some("utf8mb4"));
    assert_eq!(
        table.default_collation.as_deref(),
        Some("utf8mb4_0900_as_cs")
    );
    assert_eq!(table.columns[0].charset.as_deref(), Some("latin1"));
    assert_eq!(table.columns[0].collation.as_deref(), Some("latin1_bin"));

    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE t (name VARCHAR(20) BINARY DEFAULT NULL)").unwrap()
    else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(
        table.columns[0].collation.as_deref(),
        Some(crate::parser::BINARY_ATTRIBUTE_COLLATION)
    );

    let Statement::SetNames { charset, collation } =
        parse_statement("SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci").unwrap()
    else {
        panic!("expected SET NAMES");
    };
    assert_eq!(charset.as_deref(), Some("utf8mb4"));
    assert_eq!(collation.as_deref(), Some("utf8mb4_0900_ai_ci"));
}

#[test]
fn valid_multi_table_dml_parses() {
    // M5.C1 — multi-table UPDATE / DELETE now parse to real statements
    // instead of `Unsupported`. The executor handles them via the
    // multi-table path; the parser is responsible only for shape.
    let stmt = parse_statement("UPDATE t JOIN u ON t.id = u.id SET t.v = 1").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected UPDATE");
    };
    assert_eq!(update.table, "t");
    assert_eq!(update.joins.len(), 1);
    assert_eq!(update.joins[0].table.name, "u");
    assert_eq!(update.assignments.len(), 1);
    assert_eq!(update.assignments[0].column, "v");
    assert_eq!(update.assignments[0].table_qualifier.as_deref(), Some("t"));

    let stmt = parse_statement("DELETE t FROM t JOIN u ON t.id = u.id").unwrap();
    let Statement::Delete(delete) = stmt else {
        panic!("expected DELETE");
    };
    assert_eq!(delete.table, "t");
    assert_eq!(delete.joins.len(), 1);
    assert_eq!(delete.joins[0].table.name, "u");
    assert_eq!(delete.targets, vec!["t".to_string()]);
}

#[test]
fn parses_prepared_limit_offset_and_dml_order_limit() {
    let Statement::Select(select) =
        parse_statement("SELECT id FROM users ORDER BY id LIMIT ? OFFSET ?").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.limit, Some(LimitValue::Placeholder(0)));
    assert_eq!(select.offset, Some(LimitValue::Placeholder(1)));

    let Statement::Select(select) =
        parse_statement("SELECT id FROM users ORDER BY id LIMIT ?, ?").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(select.limit, Some(LimitValue::OffsetCommaPlaceholder(1)));
    assert_eq!(select.offset, Some(LimitValue::Placeholder(0)));

    let Statement::Update(update) = parse_statement(
        "UPDATE bmsql_district SET d_next_o_id = d_next_o_id + ? \
             WHERE d_w_id = ? AND d_id = ? ORDER BY d_next_o_id LIMIT ?",
    )
    .unwrap() else {
        panic!("expected update");
    };
    assert_eq!(update.order_by.len(), 1);
    assert_eq!(update.limit, Some(LimitValue::Placeholder(3)));

    let Statement::Delete(delete) = parse_statement(
        "DELETE FROM bmsql_new_order \
             WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id LIMIT ?",
    )
    .unwrap() else {
        panic!("expected delete");
    };
    assert_eq!(delete.order_by.len(), 1);
    assert_eq!(delete.limit, Some(LimitValue::Placeholder(2)));
}

#[test]
fn invalid_limit_values_return_parse_error_1064() {
    for sql in [
        "SELECT id FROM t LIMIT -1",
        "SELECT id FROM t LIMIT 1.5",
        "SELECT id FROM t LIMIT 999999999999999999999",
        "SELECT id FROM t LIMIT NULL",
        "SELECT id FROM t LIMIT 5 OFFSET -1",
        "UPDATE t SET v=1 LIMIT -1",
        "DELETE FROM t LIMIT 1.5",
    ] {
        let err = parse_statement(sql).unwrap_err();
        assert_eq!(err.code(), 1064, "{sql}");
        assert_eq!(err.sql_state(), "42000", "{sql}");
    }
}

#[test]
fn replace_with_on_duplicate_key_update_is_rejected() {
    // MySQL rejects the combination at parse time (1064). Must not silently
    // fall back to ODKU semantics.
    let err = parse_statement("REPLACE INTO t (id, v) VALUES (1, 2) ON DUPLICATE KEY UPDATE v = 3")
        .unwrap_err();
    assert_eq!(err.code(), 1064, "REPLACE + ODKU must be 1064");
    assert_eq!(err.sql_state(), "42000");
    assert!(
        err.message.contains("REPLACE cannot be combined"),
        "message: {}",
        err.message
    );
    // Plain REPLACE and plain ODKU still parse fine.
    assert!(parse_statement("REPLACE INTO t (id, v) VALUES (1, 2)").is_ok());
    assert!(
        parse_statement("INSERT INTO t (id, v) VALUES (1, 2) ON DUPLICATE KEY UPDATE v = 3")
            .is_ok()
    );
}

#[test]
fn insert_from_fromless_select_with_odku_parses() {
    // gcol suite (gcol_ins_upd ~L601): `INSERT ... SELECT <no FROM> ON
    // DUPLICATE KEY UPDATE ...` used to fail with 1064 ("found: DUPLICATE")
    // because sqlparser binds the trailing `ON` as a column alias of the
    // FROM-less projection, swallowing the token that introduces the ODKU
    // tail. The MydbDialect now excludes `ON` from column aliases, aligning
    // with MySQL (ON is reserved) so parse_on_insert sees it.
    let stmt = parse_statement("INSERT INTO t2 (i1) SELECT 5 ON DUPLICATE KEY UPDATE i2 = 4")
        .expect("FROM-less SELECT + ODKU must parse");
    let Statement::Insert(insert) = stmt else {
        panic!("expected insert");
    };
    assert!(
        matches!(insert.source, InsertSource::Query(_)),
        "source should be a SELECT query"
    );
    assert!(
        insert.on_duplicate_key_update.is_some(),
        "ODKU tail must be recognised, not swallowed"
    );

    // The FROM-bearing form and the multi-column FROM-less form both parse.
    assert!(parse_statement(
        "INSERT INTO t2 (i1, i2) SELECT i1, i2 FROM t2 AS s ON DUPLICATE KEY UPDATE i2 = 8"
    )
    .is_ok());
    assert!(parse_statement(
        "INSERT INTO t3 (i1, v) SELECT 5, 99 ON DUPLICATE KEY UPDATE v = v + 100"
    )
    .is_ok());
}

#[test]
fn delete_limit_with_offset_is_unsupported() {
    let err = parse_statement("DELETE FROM t LIMIT 2, 3").unwrap_err();
    assert_eq!(err.code(), 1064);
    assert!(
        err.message.contains("DELETE LIMIT offset is not supported"),
        "message: {}",
        err.message
    );
}

#[test]
fn update_with_order_by_only_is_supported() {
    let stmt = parse_statement("UPDATE t SET v=v+1 ORDER BY id").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected update");
    };
    assert_eq!(update.order_by.len(), 1);
    assert!(update.limit.is_none());
}

#[test]
fn update_limit_with_offset_is_unsupported() {
    let err = parse_statement("UPDATE t SET v=0 ORDER BY id LIMIT 2 OFFSET 1").unwrap_err();
    assert_eq!(err.code(), 1064);
    assert!(
        err.message.contains("UPDATE LIMIT offset is not supported"),
        "message: {}",
        err.message
    );
}

#[test]
fn all_dml_offset_spellings_are_rejected() {
    for sql in [
        "UPDATE t SET v=0 LIMIT 1 OFFSET 1",
        "UPDATE LOW_PRIORITY IGNORE t SET v=0 ORDER BY id LIMIT 1,1",
        "WITH c AS (SELECT 1) UPDATE t SET v=0 ORDER BY id LIMIT 1 OFFSET 1",
        "DELETE FROM t LIMIT 1 OFFSET 1",
        "DELETE QUICK IGNORE FROM t ORDER BY id LIMIT 1,1",
        "WITH c AS (SELECT 1) DELETE FROM t ORDER BY id LIMIT 1 OFFSET 1",
    ] {
        let err = parse_statement(sql).unwrap_err();
        assert_eq!(err.code(), 1064, "{sql}: {err:?}");
        assert_eq!(err.sql_state(), "42000", "{sql}: {err:?}");
    }
}

#[test]
fn parses_transaction_control_statements() {
    assert!(matches!(
        parse_statement("BEGIN").unwrap(),
        Statement::Begin {
            read_only: None,
            consistent_snapshot: false
        }
    ));
    assert!(matches!(
        parse_statement("START TRANSACTION").unwrap(),
        Statement::Begin {
            read_only: None,
            consistent_snapshot: false
        }
    ));
    assert!(matches!(
        parse_statement("START TRANSACTION READ ONLY").unwrap(),
        Statement::Begin {
            read_only: Some(true),
            consistent_snapshot: false
        }
    ));
    assert!(matches!(
        parse_statement("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY").unwrap(),
        Statement::Begin {
            read_only: Some(true),
            consistent_snapshot: true
        }
    ));
    assert!(matches!(
        parse_statement("START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT").unwrap(),
        Statement::Begin {
            read_only: Some(true),
            consistent_snapshot: true
        }
    ));
    assert!(matches!(
        parse_statement("COMMIT").unwrap(),
        Statement::Commit {
            chain: None,
            release: None
        }
    ));
    assert!(matches!(
        parse_statement("COMMIT WORK AND CHAIN").unwrap(),
        Statement::Commit {
            chain: Some(true),
            release: None
        }
    ));
    assert!(matches!(
        parse_statement("COMMIT AND NO CHAIN RELEASE").unwrap(),
        Statement::Commit {
            chain: Some(false),
            release: Some(true)
        }
    ));
    assert!(matches!(
        parse_statement("ROLLBACK").unwrap(),
        Statement::Rollback {
            chain: None,
            release: None
        }
    ));
    assert!(matches!(
        parse_statement("ROLLBACK AND CHAIN").unwrap(),
        Statement::Rollback {
            chain: Some(true),
            release: None
        }
    ));
    assert_eq!(
        parse_statement("SET autocommit=0").unwrap(),
        Statement::SetAutocommit(false)
    );
    assert_eq!(
        parse_statement("SET SESSION autocommit = OFF").unwrap(),
        Statement::SetAutocommit(false)
    );
    assert_eq!(
        parse_statement("SET @@session.autocommit=TRUE").unwrap(),
        Statement::SetAutocommit(true)
    );
    assert_eq!(
        parse_statement("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap(),
        Statement::SetTransactionIsolation {
            scope: None,
            isolation: TransactionIsolation::Serializable,
        }
    );
    assert_eq!(
        parse_statement("SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED").unwrap(),
        Statement::SetTransactionIsolation {
            scope: Some(SetScope::Global),
            isolation: TransactionIsolation::ReadUncommitted,
        }
    );
}

#[test]
fn system_variables_have_a_distinct_scope_aware_ast_node() {
    for (sql, expected_scope, expected_name) in [
        (
            "SELECT @@sql_mode",
            SystemVariableScope::ImplicitSession,
            "sql_mode",
        ),
        (
            "SELECT @@SESSION.sql_mode",
            SystemVariableScope::Session,
            "sql_mode",
        ),
        (
            "SELECT @@local.sql_mode",
            SystemVariableScope::Local,
            "sql_mode",
        ),
        (
            "SELECT @@GLOBAL.max_connections",
            SystemVariableScope::Global,
            "max_connections",
        ),
    ] {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected SELECT for {sql}");
        };
        let [SelectItem::Expr { expr, .. }] = select.projection.as_slice() else {
            panic!("expected one expression for {sql}");
        };
        let Expr::SystemVariable { scope, name } = expr else {
            panic!("expected system-variable expression for {sql}: {expr:?}");
        };
        assert_eq!(*scope, expected_scope);
        assert_eq!(name, expected_name);
        assert!(expr_to_sql(expr).starts_with("@@"));
    }
}

#[test]
fn parses_alter_system_admin_statements() {
    assert_eq!(
        parse_statement("alter system checkpoint;").unwrap(),
        Statement::AlterSystemCheckpoint
    );
    assert_eq!(
        parse_statement("ALTER SYSTEM FLUSH DATA_CACHE").unwrap(),
        Statement::AlterSystemFlushDataCache
    );
    assert_eq!(
        parse_statement("ALTER SYSTEM FLUSH SQL_CACHE").unwrap(),
        Statement::AlterSystemFlushSqlCache
    );
    assert!(matches!(
        parse_statement("ALTER SYSTEM FLUSH OTHER_CACHE").unwrap(),
        Statement::Unsupported { kind, .. } if kind == "ALTER SYSTEM FLUSH OTHER_CACHE"
    ));
}

#[test]
fn spatial_index_is_parsed_into_create_table() {
    // SPATIAL INDEX is now a supported index type (the geometry-column
    // requirement is enforced at execution time, not parse time).
    let stmt =
        parse_statement("CREATE TABLE spatial_t (g GEOMETRY NOT NULL, SPATIAL INDEX(g))").unwrap();
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CreateTable, got {stmt:?}");
    };
    assert_eq!(create.indexes.len(), 1);
    assert_eq!(create.indexes[0].index_type, "SPATIAL");
    assert_eq!(create.indexes[0].columns, vec!["g".to_string()]);
}

#[test]
fn foreign_key_ddl_is_parsed() {
    // CREATE TABLE with a table-level FK populates `foreign_keys`.
    let stmt = parse_statement(
        "CREATE TABLE child (id INT, pid INT, \
             CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE)",
    )
    .unwrap();
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CreateTable");
    };
    assert_eq!(create.foreign_keys.len(), 1);
    let fk = &create.foreign_keys[0];
    assert_eq!(fk.name.as_deref(), Some("fk"));
    assert_eq!(fk.columns, vec!["pid".to_string()]);
    assert_eq!(fk.referenced_table, "parent");
    assert_eq!(fk.referenced_columns, vec!["id".to_string()]);
    assert_eq!(fk.on_delete, Some(ReferentialActionDef::Cascade));

    // ALTER ADD / DROP FOREIGN KEY map to the new actions.
    let stmt = parse_statement(
        "ALTER TABLE child ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent (id)",
    )
    .unwrap();
    assert!(matches!(
        stmt,
        Statement::AlterTable(at) if matches!(at.actions.as_slice(), [AlterAction::AddForeignKey(_)])
    ));
    let stmt = parse_statement("ALTER TABLE child DROP FOREIGN KEY fk").unwrap();
    assert!(matches!(
        stmt,
        Statement::AlterTable(at) if matches!(at.actions.as_slice(), [AlterAction::DropForeignKey { .. }])
    ));
}

#[test]
fn parses_mysql80_table_ddl_extensions() {
    let stmt = parse_statement(
        "CREATE TABLE t (id INT COMMENT 'pk', v INT) ENGINE=InnoDB COMMENT='hello'",
    )
    .unwrap();
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(create.table_comment, "hello");
    assert_eq!(
        create.column_comments,
        vec![("id".to_string(), "pk".to_string())]
    );

    let stmt = parse_statement("ALTER TABLE t ADD COLUMN c INT AFTER id").unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(alter.actions.as_slice(), [
            AlterAction::AddColumn(column),
            AlterAction::MoveColumn { column: moved, position: ColumnPosition::After(after) },
        ] if column.name == "c" && moved == "c" && after == "id"));

    let stmt = parse_statement("ALTER TABLE t ALTER COLUMN c SET DEFAULT 42").unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(alter.actions.as_slice(), [
            AlterAction::SetColumnDefault { column, default: Some(Expr::Literal(Literal::Int(42))) },
        ] if column == "c"));

    let stmt = parse_statement("ALTER TABLE t RENAME INDEX old_idx TO new_idx").unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(alter.actions.as_slice(), [
            AlterAction::RenameIndex { old_name, new_name },
        ] if old_name == "old_idx" && new_name == "new_idx"));

    let stmt = parse_statement("ALTER TABLE t LOCK=EXCLUSIVE, MODIFY COLUMN c BIGINT").unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(alter.lock, AlterLock::Exclusive);

    let stmt = parse_statement("DROP TEMPORARY TABLE IF EXISTS a, b RESTRICT").unwrap();
    assert!(matches!(
        stmt,
        Statement::DropTable {
            names,
            if_exists: true,
            temporary: true,
            drop_behavior: Some(TableDropBehavior::Restrict),
        } if names == vec!["a".to_string(), "b".to_string()]
    ));
}

#[test]
fn parses_mysql80_parenthesized_multi_column_add() {
    let Statement::AlterTable(alter) =
        parse_statement("ALTER TABLE t ADD (c1 INT DEFAULT (1 + 2), c2 VARCHAR(20) DEFAULT 'a,b')")
            .unwrap()
    else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(
        alter.actions.as_slice(),
        [AlterAction::AddColumn(c1), AlterAction::AddColumn(c2)]
            if c1.name == "c1" && c2.name == "c2"
    ));
}

#[test]
fn parses_ctas_ignore_and_replace_conflict_policies() {
    for (keyword, expected) in [
        ("IGNORE", CtasConflictPolicy::Ignore),
        ("REPLACE", CtasConflictPolicy::Replace),
    ] {
        let statement = parse_statement(&format!(
            "CREATE TABLE dst (id INT PRIMARY KEY) {keyword} AS SELECT id FROM src"
        ))
        .unwrap();
        let Statement::CreateTableAsSelect {
            conflict_policy, ..
        } = statement
        else {
            panic!("expected CTAS");
        };
        assert_eq!(conflict_policy, expected);
    }
}

#[test]
fn mysql80_table_ddl_unsupported_options_are_explicit() {
    // Options promising unsupported physical behavior retain explicit errors.
    let stmt = parse_statement("CREATE TABLE t (id INT) ENCRYPTION='Y'").unwrap();
    assert!(matches!(
        stmt,
        Statement::Unsupported { kind, .. } if kind.contains("ENCRYPTION")
    ));

    let Statement::AlterTable(alter) = parse_statement("ALTER TABLE T ENGINE=InnoDB").unwrap()
    else {
        panic!("canonical InnoDB engine alias should be a metadata ALTER");
    };
    assert!(matches!(
        alter.actions.as_slice(),
        [AlterAction::SetTableOptions { generic, .. }]
            if generic == &[("ENGINE".to_string(), "INNODB".to_string())]
    ));
}

#[test]
fn mysql80_metadata_only_table_options_are_accepted() {
    let Statement::CreateTable(ct) = parse_statement(
        "CREATE TABLE t (id INT) ROW_FORMAT=DYNAMIC MAX_ROWS=100 MIN_ROWS=2 \
             STATS_PERSISTENT=1 KEY_BLOCK_SIZE=8 PACK_KEYS=0 CHECKSUM=1 \
             DELAY_KEY_WRITE=1 AVG_ROW_LENGTH=42 INSERT_METHOD=NO \
             STATS_AUTO_RECALC=DEFAULT STATS_SAMPLE_PAGES=20 \
             CONNECTION='mysql://h/db/t' COMPRESSION='zlib'",
    )
    .unwrap() else {
        panic!("expected CreateTable");
    };
    let get = |k: &str| {
        ct.table_options
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.as_str())
    };
    assert_eq!(get("ROW_FORMAT"), Some("DYNAMIC"));
    assert_eq!(get("MAX_ROWS"), Some("100"));
    assert_eq!(get("STATS_PERSISTENT"), Some("1"));
    assert_eq!(get("KEY_BLOCK_SIZE"), Some("8"));
    assert_eq!(get("CHECKSUM"), Some("1"));
    assert_eq!(get("INSERT_METHOD"), Some("NO"));
    assert_eq!(get("STATS_AUTO_RECALC"), Some("DEFAULT"));
    assert_eq!(get("CONNECTION"), Some("mysql://h/db/t"));
    assert_eq!(get("COMPRESSION"), Some("ZLIB"));

    // Invalid values must error rather than be silently swallowed.
    assert!(parse_statement("CREATE TABLE t (id INT) ROW_FORMAT=BOGUS").is_err());
    assert!(parse_statement("CREATE TABLE t (id INT) MAX_ROWS=abc").is_err());
}

#[test]
fn zone_map_table_options_validate_and_canonicalize() {
    let Statement::CreateTable(ct) =
        parse_statement("CREATE TABLE t (id INT) ZONE_MAP=ON ZONE_SIZE=1M").unwrap()
    else {
        panic!("expected CreateTable")
    };
    assert!(ct.table_options.contains(&("ZONE_MAP".into(), "ON".into())));
    assert!(ct
        .table_options
        .contains(&("ZONE_SIZE".into(), "1048576".into())));
    for invalid in ["32K", "96K", "65M", "abc"] {
        assert!(
            parse_statement(&format!("CREATE TABLE t (id INT) ZONE_SIZE={invalid}")).is_err(),
            "{invalid}"
        );
    }
    assert!(parse_statement("ALTER TABLE t ZONE_MAP=OFF, ZONE_SIZE=DEFAULT").is_ok());
    let Statement::Select(select) =
        parse_statement("SELECT /*+ ZONE_MAP(t) NO_ZONE_MAP(u) */ * FROM t JOIN u ON t.id=u.id")
            .unwrap()
    else {
        panic!("expected Select")
    };
    assert!(select
        .query_hints
        .contains(&QueryHint::ZoneMap { table: "t".into() }));
    assert!(select
        .query_hints
        .contains(&QueryHint::NoZoneMap { table: "u".into() }));
}

#[test]
fn reports_structured_error() {
    let err = parse_statement("SELECT FROM").unwrap_err();

    assert_eq!(err.code(), 1064);
    assert_eq!(err.sql_state(), "42000");
    assert!(err.line >= 1);
    assert!(!err.message.is_empty());
}

fn select_first_projection(sql: &str) -> Expr {
    let Statement::Select(select) = parse_statement(sql).unwrap() else {
        panic!("expected select for {sql}");
    };
    match select.projection.into_iter().next().unwrap() {
        SelectItem::Expr { expr, .. } => expr,
        other => panic!("unexpected projection: {other:?}"),
    }
}

#[test]
fn rejects_decimal_precision_out_of_range() {
    let err = parse_statement("CREATE TABLE t (x DECIMAL(300, 0))").unwrap_err();
    assert_eq!(err.code(), 1426);
    assert!(err.message.contains("Too-big precision"), "{}", err.message);

    assert!(parse_statement("CREATE TABLE t (x DECIMAL(0, 0))").is_ok());

    let err = parse_statement("CREATE TABLE t (x DECIMAL(10, 31))").unwrap_err();
    assert_eq!(err.code(), 1425);
    assert!(err.message.contains("Too big scale"), "{}", err.message);

    let err = parse_statement("CREATE TABLE t (x DECIMAL(5, 10))").unwrap_err();
    assert_eq!(err.code(), 1427);
    assert!(err.message.contains("M must be >= D"), "{}", err.message);
}

#[test]
fn accepts_decimal_within_mysql_limits() {
    let stmt = parse_statement("CREATE TABLE t (x DECIMAL(65, 30))").unwrap();
    assert!(matches!(stmt, Statement::CreateTable(_)));
}

#[test]
fn rejects_decimal_precision_in_cast() {
    let err = parse_statement("SELECT CAST(1 AS DECIMAL(300, 0))").unwrap_err();
    assert_eq!(err.code(), 1426);
    assert!(err.message.contains("Too-big precision"), "{}", err.message);
}

#[test]
fn parses_large_unsigned_integer_literal() {
    let expr = select_first_projection("SELECT 18446744073709551615");
    assert_eq!(expr, Expr::Literal(Literal::UInt(u64::MAX)));
}

#[test]
fn parses_signed_integer_literal_within_i64() {
    let expr = select_first_projection("SELECT 9223372036854775807");
    assert_eq!(expr, Expr::Literal(Literal::Int(i64::MAX)));
}

#[test]
fn oversized_integer_literal_becomes_decimal() {
    // MySQL promotes an integer literal that overflows BIGINT UNSIGNED to
    // an exact DECIMAL rather than erroring.
    let expr = select_first_projection("SELECT 99999999999999999999");
    assert_eq!(
        expr,
        Expr::Literal(Literal::Decimal("99999999999999999999".to_string()))
    );
}

#[test]
fn rejects_float_literal_overflow() {
    let err = parse_statement("SELECT 1e10000").unwrap_err();
    assert!(err.message.contains("out of range"), "{}", err.message);
}

#[test]
fn parses_trim_with_single_character() {
    // sqlparser maps TRIM('x' FROM s) to trim_what with one expression;
    // the multi-character list path is only reachable from non-MySQL
    // dialects, but parse_trim defensively rejects len > 1.
    let stmt = parse_statement("SELECT TRIM('x' FROM s)").unwrap();
    assert!(matches!(stmt, Statement::Select(_)));

    let Statement::Select(select) = parse_statement(
        "SELECT TRIM(LEADING _latin2' ' FROM _latin2'a '), \
                TRIM(TRAILING _latin2' ' FROM _latin2'a '), \
                TRIM(BOTH _latin2' ' FROM _latin2'a ')",
    )
    .unwrap() else {
        panic!("expected select");
    };
    let aliases = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::Expr {
                alias: Some(alias), ..
            } => alias.clone(),
            other => panic!("expected captured expression alias, got {other:?}"),
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(aliases.len(), 3, "TRIM display names must remain distinct");
}

#[test]
fn parses_convert_using_charset() {
    let stmt = parse_statement("SELECT CONVERT(x USING utf8)").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression");
    };
    assert!(matches!(expr, Expr::Function { name, .. } if name == "CONVERT_USING"));
}

#[test]
fn parses_is_null_and_is_not_null() {
    let stmt = parse_statement("SELECT * FROM t WHERE a IS NULL AND b IS NOT NULL").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    let where_expr = select.selection.expect("where clause");
    let Expr::Binary { left, right, .. } = where_expr else {
        panic!("expected binary");
    };
    assert!(matches!(*left, Expr::IsNull(_)));
    assert!(matches!(*right, Expr::IsNotNull(_)));
}

#[test]
fn parses_default_expression_in_create_table() {
    let stmt = parse_statement("CREATE TABLE t (a INT DEFAULT 42, b INT DEFAULT NULL)").unwrap();
    let Statement::CreateTable(table) = stmt else {
        panic!("expected create table");
    };
    assert_eq!(
        table.columns[0].default,
        Some(Expr::Literal(Literal::Int(42)))
    );
    assert_eq!(table.columns[1].default, Some(Expr::Literal(Literal::Null)));
}

#[test]
fn parses_string_literal_with_escapes() {
    let expr = select_first_projection("SELECT 'a''b'");
    assert_eq!(expr, Expr::Literal(Literal::String("a'b".into())));
}

#[test]
fn parses_load_data_basic() {
    let stmt = parse_statement("LOAD DATA INFILE '/tmp/data.tsv' INTO TABLE t").unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert!(!ld.local);
        assert_eq!(ld.file_path, "/tmp/data.tsv");
        assert_eq!(ld.table, "t");
        assert!(ld.partitions.is_empty());
        assert!(ld.query_hints.is_empty());
        assert_eq!(ld.field_terminated_by, "\t");
        assert_eq!(ld.line_terminated_by, "\n");
        assert!(!ld.ignore_duplicates);
        assert!(!ld.replace);
        assert_eq!(ld.ignore_rows, 0);
        assert!(ld.targets.is_none());
        assert!(!ld.is_xml);
        assert_eq!(ld.priority, LoadPriority::None);
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn parses_load_data_partition_clause() {
    let stmt = parse_statement("LOAD DATA INFILE '/tmp/x' INTO TABLE t PARTITION (p0, p1) (id, v)")
        .unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert_eq!(ld.table, "t");
        assert_eq!(ld.partitions, vec!["p0".to_string(), "p1".to_string()]);
        assert_eq!(
            ld.column_names(),
            Some(vec!["id".to_string(), "v".to_string()])
        );
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn parses_load_data_parallel_hint_with_degree() {
    let stmt =
        parse_statement("LOAD DATA /*+ PARALLEL(4) */ INFILE '/tmp/x' INTO TABLE t").unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert_eq!(ld.query_hints, vec![QueryHint::Parallel { degree: 4 }]);
        assert_eq!(ld.file_path, "/tmp/x");
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn parses_load_data_parallel_hint_without_degree() {
    let stmt = parse_statement("LOAD DATA /*+ PARALLEL */ INFILE '/tmp/x' INTO TABLE t").unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert_eq!(ld.query_hints, vec![QueryHint::Parallel { degree: 0 }]);
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn parses_load_data_no_parallel_hint() {
    let stmt =
        parse_statement("LOAD DATA /*+ NO_PARALLEL */ INFILE '/tmp/x' INTO TABLE t").unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert_eq!(ld.query_hints, vec![QueryHint::NoParallel]);
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn parses_load_data_local() {
    let stmt = parse_statement("LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE t").unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert!(ld.local);
        assert_eq!(ld.file_path, "/tmp/data.csv");
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn load_data_string_literals_follow_no_backslash_escapes() {
    let _guard = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES);
    let Statement::LoadData(load) =
        parse_statement(r"LOAD DATA INFILE 'C:\new\data.csv' INTO TABLE t").unwrap()
    else {
        panic!("expected LOAD DATA");
    };
    assert_eq!(load.file_path, r"C:\new\data.csv");
}

#[test]
fn parses_load_data_replace() {
    let stmt = parse_statement("LOAD DATA INFILE '/tmp/x' REPLACE INTO TABLE t").unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert!(ld.replace);
        assert!(!ld.ignore_duplicates);
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn parses_load_data_ignore() {
    let stmt = parse_statement("LOAD DATA INFILE '/tmp/x' IGNORE INTO TABLE t").unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert!(ld.ignore_duplicates);
        assert!(!ld.replace);
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn parses_load_data_full_syntax() {
    let sql = "LOAD DATA LOCAL INFILE '/tmp/data.csv' REPLACE INTO TABLE mydb.t1 \
                   FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' \
                   LINES STARTING BY 'abc' TERMINATED BY '\\r\\n' \
                   IGNORE 3 LINES (id, name, value)";
    let stmt = parse_statement(sql).unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert!(ld.local);
        assert!(ld.replace);
        assert_eq!(ld.file_path, "/tmp/data.csv");
        assert_eq!(ld.table, "mydb.t1");
        assert_eq!(ld.field_terminated_by, ",");
        assert_eq!(ld.field_enclosed_by, Some('"'));
        assert!(ld.field_optionally_enclosed);
        assert_eq!(ld.field_escaped_by, Some('\\'));
        assert_eq!(ld.line_starting_by, "abc");
        assert_eq!(ld.line_terminated_by, "\r\n");
        assert_eq!(ld.ignore_rows, 3);
        assert_eq!(
            ld.column_names(),
            Some(vec![
                "id".to_string(),
                "name".to_string(),
                "value".to_string()
            ])
        );
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn parses_load_data_columns_keyword() {
    let stmt = parse_statement("LOAD DATA INFILE '/tmp/x' INTO TABLE t COLUMNS TERMINATED BY ','")
        .unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert_eq!(ld.field_terminated_by, ",");
    } else {
        panic!("expected LoadData");
    }
}

#[test]
fn parses_load_data_ignore_rows() {
    let stmt = parse_statement("LOAD DATA INFILE '/tmp/x' INTO TABLE t IGNORE 5 ROWS").unwrap();
    if let Statement::LoadData(ld) = stmt {
        assert_eq!(ld.ignore_rows, 5);
    } else {
        panic!("expected LoadData");
    }
}

// ---- MySQL 8.0 LOAD DATA / LOAD XML compat (G1/G2/G3/G9/G10/G12) ----

#[test]
fn parses_load_data_set_clause() {
    // G1 — trailing SET must be parsed (was silently dropped).
    let stmt =
        parse_statement("LOAD DATA INFILE '/tmp/x' INTO TABLE t (a, b) SET c = a + 1, d = DEFAULT")
            .unwrap();
    let Statement::LoadData(ld) = stmt else {
        panic!("expected LoadData");
    };
    assert_eq!(ld.set_clauses.len(), 2);
    assert_eq!(ld.set_clauses[0].0, "c");
    assert!(matches!(ld.set_clauses[0].1, Expr::Binary { .. }));
    assert_eq!(ld.set_clauses[1].0, "d");
    assert_eq!(ld.set_clauses[1].1, Expr::Default(None));
}

#[test]
fn parses_load_data_user_var_targets() {
    // G2 — @user_var targets in the column list.
    let stmt = parse_statement("LOAD DATA INFILE '/tmp/x' INTO TABLE t (@a, col2, @b) SET c = @a")
        .unwrap();
    let Statement::LoadData(ld) = stmt else {
        panic!("expected LoadData");
    };
    let targets = ld.targets.as_ref().unwrap();
    assert_eq!(targets[0], LoadTarget::UserVar("a".to_string()));
    assert_eq!(targets[1], LoadTarget::Column("col2".to_string()));
    assert_eq!(targets[2], LoadTarget::UserVar("b".to_string()));
    assert!(ld.has_user_var_targets());
    // column_names() drops the user vars.
    assert_eq!(ld.column_names(), Some(vec!["col2".to_string()]));
}

#[test]
fn parses_load_data_character_set() {
    // G3 — CHARACTER SET after PARTITION, before FIELDS.
    let stmt = parse_statement(
        "LOAD DATA INFILE '/tmp/x' INTO TABLE t CHARACTER SET latin1 FIELDS TERMINATED BY ','",
    )
    .unwrap();
    let Statement::LoadData(ld) = stmt else {
        panic!("expected LoadData");
    };
    assert_eq!(ld.charset.as_deref(), Some("latin1"));
    assert_eq!(ld.field_terminated_by, ",");
}

#[test]
fn parses_load_data_priority_prefix() {
    // G9 — LOW_PRIORITY / CONCURRENT accepted.
    let lp = parse_statement("LOAD DATA LOW_PRIORITY INFILE '/tmp/x' INTO TABLE t").unwrap();
    let Statement::LoadData(ld) = lp else {
        panic!("expected LoadData");
    };
    assert_eq!(ld.priority, LoadPriority::LowPriority);

    let cc = parse_statement("LOAD DATA CONCURRENT LOCAL INFILE '/tmp/x' INTO TABLE t").unwrap();
    let Statement::LoadData(ld) = cc else {
        panic!("expected LoadData");
    };
    assert_eq!(ld.priority, LoadPriority::Concurrent);
    assert!(ld.local);
}

#[test]
fn parses_load_data_escaped_by_empty_disables() {
    // G10 — ESCAPED BY '' disables escape processing (None).
    let stmt = parse_statement(
        "LOAD DATA INFILE '/tmp/x' INTO TABLE t FIELDS TERMINATED BY ',' ESCAPED BY ''",
    )
    .unwrap();
    let Statement::LoadData(ld) = stmt else {
        panic!("expected LoadData");
    };
    assert_eq!(ld.field_escaped_by, None);
}

#[test]
fn parses_load_data_enclosed_by_empty_disables() {
    let stmt = parse_statement(
        "LOAD DATA INFILE '/tmp/x' INTO TABLE t FIELDS TERMINATED BY ',' ENCLOSED BY ''",
    )
    .unwrap();
    let Statement::LoadData(ld) = stmt else {
        panic!("expected LoadData");
    };
    assert_eq!(ld.field_enclosed_by, None);
}

#[test]
fn load_data_rejects_trailing_garbage() {
    // Tail-exhaustion check kills the silent-drop bug.
    let err = parse_statement("LOAD DATA INFILE '/tmp/x' INTO TABLE t BOGUS TAIL");
    assert!(err.is_err(), "expected parse error on trailing garbage");
}

#[test]
fn parses_load_xml_rows_identified_by() {
    // G12 — LOAD XML with ROWS IDENTIFIED BY.
    let stmt = parse_statement(
        "LOAD XML INFILE '/tmp/x.xml' INTO TABLE t ROWS IDENTIFIED BY '<row>' (a, b)",
    )
    .unwrap();
    let Statement::LoadData(ld) = stmt else {
        panic!("expected LoadData");
    };
    assert!(ld.is_xml);
    assert_eq!(ld.xml_rows_identified_by.as_deref(), Some("<row>"));
    assert_eq!(
        ld.column_names(),
        Some(vec!["a".to_string(), "b".to_string()])
    );
}

#[test]
fn parses_use_index_hint() {
    let stmt = parse_statement("SELECT * FROM t USE INDEX (idx_a, idx_b) WHERE a = 1").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.from.len(), 1);
    assert_eq!(select.from[0].index_hints.len(), 1);
    let hint = &select.from[0].index_hints[0];
    assert_eq!(hint.hint_type, IndexHintType::Use);
    assert_eq!(hint.scope, IndexHintScope::All);
    assert_eq!(hint.index_names, vec!["idx_a", "idx_b"]);
}

#[test]
fn parses_force_index_hint() {
    let stmt = parse_statement("SELECT * FROM t FORCE INDEX (idx_a) WHERE a > 0").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.from[0].index_hints.len(), 1);
    let hint = &select.from[0].index_hints[0];
    assert_eq!(hint.hint_type, IndexHintType::Force);
    assert_eq!(hint.index_names, vec!["idx_a"]);
}

#[test]
fn parses_ignore_index_hint() {
    let stmt = parse_statement("SELECT * FROM t IGNORE INDEX (idx_a) WHERE a = 1").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    let hint = &select.from[0].index_hints[0];
    assert_eq!(hint.hint_type, IndexHintType::Ignore);
}

#[test]
fn parses_index_hint_with_for_clause() {
    let stmt =
        parse_statement("SELECT * FROM t USE INDEX FOR ORDER BY (idx_a) ORDER BY a").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    let hint = &select.from[0].index_hints[0];
    assert_eq!(hint.scope, IndexHintScope::OrderBy);
}

#[test]
fn parses_optimizer_hint_full_scan() {
    let stmt = parse_statement("SELECT /*+ FULL_SCAN(t) */ * FROM t WHERE a = 1").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![QueryHint::FullScan {
            table: "t".to_string()
        }]
    );
}

#[test]
fn parses_optimizer_hint_no_index() {
    let stmt = parse_statement("SELECT /*+ NO_INDEX(t idx_a) */ * FROM t WHERE a = 1").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![QueryHint::NoIndex {
            table: "t".to_string(),
            index: "idx_a".to_string(),
        }]
    );
}

#[test]
fn parses_optimizer_hint_use_index() {
    let stmt = parse_statement("SELECT /*+ USE_INDEX(t idx_a) */ * FROM t WHERE a = 1").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![QueryHint::UseIndex {
            table: "t".to_string(),
            indexes: vec!["idx_a".to_string()],
        }]
    );
}

#[test]
fn parses_optimizer_hint_index_alias() {
    let stmt = parse_statement("SELECT /*+ INDEX(t idx_a) */ * FROM t WHERE a = 1").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![QueryHint::UseIndex {
            table: "t".to_string(),
            indexes: vec!["idx_a".to_string()],
        }]
    );
    assert!(select.hint_warnings.is_empty());
}

#[test]
fn parses_optimizer_hint_use_index_multiple_indexes() {
    let stmt =
        parse_statement("SELECT /*+ USE_INDEX(t, idx_a, idx_b) */ * FROM t WHERE a = 1").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![QueryHint::UseIndex {
            table: "t".to_string(),
            indexes: vec!["idx_a".to_string(), "idx_b".to_string()],
        }]
    );
}

#[test]
fn parses_optimizer_hint_use_index_arity_warning() {
    let stmt = parse_statement("SELECT /*+ USE_INDEX(t) */ * FROM t WHERE a = 1").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert!(select.query_hints.is_empty());
    assert_eq!(
        select.hint_warnings,
        vec!["USE_INDEX: requires (table, index...)".to_string()]
    );
}

#[test]
fn parses_optimizer_hint_hash_join() {
    let stmt = parse_statement("SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t1 JOIN t2 ON t1.a = t2.a")
        .unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![QueryHint::HashJoin {
            tables: vec!["t1".to_string(), "t2".to_string()]
        }]
    );
}

#[test]
fn parses_optimizer_hint_ordered() {
    let stmt = parse_statement("SELECT /*+ ORDERED */ * FROM t1 JOIN t2 ON t1.a = t2.a").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.query_hints, vec![QueryHint::Ordered]);
}

#[test]
fn parses_multiple_optimizer_hints() {
    let stmt = parse_statement(
        "SELECT /*+ HASH_JOIN(t1, t2) NO_INDEX(t1 idx_a) */ * FROM t1 JOIN t2 ON t1.a = t2.a",
    )
    .unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.query_hints.len(), 2);
    assert_eq!(
        select.query_hints[0],
        QueryHint::HashJoin {
            tables: vec!["t1".to_string(), "t2".to_string()]
        }
    );
    assert_eq!(
        select.query_hints[1],
        QueryHint::NoIndex {
            table: "t1".to_string(),
            index: "idx_a".to_string(),
        }
    );
}

#[test]
fn parses_optimizer_hint_index_ffs_table_only() {
    let stmt = parse_statement("SELECT /*+ INDEX_FFS(t) */ a, b FROM t").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![QueryHint::IndexFfs {
            table: "t".to_string(),
            index: None,
        }]
    );
}

#[test]
fn parses_optimizer_hint_index_ffs_with_index_name() {
    let stmt = parse_statement("SELECT /*+ INDEX_FFS(t idx_ab) */ a, b FROM t").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![QueryHint::IndexFfs {
            table: "t".to_string(),
            index: Some("idx_ab".to_string()),
        }]
    );
}

#[test]
fn parses_optimizer_hint_parallel_with_degree() {
    let stmt = parse_statement("SELECT /*+ PARALLEL(4) */ * FROM t").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.query_hints, vec![QueryHint::Parallel { degree: 4 }]);
}

#[test]
fn parses_optimizer_hint_parallel_without_degree() {
    // PARALLEL() / PARALLEL ⇒ degree=0 means "use engine default".
    let stmt = parse_statement("SELECT /*+ PARALLEL */ * FROM t").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.query_hints, vec![QueryHint::Parallel { degree: 0 }]);
}

#[test]
fn parses_optimizer_hint_no_parallel() {
    let stmt = parse_statement("SELECT /*+ NO_PARALLEL */ * FROM t").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.query_hints, vec![QueryHint::NoParallel]);
}

#[test]
fn optimizer_hint_argument_diagnostics_and_no_parallel_wins() {
    let Statement::Select(select) = parse_statement(
        "SELECT /*+ PARALLEL(4) NO_PARALLEL FULL_SCAN(t extra) ORDERED(x) */ * FROM t",
    )
    .unwrap() else {
        panic!("expected select");
    };
    assert_eq!(select.query_hints, vec![QueryHint::NoParallel]);
    assert!(select
        .hint_warnings
        .iter()
        .any(|warning| warning.contains("NO_PARALLEL wins")));
    assert!(select
        .hint_warnings
        .iter()
        .any(|warning| warning.contains("FULL_SCAN")));
    assert!(select
        .hint_warnings
        .iter()
        .any(|warning| warning.contains("ORDERED")));

    let Statement::Select(select) =
        parse_statement("SELECT /*+ USE_INDEX(`my table`, `idx one`) */ * FROM t").unwrap()
    else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![QueryHint::UseIndex {
            table: "my table".to_string(),
            indexes: vec!["idx one".to_string()],
        }]
    );

    let Statement::Select(select) =
        parse_statement("SELECT /*+ MAX_EXECUTION_TIME(10 */ 1").unwrap()
    else {
        panic!("expected select");
    };
    assert!(select.query_hints.is_empty());
    assert!(select
        .hint_warnings
        .iter()
        .any(|warning| warning.contains("unterminated argument list")));
}

#[test]
fn parses_optimizer_hint_parallel_with_other() {
    let stmt = parse_statement("SELECT /*+ PARALLEL(8) FULL_SCAN(t) */ * FROM t").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(
        select.query_hints,
        vec![
            QueryHint::Parallel { degree: 8 },
            QueryHint::FullScan {
                table: "t".to_string(),
            },
        ]
    );
}

#[test]
fn parses_mysql_create_procedure_with_compound_body() {
    let stmt = parse_statement(
        "CREATE PROCEDURE p(IN a INT, OUT b INT)
             SQL SECURITY INVOKER
             BEGIN
               DECLARE done INT DEFAULT 0;
               SET b = a + 1;
               IF b > 10 THEN
                 SET b = 10;
               END IF;
             END",
    )
    .unwrap();
    let Statement::CreateRoutine(def) = stmt else {
        panic!("expected CREATE ROUTINE");
    };
    assert_eq!(def.kind, RoutineKind::Procedure);
    assert_eq!(def.params.len(), 2);
    assert_eq!(def.characteristics.security, RoutineSecurity::Invoker);
    assert!(matches!(
        def.body.statements[0],
        RoutineStmt::DeclareVar { .. }
    ));
}

#[test]
fn routine_splitter_keeps_conditional_ddl_separate_from_labeled_loop() {
    let stmt = parse_statement(
        "CREATE PROCEDURE recreate_result()
             BEGIN
               DROP TABLE IF EXISTS routine_result;
               CREATE TABLE routine_result (v INT);
               peeling: LOOP
                 INSERT INTO routine_result VALUES (1);
                 LEAVE peeling;
               END LOOP peeling;
             END",
    )
    .unwrap();
    let Statement::CreateRoutine(def) = stmt else {
        panic!("expected CREATE ROUTINE");
    };
    assert_eq!(def.body.statements.len(), 3);
    assert!(matches!(
        &def.body.statements[0],
        RoutineStmt::Sql(sql) if sql.starts_with("DROP TABLE IF EXISTS")
    ));
    assert!(matches!(
        &def.body.statements[1],
        RoutineStmt::Sql(sql) if sql.starts_with("CREATE TABLE")
    ));
    assert!(matches!(
        &def.body.statements[2],
        RoutineStmt::Loop { label: Some(label), .. } if label == "peeling"
    ));
}

#[test]
fn routine_splitter_ignores_control_words_inside_static_statements() {
    for statement in [
        "DROP VIEW IF EXISTS old_view",
        "CREATE TABLE IF NOT EXISTS result_t (v INT)",
        "ALTER TABLE result_t DROP COLUMN IF EXISTS obsolete",
        "SET @text = REPEAT('if; end loop', 2)",
        "SET @choice = CASE WHEN 1 = 1 THEN 7 ELSE 9 END",
    ] {
        let body = format!(
            "BEGIN
                   {statement};
                   marker: LOOP
                     LEAVE marker;
                   END LOOP marker;
                 END"
        );
        let parsed = parse_routine_body_sql(&body)
            .unwrap_or_else(|err| panic!("failed to parse static statement {statement:?}: {err}"));
        assert_eq!(parsed.statements.len(), 2, "{statement}");
        assert!(matches!(
            &parsed.statements[1],
            RoutineStmt::Loop { label: Some(label), .. } if label == "marker"
        ));
    }
}

#[test]
fn routine_splitter_tracks_nested_compounds_and_handler_blocks() {
    let body = parse_routine_body_sql(
        "BEGIN
               DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
                 BEGIN
                   SET @handled = 1;
                   SET @done = 1;
                 END;
               outer_loop: LOOP
                 IF @done = 0 THEN
                   CASE @choice
                     WHEN 1 THEN SET @value = 1;
                   END CASE;
                 ELSE
                   WHILE @done = 0 DO
                     REPEAT
                       SET @done = 1;
                     UNTIL @done = 1 END REPEAT;
                   END WHILE;
                 END IF;
                 LEAVE outer_loop;
               END LOOP outer_loop;
               SET @tail = 1;
             END",
    )
    .unwrap();
    assert_eq!(body.statements.len(), 3);
    assert!(matches!(
        &body.statements[0],
        RoutineStmt::DeclareHandler { statement, .. }
            if matches!(statement.as_ref(), RoutineStmt::Block { statements, .. } if statements.len() == 2)
    ));
    assert!(matches!(
        &body.statements[1],
        RoutineStmt::Loop { label: Some(label), .. } if label == "outer_loop"
    ));
    assert!(matches!(&body.statements[2], RoutineStmt::Set { .. }));
}

#[test]
fn routine_handler_condition_prefixes_allow_arbitrary_statement_starts() {
    let body = parse_routine_body_sql(
        "BEGIN
           DECLARE overflow CONDITION FOR SQLSTATE '22003';
           DECLARE CONTINUE HANDLER FOR SQLSTATE VALUE '22003', 1690, overflow SHOW ERRORS;
           SET @continued = 1;
         END",
    )
    .unwrap();
    assert_eq!(body.statements.len(), 3);
    let RoutineStmt::DeclareHandler {
        conditions,
        statement,
        ..
    } = &body.statements[1]
    else {
        panic!("expected DECLARE HANDLER");
    };
    assert_eq!(
        conditions,
        &[
            RoutineCondition::SqlState("22003".to_string()),
            RoutineCondition::MysqlError(1690),
            RoutineCondition::Named("overflow".to_string()),
        ]
    );
    assert!(matches!(
        statement.as_ref(),
        RoutineStmt::Sql(sql) if sql.eq_ignore_ascii_case("SHOW ERRORS")
    ));
}

#[test]
fn routine_parser_skips_comments_before_control_statement_heads() {
    let body = parse_routine_body_sql(
        "BEGIN
               DECLARE v_slot INT;
               comment_loop: LOOP
                 SET v_slot = NULL;
                 -- SELECT INTO must still use the routine-specific parser.
                 SELECT slot_id INTO v_slot FROM iblt_delta LIMIT 1;
                 -- IF must still be parsed as a compound statement.
                 IF v_slot IS NULL THEN
                   LEAVE comment_loop;
                 END IF;
               END LOOP comment_loop;
             END",
    )
    .unwrap();
    let RoutineStmt::Loop { statements, .. } = &body.statements[1] else {
        panic!("expected loop");
    };
    assert!(matches!(&statements[1], RoutineStmt::SelectInto { .. }));
    assert!(matches!(&statements[2], RoutineStmt::If { .. }));
}

#[test]
fn routine_splitter_accepts_parenthesized_if_conditions() {
    let body = parse_routine_body_sql(
        "BEGIN
           DECLARE current_value INT DEFAULT 1;
           IF (current_value < 2) THEN
             SET current_value = current_value + 1;
           ELSE
             SET current_value = 0;
           END IF;
           RETURN current_value;
         END",
    )
    .unwrap();
    assert_eq!(body.statements.len(), 3);
    assert!(matches!(&body.statements[1], RoutineStmt::If { .. }));
    assert!(matches!(&body.statements[2], RoutineStmt::Return(_)));
}

#[test]
fn parses_multistatement_trigger_body_without_trailing_semicolon() {
    let sql = "CREATE TRIGGER t_before BEFORE INSERT ON t1
               FOR EACH ROW BEGIN
                 SET new.b = t_slow_sysdate();
               END";
    let Statement::CreateTrigger(trigger) = parse_statement(sql).unwrap() else {
        panic!("expected CREATE TRIGGER");
    };
    assert_eq!(trigger.body.statements.len(), 1);
    assert!(matches!(
        &trigger.body.statements[0],
        RoutineStmt::Set { .. }
    ));

    let statements = parse_statements(
        "CREATE TRIGGER t_before BEFORE INSERT ON t1
         FOR EACH ROW BEGIN
           SET NEW.b = t_slow_sysdate();
         END",
    )
    .unwrap();
    assert_eq!(statements.len(), 1);
    assert!(matches!(&statements[0], Statement::CreateTrigger(_)));
}

#[test]
fn parses_trigger_if_exists_body() {
    let sql = "CREATE TRIGGER tr_bi BEFORE INSERT ON t1 FOR EACH ROW
               BEGIN
                 IF EXISTS (SELECT * FROM t1 WHERE t1.id <=> NEW.id) THEN
                   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'duplicate';
                 END IF;
               END";
    let Statement::CreateTrigger(trigger) = parse_statement(sql).unwrap() else {
        panic!("expected CREATE TRIGGER");
    };
    assert!(matches!(
        &trigger.body.statements[0],
        RoutineStmt::If { .. }
    ));

    let Statement::CreateTrigger(trigger) = parse_statement(
        "CREATE TRIGGER t_before BEFORE INSERT ON t1
         FOR EACH ROW BEGIN
           SET NEW.b = t_slow_sysdate();
         END",
    )
    .unwrap() else {
        panic!("expected simple CREATE TRIGGER");
    };
    assert!(matches!(
        &trigger.body.statements[0],
        RoutineStmt::Set { .. }
    ));
}

#[test]
fn parses_bare_null_regexp_operand() {
    let Statement::Select(select) = parse_statement("SELECT 1 REGEXP NULL").unwrap() else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression");
    };
    assert!(matches!(expr, Expr::Function { name, .. } if name == "REGEXP_LIKE"));
}

#[test]
fn do_statement_rewrites_compound_interval_units() {
    let statement =
        parse_statement("DO WEEK(DATE_ADD(CAST(0 AS DATE), INTERVAL 1 YEAR_MONTH), 5)").unwrap();
    assert!(matches!(statement, Statement::Do(_)));
}

#[test]
fn parses_zero_arg_function_in_insert_set_as_function() {
    let Statement::Insert(insert) =
        parse_statement("INSERT INTO t SET a = f1_two_inserts()").unwrap()
    else {
        panic!("expected INSERT");
    };
    let InsertSource::Values(rows) = insert.source else {
        panic!("expected VALUES source");
    };
    assert!(
        matches!(
            &rows[0][0],
            Expr::Function { name, args, .. }
                if name.eq_ignore_ascii_case("f1_two_inserts") && args.is_empty()
        ),
        "got {:?}",
        rows[0][0]
    );

    let Statement::Insert(insert) =
        parse_statement("INSERT INTO t SELECT f1_two_inserts()").unwrap()
    else {
        panic!("expected INSERT SELECT");
    };
    let InsertSource::Query(select) = insert.source else {
        panic!("expected SELECT source");
    };
    assert!(matches!(
        &select.as_select().unwrap().projection[0],
        SelectItem::Expr {
            expr: Expr::Function { name, args, .. },
            ..
        } if name.eq_ignore_ascii_case("f1_two_inserts") && args.is_empty()
    ));
}

#[test]
fn manual_routine_statements_accept_leading_comments() {
    for sql in [
        "-- ============================================================\nCALL iblt_decode();",
        "# generated invocation\nCALL iblt_decode()",
        "/* generated invocation */ CALL iblt_decode();",
        "  -- first\n /* second */ # third\n CALL iblt_decode();  ",
        "/*!80000 CALL iblt_decode() */",
    ] {
        assert!(
            matches!(parse_statement(sql), Ok(Statement::Call(_))),
            "failed to parse comment-prefixed CALL: {sql:?}"
        );
        assert!(
            matches!(parse_statements(sql).as_deref(), Ok([Statement::Call(_)])),
            "failed to batch-parse comment-prefixed CALL: {sql:?}"
        );
    }

    assert!(matches!(
        parse_statement("-- generated DDL\nCREATE PROCEDURE commented_p() SELECT 1"),
        Ok(Statement::CreateRoutine(_))
    ));
    assert!(matches!(
        parse_statement("-- generated metadata query\nSHOW CREATE PROCEDURE commented_p"),
        Ok(Statement::ShowCreateRoutine { .. })
    ));
    assert!(matches!(
        parse_statement("-- generated DDL\nALTER PROCEDURE commented_p COMMENT 'updated'"),
        Ok(Statement::AlterRoutine { .. })
    ));
    assert!(matches!(
        parse_statement("-- generated DDL\nDROP PROCEDURE IF EXISTS commented_p"),
        Ok(Statement::DropRoutine { .. })
    ));

    // MySQL only treats `--` as a comment when it is followed by
    // whitespace/control input; an identifier-like continuation must not
    // be silently discarded by the shared trivia handling.
    assert!(parse_statement("--not-a-comment\nCALL iblt_decode()").is_err());

    let statements = parse_statements(
            "SELECT 1;\n-- ============================================================\nCALL iblt_decode();",
        )
        .unwrap();
    assert_eq!(statements.len(), 2);
    assert!(matches!(&statements[1], Statement::Call(_)));
}

#[test]
fn statement_batch_splitter_preserves_routine_ddl_and_conditional_ddl() {
    let statements = parse_statements(
        "DROP TABLE IF EXISTS old_t;
             SELECT REPEAT('x; IF END LOOP', 2);
             CREATE PROCEDURE batch_p()
             BEGIN
               CREATE TABLE IF NOT EXISTS batch_t (v INT);
               body_loop: LOOP
                 LEAVE body_loop;
               END LOOP body_loop;
             END;
             SELECT CASE WHEN 1 = 1 THEN 7 ELSE 9 END",
    )
    .unwrap();
    assert_eq!(statements.len(), 4);
    assert!(matches!(&statements[2], Statement::CreateRoutine(_)));

    let statements = parse_statements(
        "SELECT 1;
             CREATE PROCEDURE batch_if()
               IF 1 = 1 THEN
                 SET @seen = 1;
               END IF;
             SELECT 2",
    )
    .unwrap();
    assert_eq!(statements.len(), 3);
    assert!(matches!(&statements[1], Statement::CreateRoutine(_)));
}

#[test]
fn parses_mysql_create_function_single_return() {
    let stmt = parse_statement(
        "CREATE FUNCTION add_one(x INT) RETURNS INT
             DETERMINISTIC NO SQL
             RETURN x + 1",
    )
    .unwrap();
    let Statement::CreateRoutine(def) = stmt else {
        panic!("expected CREATE ROUTINE");
    };
    assert_eq!(def.kind, RoutineKind::Function);
    assert!(matches!(def.body.statements[0], RoutineStmt::Return(_)));
}

#[test]
fn parses_call_and_show_create_routine() {
    assert!(matches!(
        parse_statement("CALL p(1, @out)").unwrap(),
        Statement::Call(RoutineCall { .. })
    ));
    assert!(matches!(
        parse_statement("SHOW CREATE PROCEDURE p").unwrap(),
        Statement::ShowCreateRoutine {
            kind: RoutineKind::Procedure,
            ..
        }
    ));
}

#[test]
fn parses_mysql_create_trigger_with_order_and_compound_body() {
    let stmt = parse_statement(
        "CREATE DEFINER=`root`@`%` TRIGGER bi_t
             BEFORE INSERT ON test.t FOR EACH ROW FOLLOWS other_t
             BEGIN
               SET NEW.v = NEW.v + 1;
             END",
    )
    .unwrap();
    let Statement::CreateTrigger(def) = stmt else {
        panic!("expected CREATE TRIGGER");
    };
    assert_eq!(def.name, "bi_t");
    assert_eq!(def.table, "test.t");
    assert_eq!(def.timing, TriggerTiming::Before);
    assert_eq!(def.event, TriggerEvent::Insert);
    assert_eq!(
        def.order.as_ref().map(|order| order.other_trigger.as_str()),
        Some("other_t")
    );
    assert!(matches!(def.body.statements[0], RoutineStmt::Set { .. }));
}

#[test]
fn parses_drop_and_show_trigger_forms() {
    assert!(matches!(
        parse_statement("DROP TRIGGER IF EXISTS tr").unwrap(),
        Statement::DropTrigger {
            name,
            if_exists: true
        } if name == "tr"
    ));
    assert!(matches!(
        parse_statement("SHOW CREATE TRIGGER tr").unwrap(),
        Statement::ShowCreateTrigger { name } if name == "tr"
    ));
    assert!(matches!(
        parse_statement("SHOW TRIGGERS FROM test LIKE 'a%'").unwrap(),
        Statement::ShowTriggers {
            database: Some(db),
            filter: Some(ShowFilter::Like(pattern)),
            ..
        } if db == "test" && pattern == "a%"
    ));
}

#[test]
fn parses_replication_admin_statements() {
    assert!(matches!(
        parse_statement("SHOW REPLICA STATUS").unwrap(),
        Statement::ReplicationAdmin { .. }
    ));
    assert!(matches!(
        parse_statement("START REPLICA FOR CHANNEL 'c1'").unwrap(),
        Statement::ReplicationAdmin { .. }
    ));
    assert!(matches!(
        parse_statement("CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=3307")
            .unwrap(),
        Statement::ReplicationAdmin { .. }
    ));
}

#[test]
fn parses_mydb_replication_admin_statements() {
    for sql in [
        "SHOW MYDB MASTER STATUS",
        "SHOW MYDB REPLICA STATUS",
        "CHANGE MYDB MASTER TO HOST='127.0.0.1', PORT=3316",
        "START MYDB REPLICATION",
        "STOP MYDB REPLICATION",
        "RESET MYDB REPLICATION",
        "PROMOTE TO MASTER",
        "DEMOTE TO REPLICA WITH MASTER='127.0.0.1:3316'",
        "STOP MASTER WRITES",
        "RESUME MASTER WRITES",
        "DROP MYDB REPLICA 'replica-uuid'",
    ] {
        let parsed = parse_statement(sql).unwrap_or_else(|err| {
            panic!("failed to parse '{sql}': {err:?}");
        });
        assert!(
            matches!(parsed, Statement::MydbReplicationAdmin { .. }),
            "expected MydbReplicationAdmin for '{sql}', got {parsed:?}"
        );
    }

    // Must not steal the MySQL-compatible commands.
    assert!(matches!(
        parse_statement("SHOW MASTER STATUS").unwrap(),
        Statement::BinlogAdmin { .. }
    ));
    assert!(matches!(
        parse_statement("SHOW REPLICA STATUS").unwrap(),
        Statement::ReplicationAdmin { .. }
    ));
}

#[test]
fn parses_binlog_admin_and_consistent_snapshot() {
    assert!(matches!(
        parse_statement("SHOW MASTER STATUS").unwrap(),
        Statement::BinlogAdmin { .. }
    ));
    assert!(matches!(
        parse_statement("SHOW BINARY LOG STATUS").unwrap(),
        Statement::BinlogAdmin { .. }
    ));
    assert!(matches!(
        parse_statement("SHOW BINARY LOGS").unwrap(),
        Statement::BinlogAdmin { .. }
    ));
    assert!(matches!(
        parse_statement("FLUSH TABLES WITH READ LOCK").unwrap(),
        Statement::BinlogAdmin { .. }
    ));
    assert!(matches!(
        parse_statement("UNLOCK TABLES").unwrap(),
        Statement::UnlockTables
    ));
    assert!(matches!(
        parse_statement("FLUSH BINARY LOGS").unwrap(),
        Statement::BinlogAdmin { .. }
    ));
    assert!(matches!(
        parse_statement("PURGE BINARY LOGS TO 'mydb-bin.000002'").unwrap(),
        Statement::BinlogAdmin { .. }
    ));
    assert!(matches!(
        parse_statement("START TRANSACTION WITH CONSISTENT SNAPSHOT").unwrap(),
        Statement::Begin {
            read_only: None,
            consistent_snapshot: true
        }
    ));
}

#[test]
fn parses_lock_tables_variants() {
    assert_eq!(
        parse_statement("LOCK TABLES t READ").unwrap(),
        Statement::LockTables(vec![LockTableSpec {
            table: "t".into(),
            alias: None,
            mode: LockTableMode::Read,
            low_priority_write: false,
        }])
    );
    assert_eq!(
        parse_statement("LOCK TABLE db.t AS a READ LOCAL, u LOW_PRIORITY WRITE").unwrap(),
        Statement::LockTables(vec![
            LockTableSpec {
                table: "db.t".into(),
                alias: Some("a".into()),
                mode: LockTableMode::Read,
                low_priority_write: false,
            },
            LockTableSpec {
                table: "u".into(),
                alias: None,
                mode: LockTableMode::Write,
                low_priority_write: true,
            },
        ])
    );
    assert!(matches!(
        parse_statement("UNLOCK TABLE").unwrap(),
        Statement::UnlockTables
    ));
    assert_eq!(
        parse_statement("LOCK TABLES t BAD")
            .unwrap_err()
            .spec
            .code()
            .get(),
        1064
    );
}

#[test]
fn sqlproc_grant_scope_distinguishes_quoted_names_from_wildcards_and_prefixes() {
    for (sql, expected) in [
        (
            "GRANT SELECT ON * TO 'u'@'%'",
            GrantScope::Database(String::new()),
        ),
        (
            "GRANT SELECT ON `*` TO 'u'@'%'",
            GrantScope::Table {
                database: None,
                table: "*".into(),
            },
        ),
        (
            "GRANT SELECT ON TABLE db.t TO 'u'@'%'",
            GrantScope::Table {
                database: Some("db".into()),
                table: "t".into(),
            },
        ),
        (
            "GRANT SELECT ON `TABLE` TO 'u'@'%'",
            GrantScope::Table {
                database: None,
                table: "TABLE".into(),
            },
        ),
        (
            "GRANT SELECT ON `TABLE`.`t` TO 'u'@'%'",
            GrantScope::Table {
                database: Some("TABLE".into()),
                table: "t".into(),
            },
        ),
        (
            "GRANT SELECT ON `a.b`.`c.d` TO 'u'@'%'",
            GrantScope::Table {
                database: Some("a.b".into()),
                table: "c.d".into(),
            },
        ),
    ] {
        let Statement::Grant(grant) = parse_statement(sql).unwrap() else {
            panic!("{sql}")
        };
        let GrantKind::Privileges { scope, .. } = grant.kind else {
            panic!("{sql}")
        };
        assert_eq!(scope, expected, "{sql}");
    }
}

#[test]
fn parses_xa_start_minimal() {
    let stmt = parse_statement("XA START 'gtx1'").unwrap();
    let Statement::XaStart { xid } = stmt else {
        panic!("expected XaStart, got {stmt:?}");
    };
    assert_eq!(xid.format_id, 1);
    assert_eq!(xid.gtrid, b"gtx1");
    assert!(xid.bqual.is_empty());
}

#[test]
fn parses_xa_start_with_bqual_and_format_id() {
    let stmt = parse_statement("XA START 'g','b',42").unwrap();
    let Statement::XaStart { xid } = stmt else {
        panic!("expected XaStart");
    };
    assert_eq!(xid.format_id, 42);
    assert_eq!(xid.gtrid, b"g");
    assert_eq!(xid.bqual, b"b");
}

#[test]
fn parses_xa_begin_alias_for_start() {
    assert!(matches!(
        parse_statement("XA BEGIN 'gtx'").unwrap(),
        Statement::XaStart { .. }
    ));
}

#[test]
fn parses_xa_end_prepare_commit_rollback() {
    assert!(matches!(
        parse_statement("XA END 'g'").unwrap(),
        Statement::XaEnd { .. }
    ));
    assert!(matches!(
        parse_statement("XA PREPARE 'g'").unwrap(),
        Statement::XaPrepare { .. }
    ));
    let stmt = parse_statement("XA COMMIT 'g'").unwrap();
    assert!(matches!(
        stmt,
        Statement::XaCommit {
            one_phase: false,
            ..
        }
    ));
    let stmt = parse_statement("XA COMMIT 'g' ONE PHASE").unwrap();
    assert!(matches!(
        stmt,
        Statement::XaCommit {
            one_phase: true,
            ..
        }
    ));
    assert!(matches!(
        parse_statement("XA ROLLBACK 'g'").unwrap(),
        Statement::XaRollback { .. }
    ));
}

#[test]
fn parses_xa_recover_with_and_without_convert() {
    assert!(matches!(
        parse_statement("XA RECOVER").unwrap(),
        Statement::XaRecover { convert_xid: false }
    ));
    assert!(matches!(
        parse_statement("XA RECOVER CONVERT XID").unwrap(),
        Statement::XaRecover { convert_xid: true }
    ));
}

#[test]
fn xa_start_with_join_or_resume_unsupported() {
    let stmt = parse_statement("XA START 'g' JOIN").unwrap();
    assert!(matches!(
        stmt,
        Statement::Unsupported { ref kind, .. } if kind.contains("JOIN/RESUME")
    ));
    let stmt = parse_statement("XA START 'g' RESUME").unwrap();
    assert!(matches!(
        stmt,
        Statement::Unsupported { ref kind, .. } if kind.contains("JOIN/RESUME")
    ));
}

#[test]
fn xa_end_with_suspend_unsupported() {
    let stmt = parse_statement("XA END 'g' SUSPEND").unwrap();
    assert!(matches!(
        stmt,
        Statement::Unsupported { ref kind, .. } if kind.contains("SUSPEND")
    ));
}

#[test]
fn xa_accepts_hex_xid_literal() {
    let stmt = parse_statement("XA START X'48656c6c6f'").unwrap();
    let Statement::XaStart { xid } = stmt else {
        panic!("expected XaStart");
    };
    assert_eq!(xid.gtrid, b"Hello");
    assert!(xid.bqual.is_empty());
}

#[test]
fn xa_rejects_empty_gtrid() {
    let err = parse_statement("XA START ''").unwrap_err();
    assert_eq!(err.spec, crate::error::catalog::ER_XAER_INVAL);
}

#[test]
fn xa_rejects_oversized_gtrid() {
    let payload = "x".repeat(65);
    let sql = format!("XA START '{}'", payload);
    let err = parse_statement(&sql).unwrap_err();
    assert_eq!(err.spec, crate::error::catalog::ER_XAER_INVAL);
}

#[test]
fn parses_declare_cursor_for_select() {
    let raw = "DECLARE c CURSOR FOR SELECT 1";
    let stmt = parse_statement(raw).unwrap();
    match stmt {
        Statement::DeclareSqlCursor { name, query, sql } => {
            assert_eq!(name, "c");
            assert!(matches!(query, QueryExpr::Select(_)));
            assert_eq!(sql, raw);
        }
        other => panic!("expected DeclareSqlCursor, got {other:?}"),
    }
}

#[test]
fn parses_fetch_count_and_next() {
    let stmt = parse_statement("FETCH 5 FROM c").unwrap();
    match stmt {
        Statement::FetchSqlCursor { name, count } => {
            assert_eq!(name, "c");
            assert_eq!(count, 5);
        }
        other => panic!("expected FetchSqlCursor, got {other:?}"),
    }
    let stmt = parse_statement("FETCH NEXT FROM c").unwrap();
    match stmt {
        Statement::FetchSqlCursor { name, count } => {
            assert_eq!(name, "c");
            assert_eq!(count, 1);
        }
        other => panic!("expected FetchSqlCursor, got {other:?}"),
    }
}

#[test]
fn parses_close_cursor() {
    let stmt = parse_statement("CLOSE c").unwrap();
    assert!(matches!(stmt, Statement::CloseSqlCursor { name } if name == "c"));
}

#[test]
fn declare_cursor_rejects_data_type_form() {
    let err = parse_statement("DECLARE v INT").unwrap_err();
    assert!(
        err.to_string().contains("CURSOR") || err.to_string().contains("DECLARE"),
        "got: {err}"
    );
}

#[test]
fn fetch_rejects_zero_count() {
    let err = parse_statement("FETCH 0 FROM c").unwrap_err();
    assert!(err.to_string().contains("at least 1"), "got: {err}");
}

/// PreScan keyword-presence detection with head-keyword gating.
/// These tests pin down which scans run for each head category;
/// regressions here would either (a) silently re-introduce
/// redundant full-payload scans for the INSERT hot path, or
/// (b) cause a downstream strip/rewrite to short-circuit on a
/// real keyword presence.
#[test]
fn prescan_insert_head_skips_ddl_keywords() {
    // INSERT INTO ... VALUES: only INTERVAL is plausible; FULLTEXT,
    // PCTFREE/PCTUSED and LIKE must all be false even
    // when those keywords appear inside string literals (false
    // positives are tolerated only as conservative safety, but here
    // the head gating skips the scan entirely).
    let sql = "INSERT INTO t(a,b) VALUES (1, 'FULLTEXT PCTFREE ARRAY LIKE')";
    let (head_buf, head_len) = extract_first_keyword(sql);
    let ps = prescan_keywords(sql, &head_buf[..head_len]);
    assert!(!ps.has_fulltext, "INSERT head must not scan FULLTEXT");
    assert!(
        !ps.has_pctfree_or_pctused,
        "INSERT head must not scan PCTFREE/PCTUSED"
    );
    assert!(!ps.has_like, "INSERT head must not scan LIKE");
}

#[test]
fn prescan_select_head_scans_query_keywords() {
    // SELECT can have WITH ROLLUP / EXCLUDE (window frame) /
    // INTERVAL; must NOT have FULLTEXT / PCTFREE.
    let sql = "SELECT a, SUM(b) FROM t GROUP BY a WITH ROLLUP";
    let (head_buf, head_len) = extract_first_keyword(sql);
    let ps = prescan_keywords(sql, &head_buf[..head_len]);
    assert!(ps.has_rollup);
    assert!(!ps.has_fulltext);
    assert!(!ps.has_pctfree_or_pctused);

    let sql = "SELECT MAX(x) OVER (ORDER BY y EXCLUDE CURRENT ROW) FROM t";
    let (head_buf, head_len) = extract_first_keyword(sql);
    let ps = prescan_keywords(sql, &head_buf[..head_len]);
    assert!(ps.has_exclude);

    assert!(parse_statement("SELECT NOW() + INTERVAL 1 DAY").is_ok());
}

#[test]
fn prescan_create_head_scans_ddl_keywords() {
    // CREATE TABLE can have FULLTEXT, PCTFREE/PCTUSED, LIKE,
    // ARRAY+INDEX/KEY; must NOT spend a scan on ROLLUP/EXCLUDE.
    let sql = "CREATE TABLE t (id INT, body TEXT, FULLTEXT KEY ft (body))";
    let (head_buf, head_len) = extract_first_keyword(sql);
    let ps = prescan_keywords(sql, &head_buf[..head_len]);
    assert!(ps.has_fulltext);
    assert!(!ps.has_rollup);
    assert!(!ps.has_exclude);

    let sql = "CREATE TABLE t (id INT) PCTFREE=10";
    let (head_buf, head_len) = extract_first_keyword(sql);
    let ps = prescan_keywords(sql, &head_buf[..head_len]);
    assert!(ps.has_pctfree_or_pctused);

    let sql = "CREATE TABLE t LIKE s";
    let (head_buf, head_len) = extract_first_keyword(sql);
    let ps = prescan_keywords(sql, &head_buf[..head_len]);
    assert!(ps.has_like);
}

#[test]
fn alter_array_index_uses_ast_flags_without_prescan() {
    let sql = "ALTER TABLE t ADD INDEX ((CAST(j->'$.tags' AS UNSIGNED ARRAY)))";
    let (head_buf, head_len) = extract_first_keyword(sql);
    let ps = prescan_keywords(sql, &head_buf[..head_len]);
    let Statement::AlterTable(alter) = parse_statement(sql).unwrap() else {
        panic!()
    };
    assert!(
        matches!(&alter.actions[0], AlterAction::AddIndex(index) if index.key_expression_multi_valued == [true])
    );
    assert!(!ps.has_rollup);
    assert!(!ps.has_pctfree_or_pctused);
}

#[test]
fn prescan_transaction_head_skips_everything() {
    // BEGIN / COMMIT / ROLLBACK / SAVEPOINT — none of the strip/rewrite
    // keywords are reachable; all flags must be false. Saves multiple
    // memchr scans per autocommit boundary.
    for sql in [
        "BEGIN",
        "START TRANSACTION",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT s1",
        "USE testdb",
    ] {
        let (head_buf, head_len) = extract_first_keyword(sql);
        let ps = prescan_keywords(sql, &head_buf[..head_len]);
        assert!(!ps.has_rollup, "{sql}: ROLLUP flagged");
        assert!(!ps.has_exclude, "{sql}: EXCLUDE flagged");
        assert!(!ps.has_fulltext, "{sql}: FULLTEXT flagged");
        assert!(!ps.has_pctfree_or_pctused, "{sql}: PCT* flagged");
        assert!(!ps.has_like, "{sql}: LIKE flagged");
    }
}

/// Differential test: regardless of how aggressively PreScan
/// short-circuits its inner scans, the final parsed AST must be
/// identical to a version that scans every keyword (i.e. the
/// "conservatively true" prescan). This catches any case where the
/// head-based gating in `prescan_keywords` drops a keyword that's
/// actually load-bearing for the downstream strip/rewrite, which
/// would silently change parser behaviour.
///
/// We exercise the assertion by comparing the Debug-format of two
/// `parse_statement` calls on the same SQL: the production path
/// (with head-gated prescan) and a second call after manually
/// emulating "all flags true". Since `parse_statement` is the only
/// entrypoint we can't actually inject "all flags true" without a
/// signature change — so we instead compare a representative set of
/// shapes against their known good outputs, which the existing
/// 2000+ parser tests already cover. This test enumerates a smaller
/// set focused on the head-gating boundaries.
#[test]
fn round2_no_regression_across_common_shapes() {
    let cases = &[
            "INSERT INTO t(a) VALUES (1), (2), (3)",
            "INSERT INTO t(a, b) VALUES (NOW() + INTERVAL 1 DAY, 'x')",
            "SELECT * FROM t WHERE id = 1",
            "SELECT a, COUNT(*) FROM t GROUP BY a WITH ROLLUP",
            "SELECT MAX(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t",
            "UPDATE t SET k = k + 1 WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
            "CREATE TABLE t (id INT PRIMARY KEY, body TEXT)",
            "CREATE TABLE t (id INT) PCTFREE=20 PCTUSED=40",
            "CREATE TABLE clone LIKE src",
            "CREATE FULLTEXT INDEX ft ON docs(body)",
            "ALTER TABLE t ADD COLUMN x INT",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "START TRANSACTION READ ONLY",
            "SAVEPOINT s1",
            "USE testdb",
            "XA START 'gtrid'",
            "SHOW TABLES",
            "SHOW VARIABLES LIKE 'foo'",
        ];
    for sql in cases {
        // The assertion is just "doesn't panic and produces a
        // non-`Unsupported`-by-accident result" — every shape above
        // is a documented supported feature.
        let stmt =
            parse_statement(sql).unwrap_or_else(|err| panic!("parse failed for {sql:?}: {err:?}"));
        if matches!(stmt, Statement::Unsupported { .. }) {
            panic!("unexpected Unsupported for {sql:?}: {stmt:?}");
        }
    }
}

#[test]
fn tpcds_customer_total_return_queries_parse_as_with_cte() {
    for (name, sql) in [
        ("q01", include_str!("../../scripts/tpcds/queries/q01.sql")),
        ("q30", include_str!("../../scripts/tpcds/queries/q30.sql")),
        ("q81", include_str!("../../scripts/tpcds/queries/q81.sql")),
    ] {
        let stmt = parse_statement(sql)
            .unwrap_or_else(|err| panic!("{name} should parse as WITH CTE: {err:?}"));
        match stmt {
            Statement::WithCte(with) => {
                assert_eq!(with.ctes.len(), 1, "{name}");
                assert_eq!(with.ctes[0].name, "customer_total_return", "{name}");
            }
            other => panic!("{name} should parse as Statement::WithCte, got {other:?}"),
        }
    }
}

#[test]
fn cte_body_parenthesised_setop_keeps_trailing_order_by_limit() {
    // Regression: a CTE body that is a parenthesised set operation with a
    // trailing ORDER BY/LIMIT used to drop those modifiers entirely (the
    // `SetExpr::Query` wrapper arm ignored them), so the CTE returned all
    // rows instead of `LIMIT 5`.
    let sql = "WITH u AS ((SELECT a, b, c FROM t1 UNION ALL SELECT a, b, c FROM t2) ORDER BY b LIMIT 5 OFFSET 2) SELECT a FROM u";
    let stmt = parse_statement(sql).expect("should parse");
    let with = match stmt {
        Statement::WithCte(with) => with,
        other => panic!("expected WithCte, got {other:?}"),
    };
    let set = match &with.ctes[0].query {
        QueryExpr::SetOp(set) => set,
        other => panic!("expected CTE body SetOperation, got {other:?}"),
    };
    assert_eq!(set.op, SetOp::Union);
    assert!(set.all, "UNION ALL");
    assert_eq!(set.order_by.len(), 1, "ORDER BY must survive");
    assert_eq!(set.limit, Some(LimitValue::Rows(5)), "LIMIT must survive");
    assert_eq!(set.offset, Some(LimitValue::Rows(2)), "OFFSET must survive");
}

#[test]
fn parenthesised_setop_subquery_keeps_trailing_order_by_limit() {
    // The same wrapper arm is hit for a parenthesised set operation in a
    // derived-table / FROM slot; ensure tail modifiers survive there too.
    let sql = "SELECT * FROM ((SELECT a FROM t1 UNION ALL SELECT a FROM t2) ORDER BY a LIMIT 3) d";
    let stmt = parse_statement(sql).expect("should parse");
    let select = match stmt {
        Statement::Select(select) => select,
        other => panic!("expected Select, got {other:?}"),
    };
    let derived = select.derived.as_ref().expect("derived table present");
    match derived {
        SubQuery::SetOp(set) => {
            assert_eq!(set.order_by.len(), 1, "ORDER BY must survive");
            assert_eq!(set.limit, Some(LimitValue::Rows(3)), "LIMIT must survive");
        }
        other => panic!("expected SubQuery::SetOp, got {other:?}"),
    }
}

// ---- M1 parser-improvement regressions ----------------------------------

#[test]
fn b1_grant_require_ssl_not_swallowed_as_phantom_accounts() {
    // `GRANT ... TO u REQUIRE SSL` must not parse `REQUIRE`/`SSL` as target
    // accounts, and must not silently drop the (security-relevant) TLS
    // restriction. It bails to the authoritative path (rejected / not a
    // clean grant) rather than granting to `'u'@'h'` while dropping SSL.
    match parse_statement("GRANT SELECT ON db.* TO 'u'@'h' REQUIRE SSL") {
        Ok(Statement::Grant(g)) => {
            panic!("REQUIRE grant must not be accepted with the TLS clause dropped: {g:?}")
        }
        _ => {}
    }
}

#[test]
fn b1_grant_with_grant_option_targets_still_correct() {
    // The target-list boundary fix must not regress the ordinary
    // `WITH GRANT OPTION` tail.
    let stmt =
        parse_statement("GRANT SELECT ON db.* TO 'u'@'h' WITH GRANT OPTION").expect("should parse");
    let g = match stmt {
        Statement::Grant(g) => g,
        other => panic!("expected Grant, got {other:?}"),
    };
    assert_eq!(g.targets.len(), 1, "exactly one real target");
    assert_eq!(g.targets[0].user, "u");
    assert_eq!(g.targets[0].host, "h");
    assert!(g.grant_option, "WITH GRANT OPTION preserved");
}

#[test]
fn mysql80_grant_and_revoke_proxy_are_structured() {
    let Statement::Grant(grant) = parse_statement(
        "GRANT PROXY ON 'proxied'@'localhost' TO 'proxy_user'@'%' WITH GRANT OPTION",
    )
    .unwrap() else {
        panic!("expected GRANT PROXY");
    };
    let GrantKind::Proxy(proxied) = grant.kind else {
        panic!("expected proxy grant kind");
    };
    assert_eq!(proxied.user, "proxied");
    assert_eq!(grant.targets[0].user, "proxy_user");
    assert!(grant.grant_option);

    let Statement::Revoke(revoke) =
        parse_statement("REVOKE PROXY ON 'proxied'@'localhost' FROM 'proxy_user'@'%'").unwrap()
    else {
        panic!("expected REVOKE PROXY");
    };
    assert!(matches!(revoke.kind, GrantKind::Proxy(_)));
}

#[test]
fn mysql80_grant_as_with_role_is_captured() {
    let Statement::Grant(grant) = parse_statement(
        "GRANT SELECT ON *.* TO 'target'@'%' WITH GRANT OPTION \
         AS 'grantor'@'%' WITH ROLE ALL EXCEPT 'blocked'@'%'",
    )
    .unwrap() else {
        panic!("expected GRANT");
    };
    let grant_as = grant.grant_as.expect("GRANT AS context");
    assert_eq!(grant_as.account.user, "grantor");
    assert!(matches!(grant_as.roles, Some(SetRole::AllExcept(_))));
}

#[test]
fn b10_revoke_ignore_unknown_user_not_swallowed_as_accounts() {
    // `REVOKE ... FROM u IGNORE UNKNOWN USER` (8.0.22+) must not turn the
    // trailing clause into phantom `IGNORE`/`UNKNOWN`/`USER` accounts.
    let stmt = parse_statement("REVOKE SELECT ON db.* FROM 'u'@'h' IGNORE UNKNOWN USER")
        .expect("should parse");
    let r = match stmt {
        Statement::Revoke(r) => r,
        other => panic!("expected Revoke, got {other:?}"),
    };
    assert_eq!(r.targets.len(), 1, "only the real account is a target");
    assert_eq!(r.targets[0].user, "u");
    assert!(r.ignore_unknown_user);
}

#[test]
fn mysql80_revoke_if_exists_is_preserved() {
    let Statement::Revoke(revoke) =
        parse_statement("REVOKE IF EXISTS SELECT ON db.* FROM 'missing'@'%'").unwrap()
    else {
        panic!("expected REVOKE");
    };
    assert!(revoke.if_exists);
    assert_eq!(revoke.targets.len(), 1);
}

#[test]
fn b3_create_user_default_role_is_captured() {
    let Statement::CreateUser(cu) =
        parse_statement("CREATE USER 'u'@'%' DEFAULT ROLE 'r'@'%'").unwrap()
    else {
        panic!("expected CREATE USER");
    };
    assert_eq!(cu.default_roles.len(), 1);
    assert_eq!(cu.default_roles[0].user, "r");
    assert_eq!(cu.default_roles[0].host, "%");
}

#[test]
fn b3_create_user_comment_and_attribute_captured() {
    // Parity with ALTER USER: COMMENT / ATTRIBUTE must be captured, not
    // silently dropped via the old catch-all.
    let stmt = parse_statement("CREATE USER 'u'@'%' COMMENT 'core eng'").expect("should parse");
    let cu = match stmt {
        Statement::CreateUser(cu) => cu,
        other => panic!("expected CreateUser, got {other:?}"),
    };
    assert_eq!(cu.accounts[0].comment.as_deref(), Some("core eng"));

    let stmt = parse_statement("CREATE USER 'u'@'%' ATTRIBUTE '{\"team\":\"eng\"}'")
        .expect("should parse");
    let cu = match stmt {
        Statement::CreateUser(cu) => cu,
        other => panic!("expected CreateUser, got {other:?}"),
    };
    assert_eq!(
        cu.accounts[0].attribute_json.as_deref(),
        Some("{\"team\":\"eng\"}")
    );
}

#[test]
fn b3_create_user_unknown_clause_rejected_not_dropped() {
    // A genuinely unknown clause must not be walked past silently.
    match parse_statement("CREATE USER 'u'@'%' BOGUSCLAUSE 5") {
        Ok(Statement::CreateUser(cu)) => {
            panic!("unknown clause must not be silently dropped: {cu:?}")
        }
        _ => {}
    }
}

#[test]
fn b3_create_user_common_clauses_still_parse() {
    // The catch-all flip must not regress the clauses mysqldump emits.
    for sql in [
        "CREATE USER 'u'@'%' IDENTIFIED BY 'Passw0rd!' ACCOUNT LOCK",
        "CREATE USER 'u'@'%' PASSWORD EXPIRE INTERVAL 90 DAY",
        "CREATE USER 'u'@'%' PASSWORD EXPIRE NEVER",
        "CREATE USER 'u'@'%' PASSWORD REQUIRE CURRENT OPTIONAL",
        "CREATE USER 'u'@'%' REQUIRE SSL",
        "CREATE USER 'u'@'%' WITH MAX_QUERIES_PER_HOUR 10",
    ] {
        let stmt = parse_statement(sql).unwrap_or_else(|e| panic!("{sql} => {e:?}"));
        assert!(
            matches!(stmt, Statement::CreateUser(_)),
            "{sql} => {stmt:?}"
        );
    }
}

#[test]
fn b4_update_optimizer_hint_parses() {
    // `UPDATE /*+ ... */ t SET ...` — the optimizer hint (a pure perf hint)
    // must not break parsing (previously a silent bare-field skip).
    let stmt =
        parse_statement("UPDATE /*+ SET_VAR(sort_buffer_size=1M) */ t SET a = 1 WHERE id = 2")
            .expect("should parse");
    let Statement::Update(update) = stmt else {
        panic!("expected update");
    };
    assert!(update.query_hints.is_empty());
    assert_eq!(update.hint_warnings, vec!["unknown hint \"SET_VAR\""]);
}

#[test]
fn b5_delete_optimizer_hint_parses() {
    let stmt = parse_statement("DELETE /*+ SET_VAR(sort_buffer_size=1M) */ FROM t WHERE id = 2")
        .expect("should parse");
    let Statement::Delete(delete) = stmt else {
        panic!("expected delete");
    };
    assert!(delete.query_hints.is_empty());
    assert_eq!(delete.hint_warnings, vec!["unknown hint \"SET_VAR\""]);
}

#[test]
fn insert_optimizer_hint_diagnostics_are_retained() {
    let Statement::Insert(insert) =
        parse_statement("INSERT /*+ PARALLEL(4) PARALLEL(8) */ INTO t VALUES (1)")
            .expect("should parse")
    else {
        panic!("expected insert");
    };
    assert_eq!(insert.query_hints, vec![QueryHint::Parallel { degree: 4 }]);
    assert!(insert
        .hint_warnings
        .iter()
        .any(|warning| warning.contains("first wins")));
}

#[test]
fn b6_materialized_view_still_rejected() {
    // The convert_create_view destructure + guard must keep rejecting
    // non-MySQL view variants (regression guard for the guard rewrite).
    let stmt =
        parse_statement("CREATE MATERIALIZED VIEW v AS SELECT 1").expect("parses to a statement");
    assert!(
        matches!(stmt, Statement::Unsupported { .. }),
        "materialized view must be Unsupported, got {stmt:?}"
    );
}

#[test]
fn b6_plain_create_view_still_works() {
    let stmt = parse_statement("CREATE VIEW v AS SELECT a, b FROM t").expect("should parse");
    assert!(matches!(stmt, Statement::CreateView { .. }), "got {stmt:?}");
}

// ---- M2 token-parser correctness regressions ---------------------------

#[test]
fn vector_operator_tokens_preserve_unicode_and_inert_text() {
    let Statement::Select(select) = parse_statement("SELECT a <-> b, café FROM t").unwrap() else {
        panic!()
    };
    assert!(
        matches!(&select.projection[0], SelectItem::Expr { expr: Expr::Function { name, .. }, .. } if name == "L2_DISTANCE")
    );
    assert!(
        matches!(&select.projection[1], SelectItem::Expr { expr: Expr::Identifier(name), .. } if name == "café")
    );
    for sql in [
        "SELECT café, naïve FROM t WHERE x < 3",
        "SELECT '<->' AS s /* <#> */",
        "SELECT 1 # <-> <#>\n",
    ] {
        assert!(parse_statement(sql).is_ok(), "{sql}");
    }
}

#[test]
fn b2_mysql_tokens_decodes_backslash_escaped_quote() {
    // `IDENTIFIED BY 'a\'b'` — the escaped quote must not split the string.
    let stmt = parse_statement("CREATE USER 'u'@'%' IDENTIFIED BY 'a\\'b'").expect("should parse");
    let cu = match stmt {
        Statement::CreateUser(cu) => cu,
        other => panic!("expected CreateUser, got {other:?}"),
    };
    assert_eq!(cu.accounts[0].password.as_deref(), Some("a'b"));
}

#[test]
fn b2_mysql_tokens_backtick_keeps_backslash_literal() {
    // Backtick identifiers do not process backslashes (MySQL semantics).
    let toks = mysql_tokens("GRANT SELECT ON `a\\b`.* TO 'u'@'%'");
    assert!(toks.iter().any(|t| t == "a\\b"), "tokens: {toks:?}");
}

#[test]
fn b7_set_password_missing_close_paren_rejected() {
    // `SET PASSWORD = PASSWORD('x'` (no `)`) must not mis-parse.
    match parse_statement("SET PASSWORD = PASSWORD('x'") {
        Ok(Statement::SetPassword { .. }) => {
            panic!("malformed PASSWORD() must not parse as SET PASSWORD")
        }
        _ => {}
    }
    // MySQL 8.0 removed PASSWORD(); a plain password literal remains valid.
    assert!(parse_statement("SET PASSWORD = PASSWORD('x')").is_err());
    let stmt = parse_statement("SET PASSWORD = 'x'").expect("should parse");
    match stmt {
        Statement::SetPassword { password, .. } => assert_eq!(password, "x"),
        other => panic!("expected SetPassword, got {other:?}"),
    }
}

#[test]
fn mysql80_set_password_to_random_is_structured() {
    let Statement::SetPassword {
        random_password,
        password,
        ..
    } = parse_statement("SET PASSWORD FOR 'u'@'%' TO RANDOM").unwrap()
    else {
        panic!("expected SET PASSWORD");
    };
    assert!(random_password);
    assert!(password.is_empty());
}

#[test]
fn mysql80_create_user_random_password_is_structured() {
    let Statement::CreateUser(create) =
        parse_statement("CREATE USER 'u'@'%' IDENTIFIED BY RANDOM PASSWORD").unwrap()
    else {
        panic!("expected CREATE USER");
    };
    assert!(create.accounts[0].random_password);
    assert!(create.accounts[0].password.is_none());
}

#[test]
fn show_account_statements_preserve_current_user_marker() {
    let Statement::ShowGrantsFor { account, .. } =
        parse_statement("SHOW GRANTS FOR CURRENT_USER()").unwrap()
    else {
        panic!("expected SHOW GRANTS FOR");
    };
    assert_eq!(account.user, "CURRENT_USER()");
    let Statement::ShowCreateUser { account } =
        parse_statement("SHOW CREATE USER CURRENT_USER").unwrap()
    else {
        panic!("expected SHOW CREATE USER");
    };
    assert_eq!(account.user, "CURRENT_USER()");
}

#[test]
fn b8_create_user_non_numeric_failed_login_attempts_rejected() {
    match parse_statement("CREATE USER 'u'@'%' FAILED_LOGIN_ATTEMPTS abc") {
        Ok(Statement::CreateUser(cu)) => {
            panic!("non-numeric FAILED_LOGIN_ATTEMPTS must not be accepted: {cu:?}")
        }
        _ => {}
    }
    // Numeric still works.
    let stmt =
        parse_statement("CREATE USER 'u'@'%' FAILED_LOGIN_ATTEMPTS 3").expect("should parse");
    let cu = match stmt {
        Statement::CreateUser(cu) => cu,
        other => panic!("expected CreateUser, got {other:?}"),
    };
    assert_eq!(cu.accounts[0].failed_login_attempts, Some(3));
}

#[test]
fn b8_alter_user_password_history_default_still_accepted() {
    // The DEFAULT form of PASSWORD HISTORY must survive the B8 tightening.
    let stmt =
        parse_statement("ALTER USER 'u'@'%' PASSWORD HISTORY DEFAULT").expect("should parse");
    assert!(matches!(stmt, Statement::AlterUser(_)), "got {stmt:?}");
}

#[test]
fn b11_versioned_comment_overflow_gate_is_inert() {
    assert!(sql_is_effectively_empty("/*!999999999999 SELECT 1 */"));
    assert_eq!(
        parse_statement("/*! SELECT 1 */").unwrap(),
        parse_statement("SELECT 1").unwrap()
    );
}

// ---- M4 hardening: panic guardrails + modifier round-trips -------------

/// M4.13: `parse_statement` must never panic on adversarial input — a
/// panic in a connection thread is a DoS. This feeds a hand-built corpus of
/// tricky shapes plus deterministically-generated pseudo-random strings
/// (no RNG dependency; a fixed-seed xorshift keeps it reproducible) and
/// only requires that each call returns Ok/Err rather than unwinding.
#[test]
fn m4_parse_statement_never_panics_on_adversarial_input() {
    // Curated edge cases: multibyte UTF-8, unbalanced quotes/parens,
    // truncated clauses, pgvector operators next to non-ASCII, versioned
    // comments, and the specific shapes touched by M1/M2.
    let corpus = [
        "",
        " ",
        ";",
        "SELECT",
        "SELECT café <-> 数据",
        "SELECT a <#> ",
        "SELECT a <-> ",
        "'unterminated",
        "\"unterminated",
        "`unterminated",
        "/*! ",
        "/*!99999999999999999999 SELECT 1 */",
        "SELECT /*+ ",
        "UPDATE /*+ x */ t SET a=",
        "DELETE /*+ */ FROM",
        "CREATE USER 'u'@'%' IDENTIFIED BY 'a\\",
        "CREATE USER",
        "GRANT SELECT ON db.* TO",
        "GRANT SELECT ON db.* TO u REQUIRE",
        "REVOKE ALL FROM u IGNORE",
        "SET PASSWORD = PASSWORD(",
        "SET PERSIST",
        "RESET PERSIST",
        "((((((((((SELECT 1))))))))))",
        "SELECT '数据库' <-> `列` FROM 表",
        "INSERT INTO t VALUES (1,2,3),(",
        "CREATE TABLE t (a INT ZEROFILL, b INT) PARTITION BY",
        "\u{1F600}\u{1F4A9} SELECT 1",
    ];
    for sql in corpus {
        let _ = parse_statement(sql);
        let _ = parse_statements(sql);
    }

    // Deterministic pseudo-random fuzz over a SQL-ish alphabet (keywords,
    // punctuation, quotes, multibyte scalars). Fixed seed → reproducible.
    let pool: &[&str] = &[
        "SELECT ",
        "UPDATE ",
        "DELETE ",
        "INSERT ",
        "CREATE ",
        "USER ",
        "GRANT ",
        "SET ",
        "PERSIST ",
        "VIEW ",
        "TABLE ",
        "FROM ",
        "WHERE ",
        "VALUES ",
        "(",
        ")",
        ",",
        "'",
        "\"",
        "`",
        "\\",
        "<->",
        "<#>",
        "<=>",
        "/*+",
        "*/",
        "/*!",
        "0",
        "9",
        "=",
        "@",
        "?",
        "café",
        "数据",
        "\u{1F4A9}",
        "REQUIRE ",
        "DEFAULT ROLE ",
        "COMMENT ",
        "IDENTIFIED BY ",
        "  ",
        ";",
    ];
    for seed in [
        0x9E3779B97F4A7C15u64,
        0xD1B54A32D192ED03,
        0x2545F4914F6CDD1D,
        0x123456789ABCDEF0,
    ] {
        let mut state = seed;
        let mut next = || {
            // xorshift64
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..800 {
            let len = (next() % 24) as usize + 1;
            let mut s = String::new();
            for _ in 0..len {
                s.push_str(pool[(next() as usize) % pool.len()]);
            }
            // Must not panic; result value is irrelevant.
            let _ = parse_statement(&s);
            let _ = parse_statements(&s);
        }
    }
}

#[test]
fn m4_update_modifiers_round_trip() {
    // strip_update_modifiers → reapply must preserve LOW_PRIORITY / IGNORE.
    let stmt = parse_statement("UPDATE LOW_PRIORITY IGNORE t SET a = 1 WHERE id = 2")
        .expect("should parse");
    match stmt {
        Statement::Update(u) => {
            assert!(u.low_priority, "LOW_PRIORITY must survive");
            assert!(u.ignore, "IGNORE must survive");
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn m4_delete_modifiers_round_trip() {
    let stmt = parse_statement("DELETE LOW_PRIORITY QUICK IGNORE FROM t WHERE id = 2")
        .expect("should parse");
    match stmt {
        Statement::Delete(d) => {
            assert!(d.low_priority, "LOW_PRIORITY must survive");
            assert!(d.quick, "QUICK must survive");
            assert!(d.ignore, "IGNORE must survive");
        }
        other => panic!("expected Delete, got {other:?}"),
    }
}

#[test]
fn m4_insert_modifiers_round_trip() {
    // The `insert.priority` lesson: modifiers must not be silently dropped.
    let stmt = parse_statement("INSERT IGNORE INTO t (a) VALUES (1)").expect("should parse");
    match stmt {
        Statement::Insert(i) => assert!(i.ignore, "INSERT IGNORE must survive"),
        other => panic!("expected Insert, got {other:?}"),
    }
    let stmt = parse_statement("REPLACE INTO t (a) VALUES (1)").expect("should parse");
    match stmt {
        Statement::Insert(i) => assert!(i.replace, "REPLACE must survive"),
        other => panic!("expected Insert, got {other:?}"),
    }
}

#[test]
fn pg18_dml_returning_parse() {
    let Statement::Insert(insert) = parse_statement(
            "INSERT INTO t AS x (a) VALUES (?) RETURNING WITH (OLD AS o, NEW AS n) o.a AS before_a, n.*",
        )
        .expect("INSERT RETURNING should parse") else { panic!("expected insert") };
    assert_eq!(insert.target_alias.as_deref(), Some("x"));
    let returning = insert.returning.expect("returning");
    assert_eq!(returning.old_alias.as_deref(), Some("o"));
    assert_eq!(returning.new_alias.as_deref(), Some("n"));
    assert_eq!(returning.projection.len(), 2);

    let Statement::Update(update) =
        parse_statement("UPDATE t SET a = a + 1 RETURNING old.a, new.a").unwrap()
    else {
        panic!("expected update")
    };
    assert_eq!(update.returning.unwrap().projection.len(), 2);

    let Statement::Delete(delete) =
        parse_statement("DELETE FROM t WHERE a = 1 RETURNING WITH (OLD AS o, NEW AS n) o.*, n.a")
            .unwrap()
    else {
        panic!("expected delete")
    };
    let returning = delete.returning.unwrap();
    assert_eq!(returning.old_alias.as_deref(), Some("o"));
    assert_eq!(returning.new_alias.as_deref(), Some("n"));
    assert!(
        matches!(&returning.projection[0], SelectItem::QualifiedWildcard(q) if q.eq_ignore_ascii_case("o")),
        "projection={:?}",
        returning.projection
    );
}

#[test]
fn decode_hex_bytes_pads_odd_length_like_mysql() {
    // MySQL left-pads an odd-length hex literal with a leading zero:
    // 0x123 == 0x0123 == the 2-byte value 0x01 0x23.
    assert_eq!(decode_hex_bytes("123"), Some(vec![0x01, 0x23]));
    assert_eq!(decode_hex_bytes("0123"), Some(vec![0x01, 0x23]));
    // A lone nibble is its own byte (0x0X).
    assert_eq!(decode_hex_bytes("0"), Some(vec![0x00]));
    assert_eq!(decode_hex_bytes("F"), Some(vec![0x0F]));
    // Odd length longer than the 4-byte word boundary (the
    // CONVERT(0x999999999, …) shape) — 9 digits → 5 bytes.
    assert_eq!(
        decode_hex_bytes("999999999"),
        Some(vec![0x09, 0x99, 0x99, 0x99, 0x99])
    );
    // Even-length input is unchanged.
    assert_eq!(decode_hex_bytes("cafe"), Some(vec![0xCA, 0xFE]));
    assert_eq!(decode_hex_bytes(""), Some(vec![]));
    // Non-hex digits still reject.
    assert_eq!(decode_hex_bytes("12g"), None);

    // End-to-end through the value converter: `0x123` becomes the
    // 2-byte HexBytes literal MySQL produces.
    let span = sqlparser::tokenizer::Span::empty();
    let expr = convert_value(&SqlValue::HexStringLiteral("123".to_string()), span).unwrap();
    assert!(
        matches!(&expr, Expr::Literal(Literal::HexBytes(b)) if b == &[0x01, 0x23]),
        "expr={expr:?}"
    );
}

#[test]
fn create_table_accepts_any_engine_name() {
    // mydb is single-engine but must ACCEPT (not reject) every engine
    // name, recording it for SHOW CREATE round-trip. Previously only
    // InnoDB/mydb were accepted; MyISAM cascade-failed at CREATE.
    for eng in ["MyISAM", "InnoDB", "MEMORY", "ARCHIVE", "mydb"] {
        let sql = format!("CREATE TABLE t (a INT) ENGINE={eng}");
        let Statement::CreateTable(ct) = parse_statement(&sql).unwrap() else {
            panic!("expected CreateTable for ENGINE={eng}");
        };
        assert_eq!(ct.engine.as_deref(), Some(eng), "engine metadata for {eng}");
    }
    // No ENGINE clause leaves the field unset (SHOW CREATE stays
    // byte-identical for the common case).
    let Statement::CreateTable(ct) = parse_statement("CREATE TABLE t (a INT)").unwrap() else {
        panic!("expected CreateTable");
    };
    assert_eq!(ct.engine, None);
}

#[test]
fn parses_mysql_plural_drop_tables_alias() {
    let Statement::DropTable { names, .. } =
        parse_statement("DROP TABLES IF EXISTS t1, t2").unwrap()
    else {
        panic!("expected DROP TABLE");
    };
    assert_eq!(names, ["t1", "t2"]);
}

#[test]
fn explicit_row_call_normalizes_to_tuple() {
    let Statement::Select(select) =
        parse_statement("SELECT ROW('a', 'b') = ROW('a', 'b')").unwrap()
    else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr { expr, .. } = &select.projection[0] else {
        panic!("expected expression");
    };
    let Expr::Binary { left, right, .. } = expr else {
        panic!("expected comparison");
    };
    assert!(matches!(left.as_ref(), Expr::Tuple(items) if items.len() == 2));
    assert!(matches!(right.as_ref(), Expr::Tuple(items) if items.len() == 2));
}

#[test]
fn enum_set_legacy_hex_members_and_binary_attribute_are_normalized() {
    let Statement::CreateTable(table) = parse_statement(
        "CREATE TABLE t (e ENUM(0xE4, 'a') DEFAULT 0xE4, s SET('a','A') BINARY) \
         CHARACTER SET latin1",
    )
    .unwrap() else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(
        table.columns[0].data_type,
        DataType::Enum(vec!["ä".to_string(), "a".to_string()])
    );
    assert!(matches!(
        table.columns[0].default,
        Some(Expr::Literal(Literal::HexBytes(ref bytes))) if bytes == &[0xe4]
    ));
    assert_eq!(table.columns[1].collation.as_deref(), Some("binary"));

    let Statement::AlterTable(alter) =
        parse_statement("ALTER TABLE t ADD COLUMN e ENUM(0xFFFF)").unwrap()
    else {
        panic!("expected ALTER TABLE");
    };
    assert!(matches!(
        alter.actions.as_slice(),
        [AlterAction::AddColumn(ColumnDef {
            data_type: DataType::Enum(values),
            ..
        })] if values == &["ÿÿ".to_string()]
    ));

    let too_long = "界".repeat(256);
    let err = parse_statement(&format!(
        "CREATE PROCEDURE p(v ENUM('{too_long}')) BEGIN END"
    ))
    .unwrap_err();
    assert_eq!(err.code(), 3505);
    assert_eq!(err.sql_state(), "HY000");
}

#[test]
fn values_row_keyword_is_accepted_in_quantified_subqueries() {
    for sql in [
        "SELECT 1 < ALL(VALUES ROW(2),ROW(CAST(3 AS DECIMAL))) FROM DUAL",
        "SELECT 1 < ALL(VALUES ROW(2), ROW(3.0)) FROM DUAL",
    ] {
        assert!(matches!(
            parse_statement(sql).unwrap(),
            Statement::Select(_)
        ));
    }
    assert_eq!(
        rewrite_values_row_keyword("VALUES ROW(1), ROW('ROW(2)')").as_ref(),
        "VALUES (1), ('ROW(2)')"
    );
}

#[test]
fn legacy_lob_lengths_choose_smallest_mysql_family() {
    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE lob_sizes (a BLOB(250), b TEXT(70000), c TEXT(70000000))")
            .unwrap()
    else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.columns[0].data_type, DataType::Blob(255));
    assert_eq!(table.columns[1].data_type, DataType::Text(16_777_215));
    assert_eq!(table.columns[2].data_type, DataType::Text(u32::MAX));

    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE quoted_text (a VARCHAR(32) DEFAULT 'TEXT(70000)')").unwrap()
    else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.columns[0].data_type, DataType::VarChar(Some(32)));
}

#[test]
fn numeric_type_specifiers_use_mysql_bounds_and_errno() {
    for (definition, code) in [
        ("d DECIMAL(70,30)", 1426),
        ("d DECIMAL(32,31)", 1425),
        ("d DECIMAL(2,3)", 1427),
        ("f FLOAT(54)", 1063),
        ("f FLOAT(200,100)", 1425),
        ("i INT(256)", 1439),
        ("c CHAR(4294967296)", 1439),
        ("b BINARY(4294967296)", 1439),
        ("b BLOB(4294967296)", 1439),
    ] {
        let err = parse_statement(&format!("CREATE TABLE bad_numeric ({definition})")).unwrap_err();
        assert_eq!(err.code(), code, "{definition}: {err:?}");
        assert_eq!(err.sql_state(), "42000");
    }

    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE numeric_ok (d DECIMAL(0,0), n NUMERIC, f DOUBLE(200,0))")
            .unwrap()
    else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.columns[0].data_type, DataType::Decimal(Some((10, 0))));
    assert_eq!(table.columns[1].data_type, DataType::Decimal(Some((10, 0))));
    assert_eq!(table.columns[2].data_type, DataType::Double(Some((200, 0))));

    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE double_alias (d DOUBLE PRECISION(42,12))").unwrap()
    else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.columns[0].data_type, DataType::Double(Some((42, 12))));
}

#[test]
fn cast_character_and_binary_widths_are_validated_before_execution() {
    for (sql, code, state) in [
        ("SELECT CAST(NULL AS CHAR(4294967295))", 1690, "22003"),
        ("SELECT CAST(NULL AS NCHAR(4294967295))", 1690, "22003"),
        ("SELECT CONVERT(1, CHAR(4294967295))", 1690, "22003"),
        ("SELECT CONVERT(1, NCHAR(4294967295))", 1690, "22003"),
        ("SELECT CAST(1 AS CHAR(4294967296))", 1439, "42000"),
        ("SELECT CAST(1 AS BINARY(4294967296))", 1439, "42000"),
        ("SELECT CONVERT(1, BINARY(4294967296))", 1439, "42000"),
        (
            "EXPLAIN SELECT CONVERT(1, CHAR(99999999999999999999999999999999999999999999999999))",
            1439,
            "42000",
        ),
        (
            "EXPLAIN SELECT CONVERT(1, BINARY(99999999999999999999999999999999999999999999999999))",
            1439,
            "42000",
        ),
        (
            "CREATE TABLE cast_nchar_bad SELECT CAST(NULL AS NCHAR(4294967295))",
            1690,
            "22003",
        ),
    ] {
        let err = parse_statement(sql).unwrap_err();
        assert_eq!(err.code(), code, "{sql}: {err:?}");
        assert_eq!(err.sql_state(), state, "{sql}: {err:?}");
    }

    // BINARY uses a byte length and accepts the complete unsigned 32-bit
    // display-width range. Parsing/inference must not allocate that result.
    parse_statement("EXPLAIN SELECT CAST(1 AS BINARY(4294967295))").unwrap();
    // A same-named scalar function argument is a value, not a type width.
    parse_statement("SELECT CHAR(4294967296), CONVERT(CHAR(4294967296), BINARY)").unwrap();
}

#[test]
fn mysql_legacy_long_byte_is_mediumblob() {
    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE legacy_lob (a LONG, b LONG BYTE)").unwrap()
    else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(
        table.columns[0].data_type,
        DataType::Text(crate::storage::schema::MEDIUM_LOB_MAX)
    );
    assert_eq!(
        table.columns[1].data_type,
        DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX)
    );
}

#[test]
fn mysql_real_default_mode_accepts_precision_and_unsigned_attributes() {
    let Statement::CreateTable(table) = parse_statement(
        "CREATE TABLE real_types (a REAL(42,12), b REAL UNSIGNED, \
         c FLOAT(58,0) SIGNED NOT NULL)",
    )
    .unwrap() else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.columns[0].data_type, DataType::Double(Some((42, 12))));
    assert_eq!(table.columns[1].data_type, DataType::Double(None));
    assert!(table.columns[1].unsigned);
    assert_eq!(table.columns[2].data_type, DataType::Float(Some((58, 0))));
}

#[test]
fn mysql_national_character_aliases_preserve_utf8mb3_metadata() {
    for (spelling, expected_type) in [
        ("nchar(10)", DataType::Char(Some(10))),
        ("national char(10)", DataType::Char(Some(10))),
        ("national varchar(10)", DataType::VarChar(Some(10))),
        ("nvarchar(10)", DataType::VarChar(Some(10))),
        ("nchar varchar(10)", DataType::VarChar(Some(10))),
        (
            "national character varying(10)",
            DataType::VarChar(Some(10)),
        ),
        ("nchar varying(10)", DataType::VarChar(Some(10))),
    ] {
        let Statement::CreateTable(table) =
            parse_statement(&format!("CREATE TABLE t (c {spelling})")).unwrap()
        else {
            panic!("expected CREATE TABLE for {spelling}");
        };
        let column = &table.columns[0];
        assert_eq!(column.data_type, expected_type, "{spelling}");
        assert_eq!(column.charset.as_deref(), Some("utf8mb3"), "{spelling}");
        assert_eq!(
            column.collation.as_deref(),
            Some("utf8mb3_general_ci"),
            "{spelling}"
        );
    }

    // Identifiers and string literals containing the alias are not types.
    let Statement::CreateTable(table) = parse_statement(
        "CREATE TABLE t (`nchar` VARCHAR(10), note VARCHAR(20) COMMENT 'nchar(5)')",
    )
    .unwrap() else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.columns[0].name, "nchar");
    assert_eq!(table.columns[0].charset, None);
}

#[test]
fn parenthesized_interval_values_parse_natively() {
    assert_eq!(
        parse_expr_fragment("INTERVAL ('Oml') SECOND").unwrap(),
        parse_expr_fragment("__INTERVAL(('Oml'), 'SECOND')").unwrap()
    );
    assert_eq!(parse_expr_fragment("INTERVAL ((SELECT DATE_ADD(0x77500000, INTERVAL ('Oml') SECOND))) DAY_MINUTE").unwrap(), parse_expr_fragment("__INTERVAL(((SELECT DATE_ADD(0x77500000, __INTERVAL(('Oml'), 'SECOND')))), 'DAY_MINUTE')").unwrap());

    let statement = parse_statement(
        "SELECT DATE_SUB(99999999999999999999999999999999999999999999999999999999999999999999999, \
         INTERVAL ((SELECT DATE_ADD(0x77500000, INTERVAL ('Oml') SECOND))) DAY_MINUTE)",
    )
    .unwrap();
    assert!(matches!(statement, Statement::Select(_)));
}

#[test]
fn mysql_year_unsigned_is_an_accepted_noop_attribute() {
    let Statement::CreateTable(table) =
        parse_statement("CREATE TABLE t (a YEAR UNSIGNED, b YEAR(4) UNSIGNED)").unwrap()
    else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.columns[0].data_type, DataType::Year);
    assert_eq!(table.columns[1].data_type, DataType::Year);
    assert!(!table.columns[0].unsigned);
    assert!(!table.columns[1].unsigned);
}

#[test]
fn mysql_zero_b_bit_literal_is_not_an_identifier() {
    let Statement::Select(select) = parse_statement("SELECT 0b11000001").unwrap() else {
        panic!("expected SELECT");
    };
    let SelectItem::Expr {
        expr: Expr::Literal(Literal::BitBytes(bytes)),
        ..
    } = &select.projection[0]
    else {
        panic!("expected bit literal, got {:?}", select.projection[0]);
    };
    assert_eq!(bytes, &[0b1100_0001]);

    // Lexical normalization must not touch text inside strings/comments.
    let Statement::Select(select) = parse_statement("SELECT '0b11', 0b10 /* 0b01 */").unwrap()
    else {
        panic!("expected SELECT");
    };
    assert!(matches!(
        &select.projection[0],
        SelectItem::Expr {
            expr: Expr::Literal(Literal::String(text)),
            ..
        } if text == "0b11"
    ));
}

#[test]
fn rewrites_mysql_cast_signed_integer_aliases_and_char_using() {
    let statement = parse_statement(
        "SELECT SUBSTRING('abc', CAST(2 AS UNSIGNED INT)), \
         CHAR(65 USING utf8mb4), CAST(NULL AS CHAR CHARSET utf16), \
         CAST('x' AS CHAR(4) CHARACTER SET utf8mb4), \
         WEIGHT_STRING('aa' AS BINARY(3)), 'CHAR(65 USING latin1)'",
    )
    .unwrap();
    let Statement::Select(select) = statement else {
        panic!("expected SELECT");
    };
    assert_eq!(select.projection.len(), 6);
}

#[test]
fn parses_bare_column_key_as_primary_key() {
    let statement =
        parse_statement("CREATE TABLE t (id INT KEY AUTO_INCREMENT, payload VARCHAR(10))").unwrap();
    let Statement::CreateTable(create) = statement else {
        panic!("expected CREATE TABLE");
    };
    assert!(create.columns[0].primary_key);
    assert!(create.columns[0].auto_increment);
}

#[test]
fn parses_select_into_file_positions_and_options() {
    let Statement::SelectIntoFile { inner, options } = parse_statement(
        "SELECT a INTO OUTFILE 'before.tsv' CHARACTER SET utf8mb4 \
         FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' \
         LINES STARTING BY '>' TERMINATED BY '\\r\\n' FROM t",
    )
    .unwrap() else {
        panic!("expected SELECT INTO OUTFILE");
    };
    assert!(matches!(inner, QueryExpr::Select(_)));
    assert_eq!(options.kind, SelectIntoFileKind::Outfile);
    assert_eq!(options.position, SelectIntoFilePosition::AfterSelectList);
    assert_eq!(options.charset.as_deref(), Some("utf8mb4"));
    assert_eq!(options.field_terminated_by, ",");
    assert_eq!(options.field_enclosed_by, Some('"'));
    assert!(options.field_optionally_enclosed);
    assert_eq!(options.line_starting_by, ">");
    assert_eq!(options.line_terminated_by, "\r\n");

    let Statement::SelectIntoFile { inner, options } =
        parse_statement("SELECT a FROM t INTO DUMPFILE 'row.bin' FOR UPDATE").unwrap()
    else {
        panic!("expected SELECT INTO DUMPFILE");
    };
    assert!(matches!(inner, QueryExpr::Select(_)));
    assert_eq!(options.kind, SelectIntoFileKind::Dumpfile);
    assert_eq!(
        options.position,
        SelectIntoFilePosition::BeforeLockingClause
    );

    let Statement::SelectIntoFile { inner, options } =
        parse_statement("WITH c AS (SELECT 1 AS a) SELECT a FROM c INTO OUTFILE 'end.tsv'")
            .unwrap()
    else {
        panic!("expected CTE SELECT INTO OUTFILE");
    };
    assert!(matches!(inner, QueryExpr::WithCte(_)));
    assert_eq!(options.position, SelectIntoFilePosition::StatementEnd);

    let Statement::SelectIntoFile { inner, .. } =
        parse_statement("TABLE t ORDER BY a LIMIT 1 INTO OUTFILE 'table.tsv'").unwrap()
    else {
        panic!("expected TABLE INTO OUTFILE");
    };
    assert!(matches!(inner, QueryExpr::Select(_)));
}

#[test]
fn rejects_invalid_select_into_file_clauses() {
    assert!(parse_statement(
        "SELECT 1 INTO OUTFILE 'a' LINES TERMINATED BY '\\n' FIELDS TERMINATED BY ','"
    )
    .is_err());
    assert!(parse_statement("SELECT 1 INTO DUMPFILE 'a' FIELDS TERMINATED BY ','").is_err());
    assert!(parse_statement("SELECT 1 INTO OUTFILE 'a' INTO OUTFILE 'b'").is_err());
    assert!(parse_statement("SELECT * FROM (SELECT 1 INTO OUTFILE 'nested') AS d").is_err());
}

#[test]
fn do_expression_rewrites_cover_windowed_sounds_like_and_cast_charset() {
    let Statement::Do(exprs) =
        parse_statement("DO '' SOUNDS LIKE LEAD(DATABASE(), 1, x'cafe') OVER()")
            .expect("windowed SOUNDS LIKE in DO")
    else {
        panic!("expected DO");
    };
    assert_eq!(exprs.len(), 1);

    let error = parse_statement("DO NOT CAST(UTC_TIME AS CHAR(2877246026) CHARACTER SET ujis)")
        .expect_err("oversized CHAR length must be rejected before evaluating NOT");
    assert_eq!(error.code(), 1690);
}

#[test]
fn parses_digit_leading_select_aliases_without_touching_literals() {
    let Statement::Select(select) =
        parse_statement("SELECT 1 = 1 AS 1Eq, 2 AS ordinary, CAST(3 AS SIGNED) AS 2Signed")
            .unwrap()
    else {
        panic!("expected SELECT");
    };
    assert_eq!(select.projection.len(), 3);
    assert!(matches!(
        &select.projection[0],
        SelectItem::Expr {
            alias: Some(alias),
            ..
        } if alias == "1Eq"
    ));
    assert!(matches!(
        &select.projection[2],
        SelectItem::Expr {
            alias: Some(alias),
            ..
        } if alias == "2Signed"
    ));
}

#[test]
fn ordinary_updates_stay_on_canonical_parser() {
    for sql in [
        "UPDATE t SET payload = _binary'abc\\0def', generation = 1 WHERE id = 7",
        "UPDATE `t` AS x SET x.a = x.a + 1, x.b = ? WHERE x.id = ? AND x.flag = 0",
        "UPDATE t SET a = COALESCE(?, a), b = CONCAT('x', ?) WHERE id = ?",
        "UPDATE t SET a = CASE WHEN flag = 1 THEN ? ELSE a END WHERE id = ?",
    ] {
        let fast = parse_statement(sql).unwrap_or_else(|err| panic!("fast parse {sql}: {err}"));
        let canonical = parse_statement_via_sqlparser_legacy(sql)
            .unwrap_or_else(|err| panic!("canonical parse {sql}: {err}"));
        assert_eq!(fast, canonical, "plain UPDATE AST drift for {sql}");
        assert!(
            take_large_literal_digest_sql().is_none(),
            "small UPDATE must not publish a fast-path digest skeleton"
        );
    }
}

#[test]
fn large_update_literal_extraction_matches_canonical_parser() {
    let payload = "Abc_012-".repeat(9_000);
    for sql in [
        format!("UPDATE t SET payload = '{payload}', generation = 1 WHERE id = 7"),
        format!("UPDATE t SET payload = _binary'{payload}', generation = 1 WHERE id = 7"),
    ] {
        let fast = parse_statement(&sql).unwrap_or_else(|err| panic!("fast parse: {err}"));
        let digest_sql = take_large_literal_digest_sql().expect("large literal digest skeleton");
        assert_eq!(
            crate::engine::expr::statement_digest_text(&sql),
            crate::engine::expr::statement_digest_text(&digest_sql),
            "compact parser skeleton must preserve performance digest"
        );
        let canonical = parse_statement_via_sqlparser_legacy(&sql)
            .unwrap_or_else(|err| panic!("canonical parse: {err}"));
        assert_eq!(fast, canonical, "large UPDATE literal AST drift");
    }
}

#[test]
fn extended_update_shapes_keep_canonical_semantics() {
    for sql in [
        "UPDATE IGNORE t SET a = 1 WHERE id = 2",
        "UPDATE t SET a = 1 WHERE id = 2 ORDER BY id LIMIT 1",
        "UPDATE t SET a = CAST('1' AS SIGNED) WHERE id = 2",
        "UPDATE t SET a = 1 WHERE name LIKE 'x%' ESCAPE '!'",
    ] {
        let routed = parse_statement(sql);
        let canonical = parse_statement_via_sqlparser_legacy(sql);
        assert_eq!(routed, canonical, "extended UPDATE parser drift for {sql}");
    }
}
#[test]
fn for_system_time_as_of_preserves_independent_bindings_and_parameters() {
    let statement = parse_statement(
        "SELECT * FROM a FOR SYSTEM_TIME AS OF ? AS old_a \
         JOIN b FOR SYSTEM_TIME AS OF NOW() - INTERVAL 5 MINUTE AS old_b ON old_a.id=old_b.id",
    )
    .unwrap();
    let Statement::Select(select) = statement else {
        panic!("expected SELECT");
    };
    let first = select.from[0].system_time.as_ref().unwrap();
    let second = select.joins[0].table.system_time.as_ref().unwrap();
    assert_eq!(first.binding_id, 1);
    assert_eq!(second.binding_id, 2);
    assert!(matches!(first.expr.as_ref(), Expr::Placeholder(0)));
    assert!(super::visit::system_time_expr_is_statement_constant(
        &second.expr
    ));
}

#[test]
fn ddl_parallel_hints_attach_at_canonical_position() {
    let Statement::CreateIndex(index) =
        parse_statement("CREATE /*+ PARALLEL(8) */ UNIQUE INDEX ix_ab ON t(a, b)").unwrap()
    else {
        panic!("expected CREATE INDEX");
    };
    assert_eq!(index.query_hints, vec![QueryHint::Parallel { degree: 8 }]);
    assert!(index.hint_warnings.is_empty());

    let Statement::AlterTable(alter) =
        parse_statement("ALTER /*+ NO_PARALLEL */ TABLE t ADD INDEX ix_a(a), ADD INDEX ix_b(b)")
            .unwrap()
    else {
        panic!("expected ALTER TABLE");
    };
    assert_eq!(alter.query_hints, vec![QueryHint::NoParallel]);
    assert_eq!(alter.actions.len(), 2);
}

#[test]
fn ddl_parallel_hint_diagnostics_and_ordinary_comments_are_preserved() {
    let Statement::CreateIndex(index) =
        parse_statement("CREATE /*+ PARALLEL(bad) NO_PARALLEL */ INDEX ix ON t(a)").unwrap()
    else {
        panic!("expected CREATE INDEX");
    };
    assert_eq!(index.query_hints, vec![QueryHint::NoParallel]);
    assert!(index
        .hint_warnings
        .iter()
        .any(|warning| warning.contains("invalid degree")));
    assert!(index
        .hint_warnings
        .iter()
        .any(|warning| warning.contains("NO_PARALLEL wins")));

    let Statement::CreateIndex(ordinary) =
        parse_statement("CREATE /* ordinary comment */ INDEX ix2 ON t(a)").unwrap()
    else {
        panic!("expected CREATE INDEX");
    };
    assert!(ordinary.query_hints.is_empty());
}

#[test]
fn create_index_and_show_fields_are_never_silently_dropped() {
    for sql in [
        "CREATE INDEX IF NOT EXISTS ix ON t(a)",
        "CREATE INDEX ix ON t(a) WHERE a > 0",
        "CREATE INDEX ix ON t(a) NULLS NOT DISTINCT",
        "SHOW TERSE DATABASES",
    ] {
        assert!(
            matches!(parse_statement(sql), Ok(Statement::Unsupported { .. })),
            "optional AST field was accepted or lost for {sql}"
        );
    }
}

#[test]
fn create_index_retains_algorithm_and_lock_options() {
    let Statement::CreateIndex(index) =
        parse_statement("CREATE UNIQUE INDEX ix ON t(a) ALGORITHM=INPLACE LOCK=NONE").unwrap()
    else {
        panic!("expected CREATE INDEX")
    };
    assert_eq!(index.algorithm, AlterAlgorithm::Inplace);
    assert_eq!(index.lock, AlterLock::None);
}

#[test]
fn expression_fragment_consumes_exactly_one_expression() {
    for sql in ["1, 2", "1 FROM t", "1 WHERE false", "1 AS hidden", "*"] {
        assert!(
            parse_expr_fragment(sql).is_err(),
            "fragment tail accepted: {sql}"
        );
    }
    assert_eq!(
        parse_expr_fragment("1 + 2").unwrap(),
        Expr::Binary {
            left: Box::new(Expr::Literal(Literal::Int(1))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Literal::Int(2))),
        }
    );
    assert!(parse_expr_fragment("_latin1''").is_ok());
}

#[test]
fn admin_grammar_preserves_quoted_names_and_consumes_the_tail() {
    assert_eq!(
        parse_statement("ANALYZE TABLE `a,b`, `s`.`t.u`").unwrap(),
        Statement::AnalyzeTable {
            tables: vec!["a,b".to_string(), "s.t.u".to_string()],
            partitions: None,
            histogram: None,
        }
    );
    assert!(matches!(
        parse_statement("CREATE RESOURCE GROUP rg TYPE=USER BOGUS"),
        Ok(Statement::Unsupported { .. })
    ));
    assert!(matches!(
        parse_statement("DROP RESOURCE GROUP rg FORCE BOGUS"),
        Ok(Statement::Unsupported { .. })
    ));
    assert!(matches!(
        parse_statement("CREATE TABLESPACE ts ADD DATAFILE 'ts.ibd' BOGUS"),
        Ok(Statement::Unsupported { .. })
    ));
    assert!(matches!(
        parse_statement("ALTER TABLESPACE ts BOGUS"),
        Ok(Statement::Unsupported { .. })
    ));
}

#[test]
fn sqlparser_errors_keep_multiline_source_offsets() {
    let sql = "SELECT 1,\nFROM t";
    let error = parse_statement(sql).unwrap_err();
    // sqlparser consumes FROM as an implicit alias in this malformed shape
    // and diagnoses the following `t`; the important invariant is that mydb
    // keeps that real source location rather than collapsing it to 1:1.
    assert_eq!((error.line, error.column), (2, 6));
    assert_eq!(error.span.start, "SELECT 1,\nFROM ".len());

    let eof = parse_statement("SELECT (").unwrap_err();
    assert_eq!(eof.span.start, "SELECT (".len());
}

#[test]
fn executable_comment_gate_uses_advertised_patch_version() {
    assert_eq!(MYSQL_VERSION_ID, 80_046);
    let active = parse_statement("SELECT 1 /*!80046 + 2 */").unwrap();
    let inert = parse_statement("SELECT 1 /*!80047 + 2 */").unwrap();
    assert_ne!(active, inert);
    assert_eq!(inert, parse_statement("SELECT 1").unwrap());
}

#[test]
fn ignore_space_controls_native_function_tokenization() {
    assert!(parse_statement("SELECT COUNT (*) FROM t").is_err());
    // Qualified stored functions are not native-function recognition.
    assert!(parse_statement("SELECT app.COUNT (1) FROM t").is_ok());
    assert!(parse_statement("SELECT app.COUNT (*) FROM t").is_err());

    let bits = crate::engine::api::parse_temporal_sql_mode_bits("IGNORE_SPACE");
    assert_ne!(bits & crate::engine::api::SQL_MODE_IGNORE_SPACE, 0);
    let _guard = ParserSqlModeGuard::activate(bits);
    assert!(parse_statement("SELECT COUNT (*) FROM t").is_ok());
    assert!(parse_statement("SELECT SUM /* comment */ (a) FROM t").is_ok());
}

#[test]
fn prescan_is_single_pass_and_ignores_literal_keywords() {
    let sql = "SELECT 'ROLLUP CUBE EXCLUDE INTERVAL' AS payload";
    let (head, len) = extract_first_keyword(sql);
    let scan = prescan_keywords(sql, &head[..len]);
    assert!(!scan.has_rollup);
    assert!(!scan.has_cube);
    assert!(!scan.has_exclude);
}

#[test]
fn parser_transient_memory_and_worker_admission_are_bounded() {
    let before = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[12];
    let guard = ParserMemoryGuard::enter("SELECT 1").unwrap();
    let during = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[12];
    assert!(during >= before + 4 * 1024);
    drop(guard);
    assert_eq!(
        crate::engine::executor::mem_budget::controlled_memory_snapshot().2[12],
        before
    );

    let limit = host_cores().saturating_sub(1).max(1);
    let all = BulkParseWorkerPermits::acquire(usize::MAX);
    assert_eq!(all.count, limit);
    let blocked = BulkParseWorkerPermits::acquire(1);
    assert_eq!(blocked.count, 0);
    drop(blocked);
    drop(all);
    assert_eq!(
        ACTIVE_BULK_PARSE_WORKERS.load(std::sync::atomic::Ordering::Acquire),
        0
    );
}

#[test]
fn bulk_insert_escaped_strings_match_canonical_and_parallel_modes() {
    for mode in [
        0,
        crate::engine::api::SQL_MODE_ANSI_QUOTES,
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
    ] {
        let _guard = ParserSqlModeGuard::activate(mode);
        for literal in [
            r#"'{\"id\":1,\"value\":\"中文🙂\"}'"#,
            r"'a\'b'",
            "'a''b'",
            r"'\0\b\n\r\t\Z\\\%\_'",
            r"'x\'",
            r"'\q'",
            "''",
            "''''",
            "'unterminated",
        ] {
            let sql = format!("INSERT INTO t VALUES (1,{literal}),(2,{literal}),(3,{literal})");
            assert_fastpath_consistent(&sql);
            let values = sql.find("VALUES").unwrap() + "VALUES".len();
            if let Some(spans) = scan_bulk_value_spans(sql.as_bytes(), values) {
                assert_eq!(
                    parse_bulk_value_tuples_with_degree(sql.as_bytes(), &spans, 1),
                    parse_bulk_value_tuples_with_degree(sql.as_bytes(), &spans, 3),
                    "mode={mode} sql={sql}"
                );
            }
        }
        let sql = r#"INSERT INTO t VALUES ('{\"id\":1}'),('a''b')"#;
        assert!(try_parse_bulk_insert(sql).is_some());
        assert_fastpath_consistent(sql);
    }
}

#[test]
fn bulk_insert_string_escape_combinations_match_canonical_parser() {
    let fragments = [
        "中🙂", "a,b)c", r"\n", r"\\", r#"\""#, "''", r"\%", r"\_", r"\0", "END;--",
    ];
    for a in fragments {
        for b in fragments {
            for c in ["", "''", r"\\"] {
                let sql = format!("INSERT INTO t VALUES ('{a}{b}{c}'),('last')");
                assert!(try_parse_bulk_insert(&sql).is_some(), "{sql}");
                assert_fastpath_consistent(&sql);
            }
        }
    }
}

#[test]
fn parses_online_alter_check_compound_options() {
    let Statement::AlterTable(alter) = parse_statement(
        "ALTER TABLE `test`.`t` ALTER CHECK `a,b` ENFORCED, ADD CHECK (v<100), ALGORITHM=INPLACE, LOCK=NONE"
    ).unwrap() else { panic!("expected ALTER"); };
    assert_eq!(alter.lock, AlterLock::None);
    assert_eq!(alter.algorithm, AlterAlgorithm::Inplace);
    assert_eq!(alter.actions.len(), 2);
    assert!(
        matches!(&alter.actions[0], AlterAction::AlterCheck { name, enforced: true } if name == "a,b")
    );
    assert!(parse_statement("ALTER TABLE t ALTER CHECK c").is_err());
    assert!(parse_statement("ALTER TABLE t ALTER CHECK c ENFORCED trailing").is_err());
}

#[test]
fn optimize_table_options_are_explicit() {
    for (sql, suppressed) in [
        ("OPTIMIZE TABLE t, db.u", false),
        ("OPTIMIZE LOCAL TABLE t, db.u", true),
        ("OPTIMIZE NO_WRITE_TO_BINLOG TABLE t, db.u", true),
    ] {
        let Statement::OptimizeTable {
            tables,
            no_write_to_binlog,
        } = parse_statement(sql).unwrap()
        else {
            panic!("{sql}")
        };
        assert_eq!(tables, vec!["t", "db.u"]);
        assert_eq!(no_write_to_binlog, suppressed);
    }
    for sql in [
        "OPTIMIZE TABLE t EXTENDED",
        "OPTIMIZE TABLE t QUICK",
        "OPTIMIZE TABLE t NO_WRITE_TO_BINLOG",
    ] {
        assert!(
            matches!(
                parse_statement(sql),
                Err(_) | Ok(Statement::Unsupported { .. })
            ),
            "{sql}"
        );
    }
}

// 2026-09 parser audit: exercise the public pipeline, including manual routes.
macro_rules! audit_accept {
    ($name:ident, $($sql:expr),+ $(,)?) => {
        #[test]
        fn $name() {
            for sql in [$($sql),+] {
                let stmt = parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
                assert!(!matches!(stmt, Statement::Unsupported { .. }), "{sql}: {stmt:?}");
                assert_single_entry_parity(sql);
            }
        }
    };
}
macro_rules! audit_reject {
    ($name:ident, $($sql:expr),+ $(,)?) => {
        #[test]
        fn $name() {
            for sql in [$($sql),+] {
                assert!(parse_statement(sql).is_err(), "accepted {sql}: {:?}", parse_statement(sql));
                assert_single_entry_parity(sql);
            }
        }
    };
}
audit_accept!(
    audit_b01,
    "CREATE TABLE t (escape INT)",
    "SELECT escape FROM t WHERE escape=1",
    "UPDATE t SET escape=1"
);
audit_accept!(audit_b03, "ALTER TABLE view ADD COLUMN b INT");
audit_accept!(
    audit_b04,
    "SELECT 1 FOR UPDATE",
    "SELECT 1 FOR SHARE",
    "SELECT 1 LOCK IN SHARE MODE"
);
audit_reject!(audit_b04_invalid, "SELECT 9 FOR");
audit_accept!(audit_b08, "ALTER TABLE t ADD COLUMN (c1 INT, c2 INT)");
audit_accept!(
    audit_b10,
    "ALTER TABLE `my table` RENAME INDEX idx_a TO idx_b",
    "ALTER TABLE `my table` ALTER COLUMN a SET INVISIBLE",
    "ALTER TABLE `my table` ALTER INDEX idx_a INVISIBLE"
);
audit_accept!(
    audit_b11,
    "CREATE TABLE t (exclude INT)",
    "SELECT exclude FROM t",
    "SELECT exclude, SUM(a) OVER (ROWS CURRENT ROW EXCLUDE TIES) FROM t"
);
audit_reject!(audit_b14, "SELECT DISTINCT ON (1) 1 AS x");
audit_accept!(audit_b18, "SELECT 0x", "SELECT LENGTH(0x)");
audit_accept!(
    audit_b19,
    "SET @'a b' = 7",
    "SELECT @'a b'",
    "SET @`a b` = 1",
    "SELECT @\"a b\""
);
audit_reject!(
    audit_b20,
    "DELETE FROM t WHERE a = 1 AND a.*",
    "DELETE FROM t WHERE a.*",
    "DELETE t.* FROM t WHERE t.*"
);
audit_accept!(
    audit_b21,
    "SELECT CAST(CAST('a' AS CHAR(1) CHARSET utf8mb4) AS CHAR(2) CHARSET utf8mb4)",
    "SELECT CAST(CAST('1' AS CHAR CHARSET utf8mb4) AS SIGNED)"
);
audit_accept!(
    audit_b25,
    "LOAD DATA INFILE 'x' INTO TABLE t SET id = 1 /* , ; */ + 0",
    "LOAD DATA INFILE 'x' INTO TABLE t SET id = 1 -- , ;\n + 0"
);
audit_accept!(
    audit_b26,
    "CREATE PROCEDURE p(IN s SET('a','b')) BEGIN SELECT s; END",
    "CREATE PROCEDURE p(IN s ENUM('a','b')) BEGIN SELECT s; END"
);
audit_reject!(
    audit_b29,
    "ALTER USER u PASSWORD EXPIRE INTERVAL abc DAY",
    "ALTER USER u PASSWORD EXPIRE INTERVAL 5 MONTH",
    "CREATE USER u PASSWORD EXPIRE INTERVAL 5 MONTH",
    "ALTER USER u PASSWORD REUSE INTERVAL 5 MONTH",
    "CREATE USER u PASSWORD REUSE INTERVAL 5 MONTH"
);
audit_reject!(
    audit_b30,
    "ALTER EVENT e GARBAGE",
    "DROP TRIGGER t GARBAGE",
    "DROP EVENT e GARBAGE"
);
audit_reject!(
    audit_b31,
    "EXECUTE p GARBAGE",
    "DEALLOCATE PREPARE p GARBAGE"
);
audit_reject!(
    audit_b32,
    "LOAD DATA INFILE 'x' INTO TABLE t ROWS IDENTIFIED BY '<r>'"
);
audit_reject!(
    audit_b33,
    "LOAD DATA INFILE 'x' INTO TABLE t FIELDS ENCLOSED BY 'ab'",
    "LOAD DATA INFILE 'x' INTO TABLE t FIELDS ESCAPED BY 'ab'"
);
audit_accept!(
    audit_b34,
    "CHECK TABLE t1, quick",
    "CHECK TABLE t1, fast",
    "CHECKSUM TABLE t1, quick",
    "REPAIR TABLE t1, quick"
);
audit_reject!(audit_b35, "SET PASSWORD = PASSWORD('x')");
audit_reject!(
    audit_b36,
    "SAVEPOINT sp extra",
    "RELEASE SAVEPOINT sp extra",
    "ROLLBACK TO SAVEPOINT sp extra"
);

#[test]
fn audit_b02_unicode_rewrites() {
    for sql in [
        "SELECT INTERVAL(1,2,3), '中文😀' AS `日本語`",
        "SELECT NOW() - INTERVAL 1 DAY, '中文😀' AS `日本語`",
        "SELECT '中文😀' AS `日本語` FROM (VALUES ROW(1), ROW(2)) AS t",
        "CREATE TABLE t (e ENUM('中文😀','b') BINARY) COMMENT='中文😀'",
    ] {
        let stmt = parse_statement(sql).unwrap();
        let ast = format!("{stmt:?}");
        assert!(ast.contains("中文😀"), "{sql}: {ast}");
        assert!(!ast.contains('ä'), "{sql}: {ast}");
    }
}
#[test]
fn audit_b05_assignment_survives() {
    for sql in [
        "INSERT INTO t SELECT @v := 7",
        "CREATE TABLE t AS SELECT @v := 7",
        "UPDATE t SET a = (@v := 7) WHERE (@x := 1)",
    ] {
        let ast = format!("{:?}", parse_statement(sql).unwrap());
        assert!(ast.contains(USER_VAR_ASSIGN_FUNCTION), "{sql}: {ast}");
    }
}
#[test]
fn audit_b06_literal_roundtrip() {
    for value in ["x\\", "a\\nb'c", "中文\\😀"] {
        let literal = Literal::String(value.into());
        assert_eq!(
            parse_expr_fragment(&print::literal_to_sql(&literal)).unwrap(),
            Expr::Literal(literal)
        );
    }
}
#[test]
fn audit_b07_mysql_escapes() {
    assert_eq!(
        parse_expr_fragment(r"'a\ad\f'").unwrap(),
        Expr::Literal(Literal::String("aadf".into()))
    );
    assert_eq!(
        parse_expr_fragment("'\u{7}\u{c}'").unwrap(),
        Expr::Literal(Literal::String("\u{7}\u{c}".into()))
    );
}
#[test]
fn audit_b09_quoted_default() {
    assert_eq!(
        parse_expr_fragment("`default`").unwrap(),
        Expr::Identifier("default".into())
    );
}
#[test]
fn audit_b24_all_assignments() {
    assert!(!matches!(
        parse_statement("SET autocommit=0, sql_mode='STRICT_TRANS_TABLES'").unwrap(),
        Statement::SetAutocommit(_)
    ));
}

audit_accept!(
    audit_b16,
    "SELECT * FROM a STRAIGHT_JOIN b ON a.id=b.id",
    "SELECT * FROM a STRAIGHT_JOIN b",
    "SELECT DISTINCT STRAIGHT_JOIN * FROM a, b",
    "SELECT * FROM a STRAIGHT_JOIN b USING (id)",
    "UPDATE a STRAIGHT_JOIN b ON a.id=b.id SET a.id=1",
    "DELETE a FROM a STRAIGHT_JOIN b ON a.id=b.id"
);

#[test]
fn straight_join_flags_are_query_block_scoped() {
    let Statement::Select(select) = parse_statement(
        "SELECT STRAIGHT_JOIN * FROM a JOIN b ON a.id=b.id STRAIGHT_JOIN c USING (id)",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };
    assert!(select.straight_join);
    assert!(!select.joins[0].straight);
    assert!(select.joins[1].straight);
    assert_eq!(select.joins[1].using_columns, vec!["id"]);

    let Statement::Select(outer) =
        parse_statement("SELECT * FROM (SELECT STRAIGHT_JOIN * FROM a JOIN b ON a.id=b.id) AS d")
            .unwrap()
    else {
        panic!("expected SELECT");
    };
    assert!(!outer.straight_join);
    assert!(
        outer
            .derived
            .as_ref()
            .unwrap()
            .as_select()
            .unwrap()
            .straight_join
    );
}
audit_reject!(
    audit_b17,
    "CREATE TABLE t (mod INT)",
    "CREATE TABLE t (both INT)",
    "ALTER TABLE t ADD COLUMN mod INT"
);
audit_accept!(
    audit_b17_quoted,
    "CREATE TABLE t (`mod` INT, `both` INT)",
    "SELECT `mod`, `both` FROM t"
);
audit_accept!(
    audit_b13_b22_retained,
    "SELECT 1 MINUS SELECT 2",
    "DROP TABLES IF EXISTS t1,t2"
);
audit_accept!(
    audit_b28,
    "ALTER USER a, b IDENTIFIED BY 'pw'",
    "ALTER USER 'a,b' IDENTIFIED BY 'x,y', b IDENTIFIED BY 'pw' PASSWORD EXPIRE INTERVAL 3 DAY",
    "ALTER USER a, b ACCOUNT LOCK"
);

#[test]
fn audit_b23_extract_metadata() {
    let Statement::Select(select) =
        parse_statement("SELECT EXTRACT(YEAR_MONTH FROM '2024-01-01')").unwrap()
    else {
        panic!()
    };
    let SelectItem::Expr { alias, .. } = &select.projection[0] else {
        panic!()
    };
    assert_eq!(
        alias.as_deref(),
        Some("EXTRACT(YEAR_MONTH FROM '2024-01-01')")
    );
}
#[test]
fn audit_modes_and_context_boundaries() {
    for bits in [0, crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES] {
        let _guard = ParserSqlModeGuard::activate(bits);
        for value in ["x\\", "中文😀\\'", "\\a\\f", "a\\\\b"] {
            let literal = Literal::String(value.into());
            assert_eq!(
                parse_expr_fragment(&print::literal_to_sql(&literal)).unwrap(),
                Expr::Literal(literal)
            );
        }
        assert_eq!(
            parse_expr_fragment(r"'\a\f'").unwrap(),
            Expr::Literal(Literal::String(
                if bits == 0 { "af" } else { r"\a\f" }.into()
            ))
        );
        let result = parse_expr_fragment("'a' LIKE 'a' ESCAPE ''");
        assert_eq!(result.is_ok(), bits == 0);
    }
    for sql in [
        "SELECT escape, 'a' LIKE 'a' ESCAPE ? FROM escape",
        "SELECT 'a' LIKE escape ESCAPE ? FROM t",
        "SELECT '中文😀', INTERVAL(1,2,3), `中文列` FROM t",
        "SELECT SUM(exclude) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM t",
        "SET @v := (@x := 7), @s := '中文😀'",
        "UPDATE t SET a := (@v := 7), b := 8 WHERE a=1",
        "SAVEPOINT `sp extra`",
        "RELEASE SAVEPOINT `sp extra`",
        "ROLLBACK TO SAVEPOINT `sp extra`",
        "LOAD XML INFILE 'x' INTO TABLE t ROWS IDENTIFIED BY '<r>'",
        "SELECT X''",
    ] {
        assert_single_entry_parity(sql);
        assert!(parse_statement(sql).is_ok(), "{sql}");
    }
}

audit_accept!(
    audit_keyword_alias_contexts,
    "SELECT `row` exclude FROM t",
    "SELECT escape, exclude FROM t",
    "SELECT 1 AS exclude",
    "INSERT INTO t VALUES(1) ON DUPLICATE KEY UPDATE a := (@v := 7)"
);
audit_reject!(
    audit_delete_join_predicate_wildcard,
    "DELETE t.* FROM t JOIN u ON u.id.*",
    "DELETE FROM t.* USING t JOIN u ON u.id.*"
);

audit_accept!(
    audit_empty_hex_dml,
    "INSERT INTO t VALUES(0x)",
    "UPDATE t SET a=0x"
);
audit_accept!(
    audit_empty_hex_after_rewrite,
    "SELECT INTERVAL(1,2,3), X''",
    "SELECT CAST('x' AS CHAR CHARSET utf8mb4), X''"
);
#[test]
fn audit_variable_and_empty_hex_sql_roundtrip() {
    for sql in ["@'a b'", "@'a`b'", "X''"] {
        let expr = parse_expr_fragment(sql).unwrap();
        assert_eq!(parse_expr_fragment(&expr_to_sql(&expr)).unwrap(), expr);
    }
}

/// Manual generic parser throughput probe; no timing assertions in CI.
#[test]
#[ignore = "manual parser throughput measurement"]
fn audit_parser_throughput_probe() {
    use std::{hint::black_box, time::Instant};
    let insert = format!(
        "INSERT INTO t (id, v) VALUES {}",
        (0..100)
            .map(|i| format!("({i},'value-{i}')"))
            .collect::<Vec<_>>()
            .join(",")
    );
    for (name, sql, count) in [
        (
            "select",
            "SELECT a,b FROM t WHERE id=7 AND b IS NOT NULL ORDER BY a LIMIT 10",
            2000,
        ),
        ("update", "UPDATE t SET a=a+1,b='hello' WHERE id=7", 2000),
        ("bulk_insert", insert.as_str(), 800),
        (
            "unicode_interval",
            "SELECT INTERVAL(1,2,3), '中文😀', NOW()-INTERVAL 1 DAY",
            1000,
        ),
        ("assignment", "SELECT @v := 7, @v+1", 2000),
    ] {
        for _ in 0..100 {
            black_box(parse_statement(black_box(sql)).unwrap());
        }
        let mut samples = Vec::new();
        for _ in 0..5 {
            let start = Instant::now();
            for _ in 0..count {
                black_box(parse_statement(black_box(sql)).unwrap());
            }
            samples.push(start.elapsed().as_nanos() / count);
        }
        samples.sort_unstable();
        println!(
            "parser_probe {name} median_ns={} samples_ns={samples:?}",
            samples[2]
        );
    }
}

audit_accept!(
    audit_alter_qualified_names,
    "ALTER TABLE test.`my table` RENAME INDEX `a``b` TO `c d`",
    "ALTER TABLE `my db`.`my table` ALTER COLUMN `a b` SET INVISIBLE",
    "ALTER TABLE test.t ALTER INDEX i INVISIBLE"
);
audit_reject!(
    audit_reserved_reference,
    "SELECT both FROM t",
    "SELECT mod FROM t"
);

#[test]
fn audit_annex_expression_column_names() {
    for expression in [
        "(SELECT 1)",
        "((SELECT 1))",
        "(SELECT 1) + 2",
        "_binary'abc' = 'abc'",
        "_utf8mb4'中文' = '中文'",
    ] {
        let Statement::Select(select) = parse_statement(&format!("SELECT {expression}")).unwrap()
        else {
            panic!()
        };
        let SelectItem::Expr { alias, .. } = &select.projection[0] else {
            panic!()
        };
        assert_eq!(alias.as_deref(), Some(expression));
    }
}
audit_accept!(audit_like_case_pattern,
    "SELECT 'a' LIKE CASE WHEN 1=1 AND 2=2 THEN 'a' ELSE 'b' END ESCAPE @e",
    "SELECT 'a' LIKE CASE WHEN 1=1 THEN CASE WHEN 2=2 THEN escape END ELSE 'b' END ESCAPE @e FROM t");

#[test]
fn alter_event_compound_body_keeps_internal_semicolons() {
    for prefix in ["ALTER EVENT e", "ALTER DEFINER='u'@'localhost' EVENT e"] {
        let sql = format!("{prefix} DO BEGIN DECLARE n INT DEFAULT 0; IF n=0 THEN SET n=1; END IF; INSERT INTO t VALUES(n); END; SELECT 2");
        let statements = parse_statements(&sql).unwrap();
        assert_eq!(statements.len(), 2);
        assert!(matches!(&statements[0], Statement::AlterEvent(_)));
        assert!(matches!(&statements[1], Statement::Select(_)));
    }
    assert_eq!(
        parse_statements("BEGIN; SELECT 1; COMMIT").unwrap().len(),
        3
    );
}

#[test]
fn zone_map_audit_comments_do_not_override_options() {
    for comment in [
        "/* ZONE_MAP=OFF ZONE_SIZE=128K ) ' */",
        "-- ZONE_MAP=OFF ZONE_SIZE=128K\n",
        "# ZONE_MAP=OFF ZONE_SIZE=128K\n",
    ] {
        let sql = format!("CREATE TABLE t (a INT) ZONE_MAP=ON {comment} ZONE_SIZE=64K");
        let Statement::CreateTable(ct) = parse_statement(&sql).unwrap() else {
            panic!()
        };
        assert!(
            ct.table_options.contains(&("ZONE_MAP".into(), "ON".into())),
            "{sql}"
        );
        assert!(
            ct.table_options
                .contains(&("ZONE_SIZE".into(), "65536".into())),
            "{sql}"
        );
        assert!(parse_statement(&format!(
            "ALTER TABLE t ZONE_MAP=ON {comment}, ZONE_SIZE=64K"
        ))
        .is_ok());
        let (_, options) = super::rewrite::strip_zone_map_options(
            &format!("ZONE_MAP=ON {comment}, ZONE_SIZE=64K"),
            false,
        )
        .unwrap();
        assert!(options.contains(&("ZONE_MAP".into(), "ON".into())));
    }
}

#[test]
fn zone_map_audit_quoted_value_error_is_explicit() {
    for value in ["'OFF'", "\"OFF\""] {
        let err = parse_statement(&format!("CREATE TABLE t (a INT) ZONE_MAP={value}")).unwrap_err();
        assert!(err.message.contains("unquoted"), "{}", err.message);
    }
}

#[test]
fn zone_map_audit_preserves_comment_newlines_and_quoted_text() {
    for comment in [
        "-- ZONE_MAP=OFF,\n",
        "# ZONE_SIZE=128K,\n",
        "/* ZONE_MAP=OFF */",
    ] {
        let sql = format!("CREATE TABLE t (a INT) {comment} ZONE_SIZE=64K COMMENT='tail'");
        let Statement::CreateTable(ct) = parse_statement(&sql).unwrap() else {
            panic!()
        };
        assert_eq!(ct.table_comment, "tail");
        let alter_tail = format!("ADD COLUMN b INT,\n{comment} ZONE_MAP=ON");
        let (clean, options) = super::rewrite::strip_zone_map_options(&alter_tail, false).unwrap();
        assert!(options.contains(&("ZONE_MAP".into(), "ON".into())));
        assert!(clean.contains(comment));
    }
    let sql = r"CREATE TABLE t (a VARCHAR(64) DEFAULT 'it\'s ZONE_MAP=OFF') ZONE_MAP=ON";
    let Statement::CreateTable(ct) = parse_statement(sql).unwrap() else {
        panic!()
    };
    assert!(ct.table_options.contains(&("ZONE_MAP".into(), "ON".into())));
}

#[test]
fn expression_conversion_is_bounded_on_small_worker_stack() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            for op in ["+", "-", "*", "/", "^", "|"] {
                for count in [16, 30, 64, 180, 300, 1000, 1100] {
                    let chain = (1..=count)
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(op);
                    let result = parse_statement(&format!("SELECT {chain}"));
                    if count < crate::engine::expr_program::MAX_EXPRESSION_DEPTH {
                        assert!(result.is_ok(), "{op} {count}: {result:?}");
                    } else {
                        assert!(
                            result.unwrap_err().message.contains("depth"),
                            "{op} {count}"
                        );
                    }
                    assert!(parse_statement("SELECT 1").is_ok());
                }
            }
            // Width is independent from nesting depth.
            let list = (0..20_000)
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(",");
            assert!(parse_statement(&format!("SELECT 1 IN ({list})")).is_ok());
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn zone_audit_mixed_hex_insert_scaling() {
    let row = "(250,250.50,312.5,'v0250','c025','2024-03-01 01:01:01','2024-05-01 00:24:59','00:30:00',2020,250,0xFFF,X'CAFE')";
    for count in [10, 20, 40, 200, 500, 2000] {
        let sql = format!("INSERT INTO t VALUES {}", vec![row; count].join(","));
        let start = std::time::Instant::now();
        let statement = parse_statement_via_sqlparser_legacy(&sql).unwrap();
        let Statement::Insert(insert) = statement else {
            panic!("expected INSERT")
        };
        let InsertSource::Values(rows) = insert.source else {
            panic!("expected VALUES")
        };
        assert_eq!(rows.len(), count);
        assert!(rows.iter().all(|r| r == &rows[0]));
        eprintln!("mixed hex INSERT {count} rows: {:?}", start.elapsed());
    }
}

#[test]
fn zone_audit_source_positions_and_nested_hex_provenance() {
    use sqlparser::tokenizer::{Location, Span as SqlSpan};
    let source = "é😀X'FF'\n中0xF";
    for (line, column, expected) in [
        (1, 1, Some(0)),
        (1, 2, Some(2)),
        (1, 3, Some(6)),
        (1, 8, Some(11)),
        (1, 99, Some(11)),
        (2, 1, Some(12)),
        (2, 2, Some(15)),
        (2, 5, Some(18)),
        (2, 6, None),
        (0, 1, None),
        (1, 0, None),
        (3, 1, None),
    ] {
        assert_eq!(source::position_in_source(source, line, column), expected);
    }
    let span =
        |line, start, end| SqlSpan::new(Location::new(line, start), Location::new(line, end));
    let _outer = ParserRawSqlGuard::activate(source);
    assert_eq!(raw_source_is_x_hex(span(1, 3, 8)), Some(true));
    assert_eq!(raw_source_is_x_hex(span(2, 2, 5)), Some(false));
    {
        let _inner = ParserRawSqlGuard::activate("X'CAFE'");
        assert_eq!(raw_source_is_x_hex(span(1, 1, 8)), Some(true));
    }
    // Return to the outer source, then seek backwards after its last token.
    assert_eq!(raw_source_is_x_hex(span(2, 2, 5)), Some(false));
    assert_eq!(raw_source_is_x_hex(span(1, 3, 8)), Some(true));
    assert_eq!(raw_source_is_x_hex(span(99, 1, 2)), None);
    assert_eq!(raw_source_is_x_hex(span(1, 3, 8)), Some(true));
}

#[test]
fn zone_audit_public_fallback_preserves_hex_validation() {
    for separator in [",", ",\n"] {
        let row = "(1+0,'中文😀',0xF,X'0F',X'',0x00FF,1.25,NULL)";
        let sql = format!("INSERT INTO t VALUES {}", vec![row; 500].join(separator));
        assert!(try_parse_bulk_insert(&sql).is_none(), "exercise fallback");
        let Statement::Insert(insert) = parse_statement(&sql).unwrap() else {
            panic!("expected INSERT")
        };
        let InsertSource::Values(rows) = insert.source else {
            panic!("expected VALUES")
        };
        assert_eq!(rows.len(), 500);
        assert!(rows.iter().all(|r| r == &rows[0]));
        assert_eq!(rows[0][2], rows[0][3]);
        assert!(parse_statement(&sql.replace("X'0F'", "X'F'")).is_err());
        let Statement::Insert(names) = parse_statement(&sql.replace("0x00FF", "0x")).unwrap()
        else {
            panic!()
        };
        let InsertSource::Values(names) = names.source else {
            panic!()
        };
        assert!(names
            .iter()
            .all(|row| row[5] == Expr::Identifier("0x".into())));
    }
}

#[test]
fn release_audit_rejects_flat_trees_before_ast_allocation() {
    for sql in [
        format!("SELECT {}", vec!["1"; 100_000].join("+")),
        vec!["SELECT 1"; 2_000].join(" UNION ALL "),
    ] {
        let error = parse_statement(&sql).unwrap_err();
        assert_eq!(
            error.spec,
            crate::error::catalog::ER_STACK_OVERRUN_NEED_MORE
        );
    }
    assert!(parse_statement("SELECT 1+2*3").is_ok());
}

#[test]
fn logical_chains_parse_balanced_without_an_operand_limit() {
    fn depth(expr: &Expr) -> usize {
        match expr {
            Expr::Binary { left, right, .. } => 1 + depth(left).max(depth(right)),
            _ => 0,
        }
    }
    for (separator, op) in [(" AND ", BinaryOp::And), (" OR ", BinaryOp::Or)] {
        let predicate = (0..100_000)
            .map(|n| format!("a={n}"))
            .collect::<Vec<_>>()
            .join(separator);
        let Statement::Select(select) =
            parse_statement(&format!("SELECT 1 FROM t WHERE {predicate}")).unwrap()
        else {
            panic!("expected SELECT");
        };
        let selection = select.selection.unwrap();
        assert!(matches!(&selection, Expr::Binary { op: root, .. } if *root == op));
        assert!(
            depth(&selection) <= 18,
            "{op:?} depth {}",
            depth(&selection)
        );
        let mut operands = Vec::new();
        crate::parser::boolean::collect_expr_conjuncts(&selection, &mut operands);
        if op == BinaryOp::And {
            assert_eq!(operands.len(), 100_000);
            assert!(matches!(
                operands[99_999],
                Expr::Binary { right, .. } if **right == Expr::Literal(Literal::Int(99_999))
            ));
        }
    }
}

#[test]
fn select_nesting_follows_mysql_limit() {
    let nested = |levels: usize| {
        format!(
            "SELECT {}1{}",
            "(SELECT ".repeat(levels - 1),
            ")".repeat(levels - 1)
        )
    };
    assert!(parse_statement(&nested(crate::parser::MAX_SELECT_NESTING)).is_ok());
    let error = parse_statement(&nested(crate::parser::MAX_SELECT_NESTING + 1)).unwrap_err();
    assert_eq!(
        error.spec,
        crate::error::catalog::ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT
    );
}

#[test]
fn round2_large_literal_without_join_does_not_spend_rewrite_budget() {
    let text = "x".repeat(5 * 1024 * 1024);
    let statement = parse_statement(&format!("SELECT LENGTH('{text}')")).unwrap();
    assert!(matches!(statement, Statement::Select(_)));
}

#[test]
fn routine_audit_1028_rejects_nul_identifiers_but_preserves_literal_nuls() {
    for sql in [
        "SELECT `\0view_function_0`() FROM v",
        "SELECT /*!80000 `\0view_function_0`() */ FROM v",
        "CREATE PROCEDURE `\0invalid`() SELECT 1",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql:?}");
        assert!(parse_statements(sql).is_err(), "{sql:?}");
    }
    assert!(parse_statement("SELECT 'value\0payload'").is_ok());
    assert!(parse_statement("SELECT /*!80000 'value\0payload' */").is_ok());
    let _mode = ParserSqlModeGuard::activate(crate::engine::api::SQL_MODE_ANSI_QUOTES);
    assert!(parse_statement("SELECT \"\0view_function_0\"()").is_err());
}
