use super::*;

#[test]
fn deep_expressions_return_results_or_errors_without_killing_worker() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            engine.execute("CREATE TABLE stack_t(id INT)").unwrap();
            engine.execute("INSERT INTO stack_t VALUES(1)").unwrap();
            for count in [16, 30, 64, 180, 300, 1000] {
                let chain = (1..=count)
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join("+");
                for sql in [
                    format!("SELECT {chain}"),
                    format!("SELECT {chain}+id FROM stack_t"),
                    format!("SELECT {chain}+SUM(id) FROM stack_t"),
                ] {
                    match engine.execute(&sql) {
                        Ok(QueryResponse::ResultSet(result)) => {
                            let extra = i64::from(sql.contains("FROM"));
                            assert_eq!(
                                crate::storage::value_to_string(&result.rows[0][0]),
                                (count * (count + 1) / 2 + extra).to_string()
                            );
                        }
                        Err(error) => {
                            assert!(count >= 64, "{sql}: {error:?}");
                            assert!(
                                error.is(crate::error::catalog::ER_PARSE_ERROR)
                                    || error.is(crate::error::catalog::ER_STACK_OVERRUN_NEED_MORE),
                                "{error:?}"
                            );
                        }
                        _ => panic!("expected result set"),
                    }
                    assert!(engine.execute("SELECT 7").is_ok());
                }
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Run `f` on a minimum-size worker stack: every recursive pass reachable from
/// these shapes must extend its own stack instead of relying on thread size.
fn on_small_stack(f: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap();
}

fn first_cell(engine: &StorageQueryEngine, sql: &str) -> String {
    match engine.execute(sql) {
        Ok(QueryResponse::ResultSet(result)) => crate::storage::value_to_string(&result.rows[0][0]),
        Ok(QueryResponse::StreamingResultSet(result)) => {
            let rows = QueryResponse::StreamingResultSet(result)
                .materialize()
                .unwrap();
            let QueryResponse::ResultSet(result) = rows else {
                panic!("expected rows for {sql}");
            };
            crate::storage::value_to_string(&result.rows[0][0])
        }
        other => panic!("{sql}: {other:?}"),
    }
}

fn nested(function: &str, depth: usize, leaf: &str, tail: &str) -> String {
    format!(
        "{}{leaf}{}",
        format!("{function}(").repeat(depth),
        format!("{tail})").repeat(depth)
    )
}

#[test]
fn deep_nested_shapes_bind_in_linear_time_on_small_stacks() {
    on_small_stack(|| {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute("CREATE TABLE deep_t(c VARCHAR(20))")
            .unwrap();
        engine
            .execute("INSERT INTO deep_t VALUES('2020-01-01')")
            .unwrap();
        let depth = 1000;
        // Each shape derives per-node metadata from its children; any pass
        // re-deriving a subtree per level is exponential and never finishes.
        for (sql, expected) in [
            (
                format!(
                    "SELECT LENGTH({}) FROM deep_t",
                    nested("REPLACE", depth, "c", ",'a','b'")
                ),
                "10".to_string(),
            ),
            (
                format!(
                    "SELECT LENGTH({}) FROM deep_t",
                    nested("CONCAT", depth, "c", ",'a'")
                ),
                (10 + depth).to_string(),
            ),
            (
                format!(
                    "SELECT {} FROM deep_t",
                    nested("ABS", depth, "LENGTH(c)", "")
                ),
                "10".to_string(),
            ),
            (
                format!(
                    "SELECT LENGTH({}) FROM deep_t",
                    nested("UPPER", depth, "c", "")
                ),
                "10".to_string(),
            ),
            (
                format!(
                    "SELECT {} FROM deep_t",
                    nested("IFNULL", depth, "LENGTH(c)", ",0")
                ),
                "10".to_string(),
            ),
            (
                format!(
                    "SELECT COUNT(*) FROM deep_t WHERE {} > 0",
                    nested("ABS", depth, "LENGTH(c)", "")
                ),
                "1".to_string(),
            ),
            (
                format!(
                    "SELECT {}1{} FROM deep_t",
                    "CASE WHEN LENGTH(c)=0 THEN 0 ELSE ".repeat(depth),
                    " END".repeat(depth)
                ),
                "1".to_string(),
            ),
            (
                format!("SELECT {} FROM deep_t", vec!["LENGTH(c)"; depth].join("+")),
                (10 * depth).to_string(),
            ),
            (
                format!(
                    "SELECT {}1{}",
                    "(SELECT ".repeat(crate::parser::MAX_SELECT_NESTING - 1),
                    ")".repeat(crate::parser::MAX_SELECT_NESTING - 1)
                ),
                "1".to_string(),
            ),
        ] {
            assert_eq!(first_cell(&engine, &sql), expected, "{}", &sql[..80]);
        }
        let error = engine
            .execute(&format!("SELECT {}", nested("ABS", depth + 30, "1", "")))
            .unwrap_err();
        assert!(
            error.is(crate::error::catalog::ER_STACK_OVERRUN_NEED_MORE),
            "{error:?}"
        );
        assert_eq!(first_cell(&engine, "SELECT 7"), "7");
    });
}

#[test]
fn wide_boolean_chains_and_set_operations_run_on_small_stacks() {
    on_small_stack(|| {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine.execute("CREATE TABLE wide_t(a INT, b INT)").unwrap();
        engine
            .execute("INSERT INTO wide_t VALUES(1, 2), (5000, 5001), (7, 0)")
            .unwrap();
        let or = (0..20_000)
            .map(|n| format!("a={n}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        assert_eq!(
            first_cell(&engine, &format!("SELECT COUNT(*) FROM wide_t WHERE {or}")),
            "3"
        );
        let and = (10_000..30_000)
            .map(|n| format!("a<>{n}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        assert_eq!(
            first_cell(&engine, &format!("SELECT COUNT(*) FROM wide_t WHERE {and}")),
            "3"
        );
        let pairs = (0..=5_000)
            .map(|n| format!("(a={n} AND b={})", n + 1))
            .collect::<Vec<_>>()
            .join(" OR ");
        assert_eq!(
            first_cell(
                &engine,
                &format!("SELECT COUNT(*) FROM wide_t WHERE {pairs}")
            ),
            "2"
        );
        let branches = crate::parser::max_set_operation_branches();
        for (separator, expected) in [(" UNION ALL ", branches), (" UNION ", 7)] {
            let union = (0..branches)
                .map(|n| format!("SELECT {} AS n", if expected == 7 { n % 7 } else { n }))
                .collect::<Vec<_>>()
                .join(separator);
            assert_eq!(
                first_cell(&engine, &format!("SELECT COUNT(*) FROM ({union}) d")),
                expected.to_string()
            );
            engine
                .execute(&format!("CREATE OR REPLACE VIEW wide_v AS {union}"))
                .unwrap();
            assert_eq!(
                first_cell(&engine, "SELECT COUNT(*) FROM wide_v"),
                expected.to_string()
            );
        }
        let too_many = vec!["SELECT 1"; branches + 1].join(" UNION ALL ");
        assert!(engine
            .execute(&too_many)
            .unwrap_err()
            .is(crate::error::catalog::ER_STACK_OVERRUN_NEED_MORE));
        assert_eq!(first_cell(&engine, "SELECT 7"), "7");
    });
}
