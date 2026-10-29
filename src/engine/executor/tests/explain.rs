use super::*;
use crate::types::Value;

fn response_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set")
    };
    result.rows
}

fn strip_tree_actuals(text: &str) -> String {
    text.lines()
        .filter(|line| {
            !line.starts_with("-- CTE ")
                && !line.starts_with("-- BODY:")
                && !line.starts_with("Query I/O:")
        })
        .map(|line| {
            let cut = line
                .find(" (actual time=")
                .or_else(|| line.find(" (never executed)"))
                .or_else(|| line.find(" (included in operator"))
                .unwrap_or(line.len());
            &line[..cut]
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn remove_json_actuals(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.remove("actual");
            map.remove("execution");
            for value in map.values_mut() {
                remove_json_actuals(value);
            }
        }
        serde_json::Value::Array(items) => {
            for value in items {
                remove_json_actuals(value);
            }
        }
        _ => {}
    }
}

fn find_json_key<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    match value {
        serde_json::Value::Object(map) => map
            .get(key)
            .or_else(|| map.values().find_map(|value| find_json_key(value, key))),
        serde_json::Value::Array(values) => {
            values.iter().find_map(|value| find_json_key(value, key))
        }
        _ => None,
    }
}

fn find_json_table<'a>(
    value: &'a serde_json::Value,
    table_name: &str,
) -> Option<&'a serde_json::Value> {
    match value {
        serde_json::Value::Object(map) => {
            if map.get("table_name").and_then(|value| value.as_str()) == Some(table_name) {
                return Some(value);
            }
            map.values()
                .find_map(|value| find_json_table(value, table_name))
        }
        serde_json::Value::Array(values) => values
            .iter()
            .find_map(|value| find_json_table(value, table_name)),
        _ => None,
    }
}

#[test]
fn explain_source_free_select_reports_no_tables_used() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    for sql in ["EXPLAIN SELECT 10", "EXPLAIN SELECT 10 FROM DUAL"] {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected traditional explain result for `{sql}`");
        };
        assert_eq!(result.columns.len(), 12, "{sql}");
        assert_eq!(
            result.rows,
            vec![vec![
                Value::Int(1),
                Value::String("SIMPLE".to_string()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::String("No tables used".to_string()),
            ]],
            "{sql}"
        );
    }

    let tree = explain_single_text(&engine, "EXPLAIN FORMAT=TREE SELECT 10");
    assert!(tree.contains("Rows fetched before execution"), "{tree:?}");

    let json: serde_json::Value = serde_json::from_str(&explain_single_text(
        &engine,
        "EXPLAIN FORMAT=JSON SELECT 10",
    ))
    .unwrap();
    assert_eq!(json["query_block"]["select_id"], 1);
    assert_eq!(json["query_block"]["message"], "No tables used");
}

#[test]
fn explain_analyze_source_free_uses_runtime_semantics_and_real_row_counts() {
    std::fs::create_dir_all("/tmp/mydb/http-functions").unwrap();
    let dir = tempfile::Builder::new()
        .prefix("explain-")
        .tempdir_in("/tmp/mydb/http-functions")
        .unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.current_database = "test".into();
    engine
        .execute("CREATE TABLE source_free_probe(id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO source_free_probe VALUES (1)")
        .unwrap();
    for format in ["TREE", "JSON", "TRADITIONAL"] {
        engine
            .execute_with_session(&mut session, "SET @explain_effect=0")
            .unwrap();
        engine
            .execute_with_session(
                &mut session,
                &format!("EXPLAIN FORMAT={format} SELECT @explain_effect:=@explain_effect+1"),
            )
            .unwrap();
        let value = engine
            .execute_with_session(&mut session, "SELECT @explain_effect")
            .unwrap();
        assert_eq!(response_rows(value)[0][0], Value::Int(0));
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "EXPLAIN ANALYZE FORMAT={format} SELECT @explain_effect:=@explain_effect+1"
                ),
            )
            .unwrap();
        let value = engine
            .execute_with_session(&mut session, "SELECT @explain_effect")
            .unwrap();
        assert_eq!(response_rows(value)[0][0], Value::Int(1));

        for (target, expected) in [
            ("SELECT 10", 1),
            ("SELECT 10 WHERE 0", 0),
            ("SELECT 10 LIMIT 0", 0),
            ("SELECT 10 LIMIT 1 OFFSET 1", 0),
            ("SELECT COUNT(*)", 1),
            ("SELECT COUNT(*) HAVING 0", 0),
            ("SELECT ROW_NUMBER() OVER ()", 1),
            ("SELECT (SELECT COUNT(*) FROM source_free_probe)", 1),
        ] {
            let result = response_rows(
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("EXPLAIN ANALYZE FORMAT={format} {target}"),
                    )
                    .unwrap(),
            );
            if format == "TRADITIONAL" {
                assert_eq!(result[0].len(), 17);
                assert_eq!(result[0][16], Value::Int(expected), "{target}");
            } else {
                let Value::String(text) = &result[0][0] else {
                    panic!("expected plan")
                };
                if format == "JSON" {
                    let plan: serde_json::Value = serde_json::from_str(text).unwrap();
                    assert_eq!(
                        plan["query_block"]["execution"]["rows_total"], expected,
                        "{target}"
                    );
                } else {
                    assert!(
                        text.contains(&format!("rows={expected} loops=1")),
                        "{target}: {text}"
                    );
                }
            }
        }
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO source_free_probe VALUES (2)")
            .unwrap();
        engine.execute_with_session(&mut session, &format!("EXPLAIN ANALYZE FORMAT={format} SELECT @explain_effect := (SELECT COUNT(*) FROM source_free_probe)")).unwrap();
        let value = engine
            .execute_with_session(&mut session, "SELECT @explain_effect")
            .unwrap();
        assert_eq!(response_rows(value)[0][0], Value::Int(2));
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        let err = engine
            .execute_with_session(
                &mut session,
                &format!("EXPLAIN ANALYZE FORMAT={format} SELECT unknown_source_free_column"),
            )
            .unwrap_err();
        assert!(err.is(crate::error::catalog::ER_BAD_FIELD_ERROR), "{err}");
    }
}

#[test]
fn explain_analyze_static_plan_matches_plain_explain_in_all_formats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ep_a (id INT PRIMARY KEY, k INT, INDEX idx_k(k))")
        .unwrap();
    engine
        .execute("CREATE TABLE ep_b (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO ep_a VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    engine
        .execute("INSERT INTO ep_b VALUES (10,1,10),(20,2,20),(30,4,30)")
        .unwrap();
    let sql = "SELECT /*+ HASH_JOIN(ep_a, ep_b) */ ep_a.id, ep_b.v \
               FROM ep_a JOIN ep_b ON ep_a.k = ep_b.k WHERE ep_b.v >= 10";

    let plain_tree = explain_single_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    let analyzed_tree = explain_single_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
    assert_eq!(plain_tree.trim(), strip_tree_actuals(&analyzed_tree).trim());

    let mut plain_json: serde_json::Value = serde_json::from_str(&explain_single_text(
        &engine,
        &format!("EXPLAIN FORMAT=JSON {sql}"),
    ))
    .unwrap();
    let mut analyzed_json: serde_json::Value = serde_json::from_str(&explain_single_text(
        &engine,
        &format!("EXPLAIN ANALYZE FORMAT=JSON {sql}"),
    ))
    .unwrap();
    remove_json_actuals(&mut plain_json);
    remove_json_actuals(&mut analyzed_json);
    assert_eq!(plain_json, analyzed_json);

    let QueryResponse::ResultSet(plain) = engine.execute(&format!("EXPLAIN {sql}")).unwrap() else {
        panic!("expected traditional explain");
    };
    let QueryResponse::ResultSet(analyzed) = engine
        .execute(&format!("EXPLAIN ANALYZE FORMAT=TRADITIONAL {sql}"))
        .unwrap()
    else {
        panic!("expected traditional analyze");
    };
    assert_eq!(plain.rows.len(), analyzed.rows.len());
    for (plain, analyzed) in plain.rows.iter().zip(&analyzed.rows) {
        assert_eq!(plain.as_slice(), &analyzed[..plain.len()]);
    }
}

#[test]
fn explain_analyze_system_scan_inherits_statement_registry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let sql = "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS \
               WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='COLUMNS' \
               ORDER BY ORDINAL_POSITION";

    let plain = explain_single_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    let analyzed = explain_single_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
    assert_eq!(plain.trim(), strip_tree_actuals(&analyzed).trim());
    assert!(analyzed.contains("actual time="), "{analyzed}");
}

#[test]
fn explain_analyze_window_static_tree_matches_plain_explain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ep_w (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO ep_w VALUES (1,1,30),(2,1,10),(3,2,20)")
        .unwrap();
    let sql = "SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn FROM ep_w";
    let plain = explain_single_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    let analyzed = explain_single_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
    assert_eq!(plain.trim(), strip_tree_actuals(&analyzed).trim());
}

#[test]
fn explain_analyze_cte_static_tree_matches_plain_explain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ep_c (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO ep_c VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    let sql = "WITH c AS (SELECT id, v FROM ep_c WHERE v >= 20) \
               SELECT id FROM c ORDER BY id";
    let plain = explain_single_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    let analyzed = explain_single_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
    assert!(!analyzed.contains("-- CTE"), "{analyzed}");
    assert!(!analyzed.contains("-- BODY"), "{analyzed}");
    assert_eq!(plain.trim(), strip_tree_actuals(&analyzed).trim());
}

#[test]
fn optimizer_trace_records_bounded_cbo_plan_details() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET optimizer_trace='enabled=on'")
        .unwrap();
    assert!(session.sysvars.optimizer_trace.contains("enabled=on"));
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE trace_t (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SELECT v FROM trace_t WHERE id = 1")
        .unwrap();
    let QueryResponse::ResultSet(trace) = engine
        .execute_with_session(
            &mut session,
            "SELECT QUERY, TRACE, MISSING_BYTES_BEYOND_MAX_MEM_SIZE, INSUFFICIENT_PRIVILEGES FROM information_schema.optimizer_trace",
        )
        .unwrap()
    else {
        panic!("expected optimizer trace result set");
    };
    assert_eq!(trace.rows.len(), 1);
    let rendered = trace.rows[0][1].to_display_string();
    assert!(rendered.contains("plan_selected"), "{rendered}");
    assert!(rendered.contains("\"model\":"), "{rendered}");
    assert_eq!(trace.rows[0][3], Value::UInt(0));
}

#[test]
fn optimizer_trace_and_analyze_report_noncovering_range_probe_geometry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET optimizer_trace='enabled=on'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE trace_range (id INT PRIMARY KEY, d INT, v INT, INDEX idx_d(d))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO trace_range VALUES (1,1,10),(2,2,20),(3,3,30),(4,4,40)",
        )
        .unwrap();
    let QueryResponse::ResultSet(analyze) = engine
        .execute_with_session(
            &mut session,
            "EXPLAIN ANALYZE FORMAT=TREE SELECT /*+ USE_INDEX(trace_range idx_d) */ v FROM trace_range WHERE d BETWEEN 1 AND 4",
        )
        .unwrap()
    else {
        panic!("expected analyze result set");
    };
    let tree = analyze.rows[0][0].to_display_string();
    assert!(tree.contains("index_entries_examined=4"), "{tree}");
    assert!(tree.contains("heap_rows_fetched=4"), "{tree}");
    assert!(tree.contains("heap_page_probes="), "{tree}");

    let QueryResponse::ResultSet(trace) = engine
        .execute_with_session(
            &mut session,
            "SELECT TRACE FROM information_schema.optimizer_trace",
        )
        .unwrap()
    else {
        panic!("expected optimizer trace result set");
    };
    let rendered = trace.rows[0][0].to_display_string();
    assert!(rendered.contains("scan_candidates"), "{rendered}");
    assert!(rendered.contains("logical_heap_probes"), "{rendered}");
    assert!(rendered.contains("projection_width"), "{rendered}");
}

#[test]
fn explain_for_connection_returns_unknown_thread_for_missing_id() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = engine
        .execute_with_session(&mut session, "EXPLAIN FOR CONNECTION 99999")
        .unwrap_err();
    assert_eq!(err.code(), 1094, "{}", err.message());
}

#[test]
fn explain_for_connection_parses_format_clause() {
    let parsed = crate::parser::parse_statement("EXPLAIN FORMAT=JSON FOR CONNECTION 17").unwrap();
    let crate::parser::Statement::ExplainForConnection {
        connection_id,
        format,
    } = parsed
    else {
        panic!("expected ExplainForConnection, got {parsed:?}");
    };
    assert_eq!(connection_id, 17);
    assert!(matches!(format, Some(crate::parser::ExplainFormat::Json)));
}

struct EnvRestore {
    key: &'static str,
    prior: Option<String>,
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match self.prior.take() {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn restore_env(key: &'static str, prior: Option<String>) -> EnvRestore {
    EnvRestore { key, prior }
}

fn explain_rows_estimate(engine: &StorageQueryEngine, sql: &str) -> i64 {
    let QueryResponse::ResultSet(explain) = engine.execute(sql).unwrap() else {
        panic!("expected explain result set");
    };
    match &explain.rows[0][9] {
        Value::Int(v) => *v,
        other => panic!("unexpected rows column: {other:?}"),
    }
}

fn explain_tree_text(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(explain) = engine.execute(sql).unwrap() else {
        panic!("expected explain result set");
    };
    explain
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|value| match value {
            Value::String(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn explain_single_text(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(explain) = engine.execute(sql).unwrap() else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.rows.len(), 1);
    let Value::String(text) = &explain.rows[0][0] else {
        panic!("expected string explain output");
    };
    text.clone()
}

fn actual_time_ms(line: &str) -> f64 {
    let marker = "actual time=";
    let Some(start) = line.find(marker) else {
        panic!("line does not contain actual time: {line}");
    };
    let rest = &line[start + marker.len()..];
    let completion = rest
        .split_once("..")
        .map(|(_, value)| value)
        .unwrap_or(rest);
    let end = completion.find(' ').unwrap_or(completion.len());
    completion[..end]
        .parse::<f64>()
        .unwrap_or_else(|e| panic!("invalid actual time ({e}) in line: {line}"))
}

fn explain_all_text(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(explain) = engine.execute(sql).unwrap() else {
        panic!("expected explain result set");
    };
    explain
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|value| match value {
            Value::String(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn explain_tree_flattens_q9_shaped_derived_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE part (p_partkey INT PRIMARY KEY, p_name VARCHAR(55))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, \
             l_quantity INT, l_extendedprice INT, l_discount INT, \
             PRIMARY KEY (l_orderkey, l_linenumber), \
             INDEX idx_lineitem_partkey (l_partkey), \
             INDEX idx_lineitem_suppkey (l_suppkey), \
             INDEX idx_lineitem_partsuppkey (l_partkey, l_suppkey))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_nationkey INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE partsupp (\
             ps_partkey INT, ps_suppkey INT, ps_supplycost INT, \
             PRIMARY KEY (ps_partkey, ps_suppkey))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_orderdate DATE)")
        .unwrap();
    engine
        .execute("CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name VARCHAR(25))")
        .unwrap();
    engine
        .execute("INSERT INTO part VALUES (1, 'forest green part'), (2, 'plain red part')")
        .unwrap();
    engine
        .execute("INSERT INTO supplier VALUES (10, 1), (20, 2)")
        .unwrap();
    engine
        .execute("INSERT INTO nation VALUES (1, 'BRAZIL'), (2, 'CANADA')")
        .unwrap();
    engine
        .execute("INSERT INTO partsupp VALUES (1, 10, 3), (1, 20, 5), (2, 10, 7)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (100, '1995-03-01'), (200, '1996-07-10')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (100, 1, 10, 1, 2, 100, 0), \
             (200, 1, 20, 1, 1, 80, 0), \
             (200, 2, 10, 2, 4, 70, 0)",
        )
        .unwrap();
    for table in [
        "part", "lineitem", "supplier", "partsupp", "orders", "nation",
    ] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let tree = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE \
         SELECT nation, o_year, SUM(amount) AS sum_profit \
         FROM ( \
           SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, \
                  l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount \
           FROM part \
           INNER JOIN lineitem ON p_partkey = l_partkey \
           INNER JOIN supplier ON s_suppkey = l_suppkey \
           INNER JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey \
           INNER JOIN orders ON o_orderkey = l_orderkey \
           INNER JOIN nation ON s_nationkey = n_nationkey \
           WHERE p_name LIKE '%green%' \
         ) AS profit \
         GROUP BY nation, o_year \
         ORDER BY nation, o_year DESC",
    );
    assert!(
        tree.contains("HashAggregate"),
        "Q9-shaped derived aggregate should flatten into planned hash aggregate: {tree}"
    );
    assert!(
        tree.contains("Hash join") || tree.contains("Index nested loop join"),
        "flattened Q9-shaped query should keep a join plan visible: {tree}"
    );
    assert!(
        !tree.contains("Derived"),
        "flattened Q9-shaped tree should not expose derived materialization: {tree}"
    );
}

#[test]
fn explain_keeps_unsafe_derived_shapes_materialized() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    for sql in [
        "EXPLAIN SELECT SUM(v) FROM (SELECT id, v FROM t LIMIT 1) AS d",
        "EXPLAIN SELECT SUM(v) FROM (SELECT DISTINCT id, v FROM t) AS d",
        "EXPLAIN SELECT SUM(total) FROM (SELECT id, SUM(v) AS total FROM t GROUP BY id) AS d",
        "EXPLAIN SELECT id FROM (SELECT id, v FROM t) AS d LIMIT 1",
    ] {
        let text = explain_all_text(&engine, sql);
        assert!(
            text.contains("Derived"),
            "unsafe derived shape should keep materialized explain path for {sql}: {text}"
        );
    }
}

#[test]
fn explain_handles_multi_source_derived_cte_and_bare_correlation() {
    // Regression for the TPC-DS EXPLAIN gaps: a derived table / CTE joined
    // across multiple FROM positions, a nested derived table, a bare-column
    // correlated subquery, and a WITH-CTE (no declared column list) referenced
    // through a derived table. All execute fine; EXPLAIN must describe them
    // rather than failing with 1146 "table not found" / 1054 "unknown column".
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE item (i_item_sk INT PRIMARY KEY, i_cat INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE sales (s_item INT, s_amt INT, INDEX si (s_item))")
        .unwrap();
    engine
        .execute("INSERT INTO item VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO sales VALUES (1, 100), (2, 200)")
        .unwrap();

    // 1. Base table comma-joined with a derived table (multi-source): the
    //    derived alias must resolve as a materialised sub-plan.
    let t = explain_all_text(
        &engine,
        "EXPLAIN SELECT d.s_item FROM item, (SELECT s_item FROM sales) d \
         WHERE d.s_item = item.i_item_sk",
    );
    assert!(t.contains("Derived"), "multi-source derived: {t}");

    // 2. Nested derived table — EXPLAIN must follow the nesting down.
    let t = explain_all_text(
        &engine,
        "EXPLAIN SELECT y FROM (SELECT s_item AS y FROM (SELECT s_item FROM sales) a) b",
    );
    assert!(t.contains("Derived"), "nested derived: {t}");

    // 3. Bare-column correlated scalar subquery (`WHERE s_item = i_item_sk`,
    //    where `i_item_sk` is an outer column) under a multi-table outer. The
    //    unified planner decorrelates it into a grouped derived input and an
    //    ordinary join; EXPLAIN must show that input instead of failing with
    //    1054 while trying to describe the bare outer reference in isolation.
    let t = explain_all_text(
        &engine,
        "EXPLAIN SELECT item.i_item_sk FROM item, sales \
         WHERE item.i_item_sk = sales.s_item \
           AND sales.s_amt > (SELECT AVG(s_amt) FROM sales WHERE s_item = i_item_sk)",
    );
    assert!(t.contains("Derived"), "decorrelated derived input: {t}");
    assert!(!t.contains("Correlated"), "unexpected Apply fallback: {t}");

    // 4. WITH-CTE with no declared column list, referenced through a derived
    //    table in the body: the stub columns come from the CTE projection so
    //    `c.k` resolves.
    let t = explain_all_text(
        &engine,
        "EXPLAIN WITH c AS (SELECT s_item AS k, SUM(s_amt) AS tot FROM sales GROUP BY s_item) \
         SELECT z.k FROM (SELECT k FROM c) z",
    );
    assert!(t.contains("CTE"), "cte referenced via derived: {t}");
}

#[test]
fn explain_tree_omits_default_index_nested_loop_details() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE o (id INT PRIMARY KEY, d INT, INDEX d_idx (d))")
        .unwrap();
    engine
        .execute("CREATE TABLE l (k INT, v INT, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO o VALUES (1, 1), (2, 1)")
        .unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 10), (2, 20)")
        .unwrap();

    let tree = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE \
         SELECT /*+ no_hash_join(o,l) ordered */ COUNT(*) \
         FROM o INNER JOIN l ON l.k = o.id WHERE o.d = 1",
    );
    assert!(tree.contains("Index nested loop"), "{tree}");
    assert!(!tree.contains("batch: eligible"), "{tree}");
    assert!(!tree.contains("covering: true"), "{tree}");
    assert!(!tree.contains("parallel_frontier:"), "{tree}");
    assert!(tree.contains("condition: l.k=o.id"), "{tree}");
}

fn assert_error_code(engine: &StorageQueryEngine, sql: &str, code: u16) {
    let err = engine.execute(sql).expect_err("expected query error");
    assert_eq!(err.code(), code, "unexpected error for {sql}: {err:?}");
}

#[test]
fn select_and_explain_reject_unknown_projection_columns_before_execution() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name VARCHAR(25))")
        .unwrap();
    engine
        .execute("INSERT INTO region VALUES (1, 'AFRICA'), (2, 'AMERICA')")
        .unwrap();

    assert_error_code(&engine, "SELECT abc FROM region", 1054);
    assert_error_code(&engine, "EXPLAIN SELECT abc FROM region", 1054);
    assert_error_code(&engine, "EXPLAIN FORMAT=TREE SELECT abc FROM region", 1054);
    assert_error_code(&engine, "EXPLAIN FORMAT=JSON SELECT abc FROM region", 1054);
    assert_error_code(&engine, "SELECT r_regionkey + abc FROM region", 1054);

    let QueryResponse::ResultSet(_) = engine
        .execute("SELECT r_regionkey AS k FROM region ORDER BY k")
        .unwrap()
    else {
        panic!("expected result set");
    };
}

#[test]
fn select_rejects_ambiguous_projection_columns_before_execution() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();

    assert_error_code(&engine, "SELECT id FROM a JOIN b ON a.id = b.id", 1052);
}

#[test]
fn explain_sysbench_point_select_uses_primary_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE sbtest1 (id INT PRIMARY KEY, k INT, c CHAR(120), pad CHAR(60))")
        .unwrap();
    engine
        .execute("INSERT INTO sbtest1 VALUES (1, 10, 'c', 'pad'), (2, 20, 'c', 'pad')")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT c FROM sbtest1 WHERE id = 1")
        .unwrap()
    else {
        panic!("expected explain result set");
    };

    assert_eq!(explain.rows[0][4], Value::String("const".into()));
    assert_eq!(explain.rows[0][6], Value::String("PRIMARY".into()));
    assert_eq!(explain.rows[0][9], Value::Int(1));
}

#[test]
fn explain_sysbench_between_select_uses_primary_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE sbtest1 (id INT PRIMARY KEY, k INT, c CHAR(120), pad CHAR(60))")
        .unwrap();
    engine
        .execute("INSERT INTO sbtest1 VALUES (1, 10, 'c', 'pad'), (2, 20, 'c', 'pad')")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT c FROM sbtest1 WHERE id BETWEEN 1 AND 2")
        .unwrap()
    else {
        panic!("expected explain result set");
    };

    assert_eq!(explain.rows[0][4], Value::String("range".into()));
    assert_eq!(explain.rows[0][6], Value::String("PRIMARY".into()));
}

#[test]
fn analyze_table_populates_cardinality_and_result_status() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 10), (3, 20)")
        .unwrap();

    let QueryResponse::ResultSet(analyze) = engine.execute("ANALYZE TABLE t").unwrap() else {
        panic!("expected analyze result set");
    };
    assert_eq!(analyze.rows[0][1], Value::String("analyze".into()));
    assert_eq!(analyze.rows[0][3], Value::String("OK".into()));

    let QueryResponse::ResultSet(show_index) = engine.execute("SHOW INDEX FROM t").unwrap() else {
        panic!("expected show index result set");
    };
    assert!(show_index
        .rows
        .iter()
        .any(|row| { row[2] == Value::String("v_idx".into()) && row[6] == Value::Int(2) }));

    let QueryResponse::ResultSet(info_schema) = engine
        .execute(
            "SELECT INDEX_NAME, CARDINALITY FROM information_schema.statistics \
             WHERE TABLE_NAME = 't' AND INDEX_NAME = 'v_idx'",
        )
        .unwrap()
    else {
        panic!("expected information_schema result set");
    };
    assert_eq!(
        info_schema.rows,
        vec![vec![Value::String("v_idx".into()), Value::Int(2)]]
    );
}

#[test]
fn range_index_cost_uses_its_own_range_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE li (\
             id INT PRIMARY KEY, \
             a INT, \
             b INT, \
             d DATE, \
             q INT, \
             INDEX idx_ab (a, b), \
             INDEX idx_d (d))",
        )
        .unwrap();

    for chunk in 0..2 {
        let mut values = Vec::new();
        for i in (chunk * 100)..((chunk + 1) * 100) {
            let a = i % 100;
            let b = a;
            let year = 1992 + (i % 6);
            let day = 1 + (i % 28);
            values.push(format!("({i}, {a}, {b}, '{year}-01-{day:02}', 10)"));
        }
        engine
            .execute(&format!("INSERT INTO li VALUES {}", values.join(", ")))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE li").unwrap();

    let QueryResponse::ResultSet(tree) = engine
        .execute(
            "EXPLAIN FORMAT=TREE \
             SELECT 0.5 * SUM(q) \
             FROM li \
             WHERE a = 5 \
               AND b = 5 \
               AND d >= '1994-01-01' \
               AND d < DATE '1994-01-01' + INTERVAL '1' YEAR",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let tree_text = match &tree.rows[0][0] {
        Value::String(s) => s,
        other => panic!("expected tree text, got {other:?}"),
    };
    assert!(
        tree_text.contains("idx_ab"),
        "composite equality index should beat unrelated date range index: {tree_text}"
    );
}

#[test]
fn planner_uses_statistics_to_choose_more_selective_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, INDEX a_idx (a), INDEX b_idx (b))",
        )
        .unwrap();
    let values = (1..=80)
        .map(|id| format!("({id}, 1, {id})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT id FROM t WHERE a = 1 AND b = 42")
        .unwrap()
    else {
        panic!("expected explain result set");
    };

    assert_eq!(explain.rows[0][6], Value::String("b_idx".into()));
    assert_eq!(explain.rows[0][9], Value::Int(1));
}

#[test]
fn stale_stats_refresh_before_explain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    engine.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    let QueryResponse::ResultSet(explain) = engine.execute("EXPLAIN SELECT * FROM t").unwrap()
    else {
        panic!("expected explain result set");
    };

    assert_eq!(explain.rows[0][9], Value::Int(2));
}

#[test]
fn explain_const_access_type_for_unique_key_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.rows[0][4], Value::String("const".into()));
    assert_eq!(explain.rows[0][9], Value::Int(1));
    assert_eq!(explain.rows[0][10], Value::Double(100.0));
}

#[test]
fn explain_ref_access_type_for_non_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 10), (3, 20)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE v = 10")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.rows[0][4], Value::String("ref".into()));
    assert_eq!(explain.rows[0][6], Value::String("v_idx".into()));
}

#[test]
fn explain_key_len_for_int_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.rows[0][7], Value::String("4".into()));
}

#[test]
fn explain_key_len_for_varchar_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, INDEX name_idx (name))",
        )
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE name = 'alice'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.rows[0][6], Value::String("name_idx".into()));
    assert_eq!(explain.rows[0][7], Value::String("202".into()));
}

#[test]
fn invisible_index_is_reported_but_not_planned_until_visible() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("CREATE INDEX v_idx ON t (v) INVISIBLE")
        .unwrap();

    let QueryResponse::ResultSet(show_index) = engine.execute("SHOW INDEX FROM t").unwrap() else {
        panic!("expected show index result set");
    };
    let invisible_row = show_index
        .rows
        .iter()
        .find(|row| row[2] == Value::String("v_idx".into()))
        .expect("v_idx should be visible in SHOW INDEX");
    assert_eq!(invisible_row[13], Value::String("NO".into()));

    let QueryResponse::ResultSet(hidden_explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE v = 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_ne!(hidden_explain.rows[0][6], Value::String("v_idx".into()));

    engine
        .execute("ALTER TABLE t ALTER INDEX v_idx VISIBLE")
        .unwrap();
    let QueryResponse::ResultSet(visible_explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE v = 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(visible_explain.rows[0][6], Value::String("v_idx".into()));
}

#[test]
fn prefix_index_metadata_key_len_and_residual_filtering() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'abcd'), (2, 'abxy'), (3, 'zzzz')")
        .unwrap();
    engine
        .execute("CREATE INDEX name_pref ON t (name(2) DESC) COMMENT 'two chars'")
        .unwrap();

    let QueryResponse::ResultSet(show_index) = engine.execute("SHOW INDEX FROM t").unwrap() else {
        panic!("expected show index result set");
    };
    let prefix_row = show_index
        .rows
        .iter()
        .find(|row| row[2] == Value::String("name_pref".into()))
        .expect("name_pref should be listed");
    assert_eq!(prefix_row[5], Value::String("D".into()));
    assert_eq!(prefix_row[7], Value::Int(2));
    assert_eq!(prefix_row[10], Value::String("BTREE".into()));
    assert_eq!(prefix_row[13], Value::String("YES".into()));

    let QueryResponse::ResultSet(show_create) = engine.execute("SHOW CREATE TABLE t").unwrap()
    else {
        panic!("expected show create result set");
    };
    let Value::String(sql) = &show_create.rows[0][1] else {
        panic!("expected create sql");
    };
    assert!(sql.contains("KEY `name_pref` (`name`(2) DESC) COMMENT 'two chars'"));

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT id FROM t WHERE name = 'abcd'")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.rows[0][6], Value::String("name_pref".into()));
    assert_eq!(explain.rows[0][7], Value::String("10".into()));

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE name = 'abcd' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn like_prefix_and_is_null_predicates_use_btree_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20), INDEX name_idx (name))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'alice'), (2, 'alina'), (3, 'bob'), (4, NULL)")
        .unwrap();

    let QueryResponse::ResultSet(like_explain) = engine
        .execute("EXPLAIN SELECT id FROM t WHERE name LIKE 'ali%'")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(like_explain.rows[0][4], Value::String("range".into()));
    assert_eq!(like_explain.rows[0][6], Value::String("name_idx".into()));

    let QueryResponse::ResultSet(like_rows) = engine
        .execute("SELECT id FROM t WHERE name LIKE 'ali%' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        like_rows.rows,
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );

    let QueryResponse::ResultSet(null_explain) = engine
        .execute("EXPLAIN SELECT id FROM t WHERE name IS NULL")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(null_explain.rows[0][6], Value::String("name_idx".into()));

    let QueryResponse::ResultSet(null_rows) = engine
        .execute("SELECT id FROM t WHERE name IS NULL")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(null_rows.rows, vec![vec![Value::Int(4)]]);
}

#[test]
fn explain_ref_column_shows_const_for_literal_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.rows[0][8], Value::String("const".into()));
}

#[test]
fn explain_ref_column_null_for_range_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE id BETWEEN 1 AND 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.rows[0][4], Value::String("range".into()));
    assert_eq!(explain.rows[0][8], Value::Null);
}

#[test]
fn explain_date_range_estimates_majority_match_after_analyze() {
    // Regression: after the binary Value::Date variant was introduced,
    // `numeric_stat_value` fell into the `_ => 0.0` catch-all for
    // `Value::Date`, so the histogram min/max collapsed to 0.0 and
    // `estimate_range_rows_for_column` short-circuited at the
    // `(max_n - min_n).abs() < f64::EPSILON` guard with rows=1 — even
    // when the predicate matched essentially the whole table. Ensure
    // the estimate is now within a sane factor of the true count.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, d DATE, INDEX idx_d (d))")
        .unwrap();
    let values = (0..200)
        .map(|i| {
            let day = 1 + i % 28;
            let month = 1 + (i / 28) % 12;
            format!("({i}, '1998-{month:02}-{day:02}')")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE d <= '1998-12-31'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::Int(rows) = explain.rows[0][9] else {
        panic!("expected int rows estimate, got {:?}", explain.rows[0][9]);
    };
    assert!(
        rows >= 100,
        "predicate matches all 200 rows but estimate was only {rows}"
    );
}

#[test]
fn explain_filtered_percentage_for_seq_scan_with_filter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=100 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {v})", v = i % 10))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE v = 5")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.rows[0][4], Value::String("ALL".into()));
    if let Value::Double(filtered) = explain.rows[0][10] {
        assert!(
            filtered < 100.0,
            "filtered should be less than 100% with a filter, got {filtered}"
        );
        assert!(filtered > 0.0, "filtered should be positive");
    } else {
        panic!("expected double for filtered column");
    }
}

#[test]
fn explain_select_type_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE v IN (SELECT v FROM t WHERE id = 1)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.rows[0][1], Value::String("PRIMARY".into()),);
    // B1: a non-correlated `IN (subquery)` is materialised into a hash set —
    // MySQL 8.0 labels that block `MATERIALIZED`, not `SUBQUERY`.
    assert!(
        explain
            .rows
            .iter()
            .any(|row| row[1] == Value::String("MATERIALIZED".into())),
        "IN-subquery block should be MATERIALIZED: {:?}",
        explain.rows
    );
}

/// A4 (explain-mysql80-compat): `EXPLAIN TABLE t` (MySQL 8.0.19+) executes as
/// `EXPLAIN SELECT * FROM t` and yields a normal plan row for the table.
#[test]
fn explain_table_statement_produces_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let QueryResponse::ResultSet(explain) = engine.execute("EXPLAIN TABLE t").unwrap() else {
        panic!("expected result set");
    };
    // table column (index 2) names `t`, access type (index 4) present.
    assert!(
        explain
            .rows
            .iter()
            .any(|row| row[2] == Value::String("t".into())),
        "EXPLAIN TABLE t should scan t: {:?}",
        explain.rows
    );

    // FORMAT=TREE form runs too.
    let QueryResponse::ResultSet(tree) = engine.execute("EXPLAIN FORMAT=TREE TABLE t").unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(text) = &tree.rows[0][0] else {
        panic!("expected tree text");
    };
    assert!(
        text.to_lowercase().contains("scan") || text.contains(" t"),
        "EXPLAIN FORMAT=TREE TABLE t should show a scan: {text}"
    );
}

/// B1 (explain-mysql80-compat): a correlated subquery that is *not*
/// decorrelated is labelled `DEPENDENT SUBQUERY` (re-evaluated per outer row),
/// matching MySQL 8.0. Decorrelation (default-on) rewrites most correlated
/// subqueries into joins, so this exercises the un-decorrelated placeholder
/// path under `MYDB_DECORRELATE=off`, where the label logic lives.
#[test]
fn explain_select_type_dependent_subquery() {
    let prior = std::env::var("MYDB_DECORRELATE").ok();
    std::env::set_var("MYDB_DECORRELATE", "off");
    let _restore = restore_env("MYDB_DECORRELATE", prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE u (id INT PRIMARY KEY, w INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    engine.execute("INSERT INTO u VALUES (1, 10)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT t.id, (SELECT COUNT(*) FROM u WHERE u.w = t.v) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        explain
            .rows
            .iter()
            .any(|row| row[1] == Value::String("DEPENDENT SUBQUERY".into())),
        "correlated scalar subquery should be DEPENDENT SUBQUERY: {:?}",
        explain.rows
    );
}

/// B1: a subquery whose body is non-deterministic (`RAND()`) cannot be cached
/// and is labelled `UNCACHEABLE SUBQUERY` by MySQL 8.0. Uses an `IN` subquery
/// over a base table so a block row is emitted; runs with `MYDB_DECORRELATE=off`
/// so the non-deterministic body stays a materialised block instead of being
/// rewritten into a semijoin.
#[test]
fn explain_select_type_uncacheable_subquery() {
    let prior = std::env::var("MYDB_DECORRELATE").ok();
    std::env::set_var("MYDB_DECORRELATE", "off");
    let _restore = restore_env("MYDB_DECORRELATE", prior);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE u (id INT PRIMARY KEY, w INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    engine.execute("INSERT INTO u VALUES (1, 10)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE v IN (SELECT v * RAND() FROM t WHERE id = 1)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        explain
            .rows
            .iter()
            .any(|row| row[1] == Value::String("UNCACHEABLE SUBQUERY".into())),
        "RAND() IN-subquery should be UNCACHEABLE SUBQUERY: {:?}",
        explain.rows
    );
}

#[test]
fn explain_format_tree_produces_single_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT * FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.columns.len(), 1);
    assert_eq!(explain.columns[0].name, "EXPLAIN");
    assert_eq!(explain.rows.len(), 1);
    if let Value::String(text) = &explain.rows[0][0] {
        assert!(
            text.contains("Constant lookup"),
            "tree output should mention lookup: {text}"
        );
    } else {
        panic!("expected string for EXPLAIN TREE output");
    }
}

#[test]
fn set_session_explain_format_changes_default_output() {
    use crate::engine::api::SessionContext;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let mut session = SessionContext::default();
    assert_eq!(
        session.explain_format,
        crate::parser::ExplainFormat::Traditional
    );

    // Default (Traditional): EXPLAIN with no FORMAT= returns multi-column
    // table rows.
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "EXPLAIN SELECT * FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        rs.columns.len() > 1,
        "traditional EXPLAIN must have multiple columns"
    );

    // Flip session default to TREE — bare EXPLAIN now yields the single
    // EXPLAIN column tree output.
    engine
        .execute_with_session(&mut session, "SET SESSION explain_format = 'TREE'")
        .unwrap();
    assert_eq!(session.explain_format, crate::parser::ExplainFormat::Tree);

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "EXPLAIN SELECT * FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 1);
    assert_eq!(rs.columns[0].name, "EXPLAIN");

    // Explicit FORMAT=JSON still wins over the session default (TREE).
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(
            &mut session,
            "EXPLAIN FORMAT=JSON SELECT * FROM t WHERE id = 1",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 1);
    if let Value::String(text) = &rs.rows[0][0] {
        assert!(text.contains("query_block"));
    } else {
        panic!("expected JSON string");
    }

    // SELECT @@session.explain_format reflects the current value.
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "SELECT @@session.explain_format")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::String("TREE".into()));
}

#[test]
fn set_global_explain_format_changes_only_new_session_default_output() {
    use crate::engine::api::{set_global_explain_format, SessionContext};
    use crate::parser::ExplainFormat;

    struct RestoreGlobalExplainFormat(ExplainFormat);
    impl Drop for RestoreGlobalExplainFormat {
        fn drop(&mut self) {
            set_global_explain_format(self.0);
        }
    }

    let restore = RestoreGlobalExplainFormat(crate::engine::api::global_explain_format());
    set_global_explain_format(ExplainFormat::Traditional);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET GLOBAL explain_format = 'TREE'")
        .unwrap();
    assert_eq!(session.explain_format, ExplainFormat::Traditional);
    assert_eq!(
        crate::engine::api::global_explain_format(),
        ExplainFormat::Tree
    );

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(
            &mut session,
            "SELECT @@global.explain_format, @@session.explain_format",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0],
        vec![
            Value::String("TREE".into()),
            Value::String("TRADITIONAL".into())
        ]
    );

    // Only sessions created after SET GLOBAL inherit the new default.
    let mut new_session = SessionContext::default();
    assert_eq!(new_session.explain_format, ExplainFormat::Tree);

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut session, "EXPLAIN SELECT * FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };

    assert!(
        rs.columns.len() > 1,
        "SET GLOBAL must preserve the existing session's traditional EXPLAIN"
    );
    drop(restore);

    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut new_session, "EXPLAIN SELECT * FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 1);
    assert_eq!(rs.columns[0].name, "EXPLAIN");
    if let Value::String(text) = &rs.rows[0][0] {
        assert!(
            text.contains("Constant lookup"),
            "tree output should mention lookup: {text}"
        );
    } else {
        panic!("expected string for EXPLAIN TREE output");
    }
}

#[test]
fn explain_format_json_produces_valid_structure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=JSON SELECT * FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.columns.len(), 1);
    assert_eq!(explain.columns[0].name, "EXPLAIN");
    if let Value::String(text) = &explain.rows[0][0] {
        assert!(
            text.contains("query_block"),
            "JSON should contain query_block: {text}"
        );
        assert!(
            text.contains("\"access_type\""),
            "JSON should contain access_type: {text}"
        );
        // D1: query_block carries a whole-query `query_cost`, and each table
        // node's `cost_info` decomposes into read/eval/prefix + data_read.
        assert!(
            text.contains("\"query_cost\""),
            "JSON query_block should carry query_cost: {text}"
        );
        for field in [
            "\"read_cost\"",
            "\"eval_cost\"",
            "\"prefix_cost\"",
            "\"data_read_per_join\"",
            "\"mydb_local_total_cost\"",
            "\"mydb_cumulative_cpu_units\"",
            "\"mydb_estimate_source\"",
            "\"mydb_estimate_confidence\"",
            "\"mydb_parallel_degree\"",
            "\"mydb_cbo_model\"",
        ] {
            assert!(
                text.contains(field),
                "JSON table cost_info should contain {field}: {text}"
            );
        }
        // The emitted document must still be valid JSON after the expansion.
        assert!(
            serde_json::from_str::<serde_json::Value>(text).is_ok(),
            "EXPLAIN FORMAT=JSON must be valid JSON: {text}"
        );
    } else {
        panic!("expected string for EXPLAIN JSON output");
    }
}

#[test]
fn explain_tree_surfaces_join_keys_residuals_and_scan_filters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10, 5), (2, 20, 9), (3, 30, 1)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (11, 10, 3), (12, 20, 10), (13, 40, 7)")
        .unwrap();
    engine.execute("ANALYZE TABLE a").unwrap();
    engine.execute("ANALYZE TABLE b").unwrap();

    let text = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE \
         SELECT /*+ HASH_JOIN(a, b) */ a.id \
         FROM a JOIN b ON a.k = b.k AND a.v > b.v \
         WHERE a.v > 0",
    );
    assert!(text.contains("Hash join"), "expected hash join: {text}");
    assert!(
        text.contains("keys: a.k=b.k") || text.contains("keys: b.k=a.k"),
        "expected join keys in either physical build direction: {text}"
    );
    assert!(
        text.contains("residual: a.v>b.v"),
        "expected residual: {text}"
    );
    assert!(
        text.contains("filter: a.v>0"),
        "expected scan filter: {text}"
    );
}

#[test]
fn explain_tree_surfaces_nested_loop_condition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (11, 15), (12, 25)")
        .unwrap();
    engine.execute("ANALYZE TABLE a").unwrap();
    engine.execute("ANALYZE TABLE b").unwrap();

    let text = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE \
         SELECT /*+ NL_JOIN(a, b) */ a.id \
         FROM a JOIN b ON a.k < b.k",
    );
    assert!(
        text.contains("Nested loop join") && text.contains("condition: a.k<b.k"),
        "expected nested-loop condition: {text}"
    );
}

#[test]
fn explain_json_surfaces_parseable_join_and_filter_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10, 5), (2, 20, 9)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (11, 10, 3), (12, 20, 10)")
        .unwrap();
    engine.execute("ANALYZE TABLE a").unwrap();
    engine.execute("ANALYZE TABLE b").unwrap();

    let hash_text = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=JSON \
         SELECT /*+ HASH_JOIN(a, b) */ a.id \
         FROM a JOIN b ON a.k = b.k AND a.v > b.v \
         WHERE a.v > 0",
    );
    let hash_json: serde_json::Value =
        serde_json::from_str(&hash_text).unwrap_or_else(|e| panic!("{e}: {hash_text}"));
    let hash_join = find_json_key(&hash_json["query_block"], "hash_join")
        .unwrap_or_else(|| panic!("missing hash_join: {hash_text}"));
    assert!(
        hash_join["build_side"].is_string(),
        "missing build_side: {hash_text}"
    );
    assert_eq!(hash_join["join_keys"][0]["left"], "a.k");
    assert_eq!(hash_join["join_keys"][0]["right"], "b.k");
    assert_eq!(hash_join["attached_condition"], "a.v>b.v");
    assert!(
        hash_text.contains("\"attached_condition\": \"a.v>0\""),
        "expected scan attached_condition: {hash_text}"
    );

    let nested_text = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=JSON \
         SELECT /*+ NL_JOIN(a, b) */ a.id \
         FROM a JOIN b ON a.k < b.k",
    );
    let nested_json: serde_json::Value =
        serde_json::from_str(&nested_text).unwrap_or_else(|e| panic!("{e}: {nested_text}"));
    let nested_loop = find_json_key(&nested_json["query_block"], "nested_loop")
        .unwrap_or_else(|| panic!("missing nested_loop: {nested_text}"));
    assert_eq!(nested_loop["join_condition"], "a.k<b.k");
}

#[test]
fn explain_formatted_shows_index_in_lookup_subquery_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE inventory (\
             tenant_id INT, \
             item_id INT, \
             qty INT, \
             INDEX tenant_item_idx (tenant_id, item_id))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE recent_items (item_id INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO inventory VALUES \
             (1, 100, 8), (1, 101, 20), (1, 102, 3), (2, 100, 1)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO recent_items VALUES (100), (102)")
        .unwrap();

    let sql = "SELECT item_id FROM inventory \
               WHERE tenant_id = 1 AND qty < 10 \
                 AND item_id IN (SELECT item_id FROM recent_items)";

    let tree = explain_single_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    assert!(
        tree.contains("Index lookup on inventory using tenant_item_idx with IN subquery"),
        "expected IndexInLookup in TREE output: {tree}"
    );
    assert!(
        tree.contains("Table scan on recent_items"),
        "TREE output should show the IN-subquery plan: {tree}"
    );

    let json_text = explain_single_text(&engine, &format!("EXPLAIN FORMAT=JSON {sql}"));
    let json: serde_json::Value =
        serde_json::from_str(&json_text).unwrap_or_else(|e| panic!("{e}: {json_text}"));
    assert!(find_json_table(&json["query_block"], "inventory").is_some());
    assert!(
        find_json_table(&json["query_block"], "recent_items").is_some(),
        "JSON output should show the IN-subquery plan: {json_text}"
    );
}

#[test]
fn traditional_explain_extra_surfaces_predicates_without_column_shape_change() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10, 5), (2, 20, 9)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (11, 10, 3), (12, 20, 10)")
        .unwrap();
    engine.execute("ANALYZE TABLE a").unwrap();
    engine.execute("ANALYZE TABLE b").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT /*+ HASH_JOIN(a, b) */ a.id \
             FROM a JOIN b ON a.k = b.k AND a.v > b.v \
             WHERE a.v > 0",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.columns.len(), 12);
    assert!(
        explain.rows.iter().any(
            |row| matches!(&row[11], Value::String(extra) if extra.contains("Using where (a.v>0)"))
        ),
        "expected filter text in Extra: {:?}",
        explain.rows
    );
    assert!(
        explain
            .rows
            .iter()
            .any(|row| matches!(&row[11], Value::String(extra)
                if extra.contains("Hash join")
                    && extra.contains("keys=[a.k=b.k]")
                    && extra.contains("residual=a.v>b.v"))),
        "expected hash join diagnostics in Extra: {:?}",
        explain.rows
    );
}

#[test]
fn explain_eq_ref_for_join_on_primary_key() {
    // Phase 3 cost model: INL must beat hash join when the inner-side
    // PK lookup avoids reading a substantially larger probe relation.
    // Use 5000 customers vs 3 orders so the new IO-aware cost formulas
    // see HJ probe = full customers scan (large) clearly losing to
    // INL = 3 × PK lookup (tiny). On the prior naive `rows`-based
    // cost both shapes scored similarly and INL won by tie-break.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))")
        .unwrap();
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, INDEX cust_idx (customer_id))")
        .unwrap();
    // Bulk insert in batches to keep statement size manageable.
    for chunk in (1..=5000).collect::<Vec<_>>().chunks(500) {
        let values = chunk
            .iter()
            .map(|i| format!("({i}, 'Customer{i}')"))
            .collect::<Vec<_>>()
            .join(", ");
        engine
            .execute(&format!("INSERT INTO customers VALUES {values}"))
            .unwrap();
    }
    engine
        .execute("INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)")
        .unwrap();
    engine.execute("ANALYZE TABLE customers").unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let customer_row = explain
        .rows
        .iter()
        .find(|row| row[2] == Value::String("customers".into()))
        .expect("should have customers row");
    assert_eq!(customer_row[4], Value::String("eq_ref".into()));
}

#[test]
fn where_join_predicate_on_cross_join_participates_in_join_planning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (10, 10), (20, 20), (40, 40)")
        .unwrap();
    engine.execute("ANALYZE TABLE a").unwrap();
    engine.execute("ANALYZE TABLE b").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT a.id, b.id FROM a CROSS JOIN b WHERE a.k = b.k")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let text = explain
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|value| match value {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        text.contains("Hash join") || text.contains("Index nested loop"),
        "WHERE join predicate should drive a join algorithm, got: {text}"
    );
}

#[test]
fn where_join_predicate_with_trivial_on_participates_in_join_planning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, INDEX k_idx (k))")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (10, 10), (20, 20), (40, 40)")
        .unwrap();
    engine.execute("ANALYZE TABLE a").unwrap();
    engine.execute("ANALYZE TABLE b").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT a.id, b.id FROM a JOIN b ON 1 = 1 WHERE a.k = b.k")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let text = explain
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|value| match value {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        text.contains("Hash join") || text.contains("Index nested loop"),
        "WHERE join predicate should not remain only as a top filter, got: {text}"
    );
}

#[test]
fn semi_join_keeps_outer_where_predicate_on_base_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_orderdate DATE NOT NULL, \
             o_orderpriority CHAR(15) NOT NULL, \
             INDEX idx_orders_orderdate (o_orderdate))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT NOT NULL, \
             l_commitdate DATE NOT NULL, \
             l_receiptdate DATE NOT NULL, \
             INDEX idx_lineitem_orderkey (l_orderkey))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders VALUES \
             (1, '1993-06-30', '1-URGENT'), \
             (2, '1993-07-01', '1-URGENT'), \
             (3, '1993-08-15', '2-HIGH'), \
             (4, '1993-09-30', '3-MEDIUM'), \
             (5, '1993-10-01', '4-NOT SPECIFIED')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (2, '1993-07-05', '1993-07-06'), \
             (3, '1993-08-20', '1993-08-19'), \
             (4, '1993-10-01', '1993-10-02')",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let query = "SELECT o_orderpriority, COUNT(*) AS order_count \
             FROM orders \
             WHERE o_orderdate >= '1993-07-01' \
               AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH \
               AND EXISTS (\
                   SELECT * FROM lineitem \
                   WHERE l_orderkey = o_orderkey \
                     AND l_commitdate < l_receiptdate) \
             GROUP BY o_orderpriority \
             ORDER BY o_orderpriority";
    let QueryResponse::ResultSet(explain) = engine.execute(&format!("EXPLAIN {query}")).unwrap()
    else {
        panic!("expected result set");
    };

    let orders_row = explain
        .rows
        .iter()
        .find(|row| row[2] == Value::String("orders".into()))
        .expect("orders should appear in EXPLAIN");
    assert_eq!(orders_row[4], Value::String("range".into()));
    assert_eq!(orders_row[6], Value::String("idx_orders_orderdate".into()));

    assert!(
        explain
            .rows
            .iter()
            .all(|row| row[1] != Value::String("SUBQUERY".into())),
        "decorrelated EXISTS should not also be reported as a SUBQUERY: {:?}",
        explain.rows
    );
    assert!(
        explain.rows.iter().any(
            |row| matches!(&row[11], Value::String(extra) if extra.contains("Using semi join"))
        ),
        "semi join should be surfaced in EXPLAIN extra: {:?}",
        explain.rows
    );

    let QueryResponse::ResultSet(tree) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {query}"))
        .unwrap()
    else {
        panic!("expected result set");
    };
    let tree_text = match &tree.rows[0][0] {
        Value::String(s) => s,
        other => panic!("expected tree text, got {other:?}"),
    };
    assert!(
        tree_text.contains("semi join"),
        "tree EXPLAIN should label semi joins explicitly: {tree_text}"
    );
}

#[test]
fn semi_join_allows_leading_inner_join_island_reorder() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name CHAR(25))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE supplier (\
             s_suppkey INT PRIMARY KEY, \
             s_name CHAR(25), \
             s_nationkey INT NOT NULL, \
             INDEX idx_supplier_nation (s_nationkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_orderstatus CHAR(1) NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT NOT NULL, \
             l_suppkey INT NOT NULL, \
             l_linenumber INT NOT NULL, \
             l_receiptdate DATE NOT NULL, \
             l_commitdate DATE NOT NULL, \
             PRIMARY KEY (l_orderkey, l_linenumber), \
             INDEX idx_lineitem_orderkey (l_orderkey), \
             INDEX idx_lineitem_suppkey (l_suppkey))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO nation VALUES (1, 'SAUDI ARABIA'), (2, 'BRAZIL')")
        .unwrap();
    let suppliers = (1..=60)
        .map(|i| {
            let nation = if i <= 3 { 1 } else { 2 };
            format!("({i}, 'S{i}', {nation})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO supplier VALUES {suppliers}"))
        .unwrap();
    let orders = (1..=240)
        .map(|i| format!("({i}, 'F')"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO orders VALUES {orders}"))
        .unwrap();
    let lineitems = (1..=240)
        .flat_map(|order| {
            let supp = (order % 60) + 1;
            [
                format!("({order}, {supp}, 1, '1994-01-05', '1994-01-03')"),
                format!(
                    "({order}, {}, 2, '1994-01-04', '1994-01-03')",
                    (supp % 60) + 1
                ),
            ]
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO lineitem VALUES {lineitems}"))
        .unwrap();
    engine.execute("ANALYZE TABLE nation").unwrap();
    engine.execute("ANALYZE TABLE supplier").unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let query = "SELECT s_name, COUNT(*) AS numwait \
                 FROM supplier \
                 INNER JOIN lineitem l1 ON s_suppkey = l1.l_suppkey \
                 INNER JOIN orders ON o_orderkey = l1.l_orderkey \
                 INNER JOIN nation ON s_nationkey = n_nationkey \
                 WHERE o_orderstatus = 'F' \
                   AND l1.l_receiptdate > l1.l_commitdate \
                   AND EXISTS (\
                       SELECT * FROM lineitem l2 \
                       WHERE l2.l_orderkey = l1.l_orderkey \
                         AND l2.l_suppkey <> l1.l_suppkey) \
                   AND n_name = 'SAUDI ARABIA' \
                 GROUP BY s_name";
    let QueryResponse::ResultSet(tree) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {query}"))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let tree_text = match &tree.rows[0][0] {
        Value::String(s) => s,
        other => panic!("expected tree text, got {other:?}"),
    };
    assert!(
        tree_text.contains("Table scan on nation")
            && tree_text.contains("Index lookup on supplier using idx_supplier_nation"),
        "leading inner island should start from filtered nation and reach supplier before the semi join: {tree_text}"
    );
    assert!(
        tree_text.contains("semi join"),
        "query shape must still decorrelate EXISTS to semi join: {tree_text}"
    );
}

#[test]
fn grouped_in_subquery_drives_index_lookup() {
    let _guard = super::test_env_lock();
    let prior_adaptive = std::env::var("MYDB_ADAPTIVE_JOIN").ok();
    std::env::remove_var("MYDB_ADAPTIVE_JOIN");
    let _restore_adaptive = restore_env("MYDB_ADAPTIVE_JOIN", prior_adaptive);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customer (c_custkey INT PRIMARY KEY, c_name VARCHAR(32))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_custkey INT NOT NULL, \
             o_orderdate DATE NOT NULL, \
             o_totalprice DECIMAL(15,2) NOT NULL, \
             INDEX idx_orders_custkey (o_custkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT NOT NULL, \
             l_linenumber INT NOT NULL, \
             l_quantity INT NOT NULL, \
             PRIMARY KEY (l_orderkey, l_linenumber), \
             INDEX idx_lineitem_orderkey (l_orderkey))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO customer VALUES (1, 'alice'), (2, 'bob'), (3, 'cara')")
        .unwrap();
    let orders = (1..=300)
        .map(|i| {
            let cust = (i % 3) + 1;
            let price = 1000 + i;
            format!("({i}, {cust}, '1995-01-01', {price})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO orders VALUES {orders}"))
        .unwrap();
    let lineitems = (1..=300)
        .flat_map(|order| {
            let qty = if order % 20 == 0 { 200 } else { 1 };
            [(order, 1, qty), (order, 2, qty)]
        })
        .map(|(order, line, qty)| format!("({order}, {line}, {qty})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO lineitem VALUES {lineitems}"))
        .unwrap();
    engine.execute("ANALYZE TABLE customer").unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let query = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, \
                 SUM(l_quantity) AS total_qty \
                 FROM customer \
                 INNER JOIN orders ON c_custkey = o_custkey \
                 INNER JOIN lineitem ON o_orderkey = l_orderkey \
                 WHERE o_orderkey IN (\
                     SELECT l_orderkey \
                     FROM lineitem \
                     GROUP BY l_orderkey \
                     HAVING SUM(l_quantity) > 300) \
                 GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice \
                 ORDER BY o_totalprice DESC, o_orderdate \
                 LIMIT 100";
    let QueryResponse::ResultSet(explain) = engine.execute(&format!("EXPLAIN {query}")).unwrap()
    else {
        panic!("expected explain result set");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        explain_text.contains("Using IN subquery lookup"),
        "grouped IN subquery should stay as a keyset-driven lookup: {explain_text}"
    );

    let QueryResponse::ResultSet(tree) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {query}"))
        .unwrap()
    else {
        panic!("expected result set");
    };
    let tree_text = match &tree.rows[0][0] {
        Value::String(s) => s,
        other => panic!("expected tree text, got {other:?}"),
    };
    assert!(
        tree_text.contains("Index lookup on orders using PRIMARY")
            && (tree_text.contains("Index lookup on lineitem using idx_lineitem_orderkey")
                || tree_text.contains("Index lookup on lineitem using PRIMARY")),
        "filtered orders from grouped IN subquery should drive primary-key orders lookups and lineitem probes: {tree_text}"
    );
    assert!(
        tree_text.contains("Streaming aggregate"),
        "grouping determined by the left row should stream instead of building a large hash table: {tree_text}"
    );

    let QueryResponse::ResultSet(rs) = engine.execute(query).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 15);
    assert_eq!(rs.rows[0][2], Value::Int(300));
}

#[test]
fn explain_marks_stale_stats_when_modifications_exceed_threshold() {
    // Phase 6 diagnostic: a scan row's `extra` column must announce
    // "stats: stale" once row modifications exceed 20% of the
    // analysed row count, but the auto-refresh threshold hasn't
    // tripped (which the engine uses for tables ≥ 64 rows).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx(v))")
        .unwrap();
    // Bulk-insert 200 rows so row_count > the auto-refresh floor of 64.
    let values = (1..=200)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    // Modify ~25% of rows (50/200) — above the 20% stale threshold
    // but below the auto-refresh threshold (max(row_count/5, 64) = 64
    // for 200 rows; 50 < 64).
    let extras = (201..=250)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {extras}"))
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE v = 50")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let extra = match &explain.rows[0][11] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected extra: {:?}", other),
    };
    assert!(
        extra.contains("stats: stale"),
        "expected stats hint in extra, got: {extra}"
    );

    // ANALYZE clears the tag.
    engine.execute("ANALYZE TABLE t").unwrap();
    let QueryResponse::ResultSet(explain2) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE v = 50")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let extra2 = match &explain2.rows[0][11] {
        Value::String(s) => s.clone(),
        // Exact index predicates no longer leave a redundant Using where.
        Value::Null => String::new(),
        other => panic!("unexpected extra: {:?}", other),
    };
    assert!(
        !extra2.contains("stats: stale") && !extra2.contains("stats: missing"),
        "expected fresh stats after ANALYZE, got: {extra2}"
    );
}

#[test]
fn stale_stats_scale_selectivity_against_live_row_count() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx(v))")
        .unwrap();
    let values = (1..=200)
        .map(|i| format!("({i}, 1)"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let extras = (201..=250)
        .map(|i| format!("({i}, 1)"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {extras}"))
        .unwrap();

    let rows = explain_rows_estimate(&engine, "EXPLAIN SELECT * FROM t WHERE v = 1");
    assert!(
        rows >= 240,
        "stale stats should scale hot-value frequency toward live rows, got {rows}"
    );
}

#[test]
fn group_by_without_column_stats_defaults_to_one_percent_capped_at_100() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t \
             SELECT n, MOD(n, 500), n FROM generate_series(1, 10100) AS gs(n)",
        )
        .unwrap();

    let text = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT g + 0, COUNT(*) FROM t GROUP BY g + 0",
    );
    assert!(
        text.contains("HashAggregate") && text.contains("rows=100"),
        "GROUP BY that cannot be estimated from column stats should use min(100, input rows * 1%) fallback, got:\n{text}"
    );
}

#[test]
fn group_by_uses_column_distinct_count_after_analyze() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    let values = (1..=240)
        .map(|i| format!("({i}, {}, {i})", i % 12))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let text = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT g, COUNT(*) FROM t GROUP BY g",
    );
    assert!(
        text.contains("HashAggregate") && text.contains("rows=12"),
        "GROUP BY should use analyzed column NDV when available, got:\n{text}"
    );
}

#[test]
fn low_ndv_secondary_index_lookup_uses_column_ndv_not_sampled_index_overestimate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE date_dim (\
             d_date_sk INT PRIMARY KEY, \
             d_year INT NOT NULL, \
             INDEX idx_date_dim_year(d_year))",
        )
        .unwrap();
    let rows = (1..=3650)
        .map(|day| {
            let year = 2000 + ((day - 1) / 365);
            format!("({day},{year})")
        })
        .collect::<Vec<_>>()
        .chunks(500)
        .map(|chunk| chunk.join(", "))
        .collect::<Vec<_>>();
    for chunk in rows {
        engine
            .execute(&format!("INSERT INTO date_dim VALUES {chunk}"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE date_dim").unwrap();

    let rows = explain_rows_estimate(
        &engine,
        "EXPLAIN SELECT * FROM date_dim WHERE d_year = 2002",
    );
    assert!(
        (300..=430).contains(&rows),
        "year lookup should estimate about one year's rows, got {rows}"
    );
}

#[test]
fn dpsub_handles_four_table_join_without_cartesian_product() {
    // Phase 4 regression: the bushy DPsub must enumerate every
    // connected bipartition of a 4-relation graph and return a join
    // tree without a Cartesian-product fallback. Snake-shape join
    // graph (a-b-c-d) — disconnected subsets like {a, c} must be
    // skipped during enumeration but the final plan must still cover
    // all four relations and produce correct results.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, b_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, c_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE c (id INT PRIMARY KEY, d_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY, name VARCHAR(8))")
        .unwrap();
    engine
        .execute("INSERT INTO d VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    engine
        .execute("INSERT INTO c VALUES (1, 1), (2, 2)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (1, 1), (2, 2)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 1), (2, 2)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT a.id FROM a \
             JOIN b ON a.b_id = b.id \
             JOIN c ON b.c_id = c.id \
             JOIN d ON c.d_id = d.id \
             ORDER BY a.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int(v) => *v,
            other => panic!("unexpected value {:?}", other),
        })
        .collect();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn order_preserving_index_chosen_when_limit_caps_scan() {
    // Tier 0 #2 regression: when ORDER BY can be served by an index
    // and a small LIMIT bounds the scan, the index that preserves
    // ordering must win over a competitor with similar selectivity.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (\
             id INT PRIMARY KEY, a INT, b INT, \
             INDEX a_idx (a), INDEX b_idx (b))",
        )
        .unwrap();
    // a is constant (every row matches WHERE a = 1) so a_idx covers
    // all 80 rows; b is unique. Without the order-preserving bonus
    // a_idx wins on raw selectivity ties; with the bonus, b_idx wins
    // because LIMIT 1 + ORDER BY b can stop after a single row.
    let values = (1..=80)
        .map(|i| format!("({i}, 1, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT id FROM t WHERE a = 1 AND b > 0 ORDER BY b LIMIT 1")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.rows[0][6], Value::String("b_idx".into()));
}

#[test]
fn large_ordered_limit_with_uncovered_residual_costs_correlation_risk() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE queue_t (\
             id INT PRIMARY KEY, ordered_key INT NOT NULL, kind VARCHAR(8) NOT NULL,\
             INDEX ordered_idx(ordered_key))",
        )
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.queue_t",
        (0..12_000i64).map(|id| {
            let ordered_key = (id * 7_919) % 12_000;
            let kind = if ordered_key < 6_000 { "drop" } else { "keep" };
            vec![
                Value::Int(id),
                Value::Int(ordered_key),
                Value::String(kind.into()),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE queue_t").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT id FROM queue_t \
             WHERE kind='keep' ORDER BY ordered_key LIMIT 1",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(
        explain.rows[0][6],
        Value::Null,
        "an unrelated residual on a large ordered scan should let seq+TopK win: {:?}",
        explain.rows
    );

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT id FROM queue_t \
             WHERE kind='keep' ORDER BY ordered_key LIMIT 100",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(
        explain.rows[0][6],
        Value::Null,
        "large row goals must reject an ordered scan that risks touching >1% of the table"
    );
}

#[test]
fn skewed_range_estimate_uses_top_values_frequencies() {
    // Tier 1 #7-lite regression: when a range overlaps a hot value
    // recorded in `top_values`, the estimator must reflect the
    // observed frequency (not just min/max linear interpolation).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v))")
        .unwrap();
    // Heavy skew on v=0: 100 rows; v=1..=10: 1 row each. Linear
    // interpolation over [0, 10] would estimate ~10% of rows in
    // the range [0,0]; with top_values awareness the estimator
    // should get close to 100/110.
    let mut values = Vec::new();
    let mut id = 0;
    for _ in 0..100 {
        id += 1;
        values.push(format!("({id}, 0)"));
    }
    for v in 1..=10 {
        id += 1;
        values.push(format!("({id}, {v})"));
    }
    engine
        .execute(&format!("INSERT INTO t VALUES {}", values.join(", ")))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let mut session = crate::engine::api::SessionContext::default();
    session.explain_format = crate::parser::ExplainFormat::Traditional;
    let QueryResponse::ResultSet(explain) = engine
        .execute_with_session(
            &mut session,
            "EXPLAIN SELECT id FROM t WHERE v >= 0 AND v <= 0",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let rows_estimate = match &explain.rows[0][9] {
        Value::Int(v) => *v,
        other => panic!("unexpected rows column: {other:?}"),
    };
    // Without top_values awareness, the linear interpolation would
    // have given roughly (1/(10-0)) * 110 ~= 11 rows. The accurate
    // answer is 100. We don't pin an exact number (it depends on
    // ANALYZE sampling and inclusivity rounding) — just require
    // that the estimate is meaningfully closer to 100 than 11.
    assert!(
        rows_estimate >= 50,
        "skewed range estimate should reflect top_values; got {rows_estimate}"
    );
}

#[test]
fn left_deep_dp_picks_cheapest_inner_join_order() {
    // Tier 0 #3 regression: for a 3-table inner join where two of
    // the joins reduce cardinality drastically and the third would
    // cross-product, the DP-driven left-deep order must avoid the
    // cartesian — a cost-blind greedy reorder by single-table size
    // can otherwise put the disconnected pair adjacent.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, b_id INT, INDEX b_idx (b_id))")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, c_id INT, INDEX c_idx (c_id))")
        .unwrap();
    engine
        .execute("CREATE TABLE c (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let a_values = (1..=20)
        .map(|i| format!("({i}, {})", ((i - 1) % 10) + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let b_values = (1..=10)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    let c_values = (1..=5)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO a VALUES {a_values}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO b VALUES {b_values}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO c VALUES {c_values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE a").unwrap();
    engine.execute("ANALYZE TABLE b").unwrap();
    engine.execute("ANALYZE TABLE c").unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT a.id, b.id, c.id FROM a \
             INNER JOIN b ON a.b_id = b.id \
             INNER JOIN c ON b.c_id = c.id \
             ORDER BY a.id, b.id, c.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Sanity: only the rows where the chain a->b->c resolves
    // should come back. b_id 1..=5 cross with c_id 1..=5 produces
    // 10 a-rows (those with b_id in 1..=5).
    assert!(!rs.rows.is_empty());

    // EXPLAIN: ensure no plan row uses access_type = "ALL" on a
    // for this shape; with the DP-driven order, a should access
    // through its index when the chain is followed correctly.
    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT a.id, b.id, c.id FROM a \
             INNER JOIN b ON a.b_id = b.id \
             INNER JOIN c ON b.c_id = c.id",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    // At least one of the three tables must be reached via an
    // indexed access. (Rationale: a 3-way cartesian would not
    // select any index on b or c.)
    let any_index = explain
        .rows
        .iter()
        .any(|row| !matches!(&row[5], Value::String(s) if s.is_empty()));
    assert!(any_index, "DP must pick an order that uses indexes");
}

#[test]
fn equivalence_propagation_reaches_other_table() {
    // Tier 0 #6 regression: when an INNER JOIN equates t.x = u.y and
    // WHERE pins u.y = 10, the planner must derive t.x = 10 so the
    // t-side scan can also use its index instead of falling back to
    // a sequential scan.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, x INT, INDEX x_idx (x))")
        .unwrap();
    engine
        .execute("CREATE TABLE u (id INT PRIMARY KEY, y INT, INDEX y_idx (y))")
        .unwrap();
    let t_values = (1..=50)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    let u_values = (1..=50)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {t_values}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO u VALUES {u_values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    engine.execute("ANALYZE TABLE u").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT t.id FROM t INNER JOIN u ON t.x = u.y WHERE u.y = 10")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let t_row = explain
        .rows
        .iter()
        .find(|row| row[2] == Value::String("t".into()))
        .expect("expected an explain row for table t");
    // Without equivalence propagation the t-side has no WHERE
    // predicate and falls back to "ALL" + no key. With propagation
    // it sees t.x = 10 and uses x_idx for a const/ref lookup.
    assert_eq!(t_row[6], Value::String("x_idx".into()));
}

#[test]
fn or_chain_on_indexed_column_estimates_each_value() {
    // Tier 0 #1 regression: an OR-chain of equalities on the same
    // column is rewritten to an IN-list so estimate_filter_rows
    // reflects per-value frequencies instead of independent OR.
    // We assert the rewrite by observing that EXPLAIN's predicted
    // `rows` follows the IN-list path (sum of distinct hits).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut values = String::new();
    for i in 1..=100 {
        if !values.is_empty() {
            values.push_str(", ");
        }
        // 10 buckets, 10 rows per value 1..=10
        values.push_str(&format!("({i}, {})", ((i - 1) % 10) + 1));
    }
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let QueryResponse::ResultSet(explain_or) = engine
        .execute("EXPLAIN SELECT id FROM t WHERE v = 1 OR v = 2 OR v = 3")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let QueryResponse::ResultSet(explain_in) = engine
        .execute("EXPLAIN SELECT id FROM t WHERE v IN (1, 2, 3)")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    // After the OR→IN rewrite, both queries should produce the same
    // row estimate.
    assert_eq!(explain_or.rows[0][8], explain_in.rows[0][8]);
}

#[test]
fn histogram_improves_uniform_range_estimate() {
    // Phase B #B6: an equi-depth histogram should be built when
    // `distinct_count` exceeds 2 × top-K. For a uniformly-distributed
    // column the histogram lets the planner attribute the right
    // share of rows to a sub-range that doesn't overlap any
    // top-values. Verify by confirming ANALYZE then EXPLAIN's `rows`
    // estimate for `BETWEEN K AND K+slice` tracks the actual count
    // closer than the linear-only fallback would.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX v_idx (v))")
        .unwrap();
    // 200 distinct values 1..=200 each with one row → no top_values
    // can fully cover them (top-K is 16). Range 50..=100 contains
    // exactly 51 rows.
    let values = (1..=200)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT id FROM t WHERE v BETWEEN 50 AND 100")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let rows_estimate = match &explain.rows[0][9] {
        Value::Int(v) => *v,
        other => panic!("unexpected rows column: {other:?}"),
    };
    // Without a histogram the linear-only estimator would still
    // give roughly (51/200)*200 = ~51 in this uniform case. With
    // the histogram it should be in the same ballpark — assert the
    // estimate stays bounded near the true 51 rather than wildly
    // off (e.g., the old default 33% fallback ≈ 66).
    assert!(
        rows_estimate >= 30 && rows_estimate <= 75,
        "histogram-aided range estimate should be near 51, got {rows_estimate}"
    );
}

#[test]
fn explain_analyze_defaults_to_tree_with_actual_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN ANALYZE SELECT id FROM t WHERE v >= 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.columns.len(), 1);
    assert_eq!(explain.columns[0].name, "EXPLAIN");
    let Value::String(tree) = &explain.rows[0][0] else {
        panic!("default EXPLAIN ANALYZE output must be TREE text");
    };
    assert!(
        tree.contains("actual time="),
        "missing runtime metrics: {tree}"
    );
    assert!(
        tree.contains("rows=2"),
        "missing observed row count: {tree}"
    );

    // The tabular extension remains available when requested explicitly.
    let QueryResponse::ResultSet(traditional) = engine
        .execute("EXPLAIN ANALYZE FORMAT=TRADITIONAL SELECT id FROM t WHERE v >= 20")
        .unwrap()
    else {
        panic!("expected traditional explain result set");
    };
    assert_eq!(traditional.columns.len(), 17);
    assert_eq!(traditional.columns[12].name, "actual_rows");
    assert_eq!(traditional.columns[13].name, "actual_loops");
    assert_eq!(traditional.columns[14].name, "actual_first_row_ms");
    assert_eq!(traditional.columns[15].name, "actual_last_row_ms");
    assert_eq!(traditional.columns[16].name, "actual_rows_total");
    assert_eq!(traditional.rows[0][16], Value::Int(2));
}

#[test]
fn explain_analyze_allows_date_add_interval_in_filter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, o_orderdate DATE)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders VALUES \
             (1, '1993-12-31'), (2, '1994-01-01'), (3, '1994-06-15'), (4, '1995-01-01')",
        )
        .unwrap();

    let predicate = "o_orderdate >= '1994-01-01' \
        AND o_orderdate < DATE_ADD('1994-01-01', __INTERVAL('1','YEAR'))";
    let QueryResponse::ResultSet(counts) = engine
        .execute(&format!("SELECT COUNT(*) FROM orders WHERE {predicate}"))
        .unwrap()
    else {
        panic!("expected count result set");
    };
    assert_eq!(counts.rows[0][0], Value::Int(2));

    let QueryResponse::ResultSet(explain) = engine
        .execute(&format!(
            "EXPLAIN ANALYZE FORMAT=TRADITIONAL SELECT COUNT(*) FROM orders WHERE {predicate}"
        ))
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.columns[12].name, "actual_rows");

    let plain_tree = explain_single_text(
        &engine,
        &format!("EXPLAIN FORMAT=TREE SELECT COUNT(*) FROM orders WHERE {predicate}"),
    );
    let analyze_tree = explain_single_text(
        &engine,
        &format!("EXPLAIN ANALYZE FORMAT=TREE SELECT COUNT(*) FROM orders WHERE {predicate}"),
    );
    assert!(
        !plain_tree.contains("DATE_ADD") && !analyze_tree.contains("DATE_ADD"),
        "EXPLAIN and EXPLAIN ANALYZE should plan the same folded predicate, got plain:\n{plain_tree}\nanalyze:\n{analyze_tree}"
    );

    // Typed DATE syntax carries result metadata and is deliberately retained
    // in a SELECT projection. In a CTE scan predicate that metadata is not
    // observable, so the DATE_ADD must still fold rather than execute once per
    // scanned row.
    let cte_tree = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=TREE WITH bounded AS (\
             SELECT id FROM orders \
              WHERE o_orderdate >= '1994-01-01' \
                AND o_orderdate < DATE '1994-01-01' + INTERVAL '3' MONTH\
         ) SELECT COUNT(*) FROM bounded",
    );
    assert!(
        !cte_tree.contains("DATE_ADD") && !cte_tree.contains("__INTERVAL"),
        "typed temporal bounds in a CTE predicate should fold once: {cte_tree}"
    );
}

#[test]
fn explain_analyze_index_range_max_uses_fused_aggregate_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE lineitem (\
             id INT PRIMARY KEY, \
             l_shipdate DATE, \
             l_extendedprice DECIMAL(12,2), \
             KEY idx_lineitem_shipdate (l_shipdate))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
             (1, '1995-08-31', 10.00), \
             (2, '1995-09-01', 100.00), \
             (3, '1995-09-15', 103849.50), \
             (4, '1995-10-01', 42.00), \
             (5, '1995-10-02', 999999.00)",
        )
        .unwrap();

    let sql = "SELECT MAX(l_extendedprice) AS promo_revenue \
               FROM lineitem \
               WHERE l_shipdate BETWEEN DATE'1995-09-01' AND '1995-10-01'";
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    match &rs.rows[0][0] {
        Value::DecimalCompact(10384950, 2) => {}
        Value::Decimal(s) if s == "103849.50" => {}
        other => panic!("unexpected MAX(l_extendedprice): {other:?}"),
    }

    let text = explain_tree_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
    assert!(
        text.contains("Aggregate") && text.contains("Index range scan on lineitem"),
        "expected Aggregate over Index range scan, got: {text}"
    );
    assert!(
        text.contains("included in operator") && !text.contains("stats_source=synthetic"),
        "fused index-range aggregate should declare ownership without copied stats: {text}"
    );
}

#[test]
fn not_exists_rewrites_to_anti_join() {
    // Phase B #B1: simple NOT EXISTS subqueries now decorrelate into
    // ANTI joins even when they reference the outer row, as long as
    // the body is a single-table filter we can lift into the join
    // condition. (NOT IN intentionally still goes through the generic
    // subquery path because of NULL semantics.)
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, x INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, y INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 20)")
        .unwrap();
    engine.execute("INSERT INTO b VALUES (101, 10)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y = a.x)")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let any_anti = explain
        .rows
        .iter()
        .any(|row| matches!(&row[11], Value::String(s) if s.contains("anti join")));
    assert!(
        any_anti,
        "simple correlated NOT EXISTS should rewrite to anti join"
    );
    assert!(
        explain
            .rows
            .iter()
            .all(|row| row[1] != Value::String("SUBQUERY".into())),
        "decorrelated NOT EXISTS should not also be reported as a SUBQUERY: {:?}",
        explain.rows
    );

    // Now an uncorrelated NOT EXISTS — should rewrite.
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y = 999)")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let any_anti = explain
        .rows
        .iter()
        .any(|row| matches!(&row[11], Value::String(s) if s.contains("anti join")));
    assert!(
        any_anti,
        "Uncorrelated NOT EXISTS must rewrite to anti join"
    );
}

#[test]
fn streaming_aggregate_chosen_when_input_index_order_matches_group_by() {
    // Phase B #B4: GROUP BY g where g is the leading column of an
    // index → the planner sees that the input is already sorted on
    // g and emits StreamingAggregate (no hash table). EXPLAIN's
    // "extra" should announce that.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT, INDEX g_idx (g))")
        .unwrap();
    let mut values = Vec::new();
    for i in 1..=30 {
        values.push(format!("({i}, {}, {i})", i % 5));
    }
    engine
        .execute(&format!("INSERT INTO t VALUES {}", values.join(", ")))
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT g, COUNT(*) FROM t WHERE g >= 0 GROUP BY g")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let any_streaming = explain
        .rows
        .iter()
        .any(|row| matches!(&row[11], Value::String(s) if s.contains("streaming aggregate") || s.contains("index for aggregate")));
    assert!(
        any_streaming,
        "EXPLAIN must show streaming or covering-index aggregate when input is sorted on group_by"
    );
}

#[test]
fn streaming_distinct_chosen_when_input_index_order_matches_projection() {
    // Phase B #B4: SELECT DISTINCT g FROM t WHERE g >= 0 — input is
    // sorted on g (via g_idx range scan), so the planner emits
    // StreamingDistinct instead of HashSet-backed Projection.distinct.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, INDEX g_idx (g))")
        .unwrap();
    let values = (1..=20)
        .map(|i| format!("({i}, {})", i % 4))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT DISTINCT g FROM t WHERE g >= 0")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let any_streaming = explain
        .rows
        .iter()
        .any(|row| matches!(&row[11], Value::String(s) if s.contains("streaming distinct")));
    assert!(
        any_streaming,
        "EXPLAIN must show 'Using streaming distinct' when input is sorted on projection columns"
    );
}

#[test]
fn explain_analyze_tree_separates_query_io_from_operator_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE io_display (id INT PRIMARY KEY, v INT, INDEX idx_v (v))")
        .unwrap();
    engine
        .execute("INSERT INTO io_display VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    for sql in [
        "SELECT id FROM io_display LIMIT 0",
        "WITH c AS (SELECT * FROM io_display) SELECT * FROM c ORDER BY v",
        "SELECT COUNT(*) FROM io_display",
        "SELECT a.v FROM io_display a JOIN io_display b ON a.id=b.id",
        "SELECT id FROM io_display UNION ALL SELECT id FROM io_display",
    ] {
        let text = explain_single_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
        let mut lines = text.lines();
        let summary = lines.next().unwrap();
        assert!(summary.starts_with("Query I/O: physical_reads="), "{text}");
        assert!(
            summary.contains(" cache_reads=") && summary.contains(" cache_hit_rate="),
            "{text}"
        );
        assert!(!summary.contains("actual time="), "{text}");
        assert!(lines.next().unwrap().starts_with("-> "), "{text}");
        assert_eq!(text.matches("Query I/O:").count(), 1, "{text}");
        assert!(
            !text.contains("query_physical_reads=") && !text.contains("query_cache_reads="),
            "{text}"
        );
        for line in text
            .lines()
            .skip(1)
            .filter(|line| line.contains("cache_hit_rate="))
        {
            assert!(
                line.contains("io_scope=operator_self")
                    || line.contains("io_scope=operator_interval_inclusive"),
                "{text}"
            );
        }
        let plain = explain_single_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
        assert!(
            !plain.contains("Query I/O:") && !plain.contains("io_scope="),
            "{plain}"
        );
        println!("{sql}\n{text}");
    }
}

#[test]
fn explain_analyze_tree_format_appends_actual_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("EXPLAIN ANALYZE FORMAT=TREE SELECT * FROM t WHERE v < 25")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(text) = &rs.rows[0][0] else {
        panic!("expected string");
    };
    assert!(
        text.contains("actual time="),
        "tree analyze should include actual time: {text}"
    );
    assert!(
        (text.contains("physical_reads=") || text.contains("cache_reads="))
            && text.contains("cache_hit_rate="),
        "tree analyze should include block access stats: {text}"
    );
    assert!(
        text.starts_with("Query I/O: physical_reads=")
            && text.contains(" cache_reads=")
            && !text.contains("query_physical_reads="),
        "tree analyze should always include query-scoped block totals: {text}"
    );
    assert!(
        text.contains("rows=2"),
        "tree analyze should report 2 actual rows: {text}"
    );
}

#[test]
fn explain_analyze_limit_zero_does_not_execute_child_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE analyze_limit_zero (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO analyze_limit_zero VALUES (1,10),(2,20)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE SELECT * FROM analyze_limit_zero LIMIT 0",
    );
    let limit = text
        .lines()
        .find(|line| line.contains("Limit 0"))
        .unwrap_or_else(|| panic!("missing Limit 0 node: {text}"));
    let scan = text
        .lines()
        .find(|line| line.contains("scan on analyze_limit_zero"))
        .unwrap_or_else(|| panic!("missing child scan: {text}"));
    assert!(limit.contains("rows=0 loops=1"), "{text}");
    assert!(scan.contains("(never executed)"), "{text}");
}

#[test]
fn explain_analyze_json_actuals_obey_metric_invariants() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE analyze_metric_invariants (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("INSERT INTO analyze_metric_invariants VALUES (1,1),(2,1),(3,2)")
        .unwrap();
    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=JSON SELECT k, COUNT(*) FROM analyze_metric_invariants GROUP BY k ORDER BY k",
    );
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();

    fn verify(value: &serde_json::Value, seen: &mut usize, seen_self_io: &mut bool) {
        match value {
            serde_json::Value::Object(map) => {
                if let Some(actual) = map.get("actual") {
                    assert_ne!(actual["stats_source"], "synthetic");
                    if actual["executed"] == true {
                        *seen += 1;
                        let loops = actual["loops"].as_u64().unwrap();
                        let rows = actual["rows"].as_f64().unwrap();
                        let total = actual["rows_total"].as_u64().unwrap() as f64;
                        let first = actual["first_row_ms"].as_f64().unwrap();
                        let last = actual["last_row_ms"].as_f64().unwrap();
                        assert!(loops > 0 && first <= last, "{actual}");
                        assert!((rows * loops as f64 - total).abs() <= 0.001, "{actual}");
                        let scope = actual["io_scope"].as_str().unwrap();
                        assert!(
                            matches!(scope, "operator_self" | "operator_interval_inclusive"),
                            "{actual}"
                        );
                        *seen_self_io |= scope == "operator_self";
                    }
                }
                for child in map.values() {
                    verify(child, seen, seen_self_io);
                }
            }
            serde_json::Value::Array(values) => {
                for child in values {
                    verify(child, seen, seen_self_io);
                }
            }
            _ => {}
        }
    }

    let mut seen = 0;
    let mut seen_self_io = false;
    verify(&value, &mut seen, &mut seen_self_io);
    assert!(seen >= 2, "expected multiple measured operators: {text}");
    assert!(seen_self_io, "expected exact operator-self I/O: {text}");
    assert!(value["query_block"]["execution"]["unassigned_cache_reads"].is_number());
}

#[test]
fn explain_analyze_uses_current_transaction_read_view() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE analyze_txn_view (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO analyze_txn_view VALUES (1)")
        .unwrap();

    let normal = response_rows(
        engine
            .execute_with_session(&mut session, "SELECT id FROM analyze_txn_view")
            .unwrap(),
    );
    assert_eq!(normal.len(), 1);

    for format in ["TREE", "JSON", "TRADITIONAL"] {
        let response = engine
            .execute_with_session(
                &mut session,
                &format!("EXPLAIN ANALYZE FORMAT={format} SELECT id FROM analyze_txn_view"),
            )
            .unwrap();
        match format {
            "TREE" => {
                let actual = response_rows(response);
                let Value::String(text) = &actual[0][0] else {
                    panic!("TREE output is not text")
                };
                assert!(text.contains("rows=1 loops=1"), "{format}: {text}");
                assert!(!text.contains("stats_source=measured"), "{format}: {text}");
                assert!(!text.contains("query_physical_reads="), "{text}");
                assert!(text.starts_with("Query I/O: physical_reads="), "{text}");
                assert!(text.contains(" cache_reads="), "{text}");
            }
            "JSON" => {
                let actual = response_rows(response);
                let Value::String(text) = &actual[0][0] else {
                    panic!("JSON output is not text")
                };
                let json: serde_json::Value = serde_json::from_str(text).unwrap();
                assert_eq!(json["query_block"]["execution"]["rows_total"], 1, "{json}");
                assert!(json["query_block"]["execution"]["wall_time_ms"].is_number());
                assert_eq!(json["query_block"]["execution"]["io_scope"], "query");
                assert!(text.contains("\"stats_source\""), "{text}");
            }
            _ => {
                let actual = response_rows(response);
                assert_eq!(actual[0][16], Value::Int(1), "{actual:?}");
            }
        }
    }
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn explain_analyze_json_format_includes_actual_block() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("EXPLAIN ANALYZE FORMAT=JSON SELECT * FROM t WHERE v < 25")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(text) = &rs.rows[0][0] else {
        panic!("expected string");
    };
    assert!(
        text.contains("\"actual\""),
        "json analyze should include actual object: {text}"
    );
    assert!(
        text.contains("\"physical_reads\"")
            && text.contains("\"cache_reads\"")
            && text.contains("\"cache_hit_rate\""),
        "json analyze should include block access stats: {text}"
    );
    assert!(
        text.contains("\"rows\": 2"),
        "json analyze should report 2 actual rows: {text}"
    );
}

#[test]
fn explain_analyze_tree_per_node_stats() {
    // Each operator in the plan tree should carry its own
    // (actual time=... rows=... loops=...) suffix when ANALYZE runs.
    // A two-table join produces multiple operator lines (the join +
    // each input scan), so we expect at least two `actual time=`
    // occurrences in the tree text.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("EXPLAIN ANALYZE FORMAT=TREE SELECT a.v FROM a JOIN b ON a.id = b.a_id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(text) = &rs.rows[0][0] else {
        panic!("expected string");
    };
    let actual_count = text.matches("actual time=").count();
    assert!(
        actual_count >= 2,
        "tree analyze should annotate every operator (got {actual_count} suffixes): {text}"
    );
    assert!(
        (text.contains("physical_reads=") || text.contains("cache_reads="))
            && text.contains("cache_hit_rate="),
        "tree analyze should include non-zero block access stats: {text}"
    );
    let loops_count = text.matches("loops=").count();
    assert_eq!(
        loops_count, actual_count,
        "every operator suffix should include loops=: {text}"
    );
}

#[test]
fn explain_analyze_json_per_node_actual() {
    // JSON output should embed an "actual" object inside every node
    // object (table / hash_join / nested_loop), not just the top-level
    // query_block.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (1, 1), (2, 2)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("EXPLAIN ANALYZE FORMAT=JSON SELECT a.v FROM a JOIN b ON a.id = b.a_id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(text) = &rs.rows[0][0] else {
        panic!("expected string");
    };
    // One "actual" at query_block top, plus one per node (>= the two
    // table scans + the join). Total >= 3.
    let actual_count = text.matches("\"actual\"").count();
    assert!(
        actual_count >= 3,
        "json analyze should embed per-node actual blocks (got {actual_count}): {text}"
    );
    assert!(
        text.contains("\"loops\":"),
        "per-node actual blocks should report loops: {text}"
    );
    assert!(
        text.contains("\"physical_reads\"")
            && text.contains("\"cache_reads\"")
            && text.contains("\"cache_hit_rate\""),
        "per-node actual blocks should report block access stats: {text}"
    );
}

#[test]
fn explain_analyze_set_operation_uses_executed_plan_io_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE analyze_set_io (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO analyze_set_io VALUES (1,10),(2,20),(3,30)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT id, v FROM analyze_set_io \
         UNION ALL \
         SELECT id, v FROM analyze_set_io",
    );
    assert!(!text.contains("never executed"), "{text}");
    let root = text
        .lines()
        .find(|line| line.starts_with("-> "))
        .unwrap_or_default();
    assert!(root.contains("rows=6 loops=1"), "{text}");
    assert!(
        text.starts_with("Query I/O: physical_reads=")
            && !root.contains("query_physical_reads=")
            && text.matches("Query I/O:").count() == 1,
        "set-operation tree must expose query-scoped block totals once: {text}"
    );
    let scans = text
        .lines()
        .filter(|line| line.contains("scan on analyze_set_io"))
        .collect::<Vec<_>>();
    assert_eq!(scans.len(), 2, "{text}");
    assert!(
        scans.iter().all(|line| {
            (line.contains("physical_reads=") || line.contains("cache_reads="))
                && line.contains("cache_hit_rate=")
        }),
        "both executed set-operation branches must report block reads: {text}"
    );
}

#[test]
fn explain_analyze_tree_bloom_probe_scan_reports_actual_stats() {
    let _guard = super::test_env_lock();
    let prior_bottom = std::env::var("MYDB_HASH_JOIN_STREAM_BOTTOM").ok();
    let prior_bloom_min = std::env::var("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS").ok();
    let _restore_bottom = restore_env("MYDB_HASH_JOIN_STREAM_BOTTOM", prior_bottom);
    let _restore_bloom_min = restore_env("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", prior_bloom_min);
    std::env::set_var("MYDB_HASH_JOIN_STREAM_BOTTOM", "on");
    std::env::set_var("MYDB_HASH_JOIN_BLOOM_MIN_BUILD_ROWS", "1");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT, payload INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let values = (1..=400)
        .map(|i| {
            let a_id = if i % 5 == 0 { 999 } else { (i % 3) + 1 };
            format!("({i}, {a_id}, {})", i * 9)
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO b VALUES {values}"))
        .unwrap();

    // The payload predicate is deliberately too unselective for the CBO to
    // choose predicate-first by itself. Once HashJoin publishes its Bloom,
    // the unopened scan should select the pre-lowered combined predicate
    // source and reject both conditions before payload materialization.
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "EXPLAIN ANALYZE FORMAT=TREE \
             SELECT /*+ HASH_JOIN(a,b) */ a.v \
             FROM a JOIN b ON a.id = b.a_id \
             WHERE b.payload >= 1800",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(text) = &rs.rows[0][0] else {
        panic!("expected string");
    };
    assert!(
        text.contains("Hash join"),
        "expected hash join in plan text: {text}"
    );
    assert!(
        !text.contains("(never executed)"),
        "bloom-injected probe scan should still report actual stats on the original node: {text}"
    );
    assert!(
        text.matches("actual time=").count() >= 3,
        "expected join plus both scans to report actual stats: {text}"
    );
    assert!(
        text.contains("runtime_filter_rejected="),
        "runtime Bloom filtering must expose rejected rows in EXPLAIN ANALYZE: {text}"
    );
    let rejected = text
        .split("runtime_filter_rejected=")
        .nth(1)
        .and_then(|suffix| {
            let digits = suffix
                .chars()
                .take_while(|ch| ch.is_ascii_digit())
                .collect::<String>();
            digits.parse::<u64>().ok()
        })
        .expect("runtime filter rejection count");
    assert!(
        rejected > 0,
        "predicate-first runtime filtering must report actual rejected rows: {text}"
    );
    let probe_scan = text
        .lines()
        .find(|line| line.contains("Table scan on b"))
        .expect("probe scan line");
    assert!(
        probe_scan.contains("rows=160"),
        "combined SQL/runtime predicate should materialize only matching probe rows: {probe_scan}"
    );
}

#[test]
fn explain_analyze_runtime_filter_is_independent_of_local_predicate_kernel() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE build_side (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE probe_side (id INT PRIMARY KEY, build_id INT, payload INT, label VARCHAR(16))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO build_side VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let values = (1..=400)
        .map(|i| {
            let build_id = if i % 5 == 0 { 999 } else { (i % 3) + 1 };
            format!("({i}, {build_id}, {}, 'keep')", i * 9)
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO probe_side VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE build_side").unwrap();
    engine.execute("ANALYZE TABLE probe_side").unwrap();

    // String IN is intentionally outside the old CompiledPredicate subset
    // (its table-aware collation rule keeps that evaluator conservative).
    // The common batch expression kernel evaluates it, while the unopened
    // storage source must still accept the exact join membership filter and
    // materialize only the 320 matching probe rows.
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "EXPLAIN ANALYZE FORMAT=TREE \
             SELECT /*+ HASH_JOIN(build_side,probe_side) */ build_side.v \
             FROM build_side JOIN probe_side \
               ON build_side.id = probe_side.build_id \
             WHERE probe_side.label IN ('keep', 'other')",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(text) = &rs.rows[0][0] else {
        panic!("expected string");
    };
    let probe_scan = text
        .lines()
        .find(|line| line.contains("Table scan on probe_side"))
        .expect("probe scan line");
    assert!(
        probe_scan.contains("rows=320"),
        "runtime filter must be applied before output materialization even when the local predicate uses another batch kernel: {probe_scan}\n{text}"
    );
    assert!(
        text.contains("runtime_filter_rejected=80"),
        "the exact filter should report all non-member probe keys: {text}"
    );
    assert!(
        probe_scan.contains("execution_mode=vector")
            && probe_scan.contains("scalar_rows=320")
            && probe_scan.contains("fallback_expression_rows=320"),
        "an unsupported local expression must use the scalar oracle inside the same filtered batch pipeline without rescanning input: {text}"
    );
}

#[test]
fn multiway_star_join_tree_and_analyze_keep_costed_join_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name VARCHAR(25))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE nation (\
             n_nationkey INT PRIMARY KEY, \
             n_name VARCHAR(25), \
             n_regionkey INT, \
             INDEX idx_nation_region (n_regionkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE supplier (\
             s_suppkey INT PRIMARY KEY, \
             s_name VARCHAR(25), \
             s_nationkey INT, \
             INDEX idx_supplier_nation (s_nationkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE customer (\
             c_custkey INT PRIMARY KEY, \
             c_name VARCHAR(25), \
             c_nationkey INT, \
             INDEX idx_customer_nation (c_nationkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_custkey INT, \
             o_orderdate DATE, \
             INDEX idx_orders_custkey (o_custkey), \
             INDEX idx_orders_orderdate (o_orderdate))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT, \
             l_suppkey INT, \
             l_linenumber INT, \
             l_extendedprice DECIMAL(15,2), \
             l_discount DECIMAL(4,2), \
             PRIMARY KEY (l_orderkey, l_linenumber), \
             INDEX idx_lineitem_orderkey (l_orderkey), \
             INDEX idx_lineitem_suppkey (l_suppkey))",
        )
        .unwrap();

    engine
        .execute("INSERT INTO region VALUES (1, 'ASIA'), (2, 'EUROPE')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO nation VALUES \
             (1, 'CHINA', 1), (2, 'INDIA', 1), (3, 'FRANCE', 2)",
        )
        .unwrap();
    let suppliers = (1..=30)
        .map(|i| {
            let nation = if i % 3 == 0 {
                3
            } else if i % 2 == 0 {
                2
            } else {
                1
            };
            format!("({i}, 'S{i}', {nation})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO supplier VALUES {suppliers}"))
        .unwrap();
    let customers = (1..=90)
        .map(|i| {
            let nation = if i % 3 == 0 {
                3
            } else if i % 2 == 0 {
                2
            } else {
                1
            };
            format!("({i}, 'C{i}', {nation})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO customer VALUES {customers}"))
        .unwrap();
    let orders = (1..=180)
        .map(|i| {
            let cust = (i % 90) + 1;
            let date = if i % 4 == 0 {
                "1993-12-31"
            } else {
                "1994-06-15"
            };
            format!("({i}, {cust}, '{date}')")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO orders VALUES {orders}"))
        .unwrap();
    let lineitems = (1..=180)
        .flat_map(|i| {
            let supp = (i % 30) + 1;
            [
                format!("({i}, {supp}, 1, 100.00, 0.05)"),
                format!("({i}, {}, 2, 50.00, 0.00)", (supp % 30) + 1),
            ]
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO lineitem VALUES {lineitems}"))
        .unwrap();

    for table in [
        "region", "nation", "supplier", "customer", "orders", "lineitem",
    ] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let q5 = "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue \
              FROM customer \
              INNER JOIN orders ON c_custkey = o_custkey \
              INNER JOIN lineitem ON l_orderkey = o_orderkey \
              INNER JOIN supplier ON l_suppkey = s_suppkey \
              INNER JOIN nation ON c_nationkey = s_nationkey \
                              AND s_nationkey = n_nationkey \
              INNER JOIN region ON n_regionkey = r_regionkey \
              WHERE r_name = 'ASIA' \
                AND o_orderdate >= DATE '1994-01-01' \
                AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR \
              GROUP BY n_name \
              ORDER BY revenue DESC";

    let tree_text = explain_tree_text(&engine, &format!("EXPLAIN FORMAT=TREE {q5}"));
    assert!(
        tree_text.contains(" join "),
        "multiway query should expose a physical join tree: {tree_text}"
    );
    // Transitive `col = col` closure (planner.rs `synthesize_transitive_equalities`)
    // must derive `c_nationkey = n_nationkey` from the original
    // `c_nationkey = s_nationkey AND s_nationkey = n_nationkey` chain.
    // Without it, DPsub can't form an early `customer ⋈ nation` join and
    // the lineitem fact-side blows up (TPC-H Q5 was 4-5× slower).
    assert!(
        tree_text.contains("c_nationkey=n_nationkey")
            || tree_text.contains("n_nationkey=c_nationkey"),
        "Q5-shaped plan must surface the synthesized customer⋈nation edge \
         from transitive equality closure: {tree_text}"
    );
    assert_no_unconditional_nested_loop(&tree_text);
    for needle in [
        "customer", "orders", "lineitem", "supplier", "nation", "region",
    ] {
        assert!(
            tree_text.contains(needle),
            "Q5-shaped plan should mention {needle}: {tree_text}"
        );
    }

    let analyzed = explain_tree_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {q5}"));
    assert!(
        analyzed.contains("actual time="),
        "multiway star-join analyze tree should keep per-node timing: {analyzed}"
    );
    assert!(
        !analyzed.contains("(never executed)"),
        "every executed Q5-shaped subtree must retain its own display-plan stats: {analyzed}"
    );
    for region_line in analyzed
        .lines()
        .filter(|line| line.contains("on region") && line.contains("actual time="))
    {
        assert!(
            region_line.contains(" rows=1 loops=1")
                || region_line.contains(" rows_per_loop=1.000"),
            "streaming clone stats must stay attached to the one-row region scan per execution, not a reused-address join node: {region_line}\n{analyzed}"
        );
    }
}

#[test]
fn index_in_lookup_keeps_outer_driven_inlj_alternative() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE allowed_inlj (k INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE outer_inlj (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE fact_inlj (\
             id INT PRIMARY KEY, join_id INT NOT NULL, filter_id INT NOT NULL, \
             INDEX idx_fact_join(join_id), INDEX idx_fact_filter(filter_id))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO allowed_inlj VALUES (1)")
        .unwrap();
    engine
        .execute("INSERT INTO outer_inlj VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    let facts = (1..=100)
        .map(|i| {
            let key = (i % 5) + 1;
            format!("({i},{key},{key})")
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO fact_inlj VALUES {facts}"))
        .unwrap();
    for table in ["allowed_inlj", "outer_inlj", "fact_inlj"] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let body = "SELECT /*+ ORDERED INDEX_NLJ(f) */ COUNT(*) \
                FROM outer_inlj o \
                JOIN fact_inlj f ON o.id=f.join_id \
                WHERE f.filter_id IN (SELECT k FROM allowed_inlj)";
    let tree = explain_tree_text(&engine, &format!("EXPLAIN FORMAT=TREE {body}"));
    assert!(
        tree.contains("Index nested loop join") && tree.contains("idx_fact_join"),
        "an IN-subquery access path must retain a costable outer-driven index alternative: {tree}"
    );
    let QueryResponse::ResultSet(result) = engine.execute(body).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(20)]]);
}

#[test]
fn explain_analyze_filtered_fused_count_hash_join_reports_actual_stats() {
    let _guard = super::test_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = restore_env(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, flag INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 1), (2, 2), (3, 3), (4, 4)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO b VALUES \
             (10, 1, 1), (11, 1, 0), (12, 2, 1), \
             (13, 2, 1), (14, 3, 0), (15, 5, 1)",
        )
        .unwrap();

    let text = explain_tree_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT /*+ HASH_JOIN(a, b) */ COUNT(*) \
         FROM a JOIN b ON a.k = b.k \
         WHERE b.flag > 0",
    );
    assert!(
        text.contains("Hash join"),
        "expected hash join in plan text: {text}"
    );
    assert!(
        !text.contains("(never executed)"),
        "filtered fused hash join and probe scan should report actual stats: {text}"
    );
    assert!(
        text.matches("actual time=").count() >= 4,
        "expected aggregate, join, and both scans to report actual stats: {text}"
    );
}

#[test]
fn explain_analyze_filtered_fused_grouped_hash_join_reports_actual_stats() {
    let _guard = super::test_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = restore_env(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT, priority INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, flag INT, price INT, discount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 1, 5), (2, 2, 4), (3, 3, 3), (4, 4, 2)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO b VALUES \
             (10, 1, 1, 100, 0), (11, 1, 0, 1000, 0), \
             (12, 2, 1, 50, 0), (13, 2, 1, 70, 0), \
             (14, 3, 0, 200, 0), (15, 5, 1, 999, 0)",
        )
        .unwrap();

    let text = explain_tree_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT /*+ HASH_JOIN(a, b) */ b.k, a.priority, \
                SUM(b.price * (1 - b.discount)) AS revenue \
         FROM a JOIN b ON a.k = b.k \
         WHERE b.flag > 0 \
         GROUP BY b.k, a.priority \
         ORDER BY revenue DESC \
         LIMIT 2",
    );
    assert!(
        text.contains("Hash join"),
        "expected hash join in plan text: {text}"
    );
    assert!(
        !text.contains("(never executed)"),
        "filtered fused grouped hash join and probe scan should report actual stats: {text}"
    );
    assert!(
        text.matches("actual time=").count() >= 4,
        "expected aggregate, join, and scans to report actual stats: {text}"
    );
}

#[test]
fn explain_analyze_tpcds_q43_shape_reports_hash_join_inputs() {
    let _guard = super::test_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = restore_env(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE date_dim (\
             d_date_sk INT PRIMARY KEY, d_year INT, d_day_name VARCHAR(16), \
             INDEX idx_date_dim_year (d_year))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE store (s_store_sk INT PRIMARY KEY, s_store_name VARCHAR(32), s_store_id VARCHAR(16), s_gmt_offset INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE store_sales (\
             ss_sold_date_sk INT, ss_store_sk INT, ss_sales_price INT, \
             INDEX idx_store_sales_date (ss_sold_date_sk), \
             INDEX idx_store_sales_store (ss_store_sk))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO date_dim VALUES \
             (1, 2000, 'Sunday'), (2, 2000, 'Monday'), \
             (3, 2001, 'Sunday'), (4, 2000, 'Tuesday')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO store VALUES \
             (10, 'Alpha', 'A', -5), (20, 'Beta', 'B', -5), (30, 'Gamma', 'G', -6)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO store_sales VALUES \
             (1, 10, 100), (2, 10, 40), (4, 20, 70), \
             (3, 10, 900), (1, 30, 500)",
        )
        .unwrap();

    let text = explain_tree_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT /*+ PARALLEL(4) HASH_JOIN(date_dim, store_sales) HASH_JOIN(store_sales, store) */ \
                s_store_name, s_store_id, \
                SUM(CASE WHEN d_day_name = 'Sunday' THEN ss_sales_price ELSE NULL END) sun_sales, \
                SUM(CASE WHEN d_day_name = 'Monday' THEN ss_sales_price ELSE NULL END) mon_sales, \
                SUM(CASE WHEN d_day_name = 'Tuesday' THEN ss_sales_price ELSE NULL END) tue_sales \
         FROM date_dim, store_sales, store \
         WHERE d_date_sk = ss_sold_date_sk \
           AND s_store_sk = ss_store_sk \
           AND s_gmt_offset = -5 \
           AND d_year = 2000 \
         GROUP BY s_store_name, s_store_id \
         ORDER BY s_store_name, s_store_id, sun_sales, mon_sales, tue_sales \
         LIMIT 100",
    );
    assert!(
        text.contains("Hash join"),
        "expected hash join in q43-shaped plan: {text}"
    );
    assert!(
        !text.contains("(never executed)"),
        "q43-shaped EXPLAIN ANALYZE should annotate executed hash join inputs: {text}"
    );
    assert!(
        !text
            .lines()
            .any(|line| line.contains("Hash join") && line.contains("actual time=0.000ms")),
        "hash join nodes should report measured elapsed time, not a zero-time placeholder: {text}"
    );
}

#[test]
fn explain_analyze_tpcds_count_sip_hash_join_reports_executed() {
    let _guard = super::test_env_lock();
    let prior_stream_disable = std::env::var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE").ok();
    let _restore_stream_disable = restore_env(
        "MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE",
        prior_stream_disable,
    );
    std::env::remove_var("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE date_dim (\
             d_date_sk INT PRIMARY KEY, \
             d_year INT, \
             d_moy INT, \
             INDEX idx_date_dim_year (d_year))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE catalog_sales (\
             cs_ship_date_sk INT, \
             filler INT, \
             INDEX idx_cs_ship_date (cs_ship_date_sk))",
        )
        .unwrap();

    for i in 1..=90 {
        let year = if i <= 31 { 2002 } else { 2001 };
        let moy = if i <= 31 { 1 } else { 2 };
        engine
            .execute(&format!("INSERT INTO date_dim VALUES ({i}, {year}, {moy})"))
            .unwrap();
    }
    for i in 0..900 {
        let ship_date = (i % 90) + 1;
        engine
            .execute(&format!(
                "INSERT INTO catalog_sales VALUES ({ship_date}, {i})"
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE date_dim").unwrap();
    engine.execute("ANALYZE TABLE catalog_sales").unwrap();

    let sql = "SELECT /*+ HASH_JOIN(date_dim, catalog_sales) */ COUNT(*) \
               FROM date_dim, catalog_sales \
               WHERE cs_ship_date_sk = d_date_sk \
                 AND d_year = 2002 \
                 AND d_moy = 1";
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(310));

    let tree = explain_tree_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
    assert!(
        tree.contains("Hash join"),
        "expected Hash join in TPC-DS-shaped count plan: {tree}"
    );
    assert!(
        tree.contains("Index lookup on catalog_sales using idx_cs_ship_date with IN subquery"),
        "SIP should keep the fact-side IN-subquery index lookup: {tree}"
    );
    assert!(
        tree.contains("Index lookup on date_dim using idx_date_dim_year"),
        "tree should show the date_dim lookup used by the join/SIP subquery: {tree}"
    );
    assert!(
        !tree
            .lines()
            .any(|line| line.contains("Hash join") && line.contains("(never executed)")),
        "fused COUNT(*) hash join should report actual stats, not never executed: {tree}"
    );
}

// ───── Index Hint Tests ─────

#[test]
fn hint_use_index_selects_specified_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, INDEX idx_a(a), INDEX idx_b(b))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 20), (2, 20, 30), (3, 30, 40)")
        .unwrap();

    // Without hint: CBO picks idx_a for WHERE a = 10
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t USE INDEX (idx_b) WHERE b = 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.rows[0][6], Value::String("idx_b".into()));
    let extra = match &explain.rows[0][11] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    assert!(
        extra.contains("USE INDEX"),
        "Extra should mention USE INDEX, got: {extra}"
    );
}

#[test]
fn hint_ignore_index_avoids_specified_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    // With IGNORE INDEX on idx_a, CBO should fall back to full scan or PRIMARY
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t IGNORE INDEX (idx_a) WHERE a = 10")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    // key should NOT be idx_a
    let key = match &explain.rows[0][6] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    assert_ne!(key, "idx_a", "IGNORE INDEX should prevent using idx_a");
}

#[test]
fn hint_force_index_prefers_index_over_seqscan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t FORCE INDEX (idx_a) WHERE a > 0")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let access_type = match &explain.rows[0][4] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    let key = match &explain.rows[0][6] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    let extra = match &explain.rows[0][11] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    // FORCE INDEX should make the planner use idx_a (range or ref access)
    assert!(
        key == "idx_a" || access_type != "ALL",
        "FORCE INDEX should prefer idx_a, got key={key}, type={access_type}, extra={extra}"
    );
    assert!(
        extra.contains("FORCE INDEX"),
        "Extra should mention FORCE INDEX, got: {extra} (key={key}, type={access_type})"
    );
}

#[test]
fn hint_invalid_index_name_does_not_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    // USE INDEX with a nonexistent index should not error, just fall back
    let result = engine.execute("SELECT * FROM t USE INDEX (nonexistent) WHERE id = 1");
    assert!(
        result.is_ok(),
        "invalid hint index name should not cause error"
    );
}

#[test]
fn hint_use_index_execution_returns_correct_results() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, INDEX idx_a(a), INDEX idx_b(b))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT * FROM t USE INDEX (idx_a) WHERE a = 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[0][1], Value::Int(20));
    assert_eq!(result.rows[0][2], Value::Int(200));
}

#[test]
fn hint_on_joined_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, b INT, INDEX idx_b(b))")
        .unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO t2 VALUES (1, 100), (2, 200)")
        .unwrap();

    // Both tables with hints — should parse and execute correctly
    let result = engine.execute(
        "SELECT * FROM t1 USE INDEX (idx_a) JOIN t2 FORCE INDEX (idx_b) ON t1.id = t2.id WHERE t1.a = 10"
    );
    assert!(
        result.is_ok(),
        "JOIN with hints should execute successfully"
    );
}

#[test]
fn hint_full_scan_forces_seqscan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ FULL_SCAN(t) */ * FROM t WHERE a = 5")
        .unwrap()
    else {
        panic!("expected explain result set");
    };

    assert_eq!(
        explain.rows[0][4],
        Value::String("ALL".into()),
        "FULL_SCAN hint should force seq scan"
    );
    let extra = match &explain.rows[0][11] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    assert!(
        extra.contains("FULL_SCAN"),
        "Extra should note FULL_SCAN hint, got: {extra}"
    );
}

#[test]
fn hint_no_index_removes_index_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, INDEX idx_a(a), INDEX idx_b(b))",
        )
        .unwrap();
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t").unwrap();

    // Without hint, a=5 should use idx_a
    let QueryResponse::ResultSet(explain_normal) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE a = 5")
        .unwrap()
    else {
        panic!("expected explain");
    };
    assert_eq!(explain_normal.rows[0][6], Value::String("idx_a".into()));

    // With NO_INDEX, idx_a should be excluded
    let QueryResponse::ResultSet(explain_no) = engine
        .execute("EXPLAIN SELECT /*+ NO_INDEX(t idx_a) */ * FROM t WHERE a = 5")
        .unwrap()
    else {
        panic!("expected explain");
    };
    assert_ne!(
        explain_no.rows[0][6],
        Value::String("idx_a".into()),
        "NO_INDEX should exclude idx_a"
    );
}

#[test]
fn hash_join_ndv_range_cap_respects_key_type_and_precision() {
    for (data_type, values) in [
        (
            "VARCHAR(64)",
            (0..20).map(|i| format!("'key-{i:02}'")).collect::<Vec<_>>(),
        ),
        (
            "VARCHAR(64)",
            (0..20).map(|i| format!("'0.{i:03}'")).collect(),
        ),
        (
            "VARBINARY(64)",
            (0..20).map(|i| format!("'key-{i:02}'")).collect(),
        ),
        (
            "DECIMAL(10,3)",
            (0..20).map(|i| format!("0.{i:03}")).collect(),
        ),
        ("DOUBLE", (0..20).map(|i| format!("0.{i:03}")).collect()),
        (
            "DATETIME(6)",
            (0..20)
                .map(|i| format!("'2026-01-01 00:00:00.{i:06}'"))
                .collect(),
        ),
        (
            "BIGINT",
            (0..20)
                .map(|i| (9_007_199_254_740_992_000_i64 + i).to_string())
                .collect(),
        ),
        (
            "BIGINT UNSIGNED",
            (0..20).map(|i| (u64::MAX - i).to_string()).collect(),
        ),
        ("INT", (0..20).map(|i| i.to_string()).collect()),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute(&format!("CREATE TABLE ndv_probe (id {data_type} NOT NULL)"))
            .unwrap();
        engine
            .execute(&format!(
                "CREATE TABLE ndv_build (id {data_type} PRIMARY KEY)"
            ))
            .unwrap();
        let values = values
            .iter()
            .map(|v| format!("({v})"))
            .collect::<Vec<_>>()
            .join(",");
        for table in ["ndv_probe", "ndv_build"] {
            engine
                .execute(&format!("INSERT INTO {table} VALUES {values}"))
                .unwrap();
            engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
        }
        let tree = explain_single_text(&engine, "EXPLAIN FORMAT=TREE SELECT /*+ HASH_JOIN(ndv_probe) */ COUNT(*) FROM ndv_probe JOIN ndv_build ON ndv_probe.id=ndv_build.id");
        assert_eq!(
            join_rows_estimate(&tree, "Hash join"),
            20,
            "{data_type}:\n{tree}"
        );
        let rows = response_rows(engine.execute("SELECT /*+ HASH_JOIN(ndv_probe) */ COUNT(*) FROM ndv_probe JOIN ndv_build ON ndv_probe.id=ndv_build.id").unwrap());
        assert_eq!(rows, vec![vec![Value::Int(20)]], "{data_type}");
    }
}

#[test]
fn inlj_and_hash_join_agree_on_join_cardinality() {
    // The same logical join must not change its row estimate with the
    // physical operator. Pre-fix, the INLJ estimate divided by the
    // inner index's NDV only (here fk NDV = 10), while the hash join
    // divided by max(NDV of both sides) (dim pk NDV = 20), so the
    // hinted INLJ plan showed 2× the hash join's rows for the identical
    // join. The fk deliberately covers only half the dim key space to
    // expose the divergence without faking stats.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE dim (d_id INT PRIMARY KEY, d_pad INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE fact (f_id INT PRIMARY KEY, f_dim INT, INDEX idx_dim(f_dim))")
        .unwrap();
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO dim VALUES ({i}, {i})"))
            .unwrap();
    }
    let mut row = 0;
    for _ in 0..10 {
        let mut values = Vec::new();
        for key in 0..10 {
            values.push(format!("({row}, {key})"));
            row += 1;
        }
        engine
            .execute(&format!("INSERT INTO fact VALUES {}", values.join(",")))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE dim").unwrap();
    engine.execute("ANALYZE TABLE fact").unwrap();

    let join_rows = |text: &str, needle: &str| -> i64 {
        let line = text
            .lines()
            .find(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("no `{needle}` line in:\n{text}"));
        let rest = &line[line.find("rows=").expect("rows=") + "rows=".len()..];
        rest.chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .unwrap()
    };

    let inlj = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ NO_HASH_JOIN(dim, fact) */ COUNT(d_pad) \
         FROM dim JOIN fact ON d_id = f_dim",
    );
    assert!(
        inlj.contains("nested loop join"),
        "expected an index nested loop join plan:\n{inlj}"
    );
    let hash = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ HASH_JOIN(dim, fact) */ COUNT(d_pad) \
         FROM dim JOIN fact ON d_id = f_dim",
    );
    assert!(hash.contains("Hash join"), "expected hash join:\n{hash}");

    let inlj_rows = join_rows(&inlj, "nested loop join");
    let hash_rows = join_rows(&hash, "Hash join");
    assert_eq!(
        inlj_rows, hash_rows,
        "INLJ vs hash join cardinality diverged\nINLJ:\n{inlj}\nHASH:\n{hash}"
    );
    // True join size is 100 (every fact row matches exactly one dim
    // row); the max-NDV estimator lands exactly there.
    assert_eq!(hash_rows, 100, "hash join estimate moved:\n{hash}");
}

#[test]
fn hint_hash_join_forces_hash_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, a INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();
    for i in 0..10 {
        engine
            .execute(&format!("INSERT INTO t1 VALUES ({i}, {i})"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO t2 VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t1").unwrap();
    engine.execute("ANALYZE TABLE t2").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ HASH_JOIN(t1, t2) */ * FROM t1 JOIN t2 ON t1.a = t2.a")
        .unwrap()
    else {
        panic!("expected explain");
    };

    let has_hash = explain
        .rows
        .iter()
        .any(|r| matches!(&r[11], Value::String(s) if s.contains("Hash")));
    assert!(has_hash, "HASH_JOIN hint should force hash join");
}

#[test]
fn hint_no_hash_join_avoids_hash_join() {
    let _guard = super::test_env_lock();
    let prior_adaptive = std::env::var("MYDB_ADAPTIVE_JOIN").ok();
    std::env::set_var("MYDB_ADAPTIVE_JOIN", "1");
    let _restore_adaptive = restore_env("MYDB_ADAPTIVE_JOIN", prior_adaptive);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, a INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, a INT)")
        .unwrap();
    for i in 0..10 {
        engine
            .execute(&format!("INSERT INTO t1 VALUES ({i}, {i})"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO t2 VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t1").unwrap();
    engine.execute("ANALYZE TABLE t2").unwrap();

    let QueryResponse::ResultSet(default_explain) = engine
        .execute("EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.a = t2.a")
        .unwrap()
    else {
        panic!("expected explain");
    };
    assert!(
        default_explain.rows.iter().any(|r| match &r[11] {
            Value::String(s) => s.contains("Adaptive join") && s.contains("Hash join"),
            _ => false,
        }),
        "adaptive join should expose hash candidate before applying NO_HASH_JOIN"
    );

    let QueryResponse::ResultSet(no_hash_explain) = engine
        .execute("EXPLAIN SELECT /*+ no_hash_join(t1, t2) */ * FROM t1 JOIN t2 ON t1.a = t2.a")
        .unwrap()
    else {
        panic!("expected explain");
    };
    assert!(
        no_hash_explain
            .rows
            .iter()
            .all(|r| !matches!(&r[11], Value::String(s) if s.contains("Hash join"))),
        "NO_HASH_JOIN hint should avoid hash join"
    );
}

#[test]
fn adaptive_join_explicit_enable_explain_lists_candidates() {
    let _guard = super::test_env_lock();
    let prior_adaptive = std::env::var("MYDB_ADAPTIVE_JOIN").ok();
    std::env::set_var("MYDB_ADAPTIVE_JOIN", "1");
    let _restore_adaptive = restore_env("MYDB_ADAPTIVE_JOIN", prior_adaptive);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, a INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();
    for i in 0..10 {
        engine
            .execute(&format!("INSERT INTO t1 VALUES ({i}, {i})"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO t2 VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t1").unwrap();
    engine.execute("ANALYZE TABLE t2").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT * FROM t1 JOIN t2 ON t1.a = t2.a")
        .unwrap()
    else {
        panic!("expected explain");
    };
    let text = explain
        .rows
        .iter()
        .flat_map(|r| r.iter())
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("Adaptive join"), "{text}");
    assert!(text.contains("Hash join"), "{text}");
    assert!(text.contains("Index nested loop"), "{text}");
    assert!(text.contains("Nested loop"), "{text}");
}

#[test]
fn adaptive_join_default_off_explain_uses_fixed_join() {
    let _guard = super::test_env_lock();
    let prior_adaptive = std::env::var("MYDB_ADAPTIVE_JOIN").ok();
    std::env::remove_var("MYDB_ADAPTIVE_JOIN");
    let _restore_adaptive = restore_env("MYDB_ADAPTIVE_JOIN", prior_adaptive);

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, a INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();
    for i in 0..10 {
        engine
            .execute(&format!("INSERT INTO t1 VALUES ({i}, {i})"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO t2 VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t1").unwrap();
    engine.execute("ANALYZE TABLE t2").unwrap();

    let text = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT * FROM t1 JOIN t2 ON t1.a = t2.a",
    );
    assert!(!text.contains("Adaptive join"), "{text}");
}

#[test]
fn hint_ordered_preserves_from_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, a INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, a INT)")
        .unwrap();
    for i in 0..5 {
        engine
            .execute(&format!("INSERT INTO t1 VALUES ({i}, {i})"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO t2 VALUES ({i}, {i})"))
            .unwrap();
    }

    let result = engine.execute("SELECT /*+ ORDERED */ * FROM t1 JOIN t2 ON t1.id = t2.id");
    assert!(result.is_ok(), "ORDERED hint should execute without error");
}

#[test]
fn index_nlj_hint_applies_only_to_current_inner_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE customer (id INT PRIMARY KEY, segment VARCHAR(16))")
        .unwrap();
    for i in 1..=5 {
        engine
            .execute(&format!("INSERT INTO orders VALUES ({i}, {i})"))
            .unwrap();
        engine
            .execute(&format!(
                "INSERT INTO customer VALUES ({i}, '{}')",
                if i % 2 == 0 { "AUTO" } else { "BUILDING" }
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE customer").unwrap();

    let QueryResponse::ResultSet(wrong_side) = engine
        .execute(
            "EXPLAIN SELECT /*+ INDEX_NLJ(orders) HASH_JOIN(orders, customer) ORDERED */ orders.id \
             FROM orders JOIN customer ON customer.id = orders.cust_id",
        )
        .unwrap()
    else {
        panic!("expected explain");
    };
    assert!(
        wrong_side
            .rows
            .iter()
            .any(|row| matches!(&row[11], Value::String(extra) if extra.contains("Hash join"))),
        "INDEX_NLJ(orders) must not force the current inner customer table"
    );

    let QueryResponse::ResultSet(right_side) = engine
        .execute(
            "EXPLAIN SELECT /*+ INDEX_NLJ(customer) HASH_JOIN(orders, customer) ORDERED */ orders.id \
             FROM orders JOIN customer ON customer.id = orders.cust_id",
        )
        .unwrap()
    else {
        panic!("expected explain");
    };
    assert!(
        right_side.rows.iter().any(
            |row| matches!(&row[11], Value::String(extra) if extra.contains("Index nested loop"))
        ),
        "INDEX_NLJ(customer) should force the current inner customer table"
    );
}

#[test]
fn index_nlj_prefers_lower_fanout_equal_prefix_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE l (id INT PRIMARY KEY, fanout INT, selective INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE r (id INT PRIMARY KEY, fanout INT, selective INT, \
             INDEX idx_fanout(fanout), INDEX idx_selective(selective))",
        )
        .unwrap();
    let l_values = (1..=100)
        .map(|i| format!("({i}, 1, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    let r_values = l_values.clone();
    engine
        .execute(&format!("INSERT INTO l VALUES {l_values}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO r VALUES {r_values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE l").unwrap();
    engine.execute("ANALYZE TABLE r").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN SELECT /*+ ORDERED INDEX_NLJ(r) */ l.id \
             FROM l JOIN r ON r.fanout = l.fanout AND r.selective = l.selective",
        )
        .unwrap()
    else {
        panic!("expected explain");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        explain_text.contains("idx_selective"),
        "expected lower-fanout index for equal-prefix INLJ candidate: {explain_text}"
    );
}

#[test]
fn explain_index_nlj_batch_eligible_for_residual_range_on_non_indexed_column() {
    // A range predicate on a *non-indexed* inner column is only a
    // residual filter: it never becomes an index range bound, so the
    // batched probe still applies. TREE omits this default state, while an
    // actual indexed range would be surfaced as the exceptional
    // `batch: off(range)` state.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE l (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE r (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (1..=100)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO l VALUES {values}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO r VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE l").unwrap();
    engine.execute("ANALYZE TABLE r").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=TREE SELECT /*+ ORDERED INDEX_NLJ(r) */ l.id \
             FROM l JOIN r ON r.id = l.k WHERE r.v > 5",
        )
        .unwrap()
    else {
        panic!("expected explain");
    };
    let explain_text = explain
        .rows
        .iter()
        .flatten()
        .map(value_to_string)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        explain_text.contains("Index nested loop join"),
        "expected an index nested loop join: {explain_text}"
    );
    assert!(!explain_text.contains("batch:"), "{explain_text}");
    assert!(
        !explain_text.contains("parallel_frontier:"),
        "{explain_text}"
    );
    assert!(
        !explain_text.contains("parallel_statistics:"),
        "{explain_text}"
    );
    assert!(!explain_text.contains("probe_task_rows:"), "{explain_text}");
}

#[test]
fn hint_optimizer_hint_unknown_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let result = engine.execute("SELECT /*+ UNKNOWN_HINT(t) */ * FROM t WHERE a = 10");
    assert!(
        result.is_ok(),
        "Unknown optimizer hint should be silently ignored"
    );
}

#[test]
fn explain_format_tree_with_simple_cte() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE WITH t AS (SELECT * FROM orders) SELECT * FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(explain.columns.len(), 1);
    assert_eq!(explain.columns[0].name, "EXPLAIN");
    assert_eq!(explain.rows.len(), 1);
    if let Value::String(text) = &explain.rows[0][0] {
        assert!(
            text.contains("CTE scan"),
            "tree output should contain CTE scan: {text}"
        );
        assert!(
            text.contains("Table scan on orders"),
            "tree should show the CTE's inner plan: {text}"
        );
    } else {
        panic!("expected string");
    }
}

/// The CteScan node must inherit the CTE body plan's estimated row count
/// instead of collapsing to `rows=0`. During EXPLAIN no materialisation
/// happens, so the estimate has to flow from the planned body into the
/// registry stub the body planner reads.
#[test]
fn explain_format_tree_cte_scan_inherits_body_row_estimate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    for i in 1..=20 {
        engine
            .execute(&format!("INSERT INTO orders VALUES ({i}, {})", i * 10))
            .unwrap();
    }

    let tree = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE WITH t AS (SELECT * FROM orders) SELECT * FROM t",
    );
    let cte_line = tree
        .lines()
        .find(|l| l.contains("CTE scan on"))
        .unwrap_or_else(|| panic!("tree should contain a CTE scan: {tree}"));
    assert!(
        cte_line.contains("rows=20"),
        "CTE scan should inherit the body's 20-row estimate, got: {cte_line}\nfull tree:\n{tree}"
    );
    assert!(
        !cte_line.contains("rows=0"),
        "CTE scan must not collapse to rows=0: {cte_line}"
    );
}

#[test]
fn explain_format_tree_with_cte_in_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_name VARCHAR(50))")
        .unwrap();
    engine
        .execute("CREATE TABLE lineitem (l_suppkey INT, l_amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO supplier VALUES (1, 'Acme')")
        .unwrap();
    engine
        .execute("INSERT INTO lineitem VALUES (1, 500)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=TREE WITH revenue AS (\
                SELECT l_suppkey AS supplier_no, SUM(l_amount) AS total \
                FROM lineitem GROUP BY l_suppkey\
            ) SELECT s_suppkey, s_name, total \
              FROM supplier INNER JOIN revenue ON s_suppkey = supplier_no",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    if let Value::String(text) = &explain.rows[0][0] {
        assert!(
            text.contains("CTE scan"),
            "tree output should contain CTE scan: {text}"
        );
        assert!(
            text.contains("Nested loop join") || text.contains("Hash join"),
            "tree should show join: {text}"
        );
    } else {
        panic!("expected string");
    }
}

/// TREE join labels must surface the outer-join flavour (left/right/full),
/// not collapse every non-semi/anti join to a bare "... join". Inner joins
/// keep the bare label.
#[test]
fn explain_format_tree_shows_outer_join_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 1), (2, 2)")
        .unwrap();
    engine.execute("INSERT INTO b VALUES (1, 1)").unwrap();

    let left = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT * FROM a LEFT JOIN b ON a.k = b.k",
    );
    assert!(
        left.contains("left join"),
        "LEFT JOIN tree should label the join as a left join:\n{left}"
    );

    // Inner join keeps the bare label (no spurious "left/right/full").
    let inner = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT * FROM a INNER JOIN b ON a.k = b.k",
    );
    assert!(
        !inner.contains("left join")
            && !inner.contains("right join")
            && !inner.contains("full join"),
        "INNER JOIN tree must keep the bare join label:\n{inner}"
    );
}

fn setup_q40_join_shape(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE catalog_sales (\
             cs_order_number INT, \
             cs_item_sk INT, \
             cs_warehouse_sk INT, \
             cs_sold_date_sk INT, \
             cs_sales_price DOUBLE)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE catalog_returns (\
             cr_order_number INT, \
             cr_item_sk INT, \
             cr_refunded_cash DOUBLE)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE warehouse (w_warehouse_sk INT PRIMARY KEY, w_state CHAR(2))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE item (\
             i_item_sk INT PRIMARY KEY, \
             i_item_id VARCHAR(20), \
             i_current_price DOUBLE)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_date DATE)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO catalog_sales VALUES \
             (1, 10, 1, 100, 100.0), \
             (2, 20, 2, 101, 200.0), \
             (3, 30, 1, 100, 300.0)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO catalog_returns VALUES (1, 10, 5.0)")
        .unwrap();
    engine
        .execute("INSERT INTO warehouse VALUES (1, 'CA'), (2, 'NY')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO item VALUES \
             (10, 'item-10', 1.10), \
             (20, 'item-20', 1.20), \
             (30, 'item-30', 9.99)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO date_dim VALUES (100, '2000-05-01'), (101, '2000-06-10')")
        .unwrap();
    for table in [
        "catalog_sales",
        "catalog_returns",
        "warehouse",
        "item",
        "date_dim",
    ] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }
}

fn q40_join_shape_sql(prefix: &str, from_tail: &str) -> String {
    format!(
        "SELECT {prefix} w_state, i_item_id, \
         SUM(CASE WHEN d_date < CAST('2000-05-30' AS DATE) \
                  THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_before, \
         SUM(CASE WHEN d_date >= CAST('2000-05-30' AS DATE) \
                  THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_after \
         FROM catalog_sales LEFT OUTER JOIN catalog_returns ON \
              (cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk) \
              {from_tail} \
         WHERE i_current_price BETWEEN 0.99 AND 1.49 \
           AND i_item_sk = cs_item_sk \
           AND cs_warehouse_sk = w_warehouse_sk \
           AND cs_sold_date_sk = d_date_sk \
           AND d_date BETWEEN (CAST('2000-05-30' AS DATE) - INTERVAL 30 DAY) \
                          AND (CAST('2000-05-30' AS DATE) + INTERVAL 30 DAY) \
         GROUP BY w_state, i_item_id \
         ORDER BY w_state, i_item_id"
    )
}

fn assert_no_unconditional_nested_loop(tree: &str) {
    assert!(
        !tree
            .lines()
            .any(|line| line.contains("Nested loop") && !line.contains("condition:")),
        "unexpected unconditional nested loop in plan:\n{tree}"
    );
}

#[test]
fn outer_join_comma_tail_consumes_where_join_predicates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q40_join_shape(&engine);

    let sql = q40_join_shape_sql("", ", warehouse, item, date_dim");
    let tree = explain_tree_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    assert_no_unconditional_nested_loop(&tree);
    assert!(
        tree.contains("Hash join")
            || tree.contains("Index nested loop")
            || tree.contains("condition:"),
        "tail joins should receive WHERE join predicates:\n{tree}"
    );

    let QueryResponse::ResultSet(result) = engine.execute(&sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 2, "{:?}", result.rows);
    assert_eq!(result.rows[0][0], Value::String("CA".into()));
    assert_eq!(result.rows[0][1], Value::String("item-10".into()));
    assert_eq!(result.rows[1][0], Value::String("NY".into()));
    assert_eq!(result.rows[1][1], Value::String("item-20".into()));
}

#[test]
fn outer_join_cross_tail_and_hints_keep_extracted_condition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_q40_join_shape(&engine);

    let sql = q40_join_shape_sql(
        "/*+ ORDERED NO_HASH_JOIN(catalog_sales, warehouse) \
         NO_HASH_JOIN(catalog_sales, item) */",
        "CROSS JOIN warehouse CROSS JOIN item CROSS JOIN date_dim",
    );
    let tree = explain_tree_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    assert_no_unconditional_nested_loop(&tree);
    assert!(
        tree.contains("condition:"),
        "NO_HASH_JOIN/ORDERED path should keep a join condition instead of a cartesian join:\n{tree}"
    );
}

#[test]
fn outer_join_tail_extraction_does_not_move_right_filter_into_on() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE l (id INT PRIMARY KEY, k INT, d_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE r (id INT PRIMARY KEY, k INT, flag INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1, 10), (2, 2, 10), (3, 3, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 1, 1), (2, 2, 0)")
        .unwrap();
    engine.execute("INSERT INTO d VALUES (10), (20)").unwrap();
    for table in ["l", "r", "d"] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let sql = "SELECT COUNT(*) \
               FROM l LEFT JOIN r ON l.k = r.k, d \
               WHERE l.d_id = d.id AND r.flag = 1";
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows[0][0],
        Value::Int(1),
        "right-side WHERE filter must not be moved into LEFT JOIN ON"
    );
    let tree = explain_tree_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    assert_no_unconditional_nested_loop(&tree);
}

#[test]
fn outer_join_tail_extraction_does_not_use_or_as_join_edge() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE l (id INT PRIMARY KEY, k INT, d_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE r (id INT PRIMARY KEY, k INT, flag INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 1, 10), (2, 2, 10), (3, 3, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 1, 1), (2, 2, 0)")
        .unwrap();
    engine.execute("INSERT INTO d VALUES (10), (20)").unwrap();
    for table in ["l", "r", "d"] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let sql = "SELECT /*+ ORDERED NO_HASH_JOIN(l, d) */ COUNT(*) \
               FROM l LEFT JOIN r ON l.k = r.k, d \
               WHERE l.d_id = d.id AND (r.flag = 1 OR d.id = 20)";
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(2));

    let tree = explain_tree_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    assert_no_unconditional_nested_loop(&tree);
    assert!(
        tree.contains("l.d_id=d.id"),
        "extractable equality should still anchor the tail join while OR remains residual:\n{tree}"
    );
}

/// A LEFT join must never be estimated below its preserved (left) side:
/// every left row is emitted at least once, even when matches are sparse.
/// Before the fix the CBO used the inner-join match estimate verbatim, so a
/// large left side with few matches collapsed to a tiny row count.
#[test]
fn cbo_left_join_estimate_respects_preserved_side_floor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE small (id INT PRIMARY KEY, k INT)")
        .unwrap();
    // 200 left rows; only a handful of right rows that match very few keys,
    // so the inner-join estimate is small relative to the left side.
    for i in 1..=200 {
        engine
            .execute(&format!("INSERT INTO big VALUES ({i}, {i})"))
            .unwrap();
    }
    for i in 1..=3 {
        engine
            .execute(&format!("INSERT INTO small VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE big").ok();
    engine.execute("ANALYZE TABLE small").ok();

    let tree = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT * FROM big LEFT JOIN small ON big.k = small.k",
    );
    // Find the join line and parse its `rows=N` estimate.
    let join_line = tree
        .lines()
        .find(|l| l.contains("left join"))
        .unwrap_or_else(|| panic!("expected a left join in the tree:\n{tree}"));
    let rows = join_line
        .split("rows=")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|n| n.parse::<i64>().ok())
        .unwrap_or_else(|| panic!("could not parse rows= from: {join_line}"));
    assert!(
        rows >= 200,
        "LEFT join estimate must be >= left rows (200), got {rows} in: {join_line}\n{tree}"
    );
}

/// Parse the `rows=N` estimate off the first tree line matching `needle`.
fn join_rows_estimate(tree: &str, needle: &str) -> i64 {
    let line = tree
        .lines()
        .find(|l| l.contains(needle))
        .unwrap_or_else(|| panic!("expected `{needle}` in tree:\n{tree}"));
    line.split("rows=")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|n| n.parse::<i64>().ok())
        .unwrap_or_else(|| panic!("could not parse rows= from: {line}"))
}

/// A RIGHT join must never be estimated below its preserved (right) side.
#[test]
fn cbo_right_join_estimate_respects_preserved_side_floor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE small (id INT PRIMARY KEY, k INT)")
        .unwrap();
    for i in 1..=200 {
        engine
            .execute(&format!("INSERT INTO big VALUES ({i}, {i})"))
            .unwrap();
    }
    for i in 1..=3 {
        engine
            .execute(&format!("INSERT INTO small VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE big").ok();
    engine.execute("ANALYZE TABLE small").ok();

    // Preserved side is `big` (the RIGHT operand), 200 rows, sparse matches.
    let tree = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT * FROM small RIGHT JOIN big ON small.k = big.k",
    );
    let rows = join_rows_estimate(&tree, "right join");
    assert!(
        rows >= 200,
        "RIGHT join estimate must be >= preserved (right) rows (200), got {rows}\n{tree}"
    );
}

/// A FULL join must never be estimated below the larger of its two sides.
/// FULL routes to the in-memory join at execution, but EXPLAIN still plans
/// it (as a nested loop), so the displayed estimate must respect the floor.
#[test]
fn cbo_full_join_estimate_respects_both_sides_floor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE small (id INT PRIMARY KEY, k INT)")
        .unwrap();
    for i in 1..=200 {
        engine
            .execute(&format!("INSERT INTO big VALUES ({i}, {i})"))
            .unwrap();
    }
    for i in 1..=3 {
        engine
            .execute(&format!("INSERT INTO small VALUES ({i}, {i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE big").ok();
    engine.execute("ANALYZE TABLE small").ok();

    let tree = explain_tree_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT * FROM big FULL JOIN small ON big.k = small.k",
    );
    let rows = join_rows_estimate(&tree, "full join");
    assert!(
        rows >= 200,
        "FULL join estimate must be >= max(left, right) rows (200), got {rows}\n{tree}"
    );
}

#[test]
fn explain_format_tree_with_cte_subquery_referencing_cte() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_name VARCHAR(50), s_address VARCHAR(100), s_phone VARCHAR(20))")
        .unwrap();
    engine
        .execute("CREATE TABLE lineitem (l_suppkey INT, l_extendedprice DECIMAL(10,2), l_discount DECIMAL(3,2), l_shipdate DATE)")
        .unwrap();
    engine
        .execute("INSERT INTO supplier VALUES (1, 'Supplier#1', '123 Main St', '555-0001')")
        .unwrap();
    engine
        .execute("INSERT INTO lineitem VALUES (1, 1000.00, 0.05, '1996-01-15')")
        .unwrap();

    let result = engine.execute(
        "EXPLAIN FORMAT=TREE WITH revenue AS (\
            SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue \
            FROM lineitem \
            WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01' \
            GROUP BY l_suppkey\
        ) SELECT s_suppkey, s_name, s_address, s_phone, total_revenue \
          FROM supplier INNER JOIN revenue ON s_suppkey = supplier_no \
          WHERE total_revenue = (SELECT MAX(total_revenue) FROM revenue) \
          ORDER BY s_suppkey",
    );
    match &result {
        Ok(QueryResponse::ResultSet(explain)) => {
            if let Value::String(text) = &explain.rows[0][0] {
                assert!(
                    text.contains("CTE scan"),
                    "tree should contain CTE scan: {text}"
                );
            }
        }
        Err(e) => panic!("EXPLAIN FORMAT=TREE with CTE failed: {e:?}"),
        _ => panic!("expected result set"),
    }
}

#[test]
fn explain_format_json_with_cte() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, price INT)")
        .unwrap();
    engine.execute("INSERT INTO items VALUES (1, 50)").unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=JSON WITH cheap AS (SELECT * FROM items WHERE price < 100) \
             SELECT * FROM cheap",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    if let Value::String(text) = &explain.rows[0][0] {
        assert!(
            text.contains("cte_scan"),
            "JSON should contain cte_scan: {text}"
        );
        assert!(
            text.contains("materialized_plan"),
            "JSON should contain materialized_plan: {text}"
        );
    } else {
        panic!("expected string");
    }
}

// ───── Hint Warning Tests ─────

fn explain_extra(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(explain) = engine.execute(sql).unwrap() else {
        panic!("expected explain result set");
    };
    match &explain.rows[0][11] {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => panic!("unexpected Extra column: {other:?}"),
    }
}

#[test]
fn explain_reports_unknown_hint_name() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();

    let extra = explain_extra(&engine, "EXPLAIN SELECT /*+ PARALEL(4) */ * FROM t");
    assert!(
        extra.contains("hint warning:"),
        "Extra should carry hint warning prefix, got: {extra}"
    );
    assert!(
        extra.contains("unknown hint \"PARALEL\""),
        "Extra should flag unknown hint name, got: {extra}"
    );
}

#[test]
fn explain_reports_parallel_invalid_degree() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();

    let extra = explain_extra(&engine, "EXPLAIN SELECT /*+ PARALLEL(abc) */ * FROM t");
    assert!(
        extra.contains("PARALLEL: invalid degree \"abc\""),
        "Extra should flag invalid PARALLEL degree, got: {extra}"
    );
}

#[test]
fn explain_reports_parallel_no_parallel_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();

    let extra = explain_extra(
        &engine,
        "EXPLAIN SELECT /*+ PARALLEL(4) NO_PARALLEL */ * FROM t",
    );
    assert!(
        extra.contains("PARALLEL: conflicting hint ignored; NO_PARALLEL wins"),
        "Extra should flag PARALLEL/NO_PARALLEL conflict, got: {extra}"
    );
}

#[test]
fn explain_reports_no_index_missing_object() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();

    let extra = explain_extra(
        &engine,
        "EXPLAIN SELECT /*+ NO_INDEX(t, idx_missing) */ * FROM t WHERE a = 1",
    );
    assert!(
        extra.contains("NO_INDEX: index \"idx_missing\" not found on \"t\""),
        "Extra should flag missing index, got: {extra}"
    );

    let extra2 = explain_extra(
        &engine,
        "EXPLAIN SELECT /*+ NO_INDEX(y, idx_a) */ * FROM t WHERE a = 1",
    );
    assert!(
        extra2.contains("NO_INDEX: table \"y\" not referenced in FROM"),
        "Extra should flag missing table, got: {extra2}"
    );
}

#[test]
fn explain_reports_parallel_disabled_env() {
    let _guard = super::test_env_lock();
    let prior_disable = std::env::var("MYDB_PARALLEL_DISABLE").ok();
    let _restore_disable = restore_env("MYDB_PARALLEL_DISABLE", prior_disable);
    std::env::set_var("MYDB_PARALLEL_DISABLE", "1");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();

    let extra = explain_extra(&engine, "EXPLAIN SELECT /*+ PARALLEL(4) */ * FROM t");
    assert!(
        extra.contains("PARALLEL ignored: MYDB_PARALLEL_DISABLE is set"),
        "Extra should flag PARALLEL disabled by env, got: {extra}"
    );
}

#[test]
fn explain_tree_reports_hint_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ PARALEL(4) */ * FROM t",
    );
    assert!(
        text.starts_with("hint warning: unknown hint \"PARALEL\""),
        "TREE output should lead with hint warning, got: {text}"
    );

    // Multi-warning case: conflict + missing index.
    let text2 = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ PARALLEL(4) NO_PARALLEL */ * FROM t",
    );
    assert!(
        text2.contains("hint warning: PARALLEL: conflicting hint ignored; NO_PARALLEL wins"),
        "TREE output should surface conflict warning, got: {text2}"
    );
}

#[test]
fn explain_json_reports_hint_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=JSON SELECT /*+ NO_INDEX(t, idx_missing) */ * FROM t WHERE a = 1",
    );
    assert!(
        text.contains("\"hint_warnings\":"),
        "JSON should carry hint_warnings field, got: {text}"
    );
    assert!(
        text.contains("NO_INDEX: index \\\"idx_missing\\\" not found on \\\"t\\\""),
        "JSON should escape and embed warning message, got: {text}"
    );
}

#[test]
fn optimizer_hint_use_index_limits_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, INDEX idx_a(a), INDEX idx_b(b))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ USE_INDEX(t idx_b) */ * FROM t WHERE b = 200")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.rows[0][6], Value::String("idx_b".into()));
    let extra = match &explain.rows[0][11] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    assert!(
        extra.contains("USE_INDEX(idx_b)"),
        "Extra should mention USE_INDEX, got: {extra}"
    );
}

#[test]
fn optimizer_hint_use_index_keeps_range_scan_over_seqscan_cost_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, d DATE, v INT, INDEX idx_d(d))")
        .unwrap();
    let rows = (1..=6_000)
        .map(|i| {
            let d = if i % 2 == 0 {
                "1992-01-01"
            } else {
                "1992-01-15"
            };
            format!("({i}, '{d}', {})", i % 97)
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO t VALUES {rows}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ USE_INDEX(t idx_d) */ COUNT(*), MAX(v) FROM t WHERE d < DATE '2000-01-01'",
    );
    assert!(
        text.contains("Index range scan on t using idx_d")
            || text.contains("Parallel index range scan on t using idx_d"),
        "USE_INDEX should keep the usable range scan instead of falling back to a table scan: {text}"
    );
    assert!(
        !text.contains("table scan on t"),
        "USE_INDEX range access should not be planned as table scan: {text}"
    );
}

#[test]
fn optimizer_hint_use_index_missing_index_warns_and_returns_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT /*+ USE_INDEX(t missing_idx) */ * FROM t WHERE a = 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));

    let extra = explain_extra(
        &engine,
        "EXPLAIN SELECT /*+ USE_INDEX(t missing_idx) */ * FROM t WHERE a = 20",
    );
    assert!(
        extra.contains("USE_INDEX: index \"missing_idx\" not found on \"t\""),
        "Extra should flag missing USE_INDEX index, got: {extra}"
    );
}

#[test]
fn optimizer_hint_conflict_uses_first_hint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, INDEX idx_a(a))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT /*+ USE_INDEX(t idx_a) NO_INDEX(t idx_a) */ * FROM t WHERE a = 20")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let key = match &explain.rows[0][6] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    assert_eq!(key, "idx_a", "first USE_INDEX should win the conflict");
}

#[test]
fn optimizer_hint_use_index_surfaces_in_parallel_tree_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, d DATE, v INT, INDEX idx_d(d))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, '1999-01-01', 10), (2, '1999-02-01', 20)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ PARALLEL(2) USE_INDEX(t idx_d) */ MAX(id) FROM t WHERE v > 0",
    );
    assert!(
        text.contains("Parallel table scan"),
        "expected forced parallel table scan, got: {text}"
    );
    assert!(
        text.contains("hint: USE_INDEX(idx_d)"),
        "TREE output should surface USE_INDEX hint note, got: {text}"
    );
}

#[test]
fn optimizer_hint_use_index_steers_inlj_inner_side() {
    // The /*+ USE_INDEX(lineitem, idx_lineitem_orderkey) */ optimizer
    // hint must apply to the inner side of an Index Nested Loop Join,
    // not just to standalone scans. Before this was wired through, the
    // INLJ index picker tied PRIMARY(l_orderkey,l_linenumber) against
    // idx_lineitem_orderkey(l_orderkey) on equality probe and silently
    // kept PRIMARY (it sorts first in the index list).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY, \
             o_orderdate DATE NOT NULL, \
             INDEX idx_orders_orderdate(o_orderdate))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT NOT NULL, \
             l_linenumber INT NOT NULL, \
             l_quantity INT NOT NULL, \
             PRIMARY KEY (l_orderkey, l_linenumber), \
             INDEX idx_lineitem_orderkey(l_orderkey))",
        )
        .unwrap();
    let orders = (1..=400)
        .map(|i| {
            let month = ((i - 1) % 12) + 1;
            format!("({i}, '1993-{month:02}-15')")
        })
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO orders VALUES {orders}"))
        .unwrap();
    let lineitems = (1..=400)
        .flat_map(|i| (1..=4).map(move |ln| format!("({i}, {ln}, {})", i + ln)))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO lineitem VALUES {lineitems}"))
        .unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    // Force INLJ via INDEX_NLJ so the test isolates the inner-side
    // index choice — at this scale CBO would otherwise pick HashJoin
    // for the same shape, which would hide the bug we're guarding.
    let q = "SELECT /*+ INDEX_NLJ(orders, lineitem) \
                          USE_INDEX(lineitem, idx_lineitem_orderkey) */ \
              COUNT(*) FROM orders, lineitem \
             WHERE o_orderkey = l_orderkey \
               AND o_orderdate >= '1993-10-01' \
               AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH";
    let tree = explain_single_text(&engine, &format!("EXPLAIN FORMAT=TREE {q}"));
    assert!(
        tree.contains("Index nested loop join"),
        "expected INLJ in plan: {tree}"
    );
    assert!(
        tree.contains("Index lookup on lineitem using idx_lineitem_orderkey"),
        "USE_INDEX hint must steer the inner-side index choice: {tree}"
    );
    assert!(
        !tree.contains("Index lookup on lineitem using PRIMARY"),
        "PRIMARY must not be chosen when USE_INDEX names a different index: {tree}"
    );

    // NO_INDEX should also reach the INLJ inner side: excluding the
    // narrower single-column index forces the planner back to PRIMARY.
    let tree_no_idx = explain_single_text(
        &engine,
        &format!(
            "EXPLAIN FORMAT=TREE {}",
            q.replace(
                "USE_INDEX(lineitem, idx_lineitem_orderkey)",
                "NO_INDEX(lineitem, idx_lineitem_orderkey)",
            )
        ),
    );
    assert!(
        !tree_no_idx.contains("Index lookup on lineitem using idx_lineitem_orderkey"),
        "NO_INDEX hint must steer INLJ away from the named inner index: {tree_no_idx}"
    );
    assert!(
        tree_no_idx.contains("Index lookup on lineitem using PRIMARY"),
        "with idx_lineitem_orderkey excluded, INLJ should fall back to PRIMARY: {tree_no_idx}"
    );

    // Without any USE_INDEX hint (still forcing INLJ via INDEX_NLJ),
    // the narrower single-column index should win the tie against the
    // PRIMARY composite on `l_orderkey = ?` — same per-key cardinality
    // but smaller leaves.
    let tree_unhinted = explain_single_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ INDEX_NLJ(orders, lineitem) */ \
              COUNT(*) FROM orders, lineitem \
             WHERE o_orderkey = l_orderkey \
               AND o_orderdate >= '1993-10-01' \
               AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH",
    );
    assert!(
        tree_unhinted.contains("Index nested loop join"),
        "INDEX_NLJ hint should force INLJ: {tree_unhinted}"
    );
    assert!(
        tree_unhinted.contains("Index lookup on lineitem using idx_lineitem_orderkey"),
        "Narrower single-column index should beat PRIMARY composite on a tie: {tree_unhinted}"
    );

    // Result correctness sanity: hint-driven and unhinted plans must
    // return identical counts.
    let count_hinted = match engine.execute(q).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows[0][0].clone(),
        _ => panic!("expected result set"),
    };
    let count_unhinted = match engine
        .execute(
            "SELECT COUNT(*) FROM orders, lineitem \
                 WHERE o_orderkey = l_orderkey \
                   AND o_orderdate >= '1993-10-01' \
                   AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH",
        )
        .unwrap()
    {
        QueryResponse::ResultSet(rs) => rs.rows[0][0].clone(),
        _ => panic!("expected result set"),
    };
    assert_eq!(
        count_hinted, count_unhinted,
        "INLJ index choice must not change result"
    );
}

#[test]
fn explain_tree_no_hint_warning_when_clean() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();

    let text = explain_single_text(&engine, "EXPLAIN FORMAT=TREE SELECT * FROM t WHERE a = 1");
    assert!(
        !text.contains("hint warning:"),
        "TREE output should not contain hint warning when none, got: {text}"
    );

    let json = explain_single_text(&engine, "EXPLAIN FORMAT=JSON SELECT * FROM t WHERE a = 1");
    assert!(
        !json.contains("hint_warnings"),
        "JSON should not include hint_warnings when none, got: {json}"
    );
}

#[test]
fn explain_no_hint_warning_when_clean() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE a = 1")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    for (i, row) in explain.rows.iter().enumerate() {
        let extra = match &row[11] {
            Value::String(s) => s.clone(),
            _ => String::new(),
        };
        assert!(
            !extra.contains("hint warning:"),
            "row {i} should not carry hint warning, got: {extra}"
        );
    }
}

#[test]
fn explain_analyze_with_simple_cte_reports_per_block_timings() {
    // EXPLAIN ANALYZE on a WITH-CTE returns the same structural rows as
    // plain EXPLAIN plus two new columns (`actual_rows`, `actual_time_ms`).
    // The first row of each CTE block carries that CTE's materialisation
    // stats; the first row of the body block carries the body's stats.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100), (2, 200), (3, 50)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            // `big` is referenced twice so it stays a materialised CTE block
            // (single-reference CTEs now inline into the body — see
            // `explain_single_use_cte_inlines_matching_execution`).
            "EXPLAIN ANALYZE FORMAT=TRADITIONAL WITH big AS (SELECT * FROM orders WHERE amount > 75) \
             SELECT a.id FROM big a JOIN big b ON a.id = b.id ORDER BY a.id",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };

    assert_eq!(explain.columns.last().unwrap().name, "actual_rows_total");
    assert_eq!(explain.columns[12].name, "actual_rows");

    // Find the CTE block (select_type starts with "CTE") and verify its
    // first row carries the materialised row count (2) and a non-negative
    // time. Then find the body's first row (select_type = "PRIMARY") and
    // verify the same shape (body returns 2 rows).
    let select_type_idx = 1;
    let actual_rows_idx = 16;
    let actual_ms_idx = 15;
    let mut cte_seen = false;
    let mut body_seen = false;
    for row in &explain.rows {
        let label = match &row[select_type_idx] {
            Value::String(s) => s.clone(),
            _ => String::new(),
        };
        if !cte_seen && label.contains("CTE") {
            cte_seen = true;
            assert_eq!(row[actual_rows_idx], Value::Int(2));
            let Value::Double(ms) = row[actual_ms_idx] else {
                panic!(
                    "CTE actual_time_ms must be Double, got {:?}",
                    row[actual_ms_idx]
                );
            };
            assert!(
                ms >= 0.0 && ms < 5_000.0,
                "CTE elapsed_ms out of range: {ms}"
            );
        }
        if !body_seen && label == "PRIMARY" {
            body_seen = true;
            assert_eq!(row[actual_rows_idx], Value::Int(2));
            let Value::Double(ms) = row[actual_ms_idx] else {
                panic!(
                    "BODY actual_time_ms must be Double, got {:?}",
                    row[actual_ms_idx]
                );
            };
            assert!(
                ms >= 0.0 && ms < 5_000.0,
                "BODY elapsed_ms out of range: {ms}"
            );
        }
    }
    assert!(cte_seen, "expected a CTE row in EXPLAIN ANALYZE output");
    assert!(
        body_seen,
        "expected a PRIMARY row in EXPLAIN ANALYZE output"
    );

    // Non-first rows of each block must hold NULL placeholders so the
    // column shape stays uniform across rows.
    let total_cols = explain.columns.len();
    for row in &explain.rows {
        assert_eq!(
            row.len(),
            total_cols,
            "row must have exactly {} columns",
            total_cols
        );
    }
}

#[test]
fn explain_analyze_with_cte_in_join_reports_timings() {
    // Mirrors TPC-H Q15 (revenue CTE joined with supplier and used in a
    // subquery). Verifies EXPLAIN ANALYZE works end-to-end on a non-trivial
    // WITH-CTE that joins the CTE in the body.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE supplier (\
                s_suppkey INT PRIMARY KEY, \
                s_name VARCHAR(50), \
                s_address VARCHAR(100), \
                s_phone VARCHAR(20))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
                l_suppkey INT, \
                l_extendedprice DECIMAL(10,2), \
                l_discount DECIMAL(3,2), \
                l_shipdate DATE)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO supplier VALUES \
                (1, 'Supplier#1', '123 Main St', '555-0001'), \
                (2, 'Supplier#2', '456 Oak Ave', '555-0002')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO lineitem VALUES \
                (1, 1000.00, 0.05, '1996-01-15'), \
                (1, 2000.00, 0.10, '1996-02-15'), \
                (2,  500.00, 0.00, '1996-03-15')",
        )
        .unwrap();

    let result = engine.execute(
        "EXPLAIN ANALYZE FORMAT=TRADITIONAL WITH revenue AS (\
            SELECT l_suppkey AS supplier_no, \
                   SUM(l_extendedprice * (1 - l_discount)) AS total_revenue \
            FROM lineitem \
            WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01' \
            GROUP BY l_suppkey\
        ) \
        SELECT s_suppkey, s_name, s_address, s_phone, total_revenue \
        FROM supplier INNER JOIN revenue ON s_suppkey = supplier_no \
        WHERE total_revenue = (SELECT MAX(total_revenue) FROM revenue) \
        ORDER BY s_suppkey",
    );
    let QueryResponse::ResultSet(explain) = result.unwrap() else {
        panic!("expected explain result set");
    };

    assert_eq!(explain.columns.last().unwrap().name, "actual_rows_total");
    assert_eq!(explain.columns[12].name, "actual_rows");

    // The CTE block must report the supplier count it materialised (2
    // suppliers with shipments in the window).
    let select_type_idx = 1;
    let actual_rows_idx = 16;
    let cte_rows: Vec<&Vec<Value>> = explain
        .rows
        .iter()
        .filter(|r| matches!(&r[select_type_idx], Value::String(s) if s.contains("CTE")))
        .collect();
    assert!(!cte_rows.is_empty(), "expected at least one CTE row");
    assert_eq!(
        cte_rows[0][actual_rows_idx],
        Value::Int(2),
        "CTE block's first row should carry the materialised row count"
    );
}

#[test]
fn explain_analyze_with_recursive_cte_reports_timings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN ANALYZE FORMAT=TRADITIONAL WITH RECURSIVE seq(n) AS (\
                SELECT 1 \
                UNION ALL \
                SELECT n + 1 FROM seq WHERE n < 5\
            ) SELECT n FROM seq",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };

    assert_eq!(explain.columns.last().unwrap().name, "actual_rows_total");
    let select_type_idx = 1;
    let actual_rows_idx = 16;
    let recursive_block: Vec<&Vec<Value>> = explain
        .rows
        .iter()
        .filter(|r| matches!(&r[select_type_idx], Value::String(s) if s == "RECURSIVE_CTE"))
        .collect();
    assert!(
        !recursive_block.is_empty(),
        "expected at least one RECURSIVE_CTE row"
    );
    assert_eq!(
        recursive_block[0][actual_rows_idx],
        Value::Int(5),
        "recursive CTE should have materialised 5 rows (n=1..=5)"
    );
}

#[test]
fn explain_analyze_with_cte_tree_attaches_totals_to_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100), (2, 200)")
        .unwrap();

    let QueryResponse::ResultSet(explain) = engine
        .execute(
            // `t` referenced twice → stays a materialised CTE block with a
            // materialized CTE node (single-reference CTEs inline into body).
            "EXPLAIN ANALYZE FORMAT=TREE WITH t AS (SELECT * FROM orders) \
             SELECT a.id FROM t a JOIN t b ON a.id = b.id",
        )
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    assert_eq!(explain.rows.len(), 1);
    if let Value::String(text) = &explain.rows[0][0] {
        assert!(
            !text.contains("-- CTE") && !text.contains("-- BODY"),
            "{text}"
        );
        assert!(
            text.contains("CTE scan") && text.contains("actual time="),
            "tree output should still contain the structural plan, got: {text}"
        );
    } else {
        panic!("expected string");
    }
}

/// P0: EXPLAIN ANALYZE FORMAT=TREE must annotate the operators *inside* a
/// CTE body — not just the per-CTE total — so users see the runtime shape
/// of the clauses within the WITH. Before P0 these lines rendered
/// `(never executed)` because the executed tree's node pointers didn't
/// match the formatter's display tree.
#[test]
fn explain_analyze_with_cte_tree_annotates_inner_operators() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100), (2, 200), (3, 50), (4, 300)")
        .unwrap();

    // A plain filtered scan inside the CTE fully recurses through the plan
    // walker, so its base scan records per-node stats (unlike a *fused*
    // aggregate leaf, which scans the heap directly — a pre-existing
    // instrumentation gap shared with non-CTE top-level aggregates).
    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH t AS (SELECT id, amount FROM orders WHERE amount > 75) \
         SELECT id FROM t ORDER BY id",
    );

    let scan_line = text
        .lines()
        .find(|l| l.contains("scan on orders"))
        .unwrap_or_else(|| panic!("missing CTE-body scan line in:\n{text}"));
    assert!(
        scan_line.contains("actual time="),
        "CTE-body operator must be annotated with actual stats, got: {scan_line}\nfull:\n{text}"
    );
    assert!(
        !text.contains("(never executed)"),
        "no CTE-body operator should be (never executed):\n{text}"
    );
    // Body CteScan + body Sort + CTE-body scan → at least 3 suffixes.
    assert!(
        text.matches("actual time=").count() >= 3,
        "expected per-node stats across body and CTE interior:\n{text}"
    );
}

/// Unification: a single-reference CTE now inlines into the body for EXPLAIN
/// exactly as it does for execution — the shared
/// `apply_pre_materialization_rewrites` sequence runs in both paths. So the
/// displayed plan no longer carries a separate materialised `-- CTE` block;
/// the CTE body appears as an inlined derived subtree of the body plan,
/// carrying real per-operator stats, and matches what actually runs.
#[test]
fn explain_single_use_cte_inlines_matching_execution() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100), (2, 200), (3, 50)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH t AS (SELECT id FROM orders WHERE amount > 75) \
         SELECT id FROM t ORDER BY id",
    );
    // Inlined → no per-CTE materialisation block header.
    assert!(
        !text.contains("-- CTE t:"),
        "single-use CTE should inline (no per-CTE block), got:\n{text}"
    );
    // The CTE body's filtered scan is present inline in the body plan with
    // real stats — not a (never executed) stub.
    let scan_line = text
        .lines()
        .find(|l| l.contains("scan on orders"))
        .unwrap_or_else(|| panic!("inlined CTE-body scan missing:\n{text}"));
    assert!(
        text.contains("amount>75"),
        "CTE body filter should appear inlined in the body plan:\n{text}"
    );
    assert!(
        scan_line.contains("actual time="),
        "inlined scan must carry real stats:\n{text}"
    );

    // The displayed plan matches execution: the same query returns exactly the
    // rows the inlined plan describes.
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "WITH t AS (SELECT id FROM orders WHERE amount > 75) \
             SELECT id FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows.len(),
        2,
        "execution returns the 2 rows the inlined plan describes"
    );
}

/// Regression (TPC-DS q95 shape): traditional `EXPLAIN ANALYZE` of a WITH whose
/// body references the CTE inside a WHERE `IN`-subquery that JOINs a real table
/// with the CTE. The traditional analyze path must (a) build each CTE stub's
/// columns from its body projection — not the usually-absent explicit column
/// list — so `c.k` inside the subquery resolves, and (b) NOT inline the CTE,
/// since an inlined-derived reference sitting inside a subquery join cannot be
/// planned in isolation. Both failure modes previously surfaced as 1054
/// "unknown column" once the parallel-worker CTE registry fix let execution get
/// this far. The command must succeed and keep the CTE as its own block,
/// mirroring plain `EXPLAIN WITH`.
#[test]
fn explain_analyze_traditional_resolves_cte_in_where_subquery_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine.execute("CREATE TABLE ret (rk INT)").unwrap();
    engine
        .execute("INSERT INTO big VALUES (1,1),(2,2),(3,3),(4,4),(5,5)")
        .unwrap();
    engine
        .execute("INSERT INTO ret VALUES (1),(2),(3)")
        .unwrap();

    // The explicit traditional format keeps the tabular compatibility path.
    // `c` has no column list, so its columns must be inferred for `c.k`.
    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN ANALYZE FORMAT=TRADITIONAL WITH c AS (SELECT k FROM big) \
             SELECT b.id FROM big b \
             WHERE b.k IN (SELECT r.rk FROM ret r, c WHERE r.rk = c.k)",
        )
        .unwrap()
    else {
        panic!("expected explain result set (must not 1054)");
    };

    // Traditional analyze shape: five explicit per-loop/total columns.
    assert_eq!(explain.columns.last().unwrap().name, "actual_rows_total");
    assert_eq!(explain.columns[12].name, "actual_rows");
    // Not inlined: the CTE keeps its own block (select_type contains "CTE"),
    // matching plain EXPLAIN WITH.
    let select_type_idx = 1;
    let cte_block = explain
        .rows
        .iter()
        .any(|row| matches!(&row[select_type_idx], Value::String(s) if s.contains("CTE")));
    assert!(
        cte_block,
        "traditional EXPLAIN ANALYZE should keep the CTE as a non-inlined block: {:?}",
        explain.rows
    );

    // Sanity: the un-analyzed query returns the expected rows (k in {1,2,3}).
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "WITH c AS (SELECT k FROM big) \
             SELECT b.id FROM big b \
             WHERE b.k IN (SELECT r.rk FROM ret r, c WHERE r.rk = c.k) ORDER BY b.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<i64> = rs
        .rows
        .iter()
        .filter_map(|r| match r.first() {
            Some(Value::Int(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3], "rows where k in ret∩c = {{1,2,3}}");
}

/// Regression for TPC-DS Q23 under EXPLAIN ANALYZE FORMAT=TREE: the formatted
/// WITH-CTE planner must infer stub columns from CTE projections when the CTE
/// has no explicit column list. Otherwise body predicates such as
/// `cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)` plan against an
/// empty CTE schema and surface as 1054 unknown-column errors.
#[test]
fn explain_analyze_tree_tpcds_q23_infers_cte_projection_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE item (i_item_sk INT NOT NULL PRIMARY KEY, i_item_desc VARCHAR(64))",
        "CREATE TABLE date_dim (d_date_sk INT NOT NULL PRIMARY KEY, d_date DATE, d_year INT, d_moy INT)",
        "CREATE TABLE customer (c_customer_sk INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE store_sales (ss_item_sk INT NOT NULL, ss_sold_date_sk INT NOT NULL, ss_customer_sk INT, ss_quantity INT, ss_sales_price DECIMAL(7,2))",
        "CREATE TABLE catalog_sales (cs_item_sk INT, cs_bill_customer_sk INT, cs_sold_date_sk INT, cs_quantity INT, cs_list_price DECIMAL(7,2))",
        "CREATE TABLE web_sales (ws_item_sk INT, ws_bill_customer_sk INT, ws_sold_date_sk INT, ws_quantity INT, ws_list_price DECIMAL(7,2))",
        "INSERT INTO item VALUES (1,'alpha long description'),(2,'beta long description')",
        "INSERT INTO date_dim VALUES (1,'2000-01-01',2000,1),(2,'2001-01-01',2001,1)",
        "INSERT INTO customer VALUES (10),(20)",
        "INSERT INTO store_sales VALUES (1,1,10,10,10.00),(1,1,10,9,9.00),(2,2,20,1,1.00)",
        "INSERT INTO catalog_sales VALUES (1,10,1,2,5.00),(2,20,1,1,7.00)",
        "INSERT INTO web_sales VALUES (1,10,1,3,4.00),(2,20,1,1,8.00)",
    ] {
        engine.execute(sql).unwrap();
    }

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH frequent_ss_items AS ( \
           SELECT substr(i_item_desc,1,30) itemdesc, i_item_sk item_sk, d_date solddate, count(*) cnt \
           FROM store_sales, date_dim, item \
           WHERE ss_sold_date_sk = d_date_sk \
             AND ss_item_sk = i_item_sk \
             AND d_year IN (2000,2000+1,2000+2,2000+3) \
           GROUP BY substr(i_item_desc,1,30), i_item_sk, d_date \
           HAVING count(*) > 0), \
         max_store_sales AS ( \
           SELECT max(csales) tpcds_cmax \
           FROM (SELECT c_customer_sk, sum(ss_quantity*ss_sales_price) csales \
                 FROM store_sales, customer, date_dim \
                 WHERE ss_customer_sk = c_customer_sk \
                   AND ss_sold_date_sk = d_date_sk \
                   AND d_year IN (2000,2000+1,2000+2,2000+3) \
                 GROUP BY c_customer_sk) s), \
         best_ss_customer AS ( \
           SELECT c_customer_sk, sum(ss_quantity*ss_sales_price) ssales \
           FROM store_sales, customer \
           WHERE ss_customer_sk = c_customer_sk \
           GROUP BY c_customer_sk \
           HAVING sum(ss_quantity*ss_sales_price) > (95/100.0) * (SELECT * FROM max_store_sales)) \
         SELECT sum(sales) \
         FROM ( \
           SELECT cs_quantity*cs_list_price sales \
           FROM catalog_sales, date_dim \
           WHERE d_year = 2000 \
             AND d_moy = 1 \
             AND cs_sold_date_sk = d_date_sk \
             AND cs_item_sk IN (SELECT item_sk FROM frequent_ss_items) \
             AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer) \
           UNION ALL \
           SELECT ws_quantity*ws_list_price sales \
           FROM web_sales, date_dim \
           WHERE d_year = 2000 \
             AND d_moy = 1 \
             AND ws_sold_date_sk = d_date_sk \
             AND ws_item_sk IN (SELECT item_sk FROM frequent_ss_items) \
             AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer) \
         ) u \
         LIMIT 100",
    );

    assert!(
        text.contains("actual time="),
        "Q23-shaped EXPLAIN ANALYZE FORMAT=TREE should execute:\n{text}"
    );
    assert!(
        text.contains("CTE scan on frequent_ss_items")
            && text.contains("CTE scan on best_ss_customer"),
        "Q23-shaped WITH tree should retain CTE blocks:\n{text}"
    );
}

/// P0: a GROUP BY clause *inside* a CTE body surfaces its runtime stats —
/// the aggregate node carries `actual time=`/`rows=`, which is the core
/// of runtime information for clauses inside the CTE. (The fused-aggregate leaf scan is annotated
/// identically to a non-CTE top-level aggregate, i.e. it may show
/// `(never executed)`; that gap is orthogonal to the CTE wiring.)
#[test]
fn explain_analyze_with_cte_tree_annotates_inner_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100), (2, 200), (3, 50), (4, 200)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH t AS (SELECT amount, COUNT(*) AS c FROM orders GROUP BY amount) \
         SELECT amount, c FROM t ORDER BY amount",
    );

    let agg_line = text
        .lines()
        .find(|line| line.to_ascii_lowercase().contains("aggregate"))
        .unwrap_or_else(|| panic!("missing CTE-body aggregate line in:\n{text}"));
    assert!(
        agg_line.contains("actual time=") && agg_line.contains("rows="),
        "the GROUP BY clause inside the CTE must report runtime stats, got: {agg_line}\nfull:\n{text}"
    );
}

/// P0: EXPLAIN ANALYZE FORMAT=JSON must embed an `actual` block inside the
/// CTE body's `materialized_plan` nodes, keep the document valid (a single
/// top-level `actual` object, no duplicate key). CTE totals stay on existing
/// CteScan/materialized nodes rather than a synthetic summary array.
#[test]
fn explain_analyze_with_cte_json_embeds_inner_node_actuals() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100), (2, 200), (3, 50)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=JSON \
         WITH t AS (SELECT * FROM orders WHERE amount > 75) SELECT id FROM t ORDER BY id",
    );

    // Must be valid JSON (proves no duplicate `actual` key corruption).
    let parsed: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("invalid JSON ({e}):\n{text}"));
    let qb = &parsed["query_block"];
    assert!(
        qb["execution"]["ctes"].is_null(),
        "top-level actual must not carry a synthetic ctes array:\n{text}"
    );
    assert!(
        qb["execution"]["rows_total"].is_number(),
        "top-level actual must carry body totals:\n{text}"
    );
    // Per-node `actual` blocks plus the separate query execution summary.
    assert!(
        text.matches("\"actual\"").count() >= 2
            && parsed["query_block"]["execution"]["rows_total"] == 2,
        "json analyze should embed per-node actual blocks inside the CTE:\n{text}"
    );
    assert!(
        text.contains("\"materialized_plan\""),
        "CTE scan node should embed its materialized_plan:\n{text}"
    );
    assert!(
        text.contains("\"loops\":"),
        "per-node actual blocks should report loops:\n{text}"
    );
}

/// P1: a CTE referenced more than once is materialised a single time but
/// scanned at each reference; every `CTE scan` node must still be
/// annotated, and the shared body interior must report real stats.
#[test]
fn explain_analyze_with_multi_reference_cte_annotates_each_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100), (2, 200), (3, 50)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH c AS (SELECT id, amount FROM orders WHERE amount > 40) \
         SELECT x.id FROM c x JOIN c y ON x.id = y.id ORDER BY x.id",
    );

    assert!(
        text.matches("CTE scan").count() >= 2,
        "body should reference the CTE twice:\n{text}"
    );
    assert!(
        !text.contains("(never executed)"),
        "every referenced CTE scan and its interior must be annotated:\n{text}"
    );
    assert!(
        text.matches("scan on orders").count() >= 1 && text.contains("actual time="),
        "shared CTE body interior must report runtime stats:\n{text}"
    );
}

/// P1: nested CTEs (c2 reads c1) — both bodies must be annotated, and c2's
/// interior `CTE scan` of c1 resolves the real materialised result.
#[test]
fn explain_analyze_with_nested_cte_annotates_both_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100), (2, 200), (3, 50), (4, 300)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        // c2 referenced twice → both c1 (read only by c2) and c2 stay
        // materialised, so both per-CTE totals headers appear. A
        // single-reference c2 would inline into the body instead.
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH c1 AS (SELECT id, amount FROM orders WHERE amount > 40), \
              c2 AS (SELECT id FROM c1 WHERE amount < 250) \
         SELECT a.id FROM c2 a JOIN c2 b ON a.id = b.id ORDER BY a.id",
    );

    assert!(
        text.contains("CTE scan on c1") && text.contains("CTE scan on a <c2>"),
        "both CTE nodes should be present:\n{text}"
    );
    assert!(
        text.contains("scan on orders") && text.contains("actual time="),
        "c1 interior base scan must be annotated:\n{text}"
    );
    assert!(
        !text.contains("(never executed)"),
        "no operator in either CTE body should be (never executed):\n{text}"
    );
}

/// EXPLAIN ANALYZE on a WITH must actually execute the query: the reported
/// body row count matches the plain query's result.
#[test]
fn explain_analyze_with_cte_body_rows_match_execution() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, 100), (2, 200), (3, 50), (4, 300), (5, 80)")
        .unwrap();

    let query = "WITH t AS (SELECT id FROM orders WHERE amount > 75) SELECT id FROM t";
    let QueryResponse::ResultSet(plain) = engine.execute(query).unwrap() else {
        panic!("expected result set");
    };
    let expected = plain.rows.len();

    let text = explain_single_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {query}"));
    assert!(
        text.lines()
            .find(|line| line.starts_with("-> "))
            .is_some_and(|line| line.contains(&format!("rows={expected}"))),
        "body actual rows must match real execution ({expected}):\n{text}"
    );
}

/// EXPLAIN ANALYZE FORMAT=TREE for a non-recursive `UNION ALL` CTE body
/// renders BOTH branches under each CteScan (prefixed `Union all branch
/// N`) and attaches per-operator stats to every branch's display tree —
/// previously the right-hand branch was missing and the left-hand branch's
/// inner ops all reported `(never executed)` because materialisation went
/// through the re-planning fallback. Regression for the TPC-DS Q11 shape.
#[test]
fn explain_analyze_with_union_all_cte_shows_both_branches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE store_orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE web_orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO store_orders VALUES (1, 100), (2, 200), (3, 50)")
        .unwrap();
    engine
        .execute("INSERT INTO web_orders VALUES (10, 80), (20, 300)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH t AS ( \
            SELECT id, amount, 's' AS kind FROM store_orders WHERE amount > 75 \
            UNION ALL \
            SELECT id, amount, 'w' AS kind FROM web_orders WHERE amount > 75 \
         ) \
         SELECT id, kind FROM t ORDER BY id",
    );

    // Both branches must surface as siblings of the CteScan child list.
    assert!(
        text.contains("Union all branch 1"),
        "first UNION ALL branch must be labelled in the display tree:\n{text}"
    );
    assert!(
        text.contains("Union all branch 2"),
        "second UNION ALL branch must be labelled in the display tree:\n{text}"
    );
    // The right-side table (`web_orders`) was completely absent in the
    // pre-fix tree; assert it now shows up under the CTE.
    assert!(
        text.contains("scan on web_orders"),
        "right-hand UNION ALL branch (web_orders) must be in the display tree:\n{text}"
    );
    assert!(
        text.contains("scan on store_orders"),
        "left-hand UNION ALL branch (store_orders) must be in the display tree:\n{text}"
    );
    // Every base-table scan inside the CTE body now lines up by pointer
    // with the executed display plan, so it carries `actual time=` and
    // never reports `(never executed)`.
    let store_scan = text
        .lines()
        .find(|l| l.contains("scan on store_orders"))
        .unwrap_or_else(|| panic!("missing store_orders scan line:\n{text}"));
    let web_scan = text
        .lines()
        .find(|l| l.contains("scan on web_orders"))
        .unwrap_or_else(|| panic!("missing web_orders scan line:\n{text}"));
    assert!(
        store_scan.contains("actual time="),
        "left-branch scan must carry actual stats, got: {store_scan}\nfull:\n{text}"
    );
    assert!(
        web_scan.contains("actual time="),
        "right-branch scan must carry actual stats, got: {web_scan}\nfull:\n{text}"
    );
    // Total CTE rows = rows from both branches that satisfied amount > 75.
    // store: 100 + 200 = 2 rows; web: 80 + 300 = 2 rows → 4 rows.
    assert!(
        text.lines()
            .any(|line| line.contains("CTE scan") && line.contains("rows=4")),
        "CTE total must reflect both branches (expected 4):\n{text}"
    );
    assert!(
        text.lines()
            .find(|line| line.starts_with("-> "))
            .is_some_and(|line| line.contains("rows=4")),
        "body must consume both branches' rows:\n{text}"
    );
}

/// JSON counterpart: a UNION ALL CTE body emits `union_all_branches` as
/// an array (one entry per branch) under the `cte_scan` node, each entry
/// carrying its own per-operator `actual` block.
#[test]
fn explain_analyze_with_union_all_cte_json_emits_branches_array() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE store_orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE web_orders (id INT PRIMARY KEY, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO store_orders VALUES (1, 100), (2, 200)")
        .unwrap();
    engine
        .execute("INSERT INTO web_orders VALUES (10, 300)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=JSON \
         WITH t AS ( \
            SELECT id FROM store_orders \
            UNION ALL \
            SELECT id FROM web_orders \
         ) \
         SELECT id FROM t",
    );

    let parsed: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("invalid JSON ({e}):\n{text}"));
    // Walk down to the CteScan in the body's tree. The body is a trivial
    // `SELECT id FROM t` so the top operator under query_block is the
    // CteScan itself.
    let qb = &parsed["query_block"];
    let cte_scan = if qb["cte_scan"].is_object() {
        &qb["cte_scan"]
    } else if qb["projection"]["input"]["cte_scan"].is_object() {
        &qb["projection"]["input"]["cte_scan"]
    } else {
        panic!("could not locate cte_scan in body:\n{text}");
    };
    let branches = cte_scan["union_all_branches"]
        .as_array()
        .unwrap_or_else(|| panic!("union_all_branches missing or not array:\n{text}"));
    assert_eq!(
        branches.len(),
        2,
        "expected exactly 2 UNION ALL branches:\n{text}"
    );
}

/// Recursive CTE execution is represented by the physical RecursiveUnion
/// node and reports the materialized total in TREE and JSON.
#[test]
fn explain_analyze_with_recursive_cte_tree_and_json_report_totals() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let tree = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) \
         SELECT n FROM seq",
    );
    assert!(
        tree.contains("CTE scan on seq") && tree.contains("rows=5"),
        "recursive CTE node should report its materialised total:\n{tree}"
    );
    assert!(
        tree.contains("Recursive union [seq]") && tree.contains("max_depth=1000"),
        "TREE must expose the iterative working/delta operator:\n{tree}"
    );

    let json = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=JSON \
         WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5) \
         SELECT n FROM seq",
    );
    let _parsed: serde_json::Value =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON ({e}):\n{json}"));
    assert!(
        json.contains("\"rows_total\": 5"),
        "recursive CTE node should carry rows=5 actual metrics:\n{json}"
    );
    assert!(
        json.contains("\"recursive_union\"") && json.contains("\"max_depth\": 1000"),
        "JSON must expose RecursiveUnion metadata:\n{json}"
    );
}

/// EXPLAIN ANALYZE FORMAT=TREE/JSON for a WITH body containing inline
/// derived tables (e.g. TPC-DS Q1: `FROM (SELECT ...) y, (SELECT ...) z`)
/// must execute, not return 1235. The inline-derived sources are
/// pre-materialised so the body's display plan — whose corresponding
/// leaves are stub CteScans — runs through `execute_physical_select` under
/// the active stats collector.
#[test]
fn explain_analyze_with_cte_body_inline_derived_runs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sales (id INT PRIMARY KEY, region INT, amount INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO sales VALUES \
             (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 2, 50), (5, 1, 300)",
        )
        .unwrap();

    // The body has two inline-derived sources `y` and `z` that both reference
    // the user CTE `agg`. Mirrors the TPC-DS Q1 shape.
    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH agg AS (SELECT region, SUM(amount) AS s FROM sales GROUP BY region) \
         SELECT y.region, y.s, z.s \
         FROM (SELECT region, s FROM agg) y, \
              (SELECT region, s FROM agg) z \
         WHERE y.region = z.region \
         ORDER BY y.region",
    );
    assert!(
        text.lines()
            .any(|line| line.contains("CTE scan on agg") && line.contains("rows=2")),
        "CTE total should reflect both region groups (2):\n{text}"
    );
    assert!(
        text.lines()
            .find(|line| line.starts_with("-> "))
            .is_some_and(|line| line.contains("rows=2")),
        "body row count should match real execution (2):\n{text}"
    );
    assert!(
        !text.contains("is not yet supported for multi-source"),
        "the 1235 gate should be lifted:\n{text}"
    );
    assert!(
        text.contains("Materialize [agg]"),
        "CTE spool must be an explicit physical pipeline boundary:\n{text}"
    );

    // Same query under FORMAT=JSON must also execute and emit per-CTE actuals.
    let json = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=JSON \
         WITH agg AS (SELECT region, SUM(amount) AS s FROM sales GROUP BY region) \
         SELECT y.region, y.s, z.s \
         FROM (SELECT region, s FROM agg) y, \
              (SELECT region, s FROM agg) z \
         WHERE y.region = z.region",
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON ({e}):\n{json}"));
    assert!(
        parsed["query_block"]["execution"]["rows_total"].is_number(),
        "JSON must carry body totals:\n{json}"
    );
    assert!(
        parsed["query_block"]["execution"]["ctes"].is_null(),
        "JSON must not add a synthetic CTE summary array:\n{json}"
    );
}

/// P0-A: a plain (non-WITH) SELECT whose FROM list carries inline derived
/// tables must run under EXPLAIN ANALYZE FORMAT=TREE/JSON instead of bailing
/// with 1235 "multi-source derived/CTE joins". The display body plan and each
/// inline-derived sub-plan execute under one stats collector, so per-operator
/// `actual` annotations appear and the body row count matches real execution.
#[test]
fn explain_analyze_join_derived_runs_without_bail() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sales (id INT PRIMARY KEY, region INT, amount INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO sales VALUES \
             (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 2, 50), (5, 1, 300)",
        )
        .unwrap();

    // Two inline-derived sources joined directly in FROM (no WITH) — the
    // TPC-DS q08/q88 multi-source shape that used to hit the 1235 gate.
    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT a.region, a.s, b.cnt \
         FROM (SELECT region, SUM(amount) AS s FROM sales GROUP BY region) a, \
              (SELECT region, COUNT(*) AS cnt FROM sales GROUP BY region) b \
         WHERE a.region = b.region \
         ORDER BY a.region",
    );
    assert!(
        !text.contains("is not yet supported for multi-source"),
        "the 1235 multi-source gate must be lifted:\n{text}"
    );
    assert!(
        text.contains("actual time="),
        "per-operator actual stats must be present:\n{text}"
    );

    let json = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=JSON \
         SELECT a.region, a.s, b.cnt \
         FROM (SELECT region, SUM(amount) AS s FROM sales GROUP BY region) a, \
              (SELECT region, COUNT(*) AS cnt FROM sales GROUP BY region) b \
         WHERE a.region = b.region",
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON ({e}):\n{json}"));
    assert!(
        parsed["query_block"]["execution"]["rows_total"].is_number(),
        "JSON must carry body totals:\n{json}"
    );
}

#[test]
fn explain_analyze_derived_cte_scan_does_not_duplicate_materialization_time() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sales (id INT PRIMARY KEY, region INT, amount INT)")
        .unwrap();
    let values = (1..=200)
        .map(|i| format!("({i}, {}, {})", i % 5, i * 10))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO sales VALUES {values}"))
        .unwrap();

    // Give materialization a known cost so scheduler noise cannot reverse
    // the comparison between two otherwise sub-millisecond operators. The
    // assertion still detects a CTE scan that includes materialization time.
    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT COUNT(*) FROM (SELECT * FROM sales WHERE SLEEP(0.001)=0 LIMIT 100) d",
    );
    let cte_line = text
        .lines()
        .find(|line| line.contains("CTE scan on"))
        .unwrap_or_else(|| panic!("expected CTE scan line:\n{text}"));
    let limit_line = text
        .lines()
        .find(|line| line.contains("-> Limit 100"))
        .unwrap_or_else(|| panic!("expected materialized Limit line:\n{text}"));
    assert!(
        actual_time_ms(cte_line) < actual_time_ms(limit_line),
        "CTE scan must report only scan consumption, not duplicate its materialized plan time:\n{text}"
    );
    assert!(!cte_line.contains("stats_source=measured"), "{text}");
    assert!(!limit_line.contains("stats_source=measured"), "{text}");
}

#[test]
fn explain_analyze_large_derived_count_spills_instead_of_oom() {
    use crate::engine::executor::spill_quota;

    let _spill_threshold = restore_env(
        "MYDB_CTE_SPILL_THRESHOLD_MB",
        std::env::var("MYDB_CTE_SPILL_THRESHOLD_MB").ok(),
    );
    let _process_limit = restore_env(
        "MYDB_PROCESS_MEM_LIMIT_MB",
        std::env::var("MYDB_PROCESS_MEM_LIMIT_MB").ok(),
    );
    std::env::set_var("MYDB_CTE_SPILL_THRESHOLD_MB", "0");
    std::env::set_var("MYDB_PROCESS_MEM_LIMIT_MB", "64");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wide_sales (id INT PRIMARY KEY, region INT, pad TEXT)")
        .unwrap();
    let payload = "x".repeat(256);
    let total = 160;
    let mut next = 0;
    while next < total {
        let take = (total - next).min(80);
        let mut insert = String::from("INSERT INTO wide_sales VALUES ");
        for i in 0..take {
            if i > 0 {
                insert.push(',');
            }
            let id = next + i;
            insert.push_str(&format!("({id}, {}, '{payload}')", id % 7));
        }
        engine.execute(&insert).unwrap();
        next += take;
    }

    let before = spill_quota::global_spilled_high_water();
    let sql = "SELECT COUNT(*) FROM (SELECT * FROM wide_sales) d";
    let QueryResponse::ResultSet(count_rs) = engine.execute(sql).unwrap() else {
        panic!("expected count result set");
    };
    assert_eq!(count_rs.rows, vec![vec![Value::Int(total as i64)]]);

    let text = explain_single_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
    assert!(
        text.contains("actual time="),
        "analyze output must include actual stats:\n{text}"
    );
    assert!(
        text.contains(&format!("rows={total}")),
        "analyze output should report the derived row count:\n{text}"
    );
    assert!(
        spill_quota::global_spilled_high_water() > before,
        "large analyzed derived table should spill instead of materializing fully:\n{text}"
    );
}

/// P1-B: a plain SELECT with a window function in its projection must run
/// under EXPLAIN ANALYZE FORMAT=TREE instead of failing with 1235 "window
/// function requires window evaluation context". The analyze path routes
/// through the window-aware executor (`execute_windowed_select`).
#[test]
fn explain_analyze_plain_window_runs_without_bail() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sales (id INT PRIMARY KEY, region INT, amount INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO sales VALUES \
             (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 2, 50), (5, 1, 300)",
        )
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT region, amount, SUM(amount) OVER (PARTITION BY region) AS rs \
         FROM sales ORDER BY region, amount",
    );
    assert!(
        !text.contains("window function requires window evaluation context"),
        "windowed SELECT must execute under ANALYZE, not 1235:\n{text}"
    );
    assert!(
        !text.contains("is not yet supported"),
        "no unsupported bail expected:\n{text}"
    );
    assert!(
        text.contains("Window functions [SUM]"),
        "ANALYZE should format the actual WindowAgg execution plan:\n{text}"
    );
    assert!(
        text.contains("actual time="),
        "ANALYZE should include real operator stats:\n{text}"
    );
    assert!(
        !text.lines().all(|line| line.contains("(never executed)")),
        "ANALYZE should not report an entirely unexecuted tree:\n{text}"
    );
}

/// A WITH body whose inline derived inner is itself a UNION ALL set
/// operation (the TPC-DS Q1 `wscs` shape) is lowered as a complete query
/// expression into its own spillable spool. The parent CTE then consumes a
/// CteScan leaf without losing the right branch or restarting the owner.
#[test]
fn explain_analyze_with_cte_setop_derived_materializes_complete_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE web_sales (id INT PRIMARY KEY, price INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE catalog_sales (id INT PRIMARY KEY, price INT)")
        .unwrap();
    engine
        .execute("INSERT INTO web_sales VALUES (1, 100), (2, 200)")
        .unwrap();
    engine
        .execute("INSERT INTO catalog_sales VALUES (1, 50), (2, 75), (3, 25)")
        .unwrap();

    // wscs body has an inline-derived UNION ALL — a lossy display plan.
    // Total rows materialised should be web_sales.count() + catalog_sales.count() = 5.
    let text = explain_single_text(
        &engine,
        // wscs referenced twice → stays materialised. All distinct prices →
        // the self-join keeps the five rows emitted by both set branches.
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH wscs AS ( \
            SELECT price FROM ( \
                SELECT price FROM web_sales \
                UNION ALL \
                SELECT price FROM catalog_sales \
            ) \
         ) \
         SELECT COUNT(*) FROM wscs a JOIN wscs b ON a.price = b.price",
    );
    assert!(
        text.lines()
            .any(|line| line.contains("CTE scan") && line.contains("rows=5")),
        "set-derived CTE should materialise all five rows:\n{text}"
    );
    assert!(
        text.lines()
            .find(|line| line.starts_with("-> "))
            .is_some_and(|line| line.contains("rows=1")),
        "body should compute a single COUNT(*) row:\n{text}"
    );
}

#[test]
fn explain_analyze_with_cte_intersect_derived_shows_executed_setop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT, k INT)")
        .unwrap();
    engine.execute("CREATE TABLE left_keys (k INT)").unwrap();
    engine.execute("CREATE TABLE right_keys (k INT)").unwrap();
    engine
        .execute("INSERT INTO items VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    engine
        .execute("INSERT INTO left_keys VALUES (1),(2),(2)")
        .unwrap();
    engine
        .execute("INSERT INTO right_keys VALUES (2),(3)")
        .unwrap();

    let text = explain_single_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         WITH x AS ( \
           SELECT i.id FROM items i, ( \
             SELECT k FROM left_keys INTERSECT SELECT k FROM right_keys \
           ) d WHERE i.k=d.k \
         ) \
         SELECT COUNT(*) FROM x a JOIN x b ON a.id=b.id",
    );
    let setop_line = text
        .lines()
        .find(|line| line.contains("Intersect distinct"))
        .unwrap_or_else(|| panic!("complete INTERSECT plan must be visible:\n{text}"));
    assert!(
        setop_line.contains("actual time=") || setop_line.contains("included in operator"),
        "visible INTERSECT must be tied to executed work:\n{setop_line}\n{text}"
    );
}

// EXPLAIN ANALYZE FORMAT=TREE/JSON of a WITH-CTE query whose CTE bodies and
// inline-derived sources carry window functions (TPC-DS q51 shape). The
// `WindowAgg` operator is injected at dispatch time, not by
// `planner::plan_select`, so the EXPLAIN display plan omits it; the analyze
// path must materialise these window-bearing bodies through the window-aware
// execution path instead of re-applying the window projection in the legacy
// per-row evaluator (which raised error 1235 — regression for q51).
#[test]
fn explain_analyze_tree_window_in_full_outer_join_cte() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE web_sales (ws_sold_date_sk INT, ws_item_sk INT, ws_sales_price INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE store_sales (ss_sold_date_sk INT, ss_item_sk INT, ss_sales_price INT)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE date_dim (d_date_sk INT, d_date INT, d_month_seq INT)")
        .unwrap();
    engine
        .execute("INSERT INTO date_dim VALUES (1, 100, 1176), (2, 101, 1177), (3, 102, 1176)")
        .unwrap();
    engine
        .execute("INSERT INTO web_sales VALUES (1, 5, 10), (2, 5, 20), (3, 7, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO store_sales VALUES (1, 5, 7), (2, 5, 8), (3, 6, 9)")
        .unwrap();

    let body = "WITH web_v1 as ( \
      select ws_item_sk item_sk, d_date, \
        sum(sum(ws_sales_price)) over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales \
      from web_sales, date_dim \
      where ws_sold_date_sk=d_date_sk and d_month_seq between 1176 and 1176+11 and ws_item_sk is not NULL \
      group by ws_item_sk, d_date), \
      store_v1 as ( \
      select ss_item_sk item_sk, d_date, \
        sum(sum(ss_sales_price)) over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales \
      from store_sales, date_dim \
      where ss_sold_date_sk=d_date_sk and d_month_seq between 1176 and 1176+11 and ss_item_sk is not NULL \
      group by ss_item_sk, d_date) \
      select * from (select item_sk, d_date, web_sales, store_sales, \
        max(web_sales) over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative, \
        max(store_sales) over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative \
        from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk, \
                     case when web.d_date is not null then web.d_date else store.d_date end d_date, \
                     web.cume_sales web_sales, store.cume_sales store_sales \
              from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk and web.d_date = store.d_date) \
             )x )y \
      where web_cumulative > store_cumulative order by item_sk, d_date limit 100";

    for fmt in ["TREE", "JSON"] {
        let sql = format!("EXPLAIN ANALYZE FORMAT={fmt} {body}");
        let resp = engine.execute(&sql).unwrap_or_else(|e| {
            panic!(
                "q51 EXPLAIN ANALYZE FORMAT={fmt} failed: {} {}",
                e.code(),
                e.message()
            )
        });
        let QueryResponse::ResultSet(rs) = resp else {
            panic!("expected result set for FORMAT={fmt}");
        };
        let Value::String(text) = &rs.rows[0][0] else {
            panic!("expected explain text for FORMAT={fmt}");
        };
        // ANALYZE must have actually executed the plan (per-operator stats
        // present) rather than erroring out on the window functions.
        assert!(
            text.contains("actual"),
            "FORMAT={fmt} output missing actual stats (analyze did not run):\n{text}"
        );
        if fmt == "JSON" {
            let json: serde_json::Value = serde_json::from_str(text).unwrap();
            assert_eq!(
                json["query_block"]["limit"]["actual"]["active_time_total_ms"],
                json["query_block"]["execution"]["wall_time_ms"],
                "root timing must include CTE and inline-derived materialization: {text}"
            );
            assert!(json["query_block"]["limit"]["actual"]["active_time_total_ms"].is_number());
        }
    }
}

/// P1-E: `EXPLAIN [ANALYZE] FORMAT=TREE/JSON` of a WITH-CTE query whose body
/// joins two single-use CTEs with a FULL OUTER JOIN (the TPC-DS q97 shape).
/// The EXPLAIN rewrite must NOT inline the single-use CTEs in this case —
/// inlining replaces the `ssci`/`csci` references with anonymous derived
/// tables and drops them from the WITH list; the planner can't fold a full
/// outer join, so the synthetic fallback then tries to resolve the orphaned
/// alias as a real table and failed with 1146 ("table 'ssci' does not
/// exist"). With the references kept, the body plans as a real Hash full join
/// over the two CTE scans.
#[test]
fn explain_full_outer_join_over_single_use_ctes_q97() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE store_sales (ss_sold_date_sk INT, ss_customer_sk INT, ss_item_sk INT)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE catalog_sales (cs_sold_date_sk INT, cs_bill_customer_sk INT, cs_item_sk INT)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE date_dim (d_date_sk INT, d_month_seq INT)")
        .unwrap();
    engine
        .execute("INSERT INTO date_dim VALUES (1, 1176), (2, 1177)")
        .unwrap();
    engine
        .execute("INSERT INTO store_sales VALUES (1, 10, 100), (2, 11, 101)")
        .unwrap();
    engine
        .execute("INSERT INTO catalog_sales VALUES (1, 10, 100), (2, 12, 102)")
        .unwrap();

    let body = "with ssci as ( select ss_customer_sk customer_sk, ss_item_sk item_sk \
        from store_sales,date_dim where ss_sold_date_sk = d_date_sk \
        and d_month_seq between 1176 and 1176 + 11 group by ss_customer_sk, ss_item_sk), \
        csci as( select cs_bill_customer_sk customer_sk, cs_item_sk item_sk \
        from catalog_sales,date_dim where cs_sold_date_sk = d_date_sk \
        and d_month_seq between 1176 and 1176 + 11 group by cs_bill_customer_sk, cs_item_sk) \
        select sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only, \
        sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only, \
        sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog \
        from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk and ssci.item_sk = csci.item_sk) limit 100";

    // All six EXPLAIN variants must succeed (previously TREE/JSON raised 1146).
    for prefix in ["EXPLAIN ", "EXPLAIN ANALYZE "] {
        for fmt in ["", "FORMAT=TREE ", "FORMAT=JSON "] {
            let sql = format!("{prefix}{fmt}{body}");
            let QueryResponse::ResultSet(rs) = engine
                .execute(&sql)
                .unwrap_or_else(|e| panic!("{prefix}{fmt}failed: {} {}", e.code(), e.message()))
            else {
                panic!("{prefix}{fmt}did not return a result set");
            };
            assert!(!rs.rows.is_empty(), "{prefix}{fmt}produced no rows");
        }
    }

    // The TREE body shows a real full join over both CTE scans (not a 1146).
    let tree = explain_single_text(&engine, &format!("EXPLAIN FORMAT=TREE {body}"));
    assert!(
        tree.to_lowercase().contains("full join"),
        "TREE must show a full join:\n{tree}"
    );
    assert!(
        tree.contains("CTE scan on ssci") && tree.contains("CTE scan on csci"),
        "TREE must reference both CTE scans:\n{tree}"
    );

    // EXPLAIN ANALYZE actually executes and attaches CTE materialisation
    // metrics to the existing scan nodes.
    let analyzed = explain_single_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {body}"));
    assert!(
        analyzed.contains("actual time="),
        "ANALYZE must carry per-operator stats:\n{analyzed}"
    );
    assert!(
        analyzed
            .lines()
            .any(|line| { line.contains("CTE scan on ssci") && line.contains("actual time=") })
            && analyzed
                .lines()
                .any(|line| { line.contains("CTE scan on csci") && line.contains("actual time=") }),
        "ANALYZE must report per-CTE node metrics:\n{analyzed}"
    );

    // JSON is well-formed.
    let json = explain_single_text(&engine, &format!("EXPLAIN FORMAT=JSON {body}"));
    serde_json::from_str::<serde_json::Value>(&json)
        .unwrap_or_else(|e| panic!("invalid JSON ({e}):\n{json}"));
}

#[test]
fn unique_build_join_probe_groups_use_typed_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE distinct_dim (k INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO distinct_dim VALUES (1),(2),(3)")
        .unwrap();
    engine
        .execute("CREATE TABLE distinct_fact (k INT, a BIGINT, b DATE)")
        .unwrap();
    engine.execute("INSERT INTO distinct_fact VALUES (1,10,'2020-01-01'),(1,10,'2020-01-01'),(2,NULL,'2020-01-02'),(3,30,NULL),(4,40,'2020-01-04'),(NULL,50,NULL)").unwrap();
    for projection in ["f.a,f.b"] {
        let sql = format!("SELECT /*+ HASH_JOIN(f,d) */ {projection} FROM distinct_fact f JOIN distinct_dim d ON f.k=d.k GROUP BY f.a,f.b");
        let text = explain_single_text(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
        assert!(!text.contains("aggregate_mode=join_matches"), "{text}");
        assert!(text.contains("actual time="), "{text}");
        let QueryResponse::ResultSet(result) = engine.execute(&sql).unwrap().materialize().unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(result.rows.len(), 3);
    }
}

#[test]
fn explain_analyze_ranked_rollup_keeps_consumer_bound_and_single_loop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rank_rollup (id INT PRIMARY KEY, g INT, h INT, v DECIMAL(14,2))")
        .unwrap();
    let values = (0..1200)
        .map(|id| format!("({id},{},{},{})", id % 4, id % 113, id % 17))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO rank_rollup VALUES {values}"))
        .unwrap();
    for kind in ["RANK", "DENSE_RANK", "ROW_NUMBER"] {
        let query = format!("SELECT * FROM (SELECT g,h,s,{kind}() OVER (PARTITION BY g ORDER BY s DESC) rk FROM (SELECT g,h,SUM(v) s FROM rank_rollup GROUP BY g,h WITH ROLLUP) r) w WHERE rk <= 2 ORDER BY g,h LIMIT 100");
        for query in [
            query.clone(),
            format!("WITH ranked AS ({query}) SELECT * FROM ranked"),
        ] {
            let text = explain_single_text(&engine, &format!("EXPLAIN ANALYZE {query}"));
            let line = text
                .lines()
                .find(|line| line.contains("rollup aggregate"))
                .expect(&text);
            assert!(line.contains("loops=1"), "{text}");
            let rows = line
                .split(" rows=")
                .last()
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap();
            assert!(
                rows < 40,
                "rank bound lost during ANALYZE materialization: {text}"
            );
            let root = text.lines().find(|line| line.starts_with("->")).unwrap();
            // The plain derived query has a visible Limit root; a WITH body
            // may hide its Projection envelope in TREE formatting.
            if !query.starts_with("WITH ") {
                assert!(
                    actual_time_ms(root) >= actual_time_ms(line),
                    "prerequisite time lost: {text}"
                );
            }
        }
    }
    let unbounded = explain_single_text(&engine, "EXPLAIN ANALYZE SELECT * FROM (SELECT g,h,SUM(v) s FROM rank_rollup GROUP BY g,h WITH ROLLUP) r");
    let line = unbounded
        .lines()
        .find(|line| line.contains("rollup aggregate"))
        .unwrap();
    assert!(
        line.contains("rows=457"),
        "rank hint leaked into next statement: {unbounded}"
    );
}
