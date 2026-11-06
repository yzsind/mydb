//! Regression coverage for the two per-row projection fast paths that the
//! TPC-DS Q78 profiling work introduced:
//!
//!  * `share_field_lookup_across_rows` — every row in an eager result set
//!    shares one schema-built `FieldLookup`, so the legacy projection /
//!    ORDER-BY interpreter resolves wide-row column references against a
//!    single memoized map instead of rebuilding it per row.
//!  * `agg_cache_key` — a bare aggregate column (`sum(x)`) precomputes its
//!    `aggregate_key` hash once at compile time, so projecting an aggregate
//!    output row is a direct `agg_values` probe rather than a per-row
//!    `Debug`-format walk of the expression.
//!
//! Both are pure performance changes; these tests pin the *results* so a
//! regression in either fast path (cache key miss, wrong shared schema,
//! aggregate row mis-projected) surfaces as a value mismatch.
use super::*;

#[test]
fn aggregate_having_subquery_preserves_exact_result_metadata() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE aggregate_wire (g INT, v INT)")
        .unwrap();
    let values = (0..16)
        .map(|g| format!("({g},{})", if g < 3 { 1000 } else { 1 }))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO aggregate_wire VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE aggregate_wire").unwrap();
    let sql = "SELECT g,SUM(v) AS total FROM aggregate_wire GROUP BY g \
               HAVING SUM(v)>(SELECT SUM(v)*.005 FROM aggregate_wire) ORDER BY g";
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        result.columns[1].column_type,
        ColumnType::Decimal {
            precision: 32,
            scale: 0
        }
    );
    let column = &result.columns[1];
    let definition = crate::mysql::protocol::column_definition_packet_full(
        &column.name,
        column.column_type,
        &column.source,
    );
    assert_eq!(definition[definition.len() - 6], 0xf6); // MYSQL_TYPE_NEWDECIMAL
    assert_eq!(result.columns[1].source.decimals, Some(0));
    for row in &result.rows {
        assert_eq!(row[1].to_display_string(), "1000");
        let packet = crate::mysql::protocol::binary_row_packet(&result.columns, row);
        assert_eq!(&packet[6..], b"\x041000");
    }
}

#[test]
fn aggregate_having_metadata_preserves_numeric_and_string_domains() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE aggregate_domains(g INT,u BIGINT UNSIGNED,d DECIMAL(50,8),f DOUBLE,s VARCHAR(8))").unwrap();
    let values = (0..16)
        .map(|i| {
            if i % 5 == 0 {
                format!("({},NULL,NULL,NULL,NULL)", i % 4)
            } else {
                format!(
                    "({},18446744073709551615,9007199254740993.12345678,1.25,'v{i}')",
                    i % 4
                )
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO aggregate_domains VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE aggregate_domains").unwrap();
    let rows = |sql: &str| {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
        else {
            panic!("expected result set: {sql}");
        };
        result
    };
    for expression in [
        "SUM(u)",
        "AVG(u)",
        "MIN(u)",
        "MAX(u)",
        "SUM(d)",
        "AVG(d)",
        "MIN(d)",
        "MAX(d)",
        "SUM(f)",
        "AVG(f)",
        "COUNT(d)",
        "ABS(SUM(d))",
        "GROUP_CONCAT(s ORDER BY s)",
    ] {
        let projection = format!("SELECT g,{expression} AS v FROM aggregate_domains GROUP BY g");
        let expected = rows(&format!("{projection} ORDER BY g"));
        let actual = rows(&format!(
            "{projection} HAVING COUNT(*)>(SELECT COUNT(*)*.005 FROM aggregate_domains) ORDER BY g"
        ));
        assert_eq!(actual.columns, expected.columns, "{expression}");
        let display = |result: &ResultSet| {
            result
                .rows
                .iter()
                .map(|row| row.iter().map(Value::to_display_string).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        };
        assert_eq!(display(&actual), display(&expected), "{expression}");
    }
}

#[test]
fn cached_aggregates_ignore_function_case_and_preserve_labels() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE case_agg (id INT PRIMARY KEY, v INT, s VARCHAR(8))")
        .unwrap();
    engine
        .execute("INSERT INTO case_agg VALUES (1,10,'A'),(2,20,'a'),(3,NULL,'A')")
        .unwrap();
    for hint in [
        "",
        "/*+ FULL_SCAN(t) NO_PARALLEL */",
        "/*+ FULL_SCAN(t) PARALLEL(4) */",
    ] {
        for name in ["count", "COUNT", "couNT", "CoUnT", "count"] {
            let sql = format!("select {hint} {name}(*) from case_agg t");
            let QueryResponse::ResultSet(result) = engine.execute(&sql).unwrap() else {
                panic!("expected rows")
            };
            assert_eq!(result.rows, vec![vec![Value::Int(3)]], "{sql}");
            assert_eq!(result.columns[0].name, format!("{name}(*)"));
        }
    }
    for (count, sum, avg, min, max, abs, col) in [
        ("count", "sum", "avg", "min", "max", "abs", "v"),
        ("couNT", "sUm", "AvG", "mIn", "MaX", "aBs", "V"),
        ("COUNT", "SUM", "AVG", "MIN", "MAX", "ABS", "v"),
    ] {
        let sql = format!("select {count}({col}) as n,{count}(distinct {col}) as nd,{sum}({abs}({col})) as s,{avg}({col}) as a,{min}({col}) as lo,{max}({col}) as hi from case_agg");
        assert_eq!(
            num_rows(&engine, &sql),
            vec![vec![2.0, 2.0, 30.0, 15.0, 10.0, 20.0]],
            "{sql}"
        );
        let sql = format!("select id%2 as g,{sum}({col}) as total from case_agg group by id%2 having {count}({col})>0 order by {sum}({col})");
        assert_eq!(
            num_rows(&engine, &sql),
            vec![vec![0.0, 20.0], vec![1.0, 10.0]],
            "{sql}"
        );
    }
    // Literal bytes are part of the expression identity even when names fold.
    let QueryResponse::ResultSet(result) = engine
        .execute("select min('A'),min('a'),max('A'),max('a') from case_agg")
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(
        result.rows,
        vec![vec![
            Value::String("A".into()),
            Value::String("a".into()),
            Value::String("A".into()),
            Value::String("a".into())
        ]]
    );
}
/// Normalize a numeric `Value` to `f64` so assertions compare magnitudes
/// without pinning the exact representation (`sum()` over INT yields a
/// `DecimalCompact`, COALESCE/arithmetic may widen, etc.).
fn num(v: &Value) -> f64 {
    match v {
        Value::Int(n) => *n as f64,
        Value::UInt(n) => *n as f64,
        Value::Double(d) => *d,
        Value::DecimalCompact(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        other => panic!("non-numeric value in test: {other:?}"),
    }
}

/// Run `sql` and return rows as normalized `f64` grids, sorted for a stable
/// comparison independent of result ordering.
fn num_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<f64>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    let mut out: Vec<Vec<f64>> = result
        .rows
        .iter()
        .map(|r| r.iter().map(num).collect())
        .collect();
    out.sort_by(|a, b| a.partial_cmp(b).unwrap());
    out
}

/// Aggregate output rows carry `group: Some(empty)` *and* a populated
/// `agg_values` cache; the projection must hit the precomputed-key path and
/// still return the correct sums per group.
#[test]
fn bare_aggregate_columns_project_correctly_over_many_groups() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE s (g INT, k INT, q INT, c DECIMAL(10,2))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO s VALUES \
             (1, 10, 2, 1.50), (1, 10, 3, 2.50), \
             (1, 20, 4, 4.00), \
             (2, 10, 5, 5.00)",
        )
        .unwrap();
    // Mirrors a Q78 CTE body: GROUP BY two keys, project bare aggregates.
    let out = num_rows(
        &engine,
        "SELECT g, k, sum(q) AS sq, sum(c) AS sc FROM s GROUP BY g, k ORDER BY g, k",
    );
    assert_eq!(
        out,
        vec![
            vec![1.0, 10.0, 5.0, 4.00], // g=1,k=10: q 2+3=5, c 1.50+2.50=4.00
            vec![1.0, 20.0, 4.0, 4.00], // g=1,k=20: q 4,     c 4.00
            vec![2.0, 10.0, 5.0, 5.00], // g=2,k=10: q 5,     c 5.00
        ]
    );
}

/// A multi-CTE join (wide output rows) projected through composite COALESCE
/// arithmetic exercises the shared-`FieldLookup` path: the outer projection
/// resolves `ws_*` / `ss_*` column names against one memoized lookup for the
/// whole result set. Pins the joined values so a broken shared schema (wrong
/// column → index) would mismatch.
#[test]
fn cte_join_wide_projection_resolves_shared_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE store (item INT, cust INT, qty INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE web (item INT, cust INT, qty INT)")
        .unwrap();
    engine
        .execute("INSERT INTO store VALUES (1,100,5),(1,100,3),(2,200,7)")
        .unwrap();
    engine.execute("INSERT INTO web VALUES (1,100,4)").unwrap();
    let sql = "WITH ss AS (SELECT item ss_item, cust ss_cust, sum(qty) ss_qty \
                            FROM store GROUP BY item, cust), \
                    ws AS (SELECT item ws_item, cust ws_cust, sum(qty) ws_qty \
                            FROM web GROUP BY item, cust) \
               SELECT ss_item, ss_cust, ss_qty, \
                      coalesce(ws_qty, 0) AS other_qty, \
                      ss_qty + coalesce(ws_qty, 0) AS total_qty \
               FROM ss LEFT JOIN ws ON ws_item = ss_item AND ws_cust = ss_cust \
               ORDER BY ss_item, ss_cust";
    let out = num_rows(&engine, sql);
    assert_eq!(
        out,
        vec![
            // item 1: store qty 5+3=8, web qty 4 → total 12
            vec![1.0, 100.0, 8.0, 4.0, 12.0],
            // item 2: store qty 7, no web match → other 0, total 7
            vec![2.0, 200.0, 7.0, 0.0, 7.0],
        ]
    );
}
