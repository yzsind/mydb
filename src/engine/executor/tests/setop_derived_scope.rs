use super::*;

fn query_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine.execute(sql).unwrap().materialize().unwrap() {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected result set, got {other:?}"),
    }
}

#[test]
fn setop_derived_scope_repeated_alias() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE source (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO source VALUES (1), (2)")
        .unwrap();
    let sql = (0..8)
        .map(|i| format!("SELECT * FROM (SELECT 'branch{i}' AS tableId FROM source LIMIT 1) t"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let expected: Vec<_> = (0..8)
        .map(|i| vec![Value::String(format!("branch{i}"))])
        .collect();
    assert_eq!(query_rows(&engine, &sql), expected);
}

fn scope_engine(dir: &std::path::Path) -> StorageQueryEngine {
    let engine = open_test_engine(dir);
    engine
        .execute("CREATE TABLE source (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO source VALUES (1), (2), (3)")
        .unwrap();
    engine
}

#[test]
fn setop_derived_scope_set_operators_and_nested_consumer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = scope_engine(dir.path());
    for (operator, expected) in [
        ("UNION ALL", vec![1, 2, 2, 3]),
        ("UNION", vec![1, 2, 3]),
        ("INTERSECT", vec![2]),
        ("INTERSECT ALL", vec![2]),
        ("EXCEPT", vec![1]),
        ("EXCEPT ALL", vec![1]),
    ] {
        let body = format!(
            "SELECT t.id FROM (SELECT id FROM source WHERE id <= 2 LIMIT 2) t \
             {operator} SELECT t.id FROM (SELECT id FROM source WHERE id >= 2 LIMIT 2) t"
        );
        let expected: Vec<_> = expected
            .into_iter()
            .map(|id| vec![Value::Int(id)])
            .collect();
        for sql in [
            format!("{body} ORDER BY id"),
            format!("SELECT t.id FROM ({body}) t ORDER BY t.id"),
        ] {
            assert_eq!(query_rows(&engine, &sql), expected, "{sql}");
        }
    }
}

#[test]
fn setop_derived_scope_join_aliases_and_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = scope_engine(dir.path());
    let sql = "SELECT t.id, UPPER(j.label) AS label, j.missing FROM \
        (SELECT id FROM source WHERE id = 1 LIMIT 1) t \
        JOIN (SELECT id, 'first' AS label, NULL AS missing FROM source LIMIT 3) j ON t.id = j.id \
        UNION ALL \
        SELECT t.id, UPPER(j.label), j.missing FROM \
        (SELECT id FROM source WHERE id = 3 LIMIT 1) t \
        JOIN (SELECT id, 'last' AS label, NULL AS missing FROM source LIMIT 3) j ON t.id = j.id \
        ORDER BY id";
    assert_eq!(
        query_rows(&engine, sql),
        vec![
            vec![Value::Int(1), Value::String("FIRST".into()), Value::Null],
            vec![Value::Int(3), Value::String("LAST".into()), Value::Null],
        ]
    );
}

#[test]
fn setop_derived_scope_empty_branch_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = scope_engine(dir.path());
    let body = "SELECT t.* FROM (SELECT id FROM source WHERE id < 0 LIMIT 1) t \
        UNION ALL SELECT t.* FROM (SELECT id FROM source WHERE id = 2 LIMIT 1) t \
        UNION ALL SELECT t.* FROM (SELECT id FROM source WHERE id = 3 LIMIT 1) t";
    assert_eq!(
        query_rows(&engine, body),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
    assert_eq!(
        query_rows(&engine, &format!("{body} ORDER BY id LIMIT 1 OFFSET 1")),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn setop_derived_scope_materialized_cte_and_cte_name_collision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = scope_engine(dir.path());
    let sql = "WITH t AS (SELECT id FROM source WHERE id = 3), \
        u AS (SELECT t.id FROM (SELECT id FROM source WHERE id = 1 LIMIT 1) t \
              UNION ALL SELECT t.id FROM (SELECT id FROM source WHERE id = 2 LIMIT 1) t \
              UNION ALL SELECT id FROM t) \
        SELECT a.id FROM u a JOIN u b ON a.id = b.id ORDER BY a.id";
    assert_eq!(
        query_rows(&engine, sql),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn setop_derived_scope_cte_join_metadata() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = scope_engine(dir.path());
    engine
        .execute("CREATE TABLE dim (k INT, label VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO dim VALUES (1, 'one'), (2, 'two'), (4, 'four')")
        .unwrap();
    for (body, expected) in [
        (
            "SELECT label, COUNT(*) n FROM
             (SELECT id FROM source UNION ALL SELECT id FROM source) d, dim
             WHERE d.id=dim.k GROUP BY label",
            vec![
                vec![Value::String("one".into()), Value::Int(2)],
                vec![Value::String("two".into()), Value::Int(2)],
            ],
        ),
        (
            "SELECT DISTINCT label, k n FROM
             (SELECT id FROM source UNION ALL SELECT id FROM source) d, dim
             WHERE d.id=dim.k",
            vec![
                vec![Value::String("one".into()), Value::Int(1)],
                vec![Value::String("two".into()), Value::Int(2)],
            ],
        ),
        (
            "SELECT label, k n FROM dim,
             (SELECT id FROM source INTERSECT SELECT id FROM source WHERE id=2)
             WHERE k=id",
            vec![vec![Value::String("two".into()), Value::Int(2)]],
        ),
    ] {
        let sql = format!(
            "WITH c AS ({body}), consumer AS (SELECT label, n FROM c)
             SELECT * FROM consumer ORDER BY label"
        );
        assert_eq!(query_rows(&engine, &sql), expected, "{sql}");
        let columns = engine
            .describe_prepared_statement(&mut SessionContext::default(), &sql)
            .unwrap();
        assert_eq!(
            columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["label", "n"]
        );
        assert_eq!(columns[0].column_type, ColumnType::VarString);
    }
}

#[test]
fn setop_derived_scope_join_describe_preserves_outer_cte_and_nulls() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = scope_engine(dir.path());
    let sql = "WITH d AS (SELECT 9 AS marker), c AS (
        SELECT a.id, d.marker, j.label FROM
        (SELECT id FROM source UNION SELECT id FROM source) a
        LEFT JOIN (SELECT 2 AS id, CAST('two' AS CHAR(3)) AS label) j ON a.id=j.id
        CROSS JOIN d) SELECT * FROM c ORDER BY id";
    let columns = engine
        .describe_prepared_statement(&mut SessionContext::default(), sql)
        .unwrap();
    assert_eq!(
        columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["id", "marker", "label"]
    );
    assert_eq!(
        query_rows(&engine, sql),
        vec![
            vec![Value::Int(1), Value::Int(9), Value::Null],
            vec![Value::Int(2), Value::Int(9), Value::String("two".into())],
            vec![Value::Int(3), Value::Int(9), Value::Null],
        ]
    );
}

fn check_tpcds_empty_catalog(sql: &str) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET sql_mode='PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    for ddl in include_str!("../../../../scripts/tpcds/schema.sql")
        .split(';')
        .filter(|s| !s.trim().is_empty())
    {
        engine
            .execute_with_session(&mut session, ddl)
            .unwrap_or_else(|e| panic!("{ddl}: {e:?}"));
    }
    for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
        engine
            .execute_with_session(&mut session, statement)
            .and_then(QueryResponse::materialize)
            .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
    }
}

#[test]
fn setop_derived_scope_tpcds_q05() {
    check_tpcds_empty_catalog(include_str!("../../../../scripts/tpcds/queries/q05.sql"));
}

#[test]
fn setop_derived_scope_tpcds_q14() {
    check_tpcds_empty_catalog(include_str!("../../../../scripts/tpcds/queries/q14.sql"));
}

#[test]
fn setop_derived_scope_tpcds_q54() {
    check_tpcds_empty_catalog(include_str!("../../../../scripts/tpcds/queries/q54.sql"));
}
