use super::*;

fn explain_tree(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected explain result set");
    };
    result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn setup_q5_shape(engine: &StorageQueryEngine) {
    engine
        .execute(
            "CREATE TABLE date_dim (\
             d_date_sk INT PRIMARY KEY, \
             d_date DATE, \
             KEY idx_d_date (d_date))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE store (\
             s_store_sk INT PRIMARY KEY, \
             s_store_id VARCHAR(16))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE fact_sales (\
             id INT PRIMARY KEY, \
             f_date_sk INT, \
             f_store_sk INT, \
             f_amt INT, \
             KEY idx_f_date (f_date_sk))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE fact_returns (\
             id INT PRIMARY KEY, \
             r_date_sk INT, \
             r_store_sk INT, \
             r_amt INT, \
             KEY idx_r_date (r_date_sk))",
        )
        .unwrap();

    insert_fixture_rows(
        engine,
        "test.date_dim",
        (1..=40).map(|d| {
            vec![
                Value::Int(d),
                Value::String(format!("2020-01-{:02}", (d - 1) % 28 + 1)),
            ]
        }),
    );
    engine
        .execute("INSERT INTO store VALUES (1, 's1'), (2, 's2'), (3, 's3')")
        .unwrap();
    for (table, rows, amount) in [("test.fact_sales", 400, 10), ("test.fact_returns", 80, 1)] {
        insert_fixture_rows(
            engine,
            table,
            (1..=rows).map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id % 40 + 1),
                    Value::Int(id % 3 + 1),
                    Value::Int(amount),
                ]
            }),
        );
    }
    for table in ["date_dim", "store", "fact_sales", "fact_returns"] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }
}

fn q5_shape_sql() -> &'static str {
    "SELECT s_store_id, SUM(amount) AS net_amt \
     FROM (\
       SELECT f_store_sk AS store_sk, f_date_sk AS date_sk, f_amt AS amount FROM fact_sales \
       UNION ALL \
       SELECT r_store_sk AS store_sk, r_date_sk AS date_sk, -r_amt AS amount FROM fact_returns\
     ) salesreturns, date_dim, store \
     WHERE date_sk = d_date_sk \
       AND d_date BETWEEN CAST('2020-01-05' AS DATE) AND CAST('2020-01-06' AS DATE) \
       AND store_sk = s_store_sk \
     GROUP BY s_store_id \
     ORDER BY s_store_id"
}

fn q5_shape_sql_returns_first() -> &'static str {
    "SELECT s_store_id, SUM(amount) AS net_amt \
     FROM (\
       SELECT r_store_sk AS store_sk, r_date_sk AS date_sk, -r_amt AS amount FROM fact_returns \
       UNION ALL \
       SELECT f_store_sk AS store_sk, f_date_sk AS date_sk, f_amt AS amount FROM fact_sales\
     ) salesreturns, date_dim, store \
     WHERE date_sk = d_date_sk \
       AND d_date BETWEEN CAST('2020-01-05' AS DATE) AND CAST('2020-01-06' AS DATE) \
       AND store_sk = s_store_sk \
     GROUP BY s_store_id \
     ORDER BY s_store_id"
}

#[test]
fn union_all_derived_pushes_dimension_filter_into_branches() {
    let _guard = test_env_lock();
    with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", None)], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        setup_q5_shape(&engine);

        let QueryResponse::ResultSet(oracle) =
            with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", Some("0"))], || {
                engine.execute(q5_shape_sql()).unwrap()
            })
        else {
            panic!("expected result set");
        };
        let QueryResponse::ResultSet(result) = engine.execute(q5_shape_sql()).unwrap() else {
            panic!("expected result set");
        };
        assert_eq!(result.rows, oracle.rows);

        let tree = explain_tree(&engine, &format!("EXPLAIN FORMAT=TREE {}", q5_shape_sql()));
        assert!(
            tree.contains("Index lookup on fact_sales using idx_f_date"),
            "expected pushed date predicate to drive fact_sales date index: {tree}"
        );
        let returns_first_tree = explain_tree(
            &engine,
            &format!("EXPLAIN FORMAT=TREE {}", q5_shape_sql_returns_first()),
        );
        assert!(
            returns_first_tree.contains("Index lookup on fact_returns using idx_r_date"),
            "expected pushed date predicate to drive fact_returns date index: {returns_first_tree}"
        );
    });
}

#[test]
fn union_all_derived_pushdown_has_kill_switch() {
    let _guard = test_env_lock();
    with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", Some("0"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        setup_q5_shape(&engine);

        let tree = explain_tree(&engine, &format!("EXPLAIN FORMAT=TREE {}", q5_shape_sql()));
        assert!(
            !tree.contains("Index lookup on fact_sales using idx_f_date"),
            "kill switch should leave UNION ALL branch unfiltered: {tree}"
        );
    });
}

#[test]
fn union_all_join_source_pushdown_is_independent_of_from_position_and_on_syntax() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    setup_q5_shape(&engine);
    let producer = "(SELECT f_store_sk AS store_sk,f_date_sk AS date_sk,f_amt AS amount FROM fact_sales UNION ALL SELECT r_store_sk,r_date_sk,-r_amt FROM fact_returns)";
    let comma = format!("SELECT s_store_id,SUM(amount) AS total FROM date_dim,{producer} u,store WHERE date_sk=d_date_sk AND d_date BETWEEN CAST('2020-01-05' AS DATE) AND CAST('2020-01-06' AS DATE) AND store_sk=s_store_sk GROUP BY s_store_id ORDER BY s_store_id");
    let on = format!("SELECT s.s_store_id,SUM(u.amount) AS total FROM date_dim d JOIN {producer} u ON u.date_sk=d.d_date_sk JOIN store s ON u.store_sk=s.s_store_sk WHERE d.d_date BETWEEN CAST('2020-01-05' AS DATE) AND CAST('2020-01-06' AS DATE) AND s.s_store_id='s1' GROUP BY s.s_store_id ORDER BY s.s_store_id");
    for sql in [&comma, &on] {
        let Statement::Select(mut select) = parse_statement(sql).unwrap() else {
            unreachable!()
        };
        assert!(select.derived.is_none());
        assert!(select.joins.iter().any(|join| join.derived.is_some()));
        let pushed = with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", Some("1"))], || {
            crate::planner_decorrelate::derived::pushdown_derived_predicates_with_db(
                engine.db_for_tests(),
                &mut select,
            )
        });
        assert!(pushed >= 2, "failed to push joined UNION source: {pushed}");
        let oracle = with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", Some("0"))], || {
            engine.execute(sql).unwrap().materialize().unwrap()
        });
        let result = with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", Some("1"))], || {
            engine.execute(sql).unwrap().materialize().unwrap()
        });
        let (QueryResponse::ResultSet(oracle), QueryResponse::ResultSet(result)) = (oracle, result)
        else {
            unreachable!()
        };
        assert_eq!(result.rows, oracle.rows);
    }
}

#[test]
fn union_join_pushdown_preserves_typed_keys_nulls_and_dimension_duplicates() {
    let _guard = test_env_lock();
    for (ty, selected, rejected) in [
        ("INT", "1", "2"),
        ("BIGINT UNSIGNED", "18446744073709551614", "2"),
        ("DECIMAL(20,3)", "1.250", "2.500"),
        ("DOUBLE", "1.25", "2.5"),
        ("DATE", "'2020-01-01'", "'2020-01-02'"),
        ("VARCHAR(12)", "'a'", "'b'"),
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute(&format!("CREATE TABLE typed_dim(k {ty}, enabled INT)"))
            .unwrap();
        engine
            .execute(&format!("CREATE TABLE typed_a(k {ty}, v INT)"))
            .unwrap();
        engine
            .execute(&format!("CREATE TABLE typed_b(k {ty}, v INT)"))
            .unwrap();
        engine.execute(&format!("INSERT INTO typed_dim VALUES ({selected},1),({selected},1),({rejected},0),(NULL,1)")).unwrap();
        engine.execute(&format!("INSERT INTO typed_a VALUES ({selected},3),({selected},5),({rejected},100),(NULL,1000)")).unwrap();
        engine
            .execute(&format!(
                "INSERT INTO typed_b VALUES ({selected},7),({rejected},200),(NULL,2000)"
            ))
            .unwrap();
        let sql = "SELECT COUNT(*),SUM(v) FROM typed_dim d JOIN (SELECT k,v FROM typed_a UNION ALL SELECT k,v FROM typed_b) u ON d.k=u.k WHERE d.enabled=1";
        let Statement::Select(mut select) = parse_statement(sql).unwrap() else {
            unreachable!()
        };
        let pushed = with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", Some("1"))], || {
            crate::planner_decorrelate::derived::pushdown_derived_predicates_with_db(
                engine.db_for_tests(),
                &mut select,
            )
        });
        assert_eq!(pushed, 2, "{ty}");
        let oracle = with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", Some("0"))], || {
            engine.execute(sql).unwrap().materialize().unwrap()
        });
        let result = with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", Some("1"))], || {
            engine.execute(sql).unwrap().materialize().unwrap()
        });
        let (QueryResponse::ResultSet(oracle), QueryResponse::ResultSet(result)) = (oracle, result)
        else {
            unreachable!()
        };
        assert_eq!(result.rows, oracle.rows, "{ty}");
        assert_eq!(
            result.rows,
            vec![vec![Value::Int(6), Value::DecimalCompact(30, 0)]],
            "{ty}"
        );
    }
}

#[test]
fn union_join_pushdown_declines_outer_joins_volatile_and_mixed_domains() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ud_dim(k INT, enabled INT)")
        .unwrap();
    engine.execute("CREATE TABLE ud_a(k INT, v INT)").unwrap();
    engine.execute("CREATE TABLE ud_b(k INT, v INT)").unwrap();
    engine
        .execute("CREATE TABLE ud_text(k VARCHAR(12), v INT)")
        .unwrap();
    let producer = "(SELECT k,v FROM ud_a UNION ALL SELECT k,v FROM ud_b)";
    for sql in [
        format!("SELECT * FROM ud_dim d LEFT JOIN {producer} u ON d.k=u.k WHERE d.enabled=1"),
        format!("SELECT * FROM ud_dim d RIGHT JOIN {producer} u ON d.k=u.k WHERE d.enabled=1"),
        format!("SELECT * FROM ud_dim d JOIN {producer} u ON d.k=u.k WHERE d.enabled=RAND()"),
        format!("SELECT * FROM ud_dim d JOIN {producer} u ON d.k=u.k WHERE d.enabled=@enabled"),
        format!("SELECT * FROM ud_dim d JOIN {producer} u ON d.k=u.k WHERE d.enabled=DEFAULT(enabled)"),
        format!("SELECT * FROM ud_dim d JOIN {producer} u ON d.k=u.k WHERE mystery(d.enabled)=1"),
        format!("SELECT * FROM ud_dim d JOIN {producer} u ON d.k=u.k WHERE test.abs(d.enabled)=1"),
        format!("SELECT * FROM ud_dim d JOIN {producer} u ON d.k=u.k WHERE d.enabled=1 FOR UPDATE"),
        "SELECT * FROM ud_dim d JOIN (SELECT k,RAND() v FROM ud_a UNION ALL SELECT k,v FROM ud_b) u ON d.k=u.k WHERE d.enabled=1".into(),
        "SELECT * FROM ud_dim d JOIN ((SELECT k,v FROM ud_a LIMIT 1) UNION ALL SELECT k,v FROM ud_b) u ON d.k=u.k WHERE d.enabled=1".into(),
        "SELECT * FROM ud_dim d JOIN (SELECT k,v FROM ud_a UNION SELECT k,v FROM ud_b) u ON d.k=u.k WHERE d.enabled=1".into(),
        "SELECT * FROM ud_dim d JOIN (SELECT k,v FROM ud_a UNION ALL SELECT k,v FROM ud_text) u ON d.k=u.k WHERE d.enabled=1".into(),
        "SELECT * FROM ud_dim d JOIN (SELECT k,v FROM ud_a FOR SYSTEM_TIME AS OF '2020-01-01 00:00:00' UNION ALL SELECT k,v FROM ud_b) u ON d.k=u.k WHERE d.enabled=1".into(),
    ] {
        let Statement::Select(mut select) = parse_statement(&sql).unwrap() else { unreachable!() };
        let pushed = with_test_env(&[("MYDB_UNION_DERIVED_PUSHDOWN", Some("1"))], || crate::planner_decorrelate::derived::pushdown_derived_predicates_with_db(engine.db_for_tests(), &mut select));
        assert_eq!(pushed, 0, "unsafe pushdown in {sql}");
    }
}
