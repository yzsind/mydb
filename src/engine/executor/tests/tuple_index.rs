use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("{sql}: {other:?}"),
    }
}

fn explain(engine: &StorageQueryEngine, sql: &str) -> String {
    rows(engine, &format!("EXPLAIN FORMAT=TREE {sql}"))
        .into_iter()
        .flatten()
        .map(|v| crate::storage::value_to_string(&v))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn boolean_range_numeric_text_endpoints() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for (table, ty, extra) in [
        ("signed_ids", "BIGINT", "-9223372036854775808"),
        ("unsigned_ids", "BIGINT UNSIGNED", "18446744073709551615"),
    ] {
        engine
            .execute(&format!(
                "CREATE TABLE {table} (id {ty} PRIMARY KEY,payload TEXT)"
            ))
            .unwrap();
        engine.execute(&format!("INSERT INTO {table} VALUES (0,'zero'),(200000,'before'),(200001,'edge'),(200002,'after'),(220001,'end'),(220002,'outside'),(9007199254740992,'large'),(9007199254740993,'adjacent'),({extra},'extreme')")).unwrap();
        for upper in [
            "'200001'",
            "'220001'",
            "'220001.000'",
            "'+00220001'",
            "'2.20001e5'",
            "' 220001 '",
        ] {
            let predicate = format!("id>'200001' AND (id<{upper} OR id={upper})");
            let sql = format!("SELECT * FROM {table} FORCE INDEX(PRIMARY) WHERE {predicate}");
            let plan = explain(&engine, &sql);
            assert!(plan.contains("id>const AND id<=const"), "{plan}");
            for mode in [VecMode::On, VecMode::Off] {
                with_vec_mode(mode, || differential(&engine, table, "PRIMARY", &predicate));
            }
        }
        for upper in [
            "'200001.5'",
            "'200001.0000000000000001'",
            "'9007199254740992'",
            "'9007199254740993'",
            "'18446744073709551615'",
            "'200001x'",
            "'-1'",
            "NULL",
        ] {
            let predicate = format!("id>'200001' AND (id<{upper} OR id={upper})");
            differential(&engine, table, "PRIMARY", &predicate);
        }
        assert_eq!(rows(&engine, &format!("SELECT COUNT(*) FROM {table} WHERE id>'200001' AND (id<'200001' OR id='200001')")), vec![vec![Value::Int(0)]]);
    }
}

#[test]
fn boolean_range_numeric_text_composite_and_prepared() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT PRIMARY KEY,tenant BIGINT UNSIGNED,a BIGINT,b BIGINT,payload TEXT,KEY ab(tenant,a,b))").unwrap();
    engine.execute("INSERT INTO t VALUES (1,1,-1,0,'before'),(2,1,1,1,'edge'),(3,1,1,2,'after'),(4,1,2,0,'end'),(5,1,2,1,'outside'),(6,2,1,2,'tenant'),(7,1,NULL,2,'null')").unwrap();
    engine
        .execute(
            "INSERT INTO t SELECT n+100,1,n+100,0,'outside' FROM generate_series(1,1000) AS s(n)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    let predicate = "tenant='1' AND (a>'1' OR (a='1' AND b>'1')) AND (a<'2' OR (a='2' AND b<='0'))";
    let plan = explain(
        &engine,
        &format!("SELECT * FROM t FORCE INDEX(ab) WHERE {predicate}"),
    );
    assert!(
        plan.contains("(tenant,a,b) > (const,const,const)"),
        "{plan}"
    );
    assert!(
        plan.contains("(tenant,a,b) <= (const,const,const)"),
        "{plan}"
    );
    differential(&engine, "t", "PRIMARY,ab", predicate);
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "PREPARE br FROM 'SELECT * FROM t FORCE INDEX(ab) WHERE tenant=? AND (a<? OR a=?) ORDER BY id'").unwrap();
    for bound in [
        "'2'",
        "'1'",
        "'1.5'",
        "'9007199254740993'",
        "'2x'",
        "NULL",
        "'-1'",
        "'2'",
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET @tenant='1',@bound={bound}"))
            .unwrap();
        let QueryResponse::ResultSet(actual) = engine
            .execute_with_session(&mut session, "EXECUTE br USING @tenant,@bound,@bound")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(actual.rows, rows(&engine, &format!("SELECT * FROM t IGNORE INDEX(PRIMARY,ab) WHERE tenant='1' AND (a<{bound} OR a={bound}) ORDER BY id")), "binding: {bound}");
    }
    let schema = engine.db_for_tests().table("t").unwrap();
    let index = schema
        .indexes
        .iter()
        .find(|index| index.name == "ab")
        .unwrap();
    let mut statement = parse_statement("SELECT * FROM t WHERE tenant=? AND (a<? OR a=?)").unwrap();
    crate::parser::number_statement_placeholders(&mut statement, &mut 0);
    let Statement::Select(select) = statement else {
        panic!()
    };
    for bound in ["2", "1", "9007199254740993", "1.5", "2"] {
        crate::engine::params::with_params(
            vec![
                Value::String("1".into()),
                Value::String(bound.into()),
                Value::String(bound.into()),
            ],
            || {
                let mut low = None;
                let mut high = None;
                crate::planner::boolean_range::refine_runtime(
                    &schema,
                    index,
                    select.selection.as_ref(),
                    &mut low,
                    &mut high,
                );
                let high = high.unwrap();
                let safe = matches!(bound, "1" | "2");
                assert_eq!(
                    high.key.0.len(),
                    if safe { 2 } else { 1 },
                    "binding: {bound}, {high:?}"
                );
                if safe {
                    assert_eq!(
                        high.key.0[1],
                        crate::storage::KeyPart::Int(bound.parse().unwrap())
                    );
                }
            },
        );
    }
    let mut statement = parse_statement(
        "SELECT * FROM t WHERE tenant=? AND (a<CAST(? AS CHAR) OR a=CAST(? AS CHAR))",
    )
    .unwrap();
    crate::parser::number_statement_placeholders(&mut statement, &mut 0);
    let Statement::Select(select) = statement else {
        panic!()
    };
    crate::engine::params::with_params(
        vec![
            Value::String("1".into()),
            Value::String("2".into()),
            Value::String("2".into()),
        ],
        || {
            let mut low = None;
            let mut high = None;
            crate::planner::boolean_range::refine_runtime(
                &schema,
                index,
                select.selection.as_ref(),
                &mut low,
                &mut high,
            );
            assert_eq!(
                high.unwrap().key.0.len(),
                1,
                "nested parameters must remain expressions"
            );
        },
    );
}

#[test]
fn tuple_index_reordered_in_and_between() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a INT, b INT, payload VARCHAR(100), PRIMARY KEY(a,b))")
        .unwrap();
    let values = (0..2000)
        .map(|i| format!("({},{},'payload')", i / 10, i % 10))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    let sql = "SELECT a,b FROM t WHERE (b,a) IN ((2,10),(3,11),(2,10)) ORDER BY a,b";
    let plan = explain(&engine, sql);
    assert!(
        plan.contains("Index multi lookup") && plan.contains("probes=2"),
        "{plan}"
    );
    assert_eq!(
        rows(&engine, sql),
        vec![
            vec![Value::Int(10), Value::Int(2)],
            vec![Value::Int(11), Value::Int(3)]
        ]
    );
    let sql = "SELECT a,b FROM t WHERE (a,b) BETWEEN (10,2) AND (11,3) ORDER BY a,b";
    assert_eq!(rows(&engine, sql), rows(&engine, "SELECT a,b FROM t IGNORE INDEX (PRIMARY) WHERE (a>10 OR (a=10 AND b>=2)) AND (a<11 OR (a=11 AND b<=3)) ORDER BY a,b"));
}

fn differential(engine: &StorageQueryEngine, table: &str, indexes: &str, predicate: &str) {
    let indexed =
        format!("SELECT * FROM {table} FORCE INDEX ({indexes}) WHERE {predicate} ORDER BY id");
    let scanned =
        format!("SELECT * FROM {table} IGNORE INDEX ({indexes}) WHERE {predicate} ORDER BY id");
    assert_eq!(
        rows(engine, &indexed),
        rows(engine, &scanned),
        "predicate: {predicate}\n{}",
        explain(engine, &indexed)
    );
}

#[test]
fn tuple_index_integer_ranges_nulls_and_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT, KEY abc(a,b,c))")
        .unwrap();
    let mut values = Vec::new();
    for a in ["NULL", "-2", "0", "1", "2", "3"] {
        for b in ["NULL", "-1", "0", "1", "2", "4"] {
            for c in 0..3 {
                values.push(format!("({},{a},{b},{c})", values.len()));
            }
        }
    }
    engine
        .execute(&format!("INSERT INTO t VALUES {}", values.join(",")))
        .unwrap();
    for op in ["=", "<=>", ">", ">=", "<", "<=", "<>"] {
        for bound in ["(1,1)", "(1,NULL)", "(NULL,1)", "(-2,-1)", "(9,9)"] {
            differential(&engine, "t", "PRIMARY,abc", &format!("(a,b) {op} {bound}"));
            differential(&engine, "t", "PRIMARY,abc", &format!("{bound} {op} (a,b)"));
            differential(
                &engine,
                "t",
                "PRIMARY,abc",
                &format!("a=1 AND (b,c) {op} {bound}"),
            );
        }
    }
    for pred in [
        "(a,b) BETWEEN (-2,1) AND (2,0)",
        "(a,b) BETWEEN (2,0) AND (-2,1)",
        "(a,b) NOT BETWEEN (-2,1) AND (2,0)",
        "(a,b) BETWEEN (1,NULL) AND (2,0)",
        "(a,b) > (1,1) AND (a,b) <= (2,0)",
        "(b,a) IN ((1,2),(2,1),(NULL,1),(1,2))",
        "(a,b) NOT IN ((1,2),(NULL,1))",
        "a=1 AND (b,c) IN ((1,2),(2,1))",
        "(a,c) > (1,1)",
        "(a,b,c) >= (1,2,1)",
    ] {
        differential(&engine, "t", "PRIMARY,abc", pred);
    }
}

#[test]
fn tuple_index_type_matrix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let cases = [
        ("BIGINT", "-9223372036854775808", "0", "9223372036854775807"),
        (
            "BIGINT UNSIGNED",
            "0",
            "9223372036854775808",
            "18446744073709551615",
        ),
        (
            "DECIMAL(20,4)",
            "-123456789012.1234",
            "0.1250",
            "123456789012.1234",
        ),
        (
            "DECIMAL(65,10)",
            "-1234567890123456789012345678901234567890.1234567890",
            "0.125",
            "1234567890123456789012345678901234567890.1234567890",
        ),
        ("FLOAT", "-1.5", "0", "1.5"),
        ("DOUBLE", "-1.25", "0", "1.25"),
        ("VARCHAR(20) COLLATE utf8mb4_bin", "'A'", "'a'", "'z'"),
        (
            "VARCHAR(20) COLLATE utf8mb4_0900_ai_ci",
            "'a'",
            "'B'",
            "'é'",
        ),
        ("CHAR(10)", "'a'", "'b'", "'z'"),
        ("VARBINARY(10)", "X'00'", "X'41'", "X'FF'"),
        ("DATE", "'2020-01-01'", "'2026-09-10'", "'2030-12-31'"),
        (
            "DATETIME(6)",
            "'2020-01-01 00:00:00.000001'",
            "'2026-09-10 12:55:19.123456'",
            "'2030-12-31 23:59:59.999999'",
        ),
        (
            "TIMESTAMP",
            "'2020-01-01 00:00:00'",
            "'2026-09-10 12:55:19'",
            "'2030-12-31 23:59:59'",
        ),
        (
            "TIME(6)",
            "'-120:00:00.123456'",
            "'00:00:00'",
            "'120:00:00.123456'",
        ),
        ("YEAR", "2000", "2026", "2030"),
        ("BIT(8)", "0", "127", "255"),
        ("ENUM('z','a','b')", "'z'", "'a'", "'b'"),
        ("SET('a','b','c')", "'a'", "'b'", "'a,c'"),
    ];
    for (i, (ty, lo, mid, hi)) in cases.iter().enumerate() {
        let t = format!("typed_{i}");
        engine
            .execute(&format!(
                "CREATE TABLE {t} (id INT PRIMARY KEY,a {ty},b INT,KEY ab(a,b))"
            ))
            .unwrap();
        engine.execute(&format!("INSERT INTO {t} VALUES (1,{lo},1),(2,{mid},1),(3,{mid},2),(4,{hi},1),(5,NULL,1),(6,{mid},NULL)")).unwrap();
        let point_plan = explain(
            &engine,
            &format!("SELECT id FROM {t} FORCE INDEX(ab) WHERE (a,b) IN (({hi},1))"),
        );
        assert!(
            point_plan.contains("Index multi lookup"),
            "type {ty}: {point_plan}"
        );
        for op in ["=", ">", ">=", "<", "<="] {
            differential(&engine, &t, "PRIMARY,ab", &format!("(a,b) {op} ({mid},1)"));
        }
        differential(
            &engine,
            &t,
            "PRIMARY,ab",
            &format!("(b,a) IN ((1,{mid}),(1,{hi}),(1,{mid}))"),
        );
        differential(
            &engine,
            &t,
            "PRIMARY,ab",
            &format!("(a,b) BETWEEN ({lo},2) AND ({hi},0)"),
        );
    }
}

#[test]
fn tuple_index_mixed_collations_match_scalar_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT PRIMARY KEY,a VARCHAR(10) COLLATE utf8mb4_bin,b VARCHAR(10) COLLATE utf8mb4_0900_ai_ci,KEY ab(a,b))").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'A','x'),(2,'a','X'),(3,'a','x'),(4,'b','z')")
        .unwrap();
    let expected = rows(
        &engine,
        "SELECT id FROM t IGNORE INDEX(PRIMARY,ab) WHERE a='a' AND b='x' ORDER BY id",
    );
    assert_eq!(expected, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    for hint in ["FORCE INDEX(ab)", "IGNORE INDEX(PRIMARY,ab)"] {
        for pred in [
            "(a,b)=('a','x')",
            "(a,b) IN (('a','x'))",
            "(a,b) BETWEEN ('a','x') AND ('a','x')",
        ] {
            assert_eq!(
                rows(
                    &engine,
                    &format!("SELECT id FROM t {hint} WHERE {pred} ORDER BY id")
                ),
                expected,
                "{hint} {pred}"
            );
        }
    }
}

#[test]
fn tuple_index_lossy_domains_keep_scan_results() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,a VARCHAR(10),b INT,KEY ab(a,b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'01',1),(2,'1',2),(3,'x',3),(4,'20',4)")
        .unwrap();
    for pred in [
        "(a,b) IN ((1,1),(1,2))",
        "(a,b)>(1,1)",
        "(a,b) BETWEEN (0,0) AND (2,3)",
    ] {
        differential(&engine, "t", "PRIMARY,ab", pred);
    }
    engine
        .execute("CREATE TABLE n (id INT PRIMARY KEY,a INT,b DECIMAL(5,2),KEY ab(a,b))")
        .unwrap();
    engine
        .execute("INSERT INTO n VALUES (1,1,1.23),(2,1,1.24),(3,2,0),(4,1,NULL)")
        .unwrap();
    for pred in [
        "(a,b)>(1,1.234)",
        "(a,b)<(1,1.234)",
        "(a,b) IN ((1,1.234),(2,0))",
    ] {
        differential(&engine, "n", "PRIMARY,ab", pred);
    }
}

#[test]
fn tuple_index_prepared_rebinding_and_bound_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,a BIGINT UNSIGNED,b DECIMAL(5,2),KEY ab(a,b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,0,1.23),(2,1,1.23),(3,1,1.24),(4,2,0),(5,1,NULL)")
        .unwrap();
    let mut session = SessionContext::default();
    for (name, pred) in [
        ("rng", "(a,b) > (?,?)"),
        ("btw", "(a,b) BETWEEN (?,?) AND (?,?)"),
        ("lst", "(b,a) IN ((?,?),(?,?))"),
    ] {
        engine.execute_with_session(&mut session,&format!("PREPARE {name} FROM 'SELECT id FROM t FORCE INDEX(ab) WHERE {pred} ORDER BY id'")).unwrap();
    }
    for (a, b) in [
        ("1", "1.23"),
        ("1", "1.234"),
        ("-1", "0"),
        ("0", "1.23"),
        ("18446744073709551615", "0"),
        ("NULL", "0"),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET @a={a},@b={b}"))
            .unwrap();
        let QueryResponse::ResultSet(actual) = engine
            .execute_with_session(&mut session, "EXECUTE rng USING @a,@b")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            actual.rows,
            rows(
                &engine,
                &format!(
                    "SELECT id FROM t IGNORE INDEX(PRIMARY,ab) WHERE (a,b)>({a},{b}) ORDER BY id"
                )
            ),
            "{a},{b}"
        );
    }
    for vals in [
        ("0", "0", "2", "0"),
        ("2", "0", "0", "0"),
        ("1", "1.234", "1", "1.24"),
    ] {
        let (a, b, c, d) = vals;
        engine
            .execute_with_session(&mut session, &format!("SET @a={a},@b={b},@c={c},@d={d}"))
            .unwrap();
        for (name, pred) in [
            ("btw", format!("(a,b) BETWEEN ({a},{b}) AND ({c},{d})")),
            ("lst", format!("(b,a) IN (({a},{b}),({c},{d}))")),
        ] {
            let QueryResponse::ResultSet(actual) = engine
                .execute_with_session(&mut session, &format!("EXECUTE {name} USING @a,@b,@c,@d"))
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(
                actual.rows,
                rows(
                    &engine,
                    &format!("SELECT id FROM t IGNORE INDEX(PRIMARY,ab) WHERE {pred} ORDER BY id")
                ),
                "{name} {pred}"
            );
        }
    }
}

#[test]
fn tuple_index_partition_session_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE session (id BIGINT UNSIGNED NOT NULL,create_time DATETIME NOT NULL,payload VARCHAR(200),PRIMARY KEY(id,create_time)) PARTITION BY RANGE(TO_DAYS(create_time)) (PARTITION old VALUES LESS THAN (739039), PARTITION pmaxvalue VALUES LESS THAN MAXVALUE)").unwrap();
    let mut vals = (0..5000)
        .map(|i| format!("({i},'2023-01-01 00:00:00','payload')"))
        .collect::<Vec<_>>();
    for (id, time) in [
        (1293260, "19"),
        (1293258, "18"),
        (1293259, "19"),
        (1293257, "18"),
    ] {
        vals.push(format!("({id},'2026-09-10 12:55:{time}','target')"));
    }
    engine
        .execute(&format!("INSERT INTO session VALUES {}", vals.join(",")))
        .unwrap();
    let pred="1=1 AND (create_time,id) IN (('2026-09-10 12:55:19',1293260),('2026-09-10 12:55:18',1293258),('2026-09-10 12:55:19',1293259),('2026-09-10 12:55:18',1293257))";
    let sql = format!("SELECT COUNT(1) FROM session WHERE {pred}");
    let plan = explain(&engine, &sql);
    assert!(
        plan.contains("Index multi lookup") && plan.contains("probes=4"),
        "{plan}"
    );
    assert!(
        plan.contains("rows=4"),
        "tautology must not discount tuple selectivity: {plan}"
    );
    assert_eq!(rows(&engine, &sql), vec![vec![Value::Int(4)]]);
    assert_eq!(
        rows(&engine, &sql),
        rows(
            &engine,
            &format!("SELECT COUNT(1) FROM session IGNORE INDEX(PRIMARY) WHERE {pred}")
        )
    );
}

#[test]
fn tuple_between_constant_null_and_arity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for (sql, expected) in [
        ("SELECT (1,2) BETWEEN (1,1) AND (2,0)", Value::Int(1)),
        ("SELECT (1,NULL) BETWEEN (0,9) AND (2,0)", Value::Int(1)),
        ("SELECT (1,NULL) BETWEEN (1,1) AND (2,0)", Value::Null),
        ("SELECT (1,NULL) NOT BETWEEN (2,0) AND (3,0)", Value::Int(1)),
    ] {
        assert_eq!(rows(&engine, sql), vec![vec![expected]], "{sql}");
    }
    assert_eq!(
        engine
            .execute("SELECT (1,2) BETWEEN (1,2,3) AND (2,3)")
            .unwrap_err()
            .code(),
        1241
    );
}

#[test]
fn tuple_index_cost_selectivity_and_broad_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,a INT,b INT,payload VARCHAR(512),KEY ab(a,b))")
        .unwrap();
    let vals = (0..4000)
        .map(|i| format!("({i},{},{},'{}')", i / 10, i % 10, "x".repeat(256)))
        .collect::<Vec<_>>();
    engine
        .execute(&format!("INSERT INTO t VALUES {}", vals.join(",")))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    let narrow = explain(
        &engine,
        "SELECT payload FROM t WHERE (a,b) BETWEEN (200,2) AND (200,4)",
    );
    assert!(
        narrow.contains("Index range scan") || narrow.contains("Index fast"),
        "{narrow}"
    );
    let broad = explain(&engine, "SELECT payload FROM t WHERE (a,b) >= (-1,0)");
    assert!(
        broad.contains("Table scan") || broad.contains("table scan"),
        "{broad}"
    );
    let point = explain(&engine, "SELECT id FROM t WHERE (a,b) IN ((200,2),(200,3))");
    assert!(
        point.contains("Index multi lookup") && point.contains("probes=2"),
        "{point}"
    );
    // Strict prefix bounds exclude ALL suffixes at the boundary, inclusive
    // bounds retain them. LIMIT/ORDER BY must agree with the scalar oracle.
    for op in [">", ">=", "<", "<="] {
        let query=format!("SELECT a,b FROM t FORCE INDEX(ab) WHERE (a,b) {op} (200,2) ORDER BY a DESC,b DESC LIMIT 7");
        let oracle=format!("SELECT a,b FROM t IGNORE INDEX(PRIMARY,ab) WHERE (a,b) {op} (200,2) ORDER BY a DESC,b DESC LIMIT 7");
        assert_eq!(rows(&engine, &query), rows(&engine, &oracle), "{query}");
    }
}

#[test]
fn tuple_index_prefix_string_and_nullable_unique() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,a VARCHAR(30),b INT,KEY ab(a(2),b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'abc',1),(2,'abd',1),(3,'abc',2),(4,'ab',2),(5,'b',0)")
        .unwrap();
    for pred in [
        "(a,b) IN (('abc',1),('abd',1))",
        "(a,b)>('abc',1)",
        "(a,b) BETWEEN ('abc',0) AND ('abd',2)",
        "(b,a) IN ((1,'abc')) AND id>0",
    ] {
        differential(&engine, "t", "PRIMARY,ab", pred);
    }
    engine
        .execute("CREATE TABLE u (id INT PRIMARY KEY,a INT,b INT,UNIQUE KEY ab(a,b))")
        .unwrap();
    engine
        .execute("INSERT INTO u VALUES (1,1,NULL),(2,1,NULL),(3,NULL,2),(4,NULL,2),(5,1,2)")
        .unwrap();
    for pred in [
        "(a,b)<=> (1,NULL)",
        "(a,b)<=> (NULL,2)",
        "(a,b) IN ((1,NULL),(1,2))",
    ] {
        differential(&engine, "u", "PRIMARY,ab", pred);
    }
}

#[test]
fn tuple_index_prepared_range_keeps_both_endpoints() {
    use crate::planner::PhysicalPlan;
    fn bounds(plan: &PhysicalPlan) -> Option<(&Option<IndexBound>, &Option<IndexBound>)> {
        match plan {
            PhysicalPlan::IndexRangeScan { low, high, .. }
            | PhysicalPlan::IndexFastScan { low, high, .. } => Some((low, high)),
            _ => plan.children().into_iter().find_map(bounds),
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a INT,b INT,payload VARCHAR(100),PRIMARY KEY(a,b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,1,'one'),(2,2,'two'),(3,3,'three')")
        .unwrap();
    let Statement::Select(select) = parse_statement(
        "SELECT a,b FROM t FORCE INDEX(PRIMARY) WHERE (a,b) BETWEEN (?,?) AND (?,?)",
    )
    .unwrap() else {
        panic!()
    };
    let plan = crate::engine::params::with_params(
        vec![Value::Int(1), Value::Int(1), Value::Int(2), Value::Int(2)],
        || crate::planner::plan_prepared_physical_select(engine.db_for_tests(), &select).unwrap(),
    );
    let (low, high) = bounds(&plan).unwrap_or_else(|| panic!("{plan:?}"));
    assert_eq!(low.as_ref().unwrap().key.0.len(), 2);
    assert_eq!(high.as_ref().unwrap().key.0.len(), 2);
    assert!(low.as_ref().unwrap().key.contains_param());
    assert!(high.as_ref().unwrap().key.contains_param());
}

#[test]
fn tuple_index_functional_key_and_empty_arity_validation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,a VARCHAR(20),b INT,KEY ab ((LOWER(a)),b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'ABC',1),(2,'abc',2),(3,'DEF',1),(4,NULL,1)")
        .unwrap();
    let sql =
        "SELECT id FROM t FORCE INDEX(ab) WHERE (LOWER(a),b) IN (('abc',1),('def',1)) ORDER BY id";
    assert!(explain(&engine, sql).contains("Index multi lookup"));
    assert_eq!(
        rows(&engine, sql),
        vec![vec![Value::Int(1)], vec![Value::Int(3)]]
    );
    differential(
        &engine,
        "t",
        "PRIMARY,ab",
        "(LOWER(a),b) BETWEEN ('abc',2) AND ('def',1)",
    );
    engine
        .execute("CREATE TABLE empty_rows (a INT,b INT,PRIMARY KEY(a,b))")
        .unwrap();
    for pred in [
        "(a,b) IN ((1,2),(1,2,3))",
        "(a,b) BETWEEN (1,2,3) AND (3,4)",
        "(a,b)=(1,2,3)",
    ] {
        assert_eq!(
            engine
                .execute(&format!("SELECT * FROM empty_rows WHERE {pred}"))
                .unwrap_err()
                .code(),
            1241,
            "{pred}"
        );
    }
}

#[test]
fn boolean_range_ordered_pagination_uses_both_composite_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a INT,b INT,PRIMARY KEY(a,b))")
        .unwrap();
    let values = (0..1200)
        .map(|i| format!("({},{})", i / 4, i % 4))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    let sql = "SELECT * FROM (SELECT a,b FROM t WHERE (a>270 OR (a=270 AND b>1)) AND (a<299 OR (a=299 AND b<2) OR (a=299 AND b=2)) ORDER BY a,b LIMIT 100) x ORDER BY a DESC,b DESC LIMIT 1";
    assert_eq!(
        rows(&engine, sql),
        vec![vec![Value::Int(295), Value::Int(1)]]
    );
    let analyzed = rows(&engine, &format!("EXPLAIN ANALYZE {sql}"))
        .into_iter()
        .flatten()
        .map(|v| crate::storage::value_to_string(&v))
        .collect::<Vec<_>>()
        .join("\n");
    let examined = analyzed
        .split("index_entries_examined=")
        .nth(1)
        .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert!(examined < 200, "{analyzed}");
    let plan = explain(&engine, sql);
    assert!(
        plan.contains("(a,b) > (const,const)") && plan.contains("(a,b) <= (const,const)"),
        "{plan}"
    );
}

fn expanded_bound(columns: &[&str], values: &[&str], op: &str, reversed: bool) -> String {
    let strict = if op.starts_with('>') { ">" } else { "<" };
    let cmp = |column: &str, op: &str, value: &str| {
        if reversed {
            let flip = match op {
                ">" => "<",
                ">=" => "<=",
                "<" => ">",
                "<=" => ">=",
                _ => op,
            };
            format!("{value}{flip}{column}")
        } else {
            format!("{column}{op}{value}")
        }
    };
    let mut branches = (0..columns.len())
        .map(|i| {
            let mut terms = (0..i)
                .map(|j| cmp(columns[j], "=", values[j]))
                .collect::<Vec<_>>();
            terms.push(cmp(columns[i], strict, values[i]));
            if reversed {
                terms.reverse();
            }
            format!("({})", terms.join(" AND "))
        })
        .collect::<Vec<_>>();
    if op.ends_with('=') {
        branches.push(format!(
            "({})",
            columns
                .iter()
                .zip(values)
                .map(|(c, v)| cmp(c, "=", v))
                .collect::<Vec<_>>()
                .join(" AND ")
        ));
    }
    if reversed {
        branches.reverse();
    }
    format!("({})", branches.join(" OR "))
}

#[test]
fn boolean_range_multicolumn_differential_and_holes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT PRIMARY KEY,tenant INT,a INT,b INT,c INT,payload VARCHAR(20),KEY abc(a,b,c),KEY tabc(tenant,a,b,c))").unwrap();
    let mut values = Vec::new();
    for a in ["NULL", "-1", "0", "1", "2"] {
        for b in ["NULL", "-1", "0", "1", "2"] {
            for c in ["NULL", "0", "1"] {
                values.push(format!("({},1,{a},{b},{c},'payload')", values.len()));
            }
        }
    }
    engine
        .execute(&format!("INSERT INTO t VALUES {}", values.join(",")))
        .unwrap();
    for columns in [vec!["a", "b"], vec!["a", "b", "c"]] {
        for op in [">", ">=", "<", "<="] {
            for reversed in [false, true] {
                for bound in [
                    vec!["0", "1", "0"],
                    vec!["1", "NULL", "0"],
                    vec!["NULL", "1", "0"],
                    vec!["9", "9", "9"],
                ] {
                    let pred = expanded_bound(&columns, &bound[..columns.len()], op, reversed);
                    differential(&engine, "t", "PRIMARY,abc,tabc", &pred);
                    differential(
                        &engine,
                        "t",
                        "PRIMARY,abc,tabc",
                        &format!("tenant=1 AND {pred}"),
                    );
                }
            }
        }
    }
    for pred in [
        "(a>0 OR (a=0 AND b>1)) AND (a<1 OR (a=1 AND b<=1))",
        "(a>1 OR (a=1 AND b>1)) AND (a<0 OR (a=0 AND b<=1))",
        "a>0 OR (a=-1 AND b>1)",
        "(a=0 AND (b>0 OR (b=0 AND c>0))) OR a>0",
        "a>1 OR b=0",
        "a<0 OR (a=1 AND b=1)",
        "a>0 OR (a=0 AND c>1)",
        "a>0 OR (a=0 AND b>1.5)",
        "a>0 OR (a=0 AND b>0) OR (a=0 AND b=0 AND c=0)",
    ] {
        differential(&engine, "t", "PRIMARY,abc,tabc", pred);
    }
    let plan=explain(&engine,"SELECT a,b,c FROM t FORCE INDEX(tabc) WHERE tenant=1 AND (a>0 OR (a=0 AND b>1) OR (a=0 AND b=1 AND c>=0)) ORDER BY a,b,c LIMIT 10");
    assert!(
        plan.contains("(tenant,a,b,c) >= (const,const,const,const)"),
        "{plan}"
    );
}

#[test]
fn boolean_range_types_in_every_key_position() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let cases = [
        (
            "BIGINT",
            "-9223372036854775808",
            "1",
            "9223372036854775807",
            true,
        ),
        (
            "BIGINT UNSIGNED",
            "0",
            "9223372036854775808",
            "18446744073709551615",
            true,
        ),
        (
            "DECIMAL(20,4)",
            "-123456789012.1234",
            "0.1250",
            "123456789012.1234",
            true,
        ),
        (
            "DECIMAL(65,10)",
            "-1234567890123456789012345678901234567890.1234567890",
            "0.125",
            "1234567890123456789012345678901234567890.1234567890",
            true,
        ),
        ("FLOAT", "-1.5", "1.25", "1.5", true),
        ("DOUBLE", "-1.5", "1.25", "1.5", true),
        ("VARCHAR(20) COLLATE utf8mb4_bin", "'A'", "'a'", "'z'", true),
        (
            "VARCHAR(20) COLLATE utf8mb4_0900_ai_ci",
            "'a'",
            "'B'",
            "'é'",
            true,
        ),
        ("CHAR(10)", "'a'", "'b'", "'z'", true),
        ("VARBINARY(10)", "X'00'", "X'41'", "X'FF'", true),
        ("DATE", "'2020-01-01'", "'2026-09-10'", "'2030-12-31'", true),
        (
            "DATETIME(6)",
            "'2020-01-01 00:00:00.000001'",
            "'2026-09-10 12:55:19.123456'",
            "'2030-12-31 23:59:59.999999'",
            true,
        ),
        (
            "TIMESTAMP",
            "'2020-01-01 00:00:00'",
            "'2026-09-10 12:55:19'",
            "'2030-12-31 23:59:59'",
            true,
        ),
        (
            "TIME(6)",
            "'-120:00:00.123456'",
            "'00:00:01'",
            "'120:00:00.123456'",
            true,
        ),
        ("YEAR", "2000", "2026", "2030", true),
        ("BIT(8)", "0", "127", "255", true),
        ("ENUM('z','a','b')", "'z'", "'a'", "'b'", false),
        ("SET('a','b','c')", "'a'", "'b'", "'a,c'", false),
        ("FLOAT", "-1.5", "0", "1.5", false),
        ("DOUBLE", "-1.5", "-0.0", "1.5", false),
        ("DECIMAL(5,2)", "1.23", "1.234", "1.24", false),
        ("VARCHAR(10)", "'01'", "1", "'20'", false),
    ];
    for (i, (ty, lo, mid, hi, full)) in cases.iter().enumerate() {
        let t = format!("br_type_{i}");
        engine
            .execute(&format!(
                "CREATE TABLE {t} (id INT PRIMARY KEY,a {ty},b {ty},c INT,KEY abc(a,b,c))"
            ))
            .unwrap();
        engine.execute(&format!("INSERT INTO {t} VALUES (1,{lo},{lo},0),(2,{mid},{lo},0),(3,{mid},{mid},0),(4,{mid},{mid},1),(5,{mid},{hi},0),(6,{hi},{lo},0),(7,NULL,{mid},0),(8,{mid},NULL,0)")).unwrap();
        for op in [">", ">=", "<", "<="] {
            let pred = expanded_bound(&["a", "b", "c"], &[mid, mid, "0"], op, false);
            differential(&engine, &t, "PRIMARY,abc", &pred);
            if *full {
                let plan=explain(&engine,&format!("SELECT a,b,c FROM {t} FORCE INDEX(abc) WHERE {pred} ORDER BY a,b,c LIMIT 10"));
                assert!(
                    plan.contains(&format!("(a,b,c) {op} (const,const,const)")),
                    "type {ty}, {pred}: {plan}"
                );
            }
        }
    }
}

#[test]
fn boolean_range_prepared_rebinds_branch_relations() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,a INT,b DECIMAL(5,2),KEY ab(a,b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,0,0),(2,1,1.23),(3,1,1.24),(4,2,0),(5,3,0),(6,1,NULL)")
        .unwrap();
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session,"PREPARE br FROM 'SELECT id FROM t FORCE INDEX(ab) WHERE (a>? OR (a=? AND b>?)) AND (a<? OR (a=? AND b<=?)) ORDER BY id'").unwrap();
    for (a, eq, b, hi, heq, hb) in [
        ("1", "1", "1.23", "3", "3", "0"),
        ("2", "0", "0", "3", "3", "0"),
        ("0", "2", "0", "2", "0", "0"),
        ("3", "3", "0", "1", "1", "0"),
        ("1", "1", "1.234", "2", "2", "0"),
        ("NULL", "1", "1.23", "3", "3", "0"),
        ("1", "NULL", "0", "3", "3", "0"),
        ("-1", "-1", "-1", "9", "9", "9"),
    ] {
        engine
            .execute_with_session(
                &mut session,
                &format!("SET @a={a},@eq={eq},@b={b},@hi={hi},@heq={heq},@hb={hb}"),
            )
            .unwrap();
        let QueryResponse::ResultSet(actual) = engine
            .execute_with_session(&mut session, "EXECUTE br USING @a,@eq,@b,@hi,@heq,@hb")
            .unwrap()
        else {
            panic!()
        };
        let expected=rows(&engine,&format!("SELECT id FROM t IGNORE INDEX(PRIMARY,ab) WHERE (a>{a} OR (a={eq} AND b>{b})) AND (a<{hi} OR (a={heq} AND b<={hb})) ORDER BY id"));
        assert_eq!(actual.rows, expected, "{a},{eq},{b},{hi},{heq},{hb}");
    }
}

#[test]
fn boolean_range_eight_key_primary_and_limit_edges() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let columns = ["a", "b", "c", "d", "e", "f", "g", "h"];
    engine.execute("CREATE TABLE t (a INT,b INT,c INT,d INT,e INT,f INT,g INT,h INT,PRIMARY KEY(a,b,c,d,e,f,g,h))").unwrap();
    engine.execute("INSERT INTO t VALUES (1,1,1,1,1,1,1,0),(1,1,1,1,1,1,1,1),(1,1,1,1,1,1,1,2),(1,1,1,1,1,1,2,0),(2,0,0,0,0,0,0,0)").unwrap();
    let low = expanded_bound(&columns, &["1"; 8], ">", true);
    let high = expanded_bound(
        &columns,
        &["2", "0", "0", "0", "0", "0", "0", "0"],
        "<=",
        false,
    );
    for limit in [0, 1, 2, 3, 4, 20] {
        let sql=format!("SELECT * FROM (SELECT * FROM t WHERE {low} AND {high} ORDER BY a,b,c,d,e,f,g,h LIMIT {limit}) x ORDER BY a DESC,b DESC,c DESC,d DESC,e DESC,f DESC,g DESC,h DESC LIMIT 1");
        let scanned = sql.replace("FROM t WHERE", "FROM t IGNORE INDEX(PRIMARY) WHERE");
        assert_eq!(rows(&engine, &sql), rows(&engine, &scanned), "{sql}");
    }
    let plan=explain(&engine,&format!("SELECT * FROM t FORCE INDEX(PRIMARY) WHERE {low} AND {high} ORDER BY a,b,c,d,e,f,g,h LIMIT 20"));
    assert!(
        plan.contains("(a,b,c,d,e,f,g,h) > (const,const,const,const,const,const,const,const)"),
        "{plan}"
    );
    for suffix in ["DESC", "ASC"] {
        let sql=format!("SELECT * FROM t FORCE INDEX(PRIMARY) WHERE {low} AND {high} ORDER BY a {suffix},b {suffix},c {suffix},d {suffix},e {suffix},f {suffix},g {suffix},h {suffix} LIMIT 2");
        assert_eq!(
            rows(&engine, &sql),
            rows(
                &engine,
                &sql.replace("FORCE INDEX(PRIMARY)", "IGNORE INDEX(PRIMARY)")
            )
        );
    }
    engine
        .execute("CREATE TABLE empty_t (a INT,b INT,PRIMARY KEY(a,b))")
        .unwrap();
    assert!(rows(
        &engine,
        "SELECT * FROM empty_t WHERE a>1 OR (a=1 AND b>1) ORDER BY a,b LIMIT 20"
    )
    .is_empty());
}

#[test]
fn boolean_range_mixed_collations_prefix_function_and_partition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT PRIMARY KEY,a VARCHAR(12) COLLATE utf8mb4_bin,b VARCHAR(12) COLLATE utf8mb4_0900_ai_ci,c DATETIME(6),KEY abc(a,b,c),KEY pre(a(1),b),KEY fn ((LOWER(a)),b))").unwrap();
    engine.execute("INSERT INTO t VALUES (1,'A','é','2026-01-01'),(2,'a','e','2026-01-01'),(3,'a','É','2026-01-01 00:00:00.000001'),(4,'aa','e','2026-01-01'),(5,'a ','e ','2026-01-01'),(6,NULL,'e','2026-01-01')").unwrap();
    for op in [">", ">=", "<", "<="] {
        let pred = expanded_bound(&["a", "b", "c"], &["'a'", "'é'", "'2026-01-01'"], op, false);
        differential(&engine, "t", "PRIMARY,abc,pre,fn", &pred);
        let sql = format!("SELECT id FROM t FORCE INDEX(pre) WHERE {pred} ORDER BY id");
        assert_eq!(
            rows(&engine, &sql),
            rows(
                &engine,
                &sql.replace("FORCE INDEX(pre)", "IGNORE INDEX(PRIMARY,abc,pre,fn)")
            )
        );
        differential(
            &engine,
            "t",
            "PRIMARY,abc,pre,fn",
            &expanded_bound(&["LOWER(a)", "b"], &["'a'", "'e'"], op, false),
        );
    }
    engine.execute("CREATE TABLE p (id INT,a INT,b INT,PRIMARY KEY(id,a,b),KEY ab(a,b)) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(3),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine
        .execute("INSERT INTO p VALUES (1,1,1),(2,1,2),(3,2,0),(4,3,0)")
        .unwrap();
    differential(&engine, "p", "PRIMARY,ab", "a>1 OR (a=1 AND b>1)");
}

#[test]
fn boolean_range_runtime_prefix_rebinds_without_cached_value_proof() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (tenant INT,a INT,b DECIMAL(8,2),PRIMARY KEY(tenant,a,b))")
        .unwrap();
    let schema = engine.db_for_tests().table("t").unwrap();
    let index = &schema.indexes[0];
    let mut statement = parse_statement(
        "SELECT * FROM t WHERE tenant=? AND (a>? OR (a=? AND b>?)) AND (a<? OR (a=? AND b<=?))",
    )
    .unwrap();
    crate::parser::number_statement_placeholders(&mut statement, &mut 0);
    let Statement::Select(select) = statement else {
        panic!()
    };
    for tenant in [1, 2] {
        let params = vec![
            Value::Int(tenant),
            Value::Int(10),
            Value::Int(10),
            Value::Decimal("1.25".into()),
            Value::Int(20),
            Value::Int(20),
            Value::Decimal("2.5".into()),
        ];
        crate::engine::params::with_params(params, || {
            let mut low = Some(IndexBound {
                key: IndexKey::from(vec![crate::storage::KeyPart::Param {
                    idx: 0,
                    target: crate::storage::ParamTarget::Int,
                }]),
                inclusive: true,
            });
            let mut high = low.clone();
            crate::planner::boolean_range::refine_runtime(
                &schema,
                index,
                select.selection.as_ref(),
                &mut low,
                &mut high,
            );
            for bound in [&low, &high] {
                let bound = bound.as_ref().unwrap();
                assert_eq!(bound.key.0.len(), 3, "{bound:?}");
                assert_eq!(bound.key.0[0], crate::storage::KeyPart::Int(tenant));
                assert!(!bound.key.contains_param());
            }
            assert!(!low.unwrap().inclusive);
            assert!(high.unwrap().inclusive);
        });
    }
}

#[test]
fn boolean_range_vector_row_and_mvcc_agree() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,a INT,b INT,payload VARCHAR(30),KEY ab(a,b))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,1,1,'one'),(2,1,2,'two'),(3,2,0,'three'),(4,2,1,'four')")
        .unwrap();
    let mut reader = SessionContext::default();
    let mut writer = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    let sql="SELECT id,a,b,payload FROM t FORCE INDEX(ab) WHERE (a>1 OR (a=1 AND b>1)) AND (a<2 OR (a=2 AND b<=1)) ORDER BY a DESC,b DESC,id LIMIT 3";
    let read = |session: &mut SessionContext, query: &str, mode| {
        with_vec_mode(mode, || {
            let QueryResponse::ResultSet(r) = engine.execute_with_session(session, query).unwrap()
            else {
                panic!()
            };
            r.rows
        })
    };
    let original = read(&mut reader, sql, VecMode::On);
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET a=9 WHERE id=2")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM t WHERE id=3")
        .unwrap();
    for session in [&mut reader, &mut writer] {
        let oracle = read(
            session,
            &sql.replace("FORCE INDEX(ab)", "IGNORE INDEX(PRIMARY,ab)"),
            VecMode::Off,
        );
        for mode in [VecMode::On, VecMode::Off] {
            assert_eq!(read(session, sql, mode), oracle);
        }
    }
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    assert_eq!(read(&mut reader, sql, VecMode::On), original);
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    assert_eq!(
        read(&mut reader, sql, VecMode::On),
        vec![vec![
            Value::Int(4),
            Value::Int(2),
            Value::Int(1),
            Value::String("four".into())
        ]]
    );
}

#[test]
fn boolean_range_sixteen_key_endpoints_fit_budget() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let columns = (0..16).map(|i| format!("k{i}")).collect::<Vec<_>>();
    let names = columns.join(",");
    let ddl = columns
        .iter()
        .map(|c| format!("{c} INT"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("CREATE TABLE t ({ddl},PRIMARY KEY({names}))"))
        .unwrap();
    engine
        .execute(&format!(
            "INSERT INTO t VALUES ({}),({})",
            vec!["1"; 16].join(","),
            vec!["2"; 16].join(",")
        ))
        .unwrap();
    let refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let low = expanded_bound(&refs, &["1"; 16], ">", false);
    let high = expanded_bound(&refs, &["2"; 16], "<=", false);
    let plan = explain(
        &engine,
        &format!(
            "SELECT * FROM t FORCE INDEX(PRIMARY) WHERE {low} AND {high} ORDER BY {names} LIMIT 20"
        ),
    );
    let consts = vec!["const"; 16].join(",");
    assert!(
        plan.contains(&format!("({names}) > ({consts})"))
            && plan.contains(&format!("({names}) <= ({consts})")),
        "{plan}"
    );
}

#[test]
fn boolean_range_prepared_ordered_scan_refines_before_cursor_open() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a INT,b INT,PRIMARY KEY(a,b))")
        .unwrap();
    let values = (0..1000)
        .map(|i| format!("({},{})", i / 4, i % 4))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    let mut session = SessionContext::default();
    let query = "SELECT a,b FROM t WHERE a>? OR (a=? AND b>?) ORDER BY a,b LIMIT 10";
    engine
        .execute_with_session(&mut session, &format!("PREPARE br_rows FROM '{query}'"))
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            &format!("PREPARE br_stats FROM 'EXPLAIN ANALYZE {query}'"),
        )
        .unwrap();
    for start in [100, 200, 150] {
        engine
            .execute_with_session(&mut session, &format!("SET @lo={start},@same={start},@b=1"))
            .unwrap();
        let QueryResponse::ResultSet(actual) = engine
            .execute_with_session(&mut session, "EXECUTE br_rows USING @lo,@same,@b")
            .unwrap()
        else {
            panic!()
        };
        let expected = (start * 4 + 2..start * 4 + 12)
            .map(|i| vec![Value::Int(i / 4), Value::Int(i % 4)])
            .collect::<Vec<_>>();
        assert_eq!(actual.rows, expected);
        let QueryResponse::ResultSet(stats) = engine
            .execute_with_session(&mut session, "EXECUTE br_stats USING @lo,@same,@b")
            .unwrap()
        else {
            panic!()
        };
        let plan = stats.rows[0][0].to_display_string();
        let examined = plan
            .split("index_entries_examined=")
            .nth(1)
            .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
            .unwrap_or_else(|| panic!("{plan}"))
            .parse::<usize>()
            .unwrap();
        assert!(examined <= 20, "{plan}");
    }
}
