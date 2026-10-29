use super::*;

fn fixture(sql: &[&str]) -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for statement in sql {
        engine.execute(statement).unwrap();
    }
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) = engine
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
        .materialize()
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
    else {
        panic!("expected result set: {sql}");
    };
    result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| value.to_display_string())
                .collect()
        })
        .collect()
}

#[test]
fn audit1027_integer_probe_decimal_unique_index() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE a(id INT PRIMARY KEY,x INT)",
        "CREATE TABLE b(id DECIMAL(10,2) PRIMARY KEY,note INT)",
        "INSERT INTO a VALUES(1,5),(2,6),(3,7),(4,8),(5,9)",
        "INSERT INTO b VALUES(5.00,7),(6.00,8),(7.00,9),(8.00,10),(9.00,11)",
    ]);
    assert_eq!(
        rows(
            &engine,
            "SELECT a.id,b.id FROM a JOIN b ON a.x=b.id ORDER BY a.id"
        ),
        vec![
            vec!["1", "5.00"],
            vec!["2", "6.00"],
            vec!["3", "7.00"],
            vec!["4", "8.00"],
            vec!["5", "9.00"]
        ]
    );
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM a JOIN b ON a.x=b.id"),
        vec![vec!["5"]]
    );
}

#[test]
fn audit1027_mixed_range_bounds_rejected() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(d DATE,v INT)",
        "INSERT INTO t VALUES('2021-01-01',1),('2021-01-03',2),('2021-01-10',3)",
    ]);
    for bounds in [
        "5 PRECEDING AND INTERVAL 1 DAY FOLLOWING",
        "INTERVAL 1 DAY PRECEDING AND 5 FOLLOWING",
    ] {
        let sql = format!("SELECT SUM(v) OVER(ORDER BY d RANGE BETWEEN {bounds}) FROM t");
        let err = engine
            .execute(&sql)
            .and_then(QueryResponse::materialize)
            .unwrap_err();
        assert_eq!(err.code(), 3588, "{sql}: {err:?}");
    }
}

#[test]
fn audit1027_distinct_topk_column_collation() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(s VARCHAR(10) COLLATE utf8mb4_bin,id INT)",
        "INSERT INTO t VALUES('a',1),('B',2),('b',3)",
    ]);
    let expected = vec![vec!["B"], vec!["a"], vec!["b"]];
    assert_eq!(
        rows(&engine, "SELECT DISTINCT s FROM t ORDER BY s"),
        expected
    );
    for limit in [3, 10] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT DISTINCT s FROM t ORDER BY s LIMIT {limit}")
            ),
            expected
        );
    }
}

#[test]
fn audit1027_distinct_string_function_collations() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(s VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO t VALUES('a'),('B'),('b')",
    ]);
    for expr in [
        "REVERSE(s)",
        "TRIM(s)",
        "SUBSTRING(s,1)",
        "LEFT(s,1)",
        "REPLACE(s,'x','y')",
    ] {
        let sql = format!("SELECT DISTINCT {expr} AS label FROM t ORDER BY label");
        for suffix in ["", " LIMIT 3", " LIMIT 10"] {
            assert_eq!(
                rows(&engine, &format!("{sql}{suffix}")),
                vec![vec!["B"], vec!["a"], vec!["b"]],
                "{sql}{suffix}"
            );
        }
    }
}

#[test]
fn audit1027_distinct_topk_preserves_aggregate_projection() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(k INT,v INT)",
        "INSERT INTO t VALUES(1,1),(2,2),(3,2),(4,3),(4,4)",
    ]);
    for (projection, order) in [
        ("SUM(v)", "SUM(v)"),
        ("SUM(v)+1 AS n", "n"),
        ("COUNT(*)", "COUNT(*)"),
    ] {
        let sql = format!("SELECT DISTINCT {projection} FROM t GROUP BY k ORDER BY {order}");
        let full = rows(&engine, &sql);
        for (limit, offset) in [(10, 0), (2, 0), (2, 1)] {
            assert_eq!(
                rows(&engine, &format!("{sql} LIMIT {limit} OFFSET {offset}")),
                full.iter()
                    .skip(offset)
                    .take(limit)
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn audit1027_parallel_decimal_sum_widens_from_sql() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(k BIGINT,d DECIMAL(38,0))",
        "SET SESSION mydb_parallel_degree=4",
    ]);
    let magnitude = 10i128.pow(38) - 1;
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..65_536).map(|index| {
            vec![
                Value::Int(1),
                Value::Decimal128(
                    if index == 0 || index == 32_768 {
                        magnitude
                    } else {
                        0
                    },
                    0,
                ),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE t").unwrap();
    let expected = "199999999999999999999999999999999999998";
    assert_eq!(rows(&engine, "SELECT SUM(d) FROM t"), vec![vec![expected]]);
    for suffix in ["", " ORDER BY k"] {
        let sql = format!("SELECT /*+ PARALLEL(4) */ k,SUM(d) FROM t GROUP BY k{suffix}");
        let plan = rows(&engine, &format!("EXPLAIN {sql}"));
        assert!(
            plan.iter()
                .flatten()
                .any(|value| value.contains("Parallel aggregate")),
            "{plan:?}"
        );
        assert_eq!(rows(&engine, &sql), vec![vec!["1", expected]]);
    }
}

#[test]
fn audit1027_runtime_filter_index_domain_matrix() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    for data_type in [
        "DECIMAL(10,2)",
        "DECIMAL(38,6)",
        "DOUBLE",
        "FLOAT",
        "BIGINT",
        "BIGINT UNSIGNED",
    ] {
        for index in [
            "PRIMARY KEY(id)",
            "UNIQUE KEY idx(id)",
            "KEY idx(id)",
            "PRIMARY KEY(note,id)",
        ] {
            let (_dir, engine) = fixture(&[
                "CREATE TABLE a(id INT PRIMARY KEY,x INT)",
                &format!("CREATE TABLE b(id {data_type},note INT,{index})"),
                "INSERT INTO a VALUES(1,5),(2,6),(3,99),(4,NULL),(5,5)",
            ]);
            engine
                .execute("INSERT INTO b VALUES(5,10),(6,20),(7,30)")
                .unwrap();
            if data_type.starts_with("DECIMAL") || data_type == "DOUBLE" || data_type == "FLOAT" {
                engine.execute("INSERT INTO b VALUES(5.25,50)").unwrap();
            }
            for mode in [VecMode::Off, VecMode::On, VecMode::Auto] {
                with_vec_mode(mode, || {
                    for projection in ["a.id,b.id", "a.id,b.note"] {
                        let sql =
                            format!("SELECT {projection} FROM a JOIN b ON a.x=b.id ORDER BY a.id");
                        let result = rows(&engine, &sql);
                        assert_eq!(
                            result.iter().map(|row| row[0].as_str()).collect::<Vec<_>>(),
                            vec!["1", "2", "5"],
                            "{data_type}, {index}, {mode:?}: {sql}"
                        );
                    }
                    assert_eq!(
                        rows(&engine, "SELECT COUNT(*) FROM a JOIN b ON a.x=b.id"),
                        vec![vec!["3"]],
                        "{data_type}, {index}, {mode:?}"
                    );
                });
            }
        }
    }
}

#[test]
fn audit1027_range_validation_and_valid_domains() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(d DATE,v INT)",
        "INSERT INTO t VALUES('2021-01-01',1),('2021-01-03',2),('2021-01-10',3)",
    ]);
    for direction in ["ASC", "DESC"] {
        for bounds in [
            "5 PRECEDING AND INTERVAL 1 DAY FOLLOWING",
            "INTERVAL 1 DAY PRECEDING AND 5 FOLLOWING",
            "0 PRECEDING AND INTERVAL 1 MONTH FOLLOWING",
        ] {
            for tail in ["", " WHERE v<0", " LIMIT 0"] {
                let sql = format!("SELECT SUM(v) OVER(ORDER BY d {direction} RANGE BETWEEN {bounds}) FROM t{tail}");
                let err = engine
                    .execute(&sql)
                    .and_then(QueryResponse::materialize)
                    .unwrap_err();
                assert_eq!(err.code(), 3588, "{sql}: {err:?}");
            }
        }
    }
    for (order, bounds, expected) in [
        (
            "d",
            "INTERVAL 5 DAY PRECEDING AND INTERVAL 1 DAY FOLLOWING",
            vec![vec!["1"], vec!["3"], vec!["3"]],
        ),
        (
            "d",
            "INTERVAL 5 DAY PRECEDING AND CURRENT ROW",
            vec![vec!["1"], vec!["3"], vec!["3"]],
        ),
        (
            "d",
            "UNBOUNDED PRECEDING AND INTERVAL 1 DAY FOLLOWING",
            vec![vec!["1"], vec!["3"], vec!["6"]],
        ),
        (
            "v",
            "1 PRECEDING AND 1 FOLLOWING",
            vec![vec!["3"], vec!["6"], vec!["5"]],
        ),
    ] {
        assert_eq!(
            rows(
                &engine,
                &format!(
                    "SELECT SUM(v) OVER(ORDER BY {order} RANGE BETWEEN {bounds}) FROM t ORDER BY d"
                )
            ),
            expected
        );
    }
}

#[test]
fn audit1027_distinct_topk_collation_matrix() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    for collation in [
        "utf8mb4_bin",
        "utf8mb4_0900_as_cs",
        "utf8mb4_general_ci",
        "utf8mb4_0900_ai_ci",
    ] {
        for index in ["", ",KEY idx(s)", ",KEY idx(s,id)", ",KEY idx(s(1))"] {
            let (_dir, engine) = fixture(&[
                &format!("CREATE TABLE t(s VARCHAR(10) COLLATE {collation},id INT{index})"),
                "INSERT INTO t VALUES('a',1),('B',2),('b',3),('B',4),(NULL,5),('a ',6),('é',7),('e',8),('b2',9)",
            ]);
            let source = if index.is_empty() {
                "t"
            } else {
                "t FORCE INDEX(idx)"
            };
            for mode in [VecMode::Off, VecMode::On] {
                with_vec_mode(mode, || {
                    for (projection, order, effective_collation) in [
                        ("s", "s", collation),
                        ("s AS label", "label DESC", collation),
                        ("CONCAT(s) AS label", "label", collation),
                        ("REVERSE(s) AS label", "label", collation),
                        ("TRIM(s) AS label", "label", collation),
                        ("SUBSTRING(s,1) AS label", "label", collation),
                        ("LEFT(s,10) AS label", "label", collation),
                        ("REPLACE(s,'x','y') AS label", "label", collation),
                        ("s COLLATE utf8mb4_bin AS label", "label", "utf8mb4_bin"),
                        ("s,id", "s,id", collation),
                        ("t.*", "s,id", collation),
                    ] {
                        let normalize = |values: Vec<Vec<String>>| {
                            values
                                .into_iter()
                                .map(|mut row| {
                                    if row[0] != "NULL" {
                                        row[0] = format!(
                                            "{:?}",
                                            crate::charset::collation_key(
                                                effective_collation,
                                                &row[0]
                                            )
                                        );
                                    }
                                    row
                                })
                                .collect::<Vec<_>>()
                        };
                        let sql =
                            format!("SELECT DISTINCT {projection} FROM {source} ORDER BY {order}");
                        let full = normalize(rows(&engine, &sql));
                        for (limit, offset) in [(1, 0), (3, 0), (10, 0), (3, 1), (10, 2), (0, 0)] {
                            let bounded = format!("{sql} LIMIT {limit} OFFSET {offset}");
                            assert_eq!(
                                normalize(rows(&engine, &bounded)),
                                full.iter()
                                    .skip(offset)
                                    .take(limit)
                                    .cloned()
                                    .collect::<Vec<_>>(),
                                "{collation}, {index}, {mode:?}: {bounded}"
                            );
                        }
                    }
                });
            }
        }
    }
}
