use super::*;

fn fixture(data_type: &str) -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::Builder::new()
        .prefix("temporal-comparison-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            &format!("CREATE TABLE dates (id INT PRIMARY KEY, d {data_type}, KEY kd(d))"),
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO dates VALUES (1,'2024-02-28'),(2,'2024-02-29'),\
             (3,'2024-03-01'),(4,'2024-03-02'),(5,NULL)",
        )
        .unwrap();
    (dir, engine, session)
}

const SOURCES: &[&str] = &[
    "dates FORCE INDEX(kd)",
    "dates IGNORE INDEX(kd)",
    "(SELECT id,d FROM dates LIMIT 18446744073709551615) AS materialized",
];

#[test]
fn temporal_comparison_invalid_literals_are_plan_independent() {
    for auto in ["off", "select"] {
        with_test_env(&[("MYDB_AUTO_PARAMETERIZE", Some(auto))], || {
            for data_type in ["DATE", "DATETIME(6)", "TIMESTAMP(6)"] {
                let (_dir, engine, mut session) = fixture(data_type);
                for empty in [false, true] {
                    if empty {
                        engine
                            .execute_with_session(&mut session, "DELETE FROM dates")
                            .unwrap();
                    }
                    for mode in ["STRICT_TRANS_TABLES", ""] {
                        engine
                            .execute_with_session(&mut session, &format!("SET sql_mode='{mode}'"))
                            .unwrap();
                        for predicate in [
                            "d = '2024-02-30'",
                            "d >= '2024-02-30'",
                            "d IN ('2024-03-01','2024-02-30')",
                            "d NOT IN ('2024-02-30')",
                            "d BETWEEN '2024-02-30' AND '2024-03-01'",
                            "d BETWEEN '2024-02-28' AND '2024-02-30'",
                            "d NOT BETWEEN '2024-02-30' AND '2024-03-01'",
                            "1=0 AND d BETWEEN '2024-02-30' AND '2024-03-01'",
                            "1=0 AND d IN ('2024-02-30')",
                            "d IS NULL AND d IS NOT NULL AND d BETWEEN '2024-02-30' AND '2024-03-01'",
                        ] {
                            for source in SOURCES {
                                for prefix in ["", "EXPLAIN "] {
                                    let sql = format!(
                                        "{prefix}SELECT id FROM {source} WHERE {predicate}"
                                    );
                                    // Reuse the literal-parameterized shape before
                                    // checking the invalid value on the cache hit.
                                    engine
                                        .execute_with_session(
                                            &mut session,
                                            &sql.replace("2024-02-30", "2024-02-29"),
                                        )
                                        .unwrap();
                                    let err = engine.execute_with_session(&mut session, &sql)
                                        .expect_err(&format!("{data_type}; auto={auto}; empty={empty}; mode={mode}; {sql}"));
                                    assert_eq!(
                                        (err.code(), err.sql_state().as_ref()),
                                        (1525, "HY000"),
                                        "{sql}: {err}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}

#[test]
fn temporal_comparison_parameters_share_nullable_semantics() {
    for vectorized in ["off", "on"] {
        with_test_env(&[("MYDB_VECTORIZED", Some(vectorized))], || {
            for data_type in ["DATE", "DATETIME(6)", "TIMESTAMP(6)"] {
                let (_dir, engine, mut session) = fixture(data_type);
                engine
                    .execute_with_session(&mut session, "SET @bad='2024-02-30',@hi='2024-03-01'")
                    .unwrap();
                for (predicate, args, expected) in [
                    ("d = ?", "@bad", vec![Value::Null; 5]),
                    ("d >= ?", "@bad", vec![Value::Null; 5]),
                    ("d <> ?", "@bad", vec![Value::Null; 5]),
                    ("d <=> ?", "@bad", vec![Value::Int(0); 5]),
                    ("d IN (?)", "@bad", vec![Value::Null; 5]),
                    ("d NOT IN (?)", "@bad", vec![Value::Null; 5]),
                    (
                        "d IN (?,?)",
                        "@bad,@hi",
                        vec![
                            Value::Null,
                            Value::Null,
                            Value::Int(1),
                            Value::Null,
                            Value::Null,
                        ],
                    ),
                    (
                        "d BETWEEN ? AND ?",
                        "@bad,@hi",
                        vec![
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Int(0),
                            Value::Null,
                        ],
                    ),
                    (
                        "d NOT BETWEEN ? AND ?",
                        "@bad,@hi",
                        vec![
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Int(1),
                            Value::Null,
                        ],
                    ),
                    (
                        "d BETWEEN ? AND ?",
                        "@hi,@bad",
                        vec![
                            Value::Int(0),
                            Value::Int(0),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                        ],
                    ),
                ] {
                    for source in SOURCES {
                        for projection in [true, false] {
                            let sql = if projection {
                                format!("SELECT {predicate} AS result FROM {source} ORDER BY id")
                            } else {
                                format!("SELECT id FROM {source} WHERE {predicate} ORDER BY id")
                            };
                            engine
                                .execute_with_session(
                                    &mut session,
                                    &format!("PREPARE cmp FROM '{sql}'"),
                                )
                                .unwrap();
                            let response = engine
                                .execute_with_session(
                                    &mut session,
                                    &format!("EXECUTE cmp USING {args}"),
                                )
                                .unwrap();
                            let rows = support::result_rows(response);
                            let wanted: Vec<Vec<Value>> = if projection {
                                expected.iter().cloned().map(|value| vec![value]).collect()
                            } else {
                                expected
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, value)| matches!(value, Value::Int(1)))
                                    .map(|(i, _)| vec![Value::Int(i as i64 + 1)])
                                    .collect()
                            };
                            assert_eq!(rows, wanted, "{data_type}; vectorized={vectorized}; {sql}");
                            assert!(
                                session
                                    .warnings
                                    .iter()
                                    .any(|warning| warning.code() == 1292),
                                "missing runtime diagnostic: {sql}"
                            );
                            engine
                                .execute_with_session(&mut session, "DEALLOCATE PREPARE cmp")
                                .unwrap();
                        }
                    }
                }
            }
        });
    }
}

#[test]
fn temporal_comparison_cached_parameters_do_not_probe_zero_or_truncate_invalid_times() {
    for data_type in ["DATE", "DATETIME(6)", "TIMESTAMP(6)"] {
        let (_dir, engine, mut session) = fixture(data_type);
        engine
            .execute_with_session(&mut session, "SET @hi='2024-03-01'")
            .unwrap();
        for source in SOURCES {
            for (predicate, args, invalid_ids) in [
                ("d = ?", "@value", vec![]),
                ("d IN (?)", "@value", vec![]),
                ("d IN (?,?)", "@value,@hi", vec![3]),
                ("d BETWEEN ? AND ?", "@value,@hi", vec![]),
            ] {
                let sql = format!("SELECT id FROM {source} WHERE {predicate} ORDER BY id");
                engine
                    .execute_with_session(&mut session, &format!("PREPARE cached_cmp FROM '{sql}'"))
                    .unwrap();
                for value in [
                    "'2024-03-01'",
                    "'2024-02-30'",
                    "'2024-03-01 99:00:00'",
                    "CAST('2024-02-30' AS BINARY)",
                    "-20010101",
                    "'2024-03-01'",
                ] {
                    engine
                        .execute_with_session(&mut session, &format!("SET @value={value}"))
                        .unwrap();
                    for _ in 0..2 {
                        let rows = support::result_rows(
                            engine
                                .execute_with_session(
                                    &mut session,
                                    &format!("EXECUTE cached_cmp USING {args}"),
                                )
                                .unwrap(),
                        );
                        if value == "'2024-03-01'" {
                            assert_eq!(
                                rows,
                                vec![vec![Value::Int(3)]],
                                "{data_type}; {sql}; {value}"
                            );
                            assert!(session.warnings.is_empty(), "{sql}: {:?}", session.warnings);
                        } else {
                            assert_eq!(
                                rows,
                                invalid_ids
                                    .iter()
                                    .map(|id| vec![Value::Int(*id)])
                                    .collect::<Vec<_>>(),
                                "{data_type}; {sql}; {value}"
                            );
                            assert!(
                                session
                                    .warnings
                                    .iter()
                                    .any(|warning| warning.code() == 1292),
                                "{sql}; {value}"
                            );
                        }
                    }
                }
                engine
                    .execute_with_session(&mut session, "DEALLOCATE PREPARE cached_cmp")
                    .unwrap();
            }
        }
    }
}

#[test]
fn temporal_comparison_scope_and_dml_validate_before_empty_input_pruning() {
    let (_dir, engine, mut session) = fixture("DATE");
    engine
        .execute_with_session(&mut session, "CREATE TABLE strings(id INT,d VARCHAR(32))")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO strings VALUES (1,'2024-02-30')")
        .unwrap();
    for empty in [false, true] {
        if empty {
            engine
                .execute_with_session(&mut session, "DELETE FROM dates")
                .unwrap();
        }
        for predicate in [
            "d = '2024-02-30'",
            "d IN ('2024-02-30')",
            "d BETWEEN '2024-02-30' AND '2024-03-01'",
        ] {
            for sql in [
                format!(
                    "SELECT a.id FROM dates a JOIN strings b ON a.id=b.id WHERE {}",
                    predicate.replace("d ", "a.d ")
                ),
                format!(
                    "SELECT a.id FROM dates a JOIN strings b ON a.id=b.id WHERE {}",
                    predicate.replace("d ", "COALESCE(a.d,a.d) ")
                ),
                format!("WITH q AS (SELECT id,d FROM dates) SELECT id FROM q WHERE {predicate}"),
                format!("WITH strings AS (SELECT id,d FROM dates) SELECT id FROM strings WHERE {predicate}"),
                format!("WITH q AS (SELECT id,d FROM strings WHERE 1=0) SELECT id FROM q WHERE {}", predicate.replace("d ", "DATE(d) ")),
                format!(
                    "SELECT id FROM dates WHERE {}",
                    predicate.replace("d ", "CAST(d AS DATE) ")
                ),
                format!(
                    "SELECT id FROM dates WHERE {}",
                    predicate.replace("d ", "DATE(d) ")
                ),
                format!("UPDATE dates SET id=id WHERE {predicate}"),
                format!("DELETE FROM dates WHERE {predicate}"),
            ] {
                let err = engine
                    .execute_with_session(&mut session, &sql)
                    .expect_err(&sql);
                assert_eq!(err.code(), 1525, "empty={empty}; {sql}: {err}");
            }
        }
    }
    for predicate in [
        "DATE(d) BETWEEN '2024-02-30' AND '2024-03-01'",
        "CAST(d AS DATE) IN ('2024-02-30')",
    ] {
        let sql = format!("SELECT id FROM strings WHERE 1=0 AND {predicate}");
        engine
            .execute_with_session(&mut session, &sql.replace("2024-02-30", "2024-02-29"))
            .unwrap();
        assert_eq!(
            engine
                .execute_with_session(&mut session, &sql)
                .unwrap_err()
                .code(),
            1525,
            "{sql}"
        );
    }
    for sql in [
        "WITH dates AS (SELECT 1 AS id,'2024-02-30' AS d) SELECT id FROM dates WHERE d BETWEEN '2024-02-30' AND '2024-03-01'",
        "WITH dates AS (SELECT 1 AS id,'2024-02-30' AS d) SELECT a.id FROM dates a JOIN strings b ON a.id=b.id WHERE a.d BETWEEN '2024-02-30' AND '2024-03-01'",
    ] {
        let rows = support::result_rows(engine.execute_with_session(&mut session, sql).unwrap());
        assert_eq!(rows, vec![vec![Value::Int(1)]], "{sql}");
    }
    // A same-named VARCHAR in a join must retain string comparison semantics.
    engine.execute_with_session(&mut session, "SELECT b.id FROM dates a JOIN strings b ON a.id=b.id WHERE b.d BETWEEN '2024-02-30' AND '2024-03-01'").unwrap();
}

#[test]
fn temporal_comparison_valid_bounds_keep_leap_day_and_null_semantics() {
    for data_type in ["DATE", "DATETIME(6)", "TIMESTAMP(6)"] {
        let (_dir, engine, mut session) = fixture(data_type);
        for source in SOURCES {
            for (predicate, ids) in [
                ("d BETWEEN '2024-02-29' AND '2024-03-01'", vec![2, 3]),
                ("d IN ('2024-02-29','2024-03-01')", vec![2, 3]),
                ("d >= '2024-02-29' AND d <= '2024-03-01'", vec![2, 3]),
                ("d NOT BETWEEN NULL AND '2024-03-01'", vec![4]),
                ("d NOT IN ('2024-02-29',NULL)", vec![]),
            ] {
                let sql = format!("SELECT id FROM {source} WHERE {predicate} ORDER BY id");
                let rows =
                    support::result_rows(engine.execute_with_session(&mut session, &sql).unwrap());
                assert_eq!(
                    rows,
                    ids.into_iter()
                        .map(|id| vec![Value::Int(id)])
                        .collect::<Vec<_>>(),
                    "{data_type}; {sql}"
                );
                assert!(session.warnings.is_empty(), "unexpected diagnostic: {sql}");
            }
        }
    }
}
