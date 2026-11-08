use super::*;
use crate::engine::api::{with_shared_result_metadata, PreparedDmlState, PreparedSelectMemoState};
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

fn rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response.materialize().unwrap() {
        QueryResponse::ResultSet(result) => result.rows,
        QueryResponse::SharedResultSet(result) => result.rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn fixture(
    precision: usize,
    scale: usize,
) -> (tempfile::TempDir, StorageQueryEngine, String, String) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let low = format!("0.{}", "9".repeat(scale));
    let high = format!("1.{}1", "0".repeat(scale - 1));
    engine.execute(&format!("CREATE TABLE ds(id INT PRIMARY KEY,tenant INT NOT NULL,d DECIMAL({precision},{scale}),mark INT DEFAULT 0,KEY vd(d,id),KEY tv(tenant,d,id))")).unwrap();
    engine.execute(&format!("CREATE TABLE dp(d DECIMAL({precision},{scale}) NOT NULL PRIMARY KEY,id INT,mark INT DEFAULT 0)")).unwrap();
    engine.execute(&format!("INSERT INTO ds(id,tenant,d) VALUES(1,1,{low}),(2,1,{high}),(3,1,2),(4,2,{low}),(5,2,{high}),(6,1,NULL),(7,1,-{high}),(8,1,-{low})")).unwrap();
    engine
        .execute(&format!(
            "INSERT INTO dp(d,id) VALUES({low},1),({high},2),(2,3),(-{high},7),(-{low},8)"
        ))
        .unwrap();
    engine.execute("ANALYZE TABLE ds,dp").unwrap();
    (dir, engine, low, high)
}

fn prepared_rows(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    state: &mut PreparedSelectMemoState,
    statement: &Arc<Statement>,
    sql: &str,
    params: &[Value],
    mode: VecMode,
) -> Vec<Vec<Value>> {
    with_vec_mode(mode, || {
        with_shared_result_metadata(|| {
            rows(
                engine
                    .execute_prepared_select(session, state, statement, sql, params)
                    .unwrap()
                    .unwrap(),
            )
        })
    })
}

#[test]
fn decimal_index_literals_match_full_scans_across_precisions_and_access_shapes() {
    for (precision, scale) in [(18, 17), (38, 18), (65, 30)] {
        let (_dir, engine, _, _) = fixture(precision, scale);
        for (table, index, ignored, equal) in [
            ("ds", "vd", "PRIMARY,vd,tv", 4),
            ("dp", "PRIMARY", "PRIMARY", 2),
        ] {
            for (predicate, count) in [
                ("d=1e0", equal),
                ("d<=>1e0", equal),
                ("d<1e0", 2),
                ("d<=1e0", equal + 2),
                ("d>1e0", 1),
                ("d>=1e0", equal + 1),
                ("d BETWEEN 1e0 AND 1e0", equal),
                ("d IN(1e0,2)", equal + 1),
                ("d=-1e0", 2),
            ] {
                let sql = format!(
                    "SELECT id FROM {table} FORCE INDEX({index}) WHERE {predicate} ORDER BY id"
                );
                let oracle = format!(
                    "SELECT id FROM {table} IGNORE INDEX({ignored}) WHERE {predicate} ORDER BY id"
                );
                for mode in [VecMode::Auto, VecMode::On, VecMode::Off] {
                    let expected = with_vec_mode(mode, || rows(engine.execute(&oracle).unwrap()));
                    assert_eq!(
                        expected.len(),
                        count,
                        "DECIMAL({precision},{scale}): {oracle}"
                    );
                    let actual = with_vec_mode(mode, || rows(engine.execute(&sql).unwrap()));
                    assert_eq!(
                        actual, expected,
                        "DECIMAL({precision},{scale}): {sql}, {mode:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn decimal_index_prepared_handles_rebind_exact_float_and_null_domains() {
    for (precision, scale) in [(18, 17), (38, 18), (65, 30)] {
        let (_dir, engine, low, high) = fixture(precision, scale);
        for (table, index, ignored, copies) in [
            ("ds", "vd", "PRIMARY,vd,tv", 2),
            ("dp", "PRIMARY", "PRIMARY", 1),
        ] {
            for limited in [false, true] {
                let suffix = if limited { " LIMIT ?" } else { "" };
                let sql = format!(
                    "SELECT id FROM {table} FORCE INDEX({index}) WHERE d=? ORDER BY id{suffix}"
                );
                let oracle = format!(
                    "SELECT id FROM {table} IGNORE INDEX({ignored}) WHERE d=? ORDER BY id{suffix}"
                );
                let mut session = SessionContext::default();
                let statement = engine
                    .prepare_cached(&mut session, &sql, 1 + usize::from(limited))
                    .unwrap();
                let oracle_statement = engine
                    .prepare_cached(&mut session, &oracle, 1 + usize::from(limited))
                    .unwrap();
                let mut state = PreparedSelectMemoState::default();
                let mut oracle_state = PreparedSelectMemoState::default();
                for (value, count, indexed) in [
                    (Value::Decimal(high.clone()), copies, true),
                    (Value::Float(1.0), 2 * copies, false),
                    (Value::Decimal(low.clone()), copies, true),
                    (Value::Double(1.0), 2 * copies, false),
                    (Value::Int(2), 1, true),
                    (Value::UInt(2), 1, true),
                    (Value::Null, 0, false),
                    (Value::Decimal(high.clone()), copies, true),
                ] {
                    let mut params = vec![value];
                    if limited {
                        params.push(Value::Int(10));
                    }
                    for mode in [VecMode::Auto, VecMode::On, VecMode::Off] {
                        let expected = prepared_rows(
                            &engine,
                            &mut session,
                            &mut oracle_state,
                            &oracle_statement,
                            &oracle,
                            &params,
                            mode,
                        );
                        assert_eq!(expected.len(), count, "{oracle}: {params:?}");
                        let actual = prepared_rows(
                            &engine,
                            &mut session,
                            &mut state,
                            &statement,
                            &sql,
                            &params,
                            mode,
                        );
                        assert_eq!(actual, expected, "{sql}: {params:?}, {mode:?}");
                        if indexed {
                            let PreparedSelectMemoState::Ready(memo) = &state else {
                                panic!("expected compiled plan")
                            };
                            assert!(memo.compiled.physical.any(|plan| matches!(plan, crate::planner::PhysicalPlan::IndexLookup { index: selected, .. } if selected.name.eq_ignore_ascii_case(index))), "exact comparisons must retain index access: {sql}, {params:?}");
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn decimal_index_composite_prefix_and_mixed_in_bounds_match_full_scans() {
    let (_dir, engine, _, high) = fixture(65, 30);
    for predicate in [
        "tenant=1 AND d=1e0",
        "tenant=1 AND d>=1e0",
        "tenant=1 AND d IN(1e0,2)",
        "d=1e0 OR id=3",
    ] {
        let sql =
            format!("SELECT id FROM ds FORCE INDEX(tv,vd,PRIMARY) WHERE {predicate} ORDER BY id");
        let oracle =
            format!("SELECT id FROM ds IGNORE INDEX(tv,vd,PRIMARY) WHERE {predicate} ORDER BY id");
        assert_eq!(
            rows(engine.execute(&sql).unwrap()),
            rows(engine.execute(&oracle).unwrap()),
            "{sql}"
        );
    }
    for predicate in ["tenant=? AND d BETWEEN ? AND ?", "tenant=? AND d IN(?,?)"] {
        let sql = format!("SELECT id FROM ds FORCE INDEX(tv) WHERE {predicate} ORDER BY id");
        let oracle =
            format!("SELECT id FROM ds IGNORE INDEX(tv,vd,PRIMARY) WHERE {predicate} ORDER BY id");
        let mut session = SessionContext::default();
        let statement = engine.prepare_cached(&mut session, &sql, 3).unwrap();
        let oracle_statement = engine.prepare_cached(&mut session, &oracle, 3).unwrap();
        let mut state = PreparedSelectMemoState::default();
        let mut oracle_state = PreparedSelectMemoState::default();
        for value in [
            Value::Decimal(high.clone()),
            Value::Double(1.0),
            Value::Float(1.0),
            Value::Decimal(high.clone()),
        ] {
            let params = [Value::Int(1), value.clone(), value];
            let expected = prepared_rows(
                &engine,
                &mut session,
                &mut oracle_state,
                &oracle_statement,
                &oracle,
                &params,
                VecMode::On,
            );
            let actual = prepared_rows(
                &engine,
                &mut session,
                &mut state,
                &statement,
                &sql,
                &params,
                VecMode::Auto,
            );
            assert_eq!(actual, expected, "{sql}: {params:?}");
        }
    }
}

#[test]
fn decimal_index_locking_read_rebinds_without_losing_matching_rows() {
    let (_dir, engine, low, high) = fixture(65, 30);
    let sql = "SELECT id FROM dp WHERE d=? FOR UPDATE";
    let mut session = SessionContext::default();
    let statement = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedDmlState::default();
    for (value, mut expected) in [
        (Value::Decimal(high.clone()), vec![vec![Value::Int(2)]]),
        (
            Value::Double(1.0),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ),
        (Value::Decimal(low), vec![vec![Value::Int(1)]]),
        (
            Value::Float(1.0),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ),
    ] {
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        let response = engine
            .execute_prepared_dml(&mut session, &mut state, &statement, sql, &[value])
            .unwrap()
            .unwrap();
        let mut actual = rows(response);
        actual.sort_by_key(|row| row[0].to_display_string());
        expected.sort_by_key(|row| row[0].to_display_string());
        assert_eq!(actual, expected);
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
    }
}

#[test]
fn decimal_index_prepared_writes_rebind_exact_and_approximate_predicates() {
    let (_dir, engine, _, high) = fixture(65, 30);
    for sql in [
        "UPDATE dp SET mark=mark+1 WHERE d=?",
        "DELETE FROM dp WHERE d=?",
    ] {
        let mut session = SessionContext::default();
        let statement = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let mut state = PreparedDmlState::default();
        for (value, expected) in [
            (Value::Decimal(high.clone()), 1),
            (Value::Double(1.0), 2),
            (Value::Float(1.0), 2),
            (Value::Decimal(high.clone()), 1),
        ] {
            engine.execute_with_session(&mut session, "BEGIN").unwrap();
            let response = engine
                .execute_prepared_dml(&mut session, &mut state, &statement, sql, &[value])
                .unwrap()
                .unwrap();
            let QueryResponse::OkAffected { affected_rows } = response else {
                panic!("expected affected rows")
            };
            assert_eq!(affected_rows, expected, "{sql}");
            engine
                .execute_with_session(&mut session, "ROLLBACK")
                .unwrap();
        }
    }
}

#[test]
fn decimal_index_scale_zero_keeps_adjacent_large_integers_in_float_equality() {
    for precision in [18, 38, 65] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute(&format!(
                "CREATE TABLE dz(d DECIMAL({precision},0) PRIMARY KEY,id INT)"
            ))
            .unwrap();
        engine.execute("INSERT INTO dz VALUES(9007199254740992,1),(9007199254740993,2),(9007199254740996,3)").unwrap();
        let sql = "SELECT id FROM dz FORCE INDEX(PRIMARY) WHERE d=? ORDER BY id";
        let oracle = "SELECT id FROM dz IGNORE INDEX(PRIMARY) WHERE d=? ORDER BY id";
        let mut session = SessionContext::default();
        let statement = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let oracle_statement = engine.prepare_cached(&mut session, oracle, 1).unwrap();
        let mut state = PreparedSelectMemoState::default();
        let mut oracle_state = PreparedSelectMemoState::default();
        for (value, expected) in [
            (Value::UInt(9007199254740992), vec![vec![Value::Int(1)]]),
            (
                Value::Double(9007199254740992.0),
                vec![vec![Value::Int(1)], vec![Value::Int(2)]],
            ),
            (
                Value::Decimal("9007199254740993".into()),
                vec![vec![Value::Int(2)]],
            ),
            (
                Value::Double(9007199254740992.0),
                vec![vec![Value::Int(1)], vec![Value::Int(2)]],
            ),
        ] {
            for mode in [VecMode::Auto, VecMode::On, VecMode::Off] {
                let params = std::slice::from_ref(&value);
                let scanned = prepared_rows(
                    &engine,
                    &mut session,
                    &mut oracle_state,
                    &oracle_statement,
                    oracle,
                    params,
                    mode,
                );
                assert_eq!(scanned, expected, "precision {precision}, {value:?}");
                assert_eq!(
                    prepared_rows(
                        &engine,
                        &mut session,
                        &mut state,
                        &statement,
                        sql,
                        params,
                        mode
                    ),
                    scanned
                );
            }
        }
    }
}

#[test]
fn decimal_index_functional_keys_preserve_approximate_comparison_domain() {
    let (_dir, engine, _, _) = fixture(65, 30);
    engine.execute("CREATE INDEX ad ON ds((ABS(d)))").unwrap();
    engine.execute("ANALYZE TABLE ds").unwrap();
    for predicate in [
        "ABS(d)=1e0",
        "ABS(d) BETWEEN 1e0 AND 1e0",
        "ABS(d) IN(1e0,2)",
    ] {
        let sql = format!("SELECT id FROM ds FORCE INDEX(ad) WHERE {predicate} ORDER BY id");
        let oracle = format!(
            "SELECT id FROM ds IGNORE INDEX(PRIMARY,vd,tv,ad) WHERE {predicate} ORDER BY id"
        );
        let expected = rows(engine.execute(&oracle).unwrap());
        assert!(expected.len() >= 6);
        assert_eq!(rows(engine.execute(&sql).unwrap()), expected, "{sql}");
    }
}

#[test]
fn decimal_index_wide_exact_values_do_not_round_in_scan_predicates() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let center = "1000000000000000000000000000000";
    let low = format!("{}.{}", "9".repeat(30), "9".repeat(30));
    let high = format!("{center}.{}1", "0".repeat(29));
    engine
        .execute("CREATE TABLE dw(d DECIMAL(65,30) PRIMARY KEY,id INT)")
        .unwrap();
    engine
        .execute(&format!(
            "INSERT INTO dw VALUES({low},1),({high},2),({center},3)"
        ))
        .unwrap();
    let sql = "SELECT id FROM dw FORCE INDEX(PRIMARY) WHERE d=? ORDER BY id";
    let oracle = "SELECT id FROM dw IGNORE INDEX(PRIMARY) WHERE d=? ORDER BY id";
    let mut session = SessionContext::default();
    let statement = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let oracle_statement = engine.prepare_cached(&mut session, oracle, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let mut oracle_state = PreparedSelectMemoState::default();
    for (value, expected) in [
        (Value::Decimal(center.into()), vec![vec![Value::Int(3)]]),
        (
            Value::Double(1e30),
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)],
            ],
        ),
        (Value::Decimal(high), vec![vec![Value::Int(2)]]),
        (Value::Decimal(low), vec![vec![Value::Int(1)]]),
        (Value::Decimal(center.into()), vec![vec![Value::Int(3)]]),
    ] {
        for mode in [VecMode::Auto, VecMode::On, VecMode::Off] {
            let params = std::slice::from_ref(&value);
            let scanned = prepared_rows(
                &engine,
                &mut session,
                &mut oracle_state,
                &oracle_statement,
                oracle,
                params,
                mode,
            );
            assert_eq!(scanned, expected, "{value:?}, {mode:?}");
            assert_eq!(
                prepared_rows(
                    &engine,
                    &mut session,
                    &mut state,
                    &statement,
                    sql,
                    params,
                    mode
                ),
                scanned
            );
        }
    }
}
