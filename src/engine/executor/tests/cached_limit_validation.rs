use super::*;
use crate::engine::api::{with_shared_result_metadata, PreparedSelectMemoState};
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

fn result(response: QueryResponse) -> crate::engine::api::ResultSet {
    match response.materialize().unwrap() {
        QueryResponse::ResultSet(r) => r,
        QueryResponse::SharedResultSet(r) => crate::engine::api::ResultSet {
            columns: r.columns.to_vec(),
            rows: r.rows,
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

fn execute(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    state: &mut PreparedSelectMemoState,
    parsed: &Arc<Statement>,
    sql: &str,
    params: &[Value],
    mode: VecMode,
) -> crate::engine::api::ResultSet {
    with_vec_mode(mode, || {
        with_shared_result_metadata(|| {
            result(
                engine
                    .execute_prepared_select(session, state, parsed, sql, params)
                    .expect("compiled SELECT must be eligible")
                    .unwrap(),
            )
        })
    })
}

#[test]
fn cached_limit_validation_rejects_invalid_rebinding() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ce(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO ce VALUES(1),(2),(3)").unwrap();
    let sql = "SELECT id FROM ce FORCE INDEX(PRIMARY) WHERE id>=? ORDER BY id LIMIT ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();
    execute(
        &engine,
        &mut session,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1), Value::Int(1)],
        VecMode::Auto,
    );
    for mode in [VecMode::Auto, VecMode::On] {
        for limit in [Value::String("2".into()), Value::Bytes(b"2".to_vec())] {
            assert_eq!(
                execute(
                    &engine,
                    &mut session,
                    &mut state,
                    &parsed,
                    sql,
                    &[Value::Int(1), limit],
                    mode,
                )
                .rows,
                vec![vec![Value::Int(1)], vec![Value::Int(2)]]
            );
        }
    }
    for (limit, expected_code) in [
        (Value::Int(-1), 1690),
        (Value::String("invalid".into()), 1210),
        (Value::Bytes(b"invalid".to_vec()), 1210),
        (Value::String("".into()), 1210),
        (Value::String("2abc".into()), 1210),
        (Value::Double(2.0), 1210),
    ] {
        let params = [Value::Int(1), limit];
        let mut codes = Vec::new();
        for mode in [VecMode::Auto, VecMode::On] {
            let error = with_vec_mode(mode, || {
                with_shared_result_metadata(|| {
                    engine
                        .execute_prepared_select(&mut session, &mut state, &parsed, sql, &params)
                        .unwrap()
                        .unwrap_err()
                })
            });
            codes.push(error.code());
        }
        assert_eq!(codes, vec![expected_code, expected_code]);
    }
}

#[test]
fn cached_limit_validation_survives_aggregate_and_subquery_lowering() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE cl(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO cl VALUES(1),(2),(3)").unwrap();
    for (sql,valid,invalid) in [
        ("SELECT COUNT(*) FROM cl LIMIT ?",vec![Value::Int(1)],vec![Value::Int(-1)]),
        ("SELECT id FROM cl WHERE EXISTS(SELECT 1 FROM cl x WHERE x.id=cl.id LIMIT ?) ORDER BY id LIMIT ?",vec![Value::Int(1),Value::Int(1)],vec![Value::Int(-1),Value::Int(1)]),
        ("SELECT id FROM cl ORDER BY id LIMIT ? OFFSET ?",vec![Value::Int(1),Value::Int(0)],vec![Value::Int(1),Value::Int(-1)]),
    ] {
        let mut session=SessionContext::default();let mut state=PreparedSelectMemoState::default();
        let parsed=engine.prepare_cached(&mut session,sql,valid.len()).unwrap();
        execute(&engine,&mut session,&mut state,&parsed,sql,&valid,VecMode::Auto);
        for mode in [VecMode::Auto,VecMode::On,VecMode::Off] {
            let error=with_vec_mode(mode,||with_shared_result_metadata(||
                engine.execute_prepared_select(&mut session,&mut state,&parsed,sql,&invalid)
                    .unwrap().unwrap_err()));
            assert!(error.is(crate::error::catalog::ER_DATA_OUT_OF_RANGE),"{sql}: {error:?}");
        }
    }
}
