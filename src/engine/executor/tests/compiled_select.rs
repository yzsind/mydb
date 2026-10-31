//! Differential + behavioural tests for the prepared-handle compiled SELECT
//! memo (`crate::engine::executor::compiled_select`). The cached path
//! must produce byte-identical results to the legacy clone+dispatch
//! prepared path, reuse a single compiled plan across executes, recompile
//! after DDL, and decline (fall back) for shapes it does not model.

use super::*;
use crate::engine::api::{PreparedSelectMemoState, QueryEngine};

fn rows_of(resp: QueryResponse) -> Vec<Vec<Value>> {
    match resp.materialize().expect("compiled SELECT materializes") {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected a result set, got {other:?}"),
    }
}

/// Run `sql`+`params` through the compile-once compiled path.
fn compiled(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    state: &mut PreparedSelectMemoState,
    parsed: &std::sync::Arc<Statement>,
    sql: &str,
    params: &[Value],
) -> Option<Result<QueryResponse, QueryError>> {
    engine.execute_prepared_select(session, state, parsed, sql, params)
}

/// Run the same prepared statement through the legacy clone+dispatch path
/// (with the bind parameters installed in the thread-local context), as the
/// differential oracle.
fn legacy(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    parsed: &std::sync::Arc<Statement>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResponse, QueryError> {
    crate::engine::params::with_params(params.to_vec(), || {
        engine
            .execute_prepared_statement(session, sql, (**parsed).clone())
            .expect("legacy prepared path ran")
    })
    .and_then(QueryResponse::materialize)
}

#[test]
fn prepared_zone_map_costing_keeps_runtime_parameters_after_recompile() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zone_params (id INT PRIMARY KEY, c CHAR(120), pad CHAR(200)) ZONE_SIZE=64K")
        .unwrap();
    for base in (0..1024).step_by(128) {
        let values = (base..base + 128)
            .map(|id| format!("({id},'{id:08}','{}')", "x".repeat(200)))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zone_params VALUES {values}"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE zone_params").unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT * FROM zone_params WHERE c=?";
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();

    for invalidate in [false, true] {
        if invalidate {
            engine.execute("ANALYZE TABLE zone_params").unwrap();
        }
        for (value, expected_id) in [
            ("abc", None),
            ("00000000", Some(0)),
            ("00001023", Some(1023)),
            ("abce", None),
        ] {
            let candidates_before = zone_candidate_generations(&engine);
            let rows = rows_of(
                compiled(
                    &engine,
                    &mut session,
                    &mut state,
                    &parsed,
                    sql,
                    &[Value::String(value.into())],
                )
                .expect("compiled parameterized scan")
                .unwrap(),
            );
            assert_eq!(rows.len(), usize::from(expected_id.is_some()), "{value}");
            if let Some(id) = expected_id {
                assert_eq!(rows[0][0], Value::Int(id));
            }
            assert!(
                zone_candidate_generations(&engine) > candidates_before,
                "compiled scan must execute Zone Map candidates for {value}"
            );
            if let PreparedSelectMemoState::Ready(memo) = &state {
                assert!(
                    memo.compiled.physical.any(|node| match node {
                        crate::planner::PhysicalPlan::ZoneMapScan {
                            filter: Some(filter),
                            ..
                        }
                        | crate::planner::PhysicalPlan::ParallelZoneMapScan {
                            filter: Some(filter),
                            ..
                        } => {
                            crate::parser::visit::any_expr(filter, |expr| {
                                matches!(expr, Expr::Placeholder(_))
                            })
                        }
                        _ => false,
                    }),
                    "cached Zone Map filter must retain placeholders"
                );
            } else {
                assert!(matches!(state, PreparedSelectMemoState::Uncached(_)));
            }
        }
    }
}

fn zone_candidate_generations(engine: &StorageQueryEngine) -> u64 {
    engine
        .db_for_tests()
        .zone_map_status_rows()
        .into_iter()
        .find(|(name, _)| *name == "zone_map_candidate_generations")
        .unwrap()
        .1
        .parse()
        .unwrap()
}

#[test]
fn uncached_prepared_zone_map_costing_keeps_runtime_parameters_after_recompile() {
    with_test_env(&[("MYDB_PLAN_CACHE", Some("off"))], || {
        prepared_zone_map_costing_keeps_runtime_parameters_after_recompile();
    });
}

#[test]
fn prepared_zone_map_costing_handles_compound_predicates_and_value_domains() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE zone_domains (n INT, s VARCHAR(32), b VARBINARY(32), d DECIMAL(10,2), dt DATE, pad CHAR(200)) ZONE_SIZE=64K").unwrap();
    for base in (0..1024).step_by(128) {
        let values = (base..base + 128)
            .map(|id| {
                format!(
                    "({id},'{id:08}','{id:08}',{id}.25,'2026-10-14','{}')",
                    "x".repeat(200)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO zone_domains VALUES {values}"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE zone_domains").unwrap();
    let string = |v: &str| Value::String(v.into());
    let cases = [
        ("n = ?", vec![Value::Int(-1)], vec![Value::Int(17)]),
        (
            "n BETWEEN ? AND ?",
            vec![Value::Int(-3), Value::Int(-1)],
            vec![Value::Int(17), Value::Int(19)],
        ),
        (
            "n IN (?, ?)",
            vec![Value::Int(-3), Value::Int(-1)],
            vec![Value::Int(17), Value::Int(19)],
        ),
        (
            "n = ? OR n = ?",
            vec![Value::Int(-3), Value::Int(-1)],
            vec![Value::Int(17), Value::Int(19)],
        ),
        ("n = ? + 1", vec![Value::Int(-3)], vec![Value::Int(17)]),
        (
            "s = CONCAT(?, ?)",
            vec![string("abs"), string("ent")],
            vec![string("0000"), string("0017")],
        ),
        (
            "s LIKE ?",
            vec![string("absent%")],
            vec![string("0000001%")],
        ),
        (
            "b = ?",
            vec![Value::Bytes(b"absent".to_vec())],
            vec![Value::Bytes(b"00000017".to_vec())],
        ),
        (
            "d = ?",
            vec![Value::Decimal("-1.25".into())],
            vec![Value::Decimal("17.25".into())],
        ),
        (
            "dt = ?",
            vec![Value::Date(
                crate::storage::parse_date_to_days_bytes(b"2025-01-01").unwrap(),
            )],
            vec![Value::Date(
                crate::storage::parse_date_to_days_bytes(b"2026-10-14").unwrap(),
            )],
        ),
        ("s <=> ?", vec![Value::Null], vec![string("00000017")]),
    ];
    let mut session = SessionContext::default();
    for (predicate, missing, present) in cases {
        let sql = format!("SELECT n FROM zone_domains WHERE {predicate} ORDER BY n");
        let parsed = engine
            .prepare_cached(&mut session, &sql, missing.len())
            .unwrap();
        let mut state = PreparedSelectMemoState::default();
        let oracle_sql = format!("SELECT /*+ NO_ZONE_MAP(zone_domains) */ n FROM zone_domains WHERE {predicate} ORDER BY n");
        let oracle = engine
            .prepare_cached(&mut session, &oracle_sql, missing.len())
            .unwrap();
        for (index, params) in [missing.clone(), present, missing].into_iter().enumerate() {
            let before = zone_candidate_generations(&engine);
            let actual = rows_of(
                compiled(&engine, &mut session, &mut state, &parsed, &sql, &params)
                    .unwrap()
                    .unwrap(),
            );
            if index == 0 {
                assert!(actual.is_empty(), "{predicate}");
                assert!(
                    zone_candidate_generations(&engine) > before,
                    "missing bindings must use Zone Map: {predicate}"
                );
            }
            assert_eq!(
                actual,
                rows_of(legacy(&engine, &mut session, &oracle, &oracle_sql, &params).unwrap()),
                "{predicate}: {params:?}"
            );
        }
    }
}

#[test]
fn repeatable_read_prepared_snapshot_pins_undo_between_statements() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.database().set_undo_retention_ms(0).unwrap();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let sql = "SELECT v FROM t WHERE id = ?";
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION transaction_isolation = 'REPEATABLE-READ'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION")
        .unwrap();
    let parsed = engine.prepare_cached(&mut reader, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();

    let first = compiled(
        &engine,
        &mut reader,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1)],
    )
    .expect("eligible prepared read takes the compiled path")
    .unwrap();
    assert_eq!(rows_of(first), vec![vec![Value::Int(10)]]);
    assert_eq!(
        engine.database().active_snapshot_count(),
        1,
        "RR snapshot must remain registered after the first statement"
    );

    engine.execute("UPDATE t SET v = 20 WHERE id = 1").unwrap();
    // Purge indexes records by millisecond timestamp using an exclusive upper
    // bound. Move beyond the writer's bucket so retention=0 is deterministic.
    std::thread::sleep(Duration::from_millis(2));
    engine.database().purge_obsolete_undo();
    assert!(
        engine.database().storage_status().undo_records >= 1,
        "the transaction-wide snapshot must retain the before-image"
    );

    let second = compiled(
        &engine,
        &mut reader,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1)],
    )
    .expect("eligible prepared read takes the compiled path")
    .unwrap();
    assert_eq!(
        rows_of(second),
        vec![vec![Value::Int(10)]],
        "the second RR read must reconstruct the transaction's original row"
    );

    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    assert_eq!(engine.database().active_snapshot_count(), 0);
    engine.database().purge_obsolete_undo();
    assert_eq!(engine.database().storage_status().undo_records, 0);
}

#[test]
fn autocommit_compiled_select_keeps_snapshot_registered_until_execution_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let sql = "SELECT v, SLEEP(0.4) FROM t WHERE id = ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let first = compiled(
        &engine,
        &mut session,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1)],
    )
    .expect("eligible prepared read takes the compiled path")
    .unwrap()
    .materialize()
    .unwrap();
    assert_eq!(rows_of(first), vec![vec![Value::Int(10), Value::Int(0)]]);
    assert!(matches!(state, PreparedSelectMemoState::Ready(_)));

    let worker_engine = std::sync::Arc::clone(&engine);
    let worker_parsed = std::sync::Arc::clone(&parsed);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut worker_session = SessionContext::default();
        let mut worker_state = state;
        started_tx.send(()).unwrap();
        compiled(
            &worker_engine,
            &mut worker_session,
            &mut worker_state,
            &worker_parsed,
            sql,
            &[Value::Int(1)],
        )
        .expect("memo remains eligible")
        .and_then(QueryResponse::materialize)
    });
    started_rx.recv().unwrap();

    let deadline = std::time::Instant::now() + Duration::from_millis(250);
    let mut observed_registered_snapshot = false;
    while std::time::Instant::now() < deadline {
        if engine.database().active_snapshot_count() > 0 {
            observed_registered_snapshot = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(
        observed_registered_snapshot,
        "compiled prepared execution must publish its fixed snapshot for purge"
    );
    let response = worker.join().unwrap().unwrap();
    assert_eq!(rows_of(response), vec![vec![Value::Int(10), Value::Int(0)]]);
    assert_eq!(
        engine.database().active_snapshot_count(),
        0,
        "statement and streaming-worker guards must both release at completion"
    );
}

#[test]
fn compiled_path_matches_legacy_across_params_and_reuses_one_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c'),(4,40,'d'),(5,50,'e')")
        .unwrap();

    let sql = "SELECT id, k, v FROM t WHERE id = ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();

    for id in [1i64, 3, 2, 5, 4, 1, 999] {
        let params = vec![Value::Int(id)];
        let fast_rows = rows_of(
            compiled(&engine, &mut session, &mut state, &parsed, sql, &params)
                .expect("eligible read takes the compiled path")
                .unwrap(),
        );
        let legacy_rows = rows_of(legacy(&engine, &mut session, &parsed, sql, &params).unwrap());
        assert_eq!(fast_rows, legacy_rows, "id={id}");
    }

    assert!(
        matches!(state, PreparedSelectMemoState::Ready(_)),
        "memo should be Ready"
    );
    let status = engine.db_for_tests().storage_status();
    assert_eq!(status.prepared_plan_compiles, 1, "compiled exactly once");
    assert_eq!(status.prepared_plan_reuses, 6, "six prepared L0 memo hits");
}

#[test]
fn prepared_string_param_uses_full_composite_index_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE customers (\
             c_w_id INT NOT NULL, c_d_id INT NOT NULL, c_id INT NOT NULL, \
             c_last VARCHAR(32), c_first VARCHAR(32), \
             PRIMARY KEY (c_w_id, c_d_id, c_id), \
             INDEX by_last (c_w_id, c_d_id, c_last, c_first))",
        )
        .unwrap();
    let mut values = String::new();
    for id in 1..=3_000i64 {
        if id > 1 {
            values.push(',');
        }
        let last = if id == 229 || id == 1_202 {
            "TARGET"
        } else {
            "OTHER"
        };
        values.push_str(&format!("(1,1,{id},'{last}','F{id:04}')"));
    }
    engine
        .execute(&format!("INSERT INTO customers VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE customers").unwrap();

    let sql = "SELECT c_id FROM customers \
               WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 3).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let before = crate::engine::expr_program::expr_program_stats().program_eval_rows;
    let response = compiled(
        &engine,
        &mut session,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1), Value::Int(1), Value::String("TARGET".into())],
    )
    .expect("eligible prepared read takes the compiled path")
    .unwrap();
    assert_eq!(
        rows_of(response),
        vec![vec![Value::Int(229)], vec![Value::Int(1_202)]]
    );
    let after = crate::engine::expr_program::expr_program_stats().program_eval_rows;
    let PreparedSelectMemoState::Ready(memo) = state else {
        panic!("prepared SELECT memo was not compiled");
    };
    let mut saw_full_prefix = false;
    memo.compiled.physical.any(|node| {
        if let crate::planner::PhysicalPlan::IndexLookup { index, key, .. } = node {
            saw_full_prefix = index.name == "by_last" && key.0.len() == 3;
        }
        saw_full_prefix
    });
    assert!(
        saw_full_prefix,
        "string-domain placeholder must use all three constrained index parts: {:?}",
        memo.compiled.physical
    );
    assert!(
        after.saturating_sub(before) <= 32,
        "full string prefix lookup must not residual-filter all 3000 rows: delta={}, plan={:?}",
        after.saturating_sub(before),
        memo.compiled.physical
    );
}

#[test]
fn prepared_character_comparison_replans_when_param_domain_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE codes (id INT PRIMARY KEY, code VARCHAR(16), INDEX code_idx(code))")
        .unwrap();
    engine
        .execute("INSERT INTO codes VALUES (1,'01'),(2,'1'),(3,'x')")
        .unwrap();

    let sql = "SELECT id FROM codes WHERE code = ? ORDER BY id";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();

    let string_rows = rows_of(
        compiled(
            &engine,
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::String("1".into())],
        )
        .expect("string execute is eligible")
        .unwrap(),
    );
    assert_eq!(string_rows, vec![vec![Value::Int(2)]]);
    let compiles_after_string = engine
        .db_for_tests()
        .storage_status()
        .prepared_plan_compiles;

    // MySQL compares a character column numerically when the other operand
    // is numeric, so both '01' and '1' match. Reusing the string-domain point
    // probe here would silently lose id=1; the parameter signature must make
    // this EXECUTE choose a new, numeric-domain-safe plan.
    let numeric_rows = rows_of(
        compiled(
            &engine,
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1)],
        )
        .expect("numeric execute is eligible")
        .unwrap(),
    );
    assert_eq!(numeric_rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    assert!(
        engine
            .db_for_tests()
            .storage_status()
            .prepared_plan_compiles
            > compiles_after_string,
        "changing the parameter comparison domain must recompile the memo"
    );
}

#[test]
fn prepared_wildcard_point_lookup_uses_compiled_projection() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE wildcard_point (id VARCHAR(32) PRIMARY KEY, n INT, payload TEXT, hidden INT INVISIBLE)").unwrap();
    engine.execute("INSERT INTO wildcard_point (id,n,payload,hidden) VALUES ('Alpha',7,'value',42),('Beta',NULL,NULL,99)").unwrap();
    for sql in [
        "SELECT * FROM wildcard_point WHERE id = ?",
        "SELECT p.* FROM wildcard_point AS p WHERE p.id = ?",
        "SELECT p.*, p.n AS again, p.hidden FROM wildcard_point AS p WHERE p.id = ?",
    ] {
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let mut state = PreparedSelectMemoState::default();
        for key in ["Alpha", "Beta", "absent", "alpha"] {
            let params = [Value::String(key.into())];
            let actual = compiled(&engine, &mut session, &mut state, &parsed, sql, &params)
                .unwrap()
                .unwrap()
                .materialize()
                .unwrap();
            let expected = legacy(&engine, &mut session, &parsed, sql, &params).unwrap();
            let (QueryResponse::ResultSet(actual), QueryResponse::ResultSet(expected)) =
                (actual, expected)
            else {
                panic!("result sets expected")
            };
            assert_eq!(actual.rows, expected.rows, "{sql}: {key}");
            assert_eq!(actual.columns, expected.columns, "metadata: {sql}");
            let repeated = sql.contains("again");
            let names = actual
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                names,
                if repeated {
                    vec!["id", "n", "payload", "again", "hidden"]
                } else {
                    vec!["id", "n", "payload"]
                }
            );
            let mut expected_row = match key {
                "Alpha" | "alpha" => vec![
                    Value::String("Alpha".into()),
                    Value::Int(7),
                    Value::String("value".into()),
                ],
                "Beta" => vec![Value::String("Beta".into()), Value::Null, Value::Null],
                _ => {
                    assert!(actual.rows.is_empty());
                    continue;
                }
            };
            if repeated {
                expected_row.push(expected_row[1].clone());
                expected_row.push(Value::Int(if key == "Beta" { 99 } else { 42 }));
            }
            assert_eq!(actual.rows, vec![expected_row]);
        }
        let PreparedSelectMemoState::Ready(memo) = state else {
            panic!("compiled memo expected")
        };
        assert!(
            matches!(
                memo.compiled.executable.as_ref(),
                crate::engine::cache::CompiledOperator::ProjectedUniquePointLookup(_)
            ),
            "wildcard point lookup must reuse the compiled projection: {:?}",
            memo.compiled.executable
        );
    }
}

#[test]
fn wildcard_point_snapshot_survives_concurrent_key_repair_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE repair_race (id INT PRIMARY KEY, payload TEXT)")
        .unwrap();
    engine
        .execute("INSERT INTO repair_race VALUES (1,'original')")
        .unwrap();
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let sql = "SELECT * FROM repair_race WHERE id=?";
    let parsed = engine.prepare_cached(&mut reader, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let expected = rows_of(
        compiled(
            &engine,
            &mut reader,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1)],
        )
        .unwrap()
        .unwrap(),
    );
    assert_eq!(
        expected,
        vec![vec![Value::Int(1), Value::String("original".into())]]
    );
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut writer = SessionContext::default();
            barrier.wait();
            for round in 0..400 {
                engine.execute_with_session(&mut writer, "BEGIN").unwrap();
                let mutation = if round % 2 == 0 {
                    "UPDATE repair_race SET id=2,payload='new' WHERE id=1"
                } else {
                    "DELETE FROM repair_race WHERE id=1"
                };
                engine.execute_with_session(&mut writer, mutation).unwrap();
                engine
                    .execute_with_session(&mut writer, "ROLLBACK")
                    .unwrap();
            }
        });
        barrier.wait();
        for _ in 0..800 {
            for key in [1, 2] {
                let actual = rows_of(
                    compiled(
                        &engine,
                        &mut reader,
                        &mut state,
                        &parsed,
                        sql,
                        &[Value::Int(key)],
                    )
                    .unwrap()
                    .unwrap(),
                );
                if key == 1 {
                    assert_eq!(actual, expected);
                } else {
                    assert!(actual.is_empty());
                }
            }
        }
    });
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
}

#[test]
fn prepared_wildcard_point_lookup_types_and_schema_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE wildcard_types (id INT PRIMARY KEY, n INT, d DECIMAL(12,2), f DOUBLE, c CHAR(8), b VARBINARY(20), j JSON, dt DATETIME, v INT GENERATED ALWAYS AS (n+1) VIRTUAL, s INT GENERATED ALWAYS AS (n+2) STORED)").unwrap();
    engine.execute("INSERT INTO wildcard_types (id,n,d,f,c,b,j,dt) VALUES (1,7,12.34,1.5,'abc',X'00FF','{\"a\":1}','2026-09-24 12:34:56')").unwrap();
    let sql = "SELECT * FROM wildcard_types WHERE id=?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    for ddl in [
        None,
        Some("ALTER TABLE wildcard_types ADD COLUMN added INT NOT NULL DEFAULT 23 FIRST"),
        Some("ALTER TABLE wildcard_types DROP COLUMN f"),
        Some("ALTER TABLE wildcard_types MODIFY COLUMN c CHAR(8) AFTER id"),
    ] {
        if let Some(ddl) = ddl {
            engine.execute(ddl).unwrap();
        }
        let params = [Value::Int(1)];
        let actual = compiled(&engine, &mut session, &mut state, &parsed, sql, &params)
            .unwrap()
            .unwrap()
            .materialize()
            .unwrap();
        let expected = legacy(&engine, &mut session, &parsed, sql, &params).unwrap();
        let (QueryResponse::ResultSet(actual), QueryResponse::ResultSet(expected)) =
            (actual, expected)
        else {
            panic!("result sets expected")
        };
        assert_eq!(actual.rows, expected.rows, "{ddl:?}");
        assert_eq!(actual.columns, expected.columns, "metadata: {ddl:?}");
        for (name, value) in [("v", "8"), ("s", "9")] {
            let index = actual.columns.iter().position(|c| c.name == name).unwrap();
            assert_eq!(actual.rows[0][index].to_display_string(), value);
        }
    }
}

#[test]
fn prepared_primary_key_point_select_compiles_projected_lookup_operator() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE point_lookup_t (\
             id INT PRIMARY KEY, \
             k INT NOT NULL, \
             c CHAR(120) NOT NULL, \
             pad CHAR(60) NOT NULL, \
             KEY k_1 (k))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO point_lookup_t VALUES (1, 7, 'value', 'pad')")
        .unwrap();

    let sql = "SELECT c FROM point_lookup_t WHERE id = ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let response = compiled(
        &engine,
        &mut session,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1)],
    )
    .expect("eligible prepared read takes the compiled path")
    .unwrap();
    assert!(
        matches!(&response, QueryResponse::ResultSet(_)),
        "compiled unique point lookup must finish in its fused result kernel, not a streaming worker"
    );
    assert_eq!(rows_of(response), vec![vec![Value::String("value".into())]]);

    let PreparedSelectMemoState::Ready(memo) = state else {
        panic!("prepared SELECT memo was not compiled");
    };
    let crate::engine::cache::CompiledOperator::ProjectedUniquePointLookup(operator) =
        memo.compiled.executable.as_ref()
    else {
        panic!(
            "primary-key equality should lower to the compiled projected point lookup, got {:?}",
            memo.compiled.executable
        );
    };
    assert!(
        operator.projection.decode_plan.has_fast_steps(),
        "compiled point lookup must retain codec-specialized decode steps"
    );
    assert_eq!(operator.projection.columns.as_ref(), &[2]);
    assert_eq!(
        operator.key_projection.as_ref().unwrap().columns.as_ref(),
        &[2, 0],
        "only a visibility repair should require the extra key column"
    );
}

#[test]
fn bounded_unique_point_join_avoids_top_level_stream_worker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE point_left (id INT PRIMARY KEY, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE TABLE point_right (id INT PRIMARY KEY, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO point_left VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO point_right VALUES (1, 100), (2, 200)")
        .unwrap();

    let sql = "SELECT l.v, r.v FROM point_left l JOIN point_right r ON r.id = l.id \
               WHERE l.id = 1 AND r.id = 1";
    let response = engine.execute(sql).unwrap();
    assert!(
        matches!(&response, QueryResponse::ResultSet(_)),
        "a statically bounded unique-point join must execute inline"
    );
    assert_eq!(
        rows_of(response),
        vec![vec![Value::Int(10), Value::Int(100)]]
    );

    let forced = engine.execute_streaming_for_test(sql).unwrap();
    assert!(
        matches!(&forced, QueryResponse::StreamingResultSet(_)),
        "the explicit streaming test scope must still override the bounded-result optimization"
    );
    assert_eq!(rows_of(forced), vec![vec![Value::Int(10), Value::Int(100)]]);
}

#[test]
fn prepared_ordered_distinct_range_uses_projected_range_operator() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE range_t (id INT PRIMARY KEY, c VARCHAR(32), pad VARCHAR(64))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO range_t VALUES \
             (1,'beta','x'),(2,'Alpha','y'),(3,'BETA','z'),(4,'delta','w')",
        )
        .unwrap();

    for sql in [
        "SELECT c FROM range_t WHERE id BETWEEN ? AND ? ORDER BY c",
        "SELECT DISTINCT c FROM range_t WHERE id BETWEEN ? AND ? ORDER BY c",
    ] {
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
        let mut state = PreparedSelectMemoState::default();
        let params = [Value::Int(1), Value::Int(4)];
        let fast_rows = rows_of(
            compiled(&engine, &mut session, &mut state, &parsed, sql, &params)
                .expect("eligible prepared range takes the compiled path")
                .unwrap(),
        );
        let legacy_rows = rows_of(legacy(&engine, &mut session, &parsed, sql, &params).unwrap());
        assert_eq!(fast_rows, legacy_rows, "compiled result differs for {sql}");

        let PreparedSelectMemoState::Ready(memo) = state else {
            panic!("prepared range SELECT memo was not compiled");
        };
        assert!(
            matches!(
                memo.compiled.executable.as_ref(),
                crate::engine::cache::CompiledOperator::ProjectedIndexRangeOrMultiLookup
            ),
            "ORDER BY / DISTINCT wrappers must preserve projected range classification: {:?}",
            memo.compiled.executable
        );
    }
}

#[test]
fn prepared_unique_integer_range_uses_runtime_cardinality_proof() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE range_bound_t (id BIGINT PRIMARY KEY, c VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO range_bound_t VALUES (1,'a'),(100,'b'),(10000,'c')")
        .unwrap();

    let sql = "SELECT c FROM range_bound_t WHERE id BETWEEN ? AND ? ORDER BY c";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let params = [Value::Int(1), Value::Int(100)];
    let response = compiled(&engine, &mut session, &mut state, &parsed, sql, &params)
        .expect("eligible prepared range takes the compiled path")
        .unwrap();
    assert!(
        matches!(&response, QueryResponse::ResultSet(_)),
        "a unique integer interval bounded to one batch must execute synchronously"
    );
    assert_eq!(
        rows_of(response),
        vec![
            vec![Value::String("a".into())],
            vec![Value::String("b".into())]
        ]
    );

    let PreparedSelectMemoState::Ready(memo) = state else {
        panic!("prepared range SELECT memo was not compiled");
    };
    crate::engine::params::with_params(vec![Value::Int(1), Value::Int(100)], || {
        assert!(
            crate::engine::executor::select::simple::runtime_unique_range_fits_batch(
                &memo.compiled.physical,
                crate::engine::executor::vec::DEFAULT_BATCH_ROWS,
            )
            .unwrap(),
            "resolved inclusive unique range has a semantic 100-row upper bound"
        );
    });
    crate::engine::params::with_params(
        vec![
            Value::Int(1),
            Value::Int(crate::engine::executor::vec::DEFAULT_BATCH_ROWS as i64 + 1),
        ],
        || {
            assert!(
                !crate::engine::executor::select::simple::runtime_unique_range_fits_batch(
                    &memo.compiled.physical,
                    crate::engine::executor::vec::DEFAULT_BATCH_ROWS,
                )
                .unwrap(),
                "wide parameter intervals must retain the streaming path"
            );
        },
    );
}

#[test]
fn text_auto_param_and_prepared_placeholder_share_compiled_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE shared_t (id INT PRIMARY KEY, v VARCHAR(16))")
        .unwrap();
    engine
        .execute("INSERT INTO shared_t VALUES (1,'a'),(2,'b')")
        .unwrap();

    let QueryResponse::ResultSet(text_rows) = engine
        .execute("SELECT id, v FROM shared_t WHERE id = 1")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected text result set");
    };
    assert_eq!(text_rows.rows.len(), 1);
    let after_text = engine.db_for_tests().storage_status();

    let sql = "SELECT id, v FROM shared_t WHERE id = ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let response = compiled(
        &engine,
        &mut session,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(2)],
    )
    .expect("prepared SELECT is compiled")
    .unwrap();
    let prepared_rows = rows_of(response);
    assert_eq!(prepared_rows.len(), 1);
    assert_eq!(prepared_rows[0][0], Value::Int(2));

    let after_prepared = engine.db_for_tests().storage_status();
    assert_eq!(
        after_prepared.select_operator_compiles, after_text.select_operator_compiles,
        "prepared placeholder must reuse the text auto-param compiled operator"
    );
    assert!(
        after_prepared.select_compiled_plan_hits > after_text.select_compiled_plan_hits,
        "prepared L0 compilation should hit the shared compiled-plan cache"
    );
}

#[test]
fn compiled_path_count_star_unqualified_in_non_default_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE DATABASE shop")
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE shop")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE orders (id INT PRIMARY KEY, total INT)",
        )
        .unwrap();
    for i in 1..=5i64 {
        engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO orders VALUES ({i}, {})", i * 10),
            )
            .unwrap();
    }

    // Prepare the *unqualified* COUNT(*) while the session's default
    // database is `shop` (not the catalog default `test`). The prepared
    // AST is qualified to `shop.orders` at prepare time; the compiled path
    // must count the same 5 rows the legacy path sees.
    let sql = "SELECT COUNT(*) FROM orders";
    let parsed = engine.prepare_cached(&mut session, sql, 0).unwrap();
    let mut state = PreparedSelectMemoState::default();

    let fast_rows = rows_of(
        compiled(&engine, &mut session, &mut state, &parsed, sql, &[])
            .expect("eligible read takes the compiled path")
            .unwrap(),
    );
    let legacy_rows = rows_of(legacy(&engine, &mut session, &parsed, sql, &[]).unwrap());
    assert_eq!(legacy_rows, vec![vec![Value::Int(5)]], "legacy COUNT(*)");
    assert_eq!(
        fast_rows, legacy_rows,
        "compiled-path COUNT(*) must match legacy"
    );
}

#[test]
fn compiled_path_matches_legacy_for_varied_where_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY, k INT, label VARCHAR(16))")
        .unwrap();
    engine.execute("CREATE INDEX idx_k ON s(k)").unwrap();
    for i in 1..=20i64 {
        engine
            .execute(&format!(
                "INSERT INTO s VALUES ({i}, {}, 'r{i}')",
                (i * 7) % 11
            ))
            .unwrap();
    }

    // Each shape is exercised through its own memo; results must match the
    // legacy path for several parameter sets.
    let cases: &[(&str, usize, Vec<Vec<Value>>)] = &[
        (
            "SELECT id, k FROM s WHERE id = ? ORDER BY id",
            1,
            vec![vec![Value::Int(4)], vec![Value::Int(17)]],
        ),
        (
            "SELECT id FROM s WHERE k > ? ORDER BY id",
            1,
            vec![vec![Value::Int(3)], vec![Value::Int(7)]],
        ),
        (
            "SELECT id FROM s WHERE k BETWEEN ? AND ? ORDER BY id",
            2,
            vec![vec![Value::Int(2), Value::Int(5)]],
        ),
        (
            "SELECT id, label FROM s WHERE id IN (?, ?, ?) ORDER BY id",
            3,
            vec![vec![Value::Int(1), Value::Int(9), Value::Int(15)]],
        ),
    ];

    for (sql, n, param_sets) in cases {
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, sql, *n).unwrap();
        let mut state = PreparedSelectMemoState::default();
        for params in param_sets {
            let fast_res = compiled(&engine, &mut session, &mut state, &parsed, sql, params)
                .expect("eligible read takes the compiled path")
                .unwrap();
            let legacy_res = legacy(&engine, &mut session, &parsed, sql, params).unwrap();
            assert_eq!(
                rows_of(fast_res),
                rows_of(legacy_res),
                "sql={sql} params={params:?}"
            );
        }
    }
}

#[test]
fn compiled_path_matches_legacy_for_sysbench_read_write_range_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE sbtest1 (\
             id INT PRIMARY KEY, \
             k INT NOT NULL, \
             c CHAR(120) NOT NULL, \
             pad CHAR(60) NOT NULL, \
             KEY k_1 (k))",
        )
        .unwrap();
    for i in 1..=40i64 {
        let c = if i % 4 == 0 {
            "dup"
        } else if i % 4 == 1 {
            "z"
        } else if i % 4 == 2 {
            "a"
        } else {
            "m"
        };
        engine
            .execute(&format!(
                "INSERT INTO sbtest1 VALUES ({i}, {}, '{c}', 'pad')",
                i % 7
            ))
            .unwrap();
    }
    let cases: &[(&str, usize, Vec<Vec<Value>>)] = &[
        (
            "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?",
            2,
            vec![vec![Value::Int(3), Value::Int(18)]],
        ),
        (
            "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c",
            2,
            vec![vec![Value::Int(3), Value::Int(18)]],
        ),
        (
            "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c",
            2,
            vec![vec![Value::Int(3), Value::Int(18)]],
        ),
        (
            "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?",
            2,
            vec![
                vec![Value::Int(3), Value::Int(18)],
                vec![Value::Int(100), Value::Int(120)],
            ],
        ),
    ];

    for (sql, param_count, param_sets) in cases {
        let mut session = SessionContext::default();
        let parsed = engine
            .prepare_cached(&mut session, sql, *param_count)
            .unwrap();
        let mut state = PreparedSelectMemoState::default();
        for params in param_sets {
            let response = compiled(&engine, &mut session, &mut state, &parsed, sql, params)
                .expect("eligible read takes the compiled path")
                .unwrap();
            assert!(
                matches!(&response, QueryResponse::ResultSet(_)),
                "a one-batch prepared range must finish in the fused kernel: {sql}"
            );
            let fast_rows = rows_of(response);
            let legacy_rows = rows_of(legacy(&engine, &mut session, &parsed, sql, params).unwrap());
            assert_eq!(fast_rows, legacy_rows, "sql={sql} params={params:?}");
        }
        let PreparedSelectMemoState::Ready(memo) = state else {
            panic!("prepared range SELECT memo was not compiled: {sql}");
        };
        assert!(
            matches!(
                memo.compiled.executable.as_ref(),
                crate::engine::cache::CompiledOperator::ProjectedIndexRangeOrMultiLookup
            ),
            "sysbench range shape must retain the fused range classification: {sql}: {:?}",
            memo.compiled.executable
        );
    }
}

#[test]
fn compiled_path_recompiles_after_ddl_and_stays_correct() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY, a INT)")
        .unwrap();
    for i in 1..=10i64 {
        engine
            .execute(&format!("INSERT INTO d VALUES ({i}, {})", i * 100))
            .unwrap();
    }

    let sql = "SELECT id FROM d WHERE a = ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let p = vec![Value::Int(300)];

    let before = rows_of(
        compiled(&engine, &mut session, &mut state, &parsed, sql, &p)
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        rows_of(legacy(&engine, &mut session, &parsed, sql, &p).unwrap()),
        before
    );
    assert_eq!(
        engine
            .db_for_tests()
            .storage_status()
            .prepared_plan_compiles,
        1
    );

    // Building an index on the filtered column changes the optimal plan
    // (SeqScan+filter -> IndexLookup) and bumps the table schema version, so
    // the cached plan's deps no longer match and the memo must be recompiled.
    // The AST is unchanged, so the same prepared handle is reused.
    engine.execute("CREATE INDEX idx_a ON d(a)").unwrap();

    let after = rows_of(
        compiled(&engine, &mut session, &mut state, &parsed, sql, &p)
            .expect("still eligible")
            .unwrap(),
    );
    assert_eq!(
        after, before,
        "recompiled plan returns the same rows as before the DDL"
    );
    assert_eq!(
        rows_of(legacy(&engine, &mut session, &parsed, sql, &p).unwrap()),
        after
    );
    assert!(
        engine
            .db_for_tests()
            .storage_status()
            .prepared_plan_compiles
            >= 2,
        "schema change must force a recompile"
    );
}

#[test]
fn compiled_path_in_current_transaction_sees_own_uncommitted_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE txr (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO txr VALUES (1, 10)").unwrap();

    let sql = "SELECT v FROM txr WHERE id = ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let params = vec![Value::Int(1)];

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE txr SET v = 20 WHERE id = 1")
        .unwrap();
    assert!(
        session.current_txn.is_some(),
        "the prepared read must exercise the current-txn path"
    );

    let fast_rows = rows_of(
        compiled(&engine, &mut session, &mut state, &parsed, sql, &params)
            .expect("non-locking read in a write transaction takes the compiled path")
            .unwrap(),
    );
    assert_eq!(fast_rows, vec![vec![Value::Int(20)]]);
    assert!(
        matches!(state, PreparedSelectMemoState::Ready(_)),
        "memo should remain usable in the write transaction"
    );
    assert_eq!(
        engine.db_for_tests().storage_status().prepared_plan_reuses,
        0,
        "first execution compiles the handle memo; it is not an L0 reuse"
    );

    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let mut oracle = SessionContext::default();
    let legacy_rows = rows_of(legacy(&engine, &mut oracle, &parsed, sql, &params).unwrap());
    assert_eq!(legacy_rows, vec![vec![Value::Int(10)]]);
}

#[test]
fn compiled_path_rechecks_residual_against_visible_version_during_index_key_update() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ixr (id INT PRIMARY KEY, k INT, v VARCHAR(8), KEY idx_k(k))")
        .unwrap();
    engine
        .execute("INSERT INTO ixr VALUES (1, 1, 'old')")
        .unwrap();

    let sql = "SELECT v FROM ixr WHERE k BETWEEN ? AND ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE ixr SET k = 100 WHERE id = 1")
        .unwrap();

    let old_key_params = vec![Value::Int(1), Value::Int(1)];
    let new_key_params = vec![Value::Int(100), Value::Int(100)];
    let old_key_rows = rows_of(
        compiled(
            &engine,
            &mut session,
            &mut state,
            &parsed,
            sql,
            &old_key_params,
        )
        .expect("eligible read takes the compiled path")
        .unwrap(),
    );
    let new_key_rows = rows_of(
        compiled(
            &engine,
            &mut session,
            &mut state,
            &parsed,
            sql,
            &new_key_params,
        )
        .expect("eligible read takes the compiled path")
        .unwrap(),
    );
    assert_eq!(old_key_rows, Vec::<Vec<Value>>::new());
    assert_eq!(new_key_rows, vec![vec![Value::String("old".into())]]);

    let legacy_old = rows_of(legacy(&engine, &mut session, &parsed, sql, &old_key_params).unwrap());
    let legacy_new = rows_of(legacy(&engine, &mut session, &parsed, sql, &new_key_params).unwrap());
    assert_eq!(legacy_old, old_key_rows);
    assert_eq!(legacy_new, new_key_rows);

    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn compiled_path_declines_ineligible_statements() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE w (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO w VALUES (1, 1)").unwrap();

    // FOR UPDATE takes row locks -> not a read-only compiled path.
    let sql = "SELECT * FROM w WHERE id = ? FOR UPDATE";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let out = compiled(
        &engine,
        &mut session,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1)],
    );
    assert!(
        out.is_none(),
        "FOR UPDATE must fall back to the legacy path"
    );
    assert!(
        matches!(state, PreparedSelectMemoState::Ineligible),
        "ineligibility should be memoized"
    );
    // A second execute short-circuits without recompiling.
    assert!(compiled(
        &engine,
        &mut session,
        &mut state,
        &parsed,
        sql,
        &[Value::Int(1)]
    )
    .is_none());
    assert_eq!(
        engine
            .db_for_tests()
            .storage_status()
            .prepared_plan_compiles,
        0,
        "ineligible statements are never compiled"
    );
}

#[test]
fn prepared_exact_range_count_keeps_index_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE exact_count (id BIGINT UNSIGNED PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO exact_count VALUES (1),(2),(3)")
        .unwrap();
    let sql = "SELECT COUNT(*) FROM exact_count FORCE INDEX(PRIMARY) WHERE 1=1 AND id>=? AND id<?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();
    for (low, high, count) in [(1, 3, 2), (2, 4, 2)] {
        let response = compiled(
            &engine,
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(low), Value::Int(high)],
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows_of(response), vec![vec![Value::Int(count)]]);
        let PreparedSelectMemoState::Ready(memo) = &state else {
            panic!("expected compiled memo")
        };
        assert!(
            format!("{:?}", memo.compiled.physical).contains("IndexAggregate"),
            "{:?}",
            memo.compiled.physical
        );
    }
}

#[test]
fn projected_unique_point_rechecks_visible_keys_and_reuses_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE compact_point (id INT PRIMARY KEY, code VARCHAR(32) UNIQUE, amount DECIMAL(12,2), spare TEXT)").unwrap();
    engine
        .execute(
            "INSERT INTO compact_point VALUES (1,'Alpha',1.25,'payload'),(2,'beta',NULL,'other')",
        )
        .unwrap();
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT * FROM compact_point")
        .unwrap()
        .materialize()
        .unwrap();
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE compact_point SET id=3,code='gamma',amount=99 WHERE id=1",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "INSERT INTO compact_point VALUES (1,'Alpha',88,'replacement')",
        )
        .unwrap();
    for sql in [
        "SELECT amount,code,amount FROM compact_point WHERE id=?",
        "SELECT code,amount,code FROM compact_point WHERE code=?",
        "SELECT * FROM compact_point WHERE id=?",
        "SELECT * FROM compact_point WHERE code=?",
    ] {
        let parsed = engine.prepare_cached(&mut reader, sql, 1).unwrap();
        let mut state = PreparedSelectMemoState::default();
        let params = if sql.ends_with("id=?") {
            vec![
                Value::Int(1),
                Value::Int(3),
                Value::Int(2),
                Value::Null,
                Value::Int(1),
            ]
        } else {
            vec![
                Value::String("Alpha".into()),
                Value::String("gamma".into()),
                Value::String("beta".into()),
                Value::Null,
                Value::String("Alpha".into()),
            ]
        };
        for value in params {
            let actual = rows_of(
                compiled(
                    &engine,
                    &mut reader,
                    &mut state,
                    &parsed,
                    sql,
                    &[value.clone()],
                )
                .unwrap()
                .unwrap(),
            );
            // An explicit expected row count checks the invisible new-key
            // case independently of other executor routes.
            let missing = matches!(&value, Value::Null | Value::Int(3))
                || value == Value::String("gamma".into());
            assert_eq!(actual.len(), usize::from(!missing), "{sql}: {value:?}");
            assert_eq!(
                actual,
                rows_of(legacy(&engine, &mut reader, &parsed, sql, &[value]).unwrap())
            );
        }
    }
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    assert_eq!(
        rows_of(
            engine
                .execute_with_session(&mut reader, "SELECT amount FROM compact_point WHERE id=1")
                .unwrap()
        )[0][0]
            .to_display_string(),
        "1.25"
    );
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows_of(
            engine
                .execute("SELECT amount FROM compact_point WHERE id=1")
                .unwrap()
        )[0][0]
            .to_display_string(),
        "88.00"
    );
}

#[test]
fn unique_point_repair_proof_is_scoped_to_its_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE repair_scope (id INT PRIMARY KEY, code VARCHAR(32) UNIQUE)")
        .unwrap();
    engine
        .execute("INSERT INTO repair_scope VALUES (1,'old')")
        .unwrap();
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE repair_scope SET code='new' WHERE id=1")
        .unwrap();
    let db = engine.database();
    let table = db.table("test.repair_scope").unwrap();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let (view, _guard) =
        db.read_only_statement_view_with_guard(crate::storage::IsolationLevel::ReadCommitted, None);
    let candidate = db
        .read_unique_index_candidate(
            &table,
            primary,
            crate::storage::IndexKey::from(vec![crate::storage::KeyPart::Int(1)]),
            view,
            |row_id, recheck| {
                assert!(
                    recheck.is_none(),
                    "an unrelated secondary-key repair must not widen the primary-key projection"
                );
                Ok(Some(row_id))
            },
        )
        .unwrap();
    assert!(candidate.is_some());
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
}

#[test]
fn low_frequency_text_cache_keeps_selectivity_variants() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE cache_skew (id INT PRIMARY KEY, v INT, KEY kv(v))")
        .unwrap();
    engine.execute("INSERT INTO cache_skew SELECT n, CASE WHEN n<4 THEN 0 ELSE 1 END FROM generate_series(0,4095) gs(n)").unwrap();
    engine.execute("ANALYZE TABLE cache_skew").unwrap();
    let initial = engine.plan_cache().stats().size;
    for (value, expected) in [(0, 4), (1, 4092), (0, 4), (1, 4092)] {
        let rows = rows_of(
            engine
                .execute(&format!("SELECT COUNT(*) FROM cache_skew WHERE v={value}"))
                .unwrap(),
        );
        assert_eq!(
            crate::storage::value_to_string(&rows[0][0]),
            expected.to_string()
        );
    }
    assert_eq!(engine.plan_cache().stats().size, initial + 2);
    let variants: Vec<_> = engine
        .plan_cache()
        .snapshot()
        .into_iter()
        .filter(|e| e.sql.contains(":s0="))
        .collect();
    assert_eq!(variants.len(), 2);
    assert!(variants.iter().all(|e| e.hits >= 1));
}

#[test]
fn low_frequency_prepared_memo_reuses_only_matching_selectivity_tier() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE memo_skew (id INT PRIMARY KEY, v INT, KEY kv(v))")
        .unwrap();
    engine
        .execute("INSERT INTO memo_skew VALUES (1,0),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1)")
        .unwrap();
    engine.execute("ANALYZE TABLE memo_skew").unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT COUNT(*) FROM memo_skew WHERE v=?";
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let mut plans = Vec::new();
    for (value, expected) in [(0, 1), (1, 7), (0, 1)] {
        let rows = rows_of(
            compiled(
                &engine,
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(value)],
            )
            .unwrap()
            .unwrap(),
        );
        assert_eq!(
            crate::storage::value_to_string(&rows[0][0]),
            expected.to_string()
        );
        let PreparedSelectMemoState::Ready(memo) = &state else {
            panic!("compiled memo");
        };
        plans.push(memo.compiled.clone());
    }
    assert!(!std::sync::Arc::ptr_eq(&plans[0], &plans[1]));
    assert!(std::sync::Arc::ptr_eq(&plans[0], &plans[2]));
}

#[test]
fn scalar_point_plan_reuses_operator_and_rebinds_scalar_results() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE scalar_point_rows (tenant INT,id INT,v INT,PRIMARY KEY(tenant,id))",
        "CREATE TABLE scalar_point_limit (id INT PRIMARY KEY,n INT)",
        "INSERT INTO scalar_point_rows VALUES (1,1,10),(1,2,20),(1,3,30)",
        "INSERT INTO scalar_point_limit VALUES (1,1),(2,3)",
    ] {
        engine.execute(sql).unwrap();
    }
    let sql="SELECT v FROM scalar_point_rows WHERE tenant=? AND id=(SELECT n FROM scalar_point_limit WHERE id=?)";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();
    assert_eq!(
        rows_of(
            compiled(
                &engine,
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(1), Value::Int(1)]
            )
            .unwrap()
            .unwrap()
        ),
        vec![vec![Value::Int(10)]]
    );
    let PreparedSelectMemoState::Ready(memo) = &state else {
        panic!("memo")
    };
    let point = Arc::clone(
        &memo
            .compiled
            .runtime_scalar_point
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .compiled,
    );
    engine
        .execute("UPDATE scalar_point_limit SET n=2 WHERE id=1")
        .unwrap();
    assert_eq!(
        rows_of(
            compiled(
                &engine,
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(1), Value::Int(1)]
            )
            .unwrap()
            .unwrap()
        ),
        vec![vec![Value::Int(20)]]
    );
    let PreparedSelectMemoState::Ready(memo) = &state else {
        panic!("memo")
    };
    assert!(Arc::ptr_eq(
        &point,
        &memo
            .compiled
            .runtime_scalar_point
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .compiled
    ));
    engine
        .execute("UPDATE scalar_point_limit SET n=NULL WHERE id=1")
        .unwrap();
    assert!(rows_of(
        compiled(
            &engine,
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1), Value::Int(1)]
        )
        .unwrap()
        .unwrap()
    )
    .is_empty());
    assert_eq!(
        rows_of(
            compiled(
                &engine,
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(1), Value::Int(2)]
            )
            .unwrap()
            .unwrap()
        ),
        vec![vec![Value::Int(30)]]
    );
    engine
        .execute("ALTER TABLE scalar_point_rows ADD COLUMN extra INT DEFAULT 7")
        .unwrap();
    assert_eq!(
        rows_of(
            compiled(
                &engine,
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(1), Value::Int(2)]
            )
            .unwrap()
            .unwrap()
        ),
        vec![vec![Value::Int(30)]]
    );
    engine
        .execute("ALTER TABLE scalar_point_limit MODIFY COLUMN n BIGINT")
        .unwrap();
    assert_eq!(
        rows_of(
            compiled(
                &engine,
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(1), Value::Int(2)]
            )
            .unwrap()
            .unwrap()
        ),
        vec![vec![Value::Int(30)]]
    );
}

#[test]
fn scalar_point_numeric_boundaries_match_apply_oracle() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE scalar_decimal_rows (tenant INT,id DECIMAL(20,2),PRIMARY KEY(tenant,id))",
        "CREATE TABLE scalar_decimal_limit (id INT PRIMARY KEY,n DECIMAL(20,3))",
        "INSERT INTO scalar_decimal_rows VALUES(1,1.00),(1,1.01)",
        "INSERT INTO scalar_decimal_limit VALUES(1,1.004)",
        "CREATE TABLE scalar_float_rows (tenant INT,id FLOAT,PRIMARY KEY(tenant,id))",
        "CREATE TABLE scalar_float_limit (id INT PRIMARY KEY,n DOUBLE)",
        "INSERT INTO scalar_float_rows VALUES(1,0.1),(1,1.0)",
        "INSERT INTO scalar_float_limit VALUES(1,0.1)",
        "CREATE TABLE scalar_wide_rows (tenant INT,id DECIMAL(20,0),PRIMARY KEY(tenant,id))",
        "CREATE TABLE scalar_wide_limit (id INT PRIMARY KEY,n DOUBLE)",
        "INSERT INTO scalar_wide_rows VALUES(1,9007199254740992),(1,9007199254740993)",
        "INSERT INTO scalar_wide_limit VALUES(1,9007199254740992)",
    ] {
        engine.execute(sql).unwrap();
    }
    for kind in ["decimal", "float", "wide"] {
        let sql=format!("SELECT id FROM scalar_{kind}_rows WHERE tenant=? AND id=(SELECT n FROM scalar_{kind}_limit WHERE id=?)");
        let oracle=format!("SELECT id FROM scalar_{kind}_rows WHERE tenant=1 AND id=(SELECT n FROM scalar_{kind}_limit WHERE id=1)");
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, &sql, 2).unwrap();
        let mut state = PreparedSelectMemoState::default();
        for value in ["1.004", "1.000", "1.006"] {
            if kind == "decimal" {
                engine
                    .execute(&format!(
                        "UPDATE scalar_decimal_limit SET n={value} WHERE id=1"
                    ))
                    .unwrap();
            }
            let expected = rows_of(engine.execute(&oracle).unwrap());
            let actual = rows_of(
                compiled(
                    &engine,
                    &mut session,
                    &mut state,
                    &parsed,
                    &sql,
                    &[Value::Int(1), Value::Int(1)],
                )
                .unwrap()
                .unwrap(),
            );
            assert_eq!(actual, expected, "{kind}, {value}");
        }
    }
}
