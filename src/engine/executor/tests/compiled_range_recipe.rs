use super::*;
use crate::engine::api::{with_shared_result_metadata, PreparedSelectMemoState};

fn owned_result(response: QueryResponse) -> crate::engine::api::ResultSet {
    match response.materialize().unwrap() {
        QueryResponse::ResultSet(result) => result,
        QueryResponse::SharedResultSet(result) => crate::engine::api::ResultSet {
            columns: result.columns.to_vec(),
            rows: result.rows,
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn bounded_prepared_parallel_aggregate_uses_compact_range_execution() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE bounded_agg (id INT PRIMARY KEY, v DECIMAL(12,2), f DOUBLE, u BIGINT UNSIGNED, s VARCHAR(16), i INT)").unwrap();
    let values = (0..2048)
        .map(|id| {
            format!(
                "({id},{},1.25,{id},'1.25',{id})",
                if id % 3 == 0 { "NULL" } else { "1.25" }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO bounded_agg VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE bounded_agg").unwrap();
    for projection in [
        "SUM(v)",
        "SUM(i)",
        "SUM(u)",
        "SUM(f)",
        "SUM(s)",
        "AVG(v)",
        "COUNT(v)",
        "MIN(v)",
        "MAX(v)",
        "COALESCE(SUM(v),0)",
        "SUM(CASE WHEN id>10 THEN v ELSE 0 END)",
    ] {
        let sql = format!("SELECT /*+ PARALLEL(4) */ {projection} FROM bounded_agg FORCE INDEX(PRIMARY) WHERE id BETWEEN ? AND ?");
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, &sql, 2).unwrap();
        let mut state = PreparedSelectMemoState::default();
        let cap = crate::engine::executor::vec::DEFAULT_BATCH_ROWS as i64;
        // Rebind the same handle across small, empty, all-NULL and wide ranges.
        // A wide interval must retain the parallel plan even for sparse data.
        for (low, high, bounded) in [
            (1, 100, true),
            (20, 23, true),
            (3000, 3100, true),
            (0, 0, true),
            (0, cap, false),
            (0, cap - 1, true),
            (1, 2, true),
        ] {
            let params = [Value::Int(low), Value::Int(high)];
            let response = with_shared_result_metadata(|| {
                engine.execute_prepared_select(&mut session, &mut state, &parsed, &sql, &params)
            })
            .unwrap()
            .unwrap();
            let PreparedSelectMemoState::Ready(memo) = &state else {
                panic!("expected compiled plan")
            };
            assert!(
                memo.compiled.physical.any(|node| matches!(
                    node,
                    crate::planner::PhysicalPlan::ParallelHashAggregate { .. }
                )),
                "test requires a parallel aggregate for {projection}: {:?}",
                crate::planner::explain_rows(&memo.compiled.physical)
            );
            assert!(
                matches!(
                    memo.compiled.executable.as_ref(),
                    crate::engine::cache::CompiledOperator::ProjectedIndexRangeOrMultiLookup
                ),
                "parallel scalar aggregate must retain its bounded range kernel: {projection}"
            );
            assert_eq!(
                crate::engine::params::with_params_slice(&params, || {
                    crate::engine::executor::select::simple::runtime_unique_range_fits_batch(
                        &memo.compiled.physical,
                        cap as usize,
                    )
                    .unwrap()
                }),
                bounded,
                "range proof must use the current bindings"
            );
            let expected = engine
                .execute(&format!(
                    "SELECT {projection} FROM bounded_agg WHERE id BETWEEN {low} AND {high}"
                ))
                .unwrap();
            let actual = owned_result(response);
            let expected = owned_result(expected);
            assert_eq!(actual.rows, expected.rows, "{projection}, {low}..={high}");
            // The ordinary API fills source defaults eagerly; compiled
            // execution leaves those defaults to the protocol encoder.
            let wire_columns = |columns: &[Column]| {
                columns
                    .iter()
                    .map(|column| {
                        crate::mysql::protocol::column_definition_packet_full(
                            &column.name,
                            column.column_type,
                            &column.source,
                        )
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                wire_columns(&actual.columns),
                wire_columns(&expected.columns),
                "{projection}"
            );
        }
    }
}

#[test]
fn range_cardinality_proof_requires_a_complete_nonnull_unique_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE range_unique (id INT PRIMARY KEY, a INT, b INT, v INT, UNIQUE KEY ab(a,b))").unwrap();
    engine.execute("INSERT INTO range_unique VALUES (1,1,1,10),(2,1,2,20),(3,2,1,30),(4,2,2,40),(5,NULL,1,50),(6,NULL,1,60),(7,NULL,2,70)").unwrap();
    for (predicate, params, expected) in [
        (
            "a BETWEEN ? AND ?",
            vec![Value::Int(1), Value::Int(2)],
            false,
        ),
        (
            "a=? AND b BETWEEN ? AND ?",
            vec![Value::Int(1), Value::Int(1), Value::Int(2)],
            true,
        ),
        (
            "a IS NULL AND b BETWEEN ? AND ?",
            vec![Value::Int(1), Value::Int(2)],
            false,
        ),
    ] {
        let sql = format!(
            "SELECT /*+ PARALLEL(4) */ SUM(v) FROM range_unique FORCE INDEX(ab) WHERE {predicate}"
        );
        let mut session = SessionContext::default();
        let parsed = engine
            .prepare_cached(&mut session, &sql, params.len())
            .unwrap();
        let mut state = PreparedSelectMemoState::default();
        let result = with_shared_result_metadata(|| {
            engine.execute_prepared_select(&mut session, &mut state, &parsed, &sql, &params)
        })
        .unwrap()
        .unwrap();
        owned_result(result);
        let PreparedSelectMemoState::Ready(memo) = state else {
            panic!("expected compiled plan")
        };
        assert!(
            memo.compiled
                .physical
                .any(|node| matches!(node, crate::planner::PhysicalPlan::IndexRangeScan { .. })),
            "expected an index range for {predicate}"
        );
        assert_eq!(
            crate::engine::params::with_params_slice(&params, || {
                crate::engine::executor::select::simple::runtime_unique_range_fits_batch(
                    &memo.compiled.physical,
                    2,
                )
                .unwrap()
            }),
            expected,
            "integer width bounds row count only for a complete non-NULL unique key: {predicate}"
        );
    }
}

#[test]
fn bounded_parallel_aggregate_preserves_visible_versions_during_key_update() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE agg_versions (id INT PRIMARY KEY, v DECIMAL(12,2))")
        .unwrap();
    engine
        .execute("INSERT INTO agg_versions VALUES (1,10.25),(2,20.50),(3,NULL)")
        .unwrap();
    let sql = "SELECT /*+ PARALLEL(4) */ SUM(v) FROM agg_versions FORCE INDEX(PRIMARY) WHERE id BETWEEN ? AND ?";
    let mut reader = SessionContext::default();
    let parsed = engine.prepare_cached(&mut reader, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let read = |session: &mut SessionContext, state: &mut PreparedSelectMemoState| {
        owned_result(
            with_shared_result_metadata(|| {
                engine.execute_prepared_select(
                    session,
                    state,
                    &parsed,
                    sql,
                    &[Value::Int(1), Value::Int(2)],
                )
            })
            .unwrap()
            .unwrap(),
        )
        .rows
    };
    let initial = read(&mut reader, &mut state);
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE agg_versions SET id=1000,v=99.75 WHERE id=1",
        )
        .unwrap();
    assert_eq!(
        read(&mut reader, &mut state),
        initial,
        "a committed reader must retain the old key and value"
    );
    let mut writer_state = PreparedSelectMemoState::default();
    let own = read(&mut writer, &mut writer_state);
    let expected = owned_result(
        engine
            .execute_with_session(
                &mut writer,
                "SELECT SUM(v) FROM agg_versions WHERE id BETWEEN 1 AND 2",
            )
            .unwrap(),
    )
    .rows;
    assert_eq!(own, expected, "the writer must see its own moved key");
    assert_ne!(own, initial);
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    assert_eq!(read(&mut reader, &mut state), initial);
}

#[test]
fn cached_range_recipe_rebinds_bounds_and_preserves_collated_distinct_and_projection() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE recipe_rows (id INT PRIMARY KEY, c VARCHAR(30) COLLATE utf8mb4_0900_ai_ci, d DECIMAL(12,2))").unwrap();
    engine.execute("INSERT INTO recipe_rows VALUES (1,'a',1.1),(2,'A',2.2),(3,'b',3.3),(4,NULL,4.4),(5,'c',5.5),(6,'a',6.6)").unwrap();
    let padding = (20000..22000)
        .map(|id| format!("({id},'padding',0.0)"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO recipe_rows VALUES {padding}"))
        .unwrap();
    engine.execute("ANALYZE TABLE recipe_rows").unwrap();
    for sql in [
        "SELECT DISTINCT c FROM recipe_rows WHERE id BETWEEN ? AND ? ORDER BY c",
        "SELECT d,c FROM recipe_rows WHERE id BETWEEN ? AND ? ORDER BY id DESC",
    ] {
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
        let mut state = PreparedSelectMemoState::default();
        for (low, high) in [(1, 6), (3, 6), (1, 3), (4, 6)] {
            let response = with_shared_result_metadata(|| {
                engine.execute_prepared_select(
                    &mut session,
                    &mut state,
                    &parsed,
                    sql,
                    &[Value::Int(low), Value::Int(high)],
                )
            })
            .unwrap()
            .unwrap();
            assert!(matches!(response, QueryResponse::SharedResultSet(_)));
            let actual = owned_result(response);
            let text = sql
                .replacen('?', &low.to_string(), 1)
                .replacen('?', &high.to_string(), 1);
            let expected = owned_result(engine.execute(&text).unwrap());
            assert_eq!(
                actual.rows, expected.rows,
                "bound values must not freeze in the recipe"
            );
            assert_eq!(actual.columns, expected.columns);
        }
    }
}

#[test]
fn cached_range_recipe_rechecks_retired_keys_and_invalidates_after_column_type_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE recipe_visible (id INT PRIMARY KEY, c VARCHAR(30))")
        .unwrap();
    engine
        .execute("INSERT INTO recipe_visible VALUES (1,'one'),(2,'two'),(3,'three')")
        .unwrap();
    let padding = (20000..22000)
        .map(|id| format!("({id},'padding')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO recipe_visible VALUES {padding}"))
        .unwrap();
    engine.execute("ANALYZE TABLE recipe_visible").unwrap();
    let mut reader = SessionContext::default();
    let sql = "SELECT c FROM recipe_visible WHERE id BETWEEN ? AND ? ORDER BY id";
    let parsed = engine.prepare_cached(&mut reader, sql, 2).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE recipe_visible SET id=1000,c='hidden' WHERE id=1",
        )
        .unwrap();
    let response = with_shared_result_metadata(|| {
        engine.execute_prepared_select(
            &mut reader,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1), Value::Int(3)],
        )
    })
    .unwrap()
    .unwrap()
    .materialize()
    .unwrap();
    let result = owned_result(response);
    assert_eq!(
        result.rows,
        vec![
            vec![Value::String("one".into())],
            vec![Value::String("two".into())],
            vec![Value::String("three".into())]
        ]
    );
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    engine
        .execute("ALTER TABLE recipe_visible MODIFY c VARBINARY(30)")
        .unwrap();
    let response = with_shared_result_metadata(|| {
        engine.execute_prepared_select(
            &mut reader,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1), Value::Int(2)],
        )
    })
    .unwrap()
    .unwrap()
    .materialize()
    .unwrap();
    let result = owned_result(response);
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Bytes(b"one".to_vec())],
            vec![Value::Bytes(b"two".to_vec())]
        ]
    );
    assert_eq!(result.columns[0].source.collation_id, Some(63));
}

#[test]
fn shared_prepared_point_metadata_keeps_current_alias_and_identifier_presentation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE recipe_names (id INT PRIMARY KEY,c VARCHAR(30))")
        .unwrap();
    engine
        .execute("INSERT INTO recipe_names VALUES (1,'visible')")
        .unwrap();
    for (sql, name, alias) in [
        (
            "SELECT Src.c AS Label FROM recipe_names AS Src WHERE Src.id=?",
            "Label",
            "Src",
        ),
        (
            "SELECT src.c AS lAbEl FROM recipe_names AS src WHERE src.id=?",
            "lAbEl",
            "src",
        ),
        ("SELECT C FROM recipe_names WHERE ID=?", "C", "recipe_names"),
        ("SELECT c FROM recipe_names WHERE id=?", "c", "recipe_names"),
    ] {
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let mut state = PreparedSelectMemoState::default();
        let result = owned_result(
            with_shared_result_metadata(|| {
                engine.execute_prepared_select(
                    &mut session,
                    &mut state,
                    &parsed,
                    sql,
                    &[Value::Int(1)],
                )
            })
            .unwrap()
            .unwrap(),
        );
        assert_eq!(result.columns[0].name, name);
        assert_eq!(result.columns[0].source.table, alias);
        assert_eq!(result.rows, vec![vec![Value::String("visible".into())]]);
    }
}

#[test]
fn shared_prepared_range_metadata_keeps_current_table_alias() {
    let dir = tempfile::Builder::new()
        .prefix("range-presentation-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE range_names (id INT PRIMARY KEY, c VARCHAR(500))")
        .unwrap();
    engine
        .execute("INSERT INTO range_names VALUES (1,'visible')")
        .unwrap();
    let padding = (20000..22000)
        .map(|id| format!("({id},'padding')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO range_names VALUES {padding}"))
        .unwrap();
    engine.execute("ANALYZE TABLE range_names").unwrap();
    for (sql, alias) in [
        (
            "SELECT c FROM range_names AS Src WHERE id BETWEEN ? AND ?",
            "Src",
        ),
        (
            "SELECT c FROM range_names AS src WHERE id BETWEEN ? AND ?",
            "src",
        ),
        (
            "SELECT c FROM range_names WHERE id BETWEEN ? AND ?",
            "range_names",
        ),
    ] {
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
        let mut state = PreparedSelectMemoState::default();
        let response = with_shared_result_metadata(|| {
            engine.execute_prepared_select(
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(1), Value::Int(6)],
            )
        })
        .unwrap()
        .unwrap();
        assert!(
            matches!(response, QueryResponse::SharedResultSet(_)),
            "must exercise the shared range lane: {response:?}"
        );
        let result = owned_result(response);
        assert_eq!(result.columns[0].source.table, alias);
        assert_eq!(result.rows, vec![vec![Value::String("visible".into())]]);
    }
}
