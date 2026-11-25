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
fn exact_small_primary_range_reuses_plan_across_range_selectivity_tiers() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE memo_ordered_range (id INT PRIMARY KEY, n INT)")
        .unwrap();
    let values = (1..=4096)
        .map(|id| format!("({id},{})", id * 5))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO memo_ordered_range VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE memo_ordered_range").unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT n FROM memo_ordered_range WHERE id>=? ORDER BY id LIMIT 13 OFFSET 1";
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();

    for lower in [7, 4060, 2048, 4090, 33, 5000] {
        let result = with_shared_result_metadata(|| {
            engine.execute_prepared_select(
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(lower)],
            )
        })
        .unwrap()
        .unwrap();
        assert!(
            matches!(&result, QueryResponse::SharedResultSet(_)),
            "integer bind {lower} must keep the compact range path"
        );
        assert_eq!(
            owned_result(result).rows,
            ((lower + 1)..=(lower + 13).min(4096))
                .map(|id| vec![Value::Int(id * 5)])
                .collect::<Vec<_>>(),
        );
        let PreparedSelectMemoState::Ready(memo) = &state else {
            panic!("compiled memo expected");
        };
        assert!(memo
            .compiled
            .compact_range
            .as_ref()
            .is_some_and(|recipe| recipe.stable_row_goal));
    }
    let status = engine.db_for_tests().storage_status();
    assert_eq!(status.prepared_plan_compiles, 1);
    assert_eq!(status.prepared_plan_reuses, 5);

    // An unrelated key cannot bound or order the primary range.
    engine
        .execute("ALTER TABLE memo_ordered_range ADD KEY by_n(n)")
        .unwrap();
    with_shared_result_metadata(|| {
        engine.execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(7)])
    })
    .unwrap()
    .unwrap();
    let PreparedSelectMemoState::Ready(memo) = &state else {
        panic!("compiled memo expected");
    };
    assert!(memo
        .compiled
        .compact_range
        .as_ref()
        .is_some_and(|recipe| recipe.stable_row_goal));
    engine
        .execute("ALTER TABLE memo_ordered_range ADD KEY competing(id,n)")
        .unwrap();
    with_shared_result_metadata(|| {
        engine.execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(7)])
    })
    .unwrap()
    .unwrap();
    let PreparedSelectMemoState::Ready(memo) = &state else {
        panic!("compiled memo expected")
    };
    assert!(!memo
        .compiled
        .compact_range
        .as_ref()
        .is_some_and(|r| r.stable_row_goal));
}

#[test]
fn ordered_range_limit_rebinds_and_reads_snapshot_keys_across_leaf_windows() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ordered_recipe (id INT PRIMARY KEY, label VARCHAR(128), amount DECIMAL(10,2))")
        .unwrap();
    let data = (1..=1200)
        .rev()
        .map(|i| format!("({i},'r{i}',{i}.25)"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO ordered_recipe VALUES {data}"))
        .unwrap();
    engine.execute("ANALYZE TABLE ordered_recipe").unwrap();

    for (comparison, order, start) in [(">=", "ASC", 599), ("<=", "DESC", 602)] {
        let sql = format!(
            "SELECT amount AS a,label FROM ordered_recipe FORCE INDEX(PRIMARY) WHERE id{comparison}? ORDER BY id {order} LIMIT ? OFFSET ?"
        );
        let mut reader = SessionContext::default();
        let parsed = engine.prepare_cached(&mut reader, &sql, 3).unwrap();
        let mut state = PreparedSelectMemoState::default();
        let mut execute = |reader: &mut SessionContext, key, n| {
            with_shared_result_metadata(|| {
                engine.execute_prepared_select(
                    reader,
                    &mut state,
                    &parsed,
                    &sql,
                    &[key, Value::Int(n), Value::Int(1)],
                )
            })
            .unwrap()
            .unwrap()
        };
        let expected = |start, limit| {
            owned_result(
                engine
                    .execute(&format!(
                        "SELECT amount AS a,label FROM ordered_recipe WHERE id{comparison}{start} ORDER BY id {order} LIMIT {limit} OFFSET 1"
                    ))
                    .unwrap(),
            )
            .rows
        };
        for (start, limit) in [(start, 20), (start + 1, 3), (start, 70)] {
            let response = execute(&mut reader, Value::Int(start), limit);
            if limit < 64 {
                assert!(
                    matches!(&response, QueryResponse::SharedResultSet(_)),
                    "{sql}"
                );
            }
            assert_eq!(owned_result(response).rows, expected(start, limit));
        }
        assert!(owned_result(execute(&mut reader, Value::Null, 5))
            .rows
            .is_empty());
        let expected = expected(start, 20);
        let mut writer = SessionContext::default();
        engine.execute_with_session(&mut writer, "BEGIN").unwrap();
        engine
            .execute_with_session(
                &mut writer,
                "UPDATE ordered_recipe SET id=1500,label='moved' WHERE id=600",
            )
            .unwrap();
        assert_eq!(
            owned_result(execute(&mut reader, Value::Int(start), 20)).rows,
            expected
        );
        engine
            .execute_with_session(&mut writer, "ROLLBACK")
            .unwrap();
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
        assert_eq!(result.columns[0].source.org_name, "c");
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

#[test]
fn cached_range_prefix_sort_preserves_unicode_collisions_multikey_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE recipe_prefix (id INT PRIMARY KEY,c VARCHAR(120) COLLATE utf8mb4_0900_ai_ci,d DECIMAL(12,2))").unwrap();
    let values = (1..=2200)
        .map(|id| {
            let text = match id % 8 {
                0 => "NULL",
                1 => "'abcdefgh汉字'",
                2 => "'abcdefgh中文'",
                3 => "'é'",
                4 => "'é'",
                5 => "'ABC'",
                6 => "'abc'",
                _ => "''",
            };
            format!("({id},{text},{id}.25)")
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO recipe_prefix VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE recipe_prefix").unwrap();
    for (projection, order, limit) in [
        ("c,id", "c,id DESC", ""),
        ("d,id,c", "c DESC NULLS FIRST,id", ""),
        ("c,id", "c,id DESC", " LIMIT 71 OFFSET 3"),
    ] {
        let sql = format!("SELECT {projection} FROM recipe_prefix FORCE INDEX(PRIMARY) WHERE id BETWEEN ? AND ? ORDER BY {order}{limit}");
        let mut session = SessionContext::default();
        let parsed = engine.prepare_cached(&mut session, &sql, 2).unwrap();
        let mut state = PreparedSelectMemoState::default();
        for (low, high) in [(1, 128), (50, 250), (4, 8)] {
            let response = with_shared_result_metadata(|| {
                engine.execute_prepared_select(
                    &mut session,
                    &mut state,
                    &parsed,
                    &sql,
                    &[Value::Int(low), Value::Int(high)],
                )
            })
            .unwrap()
            .unwrap();
            if limit.is_empty() {
                assert!(matches!(response, QueryResponse::SharedResultSet(_)));
            }
            let actual = owned_result(response);
            let oracle = format!("SELECT {projection} FROM recipe_prefix IGNORE INDEX(PRIMARY) WHERE id BETWEEN {low} AND {high} ORDER BY {order}{limit}");
            let expected = owned_result(engine.execute(&oracle).unwrap());
            assert_eq!(actual.rows, expected.rows, "{sql}: {low}..{high}");
            assert_eq!(actual.columns, expected.columns);
        }
    }
}

#[test]
fn exact_literal_ranges_reuse_a_zero_bind_template_and_recheck_schema() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE literal_range(id BIGINT PRIMARY KEY,n INT,label VARCHAR(64),INDEX ni(n))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO literal_range VALUES (1,10,'界'),(2,20,'É'),(3,30,'three'),(4,40,'four')",
        )
        .unwrap();
    let mut session = SessionContext::default();
    for sql in [
        "SELECT id,n,label FROM literal_range WHERE id>1 ORDER BY id LIMIT 2",
        "SELECT id,n,label FROM literal_range WHERE id<4 ORDER BY id DESC LIMIT 2",
    ] {
        let parsed = engine.parse_cached_statement(&session, sql).unwrap();
        let (template, params, _) = engine
            .cached_text_statement(&session, sql)
            .expect("safe literal template");
        assert!(params.is_empty());
        assert_eq!(
            template.as_ref(),
            parsed.as_ref(),
            "literal endpoints must remain authoritative"
        );
        let qualified = Arc::new(
            engine
                .prepare_statement_template(&mut session, sql, template.as_ref().clone())
                .unwrap(),
        );
        let mut memo = PreparedSelectMemoState::default();
        for pass in 0..4 {
            if pass == 2 {
                engine
                    .execute("ALTER TABLE literal_range ADD COLUMN extra INT DEFAULT 7")
                    .unwrap_or_else(|e| panic!("{e:?}"));
            }
            let expected = owned_result(engine.execute_with_session(&mut session, sql).unwrap());
            let actual = owned_result(
                with_shared_result_metadata(|| {
                    engine.execute_prepared_select(&mut session, &mut memo, &qualified, sql, &[])
                })
                .unwrap()
                .unwrap(),
            );
            assert_eq!(actual.rows, expected.rows);
            assert_eq!(actual.columns, expected.columns);
        }
        // Character comparison domains must be replanned, even for an exact
        // source template that originally selected an integer index range.
        engine
            .execute("ALTER TABLE literal_range MODIFY id VARCHAR(32) NOT NULL")
            .unwrap();
        let expected = owned_result(engine.execute_with_session(&mut session, sql).unwrap());
        let actual = owned_result(
            with_shared_result_metadata(|| {
                engine.execute_prepared_select(&mut session, &mut memo, &qualified, sql, &[])
            })
            .unwrap()
            .unwrap(),
        );
        assert_eq!(actual.rows, expected.rows);
        assert_eq!(actual.columns, expected.columns);
        engine
            .execute("ALTER TABLE literal_range MODIFY id BIGINT NOT NULL")
            .unwrap();
        engine
            .execute("ALTER TABLE literal_range DROP COLUMN extra")
            .unwrap();
    }
    for sql in [
        "SELECT id,1/0 FROM literal_range WHERE id>1 LIMIT 2",
        "SELECT id FROM literal_range WHERE id>'1' ORDER BY id LIMIT 2",
    ] {
        engine.parse_cached_statement(&session, sql).unwrap();
        assert!(
            engine.cached_text_statement(&session, sql).is_none(),
            "diagnostic-sensitive literal expression: {sql}"
        );
    }
}

#[test]
fn changing_integer_text_ranges_share_templates_and_restore_literals_after_ddl() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE changing_range(id BIGINT PRIMARY KEY,n INT, INDEX ni(n))")
        .unwrap();
    engine
        .execute("INSERT INTO changing_range VALUES(1,10),(2,20),(3,30),(4,40)")
        .unwrap();
    let mut session = SessionContext::default();
    for (op, direction) in [(">", "ASC"), ("<", "DESC"), (">=", "DESC"), ("<=", "ASC")] {
        let warm = format!(
            "SELECT id,n FROM changing_range WHERE id{op}1 ORDER BY id {direction} LIMIT 2"
        );
        engine.parse_cached_statement(&session, &warm).unwrap();
        engine.execute_with_session(&mut session, &warm).unwrap();
        let (template, _, _) = engine.cached_text_statement(&session, &warm).unwrap();
        assert!(integer_range_text::is_template(&template));
        let qualified = Arc::new(
            engine
                .prepare_statement_template(&mut session, &warm, (*template).clone())
                .unwrap(),
        );
        let mut memo = PreparedSelectMemoState::default();
        let mut first_memo = None;
        for anchor in [3, 0, 4, 2, 100] {
            let sql = format!("SELECT id,n FROM changing_range WHERE id{op}{anchor} ORDER BY id {direction} LIMIT 2");
            let (reused, params, _) = engine
                .cached_text_statement(&session, &sql)
                .expect("new endpoint shares template");
            assert_eq!(template.as_ref(), reused.as_ref());
            let actual = crate::engine::params::with_auto_params(params.clone(), || {
                with_shared_result_metadata(|| {
                    engine.execute_prepared_select(
                        &mut session,
                        &mut memo,
                        &qualified,
                        &sql,
                        &params,
                    )
                })
            })
            .unwrap()
            .unwrap();
            let PreparedSelectMemoState::Ready(current) = &memo else {
                panic!("compiled memo expected")
            };
            if let Some(first) = &first_memo {
                assert!(
                    Arc::ptr_eq(first, current),
                    "range selectivity must not recompile a stable row goal"
                );
            } else {
                first_memo = Some(current.clone());
            }
            let expected = engine.execute_with_session(&mut session, &sql).unwrap();
            assert_eq!(owned_result(actual).rows, owned_result(expected).rows);
        }
        // A competing index invalidates the proof. An already borrowed template
        // must decline before executing, then restore the original literals.
        engine
            .execute("ALTER TABLE changing_range ADD KEY competing(id,n)")
            .unwrap();
        let params = vec![Value::Int(2), Value::UInt(2)];
        assert!(crate::engine::params::with_auto_params(params.clone(), || {
            with_shared_result_metadata(|| {
                engine.execute_prepared_select(&mut session, &mut memo, &qualified, &warm, &params)
            })
        })
        .is_none());
        let sql = format!(
            "SELECT id,n FROM changing_range WHERE id{op}2 ORDER BY id {direction} LIMIT 2"
        );
        let actual = engine
            .execute_parsed_with_params(
                &mut session,
                &sql,
                template,
                params,
                Arc::from(sql.as_str()),
            )
            .unwrap();
        assert_eq!(
            owned_result(actual).rows,
            owned_result(engine.execute_with_session(&mut session, &sql).unwrap()).rows
        );
        engine
            .execute("ALTER TABLE changing_range DROP INDEX competing")
            .unwrap();
    }
    for suffix in [
        "WHERE id>1.5 ORDER BY id LIMIT 2",
        "WHERE id>'1' ORDER BY id LIMIT 2",
        "WHERE id>1 ORDER BY id LIMIT 65",
        "WHERE id>1 AND n=20 ORDER BY id LIMIT 2",
        "WHERE id>1 ORDER BY n LIMIT 2",
        "WHERE id>1 ORDER BY id",
        "WHERE id>1 ORDER BY id LIMIT 2 FOR UPDATE",
    ] {
        let sql = format!("SELECT id,n FROM changing_range {suffix}");
        let parsed = engine.parse_cached_statement(&session, &sql).unwrap();
        assert!(
            !integer_range_text::candidate(&engine.db, &session, &parsed, &[]),
            "{sql}"
        );
    }
}

#[test]
fn text_range_vehicle_rechecks_transactions_permissions_and_new_row_policies() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let mut engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE TABLE text_policy(id INT PRIMARY KEY,n INT)",
        "INSERT INTO text_policy VALUES(1,10),(2,20),(3,30),(4,40)",
        "CREATE USER range_reader",
        "GRANT SELECT ON test.text_policy TO range_reader",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let warm = "SELECT id,n FROM text_policy WHERE id>1 ORDER BY id LIMIT 2";
    engine.parse_cached_statement(&root, warm).unwrap();
    engine.execute_with_session(&mut root, warm).unwrap();
    let (template, _, shape) = engine.cached_text_statement(&root, warm).unwrap();
    let qualified = Arc::new(
        engine
            .prepare_statement_template(&mut root, warm, (*template).clone())
            .unwrap(),
    );
    let mut memo = PreparedSelectMemoState::default();
    let run = |engine: &StorageQueryEngine,
               session: &mut SessionContext,
               memo: &mut PreparedSelectMemoState,
               key| {
        let params = vec![Value::Int(key), Value::UInt(2)];
        crate::engine::params::with_auto_params(params.clone(), || {
            with_shared_result_metadata(|| {
                engine.execute_prepared_select(session, memo, &qualified, warm, &params)
            })
        })
    };
    engine.execute_with_session(&mut root, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut root, "INSERT INTO text_policy VALUES(5,50)")
        .unwrap();
    assert_eq!(
        owned_result(run(&engine, &mut root, &mut memo, 3).unwrap().unwrap()).rows,
        vec![
            vec![Value::Int(4), Value::Int(40)],
            vec![Value::Int(5), Value::Int(50)]
        ]
    );
    engine.execute_with_session(&mut root, "ROLLBACK").unwrap();
    assert_eq!(
        owned_result(run(&engine, &mut root, &mut memo, 3).unwrap().unwrap()).rows,
        vec![vec![Value::Int(4), Value::Int(40)]]
    );
    engine.plan_cache_enabled = false;
    assert!(run(&engine, &mut root, &mut memo, 3).is_none());
    let uncached = engine
        .execute_parsed_with_params(
            &mut root,
            warm,
            template.clone(),
            vec![Value::Int(1), Value::UInt(2)],
            shape.clone(),
        )
        .unwrap();
    assert_eq!(
        owned_result(uncached).rows,
        vec![
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)]
        ]
    );
    engine.plan_cache_enabled = true;
    let mut reader = SessionContext {
        user: "range_reader".into(),
        login_user: "range_reader".into(),
        authenticated_as: AccountKey::new("range_reader", "%"),
        connection_id: 8731,
        ..SessionContext::default()
    };
    assert_eq!(
        owned_result(run(&engine, &mut reader, &mut memo, 1).unwrap().unwrap())
            .rows
            .len(),
        2
    );
    engine
        .execute_with_session(
            &mut root,
            "REVOKE SELECT ON test.text_policy FROM range_reader",
        )
        .unwrap();
    assert!(run(&engine, &mut reader, &mut memo, 1).unwrap().is_err());
    engine
        .execute_with_session(
            &mut root,
            "GRANT SELECT ON test.text_policy TO range_reader",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "ALTER TABLE text_policy ENABLE ROW LEVEL SECURITY",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY only_two ON text_policy USING(id=2)",
        )
        .unwrap();
    assert!(run(&engine, &mut reader, &mut memo, 1).is_none());
    let sql = "SELECT id,n FROM text_policy WHERE id>0 ORDER BY id LIMIT 2";
    assert!(engine.cached_text_statement(&reader, sql).is_none());
    let rows = owned_result(
        engine
            .execute_parsed_with_params(
                &mut reader,
                sql,
                template,
                vec![Value::Int(0), Value::UInt(2)],
                shape,
            )
            .unwrap(),
    )
    .rows;
    assert_eq!(rows, vec![vec![Value::Int(2), Value::Int(20)]]);
}

#[test]
fn text_integer_range_templates_preserve_signed_unsigned_edges_and_runtime_windows() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (number, datatype, high) in [
        (0, "TINYINT", "127"),
        (1, "SMALLINT", "32767"),
        (2, "MEDIUMINT", "8388607"),
        (3, "INT", "2147483647"),
        (4, "BIGINT", "9223372036854775807"),
        (5, "BIGINT UNSIGNED", "18446744073709551615"),
    ] {
        let table = format!("text_edges_{number}");
        engine
            .execute(&format!(
                "CREATE TABLE {table}(id {datatype} PRIMARY KEY,n VARCHAR(16))"
            ))
            .unwrap();
        engine
            .execute(&format!(
                "INSERT INTO {table} VALUES(0,'零'),(1,'one'),(2,'two'),({high},'high')"
            ))
            .unwrap();
        for (op, direction) in [(">", "ASC"), (">=", "DESC"), ("<", "DESC"), ("<=", "ASC")] {
            let warm = format!(
                "SELECT id,n FROM {table} WHERE id{op}1 ORDER BY id {direction} LIMIT 2 OFFSET 0"
            );
            engine.parse_cached_statement(&session, &warm).unwrap();
            engine.execute_with_session(&mut session, &warm).unwrap();
            let (template, _, _) = engine.cached_text_statement(&session, &warm).unwrap();
            assert!(integer_range_text::is_template(&template));
            let qualified = Arc::new(
                engine
                    .prepare_statement_template(&mut session, &warm, (*template).clone())
                    .unwrap(),
            );
            let mut memo = PreparedSelectMemoState::default();
            for (anchor, limit, offset) in [
                ("-1", 1, 0),
                ("0", 2, 1),
                ("2", 63, 1),
                ("9223372036854775807", 2, 0),
                ("18446744073709551615", 2, 0),
            ] {
                let sql = format!("SELECT id,n FROM {table} WHERE id{op}{anchor} ORDER BY id {direction} LIMIT {limit} OFFSET {offset}");
                let (reused, params, shape) = engine.cached_text_statement(&session, &sql).unwrap();
                assert_eq!(reused.as_ref(), template.as_ref());
                let fast = crate::engine::params::with_auto_params(params.clone(), || {
                    with_shared_result_metadata(|| {
                        engine.execute_prepared_select(
                            &mut session,
                            &mut memo,
                            &qualified,
                            &sql,
                            &params,
                        )
                    })
                });
                let actual = match fast {
                    Some(result) => result.unwrap(),
                    None => engine
                        .execute_parsed_with_params(&mut session, &sql, reused, params, shape)
                        .unwrap(),
                };
                let expected = engine.execute_with_session(&mut session, &format!("SELECT id,n FROM {table} IGNORE INDEX(PRIMARY) WHERE id{op}{anchor} ORDER BY id {direction} LIMIT {limit} OFFSET {offset}")).unwrap();
                let actual = owned_result(actual);
                let expected = owned_result(expected);
                assert_eq!(actual.rows, expected.rows, "{datatype}: {sql}");
                assert_eq!(actual.columns, expected.columns, "{datatype}: {sql}");
            }
        }
    }
}
