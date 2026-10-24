use super::*;
use crate::engine::executor::partition_router;
use crate::storage::{IsolationLevel, PartitionPruningPlan};

#[test]
fn unordered_exchange_over_apply_does_not_evaluate_future_rows() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        crate::engine::executor::tests::memory_test_storage_config(),
    )
    .unwrap();
    engine
        .execute("CREATE TABLE apply_stream (id INT)")
        .unwrap();
    engine
        .execute("INSERT INTO apply_stream VALUES (0),(1),(2),(3)")
        .unwrap();
    let db = engine.db_for_tests();
    let scan = PhysicalPlan::ParallelSeqScan {
        table: table(db, "apply_stream"),
        filter: None,
        estimated_rows: 4.0,
        access_rows: 4.0,
        cost: crate::engine::cbo::Cost::scalar(1.0),
        degree: 4,
        possible_keys: Vec::new(),
        hint_note: None,
        runtime_filters: Vec::new(),
        projected_columns: None,
        output_columns_in_projection: None,
    };
    let plan = PhysicalPlan::Exchange {
        input: Box::new(PhysicalPlan::Apply {
            outer: Box::new(scan),
            predicate: crate::parser::parse_expr_fragment(
                "id < 3 OR JSON_EXTRACT(CONCAT('invalid-', id), '$') IS NULL",
            )
            .unwrap(),
            estimated_rows: 3.0,
            cost: crate::engine::cbo::Cost::scalar(1.0),
        }),
        degree: 4,
        output_order: crate::planner::ExchangeOrder::Unordered,
        merge_keys: Vec::new(),
        cost: crate::engine::cbo::Cost::scalar(1.0),
        estimated_rows: 3.0,
    };
    let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
    let mut stream = execute_plan_stream(db, txn, &plan).unwrap();
    for id in 0..3 {
        let row = stream.next().unwrap().unwrap();
        assert_eq!(row.fields[0].value, Value::Int(id));
    }
    assert_eq!(stream.next().unwrap_err().code(), 3141);
}

#[test]
fn in_subquery_ids_deduplicate_collated_prefix_probes_and_own_capacity_charge() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        crate::engine::executor::tests::memory_test_storage_config(),
    )
    .unwrap();
    engine
        .execute("CREATE TABLE lookup_values (code VARCHAR(8) COLLATE utf8mb4_0900_ai_ci)")
        .unwrap();
    engine.execute("CREATE TABLE lookup_rows (scope INT, code VARCHAR(8) COLLATE utf8mb4_0900_ai_ci, payload INT, INDEX probe(scope,code,payload))").unwrap();
    engine
        .execute("INSERT INTO lookup_values VALUES ('a'),('A'),('b'),('b'),(NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO lookup_rows VALUES (1,'a',3),(1,'A',1),(1,'b',2),(2,'a',9),(1,'c',8)")
        .unwrap();
    let db = engine.db_for_tests();
    let schema = db.table("lookup_rows").unwrap();
    let index = schema
        .indexes
        .iter()
        .find(|index| index.name == "probe")
        .unwrap();
    let crate::parser::Statement::Select(subquery) =
        crate::parser::parse_statement("SELECT code FROM lookup_values").unwrap()
    else {
        panic!("expected SELECT")
    };
    let plan = crate::planner::plan_select(db, &subquery).unwrap();
    let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::new(64));
    crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
        let (ids, charge) = execute_index_in_lookup_ids(
            db,
            TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted),
            &schema,
            index,
            &IndexKey::from(vec![KeyPart::Int(1)]),
            "code",
            &subquery,
            &plan.physical,
        )
        .unwrap();
        assert_eq!(
            ids.len(),
            3,
            "IN membership must not multiply collated matches"
        );
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(budget.used(), ids.capacity() * std::mem::size_of::<RowId>());
        drop(ids);
        drop(charge);
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn residual_subquery_inlj_result_preflight_matches_bounded_kernel() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        crate::engine::executor::tests::memory_test_storage_config(),
    )
    .unwrap();
    engine.execute("CREATE TABLE route_outer (id INT)").unwrap();
    engine
        .execute("CREATE TABLE route_inner (id INT PRIMARY KEY, payload INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE route_members (k INT)")
        .unwrap();
    engine
        .execute("INSERT INTO route_outer VALUES (1),(2),(3)")
        .unwrap();
    engine
        .execute("INSERT INTO route_inner VALUES (1,10),(2,20)")
        .unwrap();
    engine
        .execute("INSERT INTO route_members VALUES (20),(NULL)")
        .unwrap();
    let db = engine.db_for_tests();
    let crate::parser::Statement::Select(left) =
        crate::parser::parse_statement("SELECT id FROM route_outer").unwrap()
    else {
        panic!("expected SELECT")
    };
    let left = crate::planner::plan_select(db, &left).unwrap().physical;
    let right_table = table(db, "route_inner");
    let right_index = right_table
        .schema
        .indexes
        .iter()
        .find(|i| i.primary)
        .unwrap()
        .clone();
    let mut plan = PhysicalPlan::IndexNestedLoopJoin {
        left: Box::new(left), right_table, right_index,
        join_type: crate::parser::JoinType::Inner,
        condition: Some(crate::parser::parse_expr_fragment(
            "route_inner.id=route_outer.id AND route_inner.payload IN (SELECT k FROM route_members)",
        ).unwrap()),
        eq_prefix_len: 1, estimated_rows: 1.0,
        cost: crate::engine::cbo::Cost::scalar(1.0),
        right_covering: false, right_projected_columns: None, right_runtime_filters: Vec::new(),
    };
    let projection = [crate::parser::SelectItem::Wildcard];
    assert!(inlj_chunk_stream_applicable(&plan));
    assert!(super::super::vec_bridge::should_route_result_to_batch(
        &plan,
        &projection
    ));
    let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
    let mut input = super::super::vec_bridge::try_native_vec_input_stream(db, txn, &plan)
        .unwrap()
        .unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = input.stream.next_batch().unwrap() {
        rows.extend(batch.visible_indices().map(|row| batch.row(row)));
    }
    assert_eq!(
        rows,
        vec![vec![Value::Int(2), Value::Int(2), Value::Int(20)]]
    );
    drop(input);
    if let PhysicalPlan::IndexNestedLoopJoin { condition, .. } = &mut plan {
        *condition = Some(
            crate::parser::parse_expr_fragment("route_inner.id=(SELECT MAX(k) FROM route_members)")
                .unwrap(),
        );
    }
    assert!(!inlj_chunk_stream_applicable(&plan));
    assert!(!super::super::vec_bridge::should_route_result_to_batch(
        &plan,
        &projection
    ));
}

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::Builder::new()
        .prefix("scan-routing-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        crate::engine::executor::tests::memory_test_storage_config(),
    )
    .unwrap();
    for (name, partitioned) in [("route_plain", false), ("route_parts", true)] {
        let partition = if partitioned {
            "PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (1000), PARTITION p1 VALUES LESS THAN MAXVALUE)"
        } else {
            ""
        };
        engine.execute(&format!("CREATE TABLE {name} (id INT, txt VARCHAR(20), amount DECIMAL(12,2), day DATE, payload VARBINARY(300)) ZONE_MAP=ON ZONE_SIZE=64K {partition}")).unwrap();
        for start in [0, 1000] {
            engine.execute(&format!("INSERT INTO {name} SELECT n, IF(n % 7 = 0, NULL, CONCAT('s', n % 11)), n * 0.01, DATE '2026-01-02', REPEAT('x',256) FROM generate_series({start}, {}) gs(n)",start+511)).unwrap();
        }
        let schema = engine.db_for_tests().table(name).unwrap();
        engine
            .db_for_tests()
            .rebuild_zone_map_for_table(&schema)
            .unwrap();
    }
    (dir, engine)
}

fn table(db: &Arc<Database>, name: &str) -> PlanTable {
    let schema = db.table(name).unwrap();
    PlanTable::new(schema.id, schema, None)
}

fn values(rows: Vec<PlanRow>) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| row.fields.into_iter().map(|field| field.value).collect())
        .collect()
}

fn streamed(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: &Option<Expr>,
    projection: &[usize],
    use_zone_map: bool,
) -> Vec<Vec<Value>> {
    let mut stream = super::super::stream::seq_scan_stream(
        db,
        txn,
        table,
        filter,
        &[],
        Some(projection),
        use_zone_map,
    )
    .unwrap();
    let mut rows = Vec::new();
    while let Some(row) = stream.next().unwrap() {
        rows.push(row);
    }
    values(rows)
}

#[test]
fn projected_and_streamed_scan_routes_match_full_rows_across_types_and_filters() {
    let (_dir, engine) = fixture();
    let db = engine.db_for_tests();
    let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
    let projection = [2, 0, 1, 3];
    for name in ["route_plain", "route_parts"] {
        let mut table = table(db, name);
        table.partition_pruning = PartitionPruningPlan::Program;
        for predicate in [
            None,
            Some("id >= 1400 AND id < 1420"),
            Some("txt LIKE 's1%' OR txt IS NULL"),
            Some("REGEXP_LIKE(txt, '^s[1-3]$')"),
            Some("amount >= 10 AND day = '2026-01-02'"),
        ] {
            let filter = predicate.map(|sql| crate::parser::parse_expr_fragment(sql).unwrap());
            let expected: Vec<Vec<Value>> =
                scan_plan_table(db, txn, &table, &filter, &[], None, false)
                    .unwrap()
                    .iter()
                    .map(|row| {
                        projection
                            .iter()
                            .map(|&index| row.fields[index].value.clone())
                            .collect()
                    })
                    .collect();
            assert!(
                !expected.is_empty(),
                "fixture must exercise {name}: {predicate:?}"
            );
            for use_zone_map in [false, true] {
                let projected = values(
                    scan_plan_table_projected(
                        db,
                        txn,
                        &table,
                        &filter,
                        &[],
                        &projection,
                        use_zone_map,
                    )
                    .unwrap(),
                );
                assert_eq!(
                    projected, expected,
                    "projected {name}: {predicate:?}, zone map={use_zone_map}"
                );
                assert_eq!(
                    streamed(db, txn, &table, &filter, &projection, use_zone_map),
                    expected,
                    "streamed {name}: {predicate:?}, zone map={use_zone_map}"
                );
            }
        }
    }
}

#[test]
fn projected_scan_routes_distinguish_empty_candidates_and_stop_on_consumer_error() {
    let (_dir, engine) = fixture();
    let db = engine.db_for_tests();
    let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
    for name in ["route_plain", "route_parts"] {
        let table = table(db, name);
        let exclude_all = crate::parser::parse_expr_fragment("id < 0").unwrap();
        let empty_ranges = if table.schema.is_partitioned() {
            for storage_id in table.schema.storage_ids() {
                assert!(db
                    .zone_map_candidate_ranges_for_storage_for_plan(
                        &table.schema,
                        storage_id,
                        &exclude_all
                    )
                    .expect("fixture has trusted zone maps")
                    .is_empty());
            }
            None
        } else {
            let ranges = db
                .zone_map_candidate_ranges_for_plan(&table.schema, &exclude_all)
                .expect("fixture has trusted zone maps");
            assert!(ranges.is_empty());
            Some(ranges)
        };
        for outside_latch in [false, true] {
            scan_projected_table(
                db,
                &table.schema,
                txn,
                &[0],
                Some(&exclude_all),
                empty_ranges.as_deref(),
                outside_latch,
                |_| panic!("disproved zones must not fall back to a full scan"),
            )
            .unwrap();

            let mut count = 0;
            scan_projected_table(
                db,
                &table.schema,
                txn,
                &[0],
                None,
                None,
                outside_latch,
                |_| {
                    count += 1;
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(count, 1024);
            let mut calls = 0;
            let err = scan_projected_table(
                db,
                &table.schema,
                txn,
                &[0],
                None,
                None,
                outside_latch,
                |_| {
                    calls += 1;
                    Err(error(
                        crate::error::catalog::ER_QUERY_INTERRUPTED,
                        "consumer stopped",
                    ))
                },
            )
            .unwrap_err();
            assert_eq!(err.code(), 1317);
            assert_eq!(calls, 1, "error must stop later pages and partitions");
            if !table.schema.is_partitioned() {
                scan_projected_table(
                    db,
                    &table.schema,
                    txn,
                    &[0],
                    None,
                    Some(&[]),
                    outside_latch,
                    |_| panic!("empty candidates must not fall back to a full scan"),
                )
                .unwrap();
            }
        }
    }
}

#[test]
fn streamed_scan_keeps_parent_partition_scope_and_explicit_empty_selection() {
    let (_dir, engine) = fixture();
    let db = engine.db_for_tests();
    let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
    let mut table = table(db, "route_parts");
    let before = partition_router::current_pruned_partitions();
    partition_router::with_pruned_partitions(vec![table.schema.storage_ids()[1]], || {
        let rows = streamed(db, txn, &table, &None, &[0], true);
        assert_eq!(rows.len(), 512);
        assert!(rows
            .iter()
            .all(|row| matches!(row[0], Value::Int(id) if id >= 1000)));
        assert_eq!(
            values(scan_plan_table_projected(db, txn, &table, &None, &[], &[0], true).unwrap()),
            rows
        );
    });
    assert_eq!(partition_router::current_pruned_partitions(), before);
    table.explicit_partition_storage_ids = Some(Vec::new());
    assert!(streamed(db, txn, &table, &None, &[0], true).is_empty());
    assert!(scan_plan_table(db, txn, &table, &None, &[], None, true)
        .unwrap()
        .is_empty());
    assert!(
        scan_plan_table_projected(db, txn, &table, &None, &[], &[0], true)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn projected_and_streamed_routes_preserve_old_snapshot_after_later_writes() {
    let (_dir, engine) = fixture();
    let db = engine.db_for_tests();
    let filter = Some(crate::parser::parse_expr_fragment("id >= 0").unwrap());
    // Heap order may change across writes and historical-row reconstruction.
    // Compare the complete snapshot by its unique fixture id, not physical order.
    let ordered = |mut rows: Vec<Vec<Value>>| {
        rows.sort_by_key(|row| match row[0] {
            Value::Int(id) => id,
            _ => panic!("integer id"),
        });
        rows
    };
    for name in ["route_plain", "route_parts"] {
        let table = table(db, name);
        let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::RepeatableRead);
        let _snapshot = db.register_statement_snapshot(txn.snapshot_scn);
        let expected = ordered(
            scan_plan_table(db, txn, &table, &filter, &[], None, false)
                .unwrap()
                .into_iter()
                .map(|row| vec![row.fields[0].value.clone(), row.fields[1].value.clone()])
                .collect(),
        );
        engine
            .execute(&format!("UPDATE {name} SET txt = 'changed' WHERE id = 0"))
            .unwrap();
        engine
            .execute(&format!("DELETE FROM {name} WHERE id = 1"))
            .unwrap();
        engine
            .execute(&format!(
                "INSERT INTO {name} VALUES (1600, 'new', 16, '2026-01-02', 'payload')"
            ))
            .unwrap();
        for zone_map in [false, true] {
            assert_eq!(
                ordered(values(
                    scan_plan_table_projected(db, txn, &table, &filter, &[], &[0, 1], zone_map)
                        .unwrap()
                )),
                expected
            );
            assert_eq!(
                ordered(streamed(db, txn, &table, &filter, &[0, 1], zone_map)),
                expected
            );
        }
        let current = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
        let current = values(
            scan_plan_table_projected(db, current, &table, &filter, &[], &[0, 1], true).unwrap(),
        );
        assert_eq!(current.len(), expected.len());
        assert!(current.iter().any(|row| row[0] == Value::Int(1600)));
        assert!(!current.iter().any(|row| row[0] == Value::Int(1)));
        let updated = current.iter().find(|row| row[0] == Value::Int(0)).unwrap();
        assert_eq!(updated[1].to_display_string(), "changed");
    }
}
