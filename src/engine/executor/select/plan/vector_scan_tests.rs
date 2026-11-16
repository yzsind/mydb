use super::*;

#[test]
fn vector_index_projects_live_batches_without_unused_lobs() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        crate::engine::executor::tests::memory_test_storage_config(),
    )
    .unwrap();
    engine.execute("CREATE TABLE vector_projection(id INT PRIMARY KEY,tenant INT,body LONGBLOB,v VECTOR(4) NOT NULL)").unwrap();
    let values = (0..160)
        .map(|id| format!("({id},{},REPEAT('x',8192),'[{},1,0,0]')", id % 2, id + 1))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO vector_projection VALUES {values}"))
        .unwrap();
    engine.execute("CREATE INDEX vx ON vector_projection(v) USING HNSW WITH(metric=l2,m=4,ef_construction=64)").unwrap();
    let db = engine.db_for_tests();
    let txn = db.begin_read_only_txn().unwrap();
    let Statement::Select(select)=crate::parser::parse_statement("SELECT /*+ HNSW_EF(200) */ id FROM vector_projection ORDER BY L2_DISTANCE(v,TO_VECTOR('[0,1,0,0]')),id LIMIT 4").unwrap() else { panic!("select required"); };
    let plan = crate::planner::plan_select(db, &select).unwrap().physical;
    let mut scan = None;
    plan.walk(|node| {
        if matches!(node, PhysicalPlan::VectorIndexScan { .. }) {
            scan = Some(node.clone());
        }
    });
    let scan = scan.expect("native vector index plan");
    let PhysicalPlan::VectorIndexScan {
        table,
        filter,
        projected_columns,
        ..
    } = &scan
    else {
        unreachable!()
    };
    let fallback = ordered_vector_fallback_input(
        table,
        filter,
        projected_columns.as_deref().map(Vec::as_slice),
    );
    let PhysicalPlan::SeqScan {
        projected_columns: fallback_columns,
        ..
    } = &fallback
    else {
        unreachable!()
    };
    assert_eq!(fallback_columns.as_deref(), projected_columns.as_deref());
    let rows = execute_plan_rows(db, txn, &scan).unwrap();
    assert_eq!(rows.len(), 4);
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(
            row.fields
                .iter()
                .map(|field| field.column.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "v"]
        );
        assert_eq!(row.fields[0].value, Value::Int(id as i64));
    }
    let mut historical = txn;
    historical.historical = true;
    let rows = execute_plan_rows(db, historical, &scan).unwrap();
    assert_eq!(rows.len(), 4);
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(
            row.fields
                .iter()
                .map(|field| field.column.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "v"]
        );
        assert_eq!(row.fields[0].value, Value::Int(id as i64));
    }
    // Residuals and selected payloads remain available after pruning.
    let result=engine.execute("SELECT /*+ HNSW_EF(200) */ id,OCTET_LENGTH(body) FROM vector_projection WHERE tenant=1 ORDER BY L2_DISTANCE(v,TO_VECTOR('[0,1,0,0]')),id LIMIT 4").unwrap().materialize().unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("rows required");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(8192)],
            vec![Value::Int(3), Value::Int(8192)],
            vec![Value::Int(5), Value::Int(8192)],
            vec![Value::Int(7), Value::Int(8192)]
        ]
    );
}

#[test]
fn vector_exact_completion_keeps_residuals_ties_and_logical_column_order() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        crate::engine::executor::tests::memory_test_storage_config(),
    )
    .unwrap();
    engine.execute("CREATE TABLE vector_fallback(id INT PRIMARY KEY,tenant INT,body LONGBLOB,v VECTOR(4) NOT NULL)").unwrap();
    let values = (0..160)
        .map(|id| format!("({id},{},REPEAT('x',8192),'[1,0,0,0]')", id % 2))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO vector_fallback VALUES {values}"))
        .unwrap();
    engine.execute("CREATE INDEX vx ON vector_fallback(v) USING HNSW WITH(metric=l2,m=4,ef_construction=64)").unwrap();
    engine
        .execute("ALTER TABLE vector_fallback ADD COLUMN extra INT DEFAULT 7 FIRST")
        .unwrap();
    engine.execute("ANALYZE TABLE vector_fallback").unwrap();
    let db = engine.db_for_tests();
    let mut txn = db.begin_read_only_txn().unwrap();
    txn.historical = true;
    for (selection, expected_fields) in [
        ("id", vec!["id", "tenant", "v"]),
        (
            "id,extra,OCTET_LENGTH(body)",
            vec!["extra", "id", "tenant", "body", "v"],
        ),
    ] {
        let Statement::Select(select) = crate::parser::parse_statement(&format!("SELECT /*+ HNSW_EF(200) */ {selection} FROM vector_fallback WHERE tenant=1 AND id<16 ORDER BY L2_DISTANCE(v,TO_VECTOR('[0,1,0,0]')),id DESC LIMIT 4")).unwrap() else {panic!("select required")};
        let plan = crate::planner::plan_select(db, &select).unwrap().physical;
        let mut scan = None;
        plan.walk(|node| {
            if matches!(node, PhysicalPlan::VectorIndexScan { .. }) {
                scan = Some(node.clone());
            }
        });
        let scan = scan.expect("native vector scan");
        let PhysicalPlan::VectorIndexScan {
            table,
            filter,
            projected_columns,
            ..
        } = &scan
        else {
            unreachable!()
        };
        let input = ordered_vector_fallback_input(
            table,
            filter,
            projected_columns.as_deref().map(Vec::as_slice),
        );
        let PhysicalPlan::SeqScan { estimated_rows, .. } = input else {
            unreachable!()
        };
        assert!(estimated_rows < table.schema.stats.row_count as f64 * 0.15);
        let rows = execute_plan_rows(db, txn, &scan).unwrap();
        assert_eq!(rows.len(), 4);
        for (row, id) in rows.iter().zip([15, 13, 11, 9]) {
            assert_eq!(
                row.fields
                    .iter()
                    .map(|f| f.column.as_str())
                    .collect::<Vec<_>>(),
                expected_fields
            );
            let id_idx = row.schema.resolve_index(&row.fields, None, "id").unwrap();
            assert_eq!(row.fields[id_idx].value, Value::Int(id));
            if selection.contains("body") {
                let body_idx = row.schema.resolve_index(&row.fields, None, "body").unwrap();
                let body_len = match &row.fields[body_idx].value {
                    Value::Bytes(bytes) => bytes.len(),
                    Value::String(text) => text.len(),
                    value => panic!("unexpected LOB value: {value:?}"),
                };
                assert_eq!(body_len, 8192);
            }
        }
    }
}
