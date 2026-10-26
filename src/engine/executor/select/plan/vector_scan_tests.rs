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
