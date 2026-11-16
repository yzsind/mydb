use super::*;

fn result_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(result) => result.rows,
        QueryResponse::SharedResultSet(result) => result.rows,
        QueryResponse::StreamingResultSet(mut result) => {
            let mut rows = Vec::new();
            while let Some(row) = result.cursor.next_row().unwrap() {
                rows.push(row);
            }
            rows
        }
        other => panic!("expected result set, got {other:?}"),
    }
}

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE im_t (id INT PRIMARY KEY, a INT, b INT, ts INT, \
             KEY idx_a(a), KEY idx_b(b))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO im_t VALUES \
             (1,1,9,10),(2,8,2,20),(3,1,2,30),(4,4,4,40),(5,5,5,50)",
        )
        .unwrap();
    // Preserve the optimizer's selectivity/cardinality fixture without
    // repeatedly planning and evaluating an unrelated INSERT SELECT.
    insert_fixture_rows(
        &engine,
        "test.im_t",
        (1..=5000).map(|n| {
            vec![
                Value::Int(n + 100),
                Value::Int(n + 1000),
                Value::Int(n + 2000),
                Value::Int(n),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE im_t").unwrap();
    (dir, engine)
}

#[test]
fn different_index_or_uses_union_and_deduplicates_rowids() {
    let (_dir, engine) = fixture();
    let tree = explain_analyze_tree(&engine, "SELECT id FROM im_t WHERE a=1 OR b=2 ORDER BY id");
    assert!(tree.contains("Index merge union"), "{tree}");
    assert!(tree.contains("idx_a,idx_b"), "{tree}");
    let rows = result_rows(
        engine
            .execute("SELECT id FROM im_t WHERE a=1 OR b=2 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn ranges_use_sort_union_and_and_uses_intersection() {
    let (_dir, engine) = fixture();
    let union = explain_analyze_tree(&engine, "SELECT id FROM im_t WHERE a<2 OR b>6998");
    assert!(union.contains("Index merge sort-union"), "{union}");

    engine
        .execute("CREATE TABLE im_and (id INT PRIMARY KEY, a INT, b INT, KEY ia(a), KEY ib(b))")
        .unwrap();
    engine.execute("INSERT INTO im_and VALUES (1,1,1)").unwrap();
    engine
        .execute("INSERT INTO im_and SELECT n+10,1,n+10000 FROM generate_series(1,100) g(n)")
        .unwrap();
    engine
        .execute("INSERT INTO im_and SELECT n+1000,n+20000,1 FROM generate_series(1,100) g(n)")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.im_and",
        (1..=5000).map(|n| {
            vec![
                Value::Int(n + 2000),
                Value::Int(n + 30000),
                Value::Int(n + 40000),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE im_and").unwrap();
    let intersection = explain_analyze_tree(&engine, "SELECT id FROM im_and WHERE a=1 AND b=1");
    assert!(
        intersection.contains("Index merge intersection"),
        "{intersection}"
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id FROM im_and WHERE a=1 AND b=1")
                .unwrap()
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn master_switch_disables_index_merge() {
    let (_dir, engine) = fixture();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET optimizer_switch='index_merge=off'")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "EXPLAIN ANALYZE FORMAT=TREE SELECT id FROM im_t WHERE a=1 OR b=2",
        )
        .unwrap()
    else {
        panic!("expected explain result")
    };
    let tree = result.rows[0][0].to_display_string();
    assert!(!tree.contains("Index merge"), "{tree}");

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "EXPLAIN FORMAT=TREE SELECT /*+ INDEX_MERGE(im_t idx_a idx_b) */ id \
             FROM im_t WHERE a=1 OR b=2",
        )
        .unwrap()
    else {
        panic!("expected explain result")
    };
    let forced = result.rows[0][0].to_display_string();
    assert!(forced.contains("Index merge union"), "{forced}");

    let no_merge = explain_analyze_tree(
        &engine,
        "SELECT /*+ NO_INDEX_MERGE(im_t) */ id FROM im_t WHERE a=1 OR b=2",
    );
    assert!(!no_merge.contains("Index merge"), "{no_merge}");
}

#[test]
fn update_and_delete_cross_index_or_match_each_row_once() {
    let (_dir, engine) = fixture();
    let explain = result_rows(
        engine
            .execute("EXPLAIN UPDATE im_t SET ts=ts+1 WHERE a=1 OR b=2")
            .unwrap(),
    );
    assert!(
        explain
            .iter()
            .flatten()
            .any(|value| value.to_display_string() == "index_merge"),
        "{explain:?}"
    );
    let update = engine
        .execute("UPDATE im_t SET ts=ts+1 WHERE a=1 OR b=2")
        .unwrap();
    assert!(
        format!("{update:?}").contains("affected_rows: 3"),
        "{update:?}"
    );
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id,ts FROM im_t WHERE id IN (1,2,3) ORDER BY id")
                .unwrap()
        ),
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(21)],
            vec![Value::Int(3), Value::Int(31)],
        ]
    );
    let delete = engine.execute("DELETE FROM im_t WHERE a=1 OR b=2").unwrap();
    assert!(
        format!("{delete:?}").contains("affected_rows: 3"),
        "{delete:?}"
    );
}

#[test]
fn partitioned_index_merge_keeps_rowid_origin() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE im_p (id INT PRIMARY KEY, a INT, b INT, \
             KEY ia(a), KEY ib(b)) PARTITION BY HASH(id) PARTITIONS 2",
        )
        .unwrap();
    engine
        .execute("INSERT INTO im_p VALUES (1,1,0),(2,0,2),(3,1,2),(4,9,9)")
        .unwrap();
    let tree = explain_analyze_tree(
        &engine,
        "SELECT /*+ INDEX_MERGE(im_p ia ib) */ id FROM im_p \
         WHERE a=1 OR b=2 ORDER BY id",
    );
    assert!(tree.contains("Index merge union"), "{tree}");
    assert_eq!(
        result_rows(
            engine
                .execute(
                    "SELECT /*+ INDEX_MERGE(im_p ia ib) */ id FROM im_p \
                     WHERE a=1 OR b=2 ORDER BY id",
                )
                .unwrap(),
        ),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn union_of_intersections_is_planned_and_correct() {
    let (_dir, engine) = fixture();
    let sql = "SELECT /*+ INDEX_MERGE(im_t idx_a idx_b) */ id FROM im_t \
               WHERE (a=1 AND b=2) OR (a=4 AND b=4) ORDER BY id";
    let tree = explain_analyze_tree(&engine, sql);
    assert!(tree.contains("Index merge union"), "{tree}");
    assert!(tree.contains("idx_a,idx_b,idx_a,idx_b"), "{tree}");
    assert_eq!(
        result_rows(engine.execute(sql).unwrap()),
        vec![vec![Value::Int(3)], vec![Value::Int(4)]]
    );
}

#[test]
fn unsafe_or_branch_falls_back_and_same_column_or_stays_single_index() {
    let (_dir, engine) = fixture();
    let unsafe_tree = explain_analyze_tree(&engine, "SELECT id FROM im_t WHERE a=1 OR ts=40");
    assert!(!unsafe_tree.contains("Index merge"), "{unsafe_tree}");
    assert_eq!(
        result_rows(
            engine
                .execute("SELECT id FROM im_t WHERE a=1 OR ts=40 ORDER BY id")
                .unwrap()
        ),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
            vec![Value::Int(140)],
        ]
    );

    let same = explain_analyze_tree(&engine, "SELECT id FROM im_t WHERE a=1 OR a=4");
    assert!(!same.contains("Index merge"), "{same}");
    assert!(same.contains("Index multi lookup"), "{same}");
}
