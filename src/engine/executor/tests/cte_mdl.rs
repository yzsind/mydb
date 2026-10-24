use super::*;

#[test]
fn window_cte_full_join_streaming_mdl() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE date_dim(d_date_sk INT, d_date DATE, d_month_seq INT)",
        "CREATE TABLE web_sales(ws_item_sk INT, ws_sold_date_sk INT, ws_sales_price INT)",
        "CREATE TABLE store_sales(ss_item_sk INT, ss_sold_date_sk INT, ss_sales_price INT)",
        "INSERT INTO date_dim VALUES (1,'2000-01-01',1176),(2,'2000-01-02',1176)",
        "INSERT INTO web_sales VALUES (1,1,20),(1,2,30),(2,1,5),(3,1,50)",
        "INSERT INTO store_sales VALUES (1,1,10),(1,2,15),(2,1,10),(4,1,5)",
    ] {
        engine.execute(sql).unwrap();
    }
    let sql = include_str!("../../../../scripts/tpcds/queries/q51.sql");
    let mut session = SessionContext::default();
    session.connection_id = 951;
    for degree in [1, 8, 8] {
        session.sysvars.mydb_parallel_degree = degree;
        let response = engine
            .execute_streaming_with_session_for_test(&mut session, sql)
            .unwrap();
        assert!(matches!(response, QueryResponse::StreamingResultSet(_)));
        let locks = engine.db.metadata_lock_snapshot();
        for table in ["web_sales", "store_sales", "date_dim"] {
            assert!(
                locks.iter().any(|lock| {
                    lock.object_name.as_deref() == Some(table)
                        && lock.source == "sql/streaming-result"
                        && lock.owner_thread_id == Some(951)
                }),
                "missing cursor protection for {table}"
            );
        }
        let QueryResponse::ResultSet(result) = response.materialize().unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0]
                .iter()
                .map(Value::to_display_string)
                .collect::<Vec<_>>(),
            ["1", "2000-01-01", "20", "10", "20", "10"]
        );
        assert_eq!(
            result.rows[1]
                .iter()
                .map(Value::to_display_string)
                .collect::<Vec<_>>(),
            ["1", "2000-01-02", "50", "25", "50", "25"]
        );
        assert!(engine.db.metadata_lock_snapshot().is_empty());
    }
    let response = engine
        .execute_streaming_with_session_for_test(&mut session, sql)
        .unwrap();
    drop(response);
    assert!(engine.db.metadata_lock_snapshot().is_empty());
}

#[test]
fn materialized_cte_stream_holds_mdl_until_close() {
    use crate::storage::{MdlDuration, MdlLockType, MdlObjectKey, MdlRequest};
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(k INT)").unwrap();
    engine.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let mut session = SessionContext::default();
    session.connection_id = 952;
    let sql = "WITH c AS (SELECT k FROM t) SELECT a.k FROM c a JOIN c b ON a.k=b.k";
    let ddl_owner = crate::storage::mdl::allocate_internal_owner().unwrap();
    let request = || {
        vec![MdlRequest {
            key: MdlObjectKey::table("test", "t"),
            lock_type: MdlLockType::Exclusive,
        }]
    };
    for in_transaction in [false, true] {
        if in_transaction {
            engine.execute_with_session(&mut session, "BEGIN").unwrap();
        }
        let response = engine
            .execute_streaming_with_session_for_test(&mut session, sql)
            .unwrap();
        assert!(matches!(response, QueryResponse::StreamingResultSet(_)));
        if in_transaction {
            engine.execute_with_session(&mut session, "COMMIT").unwrap();
        }
        assert!(engine
            .db
            .acquire_mdl(
                ddl_owner,
                None,
                MdlDuration::Statement,
                request(),
                "test/ddl"
            )
            .is_err());
        engine.db.release_mdl_owner(ddl_owner);
        drop(response);
        assert!(engine.db.metadata_lock_snapshot().is_empty());
        engine
            .db
            .acquire_mdl(
                ddl_owner,
                None,
                MdlDuration::Statement,
                request(),
                "test/ddl",
            )
            .unwrap();
        engine.db.release_mdl_owner(ddl_owner);
    }
    // Errors after acquisition must also release the complete WITH scope.
    assert!(engine
        .execute_streaming_with_session_for_test(
            &mut session,
            "WITH c AS (SELECT k FROM t) SELECT missing FROM c"
        )
        .is_err());
    assert!(engine.db.metadata_lock_snapshot().is_empty());
}
