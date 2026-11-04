use super::*;

fn result(engine: &StorageQueryEngine, sql: &str) -> ResultSet {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(result) => result,
        QueryResponse::SharedResultSet(result) => ResultSet {
            columns: result.columns.to_vec(),
            rows: result.rows,
        },
        QueryResponse::StreamingResultSet(mut result) => {
            let columns = result.columns.clone();
            let mut rows = Vec::new();
            while let Some(row) = result.cursor.next_row().unwrap() {
                rows.push(row);
            }
            ResultSet { columns, rows }
        }
        response => panic!("unexpected {response:?}"),
    }
}

#[test]
fn sql_rowid_unsigned_projection_lookup_and_hidden_wildcard() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'one'),(2,'two')")
        .unwrap();
    let rs = result(&engine, "SELECT ROWID, id FROM t ORDER BY id");
    assert_eq!(rs.rows.len(), 2);
    assert_eq!(rs.columns[0].column_type, ColumnType::LongLong);
    assert_ne!(
        rs.columns[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
        0
    );
    for row in rs.rows {
        let Value::UInt(rid) = row[0] else {
            panic!("unsigned ROWID expected")
        };
        let found = result(&engine, &format!("SELECT id FROM t WHERE ROWID={rid}"));
        assert_eq!(found.rows, vec![vec![row[1].clone()]]);
    }
    assert_eq!(result(&engine, "SELECT * FROM t").columns.len(), 2);
    assert!(result(&engine, "SELECT ROWID FROM t WHERE ROWID=0")
        .rows
        .is_empty());
}

#[test]
fn sql_rowid_costed_batch_and_numeric_fallback_match_general_comparison() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2),(3)").unwrap();
    let addresses = result(&engine, "SELECT ROWID FROM t ORDER BY id").rows;
    let Value::UInt(rid) = addresses[0][0] else {
        panic!()
    };
    for operand in [
        rid.to_string(),
        format!("'{rid}'"),
        format!("{rid}.0"),
        format!("CAST({rid} AS DECIMAL(30,0))"),
        "-1".into(),
        "NULL".into(),
    ] {
        for op in ["=", "<=>"] {
            let direct = result(
                &engine,
                &format!("SELECT id FROM t WHERE ROWID {op} {operand} ORDER BY id"),
            );
            let general = result(
                &engine,
                &format!("SELECT id FROM t WHERE (ROWID+0) {op} {operand} ORDER BY id"),
            );
            assert_eq!(direct.rows, general.rows, "{op} {operand}");
        }
    }
    engine.execute("ANALYZE TABLE t").unwrap();
    let list = addresses
        .iter()
        .map(|row| match row[0] {
            Value::UInt(n) => n.to_string(),
            _ => panic!(),
        })
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(
        result(
            &engine,
            &format!("SELECT id FROM t WHERE ROWID IN({list}) ORDER BY id")
        )
        .rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    let explain = result(
        &engine,
        &format!("EXPLAIN SELECT id FROM t WHERE ROWID IN({list})"),
    );
    assert!(!format!("{:?}", explain.rows).contains("RowIdBatchLookup"));
}

#[test]
fn sql_rowid_partition_leaf_identity_survives_reopen() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let expected;
    {
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine.execute("CREATE TABLE p (id INT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
        engine.execute("INSERT INTO p VALUES(1),(11)").unwrap();
        expected = result(&engine, "SELECT ROWID,id FROM p ORDER BY id").rows;
        let Value::UInt(a) = expected[0][0] else {
            panic!()
        };
        let Value::UInt(b) = expected[1][0] else {
            panic!()
        };
        assert_ne!(a, b);
        assert_eq!(a & ((1u64 << 48) - 1), b & ((1u64 << 48) - 1));
        assert_eq!(
            result(&engine, &format!("SELECT id FROM p WHERE ROWID={b}")).rows,
            vec![vec![Value::Int(11)]]
        );
        assert!(result(
            &engine,
            &format!("SELECT id FROM p PARTITION(p0) WHERE ROWID={b}")
        )
        .rows
        .is_empty());
    }
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    assert_eq!(
        result(&engine, "SELECT ROWID,id FROM p ORDER BY id").rows,
        expected
    );
}

#[test]
fn sql_rowid_real_column_takes_precedence() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (rowid VARCHAR(10))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES ('user')").unwrap();
    assert_eq!(
        result(&engine, "SELECT rowid FROM t").rows,
        vec![vec![Value::String("user".into())]]
    );
}

#[test]
fn sql_rowid_dml_returning_and_cross_partition_move() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE p(id INT PRIMARY KEY,v INT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    let inserted = result(
        &engine,
        "INSERT INTO p VALUES(1,7),(11,8) RETURNING ROWID,id",
    );
    assert_eq!(
        inserted
            .rows
            .iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(11)]
    );
    let Value::UInt(old_rid) = inserted.rows[0][0] else {
        panic!()
    };
    let moved = result(
        &engine,
        &format!("UPDATE p SET id=12 WHERE ROWID={old_rid} RETURNING old.ROWID,new.ROWID,new.id"),
    );
    assert_eq!(moved.rows.len(), 1);
    assert_eq!(moved.rows[0][0], Value::UInt(old_rid));
    let Value::UInt(new_rid) = moved.rows[0][1] else {
        panic!()
    };
    assert_ne!(old_rid >> 48, new_rid >> 48);
    assert_eq!(
        result(&engine, &format!("SELECT id FROM p WHERE ROWID={new_rid}")).rows,
        vec![vec![Value::Int(12)]]
    );
    let unchanged = result(
        &engine,
        &format!("UPDATE p SET v=v WHERE ROWID={new_rid} RETURNING ROWID"),
    );
    assert_eq!(unchanged.rows, vec![vec![Value::UInt(new_rid)]]);
    let deleted = result(
        &engine,
        &format!("DELETE FROM p WHERE ROWID={new_rid} RETURNING ROWID,id"),
    );
    assert_eq!(
        deleted.rows,
        vec![vec![Value::UInt(new_rid), Value::Int(12)]]
    );
    assert!(
        result(&engine, &format!("SELECT id FROM p WHERE ROWID={new_rid}"))
            .rows
            .is_empty()
    );
}

#[test]
fn sql_rowid_join_aggregate_subquery_and_empty_metadata() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY iv(v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,20),(2,10),(3,30)")
        .unwrap();
    let ids = result(&engine, "SELECT ROWID,id FROM t ORDER BY id").rows;
    let rows = result(
        &engine,
        "SELECT a.ROWID,b.ROWID FROM t a LEFT JOIN t b ON b.id=a.id+1 ORDER BY a.id",
    )
    .rows;
    assert_eq!(
        rows,
        vec![
            vec![ids[0][0].clone(), ids[1][0].clone()],
            vec![ids[1][0].clone(), ids[2][0].clone()],
            vec![ids[2][0].clone(), Value::Null]
        ]
    );
    assert!(engine
        .execute("SELECT ROWID FROM t a JOIN t b ON a.id=b.id")
        .is_err());
    assert_eq!(
        result(&engine, "SELECT COUNT(DISTINCT ROWID) FROM t").rows,
        vec![vec![Value::Int(3)]]
    );
    assert_eq!(
        result(
            &engine,
            "SELECT id FROM t WHERE ROWID IN (SELECT ROWID FROM t WHERE id=2)"
        )
        .rows,
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(
        result(&engine, "SELECT ROWID,id FROM t FORCE INDEX(iv) ORDER BY v").rows,
        vec![ids[1].clone(), ids[0].clone(), ids[2].clone()]
    );
    let empty = result(&engine, "SELECT ROWID FROM t WHERE 1=0");
    assert!(empty.rows.is_empty());
    assert_eq!(empty.columns[0].column_type, ColumnType::LongLong);
    assert_ne!(
        empty.columns[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
        0
    );
    assert!(engine.execute("UPDATE t SET ROWID=123").is_err());
}

#[test]
fn sql_rowid_prepared_binding_and_transaction_rollback() {
    use crate::engine::api::{PreparedDmlState, QueryEngine};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let ids = result(&engine, "SELECT ROWID,id FROM t ORDER BY id").rows;
    let mut session = SessionContext::default();
    let sql = "SELECT id FROM t WHERE ROWID=?";
    let statement = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let metadata = engine
        .describe_prepared_statement(&mut session, "SELECT ROWID FROM t WHERE id=?")
        .unwrap();
    assert_eq!(metadata[0].column_type, ColumnType::LongLong);
    assert_ne!(
        metadata[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
        0
    );
    for expected in &ids {
        let response = crate::engine::params::with_params(vec![expected[0].clone()], || {
            engine
                .execute_prepared_statement(&mut session, sql, (*statement).clone())
                .unwrap()
                .unwrap()
        })
        .materialize()
        .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!()
        };
        assert_eq!(result.rows, vec![vec![expected[1].clone()]]);
    }
    let sql = "UPDATE t AS u SET v=? WHERE u.ROWID=?";
    let statement = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    for row in &ids {
        let response = engine.execute_prepared_dml(
            &mut session,
            &mut state,
            &statement,
            sql,
            &[Value::Int(99), row[0].clone()],
        );
        match response {
            Some(response) => {
                response.unwrap();
            }
            None => {
                crate::engine::params::with_params(vec![Value::Int(99), row[0].clone()], || {
                    engine
                        .execute_prepared_statement(&mut session, sql, (*statement).clone())
                        .unwrap()
                        .unwrap()
                });
            }
        }
    }
    assert_eq!(
        result(&engine, "SELECT v FROM t ORDER BY id").rows,
        vec![vec![Value::Int(99)], vec![Value::Int(99)]]
    );
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    let Value::UInt(rid) = ids[0][0] else {
        panic!()
    };
    let locked = engine
        .execute_with_session(
            &mut session,
            &format!("SELECT ROWID,v FROM t WHERE ROWID={rid} FOR UPDATE"),
        )
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(locked) = locked else {
        panic!()
    };
    assert_eq!(locked.rows, vec![vec![Value::UInt(rid), Value::Int(99)]]);

    engine
        .execute_with_session(&mut session, &format!("DELETE FROM t WHERE ROWID={rid}"))
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        result(&engine, &format!("SELECT id FROM t WHERE ROWID={rid}")).rows,
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn sql_rowid_partition_ddl_preserves_survivor_tags() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE p(id INT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN(20))").unwrap();
    engine.execute("INSERT INTO p VALUES(1),(11)").unwrap();
    let before = result(&engine, "SELECT ROWID,id FROM p ORDER BY id").rows;
    engine
        .execute("ALTER TABLE p ADD PARTITION(PARTITION p2 VALUES LESS THAN(30))")
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT ROWID,id FROM p ORDER BY id").rows,
        before
    );
    engine.execute("ALTER TABLE p DROP PARTITION p0").unwrap();
    assert_eq!(
        result(&engine, "SELECT ROWID,id FROM p").rows,
        vec![before[1].clone()]
    );
    engine
        .execute("ALTER TABLE p REBUILD PARTITION p1")
        .unwrap();
    let after = result(&engine, "SELECT ROWID,id FROM p").rows;
    let Value::UInt(old) = before[1][0] else {
        panic!()
    };
    let Value::UInt(new) = after[0][0] else {
        panic!()
    };
    assert_eq!(old >> 48, new >> 48);
    engine.execute("CREATE TABLE copy LIKE p").unwrap();
    engine.execute("INSERT INTO copy VALUES(12)").unwrap();
    assert_eq!(result(&engine, "SELECT ROWID,id FROM copy").rows.len(), 1);
}

#[test]
fn sql_rowid_special_indexes_and_vector_modes() {
    use crate::engine::executor::vec::{with_vec_mode, VecMode};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ft(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY f(body))")
        .unwrap();
    engine
        .execute("INSERT INTO ft VALUES(1,'alpha beta'),(2,'gamma delta')")
        .unwrap();
    engine
        .execute("CREATE TABLE vs(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO vs VALUES(1,TO_VECTOR('[1,0]')),(2,TO_VECTOR('[0,1]'))")
        .unwrap();
    engine
        .execute("CREATE INDEX vh ON vs(v) USING HNSW WITH(metric=l2)")
        .unwrap();
    engine
        .execute("CREATE TABLE geo(id INT PRIMARY KEY,g GEOMETRY NOT NULL,SPATIAL INDEX(g))")
        .unwrap();
    engine
        .execute("INSERT INTO geo VALUES(1,POINT(1,1)),(2,POINT(10,10))")
        .unwrap();
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            for (table, predicate) in [
                ("ft", "MATCH(body) AGAINST('+alpha' IN BOOLEAN MODE)"),
                (
                    "geo",
                    "MBRContains(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'),g)",
                ),
            ] {
                let base =
                    result(&engine, &format!("SELECT ROWID,id FROM {table} WHERE id=1")).rows;
                assert_eq!(
                    result(
                        &engine,
                        &format!("SELECT ROWID,id FROM {table} WHERE {predicate}")
                    )
                    .rows,
                    base
                );
            }
            let base = result(&engine, "SELECT ROWID,id FROM vs WHERE id=1").rows;
            assert_eq!(
                result(
                    &engine,
                    "SELECT ROWID,id FROM vs ORDER BY L2_DISTANCE(v,TO_VECTOR('[1,0]')) LIMIT 1"
                )
                .rows,
                base
            );
            assert_eq!(result(&engine,"SELECT ROWID,id FROM vs FORCE INDEX(vh) ORDER BY L2_DISTANCE(v,TO_VECTOR('[1,0]')) LIMIT 1").rows,base);
            let zero = result(&engine, "SELECT ROWID,id FROM ft WHERE id=2").rows;
            assert_eq!(
                result(
                    &engine,
                    "SELECT ROWID,id FROM ft WHERE MATCH(body) AGAINST('+alpha' IN BOOLEAN MODE)=0"
                )
                .rows,
                zero
            );
            let all = result(&engine, "SELECT ROWID,id FROM ft ORDER BY id").rows;
            let scored = result(
                &engine,
                "SELECT ROWID,id,MATCH(body) AGAINST('alpha') FROM ft ORDER BY id",
            )
            .rows;
            assert_eq!(
                scored
                    .iter()
                    .map(|row| row[..2].to_vec())
                    .collect::<Vec<_>>(),
                all
            );
        });
    }
}

#[test]
fn sql_rowid_explain_counts_and_high_unsigned_partition_address() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let partitions = (0..33)
        .map(|i| format!("PARTITION p{i} VALUES LESS THAN({})", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!(
            "CREATE TABLE h(id INT PRIMARY KEY) PARTITION BY RANGE(id) ({partitions})"
        ))
        .unwrap();
    engine.execute("INSERT INTO h VALUES(32)").unwrap();
    let rows = result(&engine, "SELECT ROWID,id FROM h").rows;
    let Value::UInt(rid) = rows[0][0] else {
        panic!()
    };
    assert!(rid > (1u64 << 53));
    assert_eq!(rid >> 48, 33);
    crate::engine::executor::partition_router::reset_partition_visit_counter();
    assert_eq!(
        result(&engine, &format!("SELECT id FROM h WHERE ROWID={rid}")).rows,
        vec![vec![Value::Int(32)]]
    );
    assert_eq!(
        crate::engine::executor::partition_router::partition_visit_count(),
        1
    );
    for sql in [
        format!("EXPLAIN SELECT id FROM h WHERE ROWID={rid}"),
        format!("EXPLAIN FORMAT=TREE SELECT id FROM h WHERE ROWID={rid}"),
        format!("EXPLAIN FORMAT=JSON SELECT id FROM h WHERE ROWID={rid}"),
    ] {
        let text = format!("{:?}", result(&engine, &sql).rows);
        assert!(
            text.contains("ROWID") || text.contains("rowid_lookup"),
            "{text}"
        );
    }
    let sql = format!("EXPLAIN ANALYZE FORMAT=JSON SELECT id FROM h WHERE ROWID IN({rid},{rid},18446744073709551615)");
    let rs = result(&engine, &sql);
    let text = crate::storage::value_to_string(&rs.rows[0][0]);
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    fn counter(value: &serde_json::Value, key: &str) -> Option<u64> {
        match value {
            serde_json::Value::Object(map) => map
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .or_else(|| map.values().find_map(|value| counter(value, key))),
            serde_json::Value::Array(items) => items.iter().find_map(|value| counter(value, key)),
            _ => None,
        }
    }
    assert_eq!(counter(&value, "rowid_candidates"), Some(3), "{text}");
    assert_eq!(
        counter(&value, "rowid_unique_candidates"),
        Some(2),
        "{text}"
    );
    assert_eq!(
        counter(&value, "rowid_duplicates_removed"),
        Some(1),
        "{text}"
    );
    assert_eq!(counter(&value, "rowid_misses"), Some(1), "{text}");
    assert_eq!(counter(&value, "rowid_rows_fetched"), Some(1), "{text}");
}

#[test]
fn sql_rowid_correlated_references_views_and_join_images() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v BIGINT UNSIGNED)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let rows = result(&engine, "SELECT ROWID,id FROM t ORDER BY id").rows;
    assert_eq!(result(&engine,"SELECT a.id FROM t a WHERE EXISTS(SELECT 1 FROM t b WHERE b.id=a.id AND b.ROWID=a.ROWID) ORDER BY a.id").rows,vec![vec![Value::Int(1)],vec![Value::Int(2)]]);
    engine
        .execute("UPDATE t AS u SET v=(SELECT u.ROWID) WHERE id=1")
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT v FROM t WHERE id=1").rows,
        vec![vec![rows[0][0].clone()]]
    );
    assert!(engine
        .execute("UPDATE t SET v=1 WHERE nope.ROWID=65536")
        .is_err());
    engine
        .execute("CREATE VIEW vr AS SELECT ROWID AS rid,id FROM t")
        .unwrap();
    assert_eq!(result(&engine, "SELECT * FROM vr ORDER BY id").rows, rows);
    let updated = result(
        &engine,
        "UPDATE t a JOIN t b ON a.id=b.id SET a.v=a.v WHERE a.id=1 RETURNING old.ROWID,new.ROWID",
    );
    assert_eq!(
        updated.rows,
        vec![vec![rows[0][0].clone(), rows[0][0].clone()]]
    );
    let deleted = result(
        &engine,
        "DELETE a FROM t a JOIN t b ON a.id=b.id WHERE a.id=2 RETURNING ROWID",
    );
    assert_eq!(deleted.rows, vec![vec![rows[1][0].clone()]]);
}

#[test]
fn sql_rowid_permissions_and_temporary_table_scope() {
    use crate::engine::auth::{self, AccountKey};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    engine
        .execute("CREATE USER row_reader IDENTIFIED BY 'p'")
        .unwrap();
    engine
        .execute("GRANT SELECT(id) ON test.t TO row_reader")
        .unwrap();
    let key = AccountKey::new("row_reader", "%");
    let auth = engine.db.auth_catalog();
    let mut user = SessionContext {
        user: "row_reader".into(),
        login_user: "row_reader".into(),
        authenticated_as: key.clone(),
        account_limits: auth.accounts[&auth::account_id(&key)].resource_limits(),
        auth_epoch: auth.epoch,
        ..SessionContext::default()
    };
    engine
        .execute_with_session(&mut user, "SELECT id FROM t")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut user, "SELECT ROWID FROM t")
        .is_err());
    assert!(engine
        .execute_with_session(&mut user, "SELECT id FROM t WHERE ROWID=65536")
        .is_err());
    engine
        .execute("GRANT SELECT ON test.t TO row_reader")
        .unwrap();
    engine
        .execute_with_session(&mut user, "SELECT ROWID FROM t")
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE temp_rid(v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO temp_rid VALUES(7)")
        .unwrap();
    let response = engine
        .execute_with_session(&mut session, "SELECT ROWID,v FROM temp_rid")
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!()
    };
    let Value::UInt(rid) = rs.rows[0][0] else {
        panic!()
    };
    assert_eq!(rid >> 48, 0);
    let response = engine
        .execute_with_session(
            &mut session,
            &format!("SELECT v FROM temp_rid WHERE ROWID={rid}"),
        )
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(7)]]);
    assert!(engine.execute("SELECT ROWID FROM temp_rid").is_err());
}

#[test]
fn sql_rowid_reused_index_plan_keeps_snapshot_order_and_membership() {
    use crate::storage::{IsolationLevel, TxnView};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,k INT,KEY ik(k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,10),(2,20),(3,30)")
        .unwrap();
    let before = result(&engine, "SELECT ROWID,id,k FROM t ORDER BY k").rows;
    let Statement::Select(mut select) = crate::parser::parse_statement(
        "SELECT ROWID,id,k FROM t FORCE INDEX(ik) WHERE k>=10 ORDER BY k",
    )
    .unwrap() else {
        panic!()
    };
    super::super::select::prepare_select_for_planning(engine.db_handle(), &mut select).unwrap();
    let plan = crate::planner::plan_prepared_physical_select(engine.db_handle(), &select).unwrap();
    let snapshot = TxnView::read_only(
        engine.db_handle().visible_scn(),
        IsolationLevel::RepeatableRead,
    );
    let _pin = engine
        .db_handle()
        .register_statement_snapshot(snapshot.snapshot_scn);
    engine.execute("UPDATE t SET k=40 WHERE id=1").unwrap();
    let response = super::super::select::plan::execute_physical_select(
        engine.db_handle(),
        snapshot,
        &select,
        &plan,
    )
    .unwrap()
    .materialize()
    .unwrap();
    let QueryResponse::ResultSet(actual) = response else {
        panic!()
    };
    assert_eq!(actual.rows, before);
    let Value::UInt(rid) = before[0][0] else {
        panic!()
    };
    let Statement::Select(mut point) =
        crate::parser::parse_statement(&format!("SELECT k FROM t WHERE ROWID={rid}")).unwrap()
    else {
        panic!()
    };
    super::super::select::prepare_select_for_planning(engine.db_handle(), &mut point).unwrap();
    let point_plan =
        crate::planner::plan_prepared_physical_select(engine.db_handle(), &point).unwrap();
    let response = super::super::select::plan::execute_physical_select(
        engine.db_handle(),
        snapshot,
        &point,
        &point_plan,
    )
    .unwrap()
    .materialize()
    .unwrap();
    let QueryResponse::ResultSet(actual) = response else {
        panic!()
    };
    assert_eq!(actual.rows, vec![vec![Value::Int(10)]]);
}

#[test]
fn sql_rowid_subpartition_window_and_filtered_native_scan() {
    use crate::engine::executor::vec::{with_vec_mode, VecMode};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE sp(id INT PRIMARY KEY,v INT) PARTITION BY RANGE(id) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN(100),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine
        .execute("INSERT INTO sp SELECT n,n%7 FROM generate_series(1,200) g(n)")
        .unwrap();
    let expected = result(&engine, "SELECT ROWID,id FROM sp WHERE v=3 ORDER BY id").rows;
    assert_eq!(expected.len(), 29);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                result(&engine, "SELECT ROWID,id FROM sp WHERE v=3 ORDER BY id").rows,
                expected
            );
            let rows = result(
                &engine,
                "SELECT ROWID,id,ROW_NUMBER() OVER(ORDER BY id) FROM sp WHERE v=3 ORDER BY id",
            )
            .rows;
            assert_eq!(
                rows.iter().map(|row| row[..2].to_vec()).collect::<Vec<_>>(),
                expected
            );
            assert_eq!(result(&engine,"WITH r AS (SELECT ROWID AS rid,id FROM sp WHERE v=3) SELECT rid,id FROM r ORDER BY id").rows,expected);
            for row in &expected {
                let Value::UInt(rid) = row[0] else { panic!() };
                assert!(rid >> 48 > 0);
                assert_eq!(
                    result(&engine, &format!("SELECT id FROM sp WHERE ROWID={rid}")).rows,
                    vec![vec![row[1].clone()]]
                );
            }
        });
    }
}

#[test]
fn sql_rowid_partitioned_special_indexes_keep_leaf_provenance() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE fp(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY f(body)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO fp VALUES(1,'alpha beta'),(2,'gamma delta')")
        .unwrap();
    engine.execute("CREATE TABLE vp(id INT PRIMARY KEY,v VECTOR(2) NOT NULL) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO vp VALUES(1,TO_VECTOR('[1,0]')),(2,TO_VECTOR('[0,1]'))")
        .unwrap();
    engine
        .execute("CREATE INDEX vh ON vp(v) USING HNSW WITH(metric=l2)")
        .unwrap();
    engine.execute("CREATE TABLE gp(id INT PRIMARY KEY,g GEOMETRY NOT NULL,SPATIAL INDEX(g)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO gp VALUES(1,POINT(1,1)),(2,POINT(10,10))")
        .unwrap();
    for (table, suffix) in [
        ("fp", "WHERE MATCH(body) AGAINST('+alpha' IN BOOLEAN MODE)"),
        (
            "gp",
            "WHERE MBRContains(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'),g)",
        ),
        (
            "vp",
            "FORCE INDEX(vh) ORDER BY L2_DISTANCE(v,TO_VECTOR('[1,0]')) LIMIT 1",
        ),
    ] {
        let expected = result(&engine, &format!("SELECT ROWID,id FROM {table} WHERE id=1")).rows;
        assert_eq!(
            result(&engine, &format!("SELECT ROWID,id FROM {table} {suffix}")).rows,
            expected,
            "{table}"
        );
    }
}

#[test]
fn sql_rowid_missing_returning_images_are_nullable() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let inserted = result(
        &engine,
        "INSERT INTO t VALUES(1) RETURNING old.ROWID,old.ROWID IS NULL,new.ROWID IS NOT NULL",
    );
    assert_eq!(inserted.rows[0][0], Value::Null);
    assert_eq!(inserted.rows[0][1..], [Value::Int(1), Value::Int(1)]);
    assert_eq!(
        inserted.columns[0].source.flags & crate::mysql::protocol::NOT_NULL_FLAG,
        0
    );
    assert_ne!(
        inserted.columns[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
        0
    );
    let deleted = result(
        &engine,
        "DELETE FROM t RETURNING new.ROWID,new.ROWID IS NULL,old.ROWID IS NOT NULL",
    );
    assert_eq!(deleted.rows[0][0], Value::Null);
    assert_eq!(deleted.rows[0][1..], [Value::Int(1), Value::Int(1)]);
}

#[test]
fn sql_rowid_prepared_partition_dml_legacy_dispatch() {
    use crate::engine::api::{PreparedDmlState, QueryEngine};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO p VALUES(1),(2)").unwrap();
    let addresses = result(&engine, "SELECT ROWID,id FROM p ORDER BY id").rows;
    let mut session = SessionContext::default();
    for sql in [
        "UPDATE p SET id=id WHERE ROWID=? RETURNING old.ROWID,new.ROWID",
        "DELETE FROM p WHERE ROWID=? RETURNING ROWID",
    ] {
        let statement = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let mut state = PreparedDmlState::default();
        for row in &addresses {
            assert!(engine
                .execute_prepared_dml(&mut session, &mut state, &statement, sql, &[row[0].clone()])
                .is_none());
            let response = crate::engine::params::with_params(vec![row[0].clone()], || {
                engine
                    .execute_prepared_statement(&mut session, sql, (*statement).clone())
                    .unwrap()
                    .unwrap()
            })
            .materialize()
            .unwrap();
            let QueryResponse::ResultSet(response) = response else {
                panic!()
            };
            assert_eq!(
                response.rows,
                vec![vec![
                    row[0].clone();
                    if sql.starts_with("UPDATE") { 2 } else { 1 }
                ]]
            );
        }
    }
}

#[test]
fn sql_rowid_release_audit_locking_and_delete_order() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let rid = result(&engine, "SELECT ROWID FROM t WHERE id=1").rows[0][0].to_display_string();
    for sql in [
        format!("SELECT id FROM t WHERE ROWID={rid} ORDER BY ROWID FOR UPDATE"),
        "SELECT id FROM t WHERE ROWID=(SELECT ROWID FROM t WHERE id=1) FOR UPDATE".into(),
        "SELECT id FROM t WHERE ROWID IN(SELECT ROWID FROM t WHERE id=1) FOR UPDATE".into(),
        "SELECT id FROM t WHERE id>0 AND ROWID>0 ORDER BY id LIMIT 1 FOR UPDATE".into(),
    ] {
        assert_eq!(
            result(&engine, &sql).rows,
            vec![vec![Value::Int(1)]],
            "{sql}"
        );
    }
    engine
        .execute(&format!(
            "DELETE FROM t WHERE ROWID={rid} ORDER BY ROWID LIMIT 1"
        ))
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT id FROM t").rows,
        vec![vec![Value::Int(2)]]
    );
    assert!(engine
        .execute("DELETE FROM t ORDER BY absent LIMIT 0")
        .is_err());
}

#[test]
fn sql_rowid_release_audit_multi_table_and_view_dml() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
        "INSERT INTO t VALUES(1,10),(2,20)",
        "CREATE TABLE n(id INT)",
        "INSERT INTO n VALUES(1),(2)",
        "CREATE VIEW vt AS SELECT ROWID AS rid,id,v FROM t",
    ] {
        engine.execute(sql).unwrap();
    }
    let rid = result(&engine, "SELECT ROWID FROM t WHERE id=1").rows[0][0].to_display_string();
    engine
        .execute(&format!("UPDATE t AS a,n SET a.v=11 WHERE a.ROWID={rid}"))
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT v FROM t WHERE id=1").rows,
        vec![vec![Value::Int(11)]]
    );
    engine
        .execute(&format!("UPDATE vt SET v=12 WHERE rid={rid}"))
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT v FROM t WHERE id=1").rows,
        vec![vec![Value::Int(12)]]
    );
    for sql in ["UPDATE t,n SET t.v=0 WHERE ROWID>0", "UPDATE t,n SET t.ROWID=1"] {
        assert!(engine.execute(sql).is_err(), "{sql}");
    }
    engine
        .execute(&format!("DELETE t FROM t,n WHERE t.ROWID={rid}"))
        .unwrap();
    let rid = result(&engine, "SELECT ROWID FROM t WHERE id=2").rows[0][0].to_display_string();
    engine
        .execute(&format!("DELETE FROM vt WHERE rid={rid}"))
        .unwrap();
    assert!(result(&engine, "SELECT * FROM t").rows.is_empty());
}
