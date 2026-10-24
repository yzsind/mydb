use super::*;

fn rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(result) => result.rows,
        QueryResponse::StreamingResultSet(mut result) => {
            let mut rows = Vec::new();
            while let Some(row) = result.cursor.next_row().unwrap() {
                rows.push(row);
            }
            rows
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

fn utc_timestamp_literal_now() -> String {
    chrono::Local::now()
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

fn local_timestamp_literal_from_unix_us(unix_us: u64) -> String {
    chrono::DateTime::from_timestamp_micros(unix_us.min(i64::MAX as u64) as i64)
        .unwrap()
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

#[test]
fn enabling_retention_installs_durable_visibility_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let available_at;
    {
        let engine = open_strict_test_engine(dir.path());
        engine.database().set_undo_retention_ms(0).unwrap();
        engine
            .execute("CREATE TABLE temporal_enable_t (id BIGINT PRIMARY KEY, value VARCHAR(32))")
            .unwrap();
        engine
            .execute("INSERT INTO temporal_enable_t VALUES (1, 'before')")
            .unwrap();
        // Reproduce the production failure mode: the writer slot is gone and
        // visibility must come from the activation baseline, not an in-memory
        // transaction slot left over by this test process.
        engine.database().gc_committed_txn_slots();
        let unavailable = utc_timestamp_literal_now();
        std::thread::sleep(Duration::from_millis(2));

        let active_writer = engine.database().begin_dml_txn().unwrap();
        let err = engine.database().set_undo_retention_ms(60_000).unwrap_err();
        assert_eq!(err.code(), 1205);
        engine
            .database()
            .rollback_txn(active_writer.txn_id)
            .unwrap();
        engine.database().set_undo_retention_ms(60_000).unwrap();
        available_at = engine
            .database()
            .storage_status()
            .temporal_earliest_available_unix_us;
        assert!(available_at > 0);

        let err = engine
            .execute(&format!(
                "SELECT * FROM temporal_enable_t FOR SYSTEM_TIME AS OF '{unavailable}'"
            ))
            .unwrap_err();
        assert_eq!(err.code(), 50040);

        let available = local_timestamp_literal_from_unix_us(available_at);
        assert_eq!(
            rows(
                engine
                    .execute(&format!(
                        "SELECT id, value FROM temporal_enable_t FOR SYSTEM_TIME AS OF '{available}'"
                    ))
                    .unwrap()
            ),
            vec![vec![Value::Int(1), Value::String("before".into())]]
        );
    }

    // The baseline is part of the durable commit ledger. A clean reopen with
    // retention enabled must not move the boundary forward or lose visibility
    // for rows committed before tracking was enabled.
    let reopened = open_strict_test_engine(dir.path());
    assert_eq!(
        reopened
            .database()
            .storage_status()
            .temporal_earliest_available_unix_us,
        available_at
    );
    let available = local_timestamp_literal_from_unix_us(available_at);
    assert_eq!(
        rows(
            reopened
                .execute(&format!(
                    "SELECT id, value FROM temporal_enable_t FOR SYSTEM_TIME AS OF '{available}'"
                ))
                .unwrap()
        ),
        vec![vec![Value::Int(1), Value::String("before".into())]]
    );

    reopened.database().set_undo_retention_ms(0).unwrap();
    drop(reopened);
    std::thread::sleep(Duration::from_millis(2));
    let reopened_after_disabled_gap = open_strict_test_engine(dir.path());
    let new_available_at = reopened_after_disabled_gap
        .database()
        .storage_status()
        .temporal_earliest_available_unix_us;
    assert!(new_available_at > available_at);
    let err = reopened_after_disabled_gap
        .execute(&format!(
            "SELECT * FROM temporal_enable_t FOR SYSTEM_TIME AS OF '{available}'"
        ))
        .unwrap_err();
    assert_eq!(err.code(), 50040);
}

#[test]
fn for_system_time_as_of_reconstructs_committed_heap_version() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE temporal_t (id BIGINT PRIMARY KEY, value VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO temporal_t VALUES (1, 'old')")
        .unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let boundary = utc_timestamp_literal_now();
    std::thread::sleep(Duration::from_millis(2));
    engine
        .execute("UPDATE temporal_t SET value='new' WHERE id=1")
        .unwrap();

    let historical = rows(
        engine
            .execute(&format!(
                "SELECT id, value FROM temporal_t FOR SYSTEM_TIME AS OF '{boundary}'"
            ))
            .unwrap(),
    );
    assert_eq!(
        historical,
        vec![vec![Value::Int(1), Value::String("old".into())]]
    );
    assert_eq!(
        rows(engine.execute("SELECT id, value FROM temporal_t").unwrap()),
        vec![vec![Value::Int(1), Value::String("new".into())]]
    );
}

#[test]
fn for_system_time_rejects_nonconstant_and_expired_targets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE temporal_t (id BIGINT)")
        .unwrap();
    let err = engine
        .execute("SELECT * FROM temporal_t FOR SYSTEM_TIME AS OF id")
        .unwrap_err();
    assert_eq!(err.code(), 50042);

    engine.database().set_undo_retention_ms(0).unwrap();
    let err = engine
        .execute("SELECT * FROM temporal_t FOR SYSTEM_TIME AS OF TIMESTAMP '2000-01-01 00:00:00'")
        .unwrap_err();
    assert_eq!(err.code(), 50040);
}

#[test]
fn for_system_time_self_join_uses_independent_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE temporal_join_t (id BIGINT PRIMARY KEY, value VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO temporal_join_t VALUES (1, 'v1')")
        .unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let first = utc_timestamp_literal_now();
    std::thread::sleep(Duration::from_millis(2));
    engine
        .execute("UPDATE temporal_join_t SET value='v2' WHERE id=1")
        .unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let second = utc_timestamp_literal_now();
    std::thread::sleep(Duration::from_millis(2));
    engine
        .execute("UPDATE temporal_join_t SET value='v3' WHERE id=1")
        .unwrap();

    let result = rows(
        engine
            .execute(&format!(
                "SELECT old.value, newer.value \
                 FROM temporal_join_t FOR SYSTEM_TIME AS OF '{first}' AS old \
                 JOIN temporal_join_t FOR SYSTEM_TIME AS OF '{second}' AS newer ON old.id=newer.id"
            ))
            .unwrap(),
    );
    assert_eq!(
        result,
        vec![vec![Value::String("v1".into()), Value::String("v2".into())]]
    );
}

#[test]
fn for_system_time_rejects_crossing_schema_epoch_and_explain_marks_heap_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE temporal_epoch_t (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    engine
        .execute("INSERT INTO temporal_epoch_t VALUES (1, 10)")
        .unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let before_index = utc_timestamp_literal_now();
    std::thread::sleep(Duration::from_millis(2));
    engine
        .execute("ALTER TABLE temporal_epoch_t ADD INDEX idx_value(value)")
        .unwrap();

    let err = engine
        .execute(&format!(
            "SELECT * FROM temporal_epoch_t FOR SYSTEM_TIME AS OF '{before_index}'"
        ))
        .unwrap_err();
    assert_eq!(err.code(), 50041);

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN SELECT * FROM temporal_epoch_t FOR SYSTEM_TIME AS OF NOW(6)")
        .unwrap()
    else {
        panic!("expected EXPLAIN result");
    };
    let extra = explain.rows[0].last().unwrap().to_display_string();
    assert!(extra.contains("FOR SYSTEM_TIME AS OF"), "{extra}");
    assert!(extra.contains("historical heap fallback"), "{extra}");
}

#[test]
fn for_system_time_prepared_parameter_can_be_rebound() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE temporal_ps_t (id BIGINT PRIMARY KEY, value VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO temporal_ps_t VALUES (1, 'v1')")
        .unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let first = utc_timestamp_literal_now();
    std::thread::sleep(Duration::from_millis(2));
    engine
        .execute("UPDATE temporal_ps_t SET value='v2' WHERE id=1")
        .unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let second = utc_timestamp_literal_now();

    let sql = "SELECT value FROM temporal_ps_t FOR SYSTEM_TIME AS OF ?";
    let mut session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let execute_at = |value: String, session: &mut SessionContext| {
        crate::engine::params::with_params(vec![Value::String(value)], || {
            engine
                .execute_prepared_statement(session, sql, (*parsed).clone())
                .expect("prepared statement dispatch")
        })
        .unwrap()
    };
    assert_eq!(
        rows(execute_at(first, &mut session)),
        vec![vec![Value::String("v1".into())]]
    );
    assert_eq!(
        rows(execute_at(second, &mut session)),
        vec![vec![Value::String("v2".into())]]
    );
}

#[test]
fn for_system_time_ledger_and_undo_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let boundary;
    {
        let engine = open_strict_test_engine(dir.path());
        engine.database().set_undo_retention_ms(60_000).unwrap();
        engine
            .execute("CREATE TABLE temporal_restart_t (id BIGINT PRIMARY KEY, value VARCHAR(32))")
            .unwrap();
        engine
            .execute("INSERT INTO temporal_restart_t VALUES (1, 'before')")
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        boundary = utc_timestamp_literal_now();
        std::thread::sleep(Duration::from_millis(2));
        engine
            .execute("UPDATE temporal_restart_t SET value='after' WHERE id=1")
            .unwrap();
    }

    let reopened = open_strict_test_engine(dir.path());
    reopened.database().set_undo_retention_ms(60_000).unwrap();
    assert_eq!(
        rows(
            reopened
                .execute(&format!(
                    "SELECT value FROM temporal_restart_t FOR SYSTEM_TIME AS OF '{boundary}'"
                ))
                .unwrap()
        ),
        vec![vec![Value::String("before".into())]]
    );
}

#[test]
fn for_system_time_old_key_sidecar_survives_restart_and_stays_indexed() {
    let dir = tempfile::tempdir().unwrap();
    let boundary;
    {
        let engine = open_strict_test_engine(dir.path());
        engine.database().set_undo_retention_ms(60_000).unwrap();
        engine
            .execute(
                "CREATE TABLE temporal_restart_idx_t (\
                 id BIGINT PRIMARY KEY, k BIGINT, value VARCHAR(32), INDEX idx_k(k))",
            )
            .unwrap();
        engine
            .execute("INSERT INTO temporal_restart_idx_t VALUES (1, 10, 'before')")
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        boundary = utc_timestamp_literal_now();
        std::thread::sleep(Duration::from_millis(2));
        engine
            .execute("UPDATE temporal_restart_idx_t SET k=20, value='after' WHERE id=1")
            .unwrap();
        assert!(
            engine
                .database()
                .storage_status()
                .temporal_history_index_bytes
                > 0
        );
    }

    let reopened = open_strict_test_engine(dir.path());
    reopened.database().set_undo_retention_ms(60_000).unwrap();
    assert!(!reopened.database().temporal_history_index_degraded());
    let sql = format!(
        "SELECT id, k, value FROM temporal_restart_idx_t \
         FOR SYSTEM_TIME AS OF '{boundary}' WHERE k=10"
    );
    let QueryResponse::ResultSet(explain) = reopened.execute(&format!("EXPLAIN {sql}")).unwrap()
    else {
        panic!("expected EXPLAIN result");
    };
    let row = &explain.rows[0];
    assert_eq!(row[6].to_display_string(), "idx_k", "{row:?}");
    assert!(
        row.last()
            .unwrap()
            .to_display_string()
            .contains("historical old-key merge"),
        "{row:?}"
    );
    assert_eq!(
        rows(reopened.execute(&sql).unwrap()),
        vec![vec![
            Value::Int(1),
            Value::Int(10),
            Value::String("before".into())
        ]]
    );
}

#[test]
fn corrupt_history_sidecar_never_causes_false_negative() {
    use std::io::{Seek, SeekFrom, Write};

    let dir = tempfile::tempdir().unwrap();
    let boundary;
    {
        let engine = open_strict_test_engine(dir.path());
        engine.database().set_undo_retention_ms(60_000).unwrap();
        engine
            .execute(
                "CREATE TABLE temporal_corrupt_idx_t (\
                 id BIGINT PRIMARY KEY, k BIGINT, INDEX idx_k(k))",
            )
            .unwrap();
        engine
            .execute("INSERT INTO temporal_corrupt_idx_t VALUES (1, 10)")
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        boundary = utc_timestamp_literal_now();
        std::thread::sleep(Duration::from_millis(2));
        engine
            .execute("UPDATE temporal_corrupt_idx_t SET k=20 WHERE id=1")
            .unwrap();
        engine.database().checkpoint().unwrap();
    }

    let sidecar = dir.path().join("system/history-index.v1");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&sidecar)
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(b"CORRUPT!").unwrap();
    file.sync_all().unwrap();
    drop(file);

    let reopened = open_strict_test_engine(dir.path());
    reopened.database().set_undo_retention_ms(60_000).unwrap();
    let sql = format!(
        "SELECT id, k FROM temporal_corrupt_idx_t \
         FOR SYSTEM_TIME AS OF '{boundary}' WHERE k=10"
    );
    assert_eq!(
        rows(reopened.execute(&sql).unwrap()),
        vec![vec![Value::Int(1), Value::Int(10)]]
    );
    // A retained WAL tail may repair the damaged accelerator. Otherwise it
    // must advertise degraded and the planner uses the heap oracle.
    if reopened.database().temporal_history_index_degraded() {
        let QueryResponse::ResultSet(explain) =
            reopened.execute(&format!("EXPLAIN {sql}")).unwrap()
        else {
            panic!("expected EXPLAIN result");
        };
        assert!(explain.rows[0]
            .last()
            .unwrap()
            .to_display_string()
            .contains("historical heap fallback"));
    }
}

#[test]
fn temporal_history_memory_reservation_follows_database_lifetime() {
    let before = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[9];
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_test_engine(dir.path());
        engine
            .execute("CREATE TABLE temporal_mem_t (id BIGINT PRIMARY KEY, k BIGINT)")
            .unwrap();
        engine
            .execute("INSERT INTO temporal_mem_t VALUES (1, 10)")
            .unwrap();
        let during = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[9];
        assert!(during > before, "temporal history was not governor-charged");
    }
    assert_eq!(
        crate::engine::executor::mem_budget::controlled_memory_snapshot().2[9],
        before,
        "TemporalHistory reservation outlived its final Database owner"
    );
}

#[test]
fn for_system_time_selective_old_key_uses_history_index_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE temporal_idx_t (id BIGINT PRIMARY KEY, k BIGINT, value VARCHAR(32), INDEX idx_k(k))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO temporal_idx_t VALUES (1, 10, 'old')")
        .unwrap();
    let before_update_scn = engine.database().visible_scn();
    std::thread::sleep(Duration::from_millis(2));
    let boundary = utc_timestamp_literal_now();
    std::thread::sleep(Duration::from_millis(2));
    engine
        .execute("UPDATE temporal_idx_t SET k=20, value='new' WHERE id=1")
        .unwrap();

    let sql = format!(
        "SELECT id, k, value FROM temporal_idx_t FOR SYSTEM_TIME AS OF '{boundary}' WHERE k=10"
    );
    assert!(engine.database().has_pending_index_repairs());
    let schema = engine.database().table("temporal_idx_t").unwrap();
    let index = schema
        .indexes
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case("idx_k"))
        .unwrap();
    let direct_ids = engine
        .database()
        .lookup_index_visible(
            &schema,
            index,
            crate::storage::IndexKey::from(vec![crate::storage::KeyPart::Int(10)]),
            crate::storage::TxnView::historical(before_update_scn),
        )
        .unwrap();
    assert_eq!(direct_ids.len(), 1, "history repair lookup: {direct_ids:?}");
    let bounded_ids = engine
        .database()
        .range_index_bounds_visible(
            &schema,
            index,
            Some(crate::storage::IndexBound {
                key: crate::storage::IndexKey::from(vec![crate::storage::KeyPart::Int(9)]),
                inclusive: true,
            }),
            Some(crate::storage::IndexBound {
                key: crate::storage::IndexKey::from(vec![crate::storage::KeyPart::Int(11)]),
                inclusive: true,
            }),
            crate::storage::TxnView::historical(before_update_scn),
        )
        .unwrap();
    assert_eq!(bounded_ids, direct_ids, "bounded history repair lookup");
    let empty_exclusive_ids = engine
        .database()
        .range_index_bounds_visible(
            &schema,
            index,
            Some(crate::storage::IndexBound {
                key: crate::storage::IndexKey::from(vec![crate::storage::KeyPart::Int(10)]),
                inclusive: false,
            }),
            Some(crate::storage::IndexBound {
                key: crate::storage::IndexKey::from(vec![crate::storage::KeyPart::Int(10)]),
                inclusive: true,
            }),
            crate::storage::TxnView::historical(before_update_scn),
        )
        .unwrap();
    assert!(empty_exclusive_ids.is_empty());
    let QueryResponse::ResultSet(explain) = engine.execute(&format!("EXPLAIN {sql}")).unwrap()
    else {
        panic!("expected EXPLAIN result");
    };
    let row = &explain.rows[0];
    assert_eq!(row[6].to_display_string(), "idx_k", "{row:?}");
    let extra = row.last().unwrap().to_display_string();
    assert!(extra.contains("historical old-key merge"), "{extra}");
    assert_eq!(
        rows(engine.execute(&sql).unwrap()),
        vec![vec![
            Value::Int(1),
            Value::Int(10),
            Value::String("old".into())
        ]]
    );
    assert!(
        engine
            .database()
            .storage_status()
            .temporal_history_index_hits
            > 0,
        "historical index metric was not incremented"
    );
    let diagnostics = rows(
        engine
            .execute(
                "SELECT INDEX_NAME, HISTORY_ENTRIES, DEGRADED \
                 FROM information_schema.mydb_temporal_history \
                 WHERE TABLE_SCHEMA='test' AND TABLE_NAME='temporal_idx_t' AND INDEX_NAME='idx_k'",
            )
            .unwrap(),
    );
    assert_eq!(diagnostics.len(), 1);
    assert!(matches!(diagnostics[0][1], Value::Int(entries) if entries >= 1));
    assert_eq!(diagnostics[0][2], Value::Int(0));
}

#[test]
fn for_system_time_works_in_cte_and_union_and_rejects_view_qualification() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE temporal_shape_t (id BIGINT PRIMARY KEY, value VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO temporal_shape_t VALUES (1, 'v1')")
        .unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let first = utc_timestamp_literal_now();
    std::thread::sleep(Duration::from_millis(2));
    engine
        .execute("UPDATE temporal_shape_t SET value='v2' WHERE id=1")
        .unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let second = utc_timestamp_literal_now();

    let cte = format!(
        "WITH h AS (SELECT value FROM temporal_shape_t FOR SYSTEM_TIME AS OF '{first}') SELECT value FROM h"
    );
    assert_eq!(
        rows(engine.execute(&cte).unwrap()),
        vec![vec![Value::String("v1".into())]]
    );
    let union = format!(
        "SELECT value FROM temporal_shape_t FOR SYSTEM_TIME AS OF '{first}' \
         UNION ALL SELECT value FROM temporal_shape_t FOR SYSTEM_TIME AS OF '{second}'"
    );
    assert_eq!(
        rows(engine.execute(&union).unwrap()),
        vec![
            vec![Value::String("v1".into())],
            vec![Value::String("v2".into())]
        ]
    );

    engine
        .execute("CREATE VIEW temporal_shape_v AS SELECT * FROM temporal_shape_t")
        .unwrap();
    let err = engine
        .execute("SELECT * FROM temporal_shape_v FOR SYSTEM_TIME AS OF NOW(6)")
        .unwrap_err();
    assert_eq!(err.code(), 1235);
}
