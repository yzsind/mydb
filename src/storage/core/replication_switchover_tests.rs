#[test]
fn demoted_source_reloads_replicated_statistics_and_schema_without_restart() {
    use crate::engine::{api::QueryResponse, StorageQueryEngine};
    let open = |path: &Path| {
        StorageQueryEngine::open_with_storage_config_without_background_workers(
            path,
            test_storage_config(DurabilityMode::Strict, true),
        )
        .unwrap()
    };
    let a_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let b_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let a = open(a_dir.path());
    a.execute("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(30))")
        .unwrap();
    a.execute("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    a.database_for_replication().checkpoint().unwrap();
    crate::util::file::copy_fixture_tree(a_dir.path(), b_dir.path()).unwrap();
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    crate::repl::role::save(b_dir.path(), &role).unwrap();
    let b = open(b_dir.path());

    let master = &b;
    let follower = &a;
    let source = master.database_for_replication();
    let target = follower.database_for_replication();
    target.flush_dirty_data().unwrap();
    source.persist_replicated_undo_for_promotion().unwrap();
    source.set_physical_replica_mode(false).unwrap();
    source.flush_dirty_data().unwrap();
    target.set_physical_replica_mode(true).unwrap();
    let start = target.wal_log().lock().next_lsn();
    let mut replay_cursor = start;
    let mut applier = crate::repl::replica::applier::Applier::with_database(
        a_dir.path(),
        target.wal_log(),
        target.clone(),
        crate::repl::replica::ReplicaProgress {
            received_lsn: start - 1,
            durable_lsn: start - 1,
            applied_lsn: start - 1,
            durable_applied_lsn: start - 1,
            ..Default::default()
        },
    );
    // Exercise repeated descriptor replacements on the demoted local catalog.
    // Complete multi-round role transitions are covered by the live harness.
    for round in 0..3 {
        for statement in [
            "ANALYZE TABLE t".to_string(),
            format!("ALTER TABLE t ADD COLUMN c{round} INT DEFAULT {round}"),
            format!("INSERT INTO t(id,v) VALUES({},'replicated')", round + 4),
            "ANALYZE TABLE t".to_string(),
        ] {
            master.execute(&statement).unwrap();
            let log = source.wal_log();
            let end = log.lock().last_lsn();
            crate::wal::LogManager::sync_durable_to(&log, end).unwrap();
            let mut reader = crate::wal::WalReader::open(&log, replay_cursor).unwrap();
            while let Some(chunk) = reader.next_chunk(end, 1 << 20).unwrap() {
                applier
                    .apply_chunk(&chunk, &role.node_uuid, 1)
                    .unwrap_or_else(|err| {
                        panic!(
                            "round={round} statement={statement} chunk={}..{}: {err:?}",
                            chunk.start_lsn, chunk.end_lsn
                        )
                    });
            }
            replay_cursor = end + 1;
            for query in [
                "SHOW TABLES",
                "SELECT * FROM t ORDER BY id",
                "SHOW CREATE TABLE t",
            ] {
                let rows = |engine: &StorageQueryEngine| {
                    let QueryResponse::ResultSet(rs) =
                        engine.execute(query).unwrap().materialize().unwrap()
                    else {
                        panic!("expected result set: {query}")
                    };
                    rs.rows
                };
                assert_eq!(
                    rows(follower),
                    rows(master),
                    "round={round} {statement}: {query}"
                );
            }
            assert_eq!(target.replica_applied_lsn(), end);
        }
    }
}

#[test]
fn online_index_redo_reclaims_shared_pages_at_the_global_raw_limit() {
    use crate::engine::executor::mem_budget;
    struct RestoreRawLimit(u64);
    impl Drop for RestoreRawLimit {
        fn drop(&mut self) {
            mem_budget::set_controlled_buffer_limit_pages(self.0);
        }
    }
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    db.set_physical_replica_mode(true).unwrap();
    db.buffer_pool.shutdown_flusher();
    let _restore =
        RestoreRawLimit((mem_budget::controlled_buffer_raw_limit_bytes() / PAGE_SIZE) as u64);
    let limit = mem_budget::controlled_buffer_used_bytes() + PAGE_SIZE * 16;
    mem_budget::set_controlled_buffer_limit_pages((limit / PAGE_SIZE) as u64);
    let pressure_path = dir.path().join("pressure.heap");
    std::fs::write(&pressure_path, vec![0u8; PAGE_SIZE * 16]).unwrap();
    let file = Arc::new(BufferPool::heap_file_key(pressure_path, 999_999));
    for page_id in 0..16 {
        drop(
            db.buffer_pool
                .pin_page(PageKey {
                    file: file.clone(),
                    page_id,
                })
                .unwrap(),
        );
    }
    assert_eq!(mem_budget::controlled_buffer_used_bytes(), limit);
    let relative = "objects/indexes/999998/1/1.idx";
    std::fs::create_dir_all(dir.path().join("objects/indexes/999998/1")).unwrap();
    let mut key = Vec::new();
    encode_index_key_into(&IndexKey::from(vec![KeyPart::Int(7)]), &mut key).unwrap();
    let lsn = db.wal_log().lock().last_lsn();
    db.apply_index_batch_record(
        relative,
        lsn,
        0,
        &[WalIndexOp {
            kind: crate::wal::WalIndexOpKind::Insert,
            key,
            row_id: crate::wal::WalRowId {
                page_id: 1,
                slot_id: 0,
            },
        }],
    )
    .unwrap();
    assert!(mem_budget::controlled_buffer_used_bytes() <= limit);
    let index = IndexFile::open_with_durability(
        dir.path().join(relative),
        db.wal_log(),
        DurabilityMode::Strict,
        db.buffer_pool.clone(),
    )
    .unwrap();
    assert_eq!(
        index
            .get(&IndexKey::from(vec![KeyPart::Int(7)]), true)
            .unwrap(),
        vec![RowId::new(1, 0)]
    );
}
