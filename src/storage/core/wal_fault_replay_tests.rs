#[test]
fn wal_fault_replica_recovery_defers_incomplete_paged_graph() {
    use crate::engine::StorageQueryEngine;
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        test_storage_config(DurabilityMode::Relaxed, true),
    )
    .unwrap();
    for sql in [
        "CREATE TABLE vec_prefix(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)",
        "CREATE INDEX ann ON vec_prefix(v) USING HNSW WITH(quantization=sq8)",
        "INSERT INTO vec_prefix VALUES(1,TO_VECTOR('[1,2]'))",
    ] {
        engine.execute(sql).unwrap();
    }
    let db = engine.database_for_replication();
    let table = db.table("test.vec_prefix").unwrap();
    let index = table.indexes.iter().find(|i| i.is_vector()).unwrap();
    let graph = db
        .vector_graph_get_or_build_for_partition(&table, index, table.id)
        .unwrap();
    let VectorGraph::NativeHnsw(native) = graph.as_ref() else {
        panic!("native HNSW")
    };
    let file = native.file().unwrap().clone();
    db.set_physical_replica_mode(true).unwrap();
    // A system allocation/counter record may precede the corresponding node
    // image and source VectorIndexComplete. This is a valid replay prefix.
    let saved = file.meta();
    file.buffer_pool.flush_file_pages(&file.file_key).unwrap();
    let mut interrupted = saved.clone();
    interrupted.node_count += 1;
    interrupted.finalized = false;
    interrupted.query_ready = false;
    let path = db.relative_path(file.persistent_path());
    let lsn = db.log.lock().last_lsn();
    db.buffer_pool
        .discard_pages_for_path(file.persistent_path())
        .unwrap();
    redo_page_update(
        dir.path(),
        &path,
        0,
        lsn + 1,
        &encode_vector_meta(&interrupted),
    )
    .unwrap();
    db.vector_indexes.write().clear();
    let bytes = VectorRecoveryMutation::encode_batch(&[VectorRecoveryMutation {
        table_id: table.id,
        storage_id: table.id,
        generation: table.active_generation.max(1),
        index_id: index.index_id,
        row_id: RowId {
            page_id: 1,
            slot_id: 0,
        },
        value: None,
    }])
    .unwrap();
    let records = [
        WalRecord {
            source_time_us: 0,
            lsn: 100,
            prev_txn_lsn: 0,
            txn_id: 100,
            payload: WalPayload::VectorIndexBatch { bytes },
        },
        WalRecord {
            source_time_us: 0,
            lsn: 101,
            prev_txn_lsn: 100,
            txn_id: 100,
            payload: WalPayload::CommitTxn {
                commit_scn: db.visible_scn(),
                commit_unix_us: 0,
                binlog: None,
            },
        },
    ];
    db.replay_persistent_vector_wal(&records).unwrap();
    db.restore_replica_vector_completion(&records).unwrap();
    assert!(db.replica_vector_state.lock().transactions[&100].completing);
    assert!(db.vector_indexes.read().is_empty());
    engine
        .execute("SELECT COUNT(*) FROM vec_prefix")
        .unwrap()
        .materialize()
        .unwrap();
    // Keep the fixture healthy for orderly drop; source recovery still owns
    // validation/repair of its own graph, independently of this replica gate.
    redo_page_update(dir.path(), &path, 0, lsn + 2, &encode_vector_meta(&saved)).unwrap();
    db.replica_vector_state.lock().transactions.remove(&100);
    db.vector_pending_wal.lock().remove(&100);
    // Completion must not eagerly reload a paged graph for each source DML.
    // The next actual reader loads and validates the completed physical file.
    let next = db.log.lock().next_lsn();
    let record = WalRecord {
        lsn: next,
        ..records[0].clone()
    };
    db.log
        .lock()
        .append_reserved(next, record.txn_id, record.payload.clone())
        .unwrap();
    db.log.lock().flush().unwrap();
    db.note_replica_vector_record(&record).unwrap();
    db.begin_replica_vector_commit(record.txn_id).unwrap();
    db.complete_replica_vector_transaction(record.txn_id, db.visible_scn())
        .unwrap();
    assert!(db.vector_indexes.read().is_empty());
    db.vector_graph_get_or_build_for_partition(&table, index, table.id)
        .unwrap()
        .ensure_valid()
        .unwrap();
    assert_eq!(db.vector_indexes.read().len(), 1);
}

#[test]
fn wal_fault_vector_redo_preserves_dirty_companion_btree() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = open_relaxed_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let table = db.table("test.t").unwrap();
    let index = &table.indexes[0];
    let handle = db.cached_index_handle(table.id, &index.name).unwrap();
    let lsn = db.log.lock().last_lsn();
    handle
        .write()
        .apply_replicated_batch(
            0,
            (0..5000)
                .map(|n| IndexBatchEntry {
                    kind: WalIndexOpKind::Insert,
                    key: IndexKey::from(vec![KeyPart::Null]),
                    row_id: RowId {
                        page_id: n / 100 + 1,
                        slot_id: (n % 100) as u16,
                    },
                })
                .collect(),
            lsn,
        )
        .unwrap();
    for ext in ["hvec", "hnsw"] {
        let physical = format!(
            "objects/indexes/{}/1/{}.vector/11.{ext}",
            table.id, index.index_id
        );
        db.invalidate_buffer_pool_path(&physical).unwrap();
        let current = db.cached_index_handle(table.id, &index.name).unwrap();
        assert!(
            Arc::ptr_eq(&handle, &current),
            "vector redo retired the companion B-tree: {physical}"
        );
        assert_eq!(current.read().entries().unwrap().len(), 5000);
    }
}

#[test]
fn wal_fault_lob_allocation_prefix_is_readable_online_and_after_recovery() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "lob_prefix",
        vec![
            checkpoint_test_pk_column(),
            lob_col(
                "body",
                DataType::Text(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let id = db.table("test.lob_prefix").unwrap().id;
    db.checkpoint().unwrap();
    let path = crate::storage::naming::wal_relative_object_path(&db.overflow_path(id));
    let lsn = db.log.lock().next_lsn();
    // Reservation metadata is a system record preceding the chain pages.
    // Another transaction's catalog publication or a crash can occur here.
    let mut image = encode_overflow_meta_page(0, 5, lsn + 1);
    stamp_page_owner(&mut image, &page_owner(&db.overflow_path(id)), 0);
    set_page_lsn_if_formatted(&mut image, lsn);
    let record = WalRecord {
        source_time_us: 0,
        lsn,
        prev_txn_lsn: 0,
        txn_id: 0,
        payload: WalPayload::full_page_image(path.clone(), 0, image),
    };
    db.log
        .lock()
        .append_reserved(lsn, 0, record.payload.clone())
        .unwrap();
    db.set_physical_replica_mode(true).unwrap();
    db.apply_wal_record(&record).unwrap();
    assert_eq!(db.overflow_handle(id).unwrap().read().page_count(), 5);
    assert_eq!(
        fs::metadata(db.overflow_path(id)).unwrap().len(),
        5 * PAGE_SIZE as u64
    );
    db.remove_overflow_cache_entry(id).unwrap();
    File::options()
        .write(true)
        .open(db.overflow_path(id))
        .unwrap()
        .set_len(PAGE_SIZE as u64)
        .unwrap();
    // Equal-LSN redo must repair the missing extent, not skip it because the
    // already-persisted allocation page itself has a valid checksum.
    db.apply_wal_record(&record).unwrap();
    assert_eq!(db.overflow_handle(id).unwrap().read().page_count(), 5);
    // Replay the same durable prefix through the startup REDO helper too.
    db.remove_overflow_cache_entry(id).unwrap();
    fs::remove_file(db.overflow_path(id)).unwrap();
    let image = crate::wal::wal_full_page_image_after(&record.payload).unwrap();
    redo_page_update(dir.path(), &path, 0, lsn, &image).unwrap();
    assert_eq!(db.overflow_handle(id).unwrap().read().page_count(), 5);
    db.remove_overflow_cache_entry(id).unwrap();
    fs::remove_file(db.overflow_path(id)).unwrap();
    let recovery = RecoveryIoScope::enter().unwrap();
    redo_page_update(dir.path(), &path, 0, lsn, &image).unwrap();
    recovery.finish().unwrap();
    assert_eq!(db.overflow_handle(id).unwrap().read().page_count(), 5);
    drop(recovery);
    let mut corrupt = image.clone();
    corrupt[OVFL_META_PAGE_COUNT] ^= 1;
    assert!(redo_lob_allocation_length(&path, 0, &corrupt).is_err());
}
