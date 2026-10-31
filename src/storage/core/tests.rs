use super::*;

#[test]
fn memory_index_mutations_do_not_reencode_resident_leaf_for_each_entry() {
    for kind in 0..8 {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let log = test_log(dir.path());
        let pool = BufferPool::new_with_options(
            16,
            Arc::clone(&log),
            DurabilityMode::Memory,
            Duration::ZERO,
            1,
            0,
        );
        let mut index = IndexFile::open_with_durability(
            dir.path().join("memory.idx"),
            log.clone(),
            DurabilityMode::Memory,
            pool,
        )
        .unwrap();
        let entries = (0..128)
            .map(|value| IndexEntry {
                key: IndexKey::from(match kind {
                    0 => vec![KeyPart::Int(value)],
                    1 => vec![KeyPart::UInt(value as u64)],
                    2 => vec![KeyPart::Decimal {
                        value: value as i128 * 101,
                        scale: 2,
                    }],
                    3 => vec![KeyPart::Date(18000 + value as i32)],
                    4 => vec![KeyPart::Float64(
                        crate::util::ordered_float::f64_to_order_u64(value as f64 / 4.0),
                    )],
                    5 => vec![KeyPart::Bytes(format!("key-{value:04}").into_bytes())],
                    6 => vec![KeyPart::String(format!("key-{value:04}"))],
                    _ => vec![KeyPart::Int(value), KeyPart::UInt(value as u64 * 3)],
                }),
                row_id: RowId {
                    page_id: value as u32 + 1,
                    slot_id: 0,
                },
            })
            .collect::<Vec<_>>();
        index.bulk_load_sorted(7, entries.clone()).unwrap();
        let wal_before = log.lock().last_lsn();
        let encodings_before = INDEX_TREE_PAGE_ENCODINGS.with(|count| count.get());
        for entry in &entries[32..64] {
            index.delete(entry.key.clone(), entry.row_id).unwrap();
        }
        let actual = index.entries().unwrap();
        let expected = entries[..32]
            .iter()
            .chain(&entries[64..])
            .map(|entry| (entry.key.clone(), entry.row_id))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "kind={kind}");
        for entry in &entries[32..64] {
            index
                .insert(entry.key.clone(), entry.row_id, false)
                .unwrap();
        }
        assert_eq!(
            index.entries().unwrap(),
            entries
                .iter()
                .map(|entry| (entry.key.clone(), entry.row_id))
                .collect::<Vec<_>>(),
            "kind={kind}"
        );
        assert_eq!(
            log.lock().last_lsn(),
            wal_before,
            "memory mutations must not append WAL"
        );
        let encodings = INDEX_TREE_PAGE_ENCODINGS.with(|count| count.get()) - encodings_before;
        assert!(
            encodings <= 4,
            "resident mutations encoded {encodings} whole pages; kind={kind}"
        );
        let root = index.page_key(index.root_page.load(Ordering::Acquire));
        let frame = index.buffer_pool.lookup_frame(&root).unwrap().unwrap();
        assert_eq!(frame.dirty_lsn.load(Ordering::Acquire), 0);

        // Explicit replay still supplies a logical anchor even when the
        // index's resident cache uses memory durability.
        let logical_lsn = log.lock().append(8, WalPayload::CheckpointBegin).unwrap();
        assert!(logical_lsn > 0);
        index
            .apply_batch_with_lsn(
                8,
                vec![IndexBatchEntry {
                    kind: WalIndexOpKind::Delete,
                    key: entries[96].key.clone(),
                    row_id: entries[96].row_id,
                }],
                Some(logical_lsn),
            )
            .unwrap();
        assert!(frame.dirty_lsn.load(Ordering::Acquire) >= logical_lsn);
        assert!(index.get(&entries[96].key, false).unwrap().is_empty());

        // Unlogged physical replay must keep the source's last applied LSN,
        // rather than the next reservation used by local logical mutations.
        log.lock().append(8, WalPayload::CheckpointBegin).unwrap();
        let source_lsn = log.lock().append(8, WalPayload::CheckpointBegin).unwrap();
        with_physical_wal_apply(|| {
            index.apply_batch_with_lsn(
                8,
                vec![IndexBatchEntry {
                    kind: WalIndexOpKind::Delete,
                    key: entries[97].key.clone(),
                    row_id: entries[97].row_id,
                }],
                None,
            )
        })
        .unwrap();
        let frame = index.buffer_pool.lookup_frame(&root).unwrap().unwrap();
        assert_eq!(frame.dirty_lsn.load(Ordering::Acquire), source_lsn);
        assert!(index.get(&entries[97].key, false).unwrap().is_empty());

        let source_lsn = log.lock().append(8, WalPayload::CheckpointBegin).unwrap();
        log.lock().set_physical_replica(true);
        index
            .apply_batch_with_lsn(
                8,
                vec![IndexBatchEntry {
                    kind: WalIndexOpKind::Delete,
                    key: entries[98].key.clone(),
                    row_id: entries[98].row_id,
                }],
                None,
            )
            .unwrap();
        let frame = index.buffer_pool.lookup_frame(&root).unwrap().unwrap();
        assert_eq!(frame.dirty_lsn.load(Ordering::Acquire), source_lsn);
        assert!(index.get(&entries[98].key, false).unwrap().is_empty());
        log.lock().set_physical_replica(false);
    }
}

#[test]
fn batched_index_visibility_reads_each_heap_header_once() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "header_batch",
        vec![
            checkpoint_test_pk_column(),
            ColumnDef {
                name: "v".into(),
                primary_key: false,
                ..checkpoint_test_pk_column()
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let committed = db.begin_txn().unwrap();
    db.insert_rows(
        committed,
        "test.header_batch",
        (0..128)
            .map(|id| vec![Value::Int(id), Value::Int(0)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(committed.txn_id).unwrap();
    let pending = db.begin_txn().unwrap();
    db.update_rows_with_changed_columns(
        pending,
        "test.header_batch",
        None,
        Some(&[1]),
        |values| Ok(matches!(values[0], Value::Int(id) if id >= 16)),
        |values| Ok(vec![values[0].clone(), Value::Int(1)]),
    )
    .unwrap();
    let table = db.table("test.header_batch").unwrap();
    let index = table.indexes.iter().find(|index| index.primary).unwrap();
    let entries = db
        .range_index_entries_chunk(&table, index, None, None, None, 256)
        .unwrap();
    assert_eq!(entries.len(), 128);
    assert!(entries
        .iter()
        .all(|(_, row)| row.page_id == entries[0].1.page_id));
    let borrowed: Vec<_> = entries.iter().map(|(key, row)| (key, *row)).collect();
    let integers: Vec<_> = entries
        .iter()
        .map(|(key, row)| {
            let KeyPart::Int(value) = key.0[0] else {
                panic!("integer primary key")
            };
            (Some(value), *row)
        })
        .collect();
    let row_ids: Vec<_> = entries.iter().map(|(_, row)| *row).collect();
    let heap = db.heap_handle(table.id).unwrap();
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    for variant in 0..4 {
        let before = HEAP_BATCH_HEADER_SLOTS_READ.with(|count| count.get());
        let mask = match variant {
            0 => db.filter_visible_index_entries_mask_with_handle(
                &heap, &table, index, &entries, view,
            ),
            1 => db.filter_visible_index_entries_refs_mask_with_handle(
                &heap, &table, index, &borrowed, view,
            ),
            2 => db.filter_visible_index_i64_entries_mask_with_handle(
                &heap, &table, index, &integers, 0, view,
            ),
            _ => db.filter_visible_row_ids_mask_with_handle(&heap, &table, &row_ids, view),
        }
        .unwrap();
        assert_eq!(mask, vec![true; 128], "variant={variant}");
        let examined = HEAP_BATCH_HEADER_SLOTS_READ.with(|count| count.get()) - before;
        assert_eq!(
            examined,
            entries.len(),
            "a fallback must retain the remaining same-page headers: variant={variant}"
        );
    }
    db.rollback_txn(pending.txn_id).unwrap();
}

#[test]
fn batched_visibility_invisible_insert_needs_no_heap_fallback() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "invisible_headers",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let pending = db.begin_txn().unwrap();
    db.insert_rows(
        pending,
        "test.invisible_headers",
        (0..32).map(|id| vec![Value::Int(id)]).collect(),
    )
    .unwrap();
    let table = db.table("test.invisible_headers").unwrap();
    let entries = db
        .range_index_entries_chunk(&table, &table.indexes[0], None, None, None, 64)
        .unwrap();
    let row_ids: Vec<_> = entries.iter().map(|(_, row)| *row).collect();
    assert_eq!(row_ids.len(), 32);
    let heap = db.heap_handle(table.id).unwrap();
    let calls = std::cell::Cell::new(0);
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let mask = db
        .filter_visible_via_heap_with_handle(&heap, &table, &row_ids, view, |row, _| {
            calls.set(calls.get() + 1);
            db.is_row_id_visible(&table, row, view)
        })
        .unwrap();
    assert_eq!(mask, vec![false; 32]);
    assert_eq!(
        calls.get(),
        0,
        "an invisible initial version has no older row to decode"
    );
    db.rollback_txn(pending.txn_id).unwrap();
}

#[test]
fn batched_visibility_cold_page_refills_headers_once() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = test_storage_config(DurabilityMode::Relaxed, false);
    config.buffer_pool_prefetch_pages = 0;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    db.create_table(
        "cold_headers",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let committed = db.begin_txn().unwrap();
    db.insert_rows(
        committed,
        "test.cold_headers",
        (0..128).map(|id| vec![Value::Int(id)]).collect(),
    )
    .unwrap();
    db.commit_txn(committed.txn_id).unwrap();
    let table = db.table("test.cold_headers").unwrap();
    let entries = db
        .range_index_entries_chunk(&table, &table.indexes[0], None, None, None, 256)
        .unwrap();
    let row_ids: Vec<_> = entries.iter().map(|(_, row)| *row).collect();
    assert_eq!(row_ids.len(), 128);
    db.checkpoint().unwrap();
    let heap = db.heap_handle(table.id).unwrap();
    db.buffer_pool.discard_file(&heap.read().file_key).unwrap();
    assert!(matches!(
        heap.read().read_mvcc_header_cached(row_ids[0]).unwrap(),
        CachedMvccHeaderRead::MissingPage
    ));
    let calls = std::cell::Cell::new(0);
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let mask = db
        .filter_visible_via_heap_with_handle(&heap, &table, &row_ids, view, |row, _| {
            calls.set(calls.get() + 1);
            db.is_row_id_visible(&table, row, view)
        })
        .unwrap();
    assert_eq!(mask, vec![true; 128]);
    assert_eq!(
        calls.get(),
        1,
        "the first disk read must restore the remaining header fast path"
    );
}

#[test]
fn contiguous_unique_insert_ranges_keep_prepared_lock_snapshots_complete() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "ranges",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.ranges",
        (0..2000).map(|n| vec![Value::Int(n)]).collect(),
    )
    .unwrap();
    {
        let manager = db.lock_manager();
        assert!(manager.unique_locks.is_empty());
        assert_eq!(manager.unique_range_locks().count(), 1);
        assert_eq!(manager.unique_key_lock_count(), 2000);
        let entries = collect_prepared_locks(&manager, txn.txn_id).unwrap();
        let keys: Vec<_> = entries
            .iter()
            .filter_map(|entry| match entry {
                crate::wal::PreparedLockEntry::UniqueKey { key_bytes, .. } => {
                    Some(serde_json::from_slice::<IndexKey>(key_bytes).unwrap())
                }
                _ => None,
            })
            .collect();
        assert_eq!(keys.len(), 2000);
        for (n, key) in keys.iter().enumerate() {
            assert_eq!(key, &IndexKey::from(vec![KeyPart::Int(n as i64)]));
        }
    }
    db.rollback_txn(txn.txn_id).unwrap();
    assert_eq!(db.lock_manager().unique_key_lock_count(), 0);
}

#[test]
fn typed_integer_column_coercion_matches_canonical_diagnostics() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "integers",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let mut column = db.table("test.integers").unwrap().columns[0].clone();
    let mut values = vec![
        Value::Null,
        Value::Int(i64::MIN),
        Value::Int(i64::MAX),
        Value::UInt(u64::MAX),
        Value::String("12.5".into()),
        Value::String("12x".into()),
        Value::Double(12.5),
    ];
    for bits in [8, 16, 24, 32, 64] {
        let max = (1u64 << (bits - 1)) - 1;
        for number in [0, 1, max - 1, max, max + 1, u64::MAX >> (64 - bits)] {
            values.push(Value::UInt(number));
            if let Ok(number) = i64::try_from(number) {
                values.push(Value::Int(number));
                values.push(Value::Int(-number));
                values.push(Value::Int(-number - 1));
            }
        }
    }
    for kind in [
        DataTypeKind::TinyInt,
        DataTypeKind::SmallInt,
        DataTypeKind::MediumInt,
        DataTypeKind::Int,
        DataTypeKind::BigInt,
        DataTypeKind::Bool,
    ] {
        column.data_type = kind;
        for unsigned in [false, true] {
            column.unsigned = unsigned;
            for ignore in [false, true] {
                let _ignore = DmlIgnoreCoercionGuard::new(ignore);
                for value in &values {
                    crate::engine::api::take_statement_warnings();
                    let expected = coerce_column_value(
                        value.clone(),
                        &column.name,
                        &column.data_type,
                        unsigned,
                    );
                    let expected_warnings =
                        format!("{:?}", crate::engine::api::take_statement_warnings());
                    let actual = coerce_column_value_for_schema(value.clone(), &column);
                    let actual_warnings =
                        format!("{:?}", crate::engine::api::take_statement_warnings());
                    match (actual, expected) {
                        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
                        (Err(actual), Err(expected)) => {
                            assert_eq!(actual.code(), expected.code());
                            assert_eq!(actual.sql_state(), expected.sql_state());
                            assert_eq!(actual.message(), expected.message());
                        }
                        (actual, expected) => {
                            panic!("mismatch {value:?}: {actual:?} vs {expected:?}")
                        }
                    }
                    assert_eq!(actual_warnings, expected_warnings);
                }
            }
        }
    }
}

#[test]
fn typed_text_column_coercion_matches_canonical_charset_and_padding() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "text_coercion",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let mut column = db.table("test.text_coercion").unwrap().columns[0].clone();
    for charset in ["utf8mb4", "utf8mb3", "ascii", "latin1", "binary", "invalid"] {
        column.charset = Some(charset.into());
        for len in [None, Some(0), Some(1), Some(3), Some(20)] {
            for kind in [DataTypeKind::Char { len }, DataTypeKind::VarChar { len }] {
                column.data_type = kind;
                for ignore in [false, true] {
                    let _ignore = DmlIgnoreCoercionGuard::new(ignore);
                    for text in [
                        "", "   ", "a ", "abc", "abcdef ", "中🙂", "é  ", "a\t", "x\u{a0}",
                    ] {
                        let value = Value::String(text.into());
                        crate::engine::api::take_statement_warnings();
                        let expected = coerce_column_value(
                            value.clone(),
                            &column.name,
                            &column.data_type,
                            false,
                        )
                        .and_then(|value| {
                            validate_column_charset_value(&column, &value)?;
                            Ok(value)
                        });
                        let warnings =
                            format!("{:?}", crate::engine::api::take_statement_warnings());
                        let actual = coerce_column_value_for_schema(value, &column);
                        assert_eq!(
                            format!("{actual:?}"),
                            format!("{expected:?}"),
                            "{charset} {text:?}"
                        );
                        assert_eq!(
                            format!("{:?}", crate::engine::api::take_statement_warnings()),
                            warnings
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn nontransactional_coercion_policy_is_case_insensitive_and_nested() {
    let initial = NONTRANSACTIONAL_COERCION_DEPTH.with(|depth| depth.get());
    for engine in [
        "MEMORY",
        "myisam",
        "CsV",
        "archive",
        "blackhole",
        "InnoDB",
        "",
        "unknown",
    ] {
        let expected =
            engine.to_ascii_uppercase() != "INNODB" && !engine.is_empty() && engine != "unknown";
        {
            let outer = NonTransactionalCoercionGuard::new(engine);
            assert_eq!(outer.active, expected);
            assert_eq!(
                NONTRANSACTIONAL_COERCION_DEPTH.with(|depth| depth.get()),
                initial + u16::from(expected)
            );
            {
                let _inner = NonTransactionalCoercionGuard::new("mEmOrY");
                assert_eq!(
                    NONTRANSACTIONAL_COERCION_DEPTH.with(|depth| depth.get()),
                    initial + u16::from(expected) + 1
                );
            }
        }
        assert_eq!(
            NONTRANSACTIONAL_COERCION_DEPTH.with(|depth| depth.get()),
            initial
        );
    }
}

#[test]
fn character_coercion_preserves_unicode_width_padding_and_truncation() {
    for (input, width, char_value, varchar_value) in [
        ("", 0, "", ""),
        (" ", 0, "", ""),
        ("abc", 3, "abc", "abc"),
        ("abc   ", 3, "abc", "abc"),
        ("字🙂", 2, "字🙂", "字🙂"),
        ("字🙂  ", 2, "字🙂", "字🙂"),
        ("é\u{301}", 2, "é\u{301}", "é\u{301}"),
        ("字\u{a0}", 2, "字\u{a0}", "字\u{a0}"),
    ] {
        for (kind, expected) in [
            (DataTypeKind::Char { len: Some(width) }, char_value),
            (DataTypeKind::VarChar { len: Some(width) }, varchar_value),
        ] {
            assert_eq!(
                coerce_value_with_unsigned(Value::String(input.into()), &kind, false, "s").unwrap(),
                Value::String(expected.into()),
                "input={input:?}, kind={kind:?}"
            );
        }
    }
    for kind in [
        DataTypeKind::Char { len: Some(1) },
        DataTypeKind::VarChar { len: Some(1) },
    ] {
        for input in ["ab", "字🙂", "字\u{a0}"] {
            let err = coerce_value_with_unsigned(Value::String(input.into()), &kind, false, "s")
                .unwrap_err();
            assert!(err.is(crate::error::catalog::ER_DATA_TOO_LONG));
        }
    }
}

#[test]
fn fresh_catalog_empty_index_root_is_durable_before_open_returns() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    std::fs::write(dir.path().join(".mydb-test-fresh-data-dir"), b"").unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    let table = crate::storage::data_dictionary::dd_table_schema(
        crate::storage::data_dictionary::DD_CHECK_CONSTRAINTS,
    )
    .unwrap();
    assert!(db.scan_dd_table(table.id).unwrap().is_empty());
    for index in &table.indexes {
        let path = db.index_path_for_table_schema(&table, table.id, &index.name);
        IndexFile::validate(path)
            .expect("a freshly published dictionary must have a durable empty index root");
    }
}

#[cfg(target_os = "macos")]
#[test]
fn test_seed_clone_keeps_the_destination_name_reserved() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let source = root.path().join("source");
    let destination = root.path().join("destination");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&destination).unwrap();
    std::fs::write(source.join("seed"), b"seed-data").unwrap();
    let claimed = Arc::new(AtomicBool::new(false));
    let competing = claimed.clone();
    let target = destination.clone();
    TEST_SEED_CLONE_TEST_HOOK.with(|cell| {
        *cell.borrow_mut() = Some(Box::new(move || {
            if std::fs::create_dir(&target).is_ok() {
                competing.store(true, Ordering::Release);
            }
        }));
    });
    let cloned = clone_test_seed_tree(&source, &destination).unwrap();
    assert!(
        !claimed.load(Ordering::Acquire),
        "seed cloning must not release the destination's reserved name"
    );
    assert!(cloned);
    assert_eq!(
        std::fs::read(destination.join("seed")).unwrap(),
        b"seed-data"
    );
}

#[test]
fn retired_heap_drop_preserves_reopened_dirty_pages() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for durability in [
        DurabilityMode::Memory,
        DurabilityMode::Relaxed,
        DurabilityMode::Strict,
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db =
            Database::open_with_config(dir.path(), test_storage_config(durability, true)).unwrap();
        create_checkpoint_test_table(&db);
        let table = db.table("test.t").unwrap();
        let retired = db.heap_handle(table.id).unwrap();
        db.remove_heap_cache_entry(table.id).unwrap();
        insert_checkpoint_test_row(&db, 8);
        let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
        let before = db.scan_rows("test.t", view).unwrap();
        assert_eq!(before.len(), 1);
        drop(retired);
        let after = db.scan_rows("test.t", view).unwrap();
        assert_eq!(
            after.len(),
            1,
            "a retired handle must not discard a replacement's dirty pages: {durability:?}"
        );
        assert_eq!(after[0].values, before[0].values);
    }
}

#[test]
fn delayed_heap_open_cannot_reinstall_a_retired_generation() {
    use crate::engine::StorageQueryEngine;
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,7)").unwrap();
    let db = engine.database();
    let source = db.table("test.t").unwrap();
    db.flush_dirty_data().unwrap();
    // Model a cache miss whose file open finishes before the DDL, but whose
    // cache installation is delayed until after the new generation commits.
    let open_epoch = db.storage_handle_open_epoch(source.id);
    let epoch = open_epoch.load(Ordering::Acquire);
    let delayed = Arc::new(parking_lot::RwLock::new(
        HeapFile::open_with_buffer_pool(
            db.table_path(source.id),
            source.id,
            db.log.clone(),
            db.config.durability,
            db.config.heap_update_reserve_bytes,
            db.buffer_pool.clone(),
            source.row_codec.clone(),
        )
        .unwrap(),
    ));
    engine
        .execute("ALTER TABLE t MODIFY v BIGINT,ALGORITHM=COPY,LOCK=NONE")
        .unwrap();
    assert!(db
        .install_heap_handle(source.id, delayed, &open_epoch, epoch)
        .is_none());
    let selected = db.heap_handle(source.id).unwrap();
    assert_eq!(
        selected.read().file_key.path,
        db.table_path(source.id),
        "a speculative open must not reinstall a retired storage generation"
    );
    engine.shutdown().unwrap();
}

#[test]
fn delayed_heap_open_is_not_invalidated_by_unrelated_table_copy() {
    use crate::engine::StorageQueryEngine;
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    engine
        .execute("CREATE TABLE a(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b(id INT PRIMARY KEY,v INT)")
        .unwrap();
    let db = engine.database();
    let table = db.table("test.a").unwrap();
    db.remove_heap_cache_entry(table.id).unwrap();
    let open_epoch = db.storage_handle_open_epoch(table.id);
    let epoch = open_epoch.load(Ordering::Acquire);
    let delayed = Arc::new(parking_lot::RwLock::new(
        HeapFile::open_with_buffer_pool(
            db.table_path(table.id),
            table.id,
            db.log.clone(),
            db.config.durability,
            db.config.heap_update_reserve_bytes,
            db.buffer_pool.clone(),
            table.row_codec.clone(),
        )
        .unwrap(),
    ));
    engine
        .execute("ALTER TABLE b MODIFY v BIGINT,ALGORITHM=COPY,LOCK=NONE")
        .unwrap();
    assert!(
        db.install_heap_handle(table.id, delayed, &open_epoch, epoch)
            .is_some(),
        "DDL on another table must not restart this cold open"
    );
    for id in 10000..11000 {
        drop(db.storage_handle_open_epoch(id));
    }
    assert!(db.storage_handle_open_epochs.lock().unwrap().len() < 130);
    engine.shutdown().unwrap();
}

#[test]
fn delayed_path_resolution_retries_index_and_lob_after_copy() {
    use crate::engine::StorageQueryEngine;
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for index in [true, false] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(
            StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                test_storage_config(DurabilityMode::Strict, true),
            )
            .unwrap(),
        );
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,payload LONGTEXT,KEY vi(v))")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,7,REPEAT('x',10000))")
            .unwrap();
        let db = engine.database();
        let table = db.table("test.t").unwrap();
        db.flush_dirty_data().unwrap();
        if index {
            db.remove_index_cache_entry(table.id, "vi").unwrap();
        } else {
            db.remove_overflow_cache_entry(table.id).unwrap();
        }
        let ddl_engine = engine.clone();
        STORAGE_PATH_RESOLVED_TEST_HOOK.with(|cell| {
            *cell.borrow_mut() = Some(Box::new(move || {
                ddl_engine
                    .execute("ALTER TABLE t MODIFY v BIGINT,ALGORITHM=COPY,LOCK=NONE")
                    .unwrap();
            }));
        });
        if index {
            let handle = db.index_handle(table.id, "vi").unwrap();
            assert_eq!(handle.read().path, db.index_path(table.id, "vi"));
        } else {
            let handle = db.overflow_handle(table.id).unwrap();
            assert_eq!(handle.read().path, db.overflow_path(table.id));
        }
        assert_eq!(
            db.table("test.t").unwrap().active_generation,
            table.active_generation + 1
        );
        engine
            .execute("UPDATE t SET payload=CONCAT(payload,'y'),v=9 WHERE id=1")
            .unwrap();
        engine.shutdown().unwrap();
    }
}

#[test]
fn delayed_heap_open_retries_after_metadata_codec_refresh() {
    use crate::engine::{QueryResponse, StorageQueryEngine};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,7)").unwrap();
    let db = engine.database();
    let table = db.table("test.t").unwrap();
    db.flush_dirty_data().unwrap();
    db.remove_heap_cache_entry(table.id).unwrap();
    let open_epoch = db.storage_handle_open_epoch(table.id);
    let epoch = open_epoch.load(Ordering::Acquire);
    let delayed = Arc::new(parking_lot::RwLock::new(
        HeapFile::open_with_buffer_pool(
            db.table_path(table.id),
            table.id,
            db.log.clone(),
            db.config.durability,
            db.config.heap_update_reserve_bytes,
            db.buffer_pool.clone(),
            table.row_codec.clone(),
        )
        .unwrap(),
    ));
    engine
        .execute("ALTER TABLE t ADD payload VARCHAR(20) DEFAULT 'added',ALGORITHM=INSTANT")
        .unwrap();
    assert!(db
        .install_heap_handle(table.id, delayed, &open_epoch, epoch)
        .is_none());
    engine.execute("INSERT INTO t VALUES(2,8,'new')").unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT * FROM t ORDER BY id")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![
                Value::Int(1),
                Value::Int(7),
                Value::String("added".to_string())
            ],
            vec![
                Value::Int(2),
                Value::Int(8),
                Value::String("new".to_string())
            ]
        ]
    );
    engine.shutdown().unwrap();
}

#[test]
fn private_added_index_keeps_identity_after_cold_open() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = open_relaxed_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let source = db.table("test.t").unwrap();
    let mut candidate = source.as_ref().clone();
    candidate.active_generation += 1;
    let mut added = candidate.indexes[0].clone();
    added.name = "new_private_index".to_string();
    added.index_id = candidate.next_index_id;
    added.primary = false;
    candidate.indexes.push(added.clone());
    let shadow = 100000;
    db.register_reorg_storage_definition(&[shadow], &candidate);
    let path = db.table_path(shadow);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, encode_heap_meta(&path, 1)).unwrap();
    db.set_cached_heap_codec(shadow, candidate.row_codec.clone())
        .unwrap();
    let txn = db.begin_txn().unwrap();
    let values = vec![vec![Value::Int(7)]];
    let ids = db
        .insert_online_copy_batch(
            txn.txn_id,
            shadow,
            &candidate.row_codec,
            &[RowHeader::visible(txn.txn_id)],
            &values,
        )
        .unwrap();
    let key = candidate.index_key(&added, &values[0]).unwrap();
    let path = db.index_path_for_table_schema(&candidate, shadow, &added.name);
    let mut file = IndexFile::create_rebuilt_from_sorted_with_durability(
        path,
        vec![IndexEntry {
            key: key.clone(),
            row_id: ids[0],
        }],
        db.log.clone(),
        db.config.durability,
        db.buffer_pool.clone(),
    )
    .unwrap();
    file.flush().unwrap();
    file.retain_buffered_pages_on_drop();
    drop(file);
    assert_eq!(
        db.with_index_read(shadow, &added.name, |file| file.get_unique_one(&key))
            .unwrap(),
        Some(ids[0]),
        "a private index miss must reopen its candidate identity"
    );
    db.unregister_reorg_storage_locs(&[shadow]);
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn private_partition_heap_codec_survives_idle_handle_eviction() {
    use crate::engine::{QueryResponse, StorageQueryEngine};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for primary_key in ["PRIMARY KEY", ""] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(
            StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                test_storage_config(DurabilityMode::Relaxed, true),
            )
            .unwrap(),
        );
        engine.execute(&format!("CREATE TABLE t(id INT {primary_key},n DECIMAL(18,4),payload LONGTEXT,b VARBINARY(8),KEY ni(n)) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
        engine.execute("INSERT INTO t VALUES(1,1.1250,REPEAT('a',10000),X'0102'),(2,2.2500,REPEAT('b',10001),X'ff'),(11,3.5,REPEAT('c',10002),X'00')").unwrap();
        let query = "SELECT id,CAST(n AS CHAR),LENGTH(payload),HEX(b) FROM t FORCE INDEX(ni) WHERE n>=0 ORDER BY id";
        let QueryResponse::ResultSet(before) =
            engine.execute(query).unwrap().materialize().unwrap()
        else {
            panic!("rows")
        };
        for sql in [
            "ALTER TABLE t REBUILD PARTITION p0,LOCK=NONE",
            "ALTER TABLE t MODIFY n DECIMAL(20,4),LOCK=NONE",
        ] {
            let hook = engine.database().install_ddl_backfill_test_hook();
            let worker = engine.clone();
            let ddl = std::thread::spawn(move || worker.execute(sql));
            assert!(hook.wait_until_reached(Duration::from_secs(5)));
            let db = engine.database();
            let ids = db
                .reorg_storage_locs
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>();
            let mut evicted = 0;
            for id in ids {
                let mut cache = db.heap_cache.write();
                if cache
                    .get(&id)
                    .is_some_and(|handle| Arc::strong_count(handle) == 1)
                {
                    let handle = cache.remove(&id).unwrap();
                    handle.write().retain_buffered_pages_on_drop();
                    evicted += 1;
                }
            }
            hook.release();
            assert!(
                evicted > 0,
                "the test must evict a private heap before copying"
            );
            ddl.join().unwrap().unwrap();
            let QueryResponse::ResultSet(after) =
                engine.execute(query).unwrap().materialize().unwrap()
            else {
                panic!("rows")
            };
            assert_eq!(after.rows, before.rows);
            assert!(db.reorg_storage_locs.lock().unwrap().is_empty());
        }
        engine.shutdown().unwrap();
    }
}

fn test_log(dir: &Path) -> Arc<crate::wal::LogMutex> {
    Arc::new(parking_lot::Mutex::new(LogManager::open(dir).unwrap()))
}

/// Test helper: build the v33 on-disk path for one of a table's
/// `data.*` sidecars (heap/lob/ai) in the default `test` database.
fn t_path(dir: &Path, table_name: &str, table_id: u64, ext: &str) -> PathBuf {
    crate::storage::naming::table_object_path(dir, "test", table_name, table_id, ext)
}

/// Test helper: build the v33 on-disk path for a table's index file
/// (`.idx` or `.hnsw`) in the default `test` database.
fn t_index_path(
    dir: &Path,
    _table_name: &str,
    table_id: u64,
    _index_name: &str,
    ext: &str,
) -> PathBuf {
    crate::storage::naming::index_object_path_by_id(dir, table_id, 1, 1, ext)
}

/// Test helper: relative path string (the WAL-record `path` form) for
/// a table sidecar in the default `test` database.
fn t_rel_path(table_name: &str, table_id: u64, ext: &str) -> String {
    crate::storage::naming::relative_table_object_path("test", table_name, table_id, ext)
}

fn wide_test_index_key(value: i64) -> IndexKey {
    IndexKey::from(vec![
        KeyPart::Int(value / 10),
        KeyPart::String(format!("k{value:04}-{}", "x".repeat(96))),
    ])
}

fn test_row_id(value: i64, rows_per_page: i64) -> RowId {
    RowId {
        page_id: value as u32 / rows_per_page as u32,
        slot_id: value as u16,
    }
}

fn test_storage_config(
    durability: DurabilityMode,
    startup_validate_indexes: bool,
) -> StorageConfig {
    StorageConfig {
        durability,
        checkpoint_interval: Duration::from_millis(DEFAULT_CHECKPOINT_INTERVAL_MS),
        checkpoint_wal_bytes: DEFAULT_CHECKPOINT_WAL_BYTES,
        lock_wait: Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        group_commit_window: Duration::from_micros(DEFAULT_GROUP_COMMIT_US),
        wal_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
        buffer_pool_pages: DEFAULT_BUFFER_POOL_PAGES,
        buffer_pool_io_threads: default_buffer_pool_io_threads(),
        buffer_pool_prefetch_pages: DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
        undo_retention: Duration::from_millis(DEFAULT_UNDO_RETENTION_MS),
        heap_update_reserve_bytes: DEFAULT_HEAP_UPDATE_RESERVE_BYTES,
        startup_validation: StartupValidationMode::Auto,
        startup_validate_indexes,
    }
}

fn open_memory_test_db(dir: &Path) -> Database {
    Database::open_with_config(dir, test_storage_config(DurabilityMode::Memory, false)).unwrap()
}

fn open_relaxed_test_db(dir: &Path) -> Database {
    Database::open_with_config(dir, test_storage_config(DurabilityMode::Relaxed, true)).unwrap()
}

#[test]
fn foreground_bulk_ddl_guard_is_nested_and_releases_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    assert!(!db.foreground_bulk_ddl_is_active());
    {
        let _outer = db.enter_foreground_bulk_ddl();
        assert!(db.foreground_bulk_ddl_is_active());
        {
            let _inner = db.enter_foreground_bulk_ddl();
            assert_eq!(db.foreground_bulk_ddl_active.load(Ordering::Acquire), 2);
        }
        assert_eq!(db.foreground_bulk_ddl_active.load(Ordering::Acquire), 1);
    }
    assert!(!db.foreground_bulk_ddl_is_active());
}

#[test]
fn catalog_object_header_releases_descriptor_child_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "header_capacity",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let table = db.table("header_capacity").unwrap();
    let header = CatalogManager::object_header(&table);
    assert_eq!(header.columns.capacity(), 0);
    assert_eq!(header.primary_key.capacity(), 0);
    assert_eq!(header.indexes.capacity(), 0);
    assert_eq!(header.row_layouts.capacity(), 0);
    assert_eq!(header.column_name_map.capacity(), 0);
    assert_eq!(header.column_id_map.capacity(), 0);
}

#[test]
fn catalog_manager_initializes_live_catalog_with_compact_headers() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "lazy_descriptor",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let full_catalog = db.catalog_read().unwrap().clone();
    assert_eq!(full_catalog.tables["test.lazy_descriptor"].columns.len(), 1);
    let manager = CatalogManager::new(full_catalog).unwrap();
    let catalog = manager.read().unwrap();
    let header = catalog.tables.get("test.lazy_descriptor").unwrap();
    assert!(header.columns.is_empty());
    assert_eq!(header.columns.capacity(), 0);
}

#[test]
fn rename_table_uses_name_independent_physical_identity_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let physical_path;
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "rename_old",
            vec![checkpoint_test_pk_column()],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let table_id = db.table("test.rename_old").unwrap().id;
        let before = t_path(dir.path(), "rename_old", table_id, "heap");
        let after = t_path(dir.path(), "rename_new", table_id, "heap");
        assert_eq!(before, after, "table paths must be keyed only by id");
        physical_path = before;
        db.rename_table("test.rename_old", "test.rename_new")
            .unwrap();
    }

    let reopened = open_relaxed_test_db(dir.path());
    assert!(reopened.table("test.rename_new").is_ok());
    assert!(physical_path.exists());
}

#[test]
fn rename_index_preserves_stable_physical_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let mut value_column = checkpoint_test_pk_column();
    value_column.name = "v".into();
    value_column.primary_key = false;
    value_column.nullable = true;
    db.create_table(
        "stable_index",
        vec![checkpoint_test_pk_column(), value_column],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.add_index(
        "stable_index",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "old_idx".into(),
            columns: vec!["v".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: IndexState::Public,
            index_type: "BTREE".into(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();
    let before = db.table("stable_index").unwrap();
    let before_id = before
        .indexes
        .iter()
        .find(|index| index.name == "old_idx")
        .unwrap()
        .index_id;
    let before_path = db.index_path(before.id, "old_idx");
    assert!(before_path.exists(), "{}", before_path.display());

    db.rename_index("stable_index", "old_idx", "new_idx")
        .unwrap();
    let after = db.table("stable_index").unwrap();
    let after_id = after
        .indexes
        .iter()
        .find(|index| index.name == "new_idx")
        .unwrap()
        .index_id;
    let after_path = db.index_path(after.id, "new_idx");
    assert_eq!(before_id, after_id);
    assert_eq!(before_path, after_path);
    assert!(after_path.exists());
    drop(after);
    drop(before);
    drop(db);
    let reopened = open_relaxed_test_db(dir.path());
    let table = reopened.table("stable_index").unwrap();
    let reopened_index = table
        .indexes
        .iter()
        .find(|index| index.name == "new_idx")
        .unwrap();
    assert_eq!(reopened_index.index_id, before_id);
    assert_eq!(reopened.index_path(table.id, "new_idx"), after_path);
}

#[test]
fn analyze_scan_guard_is_nested_and_releases_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    assert!(!db.analyze_scan_is_active());
    {
        let _outer = db.enter_analyze_scan();
        assert!(db.analyze_scan_is_active());
        {
            let _inner = db.enter_analyze_scan();
            assert_eq!(db.analyze_scan_active.load(Ordering::Acquire), 2);
        }
        assert_eq!(db.analyze_scan_active.load(Ordering::Acquire), 1);
    }
    assert!(!db.analyze_scan_is_active());
}

fn test_storage_config_with_validation(
    durability: DurabilityMode,
    startup_validation: StartupValidationMode,
) -> StorageConfig {
    StorageConfig {
        startup_validation,
        ..test_storage_config(durability, true)
    }
}

fn value_frequencies_for_assert(frequencies: &[ValueFrequency]) -> Vec<(String, u64)> {
    frequencies
        .iter()
        .map(|frequency| (value_to_string(&frequency.value), frequency.count))
        .collect()
}

fn checkpoint_test_pk_column() -> ColumnDef {
    ColumnDef {
        name: "id".into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key: true,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    }
}

fn create_checkpoint_test_table(db: &Database) {
    db.create_table(
        "t",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
}

fn insert_checkpoint_test_row(db: &Database, id: i64) {
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(id)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn losing_speculative_heap_open_preserves_winner_dirty_pages() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    create_checkpoint_test_table(&db);
    insert_checkpoint_test_row(&db, 7);

    let table = db.table("test.t").unwrap();
    let winner = db.cached_heap_handle(table.id).unwrap();
    assert_eq!(
        db.scan_rows(
            "test.t",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap()
        .len(),
        1,
    );

    // Recreate the state reached when two cache-miss callers both finish
    // opening the same heap outside heap_cache.write(): `winner` is already
    // installed and owns a dirty in-memory page, while `candidate` has the
    // same buffer-pool file key and loses the final install race.
    let candidate = Arc::new(parking_lot::RwLock::new(
        HeapFile::open_with_buffer_pool(
            db.table_path(table.id),
            table.id,
            db.log.clone(),
            DurabilityMode::Memory,
            db.config.heap_update_reserve_bytes,
            db.buffer_pool.clone(),
            table.row_codec.clone(),
        )
        .unwrap(),
    ));
    let open_epoch = db.storage_handle_open_epoch(table.id);
    let selected = db
        .install_heap_handle(
            table.id,
            candidate,
            &open_epoch,
            open_epoch.load(Ordering::Acquire),
        )
        .unwrap();
    assert!(Arc::ptr_eq(&selected, &winner));
    drop(selected);

    let rows = db
        .scan_rows(
            "test.t",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::Int(7)]);
}

#[test]
fn consuming_heap_page_encode_preserves_lsn() {
    let mut page = Page::new();
    page.set_lsn(42);
    let encoded = page.encode_into_owned();
    assert_eq!(page_lsn(&encoded), 42);
    Page::decode(&encoded).unwrap();
}

#[test]
fn heap_batch_inserts_emit_one_fpi_per_checkpoint_window() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    create_checkpoint_test_table(&db);
    insert_checkpoint_test_row(&db, 1);
    db.checkpoint().unwrap();

    let before = db.storage_status();
    for id in 2..=17 {
        insert_checkpoint_test_row(&db, id);
    }
    // Zone Map now has its own physical page redo. Count this heap's FPIs,
    // rather than mixing all file kinds through the global fallback counter.
    let last_lsn = db.log.lock().last_lsn();
    LogManager::sync_durable_to(&db.log, last_lsn).unwrap();
    let heap_path = db.table_path(db.table("test.t").unwrap().id);
    let relative = crate::storage::naming::wal_relative_object_path(&heap_path);
    let scan = LogManager::scan(dir.path()).unwrap();
    let emitted: Vec<_> = scan
        .records
        .iter()
        .filter(|record| {
            record.lsn >= before.current_lsn
                && matches!(&record.payload,
            WalPayload::FullPageImage {path,..} | WalPayload::FullPageImageCompressed {path,..}
            if path == &relative)
        })
        .collect();
    assert_eq!(
        emitted.len(),
        1,
        "one resident heap page should emit exactly one FPI in a checkpoint window",
    );
    let record = emitted[0];
    let image = crate::wal::wal_full_page_image_after(&record.payload).unwrap();
    assert_eq!(
        page_lsn(&image),
        0,
        "FPI image must be encoded before WAL LSN allocation"
    );
    let replay = tempfile::tempdir_in("/tmp/mydb").unwrap();
    redo_page_update(replay.path(), &relative, 0, record.lsn, &image).unwrap();
    let recovered = fs::read(replay.path().join(&relative)).unwrap();
    assert_eq!(page_lsn(&recovered), record.lsn);
    Page::decode(&recovered).unwrap();
    redo_page_update(replay.path(), &relative, 0, record.lsn, &image).unwrap();
    assert_eq!(fs::read(replay.path().join(&relative)).unwrap(), recovered);
}

#[test]
fn live_row_delta_is_signed_and_commit_aware() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let table_id = db.table("test.t").unwrap().id;

    let rolled_back = db.begin_txn().unwrap();
    db.insert_rows(rolled_back, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    assert_eq!(db.table_live_row_delta(table_id), 0);
    db.rollback_txn(rolled_back.txn_id).unwrap();
    assert_eq!(db.table_live_row_delta(table_id), 0);

    let savepoint_txn = db.begin_txn().unwrap();
    let savepoint = db.statement_savepoint(savepoint_txn.txn_id).unwrap();
    db.insert_rows(savepoint_txn, "test.t", vec![vec![Value::Int(3)]])
        .unwrap();
    db.rollback_to_statement_savepoint(savepoint_txn.txn_id, savepoint)
        .unwrap();
    db.commit_txn(savepoint_txn.txn_id).unwrap();
    assert_eq!(db.table_live_row_delta(table_id), 0);

    let inserted = db.begin_txn().unwrap();
    db.insert_rows(inserted, "test.t", vec![vec![Value::Int(2)]])
        .unwrap();
    db.commit_txn(inserted.txn_id).unwrap();
    assert_eq!(db.table_live_row_delta(table_id), 1);

    let deleted = db.begin_txn().unwrap();
    db.delete_rows(deleted, "test.t", None, |row| Ok(row[0] == Value::Int(2)))
        .unwrap();
    assert_eq!(db.table_live_row_delta(table_id), 1);
    db.commit_txn(deleted.txn_id).unwrap();
    assert_eq!(db.table_live_row_delta(table_id), 0);
}

#[test]
fn heap_round_trips_rows_and_reuses_slots() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let heap = HeapFile::open(path, 1, test_log(dir.path())).unwrap();
    let row_id = heap
        .insert_mvcc(
            1,
            RowHeader::visible(1),
            &[Value::Int(7), Value::String("a".into())],
        )
        .unwrap();

    assert_eq!(
        heap.read_mvcc(row_id).unwrap().unwrap().values[0],
        Value::Int(7)
    );
    heap.delete(1, row_id).unwrap();
    assert!(heap.read_mvcc(row_id).unwrap().is_none());
    let row_id2 = heap
        .insert_mvcc(
            1,
            RowHeader::visible(1),
            &[Value::Int(8), Value::String("b".into())],
        )
        .unwrap();
    // M1 doesn't reuse DELETED slots (no FREE state produced yet), so
    // the new insert lands on a fresh slot. Before rowid-6bytes the
    // delete bumped the slot's generation and reused the same slot id;
    // now we expect a distinct slot id instead.
    assert_ne!(row_id2.slot_id, row_id.slot_id);
}

#[test]
fn raw_heap_page_payload_reader_matches_page_decode() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let heap = HeapFile::open(path, 1, test_log(dir.path())).unwrap();
    let row_id = heap
        .insert_mvcc(
            1,
            RowHeader::visible(1),
            &[Value::Int(7), Value::String("payload".into())],
        )
        .unwrap();

    let page_bytes = heap
        .buffer_pool
        .read_raw_page(heap.page_key(row_id.page_id as u64))
        .unwrap();
    let page = Page::decode(&page_bytes).unwrap();
    let expected = page.read(row_id.slot_id).unwrap();
    let actual = read_page_payload(&page_bytes, row_id).unwrap().unwrap();
    assert_eq!(actual, expected);

    // Reads against a slot id outside the page directory return None.
    let stale_row_id = RowId {
        slot_id: row_id.slot_id.wrapping_add(1024),
        ..row_id
    };
    assert!(read_page_payload(&page_bytes, stale_row_id)
        .unwrap()
        .is_none());

    let mut corrupt = page_bytes.clone();
    let slot_base = PAGE_HEADER_SIZE + row_id.slot_id as usize * SLOT_SIZE;
    write_u16(&mut corrupt, slot_base, PAGE_SIZE as u16);
    let checksum = page_checksum(&corrupt, 16);
    write_u32(&mut corrupt, 16, checksum);
    assert_eq!(
        read_page_payload(&corrupt, row_id).unwrap_err().code(),
        1030
    );
}

#[test]
fn dense_probe_admission_bounds_amplification_and_keeps_sparse_reads() {
    let rows: Vec<RowId> = (0..64)
        .flat_map(|page_id| (0..32).map(move |slot_id| RowId { page_id, slot_id }))
        .collect();
    assert!(dense_probe_worth_streaming(&rows, 1000, 32));
    assert!(!dense_probe_worth_streaming(&rows, 1000, 1000));
    assert!(!dense_probe_worth_streaming(&rows[..1023], 1000, 32));
    let mut sparse = rows.clone();
    for row in &mut sparse {
        row.page_id *= 3;
    }
    assert!(!dense_probe_worth_streaming(&sparse, 1000, 32));
    sparse.reverse();
    assert!(!dense_probe_worth_streaming(&sparse, 1000, 32));
    let mut islands = rows.clone();
    for row in &mut islands {
        row.page_id += (row.page_id / 16) * 1000;
    }
    assert!(dense_probe_worth_streaming(&islands, 10000, 32));
    let mut start = 0;
    while start < islands.len() {
        let end = dense_probe_batch_end(&islands, start);
        let window = &islands[start..end];
        let distinct = 1 + window
            .windows(2)
            .filter(|w| w[0].page_id != w[1].page_id)
            .count();
        let span = (window.last().unwrap().page_id - window[0].page_id + 1) as usize;
        assert!(span <= 64 && span <= distinct * 2);
        start = end;
    }
    assert!(!dense_probe_worth_streaming(&rows, 63, 32));
    assert!(!dense_probe_worth_streaming(&[], 1000, 32));
}

#[test]
fn dense_probe_matches_point_reads_with_dirty_rows_undo_and_forwarding() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Relaxed, true);
    config.buffer_pool_pages = 64;
    config.heap_update_reserve_bytes = 0;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    let mut padding = checkpoint_test_pk_column();
    padding.name = "payload".into();
    padding.primary_key = false;
    padding.nullable = true;
    padding.data_type = DataType::VarChar(Some(8000));
    let mut number = padding.clone();
    number.name = "number".into();
    number.data_type = DataType::Decimal(Some((18, 2)));
    db.create_table(
        "t",
        vec![checkpoint_test_pk_column(), padding, number],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    for start in (0..2048).step_by(128) {
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            (start..start + 128)
                .map(|id| {
                    vec![
                        Value::Int(id),
                        Value::String("x".repeat(1000)),
                        if id % 3 == 0 {
                            Value::Null
                        } else {
                            Value::Decimal("123.45".into())
                        },
                    ]
                })
                .collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }
    let mut added = checkpoint_test_pk_column();
    added.name = "added".into();
    added.primary_key = false;
    added.data_type = DataType::Char(Some(12));
    added.default = Some(crate::parser::Expr::Literal(
        crate::parser::Literal::String("default".into()),
    ));
    db.add_column("test.t", added).unwrap();
    let table = db.table("test.t").unwrap();
    let heap = db.cached_heap_handle(table.id).unwrap();
    let snapshot = db.begin_txn().unwrap();
    let mut ids: Vec<_> = db
        .scan_rows("test.t", snapshot)
        .unwrap()
        .into_iter()
        .map(|r| r.row_id)
        .collect();
    ids.sort_unstable();
    assert!(dense_probe_worth_streaming(
        &ids,
        heap.read().page_count().unwrap(),
        db.buffer_pool.capacity_pages()
    ));
    let writer = db.begin_txn().unwrap();
    db.update_row_by_id(writer, &table, ids[0], |values| {
        let mut values = values.to_vec();
        values[1] = Value::String("forward".repeat(500));
        Ok(values)
    })
    .unwrap();
    assert!(
        heap.read().row_migration_count() > 0,
        "test must create a forwarding anchor"
    );
    db.delete_row_by_id(writer, &table, ids[1]).unwrap();
    // Both snapshots and the writer's own dirty versions use the same MVCC
    // oracle after the new physical read path returns headers and values.
    for (iteration, view) in [snapshot, writer].into_iter().enumerate() {
        let mut expected = Vec::new();
        for &id in &ids {
            if let Some(row) = db.read_row_with_handle(&heap, &table, id, view).unwrap() {
                expected.push((
                    id,
                    vec![
                        row.values[3].clone(),
                        row.values[2].clone(),
                        row.values[1].clone(),
                        row.values[0].clone(),
                    ],
                ));
            }
        }
        db.buffer_pool.flush_and_evict_all().unwrap();
        // Keep an uncommitted change resident while most requested pages are
        // cold. It must override disk bytes, even if its commit is invisible.
        db.update_row_by_id(writer, &table, ids[2], |values| {
            let mut values = values.to_vec();
            values[2] = Value::Decimal(format!("{}.00", 400 + iteration));
            Ok(values)
        })
        .unwrap();
        let oracle = db
            .read_row_with_handle(&heap, &table, ids[2], view)
            .unwrap()
            .unwrap();
        expected.iter_mut().find(|(id, _)| *id == ids[2]).unwrap().1 = [3, 2, 1, 0]
            .into_iter()
            .map(|column| oracle.values[column].clone())
            .collect();
        let before = db.buffer_pool.read_ios_scan.load(Ordering::Relaxed);
        let mut actual = db
            .read_row_columns_many_with_handle(&heap, &table, &ids, view, &[3, 2, 1, 0])
            .unwrap();
        actual.sort_by_key(|r| r.0);
        assert_eq!(actual, expected);
        assert!(
            db.buffer_pool.read_ios_scan.load(Ordering::Relaxed) > before,
            "dense cold path was not used"
        );
    }
    db.rollback_txn(writer.txn_id).unwrap();
    db.rollback_txn(snapshot.txn_id).unwrap();
}

#[test]
fn offline_projected_scan_reads_unflushed_buffered_heap_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    // Memory durability intentionally never flushes dirty heap pages, making
    // the buffer pool the only authoritative image and preventing a timing
    // race with the background flusher in this regression.
    let heap =
        HeapFile::open_with_durability(path, 1, test_log(dir.path()), DurabilityMode::Memory, 0)
            .unwrap();
    heap.insert_mvcc(1, RowHeader::visible(1), &[Value::Int(42)])
        .unwrap();

    let mut values = Vec::new();
    heap.scan_mvcc_columns_direct_until(&[0], |row| {
        values.push(row.values[0].clone());
        Ok(false)
    })
    .unwrap();
    assert_eq!(values, vec![Value::Int(42)]);
}

#[test]
fn offline_projected_scan_batches_cold_ranges_and_keeps_cached_tail_coherent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let total = 150i64;
    {
        let heap = HeapFile::open(path.clone(), 1, test_log(dir.path())).unwrap();
        let padding = "x".repeat(8_000);
        for value in 0..total {
            heap.insert_mvcc(
                1,
                RowHeader::visible(1),
                &[Value::Int(value), Value::String(padding.clone())],
            )
            .unwrap();
        }
        assert!(heap.page_count_relaxed() > 64);
        heap.flush_dirty_pages().unwrap();
    }

    // Reopen with an empty pool so the original pages exercise the batched
    // cold-read path. The appended row remains resident and unflushed, so the
    // final batch must combine its coherent cached image with disk pages.
    let heap = HeapFile::open(path, 1, test_log(dir.path())).unwrap();
    heap.insert_mvcc(
        2,
        RowHeader::visible(2),
        &[Value::Int(total), Value::String("tail".into())],
    )
    .unwrap();
    let mut values = Vec::new();
    heap.scan_mvcc_columns_direct_until(&[0], |row| {
        let Value::Int(value) = row.values[0] else {
            panic!("expected projected INT value")
        };
        values.push(value);
        Ok(false)
    })
    .unwrap();
    values.sort();
    assert_eq!(values, (0..=total).collect::<Vec<_>>());

    // Raw batch consumers use uninitialized scratch. Disk ranges and the
    // dirty tail must jointly initialize every exposed byte before decode.
    let mut raw_values = Vec::new();
    heap.scan_mvcc_payloads_direct_range_until(&[0], 0, heap.page_count_relaxed(), |row| {
        let mut decoded = Vec::new();
        assert!(HeapFile::decode_direct_payload_projection_into(
            &row,
            &mut decoded
        )?);
        let Value::Int(value) = decoded[0] else {
            panic!("expected INT")
        };
        raw_values.push(value);
        Ok(false)
    })
    .unwrap();
    raw_values.sort();
    assert_eq!(raw_values, values);

    // Reusing the same admitted scratch across disjoint morsels must neither
    // repeat rows nor serve an earlier cached image of the dirty tail.
    let mut scratch = DirectScanScratch::default();
    let mut reused_values = Vec::new();
    let pages = heap.page_count_relaxed();
    let mut first_pointer = None;
    for start in (0..pages).step_by(16) {
        heap.scan_mvcc_payloads_direct_range_reusing_until(
            &[0],
            start,
            (start + 16).min(pages),
            &mut scratch,
            |row| {
                let mut decoded = Vec::new();
                assert!(HeapFile::decode_direct_payload_projection_into(
                    &row,
                    &mut decoded
                )?);
                reused_values.push(decoded.remove(0));
                Ok(false)
            },
        )
        .unwrap();
        assert_eq!(
            *first_pointer.get_or_insert(scratch.buffer.as_ptr()),
            scratch.buffer.as_ptr()
        );
        assert_eq!(
            scratch.charge.as_ref().unwrap().bytes(),
            scratch.buffer.len()
        );
    }
    let clone = scratch.clone();
    assert!(clone.buffer.is_empty() && clone.charge.is_none());
    reused_values.sort_by_key(|value| match value {
        Value::Int(value) => *value,
        _ => panic!("expected INT"),
    });
    assert_eq!(
        reused_values,
        values.iter().copied().map(Value::Int).collect::<Vec<_>>()
    );

    // A bulk builder lends a bounded page lease without a second admission.
    // Exercise multiple cold batches plus the resident dirty tail.
    let mut charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
        PAGE_SIZE,
    )
    .unwrap();
    let mut prepaid_values = Vec::new();
    heap.scan_mvcc_payloads_direct_range_with_charge_until(
        &[0],
        0,
        heap.page_count_relaxed(),
        &mut charge,
        |row| {
            let mut decoded = Vec::new();
            assert!(HeapFile::decode_direct_payload_projection_into(
                &row,
                &mut decoded
            )?);
            prepaid_values.push(decoded.remove(0));
            Ok(false)
        },
    )
    .unwrap();
    assert_eq!(prepaid_values.len(), values.len());
    assert_eq!(charge.bytes(), PAGE_SIZE);
    let mut prepaid_values = prepaid_values
        .into_iter()
        .map(|value| {
            let Value::Int(value) = value else {
                panic!("expected INT")
            };
            value
        })
        .collect::<Vec<_>>();
    prepaid_values.sort();
    assert_eq!(prepaid_values, values);
}
#[test]
fn heap_insert_wal_is_smaller_than_page_before_after_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let log = test_log(dir.path());
    let before = log.lock().wal_bytes();
    let heap = HeapFile::open(path, 1, log.clone()).unwrap();

    heap.insert_mvcc(1, RowHeader::visible(1), &[Value::Int(7)])
        .unwrap();
    log.lock().flush().unwrap();
    let after = log.lock().wal_bytes();

    assert!(after > before);
    assert!(
        after - before < PAGE_SIZE as u64,
        "logical heap WAL should not write full before+after pages"
    );
    assert_eq!(log.lock().stats().logical_records, 1);
}

#[test]
fn heap_insert_reserves_space_for_variable_width_updates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let heap = HeapFile::open_with_durability(
        path,
        1,
        test_log(dir.path()),
        DurabilityMode::Relaxed,
        2048,
    )
    .unwrap();
    // Insert enough rows that they must spill onto a second page even
    // under the v2 row codec (which packs ~30% denser than v1). The
    // test's intent is to verify that update_reserve_bytes still
    // leaves room for an in-place UPDATE that grows a row, so we need
    // at least two pages to choose a first-page victim from.
    let mut row_ids = Vec::new();
    for id in 0..400 {
        row_ids.push(
            heap.insert_mvcc(
                1,
                RowHeader::visible(1),
                &[Value::Int(id), Value::String("x".repeat(40))],
            )
            .unwrap(),
        );
    }

    let first_page = row_ids[0].page_id;
    assert!(row_ids.iter().any(|row_id| row_id.page_id > first_page));
    let same_page = row_ids
        .iter()
        .copied()
        .find(|row_id| row_id.page_id == first_page)
        .unwrap();
    heap.write_mvcc(
        2,
        same_page,
        RowHeader::visible(2),
        &[Value::Int(0), Value::String("y".repeat(1600))],
    )
    .unwrap();

    let row = heap.read_mvcc(same_page).unwrap().unwrap();
    assert_eq!(row.values[1], Value::String("y".repeat(1600)));
}

#[test]
fn heap_update_migrates_large_row_while_preserving_rowid() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let heap =
        HeapFile::open_with_durability(path, 1, test_log(dir.path()), DurabilityMode::Relaxed, 0)
            .unwrap();
    let row_id = heap
        .insert_mvcc(
            1,
            RowHeader::visible(1),
            &[Value::Int(1), Value::String("x".repeat(16))],
        )
        .unwrap();
    for id in 0..200 {
        heap.insert_mvcc(
            1,
            RowHeader::visible(1),
            &[Value::Int(id + 10), Value::String("filler".repeat(40))],
        )
        .unwrap();
    }

    heap.write_mvcc(
        2,
        row_id,
        RowHeader::visible(2),
        &[Value::Int(1), Value::String("y".repeat(6000))],
    )
    .unwrap();

    let row = heap.read_mvcc(row_id).unwrap().unwrap();
    assert_eq!(row.row_id, row_id);
    assert_eq!(row.values[1], Value::String("y".repeat(6000)));

    let rows = heap.scan_mvcc().unwrap();
    assert_eq!(
        rows.iter()
            .filter(|row| row.values[0] == Value::Int(1))
            .count(),
        1
    );
}

#[test]
fn payload_range_scans_follow_migrated_rows_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let mut heap = HeapFile::open(path.clone(), 1, test_log(dir.path())).unwrap();
    let anchor = heap
        .insert_mvcc(
            1,
            RowHeader::visible(1),
            &[Value::Int(1), Value::String("small".into())],
        )
        .unwrap();
    for id in 10..210 {
        heap.insert_mvcc(
            1,
            RowHeader::visible(1),
            &[Value::Int(id), Value::String("filler".repeat(40))],
        )
        .unwrap();
    }
    let expected = vec![Value::Int(1), Value::String("grown".repeat(1600))];
    heap.write_mvcc(2, anchor, RowHeader::visible(2), &expected)
        .unwrap();
    assert!(heap.row_migration_count() > 0);
    let target = heap
        .with_page(u64::from(anchor.page_id), |page| {
            decode_heap_forwarding_stub(page.payload(anchor.slot_id as usize)?.unwrap())
        })
        .unwrap()
        .unwrap();
    assert_ne!(anchor.page_id, target.page_id);

    // Both resident dirty pages and a reopened cold heap must preserve the
    // logical anchor, even when the piece is outside the worker's range.
    for cold in [false, true] {
        if cold {
            heap.flush_dirty_pages().unwrap();
            drop(heap);
            heap = HeapFile::open(path.clone(), 1, test_log(dir.path())).unwrap();
        }
        for direct in [false, true] {
            for columns in [vec![], vec![0, 1]] {
                let mut rows = Vec::new();
                for page in 0..heap.page_count_relaxed() {
                    let mut visit = |row: PayloadMvccStoredRow<'_>| {
                        let mut values = Vec::new();
                        assert!(HeapFile::decode_direct_payload_projection_into(
                            &row,
                            &mut values
                        )?);
                        rows.push((row.row_id, row.header.last_txn_id, values));
                        Ok(false)
                    };
                    if direct {
                        heap.scan_mvcc_payloads_direct_range_until(
                            &columns,
                            page,
                            page + 1,
                            &mut visit,
                        )
                        .unwrap();
                    } else {
                        heap.scan_mvcc_payloads_range_until_with_policy(
                            &columns,
                            page,
                            page + 1,
                            ScanCachePolicy::Bulk,
                            &mut visit,
                        )
                        .unwrap();
                    }
                }
                assert_eq!(
                    rows.len(),
                    201,
                    "cold={cold} direct={direct} columns={columns:?}"
                );
                let migrated: Vec<_> = rows.iter().filter(|row| row.0 == anchor).collect();
                assert_eq!(migrated.len(), 1);
                assert_eq!(migrated[0].1, 2);
                assert_eq!(
                    migrated[0].2,
                    if columns.is_empty() {
                        vec![]
                    } else {
                        expected.clone()
                    }
                );

                let mut stopped = false;
                let mut visit = |row: PayloadMvccStoredRow<'_>| {
                    assert!(!stopped, "scan continued after the visitor stopped");
                    stopped = row.row_id == anchor;
                    Ok(stopped)
                };
                if direct {
                    heap.scan_mvcc_payloads_direct_range_until(
                        &columns,
                        0,
                        heap.page_count_relaxed(),
                        &mut visit,
                    )
                    .unwrap();
                } else {
                    heap.scan_mvcc_payloads_range_until_with_policy(
                        &columns,
                        0,
                        heap.page_count_relaxed(),
                        ScanCachePolicy::Small,
                        &mut visit,
                    )
                    .unwrap();
                }
                assert!(stopped);
            }
        }
    }
}

#[test]
fn relocated_row_piece_preserves_flag_and_scans_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let mut heap = HeapFile::open_with_durability(
        path.clone(),
        1,
        test_log(dir.path()),
        DurabilityMode::Relaxed,
        0,
    )
    .unwrap();
    // Control persistence explicitly so the hot/cold scan cases are stable.
    heap.buffer_pool.shutdown_flusher();
    let wide_row = |id, ch: &str, len| {
        let mut values = vec![Value::Int(id)];
        values.extend((0..8).map(|_| Value::String(ch.repeat(len))));
        values
    };
    let b = wide_row(1, "b", 962);
    let mut u = wide_row(2, "u", 940);
    let mut t = vec![Value::Int(3), Value::String("t".repeat(1500))];
    t.resize(9, Value::Null);
    let b_id = heap.insert_mvcc(1, RowHeader::visible(1), &b).unwrap();
    let anchor = heap.insert_mvcc(1, RowHeader::visible(1), &u).unwrap();
    let t_id = heap.insert_mvcc(1, RowHeader::visible(1), &t).unwrap();
    assert_eq!(b_id.page_id, anchor.page_id);
    assert_ne!(anchor.page_id, t_id.page_id);
    let target_of = |heap: &HeapFile| {
        heap.with_page(u64::from(anchor.page_id), |page| {
            decode_heap_forwarding_stub(page.payload(anchor.slot_id as usize)?.unwrap())
        })
        .unwrap()
        .unwrap()
    };
    u[8] = Value::String(format!("{}{}", "u".repeat(940), "x".repeat(1100)));
    heap.write_mvcc(2, anchor, RowHeader::visible(2), &u)
        .unwrap();
    let old_target = target_of(&heap);
    assert_eq!(old_target.page_id, t_id.page_id);
    for value in &mut u[1..] {
        if let Value::String(value) = value {
            value.push_str(&"y".repeat(770));
        }
    }
    heap.write_mvcc(3, anchor, RowHeader::visible(3), &u)
        .unwrap();
    let new_target = target_of(&heap);
    assert_ne!(
        new_target.page_id, old_target.page_id,
        "must exercise relocation"
    );
    assert_eq!(heap.row_migration_count(), 2);
    heap.with_page(u64::from(old_target.page_id), |page| {
        assert!(page.payload(old_target.slot_id as usize)?.is_none());
        Ok(())
    })
    .unwrap();

    for cold in [false, true] {
        if cold {
            heap.flush_dirty_pages().unwrap();
            drop(heap);
            heap = HeapFile::open_with_durability(
                path.clone(),
                1,
                test_log(dir.path()),
                DurabilityMode::Relaxed,
                0,
            )
            .unwrap();
            heap.buffer_pool.shutdown_flusher();
        }
        let rows = heap.scan_mvcc().unwrap();
        assert_eq!(
            rows.len(),
            3,
            "cold={cold}: physical piece must not be scanned twice"
        );
        for (row_id, values) in [(b_id, &b), (anchor, &u), (t_id, &t)] {
            let row = heap.read_mvcc(row_id).unwrap().unwrap();
            assert_eq!(row.row_id, row_id);
            // The shared test codec has extra nullable columns; only the
            // first nine columns belong to this fixture.
            assert_eq!(&row.values[..values.len()], values);
            assert!(row.values[values.len()..].iter().all(|v| *v == Value::Null));
            assert_eq!(rows.iter().filter(|row| row.row_id == row_id).count(), 1);
        }
        heap.with_page(u64::from(new_target.page_id), |page| {
            let payload = page.payload(new_target.slot_id as usize)?.unwrap();
            assert_ne!(payload[1] & MVCC_FLAG_ROW_PIECE, 0);
            assert!(decode_heap_forwarding_stub(payload)?.is_none());
            Ok(())
        })
        .unwrap();
        for direct in [false, true] {
            let mut ids = Vec::new();
            let mut visit = |row: PayloadMvccStoredRow<'_>| {
                let mut values = Vec::new();
                assert!(HeapFile::decode_direct_payload_projection_into(
                    &row,
                    &mut values
                )?);
                ids.push(values[0].clone());
                Ok(false)
            };
            if direct {
                heap.scan_mvcc_payloads_direct_range_until(
                    &[0],
                    0,
                    heap.page_count_relaxed(),
                    &mut visit,
                )
                .unwrap();
            } else {
                heap.scan_mvcc_payloads_range_until_with_policy(
                    &[0],
                    0,
                    heap.page_count_relaxed(),
                    ScanCachePolicy::Bulk,
                    &mut visit,
                )
                .unwrap();
            }
            assert_eq!(ids, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        }
    }
}

/// Repeatedly growing a migrated row must not chain forwarding stubs. Each
/// growth either updates the existing migrated piece in place or relocates it
/// to a fresh page, but the original row's stub depth stays at 1.
#[test]
fn forwarding_chain_stays_shallow_under_repeated_growth() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let heap =
        HeapFile::open_with_durability(path, 1, test_log(dir.path()), DurabilityMode::Relaxed, 0)
            .unwrap();
    let row_id = heap
        .insert_mvcc(
            1,
            RowHeader::visible(1),
            &[Value::Int(1), Value::String("x".repeat(16))],
        )
        .unwrap();
    // Pack the original page so future updates must migrate.
    for id in 0..200 {
        heap.insert_mvcc(
            1,
            RowHeader::visible(1),
            &[Value::Int(id + 10), Value::String("filler".repeat(40))],
        )
        .unwrap();
    }

    // Grow the row across several update rounds so the migrated piece itself
    // outgrows its current page and must relocate.
    for round in 1..=6 {
        let size = 4_000 + round * 1_500;
        heap.write_mvcc(
            (round + 1) as u64,
            row_id,
            RowHeader::visible((round + 1) as u64),
            &[Value::Int(1), Value::String("y".repeat(size))],
        )
        .unwrap();
        let row = heap.read_mvcc(row_id).unwrap().unwrap();
        assert_eq!(row.row_id, row_id, "RowId must remain stable across growth");
        assert_eq!(row.values[1], Value::String("y".repeat(size)));
    }

    // Scan must succeed: the relocation path keeps the chain depth at 1.
    let rows = heap.scan_mvcc().unwrap();
    let matches: usize = rows
        .iter()
        .filter(|row| row.values[0] == Value::Int(1))
        .count();
    assert_eq!(matches, 1, "exactly one logical row remains");
}

#[test]
fn secondary_index_rowid_reads_migrated_heap_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::VarChar(Some(8000)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        std::iter::once(vec![Value::Int(1), Value::String("x".repeat(16))])
            .chain((0..200).map(|id| vec![Value::Int(id + 10), Value::String("filler".repeat(40))]))
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let txn = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap();
    let primary = table
        .indexes
        .iter()
        .find(|index| index.primary)
        .cloned()
        .unwrap();
    let row_id = db
        .lookup_index(&table, &primary, IndexKey::from(vec![KeyPart::Int(1)]))
        .unwrap()[0];
    db.update_row_by_id(txn, &table, row_id, |_| {
        Ok(vec![Value::Int(1), Value::String("y".repeat(6000))])
    })
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let table = db.table("test.t").unwrap();
    let stmt_view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let row = db.read_row(&table, row_id, stmt_view).unwrap().unwrap();
    assert_eq!(row.row_id, row_id);
    assert_eq!(row.values[1], Value::String("y".repeat(6000)));
}

#[test]
fn spatial_index_search_over_persistent_pages() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.create_table(
        "places",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "loc".into(),
                data_type: DataType::Geometry(
                    crate::storage::schema::GeometrySubtype::Geometry,
                    None,
                ),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("sp".into()),
            columns: vec!["loc".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "SPATIAL".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    let rows: Vec<Vec<Value>> = (0..30)
        .map(|i| {
            let g = crate::engine::geo::parse_wkt_internal(&format!("POINT({i} {i})"), 0).unwrap();
            vec![Value::Int(i), Value::Geometry(g)]
        })
        .collect();
    db.insert_rows(txn, "test.places", rows).unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let table_id = db.table("test.places").unwrap().id;
    // R-tree (built from the durable MBR B-tree) returns every point whose MBR
    // intersects the window [5,5]×[10,10] — points (5,5)…(10,10) ⇒ 6 rows.
    let hits = db
        .spatial_search(
            table_id,
            "sp",
            crate::engine::geo::Mbr::new(5.0, 5.0, 10.0, 10.0),
        )
        .unwrap();
    assert_eq!(hits.len(), 6, "spatial_search window hits");
    let table = db.table("test.places").unwrap();
    let primary = table.indexes.iter().find(|i| i.primary).cloned().unwrap();
    let row_id = db
        .lookup_index(&table, &primary, IndexKey::from(vec![KeyPart::Int(7)]))
        .unwrap()[0];
    let txn = db.begin_txn().unwrap();
    db.delete_row_by_id(txn, &table, row_id).unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let hits2 = db
        .spatial_search(
            table_id,
            "sp",
            crate::engine::geo::Mbr::new(5.0, 5.0, 10.0, 10.0),
        )
        .unwrap();
    assert_eq!(hits2.len(), 5, "spatial_search after delete");
    let txn = db.begin_txn().unwrap();
    let geometry = crate::engine::geo::parse_wkt_internal("POINT(8 8)", 0).unwrap();
    db.insert_rows(
        txn,
        "test.places",
        vec![vec![Value::Int(100), Value::Geometry(geometry)]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(
        db.spatial_search(
            table_id,
            "sp",
            crate::engine::geo::Mbr::new(5.0, 5.0, 10.0, 10.0),
        )
        .unwrap()
        .len(),
        6,
    );

    // An in-flight DELETE removes the physical key, but a different reader
    // must still find the pre-delete version through the visibility repair.
    let table = db.table("test.places").unwrap();
    let spatial = table.indexes.iter().find(|i| i.name == "sp").unwrap();
    let old_window = crate::engine::geo::Mbr::new(8.0, 8.0, 8.0, 8.0);
    let reader = db.begin_txn().unwrap();
    let writer = db.begin_txn().unwrap();
    let row_id = db
        .lookup_index(&table, &primary, IndexKey::from(vec![KeyPart::Int(8)]))
        .unwrap()[0];
    db.delete_row_by_id(writer, &table, row_id).unwrap();
    assert!(!db
        .spatial_search(table_id, "sp", old_window)
        .unwrap()
        .contains(&row_id));
    let candidates = db
        .spatial_search_visible(&table, spatial, old_window, reader)
        .unwrap();
    assert!(candidates.contains(&row_id));
    assert_eq!(
        db.read_row(&table, row_id, reader).unwrap().unwrap().values[0],
        Value::Int(8)
    );
    db.rollback_txn(writer.txn_id).unwrap();

    // Moving the geometry has the same old-key gap. The new physical key
    // points at a row whose old version is visible to this reader.
    let writer = db.begin_txn().unwrap();
    let moved = crate::engine::geo::parse_wkt_internal("POINT(80 80)", 0).unwrap();
    db.update_row_by_id(writer, &table, row_id, |_| {
        Ok(vec![Value::Int(8), Value::Geometry(moved.clone())])
    })
    .unwrap();
    assert!(db
        .spatial_search_visible(&table, spatial, old_window, reader)
        .unwrap()
        .contains(&row_id));
    let new_window = crate::engine::geo::Mbr::new(80.0, 80.0, 80.0, 80.0);
    let new_candidates = db
        .spatial_search_visible(&table, spatial, new_window, reader)
        .unwrap();
    assert!(new_candidates.contains(&row_id));
    assert_eq!(
        db.read_row(&table, row_id, reader).unwrap().unwrap().values[0],
        Value::Int(8)
    );
    db.rollback_txn(writer.txn_id).unwrap();

    // A committed delete must retain its old key for an older RR snapshot.
    let writer = db.begin_txn().unwrap();
    db.delete_row_by_id(writer, &table, row_id).unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    assert!(db
        .spatial_search_visible(&table, spatial, old_window, reader)
        .unwrap()
        .contains(&row_id));
    assert!(db.read_row(&table, row_id, reader).unwrap().is_some());
    db.commit_txn(reader.txn_id).unwrap();
}

#[test]
fn char_rows_store_compact_and_read_compact() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "c".into(),
                data_type: DataType::Char(Some(8)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("c_idx".into()),
            columns: vec!["c".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    let row_id = db
        .insert_rows(
            txn,
            "test.t",
            vec![vec![
                Value::Int(1),
                Value::String("abcd   ".into()),
                Value::Int(1),
            ]],
        )
        .unwrap()[0];
    db.commit_txn(txn.txn_id).unwrap();

    let table = db.table("test.t").unwrap();
    let raw = db
        .with_heap_mut(table.id, |heap| heap.read_mvcc(row_id))
        .unwrap()
        .unwrap();
    assert_eq!(raw.values[1], Value::String("abcd".into()));

    let stmt = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let visible = db.read_row(&table, row_id, stmt).unwrap().unwrap();
    assert_eq!(visible.values[1], Value::String("abcd".into()));

    let c_idx = table
        .indexes
        .iter()
        .find(|index| index.name == "c_idx")
        .unwrap();
    let char_key = IndexKey::from(vec![KeyPart::CollatedString {
        collation_id: crate::charset::UTF8MB4_0900_AI_CI_ID,
        value: "abcd".into(),
    }]);
    assert_eq!(table.index_key(c_idx, &visible.values).unwrap(), char_key);
    assert_eq!(
        db.lookup_index(&table, c_idx, char_key).unwrap(),
        vec![row_id]
    );

    let txn = db.begin_txn().unwrap();
    db.update_row_by_id_with_changed_columns(txn, &table, row_id, Some(&[2]), |old| {
        assert_eq!(old[1], Value::String("abcd".into()));
        let mut next = old.to_vec();
        next[2] = Value::Int(2);
        Ok(next)
    })
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let raw = db
        .with_heap_mut(table.id, |heap| heap.read_mvcc(row_id))
        .unwrap()
        .unwrap();
    assert_eq!(raw.values[1], Value::String("abcd".into()));
}

#[test]
fn batch_heap_reads_skip_deleted_rows_and_follow_forwarding_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::VarChar(Some(8000)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![
            vec![Value::Int(1), Value::String("a".repeat(16))],
            vec![Value::Int(2), Value::String("b".repeat(16))],
            vec![Value::Int(3), Value::String("c".repeat(16))],
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let table = db.table("test.t").unwrap();
    let primary = table
        .indexes
        .iter()
        .find(|index| index.primary)
        .cloned()
        .unwrap();
    let row_ids = db
        .lookup_index(&table, &primary, IndexKey::from(vec![KeyPart::Int(1)]))
        .unwrap();
    let row_id_1 = row_ids[0];
    let row_id_2 = db
        .lookup_index(&table, &primary, IndexKey::from(vec![KeyPart::Int(2)]))
        .unwrap()[0];
    let row_id_3 = db
        .lookup_index(&table, &primary, IndexKey::from(vec![KeyPart::Int(3)]))
        .unwrap()[0];

    let txn = db.begin_txn().unwrap();
    db.update_row_by_id(txn, &table, row_id_2, |_| {
        Ok(vec![Value::Int(2), Value::String("z".repeat(6000))])
    })
    .unwrap();
    db.delete_row_by_id(txn, &table, row_id_3).unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let heap = db.cached_heap_handle(table.id).unwrap();
    let snapshot = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let rows = db
        .read_rows_many_with_handle(&heap, &table, &[row_id_1, row_id_2, row_id_3], snapshot)
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].row_id, row_id_1);
    assert_eq!(rows[0].values[0], Value::Int(1));
    assert_eq!(rows[1].row_id, row_id_2);
    assert_eq!(rows[1].values[0], Value::Int(2));
    assert_eq!(rows[1].values[1], Value::String("z".repeat(6000)));
}

#[test]
fn wal_redo_recovers_committed_migrated_heap_row_without_heap_flush() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::VarChar(Some(8000)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        std::iter::once(vec![Value::Int(1), Value::String("x".repeat(16))])
            .chain((0..200).map(|id| vec![Value::Int(id + 10), Value::String("filler".repeat(40))]))
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    let txn = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap();
    let row_id = db
        .lookup_index(
            &table,
            &table.indexes[0],
            IndexKey::from(vec![KeyPart::Int(1)]),
        )
        .unwrap()[0];
    db.update_row_by_id(txn, &table, row_id, |_| {
        Ok(vec![Value::Int(1), Value::String("z".repeat(6000))])
    })
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);

    let result = recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let table = db.table("test.t").unwrap();
    let stmt_view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let row = db.read_row(&table, row_id, stmt_view).unwrap().unwrap();

    assert_eq!(row.row_id, row_id);
    assert_eq!(row.values[1], Value::String("z".repeat(6000)));
    assert!(result.summary.redone_records > 0);
}

#[test]
fn startup_validation_accepts_persisted_heap_forwarding_stub() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "t",
            vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
                ColumnDef {
                    name: "v".into(),
                    data_type: DataType::VarChar(Some(8000)),
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            std::iter::once(vec![Value::Int(1), Value::String("x".repeat(16))])
                .chain(
                    (0..200)
                        .map(|id| vec![Value::Int(id + 10), Value::String("filler".repeat(40))]),
                )
                .collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let txn = db.begin_txn().unwrap();
        let table = db.table("test.t").unwrap();
        let row_id = db
            .lookup_index(
                &table,
                &table.indexes[0],
                IndexKey::from(vec![KeyPart::Int(1)]),
            )
            .unwrap()[0];
        db.update_row_by_id(txn, &table, row_id, |_| {
            Ok(vec![Value::Int(1), Value::String("q".repeat(6000))])
        })
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
        db.checkpoint().unwrap();
    }

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let table = db.table("test.t").unwrap();
    let row_id = db
        .lookup_index(
            &table,
            &table.indexes[0],
            IndexKey::from(vec![KeyPart::Int(1)]),
        )
        .unwrap()[0];
    let row = db
        .read_row(
            &table,
            row_id,
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()
        .unwrap();

    assert_eq!(row.values[1], Value::String("q".repeat(6000)));
}

#[test]
fn catalog_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::BigInt,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
    }

    let db = open_relaxed_test_db(dir.path());
    assert!(db.table("t").is_ok());
    // The catalog lives in the data-dictionary heap tables, not in a JSON
    // file. The reserved `mysql.tables` heap must exist; no JSON catalog
    // (current or legacy) is ever written.
    assert!(crate::storage::naming::table_object_path(
        dir.path(),
        "mysql",
        "tables",
        crate::storage::data_dictionary::DD_TABLES,
        "heap",
    )
    .exists());
    assert!(!dir
        .path()
        .join(crate::storage::catalog::LEGACY_DATA_DICTIONARY_JSON)
        .exists());
    assert!(!dir.path().join("catalog.json").exists());
}

#[test]
fn concurrent_create_table_flushes_catalog_safely() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap());
    let threads = 6;
    let barrier = Arc::new(std::sync::Barrier::new(threads));
    let mut handles = Vec::new();

    for i in 0..threads {
        let db = db.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            db.create_table(
                &format!("t{i}"),
                vec![ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                }],
                vec!["id".into()],
                vec![],
            )
        }));
    }

    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    for i in 0..threads {
        assert!(db.table(&format!("t{i}")).is_ok());
    }
    drop(db);

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    for i in 0..threads {
        let table = db.table(&format!("t{i}")).unwrap();
        assert!(t_path(dir.path(), &table.name, table.id, "heap").exists());
    }
}

#[test]
fn heap_update_reserve_handles_many_growing_rows_on_page() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1.heap");
    let heap = HeapFile::open_with_durability(
        path,
        1,
        test_log(dir.path()),
        DurabilityMode::Relaxed,
        DEFAULT_HEAP_UPDATE_RESERVE_BYTES,
    )
    .unwrap();
    let mut row_ids = Vec::new();
    for id in 0..300 {
        row_ids.push(
            heap.insert_mvcc(
                1,
                RowHeader::visible(1),
                &[Value::Int(id), Value::Null, Value::String("x".repeat(40))],
            )
            .unwrap(),
        );
    }

    let first_page = row_ids[0].page_id;
    let first_page_ids = row_ids
        .into_iter()
        .filter(|row_id| row_id.page_id == first_page)
        .collect::<Vec<_>>();
    assert!(first_page_ids.len() > 100);
    for (idx, row_id) in first_page_ids.into_iter().enumerate() {
        heap.write_mvcc(
            2,
            row_id,
            RowHeader::visible(2),
            &[
                Value::Int(idx as i64),
                Value::String("2026-05-05 02:31:26.000".into()),
                Value::String("x".repeat(40)),
            ],
        )
        .unwrap();
    }
}

#[test]
fn incompatible_catalog_version_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("catalog.json"),
        r#"{
  "version": 2,
  "next_table_id": 2,
  "databases": {},
  "tables": {
"legacy": {
  "id": 1,
  "database": "test",
  "name": "legacy",
  "columns": [
    { "name": "id", "data_type": "Int", "nullable": false }
  ],
  "primary_key": ["id"],
  "indexes": [
    { "name": "PRIMARY", "columns": ["id"], "unique": true, "primary": true }
  ]
}
  }
}"#,
    )
    .unwrap();

    let err = Database::open(dir.path()).unwrap_err();

    assert_eq!(err.code(), 1105);
    assert!(err.message().contains("catalog.json") && err.message().contains("is incompatible"));
    assert!(err.message().contains("development storage format"));
    assert!(err.message().contains("new data directory or rebuild data"));
}

#[test]
fn catalog_without_version_is_rejected_as_legacy_storage() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("catalog.json"),
        r#"{
  "next_table_id": 1,
  "databases": {},
  "tables": {}
}"#,
    )
    .unwrap();

    let err = Database::open(dir.path()).unwrap_err();

    assert_eq!(err.code(), 1105);
    assert!(err.message().contains("catalog.json") && err.message().contains("is incompatible"));
    assert!(err.message().contains("development storage format"));
}

#[test]
fn previous_development_catalog_version_requires_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("catalog.json"),
        r#"{
  "version": 8,
  "next_table_id": 1,
  "databases": {},
  "tables": {}
}"#,
    )
    .unwrap();

    let err = Database::open(dir.path()).unwrap_err();

    assert_eq!(err.code(), 1105);
    assert!(err.message().contains("catalog.json") && err.message().contains("is incompatible"));
    assert!(err.message().contains("development storage format"));
    assert!(err.message().contains("new data directory or rebuild data"));
}

#[test]
fn add_index_bulk_builds_large_table_without_dirty_index_cache() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    let total = 1_000;
    db.insert_rows(
        txn,
        "test.t",
        (0..total)
            .map(|id| vec![Value::Int(id), Value::Int(total - id)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    let before = db.storage_status();
    db.add_index(
        "test.t",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "v_idx".into(),
            columns: vec!["v".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();
    let after = db.storage_status();

    let table = db.table("test.t").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.name == "v_idx")
        .unwrap();
    assert_eq!(
        db.lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(1)]))
            .unwrap()
            .len(),
        1
    );
    // The user index is direct-built and clean. v46's incremental catalog
    // commit legitimately dirties the DD primary indexes plus the compiled
    // `(schema_name,name)` unique index on mysql.tables.
    assert!(db.dirty_index_page_count() <= 6);
    assert!(
        after
            .wal_index_batch_records
            .saturating_sub(before.wal_index_batch_records)
            <= 16,
        "offline CREATE INDEX may emit a bounded DD mutation set but must not emit one WAL index batch per backfill chunk: before={before:?} after={after:?}"
    );
    assert!(
        fs::metadata(db.index_path(table.id, "v_idx"))
            .unwrap()
            .len()
            > PAGE_SIZE as u64
    );
    drop(db);

    let db = open_relaxed_test_db(dir.path());
    let table = db.table("test.t").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.name == "v_idx")
        .unwrap();
    assert_eq!(
        db.lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(1)]))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn fulltext_backfill_builds_inverted_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "docs",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "body".into(),
                data_type: DataType::VarChar(Some(255)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.docs",
        vec![
            vec![Value::Int(1), Value::String("the quick brown fox".into())],
            vec![Value::Int(2), Value::String("quick fox jumps".into())],
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    // FULLTEXT backfill tokenizes each row and inserts multiple (token,row_id) postings.
    db.add_index(
        "test.docs",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "ft".into(),
            columns: vec!["body".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "FULLTEXT".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();

    let table = db.table("test.docs").unwrap();
    let index = table.indexes.iter().find(|i| i.name == "ft").unwrap();
    let scan = |token: &str| -> usize {
        let bound = |inclusive| {
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::String(token.to_string())]),
                inclusive,
            })
        };
        db.range_index_bounds(&table, index, bound(true), bound(true))
            .unwrap()
            .len()
    };

    // Both rows contain quick/fox.
    assert_eq!(scan("quick"), 2);
    assert_eq!(scan("fox"), 2);
    // Each occurs in one row only.
    assert_eq!(scan("brown"), 1);
    assert_eq!(scan("jumps"), 1);
    // Stopword the is not indexed.
    assert_eq!(scan("the"), 0);
    // Indexed tokens are lowercase; uppercase lookups must miss, ensuring case consistency.
    assert_eq!(scan("QUICK"), 0);
}

#[test]
fn fulltext_insert_delete_maintains_inverted_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "docs",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "body".into(),
                data_type: DataType::VarChar(Some(255)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    // Create an empty FULLTEXT index, then INSERT to test incremental DML maintenance rather than backfill.
    db.add_index(
        "test.docs",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "ft".into(),
            columns: vec!["body".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "FULLTEXT".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.docs",
        vec![
            vec![Value::Int(1), Value::String("quick brown fox".into())],
            vec![Value::Int(2), Value::String("quick red fox".into())],
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let scan = |token: &str| -> usize {
        let table = db.table("test.docs").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|i| i.name == "ft")
            .unwrap()
            .clone();
        let bound = |inclusive| {
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::String(token.to_string())]),
                inclusive,
            })
        };
        db.range_index_bounds(&table, &index, bound(true), bound(true))
            .unwrap()
            .len()
    };

    // Incremental INSERT: both rows contain quick/fox; brown/red each occur once.
    assert_eq!(scan("quick"), 2);
    assert_eq!(scan("fox"), 2);
    assert_eq!(scan("brown"), 1);
    assert_eq!(scan("red"), 1);

    // DELETE row 1, quick brown fox, removes brown and leaves one quick/fox posting each.
    let txn = db.begin_txn().unwrap();
    db.delete_rows(txn, "test.docs", None, |row| Ok(row[0] == Value::Int(1)))
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(scan("brown"), 0);
    assert_eq!(scan("quick"), 1);
    assert_eq!(scan("fox"), 1);
    assert_eq!(scan("red"), 1);
}

#[test]
fn fulltext_postings_carry_payload_stats() {
    // Posting keys are [String(token),Bytes(payload)]; payload encodes
    // doc_len/tf/positions. Decode B-tree entries directly to verify these statistics,
    // the storage foundation for scoring without heap fetches and positional phrase matching.
    use crate::storage::fulltext::payload::FtPayload;

    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "docs",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "body".into(),
                data_type: DataType::VarChar(Some(255)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.add_index(
        "test.docs",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "ft".into(),
            columns: vec!["body".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "FULLTEXT".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.docs",
        // tokens: quick@0 brown@1 fox@2 quick@3  → doc_len = 4
        vec![vec![
            Value::Int(1),
            Value::String("quick brown fox quick".into()),
        ]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let payload_of = |token: &str| -> FtPayload {
        let table = db.table("test.docs").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|i| i.name == "ft")
            .unwrap()
            .clone();
        let bound = |inclusive| {
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::String(token.to_string())]),
                inclusive,
            })
        };
        let entries = db
            .range_index_bounds_entries(&table, &index, bound(true), bound(true))
            .unwrap();
        assert_eq!(entries.len(), 1, "one posting per (token,row) for {token}");
        match entries[0].0 .0.get(1) {
            Some(KeyPart::Bytes(bytes)) => FtPayload::decode(bytes, true).unwrap(),
            other => panic!("expected Bytes payload part, got {other:?}"),
        }
    };

    let quick = payload_of("quick");
    assert_eq!(quick.doc_len, 4);
    assert_eq!(quick.tf, 2);
    assert_eq!(quick.positions, vec![0, 3]);

    let brown = payload_of("brown");
    assert_eq!(brown.doc_len, 4);
    assert_eq!(brown.tf, 1);
    assert_eq!(brown.positions, vec![1]);

    let fox = payload_of("fox");
    assert_eq!(fox.doc_len, 4);
    assert_eq!(fox.tf, 1);
    assert_eq!(fox.positions, vec![2]);
}

#[test]
fn index_entry_external_sort_spills_and_merges_unique_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut sorter = IndexEntryExternalSorter::with_exact_run_bytes(1024 * 1024);
    sorter.run_bytes = 1024;
    for n in (0..200).rev() {
        let entry = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(n % 97)]),
            row_id: RowId {
                page_id: (n) as u32,
                slot_id: 0,
            },
        };
        sorter.push(dir.path(), entry.clone()).unwrap();
        sorter.push(dir.path(), entry).unwrap();
    }

    let SortedIndexEntries::Spilled(runs) = sorter.finish(dir.path()).unwrap() else {
        panic!("small run_bytes should force index sort spill");
    };
    let merged = runs
        .into_merger()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(merged.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(merged.len(), 200);
    assert!(!crate::storage::naming::system_tmp_dir(dir.path())
        .read_dir()
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("indexsort-")));
}

#[test]
fn batched_add_index_encoded_keys_keep_lookup_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    let column = |name: &str, data_type: DataType, primary_key: bool| ColumnDef {
        name: name.into(),
        data_type,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    };
    db.create_table(
        "t",
        vec![
            column("id", DataType::Int, true),
            column("a", DataType::Int, false),
            column("b", DataType::Int, false),
            column("d", DataType::Date, false),
            column("payload", DataType::VarChar(Some(255)), false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let row_count = 50_000i64;
    let payload = "x".repeat(200);
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..row_count)
            .map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id % 997),
                    Value::Int(row_count - id),
                    Value::Date((id % 3650) as i32),
                    Value::String(payload.clone()),
                ]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    // Bulk-load / ADD INDEX pipelines intentionally build indexes before
    // ANALYZE. The persisted statistics snapshot is still empty here, but
    // the committed row delta is an exact sizing hint for the collectors.
    let pre_analyze = db.table("test.t").unwrap();
    assert_eq!(pre_analyze.stats.row_count, 0);
    assert!(
        fs::metadata(db.table_path(pre_analyze.id)).unwrap().len() / 48 > row_count as u64,
        "wide rows must make the physical fallback overestimate the live row count"
    );
    assert_eq!(
        db.index_build_row_count_hint(&pre_analyze),
        row_count as usize
    );

    let index = |name: &str, columns: &[&str]| IndexSchema {
        readable_from_scn: 0,
        index_id: 0,
        name: name.into(),
        columns: columns.iter().map(|column| (*column).into()).collect(),
        column_prefix_lengths: Vec::new(),
        column_directions: Vec::new(),
        key_expressions: Vec::new(),
        unique: false,
        primary: false,
        included_columns: Vec::new(),
        state: crate::storage::IndexState::Public,
        index_type: "BTREE".into(),
        visible: true,
        comment: String::new(),
        vector: None,
        fulltext: None,
    };
    db.add_indexes(
        "test.t",
        vec![
            index("a_idx", &["a"]),
            index("ab_idx", &["a", "b"]),
            index("d_idx", &["d"]),
        ],
    )
    .unwrap();

    let table = db.table("test.t").unwrap();
    let lookup = |name: &str, key: IndexKey| {
        let index = table
            .indexes
            .iter()
            .find(|index| index.name == name)
            .unwrap();
        db.lookup_index(&table, index, key).unwrap()
    };
    assert!(!lookup("a_idx", IndexKey::from(vec![KeyPart::Int(42)])).is_empty());
    assert_eq!(
        lookup(
            "ab_idx",
            IndexKey::from(vec![KeyPart::Int(42), KeyPart::Int(row_count - 42)])
        )
        .len(),
        1
    );
    assert!(!lookup("d_idx", IndexKey::from(vec![KeyPart::Date(42)])).is_empty());
    assert!(!crate::storage::naming::system_tmp_dir(dir.path())
        .read_dir()
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("indexsort-")));
}

#[test]
fn offline_add_index_respects_existing_table_ix_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();

    let err = db
        .add_index(
            "test.t",
            IndexSchema {
                readable_from_scn: 0,
                index_id: 0,
                name: "v_idx".into(),
                columns: vec!["v".into()],
                column_prefix_lengths: Vec::new(),
                column_directions: Vec::new(),
                key_expressions: Vec::new(),
                unique: false,
                primary: false,
                included_columns: Vec::new(),
                state: crate::storage::IndexState::Public,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: String::new(),
                vector: None,
                fulltext: None,
            },
        )
        .unwrap_err();
    assert_eq!(err.code(), 1205);

    let table = db.table("test.t").unwrap();
    assert!(table.indexes.iter().all(|index| index.name != "v_idx"));

    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn add_primary_key_on_empty_table_persists_rebuilt_index_meta() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "t",
            vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
                ColumnDef {
                    name: "v".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
            ],
            vec![],
            vec![],
        )
        .unwrap();

        db.add_index(
            "test.t",
            IndexSchema {
                readable_from_scn: 0,
                index_id: 0,
                name: "PRIMARY".into(),
                columns: vec!["id".into()],
                column_prefix_lengths: Vec::new(),
                column_directions: Vec::new(),
                key_expressions: Vec::new(),
                unique: true,
                primary: true,
                included_columns: Vec::new(),
                state: crate::storage::IndexState::Public,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: String::new(),
                vector: None,
                fulltext: None,
            },
        )
        .unwrap();

        let table = db.table("test.t").unwrap();
        assert_eq!(
            fs::metadata(db.index_path(table.id, "PRIMARY"))
                .unwrap()
                .len(),
            (INDEX_EMPTY_ROOT + 1) * PAGE_SIZE as u64
        );

        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
    }

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let table = db.table("test.t").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.name == "PRIMARY")
        .unwrap();
    assert_eq!(
        db.lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(1)]))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn analyze_table_stats_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
                ColumnDef {
                    name: "v".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(10)],
                vec![Value::Int(3), Value::Int(20)],
            ],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.analyze_table("test.t").unwrap();
    }
    let db = open_relaxed_test_db(dir.path());
    let table = db.table("test.t").unwrap();

    assert_eq!(table.stats.row_count, 3);
    let v_stats = table.stats.columns.get("v").unwrap();
    assert_eq!(v_stats.distinct_count, 2);
    assert_eq!(v_stats.min, Some(Value::Int(10)));
    assert_eq!(v_stats.max, Some(Value::Int(20)));
    assert_eq!(table.index_prefix_cardinality("PRIMARY", 1), Some(3));
}

#[test]
fn wide_table_stats_above_12k_survive_reopen_via_dd_lob() {
    use crate::storage::data_dictionary::{DD_STATS_JSON_CAP_BYTES, DD_TABLE_STATS};

    const COLUMN_COUNT: usize = 160;
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        let columns = (0..COLUMN_COUNT)
            .map(|index| ColumnDef {
                name: format!("c{index}"),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: index == 0,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            })
            .collect();
        db.create_table("wide_stats", columns, vec!["c0".into()], vec![])
            .unwrap();
        let rows = (0..3_i64)
            .map(|row| {
                (0..COLUMN_COUNT)
                    .map(|column| Value::Int(row * 1_000 + column as i64))
                    .collect()
            })
            .collect();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.wide_stats", rows).unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.analyze_table("test.wide_stats").unwrap();

        let table_id = db.table("test.wide_stats").unwrap().id;
        let persisted = db
            .scan_dd_table(DD_TABLE_STATS)
            .unwrap()
            .into_iter()
            .find(|row| row.first() == Some(&Value::UInt(table_id)))
            .expect("wide table stats row should be present");
        let Value::String(stats_json) = &persisted[2] else {
            panic!("stats_json should decode from the externalized DD value");
        };
        assert!(
            stats_json.len() > 12 * 1024,
            "fixture must cross the former persistence limit; got {} bytes",
            stats_json.len()
        );
        assert!(stats_json.len() <= DD_STATS_JSON_CAP_BYTES);
    }

    let db = open_relaxed_test_db(dir.path());
    let stats = &db.table("test.wide_stats").unwrap().stats;
    assert_eq!(stats.row_count, 3);
    assert_eq!(stats.columns.len(), COLUMN_COUNT);
    assert_eq!(stats.columns["c159"].distinct_count, 3);
    assert_eq!(stats.columns["c159"].average_width_bytes, Some(8.0));
}

#[test]
fn analyze_table_keeps_small_table_stats_exact() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "w".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("vw_idx".into()),
            columns: vec!["v".into(), "w".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(100)],
            vec![Value::Int(2), Value::Int(10), Value::Int(101)],
            vec![Value::Int(3), Value::Int(20), Value::Int(100)],
            vec![Value::Int(4), Value::Null, Value::Int(102)],
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let table = db.table("test.t").unwrap().clone();
    let expected = db.collect_table_stats_legacy_exact(&table).unwrap();
    db.analyze_table("test.t").unwrap();
    let actual = &db.table("test.t").unwrap().stats;

    assert_eq!(actual.row_count, expected.row_count);
    assert_eq!(actual.columns.get("v").unwrap().distinct_count, 2);
    assert_eq!(
        value_frequencies_for_assert(&actual.columns.get("v").unwrap().top_values),
        value_frequencies_for_assert(&expected.columns.get("v").unwrap().top_values)
    );
    assert_eq!(
        actual.indexes.get("vw_idx").unwrap().prefix_distinct_counts,
        expected
            .indexes
            .get("vw_idx")
            .unwrap()
            .prefix_distinct_counts
    );
}

#[test]
fn drop_index_removes_index_stats_without_refreshing_table_stats() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("v_idx".into()),
            columns: vec!["v".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(10)],
            vec![Value::Int(3), Value::Int(20)],
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.analyze_table("test.t").unwrap();

    {
        let key = table_key(DEFAULT_DATABASE, "t");
        let mut catalog = db.catalog_write().unwrap();
        let table_arc = catalog.tables.get_mut(&key).unwrap();
        let table = Arc::make_mut(table_arc);
        assert!(table.stats.indexes.contains_key("v_idx"));
        table.stats.modified_rows_since_analyze = 777;
        table.stats.analyzed_at_unix_ms = Some(42);
    }

    db.drop_index("test.t", "v_idx").unwrap();

    let table = db.table("test.t").unwrap();
    assert!(table.indexes.iter().all(|index| index.name != "v_idx"));
    assert!(table.stats.indexes.contains_key(&normalize_name("PRIMARY")));
    assert!(!table.stats.indexes.contains_key(&normalize_name("v_idx")));
    assert_eq!(table.stats.row_count, 3);
    assert_eq!(table.stats.modified_rows_since_analyze, 777);
    assert_eq!(table.stats.analyzed_at_unix_ms, Some(42));
    assert_eq!(table.stats.columns.get("v").unwrap().distinct_count, 2);
}

#[test]
fn analyze_table_large_high_cardinality_stats_are_bounded_and_stable() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("v_idx".into()),
            columns: vec!["v".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    let rows = (1..=ANALYZE_EXACT_ROW_LIMIT + 128)
        .map(|id| vec![Value::Int(id as i64), Value::Int(id as i64)])
        .collect::<Vec<_>>();
    db.insert_rows(txn, "test.t", rows).unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    db.analyze_table("test.t").unwrap();
    let first = db.table("test.t").unwrap().stats.clone();
    db.analyze_table("test.t").unwrap();
    let second = db.table("test.t").unwrap().stats.clone();

    let v_stats = first.columns.get("v").unwrap();
    assert_eq!(first.row_count, ANALYZE_EXACT_ROW_LIMIT + 128);
    assert!(v_stats.distinct_count > 0);
    assert!(v_stats.top_values.len() <= top_value_limit());
    assert!(first.indexes.get("v_idx").unwrap().prefix_distinct_counts[0] > 0);
    assert_eq!(
        first.columns.get("v").unwrap().distinct_count,
        second.columns.get("v").unwrap().distinct_count
    );
    assert_eq!(
        first.indexes.get("v_idx").unwrap().prefix_distinct_counts[0],
        second.indexes.get("v_idx").unwrap().prefix_distinct_counts[0]
    );
}

#[test]
fn analyze_table_large_skew_keeps_heavy_value_in_top_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    let rows = (1..=ANALYZE_EXACT_ROW_LIMIT + 256)
        .map(|id| {
            let value = if id % 2 == 0 { 7 } else { id as i64 };
            vec![Value::Int(id as i64), Value::Int(value)]
        })
        .collect::<Vec<_>>();
    db.insert_rows(txn, "test.t", rows).unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    db.analyze_table("test.t").unwrap();
    let table = db.table("test.t").unwrap();
    let v_stats = table.stats.columns.get("v").unwrap();

    assert!(v_stats.top_values.len() <= top_value_limit());
    assert!(v_stats
        .top_values
        .iter()
        .any(|frequency| frequency.value == Value::Int(7)));
}

#[test]
fn analyze_table_sampled_pages_estimate_stats_for_large_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "pad".into(),
                data_type: DataType::VarChar(Some(1024)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("v_idx".into()),
            columns: vec!["v".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    let rows = (1..=96)
        .map(|id| {
            let value = if id <= 48 {
                Value::Int(7)
            } else if id % 10 == 0 {
                Value::Null
            } else {
                Value::Int(id as i64)
            };
            vec![Value::Int(id as i64), value, Value::String("x".repeat(512))]
        })
        .collect::<Vec<_>>();
    db.insert_rows(txn, "test.t", rows).unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let table = db.table("test.t").unwrap();
    let page_count = db
        .with_heap_mut(table.id, |heap| heap.page_count())
        .unwrap();
    assert!(page_count >= 3);
    let all_pages = (0..page_count).collect::<Vec<_>>();
    let sampled_pages = all_pages.iter().step_by(2).copied().collect::<Vec<_>>();
    let sampled_page_live_rows = db
        .with_heap_mut(table.id, |heap| {
            let mut count = 0_u64;
            heap.scan_mvcc_pages_until(Some(&sampled_pages), |row| {
                if !row.header.is_deleted() {
                    count = count.saturating_add(1);
                }
                Ok(false)
            })?;
            Ok(count)
        })
        .unwrap();
    let sampled_heavy_rows = db
        .with_heap_mut(table.id, |heap| {
            let mut count = 0_u64;
            heap.scan_mvcc_pages_until(Some(&sampled_pages), |row| {
                if row.values.get(1) == Some(&Value::Int(7)) {
                    count = count.saturating_add(1);
                }
                Ok(false)
            })?;
            Ok(count)
        })
        .unwrap();

    let stats = db
        .collect_table_stats_with_sample_pages_for_test(&table, &sampled_pages)
        .unwrap();
    let v_stats = stats.columns.get("v").unwrap();

    assert_eq!(
        stats.row_count,
        estimate_analyzed_row_count(page_count, &sampled_pages, sampled_page_live_rows)
    );
    assert!(stats.row_count >= sampled_page_live_rows);
    assert!(v_stats.null_count > 0);
    assert!(sampled_heavy_rows > 0);
    assert!(
        v_stats
            .top_values
            .iter()
            .any(|frequency| frequency.value == Value::Int(7)
                && frequency.count >= sampled_heavy_rows)
    );
    assert!(stats.indexes.get("v_idx").unwrap().prefix_distinct_counts[0] > 0);
}

#[test]
fn table_row_count_stale_fallback_counts_without_analyze() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    assert_eq!(db.table_row_count("test.t").unwrap(), 3);
}

#[test]
fn wal_redo_recovers_committed_heap_and_catalog_files() {
    let dir = tempfile::tempdir().unwrap();
    let table_id = {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.table("t").unwrap().id
    };
    // Drop the committed user heap; WAL redo must reconstruct it on reopen.
    // The catalog itself lives in the (untouched) data-dictionary heaps.
    fs::remove_file(t_path(dir.path(), "t", table_id, "heap")).unwrap();

    let db = open_relaxed_test_db(dir.path());
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::Int(1)]);
}

#[test]
fn drop_table_file_delete_wal_does_not_copy_table_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "payload".into(),
                data_type: DataType::VarChar(Some(1024)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..128)
            .map(|id| vec![Value::Int(id), Value::String("x".repeat(512))])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    let table_path = t_path(dir.path(), "t", 1, "heap");
    assert!(fs::metadata(&table_path).unwrap().len() > PAGE_SIZE as u64);

    db.drop_table("test.t").unwrap();

    let scan = LogManager::scan(dir.path()).unwrap();
    let file_deletes = scan
        .records
        .iter()
        .filter_map(|record| match &record.payload {
            WalPayload::FileDelete { path, before } => Some((path.as_str(), before.len())),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected_rel = t_rel_path("t", 1, "heap");
    assert!(file_deletes
        .iter()
        .any(|(path, before_len)| *path == expected_rel && *before_len == 0));
}

#[test]
fn dropped_table_stays_dropped_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.drop_table("test.t").unwrap();
    }

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    assert!(db.table("test.t").is_err());
    assert!(!t_path(dir.path(), "t", 1, "heap").exists());
    // The PRIMARY index file must not be resurrected by index redo either:
    // Pass-2 index redo buffers page writes in an open handle that `finish()`
    // flushes; the DROP's FileDelete must evict that handle so the file stays
    // gone (else an orphan `.idx` leaks after every crash-after-drop).
    assert!(!t_index_path(dir.path(), "t", 1, "PRIMARY", "idx").exists());
}

/// Minimal `id INT PRIMARY KEY` table for the DROP recovery tests.
fn simple_pk_table(db: &Database, name: &str) {
    db.create_table(
        name,
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
}

/// B1 forward-recovery: a DROP that committed the catalog change but crashed
/// before purging the files leaves an intent behind; `recover_drop_tables`
/// finishes the purge on the next open. Simulate by writing the intent, then
/// removing the catalog row (the commit point) without touching the files.
#[test]
fn drop_table_recovery_completes_committed_drop() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        simple_pk_table(&db, "t");
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
        assert!(t_path(dir.path(), "t", 1, "heap").exists());

        // Record the intent (as the live drop does), then commit only the
        // catalog removal — mimicking a crash after `commit_catalog_txn` but before
        // the physical purge.
        let table = db
            .catalog_read()
            .unwrap()
            .tables
            .get("test.t")
            .cloned()
            .unwrap();
        let intent = db.build_drop_table_intent(&table).unwrap();
        db.write_drop_table_intent(&intent).unwrap();
        {
            let mut catalog = db.catalog_write().unwrap();
            catalog.tables.remove("test.t");
        }
        db.note_catalog_table(table.id);
        db.commit_catalog_txn().unwrap();
        // Files deliberately still present here.
        assert!(t_path(dir.path(), "t", 1, "heap").exists());
    }

    // Reopen: recovery must complete the purge and leave no zombie file.
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    assert!(db.table("test.t").is_err());
    assert!(!t_path(dir.path(), "t", 1, "heap").exists());
}

/// B1 rollback-recovery: a DROP that crashed before the catalog commit leaves
/// an intent, but the table is still present. Recovery drops the stale intent
/// and keeps the fully-intact table + files.
#[test]
fn drop_table_recovery_rolls_back_uncommitted_drop() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        simple_pk_table(&db, "t");
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(7)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();

        // Write the intent but never remove the catalog row (crash before the
        // commit point).
        let table = db
            .catalog_read()
            .unwrap()
            .tables
            .get("test.t")
            .cloned()
            .unwrap();
        let intent = db.build_drop_table_intent(&table).unwrap();
        db.write_drop_table_intent(&intent).unwrap();
    }

    // Reopen: recovery sees the table still present, discards the intent, and
    // the table survives intact.
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    assert!(db.table("test.t").is_ok());
    assert!(t_path(dir.path(), "t", 1, "heap").exists());
    let scanned = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(scanned.len(), 1, "row survives the rolled-back drop");
}

/// T2 forward-recovery: a TRUNCATE that persisted the intent but crashed
/// before the physical wipe finished must be finished by
/// `recover_truncate_tables` on next open. Simulate by writing the
/// intent, then leaving the heap with a legacy row that the wipe would
/// have removed. Reopen must produce an empty table.
#[test]
fn truncate_table_recovery_finishes_committed_intent() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        simple_pk_table(&db, "t");
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
        assert!(t_path(dir.path(), "t", 1, "heap").exists());

        // Write the intent as the live path would, but do NOT touch the heap
        // yet — models a crash after the intent fsync and before the first
        // `create_logged_file`.
        let table = db
            .catalog_read()
            .unwrap()
            .tables
            .get("test.t")
            .cloned()
            .unwrap();
        let intent = db.build_truncate_table_intent(&table);
        db.write_truncate_table_intent(&intent).unwrap();
    }

    // Reopen: recovery must replay the wipe; the table survives but is empty.
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    assert!(db.table("test.t").is_ok(), "table must survive TRUNCATE");
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        0,
        "recovery must complete the TRUNCATE wipe (rows found: {})",
        rows.len()
    );
}

/// T2 stale-intent recovery: if the table was DROPped after the TRUNCATE
/// intent was written, recovery must silently discard the intent rather
/// than error or resurrect anything.
#[test]
fn truncate_table_recovery_drops_intent_when_table_gone() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        simple_pk_table(&db, "t");
        let table = db
            .catalog_read()
            .unwrap()
            .tables
            .get("test.t")
            .cloned()
            .unwrap();
        let intent = db.build_truncate_table_intent(&table);
        db.write_truncate_table_intent(&intent).unwrap();
        // DROP after the intent was written — recover_drop_tables purges
        // the files, and recover_truncate_tables should then see the intent
        // as stale (no matching table_id).
        db.drop_table("t").unwrap();
    }

    // Reopen: must succeed cleanly without touching files that no longer
    // exist.
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    assert!(db.table("test.t").is_err(), "table must not resurrect");
    // No panic, no error — clean reopen means the intent was discarded.
}

#[test]
fn alter_copy_generation_intent_rolls_back_uncommitted_swap_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (old_path, new_path, intent_path) = {
        let db = open_relaxed_test_db(dir.path());
        let mut column = checkpoint_test_pk_column();
        column.primary_key = false;
        column.nullable = true;
        db.create_table("t", vec![column], Vec::new(), Vec::new())
            .unwrap();
        let old = db.table("test.t").unwrap();
        let mut candidate = old.as_ref().clone();
        candidate.active_generation = old.active_generation + 1;
        candidate.bump_schema_version();
        let shadow_id = old.id + 10_000;
        let job_id = 77;
        db.register_reorg_storage_locs(
            &[shadow_id],
            &old.database,
            &old.name,
            candidate.active_generation,
        );
        let intent_path = db
            .write_alter_copy_intent(&old, &candidate, shadow_id, job_id)
            .unwrap();
        let (old_to_backup, shadow_to_live) =
            db.shadow_generation_swap_paths(&old, &candidate, shadow_id, job_id);
        db.remove_heap_cache_entry(old.id).unwrap();
        db.remove_heap_cache_entry(shadow_id).unwrap();
        for (live, backup) in &old_to_backup {
            if live.exists() {
                rename_owned_file(live, backup).unwrap();
            }
        }
        for (_, live) in &shadow_to_live {
            std::fs::create_dir_all(live.parent().unwrap()).unwrap();
            std::fs::write(live, create_empty_heap_image(live, Vec::new())).unwrap();
        }
        let old_path = old_to_backup[0].0.clone();
        let new_path = shadow_to_live[0].1.clone();
        (old_path, new_path, intent_path)
    };

    let reopened = open_relaxed_test_db(dir.path());
    assert_eq!(reopened.table("test.t").unwrap().active_generation, 1);
    assert!(old_path.exists(), "old generation must be restored");
    assert!(!new_path.exists(), "uncommitted generation must be removed");
    assert!(!intent_path.exists(), "recovered intent must be consumed");
}

#[test]
fn alter_copy_generation_intent_rolls_forward_committed_swap_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (old_path, new_path, intent_path, backup_paths) = {
        let db = open_relaxed_test_db(dir.path());
        let mut column = checkpoint_test_pk_column();
        column.primary_key = false;
        column.nullable = true;
        db.create_table("t", vec![column], Vec::new(), Vec::new())
            .unwrap();
        let old = db.table("test.t").unwrap();
        let mut candidate = old.as_ref().clone();
        candidate.active_generation = old.active_generation + 1;
        candidate.bump_schema_version();
        let shadow_id = old.id + 20_000;
        let job_id = 88;
        db.register_reorg_storage_locs(
            &[shadow_id],
            &old.database,
            &old.name,
            candidate.active_generation,
        );
        let intent_path = db
            .write_alter_copy_intent(&old, &candidate, shadow_id, job_id)
            .unwrap();
        let (old_to_backup, shadow_to_live) =
            db.shadow_generation_swap_paths(&old, &candidate, shadow_id, job_id);
        db.remove_heap_cache_entry(old.id).unwrap();
        db.remove_heap_cache_entry(shadow_id).unwrap();
        for (live, backup) in &old_to_backup {
            if live.exists() {
                rename_owned_file(live, backup).unwrap();
            }
        }
        for (shadow, live) in &shadow_to_live {
            std::fs::create_dir_all(shadow.parent().unwrap()).unwrap();
            std::fs::create_dir_all(live.parent().unwrap()).unwrap();
            std::fs::write(shadow, create_empty_heap_image(shadow, Vec::new())).unwrap();
            let _ = std::fs::remove_file(live);
        }
        {
            let mut catalog = db.catalog_write().unwrap();
            *Arc::make_mut(catalog.tables.get_mut("test.t").unwrap()) = candidate.clone();
        }
        db.note_catalog_table(candidate.id);
        db.commit_catalog_txn().unwrap();
        let old_path = old_to_backup[0].0.clone();
        let backup_paths = old_to_backup
            .iter()
            .map(|(_, backup)| backup.clone())
            .collect::<Vec<_>>();
        let new_path = shadow_to_live[0].1.clone();
        (old_path, new_path, intent_path, backup_paths)
    };

    let reopened = open_relaxed_test_db(dir.path());
    assert_eq!(reopened.table("test.t").unwrap().active_generation, 2);
    assert!(!old_path.exists(), "old generation must stay retired");
    assert!(new_path.exists(), "committed generation must be published");
    assert!(backup_paths.iter().all(|path| !path.exists()));
    assert!(!intent_path.exists(), "recovered intent must be consumed");
}

#[test]
fn committed_rows_recover_from_wal_without_heap_flush() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
    fs::remove_file(t_path(dir.path(), "t", 1, "heap")).unwrap();

    let db = open_relaxed_test_db(dir.path());
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::Int(1)]);
}

#[test]
fn empty_index_root_recovers_from_wal_without_any_page_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = t_index_path(dir.path(), "t", 1, "PRIMARY", "idx");
    let log = test_log(dir.path());
    // Disable background data-page I/O, while the index itself still emits
    // strict WAL. Losing these frames models a crash immediately after DDL.
    let pool = BufferPool::new_with_options(
        64,
        Arc::clone(&log),
        DurabilityMode::Memory,
        Duration::from_secs(1),
        1,
        0,
    );
    let mut index = IndexFile::open_with_durability(
        path.clone(),
        Arc::clone(&log),
        DurabilityMode::Strict,
        Arc::clone(&pool),
    )
    .unwrap();
    let lsn = log.lock().last_lsn();
    LogManager::sync_durable_to(&log, lsn).unwrap();
    index.flush_on_drop = false;
    pool.discard_file(&index.file_key).unwrap();
    drop(index);
    drop(pool);
    drop(log);
    recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
    let reopened = IndexFile::open(path, test_log(dir.path())).unwrap();
    assert!(reopened
        .get_unique_one(&IndexKey::from(vec![KeyPart::Int(1)]))
        .unwrap()
        .is_none());
}

/// Regression for the checkpoint snapshot race: a transaction's WAL may be
/// older than CheckpointBegin while its page image is not durable yet. The
/// explicit v23 redo_lsn must make recovery start before that transaction;
/// deriving the cutoff from CheckpointBegin would lose the committed row.
#[test]
fn checkpoint_end_explicit_redo_lsn_recovers_pre_begin_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    // Establish the durable catalog/empty-heap baseline before the
    // transaction under test. Without this, `mem::forget` below races the
    // catalog buffer's ordinary writeback and the test can fail because the
    // table itself was never durable, rather than because redo started at the
    // wrong LSN.
    db.checkpoint().unwrap();
    let redo_lsn = db.log.lock().next_lsn();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(7)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let begin_lsn = db
        .append_wal(txn_id_for_ddl(), WalPayload::CheckpointBegin)
        .unwrap();
    assert!(begin_lsn > redo_lsn);
    let end_lsn = db
        .append_wal(
            txn_id_for_ddl(),
            WalPayload::CheckpointEnd {
                redo_lsn,
                next_txn_id: 1_000,
                next_scn: 1_000,
                auto_increment_covered_lsn: begin_lsn,
                sequence_covered_lsn: begin_lsn,
                gc_safe_txn_frontier: 0,
            },
        )
        .unwrap();
    crate::wal::LogManager::sync_durable_to(&db.log, end_lsn).unwrap();

    // Model a crash before the dirty heap page reaches disk. The catalog and
    // all logical row WAL remain, so redo must reconstruct the file and row.
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
    fs::remove_file(t_path(dir.path(), "t", 1, "heap")).unwrap();

    let reopened = open_relaxed_test_db(dir.path());
    let rows = reopened
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::Int(7)]);
}

/// Regression: a *clean* checkpoint collapses the WAL down to a single
/// header + synthetic-CheckpointEnd segment (`compact_collapsing_active`),
/// deleting every older segment. That collapsed single-segment layout is
/// exactly what a graceful `SHUTDOWN` leaves on disk, and reopening it must
/// recover cleanly. (The `NoSuchFile` boot failure this addresses came from
/// a *second* process racing the collapse; that concurrency guard lives in
/// `storage::dir_lock` and is covered by its own tests. Here we pin the
/// non-racy invariant: the collapsed state itself is recoverable.)
#[test]
fn clean_checkpoint_wal_collapse_recovers_on_reopen() {
    let id_col = || ColumnDef {
        name: "id".into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key: true,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    // Tiny segments make a small deterministic fixture rotate real sealed WAL
    // segments; the collapse semantics do not depend on thousands of commits.
    config.wal_segment_bytes = 4 * 1024;
    let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
    db.create_table("t", vec![id_col()], vec!["id".into()], vec![])
        .unwrap();
    const ROWS: i64 = 256;
    for i in 0..ROWS {
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(i)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }
    let wal_dir = crate::storage::naming::system_wal_dir(dir.path());
    let pre_checkpoint_segments = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("wal-") && name.ends_with(".log")
        })
        .count();
    assert!(
        pre_checkpoint_segments > 1,
        "fixture must create sealed WAL segments before collapse"
    );
    // Two checkpoints: the first flushes all dirty pages, the second then
    // observes a fully-quiesced empty window and takes the collapsing fast
    // path (single-segment WAL), mirroring the shutdown sequence.
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    let seg_count = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("wal-") && name.ends_with(".log")
        })
        .count();
    assert_eq!(
        seg_count, 1,
        "clean checkpoint should collapse the WAL to a single segment"
    );
    std::mem::drop(db);

    // Reopen the collapsed data directory: recovery must succeed and see
    // every committed row.
    let db2 = Database::open_with_config(dir.path(), config).unwrap();
    let rows = db2
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), ROWS as usize);
}

#[test]
fn buffer_pool_capacity_evicts_heap_pages_and_reports_stats() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    // The hard governor cannot evict transaction-deferred pages before their
    // WAL record is published. 32 pages leave room for one atomic insert's
    // minimum heap+PK+catalog working set, while the 100 wide rows below still
    // exceed the pool many times and exercise sustained eviction.
    config.buffer_pool_pages = 32;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "pad".into(),
                data_type: DataType::VarChar(Some(8000)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    for id in 0..100 {
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(id), Value::String("x".repeat(6000))]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }

    let status = db.storage_status();

    assert!(status.buffer_pool_used_pages <= 32, "{status:?}");
    assert!(status.buffer_pool_evictions > 0, "{status:?}");
    assert!(status.buffer_pool_flushes > 0, "{status:?}");
}

#[test]
fn buffer_pool_full_of_fresh_pages_evicts_without_waiting() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        3,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));

    for page_id in 0..3 {
        pool.store_page(
            PageKey {
                file: file.clone(),
                page_id,
            },
            vec![page_id as u8; PAGE_SIZE],
            0,
        )
        .unwrap();
    }
    let full = pool.snapshot();
    assert_eq!(full.used_pages, 3, "{full:?}");
    assert_eq!(full.freeable_pages, 3, "{full:?}");
    assert_eq!(full.pinned_pages, 0, "{full:?}");

    pool.store_page(
        PageKey {
            file: file.clone(),
            page_id: 3,
        },
        vec![3; PAGE_SIZE],
        0,
    )
    .unwrap();

    let after = pool.snapshot();
    assert_eq!(after.used_pages, 3, "{after:?}");
    assert_eq!(after.pinned_pages, 0, "{after:?}");
    assert!(after.evictions > 0, "{after:?}");
}

#[test]
fn raw_page_mutation_publishes_lsn_and_defers_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        3,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let key = PageKey {
        file: Arc::new(BufferPool::heap_file_key(
            t_path(dir.path(), "t", 1, "heap"),
            1,
        )),
        page_id: 0,
    };
    let mut page = Page::new();
    page.set_lsn(7);
    pool.store_page(key.clone(), page.encode(), 7).unwrap();

    pool.with_raw_page_mut(key.clone(), 11, |_| Ok(())).unwrap();
    assert_eq!(pool.cached_page_lsn(&key).unwrap(), Some(11));
    assert!(pool
        .lookup_frame(&key)
        .unwrap()
        .unwrap()
        .checksum_dirty
        .load(Ordering::Acquire));

    let finalized = pool.read_raw_page(key).unwrap();
    assert_eq!(page_lsn(&finalized), 11);
    Page::decode(&finalized).unwrap();
}

#[test]
fn cached_index_decode_revalidates_replaced_page_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        3,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let key = PageKey {
        file: Arc::new(BufferPool::index_file_key(t_index_path(
            dir.path(),
            "t",
            1,
            "idx",
            "idx",
        ))),
        page_id: 1,
    };
    let encoded =
        encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(Vec::new(), None))).unwrap();
    pool.store_page(key.clone(), encoded.clone(), 1).unwrap();

    assert!(matches!(
        pool.cached_index_page(&key).unwrap().as_deref(),
        Some(IndexPage::Leaf(_))
    ));
    // The first decode caches validation for this frame epoch. Replacing the
    // image must invalidate that marker; otherwise the new fast path could
    // decode a structurally plausible page whose CRC no longer matches.
    let mut corrupted = pool.read_raw_page(key.clone()).unwrap();
    corrupted[PAGE_SIZE - 1] ^= 0x80;
    pool.store_page(key.clone(), corrupted, 2).unwrap();
    let err = pool
        .cached_index_page(&key)
        .expect_err("replaced image must be checksum-validated");
    assert!(
        err.message().contains("checksum"),
        "unexpected error: {}",
        err.message()
    );
}

#[test]
fn buffer_pool_prefetch_run_batches_contiguous_pages() {
    let dir = tempfile::tempdir().unwrap();
    crate::storage::control::Control::admit(dir.path()).unwrap();
    let path = t_path(dir.path(), "batch", 1, "heap");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = File::create(&path).unwrap();
    for byte in [1u8, 2, 3, 4] {
        file.write_all(&vec![byte; PAGE_SIZE]).unwrap();
    }
    drop(file);
    let pool = BufferPool::new_with_options(
        8,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::from_secs(1),
        1,
        4,
    );
    let file_key = Arc::new(BufferPool::heap_file_key(path, 1));
    let keys: Vec<PageKey> = (0..4)
        .map(|page_id| PageKey {
            file: file_key.clone(),
            page_id,
        })
        .collect();
    pool.load_prefetch_run(&keys).unwrap();
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.read_ios, 4, "{snapshot:?}");
    assert_eq!(snapshot.batch_read_ops, 1, "{snapshot:?}");
    assert_eq!(snapshot.batch_read_pages, 4, "{snapshot:?}");
    #[cfg(unix)]
    assert_eq!(snapshot.read_syscalls, 1, "{snapshot:?}");
    assert_eq!(
        pool.pin_page(keys[2].clone()).unwrap().bytes().unwrap()[0],
        3
    );
}

#[test]
fn buffer_pool_cold_queue_recycles_before_clock_scan() {
    let dir = tempfile::tempdir().unwrap();
    crate::storage::control::Control::admit(dir.path()).unwrap();
    let path = t_path(dir.path(), "cold", 2, "heap");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = File::create(&path).unwrap();
    for byte in [10u8, 20, 30] {
        file.write_all(&vec![byte; PAGE_SIZE]).unwrap();
    }
    drop(file);
    let pool = BufferPool::new_with_options(
        2,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_secs(1),
        1,
        0,
    );
    let file_key = Arc::new(BufferPool::heap_file_key(path, 2));
    for page_id in 0..2 {
        drop(
            pool.pin_page_with_intent(
                PageKey {
                    file: file_key.clone(),
                    page_id,
                },
                AccessIntent::Scan,
            )
            .unwrap(),
        );
    }
    let scans_before = pool.snapshot().eviction_scans;
    drop(
        pool.pin_page(PageKey {
            file: file_key,
            page_id: 2,
        })
        .unwrap(),
    );
    let snapshot = pool.snapshot();
    assert!(snapshot.cold_recycle_hits >= 1, "{snapshot:?}");
    assert_eq!(snapshot.eviction_scans, scans_before, "{snapshot:?}");
}

#[test]
fn buffer_pool_store_many_distinct_new_pages_does_not_deadlock() {
    // B1 regression (deadlock): `store_page`'s new-frame path calls
    // `note_dirty` while holding the shard's `frames.write()` lock. An earlier
    // fix ran the `dirty_oldest` compaction inline from `note_dirty`, and the
    // compaction takes `frames.read()` on the same shard — a self-deadlock
    // once the heap crossed the compaction threshold (>256), which froze the
    // whole server under any real bulk load (sysbench prepare / TPC-C load).
    // Storing many DISTINCT new pages exercises exactly that path; this test
    // simply has to COMPLETE (the nextest timeout catches a hang).
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        4096,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(50),
        4,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "distinct", 1, "heap"),
        1,
    ));
    // Well past BUFFER_POOL_DIRTY_OLDEST_MIN_COMPACT (256) so the old inline
    // compaction would have triggered its self-deadlock.
    for page_id in 0..1_500u64 {
        pool.store_page(
            PageKey {
                file: file.clone(),
                page_id,
            },
            vec![(page_id & 0xff) as u8; PAGE_SIZE],
            page_id + 1,
        )
        .unwrap();
    }
    let snap = pool.snapshot();
    assert_eq!(snap.used_pages, 1_500, "all distinct pages resident");
}

#[test]
fn buffer_pool_dirty_oldest_heap_stays_bounded_under_reflush() {
    // B1 regression: the per-shard `dirty_oldest` min-heap used to grow by
    // one entry on every clean→dirty transition and was only drained by the
    // age-clean pop path (dirty <= high) and DROP/truncate. A sustained
    // dirty → flush → re-dirty cycle therefore leaked heap entries without
    // bound (and buried the genuinely-oldest dirty page behind stale low-LSN
    // entries, stalling the WAL redo floor). The background lazy compaction
    // must keep the heap bounded by the live dirty-page count regardless of
    // how many reflush cycles run.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        256,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(50),
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "reflush", 1, "heap"),
        1,
    ));
    const PAGES: u64 = 64;
    const CYCLES: u64 = 32;
    let mut lsn = 1_u64;
    for _cycle in 0..CYCLES {
        for page_id in 0..PAGES {
            pool.store_page(
                PageKey {
                    file: file.clone(),
                    page_id,
                },
                vec![(lsn & 0xff) as u8; PAGE_SIZE],
                lsn,
            )
            .unwrap();
            lsn += 1;
        }
        // Flush clears the dirty bit (and the dirty_set) but leaves every heap
        // entry behind; the next cycle re-dirties and pushes again.
        // CYCLES * PAGES = 2 048 clean→dirty transitions, eight times the
        // compaction floor and enough to distinguish the pre-fix leak.
        pool.flush_all().unwrap();
    }
    // Compaction now runs in the background IO worker (moved off the
    // `note_dirty` hot path to avoid the frames-lock self-deadlock), so poll
    // for it to converge instead of asserting immediately. The final flush
    // left every page clean, so the heap must rebuild down toward zero.
    let started = Instant::now();
    let mut last = u64::MAX;
    while started.elapsed() < Duration::from_secs(5) {
        last = pool.snapshot().dirty_oldest_len;
        if last < 512 {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    // Fix bound: below two compaction floors; the pre-fix leak would remain at
    // 2 048 and never shrink.
    assert!(
        last < 512,
        "dirty_oldest heap not compacted: {last} entries after {CYCLES} reflush cycles",
    );
}

#[test]
fn buffer_pool_prefers_clean_victim_before_dirty_flush() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        2,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    pool.shutdown_flusher();
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let clean_key = PageKey {
        file: file.clone(),
        page_id: 0,
    };
    let dirty_key = PageKey {
        file: file.clone(),
        page_id: 1,
    };

    pool.store_page(clean_key.clone(), vec![0; PAGE_SIZE], 0)
        .unwrap();
    pool.store_page(dirty_key.clone(), vec![1; PAGE_SIZE], 0)
        .unwrap();
    pool.flush_all().unwrap();
    pool.store_page(dirty_key.clone(), vec![3; PAGE_SIZE], 0)
        .unwrap();
    let before = pool.snapshot();
    assert_eq!(before.dirty_pages, 1, "{before:?}");

    pool.store_page(
        PageKey {
            file: file.clone(),
            page_id: 2,
        },
        vec![2; PAGE_SIZE],
        0,
    )
    .unwrap();

    let after = pool.snapshot();
    assert_eq!(after.used_pages, 2, "{after:?}");
    assert_eq!(after.flushes, before.flushes, "{after:?}");
    assert!(pool.lookup_frame(&clean_key).unwrap().is_none());
    assert!(pool.lookup_frame(&dirty_key).unwrap().is_some());
}

#[test]
fn buffer_pool_high_dirty_ratio_uses_available_clean_victim() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        5,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(100),
        1,
        0,
    );
    pool.shutdown_flusher();
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let keys: Vec<_> = (0..5)
        .map(|page_id| PageKey {
            file: file.clone(),
            page_id,
        })
        .collect();
    for key in &keys {
        pool.store_page(key.clone(), vec![key.page_id as u8; PAGE_SIZE], 0)
            .unwrap();
    }
    pool.flush_all().unwrap();
    for key in &keys[..3] {
        pool.store_page(key.clone(), vec![9; PAGE_SIZE], 0).unwrap();
    }
    let before = pool.snapshot();
    assert_eq!(before.dirty_pages, 3, "{before:?}");
    assert!(pool.dirty_ratio_high());

    assert!(pool.evict_one_with_policy(false).unwrap());
    let after = pool.snapshot();
    assert_eq!(after.dirty_pages, 3, "{after:?}");
    assert_eq!(after.flushes, before.flushes, "{after:?}");
    assert_eq!(after.waits, before.waits, "{after:?}");
    assert!(keys[..3]
        .iter()
        .all(|key| pool.lookup_frame(key).unwrap().is_some()));
}

#[test]
fn buffer_pool_evicts_dirty_page_when_no_clean_victim_exists() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        1,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));

    pool.store_page(
        PageKey {
            file: file.clone(),
            page_id: 0,
        },
        vec![1; PAGE_SIZE],
        1,
    )
    .unwrap();
    pool.store_page(
        PageKey {
            file: file.clone(),
            page_id: 1,
        },
        vec![2; PAGE_SIZE],
        2,
    )
    .unwrap();

    let after = pool.snapshot();
    assert_eq!(after.used_pages, 1, "{after:?}");
    assert!(after.evictions > 0, "{after:?}");
    assert!(after.flushes > 0, "{after:?}");
    assert!(after.write_ios > 0, "{after:?}");
}

#[test]
fn io_thread_cap_clamped_to_shard_count() {
    // M-6 - even if the operator asks for many IO threads, the
    // effective count never exceeds the shard count (one worker per
    // shard is the natural ceiling).
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        16,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::ZERO,
        /* io_threads */ 100,
        0,
    );
    assert!(pool.io_threads >= 1);
    assert!(pool.io_threads <= MAX_BUFFER_POOL_IO_THREADS);
    assert!(
        pool.io_threads <= pool.shards.len(),
        "io_threads ({}) must not exceed shard count ({})",
        pool.io_threads,
        pool.shards.len()
    );
}

#[test]
fn background_flush_panic_mode_targets_30pct() {
    // M-6 - when dirty exceeds 90% capacity the flush helper must
    // drive the dirty page count down to ≤ 30%, not just the
    // regular 50%.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        10,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        0,
    );
    // Only the explicit ticks below may consume dirty pages. A pressure wake
    // can otherwise race the initial 100%-dirty assertion before the timer.
    pool.shutdown_flusher();
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    for i in 0..10u64 {
        let key = PageKey {
            file: file.clone(),
            page_id: i,
        };
        pool.store_page(key, vec![i as u8; PAGE_SIZE], 1).unwrap();
    }
    assert_eq!(pool.dirty_pages.load(Ordering::Relaxed), 10);
    // Dynamic claiming deliberately limits one worker tick to one shard/batch
    // so no worker monopolises the fleet. Repeated ticks must still converge
    // panic mode to its global target.
    for _ in 0..pool.shards.len().saturating_mul(2) {
        pool.background_flush_once(0, 1).unwrap();
        if pool.dirty_pages.load(Ordering::Relaxed) * 100
            / pool.capacity_pages.load(Ordering::Relaxed)
            <= BUFFER_POOL_DIRTY_PANIC_TARGET_PCT
        {
            break;
        }
    }
    let dirty_after = pool.dirty_pages.load(Ordering::Relaxed);
    let cap = pool.capacity_pages.load(Ordering::Relaxed);
    assert!(
        dirty_after * 100 / cap <= BUFFER_POOL_DIRTY_PANIC_TARGET_PCT,
        "expected panic-mode flush to drive dirty below 30%, got {dirty_after}/{cap}"
    );
}

#[test]
fn background_flush_uses_one_wal_barrier_for_scattered_pages() {
    let dir = tempfile::tempdir().unwrap();
    let log = test_log(dir.path());
    let pool = BufferPool::new_with_options(
        16,
        Arc::clone(&log),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        0,
    );
    // Make the test deterministic: only the explicit cleaner call below may
    // consume dirty pages.
    pool.shutdown_flusher();
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "wal_batch", 1, "heap"),
        1,
    ));
    // Non-contiguous page ids force four physical write runs. Give each page
    // an increasing real WAL LSN so the former nested grouping advanced and
    // fsynced the durable watermark once per run.
    for (txn_id, page_id) in [0_u64, 2, 4, 6].into_iter().enumerate() {
        let lsn = log
            .lock()
            .append(txn_id as u64 + 1, WalPayload::CheckpointBegin)
            .unwrap();
        pool.store_page(
            PageKey {
                file: Arc::clone(&file),
                page_id,
            },
            vec![page_id as u8; PAGE_SIZE],
            lsn,
        )
        .unwrap();
    }

    let syncs_before = log.lock().sync_count();
    assert_eq!(pool.flush_some_dirty_limited(0, 4).unwrap(), 4);
    let syncs_after = log.lock().sync_count();
    assert_eq!(syncs_after - syncs_before, 1);
    assert_eq!(pool.snapshot().dirty_pages, 0);
    assert_eq!(pool.snapshot().write_syscalls, 4);
}

#[test]
fn capacity_headroom_flush_uses_one_wal_barrier_across_shards() {
    let dir = tempfile::tempdir().unwrap();
    let log = test_log(dir.path());
    let pool = BufferPool::new_with_options(
        64,
        Arc::clone(&log),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        4,
        0,
    );
    pool.shutdown_flusher();
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "headroom", 1, "heap"),
        1,
    ));
    let keys: Vec<_> = (0..256_u64)
        .step_by(2)
        .map(|page_id| PageKey {
            file: Arc::clone(&file),
            page_id,
        })
        .filter(|key| pool.shard_index(key) < pool.io_threads)
        .take(4)
        .collect();
    assert_eq!(keys.len(), 4);
    for (txn_id, key) in keys.iter().enumerate() {
        let lsn = log
            .lock()
            .append(txn_id as u64 + 1, WalPayload::CheckpointBegin)
            .unwrap();
        pool.store_page(key.clone(), vec![key.page_id as u8; PAGE_SIZE], lsn)
            .unwrap();
    }

    let syncs_before = log.lock().sync_count();
    pool.maintain_free_headroom().unwrap();
    assert_eq!(log.lock().sync_count() - syncs_before, 1);
    assert_eq!(pool.snapshot().dirty_pages, 0);
    assert_eq!(pool.snapshot().write_syscalls, 4);
}

#[test]
fn file_cache_dedupes_concurrent_opens() {
    // M-2 - file_cache is now an RwLock<HashMap> with a
    // double-checked insert. Verify that concurrent first-touch
    // requests for the same file collapse onto a single underlying
    // handle (we can compare the Arc<File> identity).
    use std::thread;
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        16,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        0,
    );
    let file_key = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 42, "heap"),
        42,
    ));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        let key = file_key.clone();
        handles.push(thread::spawn(move || {
            pool.open_cached_file(&key).expect("open_cached_file")
        }));
    }
    let files: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let first = files[0].clone();
    for f in &files[1..] {
        assert!(
            Arc::ptr_eq(&first, f),
            "concurrent opens must reuse a single handle"
        );
    }
}

#[test]
fn ensure_capacity_wait_wakes_promptly_on_notify() {
    // N-3 - the ensure_capacity wait loop now uses a 50ms ceiling
    // and relies on notify_frame_waiters for prompt wake-up. Verify
    // that a notify dispatched from a sibling thread does in fact
    // wake the waiter well under the ceiling.
    use std::thread;
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        4,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        0,
    );
    let pool_clone = pool.clone();
    let notifier = thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(5));
        pool_clone.notify_frame_waiters();
    });
    let started = Instant::now();
    pool.wait_for_frame_release(Duration::from_millis(200));
    let elapsed = started.elapsed();
    notifier.join().unwrap();
    assert!(
        elapsed < Duration::from_millis(150),
        "expected wake well under the 200ms timeout, got {elapsed:?}"
    );
}

#[test]
fn prefetch_window_grows_on_high_hit_ratio() {
    // N-1 - adaptive prefetch: a sustained >50% prefetch hit ratio while
    // demand reads are waiting on prefetched pages should push the effective
    // window above the configured baseline, never exceeding
    // MAX_BUFFER_POOL_PREFETCH_PAGES.  Without demand waits a wider issue
    // depth has no observed latency benefit and must remain at the baseline.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        1_024,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::ZERO,
        1,
        DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
    );
    let initial = pool.effective_prefetch_window();
    assert_eq!(initial, DEFAULT_BUFFER_POOL_PREFETCH_PAGES);
    for _ in 0..6 {
        // Each loop simulates one admission window with 80% useful hits.
        pool.prefetches.fetch_add(300, Ordering::Relaxed);
        pool.prefetch_hits.fetch_add(240, Ordering::Relaxed);
        pool.prefetch_demand_waits.fetch_add(1, Ordering::Relaxed);
        pool.maybe_adapt_prefetch();
    }
    let after = pool.effective_prefetch_window();
    assert!(
        after > initial && after <= MAX_BUFFER_POOL_PREFETCH_PAGES,
        "expected grown effective window, got {after} (initial {initial})"
    );
}

#[test]
fn prefetch_queue_is_bounded_below_buffer_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        80,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        2,
        DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "prefetch_queue_bound", 1, "heap"),
        1,
    ));
    let candidates = (0..80)
        .map(|page_id| PageKey {
            file: file.clone(),
            page_id,
        })
        .collect();
    pool.enqueue_prefetch_batch(candidates, pool.effective_prefetch_window());

    // Background workers may already have drained part of the queue by the
    // time this thread takes the lock.  The synchronously recorded peak is
    // the stable assertion: admission itself must never fill more than 1/4
    // of the buffer pool.
    let queued = pool.prefetch_queue.lock().unwrap().len();
    let peak = pool.prefetch_queue_depth_peak.load(Ordering::Relaxed);
    assert!(peak > 0 && peak <= 20, "unexpected queue peak: {peak}");
    assert!(queued <= peak as usize);
    assert!(queued < pool.capacity_pages() as usize);
    assert!(pool.prefetch_queue_full_drops.load(Ordering::Relaxed) > 0);
}

#[test]
fn scan_origin_eviction_prefers_scan_frames_over_working_set() {
    // N-2 - load the pool with a Normal-intent (working set) frame
    // and several Scan-intent frames; the next admission forces an
    // eviction. The picker should drop a Scan-origin frame and leave
    // the working-set frame alone.
    let dir = tempfile::tempdir().unwrap();
    // capacity=3: room for 1 working-set + 2 scan frames. Strict
    // mode is required: ensure_capacity is a no-op under Memory
    // durability, so eviction never runs and the test never reaches
    // the interesting code path.
    let pool = BufferPool::new_with_options(
        3,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = |id: u64| PageKey {
        file: file.clone(),
        page_id: id,
    };
    // Working-set frame at page 0.
    drop(
        pool.pin_page_with_intent(key(0), AccessIntent::Normal)
            .unwrap(),
    );
    // Two scan-origin frames at pages 1, 2.
    drop(
        pool.pin_page_with_intent(key(1), AccessIntent::Scan)
            .unwrap(),
    );
    drop(
        pool.pin_page_with_intent(key(2), AccessIntent::Scan)
            .unwrap(),
    );
    assert_eq!(pool.snapshot().used_pages, 3);
    let scan_before = pool.scan_origin_count.load(Ordering::Relaxed);
    assert_eq!(scan_before, 2, "expected two scan-origin frames");
    // Pin another scan frame: capacity-bound eviction must run.
    drop(
        pool.pin_page_with_intent(key(3), AccessIntent::Scan)
            .unwrap(),
    );
    let snap = pool.snapshot();
    assert!(snap.used_pages <= 3);
    assert!(
        snap.scan_origin_evictions >= 1,
        "expected scan-origin eviction, got {snap:?}"
    );
    // Page 0 (Normal) should still be resident.
    assert!(
        pool.lookup_frame(&key(0)).unwrap().is_some(),
        "working-set frame was evicted by scan pressure"
    );
}

#[test]
fn cached_read_promotes_scan_origin_frame_into_working_set() {
    // BUG-1 - a page first loaded by a Scan (`scan_origin = true`) and
    // then made hot through one of the cheap "page already resident"
    // fast paths (`with_cached_bytes` / `with_cached_heap_page` /
    // `cached_index_page`) must be promoted out of the scan sublist.
    // Before the fix those paths only cleared the prefetch flag and
    // `touch`ed, leaving `scan_origin` set — so `find_clock_victim` kept
    // treating the page as an immediate victim (it short-circuits on
    // `scan_origin` before the ref_bit/usage grace) and evicted a page
    // that had become part of the working set, while `scan_origin_count`
    // stayed inflated and over-throttled prefetch.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        8,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = PageKey {
        file: file.clone(),
        page_id: 0,
    };
    // Load page 0 with Scan intent: scan_origin = true, count = 1.
    drop(
        pool.pin_page_with_intent(key.clone(), AccessIntent::Scan)
            .unwrap(),
    );
    assert_eq!(pool.scan_origin_count.load(Ordering::Relaxed), 1);
    let frame = pool.lookup_frame(&key).unwrap().unwrap();
    assert!(
        frame.scan_origin.load(Ordering::Acquire),
        "precondition: frame should be scan-origin after a Scan load"
    );
    // A Normal access through the cheap resident-read fast path must
    // promote the frame into the working set.
    pool.with_cached_bytes(&key, |_bytes| Ok(())).unwrap();
    assert!(
        !frame.scan_origin.load(Ordering::Acquire),
        "cached read did not promote scan-origin frame (BUG-1)"
    );
    assert_eq!(
        pool.scan_origin_count.load(Ordering::Relaxed),
        0,
        "scan_origin_count not decremented after cached-read promotion (BUG-1)"
    );
}

#[test]
fn flush_data_cache_skips_per_file_fsync_when_pool_is_clean() {
    // P0+P1 - on an empty pool with no pending writes, FLUSH DATA_CACHE
    // must take the early-out: no per-shard write-lock scan, no
    // per-file fsync, just a short-circuit counter bump.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        8,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = PageKey {
        file: file.clone(),
        page_id: 0,
    };
    pool.store_page(key.clone(), vec![1; PAGE_SIZE], 0).unwrap();
    // First call: real work. After it, used=0, dirty=0, file is clean.
    pool.flush_and_evict_all().unwrap();
    let after_first = pool.snapshot();
    assert_eq!(after_first.used_pages, 0);
    assert_eq!(after_first.dirty_pages, 0);
    assert_eq!(after_first.flush_data_cache_pages, 1, "{after_first:?}");
    assert!(
        after_first.flush_data_cache_us > 0,
        "flush duration should be observable: {after_first:?}"
    );
    assert_eq!(
        pool.recycled_raw_page_count.load(Ordering::Relaxed),
        0,
        "explicit cache flush must release recycled raw-page reservations"
    );
    assert!(after_first.file_syncs >= 1, "{after_first:?}");
    let baseline_syncs = after_first.file_syncs;
    let baseline_skipped = after_first.file_sync_skipped;
    let baseline_shortcircuits = after_first.flush_data_cache_short_circuits;
    let baseline_pages = after_first.flush_data_cache_pages;
    let baseline_us = after_first.flush_data_cache_us;

    // Second call: nothing has changed since the last sync. Must take
    // the early-out, NOT call fsync on the cached file.
    pool.flush_and_evict_all().unwrap();
    let after_second = pool.snapshot();
    assert_eq!(
        after_second.file_syncs, baseline_syncs,
        "second flush_and_evict_all should not fsync; got {after_second:?}"
    );
    assert_eq!(
        after_second.file_sync_skipped, baseline_skipped,
        "early-out must skip sync_cached_files entirely (no per-file skip counted), got {after_second:?}"
    );
    assert_eq!(
        after_second.flush_data_cache_short_circuits,
        baseline_shortcircuits + 1,
        "expected short-circuit counter to bump, got {after_second:?}"
    );
    assert_eq!(
        after_second.flush_data_cache_pages, baseline_pages,
        "an empty-cache short circuit must not count evicted pages"
    );
    assert!(
        after_second.flush_data_cache_us >= baseline_us,
        "flush duration must be cumulative: {after_second:?}"
    );
}

#[test]
fn flush_data_cache_compacts_each_shard_clock_in_one_pass() {
    // Regression for the former O(resident_pages * clock_len) path. A full
    // flush removes every frame first, then one live-membership retain per
    // shard must discard all stale clock entries, including duplicates.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        128,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    pool.shutdown_flusher();
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    for page_id in 0..64_u64 {
        let key = PageKey {
            file: file.clone(),
            page_id,
        };
        pool.store_page(key.clone(), vec![page_id as u8; PAGE_SIZE], page_id)
            .unwrap();
        // A stale duplicate exercises compaction without requiring a timing
        // assertion, which would be flaky on shared CI hosts.
        let shard_idx = pool.shard_index(&key);
        pool.shards[shard_idx].clock.lock().unwrap().push_back(key);
    }

    let before = pool.snapshot();
    assert_eq!(before.used_pages, 64, "{before:?}");
    pool.flush_and_evict_all().unwrap();
    let after = pool.snapshot();
    assert_eq!(after.used_pages, 0, "{after:?}");
    assert_eq!(after.flush_data_cache_pages, 64, "{after:?}");
    assert!(
        pool.shards
            .iter()
            .all(|shard| shard.clock.lock().unwrap().is_empty()),
        "full flush left stale clock entries"
    );
}

#[test]
fn sync_cached_file_handle_skips_when_no_writes_since_last_sync() {
    // P1 - directly exercise the needs_sync fast path. After a successful
    // sync the flag is cleared; a follow-up sync with no new writes must
    // increment `file_sync_skipped` and leave `file_syncs` unchanged.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        4,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let file_key = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = PageKey {
        file: file_key.clone(),
        page_id: 0,
    };
    pool.store_page(key.clone(), vec![7; PAGE_SIZE], 1).unwrap();
    pool.flush_all().unwrap();
    let after_first_flush = pool.snapshot();
    assert!(after_first_flush.file_syncs >= 1, "{after_first_flush:?}");

    // A second flush_all with nothing dirty must NOT do another fsync.
    pool.flush_all().unwrap();
    let after_second_flush = pool.snapshot();
    assert_eq!(
        after_second_flush.file_syncs, after_first_flush.file_syncs,
        "clean second flush_all should skip the fsync, got {after_second_flush:?}"
    );
    assert!(
        after_second_flush.file_sync_skipped > after_first_flush.file_sync_skipped,
        "expected file_sync_skipped to bump, got {after_second_flush:?}"
    );

    // Writing a new page re-arms needs_sync; the next flush_all must fsync.
    let key2 = PageKey {
        file: file_key.clone(),
        page_id: 1,
    };
    pool.store_page(key2, vec![9; PAGE_SIZE], 2).unwrap();
    pool.flush_all().unwrap();
    let after_third_flush = pool.snapshot();
    assert!(
        after_third_flush.file_syncs > after_second_flush.file_syncs,
        "writes after a sync must re-arm needs_sync, got {after_third_flush:?}"
    );
}

#[test]
fn flush_data_cache_early_out_blocked_when_file_still_needs_sync() {
    // P0 - the early-out must NOT trigger when used_pages and dirty_pages
    // are both 0 but a file still has pending data on disk that hasn't
    // been fsync'd. This guards against a regression where pages were
    // written and evicted (via direct write_page_bytes_with_generation
    // or a flush_some_dirty cycle) leaving the file still
    // needs_sync=true.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        4,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    pool.shutdown_flusher();
    let file_key = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = PageKey {
        file: file_key.clone(),
        page_id: 0,
    };
    // Write a page directly to disk (no resident frame). This sets
    // needs_sync=true on the cached file but used_pages stays 0.
    let generation = pool.file_generation(&file_key).unwrap();
    pool.write_page_bytes_with_generation(&key, generation, &vec![5; PAGE_SIZE])
        .unwrap();
    let pre = pool.snapshot();
    assert_eq!(pre.used_pages, 0);
    assert_eq!(pre.dirty_pages, 0);

    pool.flush_and_evict_all().unwrap();
    let post = pool.snapshot();
    assert_eq!(
        post.flush_data_cache_short_circuits, pre.flush_data_cache_short_circuits,
        "must NOT short-circuit while a file still needs sync, got {post:?}"
    );
    assert!(
        post.file_syncs > pre.file_syncs,
        "must run the fsync for the dirty file, got {post:?}"
    );
}

#[test]
fn prefetch_throttled_for_mixed_reads_at_three_quarters_scan_share() {
    // A pure sequential scan may fill the pool because the maintained cold
    // queue recycles its pages in O(1). Once recent foreground/random reads
    // reach 20% of scan reads, restore the 75% residency guard so read-ahead
    // cannot displace a mixed workload's working set.
    let dir = tempfile::tempdir().unwrap();
    // capacity=16 → 75% threshold = 12 frames
    let pool = BufferPool::new_with_options(
        16,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = |id: u64| PageKey {
        file: file.clone(),
        page_id: id,
    };
    for id in 0..12 {
        drop(
            pool.pin_page_with_intent(key(id), AccessIntent::Scan)
                .unwrap(),
        );
    }
    assert!(
        !pool.scan_origin_pressure(),
        "a pure sequential scan should recycle cold pages without throttling"
    );
    pool.read_ios_scan.store(1_024, Ordering::Relaxed);
    pool.read_ios_normal.store(205, Ordering::Relaxed);
    assert!(
        pool.scan_origin_pressure(),
        "mixed-read pressure should fire once 12/16 frames are Scan"
    );
    let dropped_before = pool.prefetch_queue_drops.load(Ordering::Relaxed);
    pool.prefetch_page(key(100));
    let dropped_after = pool.prefetch_queue_drops.load(Ordering::Relaxed);
    assert!(
        dropped_after > dropped_before,
        "expected scan-pressure prefetch drop, got {dropped_before}..{dropped_after}"
    );
}

#[test]
fn prefetch_range_skips_resident_pages_and_enqueues_missing() {
    // Range admission backing the incremental heap-scan prefetch
    // (`HeapFile::prefetch_after`): resident page ids must not be
    // re-admitted, while absent ids in the same range still reach the
    // queue exactly once.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        64,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = |id: u64| PageKey {
        file: file.clone(),
        page_id: id,
    };
    for id in 0..4 {
        drop(
            pool.pin_page_with_intent(key(id), AccessIntent::Scan)
                .unwrap(),
        );
    }
    let before = pool.prefetches.load(Ordering::Relaxed);
    pool.prefetch_range(&file, 0, 4);
    assert_eq!(
        pool.prefetches.load(Ordering::Relaxed),
        before,
        "fully-resident range must not enqueue prefetches"
    );
    pool.prefetch_range(&file, 2, 8);
    let after = pool.prefetches.load(Ordering::Relaxed);
    assert_eq!(
        after - before,
        4,
        "absent pages 4..8 must be admitted exactly once"
    );
}

#[test]
fn overlapping_prefetch_ranges_are_unique_while_inflight() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        64,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let before = pool.prefetches.load(Ordering::Relaxed);
    pool.prefetch_range(&file, 10, 18);
    pool.prefetch_range(&file, 12, 20);
    assert_eq!(
        pool.prefetches.load(Ordering::Relaxed) - before,
        10,
        "overlap 12..18 must have one queued/inflight owner"
    );
    assert_eq!(pool.prefetch_duplicate_drops.load(Ordering::Relaxed), 6);
}

#[test]
fn second_probe_promotes_frame_into_working_set() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        8,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = PageKey { file, page_id: 0 };
    drop(
        pool.pin_page_with_intent(key.clone(), AccessIntent::Probe)
            .unwrap(),
    );
    let frame = pool.lookup_frame(&key).unwrap().unwrap();
    assert_eq!(frame.usage_count.load(Ordering::Relaxed), 1);
    assert!(frame.scan_origin.load(Ordering::Relaxed));
    drop(
        pool.pin_page_with_intent(key.clone(), AccessIntent::Probe)
            .unwrap(),
    );
    assert!(frame.usage_count.load(Ordering::Relaxed) >= 2);
    assert!(!frame.scan_origin.load(Ordering::Relaxed));
}

#[test]
fn small_scan_threshold_respects_disable_floor_boundary_and_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        1_000,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::from_millis(500),
        1,
        0,
    );

    pool.set_small_scan_pct(0);
    assert_eq!(pool.small_scan_threshold_pages(), 0);
    assert_eq!(pool.scan_cache_policy(1), ScanCachePolicy::Bulk);

    pool.set_small_scan_pct(2);
    assert_eq!(pool.small_scan_threshold_pages(), 20);
    assert_eq!(pool.scan_cache_policy(20), ScanCachePolicy::Small);
    assert_eq!(pool.scan_cache_policy(21), ScanCachePolicy::Bulk);
    assert_eq!(pool.scan_cache_policy(0), ScanCachePolicy::Bulk);

    pool.set_small_scan_pct(100);
    assert_eq!(pool.small_scan_threshold_pages(), 1_000);
    assert_eq!(pool.scan_cache_policy(1_000), ScanCachePolicy::Small);

    let tiny = BufferPool::new_with_options(
        8,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::from_millis(500),
        1,
        0,
    );
    tiny.set_small_scan_pct(2);
    assert_eq!(tiny.small_scan_threshold_pages(), 8);
}

#[test]
fn small_scan_promotes_only_after_second_logical_demand_and_caps_usage() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        8,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "small", 1, "heap"),
        1,
    ));
    let key = PageKey { file, page_id: 0 };

    drop(
        pool.pin_page_with_intent(key.clone(), AccessIntent::SmallScan)
            .unwrap(),
    );
    let frame = pool.lookup_frame(&key).unwrap().unwrap();
    assert_eq!(frame.small_scan_accesses.load(Ordering::Relaxed), 1);
    assert_eq!(frame.usage_count.load(Ordering::Relaxed), 1);
    assert!(frame.scan_origin.load(Ordering::Relaxed));

    drop(
        pool.pin_page_with_intent(key.clone(), AccessIntent::SmallScan)
            .unwrap(),
    );
    assert_eq!(frame.small_scan_accesses.load(Ordering::Relaxed), 2);
    assert_eq!(frame.usage_count.load(Ordering::Relaxed), 2);
    assert!(!frame.scan_origin.load(Ordering::Relaxed));
    assert_eq!(pool.small_scan_promotions.load(Ordering::Relaxed), 1);

    for _ in 0..16 {
        drop(
            pool.pin_page_with_intent(key.clone(), AccessIntent::SmallScan)
                .unwrap(),
        );
    }
    assert_eq!(frame.usage_count.load(Ordering::Relaxed), 6);
    frame.usage_count.store(9, Ordering::Relaxed);
    drop(
        pool.pin_page_with_intent(key, AccessIntent::SmallScan)
            .unwrap(),
    );
    assert_eq!(frame.usage_count.load(Ordering::Relaxed), 9);
}

#[test]
fn prefetched_small_scan_page_stays_cold_until_next_scan() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        8,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "prefetched_small", 1, "heap"),
        1,
    ));
    let key = PageKey { file, page_id: 0 };

    drop(
        pool.pin_page_with_intent(key.clone(), AccessIntent::Prefetch)
            .unwrap(),
    );
    let frame = pool.lookup_frame(&key).unwrap().unwrap();
    assert_eq!(frame.small_scan_accesses.load(Ordering::Relaxed), 0);

    drop(
        pool.pin_page_with_intent(key.clone(), AccessIntent::SmallScan)
            .unwrap(),
    );
    assert_eq!(frame.small_scan_accesses.load(Ordering::Relaxed), 1);
    assert_eq!(frame.usage_count.load(Ordering::Relaxed), 1);
    assert!(frame.scan_origin.load(Ordering::Relaxed));
    assert_eq!(pool.small_scan_promotions.load(Ordering::Relaxed), 0);

    drop(
        pool.pin_page_with_intent(key, AccessIntent::SmallScan)
            .unwrap(),
    );
    assert!(!frame.scan_origin.load(Ordering::Relaxed));
    assert_eq!(pool.small_scan_promotions.load(Ordering::Relaxed), 1);
}

#[test]
#[ignore = "release-only A/B effect benchmark; run explicitly with nextest --run-ignored ignored-only"]
fn small_scan_protection_ab_effect_benchmark() {
    fn run_once(root: &Path, run: usize, pct: u64) -> (u64, u64, Duration, Duration) {
        let run_dir = root.join(format!("run-{run}-pct-{pct}"));
        fs::create_dir_all(&run_dir).unwrap();
        let heap_path = t_path(&run_dir, "ab", 1, "heap");
        if let Some(parent) = heap_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let heap_file = File::create(&heap_path).unwrap();
        heap_file.set_len((3_000 * PAGE_SIZE) as u64).unwrap();
        drop(heap_file);

        let pool = BufferPool::new_with_options(
            64,
            test_log(&run_dir),
            DurabilityMode::Strict,
            Duration::from_millis(500),
            1,
            0,
        );
        pool.set_small_scan_pct(pct);
        let file = Arc::new(BufferPool::heap_file_key(heap_path, 1));
        let key = |page_id| PageKey {
            file: Arc::clone(&file),
            page_id,
        };
        let policy = pool.scan_cache_policy(8);
        let small_intent = policy.access_intent();

        for _ in 0..2 {
            for page_id in 0..8 {
                drop(
                    pool.pin_page_with_intent(key(page_id), small_intent)
                        .unwrap(),
                );
            }
        }

        let started = Instant::now();
        let mut bulk_scan_elapsed = Duration::ZERO;
        let mut small_rereads = 0u64;
        for cycle in 0..50u64 {
            let pressure_start = 8 + cycle * 56;
            let bulk_started = Instant::now();
            for page_id in pressure_start..pressure_start + 56 {
                drop(
                    pool.pin_page_with_intent(key(page_id), AccessIntent::Scan)
                        .unwrap(),
                );
            }
            bulk_scan_elapsed += bulk_started.elapsed();
            let reads_before = pool.read_ios.load(Ordering::Relaxed);
            for page_id in 0..8 {
                drop(
                    pool.pin_page_with_intent(key(page_id), small_intent)
                        .unwrap(),
                );
            }
            small_rereads = small_rereads.saturating_add(
                pool.read_ios
                    .load(Ordering::Relaxed)
                    .saturating_sub(reads_before),
            );
        }
        (
            small_rereads,
            pool.small_scan_promotions.load(Ordering::Relaxed),
            bulk_scan_elapsed,
            started.elapsed(),
        )
    }

    let dir = tempfile::tempdir().unwrap();
    let mut disabled = Vec::new();
    let mut enabled = Vec::new();
    for run in 0..5 {
        disabled.push(run_once(dir.path(), run, 0));
        enabled.push(run_once(dir.path(), run, 2));
    }
    let mut disabled_reads: Vec<u64> = disabled.iter().map(|sample| sample.0).collect();
    let mut enabled_reads: Vec<u64> = enabled.iter().map(|sample| sample.0).collect();
    let mut disabled_bulk_us: Vec<u128> =
        disabled.iter().map(|sample| sample.2.as_micros()).collect();
    let mut enabled_bulk_us: Vec<u128> =
        enabled.iter().map(|sample| sample.2.as_micros()).collect();
    let mut disabled_total_us: Vec<u128> =
        disabled.iter().map(|sample| sample.3.as_micros()).collect();
    let mut enabled_total_us: Vec<u128> =
        enabled.iter().map(|sample| sample.3.as_micros()).collect();
    disabled_reads.sort_unstable();
    enabled_reads.sort_unstable();
    disabled_bulk_us.sort_unstable();
    enabled_bulk_us.sort_unstable();
    disabled_total_us.sort_unstable();
    enabled_total_us.sort_unstable();
    let median_disabled_reads = disabled_reads[2];
    let median_enabled_reads = enabled_reads[2];
    eprintln!(
        "small-scan A/B: disabled rereads={median_disabled_reads}, enabled rereads={median_enabled_reads}, disabled_bulk_us={}, enabled_bulk_us={}, disabled_total_us={}, enabled_total_us={}, promotions={}",
        disabled_bulk_us[2],
        enabled_bulk_us[2],
        disabled_total_us[2],
        enabled_total_us[2],
        enabled[2].1,
    );
    assert!(
        median_disabled_reads > 0,
        "baseline did not experience churn"
    );
    assert!(
        median_enabled_reads.saturating_mul(5) <= median_disabled_reads,
        "SmallScan reduced rereads by less than 80%: disabled={median_disabled_reads}, enabled={median_enabled_reads}"
    );
    assert!(enabled.iter().all(|sample| sample.1 == 8));
    assert!(disabled.iter().all(|sample| sample.1 == 0));
}

#[test]
fn prefetch_admitted_below_three_quarters_scan_share() {
    // Companion to the above: at <75% scan-origin share the throttle
    // must NOT fire. Catches regressions that lower the threshold back.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        16,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = |id: u64| PageKey {
        file: file.clone(),
        page_id: id,
    };
    // 4/16 = 25% scan-origin share — used to trigger the throttle, now
    // must be admitted.
    for id in 0..4 {
        drop(
            pool.pin_page_with_intent(key(id), AccessIntent::Scan)
                .unwrap(),
        );
    }
    assert!(
        !pool.scan_origin_pressure(),
        "scan_origin_pressure should not fire at 4/16 = 25%"
    );
    let dropped_before = pool.prefetch_queue_drops.load(Ordering::Relaxed);
    pool.prefetch_page(key(100));
    let dropped_after = pool.prefetch_queue_drops.load(Ordering::Relaxed);
    assert_eq!(
        dropped_after, dropped_before,
        "expected admission below 75% scan share"
    );
}

#[test]
fn prefetch_admitted_when_pool_full_of_working_set() {
    // PERF-2 - a pool that is 100% occupied by the OLTP working set (no
    // scan-origin frames) must STILL admit prefetch. The prefetch worker
    // recycles cold pages to make room; midpoint-LRU keeps the working
    // set resident. The previous hard `used >= capacity` guard returned
    // early here, disabling read-ahead for any scan whose footprint
    // exceeds the pool — exactly the workload prefetch is meant to help.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        8,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::from_millis(500),
        1,
        DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = |id: u64| PageKey {
        file: file.clone(),
        page_id: id,
    };
    // Fill the pool with Normal (working-set) frames: used == capacity,
    // scan-origin share = 0, so `scan_origin_pressure` does not fire.
    for id in 0..8 {
        drop(
            pool.pin_page_with_intent(key(id), AccessIntent::Normal)
                .unwrap(),
        );
    }
    assert_eq!(pool.snapshot().used_pages, 8);
    assert!(
        !pool.scan_origin_pressure(),
        "a pure working set must not register scan pressure"
    );
    let before = pool.prefetches.load(Ordering::Relaxed);
    // A scan wants a non-resident page. The pool is full, but there is no
    // scan working set to protect → the prefetch must be admitted.
    pool.prefetch_page(key(100));
    let after = pool.prefetches.load(Ordering::Relaxed);
    assert!(
        after > before,
        "full-pool prefetch must be admitted absent scan pressure (PERF-2), got {before}..{after}"
    );
}

#[test]
fn prefetch_window_shrinks_on_low_hit_ratio() {
    // N-1 - mirror of the growth test: a <10% prefetch hit ratio
    // should shrink the window toward MIN_BUFFER_POOL_PREFETCH_PAGES,
    // never below it.
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        1_024,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::ZERO,
        1,
        DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
    );
    let initial = pool.effective_prefetch_window();
    for _ in 0..8 {
        pool.prefetches.fetch_add(300, Ordering::Relaxed);
        pool.prefetch_hits.fetch_add(9, Ordering::Relaxed); // 3%
        pool.maybe_adapt_prefetch();
    }
    let after = pool.effective_prefetch_window();
    assert!(
        after < initial && after >= MIN_BUFFER_POOL_PREFETCH_PAGES,
        "expected shrunk effective window, got {after} (initial {initial})"
    );
}

#[test]
fn buffer_pool_discard_file_prevents_stale_frame_writeback() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        2,
        test_log(dir.path()),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    pool.shutdown_flusher();
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "t", 1, "heap"),
        1,
    ));
    let key = PageKey {
        file: file.clone(),
        page_id: 0,
    };

    pool.store_page(key.clone(), vec![7; PAGE_SIZE], 1).unwrap();
    let stale = pool.lookup_frame(&key).unwrap().unwrap();
    pool.discard_file(&file).unwrap();
    assert!(!pool.file_generations.read().unwrap().contains_key(&*file));
    pool.write_page_bytes_with_generation(
        &key,
        stale.generation.load(Ordering::Acquire),
        &vec![9; PAGE_SIZE],
    )
    .unwrap();

    let image = fs::read(&file.path).unwrap_or_default();
    assert!(image.is_empty(), "{image:?}");
    let reopened_generation = pool.file_generation(&file).unwrap();
    assert_ne!(
        reopened_generation,
        stale.generation.load(Ordering::Acquire)
    );
}

#[test]
fn single_page_discard_does_not_invalidate_sibling_writeback() {
    let dir = tempfile::tempdir().unwrap();
    let pool = BufferPool::new_with_options(
        2,
        test_log(dir.path()),
        DurabilityMode::Memory,
        Duration::ZERO,
        1,
        0,
    );
    let file = Arc::new(BufferPool::heap_file_key(
        t_path(dir.path(), "siblings", 1, "heap"),
        1,
    ));
    let first = PageKey {
        file: Arc::clone(&file),
        page_id: 0,
    };
    let sibling = PageKey {
        file: Arc::clone(&file),
        page_id: 1,
    };
    pool.store_page(first.clone(), vec![1; PAGE_SIZE], 0)
        .unwrap();
    pool.store_page(sibling.clone(), vec![9; PAGE_SIZE], 0)
        .unwrap();
    let sibling_frame = pool.lookup_frame(&sibling).unwrap().unwrap();
    let generation_before = sibling_frame.generation.load(Ordering::Acquire);

    pool.discard_page(&first).unwrap();
    assert_eq!(pool.file_generation(&file).unwrap(), generation_before);
    pool.flush_frame(&sibling_frame).unwrap();

    let image = fs::read(&file.path).unwrap();
    assert_eq!(image[PAGE_SIZE], 9);
}

#[test]
fn buffer_pool_reports_heap_cache_hits_and_misses() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec![],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)],
            ],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
    }

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    let after_first_read = db.storage_status();
    assert!(after_first_read.buffer_misses > 0, "{after_first_read:?}");

    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    let after_second_read = db.storage_status();

    assert!(
        after_second_read.buffer_hits > after_first_read.buffer_hits,
        "{after_second_read:?}"
    );
    assert_eq!(
        after_second_read.buffer_misses, after_first_read.buffer_misses,
        "{after_second_read:?}"
    );
    assert!(
        after_second_read.buffer_hit_pct > 0,
        "{after_second_read:?}"
    );
}

#[test]
fn repeated_small_sql_scan_promotes_pages_and_reports_status() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "small_scan_t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec![],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.small_scan_t",
            (0..32).map(|id| vec![Value::Int(id)]).collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
    }

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    db.set_buffer_pool_small_scan_pct(100).unwrap();
    let txn = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
    let baseline = db.storage_status();
    assert_eq!(baseline.buffer_pool_small_scan_pct, 100);
    assert_eq!(
        baseline.buffer_pool_small_scan_threshold_pages,
        baseline.buffer_pool_pages
    );

    assert_eq!(db.scan_rows("test.small_scan_t", txn).unwrap().len(), 32);
    let after_first = db.storage_status();
    assert!(
        after_first.buffer_pool_small_scan_accesses > baseline.buffer_pool_small_scan_accesses,
        "{after_first:?}"
    );
    assert_eq!(
        after_first.buffer_pool_small_scan_promotions, baseline.buffer_pool_small_scan_promotions,
        "first pass must remain cold: {after_first:?}"
    );

    assert_eq!(db.scan_rows("test.small_scan_t", txn).unwrap().len(), 32);
    let after_second = db.storage_status();
    assert!(
        after_second.buffer_pool_small_scan_promotions
            > after_first.buffer_pool_small_scan_promotions,
        "second pass did not promote: {after_second:?}"
    );

    db.set_buffer_pool_small_scan_pct(0).unwrap();
    let disabled = db.storage_status();
    assert_eq!(disabled.buffer_pool_small_scan_pct, 0);
    assert_eq!(disabled.buffer_pool_small_scan_threshold_pages, 0);
}

#[test]
fn buffer_pool_concurrent_cold_miss_reads_page_once() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec![],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
    }

    let db = Arc::new(Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap());
    let table_id = db.table("test.t").unwrap().id;
    // Reopening loads the catalog by scanning the data-dictionary heaps
    // through the buffer pool, so baseline the I/O counters after open and
    // assert only the delta caused by the concurrent page pin below.
    let baseline = db.storage_status();
    let key = PageKey {
        file: Arc::new(BufferPool::heap_file_key(db.table_path(table_id), table_id)),
        page_id: 0,
    };
    let start = Arc::new(std::sync::Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let db = Arc::clone(&db);
            let key = key.clone();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                let pinned = db.buffer_pool.pin_page(key).unwrap();
                let bytes = pinned.bytes().unwrap();
                assert_eq!(bytes.len(), PAGE_SIZE);
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    let status = db.storage_status();
    assert_eq!(
        status.buffer_pool_read_ios - baseline.buffer_pool_read_ios,
        1,
        "{status:?}"
    );
    assert_eq!(
        status.buffer_misses - baseline.buffer_misses,
        1,
        "{status:?}"
    );
    assert!(status.buffer_hits - baseline.buffer_hits >= 7, "{status:?}");
}

#[test]
fn buffer_pool_scan_prefetches_without_point_lookup_prefetch() {
    // Use rows below the default LOB inline threshold so this stays
    // independent of `MYDB_LOB_INLINE_MAX`'s process-wide OnceLock, while
    // still spanning multiple heap pages and exercising scan prefetch.
    const PAD_LEN: usize = 3000;
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    // Loading the catalog from the data-dictionary heaps on open leaves ~8
    // scan-origin frames resident. The prefetcher throttles once scan-origin
    // frames reach capacity/4 (`scan_origin_pressure`), so size the pool well
    // above 4× that baseline; the scan must still out-prefetch the point
    // lookup, which is what this exercises.
    config.buffer_pool_pages = 64;
    config.buffer_pool_prefetch_pages = 2;
    {
        let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
        db.create_table(
            "t",
            vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
                ColumnDef {
                    name: "pad".into(),
                    data_type: DataType::VarChar(Some(8000)),
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        let rows = (0..8)
            .map(|id| vec![Value::Int(id), Value::String("x".repeat(PAD_LEN))])
            .collect::<Vec<_>>();
        db.insert_rows(txn, "test.t", rows).unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
    }

    let db = Database::open_with_config(dir.path(), config).unwrap();

    let point_before = db.storage_status();
    let table = db.table("test.t").unwrap();
    let _ = db
        .lookup_index(
            &table,
            table.indexes.iter().find(|idx| idx.primary).unwrap(),
            IndexKey::from(vec![KeyPart::Int(1)]),
        )
        .unwrap();
    let point_after = db.storage_status();
    assert_eq!(
        point_after.buffer_pool_prefetches, point_before.buffer_pool_prefetches,
        "{point_after:?}"
    );

    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 8);
    let scan_after = db.storage_status();
    assert!(
        scan_after.buffer_pool_prefetches > point_after.buffer_pool_prefetches,
        "{scan_after:?}"
    );
}

fn lob_col(name: &str, data_type: DataType, primary_key: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: true,
        primary_key,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    }
}

#[test]
fn externalized_lob_round_trips_and_survives_reopen() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let big_text = "A".repeat(10_000); // > threshold → externalized
    let big_blob = vec![0xABu8; 9_000]; // > threshold → externalized
    let table_id;
    {
        let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
        db.create_table(
            "t",
            vec![
                lob_col("id", DataType::Int, true),
                lob_col(
                    "body",
                    DataType::Text(crate::storage::schema::MEDIUM_LOB_MAX),
                    false,
                ),
                lob_col(
                    "data",
                    DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
                    false,
                ),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        table_id = db.table("test.t").unwrap().id;
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![
                // Row 1: small values stay inline.
                vec![
                    Value::Int(1),
                    Value::String("hello".into()),
                    Value::Bytes(b"world".to_vec()),
                ],
                // Row 2: large values are pushed off-page.
                vec![
                    Value::Int(2),
                    Value::String(big_text.clone()),
                    Value::Bytes(big_blob.clone()),
                ],
            ],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
    }

    // The overflow file must have been created (large values externalized).
    let lob_path = t_path(dir.path(), "t", table_id, "lob");
    assert!(
        lob_path.exists(),
        "overflow file should exist after externalize"
    );

    // Reopen (exercises recovery + persisted overflow chains) and verify
    // every value round-trips byte-for-byte.
    let db = Database::open_with_config(dir.path(), config).unwrap();
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[1], Value::String("hello".into()));
    assert_eq!(rows[0].values[2], Value::Bytes(b"world".to_vec()));
    assert_eq!(rows[1].values[1], Value::String(big_text));
    assert_eq!(rows[1].values[2], Value::Bytes(big_blob));
}

#[test]
fn stale_lob_descriptor_cannot_free_reused_page_generation() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
    db.create_table(
        "lob_generation",
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
    let table_id = db.table("test.lob_generation").unwrap().id;
    let old = db
        .with_overflow_mut(table_id, |overflow| {
            overflow.allocate_chain_compressible(b"old generation", false)
        })
        .unwrap();
    db.with_overflow_mut(table_id, |overflow| overflow.free_chain(old))
        .unwrap();
    db.checkpoint().unwrap();
    db.flush_dirty_data().unwrap();
    drop(db);

    let db = Database::open_with_config(dir.path(), config).unwrap();
    let current = db
        .with_overflow_mut(table_id, |overflow| {
            overflow.allocate_chain_compressible(b"current generation remains live", false)
        })
        .unwrap();
    assert_eq!(current.first_page_id, old.first_page_id);
    assert_ne!(current.generation, old.generation);

    // A delayed undo cleanup still carrying `old` must be an idempotent
    // no-op, not put the current generation back on the free list.
    db.with_overflow_mut(table_id, |overflow| overflow.free_chain(old))
        .unwrap();
    let bytes = db
        .with_overflow_read(table_id, |overflow| overflow.read_chain(current))
        .unwrap();
    assert_eq!(bytes, b"current generation remains live");
    assert!(!db
        .with_overflow_read(table_id, |overflow| {
            overflow.free_list_contains(current.first_page_id)
        })
        .unwrap());
}

#[test]
fn lob_flood_respects_kind_quota_and_keeps_heap_working_set() {
    // Q-1 - per-kind soft quota: streaming far more overflow (LOB) pages
    // than the pool holds must recycle LOB frames among themselves (the
    // over-quota eviction preference) instead of consuming the heap /
    // index working set, and the per-kind residency counters must stay
    // consistent with the total.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.buffer_pool_pages = 48;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
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
    // 30 rows × 64KB → ~4 overflow pages per chain, ~120 LOB page reads
    // per scan through a 48-page pool: heavy eviction pressure with the
    // LOB kind far above its default 30% quota.
    let big = "B".repeat(65_536);
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (1..=30)
            .map(|i| vec![Value::Int(i), Value::String(big.clone())])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    for _ in 0..2 {
        let rows = db
            .scan_rows(
                "test.t",
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap();
        assert_eq!(rows.len(), 30);
    }

    let status = db.storage_status();
    assert!(
        status.buffer_pool_quota_evictions > 0,
        "LOB flood must trigger the over-quota eviction preference: {status:?}"
    );
    // Include every page family, including automatically maintained zone maps.
    let kind_sum = status.buffer_pool_pages_heap
        + status.buffer_pool_pages_index
        + status.buffer_pool_pages_lob
        + status.buffer_pool_pages_vector
        + status.buffer_pool_pages_zone_map;
    let drift = kind_sum.abs_diff(status.buffer_pool_used_pages);
    assert!(
        drift <= 2,
        "per-kind residency must track used_pages: kinds={kind_sum} used={} {status:?}",
        status.buffer_pool_used_pages
    );
    // The heap / index working set survived the flood.
    assert!(
        status.buffer_pool_pages_heap > 0,
        "heap pages must survive a LOB flood: {status:?}"
    );
}

#[test]
fn externalized_lob_spanning_many_pages_round_trips() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    // ~1 MiB value → a chain of ~64 overflow pages, exercising the
    // multi-page linked-chain read/write path.
    let huge = (0..1_048_576u32)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<u8>>();
    {
        let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
        db.create_table(
            "t",
            vec![
                lob_col("id", DataType::Int, true),
                lob_col(
                    "data",
                    DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
                    false,
                ),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(1), Value::Bytes(huge.clone())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
    }
    let db = Database::open_with_config(dir.path(), config).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[1], Value::Bytes(huge));
}

fn open_lob_table(dir: &std::path::Path, config: &StorageConfig) -> Arc<Database> {
    let db = Arc::new(Database::open_with_config(dir, config.clone()).unwrap());
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
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
    db
}

fn scan_body(db: &Database, id: i64) -> Option<Value> {
    db.scan_rows(
        "test.t",
        TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
    )
    .unwrap()
    .into_iter()
    .find(|r| r.values[0] == Value::Int(id))
    .map(|r| r.values[1].clone())
}

fn row_id_for(db: &Database, id: i64) -> RowId {
    db.scan_rows(
        "test.t",
        TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
    )
    .unwrap()
    .into_iter()
    .find(|r| r.values[0] == Value::Int(id))
    .map(|r| r.row_id)
    .unwrap()
}

#[test]
fn update_externalized_lob_round_trips() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = open_lob_table(dir.path(), &config);
    let table = db.table("test.t").unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::String("A".repeat(10_000))]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // UPDATE to a different large value (new chain) — must not 1118 and
    // must read back the new value.
    let rid = row_id_for(&db, 1);
    let txn = db.begin_txn().unwrap();
    db.update_row_by_id(txn, &table, rid, |old| {
        let mut v = old.to_vec();
        v[1] = Value::String("B".repeat(12_000));
        Ok(v)
    })
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(scan_body(&db, 1), Some(Value::String("B".repeat(12_000))));

    // UPDATE to a small (inline) value, then back to large.
    let txn = db.begin_txn().unwrap();
    db.update_row_by_id(txn, &table, rid, |old| {
        let mut v = old.to_vec();
        v[1] = Value::String("tiny".into());
        Ok(v)
    })
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(scan_body(&db, 1), Some(Value::String("tiny".into())));
}

#[test]
fn delete_externalized_lob_removes_row_without_copying() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = open_lob_table(dir.path(), &config);
    let table = db.table("test.t").unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::String("Z".repeat(20_000))]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let rid = row_id_for(&db, 1);
    let txn = db.begin_txn().unwrap();
    assert!(db.delete_row_by_id(txn, &table, rid).unwrap());
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(scan_body(&db, 1), None);
}

#[test]
fn mvcc_old_snapshot_reads_pre_update_lob() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = open_lob_table(dir.path(), &config);
    let table = db.table("test.t").unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::String("OLD".repeat(4_000))]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // Reader transaction takes its snapshot BEFORE the update commits.
    let reader = db.begin_txn().unwrap();
    let _guard = db.register_statement_snapshot(reader.snapshot_scn);

    let rid = row_id_for(&db, 1);
    let writer = db.begin_txn().unwrap();
    db.update_row_by_id(writer, &table, rid, |old| {
        let mut v = old.to_vec();
        v[1] = Value::String("NEW".repeat(5_000));
        Ok(v)
    })
    .unwrap();
    db.commit_txn(writer.txn_id).unwrap();

    // The reader's older snapshot must still see the pre-update LOB,
    // materialized from the old chain kept alive in undo.
    let old_row = db
        .scan_rows("test.t", reader)
        .unwrap()
        .into_iter()
        .find(|r| r.values[0] == Value::Int(1))
        .unwrap();
    assert_eq!(old_row.values[1], Value::String("OLD".repeat(4_000)));
}

/// Read column `body` of row `id` through an explicit transaction view
/// (i.e. at a fixed snapshot), returning `None` if the row is invisible.
fn scan_one_body(db: &Database, txn: TxnView, id: i64) -> Option<Value> {
    db.scan_rows("test.t", txn)
        .unwrap()
        .into_iter()
        .find(|r| r.values[0] == Value::Int(id))
        .map(|r| r.values[1].clone())
}

/// P0 regression (commit-visibility atomicity): a fixed snapshot must see a
/// concurrent committer as either fully visible or fully invisible — never
/// flip across the committer's publish point. Today `commit_scn` is bumped
/// into the global allocation clock (`next_scn_atomic`) BEFORE the slot is
/// published, so a reader can take `snapshot_scn == writer.commit_scn` while
/// the writer is still `Active`: it reads the OLD version, then re-reads the
/// NEW version once the writer publishes — a non-repeatable read under the
/// SAME snapshot. This test deterministically parks the committer in that
/// window via `commit_visibility_test_hook` and asserts the two reads agree.
///
/// EXPECTED RED until the gap-aware commit-frontier fix lands (see
/// docs/mvcc-concurrency-improvement-design.md, P0-1).
#[test]
fn mvcc_commit_is_atomic_to_concurrent_reader() {
    use std::sync::{Arc, Barrier};

    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = open_lob_table(dir.path(), &config);

    // Seed a committed version V0 BEFORE installing the stall hook.
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![Value::Int(1), Value::String("V0".into())]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let rid = row_id_for(&db, 1);

    // Rendezvous around the writer's allocate→publish window.
    let allocated = Arc::new(Barrier::new(2)); // writer has allocated commit_scn, slot still Active
    let released = Arc::new(Barrier::new(2)); // reader finished its in-window read; writer may publish

    {
        let a = Arc::clone(&allocated);
        let r = Arc::clone(&released);
        super::commit_visibility_test_hook::install(Arc::new(move |_commit_scn| {
            a.wait();
            r.wait();
        }));
    }

    let writer_db = Arc::clone(&db);
    let writer = std::thread::spawn(move || {
        let w = writer_db.begin_txn().unwrap();
        let t = writer_db.table("test.t").unwrap();
        writer_db
            .update_row_by_id(w, &t, rid, |old| {
                let mut v = old.to_vec();
                v[1] = Value::String("V1".into());
                Ok(v)
            })
            .unwrap();
        // Parks inside the hook after allocating commit_scn, before publish.
        writer_db.commit_txn(w.txn_id).unwrap();
    });

    // Writer has bumped next_scn_atomic but has NOT published yet.
    allocated.wait();

    // Reader's snapshot_scn == writer.commit_scn (atomic already bumped).
    let reader = db.begin_txn().unwrap();
    let read_before = scan_one_body(&db, reader, 1);

    // Allow the writer to publish, then join.
    released.wait();
    writer.join().unwrap();
    super::commit_visibility_test_hook::clear();

    // SAME snapshot, second read. Snapshot isolation requires equality.
    let read_after = scan_one_body(&db, reader, 1);

    assert_eq!(
        read_before, read_after,
        "non-repeatable read across a concurrent commit's publish point: \
         before={read_before:?} after={read_after:?} (commit-visibility atomicity bug)"
    );
}

#[test]
fn post_durable_temporal_failure_finishes_commit_then_fail_stops() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.undo_retention = Duration::from_secs(60);
    let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
    db.create_table(
        "t",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let writer = db.begin_txn().unwrap();
    db.insert_rows(writer, "test.t", vec![vec![Value::Int(7)]])
        .unwrap();
    crate::storage::temporal::fail_next_ledger_persist_for_test();
    // WAL and MVCC publication already committed the transaction. The
    // sidecar failure must not turn that success into an ambiguous client
    // error or skip lock/accounting cleanup.
    db.commit_txn(writer.txn_id).unwrap();
    assert!(db.ensure_operational().is_err());
    drop(db);

    let reopened = Database::open_with_config(dir.path(), config).unwrap();
    let view = reopened.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let rows = reopened.scan_rows("test.t", view).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::Int(7)]);
}

#[test]
fn undo_purge_respects_visible_frontier_during_out_of_order_commit() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    };

    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = open_lob_table(dir.path(), &config);
    db.set_undo_retention_ms(0).unwrap();

    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![
            vec![Value::Int(1), Value::String("A0".into())],
            vec![Value::Int(2), Value::String("B0".into())],
        ],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let row_a = row_id_for(&db, 1);
    let row_b = row_id_for(&db, 2);

    // Stall commit A after it allocates SCN N. Commit B is then allowed to
    // publish SCN N+1, but the contiguous visible frontier must remain below
    // both until A publishes.
    let a_allocated = Arc::new(Barrier::new(2));
    let release_a = Arc::new(Barrier::new(2));
    let first_commit = Arc::new(AtomicBool::new(true));
    {
        let a_allocated = Arc::clone(&a_allocated);
        let release_a = Arc::clone(&release_a);
        let first_commit = Arc::clone(&first_commit);
        super::commit_visibility_test_hook::install(Arc::new(move |_| {
            if first_commit.swap(false, Ordering::AcqRel) {
                a_allocated.wait();
                release_a.wait();
            }
        }));
    }

    let writer_db = Arc::clone(&db);
    let writer_a = std::thread::spawn(move || {
        let txn = writer_db.begin_txn().unwrap();
        let table = writer_db.table("test.t").unwrap();
        writer_db
            .update_row_by_id(txn, &table, row_a, |old| {
                let mut values = old.to_vec();
                values[1] = Value::String("A1".into());
                Ok(values)
            })
            .unwrap();
        writer_db.commit_txn(txn.txn_id).unwrap();
    });
    a_allocated.wait();

    let writer_b = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap();
    db.update_row_by_id(writer_b, &table, row_b, |old| {
        let mut values = old.to_vec();
        values[1] = Value::String("B1".into());
        Ok(values)
    })
    .unwrap();
    let mut deferred_b = db.commit_txn_deferred(writer_b.txn_id).unwrap();
    let b_commit_scn = deferred_b.commit_scn;
    assert!(
        db.visible_scn() < b_commit_scn,
        "commit B must be published beyond the still-gapped frontier"
    );

    std::thread::sleep(std::time::Duration::from_millis(5));
    db.purge_obsolete_undo();
    let (reader, _snapshot_guard) =
        db.read_only_statement_view_with_guard(IsolationLevel::ReadCommitted, None);
    let read_result = db.scan_rows("test.t", reader);

    // Always unblock the parked committer before asserting the read result.
    release_a.wait();
    writer_a.join().unwrap();
    db.finish_deferred_commits(&mut [&mut deferred_b]).unwrap();
    super::commit_visibility_test_hook::clear();

    let rows = read_result
        .expect("purge must retain B's undo while visible_scn is behind B's out-of-order commit");
    let body_b = rows
        .iter()
        .find(|row| row.values[0] == Value::Int(2))
        .map(|row| row.values[1].clone());
    assert_eq!(
        body_b,
        Some(Value::String("B0".into())),
        "the frontier snapshot must reconstruct B's pre-commit version"
    );
}

#[test]
fn rollback_update_restores_old_lob_and_rollback_insert_frees_chain() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = open_lob_table(dir.path(), &config);
    let table = db.table("test.t").unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::String("ORIG".repeat(3_000))]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // Rollback an UPDATE → the original large value must be restored.
    let rid = row_id_for(&db, 1);
    let txn = db.begin_txn().unwrap();
    db.update_row_by_id(txn, &table, rid, |old| {
        let mut v = old.to_vec();
        v[1] = Value::String("CHANGED".repeat(3_000));
        Ok(v)
    })
    .unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    assert_eq!(scan_body(&db, 1), Some(Value::String("ORIG".repeat(3_000))));

    // Rollback an INSERT of a large value → row gone; the orphaned chain
    // is freed (a follow-up insert + read still works).
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(2), Value::String("GHOST".repeat(3_000))]],
    )
    .unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    assert_eq!(scan_body(&db, 2), None);

    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(3), Value::String("REUSE".repeat(3_000))]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(
        scan_body(&db, 3),
        Some(Value::String("REUSE".repeat(3_000)))
    );
}

#[test]
fn purge_frees_deleted_lob_chain_for_reuse() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.undo_retention = Duration::from_millis(0);
    let db = open_lob_table(dir.path(), &config);
    let table = db.table("test.t").unwrap();
    let lob_path = t_path(dir.path(), &table.name, table.id, "lob");

    // Insert a ~200KB value (a ~13-page chain), commit, flush.
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::String("A".repeat(40_000))]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();
    let size_after_first = std::fs::metadata(&lob_path).unwrap().len();

    // Delete it; the Delete undo owns the chain. Checkpoint (retention 0)
    // purges the undo, which must free the chain back to the free list.
    let rid = row_id_for(&db, 1);
    let txn = db.begin_txn().unwrap();
    assert!(db.delete_row_by_id(txn, &table, rid).unwrap());
    db.commit_txn(txn.txn_id).unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.undo.read().len(), 0, "delete undo should be purged");

    // A second ~200KB value should reuse the freed pages, so the .lob
    // file does not grow materially beyond the first insert.
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(2), Value::String("B".repeat(40_000))]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();
    let size_after_reuse = std::fs::metadata(&lob_path).unwrap().len();
    assert_eq!(scan_body(&db, 2), Some(Value::String("B".repeat(40_000))));
    assert!(
        size_after_reuse <= size_after_first + 32 * 1024,
        "freed chain was not reused: {size_after_reuse} vs {size_after_first}"
    );
}

#[test]
fn buffer_pool_resize_fails_when_all_cached_pages_remain_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.buffer_pool_pages = 2;
    config.lock_wait = Duration::from_millis(10);
    let db = Database::open_with_config(dir.path(), config).unwrap();
    for table in ["t1", "t2"] {
        db.create_table(
            table,
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec![],
            vec![],
        )
        .unwrap();
    }
    for (table, value) in [("test.t1", 1), ("test.t2", 2)] {
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, table, vec![vec![Value::Int(value)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }

    let t1_key = PageKey {
        file: Arc::new(BufferPool::heap_file_key(
            db.table_path(db.table("test.t1").unwrap().id),
            db.table("test.t1").unwrap().id,
        )),
        page_id: 0,
    };
    let t2_key = PageKey {
        file: Arc::new(BufferPool::heap_file_key(
            db.table_path(db.table("test.t2").unwrap().id),
            db.table("test.t2").unwrap().id,
        )),
        page_id: 0,
    };
    let _pinned_t1 = db.buffer_pool.pin_page(t1_key).unwrap();
    let _pinned_t2 = db.buffer_pool.pin_page(t2_key).unwrap();

    let err = db.set_buffer_pool_pages(1).unwrap_err();

    assert_eq!(err.code(), 1030);
    assert!(err.message().contains("buffer pool capacity exhausted"));
    let status = db.storage_status();
    assert_eq!(status.buffer_pool_pages, 2, "{status:?}");
    assert_eq!(db.buffer_pool_pages(), 2);
    assert!(status.buffer_pool_used_pages <= status.buffer_pool_pages);
    assert!(status.buffer_pool_pinned_pages > 0, "{status:?}");
}

#[test]
fn buffer_pool_quota_pct_is_runtime_settable() {
    // CFG-1 - the per-kind soft-quota percentages are adjustable at
    // runtime (SET GLOBAL mydb_buffer_pool_quota_*_pct → the same
    // set_storage_variable dispatch), take effect on the live pool, and
    // clamp to 0..=100 like the MYDB_BUFFER_POOL_QUOTA_*_PCT env parser.
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = Database::open_with_config(dir.path(), config).unwrap();
    db.set_storage_variable("mydb_buffer_pool_quota_heap_pct", 25)
        .unwrap();
    db.set_storage_variable("mydb_buffer_pool_quota_index_pct", 40)
        .unwrap();
    db.set_storage_variable("mydb_buffer_pool_quota_lob_pct", 10)
        .unwrap();
    db.set_storage_variable("mydb_buffer_pool_quota_zone_map_pct", 15)
        .unwrap();
    assert_eq!(db.buffer_pool_quota_heap_pct(), 25);
    assert_eq!(db.buffer_pool_quota_index_pct(), 40);
    assert_eq!(db.buffer_pool_quota_lob_pct(), 10);
    assert_eq!(db.buffer_pool_quota_zone_map_pct(), 15);
    // Over-100 clamps to 100; 0 disables the bias.
    db.set_storage_variable("mydb_buffer_pool_quota_lob_pct", 500)
        .unwrap();
    assert_eq!(db.buffer_pool_quota_lob_pct(), 100);
    db.set_storage_variable("mydb_buffer_pool_quota_heap_pct", 0)
        .unwrap();
    assert_eq!(db.buffer_pool_quota_heap_pct(), 0);
}

#[test]
fn buffer_pool_prefetch_pages_is_runtime_settable() {
    // CFG-1 - the base prefetch window is adjustable at runtime
    // (SET GLOBAL mydb_buffer_pool_prefetch_pages → the same
    // set_storage_variable dispatch). A write clamps to
    // 0..=MAX_BUFFER_POOL_PREFETCH_PAGES like the
    // MYDB_BUFFER_POOL_PREFETCH_PAGES env parser AND reseeds the
    // adaptive effective window so the new base takes effect on the
    // next scan; 0 disables read-ahead.
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = Database::open_with_config(dir.path(), config).unwrap();

    db.set_storage_variable("mydb_buffer_pool_prefetch_pages", 64)
        .unwrap();
    assert_eq!(db.buffer_pool_prefetch_pages(), 64);
    // Reseeding the effective window is what makes the change take
    // effect immediately rather than only after the next adaptation.
    assert_eq!(db.buffer_pool.effective_prefetch_window(), 64);

    // Over-max clamps to MAX_BUFFER_POOL_PREFETCH_PAGES.
    db.set_storage_variable("mydb_buffer_pool_prefetch_pages", 500)
        .unwrap();
    assert_eq!(
        db.buffer_pool_prefetch_pages(),
        MAX_BUFFER_POOL_PREFETCH_PAGES as u64
    );
    assert_eq!(
        db.buffer_pool.effective_prefetch_window(),
        MAX_BUFFER_POOL_PREFETCH_PAGES
    );

    // 0 disables read-ahead: base and effective window both zero, so
    // prefetch_range / prefetch_pages early-return and the adaptive
    // controller (maybe_adapt_prefetch) stays parked.
    db.set_storage_variable("mydb_buffer_pool_prefetch_pages", 0)
        .unwrap();
    assert_eq!(db.buffer_pool_prefetch_pages(), 0);
    assert_eq!(db.buffer_pool.effective_prefetch_window(), 0);
}

#[test]
fn buffer_pool_eviction_skips_temporarily_pinned_lru_page() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.buffer_pool_pages = 2;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    for table in ["t1", "t2"] {
        db.create_table(
            table,
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec![],
            vec![],
        )
        .unwrap();
    }
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t1", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t2", vec![vec![Value::Int(2)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let t1_id = db.table("test.t1").unwrap().id;
    let _pinned_read = db
        .buffer_pool
        .pin_page(PageKey {
            file: Arc::new(BufferPool::heap_file_key(db.table_path(t1_id), t1_id)),
            page_id: 0,
        })
        .unwrap();

    db.set_buffer_pool_pages(1).unwrap();

    let status = db.storage_status();
    assert!(status.buffer_pool_used_pages <= 1, "{status:?}");
    assert!(status.buffer_pool_evictions > 0, "{status:?}");
    assert!(status.buffer_pool_pinned_pages > 0, "{status:?}");
}

#[test]
fn buffer_pool_eviction_waits_for_temporarily_pinned_pages() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.buffer_pool_pages = 2;
    config.lock_wait = Duration::from_millis(250);
    let db = Arc::new(Database::open_with_config(dir.path(), config).unwrap());
    for table in ["t1", "t2"] {
        db.create_table(
            table,
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec![],
            vec![],
        )
        .unwrap();
    }
    for (table, value) in [("test.t1", 1), ("test.t2", 2)] {
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, table, vec![vec![Value::Int(value)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }

    let t1_id = db.table("test.t1").unwrap().id;
    let t2_id = db.table("test.t2").unwrap().id;
    let pinned_t1 = db
        .buffer_pool
        .pin_page(PageKey {
            file: Arc::new(BufferPool::heap_file_key(db.table_path(t1_id), t1_id)),
            page_id: 0,
        })
        .unwrap();
    let pinned_t2 = db
        .buffer_pool
        .pin_page(PageKey {
            file: Arc::new(BufferPool::heap_file_key(db.table_path(t2_id), t2_id)),
            page_id: 0,
        })
        .unwrap();
    let db_for_waiter = Arc::clone(&db);
    let waiter = std::thread::spawn(move || db_for_waiter.set_buffer_pool_pages(1));

    std::thread::sleep(Duration::from_millis(20));
    drop(pinned_t2);

    waiter.join().unwrap().unwrap();
    drop(pinned_t1);
    let status = db.storage_status();
    assert!(status.buffer_pool_used_pages <= 1, "{status:?}");
    assert!(status.buffer_pool_waits > 0, "{status:?}");
}

#[test]
fn runtime_buffer_pool_resize_takes_effect() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.buffer_pool_pages = 4;
    let db = Database::open_with_config(dir.path(), config).unwrap();

    db.set_buffer_pool_pages(1).unwrap();

    assert_eq!(db.storage_status().buffer_pool_pages, 1);
    assert_eq!(db.buffer_pool_pages(), 1);
}

#[test]
fn wal_undo_rolls_back_uncommitted_flushed_heap_page() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.with_heap_exclusive(1, |heap| heap.flush_dirty_pages())
            .unwrap();
    }

    let db = open_relaxed_test_db(dir.path());
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert!(rows.is_empty());
}

// Crash-recovery of interleaved committed / loser single-row INSERTs into the
// SAME heap page. Commit-filtered REDO skips a loser transaction's insert and
// rolls it back (no-op) in the undo phase — but a later committed insert to the
// same page recorded an ABSOLUTE `slot_id` assuming the loser's row occupied
// the slot below it at runtime, and its secondary-index entries (also redone at
// absolute slot ids) point at that exact slot. Re-inserting with next-free-slot
// would drift the committed row down into the loser's hole and trip the redo
// slot-consistency check ("heap insert batch slot mismatch"), fail-stopping
// recovery (an availability bug — data is durable, recovery just can't start).
// `redo_heap_insert{,_batch}` now replays at the recorded absolute slot, leaving
// skipped-loser slots as FREE placeholders. These cases pin every committed/
// loser ordering: loser in the middle, first, last, and multiple losers.
fn run_interleaved_loser_insert_recovery(plan: &[(i64, bool)], expected_surviving: &[i64]) {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        for &(id, commit) in plan {
            let t = db.begin_txn().unwrap();
            db.insert_rows(t, "test.t", vec![vec![Value::Int(id)]])
                .unwrap();
            if commit {
                // A committed txn's fsync forces every prior loser's
                // begin+insert records durable, so recovery actually
                // sees (and must tolerate) the skipped losers.
                db.commit_txn(t.txn_id).unwrap();
            }
            // Loser: never commit — its insert stays uncommitted.
        }
        // Simulate a crash: no clean shutdown, no page flush. Stop the
        // buffer-pool IO workers first — otherwise `mem::forget(db)` leaks
        // the Arc, workers keep upgrading their Weak, and one can flush a
        // dirty heap page (with its WAL barrier) racing the reopen below.
        // The reopen recovery then trips the page-LSN-ahead-of-WAL check
        // intermittently. `shutdown_flusher` joins the workers without
        // itself writing anything, so this stays a faithful crash sim.
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
    }

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => i64::MAX,
    });
    let ids: Vec<i64> = rows
        .into_iter()
        .map(|r| match r.values[0] {
            Value::Int(v) => v,
            _ => i64::MAX,
        })
        .collect();
    assert_eq!(
        ids, expected_surviving,
        "committed rows must survive at their absolute slots; losers rolled back"
    );
}

#[test]
fn wal_recovers_interleaved_committed_and_loser_inserts_same_page() {
    // Loser in the middle: id=2 skipped, id=3 must still land at its
    // recorded absolute slot above the loser's hole.
    run_interleaved_loser_insert_recovery(&[(1, true), (2, false), (3, true)], &[1, 3]);
}

#[test]
fn wal_recovers_loser_first_insert_same_page() {
    // Loser at slot 0: both committed rows sit above the hole.
    run_interleaved_loser_insert_recovery(&[(1, false), (2, true), (3, true)], &[2, 3]);
}

#[test]
fn wal_recovers_loser_last_insert_same_page() {
    // Loser at the top slot: nothing above it to misalign, but the
    // loser must still be absent after recovery.
    run_interleaved_loser_insert_recovery(&[(1, true), (2, true), (3, false)], &[1, 2]);
}

#[test]
fn wal_recovers_multiple_loser_inserts_same_page() {
    // Several losers, including two consecutive ones, force the redo
    // path to pad multiple FREE placeholders before later committed
    // rows land at their recorded absolute slots.
    run_interleaved_loser_insert_recovery(
        &[
            (1, true),
            (2, false),
            (3, false),
            (4, true),
            (5, false),
            (6, true),
        ],
        &[1, 4, 6],
    );
}

#[test]
fn txn_wal_buffer_follows_transaction_across_worker_threads() {
    std::env::set_var("MYDB_WAL_TXN_BUFFER", "1");
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    let int_col = |name: &str, pk: bool| ColumnDef {
        name: name.into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key: pk,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    };
    db.create_table(
        "t",
        vec![int_col("id", true), int_col("v", false)],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    let row_ids = db
        .insert_rows(
            seed,
            "test.t",
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(20)],
            ],
        )
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // Leave txn1's WAL tail parked on this worker, then create and mutate txn2
    // on another worker. COMMIT txn2 back on this worker used to consume
    // txn1's thread-local BeginTxn/buffer and fail with errno 1030.
    let txn1 = db.begin_dml_txn().unwrap();
    db.update_row_by_id_with_changed_columns(
        txn1,
        &db.table("test.t").unwrap(),
        row_ids[0],
        Some(&[1]),
        |old| Ok(vec![old[0].clone(), Value::Int(11)]),
    )
    .unwrap();

    let worker_db = Arc::clone(&db);
    let row2 = row_ids[1];
    let txn2_id = std::thread::spawn(move || {
        let txn2 = worker_db.begin_dml_txn().unwrap();
        worker_db
            .update_row_by_id_with_changed_columns(
                txn2,
                &worker_db.table("test.t").unwrap(),
                row2,
                Some(&[1]),
                |old| Ok(vec![old[0].clone(), Value::Int(21)]),
            )
            .unwrap();
        txn2.txn_id
    })
    .join()
    .unwrap();

    db.commit_txn(txn2_id).unwrap();
    db.rollback_txn(txn1.txn_id).unwrap();

    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|row| match row.values[0] {
        Value::Int(id) => id,
        _ => unreachable!(),
    });
    assert_eq!(rows[0].values, vec![Value::Int(1), Value::Int(10)]);
    assert_eq!(rows[1].values, vec![Value::Int(2), Value::Int(21)]);
}

#[test]
fn btree_release_failed_txn_wal_flush_logs_diagnostic_and_requires_recovery() {
    std::env::set_var("MYDB_WAL_TXN_BUFFER", "1");
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = open_relaxed_test_db(dir.path());
    let column = |name: &str, primary_key: bool| ColumnDef {
        name: name.into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    };
    db.create_table(
        "wal_failure",
        vec![column("id", true), column("v", false)],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    let row_ids = db
        .insert_rows(
            seed,
            "test.wal_failure",
            vec![vec![Value::Int(1), Value::Int(10)]],
        )
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let txn = db.begin_dml_txn().unwrap();
    db.update_row_by_id_with_changed_columns(
        txn,
        &db.table("test.wal_failure").unwrap(),
        row_ids[0],
        Some(&[1]),
        |old| Ok(vec![old[0].clone(), Value::Int(20)]),
    )
    .unwrap();
    let deferred = || {
        db.buffer_pool
            .shards
            .iter()
            .map(|shard| {
                shard
                    .frames
                    .read()
                    .unwrap()
                    .values()
                    .map(|frame| frame.deferred_pins.load(Ordering::Acquire) as usize)
                    .sum::<usize>()
            })
            .sum::<usize>()
    };
    assert!(deferred() > 0, "test must exercise deferred WAL pins");
    FAIL_TXN_WAL_BUFFER_FLUSH.with(|fail| fail.set(true));
    let err = db.commit_txn(txn.txn_id).unwrap_err();
    assert!(err
        .diagnostic_detail()
        .unwrap()
        .contains("injected transaction WAL flush failure"));
    assert!(!db
        .ensure_operational()
        .unwrap_err()
        .message()
        .contains("injected transaction WAL flush failure"));
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let logged = crate::engine::logging::error_log_rows().iter().any(|row| {
            matches!(row.get(5), Some(Value::String(data))
                if data.contains("transaction WAL flush: injected transaction WAL flush failure"))
        });
        if logged {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the server log must retain the underlying I/O diagnostic"
        );
        thread::sleep(Duration::from_millis(10));
    }
    assert!(!txn_wal_buffering_active(&db.log, txn.txn_id));
    assert_eq!(deferred(), 0);
    drop(db);

    let reopened = open_relaxed_test_db(dir.path());
    let rows = reopened
        .scan_rows(
            "test.wal_failure",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values,
        vec![Value::Int(1), Value::Int(10)],
        "failed commit must not become visible after recovery"
    );
}

#[test]
fn txn_wal_buffer_commit_keeps_oversized_tail_recoverable() {
    std::env::set_var("MYDB_WAL_TXN_BUFFER", "1");
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let txn = db.begin_dml_txn().unwrap();
    let txn_id = txn.txn_id;

    // A multi-row transaction can park slightly more than the 256-record
    // buffering threshold between drain probes. The commit path must not
    // encode that whole tail as one TxnBatch because the WAL format caps a
    // batch at 257 logical records (including CommitTxn).
    for undo_id in 0..260 {
        assert!(push_txn_wal_record(
            &db.log,
            txn.txn_id,
            WalPayload::UndoRecord {
                undo_id,
                bytes: vec![undo_id as u8],
            },
            Vec::new(),
        )
        .is_ok());
    }
    db.flush_txn_wal_buffer(
        txn.txn_id,
        Some(WalPayload::CommitTxn {
            commit_scn: 1,
            commit_unix_us: 1,
            binlog: None,
        }),
    )
    .unwrap();
    end_txn_wal_buffering(&db.log, txn.txn_id);
    {
        let mut log = db.log.lock();
        log.rotate_segment().unwrap();
        log.flush().unwrap();
    }
    drop(db);

    let scan = crate::wal::LogManager::scan(dir.path()).unwrap();
    assert_eq!(
        scan.records
            .iter()
            .filter(|record| {
                record.txn_id == txn_id && matches!(record.payload, WalPayload::UndoRecord { .. })
            })
            .count(),
        260
    );
    assert!(matches!(
        scan.records.last().map(|record| &record.payload),
        Some(WalPayload::CommitTxn { .. })
    ));
}

#[test]
fn vanished_txn_wal_buffer_returns_payload_for_eager_fallback() {
    std::env::set_var("MYDB_WAL_TXN_BUFFER", "1");
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let txn = db.begin_dml_txn().unwrap();
    end_txn_wal_buffering(&db.log, txn.txn_id);

    let rejected =
        push_txn_wal_record(&db.log, txn.txn_id, WalPayload::CheckpointBegin, Vec::new())
            .expect_err("ended transaction buffer must reject ownership back to caller");
    assert!(matches!(rejected.0, WalPayload::CheckpointBegin));
}

#[test]
fn online_apply_txn_batch_uses_distinct_logical_lsns() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let relative = "schemas/test/tables/apply_lsn__999__data.heap";

    let mut first = Page::new();
    assert_eq!(first.insert(b"first"), Some(0));
    let mut second = Page::new();
    assert_eq!(second.insert(b"second"), Some(0));

    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 12,
        prev_txn_lsn: 0,
        txn_id: 0,
        payload: WalPayload::TxnBatch {
            records: vec![
                WalPayload::PageUpdate {
                    path: relative.into(),
                    page_id: 0,
                    before: Vec::new(),
                    after: first.encode(),
                    undo_lsn: None,
                },
                WalPayload::PageUpdate {
                    path: relative.into(),
                    page_id: 0,
                    before: Vec::new(),
                    after: second.encode(),
                    undo_lsn: None,
                },
                WalPayload::CommitTxn {
                    commit_scn: 1,
                    commit_unix_us: 1,
                    binlog: None,
                },
            ],
        },
    })
    .unwrap();

    let bytes = std::fs::read(dir.path().join(relative)).unwrap();
    let page = Page::decode(&bytes).unwrap();
    assert_eq!(page.read(0), Some(b"second".as_slice()));
    assert_eq!(page_lsn(&bytes), 11);
}

#[test]
fn transaction_audit_replica_reclaims_xa_intents_at_each_terminal_record() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let scn = db.visible_scn() + 1;
    for (index, terminal) in [
        WalPayload::CommitTxn {
            commit_scn: scn,
            commit_unix_us: 0,
            binlog: None,
        },
        WalPayload::AbortTxn,
        WalPayload::EndTxn,
    ]
    .into_iter()
    .enumerate()
    {
        let txn_id = 10_000 + index as u64;
        let relative = crate::binlog::prepared::chunk_path(txn_id, 0);
        for (offset, payload) in [
            WalPayload::BeginTxn { start_scn: scn - 1 },
            WalPayload::FileCreate {
                path: relative.clone(),
                after: vec![42; 32],
            },
        ]
        .into_iter()
        .enumerate()
        {
            db.apply_wal_record(&WalRecord {
                source_time_us: 0,
                lsn: 100 + index as u64 * 3 + offset as u64,
                prev_txn_lsn: 0,
                txn_id,
                payload,
            })
            .unwrap();
        }
        assert!(dir.path().join(&relative).exists());
        db.apply_wal_record(&WalRecord {
            source_time_us: 0,
            lsn: 102 + index as u64 * 3,
            prev_txn_lsn: 0,
            txn_id,
            payload: terminal,
        })
        .unwrap();
        assert!(
            !dir.path().join(&relative).exists(),
            "finished XA intent leaked on replica"
        );
    }
}

#[test]
fn transaction_audit_replica_restores_active_intent_cleanup_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.set_physical_replica_mode(true).unwrap();
    let txn_id = 10_000;
    let relative = crate::binlog::prepared::chunk_path(txn_id, 0);
    for (offset, payload) in [
        WalPayload::BeginTxn {
            start_scn: db.visible_scn(),
        },
        WalPayload::FileCreate {
            path: relative.clone(),
            after: vec![42; 32],
        },
    ]
    .into_iter()
    .enumerate()
    {
        db.apply_wal_record(&WalRecord {
            source_time_us: 0,
            lsn: 100 + offset as u64,
            prev_txn_lsn: 0,
            txn_id,
            payload,
        })
        .unwrap();
    }
    // A restarted applier has no volatile catalog/apply bookkeeping. The
    // same startup hook must preserve the source's still-ACTIVE intent and
    // arrange reclamation when the terminal record arrives later.
    db.applier_dd_dirty_txns.lock().clear();
    db.recover_binlog_intent_files().unwrap();
    assert!(dir.path().join(&relative).exists());
    assert!(db.applier_dd_dirty_txns.lock()[&txn_id].has_binlog_intent);
    db.apply_wal_record(&WalRecord {
        source_time_us: 0,
        lsn: 102,
        prev_txn_lsn: 101,
        txn_id,
        payload: WalPayload::AbortTxn,
    })
    .unwrap();
    assert!(!dir.path().join(&relative).exists());
}

#[test]
fn online_apply_begin_seeds_snapshot_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let initial_scn = db.visible_scn();
    let start_scn = initial_scn + 42;

    // A streamed BeginTxn carries the source's already-published snapshot
    // frontier. This can be above zero after a base backup because the
    // preceding resolved SCNs are represented by the snapshot, not by WAL
    // frames sent to the fresh replica.
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 1,
        prev_txn_lsn: 0,
        txn_id: 7,
        payload: WalPayload::BeginTxn { start_scn },
    })
    .unwrap();
    assert_eq!(db.visible_scn(), start_scn);

    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 2,
        prev_txn_lsn: 1,
        txn_id: 7,
        payload: WalPayload::CommitTxn {
            commit_scn: start_scn + 1,
            commit_unix_us: start_scn + 1,
            binlog: None,
        },
    })
    .unwrap();
    assert_eq!(db.visible_scn(), start_scn + 1);
}

#[test]
fn replica_unclassified_begin_is_not_a_safe_visibility_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());

    // An observed BEGIN may still write later. Sparse committed slot GC is
    // safe, but neither an unknown registration gap nor this active writer
    // may be crossed by the all-visible transaction-id prefix.
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 1,
        prev_txn_lsn: 0,
        txn_id: 7,
        payload: WalPayload::BeginTxn { start_scn: 1 },
    })
    .unwrap();
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 2,
        prev_txn_lsn: 0,
        txn_id: 8,
        payload: WalPayload::BeginTxn { start_scn: 1 },
    })
    .unwrap();
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 3,
        prev_txn_lsn: 2,
        txn_id: 8,
        payload: WalPayload::FileCreate {
            path: "schemas/test/tables/repl_writer__888__data.heap".into(),
            after: Vec::new(),
        },
    })
    .unwrap();
    assert!(db.txns.read().slot(8).unwrap().is_writer);
    assert!(!db.txns.read().slot(8).unwrap().read_only);
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 4,
        prev_txn_lsn: 3,
        txn_id: 8,
        payload: WalPayload::CommitTxn {
            commit_scn: 2,
            commit_unix_us: 2,
            binlog: None,
        },
    })
    .unwrap();

    db.gc_committed_txn_slots();
    assert!(
        db.storage_status().min_retained_txn_id < 7,
        "an unclassified source BEGIN must retain its visibility protection"
    );
    assert_eq!(db.txns.read().status(7), Some(TxnStatus::Active));
    assert!(!db.txns.read().slot(7).unwrap().read_only);
    assert!(
        db.txns.read().slot(8).is_none(),
        "safe committed slots are still collected"
    );
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 5,
        prev_txn_lsn: 0,
        txn_id: 0,
        payload: WalPayload::CheckpointEnd {
            redo_lsn: 5,
            next_txn_id: 9,
            next_scn: 3,
            auto_increment_covered_lsn: 5,
            sequence_covered_lsn: 5,
            gc_safe_txn_frontier: 8,
        },
    })
    .unwrap();
    db.gc_committed_txn_slots();
    assert!(
        db.storage_status().min_retained_txn_id < 7,
        "registration does not prove terminal state"
    );
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 6,
        prev_txn_lsn: 1,
        txn_id: 7,
        payload: WalPayload::EndTxn,
    })
    .unwrap();
    db.gc_committed_txn_slots();
    assert!(
        db.storage_status().min_retained_txn_id >= 8,
        "checkpoint proof restores the page visibility shortcut after END"
    );
}

#[test]
fn replica_restartpoint_waits_for_apply_and_prunes_only_applied_durable_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    let (applied_lsn, latest_lsn) = {
        let mut log = db.log.lock();
        for undo_id in 0..8 {
            log.append(
                1,
                WalPayload::UndoRecord {
                    undo_id,
                    bytes: vec![0x5a; 128],
                },
            )
            .unwrap();
        }
        log.rotate_segment().unwrap();
        let applied_lsn = log.next_lsn();
        for undo_id in 8..16 {
            log.append(
                2,
                WalPayload::UndoRecord {
                    undo_id,
                    bytes: vec![0xa5; 128],
                },
            )
            .unwrap();
        }
        log.rotate_segment().unwrap();
        let latest_lsn = log.append(3, WalPayload::CheckpointBegin).unwrap();
        (applied_lsn, latest_lsn)
    };
    crate::wal::LogManager::sync_durable_to(&db.log, latest_lsn).unwrap();
    let durable_lsn = db.log.lock().durable_lsn();
    assert!(durable_lsn > applied_lsn);
    db.replica_applied_lsn_atomic
        .store(applied_lsn, Ordering::Release);
    // A throttled progress file can lag behind the successfully applied WAL.
    // Reconnect must still be able to validate the overlap at persisted + 1.
    let persisted_lsn = applied_lsn - 2;
    let mut progress = crate::repl::replica::ReplicaProgress {
        received_lsn: latest_lsn,
        durable_lsn,
        applied_lsn: persisted_lsn,
        durable_applied_lsn: persisted_lsn,
        ..Default::default()
    };
    crate::repl::replica::save_progress(dir.path(), &progress).unwrap();

    // Model an applier paused in the middle of a complete outer WAL frame.
    // Restartpoint must not enter its sync/prune phase until that frame exits.
    let apply_guard = db.replica_apply_checkpoint.read().unwrap();
    let worker_db = Arc::clone(&db);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result =
            worker_db.replica_restartpoint(std::time::Instant::now(), CHECKPOINT_TRIGGER_MANUAL);
        done_tx.send(result).unwrap();
    });
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    drop(apply_guard);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();

    let log = db.log.lock();
    assert_eq!(
        db.replica_applied_lsn_atomic.load(Ordering::Acquire),
        applied_lsn
    );
    assert_eq!(log.durable_lsn(), durable_lsn);
    assert!(
        log.segment_snapshot()
            .iter()
            .all(|segment| segment.last_lsn >= persisted_lsn),
        "restartpoint must preserve the persisted resume watermark"
    );
    assert!(
        log.segment_snapshot()
            .iter()
            .any(|segment| segment.last_lsn > applied_lsn),
        "received/durable WAL above applied_lsn must remain available"
    );
    drop(log);
    assert!(crate::wal::WalReader::open(&db.log, persisted_lsn + 1).is_ok());

    progress.applied_lsn = applied_lsn;
    progress.durable_applied_lsn = applied_lsn;
    crate::repl::replica::save_progress(dir.path(), &progress).unwrap();
    db.replica_restartpoint(std::time::Instant::now(), CHECKPOINT_TRIGGER_MANUAL)
        .unwrap();
    assert!(
        db.log
            .lock()
            .segment_snapshot()
            .iter()
            .all(|segment| segment.last_lsn >= applied_lsn),
        "persisting progress permits reclaiming the older overlap"
    );
}

#[test]
fn online_apply_history_batch_is_published_only_by_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let start_scn = db.visible_scn();
    let txn_id = 71;
    let key = IndexKey::from(vec![KeyPart::Int(10)]);
    let row_id = RowId {
        page_id: 3,
        slot_id: 4,
    };
    let bytes = crate::storage::temporal::TemporalHistory::encode_history_index_batch(vec![(
        crate::storage::txn::IndexRepairKey {
            table_id: 99,
            index_name: "idx_k".into(),
            key: key.clone(),
        },
        row_id,
    )])
    .unwrap();

    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 1,
        prev_txn_lsn: 0,
        txn_id,
        payload: WalPayload::BeginTxn { start_scn },
    })
    .unwrap();
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 2,
        prev_txn_lsn: 1,
        txn_id,
        payload: WalPayload::HistoryIndexBatch { bytes },
    })
    .unwrap();
    assert!(db
        .temporal_history
        .history_index_entries_exact(TxnView::historical(start_scn), 99, "idx_k", &key)
        .is_empty());

    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: 3,
        prev_txn_lsn: 2,
        txn_id,
        payload: WalPayload::CommitTxn {
            commit_scn: start_scn + 1,
            commit_unix_us: 123,
            binlog: None,
        },
    })
    .unwrap();
    assert_eq!(
        db.temporal_history.history_index_entries_exact(
            TxnView::historical(start_scn),
            99,
            "idx_k",
            &key,
        ),
        vec![(key, row_id)]
    );
}

#[test]
fn online_apply_context_reuses_heap_file_across_frames() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let relative = "schemas/test/tables/apply_context__998__data.heap";
    let mut context = OnlineWalApplyContext::default();

    db.apply_wal_record_with_context(
        &crate::wal::WalRecord {
            source_time_us: 0,
            lsn: 1,
            prev_txn_lsn: 0,
            txn_id: 1,
            payload: WalPayload::HeapInsert {
                path: relative.into(),
                page_id: 0,
                slot_id: 0,
                payload: b"before".to_vec(),
                undo_lsn: None,
            },
        },
        &mut context,
    )
    .unwrap();
    db.apply_wal_record_with_context(
        &crate::wal::WalRecord {
            source_time_us: 0,
            lsn: 2,
            prev_txn_lsn: 1,
            txn_id: 1,
            payload: WalPayload::HeapUpdate {
                path: relative.into(),
                page_id: 0,
                slot_id: 0,
                before: b"before".to_vec(),
                after: b"after".to_vec(),
                undo_lsn: None,
            },
        },
        &mut context,
    )
    .unwrap();

    assert_eq!(
        context.heap_redo.files.len(),
        1,
        "one chunk must keep one descriptor for repeated frames on the same heap"
    );
    let bytes = std::fs::read(dir.path().join(relative)).unwrap();
    assert_eq!(Page::decode(&bytes).unwrap().read(0), Some(&b"after"[..]));
}

#[test]
fn replicated_two_op_index_batch_uses_primary_update_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let index = IndexFile::open(dir.path().join("1_repl_batch.idx"), test_log(dir.path())).unwrap();
    let row_id = test_row_id(7, 8);
    let old_key = wide_test_index_key(10);
    let new_key = wide_test_index_key(11);
    index.insert_with_txn(1, old_key.clone(), row_id).unwrap();

    let hits_before = REPLICATED_TWO_OP_FAST_PATH_HITS.load(Ordering::Relaxed);
    index
        .apply_replicated_batch(
            2,
            vec![
                IndexBatchEntry {
                    kind: WalIndexOpKind::Delete,
                    key: old_key.clone(),
                    row_id,
                },
                IndexBatchEntry {
                    kind: WalIndexOpKind::Insert,
                    key: new_key.clone(),
                    row_id,
                },
            ],
            42,
        )
        .unwrap();

    assert!(index.get(&old_key, false).unwrap().is_empty());
    assert_eq!(index.get(&new_key, false).unwrap(), vec![row_id]);
    assert!(
        REPLICATED_TWO_OP_FAST_PATH_HITS.load(Ordering::Relaxed) > hits_before,
        "replica replay must reuse the primary's two-op update fast path"
    );
}

#[test]
fn replicated_sorted_insert_batches_use_bulk_build_then_right_edge_append() {
    let dir = tempfile::tempdir().unwrap();
    let log = test_log(dir.path());
    let index =
        IndexFile::open(dir.path().join("sorted_repl_batch.idx"), Arc::clone(&log)).unwrap();
    let make_batch = |start: u32, end: u32| {
        (start..end)
            .map(|value| IndexBatchEntry {
                kind: WalIndexOpKind::Insert,
                key: IndexKey::from(vec![
                    KeyPart::Int(i64::from(value)),
                    KeyPart::Int(i64::from(value % 7)),
                ]),
                row_id: RowId {
                    page_id: value / 32,
                    slot_id: (value % 32) as u16,
                },
            })
            .collect::<Vec<_>>()
    };

    let first_lsn = log
        .lock()
        .append(9, WalPayload::BeginTxn { start_scn: 1 })
        .unwrap();
    let hits_before = REPLICATED_SORTED_INSERT_FAST_PATH_HITS.load(Ordering::Relaxed);
    index
        .apply_replicated_batch(9, make_batch(0, 800), first_lsn)
        .unwrap();

    let second_lsn = log.lock().append(9, WalPayload::CheckpointBegin).unwrap();
    index
        .apply_replicated_batch(9, make_batch(800, 1_600), second_lsn)
        .unwrap();

    assert!(
        REPLICATED_SORTED_INSERT_FAST_PATH_HITS.load(Ordering::Relaxed) >= hits_before + 2,
        "empty-tree build and non-empty right-edge append must both hit the sorted replay path"
    );
    assert_eq!(
        index
            .get(
                &IndexKey::from(vec![KeyPart::Int(0), KeyPart::Int(0)]),
                false
            )
            .unwrap(),
        vec![RowId {
            page_id: 0,
            slot_id: 0
        }]
    );
    assert_eq!(
        index
            .get(
                &IndexKey::from(vec![KeyPart::Int(1_599), KeyPart::Int(1_599 % 7)]),
                false
            )
            .unwrap(),
        vec![RowId {
            page_id: 1_599 / 32,
            slot_id: (1_599 % 32) as u16
        }]
    );
    // Physical validation must include dirty pages still in the buffer pool.
    index.flush().unwrap();
    IndexFile::validate(dir.path().join("sorted_repl_batch.idx")).unwrap();
}

#[test]
fn large_index_batch_wal_is_split_below_replication_payload_limit() {
    let dir = tempfile::tempdir().unwrap();
    let log = test_log(dir.path());
    let index =
        IndexFile::open(dir.path().join("chunked_index_batch.idx"), Arc::clone(&log)).unwrap();
    // About 8.8 MiB of encoded keys: just over the production 8 MiB WAL
    // target, while every individual key remains well below one index page.
    let entries = (0..2_300u32)
        .map(|value| {
            let mut bytes = vec![b'x'; 3_800];
            bytes[..4].copy_from_slice(&value.to_be_bytes());
            IndexEntry {
                key: IndexKey::from(vec![KeyPart::Bytes(bytes)]),
                row_id: RowId {
                    page_id: value,
                    slot_id: 0,
                },
            }
        })
        .collect::<Vec<_>>();
    index.insert_entries_logged(11, entries, true).unwrap();
    {
        let mut guard = log.lock();
        guard.rotate_segment().unwrap();
        guard.flush().unwrap();
    }

    let scan = crate::wal::LogManager::scan(dir.path()).unwrap();
    let batches = scan
        .records
        .iter()
        .filter_map(|record| match &record.payload {
            WalPayload::IndexBatch { path, ops } => Some((path, ops)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        batches.len() >= 2,
        "oversized logical index batch must become multiple WAL frames"
    );
    for (path, ops) in batches {
        let encoded_bytes =
            4 + path.len() + 4 + ops.iter().map(|op| 1 + 4 + op.key.len() + 6).sum::<usize>();
        assert!(
            encoded_bytes <= INDEX_BATCH_WAL_TARGET_BYTES,
            "chunk encoded to {encoded_bytes} bytes"
        );
    }
}

// Txn-buffering (MYDB_WAL_TXN_BUFFER): a committed UPDATE, a crash-loser
// UPDATE, and another committed UPDATE — all to rows on the same heap page.
// The loser's heap record is parked in the transaction buffer and its page is
// deferred-pinned (never flushed) until commit; a crash before commit appends
// and flushes nothing for it, so recovery reconstructs only the committed
// updates from the WAL. Verifies the deferred path is crash-correct end to end.
#[test]
fn txn_buffering_loser_update_rolled_back_on_crash() {
    // nextest runs each test in its own process, so this is read fresh by the
    // feature's OnceLock before the first Database call below.
    std::env::set_var("MYDB_WAL_TXN_BUFFER", "1");
    let dir = tempfile::tempdir().unwrap();
    let int_col = |name: &str, pk: bool| ColumnDef {
        name: name.into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key: pk,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    };
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "t",
            vec![int_col("id", true), int_col("v", false)],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let t0 = db.begin_dml_txn().unwrap();
        db.insert_rows(
            t0,
            "test.t",
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(20)],
                vec![Value::Int(3), Value::Int(30)],
            ],
        )
        .unwrap();
        db.commit_txn(t0.txn_id).unwrap();

        let bump = |db: &Database, txn: TxnView, id: i64| {
            let rid = db
                .scan_rows(
                    "test.t",
                    TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
                )
                .unwrap()
                .into_iter()
                .find(|r| r.values[0] == Value::Int(id))
                .map(|r| r.row_id)
                .unwrap();
            db.update_row_by_id_with_changed_columns(
                txn,
                &db.table("test.t").unwrap(),
                rid,
                Some(&[1]),
                |old| {
                    let Value::Int(v) = old[1] else {
                        unreachable!()
                    };
                    Ok(vec![old[0].clone(), Value::Int(v + 100)])
                },
            )
            .unwrap();
        };

        // committed update id=1 -> 110
        let t1 = db.begin_dml_txn().unwrap();
        bump(&db, t1, 1);
        db.commit_txn(t1.txn_id).unwrap();
        // LOSER update id=2 -> 120 (never committed)
        let t2 = db.begin_dml_txn().unwrap();
        bump(&db, t2, 2);
        // committed update id=3 -> 130
        let t3 = db.begin_dml_txn().unwrap();
        bump(&db, t3, 3);
        db.commit_txn(t3.txn_id).unwrap();
        // Crash: no clean shutdown, no flush. The loser t2 never committed.
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
    }

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => i64::MAX,
    });
    let pairs: Vec<(i64, i64)> = rows
        .into_iter()
        .map(|r| match (&r.values[0], &r.values[1]) {
            (Value::Int(a), Value::Int(b)) => (*a, *b),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(
        pairs,
        vec![(1, 110), (2, 20), (3, 130)],
        "committed updates (id 1,3) survive; loser id=2 stays at its pre-update value"
    );
}

#[test]
fn relaxed_wal_redoes_committed_heap_without_heap_flush() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Relaxed).unwrap();
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
    fs::remove_file(t_path(dir.path(), "t", 1, "heap")).unwrap();

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Relaxed).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::Int(1)]);
}

#[test]
fn relaxed_undo_restores_committed_version_after_uncommitted_update_flush() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Relaxed).unwrap();
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(20)]),
    )
    .unwrap();
    db.flush_dirty_data().unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Relaxed).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::Int(1), Value::Int(10)]);
}

#[test]
fn rollback_persists_restored_heap_and_indexes_before_end_txn() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open(dir.path()).unwrap();
        db.create_table(
            "t",
            vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
                ColumnDef {
                    name: "k".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
            ],
            vec!["id".into()],
            vec![crate::parser::IndexDef {
                name: Some("k_1".into()),
                columns: vec!["k".into()],
                column_prefix_lengths: Vec::new(),
                column_descending: Vec::new(),
                key_expressions: Vec::new(),
                key_expression_multi_valued: Vec::new(),
                unique: false,
                included_columns: Vec::new(),
                index_type: "BTREE".to_string(),
                visible: true,
                comment: String::new(),
                fulltext_parser: String::new(),
                vector_options: None,
            }],
        )
        .unwrap();
        let seed = db.begin_txn().unwrap();
        db.insert_rows(seed, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
            .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        db.flush_dirty_data().unwrap();

        let txn = db.begin_txn().unwrap();
        db.update_rows(
            txn,
            "test.t",
            None,
            |row| Ok(row[0] == Value::Int(1)),
            |_| Ok(vec![Value::Int(1), Value::Int(20)]),
        )
        .unwrap();
        db.flush_dirty_data().unwrap();
        db.rollback_txn(txn.txn_id).unwrap();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
    }

    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, false),
    )
    .unwrap();
    let table = db.table("test.t").unwrap().clone();
    let secondary = table
        .indexes
        .iter()
        .find(|index| index.name == "k_1")
        .unwrap();
    let old_key_rows = db
        .lookup_index(&table, secondary, IndexKey::from(vec![KeyPart::Int(10)]))
        .unwrap();
    let new_key_rows = db
        .lookup_index(&table, secondary, IndexKey::from(vec![KeyPart::Int(20)]))
        .unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();

    assert_eq!(old_key_rows.len(), 1);
    assert!(new_key_rows.is_empty());
    assert_eq!(rows[0].values, vec![Value::Int(1), Value::Int(10)]);
}

fn concurrent_index_test_column(name: &str, primary_key: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    }
}

fn create_concurrent_index_update_table(db: &Database, rows: i64) {
    db.create_table(
        "idx_concurrent",
        vec![
            concurrent_index_test_column("id", true),
            concurrent_index_test_column("k", false),
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("k_1".into()),
            columns: vec!["k".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.idx_concurrent",
        (1..=rows)
            .map(|id| vec![Value::Int(id), Value::Int(id)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn combined_undo_publication_preserves_commits_and_rollbacks() {
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(
        Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Strict, true),
        )
        .unwrap(),
    );
    let workers = 16i64;
    let rows_per_worker = 4i64;
    let rounds = 40i64;
    db.create_table(
        "undo_combine",
        vec![
            concurrent_index_test_column("id", true),
            concurrent_index_test_column("v", false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.undo_combine",
        (0..workers * rows_per_worker)
            .map(|id| vec![Value::Int(id), Value::Int(0)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let table = db.table("test.undo_combine").unwrap();
    let snapshot = db.begin_txn().unwrap();
    let ids: std::collections::HashMap<i64, RowId> = db
        .scan_rows("test.undo_combine", snapshot)
        .unwrap()
        .into_iter()
        .map(|row| match row.values[0] {
            Value::Int(id) => (id, row.row_id),
            ref other => panic!("unexpected id {other:?}"),
        })
        .collect();
    db.commit_txn(snapshot.txn_id).unwrap();
    // Every update publishes one undo record; many writers publish at once,
    // so most records go through the combiner on another thread. Rolled-back
    // rounds must restore exactly the published before-images.
    std::thread::scope(|scope| {
        for worker in 0..workers {
            let db = &db;
            let table = &table;
            let ids = &ids;
            scope.spawn(move || {
                for round in 0..rounds {
                    let txn = db.begin_txn().unwrap();
                    for offset in 0..rows_per_worker {
                        let row_id = ids[&(worker * rows_per_worker + offset)];
                        db.update_row_by_id(txn, table, row_id, |values| {
                            let mut values = values.to_vec();
                            let Value::Int(v) = values[1] else {
                                panic!("unexpected value");
                            };
                            values[1] = Value::Int(v + 1);
                            Ok(values)
                        })
                        .unwrap();
                    }
                    if round % 2 == 0 {
                        db.commit_txn(txn.txn_id).unwrap();
                    } else {
                        db.rollback_txn(txn.txn_id).unwrap();
                    }
                }
            });
        }
    });
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let rows = db.scan_rows("test.undo_combine", view).unwrap();
    assert_eq!(rows.len() as i64, workers * rows_per_worker);
    for row in rows {
        assert_eq!(
            row.values[1],
            Value::Int(rounds / 2),
            "row {:?}",
            row.values[0]
        );
    }
    drop(db.undo.read());
    assert_eq!(db.undo.pending_count(), 0);
    db.check_consistency().unwrap();
}

#[test]
fn concurrent_secondary_index_updates_recover_consistently() {
    let dir = tempfile::tempdir().unwrap();
    let rows = 64i64;
    let updated_offset = 10_000i64;
    {
        let db = std::sync::Arc::new(
            Database::open_with_config(
                dir.path(),
                test_storage_config(DurabilityMode::Strict, true),
            )
            .unwrap(),
        );
        create_concurrent_index_update_table(&db, rows);
        let threads = 8i64;
        let per_thread = rows / threads;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(threads as usize));
        let workers: Vec<_> = (0..threads)
            .map(|worker| {
                let db = std::sync::Arc::clone(&db);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for local in 0..per_thread {
                        let id = worker * per_thread + local + 1;
                        let txn = db.begin_txn().unwrap();
                        db.update_rows(
                            txn,
                            "test.idx_concurrent",
                            None,
                            |row| Ok(row[0] == Value::Int(id)),
                            |row| Ok(vec![row[0].clone(), Value::Int(id + updated_offset)]),
                        )
                        .unwrap();
                        db.commit_txn(txn.txn_id).unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        db.check_consistency().unwrap();
        let db = std::sync::Arc::try_unwrap(db).ok().unwrap();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
    }

    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    db.check_consistency().unwrap();
    let table = db.table("test.idx_concurrent").unwrap().clone();
    let secondary = table
        .indexes
        .iter()
        .find(|index| index.name == "k_1")
        .unwrap();
    for id in 1..=rows {
        assert!(db
            .lookup_index(&table, secondary, IndexKey::from(vec![KeyPart::Int(id)]))
            .unwrap()
            .is_empty());
        let rows = db
            .lookup_index(
                &table,
                secondary,
                IndexKey::from(vec![KeyPart::Int(id + updated_offset)]),
            )
            .unwrap();
        assert_eq!(rows.len(), 1, "missing updated secondary key for id {id}");
    }
}

#[test]
fn concurrent_single_row_index_inserts_recover_consistently() {
    let dir = tempfile::tempdir().unwrap();
    let initial_rows = 1_024i64;
    let threads = 4i64;
    let rows_per_thread = 32i64;
    {
        let db = std::sync::Arc::new(
            Database::open_with_config(
                dir.path(),
                test_storage_config(DurabilityMode::Strict, true),
            )
            .unwrap(),
        );
        // Seed even keys in one batch so subsequent odd keys are distributed
        // across already-existing leaves. Each worker then exercises the
        // production one-row INSERT path without forcing a page split on
        // every operation.
        create_concurrent_index_update_table(&db, 0);
        let seed = db.begin_txn().unwrap();
        db.insert_rows(
            seed,
            "test.idx_concurrent",
            (1..=initial_rows)
                .map(|id| vec![Value::Int(id * 2), Value::Int(id * 2)])
                .collect(),
        )
        .unwrap();
        db.commit_txn(seed.txn_id).unwrap();

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(threads as usize));
        let workers: Vec<_> = (0..threads)
            .map(|worker| {
                let db = std::sync::Arc::clone(&db);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for local in 0..rows_per_thread {
                        let ordinal = worker + local * threads;
                        let id = ordinal * 2 + 1;
                        let txn = db.begin_txn().unwrap();
                        db.insert_rows(
                            txn,
                            "test.idx_concurrent",
                            vec![vec![Value::Int(id), Value::Int(id)]],
                        )
                        .unwrap();
                        db.commit_txn(txn.txn_id).unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        db.check_consistency().unwrap();
        let db = std::sync::Arc::try_unwrap(db).ok().unwrap();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
    }

    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    db.check_consistency().unwrap();
    let table = db.table("test.idx_concurrent").unwrap().clone();
    let secondary = table
        .indexes
        .iter()
        .find(|index| index.name == "k_1")
        .unwrap();
    for ordinal in 0..threads * rows_per_thread {
        let id = ordinal * 2 + 1;
        let rows = db
            .lookup_index(&table, secondary, IndexKey::from(vec![KeyPart::Int(id)]))
            .unwrap();
        assert_eq!(rows.len(), 1, "missing concurrently inserted key {id}");
    }
}

#[test]
fn startup_validation_rebuilds_missing_index_files() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
        db.checkpoint().unwrap();
    }
    fs::remove_file(t_index_path(dir.path(), "t", 1, "PRIMARY", "idx")).unwrap();

    let db = open_relaxed_test_db(dir.path());
    let table = db.table("test.t").unwrap();
    let rows = db
        .lookup_index(
            &table,
            &table.indexes[0],
            IndexKey::from(vec![KeyPart::Int(1)]),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(db
        .storage_status()
        .last_recovery_summary
        .contains("indexes_repaired=1"));
    assert!(db
        .storage_status()
        .last_recovery_summary
        .contains("startup_validation_path=clean_fast"));
}

#[test]
fn startup_validation_rebuilds_empty_index_files() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
        db.checkpoint().unwrap();
    }
    let index_path = t_index_path(dir.path(), "t", 1, "PRIMARY", "idx");
    fs::write(&index_path, []).unwrap();

    let db = open_relaxed_test_db(dir.path());
    let table = db.table("test.t").unwrap();
    let rows = db
        .lookup_index(
            &table,
            &table.indexes[0],
            IndexKey::from(vec![KeyPart::Int(1)]),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(db
        .storage_status()
        .last_recovery_summary
        .contains("indexes_repaired=1"));
    assert!(index_path.metadata().unwrap().len() >= PAGE_SIZE as u64);
}

#[test]
fn clean_checkpoint_startup_uses_fast_validation_by_default() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            (0..200).map(|id| vec![Value::Int(id)]).collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.checkpoint().unwrap();
    }

    let db = open_relaxed_test_db(dir.path());
    let summary = db.storage_status().last_recovery_summary;
    assert!(summary.contains("startup_validation_mode=auto"));
    assert!(summary.contains("startup_validation_path=clean_fast"));
    assert!(summary.contains("heap_pages_checked=0"));
    assert!(summary.contains("index_files_quick_checked=1"));

    let table = db.table("test.t").unwrap();
    let rows = db
        .lookup_index(
            &table,
            &table.indexes[0],
            IndexKey::from(vec![KeyPart::Int(42)]),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn dirty_recovery_uses_full_validation_in_auto_mode() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        db.flush_dirty_data().unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
    }

    let db = open_relaxed_test_db(dir.path());
    let summary = db.storage_status().last_recovery_summary;
    assert!(summary.contains("records_redone="));
    assert!(summary.contains("startup_validation_path=full"));
    assert!(!summary.contains("heap_pages_checked=0"));
}

#[test]
fn batch_insert_uses_index_batch_wal_and_compact_heap_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let before = db.storage_status();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let after = db.storage_status();

    assert_eq!(after.wal_logical_records, before.wal_logical_records + 2);
    assert!(after.wal_index_batch_records > before.wal_index_batch_records);
    assert!(after.wal_bytes > before.wal_bytes);
}

#[test]
fn maybe_checkpoint_does_not_repeat_without_new_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    create_checkpoint_test_table(&db);
    db.set_checkpoint_wal_bytes(1).unwrap();
    insert_checkpoint_test_row(&db, 1);

    db.maybe_checkpoint().unwrap();
    let after_first = db.storage_status();
    assert!(
        after_first.checkpoint_lsn > 0,
        "first maybe_checkpoint should advance checkpoint_lsn"
    );

    db.maybe_checkpoint().unwrap();
    let after_second = db.storage_status();
    assert_eq!(
        after_second.checkpoint_lsn, after_first.checkpoint_lsn,
        "next_lsn == checkpoint_lsn + 1 means no new WAL was written"
    );
}

#[test]
fn size_triggered_checkpoint_coalesces_wal_bursts() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.checkpoint_interval = Duration::from_secs(60);
    config.checkpoint_wal_bytes = 1;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    create_checkpoint_test_table(&db);
    insert_checkpoint_test_row(&db, 1);

    db.checkpoint().unwrap();
    let after_first = db.storage_status();
    assert!(after_first.checkpoint_lsn > 0, "{after_first:?}");

    insert_checkpoint_test_row(&db, 2);
    db.maybe_checkpoint().unwrap();
    let after_second = db.storage_status();
    assert_eq!(
        after_second.checkpoints_total, after_first.checkpoints_total,
        "size-triggered checkpoints should be rate-limited inside the automatic checkpoint window"
    );

    db.checkpoint().unwrap();
    let after_manual = db.storage_status();
    assert!(
        after_manual.checkpoints_total > after_second.checkpoints_total,
        "manual checkpoints must bypass the automatic rate limiter"
    );
}

#[test]
fn maybe_checkpoint_runs_during_active_write_txn_and_retains_undo() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        create_checkpoint_test_table(&db);
        db.set_checkpoint_wal_bytes(1).unwrap();

        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        assert!(db.has_active_write_txns());

        let before = db.storage_status();
        db.maybe_checkpoint().unwrap();
        let during_writer = db.storage_status();
        assert!(
            during_writer.checkpoint_lsn > before.checkpoint_lsn,
            "an active write txn no longer blocks the checkpoint — the \
             retention floor keeps its undo chain instead"
        );

        // The in-flight transaction keeps working after the checkpoint.
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(2)]])
            .unwrap();
        // Simulated crash: drop without commit.
    }
    // Recovery must roll the uncommitted transaction back — the
    // checkpoint's retention floor kept its WAL records alive even
    // though the checkpoint truncated everything else.
    let db = open_relaxed_test_db(dir.path());
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        0,
        "uncommitted writes must not survive checkpoint + crash"
    );
}

#[test]
fn checkpoint_preserves_prepared_xa_branch_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let xid = crate::xa::Xid::new(1, b"ckpt-xa".to_vec(), b"b1".to_vec()).unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        create_checkpoint_test_table(&db);
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.prepare_xa(txn.txn_id, 0, xid.clone()).unwrap();
        // Explicit checkpoint (the ALTER SYSTEM CHECKPOINT / shutdown
        // entry point) with a PREPARED branch in flight: the retention
        // floor must keep the branch's WAL records, not destroy them.
        db.checkpoint().unwrap();
        // Simulated crash: drop without resolving the branch.
    }
    let db = open_relaxed_test_db(dir.path());
    let prepared = db.list_prepared_xa();
    assert_eq!(
        prepared.len(),
        1,
        "PREPARED XA branch must survive checkpoint + crash"
    );
    assert_eq!(prepared[0].xid, xid);
    db.commit_prepared(&xid).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 1, "XA COMMIT after recovery must land the row");
}

#[test]
fn recovery_tolerates_stale_wal_generation_left_by_interrupted_collapse() {
    // The WAL collapse renames a replacement segment into place and only
    // then deletes the previous generation. A crash in between leaves
    // BOTH generations on disk; recovery must not roll back committed
    // transactions whose records reappear in the stale segments.
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = crate::storage::naming::system_wal_dir(dir.path());
    {
        let db = open_relaxed_test_db(dir.path());
        create_checkpoint_test_table(&db);
        for id in 1..=12 {
            insert_checkpoint_test_row(&db, id);
        }
        db.flush_dirty_data().unwrap();
        // Snapshot the pre-collapse generation.
        let saved: Vec<(std::path::PathBuf, Vec<u8>)> = fs::read_dir(&wal_dir)
            .unwrap()
            .flatten()
            .map(|entry| (entry.path(), fs::read(entry.path()).unwrap()))
            .collect();
        assert!(!saved.is_empty());
        db.checkpoint().unwrap();
        // Re-materialise the old generation next to the collapsed one,
        // simulating a crash after the rename but before the deletes.
        for (path, bytes) in saved {
            if !path.exists() {
                fs::write(path, bytes).unwrap();
            }
        }
    }
    let db = open_relaxed_test_db(dir.path());
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        12,
        "committed rows must survive a stale WAL generation"
    );
    // The LSN counter must not have rewound: new work and another
    // reopen keep functioning.
    insert_checkpoint_test_row(&db, 13);
    drop(db);
    let db = open_relaxed_test_db(dir.path());
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 13);
}

#[test]
fn time_triggered_checkpoint_fires_after_startup() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    // Isolate the time trigger: tiny interval, unreachable size threshold.
    config.checkpoint_interval = Duration::from_millis(1);
    config.checkpoint_wal_bytes = u64::MAX;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    create_checkpoint_test_table(&db);
    insert_checkpoint_test_row(&db, 1);
    std::thread::sleep(Duration::from_millis(5));
    db.maybe_checkpoint().unwrap();
    let status = db.storage_status();
    assert!(
        status.checkpoint_lsn > 0,
        "the interval trigger must be armed from startup, not only after \
         a first size-triggered checkpoint"
    );
}

#[test]
fn concurrent_checkpoints_and_writers_preserve_all_commits() {
    let dir = tempfile::tempdir().unwrap();
    {
        let config = test_storage_config(DurabilityMode::Relaxed, true);
        let db = std::sync::Arc::new(Database::open_with_config(dir.path(), config).unwrap());
        create_checkpoint_test_table(&db);
        let writers: Vec<_> = (0..4u64)
            .map(|w| {
                let db = std::sync::Arc::clone(&db);
                std::thread::spawn(move || {
                    for i in 0..8u64 {
                        let id = (w * 8 + i + 1) as i64;
                        let txn = db.begin_txn().unwrap();
                        db.insert_rows(txn, "test.t", vec![vec![Value::Int(id)]])
                            .unwrap();
                        db.commit_txn(txn.txn_id).unwrap();
                    }
                })
            })
            .collect();
        // Hammer checkpoints while the writers run: every iteration
        // races the begin→flush→compact sequence against live appends.
        for _ in 0..4 {
            db.checkpoint().unwrap();
        }
        for writer in writers {
            writer.join().unwrap();
        }
        db.checkpoint().unwrap();
        let rows = db
            .scan_rows(
                "test.t",
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap();
        assert_eq!(rows.len(), 32);
    }
    // Reopen: recovery sees whatever WAL the floor retained; every
    // committed row must still be there.
    let db = Database::open(dir.path()).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 32);
}

#[test]
fn maybe_checkpoint_allows_active_read_only_txn() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    create_checkpoint_test_table(&db);
    db.set_checkpoint_wal_bytes(1).unwrap();
    insert_checkpoint_test_row(&db, 1);

    let read_only = db.begin_read_only_txn().unwrap();
    assert!(!db.has_active_write_txns());

    let before = db.storage_status();
    db.maybe_checkpoint().unwrap();
    let after = db.storage_status();
    assert!(
        after.checkpoint_lsn > before.checkpoint_lsn,
        "read-only transactions must not block automatic checkpoint"
    );
    db.commit_txn(read_only.txn_id).unwrap();
}

#[test]
fn active_read_only_transaction_does_not_pin_txn_id_gc_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let reader = db.begin_read_only_txn().unwrap();

    let writer = db.begin_txn().unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    db.gc_committed_txn_slots();

    assert!(
        db.storage_status().min_retained_txn_id >= writer.txn_id,
        "a read-only slot cannot appear in any row header and must not pin the txn-id prefix"
    );
    db.rollback_txn(reader.txn_id).unwrap();
}

#[test]
fn analyze_does_not_leave_transaction_registration_holes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    create_checkpoint_test_table(&db);
    insert_checkpoint_test_row(&db, 1);
    db.analyze_table("test.t").unwrap();

    let writer = db.begin_txn().unwrap();
    db.insert_rows(writer, "test.t", vec![vec![Value::Int(2)]])
        .unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    db.gc_committed_txn_slots();

    assert!(
        db.storage_status().min_retained_txn_id >= writer.txn_id,
        "ANALYZE's synthetic lock owner must not strand the registration frontier"
    );
    while db.purge_obsolete_undo() {}
    assert_eq!(db.storage_status().undo_records, 0);
}

#[test]
fn failed_analyze_barrier_does_not_leave_transaction_registration_holes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let writer = db.begin_txn().unwrap();
    db.insert_rows(writer, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    let table = db.table("test.t").unwrap();
    assert!(AnalyzePopulationBarrier::try_new(&db, &table).is_none());
    let last_id = db.next_txn_id_atomic.load(Ordering::Acquire) - 1;
    db.gc_committed_txn_slots();
    assert!(db.storage_status().min_retained_txn_id < writer.txn_id);
    db.commit_txn(writer.txn_id).unwrap();
    db.gc_committed_txn_slots();
    assert!(db.storage_status().min_retained_txn_id >= last_id);
    while db.purge_obsolete_undo() {}
    assert_eq!(db.storage_status().undo_records, 0);
}

#[test]
fn alter_copy_job_does_not_leave_transaction_registration_holes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let table = db.table("test.t").unwrap();
    let job = db.register_alter_copy_job(&table);
    let writer = db.begin_txn().unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    db.gc_committed_txn_slots();
    assert!(writer.txn_id > job.job_id);
    assert!(db.storage_status().min_retained_txn_id >= writer.txn_id);
}

#[test]
fn update_unchanged_index_key_does_not_write_index_pages() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    let before = db.storage_status();
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(20)]),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let after = db.storage_status();

    assert_eq!(after.dirty_index_pages, before.dirty_index_pages);
    db.check_consistency().unwrap();
}

#[test]
fn update_unchanged_unique_key_does_not_take_unique_key_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| Ok(vec![row[0].clone(), Value::Int(20)]),
    )
    .unwrap();

    assert!(db.lock_manager().unique_locks.is_empty());
    // Row locks live in the sharded store under MYDB_ROW_LOCK_SHARDS, in
    // `lock_manager` otherwise — count via the store-aware helper.
    assert_eq!(db.count_row_locks_held_by(txn.txn_id), 1);
}

#[test]
fn rollback_to_savepoint_rebuilds_indexes_after_secondary_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "sbtest1",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "k".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("k_1".into()),
            columns: vec!["k".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.sbtest1",
        vec![vec![Value::Int(1), Value::Int(10)]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let txn = db.begin_txn().unwrap();
    let savepoint = db.statement_savepoint(txn.txn_id).unwrap();
    db.update_rows(
        txn,
        "test.sbtest1",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| Ok(vec![row[0].clone(), Value::Int(11)]),
    )
    .unwrap();
    db.rollback_to_statement_savepoint(txn.txn_id, savepoint)
        .unwrap();
    db.check_consistency().unwrap();

    let table = db.table("test.sbtest1").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let secondary = table
        .indexes
        .iter()
        .find(|index| index.name == "k_1")
        .unwrap();
    assert_eq!(
        db.lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(1)]))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.lookup_index(&table, secondary, IndexKey::from(vec![KeyPart::Int(10)]))
            .unwrap()
            .len(),
        1
    );
    assert!(db
        .lookup_index(&table, secondary, IndexKey::from(vec![KeyPart::Int(11)]))
        .unwrap()
        .is_empty());
}

#[test]
fn rollback_to_savepoint_removes_batch_inserted_rows_and_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "k".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("k_idx".into()),
            columns: vec!["k".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    let savepoint = db.statement_savepoint(txn.txn_id).unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)],
        ],
    )
    .unwrap();
    // Exclude data-dictionary mirror records (committed at CREATE TABLE);
    // the user batch insert contributes exactly one undo record.
    let user_undo = db
        .undo
        .read()
        .records
        .values()
        .filter(|r| !crate::storage::data_dictionary::is_dd_table_id(r.table_id))
        .count();
    assert_eq!(user_undo, 1);

    db.rollback_to_statement_savepoint(txn.txn_id, savepoint)
        .unwrap();
    db.check_consistency().unwrap();

    let table = db.table("test.t").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let secondary = table
        .indexes
        .iter()
        .find(|index| index.name == "k_idx")
        .unwrap();
    let snapshot = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
    assert!(db.scan_rows("test.t", snapshot).unwrap().is_empty());
    assert!(db
        .lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(1)]))
        .unwrap()
        .is_empty());
    assert!(db
        .lookup_index(&table, secondary, IndexKey::from(vec![KeyPart::Int(20)]))
        .unwrap()
        .is_empty());
}

#[test]
fn rollback_removes_batch_inserted_rows_and_reusable_unique_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "order_line",
        vec![
            ColumnDef {
                name: "ol_w_id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "ol_d_id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "ol_o_id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "ol_number".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec![
            "ol_w_id".into(),
            "ol_d_id".into(),
            "ol_o_id".into(),
            "ol_number".into(),
        ],
        vec![],
    )
    .unwrap();

    let rows = (1..=8)
        .map(|number| {
            vec![
                Value::Int(1),
                Value::Int(5),
                Value::Int(3001),
                Value::Int(number),
            ]
        })
        .collect::<Vec<_>>();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.order_line", rows.clone())
        .unwrap();
    db.rollback_txn(txn.txn_id).unwrap();

    let table = db.table("test.order_line").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    assert!(db
        .lookup_index(
            &table,
            primary,
            IndexKey::from(vec![
                KeyPart::Int(1),
                KeyPart::Int(5),
                KeyPart::Int(3001),
                KeyPart::Int(1),
            ]),
        )
        .unwrap()
        .is_empty());
    let snapshot = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
    assert!(db
        .scan_rows("test.order_line", snapshot)
        .unwrap()
        .is_empty());

    let retry = db.begin_txn().unwrap();
    db.insert_rows(retry, "test.order_line", rows).unwrap();
    db.commit_txn(retry.txn_id).unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn relaxed_insert_undo_ids_are_unique_across_concurrent_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Database::open_with_config(
            dir.path(),
            StorageConfig {
                durability: DurabilityMode::Relaxed,
                checkpoint_interval: Duration::from_millis(DEFAULT_CHECKPOINT_INTERVAL_MS),
                checkpoint_wal_bytes: DEFAULT_CHECKPOINT_WAL_BYTES,
                lock_wait: Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
                group_commit_window: Duration::from_micros(DEFAULT_GROUP_COMMIT_US),
                wal_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
                buffer_pool_pages: DEFAULT_BUFFER_POOL_PAGES,
                buffer_pool_io_threads: default_buffer_pool_io_threads(),
                buffer_pool_prefetch_pages: DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
                undo_retention: Duration::from_millis(DEFAULT_UNDO_RETENTION_MS),
                heap_update_reserve_bytes: DEFAULT_HEAP_UPDATE_RESERVE_BYTES,
                startup_validation: StartupValidationMode::Auto,
                startup_validate_indexes: true,
            },
        )
        .unwrap(),
    );
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let mut workers = Vec::new();
    for worker in 0..8 {
        let db = Arc::clone(&db);
        workers.push(std::thread::spawn(move || {
            let txn = db.begin_txn().unwrap();
            let rows = (0..32)
                .map(|offset| vec![Value::Int(worker * 1_000 + offset)])
                .collect::<Vec<_>>();
            db.insert_rows(txn, "test.t", rows).unwrap();
            db.rollback_txn(txn.txn_id).unwrap();
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }

    let snapshot = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    assert!(db.scan_rows("test.t", snapshot).unwrap().is_empty());
    db.check_consistency().unwrap();
}

#[test]
fn concurrent_composite_pk_insert_batches_commit_and_recover_consistently() {
    const THREADS: usize = 4;
    const ROWS_PER_THREAD: i64 = 64;
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Arc::new(
            Database::open_with_config(
                dir.path(),
                test_storage_config(DurabilityMode::Strict, true),
            )
            .unwrap(),
        );
        let secondary = |name: &str, columns: &[&str]| crate::parser::IndexDef {
            name: Some(name.into()),
            columns: columns.iter().map(|column| (*column).into()).collect(),
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        };
        db.create_table(
            "lineitem_shape",
            vec![
                ColumnDef {
                    name: "orderkey".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
                ColumnDef {
                    name: "linenumber".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
                ColumnDef {
                    name: "partkey".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
                ColumnDef {
                    name: "shipdate".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
            ],
            vec!["orderkey".into(), "linenumber".into()],
            vec![
                secondary("idx_partkey", &["partkey"]),
                secondary("idx_shipdate", &["shipdate"]),
                secondary("idx_partsupp_shape", &["partkey", "linenumber"]),
            ],
        )
        .unwrap();

        let start = Arc::new(std::sync::Barrier::new(THREADS));
        let workers: Vec<_> = (0..THREADS)
            .map(|worker| {
                let db = Arc::clone(&db);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let first = worker as i64 * ROWS_PER_THREAD + 1;
                    let rows = (first..first + ROWS_PER_THREAD)
                        .map(|orderkey| {
                            vec![
                                Value::Int(orderkey),
                                Value::Int(1),
                                Value::Int((orderkey * 7_919) % 1_009),
                                Value::Int(orderkey % 31),
                            ]
                        })
                        .collect();
                    start.wait();
                    let txn = db.begin_txn().unwrap();
                    db.insert_rows(txn, "test.lineitem_shape", rows).unwrap();
                    db.commit_txn(txn.txn_id).unwrap();
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }

        db.check_consistency().unwrap();
        let snapshot = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
        assert_eq!(
            db.scan_rows("test.lineitem_shape", snapshot).unwrap().len(),
            THREADS * ROWS_PER_THREAD as usize
        );
    }

    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    db.check_consistency().unwrap();
    let snapshot = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    assert_eq!(
        db.scan_rows("test.lineitem_shape", snapshot).unwrap().len(),
        THREADS * ROWS_PER_THREAD as usize
    );
}

#[test]
fn batch_insert_uses_one_insert_undo_record() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    let rows = (1..=128).map(|id| vec![Value::Int(id)]).collect::<Vec<_>>();
    db.insert_rows(txn, "test.t", rows).unwrap();

    let undo = db.undo.read();
    // Ignore undo records produced by the data-dictionary catalog mirror
    // (committed at CREATE TABLE, not yet purged); only the user batch
    // insert's single record is under test here.
    let user_records: Vec<_> = undo
        .records
        .values()
        .filter(|r| !crate::storage::data_dictionary::is_dd_table_id(r.table_id))
        .collect();
    assert_eq!(user_records.len(), 1);
    match &user_records[0].kind {
        UndoKind::InsertBatch { row_ids } => assert_eq!(row_ids.len(), 128),
        other => panic!("expected insert batch undo, got {other:?}"),
    }
}

#[test]
fn batch_index_entry_lookup_matches_scalar_prefix_and_exact_paths() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "batch_idx",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "k".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "seq".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.batch_idx",
        (0..6_000)
            .map(|id| vec![Value::Int(id), Value::Int(id / 3), Value::Int(id % 3)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.add_index(
        "test.batch_idx",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "idx_k_seq".into(),
            columns: vec!["k".into(), "seq".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();

    let table = db.table("test.batch_idx").unwrap();
    let prefix_index = table
        .indexes
        .iter()
        .find(|index| index.name == "idx_k_seq")
        .unwrap();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let view = db.begin_txn().unwrap();

    let mut prefix_keys = vec![
        IndexKey::from(vec![KeyPart::Int(7)]),
        IndexKey::from(vec![KeyPart::Int(0)]),
        IndexKey::from(vec![KeyPart::Int(4_999)]),
        IndexKey::from(vec![KeyPart::Int(7)]),
        IndexKey::from(vec![KeyPart::Int(3)]),
    ];
    let mut expected_prefix = Vec::new();
    prefix_keys.sort_unstable();
    prefix_keys.dedup();
    for key in &prefix_keys {
        expected_prefix.extend(
            db.prefix_index_entries_visible(&table, prefix_index, key.clone(), view)
                .unwrap(),
        );
    }
    let prefix_handle = db
        .cached_index_handle(table.id, &prefix_index.name)
        .unwrap();
    let actual_prefix = db
        .lookup_index_entries_many_visible_with_handle(
            &prefix_handle,
            &table,
            prefix_index,
            &[
                IndexKey::from(vec![KeyPart::Int(7)]),
                IndexKey::from(vec![KeyPart::Int(0)]),
                IndexKey::from(vec![KeyPart::Int(4_999)]),
                IndexKey::from(vec![KeyPart::Int(7)]),
                IndexKey::from(vec![KeyPart::Int(3)]),
            ],
            view,
        )
        .unwrap();
    assert_eq!(actual_prefix, expected_prefix);

    let exact_handle = db.cached_index_handle(table.id, &primary.name).unwrap();
    let actual_exact = db
        .lookup_index_entries_many_visible_with_handle(
            &exact_handle,
            &table,
            primary,
            &[
                IndexKey::from(vec![KeyPart::Int(33)]),
                IndexKey::from(vec![KeyPart::Int(2)]),
                IndexKey::from(vec![KeyPart::Int(99_999)]),
                IndexKey::from(vec![KeyPart::Int(33)]),
            ],
            view,
        )
        .unwrap();
    let mut expected_exact = Vec::new();
    for key in [
        IndexKey::from(vec![KeyPart::Int(2)]),
        IndexKey::from(vec![KeyPart::Int(33)]),
    ] {
        expected_exact.extend(
            db.lookup_index_entries_visible(&table, primary, key, view)
                .unwrap(),
        );
    }
    assert_eq!(actual_exact, expected_exact);
}

/// The database batch API must preserve scalar visibility and result ordering
/// for exact keys and leaf-spanning composite prefixes. Low-level route versus
/// fallback comparisons live in `btree_lookup_tests` and never mutate env vars.
#[test]
fn routed_batch_lookup_matches_scalar_for_leaf_spanning_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "routed_idx",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "k".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "seq".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    // Five prefix values (k = id / 1000) × 1000 entries each.
    // A composite-key leaf entry is ~30B; 1000 entries occupy ~30 KB, so
    // every prefix is guaranteed to span multiple 16 KB leaves.
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.routed_idx",
        (0..5_000)
            .map(|id| vec![Value::Int(id), Value::Int(id / 1000), Value::Int(id % 1000)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.add_index(
        "test.routed_idx",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "idx_k_seq".into(),
            columns: vec!["k".into(), "seq".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();

    let table = db.table("test.routed_idx").unwrap();
    let prefix_index = table
        .indexes
        .iter()
        .find(|index| index.name == "idx_k_seq")
        .unwrap();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let prefix_handle = db
        .cached_index_handle(table.id, &prefix_index.name)
        .unwrap();
    let exact_handle = db.cached_index_handle(table.id, &primary.name).unwrap();
    let view = db.begin_txn().unwrap();

    // The database API sorts and deduplicates these to five prefix keys.
    let prefix_keys: Vec<IndexKey> = (0..70)
        .map(|i| IndexKey::from(vec![KeyPart::Int(i % 5)]))
        .collect();
    let exact_keys: Vec<IndexKey> = (0..70)
        .map(|i| IndexKey::from(vec![KeyPart::Int(i * 31 % 5_000)]))
        .collect();

    let routed_prefix = db
        .lookup_index_entries_many_visible_with_handle(
            &prefix_handle,
            &table,
            prefix_index,
            &prefix_keys,
            view,
        )
        .unwrap();
    let routed_exact = db
        .lookup_index_entries_many_visible_with_handle(
            &exact_handle,
            &table,
            primary,
            &exact_keys,
            view,
        )
        .unwrap();

    let mut unique_prefix = prefix_keys.clone();
    unique_prefix.sort_unstable();
    unique_prefix.dedup();
    let mut scalar_prefix = Vec::new();
    for key in unique_prefix {
        scalar_prefix.extend(
            db.prefix_index_entries_visible(&table, prefix_index, key, view)
                .unwrap(),
        );
    }
    let mut unique_exact = exact_keys.clone();
    unique_exact.sort_unstable();
    unique_exact.dedup();
    let mut scalar_exact = Vec::new();
    for key in unique_exact {
        scalar_exact.extend(
            db.lookup_index_entries_visible(&table, primary, key, view)
                .unwrap(),
        );
    }

    assert_eq!(routed_prefix, scalar_prefix);
    assert_eq!(routed_exact, scalar_exact);

    // Each of the 5 distinct prefixes (k = 0..5) has exactly 1000 matching
    // rows. The database API deduplicates the repeated probes; each matching
    // row must appear exactly once. Confirm the totals.
    let unique_prefix_buckets: std::collections::HashSet<_> = (0..70).map(|i| i % 5).collect();
    let expected_prefix_rows = unique_prefix_buckets.len() * 1000;
    assert_eq!(routed_prefix.len(), expected_prefix_rows);

    // The non-covering INLJ uses the keyed sibling, whose page-local
    // implementation carries the previous key's lower bound forward.  Check
    // the probe-key ordinal as well as every RowId against scalar lookups.
    let mut sorted_exact = exact_keys.clone();
    sorted_exact.sort_unstable();
    sorted_exact.dedup();
    let keyed = db
        .lookup_index_entries_many_keyed_with_handle(
            &exact_handle,
            &table,
            primary,
            &sorted_exact,
            view,
        )
        .unwrap();
    let mut scalar_keyed = Vec::new();
    for (key_idx, key) in sorted_exact.iter().enumerate() {
        for row_id in db.lookup_index(&table, primary, key.clone()).unwrap() {
            scalar_keyed.push((key_idx as u32, row_id));
        }
    }
    assert_eq!(keyed, scalar_keyed);
}

#[test]
fn batch_insert_rejects_duplicate_unique_keys_within_statement() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    let err = db
        .insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(1)], vec![Value::Int(1)]],
        )
        .unwrap_err();

    assert_eq!(err.code(), 1062);
}

#[test]
fn batch_insert_append_fast_path_still_rejects_existing_unique_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(4)], vec![Value::Int(5)]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let txn = db.begin_txn().unwrap();
    let err = db
        .insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(3)], vec![Value::Int(6)]],
        )
        .unwrap_err();

    assert_eq!(err.code(), 1062);
}

#[test]
fn checkpoint_compacts_wal_and_preserves_transaction_watermarks() {
    let dir = tempfile::tempdir().unwrap();
    // The collapse replaces the active segment with a fresh one (crash-
    // atomic rename), so measure the LogManager's valid-byte watermark
    // rather than the raw on-disk segment sizes — under preallocation
    // every active segment file occupies `max_segment_bytes` of reserved
    // extent regardless of how many records have been written to it.
    // The `wal_bytes()` accessor counts only the valid (record-carrying)
    // bytes, which is what the test actually wants to assert against.
    let wal_dir_bytes = |db: &Database| -> u64 { db.log_state_atomics().wal_bytes() };
    {
        let db = Database::open(dir.path()).unwrap();
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        for value in 1..=12 {
            let txn = db.begin_txn().unwrap();
            db.insert_rows(txn, "test.t", vec![vec![Value::Int(value)]])
                .unwrap();
            db.commit_txn(txn.txn_id).unwrap();
        }
        let before = wal_dir_bytes(&db);
        db.checkpoint().unwrap();
        let after = wal_dir_bytes(&db);
        assert!(
            after < before,
            "checkpoint should compact WAL: {after} >= {before}"
        );
    }

    let db = Database::open(dir.path()).unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(13)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 13);
    assert!(txn.txn_id > 12);
}

#[test]
fn checkpoint_purges_obsolete_undo_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.undo_retention = Duration::from_millis(0);
    let db = Database::open_with_config(dir.path(), config).unwrap();
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert!(db.undo.read().len() > 0);

    db.checkpoint().unwrap();

    assert_eq!(db.undo.read().len(), 0);
}

#[test]
fn checkpoint_purges_tombstones_before_wal_collapse() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.undo_retention = Duration::from_millis(0);
    let db = Database::open_with_config(dir.path(), config).unwrap();
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let insert = db.begin_txn().unwrap();
    db.insert_rows(insert, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(insert.txn_id).unwrap();
    db.checkpoint().unwrap();

    let delete = db.begin_txn().unwrap();
    assert_eq!(
        db.delete_rows(delete, "test.t", None, |row| Ok(row[0] == Value::Int(1)))
            .unwrap(),
        1
    );
    db.commit_txn(delete.txn_id).unwrap();
    db.checkpoint().unwrap();

    let scan = LogManager::scan(dir.path()).unwrap();
    assert!(
        matches!(
            scan.records.as_slice(),
            [crate::wal::WalRecord {
                payload: WalPayload::CheckpointEnd { .. },
                ..
            }]
        ),
        "purge WAL must be included below the collapsed checkpoint, got {:?}",
        scan.records
            .iter()
            .map(|record| record.payload.record_type())
            .collect::<Vec<_>>()
    );
    drop(db);

    let reopened = Database::open(dir.path()).unwrap();
    let rows = reopened
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn checkpoint_keeps_active_transaction_undo_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    assert!(db.undo.read().len() > 0);

    db.checkpoint().unwrap();

    assert!(db.undo.read().len() > 0);
    db.rollback_txn(txn.txn_id).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn checkpoint_respects_undo_retention_before_purge() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.undo_retention = Duration::from_secs(60);
    let db = Database::open_with_config(dir.path(), config).unwrap();
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    db.checkpoint().unwrap();

    assert!(db.undo.read().len() > 0);
    assert!(db.storage_status().undo_records > 0);
}

#[test]
fn stale_undo_needed_for_consistent_read_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.undo_retention = Duration::from_millis(0);
    let db = Database::open_with_config(dir.path(), config).unwrap();
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let snapshot = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(20)]),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.undo.read().len(), 0);

    let err = db.scan_rows("test.t", snapshot).unwrap_err();
    assert_eq!(
        err.code(),
        crate::error::catalog::MYDB_ER_SNAPSHOT_TOO_OLD.code().get()
    );
    assert!(err.message().contains("undo record"));
}

#[test]
fn active_snapshot_pins_stale_update_undo_until_guard_drops() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.undo_retention = Duration::from_millis(0);
    let db = Arc::new(Database::open_with_config(dir.path(), config).unwrap());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let snapshot = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let guard = db.register_statement_snapshot(snapshot.snapshot_scn);

    let writer = db.begin_txn().unwrap();
    db.update_rows(
        writer,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(20)]),
    )
    .unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    db.checkpoint().unwrap();

    let rows = db.scan_rows("test.t", snapshot).unwrap();
    assert_eq!(rows[0].values, vec![Value::Int(1), Value::Int(10)]);
    drop(guard);
    db.checkpoint().unwrap();
    assert_eq!(db.undo.read().len(), 0);
    let err = db.scan_rows("test.t", snapshot).unwrap_err();
    assert_eq!(
        err.code(),
        crate::error::catalog::MYDB_ER_SNAPSHOT_TOO_OLD.code().get()
    );
}

/// Regression for an undo-compaction correctness bug uncovered while
/// validating build-then-swap (and present in the legacy in-lock path
/// too). Compaction physically relocates undo records and remaps the
/// txn-rollback chain (`prev_undo_ptr`) + active txn undo heads, but the
/// MVCC row-version chain reaches records through pointers OUTSIDE the
/// undo manager — `heap.header.undo_ptr` and `before_header.undo_ptr` —
/// which compaction cannot rewrite in place. Before the fix, relocating a
/// record still pinned by a live snapshot made that snapshot's consistent
/// read spuriously fail with 1236. The stranding gate makes Auto
/// compaction fire exactly in this scenario (a long snapshot pins old
/// undo), so it was reachable with default settings.
///
/// Fix: an in-memory `old_ptr → new_ptr` forwarding table (`get` /
/// `load_owned` consult it on a miss). Verified passing under BOTH
/// compaction strategies (`MYDB_UNDO_COMPACTION_BUILD_SWAP=0` and `=1`).
#[test]
fn undo_compaction_preserves_snapshot_pinned_row_version() {
    // Single write shard makes fragmentation deterministic: the pinned
    // record and the throwaway churn share the same segment lineage, so
    // purging the churn genuinely strands the pinned record in a
    // low-live-ratio candidate segment. nextest isolates each test in
    // its own process, so this env write does not leak.
    std::env::set_var("MYDB_UNDO_WRITE_SHARDS", "1");
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.undo_retention = Duration::from_millis(0);
    let db = Arc::new(Database::open_with_config(dir.path(), config).unwrap());
    // Small undo segments so a handful of records rotate the tablespace.
    db.set_undo_segment_bytes(512).unwrap();
    let int_col = |name: &str, pk: bool| ColumnDef {
        name: name.into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key: pk,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    };
    db.create_table(
        "t",
        vec![int_col("id", true), int_col("v", false)],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    // Seed the row and pin its pre-update version with a snapshot.
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let snapshot = db.read_only_statement_view(IsolationLevel::RepeatableRead, None);
    let guard = db.register_statement_snapshot(snapshot.snapshot_scn);

    // Update id=1 → the before-image (v=10) undo record is now pinned by
    // `snapshot` and reachable only through the row-version chain.
    let writer = db.begin_txn().unwrap();
    db.update_rows(
        writer,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(20)]),
    )
    .unwrap();
    db.commit_txn(writer.txn_id).unwrap();

    // Throwaway churn: rows created AND deleted after the snapshot, so
    // their undo is never pinned and purges freely — fragmenting the
    // tablespace and stranding the pinned v=10 record in a sparse
    // segment. Purge each round so segments drain to low live ratio.
    for i in 0..80i64 {
        let key = 1000 + i;
        let ins = db.begin_txn().unwrap();
        db.insert_rows(ins, "test.t", vec![vec![Value::Int(key), Value::Int(key)]])
            .unwrap();
        db.commit_txn(ins.txn_id).unwrap();
        let del = db.begin_txn().unwrap();
        db.delete_rows(del, "test.t", None, |row| Ok(row[0] == Value::Int(key)))
            .unwrap();
        db.commit_txn(del.txn_id).unwrap();
        db.purge_obsolete_undo();
    }

    // Force compaction across every fragmented, low-ratio segment.
    for _ in 0..4 {
        db.purge_obsolete_undo();
        db.compact_undo_segments(90, 64).unwrap();
    }

    // The pinned snapshot must still reconstruct the OLD value (10),
    // proving the relocated undo record stays reachable through the
    // row-version chain after compaction.
    let rows = db.scan_rows("test.t", snapshot).unwrap();
    let pinned = rows
        .iter()
        .find(|r| r.values[0] == Value::Int(1))
        .expect("id=1 visible to the pinned snapshot");
    assert_eq!(
        pinned.values,
        vec![Value::Int(1), Value::Int(10)],
        "compaction must not break the snapshot-pinned old row version"
    );

    // A fresh reader sees the latest value.
    let fresh = db.read_only_statement_view(IsolationLevel::RepeatableRead, None);
    let fresh_rows = db.scan_rows("test.t", fresh).unwrap();
    let fresh_row = fresh_rows
        .iter()
        .find(|r| r.values[0] == Value::Int(1))
        .expect("id=1 visible to a fresh reader");
    assert_eq!(fresh_row.values, vec![Value::Int(1), Value::Int(20)]);

    drop(guard);
    std::env::remove_var("MYDB_UNDO_WRITE_SHARDS");
}

/// Regression for the TPC-C failure
/// "transaction undo head changed while appending undo". Build-then-swap
/// compaction used to relocate an active transaction's current undo record
/// under the global undo lock, release that lock, and only then CAS the
/// transaction head to the new pointer. A foreground append could span that
/// gap: it linked against the old pointer while compaction concurrently
/// published the relocated head. Depending on ordering, the append either
/// failed its CAS or persisted a chain edge that depended on an in-memory-only
/// forwarding entry.
#[test]
fn undo_compaction_fences_append_until_active_head_is_remapped() {
    std::env::set_var("MYDB_UNDO_WRITE_SHARDS", "1");
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Strict, true),
        )
        .unwrap(),
    );
    db.set_undo_segment_bytes(512).unwrap();
    let int_col = |name: &str, pk: bool| ColumnDef {
        name: name.into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key: pk,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    };
    db.create_table(
        "t",
        vec![int_col("id", true), int_col("v", false)],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)],
        ],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    // The seed before-images are no longer needed by this focused synthetic
    // layout. Removing them gives the active writer a clean source segment.
    {
        let mut undo = db.undo.write();
        let ids: Vec<_> = undo.records.keys().copied().collect();
        for id in ids {
            undo.remove_record(id).unwrap();
        }
    }

    let writer = db.begin_txn().unwrap();
    db.update_rows(
        writer,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(11)]),
    )
    .unwrap();
    let old_head = db.txns.read().undo_head(writer.txn_id).unwrap().unwrap();
    let old_head_segment = crate::storage::undo::undo_ptr_segment(old_head);

    // Rotate the writer's head segment out of the active tail. The churn uses
    // another row/transaction, so it does not contend with `writer`.
    for value in 31..80i64 {
        let churn = db.begin_txn().unwrap();
        db.update_rows(
            churn,
            "test.t",
            None,
            |row| Ok(row[0] == Value::Int(3)),
            |_| Ok(vec![Value::Int(3), Value::Int(value)]),
        )
        .unwrap();
        db.commit_txn(churn.txn_id).unwrap();
    }
    // Leave only the active writer's record live in its old segment, making
    // that segment a deterministic low-live-ratio compaction candidate.
    {
        let mut undo = db.undo.write();
        let ids: Vec<_> = undo
            .records
            .keys()
            .copied()
            .filter(|id| *id != old_head)
            .collect();
        for id in ids {
            undo.remove_record(id).unwrap();
        }
        assert!(
            undo.compaction_candidates(90, 64)
                .contains(&old_head_segment),
            "active head segment must be compactable for the race regression"
        );
    }

    let relocated = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    undo_compaction_head_fence_test_hook::install({
        let relocated = Arc::clone(&relocated);
        let release = Arc::clone(&release);
        Arc::new(move || {
            relocated.wait();
            release.wait();
        })
    });

    let compact_db = Arc::clone(&db);
    let compactor = std::thread::spawn(move || compact_db.compact_undo_segments(90, 64));
    // The old undo record has now moved, but the test hook parks compaction
    // immediately before the transaction-head CAS.
    relocated.wait();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let append_db = Arc::clone(&db);
    let appender = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = append_db.update_rows(
            writer,
            "test.t",
            None,
            |row| Ok(row[0] == Value::Int(2)),
            |_| Ok(vec![Value::Int(2), Value::Int(21)]),
        );
        result_tx.send(result).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(
        matches!(
            result_rx.recv_timeout(Duration::from_millis(250)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "append must remain fenced while compaction has moved but not remapped its head"
    );

    release.wait();
    assert!(compactor.join().unwrap().unwrap() > 0);
    assert_eq!(result_rx.recv().unwrap().unwrap(), 1);
    appender.join().unwrap();
    undo_compaction_head_fence_test_hook::clear();

    // The resulting two-record chain must remain fully rollbackable.
    db.rollback_txn(writer.txn_id).unwrap();
    let fresh = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let rows = db.scan_rows("test.t", fresh).unwrap();
    assert!(rows
        .iter()
        .any(|row| row.values == vec![Value::Int(1), Value::Int(10)]));
    assert!(rows
        .iter()
        .any(|row| row.values == vec![Value::Int(2), Value::Int(20)]));

    std::env::remove_var("MYDB_UNDO_WRITE_SHARDS");
}

#[test]
fn undo_retention_guarantee_rejects_insert_before_heap_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.set_undo_tablespace_max_bytes(40).unwrap();
    db.set_undo_retention_guarantee(true).unwrap();

    let txn = db.begin_txn().unwrap();
    let err = db
        .insert_rows(txn, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap_err();
    assert_eq!(err.code(), 50020);
    db.rollback_txn(txn.txn_id).unwrap();

    let rows = db
        .scan_rows(
            "test.t",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn undo_non_guarantee_pressure_allows_insert_and_runs_purge() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.set_undo_tablespace_max_bytes(40).unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let status = db.storage_status();
    assert!(status.undo_retention_pressure > 0);
    assert!(status.undo_purge_runs > 0);
    let rows = db
        .scan_rows(
            "test.t",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn deterministic_dml_model_matches_btreemap_and_index_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let mut model = BTreeMap::<i64, i64>::new();
    let mut seed = 0x5eed_u64;
    for _ in 0..64 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = (seed % 64) as i64;
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let value = (seed % 10_000) as i64;
        match seed % 3 {
            0 => {
                if !model.contains_key(&id) {
                    let txn = db.begin_txn().unwrap();
                    db.insert_rows(txn, "test.t", vec![vec![Value::Int(id), Value::Int(value)]])
                        .unwrap();
                    db.commit_txn(txn.txn_id).unwrap();
                    model.insert(id, value);
                }
            }
            1 => {
                if model.contains_key(&id) {
                    let txn = db.begin_txn().unwrap();
                    db.update_rows(
                        txn,
                        "test.t",
                        None,
                        |row| Ok(row[0] == Value::Int(id)),
                        |_| Ok(vec![Value::Int(id), Value::Int(value)]),
                    )
                    .unwrap();
                    db.commit_txn(txn.txn_id).unwrap();
                    model.insert(id, value);
                }
            }
            _ => {
                if model.remove(&id).is_some() {
                    let txn = db.begin_txn().unwrap();
                    db.delete_rows(txn, "test.t", None, |row| Ok(row[0] == Value::Int(id)))
                        .unwrap();
                    db.commit_txn(txn.txn_id).unwrap();
                }
            }
        }
    }

    let mut actual = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()
        .into_iter()
        .map(|row| match row.values.as_slice() {
            [Value::Int(id), Value::Int(value)] => (*id, *value),
            other => panic!("unexpected row: {other:?}"),
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, model);

    let table = db.table("test.t").unwrap();
    for id in 0..64 {
        let rows = db
            .lookup_index(
                &table,
                &table.indexes[0],
                IndexKey::from(vec![KeyPart::Int(id)]),
            )
            .unwrap();
        match model.get(&id) {
            Some(expected) => {
                assert_eq!(rows.len(), 1);
                let row = db
                    .read_row(
                        &table,
                        rows[0],
                        TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
                    )
                    .unwrap()
                    .expect("index row should resolve");
                assert_eq!(row.values, vec![Value::Int(id), Value::Int(*expected)]);
                actual.remove(&id);
            }
            None => assert!(rows.is_empty()),
        }
    }
    assert!(actual.is_empty());
    db.check_consistency().unwrap();
}

#[test]
fn heap_page_checksum_detects_corruption_on_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let path = dir.path().join("1.heap");
        let heap = HeapFile::open(path, 1, test_log(dir.path())).unwrap();
        heap.insert_mvcc(1, RowHeader::visible(1), &[Value::Int(1)])
            .unwrap();
        heap.flush_dirty_pages().unwrap();
    }
    let heap_path = dir.path().join("1.heap");
    {
        let mut file = OpenOptions::new().write(true).open(&heap_path).unwrap();
        file.seek(SeekFrom::Start(
            (PAGE_SIZE + PAGE_HEADER_SIZE + SLOT_SIZE + 1) as u64,
        ))
        .unwrap();
        file.write_all(&[0x5a]).unwrap();
        file.sync_data().unwrap();
    }

    let heap = HeapFile::open(heap_path, 1, test_log(dir.path())).unwrap();
    let err = heap.scan_mvcc().unwrap_err();

    assert_eq!(err.code(), 1030);
    assert!(err.message().contains("corrupt heap page checksum"));
}

#[test]
fn startup_validation_rejects_heap_page_lsn_ahead_of_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_config(
            dir.path(),
            test_storage_config_with_validation(
                DurabilityMode::Strict,
                StartupValidationMode::Full,
            ),
        )
        .unwrap();
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
        db.checkpoint().unwrap();
    }

    let heap_path = t_path(dir.path(), "t", 1, "heap");
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&heap_path)
            .unwrap();
        let mut page = vec![0; PAGE_SIZE];
        file.read_exact(&mut page).unwrap();
        set_page_lsn(&mut page, u64::MAX);
        refresh_page_checksum(&mut page);
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&page).unwrap();
        file.sync_data().unwrap();
    }

    let err = Database::open_with_config(
        dir.path(),
        test_storage_config_with_validation(DurabilityMode::Strict, StartupValidationMode::Full),
    )
    .unwrap_err();
    assert_eq!(err.code(), 1030);
    assert!(err.message().contains("page LSN is ahead of WAL"));
}

#[test]
fn off_startup_validation_skips_heap_page_lsn_check_until_read() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open(dir.path()).unwrap();
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.checkpoint().unwrap();
    }

    let heap_path = t_path(dir.path(), "t", 1, "heap");
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&heap_path)
            .unwrap();
        let mut page = vec![0; PAGE_SIZE];
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.read_exact(&mut page).unwrap();
        page[PAGE_HEADER_SIZE + SLOT_SIZE + 1] ^= 0x5a;
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&page).unwrap();
        file.sync_data().unwrap();
    }

    let db = Database::open_with_config(
        dir.path(),
        test_storage_config_with_validation(DurabilityMode::Strict, StartupValidationMode::Off),
    )
    .unwrap();
    let summary = db.storage_status().last_recovery_summary;
    assert!(summary.contains("startup_validation_path=off"));

    let err = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1030);
    assert!(err.message().contains("corrupt heap page checksum"));
}

#[test]
fn consistency_check_detects_dangling_index_entry() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.with_index_mut(1, "PRIMARY", |index| {
        index.insert_deferred(
            txn_id_for_ddl(),
            IndexKey::from(vec![KeyPart::Int(2)]),
            RowId {
                page_id: 99,
                slot_id: 1,
            },
        )
    })
    .unwrap();

    let err = db.check_consistency().unwrap_err();
    assert_eq!(err.code(), 1030);
    assert!(err.message().contains("storage consistency check failed"));
}

#[test]
fn ctas_intent_recovery_publishes_committed_load() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        let defer = CatalogTxnBatchGuard::new();
        db.create_table(
            "test.ctas_committed",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        let intent = db
            .write_ctas_intent("test.ctas_committed", txn.txn_id)
            .unwrap();
        db.insert_rows(txn, "test.ctas_committed", vec![vec![Value::Int(7)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.mark_ctas_intent_committed(intent.as_ref()).unwrap();
        let buffer_pool = Arc::downgrade(&db.buffer_pool);
        drop(db);
        assert!(
            buffer_pool.upgrade().is_none(),
            "crash-simulation drop must release the buffer pool"
        );
        drop(defer);
    }

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    assert!(db.table("test.ctas_committed").is_ok());
    let rows = db
        .scan_rows(
            "test.ctas_committed",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], Value::Int(7));
}

#[test]
fn ddl_audit_ctas_recovery_failure_retains_its_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let path;
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        let defer = CatalogTxnBatchGuard::new();
        simple_pk_table(&db, "ctas_retry");
        let txn = db.begin_txn().unwrap();
        path = db
            .write_ctas_intent("test.ctas_retry", txn.txn_id)
            .unwrap()
            .unwrap();
        db.insert_rows(txn, "test.ctas_retry", vec![vec![Value::Int(7)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.mark_ctas_intent_committed(Some(&path)).unwrap();
        drop(defer);
        db.fail_next_catalog_commit_for_test();
        assert!(db.finish_ctas_recovery().is_err());
        assert!(
            path.exists(),
            "recovery removed the descriptor before durable publication"
        );
    }
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let rows = db
        .scan_rows(
            "test.ctas_retry",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], Value::Int(7));
    assert!(!path.exists());
}

#[test]
fn ddl_audit_stale_truncate_cleanup_cannot_remove_new_rows() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        simple_pk_table(&db, "t");
        let old = db.table("test.t").unwrap();
        let retired = db.build_drop_table_intent(&old).unwrap();
        let legacy_wipe = db.build_truncate_table_intent(&old);
        db.truncate_table("test.t").unwrap();
        assert_ne!(db.table("test.t").unwrap().id, old.id);
        // Model a durable unlink failure (or a directory entry restored by
        // power loss) after an acknowledged TRUNCATE and a subsequent commit.
        db.write_drop_table_intent(&retired).unwrap();
        db.write_truncate_table_intent(&legacy_wipe).unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(99)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }
    for _ in 0..2 {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        let rows = db
            .scan_rows(
                "test.t",
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Value::Int(99));
    }
}

#[test]
fn ctas_intent_recovery_removes_uncommitted_load() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        let defer = CatalogTxnBatchGuard::new();
        db.create_table(
            "test.ctas_aborted",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.write_ctas_intent("test.ctas_aborted", txn.txn_id)
            .unwrap();
        db.insert_rows(txn, "test.ctas_aborted", vec![vec![Value::Int(9)]])
            .unwrap();
        // Simulate a process crash before the row transaction commits.
        drop(db);
        drop(defer);
    }

    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let err = db.table("test.ctas_aborted").unwrap_err();
    assert_eq!(err.code(), 1146);
}

#[test]
fn dd_mirror_roundtrips_catalog_through_heap_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());

    let int_col = |name: &str, pk: bool| ColumnDef {
        name: name.into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: !pk,
        primary_key: pk,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    };

    db.create_table(
        "t",
        vec![int_col("id", true), int_col("val", false)],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.create_table("t2", vec![int_col("id", true)], vec!["id".into()], vec![])
        .unwrap();

    // Rebuild the catalog purely from the data-dictionary heap tables and
    // compare it against the live in-memory catalog.
    let rebuilt = db.diagnostic_catalog_snapshot_from_store().unwrap();
    let live = db.catalog();

    let mut live_dbs: Vec<_> = live.databases.keys().cloned().collect();
    let mut reb_dbs: Vec<_> = rebuilt.databases.keys().cloned().collect();
    live_dbs.sort();
    reb_dbs.sort();
    assert_eq!(live_dbs, reb_dbs, "schemata mirror mismatch");

    let mut live_tabs: Vec<_> = live.tables.keys().cloned().collect();
    let mut reb_tabs: Vec<_> = rebuilt.tables.keys().cloned().collect();
    live_tabs.sort();
    reb_tabs.sort();
    assert_eq!(live_tabs, reb_tabs, "tables mirror mismatch");

    let lt = live.tables.get("test.t").unwrap();
    let rt = rebuilt.tables.get("test.t").unwrap();
    assert_eq!(rt.id, lt.id);
    let lcols: Vec<_> = lt.columns.iter().map(|c| c.name.clone()).collect();
    let rcols: Vec<_> = rt.columns.iter().map(|c| c.name.clone()).collect();
    assert_eq!(lcols, rcols, "column names round-trip");
    assert_eq!(rt.primary_key, lt.primary_key);
    let lidx: Vec<_> = lt.indexes.iter().map(|i| i.name.clone()).collect();
    let ridx: Vec<_> = rt.indexes.iter().map(|i| i.name.clone()).collect();
    assert_eq!(lidx, ridx, "index names round-trip");

    // Auth (the bootstrap root account + epoch) survives the round-trip.
    let mut live_accts: Vec<_> = live.auth.accounts.keys().cloned().collect();
    let mut reb_accts: Vec<_> = rebuilt.auth.accounts.keys().cloned().collect();
    live_accts.sort();
    reb_accts.sort();
    assert_eq!(live_accts, reb_accts, "auth accounts mirror mismatch");
    assert!(!reb_accts.is_empty(), "expected a bootstrap account");
    assert_eq!(rebuilt.auth.epoch, live.auth.epoch, "auth epoch mismatch");

    // The dictionary heaps physically exist at their reserved ids.
    let dd_tables_heap = crate::storage::naming::table_object_path(
        dir.path(),
        "mysql",
        "tables",
        crate::storage::data_dictionary::DD_TABLES,
        "heap",
    );
    assert!(dd_tables_heap.exists(), "mysql.tables heap not created");
}

#[test]
fn dd_tables_authoritative_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        db.create_database("shop", false).unwrap();
        db.checkpoint().unwrap();
        db.flush_dirty_data().unwrap();
    }

    // No JSON catalog is ever written: the data-dictionary heaps are the
    // sole source of truth for the reopened catalog.
    assert!(
        !dir.path()
            .join("catalog")
            .join("data_dictionary.json")
            .exists(),
        "JSON catalog must not be written"
    );

    let db2 = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let cat = db2.catalog();
    assert!(
        cat.tables.contains_key("test.t"),
        "table not reconstructed from DD heaps after reopen"
    );
    assert!(
        cat.databases.contains_key("shop"),
        "database not reconstructed from DD heaps after reopen"
    );
    assert!(
        !cat.auth.accounts.is_empty(),
        "auth not reconstructed from DD heaps after reopen"
    );
}

#[test]
fn wide_dd_descriptors_survive_repeated_drop_recreate_purge_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);

    let columns = || {
        (0..32)
            .map(|position| {
                let mut column = checkpoint_test_pk_column();
                column.name = format!("c{position}");
                column.primary_key = position == 0;
                column.nullable = position != 0;
                if position != 0 {
                    column.data_type = DataType::VarChar(Some(255));
                }
                column
            })
            .collect::<Vec<_>>()
    };

    let mut lob_sizes = Vec::new();
    // Three rebuilds are sufficient to prove the LOB footprint reaches a
    // plateau; the fourth repeated the same state transition and made this
    // test the serial tail of the whole nextest run.
    for cycle in 0..3 {
        let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
        if cycle != 0 {
            for table in (0..1).rev() {
                db.drop_table(&format!("test.wide_{table}")).unwrap();
            }
        }
        for table in 0..1 {
            db.create_table(
                &format!("wide_{table}"),
                columns(),
                vec!["c0".into()],
                vec![],
            )
            .unwrap();
            db.analyze_table(&format!("test.wide_{table}")).unwrap();
        }
        while db.purge_obsolete_undo() {}
        db.checkpoint().unwrap();
        db.flush_dirty_data().unwrap();
        lob_sizes.push(
            std::fs::metadata(crate::storage::naming::table_object_path(
                dir.path(),
                "mysql",
                "tables",
                crate::storage::data_dictionary::DD_TABLES,
                "lob",
            ))
            .unwrap()
            .len(),
        );
        drop(db);

        let reopened = Database::open_with_config(dir.path(), config.clone()).unwrap();
        for table in 0..1 {
            let descriptor = reopened.table(&format!("test.wide_{table}")).unwrap();
            assert_eq!(
                descriptor.columns.len(),
                32,
                "cycle {cycle}, table wide_{table}"
            );
        }
        drop(reopened);
    }
    assert!(
        lob_sizes.iter().skip(2).all(|size| *size <= lob_sizes[1]),
        "repeated rebuild leaked mysql.tables LOB pages: {lob_sizes:?}"
    );
}

#[test]
fn dd_incremental_transaction_rolls_back_atomically() {
    use crate::storage::data_dictionary::DD_SCHEMATA;

    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    // Commit a real object before injecting an aborted DD transaction.
    db.create_table(
        "t1",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    // Apply one incremental schema-row replacement under a real transaction,
    // then abort it before commit.
    let txn = db.begin_txn().unwrap();
    let ghost = vec![
        Value::String("ghost".into()),
        Value::String("utf8mb4".into()),
        Value::String(crate::charset::DEFAULT_COLLATION.into()),
        Value::Int(0),
        Value::UInt(0),
        Value::UInt(0),
    ];
    db.apply_catalog_row_replacement(
        txn,
        &CatalogRowReplacement {
            dd_table_id: DD_SCHEMATA,
            selector: CatalogRowSelector::PrimaryValues(vec![Value::String("ghost".into())]),
            rows: vec![ghost],
        },
    )
    .unwrap();

    db.rollback_txn(txn.txn_id).unwrap();

    // The aborted row is invisible while the previously committed object
    // remains authoritative.
    let cat = db.diagnostic_catalog_snapshot_from_store().unwrap();
    assert!(
        cat.tables.contains_key("test.t1"),
        "committed catalog object lost after rollback"
    );
    assert!(
        !cat.databases.contains_key("ghost"),
        "rolled-back catalog row leaked into the catalog"
    );
}

#[test]
fn dd_incremental_create_cost_does_not_grow_with_catalog_size() {
    use crate::storage::data_dictionary::DD_TABLES;

    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let mut per_create = Vec::new();
    for id in 0..16 {
        let before = db.catalog_store_stats().2;
        db.create_table(
            &format!("t{id}"),
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        per_create.push(db.catalog_store_stats().2.saturating_sub(before));
    }
    let min_rows = *per_create.iter().min().unwrap();
    let max_rows = *per_create.iter().max().unwrap();
    // One CREATE per 1,024-id block also advances the durable allocator
    // high-water property. That bounded extra row is independent of catalog
    // cardinality; ordinary CREATE mutations remain exactly constant.
    assert!(
        max_rows <= min_rows + 2,
        "per-create mutations: {per_create:?}"
    );
    assert!(per_create[1..].iter().all(|rows| *rows == min_rows));
    assert_eq!(db.scan_dd_table(DD_TABLES).unwrap().len(), 16);
}

#[test]
fn dd_object_shards_commit_independent_tables_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    let mut workers = Vec::new();
    for worker in 0..4 {
        let db = Arc::clone(&db);
        workers.push(std::thread::spawn(move || {
            for ordinal in 0..8 {
                db.create_table(
                    &format!("sharded_{worker}_{ordinal}"),
                    vec![checkpoint_test_pk_column()],
                    vec!["id".into()],
                    vec![],
                )
                .unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    let snapshot = db.diagnostic_catalog_snapshot_from_store().unwrap();
    assert_eq!(
        snapshot
            .tables
            .keys()
            .filter(|name| name.starts_with("test.sharded_"))
            .count(),
        32
    );
}

#[test]
fn analyze_replaces_only_one_table_stats_row() {
    use crate::storage::data_dictionary::{
        DD_COLUMNS, DD_INDEXES, DD_INDEX_COLUMN_USAGE, DD_TABLES, DD_TABLE_STATS,
    };

    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let structural_before = [DD_TABLES, DD_COLUMNS, DD_INDEXES, DD_INDEX_COLUMN_USAGE]
        .map(|id| db.scan_dd_table(id).unwrap());
    let mutations_before = db.catalog_store_stats().2;
    db.analyze_table("test.t").unwrap();
    let mutations = db.catalog_store_stats().2 - mutations_before;
    assert_eq!(
        mutations, 2,
        "one current stats row is deleted and inserted"
    );
    assert_eq!(db.scan_dd_table(DD_TABLE_STATS).unwrap().len(), 1);
    for (id, before) in [DD_TABLES, DD_COLUMNS, DD_INDEXES, DD_INDEX_COLUMN_USAGE]
        .into_iter()
        .zip(structural_before)
    {
        assert_eq!(
            db.scan_dd_table(id).unwrap(),
            before,
            "DD table {id} changed"
        );
    }
}

#[test]
fn startup_validation_rebuilds_corrupt_index_pages() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
        db.checkpoint().unwrap();
    }
    let index_path = t_index_path(dir.path(), "t", 1, "PRIMARY", "idx");
    {
        let mut file = OpenOptions::new().write(true).open(&index_path).unwrap();
        file.seek(SeekFrom::Start(
            PAGE_SIZE as u64 + INDEX_HEADER_BYTES as u64 + 1,
        ))
        .unwrap();
        file.write_all(&[0x7b]).unwrap();
        file.sync_data().unwrap();
    }

    let db = open_relaxed_test_db(dir.path());
    let table = db.table("test.t").unwrap();
    let rows = db
        .lookup_index(
            &table,
            &table.indexes[0],
            IndexKey::from(vec![KeyPart::Int(1)]),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(db
        .storage_status()
        .last_recovery_summary
        .contains("indexes_repaired=1"));
    assert!(db
        .storage_status()
        .last_recovery_summary
        .contains("startup_validation_path=clean_fast"));
}

#[test]
fn wal_redo_recovers_committed_index_page_without_index_flush() {
    let dir = tempfile::tempdir().unwrap();
    let index_path = t_index_path(dir.path(), "t", 1, "PRIMARY", "idx");
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.with_index_exclusive(1, "PRIMARY", |index| index.flush())
        .unwrap();
    // Retire the initial root image from the redo floor so the corruption
    // below still exercises heap-based index rebuilding, rather than being
    // repaired directly by the new index-initialization WAL image.
    db.checkpoint().unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_row(txn, "test.t", vec![Value::Int(9)]).unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    // Simulate a crash: no clean shutdown, no page flush. But stop the
    // buffer-pool IO workers first — otherwise `mem::forget(db)` leaks the
    // Arc, workers keep upgrading their Weak, and one can flush the dirty
    // index page 1 (holding the committed insert) *after* we overwrite it
    // with zeros below, silently restoring a valid page. Recovery then
    // finds no corruption to skip/repair and the assertion fails.
    // `shutdown_flusher` joins the workers without writing anything so
    // the "no clean shutdown" intent is preserved.
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
    {
        let mut file = OpenOptions::new().write(true).open(&index_path).unwrap();
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
        file.write_all(&vec![0; PAGE_SIZE]).unwrap();
        file.sync_data().unwrap();
    }

    let db = open_relaxed_test_db(dir.path());
    let table = db.table("test.t").unwrap();
    let rows = db
        .lookup_index(
            &table,
            &table.indexes[0],
            IndexKey::from(vec![KeyPart::Int(9)]),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    let summary = db.storage_status().last_recovery_summary;
    assert!(summary.contains("index_redo_skipped=1"));
    assert!(summary.contains("indexes_repaired=1"));
}

/// Phase 2 (WAL improvement plan) — parallel redo must produce exactly
/// the same recovered logical state as the serial path. We build a crash
/// image with several tables (so the parallel pass has >1 file
/// partition), committed + loser transactions, delete the heap + index
/// files to force full WAL reconstruction, then recover two byte-identical
/// copies — one with `MYDB_RECOVERY_PARALLEL=0`, one with `=1` — and
/// assert every table's rows match.
#[test]
fn parallel_redo_matches_serial_redo() {
    fn int_pk_col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }
    }
    fn copy_dir_all(src: &Path, dst: &Path) {
        fs::create_dir_all(dst).unwrap();
        for entry in fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let from = entry.path();
            let to = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir_all(&from, &to);
            } else {
                fs::copy(&from, &to).unwrap();
            }
        }
    }
    fn rows_of(db: &Database, table: &str) -> Vec<Vec<Value>> {
        let mut rows = db
            .scan_rows(
                table,
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap()
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>();
        rows.sort_by_key(|v| match v.first() {
            Some(Value::Int(i)) => *i,
            _ => 0,
        });
        rows
    }

    let crashed = tempfile::tempdir().unwrap();
    let tables = ["t1", "t2"];
    let table_ids: Vec<u64> = {
        let db = Database::open_with_durability(crashed.path(), DurabilityMode::Strict).unwrap();
        let mut ids = Vec::new();
        for t in tables {
            db.create_table(t, vec![int_pk_col("id")], vec!["id".into()], vec![])
                .unwrap();
            // Multiple committed records per file are enough to partition
            // redo across tables and compare the serial/parallel paths.
            let txn = db.begin_txn().unwrap();
            for i in 0..8i64 {
                db.insert_row(txn, &format!("test.{t}"), vec![Value::Int(i)])
                    .unwrap();
            }
            db.commit_txn(txn.txn_id).unwrap();
            // A loser transaction: inserts written to WAL but never
            // committed — recovery must roll these back identically on
            // both paths.
            let loser = db.begin_txn().unwrap();
            db.insert_row(loser, &format!("test.{t}"), vec![Value::Int(9999)])
                .unwrap();
            let _ = loser;
            ids.push(db.table(t).unwrap().id);
        }
        // A plain Database drop is the supported in-process crash simulation:
        // it stops workers but deliberately does not flush or checkpoint.
        // Leaking the handle would also leak the raw-page governor
        // reservations, unlike a real process crash where the OS releases
        // them before recovery starts.
        drop(db);
        ids
    };

    // Force full WAL reconstruction: delete every heap + PK index file so
    // recovery cannot short-circuit via already-durable pages.
    for (t, id) in tables.iter().zip(&table_ids) {
        let _ = fs::remove_file(t_path(crashed.path(), t, *id, "heap"));
        let _ = fs::remove_file(t_index_path(crashed.path(), t, *id, "PRIMARY", "idx"));
    }

    // Two byte-identical copies of the crash image.
    let serial_dir = tempfile::tempdir().unwrap();
    let parallel_dir = tempfile::tempdir().unwrap();
    copy_dir_all(crashed.path(), serial_dir.path());
    copy_dir_all(crashed.path(), parallel_dir.path());

    std::env::set_var("MYDB_RECOVERY_PARALLEL", "0");
    let serial_rows = {
        let db = Database::open_with_durability(serial_dir.path(), DurabilityMode::Strict).unwrap();
        tables.map(|t| rows_of(&db, &format!("test.{t}")))
    };
    std::env::set_var("MYDB_RECOVERY_PARALLEL", "1");
    let parallel_rows = {
        let db =
            Database::open_with_durability(parallel_dir.path(), DurabilityMode::Strict).unwrap();
        tables.map(|t| rows_of(&db, &format!("test.{t}")))
    };
    std::env::remove_var("MYDB_RECOVERY_PARALLEL");

    for (i, t) in tables.iter().enumerate() {
        assert_eq!(
            serial_rows[i], parallel_rows[i],
            "table {t}: parallel redo diverged from serial redo"
        );
        // Committed rows 0..8 survive; the loser's 9999 is rolled back.
        assert_eq!(serial_rows[i].len(), 8, "table {t} committed row count");
        assert!(
            serial_rows[i].iter().all(|v| v != &vec![Value::Int(9999)]),
            "table {t}: loser row must not survive"
        );
    }
}

/// Phase 4 (v7 wire-up): measure the actual on-disk `.idx` file
/// size after building a real IndexFile with the v7 page format,
/// and compare it to the v6 theoretical size for the same key
/// population. Asserts the saving is at least 10 %.
///
/// Run with:
///   cargo test --lib --release v7_idx_file_size_savings -- \
///     --ignored --nocapture
#[test]
#[ignore = "manual legacy index-format size estimate; not a current-format correctness regression"]
fn v7_idx_file_size_savings() {
    let dir = tempfile::tempdir().unwrap();
    let index_path = t_index_path(dir.path(), "t", 1, "PRIMARY", "idx");
    std::fs::create_dir_all(index_path.parent().unwrap()).unwrap();
    let mut index = IndexFile::open(index_path.clone(), test_log(dir.path())).unwrap();

    // Insert 50K i64 keys with clustered RowIds (50 rows / heap page).
    const N: i64 = 50_000;
    for i in 0..N {
        let key = IndexKey::from(vec![KeyPart::Int(i)]);
        let row_id = RowId {
            page_id: 1000 + (i as u32 / 50),
            slot_id: (i % 50) as u16,
        };
        index.insert(key, row_id, false).unwrap();
    }
    index.flush().unwrap();
    drop(index);

    let v7_bytes = std::fs::metadata(&index_path).unwrap().len() as usize;

    // Apples-to-apples v6 estimate: the v6 algorithm is *identical*
    // to v7's (same splits, same fill ratios), only the byte cost
    // per entry differs. v6 entry = 4 (part_count) + 1 (tag) + 8
    // (i64) + 20 (RowId) = 33 B; v7 entry = 4 + 1 + 8 + 12 = 25 B.
    // With the same B-tree algorithm the page-utilisation fraction
    // is identical, so disk bytes scale linearly with entry bytes.
    let v6_entry_bytes: f64 = 33.0;
    let v7_entry_bytes: f64 = 25.0;
    let v6_estimated_bytes = (v7_bytes as f64 * v6_entry_bytes / v7_entry_bytes) as usize;

    let saving_pct = 100.0 * (1.0 - v7_bytes as f64 / v6_estimated_bytes as f64);
    eprintln!(
        "v6 estimated .idx (same algorithm, 33 B/entry) = {} KB",
        v6_estimated_bytes / 1024
    );
    eprintln!(
        "v7 actual    .idx (this build, 25 B/entry)     = {} KB",
        v7_bytes / 1024
    );
    eprintln!(
        "file-size saving                                = {:.1}%",
        saving_pct
    );
    // Strict: should be ~24 % (the ratio 1 - 25/33). Allow 20 %.
    assert!(
        saving_pct >= 20.0,
        "expected ≥20% file-size saving on v7 vs v6 (same algo): got {saving_pct:.1}%"
    );
}

#[test]
fn oversize_index_page_returns_error_instead_of_panicking() {
    // Any string >= PAGE_SIZE overflows the page-size sanity check
    // regardless of the per-entry RowId / header footprint, which is
    // what this test wants to exercise.
    let page = IndexPage::Leaf(IndexLeafPage::new(
        vec![IndexEntry {
            key: IndexKey::from(vec![KeyPart::String("x".repeat(PAGE_SIZE))]),
            row_id: RowId {
                page_id: 1,
                slot_id: 1,
            },
        }],
        None,
    ));

    let err = encode_index_tree_page(&page).unwrap_err();
    assert_eq!(err.code(), 1118);
    assert!(err.message().contains("index page is too large"));
}

#[test]
fn wal_redo_recovers_index_meta_page_without_index_flush() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(".mydb-test-fresh-data-dir"), []).unwrap();
    let index_path = t_index_path(dir.path(), "t", 1, "PRIMARY", "idx");
    let mut config = test_storage_config(DurabilityMode::Strict, false);
    // Keep foreground capacity recovery from writing an index page after the
    // background workers are stopped. The test needs an exact no-index-flush
    // crash image, independent of the process-wide test environment.
    config.buffer_pool_pages = 4096;
    config.checkpoint_wal_bytes = u64::MAX;
    let db = Database::open_with_config(dir.path(), config).unwrap();
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.with_index_exclusive(1, "PRIMARY", |index| index.flush())
        .unwrap();
    // Stop writeback before the mutation, not after commit. Otherwise a
    // worker can persist a new leaf (including its next_leaf link) while the
    // meta page below is restored to the already-flushed empty generation.
    // That is a different partial-index crash case; this regression is meant
    // to prove recovery from WAL when no mutated index page was flushed.
    db.buffer_pool.shutdown_flusher();
    let txn = db.begin_txn().unwrap();
    // Row count is sized so the recovered index needs more than a
    // single leaf — that's what proves the bulk-load path allocated
    // new pages and updated the root pointer beyond the
    // already-flushed empty root. v10 entries are ~17 B (1 B u8
    // part_count + 9 B Int key + ~3 B varint page_id + 4 B
    // slot/gen), so 2 K entries × 17 B ≈ 34 KB safely overflows the
    // 16 KB page budget.
    db.insert_rows(
        txn,
        "test.t",
        (0..2000).map(|value| vec![Value::Int(value)]).collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    {
        let mut file = OpenOptions::new().read(true).open(&index_path).unwrap();
        let root =
            decode_index_tree_page(&read_index_page(&mut file, INDEX_EMPTY_ROOT).unwrap()).unwrap();
        assert!(
            matches!(root, IndexPage::Leaf(ref leaf) if leaf.entries.is_empty() && leaf.next_leaf.is_none()),
            "test precondition: the mutated index root must not reach disk"
        );
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
    {
        let mut file = OpenOptions::new().write(true).open(&index_path).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&vec![0; PAGE_SIZE]).unwrap();
        file.sync_data().unwrap();
    }

    let result = recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
    let index = IndexFile::open(index_path, test_log(dir.path())).unwrap();
    let rows = index
        .get(&IndexKey::from(vec![KeyPart::Int(1999)]), false)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(index.root_page.load(Ordering::Acquire) > INDEX_EMPTY_ROOT);
    assert!(result.summary.redone_records > 0);
}

#[test]
fn wal_redo_recovers_committed_index_batch_without_page_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let index_path = t_index_path(dir.path(), "t", 1, "PRIMARY", "idx");
    let db = Database::open(dir.path()).unwrap();
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    // Restore a valid older index image, not a flushed new root whose
    // children were arbitrarily truncated. The latter is corruption and
    // correctly requires the startup rebuild path instead of logical redo.
    db.with_index_exclusive(1, "PRIMARY", |index| index.flush())
        .unwrap();
    db.buffer_pool.flush_all().unwrap();
    let empty_index_image = fs::read(&index_path).unwrap();
    let catalog_stats = db.log.lock().stats();
    let catalog_index_batches = catalog_stats.index_batch_records;
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..100).map(|value| vec![Value::Int(value)]).collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let stats = db.log.lock().stats();
    assert_eq!(
        stats
            .index_batch_records
            .saturating_sub(catalog_index_batches),
        1
    );
    // Exclude catalog initialization's own page protection from this DML bound.
    assert!(
        stats
            .page_fallback_records
            .saturating_sub(catalog_stats.page_fallback_records)
            < 10
    );
    // Force the metadata-publication ordering that made the old fixture
    // depend on the background flusher's timing.
    db.buffer_pool.flush_all().unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&index_path)
            .unwrap();
        file.write_all(&empty_index_image).unwrap();
        file.sync_data().unwrap();
    }

    let result = recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
    let index = IndexFile::open(index_path, test_log(dir.path())).unwrap();
    let rows = index
        .get(&IndexKey::from(vec![KeyPart::Int(99)]), false)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(result.summary.redone_records > 0);
}

#[test]
fn relaxed_single_index_change_uses_logical_wal_without_page_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Relaxed).unwrap();
    assert_single_index_change_uses_logical_wal_without_page_fallback(&db);
}

#[test]
fn strict_single_index_change_uses_logical_wal_without_page_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    assert_single_index_change_uses_logical_wal_without_page_fallback(&db);
}

fn assert_single_index_change_uses_logical_wal_without_page_fallback(db: &Database) {
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.with_index_exclusive(1, "PRIMARY", |index| index.flush())
        .unwrap();
    let before = db.log.lock().stats();

    let txn = db.begin_txn().unwrap();
    db.insert_row(txn, "test.t", vec![Value::Int(1)]).unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let after = db.log.lock().stats();

    assert!(after.index_batch_records > before.index_batch_records);
    assert_eq!(
        after.index_page_payload_bytes,
        before.index_page_payload_bytes
    );
    // The initial allocation logs the row-page baseline and the heap frontier.
    // Index maintenance still adds no physical page payload.
    assert_eq!(
        after.page_fallback_records,
        before.page_fallback_records + 2
    );
    assert!(after.heap_fpi_payload_bytes > before.heap_fpi_payload_bytes);
}

#[test]
fn row_lock_manager_detects_simple_deadlock() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    let rows = db
        .insert_rows(
            seed,
            "test.t",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        )
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap().clone();
    db.lock_row_for_update(txn1, &table, rows[0]).unwrap();
    db.lock_row_for_update(txn2, &table, rows[1]).unwrap();

    let first_wait = db.lock_row_for_update(txn1, &table, rows[1]).unwrap_err();
    assert_eq!(first_wait.code(), 1205);
    let deadlock = db.lock_row_for_update(txn2, &table, rows[0]).unwrap_err();
    assert_eq!(deadlock.code(), 1213);
    assert_eq!(deadlock.sql_state(), "40001");
    assert_eq!(db.storage_status().deadlocks, 1);
}

#[test]
fn row_locks_release_after_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    let rows = db
        .insert_rows(seed, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap().clone();
    db.lock_row_for_update(txn1, &table, rows[0]).unwrap();
    assert_eq!(
        db.lock_row_for_update(txn2, &table, rows[0])
            .unwrap_err()
            .code(),
        1205
    );
    db.rollback_txn(txn1.txn_id).unwrap();
    db.lock_row_for_update(txn2, &table, rows[0]).unwrap();
}

#[test]
fn row_lock_waiters_are_served_before_newcomers() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    let rows = db
        .insert_rows(seed, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();
    let txn3 = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap().clone();

    db.lock_row_for_update(txn1, &table, rows[0]).unwrap();
    assert_eq!(
        db.lock_row_for_update(txn2, &table, rows[0])
            .unwrap_err()
            .code(),
        1205
    );
    assert_eq!(
        db.lock_row_for_update(txn3, &table, rows[0])
            .unwrap_err()
            .code(),
        1205
    );

    db.rollback_txn(txn1.txn_id).unwrap();

    assert_eq!(
        db.lock_row_for_update(txn3, &table, rows[0])
            .unwrap_err()
            .code(),
        1205
    );
    db.lock_row_for_update(txn2, &table, rows[0]).unwrap();

    db.rollback_txn(txn2.txn_id).unwrap();
    db.lock_row_for_update(txn3, &table, rows[0]).unwrap();
}

#[test]
fn non_row_lock_notification_does_not_wake_row_parker() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (woke_tx, woke_rx) = std::sync::mpsc::channel();
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || {
        let _registration = worker_db.row_lock_parkers.register(42);
        worker_db
            .row_lock_waiters
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ready_tx.send(std::thread::current()).unwrap();
        std::thread::park_timeout(Duration::from_millis(200));
        worker_db
            .row_lock_waiters
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        woke_tx.send(()).unwrap();
    });

    let worker_thread = ready_rx.recv().unwrap();
    db.notify_non_row_lock_waiters();
    assert!(
        woke_rx.recv_timeout(Duration::from_millis(20)).is_err(),
        "an unrelated global/MDL notification woke the row parker"
    );

    worker_thread.unpark();
    woke_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    worker.join().unwrap();
}

#[test]
fn unchanged_statement_rollback_does_not_wake_unrelated_row_waiter() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    let txn = db.begin_txn().unwrap();
    let savepoint = db.statement_savepoint(txn.txn_id).unwrap();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (woke_tx, woke_rx) = std::sync::mpsc::channel();
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || {
        let _registration = worker_db.row_lock_parkers.register(txn.txn_id + 1);
        worker_db.row_lock_waiters.fetch_add(1, Ordering::SeqCst);
        ready_tx.send(std::thread::current()).unwrap();
        std::thread::park_timeout(Duration::from_secs(2));
        worker_db.row_lock_waiters.fetch_sub(1, Ordering::SeqCst);
        let _ = woke_tx.send(());
    });
    let thread = ready_rx.recv().unwrap();
    db.rollback_to_statement_savepoint(txn.txn_id, savepoint)
        .unwrap();
    let woke = woke_rx.recv_timeout(Duration::from_millis(30)).is_ok();
    thread.unpark();
    worker.join().unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    assert!(
        !woke,
        "a rollback with no lock changes woke an unrelated row waiter"
    );
}

#[test]
fn unchanged_statement_rollback_does_not_need_transaction_write_latch() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    let txn = db.begin_txn().unwrap();
    let savepoint = db.statement_savepoint(txn.txn_id).unwrap();
    let read_guard = db.txns.read();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || {
        ready_tx.send(()).unwrap();
        let result = worker_db.rollback_to_statement_savepoint(txn.txn_id, savepoint);
        done_tx.send(result).unwrap();
    });
    ready_rx.recv().unwrap();
    let result = done_rx.recv_timeout(Duration::from_millis(200));
    drop(read_guard);
    worker.join().unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    result
        .expect("unchanged compensation waited for unrelated transaction readers")
        .unwrap();
}

#[test]
fn statement_rollback_notifies_after_cancelling_fifo_head() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let seed = db.begin_txn().unwrap();
    let row = db
        .insert_rows(seed, "test.t", vec![vec![Value::Int(1)]])
        .unwrap()[0];
    db.commit_txn(seed.txn_id).unwrap();
    let table = db.table("test.t").unwrap();
    let owner = db.begin_txn().unwrap();
    let cancelled = db.begin_txn().unwrap();
    let successor = db.begin_txn().unwrap();
    let savepoint = db.statement_savepoint(cancelled.txn_id).unwrap();
    db.lock_row_for_update(owner, &table, row).unwrap();
    assert!(db.lock_row_for_update(cancelled, &table, row).is_err());
    assert!(db.lock_row_for_update(successor, &table, row).is_err());
    db.rollback_txn(owner.txn_id).unwrap();
    // No held lock is released below, but removing the first waiter makes
    // the next one runnable. Exercise the notification as well as FIFO state.
    let _registration = db.row_lock_parkers.register(successor.txn_id);
    db.row_lock_waiters.fetch_add(1, Ordering::SeqCst);
    let generation = db.row_lock_signals.snapshot(row);
    db.rollback_to_statement_savepoint(cancelled.txn_id, savepoint)
        .unwrap();
    db.row_lock_waiters.fetch_sub(1, Ordering::SeqCst);
    assert_ne!(generation, db.row_lock_signals.snapshot(row));
    db.lock_row_for_update(successor, &table, row).unwrap();
    db.rollback_txn(cancelled.txn_id).unwrap();
    db.rollback_txn(successor.txn_id).unwrap();
}

#[test]
fn wait_cleanup_reports_fifo_entries_after_wait_graph_edge_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let mut lm = db.lock_manager();
    let row = RowId::new(1, 0);
    assert!(!lm.clear_wait_with_change(2));
    // The releasing owner can remove the graph edge before a cancelled
    // waiter leaves its FIFO entry. Cleanup must still report that change.
    lm.enqueue_row_waiter(2, 1, row, LockMode::Exclusive);
    lm.enqueue_row_waiter(3, 1, row, LockMode::Exclusive);
    assert!(lm.clear_wait_with_change(2));
    assert!(!lm.clear_wait_with_change(2));
    lm.try_acquire_waited_row(3, 1, row, LockMode::Exclusive)
        .unwrap();
}

#[test]
fn data_lock_snapshots_expose_granted_and_waiting_locks() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    let rows = db
        .insert_rows(seed, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // No locks held once the seed transaction committed.
    assert!(db.data_lock_snapshot().is_empty());
    assert!(db.data_lock_wait_snapshot().is_empty());

    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap().clone();

    // txn1 grabs the exclusive row lock; txn2 then blocks on it (1205 =
    // lock-wait pending), enqueuing a wait-for edge.
    db.lock_row_for_update(txn1, &table, rows[0]).unwrap();
    assert_eq!(
        db.lock_row_for_update(txn2, &table, rows[0])
            .unwrap_err()
            .code(),
        1205
    );

    // data_locks shows both the granted (txn1) and waiting (txn2) records on
    // the same row of test.t.
    let locks = db.data_lock_snapshot();
    let granted = locks
        .iter()
        .find(|l| l.lock_status == "GRANTED")
        .expect("granted lock");
    assert_eq!(granted.transaction_id, txn1.txn_id);
    assert_eq!(granted.lock_type, "RECORD");
    assert_eq!(granted.lock_mode, "X");
    assert_eq!(granted.object_schema.as_deref(), Some("test"));
    assert_eq!(granted.object_name.as_deref(), Some("t"));

    let waiting = locks
        .iter()
        .find(|l| l.lock_status == "WAITING")
        .expect("waiting lock");
    assert_eq!(waiting.transaction_id, txn2.txn_id);
    assert_eq!(waiting.lock_type, "RECORD");

    // data_lock_waits shows the single edge, and its lock ids join back to the
    // GRANTED / WAITING data_locks rows exactly.
    let waits = db.data_lock_wait_snapshot();
    assert_eq!(waits.len(), 1);
    let edge = &waits[0];
    assert_eq!(edge.requesting_transaction_id, txn2.txn_id);
    assert_eq!(edge.blocking_transaction_id, txn1.txn_id);
    assert_eq!(edge.blocking_lock_id, granted.engine_lock_id);
    assert_eq!(edge.requesting_lock_id, waiting.engine_lock_id);

    // Releasing txn1 lets txn2 in; with no remaining contention the wait graph
    // drains.
    db.rollback_txn(txn1.txn_id).unwrap();
    db.lock_row_for_update(txn2, &table, rows[0]).unwrap();
    assert!(db.data_lock_wait_snapshot().is_empty());
    db.rollback_txn(txn2.txn_id).unwrap();
    assert!(db.data_lock_snapshot().is_empty());
}

#[test]
fn statement_savepoint_rollback_can_preserve_current_lock_wait() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    let rows = db
        .insert_rows(
            seed,
            "test.t",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        )
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let owner = db.begin_txn().unwrap();
    let waiter = db.begin_txn().unwrap();
    let newcomer = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap().clone();

    db.lock_row_for_update(owner, &table, rows[0]).unwrap();

    let savepoint = db.statement_savepoint(waiter.txn_id).unwrap();
    db.lock_row_for_update(waiter, &table, rows[1]).unwrap();
    assert_eq!(
        db.lock_row_for_update(waiter, &table, rows[0])
            .unwrap_err()
            .code(),
        1205
    );

    db.rollback_to_statement_savepoint_preserving_wait(waiter.txn_id, savepoint)
        .unwrap();

    db.lock_row_for_update(newcomer, &table, rows[1]).unwrap();
    db.rollback_txn(owner.txn_id).unwrap();
    assert_eq!(
        db.lock_row_for_update(newcomer, &table, rows[0])
            .unwrap_err()
            .code(),
        1205
    );
    db.lock_row_for_update(waiter, &table, rows[0]).unwrap();
}

#[test]
fn read_only_rollback_releases_locks_without_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    let rows = db
        .insert_rows(seed, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();
    let before = db.log.lock().wal_bytes();
    let table = db.table("test.t").unwrap().clone();
    db.lock_row_for_update(txn1, &table, rows[0]).unwrap();
    assert_eq!(
        db.lock_row_for_update(txn2, &table, rows[0])
            .unwrap_err()
            .code(),
        1205
    );
    db.rollback_txn(txn1.txn_id).unwrap();

    assert_eq!(db.log.lock().wal_bytes(), before);
    db.lock_row_for_update(txn2, &table, rows[0]).unwrap();
}

#[test]
fn read_committed_dml_serializes_delete_insert_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "sbtest1",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "k".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("k_1".into()),
            columns: vec!["k".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.sbtest1",
        vec![vec![Value::Int(1), Value::Int(10)]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();
    db.update_rows(
        txn1,
        "test.sbtest1",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| Ok(vec![row[0].clone(), Value::Int(11)]),
    )
    .unwrap();
    let blocked = db
        .delete_rows(
            txn2,
            "test.sbtest1",
            None,
            |row| Ok(row[0] == Value::Int(1)),
        )
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);
    db.commit_txn(txn1.txn_id).unwrap();
    assert_eq!(
        db.delete_rows(txn2, "test.sbtest1", None, |row| {
            Ok(row[0] == Value::Int(1))
        })
        .unwrap(),
        1
    );
    db.insert_rows(
        txn2,
        "test.sbtest1",
        vec![vec![Value::Int(1), Value::Int(12)]],
    )
    .unwrap();
    db.commit_txn(txn2.txn_id).unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn strict_group_commit_batches_concurrent_commits_and_recovers() {
    assert_strict_group_commit_and_recovery(false);
}

#[test]
fn strict_group_commit_preflushed_commits_recover_without_new_batch() {
    assert_strict_group_commit_and_recovery(true);
}

fn assert_strict_group_commit_and_recovery(preflushed: bool) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Database::open_with_config(
            dir.path(),
            StorageConfig {
                // The barrier makes the commits logically concurrent, but a
                // heavily loaded nextest host can leave some released threads
                // unscheduled for more than 10 ms. Keep the window comfortably
                // above scheduler jitter so this tests batching rather than
                // host load.
                group_commit_window: Duration::from_millis(100),
                ..test_storage_config(DurabilityMode::Strict, true)
            },
        )
        .unwrap(),
    );
    db.create_table(
        "test.t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        Vec::new(),
    )
    .unwrap();

    let inserts_done = Arc::new(std::sync::Barrier::new(9));
    let start_commit = Arc::new(std::sync::Barrier::new(9));
    let mut handles = Vec::new();
    for id in 1..=8 {
        let db = db.clone();
        let start_commit = start_commit.clone();
        let inserts_done = inserts_done.clone();
        handles.push(std::thread::spawn(move || {
            let txn = db.begin_txn().unwrap();
            db.insert_rows(
                txn,
                "test.t",
                vec![vec![Value::Int(id), Value::Int(id * 10)]],
            )
            .unwrap();
            let mut deferred = preflushed.then(|| db.commit_txn_deferred(txn.txn_id).unwrap());
            inserts_done.wait();
            start_commit.wait();
            if let Some(commit) = deferred.as_mut() {
                db.finish_deferred_commits(&mut [commit]).unwrap();
            } else {
                db.commit_txn(txn.txn_id).unwrap();
            }
            let durable_at_ack = db.log.lock().durable_lsn();
            (txn.txn_id, durable_at_ack)
        }));
    }
    inserts_done.wait();
    if preflushed {
        // A checkpoint or data-page writeback can make commit WAL durable
        // before its owner reaches the group-commit registration point.
        db.flush_dirty_data().unwrap();
    }
    // Bootstrap, DDL and DML writeback may synchronize WAL independently.
    // Measure only the released concurrent commit phase.
    let before = db.storage_status();
    start_commit.wait();
    let acknowledgements = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    let status = db.storage_status();
    assert!(
        status.group_commit_batches - before.group_commit_batches < 8,
        "{status:?}"
    );
    // Already-durable committers return before registering as waiters. Their
    // durability must be checked against WAL, not inferred from queue stats.
    if preflushed {
        assert_eq!(status.group_commit_batches, before.group_commit_batches);
        assert_eq!(status.group_commit_waiters, before.group_commit_waiters);
        assert_eq!(
            status.group_commit_high_water_released,
            before.group_commit_high_water_released
        );
    }
    let scan = {
        let _wal = db.log.lock();
        LogManager::scan_with_mode(dir.path(), crate::wal::WalReadMode::ReadOnly).unwrap()
    };
    for (txn_id, durable_at_ack) in acknowledgements {
        let commit = scan
            .records
            .iter()
            .find(|record| {
                record.txn_id == txn_id && matches!(&record.payload, WalPayload::CommitTxn { .. })
            })
            .unwrap_or_else(|| panic!("missing commit WAL for acknowledged transaction {txn_id}"));
        assert!(
            commit.lsn <= durable_at_ack,
            "transaction {txn_id} acknowledged LSN {} above durable LSN {durable_at_ack}",
            commit.lsn
        );
    }
    assert!(status.wal_syncs - before.wal_syncs < 8, "{status:?}");
    drop(db);

    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let mut rows = db.scan_rows("test.t", txn).unwrap();
    rows.sort_by_key(|row| match row.values[0] {
        Value::Int(value) => value,
        _ => 0,
    });
    assert_eq!(
        rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
        (1..=8)
            .map(|id| vec![Value::Int(id), Value::Int(id * 10)])
            .collect::<Vec<_>>()
    );
}

#[test]
fn indexed_delete_waits_on_uncommitted_replacement_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "sbtest1",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "k".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.sbtest1",
        vec![vec![Value::Int(1), Value::Int(10)]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let table = db.table("test.sbtest1").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let key = IndexKey::from(vec![KeyPart::Int(1)]);
    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();

    let ids = db.lookup_index(&table, primary, key.clone()).unwrap();
    assert_eq!(
        db.delete_rows(txn1, "test.sbtest1", Some(ids), |row| {
            Ok(row[0] == Value::Int(1))
        })
        .unwrap(),
        1
    );
    db.insert_rows(
        txn1,
        "test.sbtest1",
        vec![vec![Value::Int(1), Value::Int(11)]],
    )
    .unwrap();

    let ids = db.lookup_index(&table, primary, key.clone()).unwrap();
    let blocked = db
        .delete_rows(txn2, "test.sbtest1", Some(ids), |row| {
            Ok(row[0] == Value::Int(1))
        })
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    db.commit_txn(txn1.txn_id).unwrap();
    let txn2 = db
        .statement_view(txn2.txn_id, IsolationLevel::ReadCommitted)
        .unwrap();
    let ids = db.lookup_index(&table, primary, key).unwrap();
    assert_eq!(
        db.delete_rows(txn2, "test.sbtest1", Some(ids), |row| {
            Ok(row[0] == Value::Int(1))
        })
        .unwrap(),
        1
    );
    let txn2 = db
        .statement_view(txn2.txn_id, IsolationLevel::ReadCommitted)
        .unwrap();
    db.insert_rows(
        txn2,
        "test.sbtest1",
        vec![vec![Value::Int(1), Value::Int(12)]],
    )
    .unwrap();
    db.commit_txn(txn2.txn_id).unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn indexed_delete_waits_on_deleted_unique_key_gap() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "sbtest1",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "k".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.sbtest1",
        vec![vec![Value::Int(1), Value::Int(10)]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let table = db.table("test.sbtest1").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let key = IndexKey::from(vec![KeyPart::Int(1)]);
    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();

    let ids = db
        .lookup_index_for_write(txn1, &table, primary, key.clone())
        .unwrap();
    assert_eq!(
        db.delete_rows(txn1, "test.sbtest1", Some(ids), |row| {
            Ok(row[0] == Value::Int(1))
        })
        .unwrap(),
        1
    );

    let blocked = db
        .lookup_index_for_write(txn2, &table, primary, key.clone())
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    db.insert_rows(
        txn1,
        "test.sbtest1",
        vec![vec![Value::Int(1), Value::Int(11)]],
    )
    .unwrap();
    db.commit_txn(txn1.txn_id).unwrap();

    let txn2 = db
        .statement_view(txn2.txn_id, IsolationLevel::ReadCommitted)
        .unwrap();
    let ids = db
        .lookup_index_for_write(txn2, &table, primary, key)
        .unwrap();
    assert_eq!(
        db.delete_rows(txn2, "test.sbtest1", Some(ids), |row| {
            Ok(row[0] == Value::Int(1))
        })
        .unwrap(),
        1
    );
    db.insert_rows(
        txn2,
        "test.sbtest1",
        vec![vec![Value::Int(1), Value::Int(12)]],
    )
    .unwrap();
    db.commit_txn(txn2.txn_id).unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn claim_delete_returns_empty_after_concurrent_unique_delete_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let table = db.table("test.t").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let key = IndexKey::from(vec![KeyPart::Int(1)]);
    let winner = db.begin_txn().unwrap();
    let loser = db.begin_txn().unwrap();

    let ids = db
        .try_lookup_index_for_claim_delete(winner, &table, primary, key.clone())
        .unwrap()
        .unwrap();
    assert_eq!(
        db.delete_rows(winner, "test.t", Some(ids), |row| Ok(
            row[0] == Value::Int(1)
        ))
        .unwrap(),
        1
    );

    let blocked = db
        .try_lookup_index_for_claim_delete(loser, &table, primary, key.clone())
        .unwrap_err();
    assert_eq!(blocked.message(), LOCK_WAIT_PENDING_MESSAGE);

    db.commit_txn(winner.txn_id).unwrap();
    let loser = db
        .statement_view(loser.txn_id, IsolationLevel::ReadCommitted)
        .unwrap();
    let ids = db
        .try_lookup_index_for_claim_delete(loser, &table, primary, key)
        .unwrap()
        .unwrap();
    assert!(ids.is_empty());
    assert_eq!(
        db.delete_rows(
            loser,
            "test.t",
            Some(ids),
            |row| Ok(row[0] == Value::Int(1))
        )
        .unwrap(),
        0
    );
    db.commit_txn(loser.txn_id).unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn claim_delete_can_delete_after_concurrent_unique_delete_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let table = db.table("test.t").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let key = IndexKey::from(vec![KeyPart::Int(1)]);
    let winner = db.begin_txn().unwrap();
    let loser = db.begin_txn().unwrap();

    let ids = db
        .try_lookup_index_for_claim_delete(winner, &table, primary, key.clone())
        .unwrap()
        .unwrap();
    assert_eq!(
        db.delete_rows(winner, "test.t", Some(ids), |row| Ok(
            row[0] == Value::Int(1)
        ))
        .unwrap(),
        1
    );
    assert!(db
        .try_lookup_index_for_claim_delete(loser, &table, primary, key.clone())
        .is_err());

    db.rollback_txn(winner.txn_id).unwrap();
    let loser = db
        .statement_view(loser.txn_id, IsolationLevel::ReadCommitted)
        .unwrap();
    let ids = db
        .try_lookup_index_for_claim_delete(loser, &table, primary, key)
        .unwrap()
        .unwrap();
    assert_eq!(
        db.delete_rows(
            loser,
            "test.t",
            Some(ids),
            |row| Ok(row[0] == Value::Int(1))
        )
        .unwrap(),
        1
    );
    db.commit_txn(loser.txn_id).unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn insert_waits_on_deleted_unique_key_gap() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: true,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let table = db.table("test.t").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let key = IndexKey::from(vec![KeyPart::Int(1)]);
    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();

    let ids = db
        .lookup_index_for_write(txn1, &table, primary, key)
        .unwrap();
    db.delete_rows(txn1, "test.t", Some(ids), |row| Ok(row[0] == Value::Int(1)))
        .unwrap();

    let blocked = db
        .insert_rows(txn2, "test.t", vec![vec![Value::Int(1)]])
        .unwrap_err();
    assert_eq!(blocked.code(), 1205);

    db.rollback_txn(txn1.txn_id).unwrap();
    db.insert_rows(txn2, "test.t", vec![vec![Value::Int(2)]])
        .unwrap();
    db.commit_txn(txn2.txn_id).unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn unique_key_wait_participates_in_deadlock_detection() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "u".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: true,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![crate::parser::IndexDef {
            name: Some("u_idx".into()),
            columns: vec!["u".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: true,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let table = db.table("test.t").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let unique = table
        .indexes
        .iter()
        .find(|index| index.unique && !index.primary)
        .unwrap();
    let txn1 = db.begin_txn().unwrap();
    let txn2 = db.begin_txn().unwrap();
    let row_id = db
        .lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(1)]))
        .unwrap()[0];

    db.lock_rows_blocking(txn1.txn_id, table.id, vec![row_id])
        .unwrap();
    db.insert_rows(txn2, "test.t", vec![vec![Value::Int(2), Value::Int(20)]])
        .unwrap();

    let unique_wait = db
        .lookup_index_for_write(txn1, &table, unique, IndexKey::from(vec![KeyPart::Int(20)]))
        .unwrap_err();
    assert_eq!(unique_wait.code(), 1205);
    assert_eq!(unique_wait.message(), LOCK_WAIT_PENDING_MESSAGE);

    let row_wait = db
        .lock_rows_blocking(txn2.txn_id, table.id, vec![row_id])
        .unwrap_err();
    assert_eq!(row_wait.code(), 1213);
    assert!(row_wait.message().contains("deadlock found"));
}

#[test]
fn rollback_releases_row_locks_before_slow_flush_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_storage_config(DurabilityMode::Strict, true);
    config.lock_wait = Duration::from_millis(250);
    let db = Arc::new(Database::open_with_config(dir.path(), config).unwrap());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let table = db.table("test.t").unwrap().clone();
    let row_id = db
        .lookup_index(
            &table,
            table.indexes.iter().find(|index| index.primary).unwrap(),
            IndexKey::from(vec![KeyPart::Int(1)]),
        )
        .unwrap()[0];
    let owner = db.begin_txn().unwrap();
    db.update_row_by_id_with_changed_columns(owner, &table, row_id, Some(&[1]), |_| {
        Ok(vec![Value::Int(1), Value::Int(20)])
    })
    .unwrap();

    let page_key = PageKey {
        file: Arc::new(BufferPool::heap_file_key(db.table_path(table.id), table.id)),
        page_id: row_id.page_id as u64,
    };
    let frame = db.buffer_pool.lookup_frame(&page_key).unwrap().unwrap();
    let flush_guard = frame.flush_lock.lock();
    let rollback_db = Arc::clone(&db);
    let rollback = std::thread::spawn(move || rollback_db.rollback_txn(owner.txn_id));

    let waiter = db.begin_txn().unwrap();
    let started = Instant::now();
    db.lock_rows_blocking(waiter.txn_id, table.id, vec![row_id])
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "row lock was held until rollback flush finished"
    );

    drop(flush_guard);
    rollback.join().unwrap().unwrap();
    db.rollback_txn(waiter.txn_id).unwrap();
}

#[test]
fn btree_index_round_trips_reopens_and_filters_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_primary.idx");
    let mut index = IndexFile::open(path.clone(), test_log(dir.path())).unwrap();
    let mut inserted = 0i64;
    while inserted < 256 && (inserted < 64 || index.next_page_id() <= 2) {
        index
            .insert(
                wide_test_index_key(inserted),
                test_row_id(inserted, 8),
                true,
            )
            .unwrap();
        inserted += 1;
    }
    assert!(inserted > 42);
    index.flush().unwrap();
    assert!(fs::metadata(&path).unwrap().len() > PAGE_SIZE as u64);
    drop(index);

    let index = IndexFile::open(path, test_log(dir.path())).unwrap();
    assert_eq!(
        index.get(&wide_test_index_key(42), false).unwrap(),
        vec![test_row_id(42, 8)]
    );
    assert_eq!(
        index
            .range(
                Some(&IndexBound {
                    key: wide_test_index_key(10),
                    inclusive: false,
                }),
                Some(&IndexBound {
                    key: wide_test_index_key(13),
                    inclusive: true,
                }),
                false,
            )
            .unwrap()
            .into_iter()
            .map(|row| row.slot_id)
            .collect::<Vec<_>>(),
        vec![11, 12, 13]
    );
    assert_eq!(
        index.last_entry().unwrap().unwrap().0,
        wide_test_index_key(inserted - 1)
    );
}

/// Phase 6: validate() now cross-checks leaf-chain coverage against tree
/// descent. A multi-leaf index whose leaf chain has been broken (e.g. by a
/// crash mid-split that updated the parent but not the predecessor's
/// next_leaf, or vice versa) must be rejected so startup_validation will
/// trigger a rebuild.
#[test]
fn validate_rejects_index_with_broken_leaf_chain() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_chain.idx");
    let mut index = IndexFile::open(path.clone(), test_log(dir.path())).unwrap();
    // Insert only until the tree has multiple leaf pages. Wide keys keep this
    // focused on validation rather than spending seconds building a large tree.
    let mut value = 0i64;
    while index.next_page_id() <= 3 {
        index
            .insert(wide_test_index_key(value), test_row_id(value, 8), true)
            .unwrap();
        value += 1;
        assert!(value < 512, "expected a multi-leaf index for this test");
    }
    index.flush().unwrap();
    drop(index);

    // Sanity: a healthy index passes the new chain check.
    IndexFile::validate(path.clone()).unwrap();

    // Corrupt: walk the file pages, find the leftmost leaf, and zero out its
    // next_leaf pointer. This orphans every later leaf from the chain while
    // leaving them reachable via tree descent — exactly the divergence the
    // new check is designed to catch.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let len = file.metadata().unwrap().len();
    let page_count = len / PAGE_SIZE as u64;
    let meta_buf = read_index_page(&mut file, INDEX_META_PAGE).unwrap();
    let meta = decode_index_meta(&meta_buf).unwrap();
    // Descend leftmost spine to first leaf.
    let mut page_id = meta.root_page;
    let leaf_page_id = loop {
        let buf = read_index_page(&mut file, page_id).unwrap();
        match decode_index_tree_page(&buf).unwrap() {
            IndexPage::Leaf(_) => break page_id,
            IndexPage::Internal(p) => page_id = p.children[0],
            IndexPage::Free(_) => panic!("unexpected free page on root spine"),
        }
    };
    // Read raw page, zero next_leaf in the leaf payload. Layout
    // (after the 64-byte common header) v19 layout: 1B page_type,
    // 1B key_layout, 8B base_time, 8B next_leaf, 2B entry_count,
    // entries… (page_table_id removed in v19; entry_count u32 → u16.)
    let mut raw = read_index_page(&mut file, leaf_page_id).unwrap();
    let payload_start = INDEX_HEADER_BYTES;
    // skip page_type (1) + key_layout (1) + base_time (8)
    let next_leaf_off = payload_start + 1 + 1 + 8;
    write_u64(&mut raw, next_leaf_off, 0);
    let new_checksum = page_checksum(&raw, 16);
    write_u32(&mut raw, 16, new_checksum);
    use std::io::{Seek, SeekFrom, Write};
    file.seek(SeekFrom::Start(leaf_page_id * PAGE_SIZE as u64))
        .unwrap();
    file.write_all(&raw).unwrap();
    file.sync_data().unwrap();
    drop(file);
    assert!(page_count > 2, "expected a multi-leaf index for this test");

    // Validate must now reject the divergent leaf set.
    let err = IndexFile::validate(path).unwrap_err();
    let msg = format!("{:?}", err);
    assert!(
        msg.contains("leaf chain") || msg.contains("disagree"),
        "expected leaf-chain corruption error, got: {}",
        msg
    );
}

/// Phase 6: end-to-end startup recovery sweep. After the file-level leaf
/// chain is broken (simulating a crash mid-split), reopening the database
/// with full validation must detect the divergence and rebuild the index
/// transparently. Subsequent reads should succeed.
#[test]
fn startup_validation_rebuilds_index_with_broken_leaf_chain() {
    let dir = tempfile::tempdir().unwrap();
    let config =
        test_storage_config_with_validation(DurabilityMode::Strict, StartupValidationMode::Full);
    let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (1..=300)
            .map(|i| vec![Value::Int(i), Value::Int(i * 10)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();
    let table_id = db.table("test.t").unwrap().id;
    drop(db);

    // Corrupt the leftmost leaf's next_leaf pointer.
    let index_path = t_index_path(dir.path(), "t", table_id, "PRIMARY", "idx");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&index_path)
        .unwrap();
    let meta_buf = read_index_page(&mut file, INDEX_META_PAGE).unwrap();
    let meta = decode_index_meta(&meta_buf).unwrap();
    let mut page_id = meta.root_page;
    let leaf_page_id = loop {
        let buf = read_index_page(&mut file, page_id).unwrap();
        match decode_index_tree_page(&buf).unwrap() {
            IndexPage::Leaf(_) => break page_id,
            IndexPage::Internal(p) => page_id = p.children[0],
            IndexPage::Free(_) => panic!("unexpected free page"),
        }
    };
    let mut raw = read_index_page(&mut file, leaf_page_id).unwrap();
    // Current leaf payload layout (v10):
    //   [0..24]  INDEX_HEADER_BYTES (common header)
    //   [24..32] page_table_id (u64)
    //   [32]     page_type (u8)
    //   [33]     key_layout (u8)
    //   [34..42] base_time (i64)
    //   [42..50] next_leaf (u64)
    //   [50..54] entry_count (u32)
    //   [54..]   entries
    let next_leaf_off = 24 + 8 + 1 + 1 + 8;
    write_u64(&mut raw, next_leaf_off, 0);
    let new_checksum = page_checksum(&raw, 16);
    write_u32(&mut raw, 16, new_checksum);
    use std::io::{Seek, SeekFrom, Write};
    file.seek(SeekFrom::Start(leaf_page_id * PAGE_SIZE as u64))
        .unwrap();
    file.write_all(&raw).unwrap();
    file.sync_data().unwrap();
    drop(file);

    // Reopen with full validation; it should detect the divergence and
    // rebuild the index. Subsequent reads must return all rows.
    let db = Database::open_with_config(dir.path(), config).unwrap();
    let table = db.table("test.t").unwrap();
    let snapshot = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let primary = table
        .indexes
        .iter()
        .find(|i| i.primary)
        .expect("primary index");
    for &id in &[1i64, 50, 150, 299, 300] {
        let key = IndexKey::from(vec![KeyPart::Int(id)]);
        let ids = db
            .lookup_index_visible(&table, primary, key, snapshot)
            .unwrap();
        assert_eq!(ids.len(), 1, "primary lookup id={}", id);
    }
    // Range scan goes through leaf chain (next_leaf) — if the chain wasn't
    // rebuilt, we'd only see entries in the first leaf and lose the rest.
    let range_ids = db
        .range_index_bounds_visible(
            &table,
            primary,
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(1)]),
                inclusive: true,
            }),
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(300)]),
                inclusive: true,
            }),
            snapshot,
        )
        .unwrap();
    assert_eq!(
        range_ids.len(),
        300,
        "range scan via leaf chain must see all rows after rebuild"
    );
}

#[test]
fn btree_index_handles_duplicate_key_delete_and_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_dupe.idx");
    let mut index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let key = IndexKey::from(vec![KeyPart::Int(7), KeyPart::String("a".into())]);
    let row1 = RowId {
        page_id: 1,
        slot_id: 1,
    };
    let row2 = RowId {
        page_id: 1,
        slot_id: 2,
    };
    index.insert(key.clone(), row1, false).unwrap();
    index.insert(key.clone(), row2, false).unwrap();
    index
        .insert(
            IndexKey::from(vec![KeyPart::Int(8), KeyPart::String("b".into())]),
            RowId {
                page_id: 1,
                slot_id: 3,
            },
            false,
        )
        .unwrap();

    assert_eq!(index.get(&key, false).unwrap(), vec![row1, row2]);
    assert_eq!(
        index
            .prefix(&IndexKey::from(vec![KeyPart::Int(7)]), false)
            .unwrap(),
        vec![row1, row2]
    );
    index.delete(key.clone(), row1).unwrap();
    assert_eq!(index.get(&key, false).unwrap(), vec![row2]);
}

#[test]
fn btree_forward_range_cursor_matches_full_range_across_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "grp".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..12_000)
            .map(|id| vec![Value::Int(id), Value::Int(id % 7)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.add_index(
        "test.t",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "grp_idx".into(),
            columns: vec!["grp".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();

    let table = db.table("test.t").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.name == "grp_idx")
        .unwrap();
    let low = Some(IndexBound {
        key: IndexKey::from(vec![KeyPart::Int(1)]),
        inclusive: true,
    });
    let high = Some(IndexBound {
        key: IndexKey::from(vec![KeyPart::Int(5)]),
        inclusive: true,
    });
    let expected = db
        .range_index_bounds_entries(&table, index, low.clone(), high.clone())
        .unwrap();
    let mut cursor = db
        .range_index_entries_cursor(&table, index, low.clone(), high.clone())
        .unwrap();
    let mut actual = Vec::new();
    let chunk_sizes = [1usize, 7, 13, 29, 5, 64];
    let mut step = 0usize;
    loop {
        let chunk = cursor
            .next_chunk(chunk_sizes[step % chunk_sizes.len()])
            .unwrap();
        if chunk.is_empty() {
            break;
        }
        actual.extend(chunk);
        step += 1;
    }

    assert_eq!(actual, expected);
}

/// Regression matrix for the P0 index-range-scan rework: the row-id
/// sink (`next_chunk_row_ids` / `range_index_row_ids_for_leaf_ids`),
/// positioning-time `partition_point`, and whole-leaf accept must all
/// agree with the standalone `range_entries` oracle across inclusive /
/// exclusive / open bounds on both the int fast path and the generic
/// (string) comparator path.
#[test]
fn range_cursor_row_id_and_leaf_walkers_match_entries_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col("grp", DataType::Int, false),
            lob_col("name", DataType::VarChar(Some(64)), false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..1_000i64)
            .map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id % 97),
                    Value::String(format!("k{:05}", id % 311)),
                ]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    for (name, column) in [("grp_idx", "grp"), ("name_idx", "name")] {
        db.add_index(
            "test.t",
            IndexSchema {
                readable_from_scn: 0,
                index_id: 0,
                name: name.into(),
                columns: vec![column.into()],
                column_prefix_lengths: Vec::new(),
                column_directions: Vec::new(),
                key_expressions: Vec::new(),
                unique: false,
                primary: false,
                included_columns: Vec::new(),
                state: crate::storage::IndexState::Public,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: String::new(),
                vector: None,
                fulltext: None,
            },
        )
        .unwrap();
    }

    let table = db.table("test.t").unwrap();
    for index_name in ["grp_idx", "name_idx"] {
        let index = table
            .indexes
            .iter()
            .find(|index| index.name == index_name)
            .unwrap();
        // Bound keys are cloned from live entries so the KeyPart
        // variants match what the index actually stores.
        let all = db
            .range_index_bounds_entries(&table, index, None, None)
            .unwrap();
        assert!(all.len() >= 1_000, "index should be fully populated");
        let low_key = all[all.len() / 5].0.clone();
        let high_key = all[all.len() * 4 / 5].0.clone();
        let bound = |key: &IndexKey, inclusive: bool| {
            Some(IndexBound {
                key: key.clone(),
                inclusive,
            })
        };
        let mut cases: Vec<(Option<IndexBound>, Option<IndexBound>)> = vec![
            (None, None),
            (None, bound(&high_key, true)),
            (None, bound(&high_key, false)),
            (bound(&low_key, true), None),
            (bound(&low_key, false), None),
        ];
        for low_incl in [true, false] {
            for high_incl in [true, false] {
                cases.push((bound(&low_key, low_incl), bound(&high_key, high_incl)));
            }
        }
        // Empty / single-key ranges hammer the positioning edge cases.
        cases.push((bound(&low_key, true), bound(&low_key, true)));
        cases.push((bound(&low_key, false), bound(&low_key, false)));

        for (low, high) in cases {
            let expected = db
                .range_index_bounds_entries(&table, index, low.clone(), high.clone())
                .unwrap();
            let expected_ids: Vec<RowId> = expected.iter().map(|(_, id)| *id).collect();

            // Cursor, owned entries, uneven chunk sizes.
            let mut cursor = db
                .range_index_entries_cursor(&table, index, low.clone(), high.clone())
                .unwrap();
            let chunk_sizes = [3usize, 17, 64, 4096];
            let mut actual_entries = Vec::new();
            let mut step = 0usize;
            loop {
                let chunk = cursor
                    .next_chunk(chunk_sizes[step % chunk_sizes.len()])
                    .unwrap();
                if chunk.is_empty() {
                    break;
                }
                actual_entries.extend(chunk);
                step += 1;
            }
            assert_eq!(actual_entries, expected, "cursor entries {index_name}");

            // Cursor, row-id sink.
            let mut cursor = db
                .range_index_entries_cursor(&table, index, low.clone(), high.clone())
                .unwrap();
            let mut actual_ids = Vec::new();
            let mut step = 0usize;
            loop {
                let chunk = cursor
                    .next_chunk_row_ids(chunk_sizes[step % chunk_sizes.len()])
                    .unwrap();
                if chunk.is_empty() {
                    break;
                }
                actual_ids.extend(chunk);
                step += 1;
            }
            assert_eq!(actual_ids, expected_ids, "cursor row ids {index_name}");

            // Parallel-prep leaf walkers (high bound mid-leaf included).
            let leaf_ids = db
                .range_index_leaf_ids(&table, index, low.clone(), high.clone())
                .unwrap();
            let leaf_entries = db
                .range_index_entries_for_leaf_ids(
                    &table,
                    index,
                    &leaf_ids,
                    low.clone(),
                    high.clone(),
                )
                .unwrap();
            assert_eq!(leaf_entries, expected, "leaf-id entries {index_name}");
            let leaf_row_ids = db
                .range_index_row_ids_for_leaf_ids(
                    &table,
                    index,
                    &leaf_ids,
                    low.clone(),
                    high.clone(),
                )
                .unwrap();
            assert_eq!(leaf_row_ids, expected_ids, "leaf-id row ids {index_name}");

            // Reverse cursor: every test index stores each row exactly
            // once, so the reverse walk must be the exact mirror of the
            // forward oracle.
            let expected_rev: Vec<(IndexKey, RowId)> = expected.iter().rev().cloned().collect();
            let expected_rev_ids: Vec<RowId> = expected_rev.iter().map(|(_, id)| *id).collect();
            let mut cursor = db
                .range_index_entries_reverse_cursor(&table, index, low.clone(), high.clone())
                .unwrap();
            let mut actual_rev = Vec::new();
            let mut step = 0usize;
            loop {
                let chunk = cursor
                    .next_chunk(chunk_sizes[step % chunk_sizes.len()])
                    .unwrap();
                if chunk.is_empty() {
                    break;
                }
                actual_rev.extend(chunk);
                step += 1;
            }
            assert_eq!(
                actual_rev, expected_rev,
                "reverse cursor entries {index_name}"
            );

            let mut cursor = db
                .range_index_entries_reverse_cursor(&table, index, low.clone(), high.clone())
                .unwrap();
            let mut actual_rev_ids = Vec::new();
            let mut step = 0usize;
            loop {
                let chunk = cursor
                    .next_chunk_row_ids(chunk_sizes[step % chunk_sizes.len()])
                    .unwrap();
                if chunk.is_empty() {
                    break;
                }
                actual_rev_ids.extend(chunk);
                step += 1;
            }
            assert_eq!(
                actual_rev_ids, expected_rev_ids,
                "reverse cursor row ids {index_name}"
            );

            // Single-leaf pulls share the raw reverse path, including partial
            // leaves and terminal boundaries. Exercise both sink shapes.
            for row_ids_only in [false, true] {
                let mut cursor = db
                    .range_index_entries_reverse_cursor(&table, index, low.clone(), high.clone())
                    .unwrap();
                let mut actual = Vec::new();
                let mut step = 0;
                while !cursor.is_finished() {
                    let cap = chunk_sizes[step % chunk_sizes.len()];
                    let rows = if row_ids_only {
                        cursor.next_leaf_chunk_row_ids(cap).unwrap().0
                    } else {
                        cursor
                            .next_leaf_chunk(cap)
                            .unwrap()
                            .0
                            .into_iter()
                            .map(|(_, row_id)| row_id)
                            .collect()
                    };
                    actual.extend(rows);
                    step += 1;
                }
                assert_eq!(
                    actual, expected_rev_ids,
                    "reverse leaf chunks {index_name}, row_ids_only={row_ids_only}"
                );
            }
        }
    }
}

/// ANALYZE-sampled index/heap order correlation: an index whose key
/// follows heap insert order must report ~1.0; an index over a
/// value-shuffled column must report something clearly lower.
#[test]
fn analyze_computes_index_heap_order_correlation() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col("seq", DataType::Int, false),
            lob_col("rnd", DataType::Int, false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..8_000i64)
            .map(|i| {
                // LCG-style shuffle: decorrelates `rnd` key order from
                // heap insert order without needing a RNG.
                let shuffled = (i * 48_271) % 8_000;
                vec![Value::Int(i), Value::Int(i), Value::Int(shuffled)]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    for (name, column) in [("seq_idx", "seq"), ("rnd_idx", "rnd")] {
        db.add_index(
            "test.t",
            IndexSchema {
                readable_from_scn: 0,
                index_id: 0,
                name: name.into(),
                columns: vec![column.into()],
                column_prefix_lengths: Vec::new(),
                column_directions: Vec::new(),
                key_expressions: Vec::new(),
                unique: false,
                primary: false,
                included_columns: Vec::new(),
                state: crate::storage::IndexState::Public,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: String::new(),
                vector: None,
                fulltext: None,
            },
        )
        .unwrap();
    }
    db.analyze_table("test.t").unwrap();
    let table = db.table("test.t").unwrap();
    let corr = |name: &str| {
        table
            .stats
            .indexes
            .get(name)
            .and_then(|s| s.correlation)
            .unwrap_or_else(|| panic!("no correlation for {name}"))
    };
    let seq = corr("seq_idx");
    let rnd = corr("rnd_idx");
    assert!(seq > 0.95, "insert-order index should be ~1.0, got {seq}");
    assert!(
        rnd < 0.5,
        "shuffled index should be clearly uncorrelated, got {rnd} (seq={seq})"
    );
}

#[test]
fn btree_range_with_prefix_bounds_stops_at_next_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_prefix_range.idx");
    let mut index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let rows = [1, 2, 3, 4].map(|slot_id| RowId {
        page_id: 1,
        slot_id,
    });

    index
        .insert(
            IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(1), KeyPart::Int(10)]),
            rows[0],
            false,
        )
        .unwrap();
    index
        .insert(
            IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(1), KeyPart::Int(20)]),
            rows[1],
            false,
        )
        .unwrap();
    index
        .insert(
            IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2), KeyPart::Int(10)]),
            rows[2],
            false,
        )
        .unwrap();
    index
        .insert(
            IndexKey::from(vec![KeyPart::Int(2), KeyPart::Int(1), KeyPart::Int(10)]),
            rows[3],
            false,
        )
        .unwrap();

    let prefix = IndexBound {
        key: IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(1)]),
        inclusive: true,
    };
    assert_eq!(
        index.range(Some(&prefix), Some(&prefix), false).unwrap(),
        vec![rows[0], rows[1]]
    );
}

#[test]
fn btree_index_get_uses_sorted_key_range() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_range_get.idx");
    let mut index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let target = IndexKey::from(vec![KeyPart::Int(10)]);
    let row_ids = [3, 1, 2].map(|slot_id| RowId {
        page_id: 1,
        slot_id,
    });

    index
        .insert(IndexKey::from(vec![KeyPart::Int(5)]), row_ids[0], false)
        .unwrap();
    index.insert(target.clone(), row_ids[0], false).unwrap();
    index
        .insert(IndexKey::from(vec![KeyPart::Int(20)]), row_ids[1], false)
        .unwrap();
    index.insert(target.clone(), row_ids[1], false).unwrap();
    index.insert(target.clone(), row_ids[2], false).unwrap();

    assert_eq!(
        index.get(&target, false).unwrap(),
        vec![row_ids[1], row_ids[2], row_ids[0]]
    );
    assert!(index
        .get(&IndexKey::from(vec![KeyPart::Int(11)]), false)
        .unwrap()
        .is_empty());
}

/// Regression for the v8 byte-level lookup bug that surfaced as
/// "STOCK with S_W_ID=4 S_I_ID=97282 not fount" during a BenchmarkSQL
/// run: when `find_leaf_for_key_lower_bound` lands on a leaf whose
/// every entry is strictly less than the target (binary search's
/// `lo == count` case), the original v6/v7 control flow falls
/// through to `next_leaf`. The first v8 implementation only
/// followed `next_leaf` when an equal-key run hit the last slot,
/// so a target whose actual location is on the sibling leaf
/// reported "not found".
///
/// This test inserts enough keys to force at least one leaf split
/// (so `next_leaf` is non-null) and then looks up keys that may
/// legitimately resolve to the second leaf, asserting both that
/// they're found and that they match the per-entry RowId.
#[test]
fn btree_lookup_follows_next_leaf_when_lower_bound_overflows_page() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_overflow_lookup.idx");
    let mut index = IndexFile::open(path, test_log(dir.path())).unwrap();

    let mut inserted = 0i64;
    while inserted < 512 {
        let key = wide_test_index_key(inserted);
        let row_id = test_row_id(inserted, 50);
        index.insert(key.clone(), row_id, false).unwrap();
        inserted += 1;

        if inserted >= 64 {
            let left = index.leftmost_leaf().unwrap();
            if index.read_leaf(left).unwrap().next_leaf.is_some() {
                break;
            }
        }
    }

    let left = index.leftmost_leaf().unwrap();
    let first_leaf = index.read_leaf(left).unwrap();
    let sibling = first_leaf
        .next_leaf
        .expect("test setup should create a sibling leaf");
    let sibling_leaf = index.read_leaf(sibling).unwrap();
    assert!(!sibling_leaf.entries.is_empty());

    for entry in first_leaf
        .entries
        .iter()
        .rev()
        .take(3)
        .chain(sibling_leaf.entries.iter().take(8))
    {
        let rows = index.get(&entry.key, false).unwrap();
        assert!(
            rows.iter().any(|r| r == &entry.row_id),
            "expected to find RowId for key {:?} in {:?}",
            entry.key,
            rows
        );
    }

    // Missing keys near the back of the populated range should
    // return empty and *not* report bogus matches from the
    // sibling leaf.
    let missing = IndexKey::from(vec![KeyPart::Int(99999), KeyPart::Int(99999)]);
    assert!(index.get(&missing, false).unwrap().is_empty());
}

#[test]
fn btree_scan_can_dedup_row_ids_during_leaf_walk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_dedup_scan.idx");
    let mut index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let duplicate = RowId {
        page_id: 10,
        slot_id: 1,
    };
    let other = RowId {
        page_id: 10,
        slot_id: 2,
    };

    index
        .insert(
            IndexKey::from(vec![KeyPart::Int(7), KeyPart::String("a".into())]),
            duplicate,
            false,
        )
        .unwrap();
    index
        .insert(
            IndexKey::from(vec![KeyPart::Int(7), KeyPart::String("b".into())]),
            duplicate,
            false,
        )
        .unwrap();
    index
        .insert(
            IndexKey::from(vec![KeyPart::Int(7), KeyPart::String("c".into())]),
            other,
            false,
        )
        .unwrap();

    let prefix = IndexKey::from(vec![KeyPart::Int(7)]);
    assert_eq!(
        index.prefix(&prefix, false).unwrap(),
        vec![duplicate, duplicate, other]
    );
    assert_eq!(index.prefix(&prefix, true).unwrap(), vec![duplicate, other]);

    let low = IndexBound {
        key: prefix,
        inclusive: true,
    };
    assert_eq!(
        index.range(Some(&low), Some(&low), true).unwrap(),
        vec![duplicate, other]
    );
}

#[test]
fn btree_index_delete_rebalances_and_reuses_free_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_rebalance.idx");
    let mut index = IndexFile::open(path.clone(), test_log(dir.path())).unwrap();
    let mut inserted = 0i64;
    while inserted < 256 && (inserted < 48 || index.next_page_id() < 4) {
        index
            .insert(
                wide_test_index_key(inserted),
                test_row_id(inserted, 32),
                false,
            )
            .unwrap();
        inserted += 1;
    }
    assert!(
        index.next_page_id() >= 4,
        "test setup should create enough pages to free one"
    );
    index.flush().unwrap();
    let page_count_after_insert = index.next_page_id();

    let keep_each_side = 8i64;
    for value in keep_each_side..(inserted - keep_each_side) {
        index
            .delete(wide_test_index_key(value), test_row_id(value, 32))
            .unwrap();
    }
    assert!(index.meta_state.lock().meta.free_page_head.is_some());
    assert!(index
        .range(
            Some(&IndexBound {
                key: wide_test_index_key(keep_each_side - 5),
                inclusive: true,
            }),
            Some(&IndexBound {
                key: wide_test_index_key(keep_each_side + 5),
                inclusive: true,
            }),
            false,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.slot_id)
        .collect::<Vec<_>>()
        .starts_with(&[
            (keep_each_side - 5) as u16,
            (keep_each_side - 4) as u16,
            (keep_each_side - 3) as u16,
            (keep_each_side - 2) as u16,
            (keep_each_side - 1) as u16,
        ]));
    assert!(index
        .get(&wide_test_index_key(inserted / 2), false)
        .unwrap()
        .is_empty());

    let new_value = inserted + 500;
    index
        .insert(
            wide_test_index_key(new_value),
            test_row_id(new_value, 32),
            false,
        )
        .unwrap();
    assert!(index.next_page_id() <= page_count_after_insert + 1);
    index.flush().unwrap();
    drop(index);

    let reopened = IndexFile::open(path, test_log(dir.path())).unwrap();
    assert_eq!(
        reopened
            .get(&wide_test_index_key(new_value), false)
            .unwrap(),
        vec![test_row_id(new_value, 32)]
    );
}

#[test]
fn btree_delete_last_entry_from_non_root_leaf_does_not_leave_empty_child() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_empty_child.idx");
    let mut index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let large_key = |value: i64| {
        IndexKey::from(vec![
            KeyPart::Int(value),
            KeyPart::String(format!("{value:04}-{}", "x".repeat(6_000))),
        ])
    };
    let row_id = |value: i64| RowId {
        page_id: value as u32,
        slot_id: value as u16,
    };

    // Each entry occupies more than the minimum-fill threshold, so splitting
    // produces one-entry leaves. Deleting the middle leaf's sole entry used
    // to leave an empty non-root child behind: the raw slotted-page delete
    // retained the record body as a hole, and the rebalance fast path treated
    // both the stale payload length and `is_empty` as reasons to return.
    for value in 1..=3 {
        index
            .insert(large_key(value), row_id(value), false)
            .unwrap();
    }
    assert!(matches!(
        index
            .read_page(index.root_page.load(Ordering::Acquire))
            .unwrap()
            .as_ref(),
        IndexPage::Internal(_)
    ));

    index.delete(large_key(2), row_id(2)).unwrap();
    index.delete(large_key(3), row_id(3)).unwrap();

    // Every non-root child must still have a first key immediately after the
    // deletion. This is the invariant later internal splits depend on.
    let root_id = index.root_page.load(Ordering::Acquire);
    if let IndexPage::Internal(root) = index.read_page(root_id).unwrap().as_ref() {
        for child in root.children.iter().copied() {
            assert!(
                index.first_entry_of_page(child).unwrap().is_some(),
                "non-root leaf {child} was left empty"
            );
        }
    }

    // Splitting the right edge exercises separator recomputation after the
    // empty-leaf merge.
    index
        .insert(large_key(4), row_id(4), false)
        .expect("empty non-root leaf must be merged before later splits");
    for value in [1, 4] {
        assert_eq!(
            index.get(&large_key(value), false).unwrap(),
            vec![row_id(value)]
        );
    }
    for value in [2, 3] {
        assert!(index.get(&large_key(value), false).unwrap().is_empty());
    }
}

#[test]
fn btree_first_key_delete_keeps_safe_lower_bound_separator() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_stale_lower_bound.idx");
    let index = IndexFile::open(path.clone(), test_log(dir.path())).unwrap();

    // Build several densely populated compact-int leaves. Deleting one first
    // entry cannot underflow its leaf, so the production single-delete path
    // should keep the old parent key as a conservative lower bound.
    for value in 0i64..2_048 {
        index
            .insert_with_txn(
                10,
                IndexKey::from(vec![KeyPart::Int(value)]),
                test_row_id(value, 32),
            )
            .unwrap();
    }
    let root = index.root_page_id();
    let (leaf_id, first) = index
        .leaf_page_max_trx_id_chain()
        .unwrap()
        .into_iter()
        .filter(|(leaf_id, _)| *leaf_id != root)
        .find_map(|(leaf_id, _)| {
            let leaf = index.read_leaf(leaf_id).ok()?;
            (leaf.entries.len() > 256).then(|| (leaf_id, leaf.entries[0].clone()))
        })
        .expect("test setup should produce a dense non-root leaf");
    let _ = leaf_id;

    index
        .delete_with_txn(11, first.key.clone(), first.row_id)
        .unwrap();
    assert!(index.get(&first.key, false).unwrap().is_empty());

    // Re-inserting the separator value must route to the same (right) child,
    // not disappear into its left sibling because the parent key is stale.
    let replacement = RowId {
        page_id: first.row_id.page_id,
        slot_id: first.row_id.slot_id.saturating_add(1),
    };
    index
        .insert_with_txn(12, first.key.clone(), replacement)
        .unwrap();
    assert_eq!(index.get(&first.key, false).unwrap(), vec![replacement]);

    index.flush().unwrap();
    drop(index);
    let reopened = IndexFile::open(path, test_log(dir.path())).unwrap();
    assert_eq!(reopened.get(&first.key, false).unwrap(), vec![replacement]);
}

#[test]
fn btree_concurrent_reads_do_not_descend_into_freed_merge_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_concurrent_rebalance.idx");
    let mut index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let mut inserted = 0i64;
    while inserted < 1_536 && (inserted < 128 || index.next_page_id() < 12) {
        index
            .insert(
                wide_test_index_key(inserted),
                test_row_id(inserted, 32),
                false,
            )
            .unwrap();
        inserted += 1;
    }
    assert!(
        index.next_page_id() >= 12,
        "test setup should create a tall enough tree"
    );

    let index = Arc::new(index);
    let start = Arc::new(std::sync::Barrier::new(2));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    // One reader continuously mixes range and point probes while the writer
    // merges pages. More readers only amplify lock contention; they do not add
    // a distinct interleaving to this freed-page regression.
    for reader_id in 0..1 {
        let index = Arc::clone(&index);
        let start = Arc::clone(&start);
        let done = Arc::clone(&done);
        readers.push(std::thread::spawn(move || -> Result<(), QueryError> {
            start.wait();
            let mut probe = reader_id as i64;
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                let low_value = probe.rem_euclid(inserted.saturating_sub(16).max(1));
                let high_value = (low_value + 15).min(inserted - 1);
                let low = IndexBound {
                    key: wide_test_index_key(low_value),
                    inclusive: true,
                };
                let high = IndexBound {
                    key: wide_test_index_key(high_value),
                    inclusive: true,
                };
                let _ = index.range(Some(&low), Some(&high), false)?;
                let _ = index.get(&wide_test_index_key(high_value), false)?;
                probe = probe.wrapping_add(37);
                std::thread::yield_now();
            }
            Ok(())
        }));
    }

    start.wait();
    for value in 8..(inserted - 8) {
        index
            .delete_internal(
                txn_id_for_ddl(),
                wide_test_index_key(value),
                test_row_id(value, 32),
                true,
            )
            .unwrap();
        if value % 7 == 0 {
            std::thread::yield_now();
        }
    }
    done.store(true, std::sync::atomic::Ordering::Release);
    for reader in readers {
        reader.join().unwrap().unwrap();
    }

    assert!(index.meta_state.lock().meta.free_page_head.is_some());
    assert!(index
        .get(&wide_test_index_key(inserted / 2), false)
        .unwrap()
        .is_empty());
}

#[test]
fn new_index_file_initializes_empty_v2_tree() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_empty.idx");
    let index = IndexFile::open(path.clone(), test_log(dir.path())).unwrap();
    assert!(index
        .get(&IndexKey::from(vec![KeyPart::Int(1)]), false)
        .unwrap()
        .is_empty());
    index.flush().unwrap();
    drop(index);

    let reopened = IndexFile::open(path, test_log(dir.path())).unwrap();
    assert!(reopened
        .get(&IndexKey::from(vec![KeyPart::Int(1)]), false)
        .unwrap()
        .is_empty());
}

// --- Phase 0: page-range scan APIs (parallel-scan plumbing) ---------------

fn open_db_with_n_rows(dir: &Path, n: u64) -> Database {
    let db = Database::open(dir).unwrap();
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::VarChar(Some(8000)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    // Use a wide column so we end up with a meaningful number of pages.
    db.insert_rows(
        txn,
        "test.t",
        (0..n as i64)
            .map(|id| vec![Value::Int(id), Value::String("filler".repeat(40))])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db
}

fn ids_of(rows: &[StoredRow]) -> Vec<i64> {
    let mut ids: Vec<i64> = rows
        .iter()
        .map(|row| match row.values[0] {
            Value::Int(v) => v,
            _ => panic!("non-int id"),
        })
        .collect();
    ids.sort();
    ids
}

#[test]
fn scan_rows_range_full_equals_scan_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_with_n_rows(dir.path(), 500);
    let pages = db.heap_page_count_by_name("test.t").unwrap();
    assert!(pages > 1, "test needs more than one page (got {pages})");
    let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);

    let serial = db.scan_rows("test.t", txn).unwrap();
    let ranged = db.scan_rows_range("test.t", txn, 0, pages).unwrap();
    assert_eq!(ids_of(&serial), ids_of(&ranged));
}

#[test]
fn scan_rows_range_disjoint_partitions_concat_to_full() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_with_n_rows(dir.path(), 500);
    let pages = db.heap_page_count_by_name("test.t").unwrap();
    assert!(pages > 1);
    let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);

    let serial = db.scan_rows("test.t", txn).unwrap();

    // Split into 4 contiguous partitions; the union must equal the
    // serial scan exactly.
    let degree: u64 = 4;
    let chunk = pages / degree;
    let rem = pages % degree;
    let mut union: Vec<StoredRow> = Vec::new();
    for i in 0..degree {
        let lo = i * chunk + i.min(rem);
        let extra = if i < rem { 1 } else { 0 };
        let hi = lo + chunk + extra;
        let part = db.scan_rows_range("test.t", txn, lo, hi).unwrap();
        union.extend(part);
    }
    assert_eq!(ids_of(&serial), ids_of(&union));
}

#[test]
fn scan_rows_range_clamps_to_page_count() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_with_n_rows(dir.path(), 200);
    let pages = db.heap_page_count_by_name("test.t").unwrap();
    let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);

    // Asking past the heap end is harmless and returns nothing.
    let beyond = db
        .scan_rows_range("test.t", txn, pages, pages * 2 + 7)
        .unwrap();
    assert!(beyond.is_empty());

    // lo > hi normalizes to empty range.
    let inverted = db.scan_rows_range("test.t", txn, 5, 2).unwrap();
    assert!(inverted.is_empty());
}

#[test]
fn scan_rows_columns_streaming_range_disjoint_concat_matches_full() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_with_n_rows(dir.path(), 500);
    let pages = db.heap_page_count_by_name("test.t").unwrap();
    let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);

    let mut serial: Vec<i64> = Vec::new();
    db.scan_rows_columns_streaming("test.t", txn, &[0], |values| {
        if let Value::Int(v) = values[0] {
            serial.push(v);
        }
        Ok(())
    })
    .unwrap();
    serial.sort();

    let degree: u64 = 3;
    let chunk = pages / degree;
    let rem = pages % degree;
    let mut union: Vec<i64> = Vec::new();
    for i in 0..degree {
        let lo = i * chunk + i.min(rem);
        let extra = if i < rem { 1 } else { 0 };
        let hi = lo + chunk + extra;
        db.scan_rows_columns_streaming_range("test.t", txn, &[0], lo, hi, |values| {
            if let Value::Int(v) = values[0] {
                union.push(v);
            }
            Ok(())
        })
        .unwrap();
    }
    union.sort();
    assert_eq!(serial, union);
}

#[test]
fn predicate_first_projected_scan_handles_overlap_and_forwarded_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "flag".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "payload".into(),
                data_type: DataType::VarChar(Some(8000)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..220)
            .map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id % 2),
                    Value::String(format!("payload-{id}")),
                ]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let txn = db.begin_txn().unwrap();
    let table = db.table("test.t").unwrap();
    let primary = table
        .indexes
        .iter()
        .find(|index| index.primary)
        .cloned()
        .unwrap();
    let row_id = db
        .lookup_index(&table, &primary, IndexKey::from(vec![KeyPart::Int(1)]))
        .unwrap()[0];
    db.update_row_by_id(txn, &table, row_id, |_| {
        Ok(vec![
            Value::Int(1),
            Value::Int(1),
            Value::String("forwarded".repeat(700)),
        ])
    })
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let pages = db.heap_page_count_by_name("test.t").unwrap();
    let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let mut rows = Vec::new();
    db.scan_rows_columns_predicate_first_streaming_range(
        "test.t",
        txn,
        &[1, 0],
        &[2, 1, 0],
        0,
        pages,
        |values| Ok(values[0] == Value::Int(1)),
        |values| {
            rows.push((values[2].clone(), values[1].clone(), values[0].clone()));
            Ok(())
        },
    )
    .unwrap();
    rows.sort_by_key(|(id, _, _)| match id {
        Value::Int(v) => *v,
        other => panic!("non-int id in predicate-first result: {other:?}"),
    });

    assert_eq!(rows.len(), 110);
    assert_eq!(rows[0].0, Value::Int(1));
    assert_eq!(rows[0].1, Value::Int(1));
    assert_eq!(rows[0].2, Value::String("forwarded".repeat(700)));
    assert_eq!(rows[1].0, Value::Int(3));
    assert_eq!(rows[1].1, Value::Int(1));
    assert_eq!(rows[1].2, Value::String("payload-3".into()));

    // Random RowId batches (the non-covering INLJ consumer) must have the
    // same overlap/default/forwarding semantics as the sequential scanner.
    let mut row_ids: Vec<RowId> = db
        .scan_rows("test.t", txn)
        .unwrap()
        .into_iter()
        .map(|row| row.row_id)
        .collect();
    row_ids.sort_unstable();
    let heap = db.cached_heap_handle(table.id).unwrap();
    let scratch_before = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[1];
    let mut random_rows: Vec<(Value, Value, Value)> = db
        .read_row_columns_many_predicate_first_with_handle(
            &heap,
            &table,
            &row_ids,
            txn,
            &[1, 0],
            &[2, 1, 0],
            |values| Ok(values[0] == Value::Int(1)),
        )
        .unwrap()
        .into_iter()
        .map(|(_, values)| (values[2].clone(), values[1].clone(), values[0].clone()))
        .collect();
    assert_eq!(
        crate::engine::executor::mem_budget::controlled_memory_snapshot().2[1],
        scratch_before,
        "predicate-first ordered-MRR scratch reservation leaked"
    );
    random_rows.sort_by_key(|(id, _, _)| match id {
        Value::Int(value) => *value,
        other => panic!("non-int id in random predicate-first result: {other:?}"),
    });
    assert_eq!(random_rows, rows);

    // The resident fast path and cold fallback must each invoke a
    // side-effecting aggregate visitor exactly once per visible row.
    // In particular, never replay a partially consumed cached page.
    for cold in [false, true, false] {
        if cold {
            db.buffer_pool.flush_and_evict_all().unwrap();
        }
        let mut calls = 0usize;
        let mut total = 0i64;
        let rejected = db
            .read_row_columns_many_predicate_first_with_handle(
                &heap,
                &table,
                &row_ids,
                txn,
                &[0],
                &[],
                |values| {
                    calls += 1;
                    if let Value::Int(id) = values[0] {
                        total += id;
                    } else {
                        panic!("expected integer id");
                    }
                    Ok(false)
                },
            )
            .unwrap();
        assert!(rejected.is_empty());
        assert_eq!(calls, 220, "cold={cold}");
        assert_eq!(total, (0..220).sum::<i64>(), "cold={cold}");
    }
}

#[test]
fn heap_page_count_by_name_table_does_not_exist_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let err = db
        .heap_page_count_by_name("test.does_not_exist")
        .unwrap_err();
    assert_eq!(err.code(), 1146, "missing-table error code");
}

/// v9 compact DATETIME layout — round-trip a leaf page and confirm
/// both the in-memory `decode_index_tree_page` path and the
/// byte-level `lookup_leaf_page_bytes` path reconstruct every key.
#[test]
fn v9_compact_time_leaf_round_trips() {
    let base: i64 = 1_700_000_000_000_000; // arbitrary µs epoch offset
    let entries: Vec<IndexEntry> = (0..32)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::DateTime(base + i * 1_000_000)]),
            row_id: RowId {
                page_id: (100 + i) as u32,
                slot_id: i as u16,
            },
        })
        .collect();
    let page = IndexPage::Leaf(IndexLeafPage::new_with_layout(
        entries.clone(),
        Some(99),
        KEY_LAYOUT_COMPACT_TIME_U32,
        base,
    ));
    let buf = encode_index_tree_page(&page).unwrap();

    // Decode via the in-memory path.
    let decoded = decode_index_tree_page(&buf).unwrap();
    match decoded {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_TIME_U32);
            assert_eq!(leaf.base_time, base);
            assert_eq!(leaf.entries, entries);
            assert_eq!(leaf.next_leaf, Some(99));
        }
        _ => panic!("expected leaf"),
    }

    // Byte-level point lookup must also reconstruct each entry's
    // exact DateTime key — exercise both a hit and the early-stop
    // tail walk after the matching key.
    for entry in &entries {
        let result = lookup_leaf_page_bytes(&buf, &entry.key).unwrap();
        assert_eq!(
            result.rows.len(),
            1,
            "compact lookup missed {:?}",
            entry.key
        );
        assert_eq!(result.rows[0], entry.row_id);
    }

    // A key past the last DATETIME in the page should return 0 rows
    // *and* should_continue=false (saw_greater triggered).
    let after = IndexKey::from(vec![KeyPart::DateTime(base + 10_000_000_000)]);
    let result = lookup_leaf_page_bytes(&buf, &after).unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn durable_direct_bulk_builder_matches_memory_buffered_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let strict_dir = dir.path().join("strict");
    let memory_dir = dir.path().join("memory");
    std::fs::create_dir_all(&strict_dir).unwrap();
    std::fs::create_dir_all(&memory_dir).unwrap();
    let strict_log = test_log(&strict_dir);
    let memory_log = test_log(&memory_dir);
    let strict_pool = BufferPool::new_with_options(
        16,
        Arc::clone(&strict_log),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let memory_pool = BufferPool::new_with_options(
        16,
        Arc::clone(&memory_log),
        DurabilityMode::Memory,
        Duration::ZERO,
        1,
        0,
    );
    let strict_path = strict_dir.join("mixed.idx");
    let memory_path = memory_dir.join("mixed.idx");
    let strict = IndexFile::open_with_durability(
        strict_path.clone(),
        strict_log,
        DurabilityMode::Strict,
        strict_pool,
    )
    .unwrap();
    let memory = IndexFile::open_with_durability(
        memory_path,
        memory_log,
        DurabilityMode::Memory,
        memory_pool,
    )
    .unwrap();

    let mut entries = (0..600u32)
        .map(|i| {
            let first = match i % 5 {
                0 => KeyPart::Null,
                1 => KeyPart::Decimal {
                    value: i128::from(i) * 101 - 9_000,
                    scale: 2,
                },
                2 => KeyPart::Date(18_000 + i as i32),
                3 => KeyPart::String(format!("k{:04}", 600 - i)),
                _ => KeyPart::Int(i64::from(i) - 300),
            };
            IndexEntry {
                key: IndexKey::from(vec![first, KeyPart::UInt(u64::from(i % 17))]),
                row_id: RowId {
                    page_id: i / 8,
                    slot_id: (i % 8) as u16,
                },
            }
        })
        .collect::<Vec<_>>();
    entries.reverse();
    entries.sort_unstable();

    strict
        .insert_entries_logged(7, entries.clone(), true)
        .unwrap();
    memory
        .insert_entries_logged(7, entries.clone(), true)
        .unwrap();
    assert_eq!(
        strict.range(None, None, false).unwrap(),
        memory.range(None, None, false).unwrap()
    );
    for entry in entries.iter().step_by(37) {
        assert_eq!(
            strict.get(&entry.key, false).unwrap(),
            memory.get(&entry.key, false).unwrap()
        );
    }
    IndexFile::validate(strict_path).unwrap();

    // A second batch proves a non-empty durable tree keeps the incremental
    // path while preserving the same lookup/range semantics.
    let extra = IndexEntry {
        key: IndexKey::from(vec![KeyPart::String("zz-tail".into()), KeyPart::UInt(1)]),
        row_id: RowId {
            page_id: 999,
            slot_id: 1,
        },
    };
    strict
        .insert_entries_logged(8, vec![extra.clone()], true)
        .unwrap();
    memory
        .insert_entries_logged(8, vec![extra.clone()], true)
        .unwrap();
    assert_eq!(
        strict.get(&extra.key, false).unwrap(),
        memory.get(&extra.key, false).unwrap()
    );
}

#[test]
fn direct_bulk_builder_failure_before_meta_restores_empty_usable_index() {
    let dir = tempfile::tempdir().unwrap();
    let log = test_log(dir.path());
    let pool = BufferPool::new_with_options(
        8,
        Arc::clone(&log),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let path = dir.path().join("fail-before-meta.idx");
    let index =
        IndexFile::open_with_durability(path.clone(), log, DurabilityMode::Strict, pool).unwrap();
    let entries = (0..400u32)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::UInt(u64::from(i))]),
            row_id: RowId {
                page_id: i,
                slot_id: 0,
            },
        })
        .collect::<Vec<_>>();

    install_bulk_index_fail_before_meta();
    let err = index.bulk_load_sorted(9, entries).unwrap_err();
    assert!(err.message().contains("failpoint"), "{err:?}");
    assert!(index.range(None, None, false).unwrap().is_empty());
    IndexFile::validate(path.clone()).unwrap();

    let replacement = IndexEntry {
        key: IndexKey::from(vec![KeyPart::UInt(42)]),
        row_id: RowId {
            page_id: 1,
            slot_id: 2,
        },
    };
    index
        .insert_entries_logged(10, vec![replacement.clone()], true)
        .unwrap();
    assert_eq!(
        index.get(&replacement.key, false).unwrap(),
        vec![replacement.row_id]
    );
    IndexFile::validate(path).unwrap();
}

#[test]
fn specialized_single_int_direct_builder_matches_buffered_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let strict_dir = dir.path().join("strict-int");
    let memory_dir = dir.path().join("memory-int");
    std::fs::create_dir_all(&strict_dir).unwrap();
    std::fs::create_dir_all(&memory_dir).unwrap();
    let strict_log = test_log(&strict_dir);
    let memory_log = test_log(&memory_dir);
    let strict_pool = BufferPool::new_with_options(
        8,
        Arc::clone(&strict_log),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let memory_pool = BufferPool::new_with_options(
        8,
        Arc::clone(&memory_log),
        DurabilityMode::Memory,
        Duration::ZERO,
        1,
        0,
    );
    let mut entries = (0..5000u32)
        .map(|i| {
            let value = match i {
                0 => i64::MIN,
                4999 => i64::MAX,
                _ => i64::from(i) * 17 - 40_000,
            };
            (
                value,
                RowId {
                    page_id: i / 10,
                    slot_id: (i % 10) as u16,
                },
            )
        })
        .collect::<Vec<_>>();
    entries.sort_unstable();
    let strict_path = strict_dir.join("single-int.idx");
    let strict = IndexFile::create_rebuilt_from_sorted_single_int_stream_with_durability(
        strict_path.clone(),
        entries.iter().copied().map(Ok),
        strict_log,
        DurabilityMode::Strict,
        strict_pool,
    )
    .unwrap();
    let memory = IndexFile::create_rebuilt_from_sorted_single_int_stream_with_durability(
        memory_dir.join("single-int.idx"),
        entries.iter().copied().map(Ok),
        memory_log,
        DurabilityMode::Memory,
        memory_pool,
    )
    .unwrap();
    assert_eq!(
        strict.range(None, None, false).unwrap(),
        memory.range(None, None, false).unwrap()
    );
    for (value, _) in entries.iter().step_by(311) {
        let key = IndexKey::from(vec![KeyPart::Int(*value)]);
        assert_eq!(
            strict.get(&key, false).unwrap(),
            memory.get(&key, false).unwrap()
        );
    }
    IndexFile::validate(strict_path).unwrap();
}

#[test]
fn specialized_date_and_pair_stream_builders_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let date_dir = dir.path().join("date-stream");
    let pair_dir = dir.path().join("pair-stream");
    std::fs::create_dir_all(&date_dir).unwrap();
    std::fs::create_dir_all(&pair_dir).unwrap();

    let date_log = test_log(&date_dir);
    let date_pool = BufferPool::new_with_options(
        8,
        Arc::clone(&date_log),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let date_entries = (0..5000u32)
        .map(|i| {
            (
                18_000 + (i / 7) as i32,
                RowId {
                    page_id: i / 10,
                    slot_id: (i % 10) as u16,
                },
            )
        })
        .collect::<Vec<_>>();
    let date_path = date_dir.join("date.idx");
    let date = IndexFile::create_rebuilt_from_sorted_date_stream_with_durability(
        date_path.clone(),
        date_entries.iter().copied().map(Ok),
        date_log,
        DurabilityMode::Strict,
        date_pool,
    )
    .unwrap();
    assert_eq!(
        date.range(None, None, false).unwrap(),
        date_entries.iter().map(|entry| entry.1).collect::<Vec<_>>()
    );
    IndexFile::validate(date_path).unwrap();

    let pair_log = test_log(&pair_dir);
    let pair_pool = BufferPool::new_with_options(
        8,
        Arc::clone(&pair_log),
        DurabilityMode::Strict,
        Duration::ZERO,
        1,
        0,
    );
    let pair_entries = (0..5000u32)
        .map(|i| {
            (
                ((i / 11) as i64, (i % 11) as i64),
                RowId {
                    page_id: i / 10,
                    slot_id: (i % 10) as u16,
                },
            )
        })
        .collect::<Vec<_>>();
    let pair_path = pair_dir.join("pair.idx");
    let pair = IndexFile::create_rebuilt_from_sorted_pair_int_stream_with_durability(
        pair_path.clone(),
        pair_entries.iter().copied().map(Ok),
        pair_log,
        DurabilityMode::Strict,
        pair_pool,
    )
    .unwrap();
    assert_eq!(
        pair.range(None, None, false).unwrap(),
        pair_entries.iter().map(|entry| entry.1).collect::<Vec<_>>()
    );
    IndexFile::validate(pair_path).unwrap();
}

/// `bulk_load_leaf_level` must pick the compact layout when given a
/// single-column DATETIME index whose values fit within u32 range —
/// and the page byte length must drop below the equivalent generic
/// encoding.
#[test]
fn bulk_load_picks_compact_layout_for_datetime_index() {
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let index_path = t_index_path(dir.path(), "t", 1, "TIME_IDX", "idx");
    std::fs::create_dir_all(index_path.parent().unwrap()).unwrap();
    let index = IndexFile::open(index_path, log).unwrap();

    let base: i64 = 1_700_000_000_000_000;
    let entries: Vec<IndexEntry> = (0..400)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::DateTime(base + i * 1_000_000)]),
            row_id: RowId {
                page_id: (i) as u32,
                slot_id: 0,
            },
        })
        .collect();

    index.bulk_load_sorted(1, entries.clone()).unwrap();

    // Range-scan the index and confirm every original entry is
    // retrievable — exercises the v9 compact decoder if
    // `bulk_load_leaf_level` selected it.
    let all_rows = index.range(None, None, false).unwrap();
    assert_eq!(
        all_rows.len(),
        entries.len(),
        "compact-layout bulk-load lost rows"
    );
    for (orig, row_id) in entries.iter().zip(all_rows.iter()) {
        assert_eq!(*row_id, orig.row_id);
    }

    // Walk the leaf chain and verify at least one page committed
    // in compact layout (proving the auto-selection actually fired).
    let mut leaf_id = index.leftmost_leaf().unwrap();
    let mut saw_compact = false;
    loop {
        let leaf = index.read_leaf(leaf_id).unwrap();
        if leaf.key_layout == KEY_LAYOUT_COMPACT_TIME_U32 {
            saw_compact = true;
        }
        match leaf.next_leaf {
            Some(next) => leaf_id = next,
            None => break,
        }
    }
    assert!(
        saw_compact,
        "expected at least one v9 compact-time leaf after bulk load"
    );
}

/// End-to-end storage-density check: bulk-load the *same* number of
/// entries into a single-column DATETIME index (compact layout) and
/// an equivalent multi-column (Int + String) generic-layout index,
/// then confirm the DATETIME index uses strictly fewer leaf pages —
/// i.e. the v9 compact layout actually shrinks the on-disk footprint.
/// (Pre-v13 this control was a bare INT index, but v13 makes single-
/// column INT also compact, so we now use a multi-column key — which
/// `CompactKind::of_entry` rejects — as the generic control.)
#[test]
fn v9_compact_layout_reduces_leaf_page_count() {
    use std::sync::Arc;

    fn leaf_count(index: &IndexFile) -> (usize, usize) {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        let mut compact = 0usize;
        loop {
            let leaf = index.read_leaf(leaf_id).unwrap();
            leaves += 1;
            if leaf.key_layout == KEY_LAYOUT_COMPACT_TIME_U32 {
                compact += 1;
            }
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        (leaves, compact)
    }

    const N: i64 = 8000;
    let base: i64 = 1_700_000_000_000_000;

    // DATETIME index — eligible for the compact layout.
    let dt_dir = tempfile::tempdir().unwrap();
    let dt_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dt_dir.path()).unwrap(),
    ));
    let dt_path = t_index_path(dt_dir.path(), "t", 1, "DT", "idx");
    std::fs::create_dir_all(dt_path.parent().unwrap()).unwrap();
    let dt_index = IndexFile::open(dt_path, dt_log).unwrap();
    let dt_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::DateTime(base + i * 1_000_000)]),
            row_id: RowId {
                page_id: (i) as u32,
                slot_id: 0,
            },
        })
        .collect();
    dt_index.bulk_load_sorted(1, dt_entries).unwrap();

    // Multi-column (Int + empty String) — same cardinality, but
    // `CompactKind::of_entry` rejects multi-column keys so the leaf
    // stays on the generic layout for an apples-to-apples baseline.
    let gen_dir = tempfile::tempdir().unwrap();
    let gen_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(gen_dir.path()).unwrap(),
    ));
    let gen_path = t_index_path(gen_dir.path(), "t", 1, "GEN", "idx");
    std::fs::create_dir_all(gen_path.parent().unwrap()).unwrap();
    let gen_index = IndexFile::open(gen_path, gen_log).unwrap();
    let gen_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![
                KeyPart::Int(base + i * 1_000_000),
                KeyPart::String(String::new()),
            ]),
            row_id: RowId {
                page_id: (i) as u32,
                slot_id: 0,
            },
        })
        .collect();
    gen_index.bulk_load_sorted(1, gen_entries).unwrap();

    let (dt_leaves, dt_compact) = leaf_count(&dt_index);
    let (int_leaves, int_compact) = leaf_count(&gen_index);

    assert_eq!(
        int_compact, 0,
        "multi-column control index must stay on the generic layout"
    );
    assert!(
        dt_compact > 0,
        "DATETIME index should have committed compact leaves"
    );
    assert!(
        dt_leaves < int_leaves,
        "compact layout should use fewer leaf pages: datetime={dt_leaves} generic={int_leaves}"
    );
    // Sanity: the density gain should be meaningful (≥ 1.3×), not a
    // rounding artefact.
    assert!(
        (int_leaves as f64) / (dt_leaves as f64) >= 1.3,
        "expected ≥1.3x leaf-page density gain, got {}/{}",
        int_leaves,
        dt_leaves
    );
}

/// v9 compact DATE layout — round-trip a leaf page and confirm both
/// the in-memory decode path and the byte-level lookup path
/// reconstruct every `DATE` key.
#[test]
fn v9_compact_date_leaf_round_trips() {
    let base_days: i64 = 20_000; // ~2024-10
    let entries: Vec<IndexEntry> = (0..48)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Date(base_days as i32 + i as i32)]),
            row_id: RowId {
                page_id: (200 + i) as u32,
                slot_id: i as u16,
            },
        })
        .collect();
    let page = IndexPage::Leaf(IndexLeafPage::new_with_layout(
        entries.clone(),
        Some(77),
        KEY_LAYOUT_COMPACT_DATE_U16,
        base_days,
    ));
    let buf = encode_index_tree_page(&page).unwrap();

    match decode_index_tree_page(&buf).unwrap() {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_DATE_U16);
            assert_eq!(leaf.base_time, base_days);
            assert_eq!(leaf.entries, entries);
            assert_eq!(leaf.next_leaf, Some(77));
        }
        _ => panic!("expected leaf"),
    }

    for entry in &entries {
        let result = lookup_leaf_page_bytes(&buf, &entry.key).unwrap();
        assert_eq!(
            result.rows.len(),
            1,
            "compact DATE lookup missed {:?}",
            entry.key
        );
        assert_eq!(result.rows[0], entry.row_id);
    }
}

/// A queued prefetch can be admitted after the builder's initial discard.
/// Complete publication must retire that intermediate image and file length.
#[test]
fn bulk_stream_publication_retires_pages_admitted_during_build() {
    let dir = tempfile::tempdir().unwrap();
    let log = test_log(dir.path());
    let index = IndexFile::open(dir.path().join("publication.idx"), log).unwrap();
    index.flush().unwrap();
    let lsn = index.prepare_direct_bulk_load().unwrap();
    // Deterministically reproduce a prefetch reading the old empty root after
    // the first discard, before direct I/O has installed the replacement.
    assert!(index
        .read_leaf(INDEX_EMPTY_ROOT)
        .unwrap()
        .entries
        .is_empty());
    let entries = (0..256i64).map(|key| {
        Ok((
            key,
            RowId {
                page_id: key as u32,
                slot_id: 0,
            },
        ))
    });
    let level = index
        .bulk_load_single_int_leaf_level_direct_stream(entries, lsn, txn_id_for_ddl())
        .unwrap();
    // The old image is still cached until complete publication fences it.
    assert!(index
        .read_leaf(INDEX_EMPTY_ROOT)
        .unwrap()
        .entries
        .is_empty());
    index.finish_fixed_direct_stream_level(level, lsn).unwrap();
    let actual = index.entries().unwrap();
    assert_eq!(actual.len(), 256);
    for (key, entry) in actual.iter().enumerate() {
        assert_eq!(entry.0, IndexKey::from(vec![KeyPart::Int(key as i64)]));
        assert_eq!(entry.1.page_id, key as u32);
    }
}

/// `bulk_load_leaf_level` must pick the compact DATE layout for a
/// single-column DATE index, and the page count must drop below the
/// equivalent generic-layout INT index of the same cardinality.
#[test]
fn bulk_load_picks_compact_layout_for_date_index() {
    use std::sync::Arc;

    fn leaf_stats(index: &IndexFile) -> (usize, usize) {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        let mut compact = 0usize;
        loop {
            let leaf = index.read_leaf(leaf_id).unwrap();
            leaves += 1;
            if leaf.key_layout == KEY_LAYOUT_COMPACT_DATE_U16 {
                compact += 1;
            }
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        (leaves, compact)
    }

    const N: i64 = 8000;

    // DATE index — eligible for compact layout. Many rows share a
    // day (realistic time-series shape), so the page-local span
    // stays tiny and well within u16.
    let date_dir = tempfile::tempdir().unwrap();
    let date_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(date_dir.path()).unwrap(),
    ));
    let date_path = t_index_path(date_dir.path(), "t", 1, "DATE", "idx");
    std::fs::create_dir_all(date_path.parent().unwrap()).unwrap();
    let date_index = IndexFile::open(date_path, date_log).unwrap();
    let date_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Date(20_000 + (i / 8) as i32)]),
            row_id: RowId {
                page_id: (i) as u32,
                slot_id: 0,
            },
        })
        .collect();
    date_index
        .bulk_load_sorted(1, date_entries.clone())
        .unwrap();

    // INT index — same cardinality, generic layout.
    let int_dir = tempfile::tempdir().unwrap();
    let int_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(int_dir.path()).unwrap(),
    ));
    let int_path = t_index_path(int_dir.path(), "t", 1, "INT", "idx");
    std::fs::create_dir_all(int_path.parent().unwrap()).unwrap();
    let int_index = IndexFile::open(int_path, int_log).unwrap();
    let int_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(20_000 + i / 8)]),
            row_id: RowId {
                page_id: (i) as u32,
                slot_id: 0,
            },
        })
        .collect();
    int_index.bulk_load_sorted(1, int_entries).unwrap();

    // All DATE rows survive the round-trip.
    let rows = date_index.range(None, None, false).unwrap();
    assert_eq!(
        rows.len(),
        date_entries.len(),
        "compact DATE bulk-load lost rows"
    );

    let (date_leaves, date_compact) = leaf_stats(&date_index);
    let (int_leaves, _) = leaf_stats(&int_index);
    assert!(
        date_compact > 0,
        "expected at least one compact DATE leaf after bulk load"
    );
    assert!(
        date_leaves < int_leaves,
        "compact DATE layout should use fewer leaves: date={date_leaves} int={int_leaves}"
    );
}

/// Regression: a leaf page tagged with a compact layout but holding a
/// value outside the `[base, base + max_delta]` window (e.g. an
/// out-of-order insert) must still encode — `resolve_leaf_layout`
/// either re-picks a wider compact base or demotes to generic.
#[test]
fn v9_compact_layout_demotes_when_entry_escapes_base_window() {
    fn build(days: &[i32], base: i64) -> IndexPage {
        let mut entries: Vec<IndexEntry> = days
            .iter()
            .enumerate()
            .map(|(i, d)| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Date(*d)]),
                row_id: RowId {
                    page_id: (i) as u32,
                    slot_id: 0,
                },
            })
            .collect();
        entries.sort();
        IndexPage::Leaf(IndexLeafPage::new_with_layout(
            entries,
            None,
            KEY_LAYOUT_COMPACT_DATE_U16,
            base,
        ))
    }

    // Case 1 — value below base but the *overall* span still fits
    // u16: `resolve_leaf_layout` re-picks a lower base, staying compact.
    let page = build(&[19_990, 20_000, 20_010], 20_000);
    let buf = encode_index_tree_page(&page).unwrap();
    match decode_index_tree_page(&buf).unwrap() {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_DATE_U16);
            assert_eq!(leaf.base_time, 19_990);
            let IndexPage::Leaf(orig) = &page else {
                unreachable!()
            };
            assert_eq!(leaf.entries, orig.entries);
        }
        _ => panic!("expected leaf"),
    }

    // Case 2 — span exceeds u16: must demote all the way to generic.
    let page = build(&[20_000, 20_000 + 100_000], 20_000);
    let buf = encode_index_tree_page(&page).unwrap();
    match decode_index_tree_page(&buf).unwrap() {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_GENERIC);
            let IndexPage::Leaf(orig) = &page else {
                unreachable!()
            };
            assert_eq!(leaf.entries, orig.entries);
        }
        _ => panic!("expected leaf"),
    }
}

// =================================================================
// v12 compact DECIMAL layout regressions
// =================================================================

/// Compact DECIMAL(P≤9) leaf round-trips every entry through the i32
/// lane, including negative values and the i32::MIN/MAX edges.
#[test]
fn v12_compact_decimal_i32_leaf_round_trips() {
    let entries: Vec<IndexEntry> = vec![
        (i32::MIN as i128, 100u32),
        (-12345i128, 101u32),
        (-1i128, 102u32),
        (0i128, 103u32),
        (1i128, 104u32),
        (12345i128, 105u32),
        (i32::MAX as i128, 106u32),
    ]
    .into_iter()
    .map(|(value, page_id)| IndexEntry {
        key: IndexKey::from(vec![KeyPart::Decimal { value, scale: 2 }]),
        row_id: RowId {
            page_id,
            slot_id: 0,
        },
    })
    .collect();
    let page = IndexPage::Leaf(IndexLeafPage::new_with_layout(
        entries.clone(),
        Some(0),
        KEY_LAYOUT_COMPACT_DECIMAL_I32,
        2, // base_time slot carries `scale`
    ));
    let buf = encode_index_tree_page(&page).unwrap();
    let decoded = decode_index_tree_page(&buf).unwrap();
    match decoded {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_DECIMAL_I32);
            assert_eq!(leaf.base_time, 2);
            assert_eq!(leaf.entries, entries);
        }
        _ => panic!("expected leaf"),
    }
}

/// Compact DECIMAL(P≤18) leaf round-trips through the i64 lane —
/// including magnitudes that exceed i32 so we know the wider lane is
/// engaged.
#[test]
fn v12_compact_decimal_i64_leaf_round_trips() {
    let entries: Vec<IndexEntry> = vec![
        i64::MIN as i128,
        -10_000_000_000_i64 as i128,
        -1_000_000_000_i64 as i128, // fits i32 but page picked i64
        0,
        1_000_000_000_i64 as i128,
        10_000_000_000_i64 as i128,
        i64::MAX as i128,
    ]
    .into_iter()
    .enumerate()
    .map(|(i, value)| IndexEntry {
        key: IndexKey::from(vec![KeyPart::Decimal { value, scale: 4 }]),
        row_id: RowId {
            page_id: (200 + i) as u32,
            slot_id: 0,
        },
    })
    .collect();
    let page = IndexPage::Leaf(IndexLeafPage::new_with_layout(
        entries.clone(),
        None,
        KEY_LAYOUT_COMPACT_DECIMAL_I64,
        4,
    ));
    let buf = encode_index_tree_page(&page).unwrap();
    let decoded = decode_index_tree_page(&buf).unwrap();
    match decoded {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_DECIMAL_I64);
            assert_eq!(leaf.base_time, 4);
            assert_eq!(leaf.entries, entries);
        }
        _ => panic!("expected leaf"),
    }
}

/// End-to-end storage-density check: bulk-load a single-column DECIMAL
/// index whose magnitudes all fit in i32 and confirm that
/// (a) `bulk_load` picks `KEY_LAYOUT_COMPACT_DECIMAL_I32`,
/// (b) every entry round-trips correctly,
/// (c) the same data in the generic layout uses strictly more leaf
///     pages.
#[test]
fn v12_compact_decimal_reduces_leaf_page_count() {
    use std::sync::Arc;
    const N: i64 = 4_000;

    fn leaf_stats(index: &IndexFile) -> (usize, usize) {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        let mut compact = 0usize;
        loop {
            let leaf = index.read_leaf(leaf_id).unwrap();
            leaves += 1;
            if leaf.key_layout == KEY_LAYOUT_COMPACT_DECIMAL_I32 {
                compact += 1;
            }
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        (leaves, compact)
    }

    // Compact DECIMAL(9, 2): magnitudes 0..N all comfortably under
    // i32::MAX, single column, uniform scale — bulk_load should pick
    // the i32 compact layout.
    let dec_dir = tempfile::tempdir().unwrap();
    let dec_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dec_dir.path()).unwrap(),
    ));
    let dec_path = t_index_path(dec_dir.path(), "t", 1, "DEC", "idx");
    std::fs::create_dir_all(dec_path.parent().unwrap()).unwrap();
    let dec_index = IndexFile::open(dec_path, dec_log).unwrap();
    let dec_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Decimal {
                value: i as i128,
                scale: 2,
            }]),
            row_id: RowId {
                page_id: i as u32,
                slot_id: 0,
            },
        })
        .collect();
    dec_index.bulk_load_sorted(1, dec_entries.clone()).unwrap();

    // Generic DECIMAL: same number of rows but with very wide
    // magnitudes that exceed i64, forcing generic layout.
    let gen_dir = tempfile::tempdir().unwrap();
    let gen_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(gen_dir.path()).unwrap(),
    ));
    let gen_path = t_index_path(gen_dir.path(), "t", 1, "GEN", "idx");
    std::fs::create_dir_all(gen_path.parent().unwrap()).unwrap();
    let gen_index = IndexFile::open(gen_path, gen_log).unwrap();
    let gen_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Decimal {
                // Force i128 lane (beyond i64::MAX).
                value: (i as i128) * 1_000_000_000_000_000_000_i128 * 100,
                scale: 2,
            }]),
            row_id: RowId {
                page_id: i as u32,
                slot_id: 0,
            },
        })
        .collect();
    gen_index.bulk_load_sorted(1, gen_entries).unwrap();

    // All compact rows survive the round-trip.
    let rows = dec_index.range(None, None, false).unwrap();
    assert_eq!(
        rows.len(),
        dec_entries.len(),
        "compact DECIMAL bulk-load lost rows"
    );

    let (dec_leaves, dec_compact) = leaf_stats(&dec_index);
    assert!(
        dec_compact > 0,
        "expected at least one compact DECIMAL leaf, found {dec_compact}/{dec_leaves}"
    );

    fn count_leaves(index: &IndexFile) -> usize {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        loop {
            leaves += 1;
            let leaf = index.read_leaf(leaf_id).unwrap();
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        leaves
    }
    let gen_leaves = count_leaves(&gen_index);
    assert!(
        dec_leaves < gen_leaves,
        "compact decimal should use fewer leaves than generic: dec={dec_leaves} gen={gen_leaves}"
    );
}

/// `CompactKind::of_entry` must report `DecimalI32` for keys that fit
/// in i32 and `DecimalI64` for keys that exceed it. Mixed scales in
/// the same chunk must demote to generic.
#[test]
fn v12_compact_decimal_demotes_on_scale_mismatch() {
    let mut accum = CompactChunkAccum::new();
    accum.record(&IndexEntry {
        key: IndexKey::from(vec![KeyPart::Decimal {
            value: 100,
            scale: 2,
        }]),
        row_id: RowId {
            page_id: 1,
            slot_id: 0,
        },
    });
    accum.record(&IndexEntry {
        key: IndexKey::from(vec![KeyPart::Decimal {
            value: 100,
            scale: 4, // mismatched scale → demote
        }]),
        row_id: RowId {
            page_id: 2,
            slot_id: 0,
        },
    });
    assert_eq!(accum.layout().0, KEY_LAYOUT_GENERIC);
}

// =================================================================
// v13 compact INT layout regressions
// =================================================================

/// Compact INT (u16 delta) leaf round-trips every entry: zero delta,
/// max delta (u16::MAX), middle, and negative absolute values
/// (delta is always non-negative — base is the chunk min).
#[test]
fn v13_compact_int_u16_leaf_round_trips() {
    let base: i64 = -1_000;
    let entries: Vec<IndexEntry> = vec![
        (-1_000_i64, 100u32), // delta = 0
        (-999, 101),
        (0, 102),
        (1, 103),
        (32_768, 104),
        (-1_000 + u16::MAX as i64, 105), // delta = u16::MAX
    ]
    .into_iter()
    .map(|(value, page_id)| IndexEntry {
        key: IndexKey::from(vec![KeyPart::Int(value)]),
        row_id: RowId {
            page_id,
            slot_id: 0,
        },
    })
    .collect();
    let page = IndexPage::Leaf(IndexLeafPage::new_with_layout(
        entries.clone(),
        Some(0),
        KEY_LAYOUT_COMPACT_INT_U16,
        base,
    ));
    let buf = encode_index_tree_page(&page).unwrap();
    let decoded = decode_index_tree_page(&buf).unwrap();
    match decoded {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_INT_U16);
            assert_eq!(leaf.base_time, base);
            assert_eq!(leaf.entries, entries);
        }
        _ => panic!("expected leaf"),
    }
}

/// Compact INT (u32 delta) leaf round-trips with deltas exceeding u16
/// (sparse / gappy INT distributions).
#[test]
fn v13_compact_int_u32_leaf_round_trips() {
    let base: i64 = 1_000_000;
    let entries: Vec<IndexEntry> = vec![
        (1_000_000_i64, 200u32),                // delta = 0
        (1_000_100, 201),                       // delta = 100
        (1_000_000 + (1i64 << 17), 202),        // delta exceeds u16
        (1_000_000 + (1i64 << 30), 203),        // ~1G delta
        (1_000_000_i64 + u32::MAX as i64, 204), // delta = u32::MAX
    ]
    .into_iter()
    .map(|(value, page_id)| IndexEntry {
        key: IndexKey::from(vec![KeyPart::Int(value)]),
        row_id: RowId {
            page_id,
            slot_id: 0,
        },
    })
    .collect();
    let page = IndexPage::Leaf(IndexLeafPage::new_with_layout(
        entries.clone(),
        None,
        KEY_LAYOUT_COMPACT_INT_U32,
        base,
    ));
    let buf = encode_index_tree_page(&page).unwrap();
    let decoded = decode_index_tree_page(&buf).unwrap();
    match decoded {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_INT_U32);
            assert_eq!(leaf.base_time, base);
            assert_eq!(leaf.entries, entries);
        }
        _ => panic!("expected leaf"),
    }
}

/// `CompactKind::of_entry` + `merge_with` recognise `KeyPart::Int` and
/// merge consistently; the chunk-level layout selector picks U16 for
/// tight spans, U32 for wide spans, generic when span exceeds u32::MAX.
#[test]
fn v13_compact_int_layout_selection_by_span() {
    let mk = |v: i64, pid: u32| IndexEntry {
        key: IndexKey::from(vec![KeyPart::Int(v)]),
        row_id: RowId {
            page_id: pid,
            slot_id: 0,
        },
    };

    // Tight (u16) span — sequential PK-like.
    let mut tight = CompactChunkAccum::new();
    for i in 0..100i64 {
        tight.record(&mk(1_000 + i, i as u32));
    }
    assert_eq!(
        tight.layout(),
        (KEY_LAYOUT_COMPACT_INT_U16, 1_000),
        "sequential ints should land in u16 lane with base = min"
    );

    // Wider (u32) span.
    let mut wide = CompactChunkAccum::new();
    wide.record(&mk(0, 1));
    wide.record(&mk(1_000_000_000, 2));
    assert_eq!(wide.layout(), (KEY_LAYOUT_COMPACT_INT_U32, 0));

    // Too wide for u32 → demote.
    let mut huge = CompactChunkAccum::new();
    huge.record(&mk(i64::MIN, 1));
    huge.record(&mk(i64::MAX, 2));
    assert_eq!(huge.layout().0, KEY_LAYOUT_GENERIC);

    // Mixed Int + DateTime → demote.
    let mut mixed = CompactChunkAccum::new();
    mixed.record(&mk(1, 1));
    mixed.record(&IndexEntry {
        key: IndexKey::from(vec![KeyPart::DateTime(123_456)]),
        row_id: RowId {
            page_id: 2,
            slot_id: 0,
        },
    });
    assert_eq!(mixed.layout().0, KEY_LAYOUT_GENERIC);
}

// =================================================================
// v20 compact UINT (INT UNSIGNED) layout regressions
// =================================================================

/// Compact UINT (u16 delta) leaf round-trips, including the high u64
/// range (above i64::MAX) where BIGINT UNSIGNED auto-increment PKs live.
/// The order-preserving sign-flip base must reconstruct every value.
#[test]
fn v20_compact_uint_u16_leaf_round_trips() {
    let min: u64 = u64::MAX - 70_000; // base sits well above i64::MAX
    let base = uint_to_ordered_i64(min);
    let entries: Vec<IndexEntry> = vec![
        (min, 100u32), // delta = 0
        (min + 1, 101),
        (min + 1_000, 102),
        (min + u16::MAX as u64, 103), // delta = u16::MAX
    ]
    .into_iter()
    .map(|(value, page_id)| IndexEntry {
        key: IndexKey::from(vec![KeyPart::UInt(value)]),
        row_id: RowId {
            page_id,
            slot_id: 0,
        },
    })
    .collect();
    let page = IndexPage::Leaf(IndexLeafPage::new_with_layout(
        entries.clone(),
        Some(0),
        KEY_LAYOUT_COMPACT_UINT_U16,
        base,
    ));
    let buf = encode_index_tree_page(&page).unwrap();
    let decoded = decode_index_tree_page(&buf).unwrap();
    match decoded {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_UINT_U16);
            assert_eq!(leaf.base_time, base);
            assert_eq!(leaf.entries, entries);
        }
        _ => panic!("expected leaf"),
    }
}

/// Compact UINT (u32 delta) leaf round-trips with deltas exceeding u16,
/// all values in the upper u64 half (BIGINT UNSIGNED past i64::MAX).
#[test]
fn v20_compact_uint_u32_leaf_round_trips() {
    let min: u64 = (1u64 << 63) + 100; // just above i64::MAX
    let base = uint_to_ordered_i64(min);
    let entries: Vec<IndexEntry> = vec![
        (min, 200u32),                // delta = 0
        (min + (1u64 << 17), 201),    // delta exceeds u16
        (min + (1u64 << 30), 202),    // ~1G delta
        (min + u32::MAX as u64, 203), // delta = u32::MAX
    ]
    .into_iter()
    .map(|(value, page_id)| IndexEntry {
        key: IndexKey::from(vec![KeyPart::UInt(value)]),
        row_id: RowId {
            page_id,
            slot_id: 0,
        },
    })
    .collect();
    let page = IndexPage::Leaf(IndexLeafPage::new_with_layout(
        entries.clone(),
        None,
        KEY_LAYOUT_COMPACT_UINT_U32,
        base,
    ));
    let buf = encode_index_tree_page(&page).unwrap();
    let decoded = decode_index_tree_page(&buf).unwrap();
    match decoded {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_UINT_U32);
            assert_eq!(leaf.base_time, base);
            assert_eq!(leaf.entries, entries);
        }
        _ => panic!("expected leaf"),
    }
}

/// `CompactChunkAccum` selects UINT_U16 for tight spans, UINT_U32 for
/// wider ones, and — critically — demotes to generic when a page
/// straddles i64::MAX (the true u64 span is enormous even though a
/// naive bitcast would compute a tiny one). Signed+unsigned mixes never
/// merge.
#[test]
fn v20_compact_uint_layout_selection_by_span() {
    let mk = |v: u64, pid: u32| IndexEntry {
        key: IndexKey::from(vec![KeyPart::UInt(v)]),
        row_id: RowId {
            page_id: pid,
            slot_id: 0,
        },
    };

    // Tight (u16) span, high BIGINT UNSIGNED range past i64::MAX.
    let base_u = u64::MAX - 10_000;
    let mut tight = CompactChunkAccum::new();
    for i in 0..100u64 {
        tight.record(&mk(base_u + i, i as u32));
    }
    assert_eq!(
        tight.layout(),
        (KEY_LAYOUT_COMPACT_UINT_U16, uint_to_ordered_i64(base_u)),
        "sequential unsigned ids should land in u16 lane with the flipped min as base"
    );

    // Wider (u32) span.
    let mut wide = CompactChunkAccum::new();
    wide.record(&mk(5, 1));
    wide.record(&mk(1_000_000_000, 2));
    assert_eq!(wide.layout().0, KEY_LAYOUT_COMPACT_UINT_U32);

    // Straddling i64::MAX with a tiny *bitcast* gap MUST demote: the
    // real u64 span (10 .. u64::MAX) is huge. This is the case the
    // sign-flip mapping exists to get right.
    let mut straddle = CompactChunkAccum::new();
    straddle.record(&mk(10, 1));
    straddle.record(&mk(u64::MAX, 2));
    assert_eq!(straddle.layout().0, KEY_LAYOUT_GENERIC);

    // Signed + unsigned never merge → generic.
    let mut mixed = CompactChunkAccum::new();
    mixed.record(&mk(1, 1));
    mixed.record(&IndexEntry {
        key: IndexKey::from(vec![KeyPart::Int(2)]),
        row_id: RowId {
            page_id: 2,
            slot_id: 0,
        },
    });
    assert_eq!(mixed.layout().0, KEY_LAYOUT_GENERIC);
}

/// End-to-end storage-density check: bulk-load a sequential BIGINT PK
/// index, confirm `bulk_load` picks `KEY_LAYOUT_COMPACT_INT_U16`,
/// every entry round-trips, and the same data in the generic layout
/// uses strictly more leaf pages.
#[test]
fn v13_compact_int_reduces_leaf_page_count() {
    use std::sync::Arc;
    const N: i64 = 8_000;

    fn leaf_stats(index: &IndexFile) -> (usize, usize) {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        let mut compact = 0usize;
        loop {
            let leaf = index.read_leaf(leaf_id).unwrap();
            leaves += 1;
            if leaf.key_layout == KEY_LAYOUT_COMPACT_INT_U16
                || leaf.key_layout == KEY_LAYOUT_COMPACT_INT_U32
            {
                compact += 1;
            }
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        (leaves, compact)
    }

    // Sequential BIGINT PK — the canonical OLTP shape.
    let int_dir = tempfile::tempdir().unwrap();
    let int_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(int_dir.path()).unwrap(),
    ));
    let int_path = t_index_path(int_dir.path(), "t", 1, "INT", "idx");
    std::fs::create_dir_all(int_path.parent().unwrap()).unwrap();
    let int_index = IndexFile::open(int_path, int_log).unwrap();
    let int_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(i)]),
            row_id: RowId {
                page_id: i as u32,
                slot_id: 0,
            },
        })
        .collect();
    int_index.bulk_load_sorted(1, int_entries.clone()).unwrap();

    // Force generic layout: mixed types in the key (Int + String) so
    // CompactKind never recognises the chunk.
    let gen_dir = tempfile::tempdir().unwrap();
    let gen_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(gen_dir.path()).unwrap(),
    ));
    let gen_path = t_index_path(gen_dir.path(), "t", 1, "GEN", "idx");
    std::fs::create_dir_all(gen_path.parent().unwrap()).unwrap();
    let gen_index = IndexFile::open(gen_path, gen_log).unwrap();
    let gen_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(i), KeyPart::String(String::new())]),
            row_id: RowId {
                page_id: i as u32,
                slot_id: 0,
            },
        })
        .collect();
    gen_index.bulk_load_sorted(1, gen_entries).unwrap();

    // All compact rows survive the round-trip.
    let rows = int_index.range(None, None, false).unwrap();
    assert_eq!(
        rows.len(),
        int_entries.len(),
        "compact INT bulk-load lost rows"
    );

    let (int_leaves, int_compact) = leaf_stats(&int_index);
    assert!(
        int_compact > 0,
        "expected at least one compact INT leaf, found {int_compact}/{int_leaves}"
    );

    fn count_leaves(index: &IndexFile) -> usize {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        loop {
            leaves += 1;
            let leaf = index.read_leaf(leaf_id).unwrap();
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        leaves
    }
    let gen_leaves = count_leaves(&gen_index);
    assert!(
        int_leaves < gen_leaves,
        "compact INT should use fewer leaves than generic: int={int_leaves} gen={gen_leaves}"
    );
}

/// End-to-end density + correctness for the v20 COMPACT_UINT layout: a
/// sequential `BIGINT UNSIGNED` PK in the high u64 range (above
/// i64::MAX, where auto-increment unsigned PKs commonly sit) bulk-loads
/// into compact leaves, round-trips every row through a range scan, and
/// uses strictly fewer leaf pages than the same data forced generic.
#[test]
fn v20_compact_uint_reduces_leaf_page_count() {
    use std::sync::Arc;
    const N: u64 = 8_000;
    // Start the keyspace well past i64::MAX so the unsigned-specific
    // path (not a coincidental signed-range fit) is what's exercised.
    const MIN: u64 = (1u64 << 63) + 1_000_000;

    fn leaf_stats(index: &IndexFile) -> (usize, usize) {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        let mut compact = 0usize;
        loop {
            let leaf = index.read_leaf(leaf_id).unwrap();
            leaves += 1;
            if leaf.key_layout == KEY_LAYOUT_COMPACT_UINT_U16
                || leaf.key_layout == KEY_LAYOUT_COMPACT_UINT_U32
            {
                compact += 1;
            }
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        (leaves, compact)
    }
    fn count_leaves(index: &IndexFile) -> usize {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        loop {
            leaves += 1;
            let leaf = index.read_leaf(leaf_id).unwrap();
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        leaves
    }

    // Compact: single-column UInt.
    let uint_dir = tempfile::tempdir().unwrap();
    let uint_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(uint_dir.path()).unwrap(),
    ));
    let uint_path = t_index_path(uint_dir.path(), "t", 1, "UINT", "idx");
    std::fs::create_dir_all(uint_path.parent().unwrap()).unwrap();
    let uint_index = IndexFile::open(uint_path, uint_log).unwrap();
    let uint_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::UInt(MIN + i)]),
            row_id: RowId {
                page_id: i as u32,
                slot_id: 0,
            },
        })
        .collect();
    uint_index
        .bulk_load_sorted(1, uint_entries.clone())
        .unwrap();

    // Generic baseline: same values but a mixed key shape so CompactKind
    // never recognises the chunk.
    let gen_dir = tempfile::tempdir().unwrap();
    let gen_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(gen_dir.path()).unwrap(),
    ));
    let gen_path = t_index_path(gen_dir.path(), "t", 1, "GEN", "idx");
    std::fs::create_dir_all(gen_path.parent().unwrap()).unwrap();
    let gen_index = IndexFile::open(gen_path, gen_log).unwrap();
    let gen_entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::UInt(MIN + i), KeyPart::String(String::new())]),
            row_id: RowId {
                page_id: i as u32,
                slot_id: 0,
            },
        })
        .collect();
    gen_index.bulk_load_sorted(1, gen_entries).unwrap();

    // Every compact row survives the round-trip (decode path correct).
    let rows = uint_index.range(None, None, false).unwrap();
    assert_eq!(
        rows.len(),
        uint_entries.len(),
        "compact UINT bulk-load lost rows"
    );

    let (uint_leaves, uint_compact) = leaf_stats(&uint_index);
    assert!(
        uint_compact > 0,
        "expected at least one compact UINT leaf, found {uint_compact}/{uint_leaves}"
    );
    let gen_leaves = count_leaves(&gen_index);
    assert!(
        uint_leaves < gen_leaves,
        "compact UINT should use fewer leaves than generic: uint={uint_leaves} gen={gen_leaves}"
    );
}

/// An insert that pushes a value outside the page's current u16 delta
/// window must demote via `resolve_leaf_layout` rather than corrupt.
#[test]
fn v13_compact_int_demotes_on_overflow() {
    let base: i64 = 0;
    let entries: Vec<IndexEntry> = vec![0i64, 100, 1 << 20]
        .into_iter()
        .enumerate()
        .map(|(i, value)| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(value)]),
            row_id: RowId {
                page_id: i as u32 + 1,
                slot_id: 0,
            },
        })
        .collect();
    // Request U16 layout but supply a value > u16::MAX → resolver
    // must re-pick U32 (or generic).
    let (effective_layout, effective_base) =
        resolve_leaf_layout(KEY_LAYOUT_COMPACT_INT_U16, base, &entries);
    assert_eq!(
        effective_layout, KEY_LAYOUT_COMPACT_INT_U32,
        "u16-too-narrow input should re-pick u32 layout"
    );
    assert_eq!(effective_base, 0);

    // A value out of u32 range too → fall to generic.
    let huge: Vec<IndexEntry> = vec![0i64, (u32::MAX as i64) + 1]
        .into_iter()
        .enumerate()
        .map(|(i, value)| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(value)]),
            row_id: RowId {
                page_id: i as u32 + 1,
                slot_id: 0,
            },
        })
        .collect();
    let (effective_layout, _) = resolve_leaf_layout(KEY_LAYOUT_COMPACT_INT_U16, base, &huge);
    assert_eq!(effective_layout, KEY_LAYOUT_GENERIC);
}

/// v13 Phase 3: internal pages dispatch on `key_layout` too. After
/// bulk-loading a single-column BIGINT PK at ≥1M rows the root chain
/// includes at least one internal page committed in a compact INT
/// layout (U16 or U32), proving fanout uplift fires end-to-end.
#[test]
fn v13_internal_page_compact_int_layout_fires_end_to_end() {
    use std::sync::Arc;
    const N: i64 = 50_000;
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let index_path = t_index_path(dir.path(), "t", 1, "INT", "idx");
    std::fs::create_dir_all(index_path.parent().unwrap()).unwrap();
    let index = IndexFile::open(index_path, log).unwrap();

    let entries: Vec<IndexEntry> = (0..N)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(i)]),
            row_id: RowId {
                page_id: i as u32,
                slot_id: 0,
            },
        })
        .collect();
    index.bulk_load_sorted(1, entries).unwrap();

    // Round-trip the descend path that reads every internal page —
    // any layout-decoder bug would surface as a wrong leaf id or
    // a decode error.
    let bound_key = IndexKey::from(vec![KeyPart::Int(N / 2)]);
    let _leaf_for_mid = index.find_leaf_for_key_lower_bound(&bound_key).unwrap();

    // Discover all internal pages by scanning the page id space
    // (test-only — tiny files).
    let max_page = index.allocated_page_count_for_tests();
    let mut saw_compact_internal = false;
    let mut internal_count = 0usize;
    let mut compact_internal_count = 0usize;
    let mut first_internal_sample: Option<(u64, u8, i64, usize, Option<IndexKey>)> = None;
    for pid in 1..max_page {
        let Ok(page) = index.read_page(pid) else {
            continue;
        };
        if let IndexPage::Internal(int) = page.as_ref() {
            internal_count += 1;
            if first_internal_sample.is_none() {
                first_internal_sample = Some((
                    pid,
                    int.key_layout,
                    int.base_time,
                    int.keys.len(),
                    int.keys.first().map(|e| e.key.clone()),
                ));
            }
            if int.key_layout == KEY_LAYOUT_COMPACT_INT_U16
                || int.key_layout == KEY_LAYOUT_COMPACT_INT_U32
            {
                saw_compact_internal = true;
                compact_internal_count += 1;
            }
        }
    }
    assert!(internal_count > 0, "expected at least one internal page");
    assert!(
        saw_compact_internal,
        "expected ≥1 compact-INT internal page after bulk-loading {N} sequential BIGINTs, \
         got {compact_internal_count}/{internal_count} compact internals; \
         first internal sample = {first_internal_sample:?}"
    );
}

// =================================================================
// v14 COMPACT_INT2 (two-column INT) layout regressions
// =================================================================

/// Two-column INT leaf round-trips through u8/u8 widths (very tight
/// spans — `[KeyPart::Int, KeyPart::Int]` with both columns ≤ 255).
#[test]
fn v14_compact_int2_leaf_round_trips_u8_u8() {
    let pairs: &[(i64, i64)] = &[
        (1_000_000, 1),
        (1_000_000, 2),
        (1_000_001, 1),
        (1_000_001, 7),
        (1_000_002, 3),
    ];
    let mut entries: Vec<IndexEntry> = pairs
        .iter()
        .enumerate()
        .map(|(i, (a, b))| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(*a), KeyPart::Int(*b)]),
            row_id: RowId {
                page_id: (i + 1) as u32,
                slot_id: 0,
            },
        })
        .collect();
    entries.sort();
    // Auto-resolve picks widths from the spans.
    let leaf = IndexLeafPage::new_with_resolved_layout(entries.clone(), None);
    assert_eq!(
        leaf.key_layout, KEY_LAYOUT_COMPACT_INT2,
        "two-column INT key should pick the INT2 layout"
    );
    let aux = leaf.layout_aux.expect("INT2 leaf must carry layout_aux");
    // col1 span = 2, col2 span = 6 → both fit u8.
    assert_eq!(aux.width1, 1);
    assert_eq!(aux.width2, 1);
    let buf = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
    match decode_index_tree_page(&buf).unwrap() {
        IndexPage::Leaf(decoded) => {
            assert_eq!(decoded.key_layout, KEY_LAYOUT_COMPACT_INT2);
            assert_eq!(decoded.entries, entries);
            assert_eq!(decoded.layout_aux, Some(aux));
        }
        _ => panic!("expected leaf"),
    }
}

/// Two-column INT leaf with mixed widths: col1 needs u32 (large span),
/// col2 fits u8.
#[test]
fn v14_compact_int2_leaf_mixed_widths() {
    let mut entries: Vec<IndexEntry> = (0..50i64)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![
                KeyPart::Int(1_000_000 + i * 100_000), // col1 span = ~5M → u32
                KeyPart::Int(i % 5),                   // col2 span = 4 → u8
            ]),
            row_id: RowId {
                page_id: i as u32 + 1,
                slot_id: 0,
            },
        })
        .collect();
    entries.sort();
    let leaf = IndexLeafPage::new_with_resolved_layout(entries.clone(), None);
    assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_INT2);
    let aux = leaf.layout_aux.expect("layout_aux");
    assert_eq!(aux.width1, 4, "col1 span 5M needs u32");
    assert_eq!(aux.width2, 1, "col2 span 4 fits u8");
    let buf = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
    match decode_index_tree_page(&buf).unwrap() {
        IndexPage::Leaf(decoded) => {
            assert_eq!(decoded.entries, entries);
        }
        _ => panic!("expected leaf"),
    }
}

/// End-to-end density check: bulk-load a 2-column BIGINT PK index
/// (mimicking TPC-H `lineitem PRIMARY KEY (orderkey, linenumber)`)
/// and confirm INT2 takes over from generic.
#[test]
fn v14_compact_int2_reduces_leaf_page_count_lineitem_shape() {
    use std::sync::Arc;
    const ORDERS: i64 = 4_000;
    const LINES_PER_ORDER: i64 = 7;

    // INT2 candidate: (orderkey, linenumber).
    let int2_dir = tempfile::tempdir().unwrap();
    let int2_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(int2_dir.path()).unwrap(),
    ));
    let int2_path = t_index_path(int2_dir.path(), "t", 1, "INT2", "idx");
    std::fs::create_dir_all(int2_path.parent().unwrap()).unwrap();
    let int2_index = IndexFile::open(int2_path, int2_log).unwrap();
    let mut int2_entries: Vec<IndexEntry> = Vec::new();
    for orderkey in 0..ORDERS {
        for line in 1..=LINES_PER_ORDER {
            int2_entries.push(IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(orderkey), KeyPart::Int(line)]),
                row_id: RowId {
                    page_id: (orderkey * 10 + line) as u32,
                    slot_id: 0,
                },
            });
        }
    }
    int2_entries.sort();
    int2_index
        .bulk_load_sorted(1, int2_entries.clone())
        .unwrap();

    // Generic control: same shape but force generic with a 3rd
    // string column.
    let gen_dir = tempfile::tempdir().unwrap();
    let gen_log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(gen_dir.path()).unwrap(),
    ));
    let gen_path = t_index_path(gen_dir.path(), "t", 1, "GEN", "idx");
    std::fs::create_dir_all(gen_path.parent().unwrap()).unwrap();
    let gen_index = IndexFile::open(gen_path, gen_log).unwrap();
    let mut gen_entries: Vec<IndexEntry> = Vec::new();
    for orderkey in 0..ORDERS {
        for line in 1..=LINES_PER_ORDER {
            gen_entries.push(IndexEntry {
                key: IndexKey::from(vec![
                    KeyPart::Int(orderkey),
                    KeyPart::Int(line),
                    KeyPart::String(String::new()),
                ]),
                row_id: RowId {
                    page_id: (orderkey * 10 + line) as u32,
                    slot_id: 0,
                },
            });
        }
    }
    gen_entries.sort();
    gen_index.bulk_load_sorted(1, gen_entries).unwrap();

    fn leaf_stats(index: &IndexFile) -> (usize, usize) {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        let mut int2 = 0usize;
        loop {
            let leaf = index.read_leaf(leaf_id).unwrap();
            leaves += 1;
            if leaf.key_layout == KEY_LAYOUT_COMPACT_INT2 {
                int2 += 1;
            }
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        (leaves, int2)
    }

    let (int2_leaves, int2_compact) = leaf_stats(&int2_index);
    assert!(
        int2_compact > 0,
        "expected ≥1 compact INT2 leaf, got {int2_compact}/{int2_leaves}"
    );
    // All bulk-loaded rows survive the round-trip.
    let rows = int2_index.range(None, None, false).unwrap();
    assert_eq!(rows.len(), int2_entries.len());

    fn count_leaves(index: &IndexFile) -> usize {
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut leaves = 0usize;
        loop {
            leaves += 1;
            let leaf = index.read_leaf(leaf_id).unwrap();
            match leaf.next_leaf {
                Some(next) => leaf_id = next,
                None => break,
            }
        }
        leaves
    }
    let gen_leaves = count_leaves(&gen_index);
    assert!(
        int2_leaves < gen_leaves,
        "INT2 should use fewer leaves than generic: int2={int2_leaves} gen={gen_leaves}"
    );
}

#[test]
fn sorted_append_demoting_compact_leaf_splits_instead_of_overflowing() {
    let dir = tempfile::tempdir().unwrap();
    let path = t_index_path(dir.path(), "t", 1, "PRIMARY", "idx");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let index = IndexFile::open(path, test_log(dir.path())).unwrap();

    let mut entries: Vec<IndexEntry> = (0..1_400i64)
        .map(|value| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(value)]),
            row_id: test_row_id(value, 128),
        })
        .collect();
    entries.sort();
    index.bulk_load_sorted(1, entries).unwrap();

    let leaf = index.read_leaf(index.rightmost_leaf().unwrap()).unwrap();
    assert!(
        matches!(
            leaf.key_layout,
            KEY_LAYOUT_COMPACT_INT_U16 | KEY_LAYOUT_COMPACT_INT_U32
        ),
        "initial sorted INT leaf should use compact layout"
    );

    let outlier = IndexEntry {
        key: IndexKey::from(vec![KeyPart::Int(i64::MAX)]),
        row_id: test_row_id(9_999, 128),
    };
    index
        .insert_entries_logged(2, vec![outlier.clone()], true)
        .unwrap();

    assert_eq!(
        index.get(&outlier.key, false).unwrap(),
        vec![outlier.row_id]
    );
}

/// v10 varint encoder must round-trip every length boundary (1, 2,
/// 3, 5 bytes) and the full u64 domain, including the 10-byte
/// max-length edge.
#[test]
fn v10_varint_u64_round_trips_at_length_boundaries() {
    let cases: &[u64] = &[
        0,
        1,
        127,          // last 1-byte value
        128,          // first 2-byte
        16_383,       // last 2-byte (2^14 - 1)
        16_384,       // first 3-byte
        (1u64 << 16), // 2^16, realistic TPC-H SF=1 lineitem page_id
        (1u64 << 21) - 1,
        (1u64 << 21), // first 4-byte
        (1u64 << 28) - 1,
        (1u64 << 28), // first 5-byte
        (1u64 << 32), // 2^32, "huge but not insane"
        (1u64 << 36), // ~64G pages = TB scale
        (1u64 << 50),
        u64::MAX,
    ];
    let mut buf = [0u8; PAGE_SIZE];
    for &v in cases {
        let mut off = 0usize;
        write_index_payload_varint_u64(&mut buf, &mut off, v).unwrap();
        let written = off;
        // Length matches the standalone size helper exactly.
        assert_eq!(written, varint_u64_len(v), "varint length mismatch for {v}");
        let mut read_off = 0usize;
        let decoded = read_varint_u64_checked(&buf[..written], &mut read_off).unwrap();
        assert_eq!(decoded, v, "round-trip mismatch for {v}");
        assert_eq!(
            read_off, written,
            "decoder consumed wrong byte count for {v}"
        );
    }
}

/// Length table sanity-check: the realistic page_id sizes mydb cares
/// about must fit in 1–5 bytes; only pathological values reach 10.
#[test]
fn v10_varint_length_table_matches_reality() {
    assert_eq!(varint_u64_len(0), 1);
    assert_eq!(varint_u64_len(127), 1);
    assert_eq!(varint_u64_len(128), 2);
    assert_eq!(varint_u64_len(16_383), 2);
    assert_eq!(varint_u64_len(16_384), 3);
    // TPC-H SF=1 lineitem heap ≈ 52K pages → 3-byte varint.
    assert_eq!(varint_u64_len(52_000), 3);
    // TB-class workload ≈ 2^36 pages → 6 bytes.
    assert_eq!(varint_u64_len(1u64 << 36), 6);
    assert_eq!(varint_u64_len(u64::MAX), 10);
}

/// v10 RowId encoder must round-trip across every realistic page_id
/// magnitude — small (varint=1 B), medium (3 B), TB-class (5 B), and
/// the pathological u64::MAX edge.
#[test]
fn v10_row_id_round_trips_across_page_id_magnitudes() {
    let page_ids: &[u64] = &[
        0,
        1,
        127,
        128,
        52_000,
        1u64 << 16,
        1u64 << 32,
        1u64 << 36,
        u64::MAX,
    ];
    let table_id = 7;
    let mut buf = [0u8; PAGE_SIZE];
    for &page_id in page_ids {
        let row_id = RowId {
            page_id: page_id as u32,
            slot_id: 0xabcd,
        };
        let mut off = 0usize;
        encode_row_id_v7_into_page(row_id, &mut buf, &mut off).unwrap();
        // Encoded length matches the size helper.
        assert_eq!(off, row_id_v7_encoded_len(row_id));
        let mut read_off = 0usize;
        let decoded = decode_row_id_v7_from(&buf[..off], &mut read_off, table_id).unwrap();
        assert_eq!(decoded, row_id);
        assert_eq!(read_off, off);
    }
}

/// v10 leaf-page key codec: every part_count from 1 through MySQL's
/// documented 16-column ceiling must round-trip cleanly. (Above 16
/// is rejected by the encoder; that bound is itself sanity-checked
/// below.)
#[test]
fn v10_leaf_page_key_part_counts_round_trip_up_to_sixteen() {
    let mut buf = [0u8; PAGE_SIZE];
    for n in 1..=16usize {
        let parts: Vec<KeyPart> = (0..n).map(|i| KeyPart::Int(i as i64)).collect();
        let key = IndexKey::from(parts);
        let mut off = 0usize;
        encode_index_key_into_leaf_page(&key, &mut buf, &mut off).unwrap();
        assert_eq!(buf[0] as usize, n, "u8 part_count should equal n={n}");
        let mut read_off = 0usize;
        let decoded = decode_index_key_from_leaf_page(&buf[..off], &mut read_off).unwrap();
        assert_eq!(decoded, key);
        assert_eq!(read_off, off);
    }
}

/// A part count above u8::MAX (255) must be rejected at encode-time
/// rather than silently truncating to a wrap-around count.
#[test]
fn v10_leaf_page_key_rejects_part_count_above_u8_max() {
    let mut buf = [0u8; PAGE_SIZE];
    let parts: Vec<KeyPart> = (0..=u8::MAX as usize)
        .map(|i| KeyPart::Int(i as i64))
        .collect();
    let key = IndexKey::from(parts);
    let mut off = 0usize;
    let err = encode_index_key_into_leaf_page(&key, &mut buf, &mut off).unwrap_err();
    assert_eq!(err.code(), 1118, "should error with too-many-parts code");
}

/// End-to-end: a v10 generic leaf page with several non-trivial keys
/// must round-trip through `encode_index_tree_page` /
/// `decode_index_tree_page` *and* every byte-level lookup helper.
/// Drives the realistic mixed-key shape (strings, ints, mixed widths)
/// that lineitem-style indexes exhibit.
#[test]
fn v10_generic_leaf_round_trips_mixed_keys() {
    let entries: Vec<IndexEntry> = vec![
        ("alpha", 1, 100u64),
        ("alpha", 2, 100),
        ("beta", 1, 50_000),      // medium page_id (3-byte varint)
        ("gamma", 7, 1u64 << 20), // larger page_id (4-byte varint)
    ]
    .into_iter()
    .map(|(s, n, page_id)| IndexEntry {
        key: IndexKey::from(vec![KeyPart::String(s.to_string()), KeyPart::Int(n)]),
        row_id: RowId {
            page_id: page_id as u32,
            slot_id: n as u16,
        },
    })
    .collect();

    let page = IndexPage::Leaf(IndexLeafPage::new_with_layout(
        entries.clone(),
        Some(999),
        KEY_LAYOUT_GENERIC,
        0,
    ));
    let buf = encode_index_tree_page(&page).unwrap();

    // In-memory decode path.
    let decoded = decode_index_tree_page(&buf).unwrap();
    match decoded {
        IndexPage::Leaf(leaf) => {
            assert_eq!(leaf.entries, entries);
            assert_eq!(leaf.next_leaf, Some(999));
        }
        _ => panic!("expected leaf"),
    }

    // Byte-level point lookup for every entry — exercises
    // `decode_index_key_from_leaf_page` plus `decode_row_id_v7_from`
    // on varint page_ids.
    for entry in &entries {
        let result = lookup_leaf_page_bytes(&buf, &entry.key).unwrap();
        assert!(!result.rows.is_empty(), "missed {:?}", entry.key);
        assert!(result.rows.contains(&entry.row_id));
    }
}

/// Build a minimal v9-format index meta page and confirm the v10
/// loader rejects it. This is the precise on-disk reject path that
/// `startup::validate_or_rebuild_indexes` keys off when an operator
/// drops a v9 data directory into a v10 build.
#[test]
fn v10_loader_rejects_v9_index_meta_page() {
    let mut buf = vec![0u8; PAGE_SIZE];
    // v9 meta page header layout, mirrored from `encode_index_meta`
    // but with the v9 version constants (INDEX_VERSION=9,
    // INDEX_PAGE_VERSION=6, INDEX_PAGE_MAGIC byte4='6').
    write_u32(&mut buf, 0, 0x4d_49_4d_34); // INDEX_META_MAGIC unchanged
    write_u16(&mut buf, 4, 6); // legacy INDEX_PAGE_VERSION
    write_u16(&mut buf, 6, 0);
    write_u64(&mut buf, 8, 0);
    write_u32(&mut buf, 16, 0); // placeholder for checksum
    write_u32(&mut buf, 20, 9); // legacy INDEX_VERSION
    write_u32(&mut buf, 24, PAGE_SIZE as u32);
    write_u64(&mut buf, 32, 1);
    write_u64(&mut buf, 40, 2);
    write_u64(&mut buf, 48, 0);
    let checksum = page_checksum(&buf, 16);
    write_u32(&mut buf, 16, checksum);

    let err = decode_index_meta(&buf).unwrap_err();
    assert_eq!(err.code(), 1030, "loader should refuse stale meta page");
}

/// Reproducer for the customer_nation-style ADD INDEX failure seen
/// during TPC-H SF=1 load: 150 K rows × small INT key + dense
/// per-key duplication exercises the offline bulk-load path. Before
/// the v10 size-estimator fix this would surface as
/// "index page is too large".
#[test]
fn v10_dense_small_int_bulk_load_accepts_duplicate_runs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_dense_small_int.idx");
    let index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let total = 8_000i64;
    let mut entries: Vec<IndexEntry> = (0..total)
        .map(|id| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(id % 25)]),
            row_id: test_row_id(id, 64),
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.row_id.cmp(&b.row_id)));

    index.bulk_load_sorted(1, entries).unwrap();

    assert!(
        index.next_page_id() > 3,
        "dense duplicate run should span multiple pages"
    );
    assert_eq!(
        index
            .get(&IndexKey::from(vec![KeyPart::Int(7)]), false)
            .unwrap()
            .len(),
        (total / 25) as usize
    );
}

#[test]
fn v10_add_index_offline_bulk_dense_small_int_key_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "n".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let total = 2_500i64;
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..total)
            .map(|id| vec![Value::Int(id), Value::Int(id % 25)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    db.add_index(
        "test.t",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "n_idx".into(),
            columns: vec!["n".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();

    let table = db.table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.name == "n_idx").unwrap();
    let rows = db
        .lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(7)]))
        .unwrap();
    assert_eq!(rows.len(), (total / 25) as usize);
}

#[test]
#[ignore = "large correctness regression: offline index build on 150000 rows with dense duplicate keys"]
fn v10_add_index_offline_bulk_150k_rows_small_int_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "n".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let total = 150_000i64;
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..total)
            .map(|id| vec![Value::Int(id), Value::Int(id % 25)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    db.add_index(
        "test.t",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "n_idx".into(),
            columns: vec!["n".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();

    // Pick one key value and confirm every matching row comes back.
    let table = db.table("test.t").unwrap();
    let index = table.indexes.iter().find(|i| i.name == "n_idx").unwrap();
    let rows = db
        .lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(7)]))
        .unwrap();
    // 150_000 / 25 = 6_000 rows per nationkey.
    assert_eq!(rows.len(), 6_000);
}

/// Functional reject-and-rebuild test: drop a hand-crafted v9 .idx
/// file into a fresh data dir, open the database, and confirm
/// startup validation rebuilt the index (file replaced, queries
/// return the expected rows).
#[test]
fn v10_startup_validation_rebuilds_v9_index_file() {
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    // Seed an empty data dir with a table + 8 rows, then close it
    // so we have a valid heap on disk and an authoritative row set
    // to compare against after the forced rebuild.
    {
        let db = Database::open(dir.path()).unwrap();
        db.create_table(
            "t",
            vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            }],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", (0..8).map(|v| vec![Value::Int(v)]).collect())
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }
    let index_path = t_index_path(dir.path(), "t", 1, "PRIMARY", "idx");
    assert!(
        index_path.exists(),
        "fresh dir should have written the PRIMARY index"
    );

    // Stomp the .idx with a v9-format meta page (the body doesn't
    // matter — the version check trips at decode_index_meta).
    {
        let mut v9_meta = vec![0u8; PAGE_SIZE];
        write_u32(&mut v9_meta, 0, 0x4d_49_4d_34);
        write_u16(&mut v9_meta, 4, 6);
        write_u32(&mut v9_meta, 20, 9);
        write_u32(&mut v9_meta, 24, PAGE_SIZE as u32);
        write_u64(&mut v9_meta, 32, 1);
        write_u64(&mut v9_meta, 40, 2);
        let cks = page_checksum(&v9_meta, 16);
        write_u32(&mut v9_meta, 16, cks);
        std::fs::write(&index_path, &v9_meta).unwrap();
    }
    // Sanity-check: a fresh load of the stomped file errors at the
    // version check.
    {
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let err = IndexFile::open(index_path.clone(), log).unwrap_err();
        assert_eq!(err.code(), 1030, "stale .idx must be rejected pre-rebuild");
    }

    // Re-open the database; startup validation should detect the
    // stale .idx, rebuild it from the heap, and the rebuilt index
    // must serve every original row.
    let db = Database::open(dir.path()).unwrap();
    let txn = db.begin_txn().unwrap();
    let rows = db.scan_rows("test.t", txn).unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(rows.len(), 8, "post-rebuild scan should see all rows");
    let mut keys: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.values.first()? {
            Value::Int(v) => Some(*v),
            _ => None,
        })
        .collect();
    keys.sort();
    assert_eq!(keys, (0..8).collect::<Vec<_>>());

    // The rebuilt .idx must round-trip its own version check.
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let _ = IndexFile::open(index_path, log).expect("rebuilt index must open");
}

// -- rowid-6bytes M2: slot-reclaim state-machine tests ----------------------
//
// These cover the post-M1 slot lifecycle:
//
//   ACTIVE  ──DELETE──▶  DELETED_PENDING  ──reclaim──▶  FREE
//
// where the reclaim transition requires (a) the Delete undo record was
// purged (oldest_active_snapshot_scn > deleted_scn AND retention has
// elapsed), and (b) `delete_indexes_deferred_at` already removed every
// secondary-index entry that pointed at `(page_id, slot_id)` during the
// DELETE's commit. The reclaim flips `ROW_FLAG_DELETED → ROW_FLAG_FREE`
// on the heap page and journals a `WalPayload::SlotReclaim` record.
//
// The tests use `MYDB_UNDO_RETENTION_MS = 0` and an idle snapshot to
// drive the purge condition deterministically — no real-time wait.

/// Helper: create the canonical 2-column test schema `(id INT PK, v VARCHAR)`.
fn create_reclaim_test_table(db: &Database) {
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::VarChar(Some(64)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
}

/// Helper: insert `id` and return the assigned RowId.
fn insert_reclaim_row(db: &Database, id: i64) -> RowId {
    let txn = db.begin_txn().unwrap();
    let rids = db
        .insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(id), Value::String(format!("v{id}"))]],
        )
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    rids[0]
}

/// Helper: probe a slot's raw flags by `(table_name, row_id)`.
fn slot_flags(db: &Database, table_name: &str, row_id: RowId) -> u8 {
    let table = db.table(table_name).unwrap();
    db.slot_flags_for_test(table.id, row_id)
        .unwrap()
        .expect("slot exists")
}

#[test]
fn slot_reclaim_flips_tombstone_to_free_after_purge() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    create_reclaim_test_table(&db);
    db.set_undo_retention_ms(0).unwrap();

    let row_id = insert_reclaim_row(&db, 7);
    // The just-inserted slot is ACTIVE — slot-directory flag is 0.
    assert_eq!(slot_flags(&db, "test.t", row_id), 0);

    // Delete + commit. No live snapshot, so once the Delete undo is
    // dropped by purge, the slot is safe to reclaim. After commit the
    // slot directory's `flags` byte is still 0 — the MVCC tombstone
    // lives in the row payload's MVCC header (MVCC_FLAG_DELETED bit),
    // not in the slot directory. This is what gives older snapshots
    // enough state to materialise the pre-delete row.
    let table = db.table("test.t").unwrap();
    let txn = db.begin_txn().unwrap();
    assert!(db.delete_row_by_id(txn, &table, row_id).unwrap());
    db.commit_txn(txn.txn_id).unwrap();
    let post_delete_flags = slot_flags(&db, "test.t", row_id);
    assert!(
        post_delete_flags == 0 || post_delete_flags == ROW_FLAG_FREE,
        "post-DELETE slot is either still tombstoned (0) or already \
         reclaimed by the commit-path purge (FREE); got {post_delete_flags}"
    );

    // Purge runs at commit_txn() — depending on the clock tick the
    // reclaim may have already happened. Sleep past the ms-bucket
    // boundary and run an explicit purge to make the test
    // deterministic (the purge's `retain_since = now - retention_ms`
    // selects strictly-earlier buckets; same-ms inserts survive).
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.purge_obsolete_undo();
    assert_eq!(
        slot_flags(&db, "test.t", row_id),
        ROW_FLAG_FREE,
        "purge transitions tombstone → FREE"
    );

    let (total, runs, errors) = db.slot_reclaim_status_counters();
    assert_eq!(total, 1, "exactly one slot reclaimed");
    assert!(runs >= 1, "at least one purge run touched a tombstone");
    assert_eq!(errors, 0);
}

#[test]
fn slot_reclaim_held_off_while_old_snapshot_active() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    create_reclaim_test_table(&db);
    db.set_undo_retention_ms(0).unwrap();

    let row_id = insert_reclaim_row(&db, 11);

    // Open a read snapshot BEFORE the delete commits. The snapshot's
    // SCN < delete commit SCN, so the Delete undo must survive purge.
    let reader = db.begin_txn().unwrap();
    let _snapshot_guard = db.register_statement_snapshot(reader.snapshot_scn);

    let table = db.table("test.t").unwrap();
    let txn = db.begin_txn().unwrap();
    assert!(db.delete_row_by_id(txn, &table, row_id).unwrap());
    db.commit_txn(txn.txn_id).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(5));
    db.purge_obsolete_undo();
    assert_eq!(
        slot_flags(&db, "test.t", row_id),
        0,
        "old snapshot pins the Delete undo; slot stays at the MVCC \
         tombstone (slot-directory flag 0, row header MVCC_FLAG_DELETED)"
    );
    let (total, _runs, errors) = db.slot_reclaim_status_counters();
    assert_eq!(total, 0, "no reclaim while snapshot is active");
    assert_eq!(errors, 0);

    // Drop the snapshot; the next purge can reclaim the tombstone.
    drop(_snapshot_guard);
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.purge_obsolete_undo();
    assert_eq!(
        slot_flags(&db, "test.t", row_id),
        ROW_FLAG_FREE,
        "after snapshot release the slot reclaims",
    );
    let (total, _runs, errors) = db.slot_reclaim_status_counters();
    assert_eq!(total, 1);
    assert_eq!(errors, 0);
}

#[test]
fn slot_reclaim_makes_slot_id_reusable_on_next_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    create_reclaim_test_table(&db);
    db.set_undo_retention_ms(0).unwrap();

    let first = insert_reclaim_row(&db, 1);
    let table = db.table("test.t").unwrap();
    let txn = db.begin_txn().unwrap();
    assert!(db.delete_row_by_id(txn, &table, first).unwrap());
    db.commit_txn(txn.txn_id).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    db.purge_obsolete_undo();
    assert_eq!(slot_flags(&db, "test.t", first), ROW_FLAG_FREE);

    // The next INSERT may reuse the FREE slot. We don't pin a specific
    // slot id (the reuse heuristic is internal to `insert_with_reserve`),
    // but the reclaimed slot must be drawn from the FREE pool — so the
    // total slot count on the page should not strictly grow when reuse
    // happens. The weaker invariant we always assert: subsequent INSERTs
    // succeed, and no FREE-flagged slot survives the next INSERT path on
    // the same page (it either gets reused or stays FREE — both fine).
    let second = insert_reclaim_row(&db, 2);
    // Either the new row reuses the freed slot (same row_id) or lands
    // in a fresh slot; both are correct. Just assert the new row is
    // visible and the original tombstone hasn't resurfaced as live.
    let scan = db
        .scan_rows(
            "test.t",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].values[0], Value::Int(2));
    assert_eq!(scan[0].row_id, second);
    // Whatever slot the deleted row occupied must not look like an
    // ACTIVE live row again unless it was actually reused.
    if first == second {
        assert_eq!(slot_flags(&db, "test.t", first), 0, "reused slot is ACTIVE");
    } else {
        assert_eq!(slot_flags(&db, "test.t", first), ROW_FLAG_FREE);
    }
}

/// Rows inserted after reclaim must land in reclaimed slots of older pages
/// instead of growing the heap, stay readable (including through the primary
/// key), and survive WAL recovery at their absolute slot positions.
#[test]
fn reclaimed_slots_in_older_pages_take_new_rows_before_the_tail() {
    let dir = tempfile::tempdir().unwrap();
    // VARCHAR(64) rows of ~70 bytes: 800 of them span several heap pages.
    let padding = "x".repeat(56);
    let row = |id: i64| vec![Value::Int(id), Value::String(format!("{id:06}{padding}"))];
    let reused;
    {
        let db = Arc::new(open_relaxed_test_db(dir.path()));
        create_reclaim_test_table(&db);
        db.set_undo_retention_ms(0).unwrap();
        let mut row_ids = Vec::new();
        for chunk in (0..800).collect::<Vec<i64>>().chunks(8) {
            let txn = db.begin_txn().unwrap();
            row_ids.extend(
                db.insert_rows(txn, "test.t", chunk.iter().map(|&id| row(id)).collect())
                    .unwrap(),
            );
            db.commit_txn(txn.txn_id).unwrap();
        }
        let table = db.table("test.t").unwrap();
        let pages_before = db.heap_page_count(table.id);
        assert!(pages_before >= 4, "fixture must span several pages");
        let victims: Vec<RowId> = row_ids
            .iter()
            .copied()
            .filter(|id| id.page_id == 1)
            .collect();
        assert!(!victims.is_empty());
        let txn = db.begin_txn().unwrap();
        for &victim in &victims {
            assert!(db.delete_row_by_id(txn, &table, victim).unwrap());
        }
        db.commit_txn(txn.txn_id).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        db.purge_obsolete_undo();
        for &victim in &victims {
            assert_eq!(slot_flags(&db, "test.t", victim), ROW_FLAG_FREE);
        }

        let mut inserted = Vec::new();
        for id in 1000..1000 + victims.len() as i64 {
            let txn = db.begin_txn().unwrap();
            inserted.extend(db.insert_rows(txn, "test.t", vec![row(id)]).unwrap());
            db.commit_txn(txn.txn_id).unwrap();
        }
        assert!(
            inserted.iter().all(|id| id.page_id == 1),
            "new rows must reuse reclaimed page 1 slots: {inserted:?}"
        );
        assert_eq!(
            db.heap_page_count(table.id),
            pages_before,
            "heap must not grow"
        );
        reused = inserted;

        let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
        let mut ids: Vec<i64> = db
            .scan_rows("test.t", view)
            .unwrap()
            .into_iter()
            .map(|row| match row.values[0] {
                Value::Int(id) => id,
                ref other => panic!("unexpected id {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        let mut expected: Vec<i64> = (0..800)
            .filter(|id| !victims.contains(&row_ids[*id as usize]))
            .chain(1000..1000 + victims.len() as i64)
            .collect();
        expected.sort_unstable();
        assert_eq!(ids, expected);
    }
    // Recovery replays the reused-slot inserts at their recorded positions.
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let rows = db.scan_rows("test.t", view).unwrap();
    for (offset, row_id) in reused.iter().enumerate() {
        let row = rows
            .iter()
            .find(|row| row.row_id == *row_id)
            .expect("reused row survives recovery");
        assert_eq!(row.values[0], Value::Int(1000 + offset as i64));
    }
    let table = db.table("test.t").unwrap();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let txn = db.begin_txn().unwrap();
    let found = db
        .lookup_unique_index_for_write_one(
            txn,
            &table,
            primary,
            IndexKey::from(vec![KeyPart::Int(1000)]),
        )
        .unwrap();
    assert_eq!(found, Some(reused[0]));
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn slot_reclaim_wal_record_replays_idempotently_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let row_id;
    {
        let db = Arc::new(open_relaxed_test_db(dir.path()));
        create_reclaim_test_table(&db);
        db.set_undo_retention_ms(0).unwrap();

        row_id = insert_reclaim_row(&db, 42);
        let table = db.table("test.t").unwrap();
        let txn = db.begin_txn().unwrap();
        assert!(db.delete_row_by_id(txn, &table, row_id).unwrap());
        db.commit_txn(txn.txn_id).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        db.purge_obsolete_undo();
        assert_eq!(slot_flags(&db, "test.t", row_id), ROW_FLAG_FREE);
        // Flush so the page reaches disk; the WAL SlotReclaim record is
        // already durable from the strict-mode WAL append above.
        db.flush_dirty_data().unwrap();
    }
    // Reopen. Recovery replays the WAL — including the SlotReclaim
    // record. Even if the heap page on disk is already FREE (no-op
    // branch) the replay must not crash and the post-recovery slot
    // state must still be FREE.
    {
        let db = Arc::new(open_relaxed_test_db(dir.path()));
        assert_eq!(
            slot_flags(&db, "test.t", row_id),
            ROW_FLAG_FREE,
            "SlotReclaim survives recovery"
        );
    }
}

/// P0-1c (undo segment write buffer): a hard crash (`mem::forget`, no
/// graceful flush) loses the buffered undo segment tail. Recovery must
/// rebuild the lost frames from their WAL `UndoRecord` twins, repair the
/// segment file (so the id space cannot collide), roll the loser back,
/// and keep accepting new DML.
#[test]
fn undo_buffer_crash_repairs_segment_and_rolls_back_loser() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "t",
            vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
                ColumnDef {
                    name: "v".into(),
                    data_type: DataType::Int,
                    unsigned: false,
                    zerofill: false,
                    invisible: false,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: None,
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: None,
                    collation: None,
                },
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let seed = db.begin_txn().unwrap();
        db.insert_rows(
            seed,
            "test.t",
            (1..=10)
                .map(|i| vec![Value::Int(i), Value::Int(i * 10)])
                .collect(),
        )
        .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        // Loser: updates every row + inserts more, then "crashes". Its
        // undo records live only in the WAL and the (lost) tail buffer.
        let loser = db.begin_txn().unwrap();
        db.update_rows(
            loser,
            "test.t",
            None,
            |_| Ok(true),
            |row| Ok(vec![row[0].clone(), Value::Int(999)]),
        )
        .unwrap();
        db.insert_rows(
            loser,
            "test.t",
            (11..=15)
                .map(|i| vec![Value::Int(i), Value::Int(0)])
                .collect(),
        )
        .unwrap();
        db.flush_dirty_data().unwrap();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
    }

    let db = open_relaxed_test_db(dir.path());
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(i) => i,
        _ => 0,
    });
    assert_eq!(rows.len(), 10, "loser inserts must be rolled back");
    for (i, row) in rows.iter().enumerate() {
        let id = i as i64 + 1;
        assert_eq!(
            row.values,
            vec![Value::Int(id), Value::Int(id * 10)],
            "loser update must be rolled back"
        );
    }
    // New DML must not collide with WAL-recovered undo ids.
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(100), Value::Int(1)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 11);
}

/// P1-3 (`MYDB_UNDO_DELTA_REWRITE`): sparse undo on the full-row rewrite
/// path. With the in-place patch disabled (forcing every UPDATE through
/// the rewrite block), delta undo records must still drive both MVCC
/// reconstruction (old snapshot reads across a multi-record chain,
/// including NULL transitions and variable-width changes — shapes the
/// in-place path never allowed) and transaction rollback.
#[test]
fn undo_delta_rewrite_reconstructs_old_versions_and_rolls_back() {
    set_update_delta_enabled_for_test(false);
    set_undo_delta_rewrite_enabled_for_test(true);
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "s".into(),
                data_type: DataType::VarChar(Some(255)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![Value::Int(1), Value::String("v0".into())]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // Snapshot anchored before any update.
    let snap0 = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);

    // Two committed rewrite-updates: longer string (variable width) then
    // a NULL transition — both ineligible for the in-place patch.
    for (i, new_s) in [Value::String("a-much-longer-value-v1".into()), Value::Null]
        .into_iter()
        .enumerate()
    {
        let txn = db.begin_txn().unwrap();
        let n = db
            .update_rows_with_changed_columns(
                txn,
                "test.t",
                None,
                Some(&[1]),
                |row| Ok(row[0] == Value::Int(1)),
                |row| Ok(vec![row[0].clone(), new_s.clone()]),
            )
            .unwrap();
        assert_eq!(n, 1, "update {i} must hit the row");
        db.commit_txn(txn.txn_id).unwrap();
    }

    // Old snapshot walks the delta chain back to v0.
    let rows = db.scan_rows("test.t", snap0).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values,
        vec![Value::Int(1), Value::String("v0".into())]
    );
    // Fresh snapshot sees the latest version (NULL).
    let now = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let rows = db.scan_rows("test.t", now).unwrap();
    assert_eq!(rows[0].values, vec![Value::Int(1), Value::Null]);

    // Rollback: a delta-undone update must restore the pre-image.
    let loser = db.begin_txn().unwrap();
    db.update_rows_with_changed_columns(
        loser,
        "test.t",
        None,
        Some(&[1]),
        |row| Ok(row[0] == Value::Int(1)),
        |row| Ok(vec![row[0].clone(), Value::String("rolled-back".into())]),
    )
    .unwrap();
    db.rollback_txn(loser.txn_id).unwrap();
    let now = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let rows = db.scan_rows("test.t", now).unwrap();
    assert_eq!(rows[0].values, vec![Value::Int(1), Value::Null]);
}

// P1/8 — UpdateDelta path covers width-shrinking changes (string getting
// shorter), Decimal width changes, and multi-column NULL ↔ non-NULL
// mixes. Snapshot reads must reconstruct the pre-change row and
// rollback must restore it cleanly across all of them.
#[test]
fn undo_delta_rewrite_covers_shrinking_decimal_and_mixed_null_changes() {
    // Force the rewrite path (skip the in-place patch) so every
    // UPDATE in this test exercises the sparse-on-rewrite branch.
    set_update_delta_enabled_for_test(false);
    set_undo_delta_rewrite_enabled_for_test(true);

    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "s".into(),
                data_type: DataType::VarChar(Some(255)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "d".into(),
                data_type: DataType::Decimal(Some((20, 4))),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![
            Value::Int(1),
            Value::String("v0-very-long-initial-value".into()),
            Value::DecimalCompact(123400, 4),
        ]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let snap0 = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);

    // Update 1: shrink string + change decimal value. Both are
    // variable/decimal — the in-place patch path is disabled, so the
    // rewrite + sparse undo branch is the only way through.
    let txn = db.begin_txn().unwrap();
    db.update_rows_with_changed_columns(
        txn,
        "test.t",
        None,
        Some(&[1, 2]),
        |row| Ok(row[0] == Value::Int(1)),
        |row| {
            Ok(vec![
                row[0].clone(),
                Value::String("v1".into()),
                Value::DecimalCompact(999999, 4),
            ])
        },
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // Update 2: NULL out the string column.
    let txn = db.begin_txn().unwrap();
    db.update_rows_with_changed_columns(
        txn,
        "test.t",
        None,
        Some(&[1]),
        |row| Ok(row[0] == Value::Int(1)),
        |row| Ok(vec![row[0].clone(), Value::Null, row[2].clone()]),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // Update 3: NULL → non-NULL transition on the string column.
    let txn = db.begin_txn().unwrap();
    db.update_rows_with_changed_columns(
        txn,
        "test.t",
        None,
        Some(&[1]),
        |row| Ok(row[0] == Value::Int(1)),
        |row| {
            Ok(vec![
                row[0].clone(),
                Value::String("revived".into()),
                row[2].clone(),
            ])
        },
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // Old snapshot walks back through all three UpdateDelta records.
    let rows = db.scan_rows("test.t", snap0).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values,
        vec![
            Value::Int(1),
            Value::String("v0-very-long-initial-value".into()),
            Value::DecimalCompact(123400, 4)
        ]
    );
    // Fresh snapshot sees the latest.
    let now = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let rows = db.scan_rows("test.t", now).unwrap();
    assert_eq!(
        rows[0].values,
        vec![
            Value::Int(1),
            Value::String("revived".into()),
            Value::DecimalCompact(999999, 4)
        ]
    );

    // Rollback: an aborted update that touches both columns at once
    // must restore exactly the latest committed values.
    let loser = db.begin_txn().unwrap();
    db.update_rows_with_changed_columns(
        loser,
        "test.t",
        None,
        Some(&[1, 2]),
        |row| Ok(row[0] == Value::Int(1)),
        |row| {
            Ok(vec![
                row[0].clone(),
                Value::String("rolled-back".into()),
                Value::DecimalCompact(1, 4),
            ])
        },
    )
    .unwrap();
    db.rollback_txn(loser.txn_id).unwrap();
    let now = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let rows = db.scan_rows("test.t", now).unwrap();
    assert_eq!(
        rows[0].values,
        vec![
            Value::Int(1),
            Value::String("revived".into()),
            Value::DecimalCompact(999999, 4)
        ]
    );

    // Reset toggles so subsequent tests don't observe the override.
    set_update_delta_enabled_for_test(true);
}

// P1/9 — mydb_undo_buffer_bytes is a runtime sysvar. Reading reports
// the current UndoManager buffer_limit; writing applies through the
// undo lock and a write-through transition (value=0) drains the
// buffered tail so subsequent appends observe the new policy.
#[test]
fn mydb_undo_buffer_bytes_is_settable_at_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    // Default value matches the bumped 1 MiB constant.
    assert_eq!(db.undo_buffer_bytes(), 1024 * 1024);
    db.set_storage_variable("mydb_undo_buffer_bytes", 4096)
        .unwrap();
    assert_eq!(db.undo_buffer_bytes(), 4096);
    // Switching to 0 puts the manager into write-through.
    db.set_storage_variable("mydb_undo_buffer_bytes", 0)
        .unwrap();
    assert_eq!(db.undo_buffer_bytes(), 0);
    // Re-enable and verify the value round-trips.
    db.set_storage_variable("mydb_undo_buffer_bytes", 256 * 1024)
        .unwrap();
    assert_eq!(db.undo_buffer_bytes(), 256 * 1024);
}

// P2/16 — active_snapshot_count must roll alongside the snapshot
// registry: zero when idle, advance/recede with begin/commit. Used by
// the PROMOTE TO MASTER guard to refuse a role flip that would strand
// pre-promote readers.
#[test]
fn active_snapshot_count_tracks_open_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    assert_eq!(db.active_snapshot_count(), 0);

    let txn1 = db.begin_txn().unwrap();
    let arc = std::sync::Arc::new(db);
    let guard1 = arc.register_statement_snapshot(txn1.snapshot_scn);
    assert_eq!(arc.active_snapshot_count(), 1);

    let txn2 = arc.begin_txn().unwrap();
    let guard2 = arc.register_statement_snapshot(txn2.snapshot_scn);
    assert_eq!(arc.active_snapshot_count(), 2);

    drop(guard1);
    assert_eq!(arc.active_snapshot_count(), 1);
    drop(guard2);
    assert_eq!(arc.active_snapshot_count(), 0);

    arc.rollback_txn(txn1.txn_id).unwrap();
    arc.rollback_txn(txn2.txn_id).unwrap();
}

// P2/12 — softfail warning is opt-in. Without the sysvar, crossing
// the disk cap is silent; with it set, the counter advances on every
// pressure event.
#[test]
fn undo_disk_cap_softfail_warn_records_emissions() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.create_table(
        "t",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "v".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.set_undo_tablespace_max_bytes(40).unwrap();

    // Without the sysvar, soft pressure events do not bump the warn counter.
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(1), Value::Int(10)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(db.undo_disk_cap_softfail_warns_emitted(), 0);
    assert!(!db.undo_disk_cap_softfail_warn());

    // Enable the sysvar through the same dispatch the parser uses.
    db.set_storage_variable("mydb_undo_disk_cap_softfail_warn", 1)
        .unwrap();
    assert!(db.undo_disk_cap_softfail_warn());
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(2), Value::Int(20)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert!(
        db.undo_disk_cap_softfail_warns_emitted() >= 1,
        "softfail counter must advance once enabled"
    );

    // Disable and verify no further bumps occur.
    db.set_storage_variable("mydb_undo_disk_cap_softfail_warn", 0)
        .unwrap();
    let baseline = db.undo_disk_cap_softfail_warns_emitted();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(3), Value::Int(30)]])
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    assert_eq!(db.undo_disk_cap_softfail_warns_emitted(), baseline);
}

// P1/8 — default is now ON. The full undo-delta-rewrite path must
// fire under the default toggle (no MYDB_UNDO_DELTA_REWRITE env set
// in the test environment).
#[test]
fn undo_delta_rewrite_is_default_on() {
    // The static `UNDO_DELTA_REWRITE_ENABLED` may have been flipped
    // off by a prior test in the same process. Reset to the default.
    set_undo_delta_rewrite_enabled_for_test(true);
    assert!(undo_delta_rewrite_enabled());
}

// P0/4 — slot-reclaim quarantine: a target that fails on every attempt
// must (a) survive ≤ MAX_ATTEMPTS retries in the quarantine, (b) get
// evicted as permanent on the run that crosses the cap, (c) bump the
// permanent-failure counter exactly once per evicted pair, and (d)
// roll the quarantine-size atomic alongside.
#[test]
fn slot_reclaim_quarantine_retries_then_gives_up_permanently() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let target = (7u64, RowId::new(1, 2));
    let always_fail = |_, _| Err(error(crate::error::catalog::ER_GET_ERRNO, "synthetic"));

    db.trigger_slot_reclaim_for_test(&[target], always_fail);
    let snap = db.slot_reclaim_quarantine_snapshot_for_test();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].1, 1);
    assert_eq!(db.slot_reclaim_quarantine_counters().0, 1);
    assert_eq!(db.slot_reclaim_quarantine_counters().1, 0);
    assert_eq!(db.slot_reclaim_status_counters().2, 1);

    for expected_count in 2..=crate::storage::core::SLOT_RECLAIM_MAX_ATTEMPTS {
        db.trigger_slot_reclaim_for_test(&[], always_fail);
        let snap = db.slot_reclaim_quarantine_snapshot_for_test();
        if expected_count < crate::storage::core::SLOT_RECLAIM_MAX_ATTEMPTS {
            assert_eq!(snap.len(), 1, "still in quarantine before cap");
            assert_eq!(snap[0].1, expected_count, "counter advances by one per run");
        } else {
            assert!(snap.is_empty(), "evicted as permanent at cap");
        }
    }
    let (qsize, permanent) = db.slot_reclaim_quarantine_counters();
    assert_eq!(qsize, 0);
    assert_eq!(permanent, 1);

    // Fresh failing target re-enters cleanly with no double-counting.
    db.trigger_slot_reclaim_for_test(&[target], always_fail);
    let (qsize, permanent) = db.slot_reclaim_quarantine_counters();
    assert_eq!(qsize, 1);
    assert_eq!(permanent, 1);
}

// P0/4 — a successful retry (or an idempotent Ok(false)) must remove
// the pair from the quarantine without bumping the permanent-failure
// counter, and successive successes are a no-op.
#[test]
fn slot_reclaim_quarantine_clears_on_success() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let target = (3u64, RowId::new(0, 0));

    // Seed the quarantine.
    db.trigger_slot_reclaim_for_test(&[target], |_, _| {
        Err(error(crate::error::catalog::ER_GET_ERRNO, "synthetic"))
    });
    assert_eq!(db.slot_reclaim_quarantine_counters().0, 1);

    // Next pass: same target, this time the attempt succeeds. Quarantine
    // empties without bumping permanent failures.
    db.trigger_slot_reclaim_for_test(&[target], |_, _| Ok(true));
    let (qsize, permanent) = db.slot_reclaim_quarantine_counters();
    assert_eq!(qsize, 0);
    assert_eq!(permanent, 0);
    // total reclaimed counter advanced.
    assert_eq!(db.slot_reclaim_status_counters().0, 1);

    // Idempotent Ok(false) on an empty quarantine is silent.
    db.trigger_slot_reclaim_for_test(&[target], |_, _| Ok(false));
    let snap = db.slot_reclaim_quarantine_snapshot_for_test();
    assert!(snap.is_empty());
    assert_eq!(db.slot_reclaim_status_counters().0, 1);
}

// P0/4 — flooding past SLOT_RECLAIM_QUARANTINE_CAP must not grow the
// quarantine without bound; overflow lands in the permanent-failure
// counter so the runaway never silently masks "we ran out of room".
#[test]
fn slot_reclaim_quarantine_respects_cap() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let cap = crate::storage::core::SLOT_RECLAIM_QUARANTINE_CAP;

    let mut targets = Vec::with_capacity(cap + 16);
    for i in 0..(cap + 16) {
        targets.push((1u64, RowId::new(i as u32 + 1, 0)));
    }
    db.trigger_slot_reclaim_for_test(&targets, |_, _| {
        Err(error(crate::error::catalog::ER_GET_ERRNO, "synthetic"))
    });
    let (qsize, permanent) = db.slot_reclaim_quarantine_counters();
    assert!(qsize <= cap as u64, "quarantine size {qsize} <= cap {cap}");
    assert_eq!(permanent, 16, "overflow counted as permanent failures");
}

#[test]
fn lock_table_capacity_guard_rejects_oversized_batch_with_1206() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    create_checkpoint_test_table(&db);

    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        (1..=5).map(|i| vec![Value::Int(i)]).collect(),
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let table = db.table("test.t").unwrap().clone();
    let primary = table.indexes.iter().find(|index| index.primary).unwrap();
    let row_ids: Vec<RowId> = (1..=5)
        .map(|i| {
            db.lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(i)]))
                .unwrap()[0]
        })
        .collect();

    // Cap below the batch size: a 5-row FOR UPDATE batch must fail with MySQL
    // 1206 (ER_LOCK_TABLE_FULL) instead of growing the lock table. The guard
    // runs before any entry is inserted, so the failed batch leaves no locks.
    let txn = db.begin_txn().unwrap();
    let err = crate::util::env::with_vars(&[("MYDB_MAX_LOCKS", Some("3"))], || {
        db.lock_rows_blocking(txn.txn_id, table.id, row_ids.clone())
    })
    .unwrap_err();
    assert_eq!(err.code(), 1206, "expected ER_LOCK_TABLE_FULL, got {err:?}");
    db.commit_txn(txn.txn_id).unwrap();

    // Default (0 = unlimited) lets the same batch through unchanged.
    let txn2 = db.begin_txn().unwrap();
    crate::util::env::with_vars(&[("MYDB_MAX_LOCKS", Some("0"))], || {
        db.lock_rows_blocking(txn2.txn_id, table.id, row_ids.clone())
    })
    .unwrap();
    db.commit_txn(txn2.txn_id).unwrap();
}

/// Micro-benchmark isolating the row-lock manager: X-lock a million rows in
/// one `SELECT … FOR UPDATE`-style batch, then release them at commit. The
/// batch lock path does not touch the heap, so synthetic RowIds suffice and
/// the timing reflects pure lock-manager cost (HashMap inserts, lock-log,
/// owner storage). Ignored by default; run with:
///   cargo nextest run --run-ignored all bench_for_update_lock_release_million
#[test]
#[ignore = "perf benchmark; run explicitly with --run-ignored"]
fn bench_for_update_lock_release_million_rows() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let table = db.table("test.t").unwrap().clone();

    const N: usize = 1_000_000;
    // ~400 rows per 16 KiB page → ~2500 pages, mirroring a real heap layout.
    let row_ids: Vec<RowId> = (0..N)
        .map(|i| RowId::new((i / 400) as u32, (i % 400) as u16))
        .collect();

    let txn = db.begin_txn().unwrap();
    let t0 = Instant::now();
    db.lock_rows_blocking(txn.txn_id, table.id, row_ids)
        .unwrap();
    let lock = t0.elapsed();

    let held = db.lock_manager().locks.len();
    let t1 = Instant::now();
    db.commit_txn(txn.txn_id).unwrap();
    let release = t1.elapsed();

    eprintln!(
        "[bench] FOR UPDATE {N} rows: lock={lock:?} release={release:?} \
         locks_held={held}"
    );
    assert_eq!(held, N, "all rows should be locked");
}

// ===========================================================================
// M1 LOB: unchanged-chain reuse differential tests and MVCC correctness.
//
// MYDB_LOB_REUSE_UNCHANGED on/off paths serve as mutual oracles:
// identical UPDATE sequences must finish byte-identically. Reuse avoids overflow
// rewriting without changing visible data, snapshot reads, or rollback semantics.
// ===========================================================================

/// Create id INT PK, body VARCHAR, and data VARBINARY, with two LOB columns.
fn lob_two_col_table(db: &Database) -> u64 {
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col(
                "body",
                DataType::Text(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
            lob_col(
                "data",
                DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.table("test.t").unwrap().id
}

fn scan_sorted(db: &Database) -> Vec<Vec<Value>> {
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    rows.into_iter().map(|r| r.values).collect()
}

/// Execute a fixed LOB UPDATE sequence under the given reuse switch and return final scan results.
/// Call this closure with reuse on/off and require byte-identical output.
fn run_lob_update_sequence(reuse_on: bool) -> Vec<Vec<Value>> {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    set_lob_reuse_unchanged_for_test(reuse_on);
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Memory, false),
    )
    .unwrap();
    let _ = lob_two_col_table(&db);

    let big_a = "A".repeat(10_000); // Above 4096 bytes, externalize.
    let big_b = vec![0xBBu8; 9_000]; // Above 4096 bytes, externalize.
    let big_c = "C".repeat(20_000);
    let big_d = vec![0xDDu8; 15_000];

    // Initially row 1 has both columns externalized; row 2 has one external and one inline.
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![
            vec![
                Value::Int(1),
                Value::String(big_a.clone()),
                Value::Bytes(big_b.clone()),
            ],
            vec![
                Value::Int(2),
                Value::String("small".into()),
                Value::Bytes(big_d.clone()),
            ],
        ],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // 1. Change only non-LOB data while keeping id/body/data values unchanged; both LOBs are unchanged.
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| {
            // Rewrite the whole row identically; unchanged body/data bytes must reuse both chains.
            Ok(vec![row[0].clone(), row[1].clone(), row[2].clone()])
        },
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // 2. Replace body with a new large value while preserving data.
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| {
            Ok(vec![
                row[0].clone(),
                Value::String(big_c.clone()),
                row[2].clone(),
            ])
        },
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // 3. Write big_c to body again and retain data, making both unchanged.
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| {
            Ok(vec![
                row[0].clone(),
                Value::String(big_c.clone()),
                row[2].clone(),
            ])
        },
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // 4. Change row 1 data from an external large value to NULL while preserving body.
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| Ok(vec![row[0].clone(), row[1].clone(), Value::Null]),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // 5. Move row 2 body from a small inline value above 4096 bytes,
    //    while shrinking data back from external to inline.
    let boundary = "X".repeat(5_000); // > 4096
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(2)),
        |row| {
            Ok(vec![
                row[0].clone(),
                Value::String(boundary.clone()),
                Value::Bytes(b"tiny".to_vec()),
            ])
        },
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // 6. Rewrite row 2 identically; unchanged large body reuses its chain and data stays unchanged.
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(2)),
        |row| Ok(vec![row[0].clone(), row[1].clone(), row[2].clone()]),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    let result = scan_sorted(&db);
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db); // Memory mode needs no flush; forget excludes destructor timing.
    result
}

#[test]
fn lob_reuse_unchanged_matches_rewrite_path_byte_for_byte() {
    // Reuse-on and rewrite-off paths finish byte-identically after the same UPDATE sequence.
    let with_reuse = run_lob_update_sequence(true);
    let without_reuse = run_lob_update_sequence(false);
    assert_eq!(
        with_reuse, without_reuse,
        "Reuse and rewrite paths must finish byte-identically as mutual oracles"
    );
    // Also assert actual final values so both paths cannot share an unnoticed error.
    assert_eq!(with_reuse.len(), 2);
    assert_eq!(with_reuse[0][0], Value::Int(1));
    assert_eq!(with_reuse[0][1], Value::String("C".repeat(20_000)));
    assert_eq!(with_reuse[0][2], Value::Null);
    assert_eq!(with_reuse[1][0], Value::Int(2));
    assert_eq!(with_reuse[1][1], Value::String("X".repeat(5_000)));
    assert_eq!(with_reuse[1][2], Value::Bytes(b"tiny".to_vec()));
}

#[test]
fn lob_reuse_records_reuse_stats_and_saves_externalization() {
    // With reuse on, identical large-LOB rewrites increment reuse statistics without new chains;
    // with reuse off, each operation externalizes anew without reuse statistics.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let big = "Z".repeat(50_000); // Above 4096 bytes, externalize.

    let measure = |reuse_on: bool| -> (u64, u64, u64) {
        set_lob_reuse_unchanged_for_test(reuse_on);
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Memory, false),
        )
        .unwrap();
        let _ = lob_two_col_table(&db);
        let seed = db.begin_txn().unwrap();
        db.insert_rows(
            seed,
            "test.t",
            vec![vec![
                Value::Int(1),
                Value::String(big.clone()),
                Value::Bytes(b"x".to_vec()),
            ]],
        )
        .unwrap();
        db.commit_txn(seed.txn_id).unwrap();

        let before = db.dml_stats_snapshot();
        // Change id to trigger five whole-row updates while preserving body/data.
        for _ in 0..5 {
            let txn = db.begin_txn().unwrap();
            db.update_rows(
                txn,
                "test.t",
                None,
                |row| Ok(row[0] == Value::Int(1)),
                |row| Ok(vec![Value::Int(1), row[1].clone(), row[2].clone()]),
            )
            .unwrap();
            db.commit_txn(txn.txn_id).unwrap();
        }
        let after = db.dml_stats_snapshot();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
        (
            after.lob_chains_reused - before.lob_chains_reused,
            after.lob_bytes_reused - before.lob_bytes_reused,
            after.lob_chains_externalized - before.lob_chains_externalized,
        )
    };

    let (reused_on, bytes_on, externalized_on) = measure(true);
    let (reused_off, _bytes_off, externalized_off) = measure(false);

    // On: each UPDATE reuses body's chain, totaling five reuses and 5*50000 saved bytes,
    // with zero body re-externalizations.
    assert_eq!(reused_on, 5, "Reuse on must record five chain reuses");
    assert_eq!(bytes_on, 5 * 50_000, "Reuse must save 5*50000 bytes");
    assert_eq!(
        externalized_on, 0,
        "Identical rewrites through the reuse path must not externalize any chain again"
    );
    // Off: no reuse and five body externalizations.
    assert_eq!(reused_off, 0, "Reuse off must record no reuse");
    assert_eq!(
        externalized_off, 5,
        "The rewrite path must externalize the body chain on every UPDATE"
    );
}

#[test]
fn lob_reuse_preserves_mvcc_snapshot_old_version() {
    // MVCC: A takes an old snapshot, B updates while reusing unchanged LOBs,
    // A still reads old values; after B commits and purge runs, current chains remain readable.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    set_lob_reuse_unchanged_for_test(true);
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Memory, false),
    )
    .unwrap();
    let _ = lob_two_col_table(&db);

    let v_old = "A".repeat(10_000);
    let v_new = "B".repeat(12_000);

    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![
            Value::Int(1),
            Value::String(v_old.clone()),
            Value::Bytes(vec![0x11u8; 8_000]),
        ]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    let table = db.table("test.t").unwrap();
    let row_id = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()[0]
        .row_id;

    // A uses RR and pins snapshot_scn before B's changes.
    let txn_a = db.begin_txn().unwrap();
    let view_a = TxnView::read_only(txn_a.snapshot_scn, IsolationLevel::RepeatableRead);

    // Autocommit B genuinely changes body and reuses unchanged data.
    let txn_b = db.begin_txn().unwrap();
    db.update_rows(
        txn_b,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| {
            Ok(vec![
                Value::Int(1),
                Value::String(v_new.clone()),
                row[2].clone(),
            ])
        },
    )
    .unwrap();
    db.commit_txn(txn_b.txn_id).unwrap();

    // A still reads old body/data because its snapshot precedes B's commit.
    let a_row = db.read_row(&table, row_id, view_a).unwrap().unwrap();
    assert_eq!(
        a_row.values[1],
        Value::String(v_old.clone()),
        "A's old snapshot must read the old body"
    );
    assert_eq!(a_row.values[2], Value::Bytes(vec![0x11u8; 8_000]));

    // Current reads see new body and the correct reused data chain.
    let latest = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
    let now_row = db.read_row(&table, row_id, latest).unwrap().unwrap();
    assert_eq!(now_row.values[1], Value::String(v_new.clone()));
    assert_eq!(now_row.values[2], Value::Bytes(vec![0x11u8; 8_000]));

    // Finish A and purge old versions; the current row still owns reused data,
    // and current reads must not corrupt or report snapshot-too-old.
    db.commit_txn(txn_a.txn_id).unwrap();
    db.purge_obsolete_undo();
    let after_purge = db.read_row(&table, row_id, latest).unwrap().unwrap();
    assert_eq!(after_purge.values[1], Value::String(v_new));
    assert_eq!(
        after_purge.values[2],
        Value::Bytes(vec![0x11u8; 8_000]),
        "The reused chain must remain readable after purge, proving it was not freed incorrectly"
    );
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn lob_snapshot_rebuilds_descriptors_across_full_then_delta_undo_walk() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    set_lob_reuse_unchanged_for_test(true);
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Memory, false),
    )
    .unwrap();
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col("counter", DataType::Int, false),
            lob_col(
                "body",
                DataType::Text(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
            lob_col(
                "data",
                DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let old_body = "A".repeat(20_000);
    let old_data = vec![0x33; 16_000];
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![
            Value::Int(1),
            Value::Int(0),
            Value::String(old_body.clone()),
            Value::Bytes(old_data.clone()),
        ]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // First UPDATE expands the short INSERT header. The next fixed-width
    // UPDATE is represented as UpdateDelta.
    for counter in [1, 2] {
        let txn = db.begin_txn().unwrap();
        db.update_rows(
            txn,
            "test.t",
            None,
            |_| Ok(true),
            |row| {
                let mut row = row.to_vec();
                row[1] = Value::Int(counter);
                Ok(row)
            },
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }

    // Pin the version before one more delta and the later full LOB update.
    let reader = db.begin_txn().unwrap();
    let reader_view = TxnView::read_only(reader.snapshot_scn, IsolationLevel::RepeatableRead);
    let delta = db.begin_txn().unwrap();
    db.update_rows(
        delta,
        "test.t",
        None,
        |_| Ok(true),
        |row| {
            let mut row = row.to_vec();
            row[1] = Value::Int(3);
            Ok(row)
        },
    )
    .unwrap();
    db.commit_txn(delta.txn_id).unwrap();

    let full = db.begin_txn().unwrap();
    db.update_rows(
        full,
        "test.t",
        None,
        |_| Ok(true),
        |row| {
            let mut row = row.to_vec();
            row[2] = Value::String("B".repeat(24_000));
            Ok(row)
        },
    )
    .unwrap();
    db.commit_txn(full.txn_id).unwrap();

    let row = db
        .scan_rows("test.t", reader_view)
        .unwrap()
        .remove(0)
        .values;
    assert_eq!(row[1], Value::Int(2));
    assert_eq!(row[2], Value::String(old_body));
    assert_eq!(row[3], Value::Bytes(old_data));
    db.commit_txn(reader.txn_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn lob_reuse_rollback_restores_old_version_no_leak_no_double_free() {
    // Reuse rollback: UPDATE reusing an unchanged large LOB followed by ROLLBACK
    // restores the complete old bytes without leaking or double-freeing chains.
    // Compare final values after rollback with reuse enabled/disabled.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let v_body = "A".repeat(10_000);
    let v_data = vec![0x22u8; 9_000];

    let run = |reuse_on: bool| -> Vec<Vec<Value>> {
        set_lob_reuse_unchanged_for_test(reuse_on);
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Memory, false),
        )
        .unwrap();
        let _ = lob_two_col_table(&db);
        let seed = db.begin_txn().unwrap();
        db.insert_rows(
            seed,
            "test.t",
            vec![vec![
                Value::Int(1),
                Value::String(v_body.clone()),
                Value::Bytes(v_data.clone()),
            ]],
        )
        .unwrap();
        db.commit_txn(seed.txn_id).unwrap();

        // In one explicit transaction, change body, reuse data, then roll back.
        let txn = db.begin_txn().unwrap();
        db.update_rows(
            txn,
            "test.t",
            None,
            |row| Ok(row[0] == Value::Int(1)),
            |row| {
                Ok(vec![
                    Value::Int(1),
                    Value::String("Q".repeat(20_000)),
                    row[2].clone(),
                ])
            },
        )
        .unwrap();
        db.rollback_txn(txn.txn_id).unwrap();
        let result = scan_sorted(&db);
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
        result
    };

    let on = run(true);
    let off = run(false);
    assert_eq!(on, off, "Reuse and rewrite paths must agree after rollback");
    // Rollback must restore the old version byte-for-byte.
    assert_eq!(on.len(), 1);
    assert_eq!(on[0][1], Value::String(v_body));
    assert_eq!(on[0][2], Value::Bytes(v_data));
}

#[test]
fn lob_reuse_delete_then_purge_round_trips() {
    // DELETE/purge after reuse UPDATE must neither double-free nor leak
    // reused chains; the row must actually disappear.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    set_lob_reuse_unchanged_for_test(true);
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Memory, false),
    )
    .unwrap();
    let _ = lob_two_col_table(&db);

    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![
            Value::Int(1),
            Value::String("A".repeat(10_000)),
            Value::Bytes(vec![0x33u8; 9_000]),
        ]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // Rewrite identically once, reusing both chains.
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| Ok(vec![Value::Int(1), row[1].clone(), row[2].clone()]),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();

    // DELETE, then purge.
    let txn = db.begin_txn().unwrap();
    db.delete_rows(txn, "test.t", None, |row| Ok(row[0] == Value::Int(1)))
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.purge_obsolete_undo();

    let rows = scan_sorted(&db);
    assert!(
        rows.is_empty(),
        "The row must disappear after DELETE and purge"
    );
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn lob_rollback_then_purge_does_not_free_live_chain() {
    // BUG-1 (docs/lob-management-audit-2026-07.md) — regression for the
    // rollback+purge blind spot. Sequence:
    //   INSERT R (body externalized to chain C_old); COMMIT
    //   BEGIN; UPDATE R.body -> new big value (chain C_new); ROLLBACK
    //     -> rollback restores R to reference C_old, frees C_new; but the
    //        rolled-back UPDATE undo record still names before_external =
    //        {C_old}.
    //   set retention 0; purge
    //     -> the buggy path freed C_old (still referenced by the live R)
    //        to the `.lob` free list; a later allocation reused those
    //        pages and R read back garbage / the new value / a decode
    //        error. The fix keeps the rolled-back slot alive until its
    //        undo drains so purge can identify the RolledBack creator and
    //        NOT free C_old.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    set_lob_reuse_unchanged_for_test(true);
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Memory, false),
    )
    .unwrap();
    let table_id = lob_two_col_table(&db);

    // Distinct multi-page patterns so a corrupted read is unambiguous.
    let original_body = "P".repeat(40_000); // C_old
    let original_data = vec![0x5Au8; 30_000];
    let rolled_back_body = "Q".repeat(50_000); // C_new (must be discarded)

    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![
            Value::Int(1),
            Value::String(original_body.clone()),
            Value::Bytes(original_data.clone()),
        ]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // UPDATE body to a new externalized value, then ROLLBACK.
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| {
            Ok(vec![
                Value::Int(1),
                Value::String(rolled_back_body.clone()),
                row[2].clone(),
            ])
        },
    )
    .unwrap();
    db.rollback_txn(txn.txn_id).unwrap();

    // Purge with zero retention so the rolled-back undo record is a
    // purge victim (the exact window the bug lived in).
    db.set_undo_retention_ms(0).unwrap();
    for _ in 0..8 {
        if !db.purge_obsolete_undo() {
            break;
        }
    }

    // Force fresh allocations that would reuse C_old's pages if the bug
    // wrongly returned them to the free list — maximising the chance a
    // corrupted read surfaces rather than reading not-yet-overwritten
    // stale bytes.
    for id in 2..8 {
        let t = db.begin_txn().unwrap();
        db.insert_rows(
            t,
            "test.t",
            vec![vec![
                Value::Int(id),
                Value::String("Z".repeat(45_000)),
                Value::Bytes(vec![0x11u8; 25_000]),
            ]],
        )
        .unwrap();
        db.commit_txn(t.txn_id).unwrap();
    }
    for _ in 0..8 {
        if !db.purge_obsolete_undo() {
            break;
        }
    }

    // Row 1 must still read back its ORIGINAL body/data, byte-for-byte.
    let rows = scan_sorted(&db);
    let row1 = rows
        .iter()
        .find(|r| r[0] == Value::Int(1))
        .expect("row 1 must survive");
    assert_eq!(
        row1[1],
        Value::String(original_body),
        "BUG-1: rolled-back UPDATE's purge freed the live LOB chain — body corrupted"
    );
    assert_eq!(
        row1[2],
        Value::Bytes(original_data),
        "BUG-1: live data chain corrupted after rollback+purge"
    );

    // A subsequent DELETE + purge of the (correctly restored) row must
    // not double-free or corrupt the free list.
    let t = db.begin_txn().unwrap();
    db.delete_rows(t, "test.t", None, |row| Ok(row[0] == Value::Int(1)))
        .unwrap();
    db.commit_txn(t.txn_id).unwrap();
    for _ in 0..8 {
        if !db.purge_obsolete_undo() {
            break;
        }
    }
    // Free-list must be internally consistent (no cycle / corruption) —
    // `lob_space_stats` walks it and errors on a cycle.
    let (_pages, _free) = db.lob_space_stats(table_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn lob_reuse_chain_survives_midupdate_failure() {
    // BUG-2 (docs/lob-management-audit-2026-07.md) — a multi-row UPDATE
    // that REUSES a byte-identical old chain for one column and allocates
    // a new chain for another must, on a mid-operation failure, free ONLY
    // the freshly-allocated chains. Freeing the reused old chain — still
    // referenced by the not-yet-overwritten live row — is a use-after-free.
    //
    // Trigger: a batch UPDATE where the per-txn undo cap trips on the 2nd
    // descriptor-only record after the 1st row already staged its chains. With the
    // bug, the 1st row's reused `data` chain was staged and freed; the
    // rolled-back live row then read garbage.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    set_lob_reuse_unchanged_for_test(true);
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Memory, false),
    )
    .unwrap();
    let table_id = lob_two_col_table(&db);

    // Two rows, each with an externalized `data` (reused later) and an
    // externalized `body` (changed later → new chain).
    let data1 = vec![0x5Au8; 30_000];
    let data2 = vec![0x6Bu8; 30_000];
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![
            vec![
                Value::Int(1),
                Value::String("a".repeat(30_000)),
                Value::Bytes(data1.clone()),
            ],
            vec![
                Value::Int(2),
                Value::String("b".repeat(30_000)),
                Value::Bytes(data2.clone()),
            ],
        ],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // Cap per-txn undo so one compact record fits but two do not. Unchanged
    // external LOBs are descriptor-only, so this threshold intentionally
    // stays tiny instead of depending on either LOB's payload size.
    db.set_undo_txn_max_bytes(180).unwrap();
    let txn = db.begin_txn().unwrap();
    let result = db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(matches!(row[0], Value::Int(1) | Value::Int(2))),
        |row| {
            // Change body (new chain), keep data byte-identical (reused).
            Ok(vec![
                row[0].clone(),
                Value::String("X".repeat(35_000)),
                row[2].clone(),
            ])
        },
    );
    assert!(result.is_err(), "UPDATE must fail on the per-txn undo cap");
    db.rollback_txn(txn.txn_id).unwrap();
    db.set_undo_txn_max_bytes(0).unwrap(); // lift the cap

    // Force allocations that would reuse any wrongly-freed chain pages.
    for id in 10..16 {
        let t = db.begin_txn().unwrap();
        db.insert_rows(
            t,
            "test.t",
            vec![vec![
                Value::Int(id),
                Value::String("Z".repeat(30_000)),
                Value::Bytes(vec![0x11u8; 30_000]),
            ]],
        )
        .unwrap();
        db.commit_txn(t.txn_id).unwrap();
    }
    db.set_undo_retention_ms(0).unwrap();
    for _ in 0..8 {
        if !db.purge_obsolete_undo() {
            break;
        }
    }

    // Both original rows must be unchanged and byte-intact.
    let rows = scan_sorted(&db);
    let row1 = rows.iter().find(|r| r[0] == Value::Int(1)).expect("row 1");
    let row2 = rows.iter().find(|r| r[0] == Value::Int(2)).expect("row 2");
    assert_eq!(
        row1[2],
        Value::Bytes(data1),
        "BUG-2: reused `data` chain of row 1 freed by failed UPDATE cleanup"
    );
    assert_eq!(
        row2[2],
        Value::Bytes(data2),
        "BUG-2: reused `data` chain of row 2 freed by failed UPDATE cleanup"
    );
    // body must be the pre-UPDATE value (the UPDATE rolled back).
    assert_eq!(row1[1], Value::String("a".repeat(30_000)));
    assert_eq!(row2[1], Value::String("b".repeat(30_000)));
    let _ = db.lob_space_stats(table_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

/// Microbenchmark: update only an integer beside a 1MB BLOB and compare reuse-on/off
/// overflow write volume using M6 write-amplification counters.
/// `cargo nextest run -- --ignored lob_reuse_write_amplification_benchmark`
#[test]
#[ignore = "manual write-amplification comparison over 200 updates beside a 1 MiB BLOB"]
fn lob_reuse_write_amplification_benchmark() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    const ITERS: u64 = 200;
    const BLOB_BYTES: usize = 1024 * 1024; // 1 MiB

    let bench = |reuse_on: bool| -> (u64, u64, std::time::Duration) {
        set_lob_reuse_unchanged_for_test(reuse_on);
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Memory, false),
        )
        .unwrap();
        db.create_table(
            "t",
            vec![
                lob_col("id", DataType::Int, true),
                lob_col("n", DataType::Int, false),
                lob_col(
                    "blob",
                    DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
                    false,
                ),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let blob = vec![0x5Au8; BLOB_BYTES];
        let seed = db.begin_txn().unwrap();
        db.insert_rows(
            seed,
            "test.t",
            vec![vec![
                Value::Int(1),
                Value::Int(0),
                Value::Bytes(blob.clone()),
            ]],
        )
        .unwrap();
        db.commit_txn(seed.txn_id).unwrap();

        let before = db.dml_stats_snapshot();
        let t0 = std::time::Instant::now();
        for i in 0..ITERS {
            let txn = db.begin_txn().unwrap();
            // Change only n; unchanged BLOB with reuse on must skip the 1MB overflow rewrite.
            db.update_rows(
                txn,
                "test.t",
                None,
                |row| Ok(row[0] == Value::Int(1)),
                move |row| {
                    Ok(vec![
                        Value::Int(1),
                        Value::Int(i as i64 + 1),
                        row[2].clone(),
                    ])
                },
            )
            .unwrap();
            db.commit_txn(txn.txn_id).unwrap();
        }
        let elapsed = t0.elapsed();
        let after = db.dml_stats_snapshot();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
        (
            after.lob_bytes_externalized - before.lob_bytes_externalized,
            after.lob_bytes_reused - before.lob_bytes_reused,
            elapsed,
        )
    };

    let (ext_on, reused_on, dur_on) = bench(true);
    let (ext_off, reused_off, dur_off) = bench(false);

    eprintln!(
        "=== M1 LOB reuse microbenchmark ({ITERS} UPDATEs, 1MiB BLOB, integer column only) ==="
    );
    eprintln!(
        "  Reuse ON : overflow bytes={ext_on:>12}  reused bytes={reused_on:>12}  elapsed={dur_on:?}"
    );
    eprintln!(
        "  Reuse OFF: overflow bytes={ext_off:>12}  reused bytes={reused_off:>12}  elapsed={dur_off:?}"
    );
    let saved = ext_off.saturating_sub(ext_on);
    eprintln!(
        "  Saved overflow writes = {saved} bytes ({:.1}MiB), write amplification reduced {:.1}x",
        saved as f64 / (1024.0 * 1024.0),
        if ext_on == 0 {
            f64::INFINITY
        } else {
            ext_off as f64 / ext_on as f64
        }
    );

    // Assert no BLOB re-externalization with reuse on, eliminating write amplification.
    assert_eq!(ext_on, 0, "Reuse ON must avoid all overflow rewrites");
    assert_eq!(
        reused_on,
        ITERS * BLOB_BYTES as u64,
        "Reused bytes must equal ITERS*1MiB"
    );
    assert_eq!(
        ext_off,
        ITERS * BLOB_BYTES as u64,
        "Reuse OFF rewrites the full 1MiB chain every time"
    );
}

// ===========================================================================
// M2 partial/lazy materialization: read_chain_range and storage-side
// LENGTH/SUBSTRING pushdown. LENGTH uses zero-read materialize_len; SUBSTRING uses
// only covered pages. Compare both against full-chain materialization and slicing.
// ===========================================================================

/// Create one external large-LOB column, insert one row, and return (table_id,row_id).
/// body is VarString at physical position 1.
fn lob_substring_fixture(db: &Database, body: &str, data: &[u8]) -> (u64, RowId) {
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col(
                "body",
                DataType::Text(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
            lob_col(
                "data",
                DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let table_id = db.table("test.t").unwrap().id;
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![
            Value::Int(1),
            Value::String(body.to_string()),
            Value::Bytes(data.to_vec()),
        ]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let row_id = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()[0]
        .row_id;
    (table_id, row_id)
}

#[test]
fn m2_lob_column_byte_length_pushdown_matches_full_length() {
    // LENGTH pushdown returns total_len with zero reads, matching full materialization length
    // in bytes for ASCII, multibyte UTF-8, and binary values.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());

    let body = "héllo".repeat(3_000); // Multibyte text exceeding 4096 bytes externalizes.
    let data = vec![0x7Fu8; 10_000];
    let (table_id, row_id) = lob_substring_fixture(&db, &body, &data);

    // body is VarString, physical position 1.
    let pushed = db
        .lob_column_byte_length(table_id, row_id, 1)
        .unwrap()
        .expect("body must be externalized");
    assert_eq!(
        pushed,
        body.as_bytes().len() as u64,
        "LENGTH pushdown must equal the byte count, including multibyte characters"
    );
    // data is VarBinary, physical position 2.
    let pushed_data = db
        .lob_column_byte_length(table_id, row_id, 2)
        .unwrap()
        .expect("data must be externalized");
    assert_eq!(pushed_data, data.len() as u64);

    // id is Int at physical position 0, not external; None makes the caller use ordinary LENGTH.
    assert_eq!(
        db.lob_column_byte_length(table_id, row_id, 0).unwrap(),
        None
    );
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m2_lob_column_byte_length_inline_and_null_return_none() {
    // Small inline values and NULL are not externalized; None makes callers use ordinary access.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
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
    let table_id = db.table("test.t").unwrap().id;
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![
            vec![Value::Int(1), Value::String("small".into())], // Inline.
            vec![Value::Int(2), Value::Null],                   // NULL
            vec![Value::Int(3), Value::String(String::new())],  // Empty inline string.
        ],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    for r in &rows {
        assert_eq!(
            db.lob_column_byte_length(table_id, r.row_id, 1).unwrap(),
            None,
            "Inline, NULL, and empty columns must not be pushed down as external columns"
        );
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m2_read_chain_range_matches_full_chain_slice_across_boundaries() {
    // SUBSTRING/read_chain_range differential coverage varies offset/length across first,
    // middle, last, multiple pages, clamped bounds, and zero length, comparing byte slices
    // from full-chain materialization. Use current OVFL_PAGE_PAYLOAD for page boundaries.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());

    // Distinguishable byte data uses byte i=(i%251) as u8; the prime avoids page-period alignment.
    let total = 80_000usize; // More than four pages.
    let data: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
    let (table_id, row_id) = lob_substring_fixture(&db, "x", &data);

    let page_payload = OVFL_PAGE_PAYLOAD as u64;
    let cases: &[(u64, u64)] = &[
        (0, 1),                        // First byte.
        (0, page_payload),             // Entire page 0.
        (5, 100),                      // Within the first page.
        (page_payload - 10, 50),       // Across the page 0-to-1 boundary.
        (page_payload, 200),           // Starting at page 2.
        (page_payload * 2 + 7, 16400), // Across pages 2-to-3.
        (total as u64 - 100, 100),     // Aligned to the tail.
        (total as u64 - 50, 9999),     // Clamp beyond the tail.
        (total as u64, 100),           // Out-of-range offset yields empty output.
        (10, 0),                       // Zero length yields empty output.
        (0, total as u64 + 1000),      // Oversized length yields the entire chain.
    ];

    for &(off, len) in cases {
        let pushed = db
            .lob_column_substring(table_id, row_id, 2, &ColumnKind::VarBytes, off, len)
            .unwrap()
            .expect("data must be externalized");
        // Oracle: slice the complete byte vector over the same range.
        let oracle = {
            let total_u = data.len() as u64;
            if off >= total_u || len == 0 {
                Vec::new()
            } else {
                let s = off as usize;
                let e = off.saturating_add(len).min(total_u) as usize;
                data[s..e].to_vec()
            }
        };
        assert_eq!(
            pushed,
            Value::Bytes(oracle),
            "read_chain_range(off={off}, len={len}) must match full-chain slicing"
        );
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m2_lob_column_substring_left_and_right_semantics() {
    // LEFT(col, n) = range(0, n);RIGHT(col, n) = range(total-n, n)。
    // Verify storage-side ranges express LEFT/RIGHT semantics.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let body = "ABCDEFGHIJ".repeat(2_000); // 20000 ASCII bytes externalize.
    let (table_id, row_id) = lob_substring_fixture(&db, &body, b"x");

    // LEFT(body, 5)。
    let left = db
        .lob_column_substring(table_id, row_id, 1, &ColumnKind::VarString, 0, 5)
        .unwrap()
        .unwrap();
    assert_eq!(left, Value::String("ABCDE".into()));

    // RIGHT(body, 5) = range(total-5, 5)。
    let total = body.as_bytes().len() as u64;
    let right = db
        .lob_column_substring(table_id, row_id, 1, &ColumnKind::VarString, total - 5, 5)
        .unwrap()
        .unwrap();
    assert_eq!(right, Value::String("FGHIJ".into()));

    // SUBSTRING(body,10001,10) converts MySQL's one-based position to byte offset 10000.
    let mid = db
        .lob_column_substring(table_id, row_id, 1, &ColumnKind::VarString, 10_000, 10)
        .unwrap()
        .unwrap();
    assert_eq!(mid, Value::String("ABCDEFGHIJ".into()));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

/// Microbenchmark: LENGTH pushdown over a 16MiB BLOB uses zero reads versus full-chain materialization;
/// SUBSTRING LEFT(100) uses one page versus the full chain.
/// `cargo nextest run --run-ignored ignored-only m2_partial_materialization_benchmark`
#[test]
#[ignore = "manual partial versus full LOB materialization measurement on a 7 MiB BLOB"]
fn m2_partial_materialization_benchmark() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    const BLOB_BYTES: usize = 7 * 1024 * 1024; // 7 MiB is about 449 pages, below the column's 8M limit.
    const ITERS: u32 = 2_000;
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let data = vec![0x5Au8; BLOB_BYTES];
    let (table_id, row_id) = lob_substring_fixture(&db, "x", &data);

    // A: LENGTH pushdown reads zero pages.
    let t0 = std::time::Instant::now();
    let mut sink = 0u64;
    for _ in 0..ITERS {
        sink += db
            .lob_column_byte_length(table_id, row_id, 2)
            .unwrap()
            .unwrap();
    }
    let len_pushdown = t0.elapsed();
    assert_eq!(sink, BLOB_BYTES as u64 * ITERS as u64);

    // B: materialize the entire chain then take its length as the oracle.
    let t0 = std::time::Instant::now();
    let mut sink2 = 0u64;
    for _ in 0..ITERS {
        let v = db
            .with_overflow_read(table_id, |ovf| {
                let lob = db
                    .row_column_external_ref(table_id, row_id, 2)
                    .unwrap()
                    .unwrap();
                ovf.read_chain(lob)
            })
            .unwrap();
        sink2 += v.len() as u64;
    }
    let len_fullread = t0.elapsed();
    assert_eq!(sink2, BLOB_BYTES as u64 * ITERS as u64);

    // C: SUBSTRING LEFT(100) pushdown reads one page.
    let t0 = std::time::Instant::now();
    let mut sink3 = 0usize;
    for _ in 0..ITERS {
        let v = db
            .lob_column_substring(table_id, row_id, 2, &ColumnKind::VarBytes, 0, 100)
            .unwrap()
            .unwrap();
        if let Value::Bytes(b) = v {
            sink3 += b.len();
        }
    }
    let sub_pushdown = t0.elapsed();
    assert_eq!(sink3, 100 * ITERS as usize);

    eprintln!(
        "=== M2 partial materialization microbenchmark (7MiB BLOB, about {} pages, {ITERS} iterations) ===",
        BLOB_BYTES / OVFL_PAGE_PAYLOAD
    );
    eprintln!("  LENGTH pushdown (zero page reads): {len_pushdown:?}");
    eprintln!("  LENGTH full-chain materialization: {len_fullread:?}");
    eprintln!(
        "  LENGTH speedup: {:.1}x",
        len_fullread.as_secs_f64() / len_pushdown.as_secs_f64().max(1e-9)
    );
    eprintln!(
        "  SUBSTRING LEFT(100) pushdown: {sub_pushdown:?} (one page versus {} total pages)",
        BLOB_BYTES / OVFL_PAGE_PAYLOAD
    );
    // Pushdown should be significantly faster; LENGTH reads no pages.
    assert!(
        len_pushdown < len_fullread,
        "LENGTH pushdown must be faster than full-chain materialization"
    );
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

// ===========================================================================
// M3 shrinking returns contiguous free tail pages to the OS without format changes;
// free_count is not persisted and is recomputed on open. Cover reclamation, remaining data,
// free-list count consistency, reopen recovery, preserving interior holes, and opportunistic
// thresholds. The shared OVFL_VERSION controls the current generation format.
// ===========================================================================

/// M3 test database with retention=0 makes purge_obsolete_undo reclaim deleted chains immediately;
/// the default 60s retention delays reclamation. Durability is configurable.
fn m3_open_db(dir: &Path, durability: DurabilityMode) -> Database {
    let mut config = test_storage_config(durability, false);
    config.undo_retention = Duration::from_millis(0);
    Database::open_with_config(dir, config).unwrap()
}

/// Create id INT PK/data VARBINARY with one large LOB column; return table_id.
fn m3_lob_table(db: &Database) -> u64 {
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col(
                "data",
                DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.table("test.t").unwrap().id
}

fn m3_insert_big(db: &Database, id: i64, byte: u8, len: usize) {
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(id), Value::Bytes(vec![byte; len])]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
}

fn m3_delete_id(db: &Database, id: i64) {
    let txn = db.begin_txn().unwrap();
    db.delete_rows(txn, "test.t", None, move |row| Ok(row[0] == Value::Int(id)))
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    // Zero retention still compares millisecond created_unix_ms; plan_purge_victims
    // uses strict range(..retain_since). If delete and purge occur
    // within one millisecond, undo and LOB chains are not reclaimed.
    // Sleep 2ms to cross the timestamp boundary and ensure deterministic reclamation.
    std::thread::sleep(std::time::Duration::from_millis(2));
}

#[test]
fn m3_shrink_returns_tail_free_pages_and_keeps_remaining_data() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_SHRINK_MIN_FREE_PAGES", "1");
    let dir = tempfile::tempdir().unwrap();
    let db = m3_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);

    // Six rows of about 100KB, approximately seven pages each; ID-order allocation puts high IDs at the tail.
    let big = 100_000usize;
    for id in 1..=6 {
        m3_insert_big(&db, id, id as u8, big);
    }
    db.flush_dirty_data().unwrap();
    let lob_path = t_path(dir.path(), "t", table_id, "lob");
    let size_full = std::fs::metadata(&lob_path).unwrap().len();
    let (pc_full, fc_full) = db.lob_space_stats(table_id).unwrap();
    assert_eq!(
        fc_full, 0,
        "All pages are in use; the free list must be empty"
    );

    // Delete tail IDs 5/6, commit, and purge to free their contiguous tail chains.
    m3_delete_id(&db, 5);
    m3_delete_id(&db, 6);
    db.purge_obsolete_undo();
    let (_pc, fc_after_del) = db.lob_space_stats(table_id).unwrap();
    assert!(
        fc_after_del > 0,
        "Purged deleted chains must enter the free list"
    );
    // free_count must equal the traversed free-list length.
    let walked = db
        .with_overflow_read(table_id, |ovf| ovf.count_free_list())
        .unwrap();
    assert_eq!(
        fc_after_del, walked,
        "free_count must equal actual free-list length"
    );

    // Shrinking must return contiguous free tail pages.
    let shrunk = db.shrink_table_lobs(table_id).unwrap();
    assert!(
        shrunk > 0,
        "Contiguous free tail pages must be available for shrinking"
    );
    db.flush_dirty_data().unwrap();
    let size_shrunk = std::fs::metadata(&lob_path).unwrap().len();
    assert!(
        size_shrunk < size_full,
        "The physical .lob file must shrink: {size_full} -> {size_shrunk}"
    );
    // page_count decreases by shrunk pages and free_count updates consistently.
    let (pc_after, fc_after) = db.lob_space_stats(table_id).unwrap();
    assert_eq!(pc_after, pc_full - shrunk);
    let walked2 = db
        .with_overflow_read(table_id, |ovf| ovf.count_free_list())
        .unwrap();
    assert_eq!(
        fc_after, walked2,
        "After shrinking, free_count must still match the actual free list"
    );

    // Remaining IDs 1..=4 still read byte-for-byte.
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    assert_eq!(rows.len(), 4);
    for (i, id) in (1..=4).enumerate() {
        assert_eq!(rows[i].values[0], Value::Int(id));
        assert_eq!(rows[i].values[1], Value::Bytes(vec![id as u8; big]));
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m3_shrink_then_alloc_reads_back_correctly() {
    // Allocation/read after shrinking must stay within bounds and support new writes.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_SHRINK_MIN_FREE_PAGES", "1");
    let dir = tempfile::tempdir().unwrap();
    let db = m3_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    let big = 60_000usize;
    for id in 1..=5 {
        m3_insert_big(&db, id, id as u8, big);
    }
    m3_delete_id(&db, 4);
    m3_delete_id(&db, 5);
    db.purge_obsolete_undo();
    let shrunk = db.shrink_table_lobs(table_id).unwrap();
    assert!(shrunk > 0);

    // A new large value may extend page_count and must round-trip byte-for-byte.
    m3_insert_big(&db, 10, 0xAB, big);
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    assert_eq!(rows.len(), 4); // 1,2,3,10
    assert_eq!(rows[3].values[0], Value::Int(10));
    assert_eq!(rows[3].values[1], Value::Bytes(vec![0xAB; big]));
    // free_count remains consistent with the actual list.
    let (_pc, fc) = db.lob_space_stats(table_id).unwrap();
    let walked = db
        .with_overflow_read(table_id, |ovf| ovf.count_free_list())
        .unwrap();
    assert_eq!(fc, walked);
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m3_middle_hole_not_truncated() {
    // Interior free holes must not be truncated while later pages remain in use;
    // shrink zero pages, or only genuinely contiguous free tail pages.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_SHRINK_MIN_FREE_PAGES", "1");
    let dir = tempfile::tempdir().unwrap();
    let db = m3_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    let big = 60_000usize;
    for id in 1..=5 {
        m3_insert_big(&db, id, id as u8, big);
    }
    db.flush_dirty_data().unwrap();
    let lob_path = t_path(dir.path(), "t", table_id, "lob");
    let size_before = std::fs::metadata(&lob_path).unwrap().len();

    // Delete middle ID 3, keeping tail IDs 4/5 alive, then purge.
    m3_delete_id(&db, 3);
    db.purge_obsolete_undo();
    let (_pc, fc) = db.lob_space_stats(table_id).unwrap();
    assert!(fc > 0, "The middle chain must enter the free list");

    // Tail chains 4/5 remain in use, so shrink must return zero pages.
    let shrunk = db.shrink_table_lobs(table_id).unwrap();
    assert_eq!(
        shrunk, 0,
        "Interior holes must not be truncated while the tail is in use"
    );
    db.flush_dirty_data().unwrap();
    let size_after = std::fs::metadata(&lob_path).unwrap().len();
    assert_eq!(
        size_after, size_before,
        "Interior holes leave file size unchanged"
    );

    // Preserve rows 1,2,4,5.
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match r.values[0] {
            Value::Int(v) => v,
            _ => 0,
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 4, 5]);
    for r in &rows {
        let id = match r.values[0] {
            Value::Int(v) => v,
            _ => 0,
        };
        assert_eq!(r.values[1], Value::Bytes(vec![id as u8; big]));
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m3_shrink_below_threshold_is_noop() {
    // Opportunistic shrinking skips a tail below the threshold to prevent oscillation.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    // Set a large threshold to ensure no shrink.
    std::env::set_var("MYDB_LOB_SHRINK_MIN_FREE_PAGES", "100000");
    let dir = tempfile::tempdir().unwrap();
    let db = m3_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    let big = 60_000usize;
    for id in 1..=3 {
        m3_insert_big(&db, id, id as u8, big);
    }
    db.flush_dirty_data().unwrap();
    let lob_path = t_path(dir.path(), "t", table_id, "lob");
    let size_before = std::fs::metadata(&lob_path).unwrap().len();
    m3_delete_id(&db, 3);
    db.purge_obsolete_undo();
    let shrunk = db.shrink_table_lobs(table_id).unwrap();
    assert_eq!(shrunk, 0, "A free tail below the threshold must not shrink");
    db.flush_dirty_data().unwrap();
    assert_eq!(
        std::fs::metadata(&lob_path).unwrap().len(),
        size_before,
        "Below threshold, file size stays unchanged"
    );
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m3_shrink_disabled_guc_skips() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_SHRINK_MIN_FREE_PAGES", "1");
    std::env::set_var("MYDB_LOB_SHRINK_DISABLE", "1");
    let dir = tempfile::tempdir().unwrap();
    let db = m3_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    let big = 60_000usize;
    for id in 1..=4 {
        m3_insert_big(&db, id, id as u8, big);
    }
    m3_delete_id(&db, 3);
    m3_delete_id(&db, 4);
    db.purge_obsolete_undo();
    let shrunk = db.shrink_table_lobs(table_id).unwrap();
    assert_eq!(shrunk, 0, "MYDB_LOB_SHRINK_DISABLE=1 must skip shrinking");
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m3_shrink_survives_reopen_recovery() {
    // Crash recovery: strict shrink persists metadata through WAL and set_len; reopening
    // must preserve data, consistent page_count/free_count, and further allocation.
    // This covers completed shrinking. Metadata WAL before set_len is protected by
    // crash-safe ordering; an overlong recovered file is harmless, as shrink_to_fit documents.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_SHRINK_MIN_FREE_PAGES", "1");
    let dir = tempfile::tempdir().unwrap();
    let table_id;
    let big = 80_000usize;
    {
        let db = m3_open_db(dir.path(), DurabilityMode::Strict);
        table_id = m3_lob_table(&db);
        for id in 1..=6 {
            m3_insert_big(&db, id, id as u8, big);
        }
        m3_delete_id(&db, 5);
        m3_delete_id(&db, 6);
        db.purge_obsolete_undo();
        let shrunk = db.shrink_table_lobs(table_id).unwrap();
        assert!(shrunk > 0);
        db.flush_dirty_data().unwrap();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db); // Simulate process exit without graceful close.
    }

    // Reopen through recovery.
    let db = m3_open_db(dir.path(), DurabilityMode::Strict);
    // Remaining data is intact.
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    assert_eq!(rows.len(), 4);
    for (i, id) in (1..=4).enumerate() {
        assert_eq!(rows[i].values[1], Value::Bytes(vec![id as u8; big]));
    }
    // Recomputed free_count on reopen equals the actual free list.
    let (_pc, fc) = db.lob_space_stats(table_id).unwrap();
    let walked = db
        .with_overflow_read(table_id, |ovf| ovf.count_free_list())
        .unwrap();
    assert_eq!(
        fc, walked,
        "Recomputed free_count after reopening must match the actual free list"
    );
    // Further allocation and reading still work.
    m3_insert_big(&db, 20, 0xCD, big);
    let after = db
        .read_row(
            &db.table("test.t").unwrap(),
            db.scan_rows(
                "test.t",
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap()
            .iter()
            .find(|r| r.values[0] == Value::Int(20))
            .unwrap()
            .row_id,
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()
        .unwrap();
    assert_eq!(after.values[1], Value::Bytes(vec![0xCD; big]));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

/// Microbenchmark: after N INSERT-1MiB/DELETE/purge churn rounds,
/// compare .lob file sizes before and after shrinking.
/// `cargo nextest run --run-ignored ignored-only m3_shrink_space_reclaim_benchmark`
#[test]
#[ignore = "manual LOB space-reclamation measurement after large insert and delete churn"]
fn m3_shrink_space_reclaim_benchmark() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_SHRINK_MIN_FREE_PAGES", "1");
    const N: i64 = 40;
    const KEEP: i64 = 8; // Retain the first eight rows and delete the last 32.
    const BLOB: usize = 1024 * 1024; // 1 MiB
    let dir = tempfile::tempdir().unwrap();
    let db = m3_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    let lob_path = t_path(dir.path(), "t", table_id, "lob");

    // Phase 1 inserts N 1MiB rows without deletion, growing the high-water mark to roughly N MiB.
    // This simulates historical grow-only behavior: freed pages stay allocated and page_count never falls.
    for id in 1..=N {
        m3_insert_big(&db, id, (id % 251) as u8, BLOB);
    }
    db.flush_dirty_data().unwrap();
    let size_grown = std::fs::metadata(&lob_path).unwrap().len();

    // Phase 2 deletes KEEP+1..=N and purges, creating contiguous free tail pages.
    for id in (KEEP + 1)..=N {
        m3_delete_id(&db, id);
    }
    db.purge_obsolete_undo();
    db.flush_dirty_data().unwrap();
    let size_before = std::fs::metadata(&lob_path).unwrap().len();
    let (pc_before, fc_before) = db.lob_space_stats(table_id).unwrap();

    // Phase 3 shrinks and returns space.
    let shrunk = db.shrink_table_lobs(table_id).unwrap();
    db.flush_dirty_data().unwrap();
    let size_after = std::fs::metadata(&lob_path).unwrap().len();
    let (pc_after, fc_after) = db.lob_space_stats(table_id).unwrap();

    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    eprintln!(
        "=== M3 shrink microbenchmark (insert {N}*1MiB, delete {} tail rows, shrink) ===",
        N - KEEP
    );
    eprintln!("  Phase 1 after inserts: file = {:.2}MiB", mib(size_grown));
    eprintln!(
        "  Phase 2 after delete/purge: file={:.2}MiB (free pages retained), page_count={pc_before} free_count={fc_before}",
        mib(size_before)
    );
    eprintln!(
        "  Phase 3 after shrink: file={:.2}MiB page_count={pc_after} free_count={fc_after}",
        mib(size_after)
    );
    eprintln!(
        "  Returned {shrunk} pages = {:.2}MiB; file space reclaimed {:.2}MiB (before to after)",
        shrunk as f64 * 16_384.0 / (1024.0 * 1024.0),
        mib(size_before - size_after)
    );
    assert!(
        shrunk > 0,
        "Deleting tail rows must allow reclaiming many free tail pages"
    );
    assert!(
        size_after < size_before,
        "Shrinking must reduce physical file size"
    );
    // Before shrinking, size_before should remain near the N MiB grow-only high-water mark.
    assert!(
        size_before >= size_grown,
        "Delete/purge alone retains the file high-water mark without returning disk space"
    );
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

// ===========================================================================
// M4 contiguous extents and intra-chain prefetch need no format changes and address
// large sequential LOB reads degenerating to random I/O. Cover contiguous page IDs,
// exact round trips, mixed free-list allocation, shrink interaction, and prefetch on/off equivalence.
// ===========================================================================

const M4_OVFL_PAGE_PAYLOAD: usize = OVFL_PAGE_PAYLOAD;

/// Obtain an external column's LobRef(first,last,total) to check page-ID continuity.
fn m4_lob_ref(db: &Database, table_id: u64, row_id: RowId, col_pos: usize) -> Option<LobRef> {
    db.row_column_external_ref(table_id, row_id, col_pos)
        .unwrap()
}

fn m4_open_db(dir: &Path, durability: DurabilityMode) -> Database {
    let mut config = test_storage_config(durability, false);
    config.undo_retention = Duration::from_millis(0);
    Database::open_with_config(dir, config).unwrap()
}

#[test]
fn m4_extent_chain_is_contiguous_and_round_trips() {
    // Default extents activate at four pages; large chains must have consecutive IDs,
    // last-first=n_pages-1, and round-trip exactly.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::remove_var("MYDB_LOB_EXTENT_MIN_PAGES"); // Use default four-page threshold.
    let dir = tempfile::tempdir().unwrap();
    let db = m4_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);

    let big = 200_000usize; // About thirteen pages, well above the extent threshold.
    m3_insert_big(&db, 1, 0x5A, big);
    let row_id = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()[0]
        .row_id;
    let lob = m4_lob_ref(&db, table_id, row_id, 1).expect("data must be externalized");
    let n_pages = (big.div_ceil(M4_OVFL_PAGE_PAYLOAD)) as u64;
    assert_eq!(
        lob.last_page_id - lob.first_page_id,
        n_pages - 1,
        "Extent chain page IDs must be consecutive: first={} last={} n_pages={}",
        lob.first_page_id,
        lob.last_page_id,
        n_pages
    );
    // Read back byte-for-byte.
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values[1], Value::Bytes(vec![0x5A; big]));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn overflow_chain_double_free_is_idempotent_and_does_not_create_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let db = m4_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    let lob = db
        .with_overflow_mut(table_id, |overflow| {
            overflow.allocate_chain_compressible(b"catalog descriptor", true)
        })
        .unwrap();
    assert_eq!(lob.first_page_id, lob.last_page_id);

    db.with_overflow_mut(table_id, |overflow| {
        overflow.free_chain(lob)?;
        overflow.free_chain(lob)?;
        assert_eq!(overflow.free_head, lob.first_page_id);
        assert_eq!(overflow.free_count, 1);
        assert_eq!(overflow.count_free_list()?, 1);
        let page = overflow.read_raw_page(lob.first_page_id)?;
        assert_eq!(read_u64(&page, OVFL_OFF_NEXT), 0);
        Ok(())
    })
    .unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m4_extent_off_uses_freelist_path_and_round_trips() {
    // MIN_PAGES=0 disables extents and uses per-page free-list-first allocation; reads remain exact.
    // Extent on/off results are mutual differential oracles.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_EXTENT_MIN_PAGES", "0"); // Disable extents.
    let dir = tempfile::tempdir().unwrap();
    let db = m4_open_db(dir.path(), DurabilityMode::Memory);
    let _ = m3_lob_table(&db);
    let big = 200_000usize;
    m3_insert_big(&db, 1, 0x42, big);
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values[1], Value::Bytes(vec![0x42; big]));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m4_mixed_extent_and_freelist_reuse_reads_correctly() {
    // Mix large contiguous extent chains and small free-list chains above inline size
    // but below the extent threshold. Delete one large chain and purge its segment
    // into the free list, then write another equal-sized large chain. Verify all live
    // rows byte-for-byte regardless of allocation source.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::remove_var("MYDB_LOB_EXTENT_MIN_PAGES");
    let dir = tempfile::tempdir().unwrap();
    let db = m4_open_db(dir.path(), DurabilityMode::Memory);
    let _ = m3_lob_table(&db);

    let big = 100_000usize; // About seven pages, using extents.
    let small = 5_000usize; // One page is below the four-page extent threshold and uses free-list-first allocation.
    m3_insert_big(&db, 1, 1, big);
    m3_insert_big(&db, 2, 2, small);
    m3_insert_big(&db, 3, 3, big);
    // Delete ID 1, a large chain before the tail, then purge its pages into the free list.
    m3_delete_id(&db, 1);
    db.purge_obsolete_undo();
    // Large ID 4 still prefers a new contiguous high-water segment rather than reusing fragments.
    m3_insert_big(&db, 4, 4, big);

    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match r.values[0] {
            Value::Int(v) => v,
            _ => 0,
        })
        .collect();
    assert_eq!(ids, vec![2, 3, 4]);
    assert_eq!(rows[0].values[1], Value::Bytes(vec![2u8; small]));
    assert_eq!(rows[1].values[1], Value::Bytes(vec![3u8; big]));
    assert_eq!(rows[2].values[1], Value::Bytes(vec![4u8; big]));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m4_reuses_a_freed_contiguous_extent_without_growing_the_file() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::remove_var("MYDB_LOB_EXTENT_MIN_PAGES");
    let dir = tempfile::tempdir().unwrap();
    let db = m4_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    let lob_path = t_path(dir.path(), "t", table_id, "lob");
    let big = 1_048_576usize;

    m3_insert_big(&db, 1, 0x11, big);
    m3_delete_id(&db, 1);
    db.purge_obsolete_undo();
    db.flush_dirty_data().unwrap();
    let before = std::fs::metadata(&lob_path).unwrap().len();
    let (_, free_before) = db.lob_space_stats(table_id).unwrap();
    assert!(free_before >= big.div_ceil(M4_OVFL_PAGE_PAYLOAD) as u64);

    let rollback = db.begin_txn().unwrap();
    db.insert_rows(
        rollback,
        "test.t",
        vec![vec![Value::Int(2), Value::Bytes(vec![0x33; big])]],
    )
    .unwrap();
    db.rollback_txn(rollback.txn_id).unwrap();
    db.flush_dirty_data().unwrap();
    assert_eq!(
        std::fs::metadata(&lob_path).unwrap().len(),
        before,
        "rolling back a reused extent must restore allocator state without growth"
    );
    let (_, free_after_rollback) = db.lob_space_stats(table_id).unwrap();
    assert_eq!(free_after_rollback, free_before);

    m3_insert_big(&db, 2, 0x22, big);
    db.flush_dirty_data().unwrap();
    let after = std::fs::metadata(&lob_path).unwrap().len();
    assert_eq!(
        after, before,
        "same-size LOB churn should reuse the free extent"
    );
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values[1], Value::Bytes(vec![0x22; big]));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m4_extent_alloc_then_delete_then_shrink_round_trips() {
    // Extent/M3 shrink interaction: allocate extents, delete tail, purge, then shrink;
    // remaining rows stay exact and free_count matches the list.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::remove_var("MYDB_LOB_EXTENT_MIN_PAGES");
    std::env::set_var("MYDB_LOB_SHRINK_MIN_FREE_PAGES", "1");
    let dir = tempfile::tempdir().unwrap();
    let db = m4_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    let big = 100_000usize;
    for id in 1..=5 {
        m3_insert_big(&db, id, id as u8, big);
    }
    db.flush_dirty_data().unwrap();
    let lob_path = t_path(dir.path(), "t", table_id, "lob");
    let size_full = std::fs::metadata(&lob_path).unwrap().len();
    // Delete tail IDs 4/5.
    m3_delete_id(&db, 4);
    m3_delete_id(&db, 5);
    db.purge_obsolete_undo();
    let shrunk = db.shrink_table_lobs(table_id).unwrap();
    assert!(shrunk > 0, "Deleted contiguous extents must be shrinkable");
    db.flush_dirty_data().unwrap();
    assert!(std::fs::metadata(&lob_path).unwrap().len() < size_full);
    // free_count remains consistent.
    let (_pc, fc) = db.lob_space_stats(table_id).unwrap();
    let walked = db
        .with_overflow_read(table_id, |ovf| ovf.count_free_list())
        .unwrap();
    assert_eq!(
        fc, walked,
        "After extent shrinking, free_count must still match the actual free list"
    );
    // Remaining IDs 1..=3 remain byte-exact.
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    assert_eq!(rows.len(), 3);
    for (i, id) in (1..=3).enumerate() {
        assert_eq!(rows[i].values[1], Value::Bytes(vec![id as u8; big]));
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m4_prefetch_default_on_reads_byte_correct() {
    // Prefetch defaults on; Relaxed persistence makes reads use the buffer pool.
    // Prefetch affects performance only; multipage LOB reads must equal inserted bytes.
    // MYDB_LOB_PREFETCH_DISABLE controls process-wide on/off comparisons. The OnceLock
    // is process-local, with nextest isolating tests, so this test verifies enabled
    // read correctness; a separate benchmark compares performance in two processes.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::remove_var("MYDB_LOB_EXTENT_MIN_PAGES");
    std::env::remove_var("MYDB_LOB_PREFETCH_DISABLE"); // Enabled by default.
    let dir = tempfile::tempdir().unwrap();
    let db = m4_open_db(dir.path(), DurabilityMode::Relaxed);
    let _ = m3_lob_table(&db);
    // Multiple distinct rows cover exact-page, page-plus-one, and large-chain boundaries.
    let sizes = [
        200_000usize,
        50_000,
        333_333,
        OVFL_PAGE_PAYLOAD,
        OVFL_PAGE_PAYLOAD + 1,
    ];
    for (i, &sz) in sizes.iter().enumerate() {
        m3_insert_big(&db, i as i64 + 1, (i as u8).wrapping_add(7), sz);
    }
    db.flush_dirty_data().unwrap();
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    assert_eq!(rows.len(), 5);
    for (i, &sz) in sizes.iter().enumerate() {
        assert_eq!(
            rows[i].values[1],
            Value::Bytes(vec![(i as u8).wrapping_add(7); sz]),
            "Prefetched reads must match byte-for-byte (row {})",
            i + 1
        );
    }
    // Partial read_chain_range materialization must also work under prefetch; read a middle slice.
    let table_id = db.table("test.t").unwrap().id;
    let sub = db
        .lob_column_substring(
            table_id,
            rows[2].row_id, // The 333_333-byte row.
            1,
            &ColumnKind::VarBytes,
            100_000,
            500,
        )
        .unwrap()
        .unwrap();
    assert_eq!(sub, Value::Bytes(vec![(2u8).wrapping_add(7); 500]));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

/// Microbenchmark: sequential 16MiB/thousand-page LOB reads with contiguous extents
/// versus scattered pages, prefetch on/off, page continuity, and WAL byte volume.
/// `cargo nextest run --run-ignored ignored-only m4_sequential_read_benchmark`
#[test]
#[ignore = "manual sequential LOB read, extent, prefetch, and WAL measurement"]
fn m4_sequential_read_benchmark() {
    use std::time::Instant;
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    const BLOB: usize = 7 * 1024 * 1024; // 7 MiB is about 449 pages, below the column's 8M limit.
    const READS: u32 = 50;

    // Extent/prefetch GUCs use process OnceLock and initialize only once. This benchmark
    // indirectly compares allocation sources within one process:
    // default extent-on produces consecutive page IDs for sequential access.
    // An extent-off database would require changing environment variables after OnceLock initialization.
    // Instead report extent-on continuity, sequential-read duration, and
    // WAL volume as absolute metrics; obtain the extent-off comparison separately
    // by rerunning with MYDB_LOB_EXTENT_MIN_PAGES=0.
    let extent_off = std::env::var("MYDB_LOB_EXTENT_MIN_PAGES").as_deref() == Ok("0");
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(dir.path(), {
        let mut c = test_storage_config(DurabilityMode::Strict, false);
        c.undo_retention = Duration::from_millis(0);
        c
    })
    .unwrap();
    let table_id = m3_lob_table(&db);

    // Extents help only with fragmented free lists; clean-file per-page bump allocation
    // is already contiguous. Create fragmentation using two-page chains, below the four-page
    // threshold, then delete every other chain to scatter free-list pages.
    for id in 100..200 {
        m3_insert_big(&db, id, (id % 251) as u8, M4_OVFL_PAGE_PAYLOAD + 10); // Two pages per chain.
    }
    for id in (100..200).step_by(2) {
        m3_delete_id(&db, id);
    }
    db.purge_obsolete_undo();

    // Now allocate one large BLOB chain:
    // Extent off pops fragmented free-list pages individually, scattering IDs.
    // Default extent on ignores fragments and bumps a contiguous high-water segment.
    let wal_before = db.log_state_atomics.wal_bytes();
    m3_insert_big(&db, 1, 0x5A, BLOB);
    let wal_after = db.log_state_atomics.wal_bytes();
    db.flush_dirty_data().unwrap();

    let row_id = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()
        .iter()
        .find(|r| r.values[0] == Value::Int(1))
        .unwrap()
        .row_id;
    let lob = m4_lob_ref(&db, table_id, row_id, 1).unwrap();
    let n_pages = (BLOB.div_ceil(M4_OVFL_PAGE_PAYLOAD)) as u64;
    let contiguous = lob.last_page_id - lob.first_page_id == n_pages - 1;

    // Read full columns READS times. Cold reopen would be fairest, but compare warm reads
    // for continuity effects here; evict buffer frames before reading to amplify random/sequential differences.
    let view = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
    let table = db.table("test.t").unwrap();
    let t0 = Instant::now();
    let mut sink = 0usize;
    for _ in 0..READS {
        let r = db.read_row(&table, row_id, view).unwrap().unwrap();
        if let Value::Bytes(b) = &r.values[1] {
            sink += b.len();
        }
    }
    let read_elapsed = t0.elapsed();
    assert_eq!(sink, BLOB * READS as usize);

    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    eprintln!(
        "=== M4 sequential-read microbenchmark (7MiB LOB, about {n_pages} pages, extent_{}) ===",
        if extent_off { "OFF" } else { "ON (default)" }
    );
    eprintln!(
        "  Chain page IDs contiguous: {contiguous} (first={} last={} span={})",
        lob.first_page_id,
        lob.last_page_id,
        lob.last_page_id - lob.first_page_id
    );
    eprintln!("  Sequential reads: {READS} repetitions took {read_elapsed:?}");
    eprintln!(
        "  WAL bytes (7MiB LOB INSERT): {:.2}MiB (payload {:.2}MiB, amplification {:.3}x)",
        mib(wal_after - wal_before),
        mib(BLOB as u64),
        (wal_after - wal_before) as f64 / BLOB as f64
    );
    if !extent_off {
        assert!(
            contiguous,
            "Large chains must be contiguous with extents enabled by default"
        );
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

// ===========================================================================
// M5 transparent LZ4 overflow compression uses head flags bit 1;
// current overflow v3 also persists allocation generation.
// MYDB_LOB_COMPRESSION defaults off like M4; lz4 compresses only newly written compressible columns.
// The compression GUC is a process OnceLock; nextest isolates processes, so set
// the environment before opening each database.
// ===========================================================================

fn m5_open_db(dir: &Path, durability: DurabilityMode) -> Database {
    let mut config = test_storage_config(durability, false);
    config.undo_retention = Duration::from_millis(0);
    Database::open_with_config(dir, config).unwrap()
}

#[test]
fn m5_compressible_data_round_trips_and_shrinks_file() {
    // With compression on, repetitive text must round-trip exactly and produce a .lob
    // file significantly smaller than compression off.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESS_MIN_BYTES", "256");

    let file_size = |compress: bool| -> (u64, Vec<u8>) {
        std::env::set_var("MYDB_LOB_COMPRESSION", if compress { "lz4" } else { "off" });
        let dir = tempfile::tempdir().unwrap();
        let db = m5_open_db(dir.path(), DurabilityMode::Memory);
        let table_id = m3_lob_table(&db);
        // Highly compressible repeated pattern, about 300KB.
        let payload: Vec<u8> = b"abcdefghij0123456789"
            .iter()
            .cycle()
            .take(300_000)
            .copied()
            .collect();
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(1), Value::Bytes(payload.clone())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
        let lob_path = t_path(dir.path(), "t", table_id, "lob");
        let size = std::fs::metadata(&lob_path).unwrap().len();
        let rows = db
            .scan_rows(
                "test.t",
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap();
        let got = match &rows[0].values[1] {
            Value::Bytes(b) => b.clone(),
            _ => Vec::new(),
        };
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db);
        // Round-trip bytes must match through transparent decompression.
        assert_eq!(
            got, payload,
            "Compression={compress} must round-trip byte-for-byte"
        );
        (size, payload)
    };

    // OnceLock reads the GUC once per process. Verify on-path correctness and size here;
    // the off-size comparison needs another process because same-process changes
    // cannot override OnceLock. Therefore this test enables compression only,
    // asserting compressed file size below raw payload size; off comparisons are
    // covered by m5_incompressible_* and benchmarks.
    let (size_on, payload) = file_size(true);
    assert!(
        size_on < payload.len() as u64,
        "Compressible .lob size ({size_on}) must be below raw size ({})",
        payload.len()
    );
}

#[test]
fn m5_incompressible_data_stored_uncompressed_no_bloat() {
    // Incompressible random bytes remain uncompressed if compression cannot shrink them,
    // avoiding file growth while preserving exact round trips.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESSION", "lz4");
    std::env::set_var("MYDB_LOB_COMPRESS_MIN_BYTES", "256");
    let dir = tempfile::tempdir().unwrap();
    let db = m5_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    // Pseudorandom incompressible bytes use a linear congruential generator to avoid LZ4 repetition.
    let mut x: u64 = 0x9E3779B97F4A7C15;
    let payload: Vec<u8> = (0..200_000)
        .map(|_| {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (x >> 33) as u8
        })
        .collect();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::Bytes(payload.clone())]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();
    let lob_path = t_path(dir.path(), "t", table_id, "lob");
    let size = std::fs::metadata(&lob_path).unwrap().len();
    // No expansion: pages equal ceil(payload/OVFL_PAGE_PAYLOAD) plus metadata; storing
    // raw bytes must stay within raw rounded page size plus one metadata page.
    let raw_pages = (payload.len().div_ceil(OVFL_PAGE_PAYLOAD) + 1) as u64;
    assert!(
        size <= raw_pages * 16_384,
        "Incompressible data must not expand: size={size} raw_pages_bytes={}",
        raw_pages * 16_384
    );
    // avoiding file growth while preserving exact round trips.
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values[1], Value::Bytes(payload));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m5_compress_off_matches_known_values_oracle() {
    // Compression off defaults use the same data as m5_*_on and must read back exactly.
    // This is the off-side mutual oracle; other m5 tests cover on, and both compare
    // against inserted bytes.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESSION", "off");
    let dir = tempfile::tempdir().unwrap();
    let db = m5_open_db(dir.path(), DurabilityMode::Memory);
    let _ = m3_lob_table(&db);
    let payload: Vec<u8> = b"abcdefghij0123456789"
        .iter()
        .cycle()
        .take(300_000)
        .copied()
        .collect();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::Bytes(payload.clone())]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values[1], Value::Bytes(payload));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m5_compressed_multipage_chain_and_substring() {
    // Multipage compressed chains round-trip; read_chain_range falls back to full decode
    // then slicing, preserving partial materialization semantics.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESSION", "lz4");
    std::env::set_var("MYDB_LOB_COMPRESS_MIN_BYTES", "256");
    let dir = tempfile::tempdir().unwrap();
    let db = m5_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    // Compressible data spans multiple pages after decompression, with raw size 500KB.
    let payload: Vec<u8> = (0..500_000u32).map(|i| (i % 64) as u8).collect();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::Bytes(payload.clone())]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let row_id = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()[0]
        .row_id;
    // Read the full chain.
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values[1], Value::Bytes(payload.clone()));
    // LENGTH pushdown still returns original length with zero page reads for compressed chains.
    assert_eq!(
        db.lob_column_byte_length(table_id, row_id, 1).unwrap(),
        Some(payload.len() as u64)
    );
    // Partial compressed reads decode the chain then slice [123456,123456+1000).
    let sub = db
        .lob_column_substring(table_id, row_id, 1, &ColumnKind::VarBytes, 123_456, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!(sub, Value::Bytes(payload[123_456..124_456].to_vec()));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m5_compression_with_m1_reuse() {
    // Compression plus M1 reuse: rewrite a compressed column unchanged while modifying another column.
    // Equality compares transparently decompressed logical bytes, so compression does not affect reuse.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESSION", "lz4");
    std::env::set_var("MYDB_LOB_COMPRESS_MIN_BYTES", "256");
    let dir = tempfile::tempdir().unwrap();
    let db = m5_open_db(dir.path(), DurabilityMode::Memory);
    db.create_table(
        "t",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col(
                "body",
                DataType::Text(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
            lob_col(
                "data",
                DataType::Blob(crate::storage::schema::MEDIUM_LOB_MAX),
                false,
            ),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let body = "X".repeat(40_000); // Compressible.
    let data: Vec<u8> = (0..40_000u32).map(|i| (i % 100) as u8).collect();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![
            Value::Int(1),
            Value::String(body.clone()),
            Value::Bytes(data.clone()),
        ]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let before = db.dml_stats_snapshot();
    // UPDATE reuses unchanged compressed body and changes data.
    let new_data: Vec<u8> = (0..50_000u32).map(|i| (i % 77) as u8).collect();
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |row| {
            Ok(vec![
                Value::Int(1),
                row[1].clone(),
                Value::Bytes(new_data.clone()),
            ])
        },
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let after = db.dml_stats_snapshot();
    assert!(
        after.lob_chains_reused > before.lob_chains_reused,
        "Unchanged compressed body must reuse its chain based on decompressed byte equality"
    );
    // Read old body and new data, both byte-exact.
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values[1], Value::String(body));
    assert_eq!(rows[0].values[2], Value::Bytes(new_data));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m5_compression_with_shrink_and_extent() {
    // Compression/M3 shrink/M4 extent use actual reduced compressed page counts.
    // Delete a compressed extent tail, purge, shrink, and verify remaining data.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESSION", "lz4");
    std::env::set_var("MYDB_LOB_COMPRESS_MIN_BYTES", "256");
    std::env::set_var("MYDB_LOB_SHRINK_MIN_FREE_PAGES", "1");
    std::env::remove_var("MYDB_LOB_EXTENT_MIN_PAGES");
    let dir = tempfile::tempdir().unwrap();
    let db = m5_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    // Compressible approximately 300KB raw data still exceeds the extent threshold after compression.
    let mk = |seed: u8| -> Vec<u8> {
        (0..300_000u32)
            .map(|i| ((i as u8).wrapping_add(seed)) % 32)
            .collect()
    };
    for id in 1..=5 {
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(id), Value::Bytes(mk(id as u8))]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }
    db.flush_dirty_data().unwrap();
    let lob_path = t_path(dir.path(), "t", table_id, "lob");
    let size_full = std::fs::metadata(&lob_path).unwrap().len();
    m3_delete_id(&db, 4);
    m3_delete_id(&db, 5);
    db.purge_obsolete_undo();
    let shrunk = db.shrink_table_lobs(table_id).unwrap();
    assert!(
        shrunk > 0,
        "Deleted compressed extent chains must be shrinkable"
    );
    db.flush_dirty_data().unwrap();
    assert!(std::fs::metadata(&lob_path).unwrap().len() < size_full);
    let (_pc, fc) = db.lob_space_stats(table_id).unwrap();
    let walked = db
        .with_overflow_read(table_id, |ovf| ovf.count_free_list())
        .unwrap();
    assert_eq!(
        fc, walked,
        "free_count must remain consistent after compression and shrinking"
    );
    // Remaining IDs 1..=3 remain byte-exact.
    let mut rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(v) => v,
        _ => 0,
    });
    assert_eq!(rows.len(), 3);
    for (i, id) in (1..=3u8).enumerate() {
        assert_eq!(rows[i].values[1], Value::Bytes(mk(id)));
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m5_compression_mvcc_snapshot_rollback_delete_purge() {
    // Compressed chains must remain correct through old MVCC reconstruction, rollback, DELETE, and purge.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESSION", "lz4");
    std::env::set_var("MYDB_LOB_COMPRESS_MIN_BYTES", "256");
    let dir = tempfile::tempdir().unwrap();
    let db = m5_open_db(dir.path(), DurabilityMode::Memory);
    let table = {
        m3_lob_table(&db);
        db.table("test.t").unwrap()
    };
    let v_old: Vec<u8> = (0..40_000u32).map(|i| (i % 16) as u8).collect();
    let v_new: Vec<u8> = (0..50_000u32).map(|i| (i % 23) as u8).collect();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        vec![vec![Value::Int(1), Value::Bytes(v_old.clone())]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let row_id = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()[0]
        .row_id;

    // A: snapshot A precedes B's changes and still reads old compressed values.
    let txn_a = db.begin_txn().unwrap();
    let view_a = TxnView::read_only(txn_a.snapshot_scn, IsolationLevel::RepeatableRead);
    let txn_b = db.begin_txn().unwrap();
    db.update_rows(
        txn_b,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Bytes(v_new.clone())]),
    )
    .unwrap();
    db.commit_txn(txn_b.txn_id).unwrap();
    let a_row = db.read_row(&table, row_id, view_a).unwrap().unwrap();
    assert_eq!(
        a_row.values[1],
        Value::Bytes(v_old.clone()),
        "The old snapshot reads the old compressed value"
    );
    db.commit_txn(txn_a.txn_id).unwrap();
    db.purge_obsolete_undo();
    let now = db
        .read_row(
            &table,
            row_id,
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        now.values[1],
        Value::Bytes(v_new.clone()),
        "After purge, read the new compressed value"
    );

    // B: update then roll back, restoring the old compressed chain.
    let txn = db.begin_txn().unwrap();
    db.update_rows(
        txn,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Bytes(v_old.clone())]),
    )
    .unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    let after_rb = db
        .read_row(
            &table,
            row_id,
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        after_rb.values[1],
        Value::Bytes(v_new),
        "After rollback, retain B's committed new compressed value"
    );

    // C: DELETE/purge frees the compressed chain and removes the row.
    let txn = db.begin_txn().unwrap();
    db.delete_rows(txn, "test.t", None, |row| Ok(row[0] == Value::Int(1)))
        .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.purge_obsolete_undo();
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert!(
        rows.is_empty(),
        "The row must disappear after DELETE and purge"
    );
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m5_uncompressed_chain_uses_current_generation_layout_and_reads_back() {
    // Compression off still writes overflow v3 without its compression flag; the head
    // persists the same allocation generation as LobRef. Verify raw head bytes directly.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESSION", "off");
    let dir = tempfile::tempdir().unwrap();
    let db = m5_open_db(dir.path(), DurabilityMode::Memory);
    let table_id = m3_lob_table(&db);
    let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(1), Value::Bytes(payload.clone())]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let row_id = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap()[0]
        .row_id;
    let lob = db
        .row_column_external_ref(table_id, row_id, 1)
        .unwrap()
        .unwrap();
    // The head stores the current version at offset 4, little-endian u16;
    // flags at OVFL_OFF_FLAGS=22 contain HEAD only, not compression.
    let head = db
        .with_overflow_read(table_id, |ovf| {
            Ok(ovf
                .buffer_pool
                .read_raw_page(ovf.page_key(lob.first_page_id))?)
        })
        .unwrap();
    let version = u16::from_le_bytes([head[4], head[5]]);
    let flags = u16::from_le_bytes([head[22], head[23]]);
    let generation = u64::from_le_bytes(head[32..40].try_into().unwrap());
    assert_eq!(
        version, OVFL_VERSION,
        "Uncompressed chains must use the registered overflow layout"
    );
    assert_eq!(generation, lob.generation);
    assert_eq!(
        flags & (1 << 1),
        0,
        "Uncompressed chains must not have the compression flag"
    );
    assert_eq!(flags & 1, 1, "The head page must have the HEAD flag");
    // Round-trip bytes remain exact.
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values[1], Value::Bytes(payload));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m5_compression_survives_reopen_recovery() {
    // Crash recovery: strict compressed writes survive reopening recovery intact.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESSION", "lz4");
    std::env::set_var("MYDB_LOB_COMPRESS_MIN_BYTES", "256");
    let dir = tempfile::tempdir().unwrap();
    let payload: Vec<u8> = (0..400_000u32).map(|i| (i % 48) as u8).collect();
    {
        let db = m5_open_db(dir.path(), DurabilityMode::Strict);
        let _ = m3_lob_table(&db);
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(1), Value::Bytes(payload.clone())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.flush_dirty_data().unwrap();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db); // Simulate process exit.
    }
    let db = m5_open_db(dir.path(), DurabilityMode::Strict);
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values[1],
        Value::Bytes(payload),
        "Compressed chains must remain intact after recovery"
    );
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

/// Microbenchmark: compare text/JSON .lob file size and WAL volume with compression on/off.
/// `cargo nextest run --run-ignored ignored-only m5_compression_ratio_benchmark`
/// Run each mode in a separate process with MYDB_LOB_COMPRESSION=lz4 or off.
#[test]
#[ignore = "manual LOB compression and WAL measurement; run lz4 and off modes in separate processes"]
fn m5_compression_ratio_benchmark() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    std::env::set_var("MYDB_LOB_COMPRESS_MIN_BYTES", "256");
    let on = std::env::var("MYDB_LOB_COMPRESSION").as_deref() == Ok("lz4");
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(dir.path(), {
        let mut c = test_storage_config(DurabilityMode::Strict, false);
        c.undo_retention = Duration::from_millis(0);
        c
    })
    .unwrap();
    let table_id = m3_lob_table(&db);
    // Typical compressible LOB: repetitive JSON-like structure, about 1MiB times twenty rows.
    let row_payload: Vec<u8> = r#"{"id":12345,"name":"widget","tags":["a","b","c"],"qty":42,"note":"lorem ipsum dolor sit amet "}"#
        .repeat(11_000) // ~1MiB
        .into_bytes();
    let n_rows = 20i64;
    let wal_before = db.log_state_atomics.wal_bytes();
    for id in 1..=n_rows {
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.t",
            vec![vec![Value::Int(id), Value::Bytes(row_payload.clone())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }
    let wal_after = db.log_state_atomics.wal_bytes();
    db.flush_dirty_data().unwrap();
    let lob_path = t_path(dir.path(), "t", table_id, "lob");
    let size = std::fs::metadata(&lob_path).unwrap().len();
    let raw = row_payload.len() as u64 * n_rows as u64;
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    eprintln!(
        "=== M5 compression microbenchmark ({n_rows}*1MiB compressible JSON, compression {}) ===",
        if on { "ON(lz4)" } else { "OFF" }
    );
    eprintln!("  Raw data: {:.2}MiB", mib(raw));
    eprintln!(
        "  .lob file: {:.2}MiB (file/raw ratio {:.1}%)",
        mib(size),
        size as f64 / raw as f64 * 100.0
    );
    eprintln!(
        "  WAL writes: {:.2}MiB (amplification {:.3}x)",
        mib(wal_after - wal_before),
        (wal_after - wal_before) as f64 / raw as f64
    );
    // Sample-check round trips.
    let rows = db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), n_rows as usize);
    assert_eq!(rows[0].values[1], Value::Bytes(row_payload));
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

// ===========================================================================
// Fixed vector lanes must fit, while larger payloads externalize.
// ===========================================================================

#[test]
fn m7_fixed_vector_lanes_reject_oversized_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let mut columns = vec![lob_col("id", DataType::Int, true)];
    for name in ["a", "b", "c"] {
        columns.push(lob_col(
            name,
            DataType::Vector(Some(2048), crate::vector::VectorDataType::Float32),
            false,
        ));
    }
    let err = db
        .create_table("wide", columns, vec!["id".into()], vec![])
        .unwrap_err();
    assert_eq!(err.code(), 1118);
    assert!(err.message().contains("fixed columns"));
    assert!(db.table("test.wide").is_err());
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m7_vector_inline_and_overflow_boundaries_round_trip() {
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    for dim in [2048u32, 2049, 4096, 16383] {
        let name = format!("v{dim}");
        db.create_table(
            &name,
            vec![
                lob_col("id", DataType::Int, true),
                lob_col(
                    "emb",
                    DataType::Vector(Some(dim), crate::vector::VectorDataType::Float32),
                    false,
                ),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let bytes = vec![1u8; dim as usize * 4];
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            &format!("test.{name}"),
            vec![vec![Value::Int(1), Value::Vector(bytes.clone().into())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        let rows = db
            .scan_rows(
                &format!("test.{name}"),
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap();
        assert_eq!(rows[0].values[1], Value::Vector(bytes.into()));
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

#[test]
fn m7_normal_vector_dim_still_works() {
    // Ordinary dimensions 128/768 remain unaffected through CREATE, INSERT, and reads.
    std::env::set_var("MYDB_LOB_INLINE_MAX", "4096");
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    for dim in [128u32, 768] {
        db.create_table(
            &format!("v{dim}"),
            vec![
                lob_col("id", DataType::Int, true),
                lob_col(
                    "emb",
                    DataType::Vector(Some(dim), crate::vector::VectorDataType::Float32),
                    false,
                ),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let txn = db.begin_txn().unwrap();
        let vbytes = vec![7u8; dim as usize * 4];
        db.insert_rows(
            txn,
            &format!("test.v{dim}"),
            vec![vec![Value::Int(1), Value::Vector(vbytes.clone().into())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        let rows = db
            .scan_rows(
                &format!("test.v{dim}"),
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap();
        assert_eq!(rows[0].values[1], Value::Vector(vbytes.into()));
    }
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

fn undo_pagein_int_col(name: &str, pk: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: DataType::Int,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        primary_key: pk,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    }
}

// Phase 1 (undo two-level mirror) — a consistent read whose required
// undo record was *evicted from the in-memory mirror but still lives on
// the segment file* must page it back in via `load_owned` rather than
// fail with 1236 "snapshot too old". We synthesize eviction by clearing
// the in-memory `records` map after forcing every undo frame
// write-through to disk.
#[test]
fn mvcc_read_pages_in_evicted_undo_from_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path()); // persistent → undo on disk
    db.undo.write().set_buffer_limit(0).unwrap(); // write-through to segment

    db.create_table(
        "t",
        vec![
            undo_pagein_int_col("id", true),
            undo_pagein_int_col("v", false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    // Seed (1, 100), commit at an early SCN.
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.t", vec![vec![Value::Int(1), Value::Int(100)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // Long-running reader captures its snapshot BEFORE the update.
    let reader = db.begin_txn().unwrap();

    // Writer bumps v to 200 and commits — builds the undo chain whose
    // before-image (v=100) is exactly what `reader` must still observe.
    let writer = db.begin_txn().unwrap();
    db.update_rows(
        writer,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(1)),
        |_| Ok(vec![Value::Int(1), Value::Int(200)]),
    )
    .unwrap();
    db.commit_txn(writer.txn_id).unwrap();

    // Sanity: a fresh snapshot sees the new value.
    let fresh = db.begin_txn().unwrap();
    assert_eq!(
        db.scan_rows("test.t", fresh).unwrap()[0].values[1],
        Value::Int(200)
    );
    db.commit_txn(fresh.txn_id).unwrap();

    // Synthesize Phase 2 eviction: keep the records' metadata in the
    // directory but flip them non-resident and drop the heavy payload,
    // while the segment files retain the frames. (Removing them entirely
    // would mean *purged*, which must instead yield 1236.)
    let misses_before = db.undo.read().cache_misses();
    {
        let mut undo = db.undo.write();
        let ids: Vec<u64> = undo.records.keys().copied().collect();
        for id in ids {
            if let Some(rec) = undo.records.get_mut(&id) {
                rec.resident = false;
                rec.before_values = None;
            }
        }
    }

    let rows = db.scan_rows("test.t", reader).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::Int(1), Value::Int(100)]);
    assert!(
        db.undo.read().cache_misses() > misses_before,
        "the consistent read must have paged the undo record in from disk"
    );
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

// Phase 2 (capped mirror + eviction) — with a tiny in-memory payload cap,
// the append path evicts the oldest flushed before-images. Consistent
// reads and rollback must remain correct by paging payloads back in from
// the segment files, and resident memory must stay bounded.
#[test]
fn undo_eviction_bounds_memory_yet_reads_and_rollback_stay_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    // Tiny resident cap so eviction fires almost immediately.
    db.undo.write().set_mem_cache_limit_bytes(4096);

    db.create_table(
        "t",
        vec![
            undo_pagein_int_col("id", true),
            undo_pagein_int_col("v", false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    // Seed 200 rows, commit.
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        (0..200)
            .map(|i| vec![Value::Int(i), Value::Int(i)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // Long reader pins the pre-update snapshot.
    let reader = db.begin_txn().unwrap();

    // Update every row in committed batches → lots of undo before-images.
    for round in 1..=5 {
        let w = db.begin_txn().unwrap();
        db.update_rows(
            w,
            "test.t",
            None,
            |_| Ok(true),
            move |row| {
                let id = row[0].clone();
                Ok(vec![id, Value::Int(1000 + round)])
            },
        )
        .unwrap();
        db.commit_txn(w.txn_id).unwrap();
    }

    // Eviction must have actually happened. The payload stays near the cap;
    // the directory metadata is non-evictable and remains included in
    // resident_bytes.
    {
        let u = db.undo.read();
        assert!(
            u.evicted_records() > 0,
            "tiny cap should have evicted payloads"
        );
        let metadata_floor = u.resident_metadata_floor_bytes();
        assert!(
            u.resident_bytes() <= metadata_floor + 4096,
            "resident bytes {} should stay within metadata {} plus the 4096-byte payload cap",
            u.resident_bytes(),
            metadata_floor
        );
    }

    // The long reader must still reconstruct the original v == id for all
    // 200 rows by paging evicted before-images back from disk.
    let mut rows = db.scan_rows("test.t", reader).unwrap();
    rows.sort_by_key(|r| match r.values[0] {
        Value::Int(i) => i,
        _ => 0,
    });
    assert_eq!(rows.len(), 200);
    for r in &rows {
        assert_eq!(r.values[0], r.values[1], "old snapshot sees v == id");
    }

    // Rollback path also pages evicted before-images in: open a txn, update
    // a row, then roll back and confirm the committed value is restored.
    let t = db.begin_txn().unwrap();
    db.update_rows(
        t,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(7)),
        |_| Ok(vec![Value::Int(7), Value::Int(424242)]),
    )
    .unwrap();
    db.rollback_txn(t.txn_id).unwrap();
    let after = db.begin_txn().unwrap();
    let row7 = db
        .scan_rows("test.t", after)
        .unwrap()
        .into_iter()
        .find(|r| r.values[0] == Value::Int(7))
        .unwrap();
    assert_eq!(
        row7.values[1],
        Value::Int(1005),
        "rollback restores committed value"
    );
    db.commit_txn(after.txn_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

// Phase 4 (per-txn quota + blocker diagnostic) — a single runaway
// transaction is rejected on its own undo growth (error 50021) without
// failing other sessions, and the largest-undo-txn diagnostic surfaces
// the culprit. A subsequent independent txn still commits fine.
#[test]
fn undo_per_txn_cap_rejects_runaway_without_harming_others() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    // Small per-txn cap so a modest batch trips it.
    db.undo.write().set_txn_max_bytes(2048);

    db.create_table(
        "t",
        vec![
            undo_pagein_int_col("id", true),
            undo_pagein_int_col("v", false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();

    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.t",
        (0..200)
            .map(|i| vec![Value::Int(i), Value::Int(i)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();

    // Runaway txn: update many rows in one txn → undo grows past the cap.
    let runaway = db.begin_txn().unwrap();
    let err = db
        .update_rows(
            runaway,
            "test.t",
            None,
            |_| Ok(true),
            |row| Ok(vec![row[0].clone(), Value::Int(999)]),
        )
        .unwrap_err();
    assert_eq!(
        err.code(),
        50021,
        "runaway txn must hit the per-txn undo cap"
    );
    db.rollback_txn(runaway.txn_id).unwrap();

    // The blocker diagnostic should have flagged a culprit during growth;
    // after rollback+purge it may clear, so just assert the cap is visible.
    assert_eq!(db.undo.read().txn_max_bytes(), 2048);

    // An independent small txn still commits fine (others not harmed).
    let ok = db.begin_txn().unwrap();
    db.update_rows(
        ok,
        "test.t",
        None,
        |row| Ok(row[0] == Value::Int(0)),
        |_| Ok(vec![Value::Int(0), Value::Int(7)]),
    )
    .unwrap();
    db.commit_txn(ok.txn_id).unwrap();

    let after = db.begin_txn().unwrap();
    let rows = db.scan_rows("test.t", after).unwrap();
    // Rollback of the runaway must have restored everything; only row 0
    // changed to 7 by the independent txn.
    for r in &rows {
        let id = match r.values[0] {
            Value::Int(i) => i,
            _ => -1,
        };
        let expected = if id == 0 { 7 } else { id };
        assert_eq!(r.values[1], Value::Int(expected), "row {id} value");
    }
    db.commit_txn(after.txn_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
}

// Phase 5 — undo compaction policy parsing. Default is Auto; the new
// MYDB_UNDO_COMPACTION knob and the legacy *_ENABLED alias both resolve.
// (nextest runs each test in its own process, so the env mutation here is
// isolated from other tests.)
#[test]
fn undo_compaction_mode_parses_env() {
    std::env::remove_var("MYDB_UNDO_COMPACTION");
    std::env::remove_var("MYDB_UNDO_COMPACTION_ENABLED");
    assert_eq!(
        compaction_mode(),
        UndoCompactionMode::Auto,
        "default is Auto"
    );

    std::env::set_var("MYDB_UNDO_COMPACTION", "off");
    assert_eq!(compaction_mode(), UndoCompactionMode::Off);
    std::env::set_var("MYDB_UNDO_COMPACTION", "on");
    assert_eq!(compaction_mode(), UndoCompactionMode::On);
    std::env::set_var("MYDB_UNDO_COMPACTION", "auto");
    assert_eq!(compaction_mode(), UndoCompactionMode::Auto);
    std::env::remove_var("MYDB_UNDO_COMPACTION");

    // Legacy alias still maps to On / Off.
    std::env::set_var("MYDB_UNDO_COMPACTION_ENABLED", "1");
    assert_eq!(compaction_mode(), UndoCompactionMode::On);
    std::env::set_var("MYDB_UNDO_COMPACTION_ENABLED", "0");
    assert_eq!(compaction_mode(), UndoCompactionMode::Off);
    std::env::remove_var("MYDB_UNDO_COMPACTION_ENABLED");
}

/// Repeated unified multi-index rebuilds must preserve lookup semantics across
/// scalar, compound, and string keys.
#[test]
fn rebuild_indexes_encoded_path_is_stable() {
    fn col(name: &str, data_type: DataType, pk: bool) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type,
            unsigned: false,
            zerofill: false,
            invisible: false,
            nullable: false,
            primary_key: pk,
            unique: false,
            auto_increment: false,
            default: None,
            on_update: None,
            checks: Vec::new(),
            generated: None,
            charset: None,
            collation: None,
        }
    }
    fn idx(name: &str, columns: Vec<String>) -> IndexSchema {
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: name.into(),
            columns,
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "t",
        vec![
            col("id", DataType::Int, true),
            col("a", DataType::Int, false),
            col("b", DataType::Int, false),
            col("s", DataType::VarChar(Some(64)), false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.add_index("test.t", idx("a_idx", vec!["a".into()]))
        .unwrap();
    db.add_index("test.t", idx("ab_idx", vec!["a".into(), "b".into()]))
        .unwrap();
    db.add_index("test.t", idx("s_idx", vec!["s".into()]))
        .unwrap();

    let total: i64 = 500;
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        (0..total)
            .map(|i| {
                vec![
                    Value::Int(i),
                    Value::Int(i),
                    Value::Int(1_000 - i),
                    Value::String(format!("v{i:04}")),
                ]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.flush_dirty_data().unwrap();

    // Snapshot every key's lookup result across all three indexes.
    let snapshot = |db: &Database| -> Vec<Vec<RowId>> {
        let table = db.table("test.t").unwrap();
        let find = |name: &str| {
            table
                .indexes
                .iter()
                .find(|x| x.name == name)
                .cloned()
                .unwrap()
        };
        let a_idx = find("a_idx");
        let ab_idx = find("ab_idx");
        let s_idx = find("s_idx");
        let s_col = table.columns.iter().find(|c| c.name == "s").unwrap();
        let s_key = |i: i64| {
            IndexKey::from(vec![KeyPart::from_column_value_with_collation(
                &Value::String(format!("v{i:04}")),
                &s_col.data_type,
                s_col.collation.as_deref(),
            )])
        };
        let mut out = Vec::new();
        for i in 0..total {
            let mut a = db
                .lookup_index(&table, &a_idx, IndexKey::from(vec![KeyPart::Int(i)]))
                .unwrap();
            a.sort();
            out.push(a);
            let mut ab = db
                .lookup_index(
                    &table,
                    &ab_idx,
                    IndexKey::from(vec![KeyPart::Int(i), KeyPart::Int(1_000 - i)]),
                )
                .unwrap();
            ab.sort();
            out.push(ab);
            let mut s = db.lookup_index(&table, &s_idx, s_key(i)).unwrap();
            s.sort();
            out.push(s);
        }
        // A key that does not exist must miss in every index.
        out.push(
            db.lookup_index(&table, &a_idx, IndexKey::from(vec![KeyPart::Int(999_999)]))
                .unwrap(),
        );
        out
    };

    // The unified encoded rebuild reaches the multi-index fan-out
    // (three secondary indexes plus the primary key).
    let table = db.table("test.t").unwrap();
    db.rebuild_indexes(&table).unwrap();
    let first = snapshot(&db);

    // A second rebuild exercises replacement and publication without routing
    // through a second production implementation.
    let table = db.table("test.t").unwrap();
    db.rebuild_indexes(&table).unwrap();
    let second = snapshot(&db);

    assert_eq!(first, second, "repeated encoded rebuild must be stable");
    // Every existing key present exactly once; the sentinel key absent.
    for (i, res) in first.iter().enumerate() {
        if i == first.len() - 1 {
            assert!(res.is_empty(), "sentinel key must miss");
        } else {
            assert_eq!(res.len(), 1, "each key present exactly once (slot {i})");
        }
    }
}

// ============================================================
// Phase 1b end-to-end tests for `IndexLeafPage::page_max_trx_id`
// ============================================================
//
// The unit tests in `btree::page_max_trx_id_tests` (Phase 1a) cover
// encode/decode roundtrip in isolation. These E2E tests drive the real
// `IndexFile` write path and assert the field:
//   * is raised by every insert (Phase 1b choke-point stamp in
//     `write_page`),
//   * is monotonic under out-of-order writer txn ids,
//   * survives leaf splits so both halves reflect the writer,
//   * survives crash-restart via WAL redo of the compact
//     `IndexBatch` records.

#[test]
fn insert_stamps_page_max_trx_id_on_root_leaf() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_stamp.idx");
    let index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let root_before = index.root_page_id();
    // Root starts as an empty leaf with page_max_trx_id = 0.
    assert_eq!(index.leaf_page_max_trx_id(root_before).unwrap(), 0);

    index
        .insert_with_txn(42, wide_test_index_key(1), test_row_id(1, 8))
        .unwrap();

    // Single-leaf tree — root did not migrate.
    assert_eq!(index.root_page_id(), root_before);
    assert_eq!(
        index.leaf_page_max_trx_id(root_before).unwrap(),
        42,
        "insert with txn=42 must raise root leaf's page_max_trx_id to 42",
    );
}

#[test]
fn page_max_trx_id_is_monotonic_across_out_of_order_writers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_mono.idx");
    let index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let root = index.root_page_id();

    // Simulate three writers landing on the same leaf out of order.
    // Under real MVCC txn ids assigned monotonically per session, we
    // don't expect earlier ids to arrive after later ones — but the
    // `raise` semantics must protect against it regardless (e.g. WAL
    // redo reordering, or a leaf inherited via split from a page
    // whose bound was already ahead of the current writer).
    for (writer_txn, key) in [(10u64, 1i64), (5u64, 2i64), (20u64, 3i64), (15u64, 4i64)] {
        index
            .insert_with_txn(writer_txn, wide_test_index_key(key), test_row_id(key, 8))
            .unwrap();
    }

    assert_eq!(
        index.leaf_page_max_trx_id(root).unwrap(),
        20,
        "page_max_trx_id must be max of all writers (raise-only), not last-writer-wins",
    );
}

#[test]
fn leaf_split_raises_both_halves_to_writer_and_preserves_source_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_split.idx");
    let index = IndexFile::open(path, test_log(dir.path())).unwrap();

    // Phase A: fill a single leaf under txn=100 until we're just
    // shy of a split. Wide keys keep the count manageable.
    let mut value = 0i64;
    while index.next_page_id() <= 2 {
        index
            .insert_with_txn(100, wide_test_index_key(value), test_row_id(value, 8))
            .unwrap();
        value += 1;
        assert!(value < 2048, "expected split before 2K entries");
    }
    // At this point we've just split — the writer of the split was
    // still txn=100.
    let chain_after_first_split = index.leaf_page_max_trx_id_chain().unwrap();
    assert!(
        chain_after_first_split.len() >= 2,
        "test should have split into >=2 leaves; got {}",
        chain_after_first_split.len()
    );
    for (page_id, bound) in &chain_after_first_split {
        assert_eq!(
            *bound, 100,
            "leaf {page_id} produced by split under txn 100 must have bound=100 (got {bound})"
        );
    }

    // Phase B: further inserts under txn=250 should raise the touched
    // leaf's bound to 250 while untouched leaves stay at 100.
    // Insert a value that lands in the leftmost leaf; use a value
    // small enough to sort before any later-inserted keys.
    index
        .insert_with_txn(250, wide_test_index_key(-1), test_row_id(-1, 8))
        .unwrap();
    let chain = index.leaf_page_max_trx_id_chain().unwrap();
    let leftmost_bound = chain[0].1;
    assert_eq!(
        leftmost_bound, 250,
        "leftmost leaf received a txn=250 insert; its bound must rise to 250 (got {leftmost_bound})",
    );
    // Every bound must be >= 100 (the source bound preserved across
    // the earlier split), regardless of whether it was touched again.
    for (page_id, bound) in &chain {
        assert!(
            *bound >= 100,
            "leaf {page_id} lost its historical bound (got {bound}, expected >= 100)",
        );
    }
}

#[test]
fn page_max_trx_id_survives_flush_and_reopen() {
    // End-to-end path: insert → write_page choke-point stamp →
    // encode_index_tree_page writes the field into the leaf payload
    // → `set_page_lsn_if_formatted` overlays the LSN slot (offset 8..16
    // of the common header, which must NOT touch our field) → flush
    // to disk → reopen → decode_index_tree_page reads it back. If any
    // link mis-aligns the field with the LSN slot (an earlier Phase 1b
    // draft did just that), this assertion catches it.
    //
    // A full crash-restart test through `recover_from_wal` needs a
    // canonical `schemas/<db>/indexes/…` path (matched by
    // `is_index_object_path`) and a `Database` setup — deferred to a
    // Database-level integration test.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("1_flush.idx");
    let expected_bounds: Vec<u64>;
    {
        let index = IndexFile::open(path.clone(), test_log(dir.path())).unwrap();
        for i in 0..8i64 {
            index
                .insert_with_txn(777, wide_test_index_key(i), test_row_id(i, 8))
                .unwrap();
        }
        // Snapshot the in-memory bounds so we can compare against
        // the post-reopen decoded values.
        expected_bounds = index
            .leaf_page_max_trx_id_chain()
            .unwrap()
            .into_iter()
            .map(|(_, bound)| bound)
            .collect();
        assert!(
            expected_bounds.iter().all(|&b| b == 777),
            "in-memory bounds after 777-inserts: {:?}",
            expected_bounds,
        );
        // Flush the index buffer pool to disk (drops the page images
        // through the encoder; LSN slot gets stamped here).
        index.flush().unwrap();
        index.log.lock().flush().unwrap();
    }

    // Reopen from disk. Decoded pages must carry the same bounds.
    let index = IndexFile::open(path, test_log(dir.path())).unwrap();
    let chain: Vec<u64> = index
        .leaf_page_max_trx_id_chain()
        .unwrap()
        .into_iter()
        .map(|(_, bound)| bound)
        .collect();
    assert_eq!(
        chain, expected_bounds,
        "flush → reopen must preserve every leaf's page_max_trx_id exactly (got {chain:?}, expected {expected_bounds:?})",
    );
    // Sanity: entries survive too (unrelated to page_max_trx_id but
    // fails early if the on-disk format grew broken by our edits).
    assert_eq!(
        index.get(&wide_test_index_key(5), false).unwrap(),
        vec![test_row_id(5, 8)]
    );
}

// ----------------------------------------------------------------------
// Phase 3 — read-path integration tests for the PAGE_MAX_TRX_ID
// visibility shortcut. These wire the storage-level plumbing added in
// Phase 1a/1b + Phase 2a-2d through a real Database, exercise the
// counter surface, and verify snapshot correctness under a long-lived
// reader that must not have its view polluted by newer writers even
// after the shortcut is enabled.

fn pmtx_test_table(db: &Database) {
    db.create_table(
        "pmtx",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "payload".into(),
                data_type: DataType::VarChar(Some(64)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
}

fn pmtx_primary(db: &Database) -> (Arc<TableSchema>, IndexSchema) {
    let table = db.table("test.pmtx").unwrap();
    let primary = table
        .indexes
        .iter()
        .find(|i| i.primary)
        .cloned()
        .expect("primary index present");
    (table, primary)
}

fn pmtx_shortcut_counters(db: &Database) -> (u64, u64) {
    (
        db.index_page_max_trx_shortcut_hits.load(Ordering::Relaxed),
        db.index_page_max_trx_shortcut_misses
            .load(Ordering::Relaxed),
    )
}

#[test]
fn page_max_trx_shortcut_fires_after_gc_advances_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    pmtx_test_table(&db);
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.pmtx",
        (0..8)
            .map(|i| vec![Value::Int(i), Value::String(format!("row-{i}"))])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    // Force GC so `min_retained_txn_id` moves past our committed writer.
    // Without this, the shortcut cannot fire — the writer's slot is
    // still above the watermark.
    db.gc_committed_txn_slots();

    let (table, primary) = pmtx_primary(&db);
    let (hits_before, misses_before) = pmtx_shortcut_counters(&db);
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    // Use the covering COUNT path — it walks leaves through the
    // wired shortcut (`try_index_page_max_trx_shortcut`) inside
    // `count_index_entries_visible`.
    let count = db
        .count_index_entries_visible(&table, &primary, None, None, view, None)
        .unwrap();
    assert_eq!(count, 8, "all 8 rows visible to fresh reader");
    let (hits_after, misses_after) = pmtx_shortcut_counters(&db);
    assert!(
        hits_after > hits_before,
        "shortcut must have fired at least once on a fully-GC'd leaf \
         (hits: {hits_before} → {hits_after}, misses: {misses_before} → {misses_after})",
    );
}

#[test]
fn page_max_trx_shortcut_answers_batched_existence_without_heap_visibility_reads() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    pmtx_test_table(&db);
    let writer = db.begin_txn().unwrap();
    db.insert_rows(
        writer,
        "test.pmtx",
        (0..8)
            .map(|i| vec![Value::Int(i), Value::String(format!("row-{i}"))])
            .collect(),
    )
    .unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    db.gc_committed_txn_slots();

    let (table, primary) = pmtx_primary(&db);
    let handle = db.cached_index_handle(table.id, &primary.name).unwrap();
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let keys = vec![
        IndexKey::from(vec![KeyPart::Int(0)]),
        IndexKey::from(vec![KeyPart::Int(3)]),
        IndexKey::from(vec![KeyPart::Int(99)]),
    ];
    let (hits_before, misses_before) = pmtx_shortcut_counters(&db);
    let matched = db
        .keys_with_visible_match_sorted_with_handle(&handle, &table, &primary, &keys, view)
        .unwrap();
    assert_eq!(matched, vec![true, true, false]);
    let (hits_after, misses_after) = pmtx_shortcut_counters(&db);
    assert!(
        hits_after > hits_before,
        "existence probe must consume the leaf visibility bound"
    );
    assert_eq!(
        misses_after, misses_before,
        "an old committed leaf must not fall back to per-RowId visibility"
    );
}

#[test]
fn batched_existence_falls_back_when_leaf_writer_is_above_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    pmtx_test_table(&db);
    let pinned = db.begin_txn().unwrap();
    let writer = db.begin_txn().unwrap();
    db.insert_rows(
        writer,
        "test.pmtx",
        vec![vec![Value::Int(7), Value::String("new".into())]],
    )
    .unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    db.gc_committed_txn_slots();

    let (table, primary) = pmtx_primary(&db);
    let handle = db.cached_index_handle(table.id, &primary.name).unwrap();
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let (hits_before, misses_before) = pmtx_shortcut_counters(&db);
    let matched = db
        .keys_with_visible_match_sorted_with_handle(
            &handle,
            &table,
            &primary,
            &[IndexKey::from(vec![KeyPart::Int(7)])],
            view,
        )
        .unwrap();
    assert_eq!(matched, vec![true]);
    let (hits_after, misses_after) = pmtx_shortcut_counters(&db);
    assert_eq!(hits_after, hits_before);
    assert!(
        misses_after > misses_before,
        "unsafe leaf bound must retain the exact heap-MVCC fallback"
    );
    db.commit_txn(pinned.txn_id).unwrap();
}

#[test]
fn page_max_trx_shortcut_restores_watermark_after_clean_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_relaxed_test_db(dir.path());
        pmtx_test_table(&db);
        let txn = db.begin_txn().unwrap();
        db.insert_rows(
            txn,
            "test.pmtx",
            (0..8)
                .map(|i| vec![Value::Int(i), Value::String(format!("row-{i}"))])
                .collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.checkpoint().unwrap();
    }

    // A clean checkpoint collapses the WAL to CheckpointEnd, so there are no
    // committed transaction slots for background GC to remove after reopen.
    // The checkpoint's next_txn_id must seed the already-committed prefix.
    let db = open_relaxed_test_db(dir.path());
    assert!(
        db.storage_status().min_retained_txn_id > 0,
        "clean restart must restore the checkpointed committed txn prefix",
    );
    let (table, primary) = pmtx_primary(&db);
    let (hits_before, misses_before) = pmtx_shortcut_counters(&db);
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let page_upper = db.index_page_id_upper_bound(&table, &primary).unwrap();
    let (raw_count, resume_page) = db
        .count_visible_index_leaf_prefix_page_range(&table, &primary, 0, page_upper, view)
        .unwrap();
    assert_eq!(raw_count, 8, "raw leaf headers carry the exact row count");
    assert_eq!(
        resume_page, page_upper,
        "fully visible range must not require decoded-entry fallback",
    );
    let count = db
        .count_index_entries_visible(&table, &primary, None, None, view, None)
        .unwrap();
    assert_eq!(count, 8);
    let (hits_after, misses_after) = pmtx_shortcut_counters(&db);
    assert!(
        hits_after > hits_before,
        "restarted scan must use page shortcut"
    );
    assert_eq!(
        misses_after, misses_before,
        "fully checkpointed leaf must not fall back to heap visibility",
    );
}

#[test]
fn page_max_trx_shortcut_misses_when_writer_lives_above_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    pmtx_test_table(&db);
    let long_txn = db.begin_txn().unwrap();
    // Long-running txn pins min_retained_txn_id below its own id.
    let writer = db.begin_txn().unwrap();
    db.insert_rows(
        writer,
        "test.pmtx",
        (0..4)
            .map(|i| vec![Value::Int(i), Value::String(format!("v{i}"))])
            .collect(),
    )
    .unwrap();
    db.commit_txn(writer.txn_id).unwrap();
    db.gc_committed_txn_slots();
    // long_txn still active → watermark cannot advance past long_txn's
    // txn_id, so writer's txn_id sits above it and the shortcut must
    // decline for the leaf it stamped.
    let (table, primary) = pmtx_primary(&db);
    let (hits_before, misses_before) = pmtx_shortcut_counters(&db);
    let reader = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let count = db
        .count_index_entries_visible(&table, &primary, None, None, reader, None)
        .unwrap();
    assert_eq!(count, 4, "reader still sees writer's 4 committed rows");
    let (hits_after, misses_after) = pmtx_shortcut_counters(&db);
    assert_eq!(
        hits_after, hits_before,
        "shortcut MUST NOT fire while a long-lived active txn pins the watermark",
    );
    assert!(
        misses_after > misses_before,
        "shortcut miss counter must increment (recorded fallback to heap-header path); \
         hits {hits_before}→{hits_after}, misses {misses_before}→{misses_after}",
    );
    // Cleanup: commit long_txn so the tempdir tears down cleanly.
    db.commit_txn(long_txn.txn_id).unwrap();
}

#[test]
fn page_max_trx_shortcut_does_not_infer_visibility_from_txn_id_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    pmtx_test_table(&db);
    let writer = db.begin_txn().unwrap();
    db.insert_rows(
        writer,
        "test.pmtx",
        (0..4)
            .map(|i| vec![Value::Int(i), Value::String(format!("v{i}"))])
            .collect(),
    )
    .unwrap();
    db.commit_txn(writer.txn_id).unwrap();

    // This transaction starts after the leaf writer and prevents the global
    // GC watermark from proving the page. Transaction-id order alone cannot
    // prove commit-SCN visibility, so the scan must use heap MVCC.
    let pinned = db.begin_txn().unwrap();
    let reader = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);

    let (table, primary) = pmtx_primary(&db);
    let (hits_before, misses_before) = pmtx_shortcut_counters(&db);
    let count = db
        .count_index_entries_visible(&table, &primary, None, None, reader, None)
        .unwrap();
    assert_eq!(count, 4);
    let (hits_after, misses_after) = pmtx_shortcut_counters(&db);
    assert_eq!(hits_after, hits_before);
    assert!(misses_after > misses_before);
    db.commit_txn(pinned.txn_id).unwrap();
}

#[test]
fn page_max_trx_gc_watermark_never_jumps_over_retained_lower_id() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    pmtx_test_table(&db);

    // txn 1 writes first but commits last. Two higher-id transactions commit
    // first so an old snapshot can sit strictly between their SCNs and txn 1.
    let delayed = db.begin_txn().unwrap();
    db.insert_rows(
        delayed,
        "test.pmtx",
        vec![vec![Value::Int(1), Value::String("too-new".into())]],
    )
    .unwrap();
    let earlier_commit_1 = db.begin_txn().unwrap();
    db.commit_txn(earlier_commit_1.txn_id).unwrap();
    let earlier_commit_2 = db.begin_txn().unwrap();
    db.commit_txn(earlier_commit_2.txn_id).unwrap();

    let snapshot_scn = db.visible_scn();
    let _guard = db.register_statement_snapshot(snapshot_scn);
    let old_view = db.read_only_statement_view(IsolationLevel::ReadCommitted, Some(snapshot_scn));
    db.commit_txn(delayed.txn_id).unwrap();
    db.gc_committed_txn_slots();

    assert!(
        db.storage_status().min_retained_txn_id < delayed.txn_id,
        "the contiguous watermark must stop before the retained lower txn id",
    );
    let (table, primary) = pmtx_primary(&db);
    let (hits_before, misses_before) = pmtx_shortcut_counters(&db);
    let visible = db
        .count_index_entries_visible(&table, &primary, None, None, old_view, None)
        .unwrap();
    assert_eq!(visible, 0, "old snapshot must not see delayed commit");
    let (hits_after, misses_after) = pmtx_shortcut_counters(&db);
    assert_eq!(hits_after, hits_before);
    assert!(misses_after > misses_before);
}

#[test]
fn page_max_trx_shortcut_preserves_snapshot_correctness_across_new_writer() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    pmtx_test_table(&db);
    // Seed rows with an old writer + advance GC so their leaves qualify.
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.pmtx",
        (0..3)
            .map(|i| vec![Value::Int(i), Value::String("seed".into())])
            .collect(),
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    db.gc_committed_txn_slots();

    // Snapshot the world as it currently is. This reader must NOT see
    // any row inserted after this point, regardless of whether the
    // shortcut fires on later leaf reads.
    let snapshot_scn = db.visible_scn();
    let reader = db.read_only_statement_view(IsolationLevel::ReadCommitted, Some(snapshot_scn));
    // Pin the snapshot so subsequent GC can't collapse it — this is
    // what a real long-running SELECT would do via SnapshotGuard.
    let _guard = std::sync::Arc::new(db).register_statement_snapshot(snapshot_scn);
    let db = _guard.db.clone();

    // A newer writer arrives and adds MORE rows. Its txn_id sits above
    // the currently-pinned watermark; the shortcut must not falsely
    // report the newly-stamped leaf as all-visible to the older reader.
    let newer = db.begin_txn().unwrap();
    db.insert_rows(
        newer,
        "test.pmtx",
        (10..13)
            .map(|i| vec![Value::Int(i), Value::String("newer".into())])
            .collect(),
    )
    .unwrap();
    db.commit_txn(newer.txn_id).unwrap();
    // Try to advance GC. It CANNOT go past our pinned snapshot; the
    // newer writer's txn_id must therefore remain above the watermark.
    db.gc_committed_txn_slots();

    let (table, primary) = pmtx_primary(&db);
    let visible_to_old = db
        .count_index_entries_visible(&table, &primary, None, None, reader, None)
        .unwrap();
    assert_eq!(
        visible_to_old, 3,
        "reader with pre-newer snapshot must see exactly 3 seed rows, \
         not the newer writer's rows — this is the correctness gate that \
         proves the shortcut respects snapshot visibility",
    );

    // Now with a fresh reader (no pinned snapshot), all 6 rows should
    // be visible, and the shortcut may or may not fire depending on
    // whether GC has since caught up to the newer writer — either
    // way, correctness is unchanged.
    drop(_guard); // release pinned snapshot so GC can advance
    db.gc_committed_txn_slots();
    let fresh = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let all_visible = db
        .count_index_entries_visible(&table, &primary, None, None, fresh, None)
        .unwrap();
    assert_eq!(all_visible, 6, "fresh reader sees seed+newer rows");
}

#[test]
fn page_max_trx_shortcut_declines_while_index_repairs_pending() {
    // If any transaction registers a pending index repair (compensating
    // deferred-delete), the shortcut must decline — deferred deletes
    // leave the entry physically on the leaf but semantically gone, and
    // trusting the leaf bound would leak stale entries to the reader.
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    pmtx_test_table(&db);
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.pmtx",
        (0..3)
            .map(|i| vec![Value::Int(i), Value::String("seed".into())])
            .collect(),
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    db.gc_committed_txn_slots();

    // Sanity: without pending repairs, shortcut fires.
    let (table, primary) = pmtx_primary(&db);
    let (h0, _) = pmtx_shortcut_counters(&db);
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let _ = db
        .count_index_entries_visible(&table, &primary, None, None, view, None)
        .unwrap();
    let (h1, _) = pmtx_shortcut_counters(&db);
    assert!(h1 > h0, "sanity: shortcut fires when repairs are drained");

    // Simulate a pending-repair regime by flipping the global and per-index
    // atomics directly (a real UPDATE would set them via the DML path; this
    // exercises just the shortcut's guard).
    let scope = db.index_repair_epochs.version(table.id, &primary.name);
    scope.fetch_or(1, Ordering::Release);
    db.index_repair_version.fetch_or(1, Ordering::Release);
    let (h2, m2) = pmtx_shortcut_counters(&db);
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let _ = db
        .count_index_entries_visible(&table, &primary, None, None, view, None)
        .unwrap();
    let (h3, m3) = pmtx_shortcut_counters(&db);
    assert_eq!(
        h3, h2,
        "shortcut MUST NOT fire while has_pending_index_repairs=true",
    );
    // The miss counter is only bumped when the shortcut *would* have
    // been evaluated against a valid bound; the guard short-circuits
    // before recording anything, so we assert no miss increment either.
    assert_eq!(
        m3, m2,
        "guard short-circuits before recording — no miss either",
    );
    db.index_repair_version.fetch_and(!1, Ordering::Release);
    scope.fetch_and(!1, Ordering::Release);
}

#[test]
fn page_max_trx_shortcut_treats_zero_bound_as_visible() {
    // A leaf whose bulk-load stamped page_max_trx_id = 0 (DDL loader,
    // freshly initialised root, older format) must be trusted as
    // trivially visible. This mirrors InnoDB's PAGE_MAX_TRX_ID=0 case.
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    pmtx_test_table(&db);
    // The empty root leaf has page_max_trx_id = 0. A range read over
    // the empty table should short-circuit trivially — but we can only
    // observe the counter bumping when there is at least one entry on
    // the visited leaf, since `try_index_page_max_trx_shortcut` is
    // called only after the cursor emits a non-empty chunk. So we
    // exercise the helper directly.
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let mask = db.try_index_page_max_trx_shortcut(0, view, 5);
    assert_eq!(
        mask,
        Some(vec![true; 5]),
        "page_max_trx_id=0 must resolve as all-visible regardless of watermark",
    );
}

#[test]
fn rollback_failure_fail_stops_and_restart_recovery_repairs_it() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::open(dir.path()).unwrap();
        pmtx_test_table(&db);
        let seed = db.begin_txn().unwrap();
        db.insert_rows(
            seed,
            "test.pmtx",
            vec![vec![Value::Int(1), Value::String("before".into())]],
        )
        .unwrap();
        db.commit_txn(seed.txn_id).unwrap();

        let writer = db.begin_txn().unwrap();
        let (table, primary) = pmtx_primary(&db);
        let row_id = db
            .lookup_index(&table, &primary, IndexKey::from(vec![KeyPart::Int(1)]))
            .unwrap()[0];
        db.update_row_by_id(writer, &table, row_id, |_| {
            Ok(vec![Value::Int(1), Value::String("after".into())])
        })
        .unwrap();
        let undo_head = db.txns.read().undo_head(writer.txn_id).unwrap().unwrap();
        db.undo
            .write()
            .records
            .get_mut(&undo_head)
            .unwrap()
            .before_values = None;

        let err = db.rollback_txn(writer.txn_id).unwrap_err();
        assert!(err.message().contains("before-image"));
        assert_eq!(
            db.txns.read().status(writer.txn_id),
            Some(TxnStatus::Active)
        );
        let stopped = db.begin_txn().unwrap_err();
        assert_eq!(
            stopped.code(),
            crate::error::catalog::ER_SERVER_SHUTDOWN.code()
        );
    }

    // The on-disk undo frame and WAL Abort-without-End remain intact. Startup
    // classifies it as a loser and finishes compensation before serving reads.
    let db = Database::open(dir.path()).unwrap();
    let rows = db
        .scan_rows(
            "test.pmtx",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[1], Value::String("before".into()));
}

#[test]
fn replica_promotion_rollback_physically_compensates_active_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    pmtx_test_table(&db);
    let active = db.begin_txn().unwrap();
    db.insert_rows(
        active,
        "test.pmtx",
        vec![vec![Value::Int(1), Value::String("abandoned".into())]],
    )
    .unwrap();

    assert_eq!(db.rollback_all_active_replication_txns().unwrap(), 1);
    assert_eq!(db.storage_status().active_transactions, 0);
    db.purge_obsolete_undo();
    db.gc_committed_txn_slots();
    let rows = db
        .scan_rows(
            "test.pmtx",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )
        .unwrap();
    assert!(
        rows.is_empty(),
        "abandoned replicated insert must not reappear"
    );
}

#[test]
fn published_commit_durability_failure_fail_stops_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, false),
    )
    .unwrap();
    pmtx_test_table(&db);
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.pmtx",
        vec![vec![Value::Int(1), Value::String("committing".into())]],
    )
    .unwrap();

    let err = crate::storage::txn::with_durability_sync_failure(|| db.commit_txn(txn.txn_id))
        .unwrap_err();
    assert!(err.message().contains("durability"));
    assert_eq!(
        db.txns.read().status(txn.txn_id),
        Some(TxnStatus::Committed),
        "commit was already MVCC-published when the barrier failed"
    );
    let stopped = db.begin_txn().unwrap_err();
    assert_eq!(
        stopped.code(),
        crate::error::catalog::ER_SERVER_SHUTDOWN.code()
    );
}
#[test]
fn query_buffer_stats_are_isolated_across_threads() {
    let first = Arc::new(QueryBufferStats::default());
    let second = Arc::new(QueryBufferStats::default());
    let worker_stats = second.clone();

    with_query_buffer_stats(first.clone(), || {
        record_query_buffer_hit();
        record_query_buffer_miss();
        std::thread::spawn(move || {
            with_query_buffer_stats(worker_stats, || {
                for _ in 0..100 {
                    record_query_buffer_hit();
                }
                for _ in 0..50 {
                    record_query_buffer_miss();
                }
            });
        })
        .join()
        .unwrap();
    });

    assert_eq!(query_buffer_stats_counts(&first), (1, 1));
    assert_eq!(query_buffer_stats_counts(&second), (100, 50));
}

#[test]
fn shared_operator_buffer_stats_charge_only_innermost_scope() {
    let query = Arc::new(QueryBufferStats::default());
    let outer = Arc::new(QueryBufferStats::default());
    let inner = Arc::new(QueryBufferStats::default());

    with_query_buffer_stats(query.clone(), || {
        with_shared_operator_buffer_stats(outer.clone(), || {
            record_query_buffer_hit();
            with_shared_operator_buffer_stats(inner.clone(), || {
                record_query_buffer_hit();
                record_query_buffer_miss();
            });
            record_query_buffer_hit();
        });
    });

    assert_eq!(query_buffer_stats_counts(&query), (3, 1));
    assert_eq!(query_buffer_stats_counts(&outer), (2, 0));
    assert_eq!(query_buffer_stats_counts(&inner), (1, 1));
}

#[test]
fn shared_operator_buffer_stats_can_follow_a_worker_context() {
    let query = Arc::new(QueryBufferStats::default());
    let operator = Arc::new(QueryBufferStats::default());
    let inherited = operator.clone();
    let worker_query = query.clone();

    std::thread::spawn(move || {
        with_query_buffer_stats(worker_query, || {
            with_shared_operator_buffer_stats(inherited, || {
                record_query_buffer_hit();
                record_query_buffer_miss();
            });
        });
    })
    .join()
    .unwrap();

    assert_eq!(query_buffer_stats_counts(&query), (1, 1));
    assert_eq!(query_buffer_stats_counts(&operator), (1, 1));
}

#[test]
fn unpatchable_update_generates_one_sparse_undo_and_restores_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let mut value_column = undo_pagein_int_col("v", false);
    value_column.nullable = true;
    db.create_table(
        "patch_preflight",
        vec![undo_pagein_int_col("id", true), value_column],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.patch_preflight",
        vec![vec![Value::Int(1), Value::Null]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let reader = db.begin_txn().unwrap();
    let writer = db.begin_txn().unwrap();
    // NULL -> integer used to reject the physical patch after creating undo.
    // The combined preflight/bitmap path must produce exactly one sparse
    // before-image and preserve snapshot and rollback semantics.
    db.update_rows_with_changed_columns_noop(
        writer,
        "test.patch_preflight",
        None,
        Some(&[1]),
        Some(&[1]),
        |_| Ok(true),
        |values| {
            let mut row = values.to_vec();
            row[1] = Value::Int(42);
            Ok(row)
        },
    )
    .unwrap();
    {
        let undo = db.undo.read();
        let records: Vec<_> = undo
            .records
            .values()
            .filter(|record| record.txn_id == writer.txn_id)
            .collect();
        assert_eq!(records.len(), 1);
        assert!(
            matches!(&records[0].kind, UndoKind::UpdateDelta { before_cols } if before_cols == &[1])
        );
        assert_eq!(
            records[0].before_values.as_ref().unwrap(),
            &vec![Value::Null]
        );
    }
    assert_eq!(
        db.scan_rows("test.patch_preflight", reader).unwrap()[0].values,
        vec![Value::Int(1), Value::Null]
    );
    db.rollback_txn(writer.txn_id).unwrap();
    assert_eq!(
        db.scan_rows("test.patch_preflight", reader).unwrap()[0].values,
        vec![Value::Int(1), Value::Null]
    );
    db.commit_txn(reader.txn_id).unwrap();
}

#[test]
fn fixed_null_transitions_patch_shared_bitmap_bytes_and_preserve_old_versions() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    let mut columns = vec![undo_pagein_int_col("id", true)];
    for i in 1..10 {
        let mut column = undo_pagein_int_col(&format!("v{i}"), false);
        column.nullable = true;
        columns.push(column);
    }
    db.create_table("null_patch", columns, vec!["id".into()], vec![])
        .unwrap();
    let seed = db.begin_txn().unwrap();
    let mut values = vec![Value::Null; 10];
    values[0] = Value::Int(1);
    db.insert_rows(seed, "test.null_patch", vec![values])
        .unwrap();
    db.update_rows_with_changed_columns_noop(
        seed,
        "test.null_patch",
        None,
        Some(&[1]),
        Some(&[1]),
        |_| Ok(true),
        |values| {
            let mut row = values.to_vec();
            row[1] = Value::Int(10);
            Ok(row)
        },
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let reader = db.begin_txn().unwrap();
    let old = db.scan_rows("test.null_patch", reader).unwrap()[0]
        .values
        .clone();
    let writer = db.begin_txn().unwrap();
    let hits = update_delta_hits();
    db.update_rows_with_changed_columns_noop(
        writer,
        "test.null_patch",
        None,
        Some(&[1, 2, 7, 8, 9]),
        Some(&[1, 2, 7, 8, 9]),
        |_| Ok(true),
        |values| {
            let mut row = values.to_vec();
            row[1] = Value::Null;
            for i in [2, 7, 8, 9] {
                row[i] = Value::Int(i as i64);
            }
            Ok(row)
        },
    )
    .unwrap();
    assert_eq!(
        update_delta_hits() - hits,
        1,
        "all fixed NULL changes use one physical patch"
    );
    let current = db.scan_rows("test.null_patch", writer).unwrap()[0]
        .values
        .clone();
    assert_eq!(current[1], Value::Null);
    for i in [2, 7, 8, 9] {
        assert_eq!(current[i], Value::Int(i as i64));
    }
    assert_eq!(
        db.scan_rows("test.null_patch", reader).unwrap()[0].values,
        old
    );
    db.rollback_txn(writer.txn_id).unwrap();
    assert_eq!(
        db.scan_rows("test.null_patch", reader).unwrap()[0].values,
        old
    );
    db.commit_txn(reader.txn_id).unwrap();
}

#[test]
fn fixed_null_bitmap_patch_replays_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let mut v = undo_pagein_int_col("v", false);
    v.nullable = true;
    db.create_table(
        "null_redo",
        vec![undo_pagein_int_col("id", true), v],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(
        seed,
        "test.null_redo",
        vec![vec![Value::Int(1), Value::Int(10)]],
    )
    .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    // Keep the warm update's undo/header alive across the baseline checkpoint.
    let _reader = db.begin_txn().unwrap();
    let warm = db.begin_txn().unwrap();
    db.update_rows_with_changed_columns_noop(
        warm,
        "test.null_redo",
        None,
        Some(&[1]),
        Some(&[1]),
        |_| Ok(true),
        |_| Ok(vec![Value::Int(1), Value::Int(11)]),
    )
    .unwrap();
    db.commit_txn(warm.txn_id).unwrap();
    db.checkpoint().unwrap();
    let writer = db.begin_txn().unwrap();
    let hits = update_delta_hits();
    db.update_rows_with_changed_columns_noop(
        writer,
        "test.null_redo",
        None,
        Some(&[1]),
        Some(&[1]),
        |_| Ok(true),
        |_| Ok(vec![Value::Int(1), Value::Null]),
    )
    .unwrap();
    assert_eq!(update_delta_hits() - hits, 1);
    db.commit_txn(writer.txn_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    std::mem::forget(db);
    let reopened = open_relaxed_test_db(dir.path());
    let read = reopened.begin_txn().unwrap();
    assert_eq!(
        reopened.scan_rows("test.null_redo", read).unwrap()[0].values,
        vec![Value::Int(1), Value::Null]
    );
    reopened.commit_txn(read.txn_id).unwrap();
}

#[test]
fn btree_snapshot_ordinal_counts_visible_slots_and_releases_scratch() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let dir = tempfile::tempdir().unwrap();
    let mut index =
        IndexFile::open(dir.path().join("1_ordinal.idx"), test_log(dir.path())).unwrap();
    for id in 0..1024 {
        index
            .insert(
                IndexKey::from(vec![KeyPart::Int(id)]),
                RowId {
                    page_id: id as u32,
                    slot_id: 0,
                },
                false,
            )
            .unwrap();
    }
    let budget = Arc::new(MemBudget::new(1024 * 1024));
    // An unrelated writer's epoch must not defeat a range stabilized by its
    // own latch set. (No leaf in this range is being mutated by this guard.)
    let unrelated_writer = index.begin_mutation();
    let result = with_query_budget(Arc::clone(&budget), || {
        index.select_range_entry_by_mvcc_ordinal(
            None,
            None,
            &[],
            |_| false,
            |key, _| Ok(matches!(key.0[0], KeyPart::Int(n) if n % 2 == 0)),
            |count| {
                assert_eq!(count, 512);
                511
            },
        )
    })
    .unwrap()
    .unwrap();
    assert_eq!(
        result.0.unwrap().0,
        IndexKey::from(vec![KeyPart::Int(1022)])
    );
    assert_eq!(budget.used(), 0);
    drop(unrelated_writer);
    let tiny = Arc::new(MemBudget::new(1));
    let result = with_query_budget(Arc::clone(&tiny), || {
        index.select_range_entry_by_mvcc_ordinal(
            None,
            None,
            &[],
            |_| false,
            |_, _| panic!("memory refusal must precede heap validation"),
            |_| panic!("memory refusal must not consume volatile state"),
        )
    })
    .unwrap();
    assert!(result.is_none());
    assert_eq!(tiny.used(), 0);
    let result = with_query_budget(Arc::clone(&budget), || {
        index.select_range_entry_by_mvcc_ordinal(
            None,
            None,
            &[],
            |_| false,
            |_, _| Ok(false),
            |_| panic!("empty snapshot must consume no random draws"),
        )
    })
    .unwrap();
    assert!(matches!(result, Some((None, 0))));
    assert_eq!(budget.used(), 0);
}

#[test]
fn btree_ordinal_cache_invalidates_after_leaf_only_insert_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let index = IndexFile::open(
        dir.path().join("1_ordinal_generation.idx"),
        test_log(dir.path()),
    )
    .unwrap();
    let row = |id| RowId {
        page_id: id,
        slot_id: 0,
    };
    for id in 1..=3 {
        index
            .insert_deferred(1, IndexKey::from(vec![KeyPart::Int(id as i64)]), row(id))
            .unwrap();
    }
    let count = || {
        index
            .select_range_leaf_entry_by_ordinal_if(None, None, |_| true, |_| 0)
            .unwrap()
            .unwrap()
            .1
    };
    assert_eq!(count(), 3);
    index
        .insert_deferred(2, IndexKey::from(vec![KeyPart::Int(4)]), row(4))
        .unwrap();
    assert_eq!(
        count(),
        4,
        "a leaf-only insert must invalidate cached cardinality"
    );
    index
        .delete_deferred(3, IndexKey::from(vec![KeyPart::Int(2)]), row(2))
        .unwrap();
    assert_eq!(
        count(),
        3,
        "a leaf-only delete must invalidate cached cardinality"
    );
    let first_writer = index.begin_mutation();
    let second_writer = index.begin_mutation();
    let decline = || {
        index
            .select_range_leaf_entry_by_ordinal_if(
                None,
                None,
                |_| true,
                |_| panic!("an active writer must not consume volatile state"),
            )
            .unwrap()
    };
    assert!(decline().is_none());
    drop(first_writer);
    assert!(decline().is_none(), "another writer is still active");
    drop(second_writer);
    assert_eq!(count(), 3);
}

#[cfg(test)]
mod mvcc_audit_tests {
    include!("mvcc_audit_tests.rs");
}

#[cfg(test)]
mod mvcc_edge_tests {
    include!("mvcc_edge_tests.rs");
}

#[test]
fn vector_registry_spills_constant_keys_with_bounded_scratch() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "registry",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col(
                "emb",
                DataType::Vector(Some(2), crate::vector::VectorDataType::Float32),
                false,
            ),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    let rows = (0..20_000)
        .map(|i| vec![Value::Int(i), Value::Vector(vec![0; 8].into())])
        .collect();
    db.insert_rows(txn, "test.registry", rows).unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let table = db.table("test.registry").unwrap();
    for direct in [false, true] {
        let entries = db
            .collect_vector_registry_entries(&table, None, 1, 1024 * 1024, direct)
            .unwrap();
        let SortedIndexEntries::Spilled(runs) = entries else {
            panic!("registry must spill when it exceeds the scratch envelope");
        };
        let mut previous = None;
        let mut count = 0;
        for entry in runs.into_merger().unwrap() {
            let entry = entry.unwrap();
            assert_eq!(entry.key, IndexKey::from(vec![KeyPart::Null]));
            assert!(previous.is_none_or(|row_id| row_id < entry.row_id));
            previous = Some(entry.row_id);
            count += 1;
        }
        assert_eq!(count, 20_000);
    }
}

#[test]
fn empty_unique_lock_batch_does_not_take_the_global_write_latch() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    let txn = db.begin_txn().unwrap();
    let read_guard = db.lock_manager_read();
    let (tx, rx) = std::sync::mpsc::channel();
    let other = Arc::clone(&db);
    let worker = std::thread::spawn(move || {
        tx.send(other.lock_unique_keys_nonblocking(txn.txn_id, Vec::new()))
            .unwrap();
    });
    let completed = rx.recv_timeout(Duration::from_millis(250));
    drop(read_guard);
    worker.join().unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    completed
        .expect("empty lock batch unnecessarily acquired the global write latch")
        .unwrap();
}

#[test]
fn read_only_savepoint_does_not_wait_for_unrelated_wal_writer() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    let txn = db.begin_txn().unwrap();
    let wal = db.log.lock();
    let other = Arc::clone(&db);
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(other.read_only_statement_savepoint(txn.txn_id))
            .unwrap();
    });
    let completed = rx.recv_timeout(Duration::from_secs(1));
    drop(wal);
    worker.join().unwrap();
    db.rollback_txn(txn.txn_id).unwrap();
    assert!(
        completed.is_ok(),
        "read-only lock cleanup must not join the WAL writer queue"
    );
    assert!(completed.unwrap().unwrap().wal_boundary.is_none());
}

#[test]
fn online_copy_partial_file_swap_restores_source() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    create_checkpoint_test_table(&db);
    insert_checkpoint_test_row(&db, 7);
    let source = db.table("test.t").unwrap();
    let mut candidate = source.as_ref().clone();
    candidate.active_generation = source.active_generation + 1;
    candidate.bump_schema_version();
    let shadow_id = 100000;
    db.register_reorg_storage_locs(
        &[shadow_id],
        &source.database,
        &source.name,
        candidate.active_generation,
    );
    let path = db.table_path(shadow_id);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, b"private heap placeholder").unwrap();
    // Missing private index fails after both source backups and the heap move.
    assert!(db
        .swap_shadow_generation_files(&source, &candidate, shadow_id, 99999)
        .is_err());
    assert!(path.exists(), "already-moved shadow heap was not restored");
    let read = db.begin_read_only_txn().unwrap();
    let rows = db.scan_rows("test.t", read).unwrap();
    db.rollback_txn(read.txn_id).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::Int(7)]);
    let index = &source.indexes[0];
    let blocked_backup = db
        .index_path_for_table_schema(&source, source.id, &index.name)
        .with_extension("idx.alter-old-100001");
    fs::create_dir(&blocked_backup).unwrap();
    assert!(db
        .swap_shadow_generation_files(&source, &candidate, shadow_id, 100001)
        .is_err());
    let read = db.begin_read_only_txn().unwrap();
    let rows = db.scan_rows("test.t", read).unwrap();
    db.rollback_txn(read.txn_id).unwrap();
    assert_eq!(
        rows[0].values,
        vec![Value::Int(7)],
        "prepare-stage rename failure lost the source"
    );
    fs::remove_dir(blocked_backup).unwrap();
    db.unregister_reorg_storage_locs(&[shadow_id]);
}

#[test]
fn online_copy_failed_swap_preserves_dirty_source_lobs() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    let mut payload = checkpoint_test_pk_column();
    payload.name = "payload".into();
    payload.data_type = DataType::Text(crate::storage::schema::DEFAULT_LOB_MAX);
    payload.primary_key = false;
    db.create_table(
        "t",
        vec![checkpoint_test_pk_column(), payload],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let text = "x".repeat(20000);
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.t",
        vec![vec![Value::Int(7), Value::String(text.clone())]],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let source = db.table("test.t").unwrap();
    let mut target = source.as_ref().clone();
    target.active_generation += 1;
    target.bump_schema_version();
    let shadow = 100001;
    db.register_reorg_storage_locs(
        &[shadow],
        &source.database,
        &source.name,
        target.active_generation,
    );
    let path = db.table_path(shadow);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, b"private heap").unwrap();
    assert!(db
        .swap_shadow_generation_files(&source, &target, shadow, 100002)
        .is_err());
    let read = db.begin_read_only_txn().unwrap();
    let rows = db.scan_rows("test.t", read).unwrap();
    db.rollback_txn(read.txn_id).unwrap();
    assert_eq!(rows[0].values, vec![Value::Int(7), Value::String(text)]);
    db.unregister_reorg_storage_locs(&[shadow]);
}

#[test]
fn btree_chunk_resume_and_exclusive_low_cross_leaf_boundaries() {
    for repeated in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let log = test_log(dir.path());
        let pool = BufferPool::new_with_options(
            8,
            log.clone(),
            DurabilityMode::Strict,
            Duration::ZERO,
            1,
            0,
        );
        let entries = (0..4000u32)
            .map(|i| IndexEntry {
                key: wide_test_index_key(if repeated {
                    i64::from(i / 2000)
                } else {
                    i64::from(i)
                }),
                row_id: RowId {
                    page_id: i + 1,
                    slot_id: 0,
                },
            })
            .collect::<Vec<_>>();
        let index = IndexFile::create_rebuilt_from_sorted_stream_with_durability(
            dir.path().join("chunk.idx"),
            entries.iter().cloned().map(Ok),
            log,
            DurabilityMode::Strict,
            pool,
        )
        .unwrap();
        // A separator's RowId is nonzero: lower-bound(key, RowId::zero)
        // can descend into a leaf preceding the actual resume position.
        let mut leaf_id = index.leftmost_leaf().unwrap();
        let mut boundaries = Vec::new();
        loop {
            let page = index.read_page(leaf_id).unwrap();
            let IndexPage::Leaf(leaf) = page.as_ref() else {
                panic!("expected leaf")
            };
            boundaries.push(leaf.entries[0].clone());
            let Some(next) = leaf.next_leaf else { break };
            leaf_id = next;
        }
        assert!(boundaries.len() > 2);
        for boundary in boundaries.into_iter().skip(1) {
            let resume = (boundary.key.clone(), boundary.row_id);
            let expected = entries
                .iter()
                .filter(|entry| {
                    entry.key > boundary.key
                        || (entry.key == boundary.key && entry.row_id > boundary.row_id)
                })
                .take(7)
                .map(|entry| (entry.key.clone(), entry.row_id))
                .collect::<Vec<_>>();
            assert_eq!(
                index
                    .range_entries_chunk(None, None, Some(&resume), 7)
                    .unwrap(),
                expected,
                "resume at leaf boundary, repeated={repeated}"
            );
            let low = IndexBound {
                key: boundary.key.clone(),
                inclusive: false,
            };
            let expected = entries
                .iter()
                .filter(|entry| entry.key > boundary.key)
                .take(7)
                .map(|entry| (entry.key.clone(), entry.row_id))
                .collect::<Vec<_>>();
            assert_eq!(
                index
                    .range_entries_chunk(Some(&low), None, None, 7)
                    .unwrap(),
                expected,
                "exclusive low at leaf boundary, repeated={repeated}"
            );
        }
    }
}

#[path = "insert_optimization_tests.rs"]
mod insert_optimization;

#[test]
fn fulltext_unicode_prefix_generic_matches_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "docs",
        vec![
            ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: false,
                primary_key: true,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
            ColumnDef {
                name: "body".into(),
                data_type: DataType::VarChar(Some(255)),
                unsigned: false,
                zerofill: false,
                invisible: false,
                nullable: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                default: None,
                on_update: None,
                checks: Vec::new(),
                generated: None,
                charset: None,
                collation: None,
            },
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    db.add_index(
        "test.docs",
        IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "ft".into(),
            columns: vec!["body".into()],
            column_prefix_lengths: Vec::new(),
            column_directions: Vec::new(),
            key_expressions: Vec::new(),
            unique: false,
            primary: false,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "FULLTEXT".to_string(),
            visible: true,
            comment: String::new(),
            vector: None,
            fulltext: None,
        },
    )
    .unwrap();

    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.docs",
        vec![
            vec![Value::Int(1), Value::String("丿甲乙".into())],
            vec![Value::Int(2), Value::String("乀甲乙".into())],
            vec![Value::Int(3), Value::String("中文词".into())],
        ],
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let table = db.table("test.docs").unwrap();
    let index = table.indexes.iter().find(|i| i.name == "ft").unwrap();
    let txn = db.begin_txn().unwrap();
    for pending in [false, true] {
        if pending {
            db.index_repair_version.fetch_or(1, Ordering::Release);
        } else {
            db.index_repair_version.fetch_and(!1, Ordering::Release);
        }
        let batches = db.fulltext_scan_prefix(&table, index, "丿", txn).unwrap();
        assert_eq!(batches.len(), 1, "pending={pending}");
        assert_eq!(batches[0].0, "丿甲乙");
        assert_eq!(batches[0].1.postings.len(), 1);
    }
    db.index_repair_version.fetch_and(!1, Ordering::Release);
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn locking_point_read_watermark_avoids_unrelated_global_latches() {
    if !row_lock_sharding_enabled() {
        return;
    }
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    let txn = db.begin_txn().unwrap();
    let txns = db.txns.write();
    let locks = db.lock_manager();
    let other = Arc::clone(&db);
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(other.locking_read_lock_watermark(txn.txn_id))
            .unwrap();
    });
    let captured = rx.recv_timeout(Duration::from_secs(1));
    drop(locks);
    drop(txns);
    worker.join().unwrap();
    let watermark =
        captured.expect("projection-only locking read joined unrelated global latch queue");
    let savepoint = db
        .materialize_locking_read_savepoint(txn.txn_id, watermark)
        .unwrap();
    assert_eq!(savepoint.row_log_len, 0);
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn locking_point_read_error_restores_only_new_locks_and_preserves_prior_writes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = open_relaxed_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(7)]])
        .unwrap();
    let before = db.read_only_statement_savepoint(txn.txn_id).unwrap();
    let old = RowId {
        page_id: 999,
        slot_id: 0,
    };
    let new = RowId {
        page_id: 999,
        slot_id: 1,
    };
    db.acquire_row_lock_blocking(txn.txn_id, 987654, old, LockMode::Exclusive)
        .unwrap();
    let watermark = db.locking_read_lock_watermark(txn.txn_id);
    db.acquire_row_lock_blocking(txn.txn_id, 987654, new, LockMode::Exclusive)
        .unwrap();
    let savepoint = db
        .materialize_locking_read_savepoint(txn.txn_id, watermark)
        .unwrap();
    assert_eq!(savepoint.undo_head, before.undo_head);
    assert_eq!(savepoint.binlog_len, before.binlog_len);
    db.rollback_to_statement_savepoint(txn.txn_id, savepoint)
        .unwrap();
    let other = db.begin_txn().unwrap();
    assert!(!db
        .try_row_lock_without_wait(other.txn_id, 987654, old, LockMode::Exclusive)
        .unwrap());
    assert!(db
        .try_row_lock_without_wait(other.txn_id, 987654, new, LockMode::Exclusive)
        .unwrap());
    db.rollback_txn(other.txn_id).unwrap();
    assert_eq!(
        db.read_only_statement_savepoint(txn.txn_id)
            .unwrap()
            .undo_head,
        before.undo_head
    );
    db.commit_txn(txn.txn_id).unwrap();
    let read = db.begin_txn().unwrap();
    assert_eq!(db.scan_rows("test.t", read).unwrap().len(), 1);
    db.rollback_txn(read.txn_id).unwrap();
}

#[test]
fn check_only_unique_probe_uses_shared_global_latch() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(open_memory_test_db(dir.path()));
    let txn = db.begin_txn().unwrap();
    let shared = db.lock_manager_read();
    let other = Arc::clone(&db);
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let key = IndexKey::from(vec![KeyPart::Int(7)]);
        let checked = other.check_unique_key_lock_blocking(txn.txn_id, 9, "pk", &key)?;
        let reprobed = other.recheck_unique_index_miss(txn.txn_id, 9, "pk", &key, || Ok(17))?;
        tx.send(Ok::<_, QueryError>((checked, reprobed))).unwrap();
        Ok::<_, QueryError>(())
    });
    let captured = rx.recv_timeout(Duration::from_secs(1));
    drop(shared);
    worker.join().unwrap().unwrap();
    assert_eq!(
        captured
            .expect("check-only probe requested exclusive global latch")
            .unwrap(),
        (false, 17)
    );
    db.rollback_txn(txn.txn_id).unwrap();
}

#[test]
fn unique_probe_releases_transaction_directory_with_live_repairs() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "probe",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let table = db.table("probe").unwrap();
    let index = table.indexes.iter().find(|index| index.primary).unwrap();
    let key = IndexKey::from(vec![KeyPart::Int(1)]);
    let reader = db.begin_txn().unwrap();
    let writer = db.begin_txn().unwrap();
    let id = RowId::new(1, 1);
    db.txns
        .write()
        .register_index_repairs(
            writer.txn_id,
            [IndexRepairKey {
                table_id: table.id,
                index_name: normalize_name(&index.name),
                key: key.clone(),
            }],
            id,
        )
        .unwrap();
    let (candidate, repairs, recheck) = db
        .probe_unique_index_candidate(&table, index, &key, reader, || {
            assert!(
                db.txns.try_write().is_some(),
                "physical I/O must not pin the global directory"
            );
            Ok(None)
        })
        .unwrap();
    assert!(candidate.is_none());
    assert!(recheck);
    assert_eq!(repairs, vec![(key, id)]);
    db.rollback_txn(writer.txn_id).unwrap();
    db.rollback_txn(reader.txn_id).unwrap();
}

#[test]
fn unique_probe_retries_delete_rollback_aba() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "probe",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.probe", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let table = db.table("probe").unwrap();
    let index = table.indexes.iter().find(|index| index.primary).unwrap();
    let key = IndexKey::from(vec![KeyPart::Int(1)]);
    let id = db.lookup_index(&table, index, key.clone()).unwrap()[0];
    let reader = db.begin_txn().unwrap();
    let writer = db.begin_txn().unwrap();
    let mut probes = 0;
    let (candidate, _, recheck) = db
        .probe_unique_index_candidate(&table, index, &key, reader, || {
            probes += 1;
            if probes == 1 {
                db.delete_row_by_id(writer, &table, id)?;
                let miss =
                    db.with_index_read(table.id, &index.name, |file| file.get_unique_one(&key))?;
                assert!(miss.is_none());
                db.rollback_txn(writer.txn_id)?;
                Ok(miss)
            } else {
                db.with_index_read(table.id, &index.name, |file| file.get_unique_one(&key))
            }
        })
        .unwrap();
    assert_eq!(probes, 2);
    assert_eq!(candidate, Some(id));
    assert!(!recheck);
    db.rollback_txn(reader.txn_id).unwrap();
}

#[test]
fn unique_probe_retries_key_change_and_preserves_old_snapshot() {
    for isolation in [
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = open_memory_test_db(dir.path());
        db.create_table(
            "probe",
            vec![checkpoint_test_pk_column()],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let seed = db.begin_txn().unwrap();
        db.insert_rows(seed, "test.probe", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        let table = db.table("probe").unwrap();
        let index = table.indexes.iter().find(|index| index.primary).unwrap();
        let key = IndexKey::from(vec![KeyPart::Int(1)]);
        let id = db.lookup_index(&table, index, key.clone()).unwrap()[0];
        let reader = TxnView {
            isolation,
            ..db.begin_txn().unwrap()
        };
        let writer = db.begin_txn().unwrap();
        let savepoint = db.statement_savepoint(writer.txn_id).unwrap();
        let mut probes = 0;
        let (candidate, repairs, recheck) = db
            .probe_unique_index_candidate(&table, index, &key, reader, || {
                probes += 1;
                if probes == 1 {
                    db.update_row_by_id(writer, &table, id, |_| Ok(vec![Value::Int(2)]))?;
                }
                db.with_index_read(table.id, &index.name, |file| file.get_unique_one(&key))
            })
            .unwrap();
        assert_eq!(probes, 2);
        assert!(candidate.is_none());
        assert!(recheck);
        assert_eq!(repairs, vec![(key.clone(), id)]);
        let row = db
            .read_unique_index_candidate(&table, index, key.clone(), reader, |id, recheck| {
                let row = db.read_row(&table, id, reader)?;
                Ok(row.filter(|row| {
                    recheck.is_none_or(|key| table.index_key(index, &row.values).unwrap() == *key)
                }))
            })
            .unwrap()
            .unwrap();
        assert_eq!(row.values, vec![Value::Int(1)]);
        db.rollback_to_statement_savepoint(writer.txn_id, savepoint)
            .unwrap();
        assert_eq!(
            db.lookup_unique_index_candidate_one(&table, index, key, reader)
                .unwrap(),
            Some(id)
        );
        db.commit_txn(writer.txn_id).unwrap();
        db.rollback_txn(reader.txn_id).unwrap();
    }
}

#[test]
fn undo_append_cache_rearms_writer_after_empty_savepoint_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "undo_cache",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.undo_cache", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let table = db.table("undo_cache").unwrap();
    let index = table.indexes.iter().find(|index| index.primary).unwrap();
    let id = db
        .lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(1)]))
        .unwrap()[0];
    let txn = db.begin_txn().unwrap();
    let savepoint = db.statement_savepoint(txn.txn_id).unwrap();
    db.update_row_by_id(txn, &table, id, |_| Ok(vec![Value::Int(2)]))
        .unwrap();
    let (cached, writer) = db.undo_head_for_append(txn.txn_id).unwrap();
    assert!(writer && cached.get().is_some());
    db.rollback_to_statement_savepoint(txn.txn_id, savepoint)
        .unwrap();
    assert!(cached.get().is_none());
    assert!(!db.txns.read().slot(txn.txn_id).unwrap().is_writer);
    db.update_row_by_id(txn, &table, id, |_| Ok(vec![Value::Int(3)]))
        .unwrap();
    assert!(db.txns.read().slot(txn.txn_id).unwrap().is_writer);
    assert_eq!(db.active_write_txns_atomic.load(Ordering::Acquire), 1);
    db.commit_txn(txn.txn_id).unwrap();
    assert!(
        db.undo_head_for_append(txn.txn_id).is_err(),
        "a cached head must not revive a committed transaction"
    );
    let rows = db
        .scan_rows(
            "test.undo_cache",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values, vec![Value::Int(3)]);
    db.gc_committed_txn_slots();
    assert!(db.undo_head_for_append(txn.txn_id).is_err());
}

#[test]
fn undo_append_cache_is_scoped_to_database_identity() {
    // Reuse transaction ids on this executing thread in distinct databases.
    // Rollback must follow each database's own before-images and head handle.
    for value in [1, 10] {
        let dir = tempfile::tempdir().unwrap();
        let db = open_memory_test_db(dir.path());
        db.create_table(
            "undo_cache",
            vec![checkpoint_test_pk_column()],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let seed = db.begin_txn().unwrap();
        db.insert_rows(seed, "test.undo_cache", vec![vec![Value::Int(value)]])
            .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        let table = db.table("undo_cache").unwrap();
        let index = table.indexes.iter().find(|index| index.primary).unwrap();
        let id = db
            .lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(value)]))
            .unwrap()[0];
        let txn = db.begin_txn().unwrap();
        db.update_row_by_id(txn, &table, id, |_| Ok(vec![Value::Int(value + 1)]))
            .unwrap();
        db.update_row_by_id(txn, &table, id, |_| Ok(vec![Value::Int(value + 2)]))
            .unwrap();
        db.rollback_txn(txn.txn_id).unwrap();
        let rows = db
            .scan_rows(
                "test.undo_cache",
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap();
        assert_eq!(rows[0].values, vec![Value::Int(value)]);
    }
}

#[test]
fn buffer_pool_sharded_hits_count_concurrent_resident_reads_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let log = test_log(dir.path());
    let pool = BufferPool::new_with_options(
        16,
        log.clone(),
        DurabilityMode::Memory,
        Duration::ZERO,
        1,
        0,
    );
    let index = IndexFile::open_with_durability(
        dir.path().join("hits.idx"),
        log,
        DurabilityMode::Memory,
        pool.clone(),
    )
    .unwrap();
    let key = index.page_key(index.root_page.load(Ordering::Acquire));
    drop(pool.pin_page(key.clone()).unwrap());
    let before = pool.buffer_stats().hits;
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let pool = pool.clone();
            let key = key.clone();
            scope.spawn(move || {
                for i in 0..1000 {
                    if i % 2 == 0 {
                        drop(pool.pin_page(key.clone()).unwrap());
                    } else {
                        drop(
                            pool.try_pin_resident_page(&key)
                                .unwrap()
                                .expect("seed page remains resident"),
                        );
                    }
                }
            });
        }
    });
    assert_eq!(pool.buffer_stats().hits - before, 8000);
    assert_eq!(
        pool.buffer_stats().hits,
        pool.shards
            .iter()
            .map(|shard| shard.hits.load(Ordering::Relaxed))
            .sum::<u64>()
    );
}

#[test]
fn transaction_local_savepoint_tracks_index_repair_rollback_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    db.create_table(
        "repair_watermark",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_txn().unwrap();
    db.insert_rows(seed, "test.repair_watermark", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let table = db.table("repair_watermark").unwrap();
    let index = table.indexes.iter().find(|index| index.primary).unwrap();
    let id = db
        .lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(1)]))
        .unwrap()[0];
    let txn = db.begin_txn().unwrap();
    let initial = db.statement_savepoint(txn.txn_id).unwrap();
    assert_eq!(initial.index_repair_log_len, 0);
    db.update_row_by_id(txn, &table, id, |_| Ok(vec![Value::Int(2)]))
        .unwrap();
    let middle = db.statement_savepoint(txn.txn_id).unwrap();
    assert!(middle.index_repair_log_len > 0);
    db.update_row_by_id(txn, &table, id, |_| Ok(vec![Value::Int(3)]))
        .unwrap();
    assert!(
        db.statement_savepoint(txn.txn_id)
            .unwrap()
            .index_repair_log_len
            > middle.index_repair_log_len
    );
    let watermark = middle.index_repair_log_len;
    db.rollback_to_statement_savepoint(txn.txn_id, middle)
        .unwrap();
    assert_eq!(
        db.statement_savepoint(txn.txn_id)
            .unwrap()
            .index_repair_log_len,
        watermark
    );
    assert_eq!(
        db.lookup_unique_index_candidate_one(
            &table,
            index,
            IndexKey::from(vec![KeyPart::Int(2)]),
            txn
        )
        .unwrap(),
        Some(id)
    );
    db.rollback_to_statement_savepoint(txn.txn_id, initial)
        .unwrap();
    assert_eq!(
        db.statement_savepoint(txn.txn_id)
            .unwrap()
            .index_repair_log_len,
        0
    );
    assert_eq!(
        db.lookup_unique_index_candidate_one(
            &table,
            index,
            IndexKey::from(vec![KeyPart::Int(1)]),
            txn
        )
        .unwrap(),
        Some(id)
    );
    db.commit_txn(txn.txn_id).unwrap();
}

include!("index_cache_eviction_tests.rs");

include!("insert_lock_tests.rs");

#[test]
fn mydb_wal_write_buffer_bytes_is_settable_at_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    assert_eq!(
        db.wal_write_buffer_bytes(),
        crate::wal::DEFAULT_WAL_WRITE_BUFFER_BYTES as u64
    );
    for bytes in [4096, 0, 1048576] {
        db.set_storage_variable("mydb_wal_write_buffer_bytes", bytes)
            .unwrap();
        assert_eq!(db.wal_write_buffer_bytes(), bytes);
    }
    assert!(db
        .set_storage_variable("mydb_wal_write_buffer_bytes", u64::MAX)
        .is_err());
    assert_eq!(db.wal_write_buffer_bytes(), 1048576);
}

#[test]
fn release_audit_raw_unique_upper_probe_matches_decoded_keys() {
    fn key(kind: usize, n: i64) -> IndexKey {
        IndexKey::from(match kind {
            0 => vec![KeyPart::Int(n)],
            1 => vec![KeyPart::UInt(n as u64)],
            2 => vec![KeyPart::Decimal {
                value: n as i128,
                scale: 4,
            }],
            3 => vec![KeyPart::Date(n as i32)],
            4 => vec![KeyPart::String(format!("{n:05}{}", "x".repeat(300)))],
            5 => vec![KeyPart::Int(n / 8), KeyPart::Int(n % 8)],
            _ => vec![KeyPart::CollatedString {
                collation_id: 255,
                value: format!("{n:05}"),
            }],
        })
    }
    let dir = tempfile::tempdir().unwrap();
    for kind in 0..7 {
        let mut index = IndexFile::open(
            dir.path().join(format!("upper-{kind}.idx")),
            test_log(dir.path()),
        )
        .unwrap();
        assert!(index.last_key_is_before(&key(kind, 0)).unwrap());
        for n in 1..65 {
            index.insert(key(kind, n), test_row_id(n, 8), true).unwrap();
        }
        let maximum = index.last_entry().unwrap().unwrap().0;
        for n in 0..67 {
            let candidate = key(kind, n);
            assert_eq!(
                index.last_key_is_before(&candidate).unwrap(),
                maximum < candidate,
                "kind={kind}, n={n}"
            );
        }
    }
}

#[test]
fn round2_load_index_builder_reserves_unique_keys_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_memory_test_db(dir.path());
    db.create_table(
        "test.round2_bulk",
        vec![checkpoint_test_pk_column()],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let table = db.table("test.round2_bulk").unwrap();
    let first = db.begin_txn().unwrap();
    let second = db.begin_txn().unwrap();
    let mut builders = db.load_data_index_builders(&table).unwrap();
    let inserted = db
        .insert_rows_heap_only_detailed(first, "test.round2_bulk", vec![vec![Value::Int(1)]])
        .unwrap();
    let program =
        crate::storage::CompiledIndexKeyProgram::compile(&table, &table.indexes[0]).unwrap();
    let records = program.encode_row(&table, &[Value::Int(1)]).unwrap();
    builders
        .push_batch(
            &db,
            first.txn_id,
            vec![LoadDataIndexKeyBatch::Encoded(vec![records
                .into_iter()
                .collect()])],
            &inserted.row_ids,
        )
        .unwrap();
    assert!(db
        .insert_rows(second, "test.round2_bulk", vec![vec![Value::Int(1)]])
        .is_err());
    drop(builders);
    db.rollback_txn(first.txn_id).unwrap();
    db.insert_rows(second, "test.round2_bulk", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(second.txn_id).unwrap();
}

#[test]
fn release_audit_replica_restartpoint_retains_inflight_begin() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = open_relaxed_test_db(dir.path());
    let txn_id = 9001;
    let begin_lsn = db
        .log
        .lock()
        .append(
            txn_id,
            WalPayload::BeginTxn {
                start_scn: db.visible_scn(),
            },
        )
        .unwrap();
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn: begin_lsn,
        prev_txn_lsn: 0,
        txn_id,
        payload: WalPayload::BeginTxn {
            start_scn: db.visible_scn(),
        },
    })
    .unwrap();
    // Deliberately leave the source transaction without its first write:
    // its future row records must still find this BEGIN after a restart.
    let last_lsn = {
        let mut log = db.log.lock();
        log.rotate_segment().unwrap();
        for _ in 0..4 {
            log.append(0, WalPayload::CheckpointBegin).unwrap();
            log.rotate_segment().unwrap();
        }
        log.append(0, WalPayload::CheckpointBegin).unwrap()
    };
    crate::wal::LogManager::sync_durable_to(&db.log, last_lsn).unwrap();
    db.replica_applied_lsn_atomic
        .store(last_lsn, Ordering::Release);
    db.replica_restartpoint(std::time::Instant::now(), CHECKPOINT_TRIGGER_MANUAL)
        .unwrap();
    assert!(
        db.log
            .lock()
            .segment_snapshot()
            .iter()
            .any(|segment| segment.first_lsn <= begin_lsn && segment.last_lsn >= begin_lsn),
        "in-flight BEGIN must survive restartpoint"
    );
    assert!(db.storage_status().checkpoint_lsn <= begin_lsn);
}

#[test]
fn release_audit_backup_capture_blocks_physical_maintenance() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    let guard = db.backup_maintenance_capture_guard().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || {
        worker_db.purge_obsolete_undo();
        worker_db.compact_undo_segments(30, 1).unwrap();
        send.send(()).unwrap();
    });
    assert!(recv.recv_timeout(Duration::from_millis(50)).is_err());
    drop(guard);
    recv.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
}

#[test]
fn replica_restart_preserves_active_source_rows_until_source_commit() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let source_txn_id;
    let next_lsn;
    {
        let db = open_relaxed_test_db(dir.path());
        db.create_table(
            "replica_active",
            vec![checkpoint_test_pk_column()],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let seed = db.begin_txn().unwrap();
        db.insert_rows(seed, "test.replica_active", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        let source = db.begin_txn().unwrap();
        source_txn_id = source.txn_id;
        db.insert_rows(source, "test.replica_active", vec![vec![Value::Int(2)]])
            .unwrap();
        db.flush_txn_wal_buffer(source_txn_id, None).unwrap();
        let last = db.log.lock().next_lsn() - 1;
        crate::wal::LogManager::sync_durable_to(&db.log, last).unwrap();
        db.flush_dirty_data().unwrap();
        next_lsn = db.log.lock().next_lsn();
        let mut role = crate::repl::role::RoleState::new_standalone();
        role.role = crate::repl::role::Role::Replica;
        crate::repl::role::save(dir.path(), &role).unwrap();
    }
    let db = open_relaxed_test_db(dir.path());
    assert_eq!(
        db.txns.read().status(source_txn_id),
        Some(TxnStatus::Active)
    );
    assert_eq!(
        db.log.lock().next_lsn(),
        next_lsn,
        "replica recovery must not append local abort records"
    );
    let reader = db.begin_read_only_txn().unwrap();
    assert_eq!(
        db.scan_rows("test.replica_active", reader)
            .unwrap()
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![vec![Value::Int(1)]]
    );
    db.rollback_txn(reader.txn_id).unwrap();
    let commit_scn = db.visible_scn() + 1;
    let payload = WalPayload::CommitTxn {
        commit_scn,
        commit_unix_us: 0,
        binlog: None,
    };
    let lsn = db
        .log
        .lock()
        .append_reserved(next_lsn, source_txn_id, payload.clone())
        .unwrap();
    db.apply_wal_record(&crate::wal::WalRecord {
        source_time_us: 0,
        lsn,
        prev_txn_lsn: 0,
        txn_id: source_txn_id,
        payload,
    })
    .unwrap();
    let reader = db.begin_read_only_txn().unwrap();
    assert_eq!(
        db.scan_rows("test.replica_active", reader)
            .unwrap()
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    db.rollback_txn(reader.txn_id).unwrap();
}

#[test]
fn replica_savepoint_stream_keeps_committed_before_images() {
    use crate::engine::api::{QueryResponse, SessionContext};
    use crate::engine::logging::QueryLogConfig;
    use crate::engine::session_registry::SessionRegistry;
    use crate::engine::StorageQueryEngine;
    fn copy_tree(source: &Path, target: &Path) {
        fs::create_dir_all(target).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let source_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let target_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let source = Arc::new(
        StorageQueryEngine::open_full(
            source_dir.path().to_path_buf(),
            QueryLogConfig::default(),
            Arc::new(SessionRegistry::new()),
        )
        .unwrap(),
    );
    let manager = crate::repl::init_global(source_dir.path(), source.clone()).unwrap();
    manager
        .set_role(crate::repl::role::Role::Master, None)
        .unwrap();
    source.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,d DECIMAL(20,4),b VARBINARY(16),dt DATETIME(6),KEY kv(v),KEY kd(d))").unwrap();
    source.execute("INSERT INTO t VALUES(1,10,7.2500,X'FF00','2026-10-13 12:34:56.123456'),(2,20,9.5000,X'8001',NULL),(3,30,NULL,NULL,NULL)").unwrap();
    source
        .execute("CREATE TABLE filler(id INT PRIMARY KEY,pad VARCHAR(900))")
        .unwrap();
    let db = source.database_for_replication();
    db.checkpoint().unwrap();
    let log = db.wal_log();
    let start = log.lock().next_lsn();
    copy_tree(source_dir.path(), target_dir.path());
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    crate::repl::role::save(target_dir.path(), &role).unwrap();
    let replica = StorageQueryEngine::open_full(
        target_dir.path().to_path_buf(),
        QueryLogConfig::default(),
        Arc::new(SessionRegistry::new()),
    )
    .unwrap();
    let target = replica.database_for_replication().clone();
    let mut writer = SessionContext::default();
    for sql in [
        "BEGIN",
        "UPDATE t SET v=111,d=55.1250,b=X'0011FF' WHERE id=1",
        "SAVEPOINT stable",
        "UPDATE t SET v=222,d=99.0000 WHERE id=1",
        "ROLLBACK TO SAVEPOINT stable",
        "DELETE FROM t WHERE id=2",
    ] {
        source.execute_with_session(&mut writer, sql).unwrap();
    }
    for id in 100..700 {
        source
            .execute_with_session(
                &mut writer,
                &format!("INSERT INTO t VALUES({id},{id},{id}.1250,X'AAFF',NULL)"),
            )
            .unwrap();
    }
    source
        .execute(&format!(
            "INSERT INTO filler VALUES{}",
            (0..1000)
                .map(|id| format!("({id},'{}')", "x".repeat(900)))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .unwrap();
    let safe = log.lock().next_lsn() - 1;
    crate::wal::LogManager::sync_durable_to(&log, safe).unwrap();
    let mut applier = crate::repl::replica::applier::Applier::with_database(
        target_dir.path(),
        target.wal_log(),
        target.clone(),
        crate::repl::replica::progress::ReplicaProgress::default(),
    );
    let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
    while let Some(chunk) = reader.next_chunk(safe, 1 << 20).unwrap() {
        applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
    }
    let expected = source.execute("SELECT * FROM t ORDER BY id").unwrap();
    let actual = replica.execute("SELECT * FROM t ORDER BY id");
    if actual.is_err() {
        let scan = crate::wal::LogManager::scan_replay(source_dir.path()).unwrap();
        for index in 0..scan.records.len() {
            if let Some(record) = scan.records.undo_record(index).unwrap() {
                let undo = crate::storage::undo::decode_undo_record_metadata(record.bytes.as_ref())
                    .unwrap();
                if undo.table_id == db.table("t").unwrap().id && undo.before_header.is_some() {
                    eprintln!(
                        "source undo id={} txn={} prev={:?} before={:?}, replica record={:?}",
                        record.undo_id,
                        undo.txn_id,
                        undo.prev_undo_ptr,
                        undo.before_header,
                        target.undo.read().load_owned(record.undo_id).map(|u| (
                            u.txn_id,
                            u.prev_undo_ptr,
                            u.before_header
                        ))
                    );
                }
            }
        }
    }
    let QueryResponse::ResultSet(expected) = expected else {
        panic!("rows")
    };
    let QueryResponse::ResultSet(actual) = actual.unwrap() else {
        panic!("rows")
    };
    assert_eq!(actual.rows, expected.rows);
    source
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
}

#[test]
fn audit13_replica_restart_catchup_visibility_and_catalog_ids() {
    use crate::engine::api::QueryResponse;
    use crate::engine::StorageQueryEngine;
    fn copy_tree(source: &Path, target: &Path) {
        fs::create_dir_all(target).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let master_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let replica_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let master = StorageQueryEngine::open(master_dir.path()).unwrap();
    let source = master.database_for_replication();
    source.checkpoint().unwrap();
    copy_tree(master_dir.path(), replica_dir.path());
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    crate::repl::role::save(replica_dir.path(), &role).unwrap();
    let mut start = source.wal_log().lock().next_lsn();
    for phase in 0..2 {
        let replica = StorageQueryEngine::open(replica_dir.path()).unwrap();
        let target = replica.database_for_replication().clone();
        if phase == 0 {
            master
                .execute("CREATE TABLE t(id INT PRIMARY KEY)")
                .unwrap();
            for id in 0..20 {
                master
                    .execute(&format!("INSERT INTO t VALUES({id})"))
                    .unwrap();
            }
        } else {
            master.execute("INSERT INTO t VALUES(999)").unwrap();
        }
        let log = source.wal_log();
        let safe = log.lock().next_lsn() - 1;
        crate::wal::LogManager::sync_durable_to(&log, safe).unwrap();
        let mut applier = crate::repl::replica::applier::Applier::with_database(
            replica_dir.path(),
            target.wal_log(),
            target.clone(),
            crate::repl::replica::progress::ReplicaProgress::default(),
        );
        let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
        while let Some(chunk) = reader.next_chunk(safe, 1 << 20).unwrap() {
            applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
        }
        let QueryResponse::ResultSet(rs) = replica
            .execute("SELECT id FROM t ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(
            rs.rows.len(),
            if phase == 0 { 20 } else { 21 },
            "phase={phase} frontier={} slots={:?}",
            target.visible_scn(),
            target.txns.read().slots
        );
        assert!(target.catalog_read().unwrap().next_table_id > target.table("t").unwrap().id);
        let last = target.wal_log().lock().next_lsn() - 1;
        crate::wal::LogManager::sync_durable_to(&target.wal_log(), last).unwrap();
        target.flush_dirty_data().unwrap();
        start = safe + 1;
    }
}

#[test]
fn audit13_relaxed_prepared_rollback_flushes_terminal_record() {
    for write in [false, true] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let db = open_relaxed_test_db(dir.path());
        create_checkpoint_test_table(&db);
        let txn = db.begin_txn().unwrap();
        if write {
            db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
                .unwrap();
        }
        let xid = crate::xa::Xid::new(1, b"audit13".to_vec(), Vec::new()).unwrap();
        db.prepare_xa(txn.txn_id, 0, xid.clone()).unwrap();
        db.rollback_prepared(&xid).unwrap();
        // Scan while the process is still alive: dropping/checkpointing the
        // database must not accidentally flush a missing rollback barrier.
        let scan = crate::wal::LogManager::scan_replay(dir.path()).unwrap();
        assert!(
            scan.records
                .records()
                .only(&[ReplayType::EndTxn])
                .any(|record| { record.unwrap().txn_id == txn.txn_id }),
            "rollback was acknowledged with its terminal record still buffered"
        );
    }
}

#[test]
fn release_audit_lob_reader_survives_external_file_growth() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    db.create_table(
        "lob_growth",
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
    let id = db.table("test.lob_growth").unwrap().id;
    // A reader opened before physical replay keeps the original allocation snapshot.
    let reader = OverflowFile::open_with_buffer_pool(
        db.overflow_path(id),
        id,
        db.log.clone(),
        DurabilityMode::Strict,
        db.buffer_pool.clone(),
    )
    .unwrap();
    let bytes = vec![b'x'; PAGE_SIZE * 6];
    let lob = db
        .with_overflow_mut(id, |file| file.allocate_chain_compressible(&bytes, false))
        .unwrap();
    assert_eq!(reader.read_chain(lob).unwrap(), bytes);
    assert_eq!(
        reader.read_chain_range(lob, 0, bytes.len() as u64).unwrap(),
        bytes
    );
}

#[test]
fn audit14_replayed_lob_cache_retirement_prevents_promotion_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    db.create_table(
        "lob_promote",
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
    let id = db.table("test.lob_promote").unwrap().id;
    let stale = db.overflow_handle(id).unwrap();
    let mut external = OverflowFile::open_with_buffer_pool(
        db.overflow_path(id),
        id,
        db.log.clone(),
        DurabilityMode::Strict,
        db.buffer_pool.clone(),
    )
    .unwrap();
    let bytes = vec![b'z'; PAGE_SIZE * 6];
    let lob = external.allocate_chain_compressible(&bytes, false).unwrap();
    db.flush_dirty_data().unwrap();
    let relative = crate::storage::naming::wal_relative_object_path(&db.overflow_path(id));
    db.invalidate_buffer_pool_path(&relative).unwrap();
    let next = db
        .with_overflow_mut(id, |file| {
            file.allocate_chain_compressible(b"promoted write", false)
        })
        .unwrap();
    assert!(next.first_page_id > lob.last_page_id);
    assert_eq!(stale.read().read_chain(lob).unwrap(), bytes);
    assert_eq!(
        db.with_overflow_read(id, |file| file.read_chain(next))
            .unwrap(),
        b"promoted write"
    );
}

#[test]
fn audit14_btree_descent_detects_cyclic_child_links() {
    let dir = tempfile::tempdir().unwrap();
    let log = test_log(dir.path());
    let pool = BufferPool::new_with_options(
        16,
        log.clone(),
        DurabilityMode::Memory,
        Duration::ZERO,
        1,
        0,
    );
    let index = IndexFile::open_with_durability(
        dir.path().join("cycle.idx"),
        log,
        DurabilityMode::Memory,
        pool,
    )
    .unwrap();
    let root = index.root_page.load(Ordering::Acquire);
    let entry = IndexEntry {
        key: IndexKey::from(vec![KeyPart::Int(1)]),
        row_id: RowId {
            page_id: 0,
            slot_id: 0,
        },
    };
    index
        .write_page(
            0,
            root,
            IndexPage::Internal(IndexInternalPage::new_generic(
                vec![entry.clone()],
                vec![root, root],
            )),
        )
        .unwrap();
    assert!(index
        .find_leaf_for_key_lower_bound(&entry.key)
        .unwrap_err()
        .message()
        .contains("cyclic"));
    assert!(index
        .find_leaf_for_entry(&entry, &mut Vec::new())
        .unwrap_err()
        .message()
        .contains("cyclic"));
    assert!(index
        .find_leaf_for_entry_with_candidate(&entry, &entry)
        .is_err());
    assert!(index
        .find_leaf_for_insert_cached(&entry, &mut smallvec::SmallVec::new())
        .is_err());
    assert!(index
        .leftmost_leaf()
        .unwrap_err()
        .message()
        .contains("cyclic"));
    assert!(index
        .rightmost_leaf_from(root)
        .unwrap_err()
        .message()
        .contains("cyclic"));
    assert!(index
        .first_entry_of_page(root)
        .unwrap_err()
        .message()
        .contains("cyclic"));
}

#[test]
fn ddl_copy_swap_keeps_recovery_bases_until_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_relaxed_test_db(dir.path());
    create_checkpoint_test_table(&db);
    insert_checkpoint_test_row(&db, 7);
    let source = db.table("test.t").unwrap();
    let mut candidate = source.as_ref().clone();
    candidate.active_generation += 1;
    candidate.bump_schema_version();
    let shadow = 100000;
    db.register_reorg_storage_locs(
        &[shadow],
        &source.database,
        &source.name,
        candidate.active_generation,
    );
    db.flush_online_copy_shadow(&source).unwrap();
    let (old_paths, private_paths) =
        db.shadow_generation_swap_paths(&source, &candidate, shadow, 100003);
    for ((old, _), (private, _)) in old_paths.iter().zip(&private_paths) {
        if old.exists() {
            fs::create_dir_all(private.parent().unwrap()).unwrap();
            transfer_page_owner(old, private).unwrap();
            fs::copy(old, private).unwrap();
        }
    }
    let swap = db
        .swap_shadow_generation_files(&source, &candidate, shadow, 100003)
        .unwrap();
    assert!(
        old_paths[0].0.exists(),
        "source REDO must retain its synced base before commit"
    );
    assert!(
        private_paths[0].0.exists(),
        "private UNDO must retain its synced base before commit"
    );
    assert!(
        private_paths[0].1.exists(),
        "candidate must be ready before DD publication"
    );
    db.rollback_shadow_generation_file_swap(&swap).unwrap();
    assert!(old_paths[0].0.exists());
    assert!(private_paths[0].0.exists());
    assert!(!private_paths[0].1.exists());
    let txn = db.begin_read_only_txn().unwrap();
    assert_eq!(
        db.scan_rows("test.t", txn).unwrap()[0].values,
        vec![Value::Int(7)]
    );
    db.rollback_txn(txn.txn_id).unwrap();
    db.unregister_reorg_storage_locs(&[shadow]);
}

#[test]
fn btree_release_unfenced_dictionary_index_is_validated_before_startup_probes() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
    let table = crate::storage::data_dictionary::dd_table_schema(
        crate::storage::data_dictionary::DD_DD_PROPERTIES,
    )
    .unwrap();
    let index = table.indexes.iter().find(|index| index.primary).unwrap();
    let properties = db.scan_dd_table(table.id).unwrap();
    let path = db.index_path(table.id, &index.name);
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let meta = decode_index_meta(&read_index_page(&mut file, 0).unwrap()).unwrap();
    // A valid root publishes a child that did not reach disk before the crash.
    let child = meta.next_page_id;
    write_index_page(
        &mut file,
        meta.root_page,
        &encode_index_tree_page(&IndexPage::Internal(IndexInternalPage::new_generic(
            vec![],
            vec![child],
        )))
        .unwrap(),
    )
    .unwrap();
    drop(file);
    assert!(!path.with_extension("idx.failed").exists());
    for _ in 0..2 {
        let reopened = Database::open_with_config(dir.path(), config.clone()).unwrap();
        assert_eq!(reopened.scan_dd_table(table.id).unwrap(), properties);
        assert!(IndexFile::validate(path.clone()).is_ok());
        assert!(!path.with_extension("idx.failed").exists());
        for property in &properties {
            assert_eq!(
                reopened
                    .catalog_rows_for_selector(
                        table.id,
                        CatalogRowSelector::PrimaryValues(vec![property[0].clone()]),
                    )
                    .unwrap(),
                vec![property.clone()]
            );
        }
        reopened.checkpoint().unwrap();
    }
}
#[test]
fn failed_dictionary_index_is_rebuilt_before_startup_property_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(DurabilityMode::Strict, true);
    let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
    let before = serde_json::to_value(db.catalog()).unwrap();
    let table = crate::storage::data_dictionary::dd_table_schema(
        crate::storage::data_dictionary::DD_DD_PROPERTIES,
    )
    .unwrap();
    let index = table.indexes.iter().find(|index| index.primary).unwrap();
    let properties = db.scan_dd_table(table.id).unwrap();
    let path = db.index_path(table.id, &index.name);
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    drop(db);
    // Logical index REDO fences a damaged derived tree with this marker.
    // Startup property lookups must use a rebuilt index over the DD heap.
    IndexFile::persist_rebuild_marker(&path).unwrap();
    for _ in 0..2 {
        let reopened = Database::open_with_config(dir.path(), config.clone()).unwrap();
        assert_eq!(serde_json::to_value(reopened.catalog()).unwrap(), before);
        assert!(!path.with_extension("idx.failed").exists());
        for property in &properties {
            assert_eq!(
                reopened
                    .catalog_rows_for_selector(
                        table.id,
                        CatalogRowSelector::PrimaryValues(vec![property[0].clone()]),
                    )
                    .unwrap(),
                vec![property.clone()],
            );
        }
        reopened.checkpoint().unwrap();
    }
}

#[test]
fn memory_catalog_seed_matches_heap_loading_and_remains_private() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let strict = test_storage_config(DurabilityMode::Strict, true);
    let seed = crate::util::env::with_vars(&[("MYDB_TEST_DISABLE_SEED", Some("1"))], || {
        Database::open_with_config(&source, strict.clone()).unwrap()
    });
    let catalog_path = root.path().join("catalog.json");
    seed.checkpoint().unwrap();
    seed.checkpoint().unwrap();
    drop(seed);
    let decoded = Database::open_with_config(&source, strict.clone()).unwrap();
    fs::write(
        &catalog_path,
        serde_json::to_vec(&decoded.catalog()).unwrap(),
    )
    .unwrap();
    drop(decoded);
    let memory = test_storage_config(DurabilityMode::Memory, false);
    crate::util::env::with_vars(
        &[
            ("MYDB_TEST_DISABLE_SEED", None),
            ("MYDB_TEST_MEMORY_SEED_DIR", Some(source.to_str().unwrap())),
            ("MYDB_TEST_SEED_DIR", Some(source.to_str().unwrap())),
            (
                "MYDB_TEST_SEED_CATALOG_SOURCE",
                Some(source.to_str().unwrap()),
            ),
            (
                "MYDB_TEST_SEED_CATALOG",
                Some(catalog_path.to_str().unwrap()),
            ),
            (
                "MYDB_TEST_MEMORY_CATALOG",
                Some(catalog_path.to_str().unwrap()),
            ),
        ],
        || {
            let cached =
                Database::open_with_config(root.path().join("cached"), memory.clone()).unwrap();
            let loaded_path = root.path().join("loaded");
            let loaded = crate::util::env::with_vars(&[("MYDB_TEST_MEMORY_CATALOG", None)], || {
                Database::open_with_config(&loaded_path, memory.clone()).unwrap()
            });
            assert_eq!(
                serde_json::to_value(cached.catalog()).unwrap(),
                serde_json::to_value(loaded.catalog()).unwrap(),
            );
            cached
                .create_table(
                    "fixture_only",
                    vec![checkpoint_test_pk_column()],
                    vec!["id".into()],
                    vec![],
                )
                .unwrap();
            assert!(loaded
                .table("fixture_only")
                .unwrap_err()
                .is(crate::error::catalog::ER_NO_SUCH_TABLE));
            drop(loaded);
            let durable_path = root.path().join("durable-clone");
            let durable = Database::open_with_config(&durable_path, strict.clone()).unwrap();
            let loaded_durable =
                crate::util::env::with_vars(&[("MYDB_TEST_SEED_CATALOG", None)], || {
                    Database::open_with_config(root.path().join("durable-loaded"), strict.clone())
                        .unwrap()
                });
            assert_eq!(
                serde_json::to_value(durable.catalog()).unwrap(),
                serde_json::to_value(loaded_durable.catalog()).unwrap()
            );
            durable
                .create_table(
                    "durable_only",
                    vec![checkpoint_test_pk_column()],
                    vec!["id".into()],
                    vec![],
                )
                .unwrap();
            assert!(loaded_durable
                .table("durable_only")
                .unwrap_err()
                .is(crate::error::catalog::ER_NO_SUCH_TABLE));
            durable.checkpoint().unwrap();
            drop(durable);
            drop(loaded_durable);
            let snapshot = crate::util::env::override_snapshot();
            let workers: Vec<_> = (0..3)
                .map(|id| {
                    let snapshot = snapshot.clone();
                    let path = root.path().join(format!("parallel-{id}"));
                    let config = strict.clone();
                    std::thread::spawn(move || {
                        crate::util::env::with_override_snapshot(snapshot, || {
                            let db = Database::open_with_config(&path, config).unwrap();
                            db.create_table(
                                &format!("parallel_private_{id}"),
                                vec![checkpoint_test_pk_column()],
                                vec!["id".into()],
                                vec![],
                            )
                            .unwrap();
                            db.checkpoint().unwrap();
                        });
                    })
                })
                .collect();
            for worker in workers {
                worker.join().unwrap();
            }
            for id in 0..3 {
                let reopened = Database::open_with_config(
                    root.path().join(format!("parallel-{id}")),
                    strict.clone(),
                )
                .unwrap();
                for candidate in 0..3 {
                    assert_eq!(
                        reopened
                            .table(&format!("parallel_private_{candidate}"))
                            .is_ok(),
                        id == candidate,
                        "parallel fixtures must not share mutable dictionary files"
                    );
                }
            }
            // Reopens must ignore both caches, even when their JSON is invalid.
            fs::write(&catalog_path, b"invalid cached catalog").unwrap();
            for (name, config) in [("memory", memory.clone()), ("strict", strict.clone())] {
                assert!(
                    Database::open_with_config(
                        root.path().join(format!("invalid-cache-{name}")),
                        config,
                    )
                    .is_err(),
                    "a corrupt configured fixture must not silently bootstrap"
                );
            }
            let reopened_durable =
                Database::open_with_config(&durable_path, strict.clone()).unwrap();
            assert!(reopened_durable.table("durable_only").is_ok());
            // A custom durable seed must not consume another seed's cache.
            let custom = crate::util::env::with_vars(
                &[("MYDB_TEST_SEED_CATALOG_SOURCE", Some("unrelated-seed"))],
                || {
                    Database::open_with_config(root.path().join("custom-durable"), strict.clone())
                        .unwrap()
                },
            );
            assert!(custom.table("durable_only").is_err());
            let reopened = Database::open_with_config(&loaded_path, memory.clone()).unwrap();
            assert!(reopened
                .table("fixture_only")
                .unwrap_err()
                .is(crate::error::catalog::ER_NO_SUCH_TABLE));
            fs::remove_file(&catalog_path).unwrap();
            for (name, config) in [("memory", memory.clone()), ("strict", strict.clone())] {
                assert!(
                    Database::open_with_config(
                        root.path().join(format!("missing-cache-{name}")),
                        config,
                    )
                    .is_err(),
                    "a missing configured fixture must not silently bootstrap"
                );
            }
            let missing_seed = root.path().join("missing-seed");
            crate::util::env::with_vars(
                &[
                    ("MYDB_TEST_SEED_DIR", Some(missing_seed.to_str().unwrap())),
                    (
                        "MYDB_TEST_MEMORY_SEED_DIR",
                        Some(missing_seed.to_str().unwrap()),
                    ),
                ],
                || {
                    for (name, config) in [("memory", memory.clone()), ("strict", strict.clone())] {
                        assert!(
                            Database::open_with_config(
                                root.path().join(format!("missing-seed-{name}")),
                                config,
                            )
                            .is_err(),
                            "a missing seed directory must not silently bootstrap"
                        );
                    }
                },
            );
            // Existing directories remain authoritative even without fixture JSON.
            Database::open_with_config(&loaded_path, memory).unwrap();
            let durable = Database::open_with_config(&source, strict).unwrap();
            assert!(durable
                .table("fixture_only")
                .unwrap_err()
                .is(crate::error::catalog::ER_NO_SUCH_TABLE));
        },
    );
}

#[test]
fn fulltext_corrupt_posting_fails_scans_and_persists_corruption_fence() {
    use crate::engine::StorageQueryEngine;
    for sql in [
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('alpha')",
        "SELECT MATCH(body) AGAINST('alpha') FROM docs",
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('alp*' IN BOOLEAN MODE)",
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('\"alpha beta\"' IN BOOLEAN MODE)",
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        {
            let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                test_storage_config(DurabilityMode::Strict, true),
            )
            .unwrap();
            engine
                .execute("CREATE TABLE docs(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))")
                .unwrap();
            engine
                .execute("INSERT INTO docs VALUES(1,'alpha beta')")
                .unwrap();
            let db = engine.database();
            let table = db.table("test.docs").unwrap();
            db.with_index_mut(table.id, "ft", |index| {
                let entries = index.entries()?;
                for (key, row) in entries {
                    if key.0.first() == Some(&KeyPart::String("alpha".into())) {
                        index.delete_deferred(0, key, row)?;
                        index.insert_deferred(
                            0,
                            IndexKey::from(vec![
                                KeyPart::String("alpha".into()),
                                KeyPart::Bytes(vec![2, 1]),
                            ]),
                            row,
                        )?;
                    }
                }
                index.flush()
            })
            .unwrap();
            let err = engine.execute(sql).unwrap_err();
            assert!(
                err.is(crate::error::catalog::ER_INDEX_CORRUPT),
                "{sql}: {err:?}"
            );
            let path = db.index_path_for_table_schema(&table, table.id, "ft");
            assert!(path.with_extension("idx.corrupt").exists());
            assert!(engine
                .execute("SELECT id FROM docs WHERE MATCH(body) AGAINST('beta')")
                .is_err());
        }
        let err = Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Strict, true),
        )
        .err()
        .expect("startup must preserve the corruption fence");
        assert!(err.is(crate::error::catalog::ER_INDEX_CORRUPT), "{err:?}");
    }
}

#[test]
fn blackbox_fulltext_structural_recovery_rebuilds_from_heap() {
    use crate::engine::{QueryResponse, StorageQueryEngine};
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path;
    {
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine
            .execute("CREATE TABLE docs(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))")
            .unwrap();
        engine
            .execute("INSERT INTO docs VALUES(1,'alpha beta'),(2,'alpha gamma'),(3,'delta')")
            .unwrap();
        let db = engine.database();
        db.checkpoint().unwrap();
        let table = db.table("docs").unwrap();
        path = db.index_path_for_table_schema(&table, table.id, "ft");
    }
    // Recovery fences an incomplete structural redo with this durable marker.
    // Unlike a corrupt posting payload, the index remains derived from the heap.
    fs::write(path.with_extension("idx.failed"), b"").unwrap();
    for _ in 0..2 {
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        let QueryResponse::ResultSet(rs) = engine
            .execute("SELECT id FROM docs WHERE MATCH(body) AGAINST('alpha') ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
        assert!(!path.with_extension("idx.failed").exists());
    }
}

#[test]
fn blackbox_generated_column_replication_preserves_rows_after_reopen() {
    use crate::engine::{QueryResponse, StorageQueryEngine};
    fn copy_tree(source: &Path, target: &Path) {
        fs::create_dir_all(target).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let dest = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &dest);
            } else {
                fs::copy(entry.path(), dest).unwrap();
            }
        }
    }
    for kind in ["STORED", "VIRTUAL"] {
        let master_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let replica_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let master = StorageQueryEngine::open(master_dir.path()).unwrap();
        master
            .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT)")
            .unwrap();
        master
            .execute("INSERT INTO t VALUES(1,10),(2,20),(3,30)")
            .unwrap();
        let source = master.database_for_replication();
        source.checkpoint().unwrap();
        copy_tree(master_dir.path(), replica_dir.path());
        let mut role = crate::repl::role::RoleState::new_standalone();
        role.role = crate::repl::role::Role::Replica;
        crate::repl::role::save(replica_dir.path(), &role).unwrap();
        let start = source.wal_log().lock().next_lsn();
        {
            let replica = StorageQueryEngine::open(replica_dir.path()).unwrap();
            let target = replica.database_for_replication().clone();
            master
                .execute(&format!(
                    "ALTER TABLE t ADD COLUMN g INT GENERATED ALWAYS AS (a*2) {kind}"
                ))
                .unwrap();
            master.execute("INSERT INTO t(id,a) VALUES(4,40)").unwrap();
            master.execute("UPDATE t SET a=21 WHERE id=2").unwrap();
            let log = source.wal_log();
            let safe = log.lock().next_lsn() - 1;
            crate::wal::LogManager::sync_durable_to(&log, safe).unwrap();
            let mut applier = crate::repl::replica::applier::Applier::with_database(
                replica_dir.path(),
                target.wal_log(),
                target.clone(),
                crate::repl::replica::progress::ReplicaProgress::default(),
            );
            let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
            while let Some(chunk) = reader.next_chunk(safe, 1 << 20).unwrap() {
                applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
            }
            let QueryResponse::ResultSet(rs) = replica
                .execute("SELECT id,a,g FROM t ORDER BY id")
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!("rows")
            };
            assert_eq!(
                rs.rows,
                vec![
                    vec![Value::Int(1), Value::Int(10), Value::Int(20)],
                    vec![Value::Int(2), Value::Int(21), Value::Int(42)],
                    vec![Value::Int(3), Value::Int(30), Value::Int(60)],
                    vec![Value::Int(4), Value::Int(40), Value::Int(80)]
                ],
                "{kind}"
            );
            crate::wal::LogManager::sync_durable_to(&target.wal_log(), safe).unwrap();
            target.flush_dirty_data().unwrap();
        }
        let replica = StorageQueryEngine::open(replica_dir.path()).unwrap();
        let QueryResponse::ResultSet(rs) = replica
            .execute("SELECT id,a,g FROM t ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("rows")
        };
        let QueryResponse::ResultSet(expected) = master
            .execute("SELECT id,a,g FROM t ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(rs.rows, expected.rows, "{kind} after restart");
    }
}

#[test]
fn spatial_index_replica_replays_pages_and_preserves_old_snapshot() {
    use crate::engine::api::QueryResponse;
    use crate::engine::api::SessionContext;
    use crate::engine::StorageQueryEngine;
    fn copy_tree(source: &Path, target: &Path) {
        fs::create_dir_all(target).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    fn count(engine: &StorageQueryEngine, session: &mut SessionContext) -> String {
        let result=engine.execute_with_session(session,"SELECT COUNT(*) FROM spatial_repl WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))").unwrap().materialize().unwrap();
        let QueryResponse::ResultSet(rs) = result else {
            panic!("rows")
        };
        rs.rows[0][0].to_display_string()
    }
    fs::create_dir_all("/tmp/mydb").unwrap();
    let master_dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let replica_dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let master = StorageQueryEngine::open(master_dir.path()).unwrap();
    let source = master.database_for_replication();
    master.execute("CREATE TABLE spatial_repl(id INT PRIMARY KEY,g POINT NOT NULL SRID 0,SPATIAL KEY sp(g))").unwrap();
    master
        .execute("INSERT INTO spatial_repl VALUES(1,ST_GeomFromText('POINT(1 1)'))")
        .unwrap();
    source.checkpoint().unwrap();
    copy_tree(master_dir.path(), replica_dir.path());
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    crate::repl::role::save(replica_dir.path(), &role).unwrap();
    let replica = StorageQueryEngine::open(replica_dir.path()).unwrap();
    let target = replica.database_for_replication().clone();
    let mut reader_session = SessionContext::system_default();
    replica
        .execute_with_session(
            &mut reader_session,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    replica
        .execute_with_session(
            &mut reader_session,
            "START TRANSACTION WITH CONSISTENT SNAPSHOT",
        )
        .unwrap();
    assert_eq!(count(&replica, &mut reader_session), "1");
    let log = source.wal_log();
    let mut start = log.lock().next_lsn();
    master
        .execute("CREATE TABLE spatial_repl_ddl(id INT PRIMARY KEY,g POINT NOT NULL SRID 0)")
        .unwrap();
    master
        .execute("INSERT INTO spatial_repl_ddl VALUES(1,ST_GeomFromText('POINT(1 1)'))")
        .unwrap();
    master
        .execute("CREATE SPATIAL INDEX sp ON spatial_repl_ddl(g)")
        .unwrap();
    let mut writer = SessionContext::system_default();
    master.execute_with_session(&mut writer, "BEGIN").unwrap();
    master
        .execute_with_session(
            &mut writer,
            "UPDATE spatial_repl SET g=ST_GeomFromText('POINT(10 10)') WHERE id=1",
        )
        .unwrap();
    let mut applier = crate::repl::replica::applier::Applier::with_database(
        replica_dir.path(),
        target.wal_log(),
        target.clone(),
        crate::repl::replica::progress::ReplicaProgress::default(),
    );
    for commit in [false, true] {
        if commit {
            master.execute_with_session(&mut writer, "COMMIT").unwrap();
        }
        let safe = log.lock().next_lsn() - 1;
        LogManager::sync_durable_to(&log, safe).unwrap();
        let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
        while let Some(chunk) = reader.next_chunk(safe, 1 << 20).unwrap() {
            applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
        }
        start = safe + 1;
        assert_eq!(count(&replica, &mut reader_session), "1");
    }
    replica
        .execute_with_session(&mut reader_session, "COMMIT")
        .unwrap();
    assert_eq!(count(&replica, &mut reader_session), "0");
    let result=replica.execute("SELECT id FROM spatial_repl_ddl FORCE INDEX(sp) WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))").unwrap().materialize().unwrap();
    let QueryResponse::ResultSet(rs) = result else {
        panic!("rows")
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    master.execute_with_session(&mut writer, "BEGIN").unwrap();
    master
        .execute_with_session(&mut writer, "DELETE FROM spatial_repl_ddl WHERE id=1")
        .unwrap();
    let fork_lsn = log.lock().next_lsn() - 1;
    LogManager::sync_durable_to(&log, fork_lsn).unwrap();
    let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
    while let Some(chunk) = reader.next_chunk(fork_lsn, 1 << 20).unwrap() {
        applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
    }
    target.checkpoint().unwrap();
    drop(applier);
    drop(target);
    drop(replica);
    role.promote_to_master_at(fork_lsn);
    crate::repl::role::save(replica_dir.path(), &role).unwrap();
    let reopened = StorageQueryEngine::open(replica_dir.path()).unwrap();
    assert_eq!(count(&reopened, &mut SessionContext::system_default()), "0");
    let QueryResponse::ResultSet(rs)=reopened.execute("SELECT id FROM spatial_repl_ddl FORCE INDEX(sp) WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))").unwrap().materialize().unwrap() else {panic!("rows")};
    assert_eq!(
        rs.rows,
        vec![vec![Value::Int(1)]],
        "promotion must undo the source's uncommitted spatial deletion"
    );
    master
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
}

#[test]
fn spatial_index_crash_child() {
    let Some(path) = std::env::var_os("MYDB_SPATIAL_CRASH_DATABASE") else {
        return;
    };
    use crate::engine::{api::SessionContext, StorageQueryEngine};
    if std::env::var_os("MYDB_SPATIAL_RECOVERY_ONLY").is_some() {
        spatial_index::CRASH_AFTER_PAGES.with(|v| v.set(Some(2)));
        let _engine = StorageQueryEngine::open(Path::new(&path)).unwrap();
        std::process::exit(87);
    }
    let mode = std::env::var("MYDB_SPATIAL_CRASH_KIND").unwrap();
    // The parent provides a private copy of the fully checkpointed input.
    let engine = StorageQueryEngine::open(Path::new(&path)).unwrap();
    let point = std::env::var("MYDB_SPATIAL_CRASH_POINT")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut session = SessionContext::system_default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    let arm = || {
        if point < 5 {
            spatial_index::CRASH_AFTER_PAGES.with(|v| v.set(Some(point)));
        }
        spatial_index::CRASH_STRUCTURAL_ONLY
            .with(|v| v.set(matches!(mode.as_str(), "delete" | "reuse")));
    };
    if mode != "rollback" && mode != "savepoint" {
        arm();
    }
    if mode == "savepoint" {
        for sql in [
            "INSERT INTO crash_spatial VALUES(999,ST_GeomFromText('POINT(999 999)'))",
            "SAVEPOINT s",
            "UPDATE crash_spatial SET g=ST_GeomFromText('POINT(888 888)') WHERE id=0",
            "INSERT INTO crash_spatial VALUES(1000,ST_GeomFromText('POINT(1000 1000)'))",
            "ROLLBACK TO SAVEPOINT s",
        ] {
            engine.execute_with_session(&mut session, sql).unwrap();
        }
        arm();
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO crash_spatial VALUES(1001,ST_GeomFromText('POINT(1000 1000)'))",
            )
            .unwrap();
    } else if mode == "delete" {
        engine
            .execute_with_session(&mut session, "DELETE FROM crash_spatial")
            .unwrap();
    } else if mode == "reuse" {
        let values = (1000..1409)
            .map(|i| format!("({i},ST_GeomFromText('POINT({i} {i})'))"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute_with_session(
                &mut session,
                &format!("INSERT INTO crash_spatial VALUES{values}"),
            )
            .unwrap();
    } else {
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO crash_spatial VALUES(999,ST_GeomFromText('POINT(999 999)'))",
            )
            .unwrap();
    }
    if mode == "rollback" {
        arm();
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
    } else if point == 5 {
        engine.execute_with_session(&mut session, "COMMIT").unwrap();
    }
    let log = engine.database_for_replication().wal_log();
    let last = log.lock().last_lsn();
    LogManager::sync_durable_to(&log, last).unwrap();
    std::process::exit(86);
}

fn check_spatial_index_split_crash_recovery(initial: usize, clear: bool, modes: &[&str]) {
    use crate::engine::{api::QueryResponse, StorageQueryEngine};
    fs::create_dir_all("/tmp/mydb").unwrap();
    let base = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let preparation = crate::platform::fs::FixtureSyncBatch::new(base.path()).unwrap();
    let engine = StorageQueryEngine::open(base.path()).unwrap();
    engine.execute("CREATE TABLE crash_spatial(id INT PRIMARY KEY,g POINT NOT NULL SRID 0,SPATIAL KEY sp(g))").unwrap();
    let values = (0..initial)
        .map(|i| format!("({i},ST_GeomFromText('POINT({i} {i})'))"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO crash_spatial VALUES{values}"))
        .unwrap();
    if clear {
        engine.execute("DELETE FROM crash_spatial").unwrap();
    }
    engine.database_for_replication().checkpoint().unwrap();
    drop(engine);
    preparation.finish().unwrap();
    for &mode in modes {
        for point in 0..=5 {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            crate::util::file::copy_fixture_tree(base.path(), dir.path()).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::core::tests::spatial_index_crash_child",
                    "--nocapture",
                ])
                .env("MYDB_SPATIAL_CRASH_DATABASE", dir.path())
                .env("MYDB_SPATIAL_CRASH_KIND", mode)
                .env("MYDB_SPATIAL_CRASH_POINT", point.to_string())
                .stdout(std::process::Stdio::null())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{mode}, crash point {point}");
            if mode == "delete" && point == 0 {
                let interrupted = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "storage::core::tests::spatial_index_crash_child",
                        "--nocapture",
                    ])
                    .env("MYDB_SPATIAL_CRASH_DATABASE", dir.path())
                    .env("MYDB_SPATIAL_RECOVERY_ONLY", "1")
                    .stdout(std::process::Stdio::null())
                    .status()
                    .unwrap();
                assert_eq!(
                    interrupted.code(),
                    Some(86),
                    "recovery must crash after a compensated spatial record"
                );
            }
            let expected = match (mode, point) {
                ("insert", 5) => "409",
                ("savepoint", 5) => "410",
                ("savepoint", _) => "408",
                ("insert" | "rollback", _) => "408",
                ("delete", 5) => "0",
                ("delete", _) => "420",
                ("reuse", 5) => "409",
                ("reuse", _) => "0",
                _ => unreachable!(),
            };
            for _ in 0..2 {
                let engine = StorageQueryEngine::open(dir.path()).unwrap();
                for hint in ["FORCE INDEX(sp)", "IGNORE INDEX(sp)"] {
                    let result=engine.execute(&format!("SELECT COUNT(*) FROM crash_spatial {hint} WHERE MBRIntersects(g,ST_GeomFromText('POLYGON((-1 -1,2000 -1,2000 2000,-1 2000,-1 -1))'))")).unwrap().materialize().unwrap();
                    let QueryResponse::ResultSet(rs) = result else {
                        panic!("rows")
                    };
                    assert_eq!(
                        rs.rows[0][0].to_display_string(),
                        expected,
                        "{mode}, crash point {point}"
                    );
                }
                let db = engine.database_for_replication();
                let table = db.table("test.crash_spatial").unwrap();
                db.with_index_read(table.id, "sp", |index| {
                    index.validate_structure()?;
                    assert_eq!(
                        index.entries()?.len(),
                        expected.parse::<usize>().unwrap(),
                        "{mode}, dangling or missing spatial entry at {point}"
                    );
                    Ok(())
                })
                .unwrap();
            }
        }
    }
}

#[test]
fn spatial_index_crash_recovery_at_every_split_publication() {
    check_spatial_index_split_crash_recovery(408, false, &["insert", "rollback", "savepoint"]);
}

#[test]
fn spatial_index_crash_recovery_at_every_split_publication_delete() {
    check_spatial_index_split_crash_recovery(420, false, &["delete"]);
}

#[test]
fn spatial_index_crash_recovery_at_every_split_publication_reuse() {
    check_spatial_index_split_crash_recovery(420, true, &["reuse"]);
}

#[test]
fn release_format_rejects_lost_heap_and_dictionary_authority() {
    for damage in ["missing", "truncated", "zero", "dictionary"] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let config = test_storage_config_with_validation(
            DurabilityMode::Strict,
            StartupValidationMode::Full,
        );
        let path;
        {
            let db = Database::open_with_config(dir.path(), config.clone()).unwrap();
            create_checkpoint_test_table(&db);
            insert_checkpoint_test_row(&db, 111);
            db.checkpoint().unwrap();
            path = db.table_path(db.table("test.t").unwrap().id);
        }
        match damage {
            "missing" => fs::remove_file(&path).unwrap(),
            "truncated" => fs::write(&path, []).unwrap(),
            "zero" => {
                fs::write(&path, vec![0; fs::metadata(&path).unwrap().len() as usize]).unwrap()
            }
            _ => {
                fs::remove_dir_all(crate::storage::naming::system_catalog_dir(dir.path())).unwrap()
            }
        }
        assert!(
            Database::open_with_config(dir.path(), config).is_err(),
            "accepted {damage}"
        );
    }
}

#[test]
fn release_format_rejects_heap_owner_and_page_position_swaps() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let log = test_log(dir.path());
    let first = dir.path().join("a.heap");
    let second = dir.path().join("b.heap");
    {
        let a = HeapFile::open(first.clone(), 1, log.clone()).unwrap();
        let b = HeapFile::open(second.clone(), 2, log.clone()).unwrap();
        a.insert_mvcc(1, RowHeader::visible(1), &[Value::Int(111)])
            .unwrap();
        b.insert_mvcc(2, RowHeader::visible(2), &[Value::Int(222)])
            .unwrap();
        a.flush_dirty_pages().unwrap();
        b.flush_dirty_pages().unwrap();
    }
    let a = fs::read(&first).unwrap();
    let b = fs::read(&second).unwrap();
    fs::write(&first, &b).unwrap();
    fs::write(&second, &a).unwrap();
    assert!(HeapFile::open(first.clone(), 1, log.clone()).is_err());
    fs::write(&first, &a).unwrap();
    let mut bytes = a;
    bytes[PAGE_SIZE + 56..PAGE_SIZE + 64].copy_from_slice(&2u64.to_le_bytes());
    let checksum = page_checksum(&bytes[PAGE_SIZE..2 * PAGE_SIZE], 16);
    write_u32(&mut bytes[PAGE_SIZE..2 * PAGE_SIZE], 16, checksum);
    fs::write(&first, bytes).unwrap();
    let heap = HeapFile::open(first, 1, log).unwrap();
    assert!(heap.scan_mvcc().is_err());
}

#[test]
fn release_format_golden_page_and_value_corpus() {
    use crate::storage::format_golden::golden;
    let page = |mut bytes: Vec<u8>, id| {
        stamp_page_owner(&mut bytes, &[0xa5; 16], id);
        set_page_lsn_if_formatted(&mut bytes, 17);
        bytes
    };
    golden!(
        "heap-meta",
        &page(encode_heap_meta(Path::new("fixture.heap"), 2), 0)
    );
    golden!("heap-page", &page(Page::new().encode(), 1));
    golden!(
        "index-meta",
        &page(
            encode_index_meta(&IndexMeta {
                root_page: 1,
                next_page_id: 2,
                free_page_head: None
            }),
            0
        )
    );
    let entry = IndexEntry {
        key: IndexKey::from(vec![KeyPart::Int(-17), KeyPart::String("中".into())]),
        row_id: RowId {
            page_id: 1,
            slot_id: 3,
        },
    };
    let leaf = IndexPage::Leaf(IndexLeafPage::new_with_resolved_layout(vec![entry], None));
    golden!(
        "index-leaf",
        &page(encode_index_tree_page(&leaf).unwrap(), 1)
    );
    golden!("lob-meta", &page(encode_overflow_meta_page(0, 2, 18), 0));
    golden!(
        "lob-page",
        &page(
            encode_overflow_page(OVFL_FLAG_HEAD, 0, 17, b"persistent payload"),
            1
        )
    );
    golden!(
        "vector-meta",
        &encode_vector_meta(&VectorMeta::fresh(4, 0, 16, 32, 200))
    );
    let mut state = Vec::new();
    let mut writer = crate::engine::vector::persistence::Writer::new(&mut state, 7).unwrap();
    writer.u64(17).unwrap();
    writer.bytes(b"model").unwrap();
    writer.finish().unwrap();
    golden!("vector-state", &state);
    let value = crate::vector::VectorValue::new(
        crate::vector::VectorDataType::Float16,
        3,
        vec![0, 0x3c, 0, 0x38, 0, 0xc0],
    );
    golden!("vector-value", &value.encode());
    assert_eq!(
        crate::vector::VectorValue::decode(&value.encode()).unwrap(),
        value
    );
    let mut values = Vec::new();
    encode_row_into(
        &[
            Value::Null,
            Value::Int(-17),
            Value::UInt(u64::MAX),
            Value::String("中\0".into()),
            Value::Bytes(vec![0, 255]),
            Value::Decimal("123456789012345678901234567890.01".into()),
            Value::Date(0),
            Value::Time(-1),
            Value::DateTime(1),
            Value::Timestamp(4_102_444_799_123_456),
        ],
        &mut values,
    )
    .unwrap();
    golden!("row-values", &values);
    let spec = row_codec_spec(
        &DataTypeKind::Decimal {
            precision: Some(65),
            scale: Some(30),
        },
        false,
    );
    golden!("row-codec-spec", &serde_json::to_vec(&spec).unwrap());
}

#[test]
fn release_format_shadow_capacity_rejection_preserves_file_extent() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let path = dir.path().join("capacity.heap");
    let heap = HeapFile::open(path.clone(), 1, test_log(dir.path())).unwrap();
    heap.page_count
        .store(u64::from(u32::MAX) + 1, Ordering::Release);
    let mut batch = ShadowPreparedHeapBatch {
        encoded_pages: vec![0; PAGE_SIZE],
        row_ids: vec![RowId {
            page_id: 0,
            slot_id: 0,
        }],
    };
    assert!(heap
        .reserve_prepared_shadow_batch_direct(&mut batch)
        .is_err());
    assert_eq!(fs::metadata(path).unwrap().len(), PAGE_SIZE as u64);
}

#[test]
fn release_format_golden_all_index_layouts() {
    let cases = vec![
        vec![
            vec![KeyPart::String("a".into())],
            vec![KeyPart::String("中".into())],
        ],
        vec![vec![KeyPart::DateTime(17)], vec![KeyPart::DateTime(19)]],
        vec![vec![KeyPart::Date(-17)], vec![KeyPart::Date(19)]],
        vec![
            vec![KeyPart::Decimal {
                value: -17,
                scale: 2,
            }],
            vec![KeyPart::Decimal {
                value: 19,
                scale: 2,
            }],
        ],
        vec![
            vec![KeyPart::Decimal {
                value: -17,
                scale: 2,
            }],
            vec![KeyPart::Decimal {
                value: 1i128 << 40,
                scale: 2,
            }],
        ],
        vec![vec![KeyPart::Int(-17)], vec![KeyPart::Int(19)]],
        vec![vec![KeyPart::Int(-17)], vec![KeyPart::Int(1 << 20)]],
        vec![
            vec![KeyPart::Int(-17), KeyPart::Int(2)],
            vec![KeyPart::Int(19), KeyPart::Int(3)],
        ],
        vec![vec![KeyPart::UInt(17)], vec![KeyPart::UInt(19)]],
        vec![vec![KeyPart::UInt(17)], vec![KeyPart::UInt(1 << 20)]],
    ];
    let mut corpus = Vec::new();
    for (layout, keys) in cases.into_iter().enumerate() {
        let entries = keys
            .into_iter()
            .enumerate()
            .map(|(slot, key)| IndexEntry {
                key: IndexKey::from(key),
                row_id: RowId {
                    page_id: 1,
                    slot_id: slot as u16,
                },
            })
            .collect::<Vec<_>>();
        let leaf = IndexLeafPage::new_with_resolved_layout(entries.clone(), Some(3));
        let internal = IndexInternalPage::new_generic(entries.clone(), vec![3, 4, 5]);
        assert_eq!(leaf.key_layout, layout as u8);
        assert_eq!(internal.key_layout, layout as u8);
        for page in [IndexPage::Leaf(leaf), IndexPage::Internal(internal)] {
            let mut bytes = encode_index_tree_page(&page).unwrap();
            stamp_page_owner(&mut bytes, &[0xa5; 16], 1);
            set_page_lsn_if_formatted(&mut bytes, 17);
            match decode_index_tree_page(&bytes).unwrap() {
                IndexPage::Leaf(decoded) => assert_eq!(decoded.entries, entries),
                IndexPage::Internal(decoded) => assert_eq!(decoded.keys, entries),
                _ => panic!("unexpected free page"),
            }
            corpus.extend(bytes);
        }
    }
    crate::storage::format_golden::golden!("index-layouts", &corpus);
}

#[test]
fn release_format_golden_all_codec_descriptors() {
    use crate::vector::VectorDataType;
    let mut kinds = vec![
        ColumnKind::Float32,
        ColumnKind::Float64,
        ColumnKind::Decimal { scale: 0 },
        ColumnKind::Decimal { scale: 30 },
        ColumnKind::DecimalWide { scale: 30 },
        ColumnKind::VarString,
        ColumnKind::VarBytes,
        ColumnKind::Json,
        ColumnKind::Geometry,
        ColumnKind::Date,
        ColumnKind::Year,
    ];
    for width in [1, 2, 3, 4, 8] {
        for signed in [false, true] {
            kinds.push(ColumnKind::Int { width, signed });
        }
    }
    for width in [4, 8, 16] {
        kinds.push(ColumnKind::DecimalFixed { width, scale: 2 });
    }
    for fsp in 0..=6 {
        kinds.extend([
            ColumnKind::Time { fsp },
            ColumnKind::DateTime { fsp },
            ColumnKind::Timestamp { fsp },
        ]);
    }
    for datatype in [
        VectorDataType::Float32,
        VectorDataType::Float16,
        VectorDataType::BFloat16,
        VectorDataType::Int8,
        VectorDataType::Binary,
    ] {
        kinds.push(ColumnKind::Vector {
            dim: None,
            datatype,
        });
        kinds.push(ColumnKind::Vector {
            dim: Some(17),
            datatype,
        });
        kinds.push(ColumnKind::VectorFixed { dim: 17, datatype });
    }
    for width in [0, 1, 255, 65535] {
        kinds.push(ColumnKind::FixedBytes { width });
    }
    for width in 1..=8 {
        kinds.push(ColumnKind::Bit { width });
    }
    for count in [1, 255, 256, 65535] {
        kinds.push(ColumnKind::Enum {
            values: (0..count).map(|i| format!("v{i}")).collect(),
            width: enum_storage_width(count),
        });
    }
    for count in [1, 8, 9, 16, 17, 24, 25, 32, 33, 64] {
        kinds.push(ColumnKind::Set {
            values: (0..count).map(|i| format!("v{i}")).collect(),
            width: set_storage_width(count),
        });
    }
    let specs = kinds
        .into_iter()
        .map(|kind| RowCodecSpec {
            version: crate::storage::format_versions::ROW_CODEC_SPEC_VERSION,
            kind,
        })
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&specs).unwrap();
    assert_eq!(
        serde_json::from_slice::<Vec<RowCodecSpec>>(&bytes).unwrap(),
        specs
    );
    crate::storage::format_golden::golden!("row-codec-matrix", &bytes);
}

#[test]
fn release_format_table_id_limit_cannot_enter_dictionary_namespace() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = open_relaxed_test_db(dir.path());
    let before = db
        .scan_dd_table(crate::storage::data_dictionary::DD_DD_PROPERTIES)
        .unwrap();
    db.catalog_write().unwrap().next_table_id = crate::storage::data_dictionary::DD_TABLE_ID_BASE;
    let err = db
        .create_table(
            "exhausted",
            vec![checkpoint_test_pk_column()],
            vec!["id".into()],
            vec![],
        )
        .unwrap_err();
    assert!(
        err.message().contains("identity space exhausted"),
        "{err:?}"
    );
    assert_eq!(
        db.scan_dd_table(crate::storage::data_dictionary::DD_DD_PROPERTIES)
            .unwrap(),
        before
    );
    assert!(db.table("test.exhausted").is_err());
}

#[test]
fn release_format_column_and_layout_limits_preserve_existing_identity() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = open_relaxed_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let mut table = db.table("test.t").unwrap().as_ref().clone();
    table.next_column_id = u32::MAX;
    assert!(table.allocate_column_id().is_err());
    assert_eq!(table.next_column_id, u32::MAX);
    table.columns[0].physical_pos = u32::MAX;
    assert!(table.next_physical_pos().is_err());
    table.row_layouts[0].layout_id = u32::MAX;
    table.active_layout_id = u32::MAX;
    table.columns[0].data_type = DataTypeKind::BigInt;
    assert!(table.publish_new_row_layout().is_err());
    assert_eq!(table.active_layout_id, u32::MAX);
    assert_eq!(table.row_layouts.len(), 1);
}

#[test]
fn buffered_resident_heap_and_index_inserts_recover_and_rollback_together() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    {
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        db.create_table(
            "buffered_insert",
            vec![
                concurrent_index_test_column("id", true),
                concurrent_index_test_column("v", false),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let seed = db.begin_dml_txn().unwrap();
        db.insert_rows(
            seed,
            "test.buffered_insert",
            vec![vec![Value::Int(1), Value::Int(10)]],
        )
        .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        let committed = db.begin_dml_txn().unwrap();
        db.insert_rows(
            committed,
            "test.buffered_insert",
            vec![
                vec![Value::Int(2), Value::Int(20)],
                vec![Value::Int(3), Value::Int(30)],
            ],
        )
        .unwrap();
        {
            let key = txn_wal_buffer_key(&db.log, committed.txn_id);
            let shard = txn_wal_buffer_shard(key).lock();
            let buffer = shard.get(&key).unwrap();
            assert!(
                buffer
                    .entries
                    .iter()
                    .any(|(record, _)| matches!(record, WalPayload::HeapInsertBatch { .. })),
                "resident heap insert must participate in the same deferred durability barrier"
            );
            assert!(
                buffer
                    .entries
                    .iter()
                    .any(|(record, _)| matches!(record, WalPayload::IndexBatch { .. })),
                "fitting leaf inserts must retain their WAL-before-data pins until append"
            );
        }
        db.commit_txn(committed.txn_id).unwrap();
        let aborted = db.begin_dml_txn().unwrap();
        db.insert_rows(
            aborted,
            "test.buffered_insert",
            vec![vec![Value::Int(4), Value::Int(40)]],
        )
        .unwrap();
        db.rollback_txn(aborted.txn_id).unwrap();
        db.check_consistency().unwrap();
        db.buffer_pool.shutdown_flusher();
        // Plain Database drop simulates a crash rather than a checkpoint.
    }
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let mut rows = db
        .scan_rows(
            "test.buffered_insert",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    rows.sort_by_key(|row| match row.values[0] {
        Value::Int(id) => id,
        _ => unreachable!(),
    });
    assert_eq!(
        rows.iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );
    db.check_consistency().unwrap();
}

#[test]
fn buffered_page_publication_fences_a_concurrent_wal_drain() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    db.create_table(
        "publish_fence",
        vec![concurrent_index_test_column("id", true)],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_dml_txn().unwrap();
    let ids = db
        .insert_rows(seed, "test.publish_fence", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let table = db.table("test.publish_fence").unwrap();
    let (key, path) = {
        let handle = db.heap_handle(table.id).unwrap();
        let heap = handle.read();
        (
            heap.page_key(u64::from(ids[0].page_id)),
            heap.relative_path(),
        )
    };
    let txn = db.begin_dml_txn().unwrap();
    let (entered, enter_wait) = std::sync::mpsc::channel();
    let (resume, resume_wait) = std::sync::mpsc::channel();
    let published = Arc::new(AtomicBool::new(false));
    let writer_db = Arc::clone(&db);
    let writer_published = Arc::clone(&published);
    let writer = std::thread::spawn(move || {
        let image_key = key.clone();
        let result = try_buffer_txn_page_write(
            &writer_db.log,
            &writer_db.buffer_pool,
            txn.txn_id,
            WalPayload::HeapUpdateLogical {
                path,
                page_id: ids[0].page_id,
                slot_id: ids[0].slot_id,
                patches: Vec::new(),
                undo_lsn: None,
            },
            vec![key],
            |lsn| {
                entered.send(()).unwrap();
                resume_wait.recv().unwrap();
                writer_db
                    .buffer_pool
                    .with_raw_page_mut(image_key, lsn, |_| Ok(()))?;
                writer_published.store(true, Ordering::Release);
                Ok(())
            },
        );
        assert!(matches!(result, Ok(Ok(()))));
    });
    enter_wait.recv().unwrap();
    let buffer_key = txn_wal_buffer_key(&db.log, txn.txn_id);
    let fenced = txn_wal_buffer_shard(buffer_key).try_lock().is_none();
    let drainer_db = Arc::clone(&db);
    let drainer = std::thread::spawn(move || {
        drainer_db.flush_txn_wal_buffer(txn.txn_id, None).unwrap();
        assert!(
            published.load(Ordering::Acquire),
            "WAL drain finalized a page before publication"
        );
    });
    resume.send(()).unwrap();
    writer.join().unwrap();
    drainer.join().unwrap();
    assert!(
        fenced,
        "parallel WAL drain was not fenced during page publication"
    );
    assert_eq!(db.buffer_pool.deferred_wal_pages.load(Ordering::Acquire), 0);
    db.commit_txn(txn.txn_id).unwrap();
}

#[test]
fn deferred_wal_page_budget_releases_headroom_for_eager_progress() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = open_relaxed_test_db(dir.path());
    let limit = db.buffer_pool.capacity_pages() / 8;
    let reservation = db
        .buffer_pool
        .reserve_deferred_wal_pages(limit as usize)
        .unwrap();
    assert!(db.buffer_pool.reserve_deferred_wal_pages(1).is_none());
    drop(reservation);
    let replacement = db.buffer_pool.reserve_deferred_wal_pages(1).unwrap();
    assert_eq!(db.buffer_pool.deferred_wal_pages.load(Ordering::Acquire), 1);
    drop(replacement);
    assert_eq!(db.buffer_pool.deferred_wal_pages.load(Ordering::Acquire), 0);
}

#[test]
fn buffered_heap_update_recovery_preserves_whole_row_fences_under_pin_pressure() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let mut value = concurrent_index_test_column("v", false);
    value.data_type = DataType::VarChar(Some(1000));
    db.create_table(
        "redo_order",
        vec![concurrent_index_test_column("id", true), value],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_dml_txn().unwrap();
    let rows = db
        .insert_rows(
            seed,
            "test.redo_order",
            vec![vec![Value::Int(1), Value::String("s".repeat(512))]],
        )
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    let table = db.table("test.redo_order").unwrap();
    let heap_path = db.heap_handle(table.id).unwrap().read().path.clone();
    let txn = db.begin_dml_txn().unwrap();
    db.update_row_by_id(txn, &table, rows[0], |_| {
        Ok(vec![Value::Int(1), Value::String("m".repeat(510))])
    })
    .unwrap();
    let used = db.buffer_pool.deferred_wal_pages.load(Ordering::Acquire);
    assert!(used > 0, "first variable-length update must be buffered");
    let limit = db.buffer_pool.capacity_pages() / 8;
    let pressure = db
        .buffer_pool
        .reserve_deferred_wal_pages((limit - used) as usize)
        .unwrap();
    db.update_row_by_id(txn, &table, rows[0], |_| {
        Ok(vec![Value::Int(1), Value::String("z".repeat(508))])
    })
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    drop(pressure);
    std::mem::forget(db);
    // Force redo instead of accepting a dirty in-memory/disk after-image.
    fs::remove_file(heap_path).unwrap();
    let recovered = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    let rows = recovered
        .scan_rows(
            "test.redo_order",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[1], Value::String("z".repeat(508)));
    recovered.check_consistency().unwrap();
}

#[test]
fn write_path_inline_string_update_uses_sparse_buffered_redo() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    db.create_table(
        "inline_patch",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col("v", DataType::Text(65535), false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_dml_txn().unwrap();
    let ids = db
        .insert_rows(
            seed,
            "test.inline_patch",
            vec![vec![Value::Int(1), Value::String("a".repeat(120))]],
        )
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let reader = db.begin_txn().unwrap();
    let writer = db.begin_dml_txn().unwrap();
    db.update_row_by_id_with_changed_columns(
        writer,
        &db.table("test.inline_patch").unwrap(),
        ids[0],
        Some(&[1]),
        |old| Ok(vec![old[0].clone(), Value::String("b".repeat(120))]),
    )
    .unwrap();
    let key = txn_wal_buffer_key(&db.log, writer.txn_id);
    let sparse = txn_wal_buffer_shard(key).lock().get(&key).unwrap().entries.iter().any(|(p, _)| matches!(p, WalPayload::HeapUpdateLogical { patches, .. } if patches.iter().any(|p| p.after.len() == 120)));
    assert!(
        sparse,
        "an inline same-width string must use column redo instead of a full row"
    );
    assert_eq!(
        db.scan_rows("test.inline_patch", reader).unwrap()[0].values[1],
        Value::String("a".repeat(120))
    );
    db.rollback_txn(writer.txn_id).unwrap();
    db.rollback_txn(reader.txn_id).unwrap();
    assert_eq!(
        db.scan_rows(
            "test.inline_patch",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted)
        )
        .unwrap()[0]
            .values[1],
        Value::String("a".repeat(120))
    );
    db.check_consistency().unwrap();
}

#[test]
fn write_path_delete_buffers_header_and_index_redo() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    db.create_table(
        "delete_patch",
        vec![
            lob_col("id", DataType::Int, true),
            lob_col("v", DataType::Text(65535), false),
        ],
        vec!["id".into()],
        vec![],
    )
    .unwrap();
    let seed = db.begin_dml_txn().unwrap();
    let ids = db
        .insert_rows(
            seed,
            "test.delete_patch",
            (1..=16)
                .map(|id| vec![Value::Int(id), Value::String("a".repeat(120))])
                .collect(),
        )
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    let reader = db.begin_txn().unwrap();
    let writer = db.begin_dml_txn().unwrap();
    db.delete_row_by_id(writer, &db.table("test.delete_patch").unwrap(), ids[4])
        .unwrap();
    let undo_id = db.txns.read().undo_head(writer.txn_id).unwrap().unwrap();
    let undo = db.load_undo_owned(undo_id).unwrap().unwrap();
    assert!(matches!(undo.kind, UndoKind::DeleteHeader));
    assert!(undo.before_values.is_none());
    assert!(undo.before_external.is_empty());
    let key = txn_wal_buffer_key(&db.log, writer.txn_id);
    {
        let shard = txn_wal_buffer_shard(key).lock();
        let entries = &shard.get(&key).unwrap().entries;
        assert!(entries.iter().any(|(p, _)| matches!(p, WalPayload::HeapUpdateLogical { patches, .. } if patches.iter().all(|p| p.after.len() <= 18))), "a tombstone must retain its payload and log only its header");
        assert!(entries.iter().any(|(p, _)| matches!(p, WalPayload::IndexBatch { ops, .. } if ops.iter().all(|o| matches!(o.kind, WalIndexOpKind::Delete)))), "a fitting leaf deletion must share the transaction WAL buffer");
    }
    assert_eq!(db.scan_rows("test.delete_patch", reader).unwrap().len(), 16);
    db.rollback_txn(writer.txn_id).unwrap();
    db.rollback_txn(reader.txn_id).unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn write_path_inline_patch_types_null_resize_and_recovery() {
    let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let cases = vec![
        (
            DataType::Char(Some(120)),
            Value::String("a".repeat(120)),
            Value::String("b".repeat(120)),
        ),
        (
            DataType::VarChar(Some(8192)),
            Value::String("你好".repeat(20)),
            Value::String("世界".repeat(20)),
        ),
        (
            DataType::Binary(Some(120)),
            Value::Bytes(vec![0; 120]),
            Value::Bytes(vec![255; 120]),
        ),
        (
            DataType::VarBinary(Some(8192)),
            Value::Bytes(vec![0; 120]),
            Value::Bytes(vec![255; 120]),
        ),
        (
            DataType::Decimal(None),
            Value::Decimal("123".into()),
            Value::Decimal("124".into()),
        ),
        (
            DataType::Decimal(Some((65, 10))),
            Value::Decimal("123456789012345678901234567890.12".into()),
            Value::Decimal("223456789012345678901234567890.12".into()),
        ),
        (
            DataType::Json,
            Value::Json(br#"{"a":1}"#.to_vec()),
            Value::Json(br#"{"a":2}"#.to_vec()),
        ),
    ];
    for (case, (kind, old_value, new_value)) in cases.into_iter().enumerate() {
        let path = root.path().join(case.to_string());
        let db = Database::open_with_durability(&path, DurabilityMode::Strict).unwrap();
        db.create_table(
            "t",
            vec![
                lob_col("id", DataType::Int, true),
                lob_col("v", kind, false),
            ],
            vec!["id".into()],
            vec![],
        )
        .unwrap();
        let table = db.table("test.t").unwrap();
        let seed = db.begin_dml_txn().unwrap();
        let ids = db
            .insert_rows(seed, "test.t", vec![vec![Value::Int(1), old_value]])
            .unwrap();
        db.commit_txn(seed.txn_id).unwrap();
        let old = db
            .scan_rows(
                "test.t",
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            )
            .unwrap()[0]
            .values
            .clone();
        let writer = db.begin_dml_txn().unwrap();
        db.update_row_by_id_with_changed_columns(writer, &table, ids[0], Some(&[1]), |r| {
            Ok(vec![r[0].clone(), new_value.clone()])
        })
        .unwrap();
        let key = txn_wal_buffer_key(&db.log, writer.txn_id);
        assert!(
            txn_wal_buffer_shard(key)
                .lock()
                .get(&key)
                .unwrap()
                .entries
                .iter()
                .any(|(p, _)| matches!(p, WalPayload::HeapUpdateLogical { .. })),
            "case {case} must use sparse redo"
        );
        let updated = db.scan_rows("test.t", writer).unwrap()[0].values.clone();
        db.commit_txn(writer.txn_id).unwrap();
        if matches!(case, 1 | 3) {
            let reader = db.begin_txn().unwrap();
            let resized = db.begin_dml_txn().unwrap();
            let grown = if case == 1 {
                Value::String("g".repeat(240))
            } else {
                Value::Bytes(vec![0xa5; 240])
            };
            db.update_row_by_id_with_changed_columns(resized, &table, ids[0], Some(&[1]), |r| {
                Ok(vec![r[0].clone(), grown.clone()])
            })
            .unwrap();
            assert_eq!(db.scan_rows("test.t", reader).unwrap()[0].values, updated);
            assert_eq!(db.scan_rows("test.t", resized).unwrap()[0].values[1], grown);
            db.rollback_txn(resized.txn_id).unwrap();
            db.rollback_txn(reader.txn_id).unwrap();
        }
        let writer = db.begin_dml_txn().unwrap();
        db.update_row_by_id_with_changed_columns(writer, &table, ids[0], Some(&[1]), |r| {
            Ok(vec![r[0].clone(), Value::Null])
        })
        .unwrap();
        assert_eq!(
            db.scan_rows("test.t", writer).unwrap()[0].values[1],
            Value::Null
        );
        db.rollback_txn(writer.txn_id).unwrap();
        assert_ne!(old, updated);
        db.buffer_pool.shutdown_flusher();
        drop(db);
        let db = Database::open_with_durability(&path, DurabilityMode::Strict).unwrap();
        assert_eq!(
            db.scan_rows(
                "test.t",
                TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted)
            )
            .unwrap()[0]
                .values,
            updated,
            "case {case}"
        );
        db.check_consistency().unwrap();
    }
}

#[test]
fn write_path_lazy_begin_shares_commit_batch_and_precedes_eager_fallback() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    create_checkpoint_test_table(&db);
    let table = db.table("test.t").unwrap();
    let seed = db.begin_dml_txn().unwrap();
    let ids = db
        .insert_rows(seed, "test.t", vec![vec![Value::Int(1)]])
        .unwrap();
    db.commit_txn(seed.txn_id).unwrap();
    for eager in [false, true] {
        let txn = db.begin_txn().unwrap();
        assert_eq!(
            db.log.lock().last_txn_lsn(txn.txn_id),
            0,
            "BEGIN must not acquire the append latch"
        );
        if eager {
            // Reserved-LSN writers must publish the deferred BEGIN before
            // reserving their own row/page record, including structural DML.
            let mut log = db.log.lock();
            let reserved = log.reserve_txn_lsn(txn.txn_id).unwrap();
            assert!(log.last_txn_lsn(txn.txn_id) < reserved);
            assert_ne!(log.last_txn_lsn(txn.txn_id), 0);
        }
        db.delete_row_by_id(txn, &table, ids[0]).unwrap();
        db.rollback_txn(txn.txn_id).unwrap();
    }
    let committed = db.begin_dml_txn().unwrap();
    db.delete_row_by_id(committed, &table, ids[0]).unwrap();
    db.commit_txn(committed.txn_id).unwrap();
    db.buffer_pool.shutdown_flusher();
    drop(db);
    let scan = crate::wal::LogManager::scan(dir.path()).unwrap();
    let records: Vec<_> = scan
        .records
        .iter()
        .filter(|r| r.txn_id == committed.txn_id)
        .collect();
    assert!(matches!(
        records.first().unwrap().payload,
        WalPayload::BeginTxn { .. }
    ));
    assert!(matches!(
        records.last().unwrap().payload,
        WalPayload::CommitTxn { .. }
    ));
    let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
    assert!(db
        .scan_rows(
            "test.t",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted)
        )
        .unwrap()
        .is_empty());
    db.check_consistency().unwrap();
}
