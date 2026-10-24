//! Deterministic regressions for the 2026-09-26 B-tree audit.
//! Fixtures exercise multi-level redistribution; fault injection and replay
//! tests cover the storage error and durability contracts.

use super::*;

fn wide_key(v: i64) -> IndexKey {
    IndexKey::from(vec![KeyPart::String(format!("{:0>200}", v.to_string()))])
}

fn leaf_entry(v: i64, seq: usize) -> IndexEntry {
    IndexEntry {
        key: wide_key(v),
        row_id: RowId {
            page_id: (seq as u32) + 100,
            slot_id: (seq % 64) as u16,
        },
    }
}

fn audit_index_raw(name: &str) -> (tempfile::TempDir, IndexFile) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(&dir.path().join("wal")).unwrap(),
    ));
    let pool = BufferPool::new(
        4096,
        log.clone(),
        DurabilityMode::Strict,
        Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
    );
    let index =
        IndexFile::open_with_durability(dir.path().join(name), log, DurabilityMode::Strict, pool)
            .unwrap();
    (dir, index)
}

fn write_leaf_chain(
    index: &IndexFile,
    txn: u64,
    leaves: &[(u64, Vec<IndexEntry>)],
) -> Result<(), QueryError> {
    let n = leaves.len();
    for (i, (page_id, entries)) in leaves.iter().enumerate() {
        let next = if i + 1 < n {
            Some(leaves[i + 1].0)
        } else {
            None
        };
        index.write_page(
            txn,
            *page_id,
            IndexPage::Leaf(IndexLeafPage::new(entries.clone(), next)),
        )?;
    }
    Ok(())
}

// Deletion underflows the left internal node, moving the first right subtree.
#[test]
fn audit_20260926_borrow_right_preserves_lookup_and_separator() {
    let (_dir, index) = audit_index_raw("audit_v2_borrow_right.idx");
    let txn = txn_id_for_ddl();

    let p1 = index.reserve_page_id().unwrap();
    let p2 = index.reserve_page_id().unwrap();
    let l0 = index.reserve_page_id().unwrap();
    let l1 = index.reserve_page_id().unwrap();
    let l2 = index.reserve_page_id().unwrap();
    let mut right_leaves = Vec::new();
    for _ in 0..22 {
        right_leaves.push(index.reserve_page_id().unwrap());
    }

    let left_leaves = vec![
        (l0, vec![leaf_entry(100, 0), leaf_entry(101, 1)]),
        (l1, vec![leaf_entry(150, 2)]),
        (l2, vec![leaf_entry(200, 3)]),
    ];
    let mut right_content = Vec::new();
    let mut seq = 4usize;
    for (i, page_id) in right_leaves.iter().enumerate() {
        let base = 300 + i as i64 * 100;
        right_content.push((
            *page_id,
            vec![
                leaf_entry(base, seq),
                leaf_entry(base + 1, seq + 1),
                leaf_entry(base + 2, seq + 2),
                leaf_entry(base + 3, seq + 3),
            ],
        ));
        seq += 4;
    }
    write_leaf_chain(
        &index,
        txn,
        &[left_leaves.clone(), right_content.clone()].concat(),
    )
    .unwrap();

    index
        .write_page(
            txn,
            p1,
            IndexPage::Internal(IndexInternalPage::new_generic(
                vec![leaf_entry(150, 2), leaf_entry(200, 3)],
                vec![l0, l1, l2],
            )),
        )
        .unwrap();
    let p2_keys: Vec<IndexEntry> = (1..22)
        .map(|i| leaf_entry(300 + i as i64 * 100, 4 + i * 4))
        .collect();
    index
        .write_page(
            txn,
            p2,
            IndexPage::Internal(IndexInternalPage::new_generic(
                p2_keys,
                right_leaves.clone(),
            )),
        )
        .unwrap();
    index
        .write_page(
            txn,
            INDEX_EMPTY_ROOT,
            IndexPage::Internal(IndexInternalPage::new_generic(
                vec![leaf_entry(300, 4)],
                vec![p1, p2],
            )),
        )
        .unwrap();
    index.write_meta_page(txn).unwrap();

    assert!(
        !index.get(&wide_key(302), true).unwrap().is_empty(),
        "precondition: key 302 must be found before the delete"
    );

    index
        .delete_internal(txn, wide_key(101), leaf_entry(101, 1).row_id, false)
        .unwrap();

    let p1_page = index.read_internal(p1).unwrap();
    let borrowed = p1_page.children.len() == 3;
    let root_page = index.read_internal(INDEX_EMPTY_ROOT).unwrap();
    let sep0 = root_page.keys[0].key.clone();

    assert!(borrowed, "fixture must trigger internal right borrow");
    assert_eq!(
        index.get(&wide_key(302), true).unwrap(),
        vec![leaf_entry(302, 6).row_id]
    );
    assert_eq!(sep0, wide_key(400));
    assert_sorted_and_searchable(&index);
}

// Left borrowing must route a later insertion into the moved subtree.
#[test]
fn audit_20260926_borrow_left_preserves_lookup_after_insert() {
    let (_dir, index) = audit_index_raw("audit_v2_borrow_left.idx");
    let txn = txn_id_for_ddl();

    let p1 = index.reserve_page_id().unwrap();
    let p2 = index.reserve_page_id().unwrap();
    let mut left_leaves_ids = Vec::new();
    for _ in 0..22 {
        left_leaves_ids.push(index.reserve_page_id().unwrap());
    }
    let r0 = index.reserve_page_id().unwrap();
    let r1 = index.reserve_page_id().unwrap();
    let r2 = index.reserve_page_id().unwrap();

    let mut left_content = Vec::new();
    let mut seq = 0usize;
    for (i, page_id) in left_leaves_ids.iter().enumerate() {
        let base = 100 + i as i64 * 100;
        left_content.push((
            *page_id,
            vec![
                leaf_entry(base, seq),
                leaf_entry(base + 1, seq + 1),
                leaf_entry(base + 2, seq + 2),
                leaf_entry(base + 3, seq + 3),
            ],
        ));
        seq += 4;
    }
    let right_leaves = vec![
        (r0, vec![leaf_entry(3000, seq), leaf_entry(3001, seq + 1)]),
        (r1, vec![leaf_entry(3050, seq + 2)]),
        (r2, vec![leaf_entry(3100, seq + 3)]),
    ];
    write_leaf_chain(
        &index,
        txn,
        &[left_content.clone(), right_leaves.clone()].concat(),
    )
    .unwrap();

    let p1_keys: Vec<IndexEntry> = (1..22)
        .map(|i| leaf_entry(100 + i as i64 * 100, i * 4))
        .collect();
    index
        .write_page(
            txn,
            p1,
            IndexPage::Internal(IndexInternalPage::new_generic(
                p1_keys,
                left_leaves_ids.clone(),
            )),
        )
        .unwrap();
    index
        .write_page(
            txn,
            p2,
            IndexPage::Internal(IndexInternalPage::new_generic(
                vec![leaf_entry(3050, seq + 2), leaf_entry(3100, seq + 3)],
                vec![r0, r1, r2],
            )),
        )
        .unwrap();
    index
        .write_page(
            txn,
            INDEX_EMPTY_ROOT,
            IndexPage::Internal(IndexInternalPage::new_generic(
                vec![leaf_entry(3000, seq)],
                vec![p1, p2],
            )),
        )
        .unwrap();
    index.write_meta_page(txn).unwrap();

    index
        .delete_internal(txn, wide_key(3001), leaf_entry(3001, seq + 1).row_id, false)
        .unwrap();

    let p2_page = index.read_internal(p2).unwrap();
    let borrowed = p2_page.children.len() == 3 && p2_page.children[0] == left_leaves_ids[21];
    if !borrowed {
        panic!("test shape did not trigger internal borrow_from_left");
    }
    let root_page = index.read_internal(INDEX_EMPTY_ROOT).unwrap();
    let sep0 = root_page.keys[0].key.clone();
    index
        .insert_internal(txn, leaf_entry(2250, 999), false)
        .unwrap();
    assert_eq!(
        index.get(&wide_key(2201), true).unwrap(),
        vec![leaf_entry(2201, 85).row_id]
    );
    assert_eq!(sep0, wide_key(2200));
    assert_sorted_and_searchable(&index);
}

// A generation change must preserve the exclusive lower bound on reseek.
#[test]
fn audit_20260926_cursor_exclusive_low_survives_reposition() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(&dir.path().join("wal")).unwrap(),
    ));
    let index = IndexFile::open(dir.path().join("f1_cursor.idx"), log).unwrap();

    let mut entries: Vec<IndexEntry> = Vec::new();
    let mut seq = 0usize;
    for _ in 0..2000 {
        entries.push(IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(5)]),
            row_id: RowId {
                page_id: seq as u32 / 64,
                slot_id: (seq % 64) as u16,
            },
        });
        seq += 1;
    }
    for k in 6i64..=5000 {
        entries.push(IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(k)]),
            row_id: RowId {
                page_id: seq as u32 / 64,
                slot_id: (seq % 64) as u16,
            },
        });
        seq += 1;
    }
    index.bulk_load_sorted(txn_id_for_ddl(), entries).unwrap();
    let index: IndexHandle = Arc::new(parking_lot::RwLock::new(index));

    let low = IndexBound {
        key: IndexKey::from(vec![KeyPart::Int(5)]),
        inclusive: false,
    };
    let mut cursor = IndexRangeCursor::new(Arc::clone(&index), false, Some(low), None).unwrap();

    let mut first_outputs: Vec<(IndexKey, RowId)> = Vec::new();
    for _ in 0..200 {
        let chunk = cursor.next_chunk(100).unwrap();
        if !chunk.is_empty() {
            first_outputs = chunk;
            break;
        }
    }
    assert!(
        first_outputs
            .iter()
            .all(|(k, _)| matches!(k.0[0], KeyPart::Int(v) if v >= 6)),
        "precondition violated: output below low before any write"
    );

    {
        let file = index.read();
        let _guard = file.begin_mutation();
    }

    let mut leaked = 0usize;
    for _ in 0..50 {
        for (k, _) in cursor.next_chunk(100).unwrap() {
            if matches!(k.0[0], KeyPart::Int(5)) {
                leaked += 1;
            }
        }
        if cursor.is_finished() {
            break;
        }
    }
    assert!(
        leaked == 0,
        "BUG F1 CONFIRMED: {leaked} rows with key=5 (below exclusive low) were emitted after reposition"
    );
}

#[test]
fn audit_20260926_wal_precedes_single_apply() {
    let (_dir, index) = audit_index_raw("wal-before.idx");
    for kind in [WalIndexOpKind::Insert, WalIndexOpKind::Delete] {
        let before = index.log_atomics.next_lsn();
        index
            .apply_single_index_op_logged(41, kind, leaf_entry(1, 1), |index, entry| {
                let lsn = btree_logical_wal::current().expect("real logical LSN before apply");
                assert!(lsn >= before);
                assert!(
                    index.log_atomics.next_lsn() > lsn,
                    "WAL record must already be appended"
                );
                match kind {
                    WalIndexOpKind::Insert => index.insert_entry_concurrent(41, entry),
                    WalIndexOpKind::Delete => index.delete_entry_concurrent(41, entry),
                }
            })
            .unwrap();
        assert!(btree_logical_wal::current().is_none());
    }
    assert!(index.entries().unwrap().is_empty());
}

#[test]
fn audit_20260926_replicated_batch_records_online_build_changes() {
    let (_dir, mut index) = audit_index_raw("online.idx");
    let log = Arc::new(OnlineIndexBuildLog::new());
    index.online_build_log = Some(log.clone());
    let old = leaf_entry(100, 0);
    let new = leaf_entry(200, 0);
    index
        .apply_replicated_batch(
            42,
            vec![
                IndexBatchEntry {
                    kind: WalIndexOpKind::Delete,
                    key: old.key.clone(),
                    row_id: old.row_id,
                },
                IndexBatchEntry {
                    kind: WalIndexOpKind::Insert,
                    key: new.key.clone(),
                    row_id: new.row_id,
                },
            ],
            99,
        )
        .unwrap();
    assert!(index.entries().unwrap().is_empty());
    let recorded = log.take().unwrap();
    assert_eq!(recorded.entries.len(), 2);
    assert_eq!(recorded.entries[0].kind, WalIndexOpKind::Delete);
    assert_eq!(recorded.entries[0].key, old.key);
    assert_eq!(recorded.entries[1].key, new.key);
}

#[test]
fn audit_20260926_empty_leaf_lookup_and_corrupt_cycle() {
    let (_dir, index) = audit_index_raw("empty.idx");
    let right = index.reserve_page_id().unwrap();
    let entry = leaf_entry(100, 0);
    index
        .write_page(
            0,
            right,
            IndexPage::Leaf(IndexLeafPage::new(vec![entry.clone()], None)),
        )
        .unwrap();
    index
        .write_page(
            0,
            INDEX_EMPTY_ROOT,
            IndexPage::Leaf(IndexLeafPage::new(Vec::new(), Some(right))),
        )
        .unwrap();
    assert_eq!(
        index
            .lookup_entries_many_sorted_fallback(&[entry.key.clone()], true, false)
            .unwrap(),
        vec![(entry.key.clone(), entry.row_id)]
    );
    assert_eq!(
        index
            .lookup_entries_many_sorted_fallback(&[entry.key.clone()], false, false)
            .unwrap(),
        vec![(entry.key.clone(), entry.row_id)]
    );
    index
        .write_page(
            0,
            right,
            IndexPage::Leaf(IndexLeafPage::new(Vec::new(), Some(INDEX_EMPTY_ROOT))),
        )
        .unwrap();
    assert!(index
        .get(&entry.key, false)
        .unwrap_err()
        .message()
        .contains("cyclic"));
    assert!(index
        .get_unique_one(&entry.key)
        .unwrap_err()
        .message()
        .contains("cyclic"));
    assert!(index
        .prefix(&entry.key, false)
        .unwrap_err()
        .message()
        .contains("cyclic"));
}

#[test]
fn audit_20260926_structural_write_failure_fences_split_and_merge() {
    for delete in [false, true] {
        let mut failures = 0;
        for nth in 0..6 {
            let (_dir, index) = audit_index_raw("failure.idx");
            index
                .bulk_load_sorted(0, (0..100).map(|i| leaf_entry(i, i as usize)).collect())
                .unwrap();
            let delete_entry = if delete {
                let left = index.leftmost_leaf().unwrap();
                let mut leaf = index.read_leaf(left).unwrap();
                leaf.entries.truncate(2);
                leaf.recompute_cached_size();
                let entry = leaf.entries[1].clone();
                index.write_page(0, left, IndexPage::Leaf(leaf)).unwrap();
                Some(entry)
            } else {
                None
            };
            INDEX_FAIL_WRITE_AFTER.with(|fail| fail.set(Some(nth)));
            let result = if let Some(entry) = delete_entry {
                index.delete_internal(0, entry.key, entry.row_id, false)
            } else {
                let _guard = index.structure_lock.write();
                index.append_sorted_entries(
                    0,
                    (100..300).map(|i| leaf_entry(i, i as usize)).collect(),
                )
            };
            INDEX_FAIL_WRITE_AFTER.with(|fail| fail.set(None));
            if let Err(err) = result {
                failures += 1;
                assert!(err.message().contains("requires rebuild"));
                assert!(index.get(&wide_key(10), false).is_err());
                assert!(index
                    .insert_deferred(42, wide_key(400), leaf_entry(400, 400).row_id)
                    .is_err());
                assert!(IndexFile::quick_validate(index.path.clone()).is_err());
                assert!(
                    IndexFile::open_for_recovery(index.path.clone(), index.log.clone(), 1000)
                        .is_err()
                );
            }
        }
        assert!(
            failures >= 2,
            "must inject failures at different page publication steps"
        );
    }
}

#[test]
fn audit_20260926_last_entry_excludes_structural_writer() {
    let (_dir, index) = audit_index_raw("last.idx");
    index
        .insert_deferred(0, wide_key(1), leaf_entry(1, 1).row_id)
        .unwrap();
    std::thread::scope(|scope| {
        let guard = index.structure_lock.write();
        let (sent, received) = std::sync::mpsc::channel();
        let file = &index;
        let reader = scope.spawn(move || {
            sent.send(()).unwrap();
            file.last_entry().unwrap()
        });
        received.recv().unwrap();
        assert!(!reader.is_finished());
        drop(guard);
        assert_eq!(reader.join().unwrap().unwrap().0, wide_key(1));
    });
}

#[test]
fn audit_20260926_unsorted_empty_batch_and_nested_wal_scope() {
    let (_dir, index) = audit_index_raw("unsorted.idx");
    let entries = vec![
        leaf_entry(3, 3),
        leaf_entry(1, 1),
        leaf_entry(3, 3),
        leaf_entry(2, 2),
    ];
    btree_logical_wal::set(Some(123));
    let result = index.insert_batch_deferred(
        0,
        entries.iter().map(|e| (e.key.clone(), e.row_id)).collect(),
        true,
    );
    let outer = btree_logical_wal::current();
    btree_logical_wal::set(None);
    result.unwrap();
    assert_eq!(outer, Some(123));
    let mut expected = entries;
    expected.sort();
    expected.dedup();
    assert_eq!(
        index.entries().unwrap(),
        expected
            .into_iter()
            .map(|e| (e.key, e.row_id))
            .collect::<Vec<_>>()
    );
}

#[test]
fn audit_20260926_decimal_cross_lane_and_short_prefix_match_ord() {
    for value in [-10, 0, 10] {
        for target_value in [-20, 0, 20] {
            let narrow = KeyPart::Decimal { value, scale: 1 };
            let wide = KeyPart::DecimalWide {
                value: num_bigint::BigInt::from(target_value),
                scale: 1,
            };
            for (left, right) in [(narrow.clone(), wide.clone()), (wide, narrow)] {
                let key = IndexKey::from(vec![left]);
                let target = IndexKey::from(vec![right]);
                let mut bytes = vec![0; 256];
                encode_index_key_into_leaf_page(&key, &mut bytes, &mut 0).unwrap();
                assert_eq!(
                    compare_generic_index_key_from(&bytes, &mut 0, &target).unwrap(),
                    key.cmp(&target)
                );
            }
        }
    }
    assert_eq!(
        compare_index_entry_to_lookup_key(
            &IndexKey::from(vec![KeyPart::Int(3)]),
            &IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2)]),
            false
        ),
        std::cmp::Ordering::Greater
    );
}

#[test]
fn audit_20260926_leaf_range_lease_prevents_split_merge_and_reuse() {
    let (_dir, index) = audit_index_raw("range-lease.idx");
    let expected: Vec<_> = (0..160).map(|i| leaf_entry(i, i as usize)).collect();
    index.bulk_load_sorted(0, expected.clone()).unwrap();
    let range = index.range_leaf_ids(None, None).unwrap();
    let worker_lease = range.lease.clone();
    std::thread::scope(|scope| {
        let (started, receiving) = std::sync::mpsc::channel();
        let file = &index;
        let writer = scope.spawn(move || {
            started.send(()).unwrap();
            let _guard = file.structure_lock.write();
            file.append_sorted_entries(0, (160..400).map(|i| leaf_entry(i, i as usize)).collect())
                .unwrap();
        });
        receiving.recv().unwrap();
        assert!(index.structure_lock.try_write().is_none());
        assert!(!writer.is_finished());
        let got = index
            .range_entries_for_leaf_ids(&range, None, None)
            .unwrap();
        assert_eq!(
            got,
            expected
                .iter()
                .map(|e| (e.key.clone(), e.row_id))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            index
                .range_row_ids_for_leaf_ids(&range, None, None)
                .unwrap(),
            expected.iter().map(|e| e.row_id).collect::<Vec<_>>()
        );
        drop(range);
        assert!(
            index.structure_lock.try_write().is_none(),
            "worker clones retain the lease"
        );
        drop(worker_lease);
        writer.join().unwrap();
    });
    assert_eq!(index.entries().unwrap().len(), 400);
}

#[test]
fn audit_20260926_persistent_write_failure_rejects_reads_writes_and_reopen() {
    let (_dir, index) = audit_index_raw("failed-restore.idx");
    index
        .bulk_load_sorted(0, (0..100).map(|i| leaf_entry(i, i as usize)).collect())
        .unwrap();
    INDEX_FAIL_WRITE_PERSIST.with(|flag| flag.set(true));
    INDEX_FAIL_WRITE_AFTER.with(|fail| fail.set(Some(1)));
    let result = {
        let _structure = index.structure_lock.write();
        index.append_sorted_entries(0, (100..300).map(|i| leaf_entry(i, i as usize)).collect())
    };
    INDEX_FAIL_WRITE_PERSIST.with(|flag| flag.set(false));
    INDEX_FAIL_WRITE_AFTER.with(|fail| fail.set(None));
    assert!(result.unwrap_err().message().contains("requires rebuild"));
    assert!(index.get(&wide_key(1), false).is_err());
    assert!(index
        .insert_deferred(10, wide_key(400), leaf_entry(400, 400).row_id)
        .is_err());
    assert!(IndexFile::quick_validate(index.path.clone()).is_err());
    assert!(IndexFile::open_with_durability(
        index.path.clone(),
        index.log.clone(),
        index.durability,
        index.buffer_pool.clone()
    )
    .is_err());
}

#[test]
fn audit_20260926_bulk_failure_logs_empty_tree_compensation() {
    let (dir, index) = audit_index_raw("bulk-failure.idx");
    install_bulk_index_fail_before_meta();
    assert!(index
        .bulk_load_sorted(41, (0..100).map(|i| leaf_entry(i, i as usize)).collect())
        .is_err());
    index.log.lock().flush().unwrap();
    let records = LogManager::scan(&dir.path().join("wal")).unwrap().records;
    assert!(records.iter().any(|r| matches!(&r.payload, WalPayload::IndexBatch { ops, .. } if ops.iter().all(|op| op.kind == WalIndexOpKind::Delete))));
    assert!(replay_records(&records, &index.relative_path()).is_empty());
    // A replica consumes logical batches and ignores primary page images.
    let (_replica_dir, replica) = audit_index_raw("replica.idx");
    for record in records {
        if let WalPayload::IndexBatch { ops, .. } = record.payload {
            let entries = ops
                .into_iter()
                .map(|op| IndexBatchEntry {
                    kind: op.kind,
                    key: decode_wal_index_key(&op.key).unwrap(),
                    row_id: wal_row_id_to_row_id(op.row_id),
                })
                .collect();
            replica
                .apply_replicated_batch(record.txn_id, entries, record.lsn)
                .unwrap();
        }
    }
    assert!(replica.entries().unwrap().is_empty());
}

#[test]
fn audit_20260926_allocator_rejects_nonfree_head_and_reserves_unique_ids() {
    let (_dir, index) = audit_index_raw("allocator.idx");
    index.meta_state.lock().meta.free_page_head = Some(INDEX_EMPTY_ROOT);
    assert!(index
        .alloc_page(0)
        .unwrap_err()
        .message()
        .contains("freelist"));
    assert_eq!(
        index.meta_state.lock().meta.free_page_head,
        Some(INDEX_EMPTY_ROOT)
    );
    index.meta_state.lock().meta.free_page_head = None;
    let ids = std::thread::scope(|scope| {
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..100)
                        .map(|_| index.reserve_page_id().unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        tasks
            .into_iter()
            .flat_map(|task| task.join().unwrap())
            .collect::<HashSet<_>>()
    });
    assert_eq!(ids.len(), 800);
}

#[test]
fn audit_20260926_truncated_internal_child_returns_error() {
    let key = leaf_entry(10, 0);
    let mut bytes = encode_index_tree_page(&IndexPage::Internal(IndexInternalPage::new_generic(
        vec![key.clone()],
        vec![2, 3],
    )))
    .unwrap();
    let page = parse_raw_internal_route_page(&bytes).unwrap().unwrap();
    let slot_start = INDEX_HEADER_BYTES + page.header.slot_array_start;
    let slot = page.header.read_slot(0);
    write_u16(&mut bytes, slot_start + 2, slot.len - 8);
    assert!(route_index_page_bytes(&bytes, &key.key, key.row_id).is_err());
}

#[test]
fn audit_20260926_restart_rebuilds_marked_index_from_heap() {
    use crate::engine::{QueryResponse, StorageQueryEngine};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let config = StorageConfig {
        durability: DurabilityMode::Strict,
        startup_validation: crate::storage::StartupValidationMode::Full,
        startup_validate_indexes: true,
        buffer_pool_pages: 128,
        buffer_pool_io_threads: 1,
        buffer_pool_prefetch_pages: 0,
        ..StorageConfig::from_env()
    };
    let index_path;
    {
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            config.clone(),
        )
        .unwrap();
        engine.execute("CREATE TABLE repair_marker (id INT PRIMARY KEY, v VARCHAR(100), KEY kv(v)); INSERT INTO repair_marker VALUES (1,'one'),(2,'two')").unwrap();
        let db = engine.database();
        let table = db.snapshot_table_schema("test.repair_marker").unwrap();
        index_path = db.index_path_for_table_schema(&table, table.id, "kv");
    }
    File::create(index_path.with_extension("idx.failed"))
        .unwrap()
        .sync_all()
        .unwrap();
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM repair_marker FORCE INDEX (kv) WHERE v='two'")
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(2)]]);
    assert!(!index_path.with_extension("idx.failed").exists());
}

fn assert_sorted_and_searchable(index: &IndexFile) {
    let entries = index.entries().unwrap();
    assert!(entries.windows(2).all(|pair| pair[0] < pair[1]));
    for (key, row_id) in entries {
        assert_eq!(index.get(&key, false).unwrap(), vec![row_id]);
    }
}

#[test]
fn audit_20260926_single_insert_split_failure_requires_rebuild() {
    for nth in 0..3 {
        let (_dir, index) = audit_index_raw("single-split.idx");
        let mut entries = Vec::new();
        for i in 0..100 {
            entries.push(leaf_entry(i, i as usize));
            if encoded_leaf_page_len(&IndexLeafPage::new(entries.clone(), None)) > PAGE_SIZE {
                entries.pop();
                break;
            }
        }
        index
            .write_page(
                0,
                INDEX_EMPTY_ROOT,
                IndexPage::Leaf(IndexLeafPage::new(entries, None)),
            )
            .unwrap();
        INDEX_FAIL_WRITE_AFTER.with(|fail| fail.set(Some(nth)));
        let result = index.insert_deferred(77, wide_key(999), leaf_entry(999, 999).row_id);
        INDEX_FAIL_WRITE_AFTER.with(|fail| fail.set(None));
        assert!(result.unwrap_err().message().contains("requires rebuild"));
        assert!(index.get(&wide_key(1), false).is_err());
        assert!(index.path.with_extension("idx.failed").exists());
    }
}

fn replay_records(records: &[crate::wal::WalRecord], relative: &str) -> Vec<(IndexKey, RowId)> {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let mut redo = RedoState::new(Some(log.clone()));
    let committed = records.iter().map(|r| r.txn_id).collect::<RecoveryTxnSet>();
    for record in records {
        apply_redo_record(dir.path(), record, &committed, 0, false, &mut redo).unwrap();
    }
    redo.finish().unwrap();
    drop(redo);
    let index = IndexFile::open(dir.path().join(relative), log).unwrap();
    index.entries().unwrap()
}

#[test]
fn temporary_audit_index_handle_reopen_keeps_cached_metadata() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(&dir.path().join("wal")).unwrap(),
    ));
    let pool = BufferPool::new(
        4096,
        log.clone(),
        DurabilityMode::Memory,
        Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
    );
    let path = crate::storage::naming::temp_indexes_dir(dir.path())
        .join(format!("{}_key.idx", 1u64 << 63));
    let index = IndexFile::open_with_durability(
        path.clone(),
        log.clone(),
        DurabilityMode::Memory,
        pool.clone(),
    )
    .unwrap();
    for n in 0..200 {
        index
            .insert_entry_concurrent(1, leaf_entry(n, n as usize))
            .unwrap();
    }
    let expected = index.range(None, None, false).unwrap();
    drop(index);
    let reopened =
        IndexFile::open_with_durability(path, log, DurabilityMode::Memory, pool).unwrap();
    assert_eq!(reopened.range(None, None, false).unwrap(), expected);
}
