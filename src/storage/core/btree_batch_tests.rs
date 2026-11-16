use super::*;

#[test]
fn raw_generic_string_comparisons_match_decoded_order_and_prefix_cursor() {
    let mut parts = vec![
        KeyPart::Null,
        KeyPart::Int(i64::MIN),
        KeyPart::UInt(u64::MAX),
        KeyPart::Float32(u32::MAX),
        KeyPart::Float64(u64::MAX),
        KeyPart::Decimal {
            value: -123,
            scale: 2,
        },
        KeyPart::Date(-1),
        KeyPart::Time(i64::MIN),
        KeyPart::DateTime(i64::MAX),
        KeyPart::Year(255),
        KeyPart::Bytes(vec![0xff, 0x80, 0]),
    ];
    let strings = [
        String::new(),
        "a".into(),
        "a\0z".into(),
        "é".into(),
        "e\u{301}".into(),
        "Résumé ".into(),
        "résumé".into(),
        "中文𐀀\u{10ffff}".into(),
        "\u{7f}\u{80}\u{7ff}\u{800}\u{ffff}".into(),
        format!("{}a", "共同前缀".repeat(64)),
        format!("{}b", "共同前缀".repeat(64)),
    ];
    for text in strings {
        parts.push(KeyPart::String(text.clone()));
        for collation_id in [45, 46, 255] {
            parts.push(KeyPart::CollatedString {
                collation_id,
                value: text.clone(),
            });
        }
    }
    let mut keys = vec![IndexKey::from(vec![])];
    for part in parts {
        keys.push(IndexKey::from(vec![part.clone()]));
        keys.push(IndexKey::from(vec![part.clone(), KeyPart::Int(-9)]));
        keys.push(IndexKey::from(vec![
            KeyPart::String("shared".into()),
            part,
            KeyPart::String("suffix".into()),
        ]));
    }
    for stored in &keys {
        let mut encoded = vec![0; 4096];
        let mut end = 0;
        encode_index_key_into_leaf_page(stored, &mut encoded, &mut end).unwrap();
        encoded.truncate(end);
        let mut decoded_end = 0;
        let decoded = decode_index_key_from_leaf_page(&encoded, &mut decoded_end).unwrap();
        assert_eq!(decoded_end, end);
        for target in &keys {
            let mut cursor = 0;
            let actual = compare_generic_index_key_from(&encoded, &mut cursor, target).unwrap();
            assert_eq!(
                actual,
                decoded.cmp(target),
                "stored={stored:?} target={target:?}"
            );
            assert_eq!(cursor, end);
            let mut cursor = 0;
            let actual =
                compare_generic_index_key_prefix_from(&encoded, &mut cursor, target).unwrap();
            let prefix = IndexKey::from(
                decoded
                    .0
                    .iter()
                    .take(target.0.len())
                    .cloned()
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                actual,
                prefix.cmp(target),
                "stored={stored:?} target={target:?}"
            );
            assert_eq!(cursor, end);
        }
    }
}

#[test]
fn raw_generic_string_comparisons_reject_corrupt_ignored_or_mismatched_parts() {
    let stored = IndexKey::from(vec![
        KeyPart::String("first".into()),
        KeyPart::CollatedString {
            collation_id: 45,
            value: "résumé".into(),
        },
        KeyPart::String("tail".into()),
        KeyPart::Bytes(vec![0xff, 0, 0x80, 0xfe]),
    ]);
    let mut encoded = vec![0; 256];
    let mut end = 0;
    encode_index_key_into_leaf_page(&stored, &mut encoded, &mut end).unwrap();
    encoded.truncate(end);
    let targets = [
        stored.clone(),
        IndexKey::from(vec![]),
        IndexKey::from(vec![KeyPart::String("first".into())]),
        IndexKey::from(vec![KeyPart::String("aaa".into())]),
        IndexKey::from(vec![KeyPart::String("zzz".into())]),
        IndexKey::from(vec![KeyPart::Int(1), KeyPart::Int(2), KeyPart::Int(3)]),
    ];
    let mut bad = Vec::new();
    for text in ["first", "résumé", "tail"] {
        let at = encoded
            .windows(text.len())
            .position(|bytes| bytes == text.as_bytes())
            .unwrap();
        for invalid in [0xff, 0x80, 0xc0, 0xf5] {
            let mut bytes = encoded.clone();
            bytes[at] = invalid;
            bad.push(bytes);
        }
    }
    bad.extend((0..encoded.len()).map(|length| encoded[..length].to_vec()));
    for bytes in &bad {
        assert!(decode_index_key_from_leaf_page(bytes, &mut 0).is_err());
        for target in &targets {
            assert!(compare_generic_index_key_from(bytes, &mut 0, target).is_err());
            assert!(compare_generic_index_key_prefix_from(bytes, &mut 0, target).is_err());
        }
    }
}

#[test]
fn raw_internal_insert_preserves_key_order_child_links_and_declines_atomically() {
    for shape in 0..5 {
        let make = |n: u32| {
            entry(
                match shape {
                    0 => vec![
                        KeyPart::String(format!("key{n:04}-é界")),
                        KeyPart::Bytes(vec![n as u8; 13]),
                    ],
                    1 => vec![KeyPart::Int(n as i64)],
                    2 => vec![KeyPart::Int(n as i64), KeyPart::Int(n as i64 % 7)],
                    3 => vec![KeyPart::CollatedString {
                        collation_id: 45,
                        value: format!("résumé{n:04}"),
                    }],
                    _ => vec![KeyPart::Null, KeyPart::UInt(n as u64)],
                },
                n,
            )
        };
        for pos in 0..4 {
            let keys = [20, 40, 60].map(make).to_vec();
            let children = vec![11, 12, 13, 14];
            let mut bytes = encode_index_tree_page(&IndexPage::Internal(
                IndexInternalPage::new_generic(keys.clone(), children.clone()),
            ))
            .unwrap();
            let before = bytes.clone();
            let incoming = make(10 + pos as u32 * 20);
            let result =
                try_insert_raw_internal_separator(&mut bytes, pos, children[pos], 99, &incoming)
                    .unwrap();
            if result {
                let IndexPage::Internal(actual) = decode_validated_index_tree_page(&bytes).unwrap()
                else {
                    panic!("expected internal")
                };
                let mut expected = keys;
                expected.insert(pos, incoming);
                let mut expected_children = children;
                expected_children.insert(pos + 1, 99);
                assert_eq!(actual.keys, expected, "shape={shape} pos={pos}");
                assert_eq!(actual.children, expected_children);
            } else {
                assert_eq!(bytes, before);
            }
        }
    }
    let make = |n| {
        entry(
            vec![KeyPart::String(format!("{n:08}{}", "w".repeat(350)))],
            n,
        )
    };
    let mut keys = Vec::new();
    loop {
        let mut next = keys.clone();
        next.push(make((keys.len() * 2) as u32));
        if encoded_internal_page_len(&IndexInternalPage::new_generic(
            next,
            (0..keys.len() as u64 + 2).collect(),
        )) > PAGE_SIZE
        {
            break;
        }
        keys.push(make((keys.len() * 2) as u32));
    }
    let mut bytes = encode_index_tree_page(&IndexPage::Internal(IndexInternalPage::new_generic(
        keys.clone(),
        (0..keys.len() as u64 + 1).collect(),
    )))
    .unwrap();
    let before = bytes.clone();
    assert!(!try_insert_raw_internal_separator(&mut bytes, 1, 1, 99, &make(1)).unwrap());
    assert_eq!(bytes, before);
    assert!(try_insert_raw_internal_separator(&mut bytes, 1, 999, 99, &make(1)).is_err());
    assert_eq!(bytes, before);
}

#[test]
fn owned_leaf_run_merge_matches_sorted_set_for_generic_keys() {
    for shape in 0..4 {
        let make = |n: u32| {
            entry(
                match shape {
                    0 => vec![
                        KeyPart::String(format!("{n:06}-é界")),
                        KeyPart::Bytes(vec![n as u8; 11]),
                    ],
                    1 => vec![KeyPart::CollatedString {
                        collation_id: 45,
                        value: format!("{n:06}-Résumé"),
                    }],
                    2 => vec![KeyPart::Null, KeyPart::UInt(n as u64)],
                    _ => vec![
                        KeyPart::Decimal {
                            value: n as i128 * 1007,
                            scale: 3,
                        },
                        KeyPart::Int(n as i64),
                    ],
                },
                n,
            )
        };
        let old = (0..120).step_by(3).map(make).collect::<Vec<_>>();
        for stride in [1, 2, 7, 31, 200] {
            let mut input = (0..145).step_by(stride).map(make).collect::<Vec<_>>();
            input.extend(input.clone());
            input.sort();
            let mut expected = old.clone();
            expected.extend(input.clone());
            expected.sort();
            expected.dedup();
            let changed = expected != old;
            let actual = merge_leaf_insert_run(old.clone(), &input);
            assert_eq!(actual, (expected, changed), "shape={shape} stride={stride}");
        }
    }
}

#[test]
fn prepared_insert_runs_preserve_boundaries_duplicates_and_stale_epochs() {
    for stale in [false, true] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            4096,
            log.clone(),
            DurabilityMode::Relaxed,
            Duration::from_secs(2),
        );
        let index = Arc::new(
            IndexFile::open_with_durability(
                crate::storage::naming::index_object_path_by_id(dir.path(), 1, 1, 1, "wide"),
                log.clone(),
                DurabilityMode::Relaxed,
                pool,
            )
            .unwrap(),
        );
        let make = |n| {
            entry(
                vec![KeyPart::String(format!("{n:08}{}", "width".repeat(70)))],
                n,
            )
        };
        let seed = (0..2000).map(|n| make(n * 2)).collect::<Vec<_>>();
        index.bulk_load_sorted(0, seed.clone()).unwrap();
        let mut incoming = (0..2000).map(|n| make(n * 2 + 1)).collect::<Vec<_>>();
        incoming.extend(seed.iter().step_by(5).cloned());
        incoming.sort();
        let removed = seed[3].clone();
        if stale {
            let peer = index.clone();
            let removed = removed.clone();
            INDEX_STRUCTURE_PREPARED_TEST_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    peer.delete_with_txn(0, removed.key, removed.row_id)
                        .unwrap();
                }))
            });
        }
        let lsn = index
            .append_index_batch_wal(81, WalIndexOpKind::Insert, &incoming)
            .unwrap();
        let _logical = btree_logical_wal::LsnScope::new(lsn);
        let mut run = SortedIndexEntryRun::prefix(&incoming);
        let mut batched = false;
        while !run.0.is_empty() {
            let consumed = index.insert_run_with_prepared_structure(81, run).unwrap();
            batched |= consumed > 1;
            run = run.suffix(consumed);
        }
        assert!(batched, "fixture must publish a multi-entry prepared image");
        let mut expected = seed;
        expected.extend(incoming);
        expected.sort();
        expected.dedup();
        if stale {
            expected.retain(|entry| entry != &removed);
        }
        assert_eq!(
            index.entries().unwrap(),
            expected
                .into_iter()
                .map(|e| (e.key, e.row_id))
                .collect::<Vec<_>>()
        );
        index.flush().unwrap();
        IndexFile::validate(index.path.clone()).unwrap();
    }
}

#[test]
fn prepared_insert_run_split_write_failure_fences_index() {
    let mut failures = 0;
    for nth in 0..5 {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            1024,
            log.clone(),
            DurabilityMode::Relaxed,
            Duration::from_secs(2),
        );
        let index = IndexFile::open_with_durability(
            crate::storage::naming::index_object_path_by_id(dir.path(), 1, 1, 1, "wide"),
            log,
            DurabilityMode::Relaxed,
            pool,
        )
        .unwrap();
        let make = |n| {
            entry(
                vec![KeyPart::String(format!("{n:08}{}", "wide".repeat(100)))],
                n,
            )
        };
        let mut seed = Vec::new();
        while encoded_leaf_page_len(&IndexLeafPage::new(seed.clone(), None))
            + encoded_index_entry_len(&make(seed.len() as u32 * 2))
            + SLOT_OFFSET_BYTES
            <= PAGE_SIZE
        {
            seed.push(make(seed.len() as u32 * 2));
        }
        index.bulk_load_sorted(0, seed).unwrap();
        let incoming = (0..8).map(|n| make(n * 2 + 1)).collect::<Vec<_>>();
        let lsn = index
            .append_index_batch_wal(82, WalIndexOpKind::Insert, &incoming)
            .unwrap();
        let _logical = btree_logical_wal::LsnScope::new(lsn);
        let prepared = index
            .prepare_insert_structure(SortedIndexEntryRun::prefix(&incoming))
            .unwrap()
            .unwrap();
        assert!(prepared.consumed > 1 && encoded_leaf_page_len(&prepared.leaf) > PAGE_SIZE);
        drop(prepared);
        INDEX_FAIL_WRITE_AFTER.with(|fail| fail.set(Some(nth)));
        let result =
            index.insert_run_with_prepared_structure(82, SortedIndexEntryRun::prefix(&incoming));
        INDEX_FAIL_WRITE_AFTER.with(|fail| fail.set(None));
        if let Err(error) = result {
            failures += 1;
            assert!(error.message().contains("requires rebuild"));
            assert!(index.path.with_extension("idx.failed").exists());
            assert!(index.entries().is_err());
        } else {
            index.flush().unwrap();
            IndexFile::validate(index.path.clone()).unwrap();
        }
    }
    assert!(
        failures >= 3,
        "exercise failures across structural publication"
    );
}

#[test]
fn prepared_insert_run_extends_leaf_max_but_honors_ancestor_fence() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let pool = BufferPool::new(
        1024,
        log.clone(),
        DurabilityMode::Relaxed,
        Duration::from_secs(2),
    );
    let index = IndexFile::open_with_durability(
        crate::storage::naming::index_object_path_by_id(dir.path(), 1, 1, 1, "wide"),
        log,
        DurabilityMode::Relaxed,
        pool,
    )
    .unwrap();
    let left_parent = index.reserve_page_id().unwrap();
    let right_parent = index.reserve_page_id().unwrap();
    let leaves = (0..4)
        .map(|_| index.reserve_page_id().unwrap())
        .collect::<Vec<_>>();
    let make = |group, n| {
        entry(
            vec![KeyPart::String(format!("group{group}{}", "x".repeat(350)))],
            n,
        )
    };
    let groups = (0..4)
        .map(|g| {
            (0..if g == 1 || g == 2 { 40 } else { 8 })
                .map(|n| make(g, n))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for pos in (0..4).rev() {
        index
            .write_page(
                0,
                leaves[pos],
                IndexPage::Leaf(IndexLeafPage::new_with_resolved_layout(
                    groups[pos].clone(),
                    leaves.get(pos + 1).copied(),
                )),
            )
            .unwrap();
    }
    index
        .write_page(
            0,
            left_parent,
            IndexPage::Internal(IndexInternalPage::new_generic(
                vec![groups[1][0].clone()],
                leaves[..2].to_vec(),
            )),
        )
        .unwrap();
    index
        .write_page(
            0,
            right_parent,
            IndexPage::Internal(IndexInternalPage::new_generic(
                vec![groups[3][0].clone()],
                leaves[2..].to_vec(),
            )),
        )
        .unwrap();
    index
        .write_page(
            0,
            INDEX_EMPTY_ROOT,
            IndexPage::Internal(IndexInternalPage::new_generic(
                vec![groups[2][0].clone()],
                vec![left_parent, right_parent],
            )),
        )
        .unwrap();
    index.write_meta_page(0).unwrap();
    let incoming = (1..=2)
        .flat_map(|g| (100..120).map(move |n| make(g, n)))
        .collect::<Vec<_>>();
    let lsn = index
        .append_index_batch_wal(83, WalIndexOpKind::Insert, &incoming)
        .unwrap();
    let _logical = btree_logical_wal::LsnScope::new(lsn);
    let prepared = index
        .prepare_insert_structure(SortedIndexEntryRun::prefix(&incoming))
        .unwrap()
        .unwrap();
    assert_eq!(
        prepared.consumed, 20,
        "the last child must retain its ancestor's upper fence"
    );
    drop(prepared);
    assert_eq!(
        index
            .insert_run_with_prepared_structure(83, SortedIndexEntryRun::prefix(&incoming))
            .unwrap(),
        20
    );
    assert_eq!(
        index
            .insert_run_with_prepared_structure(83, SortedIndexEntryRun::prefix(&incoming[20..]))
            .unwrap(),
        20
    );
    let mut expected = groups.into_iter().flatten().collect::<Vec<_>>();
    expected.extend(incoming);
    expected.sort();
    assert_eq!(
        index.entries().unwrap(),
        expected
            .into_iter()
            .map(|e| (e.key, e.row_id))
            .collect::<Vec<_>>()
    );
    index.flush().unwrap();
    IndexFile::validate(index.path.clone()).unwrap();
}

#[test]
fn buffered_sorted_insert_validates_order_once_across_leaves() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let pool = BufferPool::new(
        4096,
        log.clone(),
        DurabilityMode::Relaxed,
        Duration::from_secs(2),
    );
    let index = IndexFile::open_with_durability(
        crate::storage::naming::index_object_path_by_id(dir.path(), 1, 1, 1, "wide"),
        log.clone(),
        DurabilityMode::Relaxed,
        pool.clone(),
    )
    .unwrap();
    let seed = (0..2400)
        .map(|n| {
            entry(
                vec![KeyPart::String(format!("{n:08}{}", "wide".repeat(75)))],
                n,
            )
        })
        .collect::<Vec<_>>();
    index.insert_entries_logged(0, seed.clone(), true).unwrap();
    let incoming = seed.iter().step_by(5).cloned().collect::<Vec<_>>();
    index
        .apply_batch_deferred(
            0,
            incoming
                .iter()
                .map(|e| IndexBatchEntry {
                    kind: WalIndexOpKind::Delete,
                    key: e.key.clone(),
                    row_id: e.row_id,
                })
                .collect(),
        )
        .unwrap();
    begin_txn_wal_buffering(&log, 73, 0, true);
    SORTED_RUN_COMPARISONS.with(|count| count.set(0));
    let outcome = index
        .try_insert_entries_buffered(73, &incoming)
        .unwrap()
        .unwrap();
    assert!(
        matches!(outcome, BufferedIndexInsert::Complete),
        "holes must fit without structural fallback"
    );
    let comparisons = SORTED_RUN_COMPARISONS.with(|count| count.get());
    assert!(
        comparisons <= incoming.len(),
        "sorted-run comparisons must be linear: {comparisons}"
    );
    let mut buffered = take_txn_wal_entries(&log, 73);
    if let Some(begin) = buffered.begin.take() {
        buffered.entries.insert(0, (begin, Vec::new()));
    }
    for (payload, keys) in buffered.entries {
        let pre = LogManager::pre_encode_record(73, payload).unwrap();
        let lsn = LogManager::append_pre_encoded_to(&log, pre).unwrap();
        for key in keys {
            pool.finalize_deferred(&key, lsn);
        }
    }
    end_txn_wal_buffering(&log, 73);
    assert_eq!(
        index.entries().unwrap(),
        seed.into_iter()
            .map(|e| (e.key, e.row_id))
            .collect::<Vec<_>>()
    );
    index.flush().unwrap();
    IndexFile::validate(index.path.clone()).unwrap();
}

#[test]
fn buffered_insert_fallback_resumes_after_applied_prefix() {
    for kind in 0..5 {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            1024,
            log.clone(),
            DurabilityMode::Relaxed,
            Duration::from_secs(2),
        );
        let index = IndexFile::open_with_durability(
            crate::storage::naming::index_object_path_by_id(dir.path(), 1, 1, 1, "idx"),
            log.clone(),
            DurabilityMode::Relaxed,
            pool.clone(),
        )
        .unwrap();
        let make = |n: u32| {
            entry(
                match kind {
                    0 => vec![KeyPart::Int(n as i64)],
                    1 => vec![KeyPart::UInt((1 << 63) + n as u64)],
                    2 => vec![KeyPart::String(format!("{n:08}{}", "x".repeat(300)))],
                    3 => vec![KeyPart::Int(n as i64 / 7), KeyPart::Int(n as i64 % 7)],
                    _ => vec![KeyPart::Null, KeyPart::Int(n as i64)],
                },
                n,
            )
        };
        let mut seed = Vec::new();
        loop {
            seed.push(make(seed.len() as u32));
            let leaf = IndexLeafPage::new_with_resolved_layout(seed.clone(), None);
            if encoded_leaf_page_len(&leaf) > PAGE_SIZE {
                seed.truncate(seed.len() - 5);
                break;
            }
        }
        index
            .write_page(
                0,
                INDEX_EMPTY_ROOT,
                IndexPage::Leaf(IndexLeafPage::new_with_resolved_layout(seed.clone(), None)),
            )
            .unwrap();
        let start = seed.len() as u32;
        let incoming: Vec<_> = (start..start + 64).map(make).collect();
        let txn_id = 71;
        begin_txn_wal_buffering(&log, txn_id, 0, true);
        let outcome = index
            .try_insert_entries_buffered(txn_id, &incoming)
            .unwrap()
            .expect("bounded batch must be eligible");
        let BufferedIndexInsert::Remaining { consumed, lsn } = outcome else {
            panic!("batch must fill the leaf before completing");
        };
        assert!(consumed > 0 && consumed < incoming.len(), "kind={kind}");
        assert_eq!(index.entries().unwrap().len(), seed.len() + consumed);
        assert_eq!(pool.deferred_wal_pages.load(Ordering::Acquire), 0);
        log.lock().flush().unwrap();
        let logged: usize = crate::wal::LogManager::scan(dir.path())
            .unwrap()
            .records
            .into_iter()
            .filter(|record| record.txn_id == txn_id)
            .filter_map(|record| match record.payload {
                WalPayload::IndexBatch { ops, .. } => Some(ops.len()),
                _ => None,
            })
            .sum();
        assert_eq!(logged, incoming.len(), "WAL must retain the entire batch");
        index
            .insert_entries_with_lsn(txn_id, incoming[consumed..].to_vec(), Some(lsn))
            .unwrap();
        let mut expected = seed;
        expected.extend(incoming);
        assert_eq!(
            index.entries().unwrap(),
            expected
                .into_iter()
                .map(|entry| (entry.key, entry.row_id))
                .collect::<Vec<_>>()
        );
        let buffer = take_txn_wal_entries(&log, txn_id);
        assert!(buffer.entries.is_empty());
        end_txn_wal_buffering(&log, txn_id);
        index.flush().unwrap();
        IndexFile::validate(index.path.clone()).unwrap();
    }
}

fn entry(key: Vec<KeyPart>, id: u32) -> IndexEntry {
    IndexEntry {
        key: IndexKey::from(key),
        row_id: RowId {
            page_id: id / 32,
            slot_id: (id % 32) as u16,
        },
    }
}

fn decoded(bytes: &[u8]) -> IndexLeafPage {
    match decode_validated_index_tree_page(bytes).unwrap() {
        IndexPage::Leaf(leaf) => leaf,
        _ => panic!("expected leaf"),
    }
}

// The oracle is the old loop over single-record page insertions, including its
// range, duplicate and overflow decisions. Compare logical results rather than
// raw bytes: unused bytes may differ after moving the slot directory.
fn compare_run(bytes: &[u8], incoming: &[IndexEntry]) -> (usize, Vec<u8>) {
    compare_run_with_bound::<false>(bytes, incoming)
}

fn compare_run_with_bound<const ROUTED: bool>(
    bytes: &[u8],
    incoming: &[IndexEntry],
) -> (usize, Vec<u8>) {
    let mut oracle = bytes.to_vec();
    let mut count = 0;
    let mut changed = false;
    for row in incoming {
        if !ROUTED && count > 0 && !raw_leaf_candidate_stays(&oracle, row).unwrap() {
            break;
        }
        match try_insert_raw_leaf(&mut oracle, row, 91).unwrap() {
            Some(InPlaceInsert::Inserted) => {
                count += 1;
                changed = true;
            }
            Some(InPlaceInsert::Duplicate) => count += 1,
            _ => break,
        }
    }
    let mut actual = bytes.to_vec();
    let result = try_insert_raw_leaf_run_with_bound::<ROUTED>(
        &mut actual,
        SortedIndexEntryRun::prefix(incoming),
        91,
    )
    .unwrap();
    assert_eq!(result, (changed, (count > 0).then_some(count)));
    let expected = decoded(&oracle);
    let got = decoded(&actual);
    assert_eq!(got.entries, expected.entries);
    assert_eq!(got.next_leaf, expected.next_leaf);
    assert_eq!(got.key_layout, expected.key_layout);
    assert_eq!(got.page_max_trx_id, expected.page_max_trx_id);
    assert_eq!(read_u32(&actual, 20), read_u32(&oracle, 20));
    if !changed {
        assert_eq!(actual, bytes);
    }
    (count, actual)
}

#[test]
fn raw_leaf_batch_matches_single_insert_for_every_layout() {
    let keymakers: Vec<Box<dyn Fn(i64) -> Vec<KeyPart>>> = vec![
        Box::new(|n| vec![KeyPart::Int(n - 100)]),
        Box::new(|n| vec![KeyPart::Int(n * 100_000)]),
        Box::new(|n| vec![KeyPart::UInt(n as u64 + (1 << 63))]),
        Box::new(|n| vec![KeyPart::UInt(n as u64 * 100_000)]),
        Box::new(|n| vec![KeyPart::Int(n / 7), KeyPart::Int(n % 7)]),
        Box::new(|n| vec![KeyPart::Date(18_000 + n as i32)]),
        Box::new(|n| vec![KeyPart::DateTime(1_600_000_000_000_000 + n * 1_000_000)]),
        Box::new(|n| {
            vec![KeyPart::Decimal {
                value: (n - 100) as i128,
                scale: 2,
            }]
        }),
        Box::new(|n| {
            vec![KeyPart::Decimal {
                value: (n - 100) as i128 * 1_000_000_000,
                scale: 4,
            }]
        }),
        Box::new(|n| vec![KeyPart::String(format!("键-{n:04}"))]),
        Box::new(|n| vec![KeyPart::Bytes(vec![0, n as u8, 255])]),
        Box::new(|n| vec![KeyPart::Float32(f32_to_order_u32((n - 100) as f32 / 3.0))]),
        Box::new(|n| vec![KeyPart::Float64(f64_to_order_u64((n - 100) as f64 / 7.0))]),
        Box::new(|n| {
            vec![KeyPart::DecimalWide {
                value: num_bigint::BigInt::from(n) << 160,
                scale: 20,
            }]
        }),
        Box::new(|n| {
            vec![KeyPart::CollatedString {
                collation_id: 45,
                value: format!("Été-{n:04}"),
            }]
        }),
        Box::new(|n| vec![KeyPart::Null, KeyPart::Int(n)]),
        Box::new(|n| vec![KeyPart::Int(n / 3), KeyPart::String(format!("v-{n:04}"))]),
    ];
    let mut layouts = std::collections::BTreeSet::new();
    for make in keymakers {
        for next in [None, Some(99)] {
            let mut initial: Vec<_> = (0..240)
                .step_by(3)
                .map(|n| entry(make(n), n as u32))
                .collect();
            initial.sort();
            let leaf = IndexLeafPage::new_with_resolved_layout(initial.clone(), next);
            layouts.insert(leaf.key_layout);
            let mut bytes = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
            // Leave holes in the record area, preserving the original layout.
            for row in initial.iter().skip(2).step_by(5) {
                try_delete_raw_leaf(&mut bytes, row, 81).unwrap();
            }
            let mut incoming: Vec<_> = (1..250).map(|n| entry(make(n), n as u32)).collect();
            incoming.extend(incoming.iter().step_by(7).cloned().collect::<Vec<_>>());
            incoming.extend((0..240).step_by(3).map(|n| entry(make(n), n as u32 + 1000)));
            incoming.sort();
            compare_run(&bytes, &incoming);
            compare_run(&bytes, &incoming[..1]);
            compare_run(&bytes, &[]);
            compare_run_with_bound::<true>(&bytes, &incoming);
            compare_run_with_bound::<true>(&bytes, &incoming[..1]);
            compare_run_with_bound::<true>(&bytes, &[]);
        }
    }
    for layout in [
        KEY_LAYOUT_GENERIC,
        KEY_LAYOUT_COMPACT_INT_U16,
        KEY_LAYOUT_COMPACT_INT_U32,
        KEY_LAYOUT_COMPACT_UINT_U16,
        KEY_LAYOUT_COMPACT_UINT_U32,
        KEY_LAYOUT_COMPACT_INT2,
        KEY_LAYOUT_COMPACT_DATE_U16,
        KEY_LAYOUT_COMPACT_TIME_U32,
        KEY_LAYOUT_COMPACT_DECIMAL_I32,
        KEY_LAYOUT_COMPACT_DECIMAL_I64,
    ] {
        assert!(layouts.contains(&layout), "layout {layout} not exercised");
    }
}

#[test]
fn raw_leaf_batch_preserves_prefix_on_overflow_and_layout_change() {
    for next in [None, Some(42)] {
        let empty =
            encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(vec![], next))).unwrap();
        let mut rows: Vec<_> = (0..1000)
            .map(|n| {
                entry(
                    vec![KeyPart::String(format!("{n:05}-{}", "x".repeat(80)))],
                    n,
                )
            })
            .collect();
        rows.sort();
        let (count, bytes) = compare_run(&empty, &rows);
        assert!(count > 0 && count < rows.len());
        compare_run(&bytes, &rows[count..]);
        let (count, bytes) = compare_run_with_bound::<true>(&empty, &rows);
        assert!(count > 0 && count < rows.len());
        compare_run_with_bound::<true>(&bytes, &rows[count..]);
    }
    let initial = vec![
        entry(vec![KeyPart::Int(0)], 0),
        entry(vec![KeyPart::Int(100)], 100),
    ];
    let bytes = encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new_with_resolved_layout(
        initial.clone(),
        None,
    )))
    .unwrap();
    let input = vec![
        entry(vec![KeyPart::Int(10)], 10),
        entry(vec![KeyPart::Int(1_000_000)], 20),
    ];
    assert_eq!(compare_run(&bytes, &input).0, 1);
    assert_eq!(compare_run(&bytes, &input[1..]).0, 0);
    // An exact duplicate consumes input without changing writer metadata.
    compare_run(&bytes, &[initial[0].clone(), initial[0].clone()]);
}

#[test]
fn raw_leaf_batch_error_does_not_partially_modify_page() {
    let initial = (0..8)
        .map(|n| entry(vec![KeyPart::Int(n * 100)], n as u32))
        .collect();
    let mut bytes =
        encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(initial, None))).unwrap();
    let offset = parse_leaf_payload_header(&bytes)
        .unwrap()
        .read_slot_offset(6);
    // The first search probes slots 4, 2, 1, 0. A later search visits corrupt
    // slot 6 after the first record has been staged but not published.
    bytes[INDEX_HEADER_BYTES + offset + 1] = 255;
    let before = bytes.clone();
    let input = vec![
        entry(vec![KeyPart::Int(-1)], 1),
        entry(vec![KeyPart::Int(650)], 2),
    ];
    assert!(try_insert_raw_leaf_run(&mut bytes, SortedIndexEntryRun::prefix(&input), 91).is_err());
    assert_eq!(bytes, before);
    assert!(try_insert_raw_leaf_run_with_bound::<true>(
        &mut bytes,
        SortedIndexEntryRun::prefix(&input),
        91
    )
    .is_err());
    assert_eq!(bytes, before);
}

#[test]
fn routed_leaf_run_extends_non_rightmost_observed_maximum() {
    let initial = vec![
        entry(vec![KeyPart::Int(0)], 0),
        entry(vec![KeyPart::Int(10)], 10),
    ];
    let bytes = encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new_with_resolved_layout(
        initial,
        Some(99),
    )))
    .unwrap();
    let incoming: Vec<_> = (11..50)
        .map(|n| entry(vec![KeyPart::Int(n)], n as u32))
        .collect();
    let mut actual = bytes.clone();
    let result = try_insert_raw_leaf_run_with_bound::<true>(
        &mut actual,
        SortedIndexEntryRun::prefix(&incoming),
        91,
    )
    .unwrap();
    assert_eq!(result, (true, Some(incoming.len())));
    let mut oracle = bytes;
    for row in &incoming {
        assert!(matches!(
            try_insert_raw_leaf(&mut oracle, row, 91).unwrap(),
            Some(InPlaceInsert::Inserted)
        ));
    }
    assert_eq!(decoded(&actual).entries, decoded(&oracle).entries);
    assert_eq!(decoded(&actual).next_leaf, Some(99));
}

#[test]
fn raw_leaf_batch_random_interleaving_and_duplicate_rowids() {
    for seed in 0..64u32 {
        let make = |n: u32| {
            entry(
                vec![
                    KeyPart::Int((n % 29) as i64),
                    KeyPart::String(format!("{:03}", n % 11)),
                ],
                n,
            )
        };
        let mut initial: Vec<_> = (0..300)
            .filter(|n| (n * 17 + seed) % 5 < 2)
            .map(make)
            .collect();
        initial.sort();
        let bytes =
            encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(initial, None))).unwrap();
        let mut input: Vec<_> = (0..450)
            .filter(|n| (n * 13 + seed) % 7 < 4)
            .map(make)
            .collect();
        input.extend(input.clone());
        input.sort();
        compare_run(&bytes, &input);
        compare_run_with_bound::<true>(&bytes, &input);
    }
}

#[test]
fn compact_scalar_page_comparison_matches_key_order_and_rowid_cursor() {
    let cases = [
        (
            KEY_LAYOUT_COMPACT_DATE_U16,
            18_000,
            vec![KeyPart::Date(18_000), KeyPart::Date(18_100)],
        ),
        (
            KEY_LAYOUT_COMPACT_TIME_U32,
            -100,
            vec![KeyPart::DateTime(-100), KeyPart::DateTime(100)],
        ),
        (
            KEY_LAYOUT_COMPACT_DECIMAL_I32,
            2,
            vec![
                KeyPart::Decimal {
                    value: i32::MIN as i128,
                    scale: 2,
                },
                KeyPart::Decimal {
                    value: i32::MAX as i128,
                    scale: 2,
                },
            ],
        ),
        (
            KEY_LAYOUT_COMPACT_DECIMAL_I64,
            4,
            vec![
                KeyPart::Decimal {
                    value: i64::MIN as i128,
                    scale: 4,
                },
                KeyPart::Decimal {
                    value: i64::MAX as i128,
                    scale: 4,
                },
            ],
        ),
    ];
    for (layout, base, keys) in cases {
        let rows: Vec<_> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| entry(vec![k.clone()], i as u32))
            .collect();
        let bytes = encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new_with_layout(
            rows.clone(),
            None,
            layout,
            base,
        )))
        .unwrap();
        let header = parse_leaf_payload_header(&bytes).unwrap();
        let mut targets = vec![
            vec![],
            vec![KeyPart::Null],
            vec![KeyPart::Int(0)],
            vec![KeyPart::String("z".into())],
            vec![KeyPart::Date(i32::MIN)],
            vec![KeyPart::Date(i32::MAX)],
            vec![KeyPart::DateTime(i64::MIN)],
            vec![KeyPart::DateTime(i64::MAX)],
            vec![KeyPart::Decimal {
                value: -1,
                scale: 2,
            }],
            vec![KeyPart::Decimal { value: 0, scale: 4 }],
            vec![KeyPart::Decimal {
                value: i128::MAX,
                scale: 4,
            }],
        ];
        for key in keys {
            targets.push(vec![key.clone()]);
            targets.push(vec![key, KeyPart::Null]);
        }
        for parts in targets {
            let target = IndexKey::from(parts);
            for (slot, row) in rows.iter().enumerate() {
                let (cmp, mut cursor) = header.cmp_key_at(slot, &target).unwrap();
                assert_eq!(cmp, row.key.cmp(&target));
                assert_eq!(
                    decode_row_id_v7_from(header.payload, &mut cursor, 0).unwrap(),
                    row.row_id
                );
            }
        }
    }
}

#[test]
fn leaf_run_sparse_dense_search_matches_single_insert() {
    // Exercise exponential-search boundaries, equal insertion positions and
    // far jumps in both compact and generic layouts. The oracle performs
    // independent single-record binary searches and mutations.
    for width in [0, 96] {
        for count in [1, 2, 3, 7, 31, 127, 255] {
            let make = |n: i64, id: u32| {
                let key = if width == 0 {
                    vec![KeyPart::Int(n)]
                } else {
                    vec![KeyPart::String(format!("{n:08}-{}", "x".repeat(width)))]
                };
                entry(key, id)
            };
            let initial: Vec<_> = (0..count).map(|n| make(n * 10, n as u32)).collect();
            if encoded_leaf_page_len(&IndexLeafPage::new(initial.clone(), None)) > PAGE_SIZE {
                continue;
            }
            for next in [None, Some(99)] {
                let leaf = IndexLeafPage::new_with_resolved_layout(initial.clone(), next);
                let bytes = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
                for stride in [1, 2, 9, 10, 11, 63, 511] {
                    let mut input: Vec<_> = (0..count * 10 + 20)
                        .step_by(stride)
                        .map(|n| make(n, (n + 10000) as u32))
                        .collect();
                    input.extend(initial.iter().step_by(3).cloned());
                    input.sort();
                    compare_run(&bytes, &input);
                }
            }
        }
    }
}

#[test]
fn audit1109_buffered_single_insert_records_online_build_log() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(dir.path()).unwrap(),
    ));
    let pool = BufferPool::new(
        512,
        log.clone(),
        DurabilityMode::Relaxed,
        Duration::from_secs(2),
    );
    let mut index = IndexFile::open_with_durability(
        crate::storage::naming::index_object_path_by_id(dir.path(), 1, 1, 1, "idx"),
        log.clone(),
        DurabilityMode::Relaxed,
        pool,
    )
    .unwrap();
    let build_log = Arc::new(OnlineIndexBuildLog::new());
    index.online_build_log = Some(build_log.clone());
    begin_txn_wal_buffering(&log, 71, 0, true);
    let item = entry(vec![KeyPart::Int(7)], 7);
    index
        .insert_deferred(71, item.key.clone(), item.row_id)
        .unwrap();
    assert!(index.entries().unwrap().is_empty());
    let changes = build_log.take().unwrap();
    assert_eq!(changes.entries.len(), 1);
    assert_eq!(changes.entries[0].key, item.key);
    take_txn_wal_entries(&log, 71);
    end_txn_wal_buffering(&log, 71);
}
