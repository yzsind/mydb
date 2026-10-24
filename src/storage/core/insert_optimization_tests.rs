use super::*;

#[test]
fn internal_insert_intervals_match_decoded_separator_order() {
    for kind in 0..18 {
        let mut keys: Vec<_> = (10..240).step_by(5).map(|n| row_entry(kind, n)).collect();
        // Repeated secondary keys must route by RowId on either side of a
        // separator, including an entry exactly equal to the separator.
        keys.extend(keys.clone().into_iter().map(|mut entry| {
            entry.row_id.page_id += 1000;
            entry
        }));
        keys.sort();
        let children: Vec<_> = (0..=keys.len()).map(|n| 100 + n as u64).collect();
        let encoded = encode_index_tree_page(&IndexPage::Internal(IndexInternalPage::new_generic(
            keys.clone(),
            children.clone(),
        )))
        .unwrap();
        let page = parse_raw_internal_route_page(&encoded).unwrap().unwrap();
        let mut entries: Vec<_> = (0..270).map(|n| row_entry(kind, n)).collect();
        for separator in &keys {
            for slot_delta in [-1i32, 0, 1] {
                let mut entry = separator.clone();
                entry.row_id.slot_id = (i32::from(entry.row_id.slot_id) + slot_delta)
                    .clamp(0, i32::from(u16::MAX)) as u16;
                entries.push(entry);
            }
        }
        entries.sort();
        // Sparse batches can skip many children. Check every exponential
        // bracket size and the final-child case against decoded separators.
        for floor in (0..=keys.len()).step_by(7) {
            for probe in entries.iter().step_by(13) {
                let expected = keys.partition_point(|key| key <= probe);
                if expected >= floor {
                    assert_eq!(
                        page.route_near(&probe.key, probe.row_id, floor, children[floor])
                            .unwrap(),
                        (children[expected], expected),
                        "sparse jump kind={kind} floor={floor} expected={expected}"
                    );
                }
            }
        }
        let mut start = 0;
        let mut floor = 0;
        let mut previous_child = children[0];
        while start < entries.len() {
            let entry = &entries[start];
            let expected = keys.partition_point(|key| key <= entry);
            let (child, slot) = page
                .route_from(&entry.key, entry.row_id, floor, previous_child)
                .unwrap();
            assert_eq!(
                page.route_near(&entry.key, entry.row_id, floor, previous_child)
                    .unwrap(),
                (child, slot),
                "monotonic route kind={kind}"
            );
            assert_eq!((child, slot), (children[expected], expected), "kind={kind}");
            let end = page
                .insert_run_end(&entries, start, entries.len(), slot)
                .unwrap();
            let expected_end = entries[start..]
                .iter()
                .position(|probe| keys.partition_point(|key| key <= probe) != expected)
                .map_or(entries.len(), |offset| start + offset);
            assert_eq!(end, expected_end, "kind={kind}, start={start}");
            assert!(end > start);
            floor = slot;
            previous_child = child;
            start = end;
        }
        // A batch can be one element, sparse, dense, or a non-monotonic
        // sequence of sorted runs. Every bounded suffix obeys the same oracle.
        for start in (0..entries.len()).step_by(7).rev() {
            let entry = &entries[start];
            let expected = keys.partition_point(|key| key <= entry);
            for end in [start + 1, (start + 3).min(entries.len()), entries.len()] {
                let actual = page.insert_run_end(&entries, start, end, expected).unwrap();
                let oracle = (start + 1..end)
                    .find(|&n| keys.partition_point(|key| key <= &entries[n]) != expected)
                    .unwrap_or(end);
                assert_eq!(actual, oracle, "kind={kind}");
            }
        }
    }
}

#[test]
fn sparse_insert_search_matches_decoded_full_entry_order() {
    for kind in 0..18 {
        let mut entries: Vec<_> = (0..240).step_by(3).map(|n| row_entry(kind, n)).collect();
        entries.extend(entries.clone().into_iter().map(|mut row| {
            row.row_id.page_id += 1234;
            row
        }));
        entries.sort();
        let mut bytes = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(entries, Some(77)),
        ))
        .unwrap();
        let removed = decode_entries(&bytes)[17].clone();
        try_delete_raw_leaf(&mut bytes, &removed, 8).unwrap();
        let oracle = decode_entries(&bytes);
        for n in 0..300 {
            for page in [0, n / 32, n / 32 + 1234, u32::MAX] {
                let mut row = row_entry(kind, n);
                row.row_id.page_id = page;
                let expected = oracle.binary_search(&row);
                assert_eq!(
                    sparse_leaf_slots_and_position(&bytes, &row).unwrap().1,
                    expected,
                    "kind={kind} n={n} page={page}"
                );
                let mut actual = bytes.clone();
                let mut old = bytes.clone();
                let new_result = try_insert_raw_leaf_search::<true>(&mut actual, &row, 9).unwrap();
                let old_result = try_insert_raw_leaf(&mut old, &row, 9).unwrap();
                assert_eq!(format!("{new_result:?}"), format!("{old_result:?}"));
                assert_eq!(decode_entries(&actual), decode_entries(&old));
            }
        }
    }
}

#[test]
fn sparse_compact_search_covers_all_integer_widths_and_boundaries() {
    for (first, first_max) in [(1, 200), (2, 40000), (4, 10000000)] {
        for (second, second_max) in [(1, 199), (2, 30000), (4, 20000000)] {
            let mut entries: Vec<_> = (0..100u32)
                .map(|n| IndexEntry {
                    key: IndexKey::from(vec![
                        KeyPart::Int(-100 + n as i64 * first_max / 99),
                        KeyPart::Int(-200 + ((n * 37) % 100) as i64 * second_max / 99),
                    ]),
                    row_id: RowId {
                        page_id: n,
                        slot_id: n as u16,
                    },
                })
                .collect();
            entries.sort();
            let leaf = IndexLeafPage::new_with_resolved_layout(entries.clone(), None);
            assert_eq!(leaf.key_layout, KEY_LAYOUT_COMPACT_INT2);
            let aux = leaf.layout_aux.unwrap();
            assert_eq!((aux.width1, aux.width2), (first, second));
            let bytes = encode_index_tree_page(&IndexPage::Leaf(leaf)).unwrap();
            for row in &entries {
                for a in [-1, 0, 1] {
                    for b in [-1, 0, 1] {
                        let mut probe = row.clone();
                        let [KeyPart::Int(x), KeyPart::Int(y)] = probe.key.0.as_mut_slice() else {
                            unreachable!()
                        };
                        *x += a;
                        *y += b;
                        assert_eq!(
                            sparse_leaf_slots_and_position(&bytes, &probe).unwrap().1,
                            entries.binary_search(&probe)
                        );
                    }
                }
            }
            // Out-of-range targets and signed wrap boundaries must decline
            // packed order, including when only the second component differs.
            for x in [i64::MIN, -101, 0, first_max, i64::MAX] {
                for y in [i64::MIN, -201, 0, second_max, i64::MAX] {
                    let probe = IndexEntry {
                        key: IndexKey::from(vec![KeyPart::Int(x), KeyPart::Int(y)]),
                        row_id: RowId {
                            page_id: u32::MAX,
                            slot_id: u16::MAX,
                        },
                    };
                    assert_eq!(
                        sparse_leaf_slots_and_position(&bytes, &probe).unwrap().1,
                        entries.binary_search(&probe)
                    );
                }
            }
        }
    }
}

#[test]
fn sparse_scalar_search_preserves_wrapping_fallback_and_corruption_errors() {
    let key_sets = [
        (0..80)
            .map(|n| KeyPart::Int(i64::MAX - 100 + n))
            .collect::<Vec<_>>(),
        (0..80).map(|n| KeyPart::Int(i64::MIN + n)).collect(),
        (0..80).map(|n| KeyPart::UInt(u64::MAX - 100 + n)).collect(),
        (0..80).map(|n| KeyPart::UInt(n * 100_000)).collect(),
        (0..80).map(|n| KeyPart::Date(i32::MAX - 100 + n)).collect(),
        (0..80)
            .map(|n| KeyPart::DateTime(i64::MAX - 100 + n))
            .collect(),
    ];
    for keys in key_sets {
        let entries: Vec<_> = keys
            .iter()
            .enumerate()
            .map(|(i, key)| IndexEntry {
                key: IndexKey::from(vec![key.clone()]),
                row_id: RowId {
                    page_id: i as u32,
                    slot_id: i as u16,
                },
            })
            .collect();
        let bytes = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(entries.clone(), None),
        ))
        .unwrap();
        for key in keys {
            for page_id in [0, 37, u32::MAX] {
                let entry = IndexEntry {
                    key: IndexKey::from(vec![key.clone()]),
                    row_id: RowId {
                        page_id,
                        slot_id: u16::MAX,
                    },
                };
                assert_eq!(
                    sparse_leaf_slots_and_position(&bytes, &entry).unwrap().1,
                    entries.binary_search(&entry)
                );
            }
        }
    }
    let row = row_entry(0, 32);
    let mut bytes = encode_index_tree_page(&IndexPage::Leaf(
        IndexLeafPage::new_with_resolved_layout(vec![row.clone()], None),
    ))
    .unwrap();
    let header = parse_leaf_payload_header(&bytes).unwrap();
    let slot = INDEX_HEADER_BYTES + header.slot_array_start;
    // A complete key but truncated RowId must fail before mutating bytes.
    write_u16(&mut bytes, slot + 2, 2);
    let original = bytes.clone();
    assert!(try_insert_raw_leaf_search::<true>(&mut bytes, &row, 9).is_err());
    assert_eq!(bytes, original);
}

#[test]
fn internal_routing_rejects_truncated_tails_for_equal_and_unequal_keys() {
    let keys = vec![row_entry(0, 10), row_entry(0, 20)];
    let mut encoded = encode_index_tree_page(&IndexPage::Internal(IndexInternalPage::new_generic(
        keys,
        vec![11, 12, 13],
    )))
    .unwrap();
    let page = parse_raw_internal_route_page(&encoded).unwrap().unwrap();
    let slot_offset = INDEX_HEADER_BYTES + page.header.slot_array_start + SLOT_OFFSET_BYTES;
    let slot = page.header.read_slot(1);
    write_u16(&mut encoded, slot_offset + 2, slot.len - 1);
    let page = parse_raw_internal_route_page(&encoded).unwrap().unwrap();
    for n in [0, 20, 100] {
        let entry = row_entry(0, n);
        assert!(page
            .route_from(&entry.key, entry.row_id, 0, page.first_child)
            .is_err());
    }
}

#[test]
fn cached_insert_intervals_revalidate_parent_epochs_and_replaced_frames() {
    let dir = insert_lock_test_dir();
    let log = test_log(dir.path());
    let pool = BufferPool::new(
        1024,
        log.clone(),
        DurabilityMode::Strict,
        Duration::from_secs(5),
    );
    let index = IndexFile::open_with_durability(
        t_index_path(dir.path(), "routes", 1, "k", "idx"),
        log,
        DurabilityMode::Strict,
        pool,
    )
    .unwrap();
    let wide_entry = |n| {
        let mut entry = row_entry(6, n);
        let KeyPart::String(value) = &mut entry.key.0[0] else {
            unreachable!()
        };
        value.push_str(&"x".repeat(384));
        entry
    };
    let mut initial: Vec<_> = (0..4000).step_by(2).map(wide_entry).collect();
    initial.sort();
    index.insert_entries_logged(1, initial, false).unwrap();
    let mut probes: Vec<_> = (0..4500).map(wide_entry).collect();
    probes.sort();
    let mut path = smallvec::SmallVec::<[InsertRouteFrame; 4]>::new();
    let check = |pos, path: &mut smallvec::SmallVec<[InsertRouteFrame; 4]>| {
        let _guard = index.structure_lock.read();
        let (pin, end) = index
            .find_leaf_for_insert_run(&probes, pos, probes.len(), path)
            .unwrap();
        let actual = pin.frame.key.page_id;
        assert!(end > pos);
        for entry in &probes[pos..end] {
            let expected = index
                .find_leaf_for_entry_with_candidate(entry, entry)
                .unwrap()
                .0;
            assert_eq!(actual, expected.frame.key.page_id, "pos={pos}");
        }
        end
    };
    let mut pos = 0;
    while pos < probes.len() {
        pos = check(pos, &mut path);
    }
    check(500, &mut path);
    assert!(
        path.len() >= 3,
        "exercise routing proofs at multiple internal levels"
    );
    let root_key = path[0].pinned.frame.key.clone();
    let epoch = path[0].epoch;
    // Insert enough new keys to split leaves and modify a retained parent's
    // epoch. This is deliberately done with no topology guard held.
    let mut extra: Vec<_> = (1..9000).step_by(2).map(wide_entry).collect();
    extra.sort();
    index.insert_entries_logged(2, extra, false).unwrap();
    assert!(
        path[0].pinned.frame.dirty_epoch.load(Ordering::Acquire) != epoch
            || index.root_page.load(Ordering::Acquire) != root_key.page_id
    );
    check(501, &mut path);
    index.flush().unwrap();
    let leaf_key = path.last().unwrap().pinned.frame.key.clone();
    index.buffer_pool.discard_page(&leaf_key).unwrap();
    check(502, &mut path);
    assert_eq!(path.len(), 1, "cold admission must release ancestor pins");
    check(503, &mut path);
    assert!(path.len() > 1);
    let root_key = path[0].pinned.frame.key.clone();
    index.buffer_pool.discard_page(&root_key).unwrap();
    assert_eq!(
        FrameIoState::from_u8(path[0].pinned.frame.state.load(Ordering::Acquire)),
        FrameIoState::Discarded
    );
    check(502, &mut path);
    for pos in [0, 4000, 1000, 4499, 999] {
        check(pos, &mut path);
    }
    path.clear();
    index.flush().unwrap();
    IndexFile::validate(index.path.clone()).unwrap();
}

fn row_entry(kind: usize, n: u32) -> IndexEntry {
    let key = match kind {
        0 => vec![KeyPart::Int(n as i64 - 150)],
        1 => vec![KeyPart::UInt((1 << 63) + n as u64 * 100_000)],
        2 => vec![KeyPart::Int(n as i64 / 7), KeyPart::Int(n as i64 % 7)],
        3 => vec![KeyPart::Date(18_000 + n as i32)],
        4 => vec![KeyPart::DateTime(-1_000_000 + n as i64 * 10_000)],
        5 => vec![KeyPart::Decimal {
            value: n as i128 * 1_000_000_000,
            scale: 4,
        }],
        6 => vec![KeyPart::String(format!(
            "键-{n:05}-{}",
            "x".repeat(n as usize % 71)
        ))],
        7 => vec![KeyPart::Bytes(vec![0, (n / 256) as u8, n as u8, 255])],
        8 => vec![KeyPart::Float64(f64_to_order_u64(n as f64 / 7.0))],
        9 => vec![KeyPart::Null, KeyPart::Int(n as i64)],
        10 => vec![KeyPart::CollatedString {
            collation_id: 45,
            value: format!("Été-{n:05}"),
        }],
        11 => vec![
            KeyPart::Int(-9),
            KeyPart::Int(n as i64),
            KeyPart::Int(i64::MAX),
        ],
        12 => vec![
            KeyPart::Null,
            KeyPart::String(format!("字-{n}")),
            KeyPart::UInt(u64::MAX),
        ],
        13 => vec![KeyPart::DecimalWide {
            value: num_bigint::BigInt::from(n as i64 - 150) << 140,
            scale: 12,
        }],
        14 => vec![KeyPart::Time(n as i64 * 1_000_000 - 150_000_000)],
        15 => vec![KeyPart::Year((n % 256) as u8)],
        16 => vec![KeyPart::Float32(f32_to_order_u32(n as f32 / 7.0 - 17.0))],
        _ => vec![KeyPart::Int(if n % 2 == 0 {
            i64::MIN + n as i64
        } else {
            i64::MAX - n as i64
        })],
    };
    IndexEntry {
        key: IndexKey::from(key),
        row_id: RowId {
            page_id: n / 32,
            slot_id: (n % 32) as u16,
        },
    }
}

fn decode_entries(bytes: &[u8]) -> Vec<IndexEntry> {
    let IndexPage::Leaf(leaf) = decode_validated_index_tree_page(bytes).unwrap() else {
        panic!("leaf expected")
    };
    leaf.entries
}

#[test]
fn routed_secondary_batches_preserve_all_key_types_and_reopen() {
    for kind in 0..18 {
        let dir = insert_lock_test_dir();
        let log = test_log(dir.path());
        let pool = BufferPool::new(
            256,
            log.clone(),
            DurabilityMode::Strict,
            Duration::from_secs(5),
        );
        let path = t_index_path(dir.path(), "routed", 1, "k", "idx");
        let make = |n: u32| {
            let mut entry = row_entry(kind, n % 19);
            entry.row_id = RowId {
                page_id: n / 32,
                slot_id: (n % 32) as u16,
            };
            entry
        };
        let mut expected: Vec<_> = (0..1600).map(make).collect();
        let check = |index: &IndexFile, expected: &[IndexEntry]| {
            for key in (0..19).map(|n| row_entry(kind, n).key) {
                let mut oracle: Vec<_> = expected
                    .iter()
                    .filter(|e| e.key == key)
                    .map(|e| e.row_id)
                    .collect();
                oracle.sort_unstable();
                oracle.dedup();
                let mut actual = index.get(&key, true).unwrap();
                actual.sort_unstable();
                assert_eq!(actual, oracle, "kind={kind}, key={key:?}");
            }
        };
        {
            let index = IndexFile::open_with_durability(
                path.clone(),
                log.clone(),
                DurabilityMode::Strict,
                pool.clone(),
            )
            .unwrap();
            expected.sort();
            index
                .insert_entries_logged(1, expected.clone(), false)
                .unwrap();
            // Delete records before extending repeated keys into non-rightmost
            // gaps. This also covers stale separators and record-body holes.
            for entry in expected.iter().step_by(13) {
                index
                    .delete_with_txn(2, entry.key.clone(), entry.row_id)
                    .unwrap();
            }
            expected = expected
                .into_iter()
                .enumerate()
                .filter(|(n, _)| n % 13 != 0)
                .map(|(_, e)| e)
                .collect();
            let mut extra: Vec<_> = (1600..3200).map(make).collect();
            extra.extend(extra.iter().step_by(11).cloned().collect::<Vec<_>>());
            extra.sort();
            index
                .insert_entries_logged(3, extra.clone(), false)
                .unwrap();
            expected.extend(extra);
            // Nonmonotonic input is decomposed into independently routed
            // sorted prefixes; it must not inherit a preceding run's fence.
            let extra: Vec<_> = (3200..3400).rev().map(make).collect();
            index
                .insert_entries_logged(4, extra.clone(), false)
                .unwrap();
            expected.extend(extra);
            check(&index, &expected);
            index.flush().unwrap();
            pool.discard_file(&index.file_key).unwrap();
        }
        let index =
            IndexFile::open_with_durability(path.clone(), log, DurabilityMode::Strict, pool)
                .unwrap();
        check(&index, &expected);
        index.flush().unwrap();
        IndexFile::validate(path).unwrap();
    }
}

#[test]
fn leaf_directory_relocation_matches_independent_sorted_model() {
    for kind in 0..11 {
        let mut initial: Vec<_> = (0..240).step_by(3).map(|n| row_entry(kind, n)).collect();
        initial.sort();
        let mut bytes = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(initial.clone(), None),
        ))
        .unwrap();
        for removed in initial.iter().skip(2).step_by(5) {
            try_delete_raw_leaf(&mut bytes, removed, 80).unwrap();
        }
        let original = decode_entries(&bytes);
        // Each single insert is checked against a decoded Vec model, rather
        // than another raw-page function that might share a relocation bug.
        for n in 0..260 {
            let incoming = row_entry(kind, n);
            let mut actual = bytes.clone();
            let mut expected = original.clone();
            match try_insert_raw_leaf(&mut actual, &incoming, 91).unwrap() {
                Some(InPlaceInsert::Inserted) => expected.push(incoming),
                Some(InPlaceInsert::Duplicate) => {}
                _ => {
                    assert_eq!(actual, bytes);
                    continue;
                }
            }
            expected.sort();
            expected.dedup();
            assert_eq!(decode_entries(&actual), expected, "kind={kind}, n={n}");
        }
        let mut incoming: Vec<_> = (1..270).map(|n| row_entry(kind, n)).collect();
        incoming.extend(incoming.clone());
        incoming.sort();
        let (_, consumed) =
            try_insert_raw_leaf_run(&mut bytes, SortedIndexEntryRun::prefix(&incoming), 92)
                .unwrap();
        let mut expected = original;
        expected.extend_from_slice(&incoming[..consumed.unwrap_or(0)]);
        expected.sort();
        expected.dedup();
        assert_eq!(decode_entries(&bytes), expected, "batch kind={kind}");
    }
}

#[test]
fn cached_insert_navigation_revalidates_routes_and_discarded_frames() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    create_concurrent_index_update_table(&db, 1);
    let table = db.table("test.idx_concurrent").unwrap().clone();
    let mut path = smallvec::SmallVec::<[PinnedFrame; 4]>::new();
    db.with_index_dml(table.id, "k_1", |index| {
        let entry = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(1)]),
            row_id: min_lookup_row_id(),
        };
        let _guard = index.structure_lock.read();
        index.find_leaf_for_insert_cached(&entry, &mut path)?;
        Ok(())
    })
    .unwrap();
    let old_root = path[0].frame.key.page_id;
    // Another batch changes a leaf root into an internal root while the old
    // path is retained without any topology lock. The next lookup must start
    // from the newly published root, never from that old leaf.
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.idx_concurrent",
        (2..=2048)
            .map(|id| vec![Value::Int(id), Value::Int(id)])
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    db.with_index_dml(table.id, "k_1", |index| {
        assert_ne!(index.root_page.load(Ordering::Acquire), old_root);
        for id in [1, 1024, 2048, 17, 1999] {
            let entry = IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(id)]),
                row_id: min_lookup_row_id(),
            };
            let _guard = index.structure_lock.read();
            let cached = index
                .find_leaf_for_insert_cached(&entry, &mut path)?
                .frame
                .key
                .page_id;
            let expected = index
                .find_leaf_for_entry_with_candidate(&entry, &entry)?
                .0
                .frame
                .key
                .page_id;
            assert_eq!(cached, expected);
        }
        // A cold child must release the retained ancestors before admitting
        // its bytes. Exercise this separately from a miss at the root.
        index.flush()?;
        let leaf_key = path.last().unwrap().frame.key.clone();
        index.buffer_pool.discard_page(&leaf_key)?;
        let entry = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(1999)]),
            row_id: min_lookup_row_id(),
        };
        {
            let _guard = index.structure_lock.read();
            index.find_leaf_for_insert_cached(&entry, &mut path)?;
            assert_eq!(path.len(), 1, "cold child must release ancestor pins");
            // Both pages are now resident: the next descent can cache the
            // whole path again without any page admission.
            index.find_leaf_for_insert_cached(&entry, &mut path)?;
            assert!(path.len() > 1);
        }
        // Replace the cached root frame with a fresh map entry carrying the
        // same page id. Retaining the old pin must never reuse discarded bytes.
        index.flush()?;
        let root_key = path[0].frame.key.clone();
        index.buffer_pool.discard_page(&root_key)?;
        let misses = index.buffer_pool.misses.load(Ordering::Relaxed);
        assert!(index
            .buffer_pool
            .try_pin_resident_page(&root_key)?
            .is_none());
        assert_eq!(index.buffer_pool.misses.load(Ordering::Relaxed), misses);
        assert_eq!(
            FrameIoState::from_u8(path[0].frame.state.load(Ordering::Acquire)),
            FrameIoState::Discarded
        );
        let entry = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(1024)]),
            row_id: min_lookup_row_id(),
        };
        let _guard = index.structure_lock.read();
        let cached = index
            .find_leaf_for_insert_cached(&entry, &mut path)?
            .frame
            .key
            .page_id;
        let expected = index
            .find_leaf_for_entry_with_candidate(&entry, &entry)?
            .0
            .frame
            .key
            .page_id;
        assert_eq!(cached, expected);
        assert_eq!(path.len(), 1, "cold descent must release ancestor pins");
        assert_ne!(
            FrameIoState::from_u8(path[0].frame.state.load(Ordering::Acquire)),
            FrameIoState::Discarded
        );
        Ok(())
    })
    .unwrap();
    db.check_consistency().unwrap();
}

#[test]
fn concurrent_batch_insert_navigation_preserves_rollback_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Arc::new(
            Database::open_with_config(
                dir.path(),
                test_storage_config(DurabilityMode::Strict, true),
            )
            .unwrap(),
        );
        create_concurrent_index_update_table(&db, 0);
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let workers: Vec<_> = (0..4i64)
            .map(|worker| {
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for batch in 0..12 {
                        let txn = db.begin_txn().unwrap();
                        let rows = (0..96)
                            .map(|local| {
                                let id = 1 + worker + 4 * (batch * 96 + local);
                                vec![Value::Int(id), Value::Int((id * 173) % 4099)]
                            })
                            .collect();
                        db.insert_rows(txn, "test.idx_concurrent", rows).unwrap();
                        if batch % 3 == 0 {
                            db.rollback_txn(txn.txn_id).unwrap();
                        } else {
                            db.commit_txn(txn.txn_id).unwrap();
                        }
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        db.check_consistency().unwrap();
        let db = Arc::try_unwrap(db).ok().unwrap();
        db.buffer_pool.shutdown_flusher();
        std::mem::forget(db); // Reopen without an orderly checkpoint.
    }
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    db.check_consistency().unwrap();
    let table = db.table("test.idx_concurrent").unwrap().clone();
    let index = table.indexes.iter().find(|i| i.name == "PRIMARY").unwrap();
    for ordinal in 0..4 * 12 * 96i64 {
        let id = ordinal + 1;
        let batch = (ordinal / 4) / 96;
        let rows = db
            .lookup_index(&table, index, IndexKey::from(vec![KeyPart::Int(id)]))
            .unwrap();
        assert_eq!(rows.len(), usize::from(batch % 3 != 0), "id={id}");
    }
}

#[test]
fn committed_native_leaf_batches_recover_after_process_exit_without_checkpoint() {
    const CHILD_DIR: &str = "MYDB_TEST_NATIVE_CRASH_DIR";
    const TEST_NAME: &str = "storage::core::tests::insert_optimization::committed_native_leaf_batches_recover_after_process_exit_without_checkpoint";
    if let Some(path) = std::env::var_os(CHILD_DIR) {
        let path = std::path::PathBuf::from(path);
        let db =
            Database::open_with_config(&path, test_storage_config(DurabilityMode::Strict, true))
                .unwrap();
        create_concurrent_index_update_table(&db, 0);
        for start in (1..=80_000i64).step_by(2000) {
            let txn = db.begin_txn().unwrap();
            db.insert_rows(
                txn,
                "test.idx_concurrent",
                (start..start + 2000)
                    .map(|id| vec![Value::Int(id), Value::Int((id * 173) % 16381)])
                    .collect(),
            )
            .unwrap();
            db.commit_txn(txn.txn_id).unwrap();
        }
        db.checkpoint().unwrap();
        // Preserve the intended crash precondition independently of scheduling.
        // Otherwise the background flusher may materialize these native images
        // before the child exits, without any recovery defect.
        db.buffer_pool.shutdown_flusher();
        for start in (80_001..=82_048i64).step_by(16) {
            let txn = db.begin_txn().unwrap();
            db.insert_rows(
                txn,
                "test.idx_concurrent",
                (start..start + 16)
                    .map(|id| vec![Value::Int(id), Value::Int((id * 173) % 16381)])
                    .collect(),
            )
            .unwrap();
            db.commit_txn(txn.txn_id).unwrap();
        }
        let table = db.table("test.idx_concurrent").unwrap();
        let primary = table.indexes.iter().find(|i| i.primary).unwrap();
        for id in 80_001..=80_064i64 {
            let row_id = db
                .lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(id)]))
                .unwrap()[0];
            let txn = db.begin_txn().unwrap();
            db.update_row_by_id(txn, &table, row_id, |row| {
                Ok(vec![row[0].clone(), Value::Int((id * 173) % 16381 + 1)])
            })
            .unwrap();
            db.commit_txn(txn.txn_id).unwrap();
        }
        for id in 80_065..=80_096i64 {
            let row_id = db
                .lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(id)]))
                .unwrap()[0];
            let txn = db.begin_txn().unwrap();
            assert!(db.delete_row_by_id(txn, &table, row_id).unwrap());
            db.commit_txn(txn.txn_id).unwrap();
        }
        let txn = db.begin_txn().unwrap();
        let row_id = db
            .lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(80_100)]))
            .unwrap()[0];
        db.update_row_by_id(txn, &table, row_id, |row| {
            Ok(vec![row[0].clone(), Value::Int(99)])
        })
        .unwrap();
        db.rollback_txn(txn.txn_id).unwrap();
        let native_count = db
            .with_index_read(table.id, "k_1", |index| {
                Ok(index
                    .buffer_pool
                    .shards
                    .iter()
                    .map(|shard| {
                        shard
                            .frames
                            .read()
                            .unwrap()
                            .values()
                            .filter(|frame| {
                                frame.key.file == index.file_key
                                    && frame.packed_leaf_state.load(Ordering::Acquire) == 2
                            })
                            .count()
                    })
                    .sum::<usize>())
            })
            .unwrap();
        assert!(
            native_count >= 32,
            "crash must lose pending native images: {native_count}"
        );
        std::fs::write(path.join("native-crash.marker"), native_count.to_string()).unwrap();
        // Deliberately bypass every Rust destructor and the database's orderly
        // shutdown path. The parent can recover only from durable WAL/files.
        std::process::exit(0);
    }
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_DIR, dir.path())
        .status()
        .unwrap();
    assert!(status.success());
    assert!(dir.path().join("native-crash.marker").exists());
    let db = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    let txn = db.begin_txn().unwrap();
    let rows = db.scan_rows("test.idx_concurrent", txn).unwrap();
    assert_eq!(rows.len(), 82_016);
    let table = db.table("test.idx_concurrent").unwrap();
    let primary = table.indexes.iter().find(|i| i.primary).unwrap();
    let secondary = table.indexes.iter().find(|i| i.name == "k_1").unwrap();
    for row in &rows {
        let Value::Int(id) = row.values[0] else {
            panic!("integer id");
        };
        let expected_k = (id * 173) % 16381 + i64::from((80_001..=80_064).contains(&id));
        assert!(!(80_065..=80_096).contains(&id));
        assert_eq!(row.values[1], Value::Int(expected_k));
        if id >= 80_001 || id % 997 == 0 {
            assert_eq!(
                db.lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(id)]))
                    .unwrap(),
                vec![row.row_id]
            );
            assert!(db
                .lookup_index(
                    &table,
                    secondary,
                    IndexKey::from(vec![KeyPart::Int(expected_k)])
                )
                .unwrap()
                .contains(&row.row_id));
        }
    }
    db.commit_txn(txn.txn_id).unwrap();
    db.check_consistency().unwrap();
}
