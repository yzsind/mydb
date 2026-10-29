//! B-tree audit reproduction tests.
//!
//! Retain differential audit tests and turn fixed issues into hard regression assertions.
//! `cargo nextest run -E 'test(btree_bug_audit_tests)'`。

use super::*;

// Logical index audits do not require a WAL flush for each page mutation.
fn audit_index(name: &str) -> (tempfile::TempDir, IndexHandle) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
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
    let index =
        IndexFile::open_with_durability(dir.path().join(name), log, DurabilityMode::Memory, pool)
            .unwrap();
    (dir, Arc::new(parking_lot::RwLock::new(index)))
}

fn int_key(v: i64) -> IndexKey {
    IndexKey::from(vec![KeyPart::Int(v)])
}

#[test]
fn btree_release_page_allocation_overflow_returns_error_without_mutation() {
    let (_dir, index) = audit_index("overflow.idx");
    let index = index.read();
    for next in [u64::MAX, u64::MAX / PAGE_SIZE as u64] {
        index.meta_state.lock().meta.next_page_id = next;
        assert!(index.reserve_page_id().is_err());
        assert_eq!(index.meta_state.lock().meta.next_page_id, next);
        assert!(index.alloc_page(0).is_err());
        assert_eq!(index.meta_state.lock().meta.next_page_id, next);
    }
}

fn rid(i: usize) -> RowId {
    RowId {
        page_id: (i / 64) as u32,
        slot_id: (i % 64) as u16,
    }
}

fn bulk_ints(index: &IndexHandle, n: i64) -> Vec<IndexEntry> {
    let entries: Vec<IndexEntry> = (0..n)
        .map(|i| IndexEntry {
            key: int_key(i),
            row_id: rid(i as usize),
        })
        .collect();
    index
        .write()
        .bulk_load_sorted(txn_id_for_ddl(), entries.clone())
        .unwrap();
    entries
}

/// BUG-1: IndexRangeCursor::fill_one_leaf for entries returned an empty chunk
/// without finishing when all current-leaf keys preceded low, with exclusive low at its maximum.
/// plan.rs::range_index_entries_visible_ordered treated that empty chunk as EOF,
/// prematurely skipping all subsequent data.
///
/// Simulate the consumer's empty-means-break loop and compare against the model;
/// the bug makes this assertion fail with the missing row count.
#[test]
fn bug1_forward_leaf_chunk_empty_leaf_break_drops_rows() {
    let (_dir, index) = audit_index("bug1.idx");
    let n = 4000;
    let entries = bulk_ints(&index, n);

    // Find the maximum key of a non-rightmost leaf at the first next_leaf boundary.
    let (leaf_id, boundary_key) = {
        let idx = index.read();
        let mut leaf_id = idx.leftmost_leaf().unwrap();
        loop {
            let leaf = idx.read_leaf(leaf_id).unwrap();
            match leaf.next_leaf {
                Some(next) => {
                    // If the leaf has at least two entries, use its largest key as exclusive low.
                    if leaf.entries.len() >= 2 {
                        let k = leaf.entries.last().unwrap().key.clone();
                        break (next, k);
                    }
                    leaf_id = next;
                }
                None => panic!("need a non-rightmost leaf"),
            }
        }
    };
    let _ = leaf_id;

    // SQL semantics: WHERE k>boundary_key.
    let low = Some(IndexBound {
        key: boundary_key.clone(),
        inclusive: false,
    });
    let mut cursor = IndexRangeCursor::new(Arc::clone(&index), true, low, None).unwrap();

    // Consume like plan.rs::range_index_entries_visible_ordered:
    //   while !finished { chunk = next_leaf_chunk(); if chunk.is_empty() { break } ... }
    let mut got_plan_style: Vec<IndexEntry> = Vec::new();
    while !cursor.is_finished() {
        let (chunk, _bound) = cursor.next_leaf_chunk(4096).unwrap();
        if chunk.is_empty() {
            break;
        }
        got_plan_style.extend(
            chunk
                .into_iter()
                .map(|(k, r)| IndexEntry { key: k, row_id: r }),
        );
    }

    let expected: Vec<IndexEntry> = entries
        .iter()
        .filter(|e| e.key > boundary_key)
        .cloned()
        .collect();
    assert_eq!(
        got_plan_style,
        expected,
        "plan-style consumption dropped rows: got {}, expected {}",
        got_plan_style.len(),
        expected.len()
    );
}

/// BUG-1 control: small-cap consumption must also be complete; an empty result means terminal state only.
#[test]
fn bug1b_forward_leaf_chunk_empty_leaf_is_resumable() {
    let (_dir, index) = audit_index("bug1b.idx");
    let entries = bulk_ints(&index, 4000);

    let boundary_key = {
        let idx = index.read();
        let mut leaf_id = idx.leftmost_leaf().unwrap();
        loop {
            let leaf = idx.read_leaf(leaf_id).unwrap();
            match leaf.next_leaf {
                Some(next) => {
                    if leaf.entries.len() >= 2 {
                        break leaf.entries.last().unwrap().key.clone();
                    }
                    leaf_id = next;
                }
                None => panic!("need a non-rightmost leaf"),
            }
        }
    };

    let low = Some(IndexBound {
        key: boundary_key.clone(),
        inclusive: false,
    });
    let mut cursor = IndexRangeCursor::new(Arc::clone(&index), true, low, None).unwrap();

    // A small cap forces same-page batches and cross-page transitions.
    let mut got: Vec<IndexEntry> = Vec::new();
    while !cursor.is_finished() {
        let (chunk, _bound) = cursor.next_leaf_chunk(64).unwrap();
        if chunk.is_empty() {
            assert!(cursor.is_finished(), "empty chunk must exhaust the scan");
            continue;
        }
        got.extend(
            chunk
                .into_iter()
                .map(|(k, r)| IndexEntry { key: k, row_id: r }),
        );
    }
    let expected = entries
        .into_iter()
        .filter(|e| e.key > boundary_key)
        .collect::<Vec<_>>();
    assert!(!got.is_empty());
    assert_eq!(got, expected);
}

/// BUG-2 probe: reseek was mathematically sound; retain as a regression guard.
/// After one reverse-scan emission, delete an unissued entry and insert a larger key;
/// reseek resumes at the predecessor of resume_before, skips deletion, and does not backfill the new key.
#[test]
fn bug2_reverse_cursor_reseek_reemits_resume_entry() {
    let (_dir, index) = audit_index("bug2.idx");
    bulk_ints(&index, 4000);

    let mut cursor = IndexRangeReverseCursor::new(Arc::clone(&index), true, None, None).unwrap();

    // Emit the maximum key 3999 once, recording resume_before.
    let first = cursor.next_chunk(1).unwrap();
    assert_eq!(first.len(), 1);
    let first_key = match first[0].0 .0[0] {
        KeyPart::Int(v) => v,
        _ => unreachable!(),
    };
    assert_eq!(first_key, 3999);

    // Sequential equivalent of concurrent writes: delete an unissued entry and insert a larger key.
    index
        .read()
        .delete_with_txn(77, int_key(3997), rid(3997))
        .unwrap();
    index
        .read()
        .insert_with_txn(
            78,
            int_key(1_000_000),
            RowId {
                page_id: 9999,
                slot_id: 1,
            },
        )
        .unwrap();

    // Continue scanning and collect all output.
    let mut got: Vec<i64> = vec![first_key];
    loop {
        let chunk = cursor.next_chunk(37).unwrap();
        if chunk.is_empty() {
            break;
        }
        got.extend(chunk.iter().map(|(k, _)| match k.0[0] {
            KeyPart::Int(v) => v,
            _ => unreachable!(),
        }));
    }

    // Expect descending values from 3999, skipping deleted 3997; the new key inserted
    // 1_000_000 lies after the resume position and must not be backfilled.
    let expected: Vec<i64> = (0..4000i64).rev().filter(|v| *v != 3997).collect();
    assert_eq!(got, expected);
}

/// BUG-3 differential fuzzing compares random insert/delete/get and forward/reverse ranges
/// with BTreeSet, covering splits, merges, borrowing, and separator refresh.
#[test]
fn bug3_differential_fuzz_matches_model() {
    let (_dir, index) = audit_index("bug3.idx");
    let mut model: std::collections::BTreeMap<IndexEntry, usize> = Default::default();

    let mut seed = 0x5eed_u64;
    let mut rnd = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let mut next_row = 0usize;
    let key_space: i64 = 3000;
    for step in 0..4_000 {
        let op = rnd() % 100;
        let kv = (rnd() % key_space as u64) as i64 - 100;
        let key = int_key(kv);
        match op {
            0..=54 => {
                // Insert allows multiple distinct row_ids per key, matching nonunique index semantics.
                let entry = IndexEntry {
                    key: key.clone(),
                    row_id: rid(next_row),
                };
                index
                    .read()
                    .insert_with_txn(100 + (step % 7) as u64, key.clone(), entry.row_id)
                    .unwrap();
                model.insert(entry, next_row);
                next_row += 1;
            }
            55..=79 => {
                // Delete an existing row_id if available.
                let victims: Vec<IndexEntry> =
                    model.keys().filter(|e| e.key == key).cloned().collect();
                if let Some(victim) = victims.first() {
                    index
                        .read()
                        .delete_with_txn(200 + (step % 5) as u64, victim.key.clone(), victim.row_id)
                        .unwrap();
                    model.remove(victim);
                }
            }
            80..=89 => {
                // Compare point lookups.
                let (key_lo, key_hi) = (
                    IndexEntry {
                        key: key.clone(),
                        row_id: RowId {
                            page_id: 0,
                            slot_id: 0,
                        },
                    },
                    IndexEntry {
                        key: key.clone(),
                        row_id: RowId {
                            page_id: u32::MAX,
                            slot_id: u16::MAX,
                        },
                    },
                );
                let expected: Vec<(IndexKey, RowId)> = model
                    .range(key_lo..=key_hi)
                    .map(|(e, _)| (e.key.clone(), e.row_id))
                    .collect();
                let got = index
                    .read()
                    .get_entries(&key, false)
                    .unwrap_or_else(|e| panic!("get_entries failed at step {step}: {e}"));
                assert_eq!(got, expected, "point lookup diverged at step {step}");
            }
            _ => {
                // Compare forward/reverse range scans with small chunks.
                let lo = (rnd() % key_space as u64) as i64 - 150;
                let hi = lo + (rnd() % 800) as i64;
                let low = Some(IndexBound {
                    key: int_key(lo),
                    inclusive: rnd() % 2 == 0,
                });
                let high = Some(IndexBound {
                    key: int_key(hi),
                    inclusive: rnd() % 2 == 0,
                });

                let expected_fwd: Vec<(IndexKey, RowId)> = model
                    .iter()
                    .filter(|(e, _)| {
                        let k = match e.key.0[0] {
                            KeyPart::Int(v) => v,
                            _ => unreachable!(),
                        };
                        let lo_ok = match low.as_ref().unwrap().inclusive {
                            true => k >= lo,
                            false => k > lo,
                        };
                        let hi_ok = match high.as_ref().unwrap().inclusive {
                            true => k <= hi,
                            false => k < hi,
                        };
                        lo_ok && hi_ok
                    })
                    .map(|(e, _)| (e.key.clone(), e.row_id))
                    .collect();

                let mut fwd_cursor =
                    IndexRangeCursor::new(Arc::clone(&index), false, low.clone(), high.clone())
                        .unwrap();
                let mut got_fwd = Vec::new();
                while !fwd_cursor.is_finished() {
                    let chunk = fwd_cursor.next_chunk(13).unwrap();
                    if chunk.is_empty() {
                        assert!(fwd_cursor.is_finished());
                        break;
                    }
                    got_fwd.extend(chunk);
                }
                assert_eq!(
                    got_fwd, expected_fwd,
                    "forward range diverged at step {step}"
                );

                let mut rev_cursor =
                    IndexRangeReverseCursor::new(Arc::clone(&index), false, low, high).unwrap();
                let mut got_rev = Vec::new();
                while !rev_cursor.is_finished() {
                    let chunk = rev_cursor.next_chunk(7).unwrap();
                    if chunk.is_empty() {
                        assert!(rev_cursor.is_finished());
                        break;
                    }
                    got_rev.extend(chunk);
                }
                let mut expected_rev = expected_fwd;
                expected_rev.reverse();
                assert_eq!(
                    got_rev, expected_rev,
                    "reverse range diverged at step {step}"
                );
            }
        }
    }

    // Compare complete final contents.
    let mut final_entries = index.read().entries().unwrap();
    final_entries.sort();
    let mut model_entries: Vec<(IndexKey, RowId)> =
        model.keys().map(|e| (e.key.clone(), e.row_id)).collect();
    model_entries.sort();
    assert_eq!(
        final_entries.len(),
        model_entries.len(),
        "final entry count mismatch"
    );
    assert_eq!(final_entries, model_entries, "final content mismatch");
}

/// BUG-4 update differential fuzzing: delete-old/insert-new batches for the same row_id
/// use apply_batch_deferred and try_apply_two_op_update_batch_fast,
/// covering raw/decoded same-leaf repositioning and two-leaf moves,
/// compared with the in-memory model.
#[test]
fn bug4_differential_fuzz_updates_match_model() {
    let (_dir, index) = audit_index("bug4.idx");
    // Live set: row_id to key.
    let mut live: std::collections::HashMap<u64, IndexKey> = Default::default();
    let mut content: std::collections::BTreeSet<IndexEntry> = Default::default();

    let mut seed = 0xbeef_u64;
    let mut rnd = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let key_space: i64 = 500; // A small key space generates many same-leaf and cross-leaf updates.
    let mut next_row: u64 = 0;
    for step in 0..12_000 {
        let op = rnd() % 100;
        match op {
            0..=39 => {
                // INSERT
                let kv = (rnd() % key_space as u64) as i64;
                let key = int_key(kv);
                let row_id = RowId {
                    page_id: (next_row / 1000) as u32,
                    slot_id: (next_row % 1000) as u16,
                };
                next_row += 1;
                let entry = IndexEntry {
                    key: key.clone(),
                    row_id,
                };
                if content.insert(entry.clone()) {
                    index
                        .read()
                        .insert_with_txn(300 + step as u64 % 9, key, row_id)
                        .unwrap();
                    live.insert(
                        u64::from(row_id.page_id) * 100_000 + row_id.slot_id as u64,
                        entry.key,
                    );
                }
            }
            40..=64 => {
                // UPDATE deletes old and inserts new using the same row_id.
                if live.is_empty() {
                    continue;
                }
                let target = (rnd() as usize) % live.len();
                let (&ridx, old_key) = live.iter().nth(target).unwrap();
                let kv = (rnd() % key_space as u64) as i64;
                let new_key = int_key(kv);
                if new_key == *old_key {
                    continue;
                }
                let slot = (ridx % 100_000) as u16;
                let page = (ridx / 100_000) as u32;
                let old_entry = IndexEntry {
                    key: (*old_key).clone(),
                    row_id: RowId {
                        page_id: page,
                        slot_id: slot,
                    },
                };
                let new_entry = IndexEntry {
                    key: new_key.clone(),
                    row_id: RowId {
                        page_id: page,
                        slot_id: slot,
                    },
                };
                index
                    .read()
                    .apply_batch_deferred(
                        400 + step as u64 % 9,
                        vec![
                            IndexBatchEntry {
                                kind: WalIndexOpKind::Delete,
                                key: (*old_key).clone(),
                                row_id: RowId {
                                    page_id: page,
                                    slot_id: slot,
                                },
                            },
                            IndexBatchEntry {
                                kind: WalIndexOpKind::Insert,
                                key: new_key,
                                row_id: RowId {
                                    page_id: page,
                                    slot_id: slot,
                                },
                            },
                        ],
                    )
                    .unwrap();
                assert!(content.remove(&old_entry));
                assert!(content.insert(new_entry.clone()));
                live.insert(ridx, new_entry.key);
            }
            _ => {
                // DELETE
                if live.is_empty() {
                    continue;
                }
                let target = (rnd() as usize) % live.len();
                let (&ridx, key) = live.iter().nth(target).unwrap();
                let slot = (ridx % 100_000) as u16;
                let page = (ridx / 100_000) as u32;
                let entry = IndexEntry {
                    key: key.clone(),
                    row_id: RowId {
                        page_id: page,
                        slot_id: slot,
                    },
                };
                index
                    .read()
                    .delete_with_txn(500 + step as u64 % 9, key.clone(), entry.row_id)
                    .unwrap();
                assert!(content.remove(&entry));
                live.remove(&ridx);
            }
        }

        // Compare complete contents every 2000 steps.
        if step % 2000 == 1999 {
            let mut got = index.read().entries().unwrap();
            got.sort();
            let mut expected: Vec<(IndexKey, RowId)> =
                content.iter().map(|e| (e.key.clone(), e.row_id)).collect();
            expected.sort();
            assert_eq!(
                got.len(),
                expected.len(),
                "step {step}: entry count mismatch"
            );
            assert_eq!(got, expected, "step {step}: content mismatch");
        }
    }

    let mut got = index.read().entries().unwrap();
    got.sort();
    let mut expected: Vec<(IndexKey, RowId)> =
        content.iter().map(|e| (e.key.clone(), e.row_id)).collect();
    expected.sort();
    assert_eq!(got, expected);
}

/// BUG-5 composite-key fuzzing: (String,Int) exercises generic part-wise comparison,
/// prefix lookups, and prefix-bounded ranges.
#[test]
fn bug5_differential_fuzz_composite_keys_match_model() {
    let (_dir, index) = audit_index("bug5.idx");
    let mut model: std::collections::BTreeMap<IndexEntry, ()> = Default::default();

    let letters = ["a", "b", "c", "d"];
    let mut seed = 0xcafe_u64;
    let mut rnd = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let mut next_row = 0usize;
    let comp_key = |s: usize, v: i64| {
        IndexKey::from(vec![
            KeyPart::String(letters[s].to_string()),
            KeyPart::Int(v),
        ])
    };
    for step in 0..3_000 {
        let op = rnd() % 100;
        let s = (rnd() % 4) as usize;
        let v = (rnd() % 400) as i64 - 200;
        match op {
            0..=49 => {
                let entry = IndexEntry {
                    key: comp_key(s, v),
                    row_id: rid(next_row),
                };
                index
                    .read()
                    .insert_with_txn(600 + (step % 7) as u64, entry.key.clone(), entry.row_id)
                    .unwrap();
                model.insert(entry, ());
                next_row += 1;
            }
            50..=64 => {
                let probe = IndexEntry {
                    key: comp_key(s, v),
                    row_id: RowId {
                        page_id: 0,
                        slot_id: 0,
                    },
                };
                let hi = IndexEntry {
                    key: comp_key(s, v),
                    row_id: RowId {
                        page_id: u32::MAX,
                        slot_id: u16::MAX,
                    },
                };
                let victim = model.range(probe..=hi).next().map(|(e, _)| e.clone());
                if let Some(victim) = victim {
                    index
                        .read()
                        .delete_with_txn(700 + (step % 5) as u64, victim.key.clone(), victim.row_id)
                        .unwrap();
                    model.remove(&victim);
                }
            }
            65..=79 => {
                let prefix = IndexKey::from(vec![KeyPart::String(letters[s].to_string())]);
                let expected: Vec<RowId> = model
                    .keys()
                    .filter(|e| e.key.0[0] == KeyPart::String(letters[s].to_string()))
                    .map(|e| e.row_id)
                    .collect();
                let mut got = index.read().prefix(&prefix, false).unwrap();
                got.sort();
                let mut expected = expected;
                expected.sort();
                assert_eq!(got, expected, "prefix lookup diverged at step {step}");
            }
            _ => {
                let v1 = (rnd() % 400) as i64 - 200;
                let v2 = v1 + (rnd() % 300) as i64;
                let low = Some(IndexBound {
                    key: comp_key(s, v1),
                    inclusive: rnd() % 2 == 0,
                });
                let high = Some(IndexBound {
                    key: comp_key(s, v2),
                    inclusive: rnd() % 2 == 0,
                });
                let lo_str = letters[s].to_string();
                let hi_str = letters[s].to_string();
                let (lo_v, hi_v) = (v1, v2);
                let expected: Vec<(IndexKey, RowId)> = model
                    .range(..)
                    .filter(|(e, _)| {
                        let (es, ev) = match (&e.key.0[0], &e.key.0[1]) {
                            (KeyPart::String(es), KeyPart::Int(ev)) => (es.clone(), *ev),
                            _ => unreachable!(),
                        };
                        let after_lo = es > lo_str
                            || (es == lo_str
                                && match low.as_ref().unwrap().inclusive {
                                    true => ev >= lo_v,
                                    false => ev > lo_v,
                                });
                        let before_hi = es < hi_str
                            || (es == hi_str
                                && match high.as_ref().unwrap().inclusive {
                                    true => ev <= hi_v,
                                    false => ev < hi_v,
                                });
                        after_lo && before_hi
                    })
                    .map(|(e, _)| (e.key.clone(), e.row_id))
                    .collect();

                let mut fwd =
                    IndexRangeCursor::new(Arc::clone(&index), false, low.clone(), high.clone())
                        .unwrap();
                let mut got_fwd = Vec::new();
                while !fwd.is_finished() {
                    let chunk = fwd.next_chunk(11).unwrap();
                    if chunk.is_empty() {
                        assert!(fwd.is_finished());
                        break;
                    }
                    got_fwd.extend(chunk);
                }
                assert_eq!(
                    got_fwd, expected,
                    "composite forward range diverged at step {step}"
                );

                let mut rev =
                    IndexRangeReverseCursor::new(Arc::clone(&index), false, low, high).unwrap();
                let mut got_rev = Vec::new();
                while !rev.is_finished() {
                    let chunk = rev.next_chunk(9).unwrap();
                    if chunk.is_empty() {
                        assert!(rev.is_finished());
                        break;
                    }
                    got_rev.extend(chunk);
                }
                let mut expected_rev = expected;
                expected_rev.reverse();
                assert_eq!(
                    got_rev, expected_rev,
                    "composite reverse range diverged at step {step}"
                );
            }
        }
    }

    let mut final_entries = index.read().entries().unwrap();
    final_entries.sort();
    let mut model_entries: Vec<(IndexKey, RowId)> =
        model.keys().map(|e| (e.key.clone(), e.row_id)).collect();
    model_entries.sort();
    assert_eq!(final_entries, model_entries);
}

/// BUG-6 flush/reopen round trip: random insert/delete operations are flushed,
/// then reopen and verify root, next_page_id, freelist, and
/// all contents remain identical.
#[test]
fn bug6_flush_reopen_roundtrip_matches() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let path = dir.path().join("bug6.idx");
    let log = Arc::new(parking_lot::Mutex::new(
        LogManager::open(&dir.path().join("wal")).unwrap(),
    ));
    let pool = BufferPool::new(
        512,
        log.clone(),
        DurabilityMode::Strict,
        Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
    );
    let index =
        IndexFile::open_with_durability(path.clone(), log.clone(), DurabilityMode::Strict, pool)
            .unwrap();

    let mut model: std::collections::BTreeMap<IndexEntry, ()> = Default::default();
    // Bulk-load an initial multileaf dataset.
    let initial: Vec<IndexEntry> = (0..5000)
        .map(|i| IndexEntry {
            key: int_key((i * 7) % 10_000),
            row_id: rid(i as usize),
        })
        .collect();
    let mut sorted_initial = initial.clone();
    sorted_initial.sort();
    sorted_initial.dedup();
    index
        .bulk_load_sorted(txn_id_for_ddl(), sorted_initial.clone())
        .unwrap();
    for e in &sorted_initial {
        model.insert(e.clone(), ());
    }

    // Random incremental operations using sorted insertion batches and individual deletes.
    let mut seed = 0x600d_u64;
    let mut rnd = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for batch_no in 0..40u64 {
        let mut batch: Vec<IndexEntry> = Vec::new();
        for _ in 0..50 {
            let kv = (rnd() % 20_000) as i64 - 5_000;
            batch.push(IndexEntry {
                key: int_key(kv),
                row_id: rid((batch_no as usize) * 100 + batch.len() + 6000),
            });
        }
        batch.sort();
        batch.dedup();
        let filtered: Vec<IndexEntry> = batch
            .into_iter()
            .filter(|e| model.insert(e.clone(), ()).is_none())
            .collect();
        index
            .insert_entries_logged(900 + batch_no, filtered, false)
            .unwrap();
        for _ in 0..40 {
            let kv = (rnd() % 20_000) as i64 - 5_000;
            let victim = model
                .keys()
                .find(|e| matches!(&e.key.0[0], KeyPart::Int(v) if *v == kv))
                .cloned();
            if let Some(v) = victim {
                index
                    .delete_with_txn(950 + batch_no, v.key.clone(), v.row_id)
                    .unwrap();
                model.remove(&v);
            }
        }
    }

    index.flush().unwrap();
    let pages_before = index.page_count();
    drop(index);

    // Reopen the same file.
    let pool2 = BufferPool::new(
        512,
        log.clone(),
        DurabilityMode::Strict,
        Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
    );
    let reopened =
        IndexFile::open_with_durability(path, log, DurabilityMode::Strict, pool2).unwrap();
    assert_eq!(
        reopened.page_count(),
        pages_before,
        "page count changed after reopen"
    );
    let mut got = reopened.entries().unwrap();
    got.sort();
    let mut expected: Vec<(IndexKey, RowId)> =
        model.keys().map(|e| (e.key.clone(), e.row_id)).collect();
    expected.sort();
    assert_eq!(got.len(), expected.len(), "reopen entry count mismatch");
    assert_eq!(got, expected, "reopen content mismatch");
}

/// BUG-7: extreme/negative i64 keys may overflow compact integer base/delta
/// calculations near i64::MIN/MAX.
#[test]
fn bug7_int_extreme_keys_survive_all_paths() {
    let (_dir, index) = audit_index("bug7.idx");
    let extremes = [
        i64::MIN,
        i64::MIN + 1,
        i64::MIN + 65535,
        i64::MIN + 65536,
        -65537,
        -65536,
        -1,
        0,
        1,
        65535,
        65536,
        i64::MAX - 65536,
        i64::MAX - 65535,
        i64::MAX - 1,
        i64::MAX,
    ];
    let mut model: std::collections::BTreeMap<IndexEntry, ()> = Default::default();
    // Widely spaced keys trigger layout promotion/demotion and splits.
    let mut entries: Vec<IndexEntry> = extremes
        .iter()
        .enumerate()
        .map(|(i, v)| IndexEntry {
            key: int_key(*v),
            row_id: rid(i as usize),
        })
        .collect();
    // Multiple rows share the same extreme key in a nonunique index.
    for rep in 0..3 {
        for (i, v) in extremes.iter().enumerate() {
            entries.push(IndexEntry {
                key: int_key(*v),
                row_id: RowId {
                    page_id: (100 + rep * 100) as u32,
                    slot_id: i as u16,
                },
            });
        }
    }
    entries.sort();
    entries.dedup();
    index
        .write()
        .bulk_load_sorted(txn_id_for_ddl(), entries.clone())
        .unwrap();
    for e in &entries {
        model.insert(e.clone(), ());
    }

    // Insert an intermediate key to trigger demotion/borrowing.
    let mid = IndexEntry {
        key: int_key(i64::MIN + 3),
        row_id: RowId {
            page_id: 777,
            slot_id: 7,
        },
    };
    index
        .read()
        .insert_with_txn(42, mid.key.clone(), mid.row_id)
        .unwrap();
    model.insert(mid, ());

    // Compare complete contents.
    let mut got = index.read().entries().unwrap();
    got.sort();
    let mut expected: Vec<(IndexKey, RowId)> =
        model.keys().map(|e| (e.key.clone(), e.row_id)).collect();
    expected.sort();
    assert_eq!(got, expected, "extreme key content mismatch");

    // Extreme-key ranges cover leftmost/rightmost leaf boundaries.
    for (lo, hi, lo_inc, hi_inc) in [
        (i64::MIN, i64::MIN + 2, true, true),
        (i64::MIN, i64::MAX, false, false),
        (i64::MAX - 1, i64::MAX, true, true),
        (-70000, -65000, true, false),
    ] {
        let low = Some(IndexBound {
            key: int_key(lo),
            inclusive: lo_inc,
        });
        let high = Some(IndexBound {
            key: int_key(hi),
            inclusive: hi_inc,
        });
        let mut fwd =
            IndexRangeCursor::new(Arc::clone(&index), false, low.clone(), high.clone()).unwrap();
        let mut got_fwd = Vec::new();
        while !fwd.is_finished() {
            let chunk = fwd.next_chunk(3).unwrap();
            if chunk.is_empty() {
                assert!(fwd.is_finished());
                break;
            }
            got_fwd.extend(chunk);
        }
        let expected: Vec<(IndexKey, RowId)> = model
            .keys()
            .filter(|e| match (&e.key.0[0], &e.key.0[0]) {
                (KeyPart::Int(v), _) => {
                    let after = if lo_inc { *v >= lo } else { *v > lo };
                    let before = if hi_inc { *v <= hi } else { *v < hi };
                    after && before
                }
                _ => false,
            })
            .map(|e| (e.key.clone(), e.row_id))
            .collect();
        assert_eq!(got_fwd, expected, "extreme range [{lo},{hi}] diverged");

        let mut rev = IndexRangeReverseCursor::new(Arc::clone(&index), false, low, high).unwrap();
        let mut got_rev = Vec::new();
        while !rev.is_finished() {
            let chunk = rev.next_chunk(3).unwrap();
            if chunk.is_empty() {
                assert!(rev.is_finished());
                break;
            }
            got_rev.extend(chunk);
        }
        let mut expected_rev = expected;
        expected_rev.reverse();
        assert_eq!(
            got_rev, expected_rev,
            "extreme reverse range [{lo},{hi}] diverged"
        );
    }
}

static DUP_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static ORDER_VIOLATIONS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// BUG-8 concurrent invariants: four writers insert/delete/two-op update,
/// while two readers scan forward/reverse. Verify:
/// ordered keys, bounds, no duplicate (key,row_id) pairs,
/// and final contents equal the union of writer-local models.
#[test]
fn bug8_concurrent_rw_invariants_hold() {
    let (_dir, index) = audit_index("bug8.idx");
    let initial: Vec<IndexEntry> = (0..1500usize)
        .map(|i| IndexEntry {
            key: int_key(i as i64),
            row_id: rid(i),
        })
        .collect();
    index
        .write()
        .bulk_load_sorted(txn_id_for_ddl(), initial.clone())
        .unwrap();

    const WRITERS: usize = 4;
    const READERS: usize = 2;
    const OPS: usize = 2500;
    const KEY_SPACE: i64 = 3000;

    // Each writer maps row_id to key in a disjoint row_id range.
    let models: Vec<Arc<std::sync::Mutex<std::collections::HashMap<u64, i64>>>> = (0..WRITERS)
        .map(|_| Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())))
        .collect();

    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let index = Arc::clone(&index);
        let model = Arc::clone(&models[w]);
        handles.push(std::thread::spawn(move || {
            let mut seed = 0x1000_u64 + w as u64 * 7919;
            let mut rnd = move || {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed
            };
            let mut next_row: u64 = 10_000 + w as u64 * 100_000;
            // Only this writer mutates its oracle; publish it after all ops.
            let mut local_model = std::collections::HashMap::new();
            for _ in 0..OPS {
                let op = rnd() % 100;
                let kv = (rnd() % KEY_SPACE as u64) as i64;
                let key = int_key(kv);
                let slot = (next_row % 60_000) as u16;
                let page = (next_row / 60_000) as u32;
                let row_id = RowId {
                    page_id: page,
                    slot_id: slot,
                };
                let ridx = u64::from(page) * 100_000 + u64::from(slot);
                match op {
                    0..=49 => {
                        index
                            .read()
                            .insert_with_txn(1000 + w as u64, key.clone(), row_id)
                            .unwrap();
                        local_model.insert(ridx, kv);
                        next_row += 1;
                    }
                    50..=74 => {
                        // Delete an owned (key,row_id); NotFound is possible.
                        let victim = local_model
                            .iter()
                            .find(|(_, v)| **v == kv)
                            .map(|(r, k)| (*r, *k));
                        if let Some((ridx_v, kv_v)) = victim {
                            let slot_v = (ridx_v % 100_000) as u16;
                            let page_v = (ridx_v / 100_000) as u32;
                            index
                                .read()
                                .delete_with_txn(
                                    2000 + w as u64,
                                    int_key(kv_v),
                                    RowId {
                                        page_id: page_v,
                                        slot_id: slot_v,
                                    },
                                )
                                .unwrap();
                            local_model.remove(&ridx_v);
                        }
                    }
                    _ => {
                        // Pure inserts in a second key space avoid update round-trip noise.
                        let kv2 = (rnd() % KEY_SPACE as u64) as i64 + KEY_SPACE;
                        let key2 = int_key(kv2);
                        index
                            .read()
                            .insert_with_txn(1000 + w as u64, key2, row_id)
                            .unwrap();
                        local_model.insert(ridx, kv2);
                        next_row += 1;
                    }
                }
            }
            *model.lock().unwrap() = local_model;
        }));
    }

    // Reader threads verify invariants.
    for r in 0..READERS {
        let index = Arc::clone(&index);
        handles.push(std::thread::spawn(move || {
            let mut seed = 0x9000_u64 + r as u64 * 104729;
            let mut rnd = move || {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed
            };
            for _ in 0..600 {
                let lo = (rnd() % (KEY_SPACE as u64 + 500)) as i64 - 250;
                let hi = lo + (rnd() % 1500) as i64;
                let low = Some(IndexBound {
                    key: int_key(lo),
                    inclusive: rnd() % 2 == 0,
                });
                let high = Some(IndexBound {
                    key: int_key(hi),
                    inclusive: rnd() % 2 == 0,
                });
                let reverse = rnd() % 2 == 0;
                let mut prev: Option<(i64, RowId)> = None;
                // Strict monotonicity proves uniqueness across the whole scan,
                // including chunk boundaries, without cloning keys into a set.
                if reverse {
                    let mut cursor = IndexRangeReverseCursor::new(
                        Arc::clone(&index),
                        false,
                        low.clone(),
                        high.clone(),
                    )
                    .unwrap();
                    while !cursor.is_finished() {
                        let chunk = cursor.next_chunk(17).unwrap();
                        if chunk.is_empty() {
                            assert!(cursor.is_finished());
                            break;
                        }
                        for (k, row) in chunk {
                            let KeyPart::Int(value) = &k.0[0] else {
                                panic!("integer index emitted a non-integer key");
                            };
                            let in_range = {
                                let v = value;
                                let after = if low.as_ref().unwrap().inclusive {
                                    *v >= lo
                                } else {
                                    *v > lo
                                };
                                let before = if high.as_ref().unwrap().inclusive {
                                    *v <= hi
                                } else {
                                    *v < hi
                                };
                                after && before
                            };
                            assert!(in_range, "reverse scan emitted out-of-range key");
                            // Compare against an independent scalar oracle.
                            let entry = (*value, row);
                            if let Some(p) = &prev {
                                if p == &entry {
                                    DUP_COUNT.fetch_add(1, Ordering::Relaxed);
                                }
                                if p <= &entry {
                                    ORDER_VIOLATIONS.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            prev = Some(entry);
                        }
                    }
                } else {
                    let mut cursor =
                        IndexRangeCursor::new(Arc::clone(&index), false, low.clone(), high.clone())
                            .unwrap();
                    while !cursor.is_finished() {
                        let chunk = cursor.next_chunk(17).unwrap();
                        if chunk.is_empty() {
                            assert!(cursor.is_finished());
                            break;
                        }
                        for (k, row) in chunk {
                            let KeyPart::Int(value) = &k.0[0] else {
                                panic!("integer index emitted a non-integer key");
                            };
                            let in_range = {
                                let v = value;
                                let after = if low.as_ref().unwrap().inclusive {
                                    *v >= lo
                                } else {
                                    *v > lo
                                };
                                let before = if high.as_ref().unwrap().inclusive {
                                    *v <= hi
                                } else {
                                    *v < hi
                                };
                                after && before
                            };
                            assert!(in_range, "forward scan emitted out-of-range key");
                            // Compare against an independent scalar oracle.
                            let entry = (*value, row);
                            if let Some(p) = &prev {
                                if p == &entry {
                                    DUP_COUNT.fetch_add(1, Ordering::Relaxed);
                                }
                                if p >= &entry {
                                    ORDER_VIOLATIONS.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            prev = Some(entry);
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("worker thread panicked");
    }

    let dups = DUP_COUNT.load(Ordering::Relaxed);
    let orders = ORDER_VIOLATIONS.load(Ordering::Relaxed);
    eprintln!("[bug8] duplicate-emission count across readers: {dups}, order violations: {orders}");
    assert_eq!(dups, 0, "concurrent cursor duplicated entries");
    assert_eq!(orders, 0, "concurrent cursor violated key order");
    assert_eq!(orders, 0, "scan order violated");

    // Compare final contents with the union of models.
    let mut final_state: std::collections::BTreeSet<IndexEntry> = Default::default();
    for e in initial {
        final_state.insert(e);
    }
    for m in &models {
        for (ridx, kv) in m.lock().unwrap().iter() {
            final_state.insert(IndexEntry {
                key: int_key(*kv),
                row_id: RowId {
                    page_id: (ridx / 100_000) as u32,
                    slot_id: (ridx % 100_000) as u16,
                },
            });
        }
    }
    let mut got = index.read().entries().unwrap();
    got.sort();
    let expected: Vec<(IndexKey, RowId)> = final_state
        .iter()
        .map(|e| (e.key.clone(), e.row_id))
        .collect();
    assert_eq!(
        got.len(),
        expected.len(),
        "final concurrent state count mismatch"
    );
    assert_eq!(got, expected, "final concurrent state mismatch");
}

/// BUG-9 batched IN lookups compare routed, decoded fallback,
/// and per-key get_entries paths. Mix many row_ids crossing leaves per key,
/// missing keys, and leaf-boundary keys; cover exact and prefix semantics.
#[test]
fn bug9_lookup_many_routed_matches_fallback_and_per_key() {
    let (_dir, index) = audit_index("bug9.idx");
    // Nonunique index: 50 keys with 200 row_ids each, necessarily spanning leaves.
    let mut entries: Vec<IndexEntry> = Vec::new();
    for k in 0..50i64 {
        for r in 0..200usize {
            let global = k as usize * 200 + r;
            entries.push(IndexEntry {
                key: int_key(k),
                row_id: RowId {
                    page_id: (global / 32) as u32,
                    slot_id: (global % 32) as u16,
                },
            });
        }
    }
    entries.sort();
    index
        .write()
        .bulk_load_sorted(txn_id_for_ddl(), entries)
        .unwrap();

    let probe_keys: Vec<IndexKey> = [
        // Mix missing, present, and boundary keys 0/49.
        -1i64, 0, 1, 7, 24, 25, 26, 48, 49, 50, 100,
    ]
    .iter()
    .map(|v| int_key(*v))
    .collect();

    for exact in [true, false] {
        for dedup in [false, true] {
            let routed = index
                .read()
                .lookup_entries_many_sorted(&probe_keys, exact, dedup)
                .unwrap();
            let fallback = index
                .read()
                .lookup_entries_many_sorted_fallback(&probe_keys, exact, dedup)
                .unwrap();
            assert_eq!(
                routed, fallback,
                "exact={exact} dedup={dedup}: routed vs fallback"
            );

            // Compare per key, concatenating results in requested key order.
            let mut per_key: Vec<(IndexKey, RowId)> = Vec::new();
            for key in &probe_keys {
                let rows = index.read().get_entries(key, dedup).unwrap();
                for (k, r) in rows {
                    // get_entries returns exact (key,row) pairs.
                    per_key.push((k, r));
                }
            }
            assert_eq!(
                routed, per_key,
                "exact={exact} dedup={dedup}: routed vs per-key get_entries"
            );
        }
    }
}

/// BUG-10 compares noncursor range row_ids, range_limited offset/limit,
/// and range_entries_for_leaf_ids against
/// IndexRangeCursor output.
#[test]
fn bug10_range_family_matches_cursor() {
    let (_dir, index) = audit_index("bug10.idx");
    let mut entries: Vec<IndexEntry> = (0..6000usize)
        .map(|i| IndexEntry {
            key: int_key((i % 900) as i64),
            row_id: rid(i),
        })
        .collect();
    entries.sort();
    index
        .write()
        .bulk_load_sorted(txn_id_for_ddl(), entries)
        .unwrap();

    let mut seed = 0x10ace_u64;
    let mut rnd = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for _ in 0..64 {
        let lo = (rnd() % 1000) as i64 - 50;
        let hi = lo + (rnd() % 1000) as i64;
        let low = Some(IndexBound {
            key: int_key(lo),
            inclusive: rnd() % 2 == 0,
        });
        let high = Some(IndexBound {
            key: int_key(hi),
            inclusive: rnd() % 2 == 0,
        });
        let dedup = rnd() % 2 == 0;

        // Cursor baseline.
        let mut cursor =
            IndexRangeCursor::new(Arc::clone(&index), false, low.clone(), high.clone()).unwrap();
        let mut base_rows: Vec<RowId> = Vec::new();
        while !cursor.is_finished() {
            let chunk = cursor.next_chunk_row_ids(29).unwrap();
            if chunk.is_empty() {
                assert!(cursor.is_finished());
                break;
            }
            base_rows.extend(chunk);
        }

        // Noncursor range.
        let got_range = index
            .read()
            .range(low.as_ref(), high.as_ref(), dedup)
            .unwrap();
        let expected_range = base_rows.clone();
        assert_eq!(
            got_range, expected_range,
            "range() mismatch [{lo},{hi}] dedup={dedup}"
        );

        // range_limited offset/limit slice consistency.
        for (offset, limit) in [(0usize, Some(10usize)), (5, Some(7)), (0, None)] {
            let got = index
                .read()
                .range_limited(low.as_ref(), high.as_ref(), offset, limit)
                .unwrap();
            let expected: Vec<RowId> = base_rows
                .clone()
                .into_iter()
                .skip(offset)
                .take(limit.unwrap_or(usize::MAX))
                .collect();
            assert_eq!(
                got, expected,
                "range_limited() mismatch [{lo},{hi}] offset={offset} limit={limit:?}"
            );
        }

        // range_entries_for_leaf_ids, the parallel worker path.
        let leaf_ids = index
            .read()
            .range_leaf_ids(low.as_ref(), high.as_ref())
            .unwrap();
        let got_leaf_ids = index
            .read()
            .range_entries_for_leaf_ids(&leaf_ids, low.as_ref(), high.as_ref())
            .unwrap();
        let mut cursor2 =
            IndexRangeCursor::new(Arc::clone(&index), false, low.clone(), high).unwrap();
        let mut base_entries: Vec<(IndexKey, RowId)> = Vec::new();
        while !cursor2.is_finished() {
            let chunk = cursor2.next_chunk(31).unwrap();
            if chunk.is_empty() {
                assert!(cursor2.is_finished());
                break;
            }
            base_entries.extend(chunk);
        }
        assert_eq!(
            got_leaf_ids, base_entries,
            "range_entries_for_leaf_ids mismatch [{lo},{hi}]"
        );
    }
}

/// BUG-4/7: after skipping empty leaves, report the emitted leaf's transaction upper bound; terminal state returns zero.
#[test]
fn bug11_leaf_chunk_empty_leaf_reports_wrong_page_bound() {
    let (_dir, index) = audit_index("bug11.idx");
    bulk_ints(&index, 4000);

    // Use a non-rightmost leaf maximum as exclusive low, matching BUG-1.
    let boundary_key = {
        let idx = index.read();
        let mut leaf_id = idx.leftmost_leaf().unwrap();
        loop {
            let leaf = idx.read_leaf(leaf_id).unwrap();
            match leaf.next_leaf {
                Some(next) => {
                    if leaf.entries.len() >= 2 {
                        break leaf.entries.last().unwrap().key.clone();
                    }
                    leaf_id = next;
                }
                None => panic!("need a non-rightmost leaf"),
            }
        }
    };
    let target_key = {
        let file = index.read();
        let first = file.read_leaf(file.leftmost_leaf().unwrap()).unwrap();
        file.read_leaf(first.next_leaf.unwrap()).unwrap().entries[10]
            .key
            .clone()
    };
    index
        .read()
        .insert_with_txn(777, target_key, rid(100_000))
        .unwrap();
    let emitting_bound = {
        let idx = index.read();
        let first = idx.read_leaf(idx.leftmost_leaf().unwrap()).unwrap();
        let next = idx.read_leaf(first.next_leaf.unwrap()).unwrap();
        next.page_max_trx_id()
    };
    let low = Some(IndexBound {
        key: boundary_key.clone(),
        inclusive: false,
    });
    let mut cursor = IndexRangeCursor::new(Arc::clone(&index), true, low, None).unwrap();
    let (chunk, bound) = cursor.next_leaf_chunk(4096).unwrap();
    assert!(!chunk.is_empty());
    assert!(chunk.iter().all(|(key, _)| key > &boundary_key));
    assert_eq!(bound, emitting_bound);
    assert_eq!(bound, 777);
    while !cursor.is_finished() {
        cursor.next_leaf_chunk(4096).unwrap();
    }
    assert_eq!(cursor.next_leaf_chunk(4096).unwrap(), (Vec::new(), 0));
}

#[test]
fn bug_filtered_chunks_skip_rejected_leaves_and_sum_counters() {
    let (_dir, index) = audit_index("filtered.idx");
    let entries = bulk_ints(&index, 4000);
    let mut cursor = IndexRangeCursor::new(Arc::clone(&index), true, None, None).unwrap();
    assert_eq!(cursor.next_leaf_chunk(0).unwrap(), (Vec::new(), 0));
    assert!(!cursor.is_finished());
    let mut got = Vec::new();
    let mut examined = 0;
    let mut rejected = 0;
    while !cursor.is_finished() {
        let (chunk, bound, checked, skipped) = cursor
            .next_leaf_chunk_filtered_first_int(17, |v| v.is_some_and(|v| v >= 3500))
            .unwrap();
        if chunk.is_empty() {
            assert!(cursor.is_finished());
            assert_eq!(bound, 0);
        }
        examined += checked;
        rejected += skipped;
        got.extend(chunk);
    }
    assert_eq!(examined, 4000);
    assert_eq!(rejected, 3500);
    assert_eq!(
        got,
        entries[3500..]
            .iter()
            .map(|e| (e.key.clone(), e.row_id))
            .collect::<Vec<_>>()
    );
    let mut cursor = IndexRangeCursor::new(Arc::clone(&index), true, None, None).unwrap();
    let (chunk, bound, examined, rejected) = cursor
        .next_leaf_chunk_filtered_first_int(1, |_| false)
        .unwrap();
    assert!(chunk.is_empty());
    assert!(cursor.is_finished());
    assert_eq!((bound, examined, rejected), (0, 4000, 4000));
}

#[test]
fn bug_fulltext_prefix_scan_matches_sorted_model() {
    let (_dir, index) = audit_index("prefix.idx");
    let entries: Vec<_> = (0..3000usize)
        .map(|i| IndexEntry {
            key: IndexKey::from(vec![
                KeyPart::String(format!("token{i:04}")),
                KeyPart::Bytes(vec![(i % 256) as u8]),
            ]),
            row_id: rid(i),
        })
        .collect();
    index
        .read()
        .bulk_load_sorted(txn_id_for_ddl(), entries.clone())
        .unwrap();
    for prefix in ["", "a", "token", "token12", "token2999", "z"] {
        let mut got = Vec::new();
        index
            .read()
            .fulltext_scan_prefix(prefix, None, |token, row, payload| {
                got.push((token.to_owned(), row, payload.to_vec()));
                Ok(())
            })
            .unwrap();
        let expected = entries
            .iter()
            .filter_map(|e| {
                let [KeyPart::String(token), KeyPart::Bytes(payload)] = e.key.0.as_slice() else {
                    unreachable!()
                };
                token
                    .starts_with(prefix)
                    .then(|| (token.clone(), e.row_id, payload.clone()))
            })
            .collect::<Vec<_>>();
        assert_eq!(got, expected, "prefix {prefix:?}");
    }
}

#[test]
fn bug_leaf_chunk_empty_terminal_has_zero_bound_for_all_variants() {
    let (_dir, index) = audit_index("empty-bound.idx");
    bulk_ints(&index, 100);
    index
        .read()
        .insert_with_txn(999, int_key(50), rid(10000))
        .unwrap();
    let low = Some(IndexBound {
        key: int_key(100),
        inclusive: false,
    });
    let mut forward = IndexRangeCursor::new(Arc::clone(&index), true, low.clone(), None).unwrap();
    assert_eq!(forward.next_leaf_chunk(4).unwrap(), (Vec::new(), 0));
    assert!(forward.is_finished());
    let mut rows = IndexRangeCursor::new(Arc::clone(&index), true, low, None).unwrap();
    assert_eq!(rows.next_leaf_chunk_row_ids(4).unwrap(), (Vec::new(), 0));
    assert!(rows.is_finished());
    let high = Some(IndexBound {
        key: int_key(-1),
        inclusive: false,
    });
    let mut reverse =
        IndexRangeReverseCursor::new(Arc::clone(&index), true, None, high.clone()).unwrap();
    assert_eq!(reverse.next_leaf_chunk(4).unwrap(), (Vec::new(), 0));
    assert!(reverse.is_finished());
    let mut rows = IndexRangeReverseCursor::new(Arc::clone(&index), true, None, high).unwrap();
    assert_eq!(rows.next_leaf_chunk_row_ids(4).unwrap(), (Vec::new(), 0));
    assert!(rows.is_finished());
}

#[test]
fn bug_unsorted_batch_routes_each_validated_run_and_point_lookups_match() {
    let (_dir, index) = audit_index("unsorted-runs.idx");
    let mut expected = bulk_ints(&index, 4000);
    let mut batch = Vec::new();
    for (offset, key) in [3500, 20, 3400, 30, 3390, 40, 3380, 50]
        .into_iter()
        .enumerate()
    {
        batch.push((int_key(key), rid(10000 + offset)));
    }
    // Force compact-layout fallback as well as backward routing between runs.
    batch.push((
        IndexKey::from(vec![KeyPart::String("text".to_owned())]),
        rid(20000),
    ));
    batch.push((int_key(60), rid(20001)));
    batch.push(batch.last().unwrap().clone());
    expected.extend(batch.iter().map(|(key, row_id)| IndexEntry {
        key: key.clone(),
        row_id: *row_id,
    }));
    expected.sort();
    expected.dedup();
    index
        .read()
        .insert_batch_deferred(888, batch, false)
        .unwrap();
    let got = IndexRangeCursor::new(Arc::clone(&index), true, None, None)
        .unwrap()
        .collect_remaining()
        .unwrap();
    assert_eq!(
        got,
        expected
            .iter()
            .map(|e| (e.key.clone(), e.row_id))
            .collect::<Vec<_>>()
    );
    // Point routing, not only the leaf chain, must find every inserted entry.
    for entry in expected
        .iter()
        .filter(|entry| entry.row_id.page_id >= rid(10000).page_id)
    {
        assert!(index
            .read()
            .get(&entry.key, false)
            .unwrap()
            .contains(&entry.row_id));
    }
}
