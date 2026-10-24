use super::*;

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
