/// Generate valid schemas/rows before mutating their physical representation.
/// Full and projected decoders must agree, including retained layouts and LOBs.
pub(crate) fn fuzz_schema_rows(bytes: &[u8]) {
    let mut input = crate::fuzzing::FuzzBytes::new(bytes);
    let mut columns = Vec::new();
    let mut values = Vec::new();
    for _ in 0..1 + input.next() as usize % 24 {
        let tag = input.next();
        let n = input.u64();
        let (kind, value) = match tag % 21 {
            0 => (ColumnKind::Int { width: 8, signed: true }, Value::Int(n as i64)),
            1 => (ColumnKind::Int { width: 8, signed: false }, Value::UInt(n)),
            2 => (ColumnKind::Float32, Value::Float(f32::from_bits(n as u32))),
            3 => (ColumnKind::Float64, Value::Double(f64::from_bits(n))),
            4 => (ColumnKind::DecimalFixed { width: 16, scale: 8 }, Value::Decimal128(n as i128 - i64::MAX as i128, 8)),
            5 => (ColumnKind::DecimalWide { scale: 30 }, Value::Decimal(format!("{n}.{:030}", n))),
            6 => (ColumnKind::VarString, Value::String(format!("界\0'{n} "))),
            7 => (ColumnKind::VarBytes, Value::Bytes(n.to_le_bytes().repeat(input.next() as usize % 32))),
            8 => (ColumnKind::Date, Value::Date((n % 100_000) as i32)),
            9 => (ColumnKind::Time { fsp: 6 }, Value::Time(n as i64 % 3_000_000_000_000)),
            10 => (ColumnKind::DateTime { fsp: 6 }, Value::DateTime(n as i64 % 2_000_000_000_000_000)),
            11 => (ColumnKind::Timestamp { fsp: 6 }, Value::Timestamp((n % 4_102_444_800_000_000) as i64)),
            12 => (ColumnKind::Year, Value::Year((n % 255) as u8)),
            13 => (ColumnKind::Bit { width: 8 }, Value::UInt(n)),
            14 => (ColumnKind::Enum { values: vec!["a".into(), "中".into()], width: 1 }, Value::String(if n & 1 == 0 { "a" } else { "中" }.into())),
            15 => (ColumnKind::Set { values: vec!["a".into(), "中".into()], width: 1 }, Value::String(if n & 1 == 0 { "a,中" } else { "中" }.into())),
            16 => (ColumnKind::FixedBytes { width: 8 }, Value::Bytes(n.to_le_bytes().to_vec())),
            17 => (ColumnKind::Json, Value::Json(crate::engine::json::parse_text(&format!("{{\"n\":{n},\"a\":[null,true,\"界\"]}}"), 1, "fuzz").unwrap())),
            18 => (ColumnKind::Geometry, Value::Geometry({ let mut wkb = vec![0,0,0,0,1,1,0,0,0]; wkb.extend_from_slice(&((n % 1000) as f64).to_le_bytes()); wkb.extend_from_slice(&((n % 999) as f64).to_le_bytes()); wkb })),
            19 => (ColumnKind::VectorFixed { dim: 2, datatype: crate::vector::VectorDataType::Float32 }, Value::Vector([((n % 100) as f32).to_le_bytes(), (-0.0f32).to_le_bytes()].concat().into())),
            _ => (ColumnKind::Vector { dim: None, datatype: crate::vector::VectorDataType::Float32 }, Value::Vector([((n % 100) as f32).to_le_bytes(), (1.0f32).to_le_bytes()].concat().into())),
        };
        columns.push(ColumnCodec { kind });
        values.push(if tag & 0x80 != 0 { Value::Null } else { value });
    }
    let mut codec = RowCodec {
        layouts: vec![identity_layout_for_columns(&columns, 1)],
        defaults: vec![Value::Null; columns.len()],
        active_layout_id: 1,
        columns,
    };
    let header = RowHeader { flags: 0, last_txn_id: input.u64(), undo_ptr: Some(input.u64()) };
    let encoded = encode_mvcc_row(header, &values, &codec, &[]).unwrap();
    let (_, full) = decode_mvcc_row(&encoded, &codec).unwrap();
    // Decimal storage representations may differ; numeric-vs-text and every
    // non-decimal type must remain distinct, including IEEE signed zero bits.
    let assert_values = |actual: &[Value], expected: &[Value]| {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            let decimal = |v: &Value| matches!(v, Value::Decimal(_) | Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(_));
            if decimal(expected) {
                assert!(decimal(actual), "decimal decoded as {actual:?}");
                assert_eq!(actual.to_display_string(), expected.to_display_string());
            } else {
                assert!(actual.storage_eq(expected), "typed mismatch: {actual:?} vs {expected:?}");
            }
        }
    };
    assert_values(&full, &values);
    let selected = (0..values.len()).rev().filter(|i| i % 2 == 0).chain([0, values.len() - 1, 0]).collect::<Vec<_>>();
    let (_, projected, _) = decode_mvcc_row_columns(&encoded, &selected, &codec).unwrap();
    assert_values(&projected, &selected.iter().map(|&i| full[i].clone()).collect::<Vec<_>>());

    codec.columns.push(ColumnCodec { kind: ColumnKind::VarString });
    codec.defaults.push(Value::String("added default".into()));
    codec.active_layout_id = 2;
    codec.layouts.push(identity_layout_for_columns(&codec.columns, 2));
    let (_, retained) = decode_mvcc_row(&encoded, &codec).unwrap();
    let mut expected = full.clone();
    expected.push(Value::String("added default".into()));
    assert_values(&retained, &expected);
    // A third physical layout drops slot zero and reverses all other slots.
    codec.active_layout_id = 3;
    codec.layouts.push(RowLayoutCodec::new(3, (1..codec.columns.len()).rev().map(|column_index| RowLayoutColumnCodec {
        column_index, kind: codec.columns[column_index].kind.clone(), nullable: true,
    }).collect()));
    let new_encoded = encode_mvcc_row(header, &expected, &codec, &[]).unwrap();
    expected[0] = Value::Null;
    assert_values(&decode_mvcc_row(&new_encoded, &codec).unwrap().1, &expected);
    let mut old_expected = full;
    old_expected.push(Value::String("added default".into()));
    assert_values(&decode_mvcc_row(&encoded, &codec).unwrap().1, &old_expected);
    // All decoder variants must either reject malformed bytes or agree.
    let mut mutated = encoded.clone();
    if !bytes.is_empty() {
        let at = input.u64() as usize % mutated.len();
        mutated[at] ^= input.next().max(1);
    }
    for raw in [&mutated[..], &encoded[..input.next() as usize % encoded.len()], bytes] {
        let decoded = decode_mvcc_row(raw, &codec);
        let projected = decode_mvcc_row_columns(raw, &selected, &codec);
        if let Ok((_, full)) = decoded {
            let (_, partial, _) = projected.expect("valid full row must allow a valid projection");
            assert_values(&partial, &selected.iter().map(|&i| full[i].clone()).collect::<Vec<_>>());
        }
    }
    // A missing overflow reader is an explicit error, never silent inline data.
    let lob_codec = RowCodec {
        columns: vec![ColumnCodec { kind: ColumnKind::VarBytes }],
        layouts: vec![identity_layout_for_columns(&[ColumnCodec { kind: ColumnKind::VarBytes }], 1)],
        defaults: vec![Value::Null], active_layout_id: 1,
    };
    let lob = LobRef { first_page_id: 1, last_page_id: 2, total_len: 20_000, generation: 1 };
    let encoded = encode_mvcc_row(header, &[Value::Bytes(Vec::new())], &lob_codec, &[Some(lob)]).unwrap();
    assert!(decode_mvcc_row(&encoded, &lob_codec).is_err());
    let _ = decode_mvcc_external_refs(&encoded, &lob_codec);
}

impl Database {
    /// Real multi-page chains, projections, corruption and reuse generations.
    pub(crate) fn fuzz_lob_chain(&self, table_name: &str, bytes: &[u8]) {
        let table = self.table(table_name).unwrap();
        let handle = self.overflow_handle(table.id).unwrap();
        let mut overflow = handle.write();
        let data = (0..OVFL_PAGE_PAYLOAD * 2 + 37).map(|i| bytes.get(i % bytes.len().max(1)).copied().unwrap_or(0) ^ i as u8).collect::<Vec<_>>();
        let lob = overflow.allocate_chain_compressible(&data, false).unwrap();
        assert_ne!(lob.first_page_id, lob.last_page_id);
        assert_eq!(overflow.read_chain(lob).unwrap(), data);
        let start = OVFL_PAGE_PAYLOAD - 13;
        assert_eq!(overflow.read_chain_range(lob, start as u64, 71).unwrap(), data[start..start+71]);
        let key = overflow.page_key(lob.first_page_id);
        let original = self.buffer_pool.read_page_bytes(&key).unwrap().unwrap();
        for fault in 0..4 {
            self.buffer_pool.with_raw_page_mut(key.clone(), 0, |page| {
                match fault {
                    0 => write_u64(page, OVFL_OFF_NEXT, lob.first_page_id),
                    1 => write_u16(page, OVFL_OFF_PAYLOAD_LEN, u16::MAX),
                    2 => write_u64(page, OVFL_OFF_GENERATION, lob.generation ^ 1),
                    _ => write_u32(page, OVFL_OFF_MAGIC, 0),
                }
                Ok(())
            }).unwrap();
            assert!(overflow.read_chain(lob).is_err(), "accepted LOB corruption {fault}");
            self.buffer_pool.with_raw_page_mut(key.clone(), 0, |page| { page.copy_from_slice(&original); Ok(()) }).unwrap();
        }
        assert_eq!(overflow.read_chain(lob).unwrap(), data);
        drop(overflow);
        let codec = RowCodec { columns: vec![ColumnCodec { kind: ColumnKind::VarBytes }],
            layouts: vec![identity_layout_for_columns(&[ColumnCodec { kind: ColumnKind::VarBytes }], 1)],
            defaults: vec![Value::Null], active_layout_id: 1 };
        let encoded = encode_mvcc_row(RowHeader::visible(0), &[Value::Bytes(Vec::new())], &codec, &[Some(lob)]).unwrap();
        with_lob_materializer(Some(std::sync::Arc::new(OverflowReader::new(handle.clone()))), || {
            assert_eq!(decode_mvcc_row(&encoded, &codec).unwrap().1, vec![Value::Bytes(data.clone())]);
            assert_eq!(decode_mvcc_row_columns(&encoded, &[0, 0], &codec).unwrap().1, vec![Value::Bytes(data.clone()), Value::Bytes(data.clone())]);
        });
        let mut overflow = handle.write();
        overflow.free_chain(lob).unwrap();
        let replacement = overflow.allocate_chain_compressible(&data, false).unwrap();
        assert_ne!(replacement.generation, lob.generation);
        assert!(overflow.read_chain(lob).is_err(), "stale descriptor read a reused chain");
        assert_eq!(overflow.read_chain(replacement).unwrap(), data);
        overflow.free_chain(replacement).unwrap();
    }
}
