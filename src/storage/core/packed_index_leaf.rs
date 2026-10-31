// A compact, governed working representation for numeric resident leaves.
// The canonical encoded page remains the persistence and interchange format.

#[derive(Clone, Copy, Debug, Default)]
struct PackedLeafPart {
    kind: u8,
    scale: u8,
    base: i128,
    high: i128,
    nullable: bool,
    bits: u8,
}

fn packed_leaf_value(part: &KeyPart) -> Option<(u8, u8, i128)> {
    Some(match part {
        KeyPart::Null => (0, 0, 0),
        KeyPart::Int(v) => (1, 0, i128::from(*v)),
        KeyPart::UInt(v) => (2, 0, i128::from(*v)),
        KeyPart::Date(v) => (3, 0, i128::from(*v)),
        KeyPart::DateTime(v) => (4, 0, i128::from(*v)),
        KeyPart::Time(v) => (5, 0, i128::from(*v)),
        KeyPart::Year(v) => (6, 0, i128::from(*v)),
        KeyPart::Float32(v) => (7, 0, i128::from(*v)),
        KeyPart::Float64(v) => (8, 0, i128::from(*v)),
        KeyPart::Decimal { value, scale } => (9, *scale, *value),
        _ => return None,
    })
}

impl PackedLeafPart {
    fn observe(&mut self, part: &KeyPart) -> Option<()> {
        let (kind, scale, value) = packed_leaf_value(part)?;
        self.observe_scalar(kind, scale, value)
    }

    fn observe_scalar(&mut self, kind: u8, scale: u8, value: i128) -> Option<()> {
        if kind == 0 {
            self.nullable = true;
        } else if self.kind == 0 {
            self.kind = kind;
            self.scale = scale;
            self.base = value;
            self.high = value;
        } else if self.kind == kind && self.scale == scale {
            self.base = self.base.min(value);
            self.high = self.high.max(value);
        } else {
            return None;
        }
        Some(())
    }

    fn finish(&mut self) -> Option<()> {
        if self.kind == 0 {
            self.bits = 0;
        } else {
            let high = self
                .high
                .checked_sub(self.base)?
                .checked_add(i128::from(self.nullable))? as u128;
            self.bits = (128 - high.leading_zeros()).max(4).try_into().ok()?;
            if self.bits > 64 {
                return None;
            }
        }
        Some(())
    }

    fn encode(&self, part: &KeyPart) -> Option<u64> {
        let (kind, scale, value) = packed_leaf_value(part)?;
        if kind == 0 {
            return self.nullable.then_some(0);
        }
        if kind != self.kind || scale != self.scale {
            return None;
        }
        // Code zero is reserved for NULL in nullable domains. A value one
        // below the base must decline, rather than aliasing that sentinel.
        let delta = value.checked_sub(self.base)?;
        if delta < 0 {
            return None;
        }
        let code = delta.checked_add(i128::from(self.nullable))?;
        if code as u128 > packed_leaf_mask(self.bits) as u128 {
            return None;
        }
        Some(code as u64)
    }

    fn decode(&self, code: u64) -> KeyPart {
        if self.kind == 0 || (self.nullable && code == 0) {
            return KeyPart::Null;
        }
        let value = self.base + (i128::from(code) - i128::from(self.nullable));
        match self.kind {
            1 => KeyPart::Int(value as i64),
            2 => KeyPart::UInt(value as u64),
            3 => KeyPart::Date(value as i32),
            4 => KeyPart::DateTime(value as i64),
            5 => KeyPart::Time(value as i64),
            6 => KeyPart::Year(value as u8),
            7 => KeyPart::Float32(value as u32),
            8 => KeyPart::Float64(value as u64),
            9 => KeyPart::Decimal {
                value,
                scale: self.scale,
            },
            _ => unreachable!("validated packed leaf kind"),
        }
    }
}

#[inline]
fn packed_leaf_mask(bits: u8) -> u64 {
    if bits == 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    }
}

/// Temporary page-local numeric data. Scalars occupy their native 64 bits;
/// Decimal coefficients outside i64 retain the general encoded-page path.
#[derive(Clone, Copy)]
struct PackedLeafRawEntry {
    values: [u64; 4],
    nulls: u8,
    row_id: RowId,
}

struct PackedLeafCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl PackedLeafCursor<'_> {
    #[inline]
    fn take<const N: usize>(&mut self) -> Result<[u8; N], QueryError> {
        let end = self.offset + N;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: truncated packed numeric record",
            )
        })?;
        self.offset = end;
        Ok(value.try_into().unwrap())
    }

    #[inline]
    fn unsigned(&mut self, width: u8) -> Result<u64, QueryError> {
        Ok(match width {
            1 => u64::from(self.take::<1>()?[0]),
            2 => u64::from(u16::from_le_bytes(self.take()?)),
            4 => u64::from(u32::from_le_bytes(self.take()?)),
            8 => u64::from_le_bytes(self.take()?),
            _ => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt index: invalid numeric width",
                ))
            }
        })
    }
}

impl PackedLeafRawEntry {
    fn read(
        header: &LeafPayloadHeader<'_>,
        slot: usize,
        parts: &mut [PackedLeafPart; 4],
        arity: usize,
    ) -> Result<Option<Self>, QueryError> {
        let raw = header.read_slot(slot);
        let start = usize::from(raw.offset);
        let end = start + usize::from(raw.len);
        let prefix = LEAF_OFF_ENTRIES
            + usize::from(header.key_layout == KEY_LAYOUT_COMPACT_INT2) * COMPACT_INT2_PREFIX_BYTES;
        if start < prefix || end > header.slot_array_start {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: packed numeric record bounds",
            ));
        }
        let mut cursor = PackedLeafCursor {
            bytes: &header.payload[start..end],
            offset: 0,
        };
        let mut record = Self {
            values: [0; 4],
            nulls: 0,
            row_id: RowId {
                page_id: 0,
                slot_id: 0,
            },
        };
        let mut put = |column: usize, kind: u8, scale: u8, value: i128| {
            parts[column].observe_scalar(kind, scale, value)?;
            if kind == 0 {
                record.nulls |= 1 << column;
            }
            record.values[column] = value as u64;
            Some(())
        };
        match header.key_layout {
            KEY_LAYOUT_GENERIC => {
                if cursor.take::<1>()?[0] as usize != arity {
                    return Ok(None);
                }
                for column in 0..arity {
                    let tag = cursor.take::<1>()?[0];
                    let (kind, scale, value) = match tag {
                        0 => (0, 0, 0),
                        1 => (1, 0, i128::from(cursor.unsigned(8)? as i64)),
                        2 => (2, 0, i128::from(cursor.unsigned(8)?)),
                        6 => (7, 0, i128::from(cursor.unsigned(4)?)),
                        7 => (8, 0, i128::from(cursor.unsigned(8)?)),
                        8 => {
                            let value = i128::from_le_bytes(cursor.take()?);
                            let scale = cursor.take::<1>()?[0];
                            if i64::try_from(value).is_err() {
                                return Ok(None);
                            }
                            (9, scale, value)
                        }
                        9 => (3, 0, i128::from(cursor.unsigned(4)? as u32 as i32)),
                        10 => (5, 0, i128::from(cursor.unsigned(8)? as i64)),
                        11 => (4, 0, i128::from(cursor.unsigned(8)? as i64)),
                        12 => (6, 0, i128::from(cursor.unsigned(1)?)),
                        4 | 5 | 13 | 14 => return Ok(None),
                        _ => {
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "corrupt index: unknown numeric key tag",
                            ))
                        }
                    };
                    if put(column, kind, scale, value).is_none() {
                        return Ok(None);
                    }
                }
            }
            KEY_LAYOUT_COMPACT_INT2 => {
                if arity != 2 {
                    return Ok(None);
                }
                let aux = header.layout_aux.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt index: missing packed integer layout",
                    )
                })?;
                let first = header
                    .base_time
                    .wrapping_add(cursor.unsigned(aux.width1)? as i64);
                let second = aux
                    .col2_base
                    .wrapping_add(cursor.unsigned(aux.width2)? as i64);
                if put(0, 1, 0, i128::from(first)).is_none()
                    || put(1, 1, 0, i128::from(second)).is_none()
                {
                    return Ok(None);
                }
            }
            layout => {
                if arity != 1 {
                    return Ok(None);
                }
                let (kind, scale, value) = match layout {
                    KEY_LAYOUT_COMPACT_INT_U16 | KEY_LAYOUT_COMPACT_INT_U32 => {
                        let width = if layout == KEY_LAYOUT_COMPACT_INT_U16 {
                            2
                        } else {
                            4
                        };
                        (
                            1,
                            0,
                            i128::from(
                                header
                                    .base_time
                                    .wrapping_add(cursor.unsigned(width)? as i64),
                            ),
                        )
                    }
                    KEY_LAYOUT_COMPACT_UINT_U16 | KEY_LAYOUT_COMPACT_UINT_U32 => {
                        let width = if layout == KEY_LAYOUT_COMPACT_UINT_U16 {
                            2
                        } else {
                            4
                        };
                        (
                            2,
                            0,
                            i128::from(
                                ordered_i64_to_uint(header.base_time)
                                    .wrapping_add(cursor.unsigned(width)?),
                            ),
                        )
                    }
                    KEY_LAYOUT_COMPACT_DATE_U16 => (
                        3,
                        0,
                        i128::from(header.base_time.wrapping_add(cursor.unsigned(2)? as i64) as i32),
                    ),
                    KEY_LAYOUT_COMPACT_TIME_U32 => (
                        4,
                        0,
                        i128::from(header.base_time.wrapping_add(cursor.unsigned(4)? as i64)),
                    ),
                    KEY_LAYOUT_COMPACT_DECIMAL_I32 => (
                        9,
                        header.base_time as u8,
                        i128::from(cursor.unsigned(4)? as u32 as i32),
                    ),
                    KEY_LAYOUT_COMPACT_DECIMAL_I64 => (
                        9,
                        header.base_time as u8,
                        i128::from(cursor.unsigned(8)? as i64),
                    ),
                    _ => {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt index: unknown packed numeric layout",
                        ))
                    }
                };
                if put(0, kind, scale, value).is_none() {
                    return Ok(None);
                }
            }
        }
        record.row_id = RowId {
            page_id: u32::from_le_bytes(cursor.take()?),
            slot_id: u16::from_le_bytes(cursor.take()?),
        };
        if cursor.offset != cursor.bytes.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: packed numeric record length",
            ));
        }
        Ok(Some(record))
    }

    #[inline]
    fn value(&self, column: usize, kind: u8) -> i128 {
        match kind {
            1 | 4 | 5 | 9 => i128::from(self.values[column] as i64),
            3 => i128::from(self.values[column] as u32 as i32),
            _ => i128::from(self.values[column]),
        }
    }
}

#[derive(Debug)]
struct PackedNumericLeaf {
    parts: [PackedLeafPart; 4],
    arity: usize,
    page_bits: u8,
    slot_bits: u8,
    key_layout: u8,
    base_time: i64,
    layout_aux: Option<LayoutAux>,
    next_leaf: Option<u64>,
    page_max_trx_id: u64,
    encoded_bytes: usize,
    entries: Vec<u64>,
    gap_start: usize,
    gap_end: usize,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl PackedNumericLeaf {
    fn supported_key(key: &IndexKey) -> bool {
        (1..=4).contains(&key.0.len()) && key.0.iter().all(|part| packed_leaf_value(part).is_some())
    }

    fn build(bytes: &[u8], incoming: &IndexEntry) -> Result<Option<Self>, QueryError> {
        if !Self::supported_key(&incoming.key) {
            return Ok(None);
        }
        let header = parse_leaf_payload_header(bytes)?;
        // Tiny leaves already have a cheap raw kernel; avoid cache churn while
        // the page's key domain and layout are still changing quickly.
        if header.count < 32 {
            return Ok(None);
        }
        let mut parts = [PackedLeafPart::default(); 4];
        let arity = incoming.key.0.len();
        let mut max_page = incoming.row_id.page_id;
        let mut max_slot = incoming.row_id.slot_id;
        for (part, value) in parts.iter_mut().zip(incoming.key.0.iter()) {
            if part.observe(value).is_none() {
                return Ok(None);
            }
        }
        let _admission = crate::engine::executor::mem_budget::StorageLatchAdmissionScope::enter();
        let Some(_scratch) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                header.count * std::mem::size_of::<PackedLeafRawEntry>(),
            )
        else {
            return Ok(None);
        };
        let mut records = Vec::with_capacity(header.count);
        let mut encoded_bytes = INDEX_HEADER_BYTES
            + LEAF_OFF_ENTRIES
            + usize::from(header.key_layout == KEY_LAYOUT_COMPACT_INT2) * COMPACT_INT2_PREFIX_BYTES;
        for slot in 0..header.count {
            let Some(record) = PackedLeafRawEntry::read(&header, slot, &mut parts, arity)? else {
                return Ok(None);
            };
            max_page = max_page.max(record.row_id.page_id);
            max_slot = max_slot.max(record.row_id.slot_id);
            encoded_bytes += usize::from(header.read_slot(slot).len) + SLOT_OFFSET_BYTES;
            records.push(record);
        }
        for part in &mut parts[..arity] {
            if part.finish().is_none() {
                return Ok(None);
            }
        }
        let page_bits = (32 - max_page.leading_zeros()).max(16) as u8;
        let slot_bits = (16 - max_slot.leading_zeros()).max(10) as u8;
        let total_bits = parts[..arity]
            .iter()
            .map(|part| u32::from(part.bits))
            .sum::<u32>()
            + u32::from(page_bits)
            + u32::from(slot_bits);
        if total_bits > 64 {
            return Ok(None);
        }
        let min_key = IndexKey::from(
            parts[..arity]
                .iter()
                .map(|part| {
                    if part.nullable {
                        KeyPart::Null
                    } else {
                        part.decode(0)
                    }
                })
                .collect::<smallvec::SmallVec<[KeyPart; 4]>>(),
        );
        let min_entry = IndexEntry {
            key: min_key,
            row_id: RowId {
                page_id: 0,
                slot_id: 0,
            },
        };
        let overhead = INDEX_HEADER_BYTES
            + LEAF_OFF_ENTRIES
            + usize::from(header.key_layout == KEY_LAYOUT_COMPACT_INT2) * COMPACT_INT2_PREFIX_BYTES;
        let capacity = (PAGE_SIZE - overhead)
            / (IndexLeafPage::entry_bytes_in_layout(
                &min_entry,
                header.key_layout,
                header.layout_aux,
            ) + SLOT_OFFSET_BYTES);
        if capacity < header.count {
            return Ok(None);
        }
        let _admission = crate::engine::executor::mem_budget::StorageLatchAdmissionScope::enter();
        // This is a resident buffer representation, charged alongside encoded
        // frame bytes. Optional admission never borrows rollback's reserve.
        let Some(charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::BufferRaw,
                capacity * std::mem::size_of::<u64>() + std::mem::size_of::<Self>(),
            )
        else {
            return Ok(None);
        };
        let mut leaf = Self {
            parts,
            arity,
            page_bits,
            slot_bits,
            key_layout: header.key_layout,
            base_time: header.base_time,
            layout_aux: header.layout_aux,
            next_leaf: header.next_leaf,
            page_max_trx_id: header.page_max_trx_id,
            encoded_bytes,
            entries: Vec::with_capacity(capacity),
            gap_start: 0,
            gap_end: 0,
            _charge: charge,
        };
        for record in &records {
            let mut value = 0u64;
            for (column, part) in leaf.parts[..arity].iter().enumerate() {
                let code = if record.nulls & (1 << column) != 0 {
                    0
                } else {
                    (record.value(column, part.kind) - part.base + i128::from(part.nullable)) as u64
                };
                value = (value << part.bits) | code;
            }
            value = (value << leaf.row_bits())
                | (u64::from(record.row_id.page_id) << leaf.slot_bits)
                | u64::from(record.row_id.slot_id);
            leaf.entries.push(value);
        }
        if !leaf.entries.windows(2).all(|pair| pair[0] <= pair[1]) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt index: unordered packed leaf",
            ));
        }
        let count = leaf.entries.len();
        let Some(target) = leaf.pack(incoming) else {
            return Ok(None);
        };
        let position = leaf.entries.partition_point(|value| *value < target);
        let gap = leaf.entries.capacity() - count;
        leaf.entries.resize(leaf.entries.capacity(), 0);
        leaf.entries.copy_within(position..count, position + gap);
        leaf.gap_start = position;
        leaf.gap_end = position + gap;
        Ok(Some(leaf))
    }

    #[inline]
    fn row_bits(&self) -> u8 {
        self.page_bits + self.slot_bits
    }

    #[inline]
    fn pack_key(&self, key: &IndexKey) -> Option<u64> {
        if key.0.len() != self.arity {
            return None;
        }
        let mut value = 0u128;
        for (part, incoming) in self.parts[..self.arity].iter().zip(key.0.iter()) {
            value = (value << part.bits) | u128::from(part.encode(incoming)?);
        }
        Some(value as u64)
    }

    #[inline]
    fn pack(&self, entry: &IndexEntry) -> Option<u64> {
        if u64::from(entry.row_id.page_id) > packed_leaf_mask(self.page_bits)
            || u64::from(entry.row_id.slot_id) > packed_leaf_mask(self.slot_bits)
        {
            return None;
        }
        Some(
            (self.pack_key(&entry.key)? << self.row_bits())
                | (u64::from(entry.row_id.page_id) << self.slot_bits)
                | u64::from(entry.row_id.slot_id),
        )
    }

    fn row_id(&self, value: u64) -> RowId {
        RowId {
            page_id: ((value >> self.slot_bits) & packed_leaf_mask(self.page_bits)) as u32,
            slot_id: (value & packed_leaf_mask(self.slot_bits)) as u16,
        }
    }

    fn unpack(&self, value: u64) -> IndexEntry {
        let row_id = self.row_id(value);
        let mut key = value >> self.row_bits();
        let mut parts = smallvec::SmallVec::<[KeyPart; 4]>::new();
        for part in self.parts[..self.arity].iter().rev() {
            parts.push(part.decode(key & packed_leaf_mask(part.bits)));
            key >>= part.bits;
        }
        parts.reverse();
        IndexEntry {
            key: IndexKey::from(parts),
            row_id,
        }
    }

    fn admits_layout(&self, key: &IndexKey) -> bool {
        layout_admits(self.key_layout, self.base_time, self.layout_aux, key)
    }

    #[inline]
    fn len(&self) -> usize {
        self.gap_start + self.entries.len() - self.gap_end
    }

    #[inline]
    fn iter(&self) -> impl DoubleEndedIterator<Item = &u64> {
        self.entries[..self.gap_start]
            .iter()
            .chain(self.entries[self.gap_end..].iter())
    }

    #[inline]
    fn at(&self, slot: usize) -> u64 {
        self.entries[if slot < self.gap_start {
            slot
        } else {
            slot + self.gap_end - self.gap_start
        }]
    }

    /// Most repeated secondary keys advance at the same insertion boundary.
    /// Probe its neighbors first; sparse jumps retain logarithmic search.
    fn search(&self, value: u64) -> Result<usize, usize> {
        if self.gap_start > 0 {
            let last = self.entries[self.gap_start - 1];
            if value < last {
                return self.entries[..self.gap_start - 1].binary_search(&value);
            }
            if value == last {
                return Ok(self.gap_start - 1);
            }
        }
        if self.gap_end == self.entries.len() {
            return Err(self.len());
        }
        match value.cmp(&self.entries[self.gap_end]) {
            std::cmp::Ordering::Less => Err(self.gap_start),
            std::cmp::Ordering::Equal => Ok(self.gap_start),
            std::cmp::Ordering::Greater => self.entries[self.gap_end + 1..]
                .binary_search(&value)
                .map(|slot| self.gap_start + 1 + slot)
                .map_err(|slot| self.gap_start + 1 + slot),
        }
    }

    fn move_gap(&mut self, position: usize) {
        if position < self.gap_start {
            let count = self.gap_start - position;
            self.entries
                .copy_within(position..self.gap_start, self.gap_end - count);
            self.gap_start = position;
            self.gap_end -= count;
        } else if position > self.gap_start {
            let count = position - self.gap_start;
            self.entries
                .copy_within(self.gap_end..self.gap_end + count, self.gap_start);
            self.gap_start += count;
            self.gap_end += count;
        }
    }

    fn insert(&mut self, entry: &IndexEntry, txn_id: u64) -> Option<InPlaceInsert> {
        let value = self.pack(entry)?;
        let position = match self.search(value) {
            Ok(_) => return Some(InPlaceInsert::Duplicate),
            Err(position) => position,
        };
        if !self.admits_layout(&entry.key) {
            return None;
        }
        let extra = IndexLeafPage::entry_bytes_in_layout(entry, self.key_layout, self.layout_aux)
            + SLOT_OFFSET_BYTES;
        if self.encoded_bytes + extra > PAGE_SIZE {
            return Some(InPlaceInsert::NeedsSplit);
        }
        // Capacity is the maximum physically encodable count for this shape;
        // insertion can never reallocate while holding a page latch.
        assert!(self.gap_start < self.gap_end);
        self.move_gap(position);
        self.entries[self.gap_start] = value;
        self.gap_start += 1;
        self.encoded_bytes += extra;
        self.page_max_trx_id = self.page_max_trx_id.max(txn_id);
        Some(InPlaceInsert::Inserted)
    }

    fn delete(&mut self, entry: &IndexEntry, txn_id: u64, is_root: bool) -> Option<InPlaceDelete> {
        let value = self.pack(entry)?;
        let Ok(position) = self.search(value) else {
            return Some(InPlaceDelete::NotFound);
        };
        let removed = IndexLeafPage::entry_bytes_in_layout(entry, self.key_layout, self.layout_aux)
            + SLOT_OFFSET_BYTES;
        let after = self.encoded_bytes.checked_sub(removed)?;
        if !is_root && (self.len() <= 1 || after < INDEX_MIN_PAGE_BYTES) {
            return None;
        }
        self.move_gap(position);
        self.gap_end += 1;
        self.encoded_bytes = after;
        self.page_max_trx_id = self.page_max_trx_id.max(txn_id);
        Some(InPlaceDelete::Deleted {
            was_first: position == 0,
        })
    }

    fn reposition(
        &mut self,
        old: &IndexEntry,
        new: &IndexEntry,
        txn_id: u64,
        is_root: bool,
    ) -> Option<bool> {
        let old_value = self.pack(old)?;
        let new_value = self.pack(new)?;
        let Ok(old_pos) = self.search(old_value) else {
            return Some(false);
        };
        let Err(insertion_pos) = self.search(new_value) else {
            return Some(false);
        };
        let target_pos = insertion_pos - usize::from(old_pos < insertion_pos);
        if (!is_root && target_pos == 0 && (old_pos != 0 || new < old))
            || !self.admits_layout(&new.key)
        {
            return None;
        }
        let before_bytes =
            IndexLeafPage::entry_bytes_in_layout(old, self.key_layout, self.layout_aux);
        let after_bytes =
            IndexLeafPage::entry_bytes_in_layout(new, self.key_layout, self.layout_aux);
        let size = self
            .encoded_bytes
            .checked_sub(before_bytes)?
            .checked_add(after_bytes)?;
        if size > PAGE_SIZE || (!is_root && size < INDEX_MIN_PAGE_BYTES) {
            return None;
        }
        // All shape, capacity, separator and underflow checks precede either
        // mutation. Removal creates the gap needed by an otherwise full leaf.
        self.move_gap(old_pos);
        self.gap_end += 1;
        self.move_gap(target_pos);
        self.entries[self.gap_start] = new_value;
        self.gap_start += 1;
        self.encoded_bytes = size;
        self.page_max_trx_id = self.page_max_trx_id.max(txn_id);
        Some(true)
    }

    fn lookup(&self, key: &IndexKey) -> Option<LeafLookupResult> {
        let target = match self.pack_key(key) {
            Some(target) => target,
            None => {
                if key.0.len() != self.arity || self.len() == 0 {
                    return None;
                }
                // A cold/missing probe can be outside this leaf's packing
                // window. Prove it misses using the canonical comparator,
                // without encoding a dirty leaf merely to find no rows.
                let first = self.unpack(self.at(0));
                let last = self.unpack(self.at(self.len() - 1));
                let below = compare_index_entry_to_lookup_key(&first.key, key, true)
                    == std::cmp::Ordering::Greater;
                let above = compare_index_entry_to_lookup_key(&last.key, key, true)
                    == std::cmp::Ordering::Less;
                return (below || above).then_some(LeafLookupResult {
                    rows: Vec::new(),
                    next_leaf: self.next_leaf,
                    should_continue: above,
                });
            }
        };
        let row_bits = self.row_bits();
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.at(mid) >> row_bits < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut rows = Vec::new();
        while lo < self.len() && self.at(lo) >> row_bits == target {
            rows.push(self.row_id(self.at(lo)));
            lo += 1;
        }
        Some(LeafLookupResult {
            rows,
            next_leaf: self.next_leaf,
            should_continue: lo == self.len(),
        })
    }

    #[inline]
    fn scalar_at(&self, value: u64, column: usize) -> Option<i128> {
        let part = &self.parts[column];
        if part.kind == 0 {
            return None;
        }
        let shift = self.row_bits()
            + self.parts[column + 1..self.arity]
                .iter()
                .map(|p| p.bits)
                .sum::<u8>();
        let code = (value >> shift) & packed_leaf_mask(part.bits);
        if part.kind == 0 || (part.nullable && code == 0) {
            None
        } else {
            Some(part.base + (i128::from(code) - i128::from(part.nullable)))
        }
    }

    /// Write the canonical numeric record directly. The working image has
    /// already proved every key's type, scale and layout; rebuilding an AST
    /// and redispatching its variants per entry only repeats that proof.
    fn encode_record(
        &self,
        value: u64,
        image: &mut [u8],
        cursor: &mut usize,
    ) -> Result<(), QueryError> {
        match self.key_layout {
            KEY_LAYOUT_GENERIC => {
                write_index_payload_u8(image, cursor, self.arity as u8)?;
                for column in 0..self.arity {
                    let part = &self.parts[column];
                    let Some(scalar) = self.scalar_at(value, column) else {
                        write_index_payload_u8(image, cursor, 0)?;
                        continue;
                    };
                    let tag = [0, 1, 2, 9, 11, 10, 12, 6, 7, 8][part.kind as usize];
                    write_index_payload_u8(image, cursor, tag)?;
                    match part.kind {
                        3 | 7 => write_index_payload_u32(image, cursor, scalar as u32)?,
                        6 => write_index_payload_u8(image, cursor, scalar as u8)?,
                        9 => {
                            write_index_payload_bytes(image, cursor, &scalar.to_le_bytes())?;
                            write_index_payload_u8(image, cursor, part.scale)?;
                        }
                        _ => write_index_payload_u64(image, cursor, scalar as u64)?,
                    }
                }
            }
            KEY_LAYOUT_COMPACT_INT2 => {
                let aux = self.layout_aux.expect("validated compact integer pair");
                for (column, base, width) in [
                    (0, self.base_time, aux.width1),
                    (1, aux.col2_base, aux.width2),
                ] {
                    let delta = self.scalar_at(value, column).expect("non-null compact key")
                        - i128::from(base);
                    write_int2_delta(image, cursor, delta as u32, width)?;
                }
            }
            layout => {
                let scalar = self.scalar_at(value, 0).expect("non-null compact key");
                let base = match layout {
                    KEY_LAYOUT_COMPACT_UINT_U16 | KEY_LAYOUT_COMPACT_UINT_U32 => {
                        i128::from(ordered_i64_to_uint(self.base_time))
                    }
                    KEY_LAYOUT_COMPACT_DECIMAL_I32 | KEY_LAYOUT_COMPACT_DECIMAL_I64 => 0,
                    _ => i128::from(self.base_time),
                };
                let delta = scalar - base;
                match layout {
                    KEY_LAYOUT_COMPACT_INT_U16
                    | KEY_LAYOUT_COMPACT_UINT_U16
                    | KEY_LAYOUT_COMPACT_DATE_U16 => {
                        write_index_payload_u16(image, cursor, delta as u16)?
                    }
                    KEY_LAYOUT_COMPACT_INT_U32
                    | KEY_LAYOUT_COMPACT_UINT_U32
                    | KEY_LAYOUT_COMPACT_TIME_U32
                    | KEY_LAYOUT_COMPACT_DECIMAL_I32 => {
                        write_index_payload_u32(image, cursor, delta as u32)?
                    }
                    KEY_LAYOUT_COMPACT_DECIMAL_I64 => {
                        write_index_payload_u64(image, cursor, delta as u64)?
                    }
                    _ => unreachable!("validated numeric leaf layout"),
                }
            }
        }
        let row_id = self.row_id(value);
        write_index_payload_u32(image, cursor, row_id.page_id)?;
        write_index_payload_u16(image, cursor, row_id.slot_id)
    }

    #[inline(never)]
    fn materialize(&self, bytes: &mut [u8]) -> Result<(), QueryError> {
        let mut image = [0u8; PAGE_SIZE];
        let prefix = INDEX_HEADER_BYTES
            + LEAF_OFF_ENTRIES
            + usize::from(self.key_layout == KEY_LAYOUT_COMPACT_INT2) * COMPACT_INT2_PREFIX_BYTES;
        image[..prefix].copy_from_slice(&bytes[..prefix]);
        let slot_start = self.encoded_bytes - self.len() * SLOT_OFFSET_BYTES;
        let mut cursor = prefix;
        for (slot, value) in self.iter().enumerate() {
            let start = cursor;
            self.encode_record(*value, &mut image, &mut cursor)?;
            write_raw_index_slot(
                &mut image,
                slot_start + slot * SLOT_OFFSET_BYTES,
                IndexRawSlot {
                    offset: (start - INDEX_HEADER_BYTES) as u16,
                    len: (cursor - start) as u16,
                },
            );
        }
        if cursor != slot_start {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "internal error: packed leaf size mismatch",
            ));
        }
        write_u16(
            &mut image,
            INDEX_HEADER_BYTES + LEAF_OFF_ENTRY_COUNT,
            self.len() as u16,
        );
        write_u64(
            &mut image,
            INDEX_HEADER_BYTES + LEAF_OFF_PAGE_MAX_TRX_ID,
            self.page_max_trx_id,
        );
        write_u32(
            &mut image,
            20,
            (self.encoded_bytes - INDEX_HEADER_BYTES) as u32,
        );
        bytes.copy_from_slice(&image);
        Ok(())
    }
}

#[cfg(test)]
mod packed_numeric_leaf_tests {
    use super::*;

    fn key(kind: usize, n: i64) -> IndexKey {
        IndexKey::from(match kind {
            0 => vec![KeyPart::Int(n - 100)],
            1 => vec![KeyPart::UInt((1u64 << 63) + n as u64 * 100_000)],
            2 => vec![KeyPart::Int(n / 7), KeyPart::Int(n % 7)],
            3 => vec![KeyPart::Date(18_000 + n as i32)],
            4 => vec![KeyPart::DateTime(1_600_000_000_000_000 + n * 1000)],
            5 => vec![KeyPart::Null, KeyPart::Int(n - 100)],
            6 => vec![KeyPart::Int(n % 7), KeyPart::Int(n * 8191)],
            7 => vec![KeyPart::Decimal {
                value: i128::from(n - 100) * 101,
                scale: 2,
            }],
            8 => vec![KeyPart::Time(n * 10_000 - 200_000)],
            9 => vec![KeyPart::Year(n as u8)],
            10 => vec![
                KeyPart::Int(-1),
                KeyPart::UInt(7),
                KeyPart::Date(18000),
                KeyPart::Int(n),
            ],
            11 => vec![
                if n % 5 == 0 {
                    KeyPart::Null
                } else {
                    KeyPart::Int(n % 7)
                },
                KeyPart::Int(n),
            ],
            12 => vec![KeyPart::Float32(f32_to_order_u32((n - 100) as f32 / 3.0))],
            13 => vec![KeyPart::Float64(f64_to_order_u64(1.0 + n as f64 * 1e-12))],
            14 => vec![KeyPart::Int(n * 1_000_000 - 100)],
            15 => vec![KeyPart::UInt((1u64 << 63) + n as u64)],
            16 => vec![KeyPart::Date(i32::MIN + n as i32)],
            17 => vec![KeyPart::Decimal {
                value: i128::from(i64::MIN) + i128::from(n) * 137,
                scale: 4,
            }],
            _ => vec![KeyPart::Null, KeyPart::Int(n << 30)],
        })
    }

    fn entry(kind: usize, n: i64) -> IndexEntry {
        IndexEntry {
            key: key(kind, n),
            row_id: RowId {
                page_id: (n * 137) as u32,
                slot_id: n as u16 % 31,
            },
        }
    }

    fn decoded(bytes: &[u8]) -> IndexLeafPage {
        match decode_validated_index_tree_page(bytes).unwrap() {
            IndexPage::Leaf(leaf) => leaf,
            _ => panic!("expected leaf"),
        }
    }

    #[test]
    fn packed_leaf_mutation_and_materialization_match_canonical_oracle() {
        for kind in 0..19 {
            let mut initial: Vec<_> = (0..160).step_by(2).map(|n| entry(kind, n)).collect();
            initial.sort();
            for next in [None, Some(77)] {
                let mut bytes = encode_index_tree_page(&IndexPage::Leaf(
                    IndexLeafPage::new_with_resolved_layout(initial.clone(), next),
                ))
                .unwrap();
                for removed in initial.iter().skip(3).step_by(11) {
                    try_delete_raw_leaf(&mut bytes, removed, 3).unwrap();
                }
                let incoming = entry(kind, 3);
                let mut cache = PackedNumericLeaf::build(&bytes, &incoming)
                    .unwrap()
                    .expect("packable model");
                let mut oracle = bytes.clone();
                let mut inserted = 0;
                for n in (1..159).step_by(2) {
                    let row = entry(kind, n);
                    let Some(result) = cache.insert(&row, 91) else {
                        // Rounded domains may still reject an unseen NULL
                        // shape; declining must leave the logical leaf intact.
                        cache.materialize(&mut bytes).unwrap();
                        assert_eq!(decoded(&bytes).entries, decoded(&oracle).entries);
                        continue;
                    };
                    let reference = try_insert_raw_leaf(&mut oracle, &row, 91).unwrap();
                    assert_eq!(format!("{result:?}"), format!("{:?}", reference.unwrap()));
                    assert!(matches!(
                        cache.insert(&row, 92),
                        Some(InPlaceInsert::Duplicate)
                    ));
                    inserted += usize::from(matches!(result, InPlaceInsert::Inserted));
                    for probe in [entry(kind, 0), row.clone(), entry(kind, 159)] {
                        if let Some(actual) = cache.lookup(&probe.key) {
                            let expected = lookup_leaf_page_bytes(&oracle, &probe.key).unwrap();
                            assert_eq!(actual.rows, expected.rows);
                            assert_eq!(actual.next_leaf, expected.next_leaf);
                            assert_eq!(actual.should_continue, expected.should_continue);
                        }
                    }
                }
                assert!(inserted >= 40, "kind={kind} inserted={inserted}");
                cache.materialize(&mut bytes).unwrap();
                let actual = decoded(&bytes);
                let expected = decoded(&oracle);
                assert_eq!(actual.entries, expected.entries, "kind={kind}");
                assert_eq!(actual.key_layout, expected.key_layout);
                assert_eq!(actual.page_max_trx_id, expected.page_max_trx_id);
                assert_eq!(actual.next_leaf, next);
                assert_eq!(
                    cache.encoded_bytes,
                    IndexLeafPage::compute_encoded_size(
                        &actual.entries,
                        actual.key_layout,
                        actual.layout_aux
                    )
                );
            }
        }
    }

    #[test]
    fn packed_point_updates_and_deletes_match_the_canonical_oracle() {
        for kind in 0..19 {
            let mut initial: Vec<_> = (0..160).step_by(2).map(|n| entry(kind, n)).collect();
            initial.sort();
            let mut bytes = encode_index_tree_page(&IndexPage::Leaf(
                IndexLeafPage::new_with_resolved_layout(initial.clone(), Some(77)),
            ))
            .unwrap();
            let mut native = PackedNumericLeaf::build(&bytes, &entry(kind, 3))
                .unwrap()
                .unwrap();
            for n in (20..140).step_by(4) {
                let old = entry(kind, n);
                let mut new = entry(kind, n + 1);
                new.row_id = old.row_id;
                let actual = native.reposition(&old, &new, 20, true);
                let expected = try_reposition_raw_leaf(&mut bytes, &old, &new, 20, true).unwrap();
                assert_eq!(actual, expected, "kind={kind} n={n}");
                assert_eq!(actual, Some(true));
                let actual = native.delete(&new, 21, true).unwrap();
                let expected = try_delete_raw_leaf(&mut bytes, &new, 21).unwrap().unwrap();
                assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
                assert!(matches!(
                    native.delete(&new, 22, true),
                    Some(InPlaceDelete::NotFound)
                ));
            }
            let expected = decoded(&bytes);
            native.materialize(&mut bytes).unwrap();
            assert_eq!(decoded(&bytes).entries, expected.entries, "kind={kind}");
            assert_eq!(decoded(&bytes).page_max_trx_id, expected.page_max_trx_id);
            let before = (
                native.entries.clone(),
                native.gap_start,
                native.gap_end,
                native.encoded_bytes,
            );
            let first = native.unpack(native.at(0));
            let mut replacement = first.clone();
            replacement.row_id.slot_id += 1;
            assert_eq!(native.reposition(&first, &replacement, 23, false), None);
            assert!(native.delete(&first, 23, false).is_none());
            assert_eq!(
                (
                    native.entries.clone(),
                    native.gap_start,
                    native.gap_end,
                    native.encoded_bytes
                ),
                before
            );
        }
    }

    #[test]
    fn packed_flat_decoder_preserves_every_compact_integer_pair_width() {
        for (left_width, left_step) in [(1, 1i64), (2, 3), (4, 1000)] {
            for (right_width, right_step) in [(1, 1i64), (2, 3), (4, 1000)] {
                let entry = |n: i64| IndexEntry {
                    key: IndexKey::from(vec![
                        KeyPart::Int(-5_000_000_000 + n * left_step),
                        KeyPart::Int(5_000_000_000 + n * right_step),
                    ]),
                    row_id: RowId {
                        page_id: n as u32,
                        slot_id: 1,
                    },
                };
                let initial = (0..160).step_by(2).map(entry).collect();
                let mut raw = encode_index_tree_page(&IndexPage::Leaf(
                    IndexLeafPage::new_with_resolved_layout(initial, Some(33)),
                ))
                .unwrap();
                let header = parse_leaf_payload_header(&raw).unwrap();
                assert_eq!(header.key_layout, KEY_LAYOUT_COMPACT_INT2);
                let aux = header.layout_aux.unwrap();
                assert_eq!((aux.width1, aux.width2), (left_width, right_width));
                let mut native = PackedNumericLeaf::build(&raw, &entry(3)).unwrap().unwrap();
                for n in (1..159).step_by(2) {
                    assert!(matches!(
                        native.insert(&entry(n), 77),
                        Some(InPlaceInsert::Inserted)
                    ));
                    try_insert_raw_leaf(&mut raw, &entry(n), 77).unwrap();
                }
                let oracle = decoded(&raw);
                native.materialize(&mut raw).unwrap();
                assert_eq!(decoded(&raw).entries, oracle.entries);
            }
        }
    }

    #[test]
    fn packed_leaf_declines_large_domains_and_incompatible_shapes_without_mutation() {
        let mut initial: Vec<_> = (0..160).map(|n| entry(0, n)).collect();
        initial.sort();
        let mut bytes = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(initial, None),
        ))
        .unwrap();
        let original = bytes.clone();
        let mut cache = PackedNumericLeaf::build(&bytes, &entry(0, 100))
            .unwrap()
            .unwrap();
        for key in [
            vec![KeyPart::Int(i64::MIN)],
            vec![KeyPart::Int(i64::MAX)],
            vec![KeyPart::UInt(8)],
            vec![KeyPart::String("x".into())],
            vec![KeyPart::Null],
            vec![KeyPart::Int(1), KeyPart::Int(2)],
        ] {
            let incoming = IndexEntry {
                key: IndexKey::from(key),
                row_id: RowId {
                    page_id: 1,
                    slot_id: 1,
                },
            };
            assert!(cache.insert(&incoming, 91).is_none());
        }
        let before = cache.entries.clone();
        assert!(cache
            .insert(
                &IndexEntry {
                    key: key(0, 12),
                    row_id: RowId {
                        page_id: u32::MAX,
                        slot_id: u16::MAX
                    }
                },
                91
            )
            .is_none());
        assert_eq!(cache.entries, before);
        cache.materialize(&mut bytes).unwrap();
        assert_eq!(decoded(&bytes).entries, decoded(&original).entries);
        let unsupported = IndexEntry {
            key: IndexKey::from(vec![KeyPart::Bytes(vec![0, 255])]),
            row_id: RowId {
                page_id: 1,
                slot_id: 1,
            },
        };
        assert!(PackedNumericLeaf::build(&bytes, &unsupported)
            .unwrap()
            .is_none());
    }
}

impl Frame {
    #[inline]
    fn has_packed_leaf(&self) -> bool {
        matches!(self.packed_leaf_state.load(Ordering::Acquire), 1 | 2)
    }

    /// Caller owns bytes.write(); publication and materialization never change
    /// logical contents or their WAL/dirty epoch, only their representation.
    fn materialize_packed_leaf(&self, bytes: &mut [u8]) -> Result<(), QueryError> {
        if self.packed_leaf_state.load(Ordering::Acquire) == 2 {
            let packed = self.packed_leaf.lock();
            let leaf = packed.as_ref().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "internal error: missing packed leaf image",
                )
            })?;
            leaf.materialize(bytes)?;
            self.checksum_dirty.store(true, Ordering::Release);
            self.packed_leaf_state.store(1, Ordering::Release);
        }
        Ok(())
    }

    /// Caller owns bytes.write() and is replacing or recycling its image.
    fn invalidate_packed_leaf(&self) {
        self.packed_leaf.lock().take();
        self.packed_leaf_state.store(0, Ordering::Release);
    }

    #[inline]
    fn read_materialized_index_bytes(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, Vec<u8>>, QueryError> {
        let bytes = self.bytes.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame read lock poisoned",
            )
        })?;
        if self.packed_leaf_state.load(Ordering::Acquire) != 2 {
            return Ok(bytes);
        }
        drop(bytes);
        let mut bytes = self.bytes.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame write lock poisoned",
            )
        })?;
        self.materialize_packed_leaf(&mut bytes)?;
        // Atomic downgrade prevents a writer from making the encoded image
        // stale again between materialization and this reader's snapshot.
        Ok(std::sync::RwLockWriteGuard::downgrade(bytes))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PackedLeafRunOutcome {
    Consumed(usize),
    Structural,
}

impl BufferPool {
    fn publish_packed_leaf_mutation(&self, pinned: &PinnedFrame, dirty_lsn: u64) {
        self.write_requests.fetch_add(1, Ordering::Relaxed);
        pinned.frame.checksum_dirty.store(true, Ordering::Release);
        if btree_logical_wal::is_deferred() {
            pinned.frame.deferred_pins.fetch_add(1, Ordering::AcqRel);
            btree_logical_wal::push_deferred_key(pinned.frame.key.clone());
        }
        if pinned.frame.mark_dirty(dirty_lsn) {
            self.dirty_pages.fetch_add(1, Ordering::Relaxed);
            self.note_dirty(&pinned.frame.key, dirty_lsn);
        }
        self.mark_raw_structure_validated(&pinned.frame);
        pinned.frame.prefetch.store(false, Ordering::Release);
        pinned.frame.packed_leaf_state.store(2, Ordering::Release);
    }

    /// Mutate an existing numeric image under the same page latch and logical
    /// WAL publication contract as the canonical byte mutation path.
    fn try_modify_existing_packed_leaf<T>(
        &self,
        pinned: &PinnedFrame,
        dirty_lsn: u64,
        mutate: impl FnOnce(&mut PackedNumericLeaf) -> Option<(bool, T)>,
    ) -> Result<Option<T>, QueryError> {
        // Reuse encoding work already owed by a batch. A synchronized image
        // must use the raw point kernel instead of creating a new whole-page
        // serialization obligation for one row.
        if pinned.frame.packed_leaf_state.load(Ordering::Acquire) != 2 {
            return Ok(None);
        }
        let _bytes = pinned.frame.bytes.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame write lock poisoned",
            )
        })?;
        if pinned.frame.packed_leaf_state.load(Ordering::Acquire) != 2 {
            return Ok(None);
        }
        let mut packed = pinned.frame.packed_leaf.lock();
        let Some(leaf) = packed.as_mut() else {
            return Ok(None);
        };
        let Some((modified, result)) = mutate(leaf) else {
            return Ok(None);
        };
        if modified {
            self.publish_packed_leaf_mutation(pinned, dirty_lsn);
        }
        Ok(Some(result))
    }

    fn try_insert_packed_leaf_run(
        self: &Arc<Self>,
        pinned: &PinnedFrame,
        dirty_lsn: u64,
        entries: &[IndexEntry],
        txn_id: u64,
        allow_build: bool,
    ) -> Result<Option<PackedLeafRunOutcome>, QueryError> {
        let state = pinned.frame.packed_leaf_state.load(Ordering::Acquire);
        if state == 3
            || (!allow_build && state != 2)
            || (state == 0 && !PackedNumericLeaf::supported_key(&entries[0].key))
        {
            return Ok(None);
        }
        self.wait_until_ready(&pinned.frame)?;
        let mut bytes = pinned.frame.bytes.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame write lock poisoned",
            )
        })?;
        // A flush/raw writer may have synchronized or invalidated the cache
        // while this point writer waited for the page latch.
        if !allow_build && pinned.frame.packed_leaf_state.load(Ordering::Acquire) != 2 {
            return Ok(None);
        }
        if pinned.frame.packed_leaf_state.load(Ordering::Acquire) == 0 {
            self.validate_frame_mutation_image(&pinned.frame, &mut bytes)?;
            let header = parse_leaf_payload_header(&bytes)?;
            let count = header.count;
            let extra = IndexLeafPage::entry_bytes_in_layout(
                &entries[0],
                header.key_layout,
                header.layout_aux,
            ) + SLOT_OFFSET_BYTES;
            if layout_admits(
                header.key_layout,
                header.base_time,
                header.layout_aux,
                &entries[0].key,
            ) && INDEX_HEADER_BYTES + header.payload.len() + extra > PAGE_SIZE
                && raw_leaf_live_encoded_len_and_count(&bytes)?.0 + extra > PAGE_SIZE
            {
                // A fitting-layout record cannot fit even after reclaiming
                // body holes. Let the structural oracle handle duplicates or
                // split once; do not decode/revert the same leaf beforehand.
                return Ok(Some(PackedLeafRunOutcome::Structural));
            }
            let Some(packed) = PackedNumericLeaf::build(&bytes, &entries[0])? else {
                if count >= 32 {
                    pinned.frame.packed_leaf_state.store(3, Ordering::Release);
                }
                return Ok(None);
            };
            *pinned.frame.packed_leaf.lock() = Some(Box::new(packed));
            pinned.frame.packed_leaf_state.store(1, Ordering::Release);
        }
        let mut packed = pinned.frame.packed_leaf.lock();
        let Some(packed) = packed.as_mut() else {
            return Ok(None);
        };
        let mut consumed = 0;
        let mut modified = false;
        let mut structural = false;
        for entry in entries {
            match packed.insert(entry, txn_id) {
                Some(InPlaceInsert::Inserted) => {
                    consumed += 1;
                    modified = true;
                }
                Some(InPlaceInsert::Duplicate) => consumed += 1,
                Some(InPlaceInsert::NeedsSplit) => {
                    structural = true;
                    break;
                }
                None => break,
            }
        }
        if modified {
            self.publish_packed_leaf_mutation(pinned, dirty_lsn);
        }
        Ok(if consumed > 0 {
            Some(PackedLeafRunOutcome::Consumed(consumed))
        } else if structural {
            Some(PackedLeafRunOutcome::Structural)
        } else {
            None
        })
    }
}

impl<P: std::ops::Deref<Target = BufferPool>> PinnedFrame<P> {
    fn packed_leaf_owned_snapshot(&self) -> Result<Option<(IndexLeafPage, u64)>, QueryError> {
        if !self.frame.has_packed_leaf() {
            return Ok(None);
        }
        let _bytes = self.frame.bytes.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame read lock poisoned",
            )
        })?;
        let packed = self.frame.packed_leaf.lock();
        Ok(packed.as_ref().map(|leaf| {
            let image = IndexLeafPage {
                entries: leaf.iter().map(|value| leaf.unpack(*value)).collect(),
                next_leaf: leaf.next_leaf,
                key_layout: leaf.key_layout,
                base_time: leaf.base_time,
                layout_aux: leaf.layout_aux,
                cached_size: leaf.encoded_bytes,
                page_max_trx_id: leaf.page_max_trx_id,
            };
            (image, self.frame.dirty_epoch.load(Ordering::Acquire))
        }))
    }

    fn packed_leaf_lookup(
        &self,
        key: &IndexKey,
    ) -> Result<Option<(LeafLookupResult, u64)>, QueryError> {
        if !self.frame.has_packed_leaf() {
            return Ok(None);
        }
        let _bytes = self.frame.bytes.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame read lock poisoned",
            )
        })?;
        let packed = self.frame.packed_leaf.lock();
        Ok(packed.as_ref().and_then(|leaf| {
            leaf.lookup(key)
                .map(|result| (result, leaf.page_max_trx_id))
        }))
    }

    fn packed_leaf_lookup_many_keyed(
        &self,
        keys: &[IndexKey],
        base: usize,
        seen: &mut Option<BtreeDedupSet>,
    ) -> Result<Option<LeafEntriesManyLookupResult<(u32, RowId)>>, QueryError> {
        if !self.frame.has_packed_leaf() {
            return Ok(None);
        }
        let _bytes = self.frame.bytes.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame read lock poisoned",
            )
        })?;
        let packed = self.frame.packed_leaf.lock();
        let Some(leaf) = packed.as_ref() else {
            return Ok(None);
        };
        // Prove every probe is representable before changing the shared dedup
        // set. An unsupported key must leave the raw fallback's inputs intact.
        let Some(results) = keys
            .iter()
            .map(|key| leaf.lookup(key))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let mut entries = Vec::new();
        let mut continue_key_offsets = Vec::new();
        for (offset, result) in results.into_iter().enumerate() {
            for row_id in result.rows {
                if seen.as_mut().is_none_or(|seen| seen.insert(row_id)) {
                    entries.push(((base + offset) as u32, row_id));
                }
            }
            if result.should_continue {
                continue_key_offsets.push(offset);
            }
        }
        Ok(Some(LeafEntriesManyLookupResult {
            entries,
            next_leaf: leaf.next_leaf,
            continue_key_offsets,
        }))
    }

    fn packed_leaf_candidate_stays(&self, entry: &IndexEntry) -> Result<bool, QueryError> {
        let _bytes = self.frame.bytes.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame read lock poisoned",
            )
        })?;
        let packed = self.frame.packed_leaf.lock();
        Ok(packed.as_ref().is_some_and(|leaf| {
            leaf.len() > 0
                && leaf.pack(entry).is_some_and(|value| {
                    value >= leaf.at(0)
                        && (leaf.next_leaf.is_none() || value <= leaf.at(leaf.len() - 1))
                })
        }))
    }

    fn packed_leaf_append_candidate(&self, entry: &IndexEntry) -> Result<Option<bool>, QueryError> {
        if !self.frame.has_packed_leaf() {
            return Ok(None);
        }
        let _bytes = self.frame.bytes.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "buffer frame read lock poisoned",
            )
        })?;
        let packed = self.frame.packed_leaf.lock();
        Ok(packed.as_ref().map(|leaf| {
            leaf.next_leaf.is_none()
                && leaf
                    .iter()
                    .next_back()
                    .is_none_or(|last| leaf.unpack(*last) < *entry)
        }))
    }
}

#[cfg(test)]
mod packed_leaf_frame_tests {
    use super::*;

    fn row(n: i64) -> IndexEntry {
        IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(n)]),
            row_id: RowId {
                page_id: n as u32,
                slot_id: n as u16 % 31,
            },
        }
    }

    #[test]
    fn packed_frame_reads_raw_mutations_and_replacements_share_one_logical_image() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool =
            BufferPool::new_with_options(8, log, DurabilityMode::Memory, Duration::ZERO, 1, 0);
        let key = PageKey {
            file: Arc::new(BufferPool::index_file_key(dir.path().join("packed.idx"))),
            page_id: 1,
        };
        let original: Vec<_> = (0..160).step_by(2).map(row).collect();
        let image = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(original.clone(), None),
        ))
        .unwrap();
        pool.store_page(key.clone(), image, 1).unwrap();
        let pinned = pool.pin_page(key.clone()).unwrap();
        let old = pinned.index_page().unwrap();
        assert_eq!(
            pool.try_insert_packed_leaf_run(&pinned, 2, &[row(3)], 777, true)
                .unwrap(),
            Some(PackedLeafRunOutcome::Consumed(1))
        );
        assert_eq!(pinned.frame.packed_leaf_state.load(Ordering::Acquire), 2);
        // The native reader observes the write before canonicalization.
        let (found, bound) = pinned.packed_leaf_lookup(&row(3).key).unwrap().unwrap();
        assert_eq!(found.rows, vec![row(3).row_id]);
        assert_eq!(bound, 777);
        let changed = IndexEntry {
            key: row(4).key,
            row_id: row(3).row_id,
        };
        assert_eq!(
            pool.try_modify_existing_packed_leaf(&pinned, 2, |leaf| leaf
                .reposition(&row(3), &changed, 777, true)
                .map(|done| (done, done)))
                .unwrap(),
            Some(true)
        );
        let deleted = pool
            .try_modify_existing_packed_leaf(&pinned, 2, |leaf| {
                leaf.delete(&changed, 777, true)
                    .map(|result| (matches!(result, InPlaceDelete::Deleted { .. }), result))
            })
            .unwrap();
        assert!(matches!(deleted, Some(InPlaceDelete::Deleted { .. })));
        assert_eq!(
            pool.try_insert_packed_leaf_run(&pinned, 2, &[row(3)], 777, false)
                .unwrap(),
            Some(PackedLeafRunOutcome::Consumed(1))
        );
        assert_eq!(
            pinned.frame.packed_leaf_state.load(Ordering::Acquire),
            2,
            "point mutations must retain the native image"
        );

        assert_eq!(
            parse_leaf_payload_header(&pinned.frame.bytes.read().unwrap())
                .unwrap()
                .count,
            80
        );
        let materialized = pinned.index_page().unwrap();
        let IndexPage::Leaf(materialized) = materialized.as_ref() else {
            panic!("expected leaf")
        };
        assert_eq!(materialized.entries.len(), 81);
        assert!(materialized.entries.contains(&row(3)));
        assert_eq!(pinned.frame.packed_leaf_state.load(Ordering::Acquire), 1);
        let IndexPage::Leaf(old) = old.as_ref() else {
            panic!("expected leaf")
        };
        assert_eq!(
            old.entries, original,
            "operation-local snapshots remain immutable"
        );
        assert!(pool
            .try_modify_existing_packed_leaf::<()>(&pinned, 3, |_| {
                panic!("a clean point write must retain the raw kernel")
            })
            .unwrap()
            .is_none());
        assert_eq!(
            pool.try_insert_packed_leaf_run(&pinned, 3, &[row(5)], 778, false)
                .unwrap(),
            None
        );
        assert_eq!(pinned.frame.packed_leaf_state.load(Ordering::Acquire), 1);
        assert_eq!(
            pool.try_insert_packed_leaf_run(&pinned, 3, &[row(5)], 778, true)
                .unwrap(),
            Some(PackedLeafRunOutcome::Consumed(1))
        );
        pool.modify_pinned_raw_page(&pinned, 4, |bytes| {
            let result = try_delete_raw_leaf(bytes, &row(3), 779)?;
            Ok((true, result))
        })
        .unwrap();
        assert_eq!(pinned.frame.packed_leaf_state.load(Ordering::Acquire), 0);
        let bytes = pool.read_page_bytes(&key).unwrap().unwrap();
        let IndexPage::Leaf(leaf) = decode_validated_index_tree_page(&bytes).unwrap() else {
            panic!("expected leaf")
        };
        let mut expected = original;
        expected.push(row(5));
        expected.sort();
        assert_eq!(leaf.entries, expected);
        assert_eq!(leaf.page_max_trx_id, 779);
        assert_eq!(
            pool.try_insert_packed_leaf_run(&pinned, 5, &[row(7)], 780, true)
                .unwrap(),
            Some(PackedLeafRunOutcome::Consumed(1))
        );
        let replacement = encode_index_tree_page(&IndexPage::Leaf(
            IndexLeafPage::new_with_resolved_layout(vec![row(99)], None),
        ))
        .unwrap();
        pool.store_page(key, replacement, 6).unwrap();
        assert_eq!(pinned.frame.packed_leaf_state.load(Ordering::Acquire), 0);
        let page = pinned.index_page().unwrap();
        let IndexPage::Leaf(page) = page.as_ref() else {
            panic!("expected leaf")
        };
        assert_eq!(page.entries, vec![row(99)]);
    }

    #[test]
    fn packed_nullable_values_below_base_never_alias_null() {
        for key in [
            KeyPart::Int(1),
            KeyPart::UInt(1),
            KeyPart::Date(1),
            KeyPart::DateTime(1),
            KeyPart::Time(1),
            KeyPart::Year(1),
            KeyPart::Float32(1),
            KeyPart::Float64(1),
            KeyPart::Decimal { value: 1, scale: 2 },
        ] {
            let mut part = PackedLeafPart::default();
            part.observe(&KeyPart::Null).unwrap();
            part.observe(&key).unwrap();
            part.finish().unwrap();
            let zero = match key {
                KeyPart::Int(_) => KeyPart::Int(0),
                KeyPart::UInt(_) => KeyPart::UInt(0),
                KeyPart::Date(_) => KeyPart::Date(0),
                KeyPart::DateTime(_) => KeyPart::DateTime(0),
                KeyPart::Time(_) => KeyPart::Time(0),
                KeyPart::Year(_) => KeyPart::Year(0),
                KeyPart::Float32(_) => KeyPart::Float32(0),
                KeyPart::Float64(_) => KeyPart::Float64(0),
                KeyPart::Decimal { .. } => KeyPart::Decimal { value: 0, scale: 2 },
                _ => unreachable!(),
            };
            assert_eq!(part.encode(&zero), None);
            assert_eq!(part.decode(part.encode(&key).unwrap()), key);
            assert_eq!(
                part.decode(part.encode(&KeyPart::Null).unwrap()),
                KeyPart::Null
            );
        }
    }

    #[test]
    fn packed_nullable_decimal_extremes_round_trip_without_intermediate_overflow() {
        for value in [i128::MIN, i128::MAX] {
            let mut part = PackedLeafPart::default();
            part.observe(&KeyPart::Null).unwrap();
            part.observe(&KeyPart::Decimal { value, scale: 2 }).unwrap();
            part.finish().unwrap();
            let code = part.encode(&KeyPart::Decimal { value, scale: 2 }).unwrap();
            assert_eq!(part.decode(code), KeyPart::Decimal { value, scale: 2 });
        }
    }
}

#[cfg(test)]
mod packed_leaf_tree_tests {
    use super::*;

    fn model_key(n: u32, gap: i64) -> IndexKey {
        IndexKey::from(vec![
            KeyPart::Int(i64::from(n) * 2 + gap),
            KeyPart::Int(i64::from(n % 7)),
            KeyPart::UInt(17),
            KeyPart::Date(18000),
        ])
    }

    #[test]
    fn packed_sparse_tree_writes_are_visible_before_materialization_and_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("packed-tree.idx");
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let open = |path: PathBuf, log: Arc<crate::wal::LogMutex>| {
            let pool = BufferPool::new(
                DEFAULT_BUFFER_POOL_PAGES,
                Arc::clone(&log),
                DurabilityMode::Strict,
                Duration::from_secs(5),
            );
            IndexFile::open_with_durability(path, log, DurabilityMode::Strict, pool).unwrap()
        };
        let index = open(path.clone(), Arc::clone(&log));
        let initial: Vec<_> = (0..48_000u32)
            .map(|n| IndexEntry {
                key: model_key(n, 0),
                row_id: RowId {
                    page_id: n / 32,
                    slot_id: (n % 32) as u16,
                },
            })
            .collect();
        index.bulk_load_sorted(1, initial).unwrap();
        // Direct bulk builds deliberately fill leaves. Leave genuine body
        // holes before testing sparse, nonstructural working-image writes.
        let removals = (3..48_000u32)
            .step_by(4)
            .map(|n| IndexBatchEntry {
                kind: WalIndexOpKind::Delete,
                key: model_key(n, 0),
                row_id: RowId {
                    page_id: n / 32,
                    slot_id: (n % 32) as u16,
                },
            })
            .collect();
        index.apply_batch_deferred(2, removals).unwrap();

        let incoming: Vec<_> = (100..47_900u32)
            .step_by(751)
            .enumerate()
            .map(|(i, n)| {
                (
                    model_key(n, 1),
                    RowId {
                        page_id: 1_000_000 + i as u32,
                        slot_id: 3,
                    },
                )
            })
            .collect();
        index
            .insert_batch_deferred(888, incoming.clone(), false)
            .unwrap();
        let dirty_native = || {
            index
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
                .sum::<usize>()
        };
        assert!(
            dirty_native() >= 32,
            "exercise sparse native leaves, not only fallback"
        );
        let keys: Vec<_> = incoming.iter().map(|(key, _)| key.clone()).collect();
        let matches = index
            .lookup_entries_many_sorted_keyed_bounded(&keys, true, false, keys.len())
            .unwrap()
            .unwrap();
        let expected: Vec<_> = incoming
            .iter()
            .enumerate()
            .map(|(i, (_, row))| (i as u32, *row))
            .collect();
        assert_eq!(matches, expected);
        assert!(
            dirty_native() >= 32,
            "routed reads must retain native leaves"
        );
        for (key, row_id) in &incoming {
            assert_eq!(index.get(key, true).unwrap(), vec![*row_id]);
        }
        assert!(
            dirty_native() >= 32,
            "native point reads must not force encoding"
        );
        for (key, row_id) in incoming.iter().step_by(3) {
            index.delete_deferred(889, key.clone(), *row_id).unwrap();
            assert!(index.get(key, true).unwrap().is_empty());
        }
        index.flush().unwrap();
        IndexFile::validate(path.clone()).unwrap();
        drop(index);
        let reopened = open(path, log);
        for (i, (key, row_id)) in incoming.iter().enumerate() {
            let expected = if i % 3 == 0 { vec![] } else { vec![*row_id] };
            assert_eq!(reopened.get(key, true).unwrap(), expected);
        }
        for n in [0, 100, 1234, 47_998] {
            assert_eq!(
                reopened.get(&model_key(n, 0), true).unwrap(),
                vec![RowId {
                    page_id: n / 32,
                    slot_id: (n % 32) as u16
                }]
            );
        }
    }
}

#[cfg(test)]
mod packed_gap_boundary_tests {
    use super::*;

    #[test]
    fn packed_gap_random_jumps_duplicates_and_exhaustion_match_encoded_leaf() {
        for direction in 0..3u32 {
            let make = |page_id: u32, slot_id: u16| IndexEntry {
                key: IndexKey::from(vec![KeyPart::Int(7)]),
                row_id: RowId { page_id, slot_id },
            };
            let initial: Vec<_> = (16000..16200).map(|page| make(page, 0)).collect();
            let mut bytes = encode_index_tree_page(&IndexPage::Leaf(
                IndexLeafPage::new_with_resolved_layout(initial, Some(44)),
            ))
            .unwrap();
            let mut oracle = bytes.clone();
            let mut cache = PackedNumericLeaf::build(&bytes, &make(100, 1))
                .unwrap()
                .unwrap();
            let mut exhausted = false;
            for n in 0..2200u32 {
                let page = match direction {
                    0 => (n * 7919) % 65536,
                    1 => 32000 + n,
                    _ => 15000 - n,
                };
                let incoming = make(page, (n % 1024) as u16);
                let result = cache.insert(&incoming, 20).unwrap();
                let expected = try_insert_raw_leaf(&mut oracle, &incoming, 20)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    format!("{result:?}"),
                    format!("{expected:?}"),
                    "direction={direction} n={n}"
                );
                if matches!(result, InPlaceInsert::NeedsSplit) {
                    exhausted = true;
                    break;
                }
                assert!(matches!(
                    cache.insert(&incoming, 21),
                    Some(InPlaceInsert::Duplicate)
                ));
                if n % 73 == 0 {
                    cache.materialize(&mut bytes).unwrap();
                    let IndexPage::Leaf(actual) = decode_validated_index_tree_page(&bytes).unwrap()
                    else {
                        panic!("leaf")
                    };
                    let IndexPage::Leaf(expected) =
                        decode_validated_index_tree_page(&oracle).unwrap()
                    else {
                        panic!("leaf")
                    };
                    assert_eq!(actual.entries, expected.entries);
                    assert_eq!(actual.page_max_trx_id, expected.page_max_trx_id);
                }
            }
            assert!(exhausted, "fill the native gap completely");
            cache.materialize(&mut bytes).unwrap();
            let IndexPage::Leaf(actual) = decode_validated_index_tree_page(&bytes).unwrap() else {
                panic!("leaf")
            };
            let IndexPage::Leaf(expected) = decode_validated_index_tree_page(&oracle).unwrap()
            else {
                panic!("leaf")
            };
            assert_eq!(actual.entries, expected.entries);
            assert_eq!(cache.len(), actual.entries.len());
        }
    }
}
