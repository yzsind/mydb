use crate::storage::schema::temporal_fsp_width as temporal_width;
use crate::util::decimal::wide_to_f64 as value_to_wide_f64;
// Heap row encoding/decoding.
//
// On-disk heap row payload (current MVCC_ROW_VERSION):
//   [u8: MVCC_ROW_VERSION = 9]      (0xFF means heap-forwarding stub)
//   [u8: flags]
//       bit 0  MVCC_FLAG_DELETED        (public, set by caller)
//       bit 1  MVCC_FLAG_ROW_PIECE      (public, set by caller)
//       bit 2  FLAG_HAS_LAST_TXN_ID     (codec-internal)
//       bit 3  FLAG_HAS_UNDO_PTR        (codec-internal)
//       bit 4  FLAG_HAS_EXTERNAL        (codec-internal)
//   [if FLAG_HAS_LAST_TXN_ID: u64 LE last_txn_id]
//   [if FLAG_HAS_UNDO_PTR:    u64 LE undo_ptr]
//   [varint layout_id]
//   [layout-aware value section: see encode_row_schema_into_ext]
//
// Heap value section:
//   [varint: stored_count N]
//   [ceil(N/8) bytes: NULL bitmap — bit i set => column i is NULL]
//   [if FLAG_HAS_EXTERNAL: ceil(varlen_count/8) bytes external bitmap]
//   [fixed-width block for the row layout]
//   [2B directory entry per varlen column: cumulative u16 payload end]
//   [varlen payload bytes]
//   [for each non-NULL column in the row layout:
//       if external bitmap bit i is set:
//           [20-byte LobRef descriptor]
//       else:
//           [payload encoded by schema-derived ColumnKind]
//   ]
//
// ColumnKind payloads are compact and do not carry per-value tags:
//   Int widths are 1/2/3/4/8 bytes LE; Float/Double are IEEE LE.
//   DECIMAL(P,S) is fixed-width scaled integer when precision is declared,
//   otherwise sign+nbytes+magnitude. DATE/TIME/DATETIME/YEAR are binary
//   integer encodings. VarString/VarBytes/Json/Geometry/Vector live in
//   the varlen payload, addressed by the compact directory.
//
// The tag constants below belong to the self-describing value section used by
// undo and tests (`encode_row_into`), not to normal heap row storage. Tag 0 is
// reserved there; NULLs live in the bitmap.
//
// The MVCC header trims the `last_txn_id` / `undo_ptr` u64 fields
// when they would carry zero values, shrinking the row header from
// a flat 18B in v1 to between 2B and 18B depending on the row's
// MVCC state.

const FLAG_HAS_LAST_TXN_ID: u8 = 1 << 2;
const FLAG_HAS_UNDO_PTR: u8 = 1 << 3;
/// Codec-internal: set when the value section is followed-by/preceded
/// by an "external bitmap" marking columns whose payload is a 20-byte
/// `LobRef` descriptor pointing at an off-page BLOB/TEXT chain rather
/// than inline bytes. Like `FLAG_HAS_LAST_TXN_ID`, it never reaches
/// `RowHeader.flags` (stripped by `PUBLIC_FLAG_MASK`).
const FLAG_HAS_EXTERNAL: u8 = 1 << 4;
/// Mask of flag bits that originate from the caller (`RowHeader.flags`)
/// rather than from the codec. The decoder strips codec-internal flag
/// bits before returning a `RowHeader`, so callers never see them.
const PUBLIC_FLAG_MASK: u8 = MVCC_FLAG_DELETED | MVCC_FLAG_ROW_PIECE;

// ---------------------------------------------------------------------------
// Off-page LOB materialization context
// ---------------------------------------------------------------------------
//
// Row decode is a pure, IO-free function, but an externalized column's
// bytes live in the per-table overflow file and must be fetched to
// rebuild a `Value::Bytes` / `Value::String`. Rather than thread an
// overflow-file handle through every `HeapFile` decode method (60+ call
// sites), the decode path consults a thread-local `LobMaterializer`
// installed for the duration of a read/scan by the `Database` layer —
// the same pattern `engine::cancel` / `engine::params` already use for
// cross-cutting execution context. The hot path (rows with no external
// columns) never touches it: the `FLAG_HAS_EXTERNAL` bit is unset, so
// the external bitmap is absent and the materializer is never consulted.

pub(crate) trait LobMaterializer: Send + Sync + std::fmt::Debug {
    /// Turn an externalized descriptor into a materialized value of the
    /// column's kind (VarString → `Value::String`, else `Value::Bytes`).
    /// `col_pos` is the column's physical position, used by the
    /// descriptor-collecting reader (rollback / undo capture) to record
    /// which column a chain belongs to without fetching its bytes.
    fn materialize(
        &self,
        col_pos: usize,
        kind: &ColumnKind,
        lob: LobRef,
    ) -> Result<Value, QueryError>;

    /// M2 (partial materialization) — returns the externalized column's byte length, **reading no overflow
    /// pages** (takes `LobRef.total_len` directly). `LENGTH/OCTET_LENGTH` push down to this.
    /// The default implementation just returns `total_len`, correct for every implementation (total_len is a byte count
    /// for VarString/VarBytes/Json/Geometry/Vector alike).
    // SQL-layer LENGTH pushdown (planner→scan) is the next wave; currently driven by Database's
    // `lob_column_byte_length` + tests; not called directly by lib non-test code yet.
    #[cfg(test)]
    fn materialize_len(&self, _col_pos: usize, lob: LobRef) -> Result<u64, QueryError> {
        Ok(lob.total_len)
    }

    /// M2 (partial materialization) — materializes only the externalized column's `[offset, offset+len)` byte range
    /// into a Value of the corresponding kind (via `read_chain_range`, skipping the pages outside the range).
    /// `SUBSTRING/LEFT/RIGHT` push down to this. The default implementation falls back to full-chain materialization plus slicing
    /// (semantically correct but saves no IO); descriptor-collecting materializers can just use it.
    #[allow(dead_code)]
    fn materialize_range(
        &self,
        col_pos: usize,
        kind: &ColumnKind,
        lob: LobRef,
        offset: u64,
        len: u64,
    ) -> Result<Value, QueryError> {
        // Default: full-chain materialization → byte slicing. Concrete implementations (OverflowReader) override
        // this with read_chain_range to save IO.
        let full = self.materialize(col_pos, kind, lob)?;
        Ok(slice_value_bytes(&full, offset, len))
    }
}

/// M2 (partial materialization) — slices an already-materialized LOB Value by the byte range [offset,
/// offset+len) into a new Value of the same kind (used by materialize_range's default
/// implementation / the differential oracle). The range is clamped to the value's byte length.
#[allow(dead_code)]
pub(crate) fn slice_value_bytes(value: &Value, offset: u64, len: u64) -> Value {
    let take = |bytes: &[u8]| -> Vec<u8> {
        let total = bytes.len() as u64;
        if offset >= total || len == 0 {
            return Vec::new();
        }
        let start = offset as usize;
        let end = offset.saturating_add(len).min(total) as usize;
        bytes[start..end].to_vec()
    };
    match value {
        Value::String(s) => {
            let sub = take(s.as_bytes());
            // Byte slicing may cut a multi-byte character in half; the byte semantics of LOB text slicing are the
            // caller's responsibility (the byte path of pushed-down SUBSTRING); lossy is the fallback here.
            Value::String(String::from_utf8_lossy(&sub).into_owned())
        }
        Value::Bytes(b) => Value::Bytes(take(b)),
        Value::Json(b) => Value::Json(take(b)),
        Value::Geometry(b) => Value::Geometry(take(b)),
        Value::Vector(b) => Value::Bytes(take(b)),
        Value::Null => Value::Null,
        other => other.clone(),
    }
}

thread_local! {
    static LOB_MATERIALIZER: std::cell::RefCell<Option<std::sync::Arc<dyn LobMaterializer>>> =
        std::cell::RefCell::new(None);
}

/// Install `materializer` as the current thread's LOB reader for the
/// duration of `f`, restoring the previous one on the way out (so
/// nested scans compose). Pass `None` to explicitly run without one.
pub(crate) fn with_lob_materializer<F, R>(
    materializer: Option<std::sync::Arc<dyn LobMaterializer>>,
    f: F,
) -> R
where
    F: FnOnce() -> R,
{
    struct Reset(Option<std::sync::Arc<dyn LobMaterializer>>);
    impl Drop for Reset {
        fn drop(&mut self) {
            let previous = self.0.take();
            LOB_MATERIALIZER.with(|cell| *cell.borrow_mut() = previous);
        }
    }
    let previous = LOB_MATERIALIZER.with(|cell| cell.replace(materializer));
    let _reset = Reset(previous);
    f()
}

fn materialize_external(
    col_pos: usize,
    kind: &ColumnKind,
    lob: LobRef,
) -> Result<Value, QueryError> {
    LOB_MATERIALIZER.with(|cell| match cell.borrow().as_ref() {
        Some(m) => m.materialize(col_pos, kind, lob),
        None => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "externalized LOB column decoded without an overflow reader installed",
        )),
    })
}

#[inline]
fn external_bit(bitmap: &[u8], idx: usize) -> bool {
    bitmap
        .get(idx >> 3)
        .is_some_and(|byte| byte & (1u8 << (idx & 7)) != 0)
}

/// Encode a heap row payload for a freshly-inserted, MVCC-visible row.
/// Wrapper around `encode_mvcc_row` for callers that don't carry a
/// `RowHeader` of their own — currently the parallel LOAD DATA worker
/// thread, which prepares row bytes off the single page-insert/WAL
/// thread. The output is bit-for-bit identical to what the writer
/// thread would produce on its own, since `RowHeader::visible(txn_id)`
/// fully determines the on-disk header layout for a row that hasn't
/// been chained to an undo record yet.
pub(crate) fn encode_visible_row_payload(
    values: &[Value],
    codec: &RowCodec,
    txn_id: u64,
) -> Result<Vec<u8>, QueryError> {
    encode_mvcc_row(RowHeader::visible(txn_id), values, codec, &[])
}

/// Immutable schema program for identity-layout LOAD DATA rows. A worker
/// compiles this once and then streams one already-coerced field at a time
/// into its final MVCC payload, avoiding the intermediate `Vec<Value>` and
/// the second full-row codec walk.
#[derive(Clone)]
pub(crate) struct CompiledDirectRowEncoder {
    layout_id: u32,
    layout: RowLayoutCodec,
}

impl CompiledDirectRowEncoder {
    pub(crate) fn compile(codec: &RowCodec) -> Option<Self> {
        let layout = codec.active_layout();
        if layout
            .columns
            .iter()
            .enumerate()
            .any(|(idx, column)| column.column_index != idx)
        {
            return None;
        }
        Some(Self {
            layout_id: codec.active_layout_id,
            layout: layout.clone(),
        })
    }

    pub(crate) fn begin_row(
        &self,
        txn_id: u64,
        variable_bytes_hint: usize,
    ) -> DirectRowPayloadBuilder<'_> {
        let count = self.layout.columns.len();
        let value_layout = &self.layout.value_layout;
        let header = RowHeader::visible(txn_id);
        let fixed_overhead = mvcc_header_len(&header)
            .saturating_add(10)
            .saturating_add(count.div_ceil(8))
            .saturating_add(value_layout.fixed_len)
            .saturating_add(value_layout.var_count.saturating_mul(2));
        let mut out = Vec::with_capacity(fixed_overhead.saturating_add(variable_bytes_hint));
        write_mvcc_header(&mut out, header, false);
        write_varint(&mut out, self.layout_id as u64);
        write_varint(&mut out, count as u64);
        let null_bitmap_start = out.len();
        out.resize(null_bitmap_start + count.div_ceil(8), 0);
        let fixed_start = out.len();
        out.resize(fixed_start + value_layout.fixed_len, 0);
        let dir_start = out.len();
        out.resize(dir_start + value_layout.var_count * 2, 0);
        let payload_base = out.len();
        DirectRowPayloadBuilder {
            program: self,
            out,
            next_column: 0,
            null_bitmap_start,
            fixed_start,
            dir_start,
            payload_base,
        }
    }
}

pub(crate) struct DirectRowPayloadBuilder<'a> {
    program: &'a CompiledDirectRowEncoder,
    out: Vec<u8>,
    next_column: usize,
    null_bitmap_start: usize,
    fixed_start: usize,
    dir_start: usize,
    payload_base: usize,
}

impl DirectRowPayloadBuilder<'_> {
    #[inline]
    fn next_fixed_slot(&mut self) -> Result<(&ColumnKind, &mut [u8]), QueryError> {
        let idx = self.next_column;
        let column = self.program.layout.columns.get(idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct row encoder received too many fields",
            )
        })?;
        let layout = &self.program.layout.value_layout;
        let fixed_off = layout.fixed_offsets[idx].ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct row encoder expected a fixed-width field",
            )
        })?;
        let width = fixed_width_for_kind(&column.kind).expect("fixed slot has fixed kind");
        self.next_column += 1;
        Ok((
            &column.kind,
            &mut self.out[self.fixed_start + fixed_off..self.fixed_start + fixed_off + width],
        ))
    }

    /// Typed entry points used by `CompiledLoadRowProgram`.  These write the
    /// already-validated scalar straight into its schema-compiled slot.  The
    /// generic `push_value` remains the byte-for-byte semantic oracle for
    /// uncommon lexical forms and unsupported types.
    #[inline]
    pub(crate) fn push_i64(&mut self, value: i64) -> Result<(), QueryError> {
        let (kind, out) = self.next_fixed_slot()?;
        let ColumnKind::Int { signed: true, .. } = kind else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct signed integer field did not match row layout",
            ));
        };
        write_signed_le_width(value, out)
    }

    #[inline]
    pub(crate) fn push_u64(&mut self, value: u64) -> Result<(), QueryError> {
        let (kind, out) = self.next_fixed_slot()?;
        let ColumnKind::Int { signed: false, .. } = kind else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct unsigned integer field did not match row layout",
            ));
        };
        write_unsigned_le_width(value, out)
    }

    #[inline]
    pub(crate) fn push_decimal_compact(
        &mut self,
        magnitude: i64,
        scale: u8,
    ) -> Result<(), QueryError> {
        let (kind, out) = self.next_fixed_slot()?;
        let ColumnKind::DecimalFixed {
            width,
            scale: layout_scale,
        } = kind
        else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct decimal field did not match row layout",
            ));
        };
        if *layout_scale != scale {
            return Err(error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "direct decimal scale did not match row layout",
            ));
        }
        match *width {
            4 => out.copy_from_slice(
                &i32::try_from(magnitude)
                    .map_err(|_| {
                        error(
                            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                            "decimal value out of range for DECIMAL(P≤9) column",
                        )
                    })?
                    .to_le_bytes(),
            ),
            8 => out.copy_from_slice(&magnitude.to_le_bytes()),
            16 => out.copy_from_slice(&(magnitude as i128).to_le_bytes()),
            _ => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid fixed decimal width",
                ));
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn push_date_days(&mut self, days: i32) -> Result<(), QueryError> {
        let (kind, out) = self.next_fixed_slot()?;
        if !matches!(kind, ColumnKind::Date) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct date field did not match row layout",
            ));
        }
        let stored = if days == ZERO_DATE_SENTINEL {
            signed_min_for_width(out.len())
        } else {
            i64::from(days)
        };
        write_signed_le_width(stored, out)
    }

    #[inline]
    pub(crate) fn push_string_bytes(&mut self, value: &[u8]) -> Result<(), QueryError> {
        let idx = self.next_column;
        let column = self.program.layout.columns.get(idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct row encoder received too many fields",
            )
        })?;
        if !matches!(column.kind, ColumnKind::VarString) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct string field did not match row layout",
            ));
        }
        let layout = &self.program.layout.value_layout;
        let var_ord = layout.var_ords[idx].expect("variable column has directory ordinal");
        let offset = (self.out.len() - self.payload_base) as u32;
        self.out.extend_from_slice(value);
        write_dir_entry(
            &mut self.out,
            self.dir_start,
            var_ord,
            offset,
            value.len() as u32,
        )?;
        self.next_column += 1;
        Ok(())
    }

    pub(crate) fn push_value(&mut self, value: &Value) -> Result<(), QueryError> {
        let idx = self.next_column;
        let column = self.program.layout.columns.get(idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct row encoder received too many fields",
            )
        })?;
        let layout = &self.program.layout.value_layout;
        if matches!(value, Value::Null) {
            self.out[self.null_bitmap_start + (idx >> 3)] |= 1u8 << (idx & 7);
            if let Some(var_ord) = layout.var_ords[idx] {
                let offset = (self.out.len() - self.payload_base) as u32;
                write_dir_entry(&mut self.out, self.dir_start, var_ord, offset, 0)?;
            }
        } else if let Some(fixed_off) = layout.fixed_offsets[idx] {
            let width = fixed_width_for_kind(&column.kind).expect("fixed offset has fixed kind");
            encode_fixed_for_kind(
                &column.kind,
                value,
                &mut self.out[self.fixed_start + fixed_off..self.fixed_start + fixed_off + width],
            )?;
        } else {
            let var_ord = layout.var_ords[idx].expect("variable column has directory ordinal");
            let offset = (self.out.len() - self.payload_base) as u32;
            encode_var_for_kind(&column.kind, value, &mut self.out)?;
            let len = (self.out.len() - self.payload_base)
                .checked_sub(offset as usize)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "row is too large",
                    )
                })?;
            write_dir_entry(&mut self.out, self.dir_start, var_ord, offset, len as u32)?;
        }
        self.next_column += 1;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<u8>, QueryError> {
        if self.next_column != self.program.layout.columns.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "direct row encoder received too few fields",
            ));
        }
        Ok(self.out)
    }
}

/// Re-encode an ordinary inline row while copying byte-identical columns
/// directly from the source value section. `rewrite_mask[i]` selects columns
/// whose target encoding must come from `values`; every other column is
/// copied when the persisted source and target codec kinds match. This keeps
/// ALTER COPY generic while avoiding decode→encode work for wide unchanged
/// columns.
pub(crate) fn transcode_visible_row_payload(
    source_payload: &[u8],
    source_codec: &RowCodec,
    target_codec: &RowCodec,
    values: &[Value],
    rewrite_mask: &[bool],
    txn_id: u64,
) -> Result<Vec<u8>, QueryError> {
    if source_payload.first().copied() != Some(MVCC_ROW_VERSION) {
        return encode_visible_row_payload(values, target_codec, txn_id);
    }
    let (source_header, mut source_body_offset, has_external) = read_mvcc_header(source_payload)?;
    if has_external || source_header.flags & MVCC_FLAG_ROW_PIECE != 0 {
        return encode_visible_row_payload(values, target_codec, txn_id);
    }
    let source_layout_id = read_varint(source_payload, &mut source_body_offset)? as u32;
    let source_body = &source_payload[source_body_offset..];
    let mut source_pos = 0usize;
    let source_count = read_varint(source_body, &mut source_pos)? as usize;
    let source_layout = source_codec.layout_for_id(source_layout_id)?;
    if source_count > source_layout.columns.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "row payload has more columns than its layout",
        ));
    }
    let source_columns = &source_layout.columns[..source_count];
    let source_value_layout = value_section_layout_for_prefix(source_layout, source_columns);
    let source_null_start = source_pos;
    let source_fixed_start = source_null_start + source_count.div_ceil(8);
    let source_dir_start = source_fixed_start + source_value_layout.fixed_len;
    let source_payload_start = source_dir_start + source_value_layout.var_count * 2;
    if source_payload_start > source_body.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }

    let target_layout = target_codec.active_layout();
    let target_count = target_layout.columns.len();
    let target_values = &target_layout.value_layout;
    let mut out = Vec::with_capacity(source_payload.len().saturating_add(32));
    write_mvcc_header(&mut out, RowHeader::visible(txn_id), false);
    write_varint(&mut out, target_codec.active_layout_id as u64);
    write_varint(&mut out, target_count as u64);
    let target_null_start = out.len();
    out.resize(target_null_start + target_count.div_ceil(8), 0);
    let target_fixed_start = out.len();
    out.resize(target_fixed_start + target_values.fixed_len, 0);
    let target_dir_start = out.len();
    out.resize(target_dir_start + target_values.var_count * 2, 0);
    let target_payload_start = out.len();

    for (target_ord, target_column) in target_layout.columns.iter().enumerate() {
        let column_index = target_column.column_index;
        let source_ord = source_columns.iter().position(|column| {
            column.column_index == column_index && column.kind == target_column.kind
        });
        let copy_source = !rewrite_mask.get(column_index).copied().unwrap_or(true);
        if copy_source {
            if let Some(source_ord) = source_ord {
                let source_is_null = source_body[source_null_start + (source_ord >> 3)]
                    & (1_u8 << (source_ord & 7))
                    != 0;
                if source_is_null {
                    out[target_null_start + (target_ord >> 3)] |= 1_u8 << (target_ord & 7);
                    if let Some(var_ord) = target_values.var_ords[target_ord] {
                        let offset = (out.len() - target_payload_start) as u32;
                        write_dir_entry(&mut out, target_dir_start, var_ord, offset, 0)?;
                    }
                    continue;
                }
                if let (Some(source_fixed), Some(target_fixed)) = (
                    source_value_layout.fixed_offsets[source_ord],
                    target_values.fixed_offsets[target_ord],
                ) {
                    let width = fixed_width_for_kind(&target_column.kind)
                        .expect("fixed layout entry must have a width");
                    let source_start = source_fixed_start + source_fixed;
                    let source_end = source_start + width;
                    if source_end > source_body.len() {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "corrupt row payload",
                        ));
                    }
                    out[target_fixed_start + target_fixed
                        ..target_fixed_start + target_fixed + width]
                        .copy_from_slice(&source_body[source_start..source_end]);
                    continue;
                }
                if let (Some(source_var), Some(target_var)) = (
                    source_value_layout.var_ords[source_ord],
                    target_values.var_ords[target_ord],
                ) {
                    let (offset, len) = read_dir_entry(
                        source_body,
                        source_dir_start,
                        source_payload_start,
                        source_var,
                    )?;
                    let bytes =
                        checked_payload_slice(source_body, source_payload_start, offset, len)?;
                    let target_offset = (out.len() - target_payload_start) as u32;
                    out.extend_from_slice(bytes);
                    write_dir_entry(&mut out, target_dir_start, target_var, target_offset, len)?;
                    continue;
                }
            }
        }

        let value = values.get(column_index).unwrap_or(&Value::Null);
        if matches!(value, Value::Null) {
            out[target_null_start + (target_ord >> 3)] |= 1_u8 << (target_ord & 7);
            if let Some(var_ord) = target_values.var_ords[target_ord] {
                let offset = (out.len() - target_payload_start) as u32;
                write_dir_entry(&mut out, target_dir_start, var_ord, offset, 0)?;
            }
        } else if let Some(fixed) = target_values.fixed_offsets[target_ord] {
            let width = fixed_width_for_kind(&target_column.kind)
                .expect("fixed layout entry must have a width");
            encode_fixed_for_kind(
                &target_column.kind,
                value,
                &mut out[target_fixed_start + fixed..target_fixed_start + fixed + width],
            )?;
        } else {
            let var_ord = target_values.var_ords[target_ord]
                .expect("variable layout entry must have an ordinal");
            let offset = (out.len() - target_payload_start) as u32;
            encode_var_for_kind(&target_column.kind, value, &mut out)?;
            let len = (out.len() - target_payload_start) as u32 - offset;
            write_dir_entry(&mut out, target_dir_start, var_ord, offset, len)?;
        }
    }
    Ok(out)
}

fn encode_mvcc_row(
    header: RowHeader,
    values: &[Value],
    codec: &RowCodec,
    external: &[Option<LobRef>],
) -> Result<Vec<u8>, QueryError> {
    // Quick value-kind aware capacity estimate. The previous
    // `values.len() * 8` was always wrong for any row with a varchar
    // column — profiles showed `RawVec::finish_grow` at ~3 % of CPU
    // for bulk INSERT workloads, all of it from this same Vec
    // amortised-doubling its way from 8 B/col to the actual width.
    // Scanning `values` byte-cheaply for variable-length payloads
    // catches the common case without paying for a full encode pass.
    let has_external = external.iter().any(|e| e.is_some());
    let mut value_bytes_est: usize = 0;
    for value in values {
        // Conservative upper-bound: header byte(s) for the value kind,
        // plus payload bytes for the variable-length payload (string
        // bytes are stored verbatim, Decimal text fits in its len),
        // plus a small per-value framing slack. The codec's actual
        // emitted size is always ≤ this number for the value shapes
        // we measure, so the `Vec::push` chain never reallocates.
        value_bytes_est += match value {
            Value::Null => 1,
            Value::Int(_) | Value::UInt(_) | Value::Double(_) | Value::Float(_) => 9,
            Value::DecimalCompact(_, _) => 11,
            Value::Decimal128(_, _) => 19,
            Value::DecimalWide(_) => 8 + value.decimal_heap_bytes(),
            Value::Date(_)
            | Value::Time(_)
            | Value::DateTime(_)
            | Value::Timestamp(_)
            | Value::Year(_) => 9,
            Value::String(s) | Value::Decimal(s) => s.len() + 5,
            Value::SmallString(b) => b.len() + 5,
            Value::Bytes(b) => b.len() + 5,
            Value::Json(b) | Value::Geometry(b) => b.len() + 5,
            Value::Vector(b) => b.len() + 17,
        };
    }
    let estimated = mvcc_header_len(&header) + 3 + value_bytes_est;
    let mut out = Vec::with_capacity(estimated);
    write_mvcc_header(&mut out, header, has_external);
    write_varint(&mut out, codec.active_layout_id as u64);
    encode_row_schema_into_ext(values, codec, external, &mut out)?;
    Ok(out)
}

/// Turn a heap row payload into a "row piece" by setting the
/// `MVCC_FLAG_ROW_PIECE` flag bit in place — a pure byte edit of the
/// header's flags byte, leaving the value section (including any
/// external bitmap / `LobRef` descriptors) untouched, so a migrated row
/// keeps pointing at the same overflow chains.
fn make_heap_row_piece_payload(payload: &[u8], _codec: &RowCodec) -> Result<Vec<u8>, QueryError> {
    if payload.len() < 2 || payload.first().copied() != Some(MVCC_ROW_VERSION) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row header",
        ));
    }
    let mut out = payload.to_vec();
    out[1] |= MVCC_FLAG_ROW_PIECE;
    Ok(out)
}

/// Heap forwarding stub: `[u8 version][u32 page_id][u16 slot]`
/// = 7 bytes. Was 9 B before the rowid-6bytes refactor; the per-slot
/// `generation` ABA counter is gone. Forwarding chains stay within a
/// single table (heap files are per-table), so `table_id` is not
/// carried in the stub.
fn encode_heap_forwarding_stub(row_id: RowId) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEAP_FORWARDING_STUB_LEN);
    out.push(HEAP_FORWARDING_STUB_VERSION);
    out.extend_from_slice(&row_id.page_id.to_le_bytes());
    out.extend_from_slice(&row_id.slot_id.to_le_bytes());
    out
}

#[inline(always)]
fn decode_heap_forwarding_stub(input: &[u8]) -> Result<Option<RowId>, QueryError> {
    if input.first().copied() != Some(HEAP_FORWARDING_STUB_VERSION) {
        return Ok(None);
    }
    if input.len() != HEAP_FORWARDING_STUB_LEN {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt heap forwarding stub",
        ));
    }
    let mut offset = 1;
    Ok(Some(RowId {
        page_id: read_u32_checked(input, &mut offset)?,
        slot_id: read_u16_checked(input, &mut offset)?,
    }))
}

/// Compute the byte length of the MVCC header that will be emitted for
/// `header`. Used for buffer pre-sizing and for skipping past the
/// header during decode.
fn mvcc_header_len(header: &RowHeader) -> usize {
    let mut len = 2; // version + flags
    if header.last_txn_id != 0 {
        len += 8;
    }
    if header.undo_ptr.is_some() {
        len += 8;
    }
    len
}

/// Encode the MVCC header into `out`. The public flag bits in
/// `header.flags` are preserved verbatim; `FLAG_HAS_LAST_TXN_ID` /
/// `FLAG_HAS_UNDO_PTR` are added by the codec to record which optional
/// u64 fields follow.
fn write_mvcc_header(out: &mut Vec<u8>, header: RowHeader, has_external: bool) {
    let mut eff_flags = header.flags & PUBLIC_FLAG_MASK;
    if header.last_txn_id != 0 {
        eff_flags |= FLAG_HAS_LAST_TXN_ID;
    }
    if header.undo_ptr.is_some() {
        eff_flags |= FLAG_HAS_UNDO_PTR;
    }
    if has_external {
        eff_flags |= FLAG_HAS_EXTERNAL;
    }
    out.push(MVCC_ROW_VERSION);
    out.push(eff_flags);
    if header.last_txn_id != 0 {
        out.extend_from_slice(&header.last_txn_id.to_le_bytes());
    }
    if let Some(ptr) = header.undo_ptr {
        out.extend_from_slice(&ptr.to_le_bytes());
    }
}

/// Read the MVCC header starting at byte 0 of `input`. Returns the
/// parsed header (with codec-internal flag bits masked out), the byte
/// offset past the header at which the value section begins, and
/// whether the value section carries an external-column bitmap
/// (`FLAG_HAS_EXTERNAL`).
/// Public peek helper: same semantics as the internal `read_mvcc_header`
/// but returns `Option<...>` for the corrupt-bytes case so the raw
/// payload scan loop can skip rather than abort. Used by
/// `HeapFile::scan_mvcc_payloads_range_until_with_policy`.
pub(crate) fn peek_mvcc_header(input: &[u8]) -> Option<(RowHeader, usize, bool)> {
    read_mvcc_header(input).ok()
}

/// Public alias for the codec's internal `read_varint`, exposed so the
/// raw-payload scan path can decode the layout-id varint without
/// re-routing through a full payload decoder.
pub(crate) fn read_varint_at(input: &[u8], pos: &mut usize) -> Result<u64, QueryError> {
    read_varint(input, pos)
}

/// Forwarding-stub probe helper. Raw-payload scans must follow these
/// logical anchors; physical row pieces are excluded from page iteration.
#[inline(always)]
pub(crate) fn decode_heap_forwarding_stub_pub(input: &[u8]) -> Result<Option<RowId>, QueryError> {
    decode_heap_forwarding_stub(input)
}

pub(crate) const MVCC_FLAG_ROW_PIECE_PUB: u8 = MVCC_FLAG_ROW_PIECE;

#[inline(always)]
fn read_mvcc_header(input: &[u8]) -> Result<(RowHeader, usize, bool), QueryError> {
    if input.len() < 2 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row header",
        ));
    }
    let raw_flags = input[1];
    if raw_flags
        & !(PUBLIC_FLAG_MASK | FLAG_HAS_LAST_TXN_ID | FLAG_HAS_UNDO_PTR | FLAG_HAS_EXTERNAL)
        != 0
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unsupported row flags",
        ));
    }
    let mut pos = 2usize;
    let last_txn_id = if raw_flags & FLAG_HAS_LAST_TXN_ID != 0 {
        let bytes = read_exact(input, &mut pos, 8)?;
        u64::from_le_bytes(bytes.try_into().unwrap())
    } else {
        0
    };
    let undo_ptr = if raw_flags & FLAG_HAS_UNDO_PTR != 0 {
        let bytes = read_exact(input, &mut pos, 8)?;
        let raw = u64::from_le_bytes(bytes.try_into().unwrap());
        // `RowHeader::visible` reserves the on-disk undo_ptr slot as 8
        // zero bytes so first-UPDATE can take the in-place patch fast
        // path. `0` is a permanent sentinel — real undo ids start at
        // 1 — so we map it back to `None` here for the visibility
        // walker. The flag bit and the 8 reserved bytes stay on disk
        // so `try_write_mvcc_patched` still recognises the slot.
        if raw == 0 {
            None
        } else {
            Some(raw)
        }
    } else {
        None
    };
    Ok((
        RowHeader {
            flags: raw_flags & PUBLIC_FLAG_MASK,
            last_txn_id,
            undo_ptr,
        },
        pos,
        raw_flags & FLAG_HAS_EXTERNAL != 0,
    ))
}

/// Outcome of decoding only the MVCC header of a heap payload —
/// without paying for the per-value decode pass that
/// `decode_heap_payload` would perform. Used by covering / index-only
/// scans that need visibility information but never look at the row's
/// values, since those live in the index entry.
enum HeapPayloadHeader {
    Row(RowHeader),
    /// Forward stub: the live row was migrated to another page. Caller
    /// must follow the stub via the slow path.
    Forward,
    /// Multi-page row piece header — caller must follow forwarding to
    /// reconstruct the full row before deciding visibility.
    RowPiece,
}

/// Read just the row header from a heap payload slice. Skipping the
/// value decode is the bulk of the per-row work for a covering scan
/// over a wide table.
#[inline]
fn decode_heap_payload_header(input: &[u8]) -> Result<HeapPayloadHeader, QueryError> {
    if input.first().copied() == Some(HEAP_FORWARDING_STUB_VERSION) {
        return Ok(HeapPayloadHeader::Forward);
    }
    if input.first().copied() == Some(MVCC_ROW_VERSION) {
        // Fresh rows reserve both u64 slots, and updated rows keep them, so
        // the overwhelmingly common visibility-probe header is the fixed
        // 18-byte form. Decode it directly instead of walking two
        // `read_exact`/slice-conversion branches for every index candidate.
        // The variable-width decoder below remains the oracle for compact
        // headers and every malformed/truncated input.
        if input.len() >= 18 {
            let raw_flags = input[1];
            if raw_flags & (FLAG_HAS_LAST_TXN_ID | FLAG_HAS_UNDO_PTR)
                == FLAG_HAS_LAST_TXN_ID | FLAG_HAS_UNDO_PTR
            {
                let last_txn_id =
                    u64::from_le_bytes(input[2..10].try_into().expect("fixed-size slice"));
                let raw_undo =
                    u64::from_le_bytes(input[10..18].try_into().expect("fixed-size slice"));
                let header = RowHeader {
                    flags: raw_flags & PUBLIC_FLAG_MASK,
                    last_txn_id,
                    undo_ptr: (raw_undo != 0).then_some(raw_undo),
                };
                if header.flags & MVCC_FLAG_ROW_PIECE != 0 {
                    return Ok(HeapPayloadHeader::RowPiece);
                }
                return Ok(HeapPayloadHeader::Row(header));
            }
        }
        let (header, _, _) = read_mvcc_header(input)?;
        if header.flags & MVCC_FLAG_ROW_PIECE != 0 {
            return Ok(HeapPayloadHeader::RowPiece);
        }
        return Ok(HeapPayloadHeader::Row(header));
    }
    Err(error(
        crate::error::catalog::ER_GET_ERRNO,
        "corrupt row header",
    ))
}

pub(crate) fn heap_payload_counts_as_logical_row(input: &[u8]) -> bool {
    matches!(
        decode_heap_payload_header(input),
        Ok(HeapPayloadHeader::Row(_) | HeapPayloadHeader::Forward)
    )
}

pub(crate) fn heap_payload_live_logical_state(input: &[u8]) -> Option<bool> {
    match decode_heap_payload_header(input) {
        Ok(HeapPayloadHeader::Row(header)) => Some(!header.is_deleted()),
        Ok(HeapPayloadHeader::Forward) => Some(true),
        Ok(HeapPayloadHeader::RowPiece) | Err(_) => None,
    }
}

#[cfg(test)]
mod heap_payload_header_fast_path_tests {
    use super::*;

    #[test]
    fn fixed_header_fast_path_matches_full_decoder() {
        for header in [
            RowHeader::visible(42),
            RowHeader {
                flags: MVCC_FLAG_DELETED,
                last_txn_id: 99,
                undo_ptr: Some(7),
            },
        ] {
            let payload = encode_mvcc_row(header, &[], &RowCodec::empty(), &[]).unwrap();
            let (expected, _, _) = read_mvcc_header(&payload).unwrap();
            match decode_heap_payload_header(&payload).unwrap() {
                HeapPayloadHeader::Row(actual) => assert_eq!(actual, expected),
                HeapPayloadHeader::Forward | HeapPayloadHeader::RowPiece => {
                    panic!("expected ordinary row header")
                }
            }
        }
    }

    #[test]
    fn row_piece_and_variable_headers_keep_existing_semantics() {
        let piece = RowHeader {
            flags: MVCC_FLAG_ROW_PIECE,
            last_txn_id: 5,
            undo_ptr: Some(3),
        };
        let payload = encode_mvcc_row(piece, &[], &RowCodec::empty(), &[]).unwrap();
        assert!(matches!(
            decode_heap_payload_header(&payload).unwrap(),
            HeapPayloadHeader::RowPiece
        ));

        let compact = encode_mvcc_row(
            RowHeader {
                flags: 0,
                last_txn_id: 0,
                undo_ptr: None,
            },
            &[],
            &RowCodec::empty(),
            &[],
        )
        .unwrap();
        assert!(matches!(
            decode_heap_payload_header(&compact).unwrap(),
            HeapPayloadHeader::Row(RowHeader {
                last_txn_id: 0,
                undo_ptr: None,
                ..
            })
        ));
        assert!(decode_heap_payload_header(&[MVCC_ROW_VERSION, FLAG_HAS_LAST_TXN_ID]).is_err());
    }
}

fn decode_heap_payload(input: &[u8], codec: &RowCodec) -> Result<HeapPayload, QueryError> {
    if let Some(row_id) = decode_heap_forwarding_stub(input)? {
        return Ok(HeapPayload::Forward(row_id));
    }
    let (header, values) = decode_mvcc_row(input, codec)?;
    if header.flags & MVCC_FLAG_ROW_PIECE != 0 {
        Ok(HeapPayload::RowPiece(header, values))
    } else {
        Ok(HeapPayload::Row(header, values))
    }
}

fn decode_heap_payload_columns(
    input: &[u8],
    columns: &[usize],
    codec: &RowCodec,
) -> Result<HeapPayload, QueryError> {
    if let Some(row_id) = decode_heap_forwarding_stub(input)? {
        return Ok(HeapPayload::Forward(row_id));
    }
    let (header, values, stored_count) = decode_mvcc_row_columns(input, columns, codec)?;
    if header.flags & MVCC_FLAG_ROW_PIECE != 0 {
        Ok(HeapPayload::RowPieceColumns(header, values, stored_count))
    } else {
        Ok(HeapPayload::RowColumns(header, values, stored_count))
    }
}

fn decode_heap_payload_columns_with_plan(
    input: &[u8],
    plan: &RowDecodePlan,
    codec: &RowCodec,
) -> Result<HeapPayload, QueryError> {
    if let Some(row_id) = decode_heap_forwarding_stub(input)? {
        return Ok(HeapPayload::Forward(row_id));
    }
    if input.first().copied() != Some(MVCC_ROW_VERSION) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row header",
        ));
    }
    let (header, mut body_offset, has_external) = read_mvcc_header(input)?;
    let layout_id = read_varint(input, &mut body_offset)? as u32;
    let mut values = Vec::with_capacity(plan.out_len);
    let stored_count = decode_row_schema_columns_into_with_plan_ext_for_layout(
        &input[body_offset..],
        codec,
        layout_id,
        plan,
        &mut values,
        has_external,
    )?;
    if header.flags & MVCC_FLAG_ROW_PIECE != 0 {
        Ok(HeapPayload::RowPieceColumns(header, values, stored_count))
    } else {
        Ok(HeapPayload::RowColumns(header, values, stored_count))
    }
}

/// Buffer-reusing variant of `decode_heap_payload_columns`. Returns
/// `Ok(Some(...))` on a regular row payload (with `out` filled),
/// `Ok(None)` for forwarding stubs (caller must follow), and an error
/// on corrupt payloads. Multi-page row pieces and full-row payloads
/// are skipped silently (caller treats them as "no projected row" so
/// the streaming scan continues), matching the legacy match arms.
pub(crate) enum HeapPayloadOutcome {
    /// Successfully decoded row. `stored_count` is the number of values
    /// physically encoded in the heap payload (`row.values.len()` if a
    /// full-row decode had been performed). When a projected `columns[i]`
    /// is `>= stored_count` the corresponding `out` slot is filled with
    /// `Value::Null`; the caller is expected to compare against the
    /// catalog's `physical_pos` and substitute the column DEFAULT
    /// (rows written before an INSTANT ADD COLUMN do not carry the new
    /// slot).
    Row(RowHeader, usize),
    /// Forwarded row stub; the live row lives elsewhere. The `RowId`
    /// payload is currently unused by the streaming scan caller (which
    /// already has its own row id in hand and re-resolves the live
    /// version via `read_mvcc_columns_following`), but is kept for
    /// parity with the legacy `HeapPayload::Forward` shape so future
    /// callers don't need to re-decode the stub.
    Forward(#[allow(dead_code)] RowId),
    Skip,
}

pub(crate) fn decode_heap_payload_columns_into_with_plan(
    input: &[u8],
    plan: &RowDecodePlan,
    codec: &RowCodec,
    out: &mut Vec<Value>,
) -> Result<HeapPayloadOutcome, QueryError> {
    if let Some(row_id) = decode_heap_forwarding_stub(input)? {
        out.clear();
        return Ok(HeapPayloadOutcome::Forward(row_id));
    }
    if input.first().copied() != Some(MVCC_ROW_VERSION) {
        out.clear();
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row header",
        ));
    }
    let (header, mut body_offset, has_external) = read_mvcc_header(input)?;
    let layout_id = read_varint(input, &mut body_offset)? as u32;
    if header.flags & MVCC_FLAG_ROW_PIECE != 0 {
        out.clear();
        return Ok(HeapPayloadOutcome::Skip);
    }
    let stored_count = decode_row_schema_columns_into_with_plan_ext_for_layout(
        &input[body_offset..],
        codec,
        layout_id,
        plan,
        out,
        has_external,
    )?;
    Ok(HeapPayloadOutcome::Row(header, stored_count))
}

fn decode_mvcc_row(input: &[u8], codec: &RowCodec) -> Result<(RowHeader, Vec<Value>), QueryError> {
    if input.first().copied() != Some(MVCC_ROW_VERSION) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row header",
        ));
    }
    let (header, mut body_offset, has_external) = read_mvcc_header(input)?;
    let layout_id = read_varint(input, &mut body_offset)? as u32;
    Ok((
        header,
        decode_row_schema_ext_for_layout(&input[body_offset..], codec, layout_id, has_external)?,
    ))
}

fn decode_mvcc_row_columns(
    input: &[u8],
    columns: &[usize],
    codec: &RowCodec,
) -> Result<(RowHeader, Vec<Value>, usize), QueryError> {
    if input.first().copied() != Some(MVCC_ROW_VERSION) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row header",
        ));
    }
    let (header, mut body_offset, has_external) = read_mvcc_header(input)?;
    let layout_id = read_varint(input, &mut body_offset)? as u32;
    let mut values = Vec::with_capacity(columns.len());
    let plan = RowDecodePlan::compile(columns);
    let stored_count = decode_row_schema_columns_into_with_plan_ext_for_layout(
        &input[body_offset..],
        codec,
        layout_id,
        &plan,
        &mut values,
        has_external,
    )?;
    Ok((header, values, stored_count))
}

// ---------------------------------------------------------------------------
// Value section
// ---------------------------------------------------------------------------

// Self-describing tag set, kept in 1-to-1 correspondence with the
// `Value` enum so every `Value` round-trips losslessly through the
// undo log in its native binary form. The legacy v2 format only had
// 8 tags and reached for ASCII canonical form whenever it encountered
// a binary temporal / DecimalCompact value; v3 emits the binary form
// directly, eliminating the ASCII round-trip on the undo hot path.
const TAG_NULL_RESERVED: u8 = 0; // not emitted; NULLs live in the bitmap
const TAG_INT: u8 = 1;
const TAG_UINT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_DOUBLE: u8 = 4;
const TAG_DECIMAL: u8 = 5;
const TAG_BYTES: u8 = 6;
const TAG_STRING: u8 = 7;
const TAG_DECIMAL_COMPACT: u8 = 8;
const TAG_DATE: u8 = 9;
const TAG_TIME: u8 = 10;
const TAG_DATETIME: u8 = 11;
const TAG_YEAR: u8 = 12;
const TAG_JSON: u8 = 13;
const TAG_GEOMETRY: u8 = 14;
const TAG_VECTOR: u8 = 15;
const TAG_TIMESTAMP: u8 = 16;
const TAG_DECIMAL128: u8 = 17;
const TAG_DECIMAL_WIDE: u8 = 18;
// NOTE: off-page (externalized) BLOB/TEXT does NOT add a Phase A tag.
// The undo log stores externalized columns as `Value::Null` in this
// self-describing value section and carries their `LobRef` descriptors
// in a separate `UndoRecord::before_external` field, so the undo path
// never materializes (or copies) multi-GB values. Heap rows (Phase B)
// mark externalized columns with the external bitmap instead.

pub(crate) fn encode_row_into(values: &[Value], out: &mut Vec<u8>) -> Result<(), QueryError> {
    let count = values.len();
    if count > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "row has too many columns",
        ));
    }
    write_varint(out, count as u64);
    let bitmap_len = count.div_ceil(8);
    let bitmap_start = out.len();
    out.resize(bitmap_start + bitmap_len, 0);
    for (idx, value) in values.iter().enumerate() {
        match value {
            Value::Null => {
                let byte = bitmap_start + (idx >> 3);
                out[byte] |= 1u8 << (idx & 7);
            }
            Value::Int(v) => {
                out.push(TAG_INT);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::UInt(v) => {
                out.push(TAG_UINT);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float(v) => {
                out.push(TAG_FLOAT);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::Double(v) => {
                out.push(TAG_DOUBLE);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::Decimal(v) => write_var_bytes(out, TAG_DECIMAL, v.as_bytes())?,
            Value::Bytes(v) => write_var_bytes(out, TAG_BYTES, v)?,
            Value::String(v) => write_var_bytes(out, TAG_STRING, v.as_bytes())?,
            // SmallString is the inline-bytes lane of TAG_STRING. The
            // on-disk byte layout is identical to a `String` with the
            // same payload, so an undo record written from a SmallString
            // round-trips back to a SmallString (or String) of the same
            // bytes — equality holds either way under `compare_values`.
            Value::SmallString(b) => write_var_bytes(out, TAG_STRING, b.as_slice())?,
            Value::DecimalCompact(mag, scale) => {
                out.push(TAG_DECIMAL_COMPACT);
                out.push(*scale);
                out.extend_from_slice(&mag.to_le_bytes());
            }
            Value::Decimal128(mag, scale) => {
                out.push(TAG_DECIMAL128);
                out.push(*scale);
                out.extend_from_slice(&mag.to_le_bytes());
            }
            Value::DecimalWide(value) => {
                out.push(TAG_DECIMAL_WIDE);
                out.push(value.scale);
                let bytes = value.magnitude.to_signed_bytes_le();
                write_varint(out, bytes.len() as u64);
                out.extend_from_slice(&bytes);
            }
            Value::Date(d) => {
                out.push(TAG_DATE);
                out.extend_from_slice(&d.to_le_bytes());
            }
            Value::Time(t) => {
                out.push(TAG_TIME);
                out.extend_from_slice(&t.to_le_bytes());
            }
            Value::DateTime(dt) => {
                out.push(TAG_DATETIME);
                out.extend_from_slice(&dt.to_le_bytes());
            }
            Value::Timestamp(ts) => {
                out.push(TAG_TIMESTAMP);
                out.extend_from_slice(&ts.to_le_bytes());
            }
            Value::Year(y) => {
                out.push(TAG_YEAR);
                out.push(*y);
            }
            Value::Json(b) => write_var_bytes(out, TAG_JSON, b)?,
            Value::Geometry(b) => write_var_bytes(out, TAG_GEOMETRY, b)?,
            Value::Vector(b) => write_var_bytes(out, TAG_VECTOR, &b.encode())?,
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn encoded_row_len(values: &[Value]) -> Result<usize, QueryError> {
    let count = values.len();
    if count > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "row has too many columns",
        ));
    }
    let mut len = varint_len(count as u64) + count.div_ceil(8);
    for value in values {
        let added = match value {
            Value::Null => 0,
            Value::Int(_) | Value::UInt(_) | Value::Double(_) => 1 + 8,
            Value::Float(_) => 1 + 4,
            Value::Decimal(v) => checked_var_value_len(v.len())?,
            Value::Bytes(v) => checked_var_value_len(v.len())?,
            Value::String(v) => checked_var_value_len(v.len())?,
            Value::SmallString(b) => checked_var_value_len(b.len())?,
            // Binary forms are emitted directly by encode_row_into,
            // so their on-disk size is fixed by the variant.
            Value::DecimalCompact(..) => 1 + 1 + 8,
            Value::Decimal128(..) => 1 + 1 + 16,
            Value::DecimalWide(value) => {
                let bytes = value.magnitude.to_signed_bytes_le();
                1 + 1 + varint_len(bytes.len() as u64) + bytes.len()
            }
            Value::Date(_) => 1 + 4,
            Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => 1 + 8,
            Value::Year(_) => 1 + 1,
            Value::Json(b) => checked_var_value_len(b.len())?,
            Value::Geometry(b) => checked_var_value_len(b.len())?,
            Value::Vector(b) => checked_var_value_len(b.len() + 12)?,
        };
        len = len.checked_add(added).ok_or_else(|| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "row is too large",
            )
        })?;
    }
    Ok(len)
}

#[cfg(test)]
fn checked_var_value_len(len: usize) -> Result<usize, QueryError> {
    if len > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "value is too large",
        ));
    }
    1usize // tag
        .checked_add(varint_len(len as u64))
        .and_then(|n| n.checked_add(len))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "row is too large",
            )
        })
}

pub(crate) fn decode_row(input: &[u8]) -> Result<Vec<Value>, QueryError> {
    let mut pos = 0usize;
    let count = read_varint(input, &mut pos)? as usize;
    let bitmap_start = pos;
    let bitmap_len = count.div_ceil(8);
    if bitmap_start + bitmap_len > input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    pos = bitmap_start + bitmap_len;
    let mut values = Vec::with_capacity(count);
    for idx in 0..count {
        if column_is_null(input, bitmap_start, idx) {
            values.push(Value::Null);
            continue;
        }
        let Some(tag) = input.get(pos).copied() else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt row payload",
            ));
        };
        pos += 1;
        values.push(read_value_by_tag(input, &mut pos, tag)?);
    }
    Ok(values)
}

/// Variant of `decode_row_columns` that fills a caller-provided
/// `Vec<Value>`. Hot-path scans (e.g. `scan_mvcc_columns_until`) reuse
/// a single buffer across all rows on a page, eliminating the
/// per-row Vec heap allocation that otherwise runs O(rows) on a
/// full table scan.
#[cfg(test)]
pub(crate) fn decode_row_columns_into(
    input: &[u8],
    columns: &[usize],
    out: &mut Vec<Value>,
) -> Result<usize, QueryError> {
    out.clear();
    let mut pos = 0usize;
    let count = read_varint(input, &mut pos)? as usize;
    let bitmap_start = pos;
    let bitmap_len = count.div_ceil(8);
    if bitmap_start + bitmap_len > input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    pos = bitmap_start + bitmap_len;
    if columns.is_empty() {
        for idx in 0..count {
            if column_is_null(input, bitmap_start, idx) {
                continue;
            }
            let Some(tag) = input.get(pos).copied() else {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt row payload",
                ));
            };
            pos += 1;
            skip_value_by_tag(input, &mut pos, tag)?;
        }
        return Ok(count);
    }
    // Build a small src-col → dest-idx lookup so the per-row inner loop
    // is O(count) rather than O(count * columns.len()). For a wide table
    // with a narrow projection, this turns repeated per-row probe work into
    // a single array indexing op and materially lowers SeqScan-driven
    // aggregation cost.
    //
    // NOTE: this lookup is rebuilt per row. An earlier revision moved
    // it into a thread-local `Vec<i16>` arena to amortise the
    // `smallvec![-1; needed]` cost across a scan, but on Apple Silicon
    // the `thread_local!::with` + `RefCell::take/replace` round-trip
    // costs more than the original stack-only `SmallVec<[i16; 32]>` for
    // the common case (`needed <= 32`), which is zero-alloc inline
    // memset. Re-introducing thread-local reuse here regressed
    // wide-table scans (TPC-H Q18 lineitem, 16-col × 6M rows) at
    // multiple users' setups; keep the stack-only fast path.
    let max_col = columns.iter().copied().max().unwrap_or(0);
    let mut needed = max_col + 1;
    if needed > count {
        needed = count;
    }
    let mut dest_of: smallvec::SmallVec<[i16; 32]> = smallvec::smallvec![-1; needed];
    for (out_idx, src_idx) in columns.iter().enumerate() {
        if *src_idx < needed {
            dest_of[*src_idx] = out_idx as i16;
        }
    }
    out.resize_with(columns.len(), || Value::Null);
    let stop = needed;
    for idx in 0..count {
        if column_is_null(input, bitmap_start, idx) {
            continue;
        }
        let Some(tag) = input.get(pos).copied() else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt row payload",
            ));
        };
        pos += 1;
        let dest = if idx < stop { dest_of[idx] } else { -1 };
        if dest >= 0 {
            out[dest as usize] = read_value_by_tag(input, &mut pos, tag)?;
        } else {
            skip_value_by_tag(input, &mut pos, tag)?;
        }
    }
    Ok(count)
}

/// Walk past a value section starting at byte 0 of `input` and return
/// the number of bytes it consumed. Used by undo-log decode to find
/// the trailing `chain_depth` field after a row payload.
pub(crate) fn encoded_row_skip_len(input: &[u8]) -> Result<usize, QueryError> {
    let mut pos = 0usize;
    let count = read_varint(input, &mut pos)? as usize;
    let bitmap_start = pos;
    let bitmap_len = count.div_ceil(8);
    if bitmap_start + bitmap_len > input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    pos = bitmap_start + bitmap_len;
    for idx in 0..count {
        if column_is_null(input, bitmap_start, idx) {
            continue;
        }
        let Some(tag) = input.get(pos).copied() else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt row payload",
            ));
        };
        pos += 1;
        skip_value_by_tag(input, &mut pos, tag)?;
    }
    Ok(pos)
}

fn read_value_by_tag(input: &[u8], pos: &mut usize, tag: u8) -> Result<Value, QueryError> {
    match tag {
        TAG_NULL_RESERVED => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unexpected null tag in payload",
        )),
        TAG_INT => {
            let bytes = read_exact(input, pos, 8)?;
            Ok(Value::Int(i64::from_le_bytes(bytes.try_into().unwrap())))
        }
        TAG_UINT => {
            let bytes = read_exact(input, pos, 8)?;
            Ok(Value::UInt(u64::from_le_bytes(bytes.try_into().unwrap())))
        }
        TAG_FLOAT => {
            let bytes = read_exact(input, pos, 4)?;
            Ok(Value::Float(f32::from_le_bytes(bytes.try_into().unwrap())))
        }
        TAG_DOUBLE => {
            let bytes = read_exact(input, pos, 8)?;
            Ok(Value::Double(f64::from_le_bytes(bytes.try_into().unwrap())))
        }
        TAG_DECIMAL => Ok(Value::Decimal(
            String::from_utf8(read_var_bytes(input, pos)?)
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "invalid decimal bytes"))?,
        )),
        TAG_BYTES => Ok(Value::Bytes(read_var_bytes(input, pos)?)),
        TAG_STRING => Ok(Value::String(
            String::from_utf8(read_var_bytes(input, pos)?)
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "invalid string bytes"))?,
        )),
        TAG_DECIMAL_COMPACT => {
            let scale = read_exact(input, pos, 1)?[0];
            let bytes = read_exact(input, pos, 8)?;
            Ok(Value::DecimalCompact(
                i64::from_le_bytes(bytes.try_into().unwrap()),
                scale,
            ))
        }
        TAG_DECIMAL128 => {
            let scale = read_exact(input, pos, 1)?[0];
            let bytes = read_exact(input, pos, 16)?;
            Ok(Value::Decimal128(
                i128::from_le_bytes(bytes.try_into().unwrap()),
                scale,
            ))
        }
        TAG_DECIMAL_WIDE => {
            let scale = read_exact(input, pos, 1)?[0];
            let bytes = read_var_bytes(input, pos)?;
            Ok(Value::DecimalWide(Box::new(crate::types::DecimalWide {
                magnitude: num_bigint::BigInt::from_signed_bytes_le(&bytes),
                scale,
            })))
        }
        TAG_DATE => {
            let bytes = read_exact(input, pos, 4)?;
            Ok(Value::Date(i32::from_le_bytes(bytes.try_into().unwrap())))
        }
        TAG_TIME => {
            let bytes = read_exact(input, pos, 8)?;
            Ok(Value::Time(i64::from_le_bytes(bytes.try_into().unwrap())))
        }
        TAG_DATETIME => {
            let bytes = read_exact(input, pos, 8)?;
            Ok(Value::DateTime(i64::from_le_bytes(
                bytes.try_into().unwrap(),
            )))
        }
        TAG_TIMESTAMP => {
            let bytes = read_exact(input, pos, 8)?;
            Ok(Value::Timestamp(i64::from_le_bytes(
                bytes.try_into().unwrap(),
            )))
        }
        TAG_YEAR => {
            let b = read_exact(input, pos, 1)?[0];
            Ok(Value::Year(b))
        }
        TAG_JSON => Ok(Value::Json(read_var_bytes(input, pos)?)),
        TAG_GEOMETRY => Ok(Value::Geometry(read_var_bytes(input, pos)?)),
        TAG_VECTOR => Ok(Value::Vector(crate::engine::vector::codec::decode_value(
            &read_var_bytes(input, pos)?,
        )?)),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unknown row value tag",
        )),
    }
}

fn skip_value_by_tag(input: &[u8], pos: &mut usize, tag: u8) -> Result<(), QueryError> {
    match tag {
        TAG_NULL_RESERVED => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unexpected null tag in payload",
        )),
        TAG_INT | TAG_UINT | TAG_DOUBLE | TAG_TIME | TAG_DATETIME | TAG_TIMESTAMP => {
            read_exact(input, pos, 8)?;
            Ok(())
        }
        TAG_FLOAT | TAG_DATE => {
            read_exact(input, pos, 4)?;
            Ok(())
        }
        TAG_DECIMAL | TAG_BYTES | TAG_STRING | TAG_JSON | TAG_GEOMETRY | TAG_VECTOR => {
            let len = read_varint(input, pos)? as usize;
            read_exact(input, pos, len)?;
            Ok(())
        }
        TAG_DECIMAL_COMPACT => {
            // u8 scale + i64 LE magnitude
            read_exact(input, pos, 1 + 8)?;
            Ok(())
        }
        TAG_DECIMAL128 => {
            read_exact(input, pos, 1 + 16)?;
            Ok(())
        }
        TAG_DECIMAL_WIDE => {
            read_exact(input, pos, 1)?;
            let len = read_varint(input, pos)? as usize;
            read_exact(input, pos, len)?;
            Ok(())
        }
        TAG_YEAR => {
            read_exact(input, pos, 1)?;
            Ok(())
        }
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "unknown row value tag",
        )),
    }
}

#[inline]
fn column_is_null(input: &[u8], bitmap_start: usize, idx: usize) -> bool {
    input[bitmap_start + (idx >> 3)] & (1u8 << (idx & 7)) != 0
}

fn write_var_bytes(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) -> Result<(), QueryError> {
    if bytes.len() > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "value is too large",
        ));
    }
    out.push(tag);
    write_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_var_bytes(input: &[u8], pos: &mut usize) -> Result<Vec<u8>, QueryError> {
    let len = read_varint(input, pos)? as usize;
    Ok(read_exact(input, pos, len)?.to_vec())
}

fn read_exact<'a>(input: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8], QueryError> {
    let end = pos
        .checked_add(len)
        .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "corrupt row payload"))?;
    if end > input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    let bytes = &input[*pos..end];
    *pos = end;
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// LEB128 unsigned varints
// ---------------------------------------------------------------------------

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
fn read_varint(input: &[u8], pos: &mut usize) -> Result<u64, QueryError> {
    if *pos >= input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    let first = input[*pos];
    *pos += 1;
    // Row column counts, layout ids, and almost all OLTP scalar/string
    // lengths fit in one LEB128 byte. Keep that overwhelmingly common case
    // to one checked load and one branch; the old loop paid overflow and
    // continuation bookkeeping even when the first byte terminated.
    if first < 0x80 {
        return Ok(u64::from(first));
    }
    read_varint_continuation(input, pos, u64::from(first & 0x7f))
}

#[cold]
#[inline(never)]
fn read_varint_continuation(
    input: &[u8],
    pos: &mut usize,
    mut result: u64,
) -> Result<u64, QueryError> {
    let mut shift = 7u32;
    loop {
        if *pos >= input.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt row payload",
            ));
        }
        let byte = input[*pos];
        *pos += 1;
        // Reject overlong encodings that would overflow u64 (>= 10 bytes
        // or 10th byte > 0x01). Bounded so a corrupt payload can't spin.
        if shift >= 63 && byte > 0x01 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "varint too large",
            ));
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 70 {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "varint too large",
            ));
        }
    }
}

fn varint_len(v: u64) -> usize {
    let mut bits = 64 - v.leading_zeros() as usize;
    if bits == 0 {
        return 1;
    }
    // ceil(bits / 7)
    bits = (bits + 6) / 7;
    bits
}

// ---------------------------------------------------------------------------
// Phase B: schema-driven value section
// ---------------------------------------------------------------------------
//
// The Phase A value section above is self-describing: every non-null
// column carries a 1B type tag plus a varint length prefix when needed.
// That makes it usable by callers that don't know the schema (undo log
// records carry old row values, so they keep the self-describing path).
//
// Heap rows DO know their schema — `HeapFile` carries an `Arc<RowCodec>`
// built from the table's `ColumnSchema` list in `physical_pos` order —
// so they can drop the per-column tag entirely and pick a tight binary
// encoding driven by the column's declared type:
//
//   ColumnKind::Int { width, signed }  →  1/2/3/4/8 byte LE integer
//                                          (vs Phase A's flat 8B)
//   ColumnKind::Float32                →  4B f32 LE
//   ColumnKind::Float64                →  8B f64 LE
//   ColumnKind::Decimal { scale }      →  1B sign + 1B nbytes + magnitude
//                                          (vs Phase A's ASCII string)
//   ColumnKind::VarString              →  raw UTF-8 bytes addressed by dir
//   ColumnKind::VarBytes               →  raw bytes addressed by dir
//   ColumnKind::Date                   →  3B packed days-since-epoch
//                                          (vs Phase A's 10-byte ASCII)
//   ColumnKind::Time                   →  3-6B quantized microseconds
//   ColumnKind::DateTime               →  5-8B quantized microseconds
//                                          (vs Phase A's ~19-byte ASCII)
//   ColumnKind::Year                   →  1B (year-1900), 0 for 0000
//
// Value-section layout:
//
//   [varint stored_count N]
//   [ceil(N/8) bytes: NULL bitmap]
//   [optional external bitmap over varlen columns]
//   [dense fixed-width block]
//   [2B cumulative-end directory for varlen columns]
//   [raw varlen payload]
//
// Sentinels: i32::MIN encodes the MySQL "zero date" 0000-00-00; i64::MIN
// encodes the zero datetime 0000-00-00 00:00:00. These are produced by
// `parse_date_to_days` / `parse_datetime_to_micros` and detected on
// decode as binary `Value::Date` / `Value::DateTime` payloads.

/// Per-column encode/decode behaviour, derived once from a column's
/// declared `DataTypeKind`. `RowCodec` keeps these in catalog-slot
/// order; persisted row layouts map stored column ids back to these
/// slots during encode/decode.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnCodec {
    pub(crate) kind: ColumnKind,
}

#[derive(Debug, Clone, PartialEq)]
struct RowLayoutColumnCodec {
    column_index: usize,
    kind: ColumnKind,
    /// Schema-level nullability compiled into the row layout. NOT NULL
    /// columns never need to probe the per-row NULL bitmap during scans.
    nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct RowLayoutCodec {
    layout_id: u32,
    columns: Vec<RowLayoutColumnCodec>,
    /// Eagerly-computed [`ValueSectionLayout`]; the layout depends only
    /// on `columns` and is invariant for the lifetime of the codec. The
    /// previous per-row recomputation in `encode_row_schema_into_ext`
    /// allocated two small `Vec<Option<usize>>` per row — ~3 GB of
    /// throwaway allocation for a 6M-row TPC-H lineitem load (profile
    /// 2026-06-03). Computed via [`RowLayoutCodec::new`].
    value_layout: ValueSectionLayout,
}

impl RowLayoutCodec {
    fn new(layout_id: u32, columns: Vec<RowLayoutColumnCodec>) -> Self {
        let value_layout = value_section_layout(&columns);
        Self {
            layout_id,
            columns,
            value_layout,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "id", content = "parameters", deny_unknown_fields)]
pub(crate) enum ColumnKind {
    /// 1/2/3/4/8 byte LE integer; sign-extended on decode if `signed`.
    #[serde(rename = "1")]
    Int { width: u8, signed: bool },
    #[serde(rename = "2")]
    Float32,
    #[serde(rename = "3")]
    Float64,
    /// Variable-length `DECIMAL(P, S)` storage. 1B sign + 1B
    /// nbytes K + K bytes LE unsigned magnitude (= |value| × 10^scale).
    /// Used for bare `DECIMAL` declarations without a precision.
    #[serde(rename = "4")]
    Decimal { scale: u8 },
    /// Variable BCD storage for declared DECIMAL(P,S) with P > 38. The
    /// compact 4/8/16-byte lanes remain unchanged for ordinary decimals;
    /// only the MySQL DECIMAL(65) cold path uses this representation.
    #[serde(rename = "5")]
    DecimalWide { scale: u8 },
    /// Fixed-width `DECIMAL(P, S)` storage. The on-disk body is a plain
    /// LE signed integer of width 4/8/16 bytes — no sign/nbytes header,
    /// no padding. Chosen by `column_kind_for` based on the declared
    /// precision:
    ///   `P ≤ 9`   → `DecimalFixed { width: 4, scale }`
    ///   `P ≤ 18`  → `DecimalFixed { width: 8, scale }`
    ///   `P ≤ 38`  → `DecimalFixed { width: 16, scale }`
    /// Magnitudes are scaled integers (value × 10^scale), matching the
    /// `Value::DecimalCompact` representation. Round-tripping a
    /// DecimalCompact value through this kind skips the u128 parse +
    /// magnitude pack/unpack the variable-length codec does.
    #[serde(rename = "6")]
    DecimalFixed { width: u8, scale: u8 },
    /// CHAR / VARCHAR / TEXT — raw UTF-8 bytes addressed by the varlen
    /// directory.
    #[serde(rename = "7")]
    VarString,
    /// VARBINARY / BLOB / variable BINARY — raw bytes addressed by the
    /// varlen directory.
    #[serde(rename = "8")]
    VarBytes,
    /// JSON — binary `jsonb` bytes addressed by the varlen directory
    /// (see `engine::json`).
    /// Distinct from `VarBytes` so the decoder reconstructs a
    /// `Value::Json` rather than `Value::Bytes`, and so the column
    /// participates in off-page externalization for large documents.
    #[serde(rename = "9")]
    Json,
    /// GEOMETRY — internal `SRID + WKB` bytes addressed by the varlen
    /// directory (see
    /// `engine::geo`). Distinct from `VarBytes` so the decoder reconstructs
    /// a `Value::Geometry`, and so large geometries externalize off-page.
    #[serde(rename = "10")]
    Geometry,
    /// VECTOR without a declared dimension — internal little-endian `f32`
    /// bytes addressed by the varlen directory (see
    /// `engine::vector`). Distinct from `VarBytes` so the decoder reconstructs
    /// a `Value::Vector`, and so large vectors externalize off-page.
    #[serde(rename = "11")]
    Vector {
        dim: Option<u32>,
        datatype: crate::vector::VectorDataType,
    },
    /// Fixed-width BINARY(n) payload in the fixed block.
    #[serde(rename = "12")]
    FixedBytes { width: usize },
    /// `BIT(M)` — an M-bit value stored in `ceil(M/8)` big-endian bytes in
    /// the fixed block. Distinct from `FixedBytes` so the value round-trips
    /// as an integer (`Value::UInt`) — MySQL BIT is numeric in expression
    /// context (`b + 0`, comparisons) — with the big-endian byte form used
    /// only on disk.
    #[serde(rename = "13")]
    Bit { width: usize },
    /// Fixed-width VECTOR(n), stored as `dim * 4` little-endian f32 bytes.
    #[serde(rename = "14")]
    VectorFixed {
        dim: u32,
        datatype: crate::vector::VectorDataType,
    },
    /// ENUM stored as a 1- or 2-byte ordinal. Ordinal 0 is the empty/error
    /// value; 1-based ordinals map to declared labels.
    #[serde(rename = "15")]
    Enum { values: Vec<String>, width: u8 },
    /// SET stored as a compact bitmask over declared labels.
    #[serde(rename = "16")]
    Set { values: Vec<String>, width: u8 },
    #[serde(rename = "17")]
    Date,
    /// `TIME[(fsp)]` — 3-6B signed quantum count. `fsp` (0..=6) is the declared
    /// fractional-seconds precision; writers truncate the payload to a
    /// multiple of `10^(6-fsp)` before encoding so a `TIME(0)` value
    /// always round-trips with zero microseconds.
    #[serde(rename = "18")]
    Time { fsp: u8 },
    /// `DATETIME[(fsp)]` — 5-8B signed quantum count since 1970-01-01 UTC.
    /// See [`ColumnKind::Time`] for fsp semantics.
    #[serde(rename = "19")]
    DateTime { fsp: u8 },
    /// `TIMESTAMP[(fsp)]` — physically the same 5-8B signed quantum count as
    /// DATETIME, but semantically an absolute UTC instant. The distinct kind
    /// reconstructs `Value::Timestamp` on read so timezone semantics survive
    /// through expressions, sorting, protocol output and replication.
    #[serde(rename = "20")]
    Timestamp { fsp: u8 },
    #[serde(rename = "21")]
    Year,
}

/// Cached encode/decode plan for one table. The `columns` vector is keyed
/// by catalog column slot; `layouts` describe which slots each persisted
/// row layout stores and in what physical order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RowCodec {
    pub(crate) columns: Vec<ColumnCodec>,
    pub(crate) active_layout_id: u32,
    layouts: Vec<RowLayoutCodec>,
    defaults: Vec<Value>,
}

impl RowCodec {
    pub(crate) fn is_text_or_binary_lob_column(&self, column: usize) -> bool {
        matches!(
            self.columns.get(column).map(|codec| &codec.kind),
            Some(ColumnKind::VarString | ColumnKind::VarBytes)
        )
    }

    pub(crate) fn is_binary_lob_column(&self, column: usize) -> bool {
        matches!(
            self.columns.get(column).map(|codec| &codec.kind),
            Some(ColumnKind::VarBytes)
        )
    }

    /// Empty codec — used as a placeholder while a `TableSchema` is
    /// being constructed. `build_column_index` overwrites it.
    pub(crate) fn empty() -> Self {
        Self {
            columns: Vec::new(),
            active_layout_id: 1,
            layouts: vec![RowLayoutCodec::new(1, Vec::new())],
            defaults: Vec::new(),
        }
    }

    /// Build a codec from a table schema. Columns are emitted in
    /// catalog slot order (the same index used by expression evaluation).
    /// Heap rows themselves are written by the active row layout, which
    /// maps persisted `column_id`s back to these catalog slots. That lets
    /// new rows skip Dropped tombstones while rows written by older online
    /// DDL layouts remain readable.
    pub(crate) fn from_schema(schema: &TableSchema) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|c| ColumnCodec {
                kind: column_kind_for(&c.data_type, c.unsigned),
            })
            .collect::<Vec<_>>();
        let defaults = schema
            .columns
            .iter()
            .map(crate::engine::executor::insert::column_read_backfill_default)
            .collect::<Vec<_>>();
        let mut by_column_id = HashMap::new();
        for (idx, column) in schema.columns.iter().enumerate() {
            by_column_id.insert(column.column_id, idx);
        }
        let mut layouts = Vec::new();
        for persisted in &schema.row_layouts {
            let mut physical_columns = persisted.columns.iter().collect::<Vec<_>>();
            physical_columns.sort_by_key(|column| column.physical_pos);
            let cols = physical_columns
                .into_iter()
                .filter_map(|layout_col| {
                    let column_index = *by_column_id.get(&layout_col.column_id)?;
                    let current = schema.columns.get(column_index)?;
                    Some(RowLayoutColumnCodec {
                        column_index,
                        kind: match (&layout_col.codec_kind.kind, &columns[column_index].kind) {
                            // Appending labels without widening the ordinal/bit
                            // lane preserves every existing byte interpretation.
                            (
                                ColumnKind::Enum { values: old, width },
                                ColumnKind::Enum {
                                    values: new,
                                    width: next,
                                },
                            ) if width == next && new.starts_with(old) => {
                                columns[column_index].kind.clone()
                            }
                            (
                                ColumnKind::Set { values: old, width },
                                ColumnKind::Set {
                                    values: new,
                                    width: next,
                                },
                            ) if width == next && new.starts_with(old) => {
                                columns[column_index].kind.clone()
                            }
                            _ => layout_col.codec_kind.kind.clone(),
                        },
                        nullable: current.nullable || layout_col.nullable_ord.is_some(),
                    })
                })
                .collect::<Vec<_>>();
            if cols.is_empty() && !persisted.columns.is_empty() {
                continue;
            }
            layouts.push(RowLayoutCodec::new(persisted.layout_id.max(1), cols));
        }
        if layouts.is_empty() {
            let layout_columns = columns
                .iter()
                .enumerate()
                .map(|(column_index, col)| RowLayoutColumnCodec {
                    column_index,
                    kind: col.kind.clone(),
                    nullable: schema.columns[column_index].nullable,
                })
                .collect();
            layouts.push(RowLayoutCodec::new(
                schema.active_layout_id.max(1),
                layout_columns,
            ));
        }
        Self {
            columns,
            active_layout_id: schema.active_layout_id.max(1),
            layouts,
            defaults,
        }
    }

    fn active_layout(&self) -> &RowLayoutCodec {
        self.layout_for_id(self.active_layout_id)
            .or_else(|_| {
                self.layouts.first().ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "row codec has no layouts",
                    )
                })
            })
            .expect("RowCodec always has at least one layout")
    }

    /// M7 — byte estimate of a row's **minimum fixed footprint** (used at CREATE/ALTER to check
    /// whether the row can fit in a single page). = max MVCC header (18B) + layout_id varint (<=5) +
    /// column-count varint (<=5) + NULL bitmap (ceil(ncols/8)) + total fixed column widths
    /// (`fixed_len`, including n*4 for `VECTOR(n)`) + variable-length directory (var_count*2).
    /// Variable-length payloads themselves are excluded (externalizable / value-dependent), but `VectorFixed` is a
    /// fixed column that is never externalized, so its full width sits in `fixed_len` — precisely the over-page
    /// `VECTOR(n)` being intercepted. A row can only fit if the return value is <= ROW_FIT_BUDGET.
    pub(crate) fn min_fixed_row_footprint(&self) -> usize {
        let layout = self.active_layout();
        let ncols = layout.columns.len();
        const MAX_MVCC_HEADER: usize = 18; // version+flags+last_txn_id+undo_ptr
        const MAX_LAYOUT_ID_VARINT: usize = 5;
        const MAX_COUNT_VARINT: usize = 5;
        MAX_MVCC_HEADER
            + MAX_LAYOUT_ID_VARINT
            + MAX_COUNT_VARINT
            + ncols.div_ceil(8)
            + layout.value_layout.fixed_len
            + layout.value_layout.var_count * 2
    }

    fn layout_for_id(&self, layout_id: u32) -> Result<&RowLayoutCodec, QueryError> {
        self.layouts
            .iter()
            .find(|layout| layout.layout_id == layout_id.max(1))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("row payload references unknown layout_id {}", layout_id),
                )
            })
    }

    fn default_for_column(&self, idx: usize) -> Value {
        self.defaults.get(idx).cloned().unwrap_or(Value::Null)
    }

    /// True when every column-level default is `Value::Null`. Hot scan paths
    /// (`decode_row_schema_columns_into_with_plan_ext_for_layout`) check this
    /// to skip a per-row `extend(plan.columns.iter().map(default_for_column))`
    /// loop and instead do a single `resize_with(out_len, || Value::Null)`,
    /// which `rustc` lowers to a memset-equivalent on the hot path.
    #[inline]
    pub(crate) fn defaults_all_null(&self) -> bool {
        // In normal operation `defaults` is empty (the DDL path keeps
        // schema-default expressions out of the codec because all current
        // ALTER paths re-encode rows). The few code paths that prepopulate
        // `defaults` (tests, future INSTANT ADD with non-NULL default) take
        // the slow path below.
        self.defaults.iter().all(|v| matches!(v, Value::Null))
    }
}

#[cfg(test)]
fn identity_layout_for_columns(columns: &[ColumnCodec], layout_id: u32) -> RowLayoutCodec {
    let layout_columns: Vec<RowLayoutColumnCodec> = columns
        .iter()
        .enumerate()
        .map(|(column_index, col)| RowLayoutColumnCodec {
            column_index,
            kind: col.kind.clone(),
            // Test/legacy codecs do not carry table constraints. Conservatively
            // preserve bitmap checks for every column.
            nullable: true,
        })
        .collect();
    RowLayoutCodec::new(layout_id, layout_columns)
}

/// True when any column is string- or binary-typed and could therefore
/// hold a value large enough to be externalized off-page. Used by
/// `heap_handle` to decide whether to attach an `OverflowReader` to a
/// table's heap handle. Tables with only fixed-width columns can never
/// externalize, so they skip the reader entirely.
pub(crate) fn codec_has_externalizable_columns(codec: &RowCodec) -> bool {
    codec
        .layouts
        .iter()
        .flat_map(|layout| layout.columns.iter())
        .any(|c| {
            matches!(
                c.kind,
                ColumnKind::VarString
                    | ColumnKind::VarBytes
                    | ColumnKind::Json
                    | ColumnKind::Geometry
                    | ColumnKind::Vector { .. }
            )
        })
}

fn vector_column_kind(dim: Option<u32>, datatype: crate::vector::VectorDataType) -> ColumnKind {
    let limit = if datatype == crate::vector::VectorDataType::Float32 {
        8192
    } else {
        4096
    };
    match dim {
        Some(dim) if datatype.payload_len(dim) <= limit => {
            ColumnKind::VectorFixed { dim, datatype }
        }
        dim => ColumnKind::Vector { dim, datatype },
    }
}

fn column_kind_for(ty: &DataTypeKind, unsigned: bool) -> ColumnKind {
    match ty {
        DataTypeKind::TinyInt => ColumnKind::Int {
            width: 1,
            signed: !unsigned,
        },
        DataTypeKind::SmallInt => ColumnKind::Int {
            width: 2,
            signed: !unsigned,
        },
        DataTypeKind::MediumInt => ColumnKind::Int {
            width: 3,
            signed: !unsigned,
        },
        DataTypeKind::Int => ColumnKind::Int {
            width: 4,
            signed: !unsigned,
        },
        DataTypeKind::BigInt => ColumnKind::Int {
            width: 8,
            signed: !unsigned,
        },
        DataTypeKind::Bool => ColumnKind::Int {
            width: 1,
            signed: !unsigned,
        },
        DataTypeKind::Float { .. } => ColumnKind::Float32,
        DataTypeKind::Double { .. } => ColumnKind::Float64,
        DataTypeKind::Decimal { precision, scale } => {
            let s = scale.unwrap_or(0);
            // Pick the narrowest fixed-width body that fits the declared
            // precision. Columns declared as bare `DECIMAL` (no
            // precision) stay on the legacy variable-length path so
            // older `.heap` files keep decoding without migration.
            match precision {
                Some(p) if *p <= 9 => ColumnKind::DecimalFixed { width: 4, scale: s },
                Some(p) if *p <= 18 => ColumnKind::DecimalFixed { width: 8, scale: s },
                Some(p) if *p <= 38 => ColumnKind::DecimalFixed {
                    width: 16,
                    scale: s,
                },
                Some(_) => ColumnKind::DecimalWide { scale: s },
                None => ColumnKind::Decimal { scale: s },
            }
        }
        DataTypeKind::Char { .. } | DataTypeKind::VarChar { .. } | DataTypeKind::Text { .. } => {
            ColumnKind::VarString
        }
        DataTypeKind::Enum { values } => ColumnKind::Enum {
            values: values.clone(),
            width: enum_storage_width(values.len()),
        },
        DataTypeKind::Set { values } => ColumnKind::Set {
            values: values.clone(),
            width: set_storage_width(values.len()),
        },
        DataTypeKind::Json => ColumnKind::Json,
        DataTypeKind::Geometry { .. } => ColumnKind::Geometry,
        DataTypeKind::Vector { dim, datatype } => vector_column_kind(*dim, *datatype),
        DataTypeKind::Binary { len: Some(len) } => ColumnKind::FixedBytes { width: *len },
        DataTypeKind::Bit { len } => ColumnKind::Bit {
            width: len.unwrap_or(1).div_ceil(8),
        },
        DataTypeKind::Binary { len: None }
        | DataTypeKind::VarBinary { .. }
        | DataTypeKind::Blob { .. } => ColumnKind::VarBytes,
        DataTypeKind::Date => ColumnKind::Date,
        DataTypeKind::Time { fsp } => ColumnKind::Time { fsp: *fsp },
        DataTypeKind::DateTime { fsp } => ColumnKind::DateTime { fsp: *fsp },
        DataTypeKind::Timestamp { fsp } => ColumnKind::Timestamp { fsp: *fsp },
        DataTypeKind::Year => ColumnKind::Year,
    }
}

/// Stable persisted row-codec identifiers. Numeric string tags are assigned
/// explicitly on ColumnKind and must never be reused for another encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RowCodecSpec {
    version: u16,
    kind: ColumnKind,
}

impl<'de> Deserialize<'de> for RowCodecSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            version: u16,
            kind: ColumnKind,
        }
        let wire = Wire::deserialize(deserializer)?;
        let valid = match &wire.kind {
            ColumnKind::Int { width, .. } => matches!(width, 1 | 2 | 3 | 4 | 8),
            ColumnKind::Decimal { scale } | ColumnKind::DecimalWide { scale } => *scale <= 30,
            ColumnKind::DecimalFixed { width, scale } => {
                matches!(width, 4 | 8 | 16) && *scale <= 30
            }
            ColumnKind::Time { fsp }
            | ColumnKind::DateTime { fsp }
            | ColumnKind::Timestamp { fsp } => *fsp <= 6,
            ColumnKind::Bit { width } => (1..=8).contains(width),
            ColumnKind::FixedBytes { width } => *width <= 65535,
            ColumnKind::Vector { dim, datatype } => {
                dim.is_none_or(|dim| dim > 0 && dim <= datatype.max_dimension())
            }
            ColumnKind::VectorFixed { dim, datatype } => {
                *dim > 0 && *dim <= datatype.max_dimension()
            }
            ColumnKind::Enum { values, width } => {
                !values.is_empty()
                    && values.len() <= 65535
                    && *width == enum_storage_width(values.len())
            }
            ColumnKind::Set { values, width } => {
                !values.is_empty()
                    && values.len() <= 64
                    && *width == set_storage_width(values.len())
            }
            _ => true,
        };
        if wire.version != crate::storage::format_versions::ROW_CODEC_SPEC_VERSION || !valid {
            return Err(serde::de::Error::custom(
                "unsupported or invalid row codec specification",
            ));
        }
        Ok(Self {
            version: wire.version,
            kind: wire.kind,
        })
    }
}

pub(crate) fn row_codec_spec(data_type: &DataTypeKind, unsigned: bool) -> RowCodecSpec {
    RowCodecSpec {
        version: crate::storage::format_versions::ROW_CODEC_SPEC_VERSION,
        kind: column_kind_for(data_type, unsigned),
    }
}

// --- Phase B value section helpers ---------------------------------------

// Non-external convenience wrappers. Production heap/undo paths call the
// `_ext` variants directly (deriving `has_external` from the row header);
// these `&[]` / `false` wrappers are retained for the codec unit tests
// and as the simple public API.
#[cfg(test)]
pub(crate) fn encode_row_schema_into(
    values: &[Value],
    codec: &RowCodec,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    encode_row_schema_into_ext(values, codec, &[], out)
}

#[derive(Debug, Clone, PartialEq)]
struct ValueSectionLayout {
    fixed_offsets: Vec<Option<usize>>,
    var_ords: Vec<Option<usize>>,
    fixed_len: usize,
    var_count: usize,
}

/// Get the [`ValueSectionLayout`] for a (possibly partial) prefix of
/// `row_layout.columns`. Returns the cached `value_layout` when the
/// caller's slice covers all columns (the steady-state hot path); falls
/// back to `value_section_layout` only for INSTANT-ADD-COLUMN paths
/// where an old row carries fewer columns than the current layout.
#[inline]
fn value_section_layout_for_prefix<'a>(
    row_layout: &'a RowLayoutCodec,
    stored_columns: &[RowLayoutColumnCodec],
) -> std::borrow::Cow<'a, ValueSectionLayout> {
    if stored_columns.len() == row_layout.columns.len() {
        std::borrow::Cow::Borrowed(&row_layout.value_layout)
    } else {
        std::borrow::Cow::Owned(value_section_layout(stored_columns))
    }
}

fn value_section_layout(columns: &[RowLayoutColumnCodec]) -> ValueSectionLayout {
    let count = columns.len();
    let mut fixed_offsets = vec![None; count];
    let mut var_ords = vec![None; count];
    let mut fixed_len = 0usize;
    let mut var_count = 0usize;
    for idx in 0..count {
        let col = &columns[idx];
        if let Some(width) = fixed_width_for_kind(&col.kind) {
            fixed_offsets[idx] = Some(fixed_len);
            fixed_len += width;
        } else {
            var_ords[idx] = Some(var_count);
            var_count += 1;
        }
    }
    ValueSectionLayout {
        fixed_offsets,
        var_ords,
        fixed_len,
        var_count,
    }
}

fn fixed_width_for_kind(kind: &ColumnKind) -> Option<usize> {
    Some(match kind {
        ColumnKind::Int { width, .. } => *width as usize,
        ColumnKind::Float32 => 4,
        ColumnKind::Float64 => 8,
        ColumnKind::DecimalFixed { width, .. } => *width as usize,
        ColumnKind::Date => 3,
        ColumnKind::Time { fsp } => temporal_width(*fsp, 3),
        ColumnKind::DateTime { fsp } | ColumnKind::Timestamp { fsp } => temporal_width(*fsp, 5),
        ColumnKind::Year => 1,
        ColumnKind::FixedBytes { width } => *width,
        ColumnKind::Bit { width } => *width,
        ColumnKind::VectorFixed { dim, datatype } => datatype.payload_len(*dim),
        ColumnKind::Enum { width, .. } | ColumnKind::Set { width, .. } => *width as usize,
        ColumnKind::Decimal { .. }
        | ColumnKind::DecimalWide { .. }
        | ColumnKind::VarString
        | ColumnKind::VarBytes
        | ColumnKind::Json
        | ColumnKind::Geometry
        | ColumnKind::Vector { .. } => return None,
    })
}

#[inline]
fn dir_entry_offset(dir_start: usize, ord: usize) -> usize {
    dir_start + ord * 2
}

/// Encode the value section, externalizing the columns for which
/// `external[idx]` is `Some(LobRef)` (their payload becomes a 20-byte
/// descriptor and the external bitmap bit is set). `external` may be
/// shorter than `values` (missing entries are treated as inline). When
/// no column is externalized the output is byte-for-byte identical to
/// the pre-LOB format and carries no external bitmap.
pub(crate) fn encode_row_schema_into_ext(
    values: &[Value],
    codec: &RowCodec,
    external: &[Option<LobRef>],
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    let row_layout = codec.active_layout();
    let count = row_layout.columns.len();
    if count > u32::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "row has too many columns",
        ));
    }
    let has_external = external.iter().any(|e| e.is_some());
    let layout = &row_layout.value_layout;
    write_varint(out, count as u64);
    let bitmap_len = count.div_ceil(8);
    let null_bitmap_start = out.len();
    out.resize(null_bitmap_start + bitmap_len, 0);
    let ext_bitmap_len = layout.var_count.div_ceil(8);
    let ext_bitmap_start = if has_external {
        let start = out.len();
        out.resize(start + ext_bitmap_len, 0);
        start
    } else {
        0
    };
    let fixed_start = out.len();
    out.resize(fixed_start + layout.fixed_len, 0);
    let dir_start = out.len();
    out.resize(dir_start + layout.var_count * 2, 0);
    let payload_base = out.len();
    for (idx, layout_col) in row_layout.columns.iter().enumerate() {
        let value = values.get(layout_col.column_index).unwrap_or(&Value::Null);
        // An externalized column takes precedence over the value's own
        // variant: the in-memory `value` may be a `Null` placeholder
        // (e.g. when rollback re-externalizes an old version from its
        // undo `before_external` descriptor list).
        if let Some(Some(lob)) = external.get(layout_col.column_index) {
            let Some(var_ord) = layout.var_ords[idx] else {
                return Err(error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "fixed-width column cannot be externalized",
                ));
            };
            out[ext_bitmap_start + (var_ord >> 3)] |= 1u8 << (var_ord & 7);
            let offset = (out.len() - payload_base) as u32;
            lob.write_to(out)?;
            write_dir_entry(out, dir_start, var_ord, offset, LOB_DESCRIPTOR_LEN as u32)?;
            continue;
        }
        if matches!(value, Value::Null) {
            out[null_bitmap_start + (idx >> 3)] |= 1u8 << (idx & 7);
            if let Some(var_ord) = layout.var_ords[idx] {
                let offset = (out.len() - payload_base) as u32;
                write_dir_entry(out, dir_start, var_ord, offset, 0)?;
            }
            continue;
        }
        if let Some(fixed_off) = layout.fixed_offsets[idx] {
            let width = fixed_width_for_kind(&layout_col.kind).unwrap();
            encode_fixed_for_kind(
                &layout_col.kind,
                value,
                &mut out[fixed_start + fixed_off..fixed_start + fixed_off + width],
            )?;
        } else {
            let var_ord = layout.var_ords[idx].expect("variable column must have var ord");
            let offset = (out.len() - payload_base) as u32;
            encode_var_for_kind(&layout_col.kind, value, out)?;
            let len = (out.len() - payload_base)
                .checked_sub(offset as usize)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "row is too large",
                    )
                })?;
            write_dir_entry(out, dir_start, var_ord, offset, len as u32)?;
        }
    }
    Ok(())
}

fn write_dir_entry(
    out: &mut [u8],
    dir_start: usize,
    var_ord: usize,
    offset: u32,
    len: u32,
) -> Result<(), QueryError> {
    let start = dir_entry_offset(dir_start, var_ord);
    let end = start.checked_add(2).ok_or_else(|| {
        error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "row is too large",
        )
    })?;
    if end > out.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    let end_offset = offset.checked_add(len).ok_or_else(|| {
        error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "row variable payload is too large",
        )
    })?;
    let end_offset = u16::try_from(end_offset).map_err(|_| {
        error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "row variable payload is too large",
        )
    })?;
    out[start..end].copy_from_slice(&end_offset.to_le_bytes());
    Ok(())
}

#[cfg(test)]
pub(crate) fn encoded_row_schema_len(
    values: &[Value],
    codec: &RowCodec,
) -> Result<usize, QueryError> {
    let row_layout = codec.active_layout();
    let count = row_layout.columns.len();
    let layout = &row_layout.value_layout;
    let mut len =
        varint_len(count as u64) + count.div_ceil(8) + layout.fixed_len + layout.var_count * 2;
    for layout_col in &row_layout.columns {
        let value = values.get(layout_col.column_index).unwrap_or(&Value::Null);
        if matches!(value, Value::Null) {
            continue;
        }
        if fixed_width_for_kind(&layout_col.kind).is_some() {
            continue;
        }
        len = len
            .checked_add(value_len_for_kind(&layout_col.kind, value)?)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "row is too large",
                )
            })?;
    }
    Ok(len)
}

/// Estimate the encoded value-section length (plus a worst-case MVCC
/// header) for a row, treating columns flagged in `externalized` as
/// 20-byte descriptors. Used by the write path to decide which columns
/// to push off-page so the inline row fits one heap page. Approximate:
/// errors from `value_len_for_kind` count as 0 (estimation only).
pub(crate) fn estimate_value_section_len(
    values: &[Value],
    codec: &RowCodec,
    externalized: &[bool],
) -> usize {
    let row_layout = codec.active_layout();
    let n = row_layout.columns.len();
    let layout = &row_layout.value_layout;
    let bitmap_len = n.div_ceil(8);
    let ext_bitmap_len = layout.var_count.div_ceil(8);
    let mut len = varint_len(n as u64) + bitmap_len + layout.fixed_len + layout.var_count * 2;
    if externalized.iter().any(|&e| e) {
        len += ext_bitmap_len; // external bitmap over varlen columns
    }
    for layout_col in &row_layout.columns {
        let Some(value) = values.get(layout_col.column_index) else {
            continue;
        };
        if externalized
            .get(layout_col.column_index)
            .copied()
            .unwrap_or(false)
        {
            len += LOB_DESCRIPTOR_LEN;
        } else if !matches!(value, Value::Null) && fixed_width_for_kind(&layout_col.kind).is_none()
        {
            len += value_len_for_kind(&layout_col.kind, value).unwrap_or(0);
        }
    }
    len + 18 // worst-case MVCC header (version + flags + last_txn_id + undo_ptr)
}

#[cfg(test)]
pub(crate) fn decode_row_schema(input: &[u8], codec: &RowCodec) -> Result<Vec<Value>, QueryError> {
    decode_row_schema_ext(input, codec, false)
}

#[inline(always)]
fn read_dir_entry(
    input: &[u8],
    dir_start: usize,
    payload_start: usize,
    var_ord: usize,
) -> Result<(u32, u32), QueryError> {
    let start = dir_entry_offset(dir_start, var_ord);
    let end = start
        .checked_add(2)
        .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "corrupt row payload"))?;
    if end > payload_start || end > input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    let end_offset = u16::from_le_bytes(input[start..end].try_into().unwrap()) as u32;
    let prev_end = if var_ord == 0 {
        0
    } else {
        let prev_start = dir_entry_offset(dir_start, var_ord - 1);
        u16::from_le_bytes(input[prev_start..prev_start + 2].try_into().unwrap()) as u32
    };
    if end_offset < prev_end {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    Ok((prev_end, end_offset - prev_end))
}

fn checked_payload_slice(
    input: &[u8],
    payload_start: usize,
    offset: u32,
    len: u32,
) -> Result<&[u8], QueryError> {
    let start = payload_start
        .checked_add(offset as usize)
        .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "corrupt row payload"))?;
    let end = start
        .checked_add(len as usize)
        .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "corrupt row payload"))?;
    if end > input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    Ok(&input[start..end])
}

#[cfg(test)]
fn decode_row_schema_ext(
    input: &[u8],
    codec: &RowCodec,
    has_external: bool,
) -> Result<Vec<Value>, QueryError> {
    decode_row_schema_ext_for_layout(input, codec, codec.active_layout_id, has_external)
}

/// Borrowed row envelope shared by full, projected and typed-sink decoders.
/// No payload values are materialized here; each decoder keeps its own loop.
struct RowValueSection<'a> {
    null_bitmap_start: usize,
    fixed_start: usize,
    dir_start: usize,
    payload_start: usize,
    ext_bitmap: &'a [u8],
}

// Expand the envelope checks in the decoding caller: a large Result<Self>
// return can perturb register allocation in the per-row projected read path.
// The borrowed view and bounds policy remain shared across all four decoders.
macro_rules! row_value_section {
    ($input:expr, $pos:expr, $count:expr, $fixed_len:expr, $var_count:expr, $external:expr $(,)?) => {{
        let null_bitmap_start = $pos;
        let bitmap_len = $count.div_ceil(8);
        let ext_bitmap_len = $var_count.div_ceil(8);
        let ext_bitmap_start = null_bitmap_start + bitmap_len;
        let fixed_start = if $external {
            ext_bitmap_start + ext_bitmap_len
        } else {
            ext_bitmap_start
        };
        let dir_start = fixed_start + $fixed_len;
        let payload_start = dir_start + $var_count * 2;
        if payload_start > $input.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt row payload",
            ));
        }
        let ext_bitmap: &[u8] = if $external {
            &$input[ext_bitmap_start..ext_bitmap_start + ext_bitmap_len]
        } else {
            &[]
        };
        RowValueSection {
            null_bitmap_start,
            fixed_start,
            dir_start,
            payload_start,
            ext_bitmap,
        }
    }};
}

fn decode_row_schema_ext_for_layout(
    input: &[u8],
    codec: &RowCodec,
    layout_id: u32,
    has_external: bool,
) -> Result<Vec<Value>, QueryError> {
    let mut pos = 0usize;
    let count = read_varint(input, &mut pos)? as usize;
    let row_layout = codec.layout_for_id(layout_id)?;
    if count > row_layout.columns.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "row payload has more columns than its layout",
        ));
    }
    let stored_columns = &row_layout.columns[..count];
    let layout = value_section_layout_for_prefix(row_layout, stored_columns);
    let section = row_value_section!(
        input,
        pos,
        count,
        layout.fixed_len,
        layout.var_count,
        has_external,
    );
    let mut values = (0..codec.columns.len())
        .map(|idx| codec.default_for_column(idx))
        .collect::<Vec<_>>();
    for idx in 0..count {
        let layout_col = &stored_columns[idx];
        if column_is_null(input, section.null_bitmap_start, idx) {
            if let Some(slot) = values.get_mut(layout_col.column_index) {
                *slot = Value::Null;
            }
            continue;
        }
        let decoded = if let Some(fixed_off) = layout.fixed_offsets[idx] {
            let width = fixed_width_for_kind(&layout_col.kind).unwrap();
            decode_fixed_for_kind(
                &layout_col.kind,
                &input[section.fixed_start + fixed_off..section.fixed_start + fixed_off + width],
            )?
        } else {
            let var_ord = layout.var_ords[idx].expect("variable column must have var ord");
            let (offset, len) =
                read_dir_entry(input, section.dir_start, section.payload_start, var_ord)?;
            let payload = checked_payload_slice(input, section.payload_start, offset, len)?;
            if has_external && external_bit(section.ext_bitmap, var_ord) {
                let lob = LobRef::read_from(payload)?;
                materialize_external(layout_col.column_index, &layout_col.kind, lob)?
            } else {
                decode_var_for_kind(&layout_col.kind, payload)?
            }
        };
        if let Some(slot) = values.get_mut(layout_col.column_index) {
            *slot = decoded;
        }
    }
    Ok(values)
}

// ---------------------------------------------------------------------------
// In-place column patch (Phase 2 of the heap+index roadmap)
// ---------------------------------------------------------------------------
//
// The encode/decode functions above re-encode an entire row whenever
// any column changes. For UPDATE workloads that touch just a few
// columns (the OLTP common case), that wastes bytes on every level —
// WAL, undo, heap re-write, buffer pool dirty churn. The format
// already supports column-level random access (fixed columns live at
// known offsets inside the fixed block, varlen columns are addressed
// by the 2B cumulative-end directory), so an in-place patch just
// needs a helper that:
//
//   (a) skips the MVCC header and varint layout_id to reach the value
//       section, and
//   (b) for a given catalog column index, returns the (offset, width,
//       kind) tuple needed to overwrite the column's bytes.
//
// Only fixed-width columns are patched in place for now — same-length
// varlen patches are correct but rare (most UPDATEs of a string
// change its length). The caller falls back to a full re-encode when
// the helper returns `None`. NULL → non-NULL or non-NULL → NULL
// transitions also fall back, since they require both the null bitmap
// and the fixed slot to be touched atomically; pure
// non-NULL → non-NULL updates are the common case and the only one
// optimised here.

/// Byte range inside an encoded heap-row payload that holds the
/// fixed-width body of one column. Returned by
/// [`fixed_column_byte_range_in_payload`] when the column is eligible
/// for in-place patching.
#[derive(Debug, Clone)]
pub(crate) struct FixedColumnByteRange {
    /// Offset within the heap-row payload bytes (the same bytes
    /// `decode_heap_payload` receives — i.e. starting at the MVCC
    /// version byte).
    pub offset_in_payload: usize,
    /// Number of bytes occupied by the column body (matches
    /// `fixed_width_for_kind`).
    pub width: usize,
    /// The column's stored kind. Re-encoding a new value requires the
    /// kind (an `Int` column may be 1/2/3/4/8 bytes, a `DateTime`
    /// 5-8 bytes depending on fsp, etc.).
    #[allow(dead_code)] // Owned descriptor retained for the codec oracle.
    pub kind: ColumnKind,
}

/// Locate the byte range of `column_index`'s value within an encoded
/// heap-row payload. Returns `Some(range)` iff:
///   * `payload` is a regular row (not a forwarding stub, not a row
///     piece header, not corrupt),
///   * `column_index` is present in the row's persisted layout,
///   * the column is fixed-width per
///     [`fixed_width_for_kind`], and
///   * the column's current value is non-NULL.
///
/// Returns `Ok(None)` for any other shape (varlen, NULL, row piece,
/// forward stub, column dropped/added by online DDL). Caller treats
/// `None` as "not patchable, fall back to full row re-encode".
#[cfg(test)]
pub(crate) fn fixed_column_byte_range_in_payload(
    payload: &[u8],
    codec: &RowCodec,
    column_index: usize,
) -> Result<Option<FixedColumnByteRange>, QueryError> {
    if payload.first().copied() != Some(MVCC_ROW_VERSION) {
        // Forwarding stub or unknown version — not patchable.
        return Ok(None);
    }
    let (header, mut body_offset, has_external) = read_mvcc_header(payload)?;
    if header.flags & MVCC_FLAG_ROW_PIECE != 0 {
        // Row piece is part of a migrated row; the caller follows the
        // forwarding chain and patches the live page instead.
        return Ok(None);
    }
    let layout_id = read_varint(payload, &mut body_offset)? as u32;
    let body = &payload[body_offset..];
    // Delegate the value-section layout math to the body-relative helper,
    // then lift its body-relative offset back into a payload-relative one.
    Ok(
        fixed_column_byte_range_in_body(body, codec, layout_id, has_external, column_index)?.map(
            |range| FixedColumnByteRange {
                offset_in_payload: body_offset + range.offset_in_payload,
                ..range
            },
        ),
    )
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FixedColumnPatchRange<'a> {
    pub offset_in_payload: usize,
    pub width: usize,
    pub kind: &'a ColumnKind,
}

/// Locate a fixed slot even when NULL. Fixed slots always occupy their full
/// width; changing nullness therefore needs a bitmap patch, not a row rewrite.
/// Offsets are row-relative and cover the persisted layout, including old
/// layouts retained by INSTANT DDL.
pub(crate) fn fixed_column_patch_range<'a>(
    payload: &[u8],
    codec: &'a RowCodec,
    column_index: usize,
) -> Result<Option<(FixedColumnPatchRange<'a>, usize, u8)>, QueryError> {
    if payload.first().copied() != Some(MVCC_ROW_VERSION) {
        return Ok(None);
    }
    let (header, mut offset, has_external) = read_mvcc_header(payload)?;
    if header.flags & MVCC_FLAG_ROW_PIECE != 0 {
        return Ok(None);
    }
    let layout_id = read_varint(payload, &mut offset)? as u32;
    let row_layout = codec.layout_for_id(layout_id)?;
    let count = read_varint(payload, &mut offset)? as usize;
    if count > row_layout.columns.len() {
        return Ok(None);
    }
    let columns = &row_layout.columns[..count];
    let Some(ordinal) = columns
        .iter()
        .position(|column| column.column_index == column_index)
    else {
        return Ok(None);
    };
    let layout = value_section_layout_for_prefix(row_layout, columns);
    let fixed_start = offset
        + count.div_ceil(8)
        + if has_external {
            layout.var_count.div_ceil(8)
        } else {
            0
        };
    if fixed_start.saturating_add(layout.fixed_len) > payload.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    let Some(fixed_offset) = layout.fixed_offsets[ordinal] else {
        return Ok(None);
    };
    let width = fixed_width_for_kind(&columns[ordinal].kind).expect("fixed slot has a width");
    Ok(Some((
        FixedColumnPatchRange {
            offset_in_payload: fixed_start + fixed_offset,
            width,
            kind: &columns[ordinal].kind,
        },
        offset + ordinal / 8,
        1u8 << (ordinal % 8),
    )))
}

/// Body-relative sibling of [`fixed_column_byte_range_in_payload`]. `body`
/// is the row's value section — the slice starting at the stored-column
/// count varint (i.e. past the MVCC header AND the layout-id varint), with
/// `layout_id` / `has_external` already parsed off that header. Returns the
/// fixed column's byte range *relative to `body`*, with the same `None`
/// semantics as the payload-relative entry (column absent from the
/// persisted layout, varlen, or NULL).
///
/// The brute-force vector KNN scan uses this to score a `VECTOR(n)` column
/// straight off the heap payload bytes — one `read_unaligned`-friendly
/// slice per row — without re-parsing the MVCC header or materialising a
/// `Value::Vector` heap copy per row.
pub(crate) fn fixed_column_byte_range_in_body(
    body: &[u8],
    codec: &RowCodec,
    layout_id: u32,
    has_external: bool,
    column_index: usize,
) -> Result<Option<FixedColumnByteRange>, QueryError> {
    let row_layout = codec.layout_for_id(layout_id)?;
    let mut pos = 0usize;
    let count = read_varint(body, &mut pos)? as usize;
    if count > row_layout.columns.len() {
        return Ok(None);
    }
    let stored_columns = &row_layout.columns[..count];
    let Some(layout_idx) = stored_columns
        .iter()
        .position(|c| c.column_index == column_index)
    else {
        return Ok(None);
    };

    let layout = value_section_layout_for_prefix(row_layout, stored_columns);
    let null_bitmap_start = pos;
    let bitmap_len = count.div_ceil(8);
    let ext_bitmap_len = layout.var_count.div_ceil(8);
    let ext_bitmap_start = null_bitmap_start + bitmap_len;
    let fixed_start = if has_external {
        ext_bitmap_start + ext_bitmap_len
    } else {
        ext_bitmap_start
    };

    // Need enough bytes for null bitmap (+ ext bitmap) + fixed block.
    let fixed_end = fixed_start + layout.fixed_len;
    if fixed_end > body.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt row payload",
        ));
    }
    if column_is_null(body, null_bitmap_start, layout_idx) {
        return Ok(None);
    }
    let Some(fixed_off) = layout.fixed_offsets[layout_idx] else {
        // Varlen — caller can still in-place patch same-length values,
        // but that's a separate helper we haven't added yet.
        return Ok(None);
    };
    // `fixed_width_for_kind` cannot be None here because the
    // value-section layout only fills `fixed_offsets[i]` for kinds
    // that have a fixed width.
    let width = fixed_width_for_kind(&stored_columns[layout_idx].kind)
        .expect("fixed_offsets entry implies fixed width");
    Ok(Some(FixedColumnByteRange {
        offset_in_payload: fixed_start + fixed_off,
        width,
        kind: stored_columns[layout_idx].kind.clone(),
    }))
}

/// Encode `value` into `out` (which must be sized to the column's
/// fixed width). Pub(crate) wrapper around the codec-internal
/// `encode_fixed_for_kind` so the in-place UPDATE path can write a
/// new value into a heap page slot without going through
/// `encode_mvcc_row` (which would rebuild the entire row).
pub(crate) fn encode_fixed_value_into(
    kind: &ColumnKind,
    value: &Value,
    out: &mut [u8],
) -> Result<(), QueryError> {
    encode_fixed_for_kind(kind, value, out)
}

/// Decode a fixed-width column body. Used by the in-place patch path's
/// tests to re-decode before-image bytes captured during an UpdateDelta.
/// Production codepaths derive the before-image via Value cloning
/// rather than re-decoding bytes, so this function is currently only
/// hit from `#[cfg(test)]` code.
#[allow(dead_code)]
pub(crate) fn decode_fixed_value(kind: &ColumnKind, bytes: &[u8]) -> Result<Value, QueryError> {
    decode_fixed_for_kind(kind, bytes)
}

/// Pre-computed per-(projection) decoding plan. Hoists the `dest_of` /
/// `max_col` setup that `decode_row_schema_columns_into` used to redo
/// per row out to the batch caller (one INLJ batch, one streaming scan
/// page, …). For a 377K-row TPC-H lineitem probe this saves 377K
/// SmallVec allocations + setup loops.
///
/// **Fast steps** (`fast`): when `compile_with_codec` is called, the
/// plan also materialises a per-stored-column step list keyed by a
/// specific `layout_id`. The step list only contains *projected*
/// columns, so the per-row hot loop visits 7 entries for TPC-H Q1
/// instead of all 16 stored columns. Each step carries the
/// pre-resolved (src offset, width, scale-or-fsp, dest, op tag), so
/// the inner loop avoids the nested `fixed_width_for_kind` +
/// `decode_fixed_for_kind` enum matches that profile as ~4% on a
/// row-scan-heavy workload (TPC-H Q1). When the payload's
/// `layout_id` differs (legacy / migrated rows), or the projection
/// includes a column not in the cached layout's prefix, the loop
/// falls back to the generic per-stored-column walker.
#[derive(Debug, Clone)]
pub(crate) struct RowDecodePlan {
    /// `columns.len()` — how big `out` should be after a successful
    /// decode (every projected slot, including ones the payload did
    /// not store).
    pub(crate) out_len: usize,
    /// `max(columns) + 1`, clamped to `usize::MAX` so empty
    /// projections still produce a valid value.
    needed: usize,
    /// `dest_of[i] = j` means schema column `i` should be written into
    /// `out[j]`; `-1` means "skip".
    dest_of: smallvec::SmallVec<[i16; 32]>,
    columns: smallvec::SmallVec<[usize; 32]>,
    /// Compiled fast-path steps for a specific layout. `None` until
    /// `compile_with_codec` is called or when no specialisation
    /// applies. Walking these directly bypasses the
    /// per-stored-column enum dispatch.
    fast: Option<FastDecodeSteps>,
    /// Additional persisted layouts retained by online DDL/import history.
    /// Layout counts are normally 1-2, so a tiny linear lookup is cheaper
    /// than forcing every historical row through the generic decoder.
    fast_historical: smallvec::SmallVec<[FastDecodeSteps; 2]>,
}

/// Per-stored-column decode step. Compact enough (12 bytes) to keep
/// the step list cache-resident even for wide tables.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FastDecodeStep {
    /// Bit index in the null bitmap (== stored column ordinal).
    pub(crate) null_idx: u16,
    /// For fixed columns: offset into the fixed block. For var
    /// columns: the var ordinal used to look up the dir entry.
    pub(crate) src_loc: u16,
    /// Destination slot in `out`.
    pub(crate) dest: u16,
    /// For fixed columns: payload width in bytes. Zero for var.
    pub(crate) src_width: u8,
    /// Decimal scale (for `OP_DEC_*`) or temporal fsp (for `OP_TIME`
    /// / `OP_DATETIME`). Zero otherwise.
    pub(crate) scale_or_fsp: u8,
    /// `OP_*` constant. Selects the inline decode kernel; rare /
    /// complex kinds (Enum, Set, etc.) use `OP_FALLBACK` and route to
    /// the legacy per-kind decoder.
    pub(crate) op_tag: u8,
    /// False when the table schema proves this stored column NOT NULL.
    pub(crate) nullable: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct FastDecodeSteps {
    pub(crate) layout_id: u32,
    /// Value-section dimensions are invariant for the compiled layout.
    /// Keeping them here avoids resolving the layout and its value
    /// metadata once per row on the steady-state scan path.
    pub(crate) stored_column_count: usize,
    pub(crate) fixed_len: usize,
    pub(crate) var_count: usize,
    /// Complex kinds still need the full column codec to invoke the
    /// legacy decoder. Common numeric/string/date projections do not.
    pub(crate) has_fallback_step: bool,
    /// Steps for every projected column that is part of this layout's
    /// prefix. Walked in null-bitmap order so the bit test stride
    /// matches a forward scan.
    pub(crate) steps: smallvec::SmallVec<[FastDecodeStep; 16]>,
    /// `true` when every projected output cell is covered by a step
    /// (i.e. no projected schema column falls beyond the layout's
    /// stored prefix). When `false`, fast path is skipped — callers
    /// should still see the pre-filled `Value::Null` defaults.
    pub(crate) covers_full_projection: bool,
}

// Fast decode op tags. Numbering left as `u8` constants rather than a
// `repr(u8)` enum so we can branch on them in the inner loop without
// the optimizer second-guessing exhaustiveness.
pub(crate) const OP_FALLBACK: u8 = 0;
pub(crate) const OP_INT_S1: u8 = 1;
pub(crate) const OP_INT_S2: u8 = 2;
pub(crate) const OP_INT_S3: u8 = 3;
pub(crate) const OP_INT_S4: u8 = 4;
pub(crate) const OP_INT_S5: u8 = 5;
pub(crate) const OP_INT_S6: u8 = 6;
pub(crate) const OP_INT_S7: u8 = 7;
pub(crate) const OP_INT_S8: u8 = 8;
pub(crate) const OP_INT_U1: u8 = 9;
pub(crate) const OP_INT_U2: u8 = 10;
pub(crate) const OP_INT_U3: u8 = 11;
pub(crate) const OP_INT_U4: u8 = 12;
pub(crate) const OP_INT_U5: u8 = 13;
pub(crate) const OP_INT_U6: u8 = 14;
pub(crate) const OP_INT_U7: u8 = 15;
pub(crate) const OP_INT_U8: u8 = 16;
pub(crate) const OP_FLOAT32: u8 = 17;
pub(crate) const OP_FLOAT64: u8 = 18;
pub(crate) const OP_DEC_FIXED_W4: u8 = 19;
pub(crate) const OP_DEC_FIXED_W8: u8 = 20;
pub(crate) const OP_DEC_FIXED_W16: u8 = 21;
pub(crate) const OP_DATE: u8 = 22;
pub(crate) const OP_YEAR: u8 = 23;
pub(crate) const OP_FIXED_BYTES: u8 = 24;
pub(crate) const OP_VECTOR_FIXED: u8 = 25;
pub(crate) const OP_VAR_DECIMAL: u8 = 26;
pub(crate) const OP_VAR_STRING: u8 = 27;
pub(crate) const OP_VAR_BYTES: u8 = 28;
pub(crate) const OP_VAR_JSON: u8 = 29;
pub(crate) const OP_VAR_GEOMETRY: u8 = 30;
pub(crate) const OP_VAR_VECTOR: u8 = 31;
pub(crate) const OP_VAR_DECIMAL_WIDE: u8 = 32;
pub(crate) const OP_TIMESTAMP: u8 = 33;

/// Row-decode sink. Hot-path callers feed a `RowDecodeSink` to
/// [`decode_row_into_sink_for_layout`] which dispatches per-column
/// values directly through typed `put_*` methods, skipping the
/// `Vec<Value>` intermediate that profiling showed costing 12 % self
/// time on `core::ptr::drop_in_place<Value>` and 10 % on
/// `ColumnAccumulator::push` for TPC-H Q1 (vec=on).
///
/// Two impls live in the tree:
///   * [`ValueVecSink`] — drop-in for the legacy `Vec<Value>` output
///     buffer used by every storage path that hands rows back as
///     `&[Value]` to the engine. The body of
///     `decode_row_schema_columns_into_with_plan_ext_for_layout` is a
///     thin wrapper around `decode_row_into_sink_for_layout` with a
///     `ValueVecSink`, keeping behaviour byte-identical for that path.
///   * `BatchAccumSink` (in
///     [`crate::engine::executor::vec::batch`]) — writes straight into
///     the typed `ColumnAccumulator` lanes without ever constructing a
///     `Value`. Used by `scan_columns_as_batches` so the vectorized
///     executor's per-row Value envelope evaporates.
pub trait RowDecodeSink {
    /// Opt-in demand from a consumer that never observes the unused suffix.
    /// Text limits count Unicode scalar values; binary limits count bytes.
    /// NULL and non-string kinds retain the ordinary decoder semantics.
    fn column_prefix_limit(&self, _col: usize) -> Option<usize> {
        None
    }
    /// Whether `put_string` validates the borrowed payload itself. The
    /// default keeps storage-side UTF-8 validation for ordinary sinks. A
    /// predicate sink that already needs to inspect every byte can opt in so
    /// the decoder does not perform the same full pass immediately before it.
    fn validates_utf8_in_put_string(&self) -> bool {
        false
    }
    fn put_null(&mut self, col: usize);
    fn put_i64(&mut self, col: usize, value: i64);
    fn put_u64(&mut self, col: usize, value: u64);
    fn put_f32(&mut self, col: usize, value: f32);
    fn put_f64(&mut self, col: usize, value: f64);
    /// Magnitude fits `i64`, scale carried on the side.
    fn put_decimal_compact(&mut self, col: usize, magnitude: i64, scale: u8);
    /// Wide-magnitude decimal (DECIMAL(P>18)). Caller may demote to
    /// `Compact` when `i64::try_from` succeeds.
    fn put_decimal_wide(&mut self, col: usize, magnitude: i128, scale: u8);
    fn put_date(&mut self, col: usize, days: i32);
    fn put_year(&mut self, col: usize, year: u8);
    #[allow(dead_code)] // Reserved for codecs that preserve TIME as a typed lane.
    fn put_time(&mut self, col: usize, micros: i64);
    #[allow(dead_code)] // Reserved for codecs that preserve DATETIME as a typed lane.
    fn put_datetime(&mut self, col: usize, micros: i64);
    fn put_timestamp(&mut self, col: usize, micros: i64) {
        self.put_value(col, Value::Timestamp(micros));
    }
    /// Raw bytes (CHAR(N) binary, FixedBytes / VarBytes).
    fn put_bytes(&mut self, col: usize, bytes: &[u8]);
    /// UTF-8 string payload (VarString / CHAR/VARCHAR text).
    fn put_string(&mut self, col: usize, bytes: &[u8]);
    fn put_json(&mut self, col: usize, bytes: &[u8]);
    fn put_geometry(&mut self, col: usize, bytes: &[u8]);
    fn put_vector(&mut self, col: usize, bytes: &[u8]);
    /// Pre-formatted `Value` envelope. Fallback for paths that
    /// haven't yet been carved into a typed putter (Enum/Set,
    /// LOB-materialised externals, format conversions).
    fn put_value(&mut self, col: usize, value: Value);
}

#[cfg(test)]
/// Backwards-compatible row-mode sink that writes into a pre-sized
/// `Vec<Value>`. Indices map 1:1 to projected column indices in the
/// caller's [`RowDecodePlan`].
pub struct ValueVecSink<'a>(pub &'a mut Vec<Value>);

#[cfg(test)]
impl<'a> RowDecodeSink for ValueVecSink<'a> {
    #[inline]
    fn put_null(&mut self, col: usize) {
        self.0[col] = Value::Null;
    }
    #[inline]
    fn put_i64(&mut self, col: usize, value: i64) {
        self.0[col] = Value::Int(value);
    }
    #[inline]
    fn put_u64(&mut self, col: usize, value: u64) {
        self.0[col] = Value::UInt(value);
    }
    #[inline]
    fn put_f32(&mut self, col: usize, value: f32) {
        self.0[col] = Value::Float(value);
    }
    #[inline]
    fn put_f64(&mut self, col: usize, value: f64) {
        self.0[col] = Value::Double(value);
    }
    #[inline]
    fn put_decimal_compact(&mut self, col: usize, magnitude: i64, scale: u8) {
        self.0[col] = Value::DecimalCompact(magnitude, scale);
    }
    #[inline]
    fn put_decimal_wide(&mut self, col: usize, magnitude: i128, scale: u8) {
        if let Ok(small) = i64::try_from(magnitude) {
            self.0[col] = Value::DecimalCompact(small, scale);
        } else {
            let negative = magnitude < 0;
            self.0[col] =
                Value::Decimal(format_decimal_v3(magnitude.unsigned_abs(), scale, negative));
        }
    }
    #[inline]
    fn put_date(&mut self, col: usize, days: i32) {
        self.0[col] = Value::Date(days);
    }
    #[inline]
    fn put_year(&mut self, col: usize, year: u8) {
        self.0[col] = Value::Year(year);
    }
    #[inline]
    fn put_time(&mut self, col: usize, micros: i64) {
        self.0[col] = Value::Time(micros);
    }
    #[inline]
    fn put_datetime(&mut self, col: usize, micros: i64) {
        self.0[col] = Value::DateTime(micros);
    }
    #[inline]
    fn put_bytes(&mut self, col: usize, bytes: &[u8]) {
        if let Value::Bytes(v) = &mut self.0[col] {
            v.clear();
            v.extend_from_slice(bytes);
        } else {
            self.0[col] = Value::Bytes(bytes.to_vec());
        }
    }
    #[inline]
    fn put_string(&mut self, col: usize, bytes: &[u8]) {
        // Re-use the previous row's `String` buffer when the slot already
        // holds one (streaming callers pass the same scratch Vec per row);
        // the slot intentionally stays in the `String` lane even for
        // payloads short enough for `SmallString` — see
        // `decode_var_for_kind_into`.
        if let Value::String(s) = &mut self.0[col] {
            // Copy-then-validate, for the same unaligned-validation
            // reason as `decode_var_for_kind_into`.
            //
            // SAFETY: validated below before the String can be observed;
            // cleared on failure so the UTF-8 invariant holds.
            let buf = unsafe { s.as_mut_vec() };
            buf.clear();
            buf.extend_from_slice(bytes);
            if std::str::from_utf8(buf).is_err() {
                buf.clear();
                self.0[col] = Value::Bytes(bytes.to_vec());
            }
        } else if bytes.len() <= crate::types::SMALL_STRING_INLINE_CAP {
            self.0[col] = Value::SmallString(crate::types::SmallStringBuf::from_slice(bytes));
        } else {
            self.0[col] = match std::str::from_utf8(bytes) {
                Ok(s) => Value::String(s.to_owned()),
                Err(_) => Value::Bytes(bytes.to_vec()),
            };
        }
    }
    #[inline]
    fn put_json(&mut self, col: usize, bytes: &[u8]) {
        if let Value::Json(v) = &mut self.0[col] {
            v.clear();
            v.extend_from_slice(bytes);
        } else {
            self.0[col] = Value::Json(bytes.to_vec());
        }
    }
    #[inline]
    fn put_geometry(&mut self, col: usize, bytes: &[u8]) {
        if let Value::Geometry(v) = &mut self.0[col] {
            v.clear();
            v.extend_from_slice(bytes);
        } else {
            self.0[col] = Value::Geometry(bytes.to_vec());
        }
    }
    #[inline]
    fn put_vector(&mut self, col: usize, bytes: &[u8]) {
        if let Value::Vector(v) = &mut self.0[col] {
            v.datatype = crate::vector::VectorDataType::Float32;
            v.dimension = (bytes.len() / 4) as u32;
            v.clear();
            v.extend_from_slice(bytes);
        } else {
            self.0[col] = Value::Vector(bytes.to_vec().into());
        }
    }
    #[inline]
    fn put_value(&mut self, col: usize, value: Value) {
        self.0[col] = value;
    }
}

impl RowDecodePlan {
    pub(crate) fn empty() -> Self {
        Self {
            out_len: 0,
            needed: 0,
            dest_of: smallvec::SmallVec::new(),
            columns: smallvec::SmallVec::new(),
            fast: None,
            fast_historical: smallvec::SmallVec::new(),
        }
    }

    pub(crate) fn compile(columns: &[usize]) -> Self {
        if columns.is_empty() {
            return Self::empty();
        }
        let max_col = columns.iter().copied().max().unwrap_or(0);
        let needed = max_col + 1;
        let mut dest_of: smallvec::SmallVec<[i16; 32]> = smallvec::smallvec![-1; needed];
        for (out_idx, src_idx) in columns.iter().enumerate() {
            if *src_idx < needed {
                dest_of[*src_idx] = out_idx as i16;
            }
        }
        Self {
            out_len: columns.len(),
            needed,
            dest_of,
            columns: columns.iter().copied().collect(),
            fast: None,
            fast_historical: smallvec::SmallVec::new(),
        }
    }

    /// Like [`Self::compile`] but additionally pre-resolves the fast
    /// decode steps for the codec's active layout. The first row whose
    /// `layout_id` matches reaches the fast loop with no per-row enum
    /// dispatch; rows on other layouts (migration tail) silently fall
    /// back to the generic walker.
    pub(crate) fn compile_with_codec(columns: &[usize], codec: &RowCodec) -> Self {
        let mut plan = Self::compile(columns);
        plan.fast = build_fast_steps_for_layout(&plan, codec, codec.active_layout_id);
        plan.fast_historical = codec
            .layouts
            .iter()
            .filter(|layout| layout.layout_id != codec.active_layout_id)
            .filter_map(|layout| build_fast_steps_for_layout(&plan, codec, layout.layout_id))
            .collect();
        plan
    }

    /// Optional immutable decode recipe. Reserve a conservative capacity
    /// bound before building any SmallVec spills; reconcile to actual retained
    /// capacity. The reservation follows the last Arc owner, not the cache slot.
    pub(crate) fn try_compile_cached(
        columns: &[usize],
        codec: &RowCodec,
    ) -> Option<
        std::sync::Arc<(
            Self,
            crate::engine::executor::mem_budget::ControlledMemoryCharge,
        )>,
    > {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        fn spill_bound(count: usize, inline: usize, element: usize) -> usize {
            if count <= inline {
                0
            } else {
                count
                    .checked_next_power_of_two()
                    .unwrap_or(usize::MAX)
                    .saturating_mul(element)
            }
        }
        let needed = columns.iter().max().copied().unwrap_or(0).checked_add(1)?;
        let base = std::mem::size_of::<(Self, ControlledMemoryCharge)>()
            .saturating_add(2 * std::mem::size_of::<usize>());
        let bound = base
            .saturating_add(spill_bound(needed, 32, std::mem::size_of::<i16>()))
            .saturating_add(spill_bound(columns.len(), 32, std::mem::size_of::<usize>()))
            .saturating_add(spill_bound(
                codec.layouts.len(),
                2,
                std::mem::size_of::<FastDecodeSteps>(),
            ))
            .saturating_add(codec.layouts.len().saturating_mul(spill_bound(
                columns.len(),
                16,
                std::mem::size_of::<FastDecodeStep>(),
            )));
        let mut charge = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::DerivedCache,
            bound,
        )?;
        let plan = Self::compile_with_codec(columns, codec);
        let used = base.saturating_add(plan.heap_capacity_bytes());
        debug_assert!(used <= bound, "decode recipe capacity bound");
        if used > bound {
            // Defensive guard for future changes to SmallVec construction.
            return None;
        }
        charge.shrink(bound - used);
        Some(std::sync::Arc::new((plan, charge)))
    }

    fn heap_capacity_bytes(&self) -> usize {
        fn spill<A: smallvec::Array>(values: &smallvec::SmallVec<A>) -> usize {
            if values.spilled() {
                values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<A::Item>())
            } else {
                0
            }
        }
        spill(&self.dest_of)
            .saturating_add(spill(&self.columns))
            .saturating_add(spill(&self.fast_historical))
            .saturating_add(self.fast.as_ref().map_or(0, |fast| spill(&fast.steps)))
            .saturating_add(
                self.fast_historical
                    .iter()
                    .map(|fast| spill(&fast.steps))
                    .sum::<usize>(),
            )
    }

    #[cfg(test)]
    pub(crate) fn has_fast_steps(&self) -> bool {
        self.fast.is_some()
    }

    #[inline]
    fn fast_for_layout(&self, layout_id: u32) -> Option<&FastDecodeSteps> {
        self.fast
            .as_ref()
            .filter(|fast| fast.layout_id == layout_id)
            .or_else(|| {
                self.fast_historical
                    .iter()
                    .find(|fast| fast.layout_id == layout_id)
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FastDecodeFallbackReason {
    MissingLayout,
    ProjectionNotCovered,
    StoredCountMismatch,
    Unknown,
}

/// Classify a failed sink fast decode without materialising row values.  This
/// is intentionally called only on the fallback path; the extra varint read
/// therefore adds no work to successfully decoded rows and makes layout
/// regressions visible in EXPLAIN/SHOW diagnostics.
pub(crate) fn fast_decode_fallback_reason(
    input: &[u8],
    layout_id: u32,
    plan: &RowDecodePlan,
) -> FastDecodeFallbackReason {
    let Some(fast) = plan.fast_for_layout(layout_id) else {
        return FastDecodeFallbackReason::MissingLayout;
    };
    if !fast.covers_full_projection {
        return FastDecodeFallbackReason::ProjectionNotCovered;
    }
    let mut pos = 0usize;
    match read_varint(input, &mut pos) {
        Ok(count) if count as usize != fast.stored_column_count => {
            FastDecodeFallbackReason::StoredCountMismatch
        }
        _ => FastDecodeFallbackReason::Unknown,
    }
}

fn build_fast_steps_for_layout(
    plan: &RowDecodePlan,
    codec: &RowCodec,
    layout_id: u32,
) -> Option<FastDecodeSteps> {
    if plan.out_len == 0 {
        return None;
    }
    let row_layout = codec.layout_for_id(layout_id).ok()?;
    let value_layout = &row_layout.value_layout;
    let stop = plan.needed;
    let dest_of = plan.dest_of.as_slice();
    let mut steps: smallvec::SmallVec<[FastDecodeStep; 16]> =
        smallvec::SmallVec::with_capacity(plan.out_len);
    let mut covered = 0usize;
    for (idx, layout_col) in row_layout.columns.iter().enumerate() {
        let column_index = layout_col.column_index;
        if column_index >= stop {
            continue;
        }
        let dest = dest_of[column_index];
        if dest < 0 {
            continue;
        }
        let (op_tag, src_width, scale_or_fsp) = fast_op_for_kind(&layout_col.kind);
        let src_loc: u16 = if let Some(off) = value_layout.fixed_offsets[idx] {
            if off > u16::MAX as usize {
                return None;
            }
            off as u16
        } else if let Some(var_ord) = value_layout.var_ords[idx] {
            if var_ord > u16::MAX as usize {
                return None;
            }
            var_ord as u16
        } else {
            return None;
        };
        if idx > u16::MAX as usize || dest as i32 > u16::MAX as i32 {
            return None;
        }
        steps.push(FastDecodeStep {
            null_idx: idx as u16,
            src_loc,
            dest: dest as u16,
            src_width,
            scale_or_fsp,
            op_tag,
            nullable: layout_col.nullable,
        });
        covered += 1;
    }
    let covers_full_projection = covered == plan.out_len;
    Some(FastDecodeSteps {
        layout_id,
        stored_column_count: row_layout.columns.len(),
        fixed_len: value_layout.fixed_len,
        var_count: value_layout.var_count,
        has_fallback_step: steps.iter().any(|step| step.op_tag == OP_FALLBACK),
        steps,
        covers_full_projection,
    })
}

fn materialize_external_for_sink<S: RowDecodeSink>(
    sink: &S,
    dest: usize,
    col_pos: usize,
    kind: &ColumnKind,
    lob: LobRef,
) -> Result<Value, QueryError> {
    let Some(limit) = sink
        .column_prefix_limit(dest)
        .filter(|_| matches!(kind, ColumnKind::VarString | ColumnKind::VarBytes))
    else {
        return materialize_external(col_pos, kind, lob);
    };
    // Four UTF-8 bytes per character is a conservative upper bound. Keep
    // extra complete characters: the original SQL slice still evaluates and
    // produces the exact requested result. Never invent a replacement char
    // when the byte window ends inside a multibyte code point.
    let bytes = if matches!(kind, ColumnKind::VarString) {
        limit.saturating_mul(4)
    } else {
        limit
    } as u64;
    if bytes >= lob.total_len {
        return materialize_external(col_pos, kind, lob);
    }
    let value = LOB_MATERIALIZER.with(|cell| match cell.borrow().as_ref() {
        Some(reader) => reader.materialize_range(col_pos, &ColumnKind::VarBytes, lob, 0, bytes),
        None => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "externalized LOB column decoded without an overflow reader installed",
        )),
    })?;
    if !matches!(kind, ColumnKind::VarString) {
        return Ok(value);
    }
    let Value::Bytes(mut bytes) = value else {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid binary LOB prefix",
        ));
    };
    if let Err(err) = std::str::from_utf8(&bytes) {
        if err.error_len().is_some() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "invalid utf8 in externalized TEXT",
            ));
        }
        bytes.truncate(err.valid_up_to());
    }
    Ok(Value::String(String::from_utf8(bytes).map_err(|_| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid utf8 in externalized TEXT",
        )
    })?))
}

/// Sink-driven fast-path decoder. Returns `true` when the row was
/// decoded through the fast steps (skipping the `Vec<Value>` intermediate
/// entirely); returns `false` when the caller must fall back to the
/// legacy `Vec<Value>` path (because the row's layout differs from the
/// compiled fast steps, or the projection isn't covered).
///
/// Used by `scan_columns_as_batches` in the vectorized executor: when
/// `true` is returned, the [`RowDecodeSink`] saw exactly one typed
/// `put_*` call per projected column and no `Value` was ever allocated.
/// When `false`, the caller decodes via the existing
/// [`decode_row_schema_columns_into_with_plan_ext_for_layout`] into a
/// reusable `Vec<Value>` scratch and copies values from there.
pub(crate) fn decode_row_into_sink_fast<S: RowDecodeSink>(
    input: &[u8],
    codec: &RowCodec,
    layout_id: u32,
    plan: &RowDecodePlan,
    sink: &mut S,
    has_external: bool,
) -> Result<bool, QueryError> {
    let Some(fast) = plan.fast_for_layout(layout_id) else {
        return Ok(false);
    };
    if !fast.covers_full_projection {
        return Ok(false);
    }
    let (count, pos) = if fast.stored_column_count < 128
        && input.first().copied() == Some(fast.stored_column_count as u8)
    {
        (fast.stored_column_count, 1usize)
    } else {
        let mut pos = 0usize;
        let count = read_varint(input, &mut pos)? as usize;
        (count, pos)
    };
    if count != fast.stored_column_count {
        // Rows written before an INSTANT ADD COLUMN (and bulk loaders that
        // omit trailing all-default fields) legitimately store a prefix of
        // the active layout.  The old fast path rejected every such row and
        // forced callers to materialise a Vec<Value>.  Decode the projected
        // part of that prefix through the same typed sink while supplying
        // catalog defaults for projected trailing columns.
        if count > fast.stored_column_count {
            return Ok(false);
        }
        let row_layout = codec.layout_for_id(layout_id)?;
        let stored_columns = &row_layout.columns[..count];
        let layout = value_section_layout_for_prefix(row_layout, stored_columns);
        let section = row_value_section!(
            input,
            pos,
            count,
            layout.fixed_len,
            layout.var_count,
            has_external,
        );

        if codec.defaults_all_null() {
            for dest in 0..plan.out_len {
                sink.put_null(dest);
            }
        } else {
            for (dest, column) in plan.columns.iter().enumerate() {
                sink.put_value(dest, codec.default_for_column(*column));
            }
        }

        for step in fast.steps.iter() {
            let null_idx = step.null_idx as usize;
            if null_idx >= count {
                continue;
            }
            let dest = step.dest as usize;
            if step.nullable && column_is_null(input, section.null_bitmap_start, null_idx) {
                sink.put_null(dest);
                continue;
            }
            let layout_col = &stored_columns[null_idx];
            let op = step.op_tag;
            if op != OP_FALLBACK && (op < OP_VAR_DECIMAL || op == OP_TIMESTAMP) {
                let fixed_off = layout.fixed_offsets[null_idx]
                    .expect("fixed fast-decode column must have an offset");
                let width = step.src_width as usize;
                put_fixed_through_sink(
                    sink,
                    dest,
                    op,
                    step.scale_or_fsp,
                    &input
                        [section.fixed_start + fixed_off..section.fixed_start + fixed_off + width],
                );
                continue;
            }
            if op >= OP_VAR_DECIMAL {
                let var_ord = layout.var_ords[null_idx]
                    .expect("variable fast-decode column must have an ordinal");
                let (offset, len) =
                    read_dir_entry(input, section.dir_start, section.payload_start, var_ord)?;
                let payload = checked_payload_slice(input, section.payload_start, offset, len)?;
                if has_external && external_bit(section.ext_bitmap, var_ord) {
                    let lob = LobRef::read_from(payload)?;
                    sink.put_value(
                        dest,
                        materialize_external_for_sink(
                            sink,
                            dest,
                            layout_col.column_index,
                            &layout_col.kind,
                            lob,
                        )?,
                    );
                } else {
                    put_var_through_sink(sink, dest, op, step.scale_or_fsp, payload)?;
                }
                continue;
            }
            if let Some(fixed_off) = layout.fixed_offsets[null_idx] {
                let width = fixed_width_for_kind(&layout_col.kind).unwrap();
                sink.put_value(
                    dest,
                    decode_fixed_for_kind(
                        &layout_col.kind,
                        &input[section.fixed_start + fixed_off
                            ..section.fixed_start + fixed_off + width],
                    )?,
                );
            } else if let Some(var_ord) = layout.var_ords[null_idx] {
                let (offset, len) =
                    read_dir_entry(input, section.dir_start, section.payload_start, var_ord)?;
                let payload = checked_payload_slice(input, section.payload_start, offset, len)?;
                if has_external && external_bit(section.ext_bitmap, var_ord) {
                    let lob = LobRef::read_from(payload)?;
                    sink.put_value(
                        dest,
                        materialize_external_for_sink(
                            sink,
                            dest,
                            layout_col.column_index,
                            &layout_col.kind,
                            lob,
                        )?,
                    );
                } else {
                    sink.put_value(dest, decode_var_for_kind(&layout_col.kind, payload)?);
                }
            }
        }
        return Ok(true);
    }
    // The common fast path needs only the scalar value-section dimensions,
    // all of which were fixed when this decode plan was compiled. Resolve
    // the full layout only for external values or legacy fallback kinds.
    let row_layout = if has_external || fast.has_fallback_step {
        Some(codec.layout_for_id(layout_id)?)
    } else {
        None
    };
    let section = row_value_section!(
        input,
        pos,
        count,
        fast.fixed_len,
        fast.var_count,
        has_external,
    );

    for step in fast.steps.iter() {
        let null_idx = step.null_idx as usize;
        let dest = step.dest as usize;
        if step.nullable && column_is_null(input, section.null_bitmap_start, null_idx) {
            sink.put_null(dest);
            continue;
        }
        let op = step.op_tag;
        // Fast fixed-width lane.
        if op != OP_FALLBACK && (op < OP_VAR_DECIMAL || op == OP_TIMESTAMP) {
            let off = section.fixed_start + step.src_loc as usize;
            let w = step.src_width as usize;
            let bytes = &input[off..off + w];
            put_fixed_through_sink(sink, dest, op, step.scale_or_fsp, bytes);
            continue;
        }
        // Variable-length lane.
        if op >= OP_VAR_DECIMAL {
            let var_ord = step.src_loc as usize;
            let (offset, len) =
                read_dir_entry(input, section.dir_start, section.payload_start, var_ord)?;
            let payload = checked_payload_slice(input, section.payload_start, offset, len)?;
            if has_external && external_bit(section.ext_bitmap, var_ord) {
                let stored_columns = &row_layout
                    .expect("external rows resolve their compiled layout")
                    .columns;
                let layout_col = &stored_columns[null_idx];
                let lob = LobRef::read_from(payload)?;
                let value = materialize_external_for_sink(
                    sink,
                    dest,
                    layout_col.column_index,
                    &layout_col.kind,
                    lob,
                )?;
                sink.put_value(dest, value);
                continue;
            }
            put_var_through_sink(sink, dest, op, step.scale_or_fsp, payload)?;
            continue;
        }
        // OP_FALLBACK: route through the legacy per-kind decoder and
        // hand the materialised `Value` to the sink. Used by
        // Time/DateTime (FSP-dependent widths) and Enum/Set.
        let row_layout = row_layout.expect("fallback steps resolve their compiled layout");
        let stored_columns = &row_layout.columns;
        let layout = &row_layout.value_layout;
        let layout_col = &stored_columns[null_idx];
        if let Some(fixed_off) = layout.fixed_offsets[null_idx] {
            let width = fixed_width_for_kind(&layout_col.kind).unwrap();
            let value = decode_fixed_for_kind(
                &layout_col.kind,
                &input[section.fixed_start + fixed_off..section.fixed_start + fixed_off + width],
            )?;
            sink.put_value(dest, value);
        } else if let Some(var_ord) = layout.var_ords[null_idx] {
            let (offset, len) =
                read_dir_entry(input, section.dir_start, section.payload_start, var_ord)?;
            let payload = checked_payload_slice(input, section.payload_start, offset, len)?;
            if has_external && external_bit(section.ext_bitmap, var_ord) {
                let lob = LobRef::read_from(payload)?;
                let value = materialize_external_for_sink(
                    sink,
                    dest,
                    layout_col.column_index,
                    &layout_col.kind,
                    lob,
                )?;
                sink.put_value(dest, value);
            } else {
                let value = decode_var_for_kind(&layout_col.kind, payload)?;
                sink.put_value(dest, value);
            }
        }
    }
    Ok(true)
}

#[inline]
fn put_fixed_through_sink<S: RowDecodeSink>(
    sink: &mut S,
    dest: usize,
    op: u8,
    scale: u8,
    bytes: &[u8],
) {
    match op {
        OP_INT_S1 => sink.put_i64(dest, bytes[0] as i8 as i64),
        OP_INT_S2 => sink.put_i64(
            dest,
            i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        ),
        OP_INT_S3 => {
            let mut buf = [0u8; 4];
            buf[..3].copy_from_slice(&bytes[..3]);
            if buf[2] & 0x80 != 0 {
                buf[3] = 0xff;
            }
            sink.put_i64(dest, i32::from_le_bytes(buf) as i64);
        }
        OP_INT_S4 => sink.put_i64(
            dest,
            i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        ),
        OP_INT_S5 | OP_INT_S6 | OP_INT_S7 => {
            let w = (op - OP_INT_S1 + 1) as usize;
            let mut buf = [0u8; 8];
            buf[..w].copy_from_slice(&bytes[..w]);
            if buf[w - 1] & 0x80 != 0 {
                for b in &mut buf[w..] {
                    *b = 0xff;
                }
            }
            sink.put_i64(dest, i64::from_le_bytes(buf));
        }
        OP_INT_S8 => sink.put_i64(dest, i64::from_le_bytes(bytes[..8].try_into().unwrap())),
        OP_INT_U1 => sink.put_u64(dest, bytes[0] as u64),
        OP_INT_U2 => sink.put_u64(
            dest,
            u16::from_le_bytes(bytes[..2].try_into().unwrap()) as u64,
        ),
        OP_INT_U3 | OP_INT_U5 | OP_INT_U6 | OP_INT_U7 => {
            let w = (op - OP_INT_U1 + 1) as usize;
            let mut buf = [0u8; 8];
            buf[..w].copy_from_slice(&bytes[..w]);
            sink.put_u64(dest, u64::from_le_bytes(buf));
        }
        OP_INT_U4 => sink.put_u64(
            dest,
            u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64,
        ),
        OP_INT_U8 => sink.put_u64(dest, u64::from_le_bytes(bytes[..8].try_into().unwrap())),
        OP_FLOAT32 => sink.put_f32(dest, f32::from_le_bytes(bytes[..4].try_into().unwrap())),
        OP_FLOAT64 => sink.put_f64(dest, f64::from_le_bytes(bytes[..8].try_into().unwrap())),
        OP_DEC_FIXED_W4 => sink.put_decimal_compact(
            dest,
            i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
            scale,
        ),
        OP_DEC_FIXED_W8 => sink.put_decimal_compact(
            dest,
            i64::from_le_bytes(bytes[..8].try_into().unwrap()),
            scale,
        ),
        OP_DEC_FIXED_W16 => {
            let v = i128::from_le_bytes(bytes[..16].try_into().unwrap());
            sink.put_decimal_wide(dest, v, scale);
        }
        OP_DATE => {
            let mut buf = [0u8; 8];
            buf[..3].copy_from_slice(&bytes[..3]);
            if buf[2] & 0x80 != 0 {
                for b in &mut buf[3..] {
                    *b = 0xff;
                }
            }
            let stored = i64::from_le_bytes(buf);
            // Matches `decode_fixed_for_kind::Date` post HEAP_PAGE_VERSION 11:
            // signed_min_for_width(3) = -0x800000 is the zero-date sentinel;
            // every other value is the signed day count from 1970-01-01.
            let days = if stored == signed_min_for_width(3) {
                ZERO_DATE_SENTINEL
            } else {
                stored as i32
            };
            sink.put_date(dest, days);
        }
        OP_YEAR => sink.put_year(dest, bytes[0]),
        OP_TIMESTAMP => {
            let width = bytes.len();
            let mut buf = [0u8; 8];
            buf[..width].copy_from_slice(bytes);
            if bytes[width - 1] & 0x80 != 0 {
                for byte in &mut buf[width..] {
                    *byte = 0xff;
                }
            }
            let stored = i64::from_le_bytes(buf);
            let micros = if stored == signed_min_for_width(width) {
                ZERO_DATETIME_SENTINEL
            } else {
                stored * temporal_quantum(scale)
            };
            sink.put_timestamp(dest, micros);
        }
        OP_FIXED_BYTES => sink.put_bytes(dest, bytes),
        OP_VECTOR_FIXED => sink.put_vector(dest, bytes),
        _ => sink.put_null(dest),
    }
}

#[inline(always)]
fn put_var_through_sink<S: RowDecodeSink>(
    sink: &mut S,
    dest: usize,
    op: u8,
    scale: u8,
    payload: &[u8],
) -> Result<(), QueryError> {
    match op {
        OP_VAR_DECIMAL => {
            let mut pos = 0usize;
            let value = decode_decimal(payload, &mut pos, scale)?;
            sink.put_value(dest, value);
        }
        OP_VAR_DECIMAL_WIDE => {
            sink.put_value(dest, decode_decimal_wide(payload, scale)?);
        }
        OP_VAR_STRING => {
            // Match `decode_var_for_kind`'s VarString: validate UTF-8
            // and prefer the inline `SmallString` lane. The sink
            // chooses the on-disk representation for its lane. Predicate
            // sinks may fuse validation with comparison and opt out of this
            // otherwise duplicate payload pass.
            if !sink.validates_utf8_in_put_string()
                && !payload.is_ascii()
                && simdutf8::basic::from_utf8(payload).is_err()
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid utf8 in column payload",
                ));
            }
            sink.put_string(dest, payload);
        }
        OP_VAR_BYTES => sink.put_bytes(dest, payload),
        OP_VAR_JSON => sink.put_json(dest, payload),
        OP_VAR_GEOMETRY => sink.put_geometry(dest, payload),
        OP_VAR_VECTOR => sink.put_vector(dest, payload),
        _ => sink.put_null(dest),
    }
    Ok(())
}

/// Map a `ColumnKind` to an (op_tag, width, scale_or_fsp) tuple. Width is
/// 0 for var columns (their length is read from the dir entry at runtime).
/// `OP_FALLBACK` is emitted for shapes the fast loop doesn't specialise
/// (Enum/Set/etc.); the caller must route those rows through the legacy
/// per-kind decoder.
fn fast_op_for_kind(kind: &ColumnKind) -> (u8, u8, u8) {
    match kind {
        ColumnKind::Int { width, signed } => {
            let base = if *signed { OP_INT_S1 } else { OP_INT_U1 };
            let w = *width as u8;
            if w == 0 || w > 8 {
                return (OP_FALLBACK, 0, 0);
            }
            (base + w - 1, w, 0)
        }
        ColumnKind::Float32 => (OP_FLOAT32, 4, 0),
        ColumnKind::Float64 => (OP_FLOAT64, 8, 0),
        ColumnKind::DecimalFixed { width, scale } => {
            let op = match *width {
                4 => OP_DEC_FIXED_W4,
                8 => OP_DEC_FIXED_W8,
                16 => OP_DEC_FIXED_W16,
                _ => return (OP_FALLBACK, 0, 0),
            };
            (op, *width as u8, *scale)
        }
        ColumnKind::Date => (OP_DATE, 3, 0),
        ColumnKind::Year => (OP_YEAR, 1, 0),
        ColumnKind::FixedBytes { width } => {
            if *width > u8::MAX as usize {
                (OP_FALLBACK, 0, 0)
            } else {
                (OP_FIXED_BYTES, *width as u8, 0)
            }
        }
        // BIT round-trips as an integer, not raw bytes, so it takes the
        // general encode/decode arms rather than the byte-copy fast op.
        ColumnKind::Bit { .. } => (OP_FALLBACK, 0, 0),
        ColumnKind::VectorFixed { dim, datatype } => {
            let total = datatype.payload_len(*dim);
            if *datatype != crate::vector::VectorDataType::Float32 {
                return (OP_FALLBACK, 0, 0);
            }
            if total > u8::MAX as usize {
                (OP_FALLBACK, 0, 0)
            } else {
                (OP_VECTOR_FIXED, total as u8, 0)
            }
        }
        ColumnKind::Timestamp { fsp } => {
            let Some(width) = fixed_width_for_kind(kind) else {
                return (OP_FALLBACK, 0, 0);
            };
            (OP_TIMESTAMP, width as u8, *fsp)
        }
        // Time / DateTime have variable width depending on FSP; the fast
        // loop expects a fixed width for non-zero op_tag. Push them
        // through the fallback so correctness is unchanged.
        ColumnKind::Time { .. } | ColumnKind::DateTime { .. } => (OP_FALLBACK, 0, 0),
        ColumnKind::Enum { .. } | ColumnKind::Set { .. } => (OP_FALLBACK, 0, 0),
        ColumnKind::Decimal { scale } => (OP_VAR_DECIMAL, 0, *scale),
        ColumnKind::DecimalWide { scale } => (OP_VAR_DECIMAL_WIDE, 0, *scale),
        ColumnKind::VarString => (OP_VAR_STRING, 0, 0),
        ColumnKind::VarBytes => (OP_VAR_BYTES, 0, 0),
        ColumnKind::Json => (OP_VAR_JSON, 0, 0),
        ColumnKind::Geometry => (OP_VAR_GEOMETRY, 0, 0),
        ColumnKind::Vector { .. } => (OP_FALLBACK, 0, 0),
    }
}

#[cfg(test)]
pub(crate) fn decode_row_schema_columns_into(
    input: &[u8],
    codec: &RowCodec,
    columns: &[usize],
    out: &mut Vec<Value>,
) -> Result<usize, QueryError> {
    decode_row_schema_columns_into_ext(input, codec, columns, out, false)
}

#[cfg(test)]
pub(crate) fn decode_row_schema_columns_into_ext(
    input: &[u8],
    codec: &RowCodec,
    columns: &[usize],
    out: &mut Vec<Value>,
    has_external: bool,
) -> Result<usize, QueryError> {
    let plan = RowDecodePlan::compile(columns);
    decode_row_schema_columns_into_with_plan_ext(input, codec, &plan, out, has_external)
}

#[cfg(test)]
pub(crate) fn decode_row_schema_columns_into_with_plan(
    input: &[u8],
    codec: &RowCodec,
    plan: &RowDecodePlan,
    out: &mut Vec<Value>,
) -> Result<usize, QueryError> {
    decode_row_schema_columns_into_with_plan_ext(input, codec, plan, out, false)
}

#[cfg(test)]
pub(crate) fn decode_row_schema_columns_into_with_plan_ext(
    input: &[u8],
    codec: &RowCodec,
    plan: &RowDecodePlan,
    out: &mut Vec<Value>,
    has_external: bool,
) -> Result<usize, QueryError> {
    decode_row_schema_columns_into_with_plan_ext_for_layout(
        input,
        codec,
        codec.active_layout_id,
        plan,
        out,
        has_external,
    )
}

const LAYOUT_AWARE_STORED_COUNT: usize = usize::MAX;

pub(crate) fn decode_row_schema_columns_into_with_plan_ext_for_layout(
    input: &[u8],
    codec: &RowCodec,
    layout_id: u32,
    plan: &RowDecodePlan,
    out: &mut Vec<Value>,
    has_external: bool,
) -> Result<usize, QueryError> {
    let mut pos = 0usize;
    let count = read_varint(input, &mut pos)? as usize;
    let row_layout = codec.layout_for_id(layout_id)?;
    if count > row_layout.columns.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "row payload has more columns than its layout",
        ));
    }
    let stored_columns = &row_layout.columns[..count];
    let layout = value_section_layout_for_prefix(row_layout, stored_columns);
    let section = row_value_section!(
        input,
        pos,
        count,
        layout.fixed_len,
        layout.var_count,
        has_external,
    );
    if plan.out_len == 0 {
        out.clear();
        return Ok(LAYOUT_AWARE_STORED_COUNT);
    }

    // Fast path: pre-compiled steps for this layout cover the full
    // projection. Walk only the projected stored columns (skipping the
    // non-projected ones entirely) and dispatch each through a small
    // u8-tag match that the optimizer turns into a jump table. This
    // bypasses both the `fixed_width_for_kind` and `decode_fixed_for_kind`
    // nested `ColumnKind` matches the legacy loop runs per row per
    // stored column (profiled at ~4% self time on TPC-H Q1).
    //
    // `covers_full_projection` guarantees every projected output cell is
    // written by exactly one step (NULL or value), so the previous row's
    // values don't need to be cleared first — each step overwrites its
    // destination in place. Streaming scans hand the same `out` buffer
    // back row after row, which lets `decode_var_for_kind_into` re-use
    // the String/Vec allocation already sitting in the cell instead of
    // paying a malloc+free pair per row per var-length column (~40% of
    // the TPC-H Q13 scan profile before this).
    if let Some(fast) = plan.fast_for_layout(layout_id) {
        if fast.covers_full_projection && count == row_layout.columns.len() {
            if out.len() != plan.out_len {
                out.clear();
                out.resize_with(plan.out_len, || Value::Null);
            }
            for step in fast.steps.iter() {
                let null_idx = step.null_idx as usize;
                if column_is_null(input, section.null_bitmap_start, null_idx) {
                    out[step.dest as usize] = Value::Null;
                    continue;
                }
                let op = step.op_tag;
                if op == OP_FALLBACK || op >= OP_VAR_DECIMAL {
                    // Fallback: routes through the legacy per-kind
                    // decoder. Used by Time/DateTime (FSP-dependent
                    // widths), Enum/Set, and variable-length payloads
                    // that need `read_dir_entry` + external-bitmap
                    // gating. The hot inner loop above never sees
                    // these on TPC-H Q1.
                    let layout_col = &stored_columns[null_idx];
                    if let Some(fixed_off) = layout.fixed_offsets[null_idx] {
                        let width = fixed_width_for_kind(&layout_col.kind).unwrap();
                        out[step.dest as usize] = decode_fixed_for_kind(
                            &layout_col.kind,
                            &input[section.fixed_start + fixed_off
                                ..section.fixed_start + fixed_off + width],
                        )?;
                    } else if let Some(var_ord) = layout.var_ords[null_idx] {
                        let (offset, len) = read_dir_entry(
                            input,
                            section.dir_start,
                            section.payload_start,
                            var_ord,
                        )?;
                        let payload =
                            checked_payload_slice(input, section.payload_start, offset, len)?;
                        if has_external && external_bit(section.ext_bitmap, var_ord) {
                            let lob = LobRef::read_from(payload)?;
                            out[step.dest as usize] = materialize_external(
                                layout_col.column_index,
                                &layout_col.kind,
                                lob,
                            )?;
                        } else {
                            decode_var_for_kind_into(
                                &layout_col.kind,
                                payload,
                                &mut out[step.dest as usize],
                            )?;
                        }
                    }
                    continue;
                }
                // Fixed-width fast lane.
                let off = section.fixed_start + step.src_loc as usize;
                let w = step.src_width as usize;
                let bytes = &input[off..off + w];
                out[step.dest as usize] = decode_fixed_by_op(op, step.scale_or_fsp, bytes);
            }
            return Ok(LAYOUT_AWARE_STORED_COUNT);
        }
    }

    // Pre-fill output cells. Fast path: when every codec-level default is
    // `Value::Null` (the common case — `RowCodec.defaults` is empty unless
    // a future INSTANT ADD non-NULL default has been installed), a single
    // `resize_with(_, || Value::Null)` is dramatically cheaper than an
    // iterator-driven `extend(map(default_for_column))`. `Value::Null` is
    // a Copy-shaped enum tag, so the per-cell cost collapses to a tag
    // store. The stored-column loop below will overwrite every cell whose
    // `column_index < plan.needed`; projected columns that fall *outside*
    // the stored layout's prefix (legacy rows from before an INSTANT ADD)
    // legitimately remain `Value::Null` here, matching the existing
    // `defaults_all_null` precondition.
    out.clear();
    if codec.defaults_all_null() {
        out.resize_with(plan.out_len, || Value::Null);
    } else {
        out.extend(
            plan.columns
                .iter()
                .map(|col_idx| codec.default_for_column(*col_idx)),
        );
    }

    let stop = plan.needed;
    let dest_of = plan.dest_of.as_slice();
    for idx in 0..count {
        let layout_col = &stored_columns[idx];
        let column_index = layout_col.column_index;
        if column_is_null(input, section.null_bitmap_start, idx) {
            if column_index < stop {
                let dest = dest_of[column_index];
                if dest >= 0 {
                    out[dest as usize] = Value::Null;
                }
            }
            continue;
        }
        let dest = if column_index < stop {
            dest_of[column_index]
        } else {
            -1
        };
        if dest < 0 {
            continue;
        }
        if let Some(fixed_off) = layout.fixed_offsets[idx] {
            let width = fixed_width_for_kind(&layout_col.kind).unwrap();
            out[dest as usize] = decode_fixed_for_kind(
                &layout_col.kind,
                &input[section.fixed_start + fixed_off..section.fixed_start + fixed_off + width],
            )?;
            continue;
        }
        let var_ord = layout.var_ords[idx].expect("variable column must have var ord");
        let (offset, len) =
            read_dir_entry(input, section.dir_start, section.payload_start, var_ord)?;
        let payload = checked_payload_slice(input, section.payload_start, offset, len)?;
        if has_external && external_bit(section.ext_bitmap, var_ord) {
            let lob = LobRef::read_from(payload)?;
            out[dest as usize] = materialize_external(column_index, &layout_col.kind, lob)?;
        } else {
            out[dest as usize] = decode_var_for_kind(&layout_col.kind, payload)?;
        }
    }
    Ok(LAYOUT_AWARE_STORED_COUNT)
}

/// Per-op-tag fixed-width decoder. Cannot fail for the supported tags
/// — every `OP_*` constant emitted by `fast_op_for_kind` is paired with
/// an exact byte width, so byte slicing and `try_into().unwrap()` are
/// infallible. Time / DateTime / Enum / Set / wide-magnitude DECIMAL
/// overflows are routed through `OP_FALLBACK` and never reach here.
#[inline]
fn decode_fixed_by_op(op: u8, scale: u8, bytes: &[u8]) -> Value {
    match op {
        OP_INT_S1 => Value::Int(bytes[0] as i8 as i64),
        OP_INT_S2 => Value::Int(i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64),
        OP_INT_S3 => {
            let mut buf = [0u8; 4];
            buf[..3].copy_from_slice(&bytes[..3]);
            if buf[2] & 0x80 != 0 {
                buf[3] = 0xff;
            }
            Value::Int(i32::from_le_bytes(buf) as i64)
        }
        OP_INT_S4 => Value::Int(i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64),
        OP_INT_S5 | OP_INT_S6 | OP_INT_S7 => {
            // Sign-extend 5/6/7 bytes into i64.
            let w = (op - OP_INT_S1 + 1) as usize;
            let mut buf = [0u8; 8];
            buf[..w].copy_from_slice(&bytes[..w]);
            if buf[w - 1] & 0x80 != 0 {
                for b in &mut buf[w..] {
                    *b = 0xff;
                }
            }
            Value::Int(i64::from_le_bytes(buf))
        }
        OP_INT_S8 => Value::Int(i64::from_le_bytes(bytes[..8].try_into().unwrap())),
        OP_INT_U1 => Value::UInt(bytes[0] as u64),
        OP_INT_U2 => Value::UInt(u16::from_le_bytes(bytes[..2].try_into().unwrap()) as u64),
        OP_INT_U3 | OP_INT_U5 | OP_INT_U6 | OP_INT_U7 => {
            let w = (op - OP_INT_U1 + 1) as usize;
            let mut buf = [0u8; 8];
            buf[..w].copy_from_slice(&bytes[..w]);
            Value::UInt(u64::from_le_bytes(buf))
        }
        OP_INT_U4 => Value::UInt(u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64),
        OP_INT_U8 => Value::UInt(u64::from_le_bytes(bytes[..8].try_into().unwrap())),
        OP_FLOAT32 => Value::Float(f32::from_le_bytes(bytes[..4].try_into().unwrap())),
        OP_FLOAT64 => Value::Double(f64::from_le_bytes(bytes[..8].try_into().unwrap())),
        OP_DEC_FIXED_W4 => Value::DecimalCompact(
            i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
            scale,
        ),
        OP_DEC_FIXED_W8 => {
            Value::DecimalCompact(i64::from_le_bytes(bytes[..8].try_into().unwrap()), scale)
        }
        OP_DEC_FIXED_W16 => {
            let v = i128::from_le_bytes(bytes[..16].try_into().unwrap());
            if let Ok(small) = i64::try_from(v) {
                Value::DecimalCompact(small, scale)
            } else {
                let negative = v < 0;
                Value::Decimal(format_decimal_v3(v.unsigned_abs(), scale, negative))
            }
        }
        OP_DATE => {
            let mut buf = [0u8; 8];
            buf[..3].copy_from_slice(&bytes[..3]);
            if buf[2] & 0x80 != 0 {
                for b in &mut buf[3..] {
                    *b = 0xff;
                }
            }
            let stored = i64::from_le_bytes(buf);
            // See `decode_fixed_for_kind::Date` post HEAP_PAGE_VERSION 11.
            Value::Date(if stored == signed_min_for_width(3) {
                ZERO_DATE_SENTINEL
            } else {
                stored as i32
            })
        }
        OP_YEAR => Value::Year(bytes[0]),
        OP_FIXED_BYTES => Value::Bytes(bytes.to_vec()),
        OP_VECTOR_FIXED => Value::Vector(bytes.to_vec().into()),
        // Unknown / unsupported ops should never reach here because the
        // step list is built by `fast_op_for_kind`, which routes them
        // through `OP_FALLBACK`. Use a defensive return rather than
        // `unreachable!()` so a bad tag downgrades to NULL instead of
        // panicking on an in-flight query.
        _ => Value::Null,
    }
}

// --- per-kind encode/decode/skip dispatch --------------------------------

fn value_len_for_kind(kind: &ColumnKind, value: &Value) -> Result<usize, QueryError> {
    if let Some(width) = fixed_width_for_kind(kind) {
        return Ok(width);
    }
    Ok(match kind {
        ColumnKind::Decimal { scale } => {
            // sign + nbytes + magnitude — compute via the same path the
            // encoder uses so length stays consistent with the on-disk
            // representation (the magnitude byte count depends on the
            // value).
            let (_, magnitude) = parse_decimal_to_magnitude_from_value(value, *scale)?;
            2 + decimal_magnitude_nbytes(magnitude)
        }
        ColumnKind::DecimalWide { scale } => {
            let mut encoded = Vec::with_capacity(36);
            encode_decimal_wide(value, *scale, &mut encoded)?;
            encoded.len()
        }
        ColumnKind::VarString => {
            let bytes = value_to_string_bytes(value);
            bytes.len()
        }
        ColumnKind::VarBytes => {
            let bytes = value_to_bytes(value);
            bytes.len()
        }
        ColumnKind::Json => {
            let bytes = json_coerce_to_jsonb(value)?;
            bytes.len()
        }
        ColumnKind::Geometry => {
            let bytes = geometry_coerce_to_internal(value)?;
            bytes.len()
        }
        ColumnKind::Vector { dim, datatype } => {
            let bytes = vector_coerce_to_internal(value, *datatype, *dim)?;
            bytes.len()
        }
        _ => unreachable!("fixed-width kinds returned above"),
    })
}

fn encode_fixed_for_kind(
    kind: &ColumnKind,
    value: &Value,
    out: &mut [u8],
) -> Result<(), QueryError> {
    let width = out.len();
    match kind {
        ColumnKind::Int { width, signed } => {
            encode_int_into_slice(value, *width, *signed, out)?;
        }
        ColumnKind::Float32 => out.copy_from_slice(&value_to_f32(value).to_le_bytes()),
        ColumnKind::Float64 => out.copy_from_slice(&value_to_f64(value).to_le_bytes()),
        ColumnKind::DecimalFixed { width, scale } => {
            encode_decimal_fixed_into_slice(value, *width, *scale, out)?;
        }
        ColumnKind::Date => {
            let days = match value {
                Value::Date(d) => *d,
                Value::Int(v) => *v as i32,
                Value::String(s) | Value::Decimal(s) => parse_date_to_days(s)?,
                Value::Bytes(b) => parse_date_to_days(&String::from_utf8_lossy(b))?,
                _ => 0,
            };
            // Encode zero-date as `signed_min_for_width(width)` (mirrors
            // DateTime/Timestamp). Any real date encodes as its signed day
            // count. Old scheme `days + 1` collided at days=-1 (1969-12-31)
            // with the zero-date sentinel — HEAP_PAGE_VERSION bumped to 11.
            let stored = if days == ZERO_DATE_SENTINEL {
                signed_min_for_width(width)
            } else {
                days as i64
            };
            write_signed_le_width(stored, out)?;
        }
        ColumnKind::Time { fsp } => {
            let micros = match value {
                Value::Time(t) => *t,
                Value::Int(v) => *v,
                Value::String(s) | Value::Decimal(s) => parse_time_to_micros(s)?,
                Value::Bytes(b) => parse_time_to_micros(&String::from_utf8_lossy(b))?,
                _ => 0,
            };
            write_signed_le_width(micros / temporal_quantum(*fsp), out)?;
        }
        ColumnKind::DateTime { fsp } | ColumnKind::Timestamp { fsp } => {
            let micros = match value {
                Value::DateTime(dt) | Value::Timestamp(dt) => *dt,
                Value::Int(v) => *v,
                Value::String(s) | Value::Decimal(s) => parse_datetime_to_micros(s)?,
                Value::Bytes(b) => parse_datetime_to_micros(&String::from_utf8_lossy(b))?,
                _ => 0,
            };
            let stored = if micros == ZERO_DATETIME_SENTINEL {
                signed_min_for_width(width)
            } else if let Some(parts) = crate::types::decode_tagged_datetime(micros) {
                encode_tagged_datetime_for_width(parts, *fsp, width)?
            } else {
                micros / temporal_quantum(*fsp)
            };
            write_signed_le_width(stored, out)?;
        }
        ColumnKind::Year => {
            out[0] = match value {
                Value::Year(y) => *y,
                Value::Int(v) => crate::util::temporal::year_byte_from_int(*v)?,
                Value::UInt(v) => crate::util::temporal::year_byte_from_int(*v as i64)?,
                Value::String(s) | Value::Decimal(s) => parse_year(s)?,
                Value::Bytes(b) => parse_year(&String::from_utf8_lossy(b))?,
                _ => 0,
            };
        }
        ColumnKind::FixedBytes { width } => {
            // Borrowed-byte fast path. Profile of LOAD DATA lineitem
            // showed `value_to_bytes` as the heaviest memmove source
            // (180 samples) because every `CHAR(N)` cell was building
            // an intermediate `Vec` (e.g. `SmallString -> b.as_slice().to_vec()`)
            // only to copy out of it into the fixed-width destination
            // slice. Copy straight from the borrowed payload instead.
            let _ = *width; // `out` is already sized to the column width.
            let copied = match value {
                Value::Bytes(b) => copy_into_fixed_slice(out, b),
                Value::String(s) | Value::Decimal(s) => copy_into_fixed_slice(out, s.as_bytes()),
                Value::SmallString(b) => copy_into_fixed_slice(out, b.as_slice()),
                Value::Null => 0,
                _ => {
                    let bytes = value_to_bytes(value);
                    copy_into_fixed_slice(out, &bytes)
                }
            };
            if copied < out.len() {
                out[copied..].fill(0);
            }
        }
        ColumnKind::Bit { .. } => {
            // BIT(M): the coercion step has already validated the value
            // against M bits and yields an integer. Store it big-endian,
            // right-aligned within the `ceil(M/8)`-byte fixed slot.
            let n = bit_value_to_u64(value);
            let be = n.to_be_bytes();
            let width = out.len();
            // width <= 8 always holds (M <= 64), so the low `width` bytes of
            // the big-endian u64 are exactly the M-bit value.
            out.copy_from_slice(&be[be.len() - width..]);
        }
        ColumnKind::VectorFixed { dim, datatype } => {
            let coerced;
            let bytes = match value {
                Value::Vector(v) if v.datatype == *datatype && v.dimension == *dim => {
                    crate::engine::vector::validate_internal(v)?;
                    v.payload.as_slice()
                }
                _ => {
                    coerced = crate::engine::vector::codec::coerce_value(
                        value.clone(),
                        *datatype,
                        Some(*dim),
                    )?;
                    coerced.payload.as_slice()
                }
            };
            let expected = datatype.payload_len(*dim);
            if bytes.len() != expected {
                return Err(error(
                    crate::error::catalog::MYDB_ER_VECTOR_DIMENSION_MISMATCH,
                    format!("VECTOR({dim}) value has {} bytes", bytes.len()),
                ));
            }
            out.copy_from_slice(bytes);
        }
        ColumnKind::Enum { values, width } => {
            let ord = enum_ordinal(value, values)?;
            write_unsigned_le_width(ord as u64, &mut out[..*width as usize])?;
        }
        ColumnKind::Set { values, width } => {
            let mask = set_mask(value, values)?;
            write_unsigned_le_width(mask, &mut out[..*width as usize])?;
        }
        ColumnKind::Decimal { .. }
        | ColumnKind::DecimalWide { .. }
        | ColumnKind::VarString
        | ColumnKind::VarBytes
        | ColumnKind::Json
        | ColumnKind::Geometry
        | ColumnKind::Vector { .. } => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "variable column in fixed encoder",
            ))
        }
    }
    Ok(())
}

fn decode_fixed_for_kind(kind: &ColumnKind, input: &[u8]) -> Result<Value, QueryError> {
    let mut pos = 0usize;
    match kind {
        ColumnKind::Int { width, signed } => decode_int(*width, *signed, input, &mut pos),
        ColumnKind::Float32 => Ok(Value::Float(f32::from_le_bytes(input.try_into().unwrap()))),
        ColumnKind::Float64 => Ok(Value::Double(f64::from_le_bytes(input.try_into().unwrap()))),
        ColumnKind::DecimalFixed { width, scale } => {
            decode_decimal_fixed(input, &mut pos, *width, *scale)
        }
        ColumnKind::Date => {
            let stored = read_signed_le_width(input)?;
            Ok(Value::Date(
                if stored == signed_min_for_width(input.len()) {
                    ZERO_DATE_SENTINEL
                } else {
                    stored as i32
                },
            ))
        }
        ColumnKind::Time { fsp } => Ok(Value::Time(
            read_signed_le_width(input)? * temporal_quantum(*fsp),
        )),
        ColumnKind::DateTime { fsp } => {
            let stored = read_signed_le_width(input)?;
            Ok(Value::DateTime(
                if stored == signed_min_for_width(input.len()) {
                    ZERO_DATETIME_SENTINEL
                } else if let Some(parts) =
                    decode_tagged_datetime_for_width(stored, *fsp, input.len())
                {
                    crate::types::encode_tagged_datetime(
                        parts.0, parts.1, parts.2, parts.3, parts.4, parts.5, parts.6,
                    )
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "invalid tagged datetime payload",
                        )
                    })?
                } else {
                    stored * temporal_quantum(*fsp)
                },
            ))
        }
        ColumnKind::Timestamp { fsp } => {
            let stored = read_signed_le_width(input)?;
            Ok(Value::Timestamp(
                if stored == signed_min_for_width(input.len()) {
                    ZERO_DATETIME_SENTINEL
                } else {
                    stored * temporal_quantum(*fsp)
                },
            ))
        }
        ColumnKind::Year => Ok(Value::Year(input[0])),
        ColumnKind::FixedBytes { .. } => Ok(Value::Bytes(input.to_vec())),
        ColumnKind::Bit { .. } => {
            // Reassemble the big-endian bytes into the integer BIT value.
            let mut n = 0u64;
            for &b in input {
                n = (n << 8) | b as u64;
            }
            Ok(Value::UInt(n))
        }
        ColumnKind::VectorFixed { dim, datatype } => Ok(Value::Vector(
            crate::vector::VectorValue::new(*datatype, *dim, input.to_vec()),
        )),
        ColumnKind::Enum { values, width } => {
            let ord = read_unsigned_le_width(&input[..*width as usize])? as usize;
            if ord == 0 {
                Ok(Value::Bytes(Vec::new()))
            } else {
                let value = values.get(ord - 1).cloned().unwrap_or_default();
                if value.is_empty() {
                    Ok(Value::SmallString(
                        crate::types::SmallStringBuf::from_slice(b""),
                    ))
                } else {
                    Ok(Value::String(value))
                }
            }
        }
        ColumnKind::Set { values, width } => {
            let mask = read_unsigned_le_width(&input[..*width as usize])?;
            let s = values
                .iter()
                .enumerate()
                .filter_map(|(idx, v)| ((mask & (1u64 << idx)) != 0).then(|| v.as_str()))
                .collect::<Vec<_>>()
                .join(",");
            Ok(Value::String(s))
        }
        ColumnKind::Decimal { .. }
        | ColumnKind::DecimalWide { .. }
        | ColumnKind::VarString
        | ColumnKind::VarBytes
        | ColumnKind::Json
        | ColumnKind::Geometry
        | ColumnKind::Vector { .. } => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "variable column in fixed decoder",
        )),
    }
}

fn encode_var_for_kind(
    kind: &ColumnKind,
    value: &Value,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    match kind {
        ColumnKind::Decimal { scale } => encode_decimal(value, *scale, out)?,
        ColumnKind::DecimalWide { scale } => encode_decimal_wide(value, *scale, out)?,
        // Borrowed-byte fast path: skips a per-row `Vec<u8>` allocation
        // that `value_to_string_bytes` / `value_to_bytes` would otherwise
        // build only to be immediately `extend_from_slice`'d into `out`.
        // Profile (TPC-H SF=1 lineitem PARALLEL(2)) attributed 525 of the
        // worker thread's malloc_tiny samples to `value_to_string_bytes`
        // and 138 memmove samples to its `to_vec`+extend chain — i.e.
        // ~7% of total CPU spent allocating temporaries to copy from.
        ColumnKind::VarString => append_string_value_bytes_inline(value, out),
        ColumnKind::VarBytes => append_bytes_value_bytes_inline(value, out),
        ColumnKind::Json => out.extend_from_slice(&json_coerce_to_jsonb(value)?),
        ColumnKind::Geometry => out.extend_from_slice(&geometry_coerce_to_internal(value)?),
        ColumnKind::Vector { dim, datatype } => {
            out.extend_from_slice(&vector_coerce_to_internal_cow(value, *datatype, *dim)?)
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "fixed column in var encoder",
            ))
        }
    }
    Ok(())
}

#[inline]
fn append_string_value_bytes_inline(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::String(s) | Value::Decimal(s) => out.extend_from_slice(s.as_bytes()),
        Value::SmallString(b) => out.extend_from_slice(b.as_slice()),
        Value::Bytes(b) => out.extend_from_slice(b),
        Value::Null => {}
        // Numeric stringification, JSON-unquote etc. still allocate
        // a temporary via `value_to_string_bytes`. These shapes do
        // not appear in the LOAD DATA hot path (numeric columns are
        // declared `INTEGER`/`DECIMAL` so they bind through the
        // fixed/decimal encoders, not VarString).
        _ => out.extend_from_slice(&value_to_string_bytes(value)),
    }
}

#[inline]
fn append_bytes_value_bytes_inline(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Bytes(b) => out.extend_from_slice(b),
        Value::String(s) | Value::Decimal(s) => out.extend_from_slice(s.as_bytes()),
        Value::SmallString(b) => out.extend_from_slice(b.as_slice()),
        Value::Null => {}
        _ => out.extend_from_slice(&value_to_bytes(value)),
    }
}

/// In-place variant of [`decode_var_for_kind`] for streaming scans that
/// hand the same output buffer back row after row: when `cell` already
/// holds the matching buffer-backed variant (the previous row's value for
/// this column), the payload is copied into the existing allocation
/// instead of constructing a fresh `Value` — removing the per-row
/// malloc+free pair that dominated the TPC-H Q13 scan profile for wide
/// VARCHAR filter columns.
///
/// A `VarString` cell intentionally stays in the `Value::String` lane
/// once it is there, even for payloads short enough for the inline
/// `SmallString` form: the two lanes are semantically interchangeable
/// (equality / hash / compare / protocol writers), and staying put avoids
/// dropping the buffer when a column's lengths straddle the inline cap.
fn decode_var_for_kind_into(
    kind: &ColumnKind,
    input: &[u8],
    cell: &mut Value,
) -> Result<(), QueryError> {
    match (kind, &mut *cell) {
        (ColumnKind::VarString, Value::String(s)) => {
            // Copy first, validate second — the order matters for speed.
            // `input` borrows the heap page at an arbitrary offset, and
            // `str::from_utf8` only takes its unrolled word-at-a-time
            // ASCII fast lane on aligned input, so validating the page
            // slice in place byte-steps (measured 23x slower — 30% of the
            // whole Q13 profile). Pre-scanning with `<[u8]>::is_ascii` or
            // a hand-rolled or-fold then pushing unchecked also measured
            // ~20% slower end-to-end than this shape: the copy into the
            // (word-aligned) re-used buffer is a SIMD memcpy and the
            // aligned validate matches the legacy
            // `String::from_utf8(input.to_vec())` cost exactly — minus
            // its malloc+free pair.
            //
            // SAFETY: the bytes are validated below before the String can
            // be observed; on failure the buffer is cleared so the UTF-8
            // invariant holds.
            let buf = unsafe { s.as_mut_vec() };
            buf.clear();
            buf.extend_from_slice(input);
            if std::str::from_utf8(buf).is_err() {
                buf.clear();
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid utf8 in column payload",
                ));
            }
            Ok(())
        }
        (ColumnKind::VarString, Value::SmallString(s))
            if input.len() <= crate::types::SMALL_STRING_INLINE_CAP =>
        {
            // The output cell already has the inline representation. Keep it
            // in place instead of returning and copying a complete Value.
            // Validate before replacing the cell so malformed input preserves
            // the same error-state contract as the generic decoder.
            *s = decode_inline_string(input)?;
            Ok(())
        }
        (ColumnKind::VarBytes, Value::Bytes(v))
        | (ColumnKind::Json, Value::Json(v))
        | (ColumnKind::Geometry, Value::Geometry(v)) => {
            v.clear();
            v.extend_from_slice(input);
            Ok(())
        }
        _ => {
            *cell = decode_var_for_kind(kind, input)?;
            Ok(())
        }
    }
}

/// One checked inline-string constructor for both decoding entry points.
#[inline]
fn decode_inline_string(input: &[u8]) -> Result<crate::types::SmallStringBuf, QueryError> {
    if std::str::from_utf8(input).is_err() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid utf8 in column payload",
        ));
    }
    Ok(crate::types::SmallStringBuf::from_slice(input))
}

fn decode_var_for_kind(kind: &ColumnKind, input: &[u8]) -> Result<Value, QueryError> {
    match kind {
        ColumnKind::Decimal { scale } => {
            let mut pos = 0usize;
            decode_decimal(input, &mut pos, *scale)
        }
        ColumnKind::DecimalWide { scale } => decode_decimal_wide(input, *scale),
        ColumnKind::VarString => {
            // Short payloads stay inline (no `to_vec`); long payloads
            // promote to `Value::String`. Both lanes hash, compare and
            // serialise identically — equivalence enforced by
            // `Value::PartialEq` and the SmallString arms across the
            // hot paths (`ValueKey::from`, `write_value_inline_key`,
            // `compare_values`, protocol writers).
            if input.len() <= crate::types::SMALL_STRING_INLINE_CAP {
                decode_inline_string(input).map(Value::SmallString)
            } else {
                Ok(Value::String(String::from_utf8(input.to_vec()).map_err(
                    |_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "invalid utf8 in column payload",
                        )
                    },
                )?))
            }
        }
        ColumnKind::VarBytes => Ok(Value::Bytes(input.to_vec())),
        ColumnKind::Json => Ok(Value::Json(input.to_vec())),
        ColumnKind::Geometry => Ok(Value::Geometry(input.to_vec())),
        ColumnKind::Vector { dim, datatype } => Ok(Value::Vector(vector_from_storage(
            input.to_vec(),
            *datatype,
            *dim,
        )?)),
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "fixed column in var decoder",
        )),
    }
}

fn temporal_quantum(fsp: u8) -> i64 {
    match fsp {
        0 => 1_000_000,
        1 => 100_000,
        2 => 10_000,
        3 => 1_000,
        4 => 100,
        5 => 10,
        _ => 1,
    }
}

const EARLIEST_DATETIME_MICROS: i64 = -62_167_219_200_000_000;

fn encode_tagged_datetime_for_width(
    (year, month, day, hour, minute, second, micros): (u32, u32, u32, u32, u32, u32, u32),
    fsp: u8,
    width: usize,
) -> Result<i64, QueryError> {
    let fraction_base = 10i64.pow(fsp as u32);
    let fraction = micros as i64 / temporal_quantum(fsp);
    let payload = ((((((year as i64 * 13 + month as i64) * 32 + day as i64) * 24 + hour as i64)
        * 60
        + minute as i64)
        * 60
        + second as i64)
        * fraction_base)
        + fraction;
    let stored = signed_min_for_width(width)
        .checked_add(1)
        .and_then(|base| base.checked_add(payload))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "tagged datetime value out of range",
            )
        })?;
    let earliest_real = EARLIEST_DATETIME_MICROS / temporal_quantum(fsp);
    if stored >= earliest_real {
        return Err(error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            "tagged datetime value out of range",
        ));
    }
    Ok(stored)
}

fn decode_tagged_datetime_for_width(
    stored: i64,
    fsp: u8,
    width: usize,
) -> Option<(u32, u32, u32, u32, u32, u32, u32)> {
    let min = signed_min_for_width(width);
    let earliest_real = EARLIEST_DATETIME_MICROS / temporal_quantum(fsp);
    if stored <= min || stored >= earliest_real {
        return None;
    }
    let mut payload = stored.checked_sub(min)?.checked_sub(1)?;
    let fraction_base = 10i64.pow(fsp as u32);
    let fraction = (payload % fraction_base) as u32;
    payload /= fraction_base;
    let second = (payload % 60) as u32;
    payload /= 60;
    let minute = (payload % 60) as u32;
    payload /= 60;
    let hour = (payload % 24) as u32;
    payload /= 24;
    let day = (payload % 32) as u32;
    payload /= 32;
    let month = (payload % 13) as u32;
    let year = (payload / 13) as u32;
    (year <= 9999).then_some((
        year,
        month,
        day,
        hour,
        minute,
        second,
        fraction * temporal_quantum(fsp) as u32,
    ))
}

fn signed_min_for_width(width: usize) -> i64 {
    if width >= 8 {
        i64::MIN
    } else {
        -(1i64 << (width * 8 - 1))
    }
}

fn write_signed_le_width(value: i64, out: &mut [u8]) -> Result<(), QueryError> {
    let width = out.len();
    if width == 0 || width > 8 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid fixed-width integer",
        ));
    }
    let min = signed_min_for_width(width);
    let max = if width >= 8 {
        i64::MAX
    } else {
        (1i64 << (width * 8 - 1)) - 1
    };
    if value < min || value > max {
        return Err(error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            "fixed-width temporal value out of range",
        ));
    }
    let bytes = value.to_le_bytes();
    out.copy_from_slice(&bytes[..width]);
    Ok(())
}

fn read_signed_le_width(input: &[u8]) -> Result<i64, QueryError> {
    let width = input.len();
    if width == 0 || width > 8 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid fixed-width integer",
        ));
    }
    let mut buf = [0u8; 8];
    buf[..width].copy_from_slice(input);
    if input[width - 1] & 0x80 != 0 {
        for b in &mut buf[width..] {
            *b = 0xff;
        }
    }
    Ok(i64::from_le_bytes(buf))
}

fn write_unsigned_le_width(value: u64, out: &mut [u8]) -> Result<(), QueryError> {
    let width = out.len();
    if width == 0 || width > 8 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid fixed-width integer",
        ));
    }
    let max = if width >= 8 {
        u64::MAX
    } else {
        (1u64 << (width * 8)) - 1
    };
    if value > max {
        return Err(error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            "fixed-width value out of range",
        ));
    }
    out.copy_from_slice(&value.to_le_bytes()[..width]);
    Ok(())
}

fn read_unsigned_le_width(input: &[u8]) -> Result<u64, QueryError> {
    let width = input.len();
    if width == 0 || width > 8 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid fixed-width integer",
        ));
    }
    let mut buf = [0u8; 8];
    buf[..width].copy_from_slice(input);
    Ok(u64::from_le_bytes(buf))
}

/// Storage width (bytes) for a SET column with `n_members` members.
/// Matches MySQL 8.0: `ceil(N/8)` capped at 8. Single source of truth
/// so row_codec, constraints, schema/CBO, and binlog metadata never
/// disagree.
pub(crate) fn set_storage_width(n_members: usize) -> u8 {
    (((n_members.max(1) + 7) / 8).min(8)) as u8
}

/// Storage width (bytes) for an ENUM column with `n_members` members.
/// 1 byte for up to 255 members, otherwise 2 bytes.
pub(crate) fn enum_storage_width(n_members: usize) -> u8 {
    if n_members <= u8::MAX as usize {
        1
    } else {
        2
    }
}

fn enum_ordinal(value: &Value, values: &[String]) -> Result<u16, QueryError> {
    // Numeric input must fit in the declared member range. Constraints
    // has already validated at the SQL layer for regular INSERT paths;
    // this guard catches direct-encoder bypasses (undo, tests, ALTER
    // copy shortcut) so a stray out-of-range ordinal never lands on
    // disk masquerading as a valid member.
    match value {
        Value::Int(v) if *v >= 0 => {
            let ord = *v as u64;
            if ord > values.len() as u64 {
                return Err(error(
                    crate::error::catalog::WARN_DATA_TRUNCATED,
                    "data truncated for ENUM column",
                ));
            }
            return Ok(ord as u16);
        }
        Value::UInt(v) => {
            if *v > values.len() as u64 {
                return Err(error(
                    crate::error::catalog::WARN_DATA_TRUNCATED,
                    "data truncated for ENUM column",
                ));
            }
            return Ok(*v as u16);
        }
        Value::Int(_) => {
            return Err(error(
                crate::error::catalog::WARN_DATA_TRUNCATED,
                "data truncated for ENUM column",
            ))
        }
        _ => {}
    }
    let declared_empty_member = matches!(value, Value::SmallString(bytes) if bytes.is_empty());
    let s = match value {
        Value::String(s) | Value::Decimal(s) => s.as_str(),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) },
        Value::Bytes(b) => std::str::from_utf8(b).unwrap_or(""),
        _ => {
            return Err(error(
                crate::error::catalog::WARN_DATA_TRUNCATED,
                "data truncated for ENUM column",
            ))
        }
    };
    if s.is_empty() {
        return if declared_empty_member {
            values
                .iter()
                .position(String::is_empty)
                .map(|index| (index + 1) as u16)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::WARN_DATA_TRUNCATED,
                        "data truncated for ENUM column",
                    )
                })
        } else {
            Ok(0)
        };
    }
    values
        .iter()
        .position(|v| v == s)
        .map(|idx| (idx + 1) as u16)
        .or_else(|| {
            s.trim()
                .parse::<u16>()
                .ok()
                .filter(|ord| *ord > 0 && usize::from(*ord) <= values.len())
        })
        .ok_or_else(|| {
            error(
                crate::error::catalog::WARN_DATA_TRUNCATED,
                "data truncated for ENUM column",
            )
        })
}

fn set_mask(value: &Value, values: &[String]) -> Result<u64, QueryError> {
    match value {
        Value::Int(v) if *v >= 0 => return Ok(*v as u64),
        Value::UInt(v) => return Ok(*v),
        Value::Int(_) => {
            return Err(error(
                crate::error::catalog::WARN_DATA_TRUNCATED,
                "data truncated for SET column",
            ))
        }
        _ => {}
    }
    let s = match value {
        Value::String(s) | Value::Decimal(s) => s.as_str(),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) },
        Value::Bytes(b) => std::str::from_utf8(b).unwrap_or(""),
        _ => {
            return Err(error(
                crate::error::catalog::WARN_DATA_TRUNCATED,
                "data truncated for SET column",
            ))
        }
    };
    if s.is_empty() {
        return Ok(0);
    }
    let mut mask = 0u64;
    for token in s.split(',') {
        let trimmed = token.trim_end_matches(' ');
        if trimmed.is_empty() {
            continue;
        }
        match values.iter().position(|v| v == trimmed) {
            Some(idx) if idx < 64 => mask |= 1u64 << idx,
            Some(_) => {
                // Member exists but bit ≥ 64 — SET literally cannot
                // represent it; fail loudly rather than silently drop.
                return Err(error(
                    crate::error::catalog::WARN_DATA_TRUNCATED,
                    "data truncated for SET column",
                ));
            }
            None => {
                return Err(error(
                    crate::error::catalog::WARN_DATA_TRUNCATED,
                    "data truncated for SET column",
                ))
            }
        }
    }
    Ok(mask)
}

// --- value coercion helpers ----------------------------------------------

fn value_to_f32(v: &Value) -> f32 {
    match v {
        Value::Float(x) => *x,
        Value::Double(x) => *x as f32,
        Value::Int(x) => *x as f32,
        Value::UInt(x) => *x as f32,
        Value::String(s) | Value::Decimal(s) => s.trim().parse().unwrap_or(0.0),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }
            .trim()
            .parse()
            .unwrap_or(0.0),
        Value::Bytes(b) => String::from_utf8_lossy(b).trim().parse().unwrap_or(0.0),
        Value::DecimalCompact(mag, scale) => *mag as f32 / 10f32.powi(*scale as i32),
        Value::Decimal128(mag, scale) => *mag as f32 / 10f32.powi(*scale as i32),
        Value::DecimalWide(value) => value_to_wide_f64(value) as f32,
        Value::Date(d) => *d as f32,
        Value::Time(t) => *t as f32,
        Value::DateTime(dt) => *dt as f32,
        Value::Timestamp(ts) => *ts as f32,
        Value::Year(y) => {
            if *y == 0 {
                0.0
            } else {
                1900.0 + *y as f32
            }
        }
        Value::Json(b) => crate::engine::json::coerce_f64(b) as f32,
        Value::Geometry(_) => 0.0,
        Value::Vector(_) => 0.0,
        Value::Null => 0.0,
    }
}

fn value_to_f64(v: &Value) -> f64 {
    match v {
        Value::Float(x) => *x as f64,
        Value::Double(x) => *x,
        Value::Int(x) => *x as f64,
        Value::UInt(x) => *x as f64,
        Value::String(s) | Value::Decimal(s) => s.trim().parse().unwrap_or(0.0),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }
            .trim()
            .parse()
            .unwrap_or(0.0),
        Value::Bytes(b) => String::from_utf8_lossy(b).trim().parse().unwrap_or(0.0),
        Value::DecimalCompact(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        Value::Decimal128(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        Value::DecimalWide(value) => value_to_wide_f64(value),
        Value::Date(d) => *d as f64,
        Value::Time(t) => *t as f64,
        Value::DateTime(dt) => *dt as f64,
        Value::Timestamp(ts) => *ts as f64,
        Value::Year(y) => {
            if *y == 0 {
                0.0
            } else {
                1900.0 + *y as f64
            }
        }
        Value::Json(b) => crate::engine::json::coerce_f64(b),
        Value::Geometry(_) => 0.0,
        Value::Vector(_) => 0.0,
        Value::Null => 0.0,
    }
}

#[inline]
fn copy_into_fixed_slice(out: &mut [u8], src: &[u8]) -> usize {
    let copy = out.len().min(src.len());
    out[..copy].copy_from_slice(&src[..copy]);
    copy
}

#[allow(dead_code)] // retained for `value_len_for_kind` tests
fn value_to_string_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::String(s) | Value::Decimal(s) => s.as_bytes().to_vec(),
        Value::SmallString(b) => b.as_slice().to_vec(),
        Value::Bytes(b) => b.clone(),
        Value::Int(x) => x.to_string().into_bytes(),
        Value::UInt(x) => x.to_string().into_bytes(),
        Value::Float(x) => x.to_string().into_bytes(),
        Value::Double(x) => x.to_string().into_bytes(),
        Value::Null => Vec::new(),
        Value::Json(b) => crate::engine::json::unquote_text(b).into_bytes(),
        Value::Geometry(b) => crate::engine::geo::to_text(b).into_bytes(),
        Value::Vector(b) => crate::engine::vector::to_text(b).into_bytes(),
        Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(..)
        | Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => v.to_display_string().into_bytes(),
    }
}

#[allow(dead_code)]
fn value_to_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::Bytes(b) => b.clone(),
        Value::String(s) | Value::Decimal(s) => s.as_bytes().to_vec(),
        Value::SmallString(b) => b.as_slice().to_vec(),
        Value::Int(x) => x.to_le_bytes().to_vec(),
        Value::UInt(x) => x.to_le_bytes().to_vec(),
        Value::Float(x) => x.to_le_bytes().to_vec(),
        Value::Double(x) => x.to_le_bytes().to_vec(),
        Value::Null => Vec::new(),
        // For a binary column we keep the JSON's textual form too (CAST to
        // CHAR semantics); raw jsonb is reserved for `ColumnKind::Json`.
        Value::Json(b) => crate::engine::json::unquote_text(b).into_bytes(),
        // Geometry flowing into a binary column keeps its internal bytes.
        Value::Geometry(b) => b.clone(),
        // Vector flowing into a binary column keeps its internal bytes.
        Value::Vector(b) => b.payload.clone(),
        Value::DecimalCompact(..)
        | Value::Decimal128(..)
        | Value::DecimalWide(..)
        | Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => v.to_display_string().into_bytes(),
    }
}

/// Reduce a coerced BIT value to its unsigned integer magnitude for the
/// big-endian on-disk form. The coercion step (`coerce_value_with_unsigned`'s
/// BIT arm) has already normalised the input to an integer `Value::UInt` /
/// `Value::Int` and range-checked it against M bits; a stray `Value::Bytes`
/// (defensive) is read big-endian.
fn bit_value_to_u64(v: &Value) -> u64 {
    match v {
        Value::UInt(x) => *x,
        Value::Int(x) => *x as u64,
        Value::Bytes(b) => b.iter().fold(0u64, |acc, &byte| (acc << 8) | byte as u64),
        Value::Null => 0,
        // Coercion (`constraints::coerce_value_with_unsigned`) normalizes
        // every BIT write to `UInt`/`Int`/`Bytes`/`Null` before it reaches
        // the encoder, so the arms below are unreachable on the write path.
        // Still convert numeric forms instead of silently storing 0 (the old
        // behaviour) so any future write path that skips coercion surfaces a
        // real value rather than corrupting data; a debug build asserts.
        Value::Float(x) => *x as u64,
        Value::Double(x) => *x as u64,
        Value::DecimalCompact(mag, scale) => (*mag / 10i64.pow(*scale as u32)) as u64,
        Value::Decimal(s) | Value::String(s) => s
            .trim()
            .split_once('.')
            .map_or(s.trim(), |(i, _)| i)
            .parse::<u64>()
            .unwrap_or(0),
        other => {
            debug_assert!(false, "unexpected value in BIT column encode: {other:?}");
            0
        }
    }
}

/// Validate direct storage writes using the same policy as SQL assignment.
fn json_coerce_to_jsonb(value: &Value) -> Result<std::borrow::Cow<'_, [u8]>, QueryError> {
    crate::engine::json::column_value(value, "JSON")
}

/// Return canonical internal geometry bytes (`SRID + WKB`) for a geometry
/// column. Assignment coercion has already required `Value::Geometry`;
/// retaining the same invariant here prevents lower storage paths from
/// reintroducing implicit WKT/WKB conversion.
fn geometry_coerce_to_internal(value: &Value) -> Result<Vec<u8>, QueryError> {
    match value {
        Value::Geometry(b) => crate::engine::geo::normalize_internal(b),
        Value::Null => Ok(Vec::new()),
        _ => Err(error(
            crate::error::catalog::ER_CANT_CREATE_GEOMETRY_OBJECT,
            "Cannot get geometry object from data you send to the GEOMETRY field",
        )),
    }
}

/// Coerce any `Value` into canonical internal vector bytes (little-endian
/// `f32` array) for a `ColumnKind::Vector {..}` column. The common case
/// (`Value::Vector`) is already internal form and is returned *borrowed*
/// after an in-place validation — the encoder then copies it once into the
/// row buffer, avoiding the redundant `to_vec()` clone the old path paid per
/// row. Text/bytes must be parsed and so produce an owned `Vec`. An invalid
/// vector is rejected rather than silently stored.
fn vector_coerce_to_internal_cow(
    value: &Value,
    datatype: crate::vector::VectorDataType,
    dim: Option<u32>,
) -> Result<std::borrow::Cow<'_, [u8]>, QueryError> {
    use std::borrow::Cow;
    if let Value::Vector(v) = value {
        crate::engine::vector::validate_internal(v)?;
        if v.datatype == datatype && dim == Some(v.dimension) {
            return Ok(Cow::Borrowed(&v.payload));
        }
    }
    let value = crate::engine::vector::codec::coerce_value(value.clone(), datatype, dim)?;
    if dim.is_some() {
        return Ok(Cow::Owned(value.payload));
    }
    let mut encoded = Vec::with_capacity(value.len() + 4);
    encoded.extend_from_slice(&value.dimension.to_le_bytes());
    encoded.extend_from_slice(&value.payload);
    Ok(Cow::Owned(encoded))
}
fn vector_coerce_to_internal(
    value: &Value,
    datatype: crate::vector::VectorDataType,
    dim: Option<u32>,
) -> Result<Vec<u8>, QueryError> {
    Ok(vector_coerce_to_internal_cow(value, datatype, dim)?.into_owned())
}
fn vector_from_storage(
    mut payload: Vec<u8>,
    datatype: crate::vector::VectorDataType,
    declared: Option<u32>,
) -> Result<crate::vector::VectorValue, QueryError> {
    let dimension = if let Some(dimension) = declared {
        dimension
    } else {
        if payload.len() < 4 {
            return Err(crate::engine::vector::vector_err(
                "truncated stored vector dimension",
            ));
        }
        let dimension = u32::from_le_bytes(payload[..4].try_into().unwrap());
        payload.copy_within(4.., 0);
        payload.truncate(payload.len() - 4);
        dimension
    };
    let value = crate::vector::VectorValue::new(datatype, dimension, payload);
    crate::engine::vector::validate_internal(&value)?;
    Ok(value)
}

// --- integer encoding ----------------------------------------------------

/// Encode a validated integer directly into the fixed-row column lane.
/// Caller passes a slice of the exact column width; we range-check the
/// value and copy the little-endian bytes directly — no intermediate
/// `Vec<u8>` allocation per column. Profile (TPC-H SF=1 LOAD lineitem,
/// 2026-06-03) showed the per-row `Vec::with_capacity(width)` +
/// `extend_from_slice` + `copy_from_slice` round-trip in
/// `encode_fixed_for_kind` allocating ~8 small vecs per row (4 INT +
/// 4 DECIMAL) — 48M wasted allocations for the 6M-row lineitem.
fn encode_int_into_slice(
    value: &Value,
    width: u8,
    signed: bool,
    out: &mut [u8],
) -> Result<(), QueryError> {
    debug_assert_eq!(out.len(), width as usize);
    // LOAD's canonical parser has already range-checked signed integers.
    // Keep a defensive width check here for every other caller, then avoid
    // promoting the overwhelmingly common Value::Int lane to i128 and
    // dispatching through the generic coercion matrix once more.
    if signed {
        if let Value::Int(raw) = value {
            let bits = width as u32 * 8;
            let in_range =
                width == 8 || (*raw >= -(1_i64 << (bits - 1)) && *raw <= (1_i64 << (bits - 1)) - 1);
            if !in_range {
                return Err(error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "integer value out of range",
                ));
            }
            out.copy_from_slice(&raw.to_le_bytes()[..width as usize]);
            return Ok(());
        }
    } else if let Value::UInt(raw) = value {
        let in_range = width == 8 || *raw < (1_u64 << (width as u32 * 8));
        if !in_range {
            return Err(error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "integer value out of range",
            ));
        }
        out.copy_from_slice(&raw.to_le_bytes()[..width as usize]);
        return Ok(());
    }
    let bytes = encode_int_bytes(value, width, signed)?;
    out.copy_from_slice(&bytes[..width as usize]);
    Ok(())
}

#[inline]
fn encode_int_bytes(value: &Value, width: u8, signed: bool) -> Result<[u8; 16], QueryError> {
    let raw: i128 = match value {
        Value::Int(v) => *v as i128,
        Value::UInt(v) => *v as i128,
        Value::Float(v) => *v as i128,
        Value::Double(v) => *v as i128,
        Value::String(s) | Value::Decimal(s) => s.trim().parse().unwrap_or(0),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }
            .trim()
            .parse()
            .unwrap_or(0),
        Value::Bytes(b) => String::from_utf8_lossy(b).trim().parse().unwrap_or(0),
        // DecimalCompact lands here for SELECT-then-UPDATE on an
        // integer column; drop the fractional part.
        Value::DecimalCompact(mag, scale) => (*mag as i128) / 10i128.pow(*scale as u32),
        Value::Decimal128(mag, scale) => crate::util::decimal::rescale_trunc_i128(*mag, *scale, 0)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal value out of integer range",
                )
            })?,
        Value::DecimalWide(value) => {
            use num_traits::ToPrimitive;
            crate::util::decimal::rescale_exact(
                crate::util::decimal::ExactDecimal::Big(value.magnitude.clone(), value.scale),
                0,
                false,
            )
            .into_big_parts()
            .0
            .to_i128()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal value out of integer range",
                )
            })?
        }
        Value::Date(d) => *d as i128,
        Value::Time(t) => *t as i128,
        Value::DateTime(dt) => *dt as i128,
        Value::Timestamp(ts) => *ts as i128,
        Value::Year(y) => {
            if *y == 0 {
                0
            } else {
                1900 + *y as i128
            }
        }
        Value::Json(b) => crate::engine::json::coerce_i64(b) as i128,
        Value::Geometry(_) => 0,
        Value::Vector(_) => 0,
        Value::Null => 0,
    };
    let (lo, hi) = int_range(width, signed);
    if raw < lo || raw > hi {
        return Err(error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            format!(
                "value {} out of range for {}-byte {} integer",
                raw,
                width,
                if signed { "signed" } else { "unsigned" }
            ),
        ));
    }
    Ok(raw.to_le_bytes())
}

fn decode_int(width: u8, signed: bool, input: &[u8], pos: &mut usize) -> Result<Value, QueryError> {
    let bytes = read_exact(input, pos, width as usize)?;
    let w = width as usize;
    if signed {
        let mut buf = [0u8; 16];
        buf[..w].copy_from_slice(bytes);
        // Sign-extend by checking the top bit of the most significant byte.
        let sign_bit = bytes[w - 1] & 0x80 != 0;
        if sign_bit {
            for b in &mut buf[w..] {
                *b = 0xff;
            }
        }
        let v = i128::from_le_bytes(buf);
        Ok(Value::Int(v as i64))
    } else {
        let mut buf = [0u8; 16];
        buf[..w].copy_from_slice(bytes);
        let v = u128::from_le_bytes(buf);
        Ok(Value::UInt(v as u64))
    }
}

fn int_range(width: u8, signed: bool) -> (i128, i128) {
    let bits = (width as u32) * 8;
    if signed {
        let max = (1i128 << (bits - 1)) - 1;
        let min = -(1i128 << (bits - 1));
        (min, max)
    } else {
        let max = if bits >= 128 {
            i128::MAX
        } else {
            (1i128 << bits) - 1
        };
        (0, max)
    }
}

// --- decimal encoding ----------------------------------------------------

fn decimal_magnitude_nbytes(magnitude: u128) -> usize {
    if magnitude == 0 {
        return 0;
    }
    let bits = 128 - magnitude.leading_zeros() as usize;
    bits.div_ceil(8)
}

fn parse_decimal_to_magnitude_from_value(
    value: &Value,
    scale: u8,
) -> Result<(bool, u128), QueryError> {
    // Hot path: Value::Decimal / Value::String hand the codec a &str
    // directly, no clone. Coercion branches (Int / Float / Bytes) are
    // off the TPC-C path and allow themselves the small allocation.
    // Value::DecimalCompact comes straight from a recent decode and
    // already has the magnitude — skip the parser entirely.
    match value {
        Value::DecimalCompact(stored_mag, stored_scale) => {
            decimal_compact_to_codec_magnitude(*stored_mag, *stored_scale, scale)
        }
        Value::Decimal128(stored_mag, stored_scale) => {
            use num_traits::{Signed, ToPrimitive};
            let magnitude = crate::util::decimal::rescale_exact(
                crate::util::decimal::ExactDecimal::I128(*stored_mag, *stored_scale),
                scale,
                true,
            )
            .into_big_parts()
            .0;
            let negative = magnitude.is_negative();
            magnitude
                .abs()
                .to_u128()
                .map(|magnitude| (negative, magnitude))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                        "decimal value out of fixed range",
                    )
                })
        }
        Value::DecimalWide(value) => {
            use num_traits::{Signed, ToPrimitive};
            let magnitude = crate::util::decimal::rescale_exact(
                crate::util::decimal::ExactDecimal::Big(value.magnitude.clone(), value.scale),
                scale,
                true,
            )
            .into_big_parts()
            .0;
            let negative = magnitude.is_negative();
            magnitude
                .abs()
                .to_u128()
                .map(|magnitude| (negative, magnitude))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                        "decimal value out of fixed range",
                    )
                })
        }
        Value::Decimal(s) | Value::String(s) => parse_decimal_string_to_magnitude(s, scale),
        Value::SmallString(b) => {
            parse_decimal_string_to_magnitude(unsafe { std::str::from_utf8_unchecked(b) }, scale)
        }
        Value::Bytes(b) => {
            parse_decimal_string_to_magnitude(std::str::from_utf8(b).unwrap_or(""), scale)
        }
        Value::Int(v) => decimal_compact_to_codec_magnitude(*v, 0, scale),
        Value::UInt(v) => {
            // u64 → i64 cast may overflow for the top bit. For TPC-C
            // workloads UInt never exceeds i64::MAX in DECIMAL columns
            // but be defensive: parse via string for the unlikely
            // overflow path.
            if *v <= i64::MAX as u64 {
                decimal_compact_to_codec_magnitude(*v as i64, 0, scale)
            } else {
                parse_decimal_string_to_magnitude(&v.to_string(), scale)
            }
        }
        Value::Float(v) => parse_decimal_string_to_magnitude(&v.to_string(), scale),
        Value::Double(v) => parse_decimal_string_to_magnitude(&v.to_string(), scale),
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::Timestamp(_)
        | Value::Year(_) => {
            // These shouldn't really land in a DECIMAL column; if they
            // do, normalise via the display form.
            parse_decimal_string_to_magnitude(&value.to_display_string(), scale)
        }
        Value::Json(b) => {
            parse_decimal_string_to_magnitude(&crate::engine::json::unquote_text(b), scale)
        }
        Value::Geometry(_) => Ok((false, 0)),
        Value::Vector(_) => Ok((false, 0)),
        Value::Null => Ok((false, 0)),
    }
}

/// Convert a `Value::DecimalCompact(magnitude, stored_scale)` value
/// into the (negative, |magnitude_at_target_scale|) tuple the codec
/// expects. Shifts the magnitude to match the column's declared scale.
fn decimal_compact_to_codec_magnitude(
    mag: i64,
    stored_scale: u8,
    target_scale: u8,
) -> Result<(bool, u128), QueryError> {
    let negative = mag < 0;
    let mut abs = (mag.unsigned_abs()) as u128;
    if stored_scale < target_scale {
        let shift = 10u128
            .checked_pow((target_scale - stored_scale) as u32)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal scale overflow",
                )
            })?;
        abs = abs.checked_mul(shift).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "decimal magnitude overflow",
            )
        })?;
    } else if stored_scale > target_scale {
        // Round half away from zero when narrowing to the column scale —
        // the sign rides separately, so rounding the magnitude covers
        // both signs. Matches MySQL's INSERT-time decimal rounding.
        // Use `checked_pow` to avoid a debug panic when a corrupted SDI
        // hands us a scale delta > 38 (the raw `pow` would overflow
        // u128 and panic in debug, wrap in release).
        let shift = 10u128
            .checked_pow((stored_scale - target_scale) as u32)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal scale overflow",
                )
            })?;
        let remainder = abs % shift;
        abs /= shift;
        if remainder * 2 >= shift {
            abs = abs.checked_add(1).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal magnitude overflow",
                )
            })?;
        }
    }
    Ok((negative && abs != 0, abs))
}

/// Single-pass state-machine parser for "[+-]?[0-9]*\.?[0-9]*" with
/// truncate-to-scale + pad-to-scale semantics. Folds the integer and
/// (scale-clamped) fractional digits straight into a u128 magnitude
/// without allocating an intermediate String. Replaces the original
/// implementation, which used `String::collect()` plus two
/// `str::parse::<u128>()` calls and was a measurable TPC-C bottleneck
/// for tables like HISTORY / ORDER_LINE that touch DECIMAL columns
/// on every row.
fn parse_decimal_string_to_magnitude(s: &str, scale: u8) -> Result<(bool, u128), QueryError> {
    let trimmed = s.trim();
    if trimmed.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
        if let Some((magnitude, source_scale)) =
            crate::util::decimal::parse_exact_decimal_str(trimmed)
        {
            let rescaled = crate::util::decimal::rescale_round_i128(magnitude, source_scale, scale)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                        "decimal magnitude overflow",
                    )
                })?;
            return Ok((rescaled < 0, rescaled.unsigned_abs()));
        }
        // The general canonicalizer handles exact scientific forms wider
        // than i128 when they still fit MySQL DECIMAL(65). Feed its plain
        // decimal output back through the allocation-free parser.
        if let Some(canonical) = crate::util::decimal::canonical_decimal_str(trimmed) {
            return parse_decimal_string_to_magnitude(&canonical, scale);
        }
        // Preserve the distinction between malformed text (1366) and a
        // syntactically valid exponent that exceeds DECIMAL's range (1264).
        let valid_shape = {
            let unsigned = trimmed
                .strip_prefix('+')
                .or_else(|| trimmed.strip_prefix('-'))
                .unwrap_or(trimmed);
            unsigned
                .find(|byte: char| byte == 'e' || byte == 'E')
                .is_some_and(|index| {
                    let (mantissa, exponent) = unsigned.split_at(index);
                    let exponent = &exponent[1..];
                    let exponent = exponent
                        .strip_prefix('+')
                        .or_else(|| exponent.strip_prefix('-'))
                        .unwrap_or(exponent);
                    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
                    (!integer.is_empty() || !fraction.is_empty())
                        && integer.bytes().all(|byte| byte.is_ascii_digit())
                        && fraction.bytes().all(|byte| byte.is_ascii_digit())
                        && !exponent.is_empty()
                        && exponent.bytes().all(|byte| byte.is_ascii_digit())
                })
        };
        return Err(if valid_shape {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "decimal magnitude overflow",
            )
        } else {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                format!("incorrect decimal value: '{s}'"),
            )
        });
    }
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let n = bytes.len();
    while i < n && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= n {
        return Ok((false, 0));
    }
    let mut negative = false;
    match bytes[i] {
        b'-' => {
            negative = true;
            i += 1;
        }
        b'+' => i += 1,
        _ => {}
    }
    let mut magnitude: u128 = 0;
    let mut any_digit = false;
    while i < n && bytes[i].is_ascii_digit() {
        any_digit = true;
        let d = (bytes[i] - b'0') as u128;
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|m| m.checked_add(d))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal integer part overflow",
                )
            })?;
        i += 1;
    }
    let mut frac_digits = 0u32;
    let scale_u32 = scale as u32;
    let mut round_up = false;
    if i < n && bytes[i] == b'.' {
        i += 1;
        let mut first_dropped: Option<u8> = None;
        while i < n && bytes[i].is_ascii_digit() {
            if frac_digits < scale_u32 {
                let d = (bytes[i] - b'0') as u128;
                magnitude = magnitude
                    .checked_mul(10)
                    .and_then(|m| m.checked_add(d))
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                            "decimal fractional overflow",
                        )
                    })?;
                frac_digits += 1;
                any_digit = true;
            } else if first_dropped.is_none() {
                first_dropped = Some(bytes[i] - b'0');
            }
            i += 1;
        }
        // Beyond the declared scale MySQL *rounds* (half away from zero —
        // the sign is carried separately, so rounding the magnitude up
        // covers both signs): INSERT 1.005 into DECIMAL(4,2) stores 1.01.
        round_up = matches!(first_dropped, Some(d) if d >= 5);
    }
    while i < n && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if !any_digit || i != n {
        return Err(error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            format!("incorrect decimal value: '{}'", s),
        ));
    }
    // Logical right-pad: if input had fewer fractional digits than the
    // declared scale, shift left to canonicalise the magnitude.
    while frac_digits < scale_u32 {
        magnitude = magnitude.checked_mul(10).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "decimal magnitude overflow",
            )
        })?;
        frac_digits += 1;
    }
    if round_up {
        magnitude = magnitude.checked_add(1).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "decimal magnitude overflow",
            )
        })?;
    }
    if magnitude == 0 {
        negative = false;
    }
    Ok((negative, magnitude))
}

fn encode_decimal(value: &Value, scale: u8, out: &mut Vec<u8>) -> Result<(), QueryError> {
    let (negative, magnitude) = parse_decimal_to_magnitude_from_value(value, scale)?;
    let nbytes = decimal_magnitude_nbytes(magnitude);
    out.push(if negative { 1 } else { 0 });
    out.push(nbytes as u8);
    let bytes = magnitude.to_le_bytes();
    out.extend_from_slice(&bytes[..nbytes]);
    Ok(())
}

fn encode_decimal_wide(value: &Value, scale: u8, out: &mut Vec<u8>) -> Result<(), QueryError> {
    let text = match value {
        Value::Decimal(text) | Value::String(text) => std::borrow::Cow::Borrowed(text.as_str()),
        Value::SmallString(bytes) => {
            std::borrow::Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(bytes) })
        }
        Value::Bytes(bytes) => std::borrow::Cow::Owned(String::from_utf8_lossy(bytes).into_owned()),
        _ => std::borrow::Cow::Owned(value.to_display_string()),
    };
    let fixed = crate::util::decimal::rescale_decimal_big_str(&text, scale).ok_or_else(|| {
        error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            "incorrect decimal value",
        )
    })?;
    crate::util::decimal::encode_decimal_bcd(&fixed, out).ok_or_else(|| {
        error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            "decimal value exceeds DECIMAL(65)",
        )
    })?;
    Ok(())
}

fn decode_decimal_wide(input: &[u8], scale: u8) -> Result<Value, QueryError> {
    let (text, used) = crate::util::decimal::decode_decimal_bcd(input).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt wide decimal payload",
        )
    })?;
    if used != input.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt wide decimal payload length",
        ));
    }
    let fixed = crate::util::decimal::rescale_decimal_big_str(&text, scale).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt wide decimal scale",
        )
    })?;
    let (magnitude, scale) = crate::util::decimal::parse_exact_decimal_big(&fixed)
        .ok_or_else(|| error(crate::error::catalog::ER_GET_ERRNO, "corrupt wide decimal"))?;
    // The physical column lane is selected from declared precision, not the
    // current row's coefficient size. Keeping every P39..65 row wide avoids
    // mixed index-key/vector variants and makes plan behaviour stable.
    Ok(Value::DecimalWide(Box::new(crate::types::DecimalWide {
        magnitude,
        scale,
    })))
}

/// Encode a fixed-width signed decimal coefficient at the declared scale
/// directly into its 4-, 8- or 16-byte column lane.
fn encode_decimal_fixed_into_slice(
    value: &Value,
    width: u8,
    scale: u8,
    out: &mut [u8],
) -> Result<(), QueryError> {
    debug_assert_eq!(out.len(), width as usize);
    // Canonical DECIMAL(P,S) LOAD values already carry the declared scale.
    // In that lane the stored fixed body is exactly the signed coefficient;
    // avoid sign splitting, u128 scale arithmetic, and reconstruction.
    if let Value::DecimalCompact(magnitude, stored_scale) = value {
        if *stored_scale == scale {
            match width {
                4 => {
                    let value = i32::try_from(*magnitude).map_err(|_| {
                        error(
                            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                            "decimal value out of range for DECIMAL(P≤9) column",
                        )
                    })?;
                    out.copy_from_slice(&value.to_le_bytes());
                    return Ok(());
                }
                8 => {
                    out.copy_from_slice(&magnitude.to_le_bytes());
                    return Ok(());
                }
                16 => {
                    out.copy_from_slice(&(*magnitude as i128).to_le_bytes());
                    return Ok(());
                }
                _ => {}
            }
        }
    }
    let (negative, magnitude) = parse_decimal_to_magnitude_from_value(value, scale)?;
    let signed_mag: i128 = if negative {
        -(magnitude as i128)
    } else {
        magnitude as i128
    };
    match width {
        4 => {
            let v = i32::try_from(signed_mag).map_err(|_| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal value out of range for DECIMAL(P≤9) column",
                )
            })?;
            out.copy_from_slice(&v.to_le_bytes());
        }
        8 => {
            let v = i64::try_from(signed_mag).map_err(|_| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "decimal value out of range for DECIMAL(P≤18) column",
                )
            })?;
            out.copy_from_slice(&v.to_le_bytes());
        }
        16 => {
            out.copy_from_slice(&signed_mag.to_le_bytes());
        }
        _ => {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("internal error: invalid fixed-decimal width {width}"),
            ))
        }
    }
    Ok(())
}

fn decode_decimal_fixed(
    input: &[u8],
    pos: &mut usize,
    width: u8,
    scale: u8,
) -> Result<Value, QueryError> {
    match width {
        4 => {
            let bytes = read_exact(input, pos, 4)?;
            let v = i32::from_le_bytes(bytes.try_into().unwrap());
            Ok(Value::DecimalCompact(v as i64, scale))
        }
        8 => {
            let bytes = read_exact(input, pos, 8)?;
            let v = i64::from_le_bytes(bytes.try_into().unwrap());
            Ok(Value::DecimalCompact(v, scale))
        }
        16 => {
            let bytes = read_exact(input, pos, 16)?;
            let v = i128::from_le_bytes(bytes.try_into().unwrap());
            if let Ok(small) = i64::try_from(v) {
                Ok(Value::DecimalCompact(small, scale))
            } else {
                Ok(Value::Decimal128(v, scale))
            }
        }
        _ => Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("internal error: invalid fixed-decimal width {width}"),
        )),
    }
}

fn decode_decimal(input: &[u8], pos: &mut usize, scale: u8) -> Result<Value, QueryError> {
    let sign_byte = read_exact(input, pos, 1)?[0];
    let nbytes = read_exact(input, pos, 1)?[0] as usize;
    if nbytes > 16 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt decimal nbytes",
        ));
    }
    let mag_bytes = read_exact(input, pos, nbytes)?;
    let mut buf = [0u8; 16];
    buf[..nbytes].copy_from_slice(mag_bytes);
    let magnitude = u128::from_le_bytes(buf);
    let negative = sign_byte != 0 && magnitude != 0;
    // Fast path: magnitudes that fit in i64 — every TPC-C / TPC-H
    // DECIMAL column does — land as `Value::DecimalCompact` so the
    // executor / re-encode cycle can avoid the ASCII format step
    // entirely. Larger magnitudes (DECIMAL(P>18)) fall back to the
    // legacy String form.
    if magnitude <= i64::MAX as u128 {
        let mag_i64 = magnitude as i64;
        let signed = if negative { -mag_i64 } else { mag_i64 };
        Ok(Value::DecimalCompact(signed, scale))
    } else {
        Ok(Value::Decimal(format_decimal_v3(
            magnitude, scale, negative,
        )))
    }
}

pub(crate) fn format_decimal_v3(magnitude: u128, scale: u8, negative: bool) -> String {
    crate::util::decimal::format_decimal_magnitude(magnitude, scale, negative)
}

// --- date / datetime / time / year ---------------------------------------

const ZERO_DATE_SENTINEL: i32 = i32::MIN;
const ZERO_DATETIME_SENTINEL: i64 = i64::MIN;

// Date / DateTime fast paths
// --------------------------------------------------------------------------
//
// `chrono::NaiveDate::parse_from_str` and `NaiveDate::format` are correct
// but they run a `strftime`-style state machine on every call and allocate
// a fresh `String` for every formatted output. With Phase B's
// schema-driven codec every TPC-C INSERT/SELECT hits these for DATETIME
// columns like H_DATE / O_ENTRY_D / OL_DELIVERY_D, which produced a ~4×
// throughput regression vs Phase A's tagged-string format. The hand-
// written helpers below parse the canonical ASCII forms in a single
// pass and emit the canonical forms into pre-sized `String`s. chrono is
// only consulted via a fall-back for inputs that don't match the
// canonical shape (rare on TPC-C data); correctness is preserved.

fn parse_2_digit(b: &[u8]) -> Option<u32> {
    if b.len() < 2 {
        return None;
    }
    let h = b[0];
    let l = b[1];
    if !(b'0'..=b'9').contains(&h) || !(b'0'..=b'9').contains(&l) {
        return None;
    }
    Some(((h - b'0') as u32) * 10 + (l - b'0') as u32)
}

fn parse_4_digit(b: &[u8]) -> Option<i32> {
    if b.len() < 4 {
        return None;
    }
    let mut r: i32 = 0;
    for &c in &b[..4] {
        if !(b'0'..=b'9').contains(&c) {
            return None;
        }
        r = r * 10 + (c - b'0') as i32;
    }
    Some(r)
}

/// Howard Hinnant's `days_from_civil`: proleptic Gregorian Y/M/D →
/// days since 1970-01-01. Returns `None` for out-of-range Y/M/D.
/// Days in `m` of year `y` (proleptic Gregorian). `m` must be 1..=12.
#[inline]
pub(crate) fn days_in_month(y: i32, m: u32) -> u32 {
    crate::util::temporal::days_in_month(y, m).unwrap_or_else(|| {
        if crate::util::temporal::is_leap_year(y) {
            29
        } else {
            28
        }
    })
}

fn days_from_civil(y: i32, m: u32, d: u32) -> Option<i32> {
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // MySQL rejects years outside 0..=9999 (well outside the SQL
    // standard's 0001..9999 range too). Old code silently clamped on
    // display, which made writes of `'10000-01-01'` round-trip to
    // `'9999-01-01'` — a data-loss bug. Refuse the parse instead;
    // constraints.rs surfaces this via the sql_mode-aware
    // `invalid_temporal_policy` (strict → 1292, non-strict → warning
    // + zero-date).
    if !(0..=9999).contains(&y) {
        return None;
    }
    // Reject impossible calendar dates ('2009-02-31') instead of letting
    // the era arithmetic silently shift them into the next month.
    if d > days_in_month(y, m) {
        return None;
    }
    let m = m as i32;
    let d = d as i32;
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32; // [0, 399]
    let m_adj = if m > 2 { m - 3 } else { m + 9 } as u32;
    let doy = (153 * m_adj + 2) / 5 + d as u32 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    Some(era * 146097 + doe as i32 - 719468)
}

/// Parse "YYYY-MM-DD" (ASCII, exactly 10 chars at the start) into
/// (year, month, day). Returns `None` for any deviation; the caller
/// falls back to chrono.
#[inline]
fn parse_ymd_fast(bytes: &[u8]) -> Option<(i32, u32, u32)> {
    if bytes.len() < 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year = parse_4_digit(&bytes[0..4])?;
    let month = parse_2_digit(&bytes[5..7])?;
    let day = parse_2_digit(&bytes[8..10])?;
    Some((year, month, day))
}

/// Byte-slice variant of `parse_date_to_days`. Skips the str-side
/// utf8 validation + `trim`/`char_indices` work for the common case
/// where the input is already canonical-shaped ASCII (LOAD DATA's
/// fast path always feeds us a slice of the underlying CSV buffer).
/// Falls back to the str-based slow path on any non-canonical shape
/// so chrono still handles the exotic cases the existing tests rely
/// on. ~7% of LOAD DATA worker CPU on sf=1 lineitem was attributable
/// to the str-side trim alone (profile-p2-fix3); bypassing it here
/// turned out to be cheap and broadly applicable to any callers that
/// already have bytes in hand.
pub(crate) fn parse_date_to_days_bytes(bytes: &[u8]) -> Result<i32, QueryError> {
    let bytes = trim_ascii_ws_bytes(bytes);
    if bytes.is_empty() || bytes == b"0000-00-00" {
        return Ok(ZERO_DATE_SENTINEL);
    }
    if bytes.len() >= 10 && (bytes.len() == 10 || bytes[10] == b' ' || bytes[10] == b'T') {
        if let Some((y, m, d)) = parse_ymd_fast(bytes) {
            if y == 0 && m == 0 && d == 0 {
                return Ok(ZERO_DATE_SENTINEL);
            }
            if let Some(days) = days_from_civil(y, m, d) {
                return Ok(days);
            }
        }
    }
    let s = std::str::from_utf8(bytes).map_err(|_| {
        error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            format!("incorrect date value: '{}'", String::from_utf8_lossy(bytes)),
        )
    })?;
    parse_date_to_days(s)
}

#[inline]
fn trim_ascii_ws_bytes(mut bytes: &[u8]) -> &[u8] {
    while let Some(&b) = bytes.first() {
        if b.is_ascii_whitespace() {
            bytes = &bytes[1..];
        } else {
            break;
        }
    }
    while let Some(&b) = bytes.last() {
        if b.is_ascii_whitespace() {
            bytes = &bytes[..bytes.len() - 1];
        } else {
            break;
        }
    }
    bytes
}

pub(crate) fn parse_date_to_days(s: &str) -> Result<i32, QueryError> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed == "0000-00-00" {
        return Ok(ZERO_DATE_SENTINEL);
    }
    let bytes = trimmed.as_bytes();
    // Fast path: canonical "YYYY-MM-DD" with optional trailing
    // whitespace (datetime loaders may pass through "YYYY-MM-DD HH:MM:SS"
    // for a DATE column; consume only the date portion).
    if bytes.len() >= 10 && (bytes.len() == 10 || bytes[10] == b' ' || bytes[10] == b'T') {
        if let Some((y, m, d)) = parse_ymd_fast(bytes) {
            if y == 0 && m == 0 && d == 0 {
                return Ok(ZERO_DATE_SENTINEL);
            }
            if let Some(days) = days_from_civil(y, m, d) {
                return Ok(days);
            }
        }
    }
    // Slow path: chrono for anything non-canonical (e.g. abbreviated
    // years or unusual separators). Stays correct for the edge cases
    // covered by the existing tests.
    let date_part = trimmed.split_whitespace().next().unwrap_or(trimmed);
    let date = chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
        .ok()
        .or_else(|| {
            // MySQL compact numeric dates on the write path: `20200101`,
            // `991231`, `20200101123045` (date column keeps the date part).
            crate::util::temporal::parse_compact_datetime(trimmed)
                .and_then(|p| chrono::NaiveDate::from_ymd_opt(p.year as i32, p.month, p.day))
        })
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
                format!("incorrect date value: '{}'", s),
            )
        })?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = date.signed_duration_since(epoch).num_days();
    if days < i32::MIN as i64 + 1 || days > i32::MAX as i64 {
        return Err(error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            "date out of range",
        ));
    }
    Ok(days as i32)
}

pub(crate) fn parse_datetime_to_micros(s: &str) -> Result<i64, QueryError> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed == "0000-00-00" || trimmed == "0000-00-00 00:00:00" {
        return Ok(ZERO_DATETIME_SENTINEL);
    }
    let bytes = trimmed.as_bytes();
    // Canonical fast path: "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS"
    // optionally followed by ".ffffff". 19+ bytes.
    if bytes.len() >= 19
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && (bytes[10] == b' ' || bytes[10] == b'T')
        && bytes[13] == b':'
        && bytes[16] == b':'
    {
        if let (Some((y, m, d)), Some(hh), Some(mm), Some(ss)) = (
            parse_ymd_fast(&bytes[..10]),
            parse_2_digit(&bytes[11..13]),
            parse_2_digit(&bytes[14..16]),
            parse_2_digit(&bytes[17..19]),
        ) {
            // Optional ".ffffff"
            let mut frac_micros: i64 = 0;
            let mut tail_ok = true;
            if bytes.len() > 19 {
                if bytes[19] == b'.' {
                    let mut digits = 0u32;
                    for &b in &bytes[20..] {
                        if !b.is_ascii_digit() {
                            tail_ok = false;
                            break;
                        }
                        if digits < 6 {
                            frac_micros = frac_micros * 10 + (b - b'0') as i64;
                            digits += 1;
                        }
                        // Beyond microsecond precision: ignore extra digits.
                    }
                    // Right-pad to 6 digits so 1.5 → 1.500000 µs.
                    while digits < 6 {
                        frac_micros *= 10;
                        digits += 1;
                    }
                } else {
                    tail_ok = false;
                }
            }
            if tail_ok && hh < 24 && mm < 60 && ss < 60 {
                if let Some(days) = days_from_civil(y, m, d) {
                    let secs_of_day = (hh as i64) * 3600 + (mm as i64) * 60 + ss as i64;
                    let total_secs = days as i64 * 86_400 + secs_of_day;
                    return Ok(total_secs * 1_000_000 + frac_micros);
                }
            }
        }
    }
    // Date-only fast path: "YYYY-MM-DD" (10 chars), pad to midnight.
    if bytes.len() == 10 {
        if let Some((y, m, d)) = parse_ymd_fast(bytes) {
            if let Some(days) = days_from_civil(y, m, d) {
                return Ok(days as i64 * 86_400 * 1_000_000);
            }
        }
    }
    // chrono fallback for non-canonical inputs.
    for fmt in &[
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Ok(dt.and_utc().timestamp_micros());
        }
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let dt = date.and_hms_opt(0, 0, 0).unwrap();
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Some(parts) = crate::util::temporal::parse_delimited_datetime_components(trimmed) {
        if let Some(dt) = chrono::NaiveDate::from_ymd_opt(parts.year as i32, parts.month, parts.day)
            .and_then(|date| {
                date.and_hms_micro_opt(parts.hour, parts.minute, parts.second, parts.micros)
            })
        {
            return Ok(dt.and_utc().timestamp_micros());
        }
    }
    // MySQL compact numeric datetime on the write path: `20200101`,
    // `20200101123045`, `20200101123045.5`, `991231`.
    if let Some(p) = crate::util::temporal::parse_compact_datetime(trimmed) {
        if let Some(dt) = chrono::NaiveDate::from_ymd_opt(p.year as i32, p.month, p.day)
            .and_then(|d| d.and_hms_micro_opt(p.hour, p.minute, p.second, p.micros))
        {
            return Ok(dt.and_utc().timestamp_micros());
        }
    }
    Err(error(
        crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
        format!("incorrect datetime value: '{}'", s),
    ))
}

/// Quantize an already parsed datetime directly from its original fraction.
/// The parser keeps six digits; use the discarded digits here only when they
/// can change assignment rounding. Never round first to six and then to fsp.
pub(crate) fn quantize_datetime_text_micros(micros: i64, text: &str, fsp: u8) -> i64 {
    if micros == crate::types::ZERO_DATETIME
        || crate::types::decode_tagged_datetime(micros).is_some()
    {
        return micros;
    }
    let Some((_, tail)) = text.trim().rsplit_once('.') else {
        return micros;
    };
    if tail.len() <= 6 {
        return micros;
    }
    let fraction = tail.as_bytes();
    let digits = fraction
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digits <= 6 {
        return micros;
    }
    let fsp = usize::from(fsp.min(6));
    let quantum = temporal_quantum(fsp as u8);
    let mut units = 0i64;
    for byte in &fraction[..fsp] {
        units = units * 10 + i64::from(byte - b'0');
    }
    let truncate = crate::engine::expr::session_temporal_sql_mode_bits()
        & crate::engine::api::TEMPORAL_MODE_TIME_TRUNCATE_FRACTIONAL
        != 0;
    if !truncate && fraction[fsp] >= b'5' {
        units += 1;
    }
    micros - micros.rem_euclid(1_000_000) + units * quantum
}

/// Quantize TIME text directly at the target FSP, retaining excess source
/// digits until rounding so seven-digit fractions cannot be double-rounded.
pub(crate) fn quantize_time_text_micros(micros: i64, text: &str, fsp: u8) -> i64 {
    let negative = micros < 0 || text.trim_start().starts_with('-');
    let magnitude = quantize_datetime_text_micros(micros.saturating_abs(), text, fsp);
    let truncate = crate::engine::expr::session_temporal_sql_mode_bits()
        & crate::engine::api::TEMPORAL_MODE_TIME_TRUNCATE_FRACTIONAL
        != 0;
    let signed = if negative { -magnitude } else { magnitude };
    if truncate {
        truncate_time_micros_to_fsp(signed, fsp)
    } else {
        round_time_micros_to_fsp(signed, fsp)
    }
}

/// Quantize a signed TIME microsecond payload toward zero. TIME is a
/// signed duration; MySQL truncates the magnitude (`-00:00:00.9` →
/// `-00:00:00`, not `-00:00:01`). Companion of `round_time_micros_to_fsp`.
#[inline]
pub(crate) fn truncate_time_micros_to_fsp(micros: i64, fsp: u8) -> i64 {
    if fsp >= 6 {
        return micros;
    }
    let unit: i64 = 10i64.pow((6 - fsp as u32) as u32);
    // Truncate toward zero: divide then multiply, preserving sign.
    (micros / unit) * unit
}

/// Quantize a DATETIME/TIMESTAMP microsecond payload toward −∞ (floor).
/// Wall-clock semantics: the sub-second fraction is always non-negative,
/// so a pre-1970 value `-500_000` (`1969-12-31 23:59:59.5`) truncates to
/// `-1_000_000` (`1969-12-31 23:59:59`), not `0` (which would silently
/// jump to `1970-01-01 00:00:00`). Companion of
/// `round_datetime_micros_to_fsp`.
#[inline]
pub(crate) fn truncate_datetime_micros_to_fsp(micros: i64, fsp: u8) -> i64 {
    if fsp >= 6 {
        return micros;
    }
    let unit: i64 = 10i64.pow((6 - fsp as u32) as u32);
    let rem = micros.rem_euclid(unit);
    micros - rem
}

// Back-compat shim: some historical callers used a single
// `truncate_micros_to_fsp`. It always meant "TIME toward-zero"; keep
// the alias so we don't have to touch every callsite in one go.
#[cfg(test)]
#[inline]
pub(crate) fn truncate_micros_to_fsp(micros: i64, fsp: u8) -> i64 {
    truncate_time_micros_to_fsp(micros, fsp)
}

/// Round an i64-microsecond TIME payload to the declared precision —
/// MySQL's default (5.6.4+) behaviour when `TIME_TRUNCATE_FRACTIONAL`
/// is *not* set. TIME is a signed duration: half-way values round away
/// from zero on the magnitude (`-00:00:00.5` → `-00:00:01`), and the
/// carry may overflow into seconds/minutes/hours.
#[inline]
pub(crate) fn round_time_micros_to_fsp(micros: i64, fsp: u8) -> i64 {
    if fsp >= 6 {
        return micros;
    }
    let unit: i64 = 10i64.pow((6 - fsp as u32) as u32);
    let magnitude = micros.unsigned_abs() as i64;
    let rounded = (magnitude + unit / 2) / unit * unit;
    if micros < 0 {
        -rounded
    } else {
        rounded
    }
}

/// Round an i64-microsecond DATETIME/TIMESTAMP payload (micros since
/// epoch) to the declared precision. Wall-clock semantics: the
/// sub-second fraction is always non-negative, so half-way values round
/// toward +∞ regardless of the (pre-1970) sign, and the carry naturally
/// propagates across second/minute/day boundaries.
#[inline]
pub(crate) fn round_datetime_micros_to_fsp(micros: i64, fsp: u8) -> i64 {
    if fsp >= 6 {
        return micros;
    }
    let unit: i64 = 10i64.pow((6 - fsp as u32) as u32);
    let rem = micros.rem_euclid(unit);
    let base = micros - rem;
    if rem * 2 >= unit {
        base + unit
    } else {
        base
    }
}

pub(crate) fn parse_time_to_micros(s: &str) -> Result<i64, QueryError> {
    if s.trim().is_empty() {
        return Ok(0);
    }
    let parts = crate::util::temporal::parse_time_duration(s).map_err(|failure| {
        let code = match failure {
            crate::util::temporal::TimeParseError::Syntax => {
                crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE
            }
            crate::util::temporal::TimeParseError::MinuteOrSecond => {
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE
            }
        };
        error(code, format!("incorrect time value: '{}'", s.trim()))
    })?;
    let magnitude = (i64::from(parts.hour) * 3_600_000_000
        + i64::from(parts.minute) * 60_000_000
        + i64::from(parts.second) * 1_000_000
        + i64::from(parts.micros))
    .min(TIME_MAX_MICROS);
    Ok(if parts.neg { -magnitude } else { magnitude })
}

/// MySQL's maximum TIME magnitude, `838:59:59.000000`, in microseconds
/// (the documented range endpoint; larger values saturate here).
const TIME_MAX_MICROS: i64 = (838 * 3600 + 59 * 60 + 59) * 1_000_000;

fn parse_year(s: &str) -> Result<u8, QueryError> {
    let s = s.trim();
    if s.is_empty() || s == "0" || s == "0000" {
        return Ok(0);
    }
    let v: i64 = s.parse().map_err(|_| {
        error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            format!("incorrect year value: '{}'", s),
        )
    })?;
    crate::util::temporal::year_byte_from_int(v)
}

// Keep the storage codec's internal exports while sharing the bit transform
// with ORDER BY key encoding. Callers own signed-zero and NaN policy.
pub(crate) use crate::util::ordered_float::{
    f32_from_order_u32, f32_to_order_u32, f64_from_order_u64, f64_to_order_u64,
};

/// Test-only convenience: a codec capable of round-tripping rows that
/// look like `[BigInt, VarString, VarString, ...]` (up to 16 columns).
/// The low-level heap-file tests in `storage/core/tests.rs` exclusively
/// use that shape; full database-level tests build the codec from the
/// catalog via `Database::heap_handle` and never touch this helper.
#[cfg(test)]
fn test_row_codec_from_columns(columns: Vec<ColumnCodec>) -> RowCodec {
    let layouts = vec![identity_layout_for_columns(&columns, 1)];
    let defaults = vec![Value::Null; columns.len()];
    RowCodec {
        columns,
        active_layout_id: 1,
        layouts,
        defaults,
    }
}

#[cfg(test)]
pub(crate) fn test_default_codec() -> std::sync::Arc<RowCodec> {
    let mut cols = vec![ColumnCodec {
        kind: ColumnKind::Int {
            width: 8,
            signed: true,
        },
    }];
    for _ in 0..15 {
        cols.push(ColumnCodec {
            kind: ColumnKind::VarString,
        });
    }
    std::sync::Arc::new(test_row_codec_from_columns(cols))
}

#[cfg(test)]
mod row_codec_tests {
    use super::*;

    fn round_trip(values: Vec<Value>) {
        let mut buf = Vec::new();
        encode_row_into(&values, &mut buf).unwrap();
        assert_eq!(buf.len(), encoded_row_len(&values).unwrap());
        let skip = encoded_row_skip_len(&buf).unwrap();
        assert_eq!(skip, buf.len());
        let decoded = decode_row(&buf).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn empty_row() {
        round_trip(vec![]);
    }

    #[test]
    fn fsp_truncation_units() {
        // fsp=6 is a no-op (every microsecond is significant).
        assert_eq!(truncate_micros_to_fsp(123_456_789, 6), 123_456_789);
        // fsp=3 keeps milliseconds: round 123_456_789 µs down to the
        // nearest multiple of 1_000 → 123_456_000.
        assert_eq!(truncate_micros_to_fsp(123_456_789, 3), 123_456_000);
        // fsp=0 keeps whole seconds: 123_456_789 µs = 123.456789 s → 123 s.
        assert_eq!(truncate_micros_to_fsp(123_456_789, 0), 123_000_000);
        // Strictly below one second + fsp=0 collapses to zero.
        assert_eq!(truncate_micros_to_fsp(999_999, 0), 0);
        // Boundary: exact multiple — unchanged.
        assert_eq!(truncate_micros_to_fsp(2_000_000, 0), 2_000_000);
        // Negative TIME values truncate toward zero (mirrors MySQL).
        assert_eq!(truncate_micros_to_fsp(-123_456_789, 3), -123_456_000);
        assert_eq!(truncate_micros_to_fsp(-999_999, 0), 0);
        // fsp >= 7 falls back to no-op (defensive; clamp_fsp also rejects
        // any user-supplied value > 6 at parse time).
        assert_eq!(truncate_micros_to_fsp(42, 7), 42);
    }

    #[test]
    fn fsp_rounding_units() {
        // DATETIME wall-clock rounding: half-way rounds toward +∞ and
        // the carry crosses second boundaries.
        assert_eq!(round_datetime_micros_to_fsp(123_456_789, 6), 123_456_789);
        assert_eq!(round_datetime_micros_to_fsp(123_456_789, 3), 123_457_000);
        assert_eq!(round_datetime_micros_to_fsp(123_456_789, 0), 123_000_000);
        assert_eq!(round_datetime_micros_to_fsp(123_500_000, 0), 124_000_000);
        assert_eq!(round_datetime_micros_to_fsp(999_999, 0), 1_000_000);
        // Pre-1970 datetimes: '1969-12-31 23:59:59.6' (-400_000 µs) still
        // rounds the wall-clock fraction up → 1970-01-01 00:00:00.
        assert_eq!(round_datetime_micros_to_fsp(-400_000, 0), 0);
        // '1969-12-31 23:59:59.4' rounds down to :59.
        assert_eq!(round_datetime_micros_to_fsp(-600_000, 0), -1_000_000);

        // TIME duration rounding: half-way rounds away from zero on the
        // magnitude; -00:00:00.5 → -00:00:01.
        assert_eq!(round_time_micros_to_fsp(500_000, 0), 1_000_000);
        assert_eq!(round_time_micros_to_fsp(-500_000, 0), -1_000_000);
        assert_eq!(round_time_micros_to_fsp(-499_999, 0), 0);
        assert_eq!(round_time_micros_to_fsp(-123_456_789, 3), -123_457_000);
        // fsp >= 7 falls back to no-op.
        assert_eq!(round_time_micros_to_fsp(42, 7), 42);
        assert_eq!(round_datetime_micros_to_fsp(42, 7), 42);
    }

    #[test]
    fn vector_value_round_trip() {
        // Regression: a `Value::Vector` must encode, *skip* (the undo decoder
        // walks past the before-image via `encoded_row_skip_len`), and decode.
        // A missing TAG_VECTOR arm in `skip_value_by_tag` broke WAL replay of
        // DELETE/UPDATE undo records on vector tables (restart recovery).
        let emb = Value::Vector(vec![0, 0, 128, 63, 0, 0, 0, 64].into()); // [1.0, 2.0] LE f32
        round_trip(vec![emb.clone()]);
        // Mixed with other tagged values, and as a non-leading column so the
        // skip must land `pos` exactly for the following value to decode.
        round_trip(vec![
            Value::Int(7),
            emb,
            Value::String("after".to_string()),
            Value::Null,
        ]);
    }

    fn all_value_kinds() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Int(-1234567890123),
            Value::UInt(987654321),
            Value::Float(3.5),
            Value::Double(-2.71827),
            Value::Decimal("123.4500".to_string()),
            Value::Bytes(b"\x00\xff\x10binary".to_vec()),
            Value::String("hello, 世界".to_string()),
            Value::Null,
            Value::Int(0),
        ]
    }

    #[test]
    fn round_trip_mixed() {
        round_trip(all_value_kinds());
    }

    #[test]
    fn round_trip_all_null() {
        round_trip(vec![Value::Null; 17]);
    }

    #[derive(Debug)]
    struct FakeLobReader;
    impl LobMaterializer for FakeLobReader {
        fn materialize(
            &self,
            _col_pos: usize,
            kind: &ColumnKind,
            lob: LobRef,
        ) -> Result<Value, QueryError> {
            // Reconstruct a deterministic payload from the descriptor so
            // the test can assert the round-trip without a real overflow
            // file: `total_len` bytes of 0xAB.
            let bytes = vec![0xABu8; lob.total_len as usize];
            Ok(match kind {
                ColumnKind::VarString => {
                    Value::String(String::from_utf8_lossy(&bytes).into_owned())
                }
                _ => Value::Bytes(bytes),
            })
        }
    }

    fn ext_codec() -> RowCodec {
        test_row_codec_from_columns(vec![
            ColumnCodec {
                kind: ColumnKind::Int {
                    width: 8,
                    signed: true,
                },
            },
            ColumnCodec {
                kind: ColumnKind::VarString,
            },
            ColumnCodec {
                kind: ColumnKind::VarBytes,
            },
        ])
    }

    #[test]
    fn external_bitmap_round_trip_full() {
        let codec = ext_codec();
        let lob = LobRef {
            first_page_id: 7,
            last_page_id: 9,
            total_len: 5,
            generation: 11,
        };
        let values = vec![Value::Int(42), Value::String("hi".into()), Value::Null];
        let external = vec![None, None, Some(lob)];
        let mut buf = Vec::new();
        encode_row_schema_into_ext(&values, &codec, &external, &mut buf).unwrap();

        // Full decode with a materializer installed reconstructs the LOB.
        let decoded = with_lob_materializer(Some(std::sync::Arc::new(FakeLobReader)), || {
            decode_row_schema_ext(&buf, &codec, true)
        })
        .unwrap();
        assert_eq!(decoded[0], Value::Int(42));
        assert_eq!(decoded[1], Value::String("hi".into()));
        assert_eq!(decoded[2], Value::Bytes(vec![0xAB; 5]));
    }

    #[test]
    fn external_unprojected_needs_no_reader() {
        let codec = ext_codec();
        let lob = LobRef {
            first_page_id: 1,
            last_page_id: 1,
            total_len: 16,
            generation: 12,
        };
        let values = vec![Value::Int(42), Value::String("hi".into()), Value::Null];
        let external = vec![None, None, Some(lob)];
        let mut buf = Vec::new();
        encode_row_schema_into_ext(&values, &codec, &external, &mut buf).unwrap();

        // Projecting only the non-external columns must not touch the
        // overflow store at all (no materializer installed → no error).
        let mut out = Vec::new();
        decode_row_schema_columns_into_ext(&buf, &codec, &[0, 1], &mut out, true).unwrap();
        assert_eq!(out, vec![Value::Int(42), Value::String("hi".into())]);

        // Projecting the external column without a reader is an error.
        let mut out2 = Vec::new();
        let err =
            decode_row_schema_columns_into_ext(&buf, &codec, &[2], &mut out2, true).unwrap_err();
        assert_eq!(err.code(), 1030);
    }

    #[test]
    fn projected_fast_decode_compiles_every_persisted_layout() {
        let mut codec = test_row_codec_from_columns(vec![ColumnCodec {
            kind: ColumnKind::Int {
                width: 8,
                signed: true,
            },
        }]);
        let mut active = codec.layouts[0].clone();
        active.layout_id = 2;
        codec.layouts.push(active);
        codec.active_layout_id = 2;

        let plan = RowDecodePlan::compile_with_codec(&[0], &codec);
        assert!(plan
            .fast_for_layout(1)
            .is_some_and(|fast| fast.covers_full_projection));
        assert!(plan
            .fast_for_layout(2)
            .is_some_and(|fast| fast.covers_full_projection));
    }

    #[test]
    fn sink_fast_decode_accepts_trailing_default_prefix_rows() {
        let codec = test_row_codec_from_columns(vec![
            ColumnCodec {
                kind: ColumnKind::Int {
                    width: 8,
                    signed: true,
                },
            },
            ColumnCodec {
                kind: ColumnKind::Timestamp { fsp: 6 },
            },
            ColumnCodec {
                kind: ColumnKind::Int {
                    width: 4,
                    signed: true,
                },
            },
        ]);
        let plan = RowDecodePlan::compile_with_codec(&[0, 1, 2], &codec);
        let values = vec![Value::Int(7), Value::Timestamp(1_700_000_000_000_000)];
        let mut encoded = Vec::new();
        encode_row_schema_into(&values, &codec, &mut encoded).unwrap();

        let mut decoded = vec![Value::Int(-1); 3];
        let fast = decode_row_into_sink_fast(
            &encoded,
            &codec,
            codec.active_layout_id,
            &plan,
            &mut ValueVecSink(&mut decoded),
            false,
        )
        .unwrap();
        assert!(fast, "prefix rows must retain sink-driven decoding");
        assert_eq!(
            decoded,
            vec![values[0].clone(), values[1].clone(), Value::Null]
        );
    }

    #[test]
    fn projected_decode_reuses_var_buffers_across_rows() {
        // Streaming scans hand the same `out` Vec back row after row; the
        // fast decode path overwrites cells in place so a wide VARCHAR /
        // VARBINARY column re-uses its heap allocation instead of paying a
        // malloc+free pair per row. Push rows of different shapes through
        // one buffer and check correctness plus buffer identity.
        let codec = test_row_codec_from_columns(vec![
            ColumnCodec {
                kind: ColumnKind::Int {
                    width: 8,
                    signed: true,
                },
            },
            ColumnCodec {
                kind: ColumnKind::VarString,
            },
            ColumnCodec {
                kind: ColumnKind::VarBytes,
            },
        ]);
        let plan = RowDecodePlan::compile_with_codec(&[0, 1, 2], &codec);
        assert!(plan
            .fast
            .as_ref()
            .is_some_and(|fast| fast.covers_full_projection));

        let long_a = "a".repeat(64);
        let long_b = "b".repeat(40);
        let rows: Vec<Vec<Value>> = vec![
            vec![
                Value::Int(1),
                Value::String(long_a.clone()),
                Value::Bytes(vec![1; 50]),
            ],
            vec![
                Value::Int(2),
                Value::String(long_b.clone()),
                Value::Bytes(vec![2; 8]),
            ],
            // Short string: a fresh decode would produce the SmallString
            // inline form; the re-used cell stays in the String lane,
            // which PartialEq treats as the same value.
            vec![
                Value::Int(3),
                Value::String("tiny".to_string()),
                Value::Bytes(Vec::new()),
            ],
            vec![Value::Int(4), Value::Null, Value::Null],
            vec![
                Value::Int(5),
                Value::String(long_a.clone()),
                Value::Bytes(vec![5; 31]),
            ],
        ];
        let encoded: Vec<Vec<u8>> = rows
            .iter()
            .map(|row| {
                let mut buf = Vec::new();
                encode_row_schema_into(row, &codec, &mut buf).unwrap();
                buf
            })
            .collect();

        let mut out: Vec<Value> = Vec::new();
        decode_row_schema_columns_into_with_plan(&encoded[0], &codec, &plan, &mut out).unwrap();
        assert_eq!(out, rows[0]);
        let row0_str_ptr = match &out[1] {
            Value::String(s) => s.as_ptr(),
            other => panic!("expected String cell, got {other:?}"),
        };

        // Row 2's string (40 B) fits row 1's capacity (64 B): the cell
        // must keep the same allocation (clear + push_str, no realloc).
        decode_row_schema_columns_into_with_plan(&encoded[1], &codec, &plan, &mut out).unwrap();
        assert_eq!(out, rows[1]);
        match &out[1] {
            Value::String(s) => assert_eq!(s.as_ptr(), row0_str_ptr),
            other => panic!("expected String cell, got {other:?}"),
        }

        // Short payload on a re-used cell stays in the String lane.
        decode_row_schema_columns_into_with_plan(&encoded[2], &codec, &plan, &mut out).unwrap();
        assert_eq!(out, rows[2]);
        assert!(matches!(&out[1], Value::String(_)));

        // NULL overwrites the cell (buffer is given up).
        decode_row_schema_columns_into_with_plan(&encoded[3], &codec, &plan, &mut out).unwrap();
        assert_eq!(out, rows[3]);

        decode_row_schema_columns_into_with_plan(&encoded[4], &codec, &plan, &mut out).unwrap();
        assert_eq!(out, rows[4]);
    }

    #[test]
    fn assign_value_reusing_matches_clone() {
        use crate::types::assign_value_reusing;
        let long = Value::String("x".repeat(50));
        let small = Value::SmallString(crate::types::SmallStringBuf::from_slice(b"abc"));
        let cases: Vec<(Value, Value)> = vec![
            (Value::Null, long.clone()),
            (long.clone(), Value::String("y".repeat(10))),
            // String destination absorbs a SmallString source in place.
            (long.clone(), small),
            (long.clone(), Value::Null),
            (Value::Bytes(vec![1, 2, 3]), Value::Bytes(vec![9; 40])),
            (Value::Int(1), Value::Int(2)),
            (long.clone(), Value::Int(7)),
            (Value::Json(vec![1]), Value::Json(vec![2, 3])),
        ];
        for (mut dst, src) in cases {
            assign_value_reusing(&mut dst, &src);
            assert_eq!(dst, src);
        }
        // Buffer re-use: a String destination with enough capacity keeps
        // its allocation.
        let mut dst = Value::String(String::with_capacity(64));
        let ptr = match &dst {
            Value::String(s) => s.as_ptr(),
            _ => unreachable!(),
        };
        assign_value_reusing(&mut dst, &Value::String("hello".into()));
        assert_eq!(dst, Value::String("hello".into()));
        match &dst {
            Value::String(s) => assert_eq!(s.as_ptr(), ptr),
            _ => unreachable!(),
        }
    }

    #[test]
    fn no_external_is_byte_identical() {
        // A row with no externalized columns must encode identically
        // whether or not the ext-aware path is used, and carry no ext
        // bitmap (decoded with has_external = false).
        let codec = ext_codec();
        let values = vec![
            Value::Int(1),
            Value::String("a".into()),
            Value::Bytes(vec![1, 2, 3]),
        ];
        let mut plain = Vec::new();
        encode_row_schema_into(&values, &codec, &mut plain).unwrap();
        let mut via_ext = Vec::new();
        encode_row_schema_into_ext(&values, &codec, &[], &mut via_ext).unwrap();
        assert_eq!(plain, via_ext);
        let decoded = decode_row_schema_ext(&plain, &codec, false).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn round_trip_binary_variants() {
        // Every binary variant should round-trip losslessly through the
        // self-describing format introduced in undo v3 — DecimalCompact
        // keeps its (magnitude, scale) pair, and Date/Time/DateTime/Year
        // keep their native i32/i64/u8 form rather than detouring through
        // ASCII display strings.
        round_trip(vec![
            Value::DecimalCompact(-12_345, 2),
            Value::DecimalCompact(0, 0),
            Value::DecimalCompact(i64::MAX, 18),
            Value::Date(0),
            Value::Date(i32::MIN),       // 0000-00-00 sentinel
            Value::Time(-3_600_000_000), // negative TIME allowed
            Value::Time(0),
            Value::DateTime(1_700_000_000_000_000),
            Value::DateTime(i64::MIN), // zero datetime sentinel
            Value::Year(0),
            Value::Year(255),
        ]);
    }

    #[test]
    fn tinyint_undo_byte_count() {
        // Self-describing undo encoding spends 1 byte tag + 8 byte i64
        // for every Value::Int regardless of the original column width.
        // (Width-aware encoding only happens in the schema-aware heap
        // codec, which is intentional: undo has no codec to consult.)
        // This test pins the size so anyone who changes the tag set
        // notices the impact on undo footprint.
        let mut buf = Vec::new();
        encode_row_into(&[Value::Int(7)], &mut buf).unwrap();
        // varint(1) = 1B, NULL bitmap = 1B, tag = 1B, i64 = 8B.
        assert_eq!(buf.len(), 1 + 1 + 1 + 8);
    }

    #[test]
    fn datetime_undo_is_binary() {
        // DATETIME used to be ASCII-serialised (~24B per value); the v3
        // format stores it as a binary i64.
        let mut buf = Vec::new();
        encode_row_into(&[Value::DateTime(1_700_000_000_000_000)], &mut buf).unwrap();
        // 1B varint count + 1B bitmap + 1B tag + 8B i64.
        assert_eq!(buf.len(), 11);
    }

    #[test]
    fn float_order_preserving_round_trip() {
        for v in [
            -f32::INFINITY,
            -1.5_f32,
            -0.5_f32,
            -0.0_f32,
            0.0_f32,
            0.5_f32,
            1.5_f32,
            f32::INFINITY,
        ] {
            let enc = f32_to_order_u32(v);
            let dec = f32_from_order_u32(enc);
            assert_eq!(dec.to_bits(), v.to_bits());
        }
        // Order preservation: encode(-1.5) < encode(-0.5) < encode(0.5) < encode(1.5).
        let neg_big = f32_to_order_u32(-1.5);
        let neg_small = f32_to_order_u32(-0.5);
        let pos_small = f32_to_order_u32(0.5);
        let pos_big = f32_to_order_u32(1.5);
        assert!(neg_big < neg_small);
        assert!(neg_small < pos_small);
        assert!(pos_small < pos_big);
    }

    #[test]
    fn double_order_preserving() {
        let neg_big = f64_to_order_u64(-1.5);
        let neg_small = f64_to_order_u64(-0.5);
        let pos_small = f64_to_order_u64(0.5);
        let pos_big = f64_to_order_u64(1.5);
        assert!(neg_big < neg_small);
        assert!(neg_small < pos_small);
        assert!(pos_small < pos_big);
        assert_eq!(f64_from_order_u64(neg_big), -1.5);
        assert_eq!(f64_from_order_u64(pos_big), 1.5);
    }

    #[test]
    fn varint_encoding() {
        for v in [
            0u64,
            1,
            127,
            128,
            255,
            16_383,
            16_384,
            u32::MAX as u64,
            u64::MAX,
        ] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            assert_eq!(buf.len(), varint_len(v));
            let mut pos = 0;
            assert_eq!(read_varint(&buf, &mut pos).unwrap(), v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn varint_rejects_truncated_and_overflow_encodings() {
        for input in [&[][..], &[0x80][..], &[0xff; 10][..]] {
            let mut pos = 0;
            assert!(read_varint(input, &mut pos).is_err());
        }
    }

    fn bigint_codec(n: usize) -> RowCodec {
        test_row_codec_from_columns(
            (0..n)
                .map(|_| ColumnCodec {
                    kind: ColumnKind::Int {
                        width: 8,
                        signed: true,
                    },
                })
                .collect(),
        )
    }

    #[test]
    fn mvcc_header_trims_zero_fields() {
        let header = RowHeader {
            flags: 0,
            last_txn_id: 0,
            undo_ptr: None,
        };
        let codec = bigint_codec(0);
        assert_eq!(mvcc_header_len(&header), 2);
        let payload = encode_mvcc_row(header, &[], &codec, &[]).unwrap();
        assert_eq!(
            payload.len(),
            2 + varint_len(codec.active_layout_id as u64)
                + encoded_row_schema_len(&[], &codec).unwrap()
        );
        let (decoded, _) = decode_mvcc_row(&payload, &codec).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn mvcc_header_carries_optional_fields() {
        let header = RowHeader {
            flags: 0,
            last_txn_id: 42,
            undo_ptr: Some(0xCAFEBABE),
        };
        let codec = bigint_codec(1);
        assert_eq!(mvcc_header_len(&header), 18);
        let payload = encode_mvcc_row(header, &[Value::Int(7)], &codec, &[]).unwrap();
        let (decoded, values) = decode_mvcc_row(&payload, &codec).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(values, vec![Value::Int(7)]);
    }

    #[test]
    fn mvcc_flag_row_piece_is_public_only() {
        let header = RowHeader {
            flags: MVCC_FLAG_ROW_PIECE,
            last_txn_id: 1,
            undo_ptr: None,
        };
        let codec = bigint_codec(0);
        let payload = encode_mvcc_row(header, &[], &codec, &[]).unwrap();
        // Stored flags byte must include ROW_PIECE plus HAS_LAST_TXN_ID;
        // it must not include any of the other bits.
        let stored_flags = payload[1];
        assert_eq!(stored_flags, MVCC_FLAG_ROW_PIECE | FLAG_HAS_LAST_TXN_ID,);
        let (decoded, _) = decode_mvcc_row(&payload, &codec).unwrap();
        // Caller-visible flags only carry the public bits.
        assert_eq!(decoded.flags & !PUBLIC_FLAG_MASK, 0);
        assert!(decoded.flags & MVCC_FLAG_ROW_PIECE != 0);
    }

    #[test]
    fn projection_skips_nulls_via_bitmap() {
        let values = vec![
            Value::String("a".to_string()),
            Value::Null,
            Value::Int(99),
            Value::Null,
            Value::Bytes(b"xyz".to_vec()),
        ];
        let mut buf = Vec::new();
        encode_row_into(&values, &mut buf).unwrap();
        let mut out = Vec::new();
        let stored = decode_row_columns_into(&buf, &[4, 2, 1], &mut out).unwrap();
        assert_eq!(stored, 5);
        assert_eq!(
            out,
            vec![Value::Bytes(b"xyz".to_vec()), Value::Int(99), Value::Null]
        );
    }

    #[test]
    fn forwarding_stub_round_trip() {
        let rid = RowId {
            page_id: 99,
            slot_id: 12,
        };
        let bytes = encode_heap_forwarding_stub(rid);
        assert_eq!(bytes.len(), HEAP_FORWARDING_STUB_LEN);
        assert_eq!(decode_heap_forwarding_stub(&bytes).unwrap(), Some(rid));
    }

    #[test]
    fn savings_vs_v1_lineitem_like_row() {
        // 16 columns reminiscent of TPC-H lineitem: 5 BIGINT + 4 DECIMAL +
        // 3 DATE + 1 CHAR(1) + 2 CHAR(N) + 1 VARCHAR. Compare new encoded
        // size against a synthetic v1 lower bound to guard against
        // accidental regressions in the codec.
        let values = vec![
            Value::Int(1),
            Value::Int(155190),
            Value::Int(7706),
            Value::Int(1),
            Value::Int(17),
            Value::Decimal("21168.23".to_string()),
            Value::Decimal("0.04".to_string()),
            Value::Decimal("0.02".to_string()),
            Value::Decimal("0.03".to_string()),
            Value::String("1996-01-02".to_string()),
            Value::String("1996-01-31".to_string()),
            Value::String("1996-02-12".to_string()),
            Value::String("N".to_string()),
            Value::String("DELIVER IN PERSON       ".to_string()),
            Value::String("TRUCK     ".to_string()),
            Value::String("egular courts above the".to_string()),
        ];
        let mut buf = Vec::new();
        encode_row_into(&values, &mut buf).unwrap();
        // v1 would have spent: 2B count + per-col (1B tag + payload), with
        // each variable value prefixed by 4B u32 length. That row weighs
        // ~206B under v1; v2 should land well under 200B.
        let v1_estimate = 2 + values
            .iter()
            .map(|v| match v {
                Value::Null => 1,
                Value::Int(_) | Value::UInt(_) | Value::Double(_) => 1 + 8,
                Value::Float(_) => 1 + 4,
                Value::Decimal(s) => 1 + 4 + s.len(),
                Value::Bytes(b) => 1 + 4 + b.len(),
                Value::String(s) => 1 + 4 + s.len(),
                _ => 1 + 4 + v.to_display_string().len(),
            })
            .sum::<usize>();
        assert!(
            buf.len() < v1_estimate,
            "v2 row {} should be smaller than v1 estimate {}",
            buf.len(),
            v1_estimate,
        );
    }

    // ------- Phase B (schema-driven) per-type round-trip tests ------------

    fn codec(kinds: Vec<ColumnKind>) -> RowCodec {
        test_row_codec_from_columns(kinds.into_iter().map(|kind| ColumnCodec { kind }).collect())
    }

    #[test]
    fn release_format_golden_schema_aware_mvcc_rows_and_retained_layout() {
        let mut kinds = vec![
            ColumnKind::Int {
                width: 8,
                signed: true,
            },
            ColumnKind::Int {
                width: 8,
                signed: false,
            },
            ColumnKind::Float32,
            ColumnKind::Float64,
            ColumnKind::DecimalFixed {
                width: 16,
                scale: 8,
            },
            ColumnKind::DecimalWide { scale: 30 },
            ColumnKind::VarString,
            ColumnKind::VarBytes,
            ColumnKind::Date,
            ColumnKind::Time { fsp: 6 },
            ColumnKind::DateTime { fsp: 6 },
            ColumnKind::Timestamp { fsp: 6 },
            ColumnKind::Year,
            ColumnKind::Enum {
                values: vec!["a".into(), "中".into()],
                width: 1,
            },
            ColumnKind::Set {
                values: vec!["a".into(), "中".into()],
                width: 1,
            },
            ColumnKind::Bit { width: 8 },
            ColumnKind::VarString,
        ];
        let mut values = vec![
            Value::Int(i64::MIN),
            Value::UInt(u64::MAX),
            Value::Float(-0.0),
            Value::Double(f64::from_bits(0x3ff0000000000001)),
            Value::Decimal128(1234567890123456789, 8),
            Value::Decimal("123456789012345678901234567890.123456789012345678901234567890".into()),
            Value::String("界\0".into()),
            Value::Bytes(vec![0, 255]),
            Value::Date(i32::MIN),
            Value::Time(-123456789),
            Value::DateTime(i64::MIN),
            Value::Timestamp(4_102_444_799_123_456),
            Value::Year(126),
            Value::String("中".into()),
            Value::String("a,中".into()),
            Value::UInt(u64::MAX),
            Value::Null,
        ];
        let old_codec = codec(kinds.clone());
        let header = RowHeader {
            flags: 0,
            last_txn_id: 17,
            undo_ptr: Some(33),
        };
        let old = encode_mvcc_row(header, &values, &old_codec, &[]).unwrap();
        crate::storage::format_golden::golden!("heap-mvcc-row", &old);
        let (_, decoded) = decode_mvcc_row(&old, &old_codec).unwrap();
        let expected = values
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>();
        assert_eq!(
            decoded
                .iter()
                .map(Value::to_display_string)
                .collect::<Vec<_>>(),
            expected
        );
        kinds.push(ColumnKind::Int {
            width: 4,
            signed: true,
        });
        let mut next = codec(kinds);
        next.active_layout_id = 2;
        next.layouts = vec![
            old_codec.layouts[0].clone(),
            identity_layout_for_columns(&next.columns, 2),
        ];
        let (_, decoded) = decode_mvcc_row(&old, &next).unwrap();
        assert_eq!(
            decoded[..expected.len()]
                .iter()
                .map(Value::to_display_string)
                .collect::<Vec<_>>(),
            expected
        );
        values.push(Value::Int(7));
        let newer = encode_mvcc_row(header, &values, &next, &[]).unwrap();
        crate::storage::format_golden::golden!("heap-mvcc-next-layout", &newer);
        let (_, decoded) = decode_mvcc_row(&newer, &next).unwrap();
        assert_eq!(
            decoded
                .iter()
                .map(Value::to_display_string)
                .collect::<Vec<_>>(),
            values
                .iter()
                .map(Value::to_display_string)
                .collect::<Vec<_>>()
        );
    }

    fn schema_round_trip(values: Vec<Value>, kinds: Vec<ColumnKind>) -> usize {
        let codec = codec(kinds);
        let mut buf = Vec::new();
        encode_row_schema_into(&values, &codec, &mut buf).unwrap();
        assert_eq!(buf.len(), encoded_row_schema_len(&values, &codec).unwrap());
        let decoded = decode_row_schema(&buf, &codec).unwrap();
        // After the binary-Value optimisation, codec decode produces
        // the binary temporal / decimal forms even when the input was
        // an ASCII Value::String / Value::Decimal. They're the same
        // value at the user level — compare via the canonical display
        // string so the round-trip property is checked on the
        // observable form.
        let lhs: Vec<String> = decoded.iter().map(|v| v.to_display_string()).collect();
        let rhs: Vec<String> = values.iter().map(|v| v.to_display_string()).collect();
        assert_eq!(lhs, rhs, "round-trip canonical-form mismatch");
        buf.len()
    }

    #[test]
    fn direct_row_payload_builder_matches_value_codec_byte_for_byte() {
        let c = codec(vec![
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
            ColumnKind::DecimalFixed { width: 8, scale: 2 },
            ColumnKind::Date,
            ColumnKind::VarString,
            ColumnKind::VarString,
        ]);
        let cases = vec![
            vec![
                Value::Int(-17),
                Value::DecimalCompact(12_345, 2),
                Value::Date(10_592),
                Value::SmallString(crate::types::SmallStringBuf::from_slice(b"N")),
                Value::String("plain ascii".into()),
            ],
            vec![
                Value::Int(i32::MAX as i64),
                Value::DecimalCompact(-1, 2),
                Value::Date(0),
                Value::SmallString(crate::types::SmallStringBuf::from_slice(b"AB  ")),
                Value::Null,
            ],
        ];
        let program = CompiledDirectRowEncoder::compile(&c).unwrap();
        for values in cases {
            let expected = encode_visible_row_payload(&values, &c, 42).unwrap();
            let mut direct = program.begin_row(42, 64);
            for value in &values {
                direct.push_value(value).unwrap();
            }
            assert_eq!(direct.finish().unwrap(), expected);
        }
    }

    #[test]
    fn typed_direct_row_payload_matches_value_codec_byte_for_byte() {
        let c = codec(vec![
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
            ColumnKind::Int {
                width: 8,
                signed: false,
            },
            ColumnKind::DecimalFixed { width: 4, scale: 2 },
            ColumnKind::DecimalFixed { width: 8, scale: 2 },
            ColumnKind::DecimalFixed {
                width: 16,
                scale: 2,
            },
            ColumnKind::Date,
            ColumnKind::VarString,
            ColumnKind::VarString,
        ]);
        let program = CompiledDirectRowEncoder::compile(&c).unwrap();
        for (signed, unsigned, decimal, days, left, right) in [
            (-17, 42, -12_345, 10_592, &b"N"[..], &b"plain ascii"[..]),
            (
                i32::MAX as i64,
                u64::MAX,
                99_999_999,
                ZERO_DATE_SENTINEL,
                &b"AB"[..],
                &b""[..],
            ),
        ] {
            let values = vec![
                Value::Int(signed),
                Value::UInt(unsigned),
                Value::DecimalCompact(decimal, 2),
                Value::DecimalCompact(decimal, 2),
                Value::DecimalCompact(decimal, 2),
                Value::Date(days),
                Value::String(String::from_utf8(left.to_vec()).unwrap()),
                Value::String(String::from_utf8(right.to_vec()).unwrap()),
            ];
            let expected = encode_visible_row_payload(&values, &c, 99).unwrap();
            let mut direct = program.begin_row(99, left.len() + right.len());
            direct.push_i64(signed).unwrap();
            direct.push_u64(unsigned).unwrap();
            direct.push_decimal_compact(decimal, 2).unwrap();
            direct.push_decimal_compact(decimal, 2).unwrap();
            direct.push_decimal_compact(decimal, 2).unwrap();
            direct.push_date_days(days).unwrap();
            direct.push_string_bytes(left).unwrap();
            direct.push_string_bytes(right).unwrap();
            assert_eq!(direct.finish().unwrap(), expected);
        }
    }

    #[test]
    fn alter_copy_transcode_matches_full_encode() {
        let source_codec = codec(vec![
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
            ColumnKind::VarString,
            ColumnKind::VarString,
        ]);
        let target_codec = codec(vec![
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
            ColumnKind::VarString,
            ColumnKind::VarString,
            ColumnKind::VarString,
        ]);
        let source_values = vec![
            Value::Int(7),
            Value::Int(12345),
            Value::String("unchanged-wide-column".to_string()),
            Value::Null,
        ];
        let target_values = vec![
            Value::Int(7),
            Value::String("12345".to_string()),
            Value::String("unchanged-wide-column".to_string()),
            Value::Null,
        ];
        let source = encode_visible_row_payload(&source_values, &source_codec, 11).unwrap();
        let transcoded = transcode_visible_row_payload(
            &source,
            &source_codec,
            &target_codec,
            &target_values,
            &[false, true, false, false],
            22,
        )
        .unwrap();
        let expected = encode_visible_row_payload(&target_values, &target_codec, 22).unwrap();
        assert_eq!(transcoded, expected);
    }

    #[test]
    fn int_widths_round_trip() {
        // Signed: 1B fits -128..=127; 2B fits -32768..=32767; etc.
        schema_round_trip(
            vec![Value::Int(-1), Value::Int(127), Value::Int(-32768)],
            vec![
                ColumnKind::Int {
                    width: 1,
                    signed: true,
                },
                ColumnKind::Int {
                    width: 1,
                    signed: true,
                },
                ColumnKind::Int {
                    width: 2,
                    signed: true,
                },
            ],
        );
        schema_round_trip(
            vec![Value::Int(8_388_607), Value::Int(-8_388_608)],
            vec![
                ColumnKind::Int {
                    width: 3,
                    signed: true,
                },
                ColumnKind::Int {
                    width: 3,
                    signed: true,
                },
            ],
        );
        schema_round_trip(
            vec![Value::Int(i64::MIN), Value::Int(i64::MAX)],
            vec![
                ColumnKind::Int {
                    width: 8,
                    signed: true,
                },
                ColumnKind::Int {
                    width: 8,
                    signed: true,
                },
            ],
        );
    }

    #[test]
    fn unsigned_int_round_trip() {
        // Unsigned decodes back to Value::UInt.
        let kinds = vec![
            ColumnKind::Int {
                width: 1,
                signed: false,
            },
            ColumnKind::Int {
                width: 4,
                signed: false,
            },
            ColumnKind::Int {
                width: 8,
                signed: false,
            },
        ];
        let values = vec![
            Value::UInt(255),
            Value::UInt(4_294_967_295),
            Value::UInt(u64::MAX),
        ];
        schema_round_trip(values, kinds);
    }

    #[test]
    fn int_overflow_rejected() {
        let codec = codec(vec![ColumnKind::Int {
            width: 1,
            signed: true,
        }]);
        let mut buf = Vec::new();
        let err = encode_row_schema_into(&[Value::Int(200)], &codec, &mut buf).unwrap_err();
        assert_eq!(err.code(), 1264);
    }

    #[test]
    fn date_round_trip() {
        // Common path: ASCII date round-trips through 4B days-since-epoch.
        schema_round_trip(
            vec![Value::String("1996-01-02".to_string())],
            vec![ColumnKind::Date],
        );
        // MySQL zero date sentinel.
        schema_round_trip(
            vec![Value::String("0000-00-00".to_string())],
            vec![ColumnKind::Date],
        );
        // Pre-epoch dates: range check.
        schema_round_trip(
            vec![Value::String("1900-03-15".to_string())],
            vec![ColumnKind::Date],
        );
        // Encoded width is fixed at 3 bytes regardless of date.
        let codec = codec(vec![ColumnKind::Date]);
        let mut buf = Vec::new();
        encode_row_schema_into(&[Value::String("1996-01-02".to_string())], &codec, &mut buf)
            .unwrap();
        // 1B count + 1B bitmap + 3B date = 5B (vs Phase A: 1+1+4+10 = 16B).
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn datetime_round_trip() {
        // Phase B normalises microsecond fields away when zero.
        schema_round_trip(
            vec![Value::String("1996-01-02 12:34:56".to_string())],
            vec![ColumnKind::DateTime { fsp: 0 }],
        );
        schema_round_trip(
            vec![Value::String("0000-00-00 00:00:00".to_string())],
            vec![ColumnKind::DateTime { fsp: 0 }],
        );
        // Encoded width is 5 bytes for DATETIME(0).
        let codec = codec(vec![ColumnKind::DateTime { fsp: 0 }]);
        let mut buf = Vec::new();
        encode_row_schema_into(
            &[Value::String("2024-12-31 23:59:59".to_string())],
            &codec,
            &mut buf,
        )
        .unwrap();
        // 1B count + 1B bitmap + 5B datetime = 7B (vs Phase A: ~25B).
        assert_eq!(buf.len(), 7);
    }

    #[test]
    fn timestamp_round_trip_preserves_utc_identity_after_2038() {
        let value = Value::Timestamp(4_102_444_799_123_456); // 2099-12-31T23:59:59.123456Z
        let codec = codec(vec![ColumnKind::Timestamp { fsp: 6 }]);
        let mut buf = Vec::new();
        encode_row_schema_into(&[value.clone()], &codec, &mut buf).unwrap();
        assert_eq!(decode_row_schema(&buf, &codec).unwrap(), vec![value]);
    }

    #[test]
    fn timestamp_9999_round_trip_at_every_precision() {
        for fsp in 0..=6 {
            let quantum = temporal_quantum(fsp);
            let max = crate::types::MAX_DATETIME_MICROS / quantum * quantum;
            let codec = codec(vec![ColumnKind::Timestamp { fsp }]);
            for micros in [
                crate::types::MIN_TIMESTAMP_MICROS,
                max,
                crate::types::ZERO_DATETIME,
            ] {
                let value = Value::Timestamp(micros);
                let mut encoded = Vec::new();
                encode_row_schema_into(&[value.clone()], &codec, &mut encoded).unwrap();
                assert_eq!(decode_row_schema(&encoded, &codec).unwrap(), vec![value]);
            }
        }
    }

    #[test]
    fn decimal_round_trip() {
        // Positive, with declared scale.
        schema_round_trip(
            vec![Value::Decimal("21168.23".to_string())],
            vec![ColumnKind::Decimal { scale: 2 }],
        );
        // Zero magnitude — encoded as sign + 0 nbytes (only 2B).
        schema_round_trip(
            vec![Value::Decimal("0.00".to_string())],
            vec![ColumnKind::Decimal { scale: 2 }],
        );
        // Negative.
        schema_round_trip(
            vec![Value::Decimal("-12.40".to_string())],
            vec![ColumnKind::Decimal { scale: 2 }],
        );
        // Scale 0 (integer-shaped DECIMAL).
        schema_round_trip(
            vec![Value::Decimal("12345".to_string())],
            vec![ColumnKind::Decimal { scale: 0 }],
        );
    }

    #[test]
    fn decimal_wide_round_trip_is_lossless_and_scale_aware() {
        let maximum = "99999999999999999999999999999999999.999999999999999999999999999999";
        schema_round_trip(
            vec![Value::Decimal(maximum.to_string())],
            vec![ColumnKind::DecimalWide { scale: 30 }],
        );

        let codec = codec(vec![ColumnKind::DecimalWide { scale: 2 }]);
        let mut buf = Vec::new();
        encode_row_schema_into(
            &[Value::Decimal(
                "-1234567890123456789012345678901234567890.125".to_string(),
            )],
            &codec,
            &mut buf,
        )
        .unwrap();
        let decoded = decode_row_schema(&buf, &codec).unwrap();
        assert_eq!(
            decoded[0].to_display_string(),
            "-1234567890123456789012345678901234567890.13"
        );
    }

    #[test]
    fn decimal_extra_fractional_digits_rounded() {
        // MySQL *rounds* fractional digits beyond the declared scale
        // (half away from zero): 12.345 → 12.35, -12.345 → -12.35,
        // 12.344 → 12.34.
        let codec = codec(vec![ColumnKind::Decimal { scale: 2 }]);
        for (input, mag, text) in [
            ("12.345", 1235i64, "12.35"),
            ("-12.345", -1235, "-12.35"),
            ("12.344", 1234, "12.34"),
        ] {
            let mut buf = Vec::new();
            encode_row_schema_into(&[Value::Decimal(input.to_string())], &codec, &mut buf).unwrap();
            let decoded = decode_row_schema(&buf, &codec).unwrap();
            assert_eq!(
                decoded,
                vec![Value::DecimalCompact(mag, 2)],
                "input {input}"
            );
            assert_eq!(decoded[0].to_display_string(), text);
        }
    }

    #[test]
    fn decimal_scientific_notation_is_rescaled_exactly() {
        assert_eq!(
            parse_decimal_string_to_magnitude("1.175494345e-32", 30).unwrap(),
            (false, 0)
        );
        assert_eq!(
            parse_decimal_string_to_magnitude("1e-1", 4).unwrap(),
            (false, 1000)
        );
        assert_eq!(
            parse_decimal_string_to_magnitude("-1.25e2", 2).unwrap(),
            (true, 12_500)
        );
        assert_eq!(
            parse_decimal_string_to_magnitude("1e100", 0)
                .unwrap_err()
                .code(),
            1264
        );
    }

    // ----- Phase C: fixed-width DECIMAL kinds (i32 / i64 / i128) -----

    #[test]
    fn column_kind_for_picks_fixed_width_by_precision() {
        // P ≤ 9 → DecimalFixed { width: 4 }
        let k = column_kind_for(
            &DataTypeKind::Decimal {
                precision: Some(9),
                scale: Some(2),
            },
            false,
        );
        assert_eq!(k, ColumnKind::DecimalFixed { width: 4, scale: 2 });
        // P ≤ 18 → DecimalFixed { width: 8 }
        let k = column_kind_for(
            &DataTypeKind::Decimal {
                precision: Some(15),
                scale: Some(4),
            },
            false,
        );
        assert_eq!(k, ColumnKind::DecimalFixed { width: 8, scale: 4 });
        // P > 18, ≤ 38 → DecimalFixed { width: 16 }
        let k = column_kind_for(
            &DataTypeKind::Decimal {
                precision: Some(30),
                scale: Some(6),
            },
            false,
        );
        assert_eq!(
            k,
            ColumnKind::DecimalFixed {
                width: 16,
                scale: 6
            }
        );
        // MySQL's P=65 range uses the lossless variable BCD lane.
        let k = column_kind_for(
            &DataTypeKind::Decimal {
                precision: Some(65),
                scale: Some(30),
            },
            false,
        );
        assert_eq!(k, ColumnKind::DecimalWide { scale: 30 });
        let spec = row_codec_spec(
            &DataTypeKind::Decimal {
                precision: Some(65),
                scale: Some(30),
            },
            false,
        );
        let bytes = serde_json::to_vec(&spec).unwrap();
        assert_eq!(
            serde_json::from_slice::<RowCodecSpec>(&bytes).unwrap().kind,
            ColumnKind::DecimalWide { scale: 30 }
        );
        // No precision → legacy variable-length kind.
        let k = column_kind_for(
            &DataTypeKind::Decimal {
                precision: None,
                scale: None,
            },
            false,
        );
        assert_eq!(k, ColumnKind::Decimal { scale: 0 });
    }

    #[test]
    fn decimal_fixed_4byte_round_trip() {
        // DECIMAL(9, 2): magnitudes fit i32.
        let kinds = vec![ColumnKind::DecimalFixed { width: 4, scale: 2 }];
        let codec = codec(kinds.clone());
        // Encoded body is exactly 4 bytes regardless of value.
        for &(input, expect_mag) in &[
            (12345i64, 12345i64),
            (-9999i64, -9999i64),
            (0i64, 0i64),
            (i32::MAX as i64, i32::MAX as i64),
            (i32::MIN as i64, i32::MIN as i64),
        ] {
            let mut buf = Vec::new();
            encode_row_schema_into(&[Value::DecimalCompact(input, 2)], &codec, &mut buf).unwrap();
            // 1B count + 1B bitmap + 4B body = 6B
            assert_eq!(buf.len(), 6, "wrong encoded size for value {input}");
            let decoded = decode_row_schema(&buf, &codec).unwrap();
            assert_eq!(decoded, vec![Value::DecimalCompact(expect_mag, 2)]);
        }
    }

    #[test]
    fn decimal_fixed_8byte_round_trip() {
        // DECIMAL(15, 4) — typical TPC-H l_extendedprice shape.
        let kinds = vec![ColumnKind::DecimalFixed { width: 8, scale: 4 }];
        let codec = codec(kinds);
        let mut buf = Vec::new();
        encode_row_schema_into(&[Value::DecimalCompact(211_682_300, 4)], &codec, &mut buf).unwrap();
        // 1B count + 1B bitmap + 8B body = 10B (vs ~10–14B variable)
        assert_eq!(buf.len(), 10);
        let decoded = decode_row_schema(&buf, &codec).unwrap();
        assert_eq!(decoded, vec![Value::DecimalCompact(211_682_300, 4)]);
    }

    #[test]
    fn decimal_fixed_16byte_round_trip() {
        // DECIMAL(38, 0) — full i128 lane.
        let kinds = vec![ColumnKind::DecimalFixed {
            width: 16,
            scale: 0,
        }];
        let codec = codec(kinds);
        let mut buf = Vec::new();
        // Magnitude exceeds i64::MAX, so the decoder must use the binary
        // Decimal128 lane without formatting through a string.
        encode_row_schema_into(
            &[Value::Decimal("99999999999999999999".to_string())],
            &codec,
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf.len(), 18); // 1+1+16
        let decoded = decode_row_schema(&buf, &codec).unwrap();
        assert_eq!(
            decoded,
            vec![Value::Decimal128(99_999_999_999_999_999_999, 0)]
        );
    }

    #[test]
    fn decimal_fixed_overflow_rejected() {
        // DECIMAL(9, 0) only fits i32; a value beyond i32::MAX must
        // raise an error (matches MySQL behaviour for out-of-range
        // values).
        let codec = codec(vec![ColumnKind::DecimalFixed { width: 4, scale: 0 }]);
        let mut buf = Vec::new();
        let err = encode_row_schema_into(
            &[Value::DecimalCompact(10_000_000_000_i64, 0)],
            &codec,
            &mut buf,
        )
        .unwrap_err();
        assert_eq!(err.code(), 1264);
    }

    #[test]
    fn decimal_fixed_storage_savings_vs_variable() {
        // Same value, two kinds: variable carries 1B sign + 1B nbytes
        // + N bytes magnitude. For a typical TPC-H lineitem DECIMAL
        // value the fixed path saves header bytes.
        let val = Value::DecimalCompact(2_116_823, 2);
        let variable = codec(vec![ColumnKind::Decimal { scale: 2 }]);
        let fixed = codec(vec![ColumnKind::DecimalFixed { width: 4, scale: 2 }]);
        let mut a = Vec::new();
        let mut b = Vec::new();
        encode_row_schema_into(&[val.clone()], &variable, &mut a).unwrap();
        encode_row_schema_into(&[val], &fixed, &mut b).unwrap();
        // Variable: 1+1+(sign+nbytes+3) = 2+1+1+3 = 7 (count+bitmap=2, body=5)
        // Fixed:    1+1+4 = 6 (count+bitmap=2, body=4)
        assert_eq!(b.len(), 6);
        assert!(
            b.len() < a.len(),
            "fixed ({} bytes) should be smaller than variable ({} bytes)",
            b.len(),
            a.len(),
        );
    }

    #[test]
    fn year_round_trip() {
        // Year stores 1B (year - 1900); decode returns the binary
        // Value::Year variant whose canonical display form matches
        // the legacy YYYY string output.
        let codec = codec(vec![ColumnKind::Year]);
        let mut buf = Vec::new();
        encode_row_schema_into(&[Value::Int(1996)], &codec, &mut buf).unwrap();
        let decoded = decode_row_schema(&buf, &codec).unwrap();
        assert_eq!(decoded, vec![Value::Year(96)]);
        assert_eq!(decoded[0].to_display_string(), "1996");
        // Zero year sentinel.
        buf.clear();
        encode_row_schema_into(&[Value::Int(0)], &codec, &mut buf).unwrap();
        let decoded = decode_row_schema(&buf, &codec).unwrap();
        assert_eq!(decoded, vec![Value::Year(0)]);
        assert_eq!(decoded[0].to_display_string(), "0000");
    }

    #[test]
    fn time_round_trip() {
        schema_round_trip(
            vec![Value::String("12:34:56".to_string())],
            vec![ColumnKind::Time { fsp: 0 }],
        );
        // Negative TIME (legal in MySQL up to -838:59:59).
        schema_round_trip(
            vec![Value::String("-100:30:00".to_string())],
            vec![ColumnKind::Time { fsp: 0 }],
        );
    }

    #[test]
    fn null_bitmap_skips_payload_bytes() {
        let kinds = vec![
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
            ColumnKind::Date,
            ColumnKind::VarString,
        ];
        let codec_with = codec(kinds.clone());
        let mut nullable_buf = Vec::new();
        encode_row_schema_into(
            &[Value::Int(1), Value::Null, Value::Null],
            &codec_with,
            &mut nullable_buf,
        )
        .unwrap();
        // v5 keeps the fixed block dense for offset-direct reads:
        // 1B count + 1B bitmap + (4B int + 3B date) + 2B var dir = 11B.
        // NULL varlen columns still spend no payload bytes.
        assert_eq!(nullable_buf.len(), 11);
    }

    #[test]
    fn layout_id_maps_old_and_new_column_sets() {
        let columns = vec![
            ColumnCodec {
                kind: ColumnKind::Int {
                    width: 4,
                    signed: true,
                },
            },
            ColumnCodec {
                kind: ColumnKind::VarString,
            },
            ColumnCodec {
                kind: ColumnKind::VarString,
            },
        ];
        let old_layout = RowLayoutCodec::new(
            1,
            vec![
                RowLayoutColumnCodec {
                    column_index: 0,
                    kind: columns[0].kind.clone(),
                    nullable: true,
                },
                RowLayoutColumnCodec {
                    column_index: 1,
                    kind: columns[1].kind.clone(),
                    nullable: true,
                },
            ],
        );
        let new_layout = RowLayoutCodec::new(
            2,
            vec![
                RowLayoutColumnCodec {
                    column_index: 0,
                    kind: columns[0].kind.clone(),
                    nullable: true,
                },
                RowLayoutColumnCodec {
                    column_index: 2,
                    kind: columns[2].kind.clone(),
                    nullable: true,
                },
            ],
        );
        let old_writer = RowCodec {
            columns: columns.clone(),
            active_layout_id: 1,
            layouts: vec![old_layout.clone(), new_layout.clone()],
            defaults: vec![Value::Null, Value::Null, Value::String("fresh".to_string())],
        };
        let new_reader = RowCodec {
            columns,
            active_layout_id: 2,
            layouts: vec![old_layout, new_layout],
            defaults: vec![Value::Null, Value::Null, Value::String("fresh".to_string())],
        };

        let old_payload = encode_mvcc_row(
            RowHeader::visible(1),
            &[
                Value::Int(1),
                Value::String("old-dropped".to_string()),
                Value::String("should-use-default".to_string()),
            ],
            &old_writer,
            &[],
        )
        .unwrap();
        let (_, old_values) = decode_mvcc_row(&old_payload, &new_reader).unwrap();
        assert_eq!(old_values[0], Value::Int(1));
        assert_eq!(old_values[1], Value::String("old-dropped".to_string()));
        assert_eq!(old_values[2], Value::String("fresh".to_string()));

        let new_payload = encode_mvcc_row(
            RowHeader::visible(2),
            &[
                Value::Int(2),
                Value::String("not-written".to_string()),
                Value::String("new-public".to_string()),
            ],
            &new_reader,
            &[],
        )
        .unwrap();
        let (_, new_values) = decode_mvcc_row(&new_payload, &new_reader).unwrap();
        assert_eq!(new_values[0], Value::Int(2));
        assert_eq!(new_values[1], Value::Null);
        assert_eq!(new_values[2], Value::String("new-public".to_string()));

        let (_, old_projected, stored_count) =
            decode_mvcc_row_columns(&old_payload, &[2], &new_reader).unwrap();
        assert_eq!(stored_count, LAYOUT_AWARE_STORED_COUNT);
        assert_eq!(old_projected, vec![Value::String("fresh".to_string())]);
        let (_, new_projected, stored_count) =
            decode_mvcc_row_columns(&new_payload, &[2], &new_reader).unwrap();
        assert_eq!(stored_count, LAYOUT_AWARE_STORED_COUNT);
        assert_eq!(new_projected, vec![Value::String("new-public".to_string())]);
    }

    #[test]
    fn projection_via_schema_codec() {
        let kinds = vec![
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
            ColumnKind::VarString,
            ColumnKind::Date,
            ColumnKind::Decimal { scale: 2 },
        ];
        let c = codec(kinds);
        let values = vec![
            Value::Int(7),
            Value::String("hello".to_string()),
            Value::String("1996-01-02".to_string()),
            Value::Decimal("3.14".to_string()),
        ];
        let mut buf = Vec::new();
        encode_row_schema_into(&values, &c, &mut buf).unwrap();
        let mut out = Vec::new();
        let stored = decode_row_schema_columns_into(&buf, &c, &[3, 0], &mut out).unwrap();
        assert_eq!(stored, LAYOUT_AWARE_STORED_COUNT);
        // Projection picks col 3 (DECIMAL, binary) and col 0 (INT).
        assert_eq!(out, vec![Value::DecimalCompact(314, 2), Value::Int(7)]);
        assert_eq!(out[0].to_display_string(), "3.14");
    }

    #[test]
    fn tpch_lineitem_row_v3_density() {
        // Phase B layout for a TPC-H lineitem-shape row. We expect a
        // material reduction vs the Phase A (self-describing) lower
        // bound used in `savings_vs_v1_lineitem_like_row`.
        let kinds = vec![
            // 5 BIGINT
            ColumnKind::Int {
                width: 8,
                signed: true,
            },
            ColumnKind::Int {
                width: 8,
                signed: true,
            },
            ColumnKind::Int {
                width: 8,
                signed: true,
            },
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
            // 4 DECIMAL(15,2)
            ColumnKind::Decimal { scale: 2 },
            ColumnKind::Decimal { scale: 2 },
            ColumnKind::Decimal { scale: 2 },
            ColumnKind::Decimal { scale: 2 },
            // 3 DATE
            ColumnKind::Date,
            ColumnKind::Date,
            ColumnKind::Date,
            // CHAR(1), CHAR(25), CHAR(10)
            ColumnKind::VarString,
            ColumnKind::VarString,
            ColumnKind::VarString,
            // VARCHAR(44)
            ColumnKind::VarString,
        ];
        let values = vec![
            Value::Int(1),
            Value::Int(155190),
            Value::Int(7706),
            Value::Int(1),
            Value::Int(17),
            Value::Decimal("21168.23".to_string()),
            Value::Decimal("0.04".to_string()),
            Value::Decimal("0.02".to_string()),
            Value::Decimal("0.03".to_string()),
            Value::String("1996-01-02".to_string()),
            Value::String("1996-01-31".to_string()),
            Value::String("1996-02-12".to_string()),
            Value::String("N".to_string()),
            Value::String("DELIVER IN PERSON       ".to_string()),
            Value::String("TRUCK     ".to_string()),
            Value::String("egular courts above the".to_string()),
        ];
        let c = codec(kinds);
        let mut buf = Vec::new();
        encode_row_schema_into(&values, &c, &mut buf).unwrap();
        // Wins vs Phase A (~150-180B for the same row): 3 DATEs go
        // from a 15-byte ASCII string each to 4B binary (-33B), 4
        // DECIMAL(15,2) drop their ASCII representation for sign +
        // nbytes + magnitude (-~20B), and the per-column type tag is
        // gone (-16B). What remains is dominated by the VarString
        // payloads themselves.
        assert!(
            buf.len() <= 145,
            "v5 lineitem row {} should fit in 145B; got {}",
            buf.len(),
            buf.len()
        );
        // Round-trip preserves values via the canonical display form
        // — codec now emits binary `Value::Date / DecimalCompact` for
        // those columns rather than ASCII strings, but the user-visible
        // form is unchanged.
        let decoded = decode_row_schema(&buf, &c).unwrap();
        let decoded_disp: Vec<String> = decoded.iter().map(|v| v.to_display_string()).collect();
        let expected_disp: Vec<String> = values.iter().map(|v| v.to_display_string()).collect();
        assert_eq!(decoded_disp, expected_disp);
    }

    // ----- Phase 2a: fixed_column_byte_range_in_payload tests -----

    fn build_codec(kinds: Vec<ColumnKind>) -> RowCodec {
        let cols = kinds
            .into_iter()
            .map(|kind| ColumnCodec { kind })
            .collect::<Vec<_>>();
        test_row_codec_from_columns(cols)
    }

    #[test]
    fn fixed_column_byte_range_locates_int_columns() {
        let c = build_codec(vec![
            ColumnKind::Int {
                width: 8,
                signed: true,
            },
            ColumnKind::Int {
                width: 4,
                signed: false,
            },
            ColumnKind::Int {
                width: 1,
                signed: true,
            },
        ]);
        let values = vec![Value::Int(0xdead_beef), Value::Int(42), Value::Int(-3)];
        let payload = encode_mvcc_row(RowHeader::visible(1), &values, &c, &[]).expect("encode");

        // Column 0: 8-byte BIGINT — verify byte range and round-trip
        // decode matches original value.
        let r0 = fixed_column_byte_range_in_payload(&payload, &c, 0)
            .unwrap()
            .expect("col 0 patchable");
        assert_eq!(r0.width, 8);
        let decoded0 = decode_fixed_value(
            &r0.kind,
            &payload[r0.offset_in_payload..r0.offset_in_payload + r0.width],
        )
        .unwrap();
        assert_eq!(decoded0, Value::Int(0xdead_beef));

        let r1 = fixed_column_byte_range_in_payload(&payload, &c, 1)
            .unwrap()
            .expect("col 1 patchable");
        assert_eq!(r1.width, 4);
        let r2 = fixed_column_byte_range_in_payload(&payload, &c, 2)
            .unwrap()
            .expect("col 2 patchable");
        assert_eq!(r2.width, 1);

        // Ranges must not overlap.
        assert!(r0.offset_in_payload + r0.width <= r1.offset_in_payload);
        assert!(r1.offset_in_payload + r1.width <= r2.offset_in_payload);
    }

    #[test]
    fn fixed_column_byte_range_in_place_patch_round_trips() {
        let c = build_codec(vec![
            ColumnKind::Int {
                width: 8,
                signed: true,
            },
            ColumnKind::VarString,
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
        ]);
        let values = vec![
            Value::Int(100),
            Value::String("hello".into()),
            Value::Int(7),
        ];
        let mut payload = encode_mvcc_row(RowHeader::visible(1), &values, &c, &[]).expect("encode");

        // Patch column 0 (BIGINT) in place to 999.
        let r = fixed_column_byte_range_in_payload(&payload, &c, 0)
            .unwrap()
            .unwrap();
        let mut buf = vec![0u8; r.width];
        encode_fixed_value_into(&r.kind, &Value::Int(999), &mut buf).unwrap();
        payload[r.offset_in_payload..r.offset_in_payload + r.width].copy_from_slice(&buf);

        // Also patch column 2 (INT) to -42.
        let r2 = fixed_column_byte_range_in_payload(&payload, &c, 2)
            .unwrap()
            .unwrap();
        let mut buf2 = vec![0u8; r2.width];
        encode_fixed_value_into(&r2.kind, &Value::Int(-42), &mut buf2).unwrap();
        payload[r2.offset_in_payload..r2.offset_in_payload + r2.width].copy_from_slice(&buf2);

        // Decode whole row — varlen string must be undisturbed and
        // patched ints must be visible.
        let decoded = decode_mvcc_row(&payload, &c).unwrap().1;
        assert_eq!(
            decoded,
            vec![
                Value::Int(999),
                Value::String("hello".into()),
                Value::Int(-42)
            ]
        );
    }

    #[test]
    fn fixed_column_byte_range_returns_none_for_varlen() {
        let c = build_codec(vec![
            ColumnKind::Int {
                width: 8,
                signed: true,
            },
            ColumnKind::VarString,
            ColumnKind::VarBytes,
            ColumnKind::Json,
        ]);
        let values = vec![
            Value::Int(1),
            Value::String("v".into()),
            Value::Bytes(b"b".to_vec()),
            Value::Json(b"{}".to_vec()),
        ];
        let payload = encode_mvcc_row(RowHeader::visible(1), &values, &c, &[]).expect("encode");
        assert!(fixed_column_byte_range_in_payload(&payload, &c, 1)
            .unwrap()
            .is_none());
        assert!(fixed_column_byte_range_in_payload(&payload, &c, 2)
            .unwrap()
            .is_none());
        assert!(fixed_column_byte_range_in_payload(&payload, &c, 3)
            .unwrap()
            .is_none());
    }

    #[test]
    fn fixed_column_byte_range_returns_none_for_null() {
        let c = build_codec(vec![
            ColumnKind::Int {
                width: 8,
                signed: true,
            },
            ColumnKind::Int {
                width: 4,
                signed: true,
            },
        ]);
        let values = vec![Value::Int(1), Value::Null];
        let payload = encode_mvcc_row(RowHeader::visible(1), &values, &c, &[]).expect("encode");
        // col 0 is non-null int — patchable.
        assert!(fixed_column_byte_range_in_payload(&payload, &c, 0)
            .unwrap()
            .is_some());
        // col 1 is NULL — promoting to non-null requires bitmap toggle,
        // which the in-place patch path does not support yet.
        assert!(fixed_column_byte_range_in_payload(&payload, &c, 1)
            .unwrap()
            .is_none());
    }

    #[test]
    fn fixed_column_byte_range_returns_none_for_out_of_range_column() {
        let c = build_codec(vec![ColumnKind::Int {
            width: 8,
            signed: true,
        }]);
        let payload = encode_mvcc_row(RowHeader::visible(1), &[Value::Int(1)], &c, &[]).unwrap();
        // Column index 99 does not exist in the table — the helper
        // surfaces this as `None` so the caller can fall back without
        // an extra layout-walk.
        assert!(fixed_column_byte_range_in_payload(&payload, &c, 99)
            .unwrap()
            .is_none());
    }

    #[test]
    fn fixed_column_byte_range_returns_none_for_forwarding_stub_and_row_piece() {
        let c = build_codec(vec![ColumnKind::Int {
            width: 8,
            signed: true,
        }]);
        let stub = encode_heap_forwarding_stub(RowId {
            page_id: 1,
            slot_id: 2,
        });
        assert!(fixed_column_byte_range_in_payload(&stub, &c, 0)
            .unwrap()
            .is_none());

        let mut piece = encode_mvcc_row(RowHeader::visible(1), &[Value::Int(1)], &c, &[]).unwrap();
        // Flip the row-piece bit; in-place patch must defer to the
        // forwarding-chain path on row pieces.
        piece[1] |= MVCC_FLAG_ROW_PIECE;
        assert!(fixed_column_byte_range_in_payload(&piece, &c, 0)
            .unwrap()
            .is_none());
    }

    #[test]
    fn fixed_column_byte_range_locates_date_and_decimal_fixed() {
        // DecimalFixed(4, scale=2) → 4 bytes; Date → 3 bytes;
        // DateTime fsp=0 → 5 bytes. Round-trip a patched row through
        // the full decoder to make sure offsets are stable across kinds
        // with non-power-of-2 widths.
        let c = build_codec(vec![
            ColumnKind::DecimalFixed { width: 4, scale: 2 },
            ColumnKind::Date,
            ColumnKind::DateTime { fsp: 0 },
            ColumnKind::Int {
                width: 8,
                signed: true,
            },
        ]);
        let values = vec![
            Value::DecimalCompact(12345, 2), // 123.45
            Value::Date(20_000),             // arbitrary days
            Value::DateTime(1_700_000_000_000_000),
            Value::Int(77),
        ];
        let mut payload = encode_mvcc_row(RowHeader::visible(1), &values, &c, &[]).expect("encode");

        let r_dec = fixed_column_byte_range_in_payload(&payload, &c, 0)
            .unwrap()
            .unwrap();
        assert_eq!(r_dec.width, 4);
        let r_date = fixed_column_byte_range_in_payload(&payload, &c, 1)
            .unwrap()
            .unwrap();
        assert_eq!(r_date.width, 3);
        let r_dt = fixed_column_byte_range_in_payload(&payload, &c, 2)
            .unwrap()
            .unwrap();
        assert_eq!(r_dt.width, 5);

        // Patch the Int and verify other kinds round-trip unchanged.
        let r_int = fixed_column_byte_range_in_payload(&payload, &c, 3)
            .unwrap()
            .unwrap();
        let mut buf = vec![0u8; r_int.width];
        encode_fixed_value_into(&r_int.kind, &Value::Int(-999), &mut buf).unwrap();
        payload[r_int.offset_in_payload..r_int.offset_in_payload + r_int.width]
            .copy_from_slice(&buf);

        let decoded = decode_mvcc_row(&payload, &c).unwrap().1;
        assert_eq!(decoded[0], Value::DecimalCompact(12345, 2));
        assert_eq!(decoded[1], Value::Date(20_000));
        assert_eq!(decoded[2], Value::DateTime(1_700_000_000_000_000));
        assert_eq!(decoded[3], Value::Int(-999));
    }
}

#[cfg(test)]
mod inline_string_decode_tests {
    use super::*;

    #[test]
    fn inline_cell_decode_preserves_variants_utf8_boundaries_and_errors() {
        let cap = crate::types::SMALL_STRING_INLINE_CAP;
        let inputs = vec![
            String::new(),
            "汉字".to_owned(),
            "x".repeat(cap),
            "y".repeat(cap + 1),
            "short again".to_owned(),
        ];
        let mut cell = Value::Null;
        let mut promoted = false;
        for text in &inputs {
            decode_var_for_kind_into(&ColumnKind::VarString, text.as_bytes(), &mut cell).unwrap();
            assert_eq!(cell, Value::String(text.clone()));
            promoted |= text.len() > cap;
            assert_eq!(
                matches!(cell, Value::String(_)),
                promoted,
                "buffer-backed strings stay in that lane"
            );
        }
        for len in 0..=cap {
            let text = "a".repeat(len);
            let mut cell = Value::SmallString(crate::types::SmallStringBuf::from_slice(b"old"));
            decode_var_for_kind_into(&ColumnKind::VarString, text.as_bytes(), &mut cell).unwrap();
            assert!(matches!(cell, Value::SmallString(_)));
            assert_eq!(
                cell,
                decode_var_for_kind(&ColumnKind::VarString, text.as_bytes()).unwrap()
            );
            let before = cell.clone();
            assert!(decode_var_for_kind_into(&ColumnKind::VarString, &[0xff], &mut cell).is_err());
            assert_eq!(cell, before);
        }
    }
}
